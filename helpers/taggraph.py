import bisect
import collections
import csv
import gzip
import hashlib
import io
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import zstandard as zstd
from tqdm import tqdm

from .config import (
    DB_EXPORT_INDEX,
    DB_EXPORT_MANIFEST,
    DICT_MIN_SAMPLES,
    DOWNLOAD_CHUNK,
    ROOT,
    TAG_CATEGORIES,
    TAG_EXPORT_NAMES,
    TAG_EXPORT_PERIOD,
    TAG_EXPORT_RETRY_ATTEMPTS,
    TAG_EXPORT_RETRY_INTERVAL,
    TAG_GRAPH_TIMEOUT,
    TAG_MIN_POST_COUNT,
    TQDM_STEADY,
    USER_AGENT,
    ZSTD_LEVEL,
)
from .database import db, run_vacuum
from .e6api import _session, rate_limit
from .runtime import _log_next, log
from .tagcodec import _get_varint, _put_varint


class _TagGraph:
    """In-memory view of the pruned alias + implication graph.

    Loaded lazily from the tag_graph table on first use and cached until a
    refresh invalidates it. Every public method is a no-op passthrough when
    the table is empty, so the app behaves exactly as it did before the first
    successful download.
    """

    def __init__(self):
        self.lock = threading.RLock()
        self._loaded = False
        self._aliases = {}  # antecedent -> canonical consequent
        self._implies = {}  # antecedent -> tuple of direct consequents
        self._ancestors = {}  # memo: tag -> frozenset of transitive consequents
        self._alias_keys = ()  # antecedents, sorted, for bisect prefix lookup
        self.updated_at = None
        self.alias_count = 0
        self.implication_count = 0

    # -- loading --

    @staticmethod
    def _pack(pairs):
        """Serialize (antecedent, consequent) pairs to a compressed blob."""
        text = "\n".join(f"{a}\t{c}" for a, c in sorted(pairs))
        cctx = zstd.ZstdCompressor(level=ZSTD_LEVEL)
        return cctx.compress(text.encode("utf-8"))

    @staticmethod
    def _unpack(blob):
        """Inverse of _pack. Returns a list of (antecedent, consequent)."""
        if not blob:
            return []
        dctx = zstd.ZstdDecompressor()
        text = dctx.decompress(bytes(blob)).decode("utf-8")
        out = []
        for line in text.split("\n"):
            if not line:
                continue
            a, _, c = line.partition("\t")
            if c:
                out.append((a, c))
        return out

    def _load_locked(self):
        try:
            with db() as conn:
                row = conn.execute(
                    """SELECT aliases_blob, implications_blob,
                              alias_count, implication_count, updated_at
                       FROM tag_graph WHERE id = 1"""
                ).fetchone()
        except sqlite3.Error as e:
            log.warning(f"Tag graph: load failed ({e}); running without it.")
            row = None

        self._aliases = {}
        self._implies = {}
        self._ancestors = {}
        self._alias_keys = ()
        self.updated_at = None
        self.alias_count = 0
        self.implication_count = 0

        if row is not None:
            try:
                self._aliases = dict(self._unpack(row["aliases_blob"]))
                adjacency = {}
                for ante, cons in self._unpack(row["implications_blob"]):
                    adjacency.setdefault(ante, []).append(cons)
                self._implies = {k: tuple(v) for k, v in adjacency.items()}
                self._alias_keys = tuple(sorted(self._aliases))
                self.updated_at = row["updated_at"]
                self.alias_count = row["alias_count"]
                self.implication_count = row["implication_count"]
            except Exception as e:
                log.warning(f"Tag graph: blob decode failed ({e}); running without it.")
                self._aliases = {}
                self._implies = {}
                self._alias_keys = ()

        self._loaded = True

    def _ensure_loaded_locked(self):
        if not self._loaded:
            self._load_locked()

    def invalidate(self):
        """Force a reload from the DB on next use."""
        with self.lock:
            self._loaded = False

    def is_empty(self):
        with self.lock:
            self._ensure_loaded_locked()
            return not self._implies and not self._aliases

    def info(self):
        with self.lock:
            self._ensure_loaded_locked()
            return {
                "updated_at": self.updated_at,
                "aliases": self.alias_count,
                "implications": self.implication_count,
            }

    # -- lookups --

    def canonical(self, tag):
        """Map a tag through the alias table. Chains are pre-collapsed."""
        if not tag:
            return tag
        with self.lock:
            self._ensure_loaded_locked()
            return self._aliases.get(tag, tag)

    def alias_prefix(self, fragment, limit):
        """Aliases whose antecedent starts with `fragment`, as (dead, live).

        Lets the completion surface a renamed tag when you type the spelling
        you remember. Antecedents are held pre-sorted so this is a bisect and
        a short slice rather than a scan of the whole alias map — that matters
        because it runs on every keystroke.
        """
        if not fragment:
            return []
        with self.lock:
            self._ensure_loaded_locked()
            keys = self._alias_keys
            start = bisect.bisect_left(keys, fragment)
            out = []
            for i in range(start, len(keys)):
                if len(out) >= limit or not keys[i].startswith(fragment):
                    break
                out.append((keys[i], self._aliases[keys[i]]))
            return out

    def _ancestors_locked(self, tag):
        """Transitive set of tags implied by `tag`, excluding `tag` itself."""
        cached = self._ancestors.get(tag)
        if cached is not None:
            return cached
        out = set()
        stack = [tag]
        walked = {tag}
        while stack:
            for nxt in self._implies.get(stack.pop(), ()):
                if nxt in walked:
                    continue  # already handled, and guards against cycles
                walked.add(nxt)
                out.add(nxt)
                stack.append(nxt)
        out.discard(tag)  # self-implication would be a data bug, but be safe
        frozen = frozenset(out)
        self._ancestors[tag] = frozen
        return frozen

    def expand(self, tags):
        """Return `tags` plus everything they transitively imply.

        This is the search-time half: posts store only their most specific
        tags, so a query for a broad tag has to see the implied ones too.
        """
        with self.lock:
            self._ensure_loaded_locked()
            if not self._implies:
                return set(tags)
            out = set(tags)
            for t in tags:
                out |= self._ancestors_locked(t)
            return out

    def reduce(self, tags_dict):
        """Drop tags that another tag on the same post already implies.

        Runs across ALL categories at once, because implications cross them
        (character:midna implies copyright:twilight_princess). Category
        membership of the surviving tags is preserved. Also maps every tag
        through the alias table so nothing stale is written.

        Returns a new dict; the input is not mutated.
        """
        with self.lock:
            self._ensure_loaded_locked()
            aliases = self._aliases
            has_implications = bool(self._implies)

            canon = {}
            for cat in TAG_CATEGORIES:
                seen_in_cat = set()
                kept = []
                for t in tags_dict.get(cat) or []:
                    c = aliases.get(t, t)
                    if c in seen_in_cat:
                        continue  # two tags aliased to the same canonical form
                    seen_in_cat.add(c)
                    kept.append(c)
                canon[cat] = kept

            if not has_implications:
                return canon

            implied = set()
            for cat in TAG_CATEGORIES:
                for t in canon[cat]:
                    implied |= self._ancestors_locked(t)

            return {
                cat: [t for t in canon[cat] if t not in implied]
                for cat in TAG_CATEGORIES
            }


_tag_graph = _TagGraph()


# ---------- Tag store ----------
# The completion corpus: ~870k (name, category, post_count) rows, held as
# name-ordered chunks that are decompressed only when a lookup lands in them.
#
# Chunk payload (uncompressed), rows in ascending name order:
#   for each row:
#     varint : length of the prefix shared with the PREVIOUS name in this
#              chunk (0 for the first row, which is therefore self-contained)
#     varint : length in bytes of the remaining suffix
#     N bytes: that suffix, UTF-8
#     varint : category (indexes TAG_CATEGORIES)
#     varint : post_count
#
# Front-coding first is what makes the dictionary effective. e621 tags cluster
# hard by prefix once sorted (`wolf`, `wolf_boy`, `wolf_girl`, ...), so the
# shared-prefix elision removes most of the redundancy inside a chunk and zstd
# then works on what's left across chunks.
#
# Chunks are self-contained: a row's name depends only on rows above it within
# the same chunk, never on the previous chunk. That is what lets a lookup
# decompress one chunk in isolation.
TAG_CHUNK_SIZE = 512  # rows per chunk
TAG_STORE_DICT_SIZE = 112 * 1024
TAG_STORE_DICT_SAMPLES = 1024  # chunk payloads sampled to train the dictionary
TAG_STORE_CACHE = 96  # decompressed chunks held in memory (LRU)


class _TagStore:
    """Chunked, compressed view of the tag list.

    Only two questions are ever asked of it — "which tags start with this
    fragment" and "what is this exact tag" — and both resolve to a bisect over
    the in-memory first_name index followed by one or a few chunk decompresses.

    Every public method is a no-op passthrough when the store is empty, so the
    app behaves as it did before the first successful tag download.
    """

    def __init__(self):
        self.lock = threading.RLock()
        self._loaded = False
        self._first = []  # first_name per chunk, ascending — the bisect key
        self._last = []  # last_name per chunk, to bound a prefix walk
        self._maxpc = []  # max post_count per chunk, to bound the ranking
        self._ids = []  # chunk id per position
        self._dctx = None
        self._cache = collections.OrderedDict()  # chunk id -> decoded rows
        self.chunk_count = 0
        self.tag_count = 0
        self.raw_bytes = 0
        self.updated_at = None

    # -- codec --

    @staticmethod
    def _pack_chunk(rows):
        """Front-code and serialize one chunk's rows. Input must be name-sorted."""
        out = bytearray()
        prev = ""
        for name, category, post_count in rows:
            shared = 0
            limit = min(len(prev), len(name))
            while shared < limit and prev[shared] == name[shared]:
                shared += 1
            suffix = name[shared:].encode("utf-8")
            out += _put_varint(shared)
            out += _put_varint(len(suffix))
            out += suffix
            out += _put_varint(category)
            out += _put_varint(post_count)
            prev = name
        return bytes(out)

    @staticmethod
    def _unpack_chunk(raw):
        """Inverse of _pack_chunk. Returns [(name, category, post_count), ...].

        The shared-prefix count is in characters, not bytes, so the slice of
        the previous name is taken before encoding — tags are mostly ASCII but
        not all of them are (`hokkaidō_wolf`).
        """
        rows = []
        pos = 0
        prev = ""
        n = len(raw)
        while pos < n:
            shared, pos = _get_varint(raw, pos)
            length, pos = _get_varint(raw, pos)
            suffix = raw[pos : pos + length].decode("utf-8")
            pos += length
            category, pos = _get_varint(raw, pos)
            post_count, pos = _get_varint(raw, pos)
            name = prev[:shared] + suffix
            rows.append((name, category, post_count))
            prev = name
        return rows

    # -- loading --

    def _ensure_loaded_locked(self):
        if self._loaded:
            return
        self._first = []
        self._last = []
        self._maxpc = []
        self._ids = []
        self._cache.clear()
        self._dctx = None
        self.chunk_count = 0
        self.tag_count = 0
        self.raw_bytes = 0
        self.updated_at = None
        try:
            with db() as conn:
                meta = conn.execute(
                    """SELECT dict_blob, chunk_count, tag_count, raw_bytes,
                              updated_at FROM tag_store WHERE id = 1"""
                ).fetchone()
                if meta:
                    rows = conn.execute(
                        "SELECT id, first_name, last_name, max_post_count "
                        "FROM tag_chunks ORDER BY first_name"
                    ).fetchall()
                else:
                    rows = []
        except sqlite3.Error as e:
            log.warning(f"Tag store: load failed ({e}); completion disabled.")
            self._loaded = True
            return

        if meta:
            blob = meta["dict_blob"]
            self._dctx = zstd.ZstdDecompressor(
                dict_data=zstd.ZstdCompressionDict(bytes(blob)) if blob else None
            )
            self.chunk_count = meta["chunk_count"]
            self.tag_count = meta["tag_count"]
            self.raw_bytes = meta["raw_bytes"]
            self.updated_at = meta["updated_at"]
            self._ids = [r["id"] for r in rows]
            self._first = [r["first_name"] for r in rows]
            self._last = [r["last_name"] for r in rows]
            self._maxpc = [r["max_post_count"] for r in rows]

        self._loaded = True

    def invalidate(self):
        with self.lock:
            self._loaded = False
            self._cache.clear()

    def is_empty(self):
        with self.lock:
            self._ensure_loaded_locked()
            return not self._ids

    def _rows_locked(self, pos):
        """Decoded rows for the chunk at index `pos`, via the LRU."""
        chunk_id = self._ids[pos]
        hit = self._cache.get(chunk_id)
        if hit is not None:
            self._cache.move_to_end(chunk_id)
            return hit
        with db() as conn:
            row = conn.execute(
                "SELECT blob FROM tag_chunks WHERE id = ?", (chunk_id,)
            ).fetchone()
        if not row:
            return []
        rows = self._unpack_chunk(self._dctx.decompress(bytes(row["blob"])))
        self._cache[chunk_id] = rows
        if len(self._cache) > TAG_STORE_CACHE:
            self._cache.popitem(last=False)
        return rows

    # -- queries --

    def _candidates_locked(self, fragment):
        """Positions of every chunk whose name range overlaps the prefix."""
        # The chunk holding `fragment` may start before it, hence the -1.
        start = max(0, bisect.bisect_right(self._first, fragment) - 1)
        out = []
        for pos in range(start, len(self._ids)):
            if self._first[pos] > fragment and not self._first[pos].startswith(
                fragment
            ):
                break  # past the prefix range entirely
            if self._last[pos] < fragment:
                continue  # chunk ends before the prefix begins
            out.append(pos)
        return out

    def prefix(self, fragment, limit):
        """The `limit` highest-post_count tags starting with `fragment`.

        Candidate chunks are visited in descending max_post_count order rather
        than alphabetical order, and the walk stops as soon as the running
        `limit`-th best beats every remaining chunk's ceiling. For a one-letter
        fragment that is the difference between decoding ~400 chunks and
        decoding two: `a` matches ~50k tags, but the twelve with the highest
        post_count are concentrated in a handful of chunks, and the bound
        proves the rest cannot contribute before they are ever touched.

        The result is identical to ranking the full match set — the bound only
        skips chunks whose best possible row is strictly worse than one already
        held, so nothing that could place is discarded.
        """
        if not fragment or limit <= 0:
            return []
        with self.lock:
            self._ensure_loaded_locked()
            if not self._ids:
                return []
            candidates = self._candidates_locked(fragment)
            candidates.sort(key=lambda p: -self._maxpc[p])

            best = []  # (post_count, name, category), sorted, <= limit long
            for pos in candidates:
                # Strict `<`: an equal ceiling could still win the name
                # tie-break, so those chunks stay in the walk.
                if len(best) >= limit and self._maxpc[pos] < best[limit - 1][0]:
                    break
                for name, category, post_count in self._rows_locked(pos):
                    if name.startswith(fragment):
                        best.append((post_count, name, category))
                best.sort(key=lambda r: (-r[0], r[1]))
                del best[limit:]
            return [(n, c, pc) for pc, n, c in best]

    def get(self, name):
        """(category, post_count) for an exact tag, or None."""
        if not name:
            return None
        with self.lock:
            self._ensure_loaded_locked()
            if not self._ids:
                return None
            pos = max(0, bisect.bisect_right(self._first, name) - 1)
            if self._last[pos] < name:
                return None
            for row_name, category, post_count in self._rows_locked(pos):
                if row_name == name:
                    return (category, post_count)
                if row_name > name:
                    break
            return None

    def count(self):
        with self.lock:
            self._ensure_loaded_locked()
            return self.tag_count

    def info(self):
        with self.lock:
            self._ensure_loaded_locked()
            return {
                "tags": self.tag_count,
                "chunks": self.chunk_count,
                "raw_bytes": self.raw_bytes,
                "updated_at": self.updated_at,
                "cached_chunks": len(self._cache),
            }

    # -- building --

    @classmethod
    def build(cls, conn, row_iter, chunk_size=TAG_CHUNK_SIZE):
        """Rewrite tag_chunks + tag_store from name-sorted (name, cat, count).

        Two passes over the source: the first packs payloads to sample for
        dictionary training, the second packs again and compresses. Packing is
        cheap relative to zstd level 19, and re-walking a temp table costs far
        less memory than holding 870k rows plus their payloads at once.
        """

        def _chunks(rows):
            batch = []
            for row in rows:
                batch.append(row)
                if len(batch) >= chunk_size:
                    yield batch
                    batch = []
            if batch:
                yield batch

        samples = []
        total = 0
        for batch in _chunks(row_iter()):
            total += len(batch)
            samples.append(cls._pack_chunk(batch))
        if not total:
            conn.execute("DELETE FROM tag_chunks")
            conn.execute("DELETE FROM tag_store")
            return {"tags": 0, "chunks": 0, "raw_bytes": 0, "dict_bytes": 0}

        # Even spread rather than the first N, so the dictionary sees the whole
        # alphabet instead of overfitting to tags beginning with a digit.
        step = max(1, len(samples) // TAG_STORE_DICT_SAMPLES)
        training = samples[::step][:TAG_STORE_DICT_SAMPLES]
        dict_blob = None
        if len(training) >= DICT_MIN_SAMPLES:
            try:
                dict_blob = zstd.train_dictionary(
                    TAG_STORE_DICT_SIZE, training
                ).as_bytes()
            except zstd.ZstdError as e:
                log.warning(f"Tag store: dictionary training failed ({e}).")
        del samples, training

        cctx = zstd.ZstdCompressor(
            level=ZSTD_LEVEL,
            dict_data=zstd.ZstdCompressionDict(dict_blob) if dict_blob else None,
        )

        conn.execute("DELETE FROM tag_chunks")
        raw_bytes = 0
        chunk_count = 0

        def _packed():
            nonlocal raw_bytes, chunk_count
            for i, batch in enumerate(_chunks(row_iter())):
                raw = cls._pack_chunk(batch)
                raw_bytes += len(raw)
                chunk_count += 1
                yield (
                    i,
                    batch[0][0],
                    batch[-1][0],
                    max(r[2] for r in batch),
                    len(batch),
                    cctx.compress(raw),
                )

        conn.executemany(
            "INSERT INTO tag_chunks "
            "(id, first_name, last_name, max_post_count, tag_count, blob) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            _packed(),
        )
        conn.execute(
            """INSERT INTO tag_store
               (id, dict_blob, chunk_count, tag_count, raw_bytes, updated_at)
               VALUES (1, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                 dict_blob = excluded.dict_blob,
                 chunk_count = excluded.chunk_count,
                 tag_count = excluded.tag_count,
                 raw_bytes = excluded.raw_bytes,
                 updated_at = excluded.updated_at""",
            (dict_blob, chunk_count, total, raw_bytes, int(time.time())),
        )
        return {
            "tags": total,
            "chunks": chunk_count,
            "raw_bytes": raw_bytes,
            "dict_bytes": len(dict_blob) if dict_blob else 0,
        }


_tag_store = _TagStore()


# -- ingest --


def _find_export_file(prefix, root=None):
    """Newest local dump whose name starts with `prefix`, or None.

    Accepts anything ending .csv or .csv.gz, so a file downloaded straight
    from the browser (`tag_aliases-2026-07-27.csv.gz`) works as-is, and so
    does a plain `tag_aliases.csv`. When several are present the one with the
    newest date stamp in its filename wins; undated files are ranked by mtime.
    """
    root = root or ROOT
    candidates = [
        p
        for p in root.glob(f"{prefix}*")
        if p.is_file() and (p.name.endswith(".csv") or p.name.endswith(".csv.gz"))
    ]
    if not candidates:
        return None

    def rank(path):
        found = re.search(r"(\d{4}-\d{2}-\d{2})", path.name)
        return (found.group(1) if found else "", path.stat().st_mtime)

    return max(candidates, key=rank)


def _sha256_file(path):
    """Full SHA-256 of a file on disk, hex. Compared against the manifest."""
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fetch_export_manifest():
    """Fetch db_exports.json and return {name: entry} for the tag exports.

    Raises if the fetch fails or any of TAG_EXPORT_NAMES is absent — a partial
    manifest can't be reconciled against what we've stored, and pretending
    otherwise would strand one file at an old checksum forever.
    """
    rate_limit()
    response = _session.get(
        DB_EXPORT_MANIFEST, headers={"User-Agent": USER_AGENT}, timeout=60
    )
    response.raise_for_status()

    entries = {}
    for item in response.json():
        name = (item.get("name") or "").strip()
        if name not in TAG_EXPORT_NAMES:
            continue
        entries[name] = {
            "url": item.get("url") or f"{DB_EXPORT_INDEX}{name}.csv.gz",
            "checksum": (item.get("checksum") or "").strip().lower(),
            "file_size": int(item.get("file_size") or 0),
            "updated_at": (item.get("updated_at") or "").strip(),
        }

    missing = [n for n in TAG_EXPORT_NAMES if n not in entries]
    if missing:
        raise RuntimeError(f"manifest is missing {', '.join(missing)}")
    return entries


def _stored_exports():
    """What we last ingested, as {name: row-dict}. Empty before the first run."""
    with db() as conn:
        rows = conn.execute(
            "SELECT name, checksum, file_size, updated_at, ingested_at FROM db_exports"
        ).fetchall()
    return {row["name"]: dict(row) for row in rows}


def _record_exports(manifest):
    """Persist the manifest entries we just ingested."""
    now = int(time.time())
    with db() as conn:
        conn.executemany(
            """INSERT INTO db_exports
                   (name, checksum, file_size, updated_at, ingested_at)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(name) DO UPDATE SET
                 checksum = excluded.checksum,
                 file_size = excluded.file_size,
                 updated_at = excluded.updated_at,
                 ingested_at = excluded.ingested_at""",
            [
                (
                    name,
                    manifest[name]["checksum"],
                    manifest[name]["file_size"],
                    manifest[name]["updated_at"],
                    now,
                )
                for name in TAG_EXPORT_NAMES
            ],
        )


def _seconds_until_next_export():
    """How long until the next export is expected, from the newest stamp we
    have ingested. Zero when we're already overdue (or have never ingested).

    The manifest stamps carry a UTC offset, so they're parsed as aware
    datetimes and compared in UTC. An unparseable stamp falls back to the
    period from now, which is the same behaviour as the old fixed interval.
    """
    stamps = []
    for row in _stored_exports().values():
        try:
            parsed = datetime.fromisoformat(row["updated_at"])
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        stamps.append(parsed)

    if not stamps:
        return 0

    due = max(stamps).timestamp() + TAG_EXPORT_PERIOD
    return max(0, int(due - time.time()))


def _download_dump(url, dest):
    """Fetch one export to `dest`. Returns the name of whatever tool worked.

    Tries requests first (already a dependency, no subprocess), then falls
    back to wget and curl. The fallback exists because static1 has been
    inconsistent about non-browser clients; wget is known to work, so if the
    in-process fetch is ever refused we still get the file rather than
    silently running without a tag graph.
    """
    attempts = []

    try:
        rate_limit()
        with _session.get(
            url,
            headers={"User-Agent": USER_AGENT},
            stream=True,
            timeout=TAG_GRAPH_TIMEOUT,
        ) as response:
            response.raise_for_status()
            total = int(response.headers.get("Content-Length") or 0)
            with (
                open(dest, "wb") as handle,
                tqdm(
                    total=total or None,
                    desc=f"Downloading {dest.name}",
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    leave=False,
                    **TQDM_STEADY,
                ) as bar,
            ):
                # 64 KiB, not 1 MiB. The bar can only move when a chunk
                # lands, so the chunk size *is* the resolution of the bar —
                # at 1 MiB it stepped a megabyte at a time no matter what
                # tqdm was told. This is still far larger than the socket
                # read, so the write count barely changes; tqdm's mininterval
                # caps the repaints at 10/s regardless of how many arrive.
                for chunk in response.iter_content(DOWNLOAD_CHUNK):
                    handle.write(chunk)
                    bar.update(len(chunk))
        if dest.exists() and dest.stat().st_size:
            return "requests"
        attempts.append("requests: empty response")
    except Exception as e:
        attempts.append(f"requests: {e}")

    # No shell=True and no user-supplied components — the URL is built from a
    # module constant, so there's nothing here to inject into.
    for tool, command in (
        ("wget", ["wget", "-q", "--user-agent", USER_AGENT, "-O", str(dest), url]),
        ("curl", ["curl", "-fsSL", "-A", USER_AGENT, "-o", str(dest), url]),
    ):
        if shutil.which(tool) is None:
            attempts.append(f"{tool}: not installed")
            continue
        try:
            subprocess.run(command, check=True, timeout=TAG_GRAPH_TIMEOUT)
            if dest.exists() and dest.stat().st_size:
                return tool
            attempts.append(f"{tool}: wrote an empty file")
        except Exception as e:
            attempts.append(f"{tool}: {e}")

    raise RuntimeError(f"could not download {url} ({'; '.join(attempts)})")


def _download_exports(dest_dir, manifest):
    """Fetch all three tag exports into `dest_dir`, verified.

    Returns {name: Path}. Each file is hashed after download and compared
    against the manifest checksum; a mismatch gets one retry before giving up,
    since a truncated-but-non-empty file would otherwise be parsed as gospel.
    """
    paths = {}
    for name in TAG_EXPORT_NAMES:
        entry = manifest[name]
        dest = dest_dir / f"{name}.csv.gz"

        for attempt in (1, 2):
            tool = _download_dump(entry["url"], dest)
            actual = _sha256_file(dest)
            if not entry["checksum"] or actual == entry["checksum"]:
                log.info(
                    f"Tag data: fetched {name}.csv.gz "
                    f"({dest.stat().st_size / 1048576:.1f} MiB) via {tool}."
                )
                break
            log.warning(
                f"Tag data: {name}.csv.gz checksum mismatch on attempt "
                f"{attempt} (got {actual[:16]}, expected "
                f"{entry['checksum'][:16]})."
            )
        else:
            raise RuntimeError(f"{name}.csv.gz failed checksum verification twice")

        paths[name] = dest
    return paths


@contextmanager
def _export_sources(manifest, allow_download=True):
    """Yield ({name: path}, origin) for this refresh.

    Downloads go to a temp directory that is deleted on the way out, so no
    multi-megabyte CSVs are ever left sitting next to app.py. If the download
    fails we fall back to any dumps already in the app directory, which keeps
    the manual browser-download workflow available as an escape hatch. Local
    dumps can't be verified — there's no checksum to hold them to — so they're
    trusted as-is and the manifest is not recorded for them.

    `paths` is None (origin "missing") when the full set can't be assembled.
    """
    tmp = None
    paths = None
    origin = "missing"

    if allow_download and manifest:
        try:
            tmp = tempfile.TemporaryDirectory(prefix="curator-db_export-")
            paths = _download_exports(Path(tmp.name), manifest)
            origin = "download"
        except Exception as e:
            log.warning(f"Tag data: download failed ({e}); trying local dumps.")
            if tmp is not None:
                tmp.cleanup()
                tmp = None
            paths = None

    if paths is None:
        local = {name: _find_export_file(name) for name in TAG_EXPORT_NAMES}
        if all(local.values()):
            paths = local
            origin = "local (" + ", ".join(p.name for p in local.values()) + ")"

    # The yield is deliberately outside the try above: an error raised by the
    # caller's body must not be mistaken for a download failure.
    try:
        yield paths, origin
    finally:
        if tmp is not None:
            tmp.cleanup()


# Progress is reported every this many rows. The row count isn't knowable in
# advance, so the bar tracks byte position in the compressed file instead —
# which means a refresh is only worth doing a few times a second. tags.csv
# streams at north of 200k rows/s, so anything finer just burns time redrawing.
_EXPORT_PROGRESS_MASK = 0x7FFF  # every 32768 rows


@contextmanager
def _export_stream(path, desc):
    """Yield (text handle, tick callable) for a local .csv or .csv.gz export.

    Streams off disk rather than slurping — tags.csv is well over a million
    rows and we want three columns out of seven.

    The progress bar is driven by the raw file position, not a row count: how
    many rows an export holds isn't known until it's been read. Calling `tick`
    syncs the bar to wherever the underlying handle has reached. Text decoding
    buffers ahead, so the bar runs slightly optimistic — irrelevant for a
    progress indicator, and it still lands exactly on 100%.
    """
    total = path.stat().st_size
    raw = open(path, "rb")
    try:
        if path.name.endswith(".gz"):
            stream = gzip.open(
                raw, "rt", encoding="utf-8", errors="replace", newline=""
            )
        else:
            stream = io.TextIOWrapper(
                raw, encoding="utf-8", errors="replace", newline=""
            )
        with tqdm(
            total=total,
            desc=desc,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            leave=False,
            **TQDM_STEADY,
        ) as bar:

            def tick():
                bar.n = min(raw.tell(), total)
                bar.refresh()

            try:
                yield stream, tick
            finally:
                stream.close()
    finally:
        raw.close()


def _read_export_rows(path, desc):
    """Yield rows from an export as dicts, with a progress bar."""
    with _export_stream(path, desc) as (handle, tick):
        for i, row in enumerate(csv.DictReader(handle)):
            if not i & _EXPORT_PROGRESS_MASK:
                tick()
            yield row


def _resolve_alias_chains(raw_map):
    """Collapse alias chains (a->b->c) to their fixed point (a->c).

    Cycles are broken by refusing to revisit a node; self-aliases are dropped.
    """
    resolved = {}
    for start, first in raw_map.items():
        walked = {start}
        current = first
        while current in raw_map and current not in walked:
            walked.add(current)
            current = raw_map[current]
        if current != start:
            resolved[start] = current
    return resolved


def refresh_tag_graph(force=False, allow_download=True):
    """Poll the manifest and, if anything moved, re-ingest all three exports.

    Returns a stats dict, or None when there was nothing to do (checksums all
    matched) or the exports couldn't be obtained. `force` re-ingests even on
    matching checksums; `allow_download=False` skips the network entirely and
    reuses whatever dumps are sitting in ROOT.
    """
    manifest = None
    if allow_download:
        try:
            manifest = _fetch_export_manifest()
        except Exception as e:
            log.warning(f"Tag data: manifest fetch failed ({e}).")

    stored = _stored_exports()
    if manifest and not force:
        unchanged = [
            name
            for name in TAG_EXPORT_NAMES
            if stored.get(name, {}).get("checksum") == manifest[name]["checksum"]
        ]
        if len(unchanged) == len(TAG_EXPORT_NAMES):
            log.info(
                f"Tag data: all {len(unchanged)} exports unchanged "
                f"(newest stamp {manifest['tags']['updated_at']})."
            )
            return None
        log.info(
            f"Tag data: {len(TAG_EXPORT_NAMES) - len(unchanged)} of "
            f"{len(TAG_EXPORT_NAMES)} exports changed; fetching all three."
        )

    with _export_sources(manifest, allow_download) as (paths, origin):
        if paths is None:
            log.warning(
                f"Tag data: could not obtain the {', '.join(TAG_EXPORT_NAMES)} "
                f"exports, and a full set is not sitting in {ROOT}. Download "
                f"them from {DB_EXPORT_INDEX} and drop them in that folder as "
                f"a fallback (.csv or .csv.gz). Running without alias, "
                f"implication and completion support until then."
            )
            return None

        log.info(f"Tag data: ingesting from {origin}.")
        stats = _ingest_tag_data(paths)

    if manifest and origin == "download":
        _record_exports(manifest)
    return stats


def _ingest_aliases(path):
    """Active alias pairs from the export, chains collapsed to a fixed point."""
    raw = {}
    rows = 0
    for r in _read_export_rows(path, "Tag data: aliases"):
        rows += 1
        if (r.get("status") or "").strip() != "active":
            continue
        ante = (r.get("antecedent_name") or "").strip().lower()
        cons = (r.get("consequent_name") or "").strip().lower()
        if ante and cons and ante != cons:
            raw[ante] = cons
    aliases = _resolve_alias_chains(raw)
    log.info(f"Tag data: {rows} alias rows -> {len(aliases)} active, chain-resolved.")
    return aliases


def _ingest_implications(path, aliases):
    """Active direct implication edges, both ends alias-resolved.

    The export's own `descendant_names` column is deliberately ignored: it's
    the transitive closure, and we only ever want direct edges.
    """
    edges = set()
    rows = 0
    for r in _read_export_rows(path, "Tag data: implications"):
        rows += 1
        if (r.get("status") or "").strip() != "active":
            continue
        ante = (r.get("antecedent_name") or "").strip().lower()
        cons = (r.get("consequent_name") or "").strip().lower()
        if not ante or not cons:
            continue
        ante = aliases.get(ante, ante)
        cons = aliases.get(cons, cons)
        if ante != cons:
            edges.add((ante, cons))
    log.info(f"Tag data: {rows} implication rows -> {len(edges)} active edges.")
    return edges


def _ingest_tags_table(path, aliases):
    """Rebuild the chunked tag store from tags.csv.

    Kept rows are those with post_count >= TAG_MIN_POST_COUNT that aren't
    alias antecedents — a dead spelling should never be offered as a
    completion, and the alias map already maps it to the live tag when typed.

    Uses csv.reader with resolved column indices rather than DictReader: this
    file is well over a million rows and building a dict per row is the
    dominant cost.

    The export is not sorted by name and _TagStore.build needs it to be, so
    rows land in a TEMP table first and get read back ordered. That is also
    what makes build's two passes possible without holding the whole corpus in
    Python — sorting 870k tuples in memory would spike RSS by well over 100 MB
    for no benefit, since SQLite is going to spill to its own temp store
    anyway.
    """
    kept = 0
    rows = 0
    with _export_stream(path, "Tag data: tags") as (handle, tick):
        reader = csv.reader(handle)
        header = next(reader, None)
        if not header:
            raise RuntimeError("tags export is empty")
        try:
            i_name = header.index("name")
            i_cat = header.index("category")
            i_count = header.index("post_count")
        except ValueError as e:
            raise RuntimeError(f"unexpected tags export header {header}: {e}")

        def _rows():
            nonlocal kept, rows
            for row in reader:
                rows += 1
                if not rows & _EXPORT_PROGRESS_MASK:
                    tick()
                try:
                    count = int(row[i_count])
                    category = int(row[i_cat])
                except (ValueError, IndexError):
                    continue
                if count < TAG_MIN_POST_COUNT:
                    continue
                name = row[i_name].strip().lower()
                if not name or name in aliases:
                    continue
                if not 0 <= category < len(TAG_CATEGORIES):
                    continue  # a category we don't have a slot for
                kept += 1
                yield (name, category, count)

        with db() as conn:
            conn.execute(
                "CREATE TEMP TABLE tag_sort ("
                "  name TEXT PRIMARY KEY, category INTEGER, post_count INTEGER"
                ") WITHOUT ROWID"
            )
            try:
                conn.executemany(
                    "INSERT OR REPLACE INTO tag_sort (name, category, post_count) "
                    "VALUES (?, ?, ?)",
                    _rows(),
                )

                def _sorted_rows():
                    return conn.execute(
                        "SELECT name, category, post_count FROM tag_sort ORDER BY name"
                    )

                built = _TagStore.build(conn, _sorted_rows)
            finally:
                conn.execute("DROP TABLE IF EXISTS temp.tag_sort")

    _tag_store.invalidate()

    log.info(
        f"Tag data: {rows} tag rows -> {kept} kept "
        f"(post_count >= {TAG_MIN_POST_COUNT}, alias antecedents dropped); "
        f"packed into {built['chunks']} chunks, {built['raw_bytes'] / 1e6:.1f} MB "
        f"raw, {built['dict_bytes'] / 1024:.0f} KB dictionary."
    )
    return kept


def _ingest_tag_data(paths):
    """Parse and persist all three exports. Nothing is pruned to our corpus."""
    aliases = _ingest_aliases(paths["tag_aliases"])
    edges = _ingest_implications(paths["tag_implications"], aliases)
    tag_count = _ingest_tags_table(paths["tags"], aliases)

    aliases_blob = _TagGraph._pack(aliases.items())
    implications_blob = _TagGraph._pack(edges)

    now = int(time.time())
    with db() as conn:
        conn.execute(
            """INSERT INTO tag_graph
               (id, aliases_blob, implications_blob,
                alias_count, implication_count, updated_at)
               VALUES (1, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                 aliases_blob = excluded.aliases_blob,
                 implications_blob = excluded.implications_blob,
                 alias_count = excluded.alias_count,
                 implication_count = excluded.implication_count,
                 updated_at = excluded.updated_at""",
            (
                aliases_blob,
                implications_blob,
                len(aliases),
                len(edges),
                now,
            ),
        )

    _tag_graph.invalidate()

    stats = {
        "aliases": len(aliases),
        "aliases_bytes": len(aliases_blob),
        "implications": len(edges),
        "implications_bytes": len(implications_blob),
        "tags": tag_count,
    }
    log.info(
        f"Tag data: stored {stats['aliases']} aliases and "
        f"{stats['implications']} implications "
        f"({stats['aliases_bytes'] + stats['implications_bytes']} bytes of blob), "
        f"plus {stats['tags']} completable tags."
    )
    log.info(
        f"Tag data: graph resident size is roughly "
        f"{_estimate_graph_bytes(aliases, edges) / 1048576:.0f} MiB in memory."
    )

    canonicalize_additions()
    return stats


def _estimate_graph_bytes(aliases, edges):
    """Rough resident cost of holding the graph in memory.

    Sampled rather than exact: sys.getsizeof on every string would cost more
    than the number is worth. Reported at ingest so the memory tradeoff of an
    unpruned graph is a measurement rather than a guess.
    """
    strings = set(aliases)
    strings.update(aliases.values())
    for ante, cons in edges:
        strings.add(ante)
        strings.add(cons)
    if not strings:
        return 0
    sample = list(strings)[:2000]
    avg = sum(sys.getsizeof(s) for s in sample) / len(sample)
    # Strings, plus ~100 B/entry of dict and tuple overhead across both maps.
    return int(avg * len(strings) + 100 * (len(aliases) + len(edges)))


def canonicalize_additions():
    """Rewrite the additions list so aliased tags become their canonical form.

    Touches both the DB table and the two text files. No-op while the alias
    table is empty (i.e. before the first successful graph download).
    """
    from .userfiles import append_to_additions_file, remove_from_additions_file

    if _tag_graph.is_empty():
        return {"renamed": 0, "merged": 0}

    with db() as conn:
        rows = conn.execute("SELECT tag, category, added_at FROM additions").fetchall()

    renamed = 0
    merged = 0
    for row in rows:
        tag = row["tag"]
        category = row["category"]
        canon = _tag_graph.canonical(tag)
        if canon == tag:
            continue
        with db() as conn:
            exists = conn.execute(
                "SELECT 1 FROM additions WHERE tag = ? AND category = ?",
                (canon, category),
            ).fetchone()
            conn.execute(
                "DELETE FROM additions WHERE tag = ? AND category = ?",
                (tag, category),
            )
            if exists:
                merged += 1
            else:
                conn.execute(
                    "INSERT INTO additions (tag, category, added_at) VALUES (?, ?, ?)",
                    (canon, category, row["added_at"]),
                )
                renamed += 1
        remove_from_additions_file(tag, category)
        if not exists:
            append_to_additions_file(canon, category)
        log.info(f"Tag graph: addition '{tag}' -> '{canon}' ({category}).")

    if renamed or merged:
        log.info(
            f"Tag graph: canonicalized additions — {renamed} renamed, "
            f"{merged} merged into an existing entry."
        )
    return {"renamed": renamed, "merged": merged}


def _tag_graph_loop():
    """Background thread: keep the tag exports current.

    Refreshes once at startup, then sleeps until the next export is expected
    (newest ingested stamp + TAG_EXPORT_PERIOD). Once that time passes, polls
    on TAG_EXPORT_RETRY_INTERVAL until the checksums actually move, giving up
    after TAG_EXPORT_RETRY_ATTEMPTS and falling back to the normal period —
    e621's nightly job sometimes runs late, and each poll is one small JSON
    fetch, so waiting it out is cheaper than sleeping another full day.
    """
    log.info("Tag data thread started.")
    attempts_left = TAG_EXPORT_RETRY_ATTEMPTS
    while True:
        log.info("Tag data: activating.")
        try:
            changed = refresh_tag_graph() is not None
        except Exception as e:
            log.error(f"Tag data: refresh failed: {e}")
            changed = False

        # Whether or not the export moved, diff the exhausted set against it:
        # on a fresh ingest this is the whole point, and on a restart it picks
        # up any baseline cleared by a blacklist edit.
        try:
            from .scanner import run_export_diff

            run_export_diff()
        except Exception as e:
            log.error(f"Export diff: unhandled error: {e}")

        if changed:
            # A fresh ingest replaced the whole tags table and both graph
            # blobs. Reclaim before going back to sleep.
            run_vacuum()

        due_in = _seconds_until_next_export()
        if changed or due_in > 0:
            # Either we just ingested, or the next export isn't due yet (the
            # normal case on a restart). Reset the ladder and wait it out.
            attempts_left = TAG_EXPORT_RETRY_ATTEMPTS
            delay = due_in or TAG_EXPORT_PERIOD
        elif attempts_left > 0:
            attempts_left -= 1
            delay = TAG_EXPORT_RETRY_INTERVAL
            log.info(
                f"Tag data: export overdue, retrying in "
                f"{delay // 60} min ({attempts_left} attempts left)."
            )
        else:
            attempts_left = TAG_EXPORT_RETRY_ATTEMPTS
            log.warning("Tag data: retry ladder exhausted; backing off a full period.")
            delay = TAG_EXPORT_PERIOD

        _log_next("Tag data", delay)
        time.sleep(delay)


def start_tag_graph_sync():
    t = threading.Thread(target=_tag_graph_loop, daemon=True, name="tag-data")
    t.start()
    log.info("Tag data thread launched.")


# ---------- Tag query matcher ----------
