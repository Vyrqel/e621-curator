"""
E621 Curator — backlog management tool.

Reads queries from queries.txt (one e621 search URL per line).
Already-tracked tags are inferred from queries.txt + the additions files.
Serves a minimal review interface: random image from random query, mark seen,
favorite, or extract artist/character additions.
"""

import argparse
import bisect
import collections
import csv
import gzip
import hashlib
import io
import logging
import random
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import zstandard as zstd
from flask import Flask, jsonify, render_template, request
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

# ---------- Configuration ----------

ROOT = Path(__file__).parent
DB_PATH = ROOT / "curator.db"
CREDENTIALS_FILE = ROOT / "credentials.txt"
QUERIES_FILE = ROOT / "queries.txt"
BLACKLIST_FILE = ROOT / "blacklist.txt"
ADDITIONS_ARTISTS_FILE = ROOT / "additions_artists.txt"
ADDITIONS_CHARACTERS_FILE = ROOT / "additions_characters.txt"

# IMPORTANT: e621 requires a descriptive User-Agent with contact info.
USER_AGENT = "e621-curator-beta/1.0 (by Vyrqel on e621)"

# Post endpoints are called with v2=true&mode=extended unconditionally and
# post objects are consumed in v2 shape (see the accessors near _v2_params).
# e621's v2 format became default in Dec 2026; legacy is removed May 2027 —
# see forum topic 63849. There is no legacy fallback path.

# e621 rate limit is 2 req/sec. We aim for 1 req/sec to stay safe.
MIN_REQUEST_INTERVAL = 1.0
_last_request_time = 0.0
_server_state = {"status": "initializing"}  # "initializing" | "scanning" | "live"

POSTS_PER_PAGE = 320  # e621 max
MAX_PRIMED_ATTEMPTS = 8
RESCAN_BATCH_TAGS = (
    20  # OR-combine up to N exhausted (artist/character) queries into one gate request
)

# Tag-cache backfill: fetch tags for locally-referenced posts (seen/favorites)
# that have no entry in post_tags. Missing entries mostly come from posts seen
# before the tag cache existed, and from favorites synced off e621 (which only
# carry post_id + favorited_at). New browsing self-caches via
# cache_post_from_response, so this only ever has real work on the historical
# backlog and on freshly-synced favorites. Posts confirmed gone from e621 are
# purged from the db (see backfill_tag_cache). It runs as part of the
# event-driven maintenance chain (startup / post-rescan), not on a timer —
# idle passes are ~free anyway (one COUNT query, zero API calls).

# Tag data exports. e621 publishes a nightly CSV dump of its tags,
# tag_aliases and tag_implications tables, and a manifest listing every export
# with a SHA-256 checksum and the time it was generated. We poll the manifest
# (a couple of KB) and only pull a CSV when its checksum differs from the one
# we last ingested, so a no-op refresh costs one small JSON fetch instead of
# ~40 MB of gzip. The checksum doubles as download verification.
DB_EXPORT_MANIFEST = "https://e621.net/db_exports.json"
DB_EXPORT_INDEX = "https://static1.e621.net/data/db_export/"  # manual fallback
TAG_EXPORT_NAMES = ("tags", "tag_aliases", "tag_implications")
TAG_GRAPH_TIMEOUT = 900  # per-file download timeout; tags.csv.gz is ~34 MB

# Exports are stamped in the small hours and all three land together, so we
# wake 24h05m after the newest stamp we've ingested. If e621's job runs late
# the checksums still match and refresh_tag_graph returns None, so we walk a
# short retry ladder rather than sleeping another full day.
TAG_EXPORT_PERIOD = 86400 + 300  # 24h05m after the newest export stamp
TAG_EXPORT_RETRY_INTERVAL = 1800  # 30 min between retries once we're overdue
TAG_EXPORT_RETRY_ATTEMPTS = 8  # ~4h of ladder before falling back to the period

# Every connection waits this long for a lock before giving up. The default
# is 5s, which is nowhere near long enough to sit out a VACUUM — see
# run_vacuum(). This is a ceiling on how long a request can stall, not a
# target; normal contention resolves in milliseconds.
DB_BUSY_TIMEOUT = 300.0

# Tags below this post count are dropped at ingest: a tag with no posts can
# never match anything, and e621 carries a very large tail of them.
TAG_MIN_POST_COUNT = 1

# ---------- Tag codec ----------
# Compact binary encoding for e621 tag dicts.
#
# Raw payload format (v3, produced by _encode_raw_v3 / consumed by
# _decode_raw_v3). The raw payload is never stored on its own — only the
# wrapper's marker byte is written to disk. Note this is the codec's own
# versioning and has nothing to do with the e621 v2 response format:
#   1 byte : flags
#              bits 0-1 : rating — 00=s, 01=q, 10=e, 11=unknown/None.
#                         e621 has exactly three ratings, so 11 is free and
#                         doubles as the "not yet known" sentinel.
#              bits 2-7 : reserved, MUST be written as 0 and ignored on read
#                         (append-only room for future per-post flags).
#   For each of the 9 categories in TAG_CATEGORIES order:
#     varint (LEB128) : tag count for this category
#     N null-terminated UTF-8 strings: the tags
#
# Stored blob wrapper format (v3, marker 0x03):
#   1 byte  : format marker (0x03)
#   1 byte  : dict flag — 1 = compressed against the current dict ('dict'
#             label), 0 = stranded on the previous dict ('dict_old' label).
#   N bytes : zstd frame of the raw payload
#
# All blobs in the database are confirmed v3; the legacy v2 wrapper (2-byte
# incrementing dictionary version) and its migration path have been removed
# entirely.
#
# There are only ever at most two dictionaries in tag_dicts at once: the one
# labeled 'dict' (current) and the one labeled 'dict_old' (previous). A
# retrain decodes and re-encodes every blob against the new dict, flagging
# it 1; a blob that fails to decode is left untouched and stays flagged 0,
# which is what keeps 'dict_old' alive — it is dropped once no blob
# references it. There is no dictionary version counter to maintain.
#
# Category membership is stored BY INDEX, not by name. NEVER reorder
# TAG_CATEGORIES — it would silently corrupt every existing blob. Any future
# change must be append-only.
#
# Second reason the order is load-bearing: it matches e621's own category
# numbering, so the integer `category` column in tags.csv indexes straight
# into this tuple with no mapping table (dragon=5=species, cbee=1=artist).
# The tags table stores that integer as-is.

TAG_CATEGORIES = (
    "general",
    "artist",
    "contributor",
    "copyright",
    "character",
    "species",
    "invalid",
    "meta",
    "lore",
)

ZSTD_LEVEL = 19
DICT_SIZE = (
    248 * 1024
)  # trained dictionary target size; consider zipfian calculation in the future
DICT_MIN_SAMPLES = 64  # below this, training is pointless — go dictionary-less
DICT_TRAIN_SAMPLES = 0  # 0 = train on the whole corpus; set via --dict-samples

_BLOB_FMT_V3 = 0x03  # current blob wrapper: marker + 1-byte dict flag + frame

# 2-bit rating codes. NEVER renumber — these are baked into stored blobs.
_RATING_BITS = {"s": 0b00, "q": 0b01, "e": 0b10}
_BITS_RATING = {0b00: "s", 0b01: "q", 0b10: "e", 0b11: None}

_RATING_MASK = 0b11


def _put_varint(n):
    """LEB128-encode a non-negative int."""
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _get_varint(raw, pos):
    """Decode a LEB128 int at `pos`. Returns (value, new_pos)."""
    n = 0
    shift = 0
    while True:
        b = raw[pos]
        pos += 1
        n |= (b & 0x7F) << shift
        if not b & 0x80:
            return n, pos
        shift += 7


def _encode_raw_v3(tags_dict, rating):
    """Serialize tag dict + rating to the raw v3 (uncompressed) payload."""
    flags = _RATING_BITS.get((rating or "")[:1], 0b11)
    parts = [bytes([flags])]
    for cat in TAG_CATEGORIES:
        tags = tags_dict.get(cat) or []
        parts.append(_put_varint(len(tags)))
        for tag in tags:
            parts.append(tag.encode("utf-8") + b"\x00")
    return b"".join(parts)


def _decode_raw_v3(raw):
    """Parse a raw v3 payload back to (tags_dict, rating)."""
    rating = _BITS_RATING[raw[0] & _RATING_MASK]
    pos = 1
    tags_dict = {}
    for cat in TAG_CATEGORIES:
        count, pos = _get_varint(raw, pos)
        tags = []
        for _ in range(count):
            end = raw.index(b"\x00", pos)
            tags.append(raw[pos:end].decode("utf-8"))
            pos = end + 1
        tags_dict[cat] = tags
    return tags_dict, rating


class _TagDictManager:
    """Owns at most two compression dictionaries: 'dict' (current) and
    'dict_old' (previous). The lock is held across retrains so a concurrent
    cache write can't grab 'dict_old' the instant it's being dropped.
    """

    def __init__(self):
        self.lock = threading.RLock()
        self._current = None  # ZstdCompressionDict | None
        self._old = None  # ZstdCompressionDict | None
        self._loaded = False

    def _load_locked(self):
        with db() as conn:
            rows = conn.execute(
                "SELECT label, dict_blob FROM tag_dicts WHERE label IS NOT NULL"
            ).fetchall()
        by_label = {
            r["label"]: zstd.ZstdCompressionDict(bytes(r["dict_blob"])) for r in rows
        }
        self._current = by_label.get("dict")
        self._old = by_label.get("dict_old")
        self._loaded = True

    def _ensure_loaded_locked(self):
        if not self._loaded:
            self._load_locked()

    def current(self):
        """Return the current ZstdCompressionDict (None if none trained yet)."""
        with self.lock:
            self._ensure_loaded_locked()
            return self._current

    def for_flag(self, flag):
        """Return the ZstdCompressionDict for a blob's dict flag (1=current,
        0=old)."""
        with self.lock:
            self._ensure_loaded_locked()
            d = self._current if flag else self._old
            if d is None and (self._current is not None or self._old is not None):
                # Not in cache — maybe committed by another process. Reload.
                self._load_locked()
                d = self._current if flag else self._old
            return d


_tag_dicts = _TagDictManager()


def _compress_payload(raw):
    """Compress a raw payload with the current dictionary. Returns the full
    stored-blob bytes (marker + dict flag + frame)."""
    cdict = _tag_dicts.current()
    cctx = zstd.ZstdCompressor(level=ZSTD_LEVEL, dict_data=cdict)
    return bytes([_BLOB_FMT_V3, 1]) + cctx.compress(raw)


def _decompress_blob(blob):
    """Return the raw payload for a stored blob."""
    blob_fmt = blob[0]
    if blob_fmt != _BLOB_FMT_V3:
        raise ValueError(f"unknown tag blob format byte {blob_fmt:#04x}")
    flag = blob[1]
    dctx = zstd.ZstdDecompressor(dict_data=_tag_dicts.for_flag(flag))
    return dctx.decompress(blob[2:])


def encode_tags(tags_dict, rating):
    """Encode e621 tag dict + rating to compact compressed binary blob."""
    return _compress_payload(_encode_raw_v3(tags_dict, rating))


def decode_tags(blob):
    """Decode a stored blob (zstd + dict) to (tags_dict, rating)."""
    return _decode_raw_v3(_decompress_blob(bytes(blob)))


# ---------- Credentials ----------


def _load_credentials():
    if not CREDENTIALS_FILE.exists():
        return "", ""
    creds = {}
    for line in CREDENTIALS_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            k, v = line.split("=", 1)
            creds[k.strip()] = v.strip()
    return creds.get("E621_USERNAME", ""), creds.get("E621_API_KEY", "")


E621_USERNAME, E621_API_KEY = _load_credentials()

# ---------- App ----------


class NoRequestFilter(logging.Filter):
    def filter(self, record):
        # Return False if the log message contains typical request patterns
        return not any(
            x in record.getMessage() for x in ["GET /", "POST /", "PUT /", "DELETE /"]
        )


class TqdmLoggingHandler(logging.StreamHandler):
    """Emit log records through `tqdm.write` so they don't shred a live bar.

    Matters now that a tag refresh can resume on a background thread while the
    server is up: the scanner, favourites sync and werkzeug all log to stderr
    while the refresh bar is drawing there. `tqdm.write` clears the bar, writes
    the line, and redraws it, instead of writing over the top of it.
    """

    def emit(self, record):
        try:
            tqdm.write(self.format(record), file=sys.stderr)
        except Exception:
            self.handleError(record)


# Apply the filter specifically to the Werkzeug logger
werkzeug_logger = logging.getLogger("werkzeug")
werkzeug_logger.addFilter(NoRequestFilter())


app = Flask(__name__)
log = logging.getLogger("Curator")


def _log_next(name, delay):
    """Log when a background thread will next wake up."""
    when = datetime.now().astimezone() + timedelta(seconds=delay)
    log.info(
        f"{name}: next activation at {when:%Y-%m-%d %H:%M:%S %Z} "
        f"(in {delay / 60:.1f} min)."
    )


# ---------- Database ----------

SCHEMA = """
CREATE TABLE IF NOT EXISTS seen (
    post_id INTEGER PRIMARY KEY,
    seen_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS favorites (
    post_id INTEGER PRIMARY KEY,
    favorited_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS additions (
    tag TEXT PRIMARY KEY,
    category TEXT NOT NULL,  -- 'artist' or 'character'
    added_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS query_progress (
    query_hash TEXT PRIMARY KEY,  -- sha256 of the tag string
    query_tags TEXT NOT NULL,     -- human-readable copy for debugging
    last_page INTEGER NOT NULL DEFAULT 1,
    exhausted INTEGER NOT NULL DEFAULT 0,  -- 1 = walked off end of results
    exhausted_at INTEGER,                  -- unix timestamp when exhaustion detected
    new_posts_found INTEGER NOT NULL DEFAULT 0,  -- primed post count from last bg scan
    last_scanned_at INTEGER,               -- when background scanner last checked this
    page1_empty_at INTEGER,                -- unix timestamp page 1 was last confirmed fully seen
    post_count_at_exhaustion INTEGER,      -- tags.post_count when exhaustion was confirmed
    updated_at INTEGER NOT NULL
);

-- Single-row table: the resume point of an in-progress tag refresh sweep.
-- Written after every completed batch and DELETED when the sweep finishes, so
-- the row existing at startup means the last sweep was interrupted.
--
-- The cursor is a post ID, not a batch index: the ID list is rebuilt from
-- seen/favorites (ORDER BY post_id) on resume, so rows added or removed in
-- the meantime shift indices but never shift the ID we stopped before.
CREATE TABLE IF NOT EXISTS refresh_progress (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    next_id INTEGER NOT NULL,      -- first post ID not yet processed
    total INTEGER NOT NULL,        -- ID count at the time the sweep started
    done INTEGER NOT NULL,         -- posts processed across all runs of it
    cached INTEGER NOT NULL,       -- running totals, carried across resumes
    purged INTEGER NOT NULL,
    started_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS post_tags (
    post_id INTEGER PRIMARY KEY,
    tags_blob BLOB NOT NULL,       -- compressed binary encoding (see encode_tags/decode_tags)
    cached_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_post_tags_cached_at ON post_tags(cached_at);

-- Post relationships: parent/child links and pool membership, kept apart
-- from post_tags because they answer a different question and change on a
-- different schedule. One row per post; absence of a row means "never
-- looked", while a row with parent_id NULL, no children and no pools means
-- "looked, and the post is standalone".
--
-- children/pools are stored as comma-separated ID lists rather than join
-- tables: e621 hands them over as whole lists, we only ever read them as
-- whole lists, and the sets are small (a handful of children, rarely more
-- than one pool). parent_id gets its own column and index because that IS
-- queried across posts -- it's how a family is reassembled locally.
CREATE TABLE IF NOT EXISTS post_relations (
    post_id INTEGER PRIMARY KEY,
    parent_id INTEGER,             -- NULL when the post has no parent
    children TEXT NOT NULL DEFAULT '',  -- comma-separated post IDs, ascending
    pools TEXT NOT NULL DEFAULT '',     -- comma-separated pool IDs, ascending
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_post_relations_parent ON post_relations(parent_id);

CREATE TABLE IF NOT EXISTS tag_dicts (
    -- 'dict' = current (blobs flagged 1 reference this), 'dict_old' =
    -- previous (blobs flagged 0, stranded because they failed to rewrite
    -- during the last retrain, reference this). At most two rows ever exist.
    label TEXT PRIMARY KEY CHECK (label IN ('dict', 'dict_old')),
    dict_blob BLOB NOT NULL,       -- raw zstd dictionary bytes
    trained_on INTEGER NOT NULL,   -- number of samples used for training
    created_at INTEGER NOT NULL
);

-- Single-row table: the alias + implication graph, zstd-compressed.
-- Stored as blobs rather than relational rows because it's only ever read as
-- a whole (one decompress, held in memory) and never queried by SQL.
CREATE TABLE IF NOT EXISTS tag_graph (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    -- Both blobs are zstd of tab-separated antecedent/consequent pairs, one
    -- per line. NOTE: SCHEMA is a plain (non-raw) Python string, so never
    -- write a backslash escape in these comments -- it becomes a real tab or
    -- newline and truncates the comment mid-statement.
    aliases_blob BLOB NOT NULL,
    implications_blob BLOB NOT NULL,   -- direct edges only, not the closure
    alias_count INTEGER NOT NULL,
    implication_count INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

-- What we last ingested, one row per tracked export. The checksum is the
-- upstream SHA-256 straight out of db_exports.json: matching it means there
-- is nothing to download. updated_at is the upstream ISO stamp and drives
-- the poll schedule.
CREATE TABLE IF NOT EXISTS db_exports (
    name TEXT PRIMARY KEY,
    checksum TEXT NOT NULL,
    file_size INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    ingested_at INTEGER NOT NULL
);

-- Every live e621 tag, for override-bar completion. Formerly a relational
-- WITHOUT ROWID table with a post_count index; that pair cost ~37 MB, which
-- was most of the database. Now stored as name-ordered chunks of front-coded
-- rows, zstd-compressed against one shared trained dictionary and unpacked on
-- demand -- see _TagStore for the payload layout.
--
-- The access pattern is what makes this work: completion only ever asks for a
-- name prefix or an exact name, both of which resolve to one or two chunks via
-- the in-memory sparse index. Nothing needs the whole table at once, and
-- nothing needs post_count order any more (the substring completion tier that
-- did was removed along with its index).
CREATE TABLE IF NOT EXISTS tag_chunks (
    id INTEGER PRIMARY KEY,        -- chunk ordinal, ascending by name
    first_name TEXT NOT NULL,      -- lowest name in the chunk; the bisect key
    last_name TEXT NOT NULL,       -- highest name, so a prefix scan knows to stop
    max_post_count INTEGER NOT NULL,  -- ranking bound; see _TagStore.prefix
    tag_count INTEGER NOT NULL,
    blob BLOB NOT NULL             -- zstd(shared dict) of the front-coded payload
);

-- Single row: the dictionary the chunks were compressed against, plus totals.
-- Separate from tag_dicts, which belongs to the post_tags codec and retrains on
-- a completely different schedule. Rewritten wholesale on every tag ingest, so
-- there is no old-dictionary rotation to manage here.
CREATE TABLE IF NOT EXISTS tag_store (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    dict_blob BLOB,                -- NULL when the corpus was too small to train
    chunk_count INTEGER NOT NULL,
    tag_count INTEGER NOT NULL,
    raw_bytes INTEGER NOT NULL,    -- uncompressed payload total, for the ratio
    updated_at INTEGER NOT NULL
);

-- Small key/value slots for cross-run state that doesn't deserve a table.
CREATE TABLE IF NOT EXISTS app_meta (
    key TEXT PRIMARY KEY,
    value TEXT
) WITHOUT ROWID;
"""

# Columns added after the original schema shipped. Applied by _migrate_schema()
# against existing databases; SCHEMA already contains them for fresh ones.
_MIGRATIONS = (("query_progress", "post_count_at_exhaustion", "INTEGER"),)

# Every column tag_chunks is expected to have. A DB whose tag_chunks predates
# a change to this layout is dropped and rebuilt rather than migrated — it is
# derived data, so there is nothing in it worth preserving.
_TAG_CHUNK_COLUMNS = {
    "id",
    "first_name",
    "last_name",
    "max_post_count",
    "tag_count",
    "blob",
}


def _migrate_schema(conn):
    """Idempotently add any post-hoc columns missing from an existing DB."""
    for table, column, decl in _MIGRATIONS:
        cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
        if column not in cols:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")
            log.info(f"Schema: added {table}.{column}.")

    cols = {r["name"] for r in conn.execute("PRAGMA table_info(tag_chunks)")}
    if cols and cols != _TAG_CHUNK_COLUMNS:
        conn.execute("DROP TABLE tag_chunks")
        conn.execute("DROP TABLE IF EXISTS tag_store")
        conn.executescript(SCHEMA)
        log.info("Schema: tag_chunks layout changed; store dropped for rebuild.")


def get_meta(key, default=None):
    with db() as conn:
        row = conn.execute(
            "SELECT value FROM app_meta WHERE key = ?", (key,)
        ).fetchone()
    return row["value"] if row else default


def set_meta(key, value):
    with db() as conn:
        conn.execute(
            "INSERT INTO app_meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, str(value)),
        )


@contextmanager
def db():
    conn = sqlite3.connect(DB_PATH, timeout=DB_BUSY_TIMEOUT)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


_vacuum_lock = threading.Lock()


def run_vacuum():
    """Reclaim free pages left behind by bulk deletes and rewrites.

    Safe to call from a background thread. VACUUM holds an exclusive lock for
    as long as it takes to rewrite the file, which is far longer than
    sqlite3's 5s default busy timeout — with that default, a concurrent
    scanner or favorites write raises "database is locked" instead of
    waiting. DB_BUSY_TIMEOUT is what makes those threads block and resume
    rather than fail, so it must stay comfortably above the vacuum duration;
    the elapsed time is logged on every run so drift is visible.

    Runs after the operations that actually churn pages — a full tag data
    ingest rewrites the tags table wholesale and a dict retrain rewrites every
    blob in post_tags, both of which leave a lot of free space behind.

    The lock is advisory and non-blocking: if a vacuum is already running,
    a second caller skips rather than queueing up behind it.

    Must run outside any transaction — use a dedicated autocommit connection.
    """
    if not _vacuum_lock.acquire(blocking=False):
        log.info("VACUUM already in progress; skipping this request.")
        return False

    started = time.time()
    try:
        before = DB_PATH.stat().st_size
        log.info(f"Running VACUUM on {before / 1048576:.1f} MiB...")
        vac_conn = sqlite3.connect(
            DB_PATH, timeout=DB_BUSY_TIMEOUT, isolation_level=None
        )
        try:
            vac_conn.execute("VACUUM")
        finally:
            vac_conn.close()
        elapsed = time.time() - started
        after = DB_PATH.stat().st_size
        log.info(
            f"VACUUM complete in {elapsed:.1f}s — "
            f"{before / 1048576:.1f} -> {after / 1048576:.1f} MiB "
            f"({(before - after) / 1048576:.1f} MiB reclaimed)."
        )
        if elapsed > DB_BUSY_TIMEOUT / 2:
            log.warning(
                f"VACUUM took {elapsed:.0f}s, over half of DB_BUSY_TIMEOUT "
                f"({DB_BUSY_TIMEOUT:.0f}s). Raise the timeout before it starts "
                f"costing other threads lock errors."
            )
        return True
    finally:
        _vacuum_lock.release()


def init_db():
    with db() as conn:
        conn.executescript(SCHEMA)
        _migrate_schema(conn)

    purge_damaging_query_data()


# ---------- File loaders ----------


def load_queries():
    """Parse queries.txt — extract the `tags` parameter from each e621 URL.

    Lines that aren't URLs are treated as raw tag strings.
    Lines starting with # are comments.
    """
    if not QUERIES_FILE.exists():
        return []
    queries = []
    for line in QUERIES_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("http"):
            parsed = urllib.parse.urlparse(line)
            params = urllib.parse.parse_qs(parsed.query)
            tags = params.get("tags", [None])[0]
            if tags:
                queries.append(tags.lower())
        else:
            queries.append(line.lower())
    return queries


def load_blacklist():
    """Load blacklist lines, parsed into AND-clauses of (tag, negated) tuples.

    Mirrors e621 semantics: each line is a space-separated list of tags that
    must ALL match for the line to apply. `-tag` means the post must NOT have
    that tag. `*` is a wildcard.

    Returns a list of clauses, each clause is a list of (pattern, negated).
    A post is blacklisted if it matches any clause.
    """
    if not BLACKLIST_FILE.exists():
        return []
    clauses = []
    for line in BLACKLIST_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        terms = []
        for token in line.split():
            token = token.lower()
            negated = token.startswith("-")
            if negated:
                token = token[1:]
            if token:
                terms.append((token, negated))
        if terms:
            clauses.append(terms)
    return clauses


def load_known_tags():
    """Build the set of tags considered 'already known'.

    A tag is known if it appears as a bare positive tag in queries.txt OR
    is in either additions file. Used to dim/checkmark tag chips in the UI.

    Skips negated tags (-tag), metatags (rating:s, score:>100, order:score),
    and other operators since those aren't artist/character names.

    Returns both the spelling as written and its alias-canonical form, so a
    query written before a tag was renamed still marks the renamed tag as
    known.
    """
    known = set()

    # From queries — extract bare tags only
    for query in load_queries():
        for token in query.split():
            token = token.lower().strip()
            if not token:
                continue
            if token.startswith("-"):
                continue  # negation
            if ":" in token:
                continue  # metatag like rating:s, order:score, score:>100
            if "*" in token:
                continue  # wildcard searches aren't specific tags
            known.add(token)

    # From additions files
    known |= read_additions_file("artist")
    known |= read_additions_file("character")

    return known | {_tag_graph.canonical(t) for t in known}


def _additions_file(category):
    """Return the path of the additions file for a category."""
    if category == "artist":
        return ADDITIONS_ARTISTS_FILE
    elif category == "character":
        return ADDITIONS_CHARACTERS_FILE
    return None


def read_additions_file(category):
    """Return the set of tags currently in the additions file for a category."""
    path = _additions_file(category)
    if not path or not path.exists():
        return set()
    return {
        line.strip().lower()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    }


def append_to_additions_file(tag, category):
    """Append a tag to the appropriate additions file (skips if already present)."""
    path = _additions_file(category)
    if path is None:
        return
    existing = read_additions_file(category)
    if tag.lower() in existing:
        return
    # Create with header if missing, then append
    if not path.exists():
        header = (
            f"# additions_{category}s.txt\n"
            f"# {category}s flagged from tag clicks during curation.\n"
            f"# Move entries to queries.txt as new search queries when ready.\n"
            f"# One tag per line. Lines starting with # are comments.\n\n"
        )
        path.write_text(header, encoding="utf-8")
    with path.open("a", encoding="utf-8") as f:
        f.write(f"{tag}\n")


def remove_from_additions_file(tag, category):
    """Remove a tag from the appropriate additions file. No-op if absent."""
    path = _additions_file(category)
    if path is None or not path.exists():
        return
    target = tag.lower()
    kept_lines = []
    for line in path.read_text(encoding="utf-8").splitlines():
        # Preserve comments, blank lines, and any tag that doesn't match
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or stripped.lower() != target:
            kept_lines.append(line)
    path.write_text("\n".join(kept_lines) + "\n", encoding="utf-8")


def reconcile_additions_files():
    """On startup, ensure every DB addition exists in its corresponding text file.

    Catches drift if files were edited externally or deleted while DB has rows.
    """
    with db() as conn:
        rows = conn.execute(
            "SELECT tag, category FROM additions ORDER BY added_at ASC"
        ).fetchall()

    missing_artists = 0
    missing_characters = 0
    artists_in_file = read_additions_file("artist")
    characters_in_file = read_additions_file("character")

    for row in rows:
        tag = row["tag"]
        category = row["category"]
        if category == "artist" and tag not in artists_in_file:
            append_to_additions_file(tag, "artist")
            artists_in_file.add(tag)
            missing_artists += 1
        elif category == "character" and tag not in characters_in_file:
            append_to_additions_file(tag, "character")
            characters_in_file.add(tag)
            missing_characters += 1

    if missing_artists or missing_characters:
        log.info(
            f"Reconciled additions files: +{missing_artists} artists, "
            f"+{missing_characters} characters"
        )


def _tag_matches(pattern, post_tags):
    """Check if any tag in post_tags matches the pattern. Supports `*` wildcard."""
    if "*" not in pattern:
        return pattern in post_tags
    # Convert glob to regex-ish: simple prefix/suffix/middle match
    import fnmatch

    return any(fnmatch.fnmatchcase(t, pattern) for t in post_tags)


def post_tag_set(post):
    """Flatten every category of a post's tags into one lowercase set."""
    tags_dict = post.get("tags", {})
    all_tags = set()
    for category in TAG_CATEGORIES:
        for t in tags_dict.get(category, []):
            all_tags.add(t.lower())
    return all_tags


def is_blacklisted(post, clauses):
    """Return True if post matches any blacklist clause."""
    if not clauses:
        return False
    # Flatten all post tags into one lowercase set
    all_tags = post_tag_set(post)
    # Also add rating as a pseudo-tag (e621 does this: rating:s, rating:q, rating:e)
    rating = post.get("rating")
    if rating:
        all_tags.add(f"rating:{rating}")

    for clause in clauses:
        # All terms in this clause must be satisfied
        if all(
            (_tag_matches(pattern, all_tags) != negated) for pattern, negated in clause
        ):
            return True
    return False


# ---------- e621 API ----------

# Shared session with automatic retries for transient SSL/connection errors.
# e621 sits behind Cloudflare which occasionally terminates handshakes.
_session = requests.Session()
_retry = Retry(
    total=4,
    backoff_factor=1.5,  # 0, 1.5, 3, 6 second waits
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
    raise_on_status=False,
)
_adapter = HTTPAdapter(max_retries=_retry)
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)


def rate_limit():
    global _last_request_time
    elapsed = time.time() - _last_request_time
    if elapsed < MIN_REQUEST_INTERVAL:
        time.sleep(MIN_REQUEST_INTERVAL - elapsed)
    _last_request_time = time.time()


def _auth():
    """Return requests auth tuple if credentials are configured, else None."""
    if E621_USERNAME and E621_API_KEY:
        return (E621_USERNAME, E621_API_KEY)
    return None


def _v2_params(params):
    """Add the v2 opt-in parameters to a post-endpoint request.

    mode=extended is REQUIRED: the v2 default (mode=basic) returns tags as a
    flat array with no categories, which would silently break the tag codec.
    """
    params = dict(params)
    params["v2"] = "true"
    params["mode"] = "extended"
    return params


# ---------- v2 post accessors ----------
#
# Post objects are consumed in v2 shape directly; there is no normalization
# layer. v2 groups file data under files.{meta,original,preview,sample} and
# counters under stats.*, and leaves tags (in mode=extended), rating, flags
# and relationships at the top level. These accessors exist only so the
# nesting is spelled out in one place.


def post_file(post):
    """files.original + files.meta — the original upload's url and metadata."""
    files = post.get("files") or {}
    original = files.get("original") or {}
    meta = files.get("meta") or {}
    return {
        "url": original.get("url"),
        "width": original.get("width"),
        "height": original.get("height"),
        "ext": meta.get("ext"),
        "size": meta.get("size"),
        "md5": meta.get("md5"),
    }


def post_sample_url(post):
    """Preferred display URL: the resized sample jpg, else the original."""
    files = post.get("files") or {}
    sample = files.get("sample") or {}
    original = files.get("original") or {}
    return sample.get("jpg") or original.get("url")


def post_is_viewable(post):
    """Whether the post has any renderable image URL at all."""
    return bool(post_sample_url(post))


def post_score(post):
    """stats.score.total — net score."""
    stats = post.get("stats") or {}
    return (stats.get("score") or {}).get("total", 0)


def post_is_favorited(post):
    """stats.is_favorited — e621's own favorite flag for the auth'd user."""
    return (post.get("stats") or {}).get("is_favorited", False)


def _extract_posts(payload):
    """Get the post list out of a /posts.json response.

    v2 returns a bare array; the object form is tolerated defensively.
    """
    if isinstance(payload, list):
        return payload
    return payload.get("posts") or []


def fetch_posts(tags, page=1):
    """Fetch a page of posts from e621 for the given tag string."""
    rate_limit()
    response = _session.get(
        "https://e621.net/posts.json",
        params=_v2_params({"tags": tags, "limit": POSTS_PER_PAGE, "page": page}),
        headers={"User-Agent": USER_AGENT},
        auth=_auth(),
        timeout=15,
    )
    response.raise_for_status()
    return _extract_posts(response.json())


def e621_favorite(post_id):
    """Create a favorite on e621 for the given post ID. Raises on failure."""
    rate_limit()
    response = _session.post(
        "https://e621.net/favorites.json",
        params={"post_id": post_id},
        headers={"User-Agent": USER_AGENT},
        auth=_auth(),
        timeout=15,
    )
    # 422 means already favorited — treat as success
    if response.status_code == 422:
        return
    response.raise_for_status()


def e621_unfavorite(post_id):
    """Remove a favorite from e621 for the given post ID. Raises on failure."""
    rate_limit()
    response = _session.delete(
        f"https://e621.net/favorites/{post_id}.json",
        headers={"User-Agent": USER_AGENT},
        auth=_auth(),
        timeout=15,
    )
    # 404 means not favorited — treat as success
    if response.status_code == 404:
        return
    response.raise_for_status()


def get_seen_ids():
    with db() as conn:
        return {row["post_id"] for row in conn.execute("SELECT post_id FROM seen")}


def query_hash(tags):
    return hashlib.sha256(tags.lower().encode()).hexdigest()


def get_last_page(tags):
    """Return the last page we successfully served from for this query (default 1)."""
    with db() as conn:
        row = conn.execute(
            "SELECT last_page FROM query_progress WHERE query_hash = ?",
            (query_hash(tags),),
        ).fetchone()
    return row["last_page"] if row else 1


def set_last_page(tags, page):
    with db() as conn:
        conn.execute(
            """INSERT INTO query_progress (query_hash, query_tags, last_page, updated_at)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(query_hash) DO UPDATE SET
                 last_page = excluded.last_page,
                 updated_at = excluded.updated_at""",
            (query_hash(tags), tags, page, int(time.time())),
        )


def set_page1_empty(tags, ts):
    """Record that page 1 of this query was confirmed fully seen at timestamp ts.

    Pass None to clear the cache (e.g. when fresh content was just found).
    """
    now = int(time.time())
    with db() as conn:
        conn.execute(
            """INSERT INTO query_progress
               (query_hash, query_tags, last_page, page1_empty_at, updated_at)
               VALUES (?, ?, 1, ?, ?)
               ON CONFLICT(query_hash) DO UPDATE SET
                 page1_empty_at = excluded.page1_empty_at,
                 updated_at = excluded.updated_at""",
            (query_hash(tags), tags, ts, now),
        )
    if ts is not None:
        # Page 1 just came back fully seen, so the current export count is a
        # valid baseline for the next diff.
        snapshot_post_count(tags)


def lookup_post_count(tag):
    """Current export post_count for a single tag, alias-resolved.

    Returns None when the tag has no row — which means either it was emptied
    to zero posts (the export drops anything under TAG_MIN_POST_COUNT) or it's
    a spelling the alias map can't resolve. Callers distinguish the two.
    """
    if not tag:
        return None
    name = _tag_graph.canonical(tag.lower())
    hit = _tag_store.get(name)
    return hit[1] if hit else None


def snapshot_post_count(tags):
    """Record the tag's current export post_count as the exhaustion baseline.

    This is the number the next export gets diffed against: if it hasn't moved,
    the tag provably gained no posts and needs no request at all.
    """
    count = lookup_post_count(tags)
    if count is None:
        return
    with db() as conn:
        conn.execute(
            "UPDATE query_progress SET post_count_at_exhaustion = ? "
            "WHERE query_hash = ?",
            (count, query_hash(tags)),
        )


def clear_all_post_count_snapshots():
    """Drop every baseline, forcing the next diff pass to scan everything."""
    with db() as conn:
        cur = conn.execute(
            "UPDATE query_progress SET post_count_at_exhaustion = NULL "
            "WHERE post_count_at_exhaustion IS NOT NULL"
        )
        return cur.rowcount


def is_exhaustible(tags):
    """Whether a query is allowed to be marked exhausted / live in the exhausted list.

    Invariant: only single-token, non-wildcard queries qualify (artist/character
    tags). Multi-token queries (e.g. 'id:X status:any', 'a b'), metatag-bearing
    queries, and wildcards are never exhaustible. This is the single source of
    truth shared by mark_exhausted (write time) and purge_damaging_query_data
    (startup cleanup).
    """
    if not tags:
        return False
    toks = tags.split()
    if len(toks) != 1:
        return False
    tok = toks[0]
    if "*" in tok:
        return False
    if ":" in tok:  # metatag like id:, status:, fav:, order:, etc.
        return False
    return True


def mark_exhausted(tags):
    if not is_exhaustible(tags):
        log.info(f"Refusing to mark non-exhaustible query as exhausted: {tags!r}")
        return
    now = int(time.time())
    with db() as conn:
        conn.execute(
            """INSERT INTO query_progress
               (query_hash, query_tags, last_page, exhausted, exhausted_at, updated_at)
               VALUES (?, ?, 1, 1, ?, ?)
               ON CONFLICT(query_hash) DO UPDATE SET
                 exhausted = 1,
                 new_posts_found = 0,
                 exhausted_at = excluded.exhausted_at,
                 updated_at = excluded.updated_at""",
            (query_hash(tags), tags, now, now),
        )
    snapshot_post_count(tags)


def mark_unexhausted(tags, new_count):
    """Called by background scanner when new posts are found for an exhausted query."""
    now = int(time.time())
    with db() as conn:
        conn.execute(
            """INSERT INTO query_progress
               (query_hash, query_tags, last_page, exhausted, new_posts_found,
                last_scanned_at, updated_at)
               VALUES (?, ?, 1, 0, ?, ?, ?)
               ON CONFLICT(query_hash) DO UPDATE SET
                 exhausted = 0,
                 new_posts_found = excluded.new_posts_found,
                 last_scanned_at = excluded.last_scanned_at,
                 updated_at = excluded.updated_at""",
            (query_hash(tags), tags, new_count, now, now),
        )


def update_scanned_at(tags):
    now = int(time.time())
    with db() as conn:
        conn.execute(
            """INSERT INTO query_progress
               (query_hash, query_tags, last_page, last_scanned_at, updated_at)
               VALUES (?, ?, 1, ?, ?)
               ON CONFLICT(query_hash) DO UPDATE SET
                 last_scanned_at = excluded.last_scanned_at,
                 updated_at = excluded.updated_at""",
            (query_hash(tags), tags, now, now),
        )


def get_exhausted_queries():
    """Return (tag_string, last_scanned_at) tuples for all exhausted queries.

    last_scanned_at is None if the query has never been scanned.
    """
    with db() as conn:
        rows = conn.execute(
            "SELECT query_tags, last_scanned_at FROM query_progress WHERE exhausted = 1"
        ).fetchall()
    return [(row["query_tags"], row["last_scanned_at"]) for row in rows]


def get_primed_queries():
    """Return tag strings for queries with pending new posts from the scanner.

    A query is 'primed' if the background scanner found new posts on it that
    haven't yet been served via the main curator loop.
    """
    with db() as conn:
        rows = conn.execute(
            "SELECT query_tags FROM query_progress "
            "WHERE exhausted = 0 AND new_posts_found > 0 "
            "ORDER BY new_posts_found DESC, query_hash ASC"
        ).fetchall()
    return [row["query_tags"] for row in rows]


def decrement_primed_count(tags):
    """Reduce a primed query's pending count by 1; clamps at 0."""
    with db() as conn:
        conn.execute(
            """UPDATE query_progress
               SET new_posts_found = MAX(0, new_posts_found - 1),
                   updated_at = ?
               WHERE query_hash = ?""",
            (int(time.time()), query_hash(tags)),
        )


def clear_primed(tags):
    """Clear the new_posts_found counter after a primed query is served from."""
    with db() as conn:
        conn.execute(
            "UPDATE query_progress SET new_posts_found = 0 WHERE query_hash = ?",
            (query_hash(tags),),
        )


def purge_damaging_query_data():
    """Maintenance: remove/repair potentially damaging query_progress rows."""

    def _max(a, b):
        if a is None:
            return b
        if b is None:
            return a
        return max(a, b)

    with db() as conn:
        # --- Pass 1: drop exhausted rows that violate the exhaustion invariant ---
        # Shares is_exhaustible() with mark_exhausted() so write-time and cleanup
        # agree. Covers wildcards, multi-token queries, and metatag queries.
        bad = conn.execute(
            "SELECT query_hash, query_tags FROM query_progress WHERE exhausted = 1"
        ).fetchall()
        bad_hashes = [
            r["query_hash"] for r in bad if not is_exhaustible(r["query_tags"])
        ]
        for h in bad_hashes:
            conn.execute("DELETE FROM query_progress WHERE query_hash = ?", (h,))
        purged_wildcards = len(bad_hashes)

        # --- Pass 2 + 3: lowercase + rehash + de-duplicate ---
        rows = conn.execute(
            """SELECT query_hash, query_tags, last_page, exhausted, exhausted_at,
                      new_posts_found, last_scanned_at, page1_empty_at, updated_at
               FROM query_progress"""
        ).fetchall()

        merged = {}  # canonical_hash -> merged row dict
        needs_rewrite = False

        for r in rows:
            canon_tags = r["query_tags"].lower()
            canon_hash = query_hash(canon_tags)

            # Row changed if its tags weren't lowercase, or its stored hash was
            # already inconsistent with its tags (fixes pre-existing drift too).
            if canon_tags != r["query_tags"] or canon_hash != r["query_hash"]:
                needs_rewrite = True

            if canon_hash in merged:
                needs_rewrite = True  # collision -> dedup
                m = merged[canon_hash]
                m["last_page"] = max(m["last_page"], r["last_page"])
                # Stay exhausted only if EVERY colliding copy was exhausted.
                m["exhausted"] = min(m["exhausted"], r["exhausted"])
                m["new_posts_found"] = max(m["new_posts_found"], r["new_posts_found"])
                m["last_scanned_at"] = _max(m["last_scanned_at"], r["last_scanned_at"])
                m["page1_empty_at"] = _max(m["page1_empty_at"], r["page1_empty_at"])
                m["updated_at"] = _max(m["updated_at"], r["updated_at"])
                m["exhausted_at"] = _max(m["exhausted_at"], r["exhausted_at"])
            else:
                merged[canon_hash] = {
                    "query_hash": canon_hash,
                    "query_tags": canon_tags,
                    "last_page": r["last_page"],
                    "exhausted": r["exhausted"],
                    "exhausted_at": r["exhausted_at"],
                    "new_posts_found": r["new_posts_found"],
                    "last_scanned_at": r["last_scanned_at"],
                    "page1_empty_at": r["page1_empty_at"],
                    "updated_at": r["updated_at"],
                }

        lowercased = sum(1 for r in rows if r["query_tags"] != r["query_tags"].lower())
        duplicates_removed = len(rows) - len(merged)

        if needs_rewrite:
            # A merged row that's no longer exhausted shouldn't carry exhausted_at.
            for m in merged.values():
                if not m["exhausted"]:
                    m["exhausted_at"] = None
            conn.execute("DELETE FROM query_progress")
            conn.executemany(
                """INSERT INTO query_progress
                   (query_hash, query_tags, last_page, exhausted, exhausted_at,
                    new_posts_found, last_scanned_at, page1_empty_at, updated_at)
                   VALUES
                   (:query_hash, :query_tags, :last_page, :exhausted, :exhausted_at,
                    :new_posts_found, :last_scanned_at, :page1_empty_at, :updated_at)""",
                list(merged.values()),
            )

    if purged_wildcards or lowercased or duplicates_removed:
        log.info(
            f"Query cleanup: purged {purged_wildcards} non-exhaustible exhausted "
            f"queries, lowercased {lowercased} queries, removed "
            f"{duplicates_removed} duplicate rows."
        )


# ---------- Background scanner ----------


def _scan_one_query(tags, seen, blacklist):
    """Fetch page 1 for one query and update its progress. Returns new count."""
    try:
        posts = fetch_posts(tags, page=1)
    except Exception as e:
        log.warning(f"Scanner: error fetching '{tags}': {e}")
        return 0
    if not posts:
        update_scanned_at(tags)
        return 0
    new = [
        p
        for p in posts
        if p["id"] not in seen
        and not is_blacklisted(p, blacklist)
        and post_is_viewable(p)
    ]
    if new:
        mark_unexhausted(tags, len(new))
        set_page1_empty(tags, None)
        log.info(f"Scanner: '{tags}' has {len(new)} new post(s) — re-queued.")
    else:
        update_scanned_at(tags)
        set_page1_empty(tags, int(time.time()))
        log.info(f"Scanner: '{tags}' still fully seen.")
    return len(new)


def _scan_batch(batch, seen, blacklist):
    """OR-gate a group of single-tag exhausted queries.

    Fetch the combined page 1; if nothing on it is unseen, every tag in the
    batch is still fully seen (1 request). If any unseen post appears, fall back
    to scanning each tag individually for accurate per-tag attribution/counts.

    e621 orders by post id (upload order) and never inserts posts retroactively,
    so a genuinely new upload always lands at the top of the combined search and
    can't be buried. A full 320-result page therefore just means the batch holds
    a high-post-count tag (e.g. a prolific artist), not missed content — page
    fullness is not a signal and is ignored.
    """
    if len(batch) == 1:  # a lone ~tag is just the tag — no point gating
        _scan_one_query(batch[0], seen, blacklist)
        return

    combined = " ".join(f"~{t}" for t in batch)
    try:
        posts = fetch_posts(combined, page=1)
    except Exception as e:
        log.warning(f"Scanner: batch fetch failed ({combined}): {e} — falling back.")
        for t in batch:
            _scan_one_query(t, seen, blacklist)
        return

    unseen = [
        p
        for p in posts
        if p["id"] not in seen
        and not is_blacklisted(p, blacklist)
        and post_is_viewable(p)
    ]

    if not unseen:
        now = int(time.time())
        for t in batch:
            update_scanned_at(t)
            set_page1_empty(t, now)
        log.info(f"Scanner: batch of {len(batch)} still fully seen (1 req).")
        return

    # Attribute the unseen posts back to their tags so we only re-request the
    # tags that can actually account for them. A tag that appears somewhere on
    # the combined page but on none of the unseen posts is provably still fully
    # seen — no request needed. A tag that appears nowhere on the page can't be
    # judged (it may be an alias of a canonical tag e621 returned instead), so
    # it still gets an individual scan. If the page came back full it may be
    # clipped, so attribution is unsafe and everything is scanned.
    if len(posts) >= POSTS_PER_PAGE:
        candidates = list(batch)
        settled = []
    else:
        on_page = set()
        for p in posts:
            on_page |= post_tag_set(p)
        in_unseen = set()
        for p in unseen:
            in_unseen |= post_tag_set(p)
        candidates = [
            t for t in batch if t.lower() in in_unseen or t.lower() not in on_page
        ]
        settled = [t for t in batch if t not in candidates]

    if settled:
        now = int(time.time())
        for t in settled:
            update_scanned_at(t)
            set_page1_empty(t, now)

    log.info(
        f"Scanner: batch gate fired ({len(unseen)} new) — {len(settled)} tag(s) "
        f"cleared by attribution, scanning {len(candidates)} individually."
    )
    for t in candidates:
        _scan_one_query(t, seen, blacklist)


def remove_from_queries_file(tag):
    """Delete every queries.txt line that resolves to `tag`. Returns count.

    Lines are matched by parsing them the same way load_queries() does, so a
    tag written as a full e621 URL is caught as readily as a bare one.
    """
    if not QUERIES_FILE.exists():
        return 0
    target = tag.lower()
    kept = []
    removed = 0
    for line in QUERIES_FILE.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            kept.append(line)
            continue
        if stripped.startswith("http"):
            parsed = urllib.parse.urlparse(stripped)
            parsed_tags = urllib.parse.parse_qs(parsed.query).get("tags", [None])[0]
            value = (parsed_tags or "").lower()
        else:
            value = stripped.lower()
        if value == target:
            removed += 1
            continue
        kept.append(line)
    if removed:
        QUERIES_FILE.write_text("\n".join(kept) + "\n", encoding="utf-8")
    return removed


def confirm_tag_is_empty(tag):
    """Verify with e621 that a tag really has no posts left before expunging.

    Absence from the export is strong evidence but not proof — a tag missing
    from the alias map for any reason would otherwise get its queries.txt line
    deleted while still being live. One plain search settles it: an empty page 1
    means zero live posts, which is the same condition that makes the export
    drop the tag. Deliberately does NOT check `status:any` — that counts deleted
    posts, so a tag whose whole body of work was deleted (precisely the case
    this exists for) would always look alive. Any error answers "not empty", so
    a flaky request never deletes a hand-maintained line.
    """
    try:
        return not fetch_posts(tag, page=1)
    except Exception as e:
        log.warning(f"Expunge: could not confirm '{tag}' is empty ({e}) — keeping it.")
        return False


def expunge_empty_tag(tag):
    """Remove a tag that has dropped to zero posts from everything local.

    Drops the query_progress row, deletes its queries.txt line(s), and removes
    it from the additions table and both additions files.

    Before any of that it walks the tag's full `status:deleted` history and
    purges every one of those posts held locally. A tag reaching zero posts
    means its work was deleted, and once the query row is gone nothing will
    ever revisit the tag — so this is the last chance to clear those rows out
    of `seen` and `favorites` instead of orphaning them.
    """
    purged = _purge_deleted_for_tag(tag, max_pages=None)
    removed_lines = remove_from_queries_file(tag)
    with db() as conn:
        conn.execute(
            "DELETE FROM query_progress WHERE query_hash = ?", (query_hash(tag),)
        )
        cur = conn.execute("DELETE FROM additions WHERE tag = ?", (tag,))
        additions_removed = cur.rowcount
    for category in ("artist", "character"):
        remove_from_additions_file(tag, category)
    log.info(
        f"Expunge: '{tag}' has zero posts — purged {purged} deleted post(s), "
        f"dropped from exhausted list, removed {removed_lines} queries.txt "
        f"line(s), {additions_removed} additions row(s)."
    )


DELETED_SWEEP_PAGE_CAP = 25  # 8000 posts; guards against a runaway walk


def _purge_deleted_for_tag(tag, max_pages=1):
    """Purge locally-held posts that e621 has since deleted for this tag.

    With max_pages=1 (the routine diff case) this is one request. `status:deleted`
    sorts by post id like every other search, so page 1 shows the newest-uploaded
    deletions rather than the most recent ones — on a tag with a long deletion
    history an old post deleted yesterday can sit past the page boundary and be
    missed. That's the accepted trade for the cheap opportunistic sweep.

    Expunge passes max_pages=None to walk the tag's entire deleted history
    instead, because a tag being expunged is never coming back around for
    another sweep — anything left behind is orphaned in `seen` permanently.
    """
    purged = 0
    page = 1
    while True:
        try:
            posts = fetch_posts(f"{tag} status:deleted", page=page)
        except Exception as e:
            log.warning(f"Deleted sweep: fetch failed for '{tag}' p{page}: {e}")
            break
        if not posts:
            break

        ids = [p["id"] for p in posts]
        placeholders = ",".join("?" * len(ids))
        with db() as conn:
            rows = conn.execute(
                f"SELECT post_id FROM seen WHERE post_id IN ({placeholders}) "
                f"UNION SELECT post_id FROM favorites WHERE post_id IN ({placeholders})",
                ids + ids,
            ).fetchall()
        for r in rows:
            if purge_deleted_post(r["post_id"], "Deleted sweep"):
                purged += 1

        if max_pages is not None and page >= max_pages:
            break
        if len(posts) < POSTS_PER_PAGE:
            break  # last page of results
        page += 1
        if page > DELETED_SWEEP_PAGE_CAP:
            log.warning(
                f"Deleted sweep: '{tag}' hit the {DELETED_SWEEP_PAGE_CAP}-page cap; "
                f"remaining deleted posts will need a full --refresh-tags pass."
            )
            break

    if purged:
        log.info(f"Deleted sweep: '{tag}' — purged {purged} post(s).")
    return purged


def run_export_diff():
    """Scan only the exhausted tags whose export post_count actually moved.

    This replaces the old blanket page-1 sweep. For each exhausted query the
    baseline recorded at exhaustion is compared against the current export:

      * count unchanged -> the tag provably gained nothing; zero requests.
      * count changed (either direction) -> scan page 1 for new posts AND
        sweep `status:deleted`, since a drop means posts went away and a rise
        can hide a deletion underneath it.
      * no export row, alias-resolvable -> spelling we can't diff; scan it.
      * no export row, not an alias -> zero posts remain; expunge the tag.
      * no baseline yet (fresh row, or cleared by a blacklist edit) -> scan.

    Deletions that exactly cancel out additions inside one export window are
    invisible to this and will be missed; that's the accepted cost of dropping
    the periodic sweep.
    """
    rows = get_exhausted_queries()
    if not rows:
        log.info("Export diff: no exhausted queries to check.")
        return {"scanned": 0, "skipped": 0, "expunged": 0}

    with db() as conn:
        baselines = {
            r["query_tags"]: r["post_count_at_exhaustion"]
            for r in conn.execute(
                "SELECT query_tags, post_count_at_exhaustion FROM query_progress "
                "WHERE exhausted = 1"
            )
        }

    dirty = []
    expunge = []
    skipped = 0
    for tags, _last in tqdm(
        rows, desc="Export diff: compare", unit="tag", leave=False
    ):
        count = lookup_post_count(tags)
        if count is None:
            if _tag_graph.canonical(tags.lower()) != tags.lower():
                dirty.append((tags, None))  # renamed; can't diff, so check it
            else:
                expunge.append(tags)
            continue
        base = baselines.get(tags)
        if base is None or base != count:
            dirty.append((tags, count))
        else:
            skipped += 1

    log.info(
        f"Export diff: {len(rows)} exhausted tag(s) — {len(dirty)} changed, "
        f"{skipped} unchanged (no request), {len(expunge)} emptied."
    )

    expunged = 0
    for tags in tqdm(
        expunge, desc="Export diff: expunge", unit="tag", leave=False
    ):
        if confirm_tag_is_empty(tags):
            expunge_empty_tag(tags)
            expunged += 1
        else:
            log.info(f"Expunge: '{tags}' still has posts on e621 — left in place.")

    if dirty:
        _server_state["status"] = "scanning"
        try:
            seen = get_seen_ids()
            blacklist = load_blacklist()
            for tags, count in tqdm(
                dirty, desc="Export diff: scan", unit="tag", leave=False
            ):
                _scan_one_query(tags, seen, blacklist)
                _purge_deleted_for_tag(tags)
                if count is not None:
                    with db() as conn:
                        conn.execute(
                            "UPDATE query_progress "
                            "SET post_count_at_exhaustion = ? WHERE query_hash = ?",
                            (count, query_hash(tags)),
                        )
        finally:
            _server_state["status"] = "live"

    return {"scanned": len(dirty), "skipped": skipped, "expunged": expunged}


def check_blacklist_change():
    """Clear every baseline if blacklist.txt changed since the last run.

    Editing the blacklist changes what's viewable without moving a single
    post_count, so the diff would never notice on its own — un-blacklisting a
    tag would leave its posts permanently invisible to the scanner. Hashing the
    file turns that silent permanent miss into one full pass.
    """
    if BLACKLIST_FILE.exists():
        digest = hashlib.sha256(BLACKLIST_FILE.read_bytes()).hexdigest()
    else:
        digest = "missing"
    previous = get_meta("blacklist_hash")
    set_meta("blacklist_hash", digest)
    if previous is not None and previous != digest:
        cleared = clear_all_post_count_snapshots()
        log.info(
            f"blacklist.txt changed — cleared {cleared} post_count baseline(s); "
            f"next diff pass will scan every exhausted tag."
        )


def _scan_exhausted_queries():
    """Check page 1 of every exhausted query for new posts.

    Unconditional and exhaustive — this is the client-triggered force rescan.
    Routine scanning is driven by run_export_diff() off tag-export post_count
    deltas, so there is no freshness guard here.
    """
    _server_state["status"] = "scanning"
    try:
        rows = get_exhausted_queries()
        if not rows:
            log.info("Scanner: no exhausted queries to check.")
            return

        seen = get_seen_ids()
        blacklist = load_blacklist()
        log.info(f"Scanner: checking {len(rows)} exhausted queries for new posts.")

        tags_list = [tags for tags, _ in rows]
        for i in tqdm(
            range(0, len(tags_list), RESCAN_BATCH_TAGS),
            desc="Scanner: rescan",
            unit="batch",
            leave=False,
        ):
            _scan_batch(tags_list[i : i + RESCAN_BATCH_TAGS], seen, blacklist)
    finally:
        _server_state["status"] = "live"


# The blanket page-1 sweep no longer runs on a timer. run_export_diff() drives
# scanning off tag-export post_count deltas instead, and fires from the tag-data
# thread after every export check. _scan_exhausted_queries() below is kept for
# the client-triggered force rescan, which still checks everything.


def clear_all_page1_cache():
    """Reset every query's page1_empty_at to NULL. Called by --force-scan."""
    with db() as conn:
        cur = conn.execute(
            "UPDATE query_progress SET page1_empty_at = NULL "
            "WHERE page1_empty_at IS NOT NULL"
        )
        return cur.rowcount


# ---------- Favorites sync ----------


def _enumerate_e621_favorites():
    """Fetch all post IDs from the authenticated user's e621 favorites.

    Uses fav:USERNAME as the search query, walking all pages.
    Returns a set of integer post IDs, or None on auth failure.
    """
    if not E621_USERNAME:
        log.warning("Favorites sync: E621_USERNAME not set, skipping.")
        return None

    query = f"fav:{E621_USERNAME}"
    all_ids = set()
    page = 1
    # Total is unknown up front (the API gives no favorite count), so the bar
    # runs open-ended and just counts posts as pages come in.
    with tqdm(desc="Favorites sync: fetch", unit="post", leave=False) as bar:
        while True:
            try:
                posts = fetch_posts(query, page=page)
            except requests.RequestException as e:
                log.warning(f"Favorites sync: error on page {page}: {e}")
                break
            if not posts:
                break
            for p in posts:
                all_ids.add(p["id"])
            bar.update(len(posts))
            page += 1

    log.info(f"Favorites sync: found {len(all_ids)} favorites on e621.")
    return all_ids


def sync_favorites():
    """Sync local favorites table with e621. Full replace semantics:
    - Insert any e621 favorites not in local DB
    - Delete any local favorites no longer on e621
    """
    e621_ids = _enumerate_e621_favorites()
    if e621_ids is None:
        return

    with db() as conn:
        local_rows = conn.execute("SELECT post_id FROM favorites").fetchall()
    local_ids = {r["post_id"] for r in local_rows}

    to_add = e621_ids - local_ids
    to_remove = local_ids - e621_ids

    now = int(time.time())
    with db() as conn:
        if to_add:
            conn.executemany(
                "INSERT OR IGNORE INTO favorites (post_id, favorited_at) VALUES (?, ?)",
                [(pid, now) for pid in to_add],
            )
        if to_remove:
            conn.executemany(
                "DELETE FROM favorites WHERE post_id = ?",
                [(pid,) for pid in to_remove],
            )

    log.info(
        f"Favorites sync: +{len(to_add)} added, -{len(to_remove)} removed. "
        f"Local total: {len(e621_ids)}."
    )


_maintenance_lock = threading.Lock()


def _maintenance_pass(first=False):
    """One-shot: sync favorites, then backfill tag caches, then exit.

    Strictly ordered — the backfill's uncached set is only complete once the
    favorites sync has finished adding rows, so it always runs second. The
    thread terminates when done; it is re-launched at startup and after a
    client-triggered rescan rather than sleeping on a timer.
    """
    if not _maintenance_lock.acquire(blocking=False):
        log.info("Maintenance: already running — skipping duplicate launch.")
        return
    try:
        if first:
            _server_state["status"] = "initializing"
        log.info("Favorites sync: activating.")
        try:
            sync_favorites()
            log.info("Favorites sync: complete.")
        except Exception as e:
            log.error(f"Favorites sync: unhandled error: {e}")

        log.info("Tag backfill: activating.")
        try:
            backfill_tag_cache()
            log.info("Tag backfill: complete.")
        except Exception as e:
            log.error(f"Tag backfill: unhandled error: {e}")
        log.info("Maintenance: pass finished; thread terminating.")
    finally:
        if first and _server_state["status"] == "initializing":
            _server_state["status"] = "live"
        _maintenance_lock.release()


def start_maintenance(first=False):
    """Launch the favorites-sync → tag-backfill chain on its own thread."""
    t = threading.Thread(
        target=_maintenance_pass,
        kwargs={"first": first},
        daemon=True,
        name="maintenance",
    )
    t.start()
    log.info("Maintenance thread launched (favorites sync → tag backfill).")
    return t


# ---------- Routes ----------


def _find_unseen_post(query, seen, blacklist, persist=True):
    """Two-phase page strategy for one query.

    When persist=False, skip all writes to query_progress (no last_page tracking,
    no exhaustion marking). Used for ephemeral override queries.

    Semantics: `last_page` records the highest page known to be FULLY seen.
    Phase 2 resumes from `last_page + 1` (everything below is already exhausted).

    Phase 1 — check page 1 for new content, BUT only if we haven't recently
              confirmed page 1 is fully seen (cached via last_scanned_at).
    Phase 2 — walk forward from last_page + 1. Each page that comes back fully
              seen is recorded by advancing last_page; the first page with
              eligible content yields a post WITHOUT advancing last_page (since
              that page still has more unseen posts on it).

    Returns (post, page) on success, or None if the query is exhausted.
    """

    def unseen_on_page(page):
        posts = fetch_posts(query, page=page)
        if not posts:
            return None, []  # past end of results
        eligible = [
            p
            for p in posts
            if p["id"] not in seen
            and not is_blacklisted(p, blacklist)
            and post_is_viewable(p)
        ]
        return posts, eligible

    # Page 1 freshness cache: if the scanner or a prior request recently
    # confirmed page 1 has no eligible posts, skip the redundant fetch.
    # The scanner runs hourly, so trusting its result for ~30 minutes is safe.
    PAGE1_CACHE_SECONDS = 86400  # 24 hours — artists rarely post twice in a day

    if persist and len(query.split()) != 1:
        persist = False

    try:
        # Decide whether to skip phase 1
        skip_phase1 = False
        if persist:
            with db() as conn:
                row = conn.execute(
                    """SELECT page1_empty_at FROM query_progress
                       WHERE query_hash = ?""",
                    (query_hash(query),),
                ).fetchone()
            if row and row["page1_empty_at"]:
                age = int(time.time()) - row["page1_empty_at"]
                if age < PAGE1_CACHE_SECONDS:
                    skip_phase1 = True
                    log.info(
                        f"Query '{query}' skipping phase 1 (cached empty {age}s ago)"
                    )

        # Phase 1: check page 1 for new posts
        if not skip_phase1:
            posts, eligible = unseen_on_page(1)
            if posts is None:
                log.info(f"Query '{query}' page 1 returned no posts at all.")
                return None
            if eligible:
                # Page 1 has fresh content — clear the empty cache
                if persist:
                    set_page1_empty(query, None)
                return random.choice(eligible), 1
            # Page 1 fully seen — cache that fact
            if persist:
                set_page1_empty(query, int(time.time()))

        # Phase 2: walk forward from last_page + 1.
        if persist:
            resume_page = get_last_page(query) + 1
        else:
            resume_page = 2
        log.info(
            f"Query '{query}' resuming phase 2 from page {resume_page} "
            f"(persist={persist}, phase1_skipped={skip_phase1})"
        )
        page = resume_page
        while True:
            posts, eligible = unseen_on_page(page)
            if posts is None:
                log.info(f"Query '{query}' exhausted (last attempted page: {page}).")
                if persist:
                    mark_exhausted(query)
                return None
            if eligible:
                log.info(f"Query '{query}' found unseen post on page {page}.")
                return random.choice(eligible), page
            log.info(
                f"Query '{query}' page {page} fully seen ({len(posts)} posts); "
                f"{'advancing last_page and ' if persist else ''}continuing."
            )
            if persist:
                set_last_page(query, page)
            page += 1

    except requests.RequestException as e:
        log.warning(f"Query '{query}' network error: {e}")
        return None


def _static_version(filename):
    """mtime of a file in static/, for cache-busting its URL.

    Flask serves /static with a long max-age, so an edited app.js or
    style.css keeps coming out of the browser cache no matter how many times
    the server restarts. Stamping the URL with the file's mtime makes every
    edit a new URL. Returns 0 if the file is missing, which just means no
    stamp rather than a broken page.
    """
    try:
        return int((ROOT / "static" / filename).stat().st_mtime)
    except OSError:
        return 0


@app.route("/")
def index():
    queries = load_queries()
    return render_template(
        "index.html",
        query_count=len(queries),
        js_version=_static_version("app.js"),
        css_version=_static_version("style.css"),
    )


def mark_post_seen(post_id):
    """Mark a post as seen. Idempotent — safe to call multiple times."""
    with db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO seen (post_id, seen_at) VALUES (?, ?)",
            (post_id, int(time.time())),
        )


# ---------- Post tag cache ----------
#
# Tags are cached for every post we fetch — every display naturally refreshes
# the cache, so we don't track staleness. Cache entries are used as-is for
# review-mode filter matching. The only staleness window in practice is "a
# post whose tags changed on e621 between when we last displayed it and now,"
# which is rare and self-corrects on the next display.


def get_post_tag_cache(post_id):
    """Return (tags_dict, rating, cached_at) for a post, or None if not cached."""
    with db() as conn:
        row = conn.execute(
            "SELECT tags_blob, cached_at FROM post_tags WHERE post_id = ?",
            (post_id,),
        ).fetchone()
    if not row:
        return None
    try:
        tags_dict, rating = decode_tags(bytes(row["tags_blob"]))
    except Exception:
        return None
    return (tags_dict, rating, row["cached_at"])


def set_post_tag_cache(post_id, tags_dict, rating):
    """Upsert the tag cache for a post using compact binary encoding.

    Tags are alias-canonicalized and reduced to their most specific form
    first — anything implied by another tag on the same post is dropped, and
    put back by _flatten_post_tags() at search time. While the implication
    graph is empty this is a passthrough, so the stored set is unchanged.
    """
    tags_dict = _tag_graph.reduce(tags_dict)
    blob = encode_tags(tags_dict, rating)
    with db() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO post_tags
               (post_id, tags_blob, cached_at)
               VALUES (?, ?, ?)""",
            (post_id, blob, int(time.time())),
        )


# ---------- Post relationships (parents, children, pools) ----------
#
# Populated from the same responses that feed the tag cache, so tracking
# relationships costs zero extra API calls. Everything here is a snapshot of
# what e621 said at fetch time; the refresh/backfill sweeps re-assert it.


def _csv_ids(values):
    """Normalise an ID list into the stored comma-separated form."""
    out = sorted({int(v) for v in (values or []) if str(v).strip().isdigit() or isinstance(v, int)})
    return ",".join(str(v) for v in out)


def _parse_csv_ids(text):
    """Inverse of _csv_ids. Empty/NULL text yields []."""
    if not text:
        return []
    return [int(tok) for tok in text.split(",") if tok]


def extract_relations(post):
    """Pull (parent_id, children, pools) out of a full e621 post object.

    Reads the v2 `relationships` block and the top-level `pools` array. A
    post with none of these still returns a valid tuple -- "no relations" is
    a fact worth recording, not a reason to skip the row.
    """
    rel = post.get("relationships") or {}
    parent_id = rel.get("parent_id")
    try:
        parent_id = int(parent_id) if parent_id is not None else None
    except (TypeError, ValueError):
        parent_id = None
    return parent_id, list(rel.get("children") or []), list(post.get("pools") or [])


def set_post_relations(post_id, parent_id, children, pools):
    """Upsert the relationship row for a post."""
    with db() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO post_relations
               (post_id, parent_id, children, pools, updated_at)
               VALUES (?, ?, ?, ?, ?)""",
            (post_id, parent_id, _csv_ids(children), _csv_ids(pools), int(time.time())),
        )


def get_post_relations(post_id):
    """Return the stored relations for a post, or None if never looked up.

    Shape:
        {"parent_id": int|None, "children": [int], "pools": [int],
         "updated_at": int}
    """
    with db() as conn:
        row = conn.execute(
            "SELECT * FROM post_relations WHERE post_id = ?", (post_id,)
        ).fetchone()
    if not row:
        return None
    return {
        "parent_id": row["parent_id"],
        "children": _parse_csv_ids(row["children"]),
        "pools": _parse_csv_ids(row["pools"]),
        "updated_at": row["updated_at"],
    }


def relations_search_tags(post_id, relations):
    """Build the search strings that reassemble a post's context.

    `parent:<id>` is e621's own metatag: posts whose parent is <id>, i.e.
    the child set of <id>. It does NOT include <id> itself.

    `child:<id>` is OURS -- e621 only has the boolean child:none/child:any,
    with no ID form, so there is no upstream way to ask "which post is the
    parent of <id>". expand_child_metatag() resolves it locally into an
    `id:` search before anything reaches the API. Defining it this way makes
    it the exact mirror of parent:: parent:X walks down, child:X walks up.

    Returns a dict of label -> tag string, omitting anything that doesn't
    apply to this post.
    """
    out = {}
    if not relations:
        return out
    if relations.get("parent_id"):
        # Walk up to the parent itself.
        out["parent"] = f"child:{post_id}"
        # And the rest of the family: everything sharing that parent.
        out["siblings"] = f"parent:{relations['parent_id']}"
    if relations.get("children"):
        out["children"] = f"parent:{post_id}"
    for pool_id in relations.get("pools") or []:
        out[f"pool:{pool_id}"] = f"pool:{pool_id}"
    return out


# `child:<id>` -- our own metatag, resolved before the query leaves the app.
_CHILD_METATAG = re.compile(r"(?<!\S)child:(\d+)(?!\S)")

# What an unresolvable child: search becomes. `id:0` is a well-formed search
# for a post ID that cannot exist, so it returns nothing instead of silently
# dropping the constraint and matching the entire site.
_IMPOSSIBLE_QUERY = "id:0"


def _parent_of(post_id):
    """Parent post ID of `post_id`, or None if it has none.

    Checks the local relations table first -- for anything already curated
    the answer is free. Falls back to a single-post fetch, caching the whole
    response on the way through so the next lookup is local too.
    """
    rel = get_post_relations(post_id)
    if rel is not None:
        return rel["parent_id"]
    try:
        post = _fetch_post_by_id(post_id)
    except Exception as e:
        log.warning(f"child:{post_id} lookup failed: {e}")
        return None
    if not post:
        return None
    cache_post_from_response(post)
    parent_id, _, _ = extract_relations(post)
    return parent_id


def expand_child_metatag(tags):
    """Rewrite every `child:<id>` token in a query into `id:<parent_id>`.

    e621 never sees `child:` -- it wouldn't understand the ID form. Tokens
    it DOES understand (`child:none`, `child:any`) don't match the pattern
    and pass through untouched.

    A post with no parent, or one that can't be resolved, collapses the
    whole query to `id:0` so the search returns nothing rather than
    quietly widening.
    """
    if "child:" not in tags:
        return tags

    impossible = False

    def _sub(match):
        nonlocal impossible
        child_id = int(match.group(1))
        parent_id = _parent_of(child_id)
        if parent_id is None:
            log.info(f"child:{child_id} -> no parent; query yields nothing.")
            impossible = True
            return _IMPOSSIBLE_QUERY
        log.info(f"child:{child_id} -> id:{parent_id}")
        return f"id:{parent_id} status:any"

    rewritten = _CHILD_METATAG.sub(_sub, tags)
    return _IMPOSSIBLE_QUERY if impossible else rewritten


def cache_relations_from_response(post):
    """Record relationships straight off a fetched post object."""
    if not post or "id" not in post:
        return
    parent_id, children, pools = extract_relations(post)
    set_post_relations(post["id"], parent_id, children, pools)


def get_unrelated_post_ids():
    """Referenced post IDs with no relationship row yet.

    Mirrors get_uncached_post_ids(); both feed the same backfill sweep.
    """
    with db() as conn:
        rows = conn.execute(
            """
            SELECT post_id FROM (
                SELECT post_id FROM seen
                UNION
                SELECT post_id FROM favorites
            )
            WHERE post_id NOT IN (SELECT post_id FROM post_relations)
            ORDER BY post_id
            """
        ).fetchall()
    return [r["post_id"] for r in rows]


def cache_post_from_response(post):
    """Update the tag cache from a fully-fetched e621 post object.

    Called whenever we fetch a post for display — no extra API call needed,
    we just persist what e621 already gave us.
    """
    if not post or "id" not in post:
        return
    tags_dict = post.get("tags") or {}
    rating = post.get("rating")
    set_post_tag_cache(post["id"], tags_dict, rating)
    cache_relations_from_response(post)


# ---------- Tag-cache backfill ----------


def get_uncached_post_ids():
    """Return post IDs referenced locally (seen or favorited) with no tag cache.

    These are the posts /api/review/list reports as `uncached` and excludes
    from tag-based override filtering until their tags are known.
    """
    with db() as conn:
        rows = conn.execute(
            """
            SELECT post_id FROM (
                SELECT post_id FROM seen
                UNION
                SELECT post_id FROM favorites
            )
            WHERE post_id NOT IN (SELECT post_id FROM post_tags)
            ORDER BY post_id
            """
        ).fetchall()
    return [r["post_id"] for r in rows]


def _fetch_posts_by_ids(ids):
    """Fetch several posts in a single request via the e621 `id:` metatag.

    `id:1,2,3` is one tag (so we stay well under the tag-count limit) and a
    page holds up to 320 posts. `status:any` lifts the implicit
    `-status:deleted` filter so deleted-status posts — which still have tags
    and a file, and are worth keeping — come back and get cached instead of
    looking gone. Only truly-nonexistent IDs will be absent from the response.
    """
    if not ids:
        return []
    tag = "id:" + ",".join(str(i) for i in ids) + " status:any"
    return fetch_posts(tag, page=1)


def purge_post(post_id):
    """Remove every local trace of a post that no longer exists on e621.

    Wipes it from seen-history, favorites, and the tag cache so it can't
    resurface as a dead reference in review mode or override filtering.
    """
    with db() as conn:
        conn.execute("DELETE FROM seen WHERE post_id = ?", (post_id,))
        conn.execute("DELETE FROM favorites WHERE post_id = ?", (post_id,))
        conn.execute("DELETE FROM post_tags WHERE post_id = ?", (post_id,))
        conn.execute("DELETE FROM post_relations WHERE post_id = ?", (post_id,))


def _is_locally_favorited(post_id):
    """True if the post has a row in the local favorites table."""
    with db() as conn:
        row = conn.execute(
            "SELECT 1 FROM favorites WHERE post_id = ? LIMIT 1", (post_id,)
        ).fetchone()
    return row is not None


def purge_deleted_post(post_id, label, log_fn=None):
    """Purge a deleted-status post, un-favoriting it on e621 first if needed.

    Deleted posts keep their tags, md5 and dimensions but every file URL comes
    back null, so they can never be displayed — the review endpoint rejects
    them for having no viewable file. They're dead weight in the cache.

    The remote un-favorite has to happen BEFORE the local purge, and has to be
    decided from the local favorites table rather than the post payload:
    `stats.is_favorited` stays true on deleted posts, so leaving the favorite
    in place would let the favorites sync thread re-add the row on its next
    pass and undo the purge. Removing it upstream is what makes the purge
    stick.

    A failed un-favorite aborts the purge for that post rather than leaving
    e621 and the local DB disagreeing; it'll be retried next sweep.

    `log_fn` receives the "post is deleted" message instead of it going
    straight to the logger — used by batch callers to buffer these and flush
    them after a progress bar closes, instead of tearing up the bar's line on
    every post. Defaults to `log.info` when not given. Un-favorite failures
    always log immediately (via `log.warning`) regardless of `log_fn`, since
    they're rare and need visibility right away.
    """
    emit = log_fn if log_fn is not None else log.info
    if _is_locally_favorited(post_id):
        try:
            e621_unfavorite(post_id)
        except Exception as e:
            log.warning(
                f"{label}: post {post_id} is deleted but un-favoriting failed "
                f"({e}) — leaving it alone, will retry next sweep."
            )
            return False
        emit(
            f"{label}: post {post_id} is deleted — un-favorited on e621 "
            f"and purged locally."
        )
    else:
        emit(f"{label}: post {post_id} is deleted — purged locally.")
    purge_post(post_id)
    return True


def _get_all_referenced_post_ids():
    """Every post ID referenced locally (seen or favorited), cached or not."""
    with db() as conn:
        rows = conn.execute(
            """
            SELECT post_id FROM (
                SELECT post_id FROM seen
                UNION
                SELECT post_id FROM favorites
            )
            ORDER BY post_id
            """
        ).fetchall()
    return [r["post_id"] for r in rows]


def get_refresh_progress():
    """Return the interrupted-sweep row as a dict, or None if there isn't one."""
    with db() as conn:
        row = conn.execute("SELECT * FROM refresh_progress WHERE id = 1").fetchone()
    return dict(row) if row else None


def set_refresh_progress(next_id, total, done, cached, purged, started_at):
    """Upsert the resume point. Called once per completed batch."""
    now = int(time.time())
    with db() as conn:
        conn.execute(
            """
            INSERT INTO refresh_progress
                (id, next_id, total, done, cached, purged, started_at, updated_at)
            VALUES (1, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                next_id = excluded.next_id,
                total = excluded.total,
                done = excluded.done,
                cached = excluded.cached,
                purged = excluded.purged,
                updated_at = excluded.updated_at
            """,
            (next_id, total, done, cached, purged, started_at, now),
        )


def clear_refresh_progress():
    """Drop the resume point — the sweep finished (or was cancelled)."""
    with db() as conn:
        conn.execute("DELETE FROM refresh_progress WHERE id = 1")


class _SmoothBar:
    """A tqdm bar counting posts, driven by an easing curve between batches.

    Tags arrive a whole batch at a time (up to POSTS_PER_PAGE posts land on
    one response), so a bar driven straight off completed posts sits still
    for a whole request and then leaps. A free-running estimate is no better:
    it races to the next batch boundary, stalls against it, and leaps anyway
    when reality disagrees.

    So instead of extrapolating a rate, each batch is *eased*: while a batch
    is in flight the display closes on `actual + pending` at a velocity of
    remaining/tau, where tau comes from the observed seconds-per-post of
    previous batches. Bare exponential decay would be all front-loaded burst
    and long tail, so that velocity is also capped at a little over the
    estimated steady pace — the result is near-linear motion while the batch
    is running to schedule, decaying smoothly toward the boundary only if it
    overruns. Either way the bar never reaches the boundary early and stalls.

    Crucially, a landing batch never snaps the display forward: the few
    percent the curve hadn't covered yet is simply where the *next* batch's
    curve starts from, so the residual is absorbed into continued motion
    instead of a visible step.

    The display never moves backwards and never passes the batch boundary,
    so easing can lead reality within the current batch but can't claim a
    batch that hasn't landed.
    """

    TICK = 0.05  # seconds between display updates
    ALPHA = 0.3  # EMA weight on the newest batch's observed rate
    REACH = 3.0  # tau divisor: ~95% of the way at the expected duration
    OVERRUN = 1.25  # velocity ceiling, as a multiple of the estimated pace
    FIRST_GUESS = 4.0  # seconds to assume for the first batch, rate unknown

    def __init__(self, total, desc, initial=0):
        # `initial` is work completed before this run started (a resumed
        # refresh), so the bar opens partway along a full-sweep total instead
        # of restarting at 0 against the remaining slice.
        self.total = total
        self.actual = initial  # posts genuinely fetched and processed
        self.pending = 0  # size of the batch currently in flight
        self.shown = float(initial)  # posts currently drawn
        self.rate = 0.0  # smoothed posts/s, whole-cycle
        self.started = None  # monotonic time the in-flight batch began
        self.cap = 0.0  # max posts/s the display may move at
        self.tau = self.FIRST_GUESS / self.REACH
        self.lock = threading.Lock()
        self.done = threading.Event()
        self.bar = tqdm(
            total=total,
            desc=desc,
            unit="post",
            leave=False,
            smoothing=0,
            initial=initial,
        )
        self.ticker = threading.Thread(target=self._tick, daemon=True)
        self.ticker.start()

    def _tick(self):
        last = time.monotonic()
        while not self.done.wait(self.TICK):
            now = time.monotonic()
            dt, last = now - last, now
            with self.lock:
                if not self.pending:
                    continue
                remaining = (self.actual + self.pending) - self.shown
                if remaining <= 0:
                    continue
                velocity = min(remaining / self.tau, self.cap)
                self._draw(self.shown + velocity * dt)

    def _draw(self, target):
        """Move the display to `target`, clamped. Caller holds the lock.

        `shown` is kept as a float so sub-post-per-tick motion accumulates,
        but the bar itself is only ever advanced in whole posts — tqdm would
        otherwise render a fractional count.
        """
        ceiling = min(self.total, self.actual + self.pending)
        self.shown = max(self.shown, min(target, ceiling))
        whole = int(self.shown)
        if whole > self.bar.n:
            self.bar.update(whole - self.bar.n)

    def batch_start(self, posts):
        """Mark `posts` as in flight and pick the easing constant for them."""
        with self.lock:
            self.pending = posts
            self.started = time.monotonic()
            expected = posts / self.rate if self.rate else self.FIRST_GUESS
            self.tau = max(expected / self.REACH, 0.05)
            self.cap = (posts / expected) * self.OVERRUN

    def batch_end(self):
        """Retire the in-flight batch and refold its pace into the estimate.

        Called after the batch is fully processed — including any one-by-one
        404 verification — so the rate reflects real wall-clock progress per
        post rather than just the fetch, and the next batch's easing curve is
        stretched to match.
        """
        with self.lock:
            if not self.pending:
                return
            elapsed = time.monotonic() - self.started
            if elapsed > 0:
                observed = self.pending / elapsed
                self.rate = (
                    observed
                    if self.rate == 0
                    else self.ALPHA * observed + (1 - self.ALPHA) * self.rate
                )
            self.actual += self.pending
            self.pending = 0
            # Deliberately no catch-up draw here — whatever the curve hadn't
            # covered becomes the next batch's starting point.

    def close(self):
        self.done.set()
        self.ticker.join(timeout=1)
        with self.lock:
            self.pending = 0
            self.shown = float(self.actual)
            self.bar.n = self.actual
            self.bar.refresh()
            self.bar.close()


def _process_tag_batches(
    ids, batch_size, label, checkpoint=None, bar_total=None, bar_initial=0
):
    """Fetch tags for `ids` in batches, overwriting the cache with whatever
    e621 returns and purging any post it confirms is gone (a real 404).

    Shared by backfill (uncached subset) and refresh (everything). Anything
    absent from a `status:any` batch is verified one-by-one via
    /posts/{id}.json before deletion, so a transient miss or odd status never
    costs a seen-history or favorites row.

    `batch_size` must not exceed POSTS_PER_PAGE, or the page limit would clip
    results and make present posts look missing (they'd survive the 404 check,
    just at the cost of an extra verify request each).

    `checkpoint`, if given, is called as
    `checkpoint(next_id, done, cached, purged)` after every batch, where
    `next_id` is the first ID *not* yet processed (None once the list is
    exhausted). It fires for failed batches too — a fetch failure is skipped
    rather than retried in a normal run, so a resumed run skips it identically
    instead of re-attempting only the batches that happened to straddle the
    interruption.

    Returns {"cached": int, "purged": int}.
    """
    batch_size = min(batch_size, POSTS_PER_PAGE)
    cached = 0
    purged = 0
    deferred_logs = []
    bar = _SmoothBar(
        len(ids) + bar_initial if bar_total is None else bar_total,
        label,
        initial=bar_initial,
    )
    try:
        done = 0
        for start in range(0, len(ids), batch_size):
            batch = ids[start : start + batch_size]
            rest = ids[start + batch_size :]
            next_id = rest[0] if rest else None
            bar.batch_start(len(batch))
            try:
                posts = _fetch_posts_by_ids(batch)
            except Exception as e:
                log.warning(f"{label}: batch fetch failed ({len(batch)} ids): {e}")
                bar.batch_end()
                done += len(batch)
                if checkpoint:
                    checkpoint(next_id, done, cached, purged)
                continue

            returned = set()
            for p in posts:
                returned.add(p.get("id"))
                if (p.get("flags") or {}).get("deleted"):
                    if purge_deleted_post(p["id"], label, log_fn=deferred_logs.append):
                        purged += 1
                    continue
                cache_post_from_response(p)
                cached += 1

            # Confirm each missing ID individually before deleting anything.
            for pid in (i for i in batch if i not in returned):
                try:
                    post = _fetch_post_by_id(pid)
                except Exception as e:
                    log.warning(f"{label}: verify fetch failed for {pid}: {e}")
                    continue
                if post is None:
                    purge_post(pid)
                    purged += 1
                    deferred_logs.append(
                        f"{label}: post {pid} is gone from e621 — purged."
                    )
                elif (post.get("flags") or {}).get("deleted"):
                    # Came back on the single-post endpoint but deleted —
                    # same treatment as the batch path.
                    if purge_deleted_post(pid, label, log_fn=deferred_logs.append):
                        purged += 1
                else:
                    # Still exists after all — cache it and move on.
                    cache_post_from_response(post)
                    cached += 1

            bar.batch_end()
            done += len(batch)
            if checkpoint:
                checkpoint(next_id, done, cached, purged)
    finally:
        bar.close()

    for msg in deferred_logs:
        log.info(msg)

    return {"cached": cached, "purged": purged}


def backfill_tag_cache():
    """Fetch and cache tags for any locally-referenced post missing from the
    tag cache, batched over the `id:` metatag. Posts confirmed gone are purged.

    Runs as the second half of the maintenance chain, so it stays scoped to
    the uncached subset rather than re-touching everything.
    """
    ids = sorted(set(get_uncached_post_ids()) | set(get_unrelated_post_ids()))
    if not ids:
        return {"cached": 0, "purged": 0}

    log.info(f"Tag backfill: {len(ids)} post(s) missing cached tags or relations.")
    result = _process_tag_batches(ids, POSTS_PER_PAGE, "Tag backfill")
    log.info(
        f"Tag backfill: cached {result['cached']}, "
        f"purged {result['purged']} gone post(s)."
    )
    return result


def refresh_tag_cache(resume=True):
    """Re-fetch tags for EVERY locally-referenced post (seen and favorites),
    overwriting the cache so tag edits made on e621 are picked up, and purging
    any post that's gone. Uses full-page (maximal) batches to minimise API
    calls.

    This is the heavy sweep — it deliberately re-touches already-cached posts,
    which the periodic backfill skips. Triggered by `--refresh-tags`, and
    resumed automatically at startup if a previous sweep was interrupted.

    Progress is checkpointed to `refresh_progress` after every batch and the
    row is deleted on completion, so an interrupted sweep (Ctrl-C, crash,
    power loss) picks up at the batch boundary it reached rather than starting
    over. `resume=False` discards any stored checkpoint and sweeps from the
    top.

    Returns {"cached": int, "purged": int}, with the counts covering the whole
    sweep including work done before the interruption.
    """
    if resume:
        saved = get_refresh_progress()
    else:
        saved = None
        clear_refresh_progress()

    ids = _get_all_referenced_post_ids()
    if not ids:
        clear_refresh_progress()
        return {"cached": 0, "purged": 0}

    base_cached = base_purged = base_done = 0
    started_at = int(time.time())
    total = len(ids)

    if saved:
        base_cached = saved["cached"]
        base_purged = saved["purged"]
        base_done = saved["done"]
        started_at = saved["started_at"]
        total = saved["total"]
        # Resume at the stored cursor. Bisect rather than index() because the
        # exact ID may have been purged since the checkpoint was written.
        cut = bisect.bisect_left(ids, saved["next_id"])
        ids = ids[cut:]
        if not ids:
            clear_refresh_progress()
            log.info("Tag refresh: interrupted sweep had nothing left to do.")
            return {"cached": base_cached, "purged": base_purged}
        log.info(
            f"Tag refresh: resuming interrupted sweep at post {saved['next_id']} "
            f"— {base_done}/{total} already done, {len(ids)} to go."
        )
    else:
        log.info(f"Tag refresh: re-fetching tags for {total} referenced post(s).")

    def _checkpoint(next_id, done, cached, purged):
        set_refresh_progress(
            next_id if next_id is not None else (ids[-1] + 1),
            total,
            base_done + done,
            base_cached + cached,
            base_purged + purged,
            started_at,
        )

    result = _process_tag_batches(
        ids,
        POSTS_PER_PAGE,
        "Tag refresh",
        checkpoint=_checkpoint,
        bar_total=max(total, base_done + len(ids)),
        bar_initial=base_done,
    )
    clear_refresh_progress()

    result = {
        "cached": base_cached + result["cached"],
        "purged": base_purged + result["purged"],
    }
    log.info(
        f"Tag refresh: refreshed {result['cached']}, "
        f"purged {result['purged']} gone post(s)."
    )
    return result


def resume_interrupted_refresh(cancel=False):
    """At startup, pick up a tag refresh that was interrupted mid-sweep.

    A row in `refresh_progress` means the last `--refresh-tags` run didn't
    reach the end. Rather than making the user re-run it (and re-walk the
    posts already done), finish it here on a daemon thread so the server comes
    up immediately — this is a long API-bound sweep, not a startup dependency.

    `cancel=True` (from `--no-resume`) drops the checkpoint instead and starts
    the app clean; the next full refresh then sweeps from the top.

    Returns True if a resume was launched.
    """
    saved = get_refresh_progress()
    if not saved:
        return False

    if cancel:
        clear_refresh_progress()
        log.info(
            f"Tag refresh: discarding interrupted sweep "
            f"({saved['done']}/{saved['total']} done) — --no-resume."
        )
        return False

    log.info(
        f"Tag refresh: interrupted sweep detected "
        f"({saved['done']}/{saved['total']} done) — resuming in background."
    )

    def _resume():
        try:
            result = refresh_tag_cache(resume=True)
            log.info(f"Tag refresh: resumed sweep complete: {result}")
        except Exception as e:
            log.error(f"Tag refresh: resumed sweep failed: {e}")

    threading.Thread(target=_resume, daemon=True, name="tag-refresh-resume").start()
    return True


def retrain_tag_dict():
    """Train a fresh zstd dictionary from the corpus and rewrite every blob.

    Flow: read all blobs -> decompress to raw payloads -> train new dict ->
    (one transaction) rotate 'dict' to 'dict_old' + commit new 'dict' +
    rewrite all successfully-decoded blobs to the v3 wrapper flagged current
    -> drop 'dict_old' once nothing references it any more.

    The manager lock is held for the whole operation, so a concurrent
    set_post_tag_cache can't compress against a dictionary that's about to
    be dropped; it just blocks for the few seconds this takes. The rewrite
    is a single transaction — a crash mid-way rolls back to the old blobs
    and old dict, both fully intact (each blob's own flag byte makes any
    committed state self-describing regardless).

    Honors DICT_TRAIN_SAMPLES (--dict-samples): 0 trains on everything,
    otherwise a random sample of that many payloads. Rewrites all
    successfully-decoded blobs either way.

    Returns stats dict, or None if there was nothing to do.
    """
    with _tag_dicts.lock:
        with db() as conn:
            rows = conn.execute(
                "SELECT post_id, tags_blob, cached_at FROM post_tags"
            ).fetchall()
        if not rows:
            log.info("Dict retrain: no cached tags, skipping.")
            return None

        # Decode fully (not just decompress) since retrain is a full rewrite
        # anyway. Tags are also re-reduced against the current implication
        # graph, so a graph refresh shrinks old blobs too.
        payloads = {}
        old_bytes = 0
        errors = 0
        for r in tqdm(rows, desc="Dict retrain: decode", unit="blob", leave=False):
            blob = bytes(r["tags_blob"])
            old_bytes += len(blob)
            try:
                tags_dict, rating = _decode_raw_v3(_decompress_blob(blob))
                tags_dict = _tag_graph.reduce(tags_dict)
                payloads[r["post_id"]] = (
                    _encode_raw_v3(tags_dict, rating),
                    r["cached_at"],
                )
            except Exception as e:
                errors += 1
                log.warning(
                    f"Dict retrain: undecodable blob for post {r['post_id']}: {e}"
                )
        if errors:
            log.warning(
                f"Dict retrain: skipped {errors} undecodable blob(s) — they stay "
                f"on whatever dict they already referenced."
            )
        if not payloads:
            return None

        samples = [p for p, _ in payloads.values()]
        if DICT_TRAIN_SAMPLES and DICT_TRAIN_SAMPLES < len(samples):
            samples = random.sample(samples, DICT_TRAIN_SAMPLES)

        new_dict = None
        if len(samples) >= DICT_MIN_SAMPLES:
            try:
                t0 = time.time()
                new_dict = zstd.train_dictionary(DICT_SIZE, samples)
                log.info(
                    f"Dict retrain: trained {len(new_dict.as_bytes())}-byte "
                    f"dictionary from {len(samples)} sample(s) "
                    f"in {time.time() - t0:.1f}s."
                )
            except Exception as e:
                log.warning(
                    f"Dict retrain: training failed ({e}); going dictionary-less."
                )
        else:
            log.info(
                f"Dict retrain: only {len(samples)} sample(s) "
                f"(< {DICT_MIN_SAMPLES}), going dictionary-less."
            )

        cctx = zstd.ZstdCompressor(level=ZSTD_LEVEL, dict_data=new_dict)
        prefix = bytes([_BLOB_FMT_V3, 1])  # every rewritten blob is "current"

        rewritten = [
            (prefix + cctx.compress(raw), post_id)
            for post_id, (raw, _cached_at) in tqdm(
                payloads.items(),
                desc="Dict retrain: recompress",
                unit="blob",
                leave=False,
            )
        ]

        with db() as conn:
            if new_dict is not None:
                # Rotate: whatever was 'dict' becomes 'dict_old'. Any prior
                # 'dict_old' is dropped unconditionally here — safe because
                # every blob was either successfully decoded above (and is
                # about to be rewritten fresh against the new dict, below)
                # or failed to decode at all, meaning its zstd frame itself
                # is unreadable and no surviving dictionary would have
                # helped it anyway. So nothing can still depend on the
                # dictionary that was 'dict_old' before this call.
                conn.execute("DELETE FROM tag_dicts WHERE label = 'dict_old'")
                conn.execute(
                    "UPDATE tag_dicts SET label = 'dict_old' WHERE label = 'dict'"
                )
                conn.execute(
                    "INSERT INTO tag_dicts (label, dict_blob, trained_on, created_at) "
                    "VALUES ('dict', ?, ?, ?)",
                    (new_dict.as_bytes(), len(samples), int(time.time())),
                )
            conn.executemany(
                "UPDATE post_tags SET tags_blob = ? WHERE post_id = ?",
                rewritten,
            )

            # Anything that failed to decode above was left untouched, so it
            # still carries whatever flag/dict it already had — which, after
            # the rotation above, now points at 'dict_old'. Only drop
            # 'dict_old' if nothing still references it (flag byte 0).
            stranded = errors > 0 and new_dict is not None
            if stranded:
                log.warning(
                    f"Dict retrain: kept 'dict_old' — {errors} blob(s) could not "
                    f"be rewritten and still reference it. It stays until those "
                    f"blobs are refreshed or force-rewritten."
                )
            elif new_dict is not None:
                pruned = conn.execute(
                    "DELETE FROM tag_dicts WHERE label = 'dict_old'"
                ).rowcount
                if pruned:
                    log.info("Dict retrain: dropped unreferenced 'dict_old'.")

        # Force the manager to reload committed state on next use.
        _tag_dicts._loaded = False

        new_bytes = sum(len(b) for b, _ in rewritten)
        n = len(rewritten)
        stats = {
            "posts": n,
            "old_avg": old_bytes / n,
            "new_avg": new_bytes / n,
        }
        log.info(
            f"Dict retrain: rewrote {n} blob(s) "
            f"Avg bytes/post: {stats['old_avg']:.1f} -> {stats['new_avg']:.1f} "
            f"({(1 - new_bytes / old_bytes) * 100:.1f}% saved, "
            f"total {old_bytes} -> {new_bytes} bytes)."
        )
        return stats


# The tag backfill no longer runs on its own timer — it is the second half of
# _maintenance_pass(), which runs at startup and after each client rescan.


# ---------- Tag data: aliases, implications, tag list ----------
#
# e621 publishes a nightly CSV export of its tags, tag_aliases and
# tag_implications tables, plus a manifest (DB_EXPORT_MANIFEST) giving each
# file's SHA-256 and generation time. We poll the manifest, download only when
# a checksum has moved, verify what we get against that checksum, and ingest
# all three together. The graph lives as two zstd blobs in the single-row
# tag_graph table; the tag list lives in the relational `tags` table.
#
# All three are fetched together even when only one checksum moved. They are
# generated by the same nightly job and in practice always move as a set, and
# a partial ingest would be wrong anyway: the tags table is filtered by the
# alias map and implication edges are alias-resolved at both ends.
#
# What's stored:
#   aliases      — antecedent -> canonical consequent, chains already collapsed
#                  to a fixed point, so lookup is one dict hit with no walking.
#   implications — DIRECT EDGES ONLY (antecedent -> immediate consequent). The
#                  export also ships a `descendant_names` column holding the
#                  full transitive closure per row, but storing that would
#                  duplicate every shared ancestor across thousands of rows.
#                  Edges are the minimal representation; the closure is
#                  recovered by walking them, memoised per tag.
#
# Nothing is pruned to our corpus. An earlier version kept only the edges
# reachable from tags we had already seen, which meant a full decode of every
# blob in post_tags on every refresh and left the graph blind to anything new.
# Storing the whole thing costs a few MB on disk and makes reduce() strictly
# better informed — more redundant tags get dropped before they reach a blob.
#
# How it's used:
#   reduce()  at write time  — drop tags on a post that another of its tags
#                              already implies, so only the most specific
#                              survive into the blob.
#   expand()  at search time — walk back up so a query for `canine` still
#                              matches a post that only stores `fox`.


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

    def ancestors(self, tag):
        with self.lock:
            self._ensure_loaded_locked()
            return self._ancestors_locked(tag)

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
                ) as bar,
            ):
                for chunk in response.iter_content(1 << 20):
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


def _flatten_post_tags(tags_dict, rating):
    """Build the flat lowercase set of all tags + pseudo-tags for matching.

    Cached posts store only their most specific tags (see set_post_tag_cache),
    so the implication graph is walked upward here to put the implied ones
    back before matching. Doing it on the post side rather than expanding the
    query means wildcards and negations keep working unchanged: `-canine`
    still excludes a post that only stores `fox`.
    """
    all_tags = set()
    for category in (
        "general",
        "species",
        "character",
        "artist",
        "copyright",
        "meta",
        "lore",
        "invalid",
    ):
        for t in tags_dict.get(category, []):
            all_tags.add(t.lower())
    all_tags = _tag_graph.expand(all_tags)
    if rating:
        all_tags.add(f"rating:{rating}")
    return all_tags


def _parse_query_string(query):
    """Parse an e621-style query string into a list of (pattern, negated) terms.

    Same shape as the blacklist parser, but applies to a single query rather
    than multi-line clauses.
    """
    terms = []
    for token in query.split():
        token = token.lower().strip()
        if not token:
            continue
        negated = token.startswith("-")
        if negated:
            token = token[1:]
        if token:
            terms.append((token, negated))
    return terms


# Metatags that answer from post_relations / the post ID rather than from the
# tag set. Review mode filters against the local cache, so without these a
# search like `pool:52413` is matched as if "pool:52413" were a literal tag --
# it never matches anything and the review list comes back empty.
_RELATION_PREFIXES = (
    "parent:",
    "child:",
    "pool:",
    "id:",
    "status:",
    "ischild:",
    "hasparent:",
    "isparent:",
    "haschild:",
    "haschildren:",
)

# Boolean forms and what they assert. True = "must have the relation".
_RELATION_FLAGS = {
    "ischild": "parent",
    "hasparent": "parent",
    "isparent": "children",
    "haschild": "children",
    "haschildren": "children",
}

_EMPTY_RELATIONS = {"parent_id": None, "children": [], "pools": []}


def is_relation_term(pattern):
    """True if `pattern` is one of the metatags handled by relation matching."""
    return pattern.startswith(_RELATION_PREFIXES)


def get_relations_map(post_ids):
    """Bulk-load relations for many posts at once: {post_id: relations}.

    Posts with no row are omitted -- callers treat a miss as "no relations
    known", which _relation_term_matches() handles via _EMPTY_RELATIONS.
    """
    out = {}
    ids = list(post_ids)
    if not ids:
        return out
    CHUNK = 900  # stay under SQLite's variable limit
    with db() as conn:
        for start in range(0, len(ids), CHUNK):
            batch = ids[start : start + CHUNK]
            placeholders = ",".join("?" for _ in batch)
            rows = conn.execute(
                f"SELECT * FROM post_relations WHERE post_id IN ({placeholders})",
                batch,
            ).fetchall()
            for row in rows:
                out[row["post_id"]] = {
                    "parent_id": row["parent_id"],
                    "children": _parse_csv_ids(row["children"]),
                    "pools": _parse_csv_ids(row["pools"]),
                }
    return out


def _relation_term_matches(pattern, post_id, rel):
    """Evaluate one relation metatag against a post. Returns True/False.

    `rel` is a relations dict (or None, treated as "no relations known").
    Anything unparseable evaluates False rather than raising -- a malformed
    metatag should narrow the search, not blow up the request.
    """
    rel = rel or _EMPTY_RELATIONS
    name, _, value = pattern.partition(":")

    if name in _RELATION_FLAGS:
        field = _RELATION_FLAGS[name]
        has = bool(rel["parent_id"]) if field == "parent" else bool(rel["children"])
        return has if value == "true" else (not has) if value == "false" else False

    if name == "status":
        # No local equivalent -- every cached post is a live post. Treated as
        # satisfied so `status:any` (appended by child: expansion) is a no-op
        # here instead of emptying the list.
        return True

    if name == "id":
        wanted = {int(t) for t in value.split(",") if t.strip().isdigit()}
        return post_id in wanted

    if name == "parent":
        if value == "none":
            return rel["parent_id"] is None
        if value == "any":
            return rel["parent_id"] is not None
        return value.isdigit() and rel["parent_id"] == int(value)

    if name == "child":
        # Mirror of parent:, same as the API-side metatag -- child:<id>
        # selects the post that IS the parent of <id>. Answered straight
        # from the stored child list, no lookup needed.
        if value == "none":
            return not rel["children"]
        if value == "any":
            return bool(rel["children"])
        return value.isdigit() and int(value) in rel["children"]

    if name == "pool":
        if value == "none":
            return not rel["pools"]
        if value == "any":
            return bool(rel["pools"])
        return value.isdigit() and int(value) in rel["pools"]

    return False


def _get_local_favorite_ids():
    """Return the set of post IDs currently in the local favorites table."""
    with db() as conn:
        rows = conn.execute("SELECT post_id FROM favorites").fetchall()
    return {r["post_id"] for r in rows}


def _post_matches_query_with_favs(
    tags_dict, rating, post_id, query, favorite_ids, relations=None
):
    """Internal matcher that takes a pre-fetched favorite_ids set.

    `relations` is the post's row from post_relations (see get_relations_map).
    It's only consulted when the query actually contains a relation metatag;
    passing None means "none known", which makes parent:/child:/pool: terms
    evaluate False rather than silently matching.
    """
    terms = _parse_query_string(query)
    if not terms:
        return True

    all_tags = _flatten_post_tags(tags_dict, rating)
    fav_terms = [(pat, neg) for (pat, neg) in terms if pat.startswith("fav:")]
    rel_terms = [(pat, neg) for (pat, neg) in terms if is_relation_term(pat)]
    regular_terms = [
        (pat, neg)
        for (pat, neg) in terms
        if not pat.startswith("fav:") and not is_relation_term(pat)
    ]

    if fav_terms:
        is_favorited = post_id in favorite_ids
        for _, negated in fav_terms:
            if is_favorited == negated:
                return False

    for pattern, negated in rel_terms:
        if _relation_term_matches(pattern, post_id, relations) == negated:
            return False

    for pattern, negated in regular_terms:
        matches = _tag_matches(pattern, all_tags)
        if matches == negated:
            return False

    return True


def post_matches_query(tags_dict, rating, post_id, query):
    """Check if a post matches an e621-style query string.

    Supports positive/negated tags, rating:s/q/e, wildcards, fav:anyone.
    fav: terms are resolved against the local favorites table.
    For bulk matching, use _post_matches_query_with_favs with a pre-fetched set.
    """
    fav_ids = set()
    terms = _parse_query_string(query)
    if any(pat.startswith("fav:") for pat, _ in terms):
        fav_ids = _get_local_favorite_ids()
    relations = (
        get_post_relations(post_id)
        if any(is_relation_term(pat) for pat, _ in terms)
        else None
    )
    return _post_matches_query_with_favs(
        tags_dict, rating, post_id, query, fav_ids, relations
    )


def _relations_payload(post):
    """Relationship block for the frontend.

    Read straight off the fetched post -- it's authoritative and already in
    hand. Falls back to the stored row only if the response somehow lacks
    the fields (an abbreviated payload from a cached path).
    """
    pid = post["id"]
    if "relationships" in post or "pools" in post:
        parent_id, children, pools = extract_relations(post)
        rel = {"parent_id": parent_id, "children": sorted(set(children)),
               "pools": sorted(set(pools))}
    else:
        rel = get_post_relations(pid) or {
            "parent_id": None, "children": [], "pools": []
        }
    return {
        "parent_id": rel["parent_id"],
        "children": rel["children"],
        "pools": rel["pools"],
        "searches": relations_search_tags(pid, rel),
    }


def _build_post_response(post, query, known, from_primed):
    """Build the JSON payload for a successfully-found post.

    Note: this does NOT mark the post as seen. Marking-seen is now an
    explicit action via /api/seen, called by the frontend when the post
    actually transitions to being displayed.

    We DO cache the post's tags here — every fetched post updates the local
    tag cache, which is used by review-mode override filtering.
    """
    cache_post_from_response(post)

    file_info = post_file(post)
    tags = post.get("tags", {})
    artists = tags.get("artist", [])
    characters = tags.get("character", [])

    with db() as conn:
        fav_row = conn.execute(
            "SELECT 1 FROM favorites WHERE post_id = ?", (post["id"],)
        ).fetchone()

    is_favorited = post_is_favorited(post) or fav_row is not None

    return {
        "id": post["id"],
        "query": query,
        "file_url": file_info["url"],
        "sample_url": post_sample_url(post),
        "ext": file_info["ext"],
        "width": file_info["width"],
        "height": file_info["height"],
        "is_favorited": is_favorited,
        "from_primed": from_primed,
        "artists": [
            {"tag": a, "known": a.lower() in known}
            for a in artists
            if a not in ("conditional_dnp",)
        ],
        "characters": [{"tag": c, "known": c.lower() in known} for c in characters],
        "rating": post.get("rating"),
        "score": post_score(post),
        "post_url": f"https://e621.net/posts/{post['id']}",
        "relations": _relations_payload(post),
    }


def _try_pool(pool, seen, blacklist, persist, known, from_primed, ordered=False):
    """Sample queries from a pool; return response dict or None.

    ordered=False (general pool): pick up to 5 queries at RANDOM.
    ordered=True  (primed pool):  try queries in the given (priority) order,
        up to MAX_PRIMED_ATTEMPTS, returning the first that yields. Stable
        ordering means the top primed query is retried on consecutive requests
        until its new content is drained, instead of bouncing randomly.

    decrement_primed_count is NOT called here — deferred to /api/seen.
    """
    if ordered:
        for query in pool[:MAX_PRIMED_ATTEMPTS]:
            result = _find_unseen_post(query, seen, blacklist, persist=persist)
            if result is not None:
                post, page = result
                return _build_post_response(post, query, known, from_primed)
        return None

    tried = set()
    while len(tried) < min(len(pool), 5):
        query = random.choice(pool)
        if query in tried:
            continue
        tried.add(query)
        result = _find_unseen_post(query, seen, blacklist, persist=persist)
        if result is not None:
            post, page = result
            return _build_post_response(post, query, known, from_primed)
    return None


@app.route("/api/next")
def api_next():
    """Pick a query, fetch posts, return the first unseen one.

    Query params:
        override: If set, use this query instead of sampling from queries.txt.
                  May be a full e621 URL or raw tag string.
        persist:  '1' or '0'. When override is used and persist=0, skip writing
                  query_progress (last_page, exhausted). Default 0.

    Selection priority: primed queries (those with new posts found by the
    background scanner) are served exclusively until exhausted, then the
    full query pool is sampled randomly.
    """
    override = request.args.get("override", "").strip()
    persist = request.args.get("persist", "0") == "1"

    # Parse override if it's a URL (with or without scheme, any case)
    _low = override.lower()
    _looks_like_url = (
        _low.startswith("http")
        or _low.startswith("e621.net")
        or _low.startswith("www.e621.net")
        or _low.startswith("e926.net")
    )
    if _looks_like_url:
        # Normalize scheme-less URLs so urlparse populates netloc/path correctly.
        to_parse = override if _low.startswith("http") else "https://" + override
        parsed = urllib.parse.urlparse(to_parse)
        params = urllib.parse.parse_qs(parsed.query)
        tags = params.get("tags", [None])[0]

        if tags:
            # Search URL: /posts?tags=...
            override = tags
        else:
            # Post permalink: /posts/<id>  ->  override on that single post id
            m = re.search(r"/posts/(\d+)", parsed.path)
            if m:
                override = "id:" + m.group(1) + " status:any"
            else:
                override = ""
        persist = False  # URL-derived overrides never touch the exhausted list

    override = override.lower()
    if override:
        expanded = expand_child_metatag(override)
        if expanded != override:
            # Resolved to a concrete id: search -- never persist page
            # progress against it, the tag string is synthetic.
            override = expanded
            persist = False

    if override:
        queries = [override]
        primed_pool = []
    else:
        queries = load_queries()
        active_set = set(queries)
        primed_pool = [q for q in get_primed_queries() if q in active_set]
        persist = True

    if not queries:
        return jsonify({
            "error": "No queries loaded. Add URLs to queries.txt or set an override."
        }), 400

    seen = get_seen_ids()
    # Allow caller to exclude additional IDs (e.g. a post they've already
    # buffered but not yet marked seen — prevents serving the same post twice)
    exclude_param = request.args.get("exclude", "")
    if exclude_param:
        for tok in exclude_param.split(","):
            tok = tok.strip()
            if tok.isdigit():
                seen.add(int(tok))

    known = load_known_tags()
    blacklist = load_blacklist()

    # Try primed queries first
    if primed_pool:
        log.info(f"Primed pool active: {len(primed_pool)} querie(s) with new content.")
        response = _try_pool(
            primed_pool, seen, blacklist, persist, known, from_primed=True, ordered=True
        )
        if response is not None:
            return jsonify(response)
        log.info(
            "Primed pool yielded nothing this attempt; falling back to general pool."
        )

    # Fall back to the general pool
    response = _try_pool(queries, seen, blacklist, persist, known, from_primed=False)
    if response is not None:
        return jsonify(response)

    return jsonify({
        "error": "All sampled queries returned only seen or blacklisted posts. Try again or add queries."
    }), 404


@app.route("/api/seen", methods=["POST"])
def api_seen():
    """Mark a post as seen. Called by the frontend when the post is actually
    displayed (i.e. the user has transitioned to viewing it).
    """
    post_id = request.json.get("post_id")
    query = request.json.get("query")  # optional; used to decrement primed counter
    from_primed = request.json.get("from_primed", False)
    if not post_id:
        return jsonify({"error": "Missing post_id"}), 400
    mark_post_seen(post_id)
    if from_primed and query:
        decrement_primed_count(query)
    return jsonify({"ok": True})


def _build_review_list(override_tags=None):
    """Build the ordered list of seen post IDs for review mode.

    Ordering: chronological (seen_at ASC, oldest first).

    If override_tags is provided, filter using the LOCAL tag cache rather
    than searching e621. Posts whose tags aren't cached yet are excluded
    (they'll become reviewable once their cache is populated by display) --
    unless every term answers from another table (fav:, parent:, child:,
    pool:), in which case the tag cache isn't needed at all.

    Returns:
        ids: filtered post IDs (or all seen if no override)
        uncached: number of seen posts excluded due to missing cache
    """
    with db() as conn:
        seen_rows = conn.execute(
            "SELECT post_id FROM seen ORDER BY seen_at ASC"
        ).fetchall()
    seen_ordered = [r["post_id"] for r in seen_rows]

    if not override_tags:
        return {"ids": seen_ordered, "uncached": 0}

    if not seen_ordered:
        return {"ids": [], "uncached": 0}

    placeholders = ",".join("?" for _ in seen_ordered)
    with db() as conn:
        rows = conn.execute(
            f"SELECT post_id, tags_blob FROM post_tags "
            f"WHERE post_id IN ({placeholders})",
            seen_ordered,
        ).fetchall()

    cache_map = {}
    for row in rows:
        try:
            tags_dict, rating = decode_tags(bytes(row["tags_blob"]))
        except Exception:
            continue
        cache_map[row["post_id"]] = (tags_dict, rating)

    # Pre-fetch favorite IDs if the override query uses fav: terms
    # so we don't hit the DB once per post in the matching loop.
    terms = _parse_query_string(override_tags)
    uses_fav = any(pat.startswith("fav:") for pat, _ in terms)
    favorite_ids = _get_local_favorite_ids() if uses_fav else set()

    # Same idea as the favorites pre-fetch: one bulk read instead of a query
    # per post. Skipped entirely unless the override actually uses parent:,
    # child:, pool: or friends.
    uses_relations = any(is_relation_term(pat) for pat, _ in terms)
    relations_map = get_relations_map(seen_ordered) if uses_relations else {}

    matched = []
    uncached = 0

    # Terms that actually require the tag cache. fav: and the relation
    # metatags answer from their own tables, so a query made only of those
    # (`pool:52413`, `parent:any`) can be evaluated for a post whose tags
    # were never cached -- it shouldn't be counted as uncached and dropped.
    tag_terms = [
        (pat, neg)
        for (pat, neg) in terms
        if not pat.startswith("fav:") and not is_relation_term(pat)
    ]

    for pid in seen_ordered:
        cached = cache_map.get(pid)
        if cached is None:
            if tag_terms:
                # Can't evaluate tag-dependent terms without cache
                uncached += 1
                continue
            else:
                # fav:/relation-only query: match using an empty tag dict
                tags, rating = {}, None
        else:
            tags, rating = cached
        if _post_matches_query_with_favs(
            tags, rating, pid, override_tags, favorite_ids, relations_map.get(pid)
        ):
            matched.append(pid)

    return {"ids": matched, "uncached": uncached}


@app.route("/api/review/list")
def api_review_list():
    """Return the ordered list of seen post IDs for review mode.

    Query params:
        override: Optional tag string or e621 URL to filter the seen set.

    Response:
        {
            "count": N,
            "ids": [post_id, ...],
            "uncached": <number of seen posts excluded due to missing tag cache>
        }

    The uncached count tells the frontend how many seen posts will become
    reviewable once their tag cache is populated through normal browsing.
    """
    override = request.args.get("override", "").strip()
    if override and override.startswith("http"):
        parsed = urllib.parse.urlparse(override)
        params = urllib.parse.parse_qs(parsed.query)
        tags = params.get("tags", [None])[0]
        override = tags or ""

    override = override.lower()

    result = _build_review_list(override or None)
    return jsonify({
        "count": len(result["ids"]),
        "ids": result["ids"],
        "uncached": result["uncached"],
    })


@app.route("/api/review", methods=["GET", "POST"])
def api_review():
    """Review mode: serve a specific seen post by ID.

    The frontend manages index navigation locally (using the list returned
    by /api/review/list) and tells us which post to fetch via post_id.

    Accepts post_id either as a query param or in the JSON body.
    """
    post_id_param = None
    if request.method == "POST":
        body = request.get_json(silent=True) or {}
        post_id_param = body.get("post_id")
    if post_id_param is None:
        post_id_param = request.args.get("post_id")

    if post_id_param is None:
        return jsonify({"error": "post_id is required"}), 400

    try:
        post_id = int(post_id_param)
    except (TypeError, ValueError):
        return jsonify({"error": "post_id must be an integer"}), 400

    try:
        post = _fetch_post_by_id(post_id)
    except requests.RequestException as e:
        return jsonify({"error": f"Failed to fetch post: {e}"}), 502

    if post is None:
        return jsonify({"error": f"Post #{post_id} no longer exists on e621."}), 404
    if not post_is_viewable(post):
        return jsonify({"error": f"Post #{post_id} has no viewable file."}), 404

    known = load_known_tags()
    response = _build_post_response(post, "(review)", known, from_primed=False)
    response["is_review"] = True
    return jsonify(response)


def _fetch_post_by_id(post_id):
    """Fetch a single post's full data from e621 by ID. Returns None on 404."""
    rate_limit()
    response = _session.get(
        f"https://e621.net/posts/{post_id}.json",
        params=_v2_params({}),
        headers={"User-Agent": USER_AGENT},
        auth=_auth(),
        timeout=15,
    )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    payload = response.json()
    # v2 returns a bare object; the wrapped form is tolerated defensively.
    if isinstance(payload, dict) and "post" in payload:
        payload = payload["post"]
    return payload


def _step_history(current_post_id, direction):
    """Find the previous or next post in seen-history relative to the given post.

    direction: 'back' (older) or 'forward' (newer).
    Returns the neighbor's post_id and seen_at, or None if at the boundary.
    """
    with db() as conn:
        # First find the current post's seen_at timestamp
        if current_post_id is None:
            # No current — start at the newest seen post
            row = conn.execute(
                "SELECT post_id, seen_at FROM seen ORDER BY seen_at DESC LIMIT 1"
            ).fetchone()
            return dict(row) if row else None

        ref = conn.execute(
            "SELECT seen_at FROM seen WHERE post_id = ?", (current_post_id,)
        ).fetchone()
        if not ref:
            return None
        ref_ts = ref["seen_at"]

        if direction == "back":
            # Find the most recent post older than ref_ts
            row = conn.execute(
                "SELECT post_id, seen_at FROM seen "
                "WHERE seen_at < ? ORDER BY seen_at DESC LIMIT 1",
                (ref_ts,),
            ).fetchone()
        elif direction == "forward":
            # Find the next post newer than ref_ts
            row = conn.execute(
                "SELECT post_id, seen_at FROM seen "
                "WHERE seen_at > ? ORDER BY seen_at ASC LIMIT 1",
                (ref_ts,),
            ).fetchone()
        else:
            return None

        return dict(row) if row else None


@app.route("/api/previous")
def api_previous():
    """Step back one post in seen-history.

    Query params:
        from_id: The current post ID to step back from. If omitted, returns
                 the most recently seen post (entry point into history).
    """
    from_id = request.args.get("from_id")
    from_id = int(from_id) if from_id and from_id.isdigit() else None

    neighbor = _step_history(from_id, "back")
    if not neighbor:
        return jsonify({"error": "No earlier post in history."}), 404

    try:
        post = _fetch_post_by_id(neighbor["post_id"])
    except requests.RequestException as e:
        return jsonify({"error": f"Failed to fetch post: {e}"}), 502
    if not post:
        return jsonify({
            "error": f"Post #{neighbor['post_id']} no longer exists on e621."
        }), 404

    known = load_known_tags()
    response = _build_post_response(post, "(history)", known, from_primed=False)
    response["is_history"] = True
    response["seen_at"] = neighbor["seen_at"]
    return jsonify(response)


@app.route("/api/history_forward")
def api_history_forward():
    """Step forward one post in seen-history (the inverse of /api/previous).

    Returns the next-newer seen post, or 404 if already at the head.
    The frontend uses 404 as the signal to fall back to /api/next for
    fresh content.
    """
    from_id = request.args.get("from_id")
    from_id = int(from_id) if from_id and from_id.isdigit() else None

    if from_id is None:
        return jsonify({"error": "Missing from_id"}), 400

    neighbor = _step_history(from_id, "forward")
    if not neighbor:
        return jsonify({"error": "Already at most recent."}), 404

    try:
        post = _fetch_post_by_id(neighbor["post_id"])
    except requests.RequestException as e:
        return jsonify({"error": f"Failed to fetch post: {e}"}), 502
    if not post:
        return jsonify({
            "error": f"Post #{neighbor['post_id']} no longer exists on e621."
        }), 404

    known = load_known_tags()
    response = _build_post_response(post, "(history)", known, from_primed=False)
    response["is_history"] = True
    response["seen_at"] = neighbor["seen_at"]
    return jsonify(response)


@app.route("/api/favorite", methods=["POST"])
def api_favorite():
    data = request.json
    post_id = data["post_id"]

    e621_synced = False
    if _auth():
        try:
            e621_favorite(post_id)
            e621_synced = True
        except requests.HTTPError as e:
            log.warning(f"e621 favorite failed for post {post_id}: {e}")
            return jsonify({
                "ok": False,
                "reason": f"e621 API error: {e.response.status_code}",
            }), 502
    else:
        log.warning("No e621 credentials set — favorite stored locally only.")

    with db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO favorites (post_id, favorited_at) VALUES (?, ?)",
            (post_id, int(time.time())),
        )
    return jsonify({"ok": True, "e621_synced": e621_synced})


@app.route("/api/unfavorite", methods=["POST"])
def api_unfavorite():
    data = request.json
    post_id = data["post_id"]

    e621_synced = False
    if _auth():
        try:
            e621_unfavorite(post_id)
            e621_synced = True
        except requests.HTTPError as e:
            log.warning(f"e621 unfavorite failed for post {post_id}: {e}")
            return jsonify({
                "ok": False,
                "reason": f"e621 API error: {e.response.status_code}",
            }), 502

    with db() as conn:
        conn.execute("DELETE FROM favorites WHERE post_id = ?", (post_id,))
    return jsonify({"ok": True, "e621_synced": e621_synced})


@app.route("/api/addition", methods=["POST"])
def api_addition():
    """Add a tag to the additions list (unless already known).

    The tag is mapped through the alias table first, so clicking a chip that
    e621 has since renamed files the canonical name rather than a dead one.
    """
    data = request.json
    tag = _tag_graph.canonical(data["tag"].strip().lower())
    category = data["category"]
    known = load_known_tags()
    if tag in known:
        return jsonify({"ok": False, "reason": "already_known"})
    with db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO additions (tag, category, added_at) VALUES (?, ?, ?)",
            (tag, category, int(time.time())),
        )
    # Also append to the appropriate text file
    append_to_additions_file(tag, category)
    return jsonify({"ok": True})


@app.route("/api/addition/remove", methods=["POST"])
def api_addition_remove():
    """Remove a tag from the additions list (DB + text file)."""
    data = request.json
    tag = data["tag"].strip().lower()
    category = data["category"]
    with db() as conn:
        conn.execute(
            "DELETE FROM additions WHERE tag = ? AND category = ?",
            (tag, category),
        )
    remove_from_additions_file(tag, category)
    return jsonify({"ok": True})


@app.route("/api/stats")
def api_stats():
    with db() as conn:
        seen_count = conn.execute("SELECT COUNT(*) FROM seen").fetchone()[0]
        fav_count = conn.execute("SELECT COUNT(*) FROM favorites").fetchone()[0]
        add_count = conn.execute("SELECT COUNT(*) FROM additions").fetchone()[0]
        exhausted_count = conn.execute(
            "SELECT COUNT(*) FROM query_progress WHERE exhausted = 1"
        ).fetchone()[0]
        rel_parent_count = conn.execute(
            "SELECT COUNT(*) FROM post_relations WHERE parent_id IS NOT NULL"
        ).fetchone()[0]
        rel_child_count = conn.execute(
            "SELECT COUNT(*) FROM post_relations WHERE children != ''"
        ).fetchone()[0]
        rel_pool_count = conn.execute(
            "SELECT COUNT(*) FROM post_relations WHERE pools != ''"
        ).fetchone()[0]
        primed_count = conn.execute(
            "SELECT COALESCE(SUM(new_posts_found), 0) FROM query_progress WHERE exhausted = 0 AND new_posts_found > 0"
        ).fetchone()[0]
    tag_count = _tag_store.count()
    return jsonify({
        "seen": seen_count,
        "favorites": fav_count,
        "additions": add_count,
        "queries": len(load_queries()),
        "blacklist": len(load_blacklist()),
        "exhausted": exhausted_count,
        "primed": primed_count,
        "tags": tag_count,
        "relations": {
            "with_parent": rel_parent_count,
            "with_children": rel_child_count,
            "in_pool": rel_pool_count,
        },
        "tag_graph": _tag_graph.info(),
        "tag_store": _tag_store.info(),
    })


@app.route("/api/relations/<int:post_id>")
def api_relations(post_id):
    """Stored parent/child/pool info for a post, plus ready-made searches.

    Returns 404 if the post has never been looked up -- that's distinct from
    a post known to have no relations, which returns nulls and empty lists.
    """
    rel = get_post_relations(post_id)
    if rel is None:
        return jsonify({"error": f"No relationship data for post {post_id}."}), 404
    return jsonify({
        "post_id": post_id,
        "parent_id": rel["parent_id"],
        "children": rel["children"],
        "pools": rel["pools"],
        "searches": relations_search_tags(post_id, rel),
        "updated_at": rel["updated_at"],
    })


@app.route("/api/tag_graph")
def api_tag_graph():
    """Report what alias/implication export is currently loaded."""
    return jsonify(_tag_graph.info())


@app.route("/api/tag_graph/refresh", methods=["POST"])
def api_tag_graph_refresh():
    """Re-download and re-prune the alias/implication graph in the background.

    Pass {"force": true} to re-ingest even if the newest export is one we've
    already stored — useful after the corpus has grown, since pruning is
    relative to the tags this database references.
    """
    force = bool((request.get_json(silent=True) or {}).get("force"))

    def _do_refresh():
        try:
            refresh_tag_graph(force=force)
        except Exception as e:
            log.error(f"Tag graph refresh error: {e}")

    threading.Thread(target=_do_refresh, daemon=True).start()
    return jsonify({"ok": True, "message": "Tag graph refresh started."})


@app.route("/api/additions")
def api_additions_list():
    with db() as conn:
        rows = conn.execute(
            "SELECT tag, category, added_at FROM additions ORDER BY added_at DESC"
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/favorites")
def api_favorites_list():
    with db() as conn:
        rows = conn.execute(
            "SELECT post_id, favorited_at FROM favorites ORDER BY favorited_at DESC"
        ).fetchall()
    return jsonify([dict(r) for r in rows])


# ---------- Tag completion ----------

# Metatags that take a tag name as their value, so completion still applies
# after the colon. Everything else with a colon (score:, rating:, order:, ...)
# takes a number, a keyword or a range, and is left alone.
_COMPLETABLE_METATAGS = ("fav:", "pool:", "set:", "user:", "voted:")

SUGGEST_LIMIT = 12


@app.route("/api/tags/suggest")
def api_tags_suggest():
    """Rank tag completions for a single fragment.

    One tier: real tags whose name starts with the fragment (`wol` -> `wolf`,
    a bisect into the chunked tag store) merged with aliases whose antecedent
    starts with it (`homo` -> `male/male`), the whole pool ranked by post_count
    descending.

    Aliases are merged rather than appended because a popular alias must be
    able to outrank literal prefix matches: a fragment like `homo` matches a
    dozen low-count real tags, and appending would push `male/male` (662k
    posts) off the end of the list every time.

    There used to be a second tier that matched the fragment anywhere in the
    name (`wolf` -> `grey_wolf`). It was removed: it is blind substring
    matching, which only coincidentally overlaps the implication graph — about
    a third of implication pairs share a substring, so it missed the other
    two thirds while surfacing unrelated collisions — and the post_count index
    it needed cost 17.8 MB, more than the entire compressed tag store.

    An alias hit carries `alias_of` (the spelling that was typed); `name` is
    always the canonical tag, which is what gets inserted on accept.
    """
    fragment = request.args.get("q", "").strip().lower()
    try:
        limit = min(int(request.args.get("limit", SUGGEST_LIMIT)), 50)
    except ValueError:
        limit = SUGGEST_LIMIT

    if not fragment or "*" in fragment:
        # A wildcard is already a search operator — completing it would only
        # narrow something the user deliberately left broad.
        return jsonify({"query": fragment, "suggestions": []})

    prefix = ""
    if ":" in fragment:
        for meta in _COMPLETABLE_METATAGS:
            if fragment.startswith(meta):
                prefix, fragment = meta, fragment[len(meta) :]
                break
        else:
            return jsonify({"query": fragment, "suggestions": []})
        if not fragment:
            return jsonify({"query": prefix, "suggestions": []})

    seen = set()
    out = []

    def add(name, category, post_count, alias_of=None):
        if name in seen or len(out) >= limit:
            return
        seen.add(name)
        out.append({
            "name": prefix + name,
            "tag": name,
            "category": category,
            "category_name": TAG_CATEGORIES[category]
            if 0 <= category < len(TAG_CATEGORIES)
            else "general",
            "post_count": post_count,
            "alias_of": alias_of,
        })

    pool = []  # (post_count, name, category, alias_of)

    for name, category, post_count in _tag_store.prefix(fragment, limit):
        pool.append((post_count, name, category, None))

    # Dead spellings that resolve into the tag list. Pulled with the same cap
    # as the prefix pass so one crowded fragment can't starve either source
    # before the merge ranks them.
    for dead, live in _tag_graph.alias_prefix(fragment, limit):
        hit = _tag_store.get(live)
        if hit:
            pool.append((hit[1], live, hit[0], dead))

    # Sort by count desc, then put non-alias rows first so a tag that is both a
    # literal prefix match and an alias target shows as itself. add() dedupes
    # on name, so whichever form sorts first is the one kept.
    pool.sort(key=lambda r: (-r[0], r[3] is not None, r[1]))
    for post_count, name, category, alias_of in pool:
        add(name, category, post_count, alias_of=alias_of)

    return jsonify({"query": prefix + fragment, "suggestions": out})


@app.route("/api/ping")
def api_ping():
    return jsonify({"ok": True, "status": _server_state["status"]})


def rebuild_tag_data(allow_download=True):
    """Re-ingest the tag exports, then retrain the dictionary — in that order.

    These two always belong together: retrain re-reduces every blob against
    the current implication graph, so running it before a graph refresh just
    means doing it again afterwards. Bundled here so the CLI flag and the
    force-rescan route share one definition of "in that order".

    `allow_download=False` reuses whatever dumps are already sitting in ROOT
    instead of fetching fresh ones.

    Exposed as `--rebuild-tag-data` and used by the force-rescan route; there
    is deliberately no way to run just one half from the command line.

    Returns {"graph": <graph result or None>, "dict": <retrain stats or None>}.
    The graph refresh is forced, so a None there means the dumps could not be
    obtained at all; a None dict means there was nothing cached to rewrite.
    Neither is an error, and the retrain runs either way — it still re-reduces
    against whatever graph is currently stored.
    """
    graph = refresh_tag_graph(force=True, allow_download=allow_download)
    log.info(f"Tag graph refresh result: {graph}")
    stats = retrain_tag_dict()
    if stats is None:
        log.info("Dict retrain: nothing to do.")
    # Both halves have finished churning — the tags table was rewritten
    # wholesale and every blob in post_tags was replaced. This is the point
    # with the most free space to reclaim.
    run_vacuum()
    return {"graph": graph, "dict": stats}


@app.route("/api/force_rescan", methods=["POST"])
def api_force_rescan():
    """Clear page1 cache, trigger a scan pass, re-prune the tag graph, and
    retrain the dictionary.

    The full tag-cache refresh is deliberately NOT part of this — it's the
    heaviest API sweep in the app and is only reachable via
    `--refresh-tags` on the command line.
    """
    cleared = clear_all_page1_cache()
    log.info(f"Force rescan: cleared page1 cache for {cleared} queries.")

    def _do_scan():
        try:
            _scan_exhausted_queries()
        except Exception as e:
            log.error(f"Force rescan error: {e}")
        # Re-run maintenance in order: favorites sync first, then backfill.
        _maintenance_pass()
        try:
            rebuild_tag_data()
        except Exception as e:
            log.error(f"Force rescan tag graph/dict error: {e}")

    threading.Thread(target=_do_scan, daemon=True).start()
    return jsonify({
        "ok": True,
        "message": "Rescan + tag graph + dict retrain started.",
        "cleared": cleared,
    })


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="e6 curator — backlog management tool")
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host interface to bind (default: 0.0.0.0 — all interfaces).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8080,
        help="Port to listen on (default: 8080).",
    )
    parser.add_argument(
        "--dict-samples",
        type=int,
        default=0,
        help="Number of posts to sample for zstd dictionary training "
        "(default: 0 = use the entire corpus).",
    )
    parser.add_argument(
        "--refresh-tags",
        action="store_true",
        help="Re-fetch tags for every locally-referenced post (seen and "
        "favorites), overwriting the cache and purging posts confirmed gone "
        "from e621, then exit. Heavy — one API call per 320 posts. Resumes an "
        "interrupted sweep unless --no-resume is given.",
    )
    parser.add_argument(
        "--vacuum",
        action="store_true",
        help="Reclaim free pages and exit. Runs automatically after "
        "--rebuild-tag-data and --refresh-tags; this is the manual handle for "
        "everything else. Never runs while the server is up.",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Don't resume an interrupted tag refresh. On normal startup this "
        "discards the saved checkpoint and skips the resume entirely; with "
        "--refresh-tags it discards it and sweeps from the top.",
    )
    parser.add_argument(
        "--rebuild-tag-data",
        action="store_true",
        help="Re-ingest the tags/alias/implication exports, then retrain the "
        "zstd dictionary and rewrite every tag blob against it, then exit. The "
        "two halves are always run together — the retrain re-reduces tags "
        "using the graph, so it only means anything after a refresh.",
    )
    args = parser.parse_args()
    DICT_TRAIN_SAMPLES = max(0, args.dict_samples)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=[TqdmLoggingHandler()],
    )

    if args.vacuum:
        init_db()
        run_vacuum()
        raise SystemExit(0)

    if args.rebuild_tag_data:
        init_db()
        try:
            result = rebuild_tag_data()
        except Exception as e:
            log.error(f"Tag data rebuild failed: {e}")
            raise SystemExit(1)
        log.info(f"Tag data rebuild complete: {result}")
        raise SystemExit(0)

    if args.refresh_tags:
        init_db()
        try:
            result = refresh_tag_cache(resume=not args.no_resume)
        except Exception as e:
            log.error(f"Tag refresh failed: {e}")
            raise SystemExit(1)
        log.info(f"Tag refresh result: {result}")
        run_vacuum()
        raise SystemExit(0)

    init_db()
    resume_interrupted_refresh(cancel=args.no_resume)
    reconcile_additions_files()
    check_blacklist_change()
    start_maintenance(first=True)
    start_tag_graph_sync()
    app.run(host=args.host, port=args.port, debug=False)
