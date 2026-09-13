import random
import threading

import zstandard as zstd

from .config import (
    _BITS_RATING,
    _BLOB_FMT_V3,
    _RATING_BITS,
    _RATING_MASK,
    DICT_HOLDOUT_FRACTION,
    DICT_MIN_SAMPLES,
    DICT_SEARCH_MAX_BYTES,
    DICT_SEARCH_MIN_BYTES,
    TAG_CATEGORIES,
    ZSTD_LEVEL,
)
from .database import db
from .runtime import log


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


def train_best_dict(samples, label):
    """Train a zstd dictionary sized to minimize total storage for `samples`.

    Each sample is compressed on its own, as it is stored. Candidate sizes
    double from DICT_SEARCH_MIN_BYTES, capped by DICT_SEARCH_MAX_BYTES and by
    a tenth of the training bytes (zstd's own guidance is ~100x). Every
    candidate is trained without the held-out slice and scored as

        dict bytes + held-out compressed bytes scaled to the full corpus

    so a dictionary only wins if it pays for itself. Scoring against samples
    the dictionary never saw is what stops the search from rewarding a
    dictionary that simply memorized its input. "No dictionary" competes too.

    The search stops once two sizes in a row fail to improve. The winning
    size is retrained on every sample. Returns a ZstdCompressionDict, or None
    to go dictionary-less.
    """
    if len(samples) < DICT_MIN_SAMPLES:
        log.info(
            f"{label}: only {len(samples)} sample(s) "
            f"(< {DICT_MIN_SAMPLES}), going dictionary-less."
        )
        return None

    order = list(range(len(samples)))
    random.Random(0).shuffle(order)
    n_hold = max(1, int(len(samples) * DICT_HOLDOUT_FRACTION))
    holdout = [samples[i] for i in order[:n_hold]]
    train = [samples[i] for i in order[n_hold:]]
    total_bytes = sum(len(s) for s in samples)
    hold_bytes = sum(len(s) for s in holdout)
    scale = total_bytes / hold_bytes

    def _score(cdict):
        cctx = zstd.ZstdCompressor(level=ZSTD_LEVEL, dict_data=cdict)
        packed = sum(len(cctx.compress(s)) for s in holdout)
        dict_bytes = len(cdict.as_bytes()) if cdict else 0
        return dict_bytes + packed * scale

    best_size, best_cost = 0, _score(None)
    baseline = best_cost
    cap = min(DICT_SEARCH_MAX_BYTES, sum(len(s) for s in train) // 10)
    size = DICT_SEARCH_MIN_BYTES
    misses = 0
    tried = []
    while size <= cap and misses < 2:
        try:
            cost = _score(zstd.train_dictionary(size, train))
        except zstd.ZstdError:
            break  # too little data for this size; larger won't train either
        tried.append(f"{size // 1024}K={cost / 1e6:.2f}MB")
        if cost < best_cost:
            best_size, best_cost, misses = size, cost, 0
        else:
            misses += 1
        size *= 2

    log.info(
        f"{label}: dictionary search over {len(samples)} sample(s): "
        f"none={baseline / 1e6:.2f}MB, {', '.join(tried) or 'no sizes trainable'}."
    )
    if not best_size:
        log.info(f"{label}: no dictionary beats dictionary-less; going without.")
        return None
    try:
        cdict = zstd.train_dictionary(best_size, samples)
    except zstd.ZstdError as e:
        log.warning(f"{label}: dictionary training failed ({e}); going without.")
        return None
    log.info(
        f"{label}: chose {len(cdict.as_bytes())}-byte dictionary "
        f"(projected {(1 - best_cost / baseline) * 100:.1f}% smaller than none)."
    )
    return cdict
