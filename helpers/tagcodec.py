import threading

import zstandard as zstd

from .config import (
    _BITS_RATING,
    _BLOB_FMT_V3,
    _RATING_BITS,
    _RATING_MASK,
    TAG_CATEGORIES,
    ZSTD_LEVEL,
)
from .database import db


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
