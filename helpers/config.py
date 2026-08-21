from pathlib import Path

# ---------- Configuration ----------

# The repository root — one level up from this package, since every data file
# (the DB, queries.txt, the CSV dumps) sits beside app.py, not beside the code.
ROOT = Path(__file__).resolve().parent.parent
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

# ---------- Progress bars / downloads ----------
# right knob for a bar stepped per item.
TQDM_STEADY = {"miniters": 1, "mininterval": 0.1}

# Streaming-download read size. Doubles as the progress-bar resolution.
DOWNLOAD_CHUNK = 1 << 16  # 64 KiB
