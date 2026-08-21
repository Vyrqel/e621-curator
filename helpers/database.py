import sqlite3
import threading
import time
from contextlib import contextmanager

from .config import DB_BUSY_TIMEOUT, DB_PATH
from .runtime import log

SCHEMA = """
CREATE TABLE IF NOT EXISTS seen (
    post_id INTEGER PRIMARY KEY,
    seen_at INTEGER NOT NULL
);

-- Posts handed to the client's preload buffer but not yet displayed.
--
-- This is the durable half of the seen/reserved split. `seen` means "the user
-- looked at it"; this table means "we committed it to a buffer and owe the
-- user a look at it." Keeping the two apart is what stops pagination
-- bookkeeping writing off a page on the strength of a post nobody saw.
--
-- Rows leave by exactly two doors: /api/seen deletes on display, and the
-- drain in _drain_reserved deletes when a post is re-served after the client
-- dropped it (reload, override change, closed tab). Nothing expires on a
-- timer — an abandoned row is the whole point, it is a post we still owe.
CREATE TABLE IF NOT EXISTS preload_queue (
    post_id INTEGER PRIMARY KEY,
    query_tags TEXT,                    -- query it came from, for /api/seen accounting
    from_primed INTEGER NOT NULL DEFAULT 0,
    reserved_at INTEGER NOT NULL        -- ordering: oldest reservation resumes first
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

    from .store import purge_damaging_query_data

    purge_damaging_query_data()


# ---------- File loaders ----------
