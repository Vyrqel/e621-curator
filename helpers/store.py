import hashlib
import time

from .database import db
from .runtime import log
from .taggraph import _tag_graph, _tag_store


def get_seen_ids():
    with db() as conn:
        return {row["post_id"] for row in conn.execute("SELECT post_id FROM seen")}


# ---------- Preload reservations ----------
#
# The client keeps N posts prefetched. Those posts are spoken for but unseen,
# and the distinction is load-bearing: treat them as seen and pagination walks
# past pages that still owe content; treat them as unseen and available and
# they get served twice. So they live in their own table.
#
# The client still sends `exclude` on every request. That is not redundant
# with this table — the two answer different questions:
#
#   preload_queue  = "posts we owe the user a look at"   (durable, server-side)
#   exclude        = "posts I am still holding right now" (live, client-side)
#
# A row in the table but NOT in exclude is an abandoned reservation: the
# client dropped it without displaying it. Those are exactly the posts to
# re-serve first, which is what makes the queue resume across a reload.


def reserve_posts(entries):
    """Record posts handed to the preload buffer. `entries` is [(id, query, primed)]."""
    if not entries:
        return
    now = int(time.time())
    with db() as conn:
        conn.executemany(
            "INSERT INTO preload_queue (post_id, query_tags, from_primed, reserved_at) "
            "VALUES (?, ?, ?, ?) ON CONFLICT(post_id) DO NOTHING",
            [(pid, q, 1 if primed else 0, now) for pid, q, primed in entries],
        )


def release_post(post_id):
    """Drop a reservation — the post has been displayed, or re-served."""
    with db() as conn:
        conn.execute("DELETE FROM preload_queue WHERE post_id = ?", (post_id,))


def get_reserved_ids():
    with db() as conn:
        return {r["post_id"] for r in conn.execute("SELECT post_id FROM preload_queue")}


def take_abandoned_reservation(holding, seen):
    """Pop the oldest reservation the client is no longer holding.

    Returns (post_id, query_tags, from_primed) or None. Rows for posts that
    turned out to be seen anyway are cleaned up in passing rather than
    returned — that can happen if /api/seen raced the reservation.
    """
    with db() as conn:
        rows = conn.execute(
            "SELECT post_id, query_tags, from_primed FROM preload_queue "
            "ORDER BY reserved_at ASC"
        ).fetchall()
    for row in rows:
        pid = row["post_id"]
        if pid in holding:
            continue  # client still has it buffered; leave it reserved
        if pid in seen:
            release_post(pid)
            continue
        return pid, row["query_tags"], bool(row["from_primed"])
    return None


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
