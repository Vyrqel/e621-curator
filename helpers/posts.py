import re
import time

from .database import db
from .e6api import _fetch_post_by_id, e621_unfavorite, fetch_posts
from .runtime import log
from .tagcodec import encode_tags
from .taggraph import _tag_graph


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
    out = sorted({
        int(v) for v in (values or []) if str(v).strip().isdigit() or isinstance(v, int)
    })
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


# Shared tqdm settings for the per-item bars (rescan, export diff, dict
# retrain, downloads). tqdm's default `miniters=None` turns on
# dynamic_miniters: after each repaint it sets miniters to however many units
# went by since the last one, so a fast loop teaches it to skip ever-larger
# chunks and the bar lurches instead of moving. Pinning miniters=1 disables
# that; mininterval still throttles repaints to 10/s by time, which is the
