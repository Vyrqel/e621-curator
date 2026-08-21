import hashlib
import time
import urllib.parse

from tqdm import tqdm

from .config import (
    BLACKLIST_FILE,
    POSTS_PER_PAGE,
    QUERIES_FILE,
    RESCAN_BATCH_TAGS,
    TQDM_STEADY,
    _server_state,
)
from .database import db, get_meta, set_meta
from .e6api import fetch_posts, post_is_viewable
from .posts import purge_deleted_post
from .runtime import log
from .store import (
    clear_all_post_count_snapshots,
    get_exhausted_queries,
    get_seen_ids,
    lookup_post_count,
    mark_unexhausted,
    query_hash,
    set_page1_empty,
    update_scanned_at,
)
from .taggraph import _tag_graph
from .userfiles import (
    is_blacklisted,
    load_blacklist,
    post_tag_set,
    remove_from_additions_file,
)


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
        rows, desc="Export diff: compare", unit="tag", leave=False, **TQDM_STEADY
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
        expunge, desc="Export diff: expunge", unit="tag", leave=False, **TQDM_STEADY
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
                dirty,
                desc="Export diff: scan",
                unit="tag",
                leave=False,
                **TQDM_STEADY,
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
            **TQDM_STEADY,
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
