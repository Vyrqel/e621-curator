import threading
import time

import requests

from .config import POSTS_PER_PAGE, _server_state
from .database import db
from .e6api import fetch_posts
from .runtime import E621_USERNAME, log
from .tagcache import _SmoothBar, backfill_tag_cache


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
    # Total is unknown up front, so the bar runs open-ended. Each page is
    # assumed full while in flight and corrected on arrival, which lets the
    # same easing curve that drives the tag sweeps drive this too.
    bar = _SmoothBar(None, "Favorites sync: fetch")
    try:
        while True:
            bar.batch_start(POSTS_PER_PAGE)
            try:
                posts = fetch_posts(query, page=page)
            except requests.RequestException as e:
                log.warning(f"Favorites sync: error on page {page}: {e}")
                bar.batch_end(0)
                break
            if not posts:
                bar.batch_end(0)
                break
            for p in posts:
                all_ids.add(p["id"])
            bar.batch_end(len(posts))
            page += 1
    finally:
        bar.close()

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
