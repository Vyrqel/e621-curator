import bisect
import random
import threading
import time

import zstandard as zstd
from tqdm import tqdm

from . import config
from .config import (
    _BLOB_FMT_V3,
    DICT_MIN_SAMPLES,
    DICT_SIZE,
    POSTS_PER_PAGE,
    TQDM_STEADY,
    ZSTD_LEVEL,
)
from .database import db, run_vacuum
from .e6api import _fetch_post_by_id
from .posts import (
    _fetch_posts_by_ids,
    _get_all_referenced_post_ids,
    cache_post_from_response,
    clear_refresh_progress,
    get_refresh_progress,
    get_uncached_post_ids,
    get_unrelated_post_ids,
    purge_deleted_post,
    purge_post,
    set_refresh_progress,
)
from .runtime import log
from .tagcodec import _decode_raw_v3, _decompress_blob, _encode_raw_v3, _tag_dicts
from .taggraph import _tag_graph, refresh_tag_graph


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

    TICK = 0.1  # seconds between display updates (10 FPS)
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
            # Both of these matter. Left at its default, tqdm turns on
            # dynamic_miniters: after every repaint it sets `miniters` to
            # however many units went by since the last one, so a bar updated
            # in small steps faster than `mininterval` teaches tqdm to skip
            # ever-larger chunks — the easing curve runs, and the terminal
            # only sees it when a batch lands. Pinning miniters=1 and
            # mininterval=0 makes every update() we issue repaint, which is
            # the whole point of driving the bar off a ticker.
            miniters=1,
            mininterval=0,
        )
        self.ticker = threading.Thread(target=self._tick, daemon=True)
        self.ticker.start()

    def _tick(self):
        last = time.monotonic()
        while not self.done.wait(self.TICK):
            now = time.monotonic()
            dt, last = now - last, now
            with self.lock:
                remaining = (self.actual + self.pending) - self.shown
                if remaining <= 0:
                    continue
                # Keep easing between batches too. Once a batch lands there is
                # usually a little residual the curve hadn't covered, plus a
                # gap while the DB writes and any 404 verification happen; if
                # the tick stopped there the bar would freeze and then resume
                # with a visible step. Easing straight through the gap absorbs
                # the residual as continued motion instead.
                velocity = min(remaining / self.tau, self.cap or remaining)
                self._draw(self.shown + velocity * dt)

    def _draw(self, target):
        """Move the display to `target`, clamped. Caller holds the lock.

        `shown` is kept as a float so sub-post-per-tick motion accumulates,
        but the bar itself is only ever advanced in whole posts — tqdm would
        otherwise render a fractional count.
        """
        ceiling = self.actual + self.pending
        if self.total is not None:
            ceiling = min(self.total, ceiling)
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

    def batch_end(self, posts=None):
        """Retire the in-flight batch and refold its pace into the estimate.

        `posts`, if given, replaces the size declared at `batch_start` — for
        callers that can only guess the batch size up front (an open-ended
        page walk, where the last page comes back short).

        Called after the batch is fully processed — including any one-by-one
        404 verification — so the rate reflects real wall-clock progress per
        post rather than just the fetch, and the next batch's easing curve is
        stretched to match.
        """
        with self.lock:
            if not self.pending:
                return
            if posts is not None:
                self.pending = posts
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
            # A corrected batch can come back smaller than the curve already
            # drew against. The display never walks backwards, so just drop
            # the float back to reality and let the next batch's motion pick
            # up once it passes what's already on screen.
            self.shown = min(self.shown, float(self.actual))
            # Deliberately no catch-up draw here — whatever the curve hadn't
            # covered becomes the next batch's starting point.

    CLOSE_EASE = 0.5  # seconds allowed to walk the last residual in

    def close(self):
        """Stop the ticker and walk the display in to the real count.

        Whatever the last batch's curve didn't cover is eased in over at most
        CLOSE_EASE seconds rather than assigned, so the bar's final move looks
        like the rest of its motion instead of a snap to the end.
        """
        self.done.set()
        self.ticker.join(timeout=1)
        deadline = time.monotonic() + self.CLOSE_EASE
        while True:
            with self.lock:
                self.pending = 0
                remaining = self.actual - self.shown
                if remaining <= 0 or time.monotonic() >= deadline:
                    break
                # Linear over the remaining budget, so it lands on time.
                budget = max(deadline - time.monotonic(), self.TICK)
                self._draw(self.shown + remaining * (self.TICK / budget))
            time.sleep(self.TICK)
        with self.lock:
            # Forward-only. On an open-ended bar the last page can come back
            # shorter than the full page assumed while it was in flight, so
            # the curve may already sit past the real count; rewinding the
            # display to correct it would be the exact snap this class exists
            # to avoid. The log line carries the true number.
            self.shown = max(self.shown, float(self.actual))
            self.bar.n = max(self.bar.n, self.actual)
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
        for r in tqdm(
            rows,
            desc="Dict retrain: decode",
            unit="blob",
            leave=False,
            **TQDM_STEADY,
        ):
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
        if config.DICT_TRAIN_SAMPLES and config.DICT_TRAIN_SAMPLES < len(samples):
            samples = random.sample(samples, config.DICT_TRAIN_SAMPLES)

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
                **TQDM_STEADY,
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
