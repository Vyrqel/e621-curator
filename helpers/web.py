import re
import threading
import time
import urllib.parse

import requests
from flask import jsonify, render_template, request

from .config import ROOT, TAG_CATEGORIES, _server_state
from .database import db
from .e6api import (
    _auth,
    _fetch_post_by_id,
    e621_favorite,
    e621_unfavorite,
    post_is_viewable,
)
from .maintenance import _maintenance_pass
from .posts import expand_child_metatag, mark_post_seen
from .runtime import app, log
from .scanner import _scan_exhausted_queries, clear_all_page1_cache
from .search import (
    _build_post_response,
    _get_local_favorite_ids,
    _parse_query_string,
    _post_matches_query_with_favs,
    _try_pool,
    get_relations_map,
    is_relation_term,
)
from .store import (
    decrement_primed_count,
    get_primed_queries,
    get_reserved_ids,
    get_seen_ids,
    release_post,
    reserve_posts,
    take_abandoned_reservation,
)
from .tagcache import rebuild_tag_data
from .tagcodec import decode_tags
from .taggraph import _tag_graph, _tag_store
from .userfiles import (
    append_to_additions_file,
    is_blacklisted,
    load_blacklist,
    load_known_tags,
    load_queries,
    remove_from_additions_file,
)


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
    # Posts the caller already holds in its preload buffer but has not
    # displayed yet. These are kept OUT of `seen` deliberately — see the
    # `reserved` note in _find_unseen_post. Folding them into `seen` would
    # make a page look fully-exhausted on the strength of posts the user may
    # never actually look at, and last_page would step over them for good.
    # What the client is holding in its buffer *right now*.
    holding = set()
    exclude_param = request.args.get("exclude", "")
    if exclude_param:
        for tok in exclude_param.split(","):
            tok = tok.strip()
            if tok.isdigit():
                holding.add(int(tok))

    # Everything we owe the user a look at: the durable reservations plus
    # whatever this client is holding. Deliberately kept out of `seen` — see
    # the `reserved` note in _find_unseen_post.
    reserved = get_reserved_ids() | holding

    known = load_known_tags()
    blacklist = load_blacklist()

    # Resume the queue before picking anything new. An abandoned reservation
    # is a post we already committed to showing and never did; serving it now
    # is what makes the buffer survive a reload or a closed tab.
    if not override:
        resumed = take_abandoned_reservation(holding, seen)
        if resumed is not None:
            post_id, res_query, res_primed = resumed
            try:
                post = _fetch_post_by_id(post_id)
            except requests.RequestException as e:
                log.warning(f"Reserved post {post_id} refetch failed: {e}")
                post = None
            if post is None:
                # Gone from e621 — the debt is uncollectable, drop it.
                log.info(f"Reserved post {post_id} no longer exists; releasing.")
                release_post(post_id)
            elif is_blacklisted(post, blacklist) or not post_is_viewable(post):
                log.info(f"Reserved post {post_id} no longer eligible; releasing.")
                release_post(post_id)
            else:
                log.info(f"Resuming abandoned reservation: post {post_id}.")
                response = _build_post_response(
                    post, res_query or "(resumed)", known, from_primed=res_primed
                )
                reserve_posts([(post_id, res_query, res_primed)])  # still owed
                return jsonify(response)

    # Try primed queries first
    if primed_pool:
        log.info(f"Primed pool active: {len(primed_pool)} querie(s) with new content.")
        response = _try_pool(
            primed_pool,
            seen,
            blacklist,
            persist,
            known,
            from_primed=True,
            ordered=True,
            reserved=reserved,
        )
        if response is not None:
            return jsonify(_reserve_served(response, persist_reservation=not override))
        log.info(
            "Primed pool yielded nothing this attempt; falling back to general pool."
        )

    # Fall back to the general pool
    response = _try_pool(
        queries, seen, blacklist, persist, known, from_primed=False, reserved=reserved
    )
    if response is not None:
        return jsonify(_reserve_served(response, persist_reservation=not override))

    return jsonify({
        "error": "All sampled queries returned only seen or blacklisted posts. Try again or add queries."
    }), 404


def _reserve_served(response, persist_reservation=True):
    """Record a post we just handed out as owed, and pass the response through.

    Override queries are ephemeral — they never write query_progress, so there
    is no bookkeeping for an unseen post to corrupt and nothing to resume.
    """
    if persist_reservation and response.get("id"):
        reserve_posts([
            (
                response["id"],
                response.get("query"),
                response.get("from_primed", False),
            )
        ])
    return response


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
    release_post(post_id)  # debt settled: it's seen, not merely owed
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
        # Outstanding preload reservations — posts handed to a buffer and not
        # yet displayed. Healthy steady state is roughly the buffer depth;
        # a number that climbs and never falls means rows aren't being
        # released and the queue is leaking.
        "reserved": len(get_reserved_ids()),
        "tags": tag_count,
        "relations": {
            "with_parent": rel_parent_count,
            "with_children": rel_child_count,
            "in_pool": rel_pool_count,
        },
        "tag_graph": _tag_graph.info(),
        "tag_store": _tag_store.info(),
    })


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
