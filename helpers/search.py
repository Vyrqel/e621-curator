import random
import time

import requests

from .config import MAX_PRIMED_ATTEMPTS
from .database import db
from .e6api import (
    fetch_posts,
    post_file,
    post_is_favorited,
    post_is_viewable,
    post_sample_url,
    post_score,
)
from .posts import (
    _parse_csv_ids,
    cache_post_from_response,
    extract_relations,
    get_post_relations,
    relations_search_tags,
)
from .runtime import log
from .store import (
    get_last_page,
    mark_exhausted,
    query_hash,
    set_last_page,
    set_page1_empty,
)
from .taggraph import _tag_graph
from .userfiles import _tag_matches, is_blacklisted


def _find_unseen_post(query, seen, blacklist, persist=True, reserved=None):
    """Two-phase page strategy for one query.

    When persist=False, skip all writes to query_progress (no last_page tracking,
    no exhaustion marking). Used for ephemeral override queries.

    `reserved` holds post IDs the client already has buffered but has NOT
    displayed yet. They must not be served twice, but they are emphatically
    NOT seen: a page whose only unseen post is reserved is still a page with
    content on it, and advancing last_page past it would drop that post for
    good if the client never displays it. So reserved IDs suppress *serving*
    while leaving the page-fullness judgement alone.

    Semantics: `last_page` records the highest page known to be FULLY seen.
    Phase 2 resumes from `last_page + 1` (everything below is already exhausted).

    Phase 1 — check page 1 for new content, BUT only if we haven't recently
              confirmed page 1 is fully seen (cached via last_scanned_at).
    Phase 2 — walk forward from last_page + 1. Each page that comes back fully
              seen is recorded by advancing last_page; the first page with
              eligible content yields a post WITHOUT advancing last_page (since
              that page still has more unseen posts on it).

    Returns (post, page) on success, or None if the query is exhausted.
    """

    reserved = reserved or set()

    def unseen_on_page(page):
        """Returns (posts, eligible, servable).

        `eligible` drives progress bookkeeping — it is the honest answer to
        "does this page still hold anything the user hasn't seen?"
        `servable` is `eligible` minus the reserved buffer, and is the only
        thing we may hand back.
        """
        posts = fetch_posts(query, page=page)
        if not posts:
            return None, [], []  # past end of results
        eligible = [
            p
            for p in posts
            if p["id"] not in seen
            and not is_blacklisted(p, blacklist)
            and post_is_viewable(p)
        ]
        servable = [p for p in eligible if p["id"] not in reserved]
        return posts, eligible, servable

    # Page 1 freshness cache: if the scanner or a prior request recently
    # confirmed page 1 has no eligible posts, skip the redundant fetch.
    # The scanner runs hourly, so trusting its result for ~30 minutes is safe.
    PAGE1_CACHE_SECONDS = 86400  # 24 hours — artists rarely post twice in a day

    if persist and len(query.split()) != 1:
        persist = False

    try:
        # Decide whether to skip phase 1
        skip_phase1 = False
        if persist:
            with db() as conn:
                row = conn.execute(
                    """SELECT page1_empty_at FROM query_progress
                       WHERE query_hash = ?""",
                    (query_hash(query),),
                ).fetchone()
            if row and row["page1_empty_at"]:
                age = int(time.time()) - row["page1_empty_at"]
                if age < PAGE1_CACHE_SECONDS:
                    skip_phase1 = True
                    log.info(
                        f"Query '{query}' skipping phase 1 (cached empty {age}s ago)"
                    )

        # Phase 1: check page 1 for new posts
        if not skip_phase1:
            posts, eligible, servable = unseen_on_page(1)
            if posts is None:
                log.info(f"Query '{query}' page 1 returned no posts at all.")
                return None
            if eligible:
                # Page 1 has fresh content — clear the empty cache
                if persist:
                    set_page1_empty(query, None)
                if servable:
                    return random.choice(servable), 1
                # Everything new on page 1 is already in the client's buffer.
                # Page 1 is NOT empty, so the cache stays cleared above; fall
                # through to phase 2 for something else to show.
                log.info(
                    f"Query '{query}' page 1 unseen posts are all buffered; "
                    f"falling through to phase 2."
                )
            else:
                # Page 1 fully seen — cache that fact
                if persist:
                    set_page1_empty(query, int(time.time()))

        # Phase 2: walk forward from last_page + 1.
        if persist:
            resume_page = get_last_page(query) + 1
        else:
            resume_page = 2
        log.info(
            f"Query '{query}' resuming phase 2 from page {resume_page} "
            f"(persist={persist}, phase1_skipped={skip_phase1})"
        )
        page = resume_page
        # Set if we step over a page that had unseen posts we couldn't serve
        # because the client already holds them. Running off the end of the
        # results after that does NOT mean the query is exhausted — it means
        # what's left is in the buffer — so the exhausted flag must not be set.
        skipped_reserved = False
        while True:
            posts, eligible, servable = unseen_on_page(page)
            if posts is None:
                if skipped_reserved:
                    log.info(
                        f"Query '{query}' has no servable posts left, but its "
                        f"remaining unseen posts are buffered — not marking "
                        f"exhausted."
                    )
                    return None
                log.info(f"Query '{query}' exhausted (last attempted page: {page}).")
                if persist:
                    mark_exhausted(query)
                return None
            if eligible:
                if servable:
                    log.info(f"Query '{query}' found unseen post on page {page}.")
                    return random.choice(servable), page
                # Unseen content here, but it's all sitting in the client's
                # buffer. Step over the page WITHOUT advancing last_page, so
                # these posts stay reachable if they're never displayed.
                log.info(
                    f"Query '{query}' page {page} unseen posts are all buffered; "
                    f"skipping without advancing last_page."
                )
                skipped_reserved = True
                page += 1
                continue
            log.info(
                f"Query '{query}' page {page} fully seen ({len(posts)} posts); "
                f"{'advancing last_page and ' if persist else ''}continuing."
            )
            if persist:
                set_last_page(query, page)
            page += 1

    except requests.RequestException as e:
        log.warning(f"Query '{query}' network error: {e}")
        return None


def _flatten_post_tags(tags_dict, rating):
    """Build the flat lowercase set of all tags + pseudo-tags for matching.

    Cached posts store only their most specific tags (see set_post_tag_cache),
    so the implication graph is walked upward here to put the implied ones
    back before matching. Doing it on the post side rather than expanding the
    query means wildcards and negations keep working unchanged: `-canine`
    still excludes a post that only stores `fox`.
    """
    all_tags = set()
    for category in (
        "general",
        "species",
        "character",
        "artist",
        "copyright",
        "meta",
        "lore",
        "invalid",
    ):
        for t in tags_dict.get(category, []):
            all_tags.add(t.lower())
    all_tags = _tag_graph.expand(all_tags)
    if rating:
        all_tags.add(f"rating:{rating}")
    return all_tags


def _parse_query_string(query):
    """Parse an e621-style query string into a list of (pattern, negated) terms.

    Same shape as the blacklist parser, but applies to a single query rather
    than multi-line clauses.
    """
    terms = []
    for token in query.split():
        token = token.lower().strip()
        if not token:
            continue
        negated = token.startswith("-")
        if negated:
            token = token[1:]
        if token:
            terms.append((token, negated))
    return terms


# Metatags that answer from post_relations / the post ID rather than from the
# tag set. Review mode filters against the local cache, so without these a
# search like `pool:52413` is matched as if "pool:52413" were a literal tag --
# it never matches anything and the review list comes back empty.
_RELATION_PREFIXES = (
    "parent:",
    "child:",
    "pool:",
    "id:",
    "status:",
    "ischild:",
    "hasparent:",
    "isparent:",
    "haschild:",
    "haschildren:",
)

# Boolean forms and what they assert. True = "must have the relation".
_RELATION_FLAGS = {
    "ischild": "parent",
    "hasparent": "parent",
    "isparent": "children",
    "haschild": "children",
    "haschildren": "children",
}

_EMPTY_RELATIONS = {"parent_id": None, "children": [], "pools": []}


def is_relation_term(pattern):
    """True if `pattern` is one of the metatags handled by relation matching."""
    return pattern.startswith(_RELATION_PREFIXES)


def get_relations_map(post_ids):
    """Bulk-load relations for many posts at once: {post_id: relations}.

    Posts with no row are omitted -- callers treat a miss as "no relations
    known", which _relation_term_matches() handles via _EMPTY_RELATIONS.
    """
    out = {}
    ids = list(post_ids)
    if not ids:
        return out
    CHUNK = 900  # stay under SQLite's variable limit
    with db() as conn:
        for start in range(0, len(ids), CHUNK):
            batch = ids[start : start + CHUNK]
            placeholders = ",".join("?" for _ in batch)
            rows = conn.execute(
                f"SELECT * FROM post_relations WHERE post_id IN ({placeholders})",
                batch,
            ).fetchall()
            for row in rows:
                out[row["post_id"]] = {
                    "parent_id": row["parent_id"],
                    "children": _parse_csv_ids(row["children"]),
                    "pools": _parse_csv_ids(row["pools"]),
                }
    return out


def _relation_term_matches(pattern, post_id, rel):
    """Evaluate one relation metatag against a post. Returns True/False.

    `rel` is a relations dict (or None, treated as "no relations known").
    Anything unparseable evaluates False rather than raising -- a malformed
    metatag should narrow the search, not blow up the request.
    """
    rel = rel or _EMPTY_RELATIONS
    name, _, value = pattern.partition(":")

    if name in _RELATION_FLAGS:
        field = _RELATION_FLAGS[name]
        has = bool(rel["parent_id"]) if field == "parent" else bool(rel["children"])
        return has if value == "true" else (not has) if value == "false" else False

    if name == "status":
        # No local equivalent -- every cached post is a live post. Treated as
        # satisfied so `status:any` (appended by child: expansion) is a no-op
        # here instead of emptying the list.
        return True

    if name == "id":
        wanted = {int(t) for t in value.split(",") if t.strip().isdigit()}
        return post_id in wanted

    if name == "parent":
        if value == "none":
            return rel["parent_id"] is None
        if value == "any":
            return rel["parent_id"] is not None
        return value.isdigit() and rel["parent_id"] == int(value)

    if name == "child":
        # Mirror of parent:, same as the API-side metatag -- child:<id>
        # selects the post that IS the parent of <id>. Answered straight
        # from the stored child list, no lookup needed.
        if value == "none":
            return not rel["children"]
        if value == "any":
            return bool(rel["children"])
        return value.isdigit() and int(value) in rel["children"]

    if name == "pool":
        if value == "none":
            return not rel["pools"]
        if value == "any":
            return bool(rel["pools"])
        return value.isdigit() and int(value) in rel["pools"]

    return False


def _get_local_favorite_ids():
    """Return the set of post IDs currently in the local favorites table."""
    with db() as conn:
        rows = conn.execute("SELECT post_id FROM favorites").fetchall()
    return {r["post_id"] for r in rows}


def _post_matches_query_with_favs(
    tags_dict, rating, post_id, query, favorite_ids, relations=None
):
    """Internal matcher that takes a pre-fetched favorite_ids set.

    `relations` is the post's row from post_relations (see get_relations_map).
    It's only consulted when the query actually contains a relation metatag;
    passing None means "none known", which makes parent:/child:/pool: terms
    evaluate False rather than silently matching.
    """
    terms = _parse_query_string(query)
    if not terms:
        return True

    all_tags = _flatten_post_tags(tags_dict, rating)
    fav_terms = [(pat, neg) for (pat, neg) in terms if pat.startswith("fav:")]
    rel_terms = [(pat, neg) for (pat, neg) in terms if is_relation_term(pat)]
    regular_terms = [
        (pat, neg)
        for (pat, neg) in terms
        if not pat.startswith("fav:") and not is_relation_term(pat)
    ]

    if fav_terms:
        is_favorited = post_id in favorite_ids
        for _, negated in fav_terms:
            if is_favorited == negated:
                return False

    for pattern, negated in rel_terms:
        if _relation_term_matches(pattern, post_id, relations) == negated:
            return False

    for pattern, negated in regular_terms:
        matches = _tag_matches(pattern, all_tags)
        if matches == negated:
            return False

    return True


def _relations_payload(post):
    """Relationship block for the frontend.

    Read straight off the fetched post -- it's authoritative and already in
    hand. Falls back to the stored row only if the response somehow lacks
    the fields (an abbreviated payload from a cached path).
    """
    pid = post["id"]
    if "relationships" in post or "pools" in post:
        parent_id, children, pools = extract_relations(post)
        rel = {
            "parent_id": parent_id,
            "children": sorted(set(children)),
            "pools": sorted(set(pools)),
        }
    else:
        rel = get_post_relations(pid) or {
            "parent_id": None,
            "children": [],
            "pools": [],
        }
    return {
        "parent_id": rel["parent_id"],
        "children": rel["children"],
        "pools": rel["pools"],
        "searches": relations_search_tags(pid, rel),
    }


def _build_post_response(post, query, known, from_primed):
    """Build the JSON payload for a successfully-found post.

    Note: this does NOT mark the post as seen. Marking-seen is now an
    explicit action via /api/seen, called by the frontend when the post
    actually transitions to being displayed.

    We DO cache the post's tags here — every fetched post updates the local
    tag cache, which is used by review-mode override filtering.
    """
    cache_post_from_response(post)

    file_info = post_file(post)
    tags = post.get("tags", {})
    artists = tags.get("artist", [])
    characters = tags.get("character", [])

    with db() as conn:
        fav_row = conn.execute(
            "SELECT 1 FROM favorites WHERE post_id = ?", (post["id"],)
        ).fetchone()

    is_favorited = post_is_favorited(post) or fav_row is not None

    return {
        "id": post["id"],
        "query": query,
        "file_url": file_info["url"],
        "sample_url": post_sample_url(post),
        "ext": file_info["ext"],
        "width": file_info["width"],
        "height": file_info["height"],
        "is_favorited": is_favorited,
        "from_primed": from_primed,
        "artists": [
            {"tag": a, "known": a.lower() in known}
            for a in artists
            if a not in ("conditional_dnp",)
        ],
        "characters": [{"tag": c, "known": c.lower() in known} for c in characters],
        "rating": post.get("rating"),
        "score": post_score(post),
        "post_url": f"https://e621.net/posts/{post['id']}",
        "relations": _relations_payload(post),
    }


def _try_pool(
    pool, seen, blacklist, persist, known, from_primed, ordered=False, reserved=None
):
    """Sample queries from a pool; return response dict or None.

    ordered=False (general pool): pick up to 5 queries at RANDOM.
    ordered=True  (primed pool):  try queries in the given (priority) order,
        up to MAX_PRIMED_ATTEMPTS, returning the first that yields. Stable
        ordering means the top primed query is retried on consecutive requests
        until its new content is drained, instead of bouncing randomly.

    decrement_primed_count is NOT called here — deferred to /api/seen.
    """
    if ordered:
        for query in pool[:MAX_PRIMED_ATTEMPTS]:
            result = _find_unseen_post(
                query, seen, blacklist, persist=persist, reserved=reserved
            )
            if result is not None:
                post, page = result
                return _build_post_response(post, query, known, from_primed)
        return None

    tried = set()
    while len(tried) < min(len(pool), 5):
        query = random.choice(pool)
        if query in tried:
            continue
        tried.add(query)
        result = _find_unseen_post(
            query, seen, blacklist, persist=persist, reserved=reserved
        )
        if result is not None:
            post, page = result
            return _build_post_response(post, query, known, from_primed)
    return None
