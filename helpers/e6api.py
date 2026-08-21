import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import MIN_REQUEST_INTERVAL, POSTS_PER_PAGE, USER_AGENT
from .runtime import E621_API_KEY, E621_USERNAME

# ---------- e621 API ----------

_last_request_time = 0.0

# Shared session with automatic retries for transient SSL/connection errors.
# e621 sits behind Cloudflare which occasionally terminates handshakes.
_session = requests.Session()
_retry = Retry(
    total=4,
    backoff_factor=1.5,  # 0, 1.5, 3, 6 second waits
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
    raise_on_status=False,
)
_adapter = HTTPAdapter(max_retries=_retry)
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)


def rate_limit():
    global _last_request_time
    elapsed = time.time() - _last_request_time
    if elapsed < MIN_REQUEST_INTERVAL:
        time.sleep(MIN_REQUEST_INTERVAL - elapsed)
    _last_request_time = time.time()


def _auth():
    """Return requests auth tuple if credentials are configured, else None."""
    if E621_USERNAME and E621_API_KEY:
        return (E621_USERNAME, E621_API_KEY)
    return None


def _v2_params(params):
    """Add the v2 opt-in parameters to a post-endpoint request.

    mode=extended is REQUIRED: the v2 default (mode=basic) returns tags as a
    flat array with no categories, which would silently break the tag codec.
    """
    params = dict(params)
    params["v2"] = "true"
    params["mode"] = "extended"
    return params


# ---------- v2 post accessors ----------
#
# Post objects are consumed in v2 shape directly; there is no normalization
# layer. v2 groups file data under files.{meta,original,preview,sample} and
# counters under stats.*, and leaves tags (in mode=extended), rating, flags
# and relationships at the top level. These accessors exist only so the
# nesting is spelled out in one place.


def post_file(post):
    """files.original + files.meta — the original upload's url and metadata."""
    files = post.get("files") or {}
    original = files.get("original") or {}
    meta = files.get("meta") or {}
    return {
        "url": original.get("url"),
        "width": original.get("width"),
        "height": original.get("height"),
        "ext": meta.get("ext"),
        "size": meta.get("size"),
        "md5": meta.get("md5"),
    }


def post_sample_url(post):
    """Preferred display URL: the resized sample jpg, else the original."""
    files = post.get("files") or {}
    sample = files.get("sample") or {}
    original = files.get("original") or {}
    return sample.get("jpg") or original.get("url")


def post_is_viewable(post):
    """Whether the post has any renderable image URL at all."""
    return bool(post_sample_url(post))


def post_score(post):
    """stats.score.total — net score."""
    stats = post.get("stats") or {}
    return (stats.get("score") or {}).get("total", 0)


def post_is_favorited(post):
    """stats.is_favorited — e621's own favorite flag for the auth'd user."""
    return (post.get("stats") or {}).get("is_favorited", False)


def _extract_posts(payload):
    """Get the post list out of a /posts.json response.

    v2 returns a bare array; the object form is tolerated defensively.
    """
    if isinstance(payload, list):
        return payload
    return payload.get("posts") or []


def fetch_posts(tags, page=1):
    """Fetch a page of posts from e621 for the given tag string."""
    rate_limit()
    response = _session.get(
        "https://e621.net/posts.json",
        params=_v2_params({"tags": tags, "limit": POSTS_PER_PAGE, "page": page}),
        headers={"User-Agent": USER_AGENT},
        auth=_auth(),
        timeout=15,
    )
    response.raise_for_status()
    return _extract_posts(response.json())


def e621_favorite(post_id):
    """Create a favorite on e621 for the given post ID. Raises on failure."""
    rate_limit()
    response = _session.post(
        "https://e621.net/favorites.json",
        params={"post_id": post_id},
        headers={"User-Agent": USER_AGENT},
        auth=_auth(),
        timeout=15,
    )
    # 422 means already favorited — treat as success
    if response.status_code == 422:
        return
    response.raise_for_status()


def e621_unfavorite(post_id):
    """Remove a favorite from e621 for the given post ID. Raises on failure."""
    rate_limit()
    response = _session.delete(
        f"https://e621.net/favorites/{post_id}.json",
        headers={"User-Agent": USER_AGENT},
        auth=_auth(),
        timeout=15,
    )
    # 404 means not favorited — treat as success
    if response.status_code == 404:
        return
    response.raise_for_status()


def _fetch_post_by_id(post_id):
    """Fetch a single post's full data from e621 by ID. Returns None on 404."""
    rate_limit()
    response = _session.get(
        f"https://e621.net/posts/{post_id}.json",
        params=_v2_params({}),
        headers={"User-Agent": USER_AGENT},
        auth=_auth(),
        timeout=15,
    )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    payload = response.json()
    # v2 returns a bare object; the wrapped form is tolerated defensively.
    if isinstance(payload, dict) and "post" in payload:
        payload = payload["post"]
    return payload
