# E621 Curator

A local Flask app for working through an e621 search-query backlog without
opening 4,000 browser tabs — and for keeping that backlog *alive* once you've
worked through it. Exhausted queries get rescanned in the background, new
posts get primed for review, favorites stay synced with your e621 account,
and every post's tags are cached locally in a compressed binary format.

> **Built with heavy AI assistance.** Most of this codebase — including the
> compressed tag storage, the export ingest pipeline, and the autocomplete
> store — was written by Claude (Anthropic) working from my direction, review,
> and testing. This README was written by Claude too. Worth knowing if you're
> reading the code or the commit history and wondering why it's shaped the way
> it is.
>
> **Maintenance:** updated whenever I get around to it — no roadmap, no
> support promised. A full manual rewrite is on the someday list.

## What it does

**Curation loop (the core):**
1. Reads search queries from `queries.txt` (e621 URLs or raw tag strings).
2. Picks a random query, fetches unseen results, shows one post at a time.
3. Tracks seen post IDs in `curator.db` (SQLite) so they never come up again.
4. Per-query pagination progress is persisted, so partially-worked queries
   resume where you left off.
5. Favorite posts (synced to your real e621 account) and one-click flag new
   artists/characters into "additions" lists for later tracking.

**Background machinery:**
- **Scanner** — every 7 days, rescans queries that were previously exhausted.
  Single-tag artist/character queries are OR-batched (up to 20 per request
  using e621's `~` syntax) so a large exhausted list costs very few API calls.
  Queries with new posts become **primed** and get served first.
- **Favorites sync** — periodically enumerates your e621 favorites and
  reconciles them with the local table, both directions.
- **Tag backfill** — every 6 hours, fetches tags for any locally-referenced
  posts missing from the tag cache (batched via `id:a,b,c` + `status:any`).
  Posts confirmed gone from e621 are purged, with an individual per-post
  verification step before deletion.
- **Tag data** — polls e621's export manifest and re-ingests the tags, alias,
  and implication dumps when their checksums change (see below).
- **Refresh resume** — if a `--refresh-tags` sweep was interrupted, startup
  picks it back up from its checkpoint in the background.

## Setup

Requires Python 3.9+.

```bash
cd e621-curator
pip install flask requests zstandard tqdm
```

### credentials.txt

Create a `credentials.txt` next to `app.py` (plain `key=value` lines,
`#` comments allowed):

```
E621_USERNAME=your_username
E621_API_KEY=your_api_key
```

Credentials are needed for favoriting and favorites sync. Browsing works
without them, but there's little point running the app read-only.

## Input files

**queries.txt** — one e621 search URL (or raw tag string) per line. Comments
(`#`) and blank lines ignored. Bare positive tags from this file also mark
artists/characters as "already tracked" in the UI. Queries are canonicalized
to lowercase; a startup pass (`purge_damaging_query_data`) repairs any stale
mixed-case or wildcard tracking rows in the DB.

**blacklist.txt** — e621-syntax blacklist, one clause per line. Space-separated
tags are ANDed within a line; a post is hidden if it matches ANY line. `-`
negates, `*` wildcards, `rating:` metatags work. Re-read on every request, so
you can edit it live.

```
wolf                     # single-tag exclusion
gore male                # only if BOTH tags present
scat -joke               # scat unless also tagged joke
rating:e -solo           # explicit non-solo
*_birth                  # wildcard suffix
```

**additions_artists.txt / additions_characters.txt** — auto-generated as you
click tag chips. Your "to consider tracking" lists; when you commit to one,
move it into `queries.txt`. Known tags get a green checkmark on posts.

## Run

```bash
python app.py [--host HOST] [--port PORT]
```

Defaults to `0.0.0.0:8080` (all interfaces — handy for hitting it from a
phone over Tailscale). Open <http://localhost:8080>.

## UI features

- **Override query** — type any e621 query into the top bar to browse it
  directly instead of pulling from `queries.txt`. The **persist** checkbox
  (default on) controls whether seen-marking applies; multi-tag overrides
  never write query-tracking rows.
- **Review mode** — re-browse posts you've already seen for a query, with a
  jump-to-index counter.
- **History** — step backward/forward through the session's serve history.
- **Force rescan** — the ↺ button (confirm-before-fire) triggers an immediate
  exhausted-query rescan followed by a tag-cache refresh.
- **Connection indicator** — live/scanning/initializing dot; the whole theme
  shifts when the server is unreachable or busy.
- **Primed counter** — stats bar shows how many scanner-discovered new posts
  are waiting; decrements as you review them.
- **Tag autocomplete** — the override bar completes tags as you type, served
  from the local tag store (`GET /api/tags/suggest`). One ranked pool: real
  tags whose name starts with your fragment, merged with aliases whose
  antecedent starts with it, sorted by post count — so `homo` surfaces
  `homo → male/male` (662k) above a dozen low-count literal matches. Aliases
  display e621-style (`alias → canonical`) with the highlight on the alias;
  accepting inserts the canonical tag. There is deliberately no substring tier
  (`wolf` → `grey_wolf`): it only coincidentally overlaps the implication
  graph, and it cost an index the chunked store no longer needs.

## Keyboard shortcuts

- `Space` — next post
- `F` — toggle favorite
- `B` — previous (history)
- In the override bar: `↓`/`↑` move through suggestions, `Tab`/`Enter` accept,
  `Esc` dismisses.

## Data model

All in `curator.db`:

- `seen` — post IDs you've been shown (marked at serve time).
- `favorites` — favorited posts, kept in sync with e621.
- `post_tags` — per-post tag cache: a flags byte (2-bit rating: `00`=s,
  `01`=q, `10`=e, `11`=unknown; remaining 6 bits reserved) followed by the 9
  fixed tag categories, each a varint count plus null-terminated strings, all
  zstd-compressed against a dictionary trained on the corpus itself.
  **`TAG_CATEGORIES` order is load-bearing — never reorder it.**
- `tag_dicts` — the zstd dictionaries the blobs above are compressed against,
  keyed by the version stored in each blob. A retrain writes a new version,
  rewrites every blob against it, then drops any version nothing references —
  so this table normally holds exactly one 256 KiB row and does not grow.
  An older version is retained only if some blob failed to decode and still
  points at it, which is logged as a warning; dropping it would turn a
  possibly-transient failure into permanent loss.
- `tag_graph` — single row holding e621's tag alias and tag implication
  tables, pruned to the tags this database actually references and stored as
  two zstd blobs. Posts keep only their most specific tags; the implied ones
  are walked back at search time. See "Tags are stored minimal" below.
- `query_progress` — per-query pagination, exhaustion, primed counts, and
  scan timestamps, keyed by SHA-256 of the lowercased query string.
- `refresh_progress` — single-row checkpoint for a full tag-refresh sweep, so
  an interrupted `--refresh-tags` resumes where it stopped instead of
  restarting. Cleared by `--no-resume`.
- `tag_chunks` + `tag_store` — the autocomplete corpus. All 867k tag names,
  sorted, front-coded into chunks of 512 and zstd-19 compressed against a
  trained dictionary held in `tag_store` alongside the totals. This replaced a
  plain `tags` table plus name index: 37 MB → 5.5 MB, and the DB as a whole
  went 43.0 → 13.1 MB after vacuum. Each chunk carries `max_post_count` so a
  prefix walk can visit chunks in descending ceiling order and stop once the
  running 12th-best beats every remaining chunk — exact results, but a
  single-character fragment costs 5 ms cold instead of 83 ms. `tag_store`'s
  dictionary is **not** `tag_dicts`: different codec, different schedule,
  and dropping either breaks a different thing.
- `db_exports` — one row per tracked e621 export, holding the upstream
  SHA-256, size, and timestamp last ingested.
- `app_meta` — key/value slots for cross-run state too small to deserve a
  table.

The schema is create-only (`CREATE TABLE IF NOT EXISTS`, applied at startup)
with one small additive migration list (`_MIGRATIONS`) for columns added after
ship. There is no version table; a structural change needs a hand-written
migration. Derived tables are the exception — a `tag_chunks` whose layout
predates the current one is dropped and rebuilt rather than migrated.

To reset everything: delete the file. (You'll lose scan/primed state and the
tag cache, not your e621-side favorites.)

## Endpoints (for inspection / scripting)

- `GET /api/stats` — seen / favorites / additions / queries / exhausted / primed.
- `GET /api/next`, `POST /api/seen` — the main serve loop.
- `GET|POST /api/review`, `GET /api/review/list` — review mode.
- `GET /api/previous`, `GET /api/history_forward` — history navigation.
- `POST /api/favorite`, `POST /api/unfavorite` — favoriting (hits e621 too).
- `POST /api/addition`, `POST /api/addition/remove` — additions lists.
- `GET /api/additions`, `GET /api/favorites` — full lists as JSON.
- `GET /api/ping` — liveness/state for the connection indicator.
- `GET /api/tags/suggest` — ranked tag completions for one fragment.
- `GET /api/tag_graph` — which alias/implication export is loaded.
- `POST /api/tag_graph/refresh` — re-read and re-prune the local dumps. Body
  `{"force": true}` re-ingests even if the files haven't changed, which is what
  you want after the corpus has grown.
- `POST /api/force_rescan` — same as the ↺ button.

## Tags are stored minimal

e621 publishes nightly CSV dumps of its `tags`, `tag_aliases`, and
`tag_implications` tables at
[db_export](https://static1.e621.net/data/db_export/), plus a manifest at
`https://e621.net/db_exports.json` listing each file's URL, size, upstream
SHA-256, and timestamp. The canonical URLs carry **no date** —
`tag_aliases.csv.gz` is always the newest — so there's nothing to probe for.

A background thread reads the manifest first and re-ingests only when a
checksum differs from what `db_exports` has stored; if all three match, nothing
is downloaded at all. Filename dates and mtimes are deliberately not used: the
URLs are undated and every download has a fresh mtime, so either would re-parse
on every pass or miss a real update. Polling is scheduled ~24h05m past the
newest upstream stamp, with a retry ladder (8 attempts, 30 min apart) once
overdue before falling back to the plain period.

Downloads go to a temp directory, get ingested, and are deleted, so no
multi-megabyte CSVs are left lying around (`tags.csv.gz` alone is ~34 MB). The
download tries `requests` first and falls back to `wget`, then `curl`, logging
which one worked; if all three are refused it falls back to any dump sitting
next to `app.py` (`.csv` or `.csv.gz`, dated filename or not), which keeps the
manual browser-download workflow available.

`tags.csv` feeds the autocomplete store; the other two feed the reduction and
expansion described below.

With no dumps obtainable at all the app runs exactly as it did before the
feature existed — no reduction, no expansion, just a warning at startup.

Two things follow from having them:

**Writes shrink.** A post tagged `fox canid canine mammal` stores just `fox` —
everything the remaining tags imply is dropped, because it's recoverable.
Tags are also mapped through the alias table on the way in, so nothing stale
gets written.

**Searches expand.** Review-mode filtering walks the implication graph upward
from a post's stored tags before matching, so querying `canine` still finds
that `fox` post, and `-canine` still excludes it. Expanding the post rather
than the query is what keeps wildcards and negation behaving.

Only direct edges are stored, not the transitive closure. The export ships a
`descendant_names` column with the closure precomputed, but storing it would
repeat every shared ancestor across thousands of rows; edges are the minimal
form and the closure is cheap to rebuild and memoise in RAM.

The graph is **pruned to this database's corpus** — every tag in the post
cache, `queries.txt`, and the additions files, walked upward. If you've never
seen an mlp post, none of the mlp subtree is stored. Tags that appear after a
refresh simply aren't reduced or expanded until the next one, which degrades
to the old behaviour rather than to wrong results.

Refreshing also rewrites the additions files, replacing any tag that has since
been aliased with its current name.

## Maintenance flags

These exit without starting the server:

- `--rebuild-tag-data` — re-ingest the tags/alias/implication exports, then
  retrain the zstd dictionary and rewrite every tag blob against it, with full
  logs. Reach for this when the graph looks wrong or after a batch of tag edits
  land upstream.

  The two halves are deliberately not separately runnable: the retrain
  re-reduces tags against the current implication graph, so doing it before a
  refresh just means doing it again afterwards. The graph download is the only
  part that touches the network; the rewrite is purely local.

- `--refresh-tags` — re-fetch tags for every locally-referenced post (seen and
  favorites), overwriting the cache and purging posts confirmed gone from
  e621. Heavy: one API call per 320 posts. Resumes an interrupted sweep from
  the `refresh_progress` checkpoint.
- `--no-resume` — discard that checkpoint. With `--refresh-tags`, sweeps from
  the top; on normal startup, skips the resume entirely.
- `--vacuum` — reclaim free pages. Runs automatically after
  `--rebuild-tag-data` and `--refresh-tags`; this is the manual handle for
  everything else. Never runs while the server is up, which is why a migration
  that frees a lot of space doesn't shrink the file until later.
- `--dict-samples N` — posts to sample when training the `post_tags` zstd
  dictionary (default 0 = the whole corpus).

Long-running maintenance draws a tqdm progress bar. Two details, both
deliberate: a resumed sweep opens at its true position rather than zero, and
all logging is routed through `tqdm.write` (`TqdmLoggingHandler`) so the
background scanner and favorites sync can't scribble over the bar. The bar
eases toward each batch boundary at a capped velocity instead of snapping
forward when a batch lands, so it moves smoothly and decelerates when a batch
runs slow rather than freezing.

## Notes & invariants

- e621's rate limit is 2 req/s; this app caps at 1 req/s
  (`MIN_REQUEST_INTERVAL = 1.0`) and requests 320 posts/page (the API max).
- e621 sorts by upload ID, newest first — new posts always surface at the top
  of page 1. The exhausted-query scanner relies on this; there is deliberately
  no "page-full" safety valve, and one should not be reintroduced.
- The exhausted list only ever contains single-token artist/character tags
  (the insertion path filters everything else), which is what makes blind
  OR-batching safe.
- Favorites enumeration is server-capped at 320 results/page regardless of
  the `limit` parameter; pagination stops on an empty page, not a short one.
- Marking-as-seen happens on serve, not on next-click: closing the tab still
  counts the post as seen. Trade-off: no duplicates after a crash.
- A tag that turns out to hold no posts at all is **expunged**: its query row
  and file lines are removed, but only after a sweep walks the tag's entire
  `status:deleted` history (capped at 25 pages) purging any locally-held IDs.
  Order matters — once the row is gone nothing would ever revisit the tag. The
  purge goes through `purge_deleted_post()`, so anything still favorited is
  un-favorited on e621 first and the favorites sync can't resurrect it.
- Werkzeug per-request logging is filtered out; everything else logs to
  stdout. Restart the process manually after edits — startup runs DB
  maintenance (`init_db` → schema/migrations → purge/canonicalize) and kicks
  off the background threads.
