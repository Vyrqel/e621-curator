"""
E621 Curator — backlog management tool.

Reads queries from queries.txt (one e621 search URL per line).
Already-tracked tags are inferred from queries.txt + the additions files.
Serves a minimal review interface: random image from random query, mark seen,
favorite, or extract artist/character additions.

This module is the entry point only: it parses the CLI, wires up logging, and
either runs a one-shot maintenance command or starts the server. Everything
else lives in the helpers/ package:

    helpers/config.py       constants, paths and tunables
    helpers/tagcodec.py     compact binary tag-blob codec (varint + zstd dict)
    helpers/runtime.py      credentials, logging, the Flask app and logger
    helpers/database.py     schema, migrations, connection helper, VACUUM
    helpers/userfiles.py    queries.txt / blacklist.txt / the additions files
    helpers/e6api.py        rate-limited e621 HTTP client and post accessors
    helpers/store.py        seen set, reservations, per-query progress tables
    helpers/scanner.py      page-1 scanner, export diff, empty-tag expunge
    helpers/maintenance.py  favorites sync and the maintenance thread
    helpers/search.py       query parsing, matching, unseen-post selection
    helpers/posts.py        post/tag/relation caching and purging
    helpers/tagcache.py     tag-cache backfill/refresh, dictionary retraining
    helpers/taggraph.py     tag exports: alias/implication graph, tag store
    helpers/web.py          Flask routes

Data files (curator.db, queries.txt, the CSV dumps) stay beside this file.
"""

import argparse
import logging

from helpers import config
from helpers.database import init_db, run_vacuum
from helpers.maintenance import start_maintenance
from helpers.runtime import TqdmLoggingHandler, app, log
from helpers.scanner import check_blacklist_change
from helpers.tagcache import (
    rebuild_tag_data,
    refresh_tag_cache,
    resume_interrupted_refresh,
)
from helpers.taggraph import start_tag_graph_sync
from helpers.userfiles import reconcile_additions_files, sync_additions_files

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="e6 curator — backlog management tool")
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host interface to bind (default: 0.0.0.0 — all interfaces).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8080,
        help="Port to listen on (default: 8080).",
    )
    parser.add_argument(
        "--dict-samples",
        type=int,
        default=0,
        help="Number of posts to sample for zstd dictionary training "
        "(default: 0 = use the entire corpus).",
    )
    parser.add_argument(
        "--refresh-tags",
        action="store_true",
        help="Re-fetch tags for every locally-referenced post (seen and "
        "favorites), overwriting the cache and purging posts confirmed gone "
        "from e621, then exit. Heavy — one API call per 320 posts. Resumes an "
        "interrupted sweep unless --no-resume is given.",
    )
    parser.add_argument(
        "--vacuum",
        action="store_true",
        help="Reclaim free pages and exit. Runs automatically after "
        "--rebuild-tag-data and --refresh-tags; this is the manual handle for "
        "everything else. Never runs while the server is up.",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Don't resume an interrupted tag refresh. On normal startup this "
        "discards the saved checkpoint and skips the resume entirely; with "
        "--refresh-tags it discards it and sweeps from the top.",
    )
    parser.add_argument(
        "--rebuild-tag-data",
        action="store_true",
        help="Re-ingest the tags/alias/implication exports, then retrain the "
        "zstd dictionary and rewrite every tag blob against it, then exit. The "
        "two halves are always run together — the retrain re-reduces tags "
        "using the graph, so it only means anything after a refresh.",
    )
    parser.add_argument(
        "--sync-additions",
        action="store_true",
        help="Push the two additions files into the DB (replacing the "
        "additions table wholesale, so hand-deleted entries go away) and "
        "append any tags missing from queries.txt, then exit. queries.txt is "
        "only added to, never rewritten.",
    )
    args = parser.parse_args()
    config.DICT_TRAIN_SAMPLES = max(0, args.dict_samples)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=[TqdmLoggingHandler()],
    )

    if args.vacuum:
        init_db()
        run_vacuum()
        raise SystemExit(0)

    if args.sync_additions:
        init_db()
        result = sync_additions_files()
        log.info(
            f"Additions sync: additions table now {result['db_rows']} row(s) "
            f"(+{result['db_added']}, -{result['db_removed']}); "
            f"{result['queries_added']} tag(s) appended to queries.txt"
        )
        raise SystemExit(0)

    if args.rebuild_tag_data:
        init_db()
        try:
            result = rebuild_tag_data()
        except Exception as e:
            log.error(f"Tag data rebuild failed: {e}")
            raise SystemExit(1)
        log.info(f"Tag data rebuild complete: {result}")
        raise SystemExit(0)

    if args.refresh_tags:
        init_db()
        try:
            result = refresh_tag_cache(resume=not args.no_resume)
        except Exception as e:
            log.error(f"Tag refresh failed: {e}")
            raise SystemExit(1)
        log.info(f"Tag refresh result: {result}")
        run_vacuum()
        raise SystemExit(0)

    init_db()
    resume_interrupted_refresh(cancel=args.no_resume)
    reconcile_additions_files()
    check_blacklist_change()
    start_maintenance(first=True)
    start_tag_graph_sync()
    app.run(host=args.host, port=args.port, debug=False)
