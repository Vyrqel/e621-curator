import time
import urllib.parse

from .config import (
    ADDITIONS_ARTISTS_FILE,
    ADDITIONS_CHARACTERS_FILE,
    BLACKLIST_FILE,
    QUERIES_FILE,
    TAG_CATEGORIES,
)
from .database import db
from .runtime import log
from .taggraph import _tag_graph


def load_queries():
    """Parse queries.txt — extract the `tags` parameter from each e621 URL.

    Lines that aren't URLs are treated as raw tag strings.
    Lines starting with # are comments.
    """
    if not QUERIES_FILE.exists():
        return []
    queries = []
    for line in QUERIES_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("http"):
            parsed = urllib.parse.urlparse(line)
            params = urllib.parse.parse_qs(parsed.query)
            tags = params.get("tags", [None])[0]
            if tags:
                queries.append(tags.lower())
        else:
            queries.append(line.lower())
    return queries


def load_blacklist():
    """Load blacklist lines, parsed into AND-clauses of (tag, negated) tuples.

    Mirrors e621 semantics: each line is a space-separated list of tags that
    must ALL match for the line to apply. `-tag` means the post must NOT have
    that tag. `*` is a wildcard.

    Returns a list of clauses, each clause is a list of (pattern, negated).
    A post is blacklisted if it matches any clause.
    """
    if not BLACKLIST_FILE.exists():
        return []
    clauses = []
    for line in BLACKLIST_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        terms = []
        for token in line.split():
            token = token.lower()
            negated = token.startswith("-")
            if negated:
                token = token[1:]
            if token:
                terms.append((token, negated))
        if terms:
            clauses.append(terms)
    return clauses


def load_known_tags():
    """Build the set of tags considered 'already known'.

    A tag is known if it appears as a bare positive tag in queries.txt OR
    is in either additions file. Used to dim/checkmark tag chips in the UI.

    Skips negated tags (-tag), metatags (rating:s, score:>100, order:score),
    and other operators since those aren't artist/character names.

    Returns both the spelling as written and its alias-canonical form, so a
    query written before a tag was renamed still marks the renamed tag as
    known.
    """
    known = set()

    # From queries — extract bare tags only
    for query in load_queries():
        for token in query.split():
            token = token.lower().strip()
            if not token:
                continue
            if token.startswith("-"):
                continue  # negation
            if ":" in token:
                continue  # metatag like rating:s, order:score, score:>100
            if "*" in token:
                continue  # wildcard searches aren't specific tags
            known.add(token)

    # From additions files
    known |= read_additions_file("artist")
    known |= read_additions_file("character")

    return known | {_tag_graph.canonical(t) for t in known}


def _additions_file(category):
    """Return the path of the additions file for a category."""
    if category == "artist":
        return ADDITIONS_ARTISTS_FILE
    elif category == "character":
        return ADDITIONS_CHARACTERS_FILE
    return None


def read_additions_file(category):
    """Return the set of tags currently in the additions file for a category."""
    path = _additions_file(category)
    if not path or not path.exists():
        return set()
    return {
        line.strip().lower()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    }


def append_to_additions_file(tag, category):
    """Append a tag to the appropriate additions file (skips if already present)."""
    path = _additions_file(category)
    if path is None:
        return
    existing = read_additions_file(category)
    if tag.lower() in existing:
        return
    # Create with header if missing, then append
    if not path.exists():
        header = (
            f"# additions_{category}s.txt\n"
            f"# {category}s flagged from tag clicks during curation.\n"
            f"# Move entries to queries.txt as new search queries when ready.\n"
            f"# One tag per line. Lines starting with # are comments.\n\n"
        )
        path.write_text(header, encoding="utf-8")
    with path.open("a", encoding="utf-8") as f:
        f.write(f"{tag}\n")


def remove_from_additions_file(tag, category):
    """Remove a tag from the appropriate additions file. No-op if absent."""
    path = _additions_file(category)
    if path is None or not path.exists():
        return
    target = tag.lower()
    kept_lines = []
    for line in path.read_text(encoding="utf-8").splitlines():
        # Preserve comments, blank lines, and any tag that doesn't match
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or stripped.lower() != target:
            kept_lines.append(line)
    path.write_text("\n".join(kept_lines) + "\n", encoding="utf-8")


def reconcile_additions_files():
    """On startup, ensure every DB addition exists in its corresponding text file.

    Catches drift if files were edited externally or deleted while DB has rows.
    """
    with db() as conn:
        rows = conn.execute(
            "SELECT tag, category FROM additions ORDER BY added_at ASC"
        ).fetchall()

    missing_artists = 0
    missing_characters = 0
    artists_in_file = read_additions_file("artist")
    characters_in_file = read_additions_file("character")

    for row in rows:
        tag = row["tag"]
        category = row["category"]
        if category == "artist" and tag not in artists_in_file:
            append_to_additions_file(tag, "artist")
            artists_in_file.add(tag)
            missing_artists += 1
        elif category == "character" and tag not in characters_in_file:
            append_to_additions_file(tag, "character")
            characters_in_file.add(tag)
            missing_characters += 1

    if missing_artists or missing_characters:
        log.info(
            f"Reconciled additions files: +{missing_artists} artists, "
            f"+{missing_characters} characters"
        )


def sync_additions_files():
    """Push the two additions files into the DB and queries.txt.

    Direction is file -> everything else; the files are the source of truth.

      * additions table: replaced wholesale with the file contents, so a tag
        deleted from a file by hand disappears from the DB too (this is the
        handle for undoing accidental additions without the sqlite CLI).
        `added_at` is preserved for tags that survive the replace.
      * queries.txt: additive only. Any tag not already present as a bare
        positive tag in an existing query gets appended; nothing is removed
        or rewritten.

    Returns a dict summary.
    """
    file_tags = []  # [(tag, category)] in file order, artists first
    seen = set()
    for category in ("artist", "character"):
        for tag in sorted(read_additions_file(category)):
            if tag in seen:
                continue
            seen.add(tag)
            file_tags.append((tag, category))

    now = int(time.time())
    with db() as conn:
        old = {
            row["tag"]: row["added_at"]
            for row in conn.execute("SELECT tag, added_at FROM additions")
        }
        conn.execute("DELETE FROM additions")
        conn.executemany(
            "INSERT INTO additions (tag, category, added_at) VALUES (?, ?, ?)",
            [(tag, cat, old.get(tag, now)) for tag, cat in file_tags],
        )
    db_removed = len(set(old) - seen)
    db_added = len(seen - set(old))

    # queries.txt — additive only
    existing = set()
    for query in load_queries():
        for token in query.split():
            token = token.strip().lower()
            if token and not token.startswith("-") and ":" not in token:
                existing.add(token)
        existing.add(query.strip().lower())

    new_lines = [tag for tag, _ in file_tags if tag not in existing]
    if new_lines:
        if QUERIES_FILE.exists():
            current = QUERIES_FILE.read_text(encoding="utf-8")
        else:
            current = "# queries.txt\n"
        if current and not current.endswith("\n"):
            current += "\n"
        QUERIES_FILE.write_text(current + "\n".join(new_lines) + "\n", encoding="utf-8")

    return {
        "db_rows": len(file_tags),
        "db_added": db_added,
        "db_removed": db_removed,
        "queries_added": len(new_lines),
    }


def _tag_matches(pattern, post_tags):
    """Check if any tag in post_tags matches the pattern. Supports `*` wildcard."""
    if "*" not in pattern:
        return pattern in post_tags
    # Convert glob to regex-ish: simple prefix/suffix/middle match
    import fnmatch

    return any(fnmatch.fnmatchcase(t, pattern) for t in post_tags)


def post_tag_set(post):
    """Flatten every category of a post's tags into one lowercase set."""
    tags_dict = post.get("tags", {})
    all_tags = set()
    for category in TAG_CATEGORIES:
        for t in tags_dict.get(category, []):
            all_tags.add(t.lower())
    return all_tags


def is_blacklisted(post, clauses):
    """Return True if post matches any blacklist clause."""
    if not clauses:
        return False
    # Flatten all post tags into one lowercase set
    all_tags = post_tag_set(post)
    # Also add rating as a pseudo-tag (e621 does this: rating:s, rating:q, rating:e)
    rating = post.get("rating")
    if rating:
        all_tags.add(f"rating:{rating}")

    for clause in clauses:
        # All terms in this clause must be satisfied
        if all(
            (_tag_matches(pattern, all_tags) != negated) for pattern, negated in clause
        ):
            return True
    return False
