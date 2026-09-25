import hashlib
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
        tags = _query_line_tags(line)
        if tags:
            queries.append(tags)
    return queries


def _query_line_tags(line):
    """The lowercase tag string a queries.txt line stands for, or None for
    comments, blanks and URLs without a `tags` parameter."""
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    if line.startswith("http"):
        params = urllib.parse.parse_qs(urllib.parse.urlparse(line).query)
        tags = params.get("tags", [None])[0]
        return tags.lower() if tags else None
    return line.lower()


def _has_positive_tag(tags, tag):
    """True if the tag string requires `tag` (alias-resolved). Negated terms
    don't count — `-alcohol` is a query that already avoids it."""
    return any(
        not t.startswith("-") and _tag_graph.canonical(t) == tag
        for t in tags.split()
    )


def find_queries_with_tag(tag):
    """Queries in queries.txt that require `tag`, for the purge preview."""
    tag = _tag_graph.canonical(tag.strip().lower())
    return [q for q in load_queries() if _has_positive_tag(q, tag)]


def remove_queries_with_tag(tag):
    """Delete every queries.txt line that requires `tag`, plus its progress
    row. Comments and other lines are kept as-is. Returns the removed
    queries."""
    if not QUERIES_FILE.exists():
        return []
    tag = _tag_graph.canonical(tag.strip().lower())
    kept, removed = [], []
    for line in QUERIES_FILE.read_text(encoding="utf-8").splitlines():
        tags = _query_line_tags(line)
        if tags and _has_positive_tag(tags, tag):
            removed.append(tags)
        else:
            kept.append(line)
    if removed:
        QUERIES_FILE.write_text("\n".join(kept) + "\n", encoding="utf-8")
        with db() as conn:
            conn.executemany(
                "DELETE FROM query_progress WHERE query_hash = ?",
                [(hashlib.sha256(q.encode()).hexdigest(),) for q in removed],
            )
    return removed


def find_additions_with_tag(tag):
    """(tag, category) pairs in the additions files/table matching `tag`."""
    tag = _tag_graph.canonical(tag.strip().lower())
    found = set()
    for category in ("artist", "character"):
        for t in read_additions_file(category):
            if _tag_graph.canonical(t) == tag:
                found.add((t, category))
    with db() as conn:
        for row in conn.execute("SELECT tag, category FROM additions"):
            if _tag_graph.canonical(row["tag"]) == tag:
                found.add((row["tag"], row["category"]))
    return sorted(found)


def remove_additions_with_tag(tag):
    """Drop `tag` from both additions files and the additions table.
    Returns the (tag, category) pairs removed."""
    found = find_additions_with_tag(tag)
    with db() as conn:
        for t, category in found:
            remove_from_additions_file(t, category)
            conn.execute("DELETE FROM additions WHERE tag = ?", (t,))
    return found


def append_to_blacklist(line):
    """Append a clause line to blacklist.txt. Returns False if an identical
    clause (same terms, any order) is already there."""
    terms = line.lower().split()
    if not terms:
        return False
    existing = [
        sorted(("-" if neg else "") + pat for pat, neg in clause)
        for clause in load_blacklist()
    ]
    if sorted(terms) in existing:
        return False
    text = BLACKLIST_FILE.read_text(encoding="utf-8") if BLACKLIST_FILE.exists() else ""
    if text and not text.endswith("\n"):
        text += "\n"
    BLACKLIST_FILE.write_text(text + " ".join(terms) + "\n", encoding="utf-8")
    return True


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


def append_to_queries_file(tags):
    """Append tags to queries.txt. Additive only.

    A tag already present as a bare positive tag in any existing query is
    skipped. Returns the tags actually appended.
    """
    existing = set()
    for query in load_queries():
        for token in query.split():
            token = token.strip().lower()
            if token and not token.startswith("-") and ":" not in token:
                existing.add(token)
        existing.add(query.strip().lower())

    new_lines = []
    for tag in tags:
        tag = tag.strip().lower()
        if tag and tag not in existing:
            existing.add(tag)
            new_lines.append(tag)
    if new_lines:
        if QUERIES_FILE.exists():
            current = QUERIES_FILE.read_text(encoding="utf-8")
        else:
            current = "# queries.txt\n"
        if current and not current.endswith("\n"):
            current += "\n"
        QUERIES_FILE.write_text(current + "\n".join(new_lines) + "\n", encoding="utf-8")
    return new_lines


def append_to_additions_file(tag, category):
    """Append a tag to the appropriate additions file (skips if already
    present), and to queries.txt so it's served without waiting for a sync."""
    path = _additions_file(category)
    if path is None:
        return
    append_to_queries_file([tag])
    existing = read_additions_file(category)
    if tag.lower() in existing:
        return
    # Create with header if missing, then append
    if not path.exists():
        header = (
            f"# additions_{category}s.txt\n"
            f"# {category}s flagged from tag clicks during curation.\n"
            f"# Entries are added to queries.txt automatically.\n"
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

    new_lines = append_to_queries_file([tag for tag, _ in file_tags])

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
