import logging
import sys
from datetime import datetime, timedelta

from flask import Flask
from tqdm import tqdm

from .config import CREDENTIALS_FILE, ROOT

# ---------- Credentials ----------


def _load_credentials():
    if not CREDENTIALS_FILE.exists():
        return "", ""
    creds = {}
    for line in CREDENTIALS_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            k, v = line.split("=", 1)
            creds[k.strip()] = v.strip()
    return creds.get("E621_USERNAME", ""), creds.get("E621_API_KEY", "")


E621_USERNAME, E621_API_KEY = _load_credentials()

# ---------- App ----------


class NoRequestFilter(logging.Filter):
    def filter(self, record):
        # Return False if the log message contains typical request patterns
        return not any(
            x in record.getMessage() for x in ["GET /", "POST /", "PUT /", "DELETE /"]
        )


class TqdmLoggingHandler(logging.StreamHandler):
    """Emit log records through `tqdm.write` so they don't shred a live bar.

    Matters now that a tag refresh can resume on a background thread while the
    server is up: the scanner, favourites sync and werkzeug all log to stderr
    while the refresh bar is drawing there. `tqdm.write` clears the bar, writes
    the line, and redraws it, instead of writing over the top of it.
    """

    def emit(self, record):
        try:
            tqdm.write(self.format(record), file=sys.stderr)
        except Exception:
            self.handleError(record)


# Apply the filter specifically to the Werkzeug logger
werkzeug_logger = logging.getLogger("werkzeug")
werkzeug_logger.addFilter(NoRequestFilter())


# templates/ and static/ live at the repository root, not inside this package,
# so Flask is pointed at ROOT rather than left to infer it from __name__.
app = Flask(__name__, root_path=str(ROOT))
log = logging.getLogger("Curator")


def _log_next(name, delay):
    """Log when a background thread will next wake up."""
    when = datetime.now().astimezone() + timedelta(seconds=delay)
    log.info(
        f"{name}: next activation at {when:%Y-%m-%d %H:%M:%S %Z} "
        f"(in {delay / 60:.1f} min)."
    )


# ---------- Database ----------
