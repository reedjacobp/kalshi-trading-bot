"""
Single source of truth for where the bot reads and writes collected data.

Historically the codebase had three conventions living side by side:
  - kalshi_ws.TickRecorder honored $DATA_DIR (writes to /mnt/d/...)
  - bot.py hardcoded "data/prices", "data/prices_hf", "data/orderbooks"
  - optimize_rr.py + analyze_*.py each reimplemented their own fallback chain

That split produced split-brain storage: ticks lived on /mnt/d while
prices/orderbooks lived under the repo's data/ directory, and the optimizer
had to keep "find it wherever it is" code in every loader.

This module centralizes the resolution so every caller agrees.

Resolution order (first hit wins):
  1. $DATA_DIR environment variable
  2. python-bot/data/  (original relative location, preserved for anyone
     who hasn't set $DATA_DIR — nothing breaks on unconfigured machines)

`ensure(subdir)` returns an existing path. It will create the subdir under
the primary location if missing, which is safe because the bot has always
mkdir'd its own output directories.

`resolve(subdir)` returns whichever of (primary, legacy) actually contains
files for that subdir — useful for read-side callers when a migration is
only half-done.
"""

from __future__ import annotations

import os
from pathlib import Path


_THIS_DIR = Path(__file__).resolve().parent
_LEGACY_ROOT = _THIS_DIR / "data"


def root() -> Path:
    """Primary data root. $DATA_DIR if set, else python-bot/data."""
    env = os.environ.get("DATA_DIR")
    if env:
        return Path(env).expanduser()
    return _LEGACY_ROOT


def legacy_root() -> Path:
    """Repo-relative data/ directory, used as a read-fallback during migration."""
    return _LEGACY_ROOT


def ensure(subdir: str) -> Path:
    """Return root/subdir, creating it if missing. For writers."""
    p = root() / subdir
    p.mkdir(parents=True, exist_ok=True)
    return p


def resolve(subdir: str) -> Path:
    """Return whichever of (root/subdir, legacy/subdir) exists with files.

    For readers. Prefers the primary root; falls back to the legacy path
    only if the primary is missing or empty. If neither has files, returns
    the primary (possibly empty) path so callers can still iterate.
    """
    primary = root() / subdir
    if primary.exists():
        try:
            if any(primary.iterdir()):
                return primary
        except OSError:
            pass
    legacy = _LEGACY_ROOT / subdir
    if legacy != primary and legacy.exists():
        try:
            if any(legacy.iterdir()):
                return legacy
        except OSError:
            pass
    return primary


def all_candidates(subdir: str) -> list[Path]:
    """Return both root/subdir and legacy/subdir (deduped, existence-filtered).

    For readers that want to union data across both locations — useful if
    a user migrated midway through a session and wants the optimizer to see
    both halves of the history.
    """
    out: list[Path] = []
    seen: set[Path] = set()
    for p in (root() / subdir, _LEGACY_ROOT / subdir):
        p = p.resolve() if p.exists() else p
        if p in seen:
            continue
        seen.add(p)
        if p.exists():
            out.append(p)
    return out
