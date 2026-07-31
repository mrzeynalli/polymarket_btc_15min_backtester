from __future__ import annotations

from pathlib import Path


def retention_candidates(root: Path, older_than_utc_ns: int) -> list[Path]:
    """Return candidates only; raw data is never deleted automatically."""
    candidates: list[Path] = []
    for path in root.rglob("*"):
        if path.is_file() and path.stat().st_mtime_ns < older_than_utc_ns:
            candidates.append(path)
    return sorted(candidates)
