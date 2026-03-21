"""
agent/tools/content.py
Lightweight access to Victor content planning and queue state.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

CONTENT_DIR = Path(__file__).parent.parent.parent / "content"
PLAYWRIGHT_PYTHON = Path(__file__).parent.parent.parent / "leadgen" / ".venv" / "bin" / "python"

def _run(script: str, *args: str, python: str | None = None) -> str:
    interpreter = python or sys.executable
    result = subprocess.run(
        [interpreter, script, *args],
        cwd=str(CONTENT_DIR),
        capture_output=True,
        text=True,
        timeout=600,
    )
    output = ((result.stdout or "") + (result.stderr or "")).strip()
    if result.returncode != 0:
        raise RuntimeError(output or f"{script} failed with exit {result.returncode}")
    return output or "(no output)"


def report() -> str:
    return _run("report.py")


def plan_posts(count: int = 5, queue: bool = False) -> str:
    args = ["planner.py", "--count", str(count)]
    if queue:
        args.append("--queue")
    return _run(*args)


def prompt_manifests(item_id: str = "") -> str:
    args = ["generate.py", "--manifest-only"]
    if item_id:
        args.extend(["--id", item_id])
    return _run(*args)


def engagement_plan(niches: list[str] | None = None) -> str:
    args = ["engagement.py", "--plan"]
    if niches:
        args.extend(["--niches", *niches])
    return _run(*args)


def engagement_log() -> str:
    return _run("engagement.py", "--log")


def run_engagement_session(
    niches: list[str] | None = None,
    dry_run: bool = False,
    discover_only: bool = False,
) -> str:
    """Run a live Playwright engagement session on Instagram."""
    python = str(PLAYWRIGHT_PYTHON) if PLAYWRIGHT_PYTHON.exists() else sys.executable
    args = ["engagement.py"]
    if niches:
        args.extend(["--niches", *niches])
    if dry_run:
        args.append("--dry-run")
    if discover_only:
        args.append("--discover-only")
    return _run(*args, python=python)


def generate_images(item_id: str, sample_count: int = 2, aspect_ratio: str = "3:4") -> str:
    return _run(
        "generate.py",
        "--id",
        item_id,
        "--sample-count",
        str(sample_count),
        "--aspect-ratio",
        aspect_ratio,
    )


def provider_status() -> str:
    return _run("generate.py", "--provider-status")


def approve_post(item_id: str) -> str:
    return _run("approve.py", item_id, "approved")


def reject_post(item_id: str) -> str:
    return _run("approve.py", item_id, "rejected")


def prepare_publish(item_id: str, publish_after: str = "") -> str:
    args = ["publish.py", item_id]
    if publish_after:
        args.extend(["--publish-after", publish_after])
    return _run(*args)


def publish_post(item_id: str) -> str:
    python = str(PLAYWRIGHT_PYTHON) if PLAYWRIGHT_PYTHON.exists() else sys.executable
    return _run("poster.py", item_id, "--submit", python=python)


def plan_calendar(weeks: int = 2, queue: bool = False) -> str:
    """Generate a structured content calendar and optionally queue all posts."""
    import sys
    sys.path.insert(0, str(CONTENT_DIR.parent))
    try:
        from content.content_calendar import generate_calendar, format_calendar_telegram
    except ImportError:
        from content_calendar import generate_calendar, format_calendar_telegram
    posts = generate_calendar(weeks=weeks, queue=queue)
    return format_calendar_telegram(posts)
