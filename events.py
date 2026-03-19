"""
agent/events.py
Persistent event log for agent and workflow operations.
"""
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

EVENTS_FILE = Path(__file__).parent / "events.json"
MAX_EVENTS = 500


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load() -> list[dict]:
    if not EVENTS_FILE.exists():
        return []
    try:
        data = json.loads(EVENTS_FILE.read_text())
        if isinstance(data, list):
            return data
    except Exception:
        pass
    return []


def _save(events: list[dict]) -> None:
    EVENTS_FILE.write_text(json.dumps(events[-MAX_EVENTS:], indent=2, ensure_ascii=False))


def add(kind: str, message: str, details: dict | None = None, level: str = "info") -> dict:
    events = _load()
    event = {
        "id": str(uuid.uuid4())[:8],
        "kind": kind,
        "level": level,
        "message": message,
        "details": details or {},
        "created_at": _now(),
    }
    events.append(event)
    _save(events)
    return event


def recent(n: int = 20, kind: str | None = None) -> list[dict]:
    events = _load()
    if kind:
        events = [event for event in events if event["kind"] == kind]
    return events[-n:]


def summary(n: int = 10) -> str:
    rows = recent(n)
    if not rows:
        return "No recent events."
    lines = []
    for row in rows:
        lines.append(
            f"- [{row['level']}] {row['kind']} at {row['created_at'][:19]}: {row['message']}"
        )
    return "\n".join(lines)
