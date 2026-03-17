"""
agent/memory.py
Lightweight JSON context store.
Persists key facts, decisions, and task history across sessions.
"""
import json
from datetime import datetime, timezone
from pathlib import Path

MEMORY_FILE = Path(__file__).parent / "memory.json"
MAX_HISTORY = 50


def _load() -> dict:
    if MEMORY_FILE.exists():
        try:
            return json.loads(MEMORY_FILE.read_text())
        except Exception:
            pass
    return {"facts": {}, "history": []}


def _save(data: dict) -> None:
    MEMORY_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False))


def get_facts() -> dict:
    return _load()["facts"]


def set_fact(key: str, value) -> None:
    data = _load()
    data["facts"][key] = value
    _save(data)


def get_history(n: int = 10) -> list:
    return _load()["history"][-n:]


def add_history(role: str, content: str) -> None:
    data = _load()
    data["history"].append({
        "role": role,
        "content": content,
        "ts": datetime.now(timezone.utc).isoformat(),
    })
    # trim
    if len(data["history"]) > MAX_HISTORY:
        data["history"] = data["history"][-MAX_HISTORY:]
    _save(data)


def clear_history() -> None:
    data = _load()
    data["history"] = []
    _save(data)


def summary() -> str:
    facts = get_facts()
    if not facts:
        return "No stored facts."
    return "\n".join(f"- {k}: {v}" for k, v in facts.items())
