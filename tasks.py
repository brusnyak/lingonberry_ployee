"""
agent/tasks.py
Persistent task queue. Tasks are stored in tasks.json.
Each task has: id, description, status, result, created_at, updated_at
Statuses: pending → running → done | needs_input | failed
"""
import json
import uuid
from datetime import datetime
from pathlib import Path

TASKS_FILE = Path(__file__).parent / "tasks.json"


def _load() -> list:
    if not TASKS_FILE.exists():
        return []
    return json.loads(TASKS_FILE.read_text())


def _save(tasks: list) -> None:
    TASKS_FILE.write_text(json.dumps(tasks, indent=2))


def add(description: str) -> dict:
    tasks = _load()
    task = {
        "id": str(uuid.uuid4())[:8],
        "description": description,
        "status": "pending",
        "result": None,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
    }
    tasks.append(task)
    _save(tasks)
    return task


def get_all() -> list:
    return _load()


def get_pending() -> list:
    return [t for t in _load() if t["status"] == "pending"]


def get_by_id(task_id: str) -> dict | None:
    return next((t for t in _load() if t["id"] == task_id), None)


def update(task_id: str, status: str, result: str = None) -> None:
    tasks = _load()
    for t in tasks:
        if t["id"] == task_id:
            t["status"] = status
            t["updated_at"] = datetime.now().isoformat()
            if result is not None:
                t["result"] = result
            break
    _save(tasks)


def delete(task_id: str) -> bool:
    tasks = _load()
    new = [t for t in tasks if t["id"] != task_id]
    if len(new) == len(tasks):
        return False
    _save(new)
    return True


def clear_done() -> int:
    tasks = _load()
    remaining = [t for t in tasks if t["status"] not in ("done", "failed")]
    removed = len(tasks) - len(remaining)
    _save(remaining)
    return removed


def summary() -> str:
    tasks = _load()
    if not tasks:
        return "No tasks."
    lines = []
    for t in tasks:
        icon = {"pending": "⏳", "running": "🔄", "done": "✅", "needs_input": "❓", "failed": "❌"}.get(t["status"], "•")
        lines.append(f"{icon} [{t['id']}] {t['description'][:60]}")
    return "\n".join(lines)
