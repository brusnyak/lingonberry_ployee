"""
agent/tasks.py
Durable JSON-backed job queue for long-running operations.
"""
import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from . import events
except ImportError:
    import events

TASKS_FILE = Path(__file__).parent / "tasks.json"
LOCK_FILE = Path(__file__).parent / "tasks.lock"
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_PRIORITY = 50
DEFAULT_LEASE_SECONDS = 900
LOCK_STALE_SECONDS = 30


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now().isoformat()


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


class _FileLock:
    def __enter__(self):
        deadline = time.time() + 5
        while True:
            try:
                fd = os.open(LOCK_FILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, str(os.getpid()).encode("utf-8"))
                os.close(fd)
                return self
            except FileExistsError:
                try:
                    age = time.time() - LOCK_FILE.stat().st_mtime
                    if age > LOCK_STALE_SECONDS:
                        LOCK_FILE.unlink(missing_ok=True)
                        continue
                except FileNotFoundError:
                    continue
                if time.time() >= deadline:
                    raise TimeoutError("Timed out waiting for task lock")
                time.sleep(0.05)

    def __exit__(self, exc_type, exc, tb):
        try:
            LOCK_FILE.unlink()
        except FileNotFoundError:
            pass


def _load_unlocked() -> list[dict]:
    if not TASKS_FILE.exists():
        return []
    try:
        data = json.loads(TASKS_FILE.read_text())
        if isinstance(data, list):
            return [_normalize_task(task) for task in data if isinstance(task, dict)]
    except Exception:
        pass
    return []


def _save_unlocked(tasks: list[dict]) -> None:
    TASKS_FILE.write_text(json.dumps(tasks, indent=2, ensure_ascii=False))


def _normalize_task(task: dict) -> dict:
    task = dict(task)
    now = _now_iso()
    task.setdefault("id", str(uuid.uuid4())[:8])
    task.setdefault("kind", "general")
    task.setdefault("goal", task.get("description", ""))
    task.setdefault("objective", task.get("description", ""))
    task.setdefault("priority", DEFAULT_PRIORITY)
    task.setdefault("status", "pending")
    task.setdefault("result", None)
    task.setdefault("artifacts", {})
    task.setdefault("allowed_actions", ["internal_safe"])
    task.setdefault("approval_mode", "internal_only")
    task.setdefault("stop_conditions", ["ask_when_blocked"])
    task.setdefault("outputs_expected", [])
    task.setdefault("status_summary", "")
    task.setdefault("blockers", [])
    task.setdefault("required_approvals", [])
    task.setdefault("pending_access", [])
    task.setdefault("attempts", 0)
    task.setdefault("max_attempts", DEFAULT_MAX_ATTEMPTS)
    task.setdefault("retry_count", max(task.get("attempts", 0) - 1, 0))
    task.setdefault("next_run_at", now)
    task.setdefault("created_at", now)
    task.setdefault("updated_at", now)
    task.setdefault("lease_expires_at", None)
    task.setdefault("owner", None)
    task.setdefault("last_error", None)
    return task


def _load() -> list[dict]:
    with _FileLock():
        tasks = _load_unlocked()
        _save_unlocked(tasks)
        return tasks


def _with_tasks(mutator):
    with _FileLock():
        tasks = _load_unlocked()
        result = mutator(tasks)
        _save_unlocked(tasks)
        return result


def _is_ready(task: dict, now: datetime | None = None) -> bool:
    now = now or _now()
    if task["status"] not in {"pending", "retry"}:
        return False
    next_run = _parse_ts(task.get("next_run_at"))
    return next_run is None or next_run <= now


def _is_expired(task: dict, now: datetime | None = None) -> bool:
    now = now or _now()
    lease = _parse_ts(task.get("lease_expires_at"))
    return task["status"] == "running" and lease is not None and lease <= now


def _sort_key(task: dict):
    next_run = _parse_ts(task.get("next_run_at")) or _now()
    return (int(task.get("priority", DEFAULT_PRIORITY)), next_run, task.get("created_at", ""))


def add(
    description: str,
    *,
    kind: str = "general",
    priority: int = DEFAULT_PRIORITY,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    next_run_at: str | None = None,
    artifacts: dict | None = None,
    allowed_actions: list[str] | None = None,
    approval_mode: str = "internal_only",
    stop_conditions: list[str] | None = None,
    outputs_expected: list[str] | None = None,
) -> dict:
    created = {}

    def mutator(tasks: list[dict]):
        task = _normalize_task({
            "id": str(uuid.uuid4())[:8],
            "description": description,
            "kind": kind,
            "priority": priority,
            "max_attempts": max_attempts,
            "artifacts": artifacts or {},
            "allowed_actions": allowed_actions or ["internal_safe"],
            "approval_mode": approval_mode,
            "stop_conditions": stop_conditions or ["ask_when_blocked"],
            "outputs_expected": outputs_expected or [],
            "next_run_at": next_run_at or _now_iso(),
            "status": "pending",
            "attempts": 0,
            "retry_count": 0,
            "result": None,
            "last_error": None,
        })
        tasks.append(task)
        created.update(task)

    _with_tasks(mutator)
    events.add("task_added", f"[{created['id']}] {description}", {"task_id": created["id"], "kind": kind})
    return created


def get_all() -> list[dict]:
    return _load()


def get_pending() -> list[dict]:
    now = _now()
    tasks = _load()
    return sorted(
        [task for task in tasks if _is_ready(task, now)],
        key=_sort_key,
    )


def get_by_id(task_id: str) -> dict | None:
    return next((task for task in _load() if task["id"] == task_id), None)


def get_next_ready() -> dict | None:
    pending = get_pending()
    return pending[0] if pending else None


def claim_next(owner: str = "agent", lease_seconds: int = DEFAULT_LEASE_SECONDS) -> dict | None:
    claimed = {}

    def mutator(tasks: list[dict]):
        now = _now()

        for task in tasks:
            if _is_expired(task, now):
                task["status"] = "retry"
                task["lease_expires_at"] = None
                task["owner"] = None
                task["updated_at"] = _now_iso()
                task["last_error"] = "Lease expired"

        ready = sorted((task for task in tasks if _is_ready(task, now)), key=_sort_key)
        if not ready:
            return

        task = ready[0]
        task["status"] = "running"
        task["owner"] = owner
        task["attempts"] = int(task.get("attempts", 0)) + 1
        task["retry_count"] = max(task["attempts"] - 1, 0)
        task["lease_expires_at"] = (now + timedelta(seconds=lease_seconds)).isoformat()
        task["updated_at"] = now.isoformat()
        claimed.update(task)

    _with_tasks(mutator)
    if claimed:
        events.add(
            "task_claimed",
            f"[{claimed['id']}] claimed by {owner}",
            {"task_id": claimed["id"], "owner": owner, "attempt": claimed["attempts"]},
        )
        return claimed
    return None


def heartbeat(task_id: str, lease_seconds: int = DEFAULT_LEASE_SECONDS) -> bool:
    touched = {"ok": False}

    def mutator(tasks: list[dict]):
        for task in tasks:
            if task["id"] == task_id and task["status"] == "running":
                task["lease_expires_at"] = (_now() + timedelta(seconds=lease_seconds)).isoformat()
                task["updated_at"] = _now_iso()
                touched["ok"] = True
                return

    _with_tasks(mutator)
    return touched["ok"]


def update(task_id: str, status: str, result: str = None) -> None:
    def mutator(tasks: list[dict]):
        for task in tasks:
            if task["id"] == task_id:
                task["status"] = status
                task["updated_at"] = _now_iso()
                if result is not None:
                    task["result"] = result
                if status != "running":
                    task["lease_expires_at"] = None
                    task["owner"] = None
                return

    _with_tasks(mutator)


def set_artifact(task_id: str, key: str, value) -> bool:
    changed = {"ok": False}

    def mutator(tasks: list[dict]):
        for task in tasks:
            if task["id"] == task_id:
                task.setdefault("artifacts", {})[key] = value
                task["updated_at"] = _now_iso()
                changed["ok"] = True
                return

    _with_tasks(mutator)
    return changed["ok"]


def set_status_summary(task_id: str, summary: str) -> bool:
    changed = {"ok": False}

    def mutator(tasks: list[dict]):
        for task in tasks:
            if task["id"] == task_id:
                task["status_summary"] = summary
                task["updated_at"] = _now_iso()
                changed["ok"] = True
                return

    _with_tasks(mutator)
    return changed["ok"]


def set_blockers(task_id: str, blockers: list[str], approvals: list[str] | None = None, pending_access: list[str] | None = None) -> bool:
    changed = {"ok": False}

    def mutator(tasks: list[dict]):
        for task in tasks:
            if task["id"] == task_id:
                task["blockers"] = blockers
                if approvals is not None:
                    task["required_approvals"] = approvals
                if pending_access is not None:
                    task["pending_access"] = pending_access
                task["updated_at"] = _now_iso()
                changed["ok"] = True
                return

    _with_tasks(mutator)
    return changed["ok"]


def complete(task_id: str, result: str = None, artifacts: dict | None = None) -> dict | None:
    completed = {}

    def mutator(tasks: list[dict]):
        for task in tasks:
            if task["id"] == task_id:
                task["status"] = "done"
                task["result"] = result
                if artifacts:
                    task.setdefault("artifacts", {}).update(artifacts)
                task["lease_expires_at"] = None
                task["owner"] = None
                task["last_error"] = None
                task["updated_at"] = _now_iso()
                completed.update(task)
                return

    _with_tasks(mutator)
    if completed:
        events.add("task_done", f"[{task_id}] completed", {"task_id": task_id})
        return completed
    return None


def fail(task_id: str, error: str, retry_delay_seconds: int = 300) -> dict | None:
    failed = {}

    def mutator(tasks: list[dict]):
        for task in tasks:
            if task["id"] != task_id:
                continue
            task["last_error"] = error
            task["lease_expires_at"] = None
            task["owner"] = None
            task["updated_at"] = _now_iso()
            if int(task.get("attempts", 0)) < int(task.get("max_attempts", DEFAULT_MAX_ATTEMPTS)):
                task["status"] = "retry"
                task["next_run_at"] = (_now() + timedelta(seconds=retry_delay_seconds)).isoformat()
            else:
                task["status"] = "failed"
            failed.update(task)
            return

    _with_tasks(mutator)
    if failed:
        event_kind = "task_retry" if failed["status"] == "retry" else "task_failed"
        events.add(
            event_kind,
            f"[{task_id}] {failed['status']}: {error}",
            {"task_id": task_id, "status": failed["status"], "error": error},
            level="warning" if failed["status"] == "retry" else "error",
        )
        return failed
    return None


def mark_needs_input(task_id: str, result: str) -> dict | None:
    needed = {}

    def mutator(tasks: list[dict]):
        for task in tasks:
            if task["id"] == task_id:
                task["status"] = "needs_input"
                task["result"] = result
                task["lease_expires_at"] = None
                task["owner"] = None
                task["updated_at"] = _now_iso()
                needed.update(task)
                return

    _with_tasks(mutator)
    if needed:
        events.add("task_needs_input", f"[{task_id}] waiting for user input", {"task_id": task_id}, level="warning")
        return needed
    return None


def delete(task_id: str) -> bool:
    removed = {"ok": False}

    def mutator(tasks: list[dict]):
        new_tasks = [task for task in tasks if task["id"] != task_id]
        if len(new_tasks) != len(tasks):
            tasks[:] = new_tasks
            removed["ok"] = True

    _with_tasks(mutator)
    if removed["ok"]:
        events.add("task_deleted", f"[{task_id}] deleted", {"task_id": task_id})
    return removed["ok"]


def clear_done() -> int:
    removed = {"count": 0}

    def mutator(tasks: list[dict]):
        remaining = [task for task in tasks if task["status"] not in {"done", "failed"}]
        removed["count"] = len(tasks) - len(remaining)
        tasks[:] = remaining

    _with_tasks(mutator)
    if removed["count"]:
        events.add("task_cleared", f"Cleared {removed['count']} completed/failed tasks", {"count": removed["count"]})
    return removed["count"]


def summary() -> str:
    tasks = _load()
    if not tasks:
        return "No tasks."

    lines = []
    for task in sorted(tasks, key=_sort_key):
        icon = {
            "pending": "⏳",
            "retry": "🔁",
            "running": "🔄",
            "done": "✅",
            "needs_input": "❓",
            "failed": "❌",
        }.get(task["status"], "•")
        suffix = f" | kind={task['kind']} | p={task['priority']} | tries={task['attempts']}/{task['max_attempts']}"
        if task["status"] in {"pending", "retry"}:
            suffix += f" | next={task['next_run_at'][:16]}"
        if task["status"] == "running" and task.get("owner"):
            suffix += f" | owner={task['owner']}"
        if task.get("status_summary"):
            suffix += f" | {task['status_summary'][:40]}"
        lines.append(f"{icon} [{task['id']}] {task['description'][:60]}{suffix}")
    return "\n".join(lines)


def ops_summary() -> str:
    tasks = _load()
    if not tasks:
        return "No queued jobs."

    counts = {}
    for task in tasks:
        counts[task["status"]] = counts.get(task["status"], 0) + 1

    ordered = ["pending", "retry", "running", "needs_input", "done", "failed"]
    parts = [f"{status}={counts.get(status, 0)}" for status in ordered if counts.get(status, 0)]
    next_task = get_next_ready()
    tail = f"\nNext ready: [{next_task['id']}] {next_task['description']}" if next_task else "\nNext ready: none"
    return "Jobs: " + " | ".join(parts) + tail
