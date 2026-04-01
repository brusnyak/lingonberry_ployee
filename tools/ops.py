"""
agent/tools/ops.py
Operational visibility into the agent job queue and event log.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import tasks
import events


def jobs_summary() -> str:
    return tasks.ops_summary()


def jobs_list() -> str:
    return tasks.summary()


def task_create(description: str, kind: str = "agent_queued", priority: int = 50) -> str:
    created = tasks.add(description, kind=kind, priority=priority)
    return f"Created task [{created['id']}]: {description}"


def task_complete(task_id: str, result: str) -> str:
    completed = tasks.complete(task_id, result=result)
    if completed:
        return f"Task [{task_id}] marked done."
    return f"Task [{task_id}] not found or could not be completed."


def recent_events(n: int = 10) -> str:
    return events.summary(n)
