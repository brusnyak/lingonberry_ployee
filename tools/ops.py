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


def recent_events(n: int = 10) -> str:
    return events.summary(n)
