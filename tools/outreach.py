"""
agent/tools/outreach.py
Query outreach state. Read-only queries + trigger actions via outreach module.
"""
import sqlite3
import sys
from pathlib import Path

LEADS_DB = Path(__file__).parent.parent.parent / "leadgen" / "data" / "leads.db"
OUTREACH_DIR = Path(__file__).parent.parent.parent / "outreach"


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(LEADS_DB)
    conn.row_factory = sqlite3.Row
    return conn


def stats() -> str:
    conn = _conn()
    row = conn.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE status='sent')    AS sent,
            COUNT(*) FILTER (WHERE status='pending') AS pending,
            COUNT(*) FILTER (WHERE status='failed')  AS failed,
            COUNT(*) FILTER (WHERE status='skipped') AS skipped
        FROM outreach_log
        """
    ).fetchone()
    replies = conn.execute("SELECT COUNT(*) AS n FROM replies").fetchone()["n"]
    interested = conn.execute(
        "SELECT COUNT(*) AS n FROM reply_classification WHERE label='interested'"
    ).fetchone()["n"]
    return (
        f"Sent: {row['sent']} | Pending: {row['pending']} | "
        f"Failed: {row['failed']} | Skipped: {row['skipped']}\n"
        f"Replies: {replies} | Interested: {interested}"
    )


def recent_replies(n: int = 5) -> str:
    conn = _conn()
    rows = conn.execute(
        """
        SELECT b.name, r.channel, r.received_at, rc.label, r.content
        FROM replies r
        JOIN businesses b ON b.id = r.lead_id
        LEFT JOIN reply_classification rc ON rc.reply_id = r.id
        ORDER BY r.received_at DESC
        LIMIT ?
        """,
        (n,),
    ).fetchall()
    if not rows:
        return "No replies yet."
    lines = []
    for r in rows:
        lines.append(
            f"[{r['label'] or 'unclassified'}] {r['name']} via {r['channel']} "
            f"({r['received_at'][:10]})\n  {r['content'][:200]}"
        )
    return "\n".join(lines)


def pending_drafts_count() -> str:
    conn = _conn()
    n = conn.execute(
        "SELECT COUNT(*) AS n FROM outreach_log WHERE status='pending'"
    ).fetchone()["n"]
    return f"{n} drafts pending review"
