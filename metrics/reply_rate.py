"""
agent/metrics/reply_rate.py
Prints the reply rate (replies / sent) as a float to stdout.
Used by autoresearch as: cmd: python agent/metrics/reply_rate.py

Optionally filter to last N days:
  python agent/metrics/reply_rate.py --days 7
"""
import argparse
import sqlite3
import sys
from pathlib import Path

LEADS_DB = Path(__file__).parent.parent.parent / "leadgen" / "data" / "leads.db"


def reply_rate(days: int | None = None) -> float:
    if not LEADS_DB.exists():
        print(0.0)
        return 0.0

    conn = sqlite3.connect(LEADS_DB)
    conn.row_factory = sqlite3.Row

    date_filter = ""
    if days:
        date_filter = f"AND created_at >= datetime('now', '-{days} days')"

    sent = conn.execute(
        f"SELECT COUNT(*) AS n FROM outreach_log WHERE status = 'sent' {date_filter}"
    ).fetchone()["n"]

    if sent == 0:
        print(0.0)
        return 0.0

    replied = conn.execute(
        f"""
        SELECT COUNT(DISTINCT r.lead_id) AS n
        FROM replies r
        JOIN outreach_log o ON o.lead_id = r.lead_id
        WHERE o.status = 'sent' {date_filter}
        """
    ).fetchone()["n"]

    rate = replied / sent
    print(rate)
    return rate


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=None)
    args = parser.parse_args()
    reply_rate(args.days)
