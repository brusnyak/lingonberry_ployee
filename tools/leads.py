"""
agent/tools/leads.py
Query leadgen DB. Read-only — agent never writes to leads.db directly.
"""
import sqlite3
from pathlib import Path

LEADS_DB = Path(__file__).parent.parent.parent / "leadgen" / "data" / "leads.db"


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(LEADS_DB)
    conn.row_factory = sqlite3.Row
    return conn


def stats() -> str:
    """Return a plain-text summary of lead pipeline state."""
    conn = _conn()
    try:
        niche_row = conn.execute(
            """
            SELECT target_niche, COUNT(*) AS n
            FROM businesses
            WHERE validation_status='qualified' AND COALESCE(target_niche, '') <> ''
            GROUP BY target_niche
            ORDER BY n DESC
            LIMIT 3
            """
        ).fetchall()
    except sqlite3.OperationalError:
        niche_row = []
    rows = conn.execute(
        """
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE approved=1) AS approved,
            COUNT(*) FILTER (WHERE validation_status='qualified') AS qualified,
            COUNT(*) FILTER (WHERE validation_status='disqualified') AS disqualified,
            COUNT(*) FILTER (WHERE site_intel_done=1) AS intel_done
        FROM businesses
        """
    ).fetchone()
    enriched = conn.execute("SELECT COUNT(DISTINCT business_id) AS n FROM enrichment").fetchone()["n"]
    top_niches = ", ".join(f"{r['target_niche']}={r['n']}" for r in niche_row) if niche_row else "n/a"
    return (
        f"Total leads: {rows['total']}\n"
        f"Approved: {rows['approved']}\n"
        f"Qualified: {rows['qualified']}\n"
        f"Disqualified: {rows['disqualified']}\n"
        f"Site intel done: {rows['intel_done']}\n"
        f"Enriched: {enriched}\n"
        f"Top niches: {top_niches}"
    )


def top_qualified(n: int = 10) -> str:
    """Return top N qualified leads as plain text."""
    conn = _conn()
    rows = conn.execute(
        """
        SELECT b.name, b.category, b.address, b.score,
               b.outreach_angle, w.emails, b.target_niche
        FROM businesses b
        LEFT JOIN website_data w ON w.business_id = b.id
        WHERE b.validation_status = 'qualified'
        ORDER BY b.score DESC NULLS LAST
        LIMIT ?
        """,
        (n,),
    ).fetchall()
    if not rows:
        return "No qualified leads found."
    lines = []
    for r in rows:
        lines.append(
            f"- {r['name']} ({r['category']}) | niche={r['target_niche'] or 'unknown'} | score={r['score']} | {r['address']}\n"
            f"  angle: {r['outreach_angle']}\n"
            f"  email: {r['emails']}"
        )
    return "\n".join(lines)


def search_leads(query: str) -> str:
    """Full-text search across lead names, categories, addresses."""
    conn = _conn()
    like = f"%{query}%"
    rows = conn.execute(
        """
        SELECT name, category, address, validation_status, score, target_niche
        FROM businesses
        WHERE name LIKE ? OR category LIKE ? OR address LIKE ?
        LIMIT 20
        """,
        (like, like, like),
    ).fetchall()
    if not rows:
        return f"No leads matching '{query}'."
    return "\n".join(
        f"- {r['name']} | {r['category']} | niche={r['target_niche'] or 'unknown'} | {r['address']} | {r['validation_status']} | score={r['score']}"
        for r in rows
    )
