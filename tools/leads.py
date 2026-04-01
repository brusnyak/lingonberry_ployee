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
    """Full-text search across lead names, categories, addresses, and emails."""
    conn = _conn()
    like = f"%{query}%"
    rows = conn.execute(
        """
        SELECT b.id, b.name, b.category, b.address, b.validation_status,
               b.score, b.target_niche, b.email_maps, b.outreach_angle,
               w.emails AS site_emails
        FROM businesses b
        LEFT JOIN website_data w ON w.id = (
            SELECT MAX(w2.id) FROM website_data w2 WHERE w2.business_id = b.id
        )
        WHERE b.name LIKE ? OR b.category LIKE ? OR b.address LIKE ?
           OR b.email_maps LIKE ? OR w.emails LIKE ?
        LIMIT 20
        """,
        (like, like, like, like, like),
    ).fetchall()
    if not rows:
        return f"No leads matching '{query}'."
    lines = []
    for r in rows:
        email = r["email_maps"] or r["site_emails"] or "no email"
        lines.append(
            f"- [{r['id']}] {r['name']} | {r['category']} | niche={r['target_niche'] or 'unknown'}"
            f" | {r['validation_status']} | score={r['score']} | email={email}"
        )
        if r["outreach_angle"]:
            lines.append(f"  angle: {r['outreach_angle']}")
    return "\n".join(lines)


def get_lead(lead_id: int) -> str:
    """Get full profile for a single lead by ID, including outreach history."""
    conn = _conn()
    row = conn.execute(
        """
        SELECT b.id, b.name, b.category, b.address, b.website, b.phone,
               b.email_maps, b.validation_status, b.score, b.target_niche,
               b.outreach_angle, b.top_gap, b.brand_summary, b.pain_point_guess,
               w.emails AS site_emails, w.socials, w.language
        FROM businesses b
        LEFT JOIN website_data w ON w.id = (
            SELECT MAX(w2.id) FROM website_data w2 WHERE w2.business_id = b.id
        )
        WHERE b.id = ?
        """,
        (lead_id,),
    ).fetchone()
    if not row:
        return f"No lead found with id={lead_id}."

    email = row["email_maps"] or row["site_emails"] or "none"
    lines = [
        f"Lead [{row['id']}]: {row['name']}",
        f"  category: {row['category']}",
        f"  niche: {row['target_niche'] or 'unknown'}",
        f"  status: {row['validation_status']} | score: {row['score']}",
        f"  email: {email}",
        f"  phone: {row['phone'] or 'none'}",
        f"  website: {row['website'] or 'none'}",
        f"  address: {row['address'] or 'none'}",
        f"  outreach_angle: {row['outreach_angle'] or 'none'}",
        f"  top_gap: {row['top_gap'] or 'none'}",
        f"  brand_summary: {(row['brand_summary'] or '')[:200]}",
        f"  pain_point_guess: {row['pain_point_guess'] or 'none'}",
    ]

    # Outreach history
    try:
        import sqlite3 as _sq
        OUTREACH_DB = Path(__file__).parent.parent.parent / "leadgen" / "data" / "leads.db"
        oc = _sq.connect(OUTREACH_DB)
        oc.row_factory = _sq.Row
        history = oc.execute(
            "SELECT id, channel, address, subject, status, created_at FROM outreach_log WHERE lead_id=? ORDER BY created_at DESC LIMIT 10",
            (lead_id,),
        ).fetchall()
        if history:
            lines.append("  outreach history:")
            for h in history:
                lines.append(f"    - [{h['id']}] {h['channel']} -> {h['address']} | {h['status']} | {h['subject']} | {h['created_at'][:16]}")
        else:
            lines.append("  outreach history: none")
    except Exception as e:
        lines.append(f"  outreach history: error ({e})")

    return "\n".join(lines)


def update_lead_email(lead_id: int, email: str) -> str:
    """Update the email address for a lead."""
    conn = _conn()
    conn.execute("UPDATE businesses SET email_maps=? WHERE id=?", (email, lead_id))
    conn.commit()
    return f"Updated lead [{lead_id}] email to {email}."


def update_lead_niche(lead_id: int, niche: str) -> str:
    """Update the target_niche for a lead."""
    conn = _conn()
    conn.execute("UPDATE businesses SET target_niche=? WHERE id=?", (niche, lead_id))
    conn.commit()
    return f"Updated lead [{lead_id}] niche to {niche}."


def update_lead_field(lead_id: int, field: str, value: str) -> str:
    """Update any single field on a lead. Allowed fields are whitelisted."""
    ALLOWED = {
        "email_maps", "phone", "website", "address", "contact_name",
        "outreach_angle", "pain_point_guess", "top_gap", "target_niche",
        "validation_status", "pipeline_stage", "brand_summary", "apparent_size",
        "digital_maturity", "score", "score_reason",
    }
    if field not in ALLOWED:
        return f"Field '{field}' not allowed. Allowed: {', '.join(sorted(ALLOWED))}"
    conn = _conn()
    conn.execute(f"UPDATE businesses SET {field}=? WHERE id=?", (value, lead_id))
    conn.commit()
    return f"Updated lead [{lead_id}] {field} = {value!r}"


def delete_lead(lead_id: int) -> str:
    """
    Permanently delete a lead and all associated data (website_data, enrichment,
    outreach_log, replies). Use with care — irreversible.
    """
    conn = _conn()
    # cascade manually (no FK enforcement in SQLite by default)
    reply_ids = [r[0] for r in conn.execute("SELECT id FROM replies WHERE lead_id=?", (lead_id,)).fetchall()]
    if reply_ids:
        ph = ",".join("?" * len(reply_ids))
        conn.execute(f"DELETE FROM reply_drafts WHERE reply_id IN ({ph})", reply_ids)
        conn.execute(f"DELETE FROM reply_classification WHERE reply_id IN ({ph})", reply_ids)
    conn.execute("DELETE FROM replies WHERE lead_id=?", (lead_id,))
    conn.execute("DELETE FROM outreach_log WHERE lead_id=?", (lead_id,))
    conn.execute("DELETE FROM enrichment WHERE business_id=?", (lead_id,))
    conn.execute("DELETE FROM website_data WHERE business_id=?", (lead_id,))
    conn.execute("DELETE FROM businesses WHERE id=?", (lead_id,))
    conn.commit()
    return f"Deleted lead [{lead_id}] and all associated data."


def add_lead(
    name: str,
    email: str,
    category: str = "",
    address: str = "",
    website: str = "",
    phone: str = "",
    outreach_angle: str = "",
    target_niche: str = "",
    contact_name: str = "",
) -> str:
    """
    Manually add a lead to the DB. Useful for leads found outside the scraper pipeline.
    Returns the new lead ID.
    """
    from datetime import datetime
    conn = _conn()
    place_id = f"manual_{name.lower().replace(' ', '_')[:30]}_{int(datetime.utcnow().timestamp())}"
    # Extract first name only for contact_name
    first_name = contact_name.strip().split()[0] if contact_name.strip() else ""
    if not first_name:
        # Try to extract from the lead name
        name_parts = name.strip().split()
        if name_parts and name_parts[0].lower() not in {"the", "and", "for", "ltd", "pty", "inc"}:
            first_name = name_parts[0]
    cur = conn.execute(
        """
        INSERT INTO businesses (
            place_id, name, category, address, phone, website, email_maps,
            query, collected_at, approved, score, validation_status,
            target_niche, contact_name, outreach_angle, pipeline_stage
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            place_id, name, category, address, phone, website, email,
            "manual", datetime.utcnow().isoformat(), 1, 50.0, "qualified",
            target_niche, first_name, outreach_angle, "lead",
        ),
    )
    conn.commit()
    return f"Added lead [{cur.lastrowid}]: {name} <{email}>"


def set_lead_status(lead_id: int, status: str) -> str:
    """Set validation_status for a lead. Values: qualified, skip, needs_review, disqualified."""
    allowed = {"qualified", "skip", "needs_review", "disqualified", "pending"}
    if status not in allowed:
        return f"Invalid status '{status}'. Use: {', '.join(sorted(allowed))}"
    conn = _conn()
    conn.execute("UPDATE businesses SET validation_status=? WHERE id=?", (status, lead_id))
    conn.commit()
    return f"Lead [{lead_id}] status set to '{status}'."


def bulk_set_status(niche: str, from_status: str, to_status: str) -> str:
    """Bulk update validation_status for all leads in a niche matching from_status."""
    conn = _conn()
    cur = conn.execute(
        "UPDATE businesses SET validation_status=? WHERE target_niche=? AND validation_status=?",
        (to_status, niche, from_status),
    )
    conn.commit()
    return f"Updated {cur.rowcount} leads in niche '{niche}' from '{from_status}' to '{to_status}'."


def clear_outreach_for_lead(lead_id: int) -> str:
    """Delete all outreach history for a lead (drafts, replies, log). Resets pipeline_stage to 'lead'."""
    conn = _conn()
    reply_ids = [r[0] for r in conn.execute("SELECT id FROM replies WHERE lead_id=?", (lead_id,)).fetchall()]
    if reply_ids:
        ph = ",".join("?" * len(reply_ids))
        conn.execute(f"DELETE FROM reply_drafts WHERE reply_id IN ({ph})", reply_ids)
        conn.execute(f"DELETE FROM reply_classification WHERE reply_id IN ({ph})", reply_ids)
    conn.execute("DELETE FROM replies WHERE lead_id=?", (lead_id,))
    conn.execute("DELETE FROM outreach_log WHERE lead_id=?", (lead_id,))
    conn.execute("UPDATE businesses SET pipeline_stage='lead' WHERE id=?", (lead_id,))
    conn.commit()
    return f"Cleared all outreach history for lead [{lead_id}]. Stage reset to 'lead'."


def run_contact_enrichment(limit: int = 100) -> str:
    """Trigger contact+pain enrichment on qualified/needs_review leads missing outreach_angle."""
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent.parent / "leadgen"))
    try:
        from enrichment.contact_enrichment import run_contact_enrichment as _enrich
        from storage.db import connect as _connect, init_db
        conn = _connect(str(LEADS_DB))
        init_db(conn)
        counts = _enrich(conn, limit=int(limit), only_missing=True)
        return f"Contact enrichment done — enriched={counts['enriched']} skipped={counts['skipped']} total={counts['total']}"
    except Exception as e:
        return f"Contact enrichment failed: {e}"


def scrape_leads(source: str = "web_search", query: str = "", trade: str = "plumber", location: str = "sydney", max_results: int = 15) -> str:
    """
    Trigger a lead scrape run. source: web_search | hipages | google_maps.
    Results go into the main leads.db.
    """
    import subprocess, sys
    leadgen_dir = Path(__file__).parent.parent.parent / "leadgen"
    python = sys.executable
    cmd = [
        python, "main.py",
        "--source", source,
        "--db", "data/leads.db",
        "--auto-approve",
        "--skip-llm",
        "--workers", "3",
        "--max", str(max_results),
    ]
    if query:
        cmd += ["--query", query]
    if trade:
        cmd += ["--trade", trade]
    if location:
        cmd += ["--location", location]
    try:
        result = subprocess.run(cmd, cwd=str(leadgen_dir), capture_output=True, text=True, timeout=180)
        out = (result.stdout or "").strip()[-600:]
        err = (result.stderr or "").strip()[-300:]
        if result.returncode != 0:
            return f"Scrape failed (exit {result.returncode}):\n{err or out}"
        return f"Scrape complete ({source}):\n{out}"
    except subprocess.TimeoutExpired:
        return "Scrape timed out after 3 minutes — may still be running in background."
    except Exception as e:
        return f"Scrape error: {e}"
