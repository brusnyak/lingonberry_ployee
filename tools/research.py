"""
agent/tools/research.py
Niche research, shortlist, and manual lead-review helpers.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
LEADS_DB = ROOT / "leadgen" / "data" / "leads.db"
sys.path.insert(0, str(ROOT / "leadgen"))

from storage.db import init_db  # type: ignore
from niches import NICHES, ensure_niche_research_seed, refresh_business_niches, refresh_niche_scores, refresh_niche_validation  # type: ignore


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(LEADS_DB)
    conn.row_factory = sqlite3.Row
    return conn


def _refresh(conn: sqlite3.Connection) -> None:
    init_db(conn)
    ensure_niche_research_seed(conn)
    refresh_business_niches(conn)
    refresh_niche_scores(conn)
    refresh_niche_validation(conn)
    conn.commit()


def niche_overview() -> str:
    conn = _conn()
    _refresh(conn)
    rows = conn.execute(
        """
        SELECT
            nr.niche,
            nr.status,
            nr.score,
            nr.sample_market,
            nr.common_pains,
            nr.outreach_channel_fit,
            nv.leads_count,
            nv.qualified_count,
            nv.contacted_count,
            nv.replies_count,
            nv.interested_count
        FROM niche_research nr
        LEFT JOIN niche_validation nv ON nv.niche = nr.niche
        ORDER BY nr.score DESC, nr.niche ASC
        """
    ).fetchall()
    if not rows:
        return "No niche research rows found."
    lines = []
    for row in rows:
        label = NICHES.get(row["niche"], {}).get("label", row["niche"])
        pains = ", ".join(json.loads(row["common_pains"] or "[]")[:2]) or "no pains recorded"
        channels = ", ".join(json.loads(row["outreach_channel_fit"] or "[]")) or "unknown"
        lines.append(
            f"- {label} [{row['status']}] score={row['score']:.2f}\n"
            f"  leads={row['leads_count'] or 0} qualified={row['qualified_count'] or 0} contacted={row['contacted_count'] or 0} replies={row['replies_count'] or 0} interested={row['interested_count'] or 0}\n"
            f"  pains: {pains}\n"
            f"  channels: {channels}"
        )
    return "\n".join(lines)


def shortlist_status() -> str:
    conn = _conn()
    _refresh(conn)
    rows = conn.execute(
        """
        SELECT niche, score, status
        FROM niche_research
        WHERE status='shortlisted'
        ORDER BY score DESC, niche ASC
        """
    ).fetchall()
    if not rows:
        return "No shortlisted niches yet."
    return "\n".join(
        f"- {NICHES.get(row['niche'], {}).get('label', row['niche'])} | score={row['score']:.2f}"
        for row in rows
    )


def findings_summary(niche: str = "", limit: int = 10) -> str:
    conn = _conn()
    _refresh(conn)
    params: list[object] = []
    query = """
        SELECT niche, source_type, source_title, source_url, pain_point, opportunity_type, summary,
               suggested_offer, suggested_channel, monetization_path, evidence_strength, confidence, created_at
        FROM niche_findings
    """
    if niche:
        query += " WHERE niche=?"
        params.append(niche)
    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
    if not rows:
        if niche:
            return f"No research findings saved for niche '{niche}'."
        return "No research findings saved yet."
    lines = []
    for row in rows:
        label = NICHES.get(row["niche"], {}).get("label", row["niche"])
        title = row["source_title"] or row["source_type"]
        confidence = f" | confidence={row['confidence']:.2f}" if row["confidence"] is not None else ""
        strength = f" | evidence={row['evidence_strength']:.2f}" if row["evidence_strength"] is not None else ""
        offer = f"\n  offer: {row['suggested_offer']}" if row["suggested_offer"] else ""
        channel = f"\n  channel: {row['suggested_channel']}" if row["suggested_channel"] else ""
        money = f"\n  monetization: {row['monetization_path']}" if row["monetization_path"] else ""
        url = f"\n  url: {row['source_url']}" if row["source_url"] else ""
        lines.append(
            f"- {label} | {title}{confidence}{strength}\n"
            f"  pain: {row['pain_point'] or 'unspecified'} | opportunity: {row['opportunity_type'] or 'unspecified'}\n"
            f"  summary: {row['summary']}{offer}{channel}{money}{url}"
        )
    return "\n".join(lines)


def ingest_finding(
    niche: str,
    source_type: str,
    summary: str,
    source_query: str = "",
    source_title: str = "",
    source_url: str = "",
    market: str = "",
    pain_point: str = "",
    suggested_offer: str = "",
    suggested_channel: str = "",
    opportunity_type: str = "",
    monetization_path: str = "",
    evidence_strength: float | None = None,
    confidence: float | None = None,
    tags: list[str] | None = None,
    created_by: str = "agent",
) -> str:
    conn = _conn()
    _refresh(conn)
    if niche not in NICHES:
        return f"Unknown niche '{niche}'. Valid keys: {', '.join(sorted(NICHES))}"
    created_at = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO niche_findings (
            niche, source_type, source_query, source_title, source_url, market,
            pain_point, opportunity_type, summary, suggested_offer, suggested_channel,
            monetization_path, evidence_strength, confidence, tags, created_by, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            niche,
            source_type,
            source_query,
            source_title,
            source_url,
            market,
            pain_point,
            opportunity_type,
            summary,
            suggested_offer,
            suggested_channel,
            monetization_path,
            evidence_strength,
            confidence,
            json.dumps(tags or []),
            created_by,
            created_at,
        ),
    )
    conn.commit()
    return f"Ingested finding for {NICHES.get(niche, {}).get('label', niche)} from {source_type}."


def findings_rollup() -> str:
    conn = _conn()
    _refresh(conn)
    rows = conn.execute(
        """
        SELECT niche,
               COUNT(*) AS findings,
               COUNT(*) FILTER (WHERE suggested_offer IS NOT NULL AND suggested_offer != '') AS offers,
               COUNT(*) FILTER (WHERE suggested_channel IS NOT NULL AND suggested_channel != '') AS channels,
               COUNT(*) FILTER (WHERE monetization_path IS NOT NULL AND monetization_path != '') AS monetization_paths
        FROM niche_findings
        GROUP BY niche
        ORDER BY findings DESC, niche ASC
        """
    ).fetchall()
    if not rows:
        return "No niche findings logged yet."
    return "\n".join(
        f"- {NICHES.get(row['niche'], {}).get('label', row['niche'])} | findings={row['findings']} | offer_hints={row['offers']} | channel_hints={row['channels']} | money_paths={row['monetization_paths']}"
        for row in rows
    )


def upsert_pain(
    niche: str,
    pain_key: str,
    pain_label: str,
    description: str = "",
    evidence_summary: str = "",
    evidence_types: list[str] | None = None,
    safe_outreach_claim: str = "",
    unsafe_outreach_claim: str = "",
    offer_angles: list[str] | None = None,
    best_channels: list[str] | None = None,
    confidence: float | None = None,
) -> str:
    conn = _conn()
    _refresh(conn)
    if niche not in NICHES:
        return f"Unknown niche '{niche}'. Valid keys: {', '.join(sorted(NICHES))}"
    now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO pain_library (
            niche, pain_key, pain_label, description, evidence_summary, evidence_types,
            safe_outreach_claim, unsafe_outreach_claim, offer_angles, best_channels,
            confidence, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(niche, pain_key) DO UPDATE SET
            pain_label=excluded.pain_label,
            description=excluded.description,
            evidence_summary=excluded.evidence_summary,
            evidence_types=excluded.evidence_types,
            safe_outreach_claim=excluded.safe_outreach_claim,
            unsafe_outreach_claim=excluded.unsafe_outreach_claim,
            offer_angles=excluded.offer_angles,
            best_channels=excluded.best_channels,
            confidence=excluded.confidence,
            updated_at=excluded.updated_at
        """,
        (
            niche,
            pain_key,
            pain_label,
            description,
            evidence_summary,
            json.dumps(evidence_types or []),
            safe_outreach_claim,
            unsafe_outreach_claim,
            json.dumps(offer_angles or []),
            json.dumps(best_channels or []),
            confidence,
            now,
            now,
        ),
    )
    conn.commit()
    return f"Saved pain '{pain_label}' for {NICHES.get(niche, {}).get('label', niche)}."


def pain_library(niche: str = "", limit: int = 20) -> str:
    conn = _conn()
    _refresh(conn)
    params: list[object] = []
    query = """
        SELECT niche, pain_key, pain_label, description, evidence_summary,
               evidence_types, safe_outreach_claim, unsafe_outreach_claim,
               offer_angles, best_channels, confidence
        FROM pain_library
    """
    if niche:
        query += " WHERE niche=?"
        params.append(niche)
    query += " ORDER BY confidence DESC NULLS LAST, niche ASC, pain_key ASC LIMIT ?"
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
    if not rows:
        return "No pain-library entries saved yet." if not niche else f"No pain-library entries saved for niche '{niche}'."
    lines = []
    for row in rows:
        label = NICHES.get(row["niche"], {}).get("label", row["niche"])
        evidence_types = ", ".join(json.loads(row["evidence_types"] or "[]")) or "none"
        offers = ", ".join(json.loads(row["offer_angles"] or "[]")[:3]) or "none"
        channels = ", ".join(json.loads(row["best_channels"] or "[]")) or "unknown"
        confidence = f"{row['confidence']:.2f}" if row["confidence"] is not None else "n/a"
        unsafe = f"\n  avoid: {row['unsafe_outreach_claim']}" if row["unsafe_outreach_claim"] else ""
        lines.append(
            f"- {label} | {row['pain_label']} | confidence={confidence}\n"
            f"  description: {row['description'] or 'n/a'}\n"
            f"  evidence: {row['evidence_summary'] or 'n/a'}\n"
            f"  evidence_types: {evidence_types}\n"
            f"  safe_claim: {row['safe_outreach_claim'] or 'n/a'}{unsafe}\n"
            f"  offers: {offers}\n"
            f"  channels: {channels}"
        )
    return "\n".join(lines)


def niche_report(niche: str) -> str:
    conn = _conn()
    _refresh(conn)
    row = conn.execute(
        """
        SELECT nr.niche, nr.score, nr.status, nr.common_pains, nr.outreach_channel_fit,
               nv.leads_count, nv.qualified_count, nv.contacted_count, nv.replies_count
        FROM niche_research nr
        LEFT JOIN niche_validation nv ON nv.niche = nr.niche
        WHERE nr.niche=?
        """,
        (niche,),
    ).fetchone()
    if not row:
        return f"Unknown niche '{niche}'."
    label = NICHES.get(niche, {}).get("label", niche)
    pains = pain_library(niche, 10)
    findings = findings_summary(niche, 8)
    common = ", ".join(json.loads(row["common_pains"] or "[]")) or "none"
    channels = ", ".join(json.loads(row["outreach_channel_fit"] or "[]")) or "unknown"
    return (
        f"{label}\n"
        f"score={row['score']:.2f} status={row['status']}\n"
        f"leads={row['leads_count'] or 0} qualified={row['qualified_count'] or 0} contacted={row['contacted_count'] or 0} replies={row['replies_count'] or 0}\n"
        f"common_pains: {common}\n"
        f"channels: {channels}\n\n"
        f"Pain library\n{pains}\n\n"
        f"Findings\n{findings}"
    )


def strategy_report() -> str:
    conn = _conn()
    _refresh(conn)
    rows = conn.execute(
        """
        SELECT
            niche,
            COUNT(*) AS findings,
            GROUP_CONCAT(DISTINCT pain_point) AS pains,
            GROUP_CONCAT(DISTINCT suggested_offer) AS offers,
            GROUP_CONCAT(DISTINCT suggested_channel) AS channels,
            GROUP_CONCAT(DISTINCT monetization_path) AS money
        FROM niche_findings
        GROUP BY niche
        ORDER BY findings DESC, niche ASC
        """
    ).fetchall()
    if not rows:
        return "No strategy findings available yet."
    lines = []
    for row in rows:
        label = NICHES.get(row["niche"], {}).get("label", row["niche"])
        lines.append(
            f"- {label}\n"
            f"  pains: {(row['pains'] or 'none')[:180]}\n"
            f"  offers: {(row['offers'] or 'none')[:180]}\n"
            f"  channels: {(row['channels'] or 'none')[:180]}\n"
            f"  monetization: {(row['money'] or 'none')[:180]}"
        )
    return "\n".join(lines)


def set_shortlist(niches: list[str]) -> str:
    conn = _conn()
    _refresh(conn)
    valid = []
    invalid = []
    requested = {n.strip() for n in niches if n and n.strip()}
    for niche in requested:
        if niche in NICHES:
            valid.append(niche)
        else:
            invalid.append(niche)

    if not valid:
        return f"No valid niches supplied. Valid keys: {', '.join(sorted(NICHES))}"

    conn.execute("UPDATE niche_research SET status='candidate' WHERE status='shortlisted'")
    conn.executemany(
        "UPDATE niche_research SET status='shortlisted' WHERE niche=?",
        [(niche,) for niche in valid],
    )
    conn.commit()

    msg = "Shortlisted niches:\n" + "\n".join(
        f"- {NICHES.get(niche, {}).get('label', niche)}" for niche in valid
    )
    if invalid:
        msg += "\nIgnored unknown keys: " + ", ".join(sorted(invalid))
    return msg


def update_niche(
    niche: str,
    notes: str = "",
    external_evidence: str = "",
    sample_market: str = "",
    common_pains: list[str] | None = None,
    outreach_channel_fit: list[str] | None = None,
    pain_detectability: float | None = None,
    contactability: float | None = None,
    ability_to_deliver: float | None = None,
    price_tolerance: float | None = None,
    content_leverage: float | None = None,
    status: str = "",
) -> str:
    conn = _conn()
    _refresh(conn)
    row = conn.execute("SELECT niche FROM niche_research WHERE niche=?", (niche,)).fetchone()
    if not row:
        return f"Unknown niche '{niche}'. Valid keys: {', '.join(sorted(NICHES))}"

    updates = {}
    if notes:
        updates["notes"] = notes
    if external_evidence:
        updates["external_evidence"] = external_evidence
    if sample_market:
        updates["sample_market"] = sample_market
    if common_pains is not None:
        updates["common_pains"] = json.dumps(common_pains)
    if outreach_channel_fit is not None:
        updates["outreach_channel_fit"] = json.dumps(outreach_channel_fit)
    if pain_detectability is not None:
        updates["pain_detectability"] = pain_detectability
    if contactability is not None:
        updates["contactability"] = contactability
    if ability_to_deliver is not None:
        updates["ability_to_deliver"] = ability_to_deliver
    if price_tolerance is not None:
        updates["price_tolerance"] = price_tolerance
    if content_leverage is not None:
        updates["content_leverage"] = content_leverage
    if status:
        updates["status"] = status

    if not updates:
        return f"No changes supplied for niche '{niche}'."

    updates["updated_at"] = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
    keys = ", ".join(f"{key}=?" for key in updates)
    conn.execute(
        f"UPDATE niche_research SET {keys} WHERE niche=?",
        [*updates.values(), niche],
    )
    refresh_niche_scores(conn)
    refresh_niche_validation(conn)
    conn.commit()
    return f"Updated niche research for {NICHES.get(niche, {}).get('label', niche)}."


def lead_review_queue(n: int = 10) -> str:
    conn = _conn()
    rows = conn.execute(
        """
        SELECT
            b.id AS lead_id,
            b.name,
            b.target_niche,
            o.id AS outreach_id,
            o.status,
            o.channel,
            o.address,
            o.subject,
            lr.id AS review_id
        FROM outreach_log o
        JOIN businesses b ON b.id = o.lead_id
        LEFT JOIN lead_reviews lr ON lr.lead_id = b.id
        WHERE o.status IN ('sent', 'skipped', 'failed')
          AND lr.id IS NULL
        ORDER BY o.created_at DESC
        LIMIT ?
        """,
        (n,),
    ).fetchall()
    if not rows:
        return "No lead reviews pending."
    return "\n".join(
        f"- lead {row['lead_id']} | {row['name']} | niche={row['target_niche'] or 'unknown'} | outreach={row['status']} via {row['channel']} -> {row['address']}"
        for row in rows
    )


def candidate_queue(n: int = 20) -> str:
    conn = _conn()
    _refresh(conn)
    has_shortlist = conn.execute(
        "SELECT COUNT(*) AS n FROM niche_research WHERE status='shortlisted'"
    ).fetchone()["n"] > 0
    query = """
        SELECT
            b.id,
            b.name,
            b.target_niche,
            b.outreach_angle,
            b.top_gap,
            b.top_opportunity,
            COALESCE(w.emails, '') AS site_emails,
            COALESCE(b.email_maps, '') AS email_maps,
            COALESCE(w.instagram_url, '') AS instagram_url,
            COALESCE(w.facebook_url, '') AS facebook_url,
            COALESCE(w.language, '') AS language,
            COALESCE(lr.recommended_channel, '') AS reviewed_channel
        FROM businesses b
        LEFT JOIN website_data w ON w.id = (
            SELECT MAX(w2.id) FROM website_data w2 WHERE w2.business_id = b.id
        )
        LEFT JOIN lead_reviews lr ON lr.lead_id = b.id
        WHERE b.validation_status='qualified'
          AND b.id NOT IN (
              SELECT DISTINCT lead_id FROM outreach_log WHERE status IN ('sent', 'pending', 'approved')
          )
          AND COALESCE(b.target_niche, '') <> ''
    """
    if has_shortlist:
        query += " AND b.target_niche IN (SELECT niche FROM niche_research WHERE status='shortlisted')"
    query += " ORDER BY b.score DESC NULLS LAST, b.id ASC LIMIT ?"
    rows = conn.execute(query, (n,)).fetchall()
    if not rows:
        if has_shortlist:
            return "No outreach-ready candidates found for the current shortlist."
        return "No outreach-ready candidates found."

    lines = []
    for row in rows:
        email_ready = bool((row["site_emails"] or "").strip() or (row["email_maps"] or "").strip())
        channels = []
        if email_ready:
            channels.append("email")
        if row["instagram_url"]:
            channels.append("instagram")
        if row["facebook_url"]:
            channels.append("facebook")
        channels_str = ", ".join(channels) if channels else "none"
        angle = row["top_opportunity"] or row["top_gap"] or row["outreach_angle"] or "no angle"
        review = f" | reviewed={row['reviewed_channel']}" if row["reviewed_channel"] else ""
        lines.append(
            f"- lead {row['id']} | {row['name']} | niche={row['target_niche']} | channels={channels_str} | lang={row['language'] or 'unknown'}{review}\n"
            f"  angle: {angle}"
        )
    return "\n".join(lines)


def save_lead_review(
    lead_id: int,
    recommended_channel: str,
    recommended_angle: str,
    notes: str = "",
    actual_business_model: str = "",
    actual_pains: str = "",
    email_fit: str = "",
    social_fit: str = "",
    form_fit: str = "",
    hybrid_fit: str = "",
    reviewer: str = "telegram",
) -> str:
    conn = _conn()
    row = conn.execute(
        "SELECT target_niche, name FROM businesses WHERE id=?",
        (lead_id,),
    ).fetchone()
    if not row:
        return f"Lead {lead_id} not found."
    reviewed_at = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO lead_reviews (
            lead_id, target_niche, actual_business_model, actual_pains, email_fit,
            social_fit, form_fit, hybrid_fit, recommended_channel, recommended_angle,
            notes, reviewer, reviewed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(lead_id) DO UPDATE SET
            target_niche=excluded.target_niche,
            actual_business_model=excluded.actual_business_model,
            actual_pains=excluded.actual_pains,
            email_fit=excluded.email_fit,
            social_fit=excluded.social_fit,
            form_fit=excluded.form_fit,
            hybrid_fit=excluded.hybrid_fit,
            recommended_channel=excluded.recommended_channel,
            recommended_angle=excluded.recommended_angle,
            notes=excluded.notes,
            reviewer=excluded.reviewer,
            reviewed_at=excluded.reviewed_at
        """,
        (
            lead_id,
            row["target_niche"],
            actual_business_model,
            actual_pains,
            email_fit,
            social_fit,
            form_fit,
            hybrid_fit,
            recommended_channel,
            recommended_angle,
            notes,
            reviewer,
            reviewed_at,
        ),
    )
    conn.commit()
    return f"Saved review for lead {lead_id} ({row['name']}) with recommended channel '{recommended_channel}'."
