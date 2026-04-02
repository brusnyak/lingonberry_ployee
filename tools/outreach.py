"""
agent/tools/outreach.py
Query outreach state. Read-only queries + trigger actions via outreach module.
"""
import random
import sqlite3
import subprocess
import sys
import textwrap
import uuid
import os
from pathlib import Path

from outreach.runtime import assert_outbound_allowed
from outreach.senders import canonical_sender, internal_sender_addresses

LEADS_DB = Path(__file__).parent.parent.parent / "leadgen" / "data" / "leads.db"
OUTREACH_DIR = Path(__file__).parent.parent.parent / "outreach"
OUTREACH_PYTHON = OUTREACH_DIR / ".venv" / "bin" / "python"
TEST_RECIPIENTS = {"egorbrusnyak@gmail.com", *{addr.lower() for addr in internal_sender_addresses()}}


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
            COUNT(*) FILTER (WHERE status='approved') AS approved,
            COUNT(*) FILTER (WHERE status='scheduled') AS scheduled,
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
        f"Sent: {row['sent']} | Approved: {row['approved']} | Scheduled: {row['scheduled']} | Pending: {row['pending']} | "
        f"Failed: {row['failed']} | Skipped: {row['skipped']}\n"
        f"Replies: {replies} | Interested: {interested}"
    )


def safe_mode_status() -> str:
    from outreach.runtime import safe_mode_enabled

    state = "ON" if safe_mode_enabled() else "OFF"
    detail = "outbound sends blocked" if safe_mode_enabled() else "outbound sends allowed"
    return f"Safe mode: {state} ({detail})"


def production_test_summary() -> str:
    conn = _conn()
    quoted = ",".join("'" + addr.replace("'", "''") + "'" for addr in sorted(TEST_RECIPIENTS))
    reply_rows = conn.execute(
        f"""
        SELECT
            COUNT(*) FILTER (
                WHERE LOWER(COALESCE(subject, '')) LIKE '%internal reply workflow test%'
                   OR LOWER(COALESCE(subject, '')) LIKE '%smtp test%'
                   OR LOWER(COALESCE(from_address, '')) IN ({quoted})
            ) AS test_replies,
            COUNT(*) FILTER (
                WHERE NOT (
                    LOWER(COALESCE(subject, '')) LIKE '%internal reply workflow test%'
                    OR LOWER(COALESCE(subject, '')) LIKE '%smtp test%'
                    OR LOWER(COALESCE(from_address, '')) IN ({quoted})
                )
            ) AS prod_replies
        FROM replies
        """
    ).fetchone()
    outreach_rows = conn.execute(
        f"""
        SELECT
            COUNT(*) FILTER (
                WHERE COALESCE(message_variant_fingerprint, '') = 'internal-reply-workflow-test'
                   OR COALESCE(message_variant_fingerprint, '') = 'internal-matrix-test'
                   OR LOWER(COALESCE(subject, '')) LIKE '%smtp test%'
                   OR LOWER(COALESCE(address, '')) IN ({quoted})
            ) AS test_outreach,
            COUNT(*) FILTER (
                WHERE NOT (
                    COALESCE(message_variant_fingerprint, '') = 'internal-reply-workflow-test'
                    OR COALESCE(message_variant_fingerprint, '') = 'internal-matrix-test'
                    OR LOWER(COALESCE(subject, '')) LIKE '%smtp test%'
                    OR LOWER(COALESCE(address, '')) IN ({quoted})
                )
            ) AS prod_outreach
        FROM outreach_log
        """
    ).fetchone()
    return (
        f"Production/test split\n"
        f"- prod outreach rows: {outreach_rows['prod_outreach']}\n"
        f"- test outreach rows: {outreach_rows['test_outreach']}\n"
        f"- prod replies: {reply_rows['prod_replies']}\n"
        f"- test replies: {reply_rows['test_replies']}"
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


def reply_queue(n: int = 10) -> str:
    script = f"""
from storage.db import connect, init_outreach_tables, get_reply_queue_needing_action
from email_sender import _load_accounts

internal = {{acc["address"].lower() for acc in _load_accounts()}}
def actionable(row):
    subject = (row["reply_subject"] or "").lower()
    addr = (row["from_address"] or "").lower()
    if addr in internal:
        return False
    if "smtp test" in subject or "internal reply workflow test" in subject:
        return False
    return True

conn = connect()
init_outreach_tables(conn)
rows = [row for row in get_reply_queue_needing_action(conn, limit={int(n) * 3}) if actionable(row)][:{int(n)}]
if not rows:
    print("No replies need action.")
    raise SystemExit(0)

for row in rows:
    snippet = " ".join((row["content"] or "").split())
    if len(snippet) > 180:
        snippet = snippet[:177].rstrip() + "..."
    draft_status = row["draft_status"] or "missing"
    print(
        f"- [reply {{row['reply_id']}}] {{row['name']}} | {{row['label'] or 'unclassified'}} | "
        f"received={{row['received_at'][:19]}} | draft={{draft_status}} | from={{row['from_address'] or 'unknown'}}"
    )
    print(f"  {{snippet}}")
"""
    return _run_outreach_python(script)


def prepare_reply_drafts(limit: int = 10) -> str:
    script = f"""
from storage.db import connect, init_outreach_tables, get_reply_queue_needing_action, upsert_reply_draft
from email_sender import _load_accounts
from reply_drafter import build_reply_draft

internal = {{acc["address"].lower() for acc in _load_accounts()}}
def actionable(row):
    subject = (row["reply_subject"] or "").lower()
    addr = (row["from_address"] or "").lower()
    if addr in internal:
        return False
    if "smtp test" in subject or "internal reply workflow test" in subject:
        return False
    return True

conn = connect()
init_outreach_tables(conn)
rows = [row for row in get_reply_queue_needing_action(conn, limit={int(limit) * 3}) if actionable(row)][:{int(limit)}]
if not rows:
    print("No replies need drafting.")
    raise SystemExit(0)

prepared = []
for row in rows:
    draft = build_reply_draft(dict(row))
    upsert_reply_draft(
        conn,
        row["reply_id"],
        draft.subject,
        draft.body,
        draft.sender_name,
        draft.sender_address,
        draft.rationale,
    )
    prepared.append(f"- [reply {{row['reply_id']}}] {{row['name']}} | {{row['label'] or 'unclassified'}} | from={{draft.sender_address or 'unassigned'}}")

print(f"Prepared {{len(prepared)}} reply draft(s).")
for line in prepared:
    print(line)
"""
    return _run_outreach_python(script)


def preview_reply_drafts(limit: int = 5) -> str:
    script = f"""
from storage.db import connect, init_outreach_tables, get_reply_queue_needing_action
from email_sender import _load_accounts

internal = {{acc["address"].lower() for acc in _load_accounts()}}
def actionable(row):
    subject = (row["reply_subject"] or "").lower()
    addr = (row["from_address"] or "").lower()
    if addr in internal:
        return False
    if "smtp test" in subject or "internal reply workflow test" in subject:
        return False
    return True

conn = connect()
init_outreach_tables(conn)
rows = [row for row in get_reply_queue_needing_action(conn, limit={int(limit) * 3}) if actionable(row)]
rows = [row for row in rows if row["draft_body"]]
if not rows:
    print("No drafted replies to preview.")
    raise SystemExit(0)

for row in rows:
    body = (row["draft_body"] or "").replace("\\\\r\\\\n", "\\n").replace("\\\\n", "\\n")
    print(f"[reply {{row['reply_id']}}] {{row['name']}} -> {{row['from_address'] or 'unknown'}}")
    print(f"from: {{row['sender_name'] or 'unassigned'}} <{{row['sender_address'] or 'unassigned'}}>")
    print(f"label: {{row['label'] or 'unclassified'}} | received: {{row['received_at'][:19]}}")
    print(f"subject: {{row['draft_subject'] or '(no subject)'}}")
    if row["rationale"]:
        print(f"rationale: {{row['rationale']}}")
    print("")
    print(body.strip())
    print("")
    print("---")
"""
    return _run_outreach_python(script)


def send_reply_drafts(limit: int = 5) -> str:
    assert_outbound_allowed("send_reply_drafts")
    script = f"""
from storage.db import connect, init_outreach_tables, get_reply_queue_needing_action, mark_reply_draft_sent, mark_reply_draft_failed
from email_sender import _load_accounts, send_email

conn = connect()
init_outreach_tables(conn)
internal = {{acc["address"].lower() for acc in _load_accounts()}}
def actionable(row):
    subject = (row["reply_subject"] or "").lower()
    addr = (row["from_address"] or "").lower()
    if addr in internal:
        return False
    if "smtp test" in subject or "internal reply workflow test" in subject:
        return False
    return True

rows = get_reply_queue_needing_action(conn, limit={int(limit) * 3})
rows = [row for row in rows if actionable(row) and row["draft_body"]][:{int(limit)}]
if not rows:
    print("No drafted replies ready to send.")
    raise SystemExit(0)

accounts = {{acc["address"]: acc for acc in _load_accounts()}}
sent = []
failed = []
for row in rows:
    sender = accounts.get(row["sender_address"] or "")
    if sender is None:
        failed.append(f"[reply {{row['reply_id']}}] {{row['name']}}: sender account {{row['sender_address'] or 'missing'}} unavailable")
        mark_reply_draft_failed(conn, row["reply_id"], "sender account unavailable")
        continue
    to_address = row["from_address"] or ""
    if not to_address:
        failed.append(f"[reply {{row['reply_id']}}] {{row['name']}}: missing reply address")
        mark_reply_draft_failed(conn, row["reply_id"], "missing reply address")
        continue
    try:
        send_email(to_address, row["draft_subject"] or "", row["draft_body"] or "", sender)
        mark_reply_draft_sent(conn, row["reply_id"], sender.get("name", ""), sender.get("address", ""))
        sent.append(f"[reply {{row['reply_id']}}] {{row['name']}} -> {{to_address}} via {{sender['address']}}")
    except Exception as e:
        mark_reply_draft_failed(conn, row["reply_id"], str(e))
        failed.append(f"[reply {{row['reply_id']}}] {{row['name']}}: {{e}}")

print(f"Sent {{len(sent)}} reply draft(s).")
for line in sent:
    print(f"- {{line}}")
if failed:
    print("Failures:")
    for line in failed:
        print(f"- {{line}}")
"""
    return _run_outreach_python(script)


def reply_ops(limit: int = 10) -> str:
    script = f"""
from storage.db import connect, init_outreach_tables, get_reply_queue_needing_action
from email_sender import _load_accounts

internal = {{acc["address"].lower() for acc in _load_accounts()}}
def actionable(row):
    subject = (row["reply_subject"] or "").lower()
    addr = (row["from_address"] or "").lower()
    if addr in internal:
        return False
    if "smtp test" in subject or "internal reply workflow test" in subject:
        return False
    return True

conn = connect()
init_outreach_tables(conn)
rows = [row for row in get_reply_queue_needing_action(conn, limit={int(limit) * 3}) if actionable(row)][:{int(limit)}]
if not rows:
    print("Reply queue: empty")
    raise SystemExit(0)

counts = {{}}
for row in rows:
    key = row["label"] or "unclassified"
    counts[key] = counts.get(key, 0) + 1

print("Reply queue summary:")
for label in ["interested", "question", "not_interested", "ignore", "unclassified"]:
    if counts.get(label):
        print(f"- {{label}}: {{counts[label]}}")
print("")
print("Next up:")
for row in rows[:5]:
    print(f"- [reply {{row['reply_id']}}] {{row['name']}} | {{row['label'] or 'unclassified'}} | draft={{row['draft_status'] or 'missing'}}")
"""
    return _run_outreach_python(script)


def internal_reply_test() -> str:
    script = """
from datetime import datetime, timezone
from storage.db import connect, init_outreach_tables, log_outreach, mark_sent
from email_sender import _load_accounts, send_email

accounts = _load_accounts()
if len(accounts) < 2:
    print("Need at least 2 configured email accounts for internal reply testing.")
    raise SystemExit(1)

sender = accounts[0]
recipient = accounts[1]
subject = f"Internal reply workflow test {datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
body = (
    "Hi,\\n\\n"
    "This is a controlled internal workflow test for reply handling.\\n"
    "Please reply with a short question so the bot can draft the next response.\\n\\n"
    "Best,\\n"
    f"{sender['name']}"
)

conn = connect()
init_outreach_tables(conn)
lead_id = conn.execute("SELECT id FROM businesses ORDER BY id ASC LIMIT 1").fetchone()["id"]
outreach_id = log_outreach(
    conn,
    lead_id,
    "email",
    recipient["address"],
    body,
    subject,
    status="pending",
    message_variant_fingerprint="internal-reply-workflow-test",
)
send_email(recipient["address"], subject, body, sender)
mark_sent(conn, outreach_id, sender["name"], sender["address"])

print("Internal reply test sent.")
print(f"- outreach_id={outreach_id}")
print(f"- from={sender['name']} <{sender['address']}>")
print(f"- to={recipient['name']} <{recipient['address']}>")
print(f"- subject={subject}")
print("")
print("Next step: reply to that email from the recipient mailbox, then run /replyteststatus in Telegram.")
"""
    return _run_outreach_python(script)


def internal_matrix_test(send: bool = True, clear_history: bool = True) -> str:
    from outreach.email_sender import _load_accounts, render_outreach_body, send_email
    from outreach.generator import generate_email
    from outreach.runtime import assert_outbound_allowed
    from outreach.storage.db import init_outreach_tables, log_outreach, mark_sent

    if send:
        assert_outbound_allowed("internal_matrix_test")

    accounts = _load_accounts()
    if len(accounts) < 2:
        return "Need at least 2 configured internal mailboxes."

    conn = _conn()
    init_outreach_tables(conn)
    niches = ["real_estate", "accounting_tax", "dental_medical", "home_services"]
    lead_ids: list[int] = []

    for idx, account in enumerate(accounts):
        niche = niches[idx % len(niches)]
        address = account["address"].strip().lower()
        first_name = canonical_sender(address, account.get("name", "")).get("name", "").split()[0]
        existing = conn.execute(
            """
            SELECT id FROM businesses
            WHERE LOWER(COALESCE(email_maps, '')) = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (address,),
        ).fetchone()
        if existing:
            lead_id = existing["id"]
            conn.execute(
                """
                UPDATE businesses
                SET target_niche=?, validation_status='qualified', approved=1,
                    pipeline_stage='lead', contact_name=?, name=?
                WHERE id=?
                """,
                (niche, first_name, account.get("name", ""), lead_id),
            )
        else:
            cur = conn.execute(
                """
                INSERT INTO businesses (
                    place_id, name, category, address, phone, website, email_maps,
                    query, collected_at, approved, score, validation_status,
                    target_niche, contact_name, outreach_angle, pipeline_stage
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), 1, 50, 'qualified', ?, ?, '', 'lead')
                """,
                (
                    f"internal_{address.replace('@', '_at_').replace('.', '_')}",
                    account.get("name", ""),
                    "internal_test",
                    "internal",
                    "",
                    "",
                    address,
                    "internal matrix test",
                    niche,
                    first_name,
                ),
            )
            lead_id = cur.lastrowid
        lead_ids.append(lead_id)

    if clear_history and lead_ids:
        placeholders = ",".join("?" for _ in lead_ids)
        conn.execute(
            f"DELETE FROM outreach_log WHERE lead_id IN ({placeholders}) AND COALESCE(message_variant_fingerprint, '') = 'internal-matrix-test'",
            lead_ids,
        )
    conn.commit()

    results: list[str] = []
    for idx, sender in enumerate(accounts):
        recipient = accounts[(idx + 1) % len(accounts)]
        lead_id = lead_ids[(idx + 1) % len(lead_ids)]
        lead = conn.execute(
            """
            SELECT
                b.id, b.name, b.category, b.address, b.website, b.phone, b.email_maps,
                COALESCE(b.target_niche, '') AS target_niche,
                COALESCE(b.top_gap, '') AS top_gap,
                COALESCE(b.top_opportunity, '') AS top_opportunity,
                COALESCE(b.gap_profile, '') AS gap_profile,
                COALESCE(b.opportunity_profile, '') AS opportunity_profile,
                COALESCE(b.brand_summary, '') AS brand_summary,
                COALESCE(b.pain_point_guess, '') AS pain_point_guess,
                COALESCE(b.outreach_angle, '') AS outreach_angle,
                COALESCE(b.apparent_size, '') AS apparent_size,
                COALESCE(b.digital_maturity, '') AS digital_maturity,
                COALESCE(b.contact_name, '') AS contact_name,
                COALESCE(b.pipeline_stage, 'lead') AS pipeline_stage,
                '' AS site_emails,
                '' AS socials,
                'en' AS language
            FROM businesses b
            WHERE b.id = ?
            """,
            (lead_id,),
        ).fetchone()
        draft = generate_email(dict(lead), account=sender)
        outreach_id = log_outreach(
            conn,
            lead_id,
            "email",
            recipient["address"],
            draft["body"],
            draft["subject"],
            status="pending",
            message_variant_fingerprint="internal-matrix-test",
        )
        if send:
            final_body = render_outreach_body(draft["body"], sender, "en")
            send_email(recipient["address"], draft["subject"], final_body, sender)
            mark_sent(conn, outreach_id, sender.get("name", ""), sender.get("address", ""))
            status = "sent"
        else:
            status = "drafted"
        results.append(
            f"[{outreach_id}] {status}: {sender['address']} -> {recipient['address']} | niche={lead['target_niche'] or 'unknown'}"
        )

    return "\n".join([
        f"Internal matrix test complete ({'send' if send else 'draft only'}).",
        *results,
    ])


def deterministic_test_lead_flow(lead_id: int = 302, recipient: str = "", clear_history: bool = True, send: bool = False, sender_address: str = "") -> str:
    from outreach.email_sender import _load_accounts, pick_account, render_outreach_body, send_email
    from outreach.generator import generate_email
    from outreach.runtime import assert_outbound_allowed
    from outreach.storage.db import init_outreach_tables, log_outreach, mark_sent

    conn = _conn()  # leadgen/data/leads.db — contains both lead and outreach tables
    init_outreach_tables(conn)
    cols = {row["name"] for row in conn.execute("PRAGMA table_info(businesses)").fetchall()}

    lead = conn.execute(
        """
        SELECT
            b.id, b.name, b.category, b.address, b.website, b.phone, b.email_maps,
            COALESCE(b.target_niche, '') AS target_niche,
            COALESCE(b.top_gap, '') AS top_gap,
            COALESCE(b.top_opportunity, '') AS top_opportunity,
            COALESCE(b.gap_profile, '') AS gap_profile,
            COALESCE(b.opportunity_profile, '') AS opportunity_profile,
            COALESCE(b.brand_summary, '') AS brand_summary,
            COALESCE(b.pain_point_guess, '') AS pain_point_guess,
            COALESCE(b.outreach_angle, '') AS outreach_angle,
            COALESCE(b.apparent_size, '') AS apparent_size,
            COALESCE(b.digital_maturity, '') AS digital_maturity,
            COALESCE(b.pipeline_stage, 'lead') AS pipeline_stage,
            COALESCE(w.emails, '') AS site_emails,
            COALESCE(w.socials, '') AS socials,
            COALESCE(w.language, '') AS language
        FROM businesses b
        LEFT JOIN website_data w ON w.id = (
            SELECT MAX(w2.id) FROM website_data w2 WHERE w2.business_id = b.id
        )
        WHERE b.id = ?
        """,
        (lead_id,),
    ).fetchone()
    if not lead:
        return f"Lead {lead_id} not found."

    def _split_emails(raw: str) -> list[str]:
        values = []
        for part in (raw or "").replace(";", ",").split(","):
            candidate = part.strip()
            if "@" in candidate and candidate not in values:
                values.append(candidate)
        return values

    derived_emails = _split_emails(recipient) or _split_emails(lead["site_emails"]) or _split_emails(lead["email_maps"])
    to_address = derived_emails[0] if derived_emails else ""

    if clear_history:
        reply_ids = [row["id"] for row in conn.execute("SELECT id FROM replies WHERE lead_id = ?", (lead_id,)).fetchall()]
        if reply_ids:
            placeholders = ",".join("?" for _ in reply_ids)
            conn.execute(f"DELETE FROM reply_drafts WHERE reply_id IN ({placeholders})", reply_ids)
            conn.execute(f"DELETE FROM reply_classification WHERE reply_id IN ({placeholders})", reply_ids)
        review_keys = [
            row["review_batch_key"]
            for row in conn.execute(
                """
                SELECT DISTINCT review_batch_key
                FROM outreach_log
                WHERE lead_id = ? AND COALESCE(review_batch_key, '') != ''
                """,
                (lead_id,),
            ).fetchall()
        ]
        if review_keys:
            placeholders = ",".join("?" for _ in review_keys)
            conn.execute(f"DELETE FROM review_batches WHERE batch_key IN ({placeholders})", review_keys)
        conn.execute("DELETE FROM replies WHERE lead_id = ?", (lead_id,))
        conn.execute("DELETE FROM outreach_log WHERE lead_id = ?", (lead_id,))

        update_fields = {"pipeline_stage": "lead"}
        for field in ("service_type", "delivery_status", "onboarding_status", "next_action", "client_notes", "required_access"):
            if field in cols:
                update_fields[field] = ""
        if update_fields:
            set_sql = ", ".join(f"{key} = ?" for key in update_fields)
            conn.execute(f"UPDATE businesses SET {set_sql} WHERE id = ?", [*update_fields.values(), lead_id])
        conn.commit()

    if not to_address:
        return (
            f"Lead {lead_id} ({lead['name']}) was reset, but no usable email was found. "
            f"Needs enrichment before a test outreach can be sent."
        )

    # Resolve account early so generate_email gets the right sender name for sign-off
    _pre_account = None
    if send:
        if sender_address:
            all_accounts = _load_accounts()
            _pre_account = next((a for a in all_accounts if a["address"].lower() == sender_address.lower()), None)
        else:
            _pre_account = pick_account(conn)

    draft = generate_email(dict(lead), account=_pre_account)
    outreach_id = log_outreach(
        conn,
        lead_id,
        "email",
        to_address,
        draft["body"],
        draft["subject"],
        status="pending",
        message_variant_fingerprint=draft.get("fingerprint", "deterministic-test-flow"),
    )

    _sender_name = ""
    _sender_addr = ""
    action = "drafted"
    if send:
        assert_outbound_allowed("deterministic_test_lead_flow")
        account = _pre_account
        if account is None:
            raise RuntimeError("No outbound email account available for test send.")
        lang = (lead.get("language") or "en").strip() or "en"
        final_body = render_outreach_body(draft["body"], account, lang)
        send_email(to_address, draft["subject"], final_body, account)
        mark_sent(conn, outreach_id, account.get("name", ""), account.get("address", ""))
        _sender_name = account.get("name", "")
        _sender_addr = account.get("address", "")
        action = "sent"

    current = conn.execute(
        "SELECT status, approval_state, sent_at FROM outreach_log WHERE id = ?",
        (outreach_id,),
    ).fetchone()
    stage = conn.execute("SELECT pipeline_stage FROM businesses WHERE id = ?", (lead_id,)).fetchone()
    return "\n".join([
        f"Lead reset: {lead['name']} (id={lead_id})",
        f"Current stage: {stage['pipeline_stage'] if stage else 'unknown'}",
        f"To: {to_address}",
        f"Outreach id: {outreach_id}",
        f"Action: {action}",
        f"Status: {current['status'] if current else 'unknown'} / {current['approval_state'] if current else 'unknown'}",
        f"Sender: {_sender_name or '(draft only)'} <{_sender_addr or ''}>",
        "",
        "Next step: use the existing reply pipeline once a reply arrives, or run the reply-test helpers for an internal mailbox flow.",
    ])



def internal_reply_test_status(limit: int = 5) -> str:
    script = f"""
from storage.db import connect, init_outreach_tables, get_reply_queue, upsert_reply_draft
from reply_listener import poll_replies
from classifier import run_classifier
from reply_drafter import build_reply_draft

poll_replies()
conn = connect()
init_outreach_tables(conn)
run_classifier(conn)

rows = []
for row in get_reply_queue(conn, limit=50):
    subject = (row["reply_subject"] or "").lower()
    if "internal reply workflow test" in subject or "smtp test" in subject:
        rows.append(dict(row))

if not rows:
    print("No internal reply-test messages found yet.")
    raise SystemExit(0)

prepared = 0
for row in rows:
    draft = build_reply_draft(row)
    upsert_reply_draft(
        conn,
        row["reply_id"],
        draft.subject,
        draft.body,
        draft.sender_name,
        draft.sender_address,
        draft.rationale,
    )
    prepared += 1

rows = [dict(row) for row in get_reply_queue(conn, limit=50) if "internal reply workflow test" in ((row["reply_subject"] or "").lower()) or "smtp test" in ((row["reply_subject"] or "").lower())]
rows = rows[:{int(limit)}]

print(f"Prepared {{prepared}} internal reply draft(s).")
for row in rows:
    print(f"")
    print(f"[reply {{row['reply_id']}}] {{row['name']}} -> {{row['from_address'] or 'unknown'}}")
    print(f"from: {{row['sender_name'] or 'unassigned'}} <{{row['sender_address'] or 'unassigned'}}>")
    print(f"label: {{row['label'] or 'unclassified'}} | received: {{(row['received_at'] or '')[:19]}}")
    print(f"subject: {{row['draft_subject'] or row['reply_subject'] or '(no subject)'}}")
    if row.get("rationale"):
        print(f"rationale: {{row['rationale']}}")
    print("")
    print((row.get("draft_body") or "").strip() or "(no drafted body)")
    print("")
    print("---")
"""
    return _run_outreach_python(script)


def pending_drafts_count() -> str:
    conn = _conn()
    n = conn.execute(
        "SELECT COUNT(*) AS n FROM outreach_log WHERE status='pending'"
    ).fetchone()["n"]
    return f"{n} drafts pending review"


def pending_drafts(n: int = 10) -> str:
    conn = _conn()
    rows = conn.execute(
        """
        SELECT o.id, o.address, o.sender_address, o.subject, o.created_at, o.status, o.approval_state, o.review_batch_key, b.name, b.target_niche, o.error_note, o.send_after
        FROM outreach_log o
        JOIN businesses b ON b.id = o.lead_id
        WHERE o.status IN ('pending', 'approved', 'scheduled')
          AND NOT (o.status = 'pending' AND COALESCE(o.approval_state, 'pending') = 'rejected')
        ORDER BY CASE WHEN o.status='scheduled' THEN 0 WHEN o.status='approved' THEN 1 ELSE 2 END, COALESCE(o.send_after, o.created_at) ASC
        LIMIT ?
        """,
        (n,),
    ).fetchall()
    if not rows:
        return "No pending drafts."
    lines = []
    for row in rows:
        subject = (row["subject"] or "").strip() or "(no subject)"
        error = f"\n  error: {row['error_note']}" if row["error_note"] else ""
        scheduled = f"\n  send_after: {row['send_after']}" if row["status"] == "scheduled" and row["send_after"] else ""
        review = f" | review_batch={row['review_batch_key']}" if row["review_batch_key"] else ""
        sender = row["sender_address"] or "unassigned"
        lines.append(
            f"- [{row['id']}] {row['status']}: {row['name']} -> {row['address']}\n"
            f"  niche: {row['target_niche'] or 'unknown'}\n"
            f"  from: {sender}\n"
            f"  subject: {subject}\n"
            f"  approval: {row['approval_state'] or 'pending'}{review}\n"
            f"  created: {row['created_at'][:19]}{scheduled}{error}"
        )
    return "\n".join(lines)


def _signature_block(account: dict, language: str = "en") -> str:
    from outreach.email_sender import signature_block

    return signature_block(account, language)


def _render_final_body(body: str, account: dict, language: str = "en") -> str:
    from outreach.email_sender import render_outreach_body

    return render_outreach_body(body, account, language)


def preview_drafts(n: int = 3) -> str:
    script = f"""
from storage.db import connect, init_outreach_tables, get_pending_drafts, get_approved_drafts, get_scheduled_drafts
from email_sender import pick_account, render_outreach_body
import os

def infer_language(row):
    mode = os.environ.get("OUTREACH_LANGUAGE_MODE", "english_first").strip().lower()
    if mode in {{"english", "english_first", "en"}}:
        return "en"
    raw = (row["language"] or "").strip().lower()
    if raw:
        return raw
    website = (row["website"] or "").lower()
    address = (row["business_address"] or "").lower()
    if ".sk" in website or "bratislava" in address or "slovakia" in address or "slovensko" in address:
        return "sk"
    if ".cz" in website or "praha" in address or "brno" in address:
        return "cs"
    if ".at" in website or ".de" in website or "wien" in address or "vienna" in address:
        return "de"
    return "en"

conn = connect()
init_outreach_tables(conn)
rows = get_pending_drafts(conn)[:{int(n)}]
if not rows:
    rows = get_approved_drafts(conn)[:{int(n)}]
if not rows:
    rows = get_scheduled_drafts(conn, limit={int(n)})[:{int(n)}]
if not rows:
    print("No pending drafts to preview.")
    raise SystemExit(0)

for row in rows:
    acc = {{"name": row["sender_name"], "address": row["sender_address"]}} if row["sender_address"] else (pick_account(conn) or {{"name": "Sender", "address": "(next available account unavailable)"}})
    lang = infer_language(row)
    final_body = render_outreach_body(row["message"] or "", acc, lang)
    print(f"[{{row['id']}}] {{row['name']}} -> {{row['address']}}")
    print(f"language: {{lang}} | from: {{acc['name']}} <{{acc['address']}}>")
    print(f"subject: {{row['subject'] or '(no subject)'}} | status: {{row['status']}}")
    if row["send_after"]:
        print(f"send_after: {{row['send_after']}}")
    print("")
    print(final_body)
    print("")
    print("---")
"""
    return _run_outreach_python(script)


def _run_outreach_python(script: str) -> str:
    python = str(OUTREACH_PYTHON) if OUTREACH_PYTHON.exists() else sys.executable
    result = subprocess.run(
        [python, "-c", script],
        cwd=str(OUTREACH_DIR),
        capture_output=True,
        text=True,
        timeout=120,  # reduced from 300 — fail fast
    )
    stdout = (result.stdout or "").strip()
    stderr = (result.stderr or "").strip()
    if result.returncode != 0:
        # Surface the most useful part of stderr (last 800 chars covers tracebacks)
        err_detail = stderr[-800:] if stderr else stdout[-400:] if stdout else f"exit code {result.returncode}"
        raise RuntimeError(err_detail)
    return stdout or "(no output)"


def generate_drafts(limit: int = 5, target_niche: str = "") -> str:
    script = f"""
from storage.db import connect, init_outreach_tables, log_outreach
from generator import generate_email

conn = connect()
init_outreach_tables(conn)
tables = {{row["name"] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}}
bcols = {{row["name"] for row in conn.execute("PRAGMA table_info(businesses)").fetchall()}}
has_niche_support = "target_niche" in bcols
has_shortlist = (
    has_niche_support
    and "niche_research" in tables
    and conn.execute("SELECT COUNT(*) AS n FROM niche_research WHERE status='shortlisted'").fetchone()["n"] > 0
)
target_niche_select = "COALESCE(b.target_niche, '') AS target_niche" if has_niche_support else "'' AS target_niche"
query = '''
SELECT b.id, b.name, b.category, b.address, b.website,
       b.phone, b.email_maps, b.outreach_angle, b.top_gap, b.top_opportunity,
       b.gap_profile, b.opportunity_profile, b.brand_summary, b.pain_point_guess,
       b.apparent_size, b.digital_maturity,
       ''' + target_niche_select + ''',
       w.emails AS site_emails, w.socials,
       COALESCE(NULLIF(TRIM(w.language), ''), 'en') AS language
FROM businesses b
LEFT JOIN website_data w ON w.id = (
    SELECT MAX(w2.id) FROM website_data w2 WHERE w2.business_id = b.id
)
WHERE b.validation_status = 'qualified'
  AND COALESCE(b.target_niche, '') != ''
  AND LOWER(COALESCE(b.name, '')) NOT LIKE '%association%'
  AND LOWER(COALESCE(b.name, '')) NOT LIKE '%associat%'
  AND LOWER(COALESCE(b.name, '')) NOT LIKE '%asociáci%'
  AND LOWER(COALESCE(b.name, '')) NOT LIKE '%asociaci%'
  AND LOWER(COALESCE(b.name, '')) NOT LIKE '%komora%'
  AND LOWER(COALESCE(b.name, '')) NOT LIKE '%národná asociácia%'
  AND LOWER(COALESCE(b.name, '')) NOT LIKE '%national association%'
  AND b.id NOT IN (
      SELECT DISTINCT lead_id FROM outreach_log WHERE status IN ('sent', 'pending', 'approved')
  )
'''
if has_shortlist:
    query += " AND b.target_niche IN (SELECT niche FROM niche_research WHERE status='shortlisted')"
target_niche = {target_niche!r}.strip()
if target_niche:
    query += " AND b.target_niche = ?"
query += " ORDER BY b.score DESC NULLS LAST, b.id ASC LIMIT {int(limit)}"
if target_niche:
    leads = [dict(row) for row in conn.execute(query, (target_niche,)).fetchall()]
else:
    leads = [dict(row) for row in conn.execute(query).fetchall()]
if not leads:
    if target_niche:
        print(f"No qualified leads found for niche '{{target_niche}}'.")
    elif has_shortlist:
        print("No qualified leads found for the current shortlist.")
    else:
        print("No qualified leads with niche signal and without outreach found.")
    raise SystemExit(0)

created = []
errors = []
for lead in leads:
    try:
        emails = (lead.get("site_emails") or lead.get("email_maps") or "").split(",")
        to_addr = next((email.strip() for email in emails if "@" in email), "")
        if not to_addr:
            errors.append(f"{{lead['name']}}: no email")
            continue
        # Assign sender at draft creation time so preview shows correct name
        from email_sender import pick_account
        acc = pick_account(conn)
        result = generate_email(lead, account=acc)
        draft_id = log_outreach(conn, lead["id"], "email", to_addr, result["body"], result["subject"], status="pending", message_variant_fingerprint=result.get("fingerprint", ""))
        if acc and draft_id:
            conn.execute("UPDATE outreach_log SET sender_name=?, sender_address=? WHERE id=?",
                         (acc.get("name",""), acc.get("address",""), draft_id))
            conn.commit()
        sender_label = acc["address"] if acc else "unassigned"
        created.append(f"- [{{draft_id}}] {{lead['name']}} ({{lead.get('target_niche') or 'unknown'}}) -> {{to_addr}} | from={{sender_label}} | {{result['subject']}}")
    except Exception as e:
        errors.append(f"{{lead['name']}}: {{e}}")
    except Exception as e:
        errors.append(f"{{lead['name']}}: {{e}}")

print(f"Created {{len(created)}} draft(s).")
for line in created:
    print(line)
if errors:
    print("Errors:")
    for error in errors:
        print(f"- {{error}}")
"""
    return _run_outreach_python(script)


def approve_drafts(limit: int = 5) -> str:
    script = f"""
from storage.db import connect, init_outreach_tables, mark_approved

conn = connect()
init_outreach_tables(conn)
rows = conn.execute('''
SELECT id, address, subject
FROM outreach_log
WHERE status='pending'
ORDER BY created_at ASC
LIMIT {int(limit)}
''').fetchall()
if not rows:
    print("No pending drafts to approve.")
    raise SystemExit(0)
for row in rows:
    mark_approved(conn, row["id"])
print(f"Approved {{len(rows)}} draft(s).")
for row in rows:
    print(f"- [{{row['id']}}] {{row['address']}} | {{row['subject']}}")
"""
    return _run_outreach_python(script)


def send_review_batch(limit: int = 5, recipient: str = "egorbrusnyak@gmail.com") -> str:
    assert_outbound_allowed("send_review_batch")
    script = f"""
import uuid
import os
from storage.db import (
    connect,
    init_outreach_tables,
    get_pending_drafts,
    create_review_batch,
    mark_review_batch_sent,
)
from email_sender import _load_accounts, _last_sent_at, _sent_today, render_outreach_body, send_email

def infer_language(row):
    mode = os.environ.get("OUTREACH_LANGUAGE_MODE", "english_first").strip().lower()
    if mode in {{"english", "english_first", "en"}}:
        return "en"
    raw = (row["language"] or "").strip().lower()
    if raw:
        return raw
    website = (row["website"] or "").lower()
    address = (row["business_address"] or "").lower()
    if ".sk" in website or "bratislava" in address or "slovakia" in address or "slovensko" in address:
        return "sk"
    if ".cz" in website or "praha" in address or "brno" in address:
        return "cs"
    if ".at" in website or ".de" in website or "wien" in address or "vienna" in address:
        return "de"
    return "en"

def pick_batch_accounts(conn, count):
    eligible = []
    for acc in _load_accounts():
        sent_today = _sent_today(conn, acc["address"])
        if sent_today >= acc["daily_limit"]:
            continue
        eligible.append((sent_today, _last_sent_at(conn, acc["address"]), acc["index"], dict(acc)))
    eligible.sort(key=lambda item: (item[0], item[1], item[2]))
    if not eligible:
        return []
    usage = {{item[3]["address"]: item[0] for item in eligible}}
    ordered = [item[3] for item in eligible]
    assigned = []
    for offset in range(count):
        ordered.sort(key=lambda acc: (usage.get(acc["address"], 0), acc["index"]))
        acc = ordered[0]
        assigned.append(dict(acc))
        usage[acc["address"]] = usage.get(acc["address"], 0) + 1
    return assigned

conn = connect()
init_outreach_tables(conn)
rows = get_pending_drafts(conn)[:{int(limit)}]
if not rows:
    print("No pending drafts to send for review.")
    raise SystemExit(0)

assigned_accounts = pick_batch_accounts(conn, len(rows))
if not assigned_accounts:
    print("No sender account available for review email.")
    raise SystemExit(1)

batch_key = "review-" + uuid.uuid4().hex[:8]
review_sender = assigned_accounts[0]
subject = f"[BIZ Review {{batch_key}}] Outreach draft approval"
blocks = []
for row, account in zip(rows, assigned_accounts):
    lang = infer_language(row)
    conn.execute(
        "UPDATE outreach_log SET sender_name=?, sender_address=?, signature_name=?, error_note=NULL WHERE id=?",
        (
            account.get("name", ""),
            account.get("address", ""),
            ((account.get("name") or "").split()[0] + " " + (account.get("name") or "").split()[-1][0] + ".") if len((account.get("name") or "").split()) >= 2 else (account.get("name") or ""),
            row["id"],
        ),
    )
    final_body = render_outreach_body(row["message"] or "", account, lang)
    blocks.append(
        f"From: {{account['name']}} <{{account['address']}}>\\n"
        f"To: [{{row['id']}}] {{row['name']}} -> {{row['address']}}\\n"
        f"Language: {{lang}}\\n"
        f"Subject: {{row['subject'] or '(no subject)'}}\\n\\n"
        f"{{final_body}}"
    )

body = (
    f"Review batch: {{batch_key}}\\n"
    "Reply with APPROVE to unlock the batch for real outreach.\\n"
    "Reply with REJECT or HOLD to keep the batch blocked.\\n"
    "Any other reply keeps the batch waiting for an explicit decision.\\n\\n"
    + "\\n\\n---\\n\\n".join(blocks)
)

create_review_batch(
    conn,
    batch_key=batch_key,
    recipient={recipient!r},
    sender_name=review_sender.get("name", ""),
    sender_address=review_sender.get("address", ""),
    subject=subject,
    body=body,
    draft_ids=[row["id"] for row in rows],
)
conn.commit()
try:
    send_email({recipient!r}, subject, body, review_sender)
except Exception as e:
    conn.execute("DELETE FROM review_batches WHERE batch_key=?", (batch_key,))
    conn.execute(
        "UPDATE outreach_log "
        "SET approval_state='pending', review_batch_key=NULL, error_note=? "
        "WHERE review_batch_key=?",
        (f"review send failed: {{e}}", batch_key),
    )
    conn.commit()
    raise

mark_review_batch_sent(conn, batch_key)
print(f"Sent review batch {{batch_key}} to {recipient}.")
for row, account in zip(rows, assigned_accounts):
    print(f"- [{{row['id']}}] {{row['name']}} -> {{row['address']}} | from={{account['address']}}")
"""
    return _run_outreach_python(script)


def review_batch_status(limit: int = 10) -> str:
    script = f"""
from storage.db import connect, init_outreach_tables, get_open_review_batches
conn = connect()
init_outreach_tables(conn)
rows = get_open_review_batches(conn, limit={int(limit)})
if not rows:
    print("No open review batches.")
    raise SystemExit(0)
for row in rows:
    print(
        f"- {{row['batch_key']}} | status={{row['status']}} | reviewer={{row['recipient']}} | "
        f"review_from={{row['sender_address'] or 'unassigned'}} | drafts={{row['draft_count']}} | "
        f"sent_at={{row['sent_at'] or 'n/a'}} | replied_at={{row['replied_at'] or 'n/a'}}"
    )
"""
    return _run_outreach_python(script)


def poll_review_gate(limit: int = 10) -> str:
    script = f"""
import imaplib
import os
import re
from datetime import datetime, timedelta, timezone
from email.header import decode_header
from email.utils import parsedate_to_datetime
from email import message_from_bytes
from pathlib import Path
from dotenv import load_dotenv
from storage.db import (
    connect,
    init_outreach_tables,
    get_open_review_batches,
    mark_review_batch_replied,
    approve_review_batch,
    reject_review_batch,
)

IMAP_HOST = "imap.gmail.com"
IMAP_PORT = 993
load_dotenv(Path.cwd().parent / ".env")

def decode_value(value):
    parts = decode_header(value or "")
    out = []
    for part, enc in parts:
        if isinstance(part, bytes):
            out.append(part.decode(enc or "utf-8", errors="replace"))
        else:
            out.append(part)
    return "".join(out)

def get_body(msg):
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    return payload.decode(part.get_content_charset() or "utf-8", errors="replace")
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            return payload.decode(msg.get_content_charset() or "utf-8", errors="replace")
    return ""

def sanitize_reply(body):
    lines = []
    for raw_line in (body or "").splitlines():
        line = raw_line.rstrip()
        if not line:
            if lines and lines[-1] != "":
                lines.append("")
            continue
        if line.startswith(">"):
            continue
        lower = line.lower()
        if lower.startswith("on ") and " wrote:" in lower:
            break
        if lower.startswith("from:") or lower.startswith("subject:") or lower.startswith("to:"):
            break
        lines.append(line)
    text = "\\n".join(lines).strip()
    return text[:1000]

def load_accounts():
    accounts = []
    i = 1
    while True:
        addr = os.environ.get(f"EMAIL_{{i}}_ADDRESS")
        if not addr:
            break
        accounts.append({{
            "address": addr.strip(),
            "password": os.environ.get(f"EMAIL_{{i}}_PASSWORD", "").replace(" ", ""),
        }})
        i += 1
    return accounts

conn = connect()
init_outreach_tables(conn)
batches = [dict(row) for row in get_open_review_batches(conn, limit={int(limit)})]
if not batches:
    print("No open review batches.")
    raise SystemExit(0)

decisions = []
for batch in batches:
    sender = (batch.get("sender_address") or "").strip().lower()
    if not sender:
        continue
    account = next((acc for acc in load_accounts() if acc["address"].lower() == sender), None)
    if account is None:
        decisions.append(f"{{batch['batch_key']}}: sender account not found")
        continue
    try:
        mail = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
        mail.login(account["address"], account["password"])
        mail.select("INBOX")
        since = (datetime.now() - timedelta(days=14)).strftime("%d-%b-%Y")
        status, data = mail.search(None, f'(SINCE "{{since}}")')
        if status != "OK":
            mail.logout()
            continue
        matched = None
        ids = list(data[0].split())
        ids.reverse()
        for num in ids:
            status, msg_data = mail.fetch(num, "(RFC822)")
            if status != "OK" or not msg_data or not msg_data[0]:
                continue
            raw = msg_data[0][1]
            msg = message_from_bytes(raw)
            subject = decode_value(msg.get("Subject", ""))
            from_raw = decode_value(msg.get("From", ""))
            if batch["batch_key"] not in subject:
                continue
            if (batch.get("recipient") or "").lower() not in from_raw.lower():
                continue
            body = get_body(msg).strip()
            matched = sanitize_reply(body) or "(empty reply)"
            try:
                _ = parsedate_to_datetime(msg.get("Date", "")).astimezone(timezone.utc).isoformat()
            except Exception:
                pass
            break
        mail.logout()
        if matched is None:
            continue
        mark_review_batch_replied(conn, batch["batch_key"], matched)
        upper = matched.upper()
        if any(token in upper for token in ["REJECT", "HOLD", "DO NOT SEND"]):
            count = reject_review_batch(conn, batch["batch_key"], matched)
            decisions.append(f"{{batch['batch_key']}}: rejected {{count}} draft(s)")
        elif any(token in upper for token in ["APPROVE", "SEND", "GO AHEAD"]):
            count = approve_review_batch(conn, batch["batch_key"], matched)
            decisions.append(f"{{batch['batch_key']}}: approved {{count}} draft(s)")
        else:
            decisions.append(f"{{batch['batch_key']}}: waiting for explicit APPROVE or REJECT")
    except Exception as e:
        decisions.append(f"{{batch['batch_key']}}: poll error {{e}}")

if decisions:
    print("\\n".join(decisions))
else:
    print("No review replies yet.")
"""
    return _run_outreach_python(script)


def schedule_approved(limit: int = 5) -> str:
    assert_outbound_allowed("schedule_approved")
    script = f"""
import random
from storage.db import connect, init_outreach_tables, get_approved_drafts, mark_scheduled
from email_sender import pick_account, next_send_after

conn = connect()
init_outreach_tables(conn)
rows = get_approved_drafts(conn)[:{int(limit)}]
if not rows:
    print("No approved drafts to send.")
    raise SystemExit(0)

scheduled = []
failed = []
for row in rows:
    acc = pick_account(conn)
    if acc is None:
        failed.append(f"[{{row['id']}}] {{row['address']}}: all accounts at daily limit")
        continue
    try:
        seed = random.randint(1000, 999999)
        send_after = next_send_after(
            conn,
            acc["address"],
            jitter_seed=seed,
            lead_address=row["business_address"] or row["website"] or "",
        ).isoformat()
        mark_scheduled(conn, row["id"], acc.get("name", ""), acc.get("address", ""), send_after, seed)
        scheduled.append(f"[{{row['id']}}] {{row['address']}} via {{acc['address']}} at {{send_after}}")
    except Exception as e:
        failed.append(f"[{{row['id']}}] {{row['address']}}: {{e}}")

print(f"Scheduled {{len(scheduled)}} draft(s).")
for line in scheduled:
    print(f"- {{line}}")
if failed:
    print("Failures:")
    for line in failed:
        print(f"- {{line}}")
"""
    return _run_outreach_python(script)


def process_send_queue(limit: int = 5) -> str:
    assert_outbound_allowed("process_send_queue")
    script = f"""
from datetime import datetime, timezone
import time
import random
from storage.db import connect, init_outreach_tables, get_due_scheduled_drafts, mark_failed, mark_sent
from email_sender import render_outreach_body, send_email
import os

def infer_language(row):
    mode = os.environ.get("OUTREACH_LANGUAGE_MODE", "english_first").strip().lower()
    if mode in {{"english", "english_first", "en"}}:
        return "en"
    raw = (row["language"] or "").strip().lower()
    if raw:
        return raw
    website = (row["website"] or "").lower()
    address = (row["business_address"] or "").lower()
    if ".sk" in website or "bratislava" in address or "slovakia" in address or "slovensko" in address:
        return "sk"
    if ".cz" in website or "praha" in address or "brno" in address:
        return "cs"
    if ".at" in website or ".de" in website or "wien" in address or "vienna" in address:
        return "de"
    return "en"

conn = connect()
init_outreach_tables(conn)
rows = get_due_scheduled_drafts(conn, datetime.now(timezone.utc).isoformat(), limit={int(limit)})
if not rows:
    print("No scheduled drafts are due right now.")
    raise SystemExit(0)

# Implementation Note: 1-10 minute pacing (60-600s) as per user request to mimic human behavior.
sent = []
failed = []
for i, row in enumerate(rows):
    if i > 0:
        sleep_time = random.uniform(60.0, 600.0)
        print(f"Pacing: sleeping for {{sleep_time:.1f}}s...")
        time.sleep(sleep_time)

    acc = {{"name": row["sender_name"], "address": row["sender_address"], "password": "", "daily_limit": 0}}
    try:
        from email_sender import _load_accounts
        for known in _load_accounts():
            if known["address"] == row["sender_address"]:
                acc = known
                break
        final_body = render_outreach_body(row["message"] or "", acc, infer_language(row))
        send_email(row["address"], row["subject"] or "", final_body, acc)
        mark_sent(conn, row["id"], acc.get("name", ""), acc.get("address", ""))
        sent.append(f"[{{row['id']}}] {{row['address']}} via {{acc['address']}}")
    except Exception as e:
        mark_failed(conn, row["id"], str(e))
        failed.append(f"[{{row['id']}}] {{row['address']}}: {{e}}")

print(f"Sent {{len(sent)}} draft(s).")
for line in sent:
    print(f"- {{line}}")
if failed:
    print("Failures:")
    for line in failed:
        print(f"- {{line}}")
"""
    return _run_outreach_python(script)




def send_queue_status(limit: int = 10) -> str:
    script = f"""
from storage.db import connect, init_outreach_tables, get_scheduled_drafts, sender_utilization
conn = connect()
init_outreach_tables(conn)
rows = get_scheduled_drafts(conn, limit={int(limit)})
util = sender_utilization(conn)
if rows:
    print("Scheduled queue:")
    for row in rows:
        print(f"- [{{row['id']}}] {{row['name']}} -> {{row['address']}} | from={{row['sender_address'] or 'unassigned'}} | send_after={{row['send_after'] or 'unset'}}")
else:
    print("Scheduled queue: empty")
print("")
print("Sender utilization:")
for row in util:
    print(f"- {{row['sender_address']}} | sent={{row['sent_count']}} | scheduled={{row['scheduled_count']}} | last={{row['last_sent_at'] or 'n/a'}}")
"""
    return _run_outreach_python(script)


def poll_and_classify_replies() -> str:
    script = """
from reply_listener import poll_replies
from classifier import run_classifier
from storage.db import connect, init_outreach_tables, get_stats, get_reply_queue_needing_action, upsert_reply_draft
from reply_drafter import build_reply_draft

logged = poll_replies()
conn = connect()
init_outreach_tables(conn)
classified = run_classifier(conn)
prepared = 0
for row in get_reply_queue_needing_action(conn, limit=20):
    draft = build_reply_draft(dict(row))
    upsert_reply_draft(
        conn,
        row["reply_id"],
        draft.subject,
        draft.body,
        draft.sender_name,
        draft.sender_address,
        draft.rationale,
    )
    prepared += 1
stats = get_stats(conn)
print(f"Replies logged: {logged}")
print(f"Replies classified: {classified}")
print(f"Reply drafts prepared: {{prepared}}")
print(f"Total replies: {stats['replies']} | Pending drafts: {stats['pending']}")
"""
    return _run_outreach_python(script)


def test_send_lead(lead_id: int, recipient: str = "", clear_history: bool = True, sender_address: str = "") -> str:
    """
    Generate and immediately send an outreach email for a specific lead.
    Bypasses the scheduled queue — use for testing and demos.
    Wraps deterministic_test_lead_flow(send=True).
    Requires BIZ_SAFE_MODE=0 (or equivalent) to actually send.
    """
    return deterministic_test_lead_flow(
        lead_id=lead_id,
        recipient=recipient,
        clear_history=clear_history,
        send=True,
        sender_address=sender_address,
    )


def poll_replies_for_lead(lead_id: int) -> str:
    """
    Poll IMAP for new replies then return any replies stored for this lead.
    Read-only — safe to call without approval.
    """
    script = f"""
from reply_listener import poll_replies
from storage.db import connect, init_outreach_tables
from classifier import run_classifier

logged = poll_replies()
conn = connect()
init_outreach_tables(conn)
run_classifier(conn)

rows = conn.execute(
    '''
    SELECT r.id, r.received_at, r.content, r.channel,
           COALESCE(rc.label, 'unclassified') AS label
    FROM replies r
    LEFT JOIN reply_classification rc ON rc.reply_id = r.id
    WHERE r.lead_id = ?
    ORDER BY r.received_at DESC
    LIMIT 5
    ''',
    ({int(lead_id)},)
).fetchall()

if not rows:
    print(f"No replies found for lead {int(lead_id)} yet. (Logged {{logged}} new message(s) this poll)")
else:
    print(f"Replies for lead {int(lead_id)} ({{len(rows)}} found, {{logged}} new this poll):")
    for r in rows:
        snippet = " ".join((r["content"] or "").split())[:200]
        print(f"")
        print(f"[{{r['label']}}] {{r['received_at'][:19]}} via {{r['channel']}}")
        print(f"{{snippet}}")
"""
    return _run_outreach_python(script)


def trades_demo_status(limit: int = 10) -> str:
    from outreach.trades_demo import trades_demo_status as _status

    data = _status(limit=limit)
    stats = data["stats"]
    lines = [
        "Trades demo status",
        f"- inquiries: {stats['total']} total | {stats['new_count']} new | {stats.get('pending_approval_count', 0)} pending approval | {stats['qualified_count']} qualified | {stats['booked_count']} booked | {stats['failed_count']} failed",
        f"- jobs: {stats['total_jobs']} total | {stats['completed_jobs']} completed | {stats['manual_jobs']} manual | {stats['failed_jobs']} failed",
    ]
    for row in data["inquiries"][:limit]:
        lines.append(
            f"- [inquiry {row['id']}] {row['from_address']} | {row['status']} | approval={row.get('approval_status') or 'n/a'} | job={row['job_type'] or 'unknown'} | booking={row['booking_status'] or 'n/a'}"
        )
    return "\n".join(lines)


def run_trades_demo(limit: int = 10, since_days: int = 14, send_response: bool = False, require_approval: bool = True) -> str:
    from outreach.trades_demo import run_trades_demo_cycle

    result = run_trades_demo_cycle(
        limit=limit,
        since_days=since_days,
        send_response=send_response,
        require_approval=require_approval,
    )
    return (
        "Trades demo cycle complete.\n"
        f"- added: {result['added']}\n"
        f"- processed: {result['processed']}\n"
        f"- failed: {result['failed']}"
    )


def simulate_trades_demo_inquiry(from_email: str, subject: str, body: str, from_name: str = "Demo Prospect") -> str:
    from outreach.trades_demo import simulate_demo_inquiry

    inquiry_id = simulate_demo_inquiry(
        from_email=from_email,
        from_name=from_name,
        subject=subject,
        body=body,
    )
    return f"Created trades demo inquiry {inquiry_id} for {from_email}"


def run_trades_demo_approval(inquiry_id: int, send_response: bool = True) -> str:
    from outreach.trades_demo import approve_demo_inquiry

    result = approve_demo_inquiry(inquiry_id, send_response=send_response)
    return (
        f"Approved trades demo inquiry {inquiry_id}.\n"
        f"- status: {result['status']}\n"
        f"- booking: {result['booking'].get('status', 'n/a')}\n"
        f"- mode: {result['booking'].get('mode', 'n/a')}"
    )


def run_trades_demo_approval_all(limit: int = 20, send_response: bool = True) -> str:
    from outreach.trades_demo import approve_all_demo_inquiries

    result = approve_all_demo_inquiries(limit=limit, send_response=send_response)
    blocked = result.get("blocked")
    if blocked:
        return f"Bulk approval blocked.\n- reason: {blocked}"
    lines = [
        "Approved all staged trades demo inquiries.",
        f"- approved: {len(result['approved'])}",
        f"- failed: {len(result['failed'])}",
    ]
    for inquiry_id in result["approved"]:
        lines.append(f"  - inquiry {inquiry_id}")
    for item in result["failed"]:
        lines.append(f"  - failed {item}")
    return "\n".join(lines)


def reject_trades_demo_inquiry(inquiry_id: int, reason: str = "") -> str:
    from outreach.trades_demo import reject_demo_inquiry

    result = reject_demo_inquiry(inquiry_id, reason=reason)
    return f"Rejected trades demo inquiry {inquiry_id}. Status: {result['status']}"


def edit_trades_demo_inquiry(inquiry_id: int, response_body: str, response_subject: str = "") -> str:
    from outreach.trades_demo import edit_demo_inquiry_response

    result = edit_demo_inquiry_response(
        inquiry_id,
        response_body=response_body,
        response_subject=response_subject,
    )
    return (
        f"Edited trades demo inquiry {inquiry_id}.\n"
        f"- status: {result['status']}\n"
        f"- subject: {result['response_subject']}"
    )


def delete_draft(outreach_id: int) -> str:
    """Delete a pending outreach draft by ID. Only works on status=pending drafts."""
    conn = _conn()
    row = conn.execute("SELECT status, lead_id FROM outreach_log WHERE id=?", (outreach_id,)).fetchone()
    if not row:
        return f"Draft [{outreach_id}] not found."
    if row["status"] not in ("pending", "approved"):
        return f"Draft [{outreach_id}] has status '{row['status']}' — can only delete pending/approved drafts."
    conn.execute("DELETE FROM outreach_log WHERE id=?", (outreach_id,))
    conn.commit()
    return f"Deleted draft [{outreach_id}]."


def quality_check_send(draft_ids: list[int], target_email: str) -> str:
    """Send a preview copy of the listed outreach drafts to a test email address for quality assurance."""
    from outreach.storage.db import connect, init_outreach_tables
    from outreach.email_sender import send_email, _load_accounts, render_outreach_body
    
    conn = connect()
    init_outreach_tables(conn)
    accounts = {acc["address"]: acc for acc in _load_accounts()}
    
    results = []
    for draft_id in draft_ids:
        row = conn.execute(
            "SELECT id, lead_id, subject, message, sender_name, sender_address "
            "FROM outreach_log WHERE id = ?", (int(draft_id),)
        ).fetchone()
        
        if not row:
            results.append(f"Draft [{draft_id}] not found.")
            continue
            
        lead_row = conn.execute("SELECT language FROM website_data WHERE business_id = ? ORDER BY id DESC LIMIT 1", (row["lead_id"],)).fetchone()
        lang = lead_row["language"] if lead_row and lead_row["language"] else "en"
        
        acc = accounts.get(row["sender_address"])
        if not acc:
            results.append(f"Draft [{draft_id}] sender account '{row['sender_address']}' not found. Cannot send quality check.")
            continue
            
        full_body = render_outreach_body(row["message"], acc, lang)
        subject_str = f"[QUALITY CHECK] {row['subject']}"
        
        try:
            send_email(target_email, subject_str, full_body, acc)
            results.append(f"Draft [{draft_id}] successfully sent quality check preview to {target_email} via {acc['address']}.")
        except Exception as e:
            results.append(f"Draft [{draft_id}] failed quality check send: {e}")
            
    return "\n".join(results)


def update_draft(outreach_id: int, body: str = "", subject: str = "") -> str:
    """Edit the body and/or subject of a pending outreach draft before it's sent."""
    conn = _conn()
    row = conn.execute("SELECT status FROM outreach_log WHERE id=?", (outreach_id,)).fetchone()
    if not row:
        return f"Draft [{outreach_id}] not found."
    if row["status"] not in ("pending", "approved"):
        return f"Draft [{outreach_id}] has status '{row['status']}' — can only edit pending/approved drafts."
    updates = {}
    if body:
        updates["message"] = body
    if subject:
        updates["subject"] = subject
    if not updates:
        return "Nothing to update — provide body and/or subject."
    set_sql = ", ".join(f"{k}=?" for k in updates)
    conn.execute(f"UPDATE outreach_log SET {set_sql} WHERE id=?", [*updates.values(), outreach_id])
    conn.commit()
    return f"Updated draft [{outreach_id}]: {', '.join(updates.keys())} changed."


def dismiss_reply(reply_id: int) -> str:
    """Mark a reply as notified and skip drafting — removes it from the action queue."""
    conn = _conn()
    row = conn.execute("SELECT id FROM replies WHERE id=?", (reply_id,)).fetchone()
    if not row:
        return f"Reply [{reply_id}] not found."
    conn.execute("UPDATE replies SET notified=1 WHERE id=?", (reply_id,))
    # Mark draft as skipped if one exists
    conn.execute(
        "UPDATE reply_drafts SET status='skipped' WHERE reply_id=? AND status IN ('pending','draft')",
        (reply_id,),
    )
    conn.commit()
    return f"Reply [{reply_id}] dismissed — removed from action queue."


def update_reply_draft(reply_id: int, body: str = "", subject: str = "") -> str:
    """Edit the body and/or subject of a reply draft before sending."""
    conn = _conn()
    row = conn.execute("SELECT id FROM reply_drafts WHERE reply_id=?", (reply_id,)).fetchone()
    if not row:
        return f"No draft found for reply [{reply_id}]."
    updates = {}
    if body:
        updates["body"] = body
    if subject:
        updates["subject"] = subject
    if not updates:
        return "Nothing to update — provide body and/or subject."
    set_sql = ", ".join(f"{k}=?" for k in updates)
    conn.execute(f"UPDATE reply_drafts SET {set_sql} WHERE reply_id=?", [*updates.values(), reply_id])
    conn.commit()
    return f"Updated reply draft for reply [{reply_id}]: {', '.join(updates.keys())} changed."


def generate_draft_for_lead(lead_id: int, sender_address: str = "", recipient: str = "") -> str:
    """
    Generate a single outreach draft for a specific lead, with explicit sender control.
    sender_address: which account to send from (e.g. 'victor.brusnyak@gmail.com')
    recipient: override the to-address (useful for test sends)
    Returns the draft ID and a full preview including sign-off.
    """
    from outreach.email_sender import _load_accounts, pick_account
    from outreach.generator import generate_email
    from outreach.storage.db import connect, init_outreach_tables, log_outreach

    conn = connect()
    init_outreach_tables(conn)

    # Load lead from leadgen DB (same file as outreach DB)
    row = conn.execute(
        """
        SELECT b.id, b.name, b.category, b.address, b.website, b.phone, b.email_maps,
               COALESCE(b.target_niche,'') AS target_niche,
               COALESCE(b.top_gap,'') AS top_gap,
               COALESCE(b.top_opportunity,'') AS top_opportunity,
               COALESCE(b.gap_profile,'') AS gap_profile,
               COALESCE(b.opportunity_profile,'') AS opportunity_profile,
               COALESCE(b.brand_summary,'') AS brand_summary,
               COALESCE(b.pain_point_guess,'') AS pain_point_guess,
               COALESCE(b.outreach_angle,'') AS outreach_angle,
               COALESCE(b.apparent_size,'') AS apparent_size,
               COALESCE(b.digital_maturity,'') AS digital_maturity,
               COALESCE(b.contact_name,'') AS contact_name,
               COALESCE(b.pipeline_stage,'lead') AS pipeline_stage,
               COALESCE(w.emails,'') AS site_emails,
               COALESCE(w.socials,'') AS socials,
               COALESCE(NULLIF(TRIM(w.language),''),'en') AS language
        FROM businesses b
        LEFT JOIN website_data w ON w.id = (SELECT MAX(id) FROM website_data WHERE business_id=b.id)
        WHERE b.id = ?
        """,
        (lead_id,),
    ).fetchone()
    if not row:
        return f"Lead {lead_id} not found."

    lead = dict(row)

    # Resolve sender
    accounts = {acc["address"]: acc for acc in _load_accounts()}
    if sender_address:
        acc = accounts.get(sender_address.strip())
        if acc is None:
            return f"Sender account {sender_address!r} not found. Available: {list(accounts.keys())}"
    else:
        acc = pick_account(conn)
    if acc is None:
        return "No sender account available."

    # Resolve recipient
    emails = (lead.get("site_emails") or lead.get("email_maps") or "").split(",")
    to_addr = (recipient or "").strip() or next((e.strip() for e in emails if "@" in e), "")
    if not to_addr:
        return f"Lead {lead_id} has no email address."

    # Generate draft
    draft = generate_email(lead, account=acc)
    draft_id = log_outreach(
        conn, lead_id, "email", to_addr, draft["body"], draft["subject"],
        status="pending", message_variant_fingerprint=draft.get("fingerprint", ""),
    )
    conn.execute(
        "UPDATE outreach_log SET sender_name=?, sender_address=? WHERE id=?",
        (acc.get("name", ""), acc.get("address", ""), draft_id),
    )
    conn.commit()

    from outreach.email_sender import render_outreach_body

    lang = lead.get("language", "en")
    full_body = render_outreach_body(draft["body"], acc, lang)

    return "\n".join([
        f"Draft [{draft_id}] created.",
        f"To: {to_addr}",
        f"From: {acc['name']} <{acc['address']}>",
        f"Subject: {draft['subject']}",
        "",
        full_body,
    ])
