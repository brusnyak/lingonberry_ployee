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
from pathlib import Path

from outreach.runtime import assert_outbound_allowed
from outreach.senders import canonical_sender

LEADS_DB = Path(__file__).parent.parent.parent / "leadgen" / "data" / "leads.db"
OUTREACH_DIR = Path(__file__).parent.parent.parent / "outreach"
OUTREACH_PYTHON = OUTREACH_DIR / ".venv" / "bin" / "python"


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
    internal = {addr.lower() for addr in [
        "maxberryme68@gmail.com",
        "brusnyak.f@gmail.com",
        "victor.brusnyak@gmail.com",
        "brusnyakyegor@gmail.com",
    ]}
    reply_rows = conn.execute(
        """
        SELECT
            COUNT(*) FILTER (
                WHERE LOWER(COALESCE(subject, '')) LIKE '%internal reply workflow test%'
                   OR LOWER(COALESCE(subject, '')) LIKE '%smtp test%'
                   OR LOWER(COALESCE(from_address, '')) IN ('maxberryme68@gmail.com','brusnyak.f@gmail.com','victor.brusnyak@gmail.com','brusnyakyegor@gmail.com')
            ) AS test_replies,
            COUNT(*) FILTER (
                WHERE NOT (
                    LOWER(COALESCE(subject, '')) LIKE '%internal reply workflow test%'
                    OR LOWER(COALESCE(subject, '')) LIKE '%smtp test%'
                    OR LOWER(COALESCE(from_address, '')) IN ('maxberryme68@gmail.com','brusnyak.f@gmail.com','victor.brusnyak@gmail.com','brusnyakyegor@gmail.com')
                )
            ) AS prod_replies
        FROM replies
        """
    ).fetchone()
    outreach_rows = conn.execute(
        """
        SELECT
            COUNT(*) FILTER (
                WHERE COALESCE(message_variant_fingerprint, '') = 'internal-reply-workflow-test'
                   OR LOWER(COALESCE(subject, '')) LIKE '%smtp test%'
            ) AS test_outreach,
            COUNT(*) FILTER (
                WHERE NOT (
                    COALESCE(message_variant_fingerprint, '') = 'internal-reply-workflow-test'
                    OR LOWER(COALESCE(subject, '')) LIKE '%smtp test%'
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


def deterministic_test_lead_flow(lead_id: int = 302, recipient: str = "", clear_history: bool = True, send: bool = False) -> str:
    from outreach.email_sender import pick_account, send_email
    from outreach.generator import generate_email
    from outreach.runtime import assert_outbound_allowed
    from storage.db import connect, init_outreach_tables, log_outreach, mark_sent

    conn = _conn()
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

    draft = generate_email(dict(lead))
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

    sender_name = ""
    sender_address = ""
    action = "drafted"
    if send:
        assert_outbound_allowed("deterministic_test_lead_flow")
        account = pick_account(conn)
        if account is None:
            raise RuntimeError("No outbound email account available for test send.")
        send_email(to_address, draft["subject"], draft["body"], account)
        mark_sent(conn, outreach_id, account.get("name", ""), account.get("address", ""))
        sender_name = account.get("name", "")
        sender_address = account.get("address", "")
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
        f"Sender: {sender_name or '(draft only)'} <{sender_address or ''}>",
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
    name = (account.get("name") or "").strip()
    if name:
        parts = [part for part in name.split() if part]
        if len(parts) >= 2:
            signer = f"{parts[0]} {parts[-1][0]}."
        else:
            signer = parts[0]
    else:
        signer = "Team"
    lang = (language or "en").lower()
    if lang.startswith("sk"):
        options = ["Dajte vedieť", "Vďaka", "Budem rád za odpoveď", "Ďakujem"]
    elif lang.startswith("cs"):
        options = ["Dejte vědět", "Díky", "Budu rád za odpověď", "Děkuji"]
    elif lang.startswith("de"):
        options = ["Geben Sie gern kurz Bescheid", "Danke", "Ich bin gespannt auf Ihre Rückmeldung", "Viele Grüße"]
    else:
        options = ["Cheers", "Thanks", "Let me know", "Curious either way"]
    closing = options[sum(ord(ch) for ch in ((account.get("address") or "") + lang)) % len(options)]
    return f"{closing},\n{signer}"


def _render_final_body(body: str, account: dict, language: str = "en") -> str:
    clean = (body or "").rstrip()
    return f"{clean}\n\n{_signature_block(account, language)}"


def preview_drafts(n: int = 3) -> str:
    script = f"""
from storage.db import connect, init_outreach_tables, get_pending_drafts, get_approved_drafts, get_scheduled_drafts
from email_sender import pick_account

def infer_language(row):
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

def signature_block(account, language):
    name = (account.get("name") or "").strip()
    if name:
        parts = [part for part in name.split() if part]
        signer = f"{{parts[0]}} {{parts[-1][0]}}." if len(parts) >= 2 else parts[0]
    else:
        signer = "Team"
    lang = (language or "en").lower()
    if lang.startswith("sk"):
        options = ["Dajte vedieť", "Vďaka", "Budem rád za odpoveď", "Ďakujem"]
    elif lang.startswith("cs"):
        options = ["Dejte vědět", "Díky", "Budu rád za odpověď", "Děkuji"]
    elif lang.startswith("de"):
        options = ["Geben Sie gern kurz Bescheid", "Danke", "Ich bin gespannt auf Ihre Rückmeldung", "Viele Grüße"]
    else:
        options = ["Cheers", "Thanks", "Let me know", "Curious either way"]
    closing = options[sum(ord(ch) for ch in ((account.get("address") or "") + lang)) % len(options)]
    return f"{{closing}},\\n{{signer}}"

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
    final_body = (row["message"] or "").rstrip() + "\\n\\n" + signature_block(acc, lang)
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
        timeout=300,
    )
    output = (result.stdout or "").strip()
    error = (result.stderr or "").strip()
    if result.returncode != 0:
        raise RuntimeError(error or output or f"Outreach command failed with exit {result.returncode}")
    return output or "(no output)"


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
        result = generate_email(lead)
        draft_id = log_outreach(conn, lead["id"], "email", to_addr, result["body"], result["subject"], status="pending", message_variant_fingerprint=result.get("fingerprint", ""))
        created.append(f"- [{{draft_id}}] {{lead['name']}} ({{lead.get('target_niche') or 'unknown'}}) -> {{to_addr}} | {{result['subject']}}")
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
from storage.db import (
    connect,
    init_outreach_tables,
    get_pending_drafts,
    create_review_batch,
    mark_review_batch_sent,
)
from email_sender import _load_accounts, _last_sent_at, _sent_today, send_email

def infer_language(row):
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

def signature_block(account, language):
    name = (account.get("name") or "").strip()
    if name:
        parts = [part for part in name.split() if part]
        signer = f"{{parts[0]}} {{parts[-1][0]}}." if len(parts) >= 2 else parts[0]
    else:
        signer = "Team"
    lang = (language or "en").lower()
    if lang.startswith("sk"):
        options = ["Dajte vedieť", "Vďaka", "Budem rád za odpoveď", "Ďakujem"]
    elif lang.startswith("cs"):
        options = ["Dejte vědět", "Díky", "Budu rád za odpověď", "Děkuji"]
    elif lang.startswith("de"):
        options = ["Geben Sie gern kurz Bescheid", "Danke", "Ich bin gespannt auf Ihre Rückmeldung", "Viele Grüße"]
    else:
        options = ["Cheers", "Thanks", "Let me know", "Curious either way"]
    closing = options[sum(ord(ch) for ch in ((account.get("address") or "") + lang)) % len(options)]
    return f"{{closing}},\\n{{signer}}"

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
    final_body = (row["message"] or "").rstrip() + "\\n\\n" + signature_block(account, lang)
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
send_email({recipient!r}, subject, body, review_sender)
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
        send_after = next_send_after(conn, acc["address"], jitter_seed=seed).isoformat()
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
from storage.db import connect, init_outreach_tables, get_due_scheduled_drafts, mark_failed, mark_sent
from email_sender import send_email

def infer_language(row):
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

def signature_block(account, language):
    name = (account.get("name") or "").strip()
    if name:
        parts = [part for part in name.split() if part]
        signer = f"{{parts[0]}} {{parts[-1][0]}}." if len(parts) >= 2 else parts[0]
    else:
        signer = "Team"
    lang = (language or "en").lower()
    if lang.startswith("sk"):
        options = ["Dajte vedieť", "Vďaka", "Budem rád za odpoveď", "Ďakujem"]
    elif lang.startswith("cs"):
        options = ["Dejte vědět", "Díky", "Budu rád za odpověď", "Děkuji"]
    elif lang.startswith("de"):
        options = ["Geben Sie gern kurz Bescheid", "Danke", "Ich bin gespannt auf Ihre Rückmeldung", "Viele Grüße"]
    else:
        options = ["Cheers", "Thanks", "Let me know", "Curious either way"]
    closing = options[sum(ord(ch) for ch in ((account.get("address") or "") + lang)) % len(options)]
    return f"{{closing}},\\n{{signer}}"

conn = connect()
init_outreach_tables(conn)
rows = get_due_scheduled_drafts(conn, datetime.now(timezone.utc).isoformat(), limit={int(limit)})
if not rows:
    print("No scheduled drafts are due right now.")
    raise SystemExit(0)

sent = []
failed = []
for row in rows:
    acc = {{"name": row["sender_name"], "address": row["sender_address"], "password": "", "daily_limit": 0}}
    try:
        from email_sender import _load_accounts
        for known in _load_accounts():
            if known["address"] == row["sender_address"]:
                acc = known
                break
        final_body = (row["message"] or "").rstrip() + "\\n\\n" + signature_block(acc, infer_language(row))
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
