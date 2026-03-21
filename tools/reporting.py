"""
agent/tools/reporting.py
Unified operator-facing reporting across leads, research, outreach, and content.
"""
from __future__ import annotations

from . import leads, outreach, research, content


import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "telegram"))
from formatting import fmt_report
from outreach.runtime import safe_mode_enabled
from outreach.storage import db as outreach_db
from leadgen.storage import db as leadgen_db
from content.store import load_items

def operator_summary() -> str:
    conn_leads = leadgen_db.connect()
    count_total = conn_leads.execute("SELECT COUNT(*) as n FROM businesses").fetchone()["n"]
    count_qual = conn_leads.execute("SELECT COUNT(*) as n FROM businesses WHERE validation_status='qualified'").fetchone()["n"]
    
    conn_outreach = outreach_db.connect()
    outreach_db.init_outreach_tables(conn_outreach)
    ostats = outreach_db.get_stats(conn_outreach)
    
    content_q = load_items()
    content_approved = len([i for i in content_q if i.status == "approved"])
    content_planned = len([i for i in content_q if i.status in ("idea", "draft")])
    
    data = {"safe_mode": safe_mode_enabled()}
    data["prod_sent"] = ostats.get("sent", 0)
    data["prod_scheduled"] = ostats.get("scheduled", 0)
    data["prod_pending"] = ostats.get("pending", 0)
    data["prod_replies"] = ostats.get("replies", 0)
    
    data["leads_total"] = count_total
    data["leads_qualified"] = count_qual
    data["content_approved"] = content_approved
    data["content_planned"] = content_planned
    
    req_replies = outreach_db.get_reply_queue_needing_action(conn_outreach, 1)
    data["reply_queue_empty"] = len(req_replies) == 0
    
    open_reviews = outreach_db.get_open_review_batches(conn_outreach, 10)
    if open_reviews:
        data["review_gate_status"] = f"{len(open_reviews)} batches pending"
        
    utils = []
    for r in outreach_db.sender_utilization(conn_outreach):
        utils.append((r["sender_address"], r["sent_count"]))
    data["sender_utilization"] = utils
    
    return fmt_report(data)
