"""
agent/tools/reporting.py
Unified operator-facing reporting across leads, research, outreach, and content.
"""
from __future__ import annotations

from . import leads, outreach, research, content


def operator_summary() -> str:
    sections = [
        "BIZ operator summary",
        "",
        outreach.safe_mode_status(),
        "",
        outreach.production_test_summary(),
        "",
        "Leads",
        leads.stats(),
        "",
        "Research",
        research.shortlist_status(),
        "",
        "Strategy",
        research.strategy_report(),
        "",
        "Outreach",
        outreach.stats(),
        "",
        "Reply queue",
        outreach.reply_ops(10),
        "",
        "Review gate",
        outreach.review_batch_status(10),
        "",
        "Draft queue",
        outreach.pending_drafts(10),
        "",
        "Send queue",
        outreach.send_queue_status(10),
        "",
        "Content",
        content.report(),
    ]
    return "\n".join(str(part) for part in sections if part is not None)
