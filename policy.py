"""
agent/policy.py
Action policy for autonomous task execution.
"""
from __future__ import annotations

INTERNAL_SAFE = "internal_safe"
APPROVAL_REQUIRED = "approval_required"
APPROVED_EXECUTION = "approved_execution"
BLOCKED_WITHOUT_HUMAN = "blocked_without_human"

ACTION_POLICIES = {
    "web_search": INTERNAL_SAFE,
    "read_file": INTERNAL_SAFE,
    "write_file": INTERNAL_SAFE,
    "run_shell": INTERNAL_SAFE,
    "list_dir": INTERNAL_SAFE,
    "code_search": INTERNAL_SAFE,
    "run_tests": INTERNAL_SAFE,
    "leads_stats": INTERNAL_SAFE,
    "leads_top": INTERNAL_SAFE,
    "leads_search": INTERNAL_SAFE,
    "research_niche_overview": INTERNAL_SAFE,
    "research_findings_rollup": INTERNAL_SAFE,
    "research_findings_summary": INTERNAL_SAFE,
    "research_shortlist_status": INTERNAL_SAFE,
    "research_set_shortlist": INTERNAL_SAFE,
    "research_update_niche": INTERNAL_SAFE,
    "research_ingest_finding": INTERNAL_SAFE,
    "research_lead_review_queue": INTERNAL_SAFE,
    "research_candidate_queue": INTERNAL_SAFE,
    "research_save_lead_review": INTERNAL_SAFE,
    "outreach_stats": INTERNAL_SAFE,
    "outreach_replies": INTERNAL_SAFE,
    "outreach_reply_queue": INTERNAL_SAFE,
    "outreach_prepare_reply_drafts": INTERNAL_SAFE,
    "outreach_preview_reply_drafts": INTERNAL_SAFE,
    "outreach_internal_reply_test": APPROVAL_REQUIRED,
    "outreach_internal_reply_test_status": INTERNAL_SAFE,
    "outreach_generate_drafts": INTERNAL_SAFE,
    "outreach_approve_drafts": APPROVAL_REQUIRED,
    "outreach_schedule_approved": APPROVED_EXECUTION,
    "outreach_process_send_queue": APPROVED_EXECUTION,
    "outreach_send_reply_drafts": APPROVED_EXECUTION,
    "outreach_send_queue_status": INTERNAL_SAFE,
    "outreach_pending_drafts": INTERNAL_SAFE,
    "outreach_preview_drafts": INTERNAL_SAFE,
    "outreach_poll_and_classify_replies": INTERNAL_SAFE,
    "content_report": INTERNAL_SAFE,
    "content_plan_posts": INTERNAL_SAFE,
    "content_prompt_manifests": INTERNAL_SAFE,
    "content_generate_images": INTERNAL_SAFE,
    "content_provider_status": INTERNAL_SAFE,
    "content_approve_post": APPROVAL_REQUIRED,
    "content_reject_post": APPROVAL_REQUIRED,
    "content_prepare_publish": APPROVED_EXECUTION,
    "content_publish_post": APPROVED_EXECUTION,
    "content_engagement_plan": INTERNAL_SAFE,
    "git_status": INTERNAL_SAFE,
    "git_commit_push": APPROVAL_REQUIRED,
    "memory_set": INTERNAL_SAFE,
    "memory_get": INTERNAL_SAFE,
    "ops_jobs_summary": INTERNAL_SAFE,
    "ops_jobs_list": INTERNAL_SAFE,
    "ops_recent_events": INTERNAL_SAFE,
}


def policy_for(action: str) -> str:
    return ACTION_POLICIES.get(action, APPROVAL_REQUIRED)


def is_allowed(action: str, allowed_actions: list[str] | None, approval_mode: str = "internal_only") -> tuple[bool, str | None]:
    policy = policy_for(action)
    allowed = set(allowed_actions or [])

    if policy == INTERNAL_SAFE:
        return True, None

    if approval_mode == "broad" and action in allowed:
        return True, None

    if policy == APPROVED_EXECUTION and ("approved_execution" in allowed or action in allowed):
        return True, None

    if policy == APPROVAL_REQUIRED and ("approval_required" in allowed or action in allowed):
        return True, None

    if policy == BLOCKED_WITHOUT_HUMAN:
        return False, f"Tool {action} is blocked without live human intervention."

    return False, (
        f"Tool {action} requires approval. Ask for confirmation or mark the task as NEEDS_INPUT "
        f"before using it."
    )
