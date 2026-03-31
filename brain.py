"""
agent/brain.py
LLM core. Injects full biz context into every call.
Supports tool use via a simple dispatch loop.
"""
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

BIZ_ROOT_PATH = Path(__file__).parent.parent
load_dotenv(BIZ_ROOT_PATH / ".env", override=True)

# Ensure agent/ and biz root are both on sys.path so tools can import
# outreach.runtime, outreach.senders, leadgen.*, etc. regardless of cwd.
_agent_dir = str(Path(__file__).parent)
_biz_root = str(BIZ_ROOT_PATH)
if _agent_dir not in sys.path:
    sys.path.insert(0, _agent_dir)
if _biz_root not in sys.path:
    sys.path.insert(0, _biz_root)

try:
    from . import memory, executor, policy
    from .remote_models import create_chat_completion
    from .tools import web, git, leads, outreach, ops, research, content, reporting
except ImportError:
    import memory
    import executor
    import policy
    from remote_models import create_chat_completion
    from tools import web, git, leads, outreach, ops, research, content, reporting

BIZ_ROOT = BIZ_ROOT_PATH
PROJECT_MD = (BIZ_ROOT / "PROJECT.md").read_text()

# Compact summary injected into every prompt — full file available via read_file tool
_PROJECT_SUMMARY = "\n".join(
    line for line in PROJECT_MD.splitlines()
    if line.strip() and not line.startswith("```") and len(line) < 200
)[:2000]  # cap at 2000 chars

def _chat(messages: list, tools: list = None, max_tokens: int = 2000) -> object:
    """Try shared remote providers and keep the core agent off local inference."""
    return create_chat_completion(
        messages=messages,
        tools=tools,
        temperature=0.3,
        max_tokens=max_tokens,
    )


# ── Tool definitions ──────────────────────────────────────────────────────────

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "web_search",
            "description": "Search DuckDuckGo for current information.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "max_results": {"type": "integer", "default": 5},
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "code_search",
            "description": "Search code and files using ripgrep.",
            "parameters": {
                "type": "object",
                "properties": {
                    "pattern": {"type": "string"},
                    "path": {"type": "string", "default": "."},
                },
                "required": ["pattern"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_tests",
            "description": "Run tests or verification commands in a repo subdirectory.",
            "parameters": {
                "type": "object",
                "properties": {
                    "cmd": {"type": "string"},
                    "cwd": {"type": "string"},
                },
                "required": ["cmd"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_file",
            "description": "Read a file within the biz/ project. Path must be within allowlist.",
            "parameters": {
                "type": "object",
                "properties": {"path": {"type": "string"}},
                "required": ["path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "write_file",
            "description": "Write content to a file within the biz/ project.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "content": {"type": "string"},
                },
                "required": ["path", "content"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_shell",
            "description": "Run a shell command within the biz/ project directory.",
            "parameters": {
                "type": "object",
                "properties": {
                    "cmd": {"type": "string"},
                    "cwd": {"type": "string", "description": "Working directory (optional)"},
                },
                "required": ["cmd"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_dir",
            "description": "List files in a directory within the biz/ project.",
            "parameters": {
                "type": "object",
                "properties": {"path": {"type": "string"}},
                "required": ["path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "leads_stats",
            "description": "Get current leadgen pipeline statistics.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "leads_top",
            "description": "Get top qualified leads.",
            "parameters": {
                "type": "object",
                "properties": {"n": {"type": "integer", "default": 10}},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "leads_search",
            "description": "Search leads by name, category, address, or email. Returns id, name, niche, status, score, email, and outreach angle.",
            "parameters": {
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "leads_get",
            "description": "Get full profile for a single lead by numeric ID, including email, outreach angle, and full outreach history.",
            "parameters": {
                "type": "object",
                "properties": {"lead_id": {"type": "integer"}},
                "required": ["lead_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "leads_update_email",
            "description": "Update the email address for a lead by ID.",
            "parameters": {
                "type": "object",
                "properties": {
                    "lead_id": {"type": "integer"},
                    "email": {"type": "string"},
                },
                "required": ["lead_id", "email"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "leads_update_niche",
            "description": "Update the target_niche for a lead by ID.",
            "parameters": {
                "type": "object",
                "properties": {
                    "lead_id": {"type": "integer"},
                    "niche": {"type": "string"},
                },
                "required": ["lead_id", "niche"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "leads_update_field",
            "description": "Update any single field on a lead (email, phone, website, address, contact_name, outreach_angle, pain_point_guess, top_gap, target_niche, validation_status, pipeline_stage, brand_summary, apparent_size, digital_maturity, score).",
            "parameters": {
                "type": "object",
                "properties": {
                    "lead_id": {"type": "integer"},
                    "field": {"type": "string", "description": "Field name to update"},
                    "value": {"type": "string", "description": "New value"},
                },
                "required": ["lead_id", "field", "value"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "leads_delete",
            "description": "Permanently delete a lead and all associated data (website_data, enrichment, outreach history, replies). Irreversible.",
            "parameters": {
                "type": "object",
                "properties": {"lead_id": {"type": "integer"}},
                "required": ["lead_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "leads_add",
            "description": "Manually add a lead to the DB. Use for leads found outside the scraper pipeline.",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "email": {"type": "string"},
                    "category": {"type": "string"},
                    "address": {"type": "string"},
                    "website": {"type": "string"},
                    "phone": {"type": "string"},
                    "outreach_angle": {"type": "string"},
                    "target_niche": {"type": "string"},
                    "contact_name": {"type": "string"},
                },
                "required": ["name", "email"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "leads_set_status",
            "description": "Set validation_status for a lead. Values: qualified, skip, needs_review, disqualified, pending.",
            "parameters": {
                "type": "object",
                "properties": {
                    "lead_id": {"type": "integer"},
                    "status": {"type": "string"},
                },
                "required": ["lead_id", "status"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "leads_clear_outreach",
            "description": "Delete all outreach history for a lead (drafts, replies, log). Resets pipeline_stage to lead.",
            "parameters": {
                "type": "object",
                "properties": {"lead_id": {"type": "integer"}},
                "required": ["lead_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "leads_run_enrichment",
            "description": "Trigger contact+pain enrichment on qualified/needs_review leads missing outreach_angle.",
            "parameters": {
                "type": "object",
                "properties": {"limit": {"type": "integer", "description": "Max leads to enrich (default 100)"}},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "leads_scrape",
            "description": "Trigger a lead scrape run. source: web_search | hipages | google_maps.",
            "parameters": {
                "type": "object",
                "properties": {
                    "source": {"type": "string", "description": "web_search | hipages | google_maps"},
                    "query": {"type": "string", "description": "Search query (e.g. 'plumber sydney')"},
                    "trade": {"type": "string", "description": "Trade for hipages (plumber, electrician...)"},
                    "location": {"type": "string", "description": "City (sydney, melbourne...)"},
                    "max_results": {"type": "integer"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "outreach_quality_check_send",
            "description": "Send a preview copy of the listed outreach drafts to a test email address for quality assurance.",
            "parameters": {
                "type": "object",
                "properties": {
                    "draft_ids": {
                        "type": "array",
                        "items": {"type": "integer"},
                        "description": "List of draft IDs to preview"
                    },
                    "target_email": {
                        "type": "string",
                        "description": "Email address to send the preview to"
                    }
                },
                "required": ["draft_ids", "target_email"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "outreach_delete_draft",
            "description": "Delete a pending or approved outreach draft by ID.",
            "parameters": {
                "type": "object",
                "properties": {"outreach_id": {"type": "integer"}},
                "required": ["outreach_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "outreach_update_draft",
            "description": "Edit the body and/or subject of a pending outreach draft before it's sent.",
            "parameters": {
                "type": "object",
                "properties": {
                    "outreach_id": {"type": "integer"},
                    "body": {"type": "string"},
                    "subject": {"type": "string"},
                },
                "required": ["outreach_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "outreach_dismiss_reply",
            "description": "Mark a reply as notified and skip drafting — removes it from the action queue.",
            "parameters": {
                "type": "object",
                "properties": {"reply_id": {"type": "integer"}},
                "required": ["reply_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "outreach_update_reply_draft",
            "description": "Edit the body and/or subject of a reply draft before sending.",
            "parameters": {
                "type": "object",
                "properties": {
                    "reply_id": {"type": "integer"},
                    "body": {"type": "string"},
                    "subject": {"type": "string"},
                },
                "required": ["reply_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "research_niche_overview",
            "description": "Show ranked niche research summary with repo evidence and validation metrics.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "research_findings_rollup",
            "description": "Show how many structured market findings are stored per niche.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "research_findings_summary",
            "description": "List recent structured market findings, optionally filtered by niche.",
            "parameters": {
                "type": "object",
                "properties": {
                    "niche": {"type": "string"},
                    "limit": {"type": "integer", "default": 10},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "research_strategy_report",
            "description": "Show strategy-level summary of pains, offers, channels, and monetization paths by niche.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "research_pain_library",
            "description": "Show structured niche pain-library entries, optionally filtered by niche.",
            "parameters": {
                "type": "object",
                "properties": {
                    "niche": {"type": "string"},
                    "limit": {"type": "integer", "default": 20},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "research_upsert_pain",
            "description": "Create or update a reusable pain-library entry for a niche.",
            "parameters": {
                "type": "object",
                "properties": {
                    "niche": {"type": "string"},
                    "pain_key": {"type": "string"},
                    "pain_label": {"type": "string"},
                    "description": {"type": "string"},
                    "evidence_summary": {"type": "string"},
                    "evidence_types": {"type": "array", "items": {"type": "string"}},
                    "safe_outreach_claim": {"type": "string"},
                    "unsafe_outreach_claim": {"type": "string"},
                    "offer_angles": {"type": "array", "items": {"type": "string"}},
                    "best_channels": {"type": "array", "items": {"type": "string"}},
                    "confidence": {"type": "number"},
                },
                "required": ["niche", "pain_key", "pain_label"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "research_niche_report",
            "description": "Show one niche report combining validation, pain library, and recent findings.",
            "parameters": {
                "type": "object",
                "properties": {"niche": {"type": "string"}},
                "required": ["niche"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "research_shortlist_status",
            "description": "Show the currently shortlisted niches.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "research_set_shortlist",
            "description": "Set the active shortlist of canonical niche keys.",
            "parameters": {
                "type": "object",
                "properties": {
                    "niches": {
                        "type": "array",
                        "items": {"type": "string"},
                    }
                },
                "required": ["niches"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "research_update_niche",
            "description": "Update niche research notes, evidence, scores, channels, or status for a canonical niche key.",
            "parameters": {
                "type": "object",
                "properties": {
                    "niche": {"type": "string"},
                    "notes": {"type": "string"},
                    "external_evidence": {"type": "string"},
                    "sample_market": {"type": "string"},
                    "common_pains": {"type": "array", "items": {"type": "string"}},
                    "outreach_channel_fit": {"type": "array", "items": {"type": "string"}},
                    "pain_detectability": {"type": "number"},
                    "contactability": {"type": "number"},
                    "ability_to_deliver": {"type": "number"},
                    "price_tolerance": {"type": "number"},
                    "content_leverage": {"type": "number"},
                    "status": {"type": "string"},
                },
                "required": ["niche"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "research_ingest_finding",
            "description": "Store a structured market research finding for a niche from Reddit, X, LinkedIn, forums, or other sources.",
            "parameters": {
                "type": "object",
                "properties": {
                    "niche": {"type": "string"},
                    "source_type": {"type": "string"},
                    "summary": {"type": "string"},
                    "source_query": {"type": "string"},
                    "source_title": {"type": "string"},
                    "source_url": {"type": "string"},
                    "market": {"type": "string"},
                    "pain_point": {"type": "string"},
                    "opportunity_type": {"type": "string"},
                    "suggested_offer": {"type": "string"},
                    "suggested_channel": {"type": "string"},
                    "monetization_path": {"type": "string"},
                    "evidence_strength": {"type": "number"},
                    "confidence": {"type": "number"},
                    "tags": {"type": "array", "items": {"type": "string"}},
                    "created_by": {"type": "string"},
                },
                "required": ["niche", "source_type", "summary"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "research_lead_review_queue",
            "description": "List sent/skipped/failed leads that still need manual review.",
            "parameters": {
                "type": "object",
                "properties": {"n": {"type": "integer", "default": 10}},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "research_candidate_queue",
            "description": "List outreach-ready qualified leads with available email/social channels, filtered by shortlist if present.",
            "parameters": {
                "type": "object",
                "properties": {"n": {"type": "integer", "default": 20}},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "research_save_lead_review",
            "description": "Save a manual lead review with the recommended outreach channel and angle.",
            "parameters": {
                "type": "object",
                "properties": {
                    "lead_id": {"type": "integer"},
                    "recommended_channel": {"type": "string"},
                    "recommended_angle": {"type": "string"},
                    "notes": {"type": "string"},
                    "actual_business_model": {"type": "string"},
                    "actual_pains": {"type": "string"},
                    "email_fit": {"type": "string"},
                    "social_fit": {"type": "string"},
                    "form_fit": {"type": "string"},
                    "hybrid_fit": {"type": "string"},
                },
                "required": ["lead_id", "recommended_channel", "recommended_angle"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "reporting_operator_summary",
            "description": "Show the unified operator summary across leads, research, outreach review, queue state, and content.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "outreach_stats",
            "description": "Get outreach pipeline statistics (sent, replies, etc.).",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "outreach_replies",
            "description": "Get recent replies from leads.",
            "parameters": {
                "type": "object",
                "properties": {"n": {"type": "integer", "default": 5}},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "outreach_reply_queue",
            "description": "Show replies that need operator action, ordered by urgency.",
            "parameters": {
                "type": "object",
                "properties": {"n": {"type": "integer", "default": 10}},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "outreach_prepare_reply_drafts",
            "description": "Prepare suggested reply drafts for recent inbound replies.",
            "parameters": {
                "type": "object",
                "properties": {"limit": {"type": "integer", "default": 10}},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "outreach_preview_reply_drafts",
            "description": "Preview drafted reply emails for inbound replies.",
            "parameters": {
                "type": "object",
                "properties": {"limit": {"type": "integer", "default": 5}},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "outreach_send_reply_drafts",
            "description": "Send drafted reply emails back to leads using the original sender identity.",
            "parameters": {
                "type": "object",
                "properties": {"limit": {"type": "integer", "default": 5}},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "outreach_internal_reply_test",
            "description": "Send a controlled internal email between configured mailboxes for reply-workflow testing.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "outreach_internal_reply_test_status",
            "description": "Poll and preview only the internal reply-workflow test messages and drafted responses.",
            "parameters": {
                "type": "object",
                "properties": {"limit": {"type": "integer", "default": 5}},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "outreach_internal_matrix_test",
            "description": (
                "Create/update internal test leads for all configured sender mailboxes, assign niches, "
                "and send a deterministic internal matrix where each mailbox emails the next one."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "send": {"type": "boolean", "default": True},
                    "clear_history": {"type": "boolean", "default": True},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "outreach_generate_drafts",
            "description": (
                "Generate and queue outreach drafts for qualified leads without outreach. "
                "Returns 0 drafts if: the lead already has a pending/approved/sent draft, "
                "the lead has no target_niche set, or no leads match the niche filter. "
                "If you get 0 drafts for a specific lead, check outreach_pending_drafts — "
                "the draft likely already exists. Do NOT retry generate_drafts for the same lead."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "default": 5},
                    "target_niche": {"type": "string"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "outreach_send_review_batch",
            "description": "Send pending draft previews to an internal review email and lock them until a reply arrives.",
            "parameters": {
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "default": 5},
                    "recipient": {"type": "string", "default": "egorbrusnyak@gmail.com"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "outreach_review_batch_status",
            "description": "Show open internal review batches waiting on reply or approval.",
            "parameters": {
                "type": "object",
                "properties": {"limit": {"type": "integer", "default": 10}},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "outreach_poll_review_gate",
            "description": "Poll internal review replies and convert approved review batches into approved drafts.",
            "parameters": {
                "type": "object",
                "properties": {"limit": {"type": "integer", "default": 10}},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "outreach_approve_drafts",
            "description": "Approve pending outreach drafts so they become sendable.",
            "parameters": {
                "type": "object",
                "properties": {"limit": {"type": "integer", "default": 5}},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "outreach_schedule_approved",
            "description": "Schedule approved outreach drafts into the paced send queue.",
            "parameters": {
                "type": "object",
                "properties": {"limit": {"type": "integer", "default": 5}},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "outreach_process_send_queue",
            "description": "Process scheduled outreach drafts that are due right now.",
            "parameters": {
                "type": "object",
                "properties": {"limit": {"type": "integer", "default": 5}},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "outreach_send_queue_status",
            "description": "Show the scheduled outreach queue and sender utilization.",
            "parameters": {
                "type": "object",
                "properties": {"limit": {"type": "integer", "default": 10}},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "outreach_pending_drafts",
            "description": "List pending outreach drafts waiting for review.",
            "parameters": {
                "type": "object",
                "properties": {"n": {"type": "integer", "default": 10}},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "outreach_preview_drafts",
            "description": "Preview pending outreach drafts as they would appear when sent, including signature.",
            "parameters": {
                "type": "object",
                "properties": {"n": {"type": "integer", "default": 3}},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "outreach_poll_and_classify_replies",
            "description": "Poll inbox replies and classify them through the outreach pipeline.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "outreach_test_send_lead",
            "description": (
                "Generate and immediately send a test outreach email for a specific lead. "
                "USE THIS when operator says: 'send to my email', 'test email', 'draft and send', 'demo send'. "
                "Bypasses the scheduled queue — one step: generate + send. "
                "Set recipient to override the lead's own email (e.g. operator's own address for testing). "
                "Set sender_address to pin a specific sender account (e.g. 'victor.brusnyak@gmail.com'). "
                "Requires operator approval before the email goes out. "
                "Do NOT use outreach_generate_drafts + outreach_pending_drafts for test sends — use this instead."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "lead_id": {"type": "integer", "description": "Numeric lead ID from leads_search or leads_get"},
                    "recipient": {"type": "string", "description": "Override recipient email. Leave empty to use the lead's own email."},
                    "sender_address": {"type": "string", "description": "Pin a specific sender account by email address (e.g. 'victor.brusnyak@gmail.com'). Leave empty for automatic least-used selection."},
                    "clear_history": {"type": "boolean", "default": True, "description": "Clear previous outreach for this lead before sending (recommended for test runs)"},
                },
                "required": ["lead_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "outreach_draft_for_lead",
            "description": (
                "Generate a draft for a specific lead with explicit sender control. "
                "Use when operator says 'draft using victor.brusnyak' or 'use this specific sender'. "
                "Shows full preview including sign-off. Does NOT send — operator approves separately. "
                "Use outreach_test_send_lead if you want to generate AND send in one step."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "lead_id": {"type": "integer", "description": "Numeric lead ID"},
                    "sender_address": {"type": "string", "description": "Sender account email (e.g. 'victor.brusnyak@gmail.com'). Leave empty for auto-selection."},
                    "recipient": {"type": "string", "description": "Override recipient email. Leave empty to use lead's own email."},
                },
                "required": ["lead_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "outreach_poll_replies_for_lead",
            "description": "Poll IMAP for new replies then return any replies stored for a specific lead. Use after outreach_test_send_lead to check if a reply has arrived.",
            "parameters": {
                "type": "object",
                "properties": {
                    "lead_id": {"type": "integer", "description": "Numeric lead ID"},
                },
                "required": ["lead_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "content_report",
            "description": "Show Victor content queue and planning summary.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "content_plan_posts",
            "description": "Generate Victor post ideas, optionally queueing them.",
            "parameters": {
                "type": "object",
                "properties": {
                    "count": {"type": "integer", "default": 5},
                    "queue": {"type": "boolean", "default": False},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "content_prompt_manifests",
            "description": "Write image prompt manifests for queued Victor posts.",
            "parameters": {
                "type": "object",
                "properties": {"item_id": {"type": "string"}},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "content_generate_images",
            "description": "Generate Victor images for a queued content item using the image model.",
            "parameters": {
                "type": "object",
                "properties": {
                    "item_id": {"type": "string"},
                    "sample_count": {"type": "integer", "default": 2},
                    "aspect_ratio": {"type": "string", "default": "3:4"},
                },
                "required": ["item_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "content_provider_status",
            "description": "Show media provider status for image/video generation.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "content_approve_post",
            "description": "Approve a generated Victor content item.",
            "parameters": {
                "type": "object",
                "properties": {"item_id": {"type": "string"}},
                "required": ["item_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "content_prepare_publish",
            "description": "Prepare an approved Victor post for publishing after approval.",
            "parameters": {
                "type": "object",
                "properties": {
                    "item_id": {"type": "string"},
                    "publish_after": {"type": "string"},
                },
                "required": ["item_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "content_publish_post",
            "description": "Mark a Victor post published after the final posting step.",
            "parameters": {
                "type": "object",
                "properties": {"item_id": {"type": "string"}},
                "required": ["item_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "content_reject_post",
            "description": "Reject a Victor content item.",
            "parameters": {
                "type": "object",
                "properties": {"item_id": {"type": "string"}},
                "required": ["item_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "content_engagement_plan",
            "description": "Create a daily Victor engagement checklist for target niches.",
            "parameters": {
                "type": "object",
                "properties": {
                    "niches": {"type": "array", "items": {"type": "string"}},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "content_engagement_log",
            "description": "Show recent Victor Instagram engagement session history (likes, comments, follows, profiles discovered).",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "content_run_engagement",
            "description": "Run a Playwright Instagram engagement session for Victor. Likes posts, leaves comments, follows accounts, and discovers reference profiles. Requires human approval before running a real session.",
            "parameters": {
                "type": "object",
                "properties": {
                    "niches": {"type": "array", "items": {"type": "string"}, "description": "Target niche keys e.g. dental, real_estate, beauty"},
                    "dry_run": {"type": "boolean", "default": True, "description": "If true, browse and log but don't actually like/comment/follow"},
                    "discover_only": {"type": "boolean", "default": False, "description": "If true, only collect profile handles without any engagement"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "git_status",
            "description": "Get git status of a project repo.",
            "parameters": {
                "type": "object",
                "properties": {"repo_path": {"type": "string"}},
                "required": ["repo_path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "git_commit_push",
            "description": "Stage all changes, commit, and push a project repo.",
            "parameters": {
                "type": "object",
                "properties": {
                    "repo_path": {"type": "string"},
                    "message": {"type": "string"},
                },
                "required": ["repo_path", "message"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "memory_set",
            "description": "Store a fact in persistent memory.",
            "parameters": {
                "type": "object",
                "properties": {
                    "key": {"type": "string"},
                    "value": {"type": "string"},
                },
                "required": ["key", "value"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "memory_get",
            "description": "Retrieve all stored facts from memory.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "memory_learn",
            "description": "Remember a plain-English instruction or preference from the operator. Use when the user says 'remember', 'always', 'never', 'from now on', or gives a standing instruction.",
            "parameters": {
                "type": "object",
                "properties": {
                    "instruction": {"type": "string", "description": "The instruction or preference to remember"},
                    "key": {"type": "string", "description": "Short snake_case key (auto-generated if omitted)"},
                },
                "required": ["instruction"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "ig_browse_profiles",
            "description": "Visit Instagram profiles and collect recent post URLs and captions. No engagement — pure discovery. Use before drafting comments.",
            "parameters": {
                "type": "object",
                "properties": {
                    "usernames": {"type": "array", "items": {"type": "string"}, "description": "IG usernames to visit"},
                    "posts_per_profile": {"type": "integer", "default": 3},
                },
                "required": ["usernames"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "ig_draft_comment",
            "description": "Use OpenRouter vision model to draft a comment for an Instagram post. Returns draft text for operator review.",
            "parameters": {
                "type": "object",
                "properties": {
                    "post_url": {"type": "string"},
                    "screenshot_path": {"type": "string", "description": "Path to screenshot of the post (from ig_browse_profiles)"},
                    "context_hint": {"type": "string", "description": "Optional context about the post or account"},
                },
                "required": ["post_url"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "ig_post_comment",
            "description": "Post an approved comment to an Instagram post. Only call after operator approval.",
            "parameters": {
                "type": "object",
                "properties": {
                    "post_url": {"type": "string"},
                    "comment_text": {"type": "string"},
                },
                "required": ["post_url", "comment_text"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "ig_follow",
            "description": "Follow an Instagram user.",
            "parameters": {
                "type": "object",
                "properties": {"username": {"type": "string"}},
                "required": ["username"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "ig_discover_profiles",
            "description": "Browse IG hashtag feeds and collect interesting profile handles + screenshots for operator review. No following — discovery only.",
            "parameters": {
                "type": "object",
                "properties": {
                    "hashtags": {"type": "array", "items": {"type": "string"}, "description": "Hashtags to browse (default: bratislava, viennagram, praguelife)"},
                    "max_profiles": {"type": "integer", "default": 8},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "ig_weekly_strategy",
            "description": "Generate a weekly Instagram growth strategy: post schedule, hashtags, engagement targets, systems post idea.",
            "parameters": {
                "type": "object",
                "properties": {
                    "target_niches": {"type": "array", "items": {"type": "string"}, "description": "e.g. dental, real_estate, beauty"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "ops_jobs_summary",
            "description": "Get a summary of queued jobs and next ready work.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "ops_jobs_list",
            "description": "List queued jobs with status, priority, attempts, and ownership.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "ops_task_create",
            "description": "Create a new durable background task or reminder. Use this to schedule work for yourself later.",
            "parameters": {
                "type": "object",
                "properties": {
                    "description": {"type": "string"},
                    "kind": {"type": "string", "description": "e.g., 'agent_queued', 'reminder'"},
                    "priority": {"type": "integer", "default": 50}
                },
                "required": ["description"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "ops_task_complete",
            "description": "Mark a task as complete with a result.",
            "parameters": {
                "type": "object",
                "properties": {
                    "task_id": {"type": "string"},
                    "result": {"type": "string"}
                },
                "required": ["task_id", "result"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "ops_recent_events",
            "description": "Get recent operational events from the persistent event log.",
            "parameters": {
                "type": "object",
                "properties": {"n": {"type": "integer", "default": 10}},
            },
        },
    },
]


_EXECUTION_CONTEXT = {"task_id": None, "allowed_actions": None, "approval_mode": "internal_only"}

# Pending confirmation state: {chat_id: {"messages": [...], "pending_tool": {...}}}
_PENDING_CONFIRMATIONS: dict = {}


def _format_confirm_prompt(tool_name: str, args: dict) -> str:
    """Format a human-readable confirmation request for an approval-required action."""
    arg_summary = ", ".join(f"{k}={v!r}" for k, v in args.items() if k not in ("content",))
    if "content" in args:
        arg_summary += f", content=({len(args['content'])} chars)"
    return (
        f"About to run: {tool_name}({arg_summary})\n\n"
        f"Reply /yes to proceed or /no to cancel."
    )


def _dispatch(name: str, args: dict) -> str:
    try:
        allowed, reason = policy.is_allowed(
            name,
            _EXECUTION_CONTEXT.get("allowed_actions"),
            _EXECUTION_CONTEXT.get("approval_mode", "internal_only"),
        )
        if not allowed and _EXECUTION_CONTEXT.get("task_id"):
            return f"APPROVAL_REQUIRED: {reason}"
        if name == "web_search":
            return web.search(args["query"], args.get("max_results", 5))
        elif name == "code_search":
            return executor.code_search(args["pattern"], args.get("path", "."))
        elif name == "run_tests":
            return executor.run_tests(args["cmd"], args.get("cwd"))
        elif name == "read_file":
            return executor.read_file(args["path"])
        elif name == "write_file":
            return executor.write_file(args["path"], args["content"])
        elif name == "run_shell":
            return executor.run_shell(args["cmd"], args.get("cwd"))
        elif name == "list_dir":
            return executor.list_dir(args["path"])
        elif name == "leads_stats":
            return leads.stats()
        elif name == "leads_top":
            return leads.top_qualified(args.get("n", 10))
        elif name == "leads_search":
            return leads.search_leads(args["query"])
        elif name == "leads_get":
            return leads.get_lead(args["lead_id"])
        elif name == "leads_update_email":
            return leads.update_lead_email(args["lead_id"], args["email"])
        elif name == "leads_update_niche":
            return leads.update_lead_niche(args["lead_id"], args["niche"])
        elif name == "leads_update_field":
            return leads.update_lead_field(args["lead_id"], args["field"], args["value"])
        elif name == "leads_delete":
            return leads.delete_lead(args["lead_id"])
        elif name == "leads_add":
            return leads.add_lead(**{k: v for k, v in args.items()})
        elif name == "leads_set_status":
            return leads.set_lead_status(args["lead_id"], args["status"])
        elif name == "leads_clear_outreach":
            return leads.clear_outreach_for_lead(args["lead_id"])
        elif name == "leads_run_enrichment":
            return leads.run_contact_enrichment(args.get("limit", 100))
        elif name == "leads_scrape":
            return leads.scrape_leads(**{k: v for k, v in args.items()})
        elif name == "outreach_quality_check_send":
            return outreach.quality_check_send(args["draft_ids"], args["target_email"])
        elif name == "outreach_delete_draft":
            return outreach.delete_draft(args["outreach_id"])
        elif name == "outreach_update_draft":
            return outreach.update_draft(args["outreach_id"], args.get("body", ""), args.get("subject", ""))
        elif name == "outreach_dismiss_reply":
            return outreach.dismiss_reply(args["reply_id"])
        elif name == "outreach_update_reply_draft":
            return outreach.update_reply_draft(args["reply_id"], args.get("body", ""), args.get("subject", ""))
        elif name == "research_niche_overview":
            return research.niche_overview()
        elif name == "research_findings_rollup":
            return research.findings_rollup()
        elif name == "research_findings_summary":
            return research.findings_summary(args.get("niche", ""), args.get("limit", 10))
        elif name == "research_strategy_report":
            return research.strategy_report()
        elif name == "research_pain_library":
            return research.pain_library(args.get("niche", ""), args.get("limit", 20))
        elif name == "research_upsert_pain":
            return research.upsert_pain(
                args["niche"],
                args["pain_key"],
                args["pain_label"],
                args.get("description", ""),
                args.get("evidence_summary", ""),
                args.get("evidence_types"),
                args.get("safe_outreach_claim", ""),
                args.get("unsafe_outreach_claim", ""),
                args.get("offer_angles"),
                args.get("best_channels"),
                args.get("confidence"),
            )
        elif name == "research_niche_report":
            return research.niche_report(args["niche"])
        elif name == "research_shortlist_status":
            return research.shortlist_status()
        elif name == "research_set_shortlist":
            return research.set_shortlist(args["niches"])
        elif name == "research_update_niche":
            return research.update_niche(
                args["niche"],
                args.get("notes", ""),
                args.get("external_evidence", ""),
                args.get("sample_market", ""),
                args.get("common_pains"),
                args.get("outreach_channel_fit"),
                args.get("pain_detectability"),
                args.get("contactability"),
                args.get("ability_to_deliver"),
                args.get("price_tolerance"),
                args.get("content_leverage"),
                args.get("status", ""),
            )
        elif name == "research_ingest_finding":
            return research.ingest_finding(
                args["niche"],
                args["source_type"],
                args["summary"],
                args.get("source_query", ""),
                args.get("source_title", ""),
                args.get("source_url", ""),
                args.get("market", ""),
                args.get("pain_point", ""),
                args.get("opportunity_type", ""),
                args.get("suggested_offer", ""),
                args.get("suggested_channel", ""),
                args.get("monetization_path", ""),
                args.get("evidence_strength"),
                args.get("confidence"),
                args.get("tags"),
                args.get("created_by", "agent"),
            )
        elif name == "research_lead_review_queue":
            return research.lead_review_queue(args.get("n", 10))
        elif name == "research_candidate_queue":
            return research.candidate_queue(args.get("n", 20))
        elif name == "research_save_lead_review":
            return research.save_lead_review(
                args["lead_id"],
                args["recommended_channel"],
                args["recommended_angle"],
                args.get("notes", ""),
                args.get("actual_business_model", ""),
                args.get("actual_pains", ""),
                args.get("email_fit", ""),
                args.get("social_fit", ""),
                args.get("form_fit", ""),
                args.get("hybrid_fit", ""),
            )
        elif name == "outreach_stats":
            return outreach.stats()
        elif name == "reporting_operator_summary":
            return reporting.operator_summary()
        elif name == "outreach_replies":
            return outreach.recent_replies(args.get("n", 5))
        elif name == "outreach_reply_queue":
            return outreach.reply_queue(args.get("n", 10))
        elif name == "outreach_prepare_reply_drafts":
            return outreach.prepare_reply_drafts(args.get("limit", 10))
        elif name == "outreach_preview_reply_drafts":
            return outreach.preview_reply_drafts(args.get("limit", 5))
        elif name == "outreach_send_reply_drafts":
            return outreach.send_reply_drafts(args.get("limit", 5))
        elif name == "outreach_internal_reply_test":
            return outreach.internal_reply_test()
        elif name == "outreach_internal_reply_test_status":
            return outreach.internal_reply_test_status(args.get("limit", 5))
        elif name == "outreach_internal_matrix_test":
            return outreach.internal_matrix_test(
                send=args.get("send", True),
                clear_history=args.get("clear_history", True),
            )
        elif name == "outreach_generate_drafts":
            return outreach.generate_drafts(args.get("limit", 5), args.get("target_niche", ""))
        elif name == "outreach_send_review_batch":
            return outreach.send_review_batch(args.get("limit", 5), args.get("recipient", "egorbrusnyak@gmail.com"))
        elif name == "outreach_review_batch_status":
            return outreach.review_batch_status(args.get("limit", 10))
        elif name == "outreach_poll_review_gate":
            return outreach.poll_review_gate(args.get("limit", 10))
        elif name == "outreach_approve_drafts":
            return outreach.approve_drafts(args.get("limit", 5))
        elif name == "outreach_schedule_approved":
            return outreach.schedule_approved(args.get("limit", 5))
        elif name == "outreach_process_send_queue":
            return outreach.process_send_queue(args.get("limit", 5))
        elif name == "outreach_send_queue_status":
            return outreach.send_queue_status(args.get("limit", 10))
        elif name == "outreach_pending_drafts":
            return outreach.pending_drafts(args.get("n", 10))
        elif name == "outreach_preview_drafts":
            return outreach.preview_drafts(args.get("n", 3))
        elif name == "outreach_poll_and_classify_replies":
            return outreach.poll_and_classify_replies()
        elif name == "outreach_test_send_lead":
            return outreach.test_send_lead(
                args["lead_id"],
                args.get("recipient", ""),
                args.get("clear_history", True),
                args.get("sender_address", ""),
            )
        elif name == "outreach_draft_for_lead":
            return outreach.generate_draft_for_lead(
                args["lead_id"],
                args.get("sender_address", ""),
                args.get("recipient", ""),
            )
        elif name == "outreach_poll_replies_for_lead":
            return outreach.poll_replies_for_lead(args["lead_id"])
        elif name == "content_report":
            return content.report()
        elif name == "content_plan_posts":
            return content.plan_posts(args.get("count", 5), args.get("queue", False))
        elif name == "content_prompt_manifests":
            return content.prompt_manifests(args.get("item_id", ""))
        elif name == "content_generate_images":
            return content.generate_images(
                args["item_id"],
                args.get("sample_count", 2),
                args.get("aspect_ratio", "3:4"),
            )
        elif name == "content_provider_status":
            return content.provider_status()
        elif name == "content_approve_post":
            return content.approve_post(args["item_id"])
        elif name == "content_prepare_publish":
            return content.prepare_publish(args["item_id"], args.get("publish_after", ""))
        elif name == "content_publish_post":
            return content.publish_post(args["item_id"])
        elif name == "content_reject_post":
            return content.reject_post(args["item_id"])
        elif name == "content_engagement_plan":
            return content.engagement_plan(args.get("niches"))
        elif name == "content_engagement_log":
            return content.engagement_log()
        elif name == "content_run_engagement":
            return content.run_engagement_session(
                niches=args.get("niches"),
                dry_run=args.get("dry_run", True),
                discover_only=args.get("discover_only", False),
            )
        elif name == "git_status":
            return git.status(args["repo_path"])
        elif name == "git_commit_push":
            return git.commit_and_push(args["repo_path"], args["message"])
        elif name == "memory_set":
            memory.set_fact(args["key"], args["value"])
            return f"Stored: {args['key']} = {args['value']}"
        elif name == "memory_get":
            return memory.summary()
        elif name == "memory_learn":
            return memory.learn(args["instruction"], args.get("key", ""))
        elif name == "ig_browse_profiles":
            results = content.ig_browse_profiles(args["usernames"], args.get("posts_per_profile", 3))
            return json.dumps(results, indent=2)
        elif name == "ig_draft_comment":
            return content.ig_draft_comment(
                args["post_url"],
                args.get("screenshot_path", ""),
                args.get("context_hint", ""),
            )
        elif name == "ig_post_comment":
            return content.ig_post_comment(args["post_url"], args["comment_text"])
        elif name == "ig_follow":
            return content.ig_follow(args["username"])
        elif name == "ig_discover_profiles":
            results = content.ig_discover_profiles(
                args.get("hashtags"),
                args.get("max_profiles", 8),
            )
            return json.dumps(results, indent=2)
        elif name == "ig_weekly_strategy":
            return content.ig_weekly_strategy(args.get("target_niches"))
        elif name == "ops_jobs_summary":
            return ops.jobs_summary()
        elif name == "ops_jobs_list":
            return ops.jobs_list()
        elif name == "ops_task_create":
            return ops.task_create(args["description"], args.get("kind", "agent_queued"), args.get("priority", 50))
        elif name == "ops_task_complete":
            return ops.task_complete(args["task_id"], args.get("result", ""))
        elif name == "ops_recent_events":
            return ops.recent_events(args.get("n", 10))
        else:
            return f"Unknown tool: {name}"
    except Exception as e:
        return f"Tool error ({name}): {e}"


def _system_prompt() -> str:
    facts = memory.summary()
    instructions = memory.get_instructions()
    knowledge = memory.knowledge_summary()
    return f"""You are the biz agent — autonomous operator for a one-person automation agency.

SAFE MODE — CRITICAL:
- Safe mode is controlled by the BIZ_SAFE_MODE env var, NOT by what PROJECT.md says.
- NEVER read PROJECT.md to determine if safe mode is on. The doc may be stale.
- If outreach_test_send_lead raises "Outbound action blocked by safe mode", tell the operator to set BIZ_SAFE_MODE=0 in .env and restart. Do NOT say "safe mode is on" based on reading a file.
- If BIZ_SAFE_MODE=0 is set in .env, outbound sends are live — proceed without warning.

OUTPUT FORMAT — NON-NEGOTIABLE:
You send messages via Telegram plain text. No markdown rendering happens.
Never use: ** bold **, * italic *, # headers, ``` code blocks ```, | tables |, --- dividers.
Use only: plain sentences, numbered lists (1. 2. 3.), dashes (- item), line breaks.
Emoji is fine sparingly. Keep responses short and scannable.

CAPABILITIES (answer directly without tools when asked):
You can: check leads stats and search leads, query outreach pipeline (sent/replies/queue), generate and approve outreach drafts, schedule and send emails, poll and classify inbound replies, draft and send reply emails, manage niche research and shortlist, run web searches, read/write project files, run shell commands, commit/push git repos, manage Victor content queue (plan/generate/approve/publish posts), check ops jobs and events, store/recall facts in memory. Add/edit/delete leads and emails directly. Trigger lead scrapes (web_search, hipages, google_maps). Run contact enrichment. Edit or delete outreach drafts and reply drafts before they send.

Instagram (Victor persona @victor.brusnyak):
- ig_browse_profiles: visit profiles, collect post URLs + screenshots
- ig_draft_comment: use Gemini vision to draft a comment, send to operator for approval
- ig_post_comment: post an approved comment (ALWAYS get operator approval first)
- ig_follow: follow a user
- content_run_engagement: run a full engagement session (like/comment/follow)

Learning / memory:
- memory_learn: when operator says "remember", "always", "never", "from now on" — store it
- memory_set / memory_get: store and recall arbitrary facts
- You can also write/modify code files in content/ (not agent/) when asked to add a feature

You have full access to the project: read/write files, run shell commands, query leads and outreach DBs, search the web, commit/push code, remember facts across sessions.

Project context (summary — use read_file for full detail):
{_PROJECT_SUMMARY}

{instructions}

Stored facts:
{facts}

Knowledge:
{knowledge}

Workspace layout (BIZ_ROOT = project root):
- leadgen/   — scraping, enrichment, validation, leads DB
- outreach/  — email pipeline, reply tracking, message generation
- agent/     — brain, tools, memory (you are here)
- telegram/  — this bot
- notes/     — context files, ideas, planning
- him/       — Victor persona spec
- content/   — Victor content queue, media providers, approval, publish prep
- PROJECT.md — current status, immediate next steps
- SPEC.md    — full spec, gap detection framework, outreach philosophy
- context_next_chat.md — detailed handoff from last session (at project root)

How to orient yourself:
When asked about project state or next steps:
1. read_file("context_next_chat.md") — session handoff with DB stats and priority list
2. read_file("PROJECT.md") — current status table
Do NOT read source files unless you need to modify them.
Do NOT explore the filesystem aimlessly.
Do NOT use run_shell or sqlite3 to query the leads DB — use leads_search, leads_get, leads_stats, leads_top instead.

Lead workflow — use these tools in order, never raw shell:
1. leads_search("name or email") — find a lead, get their ID and email
2. leads_get(lead_id) — full profile + outreach history for a specific lead
3. leads_update_email / leads_update_niche / leads_update_field — fix missing data if needed
4. leads_add(name, email, ...) — add a new lead manually if it doesn't exist

For TEST sends (operator says "send to my email", "test email", "demo"):
- Use outreach_test_send_lead(lead_id, recipient="their email", sender_address="victor.brusnyak@gmail.com") — generates + sends in one step
- Do NOT go through pending_drafts or generate_drafts for test sends
- This requires approval — the UI will show a confirm card. Do not ask "/yes" in text.

For INTERNAL cross-mailbox tests (operator says "use the 4 internal emails as leads", "send them to each other", "matrix test"):
- Use outreach_internal_matrix_test(send=true)
- Do NOT manually add 4 leads one by one unless this tool fails

For DRAFT with specific sender (operator says "draft using victor", "use victor.brusnyak"):
- Use outreach_draft_for_lead(lead_id, sender_address="victor.brusnyak@gmail.com") — creates draft + shows full preview with sign-off
- This does NOT send — operator approves separately via outreach_approve_drafts or outreach_test_send_lead

For PRODUCTION drafts (operator says "draft for review", "generate drafts", "queue"):
5. outreach_pending_drafts() — check if draft already exists for this lead first
6. outreach_generate_drafts(target_niche=...) — only if no draft exists
7. outreach_preview_drafts() — show content before any send
8. outreach_send_review_batch(recipient="egorbrusnyak@gmail.com") — send for review

Sender accounts (use exact addresses):
- lingonberry.max@gmail.com → Max Lingonberry (EMAIL_1)
- brusnyak.f@gmail.com → Victor Brusnyak (EMAIL_2)
- victor.brusnyak@gmail.com → Victor Brusnyak (EMAIL_3)
- brusnyakyegor@gmail.com → Yegor Brusnyak (EMAIL_4)

Monitoring replies:
- outreach_poll_replies_for_lead(lead_id) — check for reply after sending

CRITICAL — do not re-search what you already know:
- If session context shows last_lead_id, use it directly — do NOT call leads_search again
- If session context shows lead_email, you already have it — do NOT call leads_get again
- If session context shows pending_draft_ids, drafts exist — do NOT call outreach_generate_drafts

When outreach_generate_drafts returns "Created 0 draft(s)":
- Do NOT retry it. The lead already has a draft in the pipeline.
- Call outreach_pending_drafts() to find the existing draft.
- Then call outreach_preview_drafts() to show it.
- Report this to the operator clearly.

When leads_search returns a lead with an ID, always call leads_get(id) next to see the full profile and history before doing anything else.

Behaviour:
- Never output API keys, passwords, or secrets
- When asked about capabilities: answer directly from the CAPABILITIES section above, no tools needed
- When asked to check state: read context file first, then answer directly
- Do not assume dental is the winning niche unless research and validation support it
- When asked to build something: write the code, confirm it works, report what you did
- During queued overnight tasks, respect the task contract and approval mode. If an outward action needs approval, stop and return NEEDS_INPUT with the exact blocker.
- When you take action: say what you did and the result
- Be concise — no filler, no aimless exploration
- For Instagram comments: ALWAYS draft first, send draft to operator, wait for approval before posting
- When operator gives a standing instruction: call memory_learn to store it immediately

Tool errors and unexpected results:
- If a tool returns an error or a result that doesn't match what was expected (e.g. 0 results when results were expected, a traceback, "manual_required", "login needed"), treat it as a signal to investigate — don't just retry the same call.
- Read the error message carefully. If it mentions a missing login, missing config, or a path issue, say so clearly and tell the operator what needs to be fixed.
- If a tool returns "manual_required: ..." it means human action is needed before the tool can work. Stop, explain what's needed, and ask the operator to do it.
- Never silently retry a failing tool more than once without explaining why it failed.

Task focus — non-negotiable:
- Answer exactly what was asked. Nothing more.
- "Check if you can see lead X" = search + confirm. Stop. Do NOT load profile, fetch drafts, or preview anything unless asked.
- "Draft an email" = generate draft + show preview. Stop. Do NOT send, schedule, or fetch queue status unless asked.
- Do NOT call unrelated tools ever. If the task is done, say so and stop.
- Do NOT proactively fetch pending drafts, outreach stats, or queue status unless the operator explicitly asked for them.
- If you finish and the next logical step needs approval, state what it is and wait. Do not proceed.
- If unsure what to do next, ask — do not explore.

After completing a task:
- State what you found/did in 1-2 sentences.
- Offer at most 2 numbered next steps the operator can confirm. Keep each to one line.
"""

# ── Pretty action trace ───────────────────────────────────────────────────────

_TOOL_LABELS = {
    # leads
    "leads_search":             ("🔍", "Searching leads"),
    "leads_get":                ("👤", "Loading lead profile"),
    "leads_update_email":       ("✏️", "Updating lead email"),
    "leads_update_niche":       ("✏️", "Updating lead niche"),
    "leads_update_field":       ("✏️", "Updating lead field"),
    "leads_delete":             ("🗑️", "Deleting lead"),
    "leads_add":                ("➕", "Adding lead"),
    "leads_set_status":         ("🏷️", "Setting lead status"),
    "leads_clear_outreach":     ("🧹", "Clearing outreach history"),
    "leads_run_enrichment":     ("🔬", "Running contact enrichment"),
    "leads_scrape":             ("🕷️", "Scraping leads"),
    "outreach_delete_draft":    ("🗑️", "Deleting draft"),
    "outreach_update_draft":    ("✏️", "Editing draft"),
    "outreach_dismiss_reply":   ("🚫", "Dismissing reply"),
    "outreach_update_reply_draft": ("✏️", "Editing reply draft"),
    "outreach_draft_for_lead":  ("✍️", "Drafting for lead"),
    "leads_stats":              ("📊", "Checking lead stats"),
    "leads_top":                ("📋", "Fetching top leads"),
    # outreach
    "outreach_stats":           ("📬", "Checking outreach stats"),
    "outreach_replies":         ("💬", "Fetching replies"),
    "outreach_reply_queue":     ("📥", "Checking reply queue"),
    "outreach_pending_drafts":  ("📝", "Fetching pending drafts"),
    "outreach_preview_drafts":  ("👁", "Previewing drafts"),
    "outreach_generate_drafts": ("✍️", "Generating outreach drafts"),
    "outreach_send_review_batch":("📤", "Sending review batch"),
    "outreach_review_batch_status":("🔎", "Checking review batch status"),
    "outreach_poll_review_gate":("🔄", "Polling review gate"),
    "outreach_approve_drafts":  ("✅", "Approving drafts"),
    "outreach_schedule_approved":("🗓", "Scheduling approved drafts"),
    "outreach_process_send_queue":("🚀", "Processing send queue"),
    "outreach_send_queue_status":("📊", "Checking send queue"),
    "outreach_poll_and_classify_replies":("🤖", "Classifying replies"),
    "outreach_test_send_lead":       ("🚀", "Sending test outreach"),
    "outreach_poll_replies_for_lead":("📬", "Polling replies for lead"),
    "outreach_prepare_reply_drafts":("✍️", "Preparing reply drafts"),
    "outreach_preview_reply_drafts":("👁", "Previewing reply drafts"),
    "outreach_send_reply_drafts":("📤", "Sending reply drafts"),
    "outreach_internal_reply_test":("🧪", "Running internal reply test"),
    "outreach_internal_reply_test_status":("🔎", "Checking reply test status"),
    # research
    "research_niche_overview":  ("🗺", "Loading niche overview"),
    "research_findings_summary":("📰", "Fetching research findings"),
    "research_pain_library":    ("📚", "Loading pain library"),
    "research_niche_report":    ("📄", "Generating niche report"),
    "research_strategy_report": ("🧭", "Loading strategy report"),
    "research_shortlist_status":("📌", "Checking shortlist"),
    "research_set_shortlist":   ("📌", "Updating shortlist"),
    "research_update_niche":    ("✏️", "Updating niche data"),
    "research_ingest_finding":  ("💾", "Storing research finding"),
    "research_upsert_pain":     ("💾", "Saving pain entry"),
    "research_candidate_queue": ("👥", "Fetching candidate leads"),
    "research_lead_review_queue":("🔍", "Loading lead review queue"),
    "research_save_lead_review":("💾", "Saving lead review"),
    "research_findings_rollup": ("📊", "Rolling up findings"),
    # content / IG
    "content_report":           ("📸", "Checking content queue"),
    "content_plan_posts":       ("🗓", "Planning posts"),
    "content_generate_images":  ("🎨", "Generating images"),
    "content_approve_post":     ("✅", "Approving post"),
    "content_reject_post":      ("❌", "Rejecting post"),
    "content_prepare_publish":  ("📦", "Preparing post for publish"),
    "content_publish_post":     ("🚀", "Publishing post"),
    "content_engagement_plan":  ("📅", "Building engagement plan"),
    "content_engagement_log":   ("📋", "Loading engagement log"),
    "content_run_engagement":   ("🤝", "Running engagement session"),
    "content_prompt_manifests": ("📝", "Writing image prompts"),
    "content_provider_status":  ("🔌", "Checking media providers"),
    "ig_browse_profiles":       ("👤", "Browsing IG profiles"),
    "ig_discover_profiles":     ("🔭", "Discovering IG profiles"),
    "ig_draft_comment":         ("✍️", "Drafting IG comment"),
    "ig_post_comment":          ("💬", "Posting IG comment"),
    "ig_follow":                ("➕", "Following IG account"),
    "ig_weekly_strategy":       ("📅", "Building IG weekly strategy"),
    # web / files / shell
    "web_search":               ("🌐", "Searching the web"),
    "read_file":                ("📖", "Reading file"),
    "write_file":               ("💾", "Writing file"),
    "run_shell":                ("⚙️", "Running command"),
    "list_dir":                 ("📁", "Listing directory"),
    "code_search":              ("🔍", "Searching code"),
    "run_tests":                ("🧪", "Running tests"),
    # git
    "git_status":               ("🌿", "Checking git status"),
    "git_commit_push":          ("⬆️", "Committing and pushing"),
    # memory / ops
    "memory_set":               ("🧠", "Storing fact"),
    "memory_get":               ("🧠", "Recalling facts"),
    "memory_learn":             ("🧠", "Learning instruction"),
    "ops_jobs_summary":         ("📊", "Checking job queue"),
    "ops_jobs_list":            ("📋", "Listing jobs"),
    "ops_task_create":          ("📝", "Creating task"),
    "ops_task_complete":        ("✅", "Completing task"),
    "ops_recent_events":        ("📜", "Loading recent events"),
    "reporting_operator_summary":("📊", "Loading operator summary"),
}

_TOOL_ARG_HINTS = {
    "web_search":           lambda a: a.get("query", ""),
    "leads_search":         lambda a: a.get("query", ""),
    "read_file":            lambda a: a.get("path", ""),
    "write_file":           lambda a: a.get("path", ""),
    "run_shell":            lambda a: a.get("cmd", "")[:60],
    "code_search":          lambda a: a.get("pattern", ""),
    "list_dir":             lambda a: a.get("path", ""),
    "git_commit_push":      lambda a: a.get("message", ""),
    "memory_set":           lambda a: f"{a.get('key','')} = {a.get('value','')}",
    "memory_learn":         lambda a: a.get("instruction", "")[:60],
    "ig_browse_profiles":   lambda a: ", ".join(a.get("usernames", [])),
    "ig_discover_profiles": lambda a: ", ".join(a.get("hashtags", [])),
    "ig_follow":            lambda a: a.get("username", ""),
    "ig_post_comment":      lambda a: a.get("username", a.get("post_url", ""))[:40],
    "outreach_generate_drafts": lambda a: a.get("target_niche", ""),
    "outreach_send_review_batch": lambda a: a.get("recipient", ""),
    "outreach_test_send_lead":    lambda a: f"lead={a.get('lead_id', '?')} → {a.get('recipient', 'lead email')}",
    "outreach_poll_replies_for_lead": lambda a: f"lead={a.get('lead_id', '?')}",
    "research_niche_report":lambda a: a.get("niche", ""),
    "research_update_niche":lambda a: a.get("niche", ""),
    "research_ingest_finding": lambda a: a.get("niche", ""),
    "research_upsert_pain": lambda a: f"{a.get('niche','')} / {a.get('pain_key','')}",
    "content_approve_post": lambda a: a.get("item_id", ""),
    "content_publish_post": lambda a: a.get("item_id", ""),
    "content_generate_images": lambda a: a.get("item_id", ""),
}


def _format_trace(tool_name: str, args: dict, result: str | None = None) -> str:
    """Return a single human-readable trace line for a tool call."""
    emoji, label = _TOOL_LABELS.get(tool_name, ("⚙️", tool_name.replace("_", " ").title()))
    hint_fn = _TOOL_ARG_HINTS.get(tool_name)
    hint = hint_fn(args).strip() if hint_fn else ""
    line = f"{emoji} {label}"
    if hint:
        line += f": {hint}"
    if result is not None:
        # Summarise result: first non-empty line, capped
        first = next((l.strip() for l in result.splitlines() if l.strip()), "")
        if first:
            line += f"  →  {first[:80]}"
    return line


# ── Tool subsets by task phase ────────────────────────────────────────────────
# Sending all 50+ tools every round confuses smaller free models.
# We detect the likely phase from the user message and return a focused subset.

_TOOL_NAMES_BY_GROUP = {
    "leads":    {"leads_search", "leads_get", "leads_stats", "leads_top",
                 "leads_update_email", "leads_update_niche", "leads_update_field",
                 "leads_delete", "leads_add", "leads_set_status", "leads_clear_outreach",
                 "leads_run_enrichment", "leads_scrape"},
    "outreach": {"outreach_stats", "outreach_pending_drafts", "outreach_preview_drafts",
                 "outreach_generate_drafts", "outreach_send_review_batch",
                 "outreach_review_batch_status", "outreach_poll_review_gate",
                 "outreach_approve_drafts", "outreach_schedule_approved",
                 "outreach_process_send_queue", "outreach_send_queue_status",
                 "outreach_replies", "outreach_reply_queue",
                 "outreach_prepare_reply_drafts", "outreach_preview_reply_drafts",
                 "outreach_send_reply_drafts", "outreach_poll_and_classify_replies",
                 "outreach_internal_reply_test", "outreach_internal_reply_test_status",
                 "outreach_test_send_lead", "outreach_poll_replies_for_lead",
                 "outreach_quality_check_send", "outreach_delete_draft", "outreach_update_draft",
                 "outreach_dismiss_reply", "outreach_update_reply_draft",
                 "outreach_draft_for_lead"},
    "research": {"research_niche_overview", "research_findings_rollup",
                 "research_findings_summary", "research_strategy_report",
                 "research_pain_library", "research_upsert_pain", "research_niche_report",
                 "research_shortlist_status", "research_set_shortlist",
                 "research_update_niche", "research_ingest_finding",
                 "research_lead_review_queue", "research_candidate_queue",
                 "research_save_lead_review"},
    "content":  {"content_report", "content_plan_posts", "content_prompt_manifests",
                 "content_generate_images", "content_provider_status",
                 "content_approve_post", "content_prepare_publish", "content_publish_post",
                 "content_reject_post", "content_engagement_plan", "content_engagement_log",
                 "content_run_engagement", "ig_browse_profiles", "ig_discover_profiles",
                 "ig_draft_comment", "ig_post_comment", "ig_follow", "ig_weekly_strategy"},
    "files":    {"read_file", "write_file", "list_dir", "run_shell",
                 "code_search", "run_tests"},
    "ops":      {"git_status", "git_commit_push", "memory_set", "memory_get",
                 "memory_learn", "ops_jobs_summary", "ops_jobs_list",
                 "ops_task_create", "ops_task_complete",
                 "ops_recent_events", "reporting_operator_summary", "web_search"},
}

_PHASE_KEYWORDS = {
    "leads":    ["lead", "email", "niche", "contact", "search lead", "find lead",
                 "profile", "score", "qualified"],
    "outreach": ["draft", "outreach", "send", "email", "reply", "queue", "preview",
                 "approve", "schedule", "review batch"],
    "research": ["niche", "pain", "research", "finding", "shortlist", "market",
                 "evidence", "strategy"],
    "content":  ["content", "post", "instagram", "ig", "victor", "image", "engage",
                 "publish", "caption"],
    "files":    ["read", "write", "file", "code", "shell", "run", "search code",
                 "list dir"],
    "ops":      ["git", "commit", "push", "memory", "remember", "job", "event",
                 "status", "summary", "report"],
}

_TOOL_DEFS_BY_NAME: dict = {}  # populated lazily on first call


def _index_tools():
    global _TOOL_DEFS_BY_NAME
    if not _TOOL_DEFS_BY_NAME:
        _TOOL_DEFS_BY_NAME = {t["function"]["name"]: t for t in TOOLS}


def _tools_for_message(msg: str) -> list:
    """
    Return a focused tool subset based on what the message is about.
    Always includes ops (memory/web/git) as a base.
    Falls back to all tools if no phase is detected.
    """
    _index_tools()
    msg_lower = msg.lower()
    active_groups = {"ops"}  # always included

    for group, keywords in _PHASE_KEYWORDS.items():
        if any(kw in msg_lower for kw in keywords):
            active_groups.add(group)

    # If a lead task also mentions explicit outreach actions, include outreach tools
    if "leads" in active_groups and any(
        kw in msg_lower for kw in ["draft", "send outreach", "generate draft", "outreach email", "send email"]
    ):
        active_groups.add("outreach")

    # "test send", "send to my email", "draft an email" with a lead in context → outreach
    if any(kw in msg_lower for kw in ["test send", "send to my email", "draft an email", "draft email", "send for testing", "test email"]):
        active_groups.add("outreach")
        active_groups.add("leads")

    active_names: set = set()
    for g in active_groups:
        active_names |= _TOOL_NAMES_BY_GROUP.get(g, set())

    subset = [_TOOL_DEFS_BY_NAME[n] for n in active_names if n in _TOOL_DEFS_BY_NAME]

    # Fall back to full set if detection missed everything meaningful
    if len(subset) <= len(_TOOL_NAMES_BY_GROUP["ops"]):
        return TOOLS

    return subset


def _trim_tool_result(result: str, tool_name: str = "", max_chars: int = 1200) -> str:
    """
    Keep tool results lean in the message history.
    Outreach/lead tools get a larger window since drafts + metadata are verbose.
    """
    # Larger cap for tools that return structured content (drafts, profiles, previews)
    _LARGE_RESULT_TOOLS = {
        "outreach_preview_drafts", "outreach_pending_drafts", "outreach_preview_reply_drafts",
        "leads_get", "outreach_reply_queue", "outreach_send_queue_status",
        "outreach_review_batch_status", "reporting_operator_summary",
        "research_niche_report", "research_pain_library", "research_findings_summary",
    }
    if tool_name in _LARGE_RESULT_TOOLS:
        max_chars = 3000

    if len(result) <= max_chars:
        return result
    head = result[:max_chars]
    last_nl = head.rfind("\n")
    if last_nl > max_chars // 2:
        head = head[:last_nl]
    remaining = len(result) - len(head)
    return head + f"\n... [{remaining} chars truncated — use a more specific query if you need more]"


def ask(user_message: str, max_tool_rounds: int = 20, verbose: bool = True,
        activity_cb=None, confirm_cb=None,
        _scratchpad: dict = None,
        _tool_subset: list | None = None,
        _stop_flag=None) -> str:
    """
    ReAct-style agent loop: Thought → Tool → Observe → repeat.

    Key reliability improvements over a plain tool loop:
    - Focused tool subset per message (reduces model confusion on free tiers)
    - Tool results trimmed before appending to history (keeps context lean)
    - Explicit thought step injected via system prompt (reduces drift)
    - Scratchpad persists key facts across continuation passes

    _scratchpad: shared dict for persisting key findings across continuation passes.
    _tool_subset: override the auto-detected tool subset (used by run_next_task).
    """
    memory.add_history("user", user_message)

    scratchpad = _scratchpad if _scratchpad is not None else {}
    active_tools = _tool_subset if _tool_subset is not None else _tools_for_message(user_message)

    def _scratchpad_block() -> str:
        if not scratchpad:
            return ""
        lines = ["Session context (already known — do NOT re-fetch these):"]
        for k, v in scratchpad.items():
            if not k.startswith("_"):
                lines.append(f"  {k}: {v}")
        return "\n".join(lines)

    def _build_messages():
        sys = _system_prompt()
        sp = _scratchpad_block()
        if sp:
            # Inject scratchpad both at top AND bottom of system prompt so small models see it
            sys = sp + "\n\n" + sys + "\n\n" + sp
        sys += (
            "\n\nREASONING PROTOCOL:\n"
            "Before each tool call, write one short Thought: line explaining what you are "
            "doing and why. Example: 'Thought: I need to find the lead ID before I can check "
            "outreach history.' This keeps you on track. Do not skip the Thought step.\n"
            "IMPORTANT: If the session context above already has a lead_id or lead_email, "
            "do NOT call leads_search again — use the ID you already have."
        )
        msgs = [{"role": "system", "content": sys}]
        for h in memory.get_history(n=10)[:-1]:
            msgs.append({"role": h["role"], "content": h["content"]})
        msgs.append({"role": "user", "content": user_message})
        return msgs

    messages = _build_messages()

    def _log(msg: str):
        if verbose:
            print(msg, flush=True)
        if activity_cb:
            try:
                activity_cb(msg)
            except Exception:
                pass

    # ── tool result classifier ────────────────────────────────────────────────
    def _classify_tool_error(result: str) -> str | None:
        """Return error class string if result looks like a failure, else None."""
        r = result.strip().lower()
        if r.startswith("tool error") or "traceback" in r:
            return "tool_error"
        if "no such file" in r or "not found" in r or "is a directory" in r:
            return "path_error"
        if "no leads matching" in r or "no qualified leads" in r:
            return "empty_result"
        if "manual_required" in r or "login needed" in r:
            return "manual_required"
        if "needs_input" in r or "approval_required" in r:
            return "needs_approval"
        return None

    # ── scratchpad auto-update ────────────────────────────────────────────────
    def _maybe_update_scratchpad(tool_name: str, args: dict, result: str):
        """Extract key facts from tool results and store in scratchpad."""
        import re as _re
        if tool_name == "leads_search" and "No leads matching" not in result:
            # Try [ID] format first, then bare "id=NNN" or "id: NNN"
            m = _re.search(r"\[(\d+)\]", result)
            if not m:
                m = _re.search(r"\bid\s*[=:]\s*(\d+)", result, _re.IGNORECASE)
            if m:
                scratchpad["last_lead_id"] = int(m.group(1))
                scratchpad["last_lead_search"] = args.get("query", "")
                # Also grab email if present on same line
                em = _re.search(r"email\s*[=:]\s*(\S+@\S+)", result, _re.IGNORECASE)
                if em:
                    scratchpad["lead_email"] = em.group(1).strip(".,;")
        elif tool_name == "leads_get":
            lead_id = args.get("lead_id")
            if lead_id:
                scratchpad["last_lead_id"] = lead_id
            # Build a structured context card so the agent never re-fetches this lead
            card = {}
            for line in result.splitlines():
                ll = line.lower().strip()
                if ll.startswith("lead [") and "lead_name" not in scratchpad:
                    nm = _re.search(r"Lead\s*\[\d+\]:\s*(.+)", line, _re.IGNORECASE)
                    if nm:
                        card["lead_name"] = nm.group(1).strip()
                elif ll.startswith("email:"):
                    val = line.split(":", 1)[-1].strip()
                    if "@" in val and val != "none":
                        card["lead_email"] = val
                elif ll.startswith("niche:"):
                    card["lead_niche"] = line.split(":", 1)[-1].strip()
                elif ll.startswith("status:"):
                    card["lead_status"] = line.split(":", 1)[-1].strip()
                elif ll.startswith("score:"):
                    card["lead_score"] = line.split(":", 1)[-1].strip()
                elif ll.startswith("outreach_angle:") and line.split(":", 1)[-1].strip() not in ("none", ""):
                    card["lead_angle"] = line.split(":", 1)[-1].strip()
                elif "outreach history:" in ll and "none" not in ll:
                    card["has_outreach_history"] = True
            scratchpad.update(card)
        elif tool_name == "outreach_generate_drafts":
            m = _re.search(r"Created (\d+) draft", result)
            if m and int(m.group(1)) > 0:
                draft_m = _re.search(r"\[(\d+)\]", result)
                if draft_m:
                    scratchpad["last_draft_id"] = draft_m.group(1)
            elif "Created 0" in result or (m and int(m.group(1)) == 0):
                scratchpad["generate_drafts_zero"] = result.strip()
        elif tool_name == "outreach_preview_drafts":
            scratchpad["draft_previewed"] = True
        elif tool_name == "outreach_test_send_lead":
            if "sent" in result.lower() or "outreach_id" in result.lower():
                scratchpad["test_email_sent"] = True
                scratchpad["test_send_result"] = result[:300]
        elif tool_name == "outreach_pending_drafts":
            lead_id = scratchpad.get("last_lead_id")
            if lead_id:
                if f"[{lead_id}]" in result or str(lead_id) in result:
                    ids = _re.findall(r"\[(\d+)\]", result)
                    if ids:
                        scratchpad["pending_draft_ids"] = ids

    recent_tool_signatures = []
    consecutive_errors = 0

    for round_n in range(max_tool_rounds):

        # ── check stop flag ───────────────────────────────────────────────────
        if _stop_flag and _stop_flag():
            _log("⏹ Stop requested by operator.")
            return "Stopped by operator."

        # ── warn agent when approaching limit ────────────────────────────────
        rounds_left = max_tool_rounds - round_n
        if rounds_left == 4 and not scratchpad.get("_warned_limit"):
            scratchpad["_warned_limit"] = True
            messages.append({
                "role": "system",
                "content": (
                    f"WARNING: You have {rounds_left} tool rounds remaining. "
                    "If you cannot finish, write a CHECKPOINT: summary of what you have done "
                    "and exactly what step comes next. Do not guess or make up results."
                )
            })

        resp = _chat(messages, tools=active_tools)
        msg = resp.choices[0].message
        _log(f"[model: {resp.model.split('/')[-1]}]")

        inline_content = (msg.content or "").strip()

        if not msg.tool_calls:
            answer = inline_content
            memory.add_history("assistant", answer)
            return answer

        for tc in msg.tool_calls:
            _log(_format_trace(tc.function.name, {}))
            recent_tool_signatures.append((tc.function.name, tc.function.arguments[:120]))

        # ── loop detection ────────────────────────────────────────────────────
        # Detect: same tool + same args called 2+ times in a row (exact repeat)
        if len(recent_tool_signatures) >= 2:
            last_sig = recent_tool_signatures[-1]
            prev_sig = recent_tool_signatures[-2]
            if last_sig == prev_sig:
                messages.append({
                    "role": "system",
                    "content": (
                        f"You just called `{last_sig[0]}` with the same arguments twice in a row. "
                        "This is a loop. Do NOT call it again. "
                        "If you already have the result, use it. "
                        "If the result was empty, accept that and move on or ask the operator."
                    )
                })

        if len(recent_tool_signatures) >= 3:
            last_3 = [s[0] for s in recent_tool_signatures[-3:]]
            if len(set(last_3)) == 1:
                cancelled_name = last_3[0]
                if any(
                    f"Action cancelled by operator: {cancelled_name}" in str(m.get("content", ""))
                    for m in messages if m.get("role") == "tool"
                ):
                    answer = (
                        f"I need your approval to run `{cancelled_name}`. "
                        f"Please confirm you want me to proceed, or tell me to skip it."
                    )
                    memory.add_history("assistant", answer)
                    return answer

        if len(recent_tool_signatures) >= 6:
            window = recent_tool_signatures[-6:]
            if len(set(window)) <= 2:
                # Inject a redirect instead of giving up
                messages.append({
                    "role": "system",
                    "content": (
                        "You are repeating the same tools without progress. Stop and think: "
                        "what is the actual blocker? Try a completely different tool or approach. "
                        "If you are stuck on a missing piece of data, say so explicitly."
                    )
                })
                recent_tool_signatures.clear()
                consecutive_errors = 0
                continue

        msg_dict = {"role": "assistant"}
        if inline_content:
            msg_dict["content"] = inline_content
        if msg.tool_calls:
            msg_dict["tool_calls"] = [
                {
                    "id": tc.id,
                    "type": "function",
                    "function": {"name": tc.function.name, "arguments": tc.function.arguments},
                }
                for tc in msg.tool_calls
            ]
        messages.append(msg_dict)

        for tc in msg.tool_calls:
            try:
                args = json.loads(tc.function.arguments)
            except json.JSONDecodeError:
                args = {}

            tool_policy = policy.policy_for(tc.function.name)
            if tool_policy in (policy.APPROVAL_REQUIRED, policy.APPROVED_EXECUTION):
                if confirm_cb is not None:
                    confirmed = confirm_cb(tc.function.name, args)
                    if not confirmed:
                        result = f"Action cancelled by operator: {tc.function.name}"
                        _log(f"  ↩ cancelled")
                    else:
                        result = _dispatch(tc.function.name, args)
                else:
                    prompt_text = _format_confirm_prompt(tc.function.name, args)
                    result = f"NEEDS_INPUT: {prompt_text}"
                    _log(f"  ⏸ needs approval: {tc.function.name}")
            else:
                result = _dispatch(tc.function.name, args)

            # ── scratchpad update ─────────────────────────────────────────────
            _maybe_update_scratchpad(tc.function.name, args, str(result))

            # ── error classification + recovery hint ──────────────────────────
            err_class = _classify_tool_error(str(result))
            if err_class in ("tool_error", "path_error"):
                consecutive_errors += 1
                if consecutive_errors >= 2:
                    messages.append({
                        "role": "system",
                        "content": (
                            f"Two consecutive tool errors ({err_class}). "
                            "Do NOT retry the same command. Use a different tool or approach. "
                            "If the path is wrong, use list_dir to find the correct location. "
                            "If the tool itself is broken, skip it and work around it."
                        )
                    })
            else:
                consecutive_errors = 0

            _log(_format_trace(tc.function.name, args, str(result)))
            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": _trim_tool_result(str(result), tool_name=tc.function.name),
            })

    # ── round limit hit — checkpoint instead of dying ────────────────────────
    try:
        checkpoint_resp = _chat([
            {"role": "system", "content": _system_prompt()},
            {"role": "user", "content": user_message},
            *messages[1:],  # full conversation so far
            {
                "role": "user",
                "content": (
                    "You have run out of tool rounds. Write a CHECKPOINT in this exact format:\n"
                    "CHECKPOINT:\n"
                    "Done: <what you completed>\n"
                    "Next: <the exact next step to take>\n"
                    "Blockers: <anything missing or needing approval, or 'none'>"
                )
            }
        ])
        checkpoint_text = (checkpoint_resp.choices[0].message.content or "").strip()
    except Exception:
        checkpoint_text = f"CHECKPOINT:\nDone: partial work on task\nNext: retry from beginning\nBlockers: none"

    # Store checkpoint in scratchpad for continuation
    scratchpad["_checkpoint"] = checkpoint_text
    memory.add_history("assistant", checkpoint_text)
    return checkpoint_text


def run_next_task(verbose: bool = False, activity_cb=None) -> tuple[str, str] | None:
    """
    Pick the next pending task, run it, update status.
    Returns (task_id, result) or None if no pending tasks.
    """
    import tasks as task_store
    task = task_store.claim_next(owner="brain")
    if not task:
        return None

    # Build a focused tool subset from the task kind + description
    # so the model isn't overwhelmed by all 50+ tools for a narrow task
    _index_tools()
    task_tools = _tools_for_message(
        f"{task.get('kind', '')} {task.get('description', '')} {task.get('goal', '')}"
    )

    prompt = (
        f"You are working on a queued task. Complete it fully and autonomously.\n\n"
        f"Task id: {task['id']}\n"
        f"Task kind: {task.get('kind', 'general')}\n"
        f"Task: {task['description']}\n"
        f"Goal: {task.get('goal') or task['description']}\n"
        f"Allowed actions: {task.get('allowed_actions', [])}\n"
        f"Approval mode: {task.get('approval_mode', 'internal_only')}\n"
        f"Stop conditions: {task.get('stop_conditions', [])}\n"
        f"Outputs expected: {task.get('outputs_expected', [])}\n\n"
        f"When done, summarize what you did in 2-3 sentences. "
        f"If you need human input to proceed, start your reply with NEEDS_INPUT:"
    )

    try:
        _EXECUTION_CONTEXT["task_id"] = task["id"]
        _EXECUTION_CONTEXT["allowed_actions"] = task.get("allowed_actions")
        _EXECUTION_CONTEXT["approval_mode"] = task.get("approval_mode", "internal_only")
        task_store.set_status_summary(task["id"], "running")

        scratchpad: dict = {}
        max_passes = 3
        result = ""
        for pass_n in range(max_passes):
            if pass_n > 0:
                checkpoint = scratchpad.get("_checkpoint", "")
                scratchpad.pop("_warned_limit", None)
                scratchpad.pop("_checkpoint", None)
                prompt = (
                    f"Continuing task (pass {pass_n + 1}).\n\n"
                    f"Original task:\n{task['description']}\n\n"
                    f"Progress:\n{checkpoint}\n\n"
                    f"Pick up exactly where you left off."
                )
            result = ask(prompt, verbose=verbose, activity_cb=activity_cb,
                         _scratchpad=scratchpad, _tool_subset=task_tools)
            if not result.strip().startswith("CHECKPOINT:"):
                break

        if result.startswith("NEEDS_INPUT:") or "APPROVAL_REQUIRED:" in result:
            blockers = []
            if "APPROVAL_REQUIRED:" in result:
                blockers.append(result.split("APPROVAL_REQUIRED:", 1)[1].strip())
            task_store.set_blockers(task["id"], blockers or ["Needs clarification or approval"])
            task_store.mark_needs_input(task["id"], result)
        else:
            task_store.complete(task["id"], result)
        return task["id"], result
    except Exception as e:
        err = f"Task failed: {e}"
        task_store.fail(task["id"], err)
        return task["id"], err
    finally:
        _EXECUTION_CONTEXT["task_id"] = None
        _EXECUTION_CONTEXT["allowed_actions"] = None
        _EXECUTION_CONTEXT["approval_mode"] = "internal_only"


def _repl_input(prompt: str) -> str:
    """Read a line, supporting multi-line paste (blank line ends input)."""
    try:
        first = input(prompt).strip()
    except (EOFError, KeyboardInterrupt):
        return "exit"
    if not first:
        return ""
    lines = [first]
    # If the first line looks like a multi-task dump (long or contains newlines
    # already), keep reading until a blank line so the user can paste freely.
    while True:
        try:
            line = input("... ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if line == "":
            break
        lines.append(line)
    return "\n".join(lines)


_PLAN_SYSTEM = """You are the biz agent — autonomous operator for a one-person automation agency.

The operator has given you one or more tasks. Your job right now is to:
1. Read them carefully using your project knowledge (leads DB, outreach pipeline, pain library, etc.)
2. Ask ONE short clarifying question only if something is genuinely ambiguous and you cannot proceed without it
3. Otherwise skip straight to a numbered execution plan

Project facts you already know:
- egorbrusnyak@gmail.com is the operator's own test/review email — it may or may not be in the leads DB as a test lead
- The outreach pipeline has: pending drafts, approved drafts, a send queue, and a review batch flow
- The leads DB has real estate, dental, accounting leads — search by name or email
- Pain library has real_estate entries with confidence scores
- Safe mode is ON by default — sends need explicit approval

Plan format rules:
- Numbered list, one item per original task
- Each item: what you will do + what you will show/ask before proceeding
- End with: "Type 'go' to start, a number to jump to that task, or answer any questions."
- No markdown, no bold, no headers — plain text only
"""


def _plan(original_input: str, clarifications: str = "") -> tuple[str, list[str]]:
    """
    Ask the LLM to reason about the input and produce a plan.
    Returns (plan_text, [original_task_chunks]).
    The task list preserves the original task text, not the plan summary.
    """
    content = original_input
    if clarifications:
        content += f"\n\nOperator clarification: {clarifications}"

    msgs = [
        {"role": "system", "content": _PLAN_SYSTEM},
        {"role": "user", "content": content},
    ]
    resp = _chat(msgs)
    plan_text = (resp.choices[0].message.content or "").strip()

    # Split original input into task chunks by "Task N" markers or numbered lines
    import re
    # Try "Task N —" or "Task N:" style headers first
    chunks = re.split(r"(?:^|\n)Task\s+\d+\s*[—:\-]", original_input, flags=re.IGNORECASE)
    chunks = [c.strip() for c in chunks if c.strip()]

    # Fallback: split on blank lines between numbered items
    if len(chunks) <= 1:
        chunks = re.split(r"\n{2,}", original_input.strip())
        chunks = [c.strip() for c in chunks if c.strip()]

    # Last fallback: treat whole input as one task
    if not chunks:
        chunks = [original_input.strip()]

    return plan_text, chunks


def _run_task_interactive(task_text: str, task_num: int, total: int, full_context: str = "") -> bool:
    """
    Run a single task with live tool output, then ask for approval to continue.
    Auto-retries up to 3 times on CHECKPOINT (round limit hit) before surfacing to operator.
    Returns True if operator wants to continue, False to stop.
    """
    label = next((l.strip() for l in task_text.splitlines() if l.strip()), task_text[:60])
    print(f"\n{'─'*60}")
    print(f"task {task_num}/{total}  {label}")
    print('─'*60)

    def _stream(msg: str):
        if msg.startswith("[model:"):
            print(f"  🤖 {msg[7:].strip().rstrip(']')}", flush=True)
        else:
            print(f"  {msg}", flush=True)

    prompt = task_text
    if full_context and task_num > 1:
        prompt = f"Context from earlier tasks:\n{full_context}\n\nCurrent task:\n{task_text}"

    # Focused tool subset for this task
    task_tools = _tools_for_message(task_text)

    # Shared scratchpad persists across continuation passes
    scratchpad: dict = {}
    max_continuations = 3

    for attempt in range(max_continuations):
        if attempt > 0:
            checkpoint = scratchpad.get("_checkpoint", "")
            print(f"\n  ↻ continuing (pass {attempt + 1}/{max_continuations})...")
            # Build a continuation prompt from the checkpoint
            prompt = (
                f"You are continuing a task that ran out of tool rounds.\n\n"
                f"Original task:\n{task_text}\n\n"
                f"Progress so far:\n{checkpoint}\n\n"
                f"Pick up exactly where you left off. Do not repeat completed steps."
            )
            # Clear the limit warning so it resets for the new pass
            scratchpad.pop("_warned_limit", None)
            scratchpad.pop("_checkpoint", None)

        result = ask(prompt, verbose=False, activity_cb=_stream,
                     _scratchpad=scratchpad, _tool_subset=task_tools)

        is_checkpoint = result.strip().startswith("CHECKPOINT:")
        is_needs_input = result.startswith("NEEDS_INPUT:") or "APPROVAL_REQUIRED:" in result

        if is_needs_input:
            # Surface approval requests immediately — don't retry
            print(f"\nagent: {result}\n")
            break

        if not is_checkpoint:
            # Clean completion
            print(f"\nagent: {result}\n")
            break

        # Checkpoint — show progress and auto-continue unless last attempt
        print(f"\n  📍 {result}\n")
        if attempt == max_continuations - 1:
            print(f"  ⚠ reached max continuations ({max_continuations}), surfacing to operator\n")

    if task_num >= total:
        return True

    while True:
        try:
            reply = input(f"[task {task_num} done] continue to task {task_num+1}? (go / skip / stop / <feedback>) ").strip()
        except (EOFError, KeyboardInterrupt):
            return False
        low = reply.lower()
        if low in ("go", "y", "yes", ""):
            return True
        if low == "stop":
            return False
        if low == "skip":
            print(f"  skipping task {task_num+1}")
            return True
        if reply:
            print()
            fb_result = ask(reply, verbose=False, activity_cb=_stream,
                            _scratchpad=scratchpad, _tool_subset=task_tools)
            print(f"\nagent: {fb_result}\n")


if __name__ == "__main__":
    print("biz agent — multi-task REPL")
    print("Paste one message or multiple tasks at once. Blank line ends input.")
    print("Type 'exit' to quit.\n")

    while True:
        try:
            user = _repl_input("you: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nbye")
            break

        if not user:
            continue
        if user.lower() in ("exit", "quit"):
            print("bye")
            break

        # Single-line quick messages go straight to ask()
        lines = [l for l in user.splitlines() if l.strip()]
        is_multi = len(lines) > 1 or len(user) > 200

        if not is_multi:
            def _stream(msg: str):
                print(f"  {msg}", flush=True)
            result = ask(user, verbose=False, activity_cb=_stream)
            print(f"\nagent: {result}\n")
            continue

        # Multi-task: plan first
        print("\n[planning...]\n")
        clarifications = ""
        plan_text, tasks = _plan(user, clarifications)
        print(f"agent: {plan_text}\n")

        # If the plan looks like questions (no tasks extracted), collect one round of answers
        if not tasks or len(tasks) < 2:
            try:
                clarifications = input("you: ").strip()
            except (EOFError, KeyboardInterrupt):
                clarifications = ""
            if clarifications and clarifications.lower() not in ("exit", "quit"):
                print("\n[re-planning...]\n")
                plan_text, tasks = _plan(user, clarifications)
                print(f"agent: {plan_text}\n")

        if not tasks:
            tasks = [user]  # fallback: treat whole input as one task

        # Wait for go signal
        start_idx = -1  # default: cancelled
        while True:
            try:
                go = input("you: ").strip()
            except (EOFError, KeyboardInterrupt):
                start_idx = -1
                break
            low = go.lower()
            if low in ("go", "y", "yes", ""):
                start_idx = 0
                break
            if low == "stop":
                break
            import re as _re
            m = _re.match(r"^(\d+)$", go)
            if m:
                start_idx = int(m.group(1)) - 1
                break
            # Feedback before starting — send to agent
            def _stream(msg: str):
                if msg.startswith("[model:"):
                    print(f"  🤖 {msg[7:].strip().rstrip(']')}", flush=True)
                else:
                    print(f"  {msg}", flush=True)
            fb = ask(go, verbose=False, activity_cb=_stream)
            print(f"\nagent: {fb}\n")

        if start_idx < 0:
            continue

        # Execute tasks sequentially — pass accumulated context forward
        completed_summaries = []
        for i, task in enumerate(tasks[start_idx:], start=start_idx + 1):
            ctx = "\n".join(completed_summaries) if completed_summaries else ""
            should_continue = _run_task_interactive(task, i, len(tasks), full_context=ctx)
            completed_summaries.append(f"Task {i} done: {task[:80]}")
            if not should_continue:
                print("[stopped by operator]")
                break

        print("\n[all tasks complete]\n")
