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
load_dotenv(Path(__file__).parent.parent / ".env")
sys.path.insert(0, str(Path(__file__).parent))

from openai import OpenAI
try:
    from . import memory, executor, policy
    from .tools import web, git, leads, outreach, ops, research, content, reporting
except ImportError:
    import memory
    import executor
    import policy
    from tools import web, git, leads, outreach, ops, research, content, reporting

BIZ_ROOT = Path(__file__).parent.parent
PROJECT_MD = (BIZ_ROOT / "PROJECT.md").read_text()

# Compact summary injected into every prompt — full file available via read_file tool
_PROJECT_SUMMARY = "\n".join(
    line for line in PROJECT_MD.splitlines()
    if line.strip() and not line.startswith("```") and len(line) < 200
)[:2000]  # cap at 2000 chars

_client = None
_ollama_client = None
_MODEL_BACKOFF_UNTIL = {}
_MODEL_FAILURE_COUNTS = {}


def _llm() -> OpenAI:
    global _client
    if _client is None:
        _client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=os.environ["OPENROUTER_API_KEY"],
        )
    return _client


def _ollama() -> OpenAI:
    global _ollama_client
    if _ollama_client is None:
        # Ollama cloud redirects api.ollama.com → ollama.com, use direct URL
        _ollama_client = OpenAI(
            base_url="https://ollama.com/v1",
            api_key=os.environ.get("OLLAMA_API_KEY", ""),
        )
    return _ollama_client


# Ollama cloud models — confirmed working with tool use
OLLAMA_MODELS = [
    "qwen3-next:80b",      # 80b, tool use confirmed
    "nemotron-3-super",    # strong reasoning
    "gemma3:12b",          # reliable fallback
    "ministral-3:8b",      # fast, lightweight
]

FREE_MODELS = [
    # 1T params, 1M ctx, built for agentic use — primary
    "openrouter/hunter-alpha",
    # 262k ctx, tool use — strong fallback
    "qwen/qwen3-next-80b-a3b-instruct:free",
    # 262k ctx — Nemotron super
    "nvidia/nemotron-3-super-120b-a12b:free",
    # 131k ctx, tool use
    "openai/gpt-oss-120b:free",
    # 128k ctx, tool use — reliable
    "meta-llama/llama-3.3-70b-instruct:free",
    # 128k ctx
    "mistralai/mistral-small-3.1-24b-instruct:free",
    # extra fallbacks
    "google/gemma-3-27b-it:free",
    "nousresearch/hermes-3-llama-3.1-405b:free",
    "openai/gpt-oss-20b:free",
]


def _chat(messages: list, tools: list = None) -> object:
    """Try OpenRouter free models, then fall back to Ollama."""
    import time
    import openai as _openai
    kwargs = dict(messages=messages, temperature=0.3, max_tokens=2000)
    if tools:
        kwargs["tools"] = tools
        kwargs["tool_choice"] = "auto"

    last_err = None

    # 1. Try OpenRouter free models
    for model in FREE_MODELS:
        now = time.time()
        if _MODEL_BACKOFF_UNTIL.get(model, 0) > now:
            continue
        try:
            return _llm().chat.completions.create(model=model, **kwargs)
        except (_openai.RateLimitError, _openai.NotFoundError, _openai.BadRequestError, _openai.APIStatusError) as e:
            last_err = e
            _MODEL_FAILURE_COUNTS[model] = _MODEL_FAILURE_COUNTS.get(model, 0) + 1
            _MODEL_BACKOFF_UNTIL[model] = now + min(900, 30 * _MODEL_FAILURE_COUNTS[model])
            time.sleep(1)
            continue
        except Exception:
            raise

    # 2. Fall back to Ollama
    for model in OLLAMA_MODELS:
        now = time.time()
        if _MODEL_BACKOFF_UNTIL.get(model, 0) > now:
            continue
        try:
            return _ollama().chat.completions.create(model=model, **kwargs)
        except (_openai.RateLimitError, _openai.NotFoundError, _openai.BadRequestError, _openai.APIStatusError) as e:
            last_err = e
            _MODEL_FAILURE_COUNTS[model] = _MODEL_FAILURE_COUNTS.get(model, 0) + 1
            _MODEL_BACKOFF_UNTIL[model] = now + min(900, 30 * _MODEL_FAILURE_COUNTS[model])
            time.sleep(1)
            continue
        except Exception as e:
            last_err = e
            continue

    raise RuntimeError(f"All models exhausted (OpenRouter + Ollama). Last error: {last_err}")


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
            "description": "Search leads by name, category, or address.",
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
            "name": "outreach_generate_drafts",
            "description": "Generate and queue outreach drafts for qualified leads without outreach.",
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
        elif name == "git_status":
            return git.status(args["repo_path"])
        elif name == "git_commit_push":
            return git.commit_and_push(args["repo_path"], args["message"])
        elif name == "memory_set":
            memory.set_fact(args["key"], args["value"])
            return f"Stored: {args['key']} = {args['value']}"
        elif name == "memory_get":
            return memory.summary()
        elif name == "ops_jobs_summary":
            return ops.jobs_summary()
        elif name == "ops_jobs_list":
            return ops.jobs_list()
        elif name == "ops_recent_events":
            return ops.recent_events(args.get("n", 10))
        else:
            return f"Unknown tool: {name}"
    except Exception as e:
        return f"Tool error ({name}): {e}"


def _system_prompt() -> str:
    facts = memory.summary()
    return f"""You are the biz agent — autonomous operator for a one-person automation agency.

OUTPUT FORMAT — NON-NEGOTIABLE:
You send messages via Telegram plain text. No markdown rendering happens.
Never use: ** bold **, * italic *, # headers, ``` code blocks ```, | tables |, --- dividers.
Use only: plain sentences, numbered lists (1. 2. 3.), dashes (- item), line breaks.
Emoji is fine sparingly. Keep responses short and scannable.

CAPABILITIES (answer directly without tools when asked):
You can: check leads stats and search leads, query outreach pipeline (sent/replies/queue), generate and approve outreach drafts, schedule and send emails, poll and classify inbound replies, draft and send reply emails, manage niche research and shortlist, run web searches, read/write project files, run shell commands, commit/push git repos, manage Victor content queue (plan/generate/approve/publish posts), check ops jobs and events, store/recall facts in memory.

You have full access to the project: read/write files, run shell commands, query leads and outreach DBs, search the web, commit/push code, remember facts across sessions.

Project context (summary — use read_file for full detail):
{_PROJECT_SUMMARY}

Stored facts:
{facts}

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

Behaviour:
- Never output API keys, passwords, or secrets
- When asked about capabilities: answer directly from the CAPABILITIES section above, no tools needed
- When asked to check state: read context file first, then answer directly
- Do not assume dental is the winning niche unless research and validation support it
- When asked to build something: write the code, confirm it works, report what you did
- During queued overnight tasks, respect the task contract and approval mode. If an outward action needs approval, stop and return NEEDS_INPUT with the exact blocker.
- When you take action: say what you did and the result
- Be concise — no filler, no aimless exploration
"""


def ask(user_message: str, max_tool_rounds: int = 30, verbose: bool = True,
        activity_cb=None, confirm_cb=None) -> str:
    """
    Send a message to the agent. Runs tool loop until done or max_rounds hit.
    Returns the final text response.

    activity_cb: optional callable(str) — called with each tool action line
                 so the Telegram bot can stream progress to the chat in real time.
    confirm_cb: optional callable(tool_name, args) -> bool — called before any
                APPROVAL_REQUIRED tool. Return True to proceed, False to cancel.
                If None, approval-required tools are blocked with a message.
    """
    memory.add_history("user", user_message)

    messages = [{"role": "system", "content": _system_prompt()}]
    for h in memory.get_history(n=6)[:-1]:
        messages.append({"role": h["role"], "content": h["content"]})
    messages.append({"role": "user", "content": user_message})

    def _log(msg: str):
        if verbose:
            print(msg, flush=True)
        if activity_cb:
            try:
                activity_cb(msg)
            except Exception:
                pass

    recent_tool_signatures = []
    for round_n in range(max_tool_rounds):
        _log(f"[round {round_n+1}] calling LLM...")
        resp = _chat(messages, tools=TOOLS)
        msg = resp.choices[0].message
        _log(f"[model: {resp.model}]")

        inline_content = (msg.content or "").strip()

        if not msg.tool_calls:
            answer = inline_content
            memory.add_history("assistant", answer)
            return answer

        for tc in msg.tool_calls:
            _log(f"[tool] {tc.function.name}({tc.function.arguments[:80]})")
            recent_tool_signatures.append((tc.function.name, tc.function.arguments[:120]))
        if len(recent_tool_signatures) >= 6:
            window = recent_tool_signatures[-6:]
            if len(set(window)) <= 2:
                answer = "I am looping on the same tools without making progress. I need either a narrower query or a different source/tool path."
                memory.add_history("assistant", answer)
                return answer

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

            # Check if this tool needs confirmation
            tool_policy = policy.policy_for(tc.function.name)
            if tool_policy in (policy.APPROVAL_REQUIRED, policy.APPROVED_EXECUTION):
                if confirm_cb is not None:
                    confirmed = confirm_cb(tc.function.name, args)
                    if not confirmed:
                        result = f"Action cancelled by operator: {tc.function.name}"
                        _log(f"[cancelled] {tc.function.name}")
                    else:
                        result = _dispatch(tc.function.name, args)
                else:
                    # No confirm_cb — surface as NEEDS_INPUT
                    prompt_text = _format_confirm_prompt(tc.function.name, args)
                    result = f"NEEDS_INPUT: {prompt_text}"
                    _log(f"[needs confirm] {tc.function.name}")
            else:
                result = _dispatch(tc.function.name, args)

            _log(f"[result] {str(result)[:120]}")
            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": str(result),
            })

    return "Max tool rounds reached."


def run_next_task(verbose: bool = False, activity_cb=None) -> tuple[str, str] | None:
    """
    Pick the next pending task, run it, update status.
    Returns (task_id, result) or None if no pending tasks.
    """
    import tasks as task_store
    task = task_store.claim_next(owner="brain")
    if not task:
        return None

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
        result = ask(prompt, verbose=verbose, activity_cb=activity_cb)
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


if __name__ == "__main__":
    # simple REPL for testing
    print("biz agent — type 'exit' to quit\n")
    while True:
        try:
            user = input("you: ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if user.lower() in ("exit", "quit"):
            break
        if not user:
            continue
        print(f"\nagent: {ask(user, verbose=False)}\n")
