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
import memory
import executor
from tools import web, git, leads, outreach

BIZ_ROOT = Path(__file__).parent.parent
PROJECT_MD = (BIZ_ROOT / "PROJECT.md").read_text()

# Compact summary injected into every prompt — full file available via read_file tool
_PROJECT_SUMMARY = "\n".join(
    line for line in PROJECT_MD.splitlines()
    if line.strip() and not line.startswith("```") and len(line) < 200
)[:2000]  # cap at 2000 chars

_client = None
_ollama_client = None


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
    # 262k ctx, tool use — primary
    "qwen/qwen3-next-80b-a3b-instruct:free",
    # 262k ctx, tool use — Nemotron super
    "nvidia/nemotron-3-super-120b-a12b:free",
    # 131k ctx, tool use
    "openai/gpt-oss-120b:free",
    # 128k ctx, tool use — reliable
    "meta-llama/llama-3.3-70b-instruct:free",
    # 128k ctx, tool use
    "mistralai/mistral-small-3.1-24b-instruct:free",
    # 128k ctx — extra fallbacks
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
        try:
            return _llm().chat.completions.create(model=model, **kwargs)
        except (_openai.RateLimitError, _openai.NotFoundError, _openai.BadRequestError, _openai.APIStatusError) as e:
            last_err = e
            time.sleep(1)
            continue
        except Exception:
            raise

    # 2. Fall back to Ollama
    for model in OLLAMA_MODELS:
        try:
            return _ollama().chat.completions.create(model=model, **kwargs)
        except (_openai.RateLimitError, _openai.NotFoundError, _openai.BadRequestError, _openai.APIStatusError) as e:
            last_err = e
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
]


def _dispatch(name: str, args: dict) -> str:
    try:
        if name == "web_search":
            return web.search(args["query"], args.get("max_results", 5))
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
        elif name == "outreach_stats":
            return outreach.stats()
        elif name == "outreach_replies":
            return outreach.recent_replies(args.get("n", 5))
        elif name == "git_status":
            return git.status(args["repo_path"])
        elif name == "git_commit_push":
            return git.commit_and_push(args["repo_path"], args["message"])
        elif name == "memory_set":
            memory.set_fact(args["key"], args["value"])
            return f"Stored: {args['key']} = {args['value']}"
        elif name == "memory_get":
            return memory.summary()
        else:
            return f"Unknown tool: {name}"
    except Exception as e:
        return f"Tool error ({name}): {e}"


def _system_prompt() -> str:
    facts = memory.summary()
    return f"""You are the biz agent — autonomous operator for a one-person automation agency.

You have full access to the project: read/write files, run shell commands, query leads and outreach DBs, search the web, commit/push code, remember facts across sessions.

## Project context (summary — use read_file tool for full detail)
{_PROJECT_SUMMARY}

## Stored facts
{facts}

## Scope — you operate across the full workspace
- leadgen/ — scraping, enrichment, validation, qualified leads DB
- outreach/ — email pipeline, reply tracking, message generation
- agent/ — your brain, tools, memory
- telegram/ — this bot
- notes/ — ideas, research, planning
- him/ — Victor persona, content strategy, image prompts

## Capabilities
- Web search via DuckDuckGo
- Read, write, edit any file in the project
- Run shell commands (make, python, git, etc.)
- Query leads DB — stats, top leads, search
- Query outreach DB — sent, replies, classifications
- Commit and push any repo
- Remember facts across conversations
- Plan and execute multi-step tasks autonomously
- Content creation, outreach copy, strategy
- Debug and develop code across all modules

## FORMATTING RULES — CRITICAL
You are sending messages via Telegram. Telegram does NOT render markdown.
NEVER use: tables, | pipes |, --- dividers, **bold**, *italic*, `backticks`, # headers, or any markdown syntax.
ALWAYS use: plain text only. Use line breaks for structure. Use emoji sparingly for visual separation.
Keep responses short and scannable. Use numbered lists or simple dashes for lists.

## Behaviour rules
- Never output API keys, passwords, or secrets
- Read PROJECT.md and SPEC.md before making decisions on what to build
- When asked to build something, write the code and confirm it works
- When you take action, say what you did and what the result was
- Be concise — no filler
"""


def ask(user_message: str, max_tool_rounds: int = 20, verbose: bool = True) -> str:
    """
    Send a message to the agent. Runs tool loop until done or max_rounds hit.
    Returns the final text response.
    """
    memory.add_history("user", user_message)

    messages = [{"role": "system", "content": _system_prompt()}]
    # keep last 6 turns only — reduces tokens per request significantly
    for h in memory.get_history(n=6)[:-1]:
        messages.append({"role": h["role"], "content": h["content"]})
    messages.append({"role": "user", "content": user_message})

    for round_n in range(max_tool_rounds):
        if verbose:
            print(f"  [round {round_n+1}] calling LLM...", flush=True)
        resp = _chat(messages, tools=TOOLS)
        msg = resp.choices[0].message
        model_used = resp.model
        if verbose:
            print(f"  [model: {model_used}]", flush=True)

        inline_content = (msg.content or "").strip()

        if not msg.tool_calls:
            answer = inline_content
            memory.add_history("assistant", answer)
            return answer

        if verbose:
            for tc in msg.tool_calls:
                print(f"  [tool] {tc.function.name}({tc.function.arguments[:80]})", flush=True)

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
            result = _dispatch(tc.function.name, args)
            if verbose:
                print(f"  [result] {str(result)[:120]}", flush=True)
            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": str(result),
            })

    return "Max tool rounds reached."


def run_next_task(verbose: bool = False) -> tuple[str, str] | None:
    """
    Pick the next pending task, run it, update status.
    Returns (task_id, result) or None if no pending tasks.
    """
    import tasks as task_store
    pending = task_store.get_pending()
    if not pending:
        return None

    task = pending[0]
    task_store.update(task["id"], "running")

    prompt = (
        f"You are working on a queued task. Complete it fully and autonomously.\n\n"
        f"Task: {task['description']}\n\n"
        f"When done, summarize what you did in 2-3 sentences. "
        f"If you need human input to proceed, start your reply with NEEDS_INPUT:"
    )

    try:
        result = ask(prompt, verbose=verbose)
        if result.startswith("NEEDS_INPUT:"):
            task_store.update(task["id"], "needs_input", result)
        else:
            task_store.update(task["id"], "done", result)
        return task["id"], result
    except Exception as e:
        err = f"Task failed: {e}"
        task_store.update(task["id"], "failed", err)
        return task["id"], err


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
