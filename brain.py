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

_client = None


def _llm() -> OpenAI:
    global _client
    if _client is None:
        _client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=os.environ["OPENROUTER_API_KEY"],
        )
    return _client


FREE_MODELS = [
    # 262k ctx, tool use — best option, try first
    "qwen/qwen3-next-80b-a3b-instruct:free",
    # 262k ctx, tool use — Nemotron super, your 35B active params pick
    "nvidia/nemotron-3-super-120b-a12b:free",
    # 256k ctx, tool use — step flash, fast
    "stepfun/step-3.5-flash:free",
    # 128k ctx, tool use — reliable fallback
    "meta-llama/llama-3.3-70b-instruct:free",
    # 128k ctx, tool use
    "mistralai/mistral-small-3.1-24b-instruct:free",
    # 131k ctx, tool use
    "openai/gpt-oss-120b:free",
]


def _chat(messages: list, tools: list = None) -> object:
    """Try free models in order, with brief pause between attempts."""
    import time
    import openai as _openai
    kwargs = dict(messages=messages, temperature=0.3, max_tokens=2000)
    if tools:
        kwargs["tools"] = tools
        kwargs["tool_choice"] = "auto"

    last_err = None
    for model in FREE_MODELS:
        try:
            return _llm().chat.completions.create(model=model, **kwargs)
        except (_openai.RateLimitError, _openai.NotFoundError) as e:
            last_err = e
            time.sleep(1)   # brief pause before trying next model
            continue
        except Exception:
            raise
    raise RuntimeError(f"All free models exhausted. Last error: {last_err}")


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
    return f"""You are the biz agent — an autonomous assistant managing a personal automation agency.

You have full context of the project and can read/write files, run shell commands, query the leads DB, check outreach status, search the web, and commit/push code.

## Project context
{PROJECT_MD}

## Stored facts
{facts}

## Rules
- Only operate within the biz/ project directory
- Never output API keys, passwords, or secrets
- Prefer reading PROJECT.md and SPEC.md before making decisions
- When asked to build something, write the code, then confirm it works
- Be concise — no filler, no unnecessary explanation
"""


def ask(user_message: str, max_tool_rounds: int = 8, verbose: bool = True) -> str:
    """
    Send a message to the agent. Runs tool loop until done or max_rounds hit.
    Returns the final text response.
    """
    memory.add_history("user", user_message)

    messages = [{"role": "system", "content": _system_prompt()}]
    for h in memory.get_history(n=10)[:-1]:
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
