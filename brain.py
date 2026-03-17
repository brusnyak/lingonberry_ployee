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


def ask(user_message: str, max_tool_rounds: int = 8) -> str:
    """
    Send a message to the agent. Runs tool loop until done or max_rounds hit.
    Returns the final text response.
    """
    memory.add_history("user", user_message)

    messages = [{"role": "system", "content": _system_prompt()}]
    # inject recent history for continuity
    for h in memory.get_history(n=10)[:-1]:  # exclude the one we just added
        messages.append({"role": h["role"], "content": h["content"]})
    messages.append({"role": "user", "content": user_message})

    for _ in range(max_tool_rounds):
        resp = _llm().chat.completions.create(
            model="google/gemini-2.5-pro-exp-03-25:free",
            messages=messages,
            tools=TOOLS,
            tool_choice="auto",
            temperature=0.3,
            max_tokens=2000,
        )
        msg = resp.choices[0].message

        if not msg.tool_calls:
            # final answer
            answer = msg.content or ""
            memory.add_history("assistant", answer)
            return answer

        # execute tool calls
        messages.append(msg)
        for tc in msg.tool_calls:
            args = json.loads(tc.function.arguments)
            result = _dispatch(tc.function.name, args)
            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": result,
            })

    return "Max tool rounds reached. Last response may be incomplete."


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
        print(f"\nagent: {ask(user)}\n")
