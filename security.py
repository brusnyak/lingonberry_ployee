"""
agent/security.py
Path allowlist and secret scrubbing.
All file/shell operations go through here before execution.
"""
import os
import re
from pathlib import Path

# Absolute path to biz/ root
BIZ_ROOT = Path(__file__).parent.parent.resolve()

ALLOWED_PATHS = [
    BIZ_ROOT,               # root itself — for list_dir(".") and root-level files
    BIZ_ROOT / "leadgen",
    BIZ_ROOT / "outreach",
    BIZ_ROOT / "agent",
    BIZ_ROOT / "telegram",
    BIZ_ROOT / "notes",
    BIZ_ROOT / "him",
    BIZ_ROOT / "content",
    BIZ_ROOT / "PROJECT.md",
    BIZ_ROOT / "SPEC.md",
    BIZ_ROOT / "context_next_chat.md",
]

# Files that must never be read or cat'd
_BLOCKED_FILES = {
    BIZ_ROOT / ".env",
    BIZ_ROOT / ".gitignore",
}

# Patterns that should never appear in agent output
_SECRET_PATTERNS = [
    r"sk-[a-zA-Z0-9\-_]{20,}",            # OpenAI / OpenRouter keys
    r"AIza[a-zA-Z0-9\-_]{35}",            # Google API keys
    r"hf_[a-zA-Z0-9]{20,}",               # HuggingFace
    r"[a-f0-9]{32}\.[a-zA-Z0-9_\-]{10,}", # Ollama-style keys
    r"(?:password|secret|token)\s*=\s*\S+",
]
_SECRET_RE = re.compile("|".join(_SECRET_PATTERNS), re.IGNORECASE)


def is_allowed_path(path: str | Path) -> bool:
    target = Path(path).resolve()
    return any(
        target == allowed or str(target).startswith(str(allowed))
        for allowed in ALLOWED_PATHS
    )


def assert_allowed(path: str | Path) -> Path:
    p = Path(path).resolve()
    if p in _BLOCKED_FILES:
        raise PermissionError(f"File is blocked: {p.name}")
    if not is_allowed_path(p):
        raise PermissionError(f"Path outside allowlist: {p}")
    return p


# Shell commands that could leak secrets — blocked regardless of path
_BLOCKED_SHELL_PATTERNS = re.compile(
    r"\b(cat|less|more|head|tail|nano|vim|vi|open)\b.*\.env"
    r"|\benv\b|\bprintenv\b|\bexport\b|\bset\b\s*$"
    r"|\becho\s+\$",
    re.IGNORECASE,
)


def assert_safe_shell(cmd: str) -> None:
    if _BLOCKED_SHELL_PATTERNS.search(cmd):
        raise PermissionError(f"Shell command blocked (potential secret leak): {cmd[:80]}")


def scrub_secrets(text: str) -> str:
    return _SECRET_RE.sub("[REDACTED]", text)
