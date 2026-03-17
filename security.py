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
    BIZ_ROOT / "leadgen",
    BIZ_ROOT / "outreach",
    BIZ_ROOT / "agent",
    BIZ_ROOT / "PROJECT.md",
    BIZ_ROOT / "SPEC.md",
]

# Patterns that should never appear in agent output
_SECRET_PATTERNS = [
    r"sk-[a-zA-Z0-9\-_]{20,}",          # OpenAI / OpenRouter keys
    r"AIza[a-zA-Z0-9\-_]{35}",           # Google API keys
    r"hf_[a-zA-Z0-9]{20,}",              # HuggingFace
    r"[a-f0-9]{32}\.[a-zA-Z0-9_\-]{10,}",# Ollama-style keys
    r"(?i)(password|secret|token)\s*=\s*\S+",
]
_SECRET_RE = re.compile("|".join(_SECRET_PATTERNS))


def is_allowed_path(path: str | Path) -> bool:
    target = Path(path).resolve()
    return any(
        target == allowed or str(target).startswith(str(allowed))
        for allowed in ALLOWED_PATHS
    )


def assert_allowed(path: str | Path) -> Path:
    p = Path(path).resolve()
    if not is_allowed_path(p):
        raise PermissionError(f"Path outside allowlist: {p}")
    return p


def scrub_secrets(text: str) -> str:
    return _SECRET_RE.sub("[REDACTED]", text)
