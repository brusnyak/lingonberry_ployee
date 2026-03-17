"""
agent/executor.py
Sandboxed file read/write and shell execution.
All paths validated against security allowlist before any operation.
"""
import subprocess
from pathlib import Path
from security import assert_allowed, scrub_secrets


def read_file(path: str) -> str:
    p = assert_allowed(path)
    if not p.exists():
        return f"File not found: {p}"
    content = p.read_text(encoding="utf-8", errors="replace")
    return scrub_secrets(content)


def write_file(path: str, content: str) -> str:
    p = assert_allowed(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")
    return f"Written: {p}"


def run_shell(cmd: str, cwd: str = None) -> str:
    """
    Run a shell command. cwd must be within allowlist if provided.
    Returns combined stdout+stderr, secrets scrubbed.
    """
    work_dir = None
    if cwd:
        work_dir = assert_allowed(cwd)

    result = subprocess.run(
        cmd, shell=True, capture_output=True, text=True,
        timeout=60, cwd=work_dir,
    )
    output = (result.stdout + result.stderr).strip()
    return scrub_secrets(output) or "(no output)"


def list_dir(path: str) -> str:
    p = assert_allowed(path)
    if not p.is_dir():
        return f"Not a directory: {p}"
    entries = sorted(p.iterdir())
    lines = []
    for e in entries:
        kind = "d" if e.is_dir() else "f"
        lines.append(f"[{kind}] {e.name}")
    return "\n".join(lines) or "(empty)"
