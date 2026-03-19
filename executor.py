"""
agent/executor.py
Sandboxed file read/write and shell execution.
All paths validated against security allowlist before any operation.
"""
import subprocess
from pathlib import Path

try:
    from .security import assert_allowed, assert_safe_shell, scrub_secrets
except ImportError:
    from security import assert_allowed, assert_safe_shell, scrub_secrets


BIZ_ROOT = Path(__file__).parent.parent.resolve()


def _resolve(path: str) -> Path:
    """
    Resolve a path relative to BIZ_ROOT if not absolute.
    This ensures the agent always operates from the project root
    regardless of which directory the bot process was started from.
    """
    p = Path(path)
    if not p.is_absolute():
        p = BIZ_ROOT / p
    return assert_allowed(p)


def read_file(path: str) -> str:
    p = _resolve(path)
    if not p.exists():
        return f"File not found: {p}"
    content = p.read_text(encoding="utf-8", errors="replace")
    return scrub_secrets(content[:8000])  # cap to avoid token blowout


def write_file(path: str, content: str) -> str:
    p = _resolve(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")
    return f"Written: {p}"


def run_shell(cmd: str, cwd: str = None) -> str:
    """
    Run a shell command. cwd defaults to BIZ_ROOT if not provided.
    Returns combined stdout+stderr, secrets scrubbed.
    """
    assert_safe_shell(cmd)  # block secret-leaking commands

    if cwd:
        work_dir = assert_allowed(BIZ_ROOT / cwd if not Path(cwd).is_absolute() else cwd)
    else:
        work_dir = BIZ_ROOT

    result = subprocess.run(
        cmd, shell=True, capture_output=True, text=True,
        timeout=120, cwd=work_dir,
    )
    output = (result.stdout + result.stderr).strip()
    return scrub_secrets(output[:4000]) or "(no output)"


def code_search(pattern: str, path: str = ".") -> str:
    target = _resolve(path)
    result = subprocess.run(
        ["rg", "-n", "--hidden", "--glob", "!.git", pattern, str(target)],
        capture_output=True,
        text=True,
        timeout=60,
        cwd=BIZ_ROOT,
    )
    output = (result.stdout + result.stderr).strip()
    return scrub_secrets(output[:4000]) or "(no matches)"


def run_tests(cmd: str, cwd: str = None) -> str:
    return run_shell(cmd, cwd=cwd)


def list_dir(path: str) -> str:
    p = _resolve(path)
    if not p.is_dir():
        return f"Not a directory: {p}"
    entries = sorted(p.iterdir())
    lines = []
    for e in entries:
        kind = "d" if e.is_dir() else "f"
        lines.append(f"[{kind}] {e.name}")
    return "\n".join(lines) or "(empty)"
