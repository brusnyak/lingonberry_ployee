"""
agent/tools/git.py
Git operations for biz sub-projects.
Only operates within the biz/ allowlist.
"""
import subprocess
from pathlib import Path
from security import assert_allowed


def _run(cmd: list[str], cwd: Path) -> str:
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=30)
    out = (result.stdout + result.stderr).strip()
    return out or "(no output)"


def status(repo_path: str) -> str:
    path = assert_allowed(repo_path)
    return _run(["git", "status", "--short"], path)


def commit_and_push(repo_path: str, message: str) -> str:
    path = assert_allowed(repo_path)
    _run(["git", "add", "-A"], path)
    commit_out = _run(["git", "commit", "-m", message], path)
    push_out = _run(["git", "push"], path)
    return f"{commit_out}\n{push_out}".strip()


def log(repo_path: str, n: int = 5) -> str:
    path = assert_allowed(repo_path)
    return _run(["git", "log", f"-{n}", "--oneline"], path)
