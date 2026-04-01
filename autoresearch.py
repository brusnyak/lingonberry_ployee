"""
agent/autoresearch.py
Karpathy-style autoresearch loop for the biz system.

Pattern:
  program.md  — human writes the goal, metric, and what's allowed to change
  experiment  — agent reads program.md, modifies the target file, runs the experiment
  measure     — collect the metric (reply rate, tool rounds, etc.)
  keep/revert — if metric improved, keep the change; otherwise revert
  log         — append-only experiment log survives restarts

Usage:
  from agent.autoresearch import run_experiment, run_loop
  run_loop("agent/programs/outreach_copy.md", max_experiments=10)

Or from the REPL:
  python -m agent.autoresearch agent/programs/outreach_copy.md
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

BIZ_ROOT = Path(__file__).parent.parent

# ── Venv bootstrap ────────────────────────────────────────────────────────────
# When run as `python -m agent.autoresearch` with the system Python, the agent
# venv packages (openai, dotenv, etc.) won't be on sys.path.
# Detect the venv and inject its site-packages before any imports that need them.
_VENV = BIZ_ROOT / "agent" / ".venv"
if _VENV.exists():
    import site as _site
    _py = f"python{sys.version_info.major}.{sys.version_info.minor}"
    for _candidate in [
        _VENV / "lib" / _py / "site-packages",
        _VENV / "lib" / "site-packages",          # some venvs flatten this
    ]:
        if _candidate.exists() and str(_candidate) not in sys.path:
            sys.path.insert(0, str(_candidate))
            break

# Ensure agent/ and biz root are importable regardless of cwd or run method
_AGENT_DIR = str(Path(__file__).parent)
for _p in [_AGENT_DIR, str(BIZ_ROOT)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

# Load .env so API keys are available when brain._chat initialises
try:
    from dotenv import load_dotenv as _load_dotenv
    _load_dotenv(BIZ_ROOT / ".env")
except ImportError:
    pass  # dotenv not available — keys must be set in environment

# ── Program spec ─────────────────────────────────────────────────────────────

def load_program(program_path: str | Path) -> dict:
    """
    Parse a program.md file into a structured spec.

    Expected sections (all optional except goal and metric):
      ## Goal
      ## Target file
      ## Metric
      ## Baseline
      ## Constraints
      ## Experiment budget
    """
    path = Path(program_path)
    if not path.is_absolute():
        path = BIZ_ROOT / path
    text = path.read_text()

    def _section(name: str) -> str:
        import re
        m = re.search(
            rf"##\s+{re.escape(name)}\s*\n(.*?)(?=\n##|\Z)",
            text, re.IGNORECASE | re.DOTALL
        )
        return m.group(1).strip() if m else ""

    return {
        "path": str(path),
        "goal":            _section("Goal") or text[:200],
        "target_file":     _section("Target file"),
        "metric":          _section("Metric"),
        "baseline":        _section("Baseline"),
        "constraints":     _section("Constraints"),
        "budget_seconds":  int(_section("Experiment budget") or "300"),
        "raw":             text,
    }


# ── Experiment log ────────────────────────────────────────────────────────────

def _log_path(program_path: str | Path) -> Path:
    p = Path(program_path)
    return p.parent / (p.stem + "_log.jsonl")


def _append_log(program_path: str | Path, entry: dict) -> None:
    log = _log_path(program_path)
    with log.open("a") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def read_log(program_path: str | Path) -> list[dict]:
    log = _log_path(program_path)
    if not log.exists():
        return []
    entries = []
    for line in log.read_text().splitlines():
        line = line.strip()
        if line:
            try:
                entries.append(json.loads(line))
            except Exception:
                pass
    return entries


def best_result(program_path: str | Path) -> dict | None:
    """Return the experiment entry with the highest metric value."""
    entries = [e for e in read_log(program_path) if e.get("kept") and e.get("metric_value") is not None]
    if not entries:
        return None
    return max(entries, key=lambda e: float(e["metric_value"]))


# ── File backup/restore ───────────────────────────────────────────────────────

def _backup(target: str | Path) -> Path:
    src = Path(target)
    if not src.is_absolute():
        src = BIZ_ROOT / src
    bak = src.with_suffix(src.suffix + ".autoresearch_bak")
    shutil.copy2(src, bak)
    return bak


def _restore(target: str | Path, backup: Path) -> None:
    src = Path(target)
    if not src.is_absolute():
        src = BIZ_ROOT / src
    shutil.copy2(backup, src)


# ── LLM call (reuses brain._chat) ────────────────────────────────────────────

# Regex patterns that identify copy-bearing sections worth modifying
_COPY_SECTION_PATTERNS = [
    r'(LANG_PACKS\s*=\s*\{.*?\n\})',           # language phrase banks
    r'(DEFAULT_SUBJECTS\s*=\s*\{.*?\n\})',      # subject lines
    r'(PAIN_OPENERS\s*=\s*\{.*?\n\})',          # pain-specific openers
    r'(NICHE_OPENERS\s*=\s*\{.*?\n\})',         # niche openers
]


def _extract_copy_section(code: str) -> tuple[str, int, int]:
    """
    Extract the most relevant copy section from the file.
    Returns (section_text, start_char, end_char).
    Falls back to first 3000 chars if no pattern matches.
    """
    import re
    for pattern in _COPY_SECTION_PATTERNS:
        m = re.search(pattern, code, re.DOTALL)
        if m:
            return m.group(0), m.start(), m.end()
    # Fallback: first 3000 chars (covers imports + first dict)
    chunk = code[:3000]
    return chunk, 0, len(chunk)


def _llm_modify(program: dict, current_code: str, experiment_n: int, history: list[dict]) -> str:
    """
    Ask the LLM to propose a modification to the target file.
    Sends only the relevant copy section (not the whole file) to stay within
    the model's output token budget, then splices the change back in.
    Returns the complete new file content as a string.
    """
    from brain import _chat  # type: ignore

    history_block = ""
    if history:
        lines = ["Previous experiments (most recent first):"]
        for e in reversed(history[-5:]):
            kept = "kept" if e.get("kept") else "reverted"
            lines.append(
                f"  exp {e['n']}: metric={e.get('metric_value', '?')} | {kept} | "
                f"change={e.get('change_summary', '?')[:80]}"
            )
        history_block = "\n".join(lines)

    section, start, end = _extract_copy_section(current_code)

    prompt = f"""You are running experiment {experiment_n} for the following research program.

PROGRAM GOAL:
{program['goal']}

METRIC TO OPTIMIZE:
{program['metric']}

BASELINE:
{program['baseline'] or 'Not yet established.'}

CONSTRAINTS (do not violate these):
{program['constraints'] or 'None specified.'}

{history_block}

SECTION TO MODIFY (from {program['target_file']}):
{section}

Your task:
1. Propose ONE specific, testable change to the section above.
2. Focus on copy changes: subject lines, opening sentences, CTAs, phrase variations.
3. Return ONLY the complete modified section — same structure, same variable names.
4. No explanation, no markdown fences, no extra text.
5. Make the change small and clearly attributable.

Return the modified section now:"""

    msgs = [
        {"role": "system", "content": (
            "You are a copywriting assistant optimizing cold email response rates. "
            "Return only the modified Python code section, no explanation."
        )},
        {"role": "user", "content": prompt},
    ]
    resp = _chat(msgs, max_tokens=3000)
    modified_section = (resp.choices[0].message.content or "").strip()

    # Strip markdown fences if the model added them
    if modified_section.startswith("```"):
        lines = modified_section.splitlines()
        modified_section = "\n".join(
            l for l in lines if not l.startswith("```")
        ).strip()

    if not modified_section or modified_section == section:
        # Debug: show first 200 chars of what the model returned
        preview = (modified_section or "(empty)")[:200]
        print(f"  [debug] model returned ({len(modified_section)} chars): {preview!r}")
        return current_code  # signal no change

    # Splice the modified section back into the full file
    return current_code[:start] + modified_section + current_code[end:]

def _llm_summarize_change(original: str, modified: str) -> str:
    """One-line summary of what changed between two file versions."""
    from brain import _chat  # type: ignore

    msgs = [
        {"role": "system", "content": "Summarize code changes in one short sentence."},
        {"role": "user", "content": f"Original:\n{original[:1000]}\n\nModified:\n{modified[:1000]}"},
    ]
    try:
        resp = _chat(msgs)
        return (resp.choices[0].message.content or "").strip()[:120]
    except Exception:
        return "(summary unavailable)"


# ── Metric collection ─────────────────────────────────────────────────────────

def collect_metric(program: dict) -> float | None:
    """
    Run the metric collection command from the program spec and return a float.

    The metric section can be:
      - A plain description (human-evaluated, returns None — manual mode)
      - A shell command prefixed with `cmd:` that prints a float to stdout
        e.g. `cmd: python agent/metrics/reply_rate.py`
    """
    metric_spec = program.get("metric", "")
    if not metric_spec.lower().startswith("cmd:"):
        # Manual metric — operator evaluates
        return None

    cmd = metric_spec[4:].strip()
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True,
            timeout=120, cwd=BIZ_ROOT,
        )
        output = result.stdout.strip()
        return float(output)
    except Exception as e:
        print(f"  [metric error] {e}")
        return None


# ── Single experiment ─────────────────────────────────────────────────────────

def run_experiment(
    program_path: str | Path,
    experiment_n: int = 1,
    dry_run: bool = False,
    verbose: bool = True,
) -> dict:
    """
    Run one experiment cycle:
      1. Load program spec
      2. Read current target file
      3. Ask LLM to propose a modification
      4. Apply modification
      5. Collect metric
      6. Keep if improved, revert otherwise
      7. Log result

    Returns the experiment result dict.
    """
    program = load_program(program_path)
    target_path = BIZ_ROOT / program["target_file"]

    if not target_path.exists():
        raise FileNotFoundError(f"Target file not found: {target_path}")

    original_code = target_path.read_text()
    history = read_log(program_path)

    def _log(msg: str):
        if verbose:
            print(f"  {msg}", flush=True)

    _log(f"exp {experiment_n} — reading {program['target_file']}")

    # Get baseline metric before modification
    baseline_metric = collect_metric(program)
    _log(f"baseline metric: {baseline_metric}")

    # Ask LLM to propose a change
    _log("asking LLM for modification...")
    modified_code = _llm_modify(program, original_code, experiment_n, history)

    if modified_code == original_code or not modified_code.strip():
        _log("LLM returned no change — skipping")
        result = {
            "n": experiment_n,
            "ts": datetime.now(timezone.utc).isoformat(),
            "program": str(program_path),
            "target_file": program["target_file"],
            "kept": False,
            "metric_value": baseline_metric,
            "change_summary": "no change proposed",
            "skipped": True,
        }
        _append_log(program_path, result)
        return result

    change_summary = _llm_summarize_change(original_code, modified_code)
    _log(f"change: {change_summary}")

    if dry_run:
        _log("[dry run] would apply change but not writing")
        result = {
            "n": experiment_n,
            "ts": datetime.now(timezone.utc).isoformat(),
            "program": str(program_path),
            "target_file": program["target_file"],
            "kept": False,
            "metric_value": baseline_metric,
            "change_summary": change_summary,
            "dry_run": True,
        }
        _append_log(program_path, result)
        return result

    # Backup and apply
    backup = _backup(target_path)
    target_path.write_text(modified_code)
    _log("change applied")

    # Collect metric after modification
    new_metric = collect_metric(program)
    _log(f"new metric: {new_metric}")

    # Decide keep or revert
    kept = False
    if new_metric is None:
        # Manual metric — operator decides; default keep for now, log for review
        kept = True
        _log("metric is manual — keeping change for operator review")
    elif baseline_metric is None:
        kept = True
        _log("no baseline — keeping change")
    elif new_metric > baseline_metric:
        kept = True
        _log(f"metric improved {baseline_metric} → {new_metric} — keeping")
    else:
        _log(f"metric did not improve {baseline_metric} → {new_metric} — reverting")
        _restore(target_path, backup)

    # Clean up backup
    try:
        backup.unlink()
    except Exception:
        pass

    result = {
        "n": experiment_n,
        "ts": datetime.now(timezone.utc).isoformat(),
        "program": str(program_path),
        "target_file": program["target_file"],
        "kept": kept,
        "baseline_metric": baseline_metric,
        "metric_value": new_metric,
        "change_summary": change_summary,
    }
    _append_log(program_path, result)
    return result


# ── Loop ─────────────────────────────────────────────────────────────────────

def run_loop(
    program_path: str | Path,
    max_experiments: int = 10,
    pause_seconds: int = 30,
    dry_run: bool = False,
    verbose: bool = True,
) -> list[dict]:
    """
    Run the autoresearch loop: experiment → measure → keep/revert → repeat.
    Append-only log survives restarts — pass the same program_path to resume.
    """
    history = read_log(program_path)
    start_n = len(history) + 1
    results = []

    print(f"\nautoresearch loop — {program_path}")
    print(f"starting at experiment {start_n}, max {max_experiments}")
    print(f"dry_run={dry_run}\n")

    for i in range(max_experiments):
        n = start_n + i
        print(f"\n{'─'*50}")
        print(f"experiment {n}/{start_n + max_experiments - 1}")
        print('─'*50)
        try:
            result = run_experiment(program_path, n, dry_run=dry_run, verbose=verbose)
            results.append(result)
            if result.get("kept"):
                print(f"  ✅ kept — metric: {result.get('metric_value')}")
            else:
                print(f"  ↩ reverted — metric: {result.get('metric_value')}")
        except KeyboardInterrupt:
            print("\n[stopped by operator]")
            break
        except Exception as e:
            print(f"  ❌ experiment failed: {e}")
            results.append({"n": n, "error": str(e), "kept": False})

        if i < max_experiments - 1:
            print(f"  waiting {pause_seconds}s before next experiment...")
            time.sleep(pause_seconds)

    best = best_result(program_path)
    if best:
        print(f"\nbest result: exp {best['n']} | metric={best['metric_value']} | {best['change_summary']}")
    return results


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Autoresearch loop")
    parser.add_argument("program", help="Path to program.md")
    parser.add_argument("--max", type=int, default=5, help="Max experiments")
    parser.add_argument("--pause", type=int, default=30, help="Seconds between experiments")
    parser.add_argument("--dry-run", action="store_true", help="Propose changes but don't apply")
    args = parser.parse_args()
    run_loop(args.program, max_experiments=args.max, pause_seconds=args.pause, dry_run=args.dry_run)
