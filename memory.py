"""
agent/memory.py
Structured three-zone memory store.

Zones:
  identity  — persistent agent persona, operator preferences, standing instructions
              (never decays, survives indefinitely)
  knowledge — project facts: lead profiles, niche findings, outreach decisions
              (decays after 30 days without access)
  ops       — daily task state, recent results, session scratchpad
              (decays after 3 days without access)

Short-term conversation history is kept separately (flat list, 50-turn cap).

Public API is backward-compatible with the old flat memory.py:
  get_facts(), set_fact(), get_history(), add_history(), clear_history(),
  summary(), learn(), get_instructions()

New API:
  remember(key, value, zone)  — store with explicit zone
  recall(key)                 — retrieve from any zone, updates access time
  recall_zone(zone)           — all live entries in a zone
  forget(key)                 — delete from any zone
  prune()                     — remove expired entries
  knowledge_summary()         — formatted dump of knowledge zone
"""
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

MEMORY_FILE = Path(__file__).parent / "memory.json"
MAX_HISTORY = 50

# TTL in days per zone (None = never expires)
_ZONE_TTL: dict[str, int | None] = {
    "identity":  None,
    "knowledge": 30,
    "ops":       3,
}

# ── Internal load/save ────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load() -> dict:
    if MEMORY_FILE.exists():
        try:
            raw = json.loads(MEMORY_FILE.read_text())
            # Migrate old flat format {"facts": {...}, "history": [...]}
            if "zones" not in raw:
                raw = _migrate(raw)
            return raw
        except Exception:
            pass
    return _empty()


def _empty() -> dict:
    return {
        "zones": {"identity": {}, "knowledge": {}, "ops": {}},
        "history": [],
    }


def _migrate(old: dict) -> dict:
    """Migrate flat facts dict → identity zone."""
    data = _empty()
    data["history"] = old.get("history", [])
    for k, v in old.get("facts", {}).items():
        data["zones"]["identity"][k] = _entry(v)
    return data


def _entry(value, zone: str = "identity") -> dict:
    return {
        "value": value,
        "zone": zone,
        "created_at": _now_iso(),
        "accessed_at": _now_iso(),
        "access_count": 0,
    }


def _save(data: dict) -> None:
    MEMORY_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False))


def _is_expired(entry: dict) -> bool:
    zone = entry.get("zone", "identity")
    ttl_days = _ZONE_TTL.get(zone)
    if ttl_days is None:
        return False
    accessed = entry.get("accessed_at") or entry.get("created_at")
    if not accessed:
        return False
    try:
        last = datetime.fromisoformat(accessed)
        return datetime.now(timezone.utc) - last > timedelta(days=ttl_days)
    except Exception:
        return False


# ── Zone-aware API ────────────────────────────────────────────────────────────

def remember(key: str, value, zone: str = "knowledge") -> None:
    """Store a value in the given zone. Overwrites existing entry."""
    data = _load()
    existing = None
    for z in data["zones"].values():
        if key in z:
            existing = z[key]
            del z[key]
            break
    if existing:
        existing["value"] = value
        existing["zone"] = zone
        existing["accessed_at"] = _now_iso()
        existing["access_count"] = existing.get("access_count", 0) + 1
        data["zones"][zone][key] = existing
    else:
        data["zones"][zone][key] = _entry(value, zone)
    _save(data)


def recall(key: str) -> object | None:
    """Retrieve a value from any zone. Updates access time (prevents decay)."""
    data = _load()
    for zone_name, zone in data["zones"].items():
        if key in zone:
            entry = zone[key]
            if _is_expired(entry):
                del zone[key]
                _save(data)
                return None
            entry["accessed_at"] = _now_iso()
            entry["access_count"] = entry.get("access_count", 0) + 1
            _save(data)
            return entry["value"]
    return None


def recall_zone(zone: str) -> dict:
    """Return all live (non-expired) entries in a zone as {key: value}."""
    data = _load()
    zone_data = data["zones"].get(zone, {})
    live = {}
    changed = False
    for k, entry in list(zone_data.items()):
        if _is_expired(entry):
            del zone_data[k]
            changed = True
        else:
            live[k] = entry["value"]
    if changed:
        _save(data)
    return live


def forget(key: str) -> bool:
    data = _load()
    for zone in data["zones"].values():
        if key in zone:
            del zone[key]
            _save(data)
            return True
    return False


def prune() -> int:
    """Remove all expired entries. Returns count removed."""
    data = _load()
    removed = 0
    for zone in data["zones"].values():
        expired = [k for k, e in zone.items() if _is_expired(e)]
        for k in expired:
            del zone[k]
            removed += 1
    if removed:
        _save(data)
    return removed


def knowledge_summary() -> str:
    """Formatted dump of the knowledge zone — injected into agent context."""
    entries = recall_zone("knowledge")
    if not entries:
        return ""
    lines = ["Knowledge:"]
    for k, v in entries.items():
        lines.append(f"  {k}: {v}")
    return "\n".join(lines)


# ── Backward-compatible flat API ──────────────────────────────────────────────

def get_facts() -> dict:
    """Return all live facts across all zones as a flat dict."""
    data = _load()
    out = {}
    for zone in data["zones"].values():
        for k, entry in zone.items():
            if not _is_expired(entry):
                out[k] = entry["value"]
    return out


def set_fact(key: str, value) -> None:
    """Store in identity zone (persistent, never decays)."""
    remember(key, value, zone="identity")


def get_history(n: int = 10) -> list:
    return _load()["history"][-n:]


def add_history(role: str, content: str) -> None:
    data = _load()
    data["history"].append({
        "role": role,
        "content": content,
        "ts": _now_iso(),
    })
    if len(data["history"]) > MAX_HISTORY:
        data["history"] = data["history"][-MAX_HISTORY:]
    _save(data)


def clear_history() -> None:
    data = _load()
    data["history"] = []
    _save(data)


def summary() -> str:
    facts = get_facts()
    if not facts:
        return "No stored facts."
    return "\n".join(f"- {k}: {v}" for k, v in facts.items())


def learn(instruction: str, key: str = "") -> str:
    """
    Store a plain-English instruction or preference as a persistent identity fact.
    Called when operator says 'remember', 'always', 'never', 'from now on'.
    """
    if not key:
        words = re.sub(r"[^a-z0-9 ]", "", instruction.lower()).split()
        key = "_".join(words[:4]) or f"note_{int(datetime.now(timezone.utc).timestamp())}"
    remember(key, instruction, zone="identity")
    return f"Remembered: [{key}] {instruction}"


def get_instructions() -> str:
    """Return identity zone entries as formatted string for system prompt injection."""
    entries = recall_zone("identity")
    if not entries:
        return ""
    return "Stored instructions:\n" + "\n".join(f"- {k}: {v}" for k, v in entries.items())
