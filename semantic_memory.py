"""
Semantic memory search using simple embeddings.
Falls back to substring matching if sentence-transformers not available.
"""
from pathlib import Path
from typing import List, Tuple
import json
import re

# Try to use embeddings, but don't fail if not available
try:
    from sentence_transformers import SentenceTransformer, util
    _model = SentenceTransformer('all-MiniLM-L6-v2')
    _has_embeddings = True
except Exception:
    _has_embeddings = False

from memory import recall_zone, _load


def _simple_similarity(query: str, text: str) -> float:
    """Fallback: token overlap similarity."""
    query_tokens = set(query.lower().split())
    text_tokens = set(text.lower().split())
    if not query_tokens:
        return 0.0
    overlap = query_tokens & text_tokens
    return len(overlap) / len(query_tokens)


def search_memory(query: str, top_k: int = 5, zone: str = None) -> List[Tuple[str, float]]:
    """
    Search memory for facts relevant to query.
    Returns [(key, score), ...] sorted by relevance.
    """
    data = _load()
    
    # Collect all entries
    entries = []
    zones_to_search = [zone] if zone else ["identity", "knowledge", "ops"]
    
    for z in zones_to_search:
        for key, entry in data["zones"].get(z, {}).items():
            value = entry.get("value", "")
            text = f"{key}: {value}" if isinstance(value, str) else key
            entries.append((key, text, z))
    
    if _has_embeddings and len(entries) > 0:
        # Use embeddings
        query_emb = _model.encode(query, convert_to_tensor=True)
        texts = [e[1] for e in entries]
        text_embs = _model.encode(texts, convert_to_tensor=True)
        scores = util.cos_sim(query_emb, text_embs)[0]
        results = [(entries[i][0], float(scores[i])) for i in range(len(entries))]
    else:
        # Use fallback similarity
        results = [(e[0], _simple_similarity(query, e[1])) for e in entries]
    
    # Sort by score, filter low scores
    results = [(k, s) for k, s in results if s > 0.1]
    results.sort(key=lambda x: x[1], reverse=True)
    
    return results[:top_k]


def get_relevant_context(query: str, max_facts: int = 3) -> str:
    """Get formatted context string for injection into prompts."""
    results = search_memory(query, top_k=max_facts)
    if not results:
        return ""
    
    lines = ["Relevant facts from memory:"]
    for key, score in results:
        value = recall_zone(None)  # We need to get the actual value
        # Get from all zones
        data = _load()
        for z in data["zones"].values():
            if key in z:
                value = z[key].get("value", "")
                break
        lines.append(f"  - {key}: {value}")
    
    return "\n".join(lines)


def auto_extract_facts(text: str, source: str = "auto") -> List[str]:
    """
    Extract simple facts from text (names, dates, decisions).
    Returns list of extracted fact keys that were stored.
    """
    extracted = []
    
    # Pattern: X decided to Y
    decisions = re.findall(r'(\w+)\s+(?:decided|agreed|chose|approved)\s+to\s+(.+?)[.\n]', text, re.I)
    for who, what in decisions:
        key = f"decision_{who.lower()}_{hash(what) % 10000}"
        # Store in knowledge zone
        from memory import remember
        remember(key, f"{who} decided to {what} (from {source})", zone="knowledge")
        extracted.append(key)
    
    # Pattern: remember that X
    memories = re.findall(r'remember that\s+(.+?)[.\n]', text, re.I)
    for mem in memories:
        key = f"note_{hash(mem) % 10000}"
        from memory import remember
        remember(key, mem, zone="knowledge")
        extracted.append(key)
    
    return extracted
