"""
agent/tools/web.py
DDG web search. Returns top N results as plain text snippets.
No API key needed — uses duckduckgo-search library.
"""
from duckduckgo_search import DDGS


def search(query: str, max_results: int = 5) -> str:
    """Search DuckDuckGo and return formatted results."""
    try:
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=max_results))
        if not results:
            return "No results found."
        lines = []
        for r in results:
            lines.append(f"[{r.get('title', '')}]\n{r.get('href', '')}\n{r.get('body', '')}\n")
        return "\n".join(lines)
    except Exception as e:
        return f"Search error: {e}"
