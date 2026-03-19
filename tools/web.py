"""
agent/tools/web.py
Simple DuckDuckGo HTML search without third-party search SDKs.
"""
from __future__ import annotations

import html
import re
import urllib.parse
import urllib.request


SEARCH_URL = "https://html.duckduckgo.com/html/?q={query}"
USER_AGENT = "Mozilla/5.0 (compatible; biz-agent/1.0)"


def _clean(text: str) -> str:
    value = html.unescape(re.sub(r"<[^>]+>", " ", text or ""))
    return re.sub(r"\s+", " ", value).strip()


def search(query: str, max_results: int = 5) -> str:
    """Search DuckDuckGo and return formatted results."""
    try:
        url = SEARCH_URL.format(query=urllib.parse.quote_plus(query))
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=20) as resp:
            body = resp.read().decode("utf-8", errors="replace")

        pattern = re.compile(
            r'<a[^>]+class="result__a"[^>]+href="(?P<href>[^"]+)"[^>]*>(?P<title>.*?)</a>.*?'
            r'<a[^>]+class="result__snippet"[^>]*>(?P<snippet>.*?)</a>',
            re.S,
        )
        lines = []
        seen = set()
        for match in pattern.finditer(body):
            href = html.unescape(match.group("href"))
            href = urllib.parse.unquote(href)
            if href.startswith("//duckduckgo.com/l/?uddg="):
                parsed = urllib.parse.urlparse("https:" + href)
                params = urllib.parse.parse_qs(parsed.query)
                href = params.get("uddg", [href])[0]
            elif "uddg=" in href:
                parsed = urllib.parse.urlparse(href)
                params = urllib.parse.parse_qs(parsed.query)
                href = params.get("uddg", [href])[0]
            if href in seen:
                continue
            seen.add(href)
            title = _clean(match.group("title"))
            snippet = _clean(match.group("snippet"))
            lines.append(f"[{title}]\n{href}\n{snippet}\n")
            if len(lines) >= max_results:
                break
        return "\n".join(lines) if lines else "No results found."
    except Exception as e:
        return f"Search error: {e}"
