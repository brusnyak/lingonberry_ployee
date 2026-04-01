"""
agent/tools/content.py
Lightweight access to Victor content planning and queue state.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

CONTENT_DIR = Path(__file__).parent.parent.parent / "content"
PLAYWRIGHT_PYTHON = Path(__file__).parent.parent.parent / "leadgen" / ".venv" / "bin" / "python"

def _run(script: str, *args: str, python: str | None = None) -> str:
    interpreter = python or sys.executable
    result = subprocess.run(
        [interpreter, script, *args],
        cwd=str(CONTENT_DIR),
        capture_output=True,
        text=True,
        timeout=600,
    )
    output = ((result.stdout or "") + (result.stderr or "")).strip()
    if result.returncode != 0:
        raise RuntimeError(output or f"{script} failed with exit {result.returncode}")
    return output or "(no output)"


def report() -> str:
    return _run("report.py")


def plan_posts(count: int = 5, queue: bool = False) -> str:
    args = ["planner.py", "--count", str(count)]
    if queue:
        args.append("--queue")
    return _run(*args)


def prompt_manifests(item_id: str = "") -> str:
    args = ["generate.py", "--manifest-only"]
    if item_id:
        args.extend(["--id", item_id])
    return _run(*args)


def engagement_plan(niches: list[str] | None = None) -> str:
    args = ["engagement.py", "--plan"]
    if niches:
        args.extend(["--niches", *niches])
    return _run(*args)


def engagement_log() -> str:
    return _run("engagement.py", "--log")


def run_engagement_session(
    niches: list[str] | None = None,
    dry_run: bool = False,
    discover_only: bool = False,
) -> str:
    """Run a live Playwright engagement session on Instagram."""
    python = str(PLAYWRIGHT_PYTHON) if PLAYWRIGHT_PYTHON.exists() else sys.executable
    args = ["engagement.py"]
    if niches:
        args.extend(["--niches", *niches])
    if dry_run:
        args.append("--dry-run")
    if discover_only:
        args.append("--discover-only")
    return _run(*args, python=python)


def generate_images(item_id: str, sample_count: int = 2, aspect_ratio: str = "3:4") -> str:
    return _run(
        "generate.py",
        "--id",
        item_id,
        "--sample-count",
        str(sample_count),
        "--aspect-ratio",
        aspect_ratio,
    )


def provider_status() -> str:
    return _run("generate.py", "--provider-status")


def approve_post(item_id: str) -> str:
    return _run("approve.py", item_id, "approved")


def reject_post(item_id: str) -> str:
    return _run("approve.py", item_id, "rejected")


def prepare_publish(item_id: str, publish_after: str = "") -> str:
    args = ["publish.py", item_id]
    if publish_after:
        args.extend(["--publish-after", publish_after])
    return _run(*args)


def publish_post(item_id: str) -> str:
    python = str(PLAYWRIGHT_PYTHON) if PLAYWRIGHT_PYTHON.exists() else sys.executable
    return _run("poster.py", item_id, "--submit", python=python)


def plan_calendar(weeks: int = 2, queue: bool = False) -> str:
    """Generate a structured content calendar and optionally queue all posts."""
    import sys
    sys.path.insert(0, str(CONTENT_DIR.parent))
    try:
        from content.content_calendar import generate_calendar, format_calendar_telegram
    except ImportError:
        from content_calendar import generate_calendar, format_calendar_telegram
    posts = generate_calendar(weeks=weeks, queue=queue)
    return format_calendar_telegram(posts)


def ig_browse_profiles(usernames: list[str], posts_per_profile: int = 3) -> list[dict]:
    """Visit IG profiles and collect recent post URLs + captions. No engagement."""
    sys.path.insert(0, str(CONTENT_DIR))
    try:
        from engagement import browse_profiles
    except ImportError:
        import importlib.util
        spec = importlib.util.spec_from_file_location("engagement", CONTENT_DIR / "engagement.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        browse_profiles = mod.browse_profiles
    return browse_profiles(usernames, posts_per_profile=posts_per_profile)


def ig_draft_comment(post_url: str, screenshot_path: str = "", context_hint: str = "") -> str:
    """
    Use OpenRouter vision model to draft a comment for a post.
    Falls back to Gemini if OpenRouter fails.
    Returns the draft comment text.
    """
    import os, base64
    from pathlib import Path

    prompt = (
        "You are commenting on this Instagram post as Victor Brusnyak — "
        "a calm, observant founder based in Central Europe. "
        "Write ONE short comment (max 10 words, no hashtags, no emojis, no exclamation marks). "
        "Make it feel genuine and specific to what you see. "
    )
    if context_hint:
        prompt += f"Context: {context_hint}. "
    prompt += "Reply with the comment text only."

    # Try OpenRouter vision first (free, no quota limits)
    openrouter_key = os.environ.get("OPENROUTER_API_KEY", "").strip()
    if openrouter_key and screenshot_path and Path(screenshot_path).exists():
        try:
            from openai import OpenAI
            client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=openrouter_key)
            img_b64 = base64.b64encode(Path(screenshot_path).read_bytes()).decode()
            vision_models = [
                "meta-llama/llama-3.2-11b-vision-instruct:free",
                "google/gemma-3-27b-it:free",
                "qwen/qwen2.5-vl-72b-instruct:free",
            ]
            for model in vision_models:
                try:
                    resp = client.chat.completions.create(
                        model=model,
                        messages=[{"role": "user", "content": [
                            {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{img_b64}"}},
                            {"type": "text", "text": prompt},
                        ]}],
                        max_tokens=60,
                        temperature=0.4,
                    )
                    text = (resp.choices[0].message.content or "").strip().strip('"').strip("'")
                    if text and len(text) > 2:
                        return text
                except Exception:
                    continue
        except Exception:
            pass

    # Fallback: Gemini vision
    api_key = os.environ.get("GOOGLE_AI_VICTOR_API_KEY", "").strip()
    if api_key and screenshot_path and Path(screenshot_path).exists():
        try:
            from google import genai
            from google.genai import types
            client = genai.Client(api_key=api_key)
            parts = [
                types.Part.from_bytes(data=Path(screenshot_path).read_bytes(), mime_type="image/png"),
                prompt,
            ]
            response = client.models.generate_content(model="gemini-2.0-flash", contents=parts)
            return (response.text or "").strip().strip('"').strip("'")
        except Exception as e:
            return f"draft failed: {e}"

    # No image — text-only fallback via OpenRouter
    if openrouter_key:
        try:
            from openai import OpenAI
            client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=openrouter_key)
            resp = client.chat.completions.create(
                model="meta-llama/llama-3.3-70b-instruct:free",
                messages=[{"role": "user", "content": f"Instagram post URL: {post_url}\n{prompt}"}],
                max_tokens=60, temperature=0.4,
            )
            return (resp.choices[0].message.content or "").strip().strip('"').strip("'")
        except Exception as e:
            return f"draft failed: {e}"

    return "No vision provider available — set OPENROUTER_API_KEY or GOOGLE_AI_VICTOR_API_KEY"


def ig_post_comment(post_url: str, comment_text: str) -> str:
    """Post an approved comment to an Instagram post."""
    sys.path.insert(0, str(CONTENT_DIR))
    try:
        from ig_client import comment_post
    except ImportError:
        import importlib.util
        spec = importlib.util.spec_from_file_location("ig_client", CONTENT_DIR / "ig_client.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        comment_post = mod.comment_post
    try:
        result = comment_post(post_url, comment_text)
        return f"commented: {result}"
    except Exception as e:
        return f"comment failed: {e}"


def ig_follow(username: str) -> str:
    """Follow an Instagram user."""
    sys.path.insert(0, str(CONTENT_DIR))
    try:
        from ig_client import follow_user
    except ImportError:
        import importlib.util
        spec = importlib.util.spec_from_file_location("ig_client", CONTENT_DIR / "ig_client.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        follow_user = mod.follow_user
    try:
        result = follow_user(username)
        return f"followed @{username}: {result}"
    except Exception as e:
        return f"follow failed: {e}"


def ig_discover_profiles(
    hashtags: list[str] | None = None,
    max_profiles: int = 8,
) -> list[dict]:
    """
    Browse IG hashtag feeds and collect profile handles + post samples.
    Returns a list of profile dicts for operator review — no following.
    """
    sys.path.insert(0, str(CONTENT_DIR))
    try:
        from rebrowser_playwright.sync_api import sync_playwright
    except ImportError:
        from playwright.sync_api import sync_playwright
    import time, random, urllib.parse, json as _json

    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location("engagement", CONTENT_DIR / "engagement.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        _inject = mod._inject_ig_session_cookies
        _clean = mod._clean_singleton_locks
        _human_delay = mod._human_delay
        PROFILE_ROOT = mod.PLAYWRIGHT_PROFILE_ROOT
        ARTIFACTS = mod.PLAYWRIGHT_ARTIFACTS
        _IG_NAV = mod._IG_NAV
    except Exception as e:
        return [{"error": f"Could not load engagement module: {e}"}]

    tags = hashtags or ["bratislava", "viennagram", "praguelife", "vienna", "prague"]
    profile_dir = PROFILE_ROOT / "gemini"  # use the logged-in Chrome profile
    profile_dir.mkdir(parents=True, exist_ok=True)
    _clean(profile_dir)

    discovered = {}  # username -> dict

    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            headless=True,
            channel="chrome",
            args=["--disable-blink-features=AutomationControlled"],
            ignore_default_args=["--enable-automation", "--no-sandbox"],
            viewport={"width": 1440, "height": 900},
        )
        _inject(ctx)
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        own_username = ""
        try:
            import os
            own_username = os.environ.get("IG_USERNAME", "").strip().lower()
        except Exception:
            pass

        try:
            for tag in tags:
                if len(discovered) >= max_profiles:
                    break
                try:
                    page.goto(f"https://www.instagram.com/explore/tags/{tag}/", wait_until="domcontentloaded", timeout=20000)
                    time.sleep(random.uniform(2, 4))
                    hrefs = list(dict.fromkeys(
                        l.get_attribute("href") for l in page.locator("a[href*='/p/']").all()
                        if l.get_attribute("href")
                    ))[:6]
                    for href in hrefs:
                        if len(discovered) >= max_profiles:
                            break
                        full_url = f"https://www.instagram.com{href}" if href.startswith("/") else href
                        try:
                            page.goto(full_url, wait_until="domcontentloaded", timeout=20000)
                            time.sleep(random.uniform(2, 3))
                            # Extract author handle
                            handle = None
                            for link in page.locator("a[href]").all():
                                h = (link.get_attribute("href") or "").strip()
                                parts = [x for x in h.strip("/").split("/") if x]
                                if len(parts) == 1 and parts[0] not in _IG_NAV and not parts[0].startswith("#") and parts[0].lower() != own_username:
                                    handle = parts[0]
                                    break
                            if not handle or handle in discovered:
                                continue
                            # Visit profile
                            page.goto(f"https://www.instagram.com/{handle}/", wait_until="domcontentloaded", timeout=20000)
                            time.sleep(random.uniform(2, 3))
                            # Grab follower count + bio from page
                            bio = ""
                            followers = ""
                            try:
                                bio = page.locator("meta[name='description']").get_attribute("content") or ""
                            except Exception:
                                pass
                            # Screenshot
                            ARTIFACTS.mkdir(parents=True, exist_ok=True)
                            shot = ARTIFACTS / f"discover-{handle}-{int(time.time())}.png"
                            page.screenshot(path=str(shot), full_page=False)
                            # Collect recent post URLs
                            post_hrefs = list(dict.fromkeys(
                                l.get_attribute("href") for l in page.locator("a[href*='/p/']").all()
                                if l.get_attribute("href")
                            ))[:3]
                            discovered[handle] = {
                                "username": handle,
                                "profile_url": f"https://www.instagram.com/{handle}/",
                                "bio_snippet": bio[:150],
                                "recent_posts": [
                                    f"https://www.instagram.com{h}" if h.startswith("/") else h
                                    for h in post_hrefs
                                ],
                                "screenshot": str(shot),
                                "found_via": f"#{tag}",
                            }
                        except Exception:
                            continue
                except Exception:
                    continue
        finally:
            try:
                ctx.close()
            except Exception:
                pass

    return list(discovered.values())


def ig_weekly_strategy(target_niches: list[str] | None = None) -> str:
    """
    Generate a weekly IG growth strategy: what to post, who to engage, hashtags to target.
    Uses OpenRouter LLM — no quota limits.
    """
    import os
    from openai import OpenAI

    niches = ", ".join(target_niches) if target_niches else "local business owners, service operators, founders in Central Europe"
    client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=os.environ["OPENROUTER_API_KEY"])

    prompt = f"""You are planning a week of Instagram activity for Victor Brusnyak.
Victor is a young Eastern European founder (early 20s, based in Bratislava/Vienna/Prague area).
His account is in Phase 1: building a believable personal presence, NOT a business/marketing account.
Target audience: {niches}

Rules:
- 2-3 feed posts/week, 3-4 stories/week
- Mix: 45% founder moments, 40% lifestyle/city, 15% systems observations
- Captions: 1-3 lines, observational, no hashtag spam (3-5 max), no emojis
- Engagement: like 10-15/day, comment 3-5/day, follow 5-10/day from target niche accounts
- No hard sells, no hustle-bro content, no fake luxury

Output a plain-text weekly plan with:
1. Mon-Sun post schedule (what to post each day, caption idea, scene/style)
2. 5 hashtags to target for discovery this week
3. 3-5 profile types to engage with (describe the type, not specific handles)
4. One "systems" observation post for the week (the subtle business undertone)
5. Engagement focus: what kind of accounts to like/comment/follow this week

Keep it concise and actionable. Plain text only, no markdown."""

    models = [
        "google/gemma-3-27b-it:free",
        "meta-llama/llama-3.3-70b-instruct:free",
        "qwen/qwen3-next-80b-a3b-instruct:free",
        "mistralai/mistral-small-3.1-24b-instruct:free",
        "openai/gpt-oss-20b:free",
    ]
    for model in models:
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=800,
                temperature=0.5,
            )
            return resp.choices[0].message.content or "No response."
        except Exception:
            continue
    return "All models failed — check OPENROUTER_API_KEY."
