"""
PolicyPulse AI Processor — Gemini API
Analyzes each article for domain, relevance, sentiment, summary, why-it-matters.
Only keeps articles scoring 6+.

v2: Now accepts full article_text so summaries and why-it-matters are
    generated from the actual article content, not just the title.
v3: Added async analyze_article_async() and analyze_articles_batch()
    for concurrent processing of multiple articles using asyncio + httpx.
    Concurrency capped at 5 via module-level semaphore to avoid Gemini rate limits.
    Original synchronous analyze_article() kept intact for backward compatibility.

v4 improvements:
  - Institutional context (name, role, priority domains, relevance examples)
    is now DB-configurable via the scraper_config table. Change your focus
    without touching Python — update via Settings or POST /scraper-config.
  - Fixed the {{ / }} double-brace escape tech debt. Prompt templates now use
    <PLACEHOLDER> markers and plain str.replace(), so curly braces in article
    text never cause issues and the prompts read cleanly.
  - quick_relevance_score() now applies a per-source trust boost so articles
    from high-signal government sources skip the noise threshold automatically.
  - Article text truncation raised to 6000 chars. The first 6000 chars of a
    government press release typically contain all the policy-relevant content;
    this gives Gemini more signal without approaching context limits.
  - Rate-limit (HTTP 429) handling: backs off 30s and retries rather than
    immediately falling back to the keyword classifier.
  - Smarter JSON extraction: handles cases where Gemini wraps the response in
    extra prose before/after the JSON object.
  - _default_analysis() now accepts allow_fallback=False (used by scholarly
    scraper) so papers with no policy keywords are dropped rather than saved
    with a floor relevance of 6.
  - Semaphore concurrency is now configurable via GEMINI_CONCURRENCY env var.
"""

import asyncio
import json
import logging
import os
import re
import time
from functools import lru_cache

import httpx
import requests

log = logging.getLogger(__name__)

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_URL = "https://generativelanguage.googleapis.com/v1/models/gemini-1.5-flash:generateContent"

# Concurrency cap: override with GEMINI_CONCURRENCY env var (default 5).
# Lower this if you see frequent 429 rate-limit errors in Render logs.
_GEMINI_CONCURRENCY = int(os.environ.get("GEMINI_CONCURRENCY", "5"))

# Lazily initialised inside a running event loop to avoid the RuntimeError
# Python 3.10+ raises when asyncio primitives are created at module-import time.
_AI_SEMAPHORE: asyncio.Semaphore | None = None


def _get_semaphore() -> asyncio.Semaphore:
    global _AI_SEMAPHORE
    if _AI_SEMAPHORE is None:
        _AI_SEMAPHORE = asyncio.Semaphore(_GEMINI_CONCURRENCY)
    return _AI_SEMAPHORE


# ── DB-CONFIGURABLE PROMPT CONTEXT ───────────────────────────────────────────
# These defaults are used when the DB is unavailable (first boot, tests) or
# when no custom values have been saved to scraper_config.
#
# To customise for a different institution or policy focus, POST to
# /scraper-config with one or more of these keys:
#
#   ai_institution_name     — e.g. "Simon Fraser University"
#   ai_institution_role     — e.g. "government relations team at a BC university"
#   ai_priority_domains     — comma-separated list of domains that score 9-10
#   ai_priority_context     — 1-2 sentences describing what matters most
#   ai_why_matters_examples — JSON array of example why_it_matters strings
#   ai_extra_high_terms     — comma-separated additional HIGH_RELEVANCE_TERMS
#   ai_extra_low_terms      — comma-separated additional LOW_RELEVANCE_TERMS

_PROMPT_CONFIG_DEFAULTS: dict = {
    "ai_institution_name":     "a BC university or college",
    "ai_institution_role":     "government relations team at a BC post-secondary institution",
    "ai_priority_domains":     "Indigenous, Reconciliation, Higher Education, Research Funding, Health",
    "ai_priority_context": (
        "BC and Canadian post-secondary education, Indigenous policy (DRIPA, UNDRIP, TRC, OCAP), "
        "health policy (pharmacare, FNHA), and federal/provincial research funding"
    ),
    "ai_why_matters_examples": json.dumps([
        (
            "This bill's proposed board composition changes would require SFU and UBC to amend "
            "bylaws before the fall semester — Legal and Board Relations should review the draft "
            "text before second reading."
        ),
        (
            "The 12% tri-council infrastructure cut directly reduces eligible renewal funding for "
            "lab equipment — government relations teams should coordinate a joint sector response "
            "with Universities Canada before the April budget reply deadline."
        ),
        (
            "The DRIPA Year 3 report sets new benchmarks for Indigenous partnership reporting that "
            "post-secondary institutions must align with by March 2027 — flag for the VP Indigenous "
            "portfolio and include in the next board briefing."
        ),
    ]),
}

# Module-level cache so we don't hit the DB on every single article.
# Cleared when scraper_config is updated (call _clear_prompt_config_cache()).
_prompt_config_cache: dict | None = None


def _clear_prompt_config_cache() -> None:
    """Call this after updating scraper_config so prompts rebuild on next use."""
    global _prompt_config_cache
    _prompt_config_cache = None


def _load_prompt_config() -> dict:
    """Load institutional prompt context from scraper_config, falling back to defaults.

    Merges DB values onto the defaults so partial configuration works —
    you only need to set the keys you want to override.
    """
    global _prompt_config_cache
    if _prompt_config_cache is not None:
        return _prompt_config_cache

    cfg = dict(_PROMPT_CONFIG_DEFAULTS)
    try:
        from database import get_scraper_config
        for key in _PROMPT_CONFIG_DEFAULTS:
            val = get_scraper_config(key)
            if val:
                cfg[key] = val
    except Exception as e:
        log.debug(f"Could not load prompt config from DB (using defaults): {e}")

    _prompt_config_cache = cfg
    return cfg


def _parse_extra_terms(raw: str) -> list[str]:
    """Parse a comma-separated config string into a clean list of terms."""
    if not raw:
        return []
    return [t.strip().lower() for t in raw.split(",") if t.strip()]


# ── KEYWORD PRE-FILTER ────────────────────────────────────────────────────────
# These lists power quick_relevance_score(), which runs in pure Python before
# any Gemini API call is made. Articles that score below QUICK_FILTER_THRESHOLD
# in scraper.py are discarded without ever touching the API.
#
# To add terms without editing code, use the ai_extra_high_terms /
# ai_extra_low_terms keys in scraper_config (comma-separated strings).

HIGH_RELEVANCE_TERMS: list[str] = [
    # Indigenous policy
    "indigenous", "first nations", "métis", "inuit", "reconciliation",
    "dripa", "undrip", "trc", "ocap", "fnha", "crown-indigenous",
    "residential school", "land rights", "treaty", "nation-to-nation",
    "self-determination", "inherent rights",
    # Post-secondary & research
    "post-secondary", "university", "college", "tuition", "campus",
    "sshrc", "nserc", "cihr", "tri-council", "research", "academic",
    "graduate", "scholarship", "endowment", "accreditation",
    # Government & policy
    "federal", "provincial", "legislation", "bill", "act", "regulation",
    "policy", "budget", "fiscal", "funding", "grant", "investment",
    "government", "ministry", "minister", "senate", "parliament", "hansard",
    "throne speech", "cabinet", "consultation",
    # Health
    "health", "pharmacare", "healthcare", "mental health", "wellness",
    "public health", "drug coverage", "opioid", "overdose",
    # Education & workforce
    "education", "workforce", "labour", "labor", "employment",
    "apprenticeship", "childcare", "child care", "early learning",
    # BC-specific
    "bc government", "british columbia", "bc legislature",
]

LOW_RELEVANCE_TERMS: list[str] = [
    # Sports
    "nba", "nfl", "nhl", "mlb", "fifa", "hockey game",
    "basketball", "baseball game", "soccer match", "golf tournament",
    "tennis match", "formula 1", "nascar", "wrestling",
    # Entertainment / celebrity
    "celebrity", "box office", "album release",
    "music video", "red carpet", "awards show", "oscars", "emmys",
    "reality tv", "bachelor", "survivor",
    # Lifestyle noise
    "weather forecast", "horoscope", "astrology", "recipe", "cooking tips",
    "lottery", "casino", "gambling", "real estate listing",
    "stock tip", "crypto price", "bitcoin price",
]

# Sources that get an automatic score bump so they clear the pre-filter even
# without keyword hits. Government press releases and official parliamentary
# sources are almost always relevant and should never be dropped by the
# keyword pre-filter.
TRUSTED_SOURCE_BOOSTS: dict[str, int] = {
    "bc government newsroom":             30,
    "bc legislature":                     30,
    "bc ministry":                        25,
    "government of canada":               25,
    "indigenous relations":               30,
    "first nations health authority":     30,
    "fnha":                               30,
    "crown-indigenous relations":         25,
    "sshrc":                              25,
    "nserc":                              25,
    "cihr":                               25,
    "universities canada":                20,
    "university affairs":                 20,
    "hansard":                            20,
}


def quick_relevance_score(title: str, source_name: str = "") -> int:
    """Fast keyword pre-filter — runs in pure Python before any AI call.

    Scores a title from 0 to 100 based on:
    - +15 for each HIGH_RELEVANCE_TERMS hit in the lowercased title
    - -30 for each LOW_RELEVANCE_TERMS hit in the lowercased title
    - +N source trust boost if source_name matches a TRUSTED_SOURCE_BOOSTS entry
    - Any extra terms configured via ai_extra_high_terms / ai_extra_low_terms

    A score of 0 means the article should be dropped without an AI call.
    A score >= QUICK_FILTER_THRESHOLD (15 in scraper.py = one hit) means at
    least one policy keyword matched and the article proceeds to AI analysis.
    """
    lowered       = title.lower()
    source_lower  = source_name.lower()
    score         = 0

    for term in HIGH_RELEVANCE_TERMS:
        if term in lowered:
            score += 15

    for term in LOW_RELEVANCE_TERMS:
        if term in lowered:
            score -= 30

    # Source trust boost — respected sources get a floor score regardless of
    # title keywords, so government press releases are never filtered out.
    for source_key, boost in TRUSTED_SOURCE_BOOSTS.items():
        if source_key in source_lower:
            score += boost
            break  # apply at most one boost

    # Apply any extra terms from DB config
    try:
        cfg = _load_prompt_config()
        for term in _parse_extra_terms(cfg.get("ai_extra_high_terms", "")):
            if term in lowered:
                score += 15
        for term in _parse_extra_terms(cfg.get("ai_extra_low_terms", "")):
            if term in lowered:
                score -= 30
    except Exception:
        pass  # config unavailable — use base terms only

    return max(0, min(100, score))


# ── PROMPT BUILDERS ───────────────────────────────────────────────────────────
# Prompts are built dynamically from the DB config so they can be updated
# without redeploying. The placeholders use <CAPS> markers so they're easy
# to read and never conflict with curly braces in article text.

def _build_system_prompt(cfg: dict) -> str:
    """Build the Gemini system prompt from institutional config."""
    institution    = cfg.get("ai_institution_name", _PROMPT_CONFIG_DEFAULTS["ai_institution_name"])
    role           = cfg.get("ai_institution_role", _PROMPT_CONFIG_DEFAULTS["ai_institution_role"])
    priority_ctx   = cfg.get("ai_priority_context", _PROMPT_CONFIG_DEFAULTS["ai_priority_context"])

    return f"""You are a senior Canadian policy analyst specializing in:
- {priority_ctx}
- BC and Federal government policy
- Higher education governance and funding
- Health policy (pharmacare, FNHA, mental health)

You analyze news articles and return structured JSON assessments for {institution}.

DOMAIN OPTIONS (pick the single best match):
Higher Education | Research Funding | Indigenous | Reconciliation | Health | Pharmacare | Budget | Legislation | Infrastructure | Workforce | Consultation | Political | Environment | Housing | Child Care | International | Other

JURISDICTION OPTIONS: Federal | BC | Alberta | Ontario | Quebec | Municipal | Pan-Canadian | International

SENTIMENT OPTIONS: Critical | Supportive | Neutral

TAG OPTIONS (pick up to 4 that apply):
Urgent | Briefing Note Worthy | UNDRIP | DRIPA | TRC | OCAP | Budget | Funding | Research | Legislation | Regulation | Consultation | Reconciliation | Indigenous | Health | Mental Health | Pharmacare | Workforce | Infrastructure | Political | Data/Evidence | Court Decision | Audit | International

Return ONLY valid JSON. No markdown, no explanation, no text before or after the JSON."""


def _build_analysis_prompts(cfg: dict) -> tuple[str, str]:
    """Return (FULL_PROMPT_TEMPLATE, TITLE_ONLY_TEMPLATE) built from config.

    Templates use <PLACEHOLDER> markers replaced via str.replace() in
    _build_payload(). This avoids both .format() KeyErrors (curly braces
    in article text) and the ugly {{ / }} double-brace escaping.
    """
    role           = cfg.get("ai_institution_role", _PROMPT_CONFIG_DEFAULTS["ai_institution_role"])
    priority_doms  = cfg.get("ai_priority_domains", _PROMPT_CONFIG_DEFAULTS["ai_priority_domains"])

    try:
        examples = json.loads(cfg.get("ai_why_matters_examples",
                                       _PROMPT_CONFIG_DEFAULTS["ai_why_matters_examples"]))
        good_examples = "\n".join(f'- "{ex}"' for ex in examples[:3])
    except Exception:
        good_examples = (
            '- "This bill\'s proposed changes require institutions to review bylaws before second reading."\n'
            '- "The funding cut reduces eligible renewal allocations — a joint sector response is needed."\n'
            '- "DRIPA benchmarks require new reporting by March 2027 — flag for VP Indigenous portfolio."'
        )

    full_prompt = f"""Analyze this article and return a JSON object with these exact fields:

{{
  "domain": "<single domain from list>",
  "jurisdiction": "<single jurisdiction from list>",
  "relevance": <integer 1-10, where 10 = critical for {priority_doms}>,
  "sentiment": "<Critical|Supportive|Neutral toward government policy>",
  "summary": "<2-3 sentences summarizing what the article actually says — drawn from the article text, not just the title>",
  "why_it_matters": "<2-3 sentences — see guidance below>",
  "tags": ["<tag1>", "<tag2>"]
}}

TITLE: <TITLE>
SOURCE: <SOURCE>
URL: <URL>

ARTICLE TEXT:
<ARTICLE_TEXT>

Relevance scoring guide:
9-10: Critical — directly affects {priority_doms}
7-8: High — relevant to sector, worth monitoring closely
6: Moderate — tangentially relevant, include if space allows
1-5: Low relevance — return null

WHY IT MATTERS — guidance for writing a strong entry:
Write 2-3 sentences that answer: "So what does this mean for a {role} RIGHT NOW?"
Be concrete and action-oriented. Name the specific mechanism of impact (funding formula,
legislative deadline, consultation window, board obligation).

GOOD examples:
{good_examples}

BAD examples (too generic — never write these):
- "This article may be relevant to policy professionals monitoring government activity."
- "Review this article for potential relevance to your policy priorities."
- "This could have implications for higher education stakeholders."

If relevance would be 5 or below, return exactly: null"""

    title_only_prompt = f"""Analyze this article and return a JSON object with these exact fields:

{{
  "domain": "<single domain from list>",
  "jurisdiction": "<single jurisdiction from list>",
  "relevance": <integer 1-10, where 10 = critical for {priority_doms}>,
  "sentiment": "<Critical|Supportive|Neutral toward government policy>",
  "summary": "<2-3 sentences summarizing the likely policy significance based on the title and source>",
  "why_it_matters": "<2-3 sentences — see guidance below>",
  "tags": ["<tag1>", "<tag2>"]
}}

TITLE: <TITLE>
SOURCE: <SOURCE>
URL: <URL>

Relevance scoring guide:
9-10: Critical — directly affects {priority_doms}
7-8: High — relevant to sector, worth monitoring closely
6: Moderate — tangentially relevant, include if space allows
1-5: Low relevance — return null

WHY IT MATTERS — guidance for writing a strong entry:
Write 2-3 sentences that answer: "So what does this mean for a {role} RIGHT NOW?"
Be concrete and action-oriented. Name the specific mechanism of impact.

GOOD examples:
{good_examples}

BAD examples (too generic — never write these):
- "This article may be relevant to policy professionals monitoring government activity."
- "Review this article for potential relevance to your policy priorities."

If relevance would be 5 or below, return exactly: null"""

    return full_prompt, title_only_prompt


def _build_payload(title: str, url: str, source_name: str, article_text: str) -> dict:
    """Build the Gemini API request payload. Shared by sync and async paths.

    Uses <PLACEHOLDER> markers and str.replace() so curly braces in article_text
    (JSON snippets, government press releases, code blocks) never cause issues.

    Text truncation strategy: 6000 chars (up from 4000) starting from the
    beginning of the article. Government press releases front-load their policy
    content, so the first 6000 chars almost always contain everything relevant.
    """
    cfg  = _load_prompt_config()
    full_template, title_only_template = _build_analysis_prompts(cfg)
    system_prompt = _build_system_prompt(cfg)

    has_text = bool(article_text and len(article_text.strip()) > 150)
    if has_text:
        trimmed = article_text.strip()[:6000]
        prompt = (full_template
                  .replace("<TITLE>", title)
                  .replace("<SOURCE>", source_name)
                  .replace("<URL>", url)
                  .replace("<ARTICLE_TEXT>", trimmed))
    else:
        prompt = (title_only_template
                  .replace("<TITLE>", title)
                  .replace("<SOURCE>", source_name)
                  .replace("<URL>", url))

    return {
        "contents": [{"parts": [{"text": system_prompt + "\n\n" + prompt}]}],
        "generationConfig": {"temperature": 0.1, "maxOutputTokens": 500},
    }


def _extract_json_from_text(text: str) -> str:
    """Extract a JSON object from Gemini's response even when it includes prose.

    Handles three common Gemini response shapes:
    1. Clean JSON only (ideal case)
    2. JSON wrapped in ```json ... ``` code fences
    3. JSON object embedded in prose (find outermost { ... })
    """
    text = text.strip()

    # Strip markdown code fences
    text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```\s*$", "", text)
    text = text.strip()

    # If it now starts with { we're done
    if text.startswith("{"):
        return text

    # Try to find the outermost JSON object embedded in surrounding prose.
    # This handles cases like: "Here is the analysis:\n{ ... }\nLet me know..."
    match = re.search(r"\{[\s\S]*\}", text)
    if match:
        return match.group(0)

    return text


def _parse_gemini_response(text: str, title: str, source_name: str) -> dict | None:
    """Parse the raw text from a Gemini response into a result dict.

    Returns None if relevance < 6 or text is 'null'.
    Falls back to _default_analysis() on parse errors rather than crashing.
    """
    text = text.strip()
    if text.lower() in ("null", ""):
        return None

    extracted = _extract_json_from_text(text)

    try:
        result = json.loads(extracted)
    except json.JSONDecodeError as e:
        log.warning(f"JSON parse error: {e} — raw: {text[:200]}")
        return _default_analysis(title, source_name)

    if result.get("relevance", 0) < 6:
        return None

    return {
        "domain":         result.get("domain", "Other"),
        "jurisdiction":   result.get("jurisdiction", "Unknown"),
        "relevance":      int(result.get("relevance", 6)),
        "sentiment":      result.get("sentiment", "Neutral"),
        "summary":        result.get("summary", title),
        "why_it_matters": result.get("why_it_matters",
                                     "Review this article for potential relevance to your policy priorities."),
        "tags":           result.get("tags", []),
    }


# ── SYNCHRONOUS — original function, kept intact for backward compatibility ───

def analyze_article(title: str, url: str, source_name: str = "",
                    article_text: str = "") -> dict | None:
    """Synchronous Gemini call. Used by the serial scraper loop and any code
    that calls analyze_article() one at a time.

    Retry strategy:
    - HTTP 429 (rate limit): back off 30s then retry (up to 2 retries)
    - Network / timeout errors: back off 2s then retry (up to 2 retries)
    - Malformed JSON: 1 retry then fall back to keyword classifier
    - Response structure errors (missing candidates key): no retry, fall back immediately

    Args:
        title:        Article headline.
        url:          Article URL.
        source_name:  Name of the source (e.g. "BC Government Newsroom").
        article_text: Full article body text (optional but strongly recommended).

    Returns:
        dict with domain/relevance/sentiment/summary/why_it_matters/tags,
        or None if relevance < 6 or on unrecoverable error.
    """
    if not GEMINI_API_KEY:
        log.warning("GEMINI_API_KEY not set — skipping AI analysis, using defaults")
        return _default_analysis(title, source_name)

    payload = _build_payload(title, url, source_name, article_text)

    for attempt in range(3):
        try:
            resp = requests.post(
                f"{GEMINI_URL}?key={GEMINI_API_KEY}",
                json=payload,
                timeout=30,
            )
            # Rate limit — back off significantly before retrying
            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                log.warning(f"Gemini rate limit (429) on attempt {attempt+1} — waiting {wait}s")
                if attempt < 2:
                    time.sleep(wait)
                    continue
                return _default_analysis(title, source_name)

            resp.raise_for_status()
            data = resp.json()
            text = data["candidates"][0]["content"]["parts"][0]["text"]
            return _parse_gemini_response(text, title, source_name)

        except json.JSONDecodeError as e:
            log.warning(f"JSON parse error on attempt {attempt+1}: {e}")
            if attempt < 2:
                time.sleep(1)
        except requests.RequestException as e:
            log.warning(f"Gemini API request error on attempt {attempt+1}: {e}")
            if attempt < 2:
                time.sleep(2 ** attempt)
        except (KeyError, IndexError) as e:
            log.warning(f"Gemini response structure error: {e}")
            break  # Malformed response — fall back immediately, no retry benefit

    return _default_analysis(title, source_name)


# ── ASYNC — new functions for concurrent batch processing ─────────────────────

async def analyze_article_async(title: str, url: str, source_name: str = "",
                                 article_text: str = "") -> dict | None:
    """Async version of analyze_article(). Uses httpx.AsyncClient and respects
    the module-level _AI_SEMAPHORE (max GEMINI_CONCURRENCY concurrent calls).

    Same return contract as analyze_article() — returns a result dict or None.
    """
    if not GEMINI_API_KEY:
        return _default_analysis(title, source_name)

    payload = _build_payload(title, url, source_name, article_text)
    api_url = f"{GEMINI_URL}?key={GEMINI_API_KEY}"

    async with _get_semaphore():
        async with httpx.AsyncClient(timeout=30) as client:
            for attempt in range(3):
                try:
                    resp = await client.post(api_url, json=payload)

                    # Rate limit — back off before retrying
                    if resp.status_code == 429:
                        wait = 30 * (attempt + 1)
                        log.warning(f"[async] Gemini rate limit (429) on attempt {attempt+1} — waiting {wait}s")
                        if attempt < 2:
                            await asyncio.sleep(wait)
                            continue
                        return _default_analysis(title, source_name)

                    resp.raise_for_status()
                    data = resp.json()
                    text = data["candidates"][0]["content"]["parts"][0]["text"]
                    return _parse_gemini_response(text, title, source_name)

                except json.JSONDecodeError as e:
                    log.warning(f"[async] JSON parse error attempt {attempt+1}: {e}")
                    if attempt < 2:
                        await asyncio.sleep(1)
                except httpx.HTTPStatusError as e:
                    log.warning(f"[async] HTTP {e.response.status_code} on attempt {attempt+1}: {e}")
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)
                except httpx.RequestError as e:
                    log.warning(f"[async] Request error attempt {attempt+1}: {e}")
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)
                except (KeyError, IndexError) as e:
                    log.warning(f"[async] Gemini response structure error: {e}")
                    break

    return _default_analysis(title, source_name)


async def analyze_articles_batch(items: list) -> list:
    """Analyze a list of articles concurrently using asyncio.gather.
    Concurrency is capped by _AI_SEMAPHORE (default 5, set via GEMINI_CONCURRENCY).

    Args:
        items: list of dicts, each with keys:
               - title        (str, required)
               - url          (str, required)
               - source_name  (str, optional)
               - article_text (str, optional)

    Returns:
        list of results in the SAME ORDER as the input list.
        Each element is either a result dict or None (low relevance / error).

    Example:
        results = asyncio.run(analyze_articles_batch([
            {"title": "Budget cuts", "url": "https://...", "source_name": "CBC", "article_text": "..."},
            {"title": "DRIPA update", "url": "https://...", "source_name": "BC Gov", "article_text": "..."},
        ]))
    """
    tasks = [
        analyze_article_async(
            title=item.get("title", ""),
            url=item.get("url", ""),
            source_name=item.get("source_name", ""),
            article_text=item.get("article_text", ""),
        )
        for item in items
    ]
    # asyncio.gather preserves order — result[i] corresponds to items[i]
    return list(await asyncio.gather(*tasks))


# ── FALLBACK ──────────────────────────────────────────────────────────────────

def _default_analysis(title: str, source_name: str, allow_fallback: bool = True) -> dict | None:
    """Keyword-based fallback analysis when AI is unavailable or returns null.

    Applies the same domain taxonomy as the AI prompt so results are consistent.
    Uses the DB-configured priority domains to determine which categories earn
    higher relevance scores.

    Args:
        title:          Article headline string.
        source_name:    Name of the originating source.
        allow_fallback: If False, returns None instead of a minimum-relevance
                        result when no policy keywords match. Used by the
                        scholarly scraper so genuinely off-topic papers are
                        filtered out rather than saved with a floor score of 6.
    """
    title_lower = title.lower()

    # Load priority domains from config to give them higher fallback scores.
    # This means if you add "Climate" to your priority domains, climate articles
    # will score 7+ in the fallback even when Gemini is down.
    try:
        cfg = _load_prompt_config()
        priority_domains_raw = cfg.get("ai_priority_domains", _PROMPT_CONFIG_DEFAULTS["ai_priority_domains"])
        priority_domains = [d.strip().lower() for d in priority_domains_raw.split(",")]
    except Exception:
        priority_domains = ["indigenous", "reconciliation", "higher education", "research funding", "health"]

    # Domain detection — ordered from most to least specific
    domain_rules = [
        # Indigenous / reconciliation (highest priority)
        (["indigenous", "first nations", "métis", "inuit", "reconcili", "dripa",
          "undrip", "trc", "ocap", "residential school", "land rights", "treaty",
          "nation-to-nation", "self-determination"],
         "Indigenous", "BC"),
        # Higher education
        (["university", "college", "post-secondary", "tuition", "student", "campus",
          "accreditation", "endowment"],
         "Higher Education", "BC"),
        # Research funding
        (["research", "grant", "sshrc", "nserc", "cihr", "tri-council", "funding"],
         "Research Funding", "Federal"),
        # Health / pharmacare
        (["pharmacare", "drug coverage"], "Pharmacare", "BC"),
        (["health", "mental health", "wellness", "healthcare", "opioid", "fnha"],
         "Health", "BC"),
        # Budget & fiscal
        (["budget", "fiscal", "spending", "billion", "million", "deficit", "surplus"],
         "Budget", "Federal"),
        # Legislation
        (["bill", "legislation", "act", "regulation", "law", "statute"],
         "Legislation", "BC"),
        # Environment
        (["climate", "emissions", "carbon", "clean energy", "wildfire", "flood",
          "environmental", "net zero"],
         "Environment", "Federal"),
        # Workforce
        (["workforce", "labour", "labor", "employment", "apprenticeship",
          "childcare", "child care"],
         "Workforce", "Federal"),
        # Infrastructure
        (["infrastructure", "transit", "housing", "construction"],
         "Infrastructure", "Municipal"),
    ]

    for keywords, domain, default_jurisdiction in domain_rules:
        if any(kw in title_lower for kw in keywords):
            # Boost relevance if this domain is in the configured priority list
            is_priority = any(p in domain.lower() for p in priority_domains)
            relevance = 8 if is_priority else 6
            return {
                "domain":         domain,
                "jurisdiction":   default_jurisdiction,
                "relevance":      relevance,
                "sentiment":      "Neutral",
                "summary":        f"{title} — from {source_name}.",
                "why_it_matters": "Review this article for potential relevance to your policy priorities.",
                "tags":           [],
            }

    # No policy keywords matched at all.
    if not allow_fallback:
        return None  # scholarly scraper uses this to drop off-topic papers

    return {
        "domain":         "Other",
        "jurisdiction":   "Federal",
        "relevance":      6,
        "sentiment":      "Neutral",
        "summary":        f"{title} — from {source_name}.",
        "why_it_matters": "Review this article for potential relevance to your policy priorities.",
        "tags":           [],
    }
