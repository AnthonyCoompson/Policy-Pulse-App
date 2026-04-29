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
"""

import asyncio
import json
import logging
import os
import re
import time

import httpx
import requests

log = logging.getLogger(__name__)

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_URL = "https://generativelanguage.googleapis.com/v1/models/gemini-1.5-flash:generateContent"

# Cap concurrent AI calls to avoid Gemini rate-limit errors
_AI_SEMAPHORE = asyncio.Semaphore(5)

# ── KEYWORD PRE-FILTER ────────────────────────────────────────────────────────
# These lists power quick_relevance_score(), which runs in pure Python before
# any Gemini API call is made.  Articles that score below QUICK_FILTER_THRESHOLD
# in scraper.py are discarded without ever touching the API.
#
# To tune coverage: add terms to HIGH_RELEVANCE_TERMS to catch more policy
# content; add terms to LOW_RELEVANCE_TERMS to drop more noise categories.
# Terms are matched as substrings of the lowercased title, so "health" matches
# "public health", "healthcare", "mental health" etc.

HIGH_RELEVANCE_TERMS: list[str] = [
    # Indigenous policy
    "indigenous", "first nations", "métis", "inuit", "reconciliation",
    "dripa", "undrip", "trc", "ocap", "fnha", "crown-indigenous",
    "residential school", "land rights", "treaty",
    # Post-secondary & research
    "post-secondary", "university", "college", "tuition", "campus",
    "sshrc", "nserc", "cihr", "tri-council", "research", "academic",
    "graduate", "scholarship", "endowment",
    # Government & policy
    "federal", "provincial", "legislation", "bill", "act", "regulation",
    "policy", "budget", "fiscal", "funding", "grant", "investment",
    "government", "ministry", "minister", "senate", "parliament", "hansard",
    # Health
    "health", "pharmacare", "healthcare", "mental health", "wellness",
    "public health", "drug coverage",
    # Education & workforce
    "education", "workforce", "labour", "labor", "employment",
    "apprenticeship", "childcare", "child care",
    # BC-specific
    "bc government", "british columbia", "bc legislature",
]

LOW_RELEVANCE_TERMS: list[str] = [
    # Sports
    "sports", "nba", "nfl", "nhl", "mlb", "fifa", "nfl", "hockey game",
    "basketball", "baseball game", "soccer match", "golf tournament",
    "tennis match", "formula 1", "nascar", "wrestling",
    # Entertainment / celebrity
    "celebrity", "entertainment", "box office", "album release",
    "music video", "red carpet", "awards show", "oscars", "emmys",
    "reality tv", "bachelor", "survivor",
    # Lifestyle noise
    "weather forecast", "horoscope", "astrology", "recipe", "cooking tips",
    "lottery", "casino", "gambling", "real estate listing",
    "stock tip", "crypto price", "bitcoin price",
]


def quick_relevance_score(title: str, source_name: str = "") -> int:
    """Fast keyword pre-filter — runs in pure Python before any AI call.

    Scores a title from 0 to 100 based on the presence of high-relevance
    policy terms and low-relevance noise terms.

    Scoring rules:
    - Start at 0.
    - +15 for each HIGH_RELEVANCE_TERMS hit in the lowercased title.
    - -30 for each LOW_RELEVANCE_TERMS hit in the lowercased title.
    - Clamped to [0, 100].

    A score of 0 means the article contains at least one noise signal and
    zero policy signals — safe to discard without an AI call.
    A score >= QUICK_FILTER_THRESHOLD (15 in scraper.py, i.e. one hit) means
    at least one policy keyword matched and the article proceeds to full AI
    analysis.

    The source_name parameter is accepted for future use (e.g. boosting
    scores from known trusted sources) but is not used in scoring yet.

    Args:
        title:       Article headline string.
        source_name: Name of the originating source (unused, reserved).

    Returns:
        Integer score in [0, 100].
    """
    lowered = title.lower()
    score   = 0

    for term in HIGH_RELEVANCE_TERMS:
        if term in lowered:
            score += 15

    for term in LOW_RELEVANCE_TERMS:
        if term in lowered:
            score -= 30

    return max(0, min(100, score))


SYSTEM_PROMPT = """You are a senior Canadian policy analyst specializing in:
- Post-secondary education and research funding
- Indigenous relations, UNDRIP, DRIPA, TRC Calls to Action, OCAP
- BC and Federal government policy
- Health policy (pharmacare, FNHA, mental health)
- Higher education governance and funding

You analyze news articles and return structured JSON assessments.

DOMAIN OPTIONS (pick the single best match):
Higher Education | Research Funding | Indigenous | Reconciliation | Health | Pharmacare | Budget | Legislation | Infrastructure | Workforce | Consultation | Political | Environment | Housing | Child Care | International | Other

JURISDICTION OPTIONS: Federal | BC | Alberta | Ontario | Quebec | Municipal | Pan-Canadian | International

SENTIMENT OPTIONS: Critical | Supportive | Neutral

TAG OPTIONS (pick up to 4 that apply):
Urgent | Briefing Note Worthy | UNDRIP | DRIPA | TRC | OCAP | Budget | Funding | Research | Legislation | Regulation | Consultation | Reconciliation | Indigenous | Health | Mental Health | Pharmacare | Workforce | Infrastructure | Political | Data/Evidence | Court Decision | Audit | International

Return ONLY valid JSON. No markdown, no explanation."""

# Used when we have the full article body
ANALYSIS_PROMPT_FULL = """Analyze this article and return a JSON object with these exact fields:

{{
  "domain": "<single domain from list>",
  "jurisdiction": "<single jurisdiction from list>",
  "relevance": <integer 1-10, where 10 = critical for BC/Canada post-secondary/Indigenous policy>,
  "sentiment": "<Critical|Supportive|Neutral toward government policy>",
  "summary": "<2-3 sentences summarizing what the article actually says — drawn from the article text, not just the title>",
  "why_it_matters": "<2-3 sentences on the concrete implications for a BC university government relations team — be specific, not generic>",
  "tags": ["<tag1>", "<tag2>"]
}}

TITLE: {title}
SOURCE: {source}
URL: {url}

ARTICLE TEXT:
{article_text}

Relevance scoring guide:
9-10: Critical — directly affects post-secondary funding, Indigenous policy, BC government relations
7-8: High — relevant to sector, worth monitoring closely
6: Moderate — tangentially relevant, include if space allows
1-5: Low relevance — return null

If relevance would be 5 or below, return exactly: null"""

# Fallback when we only have title (no article body fetched)
ANALYSIS_PROMPT_TITLE_ONLY = """Analyze this article and return a JSON object with these exact fields:

{{
  "domain": "<single domain from list>",
  "jurisdiction": "<single jurisdiction from list>",
  "relevance": <integer 1-10, where 10 = critical for BC/Canada post-secondary/Indigenous policy>,
  "sentiment": "<Critical|Supportive|Neutral toward government policy>",
  "summary": "<2-3 sentences summarizing the likely policy significance based on the title and source>",
  "why_it_matters": "<2-3 sentences on the concrete implications for a BC university government relations team>",
  "tags": ["<tag1>", "<tag2>"]
}}

TITLE: {title}
SOURCE: {source}
URL: {url}

Relevance scoring guide:
9-10: Critical — directly affects post-secondary funding, Indigenous policy, BC government relations
7-8: High — relevant to sector, worth monitoring closely
6: Moderate — tangentially relevant, include if space allows
1-5: Low relevance — return null

If relevance would be 5 or below, return exactly: null"""


def _build_payload(title: str, url: str, source_name: str, article_text: str) -> dict:
    """Build the Gemini API request payload. Shared by sync and async paths."""
    has_text = bool(article_text and len(article_text.strip()) > 150)
    if has_text:
        trimmed = article_text.strip()[:4000]
        prompt = ANALYSIS_PROMPT_FULL.format(
            title=title,
            source=source_name,
            url=url,
            article_text=trimmed,
        )
    else:
        prompt = ANALYSIS_PROMPT_TITLE_ONLY.format(
            title=title,
            source=source_name,
            url=url,
        )
    return {
        "contents": [{"parts": [{"text": SYSTEM_PROMPT + "\n\n" + prompt}]}],
        "generationConfig": {"temperature": 0.1, "maxOutputTokens": 500},
    }


def _parse_gemini_response(text: str, title: str, source_name: str) -> dict | None:
    """
    Parse the raw text from a Gemini response into a result dict.
    Returns None if relevance < 6 or text is 'null'.
    Returns _default_analysis() on parse errors rather than crashing.
    """
    text = text.strip()
    if text.lower() == "null" or text == "":
        return None

    text = re.sub(r"^```json\s*", "", text)
    text = re.sub(r"\s*```$", "", text)

    try:
        result = json.loads(text)
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
    """
    Synchronous Gemini call. Used by the serial scraper loop and any code
    that calls analyze_article() one at a time.

    Args:
        title:        Article headline.
        url:          Article URL.
        source_name:  Name of the source (e.g. "BC Government Newsroom").
        article_text: Full article body text (optional but strongly recommended).

    Returns:
        dict with domain/relevance/sentiment/summary/why_it_matters/tags,
        or None if relevance < 6 or on error.
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
                timeout=25,
            )
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
                time.sleep(2)
        except (KeyError, IndexError) as e:
            log.warning(f"Gemini response structure error: {e}")
            break

    return _default_analysis(title, source_name)


# ── ASYNC — new functions for concurrent batch processing ─────────────────────

async def analyze_article_async(title: str, url: str, source_name: str = "",
                                 article_text: str = "") -> dict | None:
    """
    Async version of analyze_article(). Uses httpx.AsyncClient and respects
    the module-level _AI_SEMAPHORE (max 5 concurrent calls).

    Same return contract as analyze_article() — returns a result dict or None.
    """
    if not GEMINI_API_KEY:
        return _default_analysis(title, source_name)

    payload = _build_payload(title, url, source_name, article_text)
    api_url = f"{GEMINI_URL}?key={GEMINI_API_KEY}"

    async with _AI_SEMAPHORE:
        for attempt in range(3):
            try:
                async with httpx.AsyncClient(timeout=25) as client:
                    resp = await client.post(api_url, json=payload)
                    resp.raise_for_status()
                    data = resp.json()
                    text = data["candidates"][0]["content"]["parts"][0]["text"]
                    return _parse_gemini_response(text, title, source_name)

            except json.JSONDecodeError as e:
                log.warning(f"[async] JSON parse error attempt {attempt+1}: {e}")
                if attempt < 2:
                    await asyncio.sleep(1)
            except httpx.HTTPError as e:
                log.warning(f"[async] HTTP error attempt {attempt+1}: {e}")
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
            except (KeyError, IndexError) as e:
                log.warning(f"[async] Gemini response structure error: {e}")
                break

    return _default_analysis(title, source_name)


async def analyze_articles_batch(items: list) -> list:
    """
    Analyze a list of articles concurrently using asyncio.gather.
    Concurrency is capped at 5 by _AI_SEMAPHORE.

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

def _default_analysis(title: str, source_name: str) -> dict:
    """Keyword-based fallback analysis when AI is unavailable."""
    title_lower = title.lower()

    if any(w in title_lower for w in ["indigenous", "first nations", "métis", "inuit",
                                       "reconcili", "dripa", "undrip", "trc"]):
        domain = "Indigenous"; jurisdiction = "BC"; relevance = 8
    elif any(w in title_lower for w in ["university", "college", "post-secondary",
                                         "tuition", "student", "campus"]):
        domain = "Higher Education"; jurisdiction = "BC"; relevance = 7
    elif any(w in title_lower for w in ["research", "grant", "sshrc", "nserc",
                                         "cihr", "funding"]):
        domain = "Research Funding"; jurisdiction = "Federal"; relevance = 7
    elif any(w in title_lower for w in ["budget", "fiscal", "spending",
                                         "billion", "million"]):
        domain = "Budget"; jurisdiction = "Federal"; relevance = 6
    elif any(w in title_lower for w in ["health", "pharmacare", "mental health", "wellness"]):
        domain = "Health"; jurisdiction = "BC"; relevance = 6
    elif any(w in title_lower for w in ["bill", "legislation", "act", "regulation", "law"]):
        domain = "Legislation"; jurisdiction = "BC"; relevance = 6
    else:
        domain = "Other"; jurisdiction = "Federal"; relevance = 6

    return {
        "domain":         domain,
        "jurisdiction":   jurisdiction,
        "relevance":      relevance,
        "sentiment":      "Neutral",
        "summary":        f"{title} — from {source_name}.",
        "why_it_matters": "Review this article for potential relevance to your policy priorities.",
        "tags":           [],
    }
