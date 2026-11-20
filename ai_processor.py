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
# Model is configurable. gemini-1.5-flash on the v1 endpoint has been retired.
# Verify the current model name against Google documentation before changing the
# default. Override with the GEMINI_MODEL env var without redeploying code.
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")
GEMINI_API_VERSION = os.environ.get("GEMINI_API_VERSION", "v1beta")
GEMINI_URL = (
    f"https://generativelanguage.googleapis.com/{GEMINI_API_VERSION}/models/"
    f"{GEMINI_MODEL}:generateContent"
)
_GEMINI_404_LOGGED = False


def _log_gemini_404_once(status: int, body: str = "") -> None:
    """Log a clear, one-time message if the configured model or endpoint is gone."""
    global _GEMINI_404_LOGGED
    if status == 404 and not _GEMINI_404_LOGGED:
        _GEMINI_404_LOGGED = True
        log.error(
            f"Gemini returned 404 for model '{GEMINI_MODEL}' on API '{GEMINI_API_VERSION}'. "
            f"The model name or endpoint version is probably retired. Set GEMINI_MODEL "
            f"(and GEMINI_API_VERSION if needed) to a current value. Response: {body[:200]}"
        )

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
#   ai_institution_name     e.g. "a provincial housing Crown corporation"
#   ai_institution_role     e.g. "program analyst at a provincial housing Crown corporation"
#   ai_priority_domains     — comma-separated list of domains that score 9-10
#   ai_priority_context     — 1-2 sentences describing what matters most
#   ai_why_matters_examples — JSON array of example why_it_matters strings
#   ai_extra_high_terms     — comma-separated additional HIGH_RELEVANCE_TERMS
#   ai_extra_low_terms      — comma-separated additional LOW_RELEVANCE_TERMS

_PROMPT_CONFIG_DEFAULTS: dict = {
    "ai_institution_name":     "a provincial housing Crown corporation",
    "ai_institution_role":     "program analyst at a provincial housing Crown corporation",
    "ai_priority_domains":     "Supportive Housing, Shelter and Emergency, Encampments, Funding and Budget, Operator Standards",
    "ai_priority_context": (
        "supportive housing and homelessness programs in BC, shelter and emergency response, "
        "encampments, rent supplements, Indigenous homelessness, operator oversight, and "
        "provincial and federal housing funding"
    ),
    "ai_why_matters_examples": json.dumps([
        (
            "The new per-diem rate for supportive housing sites changes the budget baseline in "
            "operating agreements, so variance analysis for affected sites should be re-based "
            "before the next quarterly review."
        ),
        (
            "The extension of Reaching Home funding for Indigenous homelessness projects changes "
            "the available funding envelope for urban Indigenous providers, so service agreement "
            "renewals should be checked against the new allocation before the next intake window."
        ),
        (
            "The Auditor General's review of shelter operator oversight points to gaps in "
            "reporting requirements, so the indicator set and reporting frequency in current "
            "monitoring tools should be reviewed ahead of the follow-up consultation."
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
    # Core housing and homelessness
    "homeless", "homelessness", "unhoused", "housing", "supportive housing",
    "shelter", "emergency shelter", "encampment", "tent city", "rent supplement",
    "rent assistance", "affordable housing", "social housing", "non-profit housing",
    "tenancy", "tenant", "eviction", "landlord", "housing first",
    "transitional housing", "single room occupancy", "sro", "housing crisis",
    "housing continuum",
    # Programs and bodies
    "bc housing", "cmhc", "reaching home", "belonging in bc", "homeless prevention",
    "community housing fund", "housing hub", "housing supply", "housing accord",
    "built for zero", "point-in-time", "homeless count",
    # Health, harm reduction and support
    "harm reduction", "overdose", "toxic drug", "opioid", "safer supply",
    "substance use", "mental health", "addiction", "supports", "wraparound",
    "outreach", "drug decriminalization",
    # Government and policy
    "provincial", "federal", "municipal", "legislation", "bill", "act",
    "regulation", "policy", "budget", "fiscal", "funding", "grant", "investment",
    "ministry", "minister", "council", "mayor", "auditor general", "audit",
    "evaluation", "report", "standards", "oversight", "operator",
    "service agreement", "per diem",
    # Indigenous housing
    "indigenous", "first nations", "métis", "inuit", "urban indigenous",
    "indigenous-led", "ocap", "reconciliation", "treaty", "on-reserve", "off-reserve",
    # Geography
    "british columbia", "bc", "b.c.", "vancouver", "surrey", "victoria", "burnaby",
    "kelowna", "nanaimo", "downtown eastside", "lower mainland", "fraser health",
    "vancouver coastal",
]

# Very short terms produce false positives as plain substrings ("act" inside
# "react", "bc" inside "abcd", "bill" inside "billion"). These are matched with
# word boundaries instead. Longer terms stay as substring matches.
_WORD_BOUNDARY_TERMS = ("act", "bc", "sro", "bill")
_WORD_BOUNDARY_RE = {t: re.compile(r"\b" + re.escape(t) + r"\b") for t in _WORD_BOUNDARY_TERMS}


def _term_hit(term: str, lowered: str) -> bool:
    """True if term matches lowered title. Short terms use word boundaries."""
    rx = _WORD_BOUNDARY_RE.get(term)
    if rx is not None:
        return rx.search(lowered) is not None
    return term in lowered


LOW_RELEVANCE_TERMS: list[str] = [
    # Sports
    "nba", "nfl", "nhl", "mlb", "fifa", "hockey game",
    "basketball", "baseball game", "soccer match", "golf tournament",
    "tennis match", "formula 1", "nascar", "wrestling",
    # Entertainment and celebrity
    "celebrity", "box office", "album release",
    "music video", "red carpet", "awards show", "oscars", "emmys",
    "reality tv", "bachelor", "survivor",
    # Lifestyle noise
    "weather forecast", "horoscope", "astrology", "recipe", "cooking tips",
    "lottery", "casino", "gambling", "real estate listing",
    "stock tip", "crypto price", "bitcoin price",
    # Real estate noise that is not housing policy
    "open house", "mortgage rates", "home staging",
]

# Sources that get an automatic score bump so they clear the pre-filter even
# without keyword hits. Official housing, health, and government sources are
# almost always relevant and should not be dropped by the keyword pre-filter.
# Keys are lowercase substrings of the source name.
TRUSTED_SOURCE_BOOSTS: dict[str, int] = {
    # Housing bodies
    "bc housing":                           30,
    "bc ministry":                          25,
    "bc government newsroom":               30,
    "cmhc":                                 30,
    "reaching home":                        30,
    "homelessness services association":    30,
    "bc non-profit housing":                30,
    "canadian alliance to end homelessness": 30,
    "auditor general":                      30,
    "bc coroners":                          25,
    # Indigenous
    "crown-indigenous":                     25,
    "first nations health authority":       30,
    "fnha":                                 30,
    "bc first nations":                     25,
    # Municipal
    "city of vancouver":                    20,
    "city of surrey":                       20,
    "city of victoria":                     20,
    # Policy commentary and research
    "policy options":                       20,
    "yellowhead":                           20,
    "homeless hub":                         25,
    "canadian centre for policy":           20,
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
        if _term_hit(term, lowered):
            score += 15

    for term in LOW_RELEVANCE_TERMS:
        if _term_hit(term, lowered):
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

    return f"""You are a senior Canadian housing and homelessness policy analyst specializing in:
- {priority_ctx}
- Supportive housing, shelter and emergency response, encampments, rent supplements, and Housing First
- Indigenous homelessness and Indigenous-led housing providers
- Operator oversight, service agreements, and provincial and federal housing funding

You analyze news articles and return structured JSON assessments for {institution}.

DOMAIN OPTIONS (return as a JSON array, usually 1 item, up to 3 for genuinely cross-cutting articles):
Supportive Housing | Shelter and Emergency | Encampments | Rent Supplements | Indigenous Housing | Health and Harm Reduction | Funding and Budget | Operator Standards | Tenancy and Legislation | Evidence and Evaluation | Affordable Housing | Political | Other

DOMAIN ARRAY RULES:

ONE domain (default): Use for articles that are primarily about one policy area, even if others
are mentioned in passing.

TWO domains: Use when the article is substantively about two distinct policy areas and a reader
monitoring either one would need to see it. This is common for:
- A funding announcement for Indigenous homelessness projects: ["Indigenous Housing", "Funding and Budget"]
- A shelter oversight audit: ["Shelter and Emergency", "Operator Standards"]
- A supportive housing site opening with health supports: ["Supportive Housing", "Health and Harm Reduction"]
- A tenancy bill that affects rent supplements: ["Tenancy and Legislation", "Rent Supplements"]
Ask: "Would a monitor of domain B miss something actionable if I only tagged domain A?" If yes, add B.

THREE domains: Rare. Only when three distinct areas are each substantively covered with their own
detail, not just mentioned. Each domain has its own mechanism, stakeholders, and implications in
the article.

HARD LIMITS:
- Never return more than 3 domains.
- Tags capture cross-cutting themes (Funding, Audit, Urgent). Do not add a domain just to match a tag.
- When genuinely unsure whether a second domain applies, include it. Over-tagging by one domain
  is less harmful than a program analyst missing a relevant article entirely.

JURISDICTION OPTIONS: Federal | BC | Municipal | Regional | Pan-Canadian | International

SENTIMENT OPTIONS: Critical | Supportive | Neutral

TAG OPTIONS (pick up to 4 that apply):
Urgent | Briefing Note Worthy | Housing | Supportive Housing | Shelter | Encampments | Rent Supplement | Housing First | Indigenous | Toxic Drug Crisis | Operator | Funding | Budget | Legislation | Regulation | Consultation | Evaluation | Data/Evidence | Audit | Court Decision | Tenancy | Mental Health | Reaching Home | International

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
            '- "The new per-diem rate for supportive housing sites changes the budget baseline in operating agreements, so variance analysis should be re-based before the next quarterly review."\n'
            '- "The Reaching Home extension changes the funding envelope for Indigenous homelessness providers, so service agreement renewals should be checked against the new allocation."\n'
            '- "The audit of shelter operator oversight points to reporting gaps, so indicator sets in monitoring tools should be reviewed before the follow-up consultation."'
        )

    full_prompt = f"""Analyze this article and return a JSON object with these exact fields:

{{
  "domain": ["<primary domain>", "<optional second domain>", "<optional third domain, rare>"],
  "jurisdiction": "<single jurisdiction from list>",
  "relevance": <integer 1-10, where 10 = critical for {priority_doms}>,
  "sentiment": "<Critical|Supportive|Neutral toward government policy>",
  "summary": "<2-3 sentences summarizing what the article actually says, drawn from the article text, not just the title>",
  "why_it_matters": "<2-3 sentences, see guidance below>",
  "tags": ["<tag1>", "<tag2>"]
}}

domain is a JSON array. Use 1 item for single-domain articles, 2 items when genuinely cross-cutting, 3 items only when all three are substantively covered (rare). See DOMAIN ARRAY RULES above.

TITLE: <TITLE>

Relevance scoring guide:
9-10: Directly affects supportive housing or homelessness programs, funding envelopes, operator obligations, or monitoring requirements in BC
7-8: Relevant to the sector, worth monitoring
6: Tangential, include if space allows
1-5: Return null

WHY IT MATTERS: guidance for writing a strong entry:
The reader is a {role}. Write 2-3 sentences that answer: what does this mean for
service agreements, funding allocations, operator reporting, monitoring tools, or delivery risk
right now? Name the specific mechanism (funding envelope, reporting requirement, standard,
consultation window, legislative change). Be concrete and action-oriented.

GOOD examples:
{good_examples}

BAD examples (too generic, never write these):
- "This article may be relevant to policy professionals monitoring government activity."
- "Review this article for potential relevance to your policy priorities."
- "This could have implications for housing stakeholders."

If relevance would be 5 or below, return exactly: null"""

    title_only_prompt = f"""Analyze this article and return a JSON object with these exact fields:

{{
  "domain": ["<primary domain>", "<optional second domain>", "<optional third domain, rare>"],
  "jurisdiction": "<single jurisdiction from list>",
  "relevance": <integer 1-10, where 10 = critical for {priority_doms}>,
  "sentiment": "<Critical|Supportive|Neutral toward government policy>",
  "summary": "<2-3 sentences summarizing the likely policy significance based on the title and source>",
  "why_it_matters": "<2-3 sentences, see guidance below>",
  "tags": ["<tag1>", "<tag2>"]
}}

domain is a JSON array. Use 1 item for single-domain articles, 2 items when genuinely cross-cutting, 3 items only when all three are substantively covered (rare). See DOMAIN ARRAY RULES above.

TITLE: <TITLE>
SOURCE: <SOURCE>
URL: <URL>

Relevance scoring guide:
9-10: Directly affects supportive housing or homelessness programs, funding envelopes, operator obligations, or monitoring requirements in BC
7-8: Relevant to the sector, worth monitoring
6: Tangential, include if space allows
1-5: Return null

WHY IT MATTERS: guidance for writing a strong entry:
The reader is a {role}. Write 2-3 sentences that answer: what does this mean for
service agreements, funding allocations, operator reporting, monitoring tools, or delivery risk
right now? Name the specific mechanism (funding envelope, reporting requirement, standard,
consultation window, legislative change). Be concrete and action-oriented.

GOOD examples:
{good_examples}

BAD examples (too generic, never write these):
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


def _normalize_domain(raw) -> str:
    """Normalise the AI's domain field into the comma-separated string format
    the DB column and frontend already expect.

    Accepts three shapes the AI may return:
      Array   → ["Health", "Research Funding"]         (correct)
      String  → "Health"                               (single domain)
      String  → "Health, Research Funding"             (model ignored array schema)
      String  → "Health,Research Funding"              (same, no space)

    All are normalised to "Health,Research Funding" (no leading/trailing spaces,
    comma-joined, deduped, hard-capped at 3).
    """
    if isinstance(raw, list):
        # Correct array format — each element may itself be a comma-separated
        # string if the model nested them, so split each element too.
        parts = []
        for item in raw:
            for seg in str(item).split(","):
                seg = seg.strip()
                if seg:
                    parts.append(seg)
    elif isinstance(raw, str) and raw.strip():
        # Model returned a plain string — split on commas so
        # "Health, Research Funding" becomes ["Health", "Research Funding"]
        parts = [seg.strip() for seg in raw.split(",") if seg.strip()]
    else:
        parts = []

    seen: set[str] = set()
    deduped: list[str] = []
    for d in parts:
        if d not in seen:
            seen.add(d)
            deduped.append(d)

    deduped = deduped[:3]  # hard cap — matches the prompt's HARD LIMITS section
    return ",".join(deduped) if deduped else "Other"


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

    # Curated sources (TRUSTED_SOURCE_BOOSTS) keep items scoring 3 or higher, so thin
    # stub pages from official sources are not dropped. Everything else needs 6.
    s_lower = source_name.lower()
    is_trusted = any(k in s_lower for k in TRUSTED_SOURCE_BOOSTS)
    min_rel = 3 if is_trusted else 6
    if result.get("relevance", 0) < min_rel:
        log.debug(f"  [ai-filter] relevance {result.get('relevance', 0)} < {min_rel} dropped: {title[:60]}")
        return None

    return {
        "domain":         _normalize_domain(result.get("domain", "Other")),
        "jurisdiction":   result.get("jurisdiction", "Unknown"),
        "relevance":      int(result.get("relevance", 6)),
        "sentiment":      result.get("sentiment", "Neutral"),
        "summary":        result.get("summary", title),
        "why_it_matters": result.get("why_it_matters",
                                     "Review this article for potential relevance to your policy priorities."),
        "tags":           result.get("tags", []),
    }


# ── GROQ (shared helper) ─────────────────────────────────────────────────────
# Groq retired llama-3.3-70b-versatile on 16 Aug 2026, which made every Groq
# call return 404. The model is now configurable with GROQ_MODEL, and the
# helper walks a fallback list if the configured model has been retired.
# Verify names against https://console.groq.com/docs/models before changing.
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL_CANDIDATES: list[str] = []
for _m in (os.environ.get("GROQ_MODEL", "").strip(), "openai/gpt-oss-120b",
           "qwen/qwen3.6-27b", "openai/gpt-oss-20b"):
    if _m and _m not in GROQ_MODEL_CANDIDATES:
        GROQ_MODEL_CANDIDATES.append(_m)
_GROQ_WORKING_MODEL: str | None = None


class GroqError(Exception):
    """Raised with a message that is safe and useful to show to the user."""
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


def _groq_models_to_try() -> list[str]:
    if _GROQ_WORKING_MODEL and _GROQ_WORKING_MODEL in GROQ_MODEL_CANDIDATES:
        rest = [m for m in GROQ_MODEL_CANDIDATES if m != _GROQ_WORKING_MODEL]
        return [_GROQ_WORKING_MODEL] + rest
    return list(GROQ_MODEL_CANDIDATES)


def _groq_payload(model: str, messages: list, max_tokens: int, temperature: float) -> dict:
    payload = {
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
    }
    if model.startswith("openai/gpt-oss"):
        # gpt-oss spends part of max_tokens on hidden reasoning. Keep it short
        # and add headroom so the visible answer is not cut off.
        payload["reasoning_effort"] = "low"
        payload["max_tokens"] = max_tokens + 800
    return payload


def _groq_error_text(resp) -> str:
    try:
        err = resp.json().get("error", {})
        return str(err.get("message") or resp.text)[:300]
    except Exception:
        return (resp.text or "")[:300]


def _groq_model_is_gone(status: int, text: str) -> bool:
    t = text.lower()
    return status == 404 or (status == 400 and any(
        w in t for w in ("model", "decommission", "not found", "does not exist")))


def _groq_content(data: dict) -> str:
    content = (data.get("choices") or [{}])[0].get("message", {}).get("content") or ""
    content = re.sub(r"<think>.*?</think>", "", content, flags=re.S).strip()
    if not content:
        raise GroqError(502, "Groq returned an empty response. Try again.")
    return content


def _groq_raise(status: int, text: str):
    if status == 401:
        raise GroqError(503, "Groq rejected the GROQ_API_KEY set in Render (invalid or expired). "
                             "Create a new key at console.groq.com and update it in Render.")
    if status == 429:
        raise GroqError(429, "Groq rate limit reached. Wait a moment and try again.")
    raise GroqError(502, f"Groq error {status}: {text}")


async def groq_chat_async(messages: list, max_tokens: int = 1000,
                          temperature: float = 0.2, timeout: float = 45) -> tuple[str, str]:
    """Returns (content, model_used). Raises GroqError on failure."""
    global _GROQ_WORKING_MODEL
    key = os.environ.get("GROQ_API_KEY", "")
    if not key:
        raise GroqError(503, "GROQ_API_KEY is not set in Render environment variables. "
                             "Get a free key at https://console.groq.com and add it in Render.")
    last = ""
    async with httpx.AsyncClient(timeout=timeout) as client:
        for model in _groq_models_to_try():
            resp = await client.post(
                GROQ_URL, json=_groq_payload(model, messages, max_tokens, temperature),
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
            if resp.status_code == 200:
                _GROQ_WORKING_MODEL = model
                return _groq_content(resp.json()), model
            last = _groq_error_text(resp)
            if _groq_model_is_gone(resp.status_code, last):
                log.warning(f"Groq model '{model}' unavailable ({resp.status_code}): {last[:120]}")
                continue
            _groq_raise(resp.status_code, last)
    raise GroqError(502, "Groq says none of the configured models exist "
                         f"({', '.join(GROQ_MODEL_CANDIDATES)}). Set GROQ_MODEL in Render to a "
                         f"current model from console.groq.com/docs/models. Last error: {last}")


def groq_chat_sync(messages: list, max_tokens: int = 1000,
                   temperature: float = 0.2, timeout: float = 30) -> tuple[str, str]:
    """Synchronous twin of groq_chat_async, used by the scraper fallback."""
    global _GROQ_WORKING_MODEL
    key = os.environ.get("GROQ_API_KEY", "")
    if not key:
        raise GroqError(503, "GROQ_API_KEY is not set.")
    last = ""
    for model in _groq_models_to_try():
        resp = requests.post(
            GROQ_URL, json=_groq_payload(model, messages, max_tokens, temperature),
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            timeout=timeout)
        if resp.status_code == 200:
            _GROQ_WORKING_MODEL = model
            return _groq_content(resp.json()), model
        last = _groq_error_text(resp)
        if _groq_model_is_gone(resp.status_code, last):
            continue
        _groq_raise(resp.status_code, last)
    raise GroqError(502, f"No Groq model available. Last error: {last}")


# ── GROQ FALLBACK ─────────────────────────────────────────────────────────────

def _analyze_with_groq(title: str, url: str, source_name: str = "",
                        article_text: str = "") -> dict | None:
    """Fallback classifier through Groq when GEMINI_API_KEY is absent or Gemini
    fails. Uses the same prompt and JSON schema as the Gemini path so
    _parse_gemini_response() can handle the output directly.

    Returns None on any problem so the caller can chain to _default_analysis().
    """
    if not os.environ.get("GROQ_API_KEY", ""):
        return None

    cfg           = _load_prompt_config()
    system_prompt = _build_system_prompt(cfg)
    full_template, title_only_template = _build_analysis_prompts(cfg)

    has_text = bool(article_text and len(article_text.strip()) > 150)
    if has_text:
        prompt = (full_template
                  .replace("<TITLE>", title)
                  .replace("<SOURCE>", source_name)
                  .replace("<URL>", url)
                  .replace("<ARTICLE_TEXT>", article_text.strip()[:6000]))
    else:
        prompt = (title_only_template
                  .replace("<TITLE>", title)
                  .replace("<SOURCE>", source_name)
                  .replace("<URL>", url))

    try:
        text, _model = groq_chat_sync(
            [{"role": "system", "content": system_prompt},
             {"role": "user", "content": prompt}],
            max_tokens=500, temperature=0.1)
        return _parse_gemini_response(text, title, source_name)
    except GroqError as e:
        log.warning(f"Groq fallback error: {e.message}")
        return None
    except Exception as e:
        log.warning(f"Groq fallback error: {e}")
        return None


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
        log.warning("GEMINI_API_KEY not set — trying Groq fallback, then keyword defaults")
        groq_result = _analyze_with_groq(title, url, source_name, article_text)
        if groq_result is not None:
            return groq_result
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

            # A 404 means the model or endpoint version is gone. Retrying cannot help.
            if resp.status_code == 404:
                _log_gemini_404_once(404, resp.text)
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

                    if resp.status_code == 404:
                        _log_gemini_404_once(404, resp.text)
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
            {"title": "Encampment response plan approved", "url": "https://...", "source_name": "BC Gov", "article_text": "..."},
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

    Applies the same housing and homelessness domain taxonomy as the AI prompt
    so results are consistent. Uses the DB-configured priority domains to decide
    which categories earn higher relevance scores.

    Args:
        title:          Article headline string.
        source_name:    Name of the originating source.
        allow_fallback: If False, returns None instead of a minimum-relevance
                        result when no policy keywords match. Used by the
                        scholarly scraper so genuinely off-topic papers are
                        filtered out rather than saved with a floor score.
    """
    title_lower = title.lower()

    try:
        cfg = _load_prompt_config()
        priority_domains_raw = cfg.get("ai_priority_domains", _PROMPT_CONFIG_DEFAULTS["ai_priority_domains"])
        priority_domains = [d.strip().lower() for d in priority_domains_raw.split(",")]
    except Exception:
        priority_domains = ["supportive housing", "shelter and emergency", "encampments",
                            "funding and budget", "operator standards"]

    # Domain detection, ordered from most to least specific.
    domain_rules = [
        # Indigenous housing and homelessness
        (["indigenous", "first nations", "métis", "inuit", "reconcili", "ocap",
          "treaty", "urban indigenous", "indigenous-led"],
         "Indigenous Housing"),
        # Encampments
        (["encampment", "tent city"], "Encampments"),
        # Supportive housing
        (["supportive housing", "housing first", "transitional housing",
          "single room occupancy", "sro"],
         "Supportive Housing"),
        # Shelter and emergency response, general homelessness
        (["homeless", "unhoused", "shelter", "point-in-time", "homeless count"],
         "Shelter and Emergency"),
        # Rent supplements
        (["rent supplement", "rent assistance"], "Rent Supplements"),
        # Tenancy and legislation
        (["tenancy", "tenant", "eviction", "landlord", "legislation", "bill", "act",
          "regulation"],
         "Tenancy and Legislation"),
        # Operator standards and oversight
        (["operator", "oversight", "service agreement", "per diem", "standards",
          "auditor general", "audit"],
         "Operator Standards"),
        # Health and harm reduction
        (["harm reduction", "overdose", "toxic drug", "opioid", "safer supply",
          "substance use", "mental health", "addiction"],
         "Health and Harm Reduction"),
        # Funding and budget
        (["funding", "budget", "fiscal", "grant", "investment", "billion", "million",
          "reaching home"],
         "Funding and Budget"),
        # Evidence and evaluation
        (["evaluation", "report", "research", "data", "evidence"],
         "Evidence and Evaluation"),
        # General housing
        (["affordable housing", "social housing", "non-profit housing", "housing"],
         "Affordable Housing"),
    ]

    why_default = (
        "Review this item for effects on service agreements, funding allocations, "
        "operator reporting, or delivery risk in housing and homelessness programs."
    )

    for keywords, domain in domain_rules:
        if any(_term_hit(kw, title_lower) for kw in keywords):
            is_priority = any(p in domain.lower() for p in priority_domains)
            relevance = 8 if is_priority else 6
            return {
                "domain":         domain,
                "jurisdiction":   "BC",
                "relevance":      relevance,
                "sentiment":      "Neutral",
                "summary":        f"{title}. From {source_name}.",
                "why_it_matters": why_default,
                "tags":           [],
            }

    # No policy keywords matched at all.
    if not allow_fallback:
        return None  # scholarly scraper uses this to drop off-topic papers

    return {
        "domain":         "Other",
        "jurisdiction":   "BC",
        "relevance":      6,
        "sentiment":      "Neutral",
        "summary":        f"{title}. From {source_name}.",
        "why_it_matters": why_default,
        "tags":           [],
    }
