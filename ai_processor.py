"""
PolicyPulse AI Processor — Gemini API
Analyzes each article using its FULL TEXT for accurate:
  - Domain, jurisdiction, relevance, sentiment tagging
  - 2-3 sentence policy-focused summary (from real content)
  - "Why It Matters" tailored to BC/Canadian policy professionals
Only keeps articles scoring 6+.
"""

import json
import logging
import os
import time
import re

import requests

log = logging.getLogger(__name__)

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_URL = "https://generativelanguage.googleapis.com/v1/models/gemini-1.5-flash:generateContent"

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


# ── PROMPT WHEN WE HAVE FULL ARTICLE BODY ────────────────

ANALYSIS_PROMPT_WITH_BODY = """Analyze this article and return a JSON object with these exact fields:

{{
  "domain": "<single domain from list>",
  "jurisdiction": "<single jurisdiction from list>",
  "relevance": <integer 1-10>,
  "sentiment": "<Critical|Supportive|Neutral toward government policy>",
  "summary": "<2-3 sentences capturing the key policy facts and decisions from the article body — be specific, cite numbers/names if present>",
  "why_it_matters": "<1-2 sentences on the concrete implications for a BC university government relations or Indigenous policy team — what action or awareness does this require?>",
  "tags": ["<tag1>", "<tag2>"]
}}

Relevance scoring guide (relative to BC/Canada post-secondary and Indigenous policy):
9-10: Critical — directly affects post-secondary funding, Indigenous rights, BC government relations; brief-worthy
7-8: High — clearly relevant to sector, worth monitoring closely
6: Moderate — tangentially relevant, include if space allows
1-5: Low — return exactly: null

TITLE: {title}
SOURCE: {source}
URL: {url}

ARTICLE BODY:
{body}

Write the summary and why_it_matters FROM THE ARTICLE BODY above — not just the title.
If the body is empty or uninformative, base analysis on the title alone but note uncertainty.
If relevance would be 5 or below, return exactly: null"""


# ── PROMPT WHEN WE ONLY HAVE THE TITLE (fallback) ────────

ANALYSIS_PROMPT_TITLE_ONLY = """Analyze this article and return a JSON object with these exact fields:

{{
  "domain": "<single domain from list>",
  "jurisdiction": "<single jurisdiction from list>",
  "relevance": <integer 1-10>,
  "sentiment": "<Critical|Supportive|Neutral toward government policy>",
  "summary": "<1-2 sentences inferring the likely policy content from the title and source>",
  "why_it_matters": "<1 sentence on the potential implications for a BC university or Indigenous policy team>",
  "tags": ["<tag1>", "<tag2>"]
}}

Relevance scoring guide:
9-10: Critical
7-8: High
6: Moderate
1-5: return exactly: null

TITLE: {title}
SOURCE: {source}
URL: {url}

Note: No article body was available. Base analysis on title and source only.
If relevance would be 5 or below, return exactly: null"""


def analyze_article(
    title: str,
    url: str,
    source_name: str = "",
    article_body: str = "",
) -> dict | None:
    """
    Call Gemini to analyze an article.

    Now accepts article_body — the full fetched text of the article page.
    When body is provided, Gemini generates a real summary and why-it-matters
    grounded in the actual content rather than guessing from the title alone.

    Returns dict with domain/relevance/sentiment/summary/why_it_matters/tags,
    or None if relevance < 6 or on persistent error.
    """
    if not GEMINI_API_KEY:
        log.warning("GEMINI_API_KEY not set — using keyword-based defaults")
        return _default_analysis(title, source_name)

    # Choose prompt based on whether we have body text
    has_body = bool(article_body and len(article_body.strip()) > 100)

    if has_body:
        # Truncate body to keep within token limits (Gemini flash handles ~8k tokens well)
        body_snippet = article_body.strip()[:3500]
        prompt = ANALYSIS_PROMPT_WITH_BODY.format(
            title=title,
            source=source_name,
            url=url,
            body=body_snippet,
        )
    else:
        prompt = ANALYSIS_PROMPT_TITLE_ONLY.format(
            title=title,
            source=source_name,
            url=url,
        )

    payload = {
        "contents": [
            {
                "parts": [
                    {"text": SYSTEM_PROMPT + "\n\n" + prompt}
                ]
            }
        ],
        "generationConfig": {
            "temperature": 0.15,
            "maxOutputTokens": 500,
        },
    }

    for attempt in range(3):
        try:
            resp = requests.post(
                f"{GEMINI_URL}?key={GEMINI_API_KEY}",
                json=payload,
                timeout=25,
            )
            resp.raise_for_status()
            data = resp.json()

            text = data["candidates"][0]["content"]["parts"][0]["text"].strip()

            # Handle null response (low relevance)
            if text.lower() in ("null", ""):
                log.debug(f"AI scored low relevance: {title[:60]}")
                return None

            # Strip markdown fences if present
            text = re.sub(r"^```json\s*", "", text)
            text = re.sub(r"\s*```$", "", text)

            result = json.loads(text)

            # Final relevance gate
            if result.get("relevance", 0) < 6:
                return None

            # Validate summary and why_it_matters are real content, not placeholders
            summary = result.get("summary", "").strip()
            why     = result.get("why_it_matters", "").strip()

            # If AI returned empty or trivial strings, fall back gracefully
            if len(summary) < 20:
                summary = f"{title} — reported by {source_name}."
            if len(why) < 20:
                why = "Review this article for potential relevance to your policy priorities."

            return {
                "domain":          result.get("domain", "Other"),
                "jurisdiction":    result.get("jurisdiction", "Unknown"),
                "relevance":       int(result.get("relevance", 6)),
                "sentiment":       result.get("sentiment", "Neutral"),
                "summary":         summary,
                "why_it_matters":  why,
                "tags":            result.get("tags", []),
            }

        except json.JSONDecodeError as e:
            log.warning(f"JSON parse error attempt {attempt+1}: {e} — raw: {text[:200]}")
            if attempt < 2:
                time.sleep(1)
        except requests.RequestException as e:
            log.warning(f"Gemini API request error attempt {attempt+1}: {e}")
            if attempt < 2:
                time.sleep(2)
        except (KeyError, IndexError) as e:
            log.warning(f"Gemini response structure error: {e}")
            break

    # All attempts failed — use keyword fallback rather than dropping the article
    log.warning(f"All Gemini attempts failed for: {title[:60]} — using defaults")
    return _default_analysis(title, source_name)


def _default_analysis(title: str, source_name: str) -> dict:
    """
    Keyword-based fallback when Gemini API is unavailable.
    Used when GEMINI_API_KEY is not set or all API attempts fail.
    """
    title_lower = title.lower()

    if any(w in title_lower for w in ["indigenous", "first nations", "métis", "inuit", "reconcili", "dripa", "undrip", "trc", "ocap"]):
        domain = "Indigenous"
        jurisdiction = "BC"
        relevance = 8
        tags = ["Indigenous", "Reconciliation"]
    elif any(w in title_lower for w in ["university", "college", "post-secondary", "postsecondary", "tuition", "student", "campus", "academic"]):
        domain = "Higher Education"
        jurisdiction = "BC"
        relevance = 7
        tags = ["Higher Education"]
    elif any(w in title_lower for w in ["research", "grant", "sshrc", "nserc", "cihr", "funding", "scholarship"]):
        domain = "Research Funding"
        jurisdiction = "Federal"
        relevance = 7
        tags = ["Research", "Funding"]
    elif any(w in title_lower for w in ["budget", "fiscal", "spending", "billion", "million", "deficit", "surplus"]):
        domain = "Budget"
        jurisdiction = "Federal"
        relevance = 6
        tags = ["Budget"]
    elif any(w in title_lower for w in ["health", "pharmacare", "mental health", "wellness", "fnha"]):
        domain = "Health"
        jurisdiction = "BC"
        relevance = 6
        tags = ["Health"]
    elif any(w in title_lower for w in ["bill", "legislation", "act ", " act", "regulation", "law", "statute"]):
        domain = "Legislation"
        jurisdiction = "BC"
        relevance = 6
        tags = ["Legislation"]
    elif any(w in title_lower for w in ["workforce", "labour", "labor", "employment", "jobs", "hiring"]):
        domain = "Workforce"
        jurisdiction = "BC"
        relevance = 6
        tags = ["Workforce"]
    else:
        domain = "Other"
        jurisdiction = "Federal"
        relevance = 6
        tags = []

    return {
        "domain":         domain,
        "jurisdiction":   jurisdiction,
        "relevance":      relevance,
        "sentiment":      "Neutral",
        "summary":        f"{title} — reported by {source_name}. Review for full details.",
        "why_it_matters": "This article may be relevant to your policy portfolio. Review directly to assess impact.",
        "tags":           tags,
    }
