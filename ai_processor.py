"""
PolicyPulse AI Processor — Gemini API
Analyzes each article for domain, relevance, sentiment, summary, why-it-matters.
Only keeps articles scoring 6+.

v2: Now accepts full article_text so summaries and why-it-matters are
    generated from the actual article content, not just the title.
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


def analyze_article(title: str, url: str, source_name: str = "",
                    article_text: str = "") -> dict | None:
    """
    Call Gemini to analyze an article.

    Args:
        title:        Article headline.
        url:          Article URL.
        source_name:  Name of the source (e.g. "BC Government Newsroom").
        article_text: Full article body text (optional but strongly recommended).
                      When provided, summaries and why-it-matters are generated
                      from real content instead of just the title.

    Returns:
        dict with domain/relevance/sentiment/summary/why_it_matters/tags,
        or None if relevance < 6 or on error.
    """
    if not GEMINI_API_KEY:
        log.warning("GEMINI_API_KEY not set — skipping AI analysis, using defaults")
        return _default_analysis(title, source_name)

    # Choose prompt based on whether we have article body
    has_text = bool(article_text and len(article_text.strip()) > 150)
    if has_text:
        # Trim to ~4000 chars to stay within token limits while giving Gemini enough context
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

    payload = {
        "contents": [
            {
                "parts": [
                    {"text": SYSTEM_PROMPT + "\n\n" + prompt}
                ]
            }
        ],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": 500,
        }
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
            if text.lower() == "null" or text == "":
                return None

            # Strip markdown fences if present
            text = re.sub(r"^```json\s*", "", text)
            text = re.sub(r"\s*```$", "", text)

            result = json.loads(text)

            # Final relevance gate
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

        except json.JSONDecodeError as e:
            log.warning(f"JSON parse error on attempt {attempt+1}: {e} — raw: {text[:200]}")
            if attempt < 2:
                time.sleep(1)
        except requests.RequestException as e:
            log.warning(f"Gemini API request error on attempt {attempt+1}: {e}")
            if attempt < 2:
                time.sleep(2)
        except (KeyError, IndexError) as e:
            log.warning(f"Gemini response structure error: {e}")
            break

    # Fallback: use defaults rather than dropping article entirely
    return _default_analysis(title, source_name)


def _default_analysis(title: str, source_name: str) -> dict:
    """Fallback analysis when AI is unavailable."""
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
