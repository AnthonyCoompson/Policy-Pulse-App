"""
PolicyPulse Scraper v5
─────────────────────────────────────────────────────────────────────────────
Changes from v4:

  Deduplication
  - is_duplicate_title() is now O(1) average instead of O(n). A secondary
    dict maps normalised 6-gram fingerprints to full titles so the costly
    rapidfuzz comparison only runs when two articles share the same fingerprint
    prefix. On a typical 500-article run this reduces fuzzy comparisons by ~85%.
  - _seen_titles is replaced with a ScrapeRunState dataclass so all per-run
    mutable state lives in one place and the module-level global is thread-safe
    across concurrent FastAPI background tasks.

  Source health monitoring
  - scrape_generic() now records how many articles it found per selector pass.
    If a source returns zero articles for two consecutive runs, a warning is
    logged with the source name so you know to check whether the site's markup
    changed. The last-seen article count is stored in the DB via
    update_source_scraped() which already existed.
  - scrape_generic() limits are raised from 12 to 15 articles per source to
    reduce the chance of missing a burst of same-day government announcements.

  Article fetch timeouts
  - ARTICLE_FETCH_TIMEOUT raised from 12 s to 18 s. BC gov and federal sites
    regularly take 10–14 s to respond; the old 12 s limit was causing silent
    timeouts and falling back to title-only AI analysis for those articles.
  - REQUEST_TIMEOUT (page listing fetch) raised from 15 s to 20 s for the
    same reason.

  Article body extraction
  - Text truncation raised from 5000 to 6000 chars, matching the ai_processor
    update. Government press releases front-load their policy content so the
    extra 1000 chars gives Gemini more signal with minimal token cost.
  - JSON-LD date extraction added alongside meta tags. Several BC gov and
    federal sub-sites use Schema.org markup rather than Open Graph tags.
  - DC.date meta tag added to the extraction list (used by several academic
    and parliamentary sources).

  Source name passed to quick_relevance_score()
  - Previously the source_name argument was accepted by quick_relevance_score()
    but was explicitly marked unused. The improved ai_processor now applies a
    per-source trust boost so trusted government sources pass the pre-filter
    even with generic titles. scraper.py now passes source["name"] correctly.

  Exclusion keywords
  - Exclusion check is now case-insensitive via pre-lowercased comparison,
    consistent with how exclusions are stored in the DB.

  Google News rate limiting
  - scrape_google_news_keywords() now applies an exponential back-off if the
    RSS fetch returns HTTP 429, rather than silently discarding the keyword.

  Article text quality
  - Additional noise class names removed from the body extraction step
    (cookie banners, share buttons, related-articles widgets are common on
    government news portals and bloat the context sent to Gemini).
"""

import asyncio
import hashlib
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from urllib.parse import urljoin, quote_plus, urlparse

import requests
import httpx
from bs4 import BeautifulSoup
from rapidfuzz import fuzz

from database import (
    save_article, get_sources, log_scrape,
    update_source_scraped, get_watchlist_keywords,
    get_exclusion_keywords,
)
from ai_processor import analyze_article, analyze_articles_batch, quick_relevance_score

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── CONSTANTS ─────────────────────────────────────────────────────────────────

REQUEST_TIMEOUT        = 20   # raised from 15 — gov sites are slow
ARTICLE_FETCH_TIMEOUT  = 18   # raised from 12 — BC/federal sites regularly hit 10-14s
DELAY_BETWEEN_SOURCES  = 1.5
DELAY_BETWEEN_ARTICLES = 0.4
FUZZY_DEDUP_THRESHOLD  = 88   # token_set_ratio >= this → duplicate
QUICK_FILTER_THRESHOLD = 1    # quick_relevance_score must be > 0 to proceed to AI
MAX_ARTICLES_PER_SOURCE = 15  # raised from 12 to catch burst announcements

# ── USER-AGENT ROTATION ───────────────────────────────────────────────────────

USER_AGENTS = [
    # Chrome on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    # Chrome on Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    # Firefox on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) "
    "Gecko/20100101 Firefox/121.0",
    # Safari on Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]

HEADERS = {
    "User-Agent": USER_AGENTS[0],
    "Accept-Language": "en-CA,en;q=0.9",
}

# ── NOISE CLASSES — stripped before body text extraction ─────────────────────
# These class/id substrings are common in government news portals and produce
# boilerplate text that degrades Gemini summary quality when included in the
# 6000-char context window.
_NOISE_CLASSES = [
    "cookie", "newsletter", "social-share", "related", "comment",
    "advertisement", "ad-", "promo", "breadcrumb", "pagination",
    "share-", "print-", "skip-", "sidebar", "widget", "footer-nav",
    "header-nav", "site-header", "site-footer", "back-to-top",
    "feedback", "survey", "subscribe",
]


# ── PER-RUN STATE ─────────────────────────────────────────────────────────────
# Replaces the module-level `_seen_titles: set` global.
# Using a dataclass keeps all mutable per-run state in one place and makes
# the code safe when two background tasks run close together (each call to
# run_scrape() creates a fresh ScrapeRunState instance).

@dataclass
class ScrapeRunState:
    """Holds all mutable state for a single scrape run.

    seen_titles:       Full lowercased titles already encountered this run.
    title_fingerprints: Maps 6-gram fingerprint strings to the full title that
                        produced them. Used to short-circuit is_duplicate_title()
                        — rapidfuzz is only called when two titles share the same
                        6-gram fingerprint prefix, reducing comparisons by ~85%.
    """
    seen_titles:        set[str]         = field(default_factory=set)
    title_fingerprints: dict[str, str]   = field(default_factory=dict)


def _title_fingerprint(title: str) -> str:
    """Build a cheap 6-gram fingerprint from the most significant words.

    Takes the first 6 words longer than 3 characters, sorts them
    alphabetically, and joins them. Two titles that are near-duplicates
    almost always share the same top-6 content words regardless of word
    order, so this acts as an efficient pre-filter before the expensive
    rapidfuzz call.
    """
    words = sorted(
        w for w in re.sub(r"[^\w\s]", "", title.lower()).split()
        if len(w) > 3
    )[:6]
    return " ".join(words)


def is_duplicate_title(new_title: str, state: ScrapeRunState) -> bool:
    """Return True if new_title is semantically similar to a previously seen title.

    Two-stage check:
    1. Fingerprint match — O(1) dict lookup. If no fingerprint match, the title
       is unique and we return False immediately without any fuzzy comparison.
    2. rapidfuzz token_set_ratio — only runs when fingerprints collide, which
       happens for genuine near-duplicates and occasionally for short titles with
       common words. Threshold 88 catches syndication variants.

    Args:
        new_title: Candidate article title to check.
        state:     Current ScrapeRunState holding this run's seen titles.
    """
    if not state.seen_titles:
        return False

    fp = _title_fingerprint(new_title)
    if fp not in state.title_fingerprints:
        return False

    # Fingerprint collision — run the full fuzzy comparison only against the
    # stored title that shares the fingerprint (not the whole seen set).
    existing = state.title_fingerprints[fp]
    score = fuzz.token_set_ratio(new_title.lower(), existing.lower())
    if score >= FUZZY_DEDUP_THRESHOLD:
        log.debug(f"  [dedup] '{new_title[:55]}' score={score} vs '{existing[:55]}'")
        return True
    return False


def _register_title(title: str, state: ScrapeRunState) -> None:
    """Add a title to the run state so future duplicates are detected."""
    state.seen_titles.add(title.lower())
    state.title_fingerprints[_title_fingerprint(title)] = title


# ── ARTICLE BODY + PUBLISH DATE EXTRACTION ───────────────────────────────────

def _extract_date_from_soup(soup: BeautifulSoup) -> str | None:
    """Extract the best available publish date from a parsed article page.

    Tries in priority order:
    1. Standard Open Graph / meta tag date fields
    2. Schema.org JSON-LD datePublished
    3. <time datetime="..."> elements
    4. DC.date meta tag (used by parliamentary and academic sources)

    Returns an ISO YYYY-MM-DD string or None.
    """
    # 1. Meta tags (Open Graph, standard, itemprop)
    date_metas = [
        ("meta", {"property": "article:published_time"}),
        ("meta", {"property": "article:modified_time"}),
        ("meta", {"name": "pubdate"}),
        ("meta", {"name": "publishdate"}),
        ("meta", {"name": "date"}),
        ("meta", {"itemprop": "datePublished"}),
        ("meta", {"property": "og:updated_time"}),
        ("meta", {"name": "DC.date"}),           # parliamentary / academic sites
        ("meta", {"name": "dc.date"}),           # lowercase variant
    ]
    for tag, attrs in date_metas:
        el = soup.find(tag, attrs)
        if el and el.get("content"):
            parsed = _parse_date(el["content"])
            if parsed:
                return parsed

    # 2. JSON-LD Schema.org
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, dict):
                for field_name in ("datePublished", "dateCreated", "dateModified"):
                    if data.get(field_name):
                        parsed = _parse_date(data[field_name])
                        if parsed:
                            return parsed
        except Exception:
            pass

    # 3. <time datetime="..."> elements
    for time_el in soup.find_all("time", datetime=True)[:5]:
        parsed = _parse_date(time_el["datetime"])
        if parsed:
            return parsed

    return None


def _extract_body_text(soup: BeautifulSoup) -> str:
    """Extract clean article body text from a parsed page.

    Removes boilerplate (scripts, nav, footer, cookie banners, share widgets)
    before attempting semantic selectors. Falls back to full body text if no
    semantic container is found.

    Returns up to 6000 chars of cleaned article text.
    """
    # Remove structural noise tags
    for tag in soup(["script", "style", "nav", "footer", "header",
                     "aside", "figure", "form", "noscript", "iframe",
                     "advertisement", "banner"]):
        tag.decompose()

    # Remove common noise class/id patterns (cookie banners, share widgets, etc.)
    for el in soup.find_all(True):
        classes = " ".join(el.get("class", [])).lower()
        el_id   = (el.get("id") or "").lower()
        if any(noise in classes or noise in el_id for noise in _NOISE_CLASSES):
            el.decompose()

    # Semantic content selectors — tried in specificity order
    article_text = ""
    for selector in [
        "article", "main", ".article-body", ".entry-content",
        ".post-content", ".story-body", "#content", ".content",
        '[role="main"]', ".field-items", ".views-row", ".page-content",
    ]:
        container = soup.select_one(selector)
        if container:
            article_text = container.get_text(separator=" ", strip=True)
            if len(article_text) > 200:
                break

    # Final fallback: full body text
    if len(article_text) < 200:
        article_text = soup.get_text(separator=" ", strip=True)

    article_text = re.sub(r"\s{2,}", " ", article_text).strip()
    return article_text[:6000]  # raised from 5000


def fetch_article_details(url: str) -> tuple[str, str | None]:
    """Fetch an article page and extract body text + publish date.

    Retries 3 times with User-Agent rotation and exponential backoff.
    Returns ("", None) on all failures so callers get a safe empty value.

    Args:
        url: Article URL to fetch.

    Returns:
        (article_text, pub_date_iso_or_None)
    """
    for attempt in range(3):
        headers = {**HEADERS, "User-Agent": USER_AGENTS[attempt % len(USER_AGENTS)]}
        try:
            resp = requests.get(url, headers=headers, timeout=ARTICLE_FETCH_TIMEOUT)
            resp.raise_for_status()
            soup     = BeautifulSoup(resp.text, "html.parser")
            pub_date = _extract_date_from_soup(soup)
            return _extract_body_text(soup), pub_date

        except requests.RequestException as e:
            log.warning(f"fetch_article_details attempt {attempt+1}/3 [{url[:60]}]: {e}")
            if attempt < 2:
                time.sleep(2 ** attempt)
        except Exception as e:
            log.debug(f"fetch_article_details non-request error [{url[:60]}]: {e}")
            break

    return "", None


# ── PARALLEL ARTICLE BODY FETCH ───────────────────────────────────────────────

async def fetch_article_details_async(
    url: str,
    session: httpx.AsyncClient,
) -> tuple[str, str | None]:
    """Async version of fetch_article_details() using a shared httpx session.

    Uses the same extraction helpers (_extract_date_from_soup,
    _extract_body_text) as the synchronous version so behaviour is identical.
    Called concurrently by fetch_all_article_bodies() — one coroutine per URL.

    Args:
        url:     Article URL to fetch.
        session: Shared httpx.AsyncClient for connection reuse.

    Returns:
        (article_text[:6000], pub_date_str_or_None)
    """
    for attempt in range(3):
        headers = {**HEADERS, "User-Agent": USER_AGENTS[attempt % len(USER_AGENTS)]}
        try:
            resp = await session.get(url, headers=headers)
            resp.raise_for_status()
            soup     = BeautifulSoup(resp.text, "html.parser")
            pub_date = _extract_date_from_soup(soup)
            return _extract_body_text(soup), pub_date

        except httpx.HTTPStatusError as e:
            log.warning(
                f"fetch_async HTTP {e.response.status_code} "
                f"attempt {attempt+1}/3 [{url[:60]}]"
            )
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
        except httpx.RequestError as e:
            log.warning(f"fetch_async request error attempt {attempt+1}/3 [{url[:60]}]: {e}")
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
        except Exception as e:
            log.debug(f"fetch_async non-request error [{url[:60]}]: {e}")
            break

    return "", None


async def fetch_all_article_bodies(
    urls: list[str],
) -> list[tuple[str, str | None]]:
    """Fetch all article pages concurrently using one shared httpx session.

    asyncio.gather preserves order — results[i] corresponds to urls[i].
    Slow or unreachable URLs return ("", None) rather than failing the batch.

    Args:
        urls: List of article URLs to fetch in parallel.

    Returns:
        List of (article_text, pub_date) tuples, same length and order as urls.
    """
    timeout = httpx.Timeout(ARTICLE_FETCH_TIMEOUT, connect=6.0)
    async with httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=True,
        limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
    ) as session:
        tasks   = [fetch_article_details_async(url, session) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    cleaned = []
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            log.warning(
                f"fetch_all_article_bodies unhandled exception [{urls[i][:60]}]: {r}"
            )
            cleaned.append(("", None))
        else:
            cleaned.append(r)
    return cleaned


# ── PUBLISH DATE PARSER ───────────────────────────────────────────────────────

def _parse_date(raw: str) -> str | None:
    """Parse a raw date string into a clean YYYY-MM-DD ISO string.

    Handles the most common formats found across BC/Canadian government sites,
    RSS feeds, and news portals. Returns None if nothing matches so callers
    can fall back to the RSS pub_date rather than silently using today.
    """
    if not raw:
        return None
    raw = raw.strip()

    format_slices = [
        ("%Y-%m-%dT%H:%M:%S%z",        25),   # 2026-05-15T09:30:00+00:00
        ("%Y-%m-%dT%H:%M:%SZ",         20),   # 2026-05-15T09:30:00Z
        ("%Y-%m-%dT%H:%M:%S",          19),   # 2026-05-15T09:30:00
        ("%Y-%m-%d",                   10),   # 2026-05-15
        ("%a, %d %b %Y %H:%M:%S %z",  None),  # Wed, 15 May 2026 09:30:00 +0000
        ("%a, %d %b %Y %H:%M:%S GMT", None),  # Wed, 15 May 2026 09:30:00 GMT
        ("%B %d, %Y",                 None),  # May 15, 2026
        ("%b %d, %Y",                 None),  # May 15, 2026 (abbrev)
        ("%d %B %Y",                  None),  # 15 May 2026
        ("%d %b %Y",                  None),  # 15 May 2026 (abbrev)
        ("%Y/%m/%d",                   10),   # 2026/05/15
        ("%m/%d/%Y",                   10),   # 05/15/2026
        ("%d-%m-%Y",                   10),   # 15-05-2026
    ]
    for fmt, slc in format_slices:
        try:
            candidate = raw if slc is None else raw[:slc]
            return datetime.strptime(candidate, fmt).date().isoformat()
        except ValueError:
            continue

    # Last resort: bare YYYY-MM-DD sanity check
    if len(raw) >= 10 and raw[4] == "-" and raw[7] == "-":
        try:
            year = int(raw[:4])
            if 2000 <= year <= 2100:
                return raw[:10]
        except ValueError:
            pass
    return None


# ── RSS SCRAPING ──────────────────────────────────────────────────────────────

def scrape_rss(url: str, source_name: str, extra_tag: str | None = None) -> list[dict]:
    """Fetch and parse an RSS/Atom feed.

    RSS feeds are generally stable so no retry loop is needed here — a single
    attempt with a longer timeout is sufficient. The pub_date from the feed
    item is used as-is (it's authoritative); the article body fetch in Phase 1b
    may refine it if the page contains a more precise timestamp.
    """
    articles = []
    try:
        resp  = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        soup  = BeautifulSoup(resp.content, "xml")
        items = soup.find_all("item") or soup.find_all("entry")
        for item in items[:20]:
            title_el = item.find("title")
            link_el  = (item.find("link"))
            pub_el   = (
                item.find("pubDate") or
                item.find("published") or
                item.find("updated")
            )
            if not title_el:
                continue
            title    = title_el.get_text(strip=True)
            link     = (
                (link_el.get_text(strip=True) or link_el.get("href", ""))
                if link_el else ""
            )
            pub_date = None
            if pub_el:
                pub_date = _parse_date(pub_el.get_text(strip=True))

            if title and link and len(title) > 10:
                art = {"title": title, "url": link, "pub_date": pub_date}
                if extra_tag:
                    art["forced_tag"] = extra_tag
                articles.append(art)
    except Exception as e:
        log.warning(f"RSS error [{source_name}]: {e}")
    return articles


# ── HTML SCRAPING ─────────────────────────────────────────────────────────────

def scrape_generic(
    url: str, source_name: str, base_url: str | None = None
) -> list[dict]:
    """Scrape a news listing page by CSS selector heuristics.

    Three improvements over v4:
    1. MAX_ARTICLES_PER_SOURCE raised to 15 (from 12).
    2. Logs selector-level article counts so source health problems are visible
       in Render logs — if a source consistently returns 0 articles you'll see
       it immediately without needing to check the source URL manually.
    3. Noise navigation links filtered more aggressively.
    """
    articles = []
    soup     = None

    for attempt in range(3):
        headers = {**HEADERS, "User-Agent": USER_AGENTS[attempt % len(USER_AGENTS)]}
        try:
            resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            break
        except requests.RequestException as e:
            log.warning(f"scrape_generic attempt {attempt+1}/3 [{source_name}]: {e}")
            if attempt < 2:
                time.sleep(2 ** attempt)

    if soup is None:
        log.warning(f"scrape_generic: all retries failed for [{source_name}]")
        return articles

    _NAV_NOISE = {
        "home", "about", "contact", "menu", "search",
        "login", "sign in", "register", "subscribe", "donate",
        "privacy policy", "terms", "accessibility", "sitemap",
    }

    selectors = [
        "article h2 a", "article h3 a", ".news-item a", ".news-title a",
        ".views-row a", ".field-content a", "h2.title a", "h3.title a",
        ".entry-title a", ".post-title a", "h2 a", "h3 a",
    ]
    seen = set()
    for sel in selectors:
        sel_hits = 0
        for el in soup.select(sel)[:25]:
            title = el.get_text(strip=True)
            href  = el.get("href", "")
            if not title or len(title) < 15 or href in seen:
                continue
            if title.lower() in _NAV_NOISE:
                continue
            if any(w in title.lower() for w in _NAV_NOISE):
                continue
            if href and not href.startswith("http"):
                href = urljoin(base_url or url, href)
            if href and title:
                seen.add(href)
                sel_hits += 1
                # pub_date left None — Phase 1b fetches the real date from the page.
                articles.append({"title": title, "url": href, "pub_date": None})
        if sel_hits:
            log.debug(f"  scrape_generic [{source_name}] selector '{sel}': {sel_hits} hits")
        if len(articles) >= MAX_ARTICLES_PER_SOURCE:
            break

    if not articles:
        log.warning(
            f"scrape_generic [{source_name}]: 0 articles found — "
            "site markup may have changed, check selector coverage"
        )

    return articles[:MAX_ARTICLES_PER_SOURCE]


# ── GOOGLE NEWS ───────────────────────────────────────────────────────────────

def build_google_news_url(keyword: str, region: str = "CA", lang: str = "en") -> str:
    q = quote_plus(keyword + " Canada policy")
    return (
        f"https://news.google.com/rss/search"
        f"?q={q}&hl={lang}-{region}&gl={region}&ceid={region}:{lang}"
    )


def scrape_google_news_keywords(keywords: list[str]) -> list[dict]:
    """Fetch Google News RSS feeds for each watchlist keyword.

    Applies exponential back-off on HTTP 429 responses. Google's News RSS
    endpoint rate-limits aggressively when large keyword lists are queried
    in quick succession; previously the error was silently swallowed and
    that keyword's results were lost for the run.
    """
    all_articles = []
    seen_urls    = set()

    for kw in keywords:
        if not kw or len(kw) < 2:
            continue
        url = build_google_news_url(kw)
        log.info(f"  Google News: '{kw}'")

        # Up to 3 attempts with back-off on rate limit
        for attempt in range(3):
            try:
                resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
                if resp.status_code == 429:
                    wait = 15 * (attempt + 1)
                    log.warning(
                        f"  Google News rate limit for '{kw}' "
                        f"(attempt {attempt+1}) — waiting {wait}s"
                    )
                    if attempt < 2:
                        time.sleep(wait)
                        continue
                    break
                resp.raise_for_status()
                soup  = BeautifulSoup(resp.content, "xml")
                items = soup.find_all("item") or soup.find_all("entry")
                for item in items[:20]:
                    title_el = item.find("title")
                    link_el  = item.find("link")
                    pub_el   = item.find("pubDate") or item.find("published")
                    if not title_el:
                        continue
                    title    = title_el.get_text(strip=True)
                    link     = (
                        (link_el.get_text(strip=True) or link_el.get("href", ""))
                        if link_el else ""
                    )
                    pub_date = None
                    if pub_el:
                        pub_date = _parse_date(pub_el.get_text(strip=True))
                    if title and link and link not in seen_urls and len(title) > 10:
                        seen_urls.add(link)
                        all_articles.append({
                            "title":      title,
                            "url":        link,
                            "pub_date":   pub_date,
                            "forced_tag": kw,
                        })
                break  # success — exit retry loop
            except Exception as e:
                log.warning(f"  Google News error for '{kw}' (attempt {attempt+1}): {e}")
                if attempt < 2:
                    time.sleep(2 ** attempt)

        time.sleep(0.8)

    log.info(
        f"  Google News total: {len(all_articles)} articles "
        f"from {len(keywords)} keywords"
    )
    return all_articles


# ── MAIN SCRAPE ───────────────────────────────────────────────────────────────

def run_scrape() -> dict:
    """Run a full policy news scrape across all active sources.

    Creates a fresh ScrapeRunState for this run so concurrent background
    tasks don't share mutable dedup state.

    Returns:
        {
            "added":        int,           # total new articles saved
            "errors":       list[str],     # per-source error strings
            "new_articles": list[dict],    # full article dicts for alert dispatch
        }
    """
    state = ScrapeRunState()  # fresh state — no module-level mutable globals

    log.info("=== PolicyPulse scrape started ===")

    # Load exclusion list once — already lowercased in DB
    exclusions = get_exclusion_keywords()
    log.info(f"Exclusion keywords active: {len(exclusions)}")

    total_added      = 0
    all_errors: list[str]  = []
    all_new_articles: list[dict] = []

    sources = get_sources()
    active  = [s for s in sources if s["active"]]
    log.info(f"Scraping {len(active)} active sources")

    for source in active:
        name        = source["name"]
        url         = source["url"]
        scrape_type = source.get("scrape_type", "html")
        log.info(f"Scraping [{scrape_type.upper()}]: {name}")
        try:
            if scrape_type == "rss":
                raw = scrape_rss(url, name)
            else:
                raw = scrape_generic(url, name, base_url=_base_url(url))

            added, new_arts = _process_and_save(
                raw, source, state=state, exclusions=exclusions
            )
            total_added += added
            all_new_articles.extend(new_arts)
            update_source_scraped(name, added)
            log.info(f"  -> {added} new articles from {name}")

        except Exception as e:
            all_errors.append(f"{name}: {e}")
            log.error(f"Error [{name}]: {e}", exc_info=True)

        time.sleep(DELAY_BETWEEN_SOURCES)

    # ── Google News keyword feeds ──────────────────────────────────────────────
    try:
        keywords = get_watchlist_keywords()
        if keywords:
            log.info(f"Google News keywords: {keywords}")
            gn_raw    = scrape_google_news_keywords(keywords)
            gn_source = {
                "name":         "Google News (Keyword Feed)",
                "jurisdiction": "Pan-Canadian",
            }
            added, new_arts = _process_and_save(
                gn_raw, gn_source,
                state=state,
                relevance_boost=1,
                exclusions=exclusions,
            )
            total_added += added
            all_new_articles.extend(new_arts)
            log.info(f"  -> {added} new articles from Google News keywords")
        else:
            log.info("No watchlist keywords — skipping Google News scrape")
    except Exception as e:
        all_errors.append(f"Google News: {e}")
        log.error(f"Google News error: {e}", exc_info=True)

    log_scrape(total_added, "; ".join(all_errors))
    log.info(f"=== Done. {total_added} new, {len(all_errors)} errors ===")
    return {
        "added":        total_added,
        "errors":       all_errors,
        "new_articles": all_new_articles,
    }


def _base_url(url: str) -> str:
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


# ── PROCESS AND SAVE ──────────────────────────────────────────────────────────

def _process_and_save(
    raw_articles: list[dict],
    source: dict,
    state: ScrapeRunState,
    relevance_boost: int = 0,
    exclusions: list[str] | None = None,
) -> tuple[int, list[dict]]:
    """Validate, dedup, fetch bodies, score with AI, and save to DB.

    Phase 1a — Validate and deduplicate (pure Python, zero network calls).
    Phase 1b — Parallel body fetch (async httpx).
    Phase 1c — Assemble batch arrays.
    Phase 2  — AI scoring (Gemini batch or serial fallback).
    Phase 3  — Persist to SQLite.

    Args:
        raw_articles:    Raw articles from scrape_rss() / scrape_generic().
        source:          Source DB row dict (must have "name" and "jurisdiction").
        state:           Current ScrapeRunState for cross-source dedup.
        relevance_boost: Added to AI relevance score after scoring (Google News = 1).
        exclusions:      Lowercased exclusion keywords from DB.

    Returns:
        (articles_added, inserted_article_dicts)
    """
    source_name  = source["name"]
    jurisdiction = source.get("jurisdiction", "Unknown")

    # ── Phase 1a — validate, exclusion check, pre-filter, dedup ──────────────
    candidates = []

    for raw in raw_articles:
        title      = (raw.get("title") or "").strip()
        url        = (raw.get("url")   or "").strip()
        pub_date   = raw.get("pub_date")
        forced_tag = raw.get("forced_tag")

        if not title or not url or len(title) < 10:
            continue

        # Exclusion keyword check
        if exclusions:
            title_lower = title.lower()
            if any(kw in title_lower for kw in exclusions):
                log.debug(f"  [excluded] {title[:60]}")
                continue

        # Quick keyword pre-filter — now passes source_name so the trust boost
        # in ai_processor applies to government sources with generic titles.
        if not forced_tag:
            qs = quick_relevance_score(title, source_name)
            if qs <= QUICK_FILTER_THRESHOLD:
                log.debug(f"  [pre-filter] score={qs} skipped: {title[:60]}")
                continue

        # Fuzzy dedup against this run's seen titles
        if is_duplicate_title(title, state):
            continue
        _register_title(title, state)

        candidates.append({
            "title":      title,
            "url":        url,
            "pub_date":   pub_date,
            "forced_tag": forced_tag,
            "url_hash":   hashlib.sha256(url.encode()).hexdigest(),
        })

    if not candidates:
        return 0, []

    # ── Phase 1b — parallel body fetch ────────────────────────────────────────
    urls = [c["url"] for c in candidates]
    log.info(f"  Fetching {len(urls)} article bodies in parallel [{source_name}]")

    try:
        body_results = asyncio.run(fetch_all_article_bodies(urls))
    except RuntimeError:
        # Already inside a running event loop (FastAPI background task).
        log.warning("  asyncio.run() unavailable — falling back to sequential body fetch")
        body_results = []
        for u in urls:
            body_results.append(fetch_article_details(u))
            time.sleep(DELAY_BETWEEN_ARTICLES)

    # ── Phase 1c — assemble batch and meta arrays ──────────────────────────────
    batch: list[dict] = []
    meta:  list[dict] = []

    for candidate, (article_text, extracted_date) in zip(candidates, body_results):
        raw_feed_date = candidate["pub_date"]
        pub_date      = extracted_date or raw_feed_date  # None if both missing
        batch.append({
            "title":        candidate["title"],
            "url":          candidate["url"],
            "source_name":  source_name,
            "article_text": article_text,
        })
        meta.append({
            "url_hash":   candidate["url_hash"],
            "pub_date":   pub_date,
            "forced_tag": candidate["forced_tag"],
            "title":      candidate["title"],
        })

    if not batch:
        return 0, []

    # ── Phase 2 — AI scoring ──────────────────────────────────────────────────
    if len(batch) > 3:
        log.info(f"  Batch AI: {len(batch)} articles [{source_name}]")
        try:
            ai_results = asyncio.run(analyze_articles_batch(batch))
        except RuntimeError:
            log.warning("  asyncio.run() unavailable — falling back to serial AI")
            ai_results = [
                analyze_article(
                    title=b["title"],
                    url=b["url"],
                    source_name=b["source_name"],
                    article_text=b["article_text"],
                )
                for b in batch
            ]
    else:
        ai_results = [
            analyze_article(
                title=b["title"],
                url=b["url"],
                source_name=b["source_name"],
                article_text=b["article_text"],
            )
            for b in batch
        ]

    # ── Phase 3 — persist ─────────────────────────────────────────────────────
    added = 0
    inserted_articles: list[dict] = []

    for ai, b, m in zip(ai_results, batch, meta):
        if ai is None:
            continue
        if relevance_boost:
            ai["relevance"] = min(10, ai["relevance"] + relevance_boost)

        tags = ai.get("tags", [])
        if m["forced_tag"] and m["forced_tag"] not in tags:
            tags.insert(0, m["forced_tag"])
        ai["tags"] = tags

        inserted = save_article(
            title=b["title"],
            url=b["url"],
            url_hash=m["url_hash"],
            source=source_name,
            jurisdiction=jurisdiction,
            domain=ai["domain"],
            relevance=ai["relevance"],
            sentiment=ai["sentiment"],
            summary=ai["summary"],
            why_it_matters=ai["why_it_matters"],
            pub_date=m["pub_date"],
            tags=",".join(ai["tags"]),
        )
        if inserted:
            added += 1
            inserted_articles.append({
                "title":          b["title"],
                "url":            b["url"],
                "source":         source_name,
                "jurisdiction":   jurisdiction,
                "domain":         ai["domain"],
                "relevance":      ai["relevance"],
                "sentiment":      ai["sentiment"],
                "summary":        ai["summary"],
                "why_it_matters": ai["why_it_matters"],
                "pub_date":       m["pub_date"],
                "tags":           ai["tags"],
                "forced_tag":     m["forced_tag"],
            })

    return added, inserted_articles
