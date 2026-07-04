"""
PolicyPulse Scraper v5
- Sources driven by DB table (scrape_type column decides RSS vs HTML)
- Multi-page pagination support per source (max_pages + pagination_style
  columns), with early-exit once a page yields only already-known articles.
  See scrape_generic_paginated() for the full design rationale.
- Fuzzy title deduplication via rapidfuzz before any AI calls are made
- User-Agent rotation + exponential backoff retry on page fetches
- Batch AI processing: articles > 3 use asyncio.gather for concurrent calls
- Google News RSS for watchlist keywords
- Full article body fetch for real AI summaries and publish dates
"""

import asyncio
import hashlib
import logging
import os
import re
import time
from datetime import datetime
from urllib.parse import urljoin, quote_plus

import requests
import httpx
from bs4 import BeautifulSoup
from rapidfuzz import fuzz

from database import (
    save_article, get_sources, log_scrape,
    update_source_scraped, get_watchlist_keywords, 
    get_exclusion_keywords, get_known_url_hashes,
)
from ai_processor import analyze_article, analyze_articles_batch, quick_relevance_score

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── CONSTANTS ─────────────────────────────────────────────────────────────────

REQUEST_TIMEOUT       = 15
ARTICLE_FETCH_TIMEOUT = 12
DELAY_BETWEEN_SOURCES = 1.5
DELAY_BETWEEN_ARTICLES = 0.4
FUZZY_DEDUP_THRESHOLD  = 88   # token_set_ratio >= this → duplicate
QUICK_FILTER_THRESHOLD = 0    # score must be > 0 to proceed; trusted sources get a
                              # boost that guarantees they always clear this bar
                              # (score 0 = a noise keyword hit with zero policy hits)

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

# ── MODULE-LEVEL DEDUP STATE ──────────────────────────────────────────────────
_seen_titles: set[str] = set()


# ── FUZZY DEDUPLICATION ───────────────────────────────────────────────────────

def is_duplicate_title(new_title: str, seen: set) -> bool:
    """
    Return True if new_title is semantically similar to any title in seen.
    Uses rapidfuzz token_set_ratio — handles word-order differences and
    syndication patterns like:
      "Federal Budget Cuts Research Funding"
      "Federal Budget: Research Funding Cuts Announced"
    Threshold 88 catches near-duplicates, misses genuinely different stories.
    """
    if not seen:
        return False
    new_lower = new_title.lower()
    for existing in seen:
        score = fuzz.token_set_ratio(new_lower, existing.lower())
        if score >= FUZZY_DEDUP_THRESHOLD:
            log.debug(f"  [dedup] '{new_title[:55]}' score={score} vs '{existing[:55]}'")
            return True
    return False


# ── ARTICLE BODY + PUBLISH DATE EXTRACTION ───────────────────────────────────

def fetch_article_details(url: str) -> tuple[str, str | None]:
    """
    Fetch article page with retry + User-Agent rotation.
    Returns (article_text, pub_date). Both may be empty/None on all failures.
    Retries 3 times with backoff: 1s, 2s then give up.
    """
    for attempt in range(3):
        headers = {**HEADERS, "User-Agent": USER_AGENTS[attempt % len(USER_AGENTS)]}
        try:
            resp = requests.get(url, headers=headers, timeout=ARTICLE_FETCH_TIMEOUT)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            # ── 1. Extract publish date ──────────────────────────────────────
            pub_date = None
            date_metas = [
                ("meta", {"property": "article:published_time"}),
                ("meta", {"property": "article:modified_time"}),
                ("meta", {"name": "pubdate"}),
                ("meta", {"name": "publishdate"}),
                ("meta", {"name": "date"}),
                ("meta", {"itemprop": "datePublished"}),
                ("meta", {"property": "og:updated_time"}),
            ]
            for tag, attrs in date_metas:
                el = soup.find(tag, attrs)
                if el and el.get("content"):
                    pub_date = _parse_date(el["content"])
                    if pub_date:
                        break
            if not pub_date:
                for time_el in soup.find_all("time", datetime=True)[:3]:
                    pub_date = _parse_date(time_el["datetime"])
                    if pub_date:
                        break

            # ── 2. Extract article body text ─────────────────────────────────
            for tag in soup(["script", "style", "nav", "footer", "header",
                             "aside", "figure", "form", "noscript", "iframe",
                             "advertisement", "banner"]):
                tag.decompose()

            article_text = ""
            for selector in ["article", "main", ".article-body", ".entry-content",
                             ".post-content", ".story-body", "#content", ".content",
                             '[role="main"]', ".field-items"]:
                container = soup.select_one(selector)
                if container:
                    article_text = container.get_text(separator=" ", strip=True)
                    if len(article_text) > 200:
                        break
            if len(article_text) < 200:
                article_text = soup.get_text(separator=" ", strip=True)

            article_text = re.sub(r"\s{2,}", " ", article_text).strip()
            return article_text[:5000], pub_date

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

    Uses the same extraction logic (pub_date meta tags, semantic selectors,
    body text) and the same UA rotation.  Called concurrently by
    fetch_all_article_bodies() — one coroutine per article URL.

    The original synchronous fetch_article_details() is kept intact and is
    still used as a fallback when asyncio.run() is unavailable, and by any
    other caller that needs a simple synchronous interface.

    Args:
        url:     Article URL to fetch.
        session: Shared httpx.AsyncClient — created once per batch in
                 fetch_all_article_bodies() so connections are reused.

    Returns:
        (article_text[:5000], pub_date_str) — same contract as the sync version.
    """
    for attempt in range(3):
        headers = {**HEADERS, "User-Agent": USER_AGENTS[attempt % len(USER_AGENTS)]}
        try:
            resp = await session.get(url, headers=headers)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            # ── 1. Extract publish date (same logic as sync version) ──────────
            pub_date = None
            date_metas = [
                ("meta", {"property": "article:published_time"}),
                ("meta", {"property": "article:modified_time"}),
                ("meta", {"name": "pubdate"}),
                ("meta", {"name": "publishdate"}),
                ("meta", {"name": "date"}),
                ("meta", {"itemprop": "datePublished"}),
                ("meta", {"property": "og:updated_time"}),
            ]
            for tag, attrs in date_metas:
                el = soup.find(tag, attrs)
                if el and el.get("content"):
                    pub_date = _parse_date(el["content"])
                    if pub_date:
                        break
            if not pub_date:
                for time_el in soup.find_all("time", datetime=True)[:3]:
                    pub_date = _parse_date(time_el["datetime"])
                    if pub_date:
                        break

            # ── 2. Extract article body text (same logic as sync version) ─────
            for tag in soup(["script", "style", "nav", "footer", "header",
                             "aside", "figure", "form", "noscript", "iframe",
                             "advertisement", "banner"]):
                tag.decompose()

            article_text = ""
            for selector in ["article", "main", ".article-body", ".entry-content",
                             ".post-content", ".story-body", "#content", ".content",
                             '[role="main"]', ".field-items"]:
                container = soup.select_one(selector)
                if container:
                    article_text = container.get_text(separator=" ", strip=True)
                    if len(article_text) > 200:
                        break
            if len(article_text) < 200:
                article_text = soup.get_text(separator=" ", strip=True)

            article_text = re.sub(r"\s{2,}", " ", article_text).strip()
            return article_text[:5000], pub_date

        except httpx.HTTPStatusError as e:
            log.warning(f"fetch_async HTTP {e.response.status_code} attempt {attempt+1}/3 [{url[:60]}]")
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
    """Fetch all article body pages concurrently using one shared httpx session.

    Creates a single AsyncClient (so TCP connections are reused across the
    batch) and gathers all fetch_article_details_async() coroutines at once.
    asyncio.gather preserves order — results[i] corresponds to urls[i].

    Timeout is set to ARTICLE_FETCH_TIMEOUT per request.  Slow or unreachable
    URLs return ("", None) rather than failing the whole batch.

    Args:
        urls: List of article URLs to fetch in parallel.

    Returns:
        List of (article_text, pub_date) tuples, same length and order as urls.
    """
    timeout = httpx.Timeout(ARTICLE_FETCH_TIMEOUT, connect=5.0)
    async with httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=True,
        limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
    ) as session:
        tasks = [fetch_article_details_async(url, session) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Replace any unhandled exceptions with the safe empty fallback
    cleaned = []
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            log.warning(f"fetch_all_article_bodies unhandled exception [{urls[i][:60]}]: {r}")
            cleaned.append(("", None))
        else:
            cleaned.append(r)
    return cleaned


# ── PUBLISH DATE PARSER ───────────────────────────────────────────────────────

def _parse_date(raw: str) -> str | None:
    """
    Parse a raw date string extracted from an article page or feed into
    a clean YYYY-MM-DD ISO string.

    Handles the most common formats found across BC/Canadian government sites,
    RSS feeds, and news portals.  Returns None if nothing matches so callers
    can fall back to the RSS pub_date rather than silently using today.
    """
    if not raw:
        return None
    raw = raw.strip()
    # Each format is tried against the appropriate slice of the raw string.
    # RSS pubDate strings ("Wed, 15 May 2026 09:30:00 +0000") are 31+ chars,
    # so we must NOT truncate them to 26.  We pair each format with its own
    # slice length so strptime never sees trailing garbage it can't handle.
    format_slices = [
        ("%Y-%m-%dT%H:%M:%S%z",       25),  # 2026-05-15T09:30:00+00:00
        ("%Y-%m-%dT%H:%M:%SZ",        20),  # 2026-05-15T09:30:00Z
        ("%Y-%m-%dT%H:%M:%S",         19),  # 2026-05-15T09:30:00
        ("%Y-%m-%d",                  10),  # 2026-05-15
        ("%a, %d %b %Y %H:%M:%S %z",  None),  # Wed, 15 May 2026 09:30:00 +0000
        ("%a, %d %b %Y %H:%M:%S GMT", None),  # Wed, 15 May 2026 09:30:00 GMT
        ("%B %d, %Y",                 None),  # May 15, 2026
        ("%b %d, %Y",                 None),  # May 15, 2026 (abbrev)
        ("%d %B %Y",                  None),  # 15 May 2026
        ("%d %b %Y",                  None),  # 15 May 2026 (abbrev)
        ("%Y/%m/%d",                  10),  # 2026/05/15
        ("%m/%d/%Y",                  10),  # 05/15/2026
        ("%d-%m-%Y",                  10),  # 15-05-2026
    ]
    for fmt, slc in format_slices:
        try:
            candidate = raw if slc is None else raw[:slc]
            return datetime.strptime(candidate, fmt).date().isoformat()
        except ValueError:
            continue
    # Last-resort: if it already looks like YYYY-MM-DD just return it
    if len(raw) >= 10 and raw[4] == "-" and raw[7] == "-":
        candidate = raw[:10]
        # Sanity-check: year must be plausible
        try:
            year = int(candidate[:4])
            if 2000 <= year <= 2100:
                return candidate
        except ValueError:
            pass
    return None


# ── RSS SCRAPING — no retry needed, feeds are stable ─────────────────────────

def scrape_rss(url, source_name, extra_tag=None):
    """Scrape an RSS 2.0 or Atom 1.0 feed.

    Handles both formats:
    - RSS 2.0:  <item><title>, <link>url</link>, <pubDate>
    - Atom 1.0: <entry><title>, <link href="url"/>, <published>/<updated>

    The canada.ca Atom feeds (.atom URLs) use Atom 1.0 format where <link>
    is a self-closing tag with an href attribute — NOT text content.
    BeautifulSoup's xml parser returns the href correctly via .get("href"),
    but .get_text() returns "" on a self-closing tag, so we must try href first.
    """
    articles = []
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        soup  = BeautifulSoup(resp.content, "xml")
        # RSS 2.0 uses <item>, Atom 1.0 uses <entry>
        items = soup.find_all("item") or soup.find_all("entry")
        for item in items[:25]:
            title_el = item.find("title")
            if not title_el:
                continue
            title = title_el.get_text(strip=True)
            if not title or len(title) < 5:
                continue

            # Link extraction — handles three variants:
            # 1. RSS <link>https://...</link>  → get_text()
            # 2. Atom <link href="https://..."/> → .get("href")
            # 3. Atom <link rel="alternate" href="..."/> — find the alternate link
            link = ""
            link_el = item.find("link", rel="alternate") or item.find("link")
            if link_el:
                # Try href attribute first (Atom), then text content (RSS)
                link = link_el.get("href", "") or link_el.get_text(strip=True)
            if not link:
                # Some Atom feeds use <id> as the canonical URL
                id_el = item.find("id")
                if id_el:
                    candidate = id_el.get_text(strip=True)
                    if candidate.startswith("http"):
                        link = candidate

            if not link:
                continue

            # Date extraction — RSS uses pubDate, Atom uses published or updated
            pub_date = datetime.utcnow().date().isoformat()
            pub_el   = (item.find("pubDate") or item.find("published")
                        or item.find("updated") or item.find("dc:date"))
            if pub_el:
                parsed = _parse_date(pub_el.get_text(strip=True))
                if parsed:
                    pub_date = parsed

            art = {"title": title, "url": link, "pub_date": pub_date}
            if extra_tag:
                art["forced_tag"] = extra_tag
            articles.append(art)

    except Exception as e:
        log.warning(f"RSS error [{source_name}]: {e}")
    return articles


# ── HTML SCRAPING — retry + UA rotation on initial page fetch ─────────────────

def _extract_articles_from_soup(soup, url, source_name, base_url=None):
    """Pull candidate article links out of a single listing page's soup.

    Returns list of dicts with keys: title, url, pub_date, snippet.
    `snippet` is the description text found near the title on the listing
    page — used as fallback article_text for aggregator sites (like BCCSU)
    where the individual article page is a thin stub that links to an
    external publication, giving the AI nothing to analyze.
    """
    articles = []

    selectors = [
        # ── Semantic / article-wrapped (most reliable, try first) ────────
        "article h2 a", "article h3 a", "article h4 a",
        # ── Named CSS patterns common in news/gov CMS ────────────────────
        ".news-item a", ".news-item h3 a", ".news-item h4 a",
        ".news-title a", ".news-heading a", ".news-list li a",
        ".entry-title a", ".post-title a",
        # ── WordPress Gutenberg block editor (BCCSU, many NGO sites) ─────
        ".wp-block-post-title a",
        ".wp-block-post-title",
        ".wp-block-query .wp-block-post-title a",
        ".wp-block-query li a",
        ".wp-block-query h2 a", ".wp-block-query h3 a",
        "ul.wp-block-post-template li h2 a",
        "ul.wp-block-post-template li h3 a",
        ".wp-block-post-template .wp-block-post-title a",
        # Classic WordPress
        ".post-title a", ".entry-title a",
        # ── Drupal Views (BC Gov Newsroom, ministry pages) ────────────────
        ".views-row a", ".views-row h3 a", ".views-row h4 a",
        ".view-content h3 a", ".view-content h4 a",
        ".field-content a", ".field-items a",
        ".view-content .views-field-title a",
        # ── Canada.ca / GCWeb ─────────────────────────────────────────────
        ".feeds-cont li a", ".feeds-cont a",
        "main ul li > a",
        ".mwsbodytext ul li a",
        "h3.gc-thickline a",
        ".gc-card h3 a", ".gc-card h2 a",
        ".card-title a",
        # ── CIHI / health research ────────────────────────────────────────
        ".news-listing a", ".news-listing h3 a", ".news-listing h2 a",
        ".results-list h3 a", ".results-list h2 a",
        ".news-list-item a", ".news-list-item h3 a",
        ".result-item a", ".result-item h3 a",
        # ── BC Gov newsroom / ministry sub-pages ─────────────────────────
        ".article-list a", ".article-list h3 a",
        ".news-releases a", ".news-releases h3 a",
        # ── Government research councils ──────────────────────────────────
        "table td.views-field a", "table.table td a",
        # ── FNHA / health authority custom layouts ────────────────────────
        ".news-listing-item a", ".news-listing-item h3 a", ".news-listing-item h4 a",
        ".news-tile a", ".news-tile h3 a",
        # ── Canadian Pharmacists / NGO news modules ───────────────────────
        ".article-listing a", ".article-listing h3 a",
        ".news-module a", ".news-module h3 a", ".news-module h4 a",
        # ── Mental Health Commission / card grids ─────────────────────────
        ".news-card a", ".news-card h3 a", ".news-card h4 a",
        ".card-grid a", ".card-grid h3 a",
        ".resource-listing a", ".resource-listing h3 a",
        # ── BC Legislature — bills listing ────────────────────────────────
        ".bill-listing a", ".legislation-list a",
        "table.bills td a",
        # ── Media release / press release patterns ────────────────────────
        ".media-release a", ".media-release h3 a",
        ".news-release a", ".news-release h3 a",
        ".press-release a", ".press-release h3 a",
        # ── Labour org patterns (WordPress archive pages) ─────────────────
        ".post-entry a", ".post-entry h2 a",
        ".archive-post a", ".archive-post h2 a",
        ".entry a", ".entry h2 a",
        # ── Generic list patterns ─────────────────────────────────────────
        "ul.post-list li a", "ul.article-list li a",
        ".post-list h2 a", ".post-list h3 a",
        ".archive-list h2 a", ".archive-list h3 a",
        # ── Broad heading fallbacks (last resort) ─────────────────────────
        "h2.title a", "h3.title a",
        "h2 a", "h3 a", "h4 a",
    ]

    _NAV_NOISE = {
        "home", "about", "contact", "menu", "search", "login", "sign in",
        "subscribe", "follow us", "share", "back to", "read more", "more news",
        "all news", "view all", "load more", "next page", "previous page",
        "français", "english", "skip to", "return to", "print", "email this",
        "donate", "donate now", "register", "sign up", "terms of use",
        "privacy policy", "accessibility", "sitemap", "careers",
    }

    _BLOCKED_DOMAINS = {
        "twitter.com", "x.com", "facebook.com", "instagram.com",
        "linkedin.com", "youtube.com", "tiktok.com", "pinterest.com",
        "t.co", "bit.ly", "ow.ly", "buff.ly",
        "addthis.com", "sharethis.com", "feedburner.com",
        "googletagmanager.com", "google-analytics.com",
    }

    from urllib.parse import urlparse as _urlparse

    seen = set()
    for sel in selectors:
        for el in soup.select(sel)[:30]:
            title = el.get_text(strip=True)
            href  = el.get("href", "")

            if not title or len(title) < 15 or len(title) > 300:
                continue
            if href in seen:
                continue

            title_lower = title.lower()
            if any(noise in title_lower for noise in _NAV_NOISE):
                continue

            if href and not href.startswith("http"):
                href = urljoin(base_url or url, href)

            if not href or href.startswith(("#", "javascript:", "mailto:")):
                continue

            _href_domain = _urlparse(href).netloc.lower()
            if any(blocked in _href_domain for blocked in _BLOCKED_DOMAINS):
                continue

            # Extract a description snippet from the listing page context.
            # Walk up to the nearest container (article, li, div) and look
            # for a paragraph or description element nearby.
            snippet = ""
            container = el.parent
            for _ in range(4):  # walk up max 4 levels
                if container is None:
                    break
                tag = getattr(container, 'name', None)
                if tag in ('article', 'li', 'div', 'section'):
                    # Try common description selectors within this container
                    for desc_sel in ('p', '.excerpt', '.summary', '.description',
                                     '.entry-summary', '.post-excerpt',
                                     '.wp-block-post-excerpt__excerpt'):
                        desc_el = container.find(desc_sel)
                        if desc_el:
                            text = desc_el.get_text(strip=True)
                            if len(text) > 30:
                                snippet = text[:500]
                                break
                    if snippet:
                        break
                container = getattr(container, 'parent', None)

            seen.add(href)
            articles.append({
                "title":   title,
                "url":     href,
                "pub_date": None,
                "snippet": snippet,
            })
        if len(articles) >= 20:
            break

    # Deduplicate by URL
    seen_urls: set[str] = set()
    unique: list[dict] = []
    for art in articles:
        if art["url"] not in seen_urls:
            seen_urls.add(art["url"])
            unique.append(art)

    return unique[:20]


def _fetch_listing_page(url, source_name):
    """Fetch and parse a single listing page with retry + UA rotation.

    Returns a BeautifulSoup object, or None if all attempts failed.
    Shared by scrape_generic() and the pagination orchestrator so retry
    behaviour is identical for page 1 and page N.
    """
    for attempt in range(3):
        headers = {**HEADERS, "User-Agent": USER_AGENTS[attempt % len(USER_AGENTS)]}
        try:
            resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")
        except requests.RequestException as e:
            log.warning(f"_fetch_listing_page attempt {attempt+1}/3 [{source_name}] {url[:70]}: {e}")
            if attempt < 2:
                time.sleep(2 ** attempt)
    return None


def scrape_generic(url, source_name, base_url=None):
    """Single-page HTML scrape — unchanged behaviour for sources with
    max_pages=1 (the default for every source unless explicitly configured
    otherwise). Kept as a thin wrapper around the shared fetch + parse
    helpers so existing callers and tests are unaffected."""
    soup = _fetch_listing_page(url, source_name)
    if soup is None:
        return []
    result = _extract_articles_from_soup(soup, url, source_name, base_url=base_url)
    log.debug(f"  scrape_generic [{source_name}]: {len(result)} candidate links found")
    return result


# ── PAGINATION — URL BUILDERS ─────────────────────────────────────────────────
# Each builder takes the page-1 URL and a 1-indexed page number (page_num=1
# always returns the original URL unchanged) and returns the URL to fetch for
# that page. Three distinct styles cover the overwhelming majority of CMS
# platforms encountered among government, university, and NGO sites:
#
#   path   — WordPress (BCCSU and most NGOs): /news/page/2/
#   query  — Drupal / generic CMS: ?page=2  (Drupal page param is 0-indexed
#            internally but we expose 1-indexed page_num to keep this
#            consistent with the other two builders, converting internally)
#   offset — Canada.ca / GCWeb item-offset pagination: ?start=10
#
# 'auto' tries all three in turn during the first multi-page fetch and
# remembers whichever one actually changed the page content (see
# scrape_generic_paginated). This means a source can be left on 'auto'
# indefinitely without ever being told which CMS it runs.

def _build_page_url_path(base_url: str, page_num: int) -> str:
    """WordPress-style path pagination: example.com/news/ -> example.com/news/page/2/"""
    if page_num <= 1:
        return base_url
    trimmed = base_url.rstrip("/")
    return f"{trimmed}/page/{page_num}/"


def _build_page_url_query(base_url: str, page_num: int) -> str:
    """Drupal/generic query-param pagination: example.com/news?page=1 (0-indexed internally)."""
    if page_num <= 1:
        return base_url
    sep = "&" if "?" in base_url else "?"
    return f"{base_url}{sep}page={page_num - 1}"


def _build_page_url_offset(base_url: str, page_num: int, items_per_page: int = 10) -> str:
    """Canada.ca/GCWeb item-offset pagination: example.com/news?start=10"""
    if page_num <= 1:
        return base_url
    sep = "&" if "?" in base_url else "?"
    offset = (page_num - 1) * items_per_page
    return f"{base_url}{sep}start={offset}"


_PAGE_URL_BUILDERS = {
    "path":   _build_page_url_path,
    "query":  _build_page_url_query,
    "offset": _build_page_url_offset,
}


def scrape_generic_paginated(url, source_name, base_url=None,
                             max_pages: int = 1, pagination_style: str = "auto",
                             known_url_hashes: set | None = None):
    """Paginated HTML scrape with early-exit on already-known articles.

    This is the entry point used by run_scrape() for any source with
    max_pages > 1. Sources left at the default max_pages=1 go through
    scrape_generic() exactly as before — zero behaviour change for the
    ~25 single-page sources already configured.

    Early-exit logic (the key cost-control mechanism):
      After the FIRST page, every subsequent page is checked against
      known_url_hashes (the set of url_hash values already in the articles
      table). If a page contributes zero new URLs, pagination stops
      immediately — we assume everything past this point has already been
      seen on a prior scrape run. This means a daily scrape of a 266-page
      site like BCCSU typically only ever fetches page 1, because new posts
      since yesterday almost always fit on the first page. Pagination only
      goes deeper than page 1 on the very first run (or after a long gap),
      which is exactly when you want a deeper catch-up fetch.

    pagination_style='auto' tries path, then query, then offset on page 2,
    keeping whichever builder returns at least one link not already present
    on page 1. If none produce new links, pagination stops after page 1
    (same behaviour as max_pages=1).

    Args:
        url:               Page 1 URL (exactly as stored in the sources table).
        source_name:       For logging.
        base_url:          Used to resolve relative hrefs (same as scrape_generic).
        max_pages:         Upper bound on how many pages to attempt.
        pagination_style:  'path' | 'query' | 'offset' | 'auto' | 'none'.
        known_url_hashes:  Set of url_hash values already in the DB, used for
                            early-exit. If None, early-exit is skipped (every
                            page up to max_pages is always fetched) — callers
                            should always pass this in production use.

    Returns:
        List of article dicts (same shape as scrape_generic), deduplicated
        across all pages fetched.
    """
    if pagination_style == "none" or max_pages <= 1:
        return scrape_generic(url, source_name, base_url=base_url)

    known_url_hashes = known_url_hashes or set()
    all_articles: list[dict] = []
    seen_urls: set[str] = set()
    resolved_style = pagination_style  # 'auto' gets pinned to a concrete style below

    # ── Page 1 — identical to the non-paginated path ──────────────────────────
    soup = _fetch_listing_page(url, source_name)
    if soup is None:
        return []
    page1_articles = _extract_articles_from_soup(soup, url, source_name, base_url=base_url)
    for art in page1_articles:
        if art["url"] not in seen_urls:
            seen_urls.add(art["url"])
            all_articles.append(art)

    if max_pages <= 1 or not page1_articles:
        return all_articles

    # ── Pages 2..max_pages ──────────────────────────────────────────────────
    for page_num in range(2, max_pages + 1):

        if resolved_style == "auto":
            # Try each builder until one yields at least one URL not seen on
            # page 1. Once resolved, stick with that builder for the rest of
            # this run (and log it so a human can hardcode it later via the
            # Pages/Style UI control instead of re-discovering it every time).
            found_style = None
            for style_name, builder in _PAGE_URL_BUILDERS.items():
                candidate_url = builder(url, page_num)
                candidate_soup = _fetch_listing_page(candidate_url, f"{source_name} (auto-detect {style_name})")
                if candidate_soup is None:
                    continue
                candidate_articles = _extract_articles_from_soup(
                    candidate_soup, candidate_url, source_name, base_url=base_url
                )
                new_on_this_page = [a for a in candidate_articles if a["url"] not in seen_urls]
                if new_on_this_page:
                    found_style = style_name
                    page_articles = candidate_articles
                    break
                time.sleep(0.5)  # be polite between auto-detect probe attempts
            if found_style is None:
                log.info(f"  [paginate] {source_name}: no pagination style produced new links at page {page_num} — stopping")
                break
            resolved_style = found_style
            log.info(f"  [paginate] {source_name}: auto-detected pagination style = '{resolved_style}'")
        else:
            builder = _PAGE_URL_BUILDERS.get(resolved_style)
            if builder is None:
                log.warning(f"  [paginate] {source_name}: unknown pagination_style '{resolved_style}' — stopping")
                break
            page_url = builder(url, page_num)
            page_soup = _fetch_listing_page(page_url, source_name)
            if page_soup is None:
                log.info(f"  [paginate] {source_name}: page {page_num} fetch failed — stopping")
                break
            page_articles = _extract_articles_from_soup(page_soup, page_url, source_name, base_url=base_url)

        new_on_this_page = [a for a in page_articles if a["url"] not in seen_urls]

        if not new_on_this_page:
            log.info(f"  [paginate] {source_name}: page {page_num} had no new links — stopping (all already seen)")
            break

        # Early-exit: if every new link on this page is already in the DB,
        # we've caught up to content we processed on a previous scrape run.
        # No point fetching page_num+1 — it will be even older.
        unseen_in_db = [a for a in new_on_this_page
                        if hashlib.sha256(a["url"].encode()).hexdigest() not in known_url_hashes]
        for art in new_on_this_page:
            seen_urls.add(art["url"])
            all_articles.append(art)

        if known_url_hashes and not unseen_in_db:
            log.info(f"  [paginate] {source_name}: page {page_num} was entirely already-known articles — stopping early "
                      f"(caught up to previous scrape)")
            break

        time.sleep(DELAY_BETWEEN_SOURCES)

    log.info(f"  [paginate] {source_name}: {len(all_articles)} total candidate links across pages")
    return all_articles


# ── GOOGLE NEWS ───────────────────────────────────────────────────────────────

def build_google_news_url(keyword, region="CA", lang="en"):
    q = quote_plus(keyword + " Canada policy")
    return f"https://news.google.com/rss/search?q={q}&hl={lang}-{region}&gl={region}&ceid={region}:{lang}"


def scrape_google_news_keywords(keywords):
    all_articles = []
    seen_urls = set()
    for kw in keywords:
        if not kw or len(kw) < 2:
            continue
        url = build_google_news_url(kw)
        log.info(f"  Google News: '{kw}'")
        results = scrape_rss(url, f"Google News: {kw}", extra_tag=kw)
        for art in results:
            if art["url"] not in seen_urls:
                seen_urls.add(art["url"])
                all_articles.append(art)
        time.sleep(0.8)
    log.info(f"  Google News total: {len(all_articles)} articles from {len(keywords)} keywords")
    return all_articles


# ── MAIN SCRAPE ───────────────────────────────────────────────────────────────

def run_scrape():
    global _seen_titles
    _seen_titles = set()   # reset dedup state for this run

    log.info("=== PolicyPulse scrape started ===")
    # Load exclusion list once per run — lowercase already stored in DB
    exclusions = get_exclusion_keywords()
    log.info(f"Exclusion keywords active: {len(exclusions)}")
    total_added      = 0
    all_errors       = []
    all_new_articles: list[dict] = []   # collects every newly-inserted article

    # Loaded once per run (not per source) — pagination early-exit checks
    # candidate URLs against this set so a multi-page source like BCCSU stops
    # fetching deeper pages the moment it hits content already in the DB.
    known_url_hashes = get_known_url_hashes()
    log.info(f"Known article hashes loaded for pagination early-exit: {len(known_url_hashes)}")

    sources = get_sources()
    for source in [s for s in sources if s["active"]]:
        name             = source["name"]
        url              = source["url"]
        scrape_type      = source.get("scrape_type", "html")
        max_pages        = int(source.get("max_pages") or 1)
        pagination_style = source.get("pagination_style") or "auto"
        log.info(f"Scraping [{scrape_type.upper()}]: {name}"
                 + (f" (max_pages={max_pages}, style={pagination_style})" if max_pages > 1 else ""))
        try:
            if scrape_type == "rss":
                raw = scrape_rss(url, name)
            elif max_pages > 1 and pagination_style != "none":
                raw = scrape_generic_paginated(
                    url, name, base_url=_base_url(url),
                    max_pages=max_pages, pagination_style=pagination_style,
                    known_url_hashes=known_url_hashes,
                )
            else:
                raw = scrape_generic(url, name, base_url=_base_url(url))
            added, new_arts = _process_and_save(raw, source, exclusions=exclusions)
            total_added += added
            all_new_articles.extend(new_arts)
            # Newly-saved articles become "known" immediately so a later
            # source in this same run (or the Google News pass below) won't
            # re-trigger pagination on the same content if it shares a URL.
            for art in new_arts:
                known_url_hashes.add(hashlib.sha256(art["url"].encode()).hexdigest())
            update_source_scraped(name, added)
            log.info(f"  -> {added} new articles")
        except Exception as e:
            all_errors.append(f"{name}: {e}")
            log.error(f"Error [{name}]: {e}")
        time.sleep(DELAY_BETWEEN_SOURCES)

    try:
        keywords = get_watchlist_keywords()
        if keywords:
            log.info(f"Google News keywords: {keywords}")
            gn_raw = scrape_google_news_keywords(keywords)
            gn_source = {"name": "Google News (Keyword Feed)", "jurisdiction": "Pan-Canadian"}
            added, new_arts = _process_and_save(gn_raw, gn_source, relevance_boost=1, exclusions=exclusions)
            total_added += added
            all_new_articles.extend(new_arts)
            log.info(f"  -> {added} new articles from Google News keywords")
        else:
            log.info("No watchlist keywords — skipping Google News scrape")
    except Exception as e:
        all_errors.append(f"Google News: {e}")
        log.error(f"Google News error: {e}")

    log_scrape(total_added, "; ".join(all_errors), scrape_type="news")
    log.info(f"=== Done. {total_added} new, {len(all_errors)} errors ===")
    return {
        "added":        total_added,
        "errors":       all_errors,
        "new_articles": all_new_articles,   # full article dicts for alert dispatch
    }


def _base_url(url: str) -> str:
    from urllib.parse import urlparse
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


# ── PROCESS AND SAVE ──────────────────────────────────────────────────────────

def _process_and_save(raw_articles, source, relevance_boost=0, exclusions=None):
    """
    Phase 1 — validate, fuzzy-dedup, parallel-fetch all article bodies at once
    Phase 2 — AI analysis: batch async if > 3 articles, else serial sync
    Phase 3 — save to DB
    """
    source_name  = source["name"]
    jurisdiction = source.get("jurisdiction", "Unknown")

    # ── Phase 1a — validate and dedup ─────────────────────────────────────────
    # Collect all articles that pass validation and dedup into a candidates
    # list before touching the network.  This lets us fire all body fetches
    # in a single parallel batch instead of one sequential request per article.
    candidates = []

    for raw in raw_articles:
        title      = (raw.get("title") or "").strip()
        url        = (raw.get("url")   or "").strip()
        pub_date   = raw.get("pub_date")
        forced_tag = raw.get("forced_tag")
        snippet    = raw.get("snippet", "")  # description from listing page

        if not title or not url or len(title) < 10:
            continue
        # Exclusion keyword check — skip before any network call
        if exclusions:
            title_lower = title.lower()
            if any(kw in title_lower for kw in exclusions):
                log.debug(f"  [excluded] {title[:60]}")
                continue

        # Quick keyword pre-filter — pure Python, zero API cost.
        # Skips titles that contain noise keywords (sports, entertainment, etc.)
        # and have zero policy keyword hits.  Google News feeds from keyword
        # watchlist searches are whitelisted (forced_tag is set) so they are
        # never filtered out here.
        if not forced_tag:
            qs = quick_relevance_score(title, source_name)
            if qs <= QUICK_FILTER_THRESHOLD:
                log.debug(f"  [pre-filter] score={qs} skipped: {title[:60]}")
                continue

        # Fuzzy dedup — still runs before any network call
        if is_duplicate_title(title, _seen_titles):
            continue
        _seen_titles.add(title)

        candidates.append({
            "title":      title,
            "url":        url,
            "pub_date":   pub_date,
            "forced_tag": forced_tag,
            "snippet":    snippet,
            "url_hash":   hashlib.sha256(url.encode()).hexdigest(),
        })

    if not candidates:
        return 0, []

    # ── Phase 1b — parallel body fetch ────────────────────────────────────────
    # Uses ThreadPoolExecutor so this is safe to call from FastAPI's
    # BackgroundTasks (which runs in a thread inside uvicorn's event loop).
    # asyncio.run() inside a running event loop raises RuntimeError, so we
    # use threads instead — same net concurrency, no event-loop conflict.
    urls = [c["url"] for c in candidates]
    log.info(f"  Fetching {len(urls)} article bodies in parallel (threads)")

    from concurrent.futures import ThreadPoolExecutor, as_completed
    body_results = [("", None)] * len(urls)
    with ThreadPoolExecutor(max_workers=min(8, len(urls))) as executor:
        future_to_idx = {executor.submit(fetch_article_details, u): i for i, u in enumerate(urls)}
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                body_results[idx] = future.result()
            except Exception as exc:
                log.warning(f"  body fetch error [{urls[idx][:60]}]: {exc}")
                body_results[idx] = ("", None)

    # ── Phase 1c — assemble batch and meta arrays ──────────────────────────────
    batch = []
    meta  = []

    for candidate, (article_text, extracted_date) in zip(candidates, body_results):
        raw_feed_date = candidate["pub_date"]
        pub_date = extracted_date or raw_feed_date

        # Use listing-page snippet as fallback when body fetch returns nothing.
        # This is the key fix for aggregator sites (BCCSU, FNHA) where the
        # individual article page is a thin stub — the description on the
        # listing page is often 100-400 chars and gives the AI enough to work
        # with for domain classification, tagging, and relevance scoring.
        effective_text = article_text
        if not effective_text and candidate.get("snippet"):
            effective_text = f"[Listing summary] {candidate['snippet']}"

        batch.append({
            "title":        candidate["title"],
            "url":          candidate["url"],
            "source_name":  source_name,
            "article_text": effective_text,
        })
        meta.append({
            "url_hash":   candidate["url_hash"],
            "pub_date":   pub_date,
            "forced_tag": candidate["forced_tag"],
            "title":      candidate["title"],
        })

    if not batch:
        return 0, []

    # ── Phase 2 — AI ──────────────────────────────────────────────────────────
    # Always use serial synchronous calls. analyze_article() is already the
    # correct sync entry point and works safely from any thread context.
    # The async batch path used asyncio.run() which crashes inside FastAPI's
    # BackgroundTasks thread (RuntimeError: This event loop is already running).
    log.info(f"  AI analysis: {len(batch)} articles (serial sync)")
    ai_results = [
        analyze_article(
            title=b["title"], url=b["url"],
            source_name=b["source_name"], article_text=b["article_text"],
        )
        for b in batch
    ]

    # ── Phase 3 — save ────────────────────────────────────────────────────────
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
            title=b["title"],    url=b["url"],       url_hash=m["url_hash"],
            source=source_name,  jurisdiction=jurisdiction,
            domain=ai["domain"], relevance=ai["relevance"],
            sentiment=ai["sentiment"], summary=ai["summary"],
            why_it_matters=ai["why_it_matters"],
            pub_date=m["pub_date"], tags=",".join(ai["tags"]),
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
