"""
PolicyPulse Scraper v4
- Sources driven by DB table (scrape_type column decides RSS vs HTML)
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
from bs4 import BeautifulSoup
from rapidfuzz import fuzz

from database import (
    save_article, get_sources, log_scrape,
    update_source_scraped, get_watchlist_keywords,
)
from ai_processor import analyze_article, analyze_articles_batch

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── CONSTANTS ─────────────────────────────────────────────────────────────────

REQUEST_TIMEOUT       = 15
ARTICLE_FETCH_TIMEOUT = 12
DELAY_BETWEEN_SOURCES = 1.5
DELAY_BETWEEN_ARTICLES = 0.4
FUZZY_DEDUP_THRESHOLD  = 88   # token_set_ratio >= this → duplicate

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


def _parse_date(raw: str) -> str | None:
    if not raw:
        return None
    raw = raw.strip()
    formats = [
        "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d", "%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S GMT",
        "%B %d, %Y", "%b %d, %Y", "%d %B %Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(raw[:26], fmt).date().isoformat()
        except ValueError:
            continue
    if len(raw) >= 10 and raw[4] == "-" and raw[7] == "-":
        return raw[:10]
    return None


# ── RSS SCRAPING — no retry needed, feeds are stable ─────────────────────────

def scrape_rss(url, source_name, extra_tag=None):
    articles = []
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "xml")
        items = soup.find_all("item") or soup.find_all("entry")
        for item in items[:20]:
            title_el = item.find("title")
            link_el  = item.find("link")
            pub_el   = (item.find("pubDate") or item.find("published") or item.find("updated"))
            if not title_el:
                continue
            title    = title_el.get_text(strip=True)
            link     = (link_el.get_text(strip=True) or link_el.get("href", "")) if link_el else ""
            pub_date = datetime.utcnow().date().isoformat()
            if pub_el:
                parsed = _parse_date(pub_el.get_text(strip=True))
                if parsed:
                    pub_date = parsed
            if title and link and len(title) > 10:
                art = {"title": title, "url": link, "pub_date": pub_date}
                if extra_tag:
                    art["forced_tag"] = extra_tag
                articles.append(art)
    except Exception as e:
        log.warning(f"RSS error [{source_name}]: {e}")
    return articles


# ── HTML SCRAPING — retry + UA rotation on initial page fetch ─────────────────

def scrape_generic(url, source_name, base_url=None):
    articles = []
    soup = None

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
        return articles

    selectors = [
        "article h2 a", "article h3 a", ".news-item a", ".news-title a",
        ".views-row a", ".field-content a", "h2.title a", "h3.title a",
        ".entry-title a", ".post-title a", "h2 a", "h3 a",
    ]
    seen = set()
    for sel in selectors:
        for el in soup.select(sel)[:20]:
            title = el.get_text(strip=True)
            href  = el.get("href", "")
            if not title or len(title) < 15 or href in seen:
                continue
            if any(w in title.lower() for w in
                   ["home", "about", "contact", "menu", "search", "login", "sign in"]):
                continue
            if href and not href.startswith("http"):
                href = urljoin(base_url or url, href)
            if href and title:
                seen.add(href)
                articles.append({"title": title, "url": href,
                                 "pub_date": datetime.utcnow().date().isoformat()})
        if len(articles) >= 12:
            break
    return articles[:12]


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
    total_added = 0
    all_errors  = []

    sources = get_sources()
    for source in [s for s in sources if s["active"]]:
        name        = source["name"]
        url         = source["url"]
        scrape_type = source.get("scrape_type", "html")
        log.info(f"Scraping [{scrape_type.upper()}]: {name}")
        try:
            if scrape_type == "rss":
                raw = scrape_rss(url, name)
            else:
                raw = scrape_generic(url, name, base_url=_base_url(url))
            added = _process_and_save(raw, source)
            total_added += added
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
            added = _process_and_save(gn_raw, gn_source, relevance_boost=1)
            total_added += added
            log.info(f"  -> {added} new articles from Google News keywords")
        else:
            log.info("No watchlist keywords — skipping Google News scrape")
    except Exception as e:
        all_errors.append(f"Google News: {e}")
        log.error(f"Google News error: {e}")

    log_scrape(total_added, "; ".join(all_errors))
    log.info(f"=== Done. {total_added} new, {len(all_errors)} errors ===")
    return {"added": total_added, "errors": all_errors}


def _base_url(url: str) -> str:
    from urllib.parse import urlparse
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


# ── PROCESS AND SAVE ──────────────────────────────────────────────────────────

def _process_and_save(raw_articles, source, relevance_boost=0):
    """
    Phase 1 — validate, fuzzy-dedup, fetch article bodies
    Phase 2 — AI analysis: batch async if > 3 articles, else serial sync
    Phase 3 — save to DB
    """
    source_name  = source["name"]
    jurisdiction = source.get("jurisdiction", "Unknown")

    # ── Phase 1 ───────────────────────────────────────────────────────────────
    batch = []
    meta  = []

    for raw in raw_articles:
        title      = (raw.get("title") or "").strip()
        url        = (raw.get("url")   or "").strip()
        pub_date   = raw.get("pub_date", datetime.utcnow().date().isoformat())
        forced_tag = raw.get("forced_tag")

        if not title or not url or len(title) < 10:
            continue

        # Fuzzy dedup — skip before any network calls
        if is_duplicate_title(title, _seen_titles):
            continue
        _seen_titles.add(title)

        url_hash = hashlib.sha256(url.encode()).hexdigest()

        log.debug(f"  Fetching body: {url[:80]}")
        article_text, extracted_date = fetch_article_details(url)
        if extracted_date:
            pub_date = extracted_date
        time.sleep(DELAY_BETWEEN_ARTICLES)

        batch.append({
            "title":        title,
            "url":          url,
            "source_name":  source_name,
            "article_text": article_text,
        })
        meta.append({
            "url_hash":   url_hash,
            "pub_date":   pub_date,
            "forced_tag": forced_tag,
            "title":      title,
        })

    if not batch:
        return 0

    # ── Phase 2 — AI ──────────────────────────────────────────────────────────
    if len(batch) > 3:
        log.info(f"  Batch AI: {len(batch)} articles")
        try:
            ai_results = asyncio.run(analyze_articles_batch(batch))
        except RuntimeError:
            # Fallback if already inside a running event loop
            log.warning("  asyncio.run() unavailable — falling back to serial AI")
            ai_results = [
                analyze_article(
                    title=b["title"], url=b["url"],
                    source_name=b["source_name"], article_text=b["article_text"],
                )
                for b in batch
            ]
    else:
        ai_results = [
            analyze_article(
                title=b["title"], url=b["url"],
                source_name=b["source_name"], article_text=b["article_text"],
            )
            for b in batch
        ]

    # ── Phase 3 — save ────────────────────────────────────────────────────────
    added = 0
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

    return added
