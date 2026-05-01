"""
PolicyPulse Scraper v5
──────────────────────
Changes from v4:
  - run_scrape() now accepts an optional filter_config dict.
  - filter_config is deep-merged with _DEFAULT_FILTER_CONFIG so every key
    always has a safe value — old callers that pass nothing still work.
  - _process_and_save() enforces all filter rules:
      * max_per_source    — cap on raw articles fetched per source
      * dedup_threshold   — overrides the module constant per-run
      * jurisdictions     — allowlist (empty list = accept all)
      * domain_whitelist  — allowlist (empty list = accept all)
      * must_include      — keyword allowlist on title+summary (empty = skip)
      * min_relevance     — minimum AI score to keep (default 6)
      * dry_run           — score + log but don't write to DB
  - scrape_google_news_keywords() accepts a delay_ms param so the
    inter-keyword delay is configurable from the UI.
  - Scheduler still calls run_scrape() with no arguments; it will pick up
    the stored config via database.get_scraper_config() once main.py
    passes that through (handled in main.py / scheduler.py updates).
"""

import asyncio
import hashlib
import logging
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
    get_exclusion_keywords,
)
from ai_processor import analyze_article, analyze_articles_batch

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── CONSTANTS ─────────────────────────────────────────────────────────────────

REQUEST_TIMEOUT        = 15
ARTICLE_FETCH_TIMEOUT  = 12
DELAY_BETWEEN_SOURCES  = 1.5
DELAY_BETWEEN_ARTICLES = 0.4
FUZZY_DEDUP_THRESHOLD  = 88   # token_set_ratio >= this → duplicate

# ── DEFAULT FILTER CONFIG ─────────────────────────────────────────────────────
# Mirrors the frontend defaults exactly.  Every key must have a value here
# so _merge_config() can always produce a complete, safe config dict.

_DEFAULT_FILTER_CONFIG: dict = {
    "min_relevance":    6,
    "max_per_source":   15,
    "dedup_threshold":  88,
    "jurisdictions":    [],   # empty = accept all
    "domain_whitelist": [],   # empty = accept all
    "must_include":     [],   # empty = skip check
    "days_back":        90,   # used by scholarly scraper
    "gn_delay_ms":      800,
    "dry_run":          False,
    "scholarly_databases": {
        "openalex":    True,
        "semantic":    True,
        "pubmed":      True,
        "doaj":        True,
        "arxiv":       True,
        "thinktanks":  True,
    },
}


def _merge_config(user_config: dict | None) -> dict:
    """Deep-merge user_config onto _DEFAULT_FILTER_CONFIG.

    The scholarly_databases sub-dict is merged key-by-key so the caller
    only needs to supply the keys they want to change.  Unknown top-level
    keys from the frontend are passed through unchanged for forward
    compatibility.
    """
    cfg = dict(_DEFAULT_FILTER_CONFIG)
    cfg["scholarly_databases"] = dict(_DEFAULT_FILTER_CONFIG["scholarly_databases"])

    if not user_config:
        return cfg

    for k, v in user_config.items():
        if k == "scholarly_databases" and isinstance(v, dict):
            cfg["scholarly_databases"].update(v)
        else:
            cfg[k] = v

    # Clamp numeric fields to sane ranges
    cfg["min_relevance"]    = max(1,   min(10,  int(cfg["min_relevance"])))
    cfg["max_per_source"]   = max(1,   min(200, int(cfg["max_per_source"])))
    cfg["dedup_threshold"]  = max(50,  min(100, int(cfg["dedup_threshold"])))
    cfg["days_back"]        = max(7,   min(730, int(cfg["days_back"])))
    cfg["gn_delay_ms"]      = max(200, min(10000, int(cfg["gn_delay_ms"])))
    cfg["dry_run"]          = bool(cfg["dry_run"])

    return cfg


# ── USER-AGENT ROTATION ───────────────────────────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) "
    "Gecko/20100101 Firefox/121.0",
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

def is_duplicate_title(new_title: str, seen: set, threshold: int = FUZZY_DEDUP_THRESHOLD) -> bool:
    """Return True if new_title is semantically similar to any title in seen.

    Now accepts a threshold parameter so the per-run dedup sensitivity can
    be overridden via filter_config["dedup_threshold"] without touching the
    module constant.
    """
    if not seen:
        return False
    new_lower = new_title.lower()
    for existing in seen:
        score = fuzz.token_set_ratio(new_lower, existing.lower())
        if score >= threshold:
            log.debug(f"  [dedup] '{new_title[:55]}' score={score} vs '{existing[:55]}'")
            return True
    return False


# ── ARTICLE BODY + PUBLISH DATE EXTRACTION ───────────────────────────────────

def fetch_article_details(url: str) -> tuple[str, str | None]:
    """Fetch article page with retry + User-Agent rotation.

    Returns (article_text, pub_date). Both may be empty/None on all failures.
    Retries 3 times with backoff: 1 s, 2 s, then give up.
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
    """Async version of fetch_article_details() using a shared httpx session."""
    for attempt in range(3):
        headers = {**HEADERS, "User-Agent": USER_AGENTS[attempt % len(USER_AGENTS)]}
        try:
            resp = await session.get(url, headers=headers)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

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


async def fetch_all_article_bodies(urls: list[str]) -> list[tuple[str, str | None]]:
    """Fetch all article body pages concurrently using one shared httpx session."""
    timeout = httpx.Timeout(ARTICLE_FETCH_TIMEOUT, connect=5.0)
    async with httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=True,
        limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
    ) as session:
        tasks = [fetch_article_details_async(url, session) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    cleaned = []
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            log.warning(f"fetch_all_article_bodies unhandled exception [{urls[i][:60]}]: {r}")
            cleaned.append(("", None))
        else:
            cleaned.append(r)
    return cleaned


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


# ── RSS SCRAPING ──────────────────────────────────────────────────────────────

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


# ── HTML SCRAPING ─────────────────────────────────────────────────────────────

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


def scrape_google_news_keywords(keywords: list[str], delay_ms: int = 800) -> list[dict]:
    """Scrape Google News RSS for each watchlist keyword.

    Args:
        keywords: List of search terms.
        delay_ms: Milliseconds to wait between keyword fetches.  Configurable
                  via filter_config["gn_delay_ms"] so the user can back off
                  if they're hitting rate limits.
    """
    all_articles = []
    seen_urls    = set()
    delay_s      = max(0.2, delay_ms / 1000)

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
        time.sleep(delay_s)

    log.info(f"  Google News total: {len(all_articles)} articles from {len(keywords)} keywords")
    return all_articles


# ── MAIN SCRAPE ───────────────────────────────────────────────────────────────

def run_scrape(filter_config: dict | None = None) -> dict:
    """Main scrape entry point.

    Args:
        filter_config: Optional dict sent by the frontend UI (or loaded from
                       the DB by the scheduler).  Merged with defaults so
                       missing keys are always safe.  Pass None (or omit) to
                       use all defaults — backward-compatible with v4 callers.

    Returns:
        {"added": int, "errors": list[str]}
    """
    global _seen_titles
    _seen_titles = set()   # reset dedup state for this run

    cfg = _merge_config(filter_config)

    log.info(
        f"=== PolicyPulse scrape started "
        f"[min_rel={cfg['min_relevance']}, max_per_src={cfg['max_per_source']}, "
        f"dedup={cfg['dedup_threshold']}%, dry_run={cfg['dry_run']}] ==="
    )

    exclusions = get_exclusion_keywords()
    log.info(f"Exclusion keywords active: {len(exclusions)}")

    total_added, all_errors = 0, []

    sources = get_sources()
    for source in [s for s in sources if s["active"]]:
        name        = source["name"]
        url         = source["url"]
        scrape_type = source.get("scrape_type", "html")

        # ── Source-level jurisdiction pre-filter ─────────────────────────────
        # Skip entire source before making any network calls if its
        # jurisdiction isn't in the allowlist.  Empty list = accept all.
        if cfg["jurisdictions"]:
            src_juris = source.get("jurisdiction", "")
            if src_juris not in cfg["jurisdictions"]:
                log.info(
                    f"  [config] Skipping '{name}' — "
                    f"jurisdiction '{src_juris}' not in allowlist"
                )
                continue

        log.info(f"Scraping [{scrape_type.upper()}]: {name}")
        try:
            if scrape_type == "rss":
                raw = scrape_rss(url, name)
            else:
                raw = scrape_generic(url, name, base_url=_base_url(url))
            added = _process_and_save(raw, source, exclusions=exclusions, cfg=cfg)
            total_added += added
            update_source_scraped(name, added)
            log.info(f"  -> {added} new articles")
        except Exception as e:
            all_errors.append(f"{name}: {e}")
            log.error(f"Error [{name}]: {e}")
        time.sleep(DELAY_BETWEEN_SOURCES)

    # ── Google News keyword feed ──────────────────────────────────────────────
    try:
        keywords = get_watchlist_keywords()
        if keywords:
            log.info(f"Google News keywords: {keywords}")
            gn_raw    = scrape_google_news_keywords(keywords, delay_ms=cfg["gn_delay_ms"])
            gn_source = {"name": "Google News (Keyword Feed)", "jurisdiction": "Pan-Canadian"}
            # Google News results get +1 relevance boost because they matched
            # an explicit watchlist keyword the user cares about.
            added = _process_and_save(
                gn_raw, gn_source, relevance_boost=1,
                exclusions=exclusions, cfg=cfg,
            )
            total_added += added
            log.info(f"  -> {added} new articles from Google News keywords")
        else:
            log.info("No watchlist keywords — skipping Google News scrape")
    except Exception as e:
        all_errors.append(f"Google News: {e}")
        log.error(f"Google News error: {e}")

    log_scrape(total_added, "; ".join(all_errors))
    dry_note = " (DRY RUN — nothing saved)" if cfg["dry_run"] else ""
    log.info(f"=== Done. {total_added} new{dry_note}, {len(all_errors)} errors ===")
    return {"added": total_added, "errors": all_errors, "dry_run": cfg["dry_run"]}


def _base_url(url: str) -> str:
    from urllib.parse import urlparse
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


# ── PROCESS AND SAVE ──────────────────────────────────────────────────────────

def _process_and_save(
    raw_articles,
    source,
    relevance_boost: int = 0,
    exclusions: list | None = None,
    cfg: dict | None = None,
) -> int:
    """Apply all filter rules, fetch bodies in parallel, run AI, and save.

    Phase 1 — validate, exclusion check, jurisdiction/domain pre-filter,
               fuzzy-dedup, cap at max_per_source, parallel body fetch
    Phase 2 — AI analysis (batch async if > 3, else serial sync)
    Phase 3 — apply min_relevance / domain whitelist / must_include /
               dry_run, then save to DB

    Args:
        raw_articles:   List of raw article dicts from a scraper function.
        source:         Source dict from the DB (has "name", "jurisdiction").
        relevance_boost:Extra points to add to AI relevance score (e.g. +1
                        for Google News keyword hits).
        exclusions:     Loaded once per run in run_scrape(); passed in to
                        avoid a DB round-trip per source.
        cfg:            Merged filter config from _merge_config().

    Returns:
        Number of articles actually added (or would-have-added in dry_run).
    """
    if cfg is None:
        cfg = _merge_config(None)

    source_name   = source["name"]
    jurisdiction  = source.get("jurisdiction", "Unknown")

    # Config values used in this function
    min_relevance    = cfg["min_relevance"]
    max_per_source   = cfg["max_per_source"]
    dedup_threshold  = cfg["dedup_threshold"]
    domain_whitelist = [d.lower() for d in cfg["domain_whitelist"]]
    must_include_kws = [k.lower() for k in cfg["must_include"]]
    dry_run          = cfg["dry_run"]

    # ── Phase 1a: validate, exclusion, dedup, cap ─────────────────────────────
    candidates: list[dict] = []

    for raw in raw_articles:
        # Cap early to avoid fetching bodies we'll discard anyway
        if len(candidates) >= max_per_source:
            break

        title      = (raw.get("title") or "").strip()
        url        = (raw.get("url")   or "").strip()
        pub_date   = raw.get("pub_date", datetime.utcnow().date().isoformat())
        forced_tag = raw.get("forced_tag")

        if not title or not url or len(title) < 10:
            continue

        # Exclusion keyword check (title only — fast, no AI needed)
        if exclusions:
            title_lower = title.lower()
            if any(kw in title_lower for kw in exclusions):
                log.debug(f"  [excluded] {title[:60]}")
                continue

        # Fuzzy dedup using the per-run threshold from config
        if is_duplicate_title(title, _seen_titles, threshold=dedup_threshold):
            continue
        _seen_titles.add(title)

        candidates.append({
            "title":      title,
            "url":        url,
            "pub_date":   pub_date,
            "forced_tag": forced_tag,
            "url_hash":   hashlib.sha256(url.encode()).hexdigest(),
        })

    if not candidates:
        return 0

    # ── Phase 1b: parallel body fetch ────────────────────────────────────────
    urls = [c["url"] for c in candidates]
    log.info(f"  Fetching {len(urls)} article bodies in parallel")

    try:
        body_results = asyncio.run(fetch_all_article_bodies(urls))
    except RuntimeError:
        log.warning("  asyncio.run() unavailable — falling back to sequential body fetch")
        body_results = []
        for u in urls:
            body_results.append(fetch_article_details(u))
            time.sleep(DELAY_BETWEEN_ARTICLES)

    # ── Phase 1c: assemble batch ──────────────────────────────────────────────
    batch: list[dict] = []
    meta:  list[dict] = []

    for candidate, (article_text, extracted_date) in zip(candidates, body_results):
        pub_date = extracted_date if extracted_date else candidate["pub_date"]
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
        return 0

    # ── Phase 2: AI ───────────────────────────────────────────────────────────
    if len(batch) > 3:
        log.info(f"  Batch AI: {len(batch)} articles")
        try:
            ai_results = asyncio.run(analyze_articles_batch(batch))
        except RuntimeError:
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

    # ── Phase 3: filter + save ────────────────────────────────────────────────
    added = 0

    for ai, b, m in zip(ai_results, batch, meta):
        # AI returned None → scored below its own internal threshold (6)
        if ai is None:
            continue

        # Apply caller-supplied relevance boost before any config checks
        if relevance_boost:
            ai["relevance"] = min(10, ai["relevance"] + relevance_boost)

        # ── min_relevance filter ─────────────────────────────────────────────
        # The AI processor already drops articles below 6, but the user may
        # have set a higher threshold (e.g. 8 = only high-priority items).
        if ai["relevance"] < min_relevance:
            log.debug(
                f"  [config] Dropping '{b['title'][:55]}' — "
                f"relevance {ai['relevance']} < {min_relevance}"
            )
            continue

        # ── Domain whitelist ─────────────────────────────────────────────────
        # AI may assign a comma-separated domain string; check each segment.
        if domain_whitelist:
            article_domains = [
                d.strip().lower()
                for d in (ai.get("domain") or "").split(",")
            ]
            if not any(wd in article_domains for wd in domain_whitelist):
                log.debug(
                    f"  [config] Dropping '{b['title'][:55]}' — "
                    f"domain '{ai.get('domain')}' not in whitelist"
                )
                continue

        # ── Must-include keyword check ───────────────────────────────────────
        # Checked against title + AI summary (not raw body) to keep it fast.
        if must_include_kws:
            hay = (b["title"] + " " + (ai.get("summary") or "")).lower()
            if not any(kw in hay for kw in must_include_kws):
                log.debug(
                    f"  [config] Dropping '{b['title'][:55]}' — "
                    f"no must-include keyword found"
                )
                continue

        # ── Assemble tags ────────────────────────────────────────────────────
        tags = ai.get("tags", [])
        if m["forced_tag"] and m["forced_tag"] not in tags:
            tags.insert(0, m["forced_tag"])
        ai["tags"] = tags

        # ── Dry run ──────────────────────────────────────────────────────────
        if dry_run:
            log.info(
                f"  [DRY RUN] Would save: '{b['title'][:60]}' "
                f"(rel={ai['relevance']}, domain={ai.get('domain')}, "
                f"juris={ai.get('jurisdiction')})"
            )
            added += 1   # count as would-have-added so UI shows a real number
            continue

        # ── Persist ──────────────────────────────────────────────────────────
        inserted = save_article(
            title=b["title"],     url=b["url"],        url_hash=m["url_hash"],
            source=source_name,   jurisdiction=jurisdiction,
            domain=ai["domain"],  relevance=ai["relevance"],
            sentiment=ai["sentiment"], summary=ai["summary"],
            why_it_matters=ai["why_it_matters"],
            pub_date=m["pub_date"], tags=",".join(ai["tags"]),
        )
        if inserted:
            added += 1

    return added
