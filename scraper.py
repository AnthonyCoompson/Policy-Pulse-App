"""
PolicyPulse Scraper v3
- Scrapes 20 fixed government/education sources
- ALSO scrapes Google News RSS for each watchlist keyword
- Fetches full article body for real AI summaries and why-it-matters
- Extracts actual publish date from article HTML meta tags
- Processes each article through Gemini AI (relevance >= 6 kept)
"""

import hashlib
import logging
import os
import time
from datetime import datetime
from urllib.parse import urljoin, quote_plus

import requests
from bs4 import BeautifulSoup

from database import (
    save_article, get_sources, log_scrape,
    update_source_scraped, get_watchlist_keywords
)
from ai_processor import analyze_article

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-CA,en;q=0.9",
}
REQUEST_TIMEOUT = 15
ARTICLE_FETCH_TIMEOUT = 12   # slightly shorter for individual article fetches
DELAY_BETWEEN_SOURCES = 1.5
DELAY_BETWEEN_ARTICLES = 0.4  # small delay between article body fetches


# ── ARTICLE BODY + PUBLISH DATE EXTRACTION ───────────────────────────────────

def fetch_article_details(url: str) -> tuple[str, str | None]:
    """
    Fetch the full article page and extract:
      - article_text: cleaned body text (up to ~5000 chars)
      - pub_date:     ISO date string if found in meta tags, else None

    Returns (article_text, pub_date). Both may be empty/None on failure.
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=ARTICLE_FETCH_TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # ── 1. Extract publish date ──────────────────────────────────────────
        pub_date = None

        # Priority order: structured meta tags → <time> elements → og:updated_time
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

        # Try <time datetime="..."> if meta didn't work
        if not pub_date:
            for time_el in soup.find_all("time", datetime=True)[:3]:
                pub_date = _parse_date(time_el["datetime"])
                if pub_date:
                    break

        # ── 2. Extract article body text ─────────────────────────────────────
        # Remove boilerplate elements
        for tag in soup(["script", "style", "nav", "footer", "header",
                         "aside", "figure", "form", "noscript", "iframe",
                         "advertisement", "banner"]):
            tag.decompose()

        # Try semantic article containers first
        article_text = ""
        for selector in ["article", "main", ".article-body", ".entry-content",
                         ".post-content", ".story-body", "#content", ".content",
                         '[role="main"]', ".field-items"]:
            container = soup.select_one(selector)
            if container:
                article_text = container.get_text(separator=" ", strip=True)
                if len(article_text) > 200:
                    break

        # Fall back to body text if containers didn't find enough
        if len(article_text) < 200:
            article_text = soup.get_text(separator=" ", strip=True)

        # Clean up whitespace
        import re
        article_text = re.sub(r"\s{2,}", " ", article_text).strip()

        return article_text[:5000], pub_date

    except Exception as e:
        log.debug(f"fetch_article_details failed for {url}: {e}")
        return "", None


def _parse_date(raw: str) -> str | None:
    """Parse various date string formats into YYYY-MM-DD. Returns None if unparseable."""
    if not raw:
        return None
    raw = raw.strip()
    formats = [
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S GMT",
        "%B %d, %Y",
        "%b %d, %Y",
        "%d %B %Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(raw[:26], fmt).date().isoformat()
        except ValueError:
            continue
    # Try just the first 10 chars if it looks like a date
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
            pub_el   = (item.find("pubDate") or item.find("published")
                        or item.find("updated"))
            if not title_el:
                continue
            title = title_el.get_text(strip=True)
            link  = (link_el.get_text(strip=True) or link_el.get("href", "")) if link_el else ""
            # Parse RSS date — RSS often has good dates, use them
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
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
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
                    # pub_date will be filled in by fetch_article_details later
                    articles.append({"title": title, "url": href,
                                     "pub_date": datetime.utcnow().date().isoformat()})
            if len(articles) >= 12:
                break
    except Exception as e:
        log.warning(f"HTML scrape error [{source_name}]: {e}")
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


# ── SOURCE MAPS ───────────────────────────────────────────────────────────────

RSS_SOURCES = {
    "University Affairs Canada":            "https://www.universityaffairs.ca/feed/",
    "Policy Options (IRPP)":                "https://policyoptions.irpp.org/feed/",
    "Maclean's Education":                  "https://www.macleans.ca/education/feed/",
    "Universities Canada":                  "https://www.univcan.ca/feed/",
    "Higher Education Strategy Associates": "https://higheredstrategy.com/feed/",
    "Times Higher Education":               "https://www.timeshighereducation.com/rss.xml",
}

HTML_SOURCES = {
    "BC Government Newsroom":               ("https://news.gov.bc.ca/", "https://news.gov.bc.ca/"),
    "BC Ministry of Post-Secondary Education": ("https://news.gov.bc.ca/ministries/post-secondary-education-and-future-skills", "https://news.gov.bc.ca/"),
    "BC Indigenous Relations & Reconciliation": ("https://news.gov.bc.ca/ministries/indigenous-relations-reconciliation", "https://news.gov.bc.ca/"),
    "BC First Nations Summit":              ("https://fns.bc.ca/news/", "https://fns.bc.ca/"),
    "First Nations Health Authority":       ("https://www.fnha.ca/about/news-and-events/news", "https://www.fnha.ca/"),
    "Crown-Indigenous Relations Canada":    ("https://www.canada.ca/en/crown-indigenous-relations-northern-affairs/news.html", "https://www.canada.ca/"),
    "Government of Canada — Education":     ("https://www.canada.ca/en/employment-social-development/news.html", "https://www.canada.ca/"),
    "Innovation, Science and Economic Development Canada": ("https://www.canada.ca/en/innovation-science-economic-development/news.html", "https://www.canada.ca/"),
    "SSHRC News":                           ("https://www.sshrc-crsh.gc.ca/news_room-salle_des_nouvelles/latest_news-nouvelles_recentes-eng.aspx", "https://www.sshrc-crsh.gc.ca/"),
    "NSERC News":                           ("https://www.nserc-crsng.gc.ca/Media-Media/NewsReleases-CommuniquesDePresse_eng.asp", "https://www.nserc-crsng.gc.ca/"),
    "CIHR News":                            ("https://cihr-irsc.gc.ca/e/51999.html", "https://cihr-irsc.gc.ca/"),
    "Burnaby City Hall News":               ("https://www.burnaby.ca/city-hall/news", "https://www.burnaby.ca/"),
    "BC Legislature News":                  ("https://www.leg.bc.ca/parliamentary-business/legislation-debates-proceedings", "https://www.leg.bc.ca/"),
}


# ── MAIN SCRAPE ───────────────────────────────────────────────────────────────

def run_scrape():
    log.info("=== PolicyPulse scrape started ===")
    total_added = 0
    all_errors  = []

    sources = get_sources()
    for source in [s for s in sources if s["active"]]:
        name = source["name"]
        log.info(f"Scraping: {name}")
        try:
            if name in RSS_SOURCES:
                raw = scrape_rss(RSS_SOURCES[name], name)
            elif name in HTML_SOURCES:
                url, base = HTML_SOURCES[name]
                raw = scrape_generic(url, name, base)
            else:
                raw = scrape_generic(source["url"], name)
            added = _process_and_save(raw, source)
            total_added += added
            update_source_scraped(name, added)
            log.info(f"  -> {added} new articles")
        except Exception as e:
            all_errors.append(f"{name}: {e}")
            log.error(f"Error [{name}]: {e}")
        time.sleep(DELAY_BETWEEN_SOURCES)

    # Google News keyword scraping
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


def _process_and_save(raw_articles, source, relevance_boost=0):
    added = 0
    source_name  = source["name"]
    jurisdiction = source.get("jurisdiction", "Unknown")

    for raw in raw_articles:
        title      = (raw.get("title") or "").strip()
        url        = (raw.get("url")   or "").strip()
        pub_date   = raw.get("pub_date", datetime.utcnow().date().isoformat())
        forced_tag = raw.get("forced_tag")

        if not title or not url or len(title) < 10:
            continue

        url_hash = hashlib.sha256(url.encode()).hexdigest()

        # ── Fetch full article body + real publish date ──────────────────────
        log.debug(f"  Fetching article body: {url[:80]}")
        article_text, extracted_date = fetch_article_details(url)

        # Prefer the date extracted from the article page over the RSS/scrape date,
        # unless the article page didn't have one
        if extracted_date:
            pub_date = extracted_date

        time.sleep(DELAY_BETWEEN_ARTICLES)

        # ── AI analysis with full text ───────────────────────────────────────
        ai = analyze_article(
            title=title,
            url=url,
            source_name=source_name,
            article_text=article_text,
        )
        if ai is None:
            continue

        if relevance_boost:
            ai["relevance"] = min(10, ai["relevance"] + relevance_boost)

        tags = ai.get("tags", [])
        if forced_tag and forced_tag not in tags:
            tags.insert(0, forced_tag)
        ai["tags"] = tags

        inserted = save_article(
            title=title, url=url, url_hash=url_hash,
            source=source_name, jurisdiction=jurisdiction,
            domain=ai["domain"], relevance=ai["relevance"],
            sentiment=ai["sentiment"], summary=ai["summary"],
            why_it_matters=ai["why_it_matters"],
            pub_date=pub_date, tags=",".join(ai["tags"]),
        )
        if inserted:
            added += 1

    return added
