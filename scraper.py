"""
PolicyPulse Scraper — fetches articles from all configured sources.
Uses requests + BeautifulSoup. Processes each article through Gemini AI.
"""

import hashlib
import logging
import os
import time
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from database import save_article, get_sources, log_scrape, update_source_scraped
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
DELAY_BETWEEN_SOURCES = 2  # seconds, polite crawling


# ── SOURCE-SPECIFIC SCRAPERS ───────────────────────────────

def scrape_bc_postsecondary(url):
    """BC Ministry of Post-Secondary Education"""
    articles = []
    try:
        resp = requests.get("https://news.gov.bc.ca/ministries/post-secondary-education-and-future-skills", headers=HEADERS, timeout=REQUEST_TIMEOUT)
        soup = BeautifulSoup(resp.text, "html.parser")
        for item in soup.select("article, .news-item, .views-row")[:15]:
            title_el = item.select_one("h2 a, h3 a, .news-title a, a")
            if not title_el:
                continue
            title = title_el.get_text(strip=True)
            href = title_el.get("href", "")
            if href and not href.startswith("http"):
                href = urljoin("https://news.gov.bc.ca", href)
            date_el = item.select_one("time, .date, .news-date, [datetime]")
            pub_date = date_el.get("datetime", date_el.get_text(strip=True))[:10] if date_el else datetime.utcnow().date().isoformat()
            if title and href:
                articles.append({"title": title, "url": href, "pub_date": pub_date[:10] if len(pub_date) >= 10 else pub_date})
    except Exception as e:
        log.warning(f"BC Post-Secondary scrape error: {e}")
    return articles


def scrape_generic_news(url, source_name, base_url=None):
    """Generic news page scraper — works for most government news pages."""
    articles = []
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Try multiple common patterns
        selectors = [
            "article h2 a", "article h3 a",
            ".news-item a", ".news-title a",
            ".views-row a", ".field-content a",
            "h2.title a", "h3.title a",
            ".entry-title a", ".post-title a",
            "li.item a", ".media-body a",
            "h2 a", "h3 a",
        ]

        seen = set()
        for sel in selectors:
            for el in soup.select(sel)[:20]:
                title = el.get_text(strip=True)
                href = el.get("href", "")
                if not title or len(title) < 15 or href in seen:
                    continue
                if any(skip in title.lower() for skip in ["home", "about", "contact", "menu", "search", "login"]):
                    continue
                if href and not href.startswith("http"):
                    base = base_url or url
                    href = urljoin(base, href)
                if href and title:
                    seen.add(href)
                    articles.append({
                        "title": title,
                        "url": href,
                        "pub_date": datetime.utcnow().date().isoformat(),
                    })
            if len(articles) >= 10:
                break

    except Exception as e:
        log.warning(f"Generic scrape error for {source_name} ({url}): {e}")

    return articles[:12]


def scrape_rss(url, source_name):
    """Scrape RSS/Atom feed — most reliable method."""
    articles = []
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "xml")

        items = soup.find_all("item") or soup.find_all("entry")
        for item in items[:15]:
            title = item.find("title")
            link = item.find("link")
            pub = item.find("pubDate") or item.find("published") or item.find("updated")

            if not title:
                continue

            title_text = title.get_text(strip=True)
            if link:
                link_url = link.get_text(strip=True) or link.get("href", "")
            else:
                link_url = ""

            pub_date = datetime.utcnow().date().isoformat()
            if pub:
                raw = pub.get_text(strip=True)
                for fmt in ["%a, %d %b %Y %H:%M:%S %z", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d"]:
                    try:
                        pub_date = datetime.strptime(raw[:25], fmt).date().isoformat()
                        break
                    except ValueError:
                        pass

            if title_text and link_url:
                articles.append({"title": title_text, "url": link_url, "pub_date": pub_date})

    except Exception as e:
        log.warning(f"RSS scrape error for {source_name} ({url}): {e}")

    return articles


# ── SOURCE ROUTING MAP ─────────────────────────────────────

RSS_SOURCES = {
    "University Affairs Canada": "https://www.universityaffairs.ca/feed/",
    "Policy Options (IRPP)": "https://policyoptions.irpp.org/feed/",
    "Maclean's Education": "https://www.macleans.ca/education/feed/",
    "Universities Canada": "https://www.univcan.ca/feed/",
    "Higher Education Strategy Associates": "https://higheredstrategy.com/feed/",
    "Times Higher Education": "https://www.timeshighereducation.com/rss.xml",
}

SCRAPE_SOURCES = {
    "BC Government Newsroom": ("https://news.gov.bc.ca/", "https://news.gov.bc.ca/"),
    "BC Ministry of Post-Secondary Education": ("https://news.gov.bc.ca/ministries/post-secondary-education-and-future-skills", "https://news.gov.bc.ca/"),
    "BC Indigenous Relations & Reconciliation": ("https://news.gov.bc.ca/ministries/indigenous-relations-reconciliation", "https://news.gov.bc.ca/"),
    "BC First Nations Summit": ("https://fns.bc.ca/news/", "https://fns.bc.ca/"),
    "First Nations Health Authority": ("https://www.fnha.ca/about/news-and-events/news", "https://www.fnha.ca/"),
    "Crown-Indigenous Relations Canada": ("https://www.canada.ca/en/crown-indigenous-relations-northern-affairs/news.html", "https://www.canada.ca/"),
    "Government of Canada — Education": ("https://www.canada.ca/en/employment-social-development/news.html", "https://www.canada.ca/"),
    "Innovation, Science and Economic Development Canada": ("https://www.canada.ca/en/innovation-science-economic-development/news.html", "https://www.canada.ca/"),
    "SSHRC News": ("https://www.sshrc-crsh.gc.ca/news_room-salle_des_nouvelles/latest_news-nouvelles_recentes-eng.aspx", "https://www.sshrc-crsh.gc.ca/"),
    "NSERC News": ("https://www.nserc-crsng.gc.ca/Media-Media/NewsReleases-CommuniquesDePresse_eng.asp", "https://www.nserc-crsng.gc.ca/"),
    "CIHR News": ("https://cihr-irsc.gc.ca/e/51999.html", "https://cihr-irsc.gc.ca/"),
    "Burnaby City Hall News": ("https://www.burnaby.ca/city-hall/news", "https://www.burnaby.ca/"),
    "BC Legislature News": ("https://www.leg.bc.ca/parliamentary-business/legislation-debates-proceedings", "https://www.leg.bc.ca/"),
    "BC Public Service Agency": ("https://www2.gov.bc.ca/gov/content/careers-myhr", "https://www2.gov.bc.ca/"),
}


# ── MAIN SCRAPE RUNNER ─────────────────────────────────────

def run_scrape():
    """Run full scrape across all active sources. Called by scheduler and /scrape endpoint."""
    log.info("=== PolicyPulse scrape started ===")
    total_added = 0
    all_errors = []

    sources = get_sources()
    active_sources = [s for s in sources if s["active"]]

    for source in active_sources:
        name = source["name"]
        log.info(f"Scraping: {name}")
        raw_articles = []

        try:
            if name in RSS_SOURCES:
                raw_articles = scrape_rss(RSS_SOURCES[name], name)
            elif name in SCRAPE_SOURCES:
                src_url, base = SCRAPE_SOURCES[name]
                raw_articles = scrape_generic_news(src_url, name, base)
            else:
                raw_articles = scrape_generic_news(source["url"], name)

            log.info(f"  Found {len(raw_articles)} raw articles from {name}")

            added = 0
            for raw in raw_articles:
                title = raw.get("title", "").strip()
                url = raw.get("url", "").strip()
                pub_date = raw.get("pub_date", datetime.utcnow().date().isoformat())

                if not title or not url or len(title) < 10:
                    continue

                url_hash = hashlib.sha256(url.encode()).hexdigest()

                # Run AI analysis
                ai = analyze_article(title, url, source_name=name)
                if ai is None:
                    continue  # Below relevance threshold or AI error

                inserted = save_article(
                    title=title,
                    url=url,
                    url_hash=url_hash,
                    source=name,
                    jurisdiction=source.get("jurisdiction", "Unknown"),
                    domain=ai["domain"],
                    relevance=ai["relevance"],
                    sentiment=ai["sentiment"],
                    summary=ai["summary"],
                    why_it_matters=ai["why_it_matters"],
                    pub_date=pub_date,
                    tags=",".join(ai.get("tags", [])),
                )
                if inserted:
                    added += 1

            total_added += added
            update_source_scraped(name, added)
            log.info(f"  Added {added} new articles from {name}")

        except Exception as e:
            err = f"{name}: {str(e)}"
            all_errors.append(err)
            log.error(f"Error scraping {name}: {e}")

        time.sleep(DELAY_BETWEEN_SOURCES)

    log_scrape(total_added, "; ".join(all_errors))
    log.info(f"=== Scrape complete. {total_added} new articles. {len(all_errors)} errors. ===")
    return {"added": total_added, "errors": all_errors}
