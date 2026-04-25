"""
PolicyPulse Scholarly Scraper v1
────────────────────────────────
Fetches peer-reviewed and grey-literature research from open-access sources:

  • OpenAlex       — 250M scholarly works, completely free, no key required
  • DOAJ           — Directory of Open Access Journals, free API
  • Semantic Scholar — free API, abstracts + metadata
  • PubMed/NCBI    — biomedical/health, free E-utilities API
  • arXiv          — preprints in policy-adjacent fields
  • SSRN           — working papers in law, policy, economics
  • Canadian policy think-tanks — CCPA, Yellowhead, CD Howe, MLI, IRPP, NCCIH

Run separately from the news scraper (weekly, not daily).
All results pass through Gemini AI for relevance scoring (same threshold: >=6 kept).
Stored in the `scholarly_articles` SQLite table.
"""

import hashlib
import logging
import os
import time
from datetime import datetime, timedelta
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "PolicyPulse/1.0 (mailto:policy@policypulse.ca) "
        "Mozilla/5.0 (compatible; research-bot)"
    ),
    "Accept": "application/json",
}
HTML_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
    )
}
TIMEOUT = 20

# ── DEFAULT SEARCH TOPICS ──────────────────────────────────────────────────────
# These augment the user's watchlist keywords for scholarly searches

DEFAULT_SCHOLARLY_KEYWORDS = [
    "Indigenous policy Canada",
    "DRIPA UNDRIP reconciliation",
    "First Nations health Canada",
    "post-secondary education Canada",
    "research funding SSHRC NSERC",
    "pharmacare Canada",
    "BC government policy",
    "OCAP data sovereignty Indigenous",
    "TRC calls to action",
    "Indigenous higher education",
]

# ── OPENALEX ───────────────────────────────────────────────────────────────────

def fetch_openalex(keywords: list[str], days_back: int = 90) -> list[dict]:
    """
    Query OpenAlex for recent open-access works matching keywords.
    No API key required for up to 100k requests/day.
    """
    results = []
    since_date = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    for kw in keywords[:8]:  # limit to 8 keywords per run to stay polite
        try:
            params = {
                "search": kw,
                "filter": f"from_publication_date:{since_date},open_access.is_oa:true",
                "sort": "publication_date:desc",
                "per_page": "12",
                "select": "id,title,abstract_inverted_index,publication_date,doi,"
                           "primary_location,authorships,concepts,open_access",
            }
            resp = requests.get(
                "https://api.openalex.org/works",
                params=params,
                headers={**HEADERS, "User-Agent": "PolicyPulse/1.0 (mailto:policy@example.com)"},
                timeout=TIMEOUT,
            )
            if resp.status_code != 200:
                log.warning(f"OpenAlex {resp.status_code} for keyword: {kw}")
                continue

            data = resp.json()
            for work in data.get("results", []):
                title = work.get("title", "").strip()
                if not title or len(title) < 10:
                    continue

                # Reconstruct abstract from inverted index
                abstract = _reconstruct_abstract(work.get("abstract_inverted_index", {}))

                # Get URL — prefer OA URL, fall back to DOI
                oa_url = (work.get("open_access") or {}).get("oa_url", "")
                doi = work.get("doi", "")
                url = oa_url or (f"https://doi.org/{doi.replace('https://doi.org/','')}" if doi else "")
                if not url:
                    continue

                # Source journal/venue
                primary_loc = work.get("primary_location") or {}
                source = (primary_loc.get("source") or {}).get("display_name", "OpenAlex")

                # Top concept tags
                concepts = [c["display_name"] for c in (work.get("concepts") or [])[:4]]

                results.append({
                    "title": title,
                    "url": url,
                    "abstract": abstract,
                    "source": source or "OpenAlex",
                    "pub_date": work.get("publication_date", "")[:10],
                    "database": "OpenAlex",
                    "tags": concepts,
                    "search_keyword": kw,
                    "doi": doi,
                    "open_access": True,
                })

            time.sleep(0.5)

        except Exception as e:
            log.warning(f"OpenAlex error for '{kw}': {e}")

    log.info(f"OpenAlex: {len(results)} works fetched")
    return results


def _reconstruct_abstract(inv_index: dict) -> str:
    """Rebuild abstract text from OpenAlex inverted index format."""
    if not inv_index:
        return ""
    positions = {}
    for word, pos_list in inv_index.items():
        for pos in pos_list:
            positions[pos] = word
    return " ".join(positions[k] for k in sorted(positions.keys()))[:1500]


# ── SEMANTIC SCHOLAR ───────────────────────────────────────────────────────────

def fetch_semantic_scholar(keywords: list[str]) -> list[dict]:
    """
    Semantic Scholar Academic Graph API — free, no key required for basic use.
    Returns abstracts + metadata for high-quality papers.
    """
    results = []
    for kw in keywords[:5]:
        try:
            params = {
                "query": kw,
                "limit": 8,
                "fields": "title,abstract,year,url,venue,authors,externalIds,openAccessPdf,publicationDate",
                "publicationTypes": "JournalArticle,Review,Conference",
            }
            resp = requests.get(
                "https://api.semanticscholar.org/graph/v1/paper/search",
                params=params,
                headers=HEADERS,
                timeout=TIMEOUT,
            )
            if resp.status_code != 200:
                log.warning(f"Semantic Scholar {resp.status_code} for: {kw}")
                continue

            for paper in resp.json().get("data", []):
                title = (paper.get("title") or "").strip()
                abstract = (paper.get("abstract") or "").strip()
                if not title or not abstract:
                    continue

                # Try to get a real URL
                pdf = (paper.get("openAccessPdf") or {}).get("url", "")
                ext = paper.get("externalIds") or {}
                doi = ext.get("DOI", "")
                url = pdf or (f"https://doi.org/{doi}" if doi else paper.get("url", ""))
                if not url:
                    continue

                year = paper.get("year") or ""
                pub_date = paper.get("publicationDate") or (str(year)+"-01-01" if year else "")
                authors = ", ".join(
                    (a.get("name","") for a in (paper.get("authors") or [])[:3])
                )

                results.append({
                    "title": title,
                    "url": url,
                    "abstract": abstract[:1200],
                    "source": paper.get("venue") or "Semantic Scholar",
                    "pub_date": pub_date[:10] if pub_date else "",
                    "database": "Semantic Scholar",
                    "tags": [],
                    "search_keyword": kw,
                    "authors": authors,
                    "open_access": bool(pdf),
                })

            time.sleep(1.0)  # Semantic Scholar rate limit: 1 req/sec without key

        except Exception as e:
            log.warning(f"Semantic Scholar error for '{kw}': {e}")

    log.info(f"Semantic Scholar: {len(results)} papers fetched")
    return results


# ── DOAJ ──────────────────────────────────────────────────────────────────────

def fetch_doaj(keywords: list[str]) -> list[dict]:
    """Directory of Open Access Journals — completely free, no key required."""
    results = []
    for kw in keywords[:4]:
        try:
            params = {
                "q": kw,
                "pageSize": 8,
                "sort": "published:desc",
            }
            resp = requests.get(
                "https://doaj.org/api/search/articles/" + quote_plus(kw),
                params={"pageSize": 8, "sort": "published:desc"},
                headers=HEADERS,
                timeout=TIMEOUT,
            )
            if resp.status_code != 200:
                continue

            for art in resp.json().get("results", []):
                bib = art.get("bibjson", {})
                title = (bib.get("title") or "").strip()
                if not title:
                    continue

                abstract = (bib.get("abstract") or "").strip()
                links = bib.get("link") or []
                url = next((l["url"] for l in links if l.get("type") == "fulltext"), "")
                if not url:
                    url = next((l.get("url","") for l in links), "")
                if not url:
                    continue

                journal = (bib.get("journal") or {}).get("title", "DOAJ Journal")
                pub_date = bib.get("year","") + ("-01-01" if bib.get("year") else "")
                keywords_list = bib.get("keywords", [])[:4]

                results.append({
                    "title": title,
                    "url": url,
                    "abstract": abstract[:1200],
                    "source": journal,
                    "pub_date": pub_date[:10] if pub_date else "",
                    "database": "DOAJ",
                    "tags": keywords_list,
                    "search_keyword": kw,
                    "open_access": True,
                })

            time.sleep(0.8)

        except Exception as e:
            log.warning(f"DOAJ error for '{kw}': {e}")

    log.info(f"DOAJ: {len(results)} articles fetched")
    return results


# ── PUBMED / NCBI ─────────────────────────────────────────────────────────────

def fetch_pubmed(keywords: list[str]) -> list[dict]:
    """
    NCBI E-utilities — completely free for health/biomedical research.
    Relevant for Indigenous health, pharmacare, mental health policy.
    """
    results = []
    combined_query = " OR ".join(f'"{kw}"[Title/Abstract]' for kw in keywords[:4])
    combined_query += " AND (Canada[Affiliation] OR Canada[Title/Abstract])"

    try:
        # Step 1: Search
        search_resp = requests.get(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
            params={
                "db": "pmc",
                "term": combined_query,
                "retmax": 12,
                "sort": "pub+date",
                "retmode": "json",
                "datetype": "pdat",
                "reldate": 365,
            },
            headers=HTML_HEADERS,
            timeout=TIMEOUT,
        )
        if search_resp.status_code != 200:
            return results

        ids = search_resp.json().get("esearchresult", {}).get("idlist", [])
        if not ids:
            return results

        # Step 2: Fetch summaries
        summary_resp = requests.get(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi",
            params={"db": "pmc", "id": ",".join(ids), "retmode": "json"},
            headers=HTML_HEADERS,
            timeout=TIMEOUT,
        )
        if summary_resp.status_code != 200:
            return results

        doc = summary_resp.json()
        for pmid, article in doc.get("result", {}).items():
            if pmid == "uids":
                continue
            title = (article.get("title") or "").strip()
            if not title:
                continue

            pmcid = article.get("pmcid", "")
            url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmcid}/" if pmcid else ""
            if not url:
                continue

            pub_date = article.get("pubdate", "")[:10]
            source = article.get("fulljournalname") or article.get("source", "PubMed Central")

            results.append({
                "title": title,
                "url": url,
                "abstract": "",  # fetch separately if needed
                "source": source,
                "pub_date": pub_date,
                "database": "PubMed Central",
                "tags": [],
                "search_keyword": combined_query[:60],
                "open_access": True,
            })

        time.sleep(0.5)

    except Exception as e:
        log.warning(f"PubMed error: {e}")

    log.info(f"PubMed: {len(results)} articles fetched")
    return results


# ── CANADIAN THINK-TANKS (HTML SCRAPING) ─────────────────────────────────────

CANADIAN_SOURCES = {
    "Canadian Centre for Policy Alternatives": {
        "url": "https://www.policyalternatives.ca/publications",
        "base": "https://www.policyalternatives.ca",
        "selectors": ["h2 a", "h3 a", ".views-row a", ".node-title a"],
        "relevance_boost": 1,
    },
    "Yellowhead Institute": {
        "url": "https://yellowheadinstitute.org/resources/",
        "base": "https://yellowheadinstitute.org",
        "selectors": ["h2 a", "h3 a", "article a", ".entry-title a"],
        "relevance_boost": 2,  # Highly relevant for Indigenous policy
    },
    "National Collaborating Centre for Indigenous Health": {
        "url": "https://www.nccih.ca/495/Publications_and_Resources.nccih",
        "base": "https://www.nccih.ca",
        "selectors": ["h3 a", "h2 a", ".pub-title a", "td a"],
        "relevance_boost": 2,
    },
    "Macdonald-Laurier Institute": {
        "url": "https://macdonaldlaurier.ca/publications/",
        "base": "https://macdonaldlaurier.ca",
        "selectors": ["h2 a", "h3 a", ".entry-title a"],
        "relevance_boost": 0,
    },
    "CD Howe Institute": {
        "url": "https://www.cdhowe.org/intelligence-memos",
        "base": "https://www.cdhowe.org",
        "selectors": ["h2 a", "h3 a", ".views-field-title a"],
        "relevance_boost": 0,
    },
    "Broadbent Institute": {
        "url": "https://www.broadbentinstitute.ca/research",
        "base": "https://www.broadbentinstitute.ca",
        "selectors": ["h2 a", "h3 a", "article a"],
        "relevance_boost": 0,
    },
}


def fetch_canadian_think_tanks() -> list[dict]:
    """Scrape publications from major Canadian policy think-tanks."""
    results = []
    for name, config in CANADIAN_SOURCES.items():
        try:
            resp = requests.get(config["url"], headers=HTML_HEADERS, timeout=TIMEOUT)
            if resp.status_code != 200:
                log.warning(f"Think-tank {name}: HTTP {resp.status_code}")
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            seen = set()

            for sel in config["selectors"]:
                for el in soup.select(sel)[:15]:
                    title = el.get_text(strip=True)
                    href = el.get("href", "")
                    if not title or len(title) < 15 or href in seen:
                        continue
                    if any(w in title.lower() for w in ["home","about","contact","menu","sign in"]):
                        continue
                    if not href.startswith("http"):
                        href = urljoin(config["base"], href)
                    if href:
                        seen.add(href)
                        results.append({
                            "title": title,
                            "url": href,
                            "abstract": "",
                            "source": name,
                            "pub_date": datetime.utcnow().strftime("%Y-%m-%d"),
                            "database": "Canadian Think Tank",
                            "tags": [],
                            "search_keyword": name,
                            "relevance_boost": config.get("relevance_boost", 0),
                            "open_access": True,
                        })
                if len(results) >= 10:
                    break

            log.info(f"Think-tank {name}: {len([r for r in results if r['source']==name])} items")
            time.sleep(1.5)

        except Exception as e:
            log.warning(f"Think-tank scrape error [{name}]: {e}")

    return results


# ── ARXIV ─────────────────────────────────────────────────────────────────────

def fetch_arxiv(keywords: list[str]) -> list[dict]:
    """
    arXiv Atom API — preprints in social sciences, economics, policy.
    Relevant cs.CY (Computers & Society), econ.GN (General Economics).
    """
    results = []
    query = " OR ".join(f'ti:"{kw}" OR abs:"{kw}"' for kw in keywords[:3])
    try:
        resp = requests.get(
            "https://export.arxiv.org/api/query",
            params={
                "search_query": query + " AND (cat:econ.GN OR cat:cs.CY OR cat:q-bio.PE)",
                "max_results": 8,
                "sortBy": "submittedDate",
                "sortOrder": "descending",
            },
            headers=HTML_HEADERS,
            timeout=TIMEOUT,
        )
        if resp.status_code != 200:
            return results

        soup = BeautifulSoup(resp.content, "xml")
        for entry in soup.find_all("entry")[:8]:
            title_el = entry.find("title")
            summary_el = entry.find("summary")
            id_el = entry.find("id")
            pub_el = entry.find("published")
            if not title_el:
                continue

            title = title_el.get_text(strip=True).replace("\n", " ")
            abstract = (summary_el.get_text(strip=True) if summary_el else "")[:1200]
            url = id_el.get_text(strip=True) if id_el else ""
            pub_date = (pub_el.get_text(strip=True) if pub_el else "")[:10]

            if title and url:
                results.append({
                    "title": title,
                    "url": url,
                    "abstract": abstract,
                    "source": "arXiv",
                    "pub_date": pub_date,
                    "database": "arXiv",
                    "tags": [],
                    "search_keyword": keywords[0] if keywords else "",
                    "open_access": True,
                })

    except Exception as e:
        log.warning(f"arXiv error: {e}")

    log.info(f"arXiv: {len(results)} preprints")
    return results


# ── AI ANALYSIS ───────────────────────────────────────────────────────────────

def analyze_scholarly(title: str, abstract: str, source: str,
                      url: str, database: str) -> dict | None:
    """
    AI analysis optimised for scholarly/research content.
    Works from title + abstract (no full text needed).
    Returns None if relevance < 6.
    """
    from ai_processor import analyze_article

    # Combine title + abstract as the "article text" for the AI
    combined = f"[ABSTRACT FROM {database}]\n\n{abstract}" if abstract else ""

    result = analyze_article(
        title=title,
        url=url,
        source_name=source,
        article_text=combined,
    )
    return result


# ── DATABASE HELPERS ──────────────────────────────────────────────────────────

def ensure_scholarly_table():
    """Create scholarly_articles table if it doesn't exist."""
    from database import get_conn
    conn = get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS scholarly_articles (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            title           TEXT NOT NULL,
            url             TEXT NOT NULL,
            url_hash        TEXT UNIQUE NOT NULL,
            source          TEXT,
            database_name   TEXT,
            jurisdiction    TEXT,
            domain          TEXT,
            relevance       INTEGER DEFAULT 5,
            sentiment       TEXT DEFAULT 'Neutral',
            summary         TEXT,
            why_it_matters  TEXT,
            abstract        TEXT,
            authors         TEXT,
            doi             TEXT,
            pub_date        TEXT,
            processed_date  TEXT,
            open_access     INTEGER DEFAULT 1,
            tags            TEXT,
            read            INTEGER DEFAULT 0,
            staged          INTEGER DEFAULT 0,
            search_keyword  TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_sch_pub_date  ON scholarly_articles(pub_date DESC);
        CREATE INDEX IF NOT EXISTS idx_sch_relevance ON scholarly_articles(relevance DESC);
        CREATE INDEX IF NOT EXISTS idx_sch_domain    ON scholarly_articles(domain);
    """)
    conn.commit()
    conn.close()


def save_scholarly_article(item: dict, ai: dict) -> bool:
    """Insert scholarly article if not already in DB. Returns True if inserted."""
    from database import get_conn
    import sqlite3

    conn = get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat()

    tags_list = list(set((item.get("tags") or []) + (ai.get("tags") or [])))
    tags_str = ",".join(tags_list[:8])

    try:
        cur.execute("""
            INSERT INTO scholarly_articles
              (title, url, url_hash, source, database_name, jurisdiction, domain,
               relevance, sentiment, summary, why_it_matters, abstract, authors,
               doi, pub_date, processed_date, open_access, tags, search_keyword)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            item["title"],
            item["url"],
            hashlib.sha256(item["url"].encode()).hexdigest(),
            item.get("source", ""),
            item.get("database", ""),
            ai.get("jurisdiction", "Unknown"),
            ai.get("domain", "Other"),
            ai.get("relevance", 6),
            ai.get("sentiment", "Neutral"),
            ai.get("summary", item["title"]),
            ai.get("why_it_matters", ""),
            item.get("abstract", "")[:2000],
            item.get("authors", ""),
            item.get("doi", ""),
            item.get("pub_date", now[:10]),
            now,
            1 if item.get("open_access") else 0,
            tags_str,
            item.get("search_keyword", ""),
        ))
        conn.commit()
        inserted = True
    except sqlite3.IntegrityError:
        inserted = False
    finally:
        conn.close()

    return inserted


def get_scholarly_articles(domain=None, database_name=None, search=None,
                            limit=50, offset=0, sort="date") -> list[dict]:
    from database import get_conn
    conn = get_conn()
    conditions = []
    params = []
    if domain:
        conditions.append("domain = ?")
        params.append(domain)
    if database_name:
        conditions.append("database_name = ?")
        params.append(database_name)
    if search:
        conditions.append("(title LIKE ? OR summary LIKE ? OR abstract LIKE ?)")
        s = f"%{search}%"
        params.extend([s, s, s])
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    order = "pub_date DESC" if sort == "date" else "relevance DESC, pub_date DESC"
    rows = conn.execute(
        f"SELECT * FROM scholarly_articles {where} ORDER BY {order} LIMIT ? OFFSET ?",
        params + [limit, offset]
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_scholarly_stats() -> dict:
    from database import get_conn
    conn = get_conn()
    total = conn.execute("SELECT COUNT(*) FROM scholarly_articles").fetchone()[0]
    unread = conn.execute("SELECT COUNT(*) FROM scholarly_articles WHERE read=0").fetchone()[0]
    dbs = conn.execute(
        "SELECT database_name, COUNT(*) as n FROM scholarly_articles GROUP BY database_name ORDER BY n DESC"
    ).fetchall()
    conn.close()
    return {
        "total": total,
        "unread": unread,
        "databases": [dict(r) for r in dbs],
    }


def update_scholarly_read(article_id: int, read: bool):
    from database import get_conn
    conn = get_conn()
    conn.execute("UPDATE scholarly_articles SET read=? WHERE id=?", (1 if read else 0, article_id))
    conn.commit()
    conn.close()


# ── MAIN SCRAPE ORCHESTRATOR ───────────────────────────────────────────────────

def run_scholarly_scrape(extra_keywords: list[str] | None = None) -> dict:
    """
    Main entry point. Call from FastAPI background task or scheduler.
    Combines all sources, deduplicates, AI-scores, and saves.
    """
    log.info("=== PolicyPulse Scholarly Scrape started ===")

    ensure_scholarly_table()

    # Build keyword list: defaults + watchlist + extras
    keywords = list(DEFAULT_SCHOLARLY_KEYWORDS)
    if extra_keywords:
        keywords = list(set(keywords + extra_keywords))

    all_items: list[dict] = []

    # 1. OpenAlex (best free scholarly API)
    try:
        all_items.extend(fetch_openalex(keywords[:6], days_back=180))
    except Exception as e:
        log.error(f"OpenAlex failed: {e}")

    # 2. Semantic Scholar
    try:
        all_items.extend(fetch_semantic_scholar(keywords[:4]))
    except Exception as e:
        log.error(f"Semantic Scholar failed: {e}")

    # 3. Canadian think-tanks (most policy-relevant)
    try:
        all_items.extend(fetch_canadian_think_tanks())
    except Exception as e:
        log.error(f"Think-tanks failed: {e}")

    # 4. DOAJ
    try:
        all_items.extend(fetch_doaj(keywords[:3]))
    except Exception as e:
        log.error(f"DOAJ failed: {e}")

    # 5. PubMed (health-focused keywords only)
    health_kws = [k for k in keywords if any(w in k.lower() for w in
                  ["health", "pharmacare", "fnha", "indigenous", "mental"])]
    if health_kws:
        try:
            all_items.extend(fetch_pubmed(health_kws[:3]))
        except Exception as e:
            log.error(f"PubMed failed: {e}")

    # 6. arXiv
    try:
        all_items.extend(fetch_arxiv([k for k in keywords if len(k) > 6][:3]))
    except Exception as e:
        log.error(f"arXiv failed: {e}")

    # Deduplicate by URL
    seen_urls: set[str] = set()
    unique_items = []
    for item in all_items:
        url = item.get("url", "").strip()
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_items.append(item)

    log.info(f"Scholarly: {len(unique_items)} unique items to process")

    added = 0
    skipped = 0
    errors = []

    for item in unique_items:
        title = (item.get("title") or "").strip()
        url = (item.get("url") or "").strip()
        if not title or not url or len(title) < 10:
            continue

        # AI analysis
        try:
            ai = analyze_scholarly(
                title=title,
                abstract=item.get("abstract", ""),
                source=item.get("source", ""),
                url=url,
                database=item.get("database", ""),
            )
            if ai is None:
                skipped += 1
                continue

            # Apply optional relevance boost for highly-relevant sources
            boost = item.get("relevance_boost", 0)
            if boost:
                ai["relevance"] = min(10, ai["relevance"] + boost)

            if save_scholarly_article(item, ai):
                added += 1
            else:
                skipped += 1  # duplicate URL

        except Exception as e:
            errors.append(f"{title[:40]}: {e}")
            log.warning(f"Scholarly analysis error: {e}")

        time.sleep(0.3)  # gentle rate limit

    log.info(f"=== Scholarly done. Added: {added}, Skipped: {skipped}, Errors: {len(errors)} ===")
    return {"added": added, "skipped": skipped, "errors": errors}
