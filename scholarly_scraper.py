"""
PolicyPulse Scholarly Scraper v2
────────────────────────────────
Changes from v1:
  - Canadian think-tank sources now read from `research_sources` DB table
    (fully manageable from the app UI) instead of a hardcoded dict.
  - Scholarly search keywords now read from `scholarly_keywords` DB table.
  - All open-access API sources (OpenAlex, Semantic Scholar, PubMed, DOAJ, arXiv)
    remain unchanged — they're queried by keyword, not by URL.
"""

import asyncio
import hashlib
import logging
import os
import time
from datetime import datetime, timedelta
from urllib.parse import quote_plus, urljoin

import requests
import httpx
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


# ── OPENALEX ───────────────────────────────────────────────────────────────────

def fetch_openalex(keywords: list[str], days_back: int = 90) -> list[dict]:
    results = []
    since_date = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    for kw in keywords[:8]:
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

                abstract = _reconstruct_abstract(work.get("abstract_inverted_index", {}))
                oa_url = (work.get("open_access") or {}).get("oa_url", "")
                doi = work.get("doi", "")
                url = oa_url or (f"https://doi.org/{doi.replace('https://doi.org/','')}" if doi else "")
                if not url:
                    continue

                primary_loc = work.get("primary_location") or {}
                source = (primary_loc.get("source") or {}).get("display_name", "OpenAlex")
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
    if not inv_index:
        return ""
    positions = {}
    for word, pos_list in inv_index.items():
        for pos in pos_list:
            positions[pos] = word
    return " ".join(positions[k] for k in sorted(positions.keys()))[:1500]


# ── SEMANTIC SCHOLAR ───────────────────────────────────────────────────────────

def fetch_semantic_scholar(keywords: list[str]) -> list[dict]:
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

            time.sleep(1.0)

        except Exception as e:
            log.warning(f"Semantic Scholar error for '{kw}': {e}")

    log.info(f"Semantic Scholar: {len(results)} papers fetched")
    return results


# ── DOAJ ──────────────────────────────────────────────────────────────────────

def fetch_doaj(keywords: list[str]) -> list[dict]:
    results = []
    for kw in keywords[:4]:
        try:
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
    results = []
    combined_query = " OR ".join(f'"{kw}"[Title/Abstract]' for kw in keywords[:4])
    combined_query += " AND (Canada[Affiliation] OR Canada[Title/Abstract])"

    try:
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
                "abstract": "",
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


# ── ARXIV ─────────────────────────────────────────────────────────────────────

def fetch_arxiv(keywords: list[str]) -> list[dict]:
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
            title_el   = entry.find("title")
            summary_el = entry.find("summary")
            id_el      = entry.find("id")
            pub_el     = entry.find("published")
            if not title_el:
                continue

            title    = title_el.get_text(strip=True).replace("\n", " ")
            abstract = (summary_el.get_text(strip=True) if summary_el else "")[:1200]
            url      = id_el.get_text(strip=True) if id_el else ""
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


# ── CANADIAN THINK-TANKS — now driven by research_sources DB ─────────────────

def fetch_canadian_think_tanks() -> list[dict]:
    """
    Scrape publications from think-tank sources stored in research_sources table.
    Falls back to an empty list if the table doesn't exist yet.

    All source pages are fetched concurrently using fetch_all_think_tank_pages()
    (one httpx.AsyncClient shared across the batch).  HTML parsing runs
    sequentially after the parallel fetch completes — parsing is CPU-bound
    and fast enough that parallelising it offers no benefit.
    """
    from database import get_research_sources
    results = []

    try:
        db_sources = [s for s in get_research_sources() if s["active"]]
    except Exception as e:
        log.warning(f"Could not load research sources from DB: {e}")
        return results

    if not db_sources:
        return results

    # ── Parallel fetch all think-tank pages at once ───────────────────────────
    log.info(f"  Fetching {len(db_sources)} think-tank pages in parallel")
    try:
        page_results = asyncio.run(fetch_all_think_tank_pages(db_sources))
    except RuntimeError:
        # Already inside a running event loop — fall back to sequential sync
        log.warning("  asyncio.run() unavailable — falling back to sequential think-tank fetch")
        page_results = []
        for source in db_sources:
            try:
                resp = requests.get(source["url"], headers=HTML_HEADERS, timeout=TIMEOUT)
                html = resp.text if resp.status_code == 200 else None
            except Exception as e:
                log.warning(f"Think-tank sync fallback [{source['name']}]: {e}")
                html = None
            page_results.append((source, html))

    # ── Parse each page (sequential — parsing is fast) ───────────────────────
    for source, html in page_results:
        name     = source["name"]
        url      = source["url"]
        base_url = _base_url(url)
        boost    = source.get("relevance_boost", 0)

        if html is None:
            log.warning(f"Think-tank {name}: skipping — page fetch failed")
            continue

        try:
            soup = BeautifulSoup(html, "html.parser")
            seen = set()
            source_results = []

            # Generic selectors that work across most think-tank sites
            selectors = ["h2 a", "h3 a", "article a", ".entry-title a",
                         ".views-row a", ".node-title a", ".pub-title a",
                         ".views-field-title a", "td a"]

            for sel in selectors:
                for el in soup.select(sel)[:15]:
                    title = el.get_text(strip=True)
                    href  = el.get("href", "")
                    if not title or len(title) < 15 or href in seen:
                        continue
                    if any(w in title.lower() for w in ["home","about","contact","menu","sign in","donate"]):
                        continue
                    if not href.startswith("http"):
                        href = urljoin(base_url, href)
                    if href:
                        seen.add(href)
                        source_results.append({
                            "title": title,
                            "url": href,
                            "abstract": "",
                            "source": name,
                            "pub_date": datetime.utcnow().strftime("%Y-%m-%d"),
                            "database": "Canadian Think Tank",
                            "tags": [],
                            "search_keyword": name,
                            "relevance_boost": boost,
                            "open_access": True,
                        })
                if len(source_results) >= 10:
                    break

            results.extend(source_results)
            log.info(f"Think-tank {name}: {len(source_results)} items")

        except Exception as e:
            log.warning(f"Think-tank parse error [{name}]: {e}")

    return results


# ── PARALLEL THINK-TANK PAGE FETCH ────────────────────────────────────────────

async def fetch_think_tank_page_async(
    source: dict,
    session: httpx.AsyncClient,
) -> tuple[dict, str | None]:
    """Fetch one think-tank publications page asynchronously.

    Args:
        source:  A research_sources row dict with at minimum 'name' and 'url'.
        session: Shared httpx.AsyncClient from fetch_all_think_tank_pages().

    Returns:
        (source_dict, html_text | None) — html_text is None on failure so the
        caller can skip parsing for that source without crashing the batch.
    """
    name = source["name"]
    url  = source["url"]
    try:
        resp = await session.get(url, headers=HTML_HEADERS)
        if resp.status_code != 200:
            log.warning(f"Think-tank async {name}: HTTP {resp.status_code}")
            return source, None
        return source, resp.text
    except httpx.TimeoutException:
        log.warning(f"Think-tank async {name}: timeout")
        return source, None
    except httpx.RequestError as e:
        log.warning(f"Think-tank async {name}: {e}")
        return source, None
    except Exception as e:
        log.warning(f"Think-tank async {name}: unexpected error: {e}")
        return source, None


async def fetch_all_think_tank_pages(
    sources: list[dict],
) -> list[tuple[dict, str | None]]:
    """Fetch all active think-tank publications pages concurrently.

    Creates one shared httpx.AsyncClient so TCP connections are reused.
    Results are returned in the same order as the input sources list.
    Each element is (source_dict, html_text | None); a None html means
    that source failed and should be skipped during parsing.

    This replaces the sequential requests.get() + time.sleep(1.5) loop
    that previously ran inside fetch_canadian_think_tanks().  For 6 default
    think-tank sources the wall-clock time drops from ~9 s to ~2 s.

    Args:
        sources: List of active research_sources row dicts.

    Returns:
        List of (source, html | None) tuples, same order as input.
    """
    timeout = httpx.Timeout(TIMEOUT, connect=5.0)
    async with httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=True,
        limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
    ) as session:
        tasks = [fetch_think_tank_page_async(s, session) for s in sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    cleaned = []
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            log.warning(f"fetch_all_think_tank_pages unhandled [{sources[i]['name']}]: {r}")
            cleaned.append((sources[i], None))
        else:
            cleaned.append(r)
    return cleaned


def _base_url(url: str) -> str:
    from urllib.parse import urlparse
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


# ── AI ANALYSIS ───────────────────────────────────────────────────────────────

def analyze_scholarly(title: str, abstract: str, source: str,
                      url: str, database: str) -> dict | None:
    from ai_processor import analyze_article
    combined = f"[ABSTRACT FROM {database}]\n\n{abstract}" if abstract else ""
    return analyze_article(title=title, url=url, source_name=source, article_text=combined)


# ── DATABASE HELPERS ──────────────────────────────────────────────────────────

def ensure_scholarly_table():
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
    from database import get_conn
    import sqlite3
    conn = get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat()
    tags_list = list(set((item.get("tags") or []) + (ai.get("tags") or [])))
    tags_str  = ",".join(tags_list[:8])

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
    conditions, params = [], []
    if domain:
        conditions.append("domain = ?"); params.append(domain)
    if database_name:
        conditions.append("database_name = ?"); params.append(database_name)
    if search:
        conditions.append("(title LIKE ? OR summary LIKE ? OR abstract LIKE ?)")
        s = f"%{search}%"; params.extend([s, s, s])
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
    total  = conn.execute("SELECT COUNT(*) FROM scholarly_articles").fetchone()[0]
    unread = conn.execute("SELECT COUNT(*) FROM scholarly_articles WHERE read=0").fetchone()[0]
    dbs    = conn.execute(
        "SELECT database_name, COUNT(*) as n FROM scholarly_articles GROUP BY database_name ORDER BY n DESC"
    ).fetchall()
    conn.close()
    return {"total": total, "unread": unread, "databases": [dict(r) for r in dbs]}


def update_scholarly_read(article_id: int, read: bool):
    from database import get_conn
    conn = get_conn()
    conn.execute("UPDATE scholarly_articles SET read=? WHERE id=?", (1 if read else 0, article_id))
    conn.commit()
    conn.close()


# ── MAIN SCRAPE ORCHESTRATOR ───────────────────────────────────────────────────

def run_scholarly_scrape(extra_keywords: list[str] | None = None) -> dict:
    log.info("=== PolicyPulse Scholarly Scrape started ===")
    ensure_scholarly_table()

    # Keywords: from scholarly_keywords DB table + watchlist + extras passed in
    from database import get_scholarly_keywords, get_watchlist_keywords
    db_kws       = [r["keyword"] for r in get_scholarly_keywords() if r.get("active")]
    watchlist_kws = get_watchlist_keywords()
    keywords = list(set(db_kws + watchlist_kws + (extra_keywords or [])))
    if not keywords:
        log.warning("No scholarly keywords configured — using built-in defaults")
        keywords = ["Indigenous policy Canada", "post-secondary education Canada",
                    "reconciliation Canada", "pharmacare Canada"]

    all_items: list[dict] = []

    try:
        all_items.extend(fetch_openalex(keywords[:6], days_back=180))
    except Exception as e:
        log.error(f"OpenAlex failed: {e}")

    try:
        all_items.extend(fetch_semantic_scholar(keywords[:4]))
    except Exception as e:
        log.error(f"Semantic Scholar failed: {e}")

    try:
        all_items.extend(fetch_canadian_think_tanks())
    except Exception as e:
        log.error(f"Think-tanks failed: {e}")

    try:
        all_items.extend(fetch_doaj(keywords[:3]))
    except Exception as e:
        log.error(f"DOAJ failed: {e}")

    health_kws = [k for k in keywords if any(w in k.lower() for w in
                  ["health", "pharmacare", "fnha", "indigenous", "mental"])]
    if health_kws:
        try:
            all_items.extend(fetch_pubmed(health_kws[:3]))
        except Exception as e:
            log.error(f"PubMed failed: {e}")

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

    added, skipped, errors = 0, 0, []

    for item in unique_items:
        title = (item.get("title") or "").strip()
        url   = (item.get("url")   or "").strip()
        if not title or not url or len(title) < 10:
            continue

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

            boost = item.get("relevance_boost", 0)
            if boost:
                ai["relevance"] = min(10, ai["relevance"] + boost)

            if save_scholarly_article(item, ai):
                added += 1
            else:
                skipped += 1

        except Exception as e:
            errors.append(f"{title[:40]}: {e}")
            log.warning(f"Scholarly analysis error: {e}")

        time.sleep(0.3)

    log.info(f"=== Scholarly done. Added: {added}, Skipped: {skipped}, Errors: {len(errors)} ===")
    return {"added": added, "skipped": skipped, "errors": errors}
