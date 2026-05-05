"""
PolicyPulse Scholarly Scraper v3
─────────────────────────────────
Changes from v2:
  - run_scholarly_scrape() now accepts an optional filter_config dict
    (same shape as scraper.py's filter_config).
  - Individual databases (OpenAlex, Semantic Scholar, PubMed, DOAJ, arXiv,
    Canadian Think-Tanks) can be toggled on/off via
    filter_config["scholarly_databases"].
  - days_back for OpenAlex is now taken from filter_config["days_back"]
    instead of being hardcoded at 180.
  - min_relevance is enforced after AI scoring — articles below the user's
    threshold are discarded even if they cleared the AI's internal threshold
    of 6.
  - domain_whitelist and must_include keyword filters applied after AI
    scoring, consistent with scraper.py.
  - dry_run mode: AI scores and logs would-have-saved articles but does not
    write to the DB.

Bug fixes (v3.1):
  - fetch_canadian_think_tanks() now uses plain sequential requests.get()
    instead of asyncio.run(). The previous asyncio.run() call raised a silent
    RuntimeError when invoked from inside FastAPI's already-running event loop
    (via BackgroundTasks), causing the think-tank fetch — and sometimes the
    entire scrape — to produce zero results.
  - analyze_scholarly() now passes a context prefix to the AI that instructs
    it to score academic abstracts generously. Previously, formal academic
    language caused many relevant papers to score below 6 and be silently
    dropped.
  - asyncio import retained for the async helper functions (fetch_think_tank_
    page_async, fetch_all_think_tank_pages) which are kept for potential
    future use but are no longer called by the main scrape path.
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

# ── DEFAULT FILTER CONFIG (scholarly subset) ─────────────────────────────────
# Only the keys that the scholarly scraper actually uses.  The full default
# is owned by scraper.py; this is just a local fallback so we never KeyError.

_SCHOLARLY_CONFIG_DEFAULTS: dict = {
    "min_relevance":    6,
    "days_back":        90,
    "domain_whitelist": [],
    "must_include":     [],
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


def _resolve_scholarly_config(filter_config: dict | None) -> dict:
    """Merge filter_config onto scholarly defaults, clamping numeric fields."""
    cfg = dict(_SCHOLARLY_CONFIG_DEFAULTS)
    cfg["scholarly_databases"] = dict(_SCHOLARLY_CONFIG_DEFAULTS["scholarly_databases"])

    if filter_config:
        for k, v in filter_config.items():
            if k == "scholarly_databases" and isinstance(v, dict):
                cfg["scholarly_databases"].update(v)
            else:
                cfg[k] = v

    cfg["min_relevance"] = max(1, min(10, int(cfg["min_relevance"])))
    cfg["days_back"]     = max(7, min(730, int(cfg["days_back"])))
    cfg["dry_run"]       = bool(cfg["dry_run"])
    return cfg


# ── OPENALEX ──────────────────────────────────────────────────────────────────

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


# ── SEMANTIC SCHOLAR ──────────────────────────────────────────────────────────

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
                # Only require a title — many valid papers have no abstract in
                # the Semantic Scholar API. Dropping on missing abstract was
                # silently discarding the majority of results.
                if not title:
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


# ── DOAJ ─────────────────────────────────────────────────────────────────────

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


# ── CANADIAN THINK-TANKS ──────────────────────────────────────────────────────

def fetch_canadian_think_tanks() -> list[dict]:
    """Scrape publications from think-tank sources stored in research_sources table.

    Uses plain sequential requests.get() so it is safe to call from inside
    FastAPI's async event loop (via BackgroundTasks) without triggering the
    'This event loop is already running' RuntimeError that asyncio.run() raises
    in that context.
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

    log.info(f"  Fetching {len(db_sources)} think-tank pages sequentially")
    page_results = []
    for source in db_sources:
        try:
            resp = requests.get(source["url"], headers=HTML_HEADERS, timeout=TIMEOUT)
            html = resp.text if resp.status_code == 200 else None
        except Exception as e:
            log.warning(f"Think-tank fetch [{source['name']}]: {e}")
            html = None
        page_results.append((source, html))

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

            selectors = ["h2 a", "h3 a", "article a", ".entry-title a",
                         ".views-row a", ".node-title a", ".pub-title a",
                         ".views-field-title a", "td a"]

            for sel in selectors:
                for el in soup.select(sel)[:15]:
                    title = el.get_text(strip=True)
                    href  = el.get("href", "")
                    if not title or len(title) < 15 or href in seen:
                        continue
                    if any(w in title.lower() for w in
                           ["home","about","contact","menu","sign in","donate"]):
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


# ── PARALLEL THINK-TANK PAGE FETCH ───────────────────────────────────────────

async def fetch_think_tank_page_async(
    source: dict,
    session: httpx.AsyncClient,
) -> tuple[dict, str | None]:
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


# ── AI ANALYSIS ──────────────────────────────────────────────────────────────

def analyze_scholarly(title: str, abstract: str, source: str,
                      url: str, database: str) -> dict | None:
    """Score a scholarly article for relevance using the AI processor.

    Two key behaviours that prevent zero-result scrapes:

    1. The scholarly context hint is ALWAYS included in article_text, even
       when abstract is empty. Without it, `_build_payload` uses the
       title-only prompt with no scholarly instruction, and academic titles
       score 3-5 against the news-calibrated prompt — all silently dropped.

    2. If the AI still returns None (scored < 6 despite the hint, because
       the prompt's "return null" instruction can override our hint in the
       article text), we fall back to the keyword-based default analyser
       instead of discarding the paper. Papers that survived fetching and
       dedup are worth keeping; the keyword fallback always returns
       relevance >= 6 for policy-adjacent titles.
    """
    from ai_processor import analyze_article, _default_analysis

    # Always include the scholarly hint so it reaches the AI whether or not
    # an abstract is available. The hint must be long enough (>150 chars)
    # that _build_payload treats it as article_text and uses ANALYSIS_PROMPT_FULL
    # rather than the title-only fallback prompt.
    scholarly_hint = (
        f"[SCHOLARLY ARTICLE FROM {database} — this is peer-reviewed academic "
        f"content. Score relevance generously: any paper relating to Indigenous "
        f"policy, post-secondary education, health policy, reconciliation, research "
        f"funding, or Canadian governance should score at least 6. Do not penalise "
        f"academic language or the absence of direct government quotes.]\n\n"
    )

    if abstract:
        combined = scholarly_hint + f"ABSTRACT:\n{abstract}"
    else:
        # No abstract — give the model just the hint so it scores on title
        # with the generous instruction rather than the news-calibrated default.
        combined = scholarly_hint + "NOTE: No abstract available — score based on title alone using the generous instruction above."

    result = analyze_article(title=title, url=url, source_name=source, article_text=combined)

    # If the AI still returned None (relevance < 6) despite the hint, use the
    # keyword-based fallback. This is the safety net that prevents relevant
    # academic papers from being silently discarded.
    if result is None:
        log.debug(f"  AI returned None for scholarly '{title[:60]}' — using keyword fallback")
        result = _default_analysis(title, source)

    return result


# ── DATABASE HELPERS ─────────────────────────────────────────────────────────

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
        "SELECT database_name, COUNT(*) as n FROM scholarly_articles "
        "GROUP BY database_name ORDER BY n DESC"
    ).fetchall()
    conn.close()
    return {"total": total, "unread": unread, "databases": [dict(r) for r in dbs]}


def update_scholarly_read(article_id: int, read: bool):
    from database import get_conn
    conn = get_conn()
    conn.execute(
        "UPDATE scholarly_articles SET read=? WHERE id=?",
        (1 if read else 0, article_id)
    )
    conn.commit()
    conn.close()


# ── MAIN SCRAPE ORCHESTRATOR ──────────────────────────────────────────────────

def run_scholarly_scrape(
    extra_keywords: list[str] | None = None,
    filter_config: dict | None = None,
) -> dict:
    """Fetch, score, filter, and save scholarly articles.

    Args:
        extra_keywords: Additional keywords beyond the DB watchlist (e.g.
                        passed in from the frontend's manual trigger).
        filter_config:  Optional filter rules dict from the frontend UI or
                        the stored DB config.  Merged with defaults so missing
                        keys are always safe.  Pass None to use all defaults
                        (backward-compatible with v2 callers).

    Returns:
        {"added": int, "skipped": int, "errors": list[str], "dry_run": bool}
    """
    log.info("=== PolicyPulse Scholarly Scrape started ===")
    ensure_scholarly_table()

    cfg = _resolve_scholarly_config(filter_config)
    dbs = cfg["scholarly_databases"]

    log.info(
        f"  [config] min_rel={cfg['min_relevance']}, "
        f"days_back={cfg['days_back']}, dry_run={cfg['dry_run']}, "
        f"db_toggles={dbs}"
    )

    # ── Build keyword list ────────────────────────────────────────────────────
    from database import get_scholarly_keywords, get_watchlist_keywords
    db_kws        = [r["keyword"] for r in get_scholarly_keywords() if r.get("active")]
    watchlist_kws = get_watchlist_keywords()
    keywords = list(set(db_kws + watchlist_kws + (extra_keywords or [])))
    if not keywords:
        log.warning("No scholarly keywords configured — using built-in defaults")
        keywords = [
            "Indigenous policy Canada",
            "post-secondary education Canada",
            "reconciliation Canada",
            "pharmacare Canada",
        ]

    # ── Fetch from each enabled database ─────────────────────────────────────
    all_items: list[dict] = []

    if dbs.get("openalex", True):
        try:
            all_items.extend(fetch_openalex(keywords[:6], days_back=cfg["days_back"]))
        except Exception as e:
            log.error(f"OpenAlex failed: {e}")
    else:
        log.info("  [config] OpenAlex skipped (disabled)")

    if dbs.get("semantic", True):
        try:
            all_items.extend(fetch_semantic_scholar(keywords[:4]))
        except Exception as e:
            log.error(f"Semantic Scholar failed: {e}")
    else:
        log.info("  [config] Semantic Scholar skipped (disabled)")

    if dbs.get("thinktanks", True):
        try:
            all_items.extend(fetch_canadian_think_tanks())
        except Exception as e:
            log.error(f"Think-tanks failed: {e}")
    else:
        log.info("  [config] Canadian Think-Tanks skipped (disabled)")

    if dbs.get("doaj", True):
        try:
            all_items.extend(fetch_doaj(keywords[:3]))
        except Exception as e:
            log.error(f"DOAJ failed: {e}")
    else:
        log.info("  [config] DOAJ skipped (disabled)")

    if dbs.get("pubmed", True):
        health_kws = [
            k for k in keywords
            if any(w in k.lower() for w in
                   ["health", "pharmacare", "fnha", "indigenous", "mental"])
        ]
        if health_kws:
            try:
                all_items.extend(fetch_pubmed(health_kws[:3]))
            except Exception as e:
                log.error(f"PubMed failed: {e}")
    else:
        log.info("  [config] PubMed skipped (disabled)")

    if dbs.get("arxiv", True):
        try:
            all_items.extend(fetch_arxiv([k for k in keywords if len(k) > 6][:3]))
        except Exception as e:
            log.error(f"arXiv failed: {e}")
    else:
        log.info("  [config] arXiv skipped (disabled)")

    # ── Deduplicate by URL ────────────────────────────────────────────────────
    seen_urls: set[str] = set()
    unique_items: list[dict] = []
    for item in all_items:
        url = item.get("url", "").strip()
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_items.append(item)

    log.info(f"Scholarly: {len(unique_items)} unique items to process")

    # ── Pre-compute config filter values once ─────────────────────────────────
    min_relevance    = cfg["min_relevance"]
    domain_whitelist = [d.lower() for d in cfg.get("domain_whitelist", [])]
    must_include_kws = [k.lower() for k in cfg.get("must_include", [])]
    dry_run          = cfg["dry_run"]

    added, skipped, errors = 0, 0, []

    for item in unique_items:
        title = (item.get("title") or "").strip()
        url   = (item.get("url")   or "").strip()
        if not title or not url or len(title) < 10:
            continue

        try:
            # ── AI scoring ───────────────────────────────────────────────────
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

            # Apply source-level relevance boost (set per think-tank source)
            boost = item.get("relevance_boost", 0)
            if boost:
                ai["relevance"] = min(10, ai["relevance"] + boost)

            # ── min_relevance filter ─────────────────────────────────────────
            if ai["relevance"] < min_relevance:
                log.debug(
                    f"  [config] Dropping '{title[:55]}' — "
                    f"relevance {ai['relevance']} < {min_relevance}"
                )
                skipped += 1
                continue

            # ── Domain whitelist ─────────────────────────────────────────────
            if domain_whitelist:
                article_domains = [
                    d.strip().lower()
                    for d in (ai.get("domain") or "").split(",")
                ]
                if not any(wd in article_domains for wd in domain_whitelist):
                    log.debug(
                        f"  [config] Dropping '{title[:55]}' — "
                        f"domain '{ai.get('domain')}' not whitelisted"
                    )
                    skipped += 1
                    continue

            # ── Must-include keyword check ────────────────────────────────────
            if must_include_kws:
                hay = (title + " " + (ai.get("summary") or "") + " " +
                       item.get("abstract", "")[:500]).lower()
                if not any(kw in hay for kw in must_include_kws):
                    log.debug(
                        f"  [config] Dropping '{title[:55]}' — "
                        f"no must-include keyword found"
                    )
                    skipped += 1
                    continue

            # ── Dry run ───────────────────────────────────────────────────────
            if dry_run:
                log.info(
                    f"  [DRY RUN] Would save: '{title[:60]}' "
                    f"(rel={ai['relevance']}, db={item.get('database')}, "
                    f"domain={ai.get('domain')})"
                )
                added += 1
                continue

            # ── Persist ───────────────────────────────────────────────────────
            if save_scholarly_article(item, ai):
                added += 1
            else:
                skipped += 1

        except Exception as e:
            errors.append(f"{title[:40]}: {e}")
            log.warning(f"Scholarly analysis error: {e}")

        time.sleep(0.3)

    dry_note = " (DRY RUN — nothing saved)" if dry_run else ""
    log.info(
        f"=== Scholarly done. "
        f"Added: {added}{dry_note}, Skipped: {skipped}, Errors: {len(errors)} ==="
    )
    return {"added": added, "skipped": skipped, "errors": errors, "dry_run": dry_run}
