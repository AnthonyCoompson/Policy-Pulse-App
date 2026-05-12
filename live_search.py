"""
PolicyPulse Live Search Module v2
──────────────────────────────────
Two independent search pipelines:

  run_news_search(params)      → Google News RSS + AI analysis
  run_scholarly_search(params) → OpenAlex / Semantic Scholar / PubMed /
                                  DOAJ / arXiv + AI analysis

Both are called from FastAPI async endpoints. Results are returned
without being saved; the frontend decides what to save via
POST /live-search/save.
"""

import asyncio
import concurrent.futures
import logging
import time
from datetime import date, datetime, timedelta
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

TIMEOUT = 15

HTML_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
    ),
    "Accept-Language": "en-CA,en;q=0.9",
}
API_HEADERS = {
    "User-Agent": "PolicyPulse/1.0 (mailto:policy@policypulse.ca)",
    "Accept": "application/json",
}

# ── REGIONS ───────────────────────────────────────────────────────────────────
REGIONS: dict[str, dict] = {
    "Canada (National)":       {"gl": "CA", "hl": "en", "ceid": "CA:en", "term": "Canada"},
    "British Columbia":        {"gl": "CA", "hl": "en", "ceid": "CA:en", "term": "British Columbia"},
    "Alberta":                 {"gl": "CA", "hl": "en", "ceid": "CA:en", "term": "Alberta"},
    "Ontario":                 {"gl": "CA", "hl": "en", "ceid": "CA:en", "term": "Ontario"},
    "Quebec":                  {"gl": "CA", "hl": "fr", "ceid": "CA:fr", "term": "Québec"},
    "Manitoba":                {"gl": "CA", "hl": "en", "ceid": "CA:en", "term": "Manitoba"},
    "Saskatchewan":            {"gl": "CA", "hl": "en", "ceid": "CA:en", "term": "Saskatchewan"},
    "Nova Scotia":             {"gl": "CA", "hl": "en", "ceid": "CA:en", "term": "Nova Scotia"},
    "New Brunswick":           {"gl": "CA", "hl": "en", "ceid": "CA:en", "term": "New Brunswick"},
    "Newfoundland & Labrador": {"gl": "CA", "hl": "en", "ceid": "CA:en", "term": "Newfoundland Labrador"},
    "PEI":                     {"gl": "CA", "hl": "en", "ceid": "CA:en", "term": "Prince Edward Island"},
    "Northwest Territories":   {"gl": "CA", "hl": "en", "ceid": "CA:en", "term": "Northwest Territories"},
    "Yukon":                   {"gl": "CA", "hl": "en", "ceid": "CA:en", "term": "Yukon"},
    "Nunavut":                 {"gl": "CA", "hl": "en", "ceid": "CA:en", "term": "Nunavut"},
    "United States":           {"gl": "US", "hl": "en", "ceid": "US:en", "term": "United States"},
    "International":           {"gl": "US", "hl": "en", "ceid": "US:en", "term": ""},
}

# ── SOURCE TYPE SITE-RESTRICTIONS ─────────────────────────────────────────────
SOURCE_TYPE_FILTERS: dict[str, str] = {
    "All":          "",
    "Government":   "site:gov.bc.ca OR site:canada.ca OR site:leg.bc.ca OR site:gov.ab.ca OR site:ontario.ca OR site:gouv.qc.ca",
    "News Media":   "site:cbc.ca OR site:thestar.com OR site:globeandmail.com OR site:nationalpost.com OR site:macleans.ca",
    "Think Tanks":  "site:policyoptions.irpp.org OR site:policyalternatives.ca OR site:macdonald-laurier.ca OR site:cdhowe.org OR site:broadbentinstitute.ca",
    "Universities": "site:ubc.ca OR site:utoronto.ca OR site:sfu.ca OR site:uvic.ca OR site:mcgill.ca OR site:queens.ca",
}

# ── DOMAIN ENRICHMENT KEYWORDS ────────────────────────────────────────────────
DOMAIN_HINTS: dict[str, str] = {
    "Higher Education":  "university college post-secondary tuition",
    "Research Funding":  "research funding grant SSHRC NSERC CIHR",
    "Indigenous":        "Indigenous First Nations Métis Inuit",
    "Reconciliation":    "reconciliation DRIPA UNDRIP TRC",
    "Health":            "health policy healthcare",
    "Pharmacare":        "pharmacare drug coverage prescription",
    "Budget":            "budget fiscal spending",
    "Legislation":       "bill legislation act regulation",
    "Infrastructure":    "infrastructure capital construction",
    "Workforce":         "workforce labour employment",
    "Consultation":      "consultation engagement stakeholder",
}


# ─────────────────────────────────────────────────────────────────────────────
# DATE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _parse_date(raw: str) -> str | None:
    if not raw:
        return None
    raw = raw.strip()
    fmts = [
        "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d", "%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S GMT",
        "%B %d, %Y", "%b %d, %Y", "%d %B %Y",
    ]
    for f in fmts:
        try:
            return datetime.strptime(raw[:26], f).date().isoformat()
        except ValueError:
            continue
    return raw[:10] if len(raw) >= 10 and raw[4] == "-" and raw[7] == "-" else None


def _to_date(s: str | None) -> date | None:
    try:
        return date.fromisoformat(s[:10]) if s else None
    except ValueError:
        return None


def _in_range(pub: str, date_from: str | None, date_to: str | None) -> bool:
    d = _to_date(pub)
    if not d:
        return True
    if date_from and d < (_to_date(date_from) or d):
        return False
    if date_to and d > (_to_date(date_to) or d):
        return False
    return True


# ─────────────────────────────────────────────────────────────────────────────
# AI BATCH HELPER (thread-pool so it's safe inside FastAPI async context)
# ─────────────────────────────────────────────────────────────────────────────

async def _ai_batch(batch: list[dict]) -> list:
    from ai_processor import analyze_article

    def _run():
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            futs = [
                pool.submit(
                    analyze_article,
                    title=b["title"], url=b["url"],
                    source_name=b.get("source", ""),
                    article_text=b.get("article_text", ""),
                )
                for b in batch
            ]
            return [f.result(timeout=30) if not f.exception() else None for f in futs]

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _run)


# ─────────────────────────────────────────────────────────────────────────────
# NEWS SEARCH
# ─────────────────────────────────────────────────────────────────────────────

def _gnews_url(query: str, region_cfg: dict, date_from: str | None) -> str:
    term = region_cfg.get("term", "")
    full = f"{query} {term}".strip() if term else query
    q    = quote_plus(full)
    gl, hl, ceid = region_cfg["gl"], region_cfg["hl"], region_cfg["ceid"]
    when = ""
    d = _to_date(date_from)
    if d:
        days = (date.today() - d).days
        if 1 <= days <= 730:
            when = f"+when:{days}d"
    return f"https://news.google.com/rss/search?q={q}{when}&hl={hl}-{gl}&gl={gl}&ceid={ceid}"


def _fetch_gnews(url: str, df: str | None, dt: str | None, max_n: int) -> list[dict]:
    out = []
    try:
        r = requests.get(url, headers=HTML_HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        soup  = BeautifulSoup(r.content, "xml")
        for item in soup.find_all("item")[:max_n]:
            t_el  = item.find("title")
            l_el  = item.find("link")
            p_el  = item.find("pubDate")
            s_el  = item.find("source")
            if not t_el:
                continue
            title  = t_el.get_text(strip=True)
            link   = l_el.get_text(strip=True) if l_el else ""
            source = s_el.get_text(strip=True) if s_el else "Google News"
            pub    = _parse_date(p_el.get_text(strip=True) if p_el else "") or date.today().isoformat()
            if not title or not link:
                continue
            if not _in_range(pub, df, dt):
                continue
            out.append({
                "title": title, "url": link, "source": source,
                "pub_date": pub, "database": "Google News",
                "abstract": "", "authors": "", "doi": "",
                "open_access": False, "search_type": "news",
            })
    except Exception as e:
        log.warning(f"Google News fetch error: {e}")
    return out


async def run_news_search(params: dict) -> dict:
    """
    Live news search via Google News RSS with full filter support.

    Params:
        query        str   required
        region       str   key in REGIONS
        jurisdiction str   override AI jurisdiction tag
        domain       str   key in DOMAIN_HINTS
        date_from    str   YYYY-MM-DD
        date_to      str   YYYY-MM-DD
        source_type  str   key in SOURCE_TYPE_FILTERS
        min_relevance int  1-10 (default 5)
        max_results  int   cap per source (default 20)
    """
    t0 = time.time()
    query        = (params.get("query") or "").strip()
    region       = params.get("region",       "Canada (National)")
    jurisdiction = params.get("jurisdiction", "")
    domain       = params.get("domain",       "")
    date_from    = params.get("date_from")
    date_to      = params.get("date_to")
    source_type  = params.get("source_type",  "All")
    min_rel      = int(params.get("min_relevance", 5))
    max_n        = min(int(params.get("max_results", 20)), 40)

    if not query:
        return {"results": [], "total": 0, "sources_queried": [], "query_time_s": 0}

    rc = REGIONS.get(region, REGIONS["Canada (National)"])

    enriched = query
    if domain in DOMAIN_HINTS:
        enriched = f"{query} {DOMAIN_HINTS[domain]}"

    site = SOURCE_TYPE_FILTERS.get(source_type, "")
    if site:
        enriched = f"({enriched}) ({site})"

    url = _gnews_url(enriched, rc, date_from)
    raw = _fetch_gnews(url, date_from, date_to, max_n)

    # Dedup
    seen: set[str] = set()
    unique = [r for r in raw if r["url"] not in seen and not seen.add(r["url"])]  # type: ignore[func-returns-value]

    if not unique:
        return {"results": [], "total": 0, "sources_queried": ["Google News"],
                "query_time_s": round(time.time() - t0, 2)}

    ai_results = await _ai_batch([
        {"title": r["title"], "url": r["url"], "source": r["source"], "article_text": ""}
        for r in unique
    ])

    final = []
    for r, ai in zip(unique, ai_results):
        if not ai or (ai.get("relevance") or 0) < min_rel:
            continue
        if jurisdiction:
            ai["jurisdiction"] = jurisdiction
        final.append({**r, **ai})

    final.sort(key=lambda x: -(x.get("relevance") or 0))
    return {
        "results": final, "total": len(final), "raw_count": len(unique),
        "sources_queried": ["Google News"],
        "query_time_s": round(time.time() - t0, 2),
    }


# ─────────────────────────────────────────────────────────────────────────────
# SCHOLARLY SEARCH — per-database fetchers
# ─────────────────────────────────────────────────────────────────────────────

def _openalex(query: str, df: str | None, dt: str | None, max_n: int) -> list[dict]:
    out = []
    since = df or (date.today() - timedelta(days=365)).isoformat()
    try:
        r = requests.get(
            "https://api.openalex.org/works",
            params={
                "search":   query,
                "filter":   f"from_publication_date:{since},open_access.is_oa:true",
                "sort":     "publication_date:desc",
                "per_page": str(min(max_n, 25)),
                "select":   "id,title,abstract_inverted_index,publication_date,doi,"
                            "primary_location,authorships,concepts,open_access",
            },
            headers=API_HEADERS, timeout=TIMEOUT,
        )
        if r.status_code != 200:
            return out
        for w in r.json().get("results", []):
            title = (w.get("title") or "").strip()
            if not title or len(title) < 10:
                continue
            inv = w.get("abstract_inverted_index") or {}
            pos: dict[int, str] = {}
            for word, ps in inv.items():
                for p in ps:
                    pos[p] = word
            abstract = " ".join(pos[k] for k in sorted(pos))[:1500]
            oa  = w.get("open_access") or {}
            doi = w.get("doi", "")
            url = oa.get("oa_url") or (
                f"https://doi.org/{doi.replace('https://doi.org/','')}" if doi else ""
            )
            if not url:
                continue
            pub = (w.get("publication_date") or "")[:10]
            if not _in_range(pub, df, dt):
                continue
            src  = ((w.get("primary_location") or {}).get("source") or {}).get("display_name", "OpenAlex")
            tags = [c["display_name"] for c in (w.get("concepts") or [])[:4]]
            auth = ", ".join(
                a.get("author", {}).get("display_name", "")
                for a in (w.get("authorships") or [])[:3]
            )
            out.append({
                "title": title, "url": url, "source": src, "pub_date": pub,
                "database": "OpenAlex", "abstract": abstract, "authors": auth,
                "doi": doi, "tags": tags, "open_access": True, "search_type": "scholarly",
            })
    except Exception as e:
        log.warning(f"OpenAlex error: {e}")
    return out


def _semantic_scholar(query: str, df: str | None, max_n: int) -> list[dict]:
    out = []
    try:
        p: dict = {
            "query":  query, "limit": min(max_n, 10),
            "fields": "title,abstract,year,url,venue,authors,externalIds,openAccessPdf,publicationDate",
        }
        yr = (df or "")[:4]
        if yr.isdigit():
            p["year"] = f"{yr}-"
        r = requests.get(
            "https://api.semanticscholar.org/graph/v1/paper/search",
            params=p, headers=API_HEADERS, timeout=TIMEOUT,
        )
        if r.status_code != 200:
            return out
        for paper in r.json().get("data", []):
            title = (paper.get("title") or "").strip()
            if not title:
                continue
            pdf  = (paper.get("openAccessPdf") or {}).get("url", "")
            doi  = (paper.get("externalIds") or {}).get("DOI", "")
            url  = pdf or (f"https://doi.org/{doi}" if doi else paper.get("url", ""))
            if not url:
                continue
            yr2  = paper.get("year") or ""
            pub  = paper.get("publicationDate") or (f"{yr2}-01-01" if yr2 else "")
            auth = ", ".join(a.get("name", "") for a in (paper.get("authors") or [])[:3])
            out.append({
                "title": title, "url": url,
                "source": paper.get("venue") or "Semantic Scholar",
                "pub_date": pub[:10] if pub else "",
                "database": "Semantic Scholar",
                "abstract": (paper.get("abstract") or "")[:1200],
                "authors": auth, "doi": doi, "tags": [],
                "open_access": bool(pdf), "search_type": "scholarly",
            })
    except Exception as e:
        log.warning(f"Semantic Scholar error: {e}")
    return out


def _pubmed(query: str, df: str | None, max_n: int) -> list[dict]:
    out = []
    try:
        p: dict = {"db": "pmc", "term": query, "retmax": min(max_n, 12),
                   "sort": "pub date", "retmode": "json"}
        if df:
            p.update({"datetype": "pdat", "mindate": df.replace("-", "/"),
                      "maxdate": date.today().isoformat().replace("-", "/")})
        sr = requests.get(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
            params=p, headers=HTML_HEADERS, timeout=TIMEOUT,
        )
        if sr.status_code != 200:
            return out
        ids = sr.json().get("esearchresult", {}).get("idlist", [])
        if not ids:
            return out
        smr = requests.get(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi",
            params={"db": "pmc", "id": ",".join(ids[:max_n]), "retmode": "json"},
            headers=HTML_HEADERS, timeout=TIMEOUT,
        )
        if smr.status_code != 200:
            return out
        for pmid, art in smr.json().get("result", {}).items():
            if pmid == "uids":
                continue
            title = (art.get("title") or "").strip()
            pmcid = art.get("pmcid", "")
            url   = f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmcid}/" if pmcid else ""
            if not title or not url:
                continue
            out.append({
                "title": title, "url": url,
                "source": art.get("fulljournalname") or "PubMed Central",
                "pub_date": (art.get("pubdate") or "")[:10],
                "database": "PubMed Central",
                "abstract": "", "authors": "", "doi": "", "tags": [],
                "open_access": True, "search_type": "scholarly",
            })
    except Exception as e:
        log.warning(f"PubMed error: {e}")
    return out


def _doaj(query: str, max_n: int) -> list[dict]:
    out = []
    try:
        r = requests.get(
            f"https://doaj.org/api/search/articles/{quote_plus(query)}",
            params={"pageSize": min(max_n, 10), "sort": "published:desc"},
            headers=API_HEADERS, timeout=TIMEOUT,
        )
        if r.status_code != 200:
            return out
        for art in r.json().get("results", []):
            bib   = art.get("bibjson", {})
            title = (bib.get("title") or "").strip()
            if not title:
                continue
            links = bib.get("link") or []
            url   = next((l["url"] for l in links if l.get("type") == "fulltext"), "") or \
                    next((l.get("url", "") for l in links), "")
            if not url:
                continue
            yr  = bib.get("year", "")
            pub = (yr + "-01-01")[:10] if yr else ""
            out.append({
                "title": title, "url": url,
                "source": (bib.get("journal") or {}).get("title", "DOAJ"),
                "pub_date": pub, "database": "DOAJ",
                "abstract": (bib.get("abstract") or "")[:1200],
                "authors": "", "doi": "",
                "tags": bib.get("keywords", [])[:4],
                "open_access": True, "search_type": "scholarly",
            })
    except Exception as e:
        log.warning(f"DOAJ error: {e}")
    return out


def _arxiv(query: str, max_n: int) -> list[dict]:
    out = []
    try:
        r = requests.get(
            "https://export.arxiv.org/api/query",
            params={
                "search_query": f'ti:"{query}" OR abs:"{query}"',
                "max_results":  min(max_n, 10),
                "sortBy":       "submittedDate", "sortOrder": "descending",
            },
            headers=HTML_HEADERS, timeout=TIMEOUT,
        )
        if r.status_code != 200:
            return out
        soup = BeautifulSoup(r.content, "xml")
        for entry in soup.find_all("entry")[:max_n]:
            t_el = entry.find("title")
            s_el = entry.find("summary")
            i_el = entry.find("id")
            p_el = entry.find("published")
            if not t_el:
                continue
            title = t_el.get_text(strip=True).replace("\n", " ")
            url   = i_el.get_text(strip=True) if i_el else ""
            if title and url:
                out.append({
                    "title": title, "url": url, "source": "arXiv",
                    "pub_date": (p_el.get_text(strip=True) if p_el else "")[:10],
                    "database": "arXiv",
                    "abstract": (s_el.get_text(strip=True) if s_el else "")[:1200],
                    "authors": "", "doi": "", "tags": [],
                    "open_access": True, "search_type": "scholarly",
                })
    except Exception as e:
        log.warning(f"arXiv error: {e}")
    return out


async def run_scholarly_search(params: dict) -> dict:
    """
    Live scholarly search across user-selected open-access databases.

    Params:
        query         str   required
        databases     list  subset of DB_NAMES (empty = all)
        domain        str   key in DOMAIN_HINTS
        date_from     str   YYYY-MM-DD
        date_to       str   YYYY-MM-DD
        min_relevance int   1-10 (default 5)
        max_results   int   per-source cap (default 15)
    """
    t0 = time.time()
    query     = (params.get("query") or "").strip()
    databases = params.get("databases") or []
    domain    = params.get("domain", "")
    date_from = params.get("date_from")
    date_to   = params.get("date_to")
    min_rel   = int(params.get("min_relevance", 5))
    max_n     = min(int(params.get("max_results", 15)), 30)

    if not query:
        return {"results": [], "total": 0, "sources_queried": [], "query_time_s": 0}

    enriched = f"{query} {DOMAIN_HINTS[domain]}" if domain in DOMAIN_HINTS else query

    ALL_DBS = ["OpenAlex", "Semantic Scholar", "PubMed Central", "DOAJ", "arXiv"]
    active   = [d for d in ALL_DBS if not databases or d in databases]

    raw: list[dict] = []
    queried: list[str] = []

    DB_MAP = {
        "OpenAlex":         lambda: _openalex(enriched, date_from, date_to, max_n),
        "Semantic Scholar": lambda: _semantic_scholar(enriched, date_from, max_n),
        "PubMed Central":   lambda: _pubmed(enriched, date_from, max_n),
        "DOAJ":             lambda: _doaj(enriched, max_n),
        "arXiv":            lambda: _arxiv(enriched, max_n),
    }
    for db in active:
        if db in DB_MAP:
            r = DB_MAP[db]()
            if r:
                raw.extend(r)
                queried.append(db)

    # Dedup
    seen: set[str] = set()
    unique = [r for r in raw if r["url"] not in seen and not seen.add(r["url"])]  # type: ignore[func-returns-value]

    if not unique:
        return {"results": [], "total": 0, "sources_queried": queried,
                "query_time_s": round(time.time() - t0, 2)}

    ai_results = await _ai_batch([
        {"title": r["title"], "url": r["url"],
         "source": r.get("source", ""), "article_text": r.get("abstract", "")}
        for r in unique
    ])

    final = []
    for r, ai in zip(unique, ai_results):
        if not ai or (ai.get("relevance") or 0) < min_rel:
            continue
        final.append({**r, **ai})

    final.sort(key=lambda x: -(x.get("relevance") or 0))
    return {
        "results": final, "total": len(final), "raw_count": len(unique),
        "sources_queried": queried,
        "query_time_s": round(time.time() - t0, 2),
    }
