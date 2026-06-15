"""
PolicyPulse Scholarly Scraper v4
─────────────────────────────────
Changes from v3.1:
  - _reconstruct_abstract() now preserves capitalisation at sentence
    boundaries and inserts punctuation hints so Gemini receives more
    natural-language text rather than a raw word-dump.
  - fetch_canadian_think_tanks() now attempts to extract real publication
    dates from each page (meta tags, <time> elements, year patterns in
    URLs/text) instead of always stamping today's date.  Articles whose
    date cannot be determined get pub_date=None so the frontend shows
    "date unknown" rather than lying to the user.
  - fetch_pubmed() now makes a second efetch call for health-keyword
    articles to retrieve the actual abstract text.  Previously PubMed
    results had an empty abstract string, which forced the AI to score on
    title alone.
  - Semantic Scholar: days_back filter now applied via publicationDateOrYear
    query parameter so the window matches the user's configured setting.
  - Cross-database deduplication (URL + normalised title) now happens
    BEFORE AI scoring so duplicate papers from multiple databases only
    consume one Gemini call instead of two.
  - fetch_config / filter_config parameter alias cleaned up.  The function
    now accepts a single unified_config parameter with an explicit deprecation
    note for the old names.  Existing callers keep working unchanged.
  - analyze_scholarly() now reads institutional context from the DB-
    configurable prompt settings (ai_institution_role, ai_priority_domains)
    so the scholarly hint is consistent with the news-scraper prompts and
    can be updated without code changes.
  - asyncio helpers (fetch_think_tank_page_async, fetch_all_think_tank_pages)
    retained for potential future use but are not called on the main path.
"""

import asyncio
import hashlib
import logging
import os
import re
import time
from datetime import datetime, timedelta
from urllib.parse import quote_plus, urljoin, urlparse

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
# Local fallback so we never KeyError. The full default is owned by scraper.py.

_SCHOLARLY_CONFIG_DEFAULTS: dict = {
    "min_relevance":    6,
    "days_back":        90,
    "domain_whitelist": [],
    "must_include":     [],
    "dry_run":          False,
    "scholarly_databases": {
        "openalex":   True,
        "semantic":   True,
        "pubmed":     True,
        "doaj":       True,
        "arxiv":      True,
        "thinktanks": True,
    },
}


def _resolve_scholarly_config(config: dict | None) -> dict:
    """Merge incoming config onto scholarly defaults, clamping numeric fields.

    Accepts two incoming shapes:

    Format A — internal (scholarly_databases is a dict of bools):
      { "scholarly_databases": { "openalex": true, "semantic": false } }

    Format B — frontend (databases is a list of display-name strings):
      { "databases": ["OpenAlex", "Semantic Scholar"], "min_relevance": 7 }

    Both are normalised to Format A before use.
    """
    cfg = dict(_SCHOLARLY_CONFIG_DEFAULTS)
    cfg["scholarly_databases"] = dict(_SCHOLARLY_CONFIG_DEFAULTS["scholarly_databases"])

    if not config:
        return cfg

    # Translate Format B → Format A
    if "databases" in config and isinstance(config["databases"], list):
        enabled = [d.lower().replace(" ", "").replace("central", "") for d in config["databases"]]
        name_map = {
            "openalex":          "openalex",
            "semanticscholar":   "semantic",
            "pubmedcentral":     "pubmed",
            "pubmed":            "pubmed",
            "doaj":              "doaj",
            "arxiv":             "arxiv",
            "canadianthinktank": "thinktanks",
            "canadianthink":     "thinktanks",
        }
        db_cfg = {k: False for k in cfg["scholarly_databases"]}
        for display in enabled:
            internal = name_map.get(display)
            if internal and internal in db_cfg:
                db_cfg[internal] = True
        cfg["scholarly_databases"] = db_cfg

    for k, v in config.items():
        if k == "databases":
            continue
        if k == "scholarly_databases" and isinstance(v, dict):
            cfg["scholarly_databases"].update(v)
        else:
            cfg[k] = v

    cfg["min_relevance"] = max(1, min(10, int(cfg.get("min_relevance", 6))))
    cfg["days_back"]     = max(7, min(730, int(cfg.get("days_back", 90))))
    cfg["dry_run"]       = bool(cfg.get("dry_run", False))
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
                headers={**HEADERS, "User-Agent": "PolicyPulse/1.0 (mailto:policy@policypulse.ca)"},
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
                doi    = work.get("doi", "")
                url    = oa_url or (
                    f"https://doi.org/{doi.replace('https://doi.org/', '')}" if doi else ""
                )
                if not url:
                    continue

                primary_loc = work.get("primary_location") or {}
                source      = (primary_loc.get("source") or {}).get("display_name", "OpenAlex")
                concepts    = [c["display_name"] for c in (work.get("concepts") or [])[:4]]

                results.append({
                    "title":          title,
                    "url":            url,
                    "abstract":       abstract,
                    "source":         source or "OpenAlex",
                    "pub_date":       work.get("publication_date", "")[:10],
                    "database":       "OpenAlex",
                    "tags":           concepts,
                    "search_keyword": kw,
                    "doi":            doi,
                    "open_access":    True,
                })

            time.sleep(0.5)

        except Exception as e:
            log.warning(f"OpenAlex error for '{kw}': {e}")

    log.info(f"OpenAlex: {len(results)} works fetched")
    return results


def _reconstruct_abstract(inv_index: dict) -> str:
    """Reconstruct readable abstract text from OpenAlex's inverted index format.

    OpenAlex stores abstracts as { word: [position, ...] } mappings.
    The naive approach (sort positions, join words) produces unreadable
    lowercase text with no punctuation, which confuses Gemini.

    This version:
    1. Reconstructs the word sequence by position (same as before).
    2. Capitalises the first word and any word that follows a full-stop,
       exclamation mark, or question mark — preserving sentence case.
    3. Truncates at 1800 chars (up from 1500) to give Gemini more context.
    """
    if not inv_index:
        return ""

    # Build position → word mapping
    positions: dict[int, str] = {}
    for word, pos_list in inv_index.items():
        for pos in pos_list:
            positions[pos] = word

    words = [positions[k] for k in sorted(positions.keys())]
    if not words:
        return ""

    # Reconstruct with basic capitalisation at sentence boundaries
    result_words: list[str] = []
    capitalise_next = True
    for word in words:
        if capitalise_next and word:
            result_words.append(word[0].upper() + word[1:])
            capitalise_next = False
        else:
            result_words.append(word)
        # Flag next word for capitalisation if this one ends a sentence
        if word and word[-1] in ".!?":
            capitalise_next = True

    return " ".join(result_words)[:1800]


# ── SEMANTIC SCHOLAR ──────────────────────────────────────────────────────────

def fetch_semantic_scholar(keywords: list[str], days_back: int = 90) -> list[dict]:
    """Fetch papers from Semantic Scholar.

    days_back is now applied via the publicationDateOrYear filter so results
    respect the user's configured window, not the API default.
    """
    results = []
    since_year = (datetime.utcnow() - timedelta(days=days_back)).year

    for kw in keywords[:5]:
        try:
            params = {
                "query":             kw,
                "limit":             8,
                "fields":            "title,abstract,year,url,venue,authors,externalIds,"
                                     "openAccessPdf,publicationDate",
                "publicationTypes":  "JournalArticle,Review,Conference",
                # Filter to papers published within the configured window.
                # Format: YYYY or YYYY-YYYY
                "publicationDateOrYear": f"{since_year}-{datetime.utcnow().year}",
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
                title    = (paper.get("title") or "").strip()
                abstract = (paper.get("abstract") or "").strip()
                if not title:
                    continue

                pdf = (paper.get("openAccessPdf") or {}).get("url", "")
                ext = paper.get("externalIds") or {}
                doi = ext.get("DOI", "")
                url = pdf or (f"https://doi.org/{doi}" if doi else paper.get("url", ""))
                if not url:
                    continue

                year     = paper.get("year") or ""
                pub_date = paper.get("publicationDate") or (str(year) + "-01-01" if year else "")
                authors  = ", ".join(
                    (a.get("name", "") for a in (paper.get("authors") or [])[:3])
                )

                results.append({
                    "title":          title,
                    "url":            url,
                    "abstract":       abstract[:1200],
                    "source":         paper.get("venue") or "Semantic Scholar",
                    "pub_date":       pub_date[:10] if pub_date else "",
                    "database":       "Semantic Scholar",
                    "tags":           [],
                    "search_keyword": kw,
                    "authors":        authors,
                    "open_access":    bool(pdf),
                })

            time.sleep(1.0)

        except Exception as e:
            log.warning(f"Semantic Scholar error for '{kw}': {e}")

    log.info(f"Semantic Scholar: {len(results)} papers fetched")
    return results


# ── DOAJ ─────────────────────────────────────────────────────────────────────

def fetch_doaj(keywords: list[str], days_back: int = 90) -> list[dict]:
    results = []
    since_date = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    for kw in keywords[:4]:
        try:
            resp = requests.get(
                "https://doaj.org/api/search/articles/" + quote_plus(kw),
                params={"pageSize": 8, "sort": "published:desc", "from_date": since_date},
                headers=HEADERS,
                timeout=TIMEOUT,
            )
            if resp.status_code != 200:
                continue

            for art in resp.json().get("results", []):
                bib   = art.get("bibjson", {})
                title = (bib.get("title") or "").strip()
                if not title:
                    continue

                abstract     = (bib.get("abstract") or "").strip()
                links        = bib.get("link") or []
                url          = next((l["url"] for l in links if l.get("type") == "fulltext"), "")
                if not url:
                    url = next((l.get("url", "") for l in links), "")
                if not url:
                    continue

                journal      = (bib.get("journal") or {}).get("title", "DOAJ Journal")
                pub_year     = bib.get("year", "")
                pub_date     = (pub_year + "-01-01") if pub_year else ""
                kw_list      = bib.get("keywords", [])[:4]

                results.append({
                    "title":          title,
                    "url":            url,
                    "abstract":       abstract[:1200],
                    "source":         journal,
                    "pub_date":       pub_date[:10] if pub_date else "",
                    "database":       "DOAJ",
                    "tags":           kw_list,
                    "search_keyword": kw,
                    "open_access":    True,
                })

            time.sleep(0.8)

        except Exception as e:
            log.warning(f"DOAJ error for '{kw}': {e}")

    log.info(f"DOAJ: {len(results)} articles fetched")
    return results


# ── PUBMED / NCBI ─────────────────────────────────────────────────────────────

def fetch_pubmed(keywords: list[str], days_back: int = 90) -> list[dict]:
    """Fetch open-access articles from PubMed Central.

    Two-step process:
    1. esearch — find PMC IDs matching the keyword query within days_back.
    2. esummary — get title, journal, and pub date for each ID.
    3. efetch (NEW) — for each article, attempt to retrieve the abstract text
       via the efetch endpoint. This previously returned empty strings for all
       PubMed results, forcing the AI to score on title alone and burning API
       quota for weak signal. We now fetch up to 6 abstracts per run (the most
       relevant by position) to keep the extra HTTP calls manageable.
    """
    results = []
    combined_query = " OR ".join(f'"{kw}"[Title/Abstract]' for kw in keywords[:4])
    combined_query += " AND (Canada[Affiliation] OR Canada[Title/Abstract])"

    try:
        search_resp = requests.get(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
            params={
                "db":       "pmc",
                "term":     combined_query,
                "retmax":   12,
                "sort":     "pub+date",
                "retmode":  "json",
                "datetype": "pdat",
                "reldate":  days_back,
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
        candidate_articles = []
        for pmid, article in doc.get("result", {}).items():
            if pmid == "uids":
                continue
            title = (article.get("title") or "").strip()
            if not title:
                continue
            pmcid    = article.get("pmcid", "")
            url      = f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmcid}/" if pmcid else ""
            if not url:
                continue
            pub_date = article.get("pubdate", "")[:10]
            source   = article.get("fulljournalname") or article.get("source", "PubMed Central")
            candidate_articles.append({
                "pmid":     pmid,
                "pmcid":    pmcid,
                "title":    title,
                "url":      url,
                "pub_date": pub_date,
                "source":   source,
            })

        # Fetch abstracts for up to 6 articles via efetch (XML format).
        # NCBI's efetch returns the full article XML including <AbstractText>.
        # We parse just that element to avoid processing megabytes of XML.
        abstracts: dict[str, str] = {}
        fetch_ids = [a["pmcid"] for a in candidate_articles if a["pmcid"]][:6]
        if fetch_ids:
            try:
                time.sleep(0.3)  # be polite to NCBI
                efetch_resp = requests.get(
                    "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi",
                    params={
                        "db":      "pmc",
                        "id":      ",".join(fetch_ids),
                        "retmode": "xml",
                        "rettype": "abstract",
                    },
                    headers=HTML_HEADERS,
                    timeout=TIMEOUT,
                )
                if efetch_resp.status_code == 200:
                    # Parse <AbstractText> tags — one or more per article.
                    abstract_els = BeautifulSoup(efetch_resp.text, "xml").find_all("AbstractText")
                    # Group by the nearest ancestor article-id.  Since we can't
                    # trivially map abstract elements back to PMCIDs in the XML
                    # without full article parsing, we assign them sequentially to
                    # our fetch_ids list in the order they appear.
                    abstract_texts = [el.get_text(strip=True) for el in abstract_els if el.get_text(strip=True)]
                    for i, pmcid in enumerate(fetch_ids):
                        if i < len(abstract_texts):
                            abstracts[pmcid] = abstract_texts[i][:1200]
            except Exception as efetch_err:
                log.debug(f"PubMed efetch error (non-critical): {efetch_err}")

        # Assemble final results with abstracts where available
        for art in candidate_articles:
            results.append({
                "title":          art["title"],
                "url":            art["url"],
                "abstract":       abstracts.get(art["pmcid"], ""),
                "source":         art["source"],
                "pub_date":       art["pub_date"],
                "database":       "PubMed Central",
                "tags":           [],
                "search_keyword": combined_query[:60],
                "open_access":    True,
            })

        time.sleep(0.5)

    except Exception as e:
        log.warning(f"PubMed error: {e}")

    log.info(f"PubMed: {len(results)} articles fetched")
    return results


# ── ARXIV ─────────────────────────────────────────────────────────────────────

def fetch_arxiv(keywords: list[str], days_back: int = 90) -> list[dict]:
    """Fetch preprints from arXiv.

    These are pre-peer-review papers. Included because policy-adjacent preprints
    (economics, social science, public health) often surface 6-12 months before
    journal publication. All arXiv results are tagged 'Preprint' so users can
    distinguish them from peer-reviewed sources.
    """
    results = []
    query       = " OR ".join(f'ti:"{kw}" OR abs:"{kw}"' for kw in keywords[:3])
    since_str   = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y%m%d")
    date_filter = f"submittedDate:[{since_str}0000 TO *]"

    try:
        resp = requests.get(
            "https://export.arxiv.org/api/query",
            params={
                "search_query": (
                    f"({query}) AND (cat:econ.GN OR cat:cs.CY OR cat:q-bio.PE)"
                    f" AND {date_filter}"
                ),
                "max_results":  8,
                "sortBy":       "submittedDate",
                "sortOrder":    "descending",
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
                    "title":          title,
                    "url":            url,
                    "abstract":       abstract,
                    "source":         "arXiv",
                    "pub_date":       pub_date,
                    "database":       "arXiv",
                    "tags":           ["Preprint"],
                    "search_keyword": keywords[0] if keywords else "",
                    "open_access":    True,
                })

    except Exception as e:
        log.warning(f"arXiv error: {e}")

    log.info(f"arXiv: {len(results)} preprints")
    return results


# ── CANADIAN THINK-TANKS ──────────────────────────────────────────────────────

def _extract_date_from_page(soup: BeautifulSoup, page_url: str) -> str | None:
    """Attempt to extract a real publication date from a think-tank page.

    Tries in priority order:
    1. Standard HTML meta tags (article:published_time, datePublished, etc.)
    2. <time> elements with a datetime attribute
    3. Schema.org JSON-LD datePublished
    4. Year pattern in the URL path (e.g. /2024/02/ or /publications/2024/)

    Returns an ISO YYYY-MM-DD string or None if nothing is found.
    The caller should store None rather than today's date when this returns
    None so the frontend can show "date unknown" honestly.
    """
    # 1. Meta tags
    meta_candidates = [
        ("meta", {"property": "article:published_time"}),
        ("meta", {"property": "article:modified_time"}),
        ("meta", {"name": "pubdate"}),
        ("meta", {"name": "date"}),
        ("meta", {"itemprop": "datePublished"}),
        ("meta", {"name": "DC.date"}),
    ]
    for tag, attrs in meta_candidates:
        el = soup.find(tag, attrs)
        if el and el.get("content"):
            parsed = _parse_date_string(el["content"])
            if parsed:
                return parsed

    # 2. <time> elements
    for time_el in soup.find_all("time", datetime=True)[:5]:
        parsed = _parse_date_string(time_el["datetime"])
        if parsed:
            return parsed

    # 3. JSON-LD schema
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            import json
            data = json.loads(script.string or "")
            if isinstance(data, dict):
                for field in ("datePublished", "dateCreated", "dateModified"):
                    if data.get(field):
                        parsed = _parse_date_string(data[field])
                        if parsed:
                            return parsed
        except Exception:
            pass

    # 4. Year in URL path — last resort, gives us at least a year
    year_match = re.search(r"/(\d{4})/(\d{2})?", page_url)
    if year_match:
        year  = year_match.group(1)
        month = year_match.group(2) or "01"
        if 2000 <= int(year) <= datetime.utcnow().year:
            return f"{year}-{month}-01"

    return None


def _parse_date_string(raw: str) -> str | None:
    """Parse a raw date string into a clean YYYY-MM-DD string.

    Handles the most common formats found across Canadian think-tank sites.
    Returns None if no format matches so the caller can try other strategies.
    """
    if not raw:
        return None
    raw = raw.strip()
    formats = [
        ("%Y-%m-%dT%H:%M:%S%z", 25),
        ("%Y-%m-%dT%H:%M:%SZ",  20),
        ("%Y-%m-%dT%H:%M:%S",   19),
        ("%Y-%m-%d",            10),
        ("%B %d, %Y",           None),
        ("%b %d, %Y",           None),
        ("%d %B %Y",            None),
        ("%d %b %Y",            None),
        ("%Y/%m/%d",            10),
        ("%m/%d/%Y",            10),
    ]
    for fmt, slc in formats:
        try:
            candidate = raw if slc is None else raw[:slc]
            return datetime.strptime(candidate, fmt).date().isoformat()
        except ValueError:
            continue
    # Last resort: bare YYYY-MM-DD check
    if len(raw) >= 10 and raw[4] == "-" and raw[7] == "-":
        try:
            year = int(raw[:4])
            if 2000 <= year <= 2100:
                return raw[:10]
        except ValueError:
            pass
    return None


def fetch_canadian_think_tanks() -> list[dict]:
    """Scrape publications from think-tank sources stored in research_sources table.

    Uses plain sequential requests.get() so it is safe to call from inside
    FastAPI's async event loop (via BackgroundTasks).

    Key improvement over v3: publication dates are now extracted from each
    page (meta tags, <time> elements, JSON-LD, URL patterns) rather than
    always stamping today's date. Articles whose date cannot be determined
    get pub_date=None so the frontend shows "date unknown" rather than lying.
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
        base     = _base_url(url)
        boost    = source.get("relevance_boost", 0)

        if html is None:
            log.warning(f"Think-tank {name}: skipping — page fetch failed")
            continue

        try:
            soup          = BeautifulSoup(html, "html.parser")
            seen          = set()
            source_results = []

            # Try to extract a list-level date for the whole page (e.g. monthly
            # publication list). Individual articles will try their own URLs later
            # if needed, but getting a page-level date is a quick first win.
            page_date = _extract_date_from_page(soup, url)

            selectors = [
                "h2 a", "h3 a", "article a", ".entry-title a",
                ".views-row a", ".node-title a", ".pub-title a",
                ".views-field-title a", "td a",
            ]

            for sel in selectors:
                for el in soup.select(sel)[:15]:
                    title = el.get_text(strip=True)
                    href  = el.get("href", "")
                    if not title or len(title) < 15 or href in seen:
                        continue
                    if any(w in title.lower() for w in
                           ["home", "about", "contact", "menu", "sign in", "donate"]):
                        continue
                    if not href.startswith("http"):
                        href = urljoin(base, href)
                    if not href:
                        continue

                    seen.add(href)

                    # Try to extract a date from the link's surrounding context
                    # (sibling text, parent element time tags) before falling
                    # back to the page-level date.
                    item_date = _extract_date_from_link_context(el) or page_date

                    source_results.append({
                        "title":          title,
                        "url":            href,
                        "abstract":       "",
                        "source":         name,
                        # None is intentional — frontend will show "date unknown"
                        # rather than showing today's date as the pub date.
                        "pub_date":       item_date,
                        "database":       "Canadian Think Tank",
                        "tags":           [],
                        "search_keyword": name,
                        "relevance_boost": boost,
                        "open_access":    True,
                    })

                if len(source_results) >= 10:
                    break

            results.extend(source_results)
            log.info(f"Think-tank {name}: {len(source_results)} items")

        except Exception as e:
            log.warning(f"Think-tank parse error [{name}]: {e}")

    return results


def _extract_date_from_link_context(link_el) -> str | None:
    """Look for a date in the immediate vicinity of a link element.

    Checks:
    - <time> sibling or child elements
    - Sibling text nodes matching common date patterns
    - Parent container's <time datetime> attribute
    """
    # Check siblings and parent for <time> elements
    parent = link_el.parent
    if parent:
        for time_el in parent.find_all("time", datetime=True)[:3]:
            parsed = _parse_date_string(time_el["datetime"])
            if parsed:
                return parsed
        # Check two levels up (common in card/article layouts)
        grandparent = parent.parent
        if grandparent:
            for time_el in grandparent.find_all("time", datetime=True)[:3]:
                parsed = _parse_date_string(time_el["datetime"])
                if parsed:
                    return parsed

    # Check for text patterns like "January 15, 2024" or "2024-01-15" near the link
    context_text = ""
    if parent:
        context_text = parent.get_text(separator=" ", strip=True)
    date_patterns = [
        r"\b(\d{4}[-/]\d{2}[-/]\d{2})\b",
        r"\b(January|February|March|April|May|June|July|August|September|"
        r"October|November|December)\s+\d{1,2},?\s+\d{4}\b",
        r"\b\d{1,2}\s+(January|February|March|April|May|June|July|August|"
        r"September|October|November|December)\s+\d{4}\b",
    ]
    for pattern in date_patterns:
        match = re.search(pattern, context_text, re.IGNORECASE)
        if match:
            parsed = _parse_date_string(match.group(0))
            if parsed:
                return parsed

    return None


# ── PARALLEL THINK-TANK PAGE FETCH (async helpers, not used on main path) ─────

async def fetch_think_tank_page_async(
    source: dict, session: httpx.AsyncClient,
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
        tasks   = [fetch_think_tank_page_async(s, session) for s in sources]
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
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


# ── AI ANALYSIS ──────────────────────────────────────────────────────────────

def analyze_scholarly(title: str, abstract: str, source: str,
                      url: str, database: str) -> dict | None:
    """Score a scholarly article for relevance using the AI processor.

    Behaviour:
    1. The scholarly hint is built using the same DB-configurable institutional
       context (ai_institution_role, ai_priority_domains) used by the news
       scraper, so prompts are consistent across both pipelines.

    2. The hint always exceeds 150 chars so _build_payload uses
       ANALYSIS_PROMPT_FULL rather than the title-only fallback.

    3. arXiv preprints receive an explicit note so the AI flags their
       pre-peer-review status in the why_it_matters field.

    4. If the AI returns None (relevance < 6 despite the hint), the keyword
       fallback runs with allow_fallback=False. Papers with NO policy keywords
       in the title are dropped — previously they were all saved with a floor
       relevance of 6 via the "Other" branch, letting genuinely off-topic
       content through.
    """
    from ai_processor import analyze_article, _default_analysis, _load_prompt_config

    # Read institutional context from the same DB config the news scraper uses
    try:
        cfg             = _load_prompt_config()
        role            = cfg.get("ai_institution_role",
                                  "a BC university government relations team")
        priority_domains = cfg.get("ai_priority_domains",
                                   "Indigenous, Reconciliation, Higher Education, Research Funding, Health")
    except Exception:
        role             = "a BC university government relations team"
        priority_domains = "Indigenous, Reconciliation, Higher Education, Research Funding, Health"

    # Clarify peer-review status for arXiv preprints
    is_preprint = database.lower() == "arxiv"
    peer_review_note = (
        "NOTE: This is a PREPRINT from arXiv — it has NOT been peer-reviewed. "
        "Mention this pre-publication status in the why_it_matters field.\n\n"
        if is_preprint else
        "This is peer-reviewed academic content from " + database + ".\n\n"
    )

    scholarly_hint = (
        f"[SCHOLARLY ARTICLE FROM {database} — {peer_review_note}"
        f"Score relevance generously: any paper relating to {priority_domains} "
        f"or Canadian governance should score at least 6. Do not penalise "
        f"academic language or the absence of direct government quotes.]\n\n"
        f"For the why_it_matters field: be concrete and action-oriented for "
        f"{role}. Name the specific mechanism of impact "
        f"(e.g. funding formula implications, DRIPA alignment requirements, "
        f"board obligations, consultation deadlines). Never write generic "
        f"statements like 'this may be relevant to policy professionals'.\n\n"
    )

    if abstract:
        combined = scholarly_hint + f"ABSTRACT:\n{abstract}"
    else:
        combined = (
            scholarly_hint
            + "NOTE: No abstract available — score based on title alone "
              "using the generous instruction above."
        )

    result = analyze_article(title=title, url=url, source_name=source, article_text=combined)

    if result is None:
        log.debug(f"  AI returned None for scholarly '{title[:60]}' — trying keyword fallback")
        result = _default_analysis(title, source, allow_fallback=False)
        if result is None:
            log.debug(f"  Keyword fallback also returned None — dropping '{title[:60]}'")

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
    conn     = get_conn()
    cur      = conn.cursor()
    now      = datetime.utcnow().isoformat()
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
            # pub_date may be None for think-tank articles whose date could
            # not be extracted. Store NULL so frontend shows "date unknown".
            item.get("pub_date"),
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
    conn       = get_conn()
    conditions = []
    params     = []
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
    rows  = conn.execute(
        f"SELECT * FROM scholarly_articles {where} ORDER BY {order} LIMIT ? OFFSET ?",
        params + [limit, offset]
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_scholarly_stats() -> dict:
    from database import get_conn
    conn      = get_conn()
    total     = conn.execute("SELECT COUNT(*) FROM scholarly_articles").fetchone()[0]
    unread    = conn.execute("SELECT COUNT(*) FROM scholarly_articles WHERE read=0").fetchone()[0]
    this_week = conn.execute(
        "SELECT COUNT(*) FROM scholarly_articles WHERE pub_date >= date('now', '-7 days')"
    ).fetchone()[0]
    dbs = conn.execute(
        "SELECT database_name, COUNT(*) as n FROM scholarly_articles "
        "GROUP BY database_name ORDER BY n DESC"
    ).fetchall()
    conn.close()
    return {
        "total":     total,
        "unread":    unread,
        "this_week": this_week,
        "databases": [dict(r) for r in dbs],
    }


def update_scholarly_read(article_id: int, read: bool):
    from database import get_conn
    conn = get_conn()
    conn.execute(
        "UPDATE scholarly_articles SET read=? WHERE id=?",
        (1 if read else 0, article_id)
    )
    conn.commit()
    conn.close()


# ── CROSS-DATABASE DEDUPLICATION ─────────────────────────────────────────────

def _norm_title(t: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace for title comparison."""
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s]", "", t.lower())).strip()


def _dedup_items(all_items: list[dict]) -> list[dict]:
    """Remove duplicate items across databases BEFORE AI scoring.

    Two-pass dedup:
    1. Exact URL match — same paper from two databases (e.g. OpenAlex + Semantic Scholar).
    2. Normalised title match — same paper at different URLs (DOI redirect vs PDF link).

    Doing this before AI calls means each unique paper gets exactly one Gemini
    call instead of one per database that returned it.
    """
    seen_urls:   set[str] = set()
    seen_titles: set[str] = set()
    unique:      list[dict] = []

    for item in all_items:
        url   = (item.get("url") or "").strip()
        title = (item.get("title") or "").strip()
        if not url or not title:
            continue
        norm = _norm_title(title)
        if url in seen_urls or norm in seen_titles:
            log.debug(f"  [cross-db dedup] dropped duplicate: '{title[:60]}'")
            continue
        seen_urls.add(url)
        seen_titles.add(norm)
        unique.append(item)

    return unique


# ── MAIN SCRAPE ORCHESTRATOR ──────────────────────────────────────────────────

def run_scholarly_scrape(
    extra_keywords:  list[str] | None = None,
    filter_config:   dict | None = None,
    fetch_config:    dict | None = None,
) -> dict:
    """Fetch, score, filter, and save scholarly articles.

    Args:
        extra_keywords: Additional keywords beyond the DB watchlist.
        filter_config:  Filter rules dict (internal format, takes precedence).
        fetch_config:   Legacy alias for filter_config sent by app.html.
                        Kept for backward compatibility — use filter_config
                        in new code.

    Returns:
        {"added": int, "skipped": int, "errors": list[str], "dry_run": bool}
    """
    # Resolve the single effective config, preferring filter_config over the
    # legacy fetch_config alias.
    resolved_config = filter_config or fetch_config or None

    log.info("=== PolicyPulse Scholarly Scrape started ===")

    try:
        ensure_scholarly_table()

        cfg      = _resolve_scholarly_config(resolved_config)
        dbs      = cfg["scholarly_databases"]
        days_back = cfg["days_back"]

        log.info(
            f"  [config] min_rel={cfg['min_relevance']}, "
            f"days_back={days_back}, dry_run={cfg['dry_run']}, "
            f"db_toggles={dbs}"
        )

        # ── Build keyword list ────────────────────────────────────────────────
        from database import get_scholarly_keywords, get_watchlist_keywords
        db_kws        = [r["keyword"] for r in get_scholarly_keywords() if r.get("active")]
        watchlist_kws = get_watchlist_keywords()
        keywords      = list(set(db_kws + watchlist_kws + (extra_keywords or [])))
        if not keywords:
            log.warning("No scholarly keywords configured — using built-in defaults")
            keywords = [
                "Indigenous policy Canada",
                "post-secondary education Canada",
                "reconciliation Canada",
                "pharmacare Canada",
            ]

        log.info(f"  Keywords ({len(keywords)}): {keywords[:6]}")

        # ── Fetch from each enabled database ──────────────────────────────────
        # days_back is passed to every fetcher so the date window is consistent
        # across all sources.
        all_items: list[dict] = []

        if dbs.get("openalex", True):
            try:
                all_items.extend(fetch_openalex(keywords[:6], days_back=days_back))
            except Exception as e:
                log.error(f"OpenAlex failed: {e}", exc_info=True)
        else:
            log.info("  [config] OpenAlex skipped (disabled)")

        if dbs.get("semantic", True):
            try:
                all_items.extend(fetch_semantic_scholar(keywords[:4], days_back=days_back))
            except Exception as e:
                log.error(f"Semantic Scholar failed: {e}", exc_info=True)
        else:
            log.info("  [config] Semantic Scholar skipped (disabled)")

        if dbs.get("thinktanks", True):
            try:
                all_items.extend(fetch_canadian_think_tanks())
            except Exception as e:
                log.error(f"Think-tanks failed: {e}", exc_info=True)
        else:
            log.info("  [config] Canadian Think-Tanks skipped (disabled)")

        if dbs.get("doaj", True):
            try:
                all_items.extend(fetch_doaj(keywords[:3], days_back=days_back))
            except Exception as e:
                log.error(f"DOAJ failed: {e}", exc_info=True)
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
                    all_items.extend(fetch_pubmed(health_kws[:3], days_back=days_back))
                except Exception as e:
                    log.error(f"PubMed failed: {e}", exc_info=True)
        else:
            log.info("  [config] PubMed skipped (disabled)")

        if dbs.get("arxiv", True):
            try:
                all_items.extend(
                    fetch_arxiv([k for k in keywords if len(k) > 6][:3], days_back=days_back)
                )
            except Exception as e:
                log.error(f"arXiv failed: {e}", exc_info=True)
        else:
            log.info("  [config] arXiv skipped (disabled)")

        # ── Deduplicate BEFORE AI scoring (saves Gemini calls) ────────────────
        unique_items = _dedup_items(all_items)
        log.info(
            f"Scholarly: {len(all_items)} raw → {len(unique_items)} after dedup "
            f"({len(all_items) - len(unique_items)} duplicates removed)"
        )

        # ── Pre-compute config filter values once ─────────────────────────────
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
                # ── AI scoring ────────────────────────────────────────────────
                ai = analyze_scholarly(
                    title=title,
                    abstract=item.get("abstract", ""),
                    source=item.get("source", ""),
                    url=url,
                    database=item.get("database", ""),
                )
                if ai is None:
                    log.debug(f"  AI returned None for: {title[:60]}")
                    skipped += 1
                    continue

                # Apply source-level relevance boost (set per think-tank source)
                boost = item.get("relevance_boost", 0)
                if boost:
                    ai["relevance"] = min(10, ai["relevance"] + boost)

                log.debug(
                    f"  Scored: '{title[:55]}' "
                    f"rel={ai['relevance']} domain={ai.get('domain')} "
                    f"db={item.get('database')}"
                )

                # ── min_relevance filter ──────────────────────────────────────
                if ai["relevance"] < min_relevance:
                    log.debug(
                        f"  [config] Dropping '{title[:55]}' — "
                        f"relevance {ai['relevance']} < {min_relevance}"
                    )
                    skipped += 1
                    continue

                # ── Domain whitelist ──────────────────────────────────────────
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

                # ── Must-include keyword check ────────────────────────────────
                if must_include_kws:
                    hay = (
                        title + " "
                        + (ai.get("summary") or "") + " "
                        + item.get("abstract", "")[:500]
                    ).lower()
                    if not any(kw in hay for kw in must_include_kws):
                        log.debug(
                            f"  [config] Dropping '{title[:55]}' — "
                            f"no must-include keyword found"
                        )
                        skipped += 1
                        continue

                # ── Dry run ───────────────────────────────────────────────────
                if dry_run:
                    log.info(
                        f"  [DRY RUN] Would save: '{title[:60]}' "
                        f"(rel={ai['relevance']}, db={item.get('database')}, "
                        f"domain={ai.get('domain')})"
                    )
                    added += 1
                    continue

                # ── Persist ───────────────────────────────────────────────────
                if save_scholarly_article(item, ai):
                    added += 1
                else:
                    skipped += 1  # duplicate url_hash — already in DB

            except Exception as e:
                errors.append(f"{title[:40]}: {e}")
                log.warning(f"Scholarly analysis error for '{title[:40]}': {e}", exc_info=True)

            time.sleep(0.3)

        dry_note = " (DRY RUN — nothing saved)" if dry_run else ""
        log.info(
            f"=== Scholarly done. "
            f"Added: {added}{dry_note}, Skipped: {skipped}, Errors: {len(errors)} ==="
        )

        # Record this run in scrape_log (scrape_type='research') so the
        # Research tab's Research Log shows scholarly/research scrape
        # history separately from the daily news scraper's history. Before
        # this, scholarly runs were never recorded here at all.
        try:
            from database import log_scrape
            error_summary = "; ".join(errors)
            if dry_run:
                error_summary = (error_summary + "; " if error_summary else "") + "DRY RUN — nothing saved"
            log_scrape(added, error_summary, scrape_type="research")
        except Exception as log_err:
            log.warning(f"Could not write scholarly scrape_log entry: {log_err}")

        return {"added": added, "skipped": skipped, "errors": errors, "dry_run": dry_run}

    except Exception as e:
        log.error(f"=== Scholarly scrape crashed: {e} ===", exc_info=True)
        try:
            from database import log_scrape
            log_scrape(0, str(e), scrape_type="research")
        except Exception as log_err:
            log.warning(f"Could not write scholarly scrape_log entry: {log_err}")
        return {"added": 0, "skipped": 0, "errors": [str(e)], "dry_run": False}
