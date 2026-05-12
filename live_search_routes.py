"""
POLICYPULSE — LIVE SEARCH ROUTES
─────────────────────────────────
HOW TO APPLY — 2 steps:

STEP 1 — Add import at the top of main.py with your other imports:
    from live_search import run_news_search, run_scholarly_search, REGIONS, SOURCE_TYPE_FILTERS

STEP 2 — Paste the four route definitions below into main.py,
         anywhere before the `if __name__ == "__main__"` block.
"""

# ── PASTE THESE FOUR ROUTES INTO main.py ─────────────────────────────────────


@app.get("/live-search/config")
def live_search_config():
    """
    Return dropdown option lists for the frontend filter UI.
    Called once when the search panel is first opened.
    """
    from live_search import REGIONS, SOURCE_TYPE_FILTERS, DOMAIN_HINTS
    return {
        "regions":       list(REGIONS.keys()),
        "source_types":  list(SOURCE_TYPE_FILTERS.keys()),
        "domains":       [""] + list(DOMAIN_HINTS.keys()),
    }


@app.post("/live-search/news")
async def live_search_news(body: dict):
    """
    Live news search via Google News RSS.

    Body params (all optional except query):
        query        str   search terms
        region       str   region name (see /live-search/config)
        jurisdiction str   override AI jurisdiction tag
        domain       str   policy domain to enrich query
        date_from    str   YYYY-MM-DD
        date_to      str   YYYY-MM-DD
        source_type  str   "All" | "Government" | "News Media" | "Think Tanks" | "Universities"
        min_relevance int  1-10  (default 5)
        max_results  int   max items to fetch (default 20, cap 40)
    """
    from live_search import run_news_search
    query = (body.get("query") or "").strip()
    if not query:
        raise HTTPException(status_code=400, detail="query is required")
    return await run_news_search(body)


@app.post("/live-search/scholarly")
async def live_search_scholarly(body: dict):
    """
    Live scholarly search across OpenAlex / Semantic Scholar /
    PubMed Central / DOAJ / arXiv.

    Body params (all optional except query):
        query         str   search terms
        databases     list  e.g. ["OpenAlex","PubMed Central"]
                            omit or [] for all databases
        domain        str   policy domain to enrich query
        date_from     str   YYYY-MM-DD
        date_to       str   YYYY-MM-DD
        min_relevance int   1-10  (default 5)
        max_results   int   per-source cap (default 15, hard cap 30)
    """
    from live_search import run_scholarly_search
    query = (body.get("query") or "").strip()
    if not query:
        raise HTTPException(status_code=400, detail="query is required")
    return await run_scholarly_search(body)


@app.post("/live-search/save")
async def live_search_save(body: dict):
    """
    Save a single live-search result into the articles OR scholarly_articles table.
    The frontend sends the full result object returned by the search endpoint.

    The `search_type` field on the result ("news" vs "scholarly") determines
    which table receives it.
    """
    import hashlib
    import sqlite3
    from database import save_article, get_conn

    title = (body.get("title") or "").strip()
    url   = (body.get("url")   or "").strip()
    if not title or not url:
        raise HTTPException(status_code=400, detail="title and url are required")

    url_hash    = hashlib.sha256(url.encode()).hexdigest()
    search_type = body.get("search_type", "news")
    tags_str    = ",".join(body.get("tags") or [])
    now         = datetime.utcnow().isoformat()

    if search_type == "scholarly":
        conn = get_conn()
        try:
            conn.execute("""
                INSERT INTO scholarly_articles
                  (title, url, url_hash, source, database_name, jurisdiction, domain,
                   relevance, sentiment, summary, why_it_matters, abstract, authors,
                   doi, pub_date, processed_date, open_access, tags, search_keyword)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                title, url, url_hash,
                body.get("source", ""),
                body.get("database", "Live Search"),
                body.get("jurisdiction", ""),
                body.get("domain", ""),
                body.get("relevance") or 6,
                body.get("sentiment", "Neutral"),
                body.get("summary", title),
                body.get("why_it_matters", ""),
                (body.get("abstract") or "")[:2000],
                body.get("authors", ""),
                body.get("doi", ""),
                body.get("pub_date", now[:10]),
                now,
                1 if body.get("open_access") else 0,
                tags_str,
                "Live Search",
            ))
            conn.commit()
            inserted = True
        except sqlite3.IntegrityError:
            inserted = False
        finally:
            conn.close()
    else:
        inserted = save_article(
            title=title,
            url=url,
            url_hash=url_hash,
            source=body.get("source", "Live Search"),
            jurisdiction=body.get("jurisdiction", ""),
            domain=body.get("domain", ""),
            relevance=body.get("relevance") or 6,
            sentiment=body.get("sentiment", "Neutral"),
            summary=body.get("summary", title),
            why_it_matters=body.get("why_it_matters", ""),
            pub_date=body.get("pub_date", ""),
            tags=tags_str,
        )

    return {"ok": True, "inserted": bool(inserted), "duplicate": not bool(inserted)}
