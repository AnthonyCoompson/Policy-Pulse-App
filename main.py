"""
PolicyPulse Backend — FastAPI + SQLite + Gemini AI
Deployed on Railway.app
"""

from fastapi import FastAPI, BackgroundTasks, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
import os
from datetime import datetime

from database import (init_db, get_articles, get_article_by_id, get_sources, get_stats,
                       get_digest_history, save_digest, update_article_read, update_article_staged,
                       get_watchlist_keywords, add_watchlist_keyword, remove_watchlist_keyword,
                       add_article_tag, remove_article_tag, update_article_sentiment)
from scraper import run_scrape
from scheduler import start_scheduler

app = FastAPI(title="PolicyPulse API", version="1.0.0")

# Allow your Netlify frontend (and local dev) to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Lock this down to your Netlify URL after testing
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup():
    init_db()
    start_scheduler()

# ── ARTICLES ──────────────────────────────────────────────

@app.get("/articles")
def list_articles(
    domain: str = Query(None),
    jurisdiction: str = Query(None),
    sentiment: str = Query(None),
    search: str = Query(None),
    sort: str = Query("date"),
    unread_only: bool = Query(False),
    limit: int = Query(50),
    offset: int = Query(0),
):
    articles = get_articles(
        domain=domain,
        jurisdiction=jurisdiction,
        sentiment=sentiment,
        search=search,
        sort=sort,
        unread_only=unread_only,
        limit=limit,
        offset=offset,
    )
    return {"articles": articles, "count": len(articles)}

@app.get("/articles/{article_id}")
def get_article(article_id: int):
    a = get_article_by_id(article_id)
    if not a:
        raise HTTPException(status_code=404, detail="Article not found")
    return a

@app.patch("/articles/{article_id}/read")
def mark_read(article_id: int, read: bool = True):
    update_article_read(article_id, read)
    return {"ok": True}

@app.patch("/articles/{article_id}/staged")
def mark_staged(article_id: int, staged: bool = True):
    update_article_staged(article_id, staged)
    return {"ok": True}

# ── SCRAPER ───────────────────────────────────────────────

@app.post("/scrape")
def trigger_scrape(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_scrape)
    return {"ok": True, "message": "Scrape started in background"}

@app.get("/scrape/status")
def scrape_status():
    from database import get_last_scrape_time
    last = get_last_scrape_time()
    return {"last_scraped": last}

# ── SOURCES ───────────────────────────────────────────────

@app.get("/sources")
def list_sources():
    return {"sources": get_sources()}

# ── STATS ─────────────────────────────────────────────────

@app.get("/stats")
def stats():
    return get_stats()

# ── DIGEST ────────────────────────────────────────────────

@app.get("/digests")
def list_digests():
    return {"digests": get_digest_history()}

@app.post("/digests")
def create_digest(body: dict):
    import secrets
    token = "pp-" + secrets.token_hex(4)
    digest_id = save_digest(
        subject=body.get("subject", "PolicyPulse Weekly"),
        html_content=body.get("html_content", ""),
        recipients=body.get("recipients", 0),
        token=token,
    )
    return {"ok": True, "id": digest_id, "token": token, "public_url": f"/digest/{token}"}

# ── WATCHLIST ─────────────────────────────────────────────

@app.get("/watchlist")
def list_watchlist():
    return {"keywords": get_watchlist_keywords()}

@app.post("/watchlist")
def add_keyword(body: dict):
    kw = body.get("keyword","").strip()
    if not kw:
        raise HTTPException(status_code=400, detail="keyword required")
    added = add_watchlist_keyword(kw)
    return {"ok": True, "added": added, "keyword": kw}

@app.delete("/watchlist/{keyword}")
def delete_keyword(keyword: str):
    remove_watchlist_keyword(keyword)
    return {"ok": True}

# ── SENTIMENT ─────────────────────────────────────────────

@app.patch("/articles/{article_id}/sentiment")
def update_sentiment(article_id: int, body: dict):
    from database import update_article_sentiment
    sentiment = body.get("sentiment","Neutral")
    update_article_sentiment(article_id, sentiment)
    return {"ok": True}

# ── MANUAL TAGS ────────────────────────────────────────────

@app.post("/articles/{article_id}/tags")
def add_tag(article_id: int, body: dict):
    tag = body.get("tag","").strip()
    if not tag:
        raise HTTPException(status_code=400, detail="tag required")
    add_article_tag(article_id, tag)
    return {"ok": True}

@app.delete("/articles/{article_id}/tags/{tag}")
def remove_tag(article_id: int, tag: str):
    remove_article_tag(article_id, tag)
    return {"ok": True}

# ── HEALTH ────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
