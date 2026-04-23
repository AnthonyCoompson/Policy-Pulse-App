"""
PolicyPulse Backend — FastAPI + SQLite + Gemini AI
Deployed on Railway.app

v2: Added PATCH /articles/{id} for frontend to save regenerated summaries.
    Added POST /digests/send for real email delivery via SMTP.
"""

from fastapi import FastAPI, BackgroundTasks, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
import os
from datetime import datetime

from database import (
    init_db, get_articles, get_article_by_id, get_sources, get_stats,
    get_digest_history, save_digest, update_article_read, update_article_staged,
    get_watchlist_keywords, add_watchlist_keyword, remove_watchlist_keyword,
    add_article_tag, remove_article_tag, update_article_sentiment,
    update_article_content,
)
from scraper import run_scrape
from scheduler import start_scheduler

app = FastAPI(title="PolicyPulse API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Lock to your Netlify URL after testing
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup():
    init_db()
    start_scheduler()


# ── ARTICLES ──────────────────────────────────────────────────────────────────

@app.get("/articles")
def list_articles(
    domain:      str  = Query(None),
    jurisdiction:str  = Query(None),
    sentiment:   str  = Query(None),
    search:      str  = Query(None),
    sort:        str  = Query("date"),
    unread_only: bool = Query(False),
    limit:       int  = Query(50),
    offset:      int  = Query(0),
):
    articles = get_articles(
        domain=domain, jurisdiction=jurisdiction, sentiment=sentiment,
        search=search, sort=sort, unread_only=unread_only,
        limit=limit, offset=offset,
    )
    return {"articles": articles, "count": len(articles)}


@app.get("/articles/{article_id}")
def get_article(article_id: int):
    a = get_article_by_id(article_id)
    if not a:
        raise HTTPException(status_code=404, detail="Article not found")
    return a


@app.patch("/articles/{article_id}/read")
def mark_read(article_id: int, body: dict = {}):
    read = body.get("read", True)
    update_article_read(article_id, read)
    return {"ok": True}


@app.patch("/articles/{article_id}/staged")
def mark_staged(article_id: int, body: dict = {}):
    staged = body.get("staged", True)
    update_article_staged(article_id, staged)
    return {"ok": True}


@app.patch("/articles/{article_id}")
def update_article(article_id: int, body: dict):
    """
    Update summary and/or why_it_matters on an existing article.
    Called by the frontend's 'Generate' button after AI regenerates these fields.
    Only the fields present in the request body are updated.
    """
    allowed = {"summary", "why_it_matters"}
    updates = {k: v for k, v in body.items() if k in allowed and isinstance(v, str)}
    if not updates:
        raise HTTPException(status_code=400,
                            detail=f"Nothing to update. Allowed fields: {allowed}")
    update_article_content(article_id, updates)
    return {"ok": True, "updated": list(updates.keys())}


# ── SCRAPER ───────────────────────────────────────────────────────────────────

@app.post("/scrape")
def trigger_scrape(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_scrape)
    return {"ok": True, "message": "Scrape started in background"}


@app.get("/scrape/status")
def scrape_status():
    from database import get_last_scrape_time
    return {"last_scraped": get_last_scrape_time()}


# ── SOURCES ───────────────────────────────────────────────────────────────────

@app.get("/sources")
def list_sources():
    return {"sources": get_sources()}


# ── STATS ─────────────────────────────────────────────────────────────────────

@app.get("/stats")
def stats():
    return get_stats()


# ── DIGEST ────────────────────────────────────────────────────────────────────

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
    return {"ok": True, "id": digest_id, "token": token,
            "public_url": f"/digest/{token}"}


@app.post("/digests/send")
def send_digest_email(body: dict):
    """
    Send the digest to all active subscribers via SMTP.

    Requires these Railway environment variables:
        SMTP_HOST     — e.g. smtp.gmail.com
        SMTP_PORT     — e.g. 587
        SMTP_USER     — your sending email address
        SMTP_PASSWORD — your email password or app password
        SMTP_FROM     — display name + address, e.g. "PolicyPulse <you@gmail.com>"

    Gmail setup: use an App Password (Google Account → Security → App passwords).
    """
    import smtplib
    import secrets
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    smtp_host = os.environ.get("SMTP_HOST", "")
    smtp_port = int(os.environ.get("SMTP_PORT", 587))
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASSWORD", "")
    smtp_from = os.environ.get("SMTP_FROM", smtp_user)

    if not all([smtp_host, smtp_user, smtp_pass]):
        raise HTTPException(
            status_code=503,
            detail=(
                "Email not configured. Add SMTP_HOST, SMTP_USER, and SMTP_PASSWORD "
                "to your Railway environment variables. See DEPLOYMENT.md for setup."
            )
        )

    subject      = body.get("subject", "PolicyPulse Weekly Digest")
    html_content = body.get("html_content", "")
    recipients   = body.get("recipients", [])  # list of {"name": ..., "email": ...}

    if not recipients:
        raise HTTPException(status_code=400, detail="No recipients provided.")
    if not html_content:
        raise HTTPException(status_code=400, detail="No digest content provided.")

    sent_count = 0
    errors = []

    try:
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.ehlo()
        server.starttls()
        server.login(smtp_user, smtp_pass)

        for r in recipients:
            try:
                msg = MIMEMultipart("alternative")
                msg["Subject"] = subject
                msg["From"]    = smtp_from
                msg["To"]      = r["email"]

                # Plain text fallback
                plain = html_content.replace("<br>", "\n").replace("</p>", "\n\n")
                from re import sub as re_sub
                plain = re_sub(r"<[^>]+>", "", plain)

                msg.attach(MIMEText(plain, "plain"))
                msg.attach(MIMEText(html_content, "html"))

                server.sendmail(smtp_user, r["email"], msg.as_string())
                sent_count += 1
            except Exception as e:
                errors.append(f"{r.get('email')}: {e}")

        server.quit()
    except smtplib.SMTPException as e:
        raise HTTPException(status_code=500,
                            detail=f"SMTP connection failed: {e}")

    # Save to digest history
    token = "pp-" + secrets.token_hex(4)
    save_digest(subject=subject, html_content=html_content,
                recipients=sent_count, token=token)

    return {
        "ok": True,
        "sent": sent_count,
        "errors": errors,
        "token": token,
    }


# ── WATCHLIST ─────────────────────────────────────────────────────────────────

@app.get("/watchlist")
def list_watchlist():
    return {"keywords": get_watchlist_keywords()}


@app.post("/watchlist")
def add_keyword(body: dict):
    kw = body.get("keyword", "").strip()
    if not kw:
        raise HTTPException(status_code=400, detail="keyword required")
    added = add_watchlist_keyword(kw)
    return {"ok": True, "added": added, "keyword": kw}


@app.delete("/watchlist/{keyword}")
def delete_keyword(keyword: str):
    remove_watchlist_keyword(keyword)
    return {"ok": True}


# ── SENTIMENT + TAGS ──────────────────────────────────────────────────────────

@app.patch("/articles/{article_id}/sentiment")
def update_sentiment(article_id: int, body: dict):
    update_article_sentiment(article_id, body.get("sentiment", "Neutral"))
    return {"ok": True}


@app.post("/articles/{article_id}/tags")
def add_tag(article_id: int, body: dict):
    tag = body.get("tag", "").strip()
    if not tag:
        raise HTTPException(status_code=400, detail="tag required")
    add_article_tag(article_id, tag)
    return {"ok": True}


@app.delete("/articles/{article_id}/tags/{tag}")
def remove_tag(article_id: int, tag: str):
    remove_article_tag(article_id, tag)
    return {"ok": True}


# ── HEALTH ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
