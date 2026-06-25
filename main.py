"""
PolicyPulse Backend — FastAPI + SQLite + Gemini AI
v6: Added POST /ai/chat — proxies Groq Llama calls so no browser-side key is needed.
    Add GROQ_API_KEY to your Render environment variables (free at console.groq.com).
"""

from fastapi import FastAPI, BackgroundTasks, Query, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import uvicorn
import os
from datetime import datetime

from database import (
    init_db, get_articles, get_article_by_id, get_sources, get_stats,
    get_digest_history, save_digest, update_article_read, update_article_staged,
    get_watchlist_keywords, add_watchlist_keyword, remove_watchlist_keyword,
    add_article_tag, remove_article_tag, update_article_sentiment,
    update_article_content,
    add_source, toggle_source, delete_source, update_source,
    get_research_sources, add_research_source, toggle_research_source,
    delete_research_source, update_research_source,
    get_scholarly_keywords, add_scholarly_keyword,
    delete_scholarly_keyword, toggle_scholarly_keyword,
    get_subscribers, add_subscriber, toggle_subscriber,
    delete_subscriber, update_subscriber,
    get_alert_subscribers, update_subscriber_alerts,
    get_scholarly_article_by_id,
    get_scraper_config, set_scraper_config, get_all_scraper_config,
    get_articles_missing_pub_date, update_article_pub_date,
    get_scholarly_articles_missing_pub_date, update_scholarly_pub_date,
    get_app_setting, set_app_setting, get_all_app_settings,
    get_trackers, get_tracker_by_id, create_tracker, update_tracker, delete_tracker,
    get_tracker_articles, add_tracker_article, remove_tracker_article,
    get_tracker_events, add_tracker_event, delete_tracker_event,
)
from scraper import run_scrape
from scheduler import start_scheduler

from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    start_scheduler()
    yield

app = FastAPI(title="PolicyPulse API", version="6.0.0", lifespan=lifespan)

# ── CORS ──────────────────────────────────────────────────────────────────────
_raw_origins = os.environ.get("ALLOWED_ORIGINS", "")
ALLOWED_ORIGINS = (
    [o.strip() for o in _raw_origins.split(",") if o.strip()]
    if _raw_origins else ["*"]
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── API KEY AUTH ──────────────────────────────────────────────────────────────
_API_KEY = os.environ.get("PP_API_KEY", "")

def verify_api_key(request: Request) -> bool:
    if not _API_KEY:
        return True
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer ") and auth[7:] == _API_KEY:
        return True
    return False

def require_auth(request: Request):
    if not verify_api_key(request):
        raise HTTPException(
            status_code=401,
            detail="Unauthorised. Set PP_API_KEY in Render and pass it as: Authorization: Bearer <key>"
        )


# ── AI CHAT PROXY ─────────────────────────────────────────────────────────────

@app.post("/ai/chat")
async def ai_chat(body: dict):
    """
    Proxy Groq Llama chat completions through the backend so no
    browser-side API key is ever needed.

    Body:    { "messages": [...], "max_tokens": 1000 }
    Returns: { "ok": true, "content": "..." }

    Requires GROQ_API_KEY in Render environment variables.
    Get a free key at https://console.groq.com
    """
    import httpx

    groq_key = os.environ.get("GROQ_API_KEY", "")
    if not groq_key:
        raise HTTPException(
            status_code=503,
            detail="GROQ_API_KEY not set in Render environment variables. "
                   "Get a free key at https://console.groq.com and add it to Render."
        )

    messages   = body.get("messages", [])
    max_tokens = int(body.get("max_tokens", 1000))

    if not messages:
        raise HTTPException(status_code=400, detail="messages required")

    payload = {
        "model":       "llama-3.3-70b-versatile",
        "messages":    messages,
        "max_tokens":  max_tokens,
        "temperature": 0.2,
    }

    try:
        async with httpx.AsyncClient(timeout=45) as client:
            resp = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                json=payload,
                headers={
                    "Authorization": f"Bearer {groq_key}",
                    "Content-Type":  "application/json",
                },
            )
            if resp.status_code == 429:
                raise HTTPException(
                    status_code=429,
                    detail="Groq rate limit — please wait a moment and try again."
                )
            resp.raise_for_status()
            data    = resp.json()
            content = data["choices"][0]["message"]["content"]
            return {"ok": True, "content": content}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"AI proxy error: {str(e)[:200]}"
        )


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
def mark_read(article_id: int, body: dict = None, _=Depends(require_auth)):
    body = body or {}
    update_article_read(article_id, body.get("read", True))
    return {"ok": True}


@app.patch("/articles/{article_id}/staged")
def mark_staged(article_id: int, body: dict = None, _=Depends(require_auth)):
    body = body or {}
    update_article_staged(article_id, body.get("staged", True))
    return {"ok": True}


@app.patch("/articles/{article_id}")
def update_article(article_id: int, body: dict, _=Depends(require_auth)):
    allowed = {"summary", "why_it_matters"}
    updates = {k: v for k, v in body.items() if k in allowed and isinstance(v, str)}
    if not updates:
        raise HTTPException(status_code=400, detail=f"Allowed fields: {allowed}")
    update_article_content(article_id, updates)
    return {"ok": True, "updated": list(updates.keys())}


@app.patch("/articles/{article_id}/sentiment")
def update_sentiment(article_id: int, body: dict, _=Depends(require_auth)):
    update_article_sentiment(article_id, body.get("sentiment", "Neutral"))
    return {"ok": True}


@app.patch("/articles/{article_id}/relevance")
def update_article_relevance_endpoint(article_id: int, body: dict, _=Depends(require_auth)):
    from database import update_article_relevance
    score = int(body.get("relevance", 6))
    update_article_relevance(article_id, score)
    return {"ok": True}


@app.post("/articles/{article_id}/tags")
def add_tag(article_id: int, body: dict, _=Depends(require_auth)):
    tag = body.get("tag", "").strip()
    if not tag:
        raise HTTPException(status_code=400, detail="tag required")
    add_article_tag(article_id, tag)
    return {"ok": True}


@app.delete("/articles/{article_id}/tags/{tag}")
def remove_tag(article_id: int, tag: str, _=Depends(require_auth)):
    remove_article_tag(article_id, tag)
    return {"ok": True}


@app.delete("/articles/{article_id}")
def delete_article(article_id: int, _=Depends(require_auth)):
    from database import get_conn
    conn = get_conn()
    conn.execute("DELETE FROM articles WHERE id = ?", (article_id,))
    conn.commit()
    conn.close()
    return {"ok": True}


# ── ARTICLE READER ────────────────────────────────────────────────────────────

@app.get("/articles/{article_id}/reader")
def get_article_reader(article_id: int):
    import re
    import requests as req_lib
    from urllib.parse import urlparse, urljoin
    from bs4 import BeautifulSoup

    a = get_article_by_id(article_id)
    if not a:
        raise HTTPException(status_code=404, detail="Article not found")

    article_url = a.get("url", "")
    if not article_url:
        return {"ok": False, "reason": "No URL on this article"}

    parsed_url = urlparse(article_url)
    base_url   = f"{parsed_url.scheme}://{parsed_url.netloc}"
    favicon    = f"{base_url}/favicon.ico"

    FETCH_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-CA,en;q=0.9",
    }

    try:
        resp = req_lib.get(article_url, headers=FETCH_HEADERS, timeout=12, allow_redirects=True)
        resp.raise_for_status()
        raw_html = resp.text
    except req_lib.exceptions.Timeout:
        return {"ok": False, "reason": "Request timed out"}
    except req_lib.exceptions.HTTPError as e:
        return {"ok": False, "reason": f"HTTP {e.response.status_code} from source"}
    except Exception as e:
        return {"ok": False, "reason": f"Could not fetch article: {str(e)[:120]}"}

    soup = BeautifulSoup(raw_html, "html.parser")

    content_node = None
    for selector in ["article","main","[role='main']",".article-body",".article-content",
                     ".entry-content",".post-content",".story-body",".content-body",
                     "#content",".page-content",".main-content"]:
        node = soup.select_one(selector)
        if node and len(node.get_text(strip=True)) > 200:
            content_node = node
            break
    if content_node is None:
        content_node = soup.find("body") or soup

    for tag_name in ["script","style","iframe","form","input","button","nav","footer",
                     "header","aside","noscript","svg","canvas","video","audio"]:
        for el in content_node.find_all(tag_name):
            el.decompose()

    NOISE = ["sidebar","widget","cookie","newsletter","social-share","related",
             "comment","advertisement","ad-","promo","breadcrumb","pagination",
             "share-","print-","skip-"]
    for el in content_node.find_all(class_=True):
        if any(p in " ".join(el.get("class",[])).lower() for p in NOISE):
            el.decompose()

    for el in content_node.find_all(True):
        if el.has_attr("style"):   del el["style"]
        if el.has_attr("class"):   del el["class"]
        on_attrs = [a for a in list(el.attrs.keys()) if a.startswith("on")]
        for attr in on_attrs:
            del el[attr]

    for img in content_node.find_all("img"):
        src = img.get("src","")
        if src and not src.startswith(("http://","https://","data:")):
            img["src"] = urljoin(base_url, src)

    for a_tag in content_node.find_all("a", href=True):
        href = a_tag["href"]
        if href and not href.startswith(("http://","https://","#","mailto:")):
            a_tag["href"] = urljoin(base_url, href)
        a_tag["target"] = "_blank"
        a_tag["rel"]    = "noopener noreferrer"

    page_title = ""
    og = soup.find("meta", property="og:title")
    if og and og.get("content"): page_title = og["content"].strip()
    if not page_title:
        h1 = content_node.find("h1")
        if h1: page_title = h1.get_text(strip=True)
    if not page_title and soup.title:
        page_title = soup.title.get_text(strip=True)

    clean_html = str(content_node)
    clean_html = re.sub(r"<script[^>]*>.*?</script>","",clean_html,flags=re.DOTALL|re.IGNORECASE)
    if len(clean_html) > 400_000:
        clean_html = clean_html[:400_000] + "<!-- [content truncated] -->"

    return {
        "ok": True, "html": clean_html,
        "title": page_title or a.get("title",""),
        "pub_date": a.get("pub_date") or a.get("pubDate",""),
        "source": a.get("source",""), "url": article_url, "favicon": favicon,
        "domain": a.get("domain",""), "jurisdiction": a.get("jurisdiction",""),
        "relevance": a.get("relevance",0), "sentiment": a.get("sentiment",""),
        "summary": a.get("summary",""),
        "why_it_matters": a.get("why_it_matters") or a.get("whyItMatters",""),
    }


# ── SCRAPER ───────────────────────────────────────────────────────────────────

@app.post("/scrape")
def trigger_scrape(background_tasks: BackgroundTasks, body: dict = None, _=Depends(require_auth)):
    body = body or {}
    filter_config = body.get("filter_config", {})
    if filter_config:
        import json
        set_scraper_config("news_filter_config", json.dumps(filter_config))
    background_tasks.add_task(_scrape_and_alert)
    return {"ok": True, "message": "Scrape started in background"}


def _scrape_and_alert():
    result = run_scrape()
    new_articles = result.get("new_articles", [])
    if not new_articles:
        return
    _dispatch_urgent_alerts(new_articles)
    _dispatch_keyword_alerts(new_articles)


@app.get("/scrape/status")
def scrape_status():
    from database import get_last_scrape_time
    return {"last_scraped": get_last_scrape_time()}


# ── SOURCES ───────────────────────────────────────────────────────────────────

@app.get("/sources")
def list_sources():
    return {"sources": get_sources()}


@app.post("/sources")
def create_source(body: dict, _=Depends(require_auth)):
    name         = (body.get("name") or "").strip()
    url          = (body.get("url")  or "").strip()
    jurisdiction = (body.get("jurisdiction") or "Federal").strip()
    scrape_type  = (body.get("scrape_type")  or "html").strip()
    if not name or not url:
        raise HTTPException(status_code=400, detail="name and url are required")
    if scrape_type not in ("rss", "html"):
        raise HTTPException(status_code=400, detail="scrape_type must be 'rss' or 'html'")
    new_id = add_source(name, url, jurisdiction, scrape_type)
    return {"ok": True, "id": new_id}


@app.patch("/sources/{source_id}/toggle")
def toggle_source_endpoint(source_id: int, _=Depends(require_auth)):
    new_state = toggle_source(source_id)
    return {"ok": True, "active": new_state}


@app.patch("/sources/{source_id}")
def edit_source(source_id: int, body: dict, _=Depends(require_auth)):
    allowed = {"name", "url", "jurisdiction", "scrape_type", "active"}
    updates = {k: v for k, v in body.items() if k in allowed}
    if not updates:
        raise HTTPException(status_code=400, detail=f"Allowed fields: {allowed}")
    update_source(source_id, updates)
    return {"ok": True}


@app.delete("/sources/{source_id}")
def remove_source(source_id: int, _=Depends(require_auth)):
    delete_source(source_id)
    return {"ok": True}


# ── RESEARCH SOURCES ──────────────────────────────────────────────────────────

@app.get("/research-sources")
def list_research_sources():
    return {"sources": get_research_sources()}


@app.post("/research-sources")
def create_research_source(body: dict, _=Depends(require_auth)):
    name        = (body.get("name") or "").strip()
    url         = (body.get("url")  or "").strip()
    source_type = (body.get("source_type") or "think_tank").strip()
    boost       = int(body.get("relevance_boost", 0))
    notes       = (body.get("notes") or "").strip()
    if not name or not url:
        raise HTTPException(status_code=400, detail="name and url are required")
    new_id = add_research_source(name, url, source_type, boost, notes)
    return {"ok": True, "id": new_id}


@app.patch("/research-sources/{source_id}/toggle")
def toggle_research_source_endpoint(source_id: int, _=Depends(require_auth)):
    new_state = toggle_research_source(source_id)
    return {"ok": True, "active": new_state}


@app.patch("/research-sources/{source_id}")
def edit_research_source(source_id: int, body: dict, _=Depends(require_auth)):
    allowed = {"name", "url", "source_type", "active", "relevance_boost", "notes"}
    updates = {k: v for k, v in body.items() if k in allowed}
    if not updates:
        raise HTTPException(status_code=400, detail=f"Allowed fields: {allowed}")
    update_research_source(source_id, updates)
    return {"ok": True}


@app.delete("/research-sources/{source_id}")
def remove_research_source(source_id: int, _=Depends(require_auth)):
    delete_research_source(source_id)
    return {"ok": True}


# ── SCHOLARLY KEYWORDS ────────────────────────────────────────────────────────

@app.get("/scholarly-keywords")
def list_scholarly_keywords():
    return {"keywords": get_scholarly_keywords()}


@app.post("/scholarly-keywords")
def create_scholarly_keyword(body: dict, _=Depends(require_auth)):
    kw = (body.get("keyword") or "").strip()
    if not kw:
        raise HTTPException(status_code=400, detail="keyword required")
    added = add_scholarly_keyword(kw)
    return {"ok": True, "added": added}


@app.patch("/scholarly-keywords/{keyword_id}/toggle")
def toggle_scholarly_keyword_endpoint(keyword_id: int, _=Depends(require_auth)):
    new_state = toggle_scholarly_keyword(keyword_id)
    return {"ok": True, "active": new_state}


@app.delete("/scholarly-keywords/{keyword_id}")
def remove_scholarly_keyword(keyword_id: int, _=Depends(require_auth)):
    delete_scholarly_keyword(keyword_id)
    return {"ok": True}


# ── WATCHLIST ─────────────────────────────────────────────────────────────────

@app.get("/watchlist")
def list_watchlist():
    return {"keywords": get_watchlist_keywords()}


@app.post("/watchlist")
def add_keyword(body: dict, _=Depends(require_auth)):
    kw = body.get("keyword", "").strip()
    if not kw:
        raise HTTPException(status_code=400, detail="keyword required")
    added = add_watchlist_keyword(kw)
    return {"ok": True, "added": added, "keyword": kw}


@app.delete("/watchlist/{keyword}")
def delete_keyword(keyword: str, _=Depends(require_auth)):
    remove_watchlist_keyword(keyword)
    return {"ok": True}


# ── EXCLUSION KEYWORDS ────────────────────────────────────────────────────────

@app.get("/exclusion-keywords")
def list_exclusion_keywords():
    from database import get_all_exclusion_keywords
    return {"keywords": get_all_exclusion_keywords()}


@app.post("/exclusion-keywords")
def create_exclusion_keyword(body: dict, _=Depends(require_auth)):
    from database import add_exclusion_keyword
    kw = (body.get("keyword") or "").strip()
    if not kw:
        raise HTTPException(status_code=400, detail="keyword required")
    added = add_exclusion_keyword(kw)
    return {"ok": True, "added": added}


@app.delete("/exclusion-keywords/{keyword_id}")
def remove_exclusion_keyword_endpoint(keyword_id: int, _=Depends(require_auth)):
    from database import delete_exclusion_keyword_by_id
    delete_exclusion_keyword_by_id(keyword_id)
    return {"ok": True}


# ── SCHOLARLY EXCLUSION ───────────────────────────────────────────────────────

@app.get("/scholarly-exclusion-keywords")
def list_scholarly_exclusion_keywords():
    from database import get_scholarly_exclusion_keywords, get_conn
    conn = get_conn()
    rows = conn.execute("SELECT id, keyword FROM scholarly_exclusion_keywords ORDER BY id").fetchall()
    conn.close()
    return {"keywords": [dict(r) for r in rows]}


@app.post("/scholarly-exclusion-keywords")
def create_scholarly_exclusion_keyword(body: dict, _=Depends(require_auth)):
    from database import add_scholarly_exclusion_keyword
    kw = (body.get("keyword") or "").strip()
    if not kw:
        raise HTTPException(status_code=400, detail="keyword required")
    added = add_scholarly_exclusion_keyword(kw)
    return {"ok": True, "added": added}


@app.delete("/scholarly-exclusion-keywords/{keyword_id}")
def remove_scholarly_exclusion_keyword_endpoint(keyword_id: int, _=Depends(require_auth)):
    from database import get_conn
    conn = get_conn()
    conn.execute("DELETE FROM scholarly_exclusion_keywords WHERE id = ?", (keyword_id,))
    conn.commit()
    conn.close()
    return {"ok": True}


# ── SUBSCRIBERS ───────────────────────────────────────────────────────────────

@app.get("/subscribers")
def list_subscribers():
    return {"subscribers": get_subscribers()}


@app.post("/subscribers")
def create_subscriber(body: dict, _=Depends(require_auth)):
    import sqlite3
    name  = (body.get("name")  or "").strip()
    email = (body.get("email") or "").strip().lower()
    role  = (body.get("role")  or "Reader").strip()
    if not name or not email:
        raise HTTPException(status_code=400, detail="name and email are required")
    if "@" not in email:
        raise HTTPException(status_code=400, detail="invalid email address")
    try:
        new_id = add_subscriber(name, email, role)
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=409, detail="Email already exists")
    return {"ok": True, "id": new_id}


@app.patch("/subscribers/{subscriber_id}/toggle")
def toggle_subscriber_endpoint(subscriber_id: int, _=Depends(require_auth)):
    new_state = toggle_subscriber(subscriber_id)
    return {"ok": True, "active": new_state}


@app.patch("/subscribers/{subscriber_id}")
def edit_subscriber(subscriber_id: int, body: dict, _=Depends(require_auth)):
    allowed = {"name", "role", "active"}
    updates = {k: v for k, v in body.items() if k in allowed}
    if not updates:
        raise HTTPException(status_code=400, detail=f"Allowed fields: {allowed}")
    update_subscriber(subscriber_id, updates)
    return {"ok": True}


@app.delete("/subscribers/{subscriber_id}")
def remove_subscriber_endpoint(subscriber_id: int, _=Depends(require_auth)):
    delete_subscriber(subscriber_id)
    return {"ok": True}


# ── STATS ─────────────────────────────────────────────────────────────────────

@app.get("/stats")
def stats():
    return get_stats()


# ── DIGEST ────────────────────────────────────────────────────────────────────

@app.get("/digests")
def list_digests():
    return {"digests": get_digest_history()}


@app.post("/digests")
def create_digest(body: dict, _=Depends(require_auth)):
    import secrets
    token = "pp-" + secrets.token_hex(4)
    digest_id = save_digest(
        subject=body.get("subject", "PolicyPulse Weekly"),
        html_content=body.get("html_content", ""),
        recipients=body.get("recipients", 0),
        token=token,
    )
    return {"ok": True, "id": digest_id, "token": token, "public_url": f"/digest/{token}"}


@app.post("/digests/send")
def send_digest_email(body: dict, _=Depends(require_auth)):
    import smtplib, secrets
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from re import sub as re_sub

    smtp_host = os.environ.get("SMTP_HOST","")
    smtp_port = int(os.environ.get("SMTP_PORT",587))
    smtp_user = os.environ.get("SMTP_USER","")
    smtp_pass = os.environ.get("SMTP_PASSWORD","")
    smtp_from = os.environ.get("SMTP_FROM", smtp_user)

    # Surface a clear diagnostic rather than a generic 503
    missing = []
    if not smtp_host: missing.append("SMTP_HOST")
    if not smtp_user: missing.append("SMTP_USER")
    if not smtp_pass: missing.append("SMTP_PASSWORD")
    if missing:
        raise HTTPException(
            status_code=503,
            detail=f"SMTP not configured — missing Render env vars: {', '.join(missing)}. "
                   f"Go to Render → Environment and add these variables. "
                   f"For Gmail use smtp.gmail.com port 587 with an App Password "
                   f"(Google Account → Security → App passwords)."
        )

    subject      = body.get("subject","PolicyPulse Weekly Digest")
    html_content = body.get("html_content","")
    recipients   = body.get("recipients",[])
    if not recipients: raise HTTPException(status_code=400, detail="No recipients provided.")
    if not html_content: raise HTTPException(status_code=400, detail="No content provided.")

    sent_count, errors = 0, []
    try:
        server = smtplib.SMTP(smtp_host, smtp_port, timeout=30)
        server.ehlo()
        server.starttls()
        server.login(smtp_user, smtp_pass)
        for r in recipients:
            try:
                msg = MIMEMultipart("alternative")
                msg["Subject"] = subject
                msg["From"]    = smtp_from
                msg["To"]      = r["email"]
                plain = re_sub(r"<[^>]+>","", html_content.replace("<br>","\n").replace("</p>","\n\n"))
                msg.attach(MIMEText(plain,"plain"))
                msg.attach(MIMEText(html_content,"html"))
                server.sendmail(smtp_user, r["email"], msg.as_string())
                sent_count += 1
            except Exception as e:
                errors.append(f"{r.get('email')}: {e}")
        server.quit()
    except smtplib.SMTPAuthenticationError as e:
        raise HTTPException(
            status_code=500,
            detail=f"SMTP authentication failed — check SMTP_USER and SMTP_PASSWORD. "
                   f"For Gmail, use an App Password not your account password. Error: {e}"
        )
    except smtplib.SMTPConnectError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Could not connect to {smtp_host}:{smtp_port} — check SMTP_HOST and SMTP_PORT. Error: {e}"
        )
    except smtplib.SMTPException as e:
        raise HTTPException(status_code=500, detail=f"SMTP error: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error sending digest: {e}")

    token = "pp-" + secrets.token_hex(4)
    save_digest(subject=subject, html_content=html_content, recipients=sent_count, token=token)
    return {"ok": True, "sent": sent_count, "errors": errors, "token": token}


# ── NOTION PROXY ─────────────────────────────────────────────────────────────

@app.get("/smtp/diagnose")
def smtp_diagnose(_=Depends(require_auth)):
    """Test SMTP configuration and return a detailed diagnostic."""
    import smtplib
    smtp_host = os.environ.get("SMTP_HOST","")
    smtp_port = int(os.environ.get("SMTP_PORT", 587))
    smtp_user = os.environ.get("SMTP_USER","")
    smtp_pass = os.environ.get("SMTP_PASSWORD","")
    smtp_from = os.environ.get("SMTP_FROM", smtp_user)

    status = {
        "SMTP_HOST":     smtp_host or "❌ NOT SET",
        "SMTP_PORT":     smtp_port,
        "SMTP_USER":     smtp_user or "❌ NOT SET",
        "SMTP_PASSWORD": "✓ set" if smtp_pass else "❌ NOT SET",
        "SMTP_FROM":     smtp_from or "❌ NOT SET",
    }
    missing = [k for k, v in status.items() if "NOT SET" in str(v)]
    if missing:
        return {"ok": False, "status": status, "error": f"Missing: {', '.join(missing)}",
                "advice": "Add these to Render → Environment. For Gmail use smtp.gmail.com:587 with an App Password."}
    try:
        server = smtplib.SMTP(smtp_host, smtp_port, timeout=15)
        server.ehlo()
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.quit()
        return {"ok": True, "status": status, "message": "SMTP connection and login successful ✓"}
    except smtplib.SMTPAuthenticationError as e:
        return {"ok": False, "status": status, "error": f"Auth failed: {e}",
                "advice": "For Gmail: use an App Password (Google Account → Security → App passwords), not your regular password."}
    except smtplib.SMTPConnectError as e:
        return {"ok": False, "status": status, "error": f"Connect failed: {e}",
                "advice": f"Cannot reach {smtp_host}:{smtp_port}. Check SMTP_HOST and SMTP_PORT."}
    except Exception as e:
        return {"ok": False, "status": status, "error": str(e),
                "advice": "Check all SMTP_* variables in Render environment settings."}
async def notion_test(body: dict):
    import requests as req_lib
    token = body.get("token","").strip()
    if not token: raise HTTPException(status_code=400, detail="token required")
    try:
        r = req_lib.get("https://api.notion.com/v1/users/me",
            headers={"Authorization":f"Bearer {token}","Notion-Version":"2022-06-28"}, timeout=10)
        if r.status_code == 200:
            data = r.json()
            workspace = data.get("bot",{}).get("workspace_name","") or data.get("name","Notion")
            return {"ok": True, "workspace": workspace}
        raise HTTPException(status_code=r.status_code, detail=r.json().get("message","Invalid token"))
    except HTTPException: raise
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))


@app.post("/notion/push")
async def notion_push(body: dict):
    import requests as req_lib
    token    = body.get("token","").strip()
    endpoint = body.get("endpoint","").strip().lstrip("/")
    payload  = body.get("payload",{})
    if not token: raise HTTPException(status_code=400, detail="token required")
    if not endpoint: raise HTTPException(status_code=400, detail="endpoint required")
    allowed_prefixes = ("pages","databases/","blocks/","search")
    if not any(endpoint.startswith(p) for p in allowed_prefixes):
        raise HTTPException(status_code=400, detail=f"endpoint not allowed: {endpoint}")
    try:
        if "properties" in payload:
            payload["properties"] = {k:v for k,v in payload["properties"].items() if v is not None}
        r = req_lib.post(f"https://api.notion.com/v1/{endpoint}",
            headers={"Authorization":f"Bearer {token}","Notion-Version":"2022-06-28","Content-Type":"application/json"},
            json=payload, timeout=15)
        if r.status_code in (200,201): return {"ok":True, **r.json()}
        raise HTTPException(status_code=r.status_code, detail=r.json().get("message",r.text[:200]))
    except HTTPException: raise
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))


# ── SCHOLARLY ─────────────────────────────────────────────────────────────────

@app.get("/scholarly")
def list_scholarly(
    domain:        str = Query(None),
    database_name: str = Query(None),
    search:        str = Query(None),
    sort:          str = Query("date"),
    limit:         int = Query(50),
    offset:        int = Query(0),
):
    try:
        from scholarly_scraper import get_scholarly_articles, ensure_scholarly_table
        ensure_scholarly_table()
        articles = get_scholarly_articles(domain=domain, database_name=database_name,
                                          search=search, sort=sort, limit=limit, offset=offset)
        return {"articles": articles, "count": len(articles)}
    except Exception as e:
        return {"articles": [], "count": 0, "note": str(e)}


@app.get("/scholarly/stats")
def scholarly_stats():
    try:
        from scholarly_scraper import get_scholarly_stats, ensure_scholarly_table
        ensure_scholarly_table()
        return get_scholarly_stats()
    except Exception as e:
        return {"total": 0, "unread": 0, "databases": [], "note": str(e)}


@app.post("/scholarly/scrape")
def trigger_scholarly_scrape(background_tasks: BackgroundTasks, body: dict = None, _=Depends(require_auth)):
    body = body or {}
    extra_keywords = body.get("keywords", [])
    fetch_config   = body.get("fetch_config", {})
    if fetch_config:
        import json
        set_scraper_config("research_fetch_config", json.dumps(fetch_config))
    background_tasks.add_task(_run_scholarly_bg, extra_keywords, fetch_config)
    return {"ok": True, "message": "Scholarly scrape started in background"}


def _run_scholarly_bg(extra_keywords: list, fetch_config: dict = None):
    from scholarly_scraper import run_scholarly_scrape
    try:
        run_scholarly_scrape(extra_keywords=extra_keywords, fetch_config=fetch_config)
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"Scholarly background task crashed: {e}", exc_info=True)


@app.patch("/scholarly/{article_id}/read")
def mark_scholarly_read(article_id: int, body: dict = None, _=Depends(require_auth)):
    body = body or {}
    from scholarly_scraper import update_scholarly_read
    update_scholarly_read(article_id, body.get("read", True))
    return {"ok": True}


@app.patch("/scholarly/{article_id}/relevance")
def update_scholarly_relevance_endpoint(article_id: int, body: dict, _=Depends(require_auth)):
    from database import update_scholarly_relevance
    score = int(body.get("relevance", 6))
    update_scholarly_relevance(article_id, score)
    return {"ok": True}


@app.get("/scholarly/{article_id}/for-note")
def get_scholarly_for_note(article_id: int):
    article = get_scholarly_article_by_id(article_id)
    if not article:
        raise HTTPException(status_code=404, detail="Research paper not found")
    return article


@app.get("/scholarly/{article_id}")
def get_scholarly_article(article_id: int):
    article = get_scholarly_article_by_id(article_id)
    if not article:
        raise HTTPException(status_code=404, detail="Article not found")
    return article


@app.post("/scholarly/{article_id}/tags")
def add_scholarly_tag(article_id: int, body: dict, _=Depends(require_auth)):
    tag = body.get("tag", "").strip()
    if not tag:
        raise HTTPException(status_code=400, detail="tag required")
    from database import get_conn
    conn = get_conn()
    row = conn.execute("SELECT tags FROM scholarly_articles WHERE id = ?", (article_id,)).fetchone()
    if row:
        existing = [t.strip() for t in (row["tags"] or "").split(",") if t.strip()]
        if tag not in existing:
            existing.append(tag)
        conn.execute("UPDATE scholarly_articles SET tags = ? WHERE id = ?", (",".join(existing), article_id))
        conn.commit()
    conn.close()
    return {"ok": True}


@app.delete("/scholarly/{article_id}/tags/{tag}")
def remove_scholarly_tag(article_id: int, tag: str, _=Depends(require_auth)):
    from database import get_conn
    conn = get_conn()
    row = conn.execute("SELECT tags FROM scholarly_articles WHERE id = ?", (article_id,)).fetchone()
    if row:
        existing = [t.strip() for t in (row["tags"] or "").split(",") if t.strip() and t.strip() != tag]
        conn.execute("UPDATE scholarly_articles SET tags = ? WHERE id = ?", (",".join(existing), article_id))
        conn.commit()
    conn.close()
    return {"ok": True}


# ── HEALTH ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


# ── SCRAPER CONFIG ────────────────────────────────────────────────────────────

@app.get("/scraper-config")
def get_scraper_config_endpoint():
    return {"config": get_all_scraper_config()}


@app.post("/scraper-config")
def set_scraper_config_endpoint(body: dict, _=Depends(require_auth)):
    import json
    for key, value in body.items():
        if isinstance(value, (dict, list)):
            value = json.dumps(value)
        set_scraper_config(str(key), str(value))
    return {"ok": True, "saved": list(body.keys())}


@app.delete("/scraper-config/{key}")
def delete_scraper_config_endpoint(key: str, _=Depends(require_auth)):
    from database import get_conn
    conn = get_conn()
    conn.execute("DELETE FROM scraper_config WHERE key = ?", (key,))
    conn.commit()
    conn.close()
    return {"ok": True}


# ── DATABASE BACKUP ───────────────────────────────────────────────────────────

@app.get("/backup/download")
def download_backup(_=Depends(require_auth)):
    import io
    import sqlite3 as _sqlite3
    from fastapi.responses import StreamingResponse

    db_path = os.environ.get("DB_PATH", "policypulse.db")
    if not os.path.exists(db_path):
        raise HTTPException(status_code=404, detail="Database file not found")

    buf = io.BytesIO()
    try:
        src2 = _sqlite3.connect(db_path)
        try:
            data = src2.serialize()
            buf.write(data)
        except AttributeError:
            src2.execute("PRAGMA wal_checkpoint(FULL)")
            src2.close()
            with open(db_path, "rb") as f:
                buf.write(f.read())
        else:
            src2.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Backup failed: {e}")

    buf.seek(0)
    filename = f"policypulse-backup-{datetime.utcnow().strftime('%Y-%m-%d')}.db"
    return StreamingResponse(
        buf,
        media_type="application/octet-stream",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@app.get("/backup/stats")
def backup_stats(_=Depends(require_auth)):
    from database import get_conn
    db_path = os.environ.get("DB_PATH", "policypulse.db")
    size_bytes = os.path.getsize(db_path) if os.path.exists(db_path) else 0
    conn = get_conn()
    tables = {}
    for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall():
        t = row["name"]
        try:
            n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            tables[t] = n
        except Exception:
            tables[t] = "error"
    conn.close()
    return {
        "db_size_bytes": size_bytes,
        "db_size_mb": round(size_bytes / 1_048_576, 2),
        "tables": tables,
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.get("/digest/{token}")
def view_public_digest(token: str):
    from fastapi.responses import HTMLResponse
    from database import get_conn
    conn = get_conn()
    row = conn.execute(
        "SELECT subject, html_content, sent_date FROM digests WHERE token = ?", (token,)
    ).fetchone()
    conn.close()
    if not row or not row["html_content"]:
        raise HTTPException(status_code=404, detail="Digest not found or has expired.")
    banner = (
        '<div style="background:#003366;color:#fff;text-align:center;padding:8px 16px;'
        'font-family:Georgia,serif;font-size:12px">'
        '&#128225; PolicyPulse Intelligence Digest &bull; '
        + (row["sent_date"] or "")[:10] +
        '</div>'
    )
    html = row["html_content"].replace("<body", banner + "<body", 1)
    return HTMLResponse(content=html, status_code=200)


# ── SCRAPE LOG ────────────────────────────────────────────────────────────────

@app.get("/scrape/log")
def get_scrape_log(limit: int = Query(20)):
    """News/RSS scraper history only. Pre-migration rows (scrape_type IS NULL)
    are treated as 'news' since the scholarly scraper never logged here
    before scrape_type existed."""
    from database import get_conn
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM scrape_log WHERE scrape_type = 'news' OR scrape_type IS NULL "
        "ORDER BY id DESC LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()
    return {"log": [dict(r) for r in rows]}


@app.get("/scholarly/scrape/log")
def get_scholarly_scrape_log(limit: int = Query(20)):
    """Scholarly/research scraper history only."""
    from database import get_conn
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM scrape_log WHERE scrape_type = 'research' ORDER BY id DESC LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()
    return {"log": [dict(r) for r in rows]}


# ── SOURCE REACHABILITY ───────────────────────────────────────────────────────

@app.get("/sources/{source_id}/check")
def check_source_reachability(source_id: int):
    import requests as req_lib
    from database import get_conn
    conn = get_conn()
    row = conn.execute("SELECT url FROM sources WHERE id = ?", (source_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Source not found")
    url = row["url"]
    try:
        r = req_lib.head(url, timeout=8, allow_redirects=True,
                         headers={"User-Agent": "PolicyPulse/1.0 link-checker"})
        reachable = r.status_code < 400
        return {"reachable": reachable, "status": r.status_code, "url": url}
    except req_lib.exceptions.Timeout:
        return {"reachable": False, "status": "timeout", "url": url}
    except Exception as e:
        return {"reachable": False, "status": str(e)[:80], "url": url}


# ── ARTICLE PRUNING ───────────────────────────────────────────────────────────

@app.delete("/articles/prune")
def prune_old_articles(
    days: int = Query(180, ge=30, le=3650),
    dry_run: bool = Query(False),
    _=Depends(require_auth),
):
    from database import get_conn
    conn = get_conn()
    cutoff = (datetime.utcnow() - __import__("datetime").timedelta(days=days)).date().isoformat()
    count_row = conn.execute(
        "SELECT COUNT(*) FROM articles WHERE pub_date < ? AND staged = 0", (cutoff,)
    ).fetchone()[0]
    if not dry_run:
        conn.execute("DELETE FROM articles WHERE pub_date < ? AND staged = 0", (cutoff,))
        conn.commit()
    conn.close()
    return {
        "ok": True, "dry_run": dry_run,
        "deleted": 0 if dry_run else count_row,
        "would_delete": count_row,
        "cutoff_date": cutoff,
        "note": "Staged articles are never pruned.",
    }


@app.get("/articles/prune/preview")
def prune_preview(days: int = Query(180, ge=30, le=3650), _=Depends(require_auth)):
    from database import get_conn
    conn = get_conn()
    cutoff = (datetime.utcnow() - __import__("datetime").timedelta(days=days)).date().isoformat()
    count = conn.execute(
        "SELECT COUNT(*) FROM articles WHERE pub_date < ? AND staged = 0", (cutoff,)
    ).fetchone()[0]
    total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    conn.close()
    return {"would_delete": count, "total": total, "cutoff_date": cutoff, "days": days}


# ── ARTICLE RE-ANALYSIS ───────────────────────────────────────────────────────

@app.post("/articles/{article_id}/reanalyze")
def reanalyze_article(article_id: int, _=Depends(require_auth)):
    from database import get_conn, get_article_by_id
    from scraper import fetch_article_details
    from ai_processor import analyze_article

    a = get_article_by_id(article_id)
    if not a:
        raise HTTPException(status_code=404, detail="Article not found")

    article_text, _ = fetch_article_details(a["url"])
    result = analyze_article(
        title=a["title"], url=a["url"],
        source_name=a["source"], article_text=article_text or "",
    )
    if not result:
        raise HTTPException(status_code=422, detail="AI scored this article below 6.")

    conn = get_conn()
    conn.execute(
        """UPDATE articles
           SET domain=?, jurisdiction=?, relevance=?, sentiment=?,
               summary=?, why_it_matters=?, tags=?
           WHERE id=?""",
        (result["domain"], result["jurisdiction"], result["relevance"],
         result["sentiment"], result["summary"], result["why_it_matters"],
         ",".join(result.get("tags", [])), article_id)
    )
    conn.commit()
    conn.close()
    return {"ok": True, "article_id": article_id, "result": result}


@app.post("/articles/reanalyze-bulk")
def reanalyze_bulk(body: dict, background_tasks: BackgroundTasks, _=Depends(require_auth)):
    from database import get_conn
    conn = get_conn()
    if "ids" in body and body["ids"]:
        ids = [int(i) for i in body["ids"][:50]]
    elif body.get("all_unread"):
        rows = conn.execute(
            "SELECT id FROM articles WHERE read=0 ORDER BY relevance DESC LIMIT 50"
        ).fetchall()
        ids = [r["id"] for r in rows]
    elif "days" in body:
        rows = conn.execute(
            "SELECT id FROM articles WHERE pub_date >= date('now', ?) ORDER BY relevance DESC LIMIT 50",
            (f"-{int(body['days'])} days",)
        ).fetchall()
        ids = [r["id"] for r in rows]
    else:
        conn.close()
        raise HTTPException(status_code=400, detail="Provide ids, all_unread, or days")
    conn.close()
    background_tasks.add_task(_reanalyze_batch_bg, ids)
    return {"ok": True, "queued": len(ids), "ids": ids}


def _reanalyze_batch_bg(ids: list):
    import time as _time
    from database import get_article_by_id, get_conn
    from scraper import fetch_article_details
    from ai_processor import analyze_article
    import logging as _log
    log = _log.getLogger(__name__)

    for article_id in ids:
        try:
            a = get_article_by_id(article_id)
            if not a:
                continue
            article_text, _ = fetch_article_details(a["url"])
            result = analyze_article(
                title=a["title"], url=a["url"],
                source_name=a["source"], article_text=article_text or "",
            )
            if not result:
                continue
            conn = get_conn()
            conn.execute(
                """UPDATE articles
                   SET domain=?, jurisdiction=?, relevance=?, sentiment=?,
                       summary=?, why_it_matters=?, tags=?
                   WHERE id=?""",
                (result["domain"], result["jurisdiction"], result["relevance"],
                 result["sentiment"], result["summary"], result["why_it_matters"],
                 ",".join(result.get("tags", [])), article_id)
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.warning(f"  [reanalyze] id={article_id} error: {e}")
        _time.sleep(0.5)


# ── EMAIL HELPER ──────────────────────────────────────────────────────────────

def _smtp_send(recipients: list[dict], subject: str, html: str) -> dict:
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from re import sub as re_sub

    smtp_host = os.environ.get("SMTP_HOST", "")
    smtp_port = int(os.environ.get("SMTP_PORT", 587))
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASSWORD", "")
    smtp_from = os.environ.get("SMTP_FROM", smtp_user)

    if not all([smtp_host, smtp_user, smtp_pass]):
        return {"sent": 0, "errors": ["SMTP not configured"]}

    sent, errors = 0, []
    try:
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.ehlo(); server.starttls(); server.login(smtp_user, smtp_pass)
        for r in recipients:
            try:
                msg = MIMEMultipart("alternative")
                msg["Subject"] = subject
                msg["From"]    = smtp_from
                msg["To"]      = r["email"]
                plain = re_sub(r"<[^>]+>", "", html.replace("<br>", "\n").replace("</p>", "\n\n"))
                msg.attach(MIMEText(plain, "plain"))
                msg.attach(MIMEText(html,  "html"))
                server.sendmail(smtp_user, r["email"], msg.as_string())
                sent += 1
            except Exception as e:
                errors.append(f"{r.get('email')}: {e}")
        server.quit()
    except smtplib.SMTPException as e:
        errors.append(f"SMTP connection: {e}")
    return {"sent": sent, "errors": errors}


# ── ALERT DISPATCH ────────────────────────────────────────────────────────────

def _build_article_card_html(a: dict, accent: str = "#c41e3a") -> str:
    rel   = a.get("relevance", 0)
    emoji = "🔥" if rel >= 9 else "⭐" if rel >= 7 else "📌"
    sent  = a.get("sentiment", "Neutral")
    sent_color = {"Critical": "#f43f5e", "Supportive": "#10b981"}.get(sent, "#94a3b8")
    return f"""
<div style="margin-bottom:20px;padding:14px 16px;background:#f8faff;
            border-left:4px solid {accent};border-radius:0 6px 6px 0">
  <div style="font-size:13px;font-weight:700;color:#0f1f35;margin-bottom:4px">
    <a href="{a.get('url','')}" style="color:#1d4ed8;text-decoration:none">
      {a.get('title','')} ↗
    </a>
  </div>
  <div style="font-size:11px;color:#6b8aaa;margin-bottom:8px">
    {a.get('source','')} &bull; {a.get('jurisdiction','')} &bull;
    {a.get('pub_date','')} &bull;
    <span style="color:{sent_color};font-weight:600">{sent}</span> &bull;
    {emoji} {rel}/10
  </div>
  <div style="font-size:12px;color:#334d6b;line-height:1.7;margin-bottom:6px">
    {a.get('summary','')}
  </div>
  <div style="font-size:12px;color:#0a4d6e;font-style:italic;
              padding:5px 10px;background:rgba(0,119,170,0.06);
              border-left:2px solid rgba(0,119,170,0.3)">
    💡 {a.get('why_it_matters','')}
  </div>
</div>"""


def _email_wrapper(title: str, subtitle: str, body_html: str, badge_color: str = "#c41e3a") -> str:
    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head>
<body style="font-family:Georgia,serif;background:#f2f5fa;margin:0;padding:20px">
<div style="max-width:620px;margin:0 auto;background:#fff;border-radius:8px;overflow:hidden;
            box-shadow:0 2px 12px rgba(0,0,0,.08)">
  <div style="background:linear-gradient(135deg,#003366,{badge_color});padding:22px 28px">
    <div style="font-size:11px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;
                color:rgba(255,255,255,.7);margin-bottom:4px">PolicyPulse Intelligence</div>
    <div style="font-size:22px;font-weight:700;color:#fff;font-family:Georgia,serif">
      {title}
    </div>
    <div style="font-size:12px;color:rgba(255,255,255,.75);margin-top:4px">{subtitle}</div>
  </div>
  <div style="padding:24px 28px">{body_html}</div>
  <div style="background:#f8faff;padding:16px 28px;border-top:1px solid #e8eef4;
              font-size:11px;color:#6b8aaa;text-align:center">
    PolicyPulse Intelligence Platform
  </div>
</div></body></html>"""


def _dispatch_urgent_alerts(new_articles: list[dict]):
    import logging as _log
    log = _log.getLogger(__name__)
    recipients = get_alert_subscribers("urgent")
    if not recipients:
        return

    # Each recipient has their own urgent_min_relevance threshold.
    # Build a per-recipient filtered list so a subscriber who wants only
    # 10/10 doesn't get flooded by 8/10 articles.
    for recipient in recipients:
        threshold = int(recipient.get("urgent_min_relevance") or 9)
        urgent    = [a for a in new_articles if a.get("relevance", 0) >= threshold]
        if not urgent:
            continue
        cards = "".join(_build_article_card_html(a, "#c41e3a") for a in urgent)
        html  = _email_wrapper(
            title    = f"🔥 {len(urgent)} Urgent Policy Alert{'s' if len(urgent)>1 else ''} (score {threshold}+)",
            subtitle = f"Articles scoring {threshold}+ scraped {datetime.utcnow().strftime('%B %d, %Y')}",
            body_html= cards,
            badge_color="#c41e3a",
        )
        result = _smtp_send(
            [{"name": recipient["name"], "email": recipient["email"]}],
            f"🔥 PolicyPulse Urgent Alert — {len(urgent)} article(s) scoring {threshold}+",
            html,
        )
        log.info(f"[alert] urgent dispatch to {recipient['email']} "
                 f"(threshold={threshold}): sent={result['sent']} errors={result['errors']}")


def _dispatch_keyword_alerts(new_articles: list[dict]):
    import logging as _log
    log = _log.getLogger(__name__)
    recipients = get_alert_subscribers("keyword")
    if not recipients:
        return
    keyword_hits: dict[str, list[dict]] = {}
    for a in new_articles:
        tag = a.get("forced_tag")
        if tag:
            keyword_hits.setdefault(tag, []).append(a)
    if not keyword_hits:
        return
    for keyword, arts in keyword_hits.items():
        cards = "".join(_build_article_card_html(a, "#1d4ed8") for a in arts)
        html  = _email_wrapper(
            title=f'📡 Watchlist Alert: "{keyword}"',
            subtitle=f"{len(arts)} new article{'s' if len(arts)>1 else ''} on {datetime.utcnow().strftime('%B %d, %Y')}",
            body_html=cards, badge_color="#1d4ed8",
        )
        result = _smtp_send(recipients, f'📡 PolicyPulse Watchlist Alert: "{keyword}" — {len(arts)} new article(s)', html)
        log.info(f"[alert] keyword '{keyword}': sent={result['sent']} errors={result['errors']}")


# ── ALERT ENDPOINTS ───────────────────────────────────────────────────────────

@app.patch("/subscribers/{subscriber_id}/alerts")
def update_subscriber_alert_prefs(subscriber_id: int, body: dict, _=Depends(require_auth)):
    urgent   = int(body.get("urgent_alerts",         0))
    keyword  = int(body.get("keyword_alerts",        0))
    min_rel  = int(body.get("urgent_min_relevance",  9))
    update_subscriber_alerts(subscriber_id, urgent, keyword, min_rel)
    return {"ok": True}


@app.post("/alerts/test-urgent")
def test_urgent_alert(body: dict, _=Depends(require_auth)):
    recipients = get_alert_subscribers("urgent")
    if not recipients:
        return {"ok": False, "detail": "No subscribers have opted into urgent alerts."}
    test_article = {
        "title": "TEST: PolicyPulse Urgent Alert — Configuration Check",
        "url": "https://policypulse.ca", "source": "PolicyPulse System",
        "jurisdiction": "Test", "domain": "Configuration", "relevance": 10,
        "sentiment": "Neutral",
        "summary": "This is a test alert sent from the PolicyPulse admin panel.",
        "why_it_matters": "If you received this email, urgent alerts are working correctly.",
        "pub_date": datetime.utcnow().date().isoformat(),
    }
    cards = _build_article_card_html(test_article, "#c41e3a")
    html  = _email_wrapper("🔥 TEST: Urgent Alert", "Configuration test — not a real alert.", cards, "#c41e3a")
    result = _smtp_send(recipients, "TEST: PolicyPulse Urgent Alert", html)
    return {"ok": True, **result}


@app.post("/alerts/test-keyword")
def test_keyword_alert(body: dict, _=Depends(require_auth)):
    recipients = get_alert_subscribers("keyword")
    if not recipients:
        return {"ok": False, "detail": "No subscribers have opted into keyword alerts."}
    keyword = body.get("keyword", "DRIPA")
    test_article = {
        "title": f"TEST: PolicyPulse Keyword Alert for '{keyword}'",
        "url": "https://policypulse.ca", "source": "PolicyPulse System",
        "jurisdiction": "Test", "domain": "Configuration", "relevance": 8,
        "sentiment": "Neutral",
        "summary": f"This is a test alert for the watchlist keyword '{keyword}'.",
        "why_it_matters": "If you received this, keyword alert delivery is working correctly.",
        "pub_date": datetime.utcnow().date().isoformat(),
    }
    cards = _build_article_card_html(test_article, "#1d4ed8")
    html  = _email_wrapper(f'📡 TEST: Keyword Alert "{keyword}"', "Configuration test.", cards, "#1d4ed8")
    result = _smtp_send(recipients, f"TEST: PolicyPulse Keyword Alert — '{keyword}'", html)
    return {"ok": True, **result}


# ── FIX DATES ─────────────────────────────────────────────────────────────────

@app.post("/articles/fix-dates")
def fix_article_dates(background_tasks: BackgroundTasks, body: dict = None, _=Depends(require_auth)):
    body = body or {}
    specific_ids = body.get("ids", [])
    limit = int(body.get("limit", 200))
    background_tasks.add_task(_fix_dates_bg, specific_ids, limit, "news")
    return {"ok": True, "message": "Date fix started in background."}


@app.post("/scholarly/fix-dates")
def fix_scholarly_dates(background_tasks: BackgroundTasks, body: dict = None, _=Depends(require_auth)):
    body = body or {}
    limit = int(body.get("limit", 100))
    background_tasks.add_task(_fix_dates_bg, [], limit, "scholarly")
    return {"ok": True, "message": "Scholarly date fix started in background."}


@app.get("/articles/fix-dates/preview")
def preview_articles_needing_date_fix(limit: int = Query(200)):
    rows = get_articles_missing_pub_date(limit=limit)
    return {
        "count": len(rows),
        "sample": [{"id": r["id"], "pub_date": r["pub_date"],
                    "processed_date": (r.get("processed_date") or "")[:10]} for r in rows[:10]]
    }


def _fix_dates_bg(specific_ids: list, limit: int, kind: str):
    import time as _time
    import logging as _log
    from scraper import fetch_article_details
    log = _log.getLogger(__name__)

    if kind == "scholarly":
        rows = get_scholarly_articles_missing_pub_date(limit=limit)
    else:
        if specific_ids:
            from database import get_conn
            conn = get_conn()
            rows = []
            for aid in specific_ids[:limit]:
                r = conn.execute(
                    "SELECT id, url, pub_date, processed_date FROM articles WHERE id=?", (aid,)
                ).fetchone()
                if r:
                    rows.append(dict(r))
            conn.close()
        else:
            rows = get_articles_missing_pub_date(limit=limit)

    fixed = skipped = 0
    for row in rows:
        try:
            _, extracted_date = fetch_article_details(row["url"])
            if extracted_date:
                if kind == "scholarly":
                    update_scholarly_pub_date(row["id"], extracted_date)
                else:
                    update_article_pub_date(row["id"], extracted_date)
                fixed += 1
            else:
                skipped += 1
        except Exception as e:
            skipped += 1
            log.warning(f"  [fix-dates] id={row['id']} error: {e}")
        _time.sleep(0.5)

    log.info(f"[fix-dates] Done. Fixed={fixed}, Skipped={skipped}")


# ── APP SETTINGS ──────────────────────────────────────────────────────────────

@app.get("/settings")
def get_settings():
    return {"settings": get_all_app_settings()}


@app.post("/settings")
def save_settings(body: dict, _=Depends(require_auth)):
    allowed_keys = {
        "timezone","date_format","scrape_time","scrape_days",
        "default_relevance_threshold","digest_day","digest_time",
        "show_pub_date_unknown_label","articles_per_page","default_sort",
        "theme","compact_cards","show_why_it_matters","keyboard_shortcuts",
        "auto_mark_read_on_open","alert_min_relevance","feed_refresh_interval",
    }
    saved = []
    for key, value in body.items():
        if key in allowed_keys:
            set_app_setting(key, str(value))
            saved.append(key)
    return {"ok": True, "saved": saved}


@app.delete("/settings/{key}")
def delete_setting(key: str, _=Depends(require_auth)):
    from database import get_conn
    conn = get_conn()
    conn.execute("DELETE FROM scraper_config WHERE key = ?", ("setting:" + key,))
    conn.commit()
    conn.close()
    return {"ok": True}


# ── POLICY TRACKERS ───────────────────────────────────────────────────────────

@app.get("/trackers")
def list_trackers():
    return {"trackers": get_trackers()}


@app.post("/trackers")
def create_tracker_endpoint(body: dict, _=Depends(require_auth)):
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name is required")
    new_id = create_tracker(
        name=name,
        description=body.get("description", ""),
        domain=body.get("domain", ""),
        keywords=body.get("keywords", ""),
        status=body.get("status", "Active"),
    )
    return {"ok": True, "id": new_id}


@app.get("/trackers/{tracker_id}")
def get_tracker_endpoint(tracker_id: int):
    t = get_tracker_by_id(tracker_id)
    if not t:
        raise HTTPException(status_code=404, detail="Tracker not found")
    t["articles"] = get_tracker_articles(tracker_id)
    t["events"]   = get_tracker_events(tracker_id)
    return t


@app.patch("/trackers/{tracker_id}")
def update_tracker_endpoint(tracker_id: int, body: dict, _=Depends(require_auth)):
    allowed = {"name", "description", "domain", "status", "keywords"}
    updates = {k: v for k, v in body.items() if k in allowed}
    if not updates:
        raise HTTPException(status_code=400, detail=f"Allowed fields: {allowed}")
    update_tracker(tracker_id, updates)
    return {"ok": True}


@app.delete("/trackers/{tracker_id}")
def delete_tracker_endpoint(tracker_id: int, _=Depends(require_auth)):
    delete_tracker(tracker_id)
    return {"ok": True}


# ── TRACKER ARTICLES ──────────────────────────────────────────────────────────

@app.get("/trackers/{tracker_id}/articles")
def list_tracker_articles(tracker_id: int):
    return {"articles": get_tracker_articles(tracker_id)}


@app.post("/trackers/{tracker_id}/articles")
def add_article_to_tracker(tracker_id: int, body: dict, _=Depends(require_auth)):
    article_id   = body.get("article_id")
    article_type = body.get("article_type", "news")
    note         = body.get("note", "")
    if not article_id:
        raise HTTPException(status_code=400, detail="article_id required")
    t = get_tracker_by_id(tracker_id)
    if not t:
        raise HTTPException(status_code=404, detail="Tracker not found")
    added = add_tracker_article(tracker_id, int(article_id), article_type, note)
    return {"ok": True, "added": added, "already_linked": not added}


@app.delete("/trackers/{tracker_id}/articles/{article_id}")
def remove_article_from_tracker(tracker_id: int, article_id: int,
                                article_type: str = "news", _=Depends(require_auth)):
    remove_tracker_article(tracker_id, article_id, article_type)
    return {"ok": True}


# ── TRACKER EVENTS ────────────────────────────────────────────────────────────

@app.get("/trackers/{tracker_id}/events")
def list_tracker_events(tracker_id: int):
    return {"events": get_tracker_events(tracker_id)}


@app.post("/trackers/{tracker_id}/events")
def add_tracker_event_endpoint(tracker_id: int, body: dict, _=Depends(require_auth)):
    title = (body.get("title") or "").strip()
    if not title:
        raise HTTPException(status_code=400, detail="title required")
    t = get_tracker_by_id(tracker_id)
    if not t:
        raise HTTPException(status_code=404, detail="Tracker not found")
    new_id = add_tracker_event(
        tracker_id=tracker_id,
        title=title,
        event_date=body.get("event_date", ""),
        note=body.get("note", ""),
    )
    return {"ok": True, "id": new_id}


@app.delete("/trackers/{tracker_id}/events/{event_id}")
def delete_tracker_event_endpoint(tracker_id: int, event_id: int,
                                  _=Depends(require_auth)):
    delete_tracker_event(event_id)
    return {"ok": True}


# ── TRACKER AI — SYNTHESIS & SUGGESTIONS ─────────────────────────────────────

async def _groq_call(messages: list, max_tokens: int = 1800) -> str:
    """Internal Groq call shared by synthesize and suggest endpoints."""
    import httpx
    groq_key = os.environ.get("GROQ_API_KEY", "")
    if not groq_key:
        raise HTTPException(
            status_code=503,
            detail="GROQ_API_KEY not set — add it in Render environment variables."
        )
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            json={
                "model":       "llama-3.3-70b-versatile",
                "messages":    messages,
                "max_tokens":  max_tokens,
                "temperature": 0.3,
            },
            headers={
                "Authorization": f"Bearer {groq_key}",
                "Content-Type":  "application/json",
            },
        )
        if resp.status_code == 429:
            raise HTTPException(status_code=429, detail="Groq rate limit — wait and retry.")
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]


@app.post("/trackers/{tracker_id}/synthesize")
async def synthesize_tracker(tracker_id: int):
    """
    Generate an AI narrative synthesis of the tracker's linked articles and
    events, presented as a chronological policy brief.

    Returns:
        { "ok": true, "synthesis": "...", "article_count": N }
    """
    t = get_tracker_by_id(tracker_id)
    if not t:
        raise HTTPException(status_code=404, detail="Tracker not found")

    linked = get_tracker_articles(tracker_id)
    events = get_tracker_events(tracker_id)

    if not linked and not events:
        raise HTTPException(
            status_code=422,
            detail="No linked articles or events yet — add some content first."
        )

    # Build a numbered source list for the AI to reference inline
    source_lines = []
    items_for_timeline = []

    for i, a in enumerate(linked, start=1):
        date  = (a.get("pub_date") or a.get("added_date") or "")[:10]
        title = a.get("title") or "Untitled"
        src   = a.get("source") or ""
        summ  = (a.get("summary") or "")[:400]
        wim   = (a.get("why_it_matters") or "")[:300]
        source_lines.append(
            f"[{i}] {date} | {src}\n"
            f"    TITLE: {title}\n"
            f"    SUMMARY: {summ}\n"
            + (f"    WHY IT MATTERS: {wim}\n" if wim else "")
        )
        items_for_timeline.append({"date": date, "label": f"[{i}] {title}"})

    for e in events:
        items_for_timeline.append({
            "date":  (e.get("event_date") or e.get("created_date") or "")[:10],
            "label": f"[EVENT] {e.get('title','')}",
            "note":  e.get("note", ""),
        })

    items_for_timeline.sort(key=lambda x: x.get("date") or "")

    tracker_context = (
        f"Tracker: {t['name']}\n"
        f"Domain: {t.get('domain','')}\n"
        f"Status: {t.get('status','Active')}\n"
        + (f"Description: {t['description']}\n" if t.get("description") else "")
        + (f"Keywords: {t['keywords']}\n"       if t.get("keywords")    else "")
    )

    prompt = f"""You are a senior Canadian government relations analyst. 
Generate a POLICY TIMELINE NARRATIVE for this tracker.

TRACKER CONTEXT:
{tracker_context}

LINKED SOURCES ({len(linked)} articles):
{"".join(source_lines)}

MANUAL EVENTS ({len(events)}):
{chr(10).join(f"- {e.get('event_date','')[:10]} | {e.get('title','')} — {e.get('note','')}" for e in events) or "None"}

INSTRUCTIONS:
Write a flowing narrative that tells the story of how this policy issue has evolved over time. Structure it as:

OVERVIEW
2-3 sentences: what this issue is, its current status, and why it matters institutionally.

TIMELINE NARRATIVE
A chronological account written as connected prose (not a bullet list). Use inline references [N] to cite specific articles. Each paragraph should cover a distinct phase or development. Aim for 3-5 paragraphs.

KEY TENSIONS
2-3 bullet points naming the core unresolved tensions or competing interests driving this issue.

INSTITUTIONAL IMPLICATIONS
2-3 concrete action-oriented sentences about what this means for the institution right now. Name specific obligations, deadlines, or decisions triggered.

WHAT TO WATCH
3 bullet points: specific upcoming dates, decisions, or events that will determine how this issue evolves.

Rules:
- Every factual claim must cite a source using [N] notation
- Write for a VP or senior director reading on their phone
- Be specific — name ministries, legislation, amounts, dates
- No hedging phrases like "may be relevant" or "could potentially"
- Current status must reflect the most recent source date"""

    try:
        synthesis = await _groq_call(
            messages=[
                {"role": "system", "content": "You are a concise, precise Canadian policy analyst. Return only the requested narrative — no preamble, no meta-commentary."},
                {"role": "user",   "content": prompt},
            ],
            max_tokens=1800,
        )
        return {"ok": True, "synthesis": synthesis, "article_count": len(linked)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Synthesis failed: {str(e)[:200]}")


@app.post("/trackers/{tracker_id}/suggest")
async def suggest_tracker_articles(tracker_id: int):
    """
    Ask AI to score all unlinked articles in the database for relevance to
    this tracker, returning the top matches as suggestions.

    Returns:
        { "ok": true, "suggestions": [ {article, score, reason}, ... ] }
    """
    t = get_tracker_by_id(tracker_id)
    if not t:
        raise HTTPException(status_code=404, detail="Tracker not found")

    # Build a set of already-linked article IDs to exclude
    already_linked = {a["article_id"] for a in get_tracker_articles(tracker_id)}

    # Pull a candidate pool — recent articles across all domains
    candidates = get_articles(limit=120, offset=0, sort="date")

    # Pre-filter by keyword match to keep the prompt within token limits
    kws = [k.strip().lower() for k in (t.get("keywords") or "").split(",") if k.strip()]
    domain = (t.get("domain") or "").lower()

    def _relevance_hint(a: dict) -> int:
        """Quick keyword pre-score so we send the best 30 candidates to AI."""
        score = 0
        hay = " ".join([
            (a.get("title") or ""), (a.get("summary") or ""),
            (a.get("domain") or ""), (a.get("tags") or ""),
        ]).lower()
        for kw in kws:
            if kw in hay: score += 3
        if domain and domain in (a.get("domain") or "").lower():
            score += 2
        score += min((a.get("relevance") or 0), 5)
        return score

    # Exclude already-linked, score the rest, take top 30
    pool = [a for a in candidates if a.get("id") not in already_linked]
    pool.sort(key=_relevance_hint, reverse=True)
    pool = pool[:30]

    if not pool:
        return {"ok": True, "suggestions": [], "note": "No unlinked candidate articles found."}

    # Build compact article list for the prompt
    article_lines = "\n".join(
        f"[{i+1}] (id={a['id']}) {(a.get('pub_date') or '')[:10]} | {a.get('source','')} | "
        f"rel={a.get('relevance',0)} | domain={a.get('domain','')}\n"
        f"     TITLE: {a.get('title','')}\n"
        f"     SUMMARY: {(a.get('summary') or '')[:200]}"
        for i, a in enumerate(pool)
    )

    tracker_desc = (
        f"Tracker: {t['name']}\n"
        f"Domain: {t.get('domain','')}\n"
        f"Status: {t.get('status','')}\n"
        f"Keywords: {t.get('keywords','')}\n"
        + (f"Description: {t['description']}" if t.get("description") else "")
    )

    prompt = f"""You are a policy intelligence assistant.

TRACKER TO MATCH AGAINST:
{tracker_desc}

CANDIDATE ARTICLES (score each for relevance to this tracker):
{article_lines}

TASK:
Return a JSON array of the top matches. Only include articles that are genuinely relevant — not every article that tangentially mentions a keyword.

For each match return:
{{
  "article_index": <1-based index from the list above>,
  "article_id": <the id= value>,
  "score": <integer 1-10, where 10 = directly and substantively about this tracker topic>,
  "reason": "<one sentence: specifically why this article matters for this tracker>"
}}

Rules:
- Only return articles scoring 6 or above
- Maximum 10 suggestions
- Be selective — if only 2 articles genuinely match, return 2
- The reason must be specific to THIS tracker, not generic
- Return ONLY the JSON array, no other text"""

    try:
        raw = await _groq_call(
            messages=[
                {"role": "system", "content": "You are a precise policy relevance scorer. Return only valid JSON arrays."},
                {"role": "user",   "content": prompt},
            ],
            max_tokens=900,
        )

        import json, re
        # Strip markdown fences if present
        cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw.strip(), flags=re.MULTILINE)
        suggestions_raw = json.loads(cleaned)

        # Enrich each suggestion with full article data
        pool_by_id = {a["id"]: a for a in pool}
        suggestions = []
        for s in suggestions_raw:
            if not isinstance(s, dict): continue
            aid = s.get("article_id")
            if not aid or aid in already_linked: continue
            article = pool_by_id.get(aid)
            if not article: continue
            suggestions.append({
                "article_id":  aid,
                "score":       int(s.get("score", 6)),
                "reason":      str(s.get("reason", ""))[:300],
                "title":       article.get("title", ""),
                "source":      article.get("source", ""),
                "pub_date":    article.get("pub_date", ""),
                "domain":      article.get("domain", ""),
                "relevance":   article.get("relevance", 0),
                "summary":     (article.get("summary") or "")[:300],
                "url":         article.get("url", ""),
            })

        suggestions.sort(key=lambda x: x["score"], reverse=True)
        return {"ok": True, "suggestions": suggestions[:10]}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Suggestion failed: {str(e)[:200]}")


@app.post("/trackers/{tracker_id}/gaps")
async def detect_tracker_gaps(tracker_id: int):
    """
    Analyse the tracker's linked articles and identify what is missing:
    which sources haven't been heard from, which related policy areas
    haven't been covered, what upcoming decisions haven't been tracked,
    and what questions remain unanswered.

    Returns:
        { "ok": true, "gaps": "...", "article_count": N }
    """
    t = get_tracker_by_id(tracker_id)
    if not t:
        raise HTTPException(status_code=404, detail="Tracker not found")

    linked  = get_tracker_articles(tracker_id)
    events  = get_tracker_events(tracker_id)

    if not linked and not events:
        raise HTTPException(
            status_code=422,
            detail="No linked articles or events yet — add some content first."
        )

    # Build a compact picture of what we already have
    source_names   = list({a.get("source", "") for a in linked if a.get("source")})
    domains_seen   = list({
        d.strip()
        for a in linked
        for d in (a.get("domain") or "").split(",")
        if d.strip()
    })
    date_range = ""
    dates = sorted([a.get("pub_date","") for a in linked if a.get("pub_date")])
    if dates:
        date_range = f"{dates[0][:10]} to {dates[-1][:10]}"

    article_summaries = "\n".join(
        f"- [{(a.get('pub_date',''))[:10]}] {a.get('source','')} | {a.get('title','')}"
        for a in sorted(linked, key=lambda x: x.get("pub_date") or "")
    )
    event_summaries = "\n".join(
        f"- [{(e.get('event_date',''))[:10]}] {e.get('title','')}"
        for e in events
    ) if events else "None"

    tracker_context = (
        f"Tracker: {t['name']}\n"
        f"Domain: {t.get('domain', '')}\n"
        f"Status: {t.get('status', 'Active')}\n"
        f"Keywords: {t.get('keywords', '')}\n"
        + (f"Description: {t['description']}\n" if t.get("description") else "")
    )

    prompt = f"""You are a senior Canadian government relations analyst conducting an intelligence gap analysis.

TRACKER CONTEXT:
{tracker_context}

WHAT WE ALREADY HAVE ({len(linked)} articles, {len(events)} events):
Coverage dates: {date_range or "unknown"}
Sources heard from: {", ".join(source_names) or "none"}
Policy domains covered: {", ".join(domains_seen) or "none"}

ARTICLE LIST:
{article_summaries or "None"}

MANUAL EVENTS:
{event_summaries}

TASK — Identify what is MISSING from this intelligence picture. Structure your response as:

MISSING VOICES
Which stakeholders or organizations should have been heard from but haven't appeared in coverage? For each: Name | Why they matter | What position we'd expect from them.

UNCOVERED POLICY AREAS
Which related policy domains or legislation intersect with this tracker but haven't been covered? Be specific — name the bills, regulations, or policy processes.

UPCOMING CRITICAL DATES
What dates, deadlines, or decision points are likely approaching that aren't yet captured in the timeline? Include: legislative calendar milestones, consultation windows, budget cycles, board obligations, regulatory review periods.

UNRESOLVED QUESTIONS
What are the 3-4 most important questions a government relations professional needs answers to right now that this intelligence doesn't yet answer?

RECOMMENDED NEXT ACTIONS
3-5 concrete steps to close these gaps. Format each as: [Action] — [Who should do it] — [By when].

Rules:
- Be specific to THIS tracker topic and domain
- Name specific organizations, ministries, and legislation — not generic categories
- Focus on what's actionable in the next 30-90 days
- Do not list gaps we already have covered"""

    try:
        gaps = await _groq_call(
            messages=[
                {"role": "system", "content": "You are a precise Canadian policy intelligence analyst. Return only the requested gap analysis — no preamble."},
                {"role": "user",   "content": prompt},
            ],
            max_tokens=1600,
        )
        return {"ok": True, "gaps": gaps, "article_count": len(linked)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Gap analysis failed: {str(e)[:200]}")


@app.get("/trackers/{tracker_id}/briefing-context")
async def get_tracker_briefing_context(tracker_id: int):
    """
    Return a pre-populated briefing note context payload built from the
    tracker's linked articles, keywords, domain, status, and description.
    The frontend uses this to pre-fill the Briefing Note generator and
    select the tracker's articles in one click.

    Returns:
        {
          "ok": true,
          "title": str,
          "context": str,
          "domain": str,
          "issue_status": str,
          "portfolio": str,
          "article_ids": [ {"id": int, "type": "news"|"research"}, ... ]
        }
    """
    t = get_tracker_by_id(tracker_id)
    if not t:
        raise HTTPException(status_code=404, detail="Tracker not found")

    linked = get_tracker_articles(tracker_id)

    # Build a sensible default briefing title from the tracker name + status
    title = f"{t['name']} — {t.get('status', 'Active')} Issue Brief"

    # Build a context sentence from the tracker description + keywords
    kws = [k.strip() for k in (t.get("keywords") or "").split(",") if k.strip()]
    context_parts = []
    if t.get("description"):
        context_parts.append(t["description"])
    if kws:
        context_parts.append(f"Key monitoring terms: {', '.join(kws[:5])}.")
    context = " ".join(context_parts) if context_parts else f"Policy tracking brief on {t['name']}."

    article_ids = [
        {"id": a["article_id"], "type": a.get("article_type", "news")}
        for a in linked
        if a.get("article_id")
    ]

    return {
        "ok":          True,
        "title":       title,
        "context":     context,
        "domain":      t.get("domain", ""),
        "issue_status": t.get("status", "Active"),
        "portfolio":   t.get("domain", ""),
        "keywords":    t.get("keywords", ""),
        "article_ids": article_ids,
    }


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    uvicorn.run('main:app', host='0.0.0.0', port=port, reload=False)
