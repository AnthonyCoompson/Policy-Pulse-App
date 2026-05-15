"""
PolicyPulse Backend — FastAPI + SQLite + Gemini AI
Deployed on Railway.app

v5: Security hardening
    - CORS locked to ALLOWED_ORIGINS env var (no more wildcard)
    - API key authentication on all write/mutating endpoints
    - scraper_config functions added to database.py (were missing, caused 500s)
    - SQLite backup endpoint added
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
    # Source CRUD
    add_source, toggle_source, delete_source, update_source,
    # Research source CRUD
    get_research_sources, add_research_source, toggle_research_source,
    delete_research_source, update_research_source,
    # Scholarly keywords
    get_scholarly_keywords, add_scholarly_keyword,
    delete_scholarly_keyword, toggle_scholarly_keyword,
    # Subscribers
    get_subscribers, add_subscriber, toggle_subscriber,
    delete_subscriber, update_subscriber,
    # Scholarly article lookup
    get_scholarly_article_by_id,
    # Scraper config — now defined in database.py
    get_scraper_config, set_scraper_config, get_all_scraper_config,
)
from scraper import run_scrape
from scheduler import start_scheduler

from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    start_scheduler()
    yield

app = FastAPI(title="PolicyPulse API", version="5.0.0", lifespan=lifespan)

# ── CORS ──────────────────────────────────────────────────────────────────────
# Set ALLOWED_ORIGINS in Railway env vars as a comma-separated list, e.g.:
#   https://policypulse.netlify.app,https://policypulse.ca
# Falls back to wildcard only if env var is not set, so local dev still works.

_raw_origins = os.environ.get("ALLOWED_ORIGINS", "")
ALLOWED_ORIGINS = (
    [o.strip() for o in _raw_origins.split(",") if o.strip()]
    if _raw_origins
    else ["*"]
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── API KEY AUTHENTICATION ────────────────────────────────────────────────────
# Set PP_API_KEY in Railway env vars to any long random string, e.g.:
#   openssl rand -hex 32
# The frontend sends this as:  Authorization: Bearer <key>
# GET /articles, GET /stats, GET /health are public (read-only, safe).
# All POST / PATCH / DELETE endpoints require the key.

_API_KEY = os.environ.get("PP_API_KEY", "")

def verify_api_key(request: Request) -> bool:
    """
    Returns True if the request is authorised.
    - If PP_API_KEY is not set in env, auth is skipped (backward-compatible
      for local dev / first deploy before key is configured).
    - If set, the Authorization header must be: Bearer <PP_API_KEY>
    """
    if not _API_KEY:
        return True  # No key configured — open access (dev mode)
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer ") and auth[7:] == _API_KEY:
        return True
    return False

def require_auth(request: Request):
    """FastAPI dependency — raises 401 if key is wrong."""
    if not verify_api_key(request):
        raise HTTPException(
            status_code=401,
            detail="Unauthorised. Set PP_API_KEY in Railway and pass it as: Authorization: Bearer <key>"
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
    """Permanently delete an article from the database."""
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
    background_tasks.add_task(run_scrape, filter_config)
    return {"ok": True, "message": "Scrape started in background"}


@app.get("/scrape/status")
def scrape_status():
    from database import get_last_scrape_time
    return {"last_scraped": get_last_scrape_time()}


# ── SOURCES — full CRUD ───────────────────────────────────────────────────────

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


# ── RESEARCH SOURCES — full CRUD ──────────────────────────────────────────────

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

    if not all([smtp_host, smtp_user, smtp_pass]):
        raise HTTPException(status_code=503, detail="SMTP not configured in Railway env vars.")

    subject      = body.get("subject","PolicyPulse Weekly Digest")
    html_content = body.get("html_content","")
    recipients   = body.get("recipients",[])
    if not recipients: raise HTTPException(status_code=400, detail="No recipients provided.")
    if not html_content: raise HTTPException(status_code=400, detail="No content provided.")

    sent_count, errors = 0, []
    try:
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.ehlo(); server.starttls(); server.login(smtp_user, smtp_pass)
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
    except smtplib.SMTPException as e:
        raise HTTPException(status_code=500, detail=f"SMTP connection failed: {e}")

    token = "pp-" + secrets.token_hex(4)
    save_digest(subject=subject, html_content=html_content, recipients=sent_count, token=token)
    return {"ok": True, "sent": sent_count, "errors": errors, "token": token}


# ── NOTION PROXY ─────────────────────────────────────────────────────────────

@app.post("/notion/test")
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


# ── SCHOLARLY / RESEARCH ──────────────────────────────────────────────────────

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


# ── SCHOLARLY TAGS ────────────────────────────────────────────────────────────

@app.post("/scholarly/{article_id}/tags")
def add_scholarly_tag(article_id: int, body: dict, _=Depends(require_auth)):
    """Add a tag to a scholarly article."""
    tag = body.get("tag", "").strip()
    if not tag:
        raise HTTPException(status_code=400, detail="tag required")
    from database import get_conn
    conn = get_conn()
    row = conn.execute(
        "SELECT tags FROM scholarly_articles WHERE id = ?", (article_id,)
    ).fetchone()
    if row:
        existing = [t.strip() for t in (row["tags"] or "").split(",") if t.strip()]
        if tag not in existing:
            existing.append(tag)
        conn.execute(
            "UPDATE scholarly_articles SET tags = ? WHERE id = ?",
            (",".join(existing), article_id)
        )
        conn.commit()
    conn.close()
    return {"ok": True}


@app.delete("/scholarly/{article_id}/tags/{tag}")
def remove_scholarly_tag(article_id: int, tag: str, _=Depends(require_auth)):
    """Remove a tag from a scholarly article."""
    from database import get_conn
    conn = get_conn()
    row = conn.execute(
        "SELECT tags FROM scholarly_articles WHERE id = ?", (article_id,)
    ).fetchone()
    if row:
        existing = [t.strip() for t in (row["tags"] or "").split(",") if t.strip() and t.strip() != tag]
        conn.execute(
            "UPDATE scholarly_articles SET tags = ? WHERE id = ?",
            (",".join(existing), article_id)
        )
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
    """Return all saved scraper filter config values."""
    return {"config": get_all_scraper_config()}


@app.post("/scraper-config")
def set_scraper_config_endpoint(body: dict, _=Depends(require_auth)):
    """Save one or more config key-value pairs."""
    import json
    for key, value in body.items():
        if isinstance(value, (dict, list)):
            value = json.dumps(value)
        set_scraper_config(str(key), str(value))
    return {"ok": True, "saved": list(body.keys())}


@app.delete("/scraper-config/{key}")
def delete_scraper_config_endpoint(key: str, _=Depends(require_auth)):
    """Delete a single config key."""
    from database import get_conn
    conn = get_conn()
    conn.execute("DELETE FROM scraper_config WHERE key = ?", (key,))
    conn.commit()
    conn.close()
    return {"ok": True}


# ── DATABASE BACKUP ───────────────────────────────────────────────────────────

@app.get("/backup/download")
def download_backup(_=Depends(require_auth)):
    """
    Stream the live SQLite database as a downloadable file.
    Uses SQLite's built-in backup API so the copy is always consistent
    even if a scrape is running concurrently.

    Usage:  GET /backup/download
            Authorization: Bearer <PP_API_KEY>

    Save the file as policypulse-backup-YYYY-MM-DD.db and keep it safe.
    To restore: replace policypulse.db on Railway with this file and redeploy.
    """
    import io
    import sqlite3 as _sqlite3
    from fastapi.responses import StreamingResponse

    db_path = os.environ.get("DB_PATH", "policypulse.db")
    if not os.path.exists(db_path):
        raise HTTPException(status_code=404, detail="Database file not found")

    buf = io.BytesIO()
    try:
        src  = _sqlite3.connect(db_path)
        dest = _sqlite3.connect(":memory:")
        src.backup(dest)
        src.close()
        # Serialise in-memory DB to bytes
        for chunk in dest.iterdump():
            pass  # iterdump is text-only; use serialize instead
        dest.close()

        # Re-open and use the serialize API (Python 3.11+) or file copy
        src2 = _sqlite3.connect(db_path)
        try:
            data = src2.serialize()   # returns bytes of the whole DB
            buf.write(data)
        except AttributeError:
            # Fallback for Python < 3.11: direct file read after WAL checkpoint
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
    """Return database size and table row counts — useful for monitoring."""
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
    """
    Serve a saved digest as a public HTML page — no auth required.
    This is the URL generated by the 'Public URL' button in the Digest tab.
    """
    from fastapi.responses import HTMLResponse
    from database import get_conn
    conn = get_conn()
    row = conn.execute(
        "SELECT subject, html_content, sent_date FROM digests WHERE token = ?",
        (token,)
    ).fetchone()
    conn.close()
    if not row or not row["html_content"]:
        raise HTTPException(status_code=404, detail="Digest not found or has expired.")
    # Inject a small read-only banner into the digest HTML
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
    """Return the last N scrape log entries for display in the UI."""
    from database import get_conn
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM scrape_log ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return {"log": [dict(r) for r in rows]}


# ── SOURCE REACHABILITY CHECK ─────────────────────────────────────────────────

@app.get("/sources/{source_id}/check")
def check_source_reachability(source_id: int):
    """
    HEAD-request a source URL and return whether it's reachable.
    Called by the Fix Links button in the Sources tab.
    Times out after 8 seconds per source.
    """
    import requests as req_lib
    from database import get_conn
    conn = get_conn()
    row = conn.execute("SELECT url FROM sources WHERE id = ?", (source_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Source not found")
    url = row["url"]
    try:
        r = req_lib.head(
            url,
            timeout=8,
            allow_redirects=True,
            headers={"User-Agent": "PolicyPulse/1.0 link-checker"}
        )
        reachable = r.status_code < 400
        return {"reachable": reachable, "status": r.status_code, "url": url}
    except req_lib.exceptions.Timeout:
        return {"reachable": False, "status": "timeout", "url": url}
    except Exception as e:
        return {"reachable": False, "status": str(e)[:80], "url": url}
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    uvicorn.run('main:app', host='0.0.0.0', port=port, reload=False)
