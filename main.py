"""
PolicyPulse Backend — FastAPI + SQLite + Gemini AI
Deployed on Railway.app

v4: Full source management — add, toggle, delete, edit news sources and
    research sources directly from the frontend UI.
    Added scholarly keywords CRUD endpoints.
"""

from fastapi import FastAPI, BackgroundTasks, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
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
)
from database import (
    init_db, get_articles, get_article_by_id, get_sources, get_stats,
    get_digest_history, save_digest, update_article_read, update_article_staged,
    get_watchlist_keywords, add_watchlist_keyword, remove_watchlist_keyword,
    add_article_tag, remove_article_tag, update_article_sentiment,
    update_article_content,
    update_article_pub_date,
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
)
from scraper import run_scrape
from scheduler import start_scheduler

app = FastAPI(title="PolicyPulse API", version="4.0.0")

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
    update_article_read(article_id, body.get("read", True))
    return {"ok": True}


@app.patch("/articles/{article_id}/staged")
def mark_staged(article_id: int, body: dict = {}):
    update_article_staged(article_id, body.get("staged", True))
    return {"ok": True}


@app.patch("/articles/{article_id}")
def update_article(article_id: int, body: dict):
    allowed = {"summary", "why_it_matters"}
    updates = {k: v for k, v in body.items() if k in allowed and isinstance(v, str)}
    if not updates:
        raise HTTPException(status_code=400, detail=f"Allowed fields: {allowed}")
    update_article_content(article_id, updates)
    return {"ok": True, "updated": list(updates.keys())}


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
        for attr in [a for a in el.attrs if a.startswith("on")]:
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
def trigger_scrape(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_scrape)
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
def create_source(body: dict):
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
def toggle_source_endpoint(source_id: int):
    new_state = toggle_source(source_id)
    return {"ok": True, "active": new_state}


@app.patch("/sources/{source_id}")
def edit_source(source_id: int, body: dict):
    allowed = {"name", "url", "jurisdiction", "scrape_type", "active"}
    updates = {k: v for k, v in body.items() if k in allowed}
    if not updates:
        raise HTTPException(status_code=400, detail=f"Allowed fields: {allowed}")
    update_source(source_id, updates)
    return {"ok": True}


@app.delete("/sources/{source_id}")
def remove_source(source_id: int):
    delete_source(source_id)
    return {"ok": True}


# ── RESEARCH SOURCES — full CRUD ──────────────────────────────────────────────

@app.get("/research-sources")
def list_research_sources():
    return {"sources": get_research_sources()}


@app.post("/research-sources")
def create_research_source(body: dict):
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
def toggle_research_source_endpoint(source_id: int):
    new_state = toggle_research_source(source_id)
    return {"ok": True, "active": new_state}


@app.patch("/research-sources/{source_id}")
def edit_research_source(source_id: int, body: dict):
    allowed = {"name", "url", "source_type", "active", "relevance_boost", "notes"}
    updates = {k: v for k, v in body.items() if k in allowed}
    if not updates:
        raise HTTPException(status_code=400, detail=f"Allowed fields: {allowed}")
    update_research_source(source_id, updates)
    return {"ok": True}


@app.delete("/research-sources/{source_id}")
def remove_research_source(source_id: int):
    delete_research_source(source_id)
    return {"ok": True}


# ── SCHOLARLY KEYWORDS ────────────────────────────────────────────────────────

@app.get("/scholarly-keywords")
def list_scholarly_keywords():
    return {"keywords": get_scholarly_keywords()}


@app.post("/scholarly-keywords")
def create_scholarly_keyword(body: dict):
    kw = (body.get("keyword") or "").strip()
    if not kw:
        raise HTTPException(status_code=400, detail="keyword required")
    added = add_scholarly_keyword(kw)
    return {"ok": True, "added": added}


@app.patch("/scholarly-keywords/{keyword_id}/toggle")
def toggle_scholarly_keyword_endpoint(keyword_id: int):
    new_state = toggle_scholarly_keyword(keyword_id)
    return {"ok": True, "active": new_state}


@app.delete("/scholarly-keywords/{keyword_id}")
def remove_scholarly_keyword(keyword_id: int):
    delete_scholarly_keyword(keyword_id)
    return {"ok": True}


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

# ── SUBSCRIBERS ───────────────────────────────────────────────────────────────

@app.get("/subscribers")
def list_subscribers():
    return {"subscribers": get_subscribers()}


@app.post("/subscribers")
def create_subscriber(body: dict):
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
def toggle_subscriber_endpoint(subscriber_id: int):
    new_state = toggle_subscriber(subscriber_id)
    return {"ok": True, "active": new_state}


@app.patch("/subscribers/{subscriber_id}")
def edit_subscriber(subscriber_id: int, body: dict):
    allowed = {"name", "role", "active"}
    updates = {k: v for k, v in body.items() if k in allowed}
    if not updates:
        raise HTTPException(status_code=400, detail=f"Allowed fields: {allowed}")
    update_subscriber(subscriber_id, updates)
    return {"ok": True}


@app.delete("/subscribers/{subscriber_id}")
def remove_subscriber_endpoint(subscriber_id: int):
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


@app.post("/digests/send")
def send_digest_email(body: dict):
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
def trigger_scholarly_scrape(background_tasks: BackgroundTasks, body: dict = {}):
    extra_keywords = body.get("keywords", [])
    background_tasks.add_task(_run_scholarly_bg, extra_keywords)
    return {"ok": True, "message": "Scholarly scrape started in background"}


def _run_scholarly_bg(extra_keywords: list):
    from scholarly_scraper import run_scholarly_scrape
    run_scholarly_scrape(extra_keywords=extra_keywords)


@app.patch("/scholarly/{article_id}/read")
def mark_scholarly_read(article_id: int, body: dict = {}):
    from scholarly_scraper import update_scholarly_read
    update_scholarly_read(article_id, body.get("read", True))
    return {"ok": True}


@app.get("/scholarly/{article_id}/for-note")
def get_scholarly_for_note(article_id: int):
    from database import get_scholarly_article_by_id
    article = get_scholarly_article_by_id(article_id)
    if not article:
        raise HTTPException(status_code=404, detail="Research paper not found")
    return article


@app.get("/scholarly/{article_id}")
def get_scholarly_article(article_id: int):


# ── HEALTH ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
