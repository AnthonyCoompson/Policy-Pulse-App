"""
PolicyPulse Backend — FastAPI + SQLite + Gemini AI
Deployed on Railway.app

v2: Added PATCH /articles/{id} for frontend to save regenerated summaries.
    Added POST /digests/send for real email delivery via SMTP.
v3: Added GET /articles/{id}/reader — fetches, sanitises, and returns
    article HTML for the in-app Reader Panel in the Notes tab.
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

app = FastAPI(title="PolicyPulse API", version="3.0.0")

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


# ── ARTICLE READER ────────────────────────────────────────────────────────────

@app.get("/articles/{article_id}/reader")
def get_article_reader(article_id: int):
    """
    Fetch, sanitise, and return the full article HTML for the in-app Reader Panel.

    Returns:
        {
          "html":     "<sanitised article body HTML>",
          "title":    "Article title",
          "pub_date": "2026-04-17",
          "source":   "BC Ministry of Post-Secondary Education",
          "url":      "https://...",
          "favicon":  "https://domain.com/favicon.ico"
        }

    On any fetch/parse failure the endpoint returns ok=False so the frontend
    can fall back gracefully to the cached summary rather than showing an error.
    """
    import re
    import requests as req_lib
    from urllib.parse import urlparse, urljoin
    from bs4 import BeautifulSoup

    # ── 1. Load article record from DB ────────────────────────────────────────
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
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-CA,en;q=0.9",
    }

    # ── 2. Fetch the article page ──────────────────────────────────────────────
    try:
        resp = req_lib.get(
            article_url,
            headers=FETCH_HEADERS,
            timeout=12,
            allow_redirects=True,
        )
        resp.raise_for_status()
        raw_html = resp.text
    except req_lib.exceptions.Timeout:
        return {"ok": False, "reason": "Request timed out — article may be slow to load"}
    except req_lib.exceptions.HTTPError as e:
        return {"ok": False, "reason": f"HTTP {e.response.status_code} from source"}
    except Exception as e:
        return {"ok": False, "reason": f"Could not fetch article: {str(e)[:120]}"}

    # ── 3. Parse and find main content ────────────────────────────────────────
    soup = BeautifulSoup(raw_html, "html.parser")

    # Try content containers in priority order (mirrors scraper.py logic)
    CONTENT_SELECTORS = [
        "article",
        "main",
        "[role='main']",
        ".article-body",
        ".article-content",
        ".entry-content",
        ".post-content",
        ".story-body",
        ".content-body",
        "#content",
        ".page-content",
        ".main-content",
    ]

    content_node = None
    for selector in CONTENT_SELECTORS:
        node = soup.select_one(selector)
        if node and len(node.get_text(strip=True)) > 200:
            content_node = node
            break

    # Last-resort: use body
    if content_node is None:
        content_node = soup.find("body") or soup

    # ── 4. Strip dangerous / noisy tags ───────────────────────────────────────
    STRIP_TAGS = [
        "script", "style", "iframe", "form", "input", "button",
        "nav", "footer", "header", "aside", "advertisement",
        "noscript", "svg", "canvas", "video", "audio",
        # Common noise patterns on gov/news sites
        ".site-header", ".site-footer", ".nav", ".navbar",
        ".sidebar", ".widget", ".cookie-banner", ".newsletter-signup",
        ".social-share", ".related-articles", ".comments",
    ]
    for tag_name in ["script", "style", "iframe", "form", "input", "button",
                     "nav", "footer", "header", "aside", "noscript",
                     "svg", "canvas", "video", "audio"]:
        for el in content_node.find_all(tag_name):
            el.decompose()

    # Strip by class patterns (noise selectors)
    NOISE_CLASS_PATTERNS = [
        "sidebar", "widget", "cookie", "newsletter", "social-share",
        "related", "comment", "advertisement", "ad-", "promo",
        "breadcrumb", "pagination", "share-", "print-", "skip-",
    ]
    for el in content_node.find_all(class_=True):
        classes = " ".join(el.get("class", [])).lower()
        if any(p in classes for p in NOISE_CLASS_PATTERNS):
            el.decompose()

    # ── 5. Strip all inline styles (so reader stylesheet controls appearance) ──
    for el in content_node.find_all(True):
        if el.has_attr("style"):
            del el["style"]
        # Strip event handlers (onclick, onmouseover, etc.)
        attrs_to_remove = [a for a in el.attrs if a.startswith("on")]
        for attr in attrs_to_remove:
            del el[attr]
        # Strip class attributes (let reader CSS control appearance)
        # Keep id so anchor links still work
        if el.has_attr("class"):
            del el["class"]

    # ── 6. Rewrite relative image src → absolute URLs ─────────────────────────
    for img in content_node.find_all("img"):
        src = img.get("src", "")
        if src and not src.startswith(("http://", "https://", "data:")):
            img["src"] = urljoin(base_url, src)
        # Also fix srcset
        srcset = img.get("srcset", "")
        if srcset:
            new_srcset_parts = []
            for part in srcset.split(","):
                part = part.strip()
                if part:
                    tokens = part.split()
                    if tokens and not tokens[0].startswith(("http://", "https://")):
                        tokens[0] = urljoin(base_url, tokens[0])
                    new_srcset_parts.append(" ".join(tokens))
            img["srcset"] = ", ".join(new_srcset_parts)

    # Rewrite relative href links → absolute (so links still work)
    for a_tag in content_node.find_all("a", href=True):
        href = a_tag["href"]
        if href and not href.startswith(("http://", "https://", "#", "mailto:")):
            a_tag["href"] = urljoin(base_url, href)
        # Open all links in new tab
        a_tag["target"] = "_blank"
        a_tag["rel"]    = "noopener noreferrer"

    # ── 7. Extract page title as fallback ─────────────────────────────────────
    page_title = ""
    og_title   = soup.find("meta", property="og:title")
    if og_title and og_title.get("content"):
        page_title = og_title["content"].strip()
    if not page_title:
        h1 = content_node.find("h1")
        if h1:
            page_title = h1.get_text(strip=True)
    if not page_title and soup.title:
        page_title = soup.title.get_text(strip=True)

    # ── 8. Serialise cleaned HTML ──────────────────────────────────────────────
    clean_html = str(content_node)

    # Safety: one final regex pass to remove any remaining script blocks
    clean_html = re.sub(
        r"<script[^>]*>.*?</script>",
        "",
        clean_html,
        flags=re.DOTALL | re.IGNORECASE,
    )

    # Limit size to 400 KB to avoid huge payloads
    if len(clean_html) > 400_000:
        clean_html = clean_html[:400_000] + "<!-- [content truncated] -->"

    return {
        "ok":      True,
        "html":    clean_html,
        "title":   page_title or a.get("title", ""),
        "pub_date": a.get("pub_date") or a.get("pubDate", ""),
        "source":  a.get("source", ""),
        "url":     article_url,
        "favicon": favicon,
        "domain":  a.get("domain", ""),
        "jurisdiction": a.get("jurisdiction", ""),
        "relevance":    a.get("relevance", 0),
        "sentiment":    a.get("sentiment", ""),
        "summary":      a.get("summary", ""),
        "why_it_matters": a.get("why_it_matters") or a.get("whyItMatters", ""),
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
    recipients   = body.get("recipients", [])

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


# ── NOTION PROXY ─────────────────────────────────────────────────────────────

@app.post("/notion/test")
async def notion_test(body: dict):
    """Test a Notion integration token by fetching the bot user."""
    import requests as req_lib
    token = body.get("token","").strip()
    if not token:
        raise HTTPException(status_code=400, detail="token required")
    try:
        r = req_lib.get(
            "https://api.notion.com/v1/users/me",
            headers={
                "Authorization": f"Bearer {token}",
                "Notion-Version": "2022-06-28",
            },
            timeout=10,
        )
        if r.status_code == 200:
            data = r.json()
            workspace = data.get("bot",{}).get("workspace_name","") or data.get("name","Notion")
            return {"ok": True, "workspace": workspace}
        raise HTTPException(status_code=r.status_code, detail=r.json().get("message","Invalid token"))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/notion/push")
async def notion_push(body: dict):
    """Forward a Notion API call from the frontend."""
    import requests as req_lib
    token    = body.get("token","").strip()
    endpoint = body.get("endpoint","").strip().lstrip("/")
    payload  = body.get("payload", {})

    if not token:
        raise HTTPException(status_code=400, detail="token required")
    if not endpoint:
        raise HTTPException(status_code=400, detail="endpoint required")

    allowed_prefixes = ("pages", "databases/", "blocks/", "search")
    if not any(endpoint.startswith(p) for p in allowed_prefixes):
        raise HTTPException(status_code=400, detail=f"endpoint not allowed: {endpoint}")

    try:
        if "properties" in payload:
            payload["properties"] = {k: v for k, v in payload["properties"].items() if v is not None}

        r = req_lib.post(
            f"https://api.notion.com/v1/{endpoint}",
            headers={
                "Authorization": f"Bearer {token}",
                "Notion-Version": "2022-06-28",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=15,
        )
        if r.status_code in (200, 201):
            return {"ok": True, **r.json()}
        detail = r.json().get("message", r.text[:200])
        raise HTTPException(status_code=r.status_code, detail=detail)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── SCHOLARLY / RESEARCH ──────────────────────────────────────────────────────

@app.get("/scholarly")
def list_scholarly(
    domain:        str  = Query(None),
    database_name: str  = Query(None),
    search:        str  = Query(None),
    sort:          str  = Query("date"),
    limit:         int  = Query(50),
    offset:        int  = Query(0),
):
    try:
        from scholarly_scraper import get_scholarly_articles, ensure_scholarly_table
        ensure_scholarly_table()
        articles = get_scholarly_articles(
            domain=domain, database_name=database_name,
            search=search, sort=sort, limit=limit, offset=offset,
        )
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


@app.get("/scholarly/{article_id}")
def get_scholarly_article(article_id: int):
    from database import get_conn
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM scholarly_articles WHERE id = ?", (article_id,)
    ).fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Article not found")
    return dict(row)


# ── HEALTH ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
