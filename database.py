"""
PolicyPulse Database Layer — SQLite
All data persists in policypulse.db
"""

import sqlite3
import os
from datetime import datetime
from typing import Optional

DB_PATH = os.environ.get("DB_PATH", "policypulse.db")


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    """Create all tables if they don't exist."""
    conn = get_conn()
    cur = conn.cursor()

    cur.executescript("""
        CREATE TABLE IF NOT EXISTS articles (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            title           TEXT NOT NULL,
            url             TEXT NOT NULL,
            url_hash        TEXT UNIQUE NOT NULL,
            source          TEXT NOT NULL,
            jurisdiction    TEXT,
            domain          TEXT,
            relevance       INTEGER DEFAULT 5,
            sentiment       TEXT DEFAULT 'Neutral',
            summary         TEXT,
            why_it_matters  TEXT,
            pub_date        TEXT,
            processed_date  TEXT,
            month_year      TEXT,
            read            INTEGER DEFAULT 0,
            staged          INTEGER DEFAULT 0,
            tags            TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_pub_date    ON articles(pub_date DESC);
        CREATE INDEX IF NOT EXISTS idx_month_year  ON articles(month_year);
        CREATE INDEX IF NOT EXISTS idx_domain      ON articles(domain);
        CREATE INDEX IF NOT EXISTS idx_relevance   ON articles(relevance DESC);

        CREATE TABLE IF NOT EXISTS sources (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            name            TEXT NOT NULL,
            url             TEXT NOT NULL,
            jurisdiction    TEXT,
            active          INTEGER DEFAULT 1,
            last_scraped    TEXT,
            article_count   INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS digests (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            subject         TEXT,
            created_date    TEXT,
            sent_date       TEXT,
            recipient_count INTEGER DEFAULT 0,
            html_content    TEXT,
            token           TEXT UNIQUE
        );

        CREATE TABLE IF NOT EXISTS scrape_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            scraped_at      TEXT,
            articles_added  INTEGER DEFAULT 0,
            errors          TEXT
        );

        CREATE TABLE IF NOT EXISTS watchlist_keywords (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword      TEXT UNIQUE NOT NULL,
            created_date TEXT
        );
    """)

    # Seed sources if empty
    cur.execute("SELECT COUNT(*) FROM sources")
    if cur.fetchone()[0] == 0:
        _seed_sources(cur)

    conn.commit()
    conn.close()


def _seed_sources(cur):
    sources = [
        # Original 8
        ("BC Ministry of Post-Secondary Education", "https://www2.gov.bc.ca/gov/content/education-training/post-secondary-education", "BC"),
        ("Government of Canada — Education", "https://www.canada.ca/en/employment-social-development/news.html", "Federal"),
        ("BC Legislature News", "https://www.leg.bc.ca/parliamentary-business/legislation-debates-proceedings", "BC"),
        ("BC Indigenous Relations & Reconciliation", "https://news.gov.bc.ca/ministries/indigenous-relations-reconciliation", "BC"),
        ("University Affairs Canada", "https://www.universityaffairs.ca/news/", "Federal"),
        ("Burnaby City Hall News", "https://www.burnaby.ca/city-hall/news", "Municipal"),
        ("Higher Education Strategy Associates", "https://higheredstrategy.com/", "Pan-Canadian"),
        ("Innovation, Science and Economic Development Canada", "https://www.canada.ca/en/innovation-science-economic-development/news.html", "Federal"),
        # Additional relevant sources
        ("BC Government Newsroom", "https://news.gov.bc.ca/", "BC"),
        ("SSHRC News", "https://www.sshrc-crsh.gc.ca/news_room-salle_des_nouvelles/latest_news-nouvelles_recentes-eng.aspx", "Federal"),
        ("NSERC News", "https://www.nserc-crsng.gc.ca/Media-Media/NewsReleases-CommuniquesDePresse_eng.asp", "Federal"),
        ("CIHR News", "https://cihr-irsc.gc.ca/e/51999.html", "Federal"),
        ("Universities Canada", "https://www.univcan.ca/media-room/media-releases/", "Federal"),
        ("First Nations Health Authority", "https://www.fnha.ca/about/news-and-events/news", "BC"),
        ("BC First Nations Summit", "https://fns.bc.ca/news/", "BC"),
        ("Crown-Indigenous Relations Canada", "https://www.canada.ca/en/crown-indigenous-relations-northern-affairs/news.html", "Federal"),
        ("Times Higher Education", "https://www.timeshighereducation.com/news", "International"),
        ("Policy Options (IRPP)", "https://policyoptions.irpp.org/", "Federal"),
        ("Maclean's Education", "https://www.macleans.ca/education/", "Federal"),
        ("BC Public Service Agency", "https://www2.gov.bc.ca/gov/content/careers-myhr/about-the-bc-public-service/our-organization", "BC"),
    ]
    cur.executemany(
        "INSERT INTO sources (name, url, jurisdiction) VALUES (?, ?, ?)",
        sources
    )


# ── ARTICLES ──────────────────────────────────────────────

def get_articles(domain=None, jurisdiction=None, sentiment=None,
                 search=None, sort="date", unread_only=False,
                 limit=50, offset=0):
    conn = get_conn()
    cur = conn.cursor()

    conditions = []
    params = []

    if domain:
        conditions.append("domain = ?")
        params.append(domain)
    if jurisdiction:
        conditions.append("jurisdiction = ?")
        params.append(jurisdiction)
    if sentiment:
        conditions.append("sentiment = ?")
        params.append(sentiment)
    if unread_only:
        conditions.append("read = 0")
    if search:
        conditions.append("(title LIKE ? OR summary LIKE ? OR source LIKE ?)")
        s = f"%{search}%"
        params.extend([s, s, s])

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    order = "pub_date DESC" if sort == "date" else "relevance DESC, pub_date DESC"

    query = f"""
        SELECT * FROM articles
        {where}
        ORDER BY {order}
        LIMIT ? OFFSET ?
    """
    params.extend([limit, offset])

    rows = cur.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_article_by_id(article_id):
    conn = get_conn()
    row = conn.execute("SELECT * FROM articles WHERE id = ?", (article_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def save_article(title, url, url_hash, source, jurisdiction, domain,
                 relevance, sentiment, summary, why_it_matters,
                 pub_date, tags=""):
    """Insert article if URL not already in DB. Returns True if inserted."""
    conn = get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat()
    month_year = pub_date[:7] if pub_date and len(pub_date) >= 7 else now[:7]

    try:
        cur.execute("""
            INSERT INTO articles
                (title, url, url_hash, source, jurisdiction, domain,
                 relevance, sentiment, summary, why_it_matters,
                 pub_date, processed_date, month_year, tags)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (title, url, url_hash, source, jurisdiction, domain,
              relevance, sentiment, summary, why_it_matters,
              pub_date, now, month_year, tags))
        conn.commit()
        inserted = True
    except sqlite3.IntegrityError:
        inserted = False  # Duplicate URL
    finally:
        conn.close()

    return inserted


def update_article_read(article_id, read):
    conn = get_conn()
    conn.execute("UPDATE articles SET read = ? WHERE id = ?", (1 if read else 0, article_id))
    conn.commit()
    conn.close()


def update_article_staged(article_id, staged):
    conn = get_conn()
    conn.execute("UPDATE articles SET staged = ? WHERE id = ?", (1 if staged else 0, article_id))
    conn.commit()
    conn.close()


def log_scrape(articles_added, errors=""):
    conn = get_conn()
    conn.execute(
        "INSERT INTO scrape_log (scraped_at, articles_added, errors) VALUES (?,?,?)",
        (datetime.utcnow().isoformat(), articles_added, errors)
    )
    conn.commit()
    conn.close()


def get_last_scrape_time():
    conn = get_conn()
    row = conn.execute("SELECT scraped_at FROM scrape_log ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    return row["scraped_at"] if row else None


def update_source_scraped(source_name, count):
    conn = get_conn()
    conn.execute(
        "UPDATE sources SET last_scraped = ?, article_count = article_count + ? WHERE name = ?",
        (datetime.utcnow().isoformat(), count, source_name)
    )
    conn.commit()
    conn.close()


# ── WATCHLIST KEYWORDS ────────────────────────────────────

def get_watchlist_keywords():
    conn = get_conn()
    rows = conn.execute("SELECT keyword FROM watchlist_keywords ORDER BY id").fetchall()
    conn.close()
    return [r["keyword"] for r in rows]

def add_watchlist_keyword(keyword):
    conn = get_conn()
    try:
        conn.execute("INSERT INTO watchlist_keywords (keyword, created_date) VALUES (?,?)",
                     (keyword.strip(), datetime.utcnow().isoformat()))
        conn.commit()
        result = True
    except sqlite3.IntegrityError:
        result = False
    finally:
        conn.close()
    return result

def remove_watchlist_keyword(keyword):
    conn = get_conn()
    conn.execute("DELETE FROM watchlist_keywords WHERE keyword = ?", (keyword,))
    conn.commit()
    conn.close()

# ── MANUAL TAGS ────────────────────────────────────────────

def add_article_tag(article_id, tag):
    conn = get_conn()
    row = conn.execute("SELECT tags FROM articles WHERE id = ?", (article_id,)).fetchone()
    if row:
        existing = [t.strip() for t in (row["tags"] or "").split(",") if t.strip()]
        if tag not in existing:
            existing.append(tag)
        conn.execute("UPDATE articles SET tags = ? WHERE id = ?", (",".join(existing), article_id))
        conn.commit()
    conn.close()

def remove_article_tag(article_id, tag):
    conn = get_conn()
    row = conn.execute("SELECT tags FROM articles WHERE id = ?", (article_id,)).fetchone()
    if row:
        existing = [t.strip() for t in (row["tags"] or "").split(",") if t.strip() and t.strip() != tag]
        conn.execute("UPDATE articles SET tags = ? WHERE id = ?", (",".join(existing), article_id))
        conn.commit()
    conn.close()

# ── SOURCES ───────────────────────────────────────────────

def get_sources():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM sources ORDER BY jurisdiction, name").fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── STATS ─────────────────────────────────────────────────

def get_stats():
    conn = get_conn()
    cur = conn.cursor()

    total = cur.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    unread = cur.execute("SELECT COUNT(*) FROM articles WHERE read = 0").fetchone()[0]
    staged = cur.execute("SELECT COUNT(*) FROM articles WHERE staged = 1").fetchone()[0]
    this_week = cur.execute(
        "SELECT COUNT(*) FROM articles WHERE pub_date >= date('now', '-7 days')"
    ).fetchone()[0]
    last_scraped = cur.execute(
        "SELECT scraped_at FROM scrape_log ORDER BY id DESC LIMIT 1"
    ).fetchone()

    conn.close()
    return {
        "total": total,
        "unread": unread,
        "staged": staged,
        "this_week": this_week,
        "last_scraped": last_scraped["scraped_at"] if last_scraped else None,
    }


# ── DIGESTS ───────────────────────────────────────────────

def get_digest_history():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM digests ORDER BY id DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def save_digest(subject, html_content, recipients, token):
    conn = get_conn()
    now = datetime.utcnow().isoformat()
    cur = conn.execute(
        "INSERT INTO digests (subject, created_date, sent_date, recipient_count, html_content, token) VALUES (?,?,?,?,?,?)",
        (subject, now, now, recipients, html_content, token)
    )
    digest_id = cur.lastrowid
    conn.commit()
    conn.close()
    return digest_id
