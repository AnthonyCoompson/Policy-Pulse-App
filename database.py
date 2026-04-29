"""
PolicyPulse Database Layer — SQLite
All data persists in policypulse.db

v2: Added full CRUD for sources (add, toggle, delete, update scrape_type).
    Added research_sources table with full CRUD.
    Added scholarly keyword management.
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
            scrape_type     TEXT DEFAULT 'html',
            active          INTEGER DEFAULT 1,
            last_scraped    TEXT,
            article_count   INTEGER DEFAULT 0
        );

         CREATE TABLE IF NOT EXISTS scholarly_articles (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            title           TEXT NOT NULL,
            url             TEXT NOT NULL,
            url_hash        TEXT UNIQUE NOT NULL,
            source          TEXT, -- The Journal or Publisher
            database_name   TEXT, -- e.g., OpenAlex, Semantic Scholar
            authors         TEXT,
            relevance       INTEGER DEFAULT 5,
            sentiment       TEXT DEFAULT 'Neutral',
            summary         TEXT,
            abstract        TEXT,
            why_it_matters  TEXT,
            pub_date        TEXT,
            processed_date  TEXT,
            read            INTEGER DEFAULT 0,
            tags            TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_scholarly_pub_date ON scholarly_articles(pub_date DESC);
        CREATE INDEX IF NOT EXISTS idx_scholarly_hash     ON scholarly_articles(url_hash);

        CREATE TABLE IF NOT EXISTS research_sources (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            name            TEXT NOT NULL,
            url             TEXT NOT NULL,
            source_type     TEXT NOT NULL DEFAULT 'think_tank',
            active          INTEGER DEFAULT 1,
            relevance_boost INTEGER DEFAULT 0,
            last_scraped    TEXT,
            article_count   INTEGER DEFAULT 0,
            notes           TEXT
        );

        CREATE TABLE IF NOT EXISTS scholarly_keywords (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword      TEXT UNIQUE NOT NULL,
            active       INTEGER DEFAULT 1,
            created_date TEXT
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

         CREATE TABLE IF NOT EXISTS subscribers (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL,
            email       TEXT UNIQUE NOT NULL,
            role        TEXT DEFAULT 'Reader',
            active      INTEGER DEFAULT 1,
            added_date  TEXT
        );
                      
        CREATE TABLE IF NOT EXISTS watchlist_keywords (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword      TEXT UNIQUE NOT NULL,
            created_date TEXT
        );
        
        CREATE TABLE IF NOT EXISTS exclusion_keywords (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword      TEXT UNIQUE NOT NULL,
            created_date TEXT
        );

        CREATE TABLE IF NOT EXISTS scholarly_exclusion_keywords (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword      TEXT UNIQUE NOT NULL,
            created_date TEXT
        );
    """)

    # Seed sources if empty
    cur.execute("SELECT COUNT(*) FROM sources")
    if cur.fetchone()[0] == 0:
        _seed_sources(cur)

    # Seed research sources if empty
    cur.execute("SELECT COUNT(*) FROM research_sources")
    if cur.fetchone()[0] == 0:
        _seed_research_sources(cur)

    # Seed scholarly keywords if empty
    cur.execute("SELECT COUNT(*) FROM scholarly_keywords")
    if cur.fetchone()[0] == 0:
        _seed_scholarly_keywords(cur)

    conn.commit()
    conn.close()


def _seed_sources(cur):
    # scrape_type: 'rss' uses RSS parser, 'html' uses generic HTML scraper
    sources = [
        ("BC Ministry of Post-Secondary Education",    "https://news.gov.bc.ca/ministries/post-secondary-education-and-future-skills", "BC",            "html"),
        ("Government of Canada — Education",           "https://www.canada.ca/en/employment-social-development/news.html",              "Federal",        "html"),
        ("BC Legislature News",                        "https://www.leg.bc.ca/parliamentary-business/legislation-debates-proceedings",   "BC",            "html"),
        ("BC Indigenous Relations & Reconciliation",   "https://news.gov.bc.ca/ministries/indigenous-relations-reconciliation",          "BC",            "html"),
        ("University Affairs Canada",                  "https://www.universityaffairs.ca/feed/",                                         "Federal",        "rss"),
        ("Burnaby City Hall News",                     "https://www.burnaby.ca/city-hall/news",                                          "Municipal",      "html"),
        ("Higher Education Strategy Associates",       "https://higheredstrategy.com/feed/",                                             "Pan-Canadian",   "rss"),
        ("Innovation Science and Economic Development","https://www.canada.ca/en/innovation-science-economic-development/news.html",     "Federal",        "html"),
        ("BC Government Newsroom",                     "https://news.gov.bc.ca/",                                                        "BC",            "html"),
        ("SSHRC News",                                 "https://www.sshrc-crsh.gc.ca/news_room-salle_des_nouvelles/latest_news-nouvelles_recentes-eng.aspx", "Federal", "html"),
        ("NSERC News",                                 "https://www.nserc-crsng.gc.ca/Media-Media/NewsReleases-CommuniquesDePresse_eng.asp", "Federal",   "html"),
        ("CIHR News",                                  "https://cihr-irsc.gc.ca/e/51999.html",                                           "Federal",        "html"),
        ("Universities Canada",                        "https://www.univcan.ca/feed/",                                                   "Federal",        "rss"),
        ("First Nations Health Authority",             "https://www.fnha.ca/about/news-and-events/news",                                 "BC",            "html"),
        ("BC First Nations Summit",                    "https://fns.bc.ca/news/",                                                        "BC",            "html"),
        ("Crown-Indigenous Relations Canada",          "https://www.canada.ca/en/crown-indigenous-relations-northern-affairs/news.html", "Federal",        "html"),
        ("Times Higher Education",                     "https://www.timeshighereducation.com/rss.xml",                                   "International",  "rss"),
        ("Policy Options (IRPP)",                      "https://policyoptions.irpp.org/feed/",                                           "Federal",        "rss"),
        ("Maclean's Education",                        "https://www.macleans.ca/education/feed/",                                        "Federal",        "rss"),
        ("BC Public Service Agency",                   "https://www2.gov.bc.ca/gov/content/careers-myhr/about-the-bc-public-service/our-organization", "BC", "html"),
    ]
    cur.executemany(
        "INSERT INTO sources (name, url, jurisdiction, scrape_type) VALUES (?,?,?,?)",
        sources
    )


def _seed_research_sources(cur):
    """Seed the default Canadian think-tank research sources."""
    sources = [
        ("Canadian Centre for Policy Alternatives", "https://www.policyalternatives.ca/publications", "think_tank", 1, 1),
        ("Yellowhead Institute",                    "https://yellowheadinstitute.org/resources/",      "think_tank", 1, 2),
        ("National Collaborating Centre for Indigenous Health", "https://www.nccih.ca/495/Publications_and_Resources.nccih", "think_tank", 1, 2),
        ("Macdonald-Laurier Institute",             "https://macdonaldlaurier.ca/publications/",       "think_tank", 1, 0),
        ("CD Howe Institute",                       "https://www.cdhowe.org/intelligence-memos",       "think_tank", 1, 0),
        ("Broadbent Institute",                     "https://www.broadbentinstitute.ca/research",      "think_tank", 1, 0),
    ]
    cur.executemany(
        "INSERT INTO research_sources (name, url, source_type, active, relevance_boost) VALUES (?,?,?,?,?)",
        sources
    )


def _seed_scholarly_keywords(cur):
    """Seed default scholarly search keywords."""
    keywords = [
        "Indigenous policy Canada",
        "DRIPA UNDRIP reconciliation",
        "First Nations health Canada",
        "post-secondary education Canada",
        "research funding SSHRC NSERC",
        "pharmacare Canada",
        "BC government policy",
        "OCAP data sovereignty Indigenous",
        "TRC calls to action",
        "Indigenous higher education",
    ]
    now = datetime.utcnow().isoformat()
    cur.executemany(
        "INSERT INTO scholarly_keywords (keyword, active, created_date) VALUES (?,1,?)",
        [(kw, now) for kw in keywords]
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


def update_article_content(article_id: int, fields: dict):
    allowed = {"summary", "why_it_matters"}
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return
    conn = get_conn()
    set_clause = ", ".join(f"{k} = ?" for k in updates)
    values = list(updates.values()) + [article_id]
    conn.execute(f"UPDATE articles SET {set_clause} WHERE id = ?", values)
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


# ── SOURCES — full CRUD ────────────────────────────────────

def get_sources():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM sources ORDER BY jurisdiction, name").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def add_source(name: str, url: str, jurisdiction: str, scrape_type: str = "html") -> int:
    conn = get_conn()
    cur = conn.execute(
        "INSERT INTO sources (name, url, jurisdiction, scrape_type, active) VALUES (?,?,?,?,1)",
        (name.strip(), url.strip(), jurisdiction.strip(), scrape_type)
    )
    new_id = cur.lastrowid
    conn.commit()
    conn.close()
    return new_id


def toggle_source(source_id: int) -> bool:
    """Flip active flag. Returns new active state."""
    conn = get_conn()
    row = conn.execute("SELECT active FROM sources WHERE id = ?", (source_id,)).fetchone()
    if not row:
        conn.close()
        return False
    new_state = 0 if row["active"] else 1
    conn.execute("UPDATE sources SET active = ? WHERE id = ?", (new_state, source_id))
    conn.commit()
    conn.close()
    return bool(new_state)


def delete_source(source_id: int):
    conn = get_conn()
    conn.execute("DELETE FROM sources WHERE id = ?", (source_id,))
    conn.commit()
    conn.close()


def update_source(source_id: int, fields: dict):
    allowed = {"name", "url", "jurisdiction", "scrape_type", "active"}
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return
    conn = get_conn()
    set_clause = ", ".join(f"{k} = ?" for k in updates)
    values = list(updates.values()) + [source_id]
    conn.execute(f"UPDATE sources SET {set_clause} WHERE id = ?", values)
    conn.commit()
    conn.close()


# ── RESEARCH SOURCES — full CRUD ──────────────────────────

def get_research_sources():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM research_sources ORDER BY name").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def add_research_source(name: str, url: str, source_type: str = "think_tank",
                         relevance_boost: int = 0, notes: str = "") -> int:
    conn = get_conn()
    cur = conn.execute(
        """INSERT INTO research_sources
           (name, url, source_type, active, relevance_boost, notes)
           VALUES (?,?,?,1,?,?)""",
        (name.strip(), url.strip(), source_type, int(relevance_boost), notes.strip())
    )
    new_id = cur.lastrowid
    conn.commit()
    conn.close()
    return new_id


def toggle_research_source(source_id: int) -> bool:
    conn = get_conn()
    row = conn.execute("SELECT active FROM research_sources WHERE id = ?", (source_id,)).fetchone()
    if not row:
        conn.close()
        return False
    new_state = 0 if row["active"] else 1
    conn.execute("UPDATE research_sources SET active = ? WHERE id = ?", (new_state, source_id))
    conn.commit()
    conn.close()
    return bool(new_state)


def delete_research_source(source_id: int):
    conn = get_conn()
    conn.execute("DELETE FROM research_sources WHERE id = ?", (source_id,))
    conn.commit()
    conn.close()


def update_research_source(source_id: int, fields: dict):
    allowed = {"name", "url", "source_type", "active", "relevance_boost", "notes"}
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return
    conn = get_conn()
    set_clause = ", ".join(f"{k} = ?" for k in updates)
    values = list(updates.values()) + [source_id]
    conn.execute(f"UPDATE research_sources SET {set_clause} WHERE id = ?", values)
    conn.commit()
    conn.close()


def update_research_source_scraped(source_id: int, count: int):
    conn = get_conn()
    conn.execute(
        "UPDATE research_sources SET last_scraped = ?, article_count = article_count + ? WHERE id = ?",
        (datetime.utcnow().isoformat(), count, source_id)
    )
    conn.commit()
    conn.close()


# ── SCHOLARLY ARTICLES ────────────────────────────────────

def get_scholarly_article_by_id(article_id: int):
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM scholarly_articles WHERE id = ?", (article_id,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None

# ── SCHOLARLY KEYWORDS ────────────────────────────────────

def get_scholarly_keywords():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM scholarly_keywords ORDER BY id").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def add_scholarly_keyword(keyword: str) -> bool:
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO scholarly_keywords (keyword, active, created_date) VALUES (?,1,?)",
            (keyword.strip(), datetime.utcnow().isoformat())
        )
        conn.commit()
        result = True
    except sqlite3.IntegrityError:
        result = False
    finally:
        conn.close()
    return result


def delete_scholarly_keyword(keyword_id: int):
    conn = get_conn()
    conn.execute("DELETE FROM scholarly_keywords WHERE id = ?", (keyword_id,))
    conn.commit()
    conn.close()


def toggle_scholarly_keyword(keyword_id: int) -> bool:
    conn = get_conn()
    row = conn.execute("SELECT active FROM scholarly_keywords WHERE id = ?", (keyword_id,)).fetchone()
    if not row:
        conn.close()
        return False
    new_state = 0 if row["active"] else 1
    conn.execute("UPDATE scholarly_keywords SET active = ? WHERE id = ?", (new_state, keyword_id))
    conn.commit()
    conn.close()
    return bool(new_state)


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

def update_article_sentiment(article_id, sentiment):
    conn = get_conn()
    conn.execute("UPDATE articles SET sentiment = ? WHERE id = ?", (sentiment, article_id))
    conn.commit()
    conn.close()


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

# ── SUBSCRIBERS ───────────────────────────────────────────

def get_subscribers():
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM subscribers ORDER BY name"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def add_subscriber(name: str, email: str, role: str = "Reader") -> int:
    conn = get_conn()
    now = datetime.utcnow().isoformat()
    cur = conn.execute(
        "INSERT INTO subscribers (name, email, role, active, added_date) VALUES (?,?,?,1,?)",
        (name.strip(), email.strip().lower(), role.strip(), now)
    )
    new_id = cur.lastrowid
    conn.commit()
    conn.close()
    return new_id


def toggle_subscriber(subscriber_id: int) -> bool:
    conn = get_conn()
    row = conn.execute(
        "SELECT active FROM subscribers WHERE id = ?", (subscriber_id,)
    ).fetchone()
    if not row:
        conn.close()
        return False
    new_state = 0 if row["active"] else 1
    conn.execute(
        "UPDATE subscribers SET active = ? WHERE id = ?", (new_state, subscriber_id)
    )
    conn.commit()
    conn.close()
    return bool(new_state)


def delete_subscriber(subscriber_id: int):
    conn = get_conn()
    conn.execute("DELETE FROM subscribers WHERE id = ?", (subscriber_id,))
    conn.commit()
    conn.close()


def update_subscriber(subscriber_id: int, fields: dict):
    allowed = {"name", "role", "active"}
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return
    conn = get_conn()
    set_clause = ", ".join(f"{k} = ?" for k in updates)
    values = list(updates.values()) + [subscriber_id]
    conn.execute(
        f"UPDATE subscribers SET {set_clause} WHERE id = ?", values
    )
    conn.commit()
    conn.close()

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

# ── EXCLUSION KEYWORDS (News Scraper) ─────────────────────────────────────────

def get_exclusion_keywords() -> list[str]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT keyword FROM exclusion_keywords ORDER BY keyword"
    ).fetchall()
    conn.close()
    return [r["keyword"] for r in rows]


def add_exclusion_keyword(keyword: str) -> bool:
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO exclusion_keywords (keyword, created_date) VALUES (?,?)",
            (keyword.strip().lower(), datetime.utcnow().isoformat())
        )
        conn.commit()
        result = True
    except sqlite3.IntegrityError:
        result = False
    finally:
        conn.close()
    return result


def remove_exclusion_keyword(keyword: str):
    conn = get_conn()
    conn.execute(
        "DELETE FROM exclusion_keywords WHERE keyword = ?",
        (keyword.strip().lower(),)
    )
    conn.commit()
    conn.close()

# ── EXCLUSION KEYWORDS (Research Scraper) ─────────────────────────────────────

def get_scholarly_exclusion_keywords() -> list[str]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT keyword FROM scholarly_exclusion_keywords ORDER BY keyword"
    ).fetchall()
    conn.close()
    return [r["keyword"] for r in rows]


def add_scholarly_exclusion_keyword(keyword: str) -> bool:
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO scholarly_exclusion_keywords (keyword, created_date) VALUES (?,?)",
            (keyword.strip().lower(), datetime.utcnow().isoformat())
        )
        conn.commit()
        result = True
    except sqlite3.IntegrityError:
        result = False
    finally:
        conn.close()
    return result


def remove_scholarly_exclusion_keyword(keyword: str):
    conn = get_conn()
    conn.execute(
        "DELETE FROM scholarly_exclusion_keywords WHERE keyword = ?",
        (keyword.strip().lower(),)
    )
    conn.commit()
    conn.close()