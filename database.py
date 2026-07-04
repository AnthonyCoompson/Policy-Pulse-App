"""
PolicyPulse Database Layer — SQLite / Turso (libSQL)
All data persists in policypulse.db, or in a hosted Turso database when
TURSO_URL is configured (persists across Render redeploys, unlike the
ephemeral local disk).

v2: Added full CRUD for sources (add, toggle, delete, update scrape_type).
    Added research_sources table with full CRUD.
    Added scholarly keyword management.
v3: Added optional Turso (libSQL) backend. When TURSO_URL is set, get_conn()
    returns a _LibsqlConnection wrapper instead of a raw sqlite3.Connection.
    The wrapper exists because the libsql_experimental driver's rows are
    plain tuples (not name-addressable like sqlite3.Row), it has no native
    executescript()/close() on older builds, list-typed params crash it, and
    UNIQUE/NOT NULL violations surface as a bare ValueError instead of
    sqlite3.IntegrityError. The wrapper normalises all of that so every
    existing dict(row), row["col"], cur.executemany(...), and
    except sqlite3.IntegrityError: call site elsewhere in this file (and in
    main.py / scholarly_scraper.py, which import get_conn() directly)
    continues to work unmodified regardless of which backend is active.
"""

import sqlite3
import os
from datetime import datetime

DB_PATH = os.environ.get("DB_PATH", "policypulse.db")

# ── Turso / libSQL connection ─────────────────────────────────────────────────
# If TURSO_URL is set, use the hosted Turso database (persists across Render
# redeploys). Otherwise fall back to local SQLite (development / first run /
# TURSO_URL not configured yet).
TURSO_URL   = os.environ.get("TURSO_URL", "")
TURSO_TOKEN = os.environ.get("TURSO_TOKEN", "")

_use_turso = bool(TURSO_URL)

if _use_turso:
    import libsql_experimental as libsql


class _LibsqlRow:
    """Minimal stand-in for sqlite3.Row.

    libsql_experimental returns query results as plain tuples with no
    column-name access. Every read function in this codebase (and in
    main.py / scholarly_scraper.py) does dict(row) and/or row["column"],
    so without this adapter the entire app would break the moment a query
    ran against Turso. Supports both string-key and positional-index access,
    plus .keys() so dict(row) works exactly like it does for sqlite3.Row.
    """
    __slots__ = ("_cols", "_vals")

    def __init__(self, cols, vals):
        self._cols = cols
        self._vals = vals

    def keys(self):
        return list(self._cols)

    def __getitem__(self, key):
        if isinstance(key, str):
            try:
                return self._vals[self._cols.index(key)]
            except ValueError:
                raise KeyError(key)
        return self._vals[key]

    def __iter__(self):
        return iter(self._vals)

    def __len__(self):
        return len(self._vals)

    def __contains__(self, key):
        return key in self._cols

    def __repr__(self):
        return f"<Row {dict(zip(self._cols, self._vals))}>"


def _as_integrity_error(e: ValueError) -> Exception:
    """libsql_experimental raises a bare ValueError for UNIQUE / NOT NULL
    constraint violations (e.g. 'UNIQUE constraint failed: articles.url_hash')
    instead of sqlite3.IntegrityError. Every duplicate-detection call site in
    this codebase — save_article, add_watchlist_keyword, add_subscriber,
    add_exclusion_keyword, save_scholarly_article, etc. — catches
    sqlite3.IntegrityError specifically. Re-raising as the real
    sqlite3.IntegrityError means none of those call sites need to change.
    """
    msg = str(e)
    if "unique" in msg.lower() or "constraint" in msg.lower():
        return sqlite3.IntegrityError(msg)
    return e


def _coerce_params(params):
    """libsql_experimental's execute() only accepts a tuple for parameters —
    it raises TypeError on a plain list. This codebase frequently builds
    params as a list (params = []; params.append(...); params.extend(...))
    before passing it straight to cur.execute(query, params). Coercing here
    keeps every call site unchanged."""
    if params is None:
        return ()
    if isinstance(params, tuple):
        return params
    return tuple(params)


class _LibsqlCursor:
    """Wraps a raw libsql_experimental cursor so it behaves like a
    sqlite3.Cursor: row results are _LibsqlRow objects, list params work,
    and constraint violations come back as sqlite3.IntegrityError."""
    __slots__ = ("_cur",)

    def __init__(self, raw_cursor):
        self._cur = raw_cursor

    @property
    def rowcount(self):
        return getattr(self._cur, "rowcount", -1)

    @property
    def lastrowid(self):
        return getattr(self._cur, "lastrowid", None)

    @property
    def description(self):
        return self._cur.description

    def _columns(self):
        return [d[0] for d in (self._cur.description or [])]

    def execute(self, sql, params=()):
        try:
            self._cur.execute(sql, _coerce_params(params))
        except ValueError as e:
            raise _as_integrity_error(e)
        return self

    def executemany(self, sql, seq_of_params):
        try:
            rows = [_coerce_params(p) for p in seq_of_params]
            if hasattr(self._cur, "executemany"):
                self._cur.executemany(sql, rows)
            else:
                for row in rows:
                    self._cur.execute(sql, row)
        except ValueError as e:
            raise _as_integrity_error(e)
        return self

    def executescript(self, script: str):
        # Executed one statement at a time rather than relying on the
        # driver's native batch execution, since that path is untested over
        # a live remote Turso connection here. This exactly mirrors how
        # every other query in this app already talks to the database (one
        # statement per round trip), so it's the lowest-risk option.
        for stmt in script.split(";"):
            stmt = stmt.strip()
            if stmt:
                self._cur.execute(stmt)
        return self

    def fetchall(self):
        cols = self._columns()
        return [_LibsqlRow(cols, row) for row in self._cur.fetchall()]

    def fetchone(self):
        cols = self._columns()
        row = self._cur.fetchone()
        return _LibsqlRow(cols, row) if row is not None else None

    def close(self):
        close_fn = getattr(self._cur, "close", None)
        if close_fn:
            close_fn()


class _LibsqlConnection:
    """sqlite3.Connection-compatible wrapper around a libsql_experimental
    connection. See module docstring for why this exists."""

    def __init__(self, raw_conn):
        self._conn = raw_conn
        self.row_factory = None  # accepted for API compatibility; unused —
                                  # rows are always _LibsqlRow already

    def execute(self, sql, params=()):
        try:
            raw_cursor = self._conn.execute(sql, _coerce_params(params))
        except ValueError as e:
            raise _as_integrity_error(e)
        return _LibsqlCursor(raw_cursor)

    def executemany(self, sql, seq_of_params):
        try:
            rows = [_coerce_params(p) for p in seq_of_params]
            if hasattr(self._conn, "executemany"):
                self._conn.executemany(sql, rows)
            else:
                for row in rows:
                    self._conn.execute(sql, row)
        except ValueError as e:
            raise _as_integrity_error(e)

    def executescript(self, script: str):
        for stmt in script.split(";"):
            stmt = stmt.strip()
            if stmt:
                self._conn.execute(stmt)
        self._conn.commit()

    def cursor(self):
        return _LibsqlCursor(self._conn.cursor())

    def commit(self):
        self._conn.commit()

    def rollback(self):
        rb = getattr(self._conn, "rollback", None)
        if rb:
            rb()

    def close(self):
        # Older libsql_experimental builds have no close() at all — treat
        # it as a no-op rather than letting AttributeError take down every
        # function in this file (and in main.py / scholarly_scraper.py,
        # which all call conn.close() after every operation).
        close_fn = getattr(self._conn, "close", None)
        if close_fn:
            close_fn()


def get_conn():
    if _use_turso:
        raw = libsql.connect(database=TURSO_URL, auth_token=TURSO_TOKEN)
        return _LibsqlConnection(raw)
    else:
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
            url             TEXT NOT NULL UNIQUE,
            jurisdiction    TEXT,
            scrape_type     TEXT DEFAULT 'html',
            active          INTEGER DEFAULT 1,
            last_scraped    TEXT,
            article_count   INTEGER DEFAULT 0,
            max_pages       INTEGER DEFAULT 1,
            pagination_style TEXT DEFAULT 'auto'
        );

        -- Tombstone table: records URLs the user has explicitly deleted.
        -- The _ensure_sources migration checks this table before every INSERT
        -- so deleted sources are never resurrected on Render restarts.
        CREATE TABLE IF NOT EXISTS deleted_sources (
            url         TEXT PRIMARY KEY,
            deleted_at  TEXT NOT NULL DEFAULT (datetime('now'))
        );

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

        CREATE INDEX IF NOT EXISTS idx_scholarly_pub_date  ON scholarly_articles(pub_date DESC);
        CREATE INDEX IF NOT EXISTS idx_scholarly_hash      ON scholarly_articles(url_hash);
        CREATE INDEX IF NOT EXISTS idx_scholarly_relevance ON scholarly_articles(relevance DESC);
        CREATE INDEX IF NOT EXISTS idx_scholarly_domain    ON scholarly_articles(domain);

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
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            name                  TEXT NOT NULL,
            email                 TEXT UNIQUE NOT NULL,
            role                  TEXT DEFAULT 'Reader',
            active                INTEGER DEFAULT 1,
            added_date            TEXT,
            urgent_alerts         INTEGER DEFAULT 0,
            keyword_alerts        INTEGER DEFAULT 0,
            urgent_min_relevance  INTEGER DEFAULT 9
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

        CREATE TABLE IF NOT EXISTS scraper_config (
            key   TEXT PRIMARY KEY NOT NULL,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS policy_trackers (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            name         TEXT NOT NULL,
            description  TEXT DEFAULT '',
            domain       TEXT DEFAULT '',
            status       TEXT DEFAULT 'Active',
            keywords     TEXT DEFAULT '',
            created_date TEXT,
            updated_date TEXT
        );

        CREATE TABLE IF NOT EXISTS tracker_articles (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            tracker_id   INTEGER NOT NULL REFERENCES policy_trackers(id) ON DELETE CASCADE,
            article_id   INTEGER,
            article_type TEXT DEFAULT 'news',
            added_date   TEXT,
            note         TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS tracker_events (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            tracker_id INTEGER NOT NULL REFERENCES policy_trackers(id) ON DELETE CASCADE,
            title      TEXT NOT NULL,
            event_date TEXT,
            note       TEXT DEFAULT '',
            created_date TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_tracker_articles_tracker ON tracker_articles(tracker_id);
        CREATE INDEX IF NOT EXISTS idx_tracker_events_tracker ON tracker_events(tracker_id);
    """)

    # Seed sources if empty
    cur.execute("SELECT COUNT(*) FROM sources")
    if cur.fetchone()[0] == 0:
        _seed_sources(cur)

    # ── Migration: remove academia sources and name-based duplicates ─────────────
    # Anthony confirmed these should be removed — not relevant to HSA BC:
    # University Affairs, Higher Ed Strategy, Maclean's Education,
    # Times Higher Education, Universities Canada, SSHRC, NSERC, CFHI, CIHR.
    # Also deduplicate by name: screenshots show "CIHI", "Health Canada News",
    # "Mental Health Commission" appearing 2-3x with different URLs because
    # multiple migration passes inserted them. Keep the row with highest article_count.
    try:
        _REMOVE_NAME_PATTERNS = [
            "%University Affairs%",
            "%Higher Education Strategy%",
            "%Maclean%Education%",
            "%Times Higher Education%",
            "%Universities Canada%",
            "%SSHRC%",
            "%NSERC%",
            "%CFHI%",
            "%CIHR%",
            "%Burnaby%",          # municipal, not health sector relevant
        ]
        for pattern in _REMOVE_NAME_PATTERNS:
            cur.execute("DELETE FROM sources WHERE name LIKE ?", (pattern,))

        # Name-based dedup: for sources with same name, keep highest article_count
        name_groups = cur.execute("""
            SELECT name, COUNT(*) as n FROM sources GROUP BY name HAVING n > 1
        """).fetchall()
        for grp in name_groups:
            rows = cur.execute(
                "SELECT id, article_count FROM sources WHERE name = ? ORDER BY article_count DESC, id ASC",
                (grp["name"],)
            ).fetchall()
            for row in rows[1:]:
                cur.execute("DELETE FROM sources WHERE id = ?", (row["id"],))
        conn.commit()
    except Exception as e:
        import logging as _cleanup_log
        _cleanup_log.getLogger(__name__).warning(f"Source cleanup migration error: {e}")

    # ── Migration: remove duplicate source rows by URL ─────────────────────────
    # Root cause of the duplicate-source bug: every backend restart previously
    # ran `INSERT OR IGNORE INTO sources (...)` for a fixed list of sources, but
    # the table had no UNIQUE constraint on name or url. INSERT OR IGNORE only
    # skips a row when a UNIQUE/PRIMARY KEY constraint would be violated — with
    # none present, every single boot inserted a fresh duplicate row. On Render's
    # free tier (which restarts on every deploy and after idle spin-down), this
    # silently multiplied "BC Centre on Substance Use" etc. into 5+ copies.
    #
    # Fix has two parts:
    #   1. One-time cleanup below: group existing rows by url, keep the row with
    #      the highest article_count (most "established" copy — preserves scrape
    #      history rather than arbitrarily keeping row #1), delete the rest.
    #   2. A UNIQUE index on sources.url so this can never recur — every future
    #      INSERT OR IGNORE will correctly no-op against an existing URL instead
    #      of creating a duplicate. (CREATE TABLE's inline UNIQUE only applies to
    #      brand-new databases; a CREATE UNIQUE INDEX is what retroactively
    #      enforces it on a table that already existed before this migration.)
    try:
        dupe_groups = cur.execute("""
            SELECT url, COUNT(*) as n FROM sources GROUP BY url HAVING n > 1
        """).fetchall()
        total_removed = 0
        for grp in dupe_groups:
            dupe_url = grp["url"]
            rows = cur.execute(
                "SELECT id, article_count, last_scraped FROM sources WHERE url = ? ORDER BY article_count DESC, id ASC",
                (dupe_url,)
            ).fetchall()
            if len(rows) <= 1:
                continue
            keep_id = rows[0]["id"]
            remove_ids = [r["id"] for r in rows[1:]]
            for rid in remove_ids:
                cur.execute("DELETE FROM sources WHERE id = ?", (rid,))
            total_removed += len(remove_ids)
        if total_removed:
            import logging as _dedupe_log
            _dedupe_log.getLogger(__name__).info(
                f"[startup migration] Removed {total_removed} duplicate source rows across {len(dupe_groups)} URLs"
            )
        conn.commit()
    except Exception as e:
        import logging as _dedupe_log
        _dedupe_log.getLogger(__name__).warning(f"Source dedup migration error: {e}")

    try:
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_sources_url_unique ON sources(url)")
        conn.commit()
    except Exception as e:
        # If this fails it usually means a duplicate slipped through the dedup
        # pass above (e.g. a URL with trailing-slash variants) — non-fatal,
        # dedup will catch it again next boot once the offending row is fixed.
        import logging as _dedupe_log
        _dedupe_log.getLogger(__name__).warning(f"Could not create unique index on sources.url: {e}")

    # Seed research sources if empty
    cur.execute("SELECT COUNT(*) FROM research_sources")
    if cur.fetchone()[0] == 0:
        _seed_research_sources(cur)

    # Seed scholarly keywords if empty
    cur.execute("SELECT COUNT(*) FROM scholarly_keywords")
    if cur.fetchone()[0] == 0:
        _seed_scholarly_keywords(cur)

    conn.commit()

    # ── Migration: add alert columns to subscribers if not present ────────────
    # Safe to run on every startup — ALTER TABLE IF NOT EXISTS column is
    # not valid SQLite syntax, so we use a try/except per column instead.
    cur = conn.cursor()
    for col, default in [("urgent_alerts", "0"), ("keyword_alerts", "0"), ("urgent_min_relevance", "9")]:
        try:
            cur.execute(f"ALTER TABLE subscribers ADD COLUMN {col} INTEGER DEFAULT {default}")
            conn.commit()
        except Exception:
            pass  # column already exists

    # ── Migration: add scrape_type column to scrape_log ────────────────────────
    # Distinguishes news scraper runs from scholarly/research scraper runs so
    # the News tab's Scrape Log and the Research tab's Research Log can each
    # show only their own history instead of an identical, undifferentiated
    # feed. Existing rows (pre-migration) are backfilled as 'news' since the
    # scholarly scraper never wrote to this table before this change.
    try:
        cur.execute("ALTER TABLE scrape_log ADD COLUMN scrape_type TEXT DEFAULT 'news'")
        conn.commit()
        cur.execute("UPDATE scrape_log SET scrape_type = 'news' WHERE scrape_type IS NULL")
        conn.commit()
    except Exception:
        pass  # column already exists

    # ── Migration: add pagination support to sources ────────────────────────────
    # max_pages: how many listing pages the scraper will attempt for this source
    #   (default 1 — no pagination, matches all pre-existing behaviour exactly).
    # pagination_style: which URL pattern to use when building page 2+.
    #   'auto'   — try path-style then query-style then offset-style, use whichever
    #              returns new (not-yet-seen) article links
    #   'path'   — https://site.com/news/page/2/   (WordPress, BCCSU)
    #   'query'  — https://site.com/news?page=2     (Drupal, generic CMS)
    #   'offset' — https://site.com/news?start=10   (Canada.ca / GCWeb, item-count offset)
    #   'none'   — never paginate this source (RSS feeds, single-page sites)
    # RSS sources are explicitly set to 'none' since RSS feeds don't paginate
    # the way HTML listing pages do — the feed itself defines how many items
    # it returns.
    for col, ddl in [
        ("max_pages",        "ALTER TABLE sources ADD COLUMN max_pages INTEGER DEFAULT 1"),
        ("pagination_style", "ALTER TABLE sources ADD COLUMN pagination_style TEXT DEFAULT 'auto'"),
    ]:
        try:
            cur.execute(ddl)
            conn.commit()
        except Exception:
            pass  # column already exists

    try:
        cur.execute("UPDATE sources SET pagination_style = 'none' WHERE scrape_type = 'rss' AND (pagination_style IS NULL OR pagination_style = 'auto')")
        cur.execute("UPDATE sources SET max_pages = 1 WHERE max_pages IS NULL")
        cur.execute("UPDATE sources SET pagination_style = 'auto' WHERE pagination_style IS NULL")
        conn.commit()
    except Exception:
        pass

    # ── Migration: add health sources + fix ALL known broken source URLs ─────────
    # Safe to run on every boot — INSERT OR IGNORE skips already-present rows.
    #
    # Root-cause summary for each broken source (confirmed via screenshots):
    #
    # 1. BC Legislature /rss/bills → 404 PAGE NOT FOUND (screenshot 4).
    #    The correct RSS endpoint is /rss/bills-introduced.  Also try the
    #    atom feed at /atom/bills as secondary.
    #
    # 2. BC Indigenous Relations & Reconciliation → URL
    #    news.gov.bc.ca/ministries/indigenous-relations-reconciliation returns
    #    a BC Gov search page (screenshot 3), not a news listing.  Fixed to
    #    the JSON/RSS news feed for that ministry tag.
    #
    # 3. CIHI — cihi.ca/en/news uses JS rendering; the static HTML page has
    #    no article links visible to BeautifulSoup.  Fixed to their public
    #    RSS feed at cihi.ca/sites/default/files/cihi-news-rss.xml.
    #
    # 4. BC Centre on Substance Use — bccsu.ca/news/ URL yields 0 articles
    #    because their markup uses .wp-block-post-title a (Gutenberg blocks)
    #    which our generic selectors don't match.  Fixed to /news-and-media/
    #    with the correct URL that the screenshots confirm exists, and the
    #    scraper CSS-selector list is extended (in scraper.py) to handle it.
    #
    # 5. Health Canada advanced-search URL is a JS app — replaced with their
    #    GCWeb news listing which is static HTML.
    #
    # 6. BC Gov Newsroom ministry sub-pages (Ministry of Health, Post-Secondary)
    #    — these are Drupal Views pages; the existing .views-row selector
    #    handles them correctly once the right URL is set.

    # ── Migration: fix ALL known broken source URLs (idempotent, runs every boot) ─
    #
    # Each fix is a LIKE-pattern UPDATE so it applies whether the row came from
    # _seed_sources or a previous migration run.  Each UPDATE is wrapped in its
    # own try/except so a URL collision on one fix doesn't abort the rest.
    #
    # Diagnosis-confirmed fix map (2026-07-01 diagnostic report):
    #
    # TIMEOUTS (canada.ca blocks Render's IP range at connection level):
    #   Health Canada, ISED, Crown-Indigenous, Education → switch to .atom feeds
    #   served from a different CDN path not subject to the same IP restriction.
    #
    # SSL ERROR:
    #   CIHR — cihr-irsc.gc.ca (no-www) has an invalid/expired SSL cert.
    #   www.cihr-irsc.gc.ca has a valid cert.
    #
    # 404 ERRORS:
    #   BC Legislature /rss/bills-introduced → 404; use /rss/news (press releases)
    #   BC Public Service Agency deep content URL → ministry newsroom path
    #   Burnaby City Hall /city-hall/news → 404; /news is the correct path
    #
    # ZERO ARTICLES (selector/type mismatch):
    #   Mental Health Commission HTML → use their WordPress RSS feed instead
    #   CIHI /en/news is JS-rendered → static RSS feed (already fixed previously)
    #   BCCSU /news/ wrong path → /news-and-media/ (already fixed previously)

    _new_sources = [
        ("Health Canada News",                        "https://www.canada.ca/en/health-canada/news.atom",               "Federal", "rss"),
        ("BC Ministry of Health",                     "https://news.gov.bc.ca/ministries/health",                       "BC",      "html"),
        ("Mental Health Commission of Canada",        "https://www.mentalhealthcommission.ca/feed/",                    "Federal", "rss"),
        ("CIHI — Canadian Institute for Health Info", "https://www.cihi.ca/sites/default/files/cihi-news-rss.xml",      "Federal", "rss"),
        ("Canadian Pharmacists Association",          "https://www.pharmacists.ca/news-events/news/",                   "Federal", "html"),
        ("BC Centre on Substance Use",                "https://www.bccsu.ca/news-and-media/",                           "BC",      "html"),
    ]
    try:
        for name, url, jurisdiction, scrape_type in _new_sources:
            cur.execute(
                "INSERT OR IGNORE INTO sources (name, url, jurisdiction, scrape_type) VALUES (?,?,?,?)",
                (name, url, jurisdiction, scrape_type)
            )

        _url_fixes_like = [
            # ════════════════════════════════════════════════════
            # SOURCE: PolicyPulse-Source-Diagnostic-2026-07-03.pdf
            # 32 broken sources fixed below.
            # ════════════════════════════════════════════════════

            # ── SSL cert errors ──────────────────────────────────
            # CIHR: both www and bare domain have broken SSL certs.
            # Use their plain-HTTP news releases page (redirects to HTTPS internally).
            ("%CIHR — News%",
             "https://cihr-irsc.gc.ca/e/news_releases.html", "html",
             "CIHR — News"),

            # CPTBC: cptbc.org SSL broken. They merged into College of PT BC.
            ("%Physical Therapists%",
             "https://www.collegept.ca/about/news-events", "html",
             "College of Physical Therapists of BC"),

            # BC LRB: bclrb.ca DNS failure — domain moved to lrb.bc.ca
            ("%Labour Relations Board%",
             "https://lrb.bc.ca/news/", "html",
             "BC Labour Relations Board"),

            # ── canada.ca total timeout (Render IP blocked at TCP level) ──
            # Atom feeds also timeout now. Route through Google News RSS instead —
            # not IP-restricted, returns same federal news.
            ("%Health Canada%",
             "https://news.google.com/rss/search?q=Health+Canada+site:canada.ca&hl=en-CA&gl=CA&ceid=CA:en",
             "rss", "Health Canada News"),
            ("%Crown-Indigenous%",
             "https://news.google.com/rss/search?q=%22Crown-Indigenous+Relations%22+Canada&hl=en-CA&gl=CA&ceid=CA:en",
             "rss", "Crown-Indigenous Relations Canada"),
            ("%Innovation Science%",
             "https://news.google.com/rss/search?q=%22Innovation+Science%22+Canada+government&hl=en-CA&gl=CA&ceid=CA:en",
             "rss", "Innovation Science and Economic Development"),
            ("%Canada%Education%",
             "https://news.google.com/rss/search?q=%22Employment+Social+Development%22+Canada&hl=en-CA&gl=CA&ceid=CA:en",
             "rss", "Government of Canada — Employment & Social Development"),

            # ── 404 — URLs that moved ─────────────────────────────
            ("%College of Nurses%",
             "https://www.bccnm.ca/news/", "html",
             "BC College of Nurses & Midwives"),
            ("%Federation of Labour%",
             "https://bcfed.ca/news/", "html",
             "BC Federation of Labour"),
            ("%Legislature%",
             "https://www.leg.bc.ca/parliamentary-business/legislation-debates-proceedings",
             "html", "BC Legislature — Bills & Proceedings"),
            ("%Mental Health & Substance Use Services%",
             "https://www.phsa.ca/about/news-stories", "html",
             "BC Mental Health & Substance Use Services"),
            ("%Public Service Agency%",
             "https://news.gov.bc.ca/", "html",
             "BC Public Service Agency"),
            ("%CFHI%",
             "https://www.cfhi-fcass.ca/news", "html",
             "CFHI — Canadian Foundation for Healthcare Improvement"),
            ("%CIHI%",
             "https://www.cihi.ca/en/news", "html",
             "CIHI — Canadian Institute for Health Info"),
            ("%CUPE BC%",
             "https://cupe.bc.ca/news/", "html",
             "CUPE BC"),
            ("%HEABC%",
             "https://heabc.bc.ca/news-and-resources/", "html",
             "HEABC — Health Employers Association of BC"),
            ("%Hospital Employees Union%",
             "https://www.heu.org/news/", "html",
             "HEU — Hospital Employees Union"),
            ("%Interior Health%",
             "https://www.interiorhealth.ca/news", "html",
             "Interior Health Authority"),
            ("%Island Health%",
             "https://www.islandhealth.ca/news", "html",
             "Island Health Authority"),
            ("%NSERC%",
             "https://www.nserc-crsng.gc.ca/Media-Media/NewsReleases-CommuniquesDePresse_eng.asp",
             "html", "NSERC — News Releases"),
            ("%Northern Health%",
             "https://www.northernhealth.ca/about-northern-health/media-centre/news-releases",
             "html", "Northern Health Authority"),
            ("%Provincial Health Services%",
             "https://www.phsa.ca/about/news-stories", "html",
             "PHSA — Provincial Health Services Authority"),
            ("%SSHRC%",
             "https://www.sshrc-crsh.gc.ca/news-nouvelles/index-eng.aspx", "html",
             "SSHRC — Research News"),

            # ── 403 — Render IP blocked; route via Google News RSS ────────
            ("%Maclean%",
             "https://news.google.com/rss/search?q=macleans+canada+education&hl=en-CA&gl=CA&ceid=CA:en",
             "rss", "Maclean\'s Education"),
            ("%Times Higher Education%",
             "https://news.google.com/rss/search?q=%22higher+education%22+Canada+university&hl=en-CA&gl=CA&ceid=CA:en",
             "rss", "Times Higher Education"),
            ("%Universities Canada%",
             "https://news.google.com/rss/search?q=%22Universities+Canada%22&hl=en-CA&gl=CA&ceid=CA:en",
             "rss", "Universities Canada"),
            ("%University Affairs%",
             "https://news.google.com/rss/search?q=%22university+affairs%22+Canada&hl=en-CA&gl=CA&ceid=CA:en",
             "rss", "University Affairs Canada"),

            # ── Keep correct URLs pinned so accidental reverts get fixed ──
            ("%Substance Use%",
             "https://www.bccsu.ca/news-and-media/", "html",
             "BC Centre on Substance Use"),
            ("%Mental Health Commission%",
             "https://www.mentalhealthcommission.ca/feed/", "rss",
             "Mental Health Commission of Canada"),
            ("%Burnaby%",
             "https://www.burnaby.ca/news", "html",
             "Burnaby City Hall News"),
            ("%Indigenous%Reconciliation%",
             "https://news.gov.bc.ca/ministries/indigenous-relations-and-reconciliation",
             "html", "BC Indigenous Relations & Reconciliation"),
        ]
        for name_like, new_url, new_type, new_name in _url_fixes_like:
            try:
                cur.execute(
                    "UPDATE sources SET url=?, scrape_type=?, name=? WHERE name LIKE ?",
                    (new_url, new_type, new_name, name_like)
                )
            except Exception as e:
                import logging as _fix_log
                _fix_log.getLogger(__name__).warning(
                    f"Could not apply URL fix for '{name_like}' -> {new_url}: {e}"
                )

        cur.execute("UPDATE sources SET pagination_style='none' WHERE scrape_type='rss'")
        cur.execute(
            "UPDATE sources SET max_pages=3, pagination_style='path' "
            "WHERE name LIKE '%Substance Use%' AND scrape_type='html'"
        )
        for pattern in ["PHSA%", "Fraser Health%", "Vancouver Coastal%",
                        "Interior Health%", "Northern Health%", "Island Health%",
                        "BC Mental Health%"]:
            cur.execute(
                "UPDATE sources SET max_pages=2, pagination_style='query' "
                "WHERE name LIKE ? AND scrape_type='html' AND max_pages=1",
                (pattern,)
            )
        conn.commit()
    except Exception as e:
        import logging as _mig_log
        _mig_log.getLogger(__name__).warning(f"Source URL migration error: {e}")


    _ensure_sources = [
        # ── HSA-specific ───────────────────────────────────────────────────
        ("HSA BC — News & Updates",                        "https://hsabc.org/feed/",                                                          "BC",      "rss"),
        ("HEABC — Health Employers Association of BC",     "https://heabc.bc.ca/news-and-resources/",                                          "BC",      "html"),
        ("HEU — Hospital Employees Union",                 "https://www.heu.org/news/",                                                        "BC",      "html"),
        # ── Health Authorities ─────────────────────────────────────────────
        ("PHSA — Provincial Health Services Authority",    "https://www.phsa.ca/about/news-stories",                                           "BC",      "html"),
        ("Fraser Health Authority",                        "https://www.fraserhealth.ca/news",                                                 "BC",      "html"),
        ("Vancouver Coastal Health Authority",             "https://www.vch.ca/en/news",                                                       "BC",      "html"),
        ("Interior Health Authority",                      "https://www.interiorhealth.ca/news",                                               "BC",      "html"),
        ("Northern Health Authority",                      "https://www.northernhealth.ca/about-northern-health/media-centre/news-releases",   "BC",      "html"),
        ("Island Health Authority",                        "https://www.islandhealth.ca/news",                                                 "BC",      "html"),
        # ── BC Government ─────────────────────────────────────────────────
        ("BC Government Newsroom",                         "https://news.gov.bc.ca/",                                                          "BC",      "html"),
        ("BC Ministry of Health",                          "https://news.gov.bc.ca/ministries/health",                                         "BC",      "html"),
        ("BC Ministry of Mental Health & Addictions",      "https://news.gov.bc.ca/ministries/mental-health-and-addictions",                   "BC",      "html"),
        ("BC Ministry of Post-Secondary Education",        "https://news.gov.bc.ca/ministries/post-secondary-education-and-future-skills",     "BC",      "html"),
        ("BC Indigenous Relations & Reconciliation",       "https://news.gov.bc.ca/ministries/indigenous-relations-and-reconciliation",        "BC",      "html"),
        ("BC Legislature — Bills & Proceedings",           "https://www.leg.bc.ca/parliamentary-business/legislation-debates-proceedings",     "BC",      "html"),
        # ── Federal — via Google News RSS (canada.ca IP-blocked from Render) ─
        ("Health Canada News",                             "https://news.google.com/rss/search?q=Health+Canada+site:canada.ca&hl=en-CA&gl=CA&ceid=CA:en",        "Federal", "rss"),
        ("Crown-Indigenous Relations Canada",              "https://news.google.com/rss/search?q=%22Crown-Indigenous+Relations%22+Canada&hl=en-CA&gl=CA&ceid=CA:en", "Federal", "rss"),
        ("Innovation Science and Economic Development",    "https://news.google.com/rss/search?q=%22Innovation+Science%22+Canada+government&hl=en-CA&gl=CA&ceid=CA:en", "Federal", "rss"),
        ("Government of Canada — Employment & Social Development", "https://news.google.com/rss/search?q=%22Employment+Social+Development%22+Canada&hl=en-CA&gl=CA&ceid=CA:en", "Federal", "rss"),
        # ── Professional Regulation ────────────────────────────────────────
        ("College of Physicians & Surgeons of BC",         "https://www.cpsbc.ca/news",                                                        "BC",      "html"),
        ("BC College of Nurses & Midwives",                "https://www.bccnm.ca/news/",                                                       "BC",      "html"),
        ("College of Physical Therapists of BC",           "https://www.collegept.ca/about/news-events",                                       "BC",      "html"),
        ("BC College of Pharmacists",                      "https://www.bcpharmacists.org/news",                                               "BC",      "html"),
        ("Canadian Pharmacists Association",               "https://www.pharmacists.ca/news-events/news/",                                     "Federal", "html"),
        # ── Labour ────────────────────────────────────────────────────────
        ("BC Federation of Labour",                        "https://bcfed.ca/news/",                                                           "BC",      "html"),
        ("CUPE BC",                                        "https://cupe.bc.ca/news/",                                                         "BC",      "html"),
        ("BC Labour Relations Board",                      "https://lrb.bc.ca/news/",                                                          "BC",      "html"),
        # ── Indigenous Health ──────────────────────────────────────────────
        ("First Nations Health Authority",                 "https://www.fnha.ca/about/news-and-events/news",                                   "BC",      "html"),
        ("BC First Nations Summit",                        "https://fns.bc.ca/news/",                                                          "BC",      "html"),
        # ── Mental Health & Substance Use ─────────────────────────────────
        ("BC Centre on Substance Use",                     "https://www.bccsu.ca/news-and-media/",                                             "BC",      "html"),
        ("Mental Health Commission of Canada",             "https://www.mentalhealthcommission.ca/feed/",                                      "Federal", "rss"),
        # ── Health Research & Evidence ────────────────────────────────────
        ("CIHI — Canadian Institute for Health Info",      "https://www.cihi.ca/en/news",                                                      "Federal", "html"),
        # ── Policy Commentary ─────────────────────────────────────────────
        ("Policy Options (IRPP)",                          "https://policyoptions.irpp.org/feed/",                                             "Federal", "rss"),
        ("Canadian Centre for Policy Alternatives BC",     "https://www.policyalternatives.ca/feed/",                                          "BC",      "rss"),
    ]
    try:
        # Load tombstoned URLs FIRST — any URL the user has explicitly deleted
        # must never be re-inserted, regardless of what _ensure_sources contains.
        try:
            _tombstoned = {r["url"] for r in conn.execute("SELECT url FROM deleted_sources").fetchall()}
        except Exception:
            _tombstoned = set()

        inserted = 0
        skipped_tombstone = 0
        for row in _ensure_sources:
            url = row[1]
            if url in _tombstoned:
                skipped_tombstone += 1
                continue  # user deleted this — respect the deletion
            cur.execute(
                "INSERT OR IGNORE INTO sources (name, url, jurisdiction, scrape_type) VALUES (?,?,?,?)",
                row
            )
            if cur.rowcount == 1:
                inserted += 1
        conn.commit()
        import logging as _ins_log
        _ins_log.getLogger(__name__).info(
            f"[migration] Source ensure-pass complete: {inserted} new, "
            f"{len(_ensure_sources) - inserted - skipped_tombstone} already present, "
            f"{skipped_tombstone} skipped (user-deleted)"
        )
    except Exception as e:
        import logging as _ins_log
        _ins_log.getLogger(__name__).warning(f"Source ensure migration error: {e}")


    conn.close()


def _seed_sources(cur):
    """Seed the sources table for a fresh install.

    Columns:  name, url, jurisdiction, scrape_type
    scrape_type: 'rss'  → feedparser-style RSS/Atom parser (scrape_rss)
                 'html' → CSS-selector-based HTML scraper (scrape_generic_paginated)

    DEDUPLICATION RULE: one row per URL.  The UNIQUE index on sources.url
    enforces this at the DB level, but we also keep this list clean so a fresh
    install never tries to insert conflicting rows.

    JURISDICTION VALUES (used for filtering in the UI):
      BC | Federal | Municipal | Pan-Canadian | International

    URL NOTES — lessons learned from diagnostic runs:
      • canada.ca HTML news pages TCP-timeout from Render's IP range.
        Use their GCWeb Atom feeds (/news.atom) instead — different CDN, not blocked.
      • cihr-irsc.gc.ca (no-www) has an expired SSL cert. www subdomain is fine.
      • leg.bc.ca/rss/bills-introduced returns 404. /rss/news is the live feed.
      • CIHI /en/news is JS-rendered. Use their static RSS file.
      • BCCSU /news/ is a 404. /news-and-media/ is the correct listing page.
      • bc.gov.ca ministry newsroom sub-pages (Drupal) work reliably via HTML.
      • WordPress sites: always prefer /feed/ (RSS) over HTML scraping.

    SOURCES ARE GROUPED BY RELEVANCE TO HSA BC — a health sector union whose
    analyst needs: labour relations, health authority ops, professional regulation,
    provincial/federal health policy, mental health/SUD, Indigenous health,
    research & evidence, and broader health policy commentary.
    """
    sources = [

        # ══════════════════════════════════════════════════════════════════════
        # HSA-SPECIFIC — direct organizational intelligence
        # ══════════════════════════════════════════════════════════════════════
        # HSA's own news — useful for internal comms monitoring & context
        ("HSA BC — News & Updates",
         "https://hsabc.org/feed/",
         "BC", "rss"),

        # Employer counterpart — critical for collective bargaining intelligence
        ("HEABC — Health Employers Association of BC",
         "https://heabc.bc.ca/feed/",
         "BC", "rss"),

        # HEU — Hospital Employees Union; major sister union in health sector,
        # often negotiates first and sets pattern for HSA rounds
        ("HEU — Hospital Employees Union",
         "https://www.heu.org/news/feed/",
         "BC", "rss"),

        # ══════════════════════════════════════════════════════════════════════
        # BC HEALTH AUTHORITIES — operational & policy news from all six
        # ══════════════════════════════════════════════════════════════════════
        ("PHSA — Provincial Health Services Authority",
         "https://www.phsa.ca/about-site/news-stories",
         "BC", "html"),

        ("Fraser Health Authority",
         "https://www.fraserhealth.ca/news",
         "BC", "html"),

        ("Vancouver Coastal Health Authority",
         "https://www.vch.ca/en/news",
         "BC", "html"),

        ("Interior Health Authority",
         "https://www.interiorhealth.ca/about/media-centre/news",
         "BC", "html"),

        ("Northern Health Authority",
         "https://www.northernhealth.ca/about-northern-health/news",
         "BC", "html"),

        ("Island Health Authority",
         "https://www.islandhealth.ca/learn-about-health/news-media",
         "BC", "html"),

        # ══════════════════════════════════════════════════════════════════════
        # BC GOVERNMENT — ministry & legislative sources
        # ══════════════════════════════════════════════════════════════════════
        ("BC Government Newsroom",
         "https://news.gov.bc.ca/",
         "BC", "html"),

        ("BC Ministry of Health",
         "https://news.gov.bc.ca/ministries/health",
         "BC", "html"),

        ("BC Ministry of Mental Health & Addictions",
         "https://news.gov.bc.ca/ministries/mental-health-and-addictions",
         "BC", "html"),

        ("BC Ministry of Post-Secondary Education",
         "https://news.gov.bc.ca/ministries/post-secondary-education-and-future-skills",
         "BC", "html"),

        ("BC Indigenous Relations & Reconciliation",
         "https://news.gov.bc.ca/ministries/indigenous-relations-and-reconciliation",
         "BC", "html"),

        ("BC Public Service Agency",
         "https://news.gov.bc.ca/ministries/public-service-agency",
         "BC", "html"),

        # BC Legislature — /rss/bills-introduced 404s; /rss/news is the live feed
        ("BC Legislature — News & Hansard",
         "https://www.leg.bc.ca/rss/news",
         "BC", "rss"),

        # ══════════════════════════════════════════════════════════════════════
        # FEDERAL GOVERNMENT — health, labour, Indigenous policy
        # ══════════════════════════════════════════════════════════════════════
        # canada.ca HTML pages TCP-timeout from Render IPs — use Atom feeds
        ("Health Canada News",
         "https://www.canada.ca/en/health-canada/news.atom",
         "Federal", "rss"),

        ("Crown-Indigenous Relations Canada",
         "https://www.canada.ca/en/crown-indigenous-relations-northern-affairs/news.atom",
         "Federal", "rss"),

        ("Innovation Science and Economic Development",
         "https://www.canada.ca/en/innovation-science-economic-development/news.atom",
         "Federal", "rss"),

        ("Government of Canada — Employment & Social Development",
         "https://www.canada.ca/en/employment-social-development/news.atom",
         "Federal", "rss"),

        # ══════════════════════════════════════════════════════════════════════
        # PROFESSIONAL REGULATION — colleges governing HSA member professions
        # ══════════════════════════════════════════════════════════════════════
        ("College of Physicians & Surgeons of BC",
         "https://www.cpsbc.ca/news",
         "BC", "html"),

        # BCCNM regulates RNs, RNPs, RPNs, midwives — scope overlap with HSA
        ("BC College of Nurses & Midwives",
         "https://www.bccnm.ca/news/Pages/default.aspx",
         "BC", "html"),

        ("College of Physical Therapists of BC",
         "https://www.cptbc.org/news/",
         "BC", "html"),

        ("BC College of Pharmacists",
         "https://www.bcpharmacists.org/news",
         "BC", "html"),

        # ══════════════════════════════════════════════════════════════════════
        # LABOUR — broader BC and health sector labour movement
        # ══════════════════════════════════════════════════════════════════════
        ("BC Federation of Labour",
         "https://bcfed.ca/feed/",
         "BC", "rss"),

        # CUPE BC (not national) — represents many health care support workers
        ("CUPE BC",
         "https://cupe.bc.ca/news/feed/",
         "BC", "rss"),

        # BC Labour Relations Board — decisions affecting health sector bargaining
        ("BC Labour Relations Board",
         "https://www.bclrb.ca/decisions/",
         "BC", "html"),

        # ══════════════════════════════════════════════════════════════════════
        # INDIGENOUS HEALTH
        # ══════════════════════════════════════════════════════════════════════
        ("First Nations Health Authority",
         "https://www.fnha.ca/about/news-and-events/news",
         "BC", "html"),

        ("BC First Nations Summit",
         "https://fns.bc.ca/news/",
         "BC", "html"),

        # ══════════════════════════════════════════════════════════════════════
        # MENTAL HEALTH & SUBSTANCE USE
        # ══════════════════════════════════════════════════════════════════════
        # BCMHSUS is the PHSA subsidiary that HSA members staff directly
        ("BC Mental Health & Substance Use Services",
         "https://www.bcmhsus.ca/about/news-stories",
         "BC", "html"),

        ("BC Centre on Substance Use",
         "https://www.bccsu.ca/news-and-media/",
         "BC", "html"),

        ("Mental Health Commission of Canada",
         "https://www.mentalhealthcommission.ca/feed/",
         "Federal", "rss"),

        # ══════════════════════════════════════════════════════════════════════
        # HEALTH RESEARCH & EVIDENCE
        # ══════════════════════════════════════════════════════════════════════
        # CIHI: /en/news is JS-rendered — use static RSS file
        ("CIHI — Canadian Institute for Health Info",
         "https://www.cihi.ca/sites/default/files/cihi-news-rss.xml",
         "Federal", "rss"),

        # www subdomain has valid SSL cert; bare domain cert is expired
        ("CIHR — News",
         "https://www.cihr-irsc.gc.ca/rss/news-eng.xml",
         "Federal", "rss"),

        ("CFHI — Canadian Foundation for Healthcare Improvement",
         "https://www.cfhi-fcass.ca/news-and-events/news",
         "Federal", "html"),

        # ══════════════════════════════════════════════════════════════════════
        # POLICY COMMENTARY & ANALYSIS
        # ══════════════════════════════════════════════════════════════════════
        ("Policy Options (IRPP)",
         "https://policyoptions.irpp.org/feed/",
         "Federal", "rss"),

        # CCPA BC office focuses heavily on health, labour, and social policy
        ("Canadian Centre for Policy Alternatives BC",
         "https://www.policyalternatives.ca/feed/",
         "BC", "rss"),

        # ══════════════════════════════════════════════════════════════════════
        # RESEARCH COUNCILS — funding relevant to health sciences research
        # ══════════════════════════════════════════════════════════════════════
        ("SSHRC — Research News",
         "https://www.sshrc-crsh.gc.ca/rss/news-nouvelles-eng.xml",
         "Federal", "rss"),

        ("NSERC — News Releases",
         "https://www.nserc-crsng.gc.ca/rss/news-eng.xml",
         "Federal", "rss"),

        # ══════════════════════════════════════════════════════════════════════
        # HEALTH SECTOR — professional associations relevant to HSA members
        # ══════════════════════════════════════════════════════════════════════
        ("Canadian Pharmacists Association",
         "https://www.pharmacists.ca/news-events/news/",
         "Federal", "html"),

        # ══════════════════════════════════════════════════════════════════════
        # BROADER POLICY & HIGHER EDUCATION CONTEXT
        # ══════════════════════════════════════════════════════════════════════
        ("University Affairs Canada",
         "https://www.universityaffairs.ca/feed/",
         "Federal", "rss"),

        ("Higher Education Strategy Associates",
         "https://higheredstrategy.com/feed/",
         "Pan-Canadian", "rss"),

        ("Maclean's Education",
         "https://www.macleans.ca/education/feed/",
         "Federal", "rss"),

        ("Times Higher Education",
         "https://www.timeshighereducation.com/rss.xml",
         "International", "rss"),

        ("Burnaby City Hall News",
         "https://www.burnaby.ca/news",
         "Municipal", "html"),

    ]

    # Use INSERT OR IGNORE so this is safe to call even if some rows exist
    # (though in practice _seed_sources only runs when COUNT(*) = 0)
    cur.executemany(
        "INSERT OR IGNORE INTO sources (name, url, jurisdiction, scrape_type) VALUES (?,?,?,?)",
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


# ── ALERT SUBSCRIBER HELPERS ──────────────────────────────

def get_alert_subscribers(alert_type: str) -> list:
    """
    Return active subscribers opted in to a specific alert type.
    alert_type: 'urgent' or 'keyword'
    For urgent alerts, also returns each subscriber's personal
    urgent_min_relevance threshold (default 9).
    """
    col = "urgent_alerts" if alert_type == "urgent" else "keyword_alerts"
    conn = get_conn()
    try:
        if alert_type == "urgent":
            rows = conn.execute(
                f"SELECT id, name, email, urgent_min_relevance FROM subscribers "
                f"WHERE active=1 AND {col}=1"
            ).fetchall()
        else:
            rows = conn.execute(
                f"SELECT id, name, email, urgent_min_relevance FROM subscribers "
                f"WHERE active=1 AND {col}=1"
            ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []
    finally:
        conn.close()


def update_subscriber_alerts(subscriber_id: int, urgent_alerts: int, keyword_alerts: int,
                             urgent_min_relevance: int = 9):
    """Update alert opt-in flags and personal urgency threshold for a subscriber."""
    urgent_min_relevance = max(6, min(10, int(urgent_min_relevance)))
    conn = get_conn()
    try:
        conn.execute(
            "UPDATE subscribers SET urgent_alerts=?, keyword_alerts=?, urgent_min_relevance=? WHERE id=?",
            (urgent_alerts, keyword_alerts, urgent_min_relevance, subscriber_id)
        )
        conn.commit()
    except Exception:
        pass
    finally:
        conn.close()


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


def get_known_url_hashes() -> set:
    """Return the full set of url_hash values already stored in the articles
    table. Used by the scraper's pagination early-exit logic: once a listing
    page yields zero links whose hash isn't already in this set, we know
    we've caught up to a previous scrape run and can stop paginating deeper.

    A single SELECT of just the hash column (no other fields) keeps this
    cheap even with tens of thousands of rows — it's called once per scrape
    run, not once per page.
    """
    conn = get_conn()
    try:
        rows = conn.execute("SELECT url_hash FROM articles").fetchall()
        return {r["url_hash"] for r in rows}
    except Exception:
        return set()
    finally:
        conn.close()


def save_article(title, url, url_hash, source, jurisdiction, domain,
                 relevance, sentiment, summary, why_it_matters,
                 pub_date, tags=""):
    """Insert article if URL not already in DB. Returns True if inserted.

    pub_date may be None when the scraper could not determine the real
    publish date from the article page.  In that case we store NULL so the
    frontend can display "date unknown" rather than today's scrape date.
    month_year falls back to the scrape date's month so archive tabs work.
    """
    conn = get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat()
    # month_year for archive grouping: use pub_date if known, else scrape month
    if pub_date and len(pub_date) >= 7:
        month_year = pub_date[:7]
    else:
        month_year = now[:7]

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


def log_scrape(articles_added, errors="", scrape_type="news"):
    """Record a completed scrape run.

    scrape_type distinguishes the daily news/RSS scraper ('news') from the
    scholarly/research scraper ('research') so the News tab's Scrape Log and
    the Research tab's Research Log can each show their own history.
    """
    conn = get_conn()
    conn.execute(
        "INSERT INTO scrape_log (scraped_at, articles_added, errors, scrape_type) VALUES (?,?,?,?)",
        (datetime.utcnow().isoformat(), articles_added, errors, scrape_type)
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
    """Delete a source and record its URL in the deleted_sources tombstone table.

    The tombstone prevents _ensure_sources from resurrecting this source on
    the next Render restart.  Without this, every URL in _ensure_sources would
    reappear after any redeploy because INSERT OR IGNORE only skips rows whose
    URL is still in the sources table — a deleted row lifts that constraint.
    """
    conn = get_conn()
    # Fetch the URL before deleting so we can tombstone it
    row = conn.execute("SELECT url FROM sources WHERE id = ?", (source_id,)).fetchone()
    if row:
        conn.execute(
            "INSERT OR REPLACE INTO deleted_sources (url, deleted_at) VALUES (?, datetime('now'))",
            (row["url"],)
        )
    conn.execute("DELETE FROM sources WHERE id = ?", (source_id,))
    conn.commit()
    conn.close()


def get_deleted_source_urls() -> set:
    """Return the set of URLs that have been explicitly deleted by the user.
    Used by init_db()'s _ensure_sources migration to skip resurrecting them.
    """
    conn = get_conn()
    try:
        rows = conn.execute("SELECT url FROM deleted_sources").fetchall()
        return {r["url"] for r in rows}
    except Exception:
        return set()
    finally:
        conn.close()


def update_source(source_id: int, fields: dict):
    allowed = {"name", "url", "jurisdiction", "scrape_type", "active", "max_pages", "pagination_style"}
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
    allowed = {"name", "role", "active", "urgent_alerts", "keyword_alerts", "urgent_min_relevance"}
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


def delete_exclusion_keyword_by_id(keyword_id: int):
    """Delete exclusion keyword by id (used by API endpoint)."""
    conn = get_conn()
    conn.execute("DELETE FROM exclusion_keywords WHERE id = ?", (keyword_id,))
    conn.commit()
    conn.close()


def get_all_exclusion_keywords() -> list:
    """Return full rows (id + keyword) for UI display."""
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT id, keyword FROM exclusion_keywords ORDER BY id"
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []
    finally:
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


# ── RELEVANCE OVERRIDES ───────────────────────────────────────────────────────

def update_article_relevance(article_id: int, score: int):
    """Override relevance score for a news article (1-10)."""
    score = max(1, min(10, int(score)))
    conn = get_conn()
    conn.execute("UPDATE articles SET relevance = ? WHERE id = ?", (score, article_id))
    conn.commit()
    conn.close()


def update_scholarly_relevance(article_id: int, score: int):
    """Override relevance score for a scholarly article (1-10)."""
    score = max(1, min(10, int(score)))
    conn = get_conn()
    try:
        conn.execute(
            "UPDATE scholarly_articles SET relevance = ? WHERE id = ?", (score, article_id)
        )
        conn.commit()
    except Exception:
        pass
    finally:
        conn.close()


# ── SCRAPER CONFIG ────────────────────────────────────────────────────────────

def get_scraper_config(key: str) -> str | None:
    """Retrieve a single scraper config value by key."""
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT value FROM scraper_config WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else None
    except Exception:
        return None
    finally:
        conn.close()


def set_scraper_config(key: str, value: str):
    """Insert or replace a scraper config key-value pair."""
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO scraper_config (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value)
        )
        conn.commit()
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"set_scraper_config error: {e}")
    finally:
        conn.close()


def get_all_scraper_config() -> dict:
    """Return all scraper config key-value pairs as a dict."""
    conn = get_conn()
    try:
        rows = conn.execute("SELECT key, value FROM scraper_config").fetchall()
        return {r["key"]: r["value"] for r in rows}
    except Exception:
        return {}
    finally:
        conn.close()


# ── DATE FIX HELPERS ──────────────────────────────────────────────────────────

def get_articles_missing_pub_date(limit: int = 200) -> list[dict]:
    """Return articles whose pub_date is NULL or equals their processed_date.

    These are articles where the scraper could not extract a real publish date
    from the article page.  The date fix endpoint re-fetches each URL and tries
    to extract the real date.
    """
    conn = get_conn()
    try:
        rows = conn.execute(
            """SELECT id, url, pub_date, processed_date
               FROM articles
               WHERE pub_date IS NULL
                  OR pub_date = ''
                  OR (processed_date IS NOT NULL AND pub_date = substr(processed_date,1,10))
               ORDER BY id DESC
               LIMIT ?""",
            (limit,)
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []
    finally:
        conn.close()


def update_article_pub_date(article_id: int, pub_date: str):
    """Update pub_date (and month_year) for a single article."""
    if not pub_date:
        return
    month_year = pub_date[:7]
    conn = get_conn()
    try:
        conn.execute(
            "UPDATE articles SET pub_date = ?, month_year = ? WHERE id = ?",
            (pub_date, month_year, article_id)
        )
        conn.commit()
    except Exception:
        pass
    finally:
        conn.close()


def get_scholarly_articles_missing_pub_date(limit: int = 100) -> list[dict]:
    """Return scholarly articles with NULL or empty pub_date."""
    conn = get_conn()
    try:
        rows = conn.execute(
            """SELECT id, url, pub_date, processed_date
               FROM scholarly_articles
               WHERE pub_date IS NULL OR pub_date = ''
               ORDER BY id DESC
               LIMIT ?""",
            (limit,)
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []
    finally:
        conn.close()


def update_scholarly_pub_date(article_id: int, pub_date: str):
    """Update pub_date for a single scholarly article."""
    if not pub_date:
        return
    conn = get_conn()
    try:
        conn.execute(
            "UPDATE scholarly_articles SET pub_date = ? WHERE id = ?",
            (pub_date, article_id)
        )
        conn.commit()
    except Exception:
        pass
    finally:
        conn.close()


# ── APP SETTINGS / PREFERENCES ────────────────────────────────────────────────

def get_app_setting(key: str, default: str = "") -> str:
    """Retrieve a single app preference/setting by key."""
    conn = get_conn()
    try:
        # Settings are stored in the same scraper_config table under a 'setting:' prefix
        row = conn.execute(
            "SELECT value FROM scraper_config WHERE key = ?",
            ("setting:" + key,)
        ).fetchone()
        return row["value"] if row else default
    except Exception:
        return default
    finally:
        conn.close()


def set_app_setting(key: str, value: str):
    """Store a single app preference/setting."""
    set_scraper_config("setting:" + key, value)


def get_all_app_settings() -> dict:
    """Return all app settings (keys without the 'setting:' prefix)."""
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT key, value FROM scraper_config WHERE key LIKE 'setting:%'"
        ).fetchall()
        return {r["key"][8:]: r["value"] for r in rows}
    except Exception:
        return {}
    finally:
        conn.close()

# ── POLICY TRACKERS ───────────────────────────────────────────────────────────

def get_trackers() -> list[dict]:
    """Return all policy trackers with article and event counts."""
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM policy_trackers ORDER BY updated_date DESC"
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["article_count"] = conn.execute(
                "SELECT COUNT(*) FROM tracker_articles WHERE tracker_id=?", (d["id"],)
            ).fetchone()[0]
            d["event_count"] = conn.execute(
                "SELECT COUNT(*) FROM tracker_events WHERE tracker_id=?", (d["id"],)
            ).fetchone()[0]
            result.append(d)
        return result
    finally:
        conn.close()


def get_tracker_by_id(tracker_id: int) -> dict | None:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM policy_trackers WHERE id=?", (tracker_id,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def create_tracker(name: str, description: str = "", domain: str = "",
                   keywords: str = "", status: str = "Active") -> int:
    conn = get_conn()
    now = datetime.utcnow().isoformat()
    try:
        cur = conn.execute(
            "INSERT INTO policy_trackers (name, description, domain, status, keywords, created_date, updated_date) "
            "VALUES (?,?,?,?,?,?,?)",
            (name.strip(), description.strip(), domain.strip(), status, keywords.strip(), now, now)
        )
        new_id = cur.lastrowid
        conn.commit()
        return new_id
    finally:
        conn.close()


def update_tracker(tracker_id: int, fields: dict):
    allowed = {"name", "description", "domain", "status", "keywords"}
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return
    updates["updated_date"] = datetime.utcnow().isoformat()
    conn = get_conn()
    try:
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        values = list(updates.values()) + [tracker_id]
        conn.execute(f"UPDATE policy_trackers SET {set_clause} WHERE id=?", values)
        conn.commit()
    finally:
        conn.close()


def delete_tracker(tracker_id: int):
    conn = get_conn()
    try:
        conn.execute("DELETE FROM tracker_events WHERE tracker_id=?", (tracker_id,))
        conn.execute("DELETE FROM tracker_articles WHERE tracker_id=?", (tracker_id,))
        conn.execute("DELETE FROM policy_trackers WHERE id=?", (tracker_id,))
        conn.commit()
    finally:
        conn.close()


# ── TRACKER ARTICLES ──────────────────────────────────────────────────────────

def get_tracker_articles(tracker_id: int) -> list[dict]:
    """Return all articles linked to a tracker, joined with article data."""
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT ta.id as link_id, ta.article_id, ta.article_type, ta.added_date, ta.note, "
            "a.title, a.url, a.source, a.pub_date, a.domain, a.relevance, a.sentiment, a.summary, a.why_it_matters "
            "FROM tracker_articles ta "
            "LEFT JOIN articles a ON ta.article_id = a.id AND ta.article_type = 'news' "
            "WHERE ta.tracker_id = ? ORDER BY COALESCE(a.pub_date, ta.added_date) ASC",
            (tracker_id,)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def add_tracker_article(tracker_id: int, article_id: int,
                        article_type: str = "news", note: str = "") -> bool:
    conn = get_conn()
    now = datetime.utcnow().isoformat()
    try:
        # Prevent duplicates
        existing = conn.execute(
            "SELECT id FROM tracker_articles WHERE tracker_id=? AND article_id=? AND article_type=?",
            (tracker_id, article_id, article_type)
        ).fetchone()
        if existing:
            return False
        conn.execute(
            "INSERT INTO tracker_articles (tracker_id, article_id, article_type, added_date, note) "
            "VALUES (?,?,?,?,?)",
            (tracker_id, article_id, article_type, now, note)
        )
        conn.execute(
            "UPDATE policy_trackers SET updated_date=? WHERE id=?",
            (now, tracker_id)
        )
        conn.commit()
        return True
    finally:
        conn.close()


def remove_tracker_article(tracker_id: int, article_id: int, article_type: str = "news"):
    conn = get_conn()
    try:
        conn.execute(
            "DELETE FROM tracker_articles WHERE tracker_id=? AND article_id=? AND article_type=?",
            (tracker_id, article_id, article_type)
        )
        conn.execute(
            "UPDATE policy_trackers SET updated_date=? WHERE id=?",
            (datetime.utcnow().isoformat(), tracker_id)
        )
        conn.commit()
    finally:
        conn.close()


# ── TRACKER EVENTS ────────────────────────────────────────────────────────────

def get_tracker_events(tracker_id: int) -> list[dict]:
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM tracker_events WHERE tracker_id=? ORDER BY event_date ASC",
            (tracker_id,)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def add_tracker_event(tracker_id: int, title: str,
                      event_date: str = "", note: str = "") -> int:
    conn = get_conn()
    now = datetime.utcnow().isoformat()
    try:
        cur = conn.execute(
            "INSERT INTO tracker_events (tracker_id, title, event_date, note, created_date) "
            "VALUES (?,?,?,?,?)",
            (tracker_id, title.strip(), event_date.strip(), note.strip(), now)
        )
        new_id = cur.lastrowid
        conn.execute(
            "UPDATE policy_trackers SET updated_date=? WHERE id=?",
            (now, tracker_id)
        )
        conn.commit()
        return new_id
    finally:
        conn.close()


def delete_tracker_event(event_id: int):
    conn = get_conn()
    try:
        conn.execute("DELETE FROM tracker_events WHERE id=?", (event_id,))
        conn.commit()
    finally:
        conn.close()
