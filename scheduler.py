"""
PolicyPulse Scheduler
─────────────────────
• Daily news scrape      — 7:00 AM Monday–Friday (Vancouver time)
• Weekly scholarly scrape — 8:00 AM every Monday (Vancouver time)
"""

import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

log = logging.getLogger(__name__)

_scheduler = None


def start_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        return

    _scheduler = BackgroundScheduler(timezone="America/Vancouver")

    # Daily news scrape at 7:00 AM Vancouver time (Mon–Fri)
    _scheduler.add_job(
        _scrape_job,
        CronTrigger(hour=7, minute=0, day_of_week="mon-fri"),
        id="daily_scrape",
        replace_existing=True,
        name="Daily Policy News Scrape",
    )

    # Weekly scholarly/research scrape every Monday at 8:00 AM
    _scheduler.add_job(
        _scholarly_scrape_job,
        CronTrigger(hour=8, minute=0, day_of_week="mon"),
        id="weekly_scholarly_scrape",
        replace_existing=True,
        name="Weekly Scholarly Research Scrape",
    )

    _scheduler.start()
    log.info(
        "Scheduler started — "
        "news scrape at 07:00 Mon–Fri, "
        "scholarly scrape at 08:00 Monday (America/Vancouver)"
    )


def _scrape_job():
    """Wrapper so scheduler import doesn't create circular dependency."""
    from scraper import run_scrape
    log.info("Scheduled daily news scrape triggered")
    run_scrape()


def _scholarly_scrape_job():
    """Weekly scholarly/research scrape using open-access APIs."""
    from scholarly_scraper import run_scholarly_scrape
    from database import get_watchlist_keywords
    log.info("Scheduled weekly scholarly scrape triggered")
    try:
        # Pull the user's watchlist keywords and pass them to the scholarly scraper
        extra_keywords = get_watchlist_keywords()
        run_scholarly_scrape(extra_keywords=extra_keywords)
    except Exception as e:
        log.error(f"Scholarly scrape job error: {e}")
