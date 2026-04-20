"""
PolicyPulse Scheduler — runs scraper daily at 7:00 AM UTC (adjust as needed).
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

    # Daily scrape at 7:00 AM Vancouver time
    _scheduler.add_job(
        _scrape_job,
        CronTrigger(hour=7, minute=0),
        id="daily_scrape",
        replace_existing=True,
        name="Daily Policy Scrape",
    )

    _scheduler.start()
    log.info("Scheduler started — daily scrape at 07:00 America/Vancouver")


def _scrape_job():
    """Wrapper so scheduler import doesn't create circular dependency."""
    from scraper import run_scrape
    log.info("Scheduled scrape triggered")
    run_scrape()
