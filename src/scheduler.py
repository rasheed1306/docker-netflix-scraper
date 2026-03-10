"""
APScheduler daemon: Sunday 2 AM AEST scheduling.
Immediate trigger if >6 days since last run (dormant in Stage 1, active in Stage 2).
"""

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime, timedelta
import pytz
from typing import Callable
import logging

logger = logging.getLogger(__name__)


def start_scheduler(run_job: Callable) -> BackgroundScheduler:
    """
    Start APScheduler daemon.
    
    Args:
        run_job: Callable that executes the scraper job
    
    Returns:
        BackgroundScheduler instance
    """
    scheduler = BackgroundScheduler()
    
    # Sunday 2 AM AEST (UTC+10) = Saturday 4 PM UTC
    # AEST = UTC+10, so Sunday 2 AM AEST = Saturday 4 PM UTC
    aest = pytz.timezone('Australia/Sydney')
    trigger = CronTrigger(
        day_of_week=5,  # Saturday (0=Monday, 5=Saturday)
        hour=16,        # 4 PM UTC
        minute=0,
        timezone=pytz.UTC
    )
    
    scheduler.add_job(
        run_job,
        trigger,
        id='netflix_scraper',
        name='Netflix AU Movie Scraper',
        replace_existing=True
    )
    
    scheduler.start()
    logger.info("Scheduler started. Next run: Sunday 2 AM AEST")
    
    return scheduler


def should_run_immediately(last_run_date) -> bool:
    """
    Check if scraper should run immediately (>6 days since last run).
    
    Args:
        last_run_date: datetime of last ingestion
    
    Returns:
        True if >6 days have passed
    """
    if not last_run_date:
        return True
    
    now = datetime.utcnow()
    delta = now - last_run_date.replace(tzinfo=None)
    return delta.total_seconds() > (6 * 24 * 3600)
