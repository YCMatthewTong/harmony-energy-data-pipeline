from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime

from src.utils.logger import logger


log = logger.bind(step="scheduler")


def start_scheduler(
        func, 
        args: list|tuple = None, 
        kwargs: dict = None, 
        interval_minutes: int = 60, 
        id="pipeline_job"
    ):
    """
    Starts background scheduler to run pipeline with given interval.
    """
    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(
        func, 
        args=args,
        kwargs=kwargs,
        trigger="interval", 
        minutes=interval_minutes, 
        next_run_time=datetime.now(),
        replace_existing=True,
        id=id,
    )
    scheduler.start()
    log.success(f"{func.__name__} scheduler started — running every {interval_minutes} min")
    
    return scheduler


if __name__ == "__main__":
    start_scheduler(interval_minutes=60)
