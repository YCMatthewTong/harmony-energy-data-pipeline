from functools import wraps
from datetime import datetime, timezone

from src.db.models import PipelineRunHistory
from src.db.client import DatabaseClient

from src.utils.logger import logger

log = logger.bind(step="pipeline-run-history")

def pipeline_run_tracker(func):
    """
    Decorator to automatically log pipeline run start/end events
    and persist run details in the database.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        # session: Session = SessionLocal()
        DbClient:DatabaseClient = kwargs.get("db_client", None)
        with DbClient.get_session() as session:
            run = PipelineRunHistory(run_start=datetime.now(timezone.utc))
            session.add(run)
            session.commit()
            session.refresh(run)  # Refresh to get run.id
            log.info(f"Pipeline run {run.id} started at {run.run_start}")

            success = False
            total_fetched = 0
            valid_records = 0
            last_fetched_id = None
            error_message = None

            try:
                # Run the actual job
                result = func(*args, **kwargs)
                # Optional: function can return metrics for richer tracking
                if isinstance(result, dict):
                    total_fetched = result.get("total_fetched", 0)
                    valid_records = result.get("valid_records", 0)
                    last_fetched_id = result.get("last_fetched_id")
                success = True
                return result

            except Exception as e:
                error_message = str(e)
                log.exception(f"Pipeline run {run.id} failed: {error_message}")
                raise

            finally:
                run.run_stop = datetime.now(timezone.utc)
                run.last_fetched_id = last_fetched_id
                run.total_fetched = total_fetched
                run.valid_records = valid_records
                run.success = success
                run.error_message = error_message
                session.commit()
                log.success(f"Pipeline run {run.id} finished. Success: {success}")
                session.close()

    return wrapper
