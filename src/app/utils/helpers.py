import streamlit as st
import polars as pl

from typing import Literal, Optional

from src.utils.logger import logger
from src.scheduler.job import start_scheduler
from src.pipeline.run import run_pipeline
from src.db.client import DatabaseClient
from src.db.models import PipelineRunHistory, Generation


log = logger.bind(step="st-helpers")

@st.cache_resource()
def start_background_scheduler(_db_client: DatabaseClient, interval: int = 60, job_id: str = "pipeline_job"):
    """Start background scheduler once per session."""
    if "scheduler" not in st.session_state:
        scheduler = start_scheduler(
            run_pipeline, 
            kwargs={"db_client": _db_client,}, 
            interval_minutes=interval, 
            id=job_id,
        )
        st.session_state["scheduler"] = scheduler
        log.info(f"Background scheduler job ({job_id}) started.")

    return st.session_state["scheduler"]


def downsample_date(
        df: pl.DataFrame, 
        interval: Optional[Literal["30m", "1h", "1d", "1mo", "1y"]] = "1y",
    ) -> pl.DataFrame:
    """
    Bins the DATETIME column to the specified interval, taking the mean of other columns.
    """
    interval = "30m" if not interval else interval
    log.debug(f"Downsampling to {interval} interval.")
    return (
        df.group_by_dynamic("DATETIME", every=interval)
        .agg(
            pl.col("*").mean(),
        )
        .sort("DATETIME")
    )


def filter_by_date(
        df: pl.DataFrame, 
        start_date: pl.Datetime, 
        end_date: pl.Datetime,
    ) -> pl.DataFrame:
    """
    Filters the DataFrame to include only rows within the specified date range.
    """
    return df.filter(
        pl.col("DATETIME").is_between(pl.lit(start_date), pl.lit(end_date))
    )
