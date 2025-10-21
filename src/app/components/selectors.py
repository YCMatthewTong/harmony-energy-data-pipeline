# src/app/components/selectors.py
import streamlit as st
from datetime import timedelta
import polars as pl
from src.utils.logger import logger

log = logger.bind(step="st-selectors")

def _cache_global_date_range(df: pl.DataFrame):
    """Cache min/max DATETIME for consistent UI across reruns."""
    if "min_datetime" not in st.session_state or "max_datetime" not in st.session_state:
        min_dt = df["DATETIME"].min().date()
        max_dt = df["DATETIME"].max().date()
        st.session_state["min_datetime"] = min_dt
        st.session_state["max_datetime"] = max_dt
        log.debug(f"Global range: {min_dt} - {max_dt}")
    return st.session_state["min_datetime"], st.session_state["max_datetime"]


def interval_selector(intervals: list[str], default_interval: str = "1y"):
    """Select the aggregation interval."""
    selected = st.radio(
        "Select interval",
        intervals,
        index=intervals.index(default_interval),
        horizontal=True,
    )
    log.debug(f"Interval selected: {selected}")
    return selected


def date_range_selector(df: pl.DataFrame, interval: str = "1y"):
    """Date range input with sensible defaults per interval."""
    min_date, max_date = _cache_global_date_range(df)
    # Use session state for persistence, or calculate defaults
    start_date = st.session_state.get("start_date")
    end_date = st.session_state.get("end_date")
    # Compute default range based on selected interval
    defaults = {
        "30m": max_date - timedelta(days=7),
        "1h":  max_date - timedelta(days=14),
        "1d":  max_date - timedelta(weeks=13),
        "1mo": max_date - timedelta(days=365 * 5),
        "1y":  min_date,
    }
    default_start = defaults.get(interval, min_date)

    # Display date input selector with default
    selected = st.date_input(
        "Select range",
        value=[
            start_date or default_start,
            end_date or max_date,
        ],
        min_value=min_date,
        max_value=max_date,
    )

    # Validate user input
    if not isinstance(selected, (list, tuple)) or len(selected) != 2:
        st.warning("Please select both start and end dates.")
        return None, None

    # Ensure logical order
    start_date, end_date = selected
    if start_date > end_date:
        st.warning("Start date must be before end date.")
        return None, None
    
    # Cache range if changed from default
    if start_date != default_start or end_date != max_date:
        st.session_state["start_date"], st.session_state["end_date"] = start_date, end_date
    log.debug(f"Range selected: {start_date} -> {end_date}")
    return start_date, end_date
