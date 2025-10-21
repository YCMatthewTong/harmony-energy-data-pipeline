import streamlit as st
import polars as pl
from sqlalchemy import select, desc, asc, func

from src.db.client import DatabaseClient
from src.db.models import PipelineRunHistory, Generation
from src.utils.logger import logger

log = logger.bind(step="st-data_loader")


def _query_db(query: str, db_client: DatabaseClient) -> pl.DataFrame:
    """
    Executes a SQL query and returns the result as a Polars DataFrame.
    """
    with db_client.get_session() as session:
        df: pl.DataFrame = pl.read_database(
            query=query,
            connection=session.connection()
        )
    log.debug(f"Loaded data from DB via query.")
    return df


def load_generation_data(db_client: DatabaseClient) -> pl.DataFrame:
    """
    Loads generation data from the database, using Streamlit's session cache.

    It checks for new data by comparing the max `_id` and refreshes the
    cached DataFrame if necessary.
    """
    data_query = select(Generation).order_by(asc(Generation.DATETIME))
    data_version_query = select(func.max(Generation._id))

    # Get latest MAX _id value to use as a data version
    data_version_results = _query_db(query=data_version_query, db_client=db_client)
    data_version_new = data_version_results[0, 0] if not data_version_results.is_empty() else 0

    # Load from cache
    gen_df = st.session_state.get("gen_df")
    data_version_cached = st.session_state.get("data_version")

    # Refresh data and cache if it's missing or the data version has changed
    if gen_df is None or data_version_cached != data_version_new:
        gen_df = _query_db(query=data_query, db_client=db_client)
        log.debug("Loaded generation data from DB.")

        # Store data and version in session cache
        st.session_state["gen_df"] = gen_df
        st.session_state["data_version"] = data_version_new
        log.debug("Cached new generation data.")
    else:
        log.debug("Loaded cached generation data.")

    return gen_df


def get_last_refresh_dt(db_client: DatabaseClient) -> str:
    """
    Retrieves the timestamp of the last successful pipeline run from the database.
    """
    query = (
        select(PipelineRunHistory.run_stop)
        .where(PipelineRunHistory.success == True)
        .order_by(desc(PipelineRunHistory.run_stop))
        .limit(1)
    )
    last_run_df = _query_db(query=query, db_client=db_client)
    last_refresh_dt = last_run_df["run_stop"][0] if not last_run_df.is_empty() else None
    log.debug(f"Last refresh: {last_refresh_dt}")

    return last_refresh_dt
