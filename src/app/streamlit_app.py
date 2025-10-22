import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import streamlit as st

from pathlib import Path
from datetime import timezone

from src.db.client import DatabaseClient

from src.utils.logger import logger
from src.utils.config import load_config

from src.app.utils.data_loader import (
    load_generation_data,
    get_last_refresh_dt,
)
from src.app.utils.helpers import downsample_date, filter_by_date, start_background_scheduler
from src.app.components.selectors import interval_selector, date_range_selector
from src.app.components.charts import (
    chart_fuel_mix,
    chart_fuel_mix_perc,
    chart_carbon_vs_zero,
    chart_zc_perc_vs_ci,
)


log = logger.bind(step="streamlit", component="app")
log.info("Streamlit app loading...")

# --- Load config ---
config = load_config(config_path="conf/config.json")
db_config = config.get("db")
db_query_config = db_config.get("query")
app_config = config.get("app")

DB_PATH = Path(db_config.get("path", "data/app.db"))

INTERVALS = app_config.get("intervals", ["30m", "1h", "1d", "1mo", "1y"])
DEFAULT_INTERVAL = app_config.get("default_interval", "1y")
SCHEDULE_INTERVAL = app_config.get("schedule_interval", 60)



# --------------------------
# Setup
# --------------------------
# --- DB ---
DBClient = DatabaseClient(db_path=DB_PATH)
# Initialise DB if it doesn't exist
DBClient.init_db()


# --- Background Pipeline Scheduler --- 
# Initialize scheduler for the session
scheduler = start_background_scheduler(_db_client=DBClient, interval=SCHEDULE_INTERVAL, job_id="pipeline_job")
log.info(f"Next schedueled run: {scheduler.get_job('pipeline_job').next_run_time.astimezone(timezone.utc).isoformat()}")

# --------------------------
# Load Data
# --------------------------
df = load_generation_data(db_client=DBClient)
if df.is_empty():
    st.warning("No data found in database.")
    st.stop()


# --------------------------
# Layout
# --------------------------

# --- Page Config ---
st.set_page_config(
    page_title="Harmony Energy - GB Generation Mix",
    layout="wide",
)


# --- Heading ---
col1, col2, col3 = st.columns([2, 1, 1], vertical_alignment="center")
with col1:
    st.title("NESO Generation Mix Dashboard")
    # Add callback/button to refresh data
    last_refresh_dt = get_last_refresh_dt(db_client=DBClient)
    st.caption(f"Data from NESO Historic Generation Mix API. Refreshed hourly. Last refresh: {last_refresh_dt}")
with col2:
    selected_interval = interval_selector(intervals=INTERVALS, default_interval=DEFAULT_INTERVAL)
with col3:
    start_date, end_date = date_range_selector(df, interval=selected_interval)


df = downsample_date(df, interval=selected_interval)
df = filter_by_date(df, start_date=start_date, end_date=end_date)


# --- Visuals ---
app_viz_config = app_config.get("viz")

fuel_cols = app_viz_config["fuel_cols"]
dt_col = app_viz_config["datetime_col"]
zc_col = app_viz_config["zero_carbon_col"]
zc_perc_col = zc_col+"_perc"
ci_col = app_viz_config["carbon_intensity_col"]
gen_col = app_viz_config["generation_col"]


col1, col2 = st.columns(2)
with col1:
    chart_fuel_mix(df, dt_col=dt_col, fuel_cols=fuel_cols)
    chart_fuel_mix_perc(df, dt_col=dt_col, fuel_cols=fuel_cols)
with col2:
    chart_carbon_vs_zero(df, dt_col=dt_col, zc_col=zc_col, gen_col=gen_col)
    chart_zc_perc_vs_ci(df, dt_col=dt_col, zc_perc_col=zc_perc_col, ci_col=ci_col)


st.markdown("---")
st.caption("Created for Harmony Energy tech test.")

log.info("Streamlit app loaded.")