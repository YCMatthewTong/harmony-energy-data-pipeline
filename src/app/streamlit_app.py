import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import streamlit as st

import polars as pl
import polars.selectors as cs

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from pathlib import Path
from typing import Literal, Optional
from datetime import timedelta

from src.db.client import DatabaseClient
from src.pipeline.run import run_pipeline
from src.scheduler.job import start_scheduler
from src.utils.logger import logger
from src.utils.config import load_config

log = logger.bind(step="streamlit")

# Load config
config = load_config(config_path="conf/config.json")
db_config = config.get("db")
db_query_config = db_config.get("query")
app_config = config.get("app")

DB_PATH = Path(db_config.get("path", "data/app.db"))

INTERVALS = app_config.get("intervals", ["30m", "1h", "1d", "1mo", "1y"])
DEFAULT_INTERVAL = app_config.get("default_interval", "1y")
SCHEDULE_INTERVAL = app_config.get("schedule_interval", 60)

# --- Page Config ---
st.set_page_config(
    page_title="Harmony Energy - GB Generation Mix",
    layout="wide",
)


# --- Helper functions ---

def _load_data(query: str) -> pl.DataFrame:
    """
    Loads data from SQLite into a Polars DataFrame.
    Cached for 30 mins.
    """
    with DBClient.get_session() as session:
        df: pl.DataFrame = pl.read_database(
            query=query,
            connection=session.connection()
        )
    log.debug(f"{_load_data.__name__} Loaded data from DB.")
    return df


def parse_date(df: pl.DataFrame) -> pl.DataFrame:
    """
    Parses the DATETIME column to a Polars Datetime type.
    """
    parsed_df = df.with_columns(
        pl.col("DATETIME")
        .str.replace(r"\.\d+$", "")  # Strip .000000 if present
        .str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False)
    )
    
    log.debug("Parsed DATETIME column.")
    return parsed_df

def set_global_date_range(df: pl.DataFrame) -> tuple[pl.Date, pl.Date]:
    """
    Sets (and caches) the global min/max datetime for date input widget.
    Uses `st.session_state` for persistence across reruns.
    """
    if st.session_state.get("min_datetime") is None or st.session_state.get("max_datetime") is None:
        min_dt = df["DATETIME"].min().date()
        max_dt = df["DATETIME"].max().date()
        st.session_state["min_datetime"] = min_dt
        st.session_state["max_datetime"] = max_dt
        log.debug(f"Set global date range: {min_dt} to {max_dt}")
    return st.session_state["min_datetime"], st.session_state["max_datetime"]

def downsample_date(df: pl.DataFrame, interval: Optional[Literal["30m", "1h", "1d", "1mo", "1y"]] = DEFAULT_INTERVAL) -> pl.DataFrame:
    """
    Bins the DATETIME column to the specified interval, taking the mean of other columns.
    """
    interval = "30m" if not interval else interval
    log.debug(f"Downsampling to {interval} interval.")
    return (
        df.group_by_dynamic("DATETIME", every=interval)
        .agg(
            pl.col("*").mean(),
        ).sort("DATETIME")
    )


def interval_selector(default_interval: Literal["30m", "1h", "1d", "1mo", "1y"]=DEFAULT_INTERVAL):
    selected_interval = st.radio(
        "Select interval",
        INTERVALS,
        index=INTERVALS.index(default_interval),
        horizontal=True
    )
    log.debug(f"Selected interval: {selected_interval}")

    return selected_interval


def date_range_selector(
        df: pl.DataFrame, 
        interval: Literal["30m", "1h", "1d", "1mo", "1y"] = DEFAULT_INTERVAL,
    ):
    """
    Creates a date range selector based on the DataFrame's date column and the selected interval.
    Returns (`start_date`, `end_date`) only if both are selected.
    If not, returns `None` and avoids refreshing the charts.
    """
    # Determine min/max dates in dataset
    min_date, max_date = set_global_date_range(df)
    start_date = st.session_state.get("start_date")
    end_date = st.session_state.get("end_date")

    # Compute default range based on interval
    if interval == "30m":
        default_start = max_date - timedelta(days=7)  # last week
    elif interval == "1h":
        default_start = max_date - timedelta(days=14)  # last 2 weeks 
    elif interval == "1d":
        default_start = max_date - timedelta(weeks=13)  # last quarter
    elif interval == "1mo":
        default_start = max_date - timedelta(days=365*5)  # Last 5 yrs
    else:
        default_start = min_date  # since start

    # Set first rendered range - use default values if no cached values
    input_range = [
        start_date if start_date is not None else default_start,
        end_date if end_date is not None else max_date,
    ]
    # Date input with default
    selected = st.date_input(
        "Select range",
        value=input_range,
        min_value=min_date,
        max_value=max_date
    )

    # Validate user input
    if not isinstance(selected, (list, tuple)) or len(selected) != 2:
        st.warning("Please select both start and end dates to continue.")
        return None, None

    start_date, end_date = selected

    # Ensure logical order
    if start_date > end_date:
        st.warning("Start date must be before end date.")
        return None, None

    log.debug(f"Date range selected: {start_date} to {end_date}")

    # Cache range if changed from default
    if start_date != default_start or end_date != max_date:
        st.session_state["start_date"] = start_date
        st.session_state["end_date"] = end_date
    return start_date, end_date


def filter_by_date(df: pl.DataFrame, start_date, end_date):
    return df.filter(
        pl.col("DATETIME").is_between(pl.lit(start_date), pl.lit(end_date))
    )


def load_generation_data() -> pl.DataFrame:
    """
    Loads generation data from the database or cache.
    Update cache if data is changed.
    """
    data_query = db_query_config.get("generation_data", "SELECT * FROM generation ORDER BY DATETIME ASC;")
    data_version_query = db_query_config.get("data_version", "SELECT MAX(_id) AS _id FROM generation;")

    data_version_cached = st.session_state.get("data_version")
    # Get latest MAX _id value
    data_version_new = _load_data(query=data_version_query)["_id"][0]
    
    # Refresh data and cache if data version has changed
    if data_version_cached != data_version_new:
        gen_df = _load_data(query=data_query)
        log.debug("Loaded generation data from DB.")
        
        # Store data in session cache
        st.session_state["gen_df"] = gen_df
        
        # Store data version
        st.session_state["data_version"] = data_version_new
        log.debug("Cached generation data.")        
        
    return st.session_state["gen_df"]


def get_last_refresh_dt() -> str:
    """
    Retrieves the timestamp of the last successful pipeline run from the database.
    """
    query = (
        f'SELECT run_stop '
        f'FROM pipeline_run_history '
        f'WHERE success=1 '
        f'ORDER BY run_stop DESC '
        f'LIMIT 1 '
    )
    query = db_query_config.get("last_refresh_dt", query)
    last_run_df = _load_data(query=query)
    last_refresh_dt = last_run_df["run_stop"][0]
    log.debug(f"Last refresh: {last_refresh_dt}")

    return last_refresh_dt


# --------------------------
# Setup
# --------------------------
# --- DB ---
DBClient = DatabaseClient(db_path=DB_PATH)
# Initialise DB if it doesn't exist
DBClient.init_db()


# --- Background Pipeline Scheduler ---
@st.cache_resource
def start_background_scheduler(interval: int = SCHEDULE_INTERVAL):
    """Start background scheduler once per session."""
    if not st.session_state.get("scheduler_started", False):
        scheduler = start_scheduler(
            run_pipeline, 
            kwargs={"db_client": DBClient,}, 
            interval_minutes=interval, 
            id="pipeline_job"
        )
        log.info("Background scheduler started.")
        st.session_state["scheduler_started"] = True
        return scheduler
    else:
        return None
    
# Initialize scheduler for the session
start_background_scheduler()


# --------------------------
# Load Data
# --------------------------
df = load_generation_data()
if df.is_empty():
    st.warning("No data found in database.")
    st.stop()

df = parse_date(df)
# set_global_date_range(df)
col1, col2, col3 = st.columns([2, 1, 1], vertical_alignment="center")

with col1:
    st.title("NESO Generation Mix Dashboard")
    # Add callback/button to refresh data
    last_refresh_dt = get_last_refresh_dt()
    st.caption(f"Data from NESO Historic Generation Mix API. Refreshed hourly. Last refresh: {last_refresh_dt}")
with col2:
    selected_interval = interval_selector(default_interval=DEFAULT_INTERVAL)
with col3:
    start_date, end_date = date_range_selector(df, interval=selected_interval)


df = downsample_date(df, interval=selected_interval)
df = filter_by_date(df, start_date=start_date, end_date=end_date)

# --------------------------
# Layout
# --------------------------


# --- Visuals ---
app_viz_config = app_config.get("viz")

FUEL_COLS = app_viz_config["fuel_cols"]
DT_COL = app_viz_config["datetime_col"]
ZC_COL = app_viz_config["zero_carbon_col"]
ZC_PERC_COL = ZC_COL+"_perc"
CI_COL = app_viz_config["carbon_intensity_col"]
GEN_COL = app_viz_config["generation_col"]

# Creat column containers
col1, col2 = st.columns(2)
with col1:

    # --- MWh Mix ---
    fuel_df = df.select(DT_COL, cs.by_name(FUEL_COLS, require_all=True))
    # Melt wide table into long format
    mix_long_df = fuel_df.unpivot(index=DT_COL, variable_name="Fuel", value_name="value")

    chart_mix_mw = px.area(
        mix_long_df,
        x="DATETIME",
        y="value",
        color="Fuel",
        title="Fuel Mix (MWh)"
    )

    ## Further customisations
    # Solid area colour
    chart_mix_mw.for_each_trace(lambda trace: trace.update(
        fillcolor = trace.line.color,  # Solid area colour
        name = trace.name.replace("_", " ").title()  # Clean labels
    ))
    # Update all traces
    chart_mix_mw.update_traces(
        hovertemplate="%{y:.0f}",  # Hover
        line=dict(width=0),  # Line width
    )
    # Update chart layout
    chart_mix_mw.update_layout(
        xaxis_title=None,
        yaxis_title="Generation (MWh)",
        hovermode="x unified",
        legend=dict(orientation="h", xanchor="left", x=0),
        margin=dict(l=40, r=40, t=50, b=40),
    )

    st.plotly_chart(chart_mix_mw, use_container_width=True)
    log.debug("Chart (MWh Mix) displayed")




    # --- % Mix ---
    fuel_perc_cols = [c + "_perc" for c in FUEL_COLS]
    mix_perc_df = df.select(DT_COL, cs.by_name(fuel_perc_cols, require_all=False))
    mix_perc_long_df = mix_perc_df.unpivot(index="DATETIME", variable_name="Fuel", value_name="value")

    chart_mix_perc = px.area(
        mix_perc_long_df,
        x="DATETIME",
        y="value",
        color="Fuel",
        title="Fuel Mix (%)"
    )
    ## Further customisations
    # Update each trace
    chart_mix_perc.for_each_trace(lambda trace: trace.update(
        fillcolor = trace.line.color,  # Solid area colour
        name = trace.name.rstrip("_perc").replace("_", " ").title()  # Clean labels
    ))
    # Update all traces
    chart_mix_perc.update_traces(
        hovertemplate="%{y:.2f} %",  # Hover
        line=dict(width=0),  # Line width
    )
    # Update chart layout
    chart_mix_perc.update_layout(
        xaxis_title=None,
        yaxis_title="Mix (%)",
        hovermode="x unified",
        legend=dict(orientation="h", xanchor="left", x=0),
        margin=dict(l=40, r=40, t=50, b=40),
    )

    st.plotly_chart(chart_mix_perc, use_container_width=True)
    log.debug("Chart (% Mix) displayed")



with col2:
    # --- Zero Carbon vs Carbon Generation ---
    carbon_mix_df = df.select(
        DT_COL, ZC_COL,
        (pl.col(GEN_COL) - pl.col(ZC_COL)).alias("CARBON")
    )

    chart_carbon_mix = px.line(
        carbon_mix_df,
        x=DT_COL,
        y=[ZC_COL, "CARBON"],
        title="Zero-Carbon vs Carbon Generation (MWh)",
        color_discrete_map={
            ZC_COL: "green",
            "CARBON": "grey",
        },
    )

    ## Further customisation
    # Rename labels
    rename_map = {
        ZC_COL: "Zero Carbon",
        "CARBON": "Carbon"
    }
    chart_carbon_mix.for_each_trace(
        lambda t: t.update(name=rename_map.get(t.name, t.name))
    )

    # Update trace config
    chart_carbon_mix.update_traces(
        hovertemplate="%{y:.0f} MWh",  # Hover
        line=dict(width=1),  # Line width
    )
    # Update chart layout
    chart_carbon_mix.update_layout(
        legend_title_text=None,
        xaxis_title=None,
        yaxis_title="Generation (MWh)",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=40, r=40, t=50, b=40),

    )

    st.plotly_chart(chart_carbon_mix, use_container_width=True)
    log.debug("Chart (ZC vs C Generation) displayed")





    # -- Zero Carbon % vs Carbon Intensity ---
    zcop_ci_df = df.select([DT_COL, ZC_PERC_COL, CI_COL])

    # Create subplot with secondary y-axis
    zcop_ci_chart = make_subplots(specs=[[{"secondary_y": True}]])

    # Add Zero Carbon % trace
    zcop_ci_chart.add_trace(
        go.Scatter(
            x=zcop_ci_df.get_column(DT_COL),
            y=zcop_ci_df.get_column(ZC_PERC_COL),
            name="Zero Carbon %",
            mode="lines",
            line=dict(color="green", width=1),
            hovertemplate="%{y:.0f} %",
        ),
        secondary_y=False,
    )

    # Add Carbon Intensity trace
    zcop_ci_chart.add_trace(
        go.Scatter(
            x=zcop_ci_df.get_column(DT_COL),
            y=zcop_ci_df.get_column(CI_COL),
            name="Carbon Intensity (gCO₂/kWh)",
            mode="lines",
            line=dict(color="grey", width=1),
            hovertemplate="%{y:.0f} g/kWh",
        ),
        secondary_y=True,
    )

    # Update layout
    zcop_ci_chart.update_layout(
        title="Zero Carbon % vs Carbon Intensity",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=40, r=40, t=50, b=40),
    )

    # Set axis titles
    # zcop_ci_chart.update_xaxes(title_text="Datetime")
    zcop_ci_chart.update_xaxes(title_text=None)
    zcop_ci_chart.update_yaxes(title_text="ZCO %", secondary_y=False)
    zcop_ci_chart.update_yaxes(title_text="Carbon Intensity (gCO₂/kWh)", secondary_y=True)

    st.plotly_chart(zcop_ci_chart, use_container_width=True)
    log.debug("Chart (ZC% vs CI) displayed")



st.markdown("---")
st.caption("Created for Harmony Energy tech test.")