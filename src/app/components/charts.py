# src/app/components/charts.py
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import polars as pl
import polars.selectors as cs
from src.utils.logger import logger

log = logger.bind(step="st-charts")

def chart_fuel_mix(df: pl.DataFrame, dt_col: str, fuel_cols: list[str]):
    """
    Generates and displays a stacked area chart for fuel mix in MWh.

    Args:
        df (pl.DataFrame): The input DataFrame.
        dt_col (str): The name of the datetime column.
        fuel_cols (list[str]): A list of fuel type column names.
    """
    # Select relevant columns and unpivot to long format for plotting
    fuel_df = df.select(dt_col, cs.by_name(fuel_cols, require_all=True))
    mix_long = fuel_df.unpivot(index=dt_col, variable_name="Fuel", value_name="value")

    # Create the area chart
    chart = px.area(mix_long, x=dt_col, y="value", color="Fuel", title="Fuel Mix (MWh)")

    # Customize trace appearance
    chart.for_each_trace(lambda t: t.update(fillcolor=t.line.color, name=t.name.replace("_", " ").title()))
    chart.update_traces(hovertemplate="%{y:.0f}", line=dict(width=0))

    # Customize chart layout
    chart.update_layout(
        xaxis_title=None, yaxis_title="Generation (MWh)",
        hovermode="x unified",
        legend=dict(orientation="h", xanchor="left", x=0),
        margin=dict(l=40, r=40, t=50, b=40)
    )

    # Display the chart in Streamlit
    st.plotly_chart(chart, use_container_width=True)
    log.debug("Fuel mix chart rendered.")


def chart_fuel_mix_perc(df: pl.DataFrame, dt_col: str, fuel_cols: list[str]):
    """
    Generates and displays a stacked area chart for fuel mix in percentage.

    Args:
        df (pl.DataFrame): The input DataFrame.
        dt_col (str): The name of the datetime column.
        fuel_cols (list[str]): A list of fuel type column names.
    """
    # Select percentage columns and unpivot to long format
    perc_cols = [f + "_perc" for f in fuel_cols]
    df_long = df.select(dt_col, cs.by_name(perc_cols, require_all=False)).unpivot(
        index=dt_col, variable_name="Fuel", value_name="value"
    )
    chart = px.area(df_long, x=dt_col, y="value", color="Fuel", title="Fuel Mix (%)")
    chart.for_each_trace(lambda t: t.update(
        fillcolor=t.line.color, name=t.name.rstrip("_perc").replace("_", " ").title()
    ))

    # Customize trace appearance
    chart.update_traces(hovertemplate="%{y:.2f} %", line=dict(width=0))

    # Customize chart layout
    chart.update_layout(
        xaxis_title=None, yaxis_title="Mix (%)",
        hovermode="x unified",
        legend=dict(orientation="h", xanchor="left", x=0),
        margin=dict(l=40, r=40, t=50, b=40),
    )

    # Display the chart in Streamlit
    st.plotly_chart(chart, use_container_width=True)
    log.debug("Fuel mix % chart rendered.")


def chart_carbon_vs_zero(df: pl.DataFrame, dt_col: str, zc_col: str, gen_col: str):
    """
    Generates and displays a line chart comparing zero-carbon vs. carbon-emitting generation.

    Args:
        df (pl.DataFrame): The input DataFrame.
        dt_col (str): The name of the datetime column.
        zc_col (str): The name of the zero-carbon generation column.
        gen_col (str): The name of the total generation column.
    """
    # Calculate carbon generation by subtracting zero-carbon from total generation
    df = df.select(dt_col, zc_col, (pl.col(gen_col) - pl.col(zc_col)).alias("CARBON"))

    # Create the line chart
    chart = px.line(df, x=dt_col, y=[zc_col, "CARBON"], title="Zero-Carbon vs Carbon Generation (MWh)",
                    color_discrete_map={zc_col: "green", "CARBON": "grey"})

    # Customize trace appearance
    chart.for_each_trace(lambda t: t.update(name={"ZC_MW": "Zero Carbon", "CARBON": "Carbon"}.get(t.name, t.name)))
    chart.update_traces(hovertemplate="%{y:.0f} MWh", line=dict(width=1))

    # Customize chart layout
    chart.update_layout(
        legend_title_text=None, xaxis_title=None, yaxis_title="Generation (MWh)",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=40, r=40, t=50, b=40),
    )

    # Display the chart in Streamlit
    st.plotly_chart(chart, use_container_width=True)
    log.debug("Carbon vs Zero Carbon chart rendered.")


def chart_zc_perc_vs_ci(df: pl.DataFrame, dt_col: str, zc_perc_col: str, ci_col: str):
    """
    Generates and displays a dual-axis line chart for Zero Carbon % vs. Carbon Intensity.

    Args:
        df (pl.DataFrame): The input DataFrame.
        dt_col (str): The name of the datetime column.
        zc_perc_col (str): The name of the zero-carbon percentage column.
        ci_col (str): The name of the carbon intensity column.
    """
    # Create a subplot with a secondary y-axis
    chart = make_subplots(specs=[[{"secondary_y": True}]])

    # Add Zero Carbon % trace to the primary y-axis
    chart.add_trace(go.Scatter(
        x=df.get_column(dt_col), y=df.get_column(zc_perc_col),
        name="Zero Carbon %", mode="lines",
        line=dict(color="green", width=1),
        hovertemplate="%{y:.0f} %",
    ), secondary_y=False)

    # Add Carbon Intensity trace to the secondary y-axis
    chart.add_trace(go.Scatter(
        x=df.get_column(dt_col), y=df.get_column(ci_col),
        name="Carbon Intensity (gCO₂/kWh)", mode="lines",
        line=dict(color="grey", width=1),
        hovertemplate="%{y:.0f} g/kWh",
    ), secondary_y=True)

    # Customize chart layout
    chart.update_layout(
        title="Zero Carbon % vs Carbon Intensity",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=40, r=40, t=50, b=40),
    )

    # Customize axes
    chart.update_xaxes(title_text=None)
    chart.update_yaxes(title_text="ZCO %", secondary_y=False, showgrid=True)
    chart.update_yaxes(title_text="Carbon Intensity (gCO₂/kWh)", secondary_y=True, showgrid=False, matches=None)

    # Display the chart in Streamlit
    st.plotly_chart(chart, use_container_width=True)
    log.debug("ZC% vs CI chart rendered.")
