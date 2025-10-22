import polars as pl
import polars.selectors as cs

from typing import List, Dict
from datetime import datetime

from src.db.models import Generation
from src.utils.logger import logger

log = logger.bind(step="transform_records")



def transform_records(records: List[Dict]) -> pl.DataFrame:
    """
    Cleans, validates, and standardizes NESO generation records.

    Steps:
    1. Convert JSON records to Polars DataFrame
    2. Align schema to Generation model
    3. Parse timestamps and cast numeric columns
    4. Validate mix % consistency and missing data
    4. Deduplicate overlapping data (`_id` and `DATETIME`)
    """
    if not records:
        log.warning("No records to transform.")
        return pl.DataFrame()

    start_time = datetime.now()
    records_count = len(records)
    log.info(f"Transforming {records_count} records...")

    # --- Create DataFrame
    # Read DATETIME as String in DF
    df = pl.DataFrame(records, schema_overrides={"DATETIME": pl.String})

    model_cols = [c.name for c in Generation.__table__.columns]
    numeric_cols = [c for c in model_cols if c not in ("_id", "DATETIME")]

    # --- Schema alignment
    # df = df.select(cs.by_name(model_cols, require_all=True))
    df = _align_schema(df, expected_cols=model_cols)

    # --- Parse and type cast
    df = _parse_and_cast(df, dt_col="DATETIME", numeric_cols=numeric_cols)


    # --- Validations
    issues = []
    # Validate percentages
    df, invalid_perc_count = _validate_perc_consistency(df)
    if invalid_perc_count > 0:
        issues.append(("Inconsistent mix % ", invalid_perc_count))

    # Handle missing values
    df, missing_count = _validate_missing_values(df)
    if missing_count > 0:
        issues.append(("Rows with missing data", missing_count))

    # Deduplication
    df, duplicate_count = _deduplicate(df)
    if duplicate_count > 0:
        issues.append(("Duplicate/overlapping rows", duplicate_count))



    # --- Generate Data Quality Summary
    quality_df = _generate_quality_summary(
        records_count,
        df.height,
        issues
    )

    log.info("Data Quality Summary: \n" + quality_df.__str__())
    log.success(
        f"Transformation complete — {df.height} valid records. Took {(datetime.now() - start_time).total_seconds():.2f}s"
    )

    return df







# --------------------------
# Helper functions
# --------------------------

def _align_schema(df: pl.DataFrame, expected_cols: list[str]) -> pl.DataFrame:
    """
    Ensures df matches the expected schema:
      - Adds missing columns with nulls
      - Drops unexpected/extra columns
      - Returns aligned DataFrame and logs summary
    """
    df_cols = set(df.columns)
    expected = set(expected_cols)

    missing_cols = list(expected - df_cols)
    extra_cols = list(df_cols - expected)

    # Log changes
    if missing_cols:
        log.warning(f"Adding missing columns (filled with nulls): {missing_cols}")
    if extra_cols:
        log.info(f"Dropping unexpected/extra columns: {extra_cols}")

    # Add missing columns as nulls
    df = df.with_columns([
        pl.lit(None).alias(col) for col in missing_cols
    ])

    # Keep only expected
    df = df.select(cs.by_name(expected_cols, require_all=False))

    return df



def _parse_and_cast(df: pl.DataFrame, dt_col: str = "DATETIME", numeric_cols: List[str] = None) -> pl.DataFrame:
    """
    Parses the DATETIME column to a Polars Datetime type and casts numeric columns.
    """
    return (
        df.with_columns(
            pl.col(dt_col).str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S", strict=False)
            .alias(dt_col)
        )
        .sort(dt_col)
        .with_columns([
            # Cast numerics as float
            pl.col(c).cast(pl.Float64, strict=False) for c in numeric_cols
        ])
    )


def _validate_perc_consistency(df: pl.DataFrame, tolerance: float = 1.0):
    """
    Verify that each *_perc column aligns with actual MW value vs total GENERATION.
    """
    fuel_cols = [
        "BIOMASS", "COAL", "GAS", "HYDRO", "IMPORTS",
        "NUCLEAR", "OTHER", "SOLAR", "STORAGE", "WIND_EMB", "WIND",
    ]

    inconsistencies = []
    total_invalid_count = 0
    for col in fuel_cols:
        perc_col = f"{col}_perc"
        if perc_col not in df.columns:
            continue
        
        calc_perc_col = f"{perc_col}_calc"
        perc_diff_col = f"{perc_col}_diff"
        # recompute % from absolute generation
        df = df.with_columns(
            (pl.col(col) / pl.col("GENERATION")*100)
            .alias(calc_perc_col)
        ).with_columns(  # Compare with percentages from raw data
            (pl.col(calc_perc_col) - pl.col(perc_col)).abs()
            .alias(f"{perc_diff_col}")
        )

        bad_rows_count = df.filter(pl.col(f"{perc_diff_col}") > tolerance).height
        # Use calculated values over raw if inconsistent
        df = df.with_columns(
            pl.when(pl.col(f"{perc_diff_col}") > tolerance)
            .then(pl.col(calc_perc_col))
            .otherwise(pl.col(perc_col))
            .alias(perc_col)
        )
        if bad_rows_count > 0:
            inconsistencies.append((col, bad_rows_count))
            total_invalid_count += bad_rows_count
            log.warning(f"{bad_rows_count} rows have inconsistent % for {col} (>±{tolerance}%)")

    # Drop helper columns
    df = df.drop(cs.ends_with("_diff", "_calc"))

    return df, total_invalid_count






def _validate_missing_values(df: pl.DataFrame) -> pl.DataFrame:
    """Check for and handle missing or null data."""

    before = df.height
    # Filter for rows with null values in any column
    null_row_count = df.filter(pl.any_horizontal(pl.all().is_null())).height
    if null_row_count > 0:
        log.warning(f"Detected {null_row_count} rows with null values.")
        # Drop null _id's & timestamps
        df = df.drop_nulls(subset=["_id", "DATETIME"])
        # Fill null numerics with 0.0
        df = df.with_columns(cs.float().fill_null(0.0))
        log.info(f"Removed {df.height - before} rows with missing _id or DATETIME.")

    return df, null_row_count


def _deduplicate(df: pl.DataFrame) -> tuple[pl.DataFrame, int]:
    """Drop duplicate _id and overlapping timestamps."""

    before = df.height

    # Drop duplicated _id's - prefer latest timestamp
    df = df.sort("DATETIME").unique(subset=["_id"], keep="last")
    # Drop duplicated timestamps - prefer latest _id
    df = df.sort("_id").unique("DATETIME", keep="last", maintain_order=True)

    # Count removed rows
    removed = before - df.height
    if removed > 0:
        log.info(f"Removed {removed} duplicate/overlapping rows.")
    return df, removed


def _generate_quality_summary(total_raw: int, total_clean: int, issues: list) -> pl.DataFrame:
    """Builds a Polars DataFrame summarizing validation results."""
    summary_data = [
        ("Total raw records", total_raw),
        ("Valid cleaned records", total_clean),
        ("Dropped / invalid records", total_raw - total_clean),
    ]
    summary_data += issues

    summary_df = pl.DataFrame(summary_data, schema=["Check", "Count"], orient="row")
    return summary_df