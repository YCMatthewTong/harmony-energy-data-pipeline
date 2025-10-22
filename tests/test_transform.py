import pytest
import polars as pl
from polars.testing import assert_frame_equal
from datetime import datetime

from src.transform.transform import (
    _align_schema,
    _parse_and_cast,
    _validate_perc_consistency,
    _validate_missing_values,
    _deduplicate,
    _generate_quality_summary,
)


@pytest.fixture
def sample_df() -> pl.DataFrame:
    """A sample DataFrame fixture for testing."""
    data = {
        "_id": [1, 2, 3],
        "DATETIME": ["2023-01-01T10:00:00", "2023-01-01T10:30:00", "2023-01-01T11:00:00"],
        "GENERATION": [100.0, 200.0, 150.0],
        "WIND": [10.0, 40.0, 30.0],
        "WIND_perc": [10.0, 20.0, 20.0],
        "SOLAR": [5.0, 10.0, 7.5],
        "SOLAR_perc": [5.0, 5.0, 5.0],
    }
    return pl.DataFrame(data)


class TestAlignSchema:
    def test_no_change(self, sample_df):
        expected_cols = sample_df.columns
        result_df = _align_schema(sample_df.clone(), expected_cols)
        assert_frame_equal(sample_df, result_df)

    def test_add_missing_columns(self, sample_df):
        expected_cols = sample_df.columns + ["NEW_COL1", "NEW_COL2"]
        result_df = _align_schema(sample_df.clone(), expected_cols)
        
        expected_df = sample_df.with_columns(
            pl.lit(None, dtype=pl.Null).alias("NEW_COL1"),
            pl.lit(None, dtype=pl.Null).alias("NEW_COL2"),
        )
        assert_frame_equal(expected_df, result_df)

    def test_drop_extra_columns(self, sample_df):
        df_with_extra = sample_df.with_columns(pl.lit(1).alias("EXTRA_COL"))
        expected_cols = sample_df.columns
        result_df = _align_schema(df_with_extra, expected_cols)
        assert_frame_equal(sample_df, result_df)



class TestParseAndCast:
    def test_successful_parse_and_cast(self):
        df = pl.DataFrame({
            "DATETIME": ["2023-01-01T10:00:00", "2023-01-01T10:30:00"],
            "VALUE1": ["10.5", "20.0"],
            "VALUE2": [1, 2]
        })
        result_df = _parse_and_cast(df, dt_col="DATETIME", numeric_cols=["VALUE1", "VALUE2"])
        
        expected_df = pl.DataFrame({
            "DATETIME": [datetime(2023, 1, 1, 10, 0), datetime(2023, 1, 1, 10, 30)],
            "VALUE1": [10.5, 20.0],
            "VALUE2": [1.0, 2.0]
        })
        assert_frame_equal(result_df, expected_df)
        assert result_df["DATETIME"].dtype == pl.Datetime
        assert result_df["VALUE1"].dtype == pl.Float64
        assert result_df["VALUE2"].dtype == pl.Float64

    def test_malformed_datetime(self):
        df = pl.DataFrame({"DATETIME": ["not-a-date"]})
        result_df = _parse_and_cast(df, dt_col="DATETIME", numeric_cols=[])
        assert result_df["DATETIME"][0] is None

    def test_non_numeric_values(self):
        df = pl.DataFrame({"DATETIME": ["2023-01-01T10:30:00"], "VALUE": ["abc"]}, schema={"DATETIME": pl.String, "VALUE": pl.String})
        result_df = _parse_and_cast(df, dt_col="DATETIME", numeric_cols=["VALUE"])
        assert result_df["VALUE"][0] is None


class TestValidatePercConsistency:
    def test_consistent_percentages(self, sample_df):
        # Make WIND_perc consistent for this test
        result_df, invalid_count = _validate_perc_consistency(sample_df, tolerance=1.0)
        assert invalid_count == 0
        # No changes should be made to the perc columns
        assert_frame_equal(sample_df.select("WIND_perc", "SOLAR_perc"), result_df.select("WIND_perc", "SOLAR_perc"))

    def test_inconsistent_percentages(self):
        df = pl.DataFrame({
            "GENERATION": [100.0, 200.0],
            "WIND": [10.0, 50.0],
            "WIND_perc": [10.0, 99.0], # Second row is inconsistent (50/200*100 = 25%)
        })
        result_df, invalid_count = _validate_perc_consistency(df, tolerance=1.0)
        assert invalid_count == 1
        # The inconsistent value should be corrected
        assert result_df["WIND_perc"][1] == pytest.approx(25.0)
        # The consistent value should remain
        assert result_df["WIND_perc"][0] == pytest.approx(10.0)

    def test_missing_perc_column(self, sample_df):
        df = sample_df.drop("SOLAR_perc")
        # This should run without error
        result_df, invalid_count = _validate_perc_consistency(df, tolerance=1.0)
        assert invalid_count == 0
        # No changes should be made to the perc columns
        assert "SOLAR_perc" not in result_df.columns


class TestValidateMissingValues:
    def test_no_missing_values(self, sample_df):
        result_df, null_count = _validate_missing_values(sample_df.clone())
        assert null_count == 0
        assert_frame_equal(sample_df, result_df)

    def test_with_missing_values(self):
        schema = {
            "_id": pl.Int64,
            "DATETIME": pl.Datetime,
            "VALUE": pl.Float64
        }
        df = pl.DataFrame({
            "_id": [1, 2, 3, 4],
            "DATETIME": [datetime(2023,1,1), None, datetime(2023,1,3), datetime(2023,1,4)],
            "VALUE": [10.0, 20.0, None, 40.0]
        }, schema=schema)
        result_df, null_count = _validate_missing_values(df)
        
        assert null_count == 2 # 1 row with null datetime, 1 row with null value
        
        expected_df = pl.DataFrame({
            "_id": [1, 3, 4],
            "DATETIME": [datetime(2023,1,1), datetime(2023,1,3), datetime(2023,1,4)],
            "VALUE": [10.0, 0.0, 40.0]
        }, schema=schema)
        
        assert_frame_equal(result_df, expected_df)

    def test_all_null_row(self):
        df = pl.DataFrame({
            "_id": [1, None],
            "DATETIME": [datetime(2023,1,1), None],
            "VALUE": [10.0, None]
        })
        result_df, null_count = _validate_missing_values(df)
        assert null_count == 1
        assert result_df.height == 1
        assert result_df["_id"][0] == 1


class TestDeduplicate:
    def test_no_duplicates(self, sample_df):
        result_df, removed_count = _deduplicate(sample_df)
        assert removed_count == 0
        assert_frame_equal(sample_df.sort("_id").unique("DATETIME", keep="last", maintain_order=True), result_df, check_row_order=True)

    def test_duplicate_ids(self):
        df = pl.DataFrame({
            "_id": [1, 1, 2],
            "DATETIME": [datetime(2023,1,1,10), datetime(2023,1,1,11), datetime(2023,1,1,12)]
        })
        result_df, removed_count = _deduplicate(df)
        assert removed_count == 1
        assert result_df.height == 2
        # Keeps the 'last' DATETIME for the duplicate _id
        assert result_df.filter(pl.col("_id") == 1)["DATETIME"][0] == datetime(2023,1,1,11)

    def test_duplicate_datetimes(self):
        df = pl.DataFrame({
            "_id": [1, 2, 3],
            "DATETIME": [datetime(2023,1,1,10), datetime(2023,1,1,10), datetime(2023,1,1,11)]
        })
        result_df, removed_count = _deduplicate(df)
        assert removed_count == 1
        assert result_df.height == 2
        # Keeps the 'last' _id for the duplicate DATETIME
        assert result_df.filter(pl.col("DATETIME") == datetime(2023,1,1,10))["_id"][0] == 2

    def test_both_duplicates(self):
        df = pl.DataFrame({
            "_id":      [1, 1, 2, 3, 4],
            "DATETIME": [
                datetime(2023,1,1,10), # id=1, dt=10
                datetime(2023,1,1,11), # id=1, dt=11 (keep this one for id=1)
                datetime(2023,1,1,12), # id=2, dt=12
                datetime(2023,1,1,12), # id=3, dt=12 (keep this one for dt=12)
                datetime(2023,1,1,13)
            ]
        })
        # After id dedupe: ids [1,2,3,4] with dts [11,12,12,13]
        # After dt dedupe: ids [1,3,4] with dts [11,12,13]
        result_df, removed_count = _deduplicate(df)
        assert removed_count == 2
        assert result_df.height == 3
        expected_df = pl.DataFrame({
            "_id": [1, 3, 4],
            "DATETIME": [datetime(2023,1,1,11), datetime(2023,1,1,12), datetime(2023,1,1,13)]
        })
        assert_frame_equal(result_df.sort("_id"), expected_df)


class TestGenerateQualitySummary:
    def test_summary_creation(self):
        issues = [
            ("Inconsistent mix %", 5),
            ("Duplicate/overlapping rows", 2)
        ]
        result_df = _generate_quality_summary(total_raw=100, total_clean=93, issues=issues)
        
        expected_data = [
            ("Total raw records", 100),
            ("Valid cleaned records", 93),
            ("Dropped / invalid records", 7),
            ("Inconsistent mix %", 5),
            ("Duplicate/overlapping rows", 2),
        ]
        expected_df = pl.DataFrame(expected_data, schema=["Check", "Count"], orient="row")
        
        assert_frame_equal(result_df, expected_df)
