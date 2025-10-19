import polars as pl
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import pytest

from datetime import datetime
from datetime import timezone

from src.db.models import Base, Generation
from src.serve.load import upsert_generation_data

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


# === Setup a test SQLite DB in memory ===
@pytest.fixture(scope="function")
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.drop_all(engine)
    # Create schema
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


# === Prepare test data ===
@pytest.fixture
def test_data():
    test_data = [
        {
            "_id": 1,
            "DATETIME": datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            "GAS": 1000.0,
            "COAL": 500.0,
            "NUCLEAR": 200.0,
            "GENERATION": 1700.0,
            "CARBON_INTENSITY": 120.0,
            "LOW_CARBON": 300.0,
            "ZERO_CARBON": 150.0,
            "RENEWABLE": 100.0,
            "FOSSIL": 1400.0,
            "GAS_perc": 58.8,
            "COAL_perc": 29.4,
            "NUCLEAR_perc": 11.8,
            "LOW_CARBON_perc": 17.6,
            "ZERO_CARBON_perc": 8.8,
            "RENEWABLE_perc": 5.9,
            "FOSSIL_perc": 82.3
        },
        {
            "_id": 2,
            "DATETIME": datetime(2025, 1, 1, 1, 0, 0, tzinfo=timezone.utc),
            "GAS": 1100.0,
            "COAL": 400.0,
            "NUCLEAR": 200.0,
            "GENERATION": 1700.0,
            "CARBON_INTENSITY": 118.0,
            "LOW_CARBON": 300.0,
            "ZERO_CARBON": 160.0,
            "RENEWABLE": 120.0,
            "FOSSIL": 1500.0,
            "GAS_perc": 64.7,
            "COAL_perc": 23.5,
            "NUCLEAR_perc": 11.8,
            "LOW_CARBON_perc": 17.6,
            "ZERO_CARBON_perc": 9.4,
            "RENEWABLE_perc": 7.1,
            "FOSSIL_perc": 88.2
        }
    ]

    return pl.DataFrame(test_data)



# === Test loading ===
def test_upsert_generation_data(session, test_data):
    """
    Tests that data is inserted correctly and upsert prevents duplicates.
    """
    # First load
    upsert_generation_data(test_data, session)

    count = session.query(Generation).count()
    assert count == 2, f"Expected 2 rows after first insert, got {count}"

    # Run again to trigger UPSERT logic
    upsert_generation_data(test_data, session)
    count_after_upsert = session.query(Generation).count()

    assert count_after_upsert == 2, "UPSERT failed — duplicates inserted."
    assert count_after_upsert == count, "Row count changed unexpectedly."

    # Check that data values match expected
    row = session.query(Generation).filter(Generation._id == 1).first()
    assert row.GAS == 1000.0, f"Unexpected GAS value: {row.GAS}"

    logger.debug("Upsert logic verified successfully.")