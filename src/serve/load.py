from math import floor

from sqlalchemy.orm import Session
from sqlalchemy.dialects.sqlite import insert
import polars as pl

from src.db.models import Generation

from src.utils.logger import logger

log = logger.bind(step="Load DB")


def upsert_generation_data(df: pl.DataFrame, session: Session):
    """
    Efficiently upserts data from a Polars DataFrame into SQLite DB in safe batches.
    - Handles SQLite's 999-variable limit automatically.
    - Vectorized via Polars (no full to_dicts() conversion).
    """
    if df.is_empty():
        log.info("No data to upsert.")
        return

    num_cols = len(df.columns)
    batch_size = floor(999 / num_cols)
    total = df.height
    log.info(f"Upserting {total:,} records in batches of {batch_size}...")

    # Prepare SQLAlchemy update data
    stmt_template = insert(Generation)
    update_data = {
        c.name: getattr(stmt_template.excluded, c.name)
        for c in Generation.__table__.columns
        if c.name != "_id"
    }

    # Process in chunks without converting all to dicts at once
    for i in range(0, total, batch_size):
        chunk_df = df.slice(i, batch_size)
        chunk_records = chunk_df.to_dicts()  # only convert small chunk

        stmt = stmt_template.values(chunk_records)
        stmt = stmt.on_conflict_do_update(
            index_elements=[Generation._id],
            set_=update_data
        )

        session.execute(stmt)
        session.commit()

    log.success(f"Upserted {total:,} records.")

