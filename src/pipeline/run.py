from src.ingest.fetch_neso import fetch_neso_data

from src.transform.transform import transform_records

from src.serve.load import upsert_generation_data
from src.serve.run_history import pipeline_run_tracker

from src.db.client import DatabaseClient
from src.db.models import Generation

from src.utils.logger import logger

log = logger.bind(step="run-pipeline")

@pipeline_run_tracker
def run_pipeline(db_client: DatabaseClient):
    
    with db_client.get_session() as session:
        # Determine last fetched ID
        last_row = session.query(Generation._id).order_by(Generation._id.desc()).first()
        last_id = last_row[0] if last_row else 0
        log.info(f"Starting fetch from NESO API, last_id={last_id}")

        # Fetch data
        records = fetch_neso_data(last_id=last_id)
        total_fetched = len(records)
        valid_records = 0
        
        # If no new records, exit
        if total_fetched == 0:
            log.info("No new records fetched.")
            return {"total_fetched": total_fetched, "valid_records": valid_records, "last_fetched_id": last_id}

        # Transform and validate
        df = transform_records(records)
        valid_records = df.height

        # Load to DB
        upsert_generation_data(df, session)

        log.info(f"Upserted {valid_records} valid records (from {total_fetched} fetched records)")
        # session.close()
    return {
        "total_fetched": total_fetched,
        "valid_records": valid_records,
        "last_fetched_id": df["_id"].max() if valid_records > 0 else last_id
    }


if __name__ == "__main__":
    run_pipeline()