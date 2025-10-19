import requests
import backoff
from urllib.parse import urlencode
from typing import List, Dict, Optional

from src.utils.logger import logger

BASE_URL = "https://api.neso.energy/api/3/action/datastore_search_sql"
RESOURCE_ID = "f93d1835-75bc-43e5-84ad-12472b180a98"

log = logger.bind(step="Fetch NESO API")

def _log_backoff(details):
    log.warning(f"Request failed ({details['tries']} tries). Retrying in {details['wait']}s...")

# Generic HTTP Helper
@backoff.on_exception(
    backoff.expo,
    (requests.exceptions.RequestException, requests.exceptions.Timeout),
    max_tries=5,
    jitter=None,
    on_backoff=_log_backoff,
)
def _get_request(url: str, params: dict) -> dict:
    """
    Perform GET request with retry/backoff and JSON validation.
    """
    response = requests.get(url=url, params=urlencode(params), timeout=60)
    response.raise_for_status()
    data = response.json()

    if not data.get("success"):
        raise requests.exceptions.RequestException("NESO API returned 'success=False'")

    return data["result"]



# --------------------------------------------------------
# Incremental pagination fetch
# --------------------------------------------------------
def fetch_neso_data(
    last_id: int = 0,
    batch_size: int = 30_000,
    max_records: Optional[int] = None,
) -> List[Dict]:
    """
    Fetches NESO generation data incrementally using last_id pagination.

    Args:
        last_id: The last `_id` retrieved from the DB to continue from.
        batch_size: Number of rows per request (NESO API limit ≈ 30k).
        max_records: Optional limit to stop early (for testing).

    Returns:
        List of dict records.
    """

    log.info(f"Starting NESO data fetch from _id > {last_id}")

    all_records = []
    total_fetched = 0
    latest_id = last_id

    while True:
        sql = (
            f'SELECT * FROM "{RESOURCE_ID}" '
            f'WHERE "_id" > {latest_id} '
            f'ORDER BY "_id" ASC '
            f'LIMIT {batch_size}'
        )

        params = {"sql": sql}
        result = _get_request(url=BASE_URL, params=params)
        records = result.get("records", [])

        if not records:
            log.info("No more records to fetch. Exiting.")
            break

        all_records.extend(records)
        total_fetched += len(records)

        latest_id = records[-1]["_id"]  # update last fetched ID

        log.info(f"Fetched {len(records)} records (latest _id={latest_id}, total={total_fetched})")

        if max_records and total_fetched >= max_records:
            log.info(f"Reached max_records={max_records}, stopping.")
            break

        # Stop if last batch is smaller than full page (end of data)
        if len(records) < batch_size:
            break

    log.success(f"Completed fetching {len(all_records)} records (last_id={latest_id})")
    return all_records
