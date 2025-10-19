set -euo pipefail
source .venv/bin/activate
python -m src.ingest.fetch_neso
python -m src.transform.transform
echo "Ingest + transform complete"
