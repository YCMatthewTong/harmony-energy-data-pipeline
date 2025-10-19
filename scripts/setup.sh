set -euo pipefail
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
echo "Virtualenv created and deps installed."
