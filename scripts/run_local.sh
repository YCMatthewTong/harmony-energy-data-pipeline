set -euo pipefail
source .venv/bin/activate
export PYTHONPATH=$(pwd)
# start streamlit for local testing
streamlit run src/app/streamlit_app.py
