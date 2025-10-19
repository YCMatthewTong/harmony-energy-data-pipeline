import json
from pathlib import Path


def load_config(config_path: str | Path = "conf/config.json") -> dict:
    """Load project config from JSON file."""
    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path, "r") as f:
        return json.load(f)