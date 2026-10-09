"""Config loader for Maven Market project."""

import pathlib
import yaml


def load_config(config_path=None):
    """Load config.yml and resolve ${catalog} placeholders in table names."""
    if config_path is None:
        config_path = pathlib.Path(__file__).parent.parent / "config.yml"

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # Resolve ${catalog} variables in table names
    catalog = config.get("catalog", "")
    tables = config.get("tables", {})
    config["tables"] = {
        key: value.replace("${catalog}", catalog) if isinstance(value, str) else value
        for key, value in tables.items()
    }

    return config
