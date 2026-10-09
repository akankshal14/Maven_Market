import os
import sys
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.config_loader import load_config


def test_config_file_exists():
    """Verify config.yml exists in the project utils directory."""
    config_path = os.path.join(os.path.dirname(__file__), "../config.yml")
    assert os.path.exists(config_path), "utils/config.yml file is missing!"


def test_catalog_and_schemas_loaded():
    """Validate catalog and schemas structure."""
    config = load_config()
    assert config["catalog"] == "maven_market_uc"
    assert config["schemas"]["bronze"] == "bronze"
    assert config["schemas"]["silver"] == "silver"
    assert config["schemas"]["gold"] == "gold"
    assert config["schemas"]["audit"] == "audit"


def test_sources_configuration():
    """Validate ADLS, MongoDB, and Kafka configuration blocks."""
    config = load_config()
    sources = config.get("sources", {})
    
    assert "adls" in sources
    assert "mongodb" in sources
    assert "kafka" in sources
    
    assert sources["mongodb"]["database"] == "maven_market_db"
    assert len(sources["kafka"]["streams"]) >= 2


def test_table_placeholders_resolved():
    """Verify that ${catalog} variables are dynamically resolved."""
    config = load_config()
    tables = config.get("tables", {})
    
    assert "${catalog}" not in tables["bronze_customers"]
    assert tables["bronze_customers"] == "maven_market_uc.bronze.raw_customers"
    assert tables["gold_fact_sales"] == "maven_market_uc.gold.fact_sales"


def test_expectations_configuration():
    """Verify data quality expectation strings exist for DLT pipeline rules."""
    config = load_config()
    expectations = config.get("expectations", {})
    
    assert "transactions" in expectations
    assert "orders" in expectations
    assert expectations["transactions"]["valid_quantity"] == "quantity > 0"
    assert expectations["customers"]["valid_email"] == "email LIKE '%@%'"