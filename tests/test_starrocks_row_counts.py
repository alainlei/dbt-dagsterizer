"""Tests for StarRocks row count querying."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from dbt_dagsterizer.dbt.row_counts import get_row_counts_from_starrocks
from dbt_dagsterizer.resources.starrocks import StarRocksClient


def test_get_row_counts_from_starrocks(tmp_path: Path):
    """Test querying StarRocks for row counts."""
    # Create mock manifest
    manifest_data = {
        "nodes": {
            "model.demo.orders": {
                "resource_type": "model",
                "name": "orders",
                "database": "production_db",
                "schema": "dws",
                "identifier": "orders",
            },
            "model.demo.customers": {
                "resource_type": "model",
                "name": "customers",
                "database": "production_db",
                "schema": "dws",
                "identifier": "customers",
            },
            "model.demo.seeds": {
                "resource_type": "seed",  # Should be skipped
                "name": "seeds",
                "database": "production_db",
                "schema": "raw",
                "identifier": "seeds",
            },
        }
    }
    
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest_data))
    
    # Mock StarRocks client
    mock_client = Mock(spec=StarRocksClient)
    
    def mock_query(sql):
        if "orders" in sql:
            return 1000
        elif "customers" in sql:
            return 500
        return None
    
    mock_client.query_scalar = Mock(side_effect=mock_query)
    
    # Get row counts
    row_counts = get_row_counts_from_starrocks(mock_client, manifest_path)
    
    # Verify results
    assert len(row_counts) == 2  # Only models, not seeds
    assert row_counts["model.demo.orders"] == 1000
    assert row_counts["model.demo.customers"] == 500
    
    # Verify correct SQL queries were made
    assert mock_client.query_scalar.call_count == 2
    mock_client.query_scalar.assert_any_call(
        "SELECT COUNT(*) FROM `production_db`.`dws`.`orders`"
    )
    mock_client.query_scalar.assert_any_call(
        "SELECT COUNT(*) FROM `production_db`.`dws`.`customers`"
    )


def test_get_row_counts_from_starrocks_missing_manifest():
    """Test handling of missing manifest file."""
    mock_client = Mock(spec=StarRocksClient)
    row_counts = get_row_counts_from_starrocks(mock_client, Path("/nonexistent/manifest.json"))
    assert row_counts == {}


def test_get_row_counts_from_starrocks_query_failure(tmp_path: Path):
    """Test handling of query failures."""
    manifest_data = {
        "nodes": {
            "model.demo.failing": {
                "resource_type": "model",
                "name": "failing",
                "database": "db",
                "schema": "schema",
                "identifier": "failing",
            },
            "model.demo.success": {
                "resource_type": "model",
                "name": "success",
                "database": "db",
                "schema": "schema",
                "identifier": "success",
            },
        }
    }
    
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest_data))
    
    mock_client = Mock(spec=StarRocksClient)
    
    def mock_query(sql):
        if "failing" in sql:
            raise Exception("Table doesn't exist")
        return 100
    
    mock_client.query_scalar = Mock(side_effect=mock_query)
    
    # Should skip failing table and return successful one
    row_counts = get_row_counts_from_starrocks(mock_client, manifest_path)
    
    assert len(row_counts) == 1
    assert row_counts["model.demo.success"] == 100


def test_get_row_counts_from_starrocks_missing_relation_info(tmp_path: Path):
    """Test handling of nodes with missing database/schema/identifier."""
    manifest_data = {
        "nodes": {
            "model.demo.incomplete": {
                "resource_type": "model",
                "name": "incomplete",
                # Missing database, schema, identifier
            },
            "model.demo.complete": {
                "resource_type": "model",
                "name": "complete",
                "database": "db",
                "schema": "schema",
                "identifier": "complete",
            },
        }
    }
    
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest_data))
    
    mock_client = Mock(spec=StarRocksClient)
    mock_client.query_scalar = Mock(return_value=50)
    
    row_counts = get_row_counts_from_starrocks(mock_client, manifest_path)
    
    # Should only query the complete node
    assert len(row_counts) == 1
    assert row_counts["model.demo.complete"] == 50
    assert mock_client.query_scalar.call_count == 1
