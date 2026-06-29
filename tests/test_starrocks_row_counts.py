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
                "unique_id": "model.demo.orders",
                "name": "orders",
                "database": "production_db",
                "schema": "dws",
                "identifier": "orders",
            },
            "model.demo.customers": {
                "resource_type": "model",
                "unique_id": "model.demo.customers",
                "name": "customers",
                "database": "production_db",
                "schema": "dws",
                "identifier": "customers",
            },
            "model.demo.seeds": {
                "resource_type": "seed",  # Should be skipped
                "unique_id": "model.demo.seeds",
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
    
    # Get row counts for orders node
    orders_node = manifest_data["nodes"]["model.demo.orders"]
    customers_node = manifest_data["nodes"]["model.demo.customers"]
    
    orders_count = get_row_counts_from_starrocks(mock_client, orders_node)
    customers_count = get_row_counts_from_starrocks(mock_client, customers_node)
    
    # Verify results
    assert orders_count == 1000
    assert customers_count == 500
    
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
    # Pass an empty node dict - should return -1 due to missing relation info
    node = {}
    result = get_row_counts_from_starrocks(mock_client, node)
    assert result == -1


def test_get_row_counts_from_starrocks_query_failure(tmp_path: Path):
    """Test handling of query failures."""
    manifest_data = {
        "nodes": {
            "model.demo.failing": {
                "resource_type": "model",
                "unique_id": "model.demo.failing",
                "name": "failing",
                "database": "db",
                "schema": "schema",
                "identifier": "failing",
            },
            "model.demo.success": {
                "resource_type": "model",
                "unique_id": "model.demo.success",
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
    
    # Test failing node
    failing_node = manifest_data["nodes"]["model.demo.failing"]
    failing_count = get_row_counts_from_starrocks(mock_client, failing_node)
    assert failing_count == -1
    
    # Test success node
    success_node = manifest_data["nodes"]["model.demo.success"]
    success_count = get_row_counts_from_starrocks(mock_client, success_node)
    assert success_count == 100


def test_get_row_counts_from_starrocks_missing_relation_info(tmp_path: Path):
    """Test handling of nodes with missing database/schema/identifier."""
    manifest_data = {
        "nodes": {
            "model.demo.incomplete": {
                "resource_type": "model",
                "unique_id": "model.demo.incomplete",
                "name": "incomplete",
                # Missing database, schema, identifier
            },
            "model.demo.complete": {
                "resource_type": "model",
                "unique_id": "model.demo.complete",
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
    
    # Test incomplete node
    incomplete_node = manifest_data["nodes"]["model.demo.incomplete"]
    incomplete_count = get_row_counts_from_starrocks(mock_client, incomplete_node)
    assert incomplete_count == -1  # Should return -1 due to missing relation
    
    # Test complete node
    complete_node = manifest_data["nodes"]["model.demo.complete"]
    complete_count = get_row_counts_from_starrocks(mock_client, complete_node)
    assert complete_count == 50
    assert mock_client.query_scalar.call_count == 1
