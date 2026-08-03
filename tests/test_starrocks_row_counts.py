"""Tests for StarRocks row count querying."""
from __future__ import annotations

from unittest.mock import Mock

from dbt_dagsterizer.dbt.row_counts import get_row_counts_from_starrocks
from dbt_dagsterizer.resources.starrocks import StarRocksClient


def test_get_row_counts_from_starrocks():
    """Test querying StarRocks for row counts."""
    node = {
        "unique_id": "model.demo.orders",
        "resource_type": "model",
        "name": "orders",
        "database": "production_db",
        "schema": "dws",
        "identifier": "orders",
    }

    mock_client = Mock(spec=StarRocksClient)
    mock_client.query_scalar = Mock(return_value=1000)

    row_count = get_row_counts_from_starrocks(mock_client, node)

    assert row_count == 1000
    mock_client.query_scalar.assert_called_once_with(
        "SELECT COUNT(*) FROM `production_db`.`dws`.`orders`"
    )


def test_get_row_counts_from_starrocks_missing_relation():
    """Test handling of node with missing database/schema/identifier."""
    node = {
        "unique_id": "model.demo.incomplete",
        "resource_type": "model",
        "name": "incomplete",
    }

    mock_client = Mock(spec=StarRocksClient)
    row_count = get_row_counts_from_starrocks(mock_client, node)
    assert row_count == -1


def test_get_row_counts_from_starrocks_query_failure():
    """Test handling of query failures."""
    node = {
        "unique_id": "model.demo.failing",
        "resource_type": "model",
        "name": "failing",
        "database": "db",
        "schema": "schema",
        "identifier": "failing",
    }

    mock_client = Mock(spec=StarRocksClient)
    mock_client.query_scalar = Mock(side_effect=Exception("Table doesn't exist"))

    row_count = get_row_counts_from_starrocks(mock_client, node)
    assert row_count == -1


def test_get_row_counts_from_starrocks_customers():
    """Test querying row counts for a different model."""
    node = {
        "unique_id": "model.demo.customers",
        "resource_type": "model",
        "name": "customers",
        "database": "production_db",
        "schema": "dws",
        "identifier": "customers",
    }

    mock_client = Mock(spec=StarRocksClient)
    mock_client.query_scalar = Mock(return_value=500)

    row_count = get_row_counts_from_starrocks(mock_client, node)

    assert row_count == 500
    mock_client.query_scalar.assert_called_once_with(
        "SELECT COUNT(*) FROM `production_db`.`dws`.`customers`"
    )