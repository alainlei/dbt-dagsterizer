"""Tests for StarRocks row count querying."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock

import pytest

from dbt_dagsterizer.dbt.row_counts import get_row_counts_from_starrocks
from dbt_dagsterizer.env_utils import dotenv_overrides_for_dbt_project, temporary_env
from dbt_dagsterizer.resources.starrocks import StarRocksClient, make_starrocks_resource


@pytest.mark.parametrize(
    ("dotenv_text", "existing_timeout", "expected_timeout"),
    [
        ("", None, 10),
        ("STARROCKS_CONNECT_TIMEOUT=30\n", None, 30),
        ("STARROCKS_CONNECT_TIMEOUT=30\n", "60", 60),
    ],
)
def test_starrocks_connect_timeout(
    tmp_path: Path, monkeypatch, dotenv_text, existing_timeout, expected_timeout
):
    monkeypatch.delenv("STARROCKS_CONNECT_TIMEOUT", raising=False)
    if existing_timeout is not None:
        monkeypatch.setenv("STARROCKS_CONNECT_TIMEOUT", existing_timeout)
    (tmp_path / ".env").write_text(dotenv_text, encoding="utf-8")
    mock_connect = Mock()
    monkeypatch.setattr("pymysql.connect", mock_connect)

    overrides = dotenv_overrides_for_dbt_project(dbt_project_dir=tmp_path / "dbt_project")
    with temporary_env(overrides):
        client = make_starrocks_resource()

    assert client.connect_timeout == expected_timeout
    with client._connection() as connection:
        assert connection is mock_connect.return_value

    mock_connect.assert_called_once_with(
        host=client.host,
        port=client.port,
        user=client.user,
        password=client.password,
        connect_timeout=expected_timeout,
        read_timeout=expected_timeout,
        write_timeout=expected_timeout,
        autocommit=True,
    )
    mock_connect.return_value.close.assert_called_once_with()


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