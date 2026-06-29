"""Tests for partition row count metadata in dbt assets."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import dagster as dg
import pytest

from dbt_dagsterizer.assets.dbt.assets import _emit_partition_row_counts
from dbt_dagsterizer.assets.dbt.translator import LubanDagsterDbtTranslator
from dbt_dagsterizer.resources.starrocks import StarRocksClient


class FakeContext:
    """Fake Dagster context for testing."""
    
    def __init__(self, partition_key=None):
        self._partition_key = partition_key
        self.log = MagicMock()
    
    @property
    def partition_key(self):
        if self._partition_key is None:
            from dagster import DagsterInvariantViolationError
            raise DagsterInvariantViolationError("Not a partitioned run")
        return self._partition_key


def test_emit_partition_row_counts_emits_observations(tmp_path: Path):
    """Test that row count observations are emitted for assets via StarRocks query."""
    # Create mock manifest.json
    manifest_data = {
        "nodes": {
            "model.demo.orders": {
                "resource_type": "model",
                "name": "orders",
                "database": "db",
                "schema": "schema",
                "identifier": "orders",
            },
            "model.demo.customers": {
                "resource_type": "model",
                "name": "customers",
                "database": "db",
                "schema": "schema",
                "identifier": "customers",
            },
        }
    }
    
    # Create mock run_results with rows_affected
    run_results_data = {
        "metadata": {},
        "results": [
            {
                "unique_id": "model.demo.orders",
                "status": "success",
                "execution_time": 1.23,
                "timing": [],
                "adapter_response": {"rows_affected": 100},
            },
            {
                "unique_id": "model.demo.customers",
                "status": "success",
                "execution_time": 0.5,
                "timing": [],
                "adapter_response": {"rows_affected": 50},
            },
        ]
    }
    
    # Write test files
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "manifest.json").write_text(json.dumps(manifest_data))
    (target_dir / "run_results.json").write_text(json.dumps(run_results_data))
    
    # Create mock translator
    translator = LubanDagsterDbtTranslator(
        daily_partitions_def=None,
        dynamic_partitions_defs={},
        automation_observable_tables=set(),
        partitions_by_model={},
    )
    
    # Create fake context with partition key
    context = FakeContext(partition_key="2026-01-01")
    
    # Mock StarRocks client to return row counts
    mock_starrocks = Mock(spec=StarRocksClient)
    mock_starrocks.query_scalar = Mock(side_effect=lambda sql: 1000 if "orders" in sql else 500)
    
    # Emit observations with mocked StarRocks
    run_results_json = run_results_data
    with patch("dbt_dagsterizer.assets.dbt.assets.make_starrocks_resource", return_value=mock_starrocks):
        observations = list(_emit_partition_row_counts(
            context=context,
            dbt_project_dir=tmp_path,
            translator=translator,
            run_results_json=run_results_json,
        ))
    
    # Verify observations were emitted (2 for last_run_affected_row_count + 2 for dagster/row_count)
    assert len(observations) == 4
    
    # Check observations for orders
    orders_obs = [obs for obs in observations if "orders" in obs.asset_key.path]
    assert len(orders_obs) == 2
    assert any("last_run_affected_row_count" in obs.metadata for obs in orders_obs)
    assert any(obs.metadata.get("dagster/row_count").value == 1000 if "dagster/row_count" in obs.metadata else False for obs in orders_obs)
    
    # Check observations for customers
    customers_obs = [obs for obs in observations if "customers" in obs.asset_key.path]
    assert len(customers_obs) == 2
    assert any("last_run_affected_row_count" in obs.metadata for obs in customers_obs)
    assert any(obs.metadata.get("dagster/row_count").value == 500 if "dagster/row_count" in obs.metadata else False for obs in customers_obs)


def test_emit_partition_row_counts_skips_missing_files(tmp_path: Path):
    """Test that function gracefully handles missing files."""
    translator = LubanDagsterDbtTranslator(
        daily_partitions_def=None,
        dynamic_partitions_defs={},
        automation_observable_tables=set(),
        partitions_by_model={},
    )
    
    context = FakeContext(partition_key="2026-01-01")
    
    # No target directory exists
    run_results_json = {"metadata": {}, "results": []}
    observations = list(_emit_partition_row_counts(
        context=context,
        dbt_project_dir=tmp_path,
        translator=translator,
        run_results_json=run_results_json,
    ))
    
    assert len(observations) == 0


def test_emit_partition_row_counts_handles_no_rows_affected(tmp_path: Path):
    """Test that assets without rows_affected are skipped."""
    run_results_data = {
        "results": [
            {
                "unique_id": "model.demo.orders",
                "status": "success",
                "execution_time": 1.23,
                "timing": [],
                "adapter_response": {},  # No rows_affected
            },
        ]
    }
    
    manifest_data = {
        "nodes": {
            "model.demo.orders": {
                "resource_type": "model",
                "name": "orders",
                "database": "db",
                "schema": "schema",
                "identifier": "orders",
            },
        }
    }
    
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "run_results.json").write_text(json.dumps(run_results_data))
    (target_dir / "manifest.json").write_text(json.dumps(manifest_data))
    
    translator = LubanDagsterDbtTranslator(
        daily_partitions_def=None,
        dynamic_partitions_defs={},
        automation_observable_tables=set(),
        partitions_by_model={},
    )
    
    context = FakeContext(partition_key="2026-01-01")
    
    run_results_json = run_results_data
    materializations = list(_emit_partition_row_counts(
        context=context,
        dbt_project_dir=tmp_path,
        translator=translator,
        run_results_json=run_results_json,
    ))
    
    # No observations should be emitted since rows_affected is missing
    assert len(materializations) == 0


def test_emit_partition_row_counts_non_partitioned_run(tmp_path: Path):
    """Test that materializations are emitted even for non-partitioned runs."""
    run_results_data = {
        "results": [
            {
                "unique_id": "model.demo.orders",
                "status": "success",
                "execution_time": 1.23,
                "timing": [],
                "adapter_response": {"rows_affected": 1000},
            },
        ]
    }
    
    manifest_data = {
        "nodes": {
            "model.demo.orders": {
                "resource_type": "model",
                "name": "orders",
                "database": "db",
                "schema": "schema",
                "identifier": "orders",
            },
        }
    }
    
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "run_results.json").write_text(json.dumps(run_results_data))
    (target_dir / "manifest.json").write_text(json.dumps(manifest_data))
    
    translator = LubanDagsterDbtTranslator(
        daily_partitions_def=None,
        dynamic_partitions_defs={},
        automation_observable_tables=set(),
        partitions_by_model={},
    )
    
    # Non-partitioned context
    context = FakeContext(partition_key=None)
    
    run_results_json = run_results_data
    materializations = list(_emit_partition_row_counts(
        context=context,
        dbt_project_dir=tmp_path,
        translator=translator,
        run_results_json=run_results_json,
    ))
    
    assert len(materializations) == 1
    mat = materializations[0]
    # The function emits 'last_run_affected_row_count' from run_results
    assert mat.metadata["last_run_affected_row_count"].value == 1000
    # partition should be None for non-partitioned runs
    assert mat.partition is None
