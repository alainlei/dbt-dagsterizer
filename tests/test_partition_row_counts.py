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
    
    # Write test files
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "manifest.json").write_text(json.dumps(manifest_data))
    
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
    with patch("dbt_dagsterizer.assets.dbt.assets.make_starrocks_resource", return_value=mock_starrocks):
        observations = list(_emit_partition_row_counts(
            context=context,
            dbt_project_dir=tmp_path,
            translator=translator,
        ))
    
    # Verify observations were emitted
    assert len(observations) == 2
    
    # Check first observation
    obs1 = observations[0]
    assert isinstance(obs1, dg.AssetObservation)
    assert obs1.asset_key.path == ["dbt", "db", "schema", "orders"]
    assert "dagster/partition_row_count" in obs1.metadata
    assert obs1.metadata["dagster/partition_row_count"].value == 1000
    assert "partition_key" in obs1.metadata
    assert obs1.metadata["partition_key"].value == "2026-01-01"
    
    # Check second observation
    obs2 = observations[1]
    assert isinstance(obs2, dg.AssetObservation)
    assert obs2.asset_key.path == ["dbt", "db", "schema", "customers"]
    assert obs2.metadata["dagster/partition_row_count"].value == 500


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
    observations = list(_emit_partition_row_counts(
        context=context,
        dbt_project_dir=tmp_path,
        translator=translator,
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
    
    materializations = list(_emit_partition_row_counts(
        context=context,
        dbt_project_dir=tmp_path,
        translator=translator,
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
    
    materializations = list(_emit_partition_row_counts(
        context=context,
        dbt_project_dir=tmp_path,
        translator=translator,
    ))
    
    assert len(materializations) == 1
    mat = materializations[0]
    assert mat.metadata["dagster/partition_row_count"].value == 1000
    # partition_key should not be in metadata for non-partitioned runs
    assert "partition_key" not in mat.metadata
