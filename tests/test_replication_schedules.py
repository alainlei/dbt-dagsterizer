"""Tests for replication schedules."""
from pathlib import Path
from unittest.mock import patch

import yaml

from dbt_dagsterizer.schedules.replication.auto_config import build_auto_replication_schedule_specs

_MANIFEST = {
    "nodes": {
        "model.demo.orders": {
            "resource_type": "model",
            "unique_id": "model.demo.orders",
            "name": "orders",
            "database": "production_db",
            "schema": "dws",
            "identifier": "orders",
        },
    }
}


def test_schedules_empty_when_replication_disabled(tmp_path: Path):
    """Test that no schedule specs are generated when replication is disabled."""
    orch_path = tmp_path / "dagsterization.yml"
    orch_path.write_text(
        yaml.dump({"version": 1, "replication": {"enabled": False, "entries": []}})
    )

    with patch("dbt_dagsterizer.schedules.replication.auto_config.get_dbt_project_dir", return_value=tmp_path):
        specs = build_auto_replication_schedule_specs()
        assert specs == []


def test_schedules_empty_by_default(tmp_path: Path):
    """Test that schedules are disabled by default (replication uses asset deps)."""
    orch_path = tmp_path / "dagsterization.yml"
    orch_path.write_text(
        yaml.dump({
            "version": 1,
            "replication": {
                "enabled": True,
                "entries": [
                    {
                        "model": "orders",
                        "enabled": True,
                        "destination_table": "orders",
                        "destination_schema": "dbo",
                        "write_disposition": "replace",
                    }
                ],
            },
        })
    )

    with patch("dbt_dagsterizer.schedules.replication.auto_config.get_dbt_project_dir", return_value=tmp_path):
        specs = build_auto_replication_schedule_specs()
        # Schedules should be empty by default - replication uses asset dependencies
        assert specs == []


def test_schedules_created_when_explicitly_enabled(tmp_path: Path):
    """Test that schedule specs are generated only when explicitly enabled."""
    orch_path = tmp_path / "dagsterization.yml"
    orch_path.write_text(
        yaml.dump({
            "version": 1,
            "replication": {
                "enabled": True,
                "schedules": {
                    "enabled": True,  # Explicitly enable schedules
                },
                "entries": [
                    {
                        "model": "orders",
                        "enabled": True,
                        "destination_table": "orders",
                        "destination_schema": "dbo",
                        "write_disposition": "replace",
                    }
                ],
            },
        })
    )

    with patch("dbt_dagsterizer.schedules.replication.auto_config.get_dbt_project_dir", return_value=tmp_path):
        specs = build_auto_replication_schedule_specs()
        assert len(specs) == 1
        spec = specs[0]
        assert spec["name"] == "replicate_orders_schedule"
        assert spec["job_name"] == "replicate_orders_job"  # Jobs have _job suffix
        assert spec["cron_schedule"] == "30 0 * * *"
        assert spec["partition_type"] == "unpartitioned"
        assert spec["enabled"] is True


def test_schedules_skips_disabled_entries(tmp_path: Path):
    """Test that disabled replication entries don't get schedules."""
    orch_path = tmp_path / "dagsterization.yml"
    orch_path.write_text(
        yaml.dump({
            "version": 1,
            "replication": {
                "enabled": True,
                "entries": [
                    {
                        "model": "orders",
                        "enabled": False,
                        "destination_table": "orders",
                        "destination_schema": "dbo",
                        "write_disposition": "replace",
                    }
                ],
            },
        })
    )

    with patch("dbt_dagsterizer.schedules.replication.auto_config.get_dbt_project_dir", return_value=tmp_path):
        specs = build_auto_replication_schedule_specs()
        assert specs == []
