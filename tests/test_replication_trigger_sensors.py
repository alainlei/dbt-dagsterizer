"""Tests for replication trigger sensors.

Covers spec building (``build_auto_replication_trigger_specs``) and sensor
evaluation (``build_replication_trigger_sensors``), in particular the
full-table trigger for unpartitioned replication entries without a
``partition_column``.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import dagster as dg
import yaml

_MANIFEST = {
    "nodes": {
        "model.demo.orders": {
            "resource_type": "model",
            "unique_id": "model.demo.orders",
            "name": "orders",
            "database": "warehouse",
            "schema": "dwd",
            "identifier": "orders",
        },
    },
    "sources": {},
}

_AUTO_CONFIG = "dbt_dagsterizer.sensors.replication.auto_config"


def _write_orchestration(tmp_path: Path, data: dict) -> None:
    (tmp_path / "dagsterization.yml").write_text(yaml.dump(data), encoding="utf-8")


def _build_specs(tmp_path: Path) -> list[dict]:
    from dbt_dagsterizer.sensors.replication.auto_config import (
        build_auto_replication_trigger_specs,
    )

    with patch(f"{_AUTO_CONFIG}.get_dbt_project_dir", return_value=tmp_path), patch(
        f"{_AUTO_CONFIG}.load_manifest", return_value=_MANIFEST
    ):
        return build_auto_replication_trigger_specs()


# --- spec building ---


def test_trigger_spec_created_for_unpartitioned_entry_without_partition_column(tmp_path: Path):
    """Unpartitioned entries without partition_column must get a trigger sensor."""
    _write_orchestration(
        tmp_path,
        {
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
        },
    )

    specs = _build_specs(tmp_path)
    assert len(specs) == 1
    spec = specs[0]
    assert spec["name"] == "replicate_orders_trigger"
    assert spec["job_name"] == "replicate_orders_job"
    assert spec["upstream_model_name"] == "orders"
    assert spec["partition_type"] == "unpartitioned"


def test_trigger_spec_created_for_partitioned_entry(tmp_path: Path):
    """Partitioned entries keep getting a per-partition trigger sensor."""
    _write_orchestration(
        tmp_path,
        {
            "version": 1,
            "partitions": {"daily": ["orders"]},
            "replication": {
                "enabled": True,
                "entries": [
                    {
                        "model": "orders",
                        "enabled": True,
                        "partition_column": "order_date",
                    }
                ],
            },
        },
    )

    specs = _build_specs(tmp_path)
    assert len(specs) == 1
    assert specs[0]["partition_type"] == "daily"


def test_trigger_spec_skips_disabled_entries(tmp_path: Path):
    _write_orchestration(
        tmp_path,
        {
            "version": 1,
            "replication": {
                "enabled": True,
                "entries": [{"model": "orders", "enabled": False}],
            },
        },
    )

    specs = _build_specs(tmp_path)
    assert specs == []


# --- sensor evaluation ---


def _make_job(name: str, partitions_def: dg.PartitionsDefinition | None = None):
    @dg.op
    def noop():
        return None

    @dg.graph
    def g():
        noop()

    return g.to_job(name=name, partitions_def=partitions_def)


def _make_sensor(spec: dict, job) -> dg.SensorDefinition:
    from dbt_dagsterizer.sensors.replication.trigger import (
        build_replication_trigger_sensors,
    )

    sensors = build_replication_trigger_sensors(
        specs=[spec], jobs_by_name={spec["job_name"]: job}
    )
    assert len(sensors) == 1
    return sensors[0]


_UPSTREAM_RELATION = ["warehouse", "dwd", "orders"]


def test_unpartitioned_trigger_emits_full_table_run_request():
    """Unpartitioned upstream materialization → one RunRequest without partition_key."""
    job = _make_job("replicate_orders_job")
    sensor = _make_sensor(
        {
            "name": "replicate_orders_trigger",
            "job_name": "replicate_orders_job",
            "upstream_model_name": "orders",
            "upstream_model_relation": _UPSTREAM_RELATION,
            "partition_type": "unpartitioned",
            "enabled": True,
            "minimum_interval_seconds": 30,
        },
        job,
    )
    defs = dg.Definitions(jobs=[job], sensors=[sensor])
    upstream_key = dg.AssetKey(_UPSTREAM_RELATION)

    with dg.DagsterInstance.ephemeral() as instance:
        # Seed a historical materialization so the cursor bootstrap has an event.
        instance.report_runless_asset_event(dg.AssetMaterialization(asset_key=upstream_key))

        # Tick 1: seeds the cursor, no run requests for historical events.
        context1 = dg.build_sensor_context(
            instance=instance,
            definitions=defs,
            sensor_name="replicate_orders_trigger",
        )
        data1 = sensor.evaluate_tick(context1)
        assert data1.run_requests == []
        assert data1.cursor

        # New upstream materialization (unpartitioned) after the cursor.
        instance.report_runless_asset_event(dg.AssetMaterialization(asset_key=upstream_key))

        # Tick 2: emits exactly one full-table run request.
        context2 = dg.build_sensor_context(
            instance=instance,
            cursor=data1.cursor,
            definitions=defs,
            sensor_name="replicate_orders_trigger",
        )
        data2 = sensor.evaluate_tick(context2)
        assert len(data2.run_requests) == 1
        request = data2.run_requests[0]
        assert request.partition_key is None
        assert request.tags["luban/replication_trigger"] == "replicate_orders_trigger"
        assert request.tags["luban/upstream_dbt_model"] == "orders"

        # Tick 3: no new events → no run requests.
        context3 = dg.build_sensor_context(
            instance=instance,
            cursor=data2.cursor,
            definitions=defs,
            sensor_name="replicate_orders_trigger",
        )
        data3 = sensor.evaluate_tick(context3)
        assert data3.run_requests == []


def test_unpartitioned_trigger_dedupes_multiple_events_into_one_request():
    """Multiple upstream materializations in one tick → a single run request."""
    job = _make_job("replicate_orders_job")
    sensor = _make_sensor(
        {
            "name": "replicate_orders_trigger",
            "job_name": "replicate_orders_job",
            "upstream_model_name": "orders",
            "upstream_model_relation": _UPSTREAM_RELATION,
            "partition_type": "unpartitioned",
            "enabled": True,
            "minimum_interval_seconds": 30,
        },
        job,
    )
    defs = dg.Definitions(jobs=[job], sensors=[sensor])
    upstream_key = dg.AssetKey(_UPSTREAM_RELATION)

    with dg.DagsterInstance.ephemeral() as instance:
        instance.report_runless_asset_event(dg.AssetMaterialization(asset_key=upstream_key))

        context1 = dg.build_sensor_context(
            instance=instance,
            definitions=defs,
            sensor_name="replicate_orders_trigger",
        )
        data1 = sensor.evaluate_tick(context1)

        instance.report_runless_asset_event(dg.AssetMaterialization(asset_key=upstream_key))
        instance.report_runless_asset_event(dg.AssetMaterialization(asset_key=upstream_key))

        context2 = dg.build_sensor_context(
            instance=instance,
            cursor=data1.cursor,
            definitions=defs,
            sensor_name="replicate_orders_trigger",
        )
        data2 = sensor.evaluate_tick(context2)
        assert len(data2.run_requests) == 1
        assert data2.run_requests[0].partition_key is None


def test_partitioned_trigger_still_emits_per_partition_requests():
    """Regression: partitioned entries keep the per-partition trigger behavior."""
    job = _make_job(
        "replicate_orders_job",
        partitions_def=dg.DailyPartitionsDefinition(start_date="2026-01-01"),
    )
    sensor = _make_sensor(
        {
            "name": "replicate_orders_trigger",
            "job_name": "replicate_orders_job",
            "upstream_model_name": "orders",
            "upstream_model_relation": _UPSTREAM_RELATION,
            "partition_type": "daily",
            "enabled": True,
            "minimum_interval_seconds": 30,
        },
        job,
    )
    defs = dg.Definitions(jobs=[job], sensors=[sensor])
    upstream_key = dg.AssetKey(_UPSTREAM_RELATION)

    with dg.DagsterInstance.ephemeral() as instance:
        instance.report_runless_asset_event(
            dg.AssetMaterialization(asset_key=upstream_key, partition="2026-01-01")
        )

        context1 = dg.build_sensor_context(
            instance=instance,
            definitions=defs,
            sensor_name="replicate_orders_trigger",
        )
        data1 = sensor.evaluate_tick(context1)
        assert data1.run_requests == []

        instance.report_runless_asset_event(
            dg.AssetMaterialization(asset_key=upstream_key, partition="2026-01-02")
        )
        instance.report_runless_asset_event(
            dg.AssetMaterialization(asset_key=upstream_key, partition="2026-01-03")
        )

        context2 = dg.build_sensor_context(
            instance=instance,
            cursor=data1.cursor,
            definitions=defs,
            sensor_name="replicate_orders_trigger",
        )
        data2 = sensor.evaluate_tick(context2)
        assert sorted(r.partition_key for r in data2.run_requests) == [
            "2026-01-02",
            "2026-01-03",
        ]


def test_partitioned_trigger_fires_for_first_ever_materialization():
    """Regression: the first-ever partition materialization must trigger replication.

    When the sensor starts before the upstream asset has any materializations,
    the bootstrap tick must seed the cursor to "0" (not leave it empty) so the
    very first partition materialized afterwards (e.g. via observable-source
    detection) is processed instead of being swallowed as the bootstrap seed.
    """
    job = _make_job(
        "replicate_orders_job",
        partitions_def=dg.DailyPartitionsDefinition(start_date="2026-01-01"),
    )
    sensor = _make_sensor(
        {
            "name": "replicate_orders_trigger",
            "job_name": "replicate_orders_job",
            "upstream_model_name": "orders",
            "upstream_model_relation": _UPSTREAM_RELATION,
            "partition_type": "daily",
            "enabled": True,
            "minimum_interval_seconds": 30,
        },
        job,
    )
    defs = dg.Definitions(jobs=[job], sensors=[sensor])
    upstream_key = dg.AssetKey(_UPSTREAM_RELATION)

    with dg.DagsterInstance.ephemeral() as instance:
        # Tick 1: no materializations exist yet → cursor must be seeded to "0".
        context1 = dg.build_sensor_context(
            instance=instance,
            definitions=defs,
            sensor_name="replicate_orders_trigger",
        )
        data1 = sensor.evaluate_tick(context1)
        assert data1.run_requests == []
        assert data1.cursor == "0"

        # First-ever materialization of the upstream partition.
        instance.report_runless_asset_event(
            dg.AssetMaterialization(asset_key=upstream_key, partition="2026-01-05")
        )

        # Tick 2: must emit a run request for that first partition.
        context2 = dg.build_sensor_context(
            instance=instance,
            cursor=data1.cursor,
            definitions=defs,
            sensor_name="replicate_orders_trigger",
        )
        data2 = sensor.evaluate_tick(context2)
        assert [r.partition_key for r in data2.run_requests] == ["2026-01-05"]


def test_unpartitioned_trigger_fires_for_first_ever_materialization():
    """Regression: first-ever unpartitioned materialization must trigger replication."""
    job = _make_job("replicate_orders_job")
    sensor = _make_sensor(
        {
            "name": "replicate_orders_trigger",
            "job_name": "replicate_orders_job",
            "upstream_model_name": "orders",
            "upstream_model_relation": _UPSTREAM_RELATION,
            "partition_type": "unpartitioned",
            "enabled": True,
            "minimum_interval_seconds": 30,
        },
        job,
    )
    defs = dg.Definitions(jobs=[job], sensors=[sensor])
    upstream_key = dg.AssetKey(_UPSTREAM_RELATION)

    with dg.DagsterInstance.ephemeral() as instance:
        context1 = dg.build_sensor_context(
            instance=instance,
            definitions=defs,
            sensor_name="replicate_orders_trigger",
        )
        data1 = sensor.evaluate_tick(context1)
        assert data1.run_requests == []
        assert data1.cursor == "0"

        instance.report_runless_asset_event(dg.AssetMaterialization(asset_key=upstream_key))

        context2 = dg.build_sensor_context(
            instance=instance,
            cursor=data1.cursor,
            definitions=defs,
            sensor_name="replicate_orders_trigger",
        )
        data2 = sensor.evaluate_tick(context2)
        assert len(data2.run_requests) == 1
        assert data2.run_requests[0].partition_key is None
