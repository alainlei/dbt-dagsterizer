from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

import dagster as dg


def test_partition_change_sensor_applies_impact_range(monkeypatch):
    from dbt_dagsterizer.sensors.partition_change.detector import factory
    from dbt_dagsterizer.sensors.partition_change.detector.sparse_lookback import (
        SparseLookbackImpactRange,
    )

    @dg.op
    def noop():
        return None

    @dg.graph
    def g():
        noop()

    job = g.to_job(
        name="dummy_job",
        partitions_def=dg.DailyPartitionsDefinition(start_date="2000-01-01"),
    )

    monkeypatch.setattr(factory, "get_dbt_jobs_by_name", lambda: {"dummy_job": job})
    monkeypatch.setattr(factory, "prepare_manifest_if_missing", lambda: None)
    monkeypatch.setattr(factory, "load_manifest", lambda: {})

    @dataclass(frozen=True)
    class _Sparse:
        detect_relation: str
        impact_range: SparseLookbackImpactRange | None

    monkeypatch.setattr(
        factory,
        "parse_sparse_lookback_meta",
        lambda meta, manifest, granularity="day": _Sparse(
            detect_relation="ods.orders",
            impact_range=SparseLookbackImpactRange(start_offset_days=-1, end_offset_days=1),
        ),
    )

    d = (datetime.now().date() - timedelta(days=3))
    w1 = datetime(d.year, d.month, d.day, 10, 0, 0)

    monkeypatch.setattr(factory, "detect_partition_max_watermarks", lambda **_: {d: w1})

    sensors = factory.build_dbt_partition_change_sensors(
        specs=[
            {
                "partition_type": "daily",
                "name": "orders_partition_change_sensor",
                "job_name": "dummy_job",
                "detector_model": "orders",
                "enabled": True,
                "lookback_days": 7,
                "offset_days": 1,
                "minimum_interval_seconds": 60,
                "meta": {
                    "detect_source": {"source": "ods", "table": "orders"},
                    "partition_date_expr": "order_datetime",
                    "updated_at_expr": "updated_at",
                    "impact": {
                        "type": "range",
                        "start_offset_days": -1,
                        "end_offset_days": 1,
                    },
                },
            }
        ]
    )

    defs = dg.Definitions(jobs=[job], sensors=sensors)
    context = dg.build_sensor_context(
        resources={"starrocks": object()},
        cursor="2000-01-01T00:00:00",
        definitions=defs,
        sensor_name="orders_partition_change_sensor",
    )
    data = sensors[0].evaluate_tick(context)

    partitions = sorted([rr.partition_key for rr in data.run_requests])
    assert partitions == sorted(
        [
            (d - timedelta(days=1)).isoformat(),
            d.isoformat(),
            (d + timedelta(days=1)).isoformat(),
        ]
    )
