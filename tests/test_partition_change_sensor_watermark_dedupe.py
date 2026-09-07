from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import dagster as dg


def test_partition_change_sensor_uses_watermark_cursor_and_dedupes(monkeypatch):
    from dbt_dagsterizer.sensors.partition_change.detector import factory

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

    monkeypatch.setattr(
        factory,
        "parse_sparse_lookback_meta",
        lambda meta, manifest, granularity="day": _Sparse(detect_relation="ods.orders"),
    )

    fixed_now = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)

    class _FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return fixed_now.replace(tzinfo=None)
            return fixed_now.astimezone(tz)

        @classmethod
        def fromisoformat(cls, date_string: str):
            return datetime.fromisoformat(date_string)

    monkeypatch.setattr(factory, "datetime", _FixedDatetime)

    d = (fixed_now - timedelta(days=1)).date()
    w1 = datetime(d.year, d.month, d.day, 10, 0, 0)
    w2 = datetime(d.year, d.month, d.day, 11, 0, 0)

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
                },
            }
        ]
    )

    defs = dg.Definitions(jobs=[job], sensors=sensors)

    context1 = dg.build_sensor_context(
        resources={"starrocks": object()},
        cursor="2000-01-01T00:00:00",
        definitions=defs,
        sensor_name="orders_partition_change_sensor",
    )
    data1 = sensors[0].evaluate_tick(context1)
    assert len(data1.run_requests) == 1
    assert data1.run_requests[0].partition_key == d.isoformat()
    assert w1.replace(microsecond=0).isoformat() in data1.run_requests[0].run_key

    cursor_payload = json.loads(data1.cursor)
    assert cursor_payload["type"] == "partition_watermark_v1"
    assert cursor_payload["partitions"][d.isoformat()] == w1.replace(microsecond=0).isoformat()

    context2 = dg.build_sensor_context(
        resources={"starrocks": object()},
        cursor=data1.cursor,
        definitions=defs,
        sensor_name="orders_partition_change_sensor",
    )
    data2 = sensors[0].evaluate_tick(context2)
    assert data2.run_requests == []

    monkeypatch.setattr(factory, "detect_partition_max_watermarks", lambda **_: {d: w2})

    context3 = dg.build_sensor_context(
        resources={"starrocks": object()},
        cursor=data1.cursor,
        definitions=defs,
        sensor_name="orders_partition_change_sensor",
    )
    data3 = sensors[0].evaluate_tick(context3)
    assert len(data3.run_requests) == 1
    assert w2.replace(microsecond=0).isoformat() in data3.run_requests[0].run_key
