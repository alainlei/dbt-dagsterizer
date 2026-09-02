from __future__ import annotations

from dataclasses import dataclass

import dagster as dg


def test_partition_change_sensor_skips_on_missing_database(monkeypatch):
    from dbt_dagsterizer.sensors.partition_change.detector import factory

    @dg.op
    def noop():
        return None

    @dg.graph
    def g():
        noop()

    job = g.to_job(name="dummy_job")

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

    def _raise(*_args, **_kwargs):
        raise Exception("Unknown database 'ods'")

    monkeypatch.setattr(factory, "detect_partition_max_watermarks", _raise)

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

    context = dg.build_sensor_context(resources={"starrocks": object()})
    data = sensors[0].evaluate_tick(context)

    assert data.run_requests == []
    assert data.skip_message is not None
    assert "Unknown database" in data.skip_message
