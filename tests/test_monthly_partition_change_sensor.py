"""Tests for month-granular partition-change detectors."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import dagster as dg
import pytest
import yaml


def _monthly_job():
    @dg.op
    def noop():
        return None

    @dg.graph
    def g():
        noop()

    # end_offset=1 keeps the in-progress month a valid partition key.
    return g.to_job(
        name="dummy_job",
        partitions_def=dg.MonthlyPartitionsDefinition(start_date="2000-01-01", end_offset=1),
    )


def _patch_common(monkeypatch, factory, job):
    monkeypatch.setattr(factory, "get_dbt_jobs_by_name", lambda: {"dummy_job": job})
    monkeypatch.setattr(factory, "prepare_manifest_if_missing", lambda: None)
    monkeypatch.setattr(factory, "load_manifest", lambda: {})


def _fixed_now(monkeypatch, factory, fixed_now: datetime):
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


_FIXED_NOW = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)


def _monthly_spec(**overrides):
    spec = {
        "partition_type": "monthly",
        "name": "orders_partition_change_sensor",
        "job_name": "dummy_job",
        "detector_model": "orders",
        "enabled": True,
        "lookback_months": 3,
        "offset_months": 1,
        "minimum_interval_seconds": 60,
        "meta": {
            "detect_source": {"source": "ods", "table": "orders"},
            "partition_date_expr": "order_datetime",
            "updated_at_expr": "updated_at",
        },
    }
    spec.update(overrides)
    return spec


def test_monthly_detector_emits_month_partition_keys(monkeypatch):
    """A monthly detector emits 'YYYY-MM-01' keys, not mid-month dates."""
    from dbt_dagsterizer.sensors.partition_change.detector import factory

    job = _monthly_job()
    _patch_common(monkeypatch, factory, job)
    _fixed_now(monkeypatch, factory, _FIXED_NOW)

    @dataclass(frozen=True)
    class _Sparse:
        detect_relation: str

    monkeypatch.setattr(
        factory,
        "parse_sparse_lookback_meta",
        lambda meta, manifest, granularity="day": _Sparse(detect_relation="ods.orders"),
    )

    # offset_months=1 -> window_end 2026-04-01
    changed_month = date(2026, 4, 1)
    w1 = datetime(2026, 4, 1, 10, 0, 0)
    monkeypatch.setattr(factory, "detect_partition_max_watermarks", lambda **_: {changed_month: w1})

    sensors = factory.build_dbt_partition_change_sensors(specs=[_monthly_spec()])
    defs = dg.Definitions(jobs=[job], sensors=sensors)

    context = dg.build_sensor_context(
        resources={"starrocks": object()},
        cursor="2000-01-01T00:00:00",
        definitions=defs,
        sensor_name="orders_partition_change_sensor",
    )
    data = sensors[0].evaluate_tick(context)

    assert len(data.run_requests) == 1
    assert data.run_requests[0].partition_key == "2026-04-01"
    valid_keys = set(job.partitions_def.get_partition_keys())
    assert data.run_requests[0].partition_key in valid_keys


def test_monthly_detector_dedupes_via_watermark_cursor(monkeypatch):
    """The watermark cursor suppresses a repeat tick and re-fires on a newer watermark."""
    from dbt_dagsterizer.sensors.partition_change.detector import factory

    job = _monthly_job()
    _patch_common(monkeypatch, factory, job)
    _fixed_now(monkeypatch, factory, _FIXED_NOW)

    @dataclass(frozen=True)
    class _Sparse:
        detect_relation: str

    monkeypatch.setattr(
        factory,
        "parse_sparse_lookback_meta",
        lambda meta, manifest, granularity="day": _Sparse(detect_relation="ods.orders"),
    )

    changed_month = date(2026, 4, 1)
    w1 = datetime(2026, 4, 1, 10, 0, 0)
    w2 = datetime(2026, 4, 1, 11, 0, 0)
    monkeypatch.setattr(factory, "detect_partition_max_watermarks", lambda **_: {changed_month: w1})

    sensors = factory.build_dbt_partition_change_sensors(specs=[_monthly_spec()])
    defs = dg.Definitions(jobs=[job], sensors=sensors)

    context1 = dg.build_sensor_context(
        resources={"starrocks": object()},
        cursor="2000-01-01T00:00:00",
        definitions=defs,
        sensor_name="orders_partition_change_sensor",
    )
    data1 = sensors[0].evaluate_tick(context1)
    assert len(data1.run_requests) == 1

    cursor_payload = json.loads(data1.cursor)
    assert cursor_payload["type"] == "partition_watermark_v1"
    assert cursor_payload["partitions"]["2026-04-01"] == w1.replace(microsecond=0).isoformat()

    context2 = dg.build_sensor_context(
        resources={"starrocks": object()},
        cursor=data1.cursor,
        definitions=defs,
        sensor_name="orders_partition_change_sensor",
    )
    assert sensors[0].evaluate_tick(context2).run_requests == []

    monkeypatch.setattr(factory, "detect_partition_max_watermarks", lambda **_: {changed_month: w2})

    context3 = dg.build_sensor_context(
        resources={"starrocks": object()},
        cursor=data1.cursor,
        definitions=defs,
        sensor_name="orders_partition_change_sensor",
    )
    data3 = sensors[0].evaluate_tick(context3)
    assert len(data3.run_requests) == 1
    assert w2.replace(microsecond=0).isoformat() in data3.run_requests[0].run_key


def test_monthly_detector_expands_impact_range_in_months(monkeypatch):
    """A month-based impact range expands to valid month keys, never mid-month dates."""
    from dbt_dagsterizer.sensors.partition_change.detector import factory
    from dbt_dagsterizer.sensors.partition_change.detector.sparse_lookback import (
        SparseLookbackImpactRange,
    )

    job = _monthly_job()
    _patch_common(monkeypatch, factory, job)
    _fixed_now(monkeypatch, factory, _FIXED_NOW)

    @dataclass(frozen=True)
    class _Sparse:
        detect_relation: str
        impact_range: SparseLookbackImpactRange | None

    monkeypatch.setattr(
        factory,
        "parse_sparse_lookback_meta",
        lambda meta, manifest, granularity="day": _Sparse(
            detect_relation="ods.orders",
            impact_range=SparseLookbackImpactRange(
                start_offset_days=0,
                end_offset_days=0,
                start_offset_months=-1,
                end_offset_months=1,
            ),
        ),
    )

    monkeypatch.setattr(
        factory,
        "detect_partition_max_watermarks",
        lambda **_: {date(2026, 4, 1): datetime(2026, 4, 1, 10, 0, 0)},
    )

    # offset_months=0 -> window covers 2026-02-01..2026-05-01
    sensors = factory.build_dbt_partition_change_sensors(specs=[_monthly_spec(offset_months=0)])
    defs = dg.Definitions(jobs=[job], sensors=sensors)

    context = dg.build_sensor_context(
        resources={"starrocks": object()},
        cursor="2000-01-01T00:00:00",
        definitions=defs,
        sensor_name="orders_partition_change_sensor",
    )
    data = sensors[0].evaluate_tick(context)

    assert sorted(rr.partition_key for rr in data.run_requests) == [
        "2026-03-01",
        "2026-04-01",
        "2026-05-01",
    ]


def test_unsupported_detector_partition_type_still_raises():
    """Hourly and unknown detector partition types remain rejected."""
    from dbt_dagsterizer.sensors.partition_change.detector import factory

    with pytest.raises(ValueError, match="Unsupported partition_type: hourly"):
        factory.build_dbt_partition_change_sensors(
            specs=[_monthly_spec(partition_type="hourly")]
        )


def test_parse_sparse_lookback_meta_rejects_day_offsets_for_month_granularity():
    """Day offsets would expand to invalid monthly keys, so they are rejected outright."""
    from dbt_dagsterizer.sensors.partition_change.detector.sparse_lookback import (
        parse_sparse_lookback_meta,
    )

    meta = {
        "detect_relation": "ods.orders",
        "partition_date_expr": "order_datetime",
        "updated_at_expr": "updated_at",
        "impact": {"type": "range", "start_offset_days": -1, "end_offset_days": 1},
    }

    with pytest.raises(ValueError, match="cannot be used with a monthly detector"):
        parse_sparse_lookback_meta(meta=meta, granularity="month")


def test_parse_sparse_lookback_meta_rejects_month_offsets_for_day_granularity():
    from dbt_dagsterizer.sensors.partition_change.detector.sparse_lookback import (
        parse_sparse_lookback_meta,
    )

    meta = {
        "detect_relation": "ods.orders",
        "partition_date_expr": "order_datetime",
        "updated_at_expr": "updated_at",
        "impact": {"type": "range", "start_offset_months": -1, "end_offset_months": 1},
    }

    with pytest.raises(ValueError, match="cannot be used with a daily detector"):
        parse_sparse_lookback_meta(meta=meta)


def test_parse_sparse_lookback_meta_month_impact_range():
    from dbt_dagsterizer.sensors.partition_change.detector.sparse_lookback import (
        parse_sparse_lookback_meta,
    )

    meta = {
        "detect_relation": "ods.orders",
        "partition_date_expr": "order_datetime",
        "updated_at_expr": "updated_at",
        "impact": {"type": "range", "start_offset_months": -2, "end_offset_months": 1},
    }

    parsed = parse_sparse_lookback_meta(meta=meta, granularity="month")
    assert parsed.impact_range is not None
    assert parsed.impact_range.start_offset_months == -2
    assert parsed.impact_range.end_offset_months == 1
    assert parsed.impact_range.start_offset_days == 0


def test_parse_sparse_lookback_meta_month_impact_range_start_after_end_raises():
    from dbt_dagsterizer.sensors.partition_change.detector.sparse_lookback import (
        parse_sparse_lookback_meta,
    )

    meta = {
        "detect_relation": "ods.orders",
        "partition_date_expr": "order_datetime",
        "updated_at_expr": "updated_at",
        "impact": {"type": "range", "start_offset_months": 2, "end_offset_months": 1},
    }

    with pytest.raises(ValueError, match="start_offset_months cannot be greater"):
        parse_sparse_lookback_meta(meta=meta, granularity="month")


def test_parse_sparse_lookback_meta_rejects_unknown_granularity():
    from dbt_dagsterizer.sensors.partition_change.detector.sparse_lookback import (
        parse_sparse_lookback_meta,
    )

    meta = {
        "detect_relation": "ods.orders",
        "partition_date_expr": "order_datetime",
        "updated_at_expr": "updated_at",
    }

    with pytest.raises(ValueError, match="Unsupported granularity: weekly"):
        parse_sparse_lookback_meta(meta=meta, granularity="weekly")


def test_expand_impacted_dates_uses_month_arithmetic():
    from dbt_dagsterizer.sensors.partition_change.detector.sparse_lookback import (
        SparseLookbackImpactRange,
        expand_impacted_dates,
    )

    impact_range = SparseLookbackImpactRange(
        start_offset_days=0,
        end_offset_days=0,
        start_offset_months=-1,
        end_offset_months=1,
    )

    expanded = expand_impacted_dates({date(2026, 1, 15)}, impact_range, granularity="month")
    assert expanded == {date(2025, 12, 1), date(2026, 1, 1), date(2026, 2, 1)}


def test_expand_impacted_dates_day_granularity_unchanged():
    from dbt_dagsterizer.sensors.partition_change.detector.sparse_lookback import (
        SparseLookbackImpactRange,
        expand_impacted_dates,
    )

    impact_range = SparseLookbackImpactRange(start_offset_days=-1, end_offset_days=1)
    base = date(2026, 3, 10)

    assert expand_impacted_dates({base}, impact_range) == {
        base - timedelta(days=1),
        base,
        base + timedelta(days=1),
    }


def test_monthly_detector_sql_truncates_to_month():
    """Month granularity queries date_trunc('month', ...) so values line up with month keys."""
    from dbt_dagsterizer.sensors.partition_change.detector.sparse_lookback import (
        SparseLookbackMeta,
        detect_partition_max_watermarks,
    )

    captured: list[str] = []

    class _FakeStarRocks:
        def query_rows(self, sql: str):
            captured.append(sql)
            return []

    meta = SparseLookbackMeta(
        detect_relation="ods.orders",
        partition_date_expr="order_datetime",
        updated_at_expr="updated_at",
    )

    detect_partition_max_watermarks(
        starrocks=_FakeStarRocks(),
        meta=meta,
        window_start=date(2026, 1, 1),
        window_end=date(2026, 4, 1),
        granularity="month",
    )

    assert "date_trunc('month', CAST((order_datetime) AS DATE))" in captured[0]

    captured.clear()
    detect_partition_max_watermarks(
        starrocks=_FakeStarRocks(),
        meta=meta,
        window_start=date(2026, 1, 1),
        window_end=date(2026, 4, 1),
    )

    assert "date_trunc" not in captured[0]
    assert "CAST((order_datetime) AS DATE)" in captured[0]


def _write_detector_config(tmp_path, monkeypatch, detectors: list[dict]):
    from dbt_dagsterizer.sensors.partition_change import auto_config

    model_names = ["daily_model", "hourly_model", "monthly_model"]
    models = [
        SimpleNamespace(name=n, database="db", schema="dws", identifier=n) for n in model_names
    ]
    monkeypatch.setattr(auto_config, "load_manifest", lambda: {"nodes": {}})
    monkeypatch.setattr(auto_config, "iter_models", lambda _manifest: models)
    monkeypatch.setattr(auto_config, "get_dbt_project_dir", lambda: tmp_path)

    (tmp_path / "dagsterization.yml").write_text(
        yaml.dump({
            "version": 1,
            "partitions": {
                "daily": ["daily_model"],
                "hourly": ["hourly_model"],
                "monthly": ["monthly_model"],
            },
            "partition_change": {"detectors": detectors},
        })
    )
    return auto_config


def _detector(model: str, job_name: str, **extra):
    entry = {
        "model": model,
        "enabled": True,
        "job_name": job_name,
        "partition_date_expr": "order_datetime",
        "updated_at_expr": "updated_at",
    }
    entry.update(extra)
    return entry


def test_auto_config_derives_monthly_detector_only_for_monthly_models(tmp_path, monkeypatch):
    """Monthly models get month-granular detectors; hourly models stay daily-granular."""
    auto_config = _write_detector_config(
        tmp_path,
        monkeypatch,
        [
            _detector("daily_model", "daily_job"),
            _detector("hourly_model", "hourly_job"),
            _detector("monthly_model", "monthly_job"),
        ],
    )

    specs = {s["detector_model"]: s for s in auto_config.build_auto_partition_change_detection_specs()}

    assert specs["monthly_model"]["partition_type"] == "monthly"
    assert specs["monthly_model"]["lookback_months"] == 3
    assert specs["monthly_model"]["offset_months"] == 0
    assert specs["monthly_model"]["lookback_days"] == 0
    assert specs["monthly_model"]["offset_days"] == 0

    # Regression guard: deriving the type naively would emit partition_type "hourly",
    # which the detector factory rejects at definitions-build time.
    assert specs["hourly_model"]["partition_type"] == "daily"
    assert specs["hourly_model"]["lookback_days"] == 7
    assert specs["hourly_model"]["offset_days"] == 1

    assert specs["daily_model"]["partition_type"] == "daily"
    assert specs["daily_model"]["lookback_days"] == 7


def test_auto_config_monthly_detector_honours_explicit_offsets(tmp_path, monkeypatch):
    auto_config = _write_detector_config(
        tmp_path,
        monkeypatch,
        [_detector("monthly_model", "monthly_job", lookback_months=6, offset_months=1)],
    )

    specs = auto_config.build_auto_partition_change_detection_specs()

    assert len(specs) == 1
    assert specs[0]["partition_type"] == "monthly"
    assert specs[0]["lookback_months"] == 6
    assert specs[0]["offset_months"] == 1
    assert specs[0]["name"] == "monthly_model_partition_change_sensor"
