import os

import pytest

from dbt_dagsterizer.otel import (
    _normalize_otlp_endpoint,
    _parse_resource_attributes,
    configure_otel,
    otel_dagster_transaction_info,
)


def test_parse_resource_attributes():
    assert _parse_resource_attributes("") == {}
    assert _parse_resource_attributes(None) == {}
    assert _parse_resource_attributes("a=b") == {"a": "b"}
    assert _parse_resource_attributes(" a = b , c=d , nope , e= ") == {"a": "b", "c": "d"}


def test_normalize_otlp_endpoint():
    assert _normalize_otlp_endpoint(endpoint="http://localhost:4317", protocol="grpc") == "localhost:4317"
    assert _normalize_otlp_endpoint(endpoint="localhost:4317", protocol="grpc") == "localhost:4317"
    assert _normalize_otlp_endpoint(endpoint="localhost:4318", protocol="http/protobuf") == "http://localhost:4318"
    assert (
        _normalize_otlp_endpoint(endpoint="https://localhost:4318", protocol="http/protobuf")
        == "https://localhost:4318"
    )


def test_configure_otel_noop_when_disabled(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("OTEL_TRACES_EXPORTER", "none")
    monkeypatch.setenv("OTEL_METRICS_EXPORTER", "none")
    res = configure_otel()
    assert res.traces_configured is False
    assert res.metrics_configured is False


def test_configure_otel_missing_endpoint_disables(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("OTEL_TRACES_EXPORTER", "otlp")
    monkeypatch.setenv("OTEL_METRICS_EXPORTER", "none")
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    res = configure_otel()
    assert res.traces_configured is False
    assert res.metrics_configured is False


def test_configure_otel_invalid_protocol_disables(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("OTEL_TRACES_EXPORTER", "otlp")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "nope")
    res = configure_otel()
    assert res.traces_configured is False
    assert res.metrics_configured is False


def test_configure_otel_handles_missing_packages(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("OTEL_TRACES_EXPORTER", "otlp")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
    res = configure_otel()
    try:
        from importlib import import_module

        import_module("opentelemetry.sdk.trace")
        import_module("opentelemetry.exporter.otlp.proto.grpc.trace_exporter")
    except Exception:
        assert res.traces_configured is False
        assert res.metrics_configured is False
    else:
        assert res.traces_configured is True


class _Run:
    def __init__(self, *, run_id: str, tags: dict[str, str]):
        self.run_id = run_id
        self.tags = tags


class _Ctx:
    def __init__(self, *, run_id: str, tags: dict[str, str], job_name: str = ""):
        self.run_id = run_id
        self.run = _Run(run_id=run_id, tags=tags)
        self.job_name = job_name


class _CtxWithExplodingPartitionKey(_Ctx):
    @property
    def partition_key(self) -> str:
        raise RuntimeError("Cannot access partition_key for non-partitioned run")


def test_dagster_transaction_schedule():
    ctx = _Ctx(
        run_id="r1",
        job_name="my_job",
        tags={"dagster/schedule_name": "my_schedule", "dagster/code_location": "demo"},
    )
    span_name, tx_type, attrs = otel_dagster_transaction_info(ctx)
    assert span_name == "schedule/my_schedule"
    assert tx_type == "schedule"
    assert attrs["transaction.type"] == "schedule"
    assert attrs["transaction.name"] == "my_schedule"


def test_dagster_transaction_detector_and_propagator():
    det = _Ctx(
        run_id="r2",
        job_name="daily_facts_job",
        tags={"dagster/sensor_name": "det_sensor", "luban/detector_model": "orders"},
    )
    span_name, tx_type, attrs = otel_dagster_transaction_info(det)
    assert span_name == "detector/det_sensor"
    assert tx_type == "detector"
    assert attrs["transaction.name"] == "det_sensor"

    prop = _Ctx(
        run_id="r3",
        job_name="daily_facts_job",
        tags={"dagster/sensor_name": "prop_sensor", "luban/upstream_dbt_model": "orders"},
    )
    span_name, tx_type, attrs = otel_dagster_transaction_info(prop)
    assert span_name == "propagator/prop_sensor"
    assert tx_type == "propagator"
    assert attrs["transaction.name"] == "prop_sensor"


def test_dagster_transaction_asset_job_uses_code_location():
    ctx = _Ctx(
        run_id="r4",
        job_name="__ASSET_JOB",
        tags={"dagster/code_location": "demo"},
    )
    span_name, tx_type, attrs = otel_dagster_transaction_info(ctx)
    assert span_name == "asset_job/demo"
    assert tx_type == "asset_job"
    assert attrs["dagster.code_location"] == "demo"


def test_dagster_transaction_handles_non_partitioned_run_context():
    ctx = _CtxWithExplodingPartitionKey(
        run_id="r5",
        job_name="my_job",
        tags={"dagster/code_location": "demo"},
    )
    span_name, tx_type, attrs = otel_dagster_transaction_info(ctx)
    assert span_name == "job/my_job"
    assert tx_type == "job"
    assert attrs["dagster.partition_key"] == ""


def test_dagster_transaction_includes_code_location_when_opted_in(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("LUBAN_OTEL_SPAN_NAME_INCLUDE_CODE_LOCATION", "1")

    schedule_ctx = _Ctx(
        run_id="r6",
        job_name="my_job",
        tags={"dagster/schedule_name": "my_schedule", "dagster/code_location": "demo"},
    )
    span_name, _, _ = otel_dagster_transaction_info(schedule_ctx)
    assert span_name == "schedule/demo/my_schedule"

    job_ctx = _CtxWithExplodingPartitionKey(
        run_id="r7",
        job_name="my_job",
        tags={"dagster/code_location": "demo"},
    )
    span_name, _, _ = otel_dagster_transaction_info(job_ctx)
    assert span_name == "job/demo/my_job"


def test_dagster_transaction_asset_job_skips_duplicate_code_location_when_opted_in(
    monkeypatch: pytest.MonkeyPatch,
):
    """Even with code-location inclusion on, the __ASSET_JOB__ branch already
    carries the code_location value inside tx_name; we must NOT double-print
    it in the span name."""
    monkeypatch.setenv("LUBAN_OTEL_SPAN_NAME_INCLUDE_CODE_LOCATION", "true")

    ctx = _Ctx(
        run_id="r8",
        job_name="__ASSET_JOB",
        tags={"dagster/code_location": "demo"},
    )
    span_name, _, attrs = otel_dagster_transaction_info(ctx)
    assert span_name == "asset_job/demo"
    assert attrs["dagster.code_location"] == "demo"


def test_dagster_transaction_skips_code_location_when_env_not_truthy(
    monkeypatch: pytest.MonkeyPatch,
):
    for value in ("", "0", "false", "off", "no"):
        monkeypatch.setenv("LUBAN_OTEL_SPAN_NAME_INCLUDE_CODE_LOCATION", value)
        ctx = _Ctx(
            run_id="r9",
            job_name="my_job",
            tags={"dagster/code_location": "demo"},
        )
        span_name, _, _ = otel_dagster_transaction_info(ctx)
        assert span_name == "job/my_job", f"unexpected span_name for env value {value!r}"
