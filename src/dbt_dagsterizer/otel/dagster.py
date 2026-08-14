from __future__ import annotations

from contextlib import contextmanager
from typing import Any


@contextmanager
def otel_span(name: str, *, attributes: dict[str, Any] | None = None):
    try:
        from opentelemetry import trace
    except Exception:
        yield None
        return

    tracer = trace.get_tracer("dbt_dagsterizer")
    attrs = dict(attributes or {})
    with tracer.start_as_current_span(name, attributes=attrs) as span:
        yield span


@contextmanager
def otel_child_span(name: str, *, attributes: dict[str, Any] | None = None):
    try:
        from opentelemetry import trace
    except Exception:
        yield None
        return

    current = trace.get_current_span()
    try:
        ctx = current.get_span_context()
        is_valid = bool(getattr(ctx, "is_valid", False))
        is_recording = bool(getattr(current, "is_recording", lambda: False)())
    except Exception:
        is_valid = False
        is_recording = False

    if not (is_valid and is_recording):
        yield None
        return

    tracer = trace.get_tracer("dbt_dagsterizer")
    attrs = dict(attributes or {})
    with tracer.start_as_current_span(name, attributes=attrs) as span:
        yield span


def otel_record_exception(span: Any, exc: BaseException) -> None:
    if span is None:
        return
    try:
        from opentelemetry.trace.status import Status, StatusCode
    except Exception:
        return

    try:
        span.record_exception(exc)
        span.set_status(Status(StatusCode.ERROR, str(exc)))
    except Exception:
        return


def _dagster_run_tags(context: Any) -> dict[str, str]:
    run = getattr(context, "run", None)
    tags = getattr(run, "tags", None)
    if isinstance(tags, dict):
        return {str(k): str(v) for k, v in tags.items()}

    tags = getattr(context, "run_tags", None)
    if isinstance(tags, dict):
        return {str(k): str(v) for k, v in tags.items()}

    tags = getattr(context, "tags", None)
    if isinstance(tags, dict):
        return {str(k): str(v) for k, v in tags.items()}

    return {}


def _safe_dagster_partition_key(context: Any) -> str:
    try:
        value = getattr(context, "partition_key")
    except Exception:
        return ""
    if not value:
        return ""
    return str(value)


def otel_dagster_transaction_info(context: Any) -> tuple[str, str, dict[str, Any]]:
    tags = _dagster_run_tags(context)

    schedule_name = tags.get("dagster/schedule_name") or ""
    sensor_name = tags.get("dagster/sensor_name") or ""
    backfill_id = tags.get("dagster/backfill") or ""
    from_ui = tags.get("dagster/from_ui") or ""

    job_name = (
        getattr(context, "job_name", "")
        or getattr(getattr(context, "job_def", None), "name", "")
        or ""
    )
    partition_key = tags.get("dagster/partition") or _safe_dagster_partition_key(context)
    code_location = tags.get("dagster/code_location") or ""

    tx_type = "job"
    tx_name = job_name

    if schedule_name:
        tx_type = "schedule"
        tx_name = schedule_name
    elif sensor_name:
        if tags.get("luban/detector_model"):
            tx_type = "detector"
        elif tags.get("luban/upstream_dbt_model"):
            tx_type = "propagator"
        else:
            tx_type = "sensor"
        tx_name = sensor_name
    elif backfill_id:
        tx_type = "backfill"
        tx_name = backfill_id
    elif job_name == "__ASSET_JOB":
        tx_type = "asset_job"
        tx_name = code_location or "asset_job"
    elif (from_ui or "").lower() in {"1", "true", "yes"}:
        tx_type = "manual"
        tx_name = job_name or "manual"

    if code_location:
        span_name = f"{tx_type}/{code_location}/{tx_name}" if tx_name else f"{tx_type}/{code_location}"
    else:
        span_name = f"{tx_type}/{tx_name}" if tx_name else tx_type

    attrs: dict[str, Any] = {
        "transaction.type": tx_type,
        "transaction.name": tx_name,
        "dagster.job_name": job_name,
        "dagster.schedule_name": schedule_name,
        "dagster.sensor_name": sensor_name,
        "dagster.backfill_id": backfill_id,
        "dagster.partition_key": partition_key,
        "dagster.code_location": code_location,
    }

    run_id = (
        getattr(context, "run_id", "")
        or getattr(getattr(context, "run", None), "run_id", "")
        or ""
    )
    if run_id:
        attrs["dagster.run_id"] = run_id

    return span_name, tx_type, attrs


@contextmanager
def otel_transaction_span(name: str, *, attributes: dict[str, Any] | None = None):
    try:
        from opentelemetry import trace
    except Exception:
        yield None
        return

    current = trace.get_current_span()
    try:
        ctx = current.get_span_context()
        is_valid = bool(getattr(ctx, "is_valid", False))
        is_recording = bool(getattr(current, "is_recording", lambda: False)())
    except Exception:
        is_valid = False
        is_recording = False

    attrs = dict(attributes or {})
    if is_valid and is_recording:
        for k, v in attrs.items():
            try:
                current.set_attribute(k, v)
            except Exception:
                continue
        yield current
        return

    tracer = trace.get_tracer("dbt_dagsterizer")
    with tracer.start_as_current_span(name, attributes=attrs) as span:
        yield span

