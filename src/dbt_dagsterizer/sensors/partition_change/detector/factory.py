from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

import dagster as dg

from ....assets.dbt.prepare import prepare_manifest_if_missing
from ....jobs.dbt.jobs import get_dbt_jobs_by_name
from .dbt_manifest import load_manifest
from .sparse_lookback import (
    detect_partition_max_watermarks,
    expand_impacted_dates,
    parse_sparse_lookback_meta,
)


def _parse_legacy_cursor_ts(cursor: str | None) -> datetime | None:
    if not cursor:
        return None
    try:
        value = datetime.fromisoformat(cursor)
    except ValueError:
        return None
    return value.replace(tzinfo=None)


def _parse_watermark_cursor(cursor: str | None) -> dict[str, str] | None:
    if not cursor:
        return None
    try:
        payload = json.loads(cursor)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    if payload.get("type") != "partition_watermark_v1":
        return None
    partitions = payload.get("partitions")
    if not isinstance(partitions, dict):
        return {}
    out: dict[str, str] = {}
    for k, v in partitions.items():
        if isinstance(k, str) and isinstance(v, str) and k and v:
            out[k] = v
    return out


def build_dbt_partition_change_sensors(*, specs: list[dict]) -> list[dg.SensorDefinition]:
    duplicated = set()
    seen = set()
    for spec in specs:
        name = spec.get("name")
        if not name:
            continue
        if name in seen:
            duplicated.add(name)
        else:
            seen.add(name)
    if duplicated:
        raise ValueError(f"Duplicate partition change sensor names: {sorted(duplicated)}")

    def _make_sensor(spec: dict) -> dg.SensorDefinition:
        enabled = bool(spec.get("enabled", True))
        default_status = dg.DefaultSensorStatus.RUNNING if enabled else dg.DefaultSensorStatus.STOPPED

        partition_type = spec.get("partition_type", "daily")
        
        if partition_type != "daily":
            raise ValueError(f"Unsupported partition_type: {partition_type}")

        sensor_name = spec["name"]
        job_name = spec["job_name"]
        detector_model = spec["detector_model"]
        job = get_dbt_jobs_by_name()[job_name]

        lookback_days = int(spec.get("lookback_days", 0))
        offset_days = int(spec.get("offset_days", 0))
        minimum_interval_seconds = int(spec.get("minimum_interval_seconds", 60))

        @dg.sensor(
            name=sensor_name,
            job=job,
            default_status=default_status,
            minimum_interval_seconds=minimum_interval_seconds,
            required_resource_keys={"starrocks"},
        )
        def _sensor(context: dg.SensorEvaluationContext):
            prepare_manifest_if_missing()
            manifest = load_manifest()
            detector_meta = spec.get("meta")
            if not isinstance(detector_meta, dict) or not detector_meta:
                raise ValueError(f"Partition-change detector spec '{sensor_name}' is missing meta")
            sparse_meta = parse_sparse_lookback_meta(meta=detector_meta, manifest=manifest)

            now = datetime.now(timezone.utc)
            anchor_day = (now - timedelta(days=offset_days)).date()
            window_start = anchor_day - timedelta(days=lookback_days)
            window_end = anchor_day

            prior_by_partition = _parse_watermark_cursor(context.cursor)
            legacy_cursor_ts = None
            if prior_by_partition is None:
                legacy_cursor_ts = _parse_legacy_cursor_ts(context.cursor)
                if legacy_cursor_ts is None:
                    legacy_cursor_ts = (now - timedelta(days=lookback_days + offset_days + 1)).replace(tzinfo=None)
                prior_by_partition = {}

            try:
                max_watermarks = detect_partition_max_watermarks(
                    starrocks=context.resources.starrocks,
                    meta=sparse_meta,
                    window_start=window_start,
                    window_end=window_end,
                )
            except Exception as e:
                msg = str(e)
                lowered = msg.lower()
                if "unknown database" in lowered or "unknown table" in lowered or "doesn't exist" in lowered:
                    context.log.warning(
                        f"Partition-change detector skipped due to missing relation '{sparse_meta.detect_relation}': {msg}"
                    )
                    yield dg.SkipReason(
                        f"Missing relation for partition-change detector '{sensor_name}': {msg}"
                    )
                    return
                raise

            new_cursor_partitions: dict[str, str] = {}
            changed_partitions: dict[date, datetime] = {}

            items = [(d, w) for d, w in max_watermarks.items() if window_start <= d <= window_end]
            items.sort(key=lambda t: t[0].isoformat())

            for partition_date, watermark in items:
                partition_key = partition_date.isoformat()
                watermark_naive = watermark.replace(tzinfo=None)
                watermark_key = watermark_naive.isoformat()

                prev_raw = prior_by_partition.get(partition_key)
                prev_dt = None
                if isinstance(prev_raw, str) and prev_raw:
                    try:
                        prev_dt = datetime.fromisoformat(prev_raw).replace(tzinfo=None)
                    except ValueError:
                        prev_dt = None

                should_emit = False
                if prev_dt is not None:
                    should_emit = watermark_naive > prev_dt
                else:
                    should_emit = legacy_cursor_ts is None or watermark_naive > legacy_cursor_ts

                new_cursor_partitions[partition_key] = watermark_key

                if not should_emit:
                    continue

                changed_partitions[partition_date] = watermark_naive

            impact_range = getattr(sparse_meta, "impact_range", None)
            if impact_range is None:
                partitions_to_emit: dict[date, datetime] = dict(changed_partitions)
            else:
                partitions_to_emit: dict[date, datetime] = {}
                for base_date, base_watermark in changed_partitions.items():
                    impacted_dates = expand_impacted_dates({base_date}, impact_range)
                    for impacted_date in impacted_dates:
                        existing = partitions_to_emit.get(impacted_date)
                        if existing is None or base_watermark > existing:
                            partitions_to_emit[impacted_date] = base_watermark

            for partition_date, watermark_naive in sorted(
                partitions_to_emit.items(),
                key=lambda t: t[0].isoformat(),
            ):
                if partition_date < window_start or partition_date > window_end:
                    continue

                partition_key = partition_date.isoformat()
                watermark_key = watermark_naive.isoformat()
                run_key = f"{sensor_name}:{partition_key}:{watermark_key}"
                yield dg.RunRequest(
                    partition_key=partition_key,
                    run_key=run_key,
                    tags={
                        "luban/detector_model": detector_model,
                        "luban/detect_relation": sparse_meta.detect_relation,
                        "luban/partition_watermark": watermark_key,
                    },
                )

            context.update_cursor(
                json.dumps(
                    {
                        "type": "partition_watermark_v1",
                        "last_check": now.isoformat(),
                        "partitions": new_cursor_partitions,
                    },
                    sort_keys=True,
                )
            )

        return _sensor

    return [_make_sensor(spec) for spec in specs]
