from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class DbtTiming:
    name: str
    started_at: str | None
    completed_at: str | None


@dataclass(frozen=True)
class DbtRunResult:
    unique_id: str
    status: str
    execution_time_s: float
    timing: list[DbtTiming]
    rows_affected: int | None = None


@dataclass(frozen=True)
class DbtManifestNode:
    unique_id: str
    resource_type: str
    name: str
    package_name: str
    path: str
    fqn: list[str]


def _parse_iso8601_to_ns(value: str) -> int | None:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000_000)


def _timing_bounds_ns(timing: list[DbtTiming]) -> tuple[int | None, int | None]:
    starts: list[int] = []
    ends: list[int] = []
    for t in timing:
        if t.started_at:
            start = _parse_iso8601_to_ns(t.started_at)
            if start is not None:
                starts.append(start)
        if t.completed_at:
            end = _parse_iso8601_to_ns(t.completed_at)
            if end is not None:
                ends.append(end)
    if not starts or not ends:
        return None, None
    start_ns = min(starts)
    end_ns = max(ends)
    if end_ns < start_ns:
        return None, None
    return start_ns, end_ns


def load_run_results(path: Path) -> list[DbtRunResult]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    
    return parse_run_results(data)

def parse_run_results(data, success_only: bool = False) -> list[DbtRunResult]:
    results = data.get("results")
    if not isinstance(results, list):
        return []

    parsed: list[DbtRunResult] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        unique_id = item.get("unique_id")
        if not isinstance(unique_id, str) or not unique_id.strip():
            continue
        status = item.get("status")
        status_str = str(status or "")

        if success_only and status_str not in {"success"}:
            continue

        execution_time = item.get("execution_time")
        try:
            execution_time_s = float(execution_time or 0.0)
        except Exception:
            execution_time_s = 0.0

        # Extract rows_affected from adapter_response if available
        rows_affected = None
        adapter_response = item.get("adapter_response")
        if isinstance(adapter_response, dict):
            rows = adapter_response.get("rows_affected")
            if rows is not None:
                try:
                    rows_affected = int(rows)
                except (ValueError, TypeError):
                    pass

        timing_raw = item.get("timing") or []
        timing: list[DbtTiming] = []
        if isinstance(timing_raw, list):
            for t in timing_raw:
                if not isinstance(t, dict):
                    continue
                name = str(t.get("name") or "")
                started_at = t.get("started_at")
                completed_at = t.get("completed_at")
                timing.append(
                    DbtTiming(
                        name=name,
                        started_at=str(started_at) if started_at is not None else None,
                        completed_at=str(completed_at) if completed_at is not None else None,
                    )
                )

        parsed.append(
            DbtRunResult(
                unique_id=unique_id.strip(),
                status=status_str,
                execution_time_s=execution_time_s,
                timing=timing,
                rows_affected=rows_affected,
            )
        )

    return parsed


def load_manifest_nodes(path: Path) -> dict[str, DbtManifestNode]:
    with path.open("r", encoding="utf-8") as f:
        manifest = json.load(f)

    index: dict[str, DbtManifestNode] = {}
    for section in ("nodes", "sources", "metrics", "exposures", "semantic_models", "saved_queries"):
        items = manifest.get(section) or {}
        if not isinstance(items, dict):
            continue
        for unique_id, props in items.items():
            if not isinstance(unique_id, str) or not unique_id:
                continue
            if not isinstance(props, dict):
                continue

            resource_type = props.get("resource_type")
            name = props.get("name")
            package_name = props.get("package_name")
            path_ = props.get("path")
            fqn = props.get("fqn")

            if not isinstance(resource_type, str) or not resource_type:
                continue
            if not isinstance(name, str) or not name:
                continue

            index[unique_id] = DbtManifestNode(
                unique_id=unique_id,
                resource_type=resource_type,
                name=name,
                package_name=str(package_name or ""),
                path=str(path_ or ""),
                fqn=list(fqn or []),
            )
    return index


def select_results_for_spans(
    results: list[DbtRunResult],
    *,
    top_n: int,
    min_execution_time_s: float,
) -> list[DbtRunResult]:
    top_n = max(0, int(top_n))
    min_execution_time_s = float(min_execution_time_s)

    failures: list[DbtRunResult] = []
    candidates: list[DbtRunResult] = []
    for r in results:
        status = (r.status or "").lower()
        if status in {"error", "fail"}:
            failures.append(r)
            continue
        if r.execution_time_s >= min_execution_time_s:
            candidates.append(r)

    candidates.sort(key=lambda x: x.execution_time_s, reverse=True)
    picked = candidates[:top_n]

    by_id: dict[str, DbtRunResult] = {r.unique_id: r for r in failures}
    for r in picked:
        by_id.setdefault(r.unique_id, r)

    merged = list(by_id.values())
    merged.sort(key=lambda x: (x.unique_id))
    return merged


def get_row_counts_by_unique_id(
    results: list[DbtRunResult],
) -> dict[str, int]:
    """Extract row counts from dbt run results, keyed by unique_id.
    
    Returns a dictionary mapping unique_id to rows_affected.
    Only includes results where rows_affected is available.
    """
    row_counts: dict[str, int] = {}
    for r in results:
        if r.rows_affected is not None:
            row_counts[r.unique_id] = r.rows_affected
    return row_counts


def env_dbt_run_results_top_n() -> int:
    raw = os.getenv("LUBAN_OTEL_DBT_RUN_RESULTS_TOP_N")
    if raw is None:
        return 20
    try:
        return max(0, int(raw.strip()))
    except Exception:
        return 20


def env_dbt_run_results_min_seconds() -> float:
    raw = os.getenv("LUBAN_OTEL_DBT_RUN_RESULTS_MIN_SECONDS")
    if raw is None:
        return 0.0
    try:
        return max(0.0, float(raw.strip()))
    except Exception:
        return 0.0


def env_dbt_run_results_mode() -> str:
    raw = (os.getenv("LUBAN_OTEL_DBT_RUN_RESULTS_MODE") or "spans").strip().lower()
    if raw in {"none", "off", "false", "0"}:
        return "none"
    if raw in {"events"}:
        return "events"
    return "spans"


def add_run_results_telemetry(
    *,
    run_results_path: Path,
    manifest_path: Path | None,
    parent_span_attributes: dict[str, Any] | None = None,
) -> None:
    mode = env_dbt_run_results_mode()
    if mode == "none":
        return

    try:
        from opentelemetry import trace
    except Exception:
        return

    try:
        current = trace.get_current_span()
        ctx = current.get_span_context()
        is_valid = bool(getattr(ctx, "is_valid", False))
        is_recording = bool(getattr(current, "is_recording", lambda: False)())
    except Exception:
        return

    if not (is_valid and is_recording):
        return

    if not run_results_path.exists():
        return

    try:
        results = load_run_results(run_results_path)
    except Exception:
        return

    top_n = env_dbt_run_results_top_n()
    min_seconds = env_dbt_run_results_min_seconds()
    selected = select_results_for_spans(results, top_n=top_n, min_execution_time_s=min_seconds)
    if not selected:
        return

    nodes: dict[str, DbtManifestNode] = {}
    if manifest_path and manifest_path.exists():
        try:
            nodes = load_manifest_nodes(manifest_path)
        except Exception:
            nodes = {}

    if mode == "events":
        for r in selected:
            node = nodes.get(r.unique_id)
            attrs: dict[str, Any] = {
                "dbt.unique_id": r.unique_id,
                "dbt.status": r.status,
                "dbt.execution_time_s": r.execution_time_s,
            }
            if node:
                attrs.update(
                    {
                        "dbt.resource_type": node.resource_type,
                        "dbt.name": node.name,
                        "dbt.package_name": node.package_name,
                        "dbt.path": node.path,
                    }
                )
            try:
                current.add_event("dbt.run_result", attributes=attrs)
            except Exception:
                continue
        return

    tracer = trace.get_tracer("dbt_dagsterizer")
    parent_ctx = trace.set_span_in_context(current)
    common_attrs = dict(parent_span_attributes or {})

    for r in selected:
        node = nodes.get(r.unique_id)
        resource_type = node.resource_type if node else ""
        node_name = node.name if node else r.unique_id
        span_name = f"dbt.{resource_type}/{node_name}" if resource_type else f"dbt.node/{node_name}"

        attrs: dict[str, Any] = dict(common_attrs)
        attrs.update(
            {
                "dbt.unique_id": r.unique_id,
                "dbt.status": r.status,
                "dbt.execution_time_s": r.execution_time_s,
            }
        )
        if node:
            attrs.update(
                {
                    "dbt.resource_type": node.resource_type,
                    "dbt.name": node.name,
                    "dbt.package_name": node.package_name,
                    "dbt.path": node.path,
                }
            )
            if node.fqn:
                attrs["dbt.fqn"] = ".".join(str(p) for p in node.fqn if p)

        start_ns, end_ns = _timing_bounds_ns(r.timing)
        try:
            span = tracer.start_span(
                span_name,
                context=parent_ctx,
                attributes=attrs,
                start_time=start_ns,
            )
            span.end(end_time=end_ns)
        except Exception:
            continue

