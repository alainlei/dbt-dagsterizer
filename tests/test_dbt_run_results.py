from __future__ import annotations

import json
from pathlib import Path

from dbt_dagsterizer.dbt.run_results import (
    DbtTiming,
    _parse_iso8601_to_ns,
    _timing_bounds_ns,
    get_row_counts_by_unique_id,
    load_manifest_nodes,
    load_run_results,
    select_results_for_spans,
)


def test_parse_iso8601_to_ns_accepts_z_suffix():
    assert _parse_iso8601_to_ns("2026-05-10T00:00:00Z") is not None


def test_timing_bounds_ns_returns_none_when_missing():
    start_ns, end_ns = _timing_bounds_ns([])
    assert start_ns is None
    assert end_ns is None


def test_timing_bounds_ns_orders_bounds():
    timing = [
        DbtTiming(name="compile", started_at="2026-05-10T00:00:02Z", completed_at="2026-05-10T00:00:03Z"),
        DbtTiming(name="execute", started_at="2026-05-10T00:00:01Z", completed_at="2026-05-10T00:00:05Z"),
    ]
    start_ns, end_ns = _timing_bounds_ns(timing)
    assert start_ns is not None
    assert end_ns is not None
    assert end_ns > start_ns


def test_load_run_results_parses_results(tmp_path: Path):
    data = {
        "results": [
            {
                "unique_id": "model.demo.orders",
                "status": "success",
                "execution_time": 1.23,
                "timing": [
                    {"name": "execute", "started_at": "2026-05-10T00:00:01Z", "completed_at": "2026-05-10T00:00:02Z"}
                ],
            }
        ]
    }
    path = tmp_path / "run_results.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    parsed = load_run_results(path)
    assert len(parsed) == 1
    assert parsed[0].unique_id == "model.demo.orders"
    assert parsed[0].execution_time_s == 1.23


def test_load_manifest_nodes_indexes_nodes(tmp_path: Path):
    data = {
        "nodes": {
            "model.demo.orders": {
                "resource_type": "model",
                "name": "orders",
                "package_name": "demo",
                "path": "models/orders.sql",
                "fqn": ["demo", "orders"],
            }
        }
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    idx = load_manifest_nodes(path)
    assert "model.demo.orders" in idx
    node = idx["model.demo.orders"]
    assert node.resource_type == "model"
    assert node.name == "orders"


def test_select_results_for_spans_picks_failures_and_top_n():
    from dbt_dagsterizer.dbt.run_results import DbtRunResult

    results = [
        DbtRunResult(unique_id="model.a", status="success", execution_time_s=1.0, timing=[]),
        DbtRunResult(unique_id="model.b", status="success", execution_time_s=5.0, timing=[]),
        DbtRunResult(unique_id="model.c", status="success", execution_time_s=3.0, timing=[]),
        DbtRunResult(unique_id="model.d", status="fail", execution_time_s=0.1, timing=[]),
    ]
    selected = select_results_for_spans(results, top_n=2, min_execution_time_s=0.0)
    ids = {r.unique_id for r in selected}
    assert "model.d" in ids
    assert "model.b" in ids
    assert "model.c" in ids or "model.a" in ids


def test_load_run_results_extracts_rows_affected(tmp_path: Path):
    """Test that rows_affected is extracted from adapter_response."""
    data = {
        "results": [
            {
                "unique_id": "model.demo.orders",
                "status": "success",
                "execution_time": 1.23,
                "timing": [],
                "adapter_response": {
                    "rows_affected": 1000,
                },
            },
            {
                "unique_id": "model.demo.customers",
                "status": "success",
                "execution_time": 2.5,
                "timing": [],
                "adapter_response": {
                    "rows_affected": "500",  # Test string conversion
                },
            },
            {
                "unique_id": "model.demo.no_rows",
                "status": "success",
                "execution_time": 0.5,
                "timing": [],
                "adapter_response": {},
            },
        ]
    }
    path = tmp_path / "run_results.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    parsed = load_run_results(path)
    
    assert len(parsed) == 3
    assert parsed[0].rows_affected == 1000
    assert parsed[1].rows_affected == 500
    assert parsed[2].rows_affected is None


def test_get_row_counts_by_unique_id_filters_and_maps():
    """Test that row counts are correctly extracted and filtered."""
    from dbt_dagsterizer.dbt.run_results import DbtRunResult

    results = [
        DbtRunResult(unique_id="model.a", status="success", execution_time_s=1.0, timing=[], rows_affected=100),
        DbtRunResult(unique_id="model.b", status="success", execution_time_s=2.0, timing=[], rows_affected=200),
        DbtRunResult(unique_id="model.c", status="success", execution_time_s=3.0, timing=[], rows_affected=None),
    ]
    
    row_counts = get_row_counts_by_unique_id(results)
    
    assert len(row_counts) == 2
    assert row_counts["model.a"] == 100
    assert row_counts["model.b"] == 200
    assert "model.c" not in row_counts
