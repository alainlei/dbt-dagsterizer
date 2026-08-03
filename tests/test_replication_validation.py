"""Tests for replication validation in validation.py."""

from __future__ import annotations

from pathlib import Path

from dbt_dagsterizer.cli_parts.validation import (
    validate_orchestration,
    validate_orchestration_structure,
)


def _manifest_with_models(*models: str) -> dict:
    nodes = {}
    for m in models:
        nodes[f"model.demo.{m}"] = {
            "resource_type": "model",
            "name": m,
            "database": "db",
            "schema": "dwd",
            "identifier": m,
        }
    return {"nodes": nodes, "sources": {}}


# --- Structure validation ---


def test_validate_structure_replication_not_mapping():
    issues = validate_orchestration_structure(
        orchestration={"version": 1, "replication": "not-a-mapping"}
    )
    assert any("replication must be a mapping" in i.message for i in issues)


def test_validate_structure_replication_enabled_not_bool():
    issues = validate_orchestration_structure(
        orchestration={"version": 1, "replication": {"enabled": "yes"}}
    )
    assert any("replication.enabled must be a boolean" in i.message for i in issues)


def test_validate_structure_replication_entries_not_list():
    issues = validate_orchestration_structure(
        orchestration={"version": 1, "replication": {"entries": "not-a-list"}}
    )
    assert any("replication.entries must be a list" in i.message for i in issues)


def test_validate_structure_entry_missing_model():
    issues = validate_orchestration_structure(
        orchestration={"version": 1, "replication": {"entries": [{"enabled": True}]}}
    )
    assert any("model must be non-empty" in i.message for i in issues)


def test_validate_structure_entry_invalid_write_disposition():
    issues = validate_orchestration_structure(
        orchestration={
            "version": 1,
            "replication": {
                "entries": [{"model": "orders", "write_disposition": "upsert"}],
            },
        }
    )
    assert any("write_disposition must be" in i.message for i in issues)


def test_validate_structure_entry_accepts_merge():
    """Verify that 'merge' is accepted as a valid write disposition."""
    issues = validate_orchestration_structure(
        orchestration={
            "version": 1,
            "replication": {
                "entries": [{"model": "orders", "write_disposition": "merge"}],
            },
        }
    )
    assert not any("write_disposition" in i.message for i in issues)


def test_validate_structure_valid_replication():
    issues = validate_orchestration_structure(
        orchestration={
            "version": 1,
            "replication": {
                "enabled": True,
                "entries": [
                    {"model": "orders", "destination_table": "orders", "write_disposition": "replace"},
                ],
            },
        }
    )
    assert not any(i.level == "error" for i in issues)


# --- Cross-reference validation ---


def test_validate_missing_model_in_manifest():
    manifest = _manifest_with_models("orders")
    issues = validate_orchestration(
        manifest=manifest,
        orchestration={
            "version": 1,
            "replication": {
                "enabled": True,
                "entries": [{"model": "nonexistent"}],
            },
        },
        require_file_exists=False,
        orchestration_path=Path("/tmp/dagsterization.yml"),
    )
    assert any("references missing model" in i.message for i in issues)


def test_validate_partitioned_model_without_partition_column_warns():
    manifest = _manifest_with_models("orders")
    issues = validate_orchestration(
        manifest=manifest,
        orchestration={
            "version": 1,
            "partitions": {"daily": ["orders"]},
            "replication": {
                "enabled": True,
                "entries": [{"model": "orders"}],
            },
        },
        require_file_exists=False,
        orchestration_path=Path("/tmp/dagsterization.yml"),
    )
    warns = [i for i in issues if i.level == "warn"]
    assert any("partition_column" in i.message for i in warns)


def test_validate_partitioned_model_with_partition_column_no_warning():
    manifest = _manifest_with_models("orders")
    issues = validate_orchestration(
        manifest=manifest,
        orchestration={
            "version": 1,
            "partitions": {"daily": ["orders"]},
            "replication": {
                "enabled": True,
                "entries": [{"model": "orders", "partition_column": "order_date"}],
            },
        },
        require_file_exists=False,
        orchestration_path=Path("/tmp/dagsterization.yml"),
    )
    warns = [i for i in issues if i.level == "warn" and "partition_column" in i.message]
    assert len(warns) == 0


def test_validate_no_issues_when_replication_disabled():
    manifest = _manifest_with_models("orders")
    issues = validate_orchestration(
        manifest=manifest,
        orchestration={
            "version": 1,
            "replication": {"enabled": False, "entries": []},
        },
        require_file_exists=False,
        orchestration_path=Path("/tmp/dagsterization.yml"),
    )
    assert issues == []


def test_validate_accepts_merge_write_disposition():
    """Verify that 'merge' is accepted in full cross-reference validation."""
    manifest = _manifest_with_models("orders")
    issues = validate_orchestration(
        manifest=manifest,
        orchestration={
            "version": 1,
            "replication": {
                "enabled": True,
                "entries": [{"model": "orders", "write_disposition": "merge"}],
            },
        },
        require_file_exists=False,
        orchestration_path=Path("/tmp/dagsterization.yml"),
    )
    assert not any("write_disposition" in i.message for i in issues)


def test_validate_no_issues_when_replication_absent():
    manifest = _manifest_with_models("orders")
    issues = validate_orchestration(
        manifest=manifest,
        orchestration={
            "version": 1,
            "jobs": {},
            "asset_jobs": [],
            "partitions": {},
            "schedules": {},
            "partition_change": {"detectors": [], "propagators": []},
        },
        require_file_exists=False,
        orchestration_path=Path("/tmp/dagsterization.yml"),
    )
    assert issues == []
