"""Tests for replication auto_config spec building.

These tests verify that ``build_auto_replication_specs()`` correctly reads
``dagsterization.yml`` and produces spec dicts.  Since the function depends on
a loaded dbt manifest and DBT_PROJECT_DIR env var, we monkeypatch the
heavyweight dependencies.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest


# Minimal manifest used for testing
_MANIFEST = {
    "nodes": {
        "model.demo.orders": {
            "resource_type": "model",
            "name": "orders",
            "database": "warehouse",
            "schema": "dwd",
            "identifier": "orders",
        },
        "model.demo.customers": {
            "resource_type": "model",
            "name": "customers",
            "database": "warehouse",
            "schema": "dwd",
            "identifier": "customers",
        },
    },
    "sources": {},
}


@pytest.fixture
def dbt_project(tmp_path: Path, monkeypatch) -> Path:
    """Create a minimal dbt project with a manifest.json and dagsterization.yml."""
    dbt_project_dir = tmp_path / "dbt_project"
    dbt_project_dir.mkdir(parents=True)

    # dbt_project.yml
    (dbt_project_dir / "dbt_project.yml").write_text(
        "name: demo\nprofile: demo\n", encoding="utf-8"
    )
    # profiles.yml
    (dbt_project_dir / "profiles.yml").write_text(
        "demo:\n  target: dev\n  outputs:\n    dev:\n      type: starrocks\n      host: localhost\n      port: 9030\n      user: root\n      password: ''\n      schema: demo\n",
        encoding="utf-8",
    )
    # models
    (dbt_project_dir / "models").mkdir()
    (dbt_project_dir / "models" / "one.sql").write_text("select 1 as one\n", encoding="utf-8")

    # manifest.json (pre-generated so no dbt parse is needed)
    target_dir = dbt_project_dir / "target"
    target_dir.mkdir()
    (target_dir / "manifest.json").write_text(json.dumps(_MANIFEST), encoding="utf-8")

    monkeypatch.setenv("DBT_PROJECT_DIR", str(dbt_project_dir))
    monkeypatch.setenv("LUBAN_DBT_PREPARE_ON_LOAD", "0")
    monkeypatch.setenv("STARROCKS_HOST", "mock_host")
    monkeypatch.setenv("STARROCKS_PORT", "9030")
    monkeypatch.setenv("STARROCKS_USER", "mock_user")
    monkeypatch.setenv("STARROCKS_PASSWORD", "mock_pass")
    monkeypatch.setenv("DAGSTER_DAILY_PARTITIONS_START_DATE", "2026-01-01")

    return dbt_project_dir


def _write_dagsterization(dbt_project_dir: Path, data: dict) -> None:
    from ruamel.yaml import YAML
    y = YAML()
    with (dbt_project_dir / "dagsterization.yml").open("w", encoding="utf-8") as f:
        y.dump(data, f)


def test_specs_empty_when_replication_disabled(dbt_project: Path):
    from dbt_dagsterizer.orchestration_config import load_or_create

    cfg_path = dbt_project / "dagsterization.yml"
    data = load_or_create(cfg_path)
    # replication.enabled defaults to False
    _write_dagsterization(dbt_project, data)

    from dbt_dagsterizer.assets.replication.auto_config import build_auto_replication_specs

    specs = build_auto_replication_specs()
    assert specs == []


def test_specs_empty_when_no_entries(dbt_project: Path):
    from dbt_dagsterizer.orchestration_config import load_or_create

    cfg_path = dbt_project / "dagsterization.yml"
    data = load_or_create(cfg_path)
    data["replication"]["enabled"] = True
    _write_dagsterization(dbt_project, data)

    from dbt_dagsterizer.assets.replication.auto_config import build_auto_replication_specs

    with patch("dbt_dagsterizer.assets.replication.auto_config.load_manifest", return_value=_MANIFEST):
        specs = build_auto_replication_specs()
    assert specs == []


def test_specs_returns_correct_entries(dbt_project: Path):
    from dbt_dagsterizer.orchestration_config import load_or_create, set_replication_entry

    cfg_path = dbt_project / "dagsterization.yml"
    data = load_or_create(cfg_path)
    data["replication"]["enabled"] = True
    set_replication_entry(
        data=data,
        model="orders",
        enabled=True,
        destination_table="orders",
        destination_schema="dbo",
        write_disposition="replace",
        partition_column="order_date",
    )
    set_replication_entry(
        data=data,
        model="customers",
        enabled=False,  # disabled entry should be skipped
        destination_table=None,
        destination_schema=None,
        write_disposition=None,
        partition_column=None,
    )
    _write_dagsterization(dbt_project, data)

    from dbt_dagsterizer.assets.replication.auto_config import build_auto_replication_specs

    with patch("dbt_dagsterizer.assets.replication.auto_config.load_manifest", return_value=_MANIFEST):
        specs = build_auto_replication_specs()
    assert len(specs) == 1
    spec = specs[0]
    assert spec["model"] == "orders"
    assert spec["name"] == "replicate_orders"
    assert spec["source_database"] == "warehouse"
    assert spec["source_schema"] == "dwd"
    assert spec["source_table"] == "orders"
    assert spec["destination_table"] == "orders"
    assert spec["destination_schema"] == "dbo"
    assert spec["write_disposition"] == "replace"
    assert spec["partition_column"] == "order_date"
    assert spec["partition_type"] == "unpartitioned"


def test_specs_includes_partition_type(dbt_project: Path):
    from dbt_dagsterizer.orchestration_config import load_or_create, set_replication_entry

    cfg_path = dbt_project / "dagsterization.yml"
    data = load_or_create(cfg_path)
    data["replication"]["enabled"] = True
    data["partitions"]["daily"] = ["orders"]
    set_replication_entry(
        data=data,
        model="orders",
        enabled=True,
        destination_table=None,
        destination_schema=None,
        write_disposition=None,
        partition_column="order_date",
    )
    _write_dagsterization(dbt_project, data)

    from dbt_dagsterizer.assets.replication.auto_config import build_auto_replication_specs

    with patch("dbt_dagsterizer.assets.replication.auto_config.load_manifest", return_value=_MANIFEST):
        specs = build_auto_replication_specs()
    assert len(specs) == 1
    assert specs[0]["partition_type"] == "daily"


def test_specs_raises_on_missing_model(dbt_project: Path):
    from dbt_dagsterizer.orchestration_config import load_or_create, set_replication_entry

    cfg_path = dbt_project / "dagsterization.yml"
    data = load_or_create(cfg_path)
    data["replication"]["enabled"] = True
    set_replication_entry(
        data=data,
        model="nonexistent_model",
        enabled=True,
        destination_table=None,
        destination_schema=None,
        write_disposition=None,
        partition_column=None,
    )
    _write_dagsterization(dbt_project, data)

    from dbt_dagsterizer.assets.replication.auto_config import build_auto_replication_specs

    with patch("dbt_dagsterizer.assets.replication.auto_config.load_manifest", return_value=_MANIFEST):
        with pytest.raises(ValueError, match="missing dbt model"):
            build_auto_replication_specs()


def test_job_specs_built_from_asset_specs(dbt_project: Path):
    from dbt_dagsterizer.orchestration_config import load_or_create, set_replication_entry

    cfg_path = dbt_project / "dagsterization.yml"
    data = load_or_create(cfg_path)
    data["replication"]["enabled"] = True
    set_replication_entry(
        data=data,
        model="orders",
        enabled=True,
        destination_table=None,
        destination_schema=None,
        write_disposition=None,
        partition_column=None,
    )
    _write_dagsterization(dbt_project, data)

    from dbt_dagsterizer.jobs.replication.auto_config import build_auto_replication_job_specs

    with patch("dbt_dagsterizer.assets.replication.auto_config.load_manifest", return_value=_MANIFEST):
        job_specs = build_auto_replication_job_specs()
    assert len(job_specs) == 1
    assert job_specs[0]["name"] == "replicate_orders_job"
    assert job_specs[0]["asset_key"] == "replicate_orders"
