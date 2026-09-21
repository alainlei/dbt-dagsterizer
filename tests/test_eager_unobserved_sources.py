from __future__ import annotations

import json
from pathlib import Path

import dagster as dg

TEST_KEY = dg.AssetKey(["dbt", "sqlserver_catalog", "dbo", "Test"])
TEST2_KEY = dg.AssetKey(["dbt", "sqlserver_catalog", "dbo", "Test2"])
EXT_KEY = dg.AssetKey(["dbt", "sqlserver_catalog", "dbo", "ext"])


def _manifest_project(tmp_path: Path, manifest: dict) -> Path:
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return tmp_path


def _source_manifest() -> dict:
    return {
        "sources": {
            "source.mssqlserver.testing": {
                "source_name": "mssqlserver",
                "name": "testing",
                "identifier": "Test",
                "database": "sqlserver_catalog",
                "schema": "dbo",
                "meta": {"luban": {"observe": {"watermark_column": "updated_at"}}},
                "source_meta": {},
            },
            "source.mssqlserver.testing2": {
                "source_name": "mssqlserver",
                "name": "testing2",
                "identifier": "Test2",
                "database": "sqlserver_catalog",
                "schema": "dbo",
                "meta": {},
                "source_meta": {"luban": {"group": "testing"}},
            },
            "source.ext_db.mkt_ext": {
                "source_name": "ext_db",
                "name": "mkt_ext",
                "identifier": "mkt_ext",
                "database": "ext",
                "schema": "mkt",
                "meta": {"luban": {"external_code_location": True}},
                "source_meta": {},
            },
        }
    }


def _condition_labels(condition) -> set[str]:
    labels: set[str] = set()
    stack = [condition]
    while stack:
        current = stack.pop()
        label = current.get_label()
        if label:
            labels.add(label)
        stack.extend(current.children)
    return labels


def test_load_unobserved_source_key_paths_classifies_sources(monkeypatch, tmp_path: Path):
    """Only sources that can never record events are returned: no observe metadata
    and not external. Observable and external_code_location sources are excluded."""
    from dbt_dagsterizer.assets.sources import automation

    _manifest_project(tmp_path, _source_manifest())
    monkeypatch.setattr(automation, "prepare_manifest_if_missing", lambda: None)
    monkeypatch.setattr(automation, "get_dbt_project_dir", lambda: tmp_path)

    assert automation.load_unobserved_source_key_paths() == [
        ["dbt", "sqlserver_catalog", "dbo", "Test2"],
    ]


def test_load_unobserved_source_key_paths_empty_without_sources(monkeypatch, tmp_path: Path):
    from dbt_dagsterizer.assets.sources import automation

    _manifest_project(tmp_path, {"sources": {}})
    monkeypatch.setattr(automation, "prepare_manifest_if_missing", lambda: None)
    monkeypatch.setattr(automation, "get_dbt_project_dir", lambda: tmp_path)

    assert automation.load_unobserved_source_key_paths() == []
    assert automation.load_unobserved_source_specs() == []


def test_load_unobserved_source_specs_include_key_and_group(monkeypatch, tmp_path: Path):
    """Unobserved source specs carry the translator-identical relation key plus the
    group from meta.luban.group (table-level first, then source-level fallback)."""
    from dbt_dagsterizer.assets.dbt.translator import LubanDagsterDbtTranslator
    from dbt_dagsterizer.assets.sources import automation

    manifest = _source_manifest()
    _manifest_project(tmp_path, manifest)
    monkeypatch.setattr(automation, "prepare_manifest_if_missing", lambda: None)
    monkeypatch.setattr(automation, "get_dbt_project_dir", lambda: tmp_path)

    specs = automation.load_unobserved_source_specs()
    assert specs == [
        {
            "source": "mssqlserver",
            "table": "Test2",
            "name": "testing2",
            "key_path": ["dbt", "sqlserver_catalog", "dbo", "Test2"],
            "group": "testing",
        }
    ]

    translator = LubanDagsterDbtTranslator(daily_partitions_def=None)
    assert (
        translator.get_asset_key(manifest["sources"]["source.mssqlserver.testing2"])
        == dg.AssetKey(specs[0]["key_path"])
    )


def test_load_unobserved_source_specs_skip_shadowed_relations(monkeypatch, tmp_path: Path):
    """A source whose relation is already produced by a model/seed/snapshot is skipped
    so the lineage-only spec cannot collide with the real asset definition."""
    from dbt_dagsterizer.assets.sources import automation

    manifest = _source_manifest()
    manifest["nodes"] = {
        "model.demo.test2_shadow": {
            "resource_type": "model",
            "name": "test2_shadow",
            "database": "sqlserver_catalog",
            "schema": "dbo",
            "identifier": "Test2",
            "config": {"materialized": "table"},
        }
    }
    _manifest_project(tmp_path, manifest)
    monkeypatch.setattr(automation, "prepare_manifest_if_missing", lambda: None)
    monkeypatch.setattr(automation, "get_dbt_project_dir", lambda: tmp_path)

    assert automation.load_unobserved_source_specs() == []
    assert automation.load_unobserved_source_key_paths() == []


def test_build_unobserved_source_assets_are_lineage_only(monkeypatch, tmp_path: Path):
    """The built assets define the unobserved key, grouped with the other sources and
    without an automation condition, and coexist with a dependent model."""
    from dbt_dagsterizer.assets.sources import automation
    from dbt_dagsterizer.assets.sources.factory import build_unobserved_source_assets

    _manifest_project(tmp_path, _source_manifest())
    monkeypatch.setattr(automation, "prepare_manifest_if_missing", lambda: None)
    monkeypatch.setattr(automation, "get_dbt_project_dir", lambda: tmp_path)

    assets = build_unobserved_source_assets(
        specs=automation.load_unobserved_source_specs()
    )
    assert len(assets) == 1

    source_specs = list(assets[0].specs)
    assert [spec.key for spec in source_specs] == [TEST2_KEY]
    assert source_specs[0].group_name == "testing"
    assert source_specs[0].automation_condition is None

    @dg.asset(key=EXT_KEY, deps=[TEST_KEY, TEST2_KEY])
    def ext(): ...

    defs = dg.Definitions(assets=[ext, *assets])
    node = defs.resolve_asset_graph().get(TEST2_KEY)
    assert node.group_name == "testing"


def test_build_unobserved_source_assets_empty_specs():
    from dbt_dagsterizer.assets.sources.factory import build_unobserved_source_assets

    assert build_unobserved_source_assets(specs=[]) == []


def test_translator_eager_excludes_unobserved_source_keys():
    """With unobserved source keys configured the missing-gate is scoped via
    .ignore(...); without them the stock eager() is returned unchanged."""
    from dbt_dagsterizer.assets.dbt.translator import LubanDagsterDbtTranslator

    props = {"resource_type": "model", "name": "ext", "tags": ["automation_table"]}

    default_condition = LubanDagsterDbtTranslator(
        daily_partitions_def=None
    ).get_automation_condition(props)
    default_labels = _condition_labels(default_condition)
    assert "any_deps_missing" in default_labels
    assert "any_deps_missing_ignoring_unobserved_sources" not in default_labels

    relaxed_condition = LubanDagsterDbtTranslator(
        daily_partitions_def=None,
        unobserved_source_keys=[TEST2_KEY],
    ).get_automation_condition(props)
    relaxed_labels = _condition_labels(relaxed_condition)
    assert "any_deps_missing_ignoring_unobserved_sources" in relaxed_labels
    assert "any_deps_missing" not in relaxed_labels


def _run_two_ticks_with_observation(unobserved_source_keys) -> tuple[int, int]:
    from dagster._core.definitions.declarative_automation.automation_condition_tester import (
        evaluate_automation_conditions,
    )

    from dbt_dagsterizer.assets.dbt.translator import LubanDagsterDbtTranslator

    condition = LubanDagsterDbtTranslator(
        daily_partitions_def=None,
        unobserved_source_keys=unobserved_source_keys,
    ).get_automation_condition(
        {"resource_type": "model", "name": "ext", "tags": ["automation_table"]}
    )
    assert condition is not None

    watermark = {"value": "2026-01-01T00:00:00"}

    @dg.observable_source_asset(key=TEST_KEY)
    def source_test() -> dg.DataVersion:
        return dg.DataVersion(watermark["value"])

    @dg.asset(key=EXT_KEY, deps=[TEST_KEY, TEST2_KEY], automation_condition=condition)
    def ext(): ...

    defs = dg.Definitions(
        assets=[
            source_test,
            ext,
            # Post-change production graph: unobserved sources carry a lineage-only
            # spec definition, which must not change the automation outcome.
            dg.AssetsDefinition(specs=[dg.AssetSpec(key=TEST2_KEY)]),
        ],
        jobs=[dg.define_asset_job("observe_test", selection=[source_test])],
    )
    instance = dg.DagsterInstance.ephemeral()

    first = evaluate_automation_conditions(defs=defs, instance=instance)
    defs.resolve_job_def("observe_test").execute_in_process(instance=instance)
    second = evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=first.cursor
    )
    return first.total_requested, second.total_requested


def test_automation_table_materializes_when_sibling_source_is_unobserved():
    """Observing the fact source requests `ext` even though `Test2` has no event
    records at all (no observe metadata)."""
    assert _run_two_ticks_with_observation(
        unobserved_source_keys=[TEST2_KEY]
    ) == (0, 1)


def test_automation_table_stays_blocked_without_unobserved_source_exclusion():
    """Documents the pre-fix behavior: stock eager() never fires while a sibling
    source key is (and stays) missing."""
    assert _run_two_ticks_with_observation(unobserved_source_keys=None) == (0, 0)
