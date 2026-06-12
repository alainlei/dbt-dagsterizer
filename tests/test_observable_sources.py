from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import dagster as dg


def test_load_automation_observable_sources_supports_optional_watermark_sql(
    monkeypatch,
    tmp_path: Path,
):
    from dbt_dagsterizer.assets.sources import automation

    manifest = {
        "sources": {
            "source.demo.orders": {
                "source_name": "demo",
                "name": "orders",
                "meta": {
                    "luban": {
                        "observe": {
                            "watermark_column": "updated_at",
                        }
                    }
                },
            },
            "source.demo.customers": {
                "source_name": "demo",
                "name": "customers",
                "meta": {
                    "luban": {
                        "observe": {
                            "watermark_sql": "select max(updated_at) from demo.customers",
                        }
                    }
                },
            },
        }
    }

    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    monkeypatch.setattr(automation, "prepare_manifest_if_missing", lambda: None)
    monkeypatch.setattr(automation, "get_dbt_project_dir", lambda: tmp_path)

    assert automation.load_automation_observable_sources() == [
        {
            "source": "demo",
            "table": "customers",
            "watermark_column": None,
            "watermark_sql": "select max(updated_at) from demo.customers",
        },
        {
            "source": "demo",
            "table": "orders",
            "watermark_column": "updated_at",
            "watermark_sql": None,
        },
    ]


def test_build_observable_source_assets_accepts_legacy_specs_without_watermark_sql(
    monkeypatch,
):
    from dbt_dagsterizer.assets.sources import factory

    queries: list[str] = []
    decorator_kwargs: dict[str, object] = {}

    class FakeStarRocks:
        def query_scalar(self, sql: str) -> str:
            queries.append(sql)
            return "2026-06-10T00:00:00"

    def fake_observable_source_asset(**kwargs):
        decorator_kwargs.update(kwargs)

        def decorator(fn):
            return fn

        return decorator

    monkeypatch.setattr(
        factory,
        "get_asset_keys_by_output_name_for_source",
        lambda _dbt_assets_seq, _source: {"orders": dg.AssetKey(["demo", "orders"])},
    )
    monkeypatch.setattr(factory.dg, "observable_source_asset", fake_observable_source_asset)

    assets = factory.build_observable_source_assets(
        dbt_assets=None,
        source_specs=[
            {
                "source": "ods",
                "table": "orders",
                "watermark_column": "updated_at",
            }
        ],
    )

    result = assets[0](SimpleNamespace(resources=SimpleNamespace(starrocks=FakeStarRocks())))

    assert isinstance(result, dg.DataVersion)
    assert decorator_kwargs["group_name"] == "source"
    assert queries == ["select max(`updated_at`) from `ods`.`orders`"]


def test_build_observable_source_assets_uses_watermark_sql_when_provided(monkeypatch):
    from dbt_dagsterizer.assets.sources import factory

    queries: list[str] = []

    class FakeStarRocks:
        def query_scalar(self, sql: str) -> str:
            queries.append(sql)
            return "42"

    def fake_observable_source_asset(**_kwargs):
        def decorator(fn):
            return fn

        return decorator

    monkeypatch.setattr(
        factory,
        "get_asset_keys_by_output_name_for_source",
        lambda _dbt_assets_seq, _source: {"orders": dg.AssetKey(["demo", "orders"])},
    )
    monkeypatch.setattr(factory.dg, "observable_source_asset", fake_observable_source_asset)

    assets = factory.build_observable_source_assets(
        dbt_assets=None,
        source_specs=[
            {
                "source": "ods",
                "table": "orders",
                "watermark_sql": "select max(updated_at) from custom.orders_view",
            }
        ],
    )

    result = assets[0](SimpleNamespace(resources=SimpleNamespace(starrocks=FakeStarRocks())))

    assert isinstance(result, dg.DataVersion)
    assert queries == ["select max(updated_at) from custom.orders_view"]
