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
            "name": "customers",
            "watermark_column": None,
            "watermark_sql": "select max(updated_at) from demo.customers",
            "group": None,
        },
        {
            "source": "demo",
            "table": "orders",
            "name": "orders",
            "watermark_column": "updated_at",
            "watermark_sql": None,
            "group": None,
        },
    ]


def test_load_automation_observable_sources_reads_custom_group(
    monkeypatch,
    tmp_path: Path,
):
    from dbt_dagsterizer.assets.sources import automation

    manifest = {
        "sources": {
            "source.demo.orders": {
                "source_name": "demo",
                "name": "orders",
                # Table-level meta wins over source-level meta.
                "meta": {
                    "luban": {
                        "group": "demo_orders",
                        "observe": {
                            "watermark_column": "updated_at",
                        },
                    }
                },
                "source_meta": {
                    "luban": {
                        "group": "demo_sources",
                    }
                },
            },
            "source.demo.customers": {
                "source_name": "demo",
                "name": "customers",
                # No table-level group: falls back to the source-level meta.
                "meta": {
                    "luban": {
                        "observe": {
                            "watermark_sql": "select max(updated_at) from demo.customers",
                        }
                    }
                },
                "source_meta": {
                    "luban": {
                        "group": "demo_sources",
                    }
                },
            },
            "source.demo.products": {
                "source_name": "demo",
                "name": "products",
                # Legacy placement under observe is still accepted.
                "meta": {
                    "luban": {
                        "observe": {
                            "watermark_column": "updated_at",
                            "group": "demo_products",
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
            "name": "customers",
            "watermark_column": None,
            "watermark_sql": "select max(updated_at) from demo.customers",
            "group": "demo_sources",
        },
        {
            "source": "demo",
            "table": "orders",
            "name": "orders",
            "watermark_column": "updated_at",
            "watermark_sql": None,
            "group": "demo_orders",
        },
        {
            "source": "demo",
            "table": "products",
            "name": "products",
            "watermark_column": "updated_at",
            "watermark_sql": None,
            "group": "demo_products",
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


def test_build_observable_source_assets_uses_custom_group_when_provided(monkeypatch):
    from dbt_dagsterizer.assets.sources import factory

    decorator_kwargs: dict[str, object] = {}

    class FakeStarRocks:
        def query_scalar(self, sql: str) -> str:
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
                "group": "ods_orders",
            }
        ],
    )

    result = assets[0](SimpleNamespace(resources=SimpleNamespace(starrocks=FakeStarRocks())))

    assert isinstance(result, dg.DataVersion)
    assert decorator_kwargs["group_name"] == "ods_orders"


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


def test_build_observable_source_assets_quotes_each_part_of_dotted_identifiers(monkeypatch):
    from dbt_dagsterizer.assets.sources import factory

    queries: list[str] = []

    class FakeStarRocks:
        def query_scalar(self, sql: str) -> str:
            queries.append(sql)
            return "2026-06-10T00:00:00"

    def fake_observable_source_asset(**_kwargs):
        def decorator(fn):
            return fn

        return decorator

    monkeypatch.setattr(
        factory,
        "get_asset_keys_by_output_name_for_source",
        lambda _dbt_assets_seq, _source: {"external.orders": dg.AssetKey(["demo", "orders"])},
    )
    monkeypatch.setattr(factory.dg, "observable_source_asset", fake_observable_source_asset)

    assets = factory.build_observable_source_assets(
        dbt_assets=None,
        source_specs=[
            {
                "source": "ods",
                "table": "external.orders",
                "watermark_column": "updated.at",
            }
        ],
        source_db_default_map={"ods": "catalog.db"},
    )

    result = assets[0](SimpleNamespace(resources=SimpleNamespace(starrocks=FakeStarRocks())))

    assert isinstance(result, dg.DataVersion)
    assert queries == ["select max(`updated`.`at`) from `catalog`.`db`.`external`.`orders`"]


def test_load_automation_observable_sources_identifier_differs_from_name(
    monkeypatch,
    tmp_path: Path,
):
    """When a dbt source uses `identifier` that differs from `name`, both
    values must be captured: `name` for Dagster asset-key resolution and
    `identifier` (as `table`) for SQL queries."""
    from dbt_dagsterizer.assets.sources import automation

    manifest = {
        "sources": {
            "source.mssqlserver.testing": {
                "source_name": "mssqlserver",
                "name": "testing",
                "identifier": "Test",
                "meta": {
                    "luban": {
                        "observe": {
                            "watermark_column": "updated_at",
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
            "source": "mssqlserver",
            "table": "Test",
            "name": "testing",
            "watermark_column": "updated_at",
            "watermark_sql": None,
            "group": None,
        },
    ]


def test_build_observable_source_assets_resolves_key_via_dbt_name(monkeypatch):
    """When `name` differs from `table` (identifier), the factory must use
    `name` for Dagster asset-key resolution and `table` for the SQL query."""
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

    # dagster_dbt generates output names from the dbt `name` field, so the
    # output name uses "testing" even though the identifier is "Test".
    monkeypatch.setattr(
        factory,
        "get_asset_keys_by_output_name_for_source",
        lambda _dbt_assets_seq, _source: {
            "source_test_dbt_mssqlserver_testing": dg.AssetKey(["mssqlserver", "testing"])
        },
    )
    monkeypatch.setattr(factory.dg, "observable_source_asset", fake_observable_source_asset)

    assets = factory.build_observable_source_assets(
        dbt_assets=None,
        source_specs=[
            {
                "source": "mssqlserver",
                "table": "Test",
                "name": "testing",
                "watermark_column": "updated_at",
            }
        ],
    )

    result = assets[0](SimpleNamespace(resources=SimpleNamespace(starrocks=FakeStarRocks())))

    assert isinstance(result, dg.DataVersion)
    # Key resolution used the dbt name "testing", not the identifier "Test"
    assert decorator_kwargs["key"] == dg.AssetKey(["mssqlserver", "testing"])
    # SQL query uses the identifier "Test" for the actual table reference
    assert queries == ["select max(`updated_at`) from `mssqlserver`.`Test`"]
