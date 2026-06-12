from __future__ import annotations

import os

import dagster as dg
from dagster_dbt import get_asset_keys_by_output_name_for_source


def _quoted_identifier(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def build_observable_source_assets(
    *,
    dbt_assets,
    source_specs: list[dict],
    source_db_env_var_map: dict[str, str] = None,
    source_db_default_map: dict[str, str] = None,
):
    source_db_env_var_map = source_db_env_var_map or {"ods": "STARROCKS_ODS_DB"}
    source_db_default_map = source_db_default_map or {}

    dbt_assets_seq = dbt_assets if isinstance(dbt_assets, list) else [dbt_assets]

    source_names = {spec["source"] for spec in source_specs}
    keys_by_source_table = {}
    for source in source_names:
        keys_by_table = get_asset_keys_by_output_name_for_source(dbt_assets_seq, source)
        keys_by_source_table[source] = keys_by_table

    def resolve_source_asset_key(source_name: str, table_name: str) -> dg.AssetKey:
        keys_by_table = keys_by_source_table[source_name]
        if table_name in keys_by_table:
            return keys_by_table[table_name]

        suffix = f"_{source_name}_{table_name}".replace("-", "_").replace("*", "_star")
        matches = [key for key in keys_by_table.keys() if key.endswith(suffix)]
        if len(matches) == 1:
            return keys_by_table[matches[0]]

        raise KeyError(
            f"Could not resolve dbt source asset key for source={source_name} table={table_name}. "
            f"Available output names: {sorted(keys_by_table.keys())}"
        )

    def make_asset(spec: dict):
        source_name = spec["source"]
        table_name = spec["table"]
        watermark_column = spec.get("watermark_column")
        watermark_sql = spec.get("watermark_sql")

        if not watermark_column and not watermark_sql:
            raise ValueError(
                f"Observable source spec for {source_name}.{table_name} requires "
                "either watermark_column or watermark_sql."
            )

        db_env_var = source_db_env_var_map.get(source_name, f"STARROCKS_{source_name.upper()}_DB")
        db_default = source_db_default_map.get(source_name)

        if db_default is None:
            if source_name == "ods":
                db_default = "ods"
            else:
                db_default = source_name

        @dg.observable_source_asset(
            key=resolve_source_asset_key(source_name, table_name),
            group_name="source",
            required_resource_keys={"starrocks"},
        )
        def _observable(context) -> dg.DataVersion:
            db_name = os.getenv(db_env_var, db_default)
            value = context.resources.starrocks.query_scalar(
                str(watermark_sql)
                if watermark_sql
                else (
                    f"select max({_quoted_identifier(str(watermark_column))}) "
                    f"from {_quoted_identifier(db_name)}.{_quoted_identifier(table_name)}"
                )
            )
            return dg.DataVersion(str(value) if value is not None else "")

        return _observable

    return [make_asset(spec) for spec in source_specs]
