"""Build Dagster ``@asset`` definitions for replication entries.

Each replication asset depends on the corresponding dbt model asset (via
``deps=[AssetKey(...)]``) and uses the same partition definition as the dbt
model so that partition-aware replication works correctly.
"""
from __future__ import annotations

from pathlib import Path

import dagster as dg

from ...orchestration_config import (
    default_orchestration_path,
    resolve_orchestration_path,
)
from ...orchestration_config import (
    index as index_orch,
)
from ...orchestration_config import (
    load_or_create as load_orch,
)
from ...partitions import get_partitions_def
from ...partitions_registry import get_dynamic_partitions_defs
from ...resources.dbt import get_dbt_project_dir
from ...resources.mssql import make_mssql_resource
from ...resources.starrocks import make_starrocks_resource
from .executor import execute_replication


def _resolve_partitions_defs() -> tuple[dg.PartitionsDefinition | None, dict[str, dg.PartitionsDefinition], bool]:
    """Load partition definitions from orchestration config.

    Returns ``(daily_partitions_def, dynamic_partitions_defs, include_current_day_partition)``.
    """
    dbt_project_dir = get_dbt_project_dir()
    cfg_path = resolve_orchestration_path(
        dbt_project_dir=dbt_project_dir,
        path_=Path(default_orchestration_path(dbt_project_dir=dbt_project_dir).name),
    )
    cfg = load_orch(cfg_path)
    idx = index_orch(cfg)
    dynamic_partitions_defs = get_dynamic_partitions_defs(dbt_project_dir)
    daily_partitions_def = get_partitions_def(
        "daily",
        dynamic_partitions_defs=dynamic_partitions_defs,
        include_current_day_partition=idx.daily_include_current_day_partition,
    )
    return daily_partitions_def, dynamic_partitions_defs, idx.daily_include_current_day_partition


def build_replication_assets(specs: list[dict]) -> list[dg.AssetsDefinition]:
    """Build Dagster ``@asset`` definitions from replication specs.

    Each asset:
    - Depends on the dbt model asset via ``deps=[AssetKey(source_relation)]``
    - Uses the same ``partitions_def`` as the dbt model (daily / dynamic / None)
    - Executes ``execute_replication`` when materialized
    """
    if not specs:
        return []

    daily_partitions_def, dynamic_partitions_defs, include_current_day = _resolve_partitions_defs()
    # daily_partitions_def is resolved per-spec via get_partitions_def() which caches internally
    _ = daily_partitions_def

    assets: list[dg.AssetsDefinition] = []
    for spec in specs:
        partition_type = spec.get("partition_type", "unpartitioned")
        partitions_def = get_partitions_def(
            partition_type,
            dynamic_partitions_defs=dynamic_partitions_defs,
            include_current_day_partition=include_current_day,
        )

        source_relation = spec["source_relation"]
        dep_key = dg.AssetKey(source_relation)

        def _make_asset(_spec: dict, _partitions_def: dg.PartitionsDefinition | None, _dep_key: dg.AssetKey):
            @dg.asset(
                name=_spec["name"],
                deps=[_dep_key],
                partitions_def=_partitions_def,
                group_name="replication",
                automation_condition=dg.AutomationCondition.eager(),
            )
            def _replication_asset(context):
                execute_replication(
                    context=context,
                    spec=_spec,
                    starrocks_client=make_starrocks_resource(),
                    mssql_client=make_mssql_resource(),
                )

            return _replication_asset

        asset = _make_asset(spec, partitions_def, dep_key)
        assets.append(asset)

    return assets
