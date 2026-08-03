"""Build Dagster ``@asset`` definitions for replication entries.

Each replication asset depends on the corresponding dbt model asset (via
``deps=[AssetKey(...)]``) and uses the same partition definition as the dbt
model so that partition-aware replication works correctly.

Per-partition triggering is handled by a dedicated replication sensor (see
``sensors/replication/``) rather than ``AutomationCondition.eager()`` with
``ins``, because ``@dbt_assets`` produce materialization events (not file
outputs) which are incompatible with ``AssetIn``-based data dependencies.
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
from ...resources.dbt import get_dbt_project_dir
from ...resources.mssql import make_mssql_resource
from ...resources.starrocks import make_starrocks_resource
from .executor import execute_replication


def _resolve_include_current_day() -> bool:
    """Load orchestration config and return ``include_current_day_partition``."""
    dbt_project_dir = get_dbt_project_dir()
    cfg_path = resolve_orchestration_path(
        dbt_project_dir=dbt_project_dir,
        path_=Path(default_orchestration_path(dbt_project_dir=dbt_project_dir).name),
    )
    cfg = load_orch(cfg_path)
    idx = index_orch(cfg)
    return idx.daily_include_current_day_partition


def build_replication_assets(specs: list[dict]) -> list[dg.AssetsDefinition]:
    """Build Dagster ``@asset`` definitions from replication specs.

    Each asset:
    - Depends on the dbt model asset via ``deps=[AssetKey(source_relation)]``
    - Uses the same ``partitions_def`` as the dbt model (daily / None)
    - Executes ``execute_replication`` when materialized
    """
    if not specs:
        return []

    include_current_day = _resolve_include_current_day()

    assets: list[dg.AssetsDefinition] = []
    for spec in specs:
        partition_type = spec.get("partition_type", "unpartitioned")
        partitions_def = get_partitions_def(
            partition_type,
            include_current_day_partition=include_current_day,
        )

        source_relation = spec["source_relation"]
        dep_key = dg.AssetKey(source_relation)

        def _make_asset(
            _spec: dict,
            _partitions_def: dg.PartitionsDefinition | None,
            _dep_key: dg.AssetKey,
        ):
            @dg.asset(
                name=_spec["name"],
                key_prefix="replication",
                deps=[_dep_key],
                partitions_def=_partitions_def,
                group_name="replication",
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
