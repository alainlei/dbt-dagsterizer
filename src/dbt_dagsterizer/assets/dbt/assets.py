import json
import os
from pathlib import Path

import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets
from dagster_dbt.errors import DagsterDbtCliRuntimeError

from ...dbt.row_counts import get_row_counts_from_starrocks
from ...dbt.run_results import (
    add_run_results_telemetry,
    get_row_counts_by_unique_id,
    parse_run_results
)
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
from ...otel import (
    otel_dagster_transaction_info,
    otel_record_exception,
    otel_span,
    otel_transaction_span,
)
from ...partitions import get_daily_partitions_def
from ...partitions_registry import get_dynamic_partitions_defs
from ...resources.dbt import get_dbt_project_dir
from ...resources.starrocks import make_starrocks_resource
from ..sources.automation import load_automation_observable_sources
from .prepare import prepare_manifest_if_missing
from .translator import LubanDagsterDbtTranslator
from .vars import _get_dbt_vars_for_context

_dbt_assets_def = None


def _emit_partition_row_counts(
    context,
    dbt_project_dir: Path,
    translator,
    run_results_json
):
    """Emit AssetObservation events with row count metadata for partitioned assets.
    
    Primary source: Query StarRocks directly for accurate row counts
    Fallback source: Extract from dbt run_results.json adapter_response
    """
    manifest_path = dbt_project_dir / "target" / "manifest.json"

    # partition_row_counts = {}
    affected_row_counts = {}
    
    # Get affected row counts from run_results_json
    try:
        run_results = parse_run_results(run_results_json, True)
    except Exception as e:
        context.log.warning(f"Row counts: Failed to load run_results_json: {e}")
        return
    
    affected_row_counts = get_row_counts_by_unique_id(run_results)
    if not affected_row_counts:
        context.log.warning("Row counts: No rows_affected found in run_results_json")
        if run_results:
            context.log.info(f"Row counts: Sample result keys - {list(run_results[0].__dict__.keys()) if hasattr(run_results[0], '__dict__') else 'N/A'}")
        return
    
    # Get partition information
    partition_key = None
    try:
        partition_key = context.partition_key
    except Exception:
        context.log.info("Row counts: No partition_key (non-partitioned run)")
        pass
    
    # Load manifest to get resource props for each node (if not already loaded)
    if not manifest_path.exists():
        context.log.warning(f"Row counts: manifest.json not found at {manifest_path}")
        return
    
    try:
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)
    except Exception as e:
        context.log.warning(f"Row counts: Failed to load manifest.json: {e}")
        return
    
    nodes = manifest.get("nodes", {})
    
    # Emit observations for each asset with row count
    for unique_id, affected_row_count in affected_row_counts.items():
        node = nodes.get(unique_id)
        total_row_count = -1

        if not node:
            context.log.warning(f"Row counts: Node {unique_id} not found in manifest")
            continue
        
        # Build AssetKey using the translator
        try:
            asset_key = translator.get_asset_key(node)
        except Exception as e:
            context.log.warning(f"Row counts: Failed to build AssetKey for {unique_id}: {e}")
            continue
        
        # Build metadata with row count
        metadata = {
            "last_run_affected_row_count": dg.MetadataValue.int(affected_row_count),
        }

        # Query StarRocks for the total table row count
        try:
            context.log.info("Row counts: Attempting to query StarRocks for row counts...")
            starrocks_client = make_starrocks_resource()
            total_row_count = get_row_counts_from_starrocks(
                client=starrocks_client,
                node=node
            )
        except Exception as e:
            context.log.warning(f"Row counts: Failed to query StarRocks: {e}")
            
        # Emit AssetObservation event with metadata
        yield dg.AssetObservation(
            asset_key=asset_key,
            metadata=metadata,
            partition=partition_key,
        )

        if total_row_count >= 0:
            metadata_total = {
                "dagster/row_count": dg.MetadataValue.int(total_row_count),
            }

            yield dg.AssetObservation(
                asset_key=asset_key,
                metadata=metadata_total,
            )
        

def _should_retry_dbt_cli_error(*, message: str, retry_number: int) -> bool:
    message = (message or "").lower()
    return "already exists" in message and retry_number < 1


def get_dbt_assets():
    global _dbt_assets_def
    if _dbt_assets_def is not None:
        return _dbt_assets_def

    dbt_project_dir = get_dbt_project_dir()
    dbt_target = os.getenv("DBT_TARGET") or os.getenv("LUBAN_DEFAULT_DBT_TARGET") or "development"
    dbt_project = DbtProject(
        project_dir=dbt_project_dir,
        target=dbt_target,
        packaged_project_dir=dbt_project_dir,
    )

    prepare_manifest_if_missing()
    automation_observable_tables = {
        spec["table"] for spec in load_automation_observable_sources() if spec.get("table")
    }

    orch_cfg_path = resolve_orchestration_path(
        dbt_project_dir=dbt_project_dir,
        path_=Path(default_orchestration_path(dbt_project_dir=dbt_project_dir).name),
    )
    orch_cfg = load_orch(orch_cfg_path)
    orch_index = index_orch(orch_cfg)
    
    # Get dynamic partition definitions from registry
    dynamic_partitions_defs = get_dynamic_partitions_defs(dbt_project_dir)
    
    # Get daily partitions definition (required for asset-level partitions)
    daily_partitions_def = get_daily_partitions_def()

    translator = LubanDagsterDbtTranslator(
        daily_partitions_def=daily_partitions_def,
        dynamic_partitions_defs=dynamic_partitions_defs,
        automation_observable_tables=automation_observable_tables,
        partitions_by_model=orch_index.partitions_by_model,
    )

    @dbt_assets(
        manifest=dbt_project.manifest_path,
        dagster_dbt_translator=translator,
    )
    def _dbt_assets(context, dbt: DbtCliResource):
        retry_number = int(getattr(context, "retry_number", 0) or 0)
        tx_span_name, _, tx_attrs = otel_dagster_transaction_info(context)
        tx_attrs["dagster.retry_number"] = retry_number
        with otel_transaction_span(tx_span_name, attributes=tx_attrs):
            with otel_span("dbt.vars"):
                dbt_vars = _get_dbt_vars_for_context(context)
            dbt_args = ["build"]
            if dbt_vars:
                dbt_args += ["--vars", json.dumps(dbt_vars)]
            with otel_span(
                "dbt.cli",
                attributes={
                    "dbt.command": dbt_args[0] if dbt_args else "",
                    "dbt.target": dbt_target,
                },
            ) as span:
                try:
                    # Process dbt stream - this yields proper Dagster events
                    dbt_invocation = dbt.cli(dbt_args, context=context)
                    yield from dbt_invocation.stream()
                except DagsterDbtCliRuntimeError as e:
                    otel_record_exception(span, e)
                    if _should_retry_dbt_cli_error(message=str(e), retry_number=retry_number):
                        context.log.warning(
                            "dbt invocation failed with 'already exists'; requesting a retry"
                        )
                        raise dg.RetryRequested(seconds_to_wait=2) from e
                    raise
                
                # After dbt stream completes, emit row count metadata
                # Add run results telemetry
                add_run_results_telemetry(
                    run_results_path=dbt_project_dir / "target" / "run_results.json",
                    manifest_path=dbt_project_dir / "target" / "manifest.json",
                    parent_span_attributes={
                        "dbt.target": dbt_target,
                    },
                )
                
                # Emit row count observations with partition context
                yield from _emit_partition_row_counts(
                    context=context,
                    dbt_project_dir=dbt_project_dir,
                    translator=translator,
                    run_results_json=dbt_invocation.get_artifact('run_results.json')
                )

    _dbt_assets_def = _dbt_assets
    return _dbt_assets_def
