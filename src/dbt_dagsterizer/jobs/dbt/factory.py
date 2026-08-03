import json

import dagster as dg
from dagster import AssetKey, AssetSelection, define_asset_job

from ...assets.dbt.vars import _get_dbt_vars_for_context
from ...k8s_tags import with_luban_run_k8s_config_tag
from ...partitions import get_partitions_def
from ...resources.dbt import get_dbt_project_dir


def _get_partitions_def(
    partitions: str | None,
    include_current_day_partition: bool | None = None,
):
    """Convert partition spec to PartitionsDefinition.
    
    Args:
        partitions: Partition specification ("daily", "unpartitioned", None, etc.)
    
    Returns:
        PartitionsDefinition or None
    
    Raises:
        ValueError: If partition spec is invalid
    """
    return get_partitions_def(partitions, include_current_day_partition=include_current_day_partition)


def _build_selection(selection_spec):
    selection_type = selection_spec.get("type")
    if selection_type == "key_prefix":
        prefix = selection_spec["prefix"]
        return AssetSelection.key_prefixes(prefix)

    if selection_type == "asset_keys":
        keys = [AssetKey(path) for path in selection_spec["keys"]]
        selection = AssetSelection.assets(*keys)
        if selection_spec.get("upstream"):
            selection = selection.upstream()
        return selection

    raise ValueError(f"Unsupported selection type: {selection_type}")


def _sanitized_name(name: str) -> str:
    return "".join([c if (c.isalnum() or c == "_") else "_" for c in name])


def _build_dbt_cli_job(job_spec, include_current_day_partition=None):
    job_name = job_spec["name"]
    command = job_spec.get("command", "build")
    select = job_spec["select"]
    vars_dict = job_spec.get("vars") or {}
    partitions = job_spec.get("partitions", "daily")
    partitions_def = _get_partitions_def(partitions, include_current_day_partition=include_current_day_partition)
    op_name = _sanitized_name(f"run_{job_name}")

    @dg.op(name=op_name, required_resource_keys={"dbt"})
    def _run(context):
        partition_vars = _get_dbt_vars_for_context(context) or {}
        combined_vars = {**partition_vars, **vars_dict}
        args = [command, "--select", select]
        if combined_vars:
            args += ["--vars", json.dumps(combined_vars)]
        target_path = get_dbt_project_dir() / "target"
        invocation = context.resources.dbt.cli(args, context=context, target_path=target_path)
        for event in invocation.stream_raw_events():
            context.log.info(str(event))
        invocation.wait()

    @dg.job(
        name=job_name,
        partitions_def=partitions_def,
        tags=with_luban_run_k8s_config_tag(job_spec.get("tags")),
    )
    def _job():
        _run()

    return _job


def build_dbt_asset_jobs(
    job_specs,
    include_current_day_partition: bool | None = None,
):
    duplicated = set()
    seen = set()
    for spec in job_specs:
        name = spec.get("name")
        if not name:
            continue
        if name in seen:
            duplicated.add(name)
        else:
            seen.add(name)
    if duplicated:
        raise ValueError(f"Duplicate dbt job names: {sorted(duplicated)}")

    jobs_by_name = {}
    for job_spec in job_specs:
        job_type = job_spec.get("type", "asset")
        name = job_spec["name"]
        if job_type == "asset":
            selection = _build_selection(job_spec["selection"])
            partitions = job_spec.get("partitions")
            # Get the partition definition for this job
            # 
            # IMPORTANT: Both assets AND jobs need partitions_def in Dagster:
            # - Assets have partitions_def for UI visibility
            # - Jobs have partitions_def for sensors to emit RunRequests
            # The job_spec.partitions comes from auto_config which infers partition type
            # from the models in the job. Use it if available.
            partitions_def = _get_partitions_def(partitions, include_current_day_partition=include_current_day_partition)
            jobs_by_name[name] = define_asset_job(
                name=name,
                selection=selection,
                partitions_def=partitions_def,
                tags=with_luban_run_k8s_config_tag(job_spec.get("tags")),
            )
        elif job_type == "dbt_cli":
            jobs_by_name[name] = _build_dbt_cli_job(job_spec, include_current_day_partition=include_current_day_partition)
        else:
            raise ValueError(f"Unsupported job type: {job_type}")

    return jobs_by_name
