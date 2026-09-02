"""Build Dagster ``define_asset_job`` definitions for replication entries."""
from __future__ import annotations

from dagster import AssetKey, AssetSelection, define_asset_job

from ...k8s_tags import with_luban_run_k8s_config_tag


def build_replication_jobs(job_specs: list[dict]) -> list:
    """Build ``define_asset_job`` for each replication spec."""
    if not job_specs:
        return []

    jobs: list = []
    for spec in job_specs:
        job_name = spec["name"]
        asset_key_str = spec["asset_key"]
        selection = AssetSelection.assets(AssetKey(["replication", asset_key_str]))

        jobs.append(
            define_asset_job(
                name=job_name,
                selection=selection,
                tags=with_luban_run_k8s_config_tag(None),
            )
        )

    return jobs
