import os
from itertools import batched

from dagster import define_asset_job

from ...k8s_tags import with_luban_run_k8s_config_tag


def get_observe_sources_jobs():
    from ...assets.dbt.assets import get_dbt_assets
    from ...assets.sources.automation import load_filtered_observable_sources
    from ...assets.sources.factory import build_observable_source_assets

    try:
        batch_size = int(os.getenv("LUBAN_OBSERVE_SOURCES_BATCH_SIZE", "5"))
    except ValueError:
        raise ValueError("LUBAN_OBSERVE_SOURCES_BATCH_SIZE must be a positive integer") from None
    if batch_size <= 0:
        raise ValueError("LUBAN_OBSERVE_SOURCES_BATCH_SIZE must be a positive integer")

    observable_source_assets = build_observable_source_assets(
        dbt_assets=get_dbt_assets(),
        source_specs=load_filtered_observable_sources(),
    )

    return [
        define_asset_job(
            name=(
                "observe_sources_job"
                if len(observable_source_assets) <= batch_size
                else f"observe_sources_job_{i}"
            ),
            selection=list(batch),
            tags=with_luban_run_k8s_config_tag(None),
        )
        for i, batch in enumerate(batched(observable_source_assets, batch_size), start=1)
    ]
