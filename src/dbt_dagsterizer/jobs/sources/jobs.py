from dagster import define_asset_job

from ...k8s_tags import with_luban_run_k8s_config_tag


def get_observe_sources_job():
    from ...assets.dbt.assets import get_dbt_assets
    from ...assets.sources.automation import load_filtered_observable_sources
    from ...assets.sources.factory import build_observable_source_assets

    observable_source_assets = build_observable_source_assets(
        dbt_assets=get_dbt_assets(),
        source_specs=load_filtered_observable_sources(),
    )

    if not observable_source_assets:
        return None

    return define_asset_job(
        name="observe_sources_job",
        selection=observable_source_assets,
        tags=with_luban_run_k8s_config_tag(None),
    )
