def get_assets():
    from .dbt.assets import get_dbt_assets
    from .replication import get_replication_assets
    from .sources.automation import load_automation_observable_sources
    from .sources.factory import build_observable_source_assets

    dbt_assets = get_dbt_assets()
    observable_source_assets = build_observable_source_assets(
        dbt_assets=dbt_assets,
        source_specs=load_automation_observable_sources(),
    )
    replication_assets = get_replication_assets()

    return (dbt_assets if isinstance(dbt_assets, list) else [dbt_assets]) + observable_source_assets + replication_assets
