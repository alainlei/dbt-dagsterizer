"""Replication assets: copy dbt model data from StarRocks to SQL Server via dlt."""

__all__ = ["get_replication_assets"]


def get_replication_assets():
    """Return a list of Dagster asset definitions for replication.

    Returns an empty list when replication is disabled or no entries are enabled.
    """
    from .auto_config import build_auto_replication_specs
    from .factory import build_replication_assets

    specs = build_auto_replication_specs()
    if not specs:
        return []
    return build_replication_assets(specs)
