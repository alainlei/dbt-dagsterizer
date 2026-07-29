"""Replication trigger sensors package.

Provides ``get_replication_trigger_sensors()`` which builds one sensor per
enabled replication entry.  Each sensor watches the upstream dbt model's
materialization events and triggers the replication job — per partition for
partitioned entries, or once (full table) for unpartitioned entries.
"""

__all__ = ["get_replication_trigger_sensors"]


def get_replication_trigger_sensors():
    """Return replication trigger sensors (lazy-loaded)."""
    from ...jobs.replication import get_replication_jobs_by_name
    from .auto_config import build_auto_replication_trigger_specs
    from .trigger import build_replication_trigger_sensors

    specs = build_auto_replication_trigger_specs()
    if not specs:
        return []

    jobs_by_name = get_replication_jobs_by_name()
    return build_replication_trigger_sensors(specs=specs, jobs_by_name=jobs_by_name)
