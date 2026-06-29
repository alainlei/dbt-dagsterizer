"""Replication schedules: automatically trigger replication jobs after dbt builds."""

__all__ = ["get_replication_schedules"]


def get_replication_schedules():
    """Return a list of Dagster schedule definitions for replication jobs.

    Returns an empty list when replication is disabled or no entries are enabled.
    """
    from .auto_config import build_auto_replication_schedule_specs
    from .factory import build_replication_schedules

    job_specs = build_auto_replication_schedule_specs()
    if not job_specs:
        return []

    return build_replication_schedules(job_specs)
