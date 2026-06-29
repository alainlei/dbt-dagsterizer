"""Replication jobs: one ``define_asset_job`` per replication asset."""

__all__ = ["get_replication_jobs", "get_replication_jobs_by_name"]

_replication_jobs = None
_replication_jobs_by_name = None


def get_replication_jobs():
    """Return a list of Dagster job definitions for replication.

    Returns an empty list when replication is disabled or no entries are enabled.
    """
    global _replication_jobs
    if _replication_jobs is None:
        from .auto_config import build_auto_replication_job_specs
        from .factory import build_replication_jobs

        specs = build_auto_replication_job_specs()
        if not specs:
            _replication_jobs = []
        else:
            _replication_jobs = build_replication_jobs(specs)
    return _replication_jobs


def get_replication_jobs_by_name():
    """Return a dict mapping job names to job definitions."""
    global _replication_jobs_by_name
    if _replication_jobs_by_name is None:
        jobs = get_replication_jobs()
        _replication_jobs_by_name = {job.name: job for job in jobs}
    return _replication_jobs_by_name
