def get_jobs():
    from .dbt.jobs import get_dbt_jobs
    from .replication import get_replication_jobs
    from .sources.jobs import get_observe_sources_jobs

    jobs = [*get_dbt_jobs()]
    jobs.extend(get_observe_sources_jobs())
    jobs.extend(get_replication_jobs())
    return jobs
