def get_jobs():
    from .dbt.jobs import get_dbt_jobs
    from .replication import get_replication_jobs
    from .sources.jobs import get_observe_sources_job

    jobs = [*get_dbt_jobs()]
    observe_sources_job = get_observe_sources_job()
    if observe_sources_job is not None:
        jobs.append(observe_sources_job)
    jobs.extend(get_replication_jobs())
    return jobs
