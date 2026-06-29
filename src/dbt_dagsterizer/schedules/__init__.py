def get_schedules():
    from .dbt.schedules import get_dbt_schedules
    from .replication import get_replication_schedules
    from .sources.schedules import get_observe_sources_schedule

    schedules = [*get_dbt_schedules()]
    schedules.extend(get_replication_schedules())
    observe_sources_schedule = get_observe_sources_schedule()
    if observe_sources_schedule is not None:
        schedules.append(observe_sources_schedule)
    return schedules
