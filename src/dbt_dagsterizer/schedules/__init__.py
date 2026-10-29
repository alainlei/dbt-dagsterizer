def get_schedules():
    from .dbt.schedules import get_dbt_schedules
    from .replication import get_replication_schedules
    from .sources.schedules import get_observe_sources_schedules

    schedules = [*get_dbt_schedules()]
    schedules.extend(get_replication_schedules())
    schedules.extend(get_observe_sources_schedules())
    return schedules
