from __future__ import annotations

import dagster as dg


def test_observe_sources_schedule_uses_job_name(monkeypatch):
    from dbt_dagsterizer.schedules.sources.schedules import get_observe_sources_schedule

    monkeypatch.setattr(
        "dbt_dagsterizer.assets.sources.automation.load_filtered_observable_sources",
        lambda: [{"source": "ods", "table": "orders", "watermark_column": "updated_at"}],
    )

    sched = get_observe_sources_schedule()
    assert isinstance(sched, dg.ScheduleDefinition)
    assert sched.name == "observe_sources_schedule"
    assert sched.job_name == "observe_sources_job"
