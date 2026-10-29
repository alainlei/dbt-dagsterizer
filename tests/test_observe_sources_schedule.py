from __future__ import annotations

from unittest.mock import Mock

import dagster as dg
import pytest

from dbt_dagsterizer.assets.sources.factory import build_observable_source_assets
from dbt_dagsterizer.jobs import get_jobs
from dbt_dagsterizer.jobs.sources.jobs import get_observe_sources_jobs
from dbt_dagsterizer.schedules import get_schedules
from dbt_dagsterizer.schedules.sources.schedules import get_observe_sources_schedules


@pytest.fixture
def source_specs(monkeypatch, tmp_path):
    specs = []
    monkeypatch.delenv("LUBAN_OBSERVE_SOURCES_BATCH_SIZE", raising=False)
    monkeypatch.delenv("LUBAN_OBSERVE_SOURCES_CRON", raising=False)
    monkeypatch.setattr("dbt_dagsterizer.assets.dbt.assets.get_dbt_assets", lambda: None)
    monkeypatch.setattr(
        "dbt_dagsterizer.assets.sources.automation.load_filtered_observable_sources",
        lambda: specs,
    )
    monkeypatch.setattr(
        "dbt_dagsterizer.assets.sources.factory.get_asset_keys_by_output_name_for_source",
        lambda _, source: {
            spec["table"]: dg.AssetKey([source, spec["table"]])
            for spec in specs
            if spec["source"] == source
        },
    )
    monkeypatch.setattr("dbt_dagsterizer.jobs.dbt.jobs.get_dbt_jobs", lambda: [])
    monkeypatch.setattr("dbt_dagsterizer.jobs.replication.get_replication_jobs", lambda: [])
    monkeypatch.setattr("dbt_dagsterizer.schedules.dbt.schedules.get_dbt_schedules", lambda: [])
    monkeypatch.setattr(
        "dbt_dagsterizer.schedules.replication.get_replication_schedules", lambda: []
    )
    monkeypatch.setattr(
        "dbt_dagsterizer.schedules.sources.schedules.get_dbt_project_dir", lambda: tmp_path
    )
    (tmp_path / "dagsterization.yml").write_text("timezone: Asia/Shanghai\n", encoding="utf-8")
    return specs


@pytest.mark.parametrize(
    "source_count,batch_size,expected_sizes",
    [
        (0, None, []),
        (1, None, [1]),
        (4, None, [4]),
        (5, None, [5]),
        (6, None, [5, 1]),
        (10, None, [5, 5]),
        (11, None, [5, 5, 1]),
        (21, None, [5, 5, 5, 5, 1]),
        (0, "3", []),
        (3, "1", [1, 1, 1]),
        (8, "3", [3, 3, 2]),
        (9, "3", [3, 3, 3]),
        (8, "20", [8]),
    ],
)
def test_observe_sources_batches(
    monkeypatch, source_specs, source_count, batch_size, expected_sizes
):
    source_specs.extend(
        {"source": "ods", "table": f"table_{i:02}", "watermark_column": "updated_at"}
        for i in range(source_count)
    )
    if batch_size is not None:
        monkeypatch.setenv("LUBAN_OBSERVE_SOURCES_BATCH_SIZE", batch_size)

    assets = build_observable_source_assets(dbt_assets=None, source_specs=source_specs)
    jobs = get_jobs()
    schedules = get_schedules()
    if len(expected_sizes) == 1:
        assert [job.name for job in jobs] == ["observe_sources_job"]
        assert [schedule.name for schedule in schedules] == ["observe_sources_schedule"]
    else:
        assert [job.name for job in jobs] == [
            f"observe_sources_job_{i}" for i in range(1, len(expected_sizes) + 1)
        ]
        assert [schedule.name for schedule in schedules] == [
            f"observe_sources_schedule_{i}" for i in range(1, len(expected_sizes) + 1)
        ]
    assert [schedule.job_name for schedule in schedules] == [job.name for job in jobs]

    selected_keys = [job.selection.resolve(assets) for job in jobs]
    assert [len(keys) for keys in selected_keys] == expected_sizes
    offset = 0
    for keys, size in zip(selected_keys, expected_sizes, strict=True):
        assert keys == {asset.key for asset in assets[offset : offset + size]}
        offset += size
    assert offset == len(assets)

    for schedule in schedules:
        assert schedule.cron_schedule == "*/5 * * * *"
        assert schedule.execution_timezone == "Asia/Shanghai"
        assert schedule.default_status == dg.DefaultScheduleStatus.RUNNING

    defs = dg.Definitions(
        assets=assets,
        jobs=jobs,
        schedules=schedules,
        resources={"starrocks": dg.ResourceDefinition.hardcoded_resource(Mock())},
    )
    dg.Definitions.validate_loadable(defs)


def test_observe_sources_schedules_share_custom_cron(monkeypatch, source_specs):
    source_specs.extend(
        {"source": "ods", "table": f"table_{i}", "watermark_column": "updated_at"} for i in range(4)
    )
    monkeypatch.setenv("LUBAN_OBSERVE_SOURCES_BATCH_SIZE", "1")
    monkeypatch.setenv("LUBAN_OBSERVE_SOURCES_CRON", "0 * * * *")

    schedules = get_observe_sources_schedules()
    assert len(schedules) == 4
    assert all(schedule.cron_schedule == "0 * * * *" for schedule in schedules)
    assert all(schedule.execution_timezone == "Asia/Shanghai" for schedule in schedules)


@pytest.mark.parametrize("batch_size", ["0", "-1", "1.5", "invalid", ""])
def test_observe_sources_reject_invalid_batch_size(monkeypatch, source_specs, batch_size):
    monkeypatch.setenv("LUBAN_OBSERVE_SOURCES_BATCH_SIZE", batch_size)

    for factory in (get_observe_sources_jobs, get_observe_sources_schedules):
        with pytest.raises(
            ValueError, match="LUBAN_OBSERVE_SOURCES_BATCH_SIZE must be a positive integer"
        ):
            factory()


def test_observation_jobs_only_observe_their_batch(source_specs):
    source_specs.extend(
        {"source": "ods", "table": f"table_{i}", "watermark_column": "updated_at"} for i in range(6)
    )
    assets = build_observable_source_assets(dbt_assets=None, source_specs=source_specs)
    jobs = get_jobs()
    starrocks = Mock()
    starrocks.query_scalar.return_value = "2026-10-28"
    defs = dg.Definitions(
        assets=assets,
        jobs=jobs,
        schedules=get_schedules(),
        resources={"starrocks": dg.ResourceDefinition.hardcoded_resource(starrocks)},
    )

    for job, expected_assets in zip(jobs, (assets[:5], assets[5:]), strict=True):
        starrocks.query_scalar.reset_mock()
        result = defs.resolve_job_def(job.name).execute_in_process()
        assert result.success
        assert starrocks.query_scalar.call_count == len(expected_assets)
        assert {event.asset_key for event in result.all_events if event.is_asset_observation} == {
            asset.key for asset in expected_assets
        }


def test_aggregators_preserve_other_jobs_and_schedules(monkeypatch, source_specs):
    source_specs.extend(
        {"source": "ods", "table": f"table_{i}", "watermark_column": "updated_at"} for i in range(6)
    )
    dbt_job, replication_job = object(), object()
    dbt_schedule, replication_schedule = object(), object()
    monkeypatch.setattr("dbt_dagsterizer.jobs.dbt.jobs.get_dbt_jobs", lambda: [dbt_job])
    monkeypatch.setattr(
        "dbt_dagsterizer.jobs.replication.get_replication_jobs", lambda: [replication_job]
    )
    monkeypatch.setattr(
        "dbt_dagsterizer.schedules.dbt.schedules.get_dbt_schedules", lambda: [dbt_schedule]
    )
    monkeypatch.setattr(
        "dbt_dagsterizer.schedules.replication.get_replication_schedules",
        lambda: [replication_schedule],
    )

    jobs = get_jobs()
    schedules = get_schedules()
    assert jobs[0] is dbt_job
    assert jobs[-1] is replication_job
    assert [job.name for job in jobs[1:-1]] == ["observe_sources_job_1", "observe_sources_job_2"]
    assert schedules[:2] == [dbt_schedule, replication_schedule]
    assert [schedule.job_name for schedule in schedules[2:]] == [job.name for job in jobs[1:-1]]
