"""End-to-end load test for the monthly partition type.

Guards the regression where a monthly-partitioned *replicated* model raised
``Unsupported partition_type`` from the replication schedule factory at
definitions-build time, taking the whole code location down with it.
"""
from pathlib import Path

from click.testing import CliRunner
from dagster import MonthlyPartitionsDefinition

from dbt_dagsterizer import partitions
from dbt_dagsterizer.api import build_definitions
from dbt_dagsterizer.cli import cli

_ORCHESTRATION_YML = """\
version: 1
timezone: UTC
partitions:
  monthly:
    - fact_revenue
  monthly_config:
    include_current_month_partition: true
jobs:
  revenue_job:
    models:
      - fact_revenue
    partitions: monthly
asset_jobs: []
schedules:
  revenue_monthly:
    type: monthly_at
    job_name: revenue_job
    hour: 1
    minute: 15
    day_of_month: 1
    lookback_months: 0
    offset_months: 1
replication:
  enabled: true
  schedules:
    enabled: true
  entries:
    - model: fact_revenue
      enabled: true
      destination_table: fact_revenue
      destination_schema: dbo
      write_disposition: replace
      partition_column: revenue_date
partition_change:
  detectors: []
  propagators: []
ssrs_reports: []
"""


def _make_dbt_project(tmp_path: Path) -> Path:
    dbt_project_dir = tmp_path / "dbt_project"
    (dbt_project_dir / "models").mkdir(parents=True)

    (dbt_project_dir / "dbt_project.yml").write_text(
        "name: demo\nprofile: demo\nconfig-version: 2\nversion: '1.0.0'\nmodel-paths: ['models']\n",
        encoding="utf-8",
    )
    (dbt_project_dir / "profiles.yml").write_text(
        "demo:\n  target: dev\n  outputs:\n    dev:\n      type: starrocks\n      host: localhost\n      port: 9030\n      user: root\n      password: ''\n      schema: demo\n",
        encoding="utf-8",
    )
    (dbt_project_dir / "packages.yml").write_text("packages: []\n", encoding="utf-8")
    (dbt_project_dir / "models" / "fact_revenue.sql").write_text(
        "select date '2026-01-01' as revenue_date, 1 as amount\n",
        encoding="utf-8",
    )
    (dbt_project_dir / "dagsterization.yml").write_text(_ORCHESTRATION_YML, encoding="utf-8")
    return dbt_project_dir


def _reset_caches(monkeypatch) -> None:
    """Clear the project-scoped module caches so this test owns its own definitions.

    ``build_definitions`` memoises into module globals; another integration test in the
    same pytest process populates them from a different scratch dbt project.
    """
    import dbt_dagsterizer.assets.dbt.assets as dbt_assets
    import dbt_dagsterizer.jobs.dbt.jobs as dbt_jobs
    import dbt_dagsterizer.jobs.replication as replication_jobs
    import dbt_dagsterizer.schedules.dbt.schedules as dbt_schedules

    monkeypatch.setattr(dbt_jobs, "_dbt_jobs_by_name", None)
    monkeypatch.setattr(dbt_schedules, "_dbt_schedules", None)
    monkeypatch.setattr(dbt_assets, "_dbt_assets_def", None)
    monkeypatch.setattr(replication_jobs, "_replication_jobs", None)
    monkeypatch.setattr(replication_jobs, "_replication_jobs_by_name", None)


def _set_env(monkeypatch) -> None:
    monkeypatch.setenv("LUBAN_DBT_PREPARE_ON_LOAD", "1")
    monkeypatch.setenv("LUBAN_DEFAULT_DBT_TARGET", "dev")
    monkeypatch.setenv("STARROCKS_HOST", "mock_host")
    monkeypatch.setenv("STARROCKS_PORT", "9030")
    monkeypatch.setenv("STARROCKS_USER", "mock_user")
    monkeypatch.setenv("STARROCKS_PASSWORD", "mock_pass")
    monkeypatch.setenv("DAGSTER_MONTHLY_PARTITIONS_START_DATE", "2026-01-01")
    monkeypatch.setattr(partitions, "_monthly_partitions_def", None)
    monkeypatch.setattr(partitions, "_monthly_partitions_tz", None)
    _reset_caches(monkeypatch)


def test_meta_validate_accepts_monthly_config(tmp_path: Path, monkeypatch):
    _set_env(monkeypatch)
    dbt_project_dir = _make_dbt_project(tmp_path)

    result = CliRunner().invoke(
        cli,
        ["meta", "validate", "--dbt-project-dir", str(dbt_project_dir)],
    )

    assert result.exit_code == 0, result.output
    assert "OK" in result.output


def test_definitions_load_with_monthly_partitions_and_replication(tmp_path: Path, monkeypatch):
    """A monthly model that is also replicated must build without raising."""
    _set_env(monkeypatch)
    dbt_project_dir = _make_dbt_project(tmp_path)

    # Warm the manifest so build_definitions does not re-run dbt parse.
    warmup = CliRunner().invoke(
        cli,
        ["meta", "validate", "--dbt-project-dir", str(dbt_project_dir)],
    )
    assert warmup.exit_code == 0, warmup.output

    defs = build_definitions(dbt_project_dir=dbt_project_dir, default_dbt_target="dev")

    repo = defs.get_repository_def()
    jobs = {j.name: j for j in repo.get_all_jobs()}
    schedules = {s.name: s for s in repo.schedule_defs}

    assert "revenue_job" in jobs
    assert isinstance(jobs["revenue_job"].partitions_def, MonthlyPartitionsDefinition)
    keys = jobs["revenue_job"].partitions_def.get_partition_keys()
    assert keys[0] == "2026-01-01"
    assert all(key.endswith("-01") for key in keys)

    assert "revenue_monthly" in schedules
    assert schedules["revenue_monthly"].cron_schedule == "15 1 1 * *"

    # The replication path is the one that used to raise Unsupported partition_type.
    assert "replicate_fact_revenue_job" in jobs
    assert isinstance(jobs["replicate_fact_revenue_job"].partitions_def, MonthlyPartitionsDefinition)
    assert "replicate_fact_revenue_schedule" in schedules
    assert schedules["replicate_fact_revenue_schedule"].cron_schedule == "30 0 1 * *"
