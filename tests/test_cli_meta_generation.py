from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner
from ruamel.yaml import YAML

from dbt_dagsterizer.cli import cli


def _load_yaml(path: Path):
    y = YAML()
    with path.open("r", encoding="utf-8") as f:
        return y.load(f)


def test_meta_init_and_job_schedule_and_partition_change(tmp_path: Path):
    dbt_project = tmp_path / "dbt_project"
    dbt_project.mkdir(parents=True)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "meta",
            "init",
            "--dbt-project-dir",
            str(dbt_project),
            "--no-parse",
        ],
    )
    assert result.exit_code == 0

    orch_path = dbt_project / "dagsterization.yml"
    assert orch_path.exists()

    result = runner.invoke(
        cli,
        [
            "meta",
            "job",
            "--dbt-project-dir",
            str(dbt_project),
            "--models",
            "orders,fact_orders_daily",
            "--name",
            "daily_core",
            "--include-upstream",
            "--partitions",
            "daily",
            "--no-prepare",
            "--no-parse",
        ],
    )
    assert result.exit_code == 0

    result = runner.invoke(
        cli,
        [
            "meta",
            "asset-job",
            "--dbt-project-dir",
            str(dbt_project),
            "--models",
            "orders",
            "--no-prepare",
            "--no-parse",
        ],
    )
    assert result.exit_code == 0

    result = runner.invoke(
        cli,
        [
            "meta",
            "schedule",
            "--dbt-project-dir",
            str(dbt_project),
            "--models",
            "orders",
            "--name",
            "orders_daily",
            "--hour",
            "2",
            "--minute",
            "0",
            "--lookback-days",
            "3",
            "--enabled",
            "--no-prepare",
            "--no-parse",
        ],
    )
    assert result.exit_code == 0

    result = runner.invoke(
        cli,
        [
            "meta",
            "partition-change",
            "detector",
            "--dbt-project-dir",
            str(dbt_project),
            "--model",
            "orders",
            "--enabled",
            "--detect-source",
            "ods.orders",
            "--partition-date-expr",
            "order_date",
            "--updated-at-expr",
            "updated_at",
            "--lookback-days",
            "7",
            "--offset-days",
            "1",
            "--minimum-interval-seconds",
            "60",
            "--no-prepare",
            "--no-parse",
        ],
    )
    assert result.exit_code == 0

    result = runner.invoke(
        cli,
        [
            "meta",
            "partition-change",
            "propagator",
            "--dbt-project-dir",
            str(dbt_project),
            "--model",
            "orders",
            "--enabled",
            "--targets",
            "daily_core",
            "--no-prepare",
            "--no-parse",
        ],
    )
    assert result.exit_code == 0

    data = _load_yaml(orch_path)
    assert data["version"] == 1
    assert set(data["jobs"]["daily_core"]["models"]) == {"orders", "fact_orders_daily"}
    assert "orders" in data["asset_jobs"]
    assert data["schedules"]["orders_daily"]["job_name"] == "dbt_orders_asset_job"
    detectors = [d for d in data["partition_change"]["detectors"] if d["model"] == "orders"]
    assert detectors and detectors[0]["enabled"] is True
    propagators = [p for p in data["partition_change"]["propagators"] if p["upstream_model"] == "orders"]
    assert propagators and propagators[0]["targets"][0]["job_name"] == "daily_core"


def test_parse_flag_invokes_runner(tmp_path: Path, monkeypatch):
    dbt_project = tmp_path / "dbt_project"
    dbt_project.mkdir(parents=True)

    called = {"count": 0}

    import dbt_dagsterizer.cli_parts.meta as meta_mod

    def _fake_parse(*, dbt_project_dir: Path, dbt_profiles_dir: Path, dbt_target: str) -> None:
        called["count"] += 1

    monkeypatch.setattr(meta_mod, "run_dbt_parse", _fake_parse)

    runner = CliRunner()
    runner.invoke(
        cli,
        [
            "meta",
            "init",
            "--dbt-project-dir",
            str(dbt_project),
        ],
    )

    result = runner.invoke(
        cli,
        [
            "meta",
            "job",
            "--dbt-project-dir",
            str(dbt_project),
            "--models",
            "orders",
            "--name",
            "daily_core",
            "--no-prepare",
            "--parse",
        ],
    )
    assert result.exit_code == 0
    assert called["count"] == 1


def test_partition_config_cli_command(tmp_path: Path):
    """partition-config CLI command writes daily_config.include_current_day_partition to dagsterization.yml."""
    dbt_project = tmp_path / "dbt_project"
    dbt_project.mkdir(parents=True)

    runner = CliRunner()

    # Init first
    result = runner.invoke(
        cli,
        ["meta", "init", "--dbt-project-dir", str(dbt_project), "--no-parse"],
    )
    assert result.exit_code == 0

    # Set partition-config with --include-current-day-partition
    result = runner.invoke(
        cli,
        [
            "meta",
            "partition-config",
            "--dbt-project-dir",
            str(dbt_project),
            "--include-current-day-partition",
            "--no-prepare",
        ],
    )
    assert result.exit_code == 0

    orch_path = dbt_project / "dagsterization.yml"
    data = _load_yaml(orch_path)
    assert data["partitions"]["daily_config"]["include_current_day_partition"] is True


def test_partition_config_cli_no_include_current_day_partition(tmp_path: Path):
    """partition-config CLI command with --no-include-current-day-partition writes false."""
    dbt_project = tmp_path / "dbt_project"
    dbt_project.mkdir(parents=True)

    runner = CliRunner()

    # Init first
    result = runner.invoke(
        cli,
        ["meta", "init", "--dbt-project-dir", str(dbt_project), "--no-parse"],
    )
    assert result.exit_code == 0

    result = runner.invoke(
        cli,
        [
            "meta",
            "partition-config",
            "--dbt-project-dir",
            str(dbt_project),
            "--no-include-current-day-partition",
            "--no-prepare",
        ],
    )
    assert result.exit_code == 0

    orch_path = dbt_project / "dagsterization.yml"
    data = _load_yaml(orch_path)
    assert data["partitions"]["daily_config"]["include_current_day_partition"] is False
