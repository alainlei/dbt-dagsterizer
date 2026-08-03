"""Tests for global timezone configuration in dagsterization.yml."""
from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner
from ruamel.yaml import YAML

from dbt_dagsterizer.cli import cli


def _load_yaml(path: Path):
    y = YAML()
    with path.open("r", encoding="utf-8") as f:
        return y.load(f)


# --- orchestration_config tests ---


def test_load_or_create_defaults_timezone_to_utc(tmp_path: Path):
    """load_or_create sets timezone='UTC' when the key is missing."""
    from dbt_dagsterizer.orchestration_config import load_or_create

    orch_file = tmp_path / "dagsterization.yml"
    # Write a minimal config without timezone
    y = YAML()
    orch_file.parent.mkdir(parents=True, exist_ok=True)
    with orch_file.open("w") as f:
        y.dump({"version": 1, "jobs": {}, "schedules": {}}, f)

    data = load_or_create(orch_file)
    assert data["timezone"] == "UTC"


def test_load_or_create_preserves_existing_timezone(tmp_path: Path):
    """load_or_create preserves an existing timezone value."""
    from dbt_dagsterizer.orchestration_config import load_or_create

    orch_file = tmp_path / "dagsterization.yml"
    y = YAML()
    orch_file.parent.mkdir(parents=True, exist_ok=True)
    with orch_file.open("w") as f:
        y.dump({"version": 1, "timezone": "Asia/Shanghai", "jobs": {}, "schedules": {}}, f)

    data = load_or_create(orch_file)
    assert data["timezone"] == "Asia/Shanghai"


def test_index_parses_timezone():
    """OrchestrationIndex picks up the timezone field."""
    from dbt_dagsterizer.orchestration_config import index

    data = {"timezone": "Europe/Berlin", "partitions": {}}
    idx = index(data)
    assert idx.timezone == "Europe/Berlin"


def test_index_defaults_timezone_to_utc():
    """OrchestrationIndex defaults timezone to UTC when absent."""
    from dbt_dagsterizer.orchestration_config import index

    data = {"partitions": {}}
    idx = index(data)
    assert idx.timezone == "UTC"


def test_index_raises_on_invalid_timezone():
    """Non-string timezone raises ValueError."""
    from dbt_dagsterizer.orchestration_config import index

    data = {"timezone": 123, "partitions": {}}
    with pytest.raises(ValueError, match="timezone"):
        index(data)


def test_index_raises_on_empty_timezone():
    """Empty-string timezone raises ValueError."""
    from dbt_dagsterizer.orchestration_config import index

    data = {"timezone": "  ", "partitions": {}}
    with pytest.raises(ValueError, match="timezone"):
        index(data)


def test_index_raises_on_unknown_timezone():
    """Unknown IANA timezone raises ValueError."""
    from dbt_dagsterizer.orchestration_config import index

    data = {"timezone": "Mars/Olympus_Mons", "partitions": {}}
    with pytest.raises(ValueError, match="invalid timezone"):
        index(data)



def test_set_timezone():
    """set_timezone writes the timezone to the config dict."""
    from dbt_dagsterizer.orchestration_config import set_timezone

    data = {"version": 1, "timezone": "UTC"}
    set_timezone(data=data, timezone="US/Eastern")
    assert data["timezone"] == "US/Eastern"


def test_set_timezone_strips_whitespace():
    """set_timezone strips leading/trailing whitespace."""
    from dbt_dagsterizer.orchestration_config import set_timezone

    data = {"version": 1}
    set_timezone(data=data, timezone="  Asia/Tokyo  ")
    assert data["timezone"] == "Asia/Tokyo"


def test_set_timezone_rejects_empty():
    """set_timezone raises ValueError for empty string."""
    from dbt_dagsterizer.orchestration_config import set_timezone

    data = {"version": 1}
    with pytest.raises(ValueError, match="non-empty"):
        set_timezone(data=data, timezone="")


def test_set_timezone_rejects_unknown_timezone():
    """set_timezone raises ValueError for an unknown timezone."""
    from dbt_dagsterizer.orchestration_config import set_timezone

    data = {"version": 1}
    with pytest.raises(ValueError, match="invalid timezone"):
        set_timezone(data=data, timezone="Mars/Olympus_Mons")



# --- preset tests ---


def test_daily_at_preset_includes_timezone():
    """daily_at preset includes timezone in the returned spec."""
    from dbt_dagsterizer.schedules.dbt.presets import daily_at

    spec = daily_at(
        name="test_schedule",
        job_name="test_job",
        hour=2,
        minute=30,
        timezone="Europe/London",
    )
    assert spec["timezone"] == "Europe/London"


def test_daily_at_preset_defaults_timezone_to_utc():
    """daily_at preset defaults timezone to UTC."""
    from dbt_dagsterizer.schedules.dbt.presets import daily_at

    spec = daily_at(
        name="test_schedule",
        job_name="test_job",
        hour=2,
        minute=30,
    )
    assert spec["timezone"] == "UTC"


# --- CLI tests ---


def test_cli_meta_timezone(tmp_path: Path):
    """CLI meta timezone command writes timezone to dagsterization.yml."""
    dbt_project = tmp_path / "dbt_project"
    dbt_project.mkdir(parents=True)

    runner = CliRunner()
    # Init first
    result = runner.invoke(
        cli,
        ["meta", "init", "--dbt-project-dir", str(dbt_project), "--no-parse"],
    )
    assert result.exit_code == 0

    # Set timezone
    result = runner.invoke(
        cli,
        [
            "meta",
            "timezone",
            "--dbt-project-dir",
            str(dbt_project),
            "--timezone",
            "Asia/Shanghai",
            "--no-prepare",
        ],
    )
    assert result.exit_code == 0

    orch_path = dbt_project / "dagsterization.yml"
    data = _load_yaml(orch_path)
    assert data["timezone"] == "Asia/Shanghai"


def test_cli_meta_init_includes_timezone(tmp_path: Path):
    """CLI meta init creates dagsterization.yml with timezone=UTC."""
    dbt_project = tmp_path / "dbt_project"
    dbt_project.mkdir(parents=True)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["meta", "init", "--dbt-project-dir", str(dbt_project), "--no-parse"],
    )
    assert result.exit_code == 0

    orch_path = dbt_project / "dagsterization.yml"
    data = _load_yaml(orch_path)
    assert data["timezone"] == "UTC"


# --- validation tests ---


def test_validate_structure_accepts_valid_timezone():
    """validate_orchestration_structure accepts a valid timezone string."""
    from dbt_dagsterizer.cli_parts.validation import validate_orchestration_structure

    orch = {
        "version": 1,
        "timezone": "America/New_York",
        "partitions": {},
        "jobs": {},
        "asset_jobs": [],
        "schedules": {},
        "partition_change": {"detectors": [], "propagators": []},
    }
    issues = validate_orchestration_structure(orchestration=orch)
    errors = [i for i in issues if i.level == "error"]
    assert len(errors) == 0


def test_validate_structure_rejects_non_string_timezone():
    """validate_orchestration_structure rejects a non-string timezone."""
    from dbt_dagsterizer.cli_parts.validation import validate_orchestration_structure

    orch = {
        "version": 1,
        "timezone": 42,
        "partitions": {},
        "jobs": {},
        "asset_jobs": [],
        "schedules": {},
        "partition_change": {"detectors": [], "propagators": []},
    }
    issues = validate_orchestration_structure(orchestration=orch)
    errors = [i for i in issues if i.level == "error"]
    assert any("timezone" in i.message for i in errors)


def test_validate_structure_rejects_unknown_timezone():
    """validate_orchestration_structure rejects unknown timezone names."""
    from dbt_dagsterizer.cli_parts.validation import validate_orchestration_structure

    orch = {
        "version": 1,
        "timezone": "Mars/Olympus_Mons",
        "partitions": {},
        "jobs": {},
        "asset_jobs": [],
        "schedules": {},
        "partition_change": {"detectors": [], "propagators": []},
    }
    issues = validate_orchestration_structure(orchestration=orch)
    errors = [i for i in issues if i.level == "error"]
    assert any("invalid timezone" in i.message for i in errors)
