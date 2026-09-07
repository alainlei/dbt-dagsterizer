from __future__ import annotations

import pytest


def test_daily_partitions_requires_env_var(monkeypatch):
    from dbt_dagsterizer import partitions

    monkeypatch.delenv("DAGSTER_DAILY_PARTITIONS_START_DATE", raising=False)
    monkeypatch.setattr(partitions, "_daily_partitions_def", None)
    with pytest.raises(ValueError, match="DAGSTER_DAILY_PARTITIONS_START_DATE"):
        partitions.get_daily_partitions_def()


def test_daily_partitions_def_is_cached(monkeypatch):
    from dagster import DailyPartitionsDefinition

    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_DAILY_PARTITIONS_START_DATE", "2024-01-01")
    monkeypatch.setattr(partitions, "_daily_partitions_def", None)
    first = partitions.get_daily_partitions_def()
    second = partitions.get_daily_partitions_def()

    assert isinstance(first, DailyPartitionsDefinition)
    assert first is second


def test_job_factory_daily_partitions_requires_env_var(monkeypatch):
    from dbt_dagsterizer import partitions
    from dbt_dagsterizer.jobs.dbt import factory as job_factory

    monkeypatch.delenv("DAGSTER_DAILY_PARTITIONS_START_DATE", raising=False)
    monkeypatch.setattr(partitions, "_daily_partitions_def", None)
    with pytest.raises(ValueError, match="DAGSTER_DAILY_PARTITIONS_START_DATE"):
        job_factory._get_partitions_def("daily")


def test_translator_can_lazy_load_daily_partitions_def(monkeypatch):
    """Test that translator returns None when daily_partitions_def is None (partitioning handled at job level)."""
    from dbt_dagsterizer.assets.dbt.translator import LubanDagsterDbtTranslator

    monkeypatch.setenv("DAGSTER_DAILY_PARTITIONS_START_DATE", "2024-01-01")

    t = LubanDagsterDbtTranslator(
        daily_partitions_def=None,
        automation_observable_tables=set(),
        partitions_by_model={"orders": "daily"},
    )

    # IMPORTANT: Translator returns None to preserve lineage across partition types
    # Partitioning is handled at the job/schedule level
    partitions_def = t.get_partitions_def({"name": "orders"})
    assert partitions_def is None


@pytest.mark.parametrize(
    ("props", "partitions_by_model", "propagator_mode", "should_enable"),
    [
        (
            {"resource_type": "model", "name": "orders", "fqn": ["pkg", "staging", "orders"]},
            {},
            "sensor",
            True,
        ),
        (
            {"resource_type": "model", "name": "fact_orders", "tags": [], "fqn": ["pkg", "mart", "fact_orders"]},
            {"fact_orders": "daily"},
            "eager",
            True,
        ),
        (
            {"resource_type": "model", "name": "dim_customer", "tags": ["dim"], "fqn": ["pkg", "shared", "dim_customer"]},
            {},
            "sensor",
            True,
        ),
        (
            {
                "resource_type": "model",
                "name": "custom_model",
                "tags": ["automation_table"],
                "fqn": ["pkg", "custom", "custom_model"],
            },
            {},
            "sensor",
            True,
        ),
        (
            {"resource_type": "model", "name": "plain_model", "tags": [], "fqn": ["pkg", "custom", "plain_model"]},
            {},
            "sensor",
            False,
        ),
        (
            {
                "resource_type": "model",
                "name": "view_model",
                "tags": ["materialize_at_startup"],
                "fqn": ["pkg", "custom", "view_model"],
            },
            {},
            "sensor",
            True,
        ),
    ],
)
def test_translator_automation_rules(monkeypatch, props, partitions_by_model, propagator_mode, should_enable):
    from dbt_dagsterizer.assets.dbt.translator import LubanDagsterDbtTranslator

    monkeypatch.setenv("LUBAN_PARTITION_CHANGE_PROPAGATOR_MODE", propagator_mode)

    translator = LubanDagsterDbtTranslator(
        daily_partitions_def=None,
        automation_observable_tables={"orders"},
        partitions_by_model=partitions_by_model,
    )

    condition = translator.get_automation_condition(props)

    if should_enable:
        assert condition is not None
    else:
        assert condition is None


def test_daily_partitions_def_default_end_offset(monkeypatch):
    """Default end_offset is 1 when no config is set (current day partition included)."""
    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_DAILY_PARTITIONS_START_DATE", "2024-01-01")
    monkeypatch.setattr(partitions, "_daily_partitions_def", None)

    result = partitions.get_daily_partitions_def()
    assert result.end_offset == 1


def test_daily_partitions_def_with_include_current_day_partition(monkeypatch):
    """include_current_day_partition=true maps to end_offset=1."""
    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_DAILY_PARTITIONS_START_DATE", "2024-01-01")
    monkeypatch.setattr(partitions, "_daily_partitions_def", None)

    result = partitions.get_daily_partitions_def(include_current_day_partition=True)
    assert result.end_offset == 1


def test_daily_partitions_def_with_include_current_day_partition_false(monkeypatch):
    """include_current_day_partition=false maps to end_offset=0."""
    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_DAILY_PARTITIONS_START_DATE", "2024-01-01")
    monkeypatch.setattr(partitions, "_daily_partitions_def", None)

    result = partitions.get_daily_partitions_def(include_current_day_partition=False)
    assert result.end_offset == 0


def test_reset_daily_partitions_def(monkeypatch):
    """reset_daily_partitions_def clears the cached singleton."""
    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_DAILY_PARTITIONS_START_DATE", "2024-01-01")
    monkeypatch.setattr(partitions, "_daily_partitions_def", None)

    first = partitions.get_daily_partitions_def()
    assert first is not None

    partitions.reset_daily_partitions_def()
    assert partitions._daily_partitions_def is None


def test_get_partitions_def_threads_include_current_day_partition(monkeypatch):
    """get_partitions_def passes include_current_day_partition through to get_daily_partitions_def."""
    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_DAILY_PARTITIONS_START_DATE", "2024-01-01")
    monkeypatch.setattr(partitions, "_daily_partitions_def", None)

    result = partitions.get_partitions_def("daily", include_current_day_partition=True)
    assert result.end_offset == 1


def test_orchestration_index_include_current_day_partition():
    """index() parses daily_config.include_current_day_partition correctly."""
    from dbt_dagsterizer.orchestration_config import index

    data = {
        "partitions": {
            "daily": ["orders"],
            "daily_config": {"include_current_day_partition": True},
        },
    }
    idx = index(data)
    assert idx.daily_include_current_day_partition is True


def test_orchestration_index_include_current_day_partition_defaults_to_true():
    """Missing daily_config results in daily_include_current_day_partition=True."""
    from dbt_dagsterizer.orchestration_config import index

    data = {"partitions": {"daily": ["orders"]}}
    idx = index(data)
    assert idx.daily_include_current_day_partition is True


def test_orchestration_index_include_current_day_partition_explicit_false():
    """Explicit include_current_day_partition=false overrides the default."""
    from dbt_dagsterizer.orchestration_config import index

    data = {
        "partitions": {
            "daily": ["orders"],
            "daily_config": {"include_current_day_partition": False},
        },
    }
    idx = index(data)
    assert idx.daily_include_current_day_partition is False


def test_orchestration_index_include_current_day_partition_must_be_boolean():
    """Non-boolean include_current_day_partition in YAML raises ValueError."""
    from dbt_dagsterizer.orchestration_config import index

    data = {
        "partitions": {
            "daily": ["orders"],
            "daily_config": {"include_current_day_partition": "yes"},
        },
    }
    with pytest.raises(ValueError, match="boolean"):
        index(data)


def test_set_daily_config():
    """set_daily_config writes daily_config.include_current_day_partition correctly."""
    from dbt_dagsterizer.orchestration_config import set_daily_config

    data = {"version": 1, "partitions": {}}
    set_daily_config(data=data, include_current_day_partition=True)
    assert data["partitions"]["daily_config"]["include_current_day_partition"] is True


def test_set_daily_config_creates_daily_config_if_missing():
    """set_daily_config creates daily_config mapping if it doesn't exist."""
    from dbt_dagsterizer.orchestration_config import set_daily_config

    data = {"version": 1, "partitions": {"daily": ["orders"]}}
    set_daily_config(data=data, include_current_day_partition=True)
    assert data["partitions"]["daily_config"]["include_current_day_partition"] is True
    assert data["partitions"]["daily"] == ["orders"]


def test_validation_daily_config_include_current_day_partition_invalid_type():
    """validate_orchestration_structure catches non-boolean include_current_day_partition."""

    data = {
        "partitions": {
            "daily": ["orders"],
            "daily_config": {"include_current_day_partition": "not_a_bool"},
        },
    }
    # index() will raise ValueError because it checks isinstance(raw_include_current_day_partition, bool)
    with pytest.raises(ValueError, match="boolean"):
        from dbt_dagsterizer.orchestration_config import index
        index(data)


def test_validation_daily_config_not_a_mapping():
    """validate_orchestration_structure catches daily_config that is not a mapping."""
    from dbt_dagsterizer.cli_parts.validation import validate_orchestration_structure

    data = {
        "partitions": {
            "daily_config": "not_a_dict",
        },
    }
    issues = validate_orchestration_structure(orchestration=data)
    errors = [i for i in issues if i.level == "error"]
    assert any("daily_config must be a mapping" in i.message for i in errors)

def test_translator_view_materialization_is_fire_once():
    from dbt_dagsterizer.assets.dbt.translator import LubanDagsterDbtTranslator

    translator = LubanDagsterDbtTranslator(
        daily_partitions_def=None,
        automation_observable_tables=set(),
        partitions_by_model={},
    )

    props = {
        "resource_type": "model",
        "name": "view_model",
        "tags": ["materialize_at_startup"],
        "fqn": ["pkg", "custom", "view_model"],
    }

    condition = translator.get_automation_condition(props)

    import dagster as dg

    assert condition is not None
    assert condition == dg.AutomationCondition.missing()


# ---------------------------------------------------------------------------
# Hourly partitions tests
# ---------------------------------------------------------------------------


def test_hourly_partitions_requires_env_var(monkeypatch):
    from dbt_dagsterizer import partitions

    monkeypatch.delenv("DAGSTER_HOURLY_PARTITIONS_START_DATE", raising=False)
    monkeypatch.setattr(partitions, "_hourly_partitions_def", None)
    with pytest.raises(ValueError, match="DAGSTER_HOURLY_PARTITIONS_START_DATE"):
        partitions.get_hourly_partitions_def()


def test_hourly_partitions_def_is_cached(monkeypatch):
    from dagster import HourlyPartitionsDefinition

    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_HOURLY_PARTITIONS_START_DATE", "2024-01-01-00:00")
    monkeypatch.setattr(partitions, "_hourly_partitions_def", None)
    first = partitions.get_hourly_partitions_def()
    second = partitions.get_hourly_partitions_def()

    assert isinstance(first, HourlyPartitionsDefinition)
    assert first is second


def test_hourly_partitions_def_default_end_offset(monkeypatch):
    """Default end_offset is 1 when include_current_hour_partition is not set."""
    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_HOURLY_PARTITIONS_START_DATE", "2024-01-01-00:00")
    monkeypatch.setattr(partitions, "_hourly_partitions_def", None)

    result = partitions.get_hourly_partitions_def()
    assert result.end_offset == 1


def test_hourly_partitions_def_with_include_current_hour_partition_true(monkeypatch):
    """include_current_hour_partition=True maps to end_offset=1."""
    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_HOURLY_PARTITIONS_START_DATE", "2024-01-01-00:00")
    monkeypatch.setattr(partitions, "_hourly_partitions_def", None)

    result = partitions.get_hourly_partitions_def(include_current_hour_partition=True)
    assert result.end_offset == 1


def test_hourly_partitions_def_with_include_current_hour_partition_false(monkeypatch):
    """include_current_hour_partition=False maps to end_offset=0."""
    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_HOURLY_PARTITIONS_START_DATE", "2024-01-01-00:00")
    monkeypatch.setattr(partitions, "_hourly_partitions_def", None)

    result = partitions.get_hourly_partitions_def(include_current_hour_partition=False)
    assert result.end_offset == 0


def test_reset_hourly_partitions_def(monkeypatch):
    """reset_hourly_partitions_def clears the cached singleton."""
    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_HOURLY_PARTITIONS_START_DATE", "2024-01-01-00:00")
    monkeypatch.setattr(partitions, "_hourly_partitions_def", None)

    first = partitions.get_hourly_partitions_def()
    assert first is not None

    partitions.reset_hourly_partitions_def()
    assert partitions._hourly_partitions_def is None


def test_get_partitions_def_hourly_routes_to_hourly(monkeypatch):
    """get_partitions_def('hourly') returns a HourlyPartitionsDefinition."""
    from dagster import HourlyPartitionsDefinition

    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_HOURLY_PARTITIONS_START_DATE", "2024-01-01-00:00")
    monkeypatch.setattr(partitions, "_hourly_partitions_def", None)

    result = partitions.get_partitions_def("hourly")
    assert isinstance(result, HourlyPartitionsDefinition)


def test_get_partitions_def_hourly_passes_include_current_hour(monkeypatch):
    """get_partitions_def threads include_current_hour_partition through."""
    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_HOURLY_PARTITIONS_START_DATE", "2024-01-01-00:00")
    monkeypatch.setattr(partitions, "_hourly_partitions_def", None)

    result = partitions.get_partitions_def("hourly", include_current_hour_partition=False)
    assert result.end_offset == 0


def test_get_partitions_def_invalid_spec_raises():
    """get_partitions_def raises ValueError for unsupported spec."""
    from dbt_dagsterizer import partitions

    with pytest.raises(ValueError, match="Unsupported partition spec"):
        partitions.get_partitions_def("weekly")


# ---------------------------------------------------------------------------
# Monthly partitions tests
# ---------------------------------------------------------------------------


def test_monthly_partitions_requires_env_var(monkeypatch):
    from dbt_dagsterizer import partitions

    monkeypatch.delenv("DAGSTER_MONTHLY_PARTITIONS_START_DATE", raising=False)
    monkeypatch.setattr(partitions, "_monthly_partitions_def", None)
    with pytest.raises(ValueError, match="DAGSTER_MONTHLY_PARTITIONS_START_DATE"):
        partitions.get_monthly_partitions_def()


def test_monthly_partitions_def_is_cached(monkeypatch):
    from dagster import MonthlyPartitionsDefinition

    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_MONTHLY_PARTITIONS_START_DATE", "2024-01-01")
    monkeypatch.setattr(partitions, "_monthly_partitions_def", None)
    first = partitions.get_monthly_partitions_def()
    second = partitions.get_monthly_partitions_def()

    assert isinstance(first, MonthlyPartitionsDefinition)
    assert first is second


def test_monthly_partitions_def_default_end_offset(monkeypatch):
    """Default end_offset is 1 when include_current_month_partition is not set."""
    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_MONTHLY_PARTITIONS_START_DATE", "2024-01-01")
    monkeypatch.setattr(partitions, "_monthly_partitions_def", None)

    result = partitions.get_monthly_partitions_def()
    assert result.end_offset == 1


def test_monthly_partitions_def_with_include_current_month_partition_true(monkeypatch):
    """include_current_month_partition=True maps to end_offset=1."""
    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_MONTHLY_PARTITIONS_START_DATE", "2024-01-01")
    monkeypatch.setattr(partitions, "_monthly_partitions_def", None)

    result = partitions.get_monthly_partitions_def(include_current_month_partition=True)
    assert result.end_offset == 1


def test_monthly_partitions_def_with_include_current_month_partition_false(monkeypatch):
    """include_current_month_partition=False maps to end_offset=0."""
    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_MONTHLY_PARTITIONS_START_DATE", "2024-01-01")
    monkeypatch.setattr(partitions, "_monthly_partitions_def", None)

    result = partitions.get_monthly_partitions_def(include_current_month_partition=False)
    assert result.end_offset == 0


def test_monthly_partitions_def_keys_are_first_of_month(monkeypatch):
    """Monthly partition keys are YYYY-MM-01 (day_offset is not configurable)."""
    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_MONTHLY_PARTITIONS_START_DATE", "2024-01-01")
    monkeypatch.setattr(partitions, "_monthly_partitions_def", None)

    keys = partitions.get_monthly_partitions_def().get_partition_keys()

    assert keys[0] == "2024-01-01"
    assert keys[1] == "2024-02-01"
    assert keys[12] == "2025-01-01"
    assert all(key.endswith("-01") for key in keys)


def test_reset_monthly_partitions_def(monkeypatch):
    """reset_monthly_partitions_def clears the cached singleton."""
    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_MONTHLY_PARTITIONS_START_DATE", "2024-01-01")
    monkeypatch.setattr(partitions, "_monthly_partitions_def", None)

    first = partitions.get_monthly_partitions_def()
    assert first is not None

    partitions.reset_monthly_partitions_def()
    assert partitions._monthly_partitions_def is None


def test_get_partitions_def_monthly_routes_to_monthly(monkeypatch):
    """get_partitions_def('monthly') returns a MonthlyPartitionsDefinition."""
    from dagster import MonthlyPartitionsDefinition

    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_MONTHLY_PARTITIONS_START_DATE", "2024-01-01")
    monkeypatch.setattr(partitions, "_monthly_partitions_def", None)

    result = partitions.get_partitions_def("monthly")
    assert isinstance(result, MonthlyPartitionsDefinition)


def test_get_partitions_def_monthly_passes_include_current_month(monkeypatch):
    """get_partitions_def threads include_current_month_partition through."""
    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_MONTHLY_PARTITIONS_START_DATE", "2024-01-01")
    monkeypatch.setattr(partitions, "_monthly_partitions_def", None)

    result = partitions.get_partitions_def("monthly", include_current_month_partition=False)
    assert result.end_offset == 0


def test_get_partitions_def_monthly_does_not_require_daily_env_var(monkeypatch):
    """Monthly routing must not fall back to the daily start-date env var."""
    from dbt_dagsterizer import partitions

    monkeypatch.delenv("DAGSTER_DAILY_PARTITIONS_START_DATE", raising=False)
    monkeypatch.setenv("DAGSTER_MONTHLY_PARTITIONS_START_DATE", "2024-01-01")
    monkeypatch.setattr(partitions, "_monthly_partitions_def", None)

    result = partitions.get_partitions_def("monthly")
    assert result.get_partition_keys()[0] == "2024-01-01"


# ---------------------------------------------------------------------------
# Hourly orchestration config tests
# ---------------------------------------------------------------------------


def test_orchestration_index_hourly_include_current_hour_partition():
    """index() parses hourly_config.include_current_hour_partition correctly."""
    from dbt_dagsterizer.orchestration_config import index

    data = {
        "partitions": {
            "hourly": ["events"],
            "hourly_config": {"include_current_hour_partition": True},
        },
    }
    idx = index(data)
    assert idx.hourly_include_current_hour_partition is True


def test_orchestration_index_hourly_include_current_hour_partition_defaults_to_true():
    """Missing hourly_config results in hourly_include_current_hour_partition=True."""
    from dbt_dagsterizer.orchestration_config import index

    data = {"partitions": {"hourly": ["events"]}}
    idx = index(data)
    assert idx.hourly_include_current_hour_partition is True


def test_orchestration_index_hourly_include_current_hour_partition_explicit_false():
    """Explicit include_current_hour_partition=false overrides the default."""
    from dbt_dagsterizer.orchestration_config import index

    data = {
        "partitions": {
            "hourly": ["events"],
            "hourly_config": {"include_current_hour_partition": False},
        },
    }
    idx = index(data)
    assert idx.hourly_include_current_hour_partition is False


def test_orchestration_index_hourly_include_current_hour_partition_must_be_boolean():
    """Non-boolean include_current_hour_partition in YAML raises ValueError."""
    from dbt_dagsterizer.orchestration_config import index

    data = {
        "partitions": {
            "hourly": ["events"],
            "hourly_config": {"include_current_hour_partition": "yes"},
        },
    }
    with pytest.raises(ValueError, match="boolean"):
        index(data)


def test_orchestration_index_hourly_partitions_by_model():
    """index() correctly maps hourly models in partitions_by_model."""
    from dbt_dagsterizer.orchestration_config import index

    data = {
        "partitions": {
            "daily": ["orders"],
            "hourly": ["events", "clicks"],
            "unpartitioned": ["config_table"],
        },
    }
    idx = index(data)
    assert idx.partitions_by_model == {
        "orders": "daily",
        "events": "hourly",
        "clicks": "hourly",
        "config_table": "unpartitioned",
    }


def test_set_hourly_config():
    """set_hourly_config writes hourly_config.include_current_hour_partition correctly."""
    from dbt_dagsterizer.orchestration_config import set_hourly_config

    data = {"version": 1, "partitions": {}}
    set_hourly_config(data=data, include_current_hour_partition=True)
    assert data["partitions"]["hourly_config"]["include_current_hour_partition"] is True


def test_set_hourly_config_creates_hourly_config_if_missing():
    """set_hourly_config creates hourly_config mapping if it doesn't exist."""
    from dbt_dagsterizer.orchestration_config import set_hourly_config

    data = {"version": 1, "partitions": {"hourly": ["events"]}}
    set_hourly_config(data=data, include_current_hour_partition=False)
    assert data["partitions"]["hourly_config"]["include_current_hour_partition"] is False
    assert data["partitions"]["hourly"] == ["events"]


def test_set_hourly_config_none_does_not_write():
    """set_hourly_config with None does not write anything."""
    from dbt_dagsterizer.orchestration_config import set_hourly_config

    data = {"version": 1, "partitions": {}}
    set_hourly_config(data=data, include_current_hour_partition=None)
    assert "hourly_config" not in data["partitions"]


def test_set_partition_hourly():
    """set_partition with 'hourly' adds model to hourly list."""
    from dbt_dagsterizer.orchestration_config import set_partition

    data = {"version": 1, "partitions": {}}
    set_partition(data=data, model="events", partition="hourly")
    assert "events" in data["partitions"]["hourly"]


def test_set_partition_hourly_removes_from_daily():
    """Re-assigning a model from daily to hourly removes it from daily."""
    from dbt_dagsterizer.orchestration_config import set_partition

    data = {"version": 1, "partitions": {"daily": ["events"]}}
    set_partition(data=data, model="events", partition="hourly")
    assert "events" not in data["partitions"]["daily"]
    assert "events" in data["partitions"]["hourly"]


def test_set_partition_invalid_type_raises():
    """set_partition with an invalid type raises ValueError."""
    from dbt_dagsterizer.orchestration_config import set_partition

    data = {"version": 1, "partitions": {}}
    with pytest.raises(ValueError, match="daily|hourly|unpartitioned"):
        set_partition(data=data, model="events", partition="weekly")


def test_set_schedule_hourly_at():
    """set_schedule with schedule_type='hourly_at' writes hourly fields."""
    from dbt_dagsterizer.orchestration_config import set_schedule

    data = {"version": 1, "schedules": {}}
    set_schedule(
        data=data,
        name="hourly_events",
        job_name="events_job",
        schedule_type="hourly_at",
        hour=0,
        minute=30,
        lookback_days=0,
        enabled=True,
        lookback_hours=2,
        offset_hours=1,
    )
    entry = data["schedules"]["hourly_events"]
    assert entry["type"] == "hourly_at"
    assert entry["minute"] == 30
    assert entry["lookback_hours"] == 2
    assert entry["offset_hours"] == 1
    # daily fields should not be present
    assert "lookback_days" not in entry
    assert "offset_days" not in entry


def test_set_schedule_daily_at_does_not_write_hourly_fields():
    """set_schedule with schedule_type='daily_at' writes daily fields, not hourly fields."""
    from dbt_dagsterizer.orchestration_config import set_schedule

    data = {"version": 1, "schedules": {}}
    set_schedule(
        data=data,
        name="daily_orders",
        job_name="orders_job",
        schedule_type="daily_at",
        hour=3,
        minute=0,
        lookback_days=1,
        offset_days=1,
        enabled=True,
    )
    entry = data["schedules"]["daily_orders"]
    assert entry["type"] == "daily_at"
    assert entry["lookback_days"] == 1
    assert entry["offset_days"] == 1
    assert "lookback_hours" not in entry
    assert "offset_hours" not in entry


def test_set_schedule_invalid_type_raises():
    """set_schedule with an invalid schedule_type raises ValueError."""
    from dbt_dagsterizer.orchestration_config import set_schedule

    data = {"version": 1, "schedules": {}}
    with pytest.raises(ValueError, match="daily_at.*hourly_at"):
        set_schedule(
            data=data,
            name="bad",
            job_name="job",
            schedule_type="weekly_at",
            hour=0,
            minute=0,
            lookback_days=0,
            enabled=True,
        )


# ---------------------------------------------------------------------------
# Monthly orchestration config tests
# ---------------------------------------------------------------------------


def test_orchestration_index_monthly_include_current_month_partition():
    """index() parses monthly_config.include_current_month_partition correctly."""
    from dbt_dagsterizer.orchestration_config import index

    data = {
        "partitions": {
            "monthly": ["fact_revenue"],
            "monthly_config": {"include_current_month_partition": True},
        },
    }
    idx = index(data)

    assert idx.monthly_include_current_month_partition is True


def test_orchestration_index_monthly_include_current_month_partition_defaults_to_true():
    """Missing monthly_config results in monthly_include_current_month_partition=True."""
    from dbt_dagsterizer.orchestration_config import index

    data = {"partitions": {"monthly": ["fact_revenue"]}}
    idx = index(data)

    assert idx.monthly_include_current_month_partition is True


def test_orchestration_index_monthly_include_current_month_partition_explicit_false():
    """Explicit false is preserved."""
    from dbt_dagsterizer.orchestration_config import index

    data = {
        "partitions": {
            "monthly": ["fact_revenue"],
            "monthly_config": {"include_current_month_partition": False},
        },
    }
    idx = index(data)

    assert idx.monthly_include_current_month_partition is False


def test_orchestration_index_monthly_include_current_month_partition_must_be_boolean():
    """Non-boolean include_current_month_partition raises ValueError."""
    from dbt_dagsterizer.orchestration_config import index

    data = {
        "partitions": {
            "monthly": ["fact_revenue"],
            "monthly_config": {"include_current_month_partition": "yes"},
        },
    }
    with pytest.raises(ValueError, match="include_current_month_partition must be a boolean"):
        index(data)


def test_orchestration_index_monthly_partitions_by_model():
    """index() correctly maps monthly models in partitions_by_model."""
    from dbt_dagsterizer.orchestration_config import index

    data = {
        "partitions": {
            "monthly": ["fact_revenue", "fact_margin"],
        },
    }
    idx = index(data)

    assert idx.partitions_by_model == {
        "fact_revenue": "monthly",
        "fact_margin": "monthly",
    }


def test_set_monthly_config():
    """set_monthly_config writes monthly_config.include_current_month_partition correctly."""
    from dbt_dagsterizer.orchestration_config import set_monthly_config

    data = {"version": 1, "partitions": {"monthly_config": {"include_current_month_partition": False}}}
    set_monthly_config(data=data, include_current_month_partition=True)
    assert data["partitions"]["monthly_config"]["include_current_month_partition"] is True


def test_set_monthly_config_creates_monthly_config_if_missing():
    """set_monthly_config creates monthly_config mapping if it doesn't exist."""
    from dbt_dagsterizer.orchestration_config import set_monthly_config

    data = {"version": 1, "partitions": {"monthly": ["fact_revenue"]}}
    set_monthly_config(data=data, include_current_month_partition=False)
    assert data["partitions"]["monthly_config"]["include_current_month_partition"] is False
    assert data["partitions"]["monthly"] == ["fact_revenue"]


def test_set_monthly_config_none_does_not_write():
    """set_monthly_config with None does not write anything."""
    from dbt_dagsterizer.orchestration_config import set_monthly_config

    data = {"version": 1, "partitions": {}}
    set_monthly_config(data=data, include_current_month_partition=None)
    assert "monthly_config" not in data["partitions"]


def test_set_partition_monthly():
    """set_partition with 'monthly' adds model to monthly list."""
    from dbt_dagsterizer.orchestration_config import set_partition

    data = {"version": 1, "partitions": {}}
    set_partition(data=data, model="fact_revenue", partition="monthly")
    assert "fact_revenue" in data["partitions"]["monthly"]


def test_set_partition_monthly_removes_from_daily():
    """Re-assigning a model from daily to monthly removes it from daily."""
    from dbt_dagsterizer.orchestration_config import set_partition

    data = {"version": 1, "partitions": {"daily": ["fact_revenue"]}}
    set_partition(data=data, model="fact_revenue", partition="monthly")
    assert "fact_revenue" not in data["partitions"]["daily"]
    assert "fact_revenue" in data["partitions"]["monthly"]


def test_set_group_job_monthly_partitions():
    """set_group_job accepts 'monthly' as a job partition type."""
    from dbt_dagsterizer.orchestration_config import set_group_job

    data = {"version": 1, "jobs": {}}
    set_group_job(
        data=data,
        job_name="monthly_core",
        models=["fact_revenue"],
        partitions="monthly",
        include_upstream=False,
    )
    assert data["jobs"]["monthly_core"]["partitions"] == "monthly"


def test_set_schedule_monthly_at():
    """set_schedule with schedule_type='monthly_at' writes monthly fields only."""
    from dbt_dagsterizer.orchestration_config import set_schedule

    data = {"version": 1, "schedules": {}}
    set_schedule(
        data=data,
        name="monthly_revenue",
        job_name="revenue_job",
        schedule_type="monthly_at",
        hour=1,
        minute=15,
        lookback_days=0,
        enabled=True,
        day_of_month=3,
        lookback_months=2,
        offset_months=1,
    )
    entry = data["schedules"]["monthly_revenue"]
    assert entry["type"] == "monthly_at"
    assert entry["hour"] == 1
    assert entry["minute"] == 15
    assert entry["day_of_month"] == 3
    assert entry["lookback_months"] == 2
    assert entry["offset_months"] == 1
    # daily/hourly fields should not be present
    assert "lookback_days" not in entry
    assert "offset_days" not in entry
    assert "lookback_hours" not in entry
    assert "offset_hours" not in entry


def test_set_schedule_daily_at_does_not_write_monthly_fields():
    """A daily_at entry never carries month fields, so validation cannot reject it later."""
    from dbt_dagsterizer.orchestration_config import set_schedule

    data = {"version": 1, "schedules": {}}
    set_schedule(
        data=data,
        name="daily_orders",
        job_name="orders_job",
        schedule_type="daily_at",
        hour=3,
        minute=0,
        lookback_days=1,
        offset_days=1,
        enabled=True,
    )
    entry = data["schedules"]["daily_orders"]
    assert entry["type"] == "daily_at"
    assert "day_of_month" not in entry
    assert "lookback_months" not in entry
    assert "offset_months" not in entry


def test_set_partition_change_detector_monthly_fields_win_over_day_fields():
    """Supplying month lookback/offset persists the month pair and drops the day pair."""
    from dbt_dagsterizer.orchestration_config import set_partition_change_detector

    data = {"version": 1}
    set_partition_change_detector(
        data=data,
        model="fact_revenue",
        enabled=True,
        name=None,
        job_name="revenue_job",
        detect_relation=None,
        detect_source={"source": "ods", "table": "revenue"},
        partition_date_expr="revenue_date",
        updated_at_expr="updated_at",
        lookback_days=7,
        offset_days=1,
        minimum_interval_seconds=60,
        lookback_months=6,
        offset_months=0,
    )

    entry = data["partition_change"]["detectors"][0]
    assert entry["lookback_months"] == 6
    assert entry["offset_months"] == 0
    assert "lookback_days" not in entry
    assert "offset_days" not in entry


def test_set_partition_change_detector_without_month_fields_keeps_day_fields():
    """Omitting both month flags leaves the existing day-granular behaviour untouched."""
    from dbt_dagsterizer.orchestration_config import set_partition_change_detector

    data = {"version": 1}
    set_partition_change_detector(
        data=data,
        model="orders",
        enabled=True,
        name=None,
        job_name="orders_job",
        detect_relation=None,
        detect_source={"source": "ods", "table": "orders"},
        partition_date_expr="order_date",
        updated_at_expr="updated_at",
        lookback_days=7,
        offset_days=1,
        minimum_interval_seconds=60,
    )

    entry = data["partition_change"]["detectors"][0]
    assert entry["lookback_days"] == 7
    assert entry["offset_days"] == 1
    assert "lookback_months" not in entry
    assert "offset_months" not in entry


# ---------------------------------------------------------------------------
# Hourly validation tests
# ---------------------------------------------------------------------------


def test_validation_hourly_config_not_a_mapping():
    """validate_orchestration_structure catches hourly_config that is not a mapping."""
    from dbt_dagsterizer.cli_parts.validation import validate_orchestration_structure

    data = {
        "partitions": {
            "hourly_config": "not_a_dict",
        },
    }
    issues = validate_orchestration_structure(orchestration=data)
    errors = [i for i in issues if i.level == "error"]
    assert any("hourly_config must be a mapping" in i.message for i in errors)


def test_validation_hourly_config_include_current_hour_partition_invalid_type():
    """validate_orchestration_structure catches non-boolean include_current_hour_partition."""
    from dbt_dagsterizer.cli_parts.validation import validate_orchestration_structure

    data = {
        "partitions": {
            "hourly": ["events"],
            "hourly_config": {"include_current_hour_partition": "not_a_bool"},
        },
    }
    issues = validate_orchestration_structure(orchestration=data)
    errors = [i for i in issues if i.level == "error"]
    assert any("must be a boolean" in i.message for i in errors)


def test_validation_hourly_schedule_type_accepted():
    """validate_orchestration_structure accepts 'hourly_at' schedule type."""
    from dbt_dagsterizer.cli_parts.validation import validate_orchestration_structure

    data = {
        "schedules": {
            "my_hourly": {
                "type": "hourly_at",
                "job_name": "some_job",
                "hour": 0,
                "minute": 0,
            },
        },
    }
    issues = validate_orchestration_structure(orchestration=data)
    errors = [i for i in issues if i.level == "error"]
    schedule_type_errors = [i for i in errors if "type must be" in i.message]
    assert not schedule_type_errors


def test_validation_hourly_partition_type_accepted():
    """validate_orchestration_structure accepts 'hourly' as a valid partition type."""
    from dbt_dagsterizer.cli_parts.validation import validate_orchestration_structure

    data = {
        "partitions": {
            "hourly": ["events"],
        },
    }
    issues = validate_orchestration_structure(orchestration=data)
    errors = [i for i in issues if i.level == "error"]
    partition_errors = [i for i in errors if "must be daily|hourly|unpartitioned" in i.message]
    assert not partition_errors


def test_validation_job_with_hourly_partitions_accepted():
    """validate_orchestration_structure accepts 'hourly' in jobs.<name>.partitions."""
    from dbt_dagsterizer.cli_parts.validation import validate_orchestration_structure

    data = {
        "jobs": {
            "events_job": {
                "models": ["events"],
                "partitions": "hourly",
            },
        },
    }
    issues = validate_orchestration_structure(orchestration=data)
    errors = [i for i in issues if i.level == "error"]
    job_partition_errors = [i for i in errors if "must be daily|hourly|unpartitioned" in i.message]
    assert not job_partition_errors


# ---------------------------------------------------------------------------
# Monthly validation tests
# ---------------------------------------------------------------------------

_MONTHLY_MANIFEST = {
    "nodes": {
        "model.demo.fact_revenue": {"resource_type": "model", "name": "fact_revenue"},
    },
    "sources": {},
}


def _monthly_orchestration(**schedule_overrides):
    schedule = {
        "type": "monthly_at",
        "job_name": "revenue_job",
        "hour": 1,
        "minute": 15,
        "day_of_month": 1,
        "lookback_months": 0,
        "offset_months": 1,
    }
    schedule.update(schedule_overrides)
    return {
        "version": 1,
        "timezone": "UTC",
        "partitions": {"monthly": ["fact_revenue"], "monthly_config": {"include_current_month_partition": True}},
        "jobs": {"revenue_job": {"models": ["fact_revenue"], "partitions": "monthly"}},
        "asset_jobs": [],
        "schedules": {"revenue_monthly": schedule},
        "partition_change": {"detectors": [], "propagators": []},
    }


def _deep_errors(orchestration):
    from pathlib import Path

    from dbt_dagsterizer.cli_parts.validation import validate_orchestration

    issues = validate_orchestration(
        manifest=_MONTHLY_MANIFEST,
        orchestration=orchestration,
        require_file_exists=False,
        orchestration_path=Path("/tmp/dagsterization.yml"),
    )
    return [i.message for i in issues if i.level == "error"]


def test_validation_monthly_config_accepted_end_to_end():
    """A fully-specified monthly config passes validate_orchestration with no errors."""
    assert _deep_errors(_monthly_orchestration()) == []


def test_validation_monthly_config_not_a_mapping():
    """validate_orchestration_structure catches monthly_config that is not a mapping."""
    from dbt_dagsterizer.cli_parts.validation import validate_orchestration_structure

    data = {"partitions": {"monthly_config": "not_a_dict"}}
    issues = validate_orchestration_structure(orchestration=data)
    errors = [i for i in issues if i.level == "error"]
    assert any("monthly_config must be a mapping" in i.message for i in errors)


def test_validation_monthly_config_include_current_month_partition_invalid_type():
    """validate_orchestration_structure catches non-boolean include_current_month_partition."""
    from dbt_dagsterizer.cli_parts.validation import validate_orchestration_structure

    data = {
        "partitions": {
            "monthly": ["fact_revenue"],
            "monthly_config": {"include_current_month_partition": "not_a_bool"},
        },
    }
    issues = validate_orchestration_structure(orchestration=data)
    errors = [i for i in issues if i.level == "error"]
    assert any("include_current_month_partition must be a boolean" in i.message for i in errors)


def test_validation_structure_accepts_monthly_config():
    """validate_orchestration_structure accepts monthly partitions, jobs and schedules."""
    from dbt_dagsterizer.cli_parts.validation import validate_orchestration_structure

    issues = validate_orchestration_structure(orchestration=_monthly_orchestration())
    assert [i.message for i in issues if i.level == "error"] == []


def test_validation_monthly_at_requires_hour():
    """monthly_at without a valid hour is rejected."""
    errors = _deep_errors(_monthly_orchestration(hour=None))
    assert any("hour must be 0..23" in e for e in errors)


def test_validation_monthly_at_rejects_day_of_month_above_28():
    """29-31 never fires in shorter months, so day_of_month is capped at 28."""
    errors = _deep_errors(_monthly_orchestration(day_of_month=29))
    assert any("day_of_month must be 1..28" in e for e in errors)


def test_validation_monthly_at_rejects_day_of_month_below_1():
    errors = _deep_errors(_monthly_orchestration(day_of_month=0))
    assert any("day_of_month must be 1..28" in e for e in errors)


def test_validation_monthly_at_rejects_negative_offset_months():
    errors = _deep_errors(_monthly_orchestration(offset_months=-1))
    assert any("offset_months must be >= 0" in e for e in errors)


def test_validation_monthly_at_rejects_negative_lookback_months():
    errors = _deep_errors(_monthly_orchestration(lookback_months=-1))
    assert any("lookback_months must be >= 0" in e for e in errors)


def test_validation_monthly_at_rejects_day_fields():
    """monthly_at must use month offsets, not day offsets."""
    errors = _deep_errors(_monthly_orchestration(offset_days=2))
    assert any("cannot set offset_days" in e for e in errors)

    errors = _deep_errors(_monthly_orchestration(lookback_days=3))
    assert any("cannot set lookback_days" in e for e in errors)


def test_validation_monthly_at_rejects_hour_fields():
    errors = _deep_errors(_monthly_orchestration(offset_hours=2))
    assert any("cannot set offset_hours" in e for e in errors)

    errors = _deep_errors(_monthly_orchestration(lookback_hours=3))
    assert any("cannot set lookback_hours" in e for e in errors)


def test_validation_daily_at_rejects_month_fields():
    """A daily_at schedule carrying month offsets is a config error."""
    orch = _monthly_orchestration()
    orch["partitions"] = {"daily": ["fact_revenue"]}
    orch["jobs"]["revenue_job"]["partitions"] = "daily"
    orch["schedules"]["revenue_monthly"] = {
        "type": "daily_at",
        "job_name": "revenue_job",
        "hour": 1,
        "minute": 15,
        "lookback_days": 0,
        "offset_days": 1,
        "offset_months": 2,
    }

    errors = _deep_errors(orch)
    assert any("daily_at schedule cannot set offset_months" in e for e in errors)


def test_validation_hourly_at_rejects_month_fields():
    orch = _monthly_orchestration()
    orch["partitions"] = {"hourly": ["fact_revenue"]}
    orch["jobs"]["revenue_job"]["partitions"] = "hourly"
    orch["schedules"]["revenue_monthly"] = {
        "type": "hourly_at",
        "job_name": "revenue_job",
        "minute": 15,
        "lookback_hours": 0,
        "offset_hours": 1,
        "lookback_months": 2,
    }

    errors = _deep_errors(orch)
    assert any("hourly_at schedule cannot set lookback_months" in e for e in errors)


def test_validation_rejects_unknown_schedule_type():
    """The schedule-type enum error message now names monthly_at."""
    errors = _deep_errors(_monthly_orchestration(type="weekly_at"))
    assert any("must be 'daily_at', 'hourly_at' or 'monthly_at'" in e for e in errors)


def test_validation_rejects_unknown_job_partition_type():
    """The jobs.<name>.partitions enum error message now names monthly."""
    orch = _monthly_orchestration()
    orch["jobs"]["revenue_job"]["partitions"] = "weekly"
    errors = _deep_errors(orch)
    assert any("must be daily|hourly|monthly|unpartitioned" in e for e in errors)


# ---------------------------------------------------------------------------
# Timezone propagation tests
# ---------------------------------------------------------------------------


def test_daily_partitions_def_with_timezone(monkeypatch):
    """get_daily_partitions_def passes timezone through to DailyPartitionsDefinition."""
    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_DAILY_PARTITIONS_START_DATE", "2024-01-01")
    monkeypatch.setattr(partitions, "_daily_partitions_def", None)
    monkeypatch.setattr(partitions, "_daily_partitions_tz", None)

    result = partitions.get_daily_partitions_def(timezone="Asia/Macau")
    assert result.timezone == "Asia/Macau"


def test_daily_partitions_def_default_timezone_is_utc(monkeypatch):
    """When timezone is not specified, daily partitions default to UTC."""
    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_DAILY_PARTITIONS_START_DATE", "2024-01-01")
    monkeypatch.setattr(partitions, "_daily_partitions_def", None)
    monkeypatch.setattr(partitions, "_daily_partitions_tz", None)

    result = partitions.get_daily_partitions_def()
    assert result.timezone == "UTC"


def test_hourly_partitions_def_with_timezone(monkeypatch):
    """get_hourly_partitions_def passes timezone through to HourlyPartitionsDefinition."""
    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_HOURLY_PARTITIONS_START_DATE", "2024-01-01-00:00")
    monkeypatch.setattr(partitions, "_hourly_partitions_def", None)
    monkeypatch.setattr(partitions, "_hourly_partitions_tz", None)

    result = partitions.get_hourly_partitions_def(timezone="Asia/Macau")
    assert result.timezone == "Asia/Macau"


def test_hourly_partitions_def_default_timezone_is_utc(monkeypatch):
    """When timezone is not specified, hourly partitions default to UTC."""
    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_HOURLY_PARTITIONS_START_DATE", "2024-01-01-00:00")
    monkeypatch.setattr(partitions, "_hourly_partitions_def", None)
    monkeypatch.setattr(partitions, "_hourly_partitions_tz", None)

    result = partitions.get_hourly_partitions_def()
    assert result.timezone == "UTC"


def test_get_partitions_def_threads_timezone_daily(monkeypatch):
    """get_partitions_def passes timezone through to daily partitions."""
    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_DAILY_PARTITIONS_START_DATE", "2024-01-01")
    monkeypatch.setattr(partitions, "_daily_partitions_def", None)
    monkeypatch.setattr(partitions, "_daily_partitions_tz", None)

    result = partitions.get_partitions_def("daily", timezone="Asia/Macau")
    assert result.timezone == "Asia/Macau"


def test_get_partitions_def_threads_timezone_hourly(monkeypatch):
    """get_partitions_def passes timezone through to hourly partitions."""
    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_HOURLY_PARTITIONS_START_DATE", "2024-01-01-00:00")
    monkeypatch.setattr(partitions, "_hourly_partitions_def", None)
    monkeypatch.setattr(partitions, "_hourly_partitions_tz", None)

    result = partitions.get_partitions_def("hourly", timezone="Asia/Macau")
    assert result.timezone == "Asia/Macau"


def test_daily_partitions_cache_invalidated_on_timezone_change(monkeypatch):
    """Changing the timezone invalidates the cached singleton."""
    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_DAILY_PARTITIONS_START_DATE", "2024-01-01")
    monkeypatch.setattr(partitions, "_daily_partitions_def", None)
    monkeypatch.setattr(partitions, "_daily_partitions_tz", None)

    utc_def = partitions.get_daily_partitions_def(timezone="UTC")
    macau_def = partitions.get_daily_partitions_def(timezone="Asia/Macau")

    # The two should be different objects (cache invalidated by timezone change)
    assert utc_def is not macau_def
    assert utc_def.timezone == "UTC"
    assert macau_def.timezone == "Asia/Macau"


def test_orchestration_index_timezone_parsed():
    """index() correctly parses the top-level timezone field."""
    from dbt_dagsterizer.orchestration_config import index

    data = {"timezone": "Asia/Macau", "partitions": {"daily": ["orders"]}}
    idx = index(data)
    assert idx.timezone == "Asia/Macau"


def test_orchestration_index_timezone_defaults_to_utc():
    """Missing timezone defaults to UTC."""
    from dbt_dagsterizer.orchestration_config import index

    data = {"partitions": {"daily": ["orders"]}}
    idx = index(data)
    assert idx.timezone == "UTC"


def test_monthly_partitions_def_with_timezone(monkeypatch):
    """get_monthly_partitions_def passes timezone through to MonthlyPartitionsDefinition."""
    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_MONTHLY_PARTITIONS_START_DATE", "2024-01-01")
    monkeypatch.setattr(partitions, "_monthly_partitions_def", None)
    monkeypatch.setattr(partitions, "_monthly_partitions_tz", None)

    result = partitions.get_monthly_partitions_def(timezone="Asia/Macau")
    assert result.timezone == "Asia/Macau"


def test_monthly_partitions_def_default_timezone_is_utc(monkeypatch):
    """When timezone is not specified, monthly partitions default to UTC."""
    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_MONTHLY_PARTITIONS_START_DATE", "2024-01-01")
    monkeypatch.setattr(partitions, "_monthly_partitions_def", None)
    monkeypatch.setattr(partitions, "_monthly_partitions_tz", None)

    result = partitions.get_monthly_partitions_def()
    assert result.timezone == "UTC"


def test_get_partitions_def_threads_timezone_monthly(monkeypatch):
    """get_partitions_def passes timezone through to monthly partitions."""
    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_MONTHLY_PARTITIONS_START_DATE", "2024-01-01")
    monkeypatch.setattr(partitions, "_monthly_partitions_def", None)
    monkeypatch.setattr(partitions, "_monthly_partitions_tz", None)

    result = partitions.get_partitions_def("monthly", timezone="Asia/Macau")
    assert result.timezone == "Asia/Macau"


def test_monthly_partitions_cache_invalidated_on_timezone_change(monkeypatch):
    """Changing the timezone invalidates the cached monthly singleton."""
    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_MONTHLY_PARTITIONS_START_DATE", "2024-01-01")
    monkeypatch.setattr(partitions, "_monthly_partitions_def", None)
    monkeypatch.setattr(partitions, "_monthly_partitions_tz", None)

    utc_def = partitions.get_monthly_partitions_def(timezone="UTC")
    macau_def = partitions.get_monthly_partitions_def(timezone="Asia/Macau")

    assert utc_def is not macau_def
    assert utc_def.timezone == "UTC"
    assert macau_def.timezone == "Asia/Macau"


# ---------------------------------------------------------------------------
# Month arithmetic helper tests
# ---------------------------------------------------------------------------


def test_month_floor_returns_first_of_month():
    from datetime import date

    from dbt_dagsterizer.partitions import month_floor

    assert month_floor(date(2026, 8, 21)) == date(2026, 8, 1)
    assert month_floor(date(2026, 8, 1)) == date(2026, 8, 1)


def test_add_months_forward():
    from datetime import date

    from dbt_dagsterizer.partitions import add_months

    assert add_months(date(2026, 8, 1), 1) == date(2026, 9, 1)
    assert add_months(date(2026, 8, 1), 4) == date(2026, 12, 1)


def test_add_months_backward_across_year_boundary():
    from datetime import date

    from dbt_dagsterizer.partitions import add_months

    assert add_months(date(2026, 1, 1), -1) == date(2025, 12, 1)
    assert add_months(date(2026, 3, 1), -3) == date(2025, 12, 1)


def test_add_months_always_returns_first_of_month():
    """Partition keys are YYYY-MM-01, so a mid-month input must not survive."""
    from datetime import date

    from dbt_dagsterizer.partitions import add_months

    assert add_months(date(2026, 8, 31), 1) == date(2026, 9, 1)
    assert add_months(date(2026, 8, 15), 0) == date(2026, 8, 1)
