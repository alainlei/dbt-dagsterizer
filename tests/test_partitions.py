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
    """Default end_offset is 0 when no config is set."""
    from dbt_dagsterizer import partitions

    monkeypatch.setenv("DAGSTER_DAILY_PARTITIONS_START_DATE", "2024-01-01")
    monkeypatch.setattr(partitions, "_daily_partitions_def", None)

    result = partitions.get_daily_partitions_def()
    assert result.end_offset == 0


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


def test_orchestration_index_include_current_day_partition_defaults_to_false():
    """Missing daily_config results in daily_include_current_day_partition=False."""
    from dbt_dagsterizer.orchestration_config import index

    data = {"partitions": {"daily": ["orders"]}}
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
