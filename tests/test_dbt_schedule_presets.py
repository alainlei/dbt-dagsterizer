from __future__ import annotations

import pytest


def test_daily_at_defaults_to_yesterday_partition():
    from dbt_dagsterizer.schedules.dbt.presets import daily_at

    spec = daily_at(name="my_daily", job_name="my_job", hour=0, minute=0)
    assert spec["partition_type"] == "daily"
    assert spec["partition_offset_days"] == 1


def test_daily_at_custom_offset_days():
    from dbt_dagsterizer.schedules.dbt.presets import daily_at

    spec = daily_at(name="my_daily", job_name="my_job", hour=0, minute=0, offset_days=2)
    assert spec["partition_offset_days"] == 2


def test_daily_at_zero_offset_days():
    from dbt_dagsterizer.schedules.dbt.presets import daily_at

    spec = daily_at(name="my_daily", job_name="my_job", hour=0, minute=0, offset_days=0)
    assert spec["partition_offset_days"] == 0


def test_daily_at_negative_offset_days_raises():
    from dbt_dagsterizer.schedules.dbt.presets import daily_at

    with pytest.raises(ValueError, match="offset_days must be >= 0"):
        daily_at(name="my_daily", job_name="my_job", hour=0, minute=0, offset_days=-1)


# ---------------------------------------------------------------------------
# hourly_at preset tests
# ---------------------------------------------------------------------------


def test_hourly_at_defaults_to_previous_hour_partition():
    from dbt_dagsterizer.schedules.dbt.presets import hourly_at

    spec = hourly_at(name="my_hourly", job_name="my_job")
    assert spec["partition_type"] == "hourly"
    assert spec["partition_offset_hours"] == 1


def test_hourly_at_custom_offset_hours():
    from dbt_dagsterizer.schedules.dbt.presets import hourly_at

    spec = hourly_at(name="my_hourly", job_name="my_job", offset_hours=3)
    assert spec["partition_offset_hours"] == 3


def test_hourly_at_zero_offset_hours():
    from dbt_dagsterizer.schedules.dbt.presets import hourly_at

    spec = hourly_at(name="my_hourly", job_name="my_job", offset_hours=0)
    assert spec["partition_offset_hours"] == 0


def test_hourly_at_negative_offset_hours_raises():
    from dbt_dagsterizer.schedules.dbt.presets import hourly_at

    with pytest.raises(ValueError, match="offset_hours must be >= 0"):
        hourly_at(name="my_hourly", job_name="my_job", offset_hours=-1)


def test_hourly_at_negative_lookback_hours_raises():
    from dbt_dagsterizer.schedules.dbt.presets import hourly_at

    with pytest.raises(ValueError, match="lookback_hours must be >= 0"):
        hourly_at(name="my_hourly", job_name="my_job", lookback_hours=-1)


def test_hourly_at_empty_name_raises():
    from dbt_dagsterizer.schedules.dbt.presets import hourly_at

    with pytest.raises(ValueError, match="Schedule name must be non-empty"):
        hourly_at(name="", job_name="my_job")


def test_hourly_at_empty_job_name_raises():
    from dbt_dagsterizer.schedules.dbt.presets import hourly_at

    with pytest.raises(ValueError, match="job_name must be non-empty"):
        hourly_at(name="my_hourly", job_name="")


def test_hourly_at_invalid_minute_raises():
    from dbt_dagsterizer.schedules.dbt.presets import hourly_at

    with pytest.raises(ValueError, match="minute must be 0..59"):
        hourly_at(name="my_hourly", job_name="my_job", minute=60)

    with pytest.raises(ValueError, match="minute must be 0..59"):
        hourly_at(name="my_hourly", job_name="my_job", minute=-1)


def test_hourly_at_cron_format():
    """hourly_at generates cron in the format '{minute} * * * *'."""
    from dbt_dagsterizer.schedules.dbt.presets import hourly_at

    spec = hourly_at(name="my_hourly", job_name="my_job", minute=15)
    assert spec["cron_schedule"] == "15 * * * *"


def test_hourly_at_returns_correct_fields():
    """hourly_at returns all expected fields with correct defaults."""
    from dbt_dagsterizer.schedules.dbt.presets import hourly_at

    spec = hourly_at(name="my_hourly", job_name="events_job", minute=30, lookback_hours=2, offset_hours=1)
    assert spec == {
        "name": "my_hourly",
        "cron_schedule": "30 * * * *",
        "job_name": "events_job",
        "partition_type": "hourly",
        "partition_offset_days": 0,
        "partition_lookback_days": 0,
        "partition_offset_hours": 1,
        "partition_lookback_hours": 2,
        "partition_offset_months": 0,
        "partition_lookback_months": 0,
        "enabled": True,
        "dedupe_across_ticks": True,
        "timezone": "UTC",
    }


def test_hourly_at_enabled_false():
    from dbt_dagsterizer.schedules.dbt.presets import hourly_at

    spec = hourly_at(name="my_hourly", job_name="my_job", enabled=False)
    assert spec["enabled"] is False


def test_hourly_at_custom_timezone():
    from dbt_dagsterizer.schedules.dbt.presets import hourly_at

    spec = hourly_at(name="my_hourly", job_name="my_job", timezone="Asia/Shanghai")
    assert spec["timezone"] == "Asia/Shanghai"


# ---------------------------------------------------------------------------
# monthly_at preset tests
# ---------------------------------------------------------------------------


def test_monthly_at_defaults_to_previous_month_partition():
    from dbt_dagsterizer.schedules.dbt.presets import monthly_at

    spec = monthly_at(name="my_monthly", job_name="my_job", hour=0, minute=0)
    assert spec["partition_type"] == "monthly"
    assert spec["partition_offset_months"] == 1
    assert spec["cron_schedule"] == "0 0 1 * *"


def test_monthly_at_custom_offset_months():
    from dbt_dagsterizer.schedules.dbt.presets import monthly_at

    spec = monthly_at(name="my_monthly", job_name="my_job", hour=0, minute=0, offset_months=3)
    assert spec["partition_offset_months"] == 3


def test_monthly_at_zero_offset_months():
    from dbt_dagsterizer.schedules.dbt.presets import monthly_at

    spec = monthly_at(name="my_monthly", job_name="my_job", hour=0, minute=0, offset_months=0)
    assert spec["partition_offset_months"] == 0


def test_monthly_at_negative_offset_months_raises():
    from dbt_dagsterizer.schedules.dbt.presets import monthly_at

    with pytest.raises(ValueError, match="offset_months must be >= 0"):
        monthly_at(name="my_monthly", job_name="my_job", hour=0, minute=0, offset_months=-1)


def test_monthly_at_negative_lookback_months_raises():
    from dbt_dagsterizer.schedules.dbt.presets import monthly_at

    with pytest.raises(ValueError, match="lookback_months must be >= 0"):
        monthly_at(name="my_monthly", job_name="my_job", hour=0, minute=0, lookback_months=-1)


def test_monthly_at_empty_name_raises():
    from dbt_dagsterizer.schedules.dbt.presets import monthly_at

    with pytest.raises(ValueError, match="Schedule name must be non-empty"):
        monthly_at(name="", job_name="my_job", hour=0, minute=0)


def test_monthly_at_empty_job_name_raises():
    from dbt_dagsterizer.schedules.dbt.presets import monthly_at

    with pytest.raises(ValueError, match="job_name must be non-empty"):
        monthly_at(name="my_monthly", job_name="", hour=0, minute=0)


def test_monthly_at_invalid_hour_raises():
    from dbt_dagsterizer.schedules.dbt.presets import monthly_at

    with pytest.raises(ValueError, match="hour must be 0..23"):
        monthly_at(name="my_monthly", job_name="my_job", hour=24, minute=0)

    with pytest.raises(ValueError, match="hour must be 0..23"):
        monthly_at(name="my_monthly", job_name="my_job", hour=-1, minute=0)


def test_monthly_at_invalid_minute_raises():
    from dbt_dagsterizer.schedules.dbt.presets import monthly_at

    with pytest.raises(ValueError, match="minute must be 0..59"):
        monthly_at(name="my_monthly", job_name="my_job", hour=0, minute=60)

    with pytest.raises(ValueError, match="minute must be 0..59"):
        monthly_at(name="my_monthly", job_name="my_job", hour=0, minute=-1)


def test_monthly_at_invalid_day_of_month_raises():
    """day_of_month is capped at 28 because cron silently skips shorter months."""
    from dbt_dagsterizer.schedules.dbt.presets import monthly_at

    with pytest.raises(ValueError, match="day_of_month must be 1..28"):
        monthly_at(name="my_monthly", job_name="my_job", hour=0, minute=0, day_of_month=0)

    with pytest.raises(ValueError, match="day_of_month must be 1..28"):
        monthly_at(name="my_monthly", job_name="my_job", hour=0, minute=0, day_of_month=29)


def test_monthly_at_cron_format():
    """monthly_at generates cron in the format '{minute} {hour} {day_of_month} * *'."""
    from dbt_dagsterizer.schedules.dbt.presets import monthly_at

    spec = monthly_at(name="my_monthly", job_name="my_job", hour=2, minute=30, day_of_month=15)
    assert spec["cron_schedule"] == "30 2 15 * *"


def test_monthly_at_returns_correct_fields():
    """monthly_at returns all expected fields with correct defaults."""
    from dbt_dagsterizer.schedules.dbt.presets import monthly_at

    spec = monthly_at(
        name="my_monthly",
        job_name="events_job",
        hour=2,
        minute=30,
        day_of_month=15,
        lookback_months=2,
        offset_months=1,
    )
    assert spec == {
        "name": "my_monthly",
        "cron_schedule": "30 2 15 * *",
        "job_name": "events_job",
        "partition_type": "monthly",
        "partition_offset_days": 0,
        "partition_lookback_days": 0,
        "partition_offset_hours": 0,
        "partition_lookback_hours": 0,
        "partition_offset_months": 1,
        "partition_lookback_months": 2,
        "enabled": True,
        "dedupe_across_ticks": True,
        "timezone": "UTC",
    }


def test_monthly_at_enabled_false():
    from dbt_dagsterizer.schedules.dbt.presets import monthly_at

    spec = monthly_at(name="my_monthly", job_name="my_job", hour=0, minute=0, enabled=False)
    assert spec["enabled"] is False


def test_monthly_at_custom_timezone():
    from dbt_dagsterizer.schedules.dbt.presets import monthly_at

    spec = monthly_at(
        name="my_monthly", job_name="my_job", hour=0, minute=0, timezone="Asia/Shanghai"
    )
    assert spec["timezone"] == "Asia/Shanghai"

