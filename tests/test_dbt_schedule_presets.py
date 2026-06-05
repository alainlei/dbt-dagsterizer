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

