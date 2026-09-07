from __future__ import annotations

import os
from datetime import date

from dagster import (
    DailyPartitionsDefinition,
    HourlyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    PartitionsDefinition,
)

_daily_partitions_def = None
_daily_partitions_tz = None
_hourly_partitions_def = None
_hourly_partitions_tz = None
_monthly_partitions_def = None
_monthly_partitions_tz = None


def month_floor(d: date) -> date:
    return d.replace(day=1)


def add_months(d: date, months: int) -> date:
    """Shift by whole months, always landing on day 1 (the monthly partition key)."""
    total = d.year * 12 + (d.month - 1) + months
    return date(total // 12, total % 12 + 1, 1)


def get_daily_partitions_def(
    include_current_day_partition: bool | None = None,
    timezone: str | None = None,
) -> DailyPartitionsDefinition:
    global _daily_partitions_def, _daily_partitions_tz
    if _daily_partitions_def is not None and _daily_partitions_tz == timezone:
        return _daily_partitions_def
    start_date = os.getenv("DAGSTER_DAILY_PARTITIONS_START_DATE")
    if not start_date:
        raise ValueError(
            "DAGSTER_DAILY_PARTITIONS_START_DATE must be set (YYYY-MM-DD) when using daily partitions"
        )

    # Resolve end_offset from boolean flag: parameter > default(include current day)
    if include_current_day_partition is False:
        resolved_end_offset = 0
    else:
        resolved_end_offset = 1

    _daily_partitions_def = DailyPartitionsDefinition(
        start_date=start_date,
        end_offset=resolved_end_offset,
        timezone=timezone,
    )
    _daily_partitions_tz = timezone
    return _daily_partitions_def


def get_hourly_partitions_def(
    include_current_hour_partition: bool | None = None,
    timezone: str | None = None,
) -> HourlyPartitionsDefinition:
    global _hourly_partitions_def, _hourly_partitions_tz
    if _hourly_partitions_def is not None and _hourly_partitions_tz == timezone:
        return _hourly_partitions_def
    start_date = os.getenv("DAGSTER_HOURLY_PARTITIONS_START_DATE")
    if not start_date:
        raise ValueError(
            "DAGSTER_HOURLY_PARTITIONS_START_DATE must be set (YYYY-MM-DD) when using hourly partitions"
        )

    # Resolve end_offset from boolean flag: parameter > default(include current hour)
    if include_current_hour_partition is False:
        resolved_end_offset = 0
    else:
        resolved_end_offset = 1

    _hourly_partitions_def = HourlyPartitionsDefinition(
        start_date=start_date,
        end_offset=resolved_end_offset,
        timezone=timezone,
    )
    _hourly_partitions_tz = timezone
    return _hourly_partitions_def


def get_monthly_partitions_def(
    include_current_month_partition: bool | None = None,
    timezone: str | None = None,
) -> MonthlyPartitionsDefinition:
    global _monthly_partitions_def, _monthly_partitions_tz
    if _monthly_partitions_def is not None and _monthly_partitions_tz == timezone:
        return _monthly_partitions_def
    start_date = os.getenv("DAGSTER_MONTHLY_PARTITIONS_START_DATE")
    if not start_date:
        raise ValueError(
            "DAGSTER_MONTHLY_PARTITIONS_START_DATE must be set (YYYY-MM-DD) when using monthly partitions"
        )

    # Resolve end_offset from boolean flag: parameter > default(include current month)
    if include_current_month_partition is False:
        resolved_end_offset = 0
    else:
        resolved_end_offset = 1

    _monthly_partitions_def = MonthlyPartitionsDefinition(
        start_date=start_date,
        end_offset=resolved_end_offset,
        timezone=timezone,
    )
    _monthly_partitions_tz = timezone
    return _monthly_partitions_def


def get_partitions_def(
    partition_spec: str | None,
    include_current_day_partition: bool | None = None,
    include_current_hour_partition: bool | None = None,
    include_current_month_partition: bool | None = None,
    timezone: str | None = None,
) -> PartitionsDefinition | None:
    """Resolve partition specification to PartitionsDefinition.
    
    Handles:
    - "daily" → DailyPartitionsDefinition
    - "hourly" → HourlyPartitionsDefinition
    - "monthly" → MonthlyPartitionsDefinition
    - None/"unpartitioned"/"" → None
    
    Args:
        partition_spec: Partition specification string
        include_current_day_partition: Whether today's partition is available (daily only)
        include_current_hour_partition: Whether the current hour's partition is available (hourly only)
        include_current_month_partition: Whether the current month's partition is available (monthly only)
        timezone: IANA timezone name for partition boundaries (e.g. 'Asia/Macau'). Defaults to UTC when None.
    
    Returns:
        PartitionsDefinition or None
    
    Raises:
        ValueError: If partition_spec is invalid
    """
    if partition_spec is None or partition_spec in {"none", "unpartitioned", ""}:
        return None
    
    if partition_spec == "daily":
        return get_daily_partitions_def(
            include_current_day_partition=include_current_day_partition,
            timezone=timezone,
        )

    if partition_spec == "hourly":
        return get_hourly_partitions_def(
            include_current_hour_partition=include_current_hour_partition,
            timezone=timezone,
        )

    if partition_spec == "monthly":
        return get_monthly_partitions_def(
            include_current_month_partition=include_current_month_partition,
            timezone=timezone,
        )
    
    raise ValueError(f"Unsupported partition spec: {partition_spec}")


def reset_daily_partitions_def() -> None:
    """Reset the cached DailyPartitionsDefinition. Useful for testing."""
    global _daily_partitions_def, _daily_partitions_tz
    _daily_partitions_def = None
    _daily_partitions_tz = None


def reset_hourly_partitions_def() -> None:
    """Reset the cached HourlyPartitionsDefinition. Useful for testing."""
    global _hourly_partitions_def, _hourly_partitions_tz
    _hourly_partitions_def = None
    _hourly_partitions_tz = None


def reset_monthly_partitions_def() -> None:
    """Reset the cached MonthlyPartitionsDefinition. Useful for testing."""
    global _monthly_partitions_def, _monthly_partitions_tz
    _monthly_partitions_def = None
    _monthly_partitions_tz = None
