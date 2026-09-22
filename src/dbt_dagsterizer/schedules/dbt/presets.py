from ...cron import validate_cron_expression


def daily_at(
    *,
    name: str,
    job_name: str,
    hour: int,
    minute: int,
    lookback_days: int = 0,
    offset_days: int = 1,
    enabled: bool = True,
    dedupe_across_ticks: bool = True,
    timezone: str = "UTC",
):
    if not name:
        raise ValueError("Schedule name must be non-empty")
    if not job_name:
        raise ValueError("job_name must be non-empty")
    if hour < 0 or hour > 23:
        raise ValueError("hour must be 0..23")
    if minute < 0 or minute > 59:
        raise ValueError("minute must be 0..59")
    if lookback_days < 0:
        raise ValueError("lookback_days must be >= 0")
    if offset_days < 0:
        raise ValueError("offset_days must be >= 0")

    cron = f"{minute} {hour} * * *"
    return {
        "name": name,
        "cron_schedule": cron,
        "job_name": job_name,
        "partition_type": "daily",
        "partition_offset_days": offset_days,
        "partition_lookback_days": lookback_days,
        "partition_offset_hours": 0,
        "partition_lookback_hours": 0,
        "partition_offset_months": 0,
        "partition_lookback_months": 0,
        "enabled": enabled,
        "dedupe_across_ticks": dedupe_across_ticks,
        "timezone": timezone,
    }


def hourly_at(
    *,
    name: str,
    job_name: str,
    minute: int = 0,
    lookback_hours: int = 0,
    offset_hours: int = 1,
    enabled: bool = True,
    dedupe_across_ticks: bool = True,
    timezone: str = "UTC",
):
    if not name:
        raise ValueError("Schedule name must be non-empty")
    if not job_name:
        raise ValueError("job_name must be non-empty")
    if minute < 0 or minute > 59:
        raise ValueError("minute must be 0..59")
    if lookback_hours < 0:
        raise ValueError("lookback_hours must be >= 0")
    if offset_hours < 0:
        raise ValueError("offset_hours must be >= 0")

    cron = f"{minute} * * * *"
    return {
        "name": name,
        "cron_schedule": cron,
        "job_name": job_name,
        "partition_type": "hourly",
        "partition_offset_days": 0,
        "partition_lookback_days": 0,
        "partition_offset_hours": offset_hours,
        "partition_lookback_hours": lookback_hours,
        "partition_offset_months": 0,
        "partition_lookback_months": 0,
        "enabled": enabled,
        "dedupe_across_ticks": dedupe_across_ticks,
        "timezone": timezone,
    }


def monthly_at(
    *,
    name: str,
    job_name: str,
    hour: int,
    minute: int,
    day_of_month: int = 1,
    lookback_months: int = 0,
    offset_months: int = 1,
    enabled: bool = True,
    dedupe_across_ticks: bool = True,
    timezone: str = "UTC",
):
    if not name:
        raise ValueError("Schedule name must be non-empty")
    if not job_name:
        raise ValueError("job_name must be non-empty")
    if hour < 0 or hour > 23:
        raise ValueError("hour must be 0..23")
    if minute < 0 or minute > 59:
        raise ValueError("minute must be 0..59")
    # cron never fires on a 29th-31st during a shorter month, so the schedule would silently stall.
    if day_of_month < 1 or day_of_month > 28:
        raise ValueError("day_of_month must be 1..28")
    if lookback_months < 0:
        raise ValueError("lookback_months must be >= 0")
    if offset_months < 0:
        raise ValueError("offset_months must be >= 0")

    cron = f"{minute} {hour} {day_of_month} * *"
    return {
        "name": name,
        "cron_schedule": cron,
        "job_name": job_name,
        "partition_type": "monthly",
        "partition_offset_days": 0,
        "partition_lookback_days": 0,
        "partition_offset_hours": 0,
        "partition_lookback_hours": 0,
        "partition_offset_months": offset_months,
        "partition_lookback_months": lookback_months,
        "enabled": enabled,
        "dedupe_across_ticks": dedupe_across_ticks,
        "timezone": timezone,
    }


def _reject_foreign_granularity_fields(
    *,
    name: str,
    partition_type: str,
    groups: dict[str, tuple[int | None, int | None]],
) -> None:
    """Reject offset/lookback values that belong to another partition granularity."""
    for group, values in groups.items():
        if any(value for value in values):
            raise ValueError(
                f"{partition_type.capitalize()} cron schedule '{name}' cannot set {group} offset/lookback fields"
            )


def cron(
    *,
    name: str,
    job_name: str,
    cron_expression: str,
    partition_type: str = "daily",
    lookback_days: int | None = None,
    offset_days: int | None = None,
    lookback_hours: int | None = None,
    offset_hours: int | None = None,
    lookback_months: int | None = None,
    offset_months: int | None = None,
    enabled: bool = True,
    dedupe_across_ticks: bool = True,
    timezone: str = "UTC",
):
    """Schedule that fires on an arbitrary cron expression.

    The partition window is derived from the tick time exactly like
    daily_at/hourly_at/monthly_at, so a cron such as "*/15 * * * *" can still target
    whole partitions. Only the offset/lookback group matching partition_type is kept;
    the fields of the other granularities must stay unset or zero.
    """
    if not name:
        raise ValueError("Schedule name must be non-empty")
    if not job_name:
        raise ValueError("job_name must be non-empty")
    if partition_type not in {"daily", "hourly", "monthly", "unpartitioned"}:
        raise ValueError("partition_type must be one of daily|hourly|monthly|unpartitioned")

    for label, value in (
        ("lookback_days", lookback_days),
        ("offset_days", offset_days),
        ("lookback_hours", lookback_hours),
        ("offset_hours", offset_hours),
        ("lookback_months", lookback_months),
        ("offset_months", offset_months),
    ):
        if value is not None and value < 0:
            raise ValueError(f"{label} must be >= 0")

    cron_schedule = validate_cron_expression(cron_expression)

    # Fields of granularities the cron schedule does not target stay zero so the
    # factory never sees a window it cannot honor.
    partitions = {
        "partition_offset_days": 0,
        "partition_lookback_days": 0,
        "partition_offset_hours": 0,
        "partition_lookback_hours": 0,
        "partition_offset_months": 0,
        "partition_lookback_months": 0,
    }

    if partition_type == "daily":
        _reject_foreign_granularity_fields(
            name=name,
            partition_type=partition_type,
            groups={"hourly": (lookback_hours, offset_hours), "monthly": (lookback_months, offset_months)},
        )
        partitions["partition_offset_days"] = 1 if offset_days is None else offset_days
        partitions["partition_lookback_days"] = 0 if lookback_days is None else lookback_days
    elif partition_type == "hourly":
        _reject_foreign_granularity_fields(
            name=name,
            partition_type=partition_type,
            groups={"daily": (lookback_days, offset_days), "monthly": (lookback_months, offset_months)},
        )
        partitions["partition_offset_hours"] = 1 if offset_hours is None else offset_hours
        partitions["partition_lookback_hours"] = 0 if lookback_hours is None else lookback_hours
    elif partition_type == "monthly":
        _reject_foreign_granularity_fields(
            name=name,
            partition_type=partition_type,
            groups={"daily": (lookback_days, offset_days), "hourly": (lookback_hours, offset_hours)},
        )
        partitions["partition_offset_months"] = 1 if offset_months is None else offset_months
        partitions["partition_lookback_months"] = 0 if lookback_months is None else lookback_months
    else:
        _reject_foreign_granularity_fields(
            name=name,
            partition_type=partition_type,
            groups={
                "daily": (lookback_days, offset_days),
                "hourly": (lookback_hours, offset_hours),
                "monthly": (lookback_months, offset_months),
            },
        )

    return {
        "name": name,
        "cron_schedule": cron_schedule,
        "job_name": job_name,
        "partition_type": partition_type,
        **partitions,
        "enabled": enabled,
        "dedupe_across_ticks": dedupe_across_ticks,
        "timezone": timezone,
    }
