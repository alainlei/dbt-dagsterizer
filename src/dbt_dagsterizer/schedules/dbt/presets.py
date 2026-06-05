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
        "enabled": enabled,
        "dedupe_across_ticks": dedupe_across_ticks,
    }
