from datetime import datetime, timedelta


def _dbt_partition_vars_from_time_window(start: datetime, end: datetime) -> dict[str, str]:
    min_date = start.date().strftime("%Y-%m-%d")
    max_date = end.date().strftime("%Y-%m-%d")
    min_datetime = start.strftime("%Y-%m-%d %H:%M:%S")
    max_datetime = end.strftime("%Y-%m-%d %H:%M:%S")

    return {
        "min_date": min_date,
        "max_date": max_date,
        "min_datetime": min_datetime,
        "max_datetime": max_datetime,
    }


def _default_daily_window_vars() -> dict[str, str]:
    # Fallback window for manual/non-partitioned Dagster runs against daily-partitioned assets.
    start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return _dbt_partition_vars_from_time_window(start, end)


def _get_dbt_vars_for_context(context) -> dict[str, str] | None:
    """Get dbt vars based on partition type.
    
    For time-window partitions (daily, hourly, etc.):
        Returns {min_date, max_date, min_datetime, max_datetime}
    
    For dynamic partitions:
        Returns {partition_key: <key>}
    """
    # Try to get time window FIRST for time-based partitions (daily, hourly, monthly)
    # IMPORTANT: Must catch exception because Dagster raises DagsterInvariantViolationError
    # when partitions_def is not defined
    try:
        time_window = context.partition_time_window
    except Exception:
        time_window = None
    
    if time_window is not None:
        # Time-based partition (daily, hourly, etc.) - return date/datetime window
        return _dbt_partition_vars_from_time_window(time_window.start, time_window.end)
    
    # Check if this is a dynamic partition by trying to get partition_key
    # IMPORTANT: Must catch the exception, not use getattr(), because Dagster
    # raises DagsterInvariantViolationError for non-partitioned runs
    partition_key = None
    try:
        partition_key = context.partition_key
    except Exception:
        # Not a partitionized run, or partition_key not available
        pass
    
    if partition_key is not None:
        # Dynamic partition - return the partition key
        return {"partition_key": partition_key}
    
    # Fallback for non-partitioned runs
    return _default_daily_window_vars()
