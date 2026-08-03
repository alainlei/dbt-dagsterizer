from __future__ import annotations

import os

from dagster import DailyPartitionsDefinition, PartitionsDefinition

_daily_partitions_def = None


def get_daily_partitions_def(include_current_day_partition: bool | None = None) -> DailyPartitionsDefinition:
    global _daily_partitions_def
    if _daily_partitions_def is not None:
        return _daily_partitions_def
    start_date = os.getenv("DAGSTER_DAILY_PARTITIONS_START_DATE")
    if not start_date:
        raise ValueError(
            "DAGSTER_DAILY_PARTITIONS_START_DATE must be set (YYYY-MM-DD) when using daily partitions"
        )

    # Resolve end_offset from boolean flag: parameter > default(0)
    if include_current_day_partition:
        resolved_end_offset = 1
    else:
        resolved_end_offset = 0

    _daily_partitions_def = DailyPartitionsDefinition(
        start_date=start_date,
        end_offset=resolved_end_offset,
    )
    return _daily_partitions_def


def get_partitions_def(
    partition_spec: str | None,
    include_current_day_partition: bool | None = None,
) -> PartitionsDefinition | None:
    """Resolve partition specification to PartitionsDefinition.
    
    Handles:
    - "daily" → DailyPartitionsDefinition
    - None/"unpartitioned"/"" → None
    
    Args:
        partition_spec: Partition specification string
    
    Returns:
        PartitionsDefinition or None
    
    Raises:
        ValueError: If partition_spec is invalid
    """
    if partition_spec is None or partition_spec in {"none", "unpartitioned", ""}:
        return None
    
    if partition_spec == "daily":
        return get_daily_partitions_def(include_current_day_partition=include_current_day_partition)
    
    raise ValueError(f"Unsupported partition spec: {partition_spec}")


def reset_daily_partitions_def() -> None:
    """Reset the cached DailyPartitionsDefinition. Useful for testing."""
    global _daily_partitions_def
    _daily_partitions_def = None