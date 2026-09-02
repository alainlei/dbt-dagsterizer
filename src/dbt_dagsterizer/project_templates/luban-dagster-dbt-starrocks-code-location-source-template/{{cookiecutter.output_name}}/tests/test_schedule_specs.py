from dbt_dagsterizer.schedules.dbt_config import DBT_SCHEDULE_SPECS


def test_schedule_specs_include_partition_type():
    for spec in DBT_SCHEDULE_SPECS:
        assert spec.get("partition_type") in {"daily", "hourly", "monthly"}


def test_schedule_specs_have_compatible_partition_fields():
    for spec in DBT_SCHEDULE_SPECS:
        partition_type = spec.get("partition_type")
        if partition_type == "daily":
            assert "partition_offset_days" in spec
            assert "partition_lookback_days" in spec
            assert int(spec.get("partition_offset_hours", 0)) == 0
            assert int(spec.get("partition_lookback_hours", 0)) == 0
            assert int(spec.get("partition_offset_months", 0)) == 0
            assert int(spec.get("partition_lookback_months", 0)) == 0
        elif partition_type == "hourly":
            assert "partition_offset_hours" in spec
            assert "partition_lookback_hours" in spec
            assert int(spec.get("partition_offset_days", 0)) == 0
            assert int(spec.get("partition_lookback_days", 0)) == 0
            assert int(spec.get("partition_offset_months", 0)) == 0
            assert int(spec.get("partition_lookback_months", 0)) == 0
        elif partition_type == "monthly":
            assert "partition_offset_months" in spec
            assert "partition_lookback_months" in spec
            assert int(spec.get("partition_offset_days", 0)) == 0
            assert int(spec.get("partition_lookback_days", 0)) == 0
            assert int(spec.get("partition_offset_hours", 0)) == 0
            assert int(spec.get("partition_lookback_hours", 0)) == 0
        else:
            raise AssertionError(f"Unsupported partition_type: {partition_type}")


def test_schedule_specs_include_dedupe_across_ticks():
    for spec in DBT_SCHEDULE_SPECS:
        assert isinstance(spec.get("dedupe_across_ticks"), bool)
