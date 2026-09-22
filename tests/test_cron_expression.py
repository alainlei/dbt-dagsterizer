"""Tests for cron expression validation in dbt_dagsterizer.cron."""

from __future__ import annotations

import pytest


def test_validate_accepts_common_expressions():
    from dbt_dagsterizer.cron import validate_cron_expression

    for expression in (
        "* * * * *",
        "*/15 * * * *",
        "0 3 * * *",
        "0 9-17 * * 1-5",
        "0 0,12 * * *",
        "0 0 1 */2 *",
        "30 2 15 * *",
        "0 0 * * MON",
        "0 0 * JAN *",
        "0 0 L * *",
        "0 0 * * 5#2",
        "0 0 ? * *",
        "0 0 * * ?",
        "5/10 * * * *",
        "0 0 * * 7",
    ):
        assert validate_cron_expression(expression) == expression


def test_validate_normalizes_whitespace():
    from dbt_dagsterizer.cron import validate_cron_expression

    assert validate_cron_expression("  0   0  *  *  * ") == "0 0 * * *"


def test_validate_accepts_macros():
    from dbt_dagsterizer.cron import validate_cron_expression

    assert validate_cron_expression("@daily") == "@daily"
    assert validate_cron_expression("@hourly") == "@hourly"
    assert validate_cron_expression("@monthly") == "@monthly"


def test_validate_rejects_empty_expression():
    from dbt_dagsterizer.cron import validate_cron_expression

    with pytest.raises(ValueError, match="non-empty"):
        validate_cron_expression("   ")


def test_validate_rejects_wrong_field_count():
    from dbt_dagsterizer.cron import validate_cron_expression

    with pytest.raises(ValueError, match="exactly 5"):
        validate_cron_expression("0 0 * *")
    with pytest.raises(ValueError, match="exactly 5"):
        validate_cron_expression("0 0 0 * * 2024")


def test_validate_rejects_unsupported_macro():
    from dbt_dagsterizer.cron import validate_cron_expression

    with pytest.raises(ValueError, match="unsupported cron macro"):
        validate_cron_expression("@reboot")


def test_validate_rejects_out_of_range_minute():
    from dbt_dagsterizer.cron import validate_cron_expression

    with pytest.raises(ValueError, match="minute field"):
        validate_cron_expression("60 * * * *")


def test_validate_rejects_out_of_range_hour():
    from dbt_dagsterizer.cron import validate_cron_expression

    with pytest.raises(ValueError, match="hour field"):
        validate_cron_expression("0 24 * * *")


def test_validate_rejects_out_of_range_day_of_month():
    from dbt_dagsterizer.cron import validate_cron_expression

    with pytest.raises(ValueError, match="day-of-month field"):
        validate_cron_expression("0 0 0 * *")
    with pytest.raises(ValueError, match="day-of-month field"):
        validate_cron_expression("0 0 32 * *")


def test_validate_rejects_out_of_range_month():
    from dbt_dagsterizer.cron import validate_cron_expression

    with pytest.raises(ValueError, match="month field"):
        validate_cron_expression("0 0 * 13 *")
    with pytest.raises(ValueError, match="month field"):
        validate_cron_expression("0 0 * 0 *")


def test_validate_rejects_out_of_range_day_of_week():
    from dbt_dagsterizer.cron import validate_cron_expression

    with pytest.raises(ValueError, match="day-of-week field"):
        validate_cron_expression("0 0 * * 8")


def test_validate_rejects_unknown_names():
    from dbt_dagsterizer.cron import validate_cron_expression

    with pytest.raises(ValueError, match="month field"):
        validate_cron_expression("0 0 * FOO *")
    with pytest.raises(ValueError, match="day-of-week field"):
        validate_cron_expression("0 0 * * FUNDAY")


def test_validate_rejects_zero_step():
    from dbt_dagsterizer.cron import validate_cron_expression

    with pytest.raises(ValueError, match="invalid step"):
        validate_cron_expression("*/0 * * * *")


def test_validate_rejects_inverted_range():
    from dbt_dagsterizer.cron import validate_cron_expression

    with pytest.raises(ValueError, match="inverted range"):
        validate_cron_expression("0 5-2 * * *")


def test_validate_rejects_february_30():
    """Dagster rejects schedules that can never fire."""
    from dbt_dagsterizer.cron import validate_cron_expression

    with pytest.raises(ValueError, match="never fires"):
        validate_cron_expression("0 0 30 2 *")
    with pytest.raises(ValueError, match="never fires"):
        validate_cron_expression("0 0 31 2 *")


def test_validate_allows_30th_in_other_months():
    from dbt_dagsterizer.cron import validate_cron_expression

    assert validate_cron_expression("0 0 30 4 *") == "0 0 30 4 *"


def test_validate_rejects_last_day_in_month_field():
    from dbt_dagsterizer.cron import validate_cron_expression

    with pytest.raises(ValueError, match="'L' is only supported in the day-of-month field"):
        validate_cron_expression("0 0 * L *")


def test_validate_rejects_question_mark_in_minute_field():
    from dbt_dagsterizer.cron import validate_cron_expression

    with pytest.raises(ValueError, match="is only supported in the day-of-month"):
        validate_cron_expression("? * * * *")


def test_validate_rejects_nth_weekday_in_month_field():
    from dbt_dagsterizer.cron import validate_cron_expression

    with pytest.raises(ValueError, match="not supported in the month field"):
        validate_cron_expression("0 0 * 1#2 *")


def test_validate_rejects_out_of_range_nth_weekday():
    from dbt_dagsterizer.cron import validate_cron_expression

    with pytest.raises(ValueError, match="weekday#1..weekday#5"):
        validate_cron_expression("0 0 * * 5#6")


def test_validate_rejects_weekday_suffix():
    """'5L' is not a supported day-of-week token."""
    from dbt_dagsterizer.cron import validate_cron_expression

    with pytest.raises(ValueError, match="day-of-week field"):
        validate_cron_expression("0 0 * * 5L")


def test_validate_rejects_empty_list_item():
    from dbt_dagsterizer.cron import validate_cron_expression

    with pytest.raises(ValueError, match="empty value"):
        validate_cron_expression("0,,1 * * * *")


def test_validate_rejects_non_string():
    from dbt_dagsterizer.cron import validate_cron_expression

    with pytest.raises(ValueError, match="must be a string"):
        validate_cron_expression(None)


# --- warnings ---


def test_warnings_flag_oversized_step():
    """Dagster accepts */90 but fires more often than the step suggests."""
    from dbt_dagsterizer.cron import cron_expression_warnings

    warnings = cron_expression_warnings("*/90 * * * *")
    assert len(warnings) == 1
    assert "minute" in warnings[0]
    assert "step of 90" in warnings[0]


def test_warnings_empty_for_normal_expression():
    from dbt_dagsterizer.cron import cron_expression_warnings

    assert cron_expression_warnings("*/15 * * * *") == []
    assert cron_expression_warnings("0 0 * * *") == []
    assert cron_expression_warnings("0 0 1 * *") == []


def test_warnings_empty_for_macros_and_junk():
    from dbt_dagsterizer.cron import cron_expression_warnings

    assert cron_expression_warnings("@daily") == []
    assert cron_expression_warnings("not a cron") == []
    assert cron_expression_warnings("") == []
