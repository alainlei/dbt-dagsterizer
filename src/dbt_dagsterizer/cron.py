"""Validation for user-supplied cron expressions.

Dagster only accepts standard five-field crontab expressions, so the CLI and the
orchestration config enforce the same rules before a schedule is written. Keeping
the checks here (instead of relying on Dagster at definition-build time) lets
`meta schedule --cron-expression` and `meta validate` fail fast with a message
that names the offending field.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping

_MONTH_NAMES = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}

_DAY_OF_WEEK_NAMES = {
    "sun": 0,
    "mon": 1,
    "tue": 2,
    "wed": 3,
    "thu": 4,
    "fri": 5,
    "sat": 6,
}

# Macros Dagster (croniter) understands. They are validated but never expanded,
# so a config keeps whatever the user wrote.
_CRON_MACROS = frozenset(
    {"@yearly", "@annually", "@monthly", "@weekly", "@daily", "@midnight", "@hourly"}
)


@dataclass(frozen=True)
class _FieldSpec:
    label: str
    minimum: int
    maximum: int
    names: Mapping[str, int] = field(default_factory=dict)
    allows_last_day_of_month: bool = False
    allows_question_mark: bool = False
    allows_nth_weekday: bool = False


_FIELDS = (
    _FieldSpec("minute", 0, 59),
    _FieldSpec("hour", 0, 23),
    _FieldSpec("day-of-month", 1, 31, allows_last_day_of_month=True, allows_question_mark=True),
    _FieldSpec("month", 1, 12, names=_MONTH_NAMES),
    _FieldSpec(
        "day-of-week",
        0,
        7,
        names=_DAY_OF_WEEK_NAMES,
        allows_question_mark=True,
        allows_nth_weekday=True,
    ),
)


def _normalize(expression: str) -> str:
    if not isinstance(expression, str):
        raise ValueError("cron expression must be a string")
    return " ".join(expression.split())


def _parse_value(token: str, spec: _FieldSpec) -> int:
    if not token:
        raise ValueError(f"the {spec.label} field of the cron expression is missing a value")
    name = spec.names.get(token)
    if name is not None:
        return name
    if not token.isdigit():
        raise ValueError(f"unexpected value '{token}' in the {spec.label} field of the cron expression")
    value = int(token)
    if value < spec.minimum or value > spec.maximum:
        raise ValueError(
            f"value '{token}' in the {spec.label} field of the cron expression is outside "
            f"the allowed range {spec.minimum}..{spec.maximum}"
        )
    return value


def _parse_token(token: str, spec: _FieldSpec) -> list[int]:
    """Expand one comma-separated token, raising ValueError when it is malformed.

    Symbolic tokens ('L', 'weekday#nth') have no fixed numeric value and expand
    to an empty list; they are validated but not enumerated.
    """
    if not token:
        raise ValueError(f"the {spec.label} field of the cron expression contains an empty value")
    lower = token.lower()

    if lower == "*":
        return list(range(spec.minimum, spec.maximum + 1))
    if lower == "?":
        if not spec.allows_question_mark:
            raise ValueError(
                f"'?' is only supported in the day-of-month and day-of-week fields, "
                f"not in the {spec.label} field"
            )
        return list(range(spec.minimum, spec.maximum + 1))
    if lower == "l":
        if not spec.allows_last_day_of_month:
            raise ValueError(f"'L' is only supported in the day-of-month field, not in the {spec.label} field")
        return []
    if "#" in lower:
        if not spec.allows_nth_weekday:
            raise ValueError(f"'{token}' is not supported in the {spec.label} field")
        weekday_text, _, nth_text = lower.partition("#")
        _parse_value(weekday_text, spec)
        if not nth_text.isdigit() or not 1 <= int(nth_text) <= 5:
            raise ValueError(
                f"nth-weekday token '{token}' in the day-of-week field must look like weekday#1..weekday#5"
            )
        return []

    body, step_separator, step_text = lower.partition("/")
    step = 1
    if step_separator:
        if not step_text.isdigit() or int(step_text) < 1:
            raise ValueError(f"invalid step '{step_text}' in the {spec.label} field of the cron expression")
        step = int(step_text)

    if body == "*":
        return list(range(spec.minimum, spec.maximum + 1, step))
    if "-" in body:
        start_text, _, end_text = body.partition("-")
        start = _parse_value(start_text, spec)
        end = _parse_value(end_text, spec)
        if start > end:
            raise ValueError(f"inverted range '{token}' in the {spec.label} field of the cron expression")
        return list(range(start, end + 1, step))

    start = _parse_value(body, spec)
    if step_separator:
        # "5/10" means "from 5 up to the end of the range, every 10".
        return list(range(start, spec.maximum + 1, step))
    return [start]


def _parse_field(raw_field: str, spec: _FieldSpec) -> list[int]:
    values: set[int] = set()
    for token in raw_field.split(","):
        values.update(_parse_token(token.strip(), spec))
    return sorted(values)


def validate_cron_expression(expression: str) -> str:
    """Validate a cron expression, returning its whitespace-normalized form.

    Accepts the five-field crontab syntax Dagster accepts (step values, ranges,
    lists, month/weekday names, 'L'/'#' day tokens and '@daily'-style macros) and
    raises ValueError naming the offending field for anything else.
    """
    normalized = _normalize(expression)
    if not normalized:
        raise ValueError("cron expression must be non-empty")

    if normalized.startswith("@"):
        if normalized.lower() not in _CRON_MACROS:
            raise ValueError(f"unsupported cron macro '{normalized}'")
        return normalized

    fields = normalized.split(" ")
    if len(fields) != 5:
        raise ValueError(
            "cron expression must have exactly 5 space-separated fields "
            f"(minute hour day-of-month month day-of-week): '{normalized}'"
        )

    for raw_field, spec in zip(fields, _FIELDS):
        for token in raw_field.split(","):
            _parse_token(token.strip(), spec)

    # Dagster rejects schedules that can never fire, e.g. February 30th.
    if fields[2] in {"30", "31"} and _parse_field(fields[3], _FIELDS[3]) == [2]:
        raise ValueError(f"cron expression '{normalized}' never fires: February has no {fields[2]}th day")

    return normalized


def cron_expression_warnings(expression: str) -> list[str]:
    """Warnings for expressions Dagster accepts but silently normalizes.

    A step larger than the field range (e.g. '*/90 * * * *') makes Dagster fire
    more often than the step suggests, which is a common source of surprise.
    """
    normalized = _normalize(expression)
    if not normalized or normalized.startswith("@"):
        return []
    fields = normalized.split(" ")
    if len(fields) != 5:
        return []

    warnings: list[str] = []
    for raw_field, spec in zip(fields, _FIELDS):
        for token in raw_field.split(","):
            _, step_separator, step_text = token.partition("/")
            if not step_separator or not step_text.isdigit():
                continue
            if int(step_text) > spec.maximum - spec.minimum:
                warnings.append(
                    f"cron expression '{normalized}' uses a {spec.label} step of {step_text}, which is "
                    f"larger than the {spec.minimum}..{spec.maximum} field range; Dagster normalizes it "
                    "and the schedule may fire more often than expected"
                )
    return warnings
