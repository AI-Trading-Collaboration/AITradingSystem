from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Literal

import pandas as pd

TIMING_CONTRACT: dict[str, str] = {
    "raw_market_data_date": "t",
    "feature_snapshot_date": "t",
    "signal_date": "t",
    "allocation_decision_date": "t",
    "earliest_execution_date": "next trading date after t",
    "portfolio_return_window": "execution_date to a later return_date",
    "future_evaluation_fields": "evaluation_only=true",
}

FEATURE_SOURCE_DATE_FIELDS = (
    "feature_source_date",
    "feature_snapshot_date",
    "raw_market_data_date",
    "source_date",
)
SIGNAL_DATE_FIELDS = (
    "signal_date",
    "decision_date",
    "allocation_decision_date",
    "date",
)
EXECUTION_DATE_FIELDS = (
    "execution_date",
    "return_date",
    "executed_at",
    "trade_date",
)
DECISION_PAYLOAD_FIELDS = (
    "decision_payload",
    "decision",
    "allocation_decision",
    "signal_payload",
    "target_payload",
)
EVALUATION_ONLY_FIELD_PREFIXES = (
    "forward_return",
    "future_return",
    "max_drawdown_next",
    "relative_return_vs",
    "portfolio_relative_return_vs",
    "weight_contribution",
    "portfolio_return",
    "signal_hit",
)
REPORT_EVALUATION_ONLY_PATTERNS = (
    "forward_return",
    "future_return",
    "forward return",
    "future return",
    "max_drawdown_next",
    "max drawdown next",
    "relative_return_vs",
    "weight_contribution",
    "portfolio_return_",
    "signal_hit",
)
SIMULATION_SECTION_MARKERS = ("simulation performance",)

RecordInput = pd.DataFrame | Mapping[str, object] | list[Mapping[str, object]] | None


@dataclass(frozen=True)
class NoLookaheadIssue:
    severity: Literal["ERROR", "WARNING"]
    code: str
    message: str
    scope: str
    row: int | None = None
    field: str | None = None
    sample: str = ""


@dataclass(frozen=True)
class NoLookaheadValidationResult:
    checked_at: datetime
    status: Literal["PASS", "FAIL"]
    timing_contract: dict[str, str]
    issues: tuple[NoLookaheadIssue, ...] = ()

    @property
    def passed(self) -> bool:
        return not any(issue.severity == "ERROR" for issue in self.issues)

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == "ERROR")


def validate_no_lookahead_records(
    *,
    feature_snapshots: RecordInput = None,
    signal_records: RecordInput = None,
    allocation_records: RecordInput = None,
    trade_records: RecordInput = None,
    simulation_records: RecordInput = None,
    backtest_records: RecordInput = None,
    report_records: RecordInput = None,
    report_markdown: str | None = None,
) -> NoLookaheadValidationResult:
    issues: list[NoLookaheadIssue] = []
    for scope, records in (
        ("feature_snapshots", feature_snapshots),
        ("signal_records", signal_records),
        ("allocation_records", allocation_records),
        ("trade_records", trade_records),
        ("simulation_records", simulation_records),
        ("backtest_records", backtest_records),
        ("report_records", report_records),
    ):
        issues.extend(_validate_record_set(scope, records))
    if report_markdown is not None:
        issues.extend(_validate_daily_brief_markdown(report_markdown))
    return _result(issues)


def validate_daily_brief_no_lookahead(markdown: str) -> NoLookaheadValidationResult:
    return _result(_validate_daily_brief_markdown(markdown))


def raise_for_no_lookahead_violations(result: NoLookaheadValidationResult) -> None:
    if result.passed:
        return
    details = "; ".join(
        f"{issue.scope}:{issue.code}:{issue.field or 'record'}" for issue in result.issues[:5]
    )
    raise ValueError(f"ETF no-lookahead validation failed: {details}")


def is_evaluation_only_field_name(name: object) -> bool:
    normalized = _normalize_key(str(name))
    return any(
        normalized == prefix or normalized.startswith(f"{prefix}_")
        for prefix in EVALUATION_ONLY_FIELD_PREFIXES
    )


def evaluation_only_columns(columns: list[object] | pd.Index) -> tuple[str, ...]:
    return tuple(str(column) for column in columns if is_evaluation_only_field_name(column))


def _validate_record_set(scope: str, records: RecordInput) -> list[NoLookaheadIssue]:
    issues: list[NoLookaheadIssue] = []
    for row_number, record in enumerate(_to_records(records), start=1):
        signal_date = _first_date(record, SIGNAL_DATE_FIELDS)
        execution_date = _first_date(record, EXECUTION_DATE_FIELDS)
        if signal_date is not None and execution_date is not None:
            if execution_date <= signal_date:
                issues.append(
                    NoLookaheadIssue(
                        severity="ERROR",
                        code="execution_date_not_after_signal_date",
                        message=(
                            "ETF execution date must be strictly after signal date "
                            f"({execution_date.isoformat()} <= {signal_date.isoformat()})."
                        ),
                        scope=scope,
                        row=row_number,
                        field="execution_date",
                    )
                )
        explicit_execution_date = _date_from_record(record, "execution_date")
        return_date = _date_from_record(record, "return_date")
        if explicit_execution_date is not None and return_date is not None:
            if return_date <= explicit_execution_date:
                issues.append(
                    NoLookaheadIssue(
                        severity="ERROR",
                        code="return_date_not_after_execution_date",
                        message=(
                            "ETF return date must be strictly after execution date "
                            f"({return_date.isoformat()} <= "
                            f"{explicit_execution_date.isoformat()})."
                        ),
                        scope=scope,
                        row=row_number,
                        field="return_date",
                    )
                )

        for field in FEATURE_SOURCE_DATE_FIELDS:
            feature_date = _date_from_record(record, field)
            if signal_date is None or feature_date is None:
                continue
            if feature_date > signal_date:
                issues.append(
                    NoLookaheadIssue(
                        severity="ERROR",
                        code="feature_source_date_after_signal_date",
                        message=(
                            "ETF feature source date must not be after signal date "
                            f"({feature_date.isoformat()} > {signal_date.isoformat()})."
                        ),
                        scope=scope,
                        row=row_number,
                        field=field,
                    )
                )

        issues.extend(_decision_payload_issues(scope, row_number, record))
        issues.extend(_evaluation_field_marker_issues(scope, row_number, record))
    return issues


def _decision_payload_issues(
    scope: str,
    row_number: int,
    record: Mapping[str, object],
) -> list[NoLookaheadIssue]:
    issues: list[NoLookaheadIssue] = []
    for field in DECISION_PAYLOAD_FIELDS:
        if field not in record:
            continue
        paths = _evaluation_field_paths(record[field], base_path=field)
        for path in paths:
            issues.append(
                NoLookaheadIssue(
                    severity="ERROR",
                    code="future_field_in_decision_payload",
                    message="ETF decision payload must not contain future evaluation fields.",
                    scope=scope,
                    row=row_number,
                    field=path,
                    sample=_sample_value(record[field]),
                )
            )
    return issues


def _evaluation_field_marker_issues(
    scope: str,
    row_number: int,
    record: Mapping[str, object],
) -> list[NoLookaheadIssue]:
    if _truthy(record.get("evaluation_only")):
        return []
    issues: list[NoLookaheadIssue] = []
    for field, value in record.items():
        if not is_evaluation_only_field_name(field):
            continue
        if not _has_non_null_value(value):
            continue
        issues.append(
            NoLookaheadIssue(
                severity="ERROR",
                code="future_field_without_evaluation_only_marker",
                message=(
                    "ETF future evaluation fields are allowed only when " "evaluation_only=true."
                ),
                scope=scope,
                row=row_number,
                field=str(field),
                sample=_sample_value(value),
            )
        )
    return issues


def _validate_daily_brief_markdown(markdown: str) -> list[NoLookaheadIssue]:
    decision_text = extract_daily_brief_decision_text(markdown)
    issues: list[NoLookaheadIssue] = []
    for pattern in _report_evaluation_matches(decision_text):
        issues.append(
            NoLookaheadIssue(
                severity="ERROR",
                code="report_decision_contains_evaluation_only_field",
                message=(
                    "ETF daily brief decision sections must not include "
                    "evaluation-only future fields."
                ),
                scope="report_markdown",
                field=pattern,
            )
        )
    return issues


def extract_daily_brief_decision_text(markdown: str) -> str:
    heading_matches = list(re.finditer(r"(?m)^##\s+(.+)$", markdown))
    if not heading_matches:
        return markdown
    chunks = [markdown[: heading_matches[0].start()]]
    for index, match in enumerate(heading_matches):
        heading = match.group(1).strip()
        start = match.start()
        end = (
            heading_matches[index + 1].start()
            if index + 1 < len(heading_matches)
            else len(markdown)
        )
        if any(marker in heading.lower() for marker in SIMULATION_SECTION_MARKERS):
            continue
        chunks.append(markdown[start:end])
    return "\n".join(chunks)


def _evaluation_field_paths(value: object, *, base_path: str) -> list[str]:
    parsed = _maybe_parse_json(value)
    if isinstance(parsed, Mapping):
        paths: list[str] = []
        for key, nested in parsed.items():
            path = f"{base_path}.{key}"
            if is_evaluation_only_field_name(key):
                paths.append(path)
            paths.extend(_evaluation_field_paths(nested, base_path=path))
        return paths
    if isinstance(parsed, list):
        paths = []
        for index, nested in enumerate(parsed):
            paths.extend(_evaluation_field_paths(nested, base_path=f"{base_path}[{index}]"))
        return paths
    return []


def _to_records(records: RecordInput) -> list[dict[str, object]]:
    if records is None:
        return []
    if isinstance(records, pd.DataFrame):
        return [dict(row) for row in records.to_dict(orient="records")]
    if isinstance(records, Mapping):
        return [dict(records)]
    return [dict(record) for record in records]


def _first_date(record: Mapping[str, object], fields: tuple[str, ...]) -> date | None:
    for field in fields:
        parsed = _date_from_record(record, field)
        if parsed is not None:
            return parsed
    return None


def _date_from_record(record: Mapping[str, object], field: str) -> date | None:
    if field not in record:
        return None
    return _parse_date(record[field])


def _parse_date(value: object) -> date | None:
    if _is_missing(value):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def _maybe_parse_json(value: object) -> object:
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text or text[0] not in "[{":
        return value
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return value


def _report_evaluation_matches(text: str) -> tuple[str, ...]:
    lower = text.lower()
    return tuple(pattern for pattern in REPORT_EVALUATION_ONLY_PATTERNS if pattern in lower)


def _normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if _is_missing(value):
        return False
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _has_non_null_value(value: object) -> bool:
    if isinstance(value, Mapping):
        return any(_has_non_null_value(item) for item in value.values())
    if isinstance(value, list):
        return any(_has_non_null_value(item) for item in value)
    return not _is_missing(value)


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip().lower() in {"", "nan", "nat", "none", "null"}:
        return True
    try:
        result = pd.isna(value)
    except (TypeError, ValueError):
        return False
    try:
        return bool(result)
    except (TypeError, ValueError):
        return False


def _sample_value(value: object) -> str:
    text = str(value)
    return text if len(text) <= 120 else f"{text[:117]}..."


def _result(issues: list[NoLookaheadIssue]) -> NoLookaheadValidationResult:
    return NoLookaheadValidationResult(
        checked_at=datetime.now(UTC),
        status="FAIL" if any(issue.severity == "ERROR" for issue in issues) else "PASS",
        timing_contract=dict(TIMING_CONTRACT),
        issues=tuple(issues),
    )
