from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path

import pandas as pd

from ai_trading_system.fundamentals.sec_pit_backfill import (
    SEC_PIT_BACKTEST_DATA_GRADE,
    SecPitBackfillConfig,
    sec_pit_safety_metadata,
)
from ai_trading_system.fundamentals.sec_pit_panel import SEC_PIT_FEATURE_PANEL_COLUMNS


@dataclass(frozen=True)
class SecPitValidationIssue:
    severity: str
    code: str
    message: str
    ticker: str = ""
    subject: str = ""


@dataclass(frozen=True)
class SecPitValidationReport:
    as_of: date
    generated_at: datetime
    raw_manifest_path: Path
    filing_timeline_path: Path
    facts_path: Path
    mapped_metrics_path: Path
    intervals_path: Path
    feature_panel_path: Path
    issues: tuple[SecPitValidationIssue, ...] = field(default_factory=tuple)
    raw_file_count: int = 0
    fact_rows: int = 0
    mapped_metric_rows: int = 0
    feature_rows: int = 0
    coverage_by_ticker: dict[str, float] = field(default_factory=dict)
    pit_grade_counts: dict[str, int] = field(default_factory=dict)
    confidence_counts: dict[str, int] = field(default_factory=dict)

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == "ERROR")

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == "WARNING")

    @property
    def status(self) -> str:
        if self.error_count:
            return "FAIL"
        if self.warning_count:
            return "PASS_WITH_WARNINGS"
        return "PASS"

    @property
    def passed(self) -> bool:
        return self.error_count == 0


def validate_sec_pit_backfill(
    *,
    as_of: date,
    raw_manifest_path: Path,
    filing_timeline_path: Path,
    facts_path: Path,
    mapped_metrics_path: Path,
    intervals_path: Path,
    feature_panel_path: Path,
    policy: SecPitBackfillConfig,
) -> SecPitValidationReport:
    issues: list[SecPitValidationIssue] = []
    raw_manifest = _read_csv(raw_manifest_path)
    timeline = _read_csv(filing_timeline_path)
    facts = _read_csv(facts_path)
    mapped_metrics = _read_csv(mapped_metrics_path)
    intervals = _read_csv(intervals_path)
    feature_panel = _read_csv(feature_panel_path)

    _validate_raw_manifest(raw_manifest, raw_manifest_path, issues)
    _validate_b_grade_facts(facts, timeline, issues)
    _validate_feature_leakage(feature_panel, issues)
    _validate_metric_forms(mapped_metrics, policy, issues)
    _validate_cross_currency_feature_rows(feature_panel, issues)
    _validate_interval_overlaps(intervals, issues)
    _validate_staleness(feature_panel, as_of, policy, issues)
    coverage_by_ticker = _coverage_by_ticker(feature_panel)
    _validate_coverage(coverage_by_ticker, policy, issues)

    return SecPitValidationReport(
        as_of=as_of,
        generated_at=datetime.now(tz=UTC),
        raw_manifest_path=raw_manifest_path,
        filing_timeline_path=filing_timeline_path,
        facts_path=facts_path,
        mapped_metrics_path=mapped_metrics_path,
        intervals_path=intervals_path,
        feature_panel_path=feature_panel_path,
        issues=tuple(issues),
        raw_file_count=len(raw_manifest),
        fact_rows=len(facts),
        mapped_metric_rows=len(mapped_metrics),
        feature_rows=len(feature_panel),
        coverage_by_ticker=coverage_by_ticker,
        pit_grade_counts=_value_counts(feature_panel, "pit_data_grade"),
        confidence_counts=_value_counts(feature_panel, "confidence_level"),
    )


def validate_and_write_sec_pit_artifacts(
    *,
    as_of: date,
    raw_manifest_path: Path,
    filing_timeline_path: Path,
    facts_path: Path,
    mapped_metrics_path: Path,
    intervals_path: Path,
    feature_panel_path: Path,
    output_dir: Path,
    policy: SecPitBackfillConfig,
) -> dict[str, Path]:
    report = validate_sec_pit_backfill(
        as_of=as_of,
        raw_manifest_path=raw_manifest_path,
        filing_timeline_path=filing_timeline_path,
        facts_path=facts_path,
        mapped_metrics_path=mapped_metrics_path,
        intervals_path=intervals_path,
        feature_panel_path=feature_panel_path,
        policy=policy,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    validation_json_path = output_dir / f"sec_pit_validation_{as_of.isoformat()}.json"
    backfill_report_path = output_dir / f"sec_pit_backfill_{as_of.isoformat()}.md"
    leakage_report_path = output_dir / f"sec_pit_leakage_check_{as_of.isoformat()}.md"
    coverage_report_path = output_dir / f"sec_pit_coverage_{as_of.isoformat()}.md"
    coverage_summary_path = feature_panel_path.parent / "sec_pit_coverage_summary.csv"
    run_log_path = output_dir / "run.log"
    validation_json_path.write_text(
        json.dumps(sec_pit_validation_summary(report), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    backfill_report_path.write_text(render_sec_pit_validation_report(report), encoding="utf-8")
    leakage_report_path.write_text(render_sec_pit_leakage_report(report), encoding="utf-8")
    coverage_report_path.write_text(render_sec_pit_coverage_report(report), encoding="utf-8")
    _write_coverage_summary_csv(report, coverage_summary_path)
    run_log_path.write_text(render_sec_pit_run_log(report), encoding="utf-8")
    return {
        "sec_pit_validation_json": validation_json_path,
        "sec_pit_backfill_report": backfill_report_path,
        "sec_pit_leakage_report": leakage_report_path,
        "sec_pit_coverage_report": coverage_report_path,
        "sec_pit_coverage_summary": coverage_summary_path,
        "sec_pit_run_log": run_log_path,
    }


def sec_pit_validation_summary(report: SecPitValidationReport) -> dict[str, object]:
    return {
        "report_type": "sec_edgar_reconstructed_pit_backfill_validation",
        "task_id": "TRADING-039",
        "status": report.status,
        "generated_at": report.generated_at.isoformat(),
        "as_of": report.as_of.isoformat(),
        "raw_file_count": report.raw_file_count,
        "fact_rows": report.fact_rows,
        "mapped_metric_rows": report.mapped_metric_rows,
        "feature_rows": report.feature_rows,
        "coverage_by_ticker": report.coverage_by_ticker,
        "pit_grade_counts": report.pit_grade_counts,
        "confidence_counts": report.confidence_counts,
        "error_count": report.error_count,
        "warning_count": report.warning_count,
        "issues": [issue.__dict__ for issue in report.issues],
        **sec_pit_safety_metadata(),
    }


def render_sec_pit_validation_report(report: SecPitValidationReport) -> str:
    lines = [
        "# SEC EDGAR Reconstructed PIT Backfill Report",
        "",
        f"- status: {report.status}",
        f"- generated_at: {report.generated_at.isoformat()}",
        f"- as_of: {report.as_of.isoformat()}",
        f"- raw files: {report.raw_file_count}",
        f"- fact rows: {report.fact_rows}",
        f"- mapped metrics: {report.mapped_metric_rows}",
        f"- feature rows: {report.feature_rows}",
        "- production_effect=none",
        f"- backtest_data_grade={SEC_PIT_BACKTEST_DATA_GRADE}",
        "- strict_vendor_archive=false",
        "- external_side_effects=false",
        "- broker_access_required=false",
        "- paid_vendor_required=false",
        "- retroactive_strict_pit=false",
        "- manual_review_required_for_grade_upgrade=true",
        "",
        "## PIT grade counts",
        "",
        *_count_lines(report.pit_grade_counts),
        "",
        "## confidence counts",
        "",
        *_count_lines(report.confidence_counts),
        "",
        "## leakage check",
        "",
        f"- status: {_leakage_status(report)}",
        "",
        "## coverage table",
        "",
        "| Ticker | Coverage |",
        "|---|---:|",
    ]
    if report.coverage_by_ticker:
        for ticker, coverage in sorted(report.coverage_by_ticker.items()):
            lines.append(f"| {ticker} | {coverage:.0%} |")
    else:
        lines.append("| n/a | 0% |")
    lines.extend(
        [
            "",
            "## blocked rows sample",
            "",
            *_issue_rows(report.issues),
            "",
            "## known limitations",
            "",
            "- 该数据层来自 SEC 当前可下载历史披露事实的 filing-time reconstruction，"
            "不是 WRDS/Compustat Snapshot。",
            "- SEC companyfacts 是聚合 API，可能受 SEC 后续解析修正影响；因此默认只能是 B 级。",
            "- 外国发行人可能缺少完整季度 XBRL，TSM/ASML 等仍需要官方 IR PIT 规则补强。",
        ]
    )
    return "\n".join(lines) + "\n"


def render_sec_pit_leakage_report(report: SecPitValidationReport) -> str:
    leakage_issues = [issue for issue in report.issues if "leakage" in issue.code]
    lines = [
        "# SEC PIT As-of Leakage Check",
        "",
        f"- status: {_leakage_status(report)}",
        f"- as_of: {report.as_of.isoformat()}",
        "- production_effect=none",
        f"- backtest_data_grade={SEC_PIT_BACKTEST_DATA_GRADE}",
        "- strict_vendor_archive=false",
        "- external_side_effects=false",
        "- broker_access_required=false",
        "- paid_vendor_required=false",
        "- retroactive_strict_pit=false",
        "- manual_review_required_for_grade_upgrade=true",
        "- rule: `max_input_available_time_utc` and source availability must not exceed "
        "`decision_date`.",
        "",
        "| Severity | Code | Ticker | Subject | Message |",
        "|---|---|---|---|---|",
    ]
    if leakage_issues:
        lines.extend(_issue_table_row(issue) for issue in leakage_issues)
    else:
        lines.append("| INFO | no_leakage_detected |  |  | 未发现未来可见时间泄漏。 |")
    return "\n".join(lines) + "\n"


def render_sec_pit_coverage_report(report: SecPitValidationReport) -> str:
    lines = [
        "# SEC PIT Coverage Report",
        "",
        f"- status: {report.status}",
        f"- as_of: {report.as_of.isoformat()}",
        f"- feature rows: {report.feature_rows}",
        "- production_effect=none",
        f"- backtest_data_grade={SEC_PIT_BACKTEST_DATA_GRADE}",
        "- strict_vendor_archive=false",
        "- external_side_effects=false",
        "- broker_access_required=false",
        "- paid_vendor_required=false",
        "- retroactive_strict_pit=false",
        "- manual_review_required_for_grade_upgrade=true",
        "",
        "| Ticker | Coverage |",
        "|---|---:|",
    ]
    if report.coverage_by_ticker:
        for ticker, coverage in sorted(report.coverage_by_ticker.items()):
            lines.append(f"| {ticker} | {coverage:.0%} |")
    else:
        lines.append("| n/a | 0% |")
    return "\n".join(lines) + "\n"


def render_sec_pit_run_log(report: SecPitValidationReport) -> str:
    return (
        "\n".join(
            [
                "task_id=TRADING-039",
                f"status={report.status}",
                f"generated_at={report.generated_at.isoformat()}",
                f"as_of={report.as_of.isoformat()}",
                f"raw_file_count={report.raw_file_count}",
                f"fact_rows={report.fact_rows}",
                f"mapped_metric_rows={report.mapped_metric_rows}",
                f"feature_rows={report.feature_rows}",
                f"error_count={report.error_count}",
                f"warning_count={report.warning_count}",
                "production_effect=none",
                f"backtest_data_grade={SEC_PIT_BACKTEST_DATA_GRADE}",
                "strict_vendor_archive=false",
                "external_side_effects=false",
                "broker_access_required=false",
                "paid_vendor_required=false",
                "retroactive_strict_pit=false",
                "manual_review_required_for_grade_upgrade=true",
            ]
        )
        + "\n"
    )


def _write_coverage_summary_csv(report: SecPitValidationReport, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    records = [
        {
            "as_of": report.as_of.isoformat(),
            "ticker": ticker,
            "coverage": coverage,
            "status": _coverage_status(coverage),
            "production_effect": "none",
            "backtest_data_grade": SEC_PIT_BACKTEST_DATA_GRADE,
            "strict_vendor_archive": False,
            "external_side_effects": False,
            "broker_access_required": False,
            "paid_vendor_required": False,
            "retroactive_strict_pit": False,
            "manual_review_required_for_grade_upgrade": True,
        }
        for ticker, coverage in sorted(report.coverage_by_ticker.items())
    ]
    if not records:
        records.append(
            {
                "as_of": report.as_of.isoformat(),
                "ticker": "",
                "coverage": 0.0,
                "status": "NO_ROWS",
                "production_effect": "none",
                "backtest_data_grade": SEC_PIT_BACKTEST_DATA_GRADE,
                "strict_vendor_archive": False,
                "external_side_effects": False,
                "broker_access_required": False,
                "paid_vendor_required": False,
                "retroactive_strict_pit": False,
                "manual_review_required_for_grade_upgrade": True,
            }
        )
    pd.DataFrame(records).to_csv(output_path, index=False)
    return output_path


def _coverage_status(coverage: float) -> str:
    if coverage <= 0:
        return "NO_ROWS"
    if coverage < 1:
        return "PARTIAL"
    return "FULL"


def _validate_raw_manifest(
    raw_manifest: pd.DataFrame,
    raw_manifest_path: Path,
    issues: list[SecPitValidationIssue],
) -> None:
    if raw_manifest.empty:
        issues.append(
            SecPitValidationIssue(
                severity="ERROR",
                code="raw_manifest_missing_or_empty",
                message=f"SEC PIT raw manifest 缺失或为空：{raw_manifest_path}",
            )
        )
        return
    required = {"output_path", "checksum_sha256", "row_count", "source_endpoint"}
    missing = sorted(required - set(raw_manifest.columns))
    if missing:
        issues.append(
            SecPitValidationIssue(
                severity="ERROR",
                code="raw_manifest_missing_columns",
                message=f"raw manifest 缺少字段：{', '.join(missing)}",
            )
        )
        return
    for record in raw_manifest.to_dict(orient="records"):
        path = Path(str(record.get("output_path") or ""))
        expected = str(record.get("checksum_sha256") or "")
        if not path.exists():
            issues.append(
                SecPitValidationIssue(
                    severity="ERROR",
                    code="raw_payload_missing",
                    message=f"raw payload 不存在：{path}",
                    ticker=str(record.get("ticker") or ""),
                    subject=str(path),
                )
            )
            continue
        actual = _sha256_file(path)
        if expected and actual != expected:
            issues.append(
                SecPitValidationIssue(
                    severity="ERROR",
                    code="raw_payload_checksum_mismatch",
                    message=f"raw payload checksum mismatch: expected={expected}, actual={actual}",
                    ticker=str(record.get("ticker") or ""),
                    subject=str(path),
                )
            )


def _validate_b_grade_facts(
    facts: pd.DataFrame,
    timeline: pd.DataFrame,
    issues: list[SecPitValidationIssue],
) -> None:
    if facts.empty:
        return
    timeline_keys = {
        (str(row.get("ticker") or "").upper(), str(row.get("accession_number") or ""))
        for row in timeline.to_dict(orient="records")
    }
    for record in facts.loc[
        facts["pit_data_grade"].astype(str) == SEC_PIT_BACKTEST_DATA_GRADE
    ].to_dict(orient="records"):
        key = (
            str(record.get("ticker") or "").upper(),
            str(record.get("accession_number") or ""),
        )
        if (
            not key[1]
            or key not in timeline_keys
            or record.get("join_status") != "matched_accession"
        ):
            issues.append(
                SecPitValidationIssue(
                    severity="ERROR",
                    code="b_grade_fact_without_matched_accession",
                    message="B-grade fact 必须有 accession 且 join 到 filing timeline。",
                    ticker=key[0],
                    subject=key[1],
                )
            )


def _validate_feature_leakage(
    feature_panel: pd.DataFrame,
    issues: list[SecPitValidationIssue],
) -> None:
    if feature_panel.empty:
        return
    missing = sorted(set(SEC_PIT_FEATURE_PANEL_COLUMNS) - set(feature_panel.columns))
    if missing:
        issues.append(
            SecPitValidationIssue(
                severity="ERROR",
                code="feature_panel_missing_columns",
                message=f"feature panel 缺少字段：{', '.join(missing)}",
            )
        )
        return
    decision_dates = pd.to_datetime(feature_panel["decision_date"], errors="coerce").dt.date
    max_available = pd.to_datetime(
        feature_panel["max_input_available_time_utc"],
        errors="coerce",
        utc=True,
    ).dt.date
    future_mask = max_available > decision_dates
    for record in feature_panel.loc[future_mask.fillna(False)].head(20).to_dict("records"):
        issues.append(
            SecPitValidationIssue(
                severity="ERROR",
                code="feature_available_time_leakage",
                message="feature input available_time 晚于 decision_date。",
                ticker=str(record.get("ticker") or ""),
                subject=f"{record.get('decision_date')}:{record.get('feature_id')}",
            )
        )


def _validate_metric_forms(
    mapped_metrics: pd.DataFrame,
    policy: SecPitBackfillConfig,
    issues: list[SecPitValidationIssue],
) -> None:
    if mapped_metrics.empty:
        return
    allowed = set(policy.metric_panel_forms)
    invalid = mapped_metrics.loc[
        ~mapped_metrics["source_form"].astype(str).str.upper().isin(allowed)
    ]
    for record in invalid.head(20).to_dict("records"):
        issues.append(
            SecPitValidationIssue(
                severity="ERROR",
                code="selected_metric_form_not_allowed",
                message="selected metric source form 不在 whitelist 内。",
                ticker=str(record.get("ticker") or ""),
                subject=f"{record.get('metric_id')}:{record.get('source_form')}",
            )
        )


def _validate_cross_currency_feature_rows(
    feature_panel: pd.DataFrame,
    issues: list[SecPitValidationIssue],
) -> None:
    if feature_panel.empty or "input_metric_units" not in feature_panel.columns:
        return
    for record in feature_panel.to_dict("records"):
        units = [item.strip() for item in str(record.get("input_metric_units") or "").split(",")]
        if len(set(units)) > 1:
            issues.append(
                SecPitValidationIssue(
                    severity="ERROR",
                    code="cross_currency_ratio_without_fx_pit_source",
                    message="ratio feature 混用不同 unit/currency，且没有 FX PIT source。",
                    ticker=str(record.get("ticker") or ""),
                    subject=str(record.get("feature_id") or ""),
                )
            )


def _validate_interval_overlaps(
    intervals: pd.DataFrame,
    issues: list[SecPitValidationIssue],
) -> None:
    if intervals.empty:
        return
    key_columns = ["ticker", "metric_id", "period_type", "period_end"]
    for key, group in intervals.groupby(key_columns, sort=False):
        previous_until: date | None = None
        for record in group.sort_values("available_from_signal_date").to_dict("records"):
            start = _date_or_none(record.get("available_from_signal_date"))
            until = _date_or_none(record.get("available_until_signal_date")) or date.max
            if start is None:
                continue
            if previous_until is not None and start <= previous_until:
                issues.append(
                    SecPitValidationIssue(
                        severity="ERROR",
                        code="duplicate_overlapping_active_interval",
                        message=(
                            "同一 ticker/metric/period_type/period_end "
                            "存在 overlapping intervals。"
                        ),
                        ticker=str(key[0]),
                        subject=":".join(str(item) for item in key[1:]),
                    )
                )
            previous_until = until


def _validate_staleness(
    feature_panel: pd.DataFrame,
    as_of: date,
    policy: SecPitBackfillConfig,
    issues: list[SecPitValidationIssue],
) -> None:
    if feature_panel.empty:
        return
    for record in (
        feature_panel.groupby(["ticker", "period_type"], sort=False).tail(1).to_dict("records")
    ):
        period_end = _date_or_none(record.get("period_end"))
        if period_end is None:
            continue
        stale_days = (as_of - period_end).days
        limit = (
            policy.stale_quarterly_days
            if record.get("period_type") == "quarterly"
            else policy.stale_annual_days
        )
        if stale_days > limit:
            issues.append(
                SecPitValidationIssue(
                    severity="WARNING",
                    code="stale_sec_pit_feature",
                    message=f"latest fiscal period is stale: {stale_days} days > {limit}",
                    ticker=str(record.get("ticker") or ""),
                    subject=str(record.get("feature_id") or ""),
                )
            )


def _validate_coverage(
    coverage_by_ticker: dict[str, float],
    policy: SecPitBackfillConfig,
    issues: list[SecPitValidationIssue],
) -> None:
    for ticker, coverage in coverage_by_ticker.items():
        if coverage < policy.coverage_error_threshold:
            issues.append(
                SecPitValidationIssue(
                    severity="ERROR",
                    code="sec_pit_ticker_coverage_below_error_threshold",
                    message=(
                        f"ticker coverage {coverage:.0%} below "
                        f"{policy.coverage_error_threshold:.0%}"
                    ),
                    ticker=ticker,
                )
            )
        elif coverage < policy.coverage_warning_threshold:
            issues.append(
                SecPitValidationIssue(
                    severity="WARNING",
                    code="sec_pit_ticker_coverage_below_warning_threshold",
                    message=(
                        f"ticker coverage {coverage:.0%} below "
                        f"{policy.coverage_warning_threshold:.0%}"
                    ),
                    ticker=ticker,
                )
            )


def _coverage_by_ticker(feature_panel: pd.DataFrame) -> dict[str, float]:
    if feature_panel.empty:
        return {}
    expected_by_ticker = feature_panel.groupby("ticker")["decision_date"].nunique()
    max_dates = int(expected_by_ticker.max()) if not expected_by_ticker.empty else 0
    if max_dates == 0:
        return {}
    return {
        str(ticker): float(count) / max_dates
        for ticker, count in expected_by_ticker.to_dict().items()
    }


def _value_counts(frame: pd.DataFrame, column: str) -> dict[str, int]:
    if frame.empty or column not in frame.columns:
        return {}
    return {str(key): int(value) for key, value in frame[column].value_counts().to_dict().items()}


def _leakage_status(report: SecPitValidationReport) -> str:
    return "FAIL" if any("leakage" in issue.code for issue in report.issues) else "PASS"


def _issue_rows(issues: tuple[SecPitValidationIssue, ...]) -> list[str]:
    if not issues:
        return ["未发现 blocked rows。"]
    lines = ["| Severity | Code | Ticker | Subject | Message |", "|---|---|---|---|---|"]
    lines.extend(_issue_table_row(issue) for issue in issues[:20])
    return lines


def _issue_table_row(issue: SecPitValidationIssue) -> str:
    return (
        "| "
        f"{issue.severity} | "
        f"{issue.code} | "
        f"{issue.ticker} | "
        f"{issue.subject} | "
        f"{_escape(issue.message)} |"
    )


def _count_lines(counts: dict[str, int]) -> list[str]:
    if not counts:
        return ["- none"]
    return [f"- {key}: {value}" for key, value in sorted(counts.items())]


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str).fillna("")
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _date_or_none(value: object) -> date | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _escape(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
