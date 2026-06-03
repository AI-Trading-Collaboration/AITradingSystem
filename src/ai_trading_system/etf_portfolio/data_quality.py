from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Literal, Self

import pandas as pd
from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.data import (
    read_price_frame,
    standardize_price_frame,
)
from ai_trading_system.etf_portfolio.models import (
    DEFAULT_ETF_REPORT_DIR,
    ETFAssetsConfig,
    PolicyMetadata,
    load_etf_config_bundle,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    build_report_index_payload,
    load_report_registry,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_ETF_DATA_QUALITY_POLICY_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "data_quality.yaml"
)
DEFAULT_ETF_DATA_QUALITY_REPORT_DIR = (
    DEFAULT_ETF_REPORT_DIR / "data_quality" / "governance"
)
DEFAULT_ETF_DATA_QUALITY_VALIDATION_DIR = (
    DEFAULT_ETF_REPORT_DIR / "data_quality" / "validation"
)

DATA_QUALITY_POLICY_SCHEMA_VERSION = "etf_data_quality_policy_v1"
DATA_QUALITY_REPORT_SCHEMA_VERSION = "etf_data_quality_report_v1"
DATA_QUALITY_VALIDATION_SCHEMA_VERSION = "etf_data_quality_validation_v1"
DATA_QUALITY_REPORT_TYPE = "etf_data_quality_governance"
DATA_QUALITY_VALIDATION_REPORT_TYPE = "etf_data_quality_validation"
DATA_QUALITY_REPORT_REGISTRY_ID = "etf_data_quality_governance_report"
DATA_QUALITY_VALIDATION_REGISTRY_ID = "etf_data_quality_validation"

DATA_QUALITY_SAFETY = {
    "observe_only": True,
    "candidate_only": True,
    "production_effect": "none",
    "broker_action": "none",
    "manual_review_required": True,
}

Severity = Literal["info", "warning", "error", "critical"]


class DataQualityPolicyError(ValueError):
    """Raised when the data quality policy is unsafe or incomplete."""


class DataQualitySafety(BaseModel):
    observe_only: Literal[True]
    candidate_only: Literal[True]
    production_effect: Literal["none"]
    broker_action: Literal["none"]
    manual_review_required: Literal[True]


class PriceFreshnessPolicy(BaseModel):
    required_assets: list[str] = Field(min_length=1)
    optional_assets: list[str] = Field(default_factory=list)
    max_trading_day_lag: int = Field(ge=0)
    severity_on_fail: Severity
    market_holidays: list[date] = Field(default_factory=list)

    @model_validator(mode="after")
    def normalize_symbols(self) -> Self:
        self.required_assets = _unique_upper(self.required_assets)
        self.optional_assets = _unique_upper(self.optional_assets)
        return self


class CalendarCoveragePolicy(BaseModel):
    lookback_trading_days: int = Field(gt=0)
    min_required_coverage_ratio: float = Field(ge=0, le=1)
    min_optional_coverage_ratio: float = Field(ge=0, le=1)
    severity_on_required_gap: Severity


class MissingBarsPolicy(BaseModel):
    max_missing_required_bars: int = Field(ge=0)
    max_missing_optional_bars: int = Field(ge=0)


class ReturnOutliersPolicy(BaseModel):
    lookback_trading_days: int = Field(gt=1)
    daily_abs_return_warning: float = Field(gt=0, le=1)
    daily_abs_return_critical: float = Field(gt=0, le=1)
    severity_on_critical: Severity

    @model_validator(mode="after")
    def validate_thresholds(self) -> Self:
        if self.daily_abs_return_warning >= self.daily_abs_return_critical:
            raise ValueError("daily_abs_return_warning must be below critical threshold")
        return self


class KnownCorporateActionEvent(BaseModel):
    symbol: str = Field(min_length=1)
    date: date
    explanation: str = Field(min_length=1)

    @model_validator(mode="after")
    def normalize_symbol(self) -> Self:
        self.symbol = self.symbol.upper()
        return self


class CorporateActionSanityPolicy(BaseModel):
    adjacent_day_reversal_abs_return: float = Field(gt=0, le=1)
    allow_known_event_downgrade: bool = True
    known_events: list[KnownCorporateActionEvent] = Field(default_factory=list)


class ConfigHashDriftPolicy(BaseModel):
    required_report_ids: list[str] = Field(default_factory=list)
    optional_report_ids: list[str] = Field(default_factory=list)
    severity_on_required_drift: Severity


class EvidenceSetPolicy(BaseModel):
    evidence_type: str = Field(min_length=1)
    report_id: str = Field(min_length=1)
    required: bool
    required_fields: list[str] = Field(default_factory=list)
    sample_count_paths: list[str] = Field(default_factory=list)
    minimum_sample_count: int = Field(ge=0)


class EvidenceCompletenessPolicy(BaseModel):
    evidence_sets: list[EvidenceSetPolicy] = Field(min_length=1)


class GateFreshnessPolicy(BaseModel):
    gate_id: str = Field(min_length=1)
    report_id: str = Field(min_length=1)
    required: bool
    max_allowed_age_days: int | None = Field(default=None, ge=0)


class ValidationGateFreshnessPolicy(BaseModel):
    max_age_days: int = Field(ge=0)
    stale_required_blocks: bool = True
    gates: list[GateFreshnessPolicy] = Field(min_length=1)


class ReportStalenessPolicy(BaseModel):
    required_report_ids: list[str] = Field(default_factory=list)
    optional_report_ids: list[str] = Field(default_factory=list)
    max_allowed_age_days: int = Field(ge=0)


class ReaderBriefLinksPolicy(BaseModel):
    required_report_ids: list[str] = Field(default_factory=list)
    optional_report_ids: list[str] = Field(default_factory=list)
    max_allowed_age_days: int = Field(ge=0)


class DataQualityOutputPolicy(BaseModel):
    report_dir: str = Field(min_length=1)
    validation_dir: str = Field(min_length=1)


class DataQualitySettings(BaseModel):
    price_freshness: PriceFreshnessPolicy
    calendar_coverage: CalendarCoveragePolicy
    missing_bars: MissingBarsPolicy
    return_outliers: ReturnOutliersPolicy
    corporate_action_sanity: CorporateActionSanityPolicy
    config_hash_drift: ConfigHashDriftPolicy
    evidence_completeness: EvidenceCompletenessPolicy
    validation_gate_freshness: ValidationGateFreshnessPolicy
    report_staleness: ReportStalenessPolicy
    reader_brief_links: ReaderBriefLinksPolicy
    output: DataQualityOutputPolicy
    safety: DataQualitySafety


class DataQualityPolicyConfig(BaseModel):
    schema_version: Literal["etf_data_quality_policy_v1"]
    policy_metadata: PolicyMetadata
    data_quality: DataQualitySettings


def load_data_quality_policy_config(
    path: Path | str = DEFAULT_ETF_DATA_QUALITY_POLICY_CONFIG_PATH,
) -> DataQualityPolicyConfig:
    raw = safe_load_yaml_path(Path(path))
    if not isinstance(raw, dict):
        raise DataQualityPolicyError("data quality policy must be a mapping")
    try:
        return DataQualityPolicyConfig.model_validate(raw)
    except ValueError as exc:
        raise DataQualityPolicyError(str(exc)) from exc


def check_price_freshness(
    prices: pd.DataFrame,
    *,
    policy: DataQualityPolicyConfig,
    as_of: date,
) -> dict[str, Any]:
    settings = policy.data_quality
    expected = expected_latest_trading_date(
        as_of,
        holidays=set(settings.price_freshness.market_holidays),
    )
    records: list[dict[str, Any]] = []
    frame = _normalized_price_frame(prices)
    for symbol, required in _policy_assets(settings.price_freshness):
        latest = _latest_symbol_date(frame, symbol, as_of=as_of)
        if latest is None:
            status = "missing"
            lag = None
        else:
            lag = trading_day_lag(
                latest,
                expected,
                holidays=set(settings.price_freshness.market_holidays),
            )
            status = (
                "fresh"
                if lag <= settings.price_freshness.max_trading_day_lag
                else "stale"
            )
        blocking_status = _blocking_status(
            required=required,
            failed=status in {"missing", "stale"},
        )
        records.append(
            {
                "asset": symbol,
                "required": required,
                "latest_available_date": "" if latest is None else latest.isoformat(),
                "expected_latest_trading_date": expected.isoformat(),
                "trading_day_lag": lag,
                "freshness_status": status,
                "blocking_status": blocking_status,
                "severity": (
                    settings.price_freshness.severity_on_fail
                    if status in {"missing", "stale"} and required
                    else ("warning" if status in {"missing", "stale"} else "info")
                ),
            }
        )
    return _section(
        "price_freshness",
        records,
        summary_keys=("freshness_status", "blocking_status"),
    )


def check_missing_bar_coverage(
    prices: pd.DataFrame,
    *,
    policy: DataQualityPolicyConfig,
    as_of: date,
) -> dict[str, Any]:
    settings = policy.data_quality
    holidays = set(settings.price_freshness.market_holidays)
    expected_latest = expected_latest_trading_date(as_of, holidays=holidays)
    expected_days = recent_trading_days(
        expected_latest,
        settings.calendar_coverage.lookback_trading_days,
        holidays=holidays,
    )
    expected_set = set(expected_days)
    frame = _normalized_price_frame(prices)
    records: list[dict[str, Any]] = []
    for symbol, required in _policy_assets(settings.price_freshness):
        symbol_dates = {
            item
            for item in _symbol_dates(frame, symbol, as_of=as_of)
            if item in expected_set
        }
        missing_dates = sorted(expected_set - symbol_dates)
        available = len(symbol_dates)
        expected_count = len(expected_days)
        coverage_ratio = available / expected_count if expected_count else 0.0
        missing_limit = (
            settings.missing_bars.max_missing_required_bars
            if required
            else settings.missing_bars.max_missing_optional_bars
        )
        min_ratio = (
            settings.calendar_coverage.min_required_coverage_ratio
            if required
            else settings.calendar_coverage.min_optional_coverage_ratio
        )
        if coverage_ratio < min_ratio:
            status = "insufficient_coverage"
        elif len(missing_dates) == 0:
            status = "complete"
        elif len(missing_dates) <= missing_limit:
            status = "minor_gap"
        else:
            status = "major_gap"
        failed = status in {"major_gap", "insufficient_coverage"} or (
            required and len(missing_dates) > missing_limit
        )
        records.append(
            {
                "asset": symbol,
                "required": required,
                "start_date": expected_days[0].isoformat() if expected_days else "",
                "end_date": expected_latest.isoformat(),
                "expected_trading_days": expected_count,
                "available_bars": available,
                "missing_dates": [item.isoformat() for item in missing_dates],
                "coverage_ratio": round(coverage_ratio, 6),
                "required_gap_count": len(missing_dates) if required else 0,
                "optional_gap_count": 0 if required else len(missing_dates),
                "coverage_status": status,
                "blocking_status": _blocking_status(required=required, failed=failed),
                "severity": (
                    settings.calendar_coverage.severity_on_required_gap
                    if required and failed
                    else ("warning" if failed else "info")
                ),
            }
        )
    return _section(
        "calendar_coverage",
        records,
        summary_keys=("coverage_status", "blocking_status"),
    )


def check_return_outliers(
    prices: pd.DataFrame,
    *,
    policy: DataQualityPolicyConfig,
    as_of: date,
) -> dict[str, Any]:
    settings = policy.data_quality
    holidays = set(settings.price_freshness.market_holidays)
    expected_latest = expected_latest_trading_date(as_of, holidays=holidays)
    lookback = set(
        recent_trading_days(
            expected_latest,
            settings.return_outliers.lookback_trading_days,
            holidays=holidays,
        )
    )
    frame = _normalized_price_frame(prices)
    if frame.empty or "adj_close" not in frame.columns:
        return _section("return_outliers", [], summary_keys=("outlier_status",))
    records: list[dict[str, Any]] = []
    data = frame.copy()
    data["_date"] = pd.to_datetime(data["date"], errors="coerce")
    data["_adj_close"] = pd.to_numeric(data["adj_close"], errors="coerce")
    data = data.loc[data["_date"].notna() & data["_adj_close"].notna()]
    data = data.loc[data["_date"].dt.date <= as_of].sort_values(["symbol", "_date"])
    data["_daily_return"] = data.groupby("symbol")["_adj_close"].pct_change()
    data["_next_return"] = data.groupby("symbol")["_daily_return"].shift(-1)
    known_events = {
        (event.symbol, event.date): event.explanation
        for event in settings.corporate_action_sanity.known_events
    }
    for _, row in data.iterrows():
        row_date = pd.Timestamp(row["_date"]).date()
        if row_date not in lookback:
            continue
        daily_return = _float_or_none(row.get("_daily_return"))
        if daily_return is None:
            continue
        abs_return = abs(daily_return)
        if abs_return < settings.return_outliers.daily_abs_return_warning:
            continue
        symbol = str(row["symbol"]).upper()
        next_return = _float_or_none(row.get("_next_return"))
        adjacent_reversal = (
            next_return is not None
            and daily_return * next_return < 0
            and abs(next_return)
            >= settings.corporate_action_sanity.adjacent_day_reversal_abs_return
        )
        explanation = known_events.get((symbol, row_date), "")
        if abs_return >= settings.return_outliers.daily_abs_return_critical:
            status = (
                "warning_outlier"
                if explanation and settings.corporate_action_sanity.allow_known_event_downgrade
                else "critical_outlier"
            )
            if adjacent_reversal and not explanation:
                status = "possible_adjustment_issue"
        else:
            status = "warning_outlier"
        critical = status in {"critical_outlier", "possible_adjustment_issue"}
        records.append(
            {
                "asset": symbol,
                "date": row_date.isoformat(),
                "daily_return": round(float(daily_return), 8),
                "absolute_return": round(float(abs_return), 8),
                "warning_threshold": settings.return_outliers.daily_abs_return_warning,
                "critical_threshold": settings.return_outliers.daily_abs_return_critical,
                "outlier_status": status,
                "adjacent_day_reversal_flag": adjacent_reversal,
                "known_event_explanation_if_available": explanation,
                "blocking_status": "block" if critical else "warn",
                "severity": (
                    settings.return_outliers.severity_on_critical
                    if critical
                    else "warning"
                ),
            }
        )
    return _section(
        "return_outliers",
        records,
        summary_keys=("outlier_status", "blocking_status"),
    )


def check_config_model_drift(
    report_index: Mapping[str, Any],
    *,
    policy: DataQualityPolicyConfig,
    current_config_hash: str,
    current_model_version: str,
) -> dict[str, Any]:
    settings = policy.data_quality.config_hash_drift
    records: list[dict[str, Any]] = []
    for report_id, required in _required_optional_ids(
        settings.required_report_ids,
        settings.optional_report_ids,
    ):
        report = _report_index_record(report_index, report_id)
        path = _path_from_report_record(report)
        payload = _read_json_object(path)
        artifact_hash = _first_nested_value(payload, _CONFIG_HASH_KEYS)
        artifact_model = _first_nested_value(payload, _MODEL_VERSION_KEYS)
        if path is None or not path.exists():
            status = "unknown"
            failed = required
        elif not artifact_hash and not artifact_model:
            status = "unknown"
            failed = False
        elif artifact_hash and artifact_hash != current_config_hash:
            status = "config_drift"
            failed = required
        elif artifact_model and artifact_model != current_model_version:
            status = "model_version_drift"
            failed = required
        else:
            status = "matched"
            failed = False
        records.append(
            {
                "report_id": report_id,
                "required": required,
                "artifact_path": "" if path is None else str(path),
                "artifact_config_hash": artifact_hash,
                "current_config_hash": current_config_hash,
                "artifact_model_version": artifact_model,
                "current_model_version": current_model_version,
                "drift_status": status,
                "blocking_status": _blocking_status(required=required, failed=failed),
                "severity": (
                    settings.severity_on_required_drift
                    if failed and required
                    else ("warning" if status != "matched" else "info")
                ),
            }
        )
    return _section(
        "config_hash_model_version_drift",
        records,
        summary_keys=("drift_status", "blocking_status"),
    )


def check_evidence_completeness(
    report_index: Mapping[str, Any],
    *,
    policy: DataQualityPolicyConfig,
) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    for evidence in policy.data_quality.evidence_completeness.evidence_sets:
        report = _report_index_record(report_index, evidence.report_id)
        path = _path_from_report_record(report)
        payload = _read_json_object(path)
        if path is None or not path.exists() or not payload:
            status = "missing"
            available_fields: list[str] = []
            sample_count = 0
        else:
            available_fields = [
                field
                for field in evidence.required_fields
                if _path_exists(payload, field)
            ]
            sample_count = _sample_count(payload, evidence.sample_count_paths)
            if len(available_fields) < len(evidence.required_fields):
                status = "partial"
            elif sample_count < evidence.minimum_sample_count:
                status = "insufficient"
            else:
                status = "complete"
        missing_fields = [
            field for field in evidence.required_fields if field not in available_fields
        ]
        coverage_ratio = (
            len(available_fields) / len(evidence.required_fields)
            if evidence.required_fields
            else 1.0
        )
        failed = evidence.required and status in {"missing", "insufficient"}
        records.append(
            {
                "evidence_type": evidence.evidence_type,
                "report_id": evidence.report_id,
                "required": evidence.required,
                "artifact_path": "" if path is None else str(path),
                "required_fields": evidence.required_fields,
                "available_fields": available_fields,
                "sample_count": sample_count,
                "minimum_sample_count": evidence.minimum_sample_count,
                "coverage_ratio": round(coverage_ratio, 6),
                "missing_fields": missing_fields,
                "completeness_status": status,
                "blocking_status": _blocking_status(
                    required=evidence.required,
                    failed=failed,
                ),
                "severity": (
                    "critical" if failed else ("warning" if status != "complete" else "info")
                ),
            }
        )
    return _section(
        "evidence_completeness",
        records,
        summary_keys=("completeness_status", "blocking_status"),
    )


def check_validation_gate_freshness(
    report_index: Mapping[str, Any],
    *,
    policy: DataQualityPolicyConfig,
    as_of: date,
) -> dict[str, Any]:
    settings = policy.data_quality.validation_gate_freshness
    records: list[dict[str, Any]] = []
    for gate in settings.gates:
        report = _report_index_record(report_index, gate.report_id)
        path = _path_from_report_record(report)
        payload = _read_json_object(path)
        artifact_date = _date_or_none(report.get("artifact_date"))
        generated_at = _datetime_or_none(_first_nested_value(payload, ("generated_at",)))
        latest_as_of = _date_or_none(
            _first_nested_value(payload, ("as_of_date", "as_of", "date"))
        ) or artifact_date
        latest_status = str(
            _first_nested_value(payload, _STATUS_KEYS)
            or report.get("artifact_status")
            or "MISSING"
        )
        max_age = gate.max_allowed_age_days
        if max_age is None:
            max_age = settings.max_age_days
        age_days = (as_of - artifact_date).days if artifact_date is not None else None
        status_upper = latest_status.upper()
        latest_failed = any(token in status_upper for token in ("FAIL", "ERROR", "BLOCKED"))
        if path is None or not path.exists():
            freshness_status = "missing"
            failed = gate.required
        elif latest_failed:
            freshness_status = "failed"
            failed = gate.required
        elif age_days is None:
            freshness_status = "unknown"
            failed = False
        elif age_days > max_age:
            freshness_status = "stale"
            failed = gate.required and settings.stale_required_blocks
        else:
            freshness_status = "fresh"
            failed = False
        records.append(
            {
                "gate_id": gate.gate_id,
                "report_id": gate.report_id,
                "required": gate.required,
                "artifact_path": "" if path is None else str(path),
                "latest_status": latest_status,
                "latest_run_at": "" if generated_at is None else generated_at.isoformat(),
                "latest_as_of_date": "" if latest_as_of is None else latest_as_of.isoformat(),
                "max_allowed_age_days": max_age,
                "age_days": age_days,
                "freshness_status": freshness_status,
                "blocking_status": _blocking_status(required=gate.required, failed=failed),
                "severity": (
                    "critical"
                    if failed
                    else ("warning" if freshness_status != "fresh" else "info")
                ),
            }
        )
    return _section(
        "validation_gate_freshness",
        records,
        summary_keys=("freshness_status", "blocking_status"),
    )


def check_report_staleness(
    report_index: Mapping[str, Any],
    *,
    policy: DataQualityPolicyConfig,
) -> dict[str, Any]:
    settings = policy.data_quality.report_staleness
    records: list[dict[str, Any]] = []
    for report_id, required in _required_optional_ids(
        settings.required_report_ids,
        settings.optional_report_ids,
    ):
        report = _report_index_record(report_index, report_id)
        path = _path_from_report_record(report)
        freshness = str(report.get("freshness_status") or "MISSING")
        age_days = _int_or_none(report.get("age_days"))
        stale_by_policy = (
            age_days is not None and age_days > settings.max_allowed_age_days
        )
        missing = path is None or not path.exists()
        stale = freshness.upper() == "STALE" or stale_by_policy
        if missing:
            status = "missing"
        elif stale:
            status = "stale"
        else:
            status = "fresh"
        production_risk = report.get("artifact_production_effect_risk") is True
        failed = required and (missing or stale or production_risk)
        records.append(
            {
                "report_id": report_id,
                "required": required,
                "report_path": "" if path is None else str(path),
                "latest_generated_at": _text(report.get("latest_generated_at")),
                "latest_as_of_date": _text(report.get("artifact_date")),
                "max_allowed_age_days": settings.max_allowed_age_days,
                "registry_freshness_status": freshness,
                "staleness_status": status,
                "reader_brief_linked": False,
                "link_status": "not_checked",
                "blocking_status": _blocking_status(required=required, failed=failed),
                "severity": "critical" if failed else ("warning" if status != "fresh" else "info"),
            }
        )
    return _section(
        "report_staleness",
        records,
        summary_keys=("staleness_status", "blocking_status"),
    )


def check_reader_brief_links(
    report_index: Mapping[str, Any],
    *,
    policy: DataQualityPolicyConfig,
) -> dict[str, Any]:
    settings = policy.data_quality.reader_brief_links
    reader_report = _report_index_record(report_index, "reader_brief")
    reader_path = _path_from_report_record(reader_report)
    reader_payload = _read_json_object(reader_path)
    reader_text = (
        json.dumps(reader_payload, ensure_ascii=False, sort_keys=True)
        if reader_payload
        else ""
    )
    records: list[dict[str, Any]] = []
    for report_id, required in _required_optional_ids(
        settings.required_report_ids,
        settings.optional_report_ids,
    ):
        target = _report_index_record(report_index, report_id)
        target_path = _path_from_report_record(target)
        target_freshness = str(target.get("freshness_status") or "MISSING")
        if reader_path is None or not reader_path.exists():
            link_status = "missing"
        elif target_path is None or not target_path.exists():
            link_status = "broken_link"
        elif report_id not in reader_text and str(target_path) not in reader_text:
            link_status = "not_linked"
        elif target_freshness.upper() == "STALE":
            link_status = "stale"
        else:
            link_status = "fresh"
        failed = required and link_status in {"missing", "broken_link", "not_linked", "stale"}
        records.append(
            {
                "report_id": report_id,
                "required": required,
                "reader_brief_path": "" if reader_path is None else str(reader_path),
                "report_path": "" if target_path is None else str(target_path),
                "latest_generated_at": _text(reader_report.get("artifact_date")),
                "latest_as_of_date": _text(target.get("artifact_date")),
                "max_allowed_age_days": settings.max_allowed_age_days,
                "staleness_status": target_freshness.lower(),
                "reader_brief_linked": link_status == "fresh",
                "link_status": link_status,
                "blocking_status": _blocking_status(required=required, failed=failed),
                "severity": (
                    "critical"
                    if failed
                    else ("warning" if link_status != "fresh" else "info")
                ),
            }
        )
    return _section(
        "reader_brief_links",
        records,
        summary_keys=("link_status", "blocking_status"),
    )


def build_data_quality_report(
    *,
    as_of: str | date,
    prices_path: Path | str,
    policy_config_path: Path | str = DEFAULT_ETF_DATA_QUALITY_POLICY_CONFIG_PATH,
    report_registry_path: Path | str = DEFAULT_REPORT_REGISTRY_PATH,
    root_path: Path | str = PROJECT_ROOT,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    run_date = _parse_date(as_of)
    generated = generated_at or datetime.now(UTC)
    policy = load_data_quality_policy_config(Path(policy_config_path))
    config = load_etf_config_bundle()
    prices, price_load_errors = _load_governance_price_frame(
        Path(prices_path),
        assets=config.assets,
        policy=policy,
    )
    report_index = build_report_index_payload(
        as_of=run_date,
        project_root=Path(root_path),
        registry_path=Path(report_registry_path),
    )
    sections = {
        "price_freshness": check_price_freshness(
            prices,
            policy=policy,
            as_of=run_date,
        ),
        "calendar_coverage": check_missing_bar_coverage(
            prices,
            policy=policy,
            as_of=run_date,
        ),
        "return_outliers": check_return_outliers(
            prices,
            policy=policy,
            as_of=run_date,
        ),
        "config_hash_model_version_drift": check_config_model_drift(
            report_index,
            policy=policy,
            current_config_hash=config.config_hash,
            current_model_version=config.strategy.model.version,
        ),
        "evidence_completeness": check_evidence_completeness(
            report_index,
            policy=policy,
        ),
        "validation_gate_freshness": check_validation_gate_freshness(
            report_index,
            policy=policy,
            as_of=run_date,
        ),
        "report_staleness": check_report_staleness(
            report_index,
            policy=policy,
        ),
        "reader_brief_links": check_reader_brief_links(
            report_index,
            policy=policy,
        ),
    }
    if price_load_errors:
        sections["price_freshness"]["records"].extend(price_load_errors)
        sections["price_freshness"] = _refresh_section_summary(
            sections["price_freshness"],
            summary_keys=("freshness_status", "blocking_status"),
        )
    blocking_findings, warnings = _collect_findings(sections)
    status = "BLOCKED" if blocking_findings else ("WARNING" if warnings else "PASS")
    safety = policy.data_quality.safety.model_dump()
    return {
        "schema_version": DATA_QUALITY_REPORT_SCHEMA_VERSION,
        "report_type": DATA_QUALITY_REPORT_TYPE,
        "report_id": _report_id("data_quality", run_date, generated),
        "as_of_date": run_date.isoformat(),
        "generated_at": generated.isoformat(),
        "status": status,
        "safety_banner": safety,
        "run_metadata": {
            "policy_config_path": str(policy_config_path),
            "policy_version": policy.policy_metadata.version,
            "prices_path": str(prices_path),
            "report_registry_path": str(report_registry_path),
            "root_path": str(root_path),
            "current_config_hash": config.config_hash,
            "current_model_version": config.strategy.model.version,
            "commands_executed": False,
            "production_state_mutated": False,
        },
        "price_freshness": sections["price_freshness"],
        "calendar_coverage": sections["calendar_coverage"],
        "missing_bars": sections["calendar_coverage"],
        "return_outliers": sections["return_outliers"],
        "corporate_action_sanity": {
            "schema_version": "etf_corporate_action_sanity_v1",
            "records": sections["return_outliers"]["records"],
            "summary": {
                "possible_adjustment_issue_count": len(
                    [
                        item
                        for item in sections["return_outliers"]["records"]
                        if item.get("outlier_status") == "possible_adjustment_issue"
                    ]
                )
            },
        },
        "config_hash_model_version_drift": sections["config_hash_model_version_drift"],
        "evidence_completeness": sections["evidence_completeness"],
        "validation_gate_freshness": sections["validation_gate_freshness"],
        "report_staleness": sections["report_staleness"],
        "reader_brief_links": sections["reader_brief_links"],
        "blocking_failures": blocking_findings,
        "warnings": warnings,
        "manual_review_items": _manual_review_items(blocking_findings, warnings),
        "source_links": _source_links(sections),
        "read_only": True,
        "commands_executed": False,
        "production_state_mutated": False,
        "observe_only": safety["observe_only"],
        "candidate_only": safety["candidate_only"],
        "production_effect": safety["production_effect"],
        "broker_action": safety["broker_action"],
        "manual_review_required": safety["manual_review_required"],
    }


def build_data_quality_validation_report(
    *,
    as_of: str | date | None = None,
    policy_config_path: Path | str = DEFAULT_ETF_DATA_QUALITY_POLICY_CONFIG_PATH,
    report_registry_path: Path | str = DEFAULT_REPORT_REGISTRY_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    run_date = _parse_date(as_of or date.today())
    generated = generated_at or datetime.now(UTC)
    checks: list[dict[str, Any]] = []
    source_schema_versions = {
        "policy": "not_checked",
        "report": "not_checked",
        "report_registry": "not_checked",
    }
    policy: DataQualityPolicyConfig | None = None
    try:
        policy = load_data_quality_policy_config(Path(policy_config_path))
        source_schema_versions["policy"] = policy.schema_version
        checks.append(
            _validation_check(
                "policy_valid",
                "PASS",
                "data quality policy loads and validates",
                {"policy_version": policy.policy_metadata.version},
            )
        )
    except Exception as exc:  # noqa: BLE001 - validation gate records failure.
        checks.append(
            _validation_check(
                "policy_valid",
                "FAIL",
                "data quality policy failed validation",
                {"error_type": type(exc).__name__, "error": str(exc)},
            )
        )

    if policy is None:
        safety = dict(DATA_QUALITY_SAFETY)
        return _validation_report(
            as_of=run_date,
            generated_at=generated,
            checks=checks,
            source_schema_versions=source_schema_versions,
            safety=safety,
        )

    fixture_prices = _validation_prices(run_date, policy)
    stale_prices = fixture_prices.loc[
        ~(
            (fixture_prices["symbol"] == "SPY")
            & (pd.to_datetime(fixture_prices["date"]).dt.date == run_date)
        )
    ].copy()
    outlier_prices = fixture_prices.copy()
    outlier_prices.loc[
        (outlier_prices["symbol"] == "QQQ")
        & (pd.to_datetime(outlier_prices["date"]).dt.date == run_date),
        ["open", "high", "low", "close", "adj_close"],
    ] = 200.0

    _add_checker_probe(
        checks,
        "price_freshness_checker_available",
        lambda: check_price_freshness(stale_prices, policy=policy, as_of=run_date),
        required_key="blocking_count",
    )
    _add_checker_probe(
        checks,
        "missing_bar_checker_available",
        lambda: check_missing_bar_coverage(stale_prices, policy=policy, as_of=run_date),
        required_key="blocking_count",
    )
    _add_checker_probe(
        checks,
        "return_outlier_checker_available",
        lambda: check_return_outliers(outlier_prices, policy=policy, as_of=run_date),
        required_key="warning_count",
    )
    empty_index = {"reports": []}
    _add_checker_probe(
        checks,
        "config_model_drift_checker_available",
        lambda: check_config_model_drift(
            empty_index,
            policy=policy,
            current_config_hash="current",
            current_model_version="model",
        ),
        required_key="record_count",
    )
    _add_checker_probe(
        checks,
        "evidence_completeness_checker_available",
        lambda: check_evidence_completeness(empty_index, policy=policy),
        required_key="record_count",
    )
    _add_checker_probe(
        checks,
        "validation_gate_freshness_checker_available",
        lambda: check_validation_gate_freshness(
            empty_index,
            policy=policy,
            as_of=run_date,
        ),
        required_key="record_count",
    )
    _add_checker_probe(
        checks,
        "report_staleness_checker_available",
        lambda: check_report_staleness(empty_index, policy=policy),
        required_key="record_count",
    )
    _add_checker_probe(
        checks,
        "reader_brief_link_checker_available",
        lambda: check_reader_brief_links(empty_index, policy=policy),
        required_key="record_count",
    )

    try:
        with TemporaryDirectory(prefix="aits_etf_dq_validate_") as temp_root:
            temp_path = Path(temp_root)
            price_path = temp_path / "prices.csv"
            fixture_prices.to_csv(price_path, index=False)
            report = build_data_quality_report(
                as_of=run_date,
                prices_path=price_path,
                policy_config_path=policy_config_path,
                report_registry_path=report_registry_path,
                root_path=temp_path,
                generated_at=generated,
            )
        source_schema_versions["report"] = str(report.get("schema_version"))
        report_available = (
            report.get("schema_version") == DATA_QUALITY_REPORT_SCHEMA_VERSION
            and report.get("commands_executed") is False
            and report.get("production_state_mutated") is False
            and _safety_matches(report.get("safety_banner"))
        )
        checks.append(
            _validation_check(
                "report_generator_available",
                "PASS" if report_available else "FAIL",
                "data quality report generator is available and non-mutating"
                if report_available
                else "data quality report generator produced unsafe output",
                {
                    "schema_version": report.get("schema_version"),
                    "status": report.get("status"),
                },
            )
        )
    except Exception as exc:  # noqa: BLE001 - validation gate records failure.
        checks.append(
            _validation_check(
                "report_generator_available",
                "FAIL",
                "data quality report generator failed validation",
                {"error_type": type(exc).__name__, "error": str(exc)},
            )
        )

    try:
        registry = load_report_registry(Path(report_registry_path))
        source_schema_versions["report_registry"] = str(registry.get("policy_version"))
        report_entry = _registry_entry(registry, DATA_QUALITY_REPORT_REGISTRY_ID)
        validation_entry = _registry_entry(registry, DATA_QUALITY_VALIDATION_REGISTRY_ID)
        integration_ok = (
            report_entry is not None
            and report_entry.get("include_in_reader_brief") is True
            and str(report_entry.get("command", "")).startswith("aits etf data-quality report")
            and validation_entry is not None
            and str(validation_entry.get("command", "")).startswith(
                "aits etf data-quality validate"
            )
        )
        checks.append(
            _validation_check(
                "reader_brief_integration_available",
                "PASS" if integration_ok else "FAIL",
                "Reader Brief data quality registry integration is available"
                if integration_ok
                else "Reader Brief data quality registry integration is missing",
                {
                    "report_id": DATA_QUALITY_REPORT_REGISTRY_ID,
                    "validation_report_id": DATA_QUALITY_VALIDATION_REGISTRY_ID,
                    "report_entry_found": report_entry is not None,
                    "validation_entry_found": validation_entry is not None,
                },
            )
        )
    except Exception as exc:  # noqa: BLE001 - validation gate records failure.
        checks.append(
            _validation_check(
                "reader_brief_integration_available",
                "FAIL",
                "report registry integration failed validation",
                {"error_type": type(exc).__name__, "error": str(exc)},
            )
        )

    safety = policy.data_quality.safety.model_dump()
    checks.append(
        _validation_check(
            "safety_fields_intact",
            "PASS" if _safety_matches(safety) else "FAIL",
            "data quality workflow preserves safety boundary"
            if _safety_matches(safety)
            else "data quality workflow safety boundary is unsafe",
            {"required_safety": dict(DATA_QUALITY_SAFETY), "actual_safety": safety},
        )
    )
    return _validation_report(
        as_of=run_date,
        generated_at=generated,
        checks=checks,
        source_schema_versions=source_schema_versions,
        safety=safety,
    )


def render_data_quality_report_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# ETF Data Quality Governance Report",
        "",
        "## Summary / 摘要",
        "",
        "| Field | Value |",
        "|---|---|",
        f"| report_id | `{_text(payload.get('report_id'))}` |",
        f"| as_of_date | `{_text(payload.get('as_of_date'))}` |",
        f"| generated_at | `{_text(payload.get('generated_at'))}` |",
        f"| status | `{_text(payload.get('status'))}` |",
        f"| blocking_failures | {len(_records(payload.get('blocking_failures')))} |",
        f"| warnings | {len(_records(payload.get('warnings')))} |",
        f"| production_effect | `{_text(payload.get('production_effect'))}` |",
        f"| broker_action | `{_text(payload.get('broker_action'))}` |",
        f"| manual_review_required | `{payload.get('manual_review_required') is True}` |",
        "",
        "## Safety Banner / 安全边界",
        "",
        "| Field | Value |",
        "|---|---|",
    ]
    safety = _mapping(payload.get("safety_banner"))
    for field in DATA_QUALITY_SAFETY:
        lines.append(f"| {field} | `{_text(safety.get(field))}` |")

    for key, title in (
        ("price_freshness", "Price Freshness / 价格新鲜度"),
        ("calendar_coverage", "Missing Bars / Calendar Coverage"),
        ("return_outliers", "Return Outliers / Corporate Action Sanity"),
        ("config_hash_model_version_drift", "Config Hash / Model Version Drift"),
        ("evidence_completeness", "Evidence Completeness"),
        ("validation_gate_freshness", "Validation Gate Freshness"),
        ("report_staleness", "Report Staleness"),
        ("reader_brief_links", "Reader Brief Links"),
    ):
        lines.extend(_markdown_section(title, _mapping(payload.get(key))))

    lines.extend(
        [
            "",
            "## Blocking Failures / 阻断项",
            "",
            _markdown_findings(_records(payload.get("blocking_failures"))),
            "",
            "## Warnings / 警告",
            "",
            _markdown_findings(_records(payload.get("warnings"))),
            "",
            "## Manual Review Items / 人工复核项",
            "",
            _markdown_findings(_records(payload.get("manual_review_items"))),
        ]
    )
    return "\n".join(lines) + "\n"


def write_data_quality_report(
    payload: Mapping[str, Any],
    *,
    json_path: Path | str,
    markdown_path: Path | str,
) -> dict[str, Path]:
    json_output = Path(json_path)
    markdown_output = Path(markdown_path)
    json_output.parent.mkdir(parents=True, exist_ok=True)
    markdown_output.parent.mkdir(parents=True, exist_ok=True)
    json_output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    markdown_output.write_text(
        render_data_quality_report_markdown(payload),
        encoding="utf-8",
    )
    return {"json": json_output, "markdown": markdown_output}


def render_data_quality_validation_report_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# ETF Data Quality Validation Gate",
        "",
        "## Summary / 摘要",
        "",
        "| Field | Value |",
        "|---|---|",
        f"| report_id | `{_text(payload.get('report_id'))}` |",
        f"| as_of_date | `{_text(payload.get('as_of_date'))}` |",
        f"| generated_at | `{_text(payload.get('generated_at'))}` |",
        f"| status | `{_text(payload.get('status'))}` |",
        f"| failed_check_count | {_int_or_none(payload.get('failed_check_count')) or 0} |",
        f"| warning_check_count | {_int_or_none(payload.get('warning_check_count')) or 0} |",
        f"| production_effect | `{_text(payload.get('production_effect'))}` |",
        f"| broker_action | `{_text(payload.get('broker_action'))}` |",
        "",
        "## Checks / 校验项",
        "",
        "| Check | Status | Message | Evidence |",
        "|---|---|---|---|",
    ]
    for check in _records(payload.get("checks")):
        lines.append(
            f"| `{_text(check.get('check_id'))}` | `{_text(check.get('status'))}` | "
            f"{_escape_md(_text(check.get('message')))} | "
            f"`{_escape_md(_json_for_markdown(check.get('evidence', {})))}` |"
        )
    lines.extend(
        [
            "",
            "## Source Schema Versions",
            "",
            "| Source | Schema Version |",
            "|---|---|",
        ]
    )
    for source, schema in _mapping(payload.get("source_schema_versions")).items():
        lines.append(f"| {source} | `{schema}` |")
    return "\n".join(lines) + "\n"


def write_data_quality_validation_report(
    payload: Mapping[str, Any],
    *,
    json_path: Path | str,
    markdown_path: Path | str,
) -> dict[str, Path]:
    json_output = Path(json_path)
    markdown_output = Path(markdown_path)
    json_output.parent.mkdir(parents=True, exist_ok=True)
    markdown_output.parent.mkdir(parents=True, exist_ok=True)
    json_output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    markdown_output.write_text(
        render_data_quality_validation_report_markdown(payload),
        encoding="utf-8",
    )
    return {"json": json_output, "markdown": markdown_output}


def expected_latest_trading_date(as_of: date, *, holidays: set[date]) -> date:
    current = as_of
    while not _is_trading_day(current, holidays):
        current -= timedelta(days=1)
    return current


def recent_trading_days(end: date, count: int, *, holidays: set[date]) -> list[date]:
    days: list[date] = []
    current = end
    while len(days) < count:
        if _is_trading_day(current, holidays):
            days.append(current)
        current -= timedelta(days=1)
    return list(reversed(days))


def trading_day_lag(latest_available: date, expected_latest: date, *, holidays: set[date]) -> int:
    if latest_available >= expected_latest:
        return 0
    lag = 0
    current = latest_available + timedelta(days=1)
    while current <= expected_latest:
        if _is_trading_day(current, holidays):
            lag += 1
        current += timedelta(days=1)
    return lag


def _load_governance_price_frame(
    prices_path: Path,
    *,
    assets: ETFAssetsConfig,
    policy: DataQualityPolicyConfig,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    try:
        raw = read_price_frame(prices_path)
        extra_symbols = set(policy.data_quality.price_freshness.optional_assets)
        frame, _ = standardize_price_frame(
            raw,
            assets=assets,
            source_name=str(prices_path),
            extra_symbols=extra_symbols,
        )
        return frame, []
    except Exception as exc:  # noqa: BLE001 - report fail-closed data load issue.
        records = [
            {
                "asset": "PRICE_CACHE",
                "required": True,
                "latest_available_date": "",
                "expected_latest_trading_date": "",
                "trading_day_lag": None,
                "freshness_status": "missing",
                "blocking_status": "block",
                "severity": "critical",
                "reason": "price cache could not be loaded",
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
        ]
        return pd.DataFrame(), records


def _validation_prices(as_of: date, policy: DataQualityPolicyConfig) -> pd.DataFrame:
    holidays = set(policy.data_quality.price_freshness.market_holidays)
    days = recent_trading_days(as_of, 25, holidays=holidays)
    rows: list[dict[str, Any]] = []
    for symbol in policy.data_quality.price_freshness.required_assets:
        base = 100.0 if symbol != "CASH" else 1.0
        for index, item in enumerate(days):
            price = base if symbol == "CASH" else base + index
            rows.append(
                {
                    "date": item.isoformat(),
                    "symbol": symbol,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "adj_close": price,
                    "volume": 0 if symbol == "CASH" else 1000,
                    "source": "data_quality_validation_fixture",
                    "created_at": datetime.now(UTC).isoformat(),
                }
            )
    return pd.DataFrame(rows)


def _add_checker_probe(
    checks: list[dict[str, Any]],
    check_id: str,
    probe,
    *,
    required_key: str,
) -> None:
    try:
        result = probe()
        summary = _mapping(result.get("summary"))
        value = summary.get(required_key)
        passed = result.get("records") is not None and value is not None
        checks.append(
            _validation_check(
                check_id,
                "PASS" if passed else "FAIL",
                f"{check_id} probe completed"
                if passed
                else f"{check_id} probe returned incomplete summary",
                {"summary": summary},
            )
        )
    except Exception as exc:  # noqa: BLE001 - validation gate records failure.
        checks.append(
            _validation_check(
                check_id,
                "FAIL",
                f"{check_id} probe failed",
                {"error_type": type(exc).__name__, "error": str(exc)},
            )
        )


def _validation_report(
    *,
    as_of: date,
    generated_at: datetime,
    checks: list[dict[str, Any]],
    source_schema_versions: dict[str, str],
    safety: Mapping[str, Any],
) -> dict[str, Any]:
    failed = [check for check in checks if check.get("status") == "FAIL"]
    warnings = [check for check in checks if check.get("status") == "WARNING"]
    return {
        "schema_version": DATA_QUALITY_VALIDATION_SCHEMA_VERSION,
        "report_type": DATA_QUALITY_VALIDATION_REPORT_TYPE,
        "report_id": _report_id("data_quality_validation", as_of, generated_at),
        "as_of_date": as_of.isoformat(),
        "generated_at": generated_at.isoformat(),
        "status": "FAIL" if failed else "PASS",
        "checks": checks,
        "failed_check_count": len(failed),
        "warning_check_count": len(warnings),
        "source_schema_versions": source_schema_versions,
        "safety_banner": dict(safety),
        "read_only": True,
        "commands_executed": False,
        "production_state_mutated": False,
        "observe_only": safety.get("observe_only") is True,
        "candidate_only": safety.get("candidate_only") is True,
        "production_effect": _text(safety.get("production_effect"), "none"),
        "broker_action": _text(safety.get("broker_action"), "none"),
        "manual_review_required": safety.get("manual_review_required") is True,
    }


def _validation_check(
    check_id: str,
    status: Literal["PASS", "FAIL", "WARNING"],
    message: str,
    evidence: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "status": status,
        "message": message,
        "evidence": dict(evidence or {}),
    }


def _section(
    section_id: str,
    records: list[dict[str, Any]],
    *,
    summary_keys: Sequence[str],
) -> dict[str, Any]:
    payload = {"schema_version": f"etf_data_quality_{section_id}_v1", "records": records}
    return _refresh_section_summary(payload, summary_keys=summary_keys)


def _refresh_section_summary(
    payload: dict[str, Any],
    *,
    summary_keys: Sequence[str],
) -> dict[str, Any]:
    records = _records(payload.get("records"))
    summary: dict[str, Any] = {
        "record_count": len(records),
        "blocking_count": len([item for item in records if item.get("blocking_status") == "block"]),
        "warning_count": len([item for item in records if item.get("blocking_status") == "warn"]),
    }
    for key in summary_keys:
        counts: dict[str, int] = {}
        for item in records:
            value = _text(item.get(key), "unknown")
            counts[value] = counts.get(value, 0) + 1
        summary[f"{key}_counts"] = counts
    payload["summary"] = summary
    return payload


def _collect_findings(
    sections: Mapping[str, Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    blocking: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    for section_id, section in sections.items():
        for index, record in enumerate(_records(section.get("records"))):
            finding = {
                "finding_id": f"{section_id}:{index + 1}",
                "section": section_id,
                "severity": _text(record.get("severity"), "info"),
                "blocking_status": _text(record.get("blocking_status"), "none"),
                "status": _record_status(record),
                "summary": _finding_summary(record),
                "record": record,
            }
            if record.get("blocking_status") == "block":
                blocking.append(finding)
            elif record.get("blocking_status") == "warn" or finding["severity"] == "warning":
                warnings.append(finding)
    return blocking, warnings


def _manual_review_items(
    blocking_findings: Sequence[Mapping[str, Any]],
    warnings: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for finding in list(blocking_findings) + list(warnings):
        items.append(
            {
                "item_id": f"manual_review:{_text(finding.get('finding_id'))}",
                "source_finding_id": _text(finding.get("finding_id")),
                "severity": _text(finding.get("severity")),
                "required": finding.get("blocking_status") == "block",
                "owner_action": "review_data_quality_finding_before_interpreting_dependent_report",
                "summary": _text(finding.get("summary")),
            }
        )
    return items


def _source_links(sections: Mapping[str, Mapping[str, Any]]) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    for section_id, section in sections.items():
        for record in _records(section.get("records")):
            for key in ("artifact_path", "report_path", "reader_brief_path"):
                value = _text(record.get(key))
                if value:
                    links.append({"section": section_id, "path": value})
    seen: set[tuple[str, str]] = set()
    unique: list[dict[str, str]] = []
    for item in links:
        key = (item["section"], item["path"])
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def _markdown_section(title: str, section: Mapping[str, Any]) -> list[str]:
    summary = _mapping(section.get("summary"))
    lines = [
        "",
        f"## {title}",
        "",
        "| Field | Value |",
        "|---|---|",
        f"| record_count | {summary.get('record_count', 0)} |",
        f"| blocking_count | {summary.get('blocking_count', 0)} |",
        f"| warning_count | {summary.get('warning_count', 0)} |",
        "",
        "| # | Status | Blocking | Severity | Summary |",
        "|---:|---|---|---|---|",
    ]
    records = _records(section.get("records"))
    if not records:
        lines.append("| 0 | none | none | info | no findings |")
    for index, record in enumerate(records, start=1):
        lines.append(
            f"| {index} | `{_escape_md(_record_status(record))}` | "
            f"`{_escape_md(_text(record.get('blocking_status'), 'none'))}` | "
            f"`{_escape_md(_text(record.get('severity'), 'info'))}` | "
            f"{_escape_md(_finding_summary(record))} |"
        )
    return lines


def _markdown_findings(findings: Sequence[Mapping[str, Any]]) -> str:
    if not findings:
        return "无。"
    lines = ["| Finding | Severity | Summary |", "|---|---|---|"]
    for finding in findings:
        lines.append(
            f"| `{_escape_md(_text(finding.get('finding_id')))}` | "
            f"`{_escape_md(_text(finding.get('severity')))}` | "
            f"{_escape_md(_text(finding.get('summary')))} |"
        )
    return "\n".join(lines)


def _finding_summary(record: Mapping[str, Any]) -> str:
    for field in (
        "asset",
        "report_id",
        "gate_id",
        "evidence_type",
        "artifact_path",
        "reason",
    ):
        value = _text(record.get(field))
        if value:
            status = _record_status(record)
            return f"{field}={value}; status={status}"
    return f"status={_record_status(record)}"


def _record_status(record: Mapping[str, Any]) -> str:
    for key in (
        "freshness_status",
        "coverage_status",
        "outlier_status",
        "drift_status",
        "completeness_status",
        "staleness_status",
        "link_status",
    ):
        value = _text(record.get(key))
        if value:
            return value
    return "unknown"


def _normalized_price_frame(prices: pd.DataFrame) -> pd.DataFrame:
    if prices.empty:
        return prices
    frame = prices.copy()
    if "symbol" in frame.columns:
        frame["symbol"] = frame["symbol"].astype(str).str.upper()
    return frame


def _policy_assets(policy: PriceFreshnessPolicy) -> list[tuple[str, bool]]:
    required = [(symbol, True) for symbol in policy.required_assets]
    optional = [
        (symbol, False)
        for symbol in policy.optional_assets
        if symbol not in set(policy.required_assets)
    ]
    return required + optional


def _latest_symbol_date(frame: pd.DataFrame, symbol: str, *, as_of: date) -> date | None:
    dates = _symbol_dates(frame, symbol, as_of=as_of)
    return max(dates) if dates else None


def _symbol_dates(frame: pd.DataFrame, symbol: str, *, as_of: date) -> set[date]:
    if frame.empty or "symbol" not in frame.columns or "date" not in frame.columns:
        return set()
    selected = frame.loc[frame["symbol"].astype(str).str.upper() == symbol.upper()]
    parsed = pd.to_datetime(selected["date"], errors="coerce").dropna()
    return {item.date() for item in parsed if item.date() <= as_of}


def _is_trading_day(value: date, holidays: set[date]) -> bool:
    return value.weekday() < 5 and value not in holidays


def _blocking_status(*, required: bool, failed: bool) -> str:
    if not failed:
        return "pass"
    return "block" if required else "warn"


def _required_optional_ids(
    required_ids: Sequence[str],
    optional_ids: Sequence[str],
) -> list[tuple[str, bool]]:
    result = [(item, True) for item in required_ids]
    required = set(required_ids)
    result.extend((item, False) for item in optional_ids if item not in required)
    return result


def _report_index_record(report_index: Mapping[str, Any], report_id: str) -> dict[str, Any]:
    for report in _records(report_index.get("reports")):
        if _text(report.get("report_id")) == report_id:
            return dict(report)
    return {
        "report_id": report_id,
        "latest_artifact_path": "",
        "freshness_status": "MISSING",
        "artifact_status": "MISSING",
        "exists": False,
    }


def _path_from_report_record(report: Mapping[str, Any]) -> Path | None:
    value = _text(report.get("latest_artifact_path"))
    return Path(value) if value else None


def _read_json_object(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists() or path.suffix.lower() != ".json":
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _first_nested_value(payload: Any, keys: Sequence[str]) -> str:
    if isinstance(payload, Mapping):
        for key in keys:
            value = payload.get(key)
            if value not in (None, ""):
                return str(value)
        for value in payload.values():
            nested = _first_nested_value(value, keys)
            if nested:
                return nested
    elif isinstance(payload, list):
        for value in payload:
            nested = _first_nested_value(value, keys)
            if nested:
                return nested
    return ""


def _path_exists(payload: Mapping[str, Any], path: str) -> bool:
    return _get_path(payload, path) not in (None, "")


def _get_path(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current[part]
    return current


def _sample_count(payload: Mapping[str, Any], paths: Sequence[str]) -> int:
    for path in paths:
        value = _get_path(payload, path)
        parsed = _int_or_none(value)
        if parsed is not None:
            return parsed
    for key in ("sample_count", "source_count", "entry_count", "candidate_count"):
        value = _first_nested_value(payload, (key,))
        parsed = _int_or_none(value)
        if parsed is not None:
            return parsed
    return 0


def _registry_entry(registry: Mapping[str, Any], report_id: str) -> dict[str, Any] | None:
    for report in _records(registry.get("reports")):
        if _text(report.get("report_id")) == report_id:
            return dict(report)
    return None


def _safety_matches(value: Any) -> bool:
    safety = _mapping(value)
    return all(safety.get(key) == expected for key, expected in DATA_QUALITY_SAFETY.items())


def _parse_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _date_or_none(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    text = _text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _datetime_or_none(value: Any) -> datetime | None:
    text = _text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def _float_or_none(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return parsed


def _int_or_none(value: Any) -> int | None:
    try:
        if value in (None, ""):
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value)
    return text if text else default


def _unique_upper(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        symbol = str(value).strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        result.append(symbol)
    return result


def _escape_md(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


def _json_for_markdown(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _report_id(prefix: str, as_of: date, generated_at: datetime) -> str:
    timestamp = generated_at.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}:{as_of.isoformat()}:{timestamp}"


_CONFIG_HASH_KEYS = (
    "artifact_config_hash",
    "config_hash",
    "source_config_hash",
    "policy_config_hash",
    "current_config_hash",
)
_MODEL_VERSION_KEYS = (
    "artifact_model_version",
    "model_version",
    "current_model_version",
)
_STATUS_KEYS = (
    "status",
    "report_status",
    "gate_status",
    "validation_status",
    "health_status",
)
