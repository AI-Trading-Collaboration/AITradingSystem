from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system.config import (
    DEFAULT_DATA_QUALITY_CONFIG_PATH,
    DEFAULT_MARKET_REGIMES_CONFIG_PATH,
    PROJECT_ROOT,
    load_market_regimes,
    market_regime_by_id,
)
from ai_trading_system.contracts.research_context import (
    DataQualityContractRef,
    DateRange,
    EffectiveCoverage,
    MarketRegimeSpec,
    PolicyRef,
    ResearchContextError,
    ResearchEvaluationContext,
    ResearchWindowSpec,
    require_research_evaluation_context,
    resolve_blocked_research_context,
    resolve_complete_research_context,
)
from ai_trading_system.contracts.status import EvidenceRole, PolicyRole, ResearchWindowRole
from ai_trading_system.research_audit_metadata import (
    DEFAULT_PRIMARY_RESEARCH_WINDOW_POLICY_PATH,
)
from ai_trading_system.research_window_extension import (
    DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH,
    load_research_window_registry,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_TRADING_CALENDAR = "XNYS"
_EVIDENCE_ROLE_BY_WINDOW_ROLE = {
    ResearchWindowRole.PRIMARY_VALIDATED: EvidenceRole.PRIMARY_DECISION_EVIDENCE,
    ResearchWindowRole.LEGACY_COMPARISON: EvidenceRole.LEGACY_COMPARISON_EVIDENCE,
    ResearchWindowRole.SENSITIVITY: EvidenceRole.SENSITIVITY_EVIDENCE_WITH_CAVEAT,
    ResearchWindowRole.PROXY_ROBUSTNESS: EvidenceRole.DIAGNOSTIC_ONLY,
    ResearchWindowRole.METADATA_ONLY: EvidenceRole.METADATA_ONLY,
}


def resolve_legacy_research_context(
    *,
    market_regime_id: str,
    research_window_id: str,
    requested_range: DateRange,
    as_of: date,
    data_quality_status: str,
    data_quality_passed: bool,
    data_quality_contract_id: str,
    actual_data_range: DateRange | None,
    effective_feature_start: date | None,
    effective_prediction_start: date | None,
    actual_portfolio_start: date | None,
    evaluation_range: DateRange | None,
    effective_coverage: EffectiveCoverage | None,
    blocking_issues: tuple[str, ...] = (),
    declared_regime_start: date | None = None,
    declared_research_window_start: date | None = None,
    trading_calendar: str = DEFAULT_TRADING_CALENDAR,
    data_quality_report_path: Path | None = None,
    extra_caveats: tuple[str, ...] = (),
    extra_policy_refs: tuple[PolicyRef, ...] = (),
    market_regimes_path: Path = DEFAULT_MARKET_REGIMES_CONFIG_PATH,
    research_window_registry_path: Path = DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH,
    primary_window_policy_path: Path = DEFAULT_PRIMARY_RESEARCH_WINDOW_POLICY_PATH,
    data_quality_policy_path: Path = DEFAULT_DATA_QUALITY_CONFIG_PATH,
) -> ResearchEvaluationContext:
    regime_config = market_regime_by_id(load_market_regimes(market_regimes_path), market_regime_id)
    registry = load_research_window_registry(research_window_registry_path)
    raw_window = registry["windows"].get(research_window_id)
    if not isinstance(raw_window, Mapping):
        raise ResearchContextError(
            "UNKNOWN_RESEARCH_WINDOW", f"research window not configured: {research_window_id}"
        )
    role = _window_role(raw_window.get("role"))
    window_start = _required_date(raw_window.get("start"), "research_window.start")
    window = ResearchWindowSpec(
        window_id=research_window_id,
        start_date=window_start,
        role=role,
        evidence_role=_EVIDENCE_ROLE_BY_WINDOW_ROLE[role],
        caveats=tuple(str(item) for item in raw_window.get("caveats", []) or []),
    )
    regime = MarketRegimeSpec(
        regime_id=regime_config.regime_id,
        anchor_date=regime_config.anchor_date,
        start_date=regime_config.start_date,
    )
    policy_refs = (
        _policy_ref(
            market_regimes_path,
            fallback_id="market_regimes",
            role=PolicyRole.MARKET_REGIME,
        ),
        _policy_ref(
            research_window_registry_path,
            fallback_id="research_window_registry",
            role=PolicyRole.RESEARCH_WINDOW,
        ),
        _policy_ref(
            primary_window_policy_path,
            fallback_id="primary_research_window_policy",
            role=PolicyRole.RESEARCH_WINDOW_POLICY,
        ),
        _policy_ref(
            data_quality_policy_path,
            fallback_id="data_quality",
            role=PolicyRole.DATA_QUALITY,
        ),
        *extra_policy_refs,
    )
    data_quality_policy_id = next(
        item.policy_id for item in policy_refs if item.role is PolicyRole.DATA_QUALITY
    )
    quality = DataQualityContractRef(
        contract_id=data_quality_contract_id,
        status=data_quality_status,
        passed=data_quality_passed,
        as_of=as_of,
        policy_ref_id=data_quality_policy_id,
        report_path=(
            None if data_quality_report_path is None else _portable_path(data_quality_report_path)
        ),
        report_sha256=(
            None
            if data_quality_report_path is None or not data_quality_report_path.is_file()
            else _sha256(data_quality_report_path)
        ),
    )
    common: dict[str, Any] = {
        "regime": regime,
        "window": window,
        "requested_range": requested_range,
        "as_of": as_of,
        "trading_calendar": trading_calendar,
        "data_quality": quality,
        "policy_refs": policy_refs,
        "caveats": extra_caveats,
        "declared_regime_start": declared_regime_start,
        "declared_research_window_start": declared_research_window_start,
    }
    if blocking_issues:
        return resolve_blocked_research_context(
            **common,
            blocking_issues=blocking_issues,
            actual_data_range=actual_data_range,
            effective_feature_start=effective_feature_start,
            effective_prediction_start=effective_prediction_start,
            actual_portfolio_start=actual_portfolio_start,
            evaluation_range=evaluation_range,
            effective_coverage=effective_coverage,
        )
    missing = [
        name
        for name, value in {
            "actual_data_range": actual_data_range,
            "effective_feature_start": effective_feature_start,
            "effective_prediction_start": effective_prediction_start,
            "actual_portfolio_start": actual_portfolio_start,
            "evaluation_range": evaluation_range,
            "effective_coverage": effective_coverage,
        }.items()
        if value is None
    ]
    if missing:
        raise ResearchContextError(
            "LEGACY_CONTEXT_FIELD_MISSING",
            "complete legacy context requires explicit " + ",".join(missing),
        )
    assert actual_data_range is not None
    assert effective_feature_start is not None
    assert effective_prediction_start is not None
    assert actual_portfolio_start is not None
    assert evaluation_range is not None
    assert effective_coverage is not None
    return resolve_complete_research_context(
        **common,
        actual_data_range=actual_data_range,
        effective_feature_start=effective_feature_start,
        effective_prediction_start=effective_prediction_start,
        actual_portfolio_start=actual_portfolio_start,
        evaluation_range=evaluation_range,
        effective_coverage=effective_coverage,
    )


def attach_research_context(
    payload: Mapping[str, Any], context: ResearchEvaluationContext
) -> dict[str, Any]:
    _validate_legacy_flat_context_parity(payload, context)
    serialized = context.to_dict()
    existing = payload.get("research_evaluation_context")
    if existing is not None and existing != serialized:
        raise ResearchContextError(
            "LEGACY_CONTEXT_OVERWRITE_CONFLICT",
            "payload already has a different research_evaluation_context",
        )
    enriched = {
        **dict(payload),
        "research_evaluation_context_id": context.context_id,
        "research_evaluation_context": serialized,
    }
    require_research_evaluation_context(enriched)
    return enriched


def _validate_legacy_flat_context_parity(
    payload: Mapping[str, Any], context: ResearchEvaluationContext
) -> None:
    expected = {
        "market_regime": context.market_regime_id,
        "research_window_id": context.research_window_id,
        "anchor_date": context.regime_anchor.isoformat(),
        "requested_start": context.requested_range.start.isoformat(),
        "as_of": context.as_of.isoformat(),
    }
    for field, expected_value in expected.items():
        actual = payload.get(field)
        if actual not in (None, "") and str(actual) != expected_value:
            raise ResearchContextError(
                "LEGACY_FLAT_CONTEXT_CONFLICT",
                f"{field}={actual!r} conflicts with {expected_value!r}",
            )
    requested = payload.get("requested_date_range")
    if isinstance(requested, Mapping):
        start = requested.get("start")
        end = requested.get("end")
        if start not in (None, "") and str(start) != context.requested_range.start.isoformat():
            raise ResearchContextError(
                "LEGACY_FLAT_CONTEXT_CONFLICT",
                "requested_date_range.start conflicts with context",
            )
        if end not in (None, "", "latest") and str(end) != context.requested_range.end.isoformat():
            raise ResearchContextError(
                "LEGACY_FLAT_CONTEXT_CONFLICT",
                "requested_date_range.end conflicts with context",
            )


def _window_role(value: object) -> ResearchWindowRole:
    try:
        return ResearchWindowRole(str(value))
    except ValueError as exc:
        raise ResearchContextError(
            "UNKNOWN_RESEARCH_WINDOW_ROLE", f"unsupported window role: {value!r}"
        ) from exc


def _required_date(value: object, field: str) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise ResearchContextError("INVALID_CONTEXT_DATE", f"{field}={value!r}") from exc
    raise ResearchContextError("INVALID_CONTEXT_DATE", f"{field}={value!r}")


def _policy_ref(path: Path, *, fallback_id: str, role: PolicyRole) -> PolicyRef:
    raw = safe_load_yaml_path(path)
    payload = dict(raw) if isinstance(raw, Mapping) else {}
    return PolicyRef(
        policy_id=str(payload.get("policy_id") or fallback_id),
        role=role,
        version=str(payload.get("schema_version") or "legacy-unversioned"),
        status=str(payload.get("status") or "active-legacy"),
        path=_portable_path(path),
        sha256=_sha256(path),
    )


def _portable_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(PROJECT_ROOT.resolve()).as_posix()
    except ValueError:
        return resolved.as_posix()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
