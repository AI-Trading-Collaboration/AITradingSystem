from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from ai_trading_system.contracts import (
    ContextResolutionStatus,
    CoverageInterval,
    DataQualityContractRef,
    DateRange,
    EffectiveCoverage,
    EvidenceRole,
    MarketRegimeSpec,
    PolicyRef,
    PolicyRole,
    ResearchContextError,
    ResearchEvaluationContext,
    ResearchWindowRole,
    ResearchWindowSpec,
    require_research_evaluation_context,
    resolve_blocked_research_context,
    resolve_complete_research_context,
)

AS_OF = date(2026, 6, 30)
POLICY_HASH = "a" * 64


def test_complete_context_keeps_2021_window_distinct_from_2022_regime() -> None:
    context = _complete_context()

    assert context.status is ContextResolutionStatus.COMPLETE
    assert context.research_window_start == date(2021, 2, 22)
    assert context.regime_start == date(2022, 12, 1)
    assert context.assert_complete() is context
    assert context.context_id.startswith("research_context_")

    serialized = context.to_dict()
    restored = ResearchEvaluationContext.from_dict(serialized)
    assert restored == context
    assert restored.context_id == context.context_id
    assert restored.to_dict() == serialized


def test_context_id_is_independent_of_policy_and_coverage_input_order() -> None:
    first = _complete_context()
    second = resolve_complete_research_context(
        **_complete_kwargs(),
        policy_refs=tuple(reversed(_policy_refs())),
        effective_coverage=EffectiveCoverage(tuple(reversed(_coverage().intervals))),
    )

    assert first.context_id == second.context_id
    assert first.to_dict() == second.to_dict()


def test_exact_primary_window_declared_as_2022_fails_closed() -> None:
    with pytest.raises(ResearchContextError) as exc_info:
        resolve_complete_research_context(
            **_complete_kwargs(),
            policy_refs=_policy_refs(),
            effective_coverage=_coverage(),
            declared_research_window_start=date(2022, 12, 1),
        )

    assert exc_info.value.code == "RESEARCH_WINDOW_START_CONFLICT"


def test_evaluation_before_effective_prediction_start_fails_closed() -> None:
    with pytest.raises(ResearchContextError) as exc_info:
        resolve_complete_research_context(
            **{
                **_complete_kwargs(),
                "evaluation_range": DateRange(date(2022, 2, 21), AS_OF),
            },
            policy_refs=_policy_refs(),
            effective_coverage=_coverage(),
        )

    assert exc_info.value.code == "EVALUATION_BEFORE_EFFECTIVE_START"


def test_actual_range_outside_requested_fails_closed() -> None:
    with pytest.raises(ResearchContextError) as exc_info:
        resolve_complete_research_context(
            **{
                **_complete_kwargs(),
                "actual_data_range": DateRange(date(2021, 2, 19), AS_OF),
            },
            policy_refs=_policy_refs(),
            effective_coverage=_coverage(),
        )

    assert exc_info.value.code == "ACTUAL_RANGE_OUTSIDE_REQUESTED"


def test_blocked_context_preserves_missing_ranges_without_fabrication() -> None:
    context = resolve_blocked_research_context(
        regime=_regime(),
        window=_window(),
        requested_range=DateRange(date(2021, 2, 22), AS_OF),
        as_of=AS_OF,
        trading_calendar="XNYS",
        data_quality=DataQualityContractRef(
            contract_id="secondary_cross_checked",
            status="FAIL",
            passed=False,
            as_of=AS_OF,
            policy_ref_id="data_quality",
        ),
        policy_refs=_policy_refs(),
        blocking_issues=("DATA_QUALITY_FAILED",),
    )

    payload = context.to_dict()
    assert context.status is ContextResolutionStatus.BLOCKED
    assert payload["actual_data_range"] is None
    assert payload["effective_coverage"] is None
    assert payload["evaluation_range"] is None
    with pytest.raises(ResearchContextError) as exc_info:
        context.assert_complete()
    assert exc_info.value.code == "RESEARCH_CONTEXT_BLOCKED"


def test_investment_artifact_context_requirement_fails_closed() -> None:
    with pytest.raises(ResearchContextError) as missing:
        require_research_evaluation_context({"status": "READY"})
    assert missing.value.code == "INVESTMENT_ARTIFACT_CONTEXT_MISSING"

    context = _complete_context()
    with pytest.raises(ResearchContextError) as mismatch:
        require_research_evaluation_context(
            {
                "research_evaluation_context_id": "wrong",
                "research_evaluation_context": context.to_dict(),
            }
        )
    assert mismatch.value.code == "INVESTMENT_ARTIFACT_CONTEXT_ID_MISMATCH"

    malformed = context.to_dict()
    malformed["status"] = "READYISH"
    with pytest.raises(ResearchContextError) as unknown_status:
        ResearchEvaluationContext.from_dict(malformed)
    assert unknown_status.value.code == "UNKNOWN_CONTEXT_STATUS"

    malformed = context.to_dict()
    malformed["data_quality"]["passed"] = "true"
    with pytest.raises(ResearchContextError) as invalid_bool:
        ResearchEvaluationContext.from_dict(malformed)
    assert invalid_bool.value.code == "INVALID_CONTEXT_BOOLEAN"


def test_contract_package_does_not_import_config_cli_report_or_domain_modules() -> None:
    root = Path("src/ai_trading_system/contracts")
    source = "\n".join(path.read_text(encoding="utf-8") for path in root.glob("*.py"))
    for forbidden in (
        "ai_trading_system.config",
        "ai_trading_system.cli",
        "ai_trading_system.reports",
        "ai_trading_system.backtest",
        "ai_trading_system.research_window_extension",
    ):
        assert forbidden not in source


def _dated_frame(start: str | None = None, end: str | None = None) -> pd.DataFrame:
    dates = [] if start is None or end is None else [start, end]
    return pd.DataFrame({"date": dates})


def _complete_context() -> ResearchEvaluationContext:
    return resolve_complete_research_context(
        **_complete_kwargs(),
        policy_refs=_policy_refs(),
        effective_coverage=_coverage(),
    )


def _complete_kwargs() -> dict[str, object]:
    return {
        "regime": _regime(),
        "window": _window(),
        "requested_range": DateRange(date(2021, 2, 22), AS_OF),
        "actual_data_range": DateRange(date(2021, 2, 22), AS_OF),
        "effective_feature_start": date(2021, 2, 22),
        "effective_prediction_start": date(2022, 2, 22),
        "actual_portfolio_start": date(2021, 2, 22),
        "evaluation_range": DateRange(date(2022, 2, 22), AS_OF),
        "as_of": AS_OF,
        "trading_calendar": "XNYS",
        "data_quality": DataQualityContractRef(
            contract_id="secondary_cross_checked",
            status="PASS_WITH_WARNINGS",
            passed=True,
            as_of=AS_OF,
            policy_ref_id="data_quality",
        ),
    }


def _regime() -> MarketRegimeSpec:
    return MarketRegimeSpec(
        regime_id="ai_after_chatgpt",
        anchor_date=date(2022, 11, 30),
        start_date=date(2022, 12, 1),
    )


def _window() -> ResearchWindowSpec:
    return ResearchWindowSpec(
        window_id="exact_three_asset_validated",
        start_date=date(2021, 2, 22),
        role=ResearchWindowRole.PRIMARY_VALIDATED,
        evidence_role=EvidenceRole.PRIMARY_DECISION_EVIDENCE,
    )


def _coverage() -> EffectiveCoverage:
    return EffectiveCoverage(
        (
            CoverageInterval(
                source_id="prices:QQQ_SGOV_TQQQ",
                date_range=DateRange(date(2021, 2, 22), AS_OF),
            ),
            CoverageInterval(
                source_id="features:first_layer",
                date_range=DateRange(date(2021, 2, 22), AS_OF),
            ),
        )
    )


def _policy_refs() -> tuple[PolicyRef, ...]:
    return (
        PolicyRef(
            policy_id="market_regimes",
            role=PolicyRole.MARKET_REGIME,
            version="legacy-unversioned",
            status="active-legacy",
            path="config/market_regimes.yaml",
            sha256=POLICY_HASH,
        ),
        PolicyRef(
            policy_id="research_window_registry_v1",
            role=PolicyRole.RESEARCH_WINDOW,
            version="research_window_registry.v1",
            status="pilot_baseline",
            path="config/research/research_window_registry.yaml",
            sha256="b" * 64,
        ),
        PolicyRef(
            policy_id="data_quality",
            role=PolicyRole.DATA_QUALITY,
            version="legacy-unversioned",
            status="active-legacy",
            path="config/data_quality.yaml",
            sha256="c" * 64,
        ),
    )
