from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.models import ETFConfigBundle, PolicyMetadata
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_ETF_PARAMETER_GOVERNANCE_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "governance.yaml"
)

GOVERNANCE_SUMMARY_SCHEMA_KEYS = (
    "schema_version",
    "report_type",
    "generated_at",
    "policy_version",
    "policy_config_hash",
    "current_model_version",
    "current_model_state",
    "candidate_model_version",
    "candidate_model_state",
    "candidate_source",
    "config_hash",
    "sample_period",
    "benchmark_comparison",
    "turnover_comparison",
    "drawdown_comparison",
    "no_lookahead_status",
    "promotion_status",
    "promotion_blockers",
    "manual_review_required",
    "production_effect",
    "p2_live_promotion_blocked",
    "source_candidate_path",
)

NO_CANDIDATE_BLOCKER = "candidate_missing"


class ETFParameterPromotionRules(BaseModel):
    allowed_candidate_states: list[str] = Field(min_length=1)
    eligible_status: str = Field(min_length=1)
    blocked_status: str = Field(min_length=1)
    no_candidate_status: str = Field(min_length=1)
    min_sample_trading_days: int = Field(ge=1)
    max_average_turnover: float = Field(ge=0, le=1)
    allow_justified_drawdown_worsening: bool
    require_all_tests_passed: bool
    require_shadow_mode: bool
    require_benchmark_comparison: bool
    require_no_lookahead_pass: bool
    reject_risk_increase_only: bool
    manual_review_required: bool
    production_effect: str = Field(min_length=1)
    p2_live_self_promotion_blocked: bool
    p2_self_promotion_sources: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_no_auto_production_effect(self) -> ETFParameterPromotionRules:
        if self.production_effect != "none":
            raise ValueError("ETF parameter governance must keep production_effect=none")
        if not self.manual_review_required:
            raise ValueError("ETF parameter governance must require manual review")
        return self


class ETFParameterGovernancePolicy(BaseModel):
    policy_metadata: PolicyMetadata
    model_states: dict[str, str]
    promotion_rules: ETFParameterPromotionRules

    @model_validator(mode="after")
    def validate_required_model_states(self) -> ETFParameterGovernancePolicy:
        required = {"production_baseline", "candidate", "shadow", "rejected", "archived"}
        missing = required - set(self.model_states)
        if missing:
            raise ValueError(
                "ETF parameter governance missing model states: "
                f"{', '.join(sorted(missing))}"
            )
        return self


def load_parameter_governance_policy(
    path: Path = DEFAULT_ETF_PARAMETER_GOVERNANCE_CONFIG_PATH,
) -> ETFParameterGovernancePolicy:
    return ETFParameterGovernancePolicy.model_validate(safe_load_yaml_path(path))


def read_parameter_candidate(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def evaluate_parameter_candidate(
    *,
    config: ETFConfigBundle,
    policy: ETFParameterGovernancePolicy,
    candidate: Mapping[str, Any] | None,
    candidate_path: Path | None = None,
    generated_at: datetime | None = None,
    policy_config_path: Path = DEFAULT_ETF_PARAMETER_GOVERNANCE_CONFIG_PATH,
) -> dict[str, Any]:
    rules = policy.promotion_rules
    generated = generated_at or datetime.now(UTC)
    blockers = _promotion_blockers(candidate, rules)
    if candidate is None:
        promotion_status = rules.no_candidate_status
    elif blockers:
        promotion_status = rules.blocked_status
    else:
        promotion_status = rules.eligible_status

    payload = {
        "schema_version": 1,
        "report_type": "etf_parameter_governance",
        "generated_at": generated.isoformat(),
        "policy_version": policy.policy_metadata.version,
        "policy_config_hash": _file_sha256(policy_config_path),
        "current_model_version": config.strategy.model.version,
        "current_model_state": "production_baseline",
        "candidate_model_version": _candidate_text(candidate, "candidate_model_version"),
        "candidate_model_state": _candidate_state(candidate),
        "candidate_source": _candidate_text(candidate, "candidate_source"),
        "config_hash": config.config_hash,
        "sample_period": _sample_period(candidate),
        "benchmark_comparison": _mapping_field(candidate, "benchmark_comparison"),
        "turnover_comparison": _mapping_field(candidate, "turnover_comparison"),
        "drawdown_comparison": _mapping_field(candidate, "drawdown_comparison"),
        "no_lookahead_status": _no_lookahead_status(candidate),
        "promotion_status": promotion_status,
        "promotion_blockers": blockers,
        "manual_review_required": True,
        "production_effect": "none",
        "p2_live_promotion_blocked": _p2_source(candidate, rules),
        "source_candidate_path": None if candidate_path is None else str(candidate_path),
    }
    return {key: payload[key] for key in GOVERNANCE_SUMMARY_SCHEMA_KEYS}


def render_parameter_governance_summary(payload: Mapping[str, Any]) -> str:
    blockers = payload.get("promotion_blockers")
    blocker_lines = (
        ["- none"] if not blockers else [f"- {blocker}" for blocker in blockers]
    )
    sample = payload.get("sample_period")
    sample_text = json.dumps(sample, ensure_ascii=False, sort_keys=True)
    benchmark = json.dumps(
        payload.get("benchmark_comparison"),
        ensure_ascii=False,
        sort_keys=True,
    )
    turnover = json.dumps(
        payload.get("turnover_comparison"),
        ensure_ascii=False,
        sort_keys=True,
    )
    drawdown = json.dumps(
        payload.get("drawdown_comparison"),
        ensure_ascii=False,
        sort_keys=True,
    )
    lines = [
        "# ETF Parameter Governance Summary",
        "",
        f"- Promotion Status: {payload.get('promotion_status')}",
        f"- Current Model Version: {payload.get('current_model_version')}",
        f"- Current Model State: {payload.get('current_model_state')}",
        f"- Candidate Model Version: {payload.get('candidate_model_version')}",
        f"- Candidate Model State: {payload.get('candidate_model_state')}",
        f"- Candidate Source: {payload.get('candidate_source')}",
        f"- Config Hash: `{payload.get('config_hash')}`",
        f"- Policy Version: {payload.get('policy_version')}",
        f"- Manual Review Required: {str(payload.get('manual_review_required')).lower()}",
        f"- Production Effect: {payload.get('production_effect')}",
        "",
        "## Required Comparisons",
        "",
        f"- Sample Period: `{sample_text}`",
        f"- Benchmark Comparison: `{benchmark}`",
        f"- Turnover Comparison: `{turnover}`",
        f"- Drawdown Comparison: `{drawdown}`",
        f"- No-Lookahead Status: {payload.get('no_lookahead_status')}",
        "",
        "## Promotion Blockers",
        "",
        *blocker_lines,
        "",
        "## Safety Boundary",
        "",
        "- production_effect=none",
        "- manual_review_required=true",
        "- candidate strategies cannot replace production_baseline automatically",
        "- P2/live candidates cannot self-promote to production effect",
    ]
    return "\n".join(lines) + "\n"


def write_parameter_governance_summary(
    payload: Mapping[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_path.write_text(render_parameter_governance_summary(payload), encoding="utf-8")
    return json_path, markdown_path


def _promotion_blockers(
    candidate: Mapping[str, Any] | None,
    rules: ETFParameterPromotionRules,
) -> list[str]:
    if candidate is None:
        return [NO_CANDIDATE_BLOCKER]

    blockers: list[str] = []
    candidate_state = _candidate_state(candidate)
    if candidate_state not in set(rules.allowed_candidate_states):
        blockers.append("candidate_state_not_allowed")
    if rules.require_all_tests_passed and not _truthy(candidate.get("tests_passed")):
        blockers.append("tests_not_passed")
    if rules.require_shadow_mode and not _truthy(candidate.get("shadow_mode")):
        blockers.append("shadow_mode_missing")
    sample_days = _sample_trading_days(candidate)
    if sample_days is None or sample_days < rules.min_sample_trading_days:
        blockers.append("sample_size_too_small")
    if rules.require_benchmark_comparison and not _mapping_field(candidate, "benchmark_comparison"):
        blockers.append("benchmark_comparison_missing")
    _turnover_blockers(candidate, rules, blockers)
    _drawdown_blockers(candidate, rules, blockers)
    if rules.reject_risk_increase_only and _truthy(
        _mapping_field(candidate, "risk_comparison").get("risk_increase_only")
    ):
        blockers.append("risk_increase_only")
    if rules.require_no_lookahead_pass and _no_lookahead_status(candidate) != "PASS":
        blockers.append("no_lookahead_not_passed")
    if _requested_production_effect(candidate) != "none":
        blockers.append("production_effect_requested")
    if _truthy(candidate.get("manual_review_required")) is False:
        blockers.append("manual_review_not_required")
    if _p2_source(candidate, rules):
        blockers.append("p2_live_self_promotion_blocked")
    return sorted(set(blockers))


def _turnover_blockers(
    candidate: Mapping[str, Any],
    rules: ETFParameterPromotionRules,
    blockers: list[str],
) -> None:
    comparison = _mapping_field(candidate, "turnover_comparison")
    turnover = _optional_float(
        _first_present(
            comparison,
            "candidate_avg_turnover",
            "average_turnover",
            "turnover",
        )
    )
    if turnover is None:
        blockers.append("turnover_comparison_missing")
        return
    threshold = _optional_float(comparison.get("threshold")) or rules.max_average_turnover
    if turnover > threshold:
        blockers.append("turnover_too_high")


def _drawdown_blockers(
    candidate: Mapping[str, Any],
    rules: ETFParameterPromotionRules,
    blockers: list[str],
) -> None:
    comparison = _mapping_field(candidate, "drawdown_comparison")
    candidate_drawdown = _optional_float(
        _first_present(comparison, "candidate_max_drawdown", "max_drawdown")
    )
    baseline_drawdown = _optional_float(comparison.get("baseline_max_drawdown"))
    if candidate_drawdown is None or baseline_drawdown is None:
        blockers.append("drawdown_comparison_missing")
        return
    if candidate_drawdown >= baseline_drawdown:
        return
    if rules.allow_justified_drawdown_worsening and (
        _truthy(comparison.get("drawdown_justified"))
        or bool(str(comparison.get("drawdown_justification") or "").strip())
    ):
        return
    blockers.append("drawdown_not_reduced_or_justified")


def _sample_period(candidate: Mapping[str, Any] | None) -> dict[str, Any]:
    if candidate is None:
        return {"start_date": None, "end_date": None, "trading_days": 0}
    raw = _mapping_field(candidate, "sample_period")
    return {
        "start_date": raw.get("start_date") or candidate.get("sample_start_date"),
        "end_date": raw.get("end_date") or candidate.get("sample_end_date"),
        "trading_days": _sample_trading_days(candidate) or 0,
    }


def _sample_trading_days(candidate: Mapping[str, Any]) -> int | None:
    raw = _mapping_field(candidate, "sample_period")
    value = _first_present(raw, "trading_days", "sample_size", "observations")
    if value is None:
        value = _first_present(candidate, "sample_trading_days", "sample_size")
    parsed = _optional_float(value)
    return None if parsed is None else int(parsed)


def _candidate_state(candidate: Mapping[str, Any] | None) -> str:
    if candidate is None:
        return "none"
    return str(candidate.get("candidate_model_state") or candidate.get("state") or "candidate")


def _candidate_text(candidate: Mapping[str, Any] | None, field: str) -> str:
    if candidate is None:
        return "none"
    return str(candidate.get(field) or "UNKNOWN")


def _mapping_field(candidate: Mapping[str, Any] | None, field: str) -> dict[str, Any]:
    if candidate is None:
        return {}
    value = candidate.get(field)
    return dict(value) if isinstance(value, Mapping) else {}


def _no_lookahead_status(candidate: Mapping[str, Any] | None) -> str:
    if candidate is None:
        return "MISSING"
    value = candidate.get("no_lookahead_validation")
    if isinstance(value, Mapping):
        return str(value.get("status") or "MISSING").upper()
    if isinstance(value, str):
        return value.upper()
    if isinstance(value, bool):
        return "PASS" if value else "FAIL"
    return "MISSING"


def _requested_production_effect(candidate: Mapping[str, Any]) -> str:
    return str(
        candidate.get("requested_production_effect")
        or candidate.get("production_effect")
        or "none"
    )


def _p2_source(
    candidate: Mapping[str, Any] | None,
    rules: ETFParameterPromotionRules,
) -> bool:
    if candidate is None or not rules.p2_live_self_promotion_blocked:
        return False
    source = str(candidate.get("candidate_source") or "")
    return source in set(rules.p2_self_promotion_sources)


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_present(mapping: Mapping[str, Any], *keys: str) -> object:
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def _file_sha256(path: Path) -> str:
    if not path.exists():
        return "missing"
    return sha256(path.read_bytes()).hexdigest()
