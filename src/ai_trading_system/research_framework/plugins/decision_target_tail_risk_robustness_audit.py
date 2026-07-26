from __future__ import annotations

import copy
import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any, TypeAlias, cast

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from ai_trading_system.platform.artifacts import canonical_json_bytes, sha256_path
from ai_trading_system.research_framework.plugins import (
    ExperimentExecutionContext,
    PluginRegistry,
)
from ai_trading_system.research_framework.plugins import (
    decision_target_capability_audit_model_ladder as source_ladder,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

SCHEMA_VERSION = "decision_target_tail_risk_robustness_audit.v1"
SUMMARY_SCHEMA_VERSION = "decision_target_tail_risk_robustness_audit_summary.v1"
REPORT_TYPE = "decision_target_tail_risk_robustness_audit"
READY_STATUS = "TAIL_RISK_ROBUSTNESS_AUDIT_READY"
BLOCKED_STATUS = "BLOCKED_INPUT_OR_PROTOCOL"

_POLICY_SCHEMA = "decision_target_tail_risk_robustness_audit_policy.v1"
_FROZEN_POLICY_SHA256 = "e47b8dff80bbdc246250cedfb4eacf9f48d8d2323da3baa612c716d9eeeafb2b"
_OWNER_DECISION = (
    "owner_decision:TRADING-2462:2026-07-27:"
    "approve_tail_risk_capability_robustness_falsification_audit_v1"
)
_TASK_ID = "TRADING-2462_TAIL_RISK_CAPABILITY_ROBUSTNESS_FALSIFICATION_AUDIT"
_SOURCE_TASK_ID = "TRADING-2461_DECISION_TARGET_CAPABILITY_AUDIT_MODEL_LADDER"
_TARGET_ID = "QQQ_FUTURE_WORST_1D_RETURN"
_PRIMARY_HORIZONS = ("1d", "5d", "10d")
_ALL_HORIZONS = ("1d", "5d", "10d", "20d")
_MANDATORY_VARIANTS = (
    "EXACT_PRIMARY",
    "FEATURE_LAG_1",
    "EMBARGO_40",
    "DROP_SPY_DERIVED",
    "DROP_SGOV_DERIVED",
)
_DIAGNOSTIC_VARIANTS = (
    "PRICE_TREND_ONLY",
    "VOLATILITY_DRAWDOWN_ONLY",
    "CROSS_ASSET_STATE_ONLY",
)
_EXPECTED_FILE_KEYS = (
    "result",
    "summary",
    "envelope",
    "run_ledger",
    "input_snapshot",
    "label_payload",
    "source_package",
    "market_panel",
    "capability_receipt",
    "source_policy",
    "source_requirement",
    "source_implementation",
)
_STRUCTURED_SOURCE_BINDINGS = {
    "result": "source_result",
    "summary": "source_summary",
    "envelope": "source_envelope",
    "run_ledger": "source_run_ledger",
    "input_snapshot": "input_snapshot",
    "source_policy": "source_policy",
}
_SNAPSHOT_ROLE_TO_FILE_KEY = {
    "label_payload": "label_payload",
    "source_package": "source_package",
    "market_panel": "market_panel",
    "capability_receipt": "capability_receipt",
}
_ALLOWED_DECISIONS = frozenset(
    {
        "TAIL_RISK_CAPABILITY_ROBUST",
        "TAIL_RISK_CAPABILITY_FRAGILE",
        "TAIL_RISK_CAPABILITY_FALSIFIED",
        "INSUFFICIENT_ROBUSTNESS_EVIDENCE",
    }
)
_PROJECT_ROOT = Path(__file__).resolve().parents[4]
_NUMERIC_EPSILON = 1.0e-12

FloatArray: TypeAlias = NDArray[np.float64]


def build_tail_risk_robustness_payload(
    sources: Mapping[str, Any],
    *,
    as_of: date,
) -> dict[str, Any]:
    policy = _mapping(sources.get("audit_policy"))
    errors = list(_policy_errors(policy, sources=sources, as_of=as_of))
    loaded, records, load_errors = _load_verified_inputs(policy, sources=sources)
    errors.extend(load_errors)
    if errors:
        return _blocked_payload(
            policy=policy,
            errors=errors,
            records=records,
            as_of=as_of,
        )

    source_policy = _mapping(loaded["source_policy"])
    source_result = _mapping(loaded["result"])
    label_payload = _mapping(loaded["label_payload"])
    panel = loaded["market_panel"]
    input_snapshot = _mapping(loaded["input_snapshot"])
    assert isinstance(panel, pd.DataFrame)

    errors.extend(
        _source_identity_errors(
            policy=policy,
            source_policy=source_policy,
            source_result=source_result,
            source_summary=_mapping(loaded["summary"]),
            input_snapshot=input_snapshot,
            as_of=as_of,
        )
    )
    if errors:
        return _blocked_payload(
            policy=policy,
            errors=errors,
            records=records,
            as_of=as_of,
        )

    rebuilt = _rebuild_source_result(
        policy=policy,
        source_policy=source_policy,
        input_snapshot=input_snapshot,
        source_requirement_text=str(loaded["source_requirement"]),
        as_of=as_of,
    )
    source_evaluation = _mapping(source_result.get("evaluation"))
    exact_reconstruction_pass = bool(
        rebuilt.get("status") == source_ladder.READY_STATUS
        and rebuilt.get("evaluation") == source_evaluation
        and rebuilt.get("evaluation_commitment_sha256")
        == source_result.get("evaluation_commitment_sha256")
        and rebuilt.get("capability_summary") == source_result.get("capability_summary")
        and rebuilt.get("style_classification") == source_result.get("style_classification")
    )

    feature_frame, feature_ids_by_family = source_ladder._build_feature_frame(
        source_policy,
        panel,
    )
    label_frame = source_ladder._label_frame(label_payload)
    all_feature_ids = [
        feature_id
        for family in source_ladder._EXPECTED_FAMILIES
        for feature_id in feature_ids_by_family[family]
    ]
    eligible_dates = [
        decision_date
        for decision_date in source_ladder._common_label_dates(label_frame)
        if decision_date in feature_frame.index
        and source_ladder._row_features_present(
            feature_frame,
            decision_date,
            all_feature_ids,
        )
    ]
    feature_roles = _feature_source_roles(source_policy)

    variant_results: list[dict[str, Any]] = []
    variant_contexts: dict[str, dict[tuple[str, str], dict[str, Any]]] = {}
    for variant in _mapping_rows(_mapping(policy.get("variant_policy")).get("variants")):
        result, contexts = _evaluate_variant(
            audit_policy=policy,
            source_policy=source_policy,
            variant=variant,
            feature_frame=feature_frame,
            feature_ids_by_family=feature_ids_by_family,
            feature_roles=feature_roles,
            label_frame=label_frame,
            eligible_dates=eligible_dates,
        )
        variant_results.append(result)
        variant_contexts[str(variant["variant_id"])] = contexts

    exact_variant = _variant_by_id(variant_results, "EXACT_PRIMARY")
    exact_reconstruction_pass = exact_reconstruction_pass and _exact_variant_matches_source(
        exact_variant,
        source_evaluation=source_evaluation,
    )
    mandatory_variants_pass = all(
        bool(_variant_by_id(variant_results, variant_id).get("target_supported"))
        for variant_id in _MANDATORY_VARIANTS
    )

    fold_influence = _fold_influence_audit(
        policy=policy,
        variant_results=variant_results,
    )
    exact_contexts = variant_contexts["EXACT_PRIMARY"]
    regime = _regime_concentration_audit(
        policy=policy,
        contexts=exact_contexts,
        feature_frame=feature_frame,
    )
    calibration = _event_calibration_audit(
        policy=policy,
        contexts=exact_contexts,
    )
    placebo = _placebo_audit(
        policy=policy,
        contexts=exact_contexts,
    )

    evaluability_errors = _evaluability_errors(
        variant_results=variant_results,
        fold_influence=fold_influence,
        regime=regime,
        calibration=calibration,
        placebo=placebo,
    )
    decision = _decision(
        exact_reconstruction_pass=exact_reconstruction_pass,
        mandatory_variants_pass=mandatory_variants_pass,
        fold_influence_pass=bool(fold_influence["overall_pass"]),
        regime_pass=bool(regime["overall_pass"]),
        calibration_pass=bool(calibration["overall_pass"]),
        placebo_pass=bool(placebo["overall_pass"]),
        evaluability_errors=evaluability_errors,
        variant_results=variant_results,
        placebo=placebo,
    )
    gate_summary = {
        "exact_reconstruction_pass": exact_reconstruction_pass,
        "all_mandatory_variants_pass": mandatory_variants_pass,
        "fold_influence_pass": bool(fold_influence["overall_pass"]),
        "regime_concentration_pass": bool(regime["overall_pass"]),
        "event_calibration_pass": bool(calibration["overall_pass"]),
        "placebo_rejection_pass": bool(placebo["overall_pass"]),
        "evaluability_errors": evaluability_errors,
    }
    audit_summary = _build_summary(
        decision=decision,
        gate_summary=gate_summary,
        variant_results=variant_results,
        fold_influence=fold_influence,
        regime=regime,
        calibration=calibration,
        placebo=placebo,
    )
    source_commitment = {
        "source_task_id": _SOURCE_TASK_ID,
        "source_evaluation_commitment_sha256": source_result.get("evaluation_commitment_sha256"),
        "records": records,
    }
    evaluation = {
        "schema_version": SCHEMA_VERSION,
        "requested_range": source_evaluation.get("requested_range"),
        "evaluated_range": source_evaluation.get("evaluated_range"),
        "source_fold_ledger": source_evaluation.get("fold_ledger"),
        "source_reconstruction": {
            "status": rebuilt.get("status"),
            "evaluation_commitment_sha256": rebuilt.get("evaluation_commitment_sha256"),
            "exact_match": exact_reconstruction_pass,
        },
        "variant_results": variant_results,
        "fold_influence": fold_influence,
        "regime_concentration": regime,
        "event_calibration": calibration,
        "placebo": placebo,
        "gate_summary": gate_summary,
        "decision": decision,
    }
    evaluation_commitment = hashlib.sha256(canonical_json_bytes(evaluation)).hexdigest()
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "task_id": _TASK_ID,
        "status": READY_STATUS,
        "report_type": REPORT_TYPE,
        "validation_status": "CONTENT_DERIVED_REBUILD_REQUIRED",
        "strict_validation_errors": [],
        "source_commitment": source_commitment,
        "data_quality_scope": {
            "full_canonical_status": "FAIL",
            "scoped_status": "PASS",
            "global_cache_pass_claimed": False,
        },
        "data_quality_evidence": source_result.get("data_quality_evidence"),
        "evaluation": evaluation,
        "evaluation_commitment_sha256": evaluation_commitment,
        "audit_summary": audit_summary,
        "decision": decision,
        "next_route": (
            "OWNER_MAY_AUTHORIZE_SEPARATE_DECISION_VALUE_AUDIT"
            if decision["decision_status"] == "TAIL_RISK_CAPABILITY_ROBUST"
            else "CLOSE_TAIL_RISK_PATH_OR_REDESIGN_DECISION_TARGET"
        ),
        **_safety_payload(policy),
    }
    payload["rendered_markdown"] = render_tail_risk_robustness_markdown(payload)
    return payload


def validate_tail_risk_robustness_payload(
    payload: Mapping[str, Any],
    sources: Mapping[str, Any],
    *,
    as_of: date,
) -> tuple[str, ...]:
    expected = build_tail_risk_robustness_payload(sources, as_of=as_of)
    errors: list[str] = []
    comparisons = (
        ("status", "TAIL_RISK_AUDIT_STATUS_MISMATCH"),
        ("strict_validation_errors", "TAIL_RISK_AUDIT_STRICT_ERRORS_MISMATCH"),
        ("source_commitment", "TAIL_RISK_AUDIT_SOURCE_COMMITMENT_MISMATCH"),
        ("data_quality_scope", "TAIL_RISK_AUDIT_DATA_QUALITY_MISMATCH"),
        ("data_quality_evidence", "TAIL_RISK_AUDIT_DATA_QUALITY_EVIDENCE_MISMATCH"),
        ("evaluation", "TAIL_RISK_AUDIT_EVALUATION_MISMATCH"),
        ("evaluation_commitment_sha256", "TAIL_RISK_AUDIT_COMMITMENT_MISMATCH"),
        ("audit_summary", "TAIL_RISK_AUDIT_SUMMARY_MISMATCH"),
        ("decision", "TAIL_RISK_AUDIT_DECISION_MISMATCH"),
        ("next_route", "TAIL_RISK_AUDIT_ROUTE_MISMATCH"),
    )
    for field, code in comparisons:
        if payload.get(field) != expected.get(field):
            errors.append(code)
    for field, expected_value in _safety_payload(_mapping(sources.get("audit_policy"))).items():
        if payload.get(field) != expected_value:
            errors.append("TAIL_RISK_AUDIT_SAFETY_MISMATCH")
            break
    if payload.get("rendered_markdown") != render_tail_risk_robustness_markdown(payload):
        errors.append("TAIL_RISK_AUDIT_MARKDOWN_MISMATCH")
    return tuple(errors)


def tail_risk_robustness_summary(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    return _mapping(payload.get("audit_summary"))


def tail_risk_robustness_view_model(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "status": payload.get("status"),
        "audit_summary": payload.get("audit_summary"),
        "decision": payload.get("decision"),
        "next_route": payload.get("next_route"),
        "production_effect": payload.get("production_effect"),
        "broker_action": payload.get("broker_action"),
    }


def render_tail_risk_robustness_markdown(payload: Mapping[str, Any]) -> str:
    status = str(payload.get("status") or BLOCKED_STATUS)
    if status != READY_STATUS:
        errors = _string_list(payload.get("strict_validation_errors"))
        lines = "\n".join(f"- `{item}`" for item in errors) or "- `UNKNOWN_BLOCKER`"
        return (
            "# Decision Target Tail-Risk Robustness / Falsification Audit\n\n"
            f"- 状态：`{status}`\n"
            "- 输入或协议未通过，未形成稳健性结论。\n\n"
            "## 阻塞原因\n\n"
            f"{lines}\n\n"
            "## 安全边界\n\n"
            "- `risk_overlay_created=false`\n"
            "- `target_weights_generated=false`\n"
            "- `production_effect=none`\n"
            "- `broker_action=none`\n"
        )

    evaluation = _mapping(payload.get("evaluation"))
    decision = _mapping(payload.get("decision"))
    summary = _mapping(payload.get("audit_summary"))
    gate = _mapping(evaluation.get("gate_summary"))
    variants = _mapping_rows(evaluation.get("variant_results"))
    variant_lines = [
        "|Variant|Mandatory|Passing horizons|Target supported|",
        "|---|---:|---|---:|",
    ]
    for row in variants:
        aggregate = _mapping(row.get("aggregate"))
        variant_lines.append(
            "|{variant}|{mandatory}|{horizons}|{supported}|".format(
                variant=row.get("variant_id"),
                mandatory=str(bool(row.get("mandatory"))).lower(),
                horizons=", ".join(_string_list(aggregate.get("passing_primary_horizons")))
                or "none",
                supported=str(bool(row.get("target_supported"))).lower(),
            )
        )
    gate_lines = [
        "|Gate|Pass|",
        "|---|---:|",
        f"|Exact reconstruction|{str(bool(gate.get('exact_reconstruction_pass'))).lower()}|",
        f"|Mandatory variants|{str(bool(gate.get('all_mandatory_variants_pass'))).lower()}|",
        f"|Fold influence|{str(bool(gate.get('fold_influence_pass'))).lower()}|",
        f"|Regime concentration|{str(bool(gate.get('regime_concentration_pass'))).lower()}|",
        f"|Event calibration|{str(bool(gate.get('event_calibration_pass'))).lower()}|",
        f"|Placebo rejection|{str(bool(gate.get('placebo_rejection_pass'))).lower()}|",
    ]
    evaluability_lines = (
        "\n".join(f"- `{error}`" for error in _string_list(gate.get("evaluability_errors")))
        or "- none"
    )
    regime = _mapping(evaluation.get("regime_concentration"))
    ineligible_regime = [
        row for row in _mapping_rows(regime.get("rows")) if not bool(row.get("eligible"))
    ]
    regime_lines = [
        "|Regime|Horizon|Stratum|Rows|Spearman|",
        "|---|---|---|---:|---:|",
    ]
    regime_lines.extend(
        "|{regime}|{horizon}|{stratum}|{rows}|{spearman:.4f}|".format(
            regime=row.get("regime_id"),
            horizon=row.get("horizon_id"),
            stratum=row.get("stratum"),
            rows=row.get("row_count"),
            spearman=float(row.get("spearman") or 0.0),
        )
        for row in ineligible_regime
    )
    calibration = _mapping(evaluation.get("event_calibration"))
    calibration_rows = _mapping_rows(calibration.get("rows"))
    calibration_lines = [
        "|Horizon|Tail quantile|Eligible folds / 7|Passing folds|",
        "|---|---:|---:|---:|",
    ]
    for horizon in _PRIMARY_HORIZONS:
        for event_quantile in (0.10, 0.20):
            subset = [
                row
                for row in calibration_rows
                if row.get("horizon_id") == horizon
                and math.isclose(float(row.get("event_quantile") or 0.0), event_quantile)
            ]
            calibration_lines.append(
                "|{horizon}|{quantile:.2f}|{eligible} / 7|{passing}|".format(
                    horizon=horizon,
                    quantile=event_quantile,
                    eligible=sum(bool(row.get("eligible")) for row in subset),
                    passing=sum(bool(row.get("fold_pass")) for row in subset),
                )
            )
    placebo = _mapping(evaluation.get("placebo"))
    placebo_lines = [
        "|Horizon|Actual Spearman|Null p95|Empirical p|Pass|",
        "|---|---:|---:|---:|---:|",
    ]
    placebo_lines.extend(
        "|{horizon}|{actual:.4f}|{null:.4f}|{p:.3f}|{passed}|".format(
            horizon=row.get("horizon_id"),
            actual=float(row.get("actual_spearman") or 0.0),
            null=float(row.get("null_percentile_spearman") or 0.0),
            p=float(row.get("empirical_p_value") or 0.0),
            passed=str(bool(row.get("horizon_pass"))).lower(),
        )
        for row in _mapping_rows(placebo.get("rows"))
    )
    return (
        "# Decision Target Tail-Risk Robustness / Falsification Audit\n\n"
        f"- 结论：`{decision.get('decision_status')}`\n"
        f"- 状态：`{status}`\n"
        f"- Primary target：`{_TARGET_ID}`\n"
        f"- Selected research window：`{summary.get('selected_range')}`\n"
        f"- Actual evaluated range：`{summary.get('evaluated_range')}`\n"
        "- 数据角色：historical-seen falsification audit，不是prospective/OOS业绩证明。\n\n"
        "## 证伪门禁\n\n"
        + "\n".join(gate_lines)
        + "\n\n## Variant 结果\n\n"
        + "\n".join(variant_lines)
        + "\n\n## 不足证据\n\n"
        + evaluability_lines
        + "\n\nRegime strata 预注册样本地板为80；以下 pooled strata 未达到地板：\n\n"
        + "\n".join(regime_lines)
        + "\n\nEvent calibration 每个 horizon/quantile 预注册要求至少6个eligible folds；"
        "实际如下：\n\n"
        + "\n".join(calibration_lines)
        + "\n\n## Placebo\n\n"
        + "\n".join(placebo_lines)
        + "\n\n## 解释\n\n"
        + str(decision.get("interpretation_zh") or "")
        + "\n\n## 下一步\n\n"
        + f"`{payload.get('next_route')}`。本任务没有建立risk overlay；只有`ROBUST`才允许Owner"
        "另立Decision Value Audit，仍不自动批准candidate或权重。\n\n"
        "## 数据质量与安全边界\n\n"
        "- full canonical DQ=`FAIL`\n"
        "- QQQ/SPY/SGOV scoped DQ=`PASS`\n"
        "- `global_cache_pass_claimed=false`\n"
        "- `candidate_family_created=false`\n"
        "- `risk_overlay_created=false`\n"
        "- `strategy_backtest_executed=false`\n"
        "- `target_weights_generated=false`\n"
        "- `qld_used_as_signal=false`\n"
        "- `production_effect=none`\n"
        "- `broker_action=none`\n"
    )


class DecisionTargetTailRiskRobustnessCalculator:
    plugin_id = "decision_target_tail_risk_robustness_audit_calculator"
    version = "v1"

    def calculate(self, context: ExperimentExecutionContext) -> dict[str, Any]:
        return build_tail_risk_robustness_payload(context.sources, as_of=context.as_of)


class DecisionTargetTailRiskRobustnessReport:
    plugin_id = "decision_target_tail_risk_robustness_audit_report"
    version = "v1"

    def section(self, payload: Mapping[str, Any], section_id: str) -> Mapping[str, Any]:
        if section_id == "audit_summary":
            return tail_risk_robustness_summary(payload)
        return {}

    def render_markdown(self, payload: Mapping[str, Any]) -> str:
        return render_tail_risk_robustness_markdown(payload)


def decision_target_tail_risk_robustness_registry() -> PluginRegistry:
    return PluginRegistry(
        calculators=[DecisionTargetTailRiskRobustnessCalculator()],
        reports=[DecisionTargetTailRiskRobustnessReport()],
    )


def _policy_errors(
    policy: Mapping[str, Any],
    *,
    sources: Mapping[str, Any],
    as_of: date,
) -> tuple[str, ...]:
    errors: list[str] = []
    if policy.get("schema_version") != _POLICY_SCHEMA:
        errors.append("AUDIT_POLICY_SCHEMA_INVALID")
    if hashlib.sha256(canonical_json_bytes(policy)).hexdigest() != _FROZEN_POLICY_SHA256:
        errors.append("AUDIT_FROZEN_POLICY_COMMITMENT_INVALID")
    if policy.get("owner_decision") != _OWNER_DECISION:
        errors.append("AUDIT_OWNER_DECISION_INVALID")
    if policy.get("protocol_frozen_before_detailed_result_rows") is not True:
        errors.append("AUDIT_PROTOCOL_FREEZE_INVALID")
    requirement = str(sources.get("requirement_text") or "")
    if _TASK_ID not in requirement or _OWNER_DECISION not in requirement:
        errors.append("AUDIT_REQUIREMENT_AUTHORITY_INVALID")
    source_requirement = str(sources.get("source_requirement_text") or "")
    if _SOURCE_TASK_ID not in source_requirement:
        errors.append("AUDIT_SOURCE_REQUIREMENT_INVALID")
    context = _mapping(policy.get("research_context"))
    if (
        context.get("requested_start") != "2021-02-22"
        or context.get("historical_seen_only") is not True
        or context.get("prospective_values_allowed") is not False
        or as_of.isoformat() != "2026-07-24"
    ):
        errors.append("AUDIT_RESEARCH_CONTEXT_INVALID")
    authority = _mapping(policy.get("input_authority"))
    if (
        authority.get("source_task_id") != _SOURCE_TASK_ID
        or authority.get("source_schema_version") != source_ladder.SCHEMA_VERSION
        or authority.get("source_status") != source_ladder.READY_STATUS
        or authority.get("source_style_status") != "TAIL_RISK_ONLY_SKILL"
        or authority.get("primary_target_id") != _TARGET_ID
        or tuple(_string_list(authority.get("primary_horizons"))) != _PRIMARY_HORIZONS
        or tuple(_string_list(authority.get("diagnostic_horizons"))) != ("20d",)
        or authority.get("full_canonical_status") != "FAIL"
        or authority.get("scoped_status") != "PASS"
        or authority.get("global_cache_pass_claimed") is not False
    ):
        errors.append("AUDIT_INPUT_AUTHORITY_INVALID")
    expected_files = _mapping(authority.get("expected_files"))
    if tuple(expected_files) != _EXPECTED_FILE_KEYS:
        errors.append("AUDIT_EXPECTED_FILES_INVALID")
    variants = _mapping_rows(_mapping(policy.get("variant_policy")).get("variants"))
    variant_ids = tuple(str(row.get("variant_id") or "") for row in variants)
    if variant_ids != _MANDATORY_VARIANTS + _DIAGNOSTIC_VARIANTS:
        errors.append("AUDIT_VARIANT_ORDER_INVALID")
    if any(
        bool(row.get("mandatory")) != (str(row.get("variant_id")) in _MANDATORY_VARIANTS)
        for row in variants
    ):
        errors.append("AUDIT_VARIANT_MANDATORY_FLAG_INVALID")
    placebo = _mapping(policy.get("placebo_gate"))
    if (
        placebo.get("kind") != "WITHIN_FOLD_BLOCK_PERMUTATION"
        or _int_value(placebo.get("block_sessions")) != 20
        or _int_value(placebo.get("replicate_count")) != 199
        or _int_value(placebo.get("random_seed")) != 2462
    ):
        errors.append("AUDIT_PLACEBO_POLICY_INVALID")
    safety = _mapping(policy.get("safety"))
    if safety != {
        "research_only": True,
        "historical_seen_only": True,
        "prospective_accessed": False,
        "candidate_family_created": False,
        "risk_overlay_created": False,
        "strategy_backtest_executed": False,
        "transaction_cost_model_applied": False,
        "target_weights_generated": False,
        "qld_used_as_signal": False,
        "paper_shadow_changed": False,
        "promotion_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }:
        errors.append("AUDIT_SAFETY_POLICY_INVALID")
    return tuple(errors)


def _load_verified_inputs(
    policy: Mapping[str, Any],
    *,
    sources: Mapping[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[str]]:
    authority = _mapping(policy.get("input_authority"))
    expected_files = _mapping(authority.get("expected_files"))
    loaded: dict[str, Any] = {}
    records: list[dict[str, Any]] = []
    errors: list[str] = []
    for key in _EXPECTED_FILE_KEYS:
        expected = _mapping(expected_files.get(key))
        path = _resolve_path(str(expected.get("path") or ""))
        if not path.is_file():
            errors.append(f"INPUT_{key.upper()}_MISSING")
            continue
        actual_size = path.stat().st_size
        actual_sha = sha256_path(path)
        if actual_size != _int_value(expected.get("size_bytes")) or actual_sha != str(
            expected.get("sha256") or ""
        ):
            errors.append(f"INPUT_{key.upper()}_COMMITMENT_MISMATCH")
            continue
        records.append(
            {
                "role": key,
                "path": str(path.resolve()),
                "size_bytes": actual_size,
                "sha256": actual_sha,
            }
        )
        try:
            if key == "market_panel":
                loaded[key] = pd.read_csv(path, low_memory=False)
            elif path.suffix.lower() in {".yaml", ".yml"}:
                loaded[key] = safe_load_yaml_path(path)
            elif path.suffix.lower() == ".json":
                loaded[key] = json.loads(path.read_text(encoding="utf-8"))
            else:
                loaded[key] = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, pd.errors.ParserError):
            errors.append(f"INPUT_{key.upper()}_PARSE_FAILED")
    for file_key, source_key in _STRUCTURED_SOURCE_BINDINGS.items():
        if file_key in loaded and _mapping(sources.get(source_key)) != _mapping(loaded[file_key]):
            errors.append(f"INPUT_{file_key.upper()}_OBJECT_MISMATCH")
    if "source_requirement" in loaded and str(sources.get("source_requirement_text") or "") != str(
        loaded["source_requirement"]
    ):
        errors.append("INPUT_SOURCE_REQUIREMENT_OBJECT_MISMATCH")
    return loaded, records, errors


def _source_identity_errors(
    *,
    policy: Mapping[str, Any],
    source_policy: Mapping[str, Any],
    source_result: Mapping[str, Any],
    source_summary: Mapping[str, Any],
    input_snapshot: Mapping[str, Any],
    as_of: date,
) -> tuple[str, ...]:
    errors: list[str] = []
    authority = _mapping(policy.get("input_authority"))
    evaluation = _mapping(source_result.get("evaluation"))
    style = _mapping(source_result.get("style_classification"))
    summary = _mapping(source_result.get("capability_summary"))
    if (
        source_result.get("schema_version") != source_ladder.SCHEMA_VERSION
        or source_result.get("task_id") != _SOURCE_TASK_ID
        or source_result.get("status") != source_ladder.READY_STATUS
        or source_result.get("as_of") != as_of.isoformat()
    ):
        errors.append("SOURCE_RESULT_IDENTITY_INVALID")
    computed_commitment = hashlib.sha256(canonical_json_bytes(evaluation)).hexdigest()
    if source_result.get(
        "evaluation_commitment_sha256"
    ) != computed_commitment or computed_commitment != authority.get(
        "evaluation_commitment_sha256"
    ):
        errors.append("SOURCE_EVALUATION_COMMITMENT_INVALID")
    if len(_sequence(evaluation.get("fold_ledger"))) != _int_value(
        authority.get("expected_fold_count")
    ) or len(_sequence(evaluation.get("predictions"))) != _int_value(
        authority.get("expected_prediction_row_count")
    ):
        errors.append("SOURCE_EVALUATION_COUNT_INVALID")
    if style.get("style_status") != authority.get("source_style_status"):
        errors.append("SOURCE_STYLE_INVALID")
    target_rows = {
        str(row.get("target_id")): row for row in _mapping_rows(style.get("target_capabilities"))
    }
    primary = _mapping(target_rows.get(_TARGET_ID))
    if (
        primary.get("status") != "TARGET_SKILL_SUPPORTED"
        or tuple(_string_list(primary.get("passing_horizons"))) != _PRIMARY_HORIZONS
    ):
        errors.append("SOURCE_PRIMARY_TARGET_INVALID")
    if _mapping(source_summary.get("capability_summary")) != summary:
        errors.append("SOURCE_SUMMARY_BINDING_INVALID")
    if (
        _int_value(summary.get("fold_count")) != 7
        or _int_value(summary.get("prediction_row_count")) != 118300
        or summary.get("classification_model") != "M1_RIDGE_LINEAR"
        or summary.get("classification_feature_prefix") != "CROSS_ASSET_STATE"
    ):
        errors.append("SOURCE_SUMMARY_IDENTITY_INVALID")
    dq = _mapping(source_result.get("data_quality_scope"))
    if (
        dq.get("full_canonical_status") != "FAIL"
        or dq.get("scoped_status") != "PASS"
        or dq.get("global_cache_pass_claimed") is not False
    ):
        errors.append("SOURCE_DATA_QUALITY_DISCLOSURE_INVALID")
    if (
        source_result.get("candidate_family_created") is not False
        or source_result.get("strategy_backtest_executed") is not False
        or source_result.get("target_weights_generated") is not False
        or source_result.get("qld_used_as_signal") is not False
        or source_result.get("production_effect") != "none"
        or source_result.get("broker_action") != "none"
    ):
        errors.append("SOURCE_SAFETY_BOUNDARY_INVALID")
    if input_snapshot.get("schema_version") != source_ladder.INPUT_SCHEMA_VERSION:
        errors.append("SOURCE_INPUT_SNAPSHOT_SCHEMA_INVALID")
    if source_policy.get("schema_version") != source_ladder._EXPECTED_POLICY_SCHEMA:
        errors.append("SOURCE_POLICY_SCHEMA_INVALID")
    return tuple(errors)


def _rebuild_source_result(
    *,
    policy: Mapping[str, Any],
    source_policy: Mapping[str, Any],
    input_snapshot: Mapping[str, Any],
    source_requirement_text: str,
    as_of: date,
) -> dict[str, Any]:
    portable_snapshot = copy.deepcopy(dict(input_snapshot))
    records = _mapping(portable_snapshot.get("records"))
    expected_files = _mapping(_mapping(policy.get("input_authority")).get("expected_files"))
    for role, file_key in _SNAPSHOT_ROLE_TO_FILE_KEY.items():
        record = _mapping(records.get(role))
        expected = _mapping(expected_files.get(file_key))
        record["path"] = str(_resolve_path(str(expected.get("path"))))
        records[role] = record
    portable_snapshot["records"] = records
    return dict(
        source_ladder.build_capability_payload(
            {
                "audit_policy": source_policy,
                "input_snapshot": portable_snapshot,
                "requirement_text": source_requirement_text,
            },
            as_of=as_of,
        )
    )


def _evaluate_variant(
    *,
    audit_policy: Mapping[str, Any],
    source_policy: Mapping[str, Any],
    variant: Mapping[str, Any],
    feature_frame: pd.DataFrame,
    feature_ids_by_family: Mapping[str, Sequence[str]],
    feature_roles: Mapping[str, frozenset[str]],
    label_frame: pd.DataFrame,
    eligible_dates: Sequence[str],
) -> tuple[dict[str, Any], dict[tuple[str, str], dict[str, Any]]]:
    variant_id = str(variant["variant_id"])
    feature_ids = _select_feature_ids(
        variant,
        feature_ids_by_family=feature_ids_by_family,
        feature_roles=feature_roles,
    )
    lag = _int_value(variant.get("feature_lag_sessions"))
    variant_frame = feature_frame.loc[:, feature_ids].shift(lag)
    variant_source_policy = copy.deepcopy(dict(source_policy))
    split = _mapping(variant_source_policy.get("split_policy"))
    split["embargo_sessions"] = _int_value(variant.get("embargo_sessions"))
    variant_source_policy["split_policy"] = split
    model_policy = _mapping(variant_source_policy.get("model_policy"))
    model_policy["primary_classification_feature_prefix"] = (
        "CROSS_ASSET_STATE" if variant_id == "EXACT_PRIMARY" else variant_id
    )
    variant_source_policy["model_policy"] = model_policy
    folds = source_ladder._build_folds(variant_source_policy, eligible_dates)
    label_rows = cast(
        list[dict[str, Any]],
        label_frame.to_dict(orient="records"),
    )
    label_lookup = {(str(row["horizon_id"]), str(row["decision_date"])): row for row in label_rows}
    model = next(
        row
        for row in source_ladder._mapping_rows(
            _mapping(source_policy.get("model_policy")).get("models")
        )
        if row.get("model_id") == "M1_RIDGE_LINEAR"
    )
    interactions = source_ladder._mapping_rows(
        _mapping(source_policy.get("model_policy")).get("interactions")
    )
    fold_metrics: list[dict[str, Any]] = []
    aggregate_input_metrics: list[dict[str, Any]] = []
    predictions: list[dict[str, Any]] = []
    aggregate_input_predictions: list[dict[str, Any]] = []
    contexts: dict[tuple[str, str], dict[str, Any]] = {}
    for fold in folds:
        for horizon in _ALL_HORIZONS:
            horizon_rows = {
                value: label_lookup[(horizon, value)]
                for value in eligible_dates
                if (horizon, value) in label_lookup
            }
            train_dates, coverage = source_ladder._eligible_train_dates(
                fold=fold,
                horizon_rows=horizon_rows,
            )
            train_dates = [
                value
                for value in train_dates
                if _features_present(variant_frame, value, feature_ids)
            ]
            test_dates = [
                str(value)
                for value in source_ladder._sequence(fold["_dates"]["test_dates"])
                if value in horizon_rows
                and _features_present(variant_frame, str(value), feature_ids)
            ]
            y_train = np.asarray(
                [
                    source_ladder._target_value(horizon_rows[value], _TARGET_ID)
                    for value in train_dates
                ],
                dtype=np.float64,
            )
            y_test = np.asarray(
                [
                    source_ladder._target_value(horizon_rows[value], _TARGET_ID)
                    for value in test_dates
                ],
                dtype=np.float64,
            )
            x_train = variant_frame.loc[train_dates, feature_ids].to_numpy(dtype=np.float64)
            x_test = variant_frame.loc[test_dates, feature_ids].to_numpy(dtype=np.float64)
            x_train, x_test = source_ladder._model_matrix(
                model_id="M1_RIDGE_LINEAR",
                train=pd.DataFrame(x_train, columns=feature_ids),
                test=pd.DataFrame(x_test, columns=feature_ids),
                feature_ids=feature_ids,
                interactions=interactions,
            )
            penalty = float(model.get("ridge_penalty") or 0.0)
            epsilon = float(
                _mapping(source_policy.get("model_policy")).get(
                    "standardization_zero_scale_epsilon"
                )
                or _NUMERIC_EPSILON
            )
            test_prediction = source_ladder._ridge_predict(
                x_train=x_train,
                y_train=y_train,
                x_test=x_test,
                penalty=penalty,
                zero_scale_epsilon=epsilon,
            )
            train_prediction = source_ladder._ridge_predict(
                x_train=x_train,
                y_train=y_train,
                x_test=x_train,
                penalty=penalty,
                zero_scale_epsilon=epsilon,
            )
            m0_prediction: FloatArray = np.full(
                len(y_test),
                float(np.mean(y_train)) if len(y_train) else np.nan,
                dtype=np.float64,
            )
            m0_row = source_ladder._metric_row(
                fold_id=str(fold["fold_id"]),
                target_id=_TARGET_ID,
                horizon_id=horizon,
                model_id="M0_TRAIN_MEAN",
                feature_prefix="NONE",
                actual=y_test,
                predicted=m0_prediction,
                m0_predicted=m0_prediction,
                train_row_count=len(train_dates),
                test_dates=test_dates,
                coverage=coverage,
                policy=variant_source_policy,
            )
            primary_prefix = "CROSS_ASSET_STATE" if variant_id == "EXACT_PRIMARY" else variant_id
            metric_row = source_ladder._metric_row(
                fold_id=str(fold["fold_id"]),
                target_id=_TARGET_ID,
                horizon_id=horizon,
                model_id="M1_RIDGE_LINEAR",
                feature_prefix=primary_prefix,
                actual=y_test,
                predicted=test_prediction,
                m0_predicted=m0_prediction,
                train_row_count=len(train_dates),
                test_dates=test_dates,
                coverage=coverage,
                policy=variant_source_policy,
            )
            fold_metrics.append(metric_row)
            aggregate_input_metrics.extend((m0_row, metric_row))
            primary_predictions = source_ladder._prediction_rows(
                fold_id=str(fold["fold_id"]),
                target_id=_TARGET_ID,
                horizon_id=horizon,
                model_id="M1_RIDGE_LINEAR",
                feature_prefix=primary_prefix,
                dates=test_dates,
                actual=y_test,
                predicted=test_prediction,
            )
            m0_predictions = source_ladder._prediction_rows(
                fold_id=str(fold["fold_id"]),
                target_id=_TARGET_ID,
                horizon_id=horizon,
                model_id="M0_TRAIN_MEAN",
                feature_prefix="NONE",
                dates=test_dates,
                actual=y_test,
                predicted=m0_prediction,
            )
            predictions.extend(primary_predictions)
            aggregate_input_predictions.extend(m0_predictions)
            aggregate_input_predictions.extend(primary_predictions)
            contexts[(str(fold["fold_id"]), horizon)] = {
                "train_dates": train_dates,
                "test_dates": test_dates,
                "actual_train": y_train,
                "prediction_train": train_prediction,
                "actual_test": y_test,
                "prediction_test": test_prediction,
            }
    aggregates = source_ladder._aggregate_metrics(
        policy=variant_source_policy,
        fold_metrics=aggregate_input_metrics,
        predictions=aggregate_input_predictions,
    )
    primary_aggregates = [
        {
            **row,
            "variant_id": variant_id,
            "feature_prefix": variant_id,
        }
        for row in aggregates
        if row.get("model_id") == "M1_RIDGE_LINEAR"
    ]
    passing_primary = [
        horizon
        for horizon in _PRIMARY_HORIZONS
        if any(
            row.get("horizon_id") == horizon and row.get("capability_pass") is True
            for row in primary_aggregates
        )
    ]
    capability_gate = _mapping(audit_policy.get("capability_gate"))
    required_horizon = str(capability_gate.get("required_primary_horizon"))
    supported = bool(
        len(passing_primary)
        >= _int_value(capability_gate.get("minimum_supported_primary_horizons"))
        and required_horizon in passing_primary
    )
    fold_ledger = [{key: value for key, value in fold.items() if key != "_dates"} for fold in folds]
    return (
        {
            "variant_id": variant_id,
            "mandatory": bool(variant.get("mandatory")),
            "feature_selector": variant.get("feature_selector"),
            "selected_feature_ids": feature_ids,
            "selected_feature_count": len(feature_ids),
            "feature_lag_sessions": lag,
            "embargo_sessions": _int_value(variant.get("embargo_sessions")),
            "fold_ledger": fold_ledger,
            "fold_metrics": fold_metrics,
            "predictions": predictions,
            "aggregate_metrics": primary_aggregates,
            "aggregate": {
                "passing_primary_horizons": passing_primary,
                "diagnostic_20d_pass": any(
                    row.get("horizon_id") == "20d" and row.get("capability_pass") is True
                    for row in primary_aggregates
                ),
            },
            "target_supported": supported,
        },
        contexts,
    )


def _select_feature_ids(
    variant: Mapping[str, Any],
    *,
    feature_ids_by_family: Mapping[str, Sequence[str]],
    feature_roles: Mapping[str, frozenset[str]],
) -> list[str]:
    all_ids = [
        str(feature_id)
        for family in source_ladder._EXPECTED_FAMILIES
        for feature_id in feature_ids_by_family[family]
    ]
    selector = str(variant.get("feature_selector"))
    if selector == "ALL":
        selected = all_ids
    elif selector == "EXCLUDE_SOURCE_ROLE":
        excluded = str(variant.get("excluded_source_role"))
        selected = [
            feature_id for feature_id in all_ids if excluded not in feature_roles[feature_id]
        ]
    elif selector == "FAMILY_ONLY":
        family = str(variant.get("included_family"))
        selected = [str(value) for value in feature_ids_by_family[family]]
    else:
        raise ValueError(f"unsupported feature selector: {selector}")
    if not selected:
        raise ValueError(f"feature selector produced no features: {variant.get('variant_id')}")
    return selected


def _feature_source_roles(
    source_policy: Mapping[str, Any],
) -> dict[str, frozenset[str]]:
    definitions = source_ladder._mapping_rows(
        _mapping(source_policy.get("feature_policy")).get("features")
    )
    roles: dict[str, frozenset[str]] = {}
    for definition in definitions:
        feature_id = str(definition["feature_id"])
        kind = str(definition["kind"])
        if kind in {"total_return", "realized_volatility", "current_drawdown"}:
            observed = {str(definition["asset"])}
        elif kind == "relative_return":
            observed = {str(definition["asset"]), str(definition["benchmark"])}
        elif kind == "difference":
            observed = set(roles[str(definition["left_feature"])])
            observed.update(roles[str(definition["right_feature"])])
        else:
            raise ValueError(f"unsupported feature kind: {kind}")
        roles[feature_id] = frozenset(observed)
    return roles


def _exact_variant_matches_source(
    exact_variant: Mapping[str, Any],
    *,
    source_evaluation: Mapping[str, Any],
) -> bool:
    source_fold_metrics = [
        row
        for row in source_ladder._mapping_rows(source_evaluation.get("fold_metrics"))
        if row.get("target_id") == _TARGET_ID
        and row.get("model_id") == "M1_RIDGE_LINEAR"
        and row.get("feature_prefix") == "CROSS_ASSET_STATE"
    ]
    source_predictions = [
        row
        for row in source_ladder._mapping_rows(source_evaluation.get("predictions"))
        if row.get("target_id") == _TARGET_ID
        and row.get("model_id") == "M1_RIDGE_LINEAR"
        and row.get("feature_prefix") == "CROSS_ASSET_STATE"
    ]
    source_aggregates = [
        {**row, "variant_id": "EXACT_PRIMARY", "feature_prefix": "EXACT_PRIMARY"}
        for row in source_ladder._mapping_rows(source_evaluation.get("aggregate_metrics"))
        if row.get("target_id") == _TARGET_ID
        and row.get("model_id") == "M1_RIDGE_LINEAR"
        and row.get("feature_prefix") == "CROSS_ASSET_STATE"
    ]
    return bool(
        exact_variant.get("fold_ledger") == source_evaluation.get("fold_ledger")
        and exact_variant.get("fold_metrics") == source_fold_metrics
        and exact_variant.get("predictions") == source_predictions
        and exact_variant.get("aggregate_metrics") == source_aggregates
    )


def _fold_influence_audit(
    *,
    policy: Mapping[str, Any],
    variant_results: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    gate = _mapping(policy.get("fold_influence_gate"))
    rows: list[dict[str, Any]] = []
    horizon_results: list[dict[str, Any]] = []
    variant_results_out: list[dict[str, Any]] = []
    for variant_id in _MANDATORY_VARIANTS:
        variant = _variant_by_id(variant_results, variant_id)
        predictions = pd.DataFrame(_mapping_rows(variant.get("predictions")))
        passing_horizons: list[str] = []
        for horizon in _PRIMARY_HORIZONS:
            horizon_frame = predictions.loc[predictions["horizon_id"] == horizon]
            fold_ids = sorted(str(value) for value in horizon_frame["fold_id"].unique())
            pass_count = 0
            for omitted_fold in fold_ids:
                retained = horizon_frame.loc[horizon_frame["fold_id"] != omitted_fold]
                value = source_ladder._spearman(
                    retained["actual"].to_numpy(dtype=np.float64),
                    retained["prediction"].to_numpy(dtype=np.float64),
                )
                passed = bool(
                    value is not None
                    and value >= float(gate.get("minimum_leave_one_fold_out_spearman") or 0.0)
                )
                pass_count += int(passed)
                rows.append(
                    {
                        "variant_id": variant_id,
                        "horizon_id": horizon,
                        "omitted_fold_id": omitted_fold,
                        "retained_row_count": len(retained),
                        "spearman": _finite_or_none(value),
                        "pass": passed,
                    }
                )
            horizon_pass = pass_count >= _int_value(gate.get("required_leave_one_fold_out_passes"))
            if horizon_pass:
                passing_horizons.append(horizon)
            horizon_results.append(
                {
                    "variant_id": variant_id,
                    "horizon_id": horizon,
                    "leave_one_fold_out_pass_count": pass_count,
                    "horizon_pass": horizon_pass,
                }
            )
        variant_pass = bool(
            len(passing_horizons) >= _int_value(gate.get("minimum_passing_primary_horizons"))
            and str(gate.get("required_horizon")) in passing_horizons
        )
        variant_results_out.append(
            {
                "variant_id": variant_id,
                "passing_primary_horizons": passing_horizons,
                "variant_pass": variant_pass,
            }
        )
    return {
        "schema_version": "decision_target_tail_risk_fold_influence.v1",
        "rows": rows,
        "horizon_results": horizon_results,
        "variant_results": variant_results_out,
        "overall_pass": all(bool(row["variant_pass"]) for row in variant_results_out),
    }


def _regime_concentration_audit(
    *,
    policy: Mapping[str, Any],
    contexts: Mapping[tuple[str, str], Mapping[str, Any]],
    feature_frame: pd.DataFrame,
) -> dict[str, Any]:
    gate = _mapping(policy.get("regime_gate"))
    quantiles = [float(value) for value in _sequence(gate.get("quantiles"))]
    dimensions = _mapping_rows(gate.get("dimensions"))
    pooled: dict[tuple[str, str, str], dict[str, list[float]]] = {}
    threshold_rows: list[dict[str, Any]] = []
    for (fold_id, horizon), context in sorted(contexts.items()):
        if horizon not in _PRIMARY_HORIZONS:
            continue
        train_dates = _string_list(context.get("train_dates"))
        test_dates = _string_list(context.get("test_dates"))
        actual = _float_array(context.get("actual_test"))
        prediction = _float_array(context.get("prediction_test"))
        for dimension in dimensions:
            regime_id = str(dimension["regime_id"])
            feature_id = str(dimension["feature_id"])
            train_values = feature_frame.loc[train_dates, feature_id].to_numpy(dtype=np.float64)
            low, high = (float(value) for value in np.quantile(train_values, quantiles))
            test_values = feature_frame.loc[test_dates, feature_id].to_numpy(dtype=np.float64)
            labels = np.where(
                test_values <= low,
                "LOW",
                np.where(test_values <= high, "MID", "HIGH"),
            )
            threshold_rows.append(
                {
                    "fold_id": fold_id,
                    "horizon_id": horizon,
                    "regime_id": regime_id,
                    "feature_id": feature_id,
                    "train_row_count": len(train_values),
                    "low_threshold": low,
                    "high_threshold": high,
                }
            )
            for stratum in ("LOW", "MID", "HIGH"):
                mask = labels == stratum
                bucket = pooled.setdefault(
                    (regime_id, horizon, stratum),
                    {"actual": [], "prediction": []},
                )
                bucket["actual"].extend(float(value) for value in actual[mask])
                bucket["prediction"].extend(float(value) for value in prediction[mask])
    rows: list[dict[str, Any]] = []
    for (regime_id, horizon, stratum), bucket in sorted(pooled.items()):
        actual = np.asarray(bucket["actual"], dtype=np.float64)
        prediction = np.asarray(bucket["prediction"], dtype=np.float64)
        spearman = source_ladder._spearman(actual, prediction)
        stratum_eligible = len(actual) >= _int_value(gate.get("minimum_rows_per_pooled_stratum"))
        rows.append(
            {
                "regime_id": regime_id,
                "horizon_id": horizon,
                "stratum": stratum,
                "row_count": len(actual),
                "spearman": _finite_or_none(spearman),
                "eligible": stratum_eligible,
            }
        )
    dimension_results: list[dict[str, Any]] = []
    horizon_results: list[dict[str, Any]] = []
    passing_horizons: list[str] = []
    for horizon in _PRIMARY_HORIZONS:
        dimension_passes: list[bool] = []
        for dimension in dimensions:
            regime_id = str(dimension["regime_id"])
            subset = [
                row
                for row in rows
                if row["horizon_id"] == horizon and row["regime_id"] == regime_id
            ]
            eligible_strata = [row for row in subset if row["eligible"]]
            positive = sum(_required_float(row["spearman"]) > 0.0 for row in eligible_strata)
            minimum = min(
                (_required_float(row["spearman"]) for row in eligible_strata),
                default=None,
            )
            passed = bool(
                len(eligible_strata)
                >= _int_value(gate.get("minimum_eligible_strata_per_dimension"))
                and positive >= _int_value(gate.get("minimum_positive_strata_per_dimension"))
                and minimum is not None
                and minimum >= _required_float(gate.get("minimum_allowed_stratum_spearman"))
            )
            dimension_passes.append(passed)
            dimension_results.append(
                {
                    "regime_id": regime_id,
                    "horizon_id": horizon,
                    "eligible_strata_count": len(eligible_strata),
                    "positive_strata_count": positive,
                    "minimum_stratum_spearman": minimum,
                    "dimension_pass": passed,
                }
            )
        horizon_pass = all(dimension_passes)
        if horizon_pass:
            passing_horizons.append(horizon)
        horizon_results.append({"horizon_id": horizon, "horizon_pass": horizon_pass})
    overall = bool(
        len(passing_horizons) >= _int_value(gate.get("minimum_passing_primary_horizons"))
        and str(gate.get("required_horizon")) in passing_horizons
    )
    return {
        "schema_version": "decision_target_tail_risk_regime_concentration.v1",
        "threshold_rows": threshold_rows,
        "rows": rows,
        "dimension_results": dimension_results,
        "horizon_results": horizon_results,
        "passing_primary_horizons": passing_horizons,
        "overall_pass": overall,
    }


def _event_calibration_audit(
    *,
    policy: Mapping[str, Any],
    contexts: Mapping[tuple[str, str], Mapping[str, Any]],
) -> dict[str, Any]:
    gate = _mapping(policy.get("event_calibration_gate"))
    event_quantiles = [float(value) for value in _sequence(gate.get("tail_event_quantiles"))]
    risk_quantile = _required_float(gate.get("predicted_risk_quantile"))
    rows: list[dict[str, Any]] = []
    for (fold_id, horizon), context in sorted(contexts.items()):
        if horizon not in _PRIMARY_HORIZONS:
            continue
        actual_train = _float_array(context.get("actual_train"))
        prediction_train = _float_array(context.get("prediction_train"))
        actual_test = _float_array(context.get("actual_test"))
        prediction_test = _float_array(context.get("prediction_test"))
        risk_threshold = float(np.quantile(prediction_train, risk_quantile))
        safe_threshold = float(np.quantile(prediction_train, 1.0 - risk_quantile))
        flagged = prediction_test <= risk_threshold
        bottom = prediction_test >= safe_threshold
        for event_quantile in event_quantiles:
            event_threshold = float(np.quantile(actual_train, event_quantile))
            events = actual_test <= event_threshold
            event_count = int(np.sum(events))
            flagged_count = int(np.sum(flagged))
            bottom_count = int(np.sum(bottom))
            flagged_event_count = int(np.sum(events & flagged))
            bottom_event_count = int(np.sum(events & bottom))
            base_rate = _safe_rate(event_count, len(events))
            flagged_rate = _safe_rate(flagged_event_count, flagged_count)
            bottom_rate = _safe_rate(bottom_event_count, bottom_count)
            lift = (
                flagged_rate / base_rate
                if flagged_rate is not None and base_rate is not None and base_rate != 0.0
                else None
            )
            separation = (
                flagged_rate - bottom_rate
                if flagged_rate is not None and bottom_rate is not None
                else None
            )
            eligible = bool(
                event_count >= _int_value(gate.get("minimum_test_events_per_fold"))
                and flagged_count >= _int_value(gate.get("minimum_flagged_rows_per_fold"))
                and bottom_count >= _int_value(gate.get("minimum_flagged_rows_per_fold"))
            )
            passed = bool(
                eligible
                and lift is not None
                and lift > _required_float(gate.get("minimum_fold_lift_exclusive"))
                and separation is not None
                and separation
                > _required_float(gate.get("minimum_fold_top_bottom_separation_exclusive"))
            )
            rows.append(
                {
                    "fold_id": fold_id,
                    "horizon_id": horizon,
                    "event_quantile": event_quantile,
                    "event_threshold": event_threshold,
                    "risk_prediction_threshold": risk_threshold,
                    "safe_prediction_threshold": safe_threshold,
                    "test_row_count": len(events),
                    "event_count": event_count,
                    "flagged_row_count": flagged_count,
                    "bottom_row_count": bottom_count,
                    "flagged_event_count": flagged_event_count,
                    "bottom_event_count": bottom_event_count,
                    "base_event_rate": base_rate,
                    "flagged_event_rate": flagged_rate,
                    "bottom_event_rate": bottom_rate,
                    "lift": lift,
                    "top_bottom_separation": separation,
                    "eligible": eligible,
                    "fold_pass": passed,
                }
            )
    quantile_results: list[dict[str, Any]] = []
    horizon_results: list[dict[str, Any]] = []
    passing_horizons: list[str] = []
    for horizon in _PRIMARY_HORIZONS:
        quantile_passes: list[bool] = []
        for event_quantile in event_quantiles:
            subset = [
                row
                for row in rows
                if row["horizon_id"] == horizon
                and math.isclose(_required_float(row["event_quantile"]), event_quantile)
            ]
            passing_folds = sum(bool(row["fold_pass"]) for row in subset)
            total_events = sum(int(row["event_count"]) for row in subset)
            total_rows = sum(int(row["test_row_count"]) for row in subset)
            flagged_events = sum(int(row["flagged_event_count"]) for row in subset)
            flagged_rows = sum(int(row["flagged_row_count"]) for row in subset)
            pooled_base = _safe_rate(total_events, total_rows)
            pooled_flagged = _safe_rate(flagged_events, flagged_rows)
            pooled_lift = (
                pooled_flagged / pooled_base
                if pooled_flagged is not None and pooled_base is not None and pooled_base != 0.0
                else None
            )
            passed = bool(
                passing_folds >= _int_value(gate.get("minimum_passing_folds"))
                and pooled_lift is not None
                and pooled_lift >= _required_float(gate.get("minimum_pooled_lift"))
            )
            quantile_passes.append(passed)
            quantile_results.append(
                {
                    "horizon_id": horizon,
                    "event_quantile": event_quantile,
                    "passing_fold_count": passing_folds,
                    "pooled_base_event_rate": pooled_base,
                    "pooled_flagged_event_rate": pooled_flagged,
                    "pooled_lift": pooled_lift,
                    "quantile_pass": passed,
                }
            )
        horizon_pass = all(quantile_passes)
        if horizon_pass:
            passing_horizons.append(horizon)
        horizon_results.append({"horizon_id": horizon, "horizon_pass": horizon_pass})
    overall = bool(
        len(passing_horizons) >= _int_value(gate.get("minimum_passing_primary_horizons"))
        and str(gate.get("required_horizon")) in passing_horizons
    )
    return {
        "schema_version": "decision_target_tail_risk_event_calibration.v1",
        "rows": rows,
        "quantile_results": quantile_results,
        "horizon_results": horizon_results,
        "passing_primary_horizons": passing_horizons,
        "overall_pass": overall,
    }


def _placebo_audit(
    *,
    policy: Mapping[str, Any],
    contexts: Mapping[tuple[str, str], Mapping[str, Any]],
) -> dict[str, Any]:
    gate = _mapping(policy.get("placebo_gate"))
    block_sessions = _int_value(gate.get("block_sessions"))
    replicates = _int_value(gate.get("replicate_count"))
    seed = _int_value(gate.get("random_seed"))
    rows: list[dict[str, Any]] = []
    passing_horizons: list[str] = []
    for horizon_index, horizon in enumerate(_PRIMARY_HORIZONS):
        fold_contexts = [
            (fold_id, context)
            for (fold_id, observed_horizon), context in sorted(contexts.items())
            if observed_horizon == horizon
        ]
        actual = np.concatenate(
            [_float_array(context.get("actual_test")) for _, context in fold_contexts]
        )
        prediction = np.concatenate(
            [_float_array(context.get("prediction_test")) for _, context in fold_contexts]
        )
        actual_spearman = source_ladder._spearman(actual, prediction)
        rng = np.random.default_rng(seed + horizon_index)
        null_values: list[float] = []
        for _ in range(replicates):
            permuted_actual = np.concatenate(
                [
                    _block_permute(
                        _float_array(context.get("actual_test")),
                        block_sessions=block_sessions,
                        rng=rng,
                    )
                    for _, context in fold_contexts
                ]
            )
            value = source_ladder._spearman(permuted_actual, prediction)
            if value is None:
                raise ValueError("placebo spearman unavailable")
            null_values.append(value)
        assert actual_spearman is not None
        empirical_p = (1 + sum(value >= actual_spearman for value in null_values)) / (
            1 + replicates
        )
        null_percentile = float(
            np.quantile(
                np.asarray(null_values, dtype=np.float64),
                _required_float(gate.get("null_percentile")),
            )
        )
        passed = bool(
            empirical_p <= _required_float(gate.get("maximum_empirical_p_value"))
            and actual_spearman > null_percentile
        )
        if passed:
            passing_horizons.append(horizon)
        rows.append(
            {
                "horizon_id": horizon,
                "row_count": len(actual),
                "actual_spearman": actual_spearman,
                "null_mean_spearman": float(np.mean(null_values)),
                "null_percentile_spearman": null_percentile,
                "empirical_p_value": empirical_p,
                "replicate_count": replicates,
                "block_sessions": block_sessions,
                "horizon_pass": passed,
            }
        )
    overall = bool(
        len(passing_horizons) >= _int_value(gate.get("minimum_passing_primary_horizons"))
        and str(gate.get("required_horizon")) in passing_horizons
    )
    return {
        "schema_version": "decision_target_tail_risk_block_placebo.v1",
        "rows": rows,
        "passing_primary_horizons": passing_horizons,
        "overall_pass": overall,
    }


def _evaluability_errors(
    *,
    variant_results: Sequence[Mapping[str, Any]],
    fold_influence: Mapping[str, Any],
    regime: Mapping[str, Any],
    calibration: Mapping[str, Any],
    placebo: Mapping[str, Any],
) -> list[str]:
    errors: list[str] = []
    for variant_id in _MANDATORY_VARIANTS:
        variant = _variant_by_id(variant_results, variant_id)
        aggregates = _mapping_rows(variant.get("aggregate_metrics"))
        if len(aggregates) != len(_ALL_HORIZONS) or any(
            _int_value(row.get("valid_fold_count")) < 6 for row in aggregates
        ):
            errors.append(f"MANDATORY_VARIANT_NOT_EVALUABLE:{variant_id}")
    if len(_mapping_rows(fold_influence.get("rows"))) != len(_MANDATORY_VARIANTS) * 3 * 7:
        errors.append("FOLD_INFLUENCE_NOT_EVALUABLE")
    if any(not bool(row.get("eligible")) for row in _mapping_rows(regime.get("rows"))):
        errors.append("REGIME_STRATUM_NOT_EVALUABLE")
    if any(
        sum(
            bool(row.get("eligible"))
            for row in _mapping_rows(calibration.get("rows"))
            if row.get("horizon_id") == horizon
            and math.isclose(_required_float(row.get("event_quantile")), quantile)
        )
        < 6
        for horizon in _PRIMARY_HORIZONS
        for quantile in (0.10, 0.20)
    ):
        errors.append("EVENT_CALIBRATION_NOT_EVALUABLE")
    if len(_mapping_rows(placebo.get("rows"))) != len(_PRIMARY_HORIZONS):
        errors.append("PLACEBO_NOT_EVALUABLE")
    return errors


def _decision(
    *,
    exact_reconstruction_pass: bool,
    mandatory_variants_pass: bool,
    fold_influence_pass: bool,
    regime_pass: bool,
    calibration_pass: bool,
    placebo_pass: bool,
    evaluability_errors: Sequence[str],
    variant_results: Sequence[Mapping[str, Any]],
    placebo: Mapping[str, Any],
) -> dict[str, Any]:
    lag_horizons = set(
        _string_list(
            _mapping(_variant_by_id(variant_results, "FEATURE_LAG_1").get("aggregate")).get(
                "passing_primary_horizons"
            )
        )
    )
    embargo_horizons = set(
        _string_list(
            _mapping(_variant_by_id(variant_results, "EMBARGO_40").get("aggregate")).get(
                "passing_primary_horizons"
            )
        )
    )
    placebo_10d = next(
        (
            bool(row.get("horizon_pass"))
            for row in _mapping_rows(placebo.get("rows"))
            if row.get("horizon_id") == "10d"
        ),
        False,
    )
    if evaluability_errors:
        status = "INSUFFICIENT_ROBUSTNESS_EVIDENCE"
        interpretation = "至少一个预注册mandatory axis不可评估，不能把缺失证据解释为稳健或失败。"
    elif not exact_reconstruction_pass or not placebo_10d or not (lag_horizons & embargo_horizons):
        status = "TAIL_RISK_CAPABILITY_FALSIFIED"
        interpretation = (
            "能力未通过exact reconstruction、required 10d placebo或timing/embargo共同生存条件；"
            "当前tail-risk路径应关闭或重新定义decision target。"
        )
    elif all(
        (
            mandatory_variants_pass,
            fold_influence_pass,
            regime_pass,
            calibration_pass,
            placebo_pass,
        )
    ):
        status = "TAIL_RISK_CAPABILITY_ROBUST"
        interpretation = (
            "当前historical-seen证据通过全部预注册证伪轴；只允许Owner另立Decision Value Audit，"
            "仍不批准risk overlay、candidate或权重。"
        )
    else:
        status = "TAIL_RISK_CAPABILITY_FRAGILE"
        interpretation = (
            "exact skill仍存在但至少一个mandatory perturbation、fold influence、regime、calibration"
            "或placebo aggregate gate失败；不得进入Decision Value Audit或risk overlay。"
        )
    if status not in _ALLOWED_DECISIONS:
        raise ValueError("unsupported tail-risk robustness decision")
    return {
        "schema_version": "decision_target_tail_risk_robustness_decision.v1",
        "decision_status": status,
        "interpretation_zh": interpretation,
        "owner_review_required": True,
        "decision_value_audit_authorized": False,
        "risk_overlay_authorized": False,
        "candidate_family_creation_authorized": False,
        "target_weights_authorized": False,
        "qld_signal_role_authorized": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _build_summary(
    *,
    decision: Mapping[str, Any],
    gate_summary: Mapping[str, Any],
    variant_results: Sequence[Mapping[str, Any]],
    fold_influence: Mapping[str, Any],
    regime: Mapping[str, Any],
    calibration: Mapping[str, Any],
    placebo: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "decision_status": decision.get("decision_status"),
        "selected_range": "2021-02-22..2026-07-24",
        "evaluated_range": "2021-08-11..2026-06-25",
        "target_id": _TARGET_ID,
        "primary_horizons": list(_PRIMARY_HORIZONS),
        "variant_count": len(variant_results),
        "mandatory_variant_count": len(_MANDATORY_VARIANTS),
        "mandatory_variant_pass_count": sum(
            bool(_variant_by_id(variant_results, variant_id).get("target_supported"))
            for variant_id in _MANDATORY_VARIANTS
        ),
        "gate_pass_count": sum(
            bool(gate_summary.get(field))
            for field in (
                "exact_reconstruction_pass",
                "all_mandatory_variants_pass",
                "fold_influence_pass",
                "regime_concentration_pass",
                "event_calibration_pass",
                "placebo_rejection_pass",
            )
        ),
        "fold_influence_row_count": len(_mapping_rows(fold_influence.get("rows"))),
        "regime_row_count": len(_mapping_rows(regime.get("rows"))),
        "calibration_row_count": len(_mapping_rows(calibration.get("rows"))),
        "placebo_row_count": len(_mapping_rows(placebo.get("rows"))),
        "historical_seen_only": True,
        "candidate_family_created": False,
        "risk_overlay_created": False,
        "strategy_backtest_executed": False,
        "target_weights_generated": False,
    }


def _blocked_payload(
    *,
    policy: Mapping[str, Any],
    errors: Sequence[str],
    records: Sequence[Mapping[str, Any]],
    as_of: date,
) -> dict[str, Any]:
    decision = {
        "schema_version": "decision_target_tail_risk_robustness_decision.v1",
        "decision_status": "INSUFFICIENT_ROBUSTNESS_EVIDENCE",
        "interpretation_zh": "输入或协议未通过，未执行证伪审计。",
        "owner_review_required": True,
        "decision_value_audit_authorized": False,
        "risk_overlay_authorized": False,
        "candidate_family_creation_authorized": False,
        "target_weights_authorized": False,
        "qld_signal_role_authorized": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "task_id": _TASK_ID,
        "status": BLOCKED_STATUS,
        "report_type": REPORT_TYPE,
        "validation_status": "BLOCKED",
        "strict_validation_errors": sorted(set(str(error) for error in errors)),
        "source_commitment": {"source_task_id": _SOURCE_TASK_ID, "records": list(records)},
        "data_quality_scope": {
            "full_canonical_status": "FAIL",
            "scoped_status": "UNKNOWN",
            "global_cache_pass_claimed": False,
        },
        "data_quality_evidence": _blocked_data_quality_evidence(errors, as_of=as_of),
        "evaluation": None,
        "evaluation_commitment_sha256": None,
        "audit_summary": {
            "schema_version": SUMMARY_SCHEMA_VERSION,
            "decision_status": "INSUFFICIENT_ROBUSTNESS_EVIDENCE",
            "historical_seen_only": True,
            "candidate_family_created": False,
            "risk_overlay_created": False,
            "strategy_backtest_executed": False,
            "target_weights_generated": False,
        },
        "decision": decision,
        "next_route": "FIX_INPUT_OR_PROTOCOL_BEFORE_RESEARCH",
        **_safety_payload(policy),
    }
    payload["rendered_markdown"] = render_tail_risk_robustness_markdown(payload)
    return payload


def _blocked_data_quality_evidence(
    errors: Sequence[str],
    *,
    as_of: date,
) -> dict[str, Any]:
    blockers = sorted(set(str(error) for error in errors)) or ["INPUT_NOT_AVAILABLE"]
    return {
        "schema_version": "data_quality_evidence.v1",
        "evidence_id": "dq_evidence_trading2462_blocked_input",
        "contract_id": "decision_target_tail_risk_robustness_audit_input",
        "policy_id": "DATA_QUALITY_CACHE_GATE",
        "policy_version": "data_quality_cache_gate.v2",
        "status": "FAIL",
        "passed": False,
        "checked_at": f"{as_of.isoformat()}T00:00:00+00:00",
        "as_of": as_of.isoformat(),
        "checked_input_count": 0,
        "error_count": len(blockers),
        "warning_count": 0,
        "blocking_issues": blockers,
        "report_path": "not_materialized",
        "report_sha256": "0" * 64,
    }


def _safety_payload(policy: Mapping[str, Any]) -> dict[str, Any]:
    del policy
    return {
        "research_only": True,
        "historical_seen_only": True,
        "prospective_accessed": False,
        "candidate_family_created": False,
        "risk_overlay_created": False,
        "strategy_backtest_executed": False,
        "transaction_cost_model_applied": False,
        "target_weights_generated": False,
        "qld_used_as_signal": False,
        "paper_shadow_changed": False,
        "promotion_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _block_permute(
    values: FloatArray,
    *,
    block_sessions: int,
    rng: np.random.Generator,
) -> FloatArray:
    blocks = [
        values[start : start + block_sessions] for start in range(0, len(values), block_sessions)
    ]
    order = rng.permutation(len(blocks))
    return np.concatenate([blocks[int(index)] for index in order])


def _features_present(
    frame: pd.DataFrame,
    decision_date: str,
    feature_ids: Sequence[str],
) -> bool:
    values = frame.loc[[decision_date], list(feature_ids)].to_numpy(dtype=np.float64)
    return bool(np.isfinite(values).all())


def _variant_by_id(
    variants: Sequence[Mapping[str, Any]],
    variant_id: str,
) -> Mapping[str, Any]:
    return next(row for row in variants if row.get("variant_id") == variant_id)


def _resolve_path(value: str) -> Path:
    path = Path(value)
    return path.resolve() if path.is_absolute() else (_PROJECT_ROOT / path).resolve()


def _safe_rate(numerator: int, denominator: int) -> float | None:
    return numerator / denominator if denominator > 0 else None


def _finite_or_none(value: float | None) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return float(value)


def _float_array(value: object) -> FloatArray:
    return np.asarray(value, dtype=np.float64)


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _sequence(value: object) -> Sequence[Any]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return value
    return ()


def _mapping_rows(value: object) -> list[Mapping[str, Any]]:
    return [row for row in _sequence(value) if isinstance(row, Mapping)]


def _string_list(value: object) -> list[str]:
    return [str(item) for item in _sequence(value)]


def _int_value(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return int(value)
    if isinstance(value, str) and value.strip():
        return int(value)
    return 0


def _required_float(value: object) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float, np.integer, np.floating)):
        result = float(value)
    elif isinstance(value, str) and value.strip():
        result = float(value)
    else:
        raise ValueError(f"numeric value required: {value!r}")
    if not math.isfinite(result):
        raise ValueError(f"finite numeric value required: {value!r}")
    return result


__all__ = [
    "BLOCKED_STATUS",
    "READY_STATUS",
    "build_tail_risk_robustness_payload",
    "decision_target_tail_risk_robustness_registry",
    "render_tail_risk_robustness_markdown",
    "tail_risk_robustness_summary",
    "tail_risk_robustness_view_model",
    "validate_tail_risk_robustness_payload",
]
