from __future__ import annotations

import hashlib
import json
import math
import shutil
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from ai_trading_system.platform.artifacts import (
    canonical_json_bytes,
    sha256_path,
    write_json_atomic,
)
from ai_trading_system.research_framework.plugins import (
    ExperimentExecutionContext,
    PluginRegistry,
)

SCHEMA_VERSION = "decision_target_capability_audit_model_ladder.v1"
INPUT_SCHEMA_VERSION = "decision_target_capability_audit_model_ladder_input.v1"
SUMMARY_SCHEMA_VERSION = "decision_target_capability_audit_model_ladder_summary.v1"
REPORT_TYPE = "decision_target_capability_audit_model_ladder"
READY_STATUS = "CAPABILITY_AUDIT_READY"
BLOCKED_STATUS = "BLOCKED_INPUT_OR_PROTOCOL"

_EXPECTED_POLICY_SCHEMA = "decision_target_capability_audit_model_ladder_policy.v1"
_OWNER_DECISION = (
    "owner_decision:TRADING-2461:2026-07-26:" "approve_decision_target_capability_audit_batch2_v1"
)
_EXPECTED_LABEL_SCHEMA = "decision_target_capability_audit_label_foundation.v1"
_EXPECTED_SOURCE_SCHEMA = "decision_target_market_panel.v1"
_EXPECTED_RECEIPT_SCHEMA = "data_quality_consumer_capability_receipt.v1"
_EXPECTED_TARGETS = (
    "QQQ_MINUS_SGOV",
    "SPY_MINUS_SGOV",
    "QQQ_MINUS_SPY",
    "QQQ_FUTURE_MAX_DRAWDOWN",
    "QQQ_FUTURE_WORST_1D_RETURN",
)
_EXPECTED_HORIZONS = ("1d", "5d", "10d", "20d")
_EXPECTED_FAMILIES = (
    "PRICE_TREND",
    "VOLATILITY_DRAWDOWN",
    "CROSS_ASSET_STATE",
)
_EXPECTED_MODELS = (
    "M0_TRAIN_MEAN",
    "M1_RIDGE_LINEAR",
    "M2_RIDGE_INTERACTION",
)
_INPUT_ROLES = (
    "label_payload",
    "source_package",
    "market_panel",
    "capability_receipt",
)
_RETURN_TARGETS = frozenset(_EXPECTED_TARGETS[:3])
_TAIL_TARGETS = frozenset(_EXPECTED_TARGETS[3:])
_STYLE_STATUSES = frozenset(
    {
        "NO_MEASURABLE_SKILL",
        "BROAD_EQUITY_RISK_PREMIUM_SKILL",
        "NASDAQ_LEADERSHIP_SKILL",
        "COMBINED_QQQ_DEFENSIVE_ALLOCATION_SKILL",
        "TAIL_RISK_ONLY_SKILL",
        "MIXED_OR_UNSTABLE_SKILL",
        "INSUFFICIENT_FOLD_COVERAGE",
    }
)
_NUMERIC_EPSILON = 1.0e-12

FloatArray = NDArray[np.float64]


def capture_input_snapshot(
    *,
    label_payload_path: Path,
    source_package_path: Path,
    market_panel_path: Path,
    capability_receipt_path: Path,
    output_root: Path,
    captured_at: datetime,
) -> dict[str, Any]:
    """Copy immutable Batch 1 evidence into one content-addressed Batch 2 snapshot."""

    observed_at = _aware_utc(captured_at)
    sources = {
        "label_payload": label_payload_path,
        "source_package": source_package_path,
        "market_panel": market_panel_path,
        "capability_receipt": capability_receipt_path,
    }
    filenames = {
        "label_payload": "batch1_label_payload.json",
        "source_package": "batch1_source_package.json",
        "market_panel": "batch1_market_panel.csv",
        "capability_receipt": "batch1_capability_receipt.json",
    }
    output_root.mkdir(parents=True, exist_ok=True)
    records: dict[str, dict[str, Any]] = {}
    for role in _INPUT_ROLES:
        source = sources[role].resolve()
        if not source.is_file():
            raise ValueError(f"snapshot source missing: {role}:{source}")
        destination = (output_root / filenames[role]).resolve()
        if destination.exists():
            if sha256_path(destination) != sha256_path(source):
                raise ValueError(f"immutable snapshot destination drift: {role}")
        else:
            shutil.copyfile(source, destination)
        records[role] = {
            "path": str(destination),
            "sha256": sha256_path(destination),
            "size_bytes": destination.stat().st_size,
            "source_path": str(source),
        }

    snapshot = {
        "schema_version": INPUT_SCHEMA_VERSION,
        "captured_at": observed_at.isoformat(),
        "records": records,
        "safety": {
            "source_files_mutated": False,
            "canonical_cache_mutated": False,
            "prospective_accessed": False,
            "production_effect": "none",
            "broker_action": "none",
        },
    }
    manifest_path = output_root / "input_snapshot.json"
    write_json_atomic(manifest_path, snapshot)
    return {
        **snapshot,
        "manifest": _file_record(manifest_path),
    }


def build_capability_payload(
    sources: Mapping[str, Any],
    *,
    as_of: date,
) -> dict[str, Any]:
    policy = _mapping(sources.get("audit_policy"))
    snapshot = _mapping(sources.get("input_snapshot"))
    requirement_text = str(sources.get("requirement_text") or "")
    errors = list(_policy_errors(policy, requirement_text=requirement_text))
    loaded, snapshot_errors = _load_snapshot(
        policy=policy,
        snapshot=snapshot,
        as_of=as_of,
    )
    errors.extend(snapshot_errors)

    data_quality = _data_quality_evidence_from_loaded(
        loaded,
        as_of=as_of,
        errors=errors,
    )
    if errors:
        return _blocked_payload(
            errors=errors,
            data_quality_evidence=data_quality,
            policy=policy,
            snapshot=snapshot,
        )

    label_payload = _mapping(loaded["label_payload"])
    panel = loaded["market_panel"]
    assert isinstance(panel, pd.DataFrame)
    try:
        evaluation = _evaluate_model_ladder(
            policy=policy,
            label_payload=label_payload,
            panel=panel,
        )
    except (FloatingPointError, KeyError, TypeError, ValueError, np.linalg.LinAlgError):
        return _blocked_payload(
            errors=["MODEL_LADDER_BUILD_FAILED"],
            data_quality_evidence=data_quality,
            policy=policy,
            snapshot=snapshot,
        )
    summary = _build_summary(policy=policy, evaluation=evaluation)
    style = _classify_style(policy=policy, evaluation=evaluation)
    evaluation_commitment = hashlib.sha256(canonical_json_bytes(evaluation)).hexdigest()
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2461_DECISION_TARGET_CAPABILITY_AUDIT_MODEL_LADDER",
        "status": READY_STATUS,
        "report_type": REPORT_TYPE,
        "validation_status": "CONTENT_DERIVED_REBUILD_REQUIRED",
        "strict_validation_errors": [],
        "data_quality_evidence": data_quality,
        "data_quality_scope": {
            "full_canonical_status": _mapping(policy.get("input_authority")).get(
                "full_canonical_status"
            ),
            "scoped_status": _mapping(policy.get("input_authority")).get("scoped_status"),
            "global_cache_pass_claimed": _mapping(policy.get("input_authority")).get(
                "global_cache_pass_claimed"
            ),
            "capability_receipt_id": str(
                _mapping(policy.get("input_authority")).get("capability_receipt_id")
            ),
        },
        "input_snapshot_commitment": _snapshot_commitment(snapshot),
        "evaluation": evaluation,
        "evaluation_commitment_sha256": evaluation_commitment,
        "capability_summary": summary,
        "style_classification": style,
        "next_route": "OWNER_STYLE_REVIEW_NO_AUTOMATIC_CANDIDATE",
        **_safety_payload(policy),
    }
    payload["rendered_markdown"] = render_capability_markdown(payload)
    return payload


def validate_capability_payload(
    payload: Mapping[str, Any],
    sources: Mapping[str, Any],
    *,
    as_of: date,
) -> tuple[str, ...]:
    expected = build_capability_payload(sources, as_of=as_of)
    errors: list[str] = []
    if payload.get("status") != expected.get("status"):
        errors.append("CAPABILITY_STATUS_MISMATCH")
    if payload.get("strict_validation_errors") != expected.get("strict_validation_errors"):
        errors.append("CAPABILITY_STRICT_ERRORS_MISMATCH")
    if payload.get("input_snapshot_commitment") != expected.get("input_snapshot_commitment"):
        errors.append("CAPABILITY_INPUT_COMMITMENT_MISMATCH")
    if payload.get("evaluation") != expected.get("evaluation"):
        errors.append("CAPABILITY_EVALUATION_MISMATCH")
    if payload.get("evaluation_commitment_sha256") != expected.get("evaluation_commitment_sha256"):
        errors.append("CAPABILITY_EVALUATION_COMMITMENT_MISMATCH")
    if payload.get("capability_summary") != expected.get("capability_summary"):
        errors.append("CAPABILITY_SUMMARY_MISMATCH")
    if payload.get("style_classification") != expected.get("style_classification"):
        errors.append("CAPABILITY_STYLE_CLASSIFICATION_MISMATCH")
    if payload.get("data_quality_evidence") != expected.get("data_quality_evidence"):
        errors.append("CAPABILITY_DATA_QUALITY_MISMATCH")
    for field, expected_value in _safety_payload(_mapping(sources.get("audit_policy"))).items():
        if payload.get(field) != expected_value:
            errors.append("CAPABILITY_SAFETY_BOUNDARY_MISMATCH")
            break
    if payload.get("rendered_markdown") != render_capability_markdown(payload):
        errors.append("CAPABILITY_MARKDOWN_MISMATCH")
    return tuple(errors)


def capability_summary(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    return _mapping(payload.get("capability_summary"))


def capability_view_model(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "status": payload.get("status"),
        "summary": payload.get("capability_summary"),
        "style_classification": payload.get("style_classification"),
        "next_route": payload.get("next_route"),
        "production_effect": payload.get("production_effect"),
        "broker_action": payload.get("broker_action"),
    }


def render_capability_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("capability_summary"))
    style = _mapping(payload.get("style_classification"))
    evaluation = _mapping(payload.get("evaluation"))
    status = str(payload.get("status") or BLOCKED_STATUS)
    errors = _string_list(payload.get("strict_validation_errors"))
    if status != READY_STATUS:
        error_lines = "\n".join(f"- `{item}`" for item in errors) or "- `UNKNOWN_BLOCKER`"
        return (
            "# Decision Target Capability Audit Batch 2\n\n"
            f"- 结论：`{status}`\n"
            "- 数据与协议未通过，未训练模型、未生成策略结论。\n\n"
            "## 阻塞原因\n\n"
            f"{error_lines}\n\n"
            "## 安全边界\n\n"
            "- 未创建 candidate family，未运行策略回测，未生成权重。\n"
            "- `production_effect=none`，`broker_action=none`。\n"
        )

    target_rows = style.get("target_capabilities")
    target_rows = target_rows if isinstance(target_rows, Sequence) else ()
    target_lines = [
        "|Target|通过 horizon|结论|",
        "|---|---:|---|",
    ]
    for row in target_rows:
        item = _mapping(row)
        target_lines.append(
            f"|`{item.get('target_id')}`|{item.get('passing_horizon_count')}|"
            f"`{item.get('status')}`|"
        )
    fold_rows = evaluation.get("fold_ledger")
    fold_rows = fold_rows if isinstance(fold_rows, Sequence) else ()
    fold_lines = [
        "|Fold|Train cutoff|Test range|Train sessions|Test sessions|",
        "|---|---|---|---:|---:|",
    ]
    for row in fold_rows:
        item = _mapping(row)
        fold_lines.append(
            f"|`{item.get('fold_id')}`|{item.get('train_cutoff')}|"
            f"{item.get('test_start')}..{item.get('test_end')}|"
            f"{item.get('eligible_train_session_count')}|"
            f"{item.get('test_session_count')}|"
        )
    requested = _mapping(evaluation.get("requested_range"))
    evaluated = _mapping(evaluation.get("evaluated_range"))
    return (
        "# Decision Target Capability Audit Batch 2\n\n"
        f"- 结论：`{style.get('style_status')}`\n"
        f"- 运行状态：`{status}`\n"
        f"- outer folds：{summary.get('fold_count')}\n"
        f"- predictions：{summary.get('prediction_row_count')}\n"
        f"- primary classification model：`{summary.get('classification_model')}`\n"
        f"- primary feature prefix：`{summary.get('classification_feature_prefix')}`\n"
        f"- selected research window：`{requested.get('start')}..{requested.get('end')}`\n"
        f"- actual evaluated feature/label range："
        f"`{evaluated.get('start')}..{evaluated.get('end')}`\n"
        "- data quality：full canonical=`FAIL`，QQQ/SPY/SGOV scoped=`PASS`，"
        "`global_cache_pass_claimed=false`\n"
        "- 数据角色：historical-seen capability audit，不是 prospective/OOS 业绩证明。\n\n"
        "## Target 能力\n\n"
        + "\n".join(target_lines)
        + "\n\n## Purged walk-forward\n\n"
        + "\n".join(fold_lines)
        + "\n\n## 解释\n\n"
        f"{style.get('interpretation_zh')}\n\n"
        "本报告只决定下一步值得研究的风格。它没有创建 candidate family，没有计算交易成本后"
        "策略收益，没有生成 target weights，也没有把 QLD 用作信号。\n\n"
        "## 下一步\n\n"
        f"`{payload.get('next_route')}`：必须由 Owner 复核后另立新 family 或终止该方向。\n\n"
        "## 安全边界\n\n"
        "- `prospective_accessed=false`\n"
        "- `candidate_family_created=false`\n"
        "- `strategy_backtest_executed=false`\n"
        "- `target_weights_generated=false`\n"
        "- `qld_used_as_signal=false`\n"
        "- `production_effect=none`\n"
        "- `broker_action=none`\n"
    )


class DecisionTargetCapabilityModelLadderCalculator:
    plugin_id = "decision_target_capability_model_ladder_calculator"
    version = "v1"

    def calculate(self, context: ExperimentExecutionContext) -> dict[str, Any]:
        return build_capability_payload(context.sources, as_of=context.as_of)


class DecisionTargetCapabilityModelLadderReport:
    plugin_id = "decision_target_capability_model_ladder_report"
    version = "v1"

    def section(self, payload: Mapping[str, Any], section_id: str) -> Mapping[str, Any]:
        if section_id != "capability_summary":
            raise ValueError(f"unknown capability section: {section_id}")
        return capability_summary(payload)

    def render_markdown(self, payload: Mapping[str, Any]) -> str:
        return render_capability_markdown(payload)


def decision_target_capability_model_ladder_registry() -> PluginRegistry:
    return PluginRegistry(
        calculators=(DecisionTargetCapabilityModelLadderCalculator(),),
        reports=(DecisionTargetCapabilityModelLadderReport(),),
    )


def _policy_errors(
    policy: Mapping[str, Any],
    *,
    requirement_text: str,
) -> tuple[str, ...]:
    errors: list[str] = []
    if policy.get("schema_version") != _EXPECTED_POLICY_SCHEMA:
        errors.append("AUDIT_POLICY_SCHEMA_INVALID")
    if policy.get("status") != "OWNER_APPROVED_HISTORICAL_SEEN_PILOT":
        errors.append("AUDIT_POLICY_STATUS_INVALID")
    if policy.get("owner_decision") != _OWNER_DECISION:
        errors.append("AUDIT_OWNER_DECISION_INVALID")
    if "TRADING-2461_DECISION_TARGET_CAPABILITY_AUDIT_MODEL_LADDER" not in requirement_text:
        errors.append("AUDIT_REQUIREMENT_BINDING_INVALID")
    target_policy = _mapping(policy.get("targets"))
    observed_targets = tuple(
        str(item)
        for item in (
            list(_sequence(target_policy.get("return")))
            + list(_sequence(target_policy.get("tail")))
        )
    )
    if observed_targets != _EXPECTED_TARGETS:
        errors.append("AUDIT_TARGETS_INVALID")
    if tuple(str(item) for item in _sequence(target_policy.get("horizons"))) != (
        _EXPECTED_HORIZONS
    ):
        errors.append("AUDIT_HORIZONS_INVALID")
    split = _mapping(policy.get("split_policy"))
    expected_split = {
        "initial_train_sessions": 378,
        "minimum_train_rows": 252,
        "test_sessions": 126,
        "minimum_final_test_sessions": 63,
        "embargo_sessions": 20,
        "label_maturity_required": True,
        "purge_label_test_overlap_required": True,
        "purge_label_embargo_overlap_required": True,
        "train_transform_fit_scope": "train_only",
    }
    if any(split.get(key) != value for key, value in expected_split.items()):
        errors.append("AUDIT_SPLIT_POLICY_INVALID")
    feature_policy = _mapping(policy.get("feature_policy"))
    if tuple(str(item) for item in _sequence(feature_policy.get("family_order"))) != (
        _EXPECTED_FAMILIES
    ):
        errors.append("AUDIT_FEATURE_FAMILY_ORDER_INVALID")
    feature_rows = _mapping_rows(feature_policy.get("features"))
    feature_ids = [str(item.get("feature_id") or "") for item in feature_rows]
    if len(feature_ids) != len(set(feature_ids)) or not feature_ids:
        errors.append("AUDIT_FEATURE_DEFINITIONS_INVALID")
    if any(str(item.get("family") or "") not in _EXPECTED_FAMILIES for item in feature_rows):
        errors.append("AUDIT_FEATURE_FAMILY_INVALID")
    model_policy = _mapping(policy.get("model_policy"))
    observed_models = tuple(
        str(item.get("model_id") or "") for item in _mapping_rows(model_policy.get("models"))
    )
    if observed_models != _EXPECTED_MODELS:
        errors.append("AUDIT_MODEL_LADDER_INVALID")
    if model_policy.get("primary_classification_model") != "M1_RIDGE_LINEAR":
        errors.append("AUDIT_CLASSIFICATION_MODEL_INVALID")
    if model_policy.get("primary_classification_feature_prefix") != "CROSS_ASSET_STATE":
        errors.append("AUDIT_CLASSIFICATION_FEATURE_PREFIX_INVALID")
    classification = _mapping(policy.get("classification_policy"))
    if classification.get("status") != "TEMPORARY_PILOT_BASELINE":
        errors.append("AUDIT_CLASSIFICATION_GOVERNANCE_INVALID")
    required_classification_fields = (
        "minimum_valid_folds",
        "minimum_positive_spearman_folds",
        "minimum_median_spearman",
        "minimum_pooled_spearman",
        "minimum_median_rmse_improvement_vs_m0",
        "minimum_median_quintile_spread",
        "minimum_median_directional_uplift_vs_m0",
        "minimum_passing_horizons",
    )
    if any(classification.get(field) is None for field in required_classification_fields):
        errors.append("AUDIT_CLASSIFICATION_THRESHOLDS_MISSING")
    safety = _mapping(policy.get("safety"))
    expected_safety = {
        "research_only": True,
        "historical_seen_only": True,
        "prospective_accessed": False,
        "candidate_family_created": False,
        "candidate_search_executed": False,
        "parameter_search_executed": False,
        "strategy_backtest_executed": False,
        "transaction_cost_model_applied": False,
        "target_weights_generated": False,
        "action_universe_changed": False,
        "qld_used_as_signal": False,
        "paper_shadow_changed": False,
        "promotion_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    if any(safety.get(key) != value for key, value in expected_safety.items()):
        errors.append("AUDIT_SAFETY_POLICY_INVALID")
    return tuple(errors)


def _load_snapshot(
    *,
    policy: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    as_of: date,
) -> tuple[dict[str, Any], list[str]]:
    errors: list[str] = []
    loaded: dict[str, Any] = {}
    if snapshot.get("schema_version") != INPUT_SCHEMA_VERSION:
        return loaded, ["INPUT_SNAPSHOT_SCHEMA_INVALID"]
    records = _mapping(snapshot.get("records"))
    authority = _mapping(policy.get("input_authority"))
    expected_files = _mapping(authority.get("expected_files"))
    for role in _INPUT_ROLES:
        record = _mapping(records.get(role))
        expected = _mapping(expected_files.get(role))
        path = Path(str(record.get("path") or ""))
        if not path.is_absolute() or not path.is_file():
            errors.append(f"INPUT_{role.upper()}_MISSING")
            continue
        expected_sha = str(expected.get("sha256") or "")
        expected_size = _int_value(expected.get("size_bytes"))
        actual_sha = sha256_path(path)
        actual_size = path.stat().st_size
        if (
            record.get("sha256") != actual_sha
            or _int_value(record.get("size_bytes")) != actual_size
            or actual_sha != expected_sha
            or actual_size != expected_size
        ):
            errors.append(f"INPUT_{role.upper()}_COMMITMENT_MISMATCH")
            continue
        try:
            if role == "market_panel":
                loaded[role] = pd.read_csv(path, low_memory=False)
            else:
                loaded[role] = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, pd.errors.ParserError):
            errors.append(f"INPUT_{role.upper()}_PARSE_FAILED")

    if errors:
        return loaded, errors
    label = _mapping(loaded.get("label_payload"))
    package = _mapping(loaded.get("source_package"))
    receipt = _mapping(loaded.get("capability_receipt"))
    panel = loaded.get("market_panel")
    if label.get("schema_version") != _EXPECTED_LABEL_SCHEMA:
        errors.append("INPUT_LABEL_SCHEMA_INVALID")
    if label.get("status") != authority.get("label_status"):
        errors.append("INPUT_LABEL_STATUS_INVALID")
    label_rows = _mapping(_mapping(label.get("evaluation"))).get("label_rows")
    if not isinstance(label_rows, Sequence) or isinstance(label_rows, (str, bytes)):
        errors.append("INPUT_LABEL_ROWS_INVALID")
    elif len(label_rows) != _int_value(authority.get("label_row_count")):
        errors.append("INPUT_LABEL_ROW_COUNT_INVALID")
    if label.get("as_of") != as_of.isoformat():
        errors.append("INPUT_AS_OF_MISMATCH")
    if package.get("schema_version") != _EXPECTED_SOURCE_SCHEMA:
        errors.append("INPUT_SOURCE_PACKAGE_SCHEMA_INVALID")
    if receipt.get("schema_version") != _EXPECTED_RECEIPT_SCHEMA:
        errors.append("INPUT_RECEIPT_SCHEMA_INVALID")
    expected_receipt_fields = {
        "receipt_id": authority.get("capability_receipt_id"),
        "capability_id": authority.get("capability_id"),
        "capability_version": authority.get("capability_version"),
        "consumer_id": authority.get("capability_consumer_id"),
        "consumer_version": authority.get("capability_consumer_version"),
        "global_cache_pass_claimed": authority.get("global_cache_pass_claimed"),
        "capability_passed": True,
    }
    if any(receipt.get(key) != value for key, value in expected_receipt_fields.items()):
        errors.append("INPUT_RECEIPT_IDENTITY_INVALID")
    if _mapping(receipt.get("full_quality")).get("status") != authority.get(
        "full_canonical_status"
    ):
        errors.append("INPUT_FULL_DQ_DISCLOSURE_INVALID")
    if _mapping(receipt.get("scoped_quality")).get("status") != authority.get("scoped_status"):
        errors.append("INPUT_SCOPED_DQ_STATUS_INVALID")
    package_panel = _mapping(package.get("panel"))
    package_receipt = _mapping(package.get("capability_receipt"))
    if package_panel.get("sha256") != _mapping(records.get("market_panel")).get("sha256"):
        errors.append("INPUT_PACKAGE_PANEL_BINDING_INVALID")
    if package_receipt.get("sha256") != _mapping(records.get("capability_receipt")).get("sha256"):
        errors.append("INPUT_PACKAGE_RECEIPT_BINDING_INVALID")
    source_commitment = _mapping(_mapping(label.get("evaluation")).get("source_package_commitment"))
    if source_commitment.get("panel_sha256") != package_panel.get("sha256"):
        errors.append("INPUT_LABEL_PANEL_BINDING_INVALID")
    if source_commitment.get("capability_receipt_id") != receipt.get("receipt_id"):
        errors.append("INPUT_LABEL_RECEIPT_BINDING_INVALID")
    if not isinstance(panel, pd.DataFrame):
        errors.append("INPUT_MARKET_PANEL_INVALID")
    else:
        errors.extend(_panel_errors(panel, as_of=as_of))
    safety = _mapping(snapshot.get("safety"))
    if (
        safety.get("source_files_mutated") is not False
        or safety.get("canonical_cache_mutated") is not False
        or safety.get("prospective_accessed") is not False
        or safety.get("production_effect") != "none"
        or safety.get("broker_action") != "none"
    ):
        errors.append("INPUT_SNAPSHOT_SAFETY_INVALID")
    return loaded, errors


def _panel_errors(panel: pd.DataFrame, *, as_of: date) -> list[str]:
    errors: list[str] = []
    required_columns = {"date", "ticker", "adj_close"}
    if not required_columns.issubset(panel.columns):
        return ["INPUT_MARKET_PANEL_COLUMNS_INVALID"]
    dates = pd.to_datetime(panel["date"], errors="coerce")
    prices = pd.to_numeric(panel["adj_close"], errors="coerce")
    tickers = panel["ticker"].astype(str)
    if dates.isna().any() or (dates.dt.date > as_of).any():
        errors.append("INPUT_MARKET_PANEL_DATE_INVALID")
    if prices.isna().any() or (~np.isfinite(prices)).any() or (prices <= 0).any():
        errors.append("INPUT_MARKET_PANEL_PRICE_INVALID")
    if set(tickers.unique()) != {"QQQ", "SPY", "SGOV"}:
        errors.append("INPUT_MARKET_PANEL_TICKERS_INVALID")
    if pd.DataFrame({"date": dates, "ticker": tickers}).duplicated().any():
        errors.append("INPUT_MARKET_PANEL_DUPLICATE_KEYS")
    order = pd.DataFrame({"date": dates, "ticker": tickers})
    sorted_order = order.sort_values(["date", "ticker"], kind="stable").reset_index(drop=True)
    if not order.reset_index(drop=True).equals(sorted_order):
        errors.append("INPUT_MARKET_PANEL_ROW_ORDER_INVALID")
    counts = order.groupby("date")["ticker"].nunique()
    if counts.empty or not (counts == 3).all():
        errors.append("INPUT_MARKET_PANEL_COMMON_SESSION_INVALID")
    return errors


def _evaluate_model_ladder(
    *,
    policy: Mapping[str, Any],
    label_payload: Mapping[str, Any],
    panel: pd.DataFrame,
) -> dict[str, Any]:
    feature_frame, feature_family_ids = _build_feature_frame(policy, panel)
    label_frame = _label_frame(label_payload)
    common_dates = _common_label_dates(label_frame)
    all_feature_ids = [
        feature for family in _EXPECTED_FAMILIES for feature in feature_family_ids[family]
    ]
    eligible_dates = [
        value
        for value in common_dates
        if value in feature_frame.index
        and _row_features_present(feature_frame, value, all_feature_ids)
    ]
    folds = _build_folds(policy, eligible_dates)
    model_specs = _mapping_rows(_mapping(policy.get("model_policy")).get("models"))
    family_prefixes = _family_prefixes(feature_family_ids)
    interactions = _mapping_rows(_mapping(policy.get("model_policy")).get("interactions"))
    label_lookup: dict[tuple[str, str], dict[str, Any]] = {}
    for raw_row in label_frame.to_dict(orient="records"):
        row = {str(key): value for key, value in raw_row.items()}
        label_lookup[(str(row["horizon_id"]), str(row["decision_date"]))] = row
    predictions: list[dict[str, Any]] = []
    fold_metrics: list[dict[str, Any]] = []
    fold_ledger: list[dict[str, Any]] = []
    for fold in folds:
        fold_ledger.append({key: value for key, value in fold.items() if key != "_dates"})
        for horizon in _EXPECTED_HORIZONS:
            horizon_rows = {
                value: label_lookup[(horizon, value)]
                for value in eligible_dates
                if (horizon, value) in label_lookup
            }
            train_dates, coverage = _eligible_train_dates(
                fold=fold,
                horizon_rows=horizon_rows,
            )
            test_dates = [
                value for value in _sequence(fold["_dates"]["test_dates"]) if value in horizon_rows
            ]
            for target_id in _EXPECTED_TARGETS:
                y_train: FloatArray = np.asarray(
                    [_target_value(horizon_rows[value], target_id) for value in train_dates],
                    dtype=np.float64,
                )
                y_test: FloatArray = np.asarray(
                    [_target_value(horizon_rows[value], target_id) for value in test_dates],
                    dtype=np.float64,
                )
                m0_predictions: FloatArray = np.full(
                    shape=len(test_dates),
                    fill_value=float(np.mean(y_train)) if len(y_train) else np.nan,
                    dtype=np.float64,
                )
                m0_metric = _metric_row(
                    fold_id=str(fold["fold_id"]),
                    target_id=target_id,
                    horizon_id=horizon,
                    model_id="M0_TRAIN_MEAN",
                    feature_prefix="NONE",
                    actual=y_test,
                    predicted=m0_predictions,
                    m0_predicted=m0_predictions,
                    train_row_count=len(train_dates),
                    test_dates=test_dates,
                    coverage=coverage,
                    policy=policy,
                )
                fold_metrics.append(m0_metric)
                predictions.extend(
                    _prediction_rows(
                        fold_id=str(fold["fold_id"]),
                        target_id=target_id,
                        horizon_id=horizon,
                        model_id="M0_TRAIN_MEAN",
                        feature_prefix="NONE",
                        dates=test_dates,
                        actual=y_test,
                        predicted=m0_predictions,
                    )
                )
                for feature_prefix, feature_ids in family_prefixes:
                    train_frame = feature_frame.loc[train_dates, feature_ids]
                    test_frame = feature_frame.loc[test_dates, feature_ids]
                    for model in model_specs:
                        model_id = str(model.get("model_id") or "")
                        if model_id == "M0_TRAIN_MEAN":
                            continue
                        x_train, x_test = _model_matrix(
                            model_id=model_id,
                            train=train_frame,
                            test=test_frame,
                            feature_ids=feature_ids,
                            interactions=interactions,
                        )
                        predicted = _ridge_predict(
                            x_train=x_train,
                            y_train=y_train,
                            x_test=x_test,
                            penalty=float(model.get("ridge_penalty") or 0.0),
                            zero_scale_epsilon=float(
                                _mapping(policy.get("model_policy")).get(
                                    "standardization_zero_scale_epsilon"
                                )
                                or _NUMERIC_EPSILON
                            ),
                        )
                        metric = _metric_row(
                            fold_id=str(fold["fold_id"]),
                            target_id=target_id,
                            horizon_id=horizon,
                            model_id=model_id,
                            feature_prefix=feature_prefix,
                            actual=y_test,
                            predicted=predicted,
                            m0_predicted=m0_predictions,
                            train_row_count=len(train_dates),
                            test_dates=test_dates,
                            coverage=coverage,
                            policy=policy,
                        )
                        fold_metrics.append(metric)
                        predictions.extend(
                            _prediction_rows(
                                fold_id=str(fold["fold_id"]),
                                target_id=target_id,
                                horizon_id=horizon,
                                model_id=model_id,
                                feature_prefix=feature_prefix,
                                dates=test_dates,
                                actual=y_test,
                                predicted=predicted,
                            )
                        )
    aggregate_metrics = _aggregate_metrics(
        policy=policy,
        fold_metrics=fold_metrics,
        predictions=predictions,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "requested_range": _mapping(label_payload.get("evaluation")).get("requested_range"),
        "evaluated_range": {
            "start": min(eligible_dates),
            "end": max(eligible_dates),
        },
        "decision_universe": {
            "common_horizon_session_count": len(common_dates),
            "feature_eligible_session_count": len(eligible_dates),
            "minimum_history_sessions": _int_value(
                _mapping(policy.get("feature_policy")).get("minimum_history_sessions")
            ),
        },
        "feature_family_order": list(_EXPECTED_FAMILIES),
        "feature_ids_by_family": feature_family_ids,
        "fold_ledger": fold_ledger,
        "fold_metrics": fold_metrics,
        "aggregate_metrics": aggregate_metrics,
        "predictions": predictions,
    }


def _build_feature_frame(
    policy: Mapping[str, Any],
    panel: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, list[str]]]:
    frame = panel.copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.strftime("%Y-%m-%d")
    frame["adj_close"] = pd.to_numeric(frame["adj_close"])
    prices = frame.pivot(index="date", columns="ticker", values="adj_close").sort_index()
    features = pd.DataFrame(index=prices.index)
    family_ids: dict[str, list[str]] = {family: [] for family in _EXPECTED_FAMILIES}
    annualization = float(
        _mapping(policy.get("feature_policy")).get("annualization_sessions") or 252
    )
    definitions = _mapping_rows(_mapping(policy.get("feature_policy")).get("features"))
    for definition in definitions:
        feature_id = str(definition["feature_id"])
        family = str(definition["family"])
        kind = str(definition["kind"])
        if kind == "total_return":
            asset = str(definition["asset"])
            sessions = _int_value(definition["sessions"])
            values = prices[asset] / prices[asset].shift(sessions) - 1.0
        elif kind == "relative_return":
            asset = str(definition["asset"])
            benchmark = str(definition["benchmark"])
            sessions = _int_value(definition["sessions"])
            relative = prices[asset] / prices[benchmark]
            values = relative / relative.shift(sessions) - 1.0
        elif kind == "realized_volatility":
            asset = str(definition["asset"])
            sessions = _int_value(definition["sessions"])
            values = prices[asset].pct_change(fill_method=None).rolling(
                sessions, min_periods=sessions
            ).std(ddof=0) * math.sqrt(annualization)
        elif kind == "current_drawdown":
            asset = str(definition["asset"])
            sessions = _int_value(definition["sessions"])
            peak = prices[asset].rolling(sessions, min_periods=sessions).max()
            values = prices[asset] / peak - 1.0
        elif kind == "difference":
            left = str(definition["left_feature"])
            right = str(definition["right_feature"])
            if left not in features or right not in features:
                raise ValueError(f"feature dependency must precede difference: {feature_id}")
            values = features[left] - features[right]
        else:
            raise ValueError(f"unsupported decision-target feature kind: {kind}")
        features[feature_id] = values.astype(float)
        family_ids[family].append(feature_id)
    return features, family_ids


def _label_frame(label_payload: Mapping[str, Any]) -> pd.DataFrame:
    rows = _mapping(_mapping(label_payload.get("evaluation"))).get("label_rows")
    frame = pd.DataFrame(list(_sequence(rows)))
    horizon_order = {value: index for index, value in enumerate(_EXPECTED_HORIZONS)}
    observed = [
        (str(row["decision_date"]), str(row["horizon_id"]))
        for row in frame.to_dict(orient="records")
    ]
    expected = sorted(observed, key=lambda item: (item[0], horizon_order[item[1]]))
    if observed != expected:
        raise ValueError("label rows are not in canonical decision-date/horizon order")
    if frame.duplicated(["decision_date", "horizon_id"]).any():
        raise ValueError("duplicate decision target label rows")
    return frame


def _common_label_dates(label_frame: pd.DataFrame) -> list[str]:
    counts = label_frame.groupby("decision_date")["horizon_id"].nunique()
    return sorted(str(value) for value in counts.loc[counts == len(_EXPECTED_HORIZONS)].index)


def _row_features_present(
    feature_frame: pd.DataFrame,
    decision_date: str,
    feature_ids: Sequence[str],
) -> bool:
    values = feature_frame.loc[[decision_date], list(feature_ids)].to_numpy(dtype=np.float64)
    return bool(np.isfinite(values).all())


def _build_folds(
    policy: Mapping[str, Any],
    eligible_dates: Sequence[str],
) -> list[dict[str, Any]]:
    split = _mapping(policy.get("split_policy"))
    initial = _int_value(split.get("initial_train_sessions"))
    test_sessions = _int_value(split.get("test_sessions"))
    minimum_final = _int_value(split.get("minimum_final_test_sessions"))
    embargo = _int_value(split.get("embargo_sessions"))
    if len(eligible_dates) < initial + minimum_final:
        return []
    folds: list[dict[str, Any]] = []
    test_start_index = initial
    fold_index = 1
    while test_start_index < len(eligible_dates):
        remaining = len(eligible_dates) - test_start_index
        if remaining < test_sessions and (
            not bool(split.get("include_final_partial_fold")) or remaining < minimum_final
        ):
            break
        test_count = min(test_sessions, remaining)
        test_end_index = test_start_index + test_count - 1
        embargo_start_index = max(0, test_start_index - embargo)
        embargo_end_index = min(len(eligible_dates) - 1, test_end_index + embargo)
        train_cutoff_index = embargo_start_index - 1
        if train_cutoff_index < 0:
            break
        test_dates = list(eligible_dates[test_start_index : test_end_index + 1])
        train_candidate_dates = list(eligible_dates[:test_start_index])
        folds.append(
            {
                "fold_id": f"F{fold_index:02d}",
                "train_start": eligible_dates[0],
                "train_cutoff": eligible_dates[train_cutoff_index],
                "test_start": test_dates[0],
                "test_end": test_dates[-1],
                "embargo_start": eligible_dates[embargo_start_index],
                "embargo_end": eligible_dates[embargo_end_index],
                "eligible_train_session_count": train_cutoff_index + 1,
                "test_session_count": len(test_dates),
                "_dates": {
                    "train_candidate_dates": train_candidate_dates,
                    "test_dates": test_dates,
                },
            }
        )
        fold_index += 1
        test_start_index += test_count
    return folds


def _eligible_train_dates(
    *,
    fold: Mapping[str, Any],
    horizon_rows: Mapping[str, Mapping[str, Any]],
) -> tuple[list[str], dict[str, int]]:
    candidates = [
        str(item)
        for item in _sequence(_mapping(fold.get("_dates")).get("train_candidate_dates"))
        if str(item) in horizon_rows
    ]
    test_start = str(fold["test_start"])
    test_end = str(fold["test_end"])
    embargo_start = str(fold["embargo_start"])
    embargo_end = str(fold["embargo_end"])
    train_cutoff = str(fold["train_cutoff"])
    matured: list[str] = []
    purged_test = 0
    purged_embargo = 0
    embargoed_decision = 0
    for decision_date in candidates:
        row = horizon_rows[decision_date]
        label_start = str(row["label_start_date"])
        label_end = str(row["label_end_date"])
        available = str(row["label_available_on_session"])
        if decision_date >= embargo_start:
            embargoed_decision += 1
            continue
        if available > train_cutoff:
            continue
        if _intervals_overlap(label_start, label_end, test_start, test_end):
            purged_test += 1
            continue
        if _intervals_overlap(label_start, label_end, embargo_start, embargo_end):
            purged_embargo += 1
            continue
        matured.append(decision_date)
    return matured, {
        "candidate_train_row_count": len(candidates),
        "matured_train_row_count": len(matured),
        "embargoed_decision_row_count": embargoed_decision,
        "purged_test_overlap_row_count": purged_test,
        "purged_embargo_overlap_row_count": purged_embargo,
    }


def _family_prefixes(
    feature_family_ids: Mapping[str, Sequence[str]],
) -> list[tuple[str, list[str]]]:
    result: list[tuple[str, list[str]]] = []
    accumulated: list[str] = []
    for family in _EXPECTED_FAMILIES:
        accumulated.extend(str(item) for item in feature_family_ids[family])
        result.append((family, list(accumulated)))
    return result


def _model_matrix(
    *,
    model_id: str,
    train: pd.DataFrame,
    test: pd.DataFrame,
    feature_ids: Sequence[str],
    interactions: Sequence[Mapping[str, Any]],
) -> tuple[FloatArray, FloatArray]:
    train_values: FloatArray = train.loc[:, list(feature_ids)].to_numpy(dtype=np.float64)
    test_values: FloatArray = test.loc[:, list(feature_ids)].to_numpy(dtype=np.float64)
    if model_id != "M2_RIDGE_INTERACTION":
        return train_values, test_values
    train_columns = [train_values]
    test_columns = [test_values]
    feature_index = {feature_id: index for index, feature_id in enumerate(feature_ids)}
    for interaction in interactions:
        left = str(interaction.get("left_feature") or "")
        right = str(interaction.get("right_feature") or "")
        if left not in feature_index or right not in feature_index:
            continue
        train_columns.append(
            (train_values[:, feature_index[left]] * train_values[:, feature_index[right]]).reshape(
                -1, 1
            )
        )
        test_columns.append(
            (test_values[:, feature_index[left]] * test_values[:, feature_index[right]]).reshape(
                -1, 1
            )
        )
    return np.concatenate(train_columns, axis=1), np.concatenate(test_columns, axis=1)


def _ridge_predict(
    *,
    x_train: FloatArray,
    y_train: FloatArray,
    x_test: FloatArray,
    penalty: float,
    zero_scale_epsilon: float,
) -> FloatArray:
    if len(y_train) == 0:
        return np.full(len(x_test), np.nan, dtype=np.float64)
    mean = np.mean(x_train, axis=0)
    scale = np.std(x_train, axis=0, ddof=0)
    scale = np.where(scale <= zero_scale_epsilon, 1.0, scale)
    standardized_train = (x_train - mean) / scale
    standardized_test = (x_test - mean) / scale
    y_mean = float(np.mean(y_train))
    centered = y_train - y_mean
    gram = standardized_train.T @ standardized_train
    rhs = standardized_train.T @ centered
    regularized = gram + penalty * np.eye(gram.shape[0], dtype=float)
    try:
        coefficients = np.linalg.solve(regularized, rhs)
    except np.linalg.LinAlgError:
        coefficients = np.linalg.lstsq(regularized, rhs, rcond=None)[0]
    return np.asarray(y_mean + standardized_test @ coefficients, dtype=np.float64)


def _metric_row(
    *,
    fold_id: str,
    target_id: str,
    horizon_id: str,
    model_id: str,
    feature_prefix: str,
    actual: FloatArray,
    predicted: FloatArray,
    m0_predicted: FloatArray,
    train_row_count: int,
    test_dates: Sequence[str],
    coverage: Mapping[str, int],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    minimum_rows = _int_value(_mapping(policy.get("metric_policy")).get("minimum_rows_per_metric"))
    minimum_train_rows = _int_value(_mapping(policy.get("split_policy")).get("minimum_train_rows"))
    valid = bool(
        train_row_count >= minimum_train_rows
        and len(actual) >= minimum_rows
        and bool(np.isfinite(actual).all())
        and bool(np.isfinite(predicted).all())
    )
    if not valid:
        metrics: dict[str, float | None] = {
            "mae": None,
            "rmse": None,
            "pearson": None,
            "spearman": None,
            "directional_accuracy": None,
            "directional_uplift_vs_m0": None,
            "quintile_spread": None,
        }
    else:
        metrics = _metrics(
            actual=actual,
            predicted=predicted,
            m0_predicted=m0_predicted,
            target_id=target_id,
            quintile_count=_int_value(_mapping(policy.get("metric_policy")).get("quintile_count")),
        )
    return {
        "fold_id": fold_id,
        "target_id": target_id,
        "horizon_id": horizon_id,
        "model_id": model_id,
        "feature_prefix": feature_prefix,
        "train_row_count": train_row_count,
        "test_row_count": len(actual),
        "test_start": min(test_dates) if test_dates else None,
        "test_end": max(test_dates) if test_dates else None,
        "valid": valid,
        **{key: int(value) for key, value in coverage.items()},
        **metrics,
    }


def _metrics(
    *,
    actual: FloatArray,
    predicted: FloatArray,
    m0_predicted: FloatArray,
    target_id: str,
    quintile_count: int,
) -> dict[str, float | None]:
    residual = predicted - actual
    mae = float(np.mean(np.abs(residual)))
    rmse = float(math.sqrt(float(np.mean(np.square(residual)))))
    pearson = _correlation(actual, predicted)
    spearman = _spearman(actual, predicted)
    directional_accuracy: float | None
    directional_uplift: float | None
    if target_id in _TAIL_TARGETS:
        directional_accuracy = None
        directional_uplift = None
    else:
        directional_accuracy = float(np.mean((predicted >= 0.0) == (actual >= 0.0)))
        m0_accuracy = float(np.mean((m0_predicted >= 0.0) == (actual >= 0.0)))
        directional_uplift = directional_accuracy - m0_accuracy
    quintile_spread = _quintile_spread(
        actual=actual,
        predicted=predicted,
        quintile_count=quintile_count,
    )
    return {
        "mae": _finite_or_none(mae),
        "rmse": _finite_or_none(rmse),
        "pearson": _finite_or_none(pearson),
        "spearman": _finite_or_none(spearman),
        "directional_accuracy": _finite_or_none(directional_accuracy),
        "directional_uplift_vs_m0": _finite_or_none(directional_uplift),
        "quintile_spread": _finite_or_none(quintile_spread),
    }


def _prediction_rows(
    *,
    fold_id: str,
    target_id: str,
    horizon_id: str,
    model_id: str,
    feature_prefix: str,
    dates: Sequence[str],
    actual: FloatArray,
    predicted: FloatArray,
) -> list[dict[str, Any]]:
    return [
        {
            "fold_id": fold_id,
            "decision_date": str(decision_date),
            "target_id": target_id,
            "horizon_id": horizon_id,
            "model_id": model_id,
            "feature_prefix": feature_prefix,
            "actual": float(actual[index]),
            "prediction": float(predicted[index]),
        }
        for index, decision_date in enumerate(dates)
    ]


def _aggregate_metrics(
    *,
    policy: Mapping[str, Any],
    fold_metrics: Sequence[Mapping[str, Any]],
    predictions: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    metrics_frame = pd.DataFrame(list(fold_metrics))
    prediction_frame = pd.DataFrame(list(predictions))
    classification = _mapping(policy.get("classification_policy"))
    primary_model = str(_mapping(policy.get("model_policy")).get("primary_classification_model"))
    primary_prefix = str(
        _mapping(policy.get("model_policy")).get("primary_classification_feature_prefix")
    )
    aggregates: list[dict[str, Any]] = []
    keys = ["target_id", "horizon_id", "model_id", "feature_prefix"]
    for values, rows in metrics_frame.groupby(keys, sort=True):
        target_id, horizon_id, model_id, feature_prefix = (str(item) for item in values)
        valid = rows.loc[rows["valid"] == True].copy()  # noqa: E712
        pred = prediction_frame.loc[
            (prediction_frame["target_id"] == target_id)
            & (prediction_frame["horizon_id"] == horizon_id)
            & (prediction_frame["model_id"] == model_id)
            & (prediction_frame["feature_prefix"] == feature_prefix)
        ]
        valid_spearman = [float(value) for value in valid["spearman"].dropna().tolist()]
        valid_rmse = [float(value) for value in valid["rmse"].dropna().tolist()]
        valid_spread = [float(value) for value in valid["quintile_spread"].dropna().tolist()]
        valid_direction = [
            float(value) for value in valid["directional_uplift_vs_m0"].dropna().tolist()
        ]
        m0_rows = metrics_frame.loc[
            (metrics_frame["target_id"] == target_id)
            & (metrics_frame["horizon_id"] == horizon_id)
            & (metrics_frame["model_id"] == "M0_TRAIN_MEAN")
            & (metrics_frame["feature_prefix"] == "NONE")
            & (metrics_frame["valid"] == True)  # noqa: E712
        ]
        m0_rmse_by_fold = {
            str(row["fold_id"]): float(row["rmse"])
            for row in m0_rows.to_dict(orient="records")
            if row["rmse"] is not None
        }
        rmse_improvements = [
            m0_rmse_by_fold[str(row["fold_id"])] - float(row["rmse"])
            for row in valid.to_dict(orient="records")
            if row["rmse"] is not None and str(row["fold_id"]) in m0_rmse_by_fold
        ]
        pooled_actual: FloatArray = pred["actual"].to_numpy(dtype=np.float64)
        pooled_prediction: FloatArray = pred["prediction"].to_numpy(dtype=np.float64)
        pooled_spearman = _spearman(pooled_actual, pooled_prediction)
        median_spearman = _median_or_none(valid_spearman)
        median_rmse = _median_or_none(valid_rmse)
        median_rmse_improvement = _median_or_none(rmse_improvements)
        median_spread = _median_or_none(valid_spread)
        median_direction = _median_or_none(valid_direction)
        gate_reasons: list[str] = []
        if len(valid) < _int_value(classification.get("minimum_valid_folds")):
            gate_reasons.append("VALID_FOLD_COUNT_BELOW_FLOOR")
        if sum(value > 0.0 for value in valid_spearman) < _int_value(
            classification.get("minimum_positive_spearman_folds")
        ):
            gate_reasons.append("POSITIVE_SPEARMAN_FOLD_COUNT_BELOW_FLOOR")
        if median_spearman is None or median_spearman < float(
            classification.get("minimum_median_spearman") or 0.0
        ):
            gate_reasons.append("MEDIAN_SPEARMAN_BELOW_FLOOR")
        if pooled_spearman is None or pooled_spearman < float(
            classification.get("minimum_pooled_spearman") or 0.0
        ):
            gate_reasons.append("POOLED_SPEARMAN_BELOW_FLOOR")
        if median_rmse_improvement is None or median_rmse_improvement <= float(
            classification.get("minimum_median_rmse_improvement_vs_m0") or 0.0
        ):
            gate_reasons.append("RMSE_IMPROVEMENT_VS_M0_NOT_POSITIVE")
        if median_spread is None or median_spread <= float(
            classification.get("minimum_median_quintile_spread") or 0.0
        ):
            gate_reasons.append("QUINTILE_SPREAD_NOT_POSITIVE")
        if target_id in _RETURN_TARGETS and (
            median_direction is None
            or median_direction
            <= float(classification.get("minimum_median_directional_uplift_vs_m0") or 0.0)
        ):
            gate_reasons.append("DIRECTIONAL_UPLIFT_VS_M0_NOT_POSITIVE")
        classification_eligible = model_id == primary_model and feature_prefix == primary_prefix
        aggregates.append(
            {
                "target_id": target_id,
                "horizon_id": horizon_id,
                "model_id": model_id,
                "feature_prefix": feature_prefix,
                "valid_fold_count": int(len(valid)),
                "positive_spearman_fold_count": int(sum(value > 0.0 for value in valid_spearman)),
                "median_mae": _median_or_none(
                    [float(value) for value in valid["mae"].dropna().tolist()]
                ),
                "median_rmse": median_rmse,
                "median_spearman": median_spearman,
                "pooled_spearman": _finite_or_none(pooled_spearman),
                "median_rmse_improvement_vs_m0": median_rmse_improvement,
                "median_directional_uplift_vs_m0": median_direction,
                "median_quintile_spread": median_spread,
                "classification_eligible": classification_eligible,
                "capability_pass": classification_eligible and not gate_reasons,
                "gate_reasons": gate_reasons,
            }
        )
    return aggregates


def _build_summary(
    *,
    policy: Mapping[str, Any],
    evaluation: Mapping[str, Any],
) -> dict[str, Any]:
    aggregate = _mapping_rows(evaluation.get("aggregate_metrics"))
    eligible = [row for row in aggregate if row.get("classification_eligible") is True]
    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "fold_count": len(_sequence(evaluation.get("fold_ledger"))),
        "feature_family_count": len(_EXPECTED_FAMILIES),
        "feature_count": sum(
            len(_sequence(value))
            for value in _mapping(evaluation.get("feature_ids_by_family")).values()
        ),
        "target_count": len(_EXPECTED_TARGETS),
        "horizon_count": len(_EXPECTED_HORIZONS),
        "model_count": len(_EXPECTED_MODELS),
        "fold_metric_row_count": len(_sequence(evaluation.get("fold_metrics"))),
        "aggregate_metric_row_count": len(aggregate),
        "prediction_row_count": len(_sequence(evaluation.get("predictions"))),
        "classification_eligible_row_count": len(eligible),
        "classification_passing_row_count": sum(
            row.get("capability_pass") is True for row in eligible
        ),
        "classification_model": _mapping(policy.get("model_policy")).get(
            "primary_classification_model"
        ),
        "classification_feature_prefix": _mapping(policy.get("model_policy")).get(
            "primary_classification_feature_prefix"
        ),
        "historical_seen_only": True,
        "candidate_family_created": False,
        "strategy_backtest_executed": False,
        "target_weights_generated": False,
    }


def _classify_style(
    *,
    policy: Mapping[str, Any],
    evaluation: Mapping[str, Any],
) -> dict[str, Any]:
    classification = _mapping(policy.get("classification_policy"))
    required_count = _int_value(classification.get("minimum_passing_horizons"))
    medium_or_long = {
        str(item) for item in _sequence(classification.get("required_medium_or_long_horizons"))
    }
    aggregate = [
        row
        for row in _mapping_rows(evaluation.get("aggregate_metrics"))
        if row.get("classification_eligible") is True
    ]
    target_rows: list[dict[str, Any]] = []
    target_pass: dict[str, bool] = {}
    partial_evidence = False
    for target_id in _EXPECTED_TARGETS:
        rows = [row for row in aggregate if row.get("target_id") == target_id]
        passing_set = {str(row["horizon_id"]) for row in rows if row.get("capability_pass") is True}
        passing = [horizon for horizon in _EXPECTED_HORIZONS if horizon in passing_set]
        passes = len(passing) >= required_count and bool(set(passing) & medium_or_long)
        target_pass[target_id] = passes
        partial_evidence = partial_evidence or bool(passing)
        target_rows.append(
            {
                "target_id": target_id,
                "passing_horizons": passing,
                "passing_horizon_count": len(passing),
                "status": "TARGET_SKILL_SUPPORTED" if passes else "TARGET_SKILL_NOT_SUPPORTED",
            }
        )

    return_skills = {target for target in _RETURN_TARGETS if target_pass.get(target, False)}
    tail_skill = any(target_pass.get(target, False) for target in _TAIL_TARGETS)
    maximum_valid_folds = max(
        (_int_value(row.get("valid_fold_count")) for row in aggregate),
        default=0,
    )
    if maximum_valid_folds < _int_value(classification.get("minimum_valid_folds")):
        style_status = "INSUFFICIENT_FOLD_COVERAGE"
        interpretation = "可用 outer fold 不足，不能判断系统擅长的策略风格。"
    elif not return_skills:
        if tail_skill:
            style_status = "TAIL_RISK_ONLY_SKILL"
            interpretation = (
                "收益目标没有通过预注册门槛，但左尾目标具有稳定能力；后续只能研究risk gate或"
                "defensive overlay，不能把它解释为收益生成策略。"
            )
        elif partial_evidence:
            style_status = "MIXED_OR_UNSTABLE_SKILL"
            interpretation = (
                "部分horizon出现能力，但没有达到跨horizon稳定门槛；不得据此创建新family。"
            )
        else:
            style_status = "NO_MEASURABLE_SKILL"
            interpretation = (
                "冻结输入和简单模型阶梯相对M0没有可重复能力；下一步应审计输入/目标，而不是"
                "扩大candidate search。"
            )
    elif target_pass.get("QQQ_MINUS_SGOV", False):
        style_status = "COMBINED_QQQ_DEFENSIVE_ALLOCATION_SKILL"
        interpretation = (
            "QQQ相对SGOV的配置目标通过跨fold门槛；若Owner批准，下一批可预注册独立的"
            "QQQ-defensive allocation假设。SPY只保持分解控制，QLD仍不是信号。"
        )
    elif return_skills == {"SPY_MINUS_SGOV"}:
        style_status = "BROAD_EQUITY_RISK_PREMIUM_SKILL"
        interpretation = "能力集中于broad equity相对防御资产，不支持把结果解释为Nasdaq特有alpha。"
    elif return_skills == {"QQQ_MINUS_SPY"}:
        style_status = "NASDAQ_LEADERSHIP_SKILL"
        interpretation = (
            "能力集中于QQQ相对SPY的leadership判断；若Owner批准，下一批只能研究相对风格，"
            "不能自动推导总风险敞口。"
        )
    else:
        style_status = "MIXED_OR_UNSTABLE_SKILL"
        interpretation = (
            "多个分解目标的能力组合没有形成预注册的单一风格映射；需要人工复核，不自动创建"
            "candidate family。"
        )
    if style_status not in _STYLE_STATUSES:
        raise ValueError("unsupported style classification")
    return {
        "schema_version": "decision_target_capability_style_classification.v1",
        "style_status": style_status,
        "target_capabilities": target_rows,
        "interpretation_zh": interpretation,
        "manual_review_required": True,
        "candidate_family_creation_authorized": False,
        "qld_signal_role_authorized": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _target_value(row: Mapping[str, Any], target_id: str) -> float:
    if target_id in _RETURN_TARGETS:
        return float(_mapping(row.get("excess_return_targets"))[target_id])
    risk = _mapping(_mapping(row.get("future_path_risk")).get("QQQ"))
    if target_id == "QQQ_FUTURE_MAX_DRAWDOWN":
        return float(risk["future_max_drawdown"])
    if target_id == "QQQ_FUTURE_WORST_1D_RETURN":
        return float(risk["future_worst_1d_return"])
    raise ValueError(f"unknown target: {target_id}")


def _blocked_payload(
    *,
    errors: Sequence[str],
    data_quality_evidence: Mapping[str, Any],
    policy: Mapping[str, Any],
    snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2461_DECISION_TARGET_CAPABILITY_AUDIT_MODEL_LADDER",
        "status": BLOCKED_STATUS,
        "report_type": REPORT_TYPE,
        "validation_status": "BLOCKED_BEFORE_MODEL",
        "strict_validation_errors": sorted(set(str(item) for item in errors)),
        "data_quality_evidence": dict(data_quality_evidence),
        "input_snapshot_commitment": _snapshot_commitment(snapshot),
        "evaluation": None,
        "evaluation_commitment_sha256": None,
        "capability_summary": {
            "schema_version": SUMMARY_SCHEMA_VERSION,
            "fold_count": 0,
            "prediction_row_count": 0,
            "model_training_executed": False,
        },
        "style_classification": None,
        "next_route": "FIX_INPUT_OR_PROTOCOL_NO_MODEL_EXECUTION",
        **_safety_payload(policy),
    }
    payload["rendered_markdown"] = render_capability_markdown(payload)
    return payload


def _data_quality_evidence_from_loaded(
    loaded: Mapping[str, Any],
    *,
    as_of: date,
    errors: Sequence[str],
) -> dict[str, Any]:
    label = _mapping(loaded.get("label_payload"))
    evidence = _mapping(label.get("data_quality_evidence"))
    if evidence:
        return dict(evidence)
    blocking = sorted(set(str(item) for item in errors)) or ["INPUT_NOT_AVAILABLE"]
    return {
        "schema_version": "data_quality_evidence.v1",
        "evidence_id": "dq_evidence_trading2461_blocked_input",
        "contract_id": "decision_target_capability_audit_model_ladder_input",
        "policy_id": "DATA_QUALITY_CACHE_GATE",
        "policy_version": "data_quality_cache_gate.v2",
        "status": "FAIL",
        "passed": False,
        "checked_at": f"{as_of.isoformat()}T00:00:00+00:00",
        "as_of": as_of.isoformat(),
        "checked_input_count": 0,
        "error_count": len(blocking),
        "warning_count": 0,
        "blocking_issues": blocking,
        "report_path": "not_materialized",
        "report_sha256": "0" * 64,
    }


def _safety_payload(policy: Mapping[str, Any]) -> dict[str, Any]:
    safety = _mapping(policy.get("safety"))
    return {
        "research_only": safety.get("research_only", True),
        "historical_seen_only": safety.get("historical_seen_only", True),
        "prospective_accessed": safety.get("prospective_accessed", False),
        "candidate_family_created": safety.get("candidate_family_created", False),
        "candidate_search_executed": safety.get("candidate_search_executed", False),
        "parameter_search_executed": safety.get("parameter_search_executed", False),
        "strategy_backtest_executed": safety.get("strategy_backtest_executed", False),
        "transaction_cost_model_applied": safety.get("transaction_cost_model_applied", False),
        "target_weights_generated": safety.get("target_weights_generated", False),
        "action_universe_changed": safety.get("action_universe_changed", False),
        "qld_used_as_signal": safety.get("qld_used_as_signal", False),
        "paper_shadow_changed": safety.get("paper_shadow_changed", False),
        "promotion_allowed": safety.get("promotion_allowed", False),
        "production_effect": safety.get("production_effect", "none"),
        "broker_action": safety.get("broker_action", "none"),
    }


def _snapshot_commitment(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    records = _mapping(snapshot.get("records"))
    return {
        "schema_version": snapshot.get("schema_version"),
        "captured_at": snapshot.get("captured_at"),
        "records": {
            role: {
                "path": _mapping(records.get(role)).get("path"),
                "sha256": _mapping(records.get(role)).get("sha256"),
                "size_bytes": _mapping(records.get(role)).get("size_bytes"),
            }
            for role in _INPUT_ROLES
        },
    }


def _file_record(path: Path) -> dict[str, Any]:
    return {
        "path": str(path.resolve()),
        "sha256": sha256_path(path),
        "size_bytes": path.stat().st_size,
    }


def _intervals_overlap(
    left_start: str,
    left_end: str,
    right_start: str,
    right_end: str,
) -> bool:
    return left_start <= right_end and right_start <= left_end


def _correlation(left: FloatArray, right: FloatArray) -> float | None:
    if len(left) < 2 or np.std(left) <= _NUMERIC_EPSILON or np.std(right) <= _NUMERIC_EPSILON:
        return None
    return _finite_or_none(float(np.corrcoef(left, right)[0, 1]))


def _spearman(left: FloatArray, right: FloatArray) -> float | None:
    if len(left) < 2:
        return None
    left_rank: FloatArray = np.asarray(
        pd.Series(left).rank(method="average").to_numpy(),
        dtype=np.float64,
    )
    right_rank: FloatArray = np.asarray(
        pd.Series(right).rank(method="average").to_numpy(),
        dtype=np.float64,
    )
    return _correlation(left_rank, right_rank)


def _quintile_spread(
    *,
    actual: FloatArray,
    predicted: FloatArray,
    quintile_count: int,
) -> float | None:
    if len(actual) < quintile_count * 2 or np.std(predicted) <= _NUMERIC_EPSILON:
        return None
    order = np.argsort(predicted, kind="stable")
    bucket_size = len(order) // quintile_count
    if bucket_size == 0:
        return None
    bottom = order[:bucket_size]
    top = order[-bucket_size:]
    return float(np.mean(actual[top]) - np.mean(actual[bottom]))


def _median_or_none(values: Sequence[float]) -> float | None:
    return _finite_or_none(float(np.median(values))) if values else None


def _finite_or_none(value: float | None) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return float(value)


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("captured_at must be timezone-aware")
    return value.astimezone(UTC).replace(microsecond=0)


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _sequence(value: object) -> Sequence[Any]:
    return value if isinstance(value, Sequence) and not isinstance(value, (str, bytes)) else ()


def _mapping_rows(value: object) -> list[Mapping[str, Any]]:
    return [_mapping(item) for item in _sequence(value)]


def _string_list(value: object) -> list[str]:
    return [str(item) for item in _sequence(value)]


def _int_value(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, np.integer)):
        return 0
    return int(value)


__all__ = [
    "BLOCKED_STATUS",
    "READY_STATUS",
    "build_capability_payload",
    "capability_summary",
    "capability_view_model",
    "capture_input_snapshot",
    "decision_target_capability_model_ladder_registry",
    "render_capability_markdown",
    "validate_capability_payload",
]
