from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.yaml_loader import safe_load_yaml_path

PAPERBROKER_FILL_MODEL_CALIBRATION_SCHEMA_VERSION = 1
PAPERBROKER_FILL_MODEL_CALIBRATION_REPORT_TYPE = "paperbroker_fill_model_calibration"
CALIBRATION_MODE = "diagnostic_only"
PRODUCTION_EFFECT = "none"
REPO_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_PAPERBROKER_FILL_MODEL_CALIBRATION_POLICY_PATH = (
    REPO_ROOT / "config" / "paperbroker_fill_model_calibration_policy.yaml"
)
COMPARISON_REPORT_TYPE = "paperbroker_vs_ibkr_paper_comparison"
CONTROLLED_FILL_REPORT_TYPE = "ibkr_paper_controlled_fill"
CONTROLLED_FILL_NO_FILL_CLASSIFICATION = "NO_FILL_LIFECYCLE_VALIDATED"

STATUS_INSUFFICIENT_SAMPLE = "INSUFFICIENT_SAMPLE"
STATUS_LIFECYCLE_ALIGNED_FILL_UNTESTED = "LIFECYCLE_ALIGNED_FILL_UNTESTED"
STATUS_LOCAL_SIM_TOO_OPTIMISTIC = "LOCAL_SIM_TOO_OPTIMISTIC"
STATUS_BROKER_REJECTION_GAP = "BROKER_REJECTION_GAP"
STATUS_OBSERVE_ONLY = "OBSERVE_ONLY"
ALLOWED_CALIBRATION_STATUSES = {
    STATUS_INSUFFICIENT_SAMPLE,
    STATUS_LIFECYCLE_ALIGNED_FILL_UNTESTED,
    STATUS_LOCAL_SIM_TOO_OPTIMISTIC,
    STATUS_BROKER_REJECTION_GAP,
    STATUS_OBSERVE_ONLY,
}

NO_FILL_LIFECYCLE_RECOMMENDATIONS = [
    "lifecycle aligned for basic LIMIT DAY cancel path",
    "fill model remains unvalidated",
    "collect near-market or controlled small-fill IBKR Paper samples later",
    "do not modify PaperBroker fill model yet",
]

REASON_EXPLANATIONS = {
    "insufficient_sample": "没有可用 comparison 样本，无法解释 fill model 差异。",
    "no_fill_lifecycle_only": (
        "当前样本只覆盖 open/cancel lifecycle，未观察到本地或 IBKR Paper fill。"
    ),
    "local_sim_too_optimistic": (
        "本地 PaperBroker 已成交但 IBKR Paper 未成交，需要继续诊断本地 fill 条件。"
    ),
    "broker_rejection_gap": (
        "IBKR Paper 出现 rejection 而本地路径未体现同等阻断，需要补充 rejection reason 映射。"
    ),
    "observe_only": "当前没有触发特定差异状态，但仍仅用于观察，不改变执行行为。",
}


def default_paperbroker_fill_model_calibration_json_path(
    reports_dir: Path,
    as_of: date,
) -> Path:
    return (
        reports_dir / f"{PAPERBROKER_FILL_MODEL_CALIBRATION_REPORT_TYPE}_{as_of.isoformat()}.json"
    )


def build_paperbroker_fill_model_calibration_payload(
    *,
    as_of: date,
    reports_dir: Path = REPO_ROOT / "outputs" / "reports",
    policy_path: Path = DEFAULT_PAPERBROKER_FILL_MODEL_CALIBRATION_POLICY_PATH,
    max_comparisons: int | None = None,
    replay_json_path: Path | None = None,
    paper_signal_quality_json_path: Path | None = None,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
) -> dict[str, Any]:
    policy = _load_policy(policy_path)
    thresholds = _mapping(policy["thresholds"])
    comparison_limit = (
        max_comparisons
        if max_comparisons is not None
        else _int_value(thresholds.get("maximum_recent_comparison_reports"), default=30)
    )
    if comparison_limit <= 0:
        raise ValueError("max_comparisons must be positive")

    output_json_path = output_json_path or default_paperbroker_fill_model_calibration_json_path(
        reports_dir,
        as_of,
    )
    output_md_path = output_md_path or output_json_path.with_suffix(".md")
    comparison_records = _select_recent_comparison_records(
        reports_dir=reports_dir,
        as_of=as_of,
        limit=comparison_limit,
    )
    controlled_fill_records = _select_recent_controlled_fill_records(
        reports_dir=reports_dir,
        as_of=as_of,
        limit=comparison_limit,
    )
    replay_payload, replay_path = _select_replay_payload(
        reports_dir=reports_dir,
        as_of=as_of,
        replay_json_path=replay_json_path,
    )
    signal_payload, signal_path = _select_paper_signal_quality_payload(
        reports_dir=reports_dir,
        as_of=as_of,
        paper_signal_quality_json_path=paper_signal_quality_json_path,
    )
    summary = _calibration_summary(
        comparison_records=comparison_records,
        controlled_fill_records=controlled_fill_records,
        replay_payload=replay_payload,
        paper_signal_quality_payload=signal_payload,
    )
    gate = _calibration_gate(summary=summary, policy=policy)
    recommendations = _recommendations(gate["status"])
    policy_report = _policy_report(policy, policy_path)
    payload = {
        "schema_version": PAPERBROKER_FILL_MODEL_CALIBRATION_SCHEMA_VERSION,
        "report_type": PAPERBROKER_FILL_MODEL_CALIBRATION_REPORT_TYPE,
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "as_of": as_of.isoformat(),
        "market_regime": "ai_after_chatgpt",
        "production_effect": PRODUCTION_EFFECT,
        "calibration_mode": CALIBRATION_MODE,
        "calibration_status": gate["status"],
        "fill_tested": summary["fill_tested"],
        "policy_id": policy_report["policy_id"],
        "policy_version": policy_report["version"],
        "thresholds_snapshot": policy_report["thresholds"],
        "policy": policy_report,
        "comparison_limit": comparison_limit,
        "evaluation_scope": {
            "diagnostic_only": True,
            "production_effect": PRODUCTION_EFFECT,
            "changes_paperbroker_fill_model": False,
            "changes_replay": False,
            "changes_paper_signal_quality": False,
            "changes_shadow_impact": False,
            "changes_production_conclusion": False,
            "changes_trade_execution": False,
            "changes_parameter_promotion": False,
        },
        "safety_boundary": {
            "reads_broker_api_key": False,
            "calls_ibkr": False,
            "calls_real_broker": False,
            "runs_paper_runner": False,
            "runs_replay": False,
            "changes_paperbroker_fill_model": False,
            "changes_replay": False,
            "changes_paper_signal_quality": False,
            "changes_shadow_impact": False,
            "changes_production_conclusion": False,
            "changes_trade_execution": False,
            "changes_parameter_promotion": False,
        },
        "production_surface_impact": {
            "replay": "none",
            "paper_signal_quality": "none",
            "shadow_impact": "none",
            "production_conclusion": "none",
            "parameter_promotion": "none",
            "trading_advice": "none",
        },
        "outputs": {
            "json": str(output_json_path),
            "markdown": str(output_md_path),
        },
        "source_artifacts": {
            "reports_dir": str(reports_dir),
            "policy_path": str(policy_path),
            "comparisons": [
                _comparison_source_artifact(path, comparison, reports_dir)
                for path, comparison in comparison_records
            ],
            "controlled_fills": [
                _controlled_fill_source_artifact(path, controlled_fill, reports_dir)
                for path, controlled_fill in controlled_fill_records
            ],
            "replay_quality": _replay_source_artifact(
                replay_path,
                replay_payload,
                reports_dir,
                provided=replay_json_path is not None,
            ),
            "paper_signal_quality": _paper_signal_quality_source_artifact(
                signal_path,
                signal_payload,
                reports_dir,
                provided=paper_signal_quality_json_path is not None,
            ),
        },
        "summary": summary,
        "calibration_gate": gate,
        "recommendations": recommendations,
        "notes": [
            "本报告只诊断 PaperBroker fill model calibration，不修改 fill model。",
            "no-fill lifecycle 对齐不代表 fill model 已验证。",
            (
                "报告不会读取 broker API key、不会调用 IBKR / broker、"
                "不会触发 paper runner 或 replay。"
            ),
        ],
    }
    _assert_allowed_status(payload["calibration_status"])
    return payload


def write_paperbroker_fill_model_calibration_report(
    *,
    as_of: date,
    reports_dir: Path = REPO_ROOT / "outputs" / "reports",
    policy_path: Path = DEFAULT_PAPERBROKER_FILL_MODEL_CALIBRATION_POLICY_PATH,
    max_comparisons: int | None = None,
    replay_json_path: Path | None = None,
    paper_signal_quality_json_path: Path | None = None,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
) -> dict[str, Any]:
    payload = build_paperbroker_fill_model_calibration_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=policy_path,
        max_comparisons=max_comparisons,
        replay_json_path=replay_json_path,
        paper_signal_quality_json_path=paper_signal_quality_json_path,
        output_json_path=output_json_path,
        output_md_path=output_md_path,
    )
    outputs = _mapping(payload.get("outputs"))
    json_path = Path(str(outputs["json"]))
    md_path = Path(str(outputs["markdown"]))
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(render_paperbroker_fill_model_calibration_report(payload), encoding="utf-8")
    return payload


def render_paperbroker_fill_model_calibration_report(payload: dict[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    gate = _mapping(payload.get("calibration_gate"))
    thresholds = _mapping(payload.get("thresholds_snapshot"))
    source_artifacts = _mapping(payload.get("source_artifacts"))
    replay_source = _mapping(source_artifacts.get("replay_quality"))
    signal_source = _mapping(source_artifacts.get("paper_signal_quality"))
    lines = [
        "# PaperBroker Fill Model Calibration Diagnostics",
        "",
        f"- 评估日期：{payload.get('as_of')}",
        "- 市场阶段：`ai_after_chatgpt`",
        f"- calibration_status：{payload.get('calibration_status')}",
        f"- calibration_mode={payload.get('calibration_mode')}",
        f"- production_effect={payload.get('production_effect')}",
        f"- fill_tested={str(payload.get('fill_tested')).lower()}",
        f"- Policy：{payload.get('policy_id')} v{payload.get('policy_version')}",
        (
            "- 安全边界：只读本地 comparison / replay quality / paper signal quality artifact；"
            "不读取 broker API key；不调用 IBKR 或 broker；不触发 paper runner / replay；"
            "不修改 PaperBroker fill model；不影响 production conclusion、paper signal quality、"
            "shadow impact、参数晋级或交易建议。"
        ),
        "",
        "## Policy Thresholds",
        "",
        "| Threshold | Value |",
        "|---|---:|",
        (
            "| maximum_recent_comparison_reports | "
            f"{thresholds.get('maximum_recent_comparison_reports', 'missing')} |"
        ),
        f"| minimum_comparison_count | {thresholds.get('minimum_comparison_count', 'missing')} |",
        "",
        "## 摘要",
        "",
        "| 指标 | 数值 |",
        "|---|---:|",
        f"| comparison_count | {summary.get('comparison_count', 0)} |",
        f"| controlled_fill_count | {summary.get('controlled_fill_count', 0)} |",
        f"| calibration_evidence_count | {summary.get('calibration_evidence_count', 0)} |",
        f"| lifecycle_match_count | {summary.get('lifecycle_match_count', 0)} |",
        (
            "| lifecycle_match_ratio | "
            f"{_format_percent(_optional_float(summary.get('lifecycle_match_ratio')))} |"
        ),
        (
            "| status_match_ratio | "
            f"{_format_percent(_optional_float(summary.get('status_match_ratio')))} |"
        ),
        (
            "| fill_match_ratio | "
            f"{_format_percent(_optional_float(summary.get('fill_match_ratio')))} |"
        ),
        (
            "| cancel_match_ratio | "
            f"{_format_percent(_optional_float(summary.get('cancel_match_ratio')))} |"
        ),
        (
            "| local_filled_but_ibkr_not_filled_count | "
            f"{summary.get('local_filled_but_ibkr_not_filled_count', 0)} |"
        ),
        (
            "| ibkr_rejected_but_local_accepted_count | "
            f"{summary.get('ibkr_rejected_but_local_accepted_count', 0)} |"
        ),
        f"| broker_rejected_count | {summary.get('broker_rejected_count', 0)} |",
        (
            "| insufficient_market_data_count | "
            f"{summary.get('insufficient_market_data_count', 0)} |"
        ),
        (
            "| synthetic_snapshot_related_count | "
            f"{summary.get('synthetic_snapshot_related_count', 0)} |"
        ),
        f"| no_fill_lifecycle_only_count | {summary.get('no_fill_lifecycle_only_count', 0)} |",
        (
            "| controlled_fill_no_fill_lifecycle_validated_count | "
            f"{summary.get('controlled_fill_no_fill_lifecycle_validated_count', 0)} |"
        ),
        (
            "| no_fill_lifecycle_validated_count | "
            f"{summary.get('no_fill_lifecycle_validated_count', 0)} |"
        ),
        "",
        "## Calibration Gate",
        "",
        f"- Gate status：{gate.get('status', STATUS_INSUFFICIENT_SAMPLE)}",
        f"- Explanation：{gate.get('explanation', '')}",
        f"- Blocking reasons：{', '.join(_strings(gate.get('blocking_reasons'))) or 'none'}",
        "",
        "| Check | Status | Observed | Threshold | Reason |",
        "|---|---|---:|---:|---|",
    ]
    for check in _records(gate.get("checks")):
        lines.append(
            "| "
            f"{check.get('check_id')} | "
            f"{check.get('status')} | "
            f"{_format_check_value(check.get('observed'))} | "
            f"{_format_check_value(check.get('threshold'))} | "
            f"{check.get('reason_code') or 'none'} |"
        )
    lines.extend(
        [
            "",
            "## Context Artifacts",
            "",
            f"- comparison artifacts：{len(_records(source_artifacts.get('comparisons')))}",
            (
                "- controlled fill artifacts："
                f"{len(_records(source_artifacts.get('controlled_fills')))}"
            ),
            (
                "- controlled fill classifications："
                f"{_json_inline(summary.get('controlled_fill_classification_counts'))}"
            ),
            f"- replay quality：{replay_source.get('path') or 'missing'}",
            f"- replay quality flags：{_json_inline(replay_source.get('quality_flags'))}",
            f"- paper_signal_quality：{signal_source.get('path') or 'missing'}",
            f"- paper_signal_quality status：{signal_source.get('evaluation_status') or 'missing'}",
            "",
            "## Recommendations",
            "",
            *[f"- {recommendation}" for recommendation in _strings(payload.get("recommendations"))],
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def _calibration_summary(
    *,
    comparison_records: list[tuple[Path, dict[str, Any]]],
    controlled_fill_records: list[tuple[Path, dict[str, Any]]],
    replay_payload: dict[str, Any],
    paper_signal_quality_payload: dict[str, Any],
) -> dict[str, Any]:
    comparison_count = len(comparison_records)
    controlled_fill_count = len(controlled_fill_records)
    calibration_evidence_count = comparison_count + controlled_fill_count
    lifecycle_match_count = 0
    status_match_count = 0
    fill_match_count = 0
    cancel_match_count = 0
    local_optimistic_count = 0
    rejection_gap_count = 0
    broker_rejected_count = 0
    insufficient_market_data_count = 0
    synthetic_snapshot_related_count = 0
    no_fill_lifecycle_only_count = 0
    controlled_fill_no_fill_lifecycle_validated_count = 0
    controlled_fill_fill_seen_count = 0
    fill_tested = False
    comparison_status_counts: dict[str, int] = {}
    difference_label_counts: dict[str, int] = {}
    controlled_fill_status_counts: dict[str, int] = {}
    controlled_fill_classification_counts: dict[str, int] = {}

    for _path, comparison in comparison_records:
        diff = _mapping(comparison.get("diff"))
        local = _mapping(comparison.get("local"))
        ibkr = _mapping(comparison.get("ibkr"))
        labels = _strings(comparison.get("difference_labels"))
        comparison_status = _string_value(comparison.get("comparison_status")) or "missing"
        comparison_status_counts[comparison_status] = (
            comparison_status_counts.get(comparison_status, 0) + 1
        )
        for label in labels:
            difference_label_counts[label] = difference_label_counts.get(label, 0) + 1

        status_match = _bool_value(diff.get("status_match"))
        fill_match = _bool_value(diff.get("fill_match"))
        cancel_match = _bool_value(diff.get("cancel_match"))
        status_match_count += int(status_match)
        fill_match_count += int(fill_match)
        cancel_match_count += int(cancel_match)
        lifecycle_match = status_match and fill_match and cancel_match
        lifecycle_match_count += int(lifecycle_match)

        local_fill_seen = _local_fill_seen(local)
        ibkr_fill_seen = _ibkr_fill_seen(ibkr)
        if local_fill_seen or ibkr_fill_seen:
            fill_tested = True
        local_optimistic = _bool_value(diff.get("local_filled_but_ibkr_not_filled"))
        rejection_gap = _bool_value(diff.get("ibkr_rejected_but_local_accepted"))
        broker_rejected = _broker_rejected(ibkr, labels)
        insufficient_market_data = (
            "INSUFFICIENT_MARKET_DATA" in labels
            or diff.get("ibkr_reference_price_available") is False
        )
        synthetic_related = _comparison_uses_synthetic_snapshot(local, diff)

        local_optimistic_count += int(local_optimistic)
        rejection_gap_count += int(rejection_gap)
        broker_rejected_count += int(broker_rejected)
        insufficient_market_data_count += int(insufficient_market_data)
        synthetic_snapshot_related_count += int(synthetic_related)
        no_fill_lifecycle_only_count += int(
            lifecycle_match and not local_fill_seen and not ibkr_fill_seen
        )

    for _path, controlled_fill in controlled_fill_records:
        test_status = _string_value(controlled_fill.get("test_status")) or "missing"
        controlled_fill_status_counts[test_status] = (
            controlled_fill_status_counts.get(test_status, 0) + 1
        )
        classification = _controlled_fill_classification(controlled_fill)
        controlled_fill_classification_counts[classification] = (
            controlled_fill_classification_counts.get(classification, 0) + 1
        )
        controlled_fill_seen = _bool_value(controlled_fill.get("fill_seen"))
        if controlled_fill_seen:
            fill_tested = True
            controlled_fill_fill_seen_count += 1
        controlled_fill_no_fill_lifecycle_validated_count += int(
            classification == CONTROLLED_FILL_NO_FILL_CLASSIFICATION
        )

    no_fill_lifecycle_validated_count = (
        no_fill_lifecycle_only_count + controlled_fill_no_fill_lifecycle_validated_count
    )
    replay_quality_flags = _mapping(replay_payload.get("quality_flags"))
    paper_signal_summary = _mapping(paper_signal_quality_payload.get("summary"))
    return {
        "comparison_count": comparison_count,
        "controlled_fill_count": controlled_fill_count,
        "calibration_evidence_count": calibration_evidence_count,
        "lifecycle_match_count": lifecycle_match_count,
        "lifecycle_match_ratio": _ratio(lifecycle_match_count, comparison_count),
        "status_match_count": status_match_count,
        "fill_match_count": fill_match_count,
        "cancel_match_count": cancel_match_count,
        "status_match_ratio": _ratio(status_match_count, comparison_count),
        "fill_match_ratio": _ratio(fill_match_count, comparison_count),
        "cancel_match_ratio": _ratio(cancel_match_count, comparison_count),
        "local_filled_but_ibkr_not_filled_count": local_optimistic_count,
        "ibkr_rejected_but_local_accepted_count": rejection_gap_count,
        "broker_rejected_count": broker_rejected_count,
        "insufficient_market_data_count": insufficient_market_data_count,
        "synthetic_snapshot_related_count": synthetic_snapshot_related_count,
        "no_fill_lifecycle_only_count": no_fill_lifecycle_only_count,
        "controlled_fill_no_fill_lifecycle_validated_count": (
            controlled_fill_no_fill_lifecycle_validated_count
        ),
        "controlled_fill_fill_seen_count": controlled_fill_fill_seen_count,
        "no_fill_lifecycle_validated_count": no_fill_lifecycle_validated_count,
        "fill_tested": fill_tested,
        "comparison_status_counts": dict(sorted(comparison_status_counts.items())),
        "difference_label_counts": dict(sorted(difference_label_counts.items())),
        "controlled_fill_status_counts": dict(sorted(controlled_fill_status_counts.items())),
        "controlled_fill_classification_counts": dict(
            sorted(controlled_fill_classification_counts.items())
        ),
        "replay_quality_flags": dict(replay_quality_flags),
        "paper_signal_quality_status": _string_value(
            paper_signal_quality_payload.get("evaluation_status")
        ),
        "paper_signal_quality_summary": dict(paper_signal_summary),
    }


def _calibration_gate(*, summary: dict[str, Any], policy: dict[str, Any]) -> dict[str, Any]:
    thresholds = _mapping(policy["thresholds"])
    evidence_count = _int_value(summary.get("calibration_evidence_count"))
    minimum_count = _int_value(thresholds.get("minimum_comparison_count"), default=1)
    checks = [
        {
            "check_id": "minimum_evidence_count",
            "status": "PASS" if evidence_count >= minimum_count else "FAIL",
            "observed": evidence_count,
            "threshold": minimum_count,
            "operator": ">=",
            "reason_code": "" if evidence_count >= minimum_count else "insufficient_sample",
        },
        {
            "check_id": "fill_tested",
            "status": "PASS" if summary.get("fill_tested") else "UNTESTED",
            "observed": bool(summary.get("fill_tested")),
            "threshold": True,
            "operator": "==",
            "reason_code": "" if summary.get("fill_tested") else "no_fill_lifecycle_only",
        },
    ]
    if evidence_count < minimum_count:
        return _gate(
            status=STATUS_INSUFFICIENT_SAMPLE,
            reason="insufficient_sample",
            checks=checks,
        )
    if _int_value(summary.get("local_filled_but_ibkr_not_filled_count")) > 0:
        return _gate(
            status=STATUS_LOCAL_SIM_TOO_OPTIMISTIC,
            reason="local_sim_too_optimistic",
            checks=checks,
        )
    if _int_value(summary.get("ibkr_rejected_but_local_accepted_count")) > 0:
        return _gate(
            status=STATUS_BROKER_REJECTION_GAP,
            reason="broker_rejection_gap",
            checks=checks,
        )
    no_fill_lifecycle = (
        evidence_count > 0
        and not summary.get("fill_tested")
        and (_int_value(summary.get("no_fill_lifecycle_validated_count")) == evidence_count)
    )
    if no_fill_lifecycle:
        return _gate(
            status=STATUS_LIFECYCLE_ALIGNED_FILL_UNTESTED,
            reason="no_fill_lifecycle_only",
            checks=checks,
        )
    return _gate(status=STATUS_OBSERVE_ONLY, reason="observe_only", checks=checks)


def _gate(*, status: str, reason: str, checks: list[dict[str, Any]]) -> dict[str, Any]:
    blocking = (
        [] if status in {STATUS_LIFECYCLE_ALIGNED_FILL_UNTESTED, STATUS_OBSERVE_ONLY} else [reason]
    )
    return {
        "status": status,
        "blocked": bool(blocking),
        "blocking_reasons": blocking,
        "reason_code": reason,
        "reason_explanations": {reason: REASON_EXPLANATIONS[reason]},
        "explanation": f"{status}：{REASON_EXPLANATIONS[reason]}",
        "checks": checks,
        "production_effect": PRODUCTION_EFFECT,
    }


def _recommendations(status: str) -> list[str]:
    if status == STATUS_LIFECYCLE_ALIGNED_FILL_UNTESTED:
        return list(NO_FILL_LIFECYCLE_RECOMMENDATIONS)
    if status == STATUS_LOCAL_SIM_TOO_OPTIMISTIC:
        return [
            "investigate local fill simulation against near-market IBKR Paper samples",
            "review whether synthetic snapshots are too optimistic",
            "collect repeated controlled small-fill samples before any model change",
            "do not modify PaperBroker fill model yet",
        ]
    if status == STATUS_BROKER_REJECTION_GAP:
        return [
            "map IBKR Paper rejection reasons before changing fill assumptions",
            "compare local pre-trade validation with broker rejection behavior",
            "collect more paper-only rejection samples",
            "do not modify PaperBroker fill model yet",
        ]
    if status == STATUS_INSUFFICIENT_SAMPLE:
        return [
            "collect PaperBroker vs IBKR Paper comparison samples",
            "include lifecycle-only and near-market controlled-fill cases",
            "do not modify PaperBroker fill model yet",
        ]
    return [
        "continue observing comparison samples",
        "separate lifecycle alignment from fill model validation",
        "do not modify PaperBroker fill model yet",
    ]


def _select_recent_comparison_records(
    *,
    reports_dir: Path,
    as_of: date,
    limit: int,
) -> list[tuple[Path, dict[str, Any]]]:
    candidates: list[tuple[date, datetime, str, Path, dict[str, Any]]] = []
    for path in reports_dir.glob(f"{COMPARISON_REPORT_TYPE}_*.json"):
        payload = _read_json_object(path)
        if payload.get("report_type") != COMPARISON_REPORT_TYPE:
            continue
        sample_date = _payload_date(payload, path)
        if sample_date is None or sample_date > as_of:
            continue
        generated_at = _parse_iso_datetime(_string_value(payload.get("generated_at")))
        candidates.append((sample_date, generated_at, path.name, path, payload))
    return [
        (path, payload)
        for _day, _generated, _name, path, payload in sorted(candidates, reverse=True)[:limit]
    ]


def _select_recent_controlled_fill_records(
    *,
    reports_dir: Path,
    as_of: date,
    limit: int,
) -> list[tuple[Path, dict[str, Any]]]:
    candidates: list[tuple[date, datetime, str, Path, dict[str, Any]]] = []
    for path in reports_dir.glob(f"{CONTROLLED_FILL_REPORT_TYPE}_*.json"):
        payload = _read_json_object(path)
        if payload.get("report_type") != CONTROLLED_FILL_REPORT_TYPE:
            continue
        sample_date = _payload_date(payload, path)
        if sample_date is None or sample_date > as_of:
            continue
        generated_at = _parse_iso_datetime(_string_value(payload.get("generated_at")))
        candidates.append((sample_date, generated_at, path.name, path, payload))
    return [
        (path, payload)
        for _day, _generated, _name, path, payload in sorted(candidates, reverse=True)[:limit]
    ]


def _select_replay_payload(
    *,
    reports_dir: Path,
    as_of: date,
    replay_json_path: Path | None,
) -> tuple[dict[str, Any], Path | None]:
    if replay_json_path is not None:
        return _read_json_object(replay_json_path), replay_json_path
    candidates: list[tuple[date, datetime, str, Path, dict[str, Any]]] = []
    for path in reports_dir.glob("paper_trading_replay_*.json"):
        payload = _read_json_object(path)
        if payload.get("report_type") != "paper_trading_replay":
            continue
        end_date = _parse_iso_date(_string_value(payload.get("end")))
        if end_date is None or end_date > as_of:
            continue
        generated_at = _parse_iso_datetime(_string_value(payload.get("generated_at")))
        candidates.append((end_date, generated_at, path.name, path, payload))
    if not candidates:
        return {}, None
    _end_date, _generated_at, _name, path, payload = max(candidates)
    return payload, path


def _select_paper_signal_quality_payload(
    *,
    reports_dir: Path,
    as_of: date,
    paper_signal_quality_json_path: Path | None,
) -> tuple[dict[str, Any], Path | None]:
    if paper_signal_quality_json_path is not None:
        return _read_json_object(paper_signal_quality_json_path), paper_signal_quality_json_path
    same_day = reports_dir / f"paper_signal_quality_{as_of.isoformat()}.json"
    same_day_payload = _read_json_object(same_day)
    if same_day_payload.get("report_type") == "paper_signal_quality":
        return same_day_payload, same_day
    candidates: list[tuple[date, datetime, str, Path, dict[str, Any]]] = []
    for path in reports_dir.glob("paper_signal_quality_*.json"):
        payload = _read_json_object(path)
        if payload.get("report_type") != "paper_signal_quality":
            continue
        sample_date = _payload_date(payload, path)
        if sample_date is None or sample_date > as_of:
            continue
        generated_at = _parse_iso_datetime(_string_value(payload.get("generated_at")))
        candidates.append((sample_date, generated_at, path.name, path, payload))
    if not candidates:
        return {}, None
    _sample_date, _generated_at, _name, path, payload = max(candidates)
    return payload, path


def _comparison_source_artifact(
    path: Path,
    payload: dict[str, Any],
    reports_dir: Path,
) -> dict[str, Any]:
    diff = _mapping(payload.get("diff"))
    ibkr = _mapping(payload.get("ibkr"))
    return {
        "path": str(path),
        "href": _report_href(path, reports_dir),
        "exists": True,
        "as_of": _string_value(payload.get("as_of"))
        or _string_value(payload.get("source_run_date")),
        "comparison_status": _string_value(payload.get("comparison_status")),
        "difference_labels": _strings(payload.get("difference_labels")),
        "status_match": _bool_value(diff.get("status_match")),
        "fill_match": _bool_value(diff.get("fill_match")),
        "cancel_match": _bool_value(diff.get("cancel_match")),
        "fills_seen": _ibkr_fill_seen(ibkr),
    }


def _controlled_fill_source_artifact(
    path: Path,
    payload: dict[str, Any],
    reports_dir: Path,
) -> dict[str, Any]:
    return {
        "path": str(path),
        "href": _report_href(path, reports_dir),
        "exists": True,
        "as_of": _string_value(payload.get("as_of"))
        or _string_value(payload.get("source_run_date")),
        "test_status": _string_value(payload.get("test_status")),
        "classification": _controlled_fill_classification(payload),
        "fill_seen": _bool_value(payload.get("fill_seen")),
        "fill_quantity": _optional_float(payload.get("fill_quantity")),
        "cancel_requested": _bool_value(payload.get("cancel_requested")),
        "final_order_status": _string_value(payload.get("final_order_status")),
        "issue_codes": [
            _string_value(issue.get("code"))
            for issue in _records(payload.get("issues"))
            if _string_value(issue.get("code"))
        ],
        "production_effect": _string_value(payload.get("production_effect")),
    }


def _replay_source_artifact(
    path: Path | None,
    payload: dict[str, Any],
    reports_dir: Path,
    *,
    provided: bool,
) -> dict[str, Any]:
    return {
        "provided": provided,
        "exists": bool(
            path and path.exists() and payload.get("report_type") == "paper_trading_replay"
        ),
        "path": "" if path is None else str(path),
        "href": "" if path is None else _report_href(path, reports_dir),
        "report_type": _string_value(payload.get("report_type")),
        "start": _string_value(payload.get("start")),
        "end": _string_value(payload.get("end")),
        "replay_mode": _string_value(payload.get("replay_mode")),
        "portfolio_carry_forward": _bool_value(payload.get("portfolio_carry_forward")),
        "quality_flags": dict(_mapping(payload.get("quality_flags"))),
    }


def _paper_signal_quality_source_artifact(
    path: Path | None,
    payload: dict[str, Any],
    reports_dir: Path,
    *,
    provided: bool,
) -> dict[str, Any]:
    return {
        "provided": provided,
        "exists": bool(
            path and path.exists() and payload.get("report_type") == "paper_signal_quality"
        ),
        "path": "" if path is None else str(path),
        "href": "" if path is None else _report_href(path, reports_dir),
        "report_type": _string_value(payload.get("report_type")),
        "as_of": _string_value(payload.get("as_of")),
        "evaluation_status": _string_value(payload.get("evaluation_status")),
        "summary": dict(_mapping(payload.get("summary"))),
    }


def _load_policy(path: Path) -> dict[str, Any]:
    try:
        raw = safe_load_yaml_path(path)
    except (OSError, yaml.YAMLError) as exc:
        raise ValueError(
            f"failed to read paperbroker fill model calibration policy: {path}"
        ) from exc
    if not isinstance(raw, dict):
        raise ValueError(
            f"paperbroker fill model calibration policy must be a YAML mapping: {path}"
        )
    thresholds = raw.get("thresholds")
    if not isinstance(thresholds, dict):
        raise ValueError("paperbroker fill model calibration policy missing thresholds")
    for key in ("maximum_recent_comparison_reports", "minimum_comparison_count"):
        if key not in thresholds:
            raise ValueError(f"paperbroker fill model calibration policy missing threshold: {key}")
    allowed_statuses = set(_strings(raw.get("allowed_statuses")))
    if allowed_statuses != ALLOWED_CALIBRATION_STATUSES:
        raise ValueError("paperbroker fill model calibration policy allowed_statuses mismatch")
    return raw


def _policy_report(policy: dict[str, Any], policy_path: Path) -> dict[str, Any]:
    return {
        "policy_id": _string_value(policy.get("policy_id"))
        or "paperbroker_fill_model_calibration_policy",
        "version": policy.get("version"),
        "status": _string_value(policy.get("status")),
        "owner": _string_value(policy.get("owner")),
        "production_effect": _string_value(policy.get("production_effect")) or PRODUCTION_EFFECT,
        "calibration_mode": _string_value(policy.get("calibration_mode")) or CALIBRATION_MODE,
        "path": str(policy_path),
        "thresholds": dict(_mapping(policy.get("thresholds"))),
        "review_condition": _string_value(policy.get("review_condition")),
    }


def _read_json_object(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _payload_date(payload: dict[str, Any], path: Path) -> date | None:
    for key in ("as_of", "source_run_date", "date", "end"):
        parsed = _parse_iso_date(_string_value(payload.get(key)))
        if parsed is not None:
            return parsed
    stem_suffix = path.stem.rsplit("_", 1)[-1]
    return _parse_iso_date(stem_suffix)


def _local_fill_seen(local: dict[str, Any]) -> bool:
    return (
        _bool_value(local.get("local_fill_seen"))
        or _normalize_status(_string_value(local.get("local_final_status"))) == "FILLED"
    )


def _ibkr_fill_seen(ibkr: dict[str, Any]) -> bool:
    return (
        _bool_value(ibkr.get("fills_seen"))
        or _normalize_status(_string_value(ibkr.get("final_status"))) == "FILLED"
    )


def _broker_rejected(ibkr: dict[str, Any], labels: Iterable[str]) -> bool:
    return "BROKER_REJECTED" in set(labels) or _normalize_status(
        _string_value(ibkr.get("final_status"))
    ) in {"REJECTED", "INACTIVE"}


def _controlled_fill_classification(payload: dict[str, Any]) -> str:
    if _bool_value(payload.get("fill_seen")):
        return "FILL_OBSERVED"
    final_status = _normalize_status(_string_value(payload.get("final_order_status")))
    if final_status == "CANCELLED" and _bool_value(payload.get("cancel_requested")):
        return CONTROLLED_FILL_NO_FILL_CLASSIFICATION
    return "CONTROLLED_FILL_INCONCLUSIVE"


def _comparison_uses_synthetic_snapshot(local: dict[str, Any], diff: dict[str, Any]) -> bool:
    fields = (
        _string_value(local.get("local_price_source")),
        _string_value(diff.get("local_price_source")),
    )
    return any("synthetic" in field.lower() for field in fields)


def _normalize_status(status: str) -> str:
    normalized = status.strip().upper()
    if normalized in {"APICANCELLED", "CANCELLED"}:
        return "CANCELLED"
    if normalized in {"REJECTED", "INACTIVE"}:
        return "REJECTED"
    if normalized in {"PRESUBMITTED", "PENDINGSUBMIT", "SUBMITTED", "APIPENDING"}:
        return "OPEN"
    if normalized == "FILLED":
        return "FILLED"
    return normalized or "UNKNOWN"


def _assert_allowed_status(status: object) -> None:
    if status not in ALLOWED_CALIBRATION_STATUSES:
        raise ValueError(f"unsupported calibration_status: {status}")


def _report_href(path: Path, reports_dir: Path) -> str:
    try:
        return path.relative_to(reports_dir).as_posix()
    except ValueError:
        try:
            return path.resolve().relative_to(reports_dir.resolve()).as_posix()
        except (OSError, RuntimeError, ValueError):
            return path.as_posix()


def _ratio(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator else 0.0


def _records(value: object) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, dict))


def _mapping(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _strings(value: object) -> list[str]:
    if isinstance(value, str):
        return [value] if value else []
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if str(item)]


def _string_value(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _int_value(value: object, *, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _optional_float(value: object) -> float | None:
    if isinstance(value, bool):
        return float(value)
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _bool_value(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return bool(value)


def _parse_iso_date(value: str) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _parse_iso_datetime(value: str) -> datetime:
    if not value:
        return datetime.min.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.min.replace(tzinfo=UTC)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _format_percent(value: float | None) -> str:
    if value is None:
        return "missing"
    return f"{value:.2%}"


def _format_check_value(value: object) -> str:
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def _json_inline(value: object) -> str:
    if not value:
        return "none"
    return "`" + json.dumps(value, ensure_ascii=False, sort_keys=True) + "`"


__all__ = [
    "ALLOWED_CALIBRATION_STATUSES",
    "CALIBRATION_MODE",
    "CONTROLLED_FILL_NO_FILL_CLASSIFICATION",
    "DEFAULT_PAPERBROKER_FILL_MODEL_CALIBRATION_POLICY_PATH",
    "NO_FILL_LIFECYCLE_RECOMMENDATIONS",
    "PAPERBROKER_FILL_MODEL_CALIBRATION_REPORT_TYPE",
    "PRODUCTION_EFFECT",
    "build_paperbroker_fill_model_calibration_payload",
    "default_paperbroker_fill_model_calibration_json_path",
    "render_paperbroker_fill_model_calibration_report",
    "write_paperbroker_fill_model_calibration_report",
]
