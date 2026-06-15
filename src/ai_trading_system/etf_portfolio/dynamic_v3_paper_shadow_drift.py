from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_daily as daily
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st

DEFAULT_PAPER_SHADOW_DRIFT_MONITOR_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "paper_shadow_drift_monitor"
)
DRIFT_FAMILIES = (
    "unexpected_turnover_increase",
    "excessive_risk_off_frequency",
    "drawdown_mismatch_regression",
    "flip_rotation_regression",
    "benchmark_underperformance",
    "missing_signal_inputs",
)
DRIFT_SEVERITIES = ("NONE", "WATCH", "WARNING", "BLOCKING")
DRIFT_NEXT_ACTIONS = (
    "continue_shadow",
    "needs_manual_review",
    "return_to_research",
    "reject_candidate",
)

# TRADING-352 pilot reporting boundaries. These labels classify one
# paper-shadow observation for manual review only; they are not production
# trading thresholds, position sizing rules, or promotion gates.
WATCH_LABELS = ("watch", "risk_off", "underperform", "rotation", "mismatch")
WARNING_LABELS = (
    "warning",
    "regression",
    "worse",
    "lag",
    "churn",
    "whipsaw",
    "high_turnover",
    "turnover_spike",
    "excessive",
    "material",
)
BLOCKING_LABELS = ("blocking", "severe", "missing", "failed", "unsafe")
NO_DRIFT_LABELS = (
    "none",
    "normal",
    "stable",
    "aligned",
    "tracking",
    "risk_on",
    "no_mismatch",
    "no_rotation",
)

PAPER_SHADOW_DRIFT_SAFETY = {
    **st.SYSTEM_TARGET_SAFETY,
    "manual_review_only": True,
    "paper_shadow_drift_monitor_only": True,
    "observation_only": True,
    "read_only_monitor": True,
    "data_downloaded_by_monitor": False,
    "pipelines_executed_by_monitor": False,
    "paper_account_state_mutated": False,
    "broker_effect": "none",
    "order_effect": "none",
    "broker_action_allowed": False,
    "broker_action_taken": False,
    "order_ticket_generated": False,
    "production_state_mutated": False,
    "official_target_weights_mutated": False,
    "automatic_candidate_promotion": False,
    "auto_apply": False,
    "production_effect": "none",
}


def build_paper_shadow_drift_monitor_report(
    *,
    observation_id: str | None = None,
    observation_dir: Path = daily.DEFAULT_PAPER_SHADOW_DAILY_DIR,
    contract_id: str | None = None,
    contract_dir: Path = readiness.DEFAULT_FORMAL_RESEARCH_METHOD_CONTRACT_DIR,
    output_dir: Path = DEFAULT_PAPER_SHADOW_DRIFT_MONITOR_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    observation_payload = daily.paper_shadow_daily_report_payload(
        observation_id=observation_id,
        latest=observation_id is None,
        output_dir=observation_dir,
    )
    observation = _mapping(observation_payload.get("paper_shadow_daily_observation"))
    resolved_contract_id = contract_id or _text(observation.get("source_contract_id")) or None
    contract_payload = readiness.formal_research_method_contract_report_payload(
        contract_id=resolved_contract_id,
        latest=resolved_contract_id is None,
        output_dir=contract_dir,
    )
    contract = _mapping(contract_payload.get("formal_research_method_contract"))
    decision = _mapping(contract_payload.get("formal_research_method_decision"))
    findings = _drift_findings(
        observation_payload=observation_payload,
        observation=observation,
        contract=contract,
        decision=decision,
    )
    severity = _overall_drift_severity(findings)
    blocking_count = sum(1 for row in findings if row.get("severity") == "BLOCKING")
    warning_count = sum(1 for row in findings if row.get("severity") == "WARNING")
    watch_count = sum(1 for row in findings if row.get("severity") == "WATCH")
    next_action = _next_action(
        severity=severity,
        blocking_count=blocking_count,
        warning_count=warning_count,
        watch_count=watch_count,
    )
    monitor_id = st._stable_id(
        "paper-shadow-drift-monitor",
        _text(observation_payload.get("observation_id")),
        _text(contract_payload.get("contract_id")),
        severity,
        generated.isoformat(),
    )
    root = st._unique_dir(output_dir / monitor_id)
    root.mkdir(parents=True, exist_ok=False)
    source_artifacts = _source_artifacts(
        observation_payload=observation_payload,
        contract_payload=contract_payload,
    )
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_drift_report",
        "monitor_id": root.name,
        "candidate": _text(observation.get("candidate")),
        "generated_at": generated.isoformat(),
        "observation_id": observation_payload.get("observation_id"),
        "observation_date": observation.get("observation_date"),
        "observation_status": observation.get("observation_status"),
        "source_contract_id": contract_payload.get("contract_id"),
        "source_contract_promotion_state": decision.get("promotion_state"),
        "source_contract_status": decision.get("formal_research_method_status"),
        "source_artifacts": source_artifacts,
        "drift_severity": severity,
        "finding_count": len(findings),
        "watch_count": watch_count,
        "warning_count": warning_count,
        "blocking_count": blocking_count,
        "next_action": next_action,
        "findings": findings,
        "limitations": [
            "single-observation label-based drift screen",
            "does not recompute historical validation evidence",
            "does not approve, reject, size, or execute any portfolio action",
        ],
        **PAPER_SHADOW_DRIFT_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_drift_manifest",
        "monitor_id": root.name,
        "candidate": report.get("candidate"),
        "generated_at": generated.isoformat(),
        "status": "PASS" if severity != "BLOCKING" else "BLOCKING",
        "drift_severity": severity,
        "observation_id": observation_payload.get("observation_id"),
        "observation_date": observation.get("observation_date"),
        "source_contract_id": contract_payload.get("contract_id"),
        "paper_shadow_drift_manifest_path": str(root / "paper_shadow_drift_manifest.json"),
        "paper_shadow_drift_report_path": str(root / "paper_shadow_drift_report.json"),
        "paper_shadow_drift_findings_path": str(root / "paper_shadow_drift_findings.jsonl"),
        "paper_shadow_drift_markdown_path": str(root / "paper_shadow_drift_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        "validation_path": str(root / "paper_shadow_drift_validation.json"),
        **PAPER_SHADOW_DRIFT_SAFETY,
    }
    reader = render_paper_shadow_drift_reader_brief(report)
    st._write_json(root / "paper_shadow_drift_manifest.json", manifest)
    st._write_json(root / "paper_shadow_drift_report.json", report)
    st._write_jsonl(root / "paper_shadow_drift_findings.jsonl", findings)
    st._write_text(
        root / "paper_shadow_drift_report.md",
        render_paper_shadow_drift_report(manifest, report),
    )
    st._write_text(root / "reader_brief_section.md", reader)
    st._write_latest_pointer(
        "latest_paper_shadow_drift_monitor",
        root.name,
        root / "paper_shadow_drift_manifest.json",
    )
    validation = validate_paper_shadow_drift_monitor_artifact(
        monitor_id=root.name,
        output_dir=output_dir,
        write_output=True,
    )
    return {
        "monitor_id": root.name,
        "monitor_dir": root,
        "manifest": manifest,
        "paper_shadow_drift_report": report,
        "paper_shadow_drift_findings": findings,
        "reader_brief_section": reader,
        "paper_shadow_drift_validation": validation,
    }


def paper_shadow_drift_monitor_report_payload(
    *,
    monitor_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PAPER_SHADOW_DRIFT_MONITOR_DIR,
) -> dict[str, Any]:
    root = st._artifact_dir(
        artifact_id=monitor_id,
        latest_pointer="latest_paper_shadow_drift_monitor",
        latest=latest,
        output_dir=output_dir,
        required_name="paper_shadow_drift_manifest.json",
    )
    payload = {
        **st._read_json(root / "paper_shadow_drift_manifest.json"),
        "paper_shadow_drift_report": st._read_json(root / "paper_shadow_drift_report.json"),
        "paper_shadow_drift_findings": st._read_jsonl(
            root / "paper_shadow_drift_findings.jsonl"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(
            encoding="utf-8"
        ),
        "monitor_dir": str(root),
    }
    validation = st._read_optional_json(root / "paper_shadow_drift_validation.json")
    if validation:
        payload["paper_shadow_drift_validation"] = validation
    return payload


def validate_paper_shadow_drift_monitor_artifact(
    *,
    monitor_id: str,
    output_dir: Path = DEFAULT_PAPER_SHADOW_DRIFT_MONITOR_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / monitor_id
    manifest = st._read_optional_json(root / "paper_shadow_drift_manifest.json") or {}
    report = st._read_optional_json(root / "paper_shadow_drift_report.json") or {}
    findings = st._read_jsonl(root / "paper_shadow_drift_findings.jsonl")
    reader = (
        (root / "reader_brief_section.md").read_text(encoding="utf-8")
        if (root / "reader_brief_section.md").exists()
        else ""
    )
    finding_families = {row.get("family") for row in findings}
    checks = st._required_file_checks(
        root,
        (
            "paper_shadow_drift_manifest.json",
            "paper_shadow_drift_report.json",
            "paper_shadow_drift_findings.jsonl",
            "paper_shadow_drift_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check("monitor_id_matches", manifest.get("monitor_id") == monitor_id, ""),
            st._check("candidate_visible", bool(_text(report.get("candidate"))), ""),
            st._check(
                "source_observation_visible",
                bool(_text(report.get("observation_id"))),
                "",
            ),
            st._check(
                "source_contract_visible",
                bool(_text(report.get("source_contract_id"))),
                "",
            ),
            st._check(
                "drift_families_complete",
                set(DRIFT_FAMILIES).issubset(finding_families),
                ",".join(sorted(_texts(finding_families))),
            ),
            st._check(
                "finding_severities_valid",
                all(row.get("severity") in DRIFT_SEVERITIES for row in findings),
                "",
            ),
            st._check(
                "overall_severity_valid",
                report.get("drift_severity") in DRIFT_SEVERITIES,
                "",
            ),
            st._check("next_action_valid", report.get("next_action") in DRIFT_NEXT_ACTIONS, ""),
            st._check(
                "finding_counts_consistent",
                report.get("finding_count") == len(findings)
                and report.get("blocking_count")
                == sum(1 for row in findings if row.get("severity") == "BLOCKING")
                and report.get("warning_count")
                == sum(1 for row in findings if row.get("severity") == "WARNING"),
                "",
            ),
            st._check(
                "source_artifacts_visible",
                len(_records(report.get("source_artifacts"))) >= 2,
                "",
            ),
            st._check(
                "reader_brief_fields",
                "paper_shadow_drift_severity" in reader
                and "paper_shadow_drift_next_action" in reader,
                "",
            ),
            st._check(
                "monitor_read_only",
                report.get("read_only_monitor") is True
                and report.get("data_downloaded_by_monitor") is False
                and report.get("pipelines_executed_by_monitor") is False,
                "",
            ),
            st._check(
                "paper_account_not_mutated",
                report.get("paper_account_state_mutated") is False
                and manifest.get("paper_account_state_mutated") is False,
                "",
            ),
            st._check("broker_forbidden", st._payload_safe(manifest, report), ""),
        ]
    )
    validation = st._validation_payload(
        "etf_dynamic_v3_paper_shadow_drift_monitor_validation",
        monitor_id,
        checks,
    )
    if write_output:
        st._write_json(root / "paper_shadow_drift_validation.json", validation)
        st._write_text(
            root / "paper_shadow_drift_validation.md",
            render_paper_shadow_drift_validation_report(validation),
        )
    return validation


def render_paper_shadow_drift_reader_brief(report: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Paper Shadow Drift Monitor",
            "",
            f"- paper_shadow_drift_monitor_id: {report.get('monitor_id')}",
            f"- paper_shadow_drift_candidate: {report.get('candidate')}",
            f"- paper_shadow_drift_observation_id: {report.get('observation_id')}",
            f"- paper_shadow_drift_severity: {report.get('drift_severity')}",
            f"- paper_shadow_drift_blocking_count: {report.get('blocking_count')}",
            f"- paper_shadow_drift_warning_count: {report.get('warning_count')}",
            f"- paper_shadow_drift_next_action: {report.get('next_action')}",
            "- safety_boundary: read-only paper-shadow drift monitor / no data refresh / "
            "no official target / no broker / no production",
            "",
        ]
    )


def render_paper_shadow_drift_report(
    manifest: Mapping[str, Any],
    report: Mapping[str, Any],
) -> str:
    finding_lines = [
        f"- {row.get('family')}: severity={row.get('severity')} "
        f"current={row.get('current_value')} expected={row.get('historical_expectation')} "
        f"action={row.get('recommended_action')}"
        for row in _records(report.get("findings"))
    ]
    source_lines = [
        f"- {row.get('source_id')}: artifact_id={row.get('artifact_id')} "
        f"path={row.get('path')}"
        for row in _records(report.get("source_artifacts"))
    ]
    return "\n".join(
        [
            f"# Paper Shadow Drift Monitor {manifest.get('monitor_id')}",
            "",
            "## Purpose",
            "Compare the latest paper-shadow observation against the validated research behavior "
            "contract and surface manual-review drift signals.",
            "",
            "## Input Artifacts",
            *source_lines,
            "",
            "## Output Decision",
            f"- drift_severity: {report.get('drift_severity')}",
            f"- blocking_count: {report.get('blocking_count')}",
            f"- warning_count: {report.get('warning_count')}",
            f"- next_action: {report.get('next_action')}",
            "",
            "## Findings",
            *finding_lines,
            "",
            "## Safety Boundary",
            "- read-only monitor",
            "- no data refresh or upstream pipeline execution",
            "- no paper account mutation",
            "- no official target weights",
            "- no broker action or order ticket",
            "- no production mutation",
            "",
            "## Limitations",
            "- This first version is a single-observation label-based screen.",
            "- Historical validation evidence is read from the formal research method contract.",
            "- Severity labels are manual-review inputs and do not execute promotion or rejection.",
            "",
        ]
    )


def render_paper_shadow_drift_validation_report(validation: Mapping[str, Any]) -> str:
    check_lines = [
        f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}"
        for row in _records(validation.get("checks"))
    ]
    return "\n".join(
        [
            f"# Paper Shadow Drift Monitor Validation {validation.get('artifact_id')}",
            "",
            f"- status: {validation.get('status')}",
            f"- failed_check_count: {validation.get('failed_check_count')}",
            "- production_effect: none",
            "",
            "## Checks",
            *check_lines,
            "",
        ]
    )


def _drift_findings(
    *,
    observation_payload: Mapping[str, Any],
    observation: Mapping[str, Any],
    contract: Mapping[str, Any],
    decision: Mapping[str, Any],
) -> list[dict[str, Any]]:
    daily_review = _mapping(observation.get("daily_review"))
    gates = _objective_gate_map(_records(contract.get("objective_gates")))
    input_artifacts = _records(observation.get("input_artifacts"))
    return [
        _finding(
            family="unexpected_turnover_increase",
            current_value=_text(daily_review.get("rotation_event")),
            historical_expectation=_gate_expectation(
                gates,
                "flip_rotation_reduction",
                "validated flip/rotation reduction should avoid high-turnover churn",
            ),
            severity=_label_severity(_text(daily_review.get("rotation_event"))),
            recommended_action="review_turnover_path_before_continuing_shadow",
            source_gate=_mapping(gates.get("flip_rotation_reduction")),
        ),
        _finding(
            family="excessive_risk_off_frequency",
            current_value=_text(daily_review.get("risk_off_risk_on_state")),
            historical_expectation=_gate_expectation(
                gates,
                "stress_result",
                "validated stress strength should not drift into unexplained risk-off clusters",
            ),
            severity=_risk_off_severity(
                _text(daily_review.get("risk_off_risk_on_state")),
                _text(daily_review.get("signal_output")),
            ),
            recommended_action="review_risk_off_reason_before_continuing_shadow",
            source_gate=_mapping(gates.get("stress_result")),
        ),
        _finding(
            family="drawdown_mismatch_regression",
            current_value=_text(daily_review.get("mismatch_event")),
            historical_expectation=_gate_expectation(
                gates,
                "drawdown_mismatch_reduction",
                "validated drawdown mismatch reduction should not regress",
            ),
            severity=_label_severity(_text(daily_review.get("mismatch_event"))),
            recommended_action="review_drawdown_mismatch_before_continuing_shadow",
            source_gate=_mapping(gates.get("drawdown_mismatch_reduction")),
        ),
        _finding(
            family="flip_rotation_regression",
            current_value=_text(daily_review.get("rotation_event")),
            historical_expectation=_gate_expectation(
                gates,
                "flip_rotation_reduction",
                "validated flip/rotation reduction should avoid noisy reversals",
            ),
            severity=_label_severity(_text(daily_review.get("rotation_event"))),
            recommended_action="review_flip_rotation_before_continuing_shadow",
            source_gate=_mapping(gates.get("flip_rotation_reduction")),
        ),
        _finding(
            family="benchmark_underperformance",
            current_value=_text(daily_review.get("benchmark_comparison")),
            historical_expectation=_gate_expectation(
                gates,
                "ab_review",
                "validated A/B review should not show material benchmark lag",
            ),
            severity=_benchmark_severity(_text(daily_review.get("benchmark_comparison"))),
            recommended_action="review_benchmark_lag_before_continuing_shadow",
            source_gate=_mapping(gates.get("ab_review")),
        ),
        _finding(
            family="missing_signal_inputs",
            current_value=_missing_input_value(
                observation_payload=observation_payload,
                observation=observation,
                input_artifacts=input_artifacts,
            ),
            historical_expectation=(
                "market panel and signal artifacts must exist with checksums before "
                "paper-shadow behavior can be interpreted"
            ),
            severity=_missing_signal_input_severity(observation, input_artifacts),
            recommended_action="block_until_signal_inputs_are_complete",
            source_gate={
                "gate_id": "input_artifact_integrity",
                "actual_value": observation.get("observation_status"),
                "passed": observation.get("observation_status") == "RECORDED",
            },
        ),
    ]


def _finding(
    *,
    family: str,
    current_value: str,
    historical_expectation: str,
    severity: str,
    recommended_action: str,
    source_gate: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "family": family,
        "severity": severity,
        "current_value": current_value or "MISSING",
        "historical_expectation": historical_expectation,
        "source_gate_id": source_gate.get("gate_id"),
        "source_gate_actual_value": source_gate.get("actual_value"),
        "source_gate_passed": source_gate.get("passed") is True,
        "recommended_action": recommended_action,
        **PAPER_SHADOW_DRIFT_SAFETY,
    }


def _source_artifacts(
    *,
    observation_payload: Mapping[str, Any],
    contract_payload: Mapping[str, Any],
) -> list[dict[str, Any]]:
    return [
        {
            "source_id": "paper_shadow_daily_observation",
            "artifact_id": observation_payload.get("observation_id"),
            "path": observation_payload.get("paper_shadow_daily_observation_path"),
            "status": observation_payload.get("status"),
        },
        {
            "source_id": "formal_research_method_contract",
            "artifact_id": contract_payload.get("contract_id"),
            "path": contract_payload.get("formal_research_method_contract_path"),
            "status": contract_payload.get("status"),
        },
    ]


def _objective_gate_map(gates: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    return {_text(row.get("gate_id")): dict(row) for row in gates if _text(row.get("gate_id"))}


def _gate_expectation(
    gates: Mapping[str, Mapping[str, Any]],
    gate_id: str,
    default: str,
) -> str:
    gate = _mapping(gates.get(gate_id))
    actual = _text(gate.get("actual_value"))
    if actual:
        return f"{gate_id}={actual}; {default}"
    return default


def _label_severity(value: str) -> str:
    text = value.lower()
    if not text:
        return "BLOCKING"
    if _contains_any(text, BLOCKING_LABELS):
        return "BLOCKING"
    if _contains_any(text, WARNING_LABELS):
        return "WARNING"
    if _contains_any(text, WATCH_LABELS) and not _contains_any(text, NO_DRIFT_LABELS):
        return "WATCH"
    return "NONE"


def _risk_off_severity(risk_state: str, signal_output: str) -> str:
    text = f"{risk_state} {signal_output}".lower()
    if not risk_state:
        return "BLOCKING"
    if _contains_any(text, BLOCKING_LABELS):
        return "BLOCKING"
    if _contains_any(text, ("excessive_risk_off", "persistent_risk_off", "risk_off_cluster")):
        return "WARNING"
    if "risk_off" in text:
        return "WATCH"
    return "NONE"


def _benchmark_severity(value: str) -> str:
    text = value.lower()
    if not text:
        return "BLOCKING"
    if _contains_any(text, BLOCKING_LABELS):
        return "BLOCKING"
    if _contains_any(text, ("material_underperformance", "severe_lag", "worse")):
        return "WARNING"
    if _contains_any(text, ("underperform", "lag", "behind")):
        return "WATCH"
    return "NONE"


def _missing_signal_input_severity(
    observation: Mapping[str, Any],
    input_artifacts: Sequence[Mapping[str, Any]],
) -> str:
    if observation.get("observation_status") != "RECORDED":
        return "BLOCKING"
    if not input_artifacts:
        return "BLOCKING"
    if any(row.get("exists") is not True for row in input_artifacts):
        return "BLOCKING"
    if any(not _text(row.get("checksum_sha256")) for row in input_artifacts):
        return "BLOCKING"
    return "NONE"


def _missing_input_value(
    *,
    observation_payload: Mapping[str, Any],
    observation: Mapping[str, Any],
    input_artifacts: Sequence[Mapping[str, Any]],
) -> str:
    missing = [
        _text(row.get("source_id"))
        for row in input_artifacts
        if row.get("exists") is not True or not _text(row.get("checksum_sha256"))
    ]
    if observation.get("observation_status") != "RECORDED":
        missing.append(f"observation_status={observation.get('observation_status')}")
    if not missing and not input_artifacts:
        missing.append("input_artifacts_missing")
    if not missing:
        return f"inputs_complete_for_{observation_payload.get('observation_id')}"
    return ",".join(missing)


def _overall_drift_severity(findings: Sequence[Mapping[str, Any]]) -> str:
    rank = {name: index for index, name in enumerate(DRIFT_SEVERITIES)}
    return max(
        (_text(row.get("severity"), "BLOCKING") for row in findings),
        key=lambda item: rank.get(item, 999),
    )


def _next_action(
    *,
    severity: str,
    blocking_count: int,
    warning_count: int,
    watch_count: int,
) -> str:
    if severity == "BLOCKING":
        return "reject_candidate" if blocking_count >= 2 else "return_to_research"
    if severity == "WARNING" or warning_count:
        return "return_to_research"
    if severity == "WATCH" or watch_count:
        return "needs_manual_review"
    return "continue_shadow"


def _contains_any(text: str, markers: Sequence[str]) -> bool:
    return any(marker in text for marker in markers)


_mapping = st._mapping
_records = st._records
_text = st._text
_texts = st._texts
