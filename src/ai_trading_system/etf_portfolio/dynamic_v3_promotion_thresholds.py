from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_PROMOTION_GATE_THRESHOLDS_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "research" / "promotion_gate_thresholds.yaml"
)
DEFAULT_PROMOTION_GATE_THRESHOLD_CALIBRATION_DIR = (
    PROJECT_ROOT
    / "run"
    / "review"
    / "register"
    / "promotion-gate-threshold-calibration"
)
REQUIRED_THRESHOLD_FAMILIES = (
    "stress_strength",
    "drawdown_mismatch_reduction",
    "flip_rotation_reduction",
    "ab_review_confidence",
    "confirmation_target_count",
)
PROMOTION_THRESHOLD_SAFETY = {
    **readiness.FORMAL_RESEARCH_CONTRACT_SAFETY,
    "governance_only": True,
    "threshold_policy_only": True,
    "manual_review_only": True,
    "not_official_target_weights": True,
    "broker_action_allowed": False,
    "broker_action_taken": False,
    "order_ticket_generated": False,
    "production_state_mutated": False,
    "official_target_weights_mutated": False,
    "automatic_candidate_promotion": False,
    "auto_apply": False,
    "production_effect": "none",
}


def load_promotion_gate_threshold_policy(
    config_path: Path = DEFAULT_PROMOTION_GATE_THRESHOLDS_CONFIG_PATH,
) -> dict[str, Any]:
    payload = safe_load_yaml_path(config_path)
    if not isinstance(payload, dict):
        raise st.DynamicV3SystemTargetError(f"policy config must be a mapping: {config_path}")
    return payload


def validate_promotion_gate_threshold_policy(
    *,
    config_path: Path = DEFAULT_PROMOTION_GATE_THRESHOLDS_CONFIG_PATH,
) -> dict[str, Any]:
    policy = load_promotion_gate_threshold_policy(config_path)
    checks = _policy_checks(policy, config_path)
    return st._validation_payload(
        "etf_dynamic_v3_promotion_gate_threshold_policy_validation",
        _text(policy.get("policy_id"), config_path.name),
        checks,
    )


def build_promotion_gate_threshold_calibration_report(
    *,
    config_path: Path = DEFAULT_PROMOTION_GATE_THRESHOLDS_CONFIG_PATH,
    contract_id: str | None = None,
    contract_dir: Path = readiness.DEFAULT_FORMAL_RESEARCH_METHOD_CONTRACT_DIR,
    output_dir: Path = DEFAULT_PROMOTION_GATE_THRESHOLD_CALIBRATION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    policy = load_promotion_gate_threshold_policy(config_path)
    policy_validation = validate_promotion_gate_threshold_policy(config_path=config_path)
    contract_payload = readiness.formal_research_method_contract_report_payload(
        contract_id=contract_id,
        latest=contract_id is None,
        output_dir=contract_dir,
    )
    contract = _mapping(contract_payload.get("formal_research_method_contract"))
    decision = _mapping(contract_payload.get("formal_research_method_decision"))
    gates = _records(contract.get("objective_gates"))
    threshold_rows = _threshold_interpretation_rows(policy, gates)
    all_thresholds_pass = all(row.get("passed") is True for row in threshold_rows)
    policy_pass = policy_validation.get("status") == "PASS"
    current_interpretation = (
        "FORMAL_RESEARCH_READY_UNDER_PILOT_THRESHOLDS"
        if all_thresholds_pass and policy_pass
        else "THRESHOLD_REVIEW_REQUIRED"
    )
    status = "PASS" if policy_pass and contract_payload.get("contract_id") else "FAIL"
    calibration_id = st._stable_id(
        "promotion-gate-threshold-calibration",
        _text(policy.get("policy_id")),
        _text(policy.get("version")),
        _text(contract_payload.get("contract_id")),
        generated.isoformat(),
    )
    root = st._unique_dir(output_dir / calibration_id)
    root.mkdir(parents=True, exist_ok=False)
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_promotion_gate_threshold_calibration_report",
        "calibration_id": root.name,
        "generated_at": generated.isoformat(),
        "status": status,
        "policy_id": _text(policy.get("policy_id")),
        "policy_version": _text(policy.get("version")),
        "policy_status": _text(policy.get("status")),
        "source_config_path": str(config_path),
        "source_contract_id": contract_payload.get("contract_id"),
        "candidate": contract_payload.get("candidate"),
        "formal_research_method_status": decision.get("formal_research_method_status"),
        "promotion_state": decision.get("promotion_state"),
        "current_threshold_interpretation": current_interpretation,
        "threshold_rows": threshold_rows,
        "stress_required": _mapping(
            _threshold_family(policy, "stress_strength")
        ).get("required_for_formal_research"),
        "confirmation_target_minimum": _mapping(
            _threshold_family(policy, "confirmation_target_count")
        ).get("minimum_for_formal_research"),
        "next_required_action": (
            "continue_with_formal_research_governance_only"
            if current_interpretation == "FORMAL_RESEARCH_READY_UNDER_PILOT_THRESHOLDS"
            else "manual_threshold_review_before_candidate_use"
        ),
        "limitations": [
            "pilot discrete-status policy, not outcome-fitted calibration",
            "does not change formal contract decision logic",
            "does not create official target weights or production approval",
        ],
        **PROMOTION_THRESHOLD_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_promotion_gate_threshold_calibration_manifest",
        "calibration_id": root.name,
        "generated_at": generated.isoformat(),
        "status": status,
        "policy_id": _text(policy.get("policy_id")),
        "policy_version": _text(policy.get("version")),
        "source_contract_id": contract_payload.get("contract_id"),
        "candidate": contract_payload.get("candidate"),
        "promotion_gate_threshold_calibration_manifest_path": str(
            root / "promotion_gate_threshold_calibration_manifest.json"
        ),
        "promotion_gate_threshold_calibration_report_path": str(
            root / "promotion_gate_threshold_calibration_report.json"
        ),
        "promotion_gate_threshold_calibration_markdown_path": str(
            root / "promotion_gate_threshold_calibration_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        "validation_path": str(root / "promotion_gate_threshold_validation.json"),
        **PROMOTION_THRESHOLD_SAFETY,
    }
    reader = render_promotion_gate_threshold_reader_brief(report)
    st._write_json(root / "promotion_gate_threshold_calibration_manifest.json", manifest)
    st._write_json(root / "promotion_gate_threshold_calibration_report.json", report)
    st._write_text(
        root / "promotion_gate_threshold_calibration_report.md",
        render_promotion_gate_threshold_report(manifest, report),
    )
    st._write_text(root / "reader_brief_section.md", reader)
    validation = validate_promotion_gate_threshold_calibration_artifact(
        calibration_id=root.name,
        output_dir=output_dir,
        write_output=True,
    )
    return {
        "calibration_id": root.name,
        "calibration_dir": root,
        "manifest": manifest,
        "report": report,
        "reader_brief_section": reader,
        "policy_validation": policy_validation,
        "validation": validation,
    }


def promotion_gate_threshold_calibration_report_payload(
    *,
    calibration_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PROMOTION_GATE_THRESHOLD_CALIBRATION_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=calibration_id,
        latest=latest,
        output_dir=output_dir,
        required_name="promotion_gate_threshold_calibration_manifest.json",
    )
    payload = {
        **st._read_json(root / "promotion_gate_threshold_calibration_manifest.json"),
        "promotion_gate_threshold_calibration_report": st._read_json(
            root / "promotion_gate_threshold_calibration_report.json"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(
            encoding="utf-8"
        ),
        "calibration_dir": str(root),
    }
    validation = st._read_optional_json(root / "promotion_gate_threshold_validation.json")
    if validation:
        payload["promotion_gate_threshold_validation"] = validation
    return payload


def validate_promotion_gate_threshold_calibration_artifact(
    *,
    calibration_id: str,
    output_dir: Path = DEFAULT_PROMOTION_GATE_THRESHOLD_CALIBRATION_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / calibration_id
    manifest = (
        st._read_optional_json(root / "promotion_gate_threshold_calibration_manifest.json")
        or {}
    )
    report = (
        st._read_optional_json(root / "promotion_gate_threshold_calibration_report.json")
        or {}
    )
    reader = (
        (root / "reader_brief_section.md").read_text(encoding="utf-8")
        if (root / "reader_brief_section.md").exists()
        else ""
    )
    threshold_rows = _records(report.get("threshold_rows"))
    checks = st._required_file_checks(
        root,
        (
            "promotion_gate_threshold_calibration_manifest.json",
            "promotion_gate_threshold_calibration_report.json",
            "promotion_gate_threshold_calibration_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "calibration_id_matches",
                manifest.get("calibration_id") == calibration_id,
                "",
            ),
            st._check("policy_id_visible", bool(_text(report.get("policy_id"))), ""),
            st._check(
                "source_contract_visible",
                bool(_text(report.get("source_contract_id"))),
                "",
            ),
            st._check(
                "threshold_families_complete",
                set(REQUIRED_THRESHOLD_FAMILIES).issubset(
                    {str(row.get("threshold_family")) for row in threshold_rows}
                ),
                "",
            ),
            st._check(
                "reader_brief_fields",
                "promotion_threshold_calibration_id" in reader,
                "",
            ),
            st._check("broker_forbidden", st._payload_safe(manifest, report), ""),
            st._check(
                "no_auto_apply",
                manifest.get("auto_apply") is False and report.get("auto_apply") is False,
                "",
            ),
        ]
    )
    validation = st._validation_payload(
        "etf_dynamic_v3_promotion_gate_threshold_calibration_validation",
        calibration_id,
        checks,
    )
    if write_output:
        st._write_json(root / "promotion_gate_threshold_validation.json", validation)
        st._write_text(
            root / "promotion_gate_threshold_validation.md",
            render_promotion_gate_threshold_validation_report(validation),
        )
    return validation


def render_promotion_gate_threshold_reader_brief(report: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Promotion Gate Threshold Calibration",
            "",
            f"- promotion_threshold_calibration_id: {report.get('calibration_id')}",
            f"- promotion_threshold_policy_id: {report.get('policy_id')}",
            f"- promotion_threshold_policy_version: {report.get('policy_version')}",
            f"- promotion_threshold_status: {report.get('status')}",
            "- promotion_threshold_current_interpretation: "
            f"{report.get('current_threshold_interpretation')}",
            f"- promotion_threshold_stress_required: {report.get('stress_required')}",
            "- promotion_threshold_confirmation_minimum: "
            f"{report.get('confirmation_target_minimum')}",
            f"- promotion_threshold_next_action: {report.get('next_required_action')}",
            "- safety_boundary: governance only / no official target / no broker / "
            "no production",
            "",
        ]
    )


def render_promotion_gate_threshold_report(
    manifest: Mapping[str, Any],
    report: Mapping[str, Any],
) -> str:
    row_lines = [
        "- {family}: observed={observed} required={required} passed={passed}".format(
            family=row.get("threshold_family"),
            observed=row.get("observed_value"),
            required=row.get("required_value"),
            passed=row.get("passed"),
        )
        for row in _records(report.get("threshold_rows"))
    ]
    return "\n".join(
        [
            f"# Promotion Gate Threshold Calibration {manifest.get('calibration_id')}",
            "",
            "## Purpose",
            "Document conservative pilot threshold bands for formal research gates.",
            "",
            "## Input Artifacts",
            f"- config: {report.get('source_config_path')}",
            f"- formal_research_method_contract: {report.get('source_contract_id')}",
            "",
            "## Output Decision",
            f"- status: {report.get('status')}",
            "- current_threshold_interpretation: "
            f"{report.get('current_threshold_interpretation')}",
            f"- next_required_action: {report.get('next_required_action')}",
            "",
            "## Threshold Rows",
            *row_lines,
            "",
            "## Safety Boundary",
            "- governance only",
            "- no threshold tuning to force candidate pass",
            "- no official target weights",
            "- no broker action or order ticket",
            "- no production mutation",
            "",
            "## Limitations",
            *[f"- {item}" for item in _texts(report.get("limitations"))],
            "",
        ]
    )


def render_promotion_gate_threshold_validation_report(validation: Mapping[str, Any]) -> str:
    rows = [
        f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}"
        for row in _records(validation.get("checks"))
    ]
    return "\n".join(
        [
            f"# Promotion Gate Threshold Validation {validation.get('artifact_id')}",
            "",
            f"- status: {validation.get('status')}",
            f"- failed_check_count: {validation.get('failed_check_count')}",
            "- production_effect: none",
            "",
            "## Checks",
            *rows,
            "",
        ]
    )


def _policy_checks(policy: Mapping[str, Any], config_path: Path) -> list[dict[str, Any]]:
    threshold_bands = _mapping(policy.get("threshold_bands"))
    safety = _mapping(policy.get("safety_boundary"))
    checks = [
        st._check("config_exists", config_path.exists(), str(config_path)),
        st._check("policy_id_visible", bool(_text(policy.get("policy_id"))), ""),
        st._check("policy_version_visible", bool(_text(policy.get("version"))), ""),
        st._check("status_visible", bool(_text(policy.get("status"))), ""),
        st._check("owner_visible", bool(_text(policy.get("owner"))), ""),
        st._check("rationale_visible", bool(_text(policy.get("rationale"))), ""),
        st._check(
            "intended_effect_visible",
            bool(_text(policy.get("intended_effect"))),
            "",
        ),
        st._check(
            "validation_evidence_visible",
            bool(_text(policy.get("validation_evidence"))),
            "",
        ),
        st._check(
            "review_condition_visible",
            bool(_text(policy.get("review_condition"))),
            "",
        ),
        st._check(
            "threshold_families_complete",
            set(REQUIRED_THRESHOLD_FAMILIES).issubset(set(threshold_bands)),
            ",".join(sorted(threshold_bands)),
        ),
        st._check(
            "safety_boundary_complete",
            safety.get("manual_review_only") is True
            and safety.get("not_official_target_weights") is True
            and safety.get("broker_action_allowed") is False
            and safety.get("order_ticket_generated") is False
            and _text(safety.get("production_effect")) == "none",
            "",
        ),
    ]
    for family in REQUIRED_THRESHOLD_FAMILIES:
        config = _mapping(threshold_bands.get(family))
        checks.extend(
            [
                st._check(
                    f"{family}_source_gate_visible",
                    bool(_text(config.get("source_gate_id"))),
                    "",
                ),
                st._check(
                    f"{family}_rationale_visible",
                    bool(_text(config.get("rationale"))),
                    "",
                ),
                st._check(
                    f"{family}_intended_effect_visible",
                    bool(_text(config.get("intended_effect"))),
                    "",
                ),
                st._check(
                    f"{family}_review_condition_visible",
                    bool(_text(config.get("review_condition"))),
                    "",
                ),
            ]
        )
    confirmation = _mapping(threshold_bands.get("confirmation_target_count"))
    checks.append(
        st._check(
            "confirmation_minimum_conservative",
            _int(confirmation.get("minimum_for_formal_research")) >= 3,
            str(confirmation.get("minimum_for_formal_research")),
        )
    )
    return checks


def _threshold_interpretation_rows(
    policy: Mapping[str, Any],
    gates: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [
        _threshold_row(policy, gates, "stress_strength"),
        _threshold_row(policy, gates, "drawdown_mismatch_reduction"),
        _threshold_row(policy, gates, "flip_rotation_reduction"),
        _threshold_row(policy, gates, "ab_review_confidence"),
        _threshold_row(policy, gates, "confirmation_target_count"),
    ]


def _threshold_row(
    policy: Mapping[str, Any],
    gates: Sequence[Mapping[str, Any]],
    family: str,
) -> dict[str, Any]:
    config = _threshold_family(policy, family)
    gate = _gate_by_id(gates, _text(config.get("source_gate_id")))
    observed = _text(gate.get("observed_status"), "MISSING")
    if family == "confirmation_target_count":
        required = _int(config.get("minimum_for_formal_research"))
        passed = _int(observed) >= required
        required_value = f">={required}"
    elif family == "flip_rotation_reduction":
        required_flip = _text(config.get("required_flip_reduction_status"))
        required_rotation = _text(config.get("required_rotation_reduction_status"))
        passed = f"flip={required_flip}" in observed and f"rotation={required_rotation}" in observed
        required_value = f"flip={required_flip}; rotation={required_rotation}"
    else:
        required_value = _text(config.get("required_for_formal_research"))
        passed = observed == required_value
    return {
        "schema_version": st.SCHEMA_VERSION,
        "threshold_family": family,
        "source_gate_id": config.get("source_gate_id"),
        "observed_value": observed,
        "required_value": required_value,
        "passed": bool(passed),
        "source_gate_passed": gate.get("passed") is True,
        "rationale": config.get("rationale"),
        "intended_effect": config.get("intended_effect"),
        "review_condition": config.get("review_condition"),
        **PROMOTION_THRESHOLD_SAFETY,
    }


def _threshold_family(policy: Mapping[str, Any], family: str) -> Mapping[str, Any]:
    return _mapping(_mapping(policy.get("threshold_bands")).get(family))


def _gate_by_id(
    gates: Sequence[Mapping[str, Any]],
    gate_id: str,
) -> Mapping[str, Any]:
    for gate in gates:
        if _text(gate.get("gate_id")) == gate_id:
            return gate
    return {}


def _artifact_dir(
    *,
    artifact_id: str | None,
    latest: bool,
    output_dir: Path,
    required_name: str,
) -> Path:
    resolved_id = artifact_id
    if latest:
        latest_dir = st._latest_child_dir_with(output_dir, required_name)
        resolved_id = latest_dir.name if latest_dir else ""
    if not resolved_id:
        raise st.DynamicV3SystemTargetError("--calibration-id or --latest is required")
    root = output_dir / resolved_id
    if not (root / required_name).exists():
        raise st.DynamicV3SystemTargetError(f"artifact not found: {root / required_name}")
    return root


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


_mapping = st._mapping
_records = st._records
_text = st._text
_texts = st._texts
