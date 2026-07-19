# ruff: noqa: E501

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio import (
    dynamic_v3_filtered_candidate_readiness_pipeline as pipeline,
)
from ai_trading_system.etf_portfolio import dynamic_v3_research_contract_ledger as readiness
from ai_trading_system.etf_portfolio import dynamic_v3_signal_diagnosis_foundation as diagnosis
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_foundation as foundation
from ai_trading_system.platform.artifacts.validation_session import (
    cached_artifact_validation,
    with_artifact_validation_session,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_PROMOTION_GATE_THRESHOLDS_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "research" / "promotion_gate_thresholds.yaml"
)
DEFAULT_PROMOTION_GATE_THRESHOLD_CALIBRATION_DIR = (
    PROJECT_ROOT / "run" / "review" / "register" / "promotion-gate-threshold-calibration"
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
    "not_empirically_calibrated": True,
    "not_official_target_weights": True,
    "broker_action_allowed": False,
    "automatic_candidate_promotion": False,
    "auto_apply": False,
    "production_effect": "none",
}
THRESHOLD_INPUT_SCHEMA = "promotion_gate_threshold_calibration_input_snapshot.v2"
THRESHOLD_VIEWS = (
    "promotion_gate_threshold_calibration_manifest.json",
    "promotion_gate_threshold_calibration_report.json",
    "promotion_gate_threshold_calibration_report.md",
    "reader_brief_section.md",
)

_mapping = foundation._mapping
_records = foundation._records
_text = foundation._text
_texts = st._texts


class DynamicV3PromotionThresholdError(ValueError):
    """Raised when the pilot threshold policy cannot be reproduced."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3PromotionThresholdError(message)


def _generated_time(value: datetime | None) -> datetime:
    generated = value or datetime.now(UTC)
    _require(generated.tzinfo is not None, "generated_at must be timezone-aware")
    return generated.astimezone(UTC)


def _aware_time(value: Any, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(_text(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise DynamicV3PromotionThresholdError(f"invalid {field}") from exc
    _require(parsed.tzinfo is not None, f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def load_promotion_gate_threshold_policy(
    config_path: Path = DEFAULT_PROMOTION_GATE_THRESHOLDS_CONFIG_PATH,
) -> dict[str, Any]:
    payload = safe_load_yaml_path(config_path)
    if not isinstance(payload, dict):
        raise DynamicV3PromotionThresholdError(f"policy config must be a mapping: {config_path}")
    return payload


def validate_promotion_gate_threshold_policy(
    *, config_path: Path = DEFAULT_PROMOTION_GATE_THRESHOLDS_CONFIG_PATH,
) -> dict[str, Any]:
    policy = load_promotion_gate_threshold_policy(config_path)
    return st._validation_payload(
        "etf_dynamic_v3_promotion_gate_threshold_policy_validation",
        _text(policy.get("policy_id"), config_path.name),
        _policy_checks(policy, config_path),
    )


def _policy_checks(policy: Mapping[str, Any], config_path: Path) -> list[dict[str, Any]]:
    bands = _mapping(policy.get("threshold_bands"))
    safety = _mapping(policy.get("safety_boundary"))
    method = _mapping(policy.get("calibration_method"))
    checks = [
        st._check("config_exists", config_path.is_file(), str(config_path)),
        st._check("pilot_baseline", policy.get("status") == "pilot_baseline", ""),
        st._check("policy_id_visible", bool(_text(policy.get("policy_id"))), ""),
        st._check("policy_version_visible", bool(_text(policy.get("version"))), ""),
        st._check("owner_visible", bool(_text(policy.get("owner"))), ""),
        st._check("rationale_visible", bool(_text(policy.get("rationale"))), ""),
        st._check("intended_effect_visible", bool(_text(policy.get("intended_effect"))), ""),
        st._check("review_condition_visible", bool(_text(policy.get("review_condition"))), ""),
        st._check(
            "not_empirically_calibrated",
            method.get("status") == "pilot_baseline"
            and method.get("not_fit_to_current_candidate") is True
            and method.get("no_outcome_backfit") is True,
            "",
        ),
        st._check(
            "threshold_families_complete",
            set(REQUIRED_THRESHOLD_FAMILIES).issubset(bands),
            ",".join(sorted(bands)),
        ),
        st._check(
            "safety_boundary_complete",
            safety.get("manual_review_only") is True
            and safety.get("threshold_policy_only") is True
            and safety.get("broker_action_allowed") is False
            and safety.get("automatic_candidate_promotion") is False
            and safety.get("production_effect") == "none",
            "",
        ),
    ]
    for family in REQUIRED_THRESHOLD_FAMILIES:
        row = _mapping(bands.get(family))
        for field in ("source_gate_id", "rationale", "intended_effect", "review_condition"):
            checks.append(st._check(f"{family}_{field}_visible", bool(_text(row.get(field))), ""))
    return checks


def _threshold_rows(
    policy: Mapping[str, Any], contract: Mapping[str, Any]
) -> list[dict[str, Any]]:
    gates = {str(row.get("gate_id")): row for row in _records(contract.get("objective_gates"))}
    bands = _mapping(policy.get("threshold_bands"))
    aliases = {
        "stress_strength": "stress_result",
        "drawdown_mismatch_reduction": "drawdown_mismatch_reduction",
        "flip_rotation_reduction": "flip_rotation_reduction",
        "ab_review_confidence": "ab_review",
        "confirmation_target_count": "confirmation_completed_observations",
    }
    rows: list[dict[str, Any]] = []
    for family in REQUIRED_THRESHOLD_FAMILIES:
        config = _mapping(bands.get(family))
        gate = _mapping(gates.get(aliases[family]))
        observed = gate.get("observed_status")
        if family == "confirmation_target_count":
            required: Any = f">={config.get('minimum_for_formal_research')} completed observations"
            diagnostic_match = False
            interpretation = "REGISTERED_TARGET_POLICY_IS_NOT_OBSERVED_CONFIRMATION"
        elif family == "flip_rotation_reduction":
            required = (
                f"flip={config.get('required_flip_reduction_status')}; "
                f"rotation={config.get('required_rotation_reduction_status')}"
            )
            diagnostic_match = observed == required
            interpretation = "PILOT_DISCRETE_STATUS_COMPARISON"
        else:
            required = config.get("required_for_formal_research")
            diagnostic_match = observed == required
            interpretation = "PILOT_DISCRETE_STATUS_COMPARISON"
        rows.append(
            {
                "schema_version": st.SCHEMA_VERSION,
                "threshold_family": family,
                "configured_source_gate_id": config.get("source_gate_id"),
                "observed_contract_gate_id": aliases[family],
                "observed_value": observed,
                "required_value": required,
                "diagnostic_match": diagnostic_match,
                "passed": False,
                "promotion_effect": False,
                "interpretation": interpretation,
                "rationale": config.get("rationale"),
                "intended_effect": config.get("intended_effect"),
                "review_condition": config.get("review_condition"),
                **PROMOTION_THRESHOLD_SAFETY,
            }
        )
    return rows


def _threshold_material(
    *, root: Path, calibration_id: str, policy: Mapping[str, Any],
    contract_payload: Mapping[str, Any], config_path: Path, generated: datetime,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    contract = _mapping(contract_payload.get("formal_research_method_contract"))
    decision = _mapping(contract_payload.get("formal_research_method_decision"))
    rows = _threshold_rows(policy, contract)
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_promotion_gate_threshold_calibration_report",
        "calibration_id": calibration_id,
        "generated_at": generated.isoformat(),
        "status": "PASS_WITH_WARNINGS",
        "calibration_status": "PILOT_POLICY_ONLY_NOT_EMPIRICALLY_CALIBRATED",
        "policy_id": policy.get("policy_id"),
        "policy_version": _text(policy.get("version")),
        "policy_status": policy.get("status"),
        "policy_owner": policy.get("owner"),
        "policy_rationale": policy.get("rationale"),
        "policy_intended_effect": policy.get("intended_effect"),
        "policy_review_condition": policy.get("review_condition"),
        "source_config_path": str(config_path.resolve()),
        "source_contract_id": contract_payload.get("contract_id"),
        "candidate": contract_payload.get("candidate"),
        "formal_research_method_status": decision.get("formal_research_method_status"),
        "promotion_state": decision.get("promotion_state"),
        "current_threshold_interpretation": "PILOT_POLICY_ONLY_NOT_EMPIRICALLY_CALIBRATED",
        "threshold_rows": rows,
        "stress_required": _mapping(
            _mapping(policy.get("threshold_bands")).get("stress_strength")
        ).get("required_for_formal_research"),
        "confirmation_target_minimum": _mapping(
            _mapping(policy.get("threshold_bands")).get("confirmation_target_count")
        ).get("minimum_for_formal_research"),
        "next_required_action": "collect_observed_evidence_before_empirical_calibration",
        "limitations": [
            "pilot discrete-status policy, not outcome-fitted calibration",
            "registered confirmation targets are not completed observations",
            "policy diagnostics cannot override the formal contract",
            "no automatic promotion or production approval",
        ],
        **PROMOTION_THRESHOLD_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_promotion_gate_threshold_calibration_manifest",
        "calibration_id": calibration_id,
        "generated_at": generated.isoformat(),
        "status": "PASS_WITH_WARNINGS",
        "policy_id": policy.get("policy_id"),
        "policy_version": _text(policy.get("version")),
        "source_contract_id": contract_payload.get("contract_id"),
        "candidate": contract_payload.get("candidate"),
        "promotion_gate_threshold_calibration_manifest_path": str(root / THRESHOLD_VIEWS[0]),
        "promotion_gate_threshold_calibration_report_path": str(root / THRESHOLD_VIEWS[1]),
        "promotion_gate_threshold_calibration_markdown_path": str(root / THRESHOLD_VIEWS[2]),
        "reader_brief_section_path": str(root / THRESHOLD_VIEWS[3]),
        "promotion_gate_threshold_input_snapshot_path": str(
            root / "promotion_gate_threshold_calibration_input_snapshot.json"
        ),
        **PROMOTION_THRESHOLD_SAFETY,
    }
    reader = render_promotion_gate_threshold_reader_brief(report)
    return manifest, {
        THRESHOLD_VIEWS[0]: foundation._json_bytes(manifest),
        THRESHOLD_VIEWS[1]: foundation._json_bytes(report),
        THRESHOLD_VIEWS[2]: foundation._text_file_bytes(
            render_promotion_gate_threshold_report(manifest, report)
        ),
        THRESHOLD_VIEWS[3]: foundation._text_file_bytes(reader),
    }


@with_artifact_validation_session
def build_promotion_gate_threshold_calibration_report(
    *, config_path: Path = DEFAULT_PROMOTION_GATE_THRESHOLDS_CONFIG_PATH,
    contract_id: str | None = None,
    contract_dir: Path = readiness.DEFAULT_FORMAL_RESEARCH_METHOD_CONTRACT_DIR,
    output_dir: Path = DEFAULT_PROMOTION_GATE_THRESHOLD_CALIBRATION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    _require(bool(contract_id), "explicit contract_id is required")
    policy = load_promotion_gate_threshold_policy(config_path)
    policy_validation = validate_promotion_gate_threshold_policy(config_path=config_path)
    _require(policy_validation.get("status") == "PASS", "promotion policy validation failed")
    contract_validation = cached_artifact_validation(
        validator=readiness.validate_formal_research_method_contract_artifact,
        validator_key="contract_id", artifact_id=_text(contract_id), root=contract_dir,
    )
    _require(contract_validation.get("status") == "PASS", "formal contract validation failed")
    contract_payload = readiness.formal_research_method_contract_report_payload(
        contract_id=contract_id, output_dir=contract_dir
    )
    _require(
        _aware_time(contract_payload.get("generated_at"), "contract.generated_at") <= generated,
        "contract generated after threshold report",
    )
    calibration_id = foundation._stable_id(
        "promotion-gate-threshold-calibration", policy.get("policy_id"),
        policy.get("version"), contract_id, generated.isoformat(),
    )
    root = foundation._unique_dir(output_dir / calibration_id)
    _, views = _threshold_material(
        root=root, calibration_id=root.name, policy=policy,
        contract_payload=contract_payload, config_path=config_path, generated=generated,
    )
    root.mkdir(parents=True, exist_ok=False)
    pipeline._write_views(root, views)
    snapshot = {
        "schema_version": THRESHOLD_INPUT_SCHEMA,
        "calibration_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": foundation._file_binding(config_path),
        "contract_source": pipeline._binding(
            kind="formal_research_method_contract", artifact_id=_text(contract_id),
            root=contract_dir / _text(contract_id), names=readiness.FORMAL_FILES,
        ),
        "view_hashes": foundation._view_hashes(root, THRESHOLD_VIEWS),
        **PROMOTION_THRESHOLD_SAFETY,
    }
    foundation._write_snapshot(
        root / "promotion_gate_threshold_calibration_input_snapshot.json", snapshot
    )
    foundation._write_latest_pointer(
        "latest_promotion_gate_threshold_calibration", root.name, root / THRESHOLD_VIEWS[0]
    )
    validation = validate_promotion_gate_threshold_calibration_artifact(
        calibration_id=root.name, output_dir=output_dir, write_output=True
    )
    payload = promotion_gate_threshold_calibration_report_payload(
        calibration_id=root.name, output_dir=output_dir
    )
    return {
        **payload,
        "calibration_dir": root,
        "manifest": foundation._read_json(root / THRESHOLD_VIEWS[0]),
        "report": payload["promotion_gate_threshold_calibration_report"],
        "reader_brief_section": payload["reader_brief_section"],
        "policy_validation": policy_validation,
        "validation": validation,
    }


def promotion_gate_threshold_calibration_report_payload(
    *, calibration_id: str | None = None, latest: bool = False,
    output_dir: Path = DEFAULT_PROMOTION_GATE_THRESHOLD_CALIBRATION_DIR,
) -> dict[str, Any]:
    root = foundation._artifact_dir(
        artifact_id=calibration_id,
        latest_pointer="latest_promotion_gate_threshold_calibration",
        latest=latest, output_dir=output_dir, required_name=THRESHOLD_VIEWS[0],
    )
    payload = {
        **foundation._read_json(root / THRESHOLD_VIEWS[0]),
        "promotion_gate_threshold_calibration_report": foundation._read_json(
            root / THRESHOLD_VIEWS[1]
        ),
        "reader_brief_section": (root / THRESHOLD_VIEWS[3]).read_text(encoding="utf-8"),
        "input_snapshot": foundation._read_json(
            root / "promotion_gate_threshold_calibration_input_snapshot.json"
        ),
        "calibration_dir": str(root),
    }
    validation = st._read_optional_json(root / "promotion_gate_threshold_validation.json")
    if validation:
        payload["promotion_gate_threshold_validation"] = validation
    return payload


def _rebuild_threshold(root: Path, calibration_id: str) -> list[dict[str, Any]]:
    snapshot = foundation._read_json(
        root / "promotion_gate_threshold_calibration_input_snapshot.json"
    )
    policy_source = _mapping(snapshot.get("policy_source"))
    foundation._validate_file_binding(policy_source)
    config_path = Path(_text(policy_source.get("path")))
    policy = load_promotion_gate_threshold_policy(config_path)
    _require(
        validate_promotion_gate_threshold_policy(config_path=config_path).get("status") == "PASS",
        "promotion policy replay validation failed",
    )
    contract_source = _mapping(snapshot.get("contract_source"))
    pipeline._validate_binding(
        contract_source, kind="formal_research_method_contract", names=readiness.FORMAL_FILES
    )
    contract_id = pipeline._binding_id(contract_source)
    contract_dir = pipeline._binding_root(contract_source).parent
    validation = cached_artifact_validation(
        validator=readiness.validate_formal_research_method_contract_artifact,
        validator_key="contract_id", artifact_id=contract_id, root=contract_dir,
    )
    _require(validation.get("status") == "PASS", "threshold contract replay failed")
    contract_payload = readiness.formal_research_method_contract_report_payload(
        contract_id=contract_id, output_dir=contract_dir
    )
    generated = _aware_time(snapshot.get("generated_at"), "snapshot.generated_at")
    _require(
        _aware_time(contract_payload.get("generated_at"), "contract.generated_at") <= generated,
        "threshold chronology drift",
    )
    _, expected = _threshold_material(
        root=root, calibration_id=calibration_id, policy=policy,
        contract_payload=contract_payload, config_path=config_path, generated=generated,
    )
    return diagnosis._check_bytes(root, expected)


@with_artifact_validation_session
def validate_promotion_gate_threshold_calibration_artifact(
    *, calibration_id: str,
    output_dir: Path = DEFAULT_PROMOTION_GATE_THRESHOLD_CALIBRATION_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    validation = pipeline._validate_content_stage(
        artifact_id=calibration_id, output_dir=output_dir,
        snapshot_name="promotion_gate_threshold_calibration_input_snapshot.json",
        schema=THRESHOLD_INPUT_SCHEMA, id_key="calibration_id", view_names=THRESHOLD_VIEWS,
        report_type="etf_dynamic_v3_promotion_gate_threshold_calibration_validation",
        rebuild=_rebuild_threshold,
    )
    if write_output:
        root = output_dir / calibration_id
        st._write_json(root / "promotion_gate_threshold_validation.json", validation)
        st._write_text(
            root / "promotion_gate_threshold_validation.md",
            render_promotion_gate_threshold_validation_report(validation),
        )
    return validation


def render_promotion_gate_threshold_reader_brief(report: Mapping[str, Any]) -> str:
    return "\n".join([
        "## Promotion Gate Threshold Calibration", "",
        f"- promotion_threshold_calibration_id: {report.get('calibration_id')}",
        f"- promotion_threshold_policy_id: {report.get('policy_id')}",
        f"- promotion_threshold_policy_version: {report.get('policy_version')}",
        f"- promotion_threshold_current_interpretation: {report.get('current_threshold_interpretation')}",
        f"- promotion_threshold_next_action: {report.get('next_required_action')}",
        "- safety_boundary: pilot governance only / no automatic promotion / no production", "",
    ])


def render_promotion_gate_threshold_report(
    manifest: Mapping[str, Any], report: Mapping[str, Any]
) -> str:
    rows = [
        f"- {row.get('threshold_family')}: observed={row.get('observed_value')} "
        f"required={row.get('required_value')} diagnostic_match={row.get('diagnostic_match')}"
        for row in _records(report.get("threshold_rows"))
    ]
    return "\n".join([
        f"# Promotion Gate Threshold Calibration {manifest.get('calibration_id')}", "",
        f"- calibration_status：{report.get('calibration_status')}",
        f"- policy_version：{report.get('policy_version')}",
        f"- rationale：{report.get('policy_rationale')}",
        f"- review_condition：{report.get('policy_review_condition')}", "",
        "## Diagnostic Rows", *rows, "",
        "- 这些pilot bands不构成实证校准，不能覆盖formal contract或产生promotion-ready。",
        "- safety：manual governance only / no official weights / no broker / no production", "",
    ])


def render_promotion_gate_threshold_validation_report(validation: Mapping[str, Any]) -> str:
    rows = [
        f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}"
        for row in _records(validation.get("checks"))
    ]
    return "\n".join([
        f"# Promotion Gate Threshold Validation {validation.get('artifact_id')}", "",
        f"- status: {validation.get('status')}",
        f"- failed_check_count: {validation.get('failed_check_count')}",
        "- production_effect: none", "", "## Checks", *rows, "",
    ])
