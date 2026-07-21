from __future__ import annotations

import json
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from hashlib import sha256
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio import dynamic_v3_parameter_research as legacy
from ai_trading_system.platform.artifacts.writer import (
    canonical_json_bytes,
    write_bytes_atomic,
    write_json_atomic,
)

SCHEMA_VERSION = "trading2453_constraint_hit_diagnosis.v1"
VALIDATION_SCHEMA_VERSION = "trading2453_constraint_hit_diagnosis_validation.v1"
DEFAULT_RUN_ID = "trading2452-historical-seen_20260721T053621Z_144f31edee91"
DEFAULT_RUN_DIR = (
    PROJECT_ROOT
    / "reports"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "trading2452_historical_seen"
    / DEFAULT_RUN_ID
)
DEFAULT_PACKAGE_ROOT = (
    PROJECT_ROOT / "inputs" / "research" / "trading2452_dynamic_v3_clean_selection"
)
DEFAULT_POLICY_PATH = (
    PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "parameter_sweep_real_smoke.yaml"
)
DEFAULT_GATE_SOURCE_PATH = (
    PROJECT_ROOT
    / "src"
    / "ai_trading_system"
    / "etf_portfolio"
    / "dynamic_v3_parameter_research.py"
)
DEFAULT_REAL_EVALUATION_SOURCE_PATH = (
    PROJECT_ROOT / "src" / "ai_trading_system" / "etf_portfolio" / "dynamic_v3_real_evaluation.py"
)
DEFAULT_RESCUE_POLICY_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "dynamic_v3_constraint_aware_rescue.yaml"
)

# These are evidence identities from the owner-approved TRADING-2452 formal run,
# not tunable investment thresholds.
EXPECTED_RUN_HASHES = {
    "effective_windows.json": "7edb158e31196717f2dc3a01a0629d4e8eef1ad454b14a5403b5740599ba7965",
    "evaluator_manifest.json": "01559415ed0be5f2fb9ebf2f0c712c6dfd3271bc1928f59a4d0ec22ebf974af4",
    "evaluator_validation.json": "41bb381a469577bb07b8ffb542d3a0bbb8de02871667319e657d5437dc5f8bc8",
    "historical_seen_report.json": (
        "e9c1f10b56a03c3b23152190dee7b5933cc8679ef1091d1bb92c2944fa4316af"
    ),
    "train_evaluations.jsonl": "4f91f4303d6a01bf16f83fa7bac4aaac4dd4683e9f35cb9ed2fecd5ed6420b99",
}
EXPECTED_PACKAGE_HASHES = {
    "candidate_universe.json": "a47d17d5adcf37afb4ab75e0ec0e0215c8b6034021828ae448bc20ad36ccc646",
    "package_manifest.json": "8319cd55d727701a2ae57c556ac8bca2bbae06b2e6bc61d589b290520ff6c47f",
    "selection_rule.yaml": "895f6208fcf13013b2457580625b9207d12b942607aadf56e809ff7c47f1aabd",
}
EXPECTED_PACKAGE_ID = "dynamic-v3-clean-trading2452_11991ac7965cfcd7aa18"
EXPECTED_RUNTIME_BINDING_HASH = "174fcdb09f3922376eab3ffd5627682c73a9c00141a44e5fb176054cea93a36b"
EXPECTED_POLICY_SHA256 = "2c6620e497178a65b91e0cd58801901693c1d0acbfc3d7a792e8bb36ce5ef924"
EXPECTED_GATE_SOURCE_SHA256 = "b5188307e79fedcd44050f6e08d20394ba0239a524f2efe892617936450ea122"
EXPECTED_REAL_EVALUATION_SOURCE_SHA256 = (
    "9073dd48a354728d0afd3c77bfd89274b5d76c77fd99e46727713c15d665044a"
)
EXPECTED_RESCUE_POLICY_SHA256 = "8a3cfbbe31843eb0a67470ac9add125e9ceaa0a5ffc67043d84fb7915ccb5fc8"

SAFETY: dict[str, Any] = {
    "research_only": True,
    "manual_review_required": True,
    "owner_decision_required": True,
    "default_decision": "KILL_PAUSE",
    "threshold_change_allowed": False,
    "candidate_expansion_allowed": False,
    "new_parameter_search_allowed": False,
    "prospective_holdout_access_allowed": False,
    "prospective_holdout_accessed": False,
    "paper_shadow_changed": False,
    "production_effect": "none",
    "broker_action": "none",
}

OUTPUT_FILENAMES = (
    "constraint_hit_recomputations.jsonl",
    "constraint_hit_attribution.json",
    "owner_review_pack.json",
    "owner_review_pack.md",
)

V0_3_TEMPLATE_IDS = (
    "dynamic_regime_overlay_v0_3a_constraint_smooth",
    "dynamic_regime_overlay_v0_3b_drawdown_guarded",
    "dynamic_regime_overlay_v0_3c_constraint_smooth_guarded",
    "dynamic_regime_overlay_v0_3d_emergency_only_guarded",
)

ROBUSTNESS_FIELDS = (
    "real_best_candidate_policy_id",
    "real_evaluation_report_id",
    "real_policy_config_hash",
    "real_promotion_gate_decision",
    "robustness_status",
    "overfit_status",
    "parameter_sensitivity_status",
    "stress_bucket_status",
)


class Trading2453ConstraintDiagnosisError(ValueError):
    """Raised when frozen evidence cannot support a fail-closed diagnosis."""


def build_trading2453_diagnosis(
    *,
    run_dir: Path = DEFAULT_RUN_DIR,
    package_root: Path = DEFAULT_PACKAGE_ROOT,
) -> dict[str, Any]:
    source_contract = _validate_frozen_inputs(run_dir=run_dir, package_root=package_root)
    candidate_payload = _load_json(package_root / "candidate_universe.json")
    candidates = {
        str(row.get("candidate_id")): row for row in _records(candidate_payload.get("candidates"))
    }
    config = legacy.load_parameter_sweep_config(DEFAULT_POLICY_PATH)
    train_rows = _read_jsonl(run_dir / "train_evaluations.jsonl")
    recomputations = [
        recompute_constraint_row(
            row=row, candidate=candidates.get(str(row.get("candidate_id"))), config=config
        )
        for row in train_rows
    ]

    expected_candidates = set(candidates)
    fold_candidate_sets = {
        fold: {
            str(row.get("candidate_id")) for row in train_rows if row.get("window_index") == fold
        }
        for fold in range(1, 7)
    }
    exact_match_count = sum(row["status"] == "PASS" for row in recomputations)
    structure_checks = {
        "train_evaluation_count_is_1800": len(train_rows) == 1800,
        "candidate_count_is_300": len(candidates) == 300,
        "six_folds_present": {row.get("window_index") for row in train_rows} == set(range(1, 7)),
        "every_fold_has_frozen_candidate_universe": all(
            candidate_ids == expected_candidates for candidate_ids in fold_candidate_sets.values()
        ),
        "all_rows_are_train_phase": all(row.get("phase") == "train" for row in train_rows),
        "all_rows_precede_prospective": all(
            str(_mapping(row.get("requested_range")).get("end", "")) < "2026-07-22"
            for row in train_rows
        ),
        "all_evidence_complete": all(
            row.get("evidence_status") == "COMPLETE" for row in train_rows
        ),
        "all_recomputations_exact": exact_match_count == len(train_rows),
    }
    status = "PASS" if all(structure_checks.values()) else "FAIL"
    attribution = _build_attribution(
        recomputations=recomputations,
        source_contract=source_contract,
        config=config,
        structure_checks=structure_checks,
        status=status,
    )
    owner_review_pack = _build_owner_review_pack(
        source_contract=source_contract,
        attribution=attribution,
        status=status,
    )
    diagnosis_id = (
        "trading2453-constraint-hit_"
        + _stable_hash(
            source_contract,
            attribution["recomputation_summary"],
            attribution["aggregations"],
            attribution["s2_semantic_audit"],
            owner_review_pack,
        )[:20]
    )
    attribution["diagnosis_id"] = diagnosis_id
    owner_review_pack["diagnosis_id"] = diagnosis_id
    recomputation_bytes = _jsonl_bytes(recomputations)
    attribution_bytes = canonical_json_bytes(attribution)
    owner_review_bytes = canonical_json_bytes(owner_review_pack)
    owner_review_markdown_bytes = _render_owner_review_pack(owner_review_pack).encode("utf-8")
    output_bytes = {
        "constraint_hit_recomputations.jsonl": recomputation_bytes,
        "constraint_hit_attribution.json": attribution_bytes,
        "owner_review_pack.json": owner_review_bytes,
        "owner_review_pack.md": owner_review_markdown_bytes,
    }
    manifest_payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "trading2453_constraint_hit_diagnosis_manifest",
        "diagnosis_id": diagnosis_id,
        "status": status,
        "source_contract": source_contract,
        "output_artifact_checksums": {
            filename: sha256(content).hexdigest() for filename, content in output_bytes.items()
        },
        "output_artifact_sizes": {
            filename: len(content) for filename, content in output_bytes.items()
        },
        "completed_stages": ["S0", "S1", "S2", "S3"],
        "thresholds_modified": False,
        "candidate_or_search_space_modified": False,
        "original_trading2452_result_status_changed": False,
        "safety": dict(SAFETY),
        **SAFETY,
    }
    return {
        "recomputations": recomputations,
        "attribution": attribution,
        "owner_review_pack": owner_review_pack,
        "manifest": manifest_payload,
        "bytes": output_bytes,
    }


def recompute_constraint_row(
    *,
    row: Mapping[str, Any],
    candidate: Mapping[str, Any] | None,
    config: legacy.DynamicV3ParameterSweepConfig,
) -> dict[str, Any]:
    metrics = _mapping(row.get("metrics"))
    artifact_hits = _optional_int(metrics.get("constraint_hits"))
    artifact_rate = _optional_float(metrics.get("constraint_hit_rate"))
    artifact_delta = _optional_int(metrics.get("constraint_hits_delta_vs_reference"))
    reduction_count = _optional_int(metrics.get("constraint_hit_reduction_count_vs_v0_4"))
    row_count = _optional_int(row.get("row_count"))

    recomputed_hits = None
    recomputed_rate = None
    reference_hits = None
    recomputed_delta = None
    if artifact_rate is not None and row_count is not None and row_count > 0:
        recomputed_hits = int(round(artifact_rate * row_count))
    if artifact_hits is not None and row_count is not None and row_count > 0:
        recomputed_rate = round(artifact_hits / row_count, 6)
    if artifact_hits is not None and artifact_delta is not None:
        reference_hits = artifact_hits - artifact_delta
    if reduction_count is not None:
        recomputed_delta = -reduction_count

    recomputed_gate, recomputed_reasons = legacy.gate_candidate(metrics, config)
    artifact_reasons = list(row.get("gate_reasons") or [])
    candidate_parameters = None if candidate is None else _mapping(candidate.get("parameters"))
    row_parameters = _mapping(row.get("parameters"))
    exact_match = {
        "candidate_is_frozen": candidate is not None,
        "candidate_parameters_match": candidate is not None
        and candidate_parameters == row_parameters,
        "constraint_hits_from_rate_match": recomputed_hits == artifact_hits,
        "constraint_rate_from_hits_match": recomputed_rate == artifact_rate,
        "constraint_delta_from_reduction_match": recomputed_delta == artifact_delta,
        "gate_match": recomputed_gate == row.get("gate"),
        "gate_reasons_exact_match": recomputed_reasons == artifact_reasons,
        "reject_score_is_null": row.get("gate") != legacy.GATE_REJECT
        or row.get("selection_score") is None,
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "window_index": row.get("window_index"),
        "candidate_id": row.get("candidate_id"),
        "candidate_template": metrics.get("real_best_candidate_policy_id"),
        "policy_hash": metrics.get("real_policy_config_hash"),
        "requested_range": _mapping(row.get("requested_range")),
        "parameters": row_parameters,
        "artifact": {
            "row_count": row_count,
            "constraint_hits": artifact_hits,
            "constraint_hit_rate": artifact_rate,
            "constraint_hits_delta_vs_reference": artifact_delta,
            "constraint_hit_reduction_count_vs_v0_4": reduction_count,
            "gate": row.get("gate"),
            "gate_reasons": artifact_reasons,
            "selection_score": row.get("selection_score"),
        },
        "recomputed": {
            "constraint_hits_from_rate": recomputed_hits,
            "constraint_hit_rate_from_hits": recomputed_rate,
            "reference_constraint_hits": reference_hits,
            "constraint_hits_delta_from_reduction": recomputed_delta,
            "gate": recomputed_gate,
            "gate_reasons": recomputed_reasons,
        },
        "robustness_fields": {field: metrics.get(field) for field in ROBUSTNESS_FIELDS},
        "exact_match": exact_match,
        "status": "PASS" if all(exact_match.values()) else "FAIL",
        "safety": dict(SAFETY),
    }


def write_trading2453_diagnosis(
    *,
    output_dir: Path,
    run_dir: Path = DEFAULT_RUN_DIR,
    package_root: Path = DEFAULT_PACKAGE_ROOT,
) -> dict[str, Any]:
    if output_dir.exists() and any(output_dir.iterdir()):
        raise Trading2453ConstraintDiagnosisError(
            f"output directory must be absent or empty: {output_dir}"
        )
    bundle = build_trading2453_diagnosis(run_dir=run_dir, package_root=package_root)
    output_dir.mkdir(parents=True, exist_ok=True)
    for filename, content in bundle["bytes"].items():
        write_bytes_atomic(output_dir / filename, content)
    write_json_atomic(output_dir / "diagnosis_manifest.json", bundle["manifest"])
    validation = validate_trading2453_diagnosis(
        output_dir=output_dir,
        run_dir=run_dir,
        package_root=package_root,
    )
    write_json_atomic(output_dir / "diagnosis_validation.json", validation)
    if validation["status"] != "PASS":
        raise Trading2453ConstraintDiagnosisError(
            f"TRADING-2453 diagnosis validation failed: {validation['failed_check_count']}"
        )
    return {
        "status": bundle["manifest"]["status"],
        "diagnosis_id": bundle["manifest"]["diagnosis_id"],
        "output_dir": output_dir,
        "manifest": bundle["manifest"],
        "validation": validation,
        "production_effect": "none",
        "broker_action": "none",
    }


def validate_trading2453_diagnosis(
    *,
    output_dir: Path,
    run_dir: Path = DEFAULT_RUN_DIR,
    package_root: Path = DEFAULT_PACKAGE_ROOT,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    try:
        expected = build_trading2453_diagnosis(run_dir=run_dir, package_root=package_root)
        manifest = _load_json(output_dir / "diagnosis_manifest.json")
        base_inventory = {*OUTPUT_FILENAMES, "diagnosis_manifest.json"}
        actual_inventory = {path.name for path in output_dir.iterdir()}
        checks.append(
            _check(
                "output_inventory_exact",
                actual_inventory
                in (base_inventory, {*base_inventory, "diagnosis_validation.json"}),
            )
        )
        checks.append(_check("manifest_content_derived", manifest == expected["manifest"]))
        for filename in OUTPUT_FILENAMES:
            path = output_dir / filename
            checks.append(
                _check(
                    f"output_content_derived:{filename}",
                    path.is_file() and path.read_bytes() == expected["bytes"][filename],
                )
            )
        checks.extend(
            [
                _check("diagnosis_status_pass", manifest.get("status") == "PASS"),
                _check(
                    "all_stages_complete",
                    manifest.get("completed_stages") == ["S0", "S1", "S2", "S3"],
                ),
                _check("safety_exact", manifest.get("safety") == SAFETY),
                _check("thresholds_unchanged", manifest.get("thresholds_modified") is False),
                _check(
                    "no_candidate_or_search_change",
                    manifest.get("candidate_or_search_space_modified") is False,
                ),
                _check(
                    "prospective_not_accessed",
                    manifest.get("prospective_holdout_accessed") is False,
                ),
            ]
        )
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        checks.append(_check("content_derived_validation", False, str(exc)))
    status = "PASS" if checks and all(row["passed"] for row in checks) else "FAIL"
    return {
        "schema_version": VALIDATION_SCHEMA_VERSION,
        "report_type": "trading2453_constraint_hit_diagnosis_validation",
        "status": status,
        "failed_check_count": sum(not row["passed"] for row in checks),
        "checks": checks,
        "safety": dict(SAFETY),
        **SAFETY,
    }


def numeric_distribution_preserving_null(values: Sequence[object]) -> dict[str, Any]:
    present = [float(value) for value in values if value is not None]
    return {
        "observation_count": len(values),
        "present_count": len(present),
        "null_count": len(values) - len(present),
        "minimum": None if not present else min(present),
        "maximum": None if not present else max(present),
        "mean": None if not present else sum(present) / len(present),
    }


def _validate_frozen_inputs(*, run_dir: Path, package_root: Path) -> dict[str, Any]:
    if run_dir.name != DEFAULT_RUN_ID:
        raise Trading2453ConstraintDiagnosisError("unexpected TRADING-2452 run id")
    input_hashes: dict[str, str] = {}
    for filename, expected_hash in EXPECTED_RUN_HASHES.items():
        actual = _file_sha256(run_dir / filename)
        if actual != expected_hash:
            raise Trading2453ConstraintDiagnosisError(f"frozen run input drift: {filename}")
        input_hashes[f"run:{filename}"] = actual
    for filename, expected_hash in EXPECTED_PACKAGE_HASHES.items():
        actual = _file_sha256(package_root / filename)
        if actual != expected_hash:
            raise Trading2453ConstraintDiagnosisError(f"frozen package input drift: {filename}")
        input_hashes[f"package:{filename}"] = actual

    manifest = _load_json(run_dir / "evaluator_manifest.json")
    validation = _load_json(run_dir / "evaluator_validation.json")
    report = _load_json(run_dir / "historical_seen_report.json")
    package_manifest = _load_json(package_root / "package_manifest.json")
    if validation.get("status") != "PASS" or validation.get("failed_check_count") != 0:
        raise Trading2453ConstraintDiagnosisError("TRADING-2452 stored validation is not PASS")
    if manifest.get("package_id") != EXPECTED_PACKAGE_ID:
        raise Trading2453ConstraintDiagnosisError("TRADING-2452 package id mismatch")
    if package_manifest.get("package_id") != EXPECTED_PACKAGE_ID:
        raise Trading2453ConstraintDiagnosisError("current frozen package id mismatch")
    if manifest.get("runtime_binding_hash") != EXPECTED_RUNTIME_BINDING_HASH:
        raise Trading2453ConstraintDiagnosisError("TRADING-2452 runtime binding mismatch")
    if manifest.get("source_config_sha256") != EXPECTED_POLICY_SHA256:
        raise Trading2453ConstraintDiagnosisError("TRADING-2452 policy hash mismatch")
    if manifest.get("status") != "INCOMPLETE_NO_ELIGIBLE_CANDIDATE":
        raise Trading2453ConstraintDiagnosisError("unexpected TRADING-2452 result status")
    if report.get("status") != manifest.get("status"):
        raise Trading2453ConstraintDiagnosisError("report and manifest status mismatch")
    if manifest.get("train_evaluation_count") != 1800:
        raise Trading2453ConstraintDiagnosisError("formal run must contain 1800 train evaluations")
    if manifest.get("selected_count") != 0 or manifest.get("test_evaluation_count") != 0:
        raise Trading2453ConstraintDiagnosisError("formal run unexpectedly selected candidates")
    if manifest.get("recent_diagnostic_count") != 0:
        raise Trading2453ConstraintDiagnosisError("formal run unexpectedly ran recent diagnostic")
    if (
        manifest.get("prospective_holdout_accessed") is not False
        or manifest.get("candidate_expansion_performed") is not False
        or manifest.get("parameter_search_performed") is not False
        or manifest.get("production_effect") != "none"
        or manifest.get("broker_action") != "none"
    ):
        raise Trading2453ConstraintDiagnosisError("formal run safety boundary mismatch")

    output_hashes = _mapping(manifest.get("output_artifact_checksums"))
    for filename, expected_hash in output_hashes.items():
        actual = _file_sha256(run_dir / filename)
        if actual != expected_hash:
            raise Trading2453ConstraintDiagnosisError(f"formal output drift: {filename}")
        input_hashes[f"formal_output:{filename}"] = actual

    gate_source_commitment = _mapping(
        _mapping(manifest.get("runtime_source_commitments")).get("dynamic_v3_parameter_evaluator")
    )
    real_evaluation_commitment = _mapping(
        _mapping(manifest.get("runtime_source_commitments")).get("dynamic_v3_real_evaluation")
    )
    rescue_policy_commitment = _mapping(
        _mapping(package_manifest.get("selection_input_commitments")).get(
            "policy:etf_dynamic_v3_rescue_policy"
        )
    )
    if gate_source_commitment.get("sha256") != EXPECTED_GATE_SOURCE_SHA256:
        raise Trading2453ConstraintDiagnosisError("frozen gate source commitment mismatch")
    if real_evaluation_commitment.get("sha256") != EXPECTED_REAL_EVALUATION_SOURCE_SHA256:
        raise Trading2453ConstraintDiagnosisError("frozen real-evaluation source mismatch")
    if rescue_policy_commitment.get("sha256") != EXPECTED_RESCUE_POLICY_SHA256:
        raise Trading2453ConstraintDiagnosisError("frozen rescue policy mismatch")
    if _file_sha256(DEFAULT_GATE_SOURCE_PATH) != EXPECTED_GATE_SOURCE_SHA256:
        raise Trading2453ConstraintDiagnosisError("gate source drift blocks exact recomputation")
    if _file_sha256(DEFAULT_POLICY_PATH) != EXPECTED_POLICY_SHA256:
        raise Trading2453ConstraintDiagnosisError("gate policy drift blocks exact recomputation")
    if _file_sha256(DEFAULT_REAL_EVALUATION_SOURCE_PATH) != EXPECTED_REAL_EVALUATION_SOURCE_SHA256:
        raise Trading2453ConstraintDiagnosisError("real-evaluation source drift blocks S2 audit")
    if _file_sha256(DEFAULT_RESCUE_POLICY_PATH) != EXPECTED_RESCUE_POLICY_SHA256:
        raise Trading2453ConstraintDiagnosisError("rescue policy drift blocks S2 audit")

    input_hashes["source:dynamic_v3_real_evaluation.py"] = EXPECTED_REAL_EVALUATION_SOURCE_SHA256
    input_hashes["policy:dynamic_v3_constraint_aware_rescue.yaml"] = EXPECTED_RESCUE_POLICY_SHA256

    return {
        "source_run_id": DEFAULT_RUN_ID,
        "source_run_status": manifest.get("status"),
        "source_run_validation_status": validation.get("status"),
        "package_id": EXPECTED_PACKAGE_ID,
        "runtime_binding_hash": EXPECTED_RUNTIME_BINDING_HASH,
        "gate_policy_path": str(DEFAULT_POLICY_PATH),
        "gate_policy_sha256": EXPECTED_POLICY_SHA256,
        "gate_source_path": str(DEFAULT_GATE_SOURCE_PATH),
        "gate_source_sha256": EXPECTED_GATE_SOURCE_SHA256,
        "real_evaluation_source_path": str(DEFAULT_REAL_EVALUATION_SOURCE_PATH),
        "real_evaluation_source_sha256": EXPECTED_REAL_EVALUATION_SOURCE_SHA256,
        "rescue_policy_path": str(DEFAULT_RESCUE_POLICY_PATH),
        "rescue_policy_sha256": EXPECTED_RESCUE_POLICY_SHA256,
        "frozen_runtime_source_commitments": manifest.get("runtime_source_commitments"),
        "input_hashes": dict(sorted(input_hashes.items())),
        "prior_market_outcome_visibility": "KNOWN",
        "historical_replay_investigator_blind": False,
        "unbiased_oos_claim_allowed": False,
        "prospective_holdout_start": "2026-07-22",
        "prospective_holdout_accessed": False,
    }


def _build_attribution(
    *,
    recomputations: Sequence[Mapping[str, Any]],
    source_contract: Mapping[str, Any],
    config: legacy.DynamicV3ParameterSweepConfig,
    structure_checks: Mapping[str, bool],
    status: str,
) -> dict[str, Any]:
    by_fold = _group_summaries(recomputations, "window_index")
    by_template = _group_summaries(recomputations, "candidate_template")
    by_policy_hash = _group_summaries(recomputations, "policy_hash")
    reason_counts: Counter[str | None] = Counter()
    for row in recomputations:
        reasons = _records_or_scalars(_mapping(row.get("artifact")).get("gate_reasons"))
        if not reasons:
            reason_counts[None] += 1
        else:
            reason_counts.update(str(reason) for reason in reasons)
    total = len(recomputations)
    by_constraint_type = []
    for reason, count in sorted(reason_counts.items(), key=lambda item: _sort_key(item[0])):
        reason_rows = [
            row
            for row in recomputations
            if reason in _records_or_scalars(_mapping(row.get("artifact")).get("gate_reasons"))
        ]
        summary = _group_summary(reason_rows, dimension="constraint_type", key=reason)
        summary.update(
            {
                "share_of_evaluations": None if total == 0 else count / total,
                "fold_counts": _value_counts(reason_rows, "window_index"),
                "candidate_template_counts": _value_counts(reason_rows, "candidate_template"),
            }
        )
        by_constraint_type.append(summary)
    template_counts = Counter(row.get("candidate_template") for row in recomputations)
    policy_counts = Counter(row.get("policy_hash") for row in recomputations)
    reason_combo_counts = Counter(
        tuple(_records_or_scalars(_mapping(row.get("artifact")).get("gate_reasons")))
        for row in recomputations
    )
    fold_rate_unique_counts = {
        int(fold): len(
            {
                _mapping(row.get("artifact")).get("constraint_hit_rate")
                for row in recomputations
                if row.get("window_index") == fold
            }
        )
        for fold in range(1, 7)
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "trading2453_constraint_hit_attribution",
        "status": status,
        "source_contract": dict(source_contract),
        "frozen_gate_policy": {
            "policy_sha256": EXPECTED_POLICY_SHA256,
            "hard_constraints": config.hard_constraints.model_dump(mode="json"),
            "thresholds_modified": False,
        },
        "recomputation_summary": {
            "evaluation_count": total,
            "exact_match_count": sum(row.get("status") == "PASS" for row in recomputations),
            "mismatch_count": sum(row.get("status") != "PASS" for row in recomputations),
            "rejected_count": sum(
                _mapping(row.get("artifact")).get("gate") == legacy.GATE_REJECT
                for row in recomputations
            ),
            "null_selection_score_count": sum(
                _mapping(row.get("artifact")).get("selection_score") is None
                for row in recomputations
            ),
            "structure_checks": dict(structure_checks),
        },
        "aggregations": {
            "overall": _group_summary(recomputations, dimension="overall", key="all"),
            "by_fold": by_fold,
            "by_candidate_template": by_template,
            "by_policy_hash": by_policy_hash,
            "by_constraint_type": by_constraint_type,
            "concentration": {
                "candidate_template_unique_count": len(template_counts),
                "candidate_template_max_share": _max_share(template_counts, total),
                "candidate_template_hhi": _hhi(template_counts, total),
                "policy_hash_unique_count": len(policy_counts),
                "policy_hash_max_share": _max_share(policy_counts, total),
                "policy_hash_hhi": _hhi(policy_counts, total),
            },
        },
        "null_policy": {
            "rule": "null_is_missing_and_must_not_be_converted_to_zero",
            "numeric_distributions_record_present_and_null_counts": True,
        },
        "s2_semantic_audit": {
            "status": "COMPLETE",
            "primary_classification": "POLICY_ROLE_MISMATCH_REQUIRES_OWNER_REVIEW",
            "production_path_modified": False,
            "calculation_assessment": {
                "classification": "CALCULATION_MATCH",
                "conclusion": (
                    "冻结 artifact 的 hit count、hit rate、delta 与 gate reasons 均按冻结"
                    " policy 正确计算。"
                ),
                "exact_match_count": sum(row.get("status") == "PASS" for row in recomputations),
                "evaluation_count": total,
            },
            "best_candidate_design_assessment": {
                "classification": "DESIGNED_TEMPLATE_SELECTION_NOT_IMPLEMENTATION_DEFECT",
                "conclusion": (
                    "payload.best_candidate 是现行 real-evaluation 设计："
                    "_best_v0_3_candidate 只在四个 dynamic_v0_3_rescue templates 中选 best，"
                    "不是算术或实现缺陷。"
                ),
                "call_chain": [
                    "dynamic_v3_r1_evidence._summarize_fold_payload",
                    "dynamic_v3_parameter_research._metrics_from_real_evaluation_payload",
                    "payload.best_candidate",
                    "dynamic_v3_real_evaluation._best_v0_3_candidate",
                ],
                "best_candidate_filter": "group == dynamic_v0_3_rescue",
                "frozen_template_count": 4,
                "frozen_template_ids": list(V0_3_TEMPLATE_IDS),
                "source_references": [
                    {
                        "path": str(DEFAULT_REAL_EVALUATION_SOURCE_PATH),
                        "sha256": EXPECTED_REAL_EVALUATION_SOURCE_SHA256,
                        "symbols": ["_materialized_policy_set", "_best_v0_3_candidate"],
                    },
                    {
                        "path": str(DEFAULT_RESCUE_POLICY_PATH),
                        "sha256": EXPECTED_RESCUE_POLICY_SHA256,
                        "section": "candidate_templates",
                    },
                ],
            },
            "policy_role_assessment": {
                "classification": "POLICY_ROLE_MISMATCH_REQUIRES_OWNER_REVIEW",
                "frozen_result_is_correct_under_policy": True,
                "policy_source_role": "2026-06-05 small_real smoke observe-only enablement",
                "policy_comment_explicitly_not_promotion_gate": True,
                "consumed_role": "TRADING-2452 fold-train hard eligibility gate",
                "threshold": config.hard_constraints.max_constraint_hit_rate,
                "conclusion": (
                    "0.65 gate 的机械应用正确，但其来源依据与 clean-selection hard "
                    "eligibility 角色不充分匹配，必须由 owner review，而不是在本诊断中改值。"
                ),
                "source_references": [
                    {
                        "path": str(DEFAULT_POLICY_PATH),
                        "sha256": EXPECTED_POLICY_SHA256,
                        "section": "hard_constraints",
                    },
                    {
                        "path": str(DEFAULT_PACKAGE_ROOT / "selection_rule.yaml"),
                        "sha256": EXPECTED_PACKAGE_HASHES["selection_rule.yaml"],
                        "section": "selection.hard_gate_source",
                    },
                ],
            },
            "constraint_outcome_discrimination_assessment": {
                "classification": "LOW_DISCRIMINATION_STRUCTURAL_DEGENERATION_NOT_CODE_BUG",
                "conclusion": (
                    "fold 1–5 全横截面 constraint_hit_rate 相同，fold 6 仅两个 rate 档，"
                    "说明冻结参数空间对 constraint outcome 的辨识度很低或发生结构性退化；"
                    "该现象本身不构成代码缺陷证据。"
                ),
                "observed_rate_unique_count_by_fold": fold_rate_unique_counts,
                "folds_with_uniform_constraint_hit_rate": [
                    fold for fold, count in fold_rate_unique_counts.items() if count == 1
                ],
                "best_template_counts": [
                    {"candidate_template": key, "evaluation_count": count}
                    for key, count in sorted(
                        template_counts.items(), key=lambda item: _sort_key(item[0])
                    )
                ],
                "gate_reason_combo_counts": [
                    {"gate_reasons": list(combo), "evaluation_count": count}
                    for combo, count in sorted(
                        reason_combo_counts.items(), key=lambda item: tuple(map(str, item[0]))
                    )
                ],
            },
            "direct_threshold_relaxation_recommended": False,
            "default_decision": "KILL_PAUSE",
        },
        "interpretation_boundary": {
            "stage": "S0_S1_S2_S3_COMPLETE_OWNER_REVIEW_REQUIRED",
            "original_trading2452_result_changed": False,
            "threshold_relaxation_authorized": False,
            "new_hypothesis_preregistration_authorized": False,
            "owner_review_required_before_next_strategy_action": True,
        },
        "safety": dict(SAFETY),
        **SAFETY,
    }


def _build_owner_review_pack(
    *,
    source_contract: Mapping[str, Any],
    attribution: Mapping[str, Any],
    status: str,
) -> dict[str, Any]:
    semantic_audit = _mapping(attribution.get("s2_semantic_audit"))
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "trading2453_constraint_hit_owner_review_pack",
        "status": "READY_FOR_OWNER_REVIEW" if status == "PASS" else "BLOCKED",
        "source_contract": dict(source_contract),
        "default_decision": "KILL_PAUSE",
        "recommended_option_id": "A_KEEP_KILL_AND_CLOSE_CURRENT_PACKAGE",
        "decision_summary": {
            "trading2452_result_status": "INCOMPLETE_NO_ELIGIBLE_CANDIDATE",
            "calculation_classification": "CALCULATION_MATCH",
            "primary_classification": "POLICY_ROLE_MISMATCH_REQUIRES_OWNER_REVIEW",
            "best_candidate_design_is_implementation_defect": False,
            "constraint_outcome_discrimination": (
                "LOW_DISCRIMINATION_STRUCTURAL_DEGENERATION_NOT_CODE_BUG"
            ),
            "direct_threshold_relaxation_recommended": False,
        },
        "evidence": {
            "evaluation_count": _mapping(attribution.get("recomputation_summary")).get(
                "evaluation_count"
            ),
            "exact_match_count": _mapping(attribution.get("recomputation_summary")).get(
                "exact_match_count"
            ),
            "semantic_audit": semantic_audit,
            "fold_constraint_summary": _records(
                _mapping(attribution.get("aggregations")).get("by_fold")
            ),
        },
        "options": [
            {
                "option_id": "A_KEEP_KILL_AND_CLOSE_CURRENT_PACKAGE",
                "label": "保持 KILL 并结束当前 package",
                "recommended": True,
                "decision_effect": (
                    "接受 TRADING-2452 的真实负面证据，不修改 gate，不重跑当前 package。"
                ),
                "risk": "可能放弃仍有研究价值但未通过当前 gate 的参数区域。",
                "new_authorization_required": False,
                "prospective_holdout_access_allowed": False,
                "promotion_allowed": False,
            },
            {
                "option_id": "B_NEW_REVIEWED_GATE_AND_NEW_PREREGISTRATION",
                "label": "建立 reviewed clean-selection gate policy 与新预注册",
                "recommended": False,
                "decision_effect": (
                    "重新论证 hard eligibility gate 的角色与依据，创建新 policy version、"
                    "新 preregistration 与新 package 后再评估。"
                ),
                "same_package_replay_allowed": False,
                "reason_not_same_package": (
                    "改变 gate policy 会改变冻结选择规则，不能称为同 package replay。"
                ),
                "new_authorization_required": True,
                "prospective_holdout_access_allowed": False,
                "promotion_allowed": False,
            },
            {
                "option_id": "C_AUTHORIZED_TEMPLATE_AXIS_CAUSAL_DIAGNOSTIC",
                "label": "新增 per-template/per-axis causal diagnostic replay",
                "recommended": False,
                "decision_effect": (
                    "在新明确授权下检查 template 与参数轴为何缺乏 constraint outcome "
                    "辨识度，不改变现行 selection result。"
                ),
                "new_authorization_required": True,
                "new_preregistered_diagnostic_contract_required": True,
                "prospective_holdout_access_allowed": False,
                "promotion_allowed": False,
            },
        ],
        "prohibited_actions": [
            "直接放宽 max_constraint_hit_rate=0.65",
            "把修改 gate 后的运行描述为 same package replay",
            "扩展候选或启动参数搜索",
            "访问 2026-07-22 及之后 prospective holdout",
            "执行 paper-shadow、promotion、production 或 broker/order",
        ],
        "owner_decision_required": True,
        "research_only": True,
        "manual_review_required": True,
        "production_effect": "none",
        "broker_action": "none",
        "safety": dict(SAFETY),
        **SAFETY,
    }


def _render_owner_review_pack(pack: Mapping[str, Any]) -> str:
    summary = _mapping(pack.get("decision_summary"))
    evidence = _mapping(pack.get("evidence"))
    fold_rows = _records(evidence.get("fold_constraint_summary"))
    options = _records(pack.get("options"))
    lines = [
        "# TRADING-2453 Constraint-hit Owner Review Pack",
        "",
        f"- Diagnosis ID：`{pack.get('diagnosis_id')}`",
        f"- 状态：`{pack.get('status')}`",
        f"- 默认决策：`{pack.get('default_decision')}`",
        f"- 推荐选项：`{pack.get('recommended_option_id')}`",
        "- Production effect：`none`",
        "- Broker action：`none`",
        "",
        "## S2 审计结论",
        "",
        f"- 计算分类：`{summary.get('calculation_classification')}`。",
        f"- 主要分类：`{summary.get('primary_classification')}`。",
        "- `payload.best_candidate` 是四个 `dynamic_v0_3_rescue` templates 内部选优的",
        "  现行设计，不判定为算术或实现缺陷。",
        "- fold 1–5 横截面 rate 相同、fold 6 仅两个 rate 档，说明 constraint outcome",
        "  低辨识或结构性退化，但不单独构成代码缺陷证据。",
        "- 冻结 policy 下的拒绝结果正确；0.65 的来源角色与 clean-selection hard",
        "  eligibility 用途不充分匹配，需要 Owner review。",
        "- 不建议直接放宽 `max_constraint_hit_rate=0.65`。",
        "",
        "## Fold constraint-hit 分布",
        "",
        "| Fold | Evaluations | Rate min | Rate max | Rate rejects | Delta rejects |",
        "|---:|---:|---:|---:|---:|---:|",
    ]
    for row in fold_rows:
        rate = _mapping(row.get("constraint_hit_rate"))
        reasons = _mapping(row.get("constraint_reason_counts"))
        lines.append(
            f"| {row.get('key')} | {row.get('evaluation_count')} | "
            f"{rate.get('minimum')} | {rate.get('maximum')} | "
            f"{reasons.get('constraint_hit_rate_exceeds_policy', 0)} | "
            f"{reasons.get('constraint_hits_delta_exceeds_policy', 0)} |"
        )
    lines.extend(["", "## Owner 选项", ""])
    for option in options:
        recommendation = "（推荐）" if option.get("recommended") is True else ""
        lines.extend(
            [
                f"### {option.get('option_id')} {recommendation}",
                "",
                f"- {option.get('label')}。",
                f"- 影响：{option.get('decision_effect')}",
                f"- 需要新授权：`{str(option.get('new_authorization_required')).lower()}`。",
                "- Prospective holdout：禁止访问。",
                "- Promotion：不授权。",
                "",
            ]
        )
    lines.extend(
        [
            "## 禁止事项",
            "",
            *[f"- {item}" for item in pack.get("prohibited_actions", [])],
            "",
            "在 Owner 作出新决定前，默认保持 `KILL_PAUSE`。",
            "",
        ]
    )
    return "\n".join(lines)


def _group_summaries(rows: Sequence[Mapping[str, Any]], dimension: str) -> list[dict[str, Any]]:
    groups: defaultdict[object, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[row.get(dimension)].append(row)
    return [
        _group_summary(group, dimension=dimension, key=key)
        for key, group in sorted(groups.items(), key=lambda item: _sort_key(item[0]))
    ]


def _group_summary(
    rows: Sequence[Mapping[str, Any]], *, dimension: str, key: object
) -> dict[str, Any]:
    artifact_rows = [_mapping(row.get("artifact")) for row in rows]
    reason_counts: Counter[str] = Counter(
        str(reason)
        for artifact in artifact_rows
        for reason in _records_or_scalars(artifact.get("gate_reasons"))
    )
    return {
        "dimension": dimension,
        "key": key,
        "evaluation_count": len(rows),
        "exact_match_count": sum(row.get("status") == "PASS" for row in rows),
        "reject_count": sum(row.get("gate") == legacy.GATE_REJECT for row in artifact_rows),
        "constraint_reason_counts": dict(sorted(reason_counts.items())),
        "row_count": numeric_distribution_preserving_null(
            [row.get("row_count") for row in artifact_rows]
        ),
        "constraint_hits": numeric_distribution_preserving_null(
            [row.get("constraint_hits") for row in artifact_rows]
        ),
        "constraint_hit_rate": numeric_distribution_preserving_null(
            [row.get("constraint_hit_rate") for row in artifact_rows]
        ),
        "constraint_hits_delta_vs_reference": numeric_distribution_preserving_null(
            [row.get("constraint_hits_delta_vs_reference") for row in artifact_rows]
        ),
    }


def _value_counts(rows: Sequence[Mapping[str, Any]], field: str) -> list[dict[str, Any]]:
    counts = Counter(row.get(field) for row in rows)
    return [
        {"key": key, "evaluation_count": count}
        for key, count in sorted(counts.items(), key=lambda item: _sort_key(item[0]))
    ]


def _max_share(counts: Mapping[object, int], total: int) -> float | None:
    return None if total == 0 or not counts else max(counts.values()) / total


def _hhi(counts: Mapping[object, int], total: int) -> float | None:
    return None if total == 0 else sum((count / total) ** 2 for count in counts.values())


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return [dict(row) for row in value if isinstance(row, Mapping)]


def _records_or_scalars(value: object) -> list[object]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return list(value)


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _optional_int(value: object) -> int | None:
    return None if value is None else int(value)


def _optional_float(value: object) -> float | None:
    return None if value is None else float(value)


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise Trading2453ConstraintDiagnosisError(f"mapping JSON required: {path}")
    return dict(payload)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        payload = json.loads(line)
        if not isinstance(payload, Mapping):
            raise Trading2453ConstraintDiagnosisError(
                f"JSONL object required: {path}:{line_number}"
            )
        rows.append(dict(payload))
    return rows


def _jsonl_bytes(rows: Sequence[Mapping[str, Any]]) -> bytes:
    return b"".join(canonical_json_bytes(dict(row), indent=None) for row in rows)


def _file_sha256(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def _stable_hash(*values: object) -> str:
    return sha256(canonical_json_bytes({"values": values})).hexdigest()


def _sort_key(value: object) -> tuple[int, str]:
    return (0, "") if value is None else (1, str(value))


def _check(check_id: str, passed: bool, details: str | None = None) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "passed": bool(passed),
        "details": [] if details is None else [details],
    }


__all__ = [
    "DEFAULT_PACKAGE_ROOT",
    "DEFAULT_POLICY_PATH",
    "DEFAULT_RUN_DIR",
    "DEFAULT_RUN_ID",
    "SAFETY",
    "Trading2453ConstraintDiagnosisError",
    "build_trading2453_diagnosis",
    "numeric_distribution_preserving_null",
    "recompute_constraint_row",
    "validate_trading2453_diagnosis",
    "write_trading2453_diagnosis",
]
