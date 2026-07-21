from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from hashlib import sha256
from itertools import product
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.contracts.research_context import (
    ResearchContextError,
    ResearchEvaluationContext,
)
from ai_trading_system.contracts.research_lifecycle import (
    ResearchLifecycleError,
    ResearchPreregistration,
)
from ai_trading_system.platform.artifacts.writer import canonical_json_bytes, write_json_atomic
from ai_trading_system.research_campaign import CampaignSpec
from ai_trading_system.yaml_loader import safe_load_yaml_path

SCHEMA_VERSION = "dynamic_v3_clean_selection_trading2452_package.v1"
ELIGIBILITY_SCHEMA_VERSION = "dynamic_v3_clean_selection_trading2452_eligibility.v1"
ELIGIBLE_STATUS = "ELIGIBLE_FOR_OWNER_AUTHORIZED_HISTORICAL_SEEN_RUN"
PACKAGE_LOGICAL_ROOT = Path("inputs/research/trading2452_dynamic_v3_clean_selection")
DEFAULT_PACKAGE_ROOT = PROJECT_ROOT / PACKAGE_LOGICAL_ROOT
LEGACY_PACKAGE_LOGICAL_ROOT = Path("inputs/research/trading2451_dynamic_v3_clean_selection")
DEFAULT_LEGACY_PACKAGE_ROOT = PROJECT_ROOT / LEGACY_PACKAGE_LOGICAL_ROOT
SELECTION_RULE_FILENAME = "selection_rule.yaml"
WINDOW_CATALOG_FILENAME = "window_catalog.yaml"
GENERATED_FILENAMES = (
    "candidate_universe.json",
    "research_context.json",
    "preregistration.json",
    "campaign.json",
    "source_contract.json",
    "eligibility.json",
    "package_manifest.json",
)
LEGACY_IMMUTABLE_FILENAMES = (
    "selection_rule.yaml",
    "window_catalog.yaml",
    "candidate_universe.json",
    "research_context.json",
    "preregistration.json",
    "campaign.json",
    "source_contract.json",
    "eligibility.json",
    "package_manifest.json",
)

# TRADING-2452 owner-frozen policy anchors. These are protocol fingerprints, not
# tunable heuristics. Any byte drift requires a new versioned preregistration.
FROZEN_SELECTION_RULE_SHA256 = "895f6208fcf13013b2457580625b9207d12b942607aadf56e809ff7c47f1aabd"
FROZEN_WINDOW_CATALOG_SHA256 = "19a0fffc6deb468bb53a104a528bdecc23b9ff9072967174548b835dfd1169b5"

# Identifier digest lengths are schema-format constants only.
PACKAGE_ID_DIGEST_LENGTH = 20
CANDIDATE_UNIVERSE_ID_DIGEST_LENGTH = 20
CANDIDATE_ID_DIGEST_LENGTH = 16

SAFETY: dict[str, Any] = {
    "research_only": True,
    "manual_review_required": True,
    "candidate_expansion_allowed": False,
    "new_parameter_search_allowed": False,
    "evaluator_execution_allowed": True,
    "clean_run_authorized": True,
    "historical_replay_execution_allowed": True,
    "recent_known_diagnostic_allowed": True,
    "prospective_holdout_access_allowed": False,
    "paper_shadow_change_allowed": False,
    "production_weight_change_allowed": False,
    "unbiased_oos_claim_allowed": False,
    "production_effect": "none",
    "broker_action": "none",
}

_SOURCE_POLICIES = (
    (
        "primary_research_window_policy_v1",
        "research_window",
        Path("config/research/primary_research_window_policy.yaml"),
    ),
    (
        "market_regime_registry_v1",
        "market_regime",
        Path("config/market_regimes.yaml"),
    ),
    (
        "project_data_quality_policy",
        "data_quality",
        Path("config/data_quality.yaml"),
    ),
    (
        "etf_dynamic_v3_real_evaluation_policy_v1",
        "strategy",
        Path("config/etf_portfolio/dynamic_v3_real_evaluation.yaml"),
    ),
    (
        "etf_dynamic_v3_rescue_policy",
        "strategy",
        Path("config/etf_portfolio/dynamic_v3_constraint_aware_rescue.yaml"),
    ),
    (
        "etf_dynamic_robustness_policy",
        "threshold",
        Path("config/etf_portfolio/dynamic_robustness.yaml"),
    ),
    (
        "etf_dynamic_allocation_policy",
        "strategy",
        Path("config/etf_portfolio/dynamic_allocation_policy.yaml"),
    ),
    (
        "etf_dynamic_failure_diagnostics_policy",
        "threshold",
        Path("config/etf_portfolio/dynamic_failure_diagnostics.yaml"),
    ),
    (
        "etf_asset_policy",
        "strategy",
        Path("config/etf_portfolio/assets.yaml"),
    ),
    (
        "etf_risk_policy",
        "execution",
        Path("config/etf_portfolio/risk.yaml"),
    ),
    (
        "etf_strategy_policy",
        "execution",
        Path("config/etf_portfolio/strategy.yaml"),
    ),
    (
        "project_universe_policy",
        "data_quality",
        Path("config/universe.yaml"),
    ),
)


class DynamicV3Trading2452Error(ValueError):
    """Raised when the TRADING-2452 frozen package cannot be built safely."""


def build_trading2452_package(
    *,
    package_root: Path = DEFAULT_PACKAGE_ROOT,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, dict[str, Any]]:
    selection_path = package_root / SELECTION_RULE_FILENAME
    windows_path = package_root / WINDOW_CATALOG_FILENAME
    _validate_frozen_policy_files(selection_path=selection_path, windows_path=windows_path)
    selection = _load_yaml_mapping(selection_path)
    windows = _load_yaml_mapping(windows_path)
    _validate_frozen_specs(selection=selection, windows=windows)

    candidate_cfg = _mapping(selection.get("candidate_universe"))
    source_config_path = _project_path(project_root, candidate_cfg.get("source_config"))
    profile_config_path = _project_path(project_root, candidate_cfg.get("source_profile_config"))
    source_config = _load_yaml_mapping(source_config_path)
    profiles = _load_yaml_mapping(profile_config_path)
    _validate_candidate_sources(
        selection=selection,
        source_config=source_config,
        profiles=profiles,
    )
    source_commitments = _source_commitments(
        project_root=project_root,
        package_root=package_root,
        selection_path=selection_path,
        windows_path=windows_path,
        source_config_path=source_config_path,
        profile_config_path=profile_config_path,
    )
    policy_refs = _policy_refs(project_root)
    candidate_universe = _candidate_universe(
        selection=selection,
        source_config=source_config,
        source_commitments=source_commitments,
    )
    context = _research_context(windows=windows, policy_refs=policy_refs)
    preregistration = _preregistration(
        selection=selection,
        candidate_universe=candidate_universe,
        context=context,
        policy_refs=policy_refs,
        selection_rule_sha256=_file_sha256(selection_path),
    )
    campaign = _campaign(
        selection=selection,
        windows=windows,
        candidate_universe=candidate_universe,
        context=context,
        preregistration=preregistration,
    )
    source_contract = _source_contract(
        selection=selection,
        candidate_universe=candidate_universe,
        preregistration=preregistration,
        context=context,
        campaign=campaign,
        selection_rule_sha256=_file_sha256(selection_path),
    )
    payloads: dict[str, dict[str, Any]] = {
        "candidate_universe.json": candidate_universe,
        "research_context.json": context,
        "preregistration.json": preregistration,
        "campaign.json": campaign,
        "source_contract.json": source_contract,
    }
    artifact_checksums = {
        filename: _payload_sha256(payload) for filename, payload in payloads.items()
    }
    package_id = (
        "dynamic-v3-clean-trading2452_"
        + _stable_hash(
            source_commitments,
            artifact_checksums,
            selection.get("frozen_at"),
        )[:PACKAGE_ID_DIGEST_LENGTH]
    )
    eligibility = _eligibility(
        package_id=package_id,
        candidate_universe=candidate_universe,
        preregistration=preregistration,
        context=context,
        campaign=campaign,
        windows=windows,
    )
    payloads["eligibility.json"] = eligibility
    artifact_checksums["eligibility.json"] = _payload_sha256(eligibility)
    payloads["package_manifest.json"] = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "dynamic_v3_clean_selection_trading2452_package_manifest",
        "package_id": package_id,
        "status": "PASS",
        "eligibility_status": ELIGIBLE_STATUS,
        "generated_at": selection.get("frozen_at"),
        "selection_input_commitments": source_commitments,
        "historical_immutable_evidence": _legacy_immutable_evidence(project_root),
        "legacy_immutable_evidence_selection_allowed": False,
        "output_artifact_checksums": artifact_checksums,
        "result_artifacts_consumed": [],
        "result_artifact_count": 0,
        "next_responsible_party": "owner_authorized_historical_seen_evaluator",
        "safety": dict(SAFETY),
        **SAFETY,
    }
    return payloads


def write_trading2452_package(
    *,
    package_root: Path = DEFAULT_PACKAGE_ROOT,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    package_root.mkdir(parents=True, exist_ok=True)
    payloads = build_trading2452_package(
        package_root=package_root,
        project_root=project_root,
    )
    for filename in GENERATED_FILENAMES:
        write_json_atomic(package_root / filename, payloads[filename])
    manifest = payloads["package_manifest.json"]
    return {
        "status": manifest["status"],
        "package_id": manifest["package_id"],
        "eligibility_status": manifest["eligibility_status"],
        "package_root": str(package_root),
        "production_effect": "none",
        "broker_action": "none",
    }


def validate_trading2452_package(
    *,
    package_root: Path = DEFAULT_PACKAGE_ROOT,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    package_id: str | None = None
    eligibility_status = "BLOCKED_INVALID_TRADING2452_PACKAGE"
    try:
        actual = {
            filename: _load_json_mapping(package_root / filename)
            for filename in GENERATED_FILENAMES
        }
        expected = build_trading2452_package(
            package_root=package_root,
            project_root=project_root,
        )
        manifest = actual["package_manifest.json"]
        package_id = str(manifest.get("package_id", "")) or None
        checks.extend(
            [
                _check("manifest_schema", manifest.get("schema_version") == SCHEMA_VERSION),
                _check("manifest_status", manifest.get("status") == "PASS"),
                _check(
                    "result_artifacts_not_consumed",
                    manifest.get("result_artifacts_consumed") == []
                    and manifest.get("result_artifact_count") == 0,
                ),
                _check("manifest_safety", manifest.get("safety") == SAFETY),
                _check(
                    "legacy_immutable_evidence_is_non_selection",
                    manifest.get("legacy_immutable_evidence_selection_allowed") is False
                    and bool(manifest.get("historical_immutable_evidence")),
                ),
                _check(
                    "selection_input_commitments_fresh",
                    _commitments_fresh(
                        _mapping(manifest.get("selection_input_commitments")),
                        project_root=project_root,
                        package_root=package_root,
                    ),
                ),
            ]
        )
        output_checksums = _mapping(manifest.get("output_artifact_checksums"))
        for filename in GENERATED_FILENAMES:
            if filename == "package_manifest.json":
                continue
            checks.append(
                _check(
                    f"output_checksum:{filename}",
                    output_checksums.get(filename) == _file_sha256(package_root / filename),
                )
            )
        for filename in GENERATED_FILENAMES:
            checks.append(
                _check(
                    f"content_recomputed:{filename}",
                    _json_equivalent(actual[filename], expected[filename]),
                )
            )
        candidates = _records(actual["candidate_universe.json"].get("candidates"))
        candidate_ids = [str(item.get("candidate_id", "")) for item in candidates]
        source_contract = actual["source_contract.json"]
        eligibility = actual["eligibility.json"]
        context = ResearchEvaluationContext.from_dict(actual["research_context.json"])
        preregistration = ResearchPreregistration.from_dict(actual["preregistration.json"])
        campaign = CampaignSpec.model_validate(actual["campaign.json"])
        active_payload_text = json.dumps(
            [
                actual["candidate_universe.json"],
                actual["research_context.json"],
                actual["preregistration.json"],
                actual["campaign.json"],
                actual["source_contract.json"],
                actual["eligibility.json"],
            ],
            ensure_ascii=False,
            sort_keys=True,
        )
        checks.extend(
            [
                _check("candidate_count_exact", len(candidates) == 300),
                _check(
                    "candidate_ids_unique",
                    len(candidate_ids) == len(set(candidate_ids)) and all(candidate_ids),
                ),
                _check(
                    "candidate_origin_preregistered",
                    actual["candidate_universe.json"].get("candidate_universe_origin")
                    == "preregistered_candidate_universe",
                ),
                _check(
                    "selection_lineage_excludes_results_and_legacy_package",
                    not _forbidden_selection_lineage(actual, manifest),
                ),
                _check(
                    "active_package_excludes_legacy_2022_boundary",
                    "2022-12-01" not in active_payload_text,
                ),
                _check(
                    "context_uses_unified_primary_window",
                    context.research_window_start == date(2021, 2, 22)
                    and context.regime_start == date(2021, 2, 22),
                ),
                _check(
                    "context_runtime_dq_blocker_explicit",
                    context.status.value == "BLOCKED"
                    and context.blocking_issues
                    == ("RUNTIME_DATA_QUALITY_GATE_REQUIRED_BEFORE_EVALUATOR",),
                ),
                _check(
                    "canonical_ids_cross_linked",
                    preregistration.research_context_id == context.context_id
                    and campaign.metadata.get("research_context_id") == context.context_id
                    and source_contract.get("preregistration_id")
                    == preregistration.preregistration_id,
                ),
                _check(
                    "result_visibility_none",
                    preregistration.result_visibility.value == "NONE",
                ),
                _check(
                    "owner_historical_run_authorized_only",
                    campaign.owner_authorized_holdout is False
                    and campaign.metadata.get("clean_run_authorized") is True
                    and campaign.metadata.get("prospective_holdout_accessed") is False,
                ),
                _check(
                    "eligibility_schema",
                    eligibility.get("schema_version") == ELIGIBILITY_SCHEMA_VERSION,
                ),
                _check("eligibility_safety", eligibility.get("safety") == SAFETY),
                _check(
                    "historical_claim_scope",
                    eligibility.get("prior_market_outcome_visibility") == "KNOWN"
                    and eligibility.get("historical_replay_investigator_blind") is False
                    and eligibility.get("unbiased_oos_claim_allowed") is False,
                ),
                _check(
                    "six_historical_folds_and_recent_diagnostic",
                    eligibility.get("historical_fold_count") == 6
                    and eligibility.get("main_historical_end") == "2025-12-31"
                    and eligibility.get("recent_known_diagnostic", {}).get(
                        "included_in_main_fold_ranking"
                    )
                    is False,
                ),
            ]
        )
        if checks and all(item["passed"] for item in checks):
            eligibility_status = ELIGIBLE_STATUS
    except (
        DynamicV3Trading2452Error,
        ResearchContextError,
        ResearchLifecycleError,
        ValidationError,
        ValueError,
        TypeError,
        OSError,
        json.JSONDecodeError,
    ) as exc:
        checks.append(_check("package_recomputation", False, str(exc)))
    status = "PASS" if checks and all(item["passed"] for item in checks) else "FAIL"
    return {
        "schema_version": "dynamic_v3_clean_selection_trading2452_validation.v1",
        "report_type": "dynamic_v3_clean_selection_trading2452_validation",
        "status": status,
        "package_id": package_id,
        "eligibility_status": eligibility_status,
        "failed_check_count": sum(not item["passed"] for item in checks),
        "checks": checks,
        "next_responsible_party": (
            "owner_authorized_historical_seen_evaluator"
            if status == "PASS"
            else "research_protocol_owner_repair_preregistration"
        ),
        "safety": dict(SAFETY),
        **SAFETY,
    }


def _validate_frozen_policy_files(*, selection_path: Path, windows_path: Path) -> None:
    expected = {
        selection_path: FROZEN_SELECTION_RULE_SHA256,
        windows_path: FROZEN_WINDOW_CATALOG_SHA256,
    }
    for path, expected_sha256 in expected.items():
        if _file_sha256_optional(path) != expected_sha256:
            raise DynamicV3Trading2452Error(
                f"TRADING-2452 frozen policy fingerprint mismatch: {path.name}"
            )


def _validate_frozen_specs(*, selection: Mapping[str, Any], windows: Mapping[str, Any]) -> None:
    if selection.get("schema_version") != "dynamic_v3_clean_selection_rule.v2":
        raise DynamicV3Trading2452Error("selection rule schema mismatch")
    if windows.get("schema_version") != "dynamic_v3_clean_selection_window_catalog.v2":
        raise DynamicV3Trading2452Error("window catalog schema mismatch")
    if selection.get("status") != "owner_frozen_preregistration":
        raise DynamicV3Trading2452Error("selection rule is not owner frozen")
    if selection.get("frozen_at") != windows.get("frozen_at"):
        raise DynamicV3Trading2452Error("freeze timestamps differ")
    if _mapping(selection.get("safety")) != SAFETY:
        raise DynamicV3Trading2452Error("selection safety boundary mismatch")
    window_safety = _mapping(windows.get("safety"))
    for key in (
        "evaluator_execution_allowed",
        "clean_run_authorized",
        "historical_replay_execution_allowed",
        "recent_known_diagnostic_allowed",
        "prospective_holdout_access_allowed",
        "unbiased_oos_claim_allowed",
        "production_effect",
        "broker_action",
    ):
        if window_safety.get(key) != SAFETY[key]:
            raise DynamicV3Trading2452Error(f"window safety mismatch: {key}")
    selection_cfg = _mapping(selection.get("selection"))
    candidate_cfg = _mapping(selection.get("candidate_universe"))
    top_n = selection_cfg.get("top_n")
    max_candidates = candidate_cfg.get("max_candidates")
    if (
        selection_cfg.get("evidence_phase") != "train_only"
        or isinstance(top_n, bool)
        or not isinstance(top_n, int)
        or top_n <= 0
        or isinstance(max_candidates, bool)
        or not isinstance(max_candidates, int)
        or max_candidates < top_n
        or selection_cfg.get("test_metric_selection_allowed") is not False
        or selection_cfg.get("rejected_candidate_backfill_allowed") is not False
        or selection_cfg.get("legacy_candidate_backfill_allowed") is not False
    ):
        raise DynamicV3Trading2452Error("train-only selection boundary mismatch")
    _validate_score_rule(selection_cfg)
    execution = _mapping(selection.get("execution"))
    row_floors = _mapping(execution.get("evidence_row_floors"))
    if (
        isinstance(row_floors.get("minimum_train_rows"), bool)
        or not isinstance(row_floors.get("minimum_train_rows"), int)
        or row_floors.get("minimum_train_rows", 0) <= 0
        or isinstance(row_floors.get("minimum_test_rows"), bool)
        or not isinstance(row_floors.get("minimum_test_rows"), int)
        or row_floors.get("minimum_test_rows", 0) <= 0
        or not str(row_floors.get("rationale", "")).strip()
    ):
        raise DynamicV3Trading2452Error("evidence row floor governance mismatch")
    _validate_windows(selection=selection, windows=windows)


def _validate_score_rule(selection_cfg: Mapping[str, Any]) -> None:
    score = _mapping(selection_cfg.get("score"))
    if score.get("objective") != "constrained_weighted_score":
        raise DynamicV3Trading2452Error("score objective boundary mismatch")
    for section in ("weights", "normalization", "robustness_mapping", "penalties"):
        values = _mapping(score.get(section))
        if not values or any(
            isinstance(value, bool) or not isinstance(value, (int, float))
            for value in values.values()
        ):
            raise DynamicV3Trading2452Error(f"score {section} must be numeric")


def _validate_windows(*, selection: Mapping[str, Any], windows: Mapping[str, Any]) -> None:
    primary = _mapping(windows.get("primary_research_window"))
    replay = _mapping(windows.get("historical_protocol_replay"))
    recent = _mapping(windows.get("recent_known_diagnostic"))
    holdout = _mapping(windows.get("prospective_holdout"))
    execution = _mapping(selection.get("execution"))
    folds = _records(replay.get("folds"))
    if (
        primary.get("window_id") != "exact_three_asset_validated"
        or primary.get("start_date") != "2021-02-22"
        or primary.get("active_default") is not True
        or primary.get("legacy_2022_boundary_consumption_allowed") is not False
    ):
        raise DynamicV3Trading2452Error("primary research window mismatch")
    if (
        len(folds) != 6
        or replay.get("main_scoring_end") != "2025-12-31"
        or replay.get("selection_uses_only_train") is not True
        or replay.get("test_metrics_available_to_selection") is not False
        or replay.get("prior_market_outcome_visibility") != "KNOWN"
        or replay.get("investigator_blind") is not False
        or replay.get("purge_trading_days") != execution.get("purge_trading_days")
        or replay.get("embargo_trading_days") != execution.get("embargo_trading_days")
    ):
        raise DynamicV3Trading2452Error("historical fold catalog boundary mismatch")
    recent_range = _date_range(recent.get("start"), recent.get("end"))
    holdout_range = _date_range(holdout.get("start"), holdout.get("end"))
    freeze_date = datetime.fromisoformat(str(selection.get("frozen_at"))).date()
    if (
        recent.get("prior_market_outcome_visibility") != "KNOWN"
        or recent.get("included_in_main_fold_ranking") is not False
        or recent.get("candidate_selection_allowed") is not False
        or recent_range[1] >= holdout_range[0]
        or recent_range[1] > freeze_date
    ):
        raise DynamicV3Trading2452Error("recent diagnostic boundary mismatch")
    if (
        holdout.get("start") != "2026-07-22"
        or holdout.get("outcome_visibility_at_freeze") != "NONE"
        or holdout.get("access") != "OWNER_AUTHORIZATION_REQUIRED"
        or holdout.get("accessed_by_this_task") is not False
        or holdout_range[0] <= freeze_date
    ):
        raise DynamicV3Trading2452Error("prospective holdout boundary mismatch")
    for index, fold in enumerate(folds, start=1):
        if fold.get("window_index") != index:
            raise DynamicV3Trading2452Error("fold ordering changed")
        train = _date_range(fold.get("train_start"), fold.get("train_end"))
        test = _date_range(fold.get("test_start"), fold.get("test_end"))
        if (
            train[0] != date(2021, 2, 22)
            or train[1] >= test[0]
            or _ranges_overlap(train, recent_range)
            or _ranges_overlap(test, recent_range)
            or _ranges_overlap(train, holdout_range)
            or _ranges_overlap(test, holdout_range)
        ):
            raise DynamicV3Trading2452Error(f"fold {index} overlaps selection boundary")
    if folds[-1].get("test_end") != "2025-12-31":
        raise DynamicV3Trading2452Error("historical main folds do not end at 2025-12-31")


def _validate_candidate_sources(
    *,
    selection: Mapping[str, Any],
    source_config: Mapping[str, Any],
    profiles: Mapping[str, Any],
) -> None:
    candidate_cfg = _mapping(selection.get("candidate_universe"))
    max_candidates = candidate_cfg.get("max_candidates")
    profile = _mapping(
        _mapping(profiles.get("profiles")).get(str(candidate_cfg.get("source_profile", "")))
    )
    parameter_space = _mapping(source_config.get("parameter_space"))
    if (
        isinstance(max_candidates, bool)
        or not isinstance(max_candidates, int)
        or max_candidates <= 0
        or profile.get("max_candidates") != max_candidates
        or profile.get("evaluator_mode") != "real_dynamic_v3_rescue"
    ):
        raise DynamicV3Trading2452Error("candidate source profile mismatch")
    axes = [str(item) for item in _sequence(candidate_cfg.get("axis_order"))]
    if axes != list(parameter_space):
        raise DynamicV3Trading2452Error("parameter axis order drift")
    if candidate_cfg.get("derivation") != "deterministic_cartesian_prefix":
        raise DynamicV3Trading2452Error("candidate derivation drift")


def _candidate_universe(
    *,
    selection: Mapping[str, Any],
    source_config: Mapping[str, Any],
    source_commitments: Mapping[str, Any],
) -> dict[str, Any]:
    candidate_cfg = _mapping(selection.get("candidate_universe"))
    parameter_space = _mapping(source_config.get("parameter_space"))
    axes = [str(item) for item in _sequence(candidate_cfg.get("axis_order"))]
    values = [_sequence(_mapping(parameter_space.get(axis)).get("values")) for axis in axes]
    version = str(candidate_cfg.get("candidate_definition_version"))
    family = str(candidate_cfg.get("strategy_family"))
    limit = int(candidate_cfg.get("max_candidates", 0))
    candidates = []
    for combination in product(*values):
        parameters = dict(zip(axes, combination, strict=True))
        candidate_id = _stable_hash(
            {
                "candidate_definition_version": version,
                "strategy_family": family,
                "parameters": parameters,
            }
        )[:CANDIDATE_ID_DIGEST_LENGTH]
        candidates.append(
            {
                "candidate_id": candidate_id,
                "candidate_definition_version": version,
                "strategy_family": family,
                "parameters": parameters,
            }
        )
        if len(candidates) == limit:
            break
    if len(candidates) != limit:
        raise DynamicV3Trading2452Error("candidate universe row count mismatch")
    universe_id = (
        "dynamic-v3-clean-universe-trading2452_"
        + _stable_hash(
            version,
            axes,
            candidates,
            {
                key: _mapping(value).get("sha256")
                for key, value in source_commitments.items()
                if key in {"parameter_sweep_config", "sweep_profile_config"}
            },
        )[:CANDIDATE_UNIVERSE_ID_DIGEST_LENGTH]
    )
    return {
        "schema_version": "dynamic_v3_clean_candidate_universe.v2",
        "candidate_universe_id": universe_id,
        "candidate_universe_origin": "preregistered_candidate_universe",
        "derivation": candidate_cfg.get("derivation"),
        "axis_order": axes,
        "candidate_count": len(candidates),
        "result_artifacts_consumed": [],
        "result_artifact_count": 0,
        "source_commitments": {
            key: value
            for key, value in source_commitments.items()
            if key in {"parameter_sweep_config", "sweep_profile_config"}
        },
        "candidates": candidates,
        "safety": dict(SAFETY),
    }


def _research_context(
    *, windows: Mapping[str, Any], policy_refs: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    primary = _mapping(windows.get("primary_research_window"))
    holdout = _mapping(windows.get("prospective_holdout"))
    frozen_date = datetime.fromisoformat(str(windows.get("frozen_at"))).date().isoformat()
    payload = {
        "schema_version": "research_evaluation_context.v1",
        "status": "BLOCKED",
        "market_regime_id": "unified_primary_2021",
        "regime_anchor": primary.get("start_date"),
        "regime_start": primary.get("start_date"),
        "research_window_id": primary.get("window_id"),
        "research_window_start": primary.get("start_date"),
        "window_role": primary.get("role"),
        "evidence_role": primary.get("evidence_role"),
        "requested_range": {
            "start": primary.get("start_date"),
            "end": holdout.get("end"),
        },
        "actual_data_range": None,
        "effective_feature_start": None,
        "effective_prediction_start": None,
        "actual_portfolio_start": None,
        "evaluation_range": None,
        "as_of": frozen_date,
        "trading_calendar": "XNYS",
        "effective_coverage": None,
        "data_quality": {
            "contract_id": "runtime_data_quality_gate_required_before_trading106",
            "status": "PENDING_RUNTIME_VALIDATION",
            "passed": False,
            "as_of": frozen_date,
            "policy_ref_id": "project_data_quality_policy",
            "report_path": None,
            "report_sha256": None,
        },
        "caveats": [
            "historical_market_outcomes_known_at_preregistration",
            "recent_2026_segment_is_diagnostic_only",
            "prospective_holdout_not_accessed",
            "unbiased_oos_claim_not_allowed",
        ],
        "policy_refs": list(policy_refs),
        "blocking_issues": ["RUNTIME_DATA_QUALITY_GATE_REQUIRED_BEFORE_EVALUATOR"],
    }
    return ResearchEvaluationContext.from_dict(payload).to_dict()


def _preregistration(
    *,
    selection: Mapping[str, Any],
    candidate_universe: Mapping[str, Any],
    context: Mapping[str, Any],
    policy_refs: Sequence[Mapping[str, Any]],
    selection_rule_sha256: str,
) -> dict[str, Any]:
    selection_cfg = _mapping(selection.get("selection"))
    preregistration = ResearchPreregistration(
        hypothesis_id="dynamic-v3-clean-selection-trading2452",
        hypothesis_statement=(
            "A fixed 300-parameter Dynamic v3 universe may retain reviewable risk "
            "behavior from 2021-02-22 through 2025-12-31 when each fold selects the "
            "train-only top 20; historical outcomes are known and cannot support an "
            "unbiased OOS claim."
        ),
        owner="project_owner",
        baseline_id="dynamic_v0_4_and_static_baseline_contract_v1",
        candidate_id=str(candidate_universe.get("candidate_universe_id")),
        research_context_id=str(context.get("context_id")),
        selection_rule_id=str(selection.get("policy_id")),
        selection_rule_sha256=selection_rule_sha256,
        metric_ids=tuple(str(item) for item in _sequence(selection_cfg.get("required_metrics"))),
        policy_ref_ids=tuple(str(item.get("policy_id")) for item in policy_refs),
        validation_plan_ids=tuple(
            str(item) for item in _sequence(selection.get("validation_plan_ids"))
        ),
        frozen_at=datetime.fromisoformat(str(selection.get("frozen_at"))),
    )
    return preregistration.to_dict()


def _campaign(
    *,
    selection: Mapping[str, Any],
    windows: Mapping[str, Any],
    candidate_universe: Mapping[str, Any],
    context: Mapping[str, Any],
    preregistration: Mapping[str, Any],
) -> dict[str, Any]:
    replay = _mapping(windows.get("historical_protocol_replay"))
    recent = _mapping(windows.get("recent_known_diagnostic"))
    holdout = _mapping(windows.get("prospective_holdout"))
    folds = _records(replay.get("folds"))
    selection_cfg = _mapping(selection.get("selection"))
    campaign_cfg = _mapping(selection.get("campaign"))
    evidence_budget = _mapping(campaign_cfg.get("evidence_budget"))
    evidence_budget["compute_budget"] = (
        f"{candidate_universe.get('candidate_count')} train evaluations plus at most "
        f"{selection_cfg.get('top_n')} test evaluations per each of {len(folds)} frozen folds"
    )
    campaign = CampaignSpec.model_validate(
        {
            "schema_version": "research_campaign.v1",
            "campaign_id": "dynamic-v3-clean-selection-trading2452",
            "program_id": "dynamic-v3-evidence-closure",
            "title": "Dynamic v3 2021-primary historical-seen fold-local evaluation",
            "market_regime": "exact_three_asset_validated",
            "requested_date_range": (
                f"2021-02-22..{replay.get('main_scoring_end')} historical seen; "
                f"{recent.get('start')}..{recent.get('end')} recent-known diagnostic; "
                f"{holdout.get('start')}..{holdout.get('end')} untouched prospective"
            ),
            "hypothesis": {
                "statement": preregistration.get("hypothesis_statement"),
                "expected_gain": ["protocol_clean_fold_local_historical_evidence"],
                "expected_failure_modes": [
                    "no_train_eligible_candidate",
                    "negative_test_evidence",
                    "regime_or_stress_concentration",
                    "runtime_data_quality_failure",
                ],
            },
            "module_graph": {
                "baseline": "dynamic_v0_4_and_static_baseline_contract_v1",
                "modules": ["dynamic_v3_rescue"],
                "allowed_mechanisms": ["train_only_fold_local_ranking"],
                "forbidden_mechanisms": [
                    "full_period_ranking",
                    "test_metric_selection",
                    "legacy_leaderboard_backfill",
                    "trading2451_package_as_selection_input",
                    "prospective_holdout_access",
                ],
                "allowed_interaction_order": campaign_cfg.get("allowed_interaction_order"),
            },
            "window_policy": {
                "development_catalog": replay.get("catalog_id"),
                "diagnostic_catalog": recent.get("catalog_id"),
                "holdout_catalog": holdout.get("catalog_id"),
                "holdout_access": "OWNER_AUTHORIZATION_REQUIRED",
            },
            "scorecard_policy": selection.get("policy_id"),
            "evidence_budget": evidence_budget,
            "stop_rules": [
                {"code": "RUNTIME_DATA_QUALITY_GATE_FAILS"},
                {"code": "SOURCE_OR_POLICY_CHECKSUM_DRIFT"},
                {"code": "FOLD_HAS_NO_ELIGIBLE_TRAIN_CANDIDATE"},
                {"code": "TEST_METRIC_ENTERS_SELECTION"},
                {"code": "PROSPECTIVE_HOLDOUT_ACCESSED"},
            ],
            "safety": {
                "research_only": True,
                "manual_review_only": True,
                "official_target_weights": False,
                "paper_shadow_allowed": False,
                "broker_effect": "none",
                "order_effect": "none",
                "production_effect": "none",
            },
            "owner_authorized_holdout": False,
            "metadata": {
                "clean_selection_preregistration_id": preregistration.get("preregistration_id"),
                "research_context_id": context.get("context_id"),
                "candidate_universe_id": candidate_universe.get("candidate_universe_id"),
                "protocol_clean": True,
                "prior_market_outcome_visibility": "KNOWN",
                "historical_replay_investigator_blind": False,
                "unbiased_oos_claim_allowed": False,
                "clean_run_authorized": True,
                "evaluator_executed": False,
                "prospective_holdout_accessed": False,
                "execution_policy": _mapping(selection.get("execution")),
            },
        }
    )
    return campaign.model_dump(mode="json")


def _source_contract(
    *,
    selection: Mapping[str, Any],
    candidate_universe: Mapping[str, Any],
    preregistration: Mapping[str, Any],
    context: Mapping[str, Any],
    campaign: Mapping[str, Any],
    selection_rule_sha256: str,
) -> dict[str, Any]:
    return {
        "schema_version": "dynamic_v3_clean_selection_source_contract.v2",
        "candidate_universe_id": candidate_universe.get("candidate_universe_id"),
        "candidate_universe_path": str(PACKAGE_LOGICAL_ROOT / "candidate_universe.json"),
        "candidate_universe_sha256": _payload_sha256(candidate_universe),
        "candidate_universe_origin": "preregistered_candidate_universe",
        "selection_rule_id": selection.get("policy_id"),
        "selection_rule_path": str(PACKAGE_LOGICAL_ROOT / SELECTION_RULE_FILENAME),
        "selection_rule_sha256": selection_rule_sha256,
        "preregistration_id": preregistration.get("preregistration_id"),
        "preregistration_path": str(PACKAGE_LOGICAL_ROOT / "preregistration.json"),
        "research_context_id": context.get("context_id"),
        "research_context_path": str(PACKAGE_LOGICAL_ROOT / "research_context.json"),
        "campaign_id": campaign.get("campaign_id"),
        "campaign_spec_path": str(PACKAGE_LOGICAL_ROOT / "campaign.json"),
        "selected_after_result_visibility": False,
        "first_selected_result_at": None,
        "first_selected_result_state": "NOT_GENERATED",
        "result_artifacts_consumed": [],
        "result_artifact_count": 0,
        "legacy_package_selection_allowed": False,
        "safety": dict(SAFETY),
    }


def _eligibility(
    *,
    package_id: str,
    candidate_universe: Mapping[str, Any],
    preregistration: Mapping[str, Any],
    context: Mapping[str, Any],
    campaign: Mapping[str, Any],
    windows: Mapping[str, Any],
) -> dict[str, Any]:
    replay = _mapping(windows.get("historical_protocol_replay"))
    recent = _mapping(windows.get("recent_known_diagnostic"))
    holdout = _mapping(windows.get("prospective_holdout"))
    return {
        "schema_version": ELIGIBILITY_SCHEMA_VERSION,
        "report_type": "dynamic_v3_clean_selection_trading2452_eligibility",
        "package_id": package_id,
        "status": ELIGIBLE_STATUS,
        "eligibility_status": ELIGIBLE_STATUS,
        "candidate_universe_id": candidate_universe.get("candidate_universe_id"),
        "candidate_count": candidate_universe.get("candidate_count"),
        "preregistration_id": preregistration.get("preregistration_id"),
        "research_context_id": context.get("context_id"),
        "campaign_id": campaign.get("campaign_id"),
        "historical_fold_count": len(_records(replay.get("folds"))),
        "main_historical_end": replay.get("main_scoring_end"),
        "recent_known_diagnostic": {
            "catalog_id": recent.get("catalog_id"),
            "start": recent.get("start"),
            "end": recent.get("end"),
            "included_in_main_fold_ranking": False,
        },
        "prospective_holdout": {
            "catalog_id": holdout.get("catalog_id"),
            "start": holdout.get("start"),
            "end": holdout.get("end"),
            "access": holdout.get("access"),
            "accessed": False,
        },
        "protocol_clean": True,
        "prior_market_outcome_visibility": "KNOWN",
        "historical_replay_investigator_blind": False,
        "runtime_data_quality_required": True,
        "runtime_data_snapshot_bound": False,
        "result_visibility": "NONE",
        "unbiased_oos_claim_allowed": False,
        "owner_historical_run_authorized": True,
        "next_responsible_party": "owner_authorized_historical_seen_evaluator",
        "safety": dict(SAFETY),
        **SAFETY,
    }


def _source_commitments(
    *,
    project_root: Path,
    package_root: Path,
    selection_path: Path,
    windows_path: Path,
    source_config_path: Path,
    profile_config_path: Path,
) -> dict[str, Any]:
    commitments = {
        "selection_rule": _commitment(
            selection_path, PACKAGE_LOGICAL_ROOT / SELECTION_RULE_FILENAME
        ),
        "window_catalog": _commitment(windows_path, PACKAGE_LOGICAL_ROOT / WINDOW_CATALOG_FILENAME),
        "parameter_sweep_config": _commitment(
            source_config_path, source_config_path.relative_to(project_root)
        ),
        "sweep_profile_config": _commitment(
            profile_config_path, profile_config_path.relative_to(project_root)
        ),
        "package_builder": _commitment(Path(__file__), Path(__file__).relative_to(project_root)),
    }
    for policy_id, _role, relative_path in _SOURCE_POLICIES:
        commitments[f"policy:{policy_id}"] = _commitment(
            project_root / relative_path, relative_path
        )
    return commitments


def _policy_refs(project_root: Path) -> list[dict[str, Any]]:
    return [
        {
            "policy_id": policy_id,
            "role": role,
            "version": "live_at_trading2452_freeze",
            "status": "owner_frozen_source_commitment",
            "path": str(relative_path).replace("\\", "/"),
            "sha256": _file_sha256(project_root / relative_path),
        }
        for policy_id, role, relative_path in _SOURCE_POLICIES
    ]


def _legacy_immutable_evidence(project_root: Path) -> dict[str, Any]:
    root = project_root / LEGACY_PACKAGE_LOGICAL_ROOT
    artifacts = {
        filename: {
            "path": str(LEGACY_PACKAGE_LOGICAL_ROOT / filename).replace("\\", "/"),
            "sha256": _file_sha256(root / filename),
            "size": (root / filename).stat().st_size,
        }
        for filename in LEGACY_IMMUTABLE_FILENAMES
    }
    return {
        "package_role": "HISTORICAL_IMMUTABLE_PREREGISTRATION_EVIDENCE_ONLY",
        "selection_input": False,
        "active_policy_source": False,
        "artifact_count": len(artifacts),
        "artifacts": artifacts,
    }


def _forbidden_selection_lineage(
    actual: Mapping[str, Mapping[str, Any]], manifest: Mapping[str, Any]
) -> list[str]:
    values = []
    universe = actual["candidate_universe.json"]
    source_contract = actual["source_contract.json"]
    for commitment in _mapping(universe.get("source_commitments")).values():
        values.append(str(_mapping(commitment).get("path", "")))
    for name, commitment in _mapping(manifest.get("selection_input_commitments")).items():
        if str(name).startswith("policy:"):
            continue
        values.append(str(_mapping(commitment).get("path", "")))
    for key in (
        "candidate_universe_path",
        "selection_rule_path",
        "preregistration_path",
        "research_context_path",
        "campaign_spec_path",
    ):
        values.append(str(source_contract.get(key, "")))
    forbidden = (
        "leaderboard",
        "candidate_results",
        "candidate_report",
        "real_evaluation",
        "fold_evaluations",
        "top_eligible_candidates",
        "trading2451_dynamic_v3_clean_selection",
    )
    return sorted({value for value in values if any(token in value.lower() for token in forbidden)})


def _commitments_fresh(
    commitments: Mapping[str, Any], *, project_root: Path, package_root: Path
) -> bool:
    if not commitments:
        return False
    for raw in commitments.values():
        item = _mapping(raw)
        logical = Path(str(item.get("path", "")))
        if logical.parts[: len(PACKAGE_LOGICAL_ROOT.parts)] == PACKAGE_LOGICAL_ROOT.parts:
            path = package_root / Path(*logical.parts[len(PACKAGE_LOGICAL_ROOT.parts) :])
        else:
            path = project_root / logical
        if item.get("sha256") != _file_sha256_optional(path) or item.get("size") != (
            path.stat().st_size if path.is_file() else None
        ):
            return False
    return True


def _commitment(path: Path, logical_path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise DynamicV3Trading2452Error(f"source file missing: {path}")
    return {
        "path": str(logical_path).replace("\\", "/"),
        "sha256": _file_sha256(path),
        "size": path.stat().st_size,
    }


def _project_path(project_root: Path, value: object) -> Path:
    path = Path(str(value or ""))
    if path.is_absolute() or ".." in path.parts:
        raise DynamicV3Trading2452Error(f"project-relative path required: {value}")
    resolved = (project_root / path).resolve(strict=False)
    if project_root.resolve(strict=False) not in (resolved, *resolved.parents):
        raise DynamicV3Trading2452Error(f"path escapes project root: {value}")
    return resolved


def _date_range(start: object, end: object) -> tuple[date, date]:
    try:
        result = (date.fromisoformat(str(start)), date.fromisoformat(str(end)))
    except ValueError as exc:
        raise DynamicV3Trading2452Error("invalid date range") from exc
    if result[0] > result[1]:
        raise DynamicV3Trading2452Error("reversed date range")
    return result


def _ranges_overlap(left: tuple[date, date], right: tuple[date, date]) -> bool:
    return left[0] <= right[1] and right[0] <= left[1]


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, Mapping):
        raise DynamicV3Trading2452Error(f"mapping YAML required: {path}")
    return dict(payload)


def _load_json_mapping(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise DynamicV3Trading2452Error(f"mapping JSON required: {path}")
    return dict(payload)


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _sequence(value: object) -> list[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return list(value)


def _payload_sha256(payload: Mapping[str, Any]) -> str:
    return sha256(canonical_json_bytes(payload)).hexdigest()


def _file_sha256(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def _file_sha256_optional(path: Path) -> str | None:
    return _file_sha256(path) if path.is_file() else None


def _stable_hash(*values: object) -> str:
    raw = json.dumps(
        values,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(raw).hexdigest()


def _json_equivalent(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    return json.dumps(left, sort_keys=True, separators=(",", ":")) == json.dumps(
        right, sort_keys=True, separators=(",", ":")
    )


def _check(check_id: str, passed: bool, details: str | None = None) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "passed": bool(passed),
        "details": [] if details is None else [details],
    }


__all__ = [
    "DEFAULT_PACKAGE_ROOT",
    "DynamicV3Trading2452Error",
    "ELIGIBLE_STATUS",
    "SAFETY",
    "build_trading2452_package",
    "validate_trading2452_package",
    "write_trading2452_package",
]
