"""Execute the single bounded schema-corrected F1 falsification run.

This V2 executor preserves the frozen V1 research calculation and changes only
the adapter into ``BootstrapInterval``: audit-only bootstrap fields remain in
the aggregate diagnostics but are not passed to the strict reducer model.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, cast

from ai_trading_system import first_layer_foundational_falsification_execution as v1
from ai_trading_system.first_layer_foundational_falsification_contract import (
    BootstrapInterval,
    FoundationalDiagnosticSummary,
    LeaveOneYearOutResult,
    load_foundational_falsification_contract,
    reduce_foundational_falsification_status,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

PROJECT_ROOT = v1.PROJECT_ROOT
DEFAULT_AUTHORIZATION_PATH = Path(
    "config/research/first_layer_composer_v2_foundational_falsification_failure_fix_run_authorization_v1.yaml"
)
DEFAULT_MANIFEST_PATH = Path(
    "inputs/research/first_layer_composer_v2_foundational_falsification_failure_fix_v1/execution_manifest.json"
)
DEFAULT_OUTPUT_DIR = Path(
    "outputs/research/first_layer_composer_v2_foundational_falsification_failure_fix_v1"
)

TASK_ID = "TRADING-2557_FIRST_LAYER_COMPOSER_V2_FOUNDATIONAL_FALSIFICATION_FAILURE_FIX_V1"
AUTHORIZATION_STATUS = "OWNER_EXACT_SCHEMA_ONLY_FAILURE_FIX_AUTHORIZED"
AUTHORIZATION_STATE = "EXACT_PREAUTHORIZED"
OWNER_DECISION_REF = (
    "owner_instruction:TRADING-2557:2026-09-03:exact_schema_only_failure_fix_1_1_1_1"
)
V1_MODULE_PATH = Path("src/ai_trading_system/first_layer_foundational_falsification_execution.py")
V1_MODULE_SHA256 = "8feb16c9328eac48c8751b2a664d21b0bcb495889653e8db352f28836444730f"
V1_MANIFEST_PATH = Path(
    "inputs/research/first_layer_composer_v2_foundational_falsification_v1/execution_manifest.json"
)
V1_MANIFEST_SHA256 = "a35124945fbfbebceb7a111dda8fe3c0d5e6edade7faa42fa931335c855a9777"
V1_RESULT_PATH = Path(
    "outputs/research/first_layer_composer_v2_foundational_falsification_v1/aggregate_result.json"
)
V1_RESULT_SHA256 = "1f0de3193b4807ed091636d4808847e9529679085d207690b2b588a8d6baaebc"
V1_RESULT_ADMISSION_PATH = Path(
    "config/research/first_layer_composer_v2_foundational_falsification_result_admission_v1.yaml"
)
V1_RESULT_ADMISSION_SHA256 = "acbdf9279e51e2e6308c7f51531afc8b553e6f0604247804e6e18c6dfac99d64"

REQUESTED_START = v1.REQUESTED_START
REQUESTED_END = v1.REQUESTED_END
EXPECTED_SESSIONS = v1.EXPECTED_SESSIONS
EXPECTED_INTERVALS = v1.EXPECTED_INTERVALS
RECONCILIATION_TOLERANCE = v1.RECONCILIATION_TOLERANCE
DIAGNOSTIC_IDS = v1.DIAGNOSTIC_IDS
INPUT_ROLES = v1.INPUT_ROLES
EXPECTED_COUNTERS = v1.EXPECTED_COUNTERS

FoundationalFalsificationExecutionError = v1.FoundationalFalsificationExecutionError
LoadedAuthorization = v1.LoadedAuthorization
InputBinding = v1.InputBinding
LoadedManifest = v1.LoadedManifest

BOOTSTRAP_INTERVAL_FIELDS = (
    "block_length_sessions",
    "percentile_2_5",
    "percentile_50",
    "percentile_97_5",
    "probability_excess_less_than_or_equal_to_zero",
)
BOOTSTRAP_AUDIT_FIELDS = ("replicates", "random_seed")


def project_bootstrap_interval(row: Mapping[str, object]) -> BootstrapInterval:
    """Project one frozen bootstrap diagnostic into the strict reducer schema."""

    expected_fields = set(BOOTSTRAP_INTERVAL_FIELDS + BOOTSTRAP_AUDIT_FIELDS)
    if set(row) != expected_fields:
        raise FoundationalFalsificationExecutionError(
            "F1_SCHEMA_ADAPTER_INPUT_DRIFT",
            f"bootstrap fields={sorted(row)} expected={sorted(expected_fields)}",
        )
    v1._expect(row["replicates"], 10_000, "bootstrap.replicates")
    v1._expect(row["random_seed"], 2555, "bootstrap.random_seed")
    return BootstrapInterval.model_validate(
        {field: row[field] for field in BOOTSTRAP_INTERVAL_FIELDS}
    )


def _validate_v1_failure_binding(
    payload: Mapping[str, Any], *, project_root: Path, label: str
) -> None:
    binding = v1._mapping(payload, label)
    expected = {
        "v1_module": (V1_MODULE_PATH, V1_MODULE_SHA256),
        "v1_manifest": (V1_MANIFEST_PATH, V1_MANIFEST_SHA256),
        "v1_terminal_result": (V1_RESULT_PATH, V1_RESULT_SHA256),
        "v1_result_admission": (V1_RESULT_ADMISSION_PATH, V1_RESULT_ADMISSION_SHA256),
    }
    if set(binding) != set(expected):
        raise FoundationalFalsificationExecutionError(
            "F1_IDENTITY_MISMATCH", f"{label} keys drifted"
        )
    for name, (path, sha256) in expected.items():
        item = v1._mapping(binding.get(name), f"{label}.{name}")
        v1._expect(item.get("path"), path.as_posix(), f"{label}.{name}.path")
        bound = v1._bound_file(path, root=project_root, label=f"{label}.{name}")
        v1._expect(v1._sha256_path(bound), sha256, f"{label}.{name}.sha256")
        v1._expect(item.get("sha256"), sha256, f"{label}.{name}.declared_sha256")


def load_run_authorization(
    path: Path = DEFAULT_AUTHORIZATION_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> LoadedAuthorization:
    resolved = v1._bound_file(path, root=project_root, label="authorization")
    raw = resolved.read_bytes()
    payload = v1._mapping(
        load_strict_yaml_text(raw.decode("utf-8"), label=path.as_posix()), "authorization"
    )
    v1._expect(
        payload.get("schema_version"),
        "first_layer_composer_v2_foundational_falsification_failure_fix_run_authorization.v1",
        "authorization.schema_version",
    )
    v1._expect(payload.get("authorization_id"), path.stem, "authorization.authorization_id")
    v1._expect(payload.get("authorization_version"), "1.0.0", "authorization.version")
    v1._expect(payload.get("status"), AUTHORIZATION_STATUS, "authorization.status")
    v1._expect(payload.get("task_id"), TASK_ID, "authorization.task_id")
    v1._expect(payload.get("scope"), "R1_BOUNDED_RESEARCH_SANDBOX", "authorization.scope")
    owner = v1._mapping(payload.get("owner_decision"), "authorization.owner_decision")
    v1._expect(owner.get("decision_ref"), OWNER_DECISION_REF, "owner_decision.decision_ref")
    v1._expect(owner.get("authorization_state"), AUTHORIZATION_STATE, "authorization_state")
    v1._expect(owner.get("exact_bounded_run_granted"), True, "exact_bounded_run_granted")
    _validate_v1_failure_binding(
        v1._mapping(payload.get("v1_failure_binding"), "authorization.v1_failure_binding"),
        project_root=project_root,
        label="authorization.v1_failure_binding",
    )
    f0 = v1._mapping(payload.get("f0_binding"), "authorization.f0_binding")
    v1._expect(f0.get("exact_main_commit"), v1.F0_EXACT_MAIN, "f0.exact_main_commit")
    v1._expect(f0.get("file_sha256"), v1.F0_FILE_SHA256, "f0.file_sha256")
    v1._expect(f0.get("canonical_sha256"), v1.F0_CANONICAL_SHA256, "f0.canonical_sha256")
    v1._expect(
        f0.get("authority_set_sha256"), v1.F0_AUTHORITY_SET_SHA256, "f0.authority_set_sha256"
    )
    loaded_f0 = load_foundational_falsification_contract(project_root=project_root)
    v1._expect(loaded_f0.policy_file_sha256, v1.F0_FILE_SHA256, "loaded_f0.file_sha256")
    v1._expect(loaded_f0.policy_canonical_sha256, v1.F0_CANONICAL_SHA256, "loaded_f0.canonical")
    v1._expect(loaded_f0.authority_set_sha256, v1.F0_AUTHORITY_SET_SHA256, "loaded_f0.authorities")
    v1._expect(
        v1._mapping(payload.get("run_envelope"), "run_envelope"),
        EXPECTED_COUNTERS,
        "run_envelope",
    )
    boundary = v1._mapping(payload.get("result_boundary"), "result_boundary")
    v1._expect(boundary.get("aggregate_result_only"), True, "aggregate_result_only")
    v1._expect(boundary.get("raw_market_payload_export_allowed"), False, "raw_market_export")
    v1._expect(boundary.get("raw_signal_payload_export_allowed"), False, "raw_signal_export")
    safety = v1._mapping(payload.get("safety"), "safety")
    for field in (
        "outcome_access_authorized",
        "market_data_read_authorized",
        "manifest_replay_authorized",
        "canonical_dq_authorized",
        "local_foundational_run_authorized",
        "independent_replay_authorized",
    ):
        v1._expect(safety.get(field), True, f"safety.{field}")
    for field in (
        "data_download_authorized",
        "cache_mutation_authorized",
        "quantconnect_authorized",
        "option_data_use_authorized",
        "option_backtest_authorized",
        "provider_authorized",
        "paper_allowed",
        "live_allowed",
        "production_allowed",
        "broker_allowed",
    ):
        v1._expect(safety.get(field), False, f"safety.{field}")
    v1._expect(safety.get("production_effect"), "none", "safety.production_effect")
    v1._expect(safety.get("broker_action"), "none", "safety.broker_action")
    allowlist = v1._mapping(payload.get("input_allowlist"), "input_allowlist")
    v1._expect(tuple(allowlist), INPUT_ROLES, "input_allowlist.role_order")
    return LoadedAuthorization(
        payload=payload,
        path=resolved,
        file_sha256=v1._sha256_bytes(raw),
        canonical_sha256=v1._sha256_bytes(v1._canonical_json_bytes(payload)),
    )


def load_execution_manifest(
    path: Path = DEFAULT_MANIFEST_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
    authorization: LoadedAuthorization | None = None,
) -> LoadedManifest:
    auth = authorization or load_run_authorization(project_root=project_root)
    resolved = v1._bound_file(path, root=project_root, label="manifest")
    raw = resolved.read_bytes()
    payload = v1._mapping(json.loads(raw), "manifest")
    v1._expect(
        payload.get("schema_version"),
        "first_layer_composer_v2_foundational_falsification_failure_fix_execution_manifest.v1",
        "manifest.schema_version",
    )
    v1._expect(payload.get("manifest_id"), path.parent.name, "manifest.manifest_id")
    v1._expect(payload.get("task_id"), TASK_ID, "manifest.task_id")
    v1._expect(payload.get("status"), "FROZEN_READY_FOR_SINGLE_DISPATCH", "manifest.status")
    auth_binding = v1._mapping(payload.get("authorization_binding"), "authorization_binding")
    v1._expect(auth_binding.get("path"), DEFAULT_AUTHORIZATION_PATH.as_posix(), "auth.path")
    v1._expect(auth_binding.get("file_sha256"), auth.file_sha256, "auth.file_sha256")
    v1._expect(auth_binding.get("canonical_sha256"), auth.canonical_sha256, "auth.canonical")
    _validate_v1_failure_binding(
        v1._mapping(payload.get("v1_failure_binding"), "manifest.v1_failure_binding"),
        project_root=project_root,
        label="manifest.v1_failure_binding",
    )
    v1._expect(
        v1._mapping(payload.get("run_envelope"), "manifest.run_envelope"),
        EXPECTED_COUNTERS,
        "run_envelope",
    )
    for field, expected in (
        ("requested_start", REQUESTED_START.isoformat()),
        ("requested_end", REQUESTED_END.isoformat()),
        ("evaluated_start", REQUESTED_START.isoformat()),
        ("evaluated_end", REQUESTED_END.isoformat()),
        ("expected_signal_sessions", EXPECTED_SESSIONS),
        ("expected_return_intervals", EXPECTED_INTERVALS),
    ):
        v1._expect(payload.get(field), expected, f"manifest.{field}")
    code = v1._mapping(payload.get("code_binding"), "manifest.code_binding")
    v1._expect(
        code.get("module_path"),
        "src/ai_trading_system/first_layer_foundational_falsification_execution_v2.py",
        "code.module_path",
    )
    module = v1._bound_file(
        str(code.get("module_path", "")), root=project_root, label="code.module"
    )
    v1._expect(v1._sha256_path(module), code.get("module_sha256"), "code.module_sha256")
    if len(str(code.get("implementation_commit_sha", ""))) != 40:
        raise FoundationalFalsificationExecutionError(
            "F1_IDENTITY_MISMATCH", "implementation_commit_sha must be a full SHA"
        )
    raw_bindings = payload.get("input_bindings")
    if not isinstance(raw_bindings, list):
        raise FoundationalFalsificationExecutionError("F1_SCHEMA_INVALID", "input_bindings")
    bindings: list[InputBinding] = []
    for raw_binding in raw_bindings:
        binding = v1._mapping(raw_binding, "input_binding")
        if set(binding) != {"role", "path", "sha256", "size_bytes"}:
            raise FoundationalFalsificationExecutionError(
                "F1_SCHEMA_INVALID", "input binding keys drifted"
            )
        bindings.append(
            InputBinding(
                role=str(binding["role"]),
                path=str(binding["path"]),
                sha256=str(binding["sha256"]),
                size_bytes=int(binding["size_bytes"]),
            )
        )
    v1._expect(tuple(item.role for item in bindings), INPUT_ROLES, "manifest.input_roles")
    allowlist = v1._mapping(auth.payload.get("input_allowlist"), "authorization.input_allowlist")
    for parsed_binding in bindings:
        allowed = v1._mapping(
            allowlist.get(parsed_binding.role), f"allowlist.{parsed_binding.role}"
        )
        v1._expect(parsed_binding.path, allowed.get("path"), f"input.{parsed_binding.role}.path")
        v1._expect(
            parsed_binding.sha256, allowed.get("sha256"), f"input.{parsed_binding.role}.sha256"
        )
        v1._expect(
            parsed_binding.size_bytes,
            allowed.get("size_bytes"),
            f"input.{parsed_binding.role}.size",
        )
    return LoadedManifest(
        payload=payload,
        path=resolved,
        file_sha256=v1._sha256_bytes(raw),
        canonical_sha256=v1._sha256_bytes(v1._canonical_json_bytes(payload)),
        inputs=tuple(bindings),
    )


def replay_execution_manifest(
    manifest: LoadedManifest, *, project_root: Path = PROJECT_ROOT
) -> Mapping[str, object]:
    replay = dict(v1.replay_execution_manifest(manifest, project_root=project_root))
    replay["schema_version"] = (
        "first_layer_foundational_falsification_failure_fix_manifest_replay.v1"
    )
    replay["v1_failure_binding"] = dict(manifest.payload["v1_failure_binding"])
    return replay


def _failure_result(
    *, code: str, message: str, counters: Mapping[str, int], runtime_git_head: str
) -> Mapping[str, object]:
    return {
        "schema_version": (
            "first_layer_composer_v2_foundational_falsification_failure_fix_result.v1"
        ),
        "task_id": TASK_ID,
        "status": "TERMINAL",
        "foundational_status": "INVALID",
        "conclusion": "FOUNDATIONAL_EVIDENCE_INVALID",
        "reason_codes": [code],
        "failure": {"code": code, "message": message},
        "v1_failure_binding": {
            "terminal_result_path": V1_RESULT_PATH.as_posix(),
            "terminal_result_sha256": V1_RESULT_SHA256,
        },
        "schema_adaptation": {
            "reducer_fields": list(BOOTSTRAP_INTERVAL_FIELDS),
            "audit_only_fields": list(BOOTSTRAP_AUDIT_FIELDS),
        },
        "requested_range": {"start": REQUESTED_START.isoformat(), "end": REQUESTED_END.isoformat()},
        "evaluated_range": None,
        "runtime_git_head": runtime_git_head,
        "actual_counters": dict(counters),
        "authorization_state": AUTHORIZATION_STATE,
        "aggregate_result_only": True,
        "raw_market_payload_exported": False,
        "raw_signal_payload_exported": False,
        "qqq_options_wave_b": "HOLD",
        "qqq_options_wave_c": "NOT_AUTHORIZED",
        "production_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
        "orders": 0,
        "fills": 0,
        "positions": 0,
    }


def execute_foundational_falsification(
    manifest_path: Path = DEFAULT_MANIFEST_PATH,
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    project_root: Path = PROJECT_ROOT,
) -> Mapping[str, object]:
    root = project_root.resolve()
    target = (root / output_dir).resolve(strict=False)
    target.relative_to(root)
    attempt_path = target / "run_attempt_consumption_receipt.json"
    result_path = target / "aggregate_result.json"
    if attempt_path.exists() or result_path.exists():
        raise FoundationalFalsificationExecutionError("F1_ATTEMPT_ALREADY_CONSUMED", str(target))
    counters = {key: 0 for key in EXPECTED_COUNTERS}
    runtime_git_head = v1._git_head(root)
    v1._write_once(
        attempt_path,
        {
            "schema_version": "first_layer_foundational_falsification_failure_fix_attempt.v1",
            "task_id": TASK_ID,
            "status": "DISPATCHED_SINGLE_ATTEMPT_RESERVED",
            "authorization_state": AUTHORIZATION_STATE,
            "authorized_maxima": EXPECTED_COUNTERS,
            "runtime_git_head": runtime_git_head,
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    try:
        authorization = load_run_authorization(project_root=root)
        manifest = load_execution_manifest(
            manifest_path, project_root=root, authorization=authorization
        )
        counters["manifest_replays"] = 1
        replay = replay_execution_manifest(manifest, project_root=root)
        v1._write_once(target / "manifest_replay_receipt.json", replay)
        bindings = v1._bindings(manifest)
        counters["canonical_dq_runs"] = 1
        dq = v1.run_canonical_data_quality_execution(
            v1.CanonicalDataQualityExecutionRequest(
                as_of=REQUESTED_END,
                requested_window=v1.DataQualityDateWindow(start=REQUESTED_START, end=REQUESTED_END),
                evaluated_window=v1.DataQualityDateWindow(start=REQUESTED_START, end=REQUESTED_END),
                prices_path=Path(bindings["canonical_prices"].path),
                rates_path=Path(bindings["canonical_rates"].path),
                manifest_path=Path(bindings["canonical_download_manifest"].path),
                secondary_prices_path=Path(bindings["canonical_secondary_prices"].path),
                require_secondary_prices=True,
                expected_price_tickers=("QQQ", "SGOV", "TQQQ"),
                expected_rate_series=("DGS10", "DGS2", "DTWEXBGS"),
                policy_path=Path(bindings["data_quality_policy"].path),
            ),
            project_root=root,
        )
        dq_receipt = {
            "schema_version": "first_layer_foundational_falsification_failure_fix_dq_receipt.v1",
            "status": dq.report.status,
            "canonical_dq_receipt_path": dq.receipt_path.relative_to(root).as_posix(),
            "canonical_dq_receipt_sha256": v1._sha256_path(dq.receipt_path),
            "canonical_dq_report_path": dq.report_path.relative_to(root).as_posix(),
            "canonical_dq_report_sha256": v1._sha256_path(dq.report_path),
            "requested_start": REQUESTED_START.isoformat(),
            "requested_end": REQUESTED_END.isoformat(),
            "evaluated_start": dq.receipt.evaluated_window.start.isoformat(),
            "evaluated_end": dq.receipt.evaluated_window.end.isoformat(),
            "error_count": dq.report.error_count,
            "warning_count": dq.report.warning_count,
        }
        v1._write_once(target / "canonical_dq_receipt.json", dq_receipt)
        if dq.report.status != "PASS":
            raise FoundationalFalsificationExecutionError("F1_DQ_OR_PIT_NOT_PASS", dq.report.status)

        counters["local_foundational_runs"] = 1
        contract = load_foundational_falsification_contract(project_root=root)
        plan = v1.load_diagnostic_plan(manifest, project_root=root)
        candidate = v1.candidate_return_path(
            plan.qqq_prices, plan.interval_targets, one_way_cost_bps=5.0
        )
        comparator = v1.comparator_return_path(
            plan.qqq_prices, plan.comparator_weight, one_way_cost_bps=5.0
        )
        old_candidate = v1.calculate_candidate_primary(plan.qqq_prices, plan.interval_targets)
        old_comparator = v1.calculate_static_comparator_primary(
            plan.qqq_prices, plan.comparator_weight
        )
        counters["independent_replays"] = 1
        reconciliation = {
            "candidate_final_value_abs_diff": abs(
                candidate.final_value - old_candidate.final_value
            ),
            "comparator_final_value_abs_diff": abs(
                comparator.final_value - old_comparator.final_value
            ),
            "candidate_compound_abs_diff_pp": abs(
                v1._compound(candidate.interval_returns) - candidate.net_total_return_pct
            ),
            "comparator_compound_abs_diff_pp": abs(
                v1._compound(comparator.interval_returns) - comparator.net_total_return_pct
            ),
        }
        if any(value > RECONCILIATION_TOLERANCE for value in reconciliation.values()):
            raise FoundationalFalsificationExecutionError(
                "F1_INDEPENDENT_REPLAY_NOT_PASS", json.dumps(reconciliation, sort_keys=True)
            )
        independent = {
            "schema_version": (
                "first_layer_foundational_falsification_failure_fix_independent_replay.v1"
            ),
            "status": "PASS",
            "tolerance": RECONCILIATION_TOLERANCE,
            "reconciliation": reconciliation,
        }
        v1._write_once(target / "independent_replay_receipt.json", independent)

        years = v1.calendar_year_attribution(plan, candidate, comparator)
        episodes = v1.contiguous_episode_attribution(plan)
        leave_out = v1.leave_one_year_out(plan, candidate, comparator)
        bootstrap = v1.paired_circular_moving_block_bootstrap(
            candidate.interval_returns, comparator.interval_returns
        )
        source_diff = v1.source_revision_diff(manifest, project_root=root)
        policy_consumption = v1._policy_consumption(contract.policy)
        primary_excess = candidate.net_total_return_pct - comparator.net_total_return_pct
        summary = FoundationalDiagnosticSummary(
            completed_diagnostic_ids=DIAGNOSTIC_IDS,
            policy_consumption_matches_contract=bool(policy_consumption["matches_contract"]),
            source_revision_status=cast(Any, source_diff["status"]),
            primary_paired_excess_percentage_points=primary_excess,
            bootstrap_intervals=tuple(project_bootstrap_interval(row) for row in bootstrap),
            leave_one_calendar_year_out=tuple(
                LeaveOneYearOutResult(
                    calendar_year=cast(Any, row["excluded_calendar_year"]),
                    paired_excess_percentage_points=cast(
                        Any, row["paired_excess_percentage_points"]
                    ),
                )
                for row in leave_out
            ),
        )
        decision = reduce_foundational_falsification_status(summary, policy=contract.policy)
        result: Mapping[str, object] = {
            "schema_version": (
                "first_layer_composer_v2_foundational_falsification_failure_fix_result.v1"
            ),
            "task_id": TASK_ID,
            "status": "TERMINAL",
            "foundational_status": decision.status,
            "conclusion": decision.conclusion,
            "reason_codes": list(decision.reason_codes),
            "authorization_state": AUTHORIZATION_STATE,
            "historical_window_role": "REUSED_DEVELOPMENT_CONFIRMATION",
            "pristine_out_of_sample_claim": False,
            "requested_range": {
                "start": REQUESTED_START.isoformat(),
                "end": REQUESTED_END.isoformat(),
            },
            "evaluated_range": {
                "start": plan.sessions[0].isoformat(),
                "end": plan.sessions[-1].isoformat(),
            },
            "signal_session_count": len(plan.sessions),
            "return_interval_count": len(plan.interval_targets),
            "signal_lag_sessions": 1,
            "long_interval_count": plan.long_interval_count,
            "exposure_matched_comparator_weight": plan.comparator_weight,
            "action_counts": dict(plan.action_counts),
            "primary_5_bps": {
                "candidate_net_total_return_pct": candidate.net_total_return_pct,
                "candidate_max_drawdown_magnitude_pct": candidate.max_drawdown_magnitude_pct,
                "comparator_net_total_return_pct": comparator.net_total_return_pct,
                "comparator_max_drawdown_magnitude_pct": comparator.max_drawdown_magnitude_pct,
                "paired_excess_percentage_points": primary_excess,
            },
            "schema_adaptation": {
                "reducer_fields": list(BOOTSTRAP_INTERVAL_FIELDS),
                "audit_only_fields": list(BOOTSTRAP_AUDIT_FIELDS),
                "audit_values": {"replicates": 10_000, "random_seed": 2555},
            },
            "v1_failure_binding": dict(manifest.payload["v1_failure_binding"]),
            "diagnostics": {
                "policy_consumption_inventory": policy_consumption,
                "calendar_year_attribution": years,
                "contiguous_episode_attribution": {
                    "episode_count": len(episodes),
                    "episodes": episodes,
                },
                "leave_one_calendar_year_out": leave_out,
                "paired_moving_block_bootstrap": bootstrap,
                "cost_sensitivity": v1.cost_sensitivity(plan),
                "sgov_carry_sensitivity": v1.sgov_carry_sensitivity(plan),
                "state_transition_attribution": v1.state_transition_attribution(plan),
                "selection_history_inventory": v1._selection_history(contract.policy),
                "source_revision_diff": source_diff,
            },
            "completed_diagnostic_ids": list(DIAGNOSTIC_IDS),
            "manifest_file_sha256": manifest.file_sha256,
            "manifest_canonical_sha256": manifest.canonical_sha256,
            "authorization_file_sha256": authorization.file_sha256,
            "authorization_canonical_sha256": authorization.canonical_sha256,
            "f0_file_sha256": contract.policy_file_sha256,
            "canonical_dq": dq_receipt,
            "independent_replay": independent,
            "runtime_git_head": runtime_git_head,
            "actual_counters": counters,
            "aggregate_result_only": True,
            "raw_market_payload_exported": False,
            "raw_signal_payload_exported": False,
            "qqq_options_wave_b": decision.qqq_options_wave_b,
            "qqq_options_wave_c": decision.qqq_options_wave_c,
            "production_allowed": False,
            "data_downloads": 0,
            "cache_mutations": 0,
            "quantconnect_actions": 0,
            "option_backtests": 0,
            "external_provider_actions": 0,
            "production_effect": "none",
            "broker_action": "none",
            "orders": 0,
            "fills": 0,
            "positions": 0,
        }
        v1._write_once(result_path, result)
        return result
    except Exception as exc:
        if isinstance(exc, FoundationalFalsificationExecutionError):
            code, message = exc.code, exc.message
        else:
            code, message = "F1_UNEXPECTED_FAILURE", str(exc)
        failure = _failure_result(
            code=code, message=message, counters=counters, runtime_git_head=runtime_git_head
        )
        v1._write_once(target / "failure_receipt.json", failure)
        v1._write_once(result_path, failure)
        return failure


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = execute_foundational_falsification(
        args.manifest, output_dir=args.output_dir, project_root=args.project_root
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result.get("foundational_status") in {"FAIL", "INSUFFICIENT", "PASS"} else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = [
    "AUTHORIZATION_STATE",
    "BOOTSTRAP_AUDIT_FIELDS",
    "BOOTSTRAP_INTERVAL_FIELDS",
    "DEFAULT_AUTHORIZATION_PATH",
    "DEFAULT_MANIFEST_PATH",
    "DEFAULT_OUTPUT_DIR",
    "EXPECTED_COUNTERS",
    "TASK_ID",
    "execute_foundational_falsification",
    "load_execution_manifest",
    "load_run_authorization",
    "project_bootstrap_interval",
    "replay_execution_manifest",
]
