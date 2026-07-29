from __future__ import annotations

import hashlib
import json
import math
import platform
import re
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final, NoReturn

import numpy as np
import pandas as pd

from ai_trading_system.data.o1_relative_opportunity_dq_candidate import (
    validate_o1_dq_gate,
)
from ai_trading_system.data.o1_relative_opportunity_event_attempt_ledger import (
    validate_o1_event_attempt_freeze_gate,
)
from ai_trading_system.platform.artifacts import (
    canonical_json_bytes,
    sha256_path,
    write_json_atomic,
)
from ai_trading_system.research_framework.plugins.decision_target_capability_audit_model_ladder import (  # noqa: E501
    _build_feature_frame,
)
from ai_trading_system.research_framework.plugins.o1_relative_opportunity_capability_audit import (  # noqa: E501
    _build_fold_ledger,
    _selected_feature_ids,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

COVERAGE_REPORT_SCHEMA: Final = "o1_relative_opportunity_coverage_report.v1"
COVERAGE_GATE_SCHEMA: Final = "o1_relative_opportunity_coverage_gate.v1"
PASS_STATUS: Final = "PASS_COVERAGE_ONLY_GATE"
BLOCKED_STATUS: Final = "BLOCKED_INSUFFICIENT_COVERAGE_OR_DQ"
BLOCKED_CLASS: Final = "INSUFFICIENT_COVERAGE_OR_DQ"
TASK_ID: Final = "TRADING-2464"
POLICY_TASK_ID: Final = (
    "TRADING-2464_O1_RELATIVE_OPPORTUNITY_SPREAD_CAPABILITY_AUDIT"
)
POLICY_SCHEMA: Final = "o1_relative_opportunity_capability_audit_policy.v1"
POLICY_ID: Final = "TRADING_2464_O1_CAPABILITY_AUDIT_V1"
OWNER_DECISION: Final = (
    "owner_decision:TRADING-2464:2026-07-30:"
    "approve_o1_m1_ridge_cross_asset_state_single_family_v1"
)
MODEL_ID: Final = "M1_RIDGE_LINEAR"
FEATURE_FAMILY: Final = "CROSS_ASSET_STATE"
ATTEMPT_FAMILY_ID: Final = "O1_M1_RIDGE_CROSS_ASSET_STATE_V1"
OUTPUT_DIRECTORY_NAME: Final = "o1_coverage_only_v1"
DEFAULT_POLICY_PATH: Final = Path(
    "config/research/o1_relative_opportunity_capability_audit_v1.yaml"
)
_INPUT_PATHS: Final = {
    "prices": "data/raw/prices_daily.csv",
    "rates": "data/raw/rates_daily.csv",
    "secondary_prices": "data/raw/prices_marketstack_daily.csv",
}
_REQUIRED_TICKERS: Final = ("QQQ", "SGOV", "SPY")
_HEX_40: Final = re.compile(r"^[0-9a-f]{40}$")
_HEX_64: Final = re.compile(r"^[0-9a-f]{64}$")


class O1CoverageError(RuntimeError):
    """Typed fail-closed error raised before or during coverage-only execution."""

    def __init__(self, code: str, message: str, *, path: Path | None = None) -> None:
        self.code = code
        self.message = message
        self.path = path
        location = "" if path is None else f" [{path}]"
        super().__init__(f"{code}{location}: {message}")


@dataclass(frozen=True)
class O1CoverageResult:
    report: Mapping[str, object]
    gate: Mapping[str, object]
    report_path: Path
    gate_path: Path


@dataclass(frozen=True)
class _BoundInputs:
    policy: Mapping[str, object]
    policy_path: Path
    policy_sha256: str
    historical_policy: Mapping[str, object]
    historical_policy_path: Path
    panel: pd.DataFrame
    event_rows: tuple[Mapping[str, object], ...]
    evidence: Mapping[str, object]
    input_commitment: Mapping[str, object]
    dq_summary: Mapping[str, object]


def run_o1_coverage_only(
    *,
    output_root: Path,
    project_root: Path,
    generated_at: datetime,
    audit_policy_path: Path = DEFAULT_POLICY_PATH,
    source_commit_sha: str | None = None,
    cli_argv: Sequence[str] = (),
) -> O1CoverageResult:
    """Run one immutable coverage-only audit without fitting or scoring a model."""

    root = project_root.resolve(strict=True)
    policy_path = _contained_file(root, audit_policy_path, "O1_COVERAGE_POLICY_MISSING")
    policy = _mapping(safe_load_yaml_path(policy_path), "policy")
    _validate_policy_contract(policy)
    expected_output = (
        Path(
            _text(
                _mapping(policy["isolated_dq_evidence"], "isolated_dq_evidence")[
                    "output_root"
                ],
                "isolated_dq_evidence.output_root",
            )
        ).resolve(strict=True)
        / OUTPUT_DIRECTORY_NAME
    )
    requested_output = output_root.resolve(strict=False)
    if requested_output != expected_output:
        _fail(
            "O1_COVERAGE_OUTPUT_ROOT_INVALID",
            f"expected={expected_output} actual={requested_output}",
            path=requested_output,
        )
    if requested_output.exists():
        _fail(
            "O1_COVERAGE_OUTPUT_ALREADY_EXISTS",
            "coverage-only output is immutable and single-run",
            path=requested_output,
        )

    software_identity = _software_identity(
        project_root=root,
        policy=policy,
        source_commit_sha=source_commit_sha,
        cli_argv=cli_argv,
    )
    bound = _load_bound_inputs(
        project_root=root,
        policy=policy,
        policy_path=policy_path,
    )
    coverage = _evaluate_coverage(
        policy=bound.policy,
        historical_policy=bound.historical_policy,
        panel=bound.panel,
        event_rows=bound.event_rows,
    )
    report = _build_report(
        bound=bound,
        coverage=coverage,
        software_identity=software_identity,
        generated_at=_aware_utc(generated_at),
    )

    requested_output.mkdir(parents=False)
    report_path = requested_output / "coverage_report.json"
    gate_path = requested_output / "coverage_gate.json"
    write_json_atomic(report_path, report)
    gate = _build_gate(report=report, report_path=report_path)
    validate_o1_coverage_gate(gate)
    write_json_atomic(gate_path, gate)
    return O1CoverageResult(
        report=report,
        gate=gate,
        report_path=report_path,
        gate_path=gate_path,
    )


def validate_o1_coverage_gate(gate: Mapping[str, object]) -> None:
    if gate.get("schema_version") != COVERAGE_GATE_SCHEMA:
        _fail("O1_COVERAGE_GATE_SCHEMA_INVALID", str(gate.get("schema_version")))
    status = gate.get("status")
    if status not in {PASS_STATUS, BLOCKED_STATUS}:
        _fail("O1_COVERAGE_GATE_STATUS_INVALID", str(status))
    body = {key: value for key, value in gate.items() if key != "gate_id"}
    expected_id = f"o1_coverage_gate_{_digest(body)[:32]}"
    if gate.get("gate_id") != expected_id:
        _fail(
            "O1_COVERAGE_GATE_ID_MISMATCH",
            f"expected={expected_id} actual={gate.get('gate_id')}",
        )
    authorization = _mapping(gate.get("next_authorization"), "next_authorization")
    expected_pass = status == PASS_STATUS
    if (
        authorization.get("coverage_only_gate_passed") is not expected_pass
        or authorization.get("canonical_policy_update_eligible") is not expected_pass
        or authorization.get("model_training_allowed_now") is not False
        or authorization.get("canonical_run_allowed_now") is not False
        or authorization.get("production_allowed") is not False
    ):
        _fail("O1_COVERAGE_GATE_SCOPE_INVALID", str(dict(authorization)))
    expected_class = None if expected_pass else BLOCKED_CLASS
    if gate.get("mechanical_classification") != expected_class:
        _fail(
            "O1_COVERAGE_GATE_CLASSIFICATION_INVALID",
            str(gate.get("mechanical_classification")),
        )
    safety = _mapping(gate.get("safety"), "safety")
    if safety != _safety_boundary():
        _fail("O1_COVERAGE_GATE_SAFETY_INVALID", str(dict(safety)))


def _load_bound_inputs(
    *,
    project_root: Path,
    policy: Mapping[str, object],
    policy_path: Path,
) -> _BoundInputs:
    policy_sha = sha256_path(policy_path)
    authority = _mapping(policy["authority"], "authority")
    target_binding = _mapping(authority["target_policy"], "target_policy")
    _verify_bound_file(project_root, target_binding)
    historical_binding = _mapping(
        authority["historical_model_policy"],
        "historical_model_policy",
    )
    historical_path = _verify_bound_file(project_root, historical_binding)
    historical_policy = _mapping(
        safe_load_yaml_path(historical_path),
        "historical_model_policy",
    )

    dq_evidence = _mapping(policy["isolated_dq_evidence"], "isolated_dq_evidence")
    dq_binding = _mapping(dq_evidence["gate"], "isolated_dq_evidence.gate")
    dq_path = _verify_bound_file(project_root, dq_binding)
    dq_gate = _read_json_mapping(dq_path, "O1_COVERAGE_DQ_GATE_PARSE_FAILED")
    validate_o1_dq_gate(dq_gate)
    if (
        dq_gate.get("gate_id") != dq_binding.get("gate_id")
        or dq_gate.get("status") != "PASS"
    ):
        _fail("O1_COVERAGE_DQ_GATE_BINDING_INVALID", str(dq_gate.get("gate_id")))
    dq_summary = _mapping(dq_gate["fresh_data_quality"], "fresh_data_quality")
    expected_receipt = _mapping(dq_evidence["fresh_receipt"], "fresh_receipt")
    for field in (
        "receipt_id",
        "receipt_sha256",
        "requested_start",
        "requested_end",
        "evaluated_start",
        "evaluated_end",
        "error_count",
        "warning_count",
    ):
        if dq_summary.get(field) != expected_receipt.get(field):
            _fail(
                "O1_COVERAGE_DQ_RECEIPT_BINDING_INVALID",
                f"field={field}",
            )

    event_evidence = _mapping(
        policy["event_attempt_freeze_evidence"],
        "event_attempt_freeze_evidence",
    )
    if event_evidence.get("status") != "PASS_EVENT_AND_ATTEMPT_LEDGERS_FROZEN":
        _fail("O1_COVERAGE_EVENT_EVIDENCE_STATUS_INVALID", str(event_evidence.get("status")))
    replay = _mapping(event_evidence["replay_artifacts"], "replay_artifacts")
    source_manifest_binding = _mapping(replay["source_manifest"], "source_manifest")
    source_manifest_path = _verify_bound_file(project_root, source_manifest_binding)
    source_manifest = _read_json_mapping(
        source_manifest_path,
        "O1_COVERAGE_SOURCE_MANIFEST_PARSE_FAILED",
    )
    if source_manifest.get("manifest_id") != source_manifest_binding.get("manifest_id"):
        _fail("O1_COVERAGE_SOURCE_MANIFEST_ID_INVALID", str(source_manifest.get("manifest_id")))

    event_binding = _mapping(replay["event_ledger"], "event_ledger")
    event_path = _verify_bound_file(project_root, event_binding)
    event_ledger = _read_json_mapping(event_path, "O1_COVERAGE_EVENT_LEDGER_PARSE_FAILED")
    if (
        event_ledger.get("ledger_id") != event_binding.get("ledger_id")
        or event_ledger.get("status") != "PASS"
    ):
        _fail("O1_COVERAGE_EVENT_LEDGER_ID_INVALID", str(event_ledger.get("ledger_id")))
    event_rows_raw = event_ledger.get("events")
    if not isinstance(event_rows_raw, Sequence) or isinstance(event_rows_raw, (str, bytes)):
        _fail("O1_COVERAGE_EVENT_ROWS_INVALID", "events must be a sequence")
    event_rows = tuple(_mapping(row, "event_row") for row in event_rows_raw)
    if len(event_rows) != _positive_int(event_binding["event_count"], "event_count"):
        _fail("O1_COVERAGE_EVENT_COUNT_INVALID", str(len(event_rows)))

    initial = _mapping(
        event_evidence["initial_failure_artifacts"],
        "initial_failure_artifacts",
    )
    attempt_binding = _mapping(initial["attempt_ledger"], "attempt_ledger")
    attempt_path = _verify_bound_file(project_root, attempt_binding)
    attempt_ledger = _read_json_mapping(
        attempt_path,
        "O1_COVERAGE_ATTEMPT_LEDGER_PARSE_FAILED",
    )
    current_attempt = _mapping(attempt_ledger.get("current_attempt"), "current_attempt")
    if (
        attempt_ledger.get("ledger_id") != attempt_binding.get("ledger_id")
        or current_attempt.get("attempt_family_id") != ATTEMPT_FAMILY_ID
        or current_attempt.get("model_id") != MODEL_ID
        or current_attempt.get("feature_family_prefix") != FEATURE_FAMILY
        or current_attempt.get("coverage_read") is not False
        or current_attempt.get("model_trained") is not False
        or current_attempt.get("result_read") is not False
    ):
        _fail("O1_COVERAGE_ATTEMPT_LEDGER_INVALID", str(dict(current_attempt)))

    event_gate_binding = _mapping(replay["gate"], "event_gate")
    event_gate_path = _verify_bound_file(project_root, event_gate_binding)
    event_gate = _read_json_mapping(
        event_gate_path,
        "O1_COVERAGE_EVENT_GATE_PARSE_FAILED",
    )
    validate_o1_event_attempt_freeze_gate(event_gate)
    authorization = _mapping(event_gate["next_authorization"], "event_authorization")
    if (
        event_gate.get("gate_id") != event_gate_binding.get("gate_id")
        or authorization.get("coverage_only_gate_allowed") is not True
        or authorization.get("model_training_allowed") is not False
        or authorization.get("canonical_run_allowed") is not False
        or authorization.get("production_allowed") is not False
    ):
        _fail("O1_COVERAGE_EVENT_GATE_BINDING_INVALID", str(event_gate.get("gate_id")))

    candidate_root = Path(
        _text(dq_evidence["candidate_project_root"], "candidate_project_root")
    ).resolve(strict=True)
    expected_members = _mapping(
        _mapping(policy["data_contract"], "data_contract")["publication"],
        "publication",
    )
    expected_members = _mapping(expected_members["immutable_members"], "immutable_members")
    dq_members = _mapping(dq_gate["candidate_objects"], "candidate_objects")
    input_commitment: dict[str, object] = {}
    for role, relative_path in _INPUT_PATHS.items():
        policy_member = _mapping(expected_members[role], f"immutable_members.{role}")
        dq_member = _mapping(dq_members[role], f"candidate_objects.{role}")
        candidate_path = (candidate_root / relative_path).resolve(strict=True)
        _require_contained(candidate_root, candidate_path, "O1_COVERAGE_INPUT_PATH_ESCAPED")
        expected_sha = _text(policy_member["sha256"], f"{role}.sha256")
        expected_size = _positive_int(policy_member["size_bytes"], f"{role}.size_bytes")
        if (
            dq_member.get("path") != relative_path
            or dq_member.get("sha256") != expected_sha
            or dq_member.get("size_bytes") != expected_size
            or dq_member.get("verified") is not True
            or sha256_path(candidate_path) != expected_sha
            or candidate_path.stat().st_size != expected_size
        ):
            _fail("O1_COVERAGE_INPUT_COMMITMENT_INVALID", role, path=candidate_path)
        input_commitment[role] = {
            "path": candidate_path.as_posix(),
            "sha256": expected_sha,
            "size_bytes": expected_size,
            "verified": True,
        }

    prices_path = Path(_mapping(input_commitment["prices"], "prices")["path"])
    try:
        raw_panel = pd.read_csv(prices_path, low_memory=False)
    except (OSError, pd.errors.ParserError, UnicodeDecodeError) as exc:
        _fail("O1_COVERAGE_PRICE_PARSE_FAILED", str(exc), path=prices_path)
    data_contract = _mapping(policy["data_contract"], "data_contract")
    panel = _normalize_price_panel(
        raw_panel,
        start=_text(data_contract["primary_research_start"], "primary_research_start"),
        end=_text(data_contract["evaluated_end"], "evaluated_end"),
    )
    return _BoundInputs(
        policy=policy,
        policy_path=policy_path,
        policy_sha256=policy_sha,
        historical_policy=historical_policy,
        historical_policy_path=historical_path,
        panel=panel,
        event_rows=event_rows,
        evidence={
            "dq_gate": _binding_projection(dq_binding, id_field="gate_id"),
            "event_source_manifest": _binding_projection(
                source_manifest_binding,
                id_field="manifest_id",
            ),
            "event_ledger": _binding_projection(event_binding, id_field="ledger_id"),
            "attempt_ledger": _binding_projection(attempt_binding, id_field="ledger_id"),
            "event_gate": _binding_projection(event_gate_binding, id_field="gate_id"),
        },
        input_commitment=input_commitment,
        dq_summary=dict(dq_summary),
    )


def _evaluate_coverage(
    *,
    policy: Mapping[str, object],
    historical_policy: Mapping[str, object],
    panel: pd.DataFrame,
    event_rows: Sequence[Mapping[str, object]],
) -> Mapping[str, object]:
    feature_frame, feature_ids_by_family = _build_feature_frame(historical_policy, panel)
    feature_ids = _selected_feature_ids(
        audit_policy=policy,
        historical_policy=historical_policy,
        feature_ids_by_family=feature_ids_by_family,
    )
    rows, eligibility = _build_coverage_rows(
        policy=policy,
        panel=panel,
        feature_frame=feature_frame,
        feature_ids=feature_ids,
    )
    folds = _build_fold_ledger(audit_policy=policy, rows=rows)
    row_lookup = {str(row["decision_date"]): row for row in rows}
    split_contract = _mapping(policy["split_contract"], "split_contract")
    horizon = _positive_int(
        _mapping(policy["target_contract"], "target_contract")[
            "primary_horizon_common_sessions"
        ],
        "primary_horizon_common_sessions",
    )
    fold_coverage: list[Mapping[str, object]] = []
    oof_dates: list[str] = []
    checks: list[dict[str, object]] = []
    coverage_contract = _mapping(policy["coverage_contract"], "coverage_contract")
    for fold in folds:
        fold_map = _mapping(fold, "fold")
        train_dates = _string_sequence(fold_map["train_decision_dates"], "train dates")
        test_dates = _string_sequence(fold_map["test_decision_dates"], "test dates")
        train_ess = _effective_sample(
            [float(row_lookup[value]["target_value"]) for value in train_dates],
            horizon=horizon,
        )
        test_ess = _effective_sample(
            [float(row_lookup[value]["target_value"]) for value in test_dates],
            horizon=horizon,
        )
        final_partial = bool(fold_map["final_partial_fold"])
        test_floor_key = (
            "minimum_test_effective_sample_final_partial_fold"
            if final_partial
            else "minimum_test_effective_sample_per_full_fold"
        )
        train_floor = _positive_int(
            coverage_contract["minimum_train_effective_sample_per_fold"],
            "minimum_train_effective_sample_per_fold",
        )
        test_floor = _positive_int(coverage_contract[test_floor_key], test_floor_key)
        train_pass = float(train_ess["effective_sample"]) >= train_floor
        test_pass = float(test_ess["effective_sample"]) >= test_floor
        fold_id = _text(fold_map["fold_id"], "fold_id")
        checks.extend(
            (
                _check(
                    f"{fold_id}_TRAIN_EFFECTIVE_SAMPLE",
                    train_floor,
                    train_ess["effective_sample"],
                    train_pass,
                ),
                _check(
                    f"{fold_id}_TEST_EFFECTIVE_SAMPLE",
                    test_floor,
                    test_ess["effective_sample"],
                    test_pass,
                ),
            )
        )
        fold_coverage.append(
            {
                "fold_id": fold_id,
                "train_start": fold_map["train_start"],
                "train_cutoff": fold_map["train_cutoff"],
                "test_start": fold_map["test_start"],
                "test_end": fold_map["test_end"],
                "final_partial_fold": final_partial,
                "train": train_ess,
                "test": test_ess,
                "train_floor": train_floor,
                "test_floor": test_floor,
                "train_pass": train_pass,
                "test_pass": test_pass,
                "train_membership_sha256": fold_map["train_membership_sha256"],
                "test_membership_sha256": fold_map["test_membership_sha256"],
            }
        )
        oof_dates.extend(test_dates)

    completed_floor = _positive_int(
        coverage_contract["minimum_completed_outer_folds"],
        "minimum_completed_outer_folds",
    )
    checks.append(
        _check(
            "MINIMUM_COMPLETED_OUTER_FOLDS",
            completed_floor,
            len(folds),
            len(folds) >= completed_floor,
        )
    )
    total_oof = _effective_sample(
        [float(row_lookup[value]["target_value"]) for value in oof_dates],
        horizon=horizon,
    )
    total_floor = _positive_int(
        coverage_contract["minimum_total_oof_effective_sample"],
        "minimum_total_oof_effective_sample",
    )
    checks.append(
        _check(
            "MINIMUM_TOTAL_OOF_EFFECTIVE_SAMPLE",
            total_floor,
            total_oof["effective_sample"],
            float(total_oof["effective_sample"]) >= total_floor,
        )
    )

    regime = _regime_coverage(
        policy=policy,
        folds=folds,
        row_lookup=row_lookup,
        horizon=horizon,
    )
    checks.extend(dict(item) for item in _sequence(regime["checks"], "regime checks"))
    events = _event_coverage(
        policy=policy,
        folds=folds,
        rows=rows,
        event_rows=event_rows,
        common_sessions=sorted(str(value) for value in panel["date"].unique()),
    )
    checks.extend(dict(item) for item in _sequence(events["checks"], "event checks"))
    all_pass = bool(checks) and all(bool(item["passed"]) for item in checks)
    return {
        "status": PASS_STATUS if all_pass else BLOCKED_STATUS,
        "mechanical_classification": None if all_pass else BLOCKED_CLASS,
        "feature_count": len(feature_ids),
        "feature_ids_sha256": _digest(list(feature_ids)),
        "eligibility": eligibility,
        "completed_outer_fold_count": len(folds),
        "fold_coverage": fold_coverage,
        "total_oof": total_oof,
        "regime_coverage": {key: value for key, value in regime.items() if key != "checks"},
        "event_coverage": {key: value for key, value in events.items() if key != "checks"},
        "checks": checks,
        "all_mandatory_coverage_checks_passed": all_pass,
        "split_contract": {
            "split_id": split_contract["split_id"],
            "initial_train_raw_rows": split_contract["initial_train_raw_rows"],
            "outer_test_raw_rows": split_contract["outer_test_raw_rows"],
            "final_partial_raw_row_floor": split_contract[
                "final_partial_raw_row_floor"
            ],
            "embargo_common_sessions": split_contract["embargo_common_sessions"],
        },
    }


def _build_coverage_rows(
    *,
    policy: Mapping[str, object],
    panel: pd.DataFrame,
    feature_frame: pd.DataFrame,
    feature_ids: Sequence[str],
) -> tuple[list[dict[str, object]], Mapping[str, object]]:
    prices = panel.pivot(index="date", columns="ticker", values="adj_close").sort_index()
    sessions = [str(value) for value in prices.index]
    target = _mapping(policy["target_contract"], "target_contract")
    data = _mapping(policy["data_contract"], "data_contract")
    horizon = _positive_int(
        target["primary_horizon_common_sessions"],
        "primary_horizon_common_sessions",
    )
    primary_start = _text(data["primary_research_start"], "primary_research_start")
    rows: list[dict[str, object]] = []
    pre_start = 0
    missing_features = 0
    immature = 0
    for index, decision_date in enumerate(sessions):
        if decision_date < primary_start:
            pre_start += 1
            continue
        label_end_index = index + horizon
        if label_end_index >= len(sessions):
            immature += 1
            continue
        values = feature_frame.loc[decision_date, list(feature_ids)].to_numpy(dtype=float)
        if not np.isfinite(values).all():
            missing_features += 1
            continue
        label_end = sessions[label_end_index]
        target_value = (
            float(prices.loc[label_end, "QQQ"] / prices.loc[decision_date, "QQQ"] - 1.0)
            - float(prices.loc[label_end, "SGOV"] / prices.loc[decision_date, "SGOV"] - 1.0)
        )
        rows.append(
            {
                "decision_date": decision_date,
                "label_interval_start": sessions[index + 1],
                "label_interval_end": label_end,
                "label_available_on_session": label_end,
                "target_value": target_value,
                "features": {
                    feature_id: float(value)
                    for feature_id, value in zip(feature_ids, values, strict=True)
                },
            }
        )
    return rows, {
        "requested_start": primary_start,
        "requested_end": data["requested_end"],
        "evaluated_start": primary_start,
        "evaluated_end": data["evaluated_end"],
        "selected_market_row_count": len(panel),
        "common_session_count": len(sessions),
        "pre_research_start_session_count": pre_start,
        "missing_feature_session_count": missing_features,
        "immature_tail_session_count": immature,
        "eligible_row_count": len(rows),
        "first_eligible_decision_date": rows[0]["decision_date"] if rows else None,
        "last_eligible_decision_date": rows[-1]["decision_date"] if rows else None,
        "eligible_membership_sha256": _digest(
            [str(row["decision_date"]) for row in rows]
        ),
    }


def _effective_sample(values: Sequence[float], *, horizon: int) -> Mapping[str, object]:
    array = np.asarray(values, dtype=float)
    n = int(array.size)
    if n == 0:
        return {
            "raw_sample": 0,
            "non_overlap_equivalent": 0,
            "autocorrelation_ess": 0.0,
            "effective_sample": 0.0,
            "positive_autocorrelation_sum_lag_1_to_5": 0.0,
        }
    non_overlap = math.floor(n / horizon)
    positive_sum = 0.0
    for lag in range(1, 6):
        if n <= lag:
            break
        left = array[:-lag]
        right = array[lag:]
        if float(np.std(left)) == 0.0 or float(np.std(right)) == 0.0:
            rho = 0.0
        else:
            rho = float(np.corrcoef(left, right)[0, 1])
            if not math.isfinite(rho):
                rho = 0.0
        positive_sum += max(rho, 0.0)
    denominator = max(1.0, 1.0 + (2.0 * positive_sum))
    autocorrelation_ess = min(float(n), max(1.0, float(n) / denominator))
    effective = min(float(non_overlap), autocorrelation_ess)
    return {
        "raw_sample": n,
        "non_overlap_equivalent": non_overlap,
        "autocorrelation_ess": _rounded(autocorrelation_ess),
        "effective_sample": _rounded(effective),
        "positive_autocorrelation_sum_lag_1_to_5": _rounded(positive_sum),
    }


def _regime_coverage(
    *,
    policy: Mapping[str, object],
    folds: Sequence[Mapping[str, object]],
    row_lookup: Mapping[str, Mapping[str, object]],
    horizon: int,
) -> Mapping[str, object]:
    regime_contract = _mapping(policy["regime_contract"], "regime_contract")
    axes = _mapping(regime_contract["axes"], "regime_contract.axes")
    quantiles = [float(value) for value in _sequence(regime_contract["quantiles"], "quantiles")]
    if quantiles != [1.0 / 3.0, 2.0 / 3.0]:
        _fail("O1_COVERAGE_REGIME_QUANTILES_INVALID", str(quantiles))
    coverage_contract = _mapping(policy["coverage_contract"], "coverage_contract")
    ess_floor = _positive_int(
        coverage_contract["mandatory_regime_cell_effective_sample"],
        "mandatory_regime_cell_effective_sample",
    )
    fold_floor = _positive_int(
        coverage_contract["mandatory_regime_cell_fold_count"],
        "mandatory_regime_cell_fold_count",
    )
    accumulator: dict[tuple[str, str], dict[str, object]] = {}
    threshold_rows: list[dict[str, object]] = []
    for axis_name, raw_axis in axes.items():
        axis = _mapping(raw_axis, f"axis.{axis_name}")
        feature_id = _text(axis["feature_id"], "feature_id")
        bins = _string_sequence(axis["bins"], "bins")
        if bins != ["LOW", "MIDDLE", "HIGH"]:
            _fail("O1_COVERAGE_REGIME_BINS_INVALID", str(bins))
        for label in bins:
            accumulator[(str(axis_name), label)] = {
                "targets": [],
                "fold_ids": set(),
            }
        for raw_fold in folds:
            fold = _mapping(raw_fold, "fold")
            train_dates = _string_sequence(fold["train_decision_dates"], "train dates")
            test_dates = _string_sequence(fold["test_decision_dates"], "test dates")
            train_values = np.asarray(
                [
                    float(_mapping(row_lookup[value]["features"], "features")[feature_id])
                    for value in train_dates
                ],
                dtype=float,
            )
            if train_values.size == 0:
                thresholds = (math.nan, math.nan)
            else:
                thresholds = tuple(
                    float(value)
                    for value in np.quantile(train_values, quantiles, method="linear")
                )
            threshold_rows.append(
                {
                    "fold_id": fold["fold_id"],
                    "axis": str(axis_name),
                    "feature_id": feature_id,
                    "lower_tertile": _rounded(thresholds[0]),
                    "upper_tertile": _rounded(thresholds[1]),
                    "fit_scope": "fold_train_only",
                }
            )
            for decision_date in test_dates:
                row = row_lookup[decision_date]
                feature_value = float(
                    _mapping(row["features"], "features")[feature_id]
                )
                label = (
                    "LOW"
                    if feature_value <= thresholds[0]
                    else "MIDDLE"
                    if feature_value <= thresholds[1]
                    else "HIGH"
                )
                bucket = accumulator[(str(axis_name), label)]
                _sequence_mutable(bucket["targets"]).append(float(row["target_value"]))
                _set_mutable(bucket["fold_ids"]).add(str(fold["fold_id"]))

    cells: list[dict[str, object]] = []
    checks: list[dict[str, object]] = []
    for (axis_name, label), bucket in accumulator.items():
        targets = [float(value) for value in _sequence(bucket["targets"], "targets")]
        fold_count = len(_set_mutable(bucket["fold_ids"]))
        ess = _effective_sample(targets, horizon=horizon)
        ess_pass = float(ess["effective_sample"]) >= ess_floor
        fold_pass = fold_count >= fold_floor
        cell_id = f"{axis_name}:{label}"
        cells.append(
            {
                "cell_id": cell_id,
                "axis": axis_name,
                "bin": label,
                "effective_sample": ess,
                "fold_count": fold_count,
                "effective_sample_floor": ess_floor,
                "fold_count_floor": fold_floor,
                "passed": ess_pass and fold_pass,
            }
        )
        checks.extend(
            (
                _check(
                    f"REGIME_{axis_name.upper()}_{label}_EFFECTIVE_SAMPLE",
                    ess_floor,
                    ess["effective_sample"],
                    ess_pass,
                ),
                _check(
                    f"REGIME_{axis_name.upper()}_{label}_FOLD_COUNT",
                    fold_floor,
                    fold_count,
                    fold_pass,
                ),
            )
        )
    return {
        "fit_scope": "fold_train_only",
        "thresholds": threshold_rows,
        "cells": cells,
        "checks": checks,
    }


def _event_coverage(
    *,
    policy: Mapping[str, object],
    folds: Sequence[Mapping[str, object]],
    rows: Sequence[Mapping[str, object]],
    event_rows: Sequence[Mapping[str, object]],
    common_sessions: Sequence[str],
) -> Mapping[str, object]:
    event_contract = _mapping(policy["event_contract"], "event_contract")
    families = _string_sequence(
        event_contract["mandatory_event_families"],
        "mandatory_event_families",
    )
    if _sequence(event_contract["event_window_common_sessions"], "event window") != [-1, 1]:
        _fail("O1_COVERAGE_EVENT_WINDOW_INVALID", "expected [-1, 1]")
    coverage_contract = _mapping(policy["coverage_contract"], "coverage_contract")
    episode_floor = _positive_int(
        coverage_contract["mandatory_event_family_episode_count"],
        "mandatory_event_family_episode_count",
    )
    fold_floor = _positive_int(
        coverage_contract["mandatory_event_family_fold_count"],
        "mandatory_event_family_fold_count",
    )
    eligible_dates = {str(row["decision_date"]) for row in rows}
    session_index = {value: index for index, value in enumerate(common_sessions)}
    fold_tests = {
        _text(_mapping(fold, "fold")["fold_id"], "fold_id"): set(
            _string_sequence(
                _mapping(fold, "fold")["test_decision_dates"],
                "test_decision_dates",
            )
        )
        for fold in folds
    }
    family_episodes = {family: set() for family in families}
    family_folds = {family: set() for family in families}
    missing_common_session = {family: 0 for family in families}
    outside_eligible_oof = {family: 0 for family in families}
    event_ids: set[str] = set()
    for raw_event in event_rows:
        event = _mapping(raw_event, "event")
        family = _text(event["event_family"], "event_family")
        event_id = _text(event["event_id"], "event_id")
        if family not in family_episodes or event_id in event_ids:
            _fail("O1_COVERAGE_EVENT_ID_OR_FAMILY_INVALID", event_id)
        event_ids.add(event_id)
        event_date = _aware_utc(
            datetime.fromisoformat(_text(event["event_timestamp"], "event_timestamp"))
        ).date().isoformat()
        if event_date not in session_index:
            missing_common_session[family] += 1
            continue
        anchor_index = session_index[event_date]
        if anchor_index == 0 or anchor_index + 1 >= len(common_sessions):
            missing_common_session[family] += 1
            continue
        window_dates = {
            common_sessions[anchor_index - 1],
            event_date,
            common_sessions[anchor_index + 1],
        }
        matched_folds = {
            fold_id
            for fold_id, test_dates in fold_tests.items()
            if window_dates & test_dates & eligible_dates
        }
        if not matched_folds:
            outside_eligible_oof[family] += 1
            continue
        family_episodes[family].add(event_id)
        family_folds[family].update(matched_folds)

    family_rows: list[dict[str, object]] = []
    checks: list[dict[str, object]] = []
    for family in families:
        episode_count = len(family_episodes[family])
        fold_count = len(family_folds[family])
        episode_pass = episode_count >= episode_floor
        fold_pass = fold_count >= fold_floor
        family_rows.append(
            {
                "event_family": family,
                "eligible_oof_episode_count": episode_count,
                "fold_count": fold_count,
                "missing_common_session_episode_count": missing_common_session[family],
                "outside_eligible_oof_episode_count": outside_eligible_oof[family],
                "episode_count_floor": episode_floor,
                "fold_count_floor": fold_floor,
                "passed": episode_pass and fold_pass,
            }
        )
        checks.extend(
            (
                _check(
                    f"EVENT_{family}_EPISODE_COUNT",
                    episode_floor,
                    episode_count,
                    episode_pass,
                ),
                _check(
                    f"EVENT_{family}_FOLD_COUNT",
                    fold_floor,
                    fold_count,
                    fold_pass,
                ),
            )
        )
    return {
        "anchor_rule": (
            "event UTC occurrence date must equal a common session; non-session "
            "events are counted as missing and are not shifted"
        ),
        "fold_membership_rule": "event [-1,+1] window intersects eligible OOF test dates",
        "families": family_rows,
        "checks": checks,
    }


def _build_report(
    *,
    bound: _BoundInputs,
    coverage: Mapping[str, object],
    software_identity: Mapping[str, object],
    generated_at: datetime,
) -> Mapping[str, object]:
    body: dict[str, object] = {
        "schema_version": COVERAGE_REPORT_SCHEMA,
        "task_id": TASK_ID,
        "status": coverage["status"],
        "generated_at": generated_at.isoformat(),
        "authority": {
            "policy_id": POLICY_ID,
            "policy_path": bound.policy_path.relative_to(
                _repository_root(bound.policy_path)
            ).as_posix(),
            "policy_sha256": bound.policy_sha256,
            "owner_decision": OWNER_DECISION,
            "attempt_family_id": ATTEMPT_FAMILY_ID,
            "model_id": MODEL_ID,
            "feature_family": FEATURE_FAMILY,
        },
        "software_identity": dict(software_identity),
        "data_quality": {
            "status": bound.dq_summary["status"],
            "error_count": bound.dq_summary["error_count"],
            "warning_count": bound.dq_summary["warning_count"],
            "requested_start": bound.dq_summary["requested_start"],
            "requested_end": bound.dq_summary["requested_end"],
            "evaluated_start": bound.dq_summary["evaluated_start"],
            "evaluated_end": bound.dq_summary["evaluated_end"],
            "receipt_id": bound.dq_summary["receipt_id"],
            "receipt_sha256": bound.dq_summary["receipt_sha256"],
        },
        "evidence": dict(bound.evidence),
        "input_commitment": dict(bound.input_commitment),
        "coverage": dict(coverage),
        "mechanical_classification": coverage["mechanical_classification"],
        "next_authorization": {
            "coverage_only_gate_passed": coverage["status"] == PASS_STATUS,
            "canonical_policy_update_eligible": coverage["status"] == PASS_STATUS,
            "model_training_allowed_now": False,
            "canonical_run_allowed_now": False,
            "canonical_run_executed": False,
            "production_allowed": False,
        },
        "attempt_execution": {
            "attempt_family_id": ATTEMPT_FAMILY_ID,
            "coverage_read": True,
            "model_trained": False,
            "prediction_generated": False,
            "metric_generated": False,
        },
        "safety": _safety_boundary(),
    }
    return {"report_id": f"o1_coverage_report_{_digest(body)[:32]}", **body}


def _build_gate(
    *,
    report: Mapping[str, object],
    report_path: Path,
) -> Mapping[str, object]:
    status = _text(report["status"], "report.status")
    body: dict[str, object] = {
        "schema_version": COVERAGE_GATE_SCHEMA,
        "task_id": TASK_ID,
        "status": status,
        "generated_at": report["generated_at"],
        "report": {
            "report_id": report["report_id"],
            "path": report_path.name,
            "sha256": sha256_path(report_path),
            "byte_size": report_path.stat().st_size,
        },
        "dq_gate": _mapping(report["evidence"], "evidence")["dq_gate"],
        "event_gate": _mapping(report["evidence"], "evidence")["event_gate"],
        "attempt_family_id": ATTEMPT_FAMILY_ID,
        "mechanical_classification": report["mechanical_classification"],
        "next_authorization": report["next_authorization"],
        "safety": _safety_boundary(),
    }
    return {"gate_id": f"o1_coverage_gate_{_digest(body)[:32]}", **body}


def _validate_policy_contract(policy: Mapping[str, object]) -> None:
    if (
        policy.get("schema_version") != POLICY_SCHEMA
        or policy.get("policy_id") != POLICY_ID
        or policy.get("task_id") != POLICY_TASK_ID
        or policy.get("owner_decision") != OWNER_DECISION
        or policy.get("status")
        != "OWNER_APPROVED_EVENT_LEDGER_FROZEN_COVERAGE_ONLY_READY"
    ):
        _fail("O1_COVERAGE_POLICY_IDENTITY_INVALID", str(policy.get("status")))
    execution = _mapping(policy["execution_binding"], "execution_binding")
    if (
        execution.get("historical_seen_only") is not True
        or execution.get("prospective_accessed") is not False
        or execution.get("real_coverage_read_allowed_now") is not True
        or execution.get("model_training_allowed_now") is not False
        or execution.get("maximum_canonical_runs") != 1
    ):
        _fail("O1_COVERAGE_EXECUTION_SCOPE_INVALID", str(dict(execution)))
    model = _mapping(policy["model_feature_contract"], "model_feature_contract")
    if (
        model.get("model_id") != MODEL_ID
        or model.get("family_prefix") != FEATURE_FAMILY
        or model.get("automatic_hyperparameter_search_allowed") is not False
        or model.get("feature_subset_selection_allowed") is not False
    ):
        _fail("O1_COVERAGE_MODEL_CONTRACT_INVALID", str(dict(model)))
    coverage = _mapping(policy["coverage_contract"], "coverage_contract")
    exact_coverage = {
        "minimum_completed_outer_folds": 5,
        "minimum_train_effective_sample_per_fold": 100,
        "minimum_test_effective_sample_per_full_fold": 24,
        "minimum_test_effective_sample_final_partial_fold": 12,
        "minimum_total_oof_effective_sample": 120,
        "non_overlap_equivalent_formula": "floor(eligible_rows / 5)",
        "effective_sample_formula": "min(non_overlap_equivalent, autocorrelation_ess)",
        "mandatory_regime_cell_effective_sample": 15,
        "mandatory_regime_cell_fold_count": 3,
        "mandatory_event_family_episode_count": 3,
        "mandatory_event_family_fold_count": 2,
        "failure_class": BLOCKED_CLASS,
    }
    if any(coverage.get(key) != value for key, value in exact_coverage.items()):
        _fail("O1_COVERAGE_THRESHOLD_CONTRACT_INVALID", str(dict(coverage)))
    autocorrelation = _mapping(coverage["autocorrelation_ess"], "autocorrelation_ess")
    if (
        autocorrelation.get("formula")
        != "n / max(1, 1 + 2 * sum(max(rho_lag, 0), lag=1..5))"
        or autocorrelation.get("lag_count") != 5
        or _sequence(autocorrelation.get("clamp_to"), "clamp_to") != [1, "n"]
    ):
        _fail("O1_COVERAGE_ESS_CONTRACT_INVALID", str(dict(autocorrelation)))


def _normalize_price_panel(
    frame: pd.DataFrame,
    *,
    start: str,
    end: str,
) -> pd.DataFrame:
    required = {"date", "ticker", "adj_close"}
    if not required.issubset(frame.columns):
        _fail("O1_COVERAGE_PRICE_COLUMNS_INVALID", str(sorted(frame.columns)))
    selected = frame.loc[:, ["date", "ticker", "adj_close"]].copy()
    dates = selected["date"].astype(str)
    parsed = pd.to_datetime(dates, format="%Y-%m-%d", errors="coerce")
    if parsed.isna().any() or not (parsed.dt.strftime("%Y-%m-%d") == dates).all():
        _fail("O1_COVERAGE_PRICE_DATE_INVALID", "non-canonical date")
    selected["date"] = dates
    selected["ticker"] = selected["ticker"].astype(str)
    selected["adj_close"] = pd.to_numeric(selected["adj_close"], errors="coerce")
    selected = selected.loc[
        selected["ticker"].isin(_REQUIRED_TICKERS)
        & (selected["date"] >= start)
        & (selected["date"] <= end)
    ].copy()
    if selected.empty or set(selected["ticker"].unique()) != set(_REQUIRED_TICKERS):
        _fail("O1_COVERAGE_PRICE_TICKERS_INVALID", str(sorted(selected["ticker"].unique())))
    if selected.duplicated(["date", "ticker"]).any():
        _fail("O1_COVERAGE_PRICE_DUPLICATE_KEY", "date/ticker duplicate")
    prices = selected["adj_close"].to_numpy(dtype=float)
    if not np.isfinite(prices).all() or not (prices > 0.0).all():
        _fail("O1_COVERAGE_PRICE_VALUE_INVALID", "nonfinite or nonpositive")
    counts = selected.groupby("date")["ticker"].nunique()
    if counts.empty or not (counts == len(_REQUIRED_TICKERS)).all():
        _fail("O1_COVERAGE_COMMON_SESSION_INVALID", "incomplete ticker session")
    return selected.sort_values(["date", "ticker"], kind="stable").reset_index(drop=True)


def _software_identity(
    *,
    project_root: Path,
    policy: Mapping[str, object],
    source_commit_sha: str | None,
    cli_argv: Sequence[str],
) -> Mapping[str, object]:
    head = _git_head(project_root)
    commit = head if source_commit_sha is None else source_commit_sha
    if _HEX_40.fullmatch(commit) is None or commit != head:
        _fail(
            "O1_COVERAGE_SOURCE_COMMIT_INVALID",
            f"head={head} supplied={commit}",
        )
    base = _text(
        _mapping(policy["authority"], "authority")["contract_freeze_source_base_sha"],
        "contract_freeze_source_base_sha",
    )
    if _HEX_40.fullmatch(base) is None or not _git_is_ancestor(
        project_root,
        base,
        commit,
    ):
        _fail(
            "O1_COVERAGE_EXECUTION_BASE_INVALID",
            f"base={base} commit={commit}",
        )
    manifest = _contained_file(
        project_root,
        Path("pyproject.toml"),
        "O1_COVERAGE_DEPENDENCY_MANIFEST_MISSING",
    )
    return {
        "python_version": platform.python_version(),
        "package_lock_path": manifest.relative_to(project_root).as_posix(),
        "package_lock_kind": "UNLOCKED_PROJECT_MANIFEST",
        "package_lock_sha256": sha256_path(manifest),
        "source_commit_sha": commit,
        "contract_freeze_source_base_sha": base,
        "cli_argv": list(cli_argv),
    }


def _git_head(project_root: Path) -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=project_root,
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    if completed.returncode != 0:
        _fail("O1_COVERAGE_GIT_HEAD_UNAVAILABLE", completed.stderr.strip())
    return completed.stdout.strip()


def _git_is_ancestor(project_root: Path, ancestor: str, descendant: str) -> bool:
    completed = subprocess.run(
        ["git", "merge-base", "--is-ancestor", ancestor, descendant],
        cwd=project_root,
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return completed.returncode == 0


def _verify_bound_file(
    project_root: Path,
    binding: Mapping[str, object],
) -> Path:
    path = _contained_file(
        project_root,
        Path(_text(binding["path"], "binding.path")),
        "O1_COVERAGE_BOUND_FILE_MISSING",
    )
    expected_sha = _text(binding["sha256"], "binding.sha256")
    if _HEX_64.fullmatch(expected_sha) is None or sha256_path(path) != expected_sha:
        _fail("O1_COVERAGE_BOUND_FILE_TAMPERED", expected_sha, path=path)
    if "byte_size" in binding and path.stat().st_size != _positive_int(
        binding["byte_size"],
        "binding.byte_size",
    ):
        _fail("O1_COVERAGE_BOUND_FILE_SIZE_INVALID", str(binding["byte_size"]), path=path)
    return path


def _binding_projection(
    binding: Mapping[str, object],
    *,
    id_field: str,
) -> Mapping[str, object]:
    return {
        id_field: binding[id_field],
        "path": binding["path"],
        "sha256": binding["sha256"],
        **({"byte_size": binding["byte_size"]} if "byte_size" in binding else {}),
    }


def _contained_file(project_root: Path, path: Path, code: str) -> Path:
    candidate = path if path.is_absolute() else project_root / path
    try:
        resolved = candidate.resolve(strict=True)
    except OSError:
        _fail(code, str(candidate), path=candidate)
    _require_contained(project_root, resolved, "O1_COVERAGE_PATH_ESCAPED")
    if not resolved.is_file():
        _fail(code, "expected file", path=resolved)
    return resolved


def _require_contained(root: Path, child: Path, code: str) -> None:
    try:
        child.relative_to(root)
    except ValueError:
        _fail(code, f"root={root}", path=child)


def _repository_root(path: Path) -> Path:
    current = path.resolve(strict=True).parent
    while current.parent != current:
        if (current / ".git").exists():
            return current
        current = current.parent
    _fail("O1_COVERAGE_REPOSITORY_ROOT_NOT_FOUND", str(path))


def _read_json_mapping(path: Path, code: str) -> Mapping[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        _fail(code, str(exc), path=path)
    return _mapping(payload, path.name)


def _check(
    check_id: str,
    threshold: object,
    observed: object,
    passed: bool,
) -> dict[str, object]:
    return {
        "check_id": check_id,
        "threshold": threshold,
        "observed": observed,
        "passed": passed,
    }


def _safety_boundary() -> Mapping[str, object]:
    return {
        "historical_seen_only": True,
        "prospective_accessed": False,
        "coverage_only": True,
        "model_training_executed": False,
        "predictions_generated": False,
        "metrics_generated": False,
        "canonical_run_executed": False,
        "decision_value_audit_started": False,
        "risk_overlay_created": False,
        "candidate_backtest_weights_created": False,
        "qld_automatic_selection_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _digest(payload: object) -> str:
    raw = canonical_json_bytes(
        payload,
        sort_keys=True,
        indent=None,
        trailing_newline=False,
    )
    return hashlib.sha256(raw).hexdigest()


def _rounded(value: float) -> float:
    if not math.isfinite(value):
        _fail("O1_COVERAGE_NONFINITE_RESULT", str(value))
    return round(value, 12)


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        _fail("O1_COVERAGE_TIMESTAMP_NAIVE", value.isoformat())
    return value.astimezone(UTC)


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        _fail("O1_COVERAGE_MAPPING_INVALID", field)
    return value


def _sequence(value: object, field: str) -> list[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        _fail("O1_COVERAGE_SEQUENCE_INVALID", field)
    return list(value)


def _string_sequence(value: object, field: str) -> list[str]:
    return [str(item) for item in _sequence(value, field)]


def _sequence_mutable(value: object) -> list[Any]:
    if not isinstance(value, list):
        _fail("O1_COVERAGE_MUTABLE_SEQUENCE_INVALID", str(type(value)))
    return value


def _set_mutable(value: object) -> set[str]:
    if not isinstance(value, set):
        _fail("O1_COVERAGE_MUTABLE_SET_INVALID", str(type(value)))
    return value


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        _fail("O1_COVERAGE_TEXT_INVALID", field)
    return value


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool):
        _fail("O1_COVERAGE_INTEGER_INVALID", field)
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        _fail("O1_COVERAGE_INTEGER_INVALID", field)
    if parsed <= 0:
        _fail("O1_COVERAGE_INTEGER_INVALID", field)
    return parsed


def _fail(code: str, message: str, *, path: Path | None = None) -> NoReturn:
    raise O1CoverageError(code, message, path=path)
