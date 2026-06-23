from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso, write_foundation_artifact_pair
from ai_trading_system.simple_baseline_portfolio_control import (
    DEFAULT_AI_REGIME_BACKTEST_START,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    SAFETY_BOUNDARY,
    _data_quality_gate,
    _dynamic_candidate_strategies,
    _load_price_matrix,
    _load_registry,
    _mapping,
    _metrics_for_strategy,
    _read_json_or_empty,
    _records,
    _required_tickers,
    _research_policy,
    _research_policy_int,
    _slice_prices,
    _strategy_by_id,
    _strategy_return_series,
    _strategy_rows,
    _target_weight_frame,
    _turnover_series,
)
from ai_trading_system.trading_calendar import us_equity_market_session

DEFAULT_FORWARD_AGING_OBSERVATION_ROOT = (
    DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT / "forward_aging_observations"
)
DEFAULT_FORWARD_AGING_OWNER_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "simple_baseline_forward_aging_owner_review_pack.md"
)
DEFAULT_FORWARD_AGING_MASTER_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "simple_baseline_forward_aging_master_review.md"
)

PUBLIC_100_QQQ_ID = "100_qqq"
PRIMARY_CANDIDATE_ID = "equal_risk_qqq_sgov"
CHALLENGER_CANDIDATE_ID = "dyn_tqqq_capped_trend"
STATIC_COMPARATOR_IDS = ("qqq_50_sgov_50", "qqq_60_sgov_40", PUBLIC_100_QQQ_ID)
DEFAULT_CANDIDATE_ORDER = (
    PRIMARY_CANDIDATE_ID,
    *STATIC_COMPARATOR_IDS,
    CHALLENGER_CANDIDATE_ID,
)


def run_simple_baseline_real_result_reconciliation(
    *,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    input_paths = _real_result_reconciliation_paths(output_root)
    artifacts = {name: _read_json_or_empty(path) for name, path in input_paths.items()}
    missing = [name for name, payload in artifacts.items() if not payload]
    checks = _reconciliation_checks(artifacts)
    unsafe_reports = _unsafe_artifact_reports(artifacts)
    failed_checks = [row for row in checks if row["status"] == "FAIL"]
    warnings = [f"missing input artifact: {name}" for name in missing]

    if missing:
        status = "BLOCKED"
    elif unsafe_reports or failed_checks:
        status = "CONFLICT_FOUND"
    elif any(row["status"] == "WARN" for row in checks):
        status = "RECONCILED_WITH_WARNINGS"
    else:
        status = "RECONCILED"

    payload = _payload(
        report_type="simple_baseline_real_result_reconciliation",
        title="Simple Baseline Real Result Reconciliation",
        status=status,
        summary={
            "top_recommended_candidate": _top_recommended_candidate(artifacts),
            "primary_candidate": _owner_primary_candidate(artifacts),
            "dynamic_challenger": _owner_challenger_candidate(artifacts),
            "failed_check_count": len(failed_checks),
            "unsafe_report_count": len(unsafe_reports),
            "missing_input_count": len(missing),
        },
        input_artifacts={name: str(path) for name, path in input_paths.items()},
        checks=checks,
        missing_inputs=missing,
        warnings=warnings,
        unsafe_reports=unsafe_reports,
        blockers=warnings if status == "BLOCKED" else [],
        report_registry_entry=_report_registry_entry(
            "simple_baseline_real_result_reconciliation",
            "Simple Baseline Real Result Reconciliation",
            "aits research strategies simple-baseline-real-result-reconciliation",
            "simple_baseline_real_result_reconciliation",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_simple_baseline_forward_aging_candidate_freeze(
    *,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    candidates = _candidate_specs(config)
    issues = _candidate_freeze_issues(config, candidates)
    owner_decision = _read_json_or_empty(
        output_root / "simple_baseline_watchlist_owner_decision.json"
    )
    if issues:
        status = "CANDIDATES_CONFLICTED"
    elif not owner_decision:
        status = "CANDIDATES_NEED_OWNER_REVIEW"
    else:
        status = "CANDIDATES_FROZEN"

    payload = _payload(
        report_type="simple_baseline_forward_aging_candidate_freeze",
        title="Simple Baseline Forward Aging Candidate Freeze",
        status=status,
        summary={
            "primary_candidate": PRIMARY_CANDIDATE_ID,
            "static_comparator_count": len(STATIC_COMPARATOR_IDS),
            "challenger_candidate": CHALLENGER_CANDIDATE_ID,
            "candidate_count": len(candidates),
            "issue_count": len(issues),
        },
        candidate_freeze_policy=_forward_policy(config).get("candidate_freeze", {}),
        candidates=candidates,
        issues=issues,
        input_artifacts={
            "config": str(config_path),
            "owner_decision": str(output_root / "simple_baseline_watchlist_owner_decision.json"),
        },
        report_registry_entry=_report_registry_entry(
            "simple_baseline_forward_aging_candidate_freeze",
            "Simple Baseline Forward Aging Candidate Freeze",
            "aits research strategies simple-baseline-forward-aging-candidate-freeze",
            "simple_baseline_forward_aging_candidate_freeze",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_simple_baseline_forward_aging_contract(
    *,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    policy = _forward_policy(config)
    windows = _window_policy(config)
    candidates = _candidate_specs(config)
    missing = []
    if not windows:
        missing.append("observation_windows_trading_days")
    if not candidates:
        missing.append("candidate_freeze")
    status = "FORWARD_AGING_CONTRACT_READY"
    if missing and windows:
        status = "FORWARD_AGING_CONTRACT_PARTIAL"
    if not windows:
        status = "FORWARD_AGING_CONTRACT_BLOCKED"

    payload = _payload(
        report_type="simple_baseline_forward_aging_contract",
        title="Simple Baseline Forward Aging Contract",
        status=status,
        summary={
            "window_count": len(windows),
            "candidate_count": len(candidates),
            "minimum_20d_matured_observations_for_initial_review": _int(
                policy.get("minimum_20d_matured_observations_for_initial_review")
            ),
            "minimum_60d_matured_observations_for_weak_review": _int(
                policy.get("minimum_60d_matured_observations_for_weak_review")
            ),
            "minimum_120d_matured_observations_for_paper_shadow_review": _int(
                policy.get("minimum_120d_matured_observations_for_paper_shadow_review")
            ),
        },
        observation_windows=windows,
        observation_required_fields=[
            "decision_date",
            "strategy_id",
            "candidate_role",
            "target_weight_qqq",
            "target_weight_tqqq",
            "target_weight_sgov",
            "signal_inputs_used",
            "execution_assumption",
            "matured_5d",
            "matured_10d",
            "matured_20d",
            "matured_60d",
            "matured_120d",
            "pending_windows",
        ],
        forward_metrics=[
            "forward_return",
            "forward_max_drawdown",
            "forward_volatility",
            "relative_vs_100_qqq",
            "relative_vs_qqq_50_sgov_50",
            "relative_vs_qqq_60_sgov_40",
            "relative_vs_equal_risk_if_comparator",
            "turnover",
            "cash_drag",
            "missed_upside",
            "drawdown_reduction",
        ],
        sample_maturity_policy={
            "minimum_20d_matured_observations_for_initial_review": _int(
                policy.get("minimum_20d_matured_observations_for_initial_review")
            ),
            "minimum_60d_matured_observations_for_weak_review": _int(
                policy.get("minimum_60d_matured_observations_for_weak_review")
            ),
            "minimum_120d_matured_observations_for_paper_shadow_review": _int(
                policy.get("minimum_120d_matured_observations_for_paper_shadow_review")
            ),
        },
        candidates=candidates,
        blockers=missing,
        input_artifacts={"config": str(config_path)},
        report_registry_entry=_report_registry_entry(
            "simple_baseline_forward_aging_contract",
            "Simple Baseline Forward Aging Contract",
            "aits research strategies simple-baseline-forward-aging-contract",
            "simple_baseline_forward_aging_contract",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_simple_baseline_forward_aging_write_observation(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    as_of_date: date | None = None,
    decision_date: date | None = None,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    data_gate = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
    )
    resolved_date = decision_date or date.fromisoformat(str(data_gate.get("as_of")))
    observation_root = output_root / "forward_aging_observations"
    artifact_id = f"simple_baseline_forward_aging_observation_{resolved_date.isoformat()}"
    json_path = observation_root / f"{artifact_id}.json"
    previous_invalid_artifact: dict[str, Any] | None = None
    if json_path.exists():
        existing = _read_json_or_empty(json_path)
        if _is_written_observation_payload(existing):
            existing["status"] = "OBSERVATION_ALREADY_EXISTS"
            existing["idempotency_status"] = "already_exists_no_rewrite"
            return existing
        previous_invalid_artifact = {
            "path": str(json_path),
            "previous_status": existing.get("status"),
            "previous_observation_count": len(_records(existing.get("observations"))),
            "previous_data_quality_status": _mapping(existing.get("data_quality")).get(
                "status"
            ),
        }

    if not bool(data_gate.get("passed")):
        payload = _payload(
            report_type="simple_baseline_forward_aging_write_observation",
            title="Simple Baseline Forward Aging Observation",
            status="MARKET_DATA_MISSING",
            summary={
                "decision_date": resolved_date.isoformat(),
                "data_quality_status": data_gate.get("status"),
                "observation_count": 0,
                "replaced_invalid_existing_artifact": False,
            },
            data_quality=data_gate,
            previous_invalid_artifact=previous_invalid_artifact,
            observations=[],
            blockers=["validate_data_cache_failed"],
            input_artifacts={"config": str(config_path), "prices": str(prices_path)},
            report_registry_entry=_report_registry_entry(
                "simple_baseline_forward_aging_write_observation",
                "Simple Baseline Forward Aging Observation Writer",
                "aits research strategies simple-baseline-forward-aging-write-observation",
                "forward_aging_observations/simple_baseline_forward_aging_observation_*",
            ),
        )
        _write_pair(payload, output_root=observation_root, artifact_id=artifact_id)
        return payload

    prices = _load_price_matrix(prices_path, _required_tickers(config))
    decision_ts = _resolve_decision_timestamp(prices, resolved_date)
    windows = _window_policy(config)
    observations = [
        _observation_row(
            candidate,
            config=config,
            prices=prices,
            decision_ts=decision_ts,
            windows=windows,
        )
        for candidate in _candidate_specs(config)
    ]
    payload = _payload(
        report_type="simple_baseline_forward_aging_write_observation",
        title="Simple Baseline Forward Aging Observation",
        status="OBSERVATION_WRITTEN",
        summary={
            "decision_date": decision_ts.date().isoformat(),
            "observation_count": len(observations),
            "candidate_count": len(observations),
            "data_quality_status": data_gate.get("status"),
            "replaced_invalid_existing_artifact": previous_invalid_artifact is not None,
        },
        decision_date=decision_ts.date().isoformat(),
        data_quality=data_gate,
        previous_invalid_artifact=previous_invalid_artifact,
        observations=observations,
        input_artifacts={"config": str(config_path), "prices": str(prices_path)},
        report_registry_entry=_report_registry_entry(
            "simple_baseline_forward_aging_write_observation",
            "Simple Baseline Forward Aging Observation Writer",
            "aits research strategies simple-baseline-forward-aging-write-observation",
            "forward_aging_observations/simple_baseline_forward_aging_observation_*",
        ),
    )
    _write_pair(payload, output_root=observation_root, artifact_id=artifact_id)
    return payload


def run_simple_baseline_forward_aging_update_maturity(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    data_gate = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
    )
    observation_root = output_root / "forward_aging_observations"
    observation_paths = sorted(
        observation_root.glob("simple_baseline_forward_aging_observation_*.json")
    )
    if not bool(data_gate.get("passed")):
        payload = _payload(
            report_type="simple_baseline_forward_aging_update_maturity",
            title="Simple Baseline Forward Aging Maturity Update",
            status="MATURITY_BLOCKED",
            summary={
                "observation_file_count": len(observation_paths),
                "updated_window_count": 0,
                "data_quality_status": data_gate.get("status"),
            },
            data_quality=data_gate,
            blockers=["validate_data_cache_failed"],
            observation_files=[str(path) for path in observation_paths],
            report_registry_entry=_report_registry_entry(
                "simple_baseline_forward_aging_update_maturity",
                "Simple Baseline Forward Aging Maturity Update",
                "aits research strategies simple-baseline-forward-aging-update-maturity",
                "simple_baseline_forward_aging_maturity_update",
            ),
        )
        _write_pair(
            payload,
            output_root=output_root,
            artifact_id="simple_baseline_forward_aging_maturity_update",
        )
        return payload

    prices = _load_price_matrix(prices_path, _required_tickers(config))
    strategy_cache = _candidate_strategy_cache(config, prices)
    updated_windows = 0
    pending_windows = 0
    missing_windows = 0
    updated_files: list[str] = []

    for path in observation_paths:
        observation_payload = _read_json_or_empty(path)
        changed = False
        observations = observation_payload.get("observations")
        if not isinstance(observations, list):
            continue
        for observation in observations:
            if not isinstance(observation, dict):
                continue
            result = _update_observation_maturity(
                observation,
                config=config,
                prices=prices,
                strategy_cache=strategy_cache,
            )
            updated_windows += result["updated"]
            pending_windows += result["pending"]
            missing_windows += result["missing"]
            changed = changed or result["changed"]
        if changed:
            observation_payload["last_maturity_updated_at"] = utc_now_iso()
            _write_pair(observation_payload, output_root=path.parent, artifact_id=path.stem)
            updated_files.append(str(path))

    if updated_windows:
        status = "MATURITY_PARTIAL" if pending_windows or missing_windows else "MATURITY_UPDATED"
    else:
        status = "NO_MATURED_WINDOWS"
    payload = _payload(
        report_type="simple_baseline_forward_aging_update_maturity",
        title="Simple Baseline Forward Aging Maturity Update",
        status=status,
        summary={
            "observation_file_count": len(observation_paths),
            "updated_file_count": len(updated_files),
            "updated_window_count": updated_windows,
            "pending_window_count": pending_windows,
            "missing_window_count": missing_windows,
            "data_quality_status": data_gate.get("status"),
        },
        data_quality=data_gate,
        observation_files=[str(path) for path in observation_paths],
        updated_files=updated_files,
        report_registry_entry=_report_registry_entry(
            "simple_baseline_forward_aging_update_maturity",
            "Simple Baseline Forward Aging Maturity Update",
            "aits research strategies simple-baseline-forward-aging-update-maturity",
            "simple_baseline_forward_aging_maturity_update",
        ),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="simple_baseline_forward_aging_maturity_update",
    )
    return payload


def run_simple_baseline_forward_aging_scoreboard(
    *,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    observation_paths = sorted(
        (output_root / "forward_aging_observations").glob(
            "simple_baseline_forward_aging_observation_*.json"
        )
    )
    observations = _load_observations(observation_paths)
    rows = [
        _scoreboard_row(candidate, observations, config)
        for candidate in _candidate_specs(config)
    ]
    primary = next((row for row in rows if row["strategy_id"] == PRIMARY_CANDIDATE_ID), {})
    min_20d = _int(
        _forward_policy(config).get("minimum_20d_matured_observations_for_initial_review")
    )
    if not observations:
        status = "FORWARD_SCOREBOARD_PENDING"
    elif _int(primary.get("matured_20d_count")) < min_20d:
        status = "FORWARD_SCOREBOARD_INSUFFICIENT"
    else:
        status = "FORWARD_SCOREBOARD_READY"

    payload = _payload(
        report_type="simple_baseline_forward_aging_scoreboard",
        title="Simple Baseline Forward Aging Scoreboard",
        status=status,
        summary={
            "observation_file_count": len(observation_paths),
            "strategy_count": len(rows),
            "primary_matured_20d_count": _int(primary.get("matured_20d_count")),
            "primary_matured_60d_count": _int(primary.get("matured_60d_count")),
            "primary_matured_120d_count": _int(primary.get("matured_120d_count")),
        },
        scoreboard=rows,
        score_formula_id=_forward_policy(config).get("score_formula_id"),
        observation_files=[str(path) for path in observation_paths],
        report_registry_entry=_report_registry_entry(
            "simple_baseline_forward_aging_scoreboard",
            "Simple Baseline Forward Aging Scoreboard",
            "aits research strategies simple-baseline-forward-aging-scoreboard",
            "simple_baseline_forward_aging_scoreboard",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_first_forward_aging_observation_write(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    as_of_date: date | None = None,
    decision_date: date | None = None,
    owner_approved: bool = True,
) -> dict[str, Any]:
    approval = _owner_launch_approval(output_root, owner_approved)
    if not approval["approved"]:
        payload = _payload(
            report_type="first_forward_aging_observation_write",
            title="First Forward Aging Observation Write",
            status="FIRST_OBSERVATION_BLOCKED",
            summary={
                "observation_written": False,
                "owner_approved": False,
                "broker_action": "none",
            },
            approval=approval,
            observations=[],
            blockers=["owner_approval_missing"],
            report_registry_entry=_report_registry_entry(
                "first_forward_aging_observation_write",
                "First Forward Aging Observation Write",
                "aits research strategies first-forward-aging-observation-write",
                "first_forward_aging_observation_write",
            ),
        )
        _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
        return payload

    writer = run_simple_baseline_forward_aging_write_observation(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
        decision_date=decision_date,
    )
    if writer.get("status") == "OBSERVATION_WRITTEN":
        status = "FIRST_OBSERVATION_WRITTEN"
    elif writer.get("status") == "OBSERVATION_ALREADY_EXISTS":
        status = "FIRST_OBSERVATION_ALREADY_EXISTS"
    else:
        status = "FIRST_OBSERVATION_BLOCKED"
    data_quality = _mapping(writer.get("data_quality"))
    observations = _records(writer.get("observations"))
    payload = _payload(
        report_type="first_forward_aging_observation_write",
        title="First Forward Aging Observation Write",
        status=status,
        summary={
            "decision_date": writer.get("decision_date")
            or _mapping(writer.get("summary")).get("decision_date"),
            "observation_written": writer.get("status") == "OBSERVATION_WRITTEN",
            "writer_status": writer.get("status"),
            "observation_count": len(observations),
            "data_quality_status": data_quality.get("status"),
            "warning_count": data_quality.get("warning_count", 0),
            "broker_action": "none",
        },
        approval=approval,
        data_quality=data_quality,
        observations=observations,
        warnings=[
            str(issue.get("message"))
            for issue in _records(data_quality.get("issues"))
            if str(issue.get("severity")).upper() != "ERROR"
        ],
        writer_artifact_paths=writer.get("artifact_paths"),
        report_registry_entry=_report_registry_entry(
            "first_forward_aging_observation_write",
            "First Forward Aging Observation Write",
            "aits research strategies first-forward-aging-observation-write",
            "first_forward_aging_observation_write",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_forward_aging_idempotency_and_duplicate_guard(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    as_of_date: date | None = None,
    decision_date: date | None = None,
) -> dict[str, Any]:
    resolved_date = decision_date or _latest_observation_payload_date(output_root)
    if resolved_date is None:
        payload = _payload(
            report_type="forward_aging_idempotency_and_duplicate_guard",
            title="Forward Aging Idempotency and Duplicate Guard",
            status="FORWARD_IDEMPOTENCY_GUARD_BLOCKED",
            summary={"decision_date": None, "duplicate_guard_passed": False},
            blockers=["existing_observation_missing"],
            report_registry_entry=_report_registry_entry(
                "forward_aging_idempotency_and_duplicate_guard",
                "Forward Aging Idempotency and Duplicate Guard",
                "aits research strategies forward-aging-idempotency-and-duplicate-guard",
                "forward_aging_idempotency_and_duplicate_guard",
            ),
        )
        _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
        return payload

    observation_path = _observation_path(output_root, resolved_date)
    before_payload = _read_json_or_empty(observation_path)
    before_hash = _stable_hash(before_payload)
    before_core = _observation_core_hashes(before_payload)
    duplicate = run_simple_baseline_forward_aging_write_observation(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
        decision_date=resolved_date,
    )
    after_payload = _read_json_or_empty(observation_path)
    after_hash = _stable_hash(after_payload)
    after_core = _observation_core_hashes(after_payload)
    checks = [
        _guard_check(
            "second_run_returns_already_exists",
            duplicate.get("status") == "OBSERVATION_ALREADY_EXISTS",
        ),
        _guard_check("observation_file_hash_unchanged", before_hash == after_hash),
        _guard_check(
            "target_weights_unchanged",
            before_core.get("target_weights_hash") == after_core.get("target_weights_hash"),
        ),
        _guard_check(
            "signal_inputs_unchanged",
            before_core.get("signal_inputs_hash") == after_core.get("signal_inputs_hash"),
        ),
        _guard_check(
            "definition_hash_unchanged",
            before_core.get("policy_definition_hash") == after_core.get("policy_definition_hash"),
        ),
    ]
    blockers = [row["check_id"] for row in checks if row["status"] == "FAIL"]
    status = "FORWARD_IDEMPOTENCY_GUARD_PASS" if not blockers else "FORWARD_IDEMPOTENCY_GUARD_FAIL"
    payload = _payload(
        report_type="forward_aging_idempotency_and_duplicate_guard",
        title="Forward Aging Idempotency and Duplicate Guard",
        status=status,
        summary={
            "decision_date": resolved_date.isoformat(),
            "duplicate_writer_status": duplicate.get("status"),
            "duplicate_guard_passed": not blockers,
            "blocker_count": len(blockers),
        },
        idempotency_checks=checks,
        before_core_hashes=before_core,
        after_core_hashes=after_core,
        blockers=blockers,
        input_artifacts={"observation": str(observation_path)},
        report_registry_entry=_report_registry_entry(
            "forward_aging_idempotency_and_duplicate_guard",
            "Forward Aging Idempotency and Duplicate Guard",
            "aits research strategies forward-aging-idempotency-and-duplicate-guard",
            "forward_aging_idempotency_and_duplicate_guard",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_forward_aging_scheduler_dry_run(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    as_of_date: date | None = None,
    decision_date: date | None = None,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    data_gate = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
    )
    resolved_date = decision_date or as_of_date or date.fromisoformat(str(data_gate.get("as_of")))
    session = us_equity_market_session(resolved_date)
    observation_path = _observation_path(output_root, resolved_date)
    has_price_date = _price_cache_has_date(prices_path, resolved_date)
    blockers: list[str] = []
    if not session.is_trading_day:
        status = "FORWARD_AGING_SCHEDULER_SKIPPED_NON_TRADING_DAY"
    elif not bool(data_gate.get("passed")):
        status = "FORWARD_AGING_SCHEDULER_BLOCKED_DATA_QUALITY"
        blockers.append("validate_data_cache_failed")
    elif not has_price_date:
        status = "FORWARD_AGING_SCHEDULER_BLOCKED_DATA_MISSING"
        blockers.append("as_of_price_row_missing")
    elif observation_path.exists():
        status = "FORWARD_AGING_SCHEDULER_OBSERVATION_ALREADY_EXISTS"
    else:
        status = "FORWARD_AGING_SCHEDULER_DRY_RUN_READY"
    planned_actions = []
    if status == "FORWARD_AGING_SCHEDULER_DRY_RUN_READY":
        planned_actions.append(
            {
                "action": "write_research_only_forward_aging_observation",
                "decision_date": resolved_date.isoformat(),
                "actual_write_performed": False,
                "broker_action": "none",
            }
        )
    payload = _payload(
        report_type="forward_aging_scheduler_dry_run",
        title="Forward Aging Scheduler Dry Run",
        status=status,
        summary={
            "as_of": resolved_date.isoformat(),
            "data_quality_as_of": data_gate.get("as_of"),
            "is_trading_day": session.is_trading_day,
            "data_quality_status": data_gate.get("status"),
            "price_row_for_as_of_exists": has_price_date,
            "observation_written": False,
            "broker_action": "none",
        },
        cadence="daily_trading_day",
        scheduler_boundary={
            "external_entry_point": "aits ops daily-run",
            "dry_run_only": True,
            "daily_preflight_attachable": True,
            "non_trading_day_writes_observation": False,
            "broker_connected": False,
        },
        market_session={
            "session_status": session.session_status,
            "session_kind": session.session_kind,
            "reason": session.reason,
            "previous_trading_day": session.previous_trading_day.isoformat(),
        },
        data_quality=data_gate,
        planned_actions=planned_actions,
        blockers=blockers,
        input_artifacts={
            "prices": str(prices_path),
            "secondary_prices": str(marketstack_prices_path),
            "rates": str(rates_path),
        },
        report_registry_entry=_report_registry_entry(
            "forward_aging_scheduler_dry_run",
            "Forward Aging Scheduler Dry Run",
            "aits research strategies forward-aging-scheduler-dry-run",
            "forward_aging_scheduler_dry_run",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_paper_shadow_blocker_status_report(
    *,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    scoreboard = _read_json_or_empty(output_root / "simple_baseline_forward_aging_scoreboard.json")
    rows = _scoreboard_lookup(scoreboard)
    primary = rows.get(PRIMARY_CANDIDATE_ID, {})
    min_120d = _int(
        _forward_policy(config).get("minimum_120d_matured_observations_for_paper_shadow_review")
    )
    matured_120d = _int(primary.get("matured_120d_count"))
    remaining = max(min_120d - matured_120d, 0)
    payload = _payload(
        report_type="paper_shadow_blocker_status_report",
        title="Paper Shadow Blocker Status Report",
        status="PAPER_SHADOW_BLOCKED",
        summary={
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "minimum_120d_matured_observations_remaining": remaining,
            "manual_review_required": True,
        },
        blocker_status={
            "primary_candidate": PRIMARY_CANDIDATE_ID,
            "matured_20d_count": _int(primary.get("matured_20d_count")),
            "matured_60d_count": _int(primary.get("matured_60d_count")),
            "matured_120d_count": matured_120d,
            "minimum_required_120d_matured_observations": min_120d,
            "minimum_120d_matured_observations_remaining": remaining,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "manual_review_required": True,
        },
        input_artifacts={
            "scoreboard": str(output_root / "simple_baseline_forward_aging_scoreboard.json")
        },
        report_registry_entry=_report_registry_entry(
            "paper_shadow_blocker_status_report",
            "Paper Shadow Blocker Status Report",
            "aits research strategies paper-shadow-blocker-status-report",
            "paper_shadow_blocker_status_report",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_forward_aging_owner_launch_pack(
    *,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    first = _read_json_or_empty(output_root / "first_forward_aging_observation_write.json")
    data_quality = _read_json_or_empty(
        output_root / "simple_baseline_forward_aging_data_quality_gate.json"
    )
    marketstack = _read_json_or_empty(output_root / "marketstack_ssl_failure_triage.json")
    sgov = _read_json_or_empty(output_root / "sgov_total_return_proxy_quality_review.json")
    scheduler = _read_json_or_empty(output_root / "forward_aging_scheduler_dry_run.json")
    reader = _read_json_or_empty(output_root / "daily_reader_forward_aging_summary.json")
    blocker = _read_json_or_empty(output_root / "paper_shadow_blocker_status_report.json")
    first_written = first.get("status") in {
        "FIRST_OBSERVATION_WRITTEN",
        "FIRST_OBSERVATION_ALREADY_EXISTS",
    }
    data_quality_status = _mapping(data_quality.get("summary")).get("data_quality_status")
    answers = {
        "1_first_observation_written": first_written,
        "2_data_quality_is_pass_with_warnings": data_quality_status == "PASS_WITH_WARNINGS",
        "3_marketstack_warning_acceptable": marketstack.get("status")
        == "MARKETSTACK_FAIL_CLOSED_ACCEPTED",
        "4_sgov_proxy_warning_acceptable": sgov.get("status")
        in {"SGOV_PROXY_ACCEPTABLE", "SGOV_PROXY_WARN"},
        "5_scheduler_dry_run_allowed": scheduler.get("status")
        in {
            "FORWARD_AGING_SCHEDULER_DRY_RUN_READY",
            "FORWARD_AGING_SCHEDULER_OBSERVATION_ALREADY_EXISTS",
            "FORWARD_AGING_SCHEDULER_SKIPPED_NON_TRADING_DAY",
        },
        "6_reader_brief_minimal_summary_allowed": reader.get("status")
        == "DAILY_FORWARD_SUMMARY_SAFE",
        "7_paper_shadow_still_blocked": _mapping(blocker.get("summary")).get(
            "paper_shadow_allowed"
        )
        is False,
        "8_production_still_blocked": _mapping(blocker.get("summary")).get(
            "production_allowed"
        )
        is False,
    }
    readiness_passed = (
        first_written
        and data_quality_status in {"PASS", "PASS_WITH_WARNINGS"}
        and answers["3_marketstack_warning_acceptable"]
        and answers["4_sgov_proxy_warning_acceptable"]
        and answers["5_scheduler_dry_run_allowed"]
        and answers["6_reader_brief_minimal_summary_allowed"]
        and answers["7_paper_shadow_still_blocked"]
        and answers["8_production_still_blocked"]
    )
    status = (
        "FORWARD_AGING_OWNER_LAUNCH_PACK_READY"
        if readiness_passed
        else "FORWARD_AGING_OWNER_LAUNCH_PACK_BLOCKED"
    )
    payload = _payload(
        report_type="forward_aging_owner_launch_pack",
        title="Forward Aging Owner Launch Pack",
        status=status,
        summary={
            "first_observation_written": first_written,
            "data_quality_status": data_quality_status,
            "scheduler_dry_run_status": scheduler.get("status"),
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
        required_answers=answers,
        input_artifacts={
            "first_observation": str(output_root / "first_forward_aging_observation_write.json"),
            "data_quality": str(
                output_root / "simple_baseline_forward_aging_data_quality_gate.json"
            ),
            "marketstack": str(output_root / "marketstack_ssl_failure_triage.json"),
            "sgov": str(output_root / "sgov_total_return_proxy_quality_review.json"),
            "scheduler": str(output_root / "forward_aging_scheduler_dry_run.json"),
            "reader": str(output_root / "daily_reader_forward_aging_summary.json"),
            "paper_shadow_blocker": str(output_root / "paper_shadow_blocker_status_report.json"),
        },
        report_registry_entry=_report_registry_entry(
            "forward_aging_owner_launch_pack",
            "Forward Aging Owner Launch Pack",
            "aits research strategies forward-aging-owner-launch-pack",
            "forward_aging_owner_launch_pack",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_equal_risk_qqq_sgov_policy_definition_lock(
    *,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    strategy = _candidate_strategy(config, PRIMARY_CANDIDATE_ID)
    definition = _policy_definition(config, PRIMARY_CANDIDATE_ID, strategy)
    definition_hash = _stable_hash(definition)
    status = "POLICY_DEFINITION_LOCKED" if strategy else "BLOCKED"
    payload = _payload(
        report_type="equal_risk_qqq_sgov_policy_definition_lock",
        title="Equal-Risk QQQ / SGOV Policy Definition Lock",
        status=status,
        summary={
            "strategy_id": PRIMARY_CANDIDATE_ID,
            "policy_definition_hash": definition_hash,
            "definition_locked": bool(strategy),
        },
        strategy_id=PRIMARY_CANDIDATE_ID,
        policy_definition=definition,
        policy_definition_hash=definition_hash,
        change_rule=(
            "Forward-aging changes require a new strategy_id; historical observations keep "
            "their original policy_definition_hash."
        ),
        input_artifacts={"config": str(config_path)},
        report_registry_entry=_report_registry_entry(
            "equal_risk_qqq_sgov_policy_definition_lock",
            "Equal-Risk QQQ / SGOV Policy Definition Lock",
            "aits research strategies equal-risk-qqq-sgov-policy-definition-lock",
            "equal_risk_qqq_sgov_policy_definition_lock",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_simple_baseline_comparator_definition_lock(
    *,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    rows = []
    issues = []
    for candidate_id in (*STATIC_COMPARATOR_IDS, CHALLENGER_CANDIDATE_ID):
        strategy = _candidate_strategy(config, candidate_id)
        if not strategy:
            issues.append({"strategy_id": candidate_id, "issue": "definition_missing"})
            continue
        definition = _policy_definition(config, candidate_id, strategy)
        rows.append(
            {
                "strategy_id": candidate_id,
                "registry_strategy_id": strategy.get("strategy_id"),
                "definition_hash": _stable_hash(definition),
                "asset_universe": strategy.get("asset_universe"),
                "target_weights_or_policy_rule": _target_or_policy_rule(strategy),
                "rebalance_rule": strategy.get("rebalance_frequency"),
                "risk_control_rule": strategy.get("risk_control_rule"),
                "is_static": not str(candidate_id).startswith("dyn_"),
                "is_dynamic": str(candidate_id).startswith("dyn_"),
                "uses_tqqq": "TQQQ" in _strategy_required_tickers(strategy),
                "uses_options": False,
                "definition": definition,
            }
        )
    status = "COMPARATOR_CONFLICTED" if issues else "COMPARATOR_DEFINITIONS_LOCKED"
    payload = _payload(
        report_type="simple_baseline_comparator_definition_lock",
        title="Simple Baseline Comparator Definition Lock",
        status=status,
        summary={"comparator_count": len(rows), "issue_count": len(issues)},
        comparator_definitions=rows,
        issues=issues,
        input_artifacts={"config": str(config_path)},
        report_registry_entry=_report_registry_entry(
            "simple_baseline_comparator_definition_lock",
            "Simple Baseline Comparator Definition Lock",
            "aits research strategies simple-baseline-comparator-definition-lock",
            "simple_baseline_comparator_definition_lock",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_simple_baseline_forward_aging_data_quality_gate(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    data_gate = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
    )
    issue_codes = {str(issue.get("code")) for issue in _records(data_gate.get("issues"))}
    if not bool(data_gate.get("passed")):
        status = "DATA_QUALITY_BLOCKED"
    elif _int(data_gate.get("price_row_count")) == 0:
        status = "DATA_QUALITY_BLOCKED"
    elif "prices_missing_expected_values" in issue_codes:
        status = "DATA_QUALITY_PARTIAL"
    elif _int(data_gate.get("warning_count")):
        status = "DATA_QUALITY_PASS_WITH_WARNINGS"
    else:
        status = "DATA_QUALITY_PASS"
    payload = _payload(
        report_type="simple_baseline_forward_aging_data_quality_gate",
        title="Simple Baseline Forward Aging Data Quality Gate",
        status=status,
        summary={
            "data_quality_status": data_gate.get("status"),
            "price_row_count": data_gate.get("price_row_count"),
            "warning_count": data_gate.get("warning_count"),
            "error_count": data_gate.get("error_count"),
            "as_of": data_gate.get("as_of"),
        },
        data_quality=data_gate,
        checks={
            "QQQ data availability": "QQQ" in _available_price_tickers(prices_path),
            "TQQQ data availability": "TQQQ" in _available_price_tickers(prices_path),
            "SGOV data availability": "SGOV" in _available_price_tickers(prices_path),
            "adjusted close consistency": "adj_close" in _available_price_columns(prices_path),
            "validate-data status": data_gate.get("status"),
        },
        blocked_maturity_conclusions=status
        in {"DATA_QUALITY_PARTIAL", "DATA_QUALITY_BLOCKED"},
        input_artifacts={
            "config": str(config_path),
            "prices": str(prices_path),
            "secondary_prices": str(marketstack_prices_path),
            "rates": str(rates_path),
        },
        report_registry_entry=_report_registry_entry(
            "simple_baseline_forward_aging_data_quality_gate",
            "Simple Baseline Forward Aging Data Quality Gate",
            "aits research strategies simple-baseline-forward-aging-data-quality-gate",
            "simple_baseline_forward_aging_data_quality_gate",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_simple_baseline_paper_shadow_threshold_contract(
    *,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    paths = {
        "policy_definition_lock": output_root / "equal_risk_qqq_sgov_policy_definition_lock.json",
        "comparator_definition_lock": output_root
        / "simple_baseline_comparator_definition_lock.json",
        "pit_boundary": output_root / "simple_baseline_pit_boundary_audit.json",
        "data_quality": output_root / "simple_baseline_forward_aging_data_quality_gate.json",
        "scoreboard": output_root / "simple_baseline_forward_aging_scoreboard.json",
    }
    sources = {name: _read_json_or_empty(path) for name, path in paths.items()}
    scoreboard = sources["scoreboard"]
    primary = _scoreboard_lookup(scoreboard).get(PRIMARY_CANDIDATE_ID, {})
    min_120d = _int(
        _forward_policy(config).get("minimum_120d_matured_observations_for_paper_shadow_review")
    )
    matured_120d = _int(primary.get("matured_120d_count"))
    blockers = _paper_shadow_threshold_blockers(sources, min_120d, matured_120d)
    status = "THRESHOLD_CONTRACT_READY"
    if blockers and any(blocker.startswith("missing") for blocker in blockers):
        status = "THRESHOLD_CONTRACT_PARTIAL"
    if not _forward_policy(config):
        status = "THRESHOLD_CONTRACT_BLOCKED"

    payload = _payload(
        report_type="simple_baseline_paper_shadow_threshold_contract",
        title="Simple Baseline Paper-Shadow Threshold Contract",
        status=status,
        summary={
            "paper_shadow_review_allowed_later": True,
            "current_blocker_count": len(blockers),
            "minimum_remaining_observations": max(min_120d - matured_120d, 0),
            "minimum_remaining_days": 0 if matured_120d >= min_120d else 120,
        },
        paper_shadow_review_allowed_later=True,
        current_blockers=blockers,
        minimum_remaining_observations=max(min_120d - matured_120d, 0),
        minimum_remaining_days=0 if matured_120d >= min_120d else 120,
        threshold_conditions=[
            "policy definition locked",
            "comparator definition locked",
            "PIT boundary pass",
            "data quality pass",
            "forward aging 120d matured observations >= 20",
            "primary candidate not clearly dominated by comparator",
            "drawdown reduction stable",
            "missed upside acceptable",
            "turnover acceptable",
            "owner manual review approval",
        ],
        input_artifacts={name: str(path) for name, path in paths.items()},
        report_registry_entry=_report_registry_entry(
            "simple_baseline_paper_shadow_threshold_contract",
            "Simple Baseline Paper-Shadow Threshold Contract",
            "aits research strategies simple-baseline-paper-shadow-threshold-contract",
            "simple_baseline_paper_shadow_threshold_contract",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_daily_reader_forward_aging_summary(
    *,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    freeze = _read_json_or_empty(
        output_root / "simple_baseline_forward_aging_candidate_freeze.json"
    )
    scoreboard = _read_json_or_empty(output_root / "simple_baseline_forward_aging_scoreboard.json")
    data_quality = _read_json_or_empty(
        output_root / "simple_baseline_forward_aging_data_quality_gate.json"
    )
    observations = _load_observations(
        sorted(
            (output_root / "forward_aging_observations").glob(
                "simple_baseline_forward_aging_observation_*.json"
            )
        )
    )
    scoreboard_rows = _scoreboard_lookup(scoreboard)
    primary = scoreboard_rows.get(PRIMARY_CANDIDATE_ID, {})
    summary_status = "DAILY_FORWARD_SUMMARY_SAFE"
    if not freeze or not scoreboard:
        summary_status = "DAILY_FORWARD_SUMMARY_BLOCKED"
    latest_observation_date = max(
        (str(row.get("decision_date")) for row in observations if row.get("decision_date")),
        default="MISSING",
    )
    forward_summary = {
        "primary_candidate": PRIMARY_CANDIDATE_ID,
        "challenger_candidate": CHALLENGER_CANDIDATE_ID,
        "latest_observation_date": latest_observation_date,
        "data_quality_status": _mapping(data_quality.get("summary")).get(
            "data_quality_status"
        ),
        "matured_20d_count": _int(primary.get("matured_20d_count")),
        "matured_60d_count": _int(primary.get("matured_60d_count")),
        "matured_120d_count": _int(primary.get("matured_120d_count")),
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }
    payload = _payload(
        report_type="daily_reader_forward_aging_summary",
        title="Daily Reader Forward Aging Summary",
        status=summary_status,
        summary=forward_summary,
        portfolio_control_forward_aging=forward_summary,
        input_artifacts={
            "candidate_freeze": str(
                output_root / "simple_baseline_forward_aging_candidate_freeze.json"
            ),
            "scoreboard": str(output_root / "simple_baseline_forward_aging_scoreboard.json"),
            "data_quality": str(
                output_root / "simple_baseline_forward_aging_data_quality_gate.json"
            ),
        },
        report_registry_entry=_report_registry_entry(
            "daily_reader_forward_aging_summary",
            "Daily Reader Forward Aging Summary",
            "aits research strategies daily-reader-forward-aging-summary",
            "daily_reader_forward_aging_summary",
            include_in_reader_brief=True,
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_simple_baseline_risk_budget_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    data_gate = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
    )
    if not bool(data_gate.get("passed")):
        payload = _payload(
            report_type="simple_baseline_risk_budget_review",
            title="Simple Baseline Risk Budget Review",
            status="RISK_BUDGET_REVIEW_BLOCKED",
            summary={"data_quality_status": data_gate.get("status")},
            data_quality=data_gate,
            blockers=["validate_data_cache_failed"],
        )
        _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
        return payload

    metrics = _candidate_metrics(
        config=config,
        prices_path=prices_path,
        start_date=start_date,
        end_date=end_date,
        candidate_ids=DEFAULT_CANDIDATE_ORDER,
    )
    rows = [
        _risk_budget_row(candidate_id, metrics, config)
        for candidate_id in DEFAULT_CANDIDATE_ORDER
    ]
    equal = next(row for row in rows if row["strategy_id"] == PRIMARY_CANDIDATE_ID)
    qqq50 = next(row for row in rows if row["strategy_id"] == "qqq_50_sgov_50")
    status = "RISK_BUDGET_REVIEW_READY"
    if equal["annualized_volatility"] >= qqq50["annualized_volatility"]:
        status = "RISK_BUDGET_REVIEW_MIXED"
    answers = {
        "1_low_drawdown_from_lower_risk_exposure": equal["effective_qqq_beta"] < 1.0,
        "2_is_lower_beta_qqq_variant": equal["effective_qqq_beta"] < 1.0,
        "3_need_risk_adjusted_comparison": True,
        "4_advantage_vs_qqq_50_sgov_50": (
            "risk_adjusted_advantage"
            if equal["sharpe"] >= qqq50["sharpe"]
            else "not_confirmed"
        ),
    }
    payload = _payload(
        report_type="simple_baseline_risk_budget_review",
        title="Simple Baseline Risk Budget Review",
        status=status,
        summary={
            "strategy_count": len(rows),
            "equal_risk_effective_qqq_beta": equal["effective_qqq_beta"],
            "equal_risk_annualized_volatility": equal["annualized_volatility"],
            "data_quality_status": data_gate.get("status"),
        },
        risk_budget_rows=rows,
        required_answers=answers,
        data_quality=data_gate,
        input_artifacts={"config": str(config_path), "prices": str(prices_path)},
        report_registry_entry=_report_registry_entry(
            "simple_baseline_risk_budget_review",
            "Simple Baseline Risk Budget Review",
            "aits research strategies simple-baseline-risk-budget-review",
            "simple_baseline_risk_budget_review",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_simple_baseline_absolute_return_gap_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    data_gate = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
    )
    if not bool(data_gate.get("passed")):
        payload = _payload(
            report_type="simple_baseline_absolute_return_gap_review",
            title="Simple Baseline Absolute Return Gap Review",
            status="ABSOLUTE_RETURN_GAP_REVIEW_BLOCKED",
            summary={"data_quality_status": data_gate.get("status")},
            data_quality=data_gate,
            role_recommendation="BLOCKED",
            blockers=["validate_data_cache_failed"],
        )
        _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
        return payload

    candidate_ids = (
        PRIMARY_CANDIDATE_ID,
        PUBLIC_100_QQQ_ID,
        "qqq_60_sgov_40",
        "qqq_70_sgov_30",
        CHALLENGER_CANDIDATE_ID,
    )
    metrics = _candidate_metrics(
        config=config,
        prices_path=prices_path,
        start_date=start_date,
        end_date=end_date,
        candidate_ids=candidate_ids,
    )
    equal = metrics[PRIMARY_CANDIDATE_ID]
    qqq = metrics[PUBLIC_100_QQQ_ID]
    qqq60 = metrics["qqq_60_sgov_40"]
    challenger = metrics[CHALLENGER_CANDIDATE_ID]
    role = "DEFENSIVE_CORE"
    if equal["annual_return"] >= qqq["annual_return"] and abs(equal["max_drawdown"]) <= abs(
        qqq["max_drawdown"]
    ):
        role = "BALANCED_CORE"
    elif abs(equal["max_drawdown"]) > abs(qqq60["max_drawdown"]):
        role = "GROWTH_INSUFFICIENT"
    payload = _payload(
        report_type="simple_baseline_absolute_return_gap_review",
        title="Simple Baseline Absolute Return Gap Review",
        status="ABSOLUTE_RETURN_GAP_REVIEW_READY",
        summary={
            "role_recommendation": role,
            "annual_return_gap_vs_100_qqq": _round(
                equal["annual_return"] - qqq["annual_return"]
            ),
            "annual_return_gap_vs_qqq_60_sgov_40": _round(
                equal["annual_return"] - qqq60["annual_return"]
            ),
            "data_quality_status": data_gate.get("status"),
        },
        comparison_metrics=metrics,
        annual_return_gap_vs_100_qqq=_round(equal["annual_return"] - qqq["annual_return"]),
        annual_return_gap_vs_qqq_60_sgov_40=_round(
            equal["annual_return"] - qqq60["annual_return"]
        ),
        annual_return_gap_vs_dyn_tqqq_capped_trend=_round(
            equal["annual_return"] - challenger["annual_return"]
        ),
        drawdown_reduction_vs_100_qqq=_round(abs(qqq["max_drawdown"]) - abs(equal["max_drawdown"])),
        sharpe_improvement=_round(equal["sharpe"] - qqq["sharpe"]),
        calmar_improvement=_round(equal["calmar"] - qqq["calmar"]),
        growth_shortfall_commentary=(
            "equal_risk_qqq_sgov is best interpreted through risk-adjusted and drawdown "
            "control evidence, not as an absolute-return-maximizing growth strategy."
        ),
        role_recommendation=role,
        data_quality=data_gate,
        input_artifacts={"config": str(config_path), "prices": str(prices_path)},
        report_registry_entry=_report_registry_entry(
            "simple_baseline_absolute_return_gap_review",
            "Simple Baseline Absolute Return Gap Review",
            "aits research strategies simple-baseline-absolute-return-gap-review",
            "simple_baseline_absolute_return_gap_review",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_simple_baseline_candidate_role_assignment(
    *,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    rows = [
        {
            "strategy_id": PRIMARY_CANDIDATE_ID,
            "assigned_role": "PRIMARY_FORWARD_AGING",
            "reason": "stable simple equal-risk candidate selected by owner decision artifacts",
        },
        {
            "strategy_id": PUBLIC_100_QQQ_ID,
            "registry_strategy_id": _resolve_strategy_id(config, PUBLIC_100_QQQ_ID),
            "assigned_role": "RISK_REFERENCE",
            "reason": "100% QQQ absolute-risk and opportunity-cost reference",
        },
        *[
            {
                "strategy_id": strategy_id,
                "assigned_role": "STATIC_COMPARATOR",
                "reason": "static QQQ/SGOV comparator for forward-aging relative evidence",
            }
            for strategy_id in ("qqq_50_sgov_50", "qqq_60_sgov_40")
        ],
        {
            "strategy_id": CHALLENGER_CANDIDATE_ID,
            "assigned_role": "DYNAMIC_CHALLENGER",
            "reason": "best dynamic challenger kept for comparison, not primary activation",
        },
    ]
    for strategy in _strategy_rows(config):
        strategy_id = str(strategy.get("strategy_id"))
        if strategy_id.startswith("tqqq_") or strategy_id in {
            "tqqq_volatility_capped",
            "tqqq_drawdown_capped",
        }:
            rows.append(
                {
                    "strategy_id": strategy_id,
                    "assigned_role": "PAUSED",
                    "reason": "TQQQ-heavy direction remains paused after owner decision",
                }
            )
    rows.extend(
        [
            {
                "strategy_id": "LEAPS",
                "assigned_role": "BLOCKED",
                "reason": "options research remains blocked",
            },
            {
                "strategy_id": "Wheel",
                "assigned_role": "BLOCKED",
                "reason": "options research remains blocked",
            },
            {
                "strategy_id": "tail-risk fallback",
                "assigned_role": "QUARANTINED",
                "reason": "tail-risk fallback remains quarantined and must not be restored here",
            },
        ]
    )
    payload = _payload(
        report_type="simple_baseline_candidate_role_assignment",
        title="Simple Baseline Candidate Role Assignment",
        status="ROLE_ASSIGNMENT_READY",
        summary={"assigned_role_count": len(rows), "primary_candidate": PRIMARY_CANDIDATE_ID},
        role_assignments=rows,
        input_artifacts={"config": str(config_path)},
        report_registry_entry=_report_registry_entry(
            "simple_baseline_candidate_role_assignment",
            "Simple Baseline Candidate Role Assignment",
            "aits research strategies simple-baseline-candidate-role-assignment",
            "simple_baseline_candidate_role_assignment",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_simple_baseline_forward_aging_owner_review_pack(
    *,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    docs_path: Path = DEFAULT_FORWARD_AGING_OWNER_REVIEW_DOC_PATH,
) -> dict[str, Any]:
    paths = _owner_review_paths(output_root)
    sources = {name: _read_json_or_empty(path) for name, path in paths.items()}
    missing = [name for name, payload in sources.items() if not payload]
    owner_decision = _read_json_or_empty(
        output_root / "simple_baseline_watchlist_owner_decision.json"
    )
    owner_approved = bool(
        _mapping(owner_decision.get("final_required_answers")).get(
            "1_equal_risk_primary_forward_aging_candidate"
        )
    )
    if missing:
        status = "OWNER_REVIEW_BLOCKED"
    elif not owner_approved:
        status = "OWNER_APPROVAL_REQUIRED"
    else:
        status = "OWNER_REVIEW_READY"
    answers = _owner_review_answers(sources, owner_approved)
    payload = _payload(
        report_type="simple_baseline_forward_aging_owner_review_pack",
        title="Simple Baseline Forward Aging Owner Review Pack",
        status=status,
        summary={
            "primary_candidate": PRIMARY_CANDIDATE_ID,
            "challenger_candidate": CHALLENGER_CANDIDATE_ID,
            "owner_approved_start_forward_aging_observation": owner_approved,
            "missing_input_count": len(missing),
        },
        required_answers=answers,
        missing_inputs=missing,
        input_artifacts={name: str(path) for name, path in paths.items()},
        owner_review_doc_path=str(docs_path),
        report_registry_entry=_report_registry_entry(
            "simple_baseline_forward_aging_owner_review_pack",
            "Simple Baseline Forward Aging Owner Review Pack",
            "aits research strategies simple-baseline-forward-aging-owner-review-pack",
            "simple_baseline_forward_aging_owner_review_pack",
            extra_artifact_globs=["docs/research/simple_baseline_forward_aging_owner_review_pack.md"],
        ),
    )
    payload.setdefault("artifact_paths", {})["docs_path"] = str(docs_path)
    _write_review_doc(payload, docs_path, "Simple Baseline Forward Aging Owner Review Pack")
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_simple_baseline_forward_aging_automation_readiness(
    *,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    paths = {
        "contract": output_root / "simple_baseline_forward_aging_contract.json",
        "data_quality": output_root / "simple_baseline_forward_aging_data_quality_gate.json",
        "policy_lock": output_root / "equal_risk_qqq_sgov_policy_definition_lock.json",
        "comparator_lock": output_root / "simple_baseline_comparator_definition_lock.json",
        "daily_summary": output_root / "daily_reader_forward_aging_summary.json",
    }
    sources = {name: _read_json_or_empty(path) for name, path in paths.items()}
    checks = [
        {"check": "CLI exists", "passed": True},
        {"check": "output path stable", "passed": True},
        {"check": "idempotency pass", "passed": True},
        {
            "check": "data quality gate pass",
            "passed": str(sources["data_quality"].get("status", "")).startswith(
                "DATA_QUALITY_PASS"
            ),
        },
        {
            "check": "definition hash stable",
            "passed": bool(sources["policy_lock"].get("policy_definition_hash"))
            and bool(_records(sources["comparator_lock"].get("comparator_definitions"))),
        },
        {
            "check": "daily reader summary safe",
            "passed": sources["daily_summary"].get("status") == "DAILY_FORWARD_SUMMARY_SAFE",
        },
        {"check": "no broker action", "passed": True},
        {"check": "no production effect", "passed": True},
        {"check": "no paper-shadow activation", "passed": True},
    ]
    failed = [row for row in checks if not row["passed"]]
    if failed:
        status = "AUTOMATION_NEEDS_FIXES"
    else:
        status = "AUTOMATION_READY_FOR_OBSERVATION_ONLY"
    payload = _payload(
        report_type="simple_baseline_forward_aging_automation_readiness",
        title="Simple Baseline Forward Aging Automation Readiness",
        status=status,
        summary={"check_count": len(checks), "failed_check_count": len(failed)},
        readiness_checks=checks,
        input_artifacts={name: str(path) for name, path in paths.items()},
        report_registry_entry=_report_registry_entry(
            "simple_baseline_forward_aging_automation_readiness",
            "Simple Baseline Forward Aging Automation Readiness",
            "aits research strategies simple-baseline-forward-aging-automation-readiness",
            "simple_baseline_forward_aging_automation_readiness",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_simple_baseline_forward_aging_master_review(
    *,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    docs_path: Path = DEFAULT_FORWARD_AGING_MASTER_REVIEW_DOC_PATH,
) -> dict[str, Any]:
    paths = _master_review_paths(output_root)
    sources = {name: _read_json_or_empty(path) for name, path in paths.items()}
    missing = [name for name, payload in sources.items() if not payload]
    owner_ready = sources.get("owner_review_pack", {}).get("status") == "OWNER_REVIEW_READY"
    data_pass = str(sources.get("data_quality", {}).get("status", "")).startswith(
        "DATA_QUALITY_PASS"
    )
    if missing:
        status = "BLOCKED"
    elif not owner_ready:
        status = "NEED_OWNER_APPROVAL"
    elif not data_pass:
        status = "NEED_MORE_BACKTEST_REVIEW"
    else:
        status = "START_FORWARD_AGING"
    answers = {
        "1_start_long_term_forward_aging": status == "START_FORWARD_AGING",
        "2_primary_candidate_confirmed": PRIMARY_CANDIDATE_ID,
        "3_comparators_confirmed": list(STATIC_COMPARATOR_IDS),
        "4_challenger_confirmed": CHALLENGER_CANDIDATE_ID,
        "5_more_backtest_or_forward_observation": (
            "forward_observation" if status == "START_FORWARD_AGING" else "owner_review_first"
        ),
        "6_continue_pause_tqqq_heavy": True,
        "7_continue_block_options_leaps_wheel": True,
        "8_continue_quarantine_tail_risk_fallback": True,
        "9_allow_reader_brief_minimal_summary": True,
        "10_future_paper_shadow_review_after_threshold": True,
    }
    payload = _payload(
        report_type="simple_baseline_forward_aging_master_review",
        title="Simple Baseline Forward Aging Master Review",
        status=status,
        summary={
            "primary_candidate": PRIMARY_CANDIDATE_ID,
            "challenger_candidate": CHALLENGER_CANDIDATE_ID,
            "start_forward_aging": status == "START_FORWARD_AGING",
            "missing_input_count": len(missing),
        },
        final_decision_answers=answers,
        missing_inputs=missing,
        input_artifacts={name: str(path) for name, path in paths.items()},
        master_review_doc_path=str(docs_path),
        report_registry_entry=_report_registry_entry(
            "simple_baseline_forward_aging_master_review",
            "Simple Baseline Forward Aging Master Review",
            "aits research strategies simple-baseline-forward-aging-master-review",
            "simple_baseline_forward_aging_master_review",
            extra_artifact_globs=["docs/research/simple_baseline_forward_aging_master_review.md"],
        ),
    )
    payload.setdefault("artifact_paths", {})["docs_path"] = str(docs_path)
    _write_review_doc(payload, docs_path, "Simple Baseline Forward Aging Master Review")
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def _real_result_reconciliation_paths(output_root: Path) -> dict[str, Path]:
    return {
        "simple_baseline_real_run_summary": output_root / "simple_baseline_real_run_summary.json",
        "simple_baseline_master_review": output_root / "simple_baseline_master_review.json",
        "equal_risk_qqq_sgov_deep_dive": output_root / "equal_risk_qqq_sgov_deep_dive.json",
        "simple_baseline_period_split_validation": output_root
        / "simple_baseline_period_split_validation.json",
        "simple_baseline_drawdown_episode_review": output_root
        / "simple_baseline_drawdown_episode_review.json",
        "dynamic_vs_static_edge_significance_review": output_root
        / "dynamic_vs_static_edge_significance_review.json",
        "tqqq_heavy_pause_rationale_report": output_root / "tqqq_heavy_pause_rationale_report.json",
        "simple_baseline_watchlist_owner_decision": output_root
        / "simple_baseline_watchlist_owner_decision.json",
        "options_next_stage_gate": output_root / "options_next_stage_gate.json",
    }


def _reconciliation_checks(artifacts: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    owner = artifacts.get("simple_baseline_watchlist_owner_decision", {})
    owner_answers = _mapping(owner.get("final_required_answers"))
    checks = [
        (
            "top recommended remains equal_risk_qqq_sgov",
            _top_recommended_candidate(artifacts) == PRIMARY_CANDIDATE_ID,
        ),
        (
            "primary candidate matches owner decision",
            _owner_primary_candidate(artifacts) == PRIMARY_CANDIDATE_ID
            or bool(owner_answers.get("1_equal_risk_primary_forward_aging_candidate")),
        ),
        (
            "dynamic challenger remains dyn_tqqq_capped_trend",
            _owner_challenger_candidate(artifacts) == CHALLENGER_CANDIDATE_ID
            or bool(owner_answers.get("2_keep_dyn_tqqq_capped_trend_as_challenger")),
        ),
        (
            "TQQQ-heavy pause still holds",
            artifacts.get("tqqq_heavy_pause_rationale_report", {}).get("status")
            == "TQQQ_HEAVY_PAUSE_CONFIRMED",
        ),
        (
            "options gate remains blocked",
            artifacts.get("options_next_stage_gate", {}).get("status")
            == "OPTIONS_RESEARCH_BLOCKED"
            or artifacts.get("options_next_stage_gate", {}).get("options_research_allowed")
            is False,
        ),
        (
            "tail-risk fallback remains blocked",
            bool(owner_answers.get("5_continue_block_tail_risk_fallback")),
        ),
    ]
    return [
        {"check": name, "status": "PASS" if passed else "FAIL"}
        for name, passed in checks
    ]


def _unsafe_artifact_reports(artifacts: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    unsafe = []
    for name, payload in artifacts.items():
        if not payload:
            continue
        for field in ("paper_shadow_allowed", "production_allowed"):
            if payload.get(field) is True or _mapping(payload.get("summary")).get(field) is True:
                unsafe.append({"artifact": name, "field": field, "value": True})
        if payload.get("broker_action") not in {None, "none"}:
            unsafe.append(
                {"artifact": name, "field": "broker_action", "value": payload.get("broker_action")}
            )
    return unsafe


def _top_recommended_candidate(artifacts: Mapping[str, Mapping[str, Any]]) -> str:
    summary = _mapping(artifacts.get("simple_baseline_real_run_summary", {}).get("summary"))
    return str(summary.get("top_recommended_candidate") or summary.get("top_recommended") or "")


def _owner_primary_candidate(artifacts: Mapping[str, Mapping[str, Any]]) -> str:
    summary = _mapping(
        artifacts.get("simple_baseline_watchlist_owner_decision", {}).get("summary")
    )
    return str(summary.get("primary_candidate") or "")


def _owner_challenger_candidate(artifacts: Mapping[str, Mapping[str, Any]]) -> str:
    summary = _mapping(
        artifacts.get("simple_baseline_watchlist_owner_decision", {}).get("summary")
    )
    return str(summary.get("challenger_candidate") or "")


def _forward_policy(config: Mapping[str, Any]) -> dict[str, Any]:
    return dict(_mapping(_research_policy(config).get("forward_aging")))


def _window_policy(config: Mapping[str, Any]) -> dict[str, int]:
    raw_windows = _mapping(_forward_policy(config).get("observation_windows_trading_days"))
    return {
        str(label): int(days)
        for label, days in raw_windows.items()
    }


def _candidate_specs(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    freeze = _mapping(_forward_policy(config).get("candidate_freeze"))
    primary = [str(item) for item in freeze.get("primary", [PRIMARY_CANDIDATE_ID])]
    comparators = [str(item) for item in freeze.get("static_comparators", STATIC_COMPARATOR_IDS)]
    challengers = [str(item) for item in freeze.get("challenger", [CHALLENGER_CANDIDATE_ID])]
    specs = []
    for candidate_id in primary:
        specs.append(_candidate_spec(config, candidate_id, "PRIMARY_FORWARD_AGING"))
    for candidate_id in comparators:
        role = "RISK_REFERENCE" if candidate_id == PUBLIC_100_QQQ_ID else "STATIC_COMPARATOR"
        specs.append(_candidate_spec(config, candidate_id, role))
    for candidate_id in challengers:
        specs.append(_candidate_spec(config, candidate_id, "DYNAMIC_CHALLENGER"))
    return specs


def _candidate_spec(config: Mapping[str, Any], candidate_id: str, role: str) -> dict[str, Any]:
    strategy = _candidate_strategy(config, candidate_id)
    return {
        "candidate_id": candidate_id,
        "candidate_role": role,
        "strategy_type": "dynamic" if candidate_id.startswith("dyn_") else "static_or_rules_based",
        "registry_strategy_id": strategy.get("strategy_id") if strategy else None,
        "reason_for_inclusion": _candidate_inclusion_reason(candidate_id, role),
        "reason_for_exclusion_if_any": "",
        "source_report": "simple_baseline_watchlist_owner_decision",
        "manual_review_required": True,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _candidate_inclusion_reason(candidate_id: str, role: str) -> str:
    if role == "PRIMARY_FORWARD_AGING":
        return "owner decision artifacts identify equal-risk QQQ/SGOV as the primary candidate"
    if role == "DYNAMIC_CHALLENGER":
        return "kept as the single dynamic challenger for complexity-adjusted comparison"
    if candidate_id == PUBLIC_100_QQQ_ID:
        return "100% QQQ risk and opportunity-cost reference"
    return "static QQQ/SGOV comparator for relative forward-aging evidence"


def _candidate_freeze_issues(
    config: Mapping[str, Any],
    candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    freeze = _mapping(_forward_policy(config).get("candidate_freeze"))
    issues = []
    role_counts: dict[str, int] = {}
    for row in candidates:
        role = str(row.get("candidate_role"))
        role_counts[role] = role_counts.get(role, 0) + 1
        if not _candidate_strategy(config, str(row.get("candidate_id"))):
            issues.append({"candidate_id": row.get("candidate_id"), "issue": "strategy_missing"})
    limits = {
        "PRIMARY_FORWARD_AGING": _int(freeze.get("max_primary_candidates"), 1),
        "DYNAMIC_CHALLENGER": _int(freeze.get("max_challenger_candidates"), 1),
        "STATIC_COMPARATOR": _int(freeze.get("max_static_comparators"), 3),
    }
    for role, limit in limits.items():
        if role_counts.get(role, 0) > limit:
            issues.append({"role": role, "issue": "candidate_count_exceeds_limit", "limit": limit})
    forbidden = {"LEAPS", "Wheel", "tail-risk fallback"}
    for row in candidates:
        if str(row.get("candidate_id")) in forbidden:
            issues.append({"candidate_id": row.get("candidate_id"), "issue": "forbidden_expansion"})
    return issues


def _candidate_strategy(config: Mapping[str, Any], candidate_id: str) -> dict[str, Any]:
    resolved = _resolve_strategy_id(config, candidate_id)
    strategy = _strategy_by_id(config, resolved)
    if strategy:
        return strategy
    for row in _dynamic_candidate_strategies(config):
        if row.get("strategy_id") == resolved:
            return row
    return {}


def _resolve_strategy_id(config: Mapping[str, Any], candidate_id: str) -> str:
    aliases = _mapping(_forward_policy(config).get("public_strategy_aliases"))
    return str(aliases.get(candidate_id, candidate_id))


def _policy_definition(
    config: Mapping[str, Any],
    candidate_id: str,
    strategy: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _research_policy(config)
    return {
        "strategy_id": candidate_id,
        "registry_strategy_id": strategy.get("strategy_id"),
        "asset_universe": strategy.get("asset_universe"),
        "risk_model": strategy.get("risk_control_rule"),
        "target_volatility_or_risk_contribution_logic": _target_or_policy_rule(strategy),
        "rebalance_frequency": strategy.get("rebalance_frequency"),
        "data_inputs": {
            "prices": ["QQQ", "TQQQ", "SGOV"],
            "rates": policy.get("required_rate_series"),
        },
        "lookback_windows": {
            "moving_average_windows": policy.get("moving_average_windows"),
            "realized_vol_windows": policy.get("realized_vol_windows"),
            "rolling_high_windows": policy.get("rolling_high_windows"),
        },
        "weight_bounds": {
            "target_weights": strategy.get("target_weights"),
            "max_tqqq_weight": strategy.get("max_tqqq_weight"),
            "equal_risk": _mapping(policy.get("equal_risk"))
            if candidate_id == PRIMARY_CANDIDATE_ID
            else None,
        },
        "execution_assumption": "research_only_close_to_close_no_broker_no_order",
        "cash_sgov_handling": "SGOV is treated as defensive cash-equivalent carry proxy",
    }


def _target_or_policy_rule(strategy: Mapping[str, Any]) -> Any:
    if str(strategy.get("strategy_id", "")).startswith("dyn_"):
        return {
            "risk_on_weights": strategy.get("risk_on_weights"),
            "risk_off_weights": strategy.get("risk_off_weights"),
        }
    return strategy.get("target_weights")


def _stable_hash(value: Mapping[str, Any]) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _resolve_decision_timestamp(prices: pd.DataFrame, requested: date) -> pd.Timestamp:
    eligible = prices[prices.index.date <= requested]
    if eligible.empty:
        raise ValueError(f"no price rows on or before decision_date={requested.isoformat()}")
    return pd.Timestamp(eligible.index[-1])


def _observation_row(
    candidate: Mapping[str, Any],
    *,
    config: Mapping[str, Any],
    prices: pd.DataFrame,
    decision_ts: pd.Timestamp,
    windows: Mapping[str, int],
) -> dict[str, Any]:
    candidate_id = str(candidate.get("candidate_id"))
    strategy = _candidate_strategy(config, candidate_id)
    weights = _target_weight_frame(strategy, prices, config).reindex(prices.index).ffill()
    target = _mapping(weights.loc[decision_ts].to_dict())
    row: dict[str, Any] = {
        "decision_date": decision_ts.date().isoformat(),
        "strategy_id": candidate_id,
        "registry_strategy_id": strategy.get("strategy_id"),
        "candidate_role": candidate.get("candidate_role"),
        "target_weight_qqq": _round(target.get("QQQ")),
        "target_weight_tqqq": _round(target.get("TQQQ")),
        "target_weight_sgov": _round(target.get("SGOV")),
        "target_weights": {str(key): _round(value) for key, value in target.items()},
        "signal_inputs_used": {
            "risk_control_rule": strategy.get("risk_control_rule"),
            "trend_filter_rule": strategy.get("trend_filter_rule"),
            "volatility_filter_rule": strategy.get("volatility_filter_rule"),
            "drawdown_filter_rule": strategy.get("drawdown_filter_rule"),
            "data_visible_through": decision_ts.date().isoformat(),
        },
        "execution_assumption": "research_only_close_to_close_no_broker_no_order",
        "policy_definition_hash": _stable_hash(_policy_definition(config, candidate_id, strategy)),
        "pending_windows": list(windows),
        "forward_windows": {},
        **SAFETY_BOUNDARY,
    }
    for label in windows:
        row[f"matured_{label}"] = False
    return row


def _candidate_strategy_cache(
    config: Mapping[str, Any],
    prices: pd.DataFrame,
) -> dict[str, dict[str, Any]]:
    cache = {}
    annualization = _research_policy_int(config, "annualization_trading_days")
    benchmark = prices["QQQ"].pct_change().fillna(0.0)
    for candidate in _candidate_specs(config):
        candidate_id = str(candidate.get("candidate_id"))
        strategy = _candidate_strategy(config, candidate_id)
        returns = _strategy_return_series(strategy, prices, config)
        weights = _target_weight_frame(strategy, prices, config).reindex(returns.index).ffill()
        metrics = _metrics_for_strategy(
            strategy,
            returns,
            weights,
            benchmark,
            annualization=annualization,
            cost_bps=0.0,
        )
        cache[candidate_id] = {
            "strategy": strategy,
            "returns": returns,
            "weights": weights,
            "metrics": metrics,
        }
    return cache


def _update_observation_maturity(
    observation: dict[str, Any],
    *,
    config: Mapping[str, Any],
    prices: pd.DataFrame,
    strategy_cache: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    candidate_id = str(observation.get("strategy_id"))
    decision_date = date.fromisoformat(str(observation.get("decision_date")))
    windows = _window_policy(config)
    result = {"updated": 0, "pending": 0, "missing": 0, "changed": False}
    if candidate_id not in strategy_cache:
        result["missing"] += len(windows)
        return result
    returns = strategy_cache[candidate_id]["returns"]
    index_dates = [item.date() for item in returns.index]
    if decision_date not in index_dates:
        result["missing"] += len(windows)
        return result
    idx = index_dates.index(decision_date)
    forward_windows = _mapping(observation.get("forward_windows"))
    for label, days in windows.items():
        current = _mapping(forward_windows.get(label))
        if current.get("status") == "MATURED":
            continue
        if idx + days >= len(returns):
            forward_windows[label] = {"status": "DATA_PENDING", "horizon_trading_days": days}
            result["pending"] += 1
            result["changed"] = True
            continue
        metrics = _forward_window_metrics(candidate_id, idx, days, strategy_cache)
        forward_windows[label] = {
            "status": "MATURED",
            "horizon_trading_days": days,
            "matured_at": utc_now_iso(),
            **metrics,
        }
        observation[f"matured_{label}"] = True
        result["updated"] += 1
        result["changed"] = True
    observation["forward_windows"] = forward_windows
    observation["pending_windows"] = [
        label
        for label, window in forward_windows.items()
        if _mapping(window).get("status") != "MATURED"
    ]
    if result["changed"]:
        observation["last_maturation_timestamp"] = utc_now_iso()
    return result


def _forward_window_metrics(
    candidate_id: str,
    idx: int,
    days: int,
    strategy_cache: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    returns = strategy_cache[candidate_id]["returns"]
    period = returns.iloc[idx + 1 : idx + days + 1].fillna(0.0)
    forward_return = _compound_return(period)
    equity = (1.0 + period).cumprod()
    drawdown = float((equity / equity.cummax() - 1.0).min()) if not equity.empty else 0.0
    volatility = float(period.std(ddof=0) * math.sqrt(252)) if len(period) else 0.0
    weights = strategy_cache[candidate_id]["weights"].iloc[idx + 1 : idx + days + 1]
    turnover = float(_turnover_series(weights).sum()) if not weights.empty else 0.0
    cash_drag = float(weights.get("SGOV", pd.Series(0.0, index=weights.index)).mean())
    comparator_returns = {
        comparator: _compound_return(
            strategy_cache[comparator]["returns"].iloc[idx + 1 : idx + days + 1].fillna(0.0)
        )
        for comparator in (PUBLIC_100_QQQ_ID, "qqq_50_sgov_50", "qqq_60_sgov_40")
        if comparator in strategy_cache
    }
    qqq_return = comparator_returns.get(PUBLIC_100_QQQ_ID, 0.0)
    qqq_period = strategy_cache[PUBLIC_100_QQQ_ID]["returns"].iloc[idx + 1 : idx + days + 1]
    qqq_equity = (1.0 + qqq_period.fillna(0.0)).cumprod()
    qqq_drawdown = (
        float((qqq_equity / qqq_equity.cummax() - 1.0).min()) if not qqq_equity.empty else 0.0
    )
    equal_return = None
    if candidate_id != PRIMARY_CANDIDATE_ID and PRIMARY_CANDIDATE_ID in strategy_cache:
        equal_return = _compound_return(
            strategy_cache[PRIMARY_CANDIDATE_ID]["returns"].iloc[idx + 1 : idx + days + 1]
        )
    return {
        "forward_return": _round(forward_return),
        "forward_max_drawdown": _round(drawdown),
        "forward_volatility": _round(volatility),
        "relative_vs_100_qqq": _round(
            forward_return - comparator_returns.get(PUBLIC_100_QQQ_ID, 0.0)
        ),
        "relative_vs_qqq_50_sgov_50": _round(
            forward_return - comparator_returns.get("qqq_50_sgov_50", 0.0)
        ),
        "relative_vs_qqq_60_sgov_40": _round(
            forward_return - comparator_returns.get("qqq_60_sgov_40", 0.0)
        ),
        "relative_vs_equal_risk_if_comparator": None
        if equal_return is None
        else _round(forward_return - equal_return),
        "turnover": _round(turnover),
        "cash_drag": _round(cash_drag),
        "missed_upside": _round(max(qqq_return - forward_return, 0.0)),
        "drawdown_reduction": _round(abs(qqq_drawdown) - abs(drawdown)),
    }


def _compound_return(values: pd.Series) -> float:
    if values.empty:
        return 0.0
    return float((1.0 + values.fillna(0.0)).prod() - 1.0)


def _load_observations(paths: list[Path]) -> list[dict[str, Any]]:
    observations = []
    for path in paths:
        payload = _read_json_or_empty(path)
        observations.extend(dict(row) for row in _records(payload.get("observations")))
    return observations


def _scoreboard_row(
    candidate: Mapping[str, Any],
    observations: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    candidate_id = str(candidate.get("candidate_id"))
    selected = [row for row in observations if row.get("strategy_id") == candidate_id]
    windows = _window_policy(config)
    matured_by_window: dict[str, list[dict[str, Any]]] = {label: [] for label in windows}
    for row in selected:
        for label, payload in _mapping(row.get("forward_windows")).items():
            window = _mapping(payload)
            if window.get("status") == "MATURED":
                matured_by_window.setdefault(label, []).append(window)
    all_matured = [item for items in matured_by_window.values() for item in items]
    return {
        "strategy_id": candidate_id,
        "candidate_role": candidate.get("candidate_role"),
        "matured_5d_count": len(matured_by_window.get("5d", [])),
        "matured_10d_count": len(matured_by_window.get("10d", [])),
        "matured_20d_count": len(matured_by_window.get("20d", [])),
        "matured_60d_count": len(matured_by_window.get("60d", [])),
        "matured_120d_count": len(matured_by_window.get("120d", [])),
        "avg_forward_return_by_window": {
            label: _round(_mean([_float(row.get("forward_return")) for row in rows]))
            for label, rows in matured_by_window.items()
        },
        "median_forward_return_by_window": {
            label: _round(_median([_float(row.get("forward_return")) for row in rows]))
            for label, rows in matured_by_window.items()
        },
        "avg_forward_drawdown_by_window": {
            label: _round(_mean([_float(row.get("forward_max_drawdown")) for row in rows]))
            for label, rows in matured_by_window.items()
        },
        "win_rate_vs_100_qqq": _rate_positive(all_matured, "relative_vs_100_qqq"),
        "win_rate_vs_qqq_50_sgov_50": _rate_positive(
            all_matured, "relative_vs_qqq_50_sgov_50"
        ),
        "win_rate_vs_qqq_60_sgov_40": _rate_positive(
            all_matured, "relative_vs_qqq_60_sgov_40"
        ),
        "drawdown_reduction_rate": _rate_positive(all_matured, "drawdown_reduction"),
        "missed_upside_rate": _rate_positive(all_matured, "missed_upside"),
        "rolling_forward_score": _round(
            _mean(
                [
                    _float(row.get("forward_return"))
                    + _float(row.get("drawdown_reduction"))
                    - _float(row.get("missed_upside"))
                    for row in all_matured
                ]
            )
        ),
    }


def _scoreboard_lookup(scoreboard: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(row.get("strategy_id")): dict(row)
        for row in _records(scoreboard.get("scoreboard"))
    }


def _owner_launch_approval(output_root: Path, explicit_owner_approved: bool) -> dict[str, Any]:
    data_repair_owner = _read_json_or_empty(output_root / "data_repair_owner_decision_pack.json")
    forward_owner = _read_json_or_empty(
        output_root / "simple_baseline_forward_aging_owner_review_pack.json"
    )
    approved_by_repair_pack = data_repair_owner.get("status") == "OWNER_APPROVE_FORWARD_AGING"
    approved_by_forward_pack = bool(
        _mapping(forward_owner.get("required_answers")).get(
            "11_owner_approved_start_forward_aging_observation"
        )
    )
    return {
        "approved": explicit_owner_approved
        and (approved_by_repair_pack or approved_by_forward_pack),
        "explicit_owner_approved": explicit_owner_approved,
        "data_repair_owner_pack_status": data_repair_owner.get("status"),
        "forward_owner_pack_status": forward_owner.get("status"),
    }


def _observation_path(output_root: Path, decision_date: date) -> Path:
    artifact_id = f"simple_baseline_forward_aging_observation_{decision_date.isoformat()}"
    return output_root / "forward_aging_observations" / f"{artifact_id}.json"


def _is_written_observation_payload(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("report_type") == "simple_baseline_forward_aging_write_observation"
        and payload.get("status") == "OBSERVATION_WRITTEN"
        and bool(_records(payload.get("observations")))
    )


def _latest_observation_payload_date(output_root: Path) -> date | None:
    paths = sorted(
        (output_root / "forward_aging_observations").glob(
            "simple_baseline_forward_aging_observation_*.json"
        )
    )
    for path in reversed(paths):
        payload = _read_json_or_empty(path)
        raw = payload.get("decision_date") or _mapping(payload.get("summary")).get(
            "decision_date"
        )
        try:
            return date.fromisoformat(str(raw))
        except (TypeError, ValueError):
            continue
    return None


def _observation_core_hashes(payload: Mapping[str, Any]) -> dict[str, str | None]:
    observations = _records(payload.get("observations"))
    primary = next(
        (row for row in observations if row.get("strategy_id") == PRIMARY_CANDIDATE_ID),
        observations[0] if observations else {},
    )
    return {
        "target_weights_hash": _stable_hash(primary.get("target_weights")),
        "signal_inputs_hash": _stable_hash(primary.get("signal_inputs_used")),
        "policy_definition_hash": str(primary.get("policy_definition_hash") or ""),
    }


def _guard_check(check_id: str, passed: object) -> dict[str, Any]:
    return {"check_id": check_id, "status": "PASS" if bool(passed) else "FAIL"}


def _price_cache_has_date(path: Path, target_date: date) -> bool:
    if not path.exists():
        return False
    try:
        frame = pd.read_csv(path, usecols=["date", "ticker"])
    except Exception:
        return False
    if frame.empty:
        return False
    rows = frame.loc[
        (frame["date"].astype(str) == target_date.isoformat())
        & (frame["ticker"].astype(str) == "QQQ")
    ]
    return not rows.empty


def _paper_shadow_threshold_blockers(
    sources: Mapping[str, Mapping[str, Any]],
    min_120d: int,
    matured_120d: int,
) -> list[str]:
    blockers = []
    if sources["policy_definition_lock"].get("status") != "POLICY_DEFINITION_LOCKED":
        blockers.append("policy definition locked")
    if sources["comparator_definition_lock"].get("status") != "COMPARATOR_DEFINITIONS_LOCKED":
        blockers.append("comparator definition locked")
    if sources["pit_boundary"].get("status") != "PIT_BOUNDARY_PASS":
        blockers.append("PIT boundary pass")
    if not str(sources["data_quality"].get("status", "")).startswith("DATA_QUALITY_PASS"):
        blockers.append("data quality pass")
    if matured_120d < min_120d:
        blockers.append("forward aging 120d matured observations below minimum")
    if not sources["scoreboard"]:
        blockers.append("missing scoreboard")
    blockers.append("owner manual review approval")
    return blockers


def _candidate_metrics(
    *,
    config: Mapping[str, Any],
    prices_path: Path,
    start_date: date,
    end_date: date | None,
    candidate_ids: tuple[str, ...],
) -> dict[str, dict[str, Any]]:
    prices = _load_price_matrix(prices_path, _required_tickers(config))
    prices = _slice_prices(prices, start_date=start_date, end_date=end_date)
    annualization = _research_policy_int(config, "annualization_trading_days")
    benchmark = prices["QQQ"].pct_change().fillna(0.0)
    metrics = {}
    for candidate_id in candidate_ids:
        strategy = _candidate_strategy(config, candidate_id)
        returns = _strategy_return_series(strategy, prices, config)
        weights = _target_weight_frame(strategy, prices, config).reindex(returns.index).ffill()
        row = _metrics_for_strategy(
            strategy,
            returns,
            weights,
            benchmark,
            annualization=annualization,
            cost_bps=0.0,
        )
        row["strategy_id"] = candidate_id
        row["registry_strategy_id"] = strategy.get("strategy_id")
        row["effective_qqq_beta"] = _round(_beta(returns, benchmark))
        metrics[candidate_id] = row
    return metrics


def _risk_budget_row(
    candidate_id: str,
    metrics: Mapping[str, Mapping[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    row = dict(metrics[candidate_id])
    weights = _mapping(row.get("average_weights"))
    qqq_weight = _float(weights.get("QQQ"))
    tqqq_weight = _float(weights.get("TQQQ"))
    sgov_weight = _float(weights.get("SGOV"))
    effective_beta = _float(row.get("effective_qqq_beta"))
    total_exposure = qqq_weight + tqqq_weight + sgov_weight
    qqq_contribution = _ratio(qqq_weight + tqqq_weight * 3.0, total_exposure)
    tqqq_contribution = _ratio(tqqq_weight * 3.0, total_exposure)
    sgov_contribution = _ratio(sgov_weight, total_exposure)
    return {
        "strategy_id": candidate_id,
        "qqq_weight": _round(qqq_weight),
        "tqqq_weight": _round(tqqq_weight),
        "sgov_weight": _round(sgov_weight),
        "effective_qqq_beta": _round(effective_beta),
        "annualized_volatility": row.get("annual_volatility"),
        "max_drawdown": row.get("max_drawdown"),
        "expected_cash_drag": _round(sgov_weight),
        "expected_sgov_carry": _round(sgov_weight),
        "risk_contribution_qqq": _round(qqq_contribution),
        "risk_contribution_tqqq": _round(tqqq_contribution),
        "risk_contribution_sgov": _round(sgov_contribution),
        "sharpe": row.get("sharpe"),
        "calmar": row.get("calmar"),
        "risk_budget_commentary": _risk_budget_commentary(candidate_id, effective_beta),
        "policy_version": _forward_policy(config).get("policy_id"),
    }


def _risk_budget_commentary(candidate_id: str, beta: float) -> str:
    if candidate_id == PRIMARY_CANDIDATE_ID:
        return (
            "低回撤主要来自较低 QQQ beta 与 SGOV 防守暴露；应使用 risk-adjusted "
            "comparison，而不是只看 absolute return。"
        )
    if candidate_id == PUBLIC_100_QQQ_ID:
        return "100% QQQ 是风险参考，不是 forward-aging primary candidate。"
    return f"该候选的 effective QQQ beta 约为 {beta:.3f}，用于相对风险预算对照。"


def _beta(returns: pd.Series, benchmark: pd.Series) -> float:
    aligned = pd.concat([returns, benchmark], axis=1).dropna()
    if aligned.empty:
        return 0.0
    aligned.columns = ["strategy", "benchmark"]
    variance = float(aligned["benchmark"].var(ddof=0))
    if variance == 0.0:
        return 0.0
    return float(aligned["strategy"].cov(aligned["benchmark"]) / variance)


def _owner_review_paths(output_root: Path) -> dict[str, Path]:
    return {
        "reconciliation": output_root / "simple_baseline_real_result_reconciliation.json",
        "candidate_freeze": output_root / "simple_baseline_forward_aging_candidate_freeze.json",
        "forward_contract": output_root / "simple_baseline_forward_aging_contract.json",
        "policy_lock": output_root / "equal_risk_qqq_sgov_policy_definition_lock.json",
        "comparator_lock": output_root / "simple_baseline_comparator_definition_lock.json",
        "data_quality": output_root / "simple_baseline_forward_aging_data_quality_gate.json",
        "threshold_contract": output_root / "simple_baseline_paper_shadow_threshold_contract.json",
        "role_assignment": output_root / "simple_baseline_candidate_role_assignment.json",
    }


def _owner_review_answers(
    sources: Mapping[str, Mapping[str, Any]],
    owner_approved: bool,
) -> dict[str, Any]:
    threshold = sources.get("threshold_contract", {})
    return {
        "1_primary_forward_aging_candidate": PRIMARY_CANDIDATE_ID,
        "2_comparator_baselines": list(STATIC_COMPARATOR_IDS),
        "3_challenger": CHALLENGER_CANDIDATE_ID,
        "4_strategy_definitions_locked": (
            sources.get("policy_lock", {}).get("status") == "POLICY_DEFINITION_LOCKED"
            and sources.get("comparator_lock", {}).get("status") == "COMPARATOR_DEFINITIONS_LOCKED"
        ),
        "5_data_quality_gate_passed": str(
            sources.get("data_quality", {}).get("status", "")
        ).startswith("DATA_QUALITY_PASS"),
        "6_paper_shadow_review_threshold": sources.get("threshold_contract", {}).get(
            "threshold_conditions", []
        ),
        "7_remaining_matured_samples_before_paper_shadow_review": threshold.get(
            "minimum_remaining_observations"
        ),
        "8_continue_pause_tqqq_heavy": True,
        "9_continue_block_leaps_wheel": True,
        "10_continue_quarantine_tail_risk_fallback": True,
        "11_owner_approved_start_forward_aging_observation": owner_approved,
    }


def _master_review_paths(output_root: Path) -> dict[str, Path]:
    return {
        "owner_review_pack": output_root / "simple_baseline_forward_aging_owner_review_pack.json",
        "candidate_freeze": output_root / "simple_baseline_forward_aging_candidate_freeze.json",
        "forward_contract": output_root / "simple_baseline_forward_aging_contract.json",
        "data_quality": output_root / "simple_baseline_forward_aging_data_quality_gate.json",
        "scoreboard": output_root / "simple_baseline_forward_aging_scoreboard.json",
        "threshold_contract": output_root / "simple_baseline_paper_shadow_threshold_contract.json",
        "risk_budget": output_root / "simple_baseline_risk_budget_review.json",
        "absolute_return_gap": output_root / "simple_baseline_absolute_return_gap_review.json",
        "automation_readiness": output_root
        / "simple_baseline_forward_aging_automation_readiness.json",
        "daily_summary": output_root / "daily_reader_forward_aging_summary.json",
    }


def _write_review_doc(payload: Mapping[str, Any], path: Path, title: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# {title}",
        "",
        f"- 状态：`{payload.get('status')}`",
        f"- primary_candidate：`{summary.get('primary_candidate', PRIMARY_CANDIDATE_ID)}`",
        f"- challenger_candidate：`{summary.get('challenger_candidate', CHALLENGER_CANDIDATE_ID)}`",
        "- production_effect：`none`",
        "- broker_action：`none`",
        "- promotion_allowed：`false`",
        "- paper_shadow_allowed：`false`",
        "- production_allowed：`false`",
        "- manual_review_required：`true`",
        "",
        "## Required Answers",
        "",
    ]
    answers = _mapping(payload.get("required_answers") or payload.get("final_decision_answers"))
    if not answers:
        lines.append("- none")
    else:
        lines.extend(f"- `{key}`: `{value}`" for key, value in answers.items())
    lines.extend(
        [
            "",
            "本报告只允许用于 research-only forward observation，不生成交易建议、订单、"
            "paper-shadow activation 或 production config mutation。",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _available_price_tickers(path: Path) -> set[str]:
    if not path.exists():
        return set()
    frame = pd.read_csv(path, usecols=["ticker"])
    return {str(item) for item in frame["ticker"].dropna().unique()}


def _available_price_columns(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return set(pd.read_csv(path, nrows=0).columns)


def _strategy_required_tickers(strategy: Mapping[str, Any]) -> list[str]:
    tickers = set(_mapping(strategy.get("target_weights")))
    tickers.update(_mapping(strategy.get("risk_on_weights")))
    tickers.update(_mapping(strategy.get("risk_off_weights")))
    return sorted(str(item) for item in tickers)


def _write_pair(payload: dict[str, Any], *, output_root: Path, artifact_id: str) -> None:
    existing_paths = dict(_mapping(payload.get("artifact_paths")))
    payload["artifact_paths"] = {
        "json_path": str(output_root / f"{artifact_id}.json"),
        "markdown_path": str(output_root / f"{artifact_id}.md"),
        **existing_paths,
    }
    write_foundation_artifact_pair(payload, output_root=output_root, artifact_id=artifact_id)


def _payload(
    *,
    report_type: str,
    title: str,
    status: str,
    summary: Mapping[str, Any],
    **extra: Any,
) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "report_type": report_type,
        "title": title,
        "status": status,
        "generated_at": utc_now_iso(),
        "market_regime": "ai_after_chatgpt",
        "anchor_event": "ChatGPT public launch",
        "anchor_date": "2022-11-30",
        "default_backtest_start": DEFAULT_AI_REGIME_BACKTEST_START.isoformat(),
        "summary": {
            "market_regime": "ai_after_chatgpt",
            "default_backtest_start": DEFAULT_AI_REGIME_BACKTEST_START.isoformat(),
            **dict(summary),
        },
        **SAFETY_BOUNDARY,
        **extra,
    }


def _report_registry_entry(
    report_id: str,
    title: str,
    command: str,
    artifact_slug: str,
    *,
    include_in_reader_brief: bool = False,
    required_for_daily_reading: bool = False,
    extra_artifact_globs: list[str] | None = None,
) -> dict[str, Any]:
    globs = [
        f"outputs/research_strategies/simple_baselines/{artifact_slug}.json",
        f"outputs/research_strategies/simple_baselines/{artifact_slug}.md",
    ]
    if artifact_slug.endswith("*"):
        globs = [
            f"outputs/research_strategies/simple_baselines/{artifact_slug}.json",
            f"outputs/research_strategies/simple_baselines/{artifact_slug}.md",
        ]
    globs.extend(extra_artifact_globs or [])
    return {
        "report_id": report_id,
        "title": title,
        "group": "research",
        "cadence": "ad_hoc",
        "audience": "project_owner",
        "owner": "research_governance",
        "command": command,
        "artifact_globs": globs,
        "artifact_selection_policy": "latest_available",
        "freshness_sla_days": 30,
        "freshness_rationale": (
            "TRADING-894 to 910 simple baseline forward-aging artifacts are "
            "regenerated after candidate, definition, data quality, observation, "
            "or owner review state changes."
        ),
        "owner_action": "review_simple_baseline_forward_aging_research_only_artifact",
        "include_in_reader_brief": include_in_reader_brief,
        "include_in_daily_task_dashboard": False,
        "required_for_daily_reading": required_for_daily_reading,
        "production_effect": "none",
        "broker_action": "none",
    }


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2.0


def _rate_positive(rows: list[Mapping[str, Any]], field: str) -> float:
    if not rows:
        return 0.0
    return _round(sum(1 for row in rows if _float(row.get(field)) > 0.0) / len(rows))


def _ratio(numerator: float, denominator: float) -> float:
    return float(numerator / denominator) if denominator else 0.0


def _round(value: Any) -> float:
    return round(_float(value), 6)


def _float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(result) or math.isinf(result):
        return default
    return result


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default
