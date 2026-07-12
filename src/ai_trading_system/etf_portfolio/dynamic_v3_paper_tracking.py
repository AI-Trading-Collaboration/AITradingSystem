from __future__ import annotations

import csv
import json
import math
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, time, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import (
    PROJECT_ROOT,
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_universe,
)
from ai_trading_system.data.quality import validate_data_cache, write_data_quality_report
from ai_trading_system.etf_portfolio.data import load_standard_prices
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_CONSENSUS_DRIFT_DIR,
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
    DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    DEFAULT_SHADOW_MONITOR_RUN_DIR,
    DEFAULT_SHADOW_SHORTLIST_DIR,
    DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    OWNER_REVIEW_DECISIONS,
    SCHEMA_VERSION,
    STRATEGY_FAMILY,
    replay_owner_review_records,
    validate_consensus_drift_artifact,
    validate_owner_review_artifact,
    validate_position_advisory_daily_artifact,
    validate_shadow_monitor_run_artifact,
    validate_shadow_shortlist_artifact,
)
from ai_trading_system.etf_portfolio.models import (
    DEFAULT_ETF_ASSETS_CONFIG_PATH,
    DEFAULT_ETF_BACKTEST_CONFIG_PATH,
    DEFAULT_ETF_P1_CONFIG_PATH,
    DEFAULT_ETF_P2_CONFIG_PATH,
    DEFAULT_ETF_PRICE_PATH,
    DEFAULT_ETF_RISK_CONFIG_PATH,
    DEFAULT_ETF_STRATEGY_CONFIG_PATH,
    load_etf_config_bundle,
)
from ai_trading_system.platform.artifacts.writer import write_text_atomic
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "dynamic_v3_rescue" / "paper_portfolio_v1.yaml"
)
DEFAULT_RATES_CACHE_PATH = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv"
DEFAULT_DYNAMIC_V3_LATEST_POINTER_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "latest"
DEFAULT_PAPER_PORTFOLIO_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "paper_portfolio"
DEFAULT_ADVISORY_OUTCOME_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "advisory_outcome"
DEFAULT_OWNER_ATTRIBUTION_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "owner_attribution"
DEFAULT_SHADOW_AGING_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "shadow_aging"
DEFAULT_WEEKLY_ADVISORY_REVIEW_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "weekly_advisory_review"

PAPER_DECISIONS_NO_STATE_CHANGE = {"monitor", "no_trade", "reject_advisory", "needs_more_data"}
PAPER_ACTION_LEDGER_SCHEMA_VERSION = "paper_action_ledger.v2"
PAPER_ACTION_EVENT_TYPE = "PAPER_PORTFOLIO_REVIEW_APPLIED"
PAPER_WEIGHT_TOLERANCE = 0.000001
OUTCOME_WINDOW_STATUSES = {"PENDING", "AVAILABLE", "INSUFFICIENT_DATA"}
OUTCOME_MANIFEST_STATUSES = {"PENDING", "AVAILABLE", "PARTIAL", "INSUFFICIENT_DATA"}
OUTCOME_UPDATE_LEDGER_SCHEMA_VERSION = "advisory_outcome_update_ledger.v2"
OUTCOME_UPDATE_EVENT_TYPE = "ADVISORY_OUTCOME_UPDATED"
OWNER_ATTRIBUTION_SNAPSHOT_SCHEMA_VERSION = "owner_attribution_snapshot.v2"
SHADOW_AGING_SNAPSHOT_SCHEMA_VERSION = "shadow_aging_snapshot.v2"
WEEKLY_ADVISORY_REVIEW_SNAPSHOT_SCHEMA_VERSION = "weekly_advisory_review_snapshot.v2"
OUTCOME_METRIC_FIELDS = (
    "paper_portfolio_return",
    "no_trade_return",
    "baseline_return",
    "target_weight_return",
    "limited_adjustment_return",
    "relative_to_no_trade",
    "relative_to_baseline",
    "max_drawdown",
    "realized_volatility",
    "paper_transaction_cost",
    "baseline_transaction_cost",
    "target_transaction_cost",
    "limited_transaction_cost",
)
PROMOTION_CLOCK_STATUSES = {
    "not_started",
    "warming_up",
    "eligible_for_review",
    "blocked",
    "downgrade_recommended",
}


class DynamicV3PaperTrackingError(ValueError):
    """Raised when paper tracking artifacts fail closed."""


def load_paper_portfolio_config(path: Path = DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise DynamicV3PaperTrackingError("paper portfolio config must be a mapping")
    metadata = _mapping(raw.get("policy_metadata"))
    required_metadata = {
        "policy_id",
        "owner",
        "version",
        "status",
        "rationale",
        "intended_effect",
        "review_condition",
    }
    missing_metadata = sorted(key for key in required_metadata if not _text(metadata.get(key)))
    if missing_metadata:
        raise DynamicV3PaperTrackingError(
            "paper portfolio policy metadata is incomplete: " + ",".join(missing_metadata)
        )
    paper_config = _mapping(raw.get("paper_portfolio"))
    if paper_config.get("enabled") is not True:
        raise DynamicV3PaperTrackingError("paper portfolio config must be enabled")
    if paper_config.get("mode") != "advisory_simulation_only":
        raise DynamicV3PaperTrackingError(
            "paper portfolio mode must be advisory_simulation_only"
        )
    if paper_config.get("initial_source") != "manual_snapshot":
        raise DynamicV3PaperTrackingError("paper portfolio initial source must be manual_snapshot")
    if not _text(paper_config.get("initial_snapshot_path")):
        raise DynamicV3PaperTrackingError("paper portfolio initial snapshot path is required")
    safety = _mapping(raw.get("safety"))
    if safety.get("broker_action_allowed") is not False:
        raise DynamicV3PaperTrackingError("paper portfolio config must forbid broker actions")
    if safety.get("broker_action_taken") is not False:
        raise DynamicV3PaperTrackingError(
            "paper portfolio config must keep broker_action_taken=false"
        )
    if safety.get("allow_auto_apply_advisory") is not False:
        raise DynamicV3PaperTrackingError("paper portfolio config must forbid auto apply")
    if safety.get("require_owner_review") is not True:
        raise DynamicV3PaperTrackingError("paper portfolio config must require owner review")
    ledger = _mapping(raw.get("ledger"))
    if (
        ledger.get("immutable_events") is not True
        or ledger.get("allow_rebuild_from_events") is not True
    ):
        raise DynamicV3PaperTrackingError(
            "paper portfolio ledger must be immutable and rebuildable from events"
        )
    simulation = _mapping(raw.get("simulation"))
    thresholds = {
        "min_trade_threshold": _strict_finite_number(
            simulation.get("min_trade_threshold"), "min_trade_threshold"
        ),
        "max_single_day_total_adjustment": _strict_finite_number(
            simulation.get("max_single_day_total_adjustment"),
            "max_single_day_total_adjustment",
        ),
        "max_single_symbol_adjustment": _strict_finite_number(
            simulation.get("max_single_symbol_adjustment"),
            "max_single_symbol_adjustment",
        ),
    }
    if not 0 <= thresholds["min_trade_threshold"] <= 1:
        raise DynamicV3PaperTrackingError("min_trade_threshold must be within [0,1]")
    if not 0 < thresholds["max_single_day_total_adjustment"] <= 2:
        raise DynamicV3PaperTrackingError(
            "max_single_day_total_adjustment must be within (0,2]"
        )
    if not 0 < thresholds["max_single_symbol_adjustment"] <= 1:
        raise DynamicV3PaperTrackingError(
            "max_single_symbol_adjustment must be within (0,1]"
        )
    if thresholds["min_trade_threshold"] > thresholds["max_single_symbol_adjustment"]:
        raise DynamicV3PaperTrackingError(
            "min_trade_threshold cannot exceed max_single_symbol_adjustment"
        )
    for field in ("transaction_cost_bps", "slippage_bps"):
        value = _strict_finite_number(simulation.get(field), field)
        if value < 0 or value > 10_000:
            raise DynamicV3PaperTrackingError(f"{field} must be within [0,10000]")
    return dict(raw)


def init_paper_portfolio(
    *,
    config_path: Path = DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
    output_dir: Path = DEFAULT_PAPER_PORTFOLIO_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    generated = generated if generated.tzinfo else generated.replace(tzinfo=UTC)
    resolved_config_path = _resolve_project_path(config_path)
    config = load_paper_portfolio_config(resolved_config_path)
    paper_config = _mapping(config.get("paper_portfolio"))
    snapshot_path = _resolve_project_path(Path(_text(paper_config.get("initial_snapshot_path"))))
    snapshot = _manual_snapshot_weights(snapshot_path)
    policy_metadata = _mapping(config.get("policy_metadata"))
    paper_portfolio_id = _stable_id(
        "paper-portfolio",
        str(resolved_config_path),
        _file_sha256(snapshot_path),
        _file_sha256(resolved_config_path),
        str(snapshot_path),
        snapshot.get("as_of"),
        generated.isoformat(),
    )
    portfolio_dir = _unique_dir(output_dir / paper_portfolio_id)
    portfolio_dir.mkdir(parents=True, exist_ok=False)
    state = _paper_state_payload(
        paper_portfolio_id=portfolio_dir.name,
        as_of=_text(snapshot.get("as_of")),
        base_currency=_text(snapshot.get("base_currency"), "USD"),
        source=_text(paper_config.get("initial_source"), "manual_snapshot"),
        positions=_mapping(snapshot.get("weights")),
        last_review_id="",
        last_action_id="",
        state_status="ACTIVE",
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_portfolio_manifest",
        "paper_portfolio_id": portfolio_dir.name,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "mode": _text(paper_config.get("mode"), "advisory_simulation_only"),
        "base_currency": state["base_currency"],
        "initial_source": _text(paper_config.get("initial_source"), "manual_snapshot"),
        "initial_as_of": state["as_of"],
        "initial_base_currency": state["base_currency"],
        "initial_snapshot_path": str(snapshot_path),
        "initial_snapshot_checksum": _file_sha256(snapshot_path),
        "initial_weights": state["positions"],
        "config_path": str(resolved_config_path),
        "config_checksum": _file_sha256(resolved_config_path),
        "config_policy_id": _text(policy_metadata.get("policy_id")),
        "config_policy_version": _text(policy_metadata.get("version")),
        "ledger_schema_version": PAPER_ACTION_LEDGER_SCHEMA_VERSION,
        "event_chain_status": "PASS",
        "paper_action_count": 0,
        "last_updated_at": generated.isoformat(),
        "last_review_id": "",
        "last_action_id": "",
        "paper_portfolio_state_path": str(portfolio_dir / "paper_portfolio_state.json"),
        "paper_action_ledger_path": str(portfolio_dir / "paper_action_ledger.jsonl"),
        "paper_position_history_path": str(portfolio_dir / "paper_position_history.jsonl"),
        "paper_portfolio_report_path": str(portfolio_dir / "paper_portfolio_report.md"),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "owner_approval_required": True,
        "manual_review_required": True,
        "official_target_weights_generated": False,
        "portfolio_mutated": False,
        "order_ticket_generated": False,
        "production_effect": "none",
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    history = [
        {
            "schema_version": SCHEMA_VERSION,
            "paper_portfolio_id": portfolio_dir.name,
            "as_of": state["as_of"],
            "event_type": "init",
            "event_sequence": 0,
            "review_id": "",
            "paper_action_id": "",
            "source_snapshot_checksum": manifest["initial_snapshot_checksum"],
            "positions": state["positions"],
            "total_weight": state["total_weight"],
            "broker_action_taken": False,
            "created_at": generated.isoformat(),
        }
    ]
    _write_json(portfolio_dir / "paper_portfolio_manifest.json", manifest)
    _write_json(portfolio_dir / "paper_portfolio_state.json", state)
    _write_jsonl(portfolio_dir / "paper_action_ledger.jsonl", [])
    _write_jsonl(portfolio_dir / "paper_position_history.jsonl", history)
    _write_text(
        portfolio_dir / "paper_portfolio_report.md",
        render_paper_portfolio_report(manifest, state, []),
    )
    _update_latest_pointer(
        "latest_paper_portfolio",
        portfolio_dir.name,
        portfolio_dir / "paper_portfolio_manifest.json",
    )
    return {
        "paper_portfolio_id": portfolio_dir.name,
        "paper_portfolio_dir": portfolio_dir,
        "manifest": manifest,
        "state": state,
    }


def apply_owner_review_to_paper_portfolio(
    *,
    review_id: str,
    paper_portfolio_id: str | None = None,
    manual_deltas: Mapping[str, Any] | None = None,
    config_path: Path = DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
    output_dir: Path = DEFAULT_PAPER_PORTFOLIO_DIR,
    owner_review_dir: Path = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    daily_advisory_dir: Path = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    generated = generated if generated.tzinfo else generated.replace(tzinfo=UTC)
    portfolio_dir = _paper_portfolio_dir(
        paper_portfolio_id=paper_portfolio_id,
        output_dir=output_dir,
    )
    resolved_portfolio_id = portfolio_dir.name
    current_validation = validate_paper_portfolio_artifact(
        paper_portfolio_id=resolved_portfolio_id,
        output_dir=output_dir,
    )
    if current_validation.get("status") != "PASS":
        raise DynamicV3PaperTrackingError(
            "paper portfolio validation must PASS before mutation; "
            f"status={current_validation.get('status')}"
        )
    manifest = _read_json(portfolio_dir / "paper_portfolio_manifest.json")
    state = _read_json(portfolio_dir / "paper_portfolio_state.json")
    ledger_path = portfolio_dir / "paper_action_ledger.jsonl"
    manifest_config_path = Path(_text(manifest.get("config_path")))
    resolved_config_path = _resolve_project_path(config_path)
    if not _paths_equal(manifest_config_path, resolved_config_path):
        raise DynamicV3PaperTrackingError(
            "paper portfolio config must match the immutable initialization source"
        )
    config = load_paper_portfolio_config(manifest_config_path)
    if manifest.get("config_checksum") != _file_sha256(manifest_config_path):
        raise DynamicV3PaperTrackingError("paper portfolio config changed after initialization")
    owner_validation = validate_owner_review_artifact(
        review_id=review_id,
        output_dir=owner_review_dir,
    )
    if owner_validation.get("status") != "PASS":
        raise DynamicV3PaperTrackingError(
            "owner review validation must PASS before paper portfolio mutation"
        )
    review = _owner_review_record(review_id=review_id, output_dir=owner_review_dir)
    owner_decision = _text(review.get("owner_decision"))
    if owner_decision not in OWNER_REVIEW_DECISIONS:
        raise DynamicV3PaperTrackingError(f"unsupported owner decision: {owner_decision}")
    daily_advisory_id = _text(review.get("daily_advisory_id"))
    source_daily_root = Path(_text(review.get("source_daily_advisory_root")))
    if not _paths_equal(source_daily_root, daily_advisory_dir):
        raise DynamicV3PaperTrackingError(
            "daily advisory root must match the owner review frozen source"
        )
    daily_validation = validate_position_advisory_daily_artifact(
        daily_advisory_id=daily_advisory_id,
        output_dir=source_daily_root,
    )
    if daily_validation.get("status") != "PASS":
        raise DynamicV3PaperTrackingError(
            "daily advisory validation must PASS before paper portfolio mutation"
        )
    source_paths = _mapping(review.get("source_artifact_paths"))
    source_checksums = _mapping(review.get("source_artifact_checksums"))
    if not source_paths or any(
        not Path(_text(path)).is_file()
        or source_checksums.get(key) != _file_sha256(Path(_text(path)))
        for key, path in source_paths.items()
    ):
        raise DynamicV3PaperTrackingError(
            "owner review daily advisory frozen source changed before paper mutation"
        )
    ledger = _read_jsonl(ledger_path)
    if any(row.get("review_id") == review_id for row in ledger):
        raise DynamicV3PaperTrackingError(
            f"owner review already applied to paper portfolio: {review_id}"
        )
    before = _clean_weights(_mapping(state.get("positions")))
    review_as_of = _date_from_any(review.get("as_of"))
    state_as_of = _date_from_any(state.get("as_of"))
    if review_as_of is None or (state_as_of is not None and review_as_of < state_as_of):
        raise DynamicV3PaperTrackingError(
            "owner review as_of cannot precede the current paper portfolio state"
        )
    manual_override = False
    action_type = "no_trade"
    reason = owner_decision
    if owner_decision == "paper_adjustment":
        action_type = "paper_adjustment"
        if manual_deltas:
            raise DynamicV3PaperTrackingError(
                "manual deltas are not allowed for paper_adjustment"
            )
        proposed = _paper_proposed_deltas(
            daily_advisory_id=daily_advisory_id,
            before_weights=before,
            daily_advisory_dir=source_daily_root,
        )
        if not proposed:
            raise DynamicV3PaperTrackingError(
                "paper_adjustment requires non-empty source-derived deltas"
            )
    elif owner_decision == "manual_adjustment":
        action_type = "manual_adjustment"
        manual_override = True
        proposed = _validated_manual_deltas(manual_deltas)
    else:
        if manual_deltas:
            raise DynamicV3PaperTrackingError(
                f"manual deltas are not allowed for owner decision: {owner_decision}"
            )
        proposed = {}
    applied = _limit_paper_deltas(proposed, config, before_weights=before)
    after = _apply_weight_deltas(before, applied)
    event = _build_paper_action_event(
        events=ledger,
        paper_portfolio_id=resolved_portfolio_id,
        review=review,
        owner_review_dir=owner_review_dir,
        source_daily_root=source_daily_root,
        source_paths=source_paths,
        source_checksums=source_checksums,
        config_path=manifest_config_path,
        config=config,
        action_type=action_type,
        manual_override=manual_override,
        before_weights=before,
        proposed_deltas=proposed,
        applied_deltas=applied,
        after_weights=after,
        reason=reason,
        event_at=generated,
    )
    _append_jsonl_atomic(ledger_path, event)
    next_state = _materialize_paper_portfolio(
        portfolio_dir=portfolio_dir,
        manifest=manifest,
        events=[*ledger, event],
    )
    _update_latest_pointer(
        "latest_paper_portfolio",
        resolved_portfolio_id,
        portfolio_dir / "paper_portfolio_manifest.json",
    )
    return {
        "paper_portfolio_id": resolved_portfolio_id,
        "paper_portfolio_dir": portfolio_dir,
        "paper_action_id": event["paper_action_id"],
        "event": event,
        "state": next_state,
    }


def paper_portfolio_state_payload(
    *,
    paper_portfolio_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PAPER_PORTFOLIO_DIR,
) -> dict[str, Any]:
    portfolio_dir = _paper_portfolio_dir(
        paper_portfolio_id=paper_portfolio_id if not latest else None,
        output_dir=output_dir,
    )
    return {
        **_read_json(portfolio_dir / "paper_portfolio_state.json"),
        "paper_portfolio_dir": str(portfolio_dir),
    }


def paper_portfolio_report_payload(
    *,
    paper_portfolio_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PAPER_PORTFOLIO_DIR,
) -> dict[str, Any]:
    portfolio_dir = _paper_portfolio_dir(
        paper_portfolio_id=paper_portfolio_id if not latest else None,
        output_dir=output_dir,
    )
    return {
        **_read_json(portfolio_dir / "paper_portfolio_manifest.json"),
        "paper_portfolio_state": _read_json(portfolio_dir / "paper_portfolio_state.json"),
        "paper_action_count": len(_read_jsonl(portfolio_dir / "paper_action_ledger.jsonl")),
        "paper_portfolio_dir": str(portfolio_dir),
    }


def validate_paper_portfolio_artifact(
    *,
    paper_portfolio_id: str,
    output_dir: Path = DEFAULT_PAPER_PORTFOLIO_DIR,
) -> dict[str, Any]:
    portfolio_dir = output_dir / paper_portfolio_id
    required_names = [
        "paper_portfolio_manifest.json",
        "paper_portfolio_state.json",
        "paper_action_ledger.jsonl",
        "paper_position_history.jsonl",
        "paper_portfolio_report.md",
    ]
    parse_error = ""
    try:
        manifest = _read_optional_json(portfolio_dir / "paper_portfolio_manifest.json") or {}
        state = _read_optional_json(portfolio_dir / "paper_portfolio_state.json") or {}
        ledger = _read_jsonl(portfolio_dir / "paper_action_ledger.jsonl")
        history = _read_jsonl(portfolio_dir / "paper_position_history.jsonl")
    except Exception as exc:  # noqa: BLE001
        manifest, state, ledger, history = {}, {}, [], []
        parse_error = str(exc)
    base_checks = [
        _check(f"artifact_exists:{name}", (portfolio_dir / name).is_file(), name)
        for name in required_names
    ]
    base_checks.extend(
        [
            _check("artifacts_parse", not parse_error, parse_error or "artifacts parsed"),
            _check(
                "paper_portfolio_id_matches",
                manifest.get("paper_portfolio_id") == paper_portfolio_id
                and state.get("paper_portfolio_id") == paper_portfolio_id,
                paper_portfolio_id,
            ),
            _check(
                "broker_action_forbidden",
                manifest.get("broker_action_allowed") is False
                and state.get("broker_action_allowed") is False,
                "broker action forbidden",
            ),
            _check(
                "broker_action_not_taken",
                manifest.get("broker_action_taken") is False
                and state.get("broker_action_taken") is False,
                "broker action not taken",
            ),
            _check(
                "total_weight_is_one",
                _is_finite_number(state.get("total_weight"))
                and abs(_float(state.get("total_weight")) - 1.0) <= PAPER_WEIGHT_TOLERANCE,
                str(state.get("total_weight")),
            ),
        ]
    )
    is_legacy = manifest.get("ledger_schema_version") != PAPER_ACTION_LEDGER_SCHEMA_VERSION
    if is_legacy:
        rebuilt = _rebuild_paper_state(manifest, ledger)
        checks = [
            *base_checks,
            _check(
                "legacy_ledger_rebuild_matches_state",
                _weights_equal(rebuilt, _mapping(state.get("positions"))),
                "legacy ledger rebuild",
            ),
        ]
        structural_pass = all(check["passed"] for check in checks)
        payload = _validation_payload(
            report_type="etf_dynamic_v3_paper_portfolio_validation",
            artifact_id_key="paper_portfolio_id",
            artifact_id=paper_portfolio_id,
            status="PASS_WITH_WARNINGS" if structural_pass else "FAIL",
            checks=checks,
        )
        payload.update(
            {
                "event_chain_status": "LEGACY_UNCHAINED",
                "mutation_allowed": False,
                "warnings": ["LEGACY_UNCHAINED"] if structural_pass else [],
            }
        )
        return payload
    replay_state: dict[str, Any] = {}
    expected_history: list[dict[str, Any]] = []
    replay_errors: list[str] = []
    source_error = ""
    try:
        config_path = Path(_text(manifest.get("config_path")))
        snapshot_path = Path(_text(manifest.get("initial_snapshot_path")))
        config = load_paper_portfolio_config(config_path)
        snapshot = _manual_snapshot_weights(snapshot_path)
        if manifest.get("config_checksum") != _file_sha256(config_path):
            raise DynamicV3PaperTrackingError("config checksum mismatch")
        if manifest.get("initial_snapshot_checksum") != _file_sha256(snapshot_path):
            raise DynamicV3PaperTrackingError("initial snapshot checksum mismatch")
        if not _weights_equal(snapshot["weights"], _mapping(manifest.get("initial_weights"))):
            raise DynamicV3PaperTrackingError("initial snapshot weights mismatch")
        paper_config = _mapping(config.get("paper_portfolio"))
        policy = _mapping(config.get("policy_metadata"))
        if (
            manifest.get("initial_as_of") != snapshot.get("as_of")
            or manifest.get("initial_base_currency") != snapshot.get("base_currency")
            or manifest.get("base_currency") != snapshot.get("base_currency")
            or manifest.get("mode") != paper_config.get("mode")
            or manifest.get("initial_source") != paper_config.get("initial_source")
            or manifest.get("config_policy_id") != policy.get("policy_id")
            or manifest.get("config_policy_version") != policy.get("version")
        ):
            raise DynamicV3PaperTrackingError("initial source-derived manifest fields mismatch")
        replay_state, expected_history, replay_errors = _replay_paper_action_events(
            manifest=manifest,
            events=ledger,
            config=config,
            validate_sources=True,
        )
    except Exception as exc:  # noqa: BLE001
        source_error = str(exc)
    expected_manifest = (
        _paper_manifest_materialized_payload(manifest=manifest, events=ledger)
        if not source_error and not replay_errors
        else {}
    )
    expected_report = (
        render_paper_portfolio_report(expected_manifest, replay_state, ledger)
        if expected_manifest
        else ""
    )
    report_path = portfolio_dir / "paper_portfolio_report.md"
    checks = [
        *base_checks,
        _check(
            "ledger_schema_current",
            manifest.get("ledger_schema_version") == PAPER_ACTION_LEDGER_SCHEMA_VERSION,
            _text(manifest.get("ledger_schema_version")),
        ),
        _check(
            "source_recomputation_succeeds",
            not source_error,
            source_error or "initial sources recomputed",
        ),
        _check(
            "event_chain_and_content_valid",
            not replay_errors,
            ",".join(replay_errors),
        ),
        _check(
            "state_matches_event_replay",
            state == replay_state,
            _text(state.get("last_action_id")),
        ),
        _check(
            "history_matches_event_replay",
            history == expected_history,
            f"history={len(history)} expected={len(expected_history)}",
        ),
        _check(
            "manifest_materialized_fields_match",
            manifest == expected_manifest,
            _text(manifest.get("last_action_id")),
        ),
        _check(
            "report_matches_event_replay",
            report_path.is_file()
            and report_path.read_text(encoding="utf-8") == expected_report,
            str(report_path),
        ),
        _check(
            "no_execution_effect",
            manifest.get("official_target_weights_generated") is False
            and manifest.get("portfolio_mutated") is False
            and manifest.get("order_ticket_generated") is False
            and manifest.get("production_effect") == "none"
            and all(_paper_event_has_no_execution_effect(row) for row in ledger),
            "paper simulation only",
        ),
    ]
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    payload = _validation_payload(
        report_type="etf_dynamic_v3_paper_portfolio_validation",
        artifact_id_key="paper_portfolio_id",
        artifact_id=paper_portfolio_id,
        status=status,
        checks=checks,
    )
    payload.update(
        {
            "event_chain_status": "PASS" if status == "PASS" else "FAIL",
            "mutation_allowed": status == "PASS",
            "event_count": len(ledger),
        }
    )
    return payload


def track_advisory_outcome(
    *,
    daily_advisory_id: str,
    config_path: Path = DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
    output_dir: Path = DEFAULT_ADVISORY_OUTCOME_DIR,
    daily_advisory_dir: Path = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    paper_portfolio_dir: Path = DEFAULT_PAPER_PORTFOLIO_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    generated = generated if generated.tzinfo else generated.replace(tzinfo=UTC)
    resolved_config_path = _resolve_project_path(config_path)
    config = load_paper_portfolio_config(resolved_config_path)
    windows = _configured_outcome_windows(config)
    _ensure_no_duplicate_outcome_tracker(
        daily_advisory_id=daily_advisory_id,
        output_dir=output_dir,
    )
    daily_source = _validated_outcome_daily_source(
        daily_advisory_id=daily_advisory_id,
        daily_advisory_dir=daily_advisory_dir,
    )
    daily_generated = _parse_datetime_text(
        _text(daily_source["manifest"].get("generated_at"))
    )
    if daily_generated is None or daily_generated > generated:
        raise DynamicV3PaperTrackingError(
            "daily advisory generated_at must be valid and not later than outcome tracking"
        )
    paper_source = _validated_outcome_paper_source(paper_portfolio_dir)
    advisory_dir = daily_advisory_dir / daily_advisory_id
    no_trade = _advisory_current_weights(advisory_dir)
    no_trade_source = "daily_advisory_current_weights"
    if not no_trade:
        no_trade = _mapping(paper_source["state"].get("positions"))
        no_trade_source = "selected_paper_portfolio_state"
    no_trade = _validated_outcome_weights(no_trade, "no_trade_weights")
    target = _validated_outcome_weights(
        _advisory_consensus_weights(advisory_dir), "target_weights"
    )
    baseline_source = _outcome_baseline_source()
    baseline = _validated_outcome_weights(
        _baseline_weights(), "baseline_weights"
    )
    proposed_limited = {
        symbol: round(_float(target.get(symbol)) - _float(no_trade.get(symbol)), 6)
        for symbol in sorted(set(target) | set(no_trade))
        if round(_float(target.get(symbol)) - _float(no_trade.get(symbol)), 6)
    }
    limited_deltas = _limit_paper_deltas(
        proposed_limited,
        config,
        before_weights=no_trade,
    )
    limited = _apply_weight_deltas(no_trade, limited_deltas)
    outcome_id = _stable_id("advisory-outcome", daily_advisory_id, generated.isoformat())
    outcome_dir = _unique_dir(output_dir / outcome_id)
    outcome_dir.mkdir(parents=True, exist_ok=False)
    frozen_sources = _freeze_outcome_track_sources(
        outcome_dir=outcome_dir,
        outcome_config_path=resolved_config_path,
        daily_source=daily_source,
        baseline_source=baseline_source,
        paper_source=paper_source,
    )
    advisory_event = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_advisory_event",
        "outcome_id": outcome_dir.name,
        "daily_advisory_id": daily_advisory_id,
        "tracked_at": generated.isoformat(),
        "as_of": _text(daily_source["manifest"].get("as_of")),
        "recommended_action": _text(daily_source["actions"].get("recommended_action")),
        "mode": _text(
            daily_source["actions"].get("mode"),
            _text(daily_source["manifest"].get("mode")),
        ),
        "outcome_config_path": str(resolved_config_path),
        "outcome_config_checksum": _file_sha256(resolved_config_path),
        "outcome_config_policy_id": _text(
            _mapping(config.get("policy_metadata")).get("policy_id")
        ),
        "outcome_config_policy_version": _text(
            _mapping(config.get("policy_metadata")).get("version")
        ),
        "source_daily_advisory_root": str(daily_advisory_dir),
        "source_daily_advisory_validation_status": "PASS",
        "source_artifact_paths": daily_source["source_paths"],
        "source_artifact_checksums": daily_source["source_checksums"],
        "baseline_source_paths": baseline_source["source_paths"],
        "baseline_source_checksums": baseline_source["source_checksums"],
        "baseline_config_hash": baseline_source["config_hash"],
        "paper_portfolio_source": paper_source["binding"],
        "frozen_track_sources": frozen_sources,
        "no_trade_source": no_trade_source,
        "no_trade_weights": no_trade,
        "paper_action_status": "PENDING",
        "paper_action_weights": {},
        "baseline_weights": baseline,
        "target_weights": target,
        "limited_adjustment_weights": limited,
        "limited_adjustment_deltas": limited_deltas,
        "official_target_weights_generated": False,
        "portfolio_mutated": False,
        "order_ticket_generated": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_effect": "none",
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    advisory_event["event_checksum"] = _advisory_event_checksum(advisory_event)
    window_rows = [
        _pending_outcome_window(
            daily_advisory_id=daily_advisory_id,
            window_days=window,
            start_date=_text(daily_source["manifest"].get("as_of")),
        )
        for window in windows
    ]
    counterfactuals = _outcome_counterfactuals_payload()
    outcome_manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_advisory_outcome_manifest",
        "outcome_id": outcome_dir.name,
        "daily_advisory_id": daily_advisory_id,
        "as_of": _text(daily_source["manifest"].get("as_of")),
        "generated_at": generated.isoformat(),
        "status": "PENDING",
        "tracked_windows": windows,
        "update_ledger_schema_version": OUTCOME_UPDATE_LEDGER_SCHEMA_VERSION,
        "update_chain_status": "PASS",
        "update_event_count": 0,
        "latest_update_event_id": "",
        "latest_update_event_checksum": "",
        "updated_at": "",
        "updated_as_of": "",
        "data_quality_status": "NOT_RUN_PENDING_ONLY",
        "data_quality_report_path": "",
        "paper_action_status": "PENDING",
        "available_window_count": 0,
        "pending_window_count": len(windows),
        "insufficient_window_count": 0,
        "advisory_event_path": str(outcome_dir / "advisory_event.json"),
        "advisory_event_checksum": advisory_event["event_checksum"],
        "outcome_windows_path": str(outcome_dir / "outcome_windows.jsonl"),
        "outcome_update_events_path": str(outcome_dir / "outcome_update_events.jsonl"),
        "source_snapshots_root": str(outcome_dir / "source_snapshots"),
        "advisory_counterfactuals_path": str(outcome_dir / "advisory_counterfactuals.json"),
        "advisory_counterfactuals_checksum": sha256(
            _canonical_json(counterfactuals).encode("utf-8")
        ).hexdigest(),
        "advisory_outcome_report_path": str(outcome_dir / "advisory_outcome_report.md"),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "owner_approval_required": True,
        "manual_review_required": True,
        "official_target_weights_generated": False,
        "portfolio_mutated": False,
        "order_ticket_generated": False,
        "production_effect": "none",
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(outcome_dir / "advisory_outcome_manifest.json", outcome_manifest)
    _write_json(outcome_dir / "advisory_event.json", advisory_event)
    _write_jsonl(outcome_dir / "outcome_windows.jsonl", window_rows)
    _write_jsonl(outcome_dir / "outcome_update_events.jsonl", [])
    _write_json(outcome_dir / "advisory_counterfactuals.json", counterfactuals)
    _write_text(
        outcome_dir / "advisory_outcome_report.md",
        render_advisory_outcome_report(outcome_manifest, advisory_event, window_rows),
    )
    _update_latest_pointer(
        "latest_advisory_outcome", outcome_dir.name, outcome_dir / "advisory_outcome_manifest.json"
    )
    return {
        "outcome_id": outcome_dir.name,
        "outcome_dir": outcome_dir,
        "manifest": outcome_manifest,
        "advisory_event": advisory_event,
        "outcome_windows": window_rows,
    }


def update_advisory_outcome(
    *,
    as_of: date,
    outcome_id: str | None = None,
    output_dir: Path = DEFAULT_ADVISORY_OUTCOME_DIR,
    paper_portfolio_dir: Path = DEFAULT_PAPER_PORTFOLIO_DIR,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    generated_at: datetime | None = None,
    allowed_window_days: set[int] | None = None,
    update_latest_pointer: bool = True,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    generated = generated if generated.tzinfo else generated.replace(tzinfo=UTC)
    outcome_dir = _outcome_dir(outcome_id=outcome_id, output_dir=output_dir)
    resolved_outcome_id = outcome_dir.name
    current_validation = validate_advisory_outcome_artifact(
        outcome_id=resolved_outcome_id,
        output_dir=output_dir,
    )
    if current_validation.get("status") != "PASS":
        raise DynamicV3PaperTrackingError(
            "advisory outcome validation must PASS before update; "
            f"status={current_validation.get('status')}"
        )
    manifest = _read_json(outcome_dir / "advisory_outcome_manifest.json")
    event = _read_json(outcome_dir / "advisory_event.json")
    start = _date_from_any(event.get("as_of"))
    if start is None:
        raise DynamicV3PaperTrackingError("advisory outcome event missing as_of")
    if as_of < start:
        raise DynamicV3PaperTrackingError("outcome update as_of cannot precede advisory as_of")
    tracked_at = _parse_datetime_text(_text(event.get("tracked_at")))
    if tracked_at is None or generated < tracked_at:
        raise DynamicV3PaperTrackingError(
            "outcome update generated_at cannot precede tracking time"
        )
    update_events = _read_jsonl(outcome_dir / "outcome_update_events.jsonl")
    if update_events:
        previous = update_events[-1]
        previous_as_of = _date_from_any(previous.get("updated_as_of"))
        previous_generated = _parse_datetime_text(_text(previous.get("event_at")))
        if previous_as_of is None or as_of < previous_as_of:
            raise DynamicV3PaperTrackingError("outcome update as_of cannot move backward")
        if previous_generated is None or generated < previous_generated:
            raise DynamicV3PaperTrackingError(
                "outcome update generated_at cannot move backward"
            )
    configured_paper_root = Path(
        _text(_mapping(event.get("paper_portfolio_source")).get("paper_portfolio_root"))
    )
    if not _paths_equal(configured_paper_root, paper_portfolio_dir):
        raise DynamicV3PaperTrackingError(
            "paper portfolio root must match the outcome tracking source"
        )
    config_path = Path(_text(event.get("outcome_config_path")))
    config = load_paper_portfolio_config(config_path)
    paper_binding = _paper_action_binding_for_outcome(
        advisory_event=event,
        paper_portfolio_dir=paper_portfolio_dir,
        updated_as_of=as_of,
        known_at=generated,
    )
    sequence = len(update_events) + 1
    update_event_id = _stable_id(
        "advisory-outcome-update",
        resolved_outcome_id,
        sequence,
        as_of.isoformat(),
        generated.isoformat(),
    )
    today = generated.date()
    data_quality_status = "NOT_RUN_FUTURE_AS_OF" if as_of > today else ""
    data_quality_report_path = ""
    data_quality_report_checksum = ""
    price_snapshot_path = ""
    price_snapshot_checksum = ""
    prices_source_checksum = ""
    rates_source_checksum = ""
    snapshot_prices: pd.DataFrame | None = None
    if as_of <= today:
        source_snapshot_dir = outcome_dir / "source_snapshots" / update_event_id
        quality_report_path = source_snapshot_dir / "validate_data_quality_report.md"
        quality = _run_cached_data_quality_gate(
            as_of=as_of,
            prices_path=prices_path,
            rates_path=rates_path,
            report_path=quality_report_path,
        )
        data_quality_status = quality.status
        data_quality_report_path = str(quality_report_path)
        if not quality.passed:
            raise DynamicV3PaperTrackingError(
                f"advisory outcome data quality gate failed: {quality.status}"
            )
        loaded_prices = _load_outcome_prices(prices_path, event, paper_binding)
        snapshot_prices = _outcome_price_snapshot(
            prices=loaded_prices,
            advisory_event=event,
            paper_binding=paper_binding,
            start=start,
            as_of=as_of,
        )
        snapshot_path = source_snapshot_dir / "required_symbol_prices.csv"
        _write_outcome_price_snapshot(snapshot_path, snapshot_prices)
        price_snapshot_path = str(snapshot_path)
        price_snapshot_checksum = _file_sha256(snapshot_path)
        data_quality_report_checksum = _file_sha256(quality_report_path)
        prices_source_checksum = _file_sha256(prices_path)
        rates_source_checksum = _file_sha256(rates_path)
    computed_rows = _compute_outcome_window_rows(
        advisory_event=event,
        config=config,
        paper_binding=paper_binding,
        prices=snapshot_prices,
        updated_as_of=as_of,
        data_gate_ran=as_of <= today,
    )
    rows = computed_rows
    effective_allowed_window_days = {
        int(row.get("window_days") or 0) for row in computed_rows
    }
    if allowed_window_days is not None:
        allowed = {int(value) for value in allowed_window_days}
        prior_rows = _read_jsonl(outcome_dir / "outcome_windows.jsonl")
        prior_by_window = {
            int(row.get("window_days") or 0): dict(row) for row in prior_rows
        }
        computed_by_window = {
            int(row.get("window_days") or 0): dict(row) for row in computed_rows
        }
        configured_windows = set(computed_by_window)
        if not allowed or not allowed <= configured_windows:
            raise DynamicV3PaperTrackingError(
                "allowed outcome update windows must be a non-empty configured subset"
            )
        rows = [
            computed_by_window[window_days]
            if window_days in allowed
            else prior_by_window[window_days]
            for window_days in sorted(configured_windows)
        ]
        effective_allowed_window_days = allowed
    status = _rollup_outcome_status(rows)
    update_event = {
        "schema_version": SCHEMA_VERSION,
        "ledger_schema_version": OUTCOME_UPDATE_LEDGER_SCHEMA_VERSION,
        "event_type": OUTCOME_UPDATE_EVENT_TYPE,
        "event_sequence": sequence,
        "update_event_id": update_event_id,
        "outcome_id": resolved_outcome_id,
        "daily_advisory_id": event.get("daily_advisory_id"),
        "updated_as_of": as_of.isoformat(),
        "event_at": generated.isoformat(),
        "status": status,
        "data_quality_status": data_quality_status,
        "data_quality_report_path": data_quality_report_path,
        "data_quality_report_checksum": data_quality_report_checksum,
        "prices_source_path": str(prices_path) if as_of <= today else "",
        "prices_source_checksum": prices_source_checksum,
        "rates_source_path": str(rates_path) if as_of <= today else "",
        "rates_source_checksum": rates_source_checksum,
        "price_snapshot_path": price_snapshot_path,
        "price_snapshot_checksum": price_snapshot_checksum,
        "paper_action_binding": paper_binding,
        "allowed_window_days": sorted(effective_allowed_window_days),
        "outcome_windows": rows,
        "previous_event_checksum": (
            _text(update_events[-1].get("event_checksum")) if update_events else "GENESIS"
        ),
        "official_target_weights_generated": False,
        "portfolio_mutated": False,
        "order_ticket_generated": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_effect": "none",
    }
    update_event["event_checksum"] = _outcome_update_event_checksum(update_event)
    _append_jsonl_atomic(
        outcome_dir / "outcome_update_events.jsonl",
        update_event,
        sequence_field="event_sequence",
        checksum_field="event_checksum",
        previous_field="previous_event_checksum",
    )
    materialized_manifest = _materialize_advisory_outcome(
        outcome_dir=outcome_dir,
        manifest=manifest,
        advisory_event=event,
        update_events=[*update_events, update_event],
    )
    if update_latest_pointer:
        _update_latest_pointer(
            "latest_advisory_outcome",
            resolved_outcome_id,
            outcome_dir / "advisory_outcome_manifest.json",
        )
    advisory_event_view = {
        **event,
        "paper_action_status": paper_binding["status"],
        "paper_action_weights": paper_binding.get("after_weights", {}),
    }
    return {
        "outcome_id": resolved_outcome_id,
        "outcome_dir": outcome_dir,
        "manifest": materialized_manifest,
        "advisory_event": advisory_event_view,
        "outcome_windows": rows,
        "update_event": update_event,
    }


def advisory_outcome_report_payload(
    *,
    outcome_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_ADVISORY_OUTCOME_DIR,
) -> dict[str, Any]:
    outcome_dir = _outcome_dir(
        outcome_id=outcome_id if not latest else None,
        output_dir=output_dir,
    )
    return {
        **_read_json(outcome_dir / "advisory_outcome_manifest.json"),
        "advisory_event": _read_json(outcome_dir / "advisory_event.json"),
        "outcome_windows": _read_jsonl(outcome_dir / "outcome_windows.jsonl"),
        "outcome_dir": str(outcome_dir),
    }


def validate_advisory_outcome_artifact(
    *,
    outcome_id: str,
    output_dir: Path = DEFAULT_ADVISORY_OUTCOME_DIR,
) -> dict[str, Any]:
    outcome_dir = output_dir / outcome_id
    paths = {
        "manifest": outcome_dir / "advisory_outcome_manifest.json",
        "event": outcome_dir / "advisory_event.json",
        "windows": outcome_dir / "outcome_windows.jsonl",
        "updates": outcome_dir / "outcome_update_events.jsonl",
        "counterfactuals": outcome_dir / "advisory_counterfactuals.json",
        "report": outcome_dir / "advisory_outcome_report.md",
    }
    existence_checks = [
        _check(f"{name}_exists", path.is_file(), str(path))
        for name, path in paths.items()
    ]
    if not all(check["passed"] for check in existence_checks):
        payload = _validation_payload(
            report_type="etf_dynamic_v3_advisory_outcome_validation",
            artifact_id_key="outcome_id",
            artifact_id=outcome_id,
            status="FAIL",
            checks=existence_checks,
        )
        payload.update(
            {"update_chain_status": "FAIL", "mutation_allowed": False, "event_count": 0}
        )
        return payload
    try:
        manifest = _read_json(paths["manifest"])
        advisory_event = _read_json(paths["event"])
        rows = _read_jsonl(paths["windows"])
        update_events = _read_jsonl(paths["updates"])
        counterfactuals = _read_json(paths["counterfactuals"])
        report = paths["report"].read_text(encoding="utf-8")
    except Exception as exc:  # noqa: BLE001
        checks = [*existence_checks, _check("artifact_parse", False, str(exc))]
        payload = _validation_payload(
            report_type="etf_dynamic_v3_advisory_outcome_validation",
            artifact_id_key="outcome_id",
            artifact_id=outcome_id,
            status="FAIL",
            checks=checks,
        )
        payload.update(
            {"update_chain_status": "FAIL", "mutation_allowed": False, "event_count": 0}
        )
        return payload
    if manifest.get("update_ledger_schema_version") != OUTCOME_UPDATE_LEDGER_SCHEMA_VERSION:
        checks = [
            *existence_checks,
            _check("outcome_id_matches", manifest.get("outcome_id") == outcome_id, outcome_id),
            _check("windows_present", bool(rows), "outcome windows required"),
            _check(
                "window_status_valid",
                all(row.get("outcome_status") in OUTCOME_WINDOW_STATUSES for row in rows),
                "legacy window statuses",
            ),
            _check(
                "legacy_no_execution_effect",
                _outcome_record_has_no_execution_effect(manifest),
                "legacy artifact remains advisory only",
            ),
        ]
        structural_pass = all(check["passed"] for check in checks)
        payload = _validation_payload(
            report_type="etf_dynamic_v3_advisory_outcome_validation",
            artifact_id_key="outcome_id",
            artifact_id=outcome_id,
            status="PASS_WITH_WARNINGS" if structural_pass else "FAIL",
            checks=checks,
        )
        payload.update(
            {
                "update_chain_status": "LEGACY_UNCHAINED",
                "mutation_allowed": False,
                "event_count": len(update_events),
                "warnings": ["LEGACY_UNCHAINED: 旧产物不可继续 update，请重新 track"],
            }
        )
        return payload
    replay_errors: list[str] = []
    config: dict[str, Any] = {}
    try:
        config = _validate_outcome_initial_context(advisory_event)
        replay_errors = _replay_outcome_update_events(
            advisory_event=advisory_event,
            config=config,
            events=update_events,
        )
    except Exception as exc:  # noqa: BLE001
        replay_errors = [f"initial_context_invalid:{exc}"]
    configured_windows = _configured_outcome_windows(config) if config else []
    expected_rows = (
        _records(update_events[-1].get("outcome_windows"))
        if update_events
        else [
            _pending_outcome_window(
                daily_advisory_id=_text(advisory_event.get("daily_advisory_id")),
                window_days=window,
                start_date=_text(advisory_event.get("as_of")),
            )
            for window in configured_windows
        ]
    )
    expected_manifest = _outcome_manifest_materialized_payload(
        manifest=manifest,
        update_events=update_events,
    )
    expected_counterfactuals = _outcome_counterfactuals_payload()
    latest_binding = (
        _mapping(update_events[-1].get("paper_action_binding")) if update_events else {}
    )
    event_view = {
        **advisory_event,
        **(
            {
                "paper_action_status": latest_binding.get("status"),
                "paper_action_weights": latest_binding.get("after_weights", {}),
            }
            if update_events
            else {}
        ),
    }
    expected_report = render_advisory_outcome_report(
        expected_manifest, event_view, expected_rows
    )
    dynamic_keys = (
        "status",
        "update_ledger_schema_version",
        "update_chain_status",
        "update_event_count",
        "latest_update_event_id",
        "latest_update_event_checksum",
        "updated_at",
        "updated_as_of",
        "data_quality_status",
        "data_quality_report_path",
        "paper_action_status",
        "available_window_count",
        "pending_window_count",
        "insufficient_window_count",
    )
    row_content_valid = all(
        row.get("outcome_status") in OUTCOME_WINDOW_STATUSES
        and (
            all(_is_finite_number(row.get(field)) for field in OUTCOME_METRIC_FIELDS)
            if row.get("outcome_status") == "AVAILABLE"
            else all(row.get(field) is None for field in OUTCOME_METRIC_FIELDS)
            and bool(_text(row.get("insufficient_reason")))
        )
        for row in rows
    )
    checks = [
        *existence_checks,
        _check("outcome_id_matches", manifest.get("outcome_id") == outcome_id, outcome_id),
        _check(
            "manifest_event_binding",
            manifest.get("daily_advisory_id") == advisory_event.get("daily_advisory_id")
            and manifest.get("as_of") == advisory_event.get("as_of")
            and manifest.get("generated_at") == advisory_event.get("tracked_at")
            and manifest.get("tracked_windows") == configured_windows
            and manifest.get("advisory_event_checksum") == advisory_event.get("event_checksum")
            and manifest.get("advisory_event_path") == str(paths["event"])
            and manifest.get("outcome_windows_path") == str(paths["windows"])
            and manifest.get("outcome_update_events_path") == str(paths["updates"])
            and manifest.get("advisory_counterfactuals_path")
            == str(paths["counterfactuals"])
            and manifest.get("advisory_outcome_report_path") == str(paths["report"])
            and manifest.get("source_snapshots_root")
            == str(outcome_dir / "source_snapshots")
            and manifest.get("status") in OUTCOME_MANIFEST_STATUSES,
            "manifest immutable event binding",
        ),
        _check("source_and_update_replay", not replay_errors, "; ".join(replay_errors)),
        _check("materialized_windows_match", rows == expected_rows, "latest replay rows"),
        _check(
            "manifest_dynamic_fields_match",
            all(manifest.get(key) == expected_manifest.get(key) for key in dynamic_keys),
            "latest update-derived manifest fields",
        ),
        _check(
            "counterfactual_policy_matches",
            counterfactuals == expected_counterfactuals
            and manifest.get("advisory_counterfactuals_checksum")
            == sha256(_canonical_json(expected_counterfactuals).encode("utf-8")).hexdigest(),
            "canonical counterfactual definitions",
        ),
        _check(
            "window_metric_semantics",
            bool(rows) and row_content_valid,
            "null vs finite metrics",
        ),
        _check("report_matches_replay", report == expected_report, str(paths["report"])),
        _check(
            "no_execution_effect",
            _outcome_record_has_no_execution_effect(manifest)
            and _outcome_record_has_no_execution_effect(advisory_event)
            and all(_outcome_record_has_no_execution_effect(item) for item in update_events),
            "advisory simulation only",
        ),
    ]
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    payload = _validation_payload(
        report_type="etf_dynamic_v3_advisory_outcome_validation",
        artifact_id_key="outcome_id",
        artifact_id=outcome_id,
        status=status,
        checks=checks,
    )
    payload.update(
        {
            "update_chain_status": "PASS" if status == "PASS" else "FAIL",
            "mutation_allowed": status == "PASS",
            "event_count": len(update_events),
        }
    )
    return payload


def run_owner_attribution(
    *,
    output_dir: Path = DEFAULT_OWNER_ATTRIBUTION_DIR,
    owner_review_dir: Path = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    outcome_dir: Path = DEFAULT_ADVISORY_OUTCOME_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    generated = generated if generated.tzinfo else generated.replace(tzinfo=UTC)
    owner_snapshot = _owner_attribution_owner_snapshot(
        owner_review_dir=owner_review_dir,
        generated_at=generated,
    )
    records = [
        _mapping(item.get("review"))
        for item in _records(owner_snapshot.get("selected_reviews"))
    ]
    outcome_snapshot = _owner_attribution_outcome_snapshot(
        records=records,
        outcome_dir=outcome_dir,
        generated_at=generated,
    )
    outcomes = _records(outcome_snapshot.get("selected_outcomes"))
    source_validation_summary = _owner_attribution_source_validation_summary(
        owner_snapshot=owner_snapshot,
        outcome_snapshot=outcome_snapshot,
    )
    summary = _owner_decision_summary(records)
    matrix = _advisory_acceptance_matrix(records)
    comparison = _decision_outcome_comparison(records, outcomes)
    attribution_id = _stable_id(
        "owner-attribution-v2",
        generated.isoformat(),
        sha256(_canonical_json(owner_snapshot).encode("utf-8")).hexdigest(),
        sha256(_canonical_json(outcome_snapshot).encode("utf-8")).hexdigest(),
    )
    artifact_dir = _unique_dir(output_dir / attribution_id)
    artifact_dir.mkdir(parents=True, exist_ok=False)
    owner_snapshot_path = artifact_dir / "owner_review_source_snapshot.json"
    outcome_snapshot_path = artifact_dir / "advisory_outcome_source_snapshot.json"
    source_validation_path = artifact_dir / "source_validation_summary.json"
    _write_json(owner_snapshot_path, owner_snapshot)
    _write_json(outcome_snapshot_path, outcome_snapshot)
    _write_json(source_validation_path, source_validation_summary)
    manifest = _owner_attribution_manifest_payload(
        attribution_dir=artifact_dir,
        generated_at=generated,
        owner_snapshot=owner_snapshot,
        outcome_snapshot=outcome_snapshot,
        comparison=comparison,
    )
    _write_json(artifact_dir / "owner_attribution_manifest.json", manifest)
    _write_json(artifact_dir / "owner_decision_summary.json", summary)
    _write_json(artifact_dir / "advisory_acceptance_matrix.json", matrix)
    _write_json(artifact_dir / "decision_outcome_comparison.json", comparison)
    _write_text(
        artifact_dir / "owner_attribution_report.md",
        render_owner_attribution_report(manifest, summary, matrix, comparison),
    )
    _update_latest_pointer(
        "latest_owner_attribution",
        artifact_dir.name,
        artifact_dir / "owner_attribution_manifest.json",
    )
    return {
        "attribution_id": artifact_dir.name,
        "attribution_dir": artifact_dir,
        "manifest": manifest,
        "owner_decision_summary": summary,
        "advisory_acceptance_matrix": matrix,
        "decision_outcome_comparison": comparison,
        "owner_review_source_snapshot": owner_snapshot,
        "advisory_outcome_source_snapshot": outcome_snapshot,
        "source_validation_summary": source_validation_summary,
    }


def owner_attribution_report_payload(
    *,
    attribution_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_OWNER_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    artifact_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=attribution_id if not latest else None,
        pointer_name="latest_owner_attribution",
    )
    return {
        **_read_json(artifact_dir / "owner_attribution_manifest.json"),
        "owner_decision_summary": _read_json(artifact_dir / "owner_decision_summary.json"),
        "advisory_acceptance_matrix": _read_json(artifact_dir / "advisory_acceptance_matrix.json"),
        "decision_outcome_comparison": _read_json(
            artifact_dir / "decision_outcome_comparison.json"
        ),
        "attribution_dir": str(artifact_dir),
    }


def validate_owner_attribution_artifact(
    *,
    attribution_id: str,
    output_dir: Path = DEFAULT_OWNER_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    artifact_dir = output_dir / attribution_id
    paths = {
        "manifest": artifact_dir / "owner_attribution_manifest.json",
        "owner_snapshot": artifact_dir / "owner_review_source_snapshot.json",
        "outcome_snapshot": artifact_dir / "advisory_outcome_source_snapshot.json",
        "source_validation": artifact_dir / "source_validation_summary.json",
        "decision_summary": artifact_dir / "owner_decision_summary.json",
        "acceptance_matrix": artifact_dir / "advisory_acceptance_matrix.json",
        "comparison": artifact_dir / "decision_outcome_comparison.json",
        "report": artifact_dir / "owner_attribution_report.md",
    }
    manifest = _read_optional_json(paths["manifest"]) or {}
    required_legacy_paths = (
        paths["manifest"],
        paths["decision_summary"],
        paths["acceptance_matrix"],
        paths["comparison"],
        paths["report"],
    )
    if manifest.get("source_snapshot_schema_version") != (
        OWNER_ATTRIBUTION_SNAPSHOT_SCHEMA_VERSION
    ):
        legacy_checks = [
            _check(
                "legacy_required_files",
                all(path.is_file() for path in required_legacy_paths),
                attribution_id,
            ),
            _check(
                "attribution_id_matches",
                manifest.get("attribution_id") == attribution_id,
                attribution_id,
            ),
            _check(
                "legacy_no_execution_effect",
                manifest.get("broker_action_allowed") is False
                and manifest.get("broker_action_taken") is False,
                "legacy attribution remains read-only evidence",
            ),
        ]
        structural_pass = all(check["passed"] for check in legacy_checks)
        payload = _validation_payload(
            report_type="etf_dynamic_v3_owner_attribution_validation",
            artifact_id_key="attribution_id",
            artifact_id=attribution_id,
            status="PASS_WITH_WARNINGS" if structural_pass else "FAIL",
            checks=legacy_checks,
        )
        payload.update(
            {
                "source_snapshot_status": "LEGACY_UNSNAPSHOTTED",
                "warnings": [
                    "LEGACY_UNSNAPSHOTTED: 旧归因报告缺少不可变source snapshots"
                ],
            }
        )
        return payload
    existence_checks = [
        _check(f"{name}_exists", path.is_file(), str(path))
        for name, path in paths.items()
    ]
    if not all(check["passed"] for check in existence_checks):
        payload = _validation_payload(
            report_type="etf_dynamic_v3_owner_attribution_validation",
            artifact_id_key="attribution_id",
            artifact_id=attribution_id,
            status="FAIL",
            checks=existence_checks,
        )
        payload["source_snapshot_status"] = "FAIL"
        return payload
    try:
        owner_snapshot = _read_json(paths["owner_snapshot"])
        outcome_snapshot = _read_json(paths["outcome_snapshot"])
        source_validation = _read_json(paths["source_validation"])
        summary = _read_json(paths["decision_summary"])
        matrix = _read_json(paths["acceptance_matrix"])
        comparison = _read_json(paths["comparison"])
        report = paths["report"].read_text(encoding="utf-8")
    except Exception as exc:  # noqa: BLE001
        checks = [*existence_checks, _check("artifact_parse", False, str(exc))]
        payload = _validation_payload(
            report_type="etf_dynamic_v3_owner_attribution_validation",
            artifact_id_key="attribution_id",
            artifact_id=attribution_id,
            status="FAIL",
            checks=checks,
        )
        payload["source_snapshot_status"] = "FAIL"
        return payload
    snapshot_errors = _owner_attribution_snapshot_errors(
        owner_snapshot=owner_snapshot,
        outcome_snapshot=outcome_snapshot,
    )
    records = [
        _mapping(item.get("review"))
        for item in _records(owner_snapshot.get("selected_reviews"))
    ]
    outcomes = _records(outcome_snapshot.get("selected_outcomes"))
    expected_summary = _owner_decision_summary(records)
    expected_matrix = _advisory_acceptance_matrix(records)
    expected_comparison = _decision_outcome_comparison(records, outcomes)
    expected_source_validation = _owner_attribution_source_validation_summary(
        owner_snapshot=owner_snapshot,
        outcome_snapshot=outcome_snapshot,
    )
    generated = _parse_datetime_text(_text(owner_snapshot.get("generated_cutoff")))
    expected_manifest: dict[str, Any] = {}
    expected_report = ""
    manifest_error = ""
    try:
        if generated is None:
            raise DynamicV3PaperTrackingError("owner attribution cutoff is invalid")
        expected_manifest = _owner_attribution_manifest_payload(
            attribution_dir=artifact_dir,
            generated_at=generated,
            owner_snapshot=owner_snapshot,
            outcome_snapshot=outcome_snapshot,
            comparison=expected_comparison,
        )
        expected_report = render_owner_attribution_report(
            expected_manifest,
            expected_summary,
            expected_matrix,
            expected_comparison,
        )
    except Exception as exc:  # noqa: BLE001
        manifest_error = str(exc)
    checks = [
        *existence_checks,
        _check(
            "attribution_id_matches",
            manifest.get("attribution_id") == attribution_id,
            attribution_id,
        ),
        _check("snapshot_content_valid", not snapshot_errors, ";".join(snapshot_errors)),
        _check(
            "owner_snapshot_checksum",
            manifest.get("owner_review_source_snapshot_checksum")
            == _file_sha256(paths["owner_snapshot"]),
            str(paths["owner_snapshot"]),
        ),
        _check(
            "outcome_snapshot_checksum",
            manifest.get("advisory_outcome_source_snapshot_checksum")
            == _file_sha256(paths["outcome_snapshot"]),
            str(paths["outcome_snapshot"]),
        ),
        _check(
            "source_validation_matches_snapshots",
            source_validation == expected_source_validation,
            "source validation materialized from snapshots",
        ),
        _check("decision_summary_matches", summary == expected_summary, "owner records"),
        _check("acceptance_matrix_matches", matrix == expected_matrix, "owner records"),
        _check("outcome_comparison_matches", comparison == expected_comparison, "outcome windows"),
        _check(
            "manifest_matches_snapshots",
            not manifest_error and manifest == expected_manifest,
            manifest_error or "manifest replay",
        ),
        _check(
            "report_matches_snapshots",
            not manifest_error and report == expected_report,
            str(paths["report"]),
        ),
        _check(
            "no_execution_effect",
            _owner_attribution_record_has_no_execution_effect(manifest),
            "owner attribution evidence only",
        ),
    ]
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    payload = _validation_payload(
        report_type="etf_dynamic_v3_owner_attribution_validation",
        artifact_id_key="attribution_id",
        artifact_id=attribution_id,
        status=status,
        checks=checks,
    )
    payload.update(
        {
            "source_snapshot_status": "PASS" if status == "PASS" else "FAIL",
            "selected_review_count": len(records),
            "selected_outcome_count": len(outcomes),
        }
    )
    return payload


def run_shadow_aging(
    *,
    shadow_shortlist_id: str,
    config_path: Path = DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
    output_dir: Path = DEFAULT_SHADOW_AGING_DIR,
    shadow_shortlist_dir: Path = DEFAULT_SHADOW_SHORTLIST_DIR,
    shadow_monitor_run_dir: Path = DEFAULT_SHADOW_MONITOR_RUN_DIR,
    consensus_drift_dir: Path = DEFAULT_CONSENSUS_DRIFT_DIR,
    advisory_outcome_dir: Path = DEFAULT_ADVISORY_OUTCOME_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    generated = generated if generated.tzinfo else generated.replace(tzinfo=UTC)
    source_snapshot = _shadow_aging_source_snapshot(
        shadow_shortlist_id=shadow_shortlist_id,
        config_path=config_path,
        shadow_shortlist_dir=shadow_shortlist_dir,
        shadow_monitor_run_dir=shadow_monitor_run_dir,
        consensus_drift_dir=consensus_drift_dir,
        advisory_outcome_dir=advisory_outcome_dir,
        generated_at=generated,
    )
    rows = _shadow_aging_candidate_rows(source_snapshot)
    summary = _shadow_aging_summary(source_snapshot=source_snapshot, rows=rows)
    source_validation = _shadow_aging_source_validation_summary(source_snapshot)
    aging_id = _stable_id(
        "shadow-aging-v2",
        shadow_shortlist_id,
        generated.isoformat(),
        sha256(_canonical_json(source_snapshot).encode("utf-8")).hexdigest(),
    )
    artifact_dir = _unique_dir(output_dir / aging_id)
    artifact_dir.mkdir(parents=True, exist_ok=False)
    source_snapshot_path = artifact_dir / "shadow_aging_source_snapshot.json"
    source_validation_path = artifact_dir / "source_validation_summary.json"
    _write_json(source_snapshot_path, source_snapshot)
    _write_json(source_validation_path, source_validation)
    summary = {**summary, "aging_id": artifact_dir.name}
    manifest = _shadow_aging_manifest_payload(
        aging_dir=artifact_dir,
        aging_id=artifact_dir.name,
        generated_at=generated,
        source_snapshot=source_snapshot,
        rows=rows,
        summary=summary,
    )
    _write_json(artifact_dir / "shadow_aging_manifest.json", manifest)
    _write_jsonl(artifact_dir / "candidate_aging_status.jsonl", rows)
    _write_json(artifact_dir / "promotion_clock_v2_summary.json", summary)
    _write_text(
        artifact_dir / "shadow_aging_report.md", render_shadow_aging_report(manifest, summary, rows)
    )
    _update_latest_pointer(
        "latest_shadow_aging", artifact_dir.name, artifact_dir / "shadow_aging_manifest.json"
    )
    return {
        "aging_id": artifact_dir.name,
        "aging_dir": artifact_dir,
        "manifest": manifest,
        "candidate_aging_status": rows,
        "promotion_clock_v2_summary": summary,
        "shadow_aging_source_snapshot": source_snapshot,
        "source_validation_summary": source_validation,
    }


def shadow_aging_report_payload(
    *,
    aging_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SHADOW_AGING_DIR,
) -> dict[str, Any]:
    artifact_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=aging_id if not latest else None,
        pointer_name="latest_shadow_aging",
    )
    return {
        **_read_json(artifact_dir / "shadow_aging_manifest.json"),
        "promotion_clock_v2_summary": _read_json(artifact_dir / "promotion_clock_v2_summary.json"),
        "candidate_aging_status": _read_jsonl(artifact_dir / "candidate_aging_status.jsonl"),
        "aging_dir": str(artifact_dir),
    }


def validate_shadow_aging_artifact(
    *,
    aging_id: str,
    output_dir: Path = DEFAULT_SHADOW_AGING_DIR,
) -> dict[str, Any]:
    artifact_dir = output_dir / aging_id
    paths = {
        "manifest": artifact_dir / "shadow_aging_manifest.json",
        "source_snapshot": artifact_dir / "shadow_aging_source_snapshot.json",
        "source_validation": artifact_dir / "source_validation_summary.json",
        "candidate_status": artifact_dir / "candidate_aging_status.jsonl",
        "summary": artifact_dir / "promotion_clock_v2_summary.json",
        "report": artifact_dir / "shadow_aging_report.md",
    }
    manifest = _read_optional_json(paths["manifest"]) or {}
    if manifest.get("source_snapshot_schema_version") != SHADOW_AGING_SNAPSHOT_SCHEMA_VERSION:
        legacy_rows = _read_jsonl(paths["candidate_status"])
        legacy_checks = [
            _check(
                "legacy_required_files",
                all(
                    paths[key].is_file()
                    for key in ("manifest", "candidate_status", "summary", "report")
                ),
                aging_id,
            ),
            _check("aging_id_matches", manifest.get("aging_id") == aging_id, aging_id),
            _check(
                "legacy_promotion_status_valid",
                all(
                    row.get("promotion_clock_status") in PROMOTION_CLOCK_STATUSES
                    for row in legacy_rows
                ),
                "promotion statuses",
            ),
            _check(
                "legacy_no_execution_effect",
                manifest.get("production_candidate_generated") is False
                and manifest.get("broker_action_allowed") is False
                and manifest.get("broker_action_taken") is False,
                "legacy aging remains manual-review-only evidence",
            ),
        ]
        structural_pass = all(check["passed"] for check in legacy_checks)
        payload = _validation_payload(
            report_type="etf_dynamic_v3_shadow_aging_validation",
            artifact_id_key="aging_id",
            artifact_id=aging_id,
            status="PASS_WITH_WARNINGS" if structural_pass else "FAIL",
            checks=legacy_checks,
        )
        payload.update(
            {
                "source_snapshot_status": "LEGACY_UNSNAPSHOTTED",
                "warnings": [
                    "LEGACY_UNSNAPSHOTTED: 旧shadow aging缺少validated source snapshot"
                ],
            }
        )
        return payload
    existence_checks = [
        _check(f"{name}_exists", path.is_file(), str(path)) for name, path in paths.items()
    ]
    if not all(check["passed"] for check in existence_checks):
        payload = _validation_payload(
            report_type="etf_dynamic_v3_shadow_aging_validation",
            artifact_id_key="aging_id",
            artifact_id=aging_id,
            status="FAIL",
            checks=existence_checks,
        )
        payload["source_snapshot_status"] = "FAIL"
        return payload
    try:
        source_snapshot = _read_json(paths["source_snapshot"])
        source_validation = _read_json(paths["source_validation"])
        rows = _read_jsonl(paths["candidate_status"])
        summary = _read_json(paths["summary"])
        report = paths["report"].read_text(encoding="utf-8")
    except Exception as exc:  # noqa: BLE001
        checks = [*existence_checks, _check("artifact_parse", False, str(exc))]
        payload = _validation_payload(
            report_type="etf_dynamic_v3_shadow_aging_validation",
            artifact_id_key="aging_id",
            artifact_id=aging_id,
            status="FAIL",
            checks=checks,
        )
        payload["source_snapshot_status"] = "FAIL"
        return payload
    snapshot_errors = _shadow_aging_snapshot_errors(source_snapshot)
    expected_rows = _shadow_aging_candidate_rows(source_snapshot)
    expected_summary = {
        **_shadow_aging_summary(source_snapshot=source_snapshot, rows=expected_rows),
        "aging_id": aging_id,
    }
    expected_source_validation = _shadow_aging_source_validation_summary(source_snapshot)
    generated = _parse_datetime_text(_text(source_snapshot.get("generated_cutoff")))
    expected_manifest: dict[str, Any] = {}
    expected_report = ""
    manifest_error = ""
    try:
        if generated is None:
            raise DynamicV3PaperTrackingError("shadow aging cutoff is invalid")
        expected_manifest = _shadow_aging_manifest_payload(
            aging_dir=artifact_dir,
            aging_id=aging_id,
            generated_at=generated,
            source_snapshot=source_snapshot,
            rows=expected_rows,
            summary=expected_summary,
        )
        expected_report = render_shadow_aging_report(
            expected_manifest, expected_summary, expected_rows
        )
    except Exception as exc:  # noqa: BLE001
        manifest_error = str(exc)
    checks = [
        *existence_checks,
        _check("aging_id_matches", manifest.get("aging_id") == aging_id, aging_id),
        _check("source_snapshot_valid", not snapshot_errors, ";".join(snapshot_errors)),
        _check(
            "source_snapshot_checksum",
            manifest.get("source_snapshot_checksum") == _file_sha256(paths["source_snapshot"]),
            str(paths["source_snapshot"]),
        ),
        _check(
            "source_validation_matches",
            source_validation == expected_source_validation,
            "source validation replay",
        ),
        _check("candidate_status_matches", rows == expected_rows, "candidate formula replay"),
        _check("summary_matches", summary == expected_summary, "summary replay"),
        _check(
            "manifest_matches",
            not manifest_error and manifest == expected_manifest,
            manifest_error or "manifest replay",
        ),
        _check(
            "report_matches",
            not manifest_error and report == expected_report,
            str(paths["report"]),
        ),
        _check(
            "no_execution_effect",
            _shadow_aging_record_has_no_execution_effect(manifest),
            "manual review evidence only",
        ),
    ]
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    payload = _validation_payload(
        report_type="etf_dynamic_v3_shadow_aging_validation",
        artifact_id_key="aging_id",
        artifact_id=aging_id,
        status=status,
        checks=checks,
    )
    payload.update(
        {
            "source_snapshot_status": "PASS" if status == "PASS" else "FAIL",
            "selected_monitor_count": len(_records(source_snapshot.get("selected_monitors"))),
            "selected_drift_count": len(_records(source_snapshot.get("selected_drifts"))),
            "selected_outcome_count": len(_records(source_snapshot.get("selected_outcomes"))),
        }
    )
    return payload


def run_weekly_advisory_review(
    *,
    week_ending: date,
    output_dir: Path = DEFAULT_WEEKLY_ADVISORY_REVIEW_DIR,
    shadow_monitor_run_dir: Path = DEFAULT_SHADOW_MONITOR_RUN_DIR,
    daily_advisory_dir: Path = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    owner_review_dir: Path = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    paper_portfolio_dir: Path = DEFAULT_PAPER_PORTFOLIO_DIR,
    advisory_outcome_dir: Path = DEFAULT_ADVISORY_OUTCOME_DIR,
    shadow_aging_dir: Path = DEFAULT_SHADOW_AGING_DIR,
    config_path: Path = DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    generated = generated if generated.tzinfo else generated.replace(tzinfo=UTC)
    week_start = week_ending - timedelta(days=6)
    source_snapshot = _weekly_advisory_source_snapshot(
        week_start=week_start,
        week_ending=week_ending,
        generated_at=generated,
        config_path=config_path,
        shadow_monitor_run_dir=shadow_monitor_run_dir,
        daily_advisory_dir=daily_advisory_dir,
        owner_review_dir=owner_review_dir,
        paper_portfolio_dir=paper_portfolio_dir,
        advisory_outcome_dir=advisory_outcome_dir,
        shadow_aging_dir=shadow_aging_dir,
    )
    weekly_advisory_summary = _weekly_advisory_summary_from_snapshot(source_snapshot)
    weekly_owner_decision_summary = _owner_decision_summary(
        [
            _mapping(item.get("review"))
            for item in _records(source_snapshot.get("selected_owner_reviews"))
        ]
    )
    weekly_paper_portfolio_summary = _weekly_paper_summary_from_snapshot(source_snapshot)
    weekly_shadow_candidate_summary = _weekly_aging_summary_from_snapshot(source_snapshot)
    source_validation_summary = _weekly_source_validation_summary(source_snapshot)
    decision = _weekly_review_decision(
        source_snapshot=source_snapshot,
        advisory_summary=weekly_advisory_summary,
        paper_summary=weekly_paper_portfolio_summary,
        aging_summary=weekly_shadow_candidate_summary,
    )
    weekly_id = _stable_id("weekly-advisory-review", week_ending.isoformat(), generated.isoformat())
    artifact_dir = _unique_dir(output_dir / weekly_id)
    artifact_dir.mkdir(parents=True, exist_ok=False)
    source_snapshot_path = artifact_dir / "weekly_review_source_snapshot.json"
    source_validation_path = artifact_dir / "source_validation_summary.json"
    _write_json(source_snapshot_path, source_snapshot)
    _write_json(source_validation_path, source_validation_summary)
    manifest = _weekly_review_manifest_payload(
        artifact_dir=artifact_dir,
        generated_at=generated,
        source_snapshot=source_snapshot,
        source_validation_summary=source_validation_summary,
        advisory_summary=weekly_advisory_summary,
        owner_summary=weekly_owner_decision_summary,
        paper_summary=weekly_paper_portfolio_summary,
        aging_summary=weekly_shadow_candidate_summary,
        decision=decision,
    )
    _write_json(artifact_dir / "weekly_advisory_summary.json", weekly_advisory_summary)
    _write_json(artifact_dir / "weekly_owner_decision_summary.json", weekly_owner_decision_summary)
    _write_json(
        artifact_dir / "weekly_paper_portfolio_summary.json", weekly_paper_portfolio_summary
    )
    _write_json(
        artifact_dir / "weekly_shadow_candidate_summary.json", weekly_shadow_candidate_summary
    )
    _write_json(artifact_dir / "weekly_review_manifest.json", manifest)
    report = render_weekly_advisory_review_report(
        manifest,
        weekly_advisory_summary,
        weekly_owner_decision_summary,
        weekly_paper_portfolio_summary,
        weekly_shadow_candidate_summary,
    )
    _write_text(artifact_dir / "weekly_review_report.md", report)
    _write_text(
        artifact_dir / "reader_brief_section.md",
        render_weekly_advisory_reader_brief(
            manifest, weekly_owner_decision_summary, weekly_shadow_candidate_summary
        ),
    )
    _update_latest_pointer(
        "latest_weekly_advisory_review",
        artifact_dir.name,
        artifact_dir / "weekly_review_manifest.json",
    )
    return {
        "weekly_review_id": artifact_dir.name,
        "weekly_review_dir": artifact_dir,
        "manifest": manifest,
        "weekly_advisory_summary": weekly_advisory_summary,
        "weekly_owner_decision_summary": weekly_owner_decision_summary,
        "weekly_paper_portfolio_summary": weekly_paper_portfolio_summary,
        "weekly_shadow_candidate_summary": weekly_shadow_candidate_summary,
        "source_snapshot": source_snapshot,
        "source_validation_summary": source_validation_summary,
    }


def weekly_advisory_review_report_payload(
    *,
    weekly_review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_WEEKLY_ADVISORY_REVIEW_DIR,
) -> dict[str, Any]:
    artifact_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=weekly_review_id if not latest else None,
        pointer_name="latest_weekly_advisory_review",
    )
    return {
        **_read_json(artifact_dir / "weekly_review_manifest.json"),
        "weekly_review_source_snapshot": _read_optional_json(
            artifact_dir / "weekly_review_source_snapshot.json"
        ),
        "source_validation_summary": _read_optional_json(
            artifact_dir / "source_validation_summary.json"
        ),
        "weekly_advisory_summary": _read_json(artifact_dir / "weekly_advisory_summary.json"),
        "weekly_owner_decision_summary": _read_json(
            artifact_dir / "weekly_owner_decision_summary.json"
        ),
        "weekly_paper_portfolio_summary": _read_json(
            artifact_dir / "weekly_paper_portfolio_summary.json"
        ),
        "weekly_shadow_candidate_summary": _read_json(
            artifact_dir / "weekly_shadow_candidate_summary.json"
        ),
        "weekly_review_dir": str(artifact_dir),
    }


def validate_weekly_advisory_review_artifact(
    *,
    weekly_review_id: str,
    output_dir: Path = DEFAULT_WEEKLY_ADVISORY_REVIEW_DIR,
) -> dict[str, Any]:
    artifact_dir = output_dir / weekly_review_id
    paths = {
        "manifest": artifact_dir / "weekly_review_manifest.json",
        "source_snapshot": artifact_dir / "weekly_review_source_snapshot.json",
        "source_validation": artifact_dir / "source_validation_summary.json",
        "advisory_summary": artifact_dir / "weekly_advisory_summary.json",
        "owner_summary": artifact_dir / "weekly_owner_decision_summary.json",
        "paper_summary": artifact_dir / "weekly_paper_portfolio_summary.json",
        "aging_summary": artifact_dir / "weekly_shadow_candidate_summary.json",
        "report": artifact_dir / "weekly_review_report.md",
        "reader_brief": artifact_dir / "reader_brief_section.md",
    }
    manifest = _read_optional_json(paths["manifest"]) or {}
    if (
        manifest.get("source_snapshot_schema_version")
        != WEEKLY_ADVISORY_REVIEW_SNAPSHOT_SCHEMA_VERSION
    ):
        legacy_checks = [
            _check(
                "legacy_required_files",
                all(
                    paths[key].is_file()
                    for key in (
                        "manifest",
                        "advisory_summary",
                        "owner_summary",
                        "paper_summary",
                        "aging_summary",
                        "report",
                        "reader_brief",
                    )
                ),
                weekly_review_id,
            ),
            _check(
                "weekly_review_id_matches",
                manifest.get("weekly_review_id") == weekly_review_id,
                weekly_review_id,
            ),
            _check(
                "legacy_no_execution_effect",
                manifest.get("production_candidate_generated") is False
                and manifest.get("broker_action_allowed") is False
                and manifest.get("broker_action_taken") is False,
                "legacy weekly review remains manual-review-only evidence",
            ),
        ]
        structural_pass = all(check["passed"] for check in legacy_checks)
        payload = _validation_payload(
            report_type="etf_dynamic_v3_weekly_advisory_review_validation",
            artifact_id_key="weekly_review_id",
            artifact_id=weekly_review_id,
            status="PASS_WITH_WARNINGS" if structural_pass else "FAIL",
            checks=legacy_checks,
        )
        payload.update(
            {
                "source_snapshot_status": "LEGACY_UNSNAPSHOTTED",
                "warnings": [
                    "LEGACY_UNSNAPSHOTTED: 旧weekly review缺少validated cutoff source snapshot"
                ],
            }
        )
        return payload
    existence_checks = [
        _check(f"{name}_exists", path.is_file(), str(path)) for name, path in paths.items()
    ]
    if not all(check["passed"] for check in existence_checks):
        payload = _validation_payload(
            report_type="etf_dynamic_v3_weekly_advisory_review_validation",
            artifact_id_key="weekly_review_id",
            artifact_id=weekly_review_id,
            status="FAIL",
            checks=existence_checks,
        )
        payload["source_snapshot_status"] = "FAIL"
        return payload
    try:
        source_snapshot = _read_json(paths["source_snapshot"])
        source_validation = _read_json(paths["source_validation"])
        advisory_summary = _read_json(paths["advisory_summary"])
        owner_summary = _read_json(paths["owner_summary"])
        paper_summary = _read_json(paths["paper_summary"])
        aging_summary = _read_json(paths["aging_summary"])
        report = paths["report"].read_text(encoding="utf-8")
        reader_brief = paths["reader_brief"].read_text(encoding="utf-8")
    except Exception as exc:  # noqa: BLE001
        checks = [*existence_checks, _check("artifact_parse", False, str(exc))]
        payload = _validation_payload(
            report_type="etf_dynamic_v3_weekly_advisory_review_validation",
            artifact_id_key="weekly_review_id",
            artifact_id=weekly_review_id,
            status="FAIL",
            checks=checks,
        )
        payload["source_snapshot_status"] = "FAIL"
        return payload
    snapshot_errors = _weekly_advisory_snapshot_errors(source_snapshot)
    expected_source_validation = _weekly_source_validation_summary(source_snapshot)
    expected_advisory = _weekly_advisory_summary_from_snapshot(source_snapshot)
    expected_owner = _owner_decision_summary(
        [
            _mapping(item.get("review"))
            for item in _records(source_snapshot.get("selected_owner_reviews"))
        ]
    )
    expected_paper = _weekly_paper_summary_from_snapshot(source_snapshot)
    expected_aging = _weekly_aging_summary_from_snapshot(source_snapshot)
    decision = _weekly_review_decision(
        source_snapshot=source_snapshot,
        advisory_summary=expected_advisory,
        paper_summary=expected_paper,
        aging_summary=expected_aging,
    )
    generated = _parse_datetime_text(_text(source_snapshot.get("generated_at")))
    expected_manifest: dict[str, Any] = {}
    expected_report = ""
    expected_reader_brief = ""
    replay_error = ""
    try:
        if generated is None:
            raise DynamicV3PaperTrackingError("weekly review generated_at is invalid")
        expected_manifest = _weekly_review_manifest_payload(
            artifact_dir=artifact_dir,
            generated_at=generated,
            source_snapshot=source_snapshot,
            source_validation_summary=expected_source_validation,
            advisory_summary=expected_advisory,
            owner_summary=expected_owner,
            paper_summary=expected_paper,
            aging_summary=expected_aging,
            decision=decision,
        )
        expected_report = render_weekly_advisory_review_report(
            expected_manifest,
            expected_advisory,
            expected_owner,
            expected_paper,
            expected_aging,
        )
        expected_reader_brief = render_weekly_advisory_reader_brief(
            expected_manifest, expected_owner, expected_aging
        )
    except Exception as exc:  # noqa: BLE001
        replay_error = str(exc)
    checks = [
        *existence_checks,
        _check(
            "weekly_review_id_matches",
            manifest.get("weekly_review_id") == weekly_review_id,
            weekly_review_id,
        ),
        _check("source_snapshot_valid", not snapshot_errors, ";".join(snapshot_errors)),
        _check(
            "source_snapshot_checksum",
            manifest.get("source_snapshot_checksum")
            == _file_sha256(paths["source_snapshot"]),
            str(paths["source_snapshot"]),
        ),
        _check(
            "source_validation_matches",
            source_validation == expected_source_validation,
            "source validation replay",
        ),
        _check("advisory_summary_matches", advisory_summary == expected_advisory, "replay"),
        _check("owner_summary_matches", owner_summary == expected_owner, "replay"),
        _check("paper_summary_matches", paper_summary == expected_paper, "replay"),
        _check("aging_summary_matches", aging_summary == expected_aging, "replay"),
        _check(
            "manifest_matches",
            not replay_error and manifest == expected_manifest,
            replay_error or "manifest replay",
        ),
        _check(
            "report_matches",
            not replay_error and report == expected_report,
            str(paths["report"]),
        ),
        _check(
            "reader_brief_matches",
            not replay_error and reader_brief == expected_reader_brief,
            str(paths["reader_brief"]),
        ),
        _check(
            "no_execution_effect",
            _weekly_review_record_has_no_execution_effect(manifest),
            "weekly manual-review evidence only",
        ),
    ]
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    payload = _validation_payload(
        report_type="etf_dynamic_v3_weekly_advisory_review_validation",
        artifact_id_key="weekly_review_id",
        artifact_id=weekly_review_id,
        status=status,
        checks=checks,
    )
    payload.update(
        {
            "source_snapshot_status": "PASS" if status == "PASS" else "FAIL",
            "evidence_status": manifest.get("evidence_status", "MISSING"),
            "selected_monitor_count": len(
                _records(source_snapshot.get("selected_monitors"))
            ),
            "selected_daily_advisory_count": len(
                _records(source_snapshot.get("selected_daily_advisories"))
            ),
            "selected_owner_review_count": len(
                _records(source_snapshot.get("selected_owner_reviews"))
            ),
        }
    )
    return payload


def render_paper_portfolio_report(
    manifest: Mapping[str, Any],
    state: Mapping[str, Any],
    ledger: Sequence[Mapping[str, Any]],
) -> str:
    weights = _format_weights(_mapping(state.get("positions")))
    return "\n".join(
        [
            "# Dynamic Rescue Paper Portfolio",
            "",
            f"- 状态：{state.get('state_status', 'UNKNOWN')}",
            f"- paper_portfolio_id：`{manifest.get('paper_portfolio_id', '')}`",
            f"- as_of：{state.get('as_of', '')}",
            f"- 当前权重：{weights}",
            f"- paper action count：{len(ledger)}",
            f"- ledger schema：{manifest.get('ledger_schema_version', 'LEGACY_UNCHAINED')}",
            f"- event chain status：{manifest.get('event_chain_status', 'LEGACY_UNCHAINED')}",
            "- config policy："
            f"{manifest.get('config_policy_id', '')}@{manifest.get('config_policy_version', '')}",
            "- broker_action_allowed：false",
            "- broker_action_taken：false",
            "- 解释：这是 advisory_simulation_only 纸面组合，"
            "不是 broker import，也不是真实仓位修改。",
            "- 完整性说明：event checksum 是可重算的审计链，不是 owner 数字签名。",
            "",
        ]
    )


def render_advisory_outcome_report(
    manifest: Mapping[str, Any],
    event: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Dynamic Rescue Advisory Outcome",
        "",
        f"- 状态：{manifest.get('status', 'UNKNOWN')}",
        f"- outcome_id：`{manifest.get('outcome_id', '')}`",
        f"- daily_advisory_id：`{event.get('daily_advisory_id', '')}`",
        f"- recommended_action：{event.get('recommended_action', '')}",
        f"- data_quality_status：{manifest.get('data_quality_status', 'UNKNOWN')}",
        f"- update_chain_status：{manifest.get('update_chain_status', 'UNKNOWN')}",
        f"- update_event_count：{manifest.get('update_event_count', 0)}",
        f"- paper_action_status：{manifest.get('paper_action_status', 'PENDING')}",
        "- broker_action_taken：false",
        "",
        "| Window | Status | Paper vs no_trade | Paper vs baseline | "
        "Limited return | Paper effective | End | Reason |",
        "|---:|---|---:|---:|---:|---|---|---|",
    ]
    for row in rows:
        lines.append(
            "| "
            f"{row.get('window_days')} | "
            f"{row.get('outcome_status')} | "
            f"{_format_optional_metric(row.get('relative_to_no_trade'))} | "
            f"{_format_optional_metric(row.get('relative_to_baseline'))} | "
            f"{_format_optional_metric(row.get('limited_adjustment_return'))} | "
            f"{row.get('paper_action_effective_date', '') or 'N/A'} | "
            f"{row.get('end_date', '') or 'N/A'} | "
            f"{row.get('insufficient_reason', '') or '-'} |"
        )
    lines.extend(
        [
            "",
            "## 阅读口径",
            "",
            "AVAILABLE 只表示该窗口已有足够价格数据完成纸面评估；"
            "PENDING 和 INSUFFICIENT_DATA 不得解释为收益为 0。",
            "静态反事实采用起点固定份额；paper_action 只在 action 已知后首个完整交易日生效，"
            "并按换手率一次性扣除 transaction cost 与 slippage，避免未来信息穿越。",
            "该报告不生成交易指令，不触发 broker action。",
            "",
        ]
    )
    return "\n".join(lines)


def _format_optional_metric(value: Any) -> str:
    return f"{float(value):.6f}" if _is_finite_number(value) else "N/A"


def render_owner_attribution_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    matrix: Mapping[str, Any],
    comparison: Mapping[str, Any],
) -> str:
    lines = [
        "# Dynamic Rescue Owner Attribution",
        "",
        f"- 工程状态：{manifest.get('status', 'UNKNOWN')}",
        f"- 证据状态：{manifest.get('evidence_status', 'UNKNOWN')}",
        f"- attribution_id：`{manifest.get('attribution_id', '')}`",
        f"- total_reviews：{summary.get('total_reviews', 0)}",
        f"- final_reviews：{summary.get('final_review_count', 0)}",
        f"- linked_outcomes：{comparison.get('linked_outcome_count', 0)}",
        f"- available_outcomes：{comparison.get('available_outcome_count', 0)}",
        f"- available_windows：{comparison.get('available_window_count', 0)}",
        f"- most_common_owner_decision：{summary.get('most_common_owner_decision', 'MISSING')}",
        f"- outcome_comparison_status：{comparison.get('status', 'UNKNOWN')}",
        "- source_snapshot_status：PASS",
        "- broker_action_taken：false",
        "",
        "## Decision / Outcome Evidence",
        "",
        "| Owner decision | Reviews | Linked outcomes | Available outcomes | "
        "Available windows | Avg 5d vs no-trade | Avg 20d vs no-trade | Reason |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in _records(comparison.get("decision_groups")):
        lines.append(
            "| "
            f"{row.get('owner_decision')} | "
            f"{row.get('review_count')} | "
            f"{row.get('linked_outcome_count')} | "
            f"{row.get('available_outcome_count')} | "
            f"{row.get('available_window_count')} | "
            f"{_format_optional_metric(row.get('avg_5d_relative_to_no_trade'))} | "
            f"{_format_optional_metric(row.get('avg_20d_relative_to_no_trade'))} | "
            f"{row.get('insufficient_reason') or '-'} |"
        )
    lines.extend(
        [
            "",
            "## Advisory / Owner Decision Matrix",
            "",
            json.dumps(
                matrix.get("by_recommended_action", {}),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ),
            "",
            "## 阅读口径",
            "",
            "review、linked outcome、available outcome 与 available window 是不同样本单位，"
            "不得混用。缺少 5d/20d 证据显示为 N/A，不解释为 0。",
            "`accepted_monitor` 仅为兼容字段，语义等于 monitor_count，不代表接受或批准建议。",
            "该报告只做 owner 决策与已验证 outcome 的事后归因；普通 checksum 不是数字签名，"
            "PASS 也不是 policy adoption、portfolio approval、order 或 broker authorization。",
            "",
        ]
    )
    return "\n".join(lines)


def render_shadow_aging_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Dynamic Rescue Shadow Aging v2",
        "",
        f"- 工程状态：{manifest.get('status', 'UNKNOWN')}",
        f"- 证据状态：{manifest.get('evidence_status', 'UNKNOWN')}",
        f"- aging_id：`{manifest.get('aging_id', '')}`",
        f"- policy：{manifest.get('policy_id', '')}@{manifest.get('policy_version', '')}",
        f"- selected monitors：{summary.get('selected_monitor_count', 0)}",
        f"- selected drifts：{summary.get('selected_drift_count', 0)}",
        f"- selected outcomes：{summary.get('selected_outcome_count', 0)}",
        f"- eligible_for_review_count：{summary.get('eligible_for_review_count', 0)}",
        f"- downgrade_recommended_count：{summary.get('downgrade_recommended_count', 0)}",
        "- production_candidate_generated：false",
        "- broker_action_taken：false",
        "",
        "| Candidate | Calendar span | Observation days | Monitor runs | True rebalances | "
        "Drift evidence/missing | Outcome windows | Outcome score | Status | Reasons |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for row in rows:
        reasons = [
            *_texts(row.get("blocking_reasons")),
            *_texts(row.get("downgrade_reasons")),
        ]
        lines.append(
            "| "
            f"{row.get('candidate_id')} | "
            f"{row.get('calendar_span_days')} | "
            f"{row.get('unique_observation_day_count')} | "
            f"{row.get('monitor_run_count')} | "
            f"{row.get('true_rebalance_count')} | "
            f"{row.get('consensus_drift_evidence_count')}/"
            f"{row.get('missing_consensus_drift_count')} | "
            f"{row.get('candidate_outcome_available_window_count')} | "
            f"{_format_optional_metric(row.get('outcome_score'))} | "
            f"{row.get('promotion_clock_status')} | "
            f"{', '.join(reasons) or 'none'} |"
        )
    lines.extend(
        [
            "",
            "## 阅读口径",
            "",
            "Calendar span、unique observation days、monitor runs 与 true rebalances "
            "是不同样本单位；true rebalance 仅在相邻日期target weights实际变化时增加。",
            "Outcome score只使用与candidate id绑定、已验证且AVAILABLE的窗口，按policy等权平均；"
            "缺少candidate outcome显示N/A并阻断资格，不解释为0。",
            "Consensus drift是shortlist级共享证据，缺失单独披露，不能由重复artifact加权。",
            "`eligible_for_review`只进入人工复核队列，不是promotion、official weights、"
            "portfolio approval、order或broker authorization。普通checksum不是数字签名。",
            "",
        ]
    )
    return "\n".join(lines)


def render_weekly_advisory_review_report(
    manifest: Mapping[str, Any],
    advisory_summary: Mapping[str, Any],
    owner_summary: Mapping[str, Any],
    paper_summary: Mapping[str, Any],
    shadow_summary: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Dynamic Rescue Weekly Advisory Review",
            "",
            f"- 状态：{manifest.get('status', 'UNKNOWN')}",
            f"- weekly_review_id：`{manifest.get('weekly_review_id', '')}`",
            f"- cadence：{manifest.get('cadence', 'MISSING')}",
            f"- market_regime：{manifest.get('market_regime', 'MISSING')}",
            f"- actual date range：{manifest.get('week_start', '')} 至 "
            f"{manifest.get('week_ending', '')}",
            f"- evidence_cutoff：{manifest.get('evidence_cutoff', '')}",
            f"- evidence_status：{manifest.get('evidence_status', 'MISSING')}",
            f"- weekly_recommendation：{manifest.get('weekly_recommendation', '')}",
            f"- blocking_reasons：{', '.join(_texts(manifest.get('blocking_reasons'))) or 'none'}",
            f"- shadow monitor runs：{advisory_summary.get('shadow_monitor_run_count', 0)}",
            f"- daily advisory count：{advisory_summary.get('daily_advisory_count', 0)}",
            f"- owner reviews：{owner_summary.get('total_reviews', 0)}",
            f"- paper_portfolio_status：{paper_summary.get('state_status', 'MISSING')}",
            "- outcome_status："
            f"{_mapping(advisory_summary.get('outcome_summary')).get('status', 'MISSING')}",
            "- available outcome windows："
            f"{_mapping(advisory_summary.get('outcome_summary')).get('available_window_count', 0)}",
            "- avg relative to no-trade："
            f"{_format_optional_metric(_mapping(advisory_summary.get('outcome_summary')).get('avg_relative_to_no_trade'))}",
            f"- eligible_for_review_count：{shadow_summary.get('eligible_for_review_count', 0)}",
            "- downgrade_recommended_count："
            f"{shadow_summary.get('downgrade_recommended_count', 0)}",
            f"- data_quality_status：{manifest.get('data_quality_status', 'MISSING')}",
            f"- next_actions：{', '.join(_texts(manifest.get('next_actions')))}",
            "- broker_action_taken：false",
            "- production_candidate_generated：false",
            "",
            "该weekly review只消费截至week ending cutoff的validated source prefix；"
            "后续owner/paper/outcome append不会倒灌本周。缺失outcome保持N/A，不解释为0。",
            "eligible_for_review只表示人工promotion review候选，不自动进入production；"
            "validation PASS只证明周报可重放，不证明策略有效。",
            "",
        ]
    )


def render_weekly_advisory_reader_brief(
    manifest: Mapping[str, Any],
    owner_summary: Mapping[str, Any],
    shadow_summary: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Weekly Advisory Review",
            "",
            f"- evidence_status: {manifest.get('evidence_status', 'MISSING')}",
            f"- weekly_recommendation: {manifest.get('weekly_recommendation', 'MISSING')}",
            f"- actual_date_range: {manifest.get('week_start', '')} to "
            f"{manifest.get('week_ending', '')}",
            f"- data_quality_status: {manifest.get('data_quality_status', 'MISSING')}",
            f"- paper_portfolio_status: {manifest.get('paper_portfolio_status', 'MISSING')}",
            "- owner_decision_summary: "
            f"{owner_summary.get('most_common_owner_decision', 'MISSING')}",
            "- candidate_aging_summary: "
            f"eligible={shadow_summary.get('eligible_for_review_count', 0)}, "
            f"downgrade={shadow_summary.get('downgrade_recommended_count', 0)}",
            f"- next_action: {', '.join(_texts(manifest.get('next_actions')))}",
            "- broker_action_taken: false",
            "",
        ]
    )


def _manual_snapshot_weights(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise DynamicV3PaperTrackingError(f"manual snapshot not found: {path}")
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise DynamicV3PaperTrackingError("manual snapshot must be a mapping")
    cash = _mapping(raw.get("cash"))
    positions = _records(raw.get("positions"))
    symbols = [_text(row.get("symbol")) for row in positions]
    if not symbols or any(not symbol for symbol in symbols):
        raise DynamicV3PaperTrackingError("manual snapshot positions require symbols")
    cash_symbol = _text(cash.get("symbol"), "CASH")
    all_symbols = [*symbols, cash_symbol]
    if len(all_symbols) != len(set(all_symbols)):
        raise DynamicV3PaperTrackingError("manual snapshot symbols must be unique")
    weights = {
        symbol: _strict_finite_number(row.get("weight"), f"weight:{symbol}")
        for symbol, row in zip(symbols, positions, strict=True)
    }
    weights[cash_symbol] = _strict_finite_number(cash.get("weight"), f"weight:{cash_symbol}")
    if any(value < 0 or value > 1 for value in weights.values()):
        raise DynamicV3PaperTrackingError("manual snapshot weights must be within [0,1]")
    if abs(sum(weights.values()) - 1.0) > PAPER_WEIGHT_TOLERANCE:
        raise DynamicV3PaperTrackingError("manual snapshot weights must sum to 1")
    weights = _normalize_weights(weights)
    metadata = _mapping(raw.get("metadata"))
    if metadata.get("owner_reviewed") is not True:
        raise DynamicV3PaperTrackingError("manual snapshot must be owner reviewed")
    if metadata.get("broker_imported") is True:
        raise DynamicV3PaperTrackingError("broker_imported=true is not allowed")
    as_of = _text(raw.get("as_of"))
    if _date_from_any(as_of) is None:
        raise DynamicV3PaperTrackingError("manual snapshot as_of must be an ISO date")
    return {
        "as_of": as_of,
        "base_currency": _text(raw.get("base_currency"), "USD"),
        "weights": weights,
    }


def _paper_state_payload(
    *,
    paper_portfolio_id: str,
    as_of: str,
    base_currency: str,
    source: str,
    positions: Mapping[str, Any],
    last_review_id: str,
    last_action_id: str,
    state_status: str,
) -> dict[str, Any]:
    weights = _normalize_weights(positions)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_portfolio_state",
        "paper_portfolio_id": paper_portfolio_id,
        "as_of": as_of,
        "base_currency": base_currency,
        "source": source,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "positions": weights,
        "total_weight": round(sum(weights.values()), 6),
        "last_review_id": last_review_id,
        "last_action_id": last_action_id,
        "state_status": state_status,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def _paper_proposed_deltas(
    *,
    daily_advisory_id: str,
    before_weights: Mapping[str, Any],
    daily_advisory_dir: Path,
) -> dict[str, float]:
    advisory_dir = daily_advisory_dir / daily_advisory_id
    target = _advisory_consensus_weights(advisory_dir)
    if not target:
        delta_rows = _read_jsonl(advisory_dir / "daily_position_deltas.jsonl")
        candidates = [
            _mapping(row.get("deltas")) for row in delta_rows if _mapping(row.get("deltas"))
        ]
        if candidates:
            target = _apply_weight_deltas(before_weights, candidates[0])
    if not target:
        return {}
    symbols = sorted(set(before_weights) | set(target))
    return {
        symbol: round(_float(target.get(symbol)) - _float(before_weights.get(symbol)), 6)
        for symbol in symbols
        if round(_float(target.get(symbol)) - _float(before_weights.get(symbol)), 6) != 0
    }


def _validated_manual_deltas(deltas: Mapping[str, Any] | None) -> dict[str, float]:
    if not isinstance(deltas, Mapping) or not deltas:
        raise DynamicV3PaperTrackingError(
            "manual_adjustment requires a non-empty manual deltas object"
        )
    raw = _validated_delta_map(deltas, label="manual_delta")
    if not raw:
        raise DynamicV3PaperTrackingError("manual_adjustment deltas cannot all be zero")
    return raw


def _validated_delta_map(deltas: Mapping[str, Any], *, label: str) -> dict[str, float]:
    raw: dict[str, float] = {}
    for symbol, value in deltas.items():
        clean_symbol = _text(symbol)
        if not clean_symbol:
            raise DynamicV3PaperTrackingError(f"{label} symbol cannot be empty")
        number = _strict_finite_number(value, f"{label}:{clean_symbol}")
        if number:
            raw[clean_symbol] = number
    if raw and abs(sum(raw.values())) > PAPER_WEIGHT_TOLERANCE:
        raise DynamicV3PaperTrackingError(f"{label} values must sum to zero")
    return dict(sorted(raw.items()))


def _limit_paper_deltas(
    deltas: Mapping[str, Any],
    config: Mapping[str, Any],
    *,
    before_weights: Mapping[str, Any],
) -> dict[str, float]:
    raw = _validated_delta_map(deltas, label="proposed_delta")
    if not raw:
        return {}
    simulation = _mapping(config.get("simulation"))
    max_total = _strict_finite_number(
        simulation.get("max_single_day_total_adjustment"),
        "max_single_day_total_adjustment",
    )
    max_symbol = _strict_finite_number(
        simulation.get("max_single_symbol_adjustment"),
        "max_single_symbol_adjustment",
    )
    min_trade = _strict_finite_number(
        simulation.get("min_trade_threshold"), "min_trade_threshold"
    )
    if all(abs(value) < min_trade for value in raw.values()):
        return {}
    total_abs = sum(abs(value) for value in raw.values())
    max_abs = max(abs(value) for value in raw.values())
    scale = 1.0
    if total_abs > max_total > 0:
        scale = min(scale, max_total / total_abs)
    if max_abs > max_symbol > 0:
        scale = min(scale, max_symbol / max_abs)
    for symbol, value in raw.items():
        available = _float(before_weights.get(symbol))
        if value < 0 and abs(value) > available:
            scale = min(scale, available / abs(value))
    limited = {symbol: round(value * scale, 6) for symbol, value in raw.items()}
    limited = {symbol: value for symbol, value in limited.items() if value != 0}
    drift = round(sum(limited.values()), 6)
    if drift and "CASH" in limited:
        limited["CASH"] = round(limited["CASH"] - drift, 6)
    elif drift:
        limited["CASH"] = round(-drift, 6)
    result = {symbol: value for symbol, value in sorted(limited.items()) if value != 0}
    if abs(sum(result.values())) > PAPER_WEIGHT_TOLERANCE:
        raise DynamicV3PaperTrackingError("limited paper deltas must sum to zero")
    if any(
        _float(before_weights.get(symbol)) + value < -PAPER_WEIGHT_TOLERANCE
        for symbol, value in result.items()
    ):
        raise DynamicV3PaperTrackingError("limited paper deltas cannot create negative weights")
    return result


def _apply_weight_deltas(weights: Mapping[str, Any], deltas: Mapping[str, Any]) -> dict[str, float]:
    symbols = sorted(set(weights) | set(deltas))
    result = {
        symbol: round(max(0.0, _float(weights.get(symbol)) + _float(deltas.get(symbol))), 6)
        for symbol in symbols
    }
    return _normalize_weights(result)


def _rebuild_paper_state(
    manifest: Mapping[str, Any], ledger: Sequence[Mapping[str, Any]]
) -> dict[str, float]:
    weights = _normalize_weights(_mapping(manifest.get("initial_weights")))
    for row in ledger:
        weights = _apply_weight_deltas(weights, _mapping(row.get("applied_paper_deltas")))
    return weights


def _paper_action_event_checksum(event: Mapping[str, Any]) -> str:
    payload = dict(event)
    payload.pop("event_checksum", None)
    return sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _build_paper_action_event(
    *,
    events: Sequence[Mapping[str, Any]],
    paper_portfolio_id: str,
    review: Mapping[str, Any],
    owner_review_dir: Path,
    source_daily_root: Path,
    source_paths: Mapping[str, Any],
    source_checksums: Mapping[str, Any],
    config_path: Path,
    config: Mapping[str, Any],
    action_type: str,
    manual_override: bool,
    before_weights: Mapping[str, Any],
    proposed_deltas: Mapping[str, Any],
    applied_deltas: Mapping[str, Any],
    after_weights: Mapping[str, Any],
    reason: str,
    event_at: datetime,
) -> dict[str, Any]:
    sequence = len(events) + 1
    event_at_text = event_at.isoformat()
    if events:
        previous_at = _parse_datetime_text(_text(events[-1].get("created_at")))
        if previous_at is None or event_at < previous_at:
            raise DynamicV3PaperTrackingError(
                "paper action event time cannot precede the prior event"
            )
    review_id = _text(review.get("review_id"))
    paper_action_id = _stable_id(
        "paper-portfolio-action",
        paper_portfolio_id,
        sequence,
        review_id,
        event_at_text,
    )
    policy = _mapping(config.get("policy_metadata"))
    event = {
        "schema_version": SCHEMA_VERSION,
        "ledger_schema_version": PAPER_ACTION_LEDGER_SCHEMA_VERSION,
        "event_sequence": sequence,
        "event_type": PAPER_ACTION_EVENT_TYPE,
        "paper_action_id": paper_action_id,
        "paper_portfolio_id": paper_portfolio_id,
        "review_id": review_id,
        "owner_review_root": str(owner_review_dir),
        "owner_review_event_id": _text(review.get("last_event_id")),
        "owner_review_event_checksum": _text(review.get("last_event_checksum")),
        "owner_review_validation_status": "PASS",
        "daily_advisory_id": _text(review.get("daily_advisory_id")),
        "source_daily_advisory_root": str(source_daily_root),
        "source_daily_advisory_validation_status": "PASS",
        "source_artifact_paths": dict(source_paths),
        "source_artifact_checksums": dict(source_checksums),
        "config_path": str(config_path),
        "config_checksum": _file_sha256(config_path),
        "config_policy_id": _text(policy.get("policy_id")),
        "config_policy_version": _text(policy.get("version")),
        "as_of": _text(review.get("as_of")),
        "owner_decision": _text(review.get("owner_decision")),
        "action_type": action_type,
        "manual_override": manual_override,
        "before_weights": dict(before_weights),
        "proposed_deltas": dict(proposed_deltas),
        "applied_paper_deltas": dict(applied_deltas),
        "after_weights": dict(after_weights),
        "reason": reason,
        "notes": _text(review.get("manual_notes")),
        "previous_event_checksum": (
            _text(events[-1].get("event_checksum")) if events else "GENESIS"
        ),
        "created_at": event_at_text,
        "paper_state_updated": True,
        "official_target_weights_generated": False,
        "portfolio_mutated": False,
        "real_portfolio_mutated": False,
        "order_ticket_generated": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_effect": "none",
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    event["event_checksum"] = _paper_action_event_checksum(event)
    return event


def _replay_paper_action_events(
    *,
    manifest: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
    config: Mapping[str, Any],
    validate_sources: bool,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[str]]:
    portfolio_id = _text(manifest.get("paper_portfolio_id"))
    weights = _normalize_weights(_mapping(manifest.get("initial_weights")))
    state = _paper_state_payload(
        paper_portfolio_id=portfolio_id,
        as_of=_text(manifest.get("initial_as_of")),
        base_currency=_text(manifest.get("initial_base_currency"), "USD"),
        source=_text(manifest.get("initial_source"), "manual_snapshot"),
        positions=weights,
        last_review_id="",
        last_action_id="",
        state_status="ACTIVE",
    )
    history = [_paper_initial_history_row(manifest=manifest, state=state)]
    errors: list[str] = []
    previous_checksum = "GENESIS"
    previous_event_at: datetime | None = None
    seen_action_ids: set[str] = set()
    seen_review_ids: set[str] = set()
    for index, raw_event in enumerate(events, start=1):
        event = dict(raw_event)
        action_id = _text(event.get("paper_action_id"))
        review_id = _text(event.get("review_id"))
        created_at = _text(event.get("created_at"))
        event_at = _parse_datetime_text(created_at)
        expected_action_id = _stable_id(
            "paper-portfolio-action",
            portfolio_id,
            index,
            review_id,
            created_at,
        )
        if event.get("ledger_schema_version") != PAPER_ACTION_LEDGER_SCHEMA_VERSION:
            errors.append(f"ledger_schema_invalid:{index}")
        if event.get("event_sequence") != index:
            errors.append(f"event_sequence_mismatch:{index}")
        if event.get("event_type") != PAPER_ACTION_EVENT_TYPE:
            errors.append(f"event_type_invalid:{index}")
        if event.get("paper_portfolio_id") != portfolio_id:
            errors.append(f"paper_portfolio_id_mismatch:{index}")
        if not review_id or review_id in seen_review_ids:
            errors.append(f"review_id_missing_or_duplicate:{index}")
        seen_review_ids.add(review_id)
        if action_id != expected_action_id or action_id in seen_action_ids:
            errors.append(f"paper_action_id_invalid_or_duplicate:{index}")
        seen_action_ids.add(action_id)
        if event.get("previous_event_checksum") != previous_checksum:
            errors.append(f"previous_event_checksum_mismatch:{index}")
        if event.get("event_checksum") != _paper_action_event_checksum(event):
            errors.append(f"event_checksum_mismatch:{index}")
        previous_checksum = _text(event.get("event_checksum"))
        if event_at is None or (previous_event_at is not None and event_at < previous_event_at):
            errors.append(f"event_time_invalid_or_reversed:{index}")
        if event_at is not None:
            previous_event_at = event_at
        review: dict[str, Any] = {}
        if validate_sources:
            try:
                owner_root = Path(_text(event.get("owner_review_root")))
                owner_validation = validate_owner_review_artifact(
                    review_id=review_id,
                    output_dir=owner_root,
                )
                if owner_validation.get("status") != "PASS":
                    raise DynamicV3PaperTrackingError("owner review validation did not PASS")
                review = _owner_review_record(review_id=review_id, output_dir=owner_root)
                daily_root = Path(_text(event.get("source_daily_advisory_root")))
                if not _paths_equal(
                    daily_root, Path(_text(review.get("source_daily_advisory_root")))
                ):
                    raise DynamicV3PaperTrackingError("daily advisory root binding mismatch")
                daily_validation = validate_position_advisory_daily_artifact(
                    daily_advisory_id=_text(review.get("daily_advisory_id")),
                    output_dir=daily_root,
                )
                if daily_validation.get("status") != "PASS":
                    raise DynamicV3PaperTrackingError("daily advisory validation did not PASS")
                review_paths = _mapping(review.get("source_artifact_paths"))
                review_checksums = _mapping(review.get("source_artifact_checksums"))
                if (
                    _mapping(event.get("source_artifact_paths")) != review_paths
                    or _mapping(event.get("source_artifact_checksums")) != review_checksums
                    or any(
                        not Path(_text(path)).is_file()
                        or review_checksums.get(key) != _file_sha256(Path(_text(path)))
                        for key, path in review_paths.items()
                    )
                ):
                    raise DynamicV3PaperTrackingError("daily source checksum binding mismatch")
                if (
                    event.get("owner_review_event_id") != review.get("last_event_id")
                    or event.get("owner_review_event_checksum")
                    != review.get("last_event_checksum")
                    or event.get("daily_advisory_id") != review.get("daily_advisory_id")
                    or event.get("owner_decision") != review.get("owner_decision")
                    or event.get("as_of") != review.get("as_of")
                    or event.get("notes") != review.get("manual_notes")
                    or event.get("owner_review_validation_status") != "PASS"
                    or event.get("source_daily_advisory_validation_status") != "PASS"
                ):
                    raise DynamicV3PaperTrackingError("owner review content binding mismatch")
                config_path = Path(_text(event.get("config_path")))
                if (
                    not _paths_equal(config_path, Path(_text(manifest.get("config_path"))))
                    or event.get("config_checksum") != _file_sha256(config_path)
                    or event.get("config_checksum") != manifest.get("config_checksum")
                    or event.get("config_policy_id")
                    != _mapping(config.get("policy_metadata")).get("policy_id")
                    or event.get("config_policy_version")
                    != _mapping(config.get("policy_metadata")).get("version")
                ):
                    raise DynamicV3PaperTrackingError("config source binding mismatch")
            except Exception as exc:  # noqa: BLE001
                errors.append(f"source_validation_failed:{index}:{exc}")
        owner_decision = _text(event.get("owner_decision"))
        action_type = _text(event.get("action_type"))
        expected_action_type = (
            "paper_adjustment"
            if owner_decision == "paper_adjustment"
            else "manual_adjustment"
            if owner_decision == "manual_adjustment"
            else "no_trade"
        )
        if owner_decision not in OWNER_REVIEW_DECISIONS or action_type != expected_action_type:
            errors.append(f"decision_action_type_mismatch:{index}")
        if event.get("reason") != owner_decision:
            errors.append(f"reason_mismatch:{index}")
        try:
            if owner_decision == "paper_adjustment":
                daily_root = Path(_text(event.get("source_daily_advisory_root")))
                expected_proposed = _paper_proposed_deltas(
                    daily_advisory_id=_text(event.get("daily_advisory_id")),
                    before_weights=weights,
                    daily_advisory_dir=daily_root,
                )
                if not expected_proposed:
                    raise DynamicV3PaperTrackingError(
                        "paper adjustment source-derived deltas are empty"
                    )
            elif owner_decision == "manual_adjustment":
                expected_proposed = _validated_manual_deltas(
                    _mapping(event.get("proposed_deltas"))
                )
            else:
                expected_proposed = {}
            expected_applied = _limit_paper_deltas(
                expected_proposed,
                config,
                before_weights=weights,
            )
            expected_after = _apply_weight_deltas(weights, expected_applied)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"event_recomputation_failed:{index}:{exc}")
            expected_proposed, expected_applied, expected_after = {}, {}, weights
        if not _weights_equal(_mapping(event.get("before_weights")), weights):
            errors.append(f"before_weights_mismatch:{index}")
        if _mapping(event.get("proposed_deltas")) != expected_proposed:
            errors.append(f"proposed_deltas_mismatch:{index}")
        if _mapping(event.get("applied_paper_deltas")) != expected_applied:
            errors.append(f"applied_deltas_mismatch:{index}")
        if not _weights_equal(_mapping(event.get("after_weights")), expected_after):
            errors.append(f"after_weights_mismatch:{index}")
        if event.get("manual_override") is not (owner_decision == "manual_adjustment"):
            errors.append(f"manual_override_mismatch:{index}")
        if not _paper_event_has_no_execution_effect(event):
            errors.append(f"execution_effect_forbidden:{index}")
        current_as_of = _date_from_any(state.get("as_of"))
        event_as_of = _date_from_any(event.get("as_of"))
        if event_as_of is None or (current_as_of is not None and event_as_of < current_as_of):
            errors.append(f"event_as_of_invalid_or_reversed:{index}")
        weights = expected_after
        state = _paper_state_payload(
            paper_portfolio_id=portfolio_id,
            as_of=_text(event.get("as_of"), _text(state.get("as_of"))),
            base_currency=_text(state.get("base_currency"), "USD"),
            source=_text(state.get("source"), "manual_snapshot"),
            positions=weights,
            last_review_id=review_id,
            last_action_id=action_id,
            state_status=(
                "NEEDS_REVIEW" if owner_decision == "needs_more_data" else "ACTIVE"
            ),
        )
        history.append(_paper_event_history_row(event=event, state=state))
    return state, history, sorted(set(errors))


def _paper_initial_history_row(
    *, manifest: Mapping[str, Any], state: Mapping[str, Any]
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "paper_portfolio_id": manifest.get("paper_portfolio_id"),
        "as_of": state.get("as_of"),
        "event_type": "init",
        "event_sequence": 0,
        "review_id": "",
        "paper_action_id": "",
        "source_snapshot_checksum": manifest.get("initial_snapshot_checksum"),
        "positions": state.get("positions"),
        "total_weight": state.get("total_weight"),
        "broker_action_taken": False,
        "created_at": manifest.get("generated_at"),
    }


def _paper_event_history_row(
    *, event: Mapping[str, Any], state: Mapping[str, Any]
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "paper_portfolio_id": event.get("paper_portfolio_id"),
        "as_of": state.get("as_of"),
        "event_type": event.get("action_type"),
        "event_sequence": event.get("event_sequence"),
        "review_id": event.get("review_id"),
        "paper_action_id": event.get("paper_action_id"),
        "event_checksum": event.get("event_checksum"),
        "positions": state.get("positions"),
        "total_weight": state.get("total_weight"),
        "broker_action_taken": False,
        "created_at": event.get("created_at"),
    }


def _paper_manifest_materialized_payload(
    *, manifest: Mapping[str, Any], events: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    payload = dict(manifest)
    latest = _mapping(events[-1]) if events else {}
    payload.update(
        {
            "status": "PASS",
            "ledger_schema_version": PAPER_ACTION_LEDGER_SCHEMA_VERSION,
            "event_chain_status": "PASS",
            "paper_action_count": len(events),
            "last_updated_at": latest.get("created_at", manifest.get("generated_at")),
            "last_review_id": latest.get("review_id", ""),
            "last_action_id": latest.get("paper_action_id", ""),
            "broker_action_allowed": False,
            "broker_action_taken": False,
            "owner_approval_required": True,
            "manual_review_required": True,
            "official_target_weights_generated": False,
            "portfolio_mutated": False,
            "order_ticket_generated": False,
            "production_effect": "none",
        }
    )
    return payload


def _materialize_paper_portfolio(
    *,
    portfolio_dir: Path,
    manifest: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    config = load_paper_portfolio_config(Path(_text(manifest.get("config_path"))))
    state, history, errors = _replay_paper_action_events(
        manifest=manifest,
        events=events,
        config=config,
        validate_sources=True,
    )
    if errors:
        raise DynamicV3PaperTrackingError(
            "paper portfolio event replay failed: " + ",".join(errors)
        )
    materialized_manifest = _paper_manifest_materialized_payload(
        manifest=manifest,
        events=events,
    )
    _write_json(portfolio_dir / "paper_portfolio_manifest.json", materialized_manifest)
    _write_json(portfolio_dir / "paper_portfolio_state.json", state)
    _write_jsonl(portfolio_dir / "paper_position_history.jsonl", history)
    _write_text(
        portfolio_dir / "paper_portfolio_report.md",
        render_paper_portfolio_report(materialized_manifest, state, events),
    )
    return state


def _paper_event_has_no_execution_effect(event: Mapping[str, Any]) -> bool:
    return (
        event.get("official_target_weights_generated") is False
        and event.get("portfolio_mutated") is False
        and event.get("real_portfolio_mutated") is False
        and event.get("order_ticket_generated") is False
        and event.get("broker_action_allowed") is False
        and event.get("broker_action_taken") is False
        and event.get("production_effect") == "none"
    )


def _outcome_daily_source_paths(
    *, daily_advisory_id: str, daily_advisory_dir: Path
) -> dict[str, str]:
    source_dir = daily_advisory_dir / daily_advisory_id
    return {
        "daily_advisory_manifest": str(source_dir / "daily_advisory_manifest.json"),
        "daily_candidate_targets": str(source_dir / "daily_candidate_targets.jsonl"),
        "daily_consensus_weights": str(source_dir / "daily_consensus_weights.csv"),
        "daily_position_deltas": str(source_dir / "daily_position_deltas.jsonl"),
        "daily_advisory_actions": str(source_dir / "daily_advisory_actions.json"),
        "daily_position_advisory_report": str(
            source_dir / "daily_position_advisory_report.md"
        ),
        "daily_reader_brief": str(source_dir / "reader_brief_section.md"),
    }


def _validated_outcome_daily_source(
    *, daily_advisory_id: str, daily_advisory_dir: Path
) -> dict[str, Any]:
    validation = validate_position_advisory_daily_artifact(
        daily_advisory_id=daily_advisory_id,
        output_dir=daily_advisory_dir,
    )
    if validation.get("status") != "PASS":
        raise DynamicV3PaperTrackingError(
            "daily advisory validation must PASS before outcome tracking"
        )
    source_dir = daily_advisory_dir / daily_advisory_id
    manifest = _read_json(source_dir / "daily_advisory_manifest.json")
    actions = _read_json(source_dir / "daily_advisory_actions.json")
    if (
        manifest.get("daily_advisory_id") != daily_advisory_id
        or actions.get("daily_advisory_id") != daily_advisory_id
    ):
        raise DynamicV3PaperTrackingError("daily advisory ids must match tracking request")
    paths = _outcome_daily_source_paths(
        daily_advisory_id=daily_advisory_id,
        daily_advisory_dir=daily_advisory_dir,
    )
    checksums = {key: _file_sha256(Path(path)) for key, path in paths.items()}
    return {
        "manifest": manifest,
        "actions": actions,
        "source_paths": paths,
        "source_checksums": checksums,
    }


def _outcome_baseline_source() -> dict[str, Any]:
    paths = {
        "assets": str(DEFAULT_ETF_ASSETS_CONFIG_PATH),
        "strategy": str(DEFAULT_ETF_STRATEGY_CONFIG_PATH),
        "risk": str(DEFAULT_ETF_RISK_CONFIG_PATH),
        "backtest": str(DEFAULT_ETF_BACKTEST_CONFIG_PATH),
        "p1": str(DEFAULT_ETF_P1_CONFIG_PATH),
        "p2": str(DEFAULT_ETF_P2_CONFIG_PATH),
    }
    bundle = load_etf_config_bundle()
    return {
        "source_paths": paths,
        "source_checksums": {
            key: _file_sha256(Path(path)) for key, path in paths.items()
        },
        "config_hash": bundle.config_hash,
    }


def _validated_outcome_paper_source(output_dir: Path) -> dict[str, Any]:
    portfolio_dir = _paper_portfolio_dir(paper_portfolio_id=None, output_dir=output_dir)
    validation = validate_paper_portfolio_artifact(
        paper_portfolio_id=portfolio_dir.name,
        output_dir=output_dir,
    )
    if validation.get("status") != "PASS":
        raise DynamicV3PaperTrackingError(
            "paper portfolio validation must PASS before outcome tracking"
        )
    manifest = _read_json(portfolio_dir / "paper_portfolio_manifest.json")
    state = _read_json(portfolio_dir / "paper_portfolio_state.json")
    events = _read_jsonl(portfolio_dir / "paper_action_ledger.jsonl")
    return {
        "manifest": manifest,
        "state": state,
        "events": events,
        "binding": {
            "paper_portfolio_root": str(output_dir),
            "paper_portfolio_id": portfolio_dir.name,
            "initial_snapshot_path": manifest.get("initial_snapshot_path"),
            "initial_snapshot_checksum": manifest.get("initial_snapshot_checksum"),
            "config_path": manifest.get("config_path"),
            "config_checksum": manifest.get("config_checksum"),
            "event_count_at_track": len(events),
            "last_event_checksum_at_track": (
                _text(events[-1].get("event_checksum")) if events else "GENESIS"
            ),
            "last_action_id_at_track": state.get("last_action_id", ""),
            "validation_status": "PASS",
        },
    }


def _validated_outcome_weights(
    weights: Mapping[str, Any], label: str
) -> dict[str, float]:
    if not weights:
        raise DynamicV3PaperTrackingError(f"{label} cannot be empty")
    clean: dict[str, float] = {}
    for symbol, value in weights.items():
        clean_symbol = _text(symbol)
        if not clean_symbol:
            raise DynamicV3PaperTrackingError(f"{label} contains an empty symbol")
        number = _strict_finite_number(value, f"{label}:{clean_symbol}")
        if number < 0 or number > 1:
            raise DynamicV3PaperTrackingError(f"{label} values must be within [0,1]")
        if number:
            clean[clean_symbol] = number
    if abs(sum(clean.values()) - 1.0) > PAPER_WEIGHT_TOLERANCE:
        raise DynamicV3PaperTrackingError(f"{label} must sum to one")
    return _normalize_weights(clean)


def _freeze_outcome_track_sources(
    *,
    outcome_dir: Path,
    outcome_config_path: Path,
    daily_source: Mapping[str, Any],
    baseline_source: Mapping[str, Any],
    paper_source: Mapping[str, Any],
) -> dict[str, Any]:
    root = outcome_dir / "source_snapshots" / "TRACK"

    def freeze_text(source: Path, target: Path) -> dict[str, str]:
        write_text_atomic(target, source.read_text(encoding="utf-8"))
        return {"path": str(target), "checksum": _file_sha256(target)}

    frozen_daily = {
        key: freeze_text(Path(_text(path)), root / "daily" / Path(_text(path)).name)
        for key, path in _mapping(daily_source.get("source_paths")).items()
    }
    frozen_baseline = {
        key: freeze_text(Path(_text(path)), root / "baseline" / Path(_text(path)).name)
        for key, path in _mapping(baseline_source.get("source_paths")).items()
    }
    paper_manifest_path = root / "paper" / "paper_portfolio_manifest.json"
    paper_state_path = root / "paper" / "paper_portfolio_state.json"
    paper_events_path = root / "paper" / "paper_action_event_prefix.jsonl"
    _write_json(paper_manifest_path, _mapping(paper_source.get("manifest")))
    _write_json(paper_state_path, _mapping(paper_source.get("state")))
    _write_jsonl(paper_events_path, _records(paper_source.get("events")))
    return {
        "outcome_config": freeze_text(
            outcome_config_path,
            root / "outcome_config" / outcome_config_path.name,
        ),
        "daily": frozen_daily,
        "baseline": frozen_baseline,
        "paper": {
            "manifest": {
                "path": str(paper_manifest_path),
                "checksum": _file_sha256(paper_manifest_path),
            },
            "state": {
                "path": str(paper_state_path),
                "checksum": _file_sha256(paper_state_path),
            },
            "event_prefix": {
                "path": str(paper_events_path),
                "checksum": _file_sha256(paper_events_path),
            },
        },
    }


def _validate_frozen_outcome_track_sources(advisory_event: Mapping[str, Any]) -> None:
    frozen = _mapping(advisory_event.get("frozen_track_sources"))
    entries = [
        _mapping(frozen.get("outcome_config")),
        *[_mapping(value) for value in _mapping(frozen.get("daily")).values()],
        *[_mapping(value) for value in _mapping(frozen.get("baseline")).values()],
        *[_mapping(value) for value in _mapping(frozen.get("paper")).values()],
    ]
    if not entries or any(
        not Path(_text(entry.get("path"))).is_file()
        or entry.get("checksum") != _file_sha256(Path(_text(entry.get("path"))))
        for entry in entries
    ):
        raise DynamicV3PaperTrackingError("frozen outcome tracking source checksum mismatch")


def _validate_outcome_initial_context(
    advisory_event: Mapping[str, Any],
) -> dict[str, Any]:
    _validate_frozen_outcome_track_sources(advisory_event)
    if advisory_event.get("event_checksum") != _advisory_event_checksum(advisory_event):
        raise DynamicV3PaperTrackingError("advisory event checksum mismatch")
    config_path = Path(_text(advisory_event.get("outcome_config_path")))
    config = load_paper_portfolio_config(config_path)
    policy = _mapping(config.get("policy_metadata"))
    if (
        advisory_event.get("outcome_config_checksum") != _file_sha256(config_path)
        or advisory_event.get("outcome_config_policy_id") != policy.get("policy_id")
        or advisory_event.get("outcome_config_policy_version") != policy.get("version")
    ):
        raise DynamicV3PaperTrackingError("outcome config source binding mismatch")
    daily_id = _text(advisory_event.get("daily_advisory_id"))
    daily_root = Path(_text(advisory_event.get("source_daily_advisory_root")))
    daily = _validated_outcome_daily_source(
        daily_advisory_id=daily_id,
        daily_advisory_dir=daily_root,
    )
    if (
        _mapping(advisory_event.get("source_artifact_paths")) != daily["source_paths"]
        or _mapping(advisory_event.get("source_artifact_checksums"))
        != daily["source_checksums"]
        or advisory_event.get("as_of") != daily["manifest"].get("as_of")
        or advisory_event.get("recommended_action")
        != daily["actions"].get("recommended_action")
        or advisory_event.get("mode")
        != _text(daily["actions"].get("mode"), _text(daily["manifest"].get("mode")))
    ):
        raise DynamicV3PaperTrackingError("daily advisory source binding mismatch")
    baseline = _outcome_baseline_source()
    if (
        _mapping(advisory_event.get("baseline_source_paths")) != baseline["source_paths"]
        or _mapping(advisory_event.get("baseline_source_checksums"))
        != baseline["source_checksums"]
        or advisory_event.get("baseline_config_hash") != baseline["config_hash"]
    ):
        raise DynamicV3PaperTrackingError("baseline config source binding mismatch")
    paper_source = _mapping(advisory_event.get("paper_portfolio_source"))
    paper_root = Path(_text(paper_source.get("paper_portfolio_root")))
    paper_id = _text(paper_source.get("paper_portfolio_id"))
    paper_validation = validate_paper_portfolio_artifact(
        paper_portfolio_id=paper_id,
        output_dir=paper_root,
    )
    if paper_validation.get("status") != "PASS":
        raise DynamicV3PaperTrackingError("paper portfolio source validation did not PASS")
    frozen_paper = _mapping(
        _mapping(advisory_event.get("frozen_track_sources")).get("paper")
    )
    frozen_manifest = _read_json(
        Path(_text(_mapping(frozen_paper.get("manifest")).get("path")))
    )
    frozen_state = _read_json(
        Path(_text(_mapping(frozen_paper.get("state")).get("path")))
    )
    frozen_events = _read_jsonl(
        Path(_text(_mapping(frozen_paper.get("event_prefix")).get("path")))
    )
    if (
        len(frozen_events) != int(paper_source.get("event_count_at_track") or 0)
        or paper_source.get("last_event_checksum_at_track")
        != (_text(frozen_events[-1].get("event_checksum")) if frozen_events else "GENESIS")
        or paper_source.get("initial_snapshot_checksum")
        != frozen_manifest.get("initial_snapshot_checksum")
        or paper_source.get("config_checksum") != frozen_manifest.get("config_checksum")
    ):
        raise DynamicV3PaperTrackingError("frozen paper source lineage mismatch")
    frozen_config = load_paper_portfolio_config(
        Path(_text(frozen_manifest.get("config_path")))
    )
    replay_state, _history, replay_errors = _replay_paper_action_events(
        manifest=frozen_manifest,
        events=frozen_events,
        config=frozen_config,
        validate_sources=False,
    )
    if replay_errors or replay_state != frozen_state:
        raise DynamicV3PaperTrackingError(
            "frozen paper source state does not match event prefix"
        )
    advisory_dir = daily_root / daily_id
    expected_no_trade = _advisory_current_weights(advisory_dir)
    expected_source = "daily_advisory_current_weights"
    if not expected_no_trade:
        expected_no_trade = _mapping(frozen_state.get("positions"))
        expected_source = "selected_paper_portfolio_state"
    expected_no_trade = _validated_outcome_weights(expected_no_trade, "no_trade_weights")
    expected_target = _validated_outcome_weights(
        _advisory_consensus_weights(advisory_dir), "target_weights"
    )
    expected_baseline = _validated_outcome_weights(
        _baseline_weights(), "baseline_weights"
    )
    proposed = {
        symbol: round(
            _float(expected_target.get(symbol)) - _float(expected_no_trade.get(symbol)), 6
        )
        for symbol in sorted(set(expected_target) | set(expected_no_trade))
        if round(
            _float(expected_target.get(symbol)) - _float(expected_no_trade.get(symbol)), 6
        )
    }
    expected_limited_deltas = _limit_paper_deltas(
        proposed,
        config,
        before_weights=expected_no_trade,
    )
    expected_limited = _apply_weight_deltas(expected_no_trade, expected_limited_deltas)
    if (
        advisory_event.get("no_trade_source") != expected_source
        or _mapping(advisory_event.get("no_trade_weights")) != expected_no_trade
        or _mapping(advisory_event.get("target_weights")) != expected_target
        or _mapping(advisory_event.get("baseline_weights")) != expected_baseline
        or _mapping(advisory_event.get("limited_adjustment_deltas"))
        != expected_limited_deltas
        or _mapping(advisory_event.get("limited_adjustment_weights"))
        != expected_limited
        or advisory_event.get("paper_action_status") != "PENDING"
        or _mapping(advisory_event.get("paper_action_weights"))
    ):
        raise DynamicV3PaperTrackingError("advisory event derived weights mismatch")
    if not _outcome_record_has_no_execution_effect(advisory_event):
        raise DynamicV3PaperTrackingError("advisory event execution effect is forbidden")
    return config


def _outcome_record_has_no_execution_effect(record: Mapping[str, Any]) -> bool:
    return (
        record.get("official_target_weights_generated") is False
        and record.get("portfolio_mutated") is False
        and record.get("order_ticket_generated") is False
        and record.get("broker_action_allowed") is False
        and record.get("broker_action_taken") is False
        and record.get("production_effect") == "none"
    )


def _replay_outcome_update_events(
    *,
    advisory_event: Mapping[str, Any],
    config: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
) -> list[str]:
    errors: list[str] = []
    previous_checksum = "GENESIS"
    previous_as_of: date | None = None
    previous_event_at: datetime | None = None
    previous_rows = [
        _pending_outcome_window(
            daily_advisory_id=_text(advisory_event.get("daily_advisory_id")),
            window_days=window,
            start_date=_text(advisory_event.get("as_of")),
        )
        for window in _configured_outcome_windows(config)
    ]
    paper_root = Path(
        _text(
            _mapping(advisory_event.get("paper_portfolio_source")).get(
                "paper_portfolio_root"
            )
        )
    )
    for index, raw in enumerate(events, start=1):
        event = dict(raw)
        updated_as_of = _date_from_any(event.get("updated_as_of"))
        event_at = _parse_datetime_text(_text(event.get("event_at")))
        expected_id = _stable_id(
            "advisory-outcome-update",
            advisory_event.get("outcome_id"),
            index,
            _text(event.get("updated_as_of")),
            _text(event.get("event_at")),
        )
        if event.get("ledger_schema_version") != OUTCOME_UPDATE_LEDGER_SCHEMA_VERSION:
            errors.append(f"ledger_schema_invalid:{index}")
        if event.get("event_type") != OUTCOME_UPDATE_EVENT_TYPE:
            errors.append(f"event_type_invalid:{index}")
        if event.get("event_sequence") != index:
            errors.append(f"event_sequence_mismatch:{index}")
        if event.get("update_event_id") != expected_id:
            errors.append(f"update_event_id_mismatch:{index}")
        if event.get("outcome_id") != advisory_event.get("outcome_id"):
            errors.append(f"outcome_id_mismatch:{index}")
        if event.get("daily_advisory_id") != advisory_event.get("daily_advisory_id"):
            errors.append(f"daily_advisory_id_mismatch:{index}")
        if event.get("previous_event_checksum") != previous_checksum:
            errors.append(f"previous_event_checksum_mismatch:{index}")
        if event.get("event_checksum") != _outcome_update_event_checksum(event):
            errors.append(f"event_checksum_mismatch:{index}")
        previous_checksum = _text(event.get("event_checksum"))
        if (
            updated_as_of is None
            or (previous_as_of is not None and updated_as_of < previous_as_of)
            or event_at is None
            or (previous_event_at is not None and event_at < previous_event_at)
        ):
            errors.append(f"update_time_or_as_of_reversed:{index}")
        if updated_as_of is None or event_at is None:
            continue
        previous_as_of = updated_as_of
        previous_event_at = event_at
        try:
            expected_binding = _paper_action_binding_for_outcome(
                advisory_event=advisory_event,
                paper_portfolio_dir=paper_root,
                updated_as_of=updated_as_of,
                known_at=event_at,
            )
            data_gate_ran = updated_as_of <= event_at.date()
            if data_gate_ran:
                snapshot_path = Path(_text(event.get("price_snapshot_path")))
                quality_path = Path(_text(event.get("data_quality_report_path")))
                if (
                    not snapshot_path.is_file()
                    or event.get("price_snapshot_checksum") != _file_sha256(snapshot_path)
                    or not quality_path.is_file()
                    or event.get("data_quality_report_checksum") != _file_sha256(quality_path)
                    or event.get("data_quality_status") not in {"PASS", "PASS_WITH_WARNINGS"}
                ):
                    raise DynamicV3PaperTrackingError(
                        "update price snapshot or data quality evidence mismatch"
                    )
                snapshot = _read_outcome_price_snapshot(snapshot_path)
            else:
                if any(
                    _text(event.get(field))
                    for field in (
                        "price_snapshot_path",
                        "price_snapshot_checksum",
                        "data_quality_report_path",
                        "data_quality_report_checksum",
                        "prices_source_path",
                        "prices_source_checksum",
                        "rates_source_path",
                        "rates_source_checksum",
                    )
                ) or event.get("data_quality_status") != "NOT_RUN_FUTURE_AS_OF":
                    raise DynamicV3PaperTrackingError(
                        "future-as-of update must not claim data quality evidence"
                    )
                snapshot = None
            computed_rows = _compute_outcome_window_rows(
                advisory_event=advisory_event,
                config=config,
                paper_binding=expected_binding,
                prices=snapshot,
                updated_as_of=updated_as_of,
                data_gate_ran=data_gate_ran,
            )
            configured_windows = {
                int(row.get("window_days") or 0) for row in computed_rows
            }
            raw_allowed = event.get("allowed_window_days")
            allowed = (
                {int(value) for value in raw_allowed}
                if isinstance(raw_allowed, list)
                else configured_windows
            )
            if not allowed or not allowed <= configured_windows:
                raise DynamicV3PaperTrackingError(
                    "update allowed windows are not a configured subset"
                )
            computed_by_window = {
                int(row.get("window_days") or 0): row for row in computed_rows
            }
            previous_by_window = {
                int(row.get("window_days") or 0): row for row in previous_rows
            }
            expected_rows = [
                computed_by_window[window]
                if window in allowed
                else previous_by_window[window]
                for window in sorted(configured_windows)
            ]
            if _mapping(event.get("paper_action_binding")) != expected_binding:
                errors.append(f"paper_action_binding_mismatch:{index}")
            if _records(event.get("outcome_windows")) != expected_rows:
                errors.append(f"outcome_windows_content_mismatch:{index}")
            if event.get("status") != _rollup_outcome_status(expected_rows):
                errors.append(f"rollup_status_mismatch:{index}")
            previous_rows = expected_rows
        except Exception as exc:  # noqa: BLE001
            errors.append(f"update_recomputation_failed:{index}:{exc}")
        if not _outcome_record_has_no_execution_effect(event):
            errors.append(f"execution_effect_forbidden:{index}")
    return sorted(set(errors))


def _ensure_no_duplicate_outcome_tracker(
    *, daily_advisory_id: str, output_dir: Path
) -> None:
    if not output_dir.exists():
        return
    for path in sorted(output_dir.glob("*/advisory_event.json")):
        try:
            payload = _read_json(path)
        except Exception as exc:  # noqa: BLE001
            raise DynamicV3PaperTrackingError(
                f"existing advisory outcome event is unreadable: {path}: {exc}"
            ) from exc
        if payload.get("daily_advisory_id") == daily_advisory_id:
            raise DynamicV3PaperTrackingError(
                f"advisory outcome already tracked for daily advisory: {daily_advisory_id}"
            )


def _paper_action_binding_for_outcome(
    *,
    advisory_event: Mapping[str, Any],
    paper_portfolio_dir: Path,
    updated_as_of: date,
    known_at: datetime,
) -> dict[str, Any]:
    source = _mapping(advisory_event.get("paper_portfolio_source"))
    portfolio_id = _text(source.get("paper_portfolio_id"))
    if not portfolio_id:
        raise DynamicV3PaperTrackingError("outcome paper portfolio source id is missing")
    validation = validate_paper_portfolio_artifact(
        paper_portfolio_id=portfolio_id,
        output_dir=paper_portfolio_dir,
    )
    if validation.get("status") != "PASS":
        raise DynamicV3PaperTrackingError(
            "selected paper portfolio validation must PASS for outcome update"
        )
    portfolio_dir = paper_portfolio_dir / portfolio_id
    manifest = _read_json(portfolio_dir / "paper_portfolio_manifest.json")
    events = _read_jsonl(portfolio_dir / "paper_action_ledger.jsonl")
    prefix_count = int(source.get("event_count_at_track") or 0)
    prefix = events[:prefix_count]
    if (
        source.get("initial_snapshot_checksum") != manifest.get("initial_snapshot_checksum")
        or source.get("config_checksum") != manifest.get("config_checksum")
        or len(events) < prefix_count
        or source.get("last_event_checksum_at_track")
        != (_text(prefix[-1].get("event_checksum")) if prefix else "GENESIS")
    ):
        raise DynamicV3PaperTrackingError(
            "selected paper portfolio lineage changed after outcome tracking"
        )
    daily_advisory_id = _text(advisory_event.get("daily_advisory_id"))
    matching = [row for row in events if row.get("daily_advisory_id") == daily_advisory_id]
    eligible = []
    future_count = 0
    for row in matching:
        created = _parse_datetime_text(_text(row.get("created_at")))
        if created is None:
            raise DynamicV3PaperTrackingError("paper action created_at is invalid")
        if created <= known_at and created.date() <= updated_as_of:
            eligible.append(row)
        else:
            future_count += 1
    if len(eligible) > 1:
        raise DynamicV3PaperTrackingError(
            "multiple eligible paper actions found for one daily advisory"
        )
    if not eligible:
        return {
            "status": "NOT_RECORDED_BY_AS_OF",
            "paper_portfolio_id": portfolio_id,
            "daily_advisory_id": daily_advisory_id,
            "paper_action_id": "",
            "paper_action_event_checksum": "",
            "action_type": "",
            "created_at": "",
            "after_weights": {},
            "applied_paper_deltas": {},
            "excluded_future_action_count": future_count,
        }
    row = eligible[0]
    after = _validated_outcome_weights(
        _mapping(row.get("after_weights")), "paper_action_after_weights"
    )
    return {
        "status": (
            "AVAILABLE"
            if row.get("action_type") in {"paper_adjustment", "manual_adjustment"}
            else "NO_STATE_CHANGE"
        ),
        "paper_portfolio_id": portfolio_id,
        "daily_advisory_id": daily_advisory_id,
        "paper_action_id": row.get("paper_action_id"),
        "paper_action_event_checksum": row.get("event_checksum"),
        "action_type": row.get("action_type"),
        "created_at": row.get("created_at"),
        "after_weights": after,
        "applied_paper_deltas": _mapping(row.get("applied_paper_deltas")),
        "excluded_future_action_count": future_count,
    }


def _outcome_required_symbols(
    advisory_event: Mapping[str, Any], paper_binding: Mapping[str, Any]
) -> set[str]:
    weights = [
        _mapping(advisory_event.get("no_trade_weights")),
        _mapping(advisory_event.get("baseline_weights")),
        _mapping(advisory_event.get("target_weights")),
        _mapping(advisory_event.get("limited_adjustment_weights")),
        _mapping(paper_binding.get("after_weights")),
    ]
    return {symbol for row in weights for symbol in row if symbol != "CASH"}


def _outcome_price_snapshot(
    *,
    prices: pd.DataFrame,
    advisory_event: Mapping[str, Any],
    paper_binding: Mapping[str, Any],
    start: date,
    as_of: date,
) -> pd.DataFrame:
    symbols = _outcome_required_symbols(advisory_event, paper_binding)
    earliest = start - timedelta(days=31)
    snapshot = prices.loc[
        prices["symbol"].isin(symbols)
        & prices["_date"].map(lambda value: earliest <= value <= as_of),
        ["_date", "symbol", "_adj_close"],
    ].copy()
    if snapshot.empty:
        raise DynamicV3PaperTrackingError("required-symbol outcome price snapshot is empty")
    return snapshot.sort_values(["_date", "symbol"]).reset_index(drop=True)


def _write_outcome_price_snapshot(path: Path, prices: pd.DataFrame) -> None:
    export = prices.rename(columns={"_date": "date", "_adj_close": "adj_close"}).copy()
    export["date"] = export["date"].map(lambda value: value.isoformat())
    text = export[["date", "symbol", "adj_close"]].to_csv(
        index=False, lineterminator="\n"
    )
    write_text_atomic(path, text)


def _read_outcome_price_snapshot(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    required = {"date", "symbol", "adj_close"}
    if set(frame.columns) != required:
        raise DynamicV3PaperTrackingError("outcome price snapshot schema is invalid")
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
    frame["_adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    return frame[["_date", "symbol", "_adj_close"]]


def _outcome_update_event_checksum(event: Mapping[str, Any]) -> str:
    payload = dict(event)
    payload.pop("event_checksum", None)
    return sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _outcome_counterfactuals_payload() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_advisory_counterfactuals",
        "counterfactuals": [
            {"name": "no_trade", "description": "Keep current portfolio snapshot unchanged"},
            {"name": "paper_action", "description": "Apply owner-approved paper action"},
            {
                "name": "candidate_consensus_target",
                "description": "Move fully to consensus target weights",
            },
            {"name": "limited_adjustment", "description": "Apply capped advisory deltas"},
        ],
        "calculation_policy": {
            "static_path": "fixed_share_from_start",
            "paper_path": "no_trade_then_rebalance_on_first_complete_date_after_action",
            "transaction_cost": "one_time_turnover_x_commission_plus_slippage_bps",
            "cash_return": "zero",
        },
        "broker_action_allowed": False,
        "broker_action_taken": False,
    }


def _advisory_event_checksum(event: Mapping[str, Any]) -> str:
    payload = dict(event)
    payload.pop("event_checksum", None)
    return sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _outcome_manifest_materialized_payload(
    *, manifest: Mapping[str, Any], update_events: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    payload = dict(manifest)
    latest = _mapping(update_events[-1]) if update_events else {}
    rows = _records(latest.get("outcome_windows")) if latest else []
    payload.update(
        {
            "status": latest.get("status", "PENDING"),
            "update_ledger_schema_version": OUTCOME_UPDATE_LEDGER_SCHEMA_VERSION,
            "update_chain_status": "PASS",
            "update_event_count": len(update_events),
            "latest_update_event_id": latest.get("update_event_id", ""),
            "latest_update_event_checksum": latest.get("event_checksum", ""),
            "updated_at": latest.get("event_at", ""),
            "updated_as_of": latest.get("updated_as_of", ""),
            "data_quality_status": latest.get(
                "data_quality_status", "NOT_RUN_PENDING_ONLY"
            ),
            "data_quality_report_path": latest.get("data_quality_report_path", ""),
            "paper_action_status": _mapping(latest.get("paper_action_binding")).get(
                "status", payload.get("paper_action_status", "PENDING")
            ),
            "available_window_count": (
                sum(row.get("outcome_status") == "AVAILABLE" for row in rows)
                if latest
                else payload.get("available_window_count", 0)
            ),
            "pending_window_count": (
                sum(row.get("outcome_status") == "PENDING" for row in rows)
                if latest
                else payload.get("pending_window_count", 0)
            ),
            "insufficient_window_count": (
                sum(row.get("outcome_status") == "INSUFFICIENT_DATA" for row in rows)
                if latest
                else payload.get("insufficient_window_count", 0)
            ),
            "broker_action_allowed": False,
            "broker_action_taken": False,
            "owner_approval_required": True,
            "manual_review_required": True,
            "official_target_weights_generated": False,
            "portfolio_mutated": False,
            "order_ticket_generated": False,
            "production_effect": "none",
        }
    )
    return payload


def _materialize_advisory_outcome(
    *,
    outcome_dir: Path,
    manifest: Mapping[str, Any],
    advisory_event: Mapping[str, Any],
    update_events: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if not update_events:
        raise DynamicV3PaperTrackingError("outcome materialization requires an update event")
    latest = update_events[-1]
    rows = _records(latest.get("outcome_windows"))
    materialized_manifest = _outcome_manifest_materialized_payload(
        manifest=manifest,
        update_events=update_events,
    )
    paper_binding = _mapping(latest.get("paper_action_binding"))
    event_view = {
        **dict(advisory_event),
        "paper_action_status": paper_binding.get("status"),
        "paper_action_weights": paper_binding.get("after_weights", {}),
    }
    _write_json(outcome_dir / "advisory_outcome_manifest.json", materialized_manifest)
    _write_jsonl(outcome_dir / "outcome_windows.jsonl", rows)
    _write_text(
        outcome_dir / "advisory_outcome_report.md",
        render_advisory_outcome_report(materialized_manifest, event_view, rows),
    )
    return materialized_manifest


def _advisory_current_weights(advisory_dir: Path) -> dict[str, float]:
    for row in _read_jsonl(advisory_dir / "daily_position_deltas.jsonl"):
        current = _mapping(row.get("current_weights"))
        if current:
            return _normalize_weights(current)
    return {}


def _advisory_consensus_weights(advisory_dir: Path) -> dict[str, float]:
    path = advisory_dir / "daily_consensus_weights.csv"
    if not path.exists():
        return {}
    rows = _read_csv(path)
    weights = {}
    for row in rows:
        symbol = _text(row.get("symbol"))
        if not symbol:
            continue
        value = row.get("median_target_weight", row.get("mean_target_weight"))
        weights[symbol] = _float(value)
    return _normalize_weights(weights) if weights else {}


def _latest_paper_weights(output_dir: Path) -> dict[str, float]:
    try:
        portfolio_dir = _paper_portfolio_dir(paper_portfolio_id=None, output_dir=output_dir)
    except DynamicV3PaperTrackingError:
        return {}
    state = _read_optional_json(portfolio_dir / "paper_portfolio_state.json") or {}
    return _normalize_weights(_mapping(state.get("positions")))


def _paper_action_weights_for_advisory(
    *,
    daily_advisory_id: str,
    fallback_weights: Mapping[str, Any],
    paper_portfolio_dir: Path,
) -> dict[str, float]:
    candidates = []
    for portfolio_dir in paper_portfolio_dir.glob("*"):
        if not portfolio_dir.is_dir():
            continue
        for row in _read_jsonl(portfolio_dir / "paper_action_ledger.jsonl"):
            if _text(row.get("daily_advisory_id")) == daily_advisory_id:
                candidates.append((_date_from_any(row.get("created_at")) or date.min, row))
    if not candidates:
        return _normalize_weights(fallback_weights)
    row = sorted(candidates, key=lambda item: str(item[0]))[-1][1]
    return _normalize_weights(_mapping(row.get("after_weights")))


def _baseline_weights() -> dict[str, float]:
    config = load_etf_config_bundle()
    return _normalize_weights(
        {
            symbol: asset.default_weight
            for symbol, asset in config.assets.assets.items()
            if asset.default_weight > 0
        }
    )


def _configured_outcome_windows(config: Mapping[str, Any]) -> list[int]:
    windows = _mapping(config.get("outcome_tracking")).get("windows_trading_days", [1, 5, 10, 20])
    result = [int(value) for value in windows if int(value) > 0]
    return result or [1, 5, 10, 20]


def _pending_outcome_window(
    *, daily_advisory_id: str, window_days: int, start_date: str
) -> dict[str, Any]:
    return {
        "daily_advisory_id": daily_advisory_id,
        "window_days": window_days,
        "start_date": start_date,
        "end_date": "",
        **{field: None for field in OUTCOME_METRIC_FIELDS},
        "paper_action_status": "PENDING",
        "paper_action_effective_date": "",
        "outcome_status": "PENDING",
        "insufficient_reason": "WINDOW_NOT_EVALUATED",
    }


def _run_cached_data_quality_gate(
    *,
    as_of: date,
    prices_path: Path,
    rates_path: Path,
    report_path: Path,
) -> Any:
    universe = load_universe()
    quality = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(universe),
        expected_rate_series=configured_rate_series(universe),
        quality_config=load_data_quality(),
        as_of=as_of,
        manifest_path=_download_manifest_path(prices_path),
        secondary_prices_path=_marketstack_prices_path(prices_path),
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    write_data_quality_report(quality, report_path)
    return quality


def _load_outcome_prices(
    prices_path: Path,
    event: Mapping[str, Any],
    paper_binding: Mapping[str, Any],
) -> pd.DataFrame:
    symbols = _outcome_required_symbols(event, paper_binding)
    config = load_etf_config_bundle()
    prices, quality = load_standard_prices(
        prices_path,
        config.assets,
        config.strategy,
        extra_symbols=symbols,
    )
    if not quality.passed:
        raise DynamicV3PaperTrackingError(f"ETF price validation failed: {quality.status}")
    prices = prices.copy()
    prices["_date"] = pd.to_datetime(prices["date"], errors="coerce").dt.date
    prices["_adj_close"] = pd.to_numeric(prices["adj_close"], errors="coerce")
    return prices.dropna(subset=["_date", "_adj_close"])


def _complete_price_dates(prices: pd.DataFrame, symbols: set[str]) -> list[date]:
    if prices.duplicated(subset=["_date", "symbol"]).any():
        raise DynamicV3PaperTrackingError("outcome price snapshot contains duplicate keys")
    complete = []
    for date_value, group in prices.groupby("_date", dropna=True):
        values = {
            _text(row["symbol"]): row["_adj_close"]
            for _, row in group.iterrows()
        }
        if symbols <= set(values) and all(
            _is_finite_number(values[symbol]) and _float(values[symbol]) > 0
            for symbol in symbols
        ):
            complete.append(date_value)
    return sorted(complete)


def _nth_trading_date_after(dates: Sequence[date], start: date, n: int) -> date | None:
    after = [item for item in dates if item > start]
    return after[n - 1] if len(after) >= n else None


def _compute_outcome_window_rows(
    *,
    advisory_event: Mapping[str, Any],
    config: Mapping[str, Any],
    paper_binding: Mapping[str, Any],
    prices: pd.DataFrame | None,
    updated_as_of: date,
    data_gate_ran: bool,
) -> list[dict[str, Any]]:
    start = _date_from_any(advisory_event.get("as_of"))
    if start is None:
        raise DynamicV3PaperTrackingError("advisory event as_of is invalid")
    windows = _configured_outcome_windows(config)
    if not data_gate_ran:
        return [
            {
                **_pending_outcome_window(
                    daily_advisory_id=_text(advisory_event.get("daily_advisory_id")),
                    window_days=window,
                    start_date=start.isoformat(),
                ),
                "paper_action_status": paper_binding.get("status"),
                "insufficient_reason": "AS_OF_AFTER_GENERATED_DATE_DATA_GATE_NOT_RUN",
            }
            for window in windows
        ]
    if prices is None:
        raise DynamicV3PaperTrackingError("price snapshot is required after data gate")
    symbols = _outcome_required_symbols(advisory_event, paper_binding)
    complete_dates = _complete_price_dates(prices, symbols)
    rows: list[dict[str, Any]] = []
    for window in windows:
        base = _pending_outcome_window(
            daily_advisory_id=_text(advisory_event.get("daily_advisory_id")),
            window_days=window,
            start_date=start.isoformat(),
        )
        end = _nth_trading_date_after(complete_dates, start, window)
        if end is None or end > updated_as_of:
            rows.append(
                {
                    **base,
                    "end_date": end.isoformat() if end else "",
                    "outcome_status": (
                        "PENDING" if end is not None else "INSUFFICIENT_DATA"
                    ),
                    "paper_action_status": paper_binding.get("status"),
                    "insufficient_reason": (
                        "WINDOW_NOT_DUE" if end is not None else "MISSING_COMPLETE_TRADING_DATES"
                    ),
                }
            )
            continue
        try:
            metrics = _outcome_window_metrics_v2(
                prices=prices,
                start=start,
                end=end,
                advisory_event=advisory_event,
                paper_binding=paper_binding,
                config=config,
            )
        except DynamicV3PaperTrackingError as exc:
            rows.append(
                {
                    **base,
                    "end_date": end.isoformat(),
                    "outcome_status": "INSUFFICIENT_DATA",
                    "paper_action_status": paper_binding.get("status"),
                    "insufficient_reason": str(exc),
                }
            )
            continue
        rows.append(
            {
                **base,
                "end_date": end.isoformat(),
                **metrics,
                "outcome_status": "AVAILABLE",
                "insufficient_reason": "",
            }
        )
    return rows


def _outcome_window_metrics_v2(
    *,
    prices: pd.DataFrame,
    start: date,
    end: date,
    advisory_event: Mapping[str, Any],
    paper_binding: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    no_trade = _validated_outcome_weights(
        _mapping(advisory_event.get("no_trade_weights")), "no_trade_weights"
    )
    baseline = _validated_outcome_weights(
        _mapping(advisory_event.get("baseline_weights")), "baseline_weights"
    )
    target = _validated_outcome_weights(
        _mapping(advisory_event.get("target_weights")), "target_weights"
    )
    limited = _validated_outcome_weights(
        _mapping(advisory_event.get("limited_adjustment_weights")),
        "limited_adjustment_weights",
    )
    cost_rate = _outcome_cost_rate(config)
    no_trade_return, _no_trade_daily, _ = _static_counterfactual_path(
        prices=prices,
        weights=no_trade,
        reference_weights=no_trade,
        start=start,
        end=end,
        cost_rate=0.0,
    )
    baseline_return, _baseline_daily, baseline_cost = _static_counterfactual_path(
        prices=prices,
        weights=baseline,
        reference_weights=no_trade,
        start=start,
        end=end,
        cost_rate=cost_rate,
    )
    target_return, _target_daily, target_cost = _static_counterfactual_path(
        prices=prices,
        weights=target,
        reference_weights=no_trade,
        start=start,
        end=end,
        cost_rate=cost_rate,
    )
    limited_return, _limited_daily, limited_cost = _static_counterfactual_path(
        prices=prices,
        weights=limited,
        reference_weights=no_trade,
        start=start,
        end=end,
        cost_rate=cost_rate,
    )
    paper_return, paper_daily, paper_cost, effective_date = _paper_counterfactual_path(
        prices=prices,
        no_trade_weights=no_trade,
        paper_binding=paper_binding,
        start=start,
        end=end,
        cost_rate=cost_rate,
    )
    metrics = {
        "paper_portfolio_return": round(paper_return, 6),
        "no_trade_return": round(no_trade_return, 6),
        "baseline_return": round(baseline_return, 6),
        "target_weight_return": round(target_return, 6),
        "limited_adjustment_return": round(limited_return, 6),
        "relative_to_no_trade": round(paper_return - no_trade_return, 6),
        "relative_to_baseline": round(paper_return - baseline_return, 6),
        "max_drawdown": round(_max_drawdown(paper_daily), 6),
        "realized_volatility": round(_realized_volatility(paper_daily), 6),
        "paper_transaction_cost": round(paper_cost, 8),
        "baseline_transaction_cost": round(baseline_cost, 8),
        "target_transaction_cost": round(target_cost, 8),
        "limited_transaction_cost": round(limited_cost, 8),
        "paper_action_status": paper_binding.get("status"),
        "paper_action_effective_date": effective_date.isoformat() if effective_date else "",
    }
    if not all(_is_finite_number(metrics[field]) for field in OUTCOME_METRIC_FIELDS):
        raise DynamicV3PaperTrackingError("outcome metrics contain non-finite values")
    return metrics


def _outcome_cost_rate(config: Mapping[str, Any]) -> float:
    simulation = _mapping(config.get("simulation"))
    total_bps = _strict_finite_number(
        simulation.get("transaction_cost_bps"), "transaction_cost_bps"
    ) + _strict_finite_number(simulation.get("slippage_bps"), "slippage_bps")
    return total_bps / 10_000.0


def _turnover_between(left: Mapping[str, Any], right: Mapping[str, Any]) -> float:
    symbols = set(left) | set(right)
    return 0.5 * sum(
        abs(_float(right.get(symbol)) - _float(left.get(symbol))) for symbol in symbols
    )


def _static_counterfactual_path(
    *,
    prices: pd.DataFrame,
    weights: Mapping[str, Any],
    reference_weights: Mapping[str, Any],
    start: date,
    end: date,
    cost_rate: float,
) -> tuple[float, list[float], float]:
    values, _dates = _fixed_share_values(prices, weights, start=start, end=end)
    turnover = _turnover_between(reference_weights, weights)
    cost = turnover * cost_rate
    total_return = (1.0 - cost) * values[-1] - 1.0
    daily = ([-cost] if cost else []) + _daily_returns_from_values(values)
    return total_return, daily, cost


def _paper_counterfactual_path(
    *,
    prices: pd.DataFrame,
    no_trade_weights: Mapping[str, Any],
    paper_binding: Mapping[str, Any],
    start: date,
    end: date,
    cost_rate: float,
) -> tuple[float, list[float], float, date | None]:
    no_trade_values, dates = _fixed_share_values(
        prices, no_trade_weights, start=start, end=end
    )
    action_at = _parse_datetime_text(_text(paper_binding.get("created_at")))
    after_weights = _mapping(paper_binding.get("after_weights"))
    if action_at is None or not after_weights:
        return no_trade_values[-1] - 1.0, _daily_returns_from_values(no_trade_values), 0.0, None
    effective = next((value for value in dates if value > action_at.date()), None)
    if effective is None or effective > end:
        return no_trade_values[-1] - 1.0, _daily_returns_from_values(no_trade_values), 0.0, None
    start_date = dates[0]
    if effective <= start_date:
        values, _ = _fixed_share_values(prices, after_weights, start=start, end=end)
        turnover = 0.5 * sum(
            abs(_float(value))
            for value in _mapping(paper_binding.get("applied_paper_deltas")).values()
        )
        cost = turnover * cost_rate
        total_return = (1.0 - cost) * values[-1] - 1.0
        daily = ([-cost] if cost else []) + _daily_returns_from_values(values)
        return total_return, daily, cost, effective
    effective_index = dates.index(effective)
    post_values, post_dates = _fixed_share_values(
        prices,
        after_weights,
        start=effective,
        end=end,
    )
    if post_dates[0] != effective:
        raise DynamicV3PaperTrackingError("paper action effective price is missing")
    turnover = 0.5 * sum(
        abs(_float(value))
        for value in _mapping(paper_binding.get("applied_paper_deltas")).values()
    )
    cost = turnover * cost_rate
    anchor = no_trade_values[effective_index] * (1.0 - cost)
    composite = [*no_trade_values[:effective_index], *[anchor * value for value in post_values]]
    return composite[-1] - 1.0, _daily_returns_from_values(composite), cost, effective


def _fixed_share_values(
    prices: pd.DataFrame,
    weights: Mapping[str, Any],
    *,
    start: date,
    end: date,
) -> tuple[list[float], list[date]]:
    clean = _validated_outcome_weights(weights, "counterfactual_weights")
    symbols = {symbol for symbol in clean if symbol != "CASH"}
    if prices.duplicated(subset=["_date", "symbol"]).any():
        raise DynamicV3PaperTrackingError("outcome price snapshot contains duplicate keys")
    pivot = prices.pivot(index="_date", columns="symbol", values="_adj_close")
    union_dates = sorted(value for value in pivot.index if value <= end)
    prior = [value for value in union_dates if value <= start]
    if not prior:
        raise DynamicV3PaperTrackingError("MISSING_COMPLETE_START_PRICE")
    start_idx = prior[-1]
    dates = [value for value in union_dates if start_idx <= value <= end]
    if not dates or end not in dates:
        raise DynamicV3PaperTrackingError("MISSING_END_PRICE")
    for date_value in dates:
        for symbol in symbols:
            if symbol not in pivot.columns:
                raise DynamicV3PaperTrackingError(f"MISSING_SYMBOL_PRICE:{symbol}")
            value = pivot.loc[date_value, symbol]
            if not _is_finite_number(value) or _float(value) <= 0:
                raise DynamicV3PaperTrackingError(
                    f"INCOMPLETE_PRICE_PATH:{symbol}:{date_value}"
                )
    base_prices = {symbol: _float(pivot.loc[start_idx, symbol]) for symbol in symbols}
    values = []
    for date_value in dates:
        value = _float(clean.get("CASH"))
        value += sum(
            _float(clean.get(symbol))
            * _float(pivot.loc[date_value, symbol])
            / base_prices[symbol]
            for symbol in symbols
        )
        if not math.isfinite(value) or value <= 0:
            raise DynamicV3PaperTrackingError("portfolio value path is invalid")
        values.append(value)
    return values, dates


def _daily_returns_from_values(values: Sequence[float]) -> list[float]:
    return [right / left - 1.0 for left, right in zip(values, values[1:], strict=False)]


def _max_drawdown(daily_returns: Sequence[float]) -> float:
    wealth = 1.0
    peak = 1.0
    max_dd = 0.0
    for value in daily_returns:
        wealth *= 1.0 + value
        peak = max(peak, wealth)
        max_dd = min(max_dd, wealth / peak - 1.0)
    return max_dd


def _realized_volatility(daily_returns: Sequence[float]) -> float:
    if len(daily_returns) < 2:
        return 0.0
    series = pd.Series(list(daily_returns), dtype=float)
    return float(series.std(ddof=1) * (252**0.5))


def _rollup_outcome_status(rows: Sequence[Mapping[str, Any]]) -> str:
    statuses = {_text(row.get("outcome_status")) for row in rows}
    if statuses == {"AVAILABLE"}:
        return "AVAILABLE"
    if "AVAILABLE" in statuses:
        return "PARTIAL"
    if "PENDING" in statuses:
        return "PENDING"
    if statuses == {"INSUFFICIENT_DATA"}:
        return "INSUFFICIENT_DATA"
    return "INSUFFICIENT_DATA"


def _owner_attribution_owner_snapshot(
    *,
    owner_review_dir: Path,
    generated_at: datetime,
) -> dict[str, Any]:
    events_path = owner_review_dir / "owner_review_events.jsonl"
    journal_path = owner_review_dir / "owner_review_journal.jsonl"
    report_path = owner_review_dir / "owner_review_report.md"
    required = (events_path, journal_path, report_path)
    if not all(path.is_file() for path in required):
        raise DynamicV3PaperTrackingError(
            "owner attribution requires the event-chain owner review artifacts"
        )
    events = _read_jsonl(events_path)
    records, replay_errors = replay_owner_review_records(events)
    if replay_errors:
        raise DynamicV3PaperTrackingError(
            "owner review event replay failed: " + ",".join(replay_errors)
        )
    if _read_jsonl(journal_path) != records:
        raise DynamicV3PaperTrackingError(
            "owner review journal does not match the event-chain replay"
        )
    for event in events:
        event_at = _parse_datetime_text(_text(event.get("event_at")))
        if event_at is None or event_at > generated_at:
            raise DynamicV3PaperTrackingError(
                "owner review event time must be valid and not later than attribution cutoff"
            )
    selected_reviews = []
    for record in records:
        review_id = _text(record.get("review_id"))
        validation = validate_owner_review_artifact(
            review_id=review_id,
            output_dir=owner_review_dir,
        )
        if validation.get("status") != "PASS":
            raise DynamicV3PaperTrackingError(
                f"owner review validation must PASS for attribution: {review_id}"
            )
        selected_reviews.append(
            {
                "review": record,
                "validation_status": "PASS",
                "validation_failed_check_count": validation.get("failed_check_count", 0),
            }
        )
    source_paths = [*required]
    for optional_name in ("latest_owner_review.json", "paper_action_log.jsonl"):
        optional_path = owner_review_dir / optional_name
        if optional_path.is_file():
            source_paths.append(optional_path)
    return {
        "schema_version": SCHEMA_VERSION,
        "snapshot_schema_version": OWNER_ATTRIBUTION_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_owner_review_source_snapshot",
        "generated_cutoff": generated_at.isoformat(),
        "source_root": str(owner_review_dir),
        "source_files": _source_file_inventory(owner_review_dir, source_paths),
        "event_chain_status": "PASS",
        "event_count": len(events),
        "last_event_checksum": (
            _text(events[-1].get("event_checksum")) if events else "GENESIS"
        ),
        "owner_review_events": events,
        "selected_review_count": len(selected_reviews),
        "selected_reviews": selected_reviews,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_effect": "none",
    }


def _owner_attribution_outcome_snapshot(
    *,
    records: Sequence[Mapping[str, Any]],
    outcome_dir: Path,
    generated_at: datetime,
) -> dict[str, Any]:
    by_daily = {_text(record.get("daily_advisory_id")): record for record in records}
    if len(by_daily) != len(records) or "" in by_daily:
        raise DynamicV3PaperTrackingError(
            "owner attribution requires unique non-empty daily advisory ids"
        )
    selected_by_daily: dict[str, dict[str, Any]] = {}
    if outcome_dir.exists():
        relevant_sources: dict[str, list[tuple[Path, dict[str, Any]]]] = {}
        for child in sorted(path for path in outcome_dir.iterdir() if path.is_dir()):
            manifest_path = child / "advisory_outcome_manifest.json"
            if not manifest_path.is_file():
                raise DynamicV3PaperTrackingError(
                    f"advisory outcome directory is missing manifest: {child}"
                )
            try:
                manifest = _read_json(manifest_path)
            except Exception as exc:  # noqa: BLE001
                raise DynamicV3PaperTrackingError(
                    f"advisory outcome manifest is unreadable: {manifest_path}: {exc}"
                ) from exc
            daily_id = _text(manifest.get("daily_advisory_id"))
            if daily_id not in by_daily:
                continue
            relevant_sources.setdefault(daily_id, []).append((child, manifest))
        duplicate_daily_ids = sorted(
            daily_id for daily_id, sources in relevant_sources.items() if len(sources) > 1
        )
        if duplicate_daily_ids:
            raise DynamicV3PaperTrackingError(
                "multiple advisory outcomes found for owner review daily id: "
                + ",".join(duplicate_daily_ids)
            )
        for daily_id in sorted(relevant_sources):
            sources = relevant_sources[daily_id]
            if len(sources) != 1:
                raise DynamicV3PaperTrackingError(
                    f"multiple advisory outcomes found for owner review daily id: {daily_id}"
                )
            child, manifest = sources[0]
            outcome_id = child.name
            validation = validate_advisory_outcome_artifact(
                outcome_id=outcome_id,
                output_dir=outcome_dir,
            )
            if validation.get("status") != "PASS":
                raise DynamicV3PaperTrackingError(
                    f"advisory outcome validation must PASS for attribution: {outcome_id}"
                )
            event = _read_json(child / "advisory_event.json")
            updates = _read_jsonl(child / "outcome_update_events.jsonl")
            tracked_at = _parse_datetime_text(_text(event.get("tracked_at")))
            event_times = [
                _parse_datetime_text(_text(update.get("event_at"))) for update in updates
            ]
            if (
                tracked_at is None
                or tracked_at > generated_at
                or any(value is None or value > generated_at for value in event_times)
            ):
                raise DynamicV3PaperTrackingError(
                    "advisory outcome tracking/update time cannot exceed attribution cutoff"
                )
            review = by_daily[daily_id]
            if (
                event.get("daily_advisory_id") != daily_id
                or event.get("as_of") != review.get("as_of")
                or manifest.get("as_of") != review.get("as_of")
            ):
                raise DynamicV3PaperTrackingError(
                    f"owner review and advisory outcome lineage mismatch: {daily_id}"
                )
            windows = _read_jsonl(child / "outcome_windows.jsonl")
            selected_by_daily[daily_id] = {
                "review_id": review.get("review_id"),
                "daily_advisory_id": daily_id,
                "review_as_of": review.get("as_of"),
                "owner_decision": review.get("owner_decision"),
                "outcome_id": outcome_id,
                "outcome_as_of": manifest.get("as_of"),
                "outcome_status": manifest.get("status"),
                "outcome_validation_status": "PASS",
                "outcome_validation_failed_check_count": validation.get(
                    "failed_check_count", 0
                ),
                "latest_update_event_id": manifest.get("latest_update_event_id", ""),
                "latest_update_event_checksum": manifest.get(
                    "latest_update_event_checksum", ""
                ),
                "advisory_event": event,
                "outcome_manifest": manifest,
                "outcome_update_events": updates,
                "source_artifact_root": str(child),
                "source_files": _source_file_inventory(
                    child,
                    sorted(path for path in child.rglob("*") if path.is_file()),
                ),
                "outcome_windows": windows,
                "broker_action_allowed": False,
                "broker_action_taken": False,
                "production_effect": "none",
            }
    return {
        "schema_version": SCHEMA_VERSION,
        "snapshot_schema_version": OWNER_ATTRIBUTION_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_advisory_outcome_source_snapshot",
        "generated_cutoff": generated_at.isoformat(),
        "source_root": str(outcome_dir),
        "selected_outcome_count": len(selected_by_daily),
        "selected_outcomes": [selected_by_daily[key] for key in sorted(selected_by_daily)],
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_effect": "none",
    }


def _source_file_inventory(root: Path, paths: Sequence[Path]) -> dict[str, Any]:
    return {
        path.relative_to(root).as_posix(): {
            "path": str(path),
            "checksum": _file_sha256(path),
        }
        for path in sorted(set(paths))
    }


def _owner_attribution_source_validation_summary(
    *,
    owner_snapshot: Mapping[str, Any],
    outcome_snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    reviews = _records(owner_snapshot.get("selected_reviews"))
    outcomes = _records(outcome_snapshot.get("selected_outcomes"))
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_owner_attribution_source_validation_summary",
        "snapshot_schema_version": OWNER_ATTRIBUTION_SNAPSHOT_SCHEMA_VERSION,
        "generated_cutoff": owner_snapshot.get("generated_cutoff"),
        "owner_review_source_root": owner_snapshot.get("source_root"),
        "owner_review_event_chain_status": owner_snapshot.get("event_chain_status"),
        "owner_review_event_count": owner_snapshot.get("event_count"),
        "selected_review_count": len(reviews),
        "selected_review_validation_pass_count": sum(
            item.get("validation_status") == "PASS" for item in reviews
        ),
        "advisory_outcome_source_root": outcome_snapshot.get("source_root"),
        "selected_outcome_count": len(outcomes),
        "selected_outcome_validation_pass_count": sum(
            item.get("outcome_validation_status") == "PASS" for item in outcomes
        ),
        "all_selected_sources_validated": all(
            item.get("validation_status") == "PASS" for item in reviews
        )
        and all(item.get("outcome_validation_status") == "PASS" for item in outcomes),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_effect": "none",
    }


def _owner_attribution_manifest_payload(
    *,
    attribution_dir: Path,
    generated_at: datetime,
    owner_snapshot: Mapping[str, Any],
    outcome_snapshot: Mapping[str, Any],
    comparison: Mapping[str, Any],
) -> dict[str, Any]:
    reviews = _records(owner_snapshot.get("selected_reviews"))
    outcomes = _records(outcome_snapshot.get("selected_outcomes"))
    owner_snapshot_path = attribution_dir / "owner_review_source_snapshot.json"
    outcome_snapshot_path = attribution_dir / "advisory_outcome_source_snapshot.json"
    source_validation_path = attribution_dir / "source_validation_summary.json"
    return {
        "schema_version": SCHEMA_VERSION,
        "source_snapshot_schema_version": OWNER_ATTRIBUTION_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_owner_attribution_manifest",
        "attribution_id": attribution_dir.name,
        "generated_at": generated_at.isoformat(),
        "status": "PASS",
        "evidence_status": comparison.get("status"),
        "total_reviews": len(reviews),
        "pending_review_count": sum(
            _mapping(item.get("review")).get("owner_decision") == "pending"
            for item in reviews
        ),
        "final_review_count": sum(
            _mapping(item.get("review")).get("owner_decision") != "pending"
            for item in reviews
        ),
        "linked_outcome_count": len(outcomes),
        "available_outcome_count": comparison.get("available_outcome_count", 0),
        "available_window_count": comparison.get("available_window_count", 0),
        "owner_review_source_snapshot_path": str(owner_snapshot_path),
        "owner_review_source_snapshot_checksum": _file_sha256(owner_snapshot_path),
        "advisory_outcome_source_snapshot_path": str(outcome_snapshot_path),
        "advisory_outcome_source_snapshot_checksum": _file_sha256(
            outcome_snapshot_path
        ),
        "source_validation_summary_path": str(source_validation_path),
        "source_validation_summary_checksum": _file_sha256(source_validation_path),
        "owner_decision_summary_path": str(
            attribution_dir / "owner_decision_summary.json"
        ),
        "advisory_acceptance_matrix_path": str(
            attribution_dir / "advisory_acceptance_matrix.json"
        ),
        "decision_outcome_comparison_path": str(
            attribution_dir / "decision_outcome_comparison.json"
        ),
        "owner_attribution_report_path": str(
            attribution_dir / "owner_attribution_report.md"
        ),
        "official_target_weights_generated": False,
        "portfolio_mutated": False,
        "order_ticket_generated": False,
        "production_candidate_generated": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "manual_review_required": True,
        "production_effect": "none",
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def _owner_attribution_record_has_no_execution_effect(
    record: Mapping[str, Any],
) -> bool:
    return (
        record.get("official_target_weights_generated") is False
        and record.get("portfolio_mutated") is False
        and record.get("order_ticket_generated") is False
        and record.get("production_candidate_generated") is False
        and record.get("broker_action_allowed") is False
        and record.get("broker_action_taken") is False
        and record.get("production_effect") == "none"
    )


def _owner_attribution_snapshot_errors(
    *,
    owner_snapshot: Mapping[str, Any],
    outcome_snapshot: Mapping[str, Any],
) -> list[str]:
    errors: list[str] = []
    if (
        owner_snapshot.get("snapshot_schema_version")
        != OWNER_ATTRIBUTION_SNAPSHOT_SCHEMA_VERSION
        or outcome_snapshot.get("snapshot_schema_version")
        != OWNER_ATTRIBUTION_SNAPSHOT_SCHEMA_VERSION
    ):
        errors.append("snapshot_schema_invalid")
    if owner_snapshot.get("generated_cutoff") != outcome_snapshot.get(
        "generated_cutoff"
    ):
        errors.append("snapshot_cutoff_mismatch")
    if owner_snapshot.get("event_chain_status") != "PASS":
        errors.append("owner_event_chain_not_pass")
    reviews = _records(owner_snapshot.get("selected_reviews"))
    records = [_mapping(item.get("review")) for item in reviews]
    owner_events = _records(owner_snapshot.get("owner_review_events"))
    replay_records, replay_errors = replay_owner_review_records(owner_events)
    if replay_errors or replay_records != records:
        errors.append("owner_snapshot_event_replay_mismatch")
    if (
        owner_snapshot.get("event_count") != len(owner_events)
        or owner_snapshot.get("selected_review_count") != len(reviews)
        or owner_snapshot.get("last_event_checksum")
        != (
            _text(owner_events[-1].get("event_checksum"))
            if owner_events
            else "GENESIS"
        )
    ):
        errors.append("owner_snapshot_counts_or_last_checksum_mismatch")
    cutoff = _parse_datetime_text(_text(owner_snapshot.get("generated_cutoff")))
    if cutoff is None:
        errors.append("snapshot_cutoff_invalid")
    elif any(
        (event_at := _parse_datetime_text(_text(event.get("event_at")))) is None
        or event_at > cutoff
        for event in owner_events
    ):
        errors.append("owner_snapshot_event_after_cutoff")
    review_ids = [_text(record.get("review_id")) for record in records]
    daily_ids = [_text(record.get("daily_advisory_id")) for record in records]
    if (
        len(review_ids) != len(set(review_ids))
        or len(daily_ids) != len(set(daily_ids))
        or any(not value for value in [*review_ids, *daily_ids])
    ):
        errors.append("owner_review_identity_invalid_or_duplicate")
    allowed_decisions = {"pending", *OWNER_REVIEW_DECISIONS}
    for index, (item, record) in enumerate(zip(reviews, records, strict=True), start=1):
        if item.get("validation_status") != "PASS":
            errors.append(f"owner_review_validation_not_pass:{index}")
        if record.get("owner_decision") not in allowed_decisions:
            errors.append(f"owner_decision_invalid:{index}")
        if not _owner_review_record_has_no_execution_effect(record):
            errors.append(f"owner_review_execution_effect_forbidden:{index}")
    errors.extend(_source_inventory_errors(owner_snapshot.get("source_files"), "owner"))
    by_daily = {daily_id: record for daily_id, record in zip(daily_ids, records, strict=True)}
    outcomes = _records(outcome_snapshot.get("selected_outcomes"))
    if outcome_snapshot.get("selected_outcome_count") != len(outcomes):
        errors.append("outcome_snapshot_count_mismatch")
    outcome_daily_ids = [_text(item.get("daily_advisory_id")) for item in outcomes]
    if len(outcome_daily_ids) != len(set(outcome_daily_ids)):
        errors.append("duplicate_outcome_daily_id")
    for index, item in enumerate(outcomes, start=1):
        daily_id = _text(item.get("daily_advisory_id"))
        review = by_daily.get(daily_id, {})
        if (
            not review
            or item.get("review_id") != review.get("review_id")
            or item.get("owner_decision") != review.get("owner_decision")
            or item.get("review_as_of") != review.get("as_of")
            or item.get("outcome_as_of") != review.get("as_of")
        ):
            errors.append(f"outcome_review_binding_mismatch:{index}")
        if item.get("outcome_validation_status") != "PASS":
            errors.append(f"outcome_validation_not_pass:{index}")
        if not _owner_attribution_snapshot_outcome_has_no_execution_effect(item):
            errors.append(f"outcome_execution_effect_forbidden:{index}")
        window_rows = _records(item.get("outcome_windows"))
        advisory_event = _mapping(item.get("advisory_event"))
        outcome_manifest = _mapping(item.get("outcome_manifest"))
        update_events = _records(item.get("outcome_update_events"))
        if (
            advisory_event.get("outcome_id") != item.get("outcome_id")
            or advisory_event.get("daily_advisory_id") != daily_id
            or advisory_event.get("as_of") != item.get("outcome_as_of")
            or outcome_manifest.get("outcome_id") != item.get("outcome_id")
            or outcome_manifest.get("daily_advisory_id") != daily_id
            or outcome_manifest.get("as_of") != item.get("outcome_as_of")
            or outcome_manifest.get("status") != item.get("outcome_status")
            or outcome_manifest.get("update_event_count") != len(update_events)
        ):
            errors.append(f"outcome_snapshot_manifest_event_mismatch:{index}")
        previous_checksum = "GENESIS"
        for sequence, update in enumerate(update_events, start=1):
            update_at = _parse_datetime_text(_text(update.get("event_at")))
            if (
                update.get("event_sequence") != sequence
                or update.get("previous_event_checksum") != previous_checksum
                or update.get("event_checksum") != _outcome_update_event_checksum(update)
                or update_at is None
                or (cutoff is not None and update_at > cutoff)
                or not _outcome_record_has_no_execution_effect(update)
            ):
                errors.append(f"outcome_snapshot_update_chain_invalid:{index}:{sequence}")
            previous_checksum = _text(update.get("event_checksum"))
        expected_latest_id = (
            _text(update_events[-1].get("update_event_id")) if update_events else ""
        )
        expected_latest_checksum = previous_checksum if update_events else ""
        expected_windows = (
            _records(update_events[-1].get("outcome_windows"))
            if update_events
            else window_rows
        )
        tracked_at = _parse_datetime_text(_text(advisory_event.get("tracked_at")))
        if (
            item.get("latest_update_event_id") != expected_latest_id
            or item.get("latest_update_event_checksum") != expected_latest_checksum
            or outcome_manifest.get("latest_update_event_id") != expected_latest_id
            or outcome_manifest.get("latest_update_event_checksum")
            != expected_latest_checksum
            or window_rows != expected_windows
            or tracked_at is None
            or (cutoff is not None and tracked_at > cutoff)
            or not _outcome_record_has_no_execution_effect(advisory_event)
        ):
            errors.append(f"outcome_snapshot_latest_binding_invalid:{index}")
        if not window_rows:
            errors.append(f"outcome_windows_missing:{index}")
        for row_index, row in enumerate(window_rows, start=1):
            status = row.get("outcome_status")
            if status not in OUTCOME_WINDOW_STATUSES:
                errors.append(f"outcome_window_status_invalid:{index}:{row_index}")
            elif status == "AVAILABLE":
                if not all(
                    _is_finite_number(row.get(field)) for field in OUTCOME_METRIC_FIELDS
                ):
                    errors.append(
                        f"available_outcome_metric_invalid:{index}:{row_index}"
                    )
            elif not all(row.get(field) is None for field in OUTCOME_METRIC_FIELDS):
                errors.append(
                    f"non_available_outcome_metric_not_null:{index}:{row_index}"
                )
        errors.extend(
            _source_inventory_errors(item.get("source_files"), f"outcome:{index}")
        )
    return sorted(set(errors))


def _source_inventory_errors(value: Any, label: str) -> list[str]:
    inventory = _mapping(value)
    if not inventory:
        return [f"source_inventory_missing:{label}"]
    errors = []
    for relative, raw in inventory.items():
        entry = _mapping(raw)
        checksum = _text(entry.get("checksum"))
        if not _text(relative) or not _text(entry.get("path")) or (
            len(checksum) != 64
            or any(character not in "0123456789abcdef" for character in checksum)
        ):
            errors.append(f"source_inventory_entry_invalid:{label}:{relative}")
    return errors


def _owner_review_record_has_no_execution_effect(record: Mapping[str, Any]) -> bool:
    return (
        record.get("official_target_weights_generated") is False
        and record.get("portfolio_mutated") is False
        and record.get("order_ticket_generated") is False
        and record.get("broker_action_allowed") is False
        and record.get("broker_action_taken") is False
        and record.get("production_effect") == "none"
    )


def _owner_attribution_snapshot_outcome_has_no_execution_effect(
    record: Mapping[str, Any],
) -> bool:
    return (
        record.get("broker_action_allowed") is False
        and record.get("broker_action_taken") is False
        and record.get("production_effect") == "none"
    )


def _owner_decision_summary(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    counter = Counter(_text(row.get("owner_decision"), "pending") for row in records)
    decisions = [
        "pending",
        "monitor",
        "no_trade",
        "paper_adjustment",
        "manual_adjustment",
        "reject_advisory",
        "needs_more_data",
    ]
    max_count = max(counter.values(), default=0)
    most_common_decisions = sorted(
        decision for decision, count in counter.items() if count == max_count
    )
    most_common = most_common_decisions[0] if most_common_decisions else "MISSING"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_owner_decision_summary",
        "total_reviews": len(records),
        **{decision: counter.get(decision, 0) for decision in decisions},
        "most_common_owner_decision": most_common,
        "most_common_owner_decisions": most_common_decisions,
        "pending_review_count": counter.get("pending", 0),
        "final_review_count": len(records) - counter.get("pending", 0),
        "official_target_weights_generated": False,
        "portfolio_mutated": False,
        "order_ticket_generated": False,
        "production_candidate_generated": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_effect": "none",
    }


def _advisory_acceptance_matrix(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    matrix: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_advisory_acceptance_matrix",
        "by_recommended_action": {},
    }
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in records:
        grouped[_text(row.get("recommended_action"), "MISSING")].append(row)
    for action, rows in sorted(grouped.items()):
        decisions = Counter(_text(row.get("owner_decision"), "pending") for row in rows)
        final_count = len(rows) - decisions.get("pending", 0)
        matrix["by_recommended_action"][action] = {
            "count": len(rows),
            "accepted_monitor": decisions.get("monitor", 0),
            "accepted_monitor_legacy_field_semantics": "monitor_count_only",
            "monitor_count": decisions.get("monitor", 0),
            "rejected": decisions.get("reject_advisory", 0),
            "paper_adjustment": decisions.get("paper_adjustment", 0),
            "pending_count": decisions.get("pending", 0),
            "final_decision_count": final_count,
            "monitor_rate_of_final": (
                round(decisions.get("monitor", 0) / final_count, 6)
                if final_count
                else None
            ),
            "paper_adjustment_rate_of_final": (
                round(decisions.get("paper_adjustment", 0) / final_count, 6)
                if final_count
                else None
            ),
            "owner_decisions": dict(decisions),
        }
    matrix.update(
        {
            "total_reviews": len(records),
            "broker_action_allowed": False,
            "broker_action_taken": False,
            "production_effect": "none",
        }
    )
    return matrix


def _decision_outcome_comparison(
    records: Sequence[Mapping[str, Any]],
    outcomes: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    outcome_by_daily = {
        _text(item.get("daily_advisory_id")): item for item in outcomes
    }
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for record in records:
        groups[_text(record.get("owner_decision"), "pending")].append(record)
    decision_groups = []
    available_outcome_count = 0
    available_window_count = 0
    for decision, decision_records in sorted(groups.items()):
        linked = [
            outcome_by_daily[_text(record.get("daily_advisory_id"))]
            for record in decision_records
            if _text(record.get("daily_advisory_id")) in outcome_by_daily
        ]
        available_by_outcome = [
            [
                row
                for row in _records(item.get("outcome_windows"))
                if row.get("outcome_status") == "AVAILABLE"
            ]
            for item in linked
        ]
        available = [row for rows in available_by_outcome for row in rows]
        group_available_outcomes = sum(bool(rows) for rows in available_by_outcome)
        available_outcome_count += group_available_outcomes
        available_window_count += len(available)
        avg_5d = _optional_avg_window(available, 5, "relative_to_no_trade")
        avg_20d = _optional_avg_window(available, 20, "relative_to_no_trade")
        decision_groups.append(
            {
                "owner_decision": decision,
                "review_count": len(decision_records),
                "linked_outcome_count": len(linked),
                "available_outcome_count": group_available_outcomes,
                "available_window_count": len(available),
                "avg_5d_relative_to_no_trade": avg_5d,
                "avg_5d_evidence_status": "AVAILABLE" if avg_5d is not None else "MISSING",
                "avg_20d_relative_to_no_trade": avg_20d,
                "avg_20d_evidence_status": "AVAILABLE" if avg_20d is not None else "MISSING",
                "avg_available_window_max_drawdown": _optional_avg_metric(
                    available, "max_drawdown"
                ),
                "insufficient_reason": (
                    "" if available else "NO_AVAILABLE_OUTCOME_WINDOW"
                ),
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_decision_outcome_comparison",
        "status": "AVAILABLE" if available_window_count else "INSUFFICIENT_DATA",
        "review_count": len(records),
        "linked_outcome_count": len(outcomes),
        "unlinked_review_count": len(records) - len(outcomes),
        "available_outcome_count": available_outcome_count,
        "available_window_count": available_window_count,
        "insufficient_reason": (
            "" if available_window_count else "NO_AVAILABLE_OUTCOME_WINDOW"
        ),
        "decision_groups": decision_groups,
        "official_target_weights_generated": False,
        "portfolio_mutated": False,
        "order_ticket_generated": False,
        "production_candidate_generated": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_effect": "none",
    }


def _optional_avg_window(
    rows: Sequence[Mapping[str, Any]], window: int, key: str
) -> float | None:
    values = [
        float(row[key])
        for row in rows
        if int(row.get("window_days") or 0) == window
        and _is_finite_number(row.get(key))
    ]
    return round(sum(values) / len(values), 6) if values else None


def _optional_avg_metric(
    rows: Sequence[Mapping[str, Any]], key: str
) -> float | None:
    values = [float(row[key]) for row in rows if _is_finite_number(row.get(key))]
    return round(sum(values) / len(values), 6) if values else None


def _shadow_aging_policy(config: Mapping[str, Any]) -> dict[str, Any]:
    raw = _mapping(config.get("promotion_clock_v2"))

    def integer(name: str, *aliases: str) -> int:
        value = next((raw.get(key) for key in (name, *aliases) if key in raw), None)
        if not isinstance(value, int) or isinstance(value, bool):
            raise DynamicV3PaperTrackingError(f"promotion_clock_v2.{name} must be an integer")
        return value

    def number(name: str, *aliases: str) -> float:
        value = next((raw.get(key) for key in (name, *aliases) if key in raw), None)
        return _strict_finite_number(value, f"promotion_clock_v2.{name}")

    policy = {
        "minimum_days_observed": integer("minimum_days_observed", "min_days_observed"),
        "minimum_rebalance_count": integer(
            "minimum_rebalance_count", "min_rebalance_count"
        ),
        "max_drift_warning_count": integer("max_drift_warning_count"),
        "max_high_disagreement_count": integer("max_high_disagreement_count"),
        "max_downgrade_warning_count": integer("max_downgrade_warning_count"),
        "minimum_outcome_score": number("minimum_outcome_score", "min_outcome_score"),
        "downgrade_outcome_score_floor": number(
            "downgrade_outcome_score_floor", "downgrade_outcome_score"
        ),
        "minimum_outcome_available_window_count": integer(
            "minimum_outcome_available_window_count"
        ),
        "outcome_aggregation": _text(raw.get("outcome_aggregation")),
        "require_consensus_drift_per_monitor": raw.get(
            "require_consensus_drift_per_monitor"
        ),
    }
    for field in (
        "minimum_days_observed",
        "minimum_rebalance_count",
        "max_drift_warning_count",
        "max_high_disagreement_count",
        "max_downgrade_warning_count",
        "minimum_outcome_available_window_count",
    ):
        if int(policy[field]) < 0:
            raise DynamicV3PaperTrackingError(f"promotion_clock_v2.{field} must be nonnegative")
    if policy["outcome_aggregation"] != "mean_available_windows_equal_weight":
        raise DynamicV3PaperTrackingError(
            "promotion_clock_v2.outcome_aggregation must be "
            "mean_available_windows_equal_weight"
        )
    if policy["require_consensus_drift_per_monitor"] is not True:
        raise DynamicV3PaperTrackingError(
            "promotion_clock_v2.require_consensus_drift_per_monitor must be true"
        )
    if float(policy["downgrade_outcome_score_floor"]) > float(
        policy["minimum_outcome_score"]
    ):
        raise DynamicV3PaperTrackingError(
            "downgrade outcome floor cannot exceed minimum outcome score"
        )
    return policy


def _shadow_aging_source_snapshot(
    *,
    shadow_shortlist_id: str,
    config_path: Path,
    shadow_shortlist_dir: Path,
    shadow_monitor_run_dir: Path,
    consensus_drift_dir: Path,
    advisory_outcome_dir: Path,
    generated_at: datetime,
) -> dict[str, Any]:
    config = load_paper_portfolio_config(config_path)
    policy = _shadow_aging_policy(config)
    shortlist_root = shadow_shortlist_dir / shadow_shortlist_id
    shortlist_validation = validate_shadow_shortlist_artifact(
        shadow_shortlist_id=shadow_shortlist_id,
        output_dir=shadow_shortlist_dir,
    )
    if shortlist_validation.get("status") != "PASS":
        raise DynamicV3PaperTrackingError(
            "shadow shortlist validation must PASS before shadow aging"
        )
    shortlist_manifest = _read_json(shortlist_root / "shadow_shortlist_manifest.json")
    shortlist_generated = _parse_datetime_text(_text(shortlist_manifest.get("generated_at")))
    if shortlist_generated is None or shortlist_generated > generated_at:
        raise DynamicV3PaperTrackingError(
            "shadow shortlist generated_at must be valid and not exceed aging cutoff"
        )
    shortlist_rows = _read_jsonl(shortlist_root / "shadow_shortlist_candidates.jsonl")
    candidate_ids = [_text(row.get("candidate_id")) for row in shortlist_rows]
    if (
        not candidate_ids
        or any(not value for value in candidate_ids)
        or len(candidate_ids) != len(set(candidate_ids))
        or int(shortlist_manifest.get("candidate_count") or 0) != len(candidate_ids)
    ):
        raise DynamicV3PaperTrackingError(
            "shadow shortlist candidate ids must be non-empty, unique, and count-bound"
        )
    candidate_set = set(candidate_ids)
    selected_monitors: list[dict[str, Any]] = []
    seen_monitor_ids: set[str] = set()
    seen_as_of: set[str] = set()
    for child in sorted(path for path in shadow_monitor_run_dir.glob("*") if path.is_dir()):
        manifest_path = child / "shadow_monitor_manifest.json"
        if not manifest_path.is_file():
            raise DynamicV3PaperTrackingError(f"monitor directory is missing manifest: {child}")
        manifest = _read_json(manifest_path)
        if _text(manifest.get("shadow_shortlist_id")) != shadow_shortlist_id:
            continue
        as_of = _date_from_any(manifest.get("as_of"))
        generated = _parse_datetime_text(_text(manifest.get("generated_at")))
        if as_of is None or generated is None:
            raise DynamicV3PaperTrackingError("selected monitor requires valid as_of/generated_at")
        if as_of > generated_at.date() or generated > generated_at:
            continue
        monitor_id = _text(manifest.get("monitor_run_id"))
        if not monitor_id or monitor_id in seen_monitor_ids or as_of.isoformat() in seen_as_of:
            raise DynamicV3PaperTrackingError(
                "selected monitors require unique monitor id and unique as_of"
            )
        validation = validate_shadow_monitor_run_artifact(
            monitor_run_id=monitor_id,
            output_dir=shadow_monitor_run_dir,
        )
        if validation.get("status") != "PASS":
            raise DynamicV3PaperTrackingError(
                f"shadow monitor validation must PASS for aging: {monitor_id}"
            )
        rows = _read_jsonl(child / "shadow_candidate_daily_results.jsonl")
        row_ids = [_text(row.get("candidate_id")) for row in rows]
        if len(row_ids) != len(set(row_ids)) or set(row_ids) != candidate_set:
            raise DynamicV3PaperTrackingError(
                f"monitor candidate set must exactly match shortlist: {monitor_id}"
            )
        normalized_rows = []
        for row in rows:
            if _date_from_any(row.get("as_of")) != as_of:
                raise DynamicV3PaperTrackingError(
                    f"monitor candidate row as_of mismatch: {monitor_id}"
                )
            normalized_rows.append(
                {
                    **row,
                    "target_weights": _validated_outcome_weights(
                        _mapping(row.get("target_weights")),
                        f"monitor_target_weights:{monitor_id}:{row.get('candidate_id')}",
                    ),
                }
            )
        source_paths = sorted(path for path in child.iterdir() if path.is_file())
        selected_monitors.append(
            {
                "monitor_run_id": monitor_id,
                "as_of": as_of.isoformat(),
                "generated_at": generated.isoformat(),
                "validation_status": "PASS",
                "source_root": str(child),
                "source_files": _source_file_inventory(child, source_paths),
                "manifest": manifest,
                "candidate_rows": sorted(
                    normalized_rows, key=lambda row: _text(row.get("candidate_id"))
                ),
            }
        )
        seen_monitor_ids.add(monitor_id)
        seen_as_of.add(as_of.isoformat())
    selected_monitors.sort(key=lambda item: (item["as_of"], item["monitor_run_id"]))
    monitor_by_id = {item["monitor_run_id"]: item for item in selected_monitors}
    selected_drifts: list[dict[str, Any]] = []
    seen_drift_monitors: set[str] = set()
    for child in sorted(path for path in consensus_drift_dir.glob("*") if path.is_dir()):
        manifest_path = child / "consensus_drift_manifest.json"
        if not manifest_path.is_file():
            raise DynamicV3PaperTrackingError(f"drift directory is missing manifest: {child}")
        manifest = _read_json(manifest_path)
        monitor_id = _text(
            manifest.get("source_shadow_monitor_run_id"),
            _text(manifest.get("monitor_run_id")),
        )
        if monitor_id not in monitor_by_id:
            continue
        if monitor_id in seen_drift_monitors:
            raise DynamicV3PaperTrackingError(
                f"multiple consensus drift artifacts found for monitor: {monitor_id}"
            )
        generated = _parse_datetime_text(_text(manifest.get("generated_at")))
        as_of = _date_from_any(manifest.get("as_of"))
        if generated is None or as_of is None or generated > generated_at:
            raise DynamicV3PaperTrackingError(
                f"selected consensus drift exceeds aging cutoff: {monitor_id}"
            )
        if as_of.isoformat() != monitor_by_id[monitor_id]["as_of"]:
            raise DynamicV3PaperTrackingError(
                f"consensus drift as_of does not match monitor: {monitor_id}"
            )
        drift_id = _text(manifest.get("drift_id"), child.name)
        validation = validate_consensus_drift_artifact(
            drift_id=drift_id,
            output_dir=consensus_drift_dir,
        )
        if validation.get("status") != "PASS":
            raise DynamicV3PaperTrackingError(
                f"consensus drift validation must PASS for aging: {drift_id}"
            )
        summary = _read_json(child / "consensus_drift_summary.json")
        selected_drifts.append(
            {
                "drift_id": drift_id,
                "monitor_run_id": monitor_id,
                "as_of": as_of.isoformat(),
                "generated_at": generated.isoformat(),
                "validation_status": "PASS",
                "source_root": str(child),
                "source_files": _source_file_inventory(
                    child, sorted(path for path in child.iterdir() if path.is_file())
                ),
                "manifest": manifest,
                "summary": summary,
            }
        )
        seen_drift_monitors.add(monitor_id)
    selected_drifts.sort(key=lambda item: (item["as_of"], item["drift_id"]))
    selected_outcomes = _shadow_aging_outcome_snapshot(
        candidate_ids=candidate_set,
        advisory_outcome_dir=advisory_outcome_dir,
        generated_at=generated_at,
    )
    config_metadata = _mapping(config.get("policy_metadata"))
    return {
        "schema_version": SCHEMA_VERSION,
        "snapshot_schema_version": SHADOW_AGING_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shadow_aging_source_snapshot",
        "generated_cutoff": generated_at.isoformat(),
        "shadow_shortlist_id": shadow_shortlist_id,
        "config": {
            "path": str(config_path),
            "checksum": _file_sha256(config_path),
            "policy_id": config_metadata.get("policy_id"),
            "policy_version": config_metadata.get("version"),
            "promotion_clock_v2": policy,
            "simulation_cost_rate": _outcome_cost_rate(config),
        },
        "shortlist": {
            "validation_status": "PASS",
            "source_root": str(shortlist_root),
            "source_files": _source_file_inventory(
                shortlist_root,
                sorted(path for path in shortlist_root.iterdir() if path.is_file()),
            ),
            "manifest": shortlist_manifest,
            "candidate_rows": sorted(
                shortlist_rows, key=lambda row: _text(row.get("candidate_id"))
            ),
        },
        "candidate_ids": sorted(candidate_ids),
        "selected_monitor_count": len(selected_monitors),
        "selected_monitors": selected_monitors,
        "selected_drift_count": len(selected_drifts),
        "selected_drifts": selected_drifts,
        "selected_outcome_count": len(selected_outcomes),
        "selected_outcomes": selected_outcomes,
        "official_target_weights_generated": False,
        "portfolio_mutated": False,
        "order_ticket_generated": False,
        "automatic_candidate_promotion": False,
        "production_candidate_generated": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_effect": "none",
    }


def _shadow_aging_outcome_snapshot(
    *,
    candidate_ids: set[str],
    advisory_outcome_dir: Path,
    generated_at: datetime,
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    seen_daily: set[str] = set()
    if not advisory_outcome_dir.exists():
        return selected
    for child in sorted(path for path in advisory_outcome_dir.glob("*") if path.is_dir()):
        manifest_path = child / "advisory_outcome_manifest.json"
        event_path = child / "advisory_event.json"
        if not manifest_path.is_file() or not event_path.is_file():
            raise DynamicV3PaperTrackingError(
                f"advisory outcome directory is incomplete: {child}"
            )
        manifest = _read_json(manifest_path)
        event = _read_json(event_path)
        frozen_daily = _mapping(_mapping(event.get("frozen_track_sources")).get("daily"))
        target_entry = _mapping(frozen_daily.get("daily_candidate_targets"))
        target_path = Path(_text(target_entry.get("path")))
        if not target_path.is_file():
            continue
        target_rows = _read_jsonl(target_path)
        target_ids = {_text(row.get("candidate_id")) for row in target_rows}
        if not (candidate_ids & target_ids):
            continue
        tracked_at = _parse_datetime_text(_text(event.get("tracked_at")))
        if tracked_at is None:
            raise DynamicV3PaperTrackingError("candidate-linked outcome tracked_at is invalid")
        if tracked_at > generated_at:
            continue
        outcome_id = _text(manifest.get("outcome_id"), child.name)
        validation = validate_advisory_outcome_artifact(
            outcome_id=outcome_id,
            output_dir=advisory_outcome_dir,
        )
        if validation.get("status") != "PASS":
            raise DynamicV3PaperTrackingError(
                f"advisory outcome validation must PASS for aging: {outcome_id}"
            )
        daily_id = _text(event.get("daily_advisory_id"))
        if not daily_id or daily_id in seen_daily:
            raise DynamicV3PaperTrackingError(
                f"candidate-linked outcomes require unique daily advisory id: {daily_id}"
            )
        updates = _read_jsonl(child / "outcome_update_events.jsonl")
        update_times = [
            _parse_datetime_text(_text(update.get("event_at"))) for update in updates
        ]
        if any(value is None or value > generated_at for value in update_times):
            raise DynamicV3PaperTrackingError(
                f"candidate-linked outcome update exceeds aging cutoff: {outcome_id}"
            )
        normalized_targets = []
        seen_candidates: set[str] = set()
        for row in target_rows:
            candidate_id = _text(row.get("candidate_id"))
            if candidate_id not in candidate_ids:
                continue
            if candidate_id in seen_candidates:
                raise DynamicV3PaperTrackingError(
                    f"duplicate candidate target in outcome source: {candidate_id}"
                )
            normalized_targets.append(
                {
                    "candidate_id": candidate_id,
                    "target_weights": _validated_outcome_weights(
                        _mapping(row.get("target_weights")),
                        f"outcome_candidate_target:{outcome_id}:{candidate_id}",
                    ),
                }
            )
            seen_candidates.add(candidate_id)
        windows = _read_jsonl(child / "outcome_windows.jsonl")
        price_rows: list[dict[str, Any]] = []
        if updates:
            snapshot_path = Path(_text(updates[-1].get("price_snapshot_path")))
            if snapshot_path.is_file():
                snapshot_prices = _read_outcome_price_snapshot(snapshot_path)
                price_rows = [
                    {
                        "date": value["_date"].isoformat(),
                        "symbol": _text(value["symbol"]),
                        "adj_close": _float(value["_adj_close"]),
                    }
                    for value in snapshot_prices.to_dict(orient="records")
                ]
        frozen_config = _mapping(_mapping(event.get("frozen_track_sources")).get("outcome_config"))
        frozen_config_path = Path(_text(frozen_config.get("path")))
        if not frozen_config_path.is_file():
            raise DynamicV3PaperTrackingError(
                f"candidate-linked outcome frozen config is missing: {outcome_id}"
            )
        selected.append(
            {
                "outcome_id": outcome_id,
                "daily_advisory_id": daily_id,
                "as_of": event.get("as_of"),
                "tracked_at": tracked_at.isoformat(),
                "validation_status": "PASS",
                "source_root": str(child),
                "source_files": _source_file_inventory(
                    child, sorted(path for path in child.rglob("*") if path.is_file())
                ),
                "manifest": manifest,
                "advisory_event": event,
                "update_events": updates,
                "outcome_windows": windows,
                "candidate_targets_path": str(target_path),
                "candidate_targets_checksum": _file_sha256(target_path),
                "candidate_targets": sorted(
                    normalized_targets, key=lambda row: row["candidate_id"]
                ),
                "latest_price_rows": price_rows,
                "cost_rate": _outcome_cost_rate(
                    load_paper_portfolio_config(frozen_config_path)
                ),
                "broker_action_allowed": False,
                "broker_action_taken": False,
                "production_effect": "none",
            }
        )
        seen_daily.add(daily_id)
    return sorted(selected, key=lambda item: (item["as_of"], item["outcome_id"]))


def _shadow_aging_candidate_outcome_evidence(
    source_snapshot: Mapping[str, Any], candidate_id: str
) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    for outcome in _records(source_snapshot.get("selected_outcomes")):
        target = next(
            (
                _mapping(item.get("target_weights"))
                for item in _records(outcome.get("candidate_targets"))
                if item.get("candidate_id") == candidate_id
            ),
            {},
        )
        if not target:
            continue
        price_rows = _records(outcome.get("latest_price_rows"))
        prices = pd.DataFrame(price_rows)
        if not prices.empty:
            prices["_date"] = pd.to_datetime(prices["date"], errors="coerce").dt.date
            prices["_adj_close"] = pd.to_numeric(prices["adj_close"], errors="coerce")
        no_trade = _mapping(_mapping(outcome.get("advisory_event")).get("no_trade_weights"))
        for window in _records(outcome.get("outcome_windows")):
            if window.get("outcome_status") != "AVAILABLE":
                continue
            base = {
                "outcome_id": outcome.get("outcome_id"),
                "daily_advisory_id": outcome.get("daily_advisory_id"),
                "window_days": window.get("window_days"),
                "start_date": window.get("start_date"),
                "end_date": window.get("end_date"),
            }
            start = _date_from_any(window.get("start_date"))
            end = _date_from_any(window.get("end_date"))
            try:
                if start is None or end is None or prices.empty:
                    raise DynamicV3PaperTrackingError("MISSING_CANDIDATE_PRICE_SNAPSHOT")
                no_trade_return, _daily, _cost = _static_counterfactual_path(
                    prices=prices,
                    weights=no_trade,
                    reference_weights=no_trade,
                    start=start,
                    end=end,
                    cost_rate=0.0,
                )
                candidate_return, _candidate_daily, candidate_cost = (
                    _static_counterfactual_path(
                        prices=prices,
                        weights=target,
                        reference_weights=no_trade,
                        start=start,
                        end=end,
                        cost_rate=_float(outcome.get("cost_rate")),
                    )
                )
                evidence.append(
                    {
                        **base,
                        "status": "AVAILABLE",
                        "candidate_return": round(candidate_return, 6),
                        "no_trade_return": round(no_trade_return, 6),
                        "relative_to_no_trade": round(
                            candidate_return - no_trade_return, 6
                        ),
                        "candidate_transaction_cost": round(candidate_cost, 8),
                        "insufficient_reason": "",
                    }
                )
            except DynamicV3PaperTrackingError as exc:
                evidence.append(
                    {
                        **base,
                        "status": "INSUFFICIENT_DATA",
                        "candidate_return": None,
                        "no_trade_return": None,
                        "relative_to_no_trade": None,
                        "candidate_transaction_cost": None,
                        "insufficient_reason": str(exc),
                    }
                )
    return evidence


def _shadow_aging_candidate_rows(source_snapshot: Mapping[str, Any]) -> list[dict[str, Any]]:
    policy = _mapping(_mapping(source_snapshot.get("config")).get("promotion_clock_v2"))
    monitors = _records(source_snapshot.get("selected_monitors"))
    drifts = _records(source_snapshot.get("selected_drifts"))
    rows = []
    for candidate_id in _texts(source_snapshot.get("candidate_ids")):
        observations = []
        for monitor in monitors:
            row = next(
                (
                    item
                    for item in _records(monitor.get("candidate_rows"))
                    if item.get("candidate_id") == candidate_id
                ),
                None,
            )
            if row is not None:
                observations.append(
                    {
                        **row,
                        "monitor_run_id": monitor.get("monitor_run_id"),
                        "as_of": monitor.get("as_of"),
                    }
                )
        observations.sort(key=lambda item: (item["as_of"], item["monitor_run_id"]))
        dates = [_date_from_any(item.get("as_of")) for item in observations]
        valid_dates = [value for value in dates if value is not None]
        calendar_span = (
            (valid_dates[-1] - valid_dates[0]).days + 1 if valid_dates else 0
        )
        rebalance_count = sum(
            not _weights_equal(
                _mapping(left.get("target_weights")),
                _mapping(right.get("target_weights")),
            )
            for left, right in zip(observations, observations[1:], strict=False)
        )
        drift_warning_count = sum(
            _text(_mapping(item.get("live_vs_backtest_drift")).get("status"))
            not in {"", "PASS", "UNKNOWN"}
            for item in observations
        )
        downgrade_warning_count = sum(
            _text(item.get("recommendation"))
            in {"required_downgrade", "remove_from_shadow"}
            for item in observations
        )
        monitor_ids = {item["monitor_run_id"] for item in observations}
        candidate_drifts = [item for item in drifts if item.get("monitor_run_id") in monitor_ids]
        high_disagreement_count = sum(
            _mapping(item.get("summary")).get("disagreement_status")
            == "HIGH_DISAGREEMENT"
            for item in candidate_drifts
        )
        missing_drift_count = len(monitor_ids) - len(candidate_drifts)
        outcome_evidence = _shadow_aging_candidate_outcome_evidence(
            source_snapshot, candidate_id
        )
        available_evidence = [
            item for item in outcome_evidence if item.get("status") == "AVAILABLE"
        ]
        available_values = [
            _float(item.get("relative_to_no_trade")) for item in available_evidence
        ]
        outcome_score = (
            round(sum(available_values) / len(available_values), 6)
            if available_values
            else None
        )
        linked_outcomes = {
            _text(item.get("outcome_id")) for item in outcome_evidence if item.get("outcome_id")
        }
        blockers: list[str] = []
        downgrade_reasons: list[str] = []
        if calendar_span < int(policy.get("minimum_days_observed") or 0):
            blockers.append("insufficient_calendar_span")
        if rebalance_count < int(policy.get("minimum_rebalance_count") or 0):
            blockers.append("insufficient_true_rebalance_count")
        if drift_warning_count > int(policy.get("max_drift_warning_count") or 0):
            blockers.append("drift_warning_count_too_high")
        if high_disagreement_count > int(
            policy.get("max_high_disagreement_count") or 0
        ):
            blockers.append("high_disagreement_count_too_high")
        if missing_drift_count and policy.get("require_consensus_drift_per_monitor") is True:
            blockers.append("missing_consensus_drift_evidence")
        if len(available_evidence) < int(
            policy.get("minimum_outcome_available_window_count") or 0
        ):
            blockers.append("insufficient_candidate_outcome_evidence")
        elif outcome_score is not None:
            if outcome_score < _float(policy.get("downgrade_outcome_score_floor")):
                downgrade_reasons.append("outcome_score_below_downgrade_floor")
            elif outcome_score < _float(policy.get("minimum_outcome_score")):
                blockers.append("outcome_score_below_minimum")
        if downgrade_warning_count > int(
            policy.get("max_downgrade_warning_count") or 0
        ):
            downgrade_reasons.append("downgrade_warning_count_too_high")
        if downgrade_reasons:
            status = "downgrade_recommended"
        elif not observations:
            status = "not_started"
        elif blockers:
            warmup_only = {
                "insufficient_calendar_span",
                "insufficient_true_rebalance_count",
            }
            status = "warming_up" if set(blockers) <= warmup_only else "blocked"
        else:
            status = "eligible_for_review"
        stability_score = round(
            max(
                0.0,
                1.0
                - (drift_warning_count + high_disagreement_count + downgrade_warning_count)
                / max(1, len(observations) + len(candidate_drifts)),
            ),
            6,
        )
        next_review_date = (
            (
                valid_dates[-1]
                + timedelta(
                    days=max(
                        0,
                        int(policy.get("minimum_days_observed") or 0) - calendar_span,
                    )
                )
            ).isoformat()
            if valid_dates
            else None
        )
        rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "candidate_id": candidate_id,
                "calendar_span_days": calendar_span,
                "unique_observation_day_count": len(valid_dates),
                "monitor_run_count": len(observations),
                "true_rebalance_count": rebalance_count,
                "days_observed": calendar_span,
                "rebalance_count_observed": rebalance_count,
                "advisory_participation_count": len(observations),
                "drift_warning_count": drift_warning_count,
                "consensus_drift_evidence_count": len(candidate_drifts),
                "missing_consensus_drift_count": missing_drift_count,
                "high_disagreement_count": high_disagreement_count,
                "downgrade_warning_count": downgrade_warning_count,
                "linked_outcome_count": len(linked_outcomes),
                "candidate_outcome_available_window_count": len(available_evidence),
                "candidate_outcome_insufficient_window_count": len(outcome_evidence)
                - len(available_evidence),
                "outcome_score_scope": "candidate_specific_available_windows",
                "outcome_aggregation": policy.get("outcome_aggregation"),
                "outcome_score": outcome_score,
                "outcome_evidence": outcome_evidence,
                "stability_score": stability_score,
                "promotion_clock_status": status,
                "blocking_reasons": sorted(set(blockers)),
                "downgrade_reasons": sorted(set(downgrade_reasons)),
                "next_review_date": next_review_date,
                "manual_review_required": True,
                "automatic_candidate_promotion": False,
                "official_target_weights_generated": False,
                "portfolio_mutated": False,
                "order_ticket_generated": False,
                "production_candidate_generated": False,
                "broker_action_allowed": False,
                "broker_action_taken": False,
                "production_effect": "none",
            }
        )
    return rows


def _shadow_aging_summary(
    *, source_snapshot: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    status_counts = Counter(_text(row.get("promotion_clock_status")) for row in rows)
    config = _mapping(source_snapshot.get("config"))
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_promotion_clock_v2_summary",
        "aging_id": "",
        "shadow_shortlist_id": source_snapshot.get("shadow_shortlist_id"),
        "policy_id": config.get("policy_id"),
        "policy_version": config.get("policy_version"),
        "candidate_count": len(rows),
        "selected_monitor_count": source_snapshot.get("selected_monitor_count", 0),
        "selected_drift_count": source_snapshot.get("selected_drift_count", 0),
        "selected_outcome_count": source_snapshot.get("selected_outcome_count", 0),
        "candidate_outcome_available_window_count": sum(
            int(row.get("candidate_outcome_available_window_count") or 0) for row in rows
        ),
        "candidate_outcome_missing_count": sum(row.get("outcome_score") is None for row in rows),
        "not_started_count": status_counts["not_started"],
        "eligible_for_review_count": status_counts["eligible_for_review"],
        "downgrade_recommended_count": status_counts["downgrade_recommended"],
        "warming_up_count": status_counts["warming_up"],
        "blocked_count": status_counts["blocked"],
        "eligible_is_automatic_promotion": False,
        "automatic_candidate_promotion": False,
        "production_candidate_generated": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_effect": "none",
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def _shadow_aging_source_validation_summary(
    source_snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    monitors = _records(source_snapshot.get("selected_monitors"))
    drifts = _records(source_snapshot.get("selected_drifts"))
    outcomes = _records(source_snapshot.get("selected_outcomes"))
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shadow_aging_source_validation_summary",
        "snapshot_schema_version": SHADOW_AGING_SNAPSHOT_SCHEMA_VERSION,
        "generated_cutoff": source_snapshot.get("generated_cutoff"),
        "shadow_shortlist_validation_status": _mapping(
            source_snapshot.get("shortlist")
        ).get("validation_status"),
        "selected_monitor_count": len(monitors),
        "selected_monitor_validation_pass_count": sum(
            item.get("validation_status") == "PASS" for item in monitors
        ),
        "selected_drift_count": len(drifts),
        "selected_drift_validation_pass_count": sum(
            item.get("validation_status") == "PASS" for item in drifts
        ),
        "selected_outcome_count": len(outcomes),
        "selected_outcome_validation_pass_count": sum(
            item.get("validation_status") == "PASS" for item in outcomes
        ),
        "all_selected_sources_validated": _mapping(
            source_snapshot.get("shortlist")
        ).get("validation_status")
        == "PASS"
        and all(item.get("validation_status") == "PASS" for item in monitors)
        and all(item.get("validation_status") == "PASS" for item in drifts)
        and all(item.get("validation_status") == "PASS" for item in outcomes),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_effect": "none",
    }


def _shadow_aging_manifest_payload(
    *,
    aging_dir: Path,
    aging_id: str,
    generated_at: datetime,
    source_snapshot: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
) -> dict[str, Any]:
    config = _mapping(source_snapshot.get("config"))
    return {
        "schema_version": SCHEMA_VERSION,
        "source_snapshot_schema_version": SHADOW_AGING_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shadow_aging_manifest",
        "aging_id": aging_id,
        "shadow_shortlist_id": source_snapshot.get("shadow_shortlist_id"),
        "generated_at": generated_at.isoformat(),
        "status": "PASS" if rows else "INSUFFICIENT_DATA",
        "evidence_status": (
            "MANUAL_REVIEW_CANDIDATES_AVAILABLE"
            if int(summary.get("eligible_for_review_count") or 0)
            else "OBSERVATION_CONTINUES"
        ),
        "candidate_count": len(rows),
        "policy_id": config.get("policy_id"),
        "policy_version": config.get("policy_version"),
        "config_path": config.get("path"),
        "config_checksum": config.get("checksum"),
        "selected_monitor_count": source_snapshot.get("selected_monitor_count", 0),
        "selected_drift_count": source_snapshot.get("selected_drift_count", 0),
        "selected_outcome_count": source_snapshot.get("selected_outcome_count", 0),
        "source_snapshot_path": str(aging_dir / "shadow_aging_source_snapshot.json"),
        "source_snapshot_checksum": _file_sha256(
            aging_dir / "shadow_aging_source_snapshot.json"
        ),
        "source_validation_summary_path": str(aging_dir / "source_validation_summary.json"),
        "source_validation_summary_checksum": _file_sha256(
            aging_dir / "source_validation_summary.json"
        ),
        "candidate_aging_status_path": str(aging_dir / "candidate_aging_status.jsonl"),
        "promotion_clock_v2_summary_path": str(aging_dir / "promotion_clock_v2_summary.json"),
        "shadow_aging_report_path": str(aging_dir / "shadow_aging_report.md"),
        "eligible_for_review_is_manual_queue_only": True,
        "automatic_candidate_promotion": False,
        "official_target_weights_generated": False,
        "portfolio_mutated": False,
        "order_ticket_generated": False,
        "production_candidate_generated": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "manual_review_required": True,
        "production_effect": "none",
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def _shadow_aging_record_has_no_execution_effect(record: Mapping[str, Any]) -> bool:
    return (
        record.get("automatic_candidate_promotion") is False
        and record.get("official_target_weights_generated") is False
        and record.get("portfolio_mutated") is False
        and record.get("order_ticket_generated") is False
        and record.get("production_candidate_generated") is False
        and record.get("broker_action_allowed") is False
        and record.get("broker_action_taken") is False
        and record.get("production_effect") == "none"
    )


def _shadow_aging_snapshot_errors(source_snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    if source_snapshot.get("snapshot_schema_version") != SHADOW_AGING_SNAPSHOT_SCHEMA_VERSION:
        errors.append("snapshot_schema_invalid")
    cutoff = _parse_datetime_text(_text(source_snapshot.get("generated_cutoff")))
    if cutoff is None:
        errors.append("generated_cutoff_invalid")
    config_snapshot = _mapping(source_snapshot.get("config"))
    try:
        config_path = Path(_text(config_snapshot.get("path")))
        current_config = load_paper_portfolio_config(config_path)
        if (
            config_snapshot.get("checksum") != _file_sha256(config_path)
            or config_snapshot.get("promotion_clock_v2")
            != _shadow_aging_policy(current_config)
            or config_snapshot.get("simulation_cost_rate")
            != _outcome_cost_rate(current_config)
        ):
            errors.append("config_snapshot_source_mismatch")
        _shadow_aging_policy(
            {
                "promotion_clock_v2": _mapping(
                    config_snapshot.get("promotion_clock_v2")
                )
            }
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(f"policy_invalid:{exc}")
    candidate_ids = _texts(source_snapshot.get("candidate_ids"))
    if not candidate_ids or len(candidate_ids) != len(set(candidate_ids)):
        errors.append("candidate_ids_invalid_or_duplicate")
    shortlist = _mapping(source_snapshot.get("shortlist"))
    shortlist_rows = _records(shortlist.get("candidate_rows"))
    if (
        shortlist.get("validation_status") != "PASS"
        or sorted(_text(row.get("candidate_id")) for row in shortlist_rows)
        != sorted(candidate_ids)
    ):
        errors.append("shortlist_snapshot_invalid")
    try:
        shortlist_root = Path(_text(shortlist.get("source_root")))
        shortlist_generated = _parse_datetime_text(
            _text(_mapping(shortlist.get("manifest")).get("generated_at"))
        )
        if (
            shortlist_generated is None
            or (cutoff is not None and shortlist_generated > cutoff)
            or _read_json(shortlist_root / "shadow_shortlist_manifest.json")
            != _mapping(shortlist.get("manifest"))
            or _read_jsonl(shortlist_root / "shadow_shortlist_candidates.jsonl")
            != shortlist_rows
        ):
            errors.append("shortlist_snapshot_source_mismatch")
    except Exception as exc:  # noqa: BLE001
        errors.append(f"shortlist_snapshot_source_unreadable:{exc}")
    errors.extend(_shadow_aging_source_inventory_errors(shortlist.get("source_files"), "shortlist"))
    monitors = _records(source_snapshot.get("selected_monitors"))
    if source_snapshot.get("selected_monitor_count") != len(monitors):
        errors.append("monitor_count_mismatch")
    monitor_ids: set[str] = set()
    monitor_dates: set[str] = set()
    for index, monitor in enumerate(monitors, start=1):
        monitor_id = _text(monitor.get("monitor_run_id"))
        as_of = _text(monitor.get("as_of"))
        generated = _parse_datetime_text(_text(monitor.get("generated_at")))
        row_ids = [
            _text(row.get("candidate_id"))
            for row in _records(monitor.get("candidate_rows"))
        ]
        if (
            not monitor_id
            or monitor_id in monitor_ids
            or not as_of
            or as_of in monitor_dates
            or monitor.get("validation_status") != "PASS"
            or generated is None
            or (cutoff is not None and generated > cutoff)
            or sorted(row_ids) != sorted(candidate_ids)
        ):
            errors.append(f"monitor_snapshot_invalid:{index}")
        for row in _records(monitor.get("candidate_rows")):
            try:
                if _text(row.get("as_of")) != as_of:
                    raise DynamicV3PaperTrackingError("row as_of mismatch")
                _validated_outcome_weights(
                    _mapping(row.get("target_weights")), "snapshot monitor weights"
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"monitor_candidate_row_invalid:{index}:{exc}")
        try:
            monitor_root = Path(_text(monitor.get("source_root")))
            current_monitor_rows = [
                {
                    **row,
                    "target_weights": _validated_outcome_weights(
                        _mapping(row.get("target_weights")), "current monitor weights"
                    ),
                }
                for row in _read_jsonl(
                    monitor_root / "shadow_candidate_daily_results.jsonl"
                )
            ]
            if (
                _read_json(monitor_root / "shadow_monitor_manifest.json")
                != _mapping(monitor.get("manifest"))
                or current_monitor_rows != _records(monitor.get("candidate_rows"))
            ):
                errors.append(f"monitor_snapshot_source_mismatch:{index}")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"monitor_snapshot_source_unreadable:{index}:{exc}")
        errors.extend(
            _shadow_aging_source_inventory_errors(
                monitor.get("source_files"), f"monitor:{index}"
            )
        )
        monitor_ids.add(monitor_id)
        monitor_dates.add(as_of)
    drifts = _records(source_snapshot.get("selected_drifts"))
    if source_snapshot.get("selected_drift_count") != len(drifts):
        errors.append("drift_count_mismatch")
    drift_monitors: set[str] = set()
    for index, drift in enumerate(drifts, start=1):
        monitor_id = _text(drift.get("monitor_run_id"))
        generated = _parse_datetime_text(_text(drift.get("generated_at")))
        if (
            monitor_id not in monitor_ids
            or monitor_id in drift_monitors
            or drift.get("validation_status") != "PASS"
            or generated is None
            or (cutoff is not None and generated > cutoff)
        ):
            errors.append(f"drift_snapshot_invalid:{index}")
        try:
            drift_root = Path(_text(drift.get("source_root")))
            if (
                _read_json(drift_root / "consensus_drift_manifest.json")
                != _mapping(drift.get("manifest"))
                or _read_json(drift_root / "consensus_drift_summary.json")
                != _mapping(drift.get("summary"))
            ):
                errors.append(f"drift_snapshot_source_mismatch:{index}")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"drift_snapshot_source_unreadable:{index}:{exc}")
        errors.extend(
            _shadow_aging_source_inventory_errors(
                drift.get("source_files"), f"drift:{index}"
            )
        )
        drift_monitors.add(monitor_id)
    outcomes = _records(source_snapshot.get("selected_outcomes"))
    if source_snapshot.get("selected_outcome_count") != len(outcomes):
        errors.append("outcome_count_mismatch")
    daily_ids: set[str] = set()
    for index, outcome in enumerate(outcomes, start=1):
        daily_id = _text(outcome.get("daily_advisory_id"))
        tracked = _parse_datetime_text(_text(outcome.get("tracked_at")))
        updates = _records(outcome.get("update_events"))
        previous_checksum = "GENESIS"
        if (
            not daily_id
            or daily_id in daily_ids
            or outcome.get("validation_status") != "PASS"
            or tracked is None
            or (cutoff is not None and tracked > cutoff)
        ):
            errors.append(f"outcome_snapshot_invalid:{index}")
        for sequence, update in enumerate(updates, start=1):
            event_at = _parse_datetime_text(_text(update.get("event_at")))
            if (
                update.get("event_sequence") != sequence
                or update.get("previous_event_checksum") != previous_checksum
                or update.get("event_checksum") != _outcome_update_event_checksum(update)
                or event_at is None
                or (cutoff is not None and event_at > cutoff)
            ):
                errors.append(f"outcome_update_chain_invalid:{index}:{sequence}")
            previous_checksum = _text(update.get("event_checksum"))
        target_ids = [
            _text(row.get("candidate_id")) for row in _records(outcome.get("candidate_targets"))
        ]
        if len(target_ids) != len(set(target_ids)) or not set(target_ids) <= set(candidate_ids):
            errors.append(f"outcome_candidate_targets_invalid:{index}")
        for target in _records(outcome.get("candidate_targets")):
            try:
                _validated_outcome_weights(
                    _mapping(target.get("target_weights")), "snapshot candidate target"
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"outcome_candidate_weights_invalid:{index}:{exc}")
        try:
            outcome_root = Path(_text(outcome.get("source_root")))
            current_targets = [
                {
                    "candidate_id": _text(row.get("candidate_id")),
                    "target_weights": _validated_outcome_weights(
                        _mapping(row.get("target_weights")), "current outcome target"
                    ),
                }
                for row in _read_jsonl(Path(_text(outcome.get("candidate_targets_path"))))
                if _text(row.get("candidate_id")) in set(candidate_ids)
            ]
            if (
                _read_json(outcome_root / "advisory_outcome_manifest.json")
                != _mapping(outcome.get("manifest"))
                or _read_json(outcome_root / "advisory_event.json")
                != _mapping(outcome.get("advisory_event"))
                or _read_jsonl(outcome_root / "outcome_update_events.jsonl")
                != updates
                or _read_jsonl(outcome_root / "outcome_windows.jsonl")
                != _records(outcome.get("outcome_windows"))
                or sorted(current_targets, key=lambda row: row["candidate_id"])
                != _records(outcome.get("candidate_targets"))
            ):
                errors.append(f"outcome_snapshot_source_mismatch:{index}")
            expected_price_rows: list[dict[str, Any]] = []
            if updates:
                current_price_path = Path(_text(updates[-1].get("price_snapshot_path")))
                if current_price_path.is_file():
                    current_prices = _read_outcome_price_snapshot(current_price_path)
                    expected_price_rows = [
                        {
                            "date": value["_date"].isoformat(),
                            "symbol": _text(value["symbol"]),
                            "adj_close": _float(value["_adj_close"]),
                        }
                        for value in current_prices.to_dict(orient="records")
                    ]
            if expected_price_rows != _records(outcome.get("latest_price_rows")):
                errors.append(f"outcome_price_snapshot_mismatch:{index}")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"outcome_snapshot_source_unreadable:{index}:{exc}")
        errors.extend(
            _shadow_aging_source_inventory_errors(
                outcome.get("source_files"), f"outcome:{index}"
            )
        )
        daily_ids.add(daily_id)
    if not _shadow_aging_record_has_no_execution_effect(source_snapshot):
        errors.append("snapshot_execution_effect_forbidden")
    return sorted(set(errors))


def _shadow_aging_source_inventory_errors(value: Any, label: str) -> list[str]:
    inventory = _mapping(value)
    if not inventory:
        return [f"source_inventory_missing:{label}"]
    errors = _source_inventory_errors(inventory, label)
    for relative, raw in inventory.items():
        entry = _mapping(raw)
        path = Path(_text(entry.get("path")))
        if not path.is_file() or entry.get("checksum") != _file_sha256(path):
            errors.append(f"source_inventory_drift:{label}:{relative}")
    return errors


def _weekly_advisory_policy(config: Mapping[str, Any]) -> dict[str, Any]:
    raw = _mapping(config.get("weekly_review_v2"))
    required_text = (
        "policy_id",
        "owner",
        "version",
        "status",
        "rationale",
        "intended_effect",
        "review_condition",
    )
    missing = [field for field in required_text if not _text(raw.get(field))]
    if missing:
        raise DynamicV3PaperTrackingError(
            "weekly review policy metadata is incomplete: " + ",".join(missing)
        )
    integer_fields = (
        "minimum_shadow_monitor_run_count",
        "minimum_daily_advisory_count",
        "minimum_owner_review_count",
        "minimum_available_outcome_window_count",
        "max_downgrade_recommended_count",
    )
    normalized = {field: _text(raw.get(field)) for field in required_text}
    for field in integer_fields:
        value = _strict_finite_number(raw.get(field), field)
        if value < 0 or not float(value).is_integer():
            raise DynamicV3PaperTrackingError(
                f"weekly review policy {field} must be a non-negative integer"
            )
        normalized[field] = int(value)
    for field in ("require_active_paper_portfolio", "require_shadow_aging"):
        if not isinstance(raw.get(field), bool):
            raise DynamicV3PaperTrackingError(
                f"weekly review policy {field} must be boolean"
            )
        normalized[field] = raw.get(field)
    precedence = _texts(raw.get("recommendation_precedence"))
    required_precedence = {
        "reduce_shortlist",
        "manual_review_required",
        "continue_monitoring",
    }
    if len(precedence) != 3 or set(precedence) != required_precedence:
        raise DynamicV3PaperTrackingError(
            "weekly review recommendation precedence must contain each governed action once"
        )
    normalized["recommendation_precedence"] = precedence
    return normalized


def _weekly_advisory_source_snapshot(
    *,
    week_start: date,
    week_ending: date,
    generated_at: datetime,
    config_path: Path,
    shadow_monitor_run_dir: Path,
    daily_advisory_dir: Path,
    owner_review_dir: Path,
    paper_portfolio_dir: Path,
    advisory_outcome_dir: Path,
    shadow_aging_dir: Path,
) -> dict[str, Any]:
    generated = generated_at.astimezone(UTC)
    period_end = datetime.combine(week_ending, time.max, tzinfo=UTC)
    cutoff = min(generated, period_end)
    resolved_config = _resolve_project_path(config_path)
    config = load_paper_portfolio_config(resolved_config)
    policy = _weekly_advisory_policy(config)
    advisories = _weekly_daily_advisory_sources(
        root=daily_advisory_dir,
        week_start=week_start,
        week_ending=week_ending,
        cutoff=cutoff,
    )
    monitors = _weekly_monitor_sources(
        root=shadow_monitor_run_dir,
        week_start=week_start,
        week_ending=week_ending,
        cutoff=cutoff,
        advisories=advisories,
    )
    owner_prefix, owner_reviews, owner_inventory = _weekly_owner_review_sources(
        root=owner_review_dir,
        week_start=week_start,
        week_ending=week_ending,
        cutoff=cutoff,
        advisories=advisories,
    )
    paper = _weekly_paper_source(
        root=paper_portfolio_dir,
        week_ending=week_ending,
        cutoff=cutoff,
    )
    outcomes = _weekly_outcome_sources(
        root=advisory_outcome_dir,
        cutoff=cutoff,
        advisories=advisories,
    )
    aging = _weekly_aging_source(root=shadow_aging_dir, cutoff=cutoff)
    return {
        "schema_version": SCHEMA_VERSION,
        "snapshot_schema_version": WEEKLY_ADVISORY_REVIEW_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weekly_advisory_review_source_snapshot",
        "cadence": "weekly_manual_or_controlled_date_gate",
        "market_regime": "ai_after_chatgpt",
        "week_start": week_start.isoformat(),
        "week_ending": week_ending.isoformat(),
        "generated_at": generated.isoformat(),
        "evidence_cutoff": cutoff.isoformat(),
        "config_source": {
            "path": str(resolved_config),
            "checksum": _file_sha256(resolved_config),
            "policy": policy,
        },
        "source_roots": {
            "shadow_monitor_run": str(shadow_monitor_run_dir),
            "daily_advisory": str(daily_advisory_dir),
            "owner_review": str(owner_review_dir),
            "paper_portfolio": str(paper_portfolio_dir),
            "advisory_outcome": str(advisory_outcome_dir),
            "shadow_aging": str(shadow_aging_dir),
        },
        "selected_monitors": monitors,
        "selected_daily_advisories": advisories,
        "owner_review_event_prefix": owner_prefix,
        "selected_owner_reviews": owner_reviews,
        "owner_review_source_files": owner_inventory,
        "selected_paper_portfolio": paper,
        "selected_outcomes": outcomes,
        "selected_shadow_aging": aging,
        "direct_cached_data_read": False,
        "data_quality_gate_required": False,
        "official_target_weights_generated": False,
        "portfolio_mutated": False,
        "order_ticket_generated": False,
        "production_candidate_generated": False,
        "automatic_candidate_promotion": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_effect": "none",
    }


def _weekly_monitor_sources(
    *,
    root: Path,
    week_start: date,
    week_ending: date,
    cutoff: datetime,
    advisories: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    selected = []
    required = {
        _text(item.get("monitor_run_id")): _text(item.get("as_of"))
        for item in advisories
    }
    if "" in required:
        raise DynamicV3PaperTrackingError("weekly daily advisory monitor id is missing")
    for monitor_id, expected_as_of in sorted(required.items()):
        child = root / monitor_id
        manifest_path = child / "shadow_monitor_manifest.json"
        if not manifest_path.is_file():
            raise DynamicV3PaperTrackingError(
                f"weekly referenced monitor is missing: {monitor_id}"
            )
        manifest = _read_json(manifest_path)
        as_of = _date_from_any(manifest.get("as_of"))
        if (
            as_of is None
            or not week_start <= as_of <= week_ending
            or as_of.isoformat() != expected_as_of
        ):
            raise DynamicV3PaperTrackingError(
                f"weekly referenced monitor as-of mismatch: {monitor_id}"
            )
        generated = _parse_datetime_text(_text(manifest.get("generated_at")))
        if generated is None:
            raise DynamicV3PaperTrackingError(
                f"weekly monitor generated_at is invalid: {child.name}"
            )
        if generated > cutoff:
            raise DynamicV3PaperTrackingError(
                f"weekly referenced monitor exceeds evidence cutoff: {monitor_id}"
            )
        manifest_id = _text(manifest.get("monitor_run_id"))
        if manifest_id != monitor_id:
            raise DynamicV3PaperTrackingError("weekly monitor id/path mismatch")
        validation = validate_shadow_monitor_run_artifact(
            monitor_run_id=monitor_id, output_dir=root
        )
        if validation.get("status") != "PASS":
            raise DynamicV3PaperTrackingError(
                f"weekly monitor validation must PASS: {monitor_id}"
            )
        selected.append(
            {
                "monitor_run_id": monitor_id,
                "as_of": as_of.isoformat(),
                "generated_at": generated.isoformat(),
                "manifest": manifest,
                "validation_status": "PASS",
                "source_root": str(child),
                "source_files": _source_file_inventory(
                    child, [path for path in child.rglob("*") if path.is_file()]
                ),
            }
        )
    ids = [_text(item.get("monitor_run_id")) for item in selected]
    dates = [_text(item.get("as_of")) for item in selected]
    if len(ids) != len(set(ids)) or len(dates) != len(set(dates)):
        raise DynamicV3PaperTrackingError(
            "weekly monitor ids and as-of dates must be unique"
        )
    return selected


def _weekly_daily_advisory_sources(
    *,
    root: Path,
    week_start: date,
    week_ending: date,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    selected = []
    for child in sorted(path for path in root.glob("*") if path.is_dir()):
        manifest_path = child / "daily_advisory_manifest.json"
        if not manifest_path.is_file():
            continue
        manifest = _read_json(manifest_path)
        as_of = _date_from_any(manifest.get("as_of"))
        if as_of is None or not week_start <= as_of <= week_ending:
            continue
        generated = _parse_datetime_text(_text(manifest.get("generated_at")))
        if generated is None:
            raise DynamicV3PaperTrackingError(
                f"weekly daily advisory generated_at is invalid: {child.name}"
            )
        if generated > cutoff:
            continue
        daily_id = _text(manifest.get("daily_advisory_id"))
        monitor_id = _text(manifest.get("source_shadow_monitor_run_id"))
        if daily_id != child.name or not monitor_id:
            raise DynamicV3PaperTrackingError(
                "weekly daily advisory id or monitor lineage is missing"
            )
        validation = validate_position_advisory_daily_artifact(
            daily_advisory_id=daily_id, output_dir=root
        )
        if validation.get("status") != "PASS":
            raise DynamicV3PaperTrackingError(
                f"weekly daily advisory validation must PASS: {daily_id}"
            )
        actions = _read_json(child / "daily_advisory_actions.json")
        selected.append(
            {
                "daily_advisory_id": daily_id,
                "monitor_run_id": monitor_id,
                "as_of": as_of.isoformat(),
                "generated_at": generated.isoformat(),
                "manifest": manifest,
                "actions": actions,
                "validation_status": "PASS",
                "source_root": str(child),
                "source_files": _source_file_inventory(
                    child, [path for path in child.rglob("*") if path.is_file()]
                ),
            }
        )
    ids = [_text(item.get("daily_advisory_id")) for item in selected]
    dates = [_text(item.get("as_of")) for item in selected]
    if len(ids) != len(set(ids)) or len(dates) != len(set(dates)):
        raise DynamicV3PaperTrackingError(
            "weekly daily advisory ids and as-of dates must be unique"
        )
    return selected


def _weekly_owner_review_sources(
    *,
    root: Path,
    week_start: date,
    week_ending: date,
    cutoff: datetime,
    advisories: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    events_path = root / "owner_review_events.jsonl"
    if not events_path.is_file():
        return [], [], {}
    events = _read_jsonl(events_path)
    full_records, full_errors = replay_owner_review_records(events)
    if full_errors:
        raise DynamicV3PaperTrackingError(
            "weekly owner review event chain is invalid: " + ",".join(full_errors)
        )
    journal_path = root / "owner_review_journal.jsonl"
    if not journal_path.is_file() or _read_jsonl(journal_path) != full_records:
        raise DynamicV3PaperTrackingError(
            "weekly owner review journal does not match event replay"
        )
    prefix = []
    for event in events:
        event_at = _parse_datetime_text(_text(event.get("event_at")))
        if event_at is None:
            raise DynamicV3PaperTrackingError("weekly owner event time is invalid")
        if event_at <= cutoff:
            prefix.append(event)
    prefix_records, prefix_errors = replay_owner_review_records(prefix)
    if prefix_errors:
        raise DynamicV3PaperTrackingError(
            "weekly owner event prefix is invalid: " + ",".join(prefix_errors)
        )
    advisory_by_id = {
        _text(item.get("daily_advisory_id")): item for item in advisories
    }
    selected = []
    daily_ids: set[str] = set()
    for record in prefix_records:
        as_of = _date_from_any(record.get("as_of"))
        if as_of is None or not week_start <= as_of <= week_ending:
            continue
        review_id = _text(record.get("review_id"))
        daily_id = _text(record.get("daily_advisory_id"))
        daily = _mapping(advisory_by_id.get(daily_id))
        if not daily or daily.get("as_of") != as_of.isoformat() or daily_id in daily_ids:
            raise DynamicV3PaperTrackingError(
                "weekly owner review daily lineage must be unique and selected"
            )
        validation = validate_owner_review_artifact(review_id=review_id, output_dir=root)
        if validation.get("status") != "PASS":
            raise DynamicV3PaperTrackingError(
                f"weekly owner review validation must PASS: {review_id}"
            )
        daily_ids.add(daily_id)
        selected.append(
            {
                "review_id": review_id,
                "daily_advisory_id": daily_id,
                "as_of": as_of.isoformat(),
                "review": record,
                "validation_status": "PASS",
            }
        )
    source_paths = [path for path in root.glob("*") if path.is_file()]
    return prefix, selected, _source_file_inventory(root, source_paths)


def _weekly_paper_source(
    *, root: Path, week_ending: date, cutoff: datetime
) -> dict[str, Any]:
    candidates = []
    for child in sorted(path for path in root.glob("*") if path.is_dir()):
        manifest_path = child / "paper_portfolio_manifest.json"
        if not manifest_path.is_file():
            continue
        manifest = _read_json(manifest_path)
        generated = _parse_datetime_text(_text(manifest.get("generated_at")))
        initial_as_of = _date_from_any(manifest.get("initial_as_of"))
        if generated is None or initial_as_of is None:
            raise DynamicV3PaperTrackingError("weekly paper manifest time is invalid")
        if generated > cutoff or initial_as_of > week_ending:
            continue
        paper_id = _text(manifest.get("paper_portfolio_id"))
        if paper_id != child.name:
            raise DynamicV3PaperTrackingError("weekly paper id/path mismatch")
        validation = validate_paper_portfolio_artifact(
            paper_portfolio_id=paper_id, output_dir=root
        )
        if validation.get("status") != "PASS":
            raise DynamicV3PaperTrackingError(
                f"weekly paper portfolio validation must PASS: {paper_id}"
            )
        events = _read_jsonl(child / "paper_action_ledger.jsonl")
        prefix = []
        for event in events:
            created = _parse_datetime_text(_text(event.get("created_at")))
            if created is None:
                raise DynamicV3PaperTrackingError("weekly paper event time is invalid")
            if created <= cutoff:
                prefix.append(event)
        config = load_paper_portfolio_config(Path(_text(manifest.get("config_path"))))
        state, history, errors = _replay_paper_action_events(
            manifest=manifest,
            events=prefix,
            config=config,
            validate_sources=True,
        )
        if errors:
            raise DynamicV3PaperTrackingError(
                "weekly paper event prefix replay failed: " + ",".join(errors)
            )
        candidates.append(
            {
                "paper_portfolio_id": paper_id,
                "generated_at": generated.isoformat(),
                "manifest": manifest,
                "event_prefix": prefix,
                "state": state,
                "history": history,
                "validation_status": "PASS",
                "source_root": str(child),
                "source_files": _source_file_inventory(
                    child, [path for path in child.rglob("*") if path.is_file()]
                ),
            }
        )
    if len(candidates) > 1:
        raise DynamicV3PaperTrackingError(
            "weekly review found ambiguous paper portfolio lineages"
        )
    return candidates[0] if candidates else {}


def _weekly_outcome_sources(
    *,
    root: Path,
    cutoff: datetime,
    advisories: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    daily_by_id = {
        _text(item.get("daily_advisory_id")): item for item in advisories
    }
    selected: dict[str, dict[str, Any]] = {}
    for child in sorted(path for path in root.glob("*") if path.is_dir()):
        manifest_path = child / "advisory_outcome_manifest.json"
        event_path = child / "advisory_event.json"
        if not manifest_path.is_file() or not event_path.is_file():
            continue
        manifest = _read_json(manifest_path)
        daily_id = _text(manifest.get("daily_advisory_id"))
        if daily_id not in daily_by_id:
            continue
        event = _read_json(event_path)
        tracked = _parse_datetime_text(_text(event.get("tracked_at")))
        if tracked is None:
            raise DynamicV3PaperTrackingError("weekly outcome tracked_at is invalid")
        if tracked > cutoff:
            continue
        if daily_id in selected:
            raise DynamicV3PaperTrackingError(
                f"weekly review found duplicate outcomes for daily advisory: {daily_id}"
            )
        outcome_id = child.name
        validation = validate_advisory_outcome_artifact(
            outcome_id=outcome_id, output_dir=root
        )
        if validation.get("status") != "PASS":
            raise DynamicV3PaperTrackingError(
                f"weekly outcome validation must PASS: {outcome_id}"
            )
        daily = _mapping(daily_by_id.get(daily_id))
        if (
            event.get("daily_advisory_id") != daily_id
            or event.get("as_of") != daily.get("as_of")
            or manifest.get("as_of") != daily.get("as_of")
        ):
            raise DynamicV3PaperTrackingError("weekly outcome daily lineage mismatch")
        updates = _read_jsonl(child / "outcome_update_events.jsonl")
        prefix = []
        for update in updates:
            event_at = _parse_datetime_text(_text(update.get("event_at")))
            if event_at is None:
                raise DynamicV3PaperTrackingError("weekly outcome update time is invalid")
            if event_at <= cutoff:
                prefix.append(update)
        if prefix:
            windows = _records(prefix[-1].get("outcome_windows"))
        else:
            frozen_config = _mapping(
                _mapping(event.get("frozen_track_sources")).get("outcome_config")
            )
            frozen_path = Path(_text(frozen_config.get("path")))
            outcome_config = load_paper_portfolio_config(frozen_path)
            windows = [
                _pending_outcome_window(
                    daily_advisory_id=daily_id,
                    window_days=window,
                    start_date=_text(event.get("as_of")),
                )
                for window in _configured_outcome_windows(outcome_config)
            ]
        selected[daily_id] = {
            "outcome_id": outcome_id,
            "daily_advisory_id": daily_id,
            "as_of": daily.get("as_of"),
            "tracked_at": tracked.isoformat(),
            "manifest": manifest,
            "advisory_event": event,
            "update_event_prefix": prefix,
            "outcome_windows": windows,
            "validation_status": "PASS",
            "source_root": str(child),
            "source_files": _source_file_inventory(
                child, [path for path in child.rglob("*") if path.is_file()]
            ),
        }
    return [selected[key] for key in sorted(selected)]


def _weekly_aging_source(*, root: Path, cutoff: datetime) -> dict[str, Any]:
    candidates = []
    for child in sorted(path for path in root.glob("*") if path.is_dir()):
        manifest_path = child / "shadow_aging_manifest.json"
        if not manifest_path.is_file():
            continue
        manifest = _read_json(manifest_path)
        generated = _parse_datetime_text(_text(manifest.get("generated_at")))
        if generated is None:
            raise DynamicV3PaperTrackingError("weekly aging generated_at is invalid")
        if generated > cutoff:
            continue
        aging_id = _text(manifest.get("aging_id"))
        if aging_id != child.name:
            raise DynamicV3PaperTrackingError("weekly aging id/path mismatch")
        validation = validate_shadow_aging_artifact(aging_id=aging_id, output_dir=root)
        if validation.get("status") != "PASS":
            raise DynamicV3PaperTrackingError(
                f"weekly shadow aging validation must PASS: {aging_id}"
            )
        candidates.append(
            {
                "aging_id": aging_id,
                "generated_at": generated.isoformat(),
                "manifest": manifest,
                "summary": _read_json(child / "promotion_clock_v2_summary.json"),
                "validation_status": "PASS",
                "source_root": str(child),
                "source_files": _source_file_inventory(
                    child, [path for path in child.rglob("*") if path.is_file()]
                ),
            }
        )
    if not candidates:
        return {}
    candidates.sort(key=lambda item: (_text(item.get("generated_at")), _text(item.get("aging_id"))))
    latest_time = _text(candidates[-1].get("generated_at"))
    if sum(_text(item.get("generated_at")) == latest_time for item in candidates) > 1:
        raise DynamicV3PaperTrackingError(
            "weekly review found ambiguous latest shadow aging artifacts"
        )
    return candidates[-1]


def _weekly_advisory_summary_from_snapshot(
    source_snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    monitors = _records(source_snapshot.get("selected_monitors"))
    advisories = _records(source_snapshot.get("selected_daily_advisories"))
    outcomes = _records(source_snapshot.get("selected_outcomes"))
    rows = [
        row
        for outcome in outcomes
        for row in _records(outcome.get("outcome_windows"))
    ]
    available = [row for row in rows if row.get("outcome_status") == "AVAILABLE"]

    def optional_average(field: str) -> float | None:
        values = [float(row[field]) for row in available if _is_finite_number(row.get(field))]
        return round(sum(values) / len(values), 6) if values else None

    pending = sum(row.get("outcome_status") == "PENDING" for row in rows)
    insufficient = sum(row.get("outcome_status") == "INSUFFICIENT_DATA" for row in rows)
    outcome_status = (
        "AVAILABLE"
        if available
        else "PENDING"
        if pending
        else "INSUFFICIENT_DATA"
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weekly_advisory_summary",
        "week_start": source_snapshot.get("week_start"),
        "week_ending": source_snapshot.get("week_ending"),
        "shadow_monitor_run_count": len(monitors),
        "daily_advisory_count": len(advisories),
        "recommended_actions": dict(
            sorted(
                Counter(
                    _text(_mapping(item.get("actions")).get("recommended_action"), "MISSING")
                    for item in advisories
                ).items()
            )
        ),
        "outcome_summary": {
            "status": outcome_status,
            "linked_outcome_count": len(outcomes),
            "window_count": len(rows),
            "available_window_count": len(available),
            "pending_window_count": pending,
            "insufficient_window_count": insufficient,
            "avg_relative_to_no_trade": optional_average("relative_to_no_trade"),
            "avg_relative_to_baseline": optional_average("relative_to_baseline"),
        },
    }


def _weekly_paper_summary_from_snapshot(source_snapshot: Mapping[str, Any]) -> dict[str, Any]:
    paper = _mapping(source_snapshot.get("selected_paper_portfolio"))
    if not paper:
        return {
            "status": "MISSING",
            "state_status": "MISSING",
            "paper_portfolio_count": 0,
            "broker_action_taken": False,
            "production_effect": "none",
        }
    state = _mapping(paper.get("state"))
    return {
        "status": "AVAILABLE",
        "paper_portfolio_count": 1,
        "paper_portfolio_id": paper.get("paper_portfolio_id"),
        "state_status": state.get("state_status", "UNKNOWN"),
        "as_of": state.get("as_of", ""),
        "positions": state.get("positions", {}),
        "total_weight": state.get("total_weight", 0.0),
        "event_prefix_count": len(_records(paper.get("event_prefix"))),
        "broker_action_taken": False,
        "production_effect": "none",
    }


def _weekly_aging_summary_from_snapshot(source_snapshot: Mapping[str, Any]) -> dict[str, Any]:
    aging = _mapping(source_snapshot.get("selected_shadow_aging"))
    if not aging:
        return {
            "status": "MISSING",
            "aging_artifact_count": 0,
            "eligible_for_review_count": 0,
            "downgrade_recommended_count": 0,
            "broker_action_taken": False,
            "production_effect": "none",
        }
    return {
        **_mapping(aging.get("summary")),
        "aging_artifact_count": 1,
        "selected_aging_id": aging.get("aging_id"),
    }


def _weekly_source_validation_summary(source_snapshot: Mapping[str, Any]) -> dict[str, Any]:
    monitors = _records(source_snapshot.get("selected_monitors"))
    advisories = _records(source_snapshot.get("selected_daily_advisories"))
    reviews = _records(source_snapshot.get("selected_owner_reviews"))
    outcomes = _records(source_snapshot.get("selected_outcomes"))
    paper = _mapping(source_snapshot.get("selected_paper_portfolio"))
    aging = _mapping(source_snapshot.get("selected_shadow_aging"))
    dq_statuses = sorted(
        {
            _text(event.get("data_quality_status"))
            for outcome in outcomes
            for event in _records(outcome.get("update_event_prefix"))
            if _text(event.get("data_quality_status"))
        }
    )
    selected = [*monitors, *advisories, *reviews, *outcomes]
    if paper:
        selected.append(paper)
    if aging:
        selected.append(aging)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weekly_advisory_source_validation_summary",
        "snapshot_schema_version": WEEKLY_ADVISORY_REVIEW_SNAPSHOT_SCHEMA_VERSION,
        "week_start": source_snapshot.get("week_start"),
        "week_ending": source_snapshot.get("week_ending"),
        "evidence_cutoff": source_snapshot.get("evidence_cutoff"),
        "selected_monitor_count": len(monitors),
        "selected_daily_advisory_count": len(advisories),
        "selected_owner_review_count": len(reviews),
        "selected_paper_portfolio_count": int(bool(paper)),
        "selected_outcome_count": len(outcomes),
        "selected_shadow_aging_count": int(bool(aging)),
        "all_selected_sources_validated": all(
            item.get("validation_status") == "PASS" for item in selected
        ),
        "direct_cached_data_read": False,
        "data_quality_gate_required": False,
        "data_quality_status": (
            "INHERITED_FROM_VALIDATED_OUTCOME_SOURCES"
            if dq_statuses
            else "NOT_REQUIRED_NO_DIRECT_CACHE_READ"
        ),
        "source_data_quality_statuses": dq_statuses,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_effect": "none",
    }


def _weekly_review_decision(
    *,
    source_snapshot: Mapping[str, Any],
    advisory_summary: Mapping[str, Any],
    paper_summary: Mapping[str, Any],
    aging_summary: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _mapping(_mapping(source_snapshot.get("config_source")).get("policy"))
    owner_count = len(_records(source_snapshot.get("selected_owner_reviews")))
    available_windows = _int(
        _mapping(advisory_summary.get("outcome_summary")).get("available_window_count")
    )
    downgrade_count = _int(aging_summary.get("downgrade_recommended_count"))
    blockers = []
    if _int(advisory_summary.get("shadow_monitor_run_count")) < _int(
        policy.get("minimum_shadow_monitor_run_count")
    ):
        blockers.append("INSUFFICIENT_SHADOW_MONITOR_RUNS")
    if _int(advisory_summary.get("daily_advisory_count")) < _int(
        policy.get("minimum_daily_advisory_count")
    ):
        blockers.append("INSUFFICIENT_DAILY_ADVISORIES")
    if owner_count < _int(policy.get("minimum_owner_review_count")):
        blockers.append("INSUFFICIENT_OWNER_REVIEWS")
    if available_windows < _int(policy.get("minimum_available_outcome_window_count")):
        blockers.append("INSUFFICIENT_AVAILABLE_OUTCOME_WINDOWS")
    if policy.get("require_active_paper_portfolio") is True and paper_summary.get(
        "state_status"
    ) != "ACTIVE":
        blockers.append("ACTIVE_PAPER_PORTFOLIO_MISSING")
    if policy.get("require_shadow_aging") is True and _int(
        aging_summary.get("aging_artifact_count")
    ) != 1:
        blockers.append("SHADOW_AGING_MISSING")
    downgrade = downgrade_count > _int(policy.get("max_downgrade_recommended_count"))
    if downgrade:
        blockers.append("DOWNGRADE_RECOMMENDED_COUNT_EXCEEDED")
    active = {
        "reduce_shortlist": downgrade,
        "manual_review_required": bool(blockers),
        "continue_monitoring": not blockers,
    }
    recommendation = next(
        action
        for action in _texts(policy.get("recommendation_precedence"))
        if active[action]
    )
    evidence_status = (
        "DOWNGRADE_REVIEW_REQUIRED"
        if downgrade
        else "INSUFFICIENT_EVIDENCE"
        if blockers
        else "COMPLETE_EVIDENCE"
    )
    next_actions = [recommendation]
    if paper_summary.get("state_status") != "ACTIVE":
        next_actions.append("update_position_snapshot")
    if blockers and "manual_review_required" not in next_actions:
        next_actions.append("manual_review_required")
    return {
        "evidence_status": evidence_status,
        "weekly_recommendation": recommendation,
        "blocking_reasons": blockers,
        "next_actions": next_actions,
    }


def _weekly_review_manifest_payload(
    *,
    artifact_dir: Path,
    generated_at: datetime,
    source_snapshot: Mapping[str, Any],
    source_validation_summary: Mapping[str, Any],
    advisory_summary: Mapping[str, Any],
    owner_summary: Mapping[str, Any],
    paper_summary: Mapping[str, Any],
    aging_summary: Mapping[str, Any],
    decision: Mapping[str, Any],
) -> dict[str, Any]:
    source_snapshot_path = artifact_dir / "weekly_review_source_snapshot.json"
    source_validation_path = artifact_dir / "source_validation_summary.json"
    policy = _mapping(_mapping(source_snapshot.get("config_source")).get("policy"))
    return {
        "schema_version": SCHEMA_VERSION,
        "source_snapshot_schema_version": WEEKLY_ADVISORY_REVIEW_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weekly_advisory_review_manifest",
        "weekly_review_id": artifact_dir.name,
        "cadence": source_snapshot.get("cadence"),
        "market_regime": source_snapshot.get("market_regime"),
        "week_start": source_snapshot.get("week_start"),
        "week_ending": source_snapshot.get("week_ending"),
        "requested_date_range": {
            "start": source_snapshot.get("week_start"),
            "end": source_snapshot.get("week_ending"),
        },
        "actual_date_range": {
            "start": source_snapshot.get("week_start"),
            "end": source_snapshot.get("week_ending"),
        },
        "generated_at": generated_at.astimezone(UTC).isoformat(),
        "evidence_cutoff": source_snapshot.get("evidence_cutoff"),
        "status": "PASS",
        "evidence_status": decision.get("evidence_status"),
        "weekly_recommendation": decision.get("weekly_recommendation"),
        "blocking_reasons": _texts(decision.get("blocking_reasons")),
        "next_actions": _texts(decision.get("next_actions")),
        "paper_portfolio_status": paper_summary.get("state_status", "MISSING"),
        "owner_review_count": owner_summary.get("total_reviews", 0),
        "shadow_monitor_run_count": advisory_summary.get("shadow_monitor_run_count", 0),
        "daily_advisory_count": advisory_summary.get("daily_advisory_count", 0),
        "linked_outcome_count": _mapping(advisory_summary.get("outcome_summary")).get(
            "linked_outcome_count", 0
        ),
        "available_outcome_window_count": _mapping(
            advisory_summary.get("outcome_summary")
        ).get("available_window_count", 0),
        "weekly_policy_id": policy.get("policy_id"),
        "weekly_policy_version": policy.get("version"),
        "source_snapshot_path": str(source_snapshot_path),
        "source_snapshot_checksum": _file_sha256(source_snapshot_path),
        "source_validation_summary_path": str(source_validation_path),
        "source_validation_summary_checksum": _file_sha256(source_validation_path),
        "data_quality_status": source_validation_summary.get("data_quality_status"),
        "source_data_quality_statuses": source_validation_summary.get(
            "source_data_quality_statuses", []
        ),
        "weekly_advisory_summary_path": str(artifact_dir / "weekly_advisory_summary.json"),
        "weekly_owner_decision_summary_path": str(
            artifact_dir / "weekly_owner_decision_summary.json"
        ),
        "weekly_paper_portfolio_summary_path": str(
            artifact_dir / "weekly_paper_portfolio_summary.json"
        ),
        "weekly_shadow_candidate_summary_path": str(
            artifact_dir / "weekly_shadow_candidate_summary.json"
        ),
        "weekly_review_report_path": str(artifact_dir / "weekly_review_report.md"),
        "reader_brief_section_path": str(artifact_dir / "reader_brief_section.md"),
        "direct_cached_data_read": False,
        "data_quality_gate_required": False,
        "production_candidate_generated": False,
        "automatic_candidate_promotion": False,
        "official_target_weights_generated": False,
        "portfolio_mutated": False,
        "order_ticket_generated": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "manual_review_required": True,
        "production_effect": "none",
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def _weekly_review_record_has_no_execution_effect(record: Mapping[str, Any]) -> bool:
    return (
        record.get("production_candidate_generated") is False
        and record.get("automatic_candidate_promotion") is False
        and record.get("official_target_weights_generated") is False
        and record.get("portfolio_mutated") is False
        and record.get("order_ticket_generated") is False
        and record.get("broker_action_allowed") is False
        and record.get("broker_action_taken") is False
        and record.get("production_effect") == "none"
    )


def _weekly_advisory_snapshot_errors(source_snapshot: Mapping[str, Any]) -> list[str]:
    errors = []
    try:
        if (
            source_snapshot.get("snapshot_schema_version")
            != WEEKLY_ADVISORY_REVIEW_SNAPSHOT_SCHEMA_VERSION
        ):
            errors.append("snapshot_schema_invalid")
        week_start = _date_from_any(source_snapshot.get("week_start"))
        week_ending = _date_from_any(source_snapshot.get("week_ending"))
        generated = _parse_datetime_text(_text(source_snapshot.get("generated_at")))
        cutoff = _parse_datetime_text(_text(source_snapshot.get("evidence_cutoff")))
        if (
            week_start is None
            or week_ending is None
            or (week_ending - week_start).days != 6
            or generated is None
            or cutoff is None
            or cutoff > generated
            or cutoff > datetime.combine(week_ending, time.max, tzinfo=UTC)
        ):
            errors.append("week_or_cutoff_invalid")
        config_source = _mapping(source_snapshot.get("config_source"))
        config_checksum = _text(config_source.get("checksum"))
        frozen_policy = _mapping(config_source.get("policy"))
        if (
            not _text(config_source.get("path"))
            or len(config_checksum) != 64
            or any(character not in "0123456789abcdef" for character in config_checksum)
            or _weekly_advisory_policy({"weekly_review_v2": frozen_policy})
            != frozen_policy
        ):
            errors.append("weekly_policy_snapshot_invalid")
        monitor_dates = []
        monitor_ids = set()
        for item in _records(source_snapshot.get("selected_monitors")):
            monitor_id = _text(item.get("monitor_run_id"))
            root = Path(_text(item.get("source_root"))).parent
            validation = validate_shadow_monitor_run_artifact(
                monitor_run_id=monitor_id, output_dir=root
            )
            current_manifest = _read_json(
                Path(_text(item.get("source_root"))) / "shadow_monitor_manifest.json"
            )
            if validation.get("status") != "PASS" or current_manifest != item.get("manifest"):
                errors.append(f"monitor_source_drift:{monitor_id}")
            monitor_ids.add(monitor_id)
            monitor_dates.append(_text(item.get("as_of")))
        if len(monitor_ids) != len(monitor_dates) or len(monitor_dates) != len(
            set(monitor_dates)
        ):
            errors.append("monitor_duplicate_id_or_as_of")
        daily_ids = set()
        daily_dates = []
        for item in _records(source_snapshot.get("selected_daily_advisories")):
            daily_id = _text(item.get("daily_advisory_id"))
            root = Path(_text(item.get("source_root"))).parent
            validation = validate_position_advisory_daily_artifact(
                daily_advisory_id=daily_id, output_dir=root
            )
            current_manifest = _read_json(
                Path(_text(item.get("source_root"))) / "daily_advisory_manifest.json"
            )
            if (
                validation.get("status") != "PASS"
                or current_manifest != item.get("manifest")
                or _text(item.get("monitor_run_id")) not in monitor_ids
            ):
                errors.append(f"daily_source_or_lineage_drift:{daily_id}")
            daily_ids.add(daily_id)
            daily_dates.append(_text(item.get("as_of")))
        if len(daily_ids) != len(daily_dates) or len(daily_dates) != len(
            set(daily_dates)
        ):
            errors.append("daily_duplicate_id_or_as_of")
        owner_prefix = _records(source_snapshot.get("owner_review_event_prefix"))
        selected_reviews = _records(source_snapshot.get("selected_owner_reviews"))
        owner_root = Path(_text(_mapping(source_snapshot.get("source_roots")).get("owner_review")))
        if owner_prefix or selected_reviews:
            current_events = _read_jsonl(owner_root / "owner_review_events.jsonl")
            if current_events[: len(owner_prefix)] != owner_prefix:
                errors.append("owner_event_prefix_drift")
            replayed, replay_errors = replay_owner_review_records(owner_prefix)
            selected_records = [_mapping(item.get("review")) for item in selected_reviews]
            replayed_selected = [
                record
                for record in replayed
                if _text(record.get("review_id"))
                in {_text(item.get("review_id")) for item in selected_reviews}
            ]
            if replay_errors or replayed_selected != selected_records:
                errors.append("owner_event_prefix_replay_mismatch")
            for item in selected_reviews:
                review_id = _text(item.get("review_id"))
                if (
                    validate_owner_review_artifact(
                        review_id=review_id, output_dir=owner_root
                    ).get("status")
                    != "PASS"
                    or _text(item.get("daily_advisory_id")) not in daily_ids
                ):
                    errors.append(f"owner_review_source_or_lineage_drift:{review_id}")
        paper = _mapping(source_snapshot.get("selected_paper_portfolio"))
        if paper:
            paper_root = Path(_text(paper.get("source_root")))
            paper_id = _text(paper.get("paper_portfolio_id"))
            if validate_paper_portfolio_artifact(
                paper_portfolio_id=paper_id, output_dir=paper_root.parent
            ).get("status") != "PASS":
                errors.append("paper_portfolio_source_invalid")
            current_events = _read_jsonl(paper_root / "paper_action_ledger.jsonl")
            prefix = _records(paper.get("event_prefix"))
            config = load_paper_portfolio_config(
                Path(_text(_mapping(paper.get("manifest")).get("config_path")))
            )
            state, history, replay_errors = _replay_paper_action_events(
                manifest=_mapping(paper.get("manifest")),
                events=prefix,
                config=config,
                validate_sources=True,
            )
            if (
                current_events[: len(prefix)] != prefix
                or replay_errors
                or state != paper.get("state")
                or history != paper.get("history")
            ):
                errors.append("paper_event_prefix_replay_mismatch")
        outcome_daily_ids = set()
        for outcome in _records(source_snapshot.get("selected_outcomes")):
            outcome_id = _text(outcome.get("outcome_id"))
            outcome_root = Path(_text(outcome.get("source_root")))
            daily_id = _text(outcome.get("daily_advisory_id"))
            if daily_id in outcome_daily_ids:
                errors.append(f"outcome_duplicate_daily:{daily_id}")
            outcome_daily_ids.add(daily_id)
            if (
                daily_id not in daily_ids
                or validate_advisory_outcome_artifact(
                    outcome_id=outcome_id, output_dir=outcome_root.parent
                ).get("status")
                != "PASS"
                or _read_json(outcome_root / "advisory_event.json")
                != outcome.get("advisory_event")
            ):
                errors.append(f"outcome_source_or_lineage_drift:{outcome_id}")
            current_updates = _read_jsonl(outcome_root / "outcome_update_events.jsonl")
            prefix = _records(outcome.get("update_event_prefix"))
            if current_updates[: len(prefix)] != prefix:
                errors.append(f"outcome_update_prefix_drift:{outcome_id}")
            for row in _records(outcome.get("outcome_windows")):
                status = row.get("outcome_status")
                if status not in OUTCOME_WINDOW_STATUSES:
                    errors.append(f"outcome_window_status_invalid:{outcome_id}")
                elif status == "AVAILABLE" and not all(
                    _is_finite_number(row.get(field)) for field in OUTCOME_METRIC_FIELDS
                ):
                    errors.append(f"outcome_available_metric_invalid:{outcome_id}")
                elif status != "AVAILABLE" and not all(
                    row.get(field) is None for field in OUTCOME_METRIC_FIELDS
                ):
                    errors.append(f"outcome_nonavailable_metric_not_null:{outcome_id}")
        aging = _mapping(source_snapshot.get("selected_shadow_aging"))
        if aging:
            aging_root = Path(_text(aging.get("source_root")))
            aging_id = _text(aging.get("aging_id"))
            if (
                validate_shadow_aging_artifact(
                    aging_id=aging_id, output_dir=aging_root.parent
                ).get("status")
                != "PASS"
                or _read_json(aging_root / "shadow_aging_manifest.json")
                != aging.get("manifest")
                or _read_json(aging_root / "promotion_clock_v2_summary.json")
                != aging.get("summary")
            ):
                errors.append("shadow_aging_source_drift")
        if not _weekly_review_record_has_no_execution_effect(source_snapshot):
            errors.append("snapshot_execution_effect_forbidden")
    except Exception as exc:  # noqa: BLE001
        errors.append(f"snapshot_replay_error:{exc}")
    return sorted(set(errors))


def _paper_portfolio_dir(*, paper_portfolio_id: str | None, output_dir: Path) -> Path:
    if paper_portfolio_id:
        path = output_dir / paper_portfolio_id
        if not path.exists():
            raise DynamicV3PaperTrackingError(f"paper portfolio not found: {paper_portfolio_id}")
        return path
    pointer = _latest_pointer_payload("latest_paper_portfolio")
    artifact_id = _text(pointer.get("artifact_id"))
    if artifact_id:
        path = output_dir / artifact_id
        if path.exists():
            return path
    latest = _latest_child_dir(output_dir)
    if latest is None:
        raise DynamicV3PaperTrackingError("paper portfolio not found")
    return latest


def _outcome_dir(*, outcome_id: str | None, output_dir: Path) -> Path:
    return _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=outcome_id,
        pointer_name="latest_advisory_outcome",
    )


def _artifact_dir_from_latest(
    *,
    output_dir: Path,
    artifact_id: str | None,
    pointer_name: str,
) -> Path:
    if artifact_id:
        path = output_dir / artifact_id
        if not path.exists():
            raise DynamicV3PaperTrackingError(f"artifact not found: {artifact_id}")
        return path
    pointer = _latest_pointer_payload(pointer_name)
    pointer_id = _text(pointer.get("artifact_id"))
    if pointer_id and (output_dir / pointer_id).exists():
        return output_dir / pointer_id
    latest = _latest_child_dir(output_dir)
    if latest is None:
        raise DynamicV3PaperTrackingError(f"latest artifact not found for {pointer_name}")
    return latest


def _owner_review_record(*, review_id: str, output_dir: Path) -> dict[str, Any]:
    for row in _read_jsonl(output_dir / "owner_review_journal.jsonl"):
        if _text(row.get("review_id")) == review_id:
            return row
    raise DynamicV3PaperTrackingError(f"owner review not found: {review_id}")


def _latest_pointer_payload(name: str) -> dict[str, Any]:
    return _read_optional_json(DEFAULT_DYNAMIC_V3_LATEST_POINTER_DIR / f"{name}.json") or {}


def _update_latest_pointer(name: str, artifact_id: str, path: Path) -> None:
    if not _is_dynamic_v3_artifact(path):
        return
    DEFAULT_DYNAMIC_V3_LATEST_POINTER_DIR.mkdir(parents=True, exist_ok=True)
    _write_json(
        DEFAULT_DYNAMIC_V3_LATEST_POINTER_DIR / f"{name}.json",
        {
            "schema_version": SCHEMA_VERSION,
            "artifact_type": name.removeprefix("latest_"),
            "artifact_id": artifact_id,
            "path": str(path),
            "updated_at": datetime.now(UTC).isoformat(),
            "exists": path.exists(),
        },
    )


def _is_dynamic_v3_artifact(path: Path) -> bool:
    try:
        return path.resolve(strict=False).is_relative_to(
            DEFAULT_DYNAMIC_V3_RESEARCH_ROOT.resolve(strict=False)
        )
    except ValueError:
        return False


def _download_manifest_path(prices_path: Path) -> Path | None:
    candidate = prices_path.parent / "download_manifests" / "prices_daily_download_manifest.json"
    return candidate if candidate.exists() else None


def _marketstack_prices_path(prices_path: Path) -> Path | None:
    candidate = prices_path.with_name("prices_marketstack_daily.csv")
    return candidate if candidate.exists() else None


def _requires_marketstack_prices(prices_path: Path) -> bool:
    try:
        return prices_path.resolve() == DEFAULT_ETF_PRICE_PATH.resolve()
    except FileNotFoundError:
        return prices_path == DEFAULT_ETF_PRICE_PATH


def _validation_payload(
    *,
    report_type: str,
    artifact_id_key: str,
    artifact_id: str,
    status: str,
    checks: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": report_type,
        artifact_id_key: artifact_id,
        "status": status,
        "checks": list(checks),
        "failed_check_count": sum(1 for check in checks if check.get("passed") is not True),
        "family": STRATEGY_FAMILY,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def _date_in_range(value: Any, start: date, end: date) -> bool:
    parsed = _date_from_any(value)
    return parsed is not None and start <= parsed <= end


def _date_from_any(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    text = _text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _parse_datetime_text(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None and parsed.utcoffset() is not None else None


def _latest_child_dir(path: Path) -> Path | None:
    if not path.exists():
        return None
    dirs = [child for child in path.iterdir() if child.is_dir()]
    return max(dirs, key=lambda child: child.stat().st_mtime) if dirs else None


def _resolve_project_path(path: Path) -> Path:
    return path if path.is_absolute() else PROJECT_ROOT / path


def _paths_equal(left: Path, right: Path) -> bool:
    return left.resolve(strict=False) == right.resolve(strict=False)


def _file_sha256(path: Path) -> str:
    if not path.is_file():
        raise DynamicV3PaperTrackingError(f"source file not found: {path}")
    return sha256(path.read_bytes()).hexdigest()


def _is_finite_number(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _strict_finite_number(value: Any, label: str) -> float:
    if not _is_finite_number(value):
        raise DynamicV3PaperTrackingError(f"{label} must be a finite number")
    return float(value)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_optional_json(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    return _read_json(path)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_jsonable(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(_jsonable(row), ensure_ascii=False, sort_keys=True) + "\n")


def _append_jsonl_atomic(
    path: Path,
    row: Mapping[str, Any],
    *,
    sequence_field: str = "event_sequence",
    checksum_field: str = "event_checksum",
    previous_field: str = "previous_event_checksum",
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    if existing and not existing.endswith("\n"):
        raise DynamicV3PaperTrackingError(f"JSONL source is not newline terminated: {path}")
    current = _read_jsonl(path)
    expected_sequence = len(current) + 1
    expected_previous = _text(current[-1].get(checksum_field)) if current else "GENESIS"
    if (
        row.get(sequence_field) != expected_sequence
        or row.get(previous_field) != expected_previous
    ):
        raise DynamicV3PaperTrackingError("paper action append precondition changed")
    line = json.dumps(_jsonable(row), ensure_ascii=False, sort_keys=True) + "\n"
    write_text_atomic(path, existing + line)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _unique_dir(path: Path) -> Path:
    if not path.exists():
        return path
    for idx in range(1, 1000):
        candidate = path.with_name(f"{path.name}_{idx:03d}")
        if not candidate.exists():
            return candidate
    raise DynamicV3PaperTrackingError(f"could not allocate unique artifact dir: {path}")


def _stable_id(*parts: Any) -> str:
    return sha256(_canonical_json(parts).encode("utf-8")).hexdigest()[:16]


def _canonical_json(payload: Any) -> str:
    return json.dumps(_jsonable(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value


def _normalize_weights(weights: Mapping[str, Any]) -> dict[str, float]:
    clean = {symbol: _float(value) for symbol, value in _clean_weights(weights).items()}
    total = sum(clean.values())
    if total <= 0:
        return {}
    normalized = {symbol: round(value / total, 6) for symbol, value in clean.items() if value > 0}
    drift = round(1.0 - sum(normalized.values()), 6)
    if drift:
        if "CASH" in normalized:
            normalized["CASH"] = round(normalized["CASH"] + drift, 6)
        elif normalized:
            first = sorted(normalized)[0]
            normalized[first] = round(normalized[first] + drift, 6)
    return {symbol: value for symbol, value in sorted(normalized.items()) if value > 0}


def _clean_weights(weights: Mapping[str, Any]) -> dict[str, float]:
    return {
        _text(symbol): _float(value)
        for symbol, value in weights.items()
        if _text(symbol) and _float(value) != 0
    }


def _weights_equal(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    symbols = set(left) | set(right)
    return all(
        abs(_float(left.get(symbol)) - _float(right.get(symbol))) <= 0.000001 for symbol in symbols
    )


def _format_weights(weights: Mapping[str, Any]) -> str:
    return ", ".join(f"{symbol}={_float(value):.2%}" for symbol, value in sorted(weights.items()))


def _avg(values: Sequence[float]) -> float:
    clean = [value for value in values if pd.notna(value)]
    return sum(clean) / len(clean) if clean else 0.0


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _texts(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, Sequence):
        return [_text(item) for item in value if _text(item)]
    return []


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _int(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _check(check_id: str, passed: bool, detail: str) -> dict[str, Any]:
    return {"check_id": check_id, "passed": bool(passed), "detail": detail}
