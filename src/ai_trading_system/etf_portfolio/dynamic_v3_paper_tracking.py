from __future__ import annotations

import csv
import json
import math
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
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
    validate_owner_review_artifact,
    validate_position_advisory_daily_artifact,
)
from ai_trading_system.etf_portfolio.models import (
    DEFAULT_ETF_PRICE_PATH,
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
    config = load_paper_portfolio_config(config_path)
    windows = _configured_outcome_windows(config)
    advisory_dir = daily_advisory_dir / daily_advisory_id
    manifest = _read_json(advisory_dir / "daily_advisory_manifest.json")
    actions = _read_json(advisory_dir / "daily_advisory_actions.json")
    no_trade = _advisory_current_weights(advisory_dir) or _latest_paper_weights(paper_portfolio_dir)
    target = _advisory_consensus_weights(advisory_dir)
    baseline = _baseline_weights()
    outcome_id = _stable_id("advisory-outcome", daily_advisory_id, generated.isoformat())
    outcome_dir = _unique_dir(output_dir / outcome_id)
    outcome_dir.mkdir(parents=True, exist_ok=False)
    advisory_event = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_advisory_event",
        "outcome_id": outcome_dir.name,
        "daily_advisory_id": daily_advisory_id,
        "as_of": _text(manifest.get("as_of")),
        "recommended_action": _text(actions.get("recommended_action")),
        "mode": _text(actions.get("mode"), _text(manifest.get("mode"))),
        "no_trade_weights": no_trade,
        "paper_action_weights": no_trade,
        "baseline_weights": baseline,
        "target_weights": target,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    window_rows = [
        _pending_outcome_window(
            daily_advisory_id=daily_advisory_id,
            window_days=window,
            start_date=_text(manifest.get("as_of")),
        )
        for window in windows
    ]
    counterfactuals = {
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
        "broker_action_allowed": False,
        "broker_action_taken": False,
    }
    outcome_manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_advisory_outcome_manifest",
        "outcome_id": outcome_dir.name,
        "daily_advisory_id": daily_advisory_id,
        "as_of": _text(manifest.get("as_of")),
        "generated_at": generated.isoformat(),
        "status": "PENDING",
        "tracked_windows": windows,
        "data_quality_status": "NOT_RUN_PENDING_ONLY",
        "data_quality_report_path": "",
        "advisory_event_path": str(outcome_dir / "advisory_event.json"),
        "outcome_windows_path": str(outcome_dir / "outcome_windows.jsonl"),
        "advisory_counterfactuals_path": str(outcome_dir / "advisory_counterfactuals.json"),
        "advisory_outcome_report_path": str(outcome_dir / "advisory_outcome_report.md"),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "owner_approval_required": True,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(outcome_dir / "advisory_outcome_manifest.json", outcome_manifest)
    _write_json(outcome_dir / "advisory_event.json", advisory_event)
    _write_jsonl(outcome_dir / "outcome_windows.jsonl", window_rows)
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
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    outcome_dir = _outcome_dir(outcome_id=outcome_id, output_dir=output_dir)
    manifest = _read_json(outcome_dir / "advisory_outcome_manifest.json")
    event = _read_json(outcome_dir / "advisory_event.json")
    start = _date_from_any(event.get("as_of"))
    if start is None:
        raise DynamicV3PaperTrackingError("advisory outcome event missing as_of")
    event["paper_action_weights"] = _paper_action_weights_for_advisory(
        daily_advisory_id=_text(event.get("daily_advisory_id")),
        fallback_weights=_mapping(event.get("no_trade_weights")),
        paper_portfolio_dir=paper_portfolio_dir,
    )
    today = generated.date()
    data_quality_status = "NOT_RUN_FUTURE_AS_OF" if as_of > today else ""
    data_quality_report_path = ""
    prices: pd.DataFrame | None = None
    price_dates: list[date] = []
    if as_of <= today:
        quality_report_path = outcome_dir / "validate_data_quality_report.md"
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
        prices = _load_outcome_prices(prices_path, event)
        price_dates = _available_price_dates(prices)
    rows = []
    for existing in _read_jsonl(outcome_dir / "outcome_windows.jsonl"):
        window_days = int(existing.get("window_days") or 0)
        end = _nth_trading_date_after(price_dates, start, window_days) if price_dates else None
        if end is None:
            status = "PENDING" if as_of > today else "INSUFFICIENT_DATA"
            if as_of > today:
                status = "PENDING"
            row = {**existing, "outcome_status": status}
            rows.append(row)
            continue
        if as_of < end:
            rows.append({**existing, "end_date": end.isoformat(), "outcome_status": "PENDING"})
            continue
        assert prices is not None
        metrics = _outcome_window_metrics(
            prices=prices,
            start=start,
            end=end,
            event=event,
        )
        rows.append(
            {
                "daily_advisory_id": event["daily_advisory_id"],
                "window_days": window_days,
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                **metrics,
                "outcome_status": (
                    "AVAILABLE" if metrics["status"] == "AVAILABLE" else "INSUFFICIENT_DATA"
                ),
            }
        )
    status = _rollup_outcome_status(rows)
    manifest.update(
        {
            "status": status,
            "updated_at": generated.isoformat(),
            "updated_as_of": as_of.isoformat(),
            "data_quality_status": data_quality_status,
            "data_quality_report_path": data_quality_report_path,
            "broker_action_allowed": False,
            "broker_action_taken": False,
        }
    )
    _write_json(outcome_dir / "advisory_outcome_manifest.json", manifest)
    _write_json(outcome_dir / "advisory_event.json", event)
    _write_jsonl(outcome_dir / "outcome_windows.jsonl", rows)
    _write_text(
        outcome_dir / "advisory_outcome_report.md",
        render_advisory_outcome_report(manifest, event, rows),
    )
    _update_latest_pointer(
        "latest_advisory_outcome", outcome_dir.name, outcome_dir / "advisory_outcome_manifest.json"
    )
    return {
        "outcome_id": outcome_dir.name,
        "outcome_dir": outcome_dir,
        "manifest": manifest,
        "advisory_event": event,
        "outcome_windows": rows,
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
    manifest = _read_optional_json(outcome_dir / "advisory_outcome_manifest.json") or {}
    rows = _read_jsonl(outcome_dir / "outcome_windows.jsonl")
    checks = [
        _check(
            "manifest_exists", (outcome_dir / "advisory_outcome_manifest.json").exists(), outcome_id
        ),
        _check("event_exists", (outcome_dir / "advisory_event.json").exists(), outcome_id),
        _check("windows_exists", (outcome_dir / "outcome_windows.jsonl").exists(), outcome_id),
        _check(
            "counterfactuals_exists",
            (outcome_dir / "advisory_counterfactuals.json").exists(),
            outcome_id,
        ),
        _check("report_exists", (outcome_dir / "advisory_outcome_report.md").exists(), outcome_id),
        _check("outcome_id_matches", manifest.get("outcome_id") == outcome_id, outcome_id),
        _check("windows_present", bool(rows), "outcome windows required"),
        _check(
            "window_status_valid",
            all(row.get("outcome_status") in OUTCOME_WINDOW_STATUSES for row in rows),
            "window statuses",
        ),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False,
            "broker action forbidden",
        ),
        _check(
            "broker_action_not_taken",
            manifest.get("broker_action_taken") is False,
            "broker action not taken",
        ),
    ]
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return _validation_payload(
        report_type="etf_dynamic_v3_advisory_outcome_validation",
        artifact_id_key="outcome_id",
        artifact_id=outcome_id,
        status=status,
        checks=checks,
    )


def run_owner_attribution(
    *,
    output_dir: Path = DEFAULT_OWNER_ATTRIBUTION_DIR,
    owner_review_dir: Path = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    outcome_dir: Path = DEFAULT_ADVISORY_OUTCOME_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    records = _read_jsonl(owner_review_dir / "owner_review_journal.jsonl")
    outcomes = _outcome_rows_by_advisory(outcome_dir)
    attribution_id = _stable_id("owner-attribution", len(records), generated.isoformat())
    artifact_dir = _unique_dir(output_dir / attribution_id)
    artifact_dir.mkdir(parents=True, exist_ok=False)
    summary = _owner_decision_summary(records)
    matrix = _advisory_acceptance_matrix(records)
    comparison = _decision_outcome_comparison(records, outcomes)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_owner_attribution_manifest",
        "attribution_id": artifact_dir.name,
        "generated_at": generated.isoformat(),
        "status": "PASS" if records else "INSUFFICIENT_DATA",
        "total_reviews": len(records),
        "linked_outcome_count": sum(
            1 for row in records if _text(row.get("daily_advisory_id")) in outcomes
        ),
        "owner_decision_summary_path": str(artifact_dir / "owner_decision_summary.json"),
        "advisory_acceptance_matrix_path": str(artifact_dir / "advisory_acceptance_matrix.json"),
        "decision_outcome_comparison_path": str(artifact_dir / "decision_outcome_comparison.json"),
        "owner_attribution_report_path": str(artifact_dir / "owner_attribution_report.md"),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
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
    manifest = _read_optional_json(artifact_dir / "owner_attribution_manifest.json") or {}
    comparison = _read_optional_json(artifact_dir / "decision_outcome_comparison.json") or {}
    checks = [
        _check(
            "manifest_exists",
            (artifact_dir / "owner_attribution_manifest.json").exists(),
            attribution_id,
        ),
        _check(
            "decision_summary_exists",
            (artifact_dir / "owner_decision_summary.json").exists(),
            attribution_id,
        ),
        _check(
            "acceptance_matrix_exists",
            (artifact_dir / "advisory_acceptance_matrix.json").exists(),
            attribution_id,
        ),
        _check(
            "outcome_comparison_exists",
            (artifact_dir / "decision_outcome_comparison.json").exists(),
            attribution_id,
        ),
        _check(
            "report_exists", (artifact_dir / "owner_attribution_report.md").exists(), attribution_id
        ),
        _check(
            "attribution_id_matches",
            manifest.get("attribution_id") == attribution_id,
            attribution_id,
        ),
        _check("comparison_status_present", bool(comparison.get("status")), "comparison status"),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False,
            "broker action forbidden",
        ),
    ]
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return _validation_payload(
        report_type="etf_dynamic_v3_owner_attribution_validation",
        artifact_id_key="attribution_id",
        artifact_id=attribution_id,
        status=status,
        checks=checks,
    )


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
    config = load_paper_portfolio_config(config_path)
    policy = _mapping(config.get("promotion_clock_v2"))
    candidate_ids = _shadow_shortlist_candidate_ids(shadow_shortlist_id, shadow_shortlist_dir)
    monitor_rows = _monitor_rows_for_shortlist(shadow_shortlist_id, shadow_monitor_run_dir)
    drift_rows = _drift_rows_for_shortlist(
        shadow_shortlist_id, shadow_monitor_run_dir, consensus_drift_dir
    )
    outcome_score = _average_available_outcome_score(advisory_outcome_dir)
    rows = [
        _candidate_aging_status(
            candidate_id=candidate_id,
            monitor_rows=monitor_rows.get(candidate_id, []),
            drift_rows=drift_rows,
            outcome_score=outcome_score,
            policy=policy,
        )
        for candidate_id in candidate_ids
    ]
    aging_id = _stable_id("shadow-aging", shadow_shortlist_id, generated.isoformat())
    artifact_dir = _unique_dir(output_dir / aging_id)
    artifact_dir.mkdir(parents=True, exist_ok=False)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_promotion_clock_v2_summary",
        "aging_id": artifact_dir.name,
        "shadow_shortlist_id": shadow_shortlist_id,
        "candidate_count": len(rows),
        "eligible_for_review_count": sum(
            1 for row in rows if row["promotion_clock_status"] == "eligible_for_review"
        ),
        "downgrade_recommended_count": sum(
            1 for row in rows if row["promotion_clock_status"] == "downgrade_recommended"
        ),
        "warming_up_count": sum(1 for row in rows if row["promotion_clock_status"] == "warming_up"),
        "blocked_count": sum(1 for row in rows if row["promotion_clock_status"] == "blocked"),
        "production_candidate_generated": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shadow_aging_manifest",
        "aging_id": artifact_dir.name,
        "shadow_shortlist_id": shadow_shortlist_id,
        "generated_at": generated.isoformat(),
        "status": "PASS" if rows else "INSUFFICIENT_DATA",
        "candidate_count": len(rows),
        "candidate_aging_status_path": str(artifact_dir / "candidate_aging_status.jsonl"),
        "promotion_clock_v2_summary_path": str(artifact_dir / "promotion_clock_v2_summary.json"),
        "shadow_aging_report_path": str(artifact_dir / "shadow_aging_report.md"),
        "automatic_candidate_promotion": False,
        "production_candidate_generated": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
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
    manifest = _read_optional_json(artifact_dir / "shadow_aging_manifest.json") or {}
    rows = _read_jsonl(artifact_dir / "candidate_aging_status.jsonl")
    checks = [
        _check("manifest_exists", (artifact_dir / "shadow_aging_manifest.json").exists(), aging_id),
        _check(
            "candidate_status_exists",
            (artifact_dir / "candidate_aging_status.jsonl").exists(),
            aging_id,
        ),
        _check(
            "summary_exists", (artifact_dir / "promotion_clock_v2_summary.json").exists(), aging_id
        ),
        _check("report_exists", (artifact_dir / "shadow_aging_report.md").exists(), aging_id),
        _check("aging_id_matches", manifest.get("aging_id") == aging_id, aging_id),
        _check(
            "promotion_status_valid",
            all(row.get("promotion_clock_status") in PROMOTION_CLOCK_STATUSES for row in rows),
            "promotion statuses",
        ),
        _check(
            "production_candidate_not_generated",
            manifest.get("production_candidate_generated") is False,
            "no production candidate",
        ),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False,
            "broker action forbidden",
        ),
    ]
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return _validation_payload(
        report_type="etf_dynamic_v3_shadow_aging_validation",
        artifact_id_key="aging_id",
        artifact_id=aging_id,
        status=status,
        checks=checks,
    )


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
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    week_start = week_ending - timedelta(days=6)
    monitor_manifests = _manifest_rows_in_week(
        shadow_monitor_run_dir,
        "shadow_monitor_manifest.json",
        week_start,
        week_ending,
    )
    advisory_manifests = _manifest_rows_in_week(
        daily_advisory_dir,
        "daily_advisory_manifest.json",
        week_start,
        week_ending,
    )
    owner_records = [
        row
        for row in _read_jsonl(owner_review_dir / "owner_review_journal.jsonl")
        if _date_in_range(row.get("as_of"), week_start, week_ending)
    ]
    latest_paper = _latest_paper_state_summary(paper_portfolio_dir)
    latest_aging = _latest_shadow_aging_summary(shadow_aging_dir)
    outcome_summary = _weekly_outcome_summary(advisory_outcome_dir, week_start, week_ending)
    weekly_recommendation, next_actions = _weekly_recommendation(
        latest_paper=latest_paper,
        latest_aging=latest_aging,
        outcome_summary=outcome_summary,
        owner_records=owner_records,
    )
    weekly_id = _stable_id("weekly-advisory-review", week_ending.isoformat(), generated.isoformat())
    artifact_dir = _unique_dir(output_dir / weekly_id)
    artifact_dir.mkdir(parents=True, exist_ok=False)
    weekly_advisory_summary = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weekly_advisory_summary",
        "week_start": week_start.isoformat(),
        "week_ending": week_ending.isoformat(),
        "shadow_monitor_run_count": len(monitor_manifests),
        "daily_advisory_count": len(advisory_manifests),
        "recommended_actions": dict(
            Counter(_text(row.get("recommended_action"), "MISSING") for row in advisory_manifests)
        ),
        "outcome_summary": outcome_summary,
    }
    weekly_owner_decision_summary = _owner_decision_summary(owner_records)
    weekly_paper_portfolio_summary = latest_paper
    weekly_shadow_candidate_summary = latest_aging
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weekly_advisory_review_manifest",
        "weekly_review_id": artifact_dir.name,
        "week_start": week_start.isoformat(),
        "week_ending": week_ending.isoformat(),
        "generated_at": generated.isoformat(),
        "status": (
            "PASS"
            if monitor_manifests or advisory_manifests or owner_records
            else "INSUFFICIENT_DATA"
        ),
        "weekly_recommendation": weekly_recommendation,
        "next_actions": next_actions,
        "paper_portfolio_status": latest_paper.get("state_status", "MISSING"),
        "owner_review_count": len(owner_records),
        "shadow_monitor_run_count": len(monitor_manifests),
        "daily_advisory_count": len(advisory_manifests),
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
        "production_candidate_generated": False,
        "automatic_candidate_promotion": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(artifact_dir / "weekly_review_manifest.json", manifest)
    _write_json(artifact_dir / "weekly_advisory_summary.json", weekly_advisory_summary)
    _write_json(artifact_dir / "weekly_owner_decision_summary.json", weekly_owner_decision_summary)
    _write_json(
        artifact_dir / "weekly_paper_portfolio_summary.json", weekly_paper_portfolio_summary
    )
    _write_json(
        artifact_dir / "weekly_shadow_candidate_summary.json", weekly_shadow_candidate_summary
    )
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
    manifest = _read_optional_json(artifact_dir / "weekly_review_manifest.json") or {}
    checks = [
        _check(
            "manifest_exists",
            (artifact_dir / "weekly_review_manifest.json").exists(),
            weekly_review_id,
        ),
        _check(
            "advisory_summary_exists",
            (artifact_dir / "weekly_advisory_summary.json").exists(),
            weekly_review_id,
        ),
        _check(
            "owner_summary_exists",
            (artifact_dir / "weekly_owner_decision_summary.json").exists(),
            weekly_review_id,
        ),
        _check(
            "paper_summary_exists",
            (artifact_dir / "weekly_paper_portfolio_summary.json").exists(),
            weekly_review_id,
        ),
        _check(
            "shadow_summary_exists",
            (artifact_dir / "weekly_shadow_candidate_summary.json").exists(),
            weekly_review_id,
        ),
        _check(
            "report_exists", (artifact_dir / "weekly_review_report.md").exists(), weekly_review_id
        ),
        _check(
            "reader_brief_exists",
            (artifact_dir / "reader_brief_section.md").exists(),
            weekly_review_id,
        ),
        _check(
            "weekly_review_id_matches",
            manifest.get("weekly_review_id") == weekly_review_id,
            weekly_review_id,
        ),
        _check(
            "weekly_recommendation_present",
            bool(manifest.get("weekly_recommendation")),
            "weekly recommendation",
        ),
        _check(
            "production_candidate_not_generated",
            manifest.get("production_candidate_generated") is False,
            "no production candidate",
        ),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False,
            "broker action forbidden",
        ),
    ]
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return _validation_payload(
        report_type="etf_dynamic_v3_weekly_advisory_review_validation",
        artifact_id_key="weekly_review_id",
        artifact_id=weekly_review_id,
        status=status,
        checks=checks,
    )


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
        "- broker_action_taken：false",
        "",
        "| Window | Status | Paper vs no_trade | Paper vs baseline | End |",
        "|---:|---|---:|---:|---|",
    ]
    for row in rows:
        lines.append(
            "| "
            f"{row.get('window_days')} | "
            f"{row.get('outcome_status')} | "
            f"{_float(row.get('relative_to_no_trade')):.6f} | "
            f"{_float(row.get('relative_to_baseline')):.6f} | "
            f"{row.get('end_date', '')} |"
        )
    lines.extend(
        [
            "",
            "## 阅读口径",
            "",
            "AVAILABLE 只表示该窗口已有足够价格数据完成纸面评估；"
            "PENDING 和 INSUFFICIENT_DATA 不得解释为收益为 0。",
            "该报告不生成交易指令，不触发 broker action。",
            "",
        ]
    )
    return "\n".join(lines)


def render_owner_attribution_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    matrix: Mapping[str, Any],
    comparison: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Dynamic Rescue Owner Attribution",
            "",
            f"- 状态：{manifest.get('status', 'UNKNOWN')}",
            f"- attribution_id：`{manifest.get('attribution_id', '')}`",
            f"- total_reviews：{summary.get('total_reviews', 0)}",
            f"- most_common_owner_decision：{summary.get('most_common_owner_decision', 'MISSING')}",
            f"- outcome_comparison_status：{comparison.get('status', 'UNKNOWN')}",
            "- broker_action_taken：false",
            "",
            "## Advisory Acceptance",
            "",
            json.dumps(
                matrix.get("by_recommended_action", {}),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ),
            "",
            "数据不足时保持 INSUFFICIENT_DATA，不把 owner decision 或 advisory rule 写成生产结论。",
            "",
        ]
    )


def render_shadow_aging_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Dynamic Rescue Shadow Aging v2",
        "",
        f"- 状态：{manifest.get('status', 'UNKNOWN')}",
        f"- aging_id：`{manifest.get('aging_id', '')}`",
        f"- eligible_for_review_count：{summary.get('eligible_for_review_count', 0)}",
        f"- downgrade_recommended_count：{summary.get('downgrade_recommended_count', 0)}",
        "- production_candidate_generated：false",
        "- broker_action_taken：false",
        "",
        "| Candidate | Days | Rebalances | Status | Blocking reasons |",
        "|---|---:|---:|---|---|",
    ]
    for row in rows:
        lines.append(
            "| "
            f"{row.get('candidate_id')} | "
            f"{row.get('days_observed')} | "
            f"{row.get('rebalance_count_observed')} | "
            f"{row.get('promotion_clock_status')} | "
            f"{', '.join(_texts(row.get('blocking_reasons'))) or 'none'} |"
        )
    lines.append("")
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
            f"- week_ending：{manifest.get('week_ending', '')}",
            f"- weekly_recommendation：{manifest.get('weekly_recommendation', '')}",
            f"- shadow monitor runs：{advisory_summary.get('shadow_monitor_run_count', 0)}",
            f"- daily advisory count：{advisory_summary.get('daily_advisory_count', 0)}",
            f"- owner reviews：{owner_summary.get('total_reviews', 0)}",
            f"- paper_portfolio_status：{paper_summary.get('state_status', 'MISSING')}",
            "- outcome_status："
            f"{_mapping(advisory_summary.get('outcome_summary')).get('status', 'MISSING')}",
            f"- eligible_for_review_count：{shadow_summary.get('eligible_for_review_count', 0)}",
            "- downgrade_recommended_count："
            f"{shadow_summary.get('downgrade_recommended_count', 0)}",
            f"- next_actions：{', '.join(_texts(manifest.get('next_actions')))}",
            "- broker_action_taken：false",
            "- production_candidate_generated：false",
            "",
            "该 weekly review 是 owner 复盘材料；eligible_for_review "
            "只表示人工 promotion review 候选，不自动进入 production。",
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
            f"- weekly_recommendation: {manifest.get('weekly_recommendation', 'MISSING')}",
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
        "paper_portfolio_return": 0.0,
        "no_trade_return": 0.0,
        "baseline_return": 0.0,
        "target_weight_return": 0.0,
        "relative_to_no_trade": 0.0,
        "relative_to_baseline": 0.0,
        "max_drawdown": 0.0,
        "realized_volatility": 0.0,
        "outcome_status": "PENDING",
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


def _load_outcome_prices(prices_path: Path, event: Mapping[str, Any]) -> pd.DataFrame:
    symbols = (
        set(_mapping(event.get("no_trade_weights")))
        | set(_mapping(event.get("paper_action_weights")))
        | set(_mapping(event.get("baseline_weights")))
        | set(_mapping(event.get("target_weights")))
    )
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


def _available_price_dates(prices: pd.DataFrame) -> list[date]:
    return sorted(date_value for date_value in prices["_date"].dropna().unique())


def _nth_trading_date_after(dates: Sequence[date], start: date, n: int) -> date | None:
    after = [item for item in dates if item > start]
    if len(after) < n:
        return None
    return after[n - 1]


def _outcome_window_metrics(
    *,
    prices: pd.DataFrame,
    start: date,
    end: date,
    event: Mapping[str, Any],
) -> dict[str, Any]:
    no_trade_weights = _mapping(event.get("no_trade_weights"))
    paper_weights = _mapping(event.get("paper_action_weights")) or no_trade_weights
    baseline_weights = _mapping(event.get("baseline_weights"))
    target_weights = _mapping(event.get("target_weights"))
    try:
        paper_return, daily_returns = _portfolio_return_and_path(prices, paper_weights, start, end)
        no_trade_return, _ = _portfolio_return_and_path(prices, no_trade_weights, start, end)
        baseline_return, _ = _portfolio_return_and_path(prices, baseline_weights, start, end)
        target_return, _ = _portfolio_return_and_path(prices, target_weights, start, end)
    except DynamicV3PaperTrackingError:
        return {
            "paper_portfolio_return": 0.0,
            "no_trade_return": 0.0,
            "baseline_return": 0.0,
            "target_weight_return": 0.0,
            "relative_to_no_trade": 0.0,
            "relative_to_baseline": 0.0,
            "max_drawdown": 0.0,
            "realized_volatility": 0.0,
            "status": "INSUFFICIENT_DATA",
        }
    return {
        "paper_portfolio_return": round(paper_return, 6),
        "no_trade_return": round(no_trade_return, 6),
        "baseline_return": round(baseline_return, 6),
        "target_weight_return": round(target_return, 6),
        "relative_to_no_trade": round(paper_return - no_trade_return, 6),
        "relative_to_baseline": round(paper_return - baseline_return, 6),
        "max_drawdown": round(_max_drawdown(daily_returns), 6),
        "realized_volatility": round(_realized_volatility(daily_returns), 6),
        "status": "AVAILABLE",
    }


def _portfolio_return_and_path(
    prices: pd.DataFrame,
    weights: Mapping[str, Any],
    start: date,
    end: date,
) -> tuple[float, list[float]]:
    clean = _normalize_weights(weights)
    if not clean:
        raise DynamicV3PaperTrackingError("empty weights")
    pivot = prices.pivot_table(index="_date", columns="symbol", values="_adj_close", aggfunc="last")
    dates = [item for item in sorted(pivot.index) if start <= item <= end]
    if start not in pivot.index:
        prior = [item for item in sorted(pivot.index) if item <= start]
        if not prior:
            raise DynamicV3PaperTrackingError("missing start price")
        start_idx = prior[-1]
    else:
        start_idx = start
    if end not in pivot.index:
        raise DynamicV3PaperTrackingError("missing end price")
    daily_returns = []
    total_return = 0.0
    for symbol, weight in clean.items():
        if symbol == "CASH":
            continue
        if symbol not in pivot.columns:
            raise DynamicV3PaperTrackingError(f"missing symbol price: {symbol}")
        start_price = _float(pivot.loc[start_idx, symbol])
        end_price = _float(pivot.loc[end, symbol])
        if start_price <= 0 or end_price <= 0:
            raise DynamicV3PaperTrackingError(f"invalid price for {symbol}")
        total_return += _float(weight) * (end_price / start_price - 1.0)
    for left, right in zip(dates, dates[1:], strict=False):
        day_ret = 0.0
        for symbol, weight in clean.items():
            if symbol == "CASH":
                continue
            left_price = _float(pivot.loc[left, symbol])
            right_price = _float(pivot.loc[right, symbol])
            if left_price <= 0 or right_price <= 0:
                continue
            day_ret += _float(weight) * (right_price / left_price - 1.0)
        daily_returns.append(day_ret)
    return total_return, daily_returns


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
    if "AVAILABLE" in statuses:
        return "AVAILABLE"
    if "INSUFFICIENT_DATA" in statuses:
        return "INSUFFICIENT_DATA"
    return "PENDING"


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
    most_common = counter.most_common(1)[0][0] if counter else "MISSING"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_owner_decision_summary",
        "total_reviews": len(records),
        **{decision: counter.get(decision, 0) for decision in decisions},
        "most_common_owner_decision": most_common,
        "broker_action_taken": False,
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
        matrix["by_recommended_action"][action] = {
            "count": len(rows),
            "accepted_monitor": decisions.get("monitor", 0),
            "rejected": decisions.get("reject_advisory", 0),
            "paper_adjustment": decisions.get("paper_adjustment", 0),
            "owner_decisions": dict(decisions),
        }
    return matrix


def _decision_outcome_comparison(
    records: Sequence[Mapping[str, Any]],
    outcomes: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for record in records:
        daily_id = _text(record.get("daily_advisory_id"))
        for row in outcomes.get(daily_id, []):
            if row.get("outcome_status") == "AVAILABLE":
                groups[_text(record.get("owner_decision"), "pending")].append(row)
    decision_groups = []
    for decision, rows in sorted(groups.items()):
        decision_groups.append(
            {
                "owner_decision": decision,
                "count": len(rows),
                "avg_5d_relative_to_no_trade": _avg_window(rows, 5, "relative_to_no_trade"),
                "avg_20d_relative_to_no_trade": _avg_window(rows, 20, "relative_to_no_trade"),
                "avg_max_drawdown": round(
                    _avg([_float(row.get("max_drawdown")) for row in rows]), 6
                ),
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_decision_outcome_comparison",
        "status": "AVAILABLE" if decision_groups else "INSUFFICIENT_DATA",
        "decision_groups": decision_groups,
    }


def _avg_window(rows: Sequence[Mapping[str, Any]], window: int, key: str) -> float:
    values = [_float(row.get(key)) for row in rows if int(row.get("window_days") or 0) == window]
    return round(_avg(values), 6)


def _outcome_rows_by_advisory(output_dir: Path) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for child in output_dir.glob("*"):
        if not child.is_dir():
            continue
        manifest = _read_optional_json(child / "advisory_outcome_manifest.json") or {}
        daily_id = _text(manifest.get("daily_advisory_id"))
        if daily_id:
            result[daily_id].extend(_read_jsonl(child / "outcome_windows.jsonl"))
    return result


def _shadow_shortlist_candidate_ids(
    shadow_shortlist_id: str, shadow_shortlist_dir: Path
) -> list[str]:
    rows = _read_jsonl(
        shadow_shortlist_dir / shadow_shortlist_id / "shadow_shortlist_candidates.jsonl"
    )
    return [_text(row.get("candidate_id")) for row in rows if _text(row.get("candidate_id"))]


def _monitor_rows_for_shortlist(
    shadow_shortlist_id: str,
    output_dir: Path,
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for child in output_dir.glob("*"):
        if not child.is_dir():
            continue
        manifest = _read_optional_json(child / "shadow_monitor_manifest.json") or {}
        if _text(manifest.get("shadow_shortlist_id")) != shadow_shortlist_id:
            continue
        for row in _read_jsonl(child / "shadow_candidate_daily_results.jsonl"):
            candidate_id = _text(row.get("candidate_id"))
            if candidate_id:
                grouped[candidate_id].append(row)
    return grouped


def _drift_rows_for_shortlist(
    shadow_shortlist_id: str,
    monitor_dir: Path,
    drift_dir: Path,
) -> list[dict[str, Any]]:
    monitor_ids = set()
    for child in monitor_dir.glob("*"):
        if not child.is_dir():
            continue
        manifest = _read_optional_json(child / "shadow_monitor_manifest.json") or {}
        if _text(manifest.get("shadow_shortlist_id")) == shadow_shortlist_id:
            monitor_ids.add(_text(manifest.get("monitor_run_id")))
    rows = []
    for child in drift_dir.glob("*"):
        if not child.is_dir():
            continue
        summary = _read_optional_json(child / "consensus_drift_summary.json") or {}
        if _text(summary.get("shadow_monitor_run_id")) in monitor_ids:
            rows.append(summary)
    return rows


def _candidate_aging_status(
    *,
    candidate_id: str,
    monitor_rows: Sequence[Mapping[str, Any]],
    drift_rows: Sequence[Mapping[str, Any]],
    outcome_score: float,
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    dates = sorted(
        {
            _date_from_any(row.get("as_of"))
            for row in monitor_rows
            if _date_from_any(row.get("as_of"))
        }
    )
    days_observed = (dates[-1] - dates[0]).days + 1 if len(dates) >= 2 else len(dates)
    weights_seen = [_canonical_json(_mapping(row.get("target_weights"))) for row in monitor_rows]
    rebalance_count = max(0, len([item for item in weights_seen if item]) - 1)
    drift_warning_count = sum(
        1
        for row in monitor_rows
        if _text(_mapping(row.get("live_vs_backtest_drift")).get("status"))
        not in {"", "PASS", "UNKNOWN"}
    )
    downgrade_warning_count = sum(
        1
        for row in monitor_rows
        if _text(row.get("recommendation")) in {"required_downgrade", "remove_from_shadow"}
    )
    high_disagreement_count = sum(
        1 for row in drift_rows if row.get("disagreement_status") == "HIGH_DISAGREEMENT"
    )
    stability_score = round(
        max(
            0.0,
            1.0
            - (drift_warning_count + high_disagreement_count + downgrade_warning_count)
            / max(1, len(monitor_rows) + len(drift_rows)),
        ),
        6,
    )
    blocking = []
    min_days = int(
        policy.get(
            "min_days_observed",
            policy.get("minimum_days_observed", 30),
        )
    )
    min_rebalances = int(
        policy.get(
            "min_rebalance_count",
            policy.get("minimum_rebalance_count", 3),
        )
    )
    max_drift = int(policy.get("max_drift_warning_count", 1))
    max_high_disagreement = int(policy.get("max_high_disagreement_count", 1))
    outcome_floor = _float(
        policy.get(
            "min_outcome_score",
            policy.get("minimum_outcome_score", -0.01),
        )
    )
    if days_observed < min_days:
        blocking.append("insufficient_days_observed")
    if rebalance_count < min_rebalances:
        blocking.append("insufficient_rebalance_count")
    if drift_warning_count > max_drift:
        blocking.append("drift_warning_count_too_high")
    if high_disagreement_count > max_high_disagreement:
        blocking.append("high_disagreement_count_too_high")
    if downgrade_warning_count > 0:
        blocking.append("downgrade_warning_present")
    if outcome_score < outcome_floor:
        blocking.append("outcome_score_below_floor")
    if downgrade_warning_count or outcome_score < outcome_floor:
        status = "downgrade_recommended"
    elif not monitor_rows:
        status = "not_started"
    elif blocking:
        status = (
            "warming_up"
            if set(blocking) <= {"insufficient_days_observed", "insufficient_rebalance_count"}
            else "blocked"
        )
    else:
        status = "eligible_for_review"
    next_review_date = (
        (dates[-1] + timedelta(days=max(0, min_days - days_observed))).isoformat()
        if dates
        else None
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "candidate_id": candidate_id,
        "days_observed": days_observed,
        "rebalance_count_observed": rebalance_count,
        "monitor_runs_count": len(monitor_rows),
        "advisory_participation_count": len(monitor_rows),
        "drift_warning_count": drift_warning_count,
        "high_disagreement_count": high_disagreement_count,
        "downgrade_warning_count": downgrade_warning_count,
        "outcome_score": round(outcome_score, 6),
        "stability_score": stability_score,
        "promotion_clock_status": status,
        "blocking_reasons": blocking,
        "next_review_date": next_review_date,
        "production_candidate_generated": False,
        "broker_action_taken": False,
    }


def _average_available_outcome_score(output_dir: Path) -> float:
    values = []
    for child in output_dir.glob("*"):
        if not child.is_dir():
            continue
        for row in _read_jsonl(child / "outcome_windows.jsonl"):
            if row.get("outcome_status") == "AVAILABLE":
                values.append(_float(row.get("relative_to_no_trade")))
    return round(_avg(values), 6) if values else 0.0


def _manifest_rows_in_week(
    output_dir: Path,
    manifest_name: str,
    start: date,
    end: date,
) -> list[dict[str, Any]]:
    rows = []
    for child in output_dir.glob("*"):
        if not child.is_dir():
            continue
        manifest = _read_optional_json(child / manifest_name) or {}
        if _date_in_range(manifest.get("as_of"), start, end):
            rows.append(manifest)
    return rows


def _latest_paper_state_summary(output_dir: Path) -> dict[str, Any]:
    try:
        portfolio_dir = _paper_portfolio_dir(paper_portfolio_id=None, output_dir=output_dir)
    except DynamicV3PaperTrackingError:
        return {"status": "MISSING", "state_status": "MISSING", "broker_action_taken": False}
    state = _read_json(portfolio_dir / "paper_portfolio_state.json")
    return {
        "status": "AVAILABLE",
        "paper_portfolio_id": state.get("paper_portfolio_id", ""),
        "state_status": state.get("state_status", "UNKNOWN"),
        "as_of": state.get("as_of", ""),
        "positions": state.get("positions", {}),
        "total_weight": state.get("total_weight", 0.0),
        "broker_action_taken": False,
    }


def _latest_shadow_aging_summary(output_dir: Path) -> dict[str, Any]:
    try:
        artifact_dir = _artifact_dir_from_latest(
            output_dir=output_dir,
            artifact_id=None,
            pointer_name="latest_shadow_aging",
        )
    except DynamicV3PaperTrackingError:
        return {
            "status": "MISSING",
            "eligible_for_review_count": 0,
            "downgrade_recommended_count": 0,
            "broker_action_taken": False,
        }
    return _read_json(artifact_dir / "promotion_clock_v2_summary.json")


def _weekly_outcome_summary(output_dir: Path, start: date, end: date) -> dict[str, Any]:
    rows = []
    for child in output_dir.glob("*"):
        if not child.is_dir():
            continue
        manifest = _read_optional_json(child / "advisory_outcome_manifest.json") or {}
        if _date_in_range(manifest.get("as_of"), start, end):
            rows.extend(_read_jsonl(child / "outcome_windows.jsonl"))
    available = [row for row in rows if row.get("outcome_status") == "AVAILABLE"]
    return {
        "status": "AVAILABLE" if available else ("PENDING" if rows else "INSUFFICIENT_DATA"),
        "window_count": len(rows),
        "available_window_count": len(available),
        "avg_relative_to_no_trade": round(
            _avg([_float(row.get("relative_to_no_trade")) for row in available]), 6
        ),
        "avg_relative_to_baseline": round(
            _avg([_float(row.get("relative_to_baseline")) for row in available]), 6
        ),
    }


def _weekly_recommendation(
    *,
    latest_paper: Mapping[str, Any],
    latest_aging: Mapping[str, Any],
    outcome_summary: Mapping[str, Any],
    owner_records: Sequence[Mapping[str, Any]],
) -> tuple[str, list[str]]:
    next_actions = ["continue_monitoring"]
    if latest_paper.get("status") == "MISSING" or latest_paper.get("state_status") != "ACTIVE":
        next_actions.append("update_position_snapshot")
    if outcome_summary.get("status") in {"INSUFFICIENT_DATA", "PENDING"}:
        next_actions.append("manual_review_required")
    if _int(latest_aging.get("downgrade_recommended_count")) > 0:
        next_actions.append("reduce_shortlist")
    if _int(latest_aging.get("eligible_for_review_count")) > 0:
        next_actions.append("manual_review_required")
    if not owner_records:
        next_actions.append("manual_review_required")
    if "reduce_shortlist" in next_actions:
        recommendation = "reduce_shortlist"
    elif "manual_review_required" in next_actions:
        recommendation = "manual_review_required"
    else:
        recommendation = "continue_monitoring"
    ordered = []
    for action in next_actions:
        if action not in ordered:
            ordered.append(action)
    return recommendation, ordered


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


def _append_jsonl_atomic(path: Path, row: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    if existing and not existing.endswith("\n"):
        raise DynamicV3PaperTrackingError(f"JSONL source is not newline terminated: {path}")
    current = _read_jsonl(path)
    expected_sequence = len(current) + 1
    expected_previous = _text(current[-1].get("event_checksum")) if current else "GENESIS"
    if (
        row.get("event_sequence") != expected_sequence
        or row.get("previous_event_checksum") != expected_previous
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
