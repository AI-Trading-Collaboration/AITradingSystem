from __future__ import annotations

import csv
import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_backtest_simulation as sim
from ai_trading_system.etf_portfolio import dynamic_v3_cost_sensitivity as cost_sensitivity
from ai_trading_system.etf_portfolio import (
    dynamic_v3_filtered_candidate_readiness as readiness,
)
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_diagnostics as diagnostics
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_foundation as foundation
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH

DEFAULT_METRIC_SOURCE_MAP_DIR = st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "metric_source_map"

METRIC_SOURCE_MAP_STATUSES = (
    "METRIC_SOURCE_MAP_READY",
    "METRIC_SOURCE_MAP_PARTIAL",
    "METRIC_SOURCE_MAP_BLOCKED",
    "INSUFFICIENT_DATA",
)
METRIC_SOURCE_MAP_INPUT_SCHEMA = "metric_source_map_input_snapshot.v2"
METRIC_SOURCE_MAP_VIEWS = (
    "metric_source_map_manifest.json",
    "metric_source_map_report.json",
    "metric_source_map_report.md",
    "reader_brief_section.md",
)
METRIC_SOURCE_MAP_SNAPSHOT = "metric_source_map_input_snapshot.json"

METRIC_SOURCE_MAP_SAFETY = {
    **st.SYSTEM_TARGET_SAFETY,
    "metric_source_map_only": True,
    "research_only": True,
    "manual_review_only": True,
    "cost_metrics_materialized": False,
    "benchmark_metrics_materialized": False,
    "cost_sensitivity_review_rerun": False,
    "benchmark_baseline_control_rerun": False,
    "data_downloaded_by_source_map": False,
    "pipelines_executed_by_source_map": False,
    "strategy_optimized_by_source_map": False,
    "official_target_weights": False,
    "official_target_weights_mutated": False,
    "broker_action_allowed": False,
    "broker_action_taken": False,
    "order_ticket_generated": False,
    "paper_account_state_mutated": False,
    "production_state_mutated": False,
    "automatic_candidate_promotion": False,
    "auto_apply": False,
    "production_effect": "none",
}

CANDIDATE_METRIC_SPECS: tuple[dict[str, Any], ...] = (
    {
        "metric_name": "turnover",
        "derivation_method": "copy avg_turnover from source_variant summary row",
        "required_fields": ("summary[variant].avg_turnover",),
    },
    {
        "metric_name": "rotation_count",
        "derivation_method": "copy event_count from source_variant summary row",
        "required_fields": ("summary[variant].event_count",),
    },
    {
        "metric_name": "gross_performance_proxy",
        "derivation_method": "copy avg_5d_return from source_variant summary row",
        "required_fields": ("summary[variant].avg_5d_return",),
    },
    {
        "metric_name": "net_performance_proxy",
        "derivation_method": (
            "derive from gross_performance_proxy, turnover, and configured cost "
            "sensitivity scenarios during materialization"
        ),
        "required_fields": (
            "summary[variant].avg_5d_return",
            "summary[variant].avg_turnover",
            "cost_sensitivity_policy.scenarios",
        ),
    },
    {
        "metric_name": "drawdown_proxy",
        "derivation_method": "copy avg_max_drawdown_20d from source_variant summary row",
        "required_fields": ("summary[variant].avg_max_drawdown_20d",),
    },
    {
        "metric_name": "candidate_return_proxy",
        "derivation_method": (
            "use avg_relative_to_no_trade_5d, or derive avg_5d_return minus "
            "no_trade.avg_5d_return"
        ),
        "required_fields": (
            "summary[variant].avg_relative_to_no_trade_5d OR "
            "(summary[variant].avg_5d_return + summary[no_trade].avg_5d_return)",
        ),
    },
)

BASELINE_SPECS: tuple[dict[str, Any], ...] = (
    {
        "metric_name": "static_allocation_baseline",
        "baseline_id": "static_allocation",
        "derivation_method": (
            "apply static baseline weights over existing BACKTEST_SIMULATION event windows "
            "using cached adjusted-close prices"
        ),
        "source_method": "price_cache_static_allocation",
    },
    {
        "metric_name": "no_trade_baseline",
        "baseline_id": "no_trade",
        "derivation_method": "read no_trade summary row and existing event windows",
        "source_method": "backtest_sim_outcome_no_trade",
    },
    {
        "metric_name": "qqq_only_baseline",
        "baseline_id": "qqq_only",
        "derivation_method": (
            "apply QQQ 100% hold-period baseline over existing event windows using "
            "cached adjusted-close prices"
        ),
        "source_method": "price_cache_single_asset",
    },
    {
        "metric_name": "spy_only_baseline",
        "baseline_id": "spy_only",
        "derivation_method": (
            "apply SPY 100% hold-period baseline over existing event windows using "
            "cached adjusted-close prices"
        ),
        "source_method": "price_cache_single_asset",
    },
    {
        "metric_name": "equal_weight_etf_baseline",
        "baseline_id": "equal_weight_etf",
        "derivation_method": (
            "apply SPY/QQQ/SMH/SOXX equal-weight hold-period baseline over existing "
            "event windows using cached adjusted-close prices"
        ),
        "source_method": "price_cache_equal_weight_etf",
    },
)


def run_metric_source_map(
    *,
    as_of: date | None = None,
    candidate: str = readiness.TOP_FILTERED_CANDIDATE,
    source_variant: str | None = None,
    sim_outcome_id: str | None = None,
    sim_outcome_dir: Path = sim.DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    price_cache_path: Path = DEFAULT_ETF_PRICE_PATH,
    output_dir: Path = DEFAULT_METRIC_SOURCE_MAP_DIR,
    generated_at: datetime | None = None,
    _validate_output: bool = True,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    if not _text(source_variant):
        raise ValueError("source_variant must be explicit")
    resolved_source_variant = _text(source_variant)
    outcome_payload = _load_sim_outcome(sim_outcome_id=sim_outcome_id, output_dir=sim_outcome_dir)
    effective_as_of = as_of or _parse_date(outcome_payload.get("as_of")) or generated.date()
    if effective_as_of > generated.date():
        raise ValueError("metric source map as_of occurs after generated_at")
    _validate_sim_source(
        outcome_payload,
        requested_id=sim_outcome_id,
        effective_as_of=effective_as_of,
        generated=generated,
    )
    sim_source_bindings = _sim_source_bindings(outcome_payload)
    price_source = _optional_file_binding(price_cache_path)
    cost_policy_source = foundation._file_binding(
        cost_sensitivity.DEFAULT_COST_SENSITIVITY_CONFIG_PATH
    )
    outcome_summary = _mapping(outcome_payload.get("simulated_variant_summary"))
    summary_rows = _records(outcome_summary.get("summary"))
    source_row = _variant_row(summary_rows, resolved_source_variant)
    no_trade_row = _variant_row(summary_rows, "no_trade")
    window_rows = _dated_window_rows(_records(outcome_payload.get("simulated_outcome_windows")))
    price_symbols = _price_symbols(price_cache_path)
    cost_policy_exists = cost_sensitivity.DEFAULT_COST_SENSITIVITY_CONFIG_PATH.exists()

    candidate_metric_sources = [
        _candidate_metric_row(
            spec,
            outcome_payload=outcome_payload,
            candidate=candidate,
            source_variant=resolved_source_variant,
            source_row=source_row,
            no_trade_row=no_trade_row,
            cost_policy_exists=cost_policy_exists,
        )
        for spec in CANDIDATE_METRIC_SPECS
    ]
    baseline_metric_sources = [
        _baseline_metric_row(
            spec,
            outcome_payload=outcome_payload,
            window_rows=window_rows,
            summary_rows=summary_rows,
            price_cache_path=price_cache_path,
            price_symbols=price_symbols,
        )
        for spec in BASELINE_SPECS
    ]
    all_rows = [*candidate_metric_sources, *baseline_metric_sources]
    derivable_count = len([row for row in all_rows if row["derivable_now"]])
    missing_rows = [row for row in all_rows if not row["derivable_now"]]
    status = _source_map_status(all_rows)
    source_map_id = st._stable_id(
        "metric-source-map",
        candidate,
        resolved_source_variant,
        _text(outcome_payload.get("sim_outcome_id")),
        status,
        generated.isoformat(),
    )
    root = st._unique_dir(output_dir / source_map_id)
    root.mkdir(parents=True, exist_ok=False)
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_metric_source_map_report",
        "source_map_id": root.name,
        "candidate": candidate,
        "source_variant": resolved_source_variant,
        "as_of": effective_as_of.isoformat(),
        "requested_as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "metric_source_map_status": status,
        "observed_evidence_status": "INSUFFICIENT_DATA",
        "candidate_lineage_status": "UNBOUND_SIMULATION_VARIANT",
        "candidate_metric_sources": candidate_metric_sources,
        "baseline_metric_sources": baseline_metric_sources,
        "source_summary": {
            "candidate_metric_count": len(candidate_metric_sources),
            "baseline_metric_count": len(baseline_metric_sources),
            "total_metric_count": len(all_rows),
            "derivable_now_count": derivable_count,
            "missing_metric_count": len(missing_rows),
            "missing_metric_names": [row["metric_name"] for row in missing_rows],
            "cost_policy_exists": cost_policy_exists,
            "price_cache_exists": price_cache_path.exists(),
            "price_symbol_count": len(price_symbols),
            "event_window_count": len(window_rows),
        },
        "source_artifacts": {
            "backtest_sim_outcome": _source_artifact(outcome_payload),
            "cost_sensitivity_policy": {
                "path": str(cost_sensitivity.DEFAULT_COST_SENSITIVITY_CONFIG_PATH),
                "exists": cost_policy_exists,
                "production_effect": "none",
            },
            "price_cache": {
                "path": str(price_cache_path),
                "exists": price_cache_path.exists(),
                "production_effect": "none",
            },
        },
        "blocking_reasons": [
            f"{row['metric_name']}:{field}"
            for row in missing_rows
            for field in _texts(row.get("missing_fields"))
        ],
        "warnings": _source_map_warnings(outcome_payload, price_cache_path),
        "next_required_action": _next_action(status),
        "limitations": [
            "Source map identifies derivable metric inputs but does not materialize metrics.",
            "BACKTEST_SIMULATION sources are research-only and not PIT/live execution evidence.",
            "limited_adjustment is not treated as the filtered candidate without explicit lineage.",
            "No fixed window is promoted to general observed evidence.",
            "Price-derived baselines still require the benchmark materialization data gate.",
        ],
        **METRIC_SOURCE_MAP_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_metric_source_map_manifest",
        "source_map_id": root.name,
        "candidate": candidate,
        "source_variant": resolved_source_variant,
        "as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": status,
        "metric_source_map_status": status,
        "metric_source_map_manifest_path": str(root / "metric_source_map_manifest.json"),
        "metric_source_map_report_path": str(root / "metric_source_map_report.json"),
        "metric_source_map_markdown_path": str(root / "metric_source_map_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        "validation_path": str(root / "metric_source_map_validation.json"),
        **METRIC_SOURCE_MAP_SAFETY,
    }
    reader = render_metric_source_map_reader_brief(report)
    st._write_json(root / "metric_source_map_manifest.json", manifest)
    st._write_json(root / "metric_source_map_report.json", report)
    st._write_text(
        root / "metric_source_map_report.md",
        render_metric_source_map_report(manifest, report),
    )
    st._write_text(root / "reader_brief_section.md", reader)
    snapshot = {
        "schema_version": METRIC_SOURCE_MAP_INPUT_SCHEMA,
        "source_map_id": root.name,
        "generated_at": generated.isoformat(),
        "effective_as_of": effective_as_of.isoformat(),
        "candidate": candidate,
        "source_variant": resolved_source_variant,
        "sim_outcome_id": sim_outcome_id,
        "sim_outcome_dir": str(sim_outcome_dir.resolve()),
        "sim_source_sha256": _payload_sha256(outcome_payload),
        "sim_source_bindings": sim_source_bindings,
        "sim_source_reference": {
            "requested_artifact_id": sim_outcome_id,
            "source_dir": str(sim_outcome_dir.resolve()),
            "exists": bool(sim_outcome_id),
        },
        "price_source": price_source,
        "cost_policy_source": cost_policy_source,
        "lineage": {
            "candidate": candidate,
            "source_variant": resolved_source_variant,
            "candidate_lineage_status": "UNBOUND_SIMULATION_VARIANT",
            "outcome_mode": outcome_payload.get("outcome_mode"),
        },
        "view_hashes": foundation._view_hashes(root, METRIC_SOURCE_MAP_VIEWS),
    }
    foundation._write_snapshot(root / METRIC_SOURCE_MAP_SNAPSHOT, snapshot)
    st._write_latest_pointer(
        "latest_metric_source_map",
        root.name,
        root / "metric_source_map_manifest.json",
    )
    validation = (
        validate_metric_source_map_artifact(
            source_map_id=root.name,
            output_dir=output_dir,
            write_output=True,
        )
        if _validate_output
        else {"status": "NOT_RUN", "failed_check_count": 0, "checks": []}
    )
    return {
        "source_map_id": root.name,
        "source_map_dir": root,
        "manifest": manifest,
        "metric_source_map_report": report,
        "reader_brief_section": reader,
        "input_snapshot": snapshot,
        "metric_source_map_validation": validation,
    }


def metric_source_map_report_payload(
    *,
    source_map_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_METRIC_SOURCE_MAP_DIR,
) -> dict[str, Any]:
    root = st._artifact_dir(
        artifact_id=source_map_id,
        latest_pointer="latest_metric_source_map",
        latest=latest,
        output_dir=output_dir,
        required_name="metric_source_map_manifest.json",
    )
    payload = {
        **st._read_json(root / "metric_source_map_manifest.json"),
        "metric_source_map_report": st._read_json(root / "metric_source_map_report.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(
            encoding="utf-8"
        ),
        "source_map_dir": str(root),
    }
    snapshot = st._read_optional_json(root / METRIC_SOURCE_MAP_SNAPSHOT)
    if snapshot:
        payload["input_snapshot"] = snapshot
    validation = st._read_optional_json(root / "metric_source_map_validation.json")
    if validation:
        payload["metric_source_map_validation"] = validation
    return payload


def validate_metric_source_map_artifact(
    *,
    source_map_id: str,
    output_dir: Path = DEFAULT_METRIC_SOURCE_MAP_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / source_map_id
    checks, ok = diagnostics._snapshot_preflight(
        root=root,
        snapshot_name=METRIC_SOURCE_MAP_SNAPSHOT,
        schema=METRIC_SOURCE_MAP_INPUT_SCHEMA,
        id_key="source_map_id",
        artifact_id=source_map_id,
        view_names=METRIC_SOURCE_MAP_VIEWS,
    )
    validation = (
        diagnostics._validate_content(
            report_type="etf_dynamic_v3_metric_source_map_validation",
            artifact_id=source_map_id,
            checks=checks,
            rebuild=lambda: _rebuild_metric_source_map(root, source_map_id),
        )
        if ok
        else st._validation_payload(
            "etf_dynamic_v3_metric_source_map_validation", source_map_id, checks
        )
    )
    if write_output:
        st._write_json(root / "metric_source_map_validation.json", validation)
        st._write_text(
            root / "metric_source_map_validation.md",
            render_metric_source_map_validation_report(validation),
        )
    return validation


def _generated_time(value: datetime | None) -> datetime:
    generated = value or datetime.now(UTC)
    return _aware_utc(generated, "generated_at")


def _aware_utc(value: object, field: str) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(_text(value))
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ValueError(f"{field} must be timezone-aware UTC")
    return parsed.astimezone(UTC)


def _payload_sha256(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def _optional_file_binding(path: Path) -> dict[str, Any]:
    resolved = path.resolve()
    if resolved.is_file():
        return {"exists": True, **foundation._file_binding(resolved)}
    return {
        "path": str(resolved),
        "exists": False,
        "sha256": None,
        "size_bytes": None,
    }


def _validate_optional_file_binding(binding: Mapping[str, Any]) -> None:
    path = Path(_text(binding.get("path")))
    expected_exists = binding.get("exists") is True
    if expected_exists:
        foundation._validate_file_binding(binding)
    elif path.is_file():
        raise ValueError(f"optional metric source appeared after snapshot: {path}")


def _validate_sim_source(
    payload: Mapping[str, Any],
    *,
    requested_id: str | None,
    effective_as_of: date,
    generated: datetime,
) -> None:
    if not requested_id:
        if payload.get("source_status") != "MISSING_EXPLICIT_SOURCE":
            raise ValueError("metric source map must not resolve an implicit simulation source")
        return
    if _text(payload.get("sim_outcome_id")) != requested_id:
        raise ValueError("metric source map simulation source id mismatch")
    source_as_of = _parse_date(payload.get("as_of"))
    if source_as_of and (source_as_of > effective_as_of or source_as_of > generated.date()):
        raise ValueError("metric source map simulation source occurs after requested chronology")
    for row in _dated_window_rows(_records(payload.get("simulated_outcome_windows"))):
        start = _parse_date(row.get("start_date"))
        end = _parse_date(row.get("end_date"))
        if start is None or end is None or start > end:
            raise ValueError("metric source map simulation window chronology is invalid")
        if end > effective_as_of or end > generated.date():
            raise ValueError(
                "metric source map simulation window occurs after requested chronology"
            )


def _sim_source_bindings(payload: Mapping[str, Any]) -> dict[str, Any]:
    source_dir = Path(_text(payload.get("sim_outcome_dir")))
    names = (
        "sim_outcome_manifest.json",
        "simulated_outcome_windows.jsonl",
        "simulated_variant_summary.json",
        "outcome_input_snapshot.json",
    )
    if source_dir.is_dir() and all((source_dir / name).is_file() for name in names):
        return foundation._artifact_binding(
            kind="backtest_sim_outcome",
            artifact_id=_text(payload.get("sim_outcome_id")),
            root=source_dir,
            names=names,
        )
    return {}


def _rebuild_metric_source_map(root: Path, source_map_id: str) -> list[dict[str, Any]]:
    snapshot = st._read_json(root / METRIC_SOURCE_MAP_SNAPSHOT)
    _validate_optional_file_binding(_mapping(snapshot.get("price_source")))
    foundation._validate_file_binding(_mapping(snapshot.get("cost_policy_source")))
    sim_binding = _mapping(snapshot.get("sim_source_bindings"))
    if sim_binding:
        foundation._validate_artifact_binding(sim_binding, kind="backtest_sim_outcome")
    generated = _aware_utc(snapshot.get("generated_at"), "snapshot.generated_at")
    effective_as_of = date.fromisoformat(_text(snapshot.get("effective_as_of")))
    sim_outcome_id = _text(snapshot.get("sim_outcome_id")) or None
    sim_outcome_dir = Path(_text(snapshot.get("sim_outcome_dir")))
    live_payload = _load_sim_outcome(
        sim_outcome_id=sim_outcome_id,
        output_dir=sim_outcome_dir,
    )
    if _payload_sha256(live_payload) != snapshot.get("sim_source_sha256"):
        raise ValueError("metric source map simulation source drift")
    with TemporaryDirectory(prefix="eb4-metric-source-map-") as temp_dir:
        result = run_metric_source_map(
            as_of=effective_as_of,
            candidate=_text(snapshot.get("candidate")),
            source_variant=_text(snapshot.get("source_variant")),
            sim_outcome_id=sim_outcome_id,
            sim_outcome_dir=sim_outcome_dir,
            price_cache_path=Path(_text(_mapping(snapshot.get("price_source")).get("path"))),
            output_dir=Path(temp_dir),
            generated_at=generated,
            _validate_output=False,
        )
        expected_root = Path(result["source_map_dir"])
        expected = {
            name: _normalize_replay_root(
                (expected_root / name).read_bytes(), expected_root=expected_root, actual_root=root
            )
            for name in METRIC_SOURCE_MAP_VIEWS
        }
    if result["source_map_id"] != source_map_id:
        raise ValueError("metric source map id is not reproducible")
    return diagnostics._check_bytes(root, expected)


def _normalize_replay_root(payload: bytes, *, expected_root: Path, actual_root: Path) -> bytes:
    old = str(expected_root)
    new = str(actual_root)
    return payload.replace(old.encode(), new.encode()).replace(
        old.replace("\\", "\\\\").encode(),
        new.replace("\\", "\\\\").encode(),
    )


def render_metric_source_map_reader_brief(report: Mapping[str, Any]) -> str:
    summary = _mapping(report.get("source_summary"))
    derivable_count = summary.get("derivable_now_count")
    total_count = summary.get("total_metric_count")
    missing_metric_names = ", ".join(_texts(summary.get("missing_metric_names"))) or "none"
    return "\n".join(
        [
            "## Dynamic v3 Metric Source Map",
            "",
            f"- metric_source_map_id: {report.get('source_map_id')}",
            f"- metric_source_map_status: {report.get('metric_source_map_status')}",
            f"- candidate: {report.get('candidate')}",
            f"- source_variant: {report.get('source_variant')}",
            f"- derivable_now: {derivable_count}/{total_count}",
            f"- missing_metric_names: {missing_metric_names}",
            f"- next_required_action: {report.get('next_required_action')}",
            "- safety: source-map only; no metric materialization, no promotion, "
            "no broker/order, production_effect=none",
            "",
        ]
    )


def render_metric_source_map_report(
    manifest: Mapping[str, Any],
    report: Mapping[str, Any],
) -> str:
    lines = [
        "# Dynamic v3 Metric Source Map",
        "",
        f"- source_map_id: {report.get('source_map_id') or manifest.get('source_map_id')}",
        f"- status: {report.get('metric_source_map_status')}",
        f"- candidate: {report.get('candidate')}",
        f"- source_variant: {report.get('source_variant')}",
        f"- production_effect: {report.get('production_effect')}",
        f"- next_required_action: {report.get('next_required_action')}",
        "",
        "## Candidate Metric Sources",
        "",
        "|metric|source_scope|derivable_now|required_fields|missing_fields|",
        "|---|---|---|---|---|",
    ]
    for row in _records(report.get("candidate_metric_sources")):
        lines.append(_source_row_markdown(row))
    lines.extend(
        [
            "",
            "## Baseline Metric Sources",
            "",
            "|metric|source_scope|derivable_now|required_fields|missing_fields|",
            "|---|---|---|---|---|",
        ]
    )
    for row in _records(report.get("baseline_metric_sources")):
        lines.append(_source_row_markdown(row))
    lines.extend(
        [
            "",
            "## Safety Boundary",
            "",
            "- This report does not materialize cost or benchmark metrics.",
            "- This report does not rerun cost-sensitivity review or benchmark baseline control.",
            "- This report does not approve promotion, extended shadow, live trading, "
            "official target weights, broker/order, paper account mutation, or "
            "production mutation.",
            "",
        ]
    )
    return "\n".join(lines)


def render_metric_source_map_validation_report(validation: Mapping[str, Any]) -> str:
    lines = [
        "# Dynamic v3 Metric Source Map Validation",
        "",
        f"- source_map_id: {validation.get('source_map_id')}",
        f"- status: {validation.get('status')}",
        f"- checks: {validation.get('check_count')}",
        f"- failed: {validation.get('failed_check_count')}",
        f"- warnings: {validation.get('warning_check_count')}",
        "",
        "|check|status|",
        "|---|---|",
    ]
    for check in _records(validation.get("checks")):
        lines.append(f"|{_text(check.get('check_id'))}|{_text(check.get('status'))}|")
    lines.append("")
    return "\n".join(lines)


def _candidate_metric_row(
    spec: Mapping[str, Any],
    *,
    outcome_payload: Mapping[str, Any],
    candidate: str,
    source_variant: str,
    source_row: Mapping[str, Any],
    no_trade_row: Mapping[str, Any],
    cost_policy_exists: bool,
) -> dict[str, Any]:
    metric_name = _text(spec.get("metric_name"))
    contract_missing = _candidate_missing_fields(
        metric_name, source_row, no_trade_row, cost_policy_exists
    )
    missing = [
        *contract_missing,
        "validated_same_candidate_lineage_dated_metric_source",
    ]
    return {
        "metric_name": metric_name,
        "metric_group": "candidate",
        "candidate": candidate,
        "source_variant": source_variant,
        "source_artifact_id": _text(outcome_payload.get("sim_outcome_id")),
        "source_artifact_path": _text(outcome_payload.get("sim_outcome_manifest_path")),
        "source_scope": f"simulated_variant_summary[variant={source_variant}]",
        "derivation_method": _text(spec.get("derivation_method")),
        "required_fields": list(_texts(spec.get("required_fields"))),
        "missing_fields": missing,
        "source_contract_available": not contract_missing,
        "derivable_now": False,
        "observed_value": None,
        "materialized_by_source_map": False,
        "outcome_mode": _text(
            outcome_payload.get("outcome_mode"), "MISSING_EXPLICIT_SOURCE"
        ),
        "limitation": (
            "research-only source contract; simulation variant is not the filtered "
            "candidate and is not observed evidence"
        ),
    }


def _baseline_metric_row(
    spec: Mapping[str, Any],
    *,
    outcome_payload: Mapping[str, Any],
    window_rows: Sequence[Mapping[str, Any]],
    summary_rows: Sequence[Mapping[str, Any]],
    price_cache_path: Path,
    price_symbols: set[str],
) -> dict[str, Any]:
    baseline_id = _text(spec.get("baseline_id"))
    required_symbols = _baseline_required_symbols(baseline_id, outcome_payload)
    contract_missing = _baseline_missing_fields(
        baseline_id=baseline_id,
        summary_rows=summary_rows,
        window_rows=window_rows,
        price_cache_path=price_cache_path,
        price_symbols=price_symbols,
        required_symbols=required_symbols,
    )
    missing = [*contract_missing, "validated_dated_baseline_metric_source"]
    return {
        "metric_name": _text(spec.get("metric_name")),
        "metric_group": "baseline",
        "baseline_id": baseline_id,
        "source_method": _text(spec.get("source_method")),
        "source_artifact_id": _text(outcome_payload.get("sim_outcome_id")),
        "source_artifact_path": _text(outcome_payload.get("sim_outcome_manifest_path")),
        "source_scope": _baseline_source_scope(baseline_id),
        "derivation_method": _text(spec.get("derivation_method")),
        "required_fields": _baseline_required_fields(baseline_id, required_symbols),
        "missing_fields": missing,
        "source_contract_available": not contract_missing,
        "derivable_now": False,
        "observed_value": None,
        "materialized_by_source_map": False,
        "outcome_mode": _text(
            outcome_payload.get("outcome_mode"), "MISSING_EXPLICIT_SOURCE"
        ),
        "limitation": "research-only source contract; no observed baseline metric bound",
    }


def _candidate_missing_fields(
    metric_name: str,
    source_row: Mapping[str, Any],
    no_trade_row: Mapping[str, Any],
    cost_policy_exists: bool,
) -> list[str]:
    if not source_row:
        return ["summary[source_variant]"]
    missing: list[str] = []
    if metric_name == "turnover":
        _require_field(source_row, "avg_turnover", missing, "summary[variant].avg_turnover")
    elif metric_name == "rotation_count":
        _require_field(source_row, "event_count", missing, "summary[variant].event_count")
    elif metric_name == "gross_performance_proxy":
        _require_field(source_row, "avg_5d_return", missing, "summary[variant].avg_5d_return")
    elif metric_name == "net_performance_proxy":
        _require_field(source_row, "avg_5d_return", missing, "summary[variant].avg_5d_return")
        _require_field(source_row, "avg_turnover", missing, "summary[variant].avg_turnover")
        if not cost_policy_exists:
            missing.append("cost_sensitivity_policy.scenarios")
    elif metric_name == "drawdown_proxy":
        _require_field(
            source_row,
            "avg_max_drawdown_20d",
            missing,
            "summary[variant].avg_max_drawdown_20d",
        )
    elif metric_name == "candidate_return_proxy":
        relative = _float_or_none(source_row.get("avg_relative_to_no_trade_5d"))
        gross = _float_or_none(source_row.get("avg_5d_return"))
        baseline = _float_or_none(no_trade_row.get("avg_5d_return"))
        if relative is None and (gross is None or baseline is None):
            missing.append(
                "summary[variant].avg_relative_to_no_trade_5d OR "
                "(summary[variant].avg_5d_return + summary[no_trade].avg_5d_return)"
            )
    return missing


def _baseline_missing_fields(
    *,
    baseline_id: str,
    summary_rows: Sequence[Mapping[str, Any]],
    window_rows: Sequence[Mapping[str, Any]],
    price_cache_path: Path,
    price_symbols: set[str],
    required_symbols: set[str],
) -> list[str]:
    missing: list[str] = []
    if baseline_id == "no_trade":
        row = _variant_row(summary_rows, "no_trade")
        if not row:
            missing.append("summary[no_trade]")
        elif _float_or_none(row.get("avg_5d_return")) is None and not window_rows:
            missing.append("summary[no_trade].avg_5d_return OR available no_trade windows")
        return missing
    if not window_rows:
        missing.append("dated_simulated_outcome_windows[variant=no_trade]")
    if baseline_id == "static_allocation" and not required_symbols:
        missing.append("explicit_static_allocation_weights")
    if not price_cache_path.exists():
        missing.append("price_cache_path")
    missing_symbols = sorted(required_symbols - price_symbols)
    if missing_symbols:
        missing.append("price_cache_symbols:" + ",".join(missing_symbols))
    return missing


def _baseline_required_fields(baseline_id: str, required_symbols: set[str]) -> list[str]:
    if baseline_id == "no_trade":
        return [
            "simulated_variant_summary[variant=no_trade].avg_5d_return",
            "optional dated simulated_outcome_windows[variant=no_trade]",
        ]
    return [
        "dated_simulated_outcome_windows[variant=no_trade].start_date",
        "dated_simulated_outcome_windows[variant=no_trade].end_date",
        "price_cache.date",
        "price_cache.ticker_or_symbol",
        "price_cache.adj_close",
        "price_cache_symbols:" + ",".join(sorted(required_symbols)),
    ]


def _baseline_required_symbols(
    baseline_id: str,
    outcome_payload: Mapping[str, Any],
) -> set[str]:
    if baseline_id == "static_allocation":
        return {
            _text(symbol).upper()
            for symbol in _mapping(outcome_payload.get("static_allocation_weights"))
            if symbol.upper() != "CASH"
        }
    if baseline_id == "qqq_only":
        return {"QQQ"}
    if baseline_id == "spy_only":
        return {"SPY"}
    if baseline_id == "equal_weight_etf":
        return {"SPY", "QQQ", "SMH", "SOXX"}
    return set()


def _baseline_source_scope(baseline_id: str) -> str:
    if baseline_id == "no_trade":
        return "simulated_variant_summary[variant=no_trade]"
    return "simulated_outcome_windows + cached ETF adjusted-close prices"


def _source_map_status(rows: Sequence[Mapping[str, Any]]) -> str:
    derivable = [row for row in rows if row.get("derivable_now") is True]
    if len(derivable) == len(rows):
        return "METRIC_SOURCE_MAP_READY"
    if derivable:
        return "METRIC_SOURCE_MAP_PARTIAL"
    return "INSUFFICIENT_DATA"


def _source_map_warnings(
    outcome_payload: Mapping[str, Any],
    price_cache_path: Path,
) -> list[str]:
    warnings = ["metric_source_map:no_metrics_materialized"]
    if _text(outcome_payload.get("outcome_mode")) == "BACKTEST_SIMULATION":
        warnings.append("source:backtest_simulation_not_pit")
    if price_cache_path.exists():
        warnings.append("price_cache_requires_validate_data_before_materialization")
    return sorted(set(warnings))


def _next_action(status: str) -> str:
    if status == "METRIC_SOURCE_MAP_READY":
        return "materialize_only_derivable_cost_and_benchmark_metrics"
    if status == "METRIC_SOURCE_MAP_PARTIAL":
        return "materialize_derivable_metrics_and_keep_missing_metrics_blocked"
    return "bind_validated_same_lineage_dated_sources_before_materialization"


def _source_artifact(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "artifact_id": _text(payload.get("sim_outcome_id")),
        "path": _text(payload.get("sim_outcome_manifest_path")),
        "summary_path": _text(payload.get("simulated_variant_summary_path")),
        "window_path": _text(payload.get("simulated_outcome_windows_path")),
        "outcome_mode": _text(payload.get("outcome_mode"), "MISSING_EXPLICIT_SOURCE"),
        "production_effect": "none",
    }


def _load_sim_outcome(*, sim_outcome_id: str | None, output_dir: Path) -> dict[str, Any]:
    if not sim_outcome_id:
        return {
            "schema_version": st.SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_metric_source_map_missing_sim_source",
            "sim_outcome_id": "",
            "source_status": "MISSING_EXPLICIT_SOURCE",
            "outcome_mode": "MISSING_EXPLICIT_SOURCE",
            "simulated_variant_summary": {"summary": []},
            "simulated_outcome_windows": [],
            "production_effect": "none",
        }
    return sim.backtest_sim_outcome_report_payload(
        sim_outcome_id=sim_outcome_id,
        latest=False,
        output_dir=output_dir,
    )


def _dated_window_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for row in rows:
        if (
            _text(row.get("variant")) == "no_trade"
            and _text(row.get("outcome_status")) == "AVAILABLE"
            and bool(_text(row.get("start_date")))
            and bool(_text(row.get("end_date")))
        ):
            result.append(dict(row))
    return result


def _variant_row(rows: Sequence[Mapping[str, Any]], variant: str) -> dict[str, Any]:
    for row in rows:
        if _text(row.get("variant")) == variant:
            return dict(row)
    return {}


def _price_symbols(path: Path) -> set[str]:
    if not path.exists():
        return set()
    symbols: set[str] = set()
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                symbol = _text(row.get("ticker") or row.get("symbol")).upper()
                if symbol:
                    symbols.add(symbol)
    except OSError:
        return set()
    return symbols


def _require_field(
    row: Mapping[str, Any],
    field: str,
    missing: list[str],
    label: str,
) -> None:
    if _float_or_none(row.get(field)) is None:
        missing.append(label)


def _source_row_complete(row: Mapping[str, Any]) -> bool:
    return (
        bool(_text(row.get("metric_name")))
        and bool(_text(row.get("source_artifact_id")))
        and bool(_text(row.get("source_scope")))
        and bool(_text(row.get("derivation_method")))
        and bool(_texts(row.get("required_fields")))
        and isinstance(row.get("missing_fields"), list)
        and isinstance(row.get("derivable_now"), bool)
        and row.get("materialized_by_source_map") is False
    )


def _check(check_id: str, condition: bool) -> dict[str, str]:
    return {"check_id": check_id, "status": "PASS" if condition else "FAIL"}


def _source_row_markdown(row: Mapping[str, Any]) -> str:
    return (
        f"|{_md(row.get('metric_name'))}|"
        f"{_md(row.get('source_scope'))}|"
        f"{_md(row.get('derivable_now'))}|"
        f"{_md(', '.join(_texts(row.get('required_fields'))))}|"
        f"{_md(', '.join(_texts(row.get('missing_fields'))) or 'none')}|"
    )


def _md(value: object) -> str:
    text = _text(value)
    return text.replace("|", "\\|").replace("\n", " ")


def _parse_date(value: object) -> date | None:
    text = _text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _float_or_none(value: object) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value: object) -> int:
    try:
        if value is None or value == "":
            return 0
        return int(float(value))
    except (TypeError, ValueError):
        return 0


_mapping = st._mapping
_records = st._records
_text = st._text
_texts = st._texts
