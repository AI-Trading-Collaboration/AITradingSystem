from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_backtest_simulation as sim
from ai_trading_system.etf_portfolio import dynamic_v3_cost_sensitivity as cost
from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_health as health
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_weekly as weekly
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_diagnostics as diagnostics
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_foundation as foundation

DEFAULT_COST_METRICS_MATERIALIZATION_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "cost_metrics_materialization"
)

COST_METRICS_MATERIALIZATION_STATUSES = (
    "COST_INPUTS_AVAILABLE",
    "COST_INPUTS_PARTIAL",
    "INSUFFICIENT_COST_INPUTS",
)
COST_METRICS_MATERIALIZATION_SAFETY = {
    **st.SYSTEM_TARGET_SAFETY,
    "cost_metrics_materialization_only": True,
    "research_only": True,
    "manual_review_only": True,
    "backtest_simulation_evidence_only": False,
    "backtest_simulation_diagnostic_only": True,
    "execution_model_ready": False,
    "data_downloaded_by_materialization": False,
    "pipelines_executed_by_materialization": False,
    "strategy_optimized_by_materialization": False,
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
COST_METRICS_INPUT_SCHEMA = "cost_metrics_materialization_input_snapshot.v2"
COST_METRICS_VIEWS = (
    "cost_metrics_materialization_manifest.json",
    "cost_metrics_materialization_report.json",
    "cost_metrics_materialization_report.md",
    "candidate_cost_metrics.json",
    "reader_brief_section.md",
)
COST_METRICS_SNAPSHOT = "cost_metrics_materialization_input_snapshot.json"


def run_cost_metrics_materialization(
    *,
    as_of: date | None = None,
    candidate: str = readiness.TOP_FILTERED_CANDIDATE,
    source_variant: str | None = None,
    sim_outcome_id: str | None = None,
    sim_outcome_dir: Path = sim.DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    weekly_review_id: str | None = None,
    weekly_review_dir: Path = weekly.DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR,
    paper_shadow_health_id: str | None = None,
    paper_shadow_health_dir: Path = health.DEFAULT_PAPER_SHADOW_HEALTH_DIR,
    cost_sensitivity_output_dir: Path = cost.DEFAULT_COST_SENSITIVITY_REVIEW_DIR,
    output_dir: Path = DEFAULT_COST_METRICS_MATERIALIZATION_DIR,
    generated_at: datetime | None = None,
    _validate_output: bool = True,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    if not _text(source_variant):
        raise ValueError("source_variant must be explicit")
    resolved_source_variant = _text(source_variant)
    outcome_payload = _load_sim_outcome(sim_outcome_id=sim_outcome_id, output_dir=sim_outcome_dir)
    outcome_payload_sha = _payload_sha(outcome_payload)
    outcome_source_bindings = _outcome_source_bindings(outcome_payload)
    outcome_summary = _mapping(outcome_payload.get("simulated_variant_summary"))
    rows = _records(outcome_summary.get("summary"))
    source_row = _variant_row(rows, resolved_source_variant)
    baseline_row = _variant_row(rows, "no_trade")
    effective_as_of = as_of or _parse_date(outcome_payload.get("as_of")) or generated.date()
    if effective_as_of > generated.date():
        raise ValueError("cost metrics materialization as_of occurs after generated_at")
    _validate_outcome_source(outcome_payload, generated=generated)
    weekly_context = cost._weekly_source(
        weekly_review_id=weekly_review_id,
        output_dir=weekly_review_dir,
    )
    health_context = cost._health_source(
        health_id=paper_shadow_health_id,
        output_dir=paper_shadow_health_dir,
    )
    cost._validate_cost_sources(
        metrics_source={"status": "INSUFFICIENT_COST_INPUTS", "summary": {}},
        weekly_source=weekly_context,
        health_source=health_context,
        effective_as_of=effective_as_of,
        generated=generated,
    )
    context_source_bindings = cost._cost_source_bindings(
        metrics_source={},
        weekly_source=weekly_context,
        health_source=health_context,
    )
    materialized_metrics = _materialized_candidate_metrics(
        candidate=candidate,
        source_variant=resolved_source_variant,
        source_row=source_row,
        baseline_row=baseline_row,
        outcome_payload=outcome_payload,
        generated_at=generated,
        as_of=effective_as_of,
    )
    metric_statuses = _metric_statuses(materialized_metrics)
    pre_status = _materialization_status_from_metrics(metric_statuses)
    materialization_id = st._stable_id(
        "cost-metrics-materialization",
        candidate,
        resolved_source_variant,
        _text(outcome_payload.get("sim_outcome_id")),
        pre_status,
        generated.isoformat(),
    )
    root = st._unique_dir(output_dir / materialization_id)
    root.mkdir(parents=True, exist_ok=False)
    metrics_path = root / "candidate_cost_metrics.json"
    st._write_json(metrics_path, materialized_metrics)

    cost_result = cost.run_cost_sensitivity_review(
        as_of=effective_as_of,
        candidate_metrics_path=metrics_path,
        weekly_review_id=weekly_review_id,
        weekly_review_dir=weekly_review_dir,
        paper_shadow_health_id=paper_shadow_health_id,
        paper_shadow_health_dir=paper_shadow_health_dir,
        output_dir=cost_sensitivity_output_dir,
        generated_at=generated,
    )
    cost_review = _mapping(cost_result.get("cost_sensitivity_review"))
    final_status = _materialization_status(metric_statuses, cost_review)
    blocking_reasons = _blocking_reasons(metric_statuses, cost_review)
    warnings = _warnings(materialized_metrics, cost_review, source_row, baseline_row)
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_cost_metrics_materialization_report",
        "materialization_id": root.name,
        "candidate": candidate,
        "source_variant": resolved_source_variant,
        "candidate_to_source_mapping": {
            "candidate": candidate,
            "source_variant": resolved_source_variant,
            "mapping_reason": (
                "simulation variant is retained only as a diagnostic source reference; "
                "it is not the filtered candidate and cannot materialize cost evidence"
            ),
            "mapping_accepted": False,
            "candidate_lineage_status": "UNBOUND_SIMULATION_VARIANT",
        },
        "as_of": effective_as_of.isoformat(),
        "requested_as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "cost_metrics_materialization_status": final_status,
        "materialized_metrics": materialized_metrics,
        "required_metric_statuses": metric_statuses,
        "candidate_metrics_path": str(metrics_path),
        "cost_sensitivity_review_id": cost_result.get("review_id"),
        "cost_sensitivity_status": cost_review.get("cost_sensitivity_status"),
        "cost_sensitivity_validation_status": _mapping(
            cost_result.get("cost_sensitivity_validation")
        ).get("status"),
        "net_performance_proxy_by_scenario": _net_performance_by_scenario(cost_review),
        "source_artifacts": {
            "backtest_sim_outcome": _source_artifact(outcome_payload),
            "candidate_metrics": {
                "artifact_id": materialized_metrics.get("metrics_id"),
                "path": str(metrics_path),
                "status": "OK"
                if pre_status != "INSUFFICIENT_COST_INPUTS"
                else "INSUFFICIENT_COST_INPUTS",
            },
            "cost_sensitivity_review": {
                "artifact_id": cost_result.get("review_id"),
                "status": cost_review.get("cost_sensitivity_status"),
                "report_path": _mapping(cost_result.get("manifest")).get(
                    "cost_sensitivity_report_path"
                ),
                "validation_status": _mapping(
                    cost_result.get("cost_sensitivity_validation")
                ).get("status"),
            },
        },
        "blocking_reasons": blocking_reasons,
        "warnings": warnings,
        "next_required_action": _next_action(final_status, cost_review),
        "limitations": [
            "metrics are materialized from existing research artifacts only",
            "BACKTEST_SIMULATION is not PIT/live execution evidence; numeric fields remain null",
            "limited_adjustment is not treated as the filtered candidate",
            "fixed 5d simulation summaries are not generalized into dated cost evidence",
            "gross simulation values are never relabeled as net performance",
            "cost review rerun does not approve promotion, extended shadow, or production",
        ],
        **COST_METRICS_MATERIALIZATION_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_cost_metrics_materialization_manifest",
        "materialization_id": root.name,
        "candidate": candidate,
        "source_variant": resolved_source_variant,
        "as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": final_status,
        "cost_metrics_materialization_status": final_status,
        "candidate_metrics_path": str(metrics_path),
        "cost_sensitivity_review_id": cost_result.get("review_id"),
        "cost_sensitivity_status": cost_review.get("cost_sensitivity_status"),
        "cost_metrics_materialization_manifest_path": str(
            root / "cost_metrics_materialization_manifest.json"
        ),
        "cost_metrics_materialization_report_path": str(
            root / "cost_metrics_materialization_report.json"
        ),
        "cost_metrics_materialization_markdown_path": str(
            root / "cost_metrics_materialization_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        "validation_path": str(root / "cost_metrics_materialization_validation.json"),
        **COST_METRICS_MATERIALIZATION_SAFETY,
    }
    reader = render_cost_metrics_materialization_reader_brief(report)
    st._write_json(root / "cost_metrics_materialization_manifest.json", manifest)
    st._write_json(root / "cost_metrics_materialization_report.json", report)
    st._write_text(
        root / "cost_metrics_materialization_report.md",
        render_cost_metrics_materialization_report(manifest, report),
    )
    st._write_text(root / "reader_brief_section.md", reader)
    nested_root = Path(cost_result["review_dir"])
    nested_snapshot = _mapping(cost_result.get("input_snapshot"))
    nested_binding = foundation._artifact_binding(
        kind="cost_sensitivity_review",
        artifact_id=_text(cost_result.get("review_id")),
        root=nested_root,
        names=(
            *cost.COST_SENSITIVITY_VIEWS,
            cost.COST_SENSITIVITY_SNAPSHOT,
            "cost_sensitivity_validation.json",
        ),
    )
    snapshot = {
        "schema_version": COST_METRICS_INPUT_SCHEMA,
        "materialization_id": root.name,
        "generated_at": generated.isoformat(),
        "effective_as_of": effective_as_of.isoformat(),
        "outcome_payload_sha256": outcome_payload_sha,
        "outcome_source": {
            "requested_source_dir": str(sim_outcome_dir.resolve()),
            "declared_manifest_path": _text(
                outcome_payload.get("sim_outcome_manifest_path")
            ),
            "payload_sha256": outcome_payload_sha,
            "binding_mode": "ARTIFACT_FILES"
            if outcome_source_bindings
            else "UNBOUND_DIAGNOSTIC_PAYLOAD",
        },
        "outcome_source_bindings": outcome_source_bindings,
        "context_source_bindings": context_source_bindings,
        "source_lineage": {
            "candidate": candidate,
            "source_variant": resolved_source_variant,
            "resolved_sim_outcome_id": outcome_payload.get("sim_outcome_id"),
            "outcome_mode": _text(outcome_payload.get("outcome_mode"), "BACKTEST_SIMULATION"),
            "candidate_lineage_status": "UNBOUND_SIMULATION_VARIANT",
            "mapping_accepted": False,
        },
        "cost_review_source": nested_binding,
        "cost_policy_source": nested_snapshot.get("policy_source"),
        "cost_policy_lineage": nested_snapshot.get("policy_lineage"),
        "replay": {
            "candidate": candidate,
            "source_variant": resolved_source_variant,
            "sim_outcome_id": _text(outcome_payload.get("sim_outcome_id")) or sim_outcome_id,
            "sim_outcome_dir": str(sim_outcome_dir.resolve()),
            "weekly_review_id": weekly_review_id,
            "weekly_review_dir": str(weekly_review_dir.resolve()),
            "paper_shadow_health_id": paper_shadow_health_id,
            "paper_shadow_health_dir": str(paper_shadow_health_dir.resolve()),
        },
        "view_hashes": foundation._view_hashes(root, COST_METRICS_VIEWS),
    }
    foundation._write_snapshot(root / COST_METRICS_SNAPSHOT, snapshot)
    st._write_latest_pointer(
        "latest_cost_metrics_materialization",
        root.name,
        root / "cost_metrics_materialization_manifest.json",
    )
    validation = (
        validate_cost_metrics_materialization_artifact(
            materialization_id=root.name,
            output_dir=output_dir,
            write_output=True,
        )
        if _validate_output
        else {"status": "NOT_RUN", "failed_check_count": 0, "checks": []}
    )
    return {
        "materialization_id": root.name,
        "materialization_dir": root,
        "manifest": manifest,
        "cost_metrics_materialization_report": report,
        "reader_brief_section": reader,
        "input_snapshot": snapshot,
        "cost_metrics_materialization_validation": validation,
        "cost_sensitivity_result": cost_result,
    }


def cost_metrics_materialization_report_payload(
    *,
    materialization_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_COST_METRICS_MATERIALIZATION_DIR,
) -> dict[str, Any]:
    root = st._artifact_dir(
        artifact_id=materialization_id,
        latest_pointer="latest_cost_metrics_materialization",
        latest=latest,
        output_dir=output_dir,
        required_name="cost_metrics_materialization_manifest.json",
    )
    payload = {
        **st._read_json(root / "cost_metrics_materialization_manifest.json"),
        "cost_metrics_materialization_report": st._read_json(
            root / "cost_metrics_materialization_report.json"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(
            encoding="utf-8"
        ),
        "materialization_dir": str(root),
    }
    snapshot = st._read_optional_json(root / COST_METRICS_SNAPSHOT)
    if snapshot:
        payload["input_snapshot"] = snapshot
    validation = st._read_optional_json(root / "cost_metrics_materialization_validation.json")
    if validation:
        payload["cost_metrics_materialization_validation"] = validation
    return payload


def validate_cost_metrics_materialization_artifact(
    *,
    materialization_id: str,
    output_dir: Path = DEFAULT_COST_METRICS_MATERIALIZATION_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / materialization_id
    checks, ok = diagnostics._snapshot_preflight(
        root=root,
        snapshot_name=COST_METRICS_SNAPSHOT,
        schema=COST_METRICS_INPUT_SCHEMA,
        id_key="materialization_id",
        artifact_id=materialization_id,
        view_names=COST_METRICS_VIEWS,
    )
    validation = (
        diagnostics._validate_content(
            report_type="etf_dynamic_v3_cost_metrics_materialization_validation",
            artifact_id=materialization_id,
            checks=checks,
            rebuild=lambda: _rebuild_cost_metrics_materialization(root, materialization_id),
        )
        if ok
        else st._validation_payload(
            "etf_dynamic_v3_cost_metrics_materialization_validation",
            materialization_id,
            checks,
        )
    )
    if write_output:
        st._write_json(root / "cost_metrics_materialization_validation.json", validation)
        st._write_text(
            root / "cost_metrics_materialization_validation.md",
            render_cost_metrics_materialization_validation_report(validation),
        )
    return validation


def _generated_time(value: datetime | None) -> datetime:
    generated = value or datetime.now(UTC)
    if generated.tzinfo is None or generated.utcoffset() != UTC.utcoffset(generated):
        raise ValueError("generated_at must be timezone-aware UTC")
    return generated.astimezone(UTC)


def _aware_utc(value: object, field: str) -> datetime:
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(_text(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be an ISO datetime") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ValueError(f"{field} must be timezone-aware UTC")
    return parsed.astimezone(UTC)


def _payload_sha(payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _outcome_source_bindings(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    if payload.get("source_status") == "MISSING_EXPLICIT_SOURCE":
        return []
    root_text = _text(payload.get("sim_outcome_dir"))
    root = (
        Path(root_text)
        if root_text
        else Path(_text(payload.get("sim_outcome_manifest_path"))).parent
    )
    if not root.is_dir():
        return []
    bindings: list[dict[str, Any]] = []
    for name in (
        "sim_outcome_manifest.json",
        "simulated_outcome_windows.jsonl",
        "simulated_variant_summary.json",
        "outcome_input_snapshot.json",
        "backtest_sim_outcome_report.md",
    ):
        path = root / name
        if path.is_file():
            bindings.append({"role": "backtest_sim_outcome", **foundation._file_binding(path)})
    if not any(
        binding.get("path", "").endswith("sim_outcome_manifest.json")
        for binding in bindings
    ):
        raise ValueError("sim outcome source directory lacks manifest")
    return bindings


def _validate_outcome_source(payload: Mapping[str, Any], *, generated: datetime) -> None:
    if payload.get("source_status") == "MISSING_EXPLICIT_SOURCE":
        return
    if not _text(payload.get("sim_outcome_id")):
        raise ValueError("sim outcome source id is required")
    summary = _mapping(payload.get("simulated_variant_summary"))
    outcome_mode = _text(payload.get("outcome_mode"), _text(summary.get("outcome_mode")))
    if outcome_mode != "BACKTEST_SIMULATION":
        raise ValueError("cost metrics diagnostic source must declare BACKTEST_SIMULATION")
    source_generated = _text(payload.get("generated_at"))
    if source_generated and _aware_utc(source_generated, "sim_outcome.generated_at") > generated:
        raise ValueError("sim outcome source occurs after generated_at")
    if not st._payload_safe(payload):
        raise ValueError("sim outcome source violates research safety boundary")


def _rebuild_cost_metrics_materialization(
    root: Path, materialization_id: str
) -> list[dict[str, Any]]:
    snapshot = st._read_json(root / COST_METRICS_SNAPSHOT)
    for binding in _records(snapshot.get("outcome_source_bindings")):
        foundation._validate_file_binding(binding)
    for binding in _records(snapshot.get("context_source_bindings")):
        foundation._validate_file_binding(binding)
    cost_policy_source = _mapping(snapshot.get("cost_policy_source"))
    foundation._validate_file_binding(cost_policy_source)
    live_policy = cost.load_cost_sensitivity_policy(
        Path(_text(cost_policy_source.get("path")))
    )
    cost_policy_lineage = _mapping(snapshot.get("cost_policy_lineage"))
    if live_policy.get("policy_id") != cost_policy_lineage.get(
        "policy_id"
    ) or live_policy.get("version") != cost_policy_lineage.get("policy_version"):
        raise ValueError("cost materialization policy lineage drift")
    nested_binding = _mapping(snapshot.get("cost_review_source"))
    foundation._validate_artifact_binding(nested_binding, kind="cost_sensitivity_review")
    nested_root = Path(_text(nested_binding.get("source_dir")))
    nested_id = _text(nested_binding.get("artifact_id"))
    if cost.validate_cost_sensitivity_artifact(
        review_id=nested_id,
        output_dir=nested_root.parent,
        write_output=False,
    ).get("status") != "PASS":
        raise ValueError("nested cost sensitivity source validation failed")
    generated = _aware_utc(snapshot.get("generated_at"), "snapshot.generated_at")
    effective_as_of = date.fromisoformat(_text(snapshot.get("effective_as_of")))
    replay = _mapping(snapshot.get("replay"))
    live_payload = _load_sim_outcome(
        sim_outcome_id=_text(replay.get("sim_outcome_id")) or None,
        output_dir=Path(_text(replay.get("sim_outcome_dir"))),
    )
    _validate_outcome_source(live_payload, generated=generated)
    if _payload_sha(live_payload) != snapshot.get("outcome_payload_sha256"):
        raise ValueError("sim outcome live payload drift")
    lineage = _mapping(snapshot.get("source_lineage"))
    if live_payload.get("sim_outcome_id") != lineage.get("resolved_sim_outcome_id"):
        raise ValueError("sim outcome lineage id drift")
    with TemporaryDirectory(prefix="eb4-cost-metrics-") as material_temp, TemporaryDirectory(
        prefix="eb4-cost-review-"
    ) as cost_temp:
        result = run_cost_metrics_materialization(
            as_of=effective_as_of,
            candidate=_text(replay.get("candidate")),
            source_variant=_text(replay.get("source_variant")),
            sim_outcome_id=_text(replay.get("sim_outcome_id")) or None,
            sim_outcome_dir=Path(_text(replay.get("sim_outcome_dir"))),
            weekly_review_id=_text(replay.get("weekly_review_id")) or None,
            weekly_review_dir=Path(_text(replay.get("weekly_review_dir"))),
            paper_shadow_health_id=_text(replay.get("paper_shadow_health_id")) or None,
            paper_shadow_health_dir=Path(_text(replay.get("paper_shadow_health_dir"))),
            cost_sensitivity_output_dir=Path(cost_temp),
            output_dir=Path(material_temp),
            generated_at=generated,
            _validate_output=False,
        )
        expected_root = Path(result["materialization_dir"])
        expected_nested_root = Path(result["cost_sensitivity_result"]["review_dir"])
        expected = {
            name: _normalize_replay_roots(
                (expected_root / name).read_bytes(),
                replacements=(
                    (expected_root, root),
                    (expected_nested_root, nested_root),
                ),
            )
            for name in COST_METRICS_VIEWS
        }
    if result["materialization_id"] != materialization_id:
        raise ValueError("cost metrics materialization id is not reproducible")
    if result["cost_sensitivity_result"]["review_id"] != nested_id:
        raise ValueError("nested cost sensitivity review id is not reproducible")
    return diagnostics._check_bytes(root, expected)


def _normalize_replay_roots(
    payload: bytes, *, replacements: tuple[tuple[Path, Path], ...]
) -> bytes:
    normalized = payload
    for expected, actual in replacements:
        old = str(expected)
        new = str(actual)
        normalized = normalized.replace(old.encode(), new.encode()).replace(
            old.replace("\\", "\\\\").encode(),
            new.replace("\\", "\\\\").encode(),
        )
    return normalized


def render_cost_metrics_materialization_reader_brief(report: Mapping[str, Any]) -> str:
    metrics = _mapping(report.get("materialized_metrics"))
    return "\n".join(
        [
            "## Cost Metrics Materialization",
            "",
            f"- cost_metrics_materialization_id: {report.get('materialization_id')}",
            "- cost_metrics_materialization_status: "
            f"{report.get('cost_metrics_materialization_status')}",
            f"- candidate: {report.get('candidate')}",
            f"- source_variant: {report.get('source_variant')}",
            f"- turnover: {metrics.get('turnover')}",
            f"- gross_performance_proxy: {metrics.get('gross_performance_proxy')}",
            f"- gross_improvement_proxy: {metrics.get('gross_improvement_proxy')}",
            f"- drawdown_proxy: {metrics.get('drawdown_proxy')}",
            f"- trade_rotation_count: {metrics.get('trade_rotation_count')}",
            f"- cost_sensitivity_review_id: {report.get('cost_sensitivity_review_id')}",
            f"- cost_sensitivity_status: {report.get('cost_sensitivity_status')}",
            f"- blocking_reasons: {_joined_texts(report.get('blocking_reasons'))}",
            f"- warnings: {_joined_texts(report.get('warnings'))}",
            f"- next_required_action: {report.get('next_required_action')}",
            "- safety_boundary: research-only metrics materialization / no execution model / "
            "no official target / no broker / no production",
            "",
        ]
    )


def render_cost_metrics_materialization_report(
    manifest: Mapping[str, Any],
    report: Mapping[str, Any],
) -> str:
    metric_lines = [
        f"- {key}: status={value} value={_mapping(report.get('materialized_metrics')).get(key)}"
        for key, value in sorted(_mapping(report.get("required_metric_statuses")).items())
    ]
    net_lines = [
        f"- {key}: {value}"
        for key, value in sorted(_mapping(report.get("net_performance_proxy_by_scenario")).items())
    ]
    return "\n".join(
        [
            f"# Cost Metrics Materialization {manifest.get('materialization_id')}",
            "",
            "## Purpose",
            (
                "Check whether existing sources provide lineage-bound dated candidate "
                "cost inputs and rerun the fail-closed cost review."
            ),
            "",
            "## Summary",
            f"- status: {report.get('cost_metrics_materialization_status')}",
            f"- candidate: {report.get('candidate')}",
            f"- source_variant: {report.get('source_variant')}",
            f"- candidate_metrics_path: {report.get('candidate_metrics_path')}",
            f"- cost_sensitivity_review_id: {report.get('cost_sensitivity_review_id')}",
            f"- cost_sensitivity_status: {report.get('cost_sensitivity_status')}",
            f"- blocking_reasons: {_joined_texts(report.get('blocking_reasons'))}",
            f"- warnings: {_joined_texts(report.get('warnings'))}",
            f"- next_required_action: {report.get('next_required_action')}",
            "",
            "## Required Metrics",
            *metric_lines,
            "",
            "## Net Performance Proxy By Scenario",
            *net_lines,
            "",
            "## Candidate Mapping",
            f"- candidate: {_mapping(report.get('candidate_to_source_mapping')).get('candidate')}",
            "- source_variant: "
            f"{_mapping(report.get('candidate_to_source_mapping')).get('source_variant')}",
            "- mapping_reason: "
            f"{_mapping(report.get('candidate_to_source_mapping')).get('mapping_reason')}",
            "",
            "## Safety Boundary",
            "- research-only materialization",
            "- backtest simulation diagnostic source only; not candidate evidence",
            "- no new optimization or backtest execution",
            (
                "- no execution model, broker action, order ticket, official target, "
                "or production mutation"
            ),
            "",
        ]
    )


def render_cost_metrics_materialization_validation_report(
    validation: Mapping[str, Any],
) -> str:
    checks = [
        f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}"
        for row in _records(validation.get("checks"))
    ]
    return "\n".join(
        [
            f"# Cost Metrics Materialization Validation {validation.get('artifact_id')}",
            "",
            f"- status: {validation.get('status')}",
            f"- failed_check_count: {validation.get('failed_check_count')}",
            "- production_effect: none",
            "",
            "## Checks",
            *checks,
            "",
        ]
    )


def _load_sim_outcome(*, sim_outcome_id: str | None, output_dir: Path) -> dict[str, Any]:
    if not sim_outcome_id:
        return {
            "schema_version": st.SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_missing_sim_outcome_source",
            "sim_outcome_id": "",
            "source_status": "MISSING_EXPLICIT_SOURCE",
            "outcome_mode": "MISSING_EXPLICIT_SOURCE",
            "simulated_variant_summary": {"summary": []},
            "production_effect": "none",
        }
    return sim.backtest_sim_outcome_report_payload(
        sim_outcome_id=sim_outcome_id,
        latest=False,
        output_dir=output_dir,
    )


def _materialized_candidate_metrics(
    *,
    candidate: str,
    source_variant: str,
    source_row: Mapping[str, Any],
    baseline_row: Mapping[str, Any],
    outcome_payload: Mapping[str, Any],
    generated_at: datetime,
    as_of: date,
) -> dict[str, Any]:
    metrics_id = st._stable_id(
        "candidate-cost-metrics",
        candidate,
        source_variant,
        _text(outcome_payload.get("sim_outcome_id")),
        generated_at.isoformat(),
    )
    return {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_candidate_cost_metrics",
        "metrics_id": metrics_id,
        "candidate": candidate,
        "source_variant": source_variant,
        "as_of": as_of.isoformat(),
        "generated_at": generated_at.isoformat(),
        "metric_source": "backtest_sim_outcome.simulated_variant_summary",
        "source_artifact_id": _text(outcome_payload.get("sim_outcome_id")),
        "source_artifact_path": _text(outcome_payload.get("sim_outcome_manifest_path")),
        "outcome_mode": "BACKTEST_SIMULATION",
        "pit_safety_status": "BACKTEST_SIMULATION_NOT_PIT",
        "validation_status": "NOT_APPLICABLE",
        "evidence_status": "INSUFFICIENT_DATA",
        "candidate_lineage_id": None,
        "window_start": None,
        "window_end": None,
        "turnover": None,
        "gross_performance_proxy": None,
        "baseline_performance_proxy": None,
        "gross_improvement_proxy": None,
        "drawdown_proxy": None,
        "trade_rotation_count": None,
        "available_count": None,
        "win_rate_vs_no_trade_5d": None,
        "candidate_to_source_mapping": {
            "candidate": candidate,
            "source_variant": source_variant,
            "source_candidate_row_exists": bool(source_row),
            "baseline_variant": "no_trade",
            "mapping_accepted": False,
            "candidate_lineage_status": "UNBOUND_SIMULATION_VARIANT",
        },
        "limitation": (
            "BACKTEST_SIMULATION and fixed-window proxy fields are diagnostic only; "
            "same-candidate lineage-bound dated metrics are unavailable"
        ),
        **COST_METRICS_MATERIALIZATION_SAFETY,
    }


def _metric_statuses(metrics: Mapping[str, Any]) -> dict[str, str]:
    return {
        "turnover": _available(metrics.get("turnover")),
        "gross_performance_proxy": _available(metrics.get("gross_performance_proxy")),
        "baseline_performance_proxy": _available(metrics.get("baseline_performance_proxy")),
        "gross_improvement_proxy": _available(metrics.get("gross_improvement_proxy")),
        "drawdown_proxy": _available(metrics.get("drawdown_proxy")),
        "trade_rotation_count": _available(metrics.get("trade_rotation_count")),
    }


def _materialization_status_from_metrics(metric_statuses: Mapping[str, Any]) -> str:
    cost_required = ("turnover", "gross_performance_proxy", "gross_improvement_proxy")
    if any(_text(metric_statuses.get(key)) != "AVAILABLE" for key in cost_required):
        return "INSUFFICIENT_COST_INPUTS"
    if any(_text(value) != "AVAILABLE" for value in metric_statuses.values()):
        return "COST_INPUTS_PARTIAL"
    return "COST_INPUTS_AVAILABLE"


def _materialization_status(
    metric_statuses: Mapping[str, Any],
    cost_review: Mapping[str, Any],
) -> str:
    status = _materialization_status_from_metrics(metric_statuses)
    if _text(cost_review.get("cost_sensitivity_status")) == "INSUFFICIENT_COST_INPUTS":
        return "INSUFFICIENT_COST_INPUTS"
    return status


def _blocking_reasons(
    metric_statuses: Mapping[str, Any],
    cost_review: Mapping[str, Any],
) -> list[str]:
    reasons = [
        f"{key}:missing"
        for key, value in metric_statuses.items()
        if _text(value) != "AVAILABLE"
        and key in {"turnover", "gross_performance_proxy", "gross_improvement_proxy"}
    ]
    if _text(cost_review.get("cost_sensitivity_status")) == "INSUFFICIENT_COST_INPUTS":
        reasons.append("cost_sensitivity_review:insufficient_cost_inputs")
    return sorted(set(reasons))


def _warnings(
    metrics: Mapping[str, Any],
    cost_review: Mapping[str, Any],
    source_row: Mapping[str, Any],
    baseline_row: Mapping[str, Any],
) -> list[str]:
    warnings: list[str] = []
    if not source_row:
        warnings.append("source_variant_row_missing")
    if not baseline_row:
        warnings.append("baseline_no_trade_row_missing")
    if _text(metrics.get("outcome_mode")) == "BACKTEST_SIMULATION":
        warnings.append("metrics_source:backtest_simulation_not_pit")
    if _text(cost_review.get("cost_sensitivity_status")) in {
        "NOT_MEANINGFUL_UNDER_COSTS",
        "MEANINGFUL_LOW_MEDIUM_ONLY",
    }:
        warnings.append(
            f"cost_sensitivity_review:{_text(cost_review.get('cost_sensitivity_status')).lower()}"
        )
    return sorted(set(warnings))


def _next_action(status: str, cost_review: Mapping[str, Any]) -> str:
    if status == "INSUFFICIENT_COST_INPUTS":
        return "identify_existing_numeric_cost_metrics_or_keep_cost_review_insufficient"
    if _text(cost_review.get("cost_sensitivity_status")) == "NOT_MEANINGFUL_UNDER_COSTS":
        return "keep_promotion_blocked_until_candidate_net_improvement_survives_costs"
    if status == "COST_INPUTS_PARTIAL":
        return "review_partial_cost_metrics_before_promotion_board_use"
    return "use_materialized_cost_metrics_as_research_cost_review_input_only"


def _net_performance_by_scenario(cost_review: Mapping[str, Any]) -> dict[str, Any]:
    return {
        _text(row.get("scenario_id")): row.get("net_performance_proxy")
        for row in _records(cost_review.get("scenario_results"))
    }


def _source_artifact(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "artifact_id": _text(payload.get("sim_outcome_id")),
        "path": _text(payload.get("sim_outcome_manifest_path")),
        "status": _text(payload.get("status"), "AVAILABLE"),
        "outcome_mode": "BACKTEST_SIMULATION",
    }


def _variant_row(rows: list[Mapping[str, Any]], variant: str) -> dict[str, Any]:
    for row in rows:
        if _text(row.get("variant")) == variant:
            return dict(row)
    return {}


def _available(value: object) -> str:
    return "AVAILABLE" if _float_or_none(value) is not None else "MISSING"


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


def _round_or_none(value: object) -> float | None:
    parsed = _float_or_none(value)
    return None if parsed is None else round(parsed, 6)


def _int_or_none(value: object) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _joined_texts(value: object, sep: str = ", ") -> str:
    return sep.join(_texts(value)) or "none"


_mapping = st._mapping
_records = st._records
_text = st._text
_texts = st._texts
