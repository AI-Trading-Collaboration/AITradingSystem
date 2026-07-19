from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_health as health
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_weekly as weekly
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_diagnostics as diagnostics
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_foundation as foundation

DEFAULT_COST_SENSITIVITY_CONFIG_PATH = (
    st.PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "cost_sensitivity_review_v1.yaml"
)
DEFAULT_COST_SENSITIVITY_REVIEW_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "cost_sensitivity_review"
)
REQUIRED_COST_SCENARIOS = ("zero", "low", "medium", "high")
COST_SENSITIVITY_STATUSES = (
    "MEANINGFUL_ALL_SCENARIOS",
    "MEANINGFUL_LOW_MEDIUM_ONLY",
    "NOT_MEANINGFUL_UNDER_COSTS",
    "INSUFFICIENT_COST_INPUTS",
    "BLOCKED_SOURCE",
)
COST_SENSITIVITY_SAFETY = {
    **st.SYSTEM_TARGET_SAFETY,
    "manual_review_only": True,
    "research_only": True,
    "cost_sensitivity_review_only": True,
    "execution_model_ready": False,
    "data_downloaded_by_review": False,
    "pipelines_executed_by_review": False,
    "official_target_weights": False,
    "official_target_weights_mutated": False,
    "not_official_target_weights": True,
    "broker_effect": "none",
    "order_effect": "none",
    "broker_action_allowed": False,
    "broker_action_taken": False,
    "order_ticket_generated": False,
    "paper_account_state_mutated": False,
    "production_state_mutated": False,
    "automatic_candidate_promotion": False,
    "auto_apply": False,
    "production_effect": "none",
}
COST_SENSITIVITY_INPUT_SCHEMA = "cost_sensitivity_review_input_snapshot.v2"
COST_SENSITIVITY_VIEWS = (
    "cost_sensitivity_manifest.json",
    "cost_sensitivity_review.json",
    "cost_sensitivity_report.md",
    "reader_brief_section.md",
)
COST_SENSITIVITY_SNAPSHOT = "cost_sensitivity_review_input_snapshot.json"


def load_cost_sensitivity_policy(
    config_path: Path = DEFAULT_COST_SENSITIVITY_CONFIG_PATH,
) -> dict[str, Any]:
    return _normalized_policy(st._load_yaml_mapping(config_path), config_path=config_path)


def run_cost_sensitivity_review(
    *,
    as_of: date | None = None,
    candidate_metrics_path: Path | None = None,
    candidate_metrics: Mapping[str, Any] | None = None,
    weekly_review_id: str | None = None,
    weekly_review_dir: Path = weekly.DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR,
    paper_shadow_health_id: str | None = None,
    paper_shadow_health_dir: Path = health.DEFAULT_PAPER_SHADOW_HEALTH_DIR,
    config_path: Path = DEFAULT_COST_SENSITIVITY_CONFIG_PATH,
    output_dir: Path = DEFAULT_COST_SENSITIVITY_REVIEW_DIR,
    generated_at: datetime | None = None,
    _validate_output: bool = True,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    policy_source = foundation._file_binding(config_path)
    policy = load_cost_sensitivity_policy(config_path)
    weekly_source = _weekly_source(
        weekly_review_id=weekly_review_id,
        output_dir=weekly_review_dir,
    )
    health_source = _health_source(
        health_id=paper_shadow_health_id,
        output_dir=paper_shadow_health_dir,
    )
    metrics_source = _candidate_metrics_source(
        metrics=candidate_metrics,
        metrics_path=candidate_metrics_path,
        weekly_source=weekly_source,
    )
    effective_as_of = (
        as_of
        or _parse_optional_date(metrics_source.get("as_of"))
        or _parse_optional_date(_mapping(weekly_source.get("summary")).get("week_end"))
        or generated.date()
    )
    if effective_as_of > generated.date():
        raise ValueError("cost sensitivity as_of occurs after generated_at")
    source_bindings = _cost_source_bindings(
        metrics_source=metrics_source,
        weekly_source=weekly_source,
        health_source=health_source,
    )
    _validate_cost_sources(
        metrics_source=metrics_source,
        weekly_source=weekly_source,
        health_source=health_source,
        effective_as_of=effective_as_of,
        generated=generated,
    )
    scenario_results = _scenario_results(
        policy=policy,
        metrics_summary=_mapping(metrics_source.get("summary")),
    )
    blocking_reasons = _blocking_reasons(
        weekly_source=weekly_source,
        metrics_source=metrics_source,
    )
    warnings = _warnings(health_source=health_source, metrics_source=metrics_source)
    cost_status = _cost_sensitivity_status(
        blocking_reasons=blocking_reasons,
        metrics_source=metrics_source,
        scenario_results=scenario_results,
    )
    candidate = _text(
        metrics_source.get("candidate"),
        _text(_mapping(weekly_source.get("summary")).get("candidate"), "UNKNOWN"),
    )
    review_id = st._stable_id(
        "cost-sensitivity-review",
        candidate,
        effective_as_of.isoformat(),
        _text(metrics_source.get("artifact_id")),
        _text(weekly_source.get("artifact_id")),
        _text(policy.get("policy_id")),
        _text(policy.get("version")),
        generated.isoformat(),
    )
    root = st._unique_dir(output_dir / review_id)
    root.mkdir(parents=True, exist_ok=False)
    promotion_board_inputs = _promotion_board_inputs(
        review_id=root.name,
        candidate=candidate,
        cost_status=cost_status,
        scenario_results=scenario_results,
        weekly_source=weekly_source,
        health_source=health_source,
        blocking_reasons=blocking_reasons,
        warnings=warnings,
    )
    review = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_cost_sensitivity_review",
        "review_id": root.name,
        "candidate": candidate,
        "as_of": effective_as_of.isoformat(),
        "requested_as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "config_path": str(config_path),
        "policy": policy,
        "policy_id": policy.get("policy_id"),
        "policy_version": policy.get("version"),
        "meaningful_improvement_threshold": _float(
            _mapping(policy.get("meaningful_improvement")).get("threshold"),
        ),
        "candidate_metrics_summary": metrics_source.get("summary"),
        "candidate_lineage_id": _mapping(metrics_source.get("summary")).get(
            "candidate_lineage_id"
        ),
        "evidence_status": _mapping(metrics_source.get("summary")).get(
            "evidence_status"
        ),
        "source_artifacts": {
            "candidate_metrics": metrics_source,
            "paper_shadow_weekly_review": weekly_source,
            "paper_shadow_health": health_source,
        },
        "cost_sensitivity_status": cost_status,
        "scenario_results": scenario_results,
        "scenario_count": len(scenario_results),
        "turnover": _mapping(metrics_source.get("summary")).get("turnover"),
        "gross_performance_proxy": _mapping(metrics_source.get("summary")).get(
            "gross_performance_proxy"
        ),
        "gross_improvement_proxy": _mapping(metrics_source.get("summary")).get(
            "gross_improvement_proxy"
        ),
        "worst_net_improvement_proxy": _worst_net_improvement(scenario_results),
        "high_cost_improvement_meaningful": _scenario_meaningful(
            scenario_results,
            "high",
        ),
        "low_cost_improvement_meaningful": _scenario_meaningful(
            scenario_results,
            "low",
        ),
        "medium_cost_improvement_meaningful": _scenario_meaningful(
            scenario_results,
            "medium",
        ),
        "blocking_reasons": blocking_reasons,
        "warnings": warnings,
        "weekly_review_input": _weekly_review_input(weekly_source),
        "promotion_board_inputs": promotion_board_inputs,
        "next_required_action": _next_required_action(cost_status),
        "limitations": [
            "research-level linear cost sensitivity over validated dated gross metrics only",
            (
                "legacy proxy fields, simulation variants, gross-as-net inputs, "
                "and missing-as-zero are rejected"
            ),
            "does not model live spreads, market impact, taxes, financing, or fills",
            "does not refresh market data or rerun paper-shadow source artifacts",
            "does not approve candidate promotion or production target weights",
        ],
        **COST_SENSITIVITY_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_cost_sensitivity_manifest",
        "review_id": root.name,
        "candidate": candidate,
        "as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": "PASS" if cost_status != "BLOCKED_SOURCE" else "BLOCKED_SOURCE",
        "cost_sensitivity_status": cost_status,
        "policy_id": policy.get("policy_id"),
        "policy_version": policy.get("version"),
        "cost_sensitivity_manifest_path": str(root / "cost_sensitivity_manifest.json"),
        "cost_sensitivity_review_path": str(root / "cost_sensitivity_review.json"),
        "cost_sensitivity_report_path": str(root / "cost_sensitivity_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        "validation_path": str(root / "cost_sensitivity_validation.json"),
        **COST_SENSITIVITY_SAFETY,
    }
    reader = render_cost_sensitivity_reader_brief(review)
    st._write_json(root / "cost_sensitivity_manifest.json", manifest)
    st._write_json(root / "cost_sensitivity_review.json", review)
    st._write_text(
        root / "cost_sensitivity_report.md",
        render_cost_sensitivity_report(manifest, review),
    )
    st._write_text(root / "reader_brief_section.md", reader)
    snapshot = {
        "schema_version": COST_SENSITIVITY_INPUT_SCHEMA,
        "review_id": root.name,
        "generated_at": generated.isoformat(),
        "effective_as_of": effective_as_of.isoformat(),
        "policy_source": policy_source,
        "policy_lineage": {
            "policy_id": policy.get("policy_id"),
            "policy_version": policy.get("version"),
        },
        "source_bindings": source_bindings,
        "source_lineage": {
            "candidate": candidate,
            "candidate_lineage_id": _mapping(metrics_source.get("summary")).get(
                "candidate_lineage_id"
            ),
            "metrics_id": metrics_source.get("artifact_id"),
            "weekly_review_id": weekly_source.get("artifact_id"),
            "paper_shadow_health_id": health_source.get("artifact_id"),
            "metrics_window_start": _mapping(metrics_source.get("summary")).get(
                "window_start"
            ),
            "metrics_window_end": _mapping(metrics_source.get("summary")).get(
                "window_end"
            ),
        },
        "replay": {
            "candidate_metrics_path": None
            if candidate_metrics_path is None
            else str(candidate_metrics_path.resolve()),
            "inline_metrics": dict(candidate_metrics) if candidate_metrics is not None else None,
            "weekly_review_id": weekly_review_id,
            "weekly_review_dir": str(weekly_review_dir.resolve()),
            "paper_shadow_health_id": paper_shadow_health_id,
            "paper_shadow_health_dir": str(paper_shadow_health_dir.resolve()),
        },
        "view_hashes": foundation._view_hashes(root, COST_SENSITIVITY_VIEWS),
    }
    foundation._write_snapshot(root / COST_SENSITIVITY_SNAPSHOT, snapshot)
    st._write_latest_pointer(
        "latest_cost_sensitivity_review",
        root.name,
        root / "cost_sensitivity_manifest.json",
    )
    validation = (
        validate_cost_sensitivity_artifact(
            review_id=root.name,
            output_dir=output_dir,
            write_output=True,
        )
        if _validate_output
        else {"status": "NOT_RUN", "failed_check_count": 0, "checks": []}
    )
    return {
        "review_id": root.name,
        "review_dir": root,
        "manifest": manifest,
        "cost_sensitivity_review": review,
        "reader_brief_section": reader,
        "input_snapshot": snapshot,
        "cost_sensitivity_validation": validation,
    }


def cost_sensitivity_report_payload(
    *,
    review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_COST_SENSITIVITY_REVIEW_DIR,
) -> dict[str, Any]:
    root = st._artifact_dir(
        artifact_id=review_id,
        latest_pointer="latest_cost_sensitivity_review",
        latest=latest,
        output_dir=output_dir,
        required_name="cost_sensitivity_manifest.json",
    )
    payload = {
        **st._read_json(root / "cost_sensitivity_manifest.json"),
        "cost_sensitivity_review": st._read_json(root / "cost_sensitivity_review.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(
            encoding="utf-8",
        ),
        "review_dir": str(root),
    }
    snapshot = st._read_optional_json(root / COST_SENSITIVITY_SNAPSHOT)
    if snapshot:
        payload["input_snapshot"] = snapshot
    validation = st._read_optional_json(root / "cost_sensitivity_validation.json")
    if validation:
        payload["cost_sensitivity_validation"] = validation
    return payload


def validate_cost_sensitivity_artifact(
    *,
    review_id: str,
    output_dir: Path = DEFAULT_COST_SENSITIVITY_REVIEW_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / review_id
    checks, ok = diagnostics._snapshot_preflight(
        root=root,
        snapshot_name=COST_SENSITIVITY_SNAPSHOT,
        schema=COST_SENSITIVITY_INPUT_SCHEMA,
        id_key="review_id",
        artifact_id=review_id,
        view_names=COST_SENSITIVITY_VIEWS,
    )
    validation = (
        diagnostics._validate_content(
            report_type="etf_dynamic_v3_cost_sensitivity_validation",
            artifact_id=review_id,
            checks=checks,
            rebuild=lambda: _rebuild_cost_sensitivity(root, review_id),
        )
        if ok
        else st._validation_payload(
            "etf_dynamic_v3_cost_sensitivity_validation", review_id, checks
        )
    )
    if write_output:
        st._write_json(root / "cost_sensitivity_validation.json", validation)
        st._write_text(
            root / "cost_sensitivity_validation.md",
            render_cost_sensitivity_validation_report(validation),
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


def _cost_source_bindings(
    *,
    metrics_source: Mapping[str, Any],
    weekly_source: Mapping[str, Any],
    health_source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    bindings: list[dict[str, Any]] = []
    metrics_path = Path(_text(metrics_source.get("source_path")))
    if metrics_path.is_file():
        bindings.append({"role": "candidate_metrics", **foundation._file_binding(metrics_path)})
    for role, source, names in (
        (
            "paper_shadow_weekly",
            weekly_source,
            (
                "paper_shadow_weekly_manifest.json",
                "paper_shadow_weekly_review.json",
                "paper_shadow_weekly_validation.json",
            ),
        ),
        (
            "paper_shadow_health",
            health_source,
            (
                "paper_shadow_health_manifest.json",
                "paper_shadow_health_report.json",
                "paper_shadow_health_validation.json",
            ),
        ),
    ):
        manifest_path = Path(_text(source.get("source_path")))
        if source.get("exists") is not True:
            continue
        if not manifest_path.is_file():
            raise ValueError(f"{role} manifest source missing: {manifest_path}")
        for name in names:
            bindings.append(
                {
                    "role": role,
                    "artifact_id": source.get("artifact_id"),
                    "name": name,
                    **foundation._file_binding(manifest_path.parent / name),
                }
            )
    return bindings


def _validate_cost_sources(
    *,
    metrics_source: Mapping[str, Any],
    weekly_source: Mapping[str, Any],
    health_source: Mapping[str, Any],
    effective_as_of: date,
    generated: datetime,
) -> None:
    for label, source in (
        ("paper_shadow_weekly", weekly_source),
        ("paper_shadow_health", health_source),
    ):
        if source.get("exists") is True and source.get("validation_status") != "PASS":
            raise ValueError(f"{label} source validation must PASS")
        source_generated = _text(_mapping(source.get("summary")).get("generated_at"))
        if source_generated and _aware_utc(source_generated, f"{label}.generated_at") > generated:
            raise ValueError(f"{label} source occurs after generated_at")
    metrics_summary = _mapping(metrics_source.get("summary"))
    metrics_end = _parse_optional_date(metrics_summary.get("window_end"))
    if metrics_end and metrics_end > effective_as_of:
        raise ValueError("candidate metrics window_end occurs after requested as_of")
    if metrics_source.get("status") != "OK":
        return
    metrics_generated = _aware_utc(
        metrics_summary.get("generated_at"), "candidate_metrics.generated_at"
    )
    if metrics_generated > generated:
        raise ValueError("candidate metrics source occurs after generated_at")
    candidate = _text(metrics_summary.get("candidate"))
    lineage = _text(metrics_summary.get("candidate_lineage_id"))
    for label, source in (
        ("paper_shadow_weekly", weekly_source),
        ("paper_shadow_health", health_source),
    ):
        summary = _mapping(source.get("summary"))
        source_candidate = _text(summary.get("candidate"))
        source_lineage = _text(summary.get("candidate_lineage_id"))
        if source_candidate and source_candidate != candidate:
            raise ValueError(f"{label} candidate lineage candidate mismatch")
        if source_lineage and source_lineage != lineage:
            raise ValueError(f"{label} candidate lineage id mismatch")


def _rebuild_cost_sensitivity(root: Path, review_id: str) -> list[dict[str, Any]]:
    snapshot = st._read_json(root / COST_SENSITIVITY_SNAPSHOT)
    policy_source = _mapping(snapshot.get("policy_source"))
    foundation._validate_file_binding(policy_source)
    for binding in _records(snapshot.get("source_bindings")):
        foundation._validate_file_binding(binding)
    policy = load_cost_sensitivity_policy(Path(_text(policy_source.get("path"))))
    policy_lineage = _mapping(snapshot.get("policy_lineage"))
    if policy.get("policy_id") != policy_lineage.get("policy_id") or policy.get(
        "version"
    ) != policy_lineage.get("policy_version"):
        raise ValueError("cost sensitivity policy lineage drift")
    generated = _aware_utc(snapshot.get("generated_at"), "snapshot.generated_at")
    effective_as_of = date.fromisoformat(_text(snapshot.get("effective_as_of")))
    replay = _mapping(snapshot.get("replay"))
    metrics_path_text = _text(replay.get("candidate_metrics_path"))
    with TemporaryDirectory(prefix="eb4-cost-sensitivity-") as temp_dir:
        result = run_cost_sensitivity_review(
            as_of=effective_as_of,
            candidate_metrics_path=Path(metrics_path_text) if metrics_path_text else None,
            candidate_metrics=_mapping(replay.get("inline_metrics"))
            if replay.get("inline_metrics") is not None
            else None,
            weekly_review_id=_text(replay.get("weekly_review_id")) or None,
            weekly_review_dir=Path(_text(replay.get("weekly_review_dir"))),
            paper_shadow_health_id=_text(replay.get("paper_shadow_health_id")) or None,
            paper_shadow_health_dir=Path(_text(replay.get("paper_shadow_health_dir"))),
            config_path=Path(_text(policy_source.get("path"))),
            output_dir=Path(temp_dir),
            generated_at=generated,
            _validate_output=False,
        )
        expected_root = Path(result["review_dir"])
        expected = {
            name: _normalize_replay_root(
                (expected_root / name).read_bytes(),
                expected_root=expected_root,
                actual_root=root,
            )
            for name in COST_SENSITIVITY_VIEWS
        }
    if result["review_id"] != review_id:
        raise ValueError("cost sensitivity review id is not reproducible")
    return diagnostics._check_bytes(root, expected)


def _normalize_replay_root(payload: bytes, *, expected_root: Path, actual_root: Path) -> bytes:
    old = str(expected_root)
    new = str(actual_root)
    return payload.replace(old.encode(), new.encode()).replace(
        old.replace("\\", "\\\\").encode(),
        new.replace("\\", "\\\\").encode(),
    )


def latest_cost_sensitivity_summary(
    *,
    review_id: str | None = None,
    output_dir: Path = DEFAULT_COST_SENSITIVITY_REVIEW_DIR,
) -> dict[str, Any]:
    if not review_id:
        return {
            "availability": "MISSING",
            "review_id": None,
            "limitation": "explicit cost sensitivity review id is required",
            "production_effect": "none",
        }
    try:
        payload = cost_sensitivity_report_payload(
            review_id=review_id,
            latest=False,
            output_dir=output_dir,
        )
        review = _mapping(payload.get("cost_sensitivity_review"))
        return {
            "availability": "AVAILABLE",
            "review_id": payload.get("review_id"),
            "candidate": review.get("candidate"),
            "cost_sensitivity_status": review.get("cost_sensitivity_status"),
            "high_cost_improvement_meaningful": review.get(
                "high_cost_improvement_meaningful"
            ),
            "worst_net_improvement_proxy": review.get("worst_net_improvement_proxy"),
            "policy_id": review.get("policy_id"),
            "policy_version": review.get("policy_version"),
            "validation_status": _mapping(
                payload.get("cost_sensitivity_validation")
            ).get("status", "NOT_RUN"),
            "report_path": payload.get("cost_sensitivity_report_path"),
            "next_required_action": review.get("next_required_action"),
        }
    except Exception as exc:
        return {
            "availability": "MISSING",
            "review_id": "MISSING",
            "cost_sensitivity_status": "MISSING",
            "high_cost_improvement_meaningful": "MISSING",
            "worst_net_improvement_proxy": "MISSING",
            "validation_status": "MISSING",
            "report_path": "",
            "next_required_action": "run_cost_sensitivity_review_before_promotion_board",
            "limitation": str(exc),
        }


def render_cost_sensitivity_reader_brief(review: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Cost Sensitivity Review",
            "",
            f"- cost_sensitivity_review_id: {review.get('review_id')}",
            f"- cost_sensitivity_candidate: {review.get('candidate')}",
            f"- cost_sensitivity_status: {review.get('cost_sensitivity_status')}",
            f"- policy_id: {review.get('policy_id')}",
            f"- policy_version: {review.get('policy_version')}",
            f"- turnover: {review.get('turnover')}",
            f"- gross_performance_proxy: {review.get('gross_performance_proxy')}",
            f"- gross_improvement_proxy: {review.get('gross_improvement_proxy')}",
            f"- worst_net_improvement_proxy: {review.get('worst_net_improvement_proxy')}",
            "- high_cost_improvement_meaningful: "
            f"{review.get('high_cost_improvement_meaningful')}",
            f"- blocking_reasons: {_joined_texts(review.get('blocking_reasons'))}",
            f"- warnings: {_joined_texts(review.get('warnings'))}",
            f"- next_required_action: {review.get('next_required_action')}",
            "- safety_boundary: research-only cost sensitivity / no broker / "
            "no order / no official target / no production",
            "",
        ]
    )


def render_cost_sensitivity_report(
    manifest: Mapping[str, Any],
    review: Mapping[str, Any],
) -> str:
    scenario_lines = [
        (
            f"| `{row.get('scenario_id')}` | {row.get('total_cost_bps')} | "
            f"{row.get('turnover')} | {row.get('cost_drag')} | "
            f"{row.get('gross_performance_proxy')} | "
            f"{row.get('net_performance_proxy')} | "
            f"{row.get('net_improvement_proxy')} | "
            f"{row.get('improvement_remains_meaningful')} |"
        )
        for row in _records(review.get("scenario_results"))
    ]
    return "\n".join(
        [
            f"# Cost Sensitivity Review {manifest.get('review_id')}",
            "",
            "## Purpose",
            "Estimate whether paper-shadow candidate improvement survives configured "
            "transaction-cost assumptions using research-level linear cost drag.",
            "",
            "## Summary",
            f"- candidate: {review.get('candidate')}",
            f"- cost_sensitivity_status: {review.get('cost_sensitivity_status')}",
            f"- policy: {review.get('policy_id')} / {review.get('policy_version')}",
            f"- meaningful_improvement_threshold: "
            f"{review.get('meaningful_improvement_threshold')}",
            f"- turnover: {review.get('turnover')}",
            f"- gross_performance_proxy: {review.get('gross_performance_proxy')}",
            f"- gross_improvement_proxy: {review.get('gross_improvement_proxy')}",
            f"- worst_net_improvement_proxy: {review.get('worst_net_improvement_proxy')}",
            "- high_cost_improvement_meaningful: "
            f"{review.get('high_cost_improvement_meaningful')}",
            f"- blocking_reasons: {_joined_texts(review.get('blocking_reasons'))}",
            f"- warnings: {_joined_texts(review.get('warnings'))}",
            f"- next_required_action: {review.get('next_required_action')}",
            "",
            "## Scenarios",
            "| scenario | cost bps | turnover | cost drag | gross perf | net perf | "
            "net improvement | meaningful |",
            "|---|---:|---:|---:|---:|---:|---:|---|",
            *scenario_lines,
            "",
            "## Promotion Board Input",
            f"- use: {_mapping(review.get('promotion_board_inputs')).get('board_use')}",
            "- automatic_candidate_promotion: false",
            "- owner_review_required: true",
            "",
            "## Safety Boundary",
            "- research-only cost sensitivity review",
            "- no broker integration or order ticket",
            "- no paper account or production mutation",
            "- no official target weights",
            "- no data refresh or upstream rerun",
            "",
            "## Limitations",
            "- Cost drag is turnover multiplied by configured total cost bps.",
            "- These assumptions are not live execution, tax, financing, or capacity evidence.",
            "- Missing numeric candidate metrics produce INSUFFICIENT_COST_INPUTS.",
            "",
        ]
    )


def render_cost_sensitivity_validation_report(validation: Mapping[str, Any]) -> str:
    check_lines = [
        f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}"
        for row in _records(validation.get("checks"))
    ]
    return "\n".join(
        [
            f"# Cost Sensitivity Validation {validation.get('artifact_id')}",
            "",
            f"- status: {validation.get('status')}",
            f"- failed_check_count: {validation.get('failed_check_count')}",
            "- production_effect: none",
            "",
            "## Checks",
            *check_lines,
            "",
        ]
    )


def _normalized_policy(config: Mapping[str, Any], *, config_path: Path) -> dict[str, Any]:
    safety = {**COST_SENSITIVITY_SAFETY, **_mapping(config.get("safety_boundaries"))}
    return {
        "schema_version": st.SCHEMA_VERSION,
        "policy_id": _text(
            config.get("policy_id"),
            "dynamic_v3_rescue_cost_sensitivity_review_v1",
        ),
        "version": _text(config.get("version")),
        "status": _text(config.get("status"), "pilot_manual_review_baseline"),
        "owner": _text(config.get("owner"), "system_validation"),
        "rationale": _text(config.get("rationale")),
        "intended_effect": _text(config.get("intended_effect")),
        "validation_evidence": _text(config.get("validation_evidence")),
        "review_condition": _text(config.get("review_condition")),
        "config_path": str(config_path),
        "meaningful_improvement": _mapping(config.get("meaningful_improvement")),
        "scenarios": [_normalized_scenario(row) for row in _records(config.get("scenarios"))],
        "safety_boundaries": safety,
        **safety,
    }


def _normalized_scenario(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "scenario_id": _text(row.get("scenario_id")),
        "label": _text(row.get("label")),
        "total_cost_bps": _float(row.get("total_cost_bps")),
        "commission_bps": _float(row.get("commission_bps")),
        "spread_bps": _float(row.get("spread_bps")),
        "slippage_bps": _float(row.get("slippage_bps")),
        "market_impact_bps": _float(row.get("market_impact_bps")),
        "rationale": _text(row.get("rationale")),
        **COST_SENSITIVITY_SAFETY,
    }


def _weekly_source(*, weekly_review_id: str | None, output_dir: Path) -> dict[str, Any]:
    if not weekly_review_id:
        return _missing_source(
            "paper_shadow_weekly_review",
            "explicit weekly review id is required; latest resolution is forbidden",
        )
    try:
        payload = weekly.paper_shadow_weekly_review_report_payload(
            weekly_review_id=weekly_review_id,
            latest=False,
            output_dir=output_dir,
        )
    except Exception as exc:
        return _missing_source("paper_shadow_weekly_review", f"weekly review missing: {exc}")
    review = _mapping(payload.get("paper_shadow_weekly_review"))
    return _source(
        "paper_shadow_weekly_review",
        exists=True,
        artifact_id=_text(payload.get("weekly_review_id")),
        status=_text(review.get("weekly_decision"), _text(payload.get("status"), "UNKNOWN")),
        validation_status=_text(
            _mapping(payload.get("paper_shadow_weekly_validation")).get("status"),
            "NOT_RUN",
        ),
        source_path=Path(_text(payload.get("paper_shadow_weekly_manifest_path"))),
        summary={
            "weekly_review_id": payload.get("weekly_review_id"),
            "candidate": review.get("candidate"),
            "candidate_lineage_id": review.get("candidate_lineage_id"),
            "week_start": review.get("week_start"),
            "week_end": review.get("week_end"),
            "generated_at": review.get("generated_at", payload.get("generated_at")),
            "weekly_decision": review.get("weekly_decision"),
            "coverage_status": review.get("coverage_status"),
            "coverage_classification": review.get("coverage_classification"),
            "cost_review_role": "source_weekly_paper_shadow_context",
        },
        payload=review,
    )


def _health_source(*, health_id: str | None, output_dir: Path) -> dict[str, Any]:
    if not health_id:
        return _missing_source(
            "paper_shadow_health",
            "explicit paper-shadow health id is required; latest resolution is forbidden",
        )
    try:
        payload = health.paper_shadow_health_report_payload(
            health_id=health_id,
            latest=False,
            output_dir=output_dir,
        )
    except Exception as exc:
        return _missing_source("paper_shadow_health", f"paper-shadow health missing: {exc}")
    report = _mapping(payload.get("paper_shadow_health_report"))
    return _source(
        "paper_shadow_health",
        exists=True,
        artifact_id=_text(payload.get("health_id")),
        status=_text(report.get("paper_shadow_health_status"), "UNKNOWN"),
        validation_status=_text(
            _mapping(payload.get("paper_shadow_health_validation")).get("status"),
            "NOT_RUN",
        ),
        source_path=Path(_text(payload.get("paper_shadow_health_manifest_path"))),
        summary={
            "health_id": payload.get("health_id"),
            "candidate": report.get("candidate"),
            "candidate_lineage_id": report.get("candidate_lineage_id"),
            "as_of": report.get("as_of"),
            "generated_at": report.get("generated_at", payload.get("generated_at")),
            "paper_shadow_health_status": report.get("paper_shadow_health_status"),
            "safe_to_continue_shadow": report.get("safe_to_continue_shadow"),
            "signal_input_status": report.get("signal_input_status"),
            "cost_review_role": "source_health_context",
        },
        payload=report,
    )


def _candidate_metrics_source(
    *,
    metrics: Mapping[str, Any] | None,
    metrics_path: Path | None,
    weekly_source: Mapping[str, Any],
) -> dict[str, Any]:
    if metrics is not None:
        payload = dict(metrics)
        source_path: Path | None = None
    elif metrics_path is not None:
        payload = st._read_json(metrics_path)
        source_path = metrics_path
    else:
        payload = _metrics_from_weekly_source(weekly_source)
        source_path = None
    is_bound_file = source_path is not None and source_path.is_file()
    summary = _normalized_metrics_summary(
        payload,
        weekly_source=weekly_source,
        bound_file=is_bound_file,
    )
    return _source(
        "candidate_metrics",
        exists=True,
        artifact_id=_text(
            payload.get("metrics_id"),
            _text(payload.get("artifact_id"), "candidate_metrics_inline_or_weekly_proxy"),
        ),
        status="OK" if _metrics_complete(summary) else "INSUFFICIENT_COST_INPUTS",
        validation_status=_text(payload.get("validation_status"), "NOT_RUN"),
        source_path=source_path,
        summary=summary,
        payload={**payload, **COST_SENSITIVITY_SAFETY},
    )


def _metrics_from_weekly_source(weekly_source: Mapping[str, Any]) -> dict[str, Any]:
    summary = _mapping(weekly_source.get("summary"))
    return {
        "metrics_id": f"weekly_proxy:{summary.get('weekly_review_id', 'MISSING')}",
        "candidate": summary.get("candidate"),
        "as_of": summary.get("week_end"),
        "turnover": None,
        "gross_performance": None,
        "baseline_performance": None,
        "metric_source": "paper_shadow_weekly_review_proxy",
        "limitation": "weekly review does not contain numeric turnover/performance metrics",
        **COST_SENSITIVITY_SAFETY,
    }


def _normalized_metrics_summary(
    payload: Mapping[str, Any],
    *,
    weekly_source: Mapping[str, Any],
    bound_file: bool,
) -> dict[str, Any]:
    eligible, reason = _validated_dated_metrics(
        payload,
        weekly_source=weekly_source,
        bound_file=bound_file,
    )
    gross = _float_or_none(payload.get("gross_performance")) if eligible else None
    baseline = _float_or_none(payload.get("baseline_performance")) if eligible else None
    turnover = _float_or_none(payload.get("turnover")) if eligible else None
    gross_improvement = gross - baseline if gross is not None and baseline is not None else None
    return {
        "metrics_id": _text(payload.get("metrics_id"), _text(payload.get("artifact_id"))),
        "candidate": _text(payload.get("candidate")),
        "candidate_lineage_id": _text(payload.get("candidate_lineage_id")),
        "source_variant": _text(payload.get("source_variant")),
        "as_of": _text(payload.get("window_end")),
        "window_start": _text(payload.get("window_start")),
        "window_end": _text(payload.get("window_end")),
        "generated_at": _text(payload.get("generated_at")),
        "validation_status": _text(payload.get("validation_status"), "NOT_RUN"),
        "evidence_status": "VALIDATED_DATED_METRICS" if eligible else "INSUFFICIENT_DATA",
        "outcome_mode": _text(payload.get("outcome_mode")),
        "metric_source": _text(payload.get("metric_source")),
        "turnover": _round_or_none(turnover),
        "gross_performance_proxy": _round_or_none(gross),
        "baseline_performance_proxy": _round_or_none(baseline),
        "gross_improvement_proxy": _round_or_none(gross_improvement),
        "limitation": "" if eligible else reason,
    }


def _validated_dated_metrics(
    payload: Mapping[str, Any],
    *,
    weekly_source: Mapping[str, Any],
    bound_file: bool,
) -> tuple[bool, str]:
    if not bound_file:
        return False, "candidate metrics require a bound source file"
    if payload.get("validation_status") != "PASS":
        return False, "candidate metrics validation_status must PASS"
    if payload.get("evidence_status") != "VALIDATED_DATED_METRICS":
        return False, "candidate metrics evidence_status is not validated dated metrics"
    if _text(payload.get("outcome_mode")) in {"", "BACKTEST_SIMULATION"}:
        return False, "backtest simulation or unspecified outcome mode is not cost evidence"
    candidate = _text(payload.get("candidate"))
    lineage = _text(payload.get("candidate_lineage_id"))
    source_variant = _text(payload.get("source_variant"))
    if not candidate or not lineage or not source_variant:
        return False, "candidate, candidate_lineage_id, and source_variant are required"
    if source_variant == "limited_adjustment":
        return False, "limited_adjustment is not the filtered candidate"
    weekly_candidate = _text(_mapping(weekly_source.get("summary")).get("candidate"))
    if weekly_candidate and candidate != weekly_candidate:
        return False, "candidate does not match paper-shadow weekly candidate"
    start = _parse_optional_date(payload.get("window_start"))
    end = _parse_optional_date(payload.get("window_end"))
    if start is None or end is None or start > end:
        return False, "valid dated metric window_start/window_end is required"
    try:
        _aware_utc(payload.get("generated_at"), "candidate_metrics.generated_at")
    except ValueError as exc:
        return False, str(exc)
    if any(
        _float_or_none(payload.get(key)) is None
        for key in ("turnover", "gross_performance", "baseline_performance")
    ):
        return False, "turnover, gross_performance, and baseline_performance are required"
    return True, ""


def _scenario_results(
    *,
    policy: Mapping[str, Any],
    metrics_summary: Mapping[str, Any],
) -> list[dict[str, Any]]:
    threshold = _float(_mapping(policy.get("meaningful_improvement")).get("threshold"))
    return [
        _scenario_result(row, metrics_summary=metrics_summary, threshold=threshold)
        for row in _records(policy.get("scenarios"))
    ]


def _scenario_result(
    scenario: Mapping[str, Any],
    *,
    metrics_summary: Mapping[str, Any],
    threshold: float,
) -> dict[str, Any]:
    turnover = _float_or_none(metrics_summary.get("turnover"))
    gross = _float_or_none(metrics_summary.get("gross_performance_proxy"))
    gross_improvement = _float_or_none(metrics_summary.get("gross_improvement_proxy"))
    total_cost_bps = _float(scenario.get("total_cost_bps"))
    if turnover is None or gross is None or gross_improvement is None:
        cost_drag = None
        net_performance = None
        net_improvement = None
        meaningful = None
        classification = "INSUFFICIENT_INPUTS"
    else:
        cost_drag = turnover * total_cost_bps / 10_000.0
        net_performance = gross - cost_drag
        net_improvement = gross_improvement - cost_drag
        meaningful = net_improvement >= threshold
        classification = "MEANINGFUL" if meaningful else "NOT_MEANINGFUL"
    return {
        "schema_version": st.SCHEMA_VERSION,
        "scenario_id": _text(scenario.get("scenario_id")),
        "label": _text(scenario.get("label")),
        "total_cost_bps": total_cost_bps,
        "turnover": _round_or_none(turnover),
        "cost_drag": _round_or_none(cost_drag),
        "gross_performance_proxy": _round_or_none(gross),
        "net_performance_proxy": _round_or_none(net_performance),
        "gross_improvement_proxy": _round_or_none(gross_improvement),
        "net_improvement_proxy": _round_or_none(net_improvement),
        "meaningful_improvement_threshold": threshold,
        "improvement_remains_meaningful": meaningful,
        "classification": classification,
        "cost_assumption": {
            key: scenario.get(key)
            for key in (
                "commission_bps",
                "spread_bps",
                "slippage_bps",
                "market_impact_bps",
                "rationale",
            )
        },
        **COST_SENSITIVITY_SAFETY,
    }


def _blocking_reasons(
    *,
    weekly_source: Mapping[str, Any],
    metrics_source: Mapping[str, Any],
) -> list[str]:
    reasons: list[str] = []
    if weekly_source.get("exists") is not True:
        reasons.append("paper_shadow_weekly_review:missing")
    if _text(metrics_source.get("status")) == "INSUFFICIENT_COST_INPUTS":
        reasons.append("candidate_metrics:insufficient_cost_inputs")
    return _dedupe(reasons)


def _warnings(
    *,
    health_source: Mapping[str, Any],
    metrics_source: Mapping[str, Any],
) -> list[str]:
    warnings: list[str] = []
    if health_source.get("exists") is not True:
        warnings.append("paper_shadow_health:missing")
    elif _text(health_source.get("status")) not in {
        "HEALTHY",
        "HEALTHY_WITH_WARNINGS",
    }:
        warnings.append(f"paper_shadow_health:{_text(health_source.get('status')).lower()}")
    if _text(_mapping(metrics_source.get("summary")).get("limitation")):
        warnings.append("candidate_metrics:limited_source")
    return _dedupe(warnings)


def _cost_sensitivity_status(
    *,
    blocking_reasons: list[str],
    metrics_source: Mapping[str, Any],
    scenario_results: list[Mapping[str, Any]],
) -> str:
    if any(reason.endswith(":missing") for reason in blocking_reasons):
        return "BLOCKED_SOURCE"
    if _text(metrics_source.get("status")) != "OK":
        return "INSUFFICIENT_COST_INPUTS"
    meaningful = {
        _text(row.get("scenario_id")): row.get("improvement_remains_meaningful") is True
        for row in scenario_results
    }
    if all(meaningful.get(scenario_id) for scenario_id in REQUIRED_COST_SCENARIOS):
        return "MEANINGFUL_ALL_SCENARIOS"
    if meaningful.get("low") and meaningful.get("medium"):
        return "MEANINGFUL_LOW_MEDIUM_ONLY"
    return "NOT_MEANINGFUL_UNDER_COSTS"


def _promotion_board_inputs(
    *,
    review_id: str,
    candidate: str,
    cost_status: str,
    scenario_results: list[Mapping[str, Any]],
    weekly_source: Mapping[str, Any],
    health_source: Mapping[str, Any],
    blocking_reasons: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "review_id": review_id,
        "candidate": candidate,
        "board_use": "research_cost_sensitivity_input_only",
        "cost_sensitivity_status": cost_status,
        "source_weekly_review_id": _mapping(weekly_source.get("summary")).get(
            "weekly_review_id"
        ),
        "source_paper_shadow_health_id": _mapping(health_source.get("summary")).get(
            "health_id"
        ),
        "scenario_count": len(scenario_results),
        "scenario_ids": [_text(row.get("scenario_id")) for row in scenario_results],
        "high_cost_improvement_meaningful": _scenario_meaningful(
            scenario_results,
            "high",
        ),
        "worst_net_improvement_proxy": _worst_net_improvement(scenario_results),
        "blocking_reasons": blocking_reasons,
        "warnings": warnings,
        "owner_review_required": True,
        "automatic_candidate_promotion": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _weekly_review_input(weekly_source: Mapping[str, Any]) -> dict[str, Any]:
    summary = _mapping(weekly_source.get("summary"))
    return {
        "weekly_review_id": summary.get("weekly_review_id"),
        "candidate": summary.get("candidate"),
        "week_start": summary.get("week_start"),
        "week_end": summary.get("week_end"),
        "weekly_decision": summary.get("weekly_decision"),
        "coverage_status": summary.get("coverage_status"),
        "coverage_classification": summary.get("coverage_classification"),
        "source_status": weekly_source.get("status"),
        "source_validation_status": weekly_source.get("validation_status"),
    }


def _next_required_action(status: str) -> str:
    if status == "MEANINGFUL_ALL_SCENARIOS":
        return "include_cost_sensitivity_in_next_weekly_or_promotion_board_review"
    if status == "MEANINGFUL_LOW_MEDIUM_ONLY":
        return "owner_review_high_cost_fragility_before_promotion_board"
    if status == "NOT_MEANINGFUL_UNDER_COSTS":
        return "return_candidate_to_research_until_net_improvement_survives_costs"
    if status == "BLOCKED_SOURCE":
        return "restore_weekly_review_source_before_cost_sensitivity"
    return "provide_numeric_turnover_and_performance_metrics_before_cost_review"


def _source(
    source_id: str,
    *,
    exists: bool,
    artifact_id: str,
    status: str,
    validation_status: str,
    source_path: Path | None,
    summary: Mapping[str, Any],
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "source_id": source_id,
        "exists": exists,
        "artifact_id": artifact_id,
        "status": status,
        "validation_status": validation_status,
        "source_path": "" if source_path is None else str(source_path),
        "summary": dict(summary),
        "safety_status": "PASS" if st._payload_safe(payload) else "FAIL",
        "production_effect": _text(payload.get("production_effect"), "none"),
    }


def _missing_source(source_id: str, reason: str) -> dict[str, Any]:
    return {
        "source_id": source_id,
        "exists": False,
        "artifact_id": "MISSING",
        "status": "MISSING",
        "validation_status": "MISSING",
        "source_path": "",
        "summary": {"limitation": reason},
        "safety_status": "PASS",
        "production_effect": "none",
    }


def _scenario_output_complete(row: Mapping[str, Any]) -> bool:
    return all(
        key in row
        for key in (
            "scenario_id",
            "total_cost_bps",
            "turnover",
            "cost_drag",
            "gross_performance_proxy",
            "net_performance_proxy",
            "gross_improvement_proxy",
            "net_improvement_proxy",
            "improvement_remains_meaningful",
            "classification",
        )
    )


def _metrics_complete(summary: Mapping[str, Any]) -> bool:
    return all(
        _float_or_none(summary.get(key)) is not None
        for key in (
            "turnover",
            "gross_performance_proxy",
            "gross_improvement_proxy",
        )
    )


def _scenario_meaningful(
    scenario_results: list[Mapping[str, Any]],
    scenario_id: str,
) -> bool | None | str:
    for row in scenario_results:
        if _text(row.get("scenario_id")) == scenario_id:
            value = row.get("improvement_remains_meaningful")
            return None if value is None else value is True
    return "MISSING"


def _worst_net_improvement(scenario_results: list[Mapping[str, Any]]) -> float | None:
    values = [
        value
        for row in scenario_results
        if (value := _float_or_none(row.get("net_improvement_proxy"))) is not None
    ]
    if not values:
        return None
    return _round_or_none(min(values))


def _first_float(payload: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _float_or_none(payload.get(key))
        if value is not None:
            return value
    return None


def _float(value: object) -> float:
    parsed = _float_or_none(value)
    if parsed is None:
        raise ValueError("required numeric policy value is missing")
    return parsed


def _float_or_none(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _round_or_none(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 10)


def _parse_optional_date(value: object) -> date | None:
    text = _text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = _text(value)
        if text and text not in seen:
            result.append(text)
            seen.add(text)
    return result


def _joined_texts(value: object) -> str:
    return ", ".join(_texts(value)) or "none"


_mapping = st._mapping
_records = st._records
_text = st._text
_texts = st._texts
