from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from ai_trading_system.config import (
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_universe,
)
from ai_trading_system.data.quality import validate_data_cache, write_data_quality_report
from ai_trading_system.etf_portfolio import dynamic_v3_backtest_simulation as sim
from ai_trading_system.etf_portfolio import (
    dynamic_v3_benchmark_baseline_control as baseline_control,
)
from ai_trading_system.etf_portfolio import dynamic_v3_cost_metrics_materialization as cost_metrics
from ai_trading_system.etf_portfolio import dynamic_v3_cost_sensitivity as cost
from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness
from ai_trading_system.etf_portfolio import dynamic_v3_historical_replay as replay
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_weekly as weekly
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_diagnostics as diagnostics
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_foundation as foundation
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH

DEFAULT_BENCHMARK_BASELINE_METRICS_MATERIALIZATION_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "benchmark_baseline_metrics_materialization"
)

BENCHMARK_BASELINE_METRICS_MATERIALIZATION_STATUSES = (
    "BASELINE_METRICS_AVAILABLE",
    "BASELINE_METRICS_PARTIAL",
    "INSUFFICIENT_BASELINE_METRICS",
)

BENCHMARK_BASELINE_METRICS_MATERIALIZATION_SAFETY = {
    **baseline_control.BENCHMARK_BASELINE_SAFETY,
    "benchmark_baseline_metrics_materialization_only": True,
    "backtest_simulation_event_window_evidence_only": False,
    "backtest_simulation_contract_only": True,
    "data_quality_gate_required": True,
    "data_downloaded_by_materialization": False,
    "pipelines_executed_by_materialization": False,
    "strategy_optimized_by_materialization": False,
    "benchmark_comparison_live_signal": False,
}
BENCHMARK_BASELINE_METRICS_INPUT_SCHEMA = (
    "benchmark_baseline_metrics_materialization_input_snapshot.v2"
)
BENCHMARK_BASELINE_METRICS_VIEWS = (
    "benchmark_baseline_metrics_materialization_manifest.json",
    "benchmark_baseline_metrics_materialization_report.json",
    "benchmark_baseline_metrics_materialization_report.md",
    "candidate_benchmark_metrics.json",
    "baseline_metrics.json",
    "reader_brief_section.md",
    "validate_data_quality_report.md",
)
BENCHMARK_BASELINE_METRICS_SNAPSHOT = (
    "benchmark_baseline_metrics_materialization_input_snapshot.json"
)


def run_benchmark_baseline_metrics_materialization(
    *,
    as_of: date | None = None,
    candidate: str = readiness.TOP_FILTERED_CANDIDATE,
    source_variant: str | None = None,
    sim_outcome_id: str | None = None,
    sim_outcome_dir: Path = sim.DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    candidate_metrics_path: Path | None = None,
    candidate_cost_materialization_id: str | None = None,
    candidate_cost_materialization_dir: Path = (
        cost_metrics.DEFAULT_COST_METRICS_MATERIALIZATION_DIR
    ),
    weekly_review_id: str | None = None,
    weekly_review_dir: Path = weekly.DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR,
    cost_sensitivity_review_id: str | None = None,
    cost_sensitivity_dir: Path = cost.DEFAULT_COST_SENSITIVITY_REVIEW_DIR,
    benchmark_baseline_output_dir: Path = (
        baseline_control.DEFAULT_BENCHMARK_BASELINE_CONTROL_DIR
    ),
    price_cache_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_cache_path: Path = st.DEFAULT_RATES_CACHE_PATH,
    output_dir: Path = DEFAULT_BENCHMARK_BASELINE_METRICS_MATERIALIZATION_DIR,
    generated_at: datetime | None = None,
    _validate_output: bool = True,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    if not _text(source_variant):
        raise ValueError("source_variant must be explicit")
    resolved_source_variant = _text(source_variant)
    outcome_payload = _load_sim_outcome(
        sim_outcome_id=sim_outcome_id,
        output_dir=sim_outcome_dir,
    )
    effective_as_of = as_of or _parse_date(outcome_payload.get("as_of")) or generated.date()
    if effective_as_of > generated.date():
        raise ValueError("benchmark baseline metrics as_of occurs after generated_at")
    candidate_metrics_input = _load_candidate_cost_metrics(
        metrics_path=candidate_metrics_path,
        materialization_id=candidate_cost_materialization_id,
        output_dir=candidate_cost_materialization_dir,
    )
    cost_review_payload = _load_cost_review(
        review_id=cost_sensitivity_review_id,
        output_dir=cost_sensitivity_dir,
    )
    _validate_materialization_sources(
        outcome_payload=outcome_payload,
        requested_sim_outcome_id=sim_outcome_id,
        candidate_metrics_payload=candidate_metrics_input,
        effective_as_of=effective_as_of,
        generated=generated,
    )
    source_bindings = _materialization_source_bindings(
        outcome_payload=outcome_payload,
        candidate_metrics_path=candidate_metrics_path,
        candidate_cost_materialization_id=candidate_cost_materialization_id,
        candidate_cost_materialization_dir=candidate_cost_materialization_dir,
        weekly_review_id=weekly_review_id,
        weekly_review_dir=weekly_review_dir,
        cost_sensitivity_review_id=cost_sensitivity_review_id,
        cost_sensitivity_dir=cost_sensitivity_dir,
        price_cache_path=price_cache_path,
        rates_cache_path=rates_cache_path,
    )
    materialization_id = st._stable_id(
        "benchmark-baseline-metrics-materialization",
        candidate,
        resolved_source_variant,
        _text(outcome_payload.get("sim_outcome_id")),
        _text(candidate_cost_materialization_id),
        generated.isoformat(),
    )
    root = st._unique_dir(output_dir / materialization_id)
    root.mkdir(parents=True, exist_ok=False)

    quality = _run_data_quality_gate(
        as_of=effective_as_of,
        price_cache_path=price_cache_path,
        rates_cache_path=rates_cache_path,
        report_path=root / "validate_data_quality_report.md",
    )
    if not quality.passed:
        raise RuntimeError(
            "benchmark baseline metrics materialization stopped because "
            f"data quality gate failed: {quality.status}"
        )

    candidate_metrics_payload = _candidate_benchmark_metrics(
        candidate=candidate,
        source_variant=resolved_source_variant,
        source_row={},
        cost_review_payload=cost_review_payload,
        candidate_metrics_payload=candidate_metrics_input,
        outcome_payload=outcome_payload,
        effective_as_of=effective_as_of,
        generated_at=generated,
    )
    baseline_metrics_payload = _baseline_metrics(
        outcome_payload=outcome_payload,
        outcome_rows=(),
        summary_rows=(),
        prices=None,
        effective_as_of=effective_as_of,
        generated_at=generated,
        price_cache_path=price_cache_path,
        data_quality_summary=_quality_summary(
            quality,
            report_path=root / "validate_data_quality_report.md",
        ),
    )
    metric_statuses = _metric_statuses(
        candidate_metrics_payload=candidate_metrics_payload,
        baseline_metrics_payload=baseline_metrics_payload,
    )
    candidate_path = root / "candidate_benchmark_metrics.json"
    baseline_path = root / "baseline_metrics.json"
    st._write_json(candidate_path, candidate_metrics_payload)
    st._write_json(baseline_path, baseline_metrics_payload)

    control_result = baseline_control.run_benchmark_baseline_control_pack(
        as_of=effective_as_of,
        candidate_metrics_path=candidate_path,
        baseline_metrics_path=baseline_path,
        weekly_review_id=weekly_review_id,
        weekly_review_dir=weekly_review_dir,
        cost_sensitivity_review_id=cost_sensitivity_review_id,
        cost_sensitivity_dir=cost_sensitivity_dir,
        output_dir=benchmark_baseline_output_dir,
        generated_at=generated,
    )
    control_pack = _mapping(control_result.get("benchmark_baseline_control_pack"))
    final_status = _materialization_status(metric_statuses, control_pack)
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_benchmark_baseline_metrics_materialization_report",
        "materialization_id": root.name,
        "candidate": candidate,
        "source_variant": resolved_source_variant,
        "as_of": effective_as_of.isoformat(),
        "requested_as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "benchmark_baseline_metrics_status": final_status,
        "observed_evidence_status": "INSUFFICIENT_DATA",
        "candidate_lineage_status": "UNBOUND_SIMULATION_VARIANT",
        "candidate_metrics_path": str(candidate_path),
        "baseline_metrics_path": str(baseline_path),
        "candidate_metrics": candidate_metrics_payload,
        "baseline_metrics": baseline_metrics_payload,
        "required_metric_statuses": metric_statuses,
        "benchmark_baseline_control_id": control_result.get("control_id"),
        "benchmark_baseline_status": control_pack.get("benchmark_baseline_status"),
        "benchmark_baseline_validation_status": _mapping(
            control_result.get("benchmark_baseline_validation")
        ).get("status"),
        "comparison_summary": control_pack.get("comparison_summary"),
        "source_artifacts": {
            "backtest_sim_outcome": _source_artifact(outcome_payload),
            "candidate_cost_metrics": _candidate_source_artifact(candidate_metrics_payload),
            "cost_sensitivity_review": _cost_source_artifact(
                _mapping(candidate_metrics_payload.get("cost_sensitivity_source"))
            ),
            "price_cache": _price_source_artifact(price_cache_path),
            "data_quality_gate": _quality_summary(
                quality,
                report_path=root / "validate_data_quality_report.md",
            ),
            "benchmark_baseline_control": {
                "artifact_id": control_result.get("control_id"),
                "status": control_pack.get("benchmark_baseline_status"),
                "validation_status": _mapping(
                    control_result.get("benchmark_baseline_validation")
                ).get("status"),
                "report_path": _mapping(control_result.get("manifest")).get(
                    "benchmark_baseline_report_path"
                ),
            },
        },
        "blocking_reasons": _blocking_reasons(metric_statuses, control_pack),
        "warnings": _warnings(
            candidate_metrics_payload=candidate_metrics_payload,
            baseline_metrics_payload=baseline_metrics_payload,
            control_pack=control_pack,
            data_quality=quality,
        ),
        "next_required_action": _next_action(final_status, control_pack),
        "limitations": [
            (
                "simulation contracts are inventoried but no observed baseline metrics "
                "are materialized"
            ),
            "BACKTEST_SIMULATION event windows are not PIT/live execution evidence",
            "limited_adjustment is not the filtered candidate without explicit lineage",
            "no fixed event window is generalized into investment evidence",
            "gross performance is never substituted for missing net performance",
            "no default portfolio weights are used as evidence",
            "benchmark comparison is research-only and not a live allocation signal",
            "equal_weight_shadow_candidates is not mapped to equal_weight_etf",
        ],
        **BENCHMARK_BASELINE_METRICS_MATERIALIZATION_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_benchmark_baseline_metrics_materialization_manifest",
        "materialization_id": root.name,
        "candidate": candidate,
        "source_variant": resolved_source_variant,
        "as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": final_status,
        "benchmark_baseline_metrics_status": final_status,
        "candidate_metrics_path": str(candidate_path),
        "baseline_metrics_path": str(baseline_path),
        "benchmark_baseline_control_id": control_result.get("control_id"),
        "benchmark_baseline_status": control_pack.get("benchmark_baseline_status"),
        "benchmark_baseline_metrics_materialization_manifest_path": str(
            root / "benchmark_baseline_metrics_materialization_manifest.json"
        ),
        "benchmark_baseline_metrics_materialization_report_path": str(
            root / "benchmark_baseline_metrics_materialization_report.json"
        ),
        "benchmark_baseline_metrics_materialization_markdown_path": str(
            root / "benchmark_baseline_metrics_materialization_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        "validation_path": str(
            root / "benchmark_baseline_metrics_materialization_validation.json"
        ),
        **BENCHMARK_BASELINE_METRICS_MATERIALIZATION_SAFETY,
    }
    reader = render_benchmark_baseline_metrics_materialization_reader_brief(report)
    st._write_json(
        root / "benchmark_baseline_metrics_materialization_manifest.json",
        manifest,
    )
    st._write_json(root / "benchmark_baseline_metrics_materialization_report.json", report)
    st._write_text(
        root / "benchmark_baseline_metrics_materialization_report.md",
        render_benchmark_baseline_metrics_materialization_report(manifest, report),
    )
    st._write_text(root / "reader_brief_section.md", reader)
    control_id = _text(control_result.get("control_id"))
    control_binding = foundation._artifact_binding(
        kind="benchmark_baseline_control",
        artifact_id=control_id,
        root=benchmark_baseline_output_dir / control_id,
        names=(
            "benchmark_baseline_manifest.json",
            "benchmark_baseline_control_pack.json",
            "benchmark_baseline_report.md",
            "reader_brief_section.md",
            baseline_control.BENCHMARK_BASELINE_SNAPSHOT,
            "benchmark_baseline_validation.json",
        ),
    )
    snapshot = {
        "schema_version": BENCHMARK_BASELINE_METRICS_INPUT_SCHEMA,
        "materialization_id": root.name,
        "generated_at": generated.isoformat(),
        "effective_as_of": effective_as_of.isoformat(),
        "source_bindings": source_bindings,
        "benchmark_control_binding": control_binding,
        "lineage": {
            "candidate": candidate,
            "source_variant": resolved_source_variant,
            "sim_outcome_id": sim_outcome_id,
            "outcome_mode": outcome_payload.get("outcome_mode"),
            "candidate_lineage_status": "UNBOUND_SIMULATION_VARIANT",
            "observed_evidence_status": "INSUFFICIENT_DATA",
        },
        "replay": {
            "candidate": candidate,
            "source_variant": resolved_source_variant,
            "sim_outcome_id": sim_outcome_id,
            "sim_outcome_dir": str(sim_outcome_dir.resolve()),
            "candidate_metrics_path": (
                None
                if candidate_metrics_path is None
                else str(candidate_metrics_path.resolve())
            ),
            "candidate_cost_materialization_id": candidate_cost_materialization_id,
            "candidate_cost_materialization_dir": str(
                candidate_cost_materialization_dir.resolve()
            ),
            "weekly_review_id": weekly_review_id,
            "weekly_review_dir": str(weekly_review_dir.resolve()),
            "cost_sensitivity_review_id": cost_sensitivity_review_id,
            "cost_sensitivity_dir": str(cost_sensitivity_dir.resolve()),
            "benchmark_baseline_output_dir": str(
                benchmark_baseline_output_dir.resolve()
            ),
            "price_cache_path": str(price_cache_path.resolve()),
            "rates_cache_path": str(rates_cache_path.resolve()),
        },
        "view_hashes": foundation._view_hashes(root, BENCHMARK_BASELINE_METRICS_VIEWS),
    }
    foundation._write_snapshot(root / BENCHMARK_BASELINE_METRICS_SNAPSHOT, snapshot)
    st._write_latest_pointer(
        "latest_benchmark_baseline_metrics_materialization",
        root.name,
        root / "benchmark_baseline_metrics_materialization_manifest.json",
    )
    validation = (
        validate_benchmark_baseline_metrics_materialization_artifact(
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
        "benchmark_baseline_metrics_materialization_report": report,
        "reader_brief_section": reader,
        "input_snapshot": snapshot,
        "benchmark_baseline_metrics_materialization_validation": validation,
        "benchmark_baseline_control_result": control_result,
    }


def benchmark_baseline_metrics_materialization_report_payload(
    *,
    materialization_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_BENCHMARK_BASELINE_METRICS_MATERIALIZATION_DIR,
) -> dict[str, Any]:
    root = st._artifact_dir(
        artifact_id=materialization_id,
        latest_pointer="latest_benchmark_baseline_metrics_materialization",
        latest=latest,
        output_dir=output_dir,
        required_name="benchmark_baseline_metrics_materialization_manifest.json",
    )
    payload = {
        **st._read_json(root / "benchmark_baseline_metrics_materialization_manifest.json"),
        "benchmark_baseline_metrics_materialization_report": st._read_json(
            root / "benchmark_baseline_metrics_materialization_report.json"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(
            encoding="utf-8"
        ),
        "materialization_dir": str(root),
    }
    snapshot = st._read_optional_json(root / BENCHMARK_BASELINE_METRICS_SNAPSHOT)
    if snapshot:
        payload["input_snapshot"] = snapshot
    validation = st._read_optional_json(
        root / "benchmark_baseline_metrics_materialization_validation.json"
    )
    if validation:
        payload["benchmark_baseline_metrics_materialization_validation"] = validation
    return payload


def validate_benchmark_baseline_metrics_materialization_artifact(
    *,
    materialization_id: str,
    output_dir: Path = DEFAULT_BENCHMARK_BASELINE_METRICS_MATERIALIZATION_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / materialization_id
    checks, ok = diagnostics._snapshot_preflight(
        root=root,
        snapshot_name=BENCHMARK_BASELINE_METRICS_SNAPSHOT,
        schema=BENCHMARK_BASELINE_METRICS_INPUT_SCHEMA,
        id_key="materialization_id",
        artifact_id=materialization_id,
        view_names=BENCHMARK_BASELINE_METRICS_VIEWS,
    )
    validation = (
        diagnostics._validate_content(
            report_type=(
                "etf_dynamic_v3_benchmark_baseline_metrics_materialization_validation"
            ),
            artifact_id=materialization_id,
            checks=checks,
            rebuild=lambda: _rebuild_benchmark_baseline_metrics(
                root, materialization_id
            ),
        )
        if ok
        else st._validation_payload(
            "etf_dynamic_v3_benchmark_baseline_metrics_materialization_validation",
            materialization_id,
            checks,
        )
    )
    if write_output:
        st._write_json(
            root / "benchmark_baseline_metrics_materialization_validation.json",
            validation,
        )
        st._write_text(
            root / "benchmark_baseline_metrics_materialization_validation.md",
            render_benchmark_baseline_metrics_materialization_validation_report(validation),
        )
    return validation


def _generated_time(value: datetime | None) -> datetime:
    generated = value or datetime.now(UTC)
    if generated.tzinfo is None or generated.utcoffset() != UTC.utcoffset(generated):
        raise ValueError("generated_at must be timezone-aware UTC")
    return generated.astimezone(UTC)


def _validate_materialization_sources(
    *,
    outcome_payload: Mapping[str, Any],
    requested_sim_outcome_id: str | None,
    candidate_metrics_payload: Mapping[str, Any],
    effective_as_of: date,
    generated: datetime,
) -> None:
    if requested_sim_outcome_id:
        if _text(outcome_payload.get("sim_outcome_id")) != requested_sim_outcome_id:
            raise ValueError("benchmark baseline simulation source id mismatch")
    elif outcome_payload.get("source_status") != "MISSING_EXPLICIT_SOURCE":
        raise ValueError("benchmark baseline materialization cannot resolve implicit latest")
    for label, value in (
        ("simulation as_of", outcome_payload.get("as_of")),
        (
            "candidate metric as_of",
            candidate_metrics_payload.get("window_end", candidate_metrics_payload.get("as_of")),
        ),
    ):
        parsed = _parse_date(value)
        if parsed and (parsed > effective_as_of or parsed > generated.date()):
            raise ValueError(f"benchmark baseline {label} occurs after requested chronology")
    generated_text = _text(candidate_metrics_payload.get("generated_at"))
    if generated_text:
        candidate_generated = datetime.fromisoformat(generated_text)
        if (
            candidate_generated.tzinfo is None
            or candidate_generated.utcoffset() != UTC.utcoffset(candidate_generated)
        ):
            raise ValueError("candidate metrics generated_at must be timezone-aware UTC")
        if candidate_generated > generated:
            raise ValueError("candidate metrics generated_at occurs after materialization")


def _materialization_source_bindings(
    *,
    outcome_payload: Mapping[str, Any],
    candidate_metrics_path: Path | None,
    candidate_cost_materialization_id: str | None,
    candidate_cost_materialization_dir: Path,
    weekly_review_id: str | None,
    weekly_review_dir: Path,
    cost_sensitivity_review_id: str | None,
    cost_sensitivity_dir: Path,
    price_cache_path: Path,
    rates_cache_path: Path,
) -> list[dict[str, Any]]:
    bindings: list[dict[str, Any]] = [
        {"role": "price_cache", **foundation._file_binding(price_cache_path)},
        {"role": "rates_cache", **foundation._file_binding(rates_cache_path)},
    ]
    manifest_path = Path(_text(outcome_payload.get("sim_outcome_manifest_path")))
    if manifest_path.is_file():
        for name in (
            "sim_outcome_manifest.json",
            "simulated_outcome_windows.jsonl",
            "simulated_variant_summary.json",
            "outcome_input_snapshot.json",
        ):
            path = manifest_path.parent / name
            if path.is_file():
                bindings.append({"role": f"sim_outcome:{name}", **foundation._file_binding(path)})
    if candidate_metrics_path is not None:
        bindings.append(
            {"role": "candidate_metrics", **foundation._file_binding(candidate_metrics_path)}
        )
    elif candidate_cost_materialization_id:
        source_root = candidate_cost_materialization_dir / candidate_cost_materialization_id
        for name in (
            "cost_metrics_materialization_manifest.json",
            "cost_metrics_materialization_report.json",
            "candidate_cost_metrics.json",
            "cost_metrics_materialization_input_snapshot.json",
        ):
            path = source_root / name
            if path.is_file():
                bindings.append(
                    {
                        "role": f"candidate_cost_materialization:{name}",
                        **foundation._file_binding(path),
                    }
                )
    for role, artifact_id, artifact_dir, names in (
        (
            "weekly_review",
            weekly_review_id,
            weekly_review_dir,
            (
                "paper_shadow_weekly_manifest.json",
                "paper_shadow_weekly_review.json",
                "paper_shadow_weekly_validation.json",
            ),
        ),
        (
            "cost_sensitivity",
            cost_sensitivity_review_id,
            cost_sensitivity_dir,
            (
                "cost_sensitivity_manifest.json",
                "cost_sensitivity_review.json",
                "cost_sensitivity_validation.json",
            ),
        ),
    ):
        if not artifact_id:
            continue
        for name in names:
            path = artifact_dir / artifact_id / name
            if path.is_file():
                bindings.append({"role": f"{role}:{name}", **foundation._file_binding(path)})
    return bindings


def _rebuild_benchmark_baseline_metrics(
    root: Path, materialization_id: str
) -> list[dict[str, Any]]:
    snapshot = st._read_json(root / BENCHMARK_BASELINE_METRICS_SNAPSHOT)
    for binding in _records(snapshot.get("source_bindings")):
        foundation._validate_file_binding(binding)
    control_binding = _mapping(snapshot.get("benchmark_control_binding"))
    foundation._validate_artifact_binding(
        control_binding, kind="benchmark_baseline_control"
    )
    replay_args = _mapping(snapshot.get("replay"))
    generated = _generated_time(
        datetime.fromisoformat(_text(snapshot.get("generated_at")))
    )
    candidate_path = _text(replay_args.get("candidate_metrics_path"))
    with TemporaryDirectory(prefix="eb4-benchmark-metrics-") as temp_dir:
        temp_root = Path(temp_dir)
        temp_control_dir = temp_root / "benchmark_control"
        result = run_benchmark_baseline_metrics_materialization(
            as_of=date.fromisoformat(_text(snapshot.get("effective_as_of"))),
            candidate=_text(replay_args.get("candidate")),
            source_variant=_text(replay_args.get("source_variant")),
            sim_outcome_id=_text(replay_args.get("sim_outcome_id")) or None,
            sim_outcome_dir=Path(_text(replay_args.get("sim_outcome_dir"))),
            candidate_metrics_path=Path(candidate_path) if candidate_path else None,
            candidate_cost_materialization_id=(
                _text(replay_args.get("candidate_cost_materialization_id")) or None
            ),
            candidate_cost_materialization_dir=Path(
                _text(replay_args.get("candidate_cost_materialization_dir"))
            ),
            weekly_review_id=_text(replay_args.get("weekly_review_id")) or None,
            weekly_review_dir=Path(_text(replay_args.get("weekly_review_dir"))),
            cost_sensitivity_review_id=(
                _text(replay_args.get("cost_sensitivity_review_id")) or None
            ),
            cost_sensitivity_dir=Path(_text(replay_args.get("cost_sensitivity_dir"))),
            benchmark_baseline_output_dir=temp_control_dir,
            price_cache_path=Path(_text(replay_args.get("price_cache_path"))),
            rates_cache_path=Path(_text(replay_args.get("rates_cache_path"))),
            output_dir=temp_root / "materialization",
            generated_at=generated,
            _validate_output=False,
        )
        expected_root = Path(result["materialization_dir"])
        expected_control_root = Path(
            _mapping(result.get("benchmark_baseline_control_result")).get("control_dir")
        )
        actual_control_root = Path(_text(control_binding.get("source_dir")))
        expected = {
            name: _normalize_materialization_roots(
                (expected_root / name).read_bytes(),
                replacements=(
                    (expected_root, root),
                    (expected_control_root, actual_control_root),
                ),
            )
            for name in BENCHMARK_BASELINE_METRICS_VIEWS
        }
    if result["materialization_id"] != materialization_id:
        raise ValueError("benchmark baseline metrics materialization id is not reproducible")
    return diagnostics._check_bytes(root, expected)


def _normalize_materialization_roots(
    payload: bytes, *, replacements: Sequence[tuple[Path, Path]]
) -> bytes:
    normalized = payload
    for old_path, new_path in replacements:
        old = str(old_path)
        new = str(new_path)
        normalized = normalized.replace(old.encode(), new.encode()).replace(
            old.replace("\\", "\\\\").encode(),
            new.replace("\\", "\\\\").encode(),
        )
    return normalized


def render_benchmark_baseline_metrics_materialization_reader_brief(
    report: Mapping[str, Any],
) -> str:
    summary = _mapping(report.get("comparison_summary"))
    metric_statuses = _mapping(report.get("required_metric_statuses"))
    return "\n".join(
        [
            "## Benchmark Baseline Metrics Materialization",
            "",
            f"- benchmark_baseline_metrics_materialization_id: {report.get('materialization_id')}",
            "- benchmark_baseline_metrics_status: "
            f"{report.get('benchmark_baseline_metrics_status')}",
            f"- benchmark_baseline_candidate: {report.get('candidate')}",
            f"- source_variant: {report.get('source_variant')}",
            f"- candidate_metric_status: {metric_statuses.get('candidate')}",
            f"- available_baseline_count: {metric_statuses.get('available_baseline_count')}",
            f"- missing_baseline_count: {metric_statuses.get('missing_baseline_count')}",
            f"- benchmark_baseline_control_id: {report.get('benchmark_baseline_control_id')}",
            f"- benchmark_baseline_status: {report.get('benchmark_baseline_status')}",
            f"- outperformed_baseline_count: {summary.get('outperformed_baseline_count')}",
            f"- underperformed_baseline_count: {summary.get('underperformed_baseline_count')}",
            f"- blocking_reasons: {_joined_texts(report.get('blocking_reasons'))}",
            f"- warnings: {_joined_texts(report.get('warnings'))}",
            f"- next_required_action: {report.get('next_required_action')}",
            "- safety_boundary: research-only benchmark metrics / not a live allocation signal / "
            "no broker / no order / no official target / no production",
            "",
        ]
    )


def render_benchmark_baseline_metrics_materialization_report(
    manifest: Mapping[str, Any],
    report: Mapping[str, Any],
) -> str:
    rows = [
        (
            f"| `{row.get('baseline_id')}` | {row.get('source_method')} | "
            f"{row.get('net_performance_proxy')} | {row.get('sample_count')} | "
            f"{row.get('missing_window_count')} | {row.get('metric_status')} |"
        )
        for row in _records(_mapping(report.get("baseline_metrics")).get("baselines"))
    ]
    summary = _mapping(report.get("comparison_summary"))
    data_quality = _mapping(_mapping(report.get("source_artifacts")).get("data_quality_gate"))
    return "\n".join(
        [
            f"# Benchmark Baseline Metrics Materialization {manifest.get('materialization_id')}",
            "",
            "## Purpose",
            "Materialize explicit candidate and benchmark baseline metrics for the existing "
            "benchmark baseline control pack.",
            "",
            "## Summary",
            f"- candidate: {report.get('candidate')}",
            f"- status: {report.get('benchmark_baseline_metrics_status')}",
            f"- benchmark_baseline_status: {report.get('benchmark_baseline_status')}",
            f"- benchmark_baseline_control_id: {report.get('benchmark_baseline_control_id')}",
            f"- outperformed_baseline_count: {summary.get('outperformed_baseline_count')}",
            f"- underperformed_baseline_count: {summary.get('underperformed_baseline_count')}",
            f"- data_quality_status: {data_quality.get('status')}",
            f"- candidate_metrics_path: {report.get('candidate_metrics_path')}",
            f"- baseline_metrics_path: {report.get('baseline_metrics_path')}",
            f"- blocking_reasons: {_joined_texts(report.get('blocking_reasons'))}",
            f"- warnings: {_joined_texts(report.get('warnings'))}",
            f"- next_required_action: {report.get('next_required_action')}",
            "",
            "## Baseline Metrics",
            "| baseline | source method | net proxy | sample count | missing windows | status |",
            "|---|---|---:|---:|---:|---|",
            *rows,
            "",
            "## Safety Boundary",
            "- research-only benchmark metrics materialization",
            "- BACKTEST_SIMULATION event windows are not PIT/live evidence",
            "- benchmark comparison is not a live allocation signal",
            "- no broker integration or order ticket",
            "- no official target weights",
            "- no paper account or production mutation",
            "",
        ]
    )


def render_benchmark_baseline_metrics_materialization_validation_report(
    validation: Mapping[str, Any],
) -> str:
    checks = [
        f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}"
        for row in _records(validation.get("checks"))
    ]
    return "\n".join(
        [
            "# Benchmark Baseline Metrics Materialization Validation "
            f"{validation.get('artifact_id')}",
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


def _candidate_benchmark_metrics(
    *,
    candidate: str,
    source_variant: str,
    source_row: Mapping[str, Any],
    cost_review_payload: Mapping[str, Any],
    candidate_metrics_payload: Mapping[str, Any],
    outcome_payload: Mapping[str, Any],
    effective_as_of: date,
    generated_at: datetime,
) -> dict[str, Any]:
    cost_review = _mapping(cost_review_payload.get("cost_sensitivity_review"))
    return {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_candidate_benchmark_metrics",
        "metrics_id": st._stable_id(
            "candidate-benchmark-metrics",
            candidate,
            source_variant,
            _text(outcome_payload.get("sim_outcome_id")),
            generated_at.isoformat(),
        ),
        "candidate": candidate,
        "candidate_lineage_id": None,
        "as_of": effective_as_of.isoformat(),
        "source_variant": source_variant,
        "source_artifact_id": outcome_payload.get("sim_outcome_id"),
        "source_candidate_metrics_id": candidate_metrics_payload.get("metrics_id"),
        "cost_sensitivity_source": {
            "review_id": cost_review_payload.get("review_id"),
            "cost_sensitivity_status": cost_review.get("cost_sensitivity_status"),
            "validation_status": _mapping(
                cost_review_payload.get("cost_sensitivity_validation")
            ).get("status"),
            "scenario_id": None,
            "scenario_label": None,
        },
        "gross_performance": None,
        "net_performance": None,
        "gross_performance_proxy": None,
        "net_performance_proxy": None,
        "turnover": None,
        "drawdown_proxy": None,
        "trade_rotation_count": None,
        "sample_count": 0,
        "evidence_status": "INSUFFICIENT_DATA",
        "validation_status": "NOT_APPLICABLE",
        "lineage_match": False,
        "metric_source": "simulation_contract_only",
        "limitation": (
            "BACKTEST_SIMULATION and cost proxy inputs are not validated dated "
            "same-candidate lineage evidence; numeric views remain null."
        ),
        **BENCHMARK_BASELINE_METRICS_MATERIALIZATION_SAFETY,
    }


def _baseline_metrics(
    *,
    outcome_payload: Mapping[str, Any],
    outcome_rows: Sequence[Mapping[str, Any]],
    summary_rows: Sequence[Mapping[str, Any]],
    prices: Any,
    effective_as_of: date,
    generated_at: datetime,
    price_cache_path: Path,
    data_quality_summary: Mapping[str, Any],
) -> dict[str, Any]:
    baseline_rows = [
        {
            "baseline_id": baseline_id,
            "source_method": "simulation_contract_unbound",
            "weights": None,
            "gross_performance": None,
            "net_performance": None,
            "gross_performance_proxy": None,
            "net_performance_proxy": None,
            "turnover": None,
            "drawdown_proxy": None,
            "sample_count": 0,
            "missing_window_count": None,
            "metric_status": "INSUFFICIENT_DATA",
            "evidence_status": "INSUFFICIENT_DATA",
            "limitation": (
                "No validated dated baseline metric source with explicit weights "
                "and cost treatment is bound."
            ),
        }
        for baseline_id in baseline_control.REQUIRED_BASELINE_IDS
    ]
    return {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_benchmark_baseline_metrics",
        "metrics_id": st._stable_id(
            "baseline-metrics",
            _text(outcome_payload.get("sim_outcome_id")),
            effective_as_of.isoformat(),
            generated_at.isoformat(),
        ),
        "as_of": effective_as_of.isoformat(),
        "source_artifact_id": outcome_payload.get("sim_outcome_id"),
        "source_window_definition": None,
        "source_window_count": 0,
        "price_cache_path": str(price_cache_path),
        "price_cache_sha256": _file_sha256(price_cache_path),
        "data_quality_status": data_quality_summary.get("status"),
        "baselines": baseline_rows,
        "baseline_count": len(baseline_rows),
        "evidence_status": "INSUFFICIENT_DATA",
        "validation_status": "NOT_APPLICABLE",
        "limitation": (
            "BACKTEST_SIMULATION windows and cached prices are source contracts only; "
            "no numeric baseline evidence is materialized."
        ),
        **BENCHMARK_BASELINE_METRICS_MATERIALIZATION_SAFETY,
    }


def _load_sim_outcome(*, sim_outcome_id: str | None, output_dir: Path) -> dict[str, Any]:
    if not sim_outcome_id:
        return {
            "schema_version": st.SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_missing_sim_outcome_source",
            "sim_outcome_id": "",
            "source_status": "MISSING_EXPLICIT_SOURCE",
            "outcome_mode": "MISSING_EXPLICIT_SOURCE",
            "production_effect": "none",
        }
    return sim.backtest_sim_outcome_report_payload(
        sim_outcome_id=sim_outcome_id,
        latest=False,
        output_dir=output_dir,
    )


def _load_candidate_cost_metrics(
    *,
    metrics_path: Path | None,
    materialization_id: str | None,
    output_dir: Path,
) -> dict[str, Any]:
    if metrics_path is not None:
        return st._read_json(metrics_path)
    if not materialization_id:
        return {
            "metrics_id": "MISSING",
            "evidence_status": "INSUFFICIENT_DATA",
            "limitation": "explicit candidate metrics source is required",
        }
    payload = cost_metrics.cost_metrics_materialization_report_payload(
        materialization_id=materialization_id,
        latest=False,
        output_dir=output_dir,
    )
    report = _mapping(payload.get("cost_metrics_materialization_report"))
    path = Path(_text(report.get("candidate_metrics_path")))
    metrics = st._read_json(path)
    metrics["source_materialization_id"] = payload.get("materialization_id")
    metrics["source_materialization_path"] = payload.get(
        "cost_metrics_materialization_report_path"
    )
    return metrics


def _load_cost_review(*, review_id: str | None, output_dir: Path) -> dict[str, Any]:
    if not review_id:
        return {
            "review_id": "MISSING",
            "cost_sensitivity_review": {},
            "cost_sensitivity_validation": {"status": "MISSING"},
        }
    return cost.cost_sensitivity_report_payload(
        review_id=review_id,
        latest=False,
        output_dir=output_dir,
    )


def _run_data_quality_gate(
    *,
    as_of: date,
    price_cache_path: Path,
    rates_cache_path: Path,
    report_path: Path,
) -> Any:
    universe = load_universe()
    quality = validate_data_cache(
        prices_path=price_cache_path,
        rates_path=rates_cache_path,
        expected_price_tickers=configured_price_tickers(universe),
        expected_rate_series=configured_rate_series(universe),
        quality_config=load_data_quality(),
        as_of=as_of,
        manifest_path=replay._download_manifest_path(price_cache_path),  # noqa: SLF001
        secondary_prices_path=replay._marketstack_prices_path(price_cache_path),  # noqa: SLF001
        require_secondary_prices=replay._requires_marketstack_prices(price_cache_path),  # noqa: SLF001
    )
    write_data_quality_report(quality, report_path)
    return quality


def _quality_summary(quality: Any, *, report_path: Path) -> dict[str, Any]:
    return {
        "status": _text(getattr(quality, "status", "")),
        "as_of": getattr(quality, "as_of", None).isoformat()
        if getattr(quality, "as_of", None)
        else "",
        "error_count": _int(getattr(quality, "error_count", 0)),
        "warning_count": _int(getattr(quality, "warning_count", 0)),
        "report_path": str(report_path),
        "production_effect": "none",
    }


def _metric_statuses(
    *,
    candidate_metrics_payload: Mapping[str, Any],
    baseline_metrics_payload: Mapping[str, Any],
) -> dict[str, Any]:
    baseline_rows = _records(baseline_metrics_payload.get("baselines"))
    available_ids = [
        _text(row.get("baseline_id"))
        for row in baseline_rows
        if _float_or_none(row.get("net_performance_proxy")) is not None
    ]
    missing_ids = sorted(set(baseline_control.REQUIRED_BASELINE_IDS) - set(available_ids))
    return {
        "candidate": "AVAILABLE"
        if _float_or_none(candidate_metrics_payload.get("net_performance_proxy")) is not None
        else "MISSING",
        "required_baseline_ids": list(baseline_control.REQUIRED_BASELINE_IDS),
        "available_baseline_ids": sorted(available_ids),
        "missing_baseline_ids": missing_ids,
        "available_baseline_count": len(available_ids),
        "missing_baseline_count": len(missing_ids),
    }


def _materialization_status_from_metrics(metric_statuses: Mapping[str, Any]) -> str:
    if metric_statuses.get("candidate") != "AVAILABLE":
        return "INSUFFICIENT_BASELINE_METRICS"
    missing = _texts(metric_statuses.get("missing_baseline_ids"))
    if not missing:
        return "BASELINE_METRICS_AVAILABLE"
    if _int(metric_statuses.get("available_baseline_count")):
        return "BASELINE_METRICS_PARTIAL"
    return "INSUFFICIENT_BASELINE_METRICS"


def _materialization_status(
    metric_statuses: Mapping[str, Any],
    control_pack: Mapping[str, Any],
) -> str:
    status = _materialization_status_from_metrics(metric_statuses)
    if _text(control_pack.get("benchmark_baseline_status")) == "INSUFFICIENT_BASELINE_METRICS":
        return "INSUFFICIENT_BASELINE_METRICS"
    return status


def _blocking_reasons(
    metric_statuses: Mapping[str, Any],
    control_pack: Mapping[str, Any],
) -> list[str]:
    reasons: list[str] = []
    if metric_statuses.get("candidate") != "AVAILABLE":
        reasons.append("candidate_metrics:missing_validated_net_performance")
    for baseline_id in _texts(metric_statuses.get("missing_baseline_ids")):
        reasons.append(
            f"baseline_metrics:{baseline_id}:missing_validated_net_performance"
        )
    if _text(control_pack.get("benchmark_baseline_status")) == "INSUFFICIENT_BASELINE_METRICS":
        reasons.append("benchmark_baseline_control:insufficient_metrics")
    return _dedupe(reasons)


def _warnings(
    *,
    candidate_metrics_payload: Mapping[str, Any],
    baseline_metrics_payload: Mapping[str, Any],
    control_pack: Mapping[str, Any],
    data_quality: Any,
) -> list[str]:
    warnings: list[str] = []
    if _text(getattr(data_quality, "status", "")) == "PASS_WITH_WARNINGS":
        warnings.append("data_quality:pass_with_warnings")
    if _text(candidate_metrics_payload.get("outcome_mode")) == "BACKTEST_SIMULATION":
        warnings.append("candidate_metrics:backtest_simulation_not_pit")
    if _text(baseline_metrics_payload.get("limitation")):
        warnings.append("baseline_metrics:backtest_simulation_event_windows")
    benchmark_status = _text(control_pack.get("benchmark_baseline_status"))
    if benchmark_status in {"MIXED_BASELINE_RESULT", "CANDIDATE_UNDERPERFORMS_BASELINES"}:
        warnings.append(f"benchmark_baseline_control:{benchmark_status.lower()}")
    cost_status = _text(
        _mapping(candidate_metrics_payload.get("cost_sensitivity_source")).get(
            "cost_sensitivity_status"
        )
    )
    if cost_status and cost_status not in {
        "MEANINGFUL_ALL_SCENARIOS",
        "MEANINGFUL_LOW_MEDIUM_ONLY",
    }:
        warnings.append(f"cost_sensitivity_review:{cost_status.lower()}")
    return _dedupe(warnings)


def _next_action(status: str, control_pack: Mapping[str, Any]) -> str:
    benchmark_status = _text(control_pack.get("benchmark_baseline_status"))
    if status == "INSUFFICIENT_BASELINE_METRICS":
        return "keep_baseline_control_insufficient_until_candidate_and_baseline_metrics_exist"
    if benchmark_status == "CANDIDATE_OUTPERFORMS_BASELINES":
        return "owner_review_required_before_any_promotion_board_use"
    if benchmark_status == "MIXED_BASELINE_RESULT":
        return "owner_review_mixed_benchmark_results_before_promotion_board"
    if benchmark_status == "CANDIDATE_UNDERPERFORMS_BASELINES":
        return "return_candidate_to_research_until_it_outperforms_baseline_controls"
    return _text(
        control_pack.get("next_required_action"),
        "review_benchmark_baseline_metrics_materialization",
    )


def _source_artifact(outcome_payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "artifact_id": outcome_payload.get("sim_outcome_id"),
        "event_set_id": outcome_payload.get("event_set_id"),
        "variant_set_id": outcome_payload.get("variant_set_id"),
        "outcome_mode": outcome_payload.get("outcome_mode"),
        "pit_safety_status": outcome_payload.get("pit_safety_status"),
        "manifest_path": outcome_payload.get("sim_outcome_manifest_path"),
        "window_path": outcome_payload.get("simulated_outcome_windows_path"),
        "summary_path": outcome_payload.get("simulated_variant_summary_path"),
        "production_effect": "none",
    }


def _candidate_source_artifact(candidate_metrics: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "artifact_id": candidate_metrics.get("source_candidate_metrics_id")
        or candidate_metrics.get("metrics_id"),
        "source_materialization_id": candidate_metrics.get("source_materialization_id"),
        "status": "OK"
        if _float_or_none(candidate_metrics.get("net_performance_proxy")) is not None
        else "MISSING",
        "production_effect": "none",
    }


def _cost_source_artifact(source: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "artifact_id": source.get("review_id"),
        "status": source.get("cost_sensitivity_status"),
        "validation_status": source.get("validation_status"),
        "scenario_id": source.get("scenario_id"),
        "production_effect": "none",
    }


def _price_source_artifact(path: Path) -> dict[str, Any]:
    return {
        "path": str(path),
        "sha256": _file_sha256(path),
        "row_count": _csv_row_count(path),
        "provider": "existing_price_cache",
        "endpoint": "local_cache",
        "request_parameters": "not_downloaded_by_materialization",
        "production_effect": "none",
    }


def _variant_row(rows: Sequence[Mapping[str, Any]], variant: str) -> dict[str, Any]:
    return next((dict(row) for row in rows if _text(row.get("variant")) == variant), {})


def _first_float(payload: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _float_or_none(payload.get(key))
        if value is not None:
            return value
    return None


def _float_or_none(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _float(value: object, default: float = 0.0) -> float:
    parsed = _float_or_none(value)
    return default if parsed is None else parsed


def _int(value: object, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _round_or_none(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 10)


def _parse_date(value: object) -> date | None:
    text = _text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _file_sha256(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _csv_row_count(path: Path) -> int:
    if not path.exists() or not path.is_file():
        return 0
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        return max(0, sum(1 for _ in handle) - 1)


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
