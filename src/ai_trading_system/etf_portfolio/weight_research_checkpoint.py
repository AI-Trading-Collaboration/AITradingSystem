from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_REPORT_DIR

DEFAULT_WEIGHT_RESEARCH_REPORT_DIR = DEFAULT_ETF_REPORT_DIR / "weight_research"
DEFAULT_RESEARCH_SOURCE_DIR = PROJECT_ROOT / "docs" / "research"

SAFETY_BOUNDARY = {
    "research_only": True,
    "manual_review_only": True,
    "paper_shadow_activation": False,
    "extended_shadow_allowed": False,
    "live_trading_allowed": False,
    "official_target_weights": False,
    "broker_action_allowed": False,
    "order_ticket_generated": False,
    "owner_decision_appended": False,
    "production_effect": "none",
}

ARTIFACT_PATHS = {
    "b0": DEFAULT_RESEARCH_SOURCE_DIR / "b0_static_strategic_baseline_result.json",
    "b1": DEFAULT_RESEARCH_SOURCE_DIR / "b1_isolated_attribution_result.json",
    "b2": DEFAULT_RESEARCH_SOURCE_DIR / "b2_risk_scaler_research_result.json",
    "b3": DEFAULT_RESEARCH_SOURCE_DIR / "b3_relative_tilt_research_result.json",
    "b4": DEFAULT_RESEARCH_SOURCE_DIR / "b4_risk_tilt_interaction_result.json",
    "scorecard": DEFAULT_RESEARCH_SOURCE_DIR / "portfolio_utility_scorecard_contract.json",
    "previous_snapshot": DEFAULT_RESEARCH_SOURCE_DIR / "weight_research_program_v1_snapshot.json",
}


def run_weight_research_checkpoint(
    *,
    output_dir: Path = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    alias_dir: Path | None = DEFAULT_RESEARCH_SOURCE_DIR,
    generated_at: datetime | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, tuple[Path, Path]]]:
    generated = generated_at or datetime.now(UTC)
    sources = {name: _read_json(path) for name, path in ARTIFACT_PATHS.items()}
    payloads = build_checkpoint_payloads(sources=sources, generated_at=generated)
    paths = write_checkpoint_payloads(payloads, output_dir=output_dir, alias_dir=alias_dir)
    return payloads, paths


def build_checkpoint_payloads(
    *,
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
) -> dict[str, dict[str, Any]]:
    confidence_contract = _base_payload(
        task_id="TRADING-515A",
        report_type="confidence_shrinkage_contract",
        status="CONFIDENCE_CONTRACT_READY_RESEARCH_ONLY",
        generated_at=generated_at,
        reader_summary="Confidence shrinkage contract is frozen but not allowed to run yet.",
    )
    confidence_contract.update(
        {
            "allowed_inputs": [
                "input_completeness",
                "signal_agreement",
                "schema_compatibility",
                "market_coverage",
                "recent_calibration_stability",
                "uncertainty_measures",
            ],
            "forbidden_inputs": [
                "future_returns",
                "evaluation_result",
                "holdout_performance",
                "owner_decision",
                "broker_or_order_state",
            ],
            "entry_rule": (
                "Run only after a core E/R/T combination has non-BLOCKED signal diagnostics, "
                "same-window controls, and non-INCONCLUSIVE interaction evidence."
            ),
        }
    )

    b4 = sources["b4"]
    confidence_review = _base_payload(
        task_id="TRADING-515B",
        report_type="confidence_interaction_review",
        status="CONFIDENCE_INTERACTION_BLOCKED_CORE_COMBO_INCONCLUSIVE",
        generated_at=generated_at,
        reader_summary="Confidence interaction review is blocked by B4 inconclusive evidence.",
    )
    confidence_review.update(
        {
            "blocking_source": "docs/research/b4_risk_tilt_interaction_result.json",
            "b4_status": b4.get("status"),
            "b4_interaction_classification": _nested(
                b4,
                "interaction_effects",
                "classification",
            ),
            "blocked_comparisons": ["C x R", "C x T", "C x E", "C x R x T"],
            "reason": (
                "No screened core combination is ready for confidence shrinkage; "
                "B4 reports only partial utility and missing full scorecard gates."
            ),
        }
    )

    regime_contract = _base_payload(
        task_id="TRADING-516A",
        report_type="regime_information_contract",
        status="REGIME_INFORMATION_CONTRACT_READY_RESEARCH_ONLY",
        generated_at=generated_at,
        reader_summary="Regime information contract is frozen for later incremental testing.",
    )
    regime_contract.update(
        {
            "must_distinguish_from": [
                "trend_features",
                "risk_score",
                "relative_strength_signal",
                "confidence_shrinkage",
            ],
            "required_evidence": [
                "information_increment_source",
                "redundancy_check",
                "complexity_penalty",
                "window_stability_delta",
                "stress_delta",
                "overfit_risk",
            ],
            "forbidden_inputs": ["untouched_holdout_tuning", "repackaged_existing_risk_score"],
        }
    )

    regime_eval = _base_payload(
        task_id="TRADING-516B",
        report_type="regime_incremental_evaluation",
        status="REGIME_INCREMENTAL_EVALUATION_BLOCKED_NO_PRE_REGIME_COMBO",
        generated_at=generated_at,
        reader_summary=(
            "Regime incremental evaluation is blocked before a best pre-regime combo exists."
        ),
    )
    regime_eval.update(
        {
            "blocked_comparison": "best_pre_regime_combination_vs_same_plus_regime",
            "reason": (
                "B5 confidence review is blocked and B4 is INCONCLUSIVE; there is no "
                "selected pre-regime combination to test."
            ),
        }
    )

    synthesis = _build_synthesis_payload(
        sources=sources,
        confidence_review=confidence_review,
        regime_eval=regime_eval,
        generated_at=generated_at,
    )
    candidate_spec = _base_payload(
        task_id="TRADING-518",
        report_type="candidate_v3_spec_from_proven_effects",
        status="V3_SPEC_BLOCKED_NO_PROVEN_EFFECTS",
        generated_at=generated_at,
        reader_summary="Candidate v3 spec is blocked because no module set is selected.",
    )
    candidate_spec.update(
        {
            "selected_modules": synthesis["selected_modules"],
            "blocked_by": [
                "B4 interaction classification is INCONCLUSIVE",
                "B5 confidence interaction review is blocked",
                "B6 regime incremental evaluation is blocked",
                "full scorecard/stress/benchmark gates are incomplete",
            ],
        }
    )
    candidate_gate = _base_payload(
        task_id="TRADING-519",
        report_type="candidate_v3_mini_gate_result",
        status="V3_BLOCKED",
        generated_at=generated_at,
        reader_summary="V3 mini gate is blocked because no candidate spec exists.",
    )
    candidate_gate.update(
        {
            "gate_checks": [
                {"check": "candidate_spec_exists", "status": "FAIL"},
                {"check": "signal_robustness_not_blocked", "status": "NOT_EVALUATED"},
                {"check": "window_not_fragile", "status": "NOT_EVALUATED"},
                {"check": "stress_not_weak", "status": "NOT_EVALUATED"},
                {"check": "medium_cost_result_not_weak", "status": "NOT_EVALUATED"},
                {"check": "benchmark_relative_result_not_weak", "status": "NOT_EVALUATED"},
                {"check": "no_unresolved_negative_interaction", "status": "FAIL"},
            ],
            "untouched_holdout_accessed": False,
        }
    )
    snapshot = _build_program_snapshot(
        sources=sources,
        confidence_review=confidence_review,
        regime_eval=regime_eval,
        synthesis=synthesis,
        candidate_spec=candidate_spec,
        candidate_gate=candidate_gate,
        generated_at=generated_at,
    )
    return {
        "confidence_shrinkage_contract": confidence_contract,
        "confidence_interaction_review": confidence_review,
        "regime_information_contract": regime_contract,
        "regime_incremental_evaluation": regime_eval,
        "main_interaction_effect_synthesis": synthesis,
        "candidate_v3_spec_from_proven_effects": candidate_spec,
        "candidate_v3_mini_gate_result": candidate_gate,
        "weight_research_program_v1_snapshot": snapshot,
    }


def write_checkpoint_payloads(
    payloads: dict[str, dict[str, Any]],
    *,
    output_dir: Path,
    alias_dir: Path | None,
) -> dict[str, tuple[Path, Path]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    paths: dict[str, tuple[Path, Path]] = {}
    for stem, payload in payloads.items():
        json_path = output_dir / f"{stem}_{stamp}.json"
        md_path = output_dir / f"{stem}_{stamp}.md"
        markdown = render_checkpoint_payload(stem, payload)
        _write_json(json_path, payload)
        md_path.write_text(markdown, encoding="utf-8")
        if alias_dir is not None:
            alias_dir.mkdir(parents=True, exist_ok=True)
            _write_json(alias_dir / f"{stem}.json", payload)
            (alias_dir / f"{stem}.md").write_text(markdown, encoding="utf-8")
        paths[stem] = (json_path, md_path)
    if alias_dir is not None:
        (alias_dir / "weight_research_program_v1_reader_brief.md").write_text(
            render_reader_brief(payloads["weight_research_program_v1_snapshot"]),
            encoding="utf-8",
        )
    return paths


def render_checkpoint_payload(stem: str, payload: dict[str, Any]) -> str:
    title = stem.replace("_", " ").title()
    lines = [
        f"# {title}",
        "",
        f"- Status：{payload['status']}",
        f"- Market Regime：{payload['market_regime']}",
        f"- Production Effect：{payload['safety_boundary']['production_effect']}",
        "",
        "## Reader Brief",
        "",
        f"- Summary：{payload['reader_brief']['summary']}",
        f"- Key Result：{payload['reader_brief']['key_result']}",
        f"- Blocking Issues：{payload['reader_brief']['blocking_issues']}",
        f"- Warnings：{payload['reader_brief']['warnings']}",
        f"- Safety Boundary：{payload['reader_brief']['safety_boundary']}",
        f"- Next Action：{payload['reader_brief']['next_action']}",
    ]
    if stem == "weight_research_program_v1_snapshot":
        lines.extend(["", "## Layer Status", ""])
        for row in payload["b0_to_b6_results"]:
            lines.append(
                f"- {row['layer_id']}：{row['status']} "
                f"({row.get('result_artifact') or row.get('blocking_reason')})"
            )
    return "\n".join(lines) + "\n"


def render_reader_brief(snapshot: dict[str, Any]) -> str:
    brief = snapshot["reader_brief"]
    return "\n".join(
        [
            "# Weight Research Program v1 Reader Brief",
            "",
            f"最后更新：{snapshot['as_of']}",
            "",
            "## Summary",
            "",
            brief["summary"],
            "",
            "## Key Result",
            "",
            f"`{brief['key_result']}`",
            "",
            "## Blocking Issues",
            "",
            brief["blocking_issues"],
            "",
            "## Warnings",
            "",
            brief["warnings"],
            "",
            "## Safety Boundary",
            "",
            f"`{brief['safety_boundary']}`",
            "",
            "## Next Action",
            "",
            brief["next_action"],
            "",
        ]
    )


def _build_synthesis_payload(
    *,
    sources: dict[str, dict[str, Any]],
    confidence_review: dict[str, Any],
    regime_eval: dict[str, Any],
    generated_at: datetime,
) -> dict[str, Any]:
    payload = _base_payload(
        task_id="TRADING-517",
        report_type="main_interaction_effect_synthesis",
        status="INCONCLUSIVE",
        generated_at=generated_at,
        reader_summary="Main and interaction synthesis is inconclusive after B4.",
    )
    payload.update(
        {
            "module_decisions": [
                {
                    "module": "E",
                    "decision": "CONDITIONAL",
                    "reason": (
                        "B1E is valid mixed; improves turnover but cannot default into "
                        "final candidates."
                    ),
                },
                {
                    "module": "R",
                    "decision": "INCONCLUSIVE",
                    "reason": (
                        "B2 completed mini-backfill but full stress/benchmark/scorecard "
                        "gates are missing."
                    ),
                },
                {
                    "module": "T",
                    "decision": "INCONCLUSIVE",
                    "reason": (
                        "B3 completed mini-backfill but worsened E1 return/drawdown in "
                        "the mini window."
                    ),
                },
                {
                    "module": "R x T",
                    "decision": "INCONCLUSIVE",
                    "reason": _nested(
                        sources["b4"],
                        "interaction_effects",
                        "classification_reason",
                    ),
                },
                {
                    "module": "C",
                    "decision": "BLOCKED",
                    "reason": confidence_review["reason"],
                },
                {
                    "module": "G",
                    "decision": "BLOCKED",
                    "reason": regime_eval["reason"],
                },
            ],
            "selected_modules": [],
            "rejected_modules": [],
            "conditional_modules": ["E"],
            "inconclusive_modules": ["R", "T", "R x T"],
            "blocked_modules": ["C", "G"],
            "source_statuses": {
                "B1": sources["b1"].get("status"),
                "B2": sources["b2"].get("status"),
                "B3": sources["b3"].get("status"),
                "B4": sources["b4"].get("status"),
            },
        }
    )
    return payload


def _build_program_snapshot(
    *,
    sources: dict[str, dict[str, Any]],
    confidence_review: dict[str, Any],
    regime_eval: dict[str, Any],
    synthesis: dict[str, Any],
    candidate_spec: dict[str, Any],
    candidate_gate: dict[str, Any],
    generated_at: datetime,
) -> dict[str, Any]:
    previous = sources["previous_snapshot"]
    previous_results = {
        row["layer_id"]: row
        for row in previous.get("b0_to_b6_results", [])
        if isinstance(row, dict) and "layer_id" in row
    }
    payload = _base_payload(
        task_id="TRADING-520",
        report_type="weight_research_program_v1_snapshot",
        status="WEIGHT_RESEARCH_PROGRAM_NEEDS_MORE_EVIDENCE",
        generated_at=generated_at,
        reader_summary=(
            "B1-B4 research-only mini-backfills are complete, but B5/B6 and v3 gates "
            "remain blocked by inconclusive interaction evidence and missing full scorecard gates."
        ),
    )
    payload.update(
        {
            "as_of": generated_at.date().isoformat(),
            "included_artifacts": [
                "docs/research/b0_static_strategic_baseline_result.json",
                "docs/research/b1_isolated_attribution_result.json",
                "docs/research/b2_risk_scaler_research_result.json",
                "docs/research/b3_relative_tilt_research_result.json",
                "docs/research/b4_risk_tilt_interaction_result.json",
                "docs/research/confidence_shrinkage_contract.json",
                "docs/research/confidence_interaction_review.json",
                "docs/research/regime_information_contract.json",
                "docs/research/regime_incremental_evaluation.json",
                "docs/research/main_interaction_effect_synthesis.json",
                "docs/research/candidate_v3_spec_from_proven_effects.json",
                "docs/research/candidate_v3_mini_gate_result.json",
            ],
            "phase_statuses": [
                {
                    "phase": "Phase 0",
                    "status": "B1_ATTRIBUTION_REPAIRED_VALID_MIXED",
                    "tasks": ["TRADING-511E", "TRADING-511F", "TRADING-511G"],
                },
                {
                    "phase": "Phase 1",
                    "status": "INTERFACES_AND_DIAGNOSTICS_READY",
                    "tasks": ["TRADING-512A", "TRADING-512B"],
                },
                {
                    "phase": "Phase 2-4",
                    "status": "B2_B3_B4_MINI_BACKFILLS_DONE_RESEARCH_ONLY",
                    "tasks": ["TRADING-512C-F", "TRADING-513A-D", "TRADING-514A-B"],
                },
                {
                    "phase": "Phase 5-7",
                    "status": "BLOCKED_PENDING_FULL_SCORECARD_AND_OWNER_REVIEW",
                    "tasks": [
                        "TRADING-515",
                        "TRADING-516",
                        "TRADING-517",
                        "TRADING-518",
                        "TRADING-519",
                    ],
                },
            ],
            "b0_to_b6_results": [
                previous_results.get("B0", {"layer_id": "B0", "status": "NOT_AVAILABLE"}),
                previous_results.get(
                    "B1",
                    {"layer_id": "B1", "status": sources["b1"].get("status")},
                ),
                _layer_result(
                    "B2",
                    sources["b2"],
                    "b2_e1_metrics",
                    "docs/research/b2_risk_scaler_research_result.json",
                ),
                _layer_result(
                    "B3",
                    sources["b3"],
                    "b3_e1_metrics",
                    "docs/research/b3_relative_tilt_research_result.json",
                ),
                _layer_result(
                    "B4",
                    sources["b4"],
                    "b4_e1_metrics",
                    "docs/research/b4_risk_tilt_interaction_result.json",
                ),
                {
                    "layer_id": "B5",
                    "status": confidence_review["status"],
                    "result_artifact": "docs/research/confidence_interaction_review.json",
                    "blocking_reason": confidence_review["reason"],
                },
                {
                    "layer_id": "B6",
                    "status": regime_eval["status"],
                    "result_artifact": "docs/research/regime_incremental_evaluation.json",
                    "blocking_reason": regime_eval["reason"],
                },
            ],
            "selected_modules": synthesis["selected_modules"],
            "conditional_modules": synthesis["conditional_modules"],
            "inconclusive_modules": synthesis["inconclusive_modules"],
            "blocked_modules": synthesis["blocked_modules"],
            "v3_candidate_status": candidate_spec["status"],
            "v3_mini_gate_status": candidate_gate["status"],
            "next_recommended_owner_action": (
                "Review B1-B4 mixed/inconclusive evidence, decide whether to run full "
                "scorecard/stress/benchmark gates or redesign B2/B3 policies before B5/B6."
            ),
        }
    )
    payload["reader_brief"].update(
        {
            "blocking_issues": (
                "B4 interaction classification is INCONCLUSIVE; B5 confidence review and "
                "B6 regime incremental evaluation are blocked; v3 spec/gate remain blocked."
            ),
            "warnings": (
                "B2/B3/B4 are research-only mini-backfills and do not prove a production "
                "candidate; untouched holdout was not accessed."
            ),
            "next_action": payload["next_recommended_owner_action"],
        }
    )
    return payload


def _layer_result(
    layer_id: str,
    source: dict[str, Any],
    metrics_key: str,
    artifact: str,
) -> dict[str, Any]:
    metrics = source.get(metrics_key, {})
    return {
        "layer_id": layer_id,
        "status": source.get("status"),
        "result_artifact": artifact,
        "return_proxy": _maybe_float(metrics, "total_return"),
        "drawdown_proxy": _maybe_float(metrics, "max_drawdown"),
        "turnover": _maybe_float(metrics, "turnover"),
        "blocking_reason": None,
    }


def _base_payload(
    *,
    task_id: str,
    report_type: str,
    status: str,
    generated_at: datetime,
    reader_summary: str,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "task_id": task_id,
        "report_type": report_type,
        "status": status,
        "generated_at": generated_at.isoformat(),
        "market_regime": "ai_after_chatgpt",
        "reader_brief": {
            "summary": reader_summary,
            "key_result": status,
            "blocking_issues": "see artifact body",
            "warnings": "research-only; no official target weights or production effect",
            "safety_boundary": (
                "research_only=true; manual_review_only=true; "
                "official_target_weights=false; production_effect=none"
            ),
            "next_action": "continue only after the stated blocker is resolved",
        },
        "safety_boundary": dict(SAFETY_BOUNDARY),
    }


def _nested(payload: dict[str, Any], *keys: str) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _maybe_float(payload: Any, key: str) -> float | None:
    if not isinstance(payload, dict) or payload.get(key) is None:
        return None
    return float(payload[key])


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"status": "MISSING", "path": str(path)}
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
