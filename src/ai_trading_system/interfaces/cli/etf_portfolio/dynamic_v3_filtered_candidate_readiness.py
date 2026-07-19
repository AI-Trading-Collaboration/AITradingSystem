from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio import (
    dynamic_v3_filtered_candidate_readiness_pipeline as readiness,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import mapping_obj as _mapping_obj
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_drawdown_mismatch_reduction_app,
    dynamic_v3_filtered_candidate_ab_review_app,
    dynamic_v3_filtered_candidate_evidence_app,
    dynamic_v3_filtered_candidate_stress_backfill_app,
    dynamic_v3_filtered_formalization_readiness_app,
    dynamic_v3_filtered_next_decision_app,
    dynamic_v3_flip_rotation_reduction_app,
    dynamic_v3_median_regime_filter_spec_app,
    dynamic_v3_owner_filtered_candidate_review_app,
    dynamic_v3_rescue_app,
    dynamic_v3_signal_gate_confirmation_app,
)


def _echo_validation(payload: dict[str, object]) -> None:
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_filtered_candidate_evidence_app.command("run")
def dynamic_v3_filtered_candidate_evidence_run_command(
    candidate: Annotated[str, typer.Option("--candidate", help="filtered candidate id。")] = (
        readiness.TOP_FILTERED_CANDIDATE
    ),
    filtered_comparison_id: Annotated[
        str,
        typer.Option("--filtered-comparison-id", help="filtered-vs-original comparison id。"),
    ] = "",
    promotion_review_id: Annotated[
        str,
        typer.Option("--promotion-review-id", help="filtered candidate promotion review id。"),
    ] = "",
    comparison_dir: Annotated[
        Path,
        typer.Option("--comparison-dir", help="filtered-vs-original comparison root。"),
    ] = readiness.DEFAULT_FILTERED_VS_ORIGINAL_COMPARISON_DIR,
    promotion_review_dir: Annotated[
        Path,
        typer.Option("--promotion-review-dir", help="filtered candidate promotion review root。"),
    ] = readiness.DEFAULT_FILTERED_CANDIDATE_PROMOTION_REVIEW_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="filtered candidate evidence root。"),
    ] = readiness.DEFAULT_FILTERED_CANDIDATE_EVIDENCE_DIR,
) -> None:
    if not filtered_comparison_id or not promotion_review_id:
        raise typer.BadParameter("filtered-comparison-id and promotion-review-id are required")
    result = readiness.run_filtered_candidate_evidence(
        candidate=candidate,
        filtered_comparison_id=filtered_comparison_id,
        promotion_review_id=promotion_review_id,
        comparison_dir=comparison_dir,
        promotion_review_dir=promotion_review_dir,
        output_dir=output_dir,
    )
    summary = _mapping_obj(result.get("filtered_candidate_evidence_summary"))
    typer.echo(f"evidence_id={result['evidence_id']}")
    typer.echo(f"status={result['status']}")
    typer.echo(f"evidence_status={summary.get('evidence_status')}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_filtered_candidate_evidence_app.command("report")
def dynamic_v3_filtered_candidate_evidence_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    evidence_id: Annotated[
        str | None, typer.Option("--evidence-id", help="filtered candidate evidence id。")
    ] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="filtered candidate evidence root。")
    ] = readiness.DEFAULT_FILTERED_CANDIDATE_EVIDENCE_DIR,
) -> None:
    payload = readiness.filtered_candidate_evidence_report_payload(
        evidence_id=evidence_id, latest=latest, output_dir=output_dir
    )
    summary = _mapping_obj(payload.get("filtered_candidate_evidence_summary"))
    typer.echo(f"evidence_id={payload['evidence_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"evidence_status={summary.get('evidence_status')}")
    typer.echo(f"report_path={payload['filtered_candidate_evidence_report_path']}")


@dynamic_v3_rescue_app.command("validate-filtered-candidate-evidence")
def dynamic_v3_validate_filtered_candidate_evidence_command(
    evidence_id: Annotated[str, typer.Option("--evidence-id", help="evidence id。")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="filtered candidate evidence root。")
    ] = readiness.DEFAULT_FILTERED_CANDIDATE_EVIDENCE_DIR,
) -> None:
    _echo_validation(
        readiness.validate_filtered_candidate_evidence_artifact(
            evidence_id=evidence_id, output_dir=output_dir
        )
    )


@dynamic_v3_median_regime_filter_spec_app.command("review")
def dynamic_v3_median_regime_filter_spec_review_command(
    candidate: Annotated[str, typer.Option("--candidate", help="filtered candidate id。")] = (
        readiness.TOP_FILTERED_CANDIDATE
    ),
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="median regime filter spec root。")
    ] = readiness.DEFAULT_MEDIAN_REGIME_FILTER_SPEC_DIR,
) -> None:
    result = readiness.review_median_regime_filter_spec(candidate=candidate, output_dir=output_dir)
    contract = _mapping_obj(result.get("median_regime_filter_contract"))
    typer.echo(f"spec_id={result['spec_id']}")
    typer.echo(f"status={result['status']}")
    typer.echo(f"contract_status={contract.get('contract_status')}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_median_regime_filter_spec_app.command("report")
def dynamic_v3_median_regime_filter_spec_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    spec_id: Annotated[str | None, typer.Option("--spec-id", help="spec id。")] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="median regime filter spec root。")
    ] = readiness.DEFAULT_MEDIAN_REGIME_FILTER_SPEC_DIR,
) -> None:
    payload = readiness.median_regime_filter_spec_report_payload(
        spec_id=spec_id, latest=latest, output_dir=output_dir
    )
    contract = _mapping_obj(payload.get("median_regime_filter_contract"))
    typer.echo(f"spec_id={payload['spec_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"contract_status={contract.get('contract_status')}")
    typer.echo(f"report_path={payload['median_regime_filter_spec_report_path']}")


@dynamic_v3_rescue_app.command("validate-median-regime-filter-spec")
def dynamic_v3_validate_median_regime_filter_spec_command(
    spec_id: Annotated[str, typer.Option("--spec-id", help="spec id。")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="median regime filter spec root。")
    ] = readiness.DEFAULT_MEDIAN_REGIME_FILTER_SPEC_DIR,
) -> None:
    _echo_validation(
        readiness.validate_median_regime_filter_spec_artifact(
            spec_id=spec_id, output_dir=output_dir
        )
    )


@dynamic_v3_filtered_candidate_stress_backfill_app.command("run")
def dynamic_v3_filtered_candidate_stress_backfill_run_command(
    candidate: Annotated[str, typer.Option("--candidate", help="filtered candidate id。")] = (
        readiness.TOP_FILTERED_CANDIDATE
    ),
    spec_id: Annotated[str, typer.Option("--spec-id", help="median regime filter spec id。")] = "",
    spec_dir: Annotated[
        Path, typer.Option("--spec-dir", help="median regime filter spec root。")
    ] = readiness.DEFAULT_MEDIAN_REGIME_FILTER_SPEC_DIR,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="stress backfill root。")
    ] = readiness.DEFAULT_FILTERED_CANDIDATE_STRESS_BACKFILL_DIR,
) -> None:
    if not spec_id:
        raise typer.BadParameter("spec-id is required")
    result = readiness.run_filtered_candidate_stress_backfill(
        candidate=candidate, spec_id=spec_id, spec_dir=spec_dir, output_dir=output_dir
    )
    summary = _mapping_obj(result.get("filtered_candidate_stress_summary"))
    typer.echo(f"stress_backfill_id={result['stress_backfill_id']}")
    typer.echo(f"status={result['status']}")
    typer.echo(f"stress_robustness_status={summary.get('stress_robustness_status')}")
    typer.echo("production_effect=none")


@dynamic_v3_filtered_candidate_stress_backfill_app.command("report")
def dynamic_v3_filtered_candidate_stress_backfill_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    stress_backfill_id: Annotated[
        str | None, typer.Option("--stress-backfill-id", help="stress backfill id。")
    ] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="stress backfill root。")
    ] = readiness.DEFAULT_FILTERED_CANDIDATE_STRESS_BACKFILL_DIR,
) -> None:
    payload = readiness.filtered_candidate_stress_backfill_report_payload(
        stress_backfill_id=stress_backfill_id, latest=latest, output_dir=output_dir
    )
    summary = _mapping_obj(payload.get("filtered_candidate_stress_summary"))
    typer.echo(f"stress_backfill_id={payload['stress_backfill_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"stress_robustness_status={summary.get('stress_robustness_status')}")
    typer.echo(f"report_path={payload['filtered_candidate_stress_report_path']}")


@dynamic_v3_rescue_app.command("validate-filtered-candidate-stress-backfill")
def dynamic_v3_validate_filtered_candidate_stress_backfill_command(
    stress_backfill_id: Annotated[
        str, typer.Option("--stress-backfill-id", help="stress backfill id。")
    ],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="stress backfill root。")
    ] = readiness.DEFAULT_FILTERED_CANDIDATE_STRESS_BACKFILL_DIR,
) -> None:
    _echo_validation(
        readiness.validate_filtered_candidate_stress_backfill_artifact(
            stress_backfill_id=stress_backfill_id, output_dir=output_dir
        )
    )


@dynamic_v3_drawdown_mismatch_reduction_app.command("run")
def dynamic_v3_drawdown_mismatch_reduction_run_command(
    stress_backfill_id: Annotated[
        str, typer.Option("--stress-backfill-id", help="stress backfill id。")
    ],
    stress_backfill_dir: Annotated[
        Path, typer.Option("--stress-backfill-dir", help="stress backfill root。")
    ] = readiness.DEFAULT_FILTERED_CANDIDATE_STRESS_BACKFILL_DIR,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="drawdown mismatch reduction root。")
    ] = readiness.DEFAULT_DRAWDOWN_MISMATCH_REDUCTION_DIR,
) -> None:
    result = readiness.run_drawdown_mismatch_reduction(
        stress_backfill_id=stress_backfill_id,
        stress_backfill_dir=stress_backfill_dir,
        output_dir=output_dir,
    )
    summary = _mapping_obj(result.get("mismatch_reduction_summary"))
    typer.echo(f"reduction_id={result['reduction_id']}")
    typer.echo(f"status={result['status']}")
    typer.echo(
        f"drawdown_mismatch_reduction_status={summary.get('drawdown_mismatch_reduction_status')}"
    )
    typer.echo("production_effect=none")


@dynamic_v3_drawdown_mismatch_reduction_app.command("report")
def dynamic_v3_drawdown_mismatch_reduction_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    reduction_id: Annotated[
        str | None, typer.Option("--reduction-id", help="reduction id。")
    ] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="drawdown mismatch reduction root。")
    ] = readiness.DEFAULT_DRAWDOWN_MISMATCH_REDUCTION_DIR,
) -> None:
    payload = readiness.drawdown_mismatch_reduction_report_payload(
        reduction_id=reduction_id, latest=latest, output_dir=output_dir
    )
    summary = _mapping_obj(payload.get("mismatch_reduction_summary"))
    typer.echo(f"reduction_id={payload['reduction_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(
        f"drawdown_mismatch_reduction_status={summary.get('drawdown_mismatch_reduction_status')}"
    )
    typer.echo(f"report_path={payload['drawdown_mismatch_reduction_report_path']}")


@dynamic_v3_rescue_app.command("validate-drawdown-mismatch-reduction")
def dynamic_v3_validate_drawdown_mismatch_reduction_command(
    reduction_id: Annotated[str, typer.Option("--reduction-id", help="reduction id。")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="drawdown mismatch reduction root。")
    ] = readiness.DEFAULT_DRAWDOWN_MISMATCH_REDUCTION_DIR,
) -> None:
    _echo_validation(
        readiness.validate_drawdown_mismatch_reduction_artifact(
            reduction_id=reduction_id, output_dir=output_dir
        )
    )


@dynamic_v3_flip_rotation_reduction_app.command("run")
def dynamic_v3_flip_rotation_reduction_run_command(
    stress_backfill_id: Annotated[
        str, typer.Option("--stress-backfill-id", help="stress backfill id。")
    ],
    stress_backfill_dir: Annotated[
        Path, typer.Option("--stress-backfill-dir", help="stress backfill root。")
    ] = readiness.DEFAULT_FILTERED_CANDIDATE_STRESS_BACKFILL_DIR,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="flip rotation reduction root。")
    ] = readiness.DEFAULT_FLIP_ROTATION_REDUCTION_DIR,
) -> None:
    result = readiness.run_flip_rotation_reduction(
        stress_backfill_id=stress_backfill_id,
        stress_backfill_dir=stress_backfill_dir,
        output_dir=output_dir,
    )
    summary = _mapping_obj(result.get("flip_rotation_reduction_summary"))
    typer.echo(f"flip_reduction_id={result['flip_reduction_id']}")
    typer.echo(f"status={result['status']}")
    typer.echo(f"flip_reduction_status={summary.get('flip_reduction_status')}")
    typer.echo(f"rotation_reduction_status={summary.get('rotation_reduction_status')}")


@dynamic_v3_flip_rotation_reduction_app.command("report")
def dynamic_v3_flip_rotation_reduction_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    flip_reduction_id: Annotated[
        str | None, typer.Option("--flip-reduction-id", help="flip reduction id。")
    ] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="flip rotation reduction root。")
    ] = readiness.DEFAULT_FLIP_ROTATION_REDUCTION_DIR,
) -> None:
    payload = readiness.flip_rotation_reduction_report_payload(
        flip_reduction_id=flip_reduction_id, latest=latest, output_dir=output_dir
    )
    summary = _mapping_obj(payload.get("flip_rotation_reduction_summary"))
    typer.echo(f"flip_reduction_id={payload['flip_reduction_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"flip_reduction_status={summary.get('flip_reduction_status')}")
    typer.echo(f"report_path={payload['flip_rotation_reduction_report_path']}")


@dynamic_v3_rescue_app.command("validate-flip-rotation-reduction")
def dynamic_v3_validate_flip_rotation_reduction_command(
    flip_reduction_id: Annotated[
        str, typer.Option("--flip-reduction-id", help="flip reduction id。")
    ],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="flip rotation reduction root。")
    ] = readiness.DEFAULT_FLIP_ROTATION_REDUCTION_DIR,
) -> None:
    _echo_validation(
        readiness.validate_flip_rotation_reduction_artifact(
            flip_reduction_id=flip_reduction_id, output_dir=output_dir
        )
    )


@dynamic_v3_filtered_candidate_ab_review_app.command("run")
def dynamic_v3_filtered_candidate_ab_review_run_command(
    stress_backfill_id: Annotated[
        str, typer.Option("--stress-backfill-id", help="stress backfill id。")
    ],
    mismatch_reduction_id: Annotated[
        str, typer.Option("--mismatch-reduction-id", help="drawdown mismatch reduction id。")
    ],
    flip_reduction_id: Annotated[
        str, typer.Option("--flip-reduction-id", help="flip reduction id。")
    ],
    stress_backfill_dir: Annotated[
        Path, typer.Option("--stress-backfill-dir", help="stress backfill root。")
    ] = readiness.DEFAULT_FILTERED_CANDIDATE_STRESS_BACKFILL_DIR,
    mismatch_reduction_dir: Annotated[
        Path, typer.Option("--mismatch-reduction-dir", help="drawdown mismatch reduction root。")
    ] = readiness.DEFAULT_DRAWDOWN_MISMATCH_REDUCTION_DIR,
    flip_reduction_dir: Annotated[
        Path, typer.Option("--flip-reduction-dir", help="flip rotation reduction root。")
    ] = readiness.DEFAULT_FLIP_ROTATION_REDUCTION_DIR,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="filtered candidate A/B root。")
    ] = readiness.DEFAULT_FILTERED_CANDIDATE_AB_REVIEW_DIR,
) -> None:
    result = readiness.run_filtered_candidate_ab_review(
        stress_backfill_id=stress_backfill_id,
        mismatch_reduction_id=mismatch_reduction_id,
        flip_reduction_id=flip_reduction_id,
        stress_backfill_dir=stress_backfill_dir,
        mismatch_reduction_dir=mismatch_reduction_dir,
        flip_reduction_dir=flip_reduction_dir,
        output_dir=output_dir,
    )
    summary = _mapping_obj(result.get("ab_summary"))
    typer.echo(f"ab_review_id={result['ab_review_id']}")
    typer.echo(f"status={result['status']}")
    typer.echo(f"overall_ab_status={summary.get('overall_ab_status')}")
    typer.echo("production_effect=none")


@dynamic_v3_filtered_candidate_ab_review_app.command("report")
def dynamic_v3_filtered_candidate_ab_review_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    ab_review_id: Annotated[
        str | None, typer.Option("--ab-review-id", help="A/B review id。")
    ] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="filtered candidate A/B root。")
    ] = readiness.DEFAULT_FILTERED_CANDIDATE_AB_REVIEW_DIR,
) -> None:
    payload = readiness.filtered_candidate_ab_review_report_payload(
        ab_review_id=ab_review_id, latest=latest, output_dir=output_dir
    )
    summary = _mapping_obj(payload.get("ab_summary"))
    typer.echo(f"ab_review_id={payload['ab_review_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"overall_ab_status={summary.get('overall_ab_status')}")
    typer.echo(f"report_path={payload['filtered_candidate_ab_report_path']}")


@dynamic_v3_rescue_app.command("validate-filtered-candidate-ab-review")
def dynamic_v3_validate_filtered_candidate_ab_review_command(
    ab_review_id: Annotated[str, typer.Option("--ab-review-id", help="A/B review id。")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="filtered candidate A/B root。")
    ] = readiness.DEFAULT_FILTERED_CANDIDATE_AB_REVIEW_DIR,
) -> None:
    _echo_validation(
        readiness.validate_filtered_candidate_ab_review_artifact(
            ab_review_id=ab_review_id, output_dir=output_dir
        )
    )


@dynamic_v3_signal_gate_confirmation_app.command("register")
def dynamic_v3_signal_gate_confirmation_register_command(
    ab_review_id: Annotated[str, typer.Option("--ab-review-id", help="A/B review id。")],
    ab_review_dir: Annotated[
        Path, typer.Option("--ab-review-dir", help="filtered candidate A/B root。")
    ] = readiness.DEFAULT_FILTERED_CANDIDATE_AB_REVIEW_DIR,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="signal gate confirmation root。")
    ] = readiness.DEFAULT_SIGNAL_GATE_CONFIRMATION_DIR,
) -> None:
    result = readiness.register_signal_gate_confirmation(
        ab_review_id=ab_review_id, ab_review_dir=ab_review_dir, output_dir=output_dir
    )
    targets = _mapping_obj(result.get("signal_gate_confirmation_targets"))
    typer.echo(f"confirmation_id={result['confirmation_id']}")
    typer.echo(f"status={result['status']}")
    typer.echo(f"target_count={len(targets.get('targets', []))}")
    typer.echo("auto_apply=false")


@dynamic_v3_signal_gate_confirmation_app.command("report")
def dynamic_v3_signal_gate_confirmation_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    confirmation_id: Annotated[
        str | None, typer.Option("--confirmation-id", help="confirmation id。")
    ] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="signal gate confirmation root。")
    ] = readiness.DEFAULT_SIGNAL_GATE_CONFIRMATION_DIR,
) -> None:
    payload = readiness.signal_gate_confirmation_report_payload(
        confirmation_id=confirmation_id, latest=latest, output_dir=output_dir
    )
    targets = _mapping_obj(payload.get("signal_gate_confirmation_targets"))
    typer.echo(f"confirmation_id={payload['confirmation_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"target_count={len(targets.get('targets', []))}")
    typer.echo(f"report_path={payload['signal_gate_confirmation_report_path']}")


@dynamic_v3_rescue_app.command("validate-signal-gate-confirmation")
def dynamic_v3_validate_signal_gate_confirmation_command(
    confirmation_id: Annotated[str, typer.Option("--confirmation-id", help="confirmation id。")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="signal gate confirmation root。")
    ] = readiness.DEFAULT_SIGNAL_GATE_CONFIRMATION_DIR,
) -> None:
    _echo_validation(
        readiness.validate_signal_gate_confirmation_artifact(
            confirmation_id=confirmation_id, output_dir=output_dir
        )
    )


@dynamic_v3_filtered_formalization_readiness_app.command("run")
def dynamic_v3_filtered_formalization_readiness_run_command(
    ab_review_id: Annotated[str, typer.Option("--ab-review-id", help="A/B review id。")],
    confirmation_id: Annotated[str, typer.Option("--confirmation-id", help="confirmation id。")],
    ab_review_dir: Annotated[
        Path, typer.Option("--ab-review-dir", help="filtered candidate A/B root。")
    ] = readiness.DEFAULT_FILTERED_CANDIDATE_AB_REVIEW_DIR,
    confirmation_dir: Annotated[
        Path, typer.Option("--confirmation-dir", help="signal gate confirmation root。")
    ] = readiness.DEFAULT_SIGNAL_GATE_CONFIRMATION_DIR,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="filtered formalization readiness root。")
    ] = readiness.DEFAULT_FILTERED_FORMALIZATION_READINESS_DIR,
) -> None:
    result = readiness.run_filtered_formalization_readiness(
        ab_review_id=ab_review_id,
        confirmation_id=confirmation_id,
        ab_review_dir=ab_review_dir,
        confirmation_dir=confirmation_dir,
        output_dir=output_dir,
    )
    decision = _mapping_obj(result.get("formalization_readiness_decision"))
    typer.echo(f"readiness_id={result['readiness_id']}")
    typer.echo(f"status={result['status']}")
    typer.echo(f"decision={decision.get('decision')}")
    typer.echo("can_write_official_target_weights=false")


@dynamic_v3_filtered_formalization_readiness_app.command("report")
def dynamic_v3_filtered_formalization_readiness_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    readiness_id: Annotated[
        str | None, typer.Option("--readiness-id", help="readiness id。")
    ] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="filtered formalization readiness root。")
    ] = readiness.DEFAULT_FILTERED_FORMALIZATION_READINESS_DIR,
) -> None:
    payload = readiness.filtered_formalization_readiness_report_payload(
        readiness_id=readiness_id, latest=latest, output_dir=output_dir
    )
    decision = _mapping_obj(payload.get("formalization_readiness_decision"))
    typer.echo(f"readiness_id={payload['readiness_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"decision={decision.get('decision')}")
    typer.echo(f"report_path={payload['filtered_formalization_report_path']}")


@dynamic_v3_rescue_app.command("validate-filtered-formalization-readiness")
def dynamic_v3_validate_filtered_formalization_readiness_command(
    readiness_id: Annotated[str, typer.Option("--readiness-id", help="readiness id。")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="filtered formalization readiness root。")
    ] = readiness.DEFAULT_FILTERED_FORMALIZATION_READINESS_DIR,
) -> None:
    _echo_validation(
        readiness.validate_filtered_formalization_readiness_artifact(
            readiness_id=readiness_id, output_dir=output_dir
        )
    )


@dynamic_v3_owner_filtered_candidate_review_app.command("pack")
def dynamic_v3_owner_filtered_candidate_review_pack_command(
    readiness_id: Annotated[str, typer.Option("--readiness-id", help="readiness id。")],
    readiness_dir: Annotated[
        Path, typer.Option("--readiness-dir", help="filtered formalization readiness root。")
    ] = readiness.DEFAULT_FILTERED_FORMALIZATION_READINESS_DIR,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="owner filtered candidate review root。")
    ] = readiness.DEFAULT_OWNER_FILTERED_CANDIDATE_REVIEW_DIR,
) -> None:
    result = readiness.build_owner_filtered_candidate_review(
        readiness_id=readiness_id, readiness_dir=readiness_dir, output_dir=output_dir
    )
    summary = _mapping_obj(result.get("owner_filtered_candidate_summary"))
    typer.echo(f"owner_review_id={result['owner_review_id']}")
    typer.echo(f"status={result['status']}")
    typer.echo(f"recommended_owner_action={summary.get('recommended_owner_action')}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_owner_filtered_candidate_review_app.command("report")
def dynamic_v3_owner_filtered_candidate_review_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    owner_review_id: Annotated[
        str | None, typer.Option("--owner-review-id", help="owner review id。")
    ] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="owner filtered candidate review root。")
    ] = readiness.DEFAULT_OWNER_FILTERED_CANDIDATE_REVIEW_DIR,
) -> None:
    payload = readiness.owner_filtered_candidate_review_report_payload(
        owner_review_id=owner_review_id, latest=latest, output_dir=output_dir
    )
    summary = _mapping_obj(payload.get("owner_filtered_candidate_summary"))
    typer.echo(f"owner_review_id={payload['owner_review_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"recommended_owner_action={summary.get('recommended_owner_action')}")
    typer.echo(f"report_path={payload['owner_filtered_candidate_review_report_path']}")


@dynamic_v3_rescue_app.command("validate-owner-filtered-candidate-review")
def dynamic_v3_validate_owner_filtered_candidate_review_command(
    owner_review_id: Annotated[str, typer.Option("--owner-review-id", help="owner review id。")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="owner filtered candidate review root。")
    ] = readiness.DEFAULT_OWNER_FILTERED_CANDIDATE_REVIEW_DIR,
) -> None:
    _echo_validation(
        readiness.validate_owner_filtered_candidate_review_artifact(
            owner_review_id=owner_review_id, output_dir=output_dir
        )
    )


@dynamic_v3_filtered_next_decision_app.command("run")
def dynamic_v3_filtered_next_decision_run_command(
    owner_review_id: Annotated[str, typer.Option("--owner-review-id", help="owner review id。")],
    owner_review_dir: Annotated[
        Path, typer.Option("--owner-review-dir", help="owner filtered candidate review root。")
    ] = readiness.DEFAULT_OWNER_FILTERED_CANDIDATE_REVIEW_DIR,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="filtered next decision root。")
    ] = readiness.DEFAULT_FILTERED_NEXT_DECISION_DIR,
) -> None:
    result = readiness.run_filtered_next_decision(
        owner_review_id=owner_review_id,
        owner_review_dir=owner_review_dir,
        output_dir=output_dir,
    )
    decision = _mapping_obj(result.get("filtered_next_decision"))
    typer.echo(f"decision_id={result['decision_id']}")
    typer.echo(f"status={result['status']}")
    typer.echo(f"decision={decision.get('decision')}")
    typer.echo("production_effect=none")


@dynamic_v3_filtered_next_decision_app.command("report")
def dynamic_v3_filtered_next_decision_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    decision_id: Annotated[str | None, typer.Option("--decision-id", help="decision id。")] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="filtered next decision root。")
    ] = readiness.DEFAULT_FILTERED_NEXT_DECISION_DIR,
) -> None:
    payload = readiness.filtered_next_decision_report_payload(
        decision_id=decision_id, latest=latest, output_dir=output_dir
    )
    decision = _mapping_obj(payload.get("filtered_next_decision"))
    typer.echo(f"decision_id={payload['decision_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"decision={decision.get('decision')}")
    typer.echo(f"report_path={payload['filtered_next_decision_report_path']}")


@dynamic_v3_rescue_app.command("validate-filtered-next-decision")
def dynamic_v3_validate_filtered_next_decision_command(
    decision_id: Annotated[str, typer.Option("--decision-id", help="decision id。")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="filtered next decision root。")
    ] = readiness.DEFAULT_FILTERED_NEXT_DECISION_DIR,
) -> None:
    _echo_validation(
        readiness.validate_filtered_next_decision_artifact(
            decision_id=decision_id, output_dir=output_dir
        )
    )
