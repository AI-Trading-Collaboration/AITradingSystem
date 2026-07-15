from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio import (
    dynamic_v3_signal_diagnosis_foundation as diagnosis_foundation,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import mapping_obj as _mapping_obj
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_consensus_quality_review_app,
    dynamic_v3_gate_calibration_review_app,
    dynamic_v3_rescue_app,
    dynamic_v3_scorecard_attribution_app,
    dynamic_v3_signal_instability_diagnosis_app,
)


@dynamic_v3_gate_calibration_review_app.command("run")
def dynamic_v3_gate_calibration_review_run_command(
    no_promotion_review_id: Annotated[
        str,
        typer.Option("--no-promotion-review-id", help="no-promotion review id。"),
    ],
    threshold_sensitivity_id: Annotated[
        str,
        typer.Option("--threshold-sensitivity-id", help="threshold sensitivity id。"),
    ],
    review_dir: Annotated[
        Path,
        typer.Option("--review-dir", help="no-promotion review root。"),
    ] = diagnosis_foundation.DEFAULT_NO_PROMOTION_REVIEW_DIR,
    sensitivity_dir: Annotated[
        Path,
        typer.Option("--sensitivity-dir", help="threshold sensitivity root。"),
    ] = diagnosis_foundation.DEFAULT_PROMOTION_THRESHOLD_SENSITIVITY_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="gate calibration root。"),
    ] = diagnosis_foundation.DEFAULT_GATE_CALIBRATION_REVIEW_DIR,
) -> None:
    result = diagnosis_foundation.run_gate_calibration_review(
        no_promotion_review_id=no_promotion_review_id,
        threshold_sensitivity_id=threshold_sensitivity_id,
        review_dir=review_dir,
        sensitivity_dir=sensitivity_dir,
        output_dir=output_dir,
    )
    diagnosis = _mapping_obj(result.get("gate_strictness_diagnosis"))
    typer.echo(f"gate_calibration_id={result['gate_calibration_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"calibrated_assessment={diagnosis.get('calibrated_assessment')}")
    typer.echo("official_gate_changed=false")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_gate_calibration_review_app.command("report")
def dynamic_v3_gate_calibration_review_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    gate_calibration_id: Annotated[
        str | None,
        typer.Option("--gate-calibration-id", help="gate calibration id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="gate calibration root。"),
    ] = diagnosis_foundation.DEFAULT_GATE_CALIBRATION_REVIEW_DIR,
) -> None:
    payload = diagnosis_foundation.gate_calibration_review_report_payload(
        gate_calibration_id=gate_calibration_id,
        latest=latest,
        output_dir=output_dir,
    )
    diagnosis = _mapping_obj(payload.get("gate_strictness_diagnosis"))
    typer.echo(f"gate_calibration_id={payload['gate_calibration_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"calibrated_assessment={diagnosis.get('calibrated_assessment')}")
    typer.echo(f"report_path={payload['gate_calibration_review_report_path']}")


@dynamic_v3_rescue_app.command("validate-gate-calibration-review")
def dynamic_v3_validate_gate_calibration_review_command(
    gate_calibration_id: Annotated[
        str,
        typer.Option("--gate-calibration-id", help="gate calibration id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="gate calibration root。"),
    ] = diagnosis_foundation.DEFAULT_GATE_CALIBRATION_REVIEW_DIR,
) -> None:
    payload = diagnosis_foundation.validate_gate_calibration_review_artifact(
        gate_calibration_id=gate_calibration_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_scorecard_attribution_app.command("run")
def dynamic_v3_scorecard_attribution_run_command(
    scorecard_id: Annotated[str, typer.Option("--scorecard-id", help="source scorecard id。")],
    v3_backfill_id: Annotated[str, typer.Option("--v3-backfill-id", help="v3 backfill id。")],
    scorecard_dir: Annotated[
        Path,
        typer.Option("--scorecard-dir", help="weight scorecard root。"),
    ] = diagnosis_foundation.DEFAULT_WEIGHT_SCORECARD_DIR,
    v3_backfill_dir: Annotated[
        Path,
        typer.Option("--v3-backfill-dir", help="targeted v3 backfill root。"),
    ] = diagnosis_foundation.DEFAULT_TARGETED_V3_BACKFILL_DIR,
    v3_matrix_dir: Annotated[
        Path,
        typer.Option("--v3-matrix-dir", help="targeted v3 matrix root。"),
    ] = diagnosis_foundation.DEFAULT_TARGETED_SEARCH_V3_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="scorecard attribution root。"),
    ] = diagnosis_foundation.DEFAULT_SCORECARD_ATTRIBUTION_DIR,
) -> None:
    result = diagnosis_foundation.run_scorecard_attribution(
        scorecard_id=scorecard_id,
        v3_backfill_id=v3_backfill_id,
        scorecard_dir=scorecard_dir,
        v3_backfill_dir=v3_backfill_dir,
        v3_matrix_dir=v3_matrix_dir,
        output_dir=output_dir,
    )
    distribution = _mapping_obj(result.get("score_component_distribution"))
    typer.echo(f"scorecard_attribution_id={result['scorecard_attribution_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"variant_count={result['manifest']['variant_count']}")
    typer.echo(
        "dominant_weak_components="
        + ",".join(str(item) for item in distribution.get("dominant_weak_components", []))
    )
    typer.echo("broker_action_allowed=false")


@dynamic_v3_scorecard_attribution_app.command("report")
def dynamic_v3_scorecard_attribution_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    scorecard_attribution_id: Annotated[
        str | None,
        typer.Option("--scorecard-attribution-id", help="scorecard attribution id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="scorecard attribution root。"),
    ] = diagnosis_foundation.DEFAULT_SCORECARD_ATTRIBUTION_DIR,
) -> None:
    payload = diagnosis_foundation.scorecard_attribution_report_payload(
        scorecard_attribution_id=scorecard_attribution_id,
        latest=latest,
        output_dir=output_dir,
    )
    distribution = _mapping_obj(payload.get("score_component_distribution"))
    typer.echo(f"scorecard_attribution_id={payload['scorecard_attribution_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(
        "dominant_weak_components="
        + ",".join(str(item) for item in distribution.get("dominant_weak_components", []))
    )
    typer.echo(f"report_path={payload['scorecard_attribution_report_path']}")


@dynamic_v3_rescue_app.command("validate-scorecard-attribution")
def dynamic_v3_validate_scorecard_attribution_command(
    scorecard_attribution_id: Annotated[
        str,
        typer.Option("--scorecard-attribution-id", help="scorecard attribution id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="scorecard attribution root。"),
    ] = diagnosis_foundation.DEFAULT_SCORECARD_ATTRIBUTION_DIR,
) -> None:
    payload = diagnosis_foundation.validate_scorecard_attribution_artifact(
        scorecard_attribution_id=scorecard_attribution_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_signal_instability_diagnosis_app.command("run")
def dynamic_v3_signal_instability_diagnosis_run_command(
    scorecard_attribution_id: Annotated[
        str,
        typer.Option("--scorecard-attribution-id", help="scorecard attribution id。"),
    ],
    attribution_dir: Annotated[
        Path,
        typer.Option("--attribution-dir", help="scorecard attribution root。"),
    ] = diagnosis_foundation.DEFAULT_SCORECARD_ATTRIBUTION_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="signal diagnosis root。"),
    ] = diagnosis_foundation.DEFAULT_SIGNAL_INSTABILITY_DIAGNOSIS_DIR,
) -> None:
    result = diagnosis_foundation.run_signal_instability_diagnosis(
        scorecard_attribution_id=scorecard_attribution_id,
        attribution_dir=attribution_dir,
        output_dir=output_dir,
    )
    summary = _mapping_obj(result.get("signal_instability_summary"))
    typer.echo(f"signal_diagnosis_id={result['signal_diagnosis_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"dominant_signal_issue={summary.get('dominant_signal_issue')}")
    typer.echo(f"requires_signal_level_fix={summary.get('requires_signal_level_fix')}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_signal_instability_diagnosis_app.command("report")
def dynamic_v3_signal_instability_diagnosis_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    signal_diagnosis_id: Annotated[
        str | None,
        typer.Option("--signal-diagnosis-id", help="signal diagnosis id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="signal diagnosis root。"),
    ] = diagnosis_foundation.DEFAULT_SIGNAL_INSTABILITY_DIAGNOSIS_DIR,
) -> None:
    payload = diagnosis_foundation.signal_instability_diagnosis_report_payload(
        signal_diagnosis_id=signal_diagnosis_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("signal_instability_summary"))
    typer.echo(f"signal_diagnosis_id={payload['signal_diagnosis_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"dominant_signal_issue={summary.get('dominant_signal_issue')}")
    typer.echo(f"report_path={payload['signal_instability_diagnosis_report_path']}")


@dynamic_v3_rescue_app.command("validate-signal-instability-diagnosis")
def dynamic_v3_validate_signal_instability_diagnosis_command(
    signal_diagnosis_id: Annotated[
        str,
        typer.Option("--signal-diagnosis-id", help="signal diagnosis id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="signal diagnosis root。"),
    ] = diagnosis_foundation.DEFAULT_SIGNAL_INSTABILITY_DIAGNOSIS_DIR,
) -> None:
    payload = diagnosis_foundation.validate_signal_instability_diagnosis_artifact(
        signal_diagnosis_id=signal_diagnosis_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_consensus_quality_review_app.command("run")
def dynamic_v3_consensus_quality_review_run_command(
    signal_diagnosis_id: Annotated[
        str,
        typer.Option("--signal-diagnosis-id", help="signal diagnosis id。"),
    ],
    signal_dir: Annotated[
        Path,
        typer.Option("--signal-dir", help="signal diagnosis root。"),
    ] = diagnosis_foundation.DEFAULT_SIGNAL_INSTABILITY_DIAGNOSIS_DIR,
    attribution_dir: Annotated[
        Path,
        typer.Option("--attribution-dir", help="scorecard attribution root。"),
    ] = diagnosis_foundation.DEFAULT_SCORECARD_ATTRIBUTION_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="consensus review root。"),
    ] = diagnosis_foundation.DEFAULT_CONSENSUS_QUALITY_REVIEW_DIR,
) -> None:
    result = diagnosis_foundation.run_consensus_quality_review(
        signal_diagnosis_id=signal_diagnosis_id,
        signal_dir=signal_dir,
        attribution_dir=attribution_dir,
        output_dir=output_dir,
    )
    failure = _mapping_obj(result.get("consensus_failure_reasons"))
    typer.echo(f"consensus_review_id={result['consensus_review_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"primary_failure_reason={failure.get('primary_failure_reason')}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_consensus_quality_review_app.command("report")
def dynamic_v3_consensus_quality_review_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    consensus_review_id: Annotated[
        str | None,
        typer.Option("--consensus-review-id", help="consensus review id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="consensus review root。"),
    ] = diagnosis_foundation.DEFAULT_CONSENSUS_QUALITY_REVIEW_DIR,
) -> None:
    payload = diagnosis_foundation.consensus_quality_review_report_payload(
        consensus_review_id=consensus_review_id,
        latest=latest,
        output_dir=output_dir,
    )
    failure = _mapping_obj(payload.get("consensus_failure_reasons"))
    typer.echo(f"consensus_review_id={payload['consensus_review_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"primary_failure_reason={failure.get('primary_failure_reason')}")
    typer.echo(f"report_path={payload['consensus_quality_review_report_path']}")


@dynamic_v3_rescue_app.command("validate-consensus-quality-review")
def dynamic_v3_validate_consensus_quality_review_command(
    consensus_review_id: Annotated[
        str,
        typer.Option("--consensus-review-id", help="consensus review id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="consensus review root。"),
    ] = diagnosis_foundation.DEFAULT_CONSENSUS_QUALITY_REVIEW_DIR,
) -> None:
    payload = diagnosis_foundation.validate_consensus_quality_review_artifact(
        consensus_review_id=consensus_review_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
