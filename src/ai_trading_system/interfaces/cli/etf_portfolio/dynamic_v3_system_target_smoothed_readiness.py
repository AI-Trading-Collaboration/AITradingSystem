from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_smoothed_readiness as system_target,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_rescue_app,
    dynamic_v3_sideways_mixed_attribution_app,
    dynamic_v3_smoothed_churn_backfill_app,
    dynamic_v3_smoothed_evidence_gap_app,
    dynamic_v3_smoothed_owner_review_update_app,
    dynamic_v3_smoothed_readiness_scorecard_app,
)


def _mapping_obj(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records_obj(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [dict(row) for row in value if isinstance(row, Mapping)]


def _status_summary(rows: Any, field: str) -> str:
    return ",".join(f"{row.get('method')}:{row.get(field)}" for row in _records_obj(rows))


@dynamic_v3_smoothed_evidence_gap_app.command("run")
def dynamic_v3_smoothed_evidence_gap_run_command(
    benefit_lag_id: Annotated[str, typer.Option("--benefit-lag-id", help="benefit lag id。")],
    regime_validation_id: Annotated[
        str,
        typer.Option("--regime-validation-id", help="regime validation id。"),
    ],
    watch_pack_id: Annotated[str, typer.Option("--watch-pack-id", help="watch pack id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed evidence gap artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_EVIDENCE_GAP_DIR,
) -> None:
    """运行 TRADING-256 smoothed evidence gap diagnosis。"""
    result = system_target.run_smoothed_evidence_gap_diagnosis(
        benefit_lag_id=benefit_lag_id,
        regime_validation_id=regime_validation_id,
        watch_pack_id=watch_pack_id,
        output_dir=output_dir,
    )
    reason = result["evidence_gap_reason_summary"]
    typer.echo(f"gap_id={result['gap_id']}")
    typer.echo(f"gap_dir={result['gap_dir']}")
    typer.echo(
        f"tradeoff_can_be_resolved_by_backfill={reason['tradeoff_can_be_resolved_by_backfill']}"
    )
    typer.echo(f"requires_forward_data={reason['requires_forward_data']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_smoothed_evidence_gap_app.command("report")
def dynamic_v3_smoothed_evidence_gap_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest evidence gap。"),
    ] = False,
    gap_id: Annotated[str | None, typer.Option("--gap-id", help="gap id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed evidence gap artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_EVIDENCE_GAP_DIR,
) -> None:
    """展示 TRADING-256 smoothed evidence gap 摘要。"""
    payload = system_target.smoothed_evidence_gap_report_payload(
        gap_id=gap_id,
        latest=latest,
        output_dir=output_dir,
    )
    reason = _mapping_obj(payload.get("evidence_gap_reason_summary"))
    typer.echo(f"gap_id={payload['gap_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(
        f"tradeoff_can_be_resolved_by_backfill={reason.get('tradeoff_can_be_resolved_by_backfill')}"
    )
    typer.echo(f"requires_forward_data={reason.get('requires_forward_data')}")
    typer.echo(f"report_path={payload['smoothed_evidence_gap_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-smoothed-evidence-gap")
def dynamic_v3_validate_smoothed_evidence_gap_command(
    gap_id: Annotated[str, typer.Option("--gap-id", help="gap id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed evidence gap artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_EVIDENCE_GAP_DIR,
) -> None:
    """校验 TRADING-256 smoothed evidence gap artifact。"""
    payload = system_target.validate_smoothed_evidence_gap_artifact(
        gap_id=gap_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_smoothed_churn_backfill_app.command("run")
def dynamic_v3_smoothed_churn_backfill_run_command(
    smoothed_backfill_id: Annotated[
        str,
        typer.Option("--smoothed-backfill-id", help="smoothed backfill id。"),
    ],
    baseline_backfill_id: Annotated[
        str,
        typer.Option("--baseline-backfill-id", help="baseline paper-shadow backfill id。"),
    ],
    risk_capped_backfill_id: Annotated[
        str,
        typer.Option("--risk-capped-backfill-id", help="risk-capped backfill id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed churn backfill artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_CHURN_BACKFILL_DIR,
) -> None:
    """运行 TRADING-257 smoothed churn / weight jump metric backfill。"""
    result = system_target.run_smoothed_churn_backfill(
        smoothed_backfill_id=smoothed_backfill_id,
        baseline_backfill_id=baseline_backfill_id,
        risk_capped_backfill_id=risk_capped_backfill_id,
        output_dir=output_dir,
    )
    summary = result["churn_reduction_summary"]
    typer.echo(f"churn_id={result['churn_id']}")
    typer.echo(f"churn_dir={result['churn_dir']}")
    typer.echo(f"best_churn_reduction_method={summary['best_churn_reduction_method']}")
    typer.echo("candidate_role_fixed=false")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_smoothed_churn_backfill_app.command("report")
def dynamic_v3_smoothed_churn_backfill_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest churn backfill。"),
    ] = False,
    churn_id: Annotated[str | None, typer.Option("--churn-id", help="churn id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed churn backfill artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_CHURN_BACKFILL_DIR,
) -> None:
    """展示 TRADING-257 smoothed churn 摘要。"""
    payload = system_target.smoothed_churn_backfill_report_payload(
        churn_id=churn_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("churn_reduction_summary"))
    typer.echo(f"churn_id={payload['churn_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"best_churn_reduction_method={summary.get('best_churn_reduction_method')}")
    typer.echo(
        "churn_reduction_statuses="
        + _status_summary(summary.get("methods"), "churn_reduction_status")
    )
    typer.echo("candidate_role_fixed=false")
    typer.echo(f"report_path={payload['smoothed_churn_backfill_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-smoothed-churn-backfill")
def dynamic_v3_validate_smoothed_churn_backfill_command(
    churn_id: Annotated[str, typer.Option("--churn-id", help="churn id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed churn backfill artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_CHURN_BACKFILL_DIR,
) -> None:
    """校验 TRADING-257 smoothed churn backfill artifact。"""
    payload = system_target.validate_smoothed_churn_backfill_artifact(
        churn_id=churn_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_sideways_mixed_attribution_app.command("run")
def dynamic_v3_sideways_mixed_attribution_run_command(
    regime_validation_id: Annotated[
        str,
        typer.Option("--regime-validation-id", help="regime validation id。"),
    ],
    churn_id: Annotated[str, typer.Option("--churn-id", help="churn id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="sideways mixed attribution artifact root。"),
    ] = system_target.DEFAULT_SIDEWAYS_MIXED_ATTRIBUTION_DIR,
) -> None:
    """运行 TRADING-258 sideways mixed attribution。"""
    result = system_target.run_sideways_mixed_attribution(
        regime_validation_id=regime_validation_id,
        churn_id=churn_id,
        output_dir=output_dir,
    )
    reason = result["sideways_mixed_reason_summary"]
    typer.echo(f"sideways_attribution_id={result['sideways_attribution_id']}")
    typer.echo(f"sideways_attribution_dir={result['sideways_attribution_dir']}")
    typer.echo(
        "sideways_statuses="
        + _status_summary(reason.get("methods"), "sideways_validation")
    )
    typer.echo(f"recommendation={reason['recommendation']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_sideways_mixed_attribution_app.command("report")
def dynamic_v3_sideways_mixed_attribution_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest sideways attribution。"),
    ] = False,
    sideways_attribution_id: Annotated[
        str | None,
        typer.Option("--sideways-attribution-id", help="sideways attribution id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="sideways mixed attribution artifact root。"),
    ] = system_target.DEFAULT_SIDEWAYS_MIXED_ATTRIBUTION_DIR,
) -> None:
    """展示 TRADING-258 sideways mixed attribution 摘要。"""
    payload = system_target.sideways_mixed_attribution_report_payload(
        sideways_attribution_id=sideways_attribution_id,
        latest=latest,
        output_dir=output_dir,
    )
    reason = _mapping_obj(payload.get("sideways_mixed_reason_summary"))
    typer.echo(f"sideways_attribution_id={payload['sideways_attribution_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(
        "sideways_statuses="
        + _status_summary(reason.get("methods"), "sideways_validation")
    )
    typer.echo(f"recommendation={reason.get('recommendation')}")
    typer.echo(f"report_path={payload['sideways_mixed_attribution_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-sideways-mixed-attribution")
def dynamic_v3_validate_sideways_mixed_attribution_command(
    sideways_attribution_id: Annotated[
        str,
        typer.Option("--sideways-attribution-id", help="sideways attribution id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="sideways mixed attribution artifact root。"),
    ] = system_target.DEFAULT_SIDEWAYS_MIXED_ATTRIBUTION_DIR,
) -> None:
    """校验 TRADING-258 sideways mixed attribution artifact。"""
    payload = system_target.validate_sideways_mixed_attribution_artifact(
        sideways_attribution_id=sideways_attribution_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_smoothed_readiness_scorecard_app.command("run")
def dynamic_v3_smoothed_readiness_scorecard_run_command(
    attribution_id: Annotated[str, typer.Option("--attribution-id", help="attribution id。")],
    benefit_lag_id: Annotated[str, typer.Option("--benefit-lag-id", help="benefit lag id。")],
    churn_id: Annotated[str, typer.Option("--churn-id", help="churn id。")],
    sideways_attribution_id: Annotated[
        str,
        typer.Option("--sideways-attribution-id", help="sideways attribution id。"),
    ],
    confirmation_id: Annotated[str, typer.Option("--confirmation-id", help="confirmation id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="readiness scorecard artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_READINESS_SCORECARD_DIR,
) -> None:
    """运行 TRADING-259 smoothed readiness scorecard。"""
    result = system_target.run_smoothed_readiness_scorecard(
        attribution_id=attribution_id,
        benefit_lag_id=benefit_lag_id,
        churn_id=churn_id,
        sideways_attribution_id=sideways_attribution_id,
        confirmation_id=confirmation_id,
        output_dir=output_dir,
    )
    decision = result["promotion_readiness_decision"]
    typer.echo(f"scorecard_id={result['scorecard_id']}")
    typer.echo(f"scorecard_dir={result['scorecard_dir']}")
    typer.echo(f"candidate_method={decision['recommended_method']}")
    typer.echo(f"decision={decision['decision']}")
    typer.echo(f"confidence={decision['confidence']}")
    typer.echo("auto_apply=false")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_smoothed_readiness_scorecard_app.command("report")
def dynamic_v3_smoothed_readiness_scorecard_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest readiness scorecard。"),
    ] = False,
    scorecard_id: Annotated[
        str | None,
        typer.Option("--scorecard-id", help="scorecard id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="readiness scorecard artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_READINESS_SCORECARD_DIR,
) -> None:
    """展示 TRADING-259 smoothed readiness scorecard 摘要。"""
    payload = system_target.smoothed_readiness_scorecard_report_payload(
        scorecard_id=scorecard_id,
        latest=latest,
        output_dir=output_dir,
    )
    decision = _mapping_obj(payload.get("promotion_readiness_decision"))
    typer.echo(f"scorecard_id={payload['scorecard_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"decision={decision.get('decision')}")
    typer.echo(f"confidence={decision.get('confidence')}")
    typer.echo(f"report_path={payload['smoothed_readiness_scorecard_report_path']}")
    typer.echo("auto_apply=false")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-smoothed-readiness-scorecard")
def dynamic_v3_validate_smoothed_readiness_scorecard_command(
    scorecard_id: Annotated[str, typer.Option("--scorecard-id", help="scorecard id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="readiness scorecard artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_READINESS_SCORECARD_DIR,
) -> None:
    """校验 TRADING-259 smoothed readiness scorecard artifact。"""
    payload = system_target.validate_smoothed_readiness_scorecard_artifact(
        scorecard_id=scorecard_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("auto_apply=false")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_smoothed_owner_review_update_app.command("run")
def dynamic_v3_smoothed_owner_review_update_run_command(
    scorecard_id: Annotated[str, typer.Option("--scorecard-id", help="scorecard id。")],
    watch_pack_id: Annotated[str, typer.Option("--watch-pack-id", help="watch pack id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed owner review update artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_OWNER_REVIEW_UPDATE_DIR,
) -> None:
    """运行 TRADING-260 smoothed owner review update。"""
    result = system_target.run_smoothed_owner_review_update(
        scorecard_id=scorecard_id,
        watch_pack_id=watch_pack_id,
        output_dir=output_dir,
    )
    options = result["smoothed_owner_decision_options"]
    typer.echo(f"owner_update_id={result['owner_update_id']}")
    typer.echo(f"owner_update_dir={result['owner_update_dir']}")
    typer.echo(f"readiness_decision={options['readiness_decision']}")
    typer.echo(f"recommended_owner_action={options['recommended_owner_action']}")
    typer.echo("auto_apply=false")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_smoothed_owner_review_update_app.command("report")
def dynamic_v3_smoothed_owner_review_update_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest owner update。"),
    ] = False,
    owner_update_id: Annotated[
        str | None,
        typer.Option("--owner-update-id", help="owner update id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed owner update artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_OWNER_REVIEW_UPDATE_DIR,
) -> None:
    """展示 TRADING-260 smoothed owner review update 摘要。"""
    payload = system_target.smoothed_owner_review_update_report_payload(
        owner_update_id=owner_update_id,
        latest=latest,
        output_dir=output_dir,
    )
    options = _mapping_obj(payload.get("smoothed_owner_decision_options"))
    typer.echo(f"owner_update_id={payload['owner_update_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"readiness_decision={options.get('readiness_decision')}")
    typer.echo(f"recommended_owner_action={options.get('recommended_owner_action')}")
    typer.echo(f"report_path={payload['smoothed_owner_review_update_report_path']}")
    typer.echo("auto_apply=false")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-smoothed-owner-review-update")
def dynamic_v3_validate_smoothed_owner_review_update_command(
    owner_update_id: Annotated[
        str,
        typer.Option("--owner-update-id", help="owner update id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed owner update artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_OWNER_REVIEW_UPDATE_DIR,
) -> None:
    """校验 TRADING-260 smoothed owner review update artifact。"""
    payload = system_target.validate_smoothed_owner_review_update_artifact(
        owner_update_id=owner_update_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("auto_apply=false")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
