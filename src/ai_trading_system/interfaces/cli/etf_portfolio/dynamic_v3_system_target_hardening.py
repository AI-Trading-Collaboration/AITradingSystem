from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.etf_portfolio import dynamic_v3_system_target_hardening as system_target
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_data_warning_impact_app,
    dynamic_v3_limited_consistency_app,
    dynamic_v3_limited_long_risk_app,
    dynamic_v3_rescue_app,
    dynamic_v3_research_method_hardening_app,
    dynamic_v3_selection_attribution_app,
)


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


@dynamic_v3_selection_attribution_app.command("run")
def dynamic_v3_selection_attribution_run_command(
    selection_review_id: Annotated[
        str, typer.Option("--selection-review-id", help="selection review id。")
    ],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="selection attribution artifact root。")
    ] = system_target.DEFAULT_SELECTION_ATTRIBUTION_DIR,
) -> None:
    """运行 TRADING-219 selection review reason attribution。"""
    result = system_target.run_selection_attribution(
        selection_review_id=selection_review_id, output_dir=output_dir
    )
    recommendation = result["recommendation_reason_breakdown"]
    review = result["review_required_reason_breakdown"]
    typer.echo(f"attribution_id={result['attribution_id']}")
    typer.echo(f"attribution_dir={result['attribution_dir']}")
    typer.echo(f"recommended_research_method={recommendation['recommended_research_method']}")
    typer.echo(f"decision_status={review['decision_status']}")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_selection_attribution_app.command("report")
def dynamic_v3_selection_attribution_report_command(
    latest: Annotated[
        bool, typer.Option("--latest/--no-latest", help="读取 latest selection attribution。")
    ] = False,
    attribution_id: Annotated[
        str | None, typer.Option("--attribution-id", help="selection attribution id。")
    ] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="selection attribution artifact root。")
    ] = system_target.DEFAULT_SELECTION_ATTRIBUTION_DIR,
) -> None:
    """展示 TRADING-219 selection attribution 摘要。"""
    payload = system_target.selection_attribution_report_payload(
        attribution_id=attribution_id, latest=latest, output_dir=output_dir
    )
    recommendation = _mapping(payload.get("recommendation_reason_breakdown"))
    review = _mapping(payload.get("review_required_reason_breakdown"))
    typer.echo(f"attribution_id={payload['attribution_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"recommended_research_method={recommendation.get('recommended_research_method')}")
    typer.echo(f"decision_status={review.get('decision_status')}")
    typer.echo(f"report_path={payload['selection_attribution_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-selection-attribution")
def dynamic_v3_validate_selection_attribution_command(
    attribution_id: Annotated[str, typer.Option("--attribution-id", help="attribution id。")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="selection attribution artifact root。")
    ] = system_target.DEFAULT_SELECTION_ATTRIBUTION_DIR,
) -> None:
    """校验 TRADING-219 selection attribution artifact。"""
    payload = system_target.validate_selection_attribution_artifact(
        attribution_id=attribution_id, output_dir=output_dir
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_limited_long_risk_app.command("run")
def dynamic_v3_limited_long_risk_run_command(
    backfill_id: Annotated[str, typer.Option("--backfill-id", help="backfill id。")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="limited long risk artifact root。")
    ] = system_target.DEFAULT_LIMITED_LONG_RISK_DIR,
) -> None:
    """运行 TRADING-220 limited_adjustment long-window risk review。"""
    result = system_target.run_limited_long_risk_review(
        backfill_id=backfill_id, output_dir=output_dir
    )
    long_window = result["long_window_risk_return"]
    metrics = _mapping(long_window.get("metrics"))
    typer.echo(f"risk_review_id={result['risk_review_id']}")
    typer.echo(f"risk_review_dir={result['risk_review_dir']}")
    typer.echo(f"total_return={metrics.get('total_return')}")
    typer.echo(f"max_drawdown={metrics.get('max_drawdown')}")
    typer.echo(f"turnover={metrics.get('turnover')}")
    typer.echo(f"risk_return_status={long_window['risk_return_status']}")
    typer.echo("official_target_weights_allowed=false")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_limited_long_risk_app.command("report")
def dynamic_v3_limited_long_risk_report_command(
    latest: Annotated[
        bool, typer.Option("--latest/--no-latest", help="读取 latest limited long risk。")
    ] = False,
    risk_review_id: Annotated[
        str | None, typer.Option("--risk-review-id", help="risk review id。")
    ] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="limited long risk artifact root。")
    ] = system_target.DEFAULT_LIMITED_LONG_RISK_DIR,
) -> None:
    """展示 TRADING-220 limited long risk 摘要。"""
    payload = system_target.limited_long_risk_report_payload(
        risk_review_id=risk_review_id, latest=latest, output_dir=output_dir
    )
    long_window = _mapping(payload.get("long_window_risk_return"))
    metrics = _mapping(long_window.get("metrics"))
    typer.echo(f"risk_review_id={payload['risk_review_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"total_return={metrics.get('total_return')}")
    typer.echo(f"max_drawdown={metrics.get('max_drawdown')}")
    typer.echo(f"turnover={metrics.get('turnover')}")
    typer.echo(f"risk_return_status={long_window.get('risk_return_status')}")
    typer.echo(f"report_path={payload['limited_long_risk_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-limited-long-risk")
def dynamic_v3_validate_limited_long_risk_command(
    risk_review_id: Annotated[str, typer.Option("--risk-review-id", help="risk review id。")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="limited long risk artifact root。")
    ] = system_target.DEFAULT_LIMITED_LONG_RISK_DIR,
) -> None:
    """校验 TRADING-220 limited long risk artifact。"""
    payload = system_target.validate_limited_long_risk_artifact(
        risk_review_id=risk_review_id, output_dir=output_dir
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_limited_consistency_app.command("run")
def dynamic_v3_limited_consistency_run_command(
    backfill_id: Annotated[str, typer.Option("--backfill-id", help="backfill id。")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="limited consistency artifact root。")
    ] = system_target.DEFAULT_LIMITED_CONSISTENCY_DIR,
) -> None:
    """运行 TRADING-221 limited_adjustment rolling/regime/stability consistency。"""
    result = system_target.run_limited_consistency_check(
        backfill_id=backfill_id,
        output_dir=output_dir,
    )
    rolling = result["rolling_consistency_summary"]
    regime = result["regime_consistency_summary"]
    stability = result["stability_consistency_summary"]
    typer.echo(f"consistency_id={result['consistency_id']}")
    typer.echo(f"consistency_dir={result['consistency_dir']}")
    typer.echo(f"rolling_consistency_status={rolling['rolling_consistency_status']}")
    typer.echo(f"regime_consistency_status={regime['regime_consistency_status']}")
    typer.echo(f"stability_status={stability['stability_status']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_limited_consistency_app.command("report")
def dynamic_v3_limited_consistency_report_command(
    latest: Annotated[
        bool, typer.Option("--latest/--no-latest", help="读取 latest limited consistency。")
    ] = False,
    consistency_id: Annotated[
        str | None, typer.Option("--consistency-id", help="consistency id。")
    ] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="limited consistency artifact root。")
    ] = system_target.DEFAULT_LIMITED_CONSISTENCY_DIR,
) -> None:
    """展示 TRADING-221 limited consistency 摘要。"""
    payload = system_target.limited_consistency_report_payload(
        consistency_id=consistency_id, latest=latest, output_dir=output_dir
    )
    rolling = _mapping(payload.get("rolling_consistency_summary"))
    regime = _mapping(payload.get("regime_consistency_summary"))
    stability = _mapping(payload.get("stability_consistency_summary"))
    typer.echo(f"consistency_id={payload['consistency_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"rolling_consistency_status={rolling.get('rolling_consistency_status')}")
    typer.echo(f"regime_consistency_status={regime.get('regime_consistency_status')}")
    typer.echo(f"stability_status={stability.get('stability_status')}")
    typer.echo(f"report_path={payload['limited_consistency_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-limited-consistency")
def dynamic_v3_validate_limited_consistency_command(
    consistency_id: Annotated[str, typer.Option("--consistency-id", help="consistency id。")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="limited consistency artifact root。")
    ] = system_target.DEFAULT_LIMITED_CONSISTENCY_DIR,
) -> None:
    """校验 TRADING-221 limited consistency artifact。"""
    payload = system_target.validate_limited_consistency_artifact(
        consistency_id=consistency_id, output_dir=output_dir
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_data_warning_impact_app.command("run")
def dynamic_v3_data_warning_impact_run_command(
    backfill_id: Annotated[str, typer.Option("--backfill-id", help="backfill id。")],
    selection_review_id: Annotated[
        str, typer.Option("--selection-review-id", help="selection review id。")
    ],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="data warning impact artifact root。")
    ] = system_target.DEFAULT_DATA_WARNING_IMPACT_DIR,
) -> None:
    """运行 TRADING-222 data quality warning impact review。"""
    result = system_target.run_data_warning_impact_review(
        backfill_id=backfill_id,
        selection_review_id=selection_review_id,
        output_dir=output_dir,
    )
    sensitivity = result["recommendation_sensitivity_to_warnings"]
    typer.echo(f"impact_id={result['impact_id']}")
    typer.echo(f"impact_dir={result['impact_dir']}")
    typer.echo(f"recommendation_stability={sensitivity['recommendation_stability']}")
    typer.echo(f"data_quality_decision={sensitivity['data_quality_decision']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_data_warning_impact_app.command("report")
def dynamic_v3_data_warning_impact_report_command(
    latest: Annotated[
        bool, typer.Option("--latest/--no-latest", help="读取 latest data warning impact。")
    ] = False,
    impact_id: Annotated[
        str | None, typer.Option("--impact-id", help="data warning impact id。")
    ] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="data warning impact artifact root。")
    ] = system_target.DEFAULT_DATA_WARNING_IMPACT_DIR,
) -> None:
    """展示 TRADING-222 data warning impact 摘要。"""
    payload = system_target.data_warning_impact_report_payload(
        impact_id=impact_id, latest=latest, output_dir=output_dir
    )
    inventory = _mapping(payload.get("data_warning_inventory"))
    sensitivity = _mapping(payload.get("recommendation_sensitivity_to_warnings"))
    warning_ids = [
        str(row.get("warning_id"))
        for row in inventory.get("warnings", [])
        if isinstance(row, dict) and row.get("warning_id")
    ]
    typer.echo(f"impact_id={payload['impact_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"warning_ids={','.join(warning_ids)}")
    typer.echo(f"recommendation_stability={sensitivity.get('recommendation_stability')}")
    typer.echo(f"data_quality_decision={sensitivity.get('data_quality_decision')}")
    typer.echo(f"report_path={payload['data_warning_impact_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-data-warning-impact")
def dynamic_v3_validate_data_warning_impact_command(
    impact_id: Annotated[str, typer.Option("--impact-id", help="impact id。")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="data warning impact artifact root。")
    ] = system_target.DEFAULT_DATA_WARNING_IMPACT_DIR,
) -> None:
    """校验 TRADING-222 data warning impact artifact。"""
    payload = system_target.validate_data_warning_impact_artifact(
        impact_id=impact_id, output_dir=output_dir
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_research_method_hardening_app.command("run")
def dynamic_v3_research_method_hardening_run_command(
    selection_attribution_id: Annotated[
        str, typer.Option("--selection-attribution-id", help="selection attribution id。")
    ],
    risk_review_id: Annotated[str, typer.Option("--risk-review-id", help="risk review id。")],
    consistency_id: Annotated[str, typer.Option("--consistency-id", help="consistency id。")],
    data_warning_impact_id: Annotated[
        str, typer.Option("--data-warning-impact-id", help="data warning impact id。")
    ],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="research method hardening artifact root。")
    ] = system_target.DEFAULT_RESEARCH_METHOD_HARDENING_DIR,
) -> None:
    """运行 TRADING-223 research method hardening pack。"""
    result = system_target.run_research_method_hardening_pack(
        selection_attribution_id=selection_attribution_id,
        risk_review_id=risk_review_id,
        consistency_id=consistency_id,
        data_warning_impact_id=data_warning_impact_id,
        output_dir=output_dir,
    )
    decision = result["hardening_decision"]
    typer.echo(f"hardening_id={result['hardening_id']}")
    typer.echo(f"hardening_dir={result['hardening_dir']}")
    typer.echo(f"candidate_method={decision['candidate_method']}")
    typer.echo(f"hardening_decision={decision['hardening_decision']}")
    typer.echo(f"decision_confidence={decision['decision_confidence']}")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_research_method_hardening_app.command("report")
def dynamic_v3_research_method_hardening_report_command(
    latest: Annotated[
        bool, typer.Option("--latest/--no-latest", help="读取 latest hardening pack。")
    ] = False,
    hardening_id: Annotated[
        str | None, typer.Option("--hardening-id", help="hardening id。")
    ] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="research method hardening artifact root。")
    ] = system_target.DEFAULT_RESEARCH_METHOD_HARDENING_DIR,
) -> None:
    """展示 TRADING-223 research method hardening 摘要。"""
    payload = system_target.research_method_hardening_report_payload(
        hardening_id=hardening_id, latest=latest, output_dir=output_dir
    )
    decision = _mapping(payload.get("hardening_decision_payload"))
    typer.echo(f"hardening_id={payload['hardening_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_method={decision.get('candidate_method')}")
    typer.echo(f"hardening_decision={decision.get('hardening_decision')}")
    typer.echo(f"decision_confidence={decision.get('decision_confidence')}")
    typer.echo(f"report_path={payload['research_method_hardening_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-research-method-hardening")
def dynamic_v3_validate_research_method_hardening_command(
    hardening_id: Annotated[str, typer.Option("--hardening-id", help="hardening id。")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="research method hardening artifact root。")
    ] = system_target.DEFAULT_RESEARCH_METHOD_HARDENING_DIR,
) -> None:
    """校验 TRADING-223 research method hardening artifact。"""
    payload = system_target.validate_research_method_hardening_artifact(
        hardening_id=hardening_id, output_dir=output_dir
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
