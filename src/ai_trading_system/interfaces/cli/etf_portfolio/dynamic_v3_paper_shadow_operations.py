from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.etf_portfolio import (
    dynamic_v3_filtered_candidate_readiness as filtered_readiness,
)
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_daily as paper_shadow_daily
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_drift as paper_shadow_drift
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_weekly as paper_shadow_weekly
from ai_trading_system.etf_portfolio import (
    dynamic_v3_signal_input_completeness as signal_input_completeness,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import mapping_obj as _mapping_obj
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_paper_shadow_daily_app,
    dynamic_v3_paper_shadow_drift_monitor_app,
    dynamic_v3_paper_shadow_protocol_app,
    dynamic_v3_paper_shadow_weekly_review_app,
    dynamic_v3_rescue_app,
)


def _texts(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, list | tuple | set):
        return [str(item) for item in value if item not in (None, "")]
    return [str(value)] if value != "" else []


def _echo_validation_payload(payload: Mapping[str, Any]) -> None:
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_paper_shadow_protocol_app.command("build")
def dynamic_v3_paper_shadow_protocol_build_command(
    contract_id: Annotated[
        str | None,
        typer.Option("--contract-id", help="formal research method contract id；缺省读取 latest。"),
    ] = None,
    contract_dir: Annotated[
        Path,
        typer.Option("--contract-dir", help="formal research method contract root。"),
    ] = filtered_readiness.DEFAULT_FORMAL_RESEARCH_METHOD_CONTRACT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper-shadow protocol artifact root。"),
    ] = filtered_readiness.DEFAULT_PAPER_SHADOW_PROTOCOL_DIR,
) -> None:
    result = filtered_readiness.build_paper_shadow_protocol(
        contract_id=contract_id,
        contract_dir=contract_dir,
        output_dir=output_dir,
    )
    protocol = _mapping_obj(result.get("paper_shadow_protocol"))
    validation = _mapping_obj(result.get("paper_shadow_protocol_validation"))
    typer.echo(f"protocol_id={result['protocol_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"protocol_status={protocol.get('protocol_status')}")
    typer.echo(f"eligibility_status={protocol.get('eligibility_status')}")
    typer.echo(f"next_required_action={protocol.get('next_required_action')}")
    typer.echo(f"validation_status={validation.get('status')}")
    typer.echo("paper_shadow_protocol_only=true")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_paper_shadow_protocol_app.command("report")
def dynamic_v3_paper_shadow_protocol_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    protocol_id: Annotated[str | None, typer.Option("--protocol-id", help="protocol id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper-shadow protocol artifact root。"),
    ] = filtered_readiness.DEFAULT_PAPER_SHADOW_PROTOCOL_DIR,
) -> None:
    payload = filtered_readiness.paper_shadow_protocol_report_payload(
        protocol_id=protocol_id,
        latest=latest,
        output_dir=output_dir,
    )
    protocol = _mapping_obj(payload.get("paper_shadow_protocol"))
    typer.echo(f"protocol_id={payload['protocol_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"protocol_status={protocol.get('protocol_status')}")
    typer.echo(f"eligibility_status={protocol.get('eligibility_status')}")
    typer.echo(f"report_path={payload['paper_shadow_protocol_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-paper-shadow-protocol")
def dynamic_v3_validate_paper_shadow_protocol_command(
    protocol_id: Annotated[str, typer.Option("--protocol-id", help="protocol id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper-shadow protocol artifact root。"),
    ] = filtered_readiness.DEFAULT_PAPER_SHADOW_PROTOCOL_DIR,
) -> None:
    _echo_validation_payload(
        filtered_readiness.validate_paper_shadow_protocol_artifact(
            protocol_id=protocol_id,
            output_dir=output_dir,
        )
    )


@dynamic_v3_paper_shadow_daily_app.command("run")
def dynamic_v3_paper_shadow_daily_run_command(
    candidate: Annotated[str, typer.Option("--candidate", help="candidate id。")],
    observation_date: Annotated[str, typer.Option("--date", help="observation date。")],
    market_panel_artifact: Annotated[
        Path,
        typer.Option("--market-panel-artifact", help="market panel artifact path。"),
    ],
    signal_artifact: Annotated[
        Path,
        typer.Option("--signal-artifact", help="latest signal artifact path。"),
    ],
    signal_output: Annotated[str, typer.Option("--signal-output", help="signal output。")],
    hypothetical_weight_recommendation: Annotated[
        str,
        typer.Option("--hypothetical-weight-recommendation", help="paper-shadow-only weight。"),
    ],
    risk_off_risk_on_state: Annotated[
        str,
        typer.Option("--risk-state", help="risk-off / risk-on state。"),
    ],
    drawdown_state: Annotated[str, typer.Option("--drawdown-state", help="drawdown state。")],
    rotation_event: Annotated[str, typer.Option("--rotation-event", help="rotation event。")],
    mismatch_event: Annotated[str, typer.Option("--mismatch-event", help="mismatch event。")],
    benchmark_comparison: Annotated[
        str,
        typer.Option("--benchmark-comparison", help="benchmark comparison。"),
    ],
    manual_reviewer_notes: Annotated[
        str,
        typer.Option("--manual-reviewer-notes", help="manual reviewer notes。"),
    ],
    contract_id: Annotated[
        str | None,
        typer.Option("--contract-id", help="formal research method contract id；缺省 latest。"),
    ] = None,
    protocol_id: Annotated[
        str | None,
        typer.Option("--protocol-id", help="paper-shadow protocol id；缺省 latest。"),
    ] = None,
    signal_input_completeness_id: Annotated[
        str | None,
        typer.Option(
            "--signal-input-completeness-id",
            "--signal-input-monitor-id",
            help="signal input completeness monitor id；缺省读取 latest。",
        ),
    ] = None,
    signal_input_completeness_report_path: Annotated[
        Path | None,
        typer.Option(
            "--signal-input-completeness-report-path",
            help="显式 signal input completeness report JSON。",
        ),
    ] = None,
    contract_dir: Annotated[
        Path,
        typer.Option("--contract-dir", help="formal research method contract artifact root。"),
    ] = filtered_readiness.DEFAULT_FORMAL_RESEARCH_METHOD_CONTRACT_DIR,
    protocol_dir: Annotated[
        Path,
        typer.Option("--protocol-dir", help="paper-shadow protocol artifact root。"),
    ] = filtered_readiness.DEFAULT_PAPER_SHADOW_PROTOCOL_DIR,
    signal_input_completeness_dir: Annotated[
        Path,
        typer.Option(
            "--signal-input-completeness-dir",
            help="signal input completeness artifact root。",
        ),
    ] = signal_input_completeness.DEFAULT_SIGNAL_INPUT_COMPLETENESS_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper-shadow daily artifact root。"),
    ] = paper_shadow_daily.DEFAULT_PAPER_SHADOW_DAILY_DIR,
) -> None:
    result = paper_shadow_daily.run_paper_shadow_daily_observation(
        candidate=candidate,
        observation_date=observation_date,
        market_panel_artifact=market_panel_artifact,
        signal_artifact=signal_artifact,
        signal_output=signal_output,
        hypothetical_weight_recommendation=hypothetical_weight_recommendation,
        risk_off_risk_on_state=risk_off_risk_on_state,
        drawdown_state=drawdown_state,
        rotation_event=rotation_event,
        mismatch_event=mismatch_event,
        benchmark_comparison=benchmark_comparison,
        manual_reviewer_notes=manual_reviewer_notes,
        contract_id=contract_id,
        protocol_id=protocol_id,
        signal_input_completeness_id=signal_input_completeness_id,
        signal_input_completeness_report_path=signal_input_completeness_report_path,
        contract_dir=contract_dir,
        protocol_dir=protocol_dir,
        signal_input_completeness_dir=signal_input_completeness_dir,
        output_dir=output_dir,
    )
    manifest = _mapping_obj(result.get("manifest"))
    observation = _mapping_obj(result.get("paper_shadow_daily_observation"))
    validation = _mapping_obj(result.get("paper_shadow_daily_validation"))
    typer.echo(f"observation_id={manifest.get('observation_id')}")
    typer.echo(f"candidate={observation.get('candidate')}")
    typer.echo(f"observation_date={observation.get('observation_date')}")
    typer.echo(f"observation_status={observation.get('observation_status')}")
    typer.echo(f"signal_input_status={observation.get('signal_input_status')}")
    daily_review = _mapping_obj(observation.get("daily_review"))
    typer.echo(f"signal_output={daily_review.get('signal_output')}")
    typer.echo(f"validation_status={validation.get('status', 'NOT_RUN')}")
    typer.echo("paper_shadow_daily_only=true")
    typer.echo("hypothetical_weight_paper_shadow_only=true")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_paper_shadow_daily_app.command("report")
def dynamic_v3_paper_shadow_daily_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    observation_id: Annotated[
        str | None,
        typer.Option("--observation-id", help="paper-shadow daily observation id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper-shadow daily artifact root。"),
    ] = paper_shadow_daily.DEFAULT_PAPER_SHADOW_DAILY_DIR,
) -> None:
    payload = paper_shadow_daily.paper_shadow_daily_report_payload(
        observation_id=observation_id,
        latest=latest,
        output_dir=output_dir,
    )
    observation = _mapping_obj(payload.get("paper_shadow_daily_observation"))
    validation = _mapping_obj(payload.get("paper_shadow_daily_validation"))
    typer.echo(f"observation_id={payload.get('observation_id')}")
    typer.echo(f"candidate={payload.get('candidate')}")
    typer.echo(f"observation_date={payload.get('observation_date')}")
    typer.echo(f"observation_status={observation.get('observation_status')}")
    typer.echo(f"next_required_action={observation.get('next_required_action')}")
    typer.echo(f"validation_status={validation.get('status', 'NOT_RUN')}")
    typer.echo(f"report_path={payload['paper_shadow_daily_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-paper-shadow-daily")
def dynamic_v3_validate_paper_shadow_daily_command(
    observation_id: Annotated[
        str | None,
        typer.Option("--observation-id", help="paper-shadow daily observation id。"),
    ] = None,
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper-shadow daily artifact root。"),
    ] = paper_shadow_daily.DEFAULT_PAPER_SHADOW_DAILY_DIR,
) -> None:
    resolved_id = observation_id
    if latest:
        payload = paper_shadow_daily.paper_shadow_daily_report_payload(
            latest=True,
            output_dir=output_dir,
        )
        resolved_id = str(payload.get("observation_id") or "")
    if not resolved_id:
        raise typer.BadParameter("--observation-id or --latest is required")
    _echo_validation_payload(
        paper_shadow_daily.validate_paper_shadow_daily_artifact(
            observation_id=resolved_id,
            output_dir=output_dir,
        )
    )


@dynamic_v3_paper_shadow_drift_monitor_app.command("report")
def dynamic_v3_paper_shadow_drift_monitor_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest drift monitor artifact。"),
    ] = False,
    monitor_id: Annotated[
        str | None,
        typer.Option("--monitor-id", help="paper-shadow drift monitor id。"),
    ] = None,
    observation_id: Annotated[
        str | None,
        typer.Option("--observation-id", help="paper-shadow daily observation id；缺省 latest。"),
    ] = None,
    observation_dir: Annotated[
        Path,
        typer.Option("--observation-dir", help="paper-shadow daily artifact root。"),
    ] = paper_shadow_daily.DEFAULT_PAPER_SHADOW_DAILY_DIR,
    contract_id: Annotated[
        str | None,
        typer.Option(
            "--contract-id",
            help="formal research method contract id；缺省 observation source。",
        ),
    ] = None,
    contract_dir: Annotated[
        Path,
        typer.Option("--contract-dir", help="formal research method contract root。"),
    ] = filtered_readiness.DEFAULT_FORMAL_RESEARCH_METHOD_CONTRACT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper-shadow drift monitor artifact root。"),
    ] = paper_shadow_drift.DEFAULT_PAPER_SHADOW_DRIFT_MONITOR_DIR,
) -> None:
    if latest or monitor_id:
        payload = paper_shadow_drift.paper_shadow_drift_monitor_report_payload(
            monitor_id=monitor_id,
            latest=latest,
            output_dir=output_dir,
        )
        report = _mapping_obj(payload.get("paper_shadow_drift_report"))
        validation = _mapping_obj(payload.get("paper_shadow_drift_validation"))
        manifest = _mapping_obj(payload)
    else:
        result = paper_shadow_drift.build_paper_shadow_drift_monitor_report(
            observation_id=observation_id,
            observation_dir=observation_dir,
            contract_id=contract_id,
            contract_dir=contract_dir,
            output_dir=output_dir,
        )
        report = _mapping_obj(result.get("paper_shadow_drift_report"))
        validation = _mapping_obj(result.get("paper_shadow_drift_validation"))
        manifest = _mapping_obj(result.get("manifest"))
    typer.echo(f"monitor_id={manifest.get('monitor_id')}")
    typer.echo(f"candidate={report.get('candidate')}")
    typer.echo(f"observation_id={report.get('observation_id')}")
    typer.echo(f"drift_severity={report.get('drift_severity')}")
    typer.echo(f"blocking_count={report.get('blocking_count')}")
    typer.echo(f"warning_count={report.get('warning_count')}")
    typer.echo(f"next_action={report.get('next_action')}")
    typer.echo(f"validation_status={validation.get('status', 'NOT_RUN')}")
    typer.echo(f"report_path={manifest.get('paper_shadow_drift_markdown_path')}")
    typer.echo("paper_shadow_drift_monitor_only=true")
    typer.echo("read_only_monitor=true")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-paper-shadow-drift-monitor")
def dynamic_v3_validate_paper_shadow_drift_monitor_command(
    monitor_id: Annotated[
        str | None,
        typer.Option("--monitor-id", help="paper-shadow drift monitor id。"),
    ] = None,
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper-shadow drift monitor artifact root。"),
    ] = paper_shadow_drift.DEFAULT_PAPER_SHADOW_DRIFT_MONITOR_DIR,
) -> None:
    resolved_id = monitor_id
    if latest:
        payload = paper_shadow_drift.paper_shadow_drift_monitor_report_payload(
            latest=True,
            output_dir=output_dir,
        )
        resolved_id = str(payload.get("monitor_id") or "")
    if not resolved_id:
        raise typer.BadParameter("--monitor-id or --latest is required")
    _echo_validation_payload(
        paper_shadow_drift.validate_paper_shadow_drift_monitor_artifact(
            monitor_id=resolved_id,
            output_dir=output_dir,
        )
    )


@dynamic_v3_paper_shadow_weekly_review_app.command("build")
def dynamic_v3_paper_shadow_weekly_review_build_command(
    candidate: Annotated[
        str,
        typer.Option("--candidate", help="filtered candidate id。"),
    ] = filtered_readiness.TOP_FILTERED_CANDIDATE,
    week_start: Annotated[
        str,
        typer.Option("--week-start", help="weekly review start date YYYY-MM-DD。"),
    ] = ...,
    week_end: Annotated[
        str,
        typer.Option("--week-end", help="weekly review end date YYYY-MM-DD。"),
    ] = ...,
    daily_observation_id: Annotated[
        list[str] | None,
        typer.Option(
            "--daily-observation-id",
            help="paper-shadow daily observation id；可重复；缺省读取 latest。",
        ),
    ] = None,
    drift_monitor_id: Annotated[
        list[str] | None,
        typer.Option(
            "--drift-monitor-id",
            help="paper-shadow drift monitor id；可重复；缺省读取 latest。",
        ),
    ] = None,
    contract_id: Annotated[
        str | None,
        typer.Option(
            "--contract-id",
            help="formal research method contract id；缺省读取 daily source。",
        ),
    ] = None,
    ledger_run_id: Annotated[
        str | None,
        typer.Option("--ledger-run-id", help="candidate decision ledger run id；缺省 latest。"),
    ] = None,
    signal_input_completeness_id: Annotated[
        str | None,
        typer.Option(
            "--signal-input-completeness-id",
            "--signal-input-monitor-id",
            help="signal input completeness monitor id；缺省读取 latest。",
        ),
    ] = None,
    signal_input_completeness_report_path: Annotated[
        Path | None,
        typer.Option(
            "--signal-input-completeness-report-path",
            help="显式 signal input completeness report JSON。",
        ),
    ] = None,
    observation_dir: Annotated[
        Path,
        typer.Option("--observation-dir", help="paper-shadow daily artifact root。"),
    ] = paper_shadow_daily.DEFAULT_PAPER_SHADOW_DAILY_DIR,
    drift_dir: Annotated[
        Path,
        typer.Option("--drift-dir", help="paper-shadow drift monitor artifact root。"),
    ] = paper_shadow_drift.DEFAULT_PAPER_SHADOW_DRIFT_MONITOR_DIR,
    contract_dir: Annotated[
        Path,
        typer.Option("--contract-dir", help="formal research method contract root。"),
    ] = filtered_readiness.DEFAULT_FORMAL_RESEARCH_METHOD_CONTRACT_DIR,
    ledger_dir: Annotated[
        Path,
        typer.Option("--ledger-dir", help="candidate decision ledger artifact root。"),
    ] = filtered_readiness.DEFAULT_CANDIDATE_DECISION_LEDGER_DIR,
    signal_input_completeness_dir: Annotated[
        Path,
        typer.Option(
            "--signal-input-completeness-dir",
            help="signal input completeness artifact root。",
        ),
    ] = signal_input_completeness.DEFAULT_SIGNAL_INPUT_COMPLETENESS_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper-shadow weekly review artifact root。"),
    ] = paper_shadow_weekly.DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR,
    manual_coverage_override: Annotated[
        bool,
        typer.Option(
            "--manual-coverage-override/--no-manual-coverage-override",
            help="显式允许 partial/recovery coverage 继续，仅限人工复核留痕。",
        ),
    ] = False,
    manual_coverage_override_reason: Annotated[
        str,
        typer.Option(
            "--manual-coverage-override-reason",
            help="manual coverage override 的原因；启用 override 时必填。",
        ),
    ] = "",
) -> None:
    result = paper_shadow_weekly.build_paper_shadow_weekly_review(
        candidate=candidate,
        week_start=week_start,
        week_end=week_end,
        daily_observation_ids=daily_observation_id,
        drift_monitor_ids=drift_monitor_id,
        contract_id=contract_id,
        ledger_run_id=ledger_run_id,
        signal_input_completeness_id=signal_input_completeness_id,
        signal_input_completeness_report_path=signal_input_completeness_report_path,
        observation_dir=observation_dir,
        drift_dir=drift_dir,
        contract_dir=contract_dir,
        ledger_dir=ledger_dir,
        signal_input_completeness_dir=signal_input_completeness_dir,
        output_dir=output_dir,
        manual_coverage_override=manual_coverage_override,
        manual_coverage_override_reason=manual_coverage_override_reason,
    )
    review = _mapping_obj(result.get("paper_shadow_weekly_review"))
    summary = _mapping_obj(review.get("summary"))
    validation = _mapping_obj(result.get("paper_shadow_weekly_validation"))
    typer.echo(f"weekly_review_id={result['weekly_review_id']}")
    typer.echo(f"candidate={review.get('candidate')}")
    typer.echo(f"week_start={review.get('week_start')}")
    typer.echo(f"week_end={review.get('week_end')}")
    typer.echo(f"weekly_decision={review.get('weekly_decision')}")
    typer.echo(f"signal_input_status={review.get('signal_input_status')}")
    typer.echo(f"coverage_classification={review.get('coverage_classification')}")
    typer.echo(f"coverage_safe_for_continuation={review.get('coverage_safe_for_continuation')}")
    typer.echo(f"coverage_status={review.get('coverage_status')}")
    typer.echo(f"coverage_ratio={review.get('coverage_ratio')}")
    typer.echo(
        f"missing_input_artifacts={','.join(_texts(summary.get('missing_input_artifacts')))}"
    )
    typer.echo(f"validation_status={validation.get('status')}")
    typer.echo("paper_shadow_weekly_review_only=true")
    typer.echo("read_only_review=true")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_paper_shadow_weekly_review_app.command("report")
def dynamic_v3_paper_shadow_weekly_review_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    weekly_review_id: Annotated[
        str | None,
        typer.Option("--weekly-review-id", help="paper-shadow weekly review id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper-shadow weekly review artifact root。"),
    ] = paper_shadow_weekly.DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR,
) -> None:
    if not latest and not weekly_review_id:
        raise typer.BadParameter("--weekly-review-id or --latest is required")
    payload = paper_shadow_weekly.paper_shadow_weekly_review_report_payload(
        weekly_review_id=weekly_review_id,
        latest=latest,
        output_dir=output_dir,
    )
    review = _mapping_obj(payload.get("paper_shadow_weekly_review"))
    summary = _mapping_obj(review.get("summary"))
    validation = _mapping_obj(payload.get("paper_shadow_weekly_validation"))
    typer.echo(f"weekly_review_id={payload.get('weekly_review_id')}")
    typer.echo(f"candidate={review.get('candidate')}")
    typer.echo(f"week_start={review.get('week_start')}")
    typer.echo(f"week_end={review.get('week_end')}")
    typer.echo(f"weekly_decision={review.get('weekly_decision')}")
    typer.echo(f"signal_input_status={review.get('signal_input_status')}")
    typer.echo(f"coverage_classification={review.get('coverage_classification')}")
    typer.echo(f"coverage_safe_for_continuation={review.get('coverage_safe_for_continuation')}")
    typer.echo(f"coverage_status={review.get('coverage_status')}")
    typer.echo(f"coverage_ratio={review.get('coverage_ratio')}")
    typer.echo(
        f"missing_input_artifacts={','.join(_texts(summary.get('missing_input_artifacts')))}"
    )
    typer.echo(f"validation_status={validation.get('status', 'NOT_RUN')}")
    typer.echo(f"report_path={payload['paper_shadow_weekly_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-paper-shadow-weekly-review")
def dynamic_v3_validate_paper_shadow_weekly_review_command(
    weekly_review_id: Annotated[
        str | None,
        typer.Option("--weekly-review-id", help="paper-shadow weekly review id。"),
    ] = None,
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper-shadow weekly review artifact root。"),
    ] = paper_shadow_weekly.DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR,
) -> None:
    resolved_id = weekly_review_id
    if latest:
        payload = paper_shadow_weekly.paper_shadow_weekly_review_report_payload(
            latest=True,
            output_dir=output_dir,
        )
        resolved_id = str(payload.get("weekly_review_id") or "")
    if not resolved_id:
        raise typer.BadParameter("--weekly-review-id or --latest is required")
    _echo_validation_payload(
        paper_shadow_weekly.validate_paper_shadow_weekly_review_artifact(
            weekly_review_id=resolved_id,
            output_dir=output_dir,
        )
    )
