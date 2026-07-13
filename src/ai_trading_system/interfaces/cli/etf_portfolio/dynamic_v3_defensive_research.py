from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_defensive_research import (
    DEFAULT_DEFENSIVE_FAILURE_STUDY_DIR,
    DEFAULT_DEFENSIVE_HYPOTHESIS_DEEP_DIVE_DIR,
    DEFAULT_DEFENSIVE_LABEL_REVIEW_DIR,
    DEFAULT_DEFENSIVE_OWNER_PACK_DIR,
    DEFAULT_DEFENSIVE_RESEARCH_NOTE_DIR,
    defensive_failure_study_report_payload,
    defensive_hypothesis_deep_dive_report_payload,
    defensive_label_review_report_payload,
    defensive_owner_pack_report_payload,
    defensive_research_note_report_payload,
    run_defensive_failure_study,
    run_defensive_hypothesis_deep_dive,
    run_defensive_label_review,
    run_defensive_owner_pack,
    run_defensive_research_note,
    validate_defensive_failure_study_artifact,
    validate_defensive_hypothesis_deep_dive_artifact,
    validate_defensive_label_review_artifact,
    validate_defensive_owner_pack_artifact,
    validate_defensive_research_note_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_defensive_failure_study_app,
    dynamic_v3_defensive_hypothesis_deep_dive_app,
    dynamic_v3_defensive_label_review_app,
    dynamic_v3_defensive_owner_pack_app,
    dynamic_v3_defensive_research_note_app,
    dynamic_v3_rescue_app,
)


def _echo_validation_payload(payload: Mapping[str, Any]) -> None:
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_defensive_hypothesis_deep_dive_app.command("run")
def dynamic_v3_defensive_hypothesis_deep_dive_run_command(
    pressure_backfill_id: Annotated[
        str,
        typer.Option("--pressure-backfill-id", "--backfill-id", help="pressure backfill id。"),
    ],
    comparison_id: Annotated[
        str,
        typer.Option("--comparison-id", "--comparison_id", help="defensive comparison id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="defensive hypothesis deep-dive artifact root。"),
    ] = DEFAULT_DEFENSIVE_HYPOTHESIS_DEEP_DIVE_DIR,
) -> None:
    """运行 TRADING-189 defensive hypothesis deep dive。"""
    result = run_defensive_hypothesis_deep_dive(
        pressure_backfill_id=pressure_backfill_id,
        comparison_id=comparison_id,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"deep_dive_id={result['deep_dive_id']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"supporting_cases={manifest['supporting_case_count']}")
    typer.echo(f"contradicting_cases={manifest['contradicting_case_count']}")
    typer.echo("can_support_rule_approval=false")
    typer.echo("production_effect=none")


@dynamic_v3_defensive_hypothesis_deep_dive_app.command("report")
def dynamic_v3_defensive_hypothesis_deep_dive_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest defensive deep dive。"),
    ] = False,
    deep_dive_id: Annotated[
        str | None,
        typer.Option("--deep-dive-id", "--deep_dive_id", help="deep dive id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="defensive hypothesis deep-dive artifact root。"),
    ] = DEFAULT_DEFENSIVE_HYPOTHESIS_DEEP_DIVE_DIR,
) -> None:
    """展示 TRADING-189 defensive hypothesis deep dive 摘要。"""
    payload = defensive_hypothesis_deep_dive_report_payload(
        deep_dive_id=deep_dive_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"deep_dive_id={payload['deep_dive_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"supporting_cases={payload['supporting_case_count']}")
    typer.echo(f"contradicting_cases={payload['contradicting_case_count']}")
    typer.echo(f"report_path={payload['defensive_hypothesis_deep_dive_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-defensive-hypothesis-deep-dive")
def dynamic_v3_validate_defensive_hypothesis_deep_dive_command(
    deep_dive_id: Annotated[
        str,
        typer.Option("--deep-dive-id", "--deep_dive_id", help="deep dive id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="defensive hypothesis deep-dive artifact root。"),
    ] = DEFAULT_DEFENSIVE_HYPOTHESIS_DEEP_DIVE_DIR,
) -> None:
    """校验 TRADING-189 defensive hypothesis deep dive artifact。"""
    _echo_validation_payload(
        validate_defensive_hypothesis_deep_dive_artifact(
            deep_dive_id=deep_dive_id, output_dir=output_dir
        )
    )


@dynamic_v3_defensive_label_review_app.command("run")
def dynamic_v3_defensive_label_review_run_command(
    deep_dive_id: Annotated[
        str,
        typer.Option("--deep-dive-id", "--deep_dive_id", help="deep dive id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="defensive label review artifact root。"),
    ] = DEFAULT_DEFENSIVE_LABEL_REVIEW_DIR,
) -> None:
    """运行 TRADING-190 defensive label review。"""
    result = run_defensive_label_review(deep_dive_id=deep_dive_id, output_dir=output_dir)
    matrix = result["label_decision_matrix"]
    typer.echo(f"label_review_id={result['label_review_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"label_status={matrix['label_status']}")
    typer.echo(f"recommended_label={matrix['recommended_label']}")
    typer.echo("auto_rename=false")
    typer.echo("production_effect=none")


@dynamic_v3_defensive_label_review_app.command("report")
def dynamic_v3_defensive_label_review_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest defensive label review。"),
    ] = False,
    label_review_id: Annotated[
        str | None,
        typer.Option("--label-review-id", "--label_review_id", help="label review id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="defensive label review artifact root。"),
    ] = DEFAULT_DEFENSIVE_LABEL_REVIEW_DIR,
) -> None:
    """展示 TRADING-190 defensive label review 摘要。"""
    payload = defensive_label_review_report_payload(
        label_review_id=label_review_id, latest=latest, output_dir=output_dir
    )
    matrix = payload["label_decision_matrix"]
    typer.echo(f"label_review_id={payload['label_review_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"label_status={matrix['label_status']}")
    typer.echo(f"recommended_label={matrix['recommended_label']}")
    typer.echo(f"report_path={payload['defensive_label_review_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-defensive-label-review")
def dynamic_v3_validate_defensive_label_review_command(
    label_review_id: Annotated[
        str,
        typer.Option("--label-review-id", "--label_review_id", help="label review id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="defensive label review artifact root。"),
    ] = DEFAULT_DEFENSIVE_LABEL_REVIEW_DIR,
) -> None:
    """校验 TRADING-190 defensive label review artifact。"""
    _echo_validation_payload(
        validate_defensive_label_review_artifact(
            label_review_id=label_review_id, output_dir=output_dir
        )
    )


@dynamic_v3_defensive_failure_study_app.command("run")
def dynamic_v3_defensive_failure_study_run_command(
    deep_dive_id: Annotated[
        str,
        typer.Option("--deep-dive-id", "--deep_dive_id", help="deep dive id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="defensive failure study artifact root。"),
    ] = DEFAULT_DEFENSIVE_FAILURE_STUDY_DIR,
) -> None:
    """运行 TRADING-191 defensive failure study。"""
    result = run_defensive_failure_study(deep_dive_id=deep_dive_id, output_dir=output_dir)
    typer.echo(f"failure_study_id={result['failure_study_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"failure_case_count={result['manifest']['failure_case_count']}")
    typer.echo("auto_apply=false")
    typer.echo("production_effect=none")


@dynamic_v3_defensive_failure_study_app.command("report")
def dynamic_v3_defensive_failure_study_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest defensive failure study。"),
    ] = False,
    failure_study_id: Annotated[
        str | None,
        typer.Option("--failure-study-id", "--failure_study_id", help="failure study id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="defensive failure study artifact root。"),
    ] = DEFAULT_DEFENSIVE_FAILURE_STUDY_DIR,
) -> None:
    """展示 TRADING-191 defensive failure study 摘要。"""
    payload = defensive_failure_study_report_payload(
        failure_study_id=failure_study_id, latest=latest, output_dir=output_dir
    )
    typer.echo(f"failure_study_id={payload['failure_study_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failure_case_count={payload['failure_case_count']}")
    typer.echo(f"report_path={payload['defensive_failure_study_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-defensive-failure-study")
def dynamic_v3_validate_defensive_failure_study_command(
    failure_study_id: Annotated[
        str,
        typer.Option("--failure-study-id", "--failure_study_id", help="failure study id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="defensive failure study artifact root。"),
    ] = DEFAULT_DEFENSIVE_FAILURE_STUDY_DIR,
) -> None:
    """校验 TRADING-191 defensive failure study artifact。"""
    _echo_validation_payload(
        validate_defensive_failure_study_artifact(
            failure_study_id=failure_study_id, output_dir=output_dir
        )
    )


@dynamic_v3_defensive_research_note_app.command("run")
def dynamic_v3_defensive_research_note_run_command(
    deep_dive_id: Annotated[
        str,
        typer.Option("--deep-dive-id", "--deep_dive_id", help="deep dive id。"),
    ],
    label_review_id: Annotated[
        str,
        typer.Option("--label-review-id", "--label_review_id", help="label review id。"),
    ],
    failure_study_id: Annotated[
        str,
        typer.Option("--failure-study-id", "--failure_study_id", help="failure study id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="defensive research note artifact root。"),
    ] = DEFAULT_DEFENSIVE_RESEARCH_NOTE_DIR,
) -> None:
    """运行 TRADING-192 defensive research note。"""
    result = run_defensive_research_note(
        deep_dive_id=deep_dive_id,
        label_review_id=label_review_id,
        failure_study_id=failure_study_id,
        output_dir=output_dir,
    )
    summary = result["defensive_hypothesis_summary"]
    typer.echo(f"note_id={result['note_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"current_status={summary['current_status']}")
    typer.echo(f"recommended_action={summary['recommended_action']}")
    typer.echo("can_support_rule_approval=false")
    typer.echo("production_effect=none")


@dynamic_v3_defensive_research_note_app.command("report")
def dynamic_v3_defensive_research_note_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest defensive research note。"),
    ] = False,
    note_id: Annotated[
        str | None,
        typer.Option("--note-id", "--note_id", help="research note id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="defensive research note artifact root。"),
    ] = DEFAULT_DEFENSIVE_RESEARCH_NOTE_DIR,
) -> None:
    """展示 TRADING-192 defensive research note 摘要。"""
    payload = defensive_research_note_report_payload(
        note_id=note_id, latest=latest, output_dir=output_dir
    )
    summary = payload["defensive_hypothesis_summary"]
    typer.echo(f"note_id={payload['note_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"current_status={summary['current_status']}")
    typer.echo(f"recommended_action={summary['recommended_action']}")
    typer.echo(f"report_path={payload['defensive_research_note_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-defensive-research-note")
def dynamic_v3_validate_defensive_research_note_command(
    note_id: Annotated[str, typer.Option("--note-id", "--note_id", help="note id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="defensive research note artifact root。"),
    ] = DEFAULT_DEFENSIVE_RESEARCH_NOTE_DIR,
) -> None:
    """校验 TRADING-192 defensive research note artifact。"""
    _echo_validation_payload(
        validate_defensive_research_note_artifact(note_id=note_id, output_dir=output_dir)
    )


@dynamic_v3_defensive_owner_pack_app.command("run")
def dynamic_v3_defensive_owner_pack_run_command(
    note_id: Annotated[str, typer.Option("--note-id", "--note_id", help="note id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="defensive owner pack artifact root。"),
    ] = DEFAULT_DEFENSIVE_OWNER_PACK_DIR,
) -> None:
    """运行 TRADING-193 defensive owner decision pack。"""
    result = run_defensive_owner_pack(note_id=note_id, output_dir=output_dir)
    options = result["owner_decision_options"]
    typer.echo(f"pack_id={result['pack_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"auto_apply={options['auto_apply']}")
    typer.echo(f"policy_change_allowed={options['policy_change_allowed']}")
    typer.echo(f"broker_action_allowed={options['broker_action_allowed']}")
    typer.echo("production_effect=none")


@dynamic_v3_defensive_owner_pack_app.command("report")
def dynamic_v3_defensive_owner_pack_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest defensive owner pack。"),
    ] = False,
    pack_id: Annotated[
        str | None,
        typer.Option("--pack-id", "--pack_id", help="owner pack id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="defensive owner pack artifact root。"),
    ] = DEFAULT_DEFENSIVE_OWNER_PACK_DIR,
) -> None:
    """展示 TRADING-193 defensive owner pack 摘要。"""
    payload = defensive_owner_pack_report_payload(
        pack_id=pack_id, latest=latest, output_dir=output_dir
    )
    options = payload["owner_decision_options"]
    typer.echo(f"pack_id={payload['pack_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"auto_apply={options['auto_apply']}")
    typer.echo(f"policy_change_allowed={options['policy_change_allowed']}")
    typer.echo(f"report_path={payload['defensive_owner_pack_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-defensive-owner-pack")
def dynamic_v3_validate_defensive_owner_pack_command(
    pack_id: Annotated[str, typer.Option("--pack-id", "--pack_id", help="owner pack id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="defensive owner pack artifact root。"),
    ] = DEFAULT_DEFENSIVE_OWNER_PACK_DIR,
) -> None:
    """校验 TRADING-193 defensive owner pack artifact。"""
    _echo_validation_payload(
        validate_defensive_owner_pack_artifact(pack_id=pack_id, output_dir=output_dir)
    )
