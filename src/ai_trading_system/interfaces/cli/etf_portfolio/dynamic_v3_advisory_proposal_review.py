from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    DEFAULT_ADVISORY_PROPOSAL_REVIEW_DIR,
    DEFAULT_BACKTEST_SIM_CALIBRATION_DIR,
    DEFAULT_SIM_DEFENSIVE_VALIDATION_DIR,
    DEFAULT_SIM_INTERPRETATION_DIR,
    DEFAULT_SIM_RISK_RETURN_DIR,
    advisory_proposal_review_report_payload,
    run_advisory_proposal_review,
    validate_advisory_proposal_review_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_advisory_proposal_review_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_advisory_proposal_review_app.command("run")
def dynamic_v3_advisory_proposal_review_run_command(
    interpretation_id: Annotated[
        str, typer.Option("--interpretation-id", help="interpretation id。")
    ],
    risk_return_id: Annotated[
        str, typer.Option("--risk-return-id", "--risk_return_id", help="risk-return id。")
    ],
    defensive_validation_id: Annotated[
        str,
        typer.Option(
            "--defensive-validation-id",
            "--defensive_validation_id",
            help="defensive validation id。",
        ),
    ],
    calibration_id: Annotated[
        str, typer.Option("--calibration-id", help="simulation calibration pack id。")
    ],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="advisory proposal review artifact root。")
    ] = DEFAULT_ADVISORY_PROPOSAL_REVIEW_DIR,
    interpretation_dir: Annotated[
        Path, typer.Option("--interpretation-dir", help="simulation interpretation root。")
    ] = DEFAULT_SIM_INTERPRETATION_DIR,
    risk_return_dir: Annotated[
        Path, typer.Option("--risk-return-dir", help="simulation risk-return root。")
    ] = DEFAULT_SIM_RISK_RETURN_DIR,
    defensive_validation_dir: Annotated[
        Path, typer.Option("--defensive-validation-dir", help="defensive validation root。")
    ] = DEFAULT_SIM_DEFENSIVE_VALIDATION_DIR,
    calibration_dir: Annotated[
        Path, typer.Option("--calibration-dir", help="backtest simulation calibration root。")
    ] = DEFAULT_BACKTEST_SIM_CALIBRATION_DIR,
) -> None:
    """生成 TRADING-172 advisory proposal review。"""
    result = run_advisory_proposal_review(
        interpretation_id=interpretation_id,
        risk_return_id=risk_return_id,
        defensive_validation_id=defensive_validation_id,
        calibration_id=calibration_id,
        output_dir=output_dir,
        interpretation_dir=interpretation_dir,
        risk_return_dir=risk_return_dir,
        defensive_validation_dir=defensive_validation_dir,
        calibration_dir=calibration_dir,
    )
    manifest = result["manifest"]
    matrix = result["proposal_decision_matrix"]
    typer.echo(f"proposal_review_id={result['proposal_review_id']}")
    typer.echo(f"proposal_review_dir={result['proposal_review_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"proposal_count={len(matrix['proposals'])}")
    typer.echo(f"auto_apply={manifest['auto_apply']}")
    typer.echo(f"owner_approval_required={manifest['owner_approval_required']}")
    typer.echo("production_effect=none")


@dynamic_v3_advisory_proposal_review_app.command("report")
def dynamic_v3_advisory_proposal_review_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest advisory proposal review。"),
    ] = False,
    proposal_review_id: Annotated[
        str | None,
        typer.Option("--proposal-review-id", "--proposal_review_id", help="proposal review id。"),
    ] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="advisory proposal review artifact root。")
    ] = DEFAULT_ADVISORY_PROPOSAL_REVIEW_DIR,
) -> None:
    """展示 TRADING-172 proposal review 摘要。"""
    payload = advisory_proposal_review_report_payload(
        proposal_review_id=proposal_review_id, latest=latest, output_dir=output_dir
    )
    matrix = payload["proposal_decision_matrix"]
    typer.echo(f"proposal_review_id={payload['proposal_review_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"proposal_count={len(matrix['proposals'])}")
    typer.echo(f"auto_apply={payload['auto_apply']}")
    typer.echo(f"report_path={payload['advisory_proposal_review_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-advisory-proposal-review")
def dynamic_v3_validate_advisory_proposal_review_command(
    proposal_review_id: Annotated[
        str,
        typer.Option("--proposal-review-id", "--proposal_review_id", help="proposal review id。"),
    ],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="advisory proposal review artifact root。")
    ] = DEFAULT_ADVISORY_PROPOSAL_REVIEW_DIR,
) -> None:
    """校验 TRADING-172 advisory proposal review artifact。"""
    payload = validate_advisory_proposal_review_artifact(
        proposal_review_id=proposal_review_id, output_dir=output_dir
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_advisory_proposal_review_report_command",
    "dynamic_v3_advisory_proposal_review_run_command",
    "dynamic_v3_validate_advisory_proposal_review_command",
]
