# ruff: noqa: E501

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio import dynamic_v3_promotion_thresholds as thresholds
from ai_trading_system.etf_portfolio import dynamic_v3_research_contract_ledger as research
from ai_trading_system.interfaces.cli.etf_portfolio.common import mapping_obj as _mapping
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_candidate_decision_ledger_app,
    dynamic_v3_formal_research_method_contract_app,
    dynamic_v3_promotion_gate_threshold_calibration_app,
    dynamic_v3_rescue_app,
)


def _echo_validation(payload: dict[str, object]) -> None:
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_formal_research_method_contract_app.command("build")
def dynamic_v3_formal_research_method_contract_build_command(
    candidate: Annotated[
        str,
        typer.Option("--candidate", help="filtered candidate id。"),
    ] = research.TOP_FILTERED_CANDIDATE,
    evidence_id: Annotated[
        str | None,
        typer.Option("--evidence-id", help="filtered candidate evidence id；缺省读取 latest。"),
    ] = None,
    spec_id: Annotated[
        str | None,
        typer.Option("--spec-id", help="median regime filter spec id；缺省读取 latest。"),
    ] = None,
    stress_backfill_id: Annotated[
        str | None,
        typer.Option("--stress-backfill-id", help="stress backfill id；缺省读取 latest。"),
    ] = None,
    mismatch_reduction_id: Annotated[
        str | None,
        typer.Option(
            "--mismatch-reduction-id",
            help="drawdown mismatch reduction id；缺省读取 latest。",
        ),
    ] = None,
    flip_reduction_id: Annotated[
        str | None,
        typer.Option("--flip-reduction-id", help="flip rotation reduction id；缺省读取 latest。"),
    ] = None,
    ab_review_id: Annotated[
        str | None,
        typer.Option("--ab-review-id", help="filtered candidate A/B review id；缺省读取 latest。"),
    ] = None,
    confirmation_id: Annotated[
        str | None,
        typer.Option("--confirmation-id", help="signal gate confirmation id；缺省读取 latest。"),
    ] = None,
    readiness_id: Annotated[
        str | None,
        typer.Option("--readiness-id", help="formalization readiness id；缺省读取 latest。"),
    ] = None,
    owner_review_id: Annotated[
        str | None,
        typer.Option("--owner-review-id", help="owner filtered review id；缺省读取 latest。"),
    ] = None,
    next_decision_id: Annotated[
        str | None,
        typer.Option("--next-decision-id", help="filtered next decision id；缺省读取 latest。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="formal research method contract root。"),
    ] = research.DEFAULT_FORMAL_RESEARCH_METHOD_CONTRACT_DIR,
) -> None:
    required = {
        "evidence-id": evidence_id, "spec-id": spec_id,
        "stress-backfill-id": stress_backfill_id,
        "mismatch-reduction-id": mismatch_reduction_id,
        "flip-reduction-id": flip_reduction_id, "ab-review-id": ab_review_id,
        "confirmation-id": confirmation_id, "readiness-id": readiness_id,
        "owner-review-id": owner_review_id, "next-decision-id": next_decision_id,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise typer.BadParameter(f"explicit source ids required: {','.join(missing)}")
    result = research.build_formal_research_method_contract(
        candidate=candidate, evidence_id=evidence_id, spec_id=spec_id,
        stress_backfill_id=stress_backfill_id, mismatch_reduction_id=mismatch_reduction_id,
        flip_reduction_id=flip_reduction_id, ab_review_id=ab_review_id,
        confirmation_id=confirmation_id, readiness_id=readiness_id,
        owner_review_id=owner_review_id, next_decision_id=next_decision_id,
        output_dir=output_dir,
    )
    decision = _mapping(result.get("formal_research_method_decision"))
    typer.echo(f"contract_id={result['contract_id']}")
    typer.echo(f"formal_research_method_status={decision.get('formal_research_method_status')}")
    typer.echo(f"promotion_state={decision.get('promotion_state')}")
    typer.echo("production_effect=none")


@dynamic_v3_formal_research_method_contract_app.command("report")
def dynamic_v3_formal_research_method_contract_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    contract_id: Annotated[str | None, typer.Option("--contract-id", help="contract id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="formal research method contract root。"),
    ] = research.DEFAULT_FORMAL_RESEARCH_METHOD_CONTRACT_DIR,
) -> None:
    payload = research.formal_research_method_contract_report_payload(
        contract_id=contract_id, latest=latest, output_dir=output_dir
    )
    decision = _mapping(payload.get("formal_research_method_decision"))
    typer.echo(f"contract_id={payload['contract_id']}")
    typer.echo(f"formal_research_method_status={decision.get('formal_research_method_status')}")
    typer.echo(f"promotion_state={decision.get('promotion_state')}")
    typer.echo(f"report_path={payload['formal_research_method_contract_report_path']}")


@dynamic_v3_rescue_app.command("validate-research-method-contract")
def dynamic_v3_validate_formal_research_method_contract_command(
    contract_id: Annotated[str, typer.Option("--contract-id", help="contract id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="formal research method contract root。"),
    ] = research.DEFAULT_FORMAL_RESEARCH_METHOD_CONTRACT_DIR,
) -> None:
    _echo_validation(research.validate_formal_research_method_contract_artifact(
        contract_id=contract_id, output_dir=output_dir
    ))


@dynamic_v3_promotion_gate_threshold_calibration_app.command("report")
def dynamic_v3_promotion_gate_threshold_calibration_report_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="promotion gate threshold policy config。"),
    ] = thresholds.DEFAULT_PROMOTION_GATE_THRESHOLDS_CONFIG_PATH,
    contract_id: Annotated[
        str | None,
        typer.Option("--contract-id", help="formal research method contract id；缺省 latest。"),
    ] = None,
    contract_dir: Annotated[
        Path,
        typer.Option("--contract-dir", help="formal research method contract root。"),
    ] = research.DEFAULT_FORMAL_RESEARCH_METHOD_CONTRACT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="promotion threshold calibration root。"),
    ] = thresholds.DEFAULT_PROMOTION_GATE_THRESHOLD_CALIBRATION_DIR,
) -> None:
    if not contract_id:
        raise typer.BadParameter("explicit --contract-id is required")
    result = thresholds.build_promotion_gate_threshold_calibration_report(
        config_path=config_path, contract_id=contract_id, contract_dir=contract_dir,
        output_dir=output_dir,
    )
    report = _mapping(result.get("report"))
    typer.echo(f"calibration_id={result['calibration_id']}")
    typer.echo(f"status={report.get('status')}")
    typer.echo(f"current_threshold_interpretation={report.get('current_threshold_interpretation')}")
    typer.echo("threshold_policy_only=true")
    typer.echo("production_effect=none")


@dynamic_v3_promotion_gate_threshold_calibration_app.command("validate")
def dynamic_v3_promotion_gate_threshold_calibration_validate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="promotion gate threshold policy config。"),
    ] = thresholds.DEFAULT_PROMOTION_GATE_THRESHOLDS_CONFIG_PATH,
    calibration_id: Annotated[
        str | None,
        typer.Option("--calibration-id", help="promotion threshold calibration id。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="校验 latest artifact。"),
    ] = False,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="promotion threshold calibration root。"),
    ] = thresholds.DEFAULT_PROMOTION_GATE_THRESHOLD_CALIBRATION_DIR,
) -> None:
    if calibration_id or latest:
        resolved = calibration_id
        if latest:
            resolved = str(thresholds.promotion_gate_threshold_calibration_report_payload(
                latest=True, output_dir=output_dir
            ).get("calibration_id") or "")
        if not resolved:
            raise typer.BadParameter("--calibration-id or --latest is required")
        _echo_validation(thresholds.validate_promotion_gate_threshold_calibration_artifact(
            calibration_id=resolved, output_dir=output_dir
        ))
        return
    _echo_validation(thresholds.validate_promotion_gate_threshold_policy(config_path=config_path))


@dynamic_v3_candidate_decision_ledger_app.command("record")
def dynamic_v3_candidate_decision_ledger_record_command(
    candidate: Annotated[
        str,
        typer.Option("--candidate", help="filtered candidate id。"),
    ] = research.TOP_FILTERED_CANDIDATE,
    evidence_id: Annotated[
        str | None,
        typer.Option("--evidence-id", help="filtered candidate evidence id；缺省读取 latest。"),
    ] = None,
    stress_backfill_id: Annotated[
        str | None,
        typer.Option("--stress-backfill-id", help="stress backfill id；缺省读取 latest。"),
    ] = None,
    mismatch_reduction_id: Annotated[
        str | None,
        typer.Option(
            "--mismatch-reduction-id",
            help="drawdown mismatch reduction id；缺省读取 latest。",
        ),
    ] = None,
    flip_reduction_id: Annotated[
        str | None,
        typer.Option("--flip-reduction-id", help="flip rotation reduction id；缺省读取 latest。"),
    ] = None,
    ab_review_id: Annotated[
        str | None,
        typer.Option("--ab-review-id", help="filtered candidate A/B review id；缺省读取 latest。"),
    ] = None,
    confirmation_id: Annotated[
        str | None,
        typer.Option("--confirmation-id", help="signal gate confirmation id；缺省读取 latest。"),
    ] = None,
    owner_review_id: Annotated[
        str | None,
        typer.Option("--owner-review-id", help="owner filtered review id；缺省读取 latest。"),
    ] = None,
    next_decision_id: Annotated[
        str | None,
        typer.Option("--next-decision-id", help="filtered next decision id；缺省读取 latest。"),
    ] = None,
    contract_id: Annotated[
        str | None,
        typer.Option("--contract-id", help="formal research method contract id；缺省读取 latest。"),
    ] = None,
    protocol_id: Annotated[
        str | None,
        typer.Option("--protocol-id", help="paper-shadow protocol id；缺省读取 latest。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate decision ledger artifact root。"),
    ] = research.DEFAULT_CANDIDATE_DECISION_LEDGER_DIR,
) -> None:
    if not contract_id:
        raise typer.BadParameter("explicit --contract-id is required")
    result = research.record_candidate_decision_ledger(
        candidate=candidate,
        evidence_id=evidence_id,
        stress_backfill_id=stress_backfill_id,
        mismatch_reduction_id=mismatch_reduction_id,
        flip_reduction_id=flip_reduction_id,
        ab_review_id=ab_review_id,
        confirmation_id=confirmation_id,
        owner_review_id=owner_review_id,
        next_decision_id=next_decision_id,
        contract_id=contract_id,
        protocol_id=protocol_id,
        output_dir=output_dir,
    )
    record = _mapping(result.get("candidate_decision_record"))
    typer.echo(f"ledger_run_id={result['ledger_run_id']}")
    typer.echo(f"record_id={result['record_id']}")
    typer.echo(f"final_decision={record.get('final_decision')}")
    typer.echo(f"eb5_protocol_status={record.get('eb5_protocol_status')}")
    typer.echo("production_effect=none")


@dynamic_v3_candidate_decision_ledger_app.command("report")
def dynamic_v3_candidate_decision_ledger_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    ledger_run_id: Annotated[
        str | None,
        typer.Option("--ledger-run-id", help="ledger run id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate decision ledger artifact root。"),
    ] = research.DEFAULT_CANDIDATE_DECISION_LEDGER_DIR,
) -> None:
    payload = research.candidate_decision_ledger_report_payload(
        ledger_run_id=ledger_run_id, latest=latest, output_dir=output_dir
    )
    record = _mapping(payload.get("candidate_decision_record"))
    typer.echo(f"ledger_run_id={payload['ledger_run_id']}")
    typer.echo(f"record_id={payload['record_id']}")
    typer.echo(f"final_decision={record.get('final_decision')}")
    typer.echo(f"report_path={payload['candidate_decision_ledger_report_path']}")


@dynamic_v3_rescue_app.command("validate-candidate-decision-ledger")
def dynamic_v3_validate_candidate_decision_ledger_command(
    ledger_run_id: Annotated[str, typer.Option("--ledger-run-id", help="ledger run id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate decision ledger artifact root。"),
    ] = research.DEFAULT_CANDIDATE_DECISION_LEDGER_DIR,
) -> None:
    _echo_validation(research.validate_candidate_decision_ledger_artifact(
        ledger_run_id=ledger_run_id, output_dir=output_dir
    ))
