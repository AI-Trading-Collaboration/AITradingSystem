from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio import (
    dynamic_v3_signal_filter_foundation as signal_filter_foundation,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    mapping_obj as _mapping_obj,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_candidate_quality_filter_design_app,
    dynamic_v3_candidate_signal_ledger_app,
    dynamic_v3_regime_mismatch_attribution_app,
    dynamic_v3_rescue_app,
    dynamic_v3_signal_churn_root_cause_app,
    dynamic_v3_signal_failure_taxonomy_app,
)


def _records_obj(value: object) -> list[dict[str, object]]:
    return (
        [dict(item) for item in value if isinstance(item, dict)] if isinstance(value, list) else []
    )


@dynamic_v3_signal_failure_taxonomy_app.command("validate")
def dynamic_v3_signal_failure_taxonomy_validate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="signal failure taxonomy config。"),
    ] = signal_filter_foundation.DEFAULT_SIGNAL_FAILURE_TAXONOMY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="signal failure taxonomy artifact root。"),
    ] = signal_filter_foundation.DEFAULT_SIGNAL_FAILURE_TAXONOMY_DIR,
) -> None:
    result = signal_filter_foundation.run_signal_failure_taxonomy_validation(
        config_path=config_path,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"taxonomy_id={result['taxonomy_id']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"failure_mode_count={manifest['failure_mode_count']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_signal_failure_taxonomy_app.command("report")
def dynamic_v3_signal_failure_taxonomy_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    taxonomy_id: Annotated[
        str | None,
        typer.Option("--taxonomy-id", help="signal failure taxonomy id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="signal failure taxonomy artifact root。"),
    ] = signal_filter_foundation.DEFAULT_SIGNAL_FAILURE_TAXONOMY_DIR,
) -> None:
    payload = signal_filter_foundation.signal_failure_taxonomy_report_payload(
        taxonomy_id=taxonomy_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"taxonomy_id={payload['taxonomy_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failure_mode_count={payload['failure_mode_count']}")
    typer.echo(f"report_path={payload['signal_failure_taxonomy_report_path']}")


@dynamic_v3_rescue_app.command("validate-signal-failure-taxonomy")
def dynamic_v3_validate_signal_failure_taxonomy_command(
    taxonomy_id: Annotated[str, typer.Option("--taxonomy-id", help="taxonomy id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="signal failure taxonomy artifact root。"),
    ] = signal_filter_foundation.DEFAULT_SIGNAL_FAILURE_TAXONOMY_DIR,
) -> None:
    payload = signal_filter_foundation.validate_signal_failure_taxonomy_artifact(
        taxonomy_id=taxonomy_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_candidate_signal_ledger_app.command("build")
def dynamic_v3_candidate_signal_ledger_build_command(
    taxonomy_id: Annotated[str, typer.Option("--taxonomy-id", help="taxonomy id。")],
    source_backfill_id: Annotated[
        str,
        typer.Option("--source-backfill-id", help="micro-search-v4 backfill id。"),
    ],
    taxonomy_dir: Annotated[
        Path,
        typer.Option("--taxonomy-dir", help="signal failure taxonomy artifact root。"),
    ] = signal_filter_foundation.DEFAULT_SIGNAL_FAILURE_TAXONOMY_DIR,
    source_backfill_dir: Annotated[
        Path,
        typer.Option("--source-backfill-dir", help="micro-search-v4 backfill root。"),
    ] = signal_filter_foundation.DEFAULT_MICRO_SEARCH_V4_BACKFILL_DIR,
    v4_design_dir: Annotated[
        Path,
        typer.Option("--v4-design-dir", help="micro-search-v4 design root。"),
    ] = signal_filter_foundation.DEFAULT_MICRO_SEARCH_V4_DESIGN_DIR,
    signal_dir: Annotated[
        Path,
        typer.Option("--signal-dir", help="signal instability diagnosis root。"),
    ] = signal_filter_foundation.DEFAULT_SIGNAL_INSTABILITY_DIAGNOSIS_DIR,
    consensus_dir: Annotated[
        Path,
        typer.Option("--consensus-dir", help="consensus quality review root。"),
    ] = signal_filter_foundation.DEFAULT_CONSENSUS_QUALITY_REVIEW_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate signal ledger artifact root。"),
    ] = signal_filter_foundation.DEFAULT_CANDIDATE_SIGNAL_LEDGER_DIR,
) -> None:
    result = signal_filter_foundation.build_candidate_signal_ledger(
        taxonomy_id=taxonomy_id,
        source_backfill_id=source_backfill_id,
        taxonomy_dir=taxonomy_dir,
        source_backfill_dir=source_backfill_dir,
        v4_design_dir=v4_design_dir,
        signal_dir=signal_dir,
        consensus_dir=consensus_dir,
        output_dir=output_dir,
    )
    summary = _mapping_obj(result.get("candidate_signal_summary"))
    typer.echo(f"ledger_id={result['ledger_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"data_quality_status={result['manifest']['data_quality_status']}")
    typer.echo(f"dominant_failure_mode={summary.get('dominant_failure_mode')}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_candidate_signal_ledger_app.command("report")
def dynamic_v3_candidate_signal_ledger_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    ledger_id: Annotated[str | None, typer.Option("--ledger-id", help="ledger id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate signal ledger artifact root。"),
    ] = signal_filter_foundation.DEFAULT_CANDIDATE_SIGNAL_LEDGER_DIR,
) -> None:
    payload = signal_filter_foundation.candidate_signal_ledger_report_payload(
        ledger_id=ledger_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("candidate_signal_summary"))
    typer.echo(f"ledger_id={payload['ledger_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"dominant_failure_mode={summary.get('dominant_failure_mode')}")
    typer.echo(f"report_path={payload['candidate_signal_ledger_report_path']}")


@dynamic_v3_rescue_app.command("validate-candidate-signal-ledger")
def dynamic_v3_validate_candidate_signal_ledger_command(
    ledger_id: Annotated[str, typer.Option("--ledger-id", help="ledger id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate signal ledger artifact root。"),
    ] = signal_filter_foundation.DEFAULT_CANDIDATE_SIGNAL_LEDGER_DIR,
) -> None:
    payload = signal_filter_foundation.validate_candidate_signal_ledger_artifact(
        ledger_id=ledger_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_signal_churn_root_cause_app.command("run")
def dynamic_v3_signal_churn_root_cause_run_command(
    ledger_id: Annotated[str, typer.Option("--ledger-id", help="candidate signal ledger id。")],
    ledger_dir: Annotated[
        Path,
        typer.Option("--ledger-dir", help="candidate signal ledger artifact root。"),
    ] = signal_filter_foundation.DEFAULT_CANDIDATE_SIGNAL_LEDGER_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="signal churn root-cause artifact root。"),
    ] = signal_filter_foundation.DEFAULT_SIGNAL_CHURN_ROOT_CAUSE_DIR,
) -> None:
    result = signal_filter_foundation.run_signal_churn_root_cause_review(
        ledger_id=ledger_id,
        ledger_dir=ledger_dir,
        output_dir=output_dir,
    )
    summary = _mapping_obj(result.get("churn_root_cause_summary"))
    typer.echo(f"root_cause_id={result['root_cause_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"dominant_root_cause={summary.get('dominant_root_cause')}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_signal_churn_root_cause_app.command("report")
def dynamic_v3_signal_churn_root_cause_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    root_cause_id: Annotated[
        str | None,
        typer.Option("--root-cause-id", help="signal churn root-cause id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="signal churn root-cause artifact root。"),
    ] = signal_filter_foundation.DEFAULT_SIGNAL_CHURN_ROOT_CAUSE_DIR,
) -> None:
    payload = signal_filter_foundation.signal_churn_root_cause_report_payload(
        root_cause_id=root_cause_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("churn_root_cause_summary"))
    typer.echo(f"root_cause_id={payload['root_cause_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"dominant_root_cause={summary.get('dominant_root_cause')}")
    typer.echo(f"report_path={payload['signal_churn_root_cause_report_path']}")


@dynamic_v3_rescue_app.command("validate-signal-churn-root-cause")
def dynamic_v3_validate_signal_churn_root_cause_command(
    root_cause_id: Annotated[str, typer.Option("--root-cause-id", help="root-cause id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="signal churn root-cause artifact root。"),
    ] = signal_filter_foundation.DEFAULT_SIGNAL_CHURN_ROOT_CAUSE_DIR,
) -> None:
    payload = signal_filter_foundation.validate_signal_churn_root_cause_artifact(
        root_cause_id=root_cause_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_regime_mismatch_attribution_app.command("run")
def dynamic_v3_regime_mismatch_attribution_run_command(
    ledger_id: Annotated[str, typer.Option("--ledger-id", help="candidate signal ledger id。")],
    ledger_dir: Annotated[
        Path,
        typer.Option("--ledger-dir", help="candidate signal ledger artifact root。"),
    ] = signal_filter_foundation.DEFAULT_CANDIDATE_SIGNAL_LEDGER_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="regime mismatch attribution artifact root。"),
    ] = signal_filter_foundation.DEFAULT_REGIME_MISMATCH_ATTRIBUTION_DIR,
) -> None:
    result = signal_filter_foundation.run_regime_mismatch_attribution(
        ledger_id=ledger_id,
        ledger_dir=ledger_dir,
        output_dir=output_dir,
    )
    summary = _mapping_obj(result.get("regime_mismatch_summary"))
    typer.echo(f"mismatch_id={result['mismatch_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"dominant_mismatch_type={summary.get('dominant_mismatch_type')}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_regime_mismatch_attribution_app.command("report")
def dynamic_v3_regime_mismatch_attribution_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    mismatch_id: Annotated[str | None, typer.Option("--mismatch-id", help="mismatch id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="regime mismatch attribution artifact root。"),
    ] = signal_filter_foundation.DEFAULT_REGIME_MISMATCH_ATTRIBUTION_DIR,
) -> None:
    payload = signal_filter_foundation.regime_mismatch_attribution_report_payload(
        mismatch_id=mismatch_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("regime_mismatch_summary"))
    typer.echo(f"mismatch_id={payload['mismatch_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"dominant_mismatch_type={summary.get('dominant_mismatch_type')}")
    typer.echo(f"report_path={payload['regime_mismatch_report_path']}")


@dynamic_v3_rescue_app.command("validate-regime-mismatch-attribution")
def dynamic_v3_validate_regime_mismatch_attribution_command(
    mismatch_id: Annotated[str, typer.Option("--mismatch-id", help="mismatch id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="regime mismatch attribution artifact root。"),
    ] = signal_filter_foundation.DEFAULT_REGIME_MISMATCH_ATTRIBUTION_DIR,
) -> None:
    payload = signal_filter_foundation.validate_regime_mismatch_attribution_artifact(
        mismatch_id=mismatch_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_candidate_quality_filter_design_app.command("run")
def dynamic_v3_candidate_quality_filter_design_run_command(
    root_cause_id: Annotated[str, typer.Option("--root-cause-id", help="root-cause id。")],
    mismatch_id: Annotated[str, typer.Option("--mismatch-id", help="mismatch id。")],
    root_cause_dir: Annotated[
        Path,
        typer.Option("--root-cause-dir", help="signal churn root-cause artifact root。"),
    ] = signal_filter_foundation.DEFAULT_SIGNAL_CHURN_ROOT_CAUSE_DIR,
    mismatch_dir: Annotated[
        Path,
        typer.Option("--mismatch-dir", help="regime mismatch attribution artifact root。"),
    ] = signal_filter_foundation.DEFAULT_REGIME_MISMATCH_ATTRIBUTION_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate quality filter design artifact root。"),
    ] = signal_filter_foundation.DEFAULT_CANDIDATE_QUALITY_FILTER_DESIGN_DIR,
) -> None:
    result = signal_filter_foundation.run_candidate_quality_filter_design(
        root_cause_id=root_cause_id,
        mismatch_id=mismatch_id,
        root_cause_dir=root_cause_dir,
        mismatch_dir=mismatch_dir,
        output_dir=output_dir,
    )
    filters = _mapping_obj(result.get("proposed_quality_filters"))
    typer.echo(f"filter_design_id={result['filter_design_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"filter_count={len(_records_obj(filters.get('filters')))}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_candidate_quality_filter_design_app.command("report")
def dynamic_v3_candidate_quality_filter_design_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    filter_design_id: Annotated[
        str | None,
        typer.Option("--filter-design-id", help="filter design id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate quality filter design artifact root。"),
    ] = signal_filter_foundation.DEFAULT_CANDIDATE_QUALITY_FILTER_DESIGN_DIR,
) -> None:
    payload = signal_filter_foundation.candidate_quality_filter_design_report_payload(
        filter_design_id=filter_design_id,
        latest=latest,
        output_dir=output_dir,
    )
    filters = _mapping_obj(payload.get("proposed_quality_filters"))
    typer.echo(f"filter_design_id={payload['filter_design_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"filter_count={len(_records_obj(filters.get('filters')))}")
    typer.echo(f"report_path={payload['candidate_quality_filter_design_report_path']}")


@dynamic_v3_rescue_app.command("validate-candidate-quality-filter-design")
def dynamic_v3_validate_candidate_quality_filter_design_command(
    filter_design_id: Annotated[str, typer.Option("--filter-design-id", help="filter design id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate quality filter design artifact root。"),
    ] = signal_filter_foundation.DEFAULT_CANDIDATE_QUALITY_FILTER_DESIGN_DIR,
) -> None:
    payload = signal_filter_foundation.validate_candidate_quality_filter_design_artifact(
        filter_design_id=filter_design_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
