from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.etf_portfolio import dynamic_v3_micro_search_foundation as micro
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_gate_calibrated_review_app,
    dynamic_v3_micro_search_v4_backfill_app,
    dynamic_v3_micro_search_v4_design_app,
    dynamic_v3_rescue_app,
    dynamic_v3_signal_vs_parameter_attribution_app,
)


def _mapping_obj(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


@dynamic_v3_micro_search_v4_design_app.command("run")
def dynamic_v3_micro_search_v4_design_run_command(
    gate_calibration_id: Annotated[
        str, typer.Option("--gate-calibration-id", help="gate calibration id。")
    ],
    scorecard_attribution_id: Annotated[
        str, typer.Option("--scorecard-attribution-id", help="scorecard attribution id。")
    ],
    signal_diagnosis_id: Annotated[
        str, typer.Option("--signal-diagnosis-id", help="signal diagnosis id。")
    ],
    consensus_review_id: Annotated[
        str, typer.Option("--consensus-review-id", help="consensus review id。")
    ],
    gate_calibration_dir: Annotated[
        Path, typer.Option("--gate-calibration-dir", help="gate calibration root。")
    ] = micro.DEFAULT_GATE_CALIBRATION_REVIEW_DIR,
    attribution_dir: Annotated[
        Path, typer.Option("--attribution-dir", help="scorecard attribution root。")
    ] = micro.DEFAULT_SCORECARD_ATTRIBUTION_DIR,
    signal_dir: Annotated[
        Path, typer.Option("--signal-dir", help="signal diagnosis root。")
    ] = micro.DEFAULT_SIGNAL_INSTABILITY_DIAGNOSIS_DIR,
    consensus_dir: Annotated[
        Path, typer.Option("--consensus-dir", help="consensus review root。")
    ] = micro.DEFAULT_CONSENSUS_QUALITY_REVIEW_DIR,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="micro search v4 design root。")
    ] = micro.DEFAULT_MICRO_SEARCH_V4_DESIGN_DIR,
) -> None:
    result = micro.run_micro_search_v4_design(
        gate_calibration_id=gate_calibration_id,
        scorecard_attribution_id=scorecard_attribution_id,
        signal_diagnosis_id=signal_diagnosis_id,
        consensus_review_id=consensus_review_id,
        gate_calibration_dir=gate_calibration_dir,
        attribution_dir=attribution_dir,
        signal_dir=signal_dir,
        consensus_dir=consensus_dir,
        output_dir=output_dir,
    )
    typer.echo(f"v4_design_id={result['v4_design_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"variant_count={result['manifest']['variant_count']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_micro_search_v4_design_app.command("report")
def dynamic_v3_micro_search_v4_design_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    v4_design_id: Annotated[
        str | None, typer.Option("--v4-design-id", help="v4 design id。")
    ] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="micro search v4 design root。")
    ] = micro.DEFAULT_MICRO_SEARCH_V4_DESIGN_DIR,
) -> None:
    payload = micro.micro_search_v4_design_report_payload(
        v4_design_id=v4_design_id, latest=latest, output_dir=output_dir
    )
    typer.echo(f"v4_design_id={payload['v4_design_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"variant_count={payload['variant_count']}")
    typer.echo(f"report_path={payload['micro_search_v4_design_report_path']}")


@dynamic_v3_rescue_app.command("validate-micro-search-v4-design")
def dynamic_v3_validate_micro_search_v4_design_command(
    v4_design_id: Annotated[str, typer.Option("--v4-design-id", help="v4 design id。")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="micro search v4 design root。")
    ] = micro.DEFAULT_MICRO_SEARCH_V4_DESIGN_DIR,
) -> None:
    payload = micro.validate_micro_search_v4_design_artifact(
        v4_design_id=v4_design_id, output_dir=output_dir
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_micro_search_v4_backfill_app.command("run")
def dynamic_v3_micro_search_v4_backfill_run_command(
    v4_design_id: Annotated[str, typer.Option("--v4-design-id", help="v4 design id。")],
    v4_design_dir: Annotated[
        Path, typer.Option("--v4-design-dir", help="micro search v4 design root。")
    ] = micro.DEFAULT_MICRO_SEARCH_V4_DESIGN_DIR,
    baseline_backfill_dir: Annotated[
        Path, typer.Option("--baseline-backfill-dir", help="paper shadow backfill root。")
    ] = st.DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="micro search v4 backfill root。")
    ] = micro.DEFAULT_MICRO_SEARCH_V4_BACKFILL_DIR,
    price_cache_path: Annotated[
        Path | None, typer.Option("--price-cache-path", help="price cache override。")
    ] = None,
    rates_cache_path: Annotated[
        Path, typer.Option("--rates-cache-path", help="rates cache path。")
    ] = st.DEFAULT_RATES_CACHE_PATH,
) -> None:
    result = micro.run_micro_search_v4_backfill(
        v4_design_id=v4_design_id,
        v4_design_dir=v4_design_dir,
        baseline_backfill_dir=baseline_backfill_dir,
        output_dir=output_dir,
        price_cache_path=price_cache_path,
        rates_cache_path=rates_cache_path,
    )
    manifest = result["manifest"]
    typer.echo(f"v4_backfill_id={result['v4_backfill_id']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"data_quality_status={manifest['data_quality_status']}")
    typer.echo(f"variants_completed={manifest['variants_completed']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_micro_search_v4_backfill_app.command("report")
def dynamic_v3_micro_search_v4_backfill_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    v4_backfill_id: Annotated[
        str | None, typer.Option("--v4-backfill-id", help="v4 backfill id。")
    ] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="micro search v4 backfill root。")
    ] = micro.DEFAULT_MICRO_SEARCH_V4_BACKFILL_DIR,
) -> None:
    payload = micro.micro_search_v4_backfill_report_payload(
        v4_backfill_id=v4_backfill_id, latest=latest, output_dir=output_dir
    )
    typer.echo(f"v4_backfill_id={payload['v4_backfill_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"data_quality_status={payload['data_quality_status']}")
    typer.echo(f"report_path={payload['micro_search_v4_backfill_report_path']}")


@dynamic_v3_rescue_app.command("validate-micro-search-v4-backfill")
def dynamic_v3_validate_micro_search_v4_backfill_command(
    v4_backfill_id: Annotated[
        str, typer.Option("--v4-backfill-id", help="v4 backfill id。")
    ],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="micro search v4 backfill root。")
    ] = micro.DEFAULT_MICRO_SEARCH_V4_BACKFILL_DIR,
) -> None:
    payload = micro.validate_micro_search_v4_backfill_artifact(
        v4_backfill_id=v4_backfill_id, output_dir=output_dir
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_gate_calibrated_review_app.command("run")
def dynamic_v3_gate_calibrated_review_run_command(
    v4_backfill_id: Annotated[
        str, typer.Option("--v4-backfill-id", help="v4 backfill id。")
    ],
    gate_calibration_id: Annotated[
        str, typer.Option("--gate-calibration-id", help="gate calibration id。")
    ],
    v4_backfill_dir: Annotated[
        Path, typer.Option("--v4-backfill-dir", help="micro search v4 backfill root。")
    ] = micro.DEFAULT_MICRO_SEARCH_V4_BACKFILL_DIR,
    v4_design_dir: Annotated[
        Path, typer.Option("--v4-design-dir", help="micro search v4 design root。")
    ] = micro.DEFAULT_MICRO_SEARCH_V4_DESIGN_DIR,
    gate_calibration_dir: Annotated[
        Path, typer.Option("--gate-calibration-dir", help="gate calibration root。")
    ] = micro.DEFAULT_GATE_CALIBRATION_REVIEW_DIR,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="gate calibrated review root。")
    ] = micro.DEFAULT_GATE_CALIBRATED_REVIEW_DIR,
) -> None:
    result = micro.run_gate_calibrated_review(
        v4_backfill_id=v4_backfill_id,
        gate_calibration_id=gate_calibration_id,
        v4_backfill_dir=v4_backfill_dir,
        v4_design_dir=v4_design_dir,
        gate_calibration_dir=gate_calibration_dir,
        output_dir=output_dir,
    )
    summary = _mapping_obj(result.get("gate_calibrated_summary"))
    typer.echo(f"gate_review_id={result['gate_review_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"official_gate_promoted_count={summary.get('official_gate_promoted_count')}")
    typer.echo(f"diagnostic_gate_promoted_count={summary.get('diagnostic_gate_promoted_count')}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_gate_calibrated_review_app.command("report")
def dynamic_v3_gate_calibrated_review_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    gate_review_id: Annotated[
        str | None, typer.Option("--gate-review-id", help="gate review id。")
    ] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="gate calibrated review root。")
    ] = micro.DEFAULT_GATE_CALIBRATED_REVIEW_DIR,
) -> None:
    payload = micro.gate_calibrated_review_report_payload(
        gate_review_id=gate_review_id, latest=latest, output_dir=output_dir
    )
    summary = _mapping_obj(payload.get("gate_calibrated_summary"))
    typer.echo(f"gate_review_id={payload['gate_review_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"official_gate_promoted_count={summary.get('official_gate_promoted_count')}")
    typer.echo(f"report_path={payload['gate_calibrated_review_report_path']}")


@dynamic_v3_rescue_app.command("validate-gate-calibrated-review")
def dynamic_v3_validate_gate_calibrated_review_command(
    gate_review_id: Annotated[
        str, typer.Option("--gate-review-id", help="gate review id。")
    ],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="gate calibrated review root。")
    ] = micro.DEFAULT_GATE_CALIBRATED_REVIEW_DIR,
) -> None:
    payload = micro.validate_gate_calibrated_review_artifact(
        gate_review_id=gate_review_id, output_dir=output_dir
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_signal_vs_parameter_attribution_app.command("run")
def dynamic_v3_signal_vs_parameter_attribution_run_command(
    signal_diagnosis_id: Annotated[
        str, typer.Option("--signal-diagnosis-id", help="signal diagnosis id。")
    ],
    consensus_review_id: Annotated[
        str, typer.Option("--consensus-review-id", help="consensus review id。")
    ],
    gate_review_id: Annotated[
        str, typer.Option("--gate-review-id", help="gate review id。")
    ],
    signal_dir: Annotated[
        Path, typer.Option("--signal-dir", help="signal diagnosis root。")
    ] = micro.DEFAULT_SIGNAL_INSTABILITY_DIAGNOSIS_DIR,
    consensus_dir: Annotated[
        Path, typer.Option("--consensus-dir", help="consensus review root。")
    ] = micro.DEFAULT_CONSENSUS_QUALITY_REVIEW_DIR,
    gate_review_dir: Annotated[
        Path, typer.Option("--gate-review-dir", help="gate calibrated review root。")
    ] = micro.DEFAULT_GATE_CALIBRATED_REVIEW_DIR,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="signal-vs-parameter attribution root。")
    ] = micro.DEFAULT_SIGNAL_VS_PARAMETER_ATTRIBUTION_DIR,
) -> None:
    result = micro.run_signal_vs_parameter_attribution(
        signal_diagnosis_id=signal_diagnosis_id,
        consensus_review_id=consensus_review_id,
        gate_review_id=gate_review_id,
        signal_dir=signal_dir,
        consensus_dir=consensus_dir,
        gate_review_dir=gate_review_dir,
        output_dir=output_dir,
    )
    failure = _mapping_obj(result.get("failure_source_attribution"))
    typer.echo(f"attribution_id={result['signal_vs_parameter_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"failure_source={failure.get('failure_source')}")
    typer.echo(f"confidence={failure.get('confidence')}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_signal_vs_parameter_attribution_app.command("report")
def dynamic_v3_signal_vs_parameter_attribution_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    attribution_id: Annotated[
        str | None, typer.Option("--attribution-id", help="signal-vs-parameter attribution id。")
    ] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="signal-vs-parameter attribution root。")
    ] = micro.DEFAULT_SIGNAL_VS_PARAMETER_ATTRIBUTION_DIR,
) -> None:
    payload = micro.signal_vs_parameter_attribution_report_payload(
        attribution_id=attribution_id, latest=latest, output_dir=output_dir
    )
    failure = _mapping_obj(payload.get("failure_source_attribution"))
    typer.echo(f"attribution_id={payload['attribution_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failure_source={failure.get('failure_source')}")
    typer.echo(f"report_path={payload['signal_vs_parameter_attribution_report_path']}")


@dynamic_v3_rescue_app.command("validate-signal-vs-parameter-attribution")
def dynamic_v3_validate_signal_vs_parameter_attribution_command(
    attribution_id: Annotated[
        str, typer.Option("--attribution-id", help="signal-vs-parameter attribution id。")
    ],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="signal-vs-parameter attribution root。")
    ] = micro.DEFAULT_SIGNAL_VS_PARAMETER_ATTRIBUTION_DIR,
) -> None:
    payload = micro.validate_signal_vs_parameter_attribution_artifact(
        attribution_id=attribution_id, output_dir=output_dir
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_gate_calibrated_review_report_command",
    "dynamic_v3_gate_calibrated_review_run_command",
    "dynamic_v3_micro_search_v4_backfill_report_command",
    "dynamic_v3_micro_search_v4_backfill_run_command",
    "dynamic_v3_micro_search_v4_design_report_command",
    "dynamic_v3_micro_search_v4_design_run_command",
    "dynamic_v3_signal_vs_parameter_attribution_report_command",
    "dynamic_v3_signal_vs_parameter_attribution_run_command",
    "dynamic_v3_validate_gate_calibrated_review_command",
    "dynamic_v3_validate_micro_search_v4_backfill_command",
    "dynamic_v3_validate_micro_search_v4_design_command",
    "dynamic_v3_validate_signal_vs_parameter_attribution_command",
]
