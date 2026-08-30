from __future__ import annotations

import os
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import typer

from ai_trading_system.cli_commands import data as data_cli
from ai_trading_system.cli_commands import data_cache as data_cache_cli
from ai_trading_system.cli_commands import docs as docs_cli
from ai_trading_system.cli_commands import etf_portfolio as etf_cli
from ai_trading_system.cli_commands import feedback as feedback_cli
from ai_trading_system.cli_commands import forward_evidence as forward_evidence_cli
from ai_trading_system.cli_commands import fundamentals as fundamentals_cli
from ai_trading_system.cli_commands import ops as ops_cli
from ai_trading_system.cli_commands import parameters as parameters_cli
from ai_trading_system.cli_commands import pit_snapshots as pit_snapshots_cli
from ai_trading_system.cli_commands import portfolio as portfolio_cli
from ai_trading_system.cli_commands import reports as reports_cli
from ai_trading_system.cli_commands import risk_events as risk_events_cli
from ai_trading_system.cli_commands import score_daily as score_daily_cli
from ai_trading_system.cli_commands import sec_pit as sec_pit_cli
from ai_trading_system.cli_commands import security as security_cli
from ai_trading_system.cli_commands import signals as signals_cli
from ai_trading_system.cli_commands import valuation as valuation_cli
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.contracts.data_quality_execution import VerifiedDataQualityPreflight
from ai_trading_system.contracts.status import CanonicalStatus
from ai_trading_system.data.download_publication import (
    ValidatedDownloadPublication,
    resolve_download_publication,
)
from ai_trading_system.data.quality_consumer_authorization import (
    DAILY_SCORE_CONSUMER_AUTHORIZATION_TOKEN,
    DAILY_SCORE_CONSUMER_ID,
    DAILY_SCORE_CONSUMER_VERSION,
    DataQualityConsumerAuthorizationError,
    build_data_quality_consumer_authorization_attestation,
    load_reviewed_data_quality_consumer_authorization_policy,
    verify_data_quality_consumer_authorization,
    write_data_quality_consumer_authorization_attestation,
)
from ai_trading_system.data.quality_execution import (
    DataQualityExecutionError,
    verify_data_quality_execution_receipt,
)
from ai_trading_system.data.quality_execution_discovery import (
    DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID,
    DiscoveredDataQualityExecution,
    load_default_data_quality_execution_discovery,
)
from ai_trading_system.interfaces.cli.etf_portfolio import data_quality as etf_data_quality_cli
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_observation_lifecycle as etf_observation_lifecycle_cli,
)
from ai_trading_system.interfaces.cli.etf_portfolio import operations as etf_operations_cli
from ai_trading_system.interfaces.cli.etf_portfolio import reporting as etf_reporting_cli
from ai_trading_system.platform.operations.periodic_consumer_migration import (
    NativeConsumerExpectedContext,
    NativeParityRunnerResult,
    NativePeriodicConsumerPlanEntry,
    NativePeriodicConsumerRehearsalResult,
    PeriodicConsumerMigrationError,
    build_native_periodic_consumer_parity_plan,
    dispatch_native_periodic_consumer,
)
from ai_trading_system.platform.operations.runtime_control import (
    OperationsRunControl,
    load_operations_runtime_control_policy,
)
from ai_trading_system.scheduled_tasks import load_scheduled_tasks_config


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    try:
        _dispatch(args)
    except typer.Exit as exc:
        return int(exc.exit_code or 0)
    except typer.BadParameter as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except (
        DataQualityConsumerAuthorizationError,
        DataQualityExecutionError,
        PeriodicConsumerMigrationError,
    ) as exc:
        print(f"daily_score_daily 授权阻断：{exc}", file=sys.stderr)
        return 1
    return 0


@dataclass(frozen=True)
class _DirectDispatchClock:
    value: datetime

    def now(self) -> datetime:
        return self.value


def _dispatch_daily_score_with_consumer_authorization(
    *,
    as_of: date,
    score_runner: Callable[[], None],
    run_id: str | None,
    project_root: Path,
    now: datetime | None = None,
    discovery_loader: Callable[..., DiscoveredDataQualityExecution] = (
        load_default_data_quality_execution_discovery
    ),
    receipt_verifier: Callable[..., VerifiedDataQualityPreflight] = (
        verify_data_quality_execution_receipt
    ),
    publication_resolver: Callable[..., ValidatedDownloadPublication] = (
        resolve_download_publication
    ),
    runtime_control: OperationsRunControl | None = None,
) -> NativePeriodicConsumerRehearsalResult:
    """Authorize and dispatch the only Wave15 score consumer.

    This is the subprocess boundary reached by ``ops_daily``. It discovers
    canonical daily-default evidence itself; caller-supplied PASS/status values
    are never accepted.
    """

    root = project_root.resolve()
    timestamp = (now or datetime.now(tz=UTC)).astimezone(UTC)
    discovered = discovery_loader(as_of, project_root=root)
    preflight = receipt_verifier(
        discovered.receipt_path,
        expected_as_of=as_of,
        expected_policy_path=root / "config/data_quality.yaml",
        expected_input_roles=("prices", "rates", "secondary_prices"),
        project_root=root,
    ).assert_strict_passed()
    pointer = discovered.pointer
    if (
        pointer.profile_id != DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID
        or pointer.as_of != as_of
        or discovered.receipt.as_of != as_of
        or preflight.as_of != as_of
    ):
        raise DataQualityConsumerAuthorizationError(
            "DQ_AS_OF_MISMATCH",
            "daily discovery, receipt and preflight must match score as_of",
        )
    if (
        pointer.receipt_id != preflight.receipt_id
        or pointer.receipt_path != preflight.receipt_path
        or pointer.receipt_sha256 != preflight.receipt_sha256
        or pointer.receipt_size_bytes != preflight.receipt_size_bytes
        or discovered.receipt.receipt_id != preflight.receipt_id
    ):
        raise DataQualityConsumerAuthorizationError(
            "DQ_RECEIPT_ID_MISMATCH",
            "daily discovery pointer differs from strict preflight",
        )

    policy = load_reviewed_data_quality_consumer_authorization_policy(project_root=root)
    publication = publication_resolver(output_dir=root / Path(policy.publication_output_dir))
    attestation = build_data_quality_consumer_authorization_attestation(
        policy=policy,
        preflight=preflight,
        publication=publication,
        authorized_at=timestamp,
        project_root=root,
    )
    attestation_path = write_data_quality_consumer_authorization_attestation(
        attestation,
        project_root=root,
    )
    authorization = verify_data_quality_consumer_authorization(
        attestation_path,
        expected_consumer_id=DAILY_SCORE_CONSUMER_ID,
        expected_consumer_version=DAILY_SCORE_CONSUMER_VERSION,
        expected_as_of=as_of,
        expected_data_quality_policy_path=Path("config/data_quality.yaml"),
        receipt_verifier=receipt_verifier,
        publication_resolver=publication_resolver,
        now=timestamp,
        project_root=root,
    )

    def fixed_receipt_verifier(
        receipt_path: Path,
        *,
        expected_as_of: date,
        expected_policy_path: Path,
        expected_input_roles: tuple[str, ...],
        project_root: Path = PROJECT_ROOT,
    ) -> VerifiedDataQualityPreflight:
        if (
            receipt_path.resolve() != discovered.receipt_path.resolve()
            or expected_as_of != as_of
            or expected_policy_path.resolve() != (root / "config/data_quality.yaml").resolve()
            or tuple(sorted(expected_input_roles)) != ("prices", "rates", "secondary_prices")
            or project_root != root
        ):
            raise DataQualityConsumerAuthorizationError(
                "DQ_CONSUMER_AUTHORIZATION_LINEAGE_MISMATCH",
                "native plan requested a different receipt context",
            )
        return preflight

    context = NativeConsumerExpectedContext(
        as_of=as_of,
        data_quality_as_of=as_of,
        expected_policy_path=root / "config/data_quality.yaml",
        expected_input_roles=("prices", "rates", "secondary_prices"),
        daily_status=CanonicalStatus.PASS,
        required_artifacts_ready=True,
        source_artifact_ids=(attestation.authorization_id,),
        owner_gate_approved=True,
        owner_decision_id=policy.owner_decision_id,
    )
    clock = _DirectDispatchClock(timestamp)
    plan = build_native_periodic_consumer_parity_plan(
        discovered.receipt_path,
        expected_context=context,
        scheduled=load_scheduled_tasks_config(),
        verifier=fixed_receipt_verifier,
        clock=clock,
        project_root=root,
    )
    control = runtime_control or OperationsRunControl(
        root=root / "outputs/run_control/periodic/daily_score_consumer",
        policy=load_operations_runtime_control_policy(),
    )

    def controlled_runner(
        entry: NativePeriodicConsumerPlanEntry,
    ) -> NativeParityRunnerResult:
        if entry.definition.task_id != DAILY_SCORE_CONSUMER_ID:
            raise PeriodicConsumerMigrationError(
                "G4B_CONSUMER_IDENTITY_MISMATCH",
                entry.definition.task_id,
            )
        score_runner()
        return NativeParityRunnerResult(
            passed=True,
            retryable=False,
            artifact_refs=(
                f"daily_score:{as_of.isoformat()}",
                attestation.authorization_id,
            ),
        )

    result = dispatch_native_periodic_consumer(
        plan,
        task_id=DAILY_SCORE_CONSUMER_ID,
        authorization=authorization,
        control=control,
        runner=controlled_runner,
        run_id=f"g4b:{run_id or attestation.authorization_id}",
        clock=clock,
    )
    if result.status is not CanonicalStatus.PASS:
        raise PeriodicConsumerMigrationError(
            "G4B_DAILY_SCORE_NOT_DISPATCHED",
            ",".join(result.blocker_codes) or result.status.value,
        )
    return result


def _dispatch(args: list[str]) -> None:
    if args[:2] == ["ops", "capture-daily-inputs"]:
        as_of = _option(args, "--as-of")
        if as_of is None:
            raise typer.BadParameter("capture-daily-inputs 必须显式提供 --as-of")
        ops_cli.capture_daily_inputs_command(
            as_of=as_of,
            policy_path=_path_option_with_default(
                args,
                "--policy-path",
                ops_cli.DEFAULT_DAILY_INPUT_CAPTURE_POLICY_PATH,
            ),
            download_start=_option(args, "--download-start", "2018-01-01"),
            full_universe=_flag(args, "--full-universe"),
        )
        return
    if args[:1] == ["download-data"]:
        data_cache_cli.download_data(
            start=_option(args, "--start", "2018-01-01"),
            end=_option(args, "--end"),
            full_universe=_flag(args, "--full-universe"),
        )
        return
    if args[:1] == ["validate-data"]:
        data_cache_cli.validate_data(
            as_of=_option(args, "--as-of"),
            execution_profile=_option(
                args,
                "--execution-profile",
                data_cache_cli.AUTO_DATA_QUALITY_EXECUTION_PROFILE_ID,
            )
            or data_cache_cli.AUTO_DATA_QUALITY_EXECUTION_PROFILE_ID,
            full_universe=_flag(args, "--full-universe"),
        )
        return
    if args[:2] == ["data", "diagnose-backtest-inputs"]:
        data_cli.data_diagnose_backtest_inputs_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=_path_option_with_default(
                args,
                "--config",
                data_cli.DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
            ),
        )
        return
    if args[:2] == ["data", "repair-backtest-inputs"]:
        data_cli.data_repair_backtest_inputs_command(
            ctx=SimpleNamespace(args=[]),
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=_path_option_with_default(
                args,
                "--config",
                data_cli.DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
            ),
            dry_run=_flag(args, "--dry-run"),
            price_only=_flag(args, "--price-only"),
            symbols=_values_after_option(args, "--symbols"),
            price_provider=_option(args, "--price-provider", "fmp") or "fmp",
            fmp_api_key_env=_option(args, "--fmp-api-key-env", "FMP_API_KEY") or "FMP_API_KEY",
        )
        return
    if args[:2] == ["data", "freshness"]:
        data_cli.data_freshness_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            market=_option(args, "--market", "US") or "US",
            config_path=_path_option_with_default(
                args,
                "--config",
                data_cli.DEFAULT_MARKET_DATA_FRESHNESS_CONFIG_PATH,
            ),
            dry_run=_flag(args, "--dry-run"),
        )
        return
    if args[:2] == ["data", "recover-freshness"]:
        data_cli.data_recover_freshness_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            refresh_config_path=_path_option_with_default(
                args,
                "--refresh-config",
                data_cli.DEFAULT_MARKET_DATA_REFRESH_CONFIG_PATH,
            ),
            freshness_config_path=_path_option_with_default(
                args,
                "--freshness-config",
                data_cli.DEFAULT_MARKET_DATA_FRESHNESS_CONFIG_PATH,
            ),
        )
        return
    if args[:2] == ["signals", "build-snapshot"]:
        signals_cli.signals_build_snapshot_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=_path_option(args, "--config"),
            dry_run=_flag(args, "--dry-run"),
            price_derived_only=_flag(args, "--price-derived-only"),
        )
        return
    if args[:2] == ["signals", "validate-snapshot"]:
        signals_cli.signals_validate_snapshot_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            input_path=_optional_path(args, "--input-path"),
        )
        return
    if args[:2] == ["signals", "ablation"]:
        signals_cli.signals_ablation_command(
            ctx=SimpleNamespace(args=[]),
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=_signal_ablation_config_option(args, "--config"),
            signals=_values_after_option(args, "--signals"),
            dry_run=_flag(args, "--dry-run"),
            debug=_flag(args, "--debug"),
        )
        return
    if args[:2] == ["signals", "calibrate"]:
        signals_cli.signals_calibrate_command(
            ctx=SimpleNamespace(args=[]),
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=_signal_calibration_config_option(args, "--config"),
            profile=_option(args, "--profile"),
            profiles=_values_after_option(args, "--profiles"),
            dry_run=_flag(args, "--dry-run"),
        )
        return
    if args[:2] == ["signals", "explain-ablation"]:
        signals_cli.signals_explain_ablation_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            input_path=_optional_path(args, "--input-path"),
        )
        return
    if args[:2] == ["signals", "validate-ablation"]:
        signals_cli.signals_validate_ablation_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            input_path=_optional_path(args, "--input-path"),
        )
        return
    if args[:2] == ["pit-snapshots", "fetch-fmp-forward"]:
        pit_snapshots_cli.fetch_fmp_forward_pit_command(
            as_of=_option(args, "--as-of"),
            raw_output_dir=_path_option_with_default(
                args,
                "--raw-output-dir",
                pit_snapshots_cli.DEFAULT_FMP_FORWARD_PIT_RAW_DIR,
            ),
            normalized_output_path=_optional_path(args, "--normalized-output-path"),
            manifest_path=_path_option_with_default(
                args,
                "--manifest-path",
                pit_snapshots_cli.DEFAULT_PIT_SNAPSHOT_MANIFEST_PATH,
            ),
            output_path=_optional_path(args, "--output-path"),
            pit_validation_report_path=_optional_path(
                args,
                "--pit-validation-report-path",
            ),
            continue_on_failure=_flag(args, "--continue-on-failure"),
        )
        return
    if args[:2] == ["pit-snapshots", "project-fmp-forward-capture"]:
        pit_snapshots_cli.project_fmp_forward_pit_capture_command(
            as_of=_option(args, "--as-of") or "",
            raw_input_dir=_path_option(args, "--raw-input-dir"),
            capture_normalized_input_path=_path_option(
                args,
                "--capture-normalized-input-path",
            ),
            normalized_output_path=_path_option(args, "--normalized-output-path"),
            output_path=_path_option(args, "--output-path"),
        )
        return
    if args[:2] == ["pit-snapshots", "build-manifest"]:
        pit_snapshots_cli.build_pit_snapshot_manifest_command(
            as_of=_option(args, "--as-of"),
            output_path=_path_option_with_default(
                args,
                "--output-path",
                pit_snapshots_cli.DEFAULT_PIT_SNAPSHOT_MANIFEST_PATH,
            ),
            fmp_analyst_history_dir=_path_option_with_default(
                args,
                "--fmp-analyst-history-dir",
                pit_snapshots_cli.DEFAULT_FMP_ANALYST_ESTIMATE_HISTORY_DIR,
            ),
            fmp_forward_pit_dir=_path_option_with_default(
                args,
                "--fmp-forward-pit-dir",
                pit_snapshots_cli.DEFAULT_FMP_FORWARD_PIT_RAW_DIR,
            ),
            validation_report_path=_optional_path(
                args,
                "--validation-report-path",
            ),
            required_snapshot_kinds=_option(args, "--required-snapshot-kinds"),
        )
        return
    if args[:2] == ["pit-snapshots", "validate"]:
        pit_snapshots_cli.validate_pit_snapshots_command(
            as_of=_option(args, "--as-of"),
            input_path=_path_option_with_default(
                args,
                "--input-path",
                pit_snapshots_cli.DEFAULT_PIT_SNAPSHOT_MANIFEST_PATH,
            ),
            output_path=_optional_path(args, "--output-path"),
            required_snapshot_kinds=_option(args, "--required-snapshot-kinds"),
        )
        return
    if args[:2] == ["fundamentals", "download-sec-companyfacts"]:
        fundamentals_cli.download_sec_companyfacts_command(
            output_dir=_path_option_with_default(
                args,
                "--output-dir",
                fundamentals_cli.PROJECT_ROOT / "data" / "raw" / "sec_companyfacts",
            ),
            user_agent=_option(args, "--user-agent") or os.getenv("SEC_USER_AGENT"),
        )
        return
    if args[:2] == ["fundamentals", "extract-sec-metrics"]:
        fundamentals_cli.extract_sec_metrics_command(
            as_of=_option(args, "--as-of"),
            input_dir=_path_option_with_default(
                args,
                "--input-dir",
                fundamentals_cli.PROJECT_ROOT / "data" / "raw" / "sec_companyfacts",
            ),
        )
        return
    if args[:2] == ["fundamentals", "merge-tsm-ir-sec-metrics"]:
        fundamentals_cli.merge_tsm_ir_sec_metrics(as_of=_option(args, "--as-of"))
        return
    if args[:2] == ["fundamentals", "validate-sec-metrics"]:
        fundamentals_cli.validate_sec_metrics_command(as_of=_option(args, "--as-of"))
        return
    if args[:2] == ["valuation", "fetch-fmp"]:
        valuation_cli.fetch_fmp_valuations(
            as_of=_option(args, "--as-of"),
            output_dir=_path_option_with_default(
                args,
                "--output-dir",
                valuation_cli.PROJECT_ROOT / "data" / "external" / "valuation_snapshots",
            ),
            analyst_history_dir=_path_option_with_default(
                args,
                "--analyst-history-dir",
                valuation_cli.DEFAULT_FMP_ANALYST_ESTIMATE_HISTORY_DIR,
            ),
            pit_normalized_path=(
                _optional_path(args, "--pit-normalized-path")
                or valuation_cli.DEFAULT_FMP_FORWARD_PIT_NORMALIZED_DIR
            ),
            valuation_history_dir=_optional_path(args, "--valuation-history-dir"),
            output_path=_optional_path(args, "--output-path"),
            validation_report_path=_optional_path(
                args,
                "--validation-report-path",
            ),
        )
        return
    if args[:2] == ["risk-events", "fetch-official-sources"]:
        risk_events_cli.fetch_official_policy_sources_command(
            as_of=_option(args, "--as-of"),
            raw_dir=_path_option_with_default(
                args,
                "--raw-dir",
                risk_events_cli.DEFAULT_OFFICIAL_POLICY_RAW_DIR,
            ),
            processed_dir=_path_option_with_default(
                args,
                "--processed-dir",
                risk_events_cli.DEFAULT_OFFICIAL_POLICY_PROCESSED_DIR,
            ),
            download_manifest_path=_path_option_with_default(
                args,
                "--download-manifest-path",
                risk_events_cli.PROJECT_ROOT / "data" / "raw" / "download_manifest.csv",
            ),
            output_path=_optional_path(args, "--output-path"),
        )
        return
    if args[:2] == ["score-daily", "backfill-baseline"]:
        raise typer.BadParameter(
            "daily-run direct dispatcher 不支持 score-daily backfill-baseline；请使用主 CLI。"
        )
    if args[:1] == ["score-daily"]:
        as_of = _option(args, "--as-of")
        max_candidates = _option(args, "--risk-event-openai-precheck-max-candidates")
        run_id = _option(args, "--run-id")

        def run_score_daily() -> None:
            score_daily_cli.score_daily(
                as_of=as_of,
                risk_event_openai_precheck_max_candidates=(
                    int(max_candidates) if max_candidates is not None else None
                ),
                risk_event_openai_precheck=not _flag(args, "--skip-risk-event-openai-precheck"),
                llm_request_profile=_option(
                    args,
                    "--llm-request-profile",
                    "risk_event_daily_official_precheck",
                )
                or "risk_event_daily_official_precheck",
                run_id=run_id,
                risk_event_openai_precheck_visibility_cutoff=_option(
                    args,
                    "--risk-event-openai-precheck-visibility-cutoff",
                ),
                valuation_path=(
                    _optional_path(args, "--valuation-path")
                    or score_daily_cli.PROJECT_ROOT / "data" / "external" / "valuation_snapshots"
                ),
                official_policy_capture_manifest_path=_optional_path(
                    args,
                    "--official-policy-capture-manifest-path",
                ),
            )

        authorization_profile = _option(args, "--consumer-authorization-profile")
        if authorization_profile is None:
            run_score_daily()
            return
        if authorization_profile != DAILY_SCORE_CONSUMER_AUTHORIZATION_TOKEN:
            raise typer.BadParameter(
                "score-daily consumer authorization profile 不受支持：" f"{authorization_profile}"
            )
        if as_of is None:
            raise typer.BadParameter("受控 score-daily dispatch 必须显式提供 --as-of YYYY-MM-DD")
        try:
            parsed_as_of = date.fromisoformat(as_of)
        except ValueError as exc:
            raise typer.BadParameter("score-daily --as-of 必须是 ISO 日期") from exc
        _dispatch_daily_score_with_consumer_authorization(
            as_of=parsed_as_of,
            score_runner=run_score_daily,
            run_id=run_id,
            project_root=Path.cwd(),
        )
        return
    if args[:2] == ["forward-evidence", "capture-dry-run-daily"]:
        forward_evidence_cli.forward_evidence_capture_dry_run_daily_command(
            as_of=_option(args, "--as-of") or _option(args, "--as-of-date"),
            benchmark_expansion=_optional_path(args, "--benchmark-expansion")
            or forward_evidence_cli.DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_REPORT_PATH,
            control_audit=_optional_path(args, "--control-audit")
            or forward_evidence_cli.DEFAULT_CONTROL_AUDIT_REPORT_PATH,
            feature_snapshot_reference=(
                _option(args, "--feature-snapshot-reference") or "daily_score_decision_snapshot"
            ),
            output_root=_optional_path(args, "--output-root")
            or forward_evidence_cli.DEFAULT_FORWARD_DRY_RUN_ARCHIVE_OUTPUT_ROOT,
            ledger_path=_optional_path(args, "--ledger-path")
            or forward_evidence_cli.DEFAULT_FORWARD_DAILY_DRY_RUN_LEDGER_PATH,
        )
        return
    if args[:2] == ["feedback", "optimize-market-feedback"]:
        feedback_cli.optimize_market_feedback_command(as_of=_option(args, "--as-of"))
        return
    if args[:2] == ["feedback", "evaluate-parameter-governance"]:
        feedback_cli.evaluate_parameter_governance_command(as_of=_option(args, "--as-of"))
        return
    if args[:2] == ["feedback", "loop-review"]:
        feedback_cli.feedback_loop_review_command(as_of=_option(args, "--as-of"))
        return
    if args[:2] == ["reports", "investment-review"]:
        reports_cli.investment_periodic_review_command(
            period=_option(args, "--period", "weekly") or "weekly",
            as_of=_option(args, "--as-of"),
        )
        return
    if args[:2] == ["reports", "dashboard"]:
        reports_cli.evidence_dashboard_command(as_of=_option(args, "--as-of"))
        return
    if args[:2] == ["reports", "calculation-explainers"]:
        reports_cli.calculation_explainers_command(as_of=_option(args, "--as-of"))
        return
    if args[:2] == ["portfolio", "track-candidate"]:
        portfolio_cli.portfolio_track_candidate_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            review_path=_optional_path(args, "--review"),
            config_path=_path_option_with_default(
                args,
                "--config",
                portfolio_cli.DEFAULT_PORTFOLIO_CANDIDATE_TRACKING_CONFIG_PATH,
            ),
            dry_run=_flag(args, "--dry-run"),
        )
        return
    if args[:2] == ["portfolio", "review-tracking"]:
        portfolio_cli.portfolio_review_tracking_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            candidate_profile=_option(args, "--candidate"),
            window=_option(args, "--window"),
            show_window_progress=_flag(args, "--show-window-progress"),
            config_path=_path_option_with_default(
                args,
                "--config",
                portfolio_cli.DEFAULT_PORTFOLIO_TRACKING_REVIEW_CONFIG_PATH,
            ),
            dry_run=_flag(args, "--dry-run"),
        )
        return
    if args[:3] == ["etf", "forward", "update"]:
        etf_cli.forward_update_command(
            date_option=_option(args, "--as-of") or _option(args, "--date"),
            latest=_flag(args, "--latest"),
            config_path=_path_option_with_default(
                args,
                "--config-path",
                etf_cli.DEFAULT_ETF_FORWARD_CONFIG_PATH,
            ),
            registry_path=_path_option_with_default(
                args,
                "--registry-path",
                etf_cli.DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
            ),
            decision_ledger_path=_path_option_with_default(
                args,
                "--decision-ledger-path",
                etf_cli.DEFAULT_ETF_FORWARD_DECISION_LEDGER_PATH,
            ),
            prices_path=_path_option_with_default(
                args,
                "--prices-path",
                etf_cli.DEFAULT_ETF_PRICE_PATH,
            ),
            output_dir=_path_option_with_default(
                args,
                "--output-dir",
                etf_cli.DEFAULT_ETF_FORWARD_REPORT_DIR / "updates",
            ),
        )
        return
    if args[:3] == ["etf", "forward", "dashboard"]:
        etf_cli.forward_dashboard_command(
            date_option=_option(args, "--as-of") or _option(args, "--date"),
            latest=_flag(args, "--latest"),
            registry_path=_path_option_with_default(
                args,
                "--registry-path",
                etf_cli.DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
            ),
            update_dir=_path_option_with_default(
                args,
                "--update-dir",
                etf_cli.DEFAULT_ETF_FORWARD_REPORT_DIR / "updates",
            ),
            output_dir=_path_option_with_default(
                args,
                "--output-dir",
                etf_cli.DEFAULT_ETF_FORWARD_REPORT_DIR / "dashboard",
            ),
        )
        return
    if args[:3] == ["etf", "forward", "watchlist"]:
        etf_cli.forward_watchlist_command(
            date_option=_option(args, "--as-of") or _option(args, "--date"),
            latest=_flag(args, "--latest"),
            dashboard_dir=_path_option_with_default(
                args,
                "--dashboard-dir",
                etf_cli.DEFAULT_ETF_FORWARD_REPORT_DIR / "dashboard",
            ),
            output_dir=_path_option_with_default(
                args,
                "--output-dir",
                etf_cli.DEFAULT_ETF_FORWARD_REPORT_DIR / "watchlist",
            ),
        )
        return
    if args[:3] == ["etf", "ops", "dry-run"]:
        etf_operations_cli.ops_dry_run_command(
            cadence=_option(args, "--cadence", "daily") or "daily",
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            root_path=_path_option_with_default(args, "--root-path", etf_cli.PROJECT_ROOT),
            output_path=_optional_path(args, "--output-path"),
            include_optional=not _flag(args, "--skip-optional"),
            no_write=_flag(args, "--no-write"),
        )
        return
    if args[:3] == ["etf", "ops", "validate"]:
        etf_operations_cli.ops_validate_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            root_path=_path_option_with_default(args, "--root-path", etf_cli.PROJECT_ROOT),
            config_path=(
                _optional_path(args, "--config-path")
                or _optional_path(args, "--config")
                or etf_operations_cli.DEFAULT_ETF_OPERATIONS_SCHEDULE_CONFIG_PATH
            ),
            output_dir=_path_option_with_default(
                args,
                "--output-dir",
                etf_operations_cli.DEFAULT_ETF_OPERATIONS_VALIDATION_DIR,
            ),
            json_path=_optional_path(args, "--json-path"),
            markdown_path=_optional_path(args, "--markdown-path"),
        )
        return
    if args[:3] == ["etf", "ops", "report"]:
        etf_operations_cli.ops_report_command(
            cadence=_option(args, "--cadence", "daily") or "daily",
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            root_path=_path_option_with_default(args, "--root-path", etf_cli.PROJECT_ROOT),
            output_dir=_path_option_with_default(
                args,
                "--output-dir",
                etf_operations_cli.DEFAULT_ETF_OPERATIONS_REPORT_DIR,
            ),
            json_path=_optional_path(args, "--json-path"),
            markdown_path=_optional_path(args, "--markdown-path"),
            include_optional=not _flag(args, "--skip-optional"),
        )
        return
    if args[:3] == ["etf", "data-quality", "report"]:
        etf_data_quality_cli.data_quality_report_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            prices_path=_path_option_with_default(
                args,
                "--prices-path",
                etf_cli.DEFAULT_ETF_PRICE_PATH,
            ),
            config_path=(
                _optional_path(args, "--config-path")
                or _optional_path(args, "--config")
                or etf_cli.DEFAULT_ETF_DATA_QUALITY_POLICY_CONFIG_PATH
            ),
            report_registry_path=_path_option_with_default(
                args,
                "--report-registry-path",
                etf_cli.DEFAULT_REPORT_REGISTRY_PATH,
            ),
            root_path=_path_option_with_default(args, "--root-path", etf_cli.PROJECT_ROOT),
            output_dir=_path_option_with_default(
                args,
                "--output-dir",
                etf_cli.DEFAULT_ETF_DATA_QUALITY_REPORT_DIR,
            ),
            json_path=_optional_path(args, "--json-path"),
            markdown_path=_optional_path(args, "--markdown-path"),
        )
        return
    if args[:3] == ["etf", "data-quality", "validate"]:
        etf_data_quality_cli.data_quality_validate_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=(
                _optional_path(args, "--config-path")
                or _optional_path(args, "--config")
                or etf_cli.DEFAULT_ETF_DATA_QUALITY_POLICY_CONFIG_PATH
            ),
            report_registry_path=_path_option_with_default(
                args,
                "--report-registry-path",
                etf_cli.DEFAULT_REPORT_REGISTRY_PATH,
            ),
            output_dir=_path_option_with_default(
                args,
                "--output-dir",
                etf_cli.DEFAULT_ETF_DATA_QUALITY_VALIDATION_DIR,
            ),
            json_path=_optional_path(args, "--json-path"),
            markdown_path=_optional_path(args, "--markdown-path"),
        )
        return
    if args[:4] == ["etf", "dynamic-v3-rescue", "schedule", "observe"]:
        etf_observation_lifecycle_cli.dynamic_v3_schedule_observe_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            family=_option(args, "--family", "dynamic_v3_rescue") or "dynamic_v3_rescue",
            config_path=(
                _optional_path(args, "--config-path")
                or _optional_path(args, "--config")
                or etf_observation_lifecycle_cli.DEFAULT_PARAMETER_SWEEP_CONFIG_PATH
            ),
            pointer_dir=_path_option_with_default(
                args,
                "--pointer-dir",
                etf_observation_lifecycle_cli.DEFAULT_LATEST_POINTER_DIR,
            ),
            registry_path=(
                _optional_path(args, "--registry-path")
                or _optional_path(args, "--registry")
                or etf_observation_lifecycle_cli.DEFAULT_SHADOW_REGISTRY_PATH
            ),
            output_dir=_path_option_with_default(
                args,
                "--output-dir",
                etf_observation_lifecycle_cli.DEFAULT_SCHEDULE_OBSERVE_DIR,
            ),
            run_shadow_monitor=not _flag(args, "--skip-shadow-monitor"),
            force_due=_flag(args, "--force-due"),
        )
        return
    if args[:3] == ["etf", "evidence-dashboard", "aggregate"]:
        etf_reporting_cli.evidence_dashboard_aggregate_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=(
                _optional_path(args, "--config-path")
                or _optional_path(args, "--config")
                or etf_reporting_cli.DEFAULT_STRATEGY_EVIDENCE_CONFIG_PATH
            ),
            report_index_path=_optional_path(args, "--report-index-path"),
            report_registry_path=_path_option_with_default(
                args,
                "--report-registry-path",
                etf_reporting_cli.DEFAULT_REPORT_REGISTRY_PATH,
            ),
            root_path=_path_option_with_default(args, "--root-path", etf_cli.PROJECT_ROOT),
            output_dir=_path_option_with_default(
                args,
                "--output-dir",
                etf_reporting_cli.DEFAULT_STRATEGY_EVIDENCE_AGGREGATION_DIR,
            ),
            json_path=_optional_path(args, "--json-path"),
        )
        return
    if args[:3] == ["etf", "evidence-dashboard", "report"]:
        etf_reporting_cli.evidence_dashboard_report_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=(
                _optional_path(args, "--config-path")
                or _optional_path(args, "--config")
                or etf_reporting_cli.DEFAULT_STRATEGY_EVIDENCE_CONFIG_PATH
            ),
            report_index_path=_optional_path(args, "--report-index-path"),
            report_registry_path=_path_option_with_default(
                args,
                "--report-registry-path",
                etf_reporting_cli.DEFAULT_REPORT_REGISTRY_PATH,
            ),
            root_path=_path_option_with_default(args, "--root-path", etf_cli.PROJECT_ROOT),
            output_dir=_path_option_with_default(
                args,
                "--output-dir",
                etf_reporting_cli.DEFAULT_STRATEGY_EVIDENCE_REPORT_DIR,
            ),
            json_path=_optional_path(args, "--json-path"),
            markdown_path=_optional_path(args, "--markdown-path"),
        )
        return
    if args[:3] == ["etf", "evidence-dashboard", "validate"]:
        etf_reporting_cli.evidence_dashboard_validate_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=(
                _optional_path(args, "--config-path")
                or _optional_path(args, "--config")
                or etf_reporting_cli.DEFAULT_STRATEGY_EVIDENCE_CONFIG_PATH
            ),
            report_registry_path=_path_option_with_default(
                args,
                "--report-registry-path",
                etf_reporting_cli.DEFAULT_REPORT_REGISTRY_PATH,
            ),
            output_dir=_path_option_with_default(
                args,
                "--output-dir",
                etf_reporting_cli.DEFAULT_STRATEGY_EVIDENCE_VALIDATION_DIR,
            ),
            json_path=_optional_path(args, "--json-path"),
            markdown_path=_optional_path(args, "--markdown-path"),
        )
        return
    if args[:2] == ["parameters", "shadow-backtest"]:
        parameters_cli.parameters_shadow_backtest_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=_path_option_with_default(
                args,
                "--config",
                parameters_cli.DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
            ),
            dry_run=_flag(args, "--dry-run"),
        )
        return
    if args[:2] == ["parameters", "validate-shadow-backtest"]:
        parameters_cli.validate_shadow_backtest_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            input_path=_optional_path(args, "--input-path"),
        )
        return
    if args[:2] == ["parameters", "tune-weights-stable"]:
        parameters_cli.parameters_tune_weights_stable_command(
            ctx=SimpleNamespace(args=[]),
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=_path_option_with_default(
                args,
                "--config",
                parameters_cli.DEFAULT_WEIGHT_STABILITY_CONFIG_PATH,
            ),
            portfolio_profile=_option(args, "--portfolio-profile"),
            signals=_values_after_option(args, "--signals"),
            dry_run=_flag(args, "--dry-run"),
        )
        return
    if args[:2] == ["parameters", "diagnose-weight-stability-inputs"]:
        parameters_cli.diagnose_weight_stability_inputs_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=_path_option_with_default(
                args,
                "--config",
                parameters_cli.DEFAULT_WEIGHT_STABILITY_READINESS_CONFIG_PATH,
            ),
            dry_run=_flag(args, "--dry-run"),
        )
        return
    if args[:2] == ["parameters", "recover-weight-stability-inputs"]:
        parameters_cli.recover_weight_stability_inputs_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=_path_option_with_default(
                args,
                "--config",
                parameters_cli.DEFAULT_WEIGHT_STABILITY_READINESS_CONFIG_PATH,
            ),
            dry_run=_flag(args, "--dry-run"),
        )
        return
    if args[:2] == ["parameters", "validate-weight-stability-readiness"]:
        parameters_cli.validate_weight_stability_readiness_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            input_path=_optional_path(args, "--input-path"),
        )
        return
    if args[:2] == ["parameters", "validate-weight-stability"]:
        parameters_cli.validate_weight_stability_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            input_path=_optional_path(args, "--input-path"),
        )
        return
    if args[:2] == ["parameters", "explain-weight-stability"]:
        parameters_cli.explain_weight_stability_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            input_path=_optional_path(args, "--input-path"),
        )
        return
    if args[:2] == ["reports", "shadow-parameter-backtest"]:
        reports_cli.shadow_parameter_backtest_report_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            source_path=_optional_path(args, "--source-path"),
        )
        return
    if args[:2] == ["reports", "parameter-promotion"]:
        reports_cli.parameter_promotion_report_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            source_path=_optional_path(args, "--source-path"),
        )
        return
    if args[:2] == ["reports", "weight-stability"]:
        reports_cli.weight_stability_report_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            source_path=_optional_path(args, "--source-path"),
        )
        return
    if args[:2] == ["reports", "weight-stability-readiness"]:
        reports_cli.weight_stability_readiness_report_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            source_path=_optional_path(args, "--source-path"),
        )
        return
    if args[:2] == ["reports", "signal-snapshot"]:
        reports_cli.signal_snapshot_report_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            source_path=_optional_path(args, "--source-path"),
        )
        return
    if args[:2] == ["reports", "signal-ablation"]:
        reports_cli.signal_ablation_report_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            source_path=_optional_path(args, "--source-path"),
        )
        return
    if args[:2] == ["reports", "signal-calibration"]:
        reports_cli.signal_calibration_report_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            source_path=_optional_path(args, "--source-path"),
        )
        return
    if args[:2] == ["reports", "reader-brief"]:
        reports_cli.reader_brief_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            latest=_flag(args, "--latest"),
        )
        return
    if args[:2] == ["reports", "validate-reader-brief"]:
        reports_cli.validate_reader_brief_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            latest=_flag(args, "--latest"),
        )
        return
    if args[:2] == ["reports", "quality-gate"]:
        reports_cli.report_quality_gate_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            latest=_flag(args, "--latest"),
        )
        return
    if args[:2] == ["reports", "artifact-lineage"]:
        reports_cli.artifact_lineage_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            latest=_flag(args, "--latest"),
        )
        return
    if args[:2] == ["reports", "validate-artifact-lineage"]:
        reports_cli.validate_artifact_lineage_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            latest=_flag(args, "--latest"),
        )
        return
    if args[:2] == ["reports", "score-change-attribution"]:
        reports_cli.score_change_attribution_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            latest=_flag(args, "--latest"),
        )
        return
    if args[:2] == ["reports", "market-panel"]:
        reports_cli.market_panel_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            latest=_flag(args, "--latest"),
        )
        return
    if args[:2] == ["reports", "research-governance-summary"]:
        reports_cli.research_governance_summary_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            latest=_flag(args, "--latest"),
        )
        return
    if args[:2] == ["reports", "portfolio-tracking-review"]:
        reports_cli.portfolio_tracking_review_report_command(
            latest=_flag(args, "--latest"),
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            source_path=_optional_path(args, "--source-path"),
        )
        return
    if args[:2] == ["reports", "index"]:
        reports_cli.report_index_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            latest=_flag(args, "--latest"),
        )
        return
    if args[:2] == ["docs", "report-contract"]:
        docs_cli.documentation_contract_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            latest=_flag(args, "--latest"),
        )
        return
    if args[:2] == ["docs", "heuristic-audit"]:
        docs_cli.heuristic_governance_audit_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            config_path=_path_option_with_default(
                args,
                "--config-path",
                docs_cli.DEFAULT_HEURISTIC_GOVERNANCE_CONFIG_PATH,
            ),
            output_path=_optional_path(args, "--output-path"),
            json_output_path=_optional_path(args, "--json-output-path"),
            fail_on_warning=_flag(args, "--fail-on-warning"),
        )
        return
    if args[:2] == ["sec-pit", "shadow-observe"]:
        sec_pit_cli.shadow_observe_command(
            start=_option(args, "--start"),
            end=_option(args, "--end"),
            latest=_flag(args, "--latest"),
        )
        return
    if args[:2] == ["sec-pit", "shadow-monitor"]:
        sec_pit_cli.shadow_monitor_command(
            as_of=_option(args, "--as-of") or _option(args, "--date"),
            latest=_flag(args, "--latest"),
        )
        return
    if args[:2] == ["ops", "health"]:
        ops_cli.pipeline_health_command(
            as_of=_option(args, "--as-of"),
            non_trading_day=_flag(args, "--non-trading-day"),
        )
        return
    if args[:2] == ["security", "scan-secrets"]:
        security_cli.security_scan_secrets_command(as_of=_option(args, "--as-of"))
        return
    raise typer.BadParameter(f"daily-run direct dispatcher 不支持命令：{' '.join(args)}")


def _option(args: Sequence[str], name: str, default: str | None = None) -> str | None:
    try:
        index = args.index(name)
    except ValueError:
        return default
    if index + 1 >= len(args):
        raise typer.BadParameter(f"缺少 {name} 的参数值")
    return args[index + 1]


def _flag(args: Sequence[str], name: str) -> bool:
    return name in args


def _path_option(args: Sequence[str], name: str):
    value = _option(args, name)
    return signals_cli.DEFAULT_SHADOW_BACKTEST_CONFIG_PATH if value is None else Path(value)


def _path_option_with_default(args: Sequence[str], name: str, default: Path):
    value = _option(args, name)
    return default if value is None else Path(value)


def _optional_path(args: Sequence[str], name: str):
    value = _option(args, name)
    return None if value is None else Path(value)


def _signal_ablation_config_option(args: Sequence[str], name: str):
    value = _option(args, name)
    return signals_cli.DEFAULT_SIGNAL_ABLATION_CONFIG_PATH if value is None else Path(value)


def _signal_calibration_config_option(args: Sequence[str], name: str):
    value = _option(args, name)
    return signals_cli.DEFAULT_SIGNAL_CALIBRATION_PROFILES_PATH if value is None else Path(value)


def _values_after_option(args: Sequence[str], name: str) -> list[str]:
    values: list[str] = []
    index = 0
    while index < len(args):
        if args[index] != name:
            index += 1
            continue
        index += 1
        while index < len(args) and not args[index].startswith("--"):
            values.append(args[index])
            index += 1
    return values


if __name__ == "__main__":
    raise SystemExit(main())
