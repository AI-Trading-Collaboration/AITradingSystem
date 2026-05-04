from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.backtest.audit import BacktestAuditReport
from ai_trading_system.backtest.daily import BacktestRegimeContext, DailyBacktestResult
from ai_trading_system.data.quality import DataFileSummary, DataQualityReport
from ai_trading_system.scoring.daily import DailyScoreReport

TraceRecord = dict[str, Any]

SCHEMA_VERSION = 1


@dataclass(frozen=True)
class ReportTraceBundle:
    schema_version: int
    report_id: str
    report_type: str
    generated_at: datetime
    report_path: Path
    run_manifest: TraceRecord
    quality_refs: tuple[TraceRecord, ...]
    dataset_refs: tuple[TraceRecord, ...]
    evidence_cards: tuple[TraceRecord, ...]
    claims: tuple[TraceRecord, ...]

    def to_dict(self) -> TraceRecord:
        return {
            "schema_version": self.schema_version,
            "report_id": self.report_id,
            "report_type": self.report_type,
            "generated_at": self.generated_at.isoformat(),
            "report_path": str(self.report_path),
            "run_manifest": self.run_manifest,
            "quality_refs": list(self.quality_refs),
            "dataset_refs": list(self.dataset_refs),
            "evidence_cards": list(self.evidence_cards),
            "claims": list(self.claims),
        }


def default_report_trace_bundle_path(report_path: Path) -> Path:
    return report_path.parent / "evidence" / f"{report_path.stem}_trace.json"


def build_daily_score_trace_bundle(
    *,
    report: DailyScoreReport,
    report_path: Path,
    data_quality_report_path: Path,
    feature_report_path: Path,
    features_path: Path,
    scores_path: Path,
    market_regime: BacktestRegimeContext | None,
    config_paths: dict[str, Path],
    sec_metrics_validation_report_path: Path | None = None,
    sec_fundamental_feature_report_path: Path | None = None,
    sec_fundamental_features_path: Path | None = None,
    risk_event_occurrence_report_path: Path | None = None,
    belief_state_path: Path | None = None,
) -> ReportTraceBundle:
    report_id = f"daily_score:{report.as_of.isoformat()}"
    date_window = _date_window(report.as_of, report.as_of)
    manifest_records = _download_manifest_records(report.data_quality_report)
    dataset_refs: list[TraceRecord] = [
        _dataset_ref_from_summary(
            dataset_id="dataset:prices_daily",
            label="价格日线缓存",
            dataset_type="raw_market_data",
            summary=report.data_quality_report.price_summary,
            manifest_records=manifest_records,
        ),
        _dataset_ref_from_summary(
            dataset_id="dataset:rates_daily",
            label="利率日线缓存",
            dataset_type="raw_macro_data",
            summary=report.data_quality_report.rate_summary,
            manifest_records=manifest_records,
        ),
        _file_dataset_ref(
            dataset_id=f"dataset:features_daily:{report.as_of.isoformat()}",
            label="市场特征缓存",
            dataset_type="processed_feature_cache",
            path=features_path,
        ),
        _file_dataset_ref(
            dataset_id=f"dataset:scores_daily:{report.as_of.isoformat()}",
            label="每日评分缓存",
            dataset_type="processed_score_cache",
            path=scores_path,
        ),
    ]
    if manifest_ref := _optional_manifest_dataset_ref(report.data_quality_report):
        dataset_refs.append(manifest_ref)
    if sec_fundamental_features_path is not None:
        dataset_refs.append(
            _file_dataset_ref(
                dataset_id=(
                    f"dataset:sec_fundamental_features:{report.as_of.isoformat()}"
                ),
                label="SEC 基本面特征缓存",
                dataset_type="processed_fundamental_feature_cache",
                path=sec_fundamental_features_path,
            )
        )
    if report.valuation_review_report is not None:
        dataset_refs.append(
            _path_dataset_ref(
                dataset_id=f"dataset:valuation_snapshots:{report.as_of.isoformat()}",
                label="估值快照输入",
                dataset_type="valuation_snapshot_store",
                path=report.valuation_review_report.validation_report.input_path,
            )
        )
    if report.risk_event_occurrence_review_report is not None:
        dataset_refs.append(
            _path_dataset_ref(
                dataset_id=(
                    f"dataset:risk_event_occurrences:{report.as_of.isoformat()}"
                ),
                label="风险事件发生记录输入",
                dataset_type="risk_event_occurrence_store",
                path=(
                    report.risk_event_occurrence_review_report.validation_report.input_path
                ),
            )
        )
    if belief_state_path is not None:
        dataset_refs.append(
            _file_dataset_ref(
                dataset_id=f"dataset:belief_state:{report.as_of.isoformat()}",
                label="只读认知状态",
                dataset_type="processed_belief_state",
                path=belief_state_path,
            )
        )
    dataset_refs.extend(_config_dataset_refs(config_paths))

    quality_refs = (
        _data_quality_ref(
            quality_id=f"quality:market_data:{report.as_of.isoformat()}",
            report_path=data_quality_report_path,
            data_quality_report=report.data_quality_report,
        ),
    )
    evidence_cards = (
        {
            "evidence_id": f"evidence:daily_score:{report.as_of.isoformat()}:position",
            "summary": "综合评分、仓位模型和 position_gate 共同支持最终 AI 仓位结论。",
            "signal_ids": [
                f"position_gate:{gate.gate_id}"
                for gate in report.recommendation.position_gates
            ],
            "ticker_ids": _daily_signal_tickers(report),
            "date_window": date_window,
            "dataset_ids": _dataset_ids(dataset_refs),
            "quality_ids": _quality_ids(quality_refs),
            "config_ids": sorted(config_paths),
            "artifact_paths": _artifact_paths(
                report_path,
                feature_report_path,
                scores_path,
                sec_metrics_validation_report_path,
                sec_fundamental_feature_report_path,
                risk_event_occurrence_report_path,
            ),
        },
        {
            "evidence_id": f"evidence:daily_score:{report.as_of.isoformat()}:components",
            "summary": "各评分模块的分数、覆盖率、来源类型和底层信号。",
            "signal_ids": _daily_signal_ids(report),
            "ticker_ids": _daily_signal_tickers(report),
            "date_window": date_window,
            "dataset_ids": _dataset_ids(dataset_refs),
            "quality_ids": _quality_ids(quality_refs),
            "config_ids": sorted(config_paths),
            "artifact_paths": _artifact_paths(report_path, scores_path),
        },
        {
            "evidence_id": f"evidence:daily_score:{report.as_of.isoformat()}:quality",
            "summary": "市场数据质量门禁和上游质量报告。",
            "signal_ids": ["data_quality_gate"],
            "ticker_ids": list(report.data_quality_report.expected_price_tickers),
            "date_window": date_window,
            "dataset_ids": ["dataset:prices_daily", "dataset:rates_daily"],
            "quality_ids": _quality_ids(quality_refs),
            "config_ids": ["data_quality"],
            "artifact_paths": _artifact_paths(data_quality_report_path),
        },
    )
    if belief_state_path is not None:
        belief_evidence_id = f"evidence:daily_score:{report.as_of.isoformat()}:belief_state"
        evidence_cards = (
            *evidence_cards,
            {
                "evidence_id": belief_evidence_id,
                "summary": (
                    "只读 belief_state 汇总市场、产业链、估值、风险、thesis "
                    "和仓位边界状态。"
                ),
                "signal_ids": [
                    "belief_state:read_only",
                    *[
                        f"position_gate:{gate.gate_id}"
                        for gate in report.recommendation.position_gates
                    ],
                ],
                "ticker_ids": _daily_signal_tickers(report),
                "date_window": date_window,
                "dataset_ids": [f"dataset:belief_state:{report.as_of.isoformat()}"],
                "quality_ids": _quality_ids(quality_refs),
                "config_ids": sorted(config_paths),
                "artifact_paths": _artifact_paths(belief_state_path),
            },
        )
    claims = (
        {
            "claim_id": f"daily_score:{report.as_of.isoformat()}:overall_position",
            "statement": (
                "最终 AI 仓位为 "
                f"{report.recommendation.risk_asset_ai_band.min_position:.0%}-"
                f"{report.recommendation.risk_asset_ai_band.max_position:.0%}，"
                f"仓位状态为 {report.recommendation.label}。"
            ),
            "report_section": "顶部摘要 / 仓位闸门",
            "evidence_ids": [evidence_cards[0]["evidence_id"]],
            "dataset_ids": _dataset_ids(dataset_refs),
            "quality_ids": _quality_ids(quality_refs),
        },
        {
            "claim_id": f"daily_score:{report.as_of.isoformat()}:data_quality",
            "statement": f"市场数据质量门禁状态为 {report.data_quality_report.status}。",
            "report_section": "数据门禁",
            "evidence_ids": [evidence_cards[2]["evidence_id"]],
            "dataset_ids": ["dataset:prices_daily", "dataset:rates_daily"],
            "quality_ids": _quality_ids(quality_refs),
        },
        {
            "claim_id": f"daily_score:{report.as_of.isoformat()}:component_scores",
            "statement": "日报模块评分来自市场特征、基本面、估值和风险事件输入。",
            "report_section": "模块评分 / 硬数据信号",
            "evidence_ids": [evidence_cards[1]["evidence_id"]],
            "dataset_ids": _dataset_ids(dataset_refs),
            "quality_ids": _quality_ids(quality_refs),
        },
    )
    if belief_state_path is not None:
        claims = (
            *claims,
            {
                "claim_id": f"daily_score:{report.as_of.isoformat()}:belief_state",
                "statement": "日报生成只读 belief_state；该状态不直接改变评分、闸门或仓位。",
                "report_section": "认知状态",
                "evidence_ids": [
                    f"evidence:daily_score:{report.as_of.isoformat()}:belief_state"
                ],
                "dataset_ids": [f"dataset:belief_state:{report.as_of.isoformat()}"],
                "quality_ids": _quality_ids(quality_refs),
            },
        )
    run_manifest = _run_manifest(
        run_id=f"run:daily_score:{report.as_of.isoformat()}",
        command="aits score-daily",
        date_window=date_window,
        market_regime=market_regime,
        config_paths=config_paths,
        output_artifacts=_artifact_paths(
            report_path,
            data_quality_report_path,
            feature_report_path,
            features_path,
            scores_path,
            sec_metrics_validation_report_path,
            sec_fundamental_feature_report_path,
            sec_fundamental_features_path,
            risk_event_occurrence_report_path,
            belief_state_path,
        ),
    )
    return ReportTraceBundle(
        schema_version=SCHEMA_VERSION,
        report_id=report_id,
        report_type="daily_score",
        generated_at=datetime.now(tz=UTC),
        report_path=report_path,
        run_manifest=run_manifest,
        quality_refs=quality_refs,
        dataset_refs=tuple(dataset_refs),
        evidence_cards=evidence_cards,
        claims=claims,
    )


def build_backtest_trace_bundle(
    *,
    result: DailyBacktestResult,
    audit_report: BacktestAuditReport,
    report_path: Path,
    data_quality_report_path: Path,
    daily_output_path: Path,
    input_coverage_output_path: Path,
    audit_report_path: Path,
    config_paths: dict[str, Path],
    sec_companyfacts_validation_report_path: Path | None = None,
) -> ReportTraceBundle:
    report_id = (
        f"backtest:{result.requested_start.isoformat()}:{result.requested_end.isoformat()}"
    )
    date_window = _date_window(result.requested_start, result.requested_end)
    manifest_records = _download_manifest_records(result.data_quality_report)
    dataset_refs: list[TraceRecord] = [
        _dataset_ref_from_summary(
            dataset_id="dataset:prices_daily",
            label="价格日线缓存",
            dataset_type="raw_market_data",
            summary=result.data_quality_report.price_summary,
            manifest_records=manifest_records,
        ),
        _dataset_ref_from_summary(
            dataset_id="dataset:rates_daily",
            label="利率日线缓存",
            dataset_type="raw_macro_data",
            summary=result.data_quality_report.rate_summary,
            manifest_records=manifest_records,
        ),
        _file_dataset_ref(
            dataset_id=(
                f"dataset:backtest_daily:{result.requested_start.isoformat()}:"
                f"{result.requested_end.isoformat()}"
            ),
            label="回测每日明细",
            dataset_type="backtest_daily_output",
            path=daily_output_path,
        ),
        _file_dataset_ref(
            dataset_id=(
                f"dataset:backtest_input_coverage:{result.requested_start.isoformat()}:"
                f"{result.requested_end.isoformat()}"
            ),
            label="回测输入覆盖诊断",
            dataset_type="backtest_input_coverage",
            path=input_coverage_output_path,
        ),
    ]
    if manifest_ref := _optional_manifest_dataset_ref(result.data_quality_report):
        dataset_refs.append(manifest_ref)
    dataset_refs.extend(_config_dataset_refs(config_paths))

    quality_refs = (
        _data_quality_ref(
            quality_id=f"quality:market_data:{result.requested_end.isoformat()}",
            report_path=data_quality_report_path,
            data_quality_report=result.data_quality_report,
        ),
        {
            "quality_id": (
                f"quality:backtest_input_audit:{result.requested_start.isoformat()}:"
                f"{result.requested_end.isoformat()}"
            ),
            "label": "回测输入审计",
            "status": audit_report.status,
            "report_path": str(audit_report_path),
            "error_count": audit_report.error_count,
            "warning_count": audit_report.warning_count,
        },
    )
    evidence_cards = (
        {
            "evidence_id": (
                f"evidence:backtest:{result.requested_start.isoformat()}:"
                f"{result.requested_end.isoformat()}:performance"
            ),
            "summary": "动态仓位策略绩效、执行成本和基准对比。",
            "signal_ids": ["strategy_return", "max_drawdown", "benchmark_comparison"],
            "ticker_ids": [result.strategy_ticker, *result.benchmark_tickers],
            "date_window": date_window,
            "dataset_ids": _dataset_ids(dataset_refs),
            "quality_ids": _quality_ids(quality_refs),
            "config_ids": sorted(config_paths),
            "artifact_paths": _artifact_paths(report_path, daily_output_path),
        },
        {
            "evidence_id": (
                f"evidence:backtest:{result.requested_start.isoformat()}:"
                f"{result.requested_end.isoformat()}:input_coverage"
            ),
            "summary": "point-in-time 输入覆盖率、来源类型、输入问题和证据 URL。",
            "signal_ids": [
                "component_coverage",
                "input_source_url",
                "risk_event_evidence_url",
            ],
            "ticker_ids": [result.strategy_ticker, *result.benchmark_tickers],
            "date_window": date_window,
            "dataset_ids": _dataset_ids(dataset_refs),
            "quality_ids": _quality_ids(quality_refs),
            "config_ids": sorted(config_paths),
            "artifact_paths": _artifact_paths(
                input_coverage_output_path,
                audit_report_path,
                sec_companyfacts_validation_report_path,
            ),
        },
        {
            "evidence_id": (
                f"evidence:backtest:{result.requested_start.isoformat()}:"
                f"{result.requested_end.isoformat()}:quality"
            ),
            "summary": "市场数据质量门禁和回测输入审计结果。",
            "signal_ids": ["data_quality_gate", "backtest_input_audit"],
            "ticker_ids": list(result.data_quality_report.expected_price_tickers),
            "date_window": date_window,
            "dataset_ids": ["dataset:prices_daily", "dataset:rates_daily"],
            "quality_ids": _quality_ids(quality_refs),
            "config_ids": ["data_quality"],
            "artifact_paths": _artifact_paths(data_quality_report_path, audit_report_path),
        },
    )
    claims = (
        {
            "claim_id": (
                f"backtest:{result.requested_start.isoformat()}:"
                f"{result.requested_end.isoformat()}:performance"
            ),
            "statement": (
                f"策略总收益 {result.strategy_metrics.total_return:.1%}，"
                f"最大回撤 {result.strategy_metrics.max_drawdown:.1%}。"
            ),
            "report_section": "核心指标 / 执行成本摘要",
            "evidence_ids": [evidence_cards[0]["evidence_id"]],
            "dataset_ids": _dataset_ids(dataset_refs),
            "quality_ids": _quality_ids(quality_refs),
        },
        {
            "claim_id": (
                f"backtest:{result.requested_start.isoformat()}:"
                f"{result.requested_end.isoformat()}:data_quality"
            ),
            "statement": f"市场数据质量门禁状态为 {result.data_quality_report.status}。",
            "report_section": "数据质量门禁摘要",
            "evidence_ids": [evidence_cards[2]["evidence_id"]],
            "dataset_ids": ["dataset:prices_daily", "dataset:rates_daily"],
            "quality_ids": _quality_ids(quality_refs),
        },
        {
            "claim_id": (
                f"backtest:{result.requested_start.isoformat()}:"
                f"{result.requested_end.isoformat()}:input_coverage"
            ),
            "statement": "回测输入覆盖诊断记录 point-in-time 输入覆盖和来源下钻。",
            "report_section": "模块覆盖率摘要 / 输入覆盖诊断 / 输入审计报告",
            "evidence_ids": [evidence_cards[1]["evidence_id"]],
            "dataset_ids": _dataset_ids(dataset_refs),
            "quality_ids": _quality_ids(quality_refs),
        },
    )
    run_manifest = _run_manifest(
        run_id=(
            f"run:backtest:{result.requested_start.isoformat()}:"
            f"{result.requested_end.isoformat()}"
        ),
        command="aits backtest",
        date_window=date_window,
        market_regime=result.market_regime,
        config_paths=config_paths,
        output_artifacts=_artifact_paths(
            report_path,
            daily_output_path,
            input_coverage_output_path,
            audit_report_path,
            data_quality_report_path,
            sec_companyfacts_validation_report_path,
        ),
    )
    return ReportTraceBundle(
        schema_version=SCHEMA_VERSION,
        report_id=report_id,
        report_type="backtest",
        generated_at=datetime.now(tz=UTC),
        report_path=report_path,
        run_manifest=run_manifest,
        quality_refs=quality_refs,
        dataset_refs=tuple(dataset_refs),
        evidence_cards=evidence_cards,
        claims=claims,
    )


def write_trace_bundle(bundle: ReportTraceBundle, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(bundle.to_dict(), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def render_traceability_section(bundle: ReportTraceBundle, bundle_path: Path) -> str:
    lines = [
        "",
        "## 可追溯引用",
        "",
        f"- Trace Bundle：`{bundle_path}`",
        f"- Report ID：`{bundle.report_id}`",
        f"- Run Manifest：`{bundle.run_manifest['run_id']}`",
        (
            "- 反查命令："
            f"`aits trace lookup --bundle-path {bundle_path} --id "
            f"{bundle.claims[0]['claim_id']}`"
        ),
        "",
        "| Claim | Evidence | Dataset | Quality | 结论 |",
        "|---|---|---|---|---|",
    ]
    for claim in bundle.claims:
        lines.append(
            "| "
            f"`{claim['claim_id']}` | "
            f"{_inline_ids(claim['evidence_ids'])} | "
            f"{_inline_ids(claim['dataset_ids'])} | "
            f"{_inline_ids(claim['quality_ids'])} | "
            f"{_escape_markdown_table(str(claim['statement']))} |"
        )
    return "\n".join(lines)


def lookup_trace_record(bundle_path: Path, object_id: str) -> tuple[str, TraceRecord]:
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    run_manifest = _dict_value(bundle, "run_manifest")
    if run_manifest.get("run_id") == object_id:
        return ("run_manifest", run_manifest)

    collections = (
        ("claim", "claims", "claim_id"),
        ("evidence", "evidence_cards", "evidence_id"),
        ("dataset", "dataset_refs", "dataset_id"),
        ("quality", "quality_refs", "quality_id"),
    )
    for record_type, collection_key, id_key in collections:
        for record in _list_value(bundle, collection_key):
            if isinstance(record, dict) and record.get(id_key) == object_id:
                return (record_type, record)
    raise KeyError(f"trace id not found: {object_id}")


def render_trace_lookup(record_type: str, record: TraceRecord) -> str:
    lines = [
        "# Trace Lookup",
        "",
        f"- 类型：{record_type}",
        f"- ID：`{_record_id(record_type, record)}`",
    ]
    if "statement" in record:
        lines.append(f"- 结论：{record['statement']}")
    if "summary" in record:
        lines.append(f"- 摘要：{record['summary']}")
    if "status" in record:
        lines.append(f"- 状态：{record['status']}")
    if "path" in record:
        lines.append(f"- 路径：`{record['path']}`")
    if "report_path" in record:
        lines.append(f"- 报告：`{record['report_path']}`")
    if "date_window" in record:
        lines.append(f"- 日期窗口：{record['date_window']}")

    for key, label in (
        ("evidence_ids", "Evidence"),
        ("dataset_ids", "Dataset"),
        ("quality_ids", "Quality"),
        ("config_ids", "Config"),
        ("artifact_paths", "Artifacts"),
    ):
        if values := record.get(key):
            lines.append(f"- {label}：{_inline_ids(values)}")

    lines.extend(["", "```json", json.dumps(record, ensure_ascii=False, indent=2), "```"])
    return "\n".join(lines) + "\n"


def _data_quality_ref(
    *,
    quality_id: str,
    report_path: Path,
    data_quality_report: DataQualityReport,
) -> TraceRecord:
    return {
        "quality_id": quality_id,
        "label": "市场数据质量门禁",
        "status": data_quality_report.status,
        "report_path": str(report_path),
        "as_of": data_quality_report.as_of.isoformat(),
        "error_count": data_quality_report.error_count,
        "warning_count": data_quality_report.warning_count,
        "price_dataset_id": "dataset:prices_daily",
        "rate_dataset_id": "dataset:rates_daily",
    }


def _dataset_ref_from_summary(
    *,
    dataset_id: str,
    label: str,
    dataset_type: str,
    summary: DataFileSummary,
    manifest_records: tuple[TraceRecord, ...],
) -> TraceRecord:
    record = {
        "dataset_id": dataset_id,
        "label": label,
        "dataset_type": dataset_type,
        "path": str(summary.path),
        "row_count": summary.rows if summary.exists else 0,
        "checksum_sha256": summary.sha256,
        "date_range": {
            "start": summary.min_date.isoformat() if summary.min_date else None,
            "end": summary.max_date.isoformat() if summary.max_date else None,
        },
    }
    manifest_record = _manifest_record_for_path(summary.path, manifest_records)
    if manifest_record:
        record.update(
            {
                "provider": manifest_record.get("provider"),
                "endpoint": manifest_record.get("endpoint"),
                "request_params": manifest_record.get("request_parameters"),
                "downloaded_at": manifest_record.get("downloaded_at"),
                "source_id": manifest_record.get("source_id"),
            }
        )
    return record


def _optional_manifest_dataset_ref(
    data_quality_report: DataQualityReport,
) -> TraceRecord | None:
    if data_quality_report.manifest_summary is None:
        return None
    return _dataset_ref_from_summary(
        dataset_id="dataset:download_manifest",
        label="下载审计 manifest",
        dataset_type="download_manifest",
        summary=data_quality_report.manifest_summary,
        manifest_records=(),
    )


def _file_dataset_ref(
    *,
    dataset_id: str,
    label: str,
    dataset_type: str,
    path: Path,
) -> TraceRecord:
    record = _path_dataset_ref(
        dataset_id=dataset_id,
        label=label,
        dataset_type=dataset_type,
        path=path,
    )
    if path.exists() and path.is_file() and path.suffix.lower() == ".csv":
        record["row_count"] = _csv_row_count(path)
    return record


def _path_dataset_ref(
    *,
    dataset_id: str,
    label: str,
    dataset_type: str,
    path: Path,
) -> TraceRecord:
    record: TraceRecord = {
        "dataset_id": dataset_id,
        "label": label,
        "dataset_type": dataset_type,
        "path": str(path),
        "exists": path.exists(),
    }
    if path.exists() and path.is_file():
        record["checksum_sha256"] = _sha256_file(path)
    elif path.exists() and path.is_dir():
        record["file_count"] = sum(1 for child in path.rglob("*") if child.is_file())
    return record


def _config_dataset_refs(config_paths: dict[str, Path]) -> list[TraceRecord]:
    return [
        _path_dataset_ref(
            dataset_id=f"config:{config_id}",
            label=f"配置：{config_id}",
            dataset_type="configuration",
            path=config_path,
        )
        for config_id, config_path in sorted(config_paths.items())
    ]


def _download_manifest_records(report: DataQualityReport) -> tuple[TraceRecord, ...]:
    if report.manifest_summary is None or not report.manifest_summary.path.exists():
        return ()
    try:
        frame = pd.read_csv(report.manifest_summary.path)
    except (OSError, pd.errors.ParserError):
        return ()

    records: list[TraceRecord] = []
    for raw_record in frame.to_dict(orient="records"):
        record = {str(key): value for key, value in raw_record.items()}
        params = record.get("request_parameters")
        if isinstance(params, str) and params.strip():
            try:
                record["request_parameters"] = json.loads(params)
            except json.JSONDecodeError:
                record["request_parameters"] = params
        records.append(record)
    return tuple(records)


def _manifest_record_for_path(
    path: Path,
    manifest_records: tuple[TraceRecord, ...],
) -> TraceRecord | None:
    if not manifest_records:
        return None
    target = _normalized_path_text(path)
    target_name = path.name.lower()
    for record in reversed(manifest_records):
        output_path = record.get("output_path")
        if not isinstance(output_path, str):
            continue
        candidate = _normalized_path_text(Path(output_path))
        if candidate == target or Path(output_path).name.lower() == target_name:
            return record
    return None


def _run_manifest(
    *,
    run_id: str,
    command: str,
    date_window: TraceRecord,
    market_regime: BacktestRegimeContext | None,
    config_paths: dict[str, Path],
    output_artifacts: list[str],
) -> TraceRecord:
    return {
        "run_id": run_id,
        "command": command,
        "date_window": date_window,
        "market_regime": _market_regime_record(market_regime),
        "config_ids": sorted(config_paths),
        "config_paths": {key: str(path) for key, path in sorted(config_paths.items())},
        "output_artifacts": output_artifacts,
    }


def _market_regime_record(market_regime: BacktestRegimeContext | None) -> TraceRecord:
    if market_regime is None:
        return {
            "regime_id": "ai_after_chatgpt",
            "name": "默认 AI regime",
            "anchor_date": "2022-11-30",
            "anchor_event": "ChatGPT public launch",
            "start_date": "2022-12-01",
            "source": "config/market_regimes.yaml default_backtest_regime",
        }
    return {
        "regime_id": market_regime.regime_id,
        "name": market_regime.name,
        "anchor_date": market_regime.anchor_date.isoformat(),
        "anchor_event": market_regime.anchor_event,
        "start_date": market_regime.start_date.isoformat(),
        "description": market_regime.description,
    }


def _date_window(start: date, end: date) -> TraceRecord:
    return {"start": start.isoformat(), "end": end.isoformat()}


def _daily_signal_ids(report: DailyScoreReport) -> list[str]:
    return [
        f"{component.name}:{signal.subject}:{signal.feature}"
        for component in report.components
        for signal in component.signals
    ]


def _daily_signal_tickers(report: DailyScoreReport) -> list[str]:
    subjects = {
        signal.subject
        for component in report.components
        for signal in component.signals
        if signal.subject not in {"AI_CORE_MEDIAN", "AI_CORE", "POLICY_GEOPOLITICS"}
    }
    return sorted(subjects)


def _dataset_ids(dataset_refs: list[TraceRecord] | tuple[TraceRecord, ...]) -> list[str]:
    return [str(record["dataset_id"]) for record in dataset_refs]


def _quality_ids(quality_refs: tuple[TraceRecord, ...]) -> list[str]:
    return [str(record["quality_id"]) for record in quality_refs]


def _artifact_paths(*paths: Path | None) -> list[str]:
    return [str(path) for path in paths if path is not None]


def _inline_ids(values: object) -> str:
    if not isinstance(values, list | tuple):
        return f"`{values}`"
    return "<br/>".join(f"`{value}`" for value in values)


def _csv_row_count(path: Path) -> int | None:
    try:
        return len(pd.read_csv(path))
    except (OSError, pd.errors.ParserError):
        return None


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normalized_path_text(path: Path) -> str:
    try:
        return str(path.resolve()).lower()
    except OSError:
        return str(path).lower()


def _dict_value(record: TraceRecord, key: str) -> TraceRecord:
    value = record.get(key)
    if isinstance(value, dict):
        return value
    return {}


def _list_value(record: TraceRecord, key: str) -> list[object]:
    value = record.get(key)
    if isinstance(value, list):
        return value
    return []


def _record_id(record_type: str, record: TraceRecord) -> str:
    key_by_type = {
        "run_manifest": "run_id",
        "claim": "claim_id",
        "evidence": "evidence_id",
        "dataset": "dataset_id",
        "quality": "quality_id",
    }
    key = key_by_type.get(record_type, "id")
    return str(record.get(key, ""))


def _escape_markdown_table(value: str) -> str:
    return value.replace("\n", " ").replace("|", "\\|")
