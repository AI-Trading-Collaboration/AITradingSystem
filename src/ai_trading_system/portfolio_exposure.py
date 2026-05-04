from __future__ import annotations

import csv
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, ValidationError

from ai_trading_system.config import IndustryChainConfig, WatchlistConfig

PortfolioExposureSeverity = Literal["ERROR", "WARNING"]

REQUIRED_COLUMNS = (
    "as_of",
    "ticker",
    "instrument_type",
    "quantity",
    "market_value",
    "currency",
    "ai_exposure_pct",
)
OPTIONAL_COLUMNS = (
    "region",
    "customer_chain",
    "factor_tags",
    "correlation_cluster",
    "etf_beta_to_ai_proxy",
    "notes",
)


class PortfolioPosition(BaseModel):
    as_of: date
    ticker: str = Field(min_length=1)
    instrument_type: Literal["single_stock", "etf", "cash", "other"]
    quantity: float
    market_value: float = Field(ge=0)
    currency: str = Field(min_length=1)
    ai_exposure_pct: float = Field(ge=0, le=1)
    region: str = ""
    customer_chain: str = ""
    factor_tags: str = ""
    correlation_cluster: str = ""
    etf_beta_to_ai_proxy: float | None = None
    notes: str = ""


@dataclass(frozen=True)
class PortfolioExposureIssue:
    severity: PortfolioExposureSeverity
    code: str
    message: str
    ticker: str | None = None
    row_number: int | None = None


@dataclass(frozen=True)
class PortfolioExposureBucket:
    name: str
    market_value: float
    ai_market_value: float
    share_of_ai: float
    members: tuple[str, ...]


@dataclass(frozen=True)
class PortfolioExposureReport:
    as_of: date
    input_path: Path
    snapshot_date: date | None
    positions: tuple[PortfolioPosition, ...]
    selected_positions: tuple[PortfolioPosition, ...]
    ticker_exposures: tuple[PortfolioExposureBucket, ...]
    node_exposures: tuple[PortfolioExposureBucket, ...]
    region_exposures: tuple[PortfolioExposureBucket, ...]
    customer_chain_exposures: tuple[PortfolioExposureBucket, ...]
    factor_exposures: tuple[PortfolioExposureBucket, ...]
    correlation_cluster_exposures: tuple[PortfolioExposureBucket, ...]
    issues: tuple[PortfolioExposureIssue, ...] = field(default_factory=tuple)

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == "ERROR")

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == "WARNING")

    @property
    def passed(self) -> bool:
        return self.error_count == 0

    @property
    def status(self) -> str:
        if self.error_count:
            return "FAIL"
        if self.snapshot_date is None:
            return "NOT_CONNECTED"
        if self.warning_count:
            return "PASS_WITH_WARNINGS"
        return "PASS"

    @property
    def total_market_value(self) -> float:
        return sum(position.market_value for position in self.selected_positions)

    @property
    def ai_market_value(self) -> float:
        return sum(
            position.market_value * position.ai_exposure_pct
            for position in self.selected_positions
        )

    @property
    def ai_exposure_pct_total(self) -> float:
        if self.total_market_value <= 0:
            return 0.0
        return self.ai_market_value / self.total_market_value

    @property
    def max_single_ticker_share_of_ai(self) -> float:
        if not self.ticker_exposures:
            return 0.0
        return max(bucket.share_of_ai for bucket in self.ticker_exposures)

    @property
    def etf_beta_coverage(self) -> float:
        etf_ai_value = sum(
            position.market_value * position.ai_exposure_pct
            for position in self.selected_positions
            if position.instrument_type == "etf"
        )
        if etf_ai_value <= 0:
            return 1.0
        covered = sum(
            position.market_value * position.ai_exposure_pct
            for position in self.selected_positions
            if position.instrument_type == "etf"
            and position.etf_beta_to_ai_proxy is not None
        )
        return covered / etf_ai_value


def build_portfolio_exposure_report(
    *,
    input_path: Path,
    as_of: date,
    industry_chain: IndustryChainConfig,
    watchlist: WatchlistConfig,
) -> PortfolioExposureReport:
    if not input_path.exists():
        return _empty_report(
            input_path=input_path,
            as_of=as_of,
            issue=PortfolioExposureIssue(
                severity="WARNING",
                code="portfolio_positions_missing",
                message="未发现真实持仓文件；不能把观察池或模型建议仓位当作账户持仓。",
            ),
        )

    positions, issues = _load_positions(input_path)
    if any(issue.severity == "ERROR" for issue in issues):
        return PortfolioExposureReport(
            as_of=as_of,
            input_path=input_path,
            snapshot_date=None,
            positions=tuple(positions),
            selected_positions=(),
            ticker_exposures=(),
            node_exposures=(),
            region_exposures=(),
            customer_chain_exposures=(),
            factor_exposures=(),
            correlation_cluster_exposures=(),
            issues=tuple(issues),
        )

    visible_dates = sorted({position.as_of for position in positions if position.as_of <= as_of})
    if not visible_dates:
        return PortfolioExposureReport(
            as_of=as_of,
            input_path=input_path,
            snapshot_date=None,
            positions=tuple(positions),
            selected_positions=(),
            ticker_exposures=(),
            node_exposures=(),
            region_exposures=(),
            customer_chain_exposures=(),
            factor_exposures=(),
            correlation_cluster_exposures=(),
            issues=(
                *issues,
                PortfolioExposureIssue(
                    severity="WARNING",
                    code="portfolio_positions_no_visible_snapshot",
                    message="持仓文件中没有 as_of 不晚于评估日期的快照。",
                ),
            ),
        )

    snapshot_date = visible_dates[-1]
    selected_positions = tuple(
        position for position in positions if position.as_of == snapshot_date
    )
    total_market_value = sum(position.market_value for position in selected_positions)
    report_issues = list(issues)
    if total_market_value <= 0:
        report_issues.append(
            PortfolioExposureIssue(
                severity="ERROR",
                code="portfolio_total_market_value_non_positive",
                message="持仓快照总市值必须大于 0。",
            )
        )
    for position in selected_positions:
        if position.currency.upper() != "USD":
            report_issues.append(
                PortfolioExposureIssue(
                    severity="WARNING",
                    code="portfolio_non_usd_position",
                    ticker=position.ticker,
                    message="持仓文件包含非 USD 货币；当前基础版不会做 FX 换算。",
                )
            )

    node_map = _ticker_node_map(industry_chain=industry_chain, watchlist=watchlist)
    unmapped = [
        position.ticker
        for position in selected_positions
        if position.ai_exposure_pct > 0 and not node_map.get(position.ticker.upper())
    ]
    if unmapped:
        report_issues.append(
            PortfolioExposureIssue(
                severity="WARNING",
                code="portfolio_ai_position_missing_node_mapping",
                message="部分 AI 暴露缺少产业链节点映射："
                f"{', '.join(sorted(set(unmapped)))}。",
            )
        )

    return PortfolioExposureReport(
        as_of=as_of,
        input_path=input_path,
        snapshot_date=snapshot_date,
        positions=tuple(positions),
        selected_positions=selected_positions,
        ticker_exposures=_ticker_buckets(selected_positions),
        node_exposures=_node_buckets(selected_positions, node_map),
        region_exposures=_metadata_buckets(selected_positions, "region", "未标注地区"),
        customer_chain_exposures=_metadata_buckets(
            selected_positions,
            "customer_chain",
            "未标注客户链",
        ),
        factor_exposures=_factor_buckets(selected_positions),
        correlation_cluster_exposures=_metadata_buckets(
            selected_positions,
            "correlation_cluster",
            "未标注相关性簇",
        ),
        issues=tuple(report_issues),
    )


def render_portfolio_exposure_report(report: PortfolioExposureReport) -> str:
    return _render_portfolio_exposure(report, include_title=True)


def render_portfolio_exposure_section(report: PortfolioExposureReport) -> str:
    return _render_portfolio_exposure(report, include_title=False)


def write_portfolio_exposure_report(
    report: PortfolioExposureReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_portfolio_exposure_report(report), encoding="utf-8")
    return output_path


def default_portfolio_exposure_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"portfolio_exposure_{as_of.isoformat()}.md"


def _load_positions(
    input_path: Path,
) -> tuple[list[PortfolioPosition], list[PortfolioExposureIssue]]:
    issues: list[PortfolioExposureIssue] = []
    positions: list[PortfolioPosition] = []
    with input_path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        columns = tuple(reader.fieldnames or ())
        missing_columns = [column for column in REQUIRED_COLUMNS if column not in columns]
        if missing_columns:
            return (
                [],
                [
                    PortfolioExposureIssue(
                        severity="ERROR",
                        code="portfolio_positions_missing_columns",
                        message="持仓 CSV 缺少必要字段："
                        f"{', '.join(missing_columns)}。",
                    )
                ],
            )
        for row_number, row in enumerate(reader, start=2):
            normalized = {column: (row.get(column) or "").strip() for column in columns}
            try:
                position = PortfolioPosition.model_validate(
                    {
                        "as_of": normalized["as_of"],
                        "ticker": normalized["ticker"].upper(),
                        "instrument_type": normalized["instrument_type"],
                        "quantity": _float_cell(normalized["quantity"]),
                        "market_value": _float_cell(normalized["market_value"]),
                        "currency": normalized["currency"].upper(),
                        "ai_exposure_pct": _float_cell(normalized["ai_exposure_pct"]),
                        "region": normalized.get("region", ""),
                        "customer_chain": normalized.get("customer_chain", ""),
                        "factor_tags": normalized.get("factor_tags", ""),
                        "correlation_cluster": normalized.get("correlation_cluster", ""),
                        "etf_beta_to_ai_proxy": _optional_float_cell(
                            normalized.get("etf_beta_to_ai_proxy", "")
                        ),
                        "notes": normalized.get("notes", ""),
                    }
                )
            except (ValueError, ValidationError) as exc:
                issues.append(
                    PortfolioExposureIssue(
                        severity="ERROR",
                        code="portfolio_position_row_invalid",
                        row_number=row_number,
                        ticker=normalized.get("ticker") or None,
                        message=f"持仓 CSV 第 {row_number} 行无法解析：{exc}",
                    )
                )
                continue
            positions.append(position)
    return positions, issues


def _empty_report(
    *,
    input_path: Path,
    as_of: date,
    issue: PortfolioExposureIssue,
) -> PortfolioExposureReport:
    return PortfolioExposureReport(
        as_of=as_of,
        input_path=input_path,
        snapshot_date=None,
        positions=(),
        selected_positions=(),
        ticker_exposures=(),
        node_exposures=(),
        region_exposures=(),
        customer_chain_exposures=(),
        factor_exposures=(),
        correlation_cluster_exposures=(),
        issues=(issue,),
    )


def _ticker_node_map(
    *,
    industry_chain: IndustryChainConfig,
    watchlist: WatchlistConfig,
) -> dict[str, tuple[str, ...]]:
    mapping: dict[str, set[str]] = {}
    for node in industry_chain.nodes:
        for ticker in node.related_tickers:
            mapping.setdefault(ticker.upper(), set()).add(node.node_id)
    for item in watchlist.items:
        if item.ai_chain_nodes:
            mapping[item.ticker.upper()] = set(item.ai_chain_nodes)
    return {ticker: tuple(sorted(nodes)) for ticker, nodes in mapping.items()}


def _ticker_buckets(
    positions: tuple[PortfolioPosition, ...],
) -> tuple[PortfolioExposureBucket, ...]:
    values: dict[str, float] = {}
    market_values: dict[str, float] = {}
    for position in positions:
        ai_value = position.market_value * position.ai_exposure_pct
        if ai_value <= 0:
            continue
        values[position.ticker] = values.get(position.ticker, 0.0) + ai_value
        market_values[position.ticker] = market_values.get(position.ticker, 0.0) + (
            position.market_value
        )
    return _buckets_from_values(values, market_values)


def _node_buckets(
    positions: tuple[PortfolioPosition, ...],
    node_map: dict[str, tuple[str, ...]],
) -> tuple[PortfolioExposureBucket, ...]:
    values: dict[str, float] = {}
    members: dict[str, set[str]] = {}
    for position in positions:
        ai_value = position.market_value * position.ai_exposure_pct
        if ai_value <= 0:
            continue
        nodes = node_map.get(position.ticker.upper(), ())
        if not nodes:
            nodes = ("未映射产业链节点",)
        allocated = ai_value / len(nodes)
        for node in nodes:
            values[node] = values.get(node, 0.0) + allocated
            members.setdefault(node, set()).add(position.ticker)
    return _buckets_from_values(values, values, members)


def _metadata_buckets(
    positions: tuple[PortfolioPosition, ...],
    attribute: str,
    missing_label: str,
) -> tuple[PortfolioExposureBucket, ...]:
    values: dict[str, float] = {}
    members: dict[str, set[str]] = {}
    for position in positions:
        ai_value = position.market_value * position.ai_exposure_pct
        if ai_value <= 0:
            continue
        key = str(getattr(position, attribute)).strip() or missing_label
        values[key] = values.get(key, 0.0) + ai_value
        members.setdefault(key, set()).add(position.ticker)
    return _buckets_from_values(values, values, members)


def _factor_buckets(
    positions: tuple[PortfolioPosition, ...],
) -> tuple[PortfolioExposureBucket, ...]:
    values: dict[str, float] = {}
    members: dict[str, set[str]] = {}
    for position in positions:
        ai_value = position.market_value * position.ai_exposure_pct
        if ai_value <= 0:
            continue
        factors = _split_tags(position.factor_tags) or ("未标注因子",)
        allocated = ai_value / len(factors)
        for factor in factors:
            values[factor] = values.get(factor, 0.0) + allocated
            members.setdefault(factor, set()).add(position.ticker)
    return _buckets_from_values(values, values, members)


def _buckets_from_values(
    ai_values: dict[str, float],
    market_values: dict[str, float],
    members: dict[str, set[str]] | None = None,
) -> tuple[PortfolioExposureBucket, ...]:
    total_ai = sum(ai_values.values())
    if total_ai <= 0:
        return ()
    buckets = [
        PortfolioExposureBucket(
            name=name,
            market_value=market_values.get(name, ai_value),
            ai_market_value=ai_value,
            share_of_ai=ai_value / total_ai,
            members=tuple(sorted((members or {}).get(name, {name}))),
        )
        for name, ai_value in ai_values.items()
    ]
    return tuple(sorted(buckets, key=lambda bucket: bucket.ai_market_value, reverse=True))


def _render_portfolio_exposure(
    report: PortfolioExposureReport,
    *,
    include_title: bool,
) -> str:
    snapshot_label = report.snapshot_date.isoformat() if report.snapshot_date else "未接入"
    lines: list[str] = []
    if include_title:
        lines.extend(["# 组合暴露分解", ""])
    else:
        lines.extend(["## 组合暴露", ""])
    lines.extend(
        [
            f"- 状态：{report.status}",
            f"- 评估日期：{report.as_of.isoformat()}",
            f"- 输入路径：`{report.input_path}`",
            f"- 持仓快照日期：{snapshot_label}",
            f"- 持仓行数：{len(report.selected_positions)}",
            f"- 总市值：{report.total_market_value:.2f}",
            f"- AI 名义暴露：{report.ai_market_value:.2f}",
            f"- AI 占总持仓：{report.ai_exposure_pct_total:.1%}",
            f"- 最大单票占 AI 暴露：{report.max_single_ticker_share_of_ai:.1%}",
            f"- ETF beta 覆盖率：{report.etf_beta_coverage:.1%}",
            "- 生产影响：none",
            "- 解释边界：本节只读取真实持仓文件并做只读暴露分解，"
            "不改变评分、仓位闸门、执行建议或回测仓位。",
            "",
        ]
    )
    if report.status == "NOT_CONNECTED":
        lines.append("未接入真实持仓文件；日报仓位仍只能解释为模型建议区间，不能直接转成账户买卖数量。")
        lines.append("")

    _append_bucket_table(lines, "### Ticker 暴露", report.ticker_exposures)
    _append_bucket_table(lines, "### 产业链节点暴露", report.node_exposures)
    _append_bucket_table(lines, "### 地区暴露", report.region_exposures)
    _append_bucket_table(lines, "### 客户链暴露", report.customer_chain_exposures)
    _append_bucket_table(lines, "### 因子暴露", report.factor_exposures)
    _append_bucket_table(lines, "### 相关性簇暴露", report.correlation_cluster_exposures)

    lines.extend(["### 问题", ""])
    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(["| 级别 | Code | Ticker | 行号 | 说明 |", "|---|---|---|---:|---|"])
        for issue in report.issues:
            lines.append(
                "| "
                f"{_severity_label(issue.severity)} | "
                f"{issue.code} | "
                f"{issue.ticker or ''} | "
                f"{issue.row_number or ''} | "
                f"{_escape_markdown_table(issue.message)} |"
            )
    return "\n".join(lines) + "\n"


def _append_bucket_table(
    lines: list[str],
    title: str,
    buckets: tuple[PortfolioExposureBucket, ...],
) -> None:
    lines.extend([title, ""])
    if not buckets:
        lines.extend(["无可展示暴露。", ""])
        return
    lines.extend(
        [
            "| 分组 | AI 暴露 | 占 AI 暴露 | 成员 |",
            "|---|---:|---:|---|",
        ]
    )
    for bucket in buckets:
        lines.append(
            "| "
            f"{_escape_markdown_table(bucket.name)} | "
            f"{bucket.ai_market_value:.2f} | "
            f"{bucket.share_of_ai:.1%} | "
            f"{_escape_markdown_table(', '.join(bucket.members))} |"
        )
    lines.append("")


def _float_cell(value: str) -> float:
    if not value:
        raise ValueError("empty numeric cell")
    return float(value)


def _optional_float_cell(value: str) -> float | None:
    if not value:
        return None
    return float(value)


def _split_tags(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in value.replace(";", ",").split(",") if item.strip())


def _severity_label(severity: PortfolioExposureSeverity) -> str:
    return "错误" if severity == "ERROR" else "警告"


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
