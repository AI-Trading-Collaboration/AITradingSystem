from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.fundamentals.sec_pit_aliases import (
    canonicalize_ticker_series,
    load_ticker_aliases,
)
from ai_trading_system.fundamentals.sec_pit_evaluation import DEFAULT_SEC_PIT_FEATURE_PANEL_PATH

SEC_PIT_BASELINE_COVERAGE_TASK_ID = "TRADING-045"
SEC_PIT_BASELINE_COVERAGE_REPORT_TYPE = "sec_pit_baseline_coverage"
DEFAULT_SEC_PIT_BASELINE_COVERAGE_OUTPUT_DIR = (
    PROJECT_ROOT / "outputs" / "sec_pit_baseline_coverage"
)
DEFAULT_SEC_PIT_BASELINE_SCORE_PATH = PROJECT_ROOT / "data" / "processed" / "scores_daily.csv"
LEGACY_SEC_PIT_FEATURE_PANEL_PATH = (
    PROJECT_ROOT / "data" / "processed" / "sec_pit" / "sec_pit_feature_panel.csv"
)

COVERAGE_STATUS_OK_MIN = 0.90
COVERAGE_STATUS_LIMITED_MIN = 0.50
LOW_COMPLETENESS_RATIO_MIN = 0.90

COVERAGE_STATUSES: tuple[str, ...] = (
    "OK",
    "LIMITED_COVERAGE",
    "INSUFFICIENT_COVERAGE",
    "MISSING_BASELINE",
    "FAILED_VALIDATION",
)

BASELINE_COVERAGE_BY_TICKER_COLUMNS: tuple[str, ...] = (
    "ticker",
    "expected_rows",
    "actual_rows",
    "missing_rows",
    "coverage_ratio",
    "score_completeness_avg",
    "first_available_date",
    "last_available_date",
    "first_missing_date",
    "last_missing_date",
    "coverage_status",
    "recommended_action",
)

BASELINE_COVERAGE_BY_DATE_COLUMNS: tuple[str, ...] = (
    "decision_date",
    "expected_ticker_count",
    "actual_ticker_count",
    "missing_ticker_count",
    "coverage_ratio",
    "score_completeness_avg",
    "coverage_status",
    "missing_tickers",
)

BASELINE_GAP_COLUMNS: tuple[str, ...] = (
    "decision_date",
    "ticker",
    "gap_type",
    "reason",
    "recommended_fix",
)


@dataclass(frozen=True)
class SecPitBaselineCoverageArtifacts:
    status: str
    summary_json_path: Path
    summary_markdown_path: Path
    by_ticker_path: Path
    by_date_path: Path
    gap_path: Path


def run_sec_pit_baseline_coverage_audit(
    *,
    start: date,
    end: date,
    baseline_score_path: Path = DEFAULT_SEC_PIT_BASELINE_SCORE_PATH,
    feature_panel_path: Path = DEFAULT_SEC_PIT_FEATURE_PANEL_PATH,
    output_dir: Path = DEFAULT_SEC_PIT_BASELINE_COVERAGE_OUTPUT_DIR,
) -> SecPitBaselineCoverageArtifacts:
    if start > end:
        raise ValueError("start must be on or before end")

    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = end.isoformat()
    summary_json_path = output_dir / f"sec_pit_baseline_coverage_summary_{suffix}.json"
    summary_markdown_path = output_dir / f"sec_pit_baseline_coverage_summary_{suffix}.md"
    by_ticker_path = output_dir / f"sec_pit_baseline_coverage_by_ticker_{suffix}.csv"
    by_date_path = output_dir / f"sec_pit_baseline_coverage_by_date_{suffix}.csv"
    gap_path = output_dir / f"sec_pit_baseline_gap_{suffix}.csv"

    limitations: list[str] = []
    try:
        expected = _expected_rows(_resolve_feature_panel_path(feature_panel_path), start, end)
        baseline = _baseline_rows(baseline_score_path, start, end)
        joined = _join_expected_baseline(expected, baseline)
        gaps = _gap_rows(joined)
        by_ticker = _coverage_by_ticker(joined)
        by_date = _coverage_by_date(joined)
        status = _overall_status(joined, baseline)
    except (KeyError, ValueError, pd.errors.ParserError) as exc:
        limitations.append(f"Input artifact validation failed: {exc}")
        expected = pd.DataFrame(columns=["decision_date", "ticker"])
        baseline = pd.DataFrame(columns=["decision_date", "ticker"])
        joined = pd.DataFrame(columns=["decision_date", "ticker", "has_baseline_row"])
        gaps = _empty_gaps()
        by_ticker = _empty_by_ticker()
        by_date = _empty_by_date()
        status = "FAILED_VALIDATION"

    gaps.to_csv(gap_path, index=False)
    by_ticker.to_csv(by_ticker_path, index=False)
    by_date.to_csv(by_date_path, index=False)
    summary = _summary_payload(
        status=status,
        start=start,
        end=end,
        baseline_score_path=baseline_score_path,
        expected=expected,
        baseline=baseline,
        joined=joined,
        limitations=limitations,
        artifacts={
            "summary_json": summary_json_path,
            "summary_markdown": summary_markdown_path,
            "by_ticker_csv": by_ticker_path,
            "by_date_csv": by_date_path,
            "gap_csv": gap_path,
        },
    )
    summary_json_path.write_text(
        json.dumps(_json_value(summary), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    summary_markdown_path.write_text(
        render_sec_pit_baseline_coverage_summary(summary, by_ticker, by_date, gaps),
        encoding="utf-8",
    )
    return SecPitBaselineCoverageArtifacts(
        status=status,
        summary_json_path=summary_json_path,
        summary_markdown_path=summary_markdown_path,
        by_ticker_path=by_ticker_path,
        by_date_path=by_date_path,
        gap_path=gap_path,
    )


def render_sec_pit_baseline_coverage_summary(
    summary: dict[str, Any],
    by_ticker: pd.DataFrame,
    by_date: pd.DataFrame,
    gaps: pd.DataFrame,
) -> str:
    worst_tickers = (
        by_ticker.sort_values(["coverage_ratio", "ticker"], ascending=[True, True]).head(10)
        if not by_ticker.empty
        else pd.DataFrame()
    )
    worst_dates = (
        by_date.sort_values(["coverage_ratio", "decision_date"], ascending=[True, True]).head(10)
        if not by_date.empty
        else pd.DataFrame()
    )
    lines = [
        "# SEC PIT Baseline Coverage Summary",
        "",
        "## Metadata",
        f"- generated_at: {summary.get('generated_at', '')}",
        f"- start_date: {summary.get('start_date', '')}",
        f"- end_date: {summary.get('end_date', '')}",
        f"- baseline_score_path: {summary.get('baseline_score_path', '')}",
        f"- coverage_status: {summary.get('coverage_status', '')}",
        "",
        "## Coverage",
        f"- expected_rows: {summary.get('expected_rows', 0)}",
        f"- actual_rows: {summary.get('actual_rows', 0)}",
        f"- missing_rows: {summary.get('missing_rows', 0)}",
        f"- coverage_ratio: {_format_ratio(summary.get('coverage_ratio'))}",
        f"- score_completeness_avg: {_format_ratio(summary.get('score_completeness_avg'))}",
        f"- recommended_action: {summary.get('recommended_action', '')}",
        "",
        "## Worst Tickers",
        "| ticker | expected | actual | coverage | completeness | status |",
        "|---|---:|---:|---:|---:|---|",
    ]
    if worst_tickers.empty:
        lines.append("| none | 0 | 0 |  |  | MISSING |")
    else:
        for row in worst_tickers.to_dict(orient="records"):
            lines.append(
                "| "
                f"{row.get('ticker', '')} | "
                f"{row.get('expected_rows', 0)} | "
                f"{row.get('actual_rows', 0)} | "
                f"{_format_ratio(row.get('coverage_ratio'))} | "
                f"{_format_ratio(row.get('score_completeness_avg'))} | "
                f"{row.get('coverage_status', '')} |"
            )
    lines.extend(
        [
            "",
            "## Worst Dates",
            "| decision_date | expected tickers | actual tickers | coverage | missing tickers |",
            "|---|---:|---:|---:|---|",
        ]
    )
    if worst_dates.empty:
        lines.append("| none | 0 | 0 |  |  |")
    else:
        for row in worst_dates.to_dict(orient="records"):
            lines.append(
                "| "
                f"{row.get('decision_date', '')} | "
                f"{row.get('expected_ticker_count', 0)} | "
                f"{row.get('actual_ticker_count', 0)} | "
                f"{_format_ratio(row.get('coverage_ratio'))} | "
                f"{row.get('missing_tickers', '')} |"
            )
    lines.extend(
        [
            "",
            "## Gap Summary",
            f"- gap rows: {len(gaps)}",
            "- 输出只用于 SEC PIT shadow monitoring 覆盖诊断，不修改 production scoring。",
            "",
        ]
    )
    return "\n".join(lines)


def _expected_rows(path: Path, start: date, end: date) -> pd.DataFrame:
    if not path.exists():
        raise ValueError(f"feature panel not found: {path}")
    frame = pd.read_csv(path)
    required = {"decision_date", "ticker"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"feature panel is missing required columns: {', '.join(missing)}")
    aliases = load_ticker_aliases()
    normalized = frame.copy()
    normalized["decision_date"] = pd.to_datetime(
        normalized["decision_date"],
        errors="coerce",
    ).dt.date
    normalized["ticker"] = canonicalize_ticker_series(normalized["ticker"], aliases=aliases)
    normalized = normalized.loc[
        normalized["decision_date"].notna()
        & normalized["decision_date"].map(lambda value: start <= value <= end)
        & normalized["ticker"].fillna("").astype(str).str.strip().astype(bool)
    ].copy()
    return (
        normalized.loc[:, ["decision_date", "ticker"]]
        .drop_duplicates()
        .sort_values(["decision_date", "ticker"])
        .reset_index(drop=True)
    )


def _baseline_rows(path: Path, start: date, end: date) -> pd.DataFrame:
    columns = [
        "decision_date",
        "ticker",
        "baseline_score",
        "baseline_rank",
        "baseline_action",
        "score_completeness_ratio",
    ]
    if not path.exists():
        return pd.DataFrame(columns=columns)
    frame = pd.read_csv(path)
    date_column = _first_existing_column(frame, ("decision_date", "as_of", "date"))
    score_column = _first_existing_column(
        frame,
        ("baseline_score", "score", "overall_score", "risk_adjusted_score"),
    )
    if date_column is None or score_column is None:
        raise ValueError("baseline score CSV must include a date column and score column")
    normalized = frame.copy()
    if "component" in normalized.columns:
        overall = normalized.loc[normalized["component"].astype(str) == "overall"].copy()
        if not overall.empty:
            normalized = overall
    aliases = load_ticker_aliases()
    normalized["decision_date"] = pd.to_datetime(
        normalized[date_column],
        errors="coerce",
    ).dt.date
    normalized["baseline_score"] = pd.to_numeric(normalized[score_column], errors="coerce")
    ticker_column = _first_existing_column(normalized, ("ticker", "symbol"))
    if ticker_column is None:
        normalized["ticker"] = ""
    else:
        normalized["ticker"] = canonicalize_ticker_series(
            normalized[ticker_column], aliases=aliases
        )
    normalized["baseline_rank"] = _optional_numeric_column(normalized, "baseline_rank")
    action_column = _first_existing_column(normalized, ("baseline_action", "action", "label"))
    normalized["baseline_action"] = (
        "" if action_column is None else normalized[action_column].fillna("").astype(str)
    )
    normalized["score_completeness_ratio"] = _optional_numeric_column(
        normalized,
        "score_completeness_ratio",
        default=1.0,
    )
    normalized = normalized.loc[
        normalized["decision_date"].notna()
        & normalized["decision_date"].map(lambda value: start <= value <= end)
        & normalized["ticker"].fillna("").astype(str).str.strip().astype(bool)
    ].copy()
    return (
        normalized.loc[:, columns]
        .drop_duplicates(subset=["decision_date", "ticker"], keep="last")
        .sort_values(["decision_date", "ticker"])
        .reset_index(drop=True)
    )


def _join_expected_baseline(expected: pd.DataFrame, baseline: pd.DataFrame) -> pd.DataFrame:
    if expected.empty:
        result = expected.copy()
        result["has_baseline_row"] = False
        return result
    merged = expected.merge(
        baseline,
        on=["decision_date", "ticker"],
        how="left",
        indicator=True,
    )
    merged["has_baseline_row"] = merged["_merge"].astype(str) == "both"
    return merged.drop(columns=["_merge"])


def _gap_rows(joined: pd.DataFrame) -> pd.DataFrame:
    if joined.empty:
        return _empty_gaps()
    records: list[dict[str, str]] = []
    for row in joined.to_dict(orient="records"):
        decision_date = _date_text(row.get("decision_date"))
        ticker = str(row.get("ticker") or "")
        if not row.get("has_baseline_row"):
            records.append(
                _gap_row(
                    decision_date,
                    ticker,
                    "MISSING_SCORE_ROW",
                    "No baseline score row exists for this SEC PIT decision row.",
                    "Run `aits score-daily backfill-baseline` for the missing date/ticker.",
                )
            )
            continue
        if pd.isna(row.get("baseline_score")):
            records.append(
                _gap_row(
                    decision_date,
                    ticker,
                    "MISSING_BASELINE_SCORE",
                    "Baseline row exists but baseline_score is blank or non-numeric.",
                    "Regenerate the baseline score row with a numeric baseline_score.",
                )
            )
        if pd.isna(row.get("baseline_rank")):
            records.append(
                _gap_row(
                    decision_date,
                    ticker,
                    "MISSING_BASELINE_RANK",
                    "Baseline row exists but baseline_rank is missing.",
                    "Regenerate baseline ranks for each decision_date.",
                )
            )
        if not str(row.get("baseline_action") or "").strip():
            records.append(
                _gap_row(
                    decision_date,
                    ticker,
                    "MISSING_ACTION",
                    "Baseline row exists but baseline_action is missing.",
                    "Regenerate baseline actions from score policy.",
                )
            )
        completeness = _float_or_none(row.get("score_completeness_ratio"))
        if completeness is not None and completeness < LOW_COMPLETENESS_RATIO_MIN:
            records.append(
                _gap_row(
                    decision_date,
                    ticker,
                    "LOW_COMPLETENESS",
                    "Baseline row completeness is below the monitoring coverage threshold.",
                    "Backfill or document missing historical score inputs.",
                )
            )
    if not records:
        return _empty_gaps()
    return pd.DataFrame(records, columns=list(BASELINE_GAP_COLUMNS))


def _coverage_by_ticker(joined: pd.DataFrame) -> pd.DataFrame:
    if joined.empty:
        return _empty_by_ticker()
    records: list[dict[str, Any]] = []
    for ticker, group in joined.groupby("ticker", sort=True):
        expected_rows = int(len(group))
        actual_rows = int(group["has_baseline_row"].sum())
        missing = group.loc[~group["has_baseline_row"]]
        available = group.loc[group["has_baseline_row"]]
        coverage_ratio = actual_rows / expected_rows if expected_rows else 0.0
        status = _coverage_status(coverage_ratio, actual_rows)
        records.append(
            {
                "ticker": ticker,
                "expected_rows": expected_rows,
                "actual_rows": actual_rows,
                "missing_rows": expected_rows - actual_rows,
                "coverage_ratio": round(float(coverage_ratio), 6),
                "score_completeness_avg": _mean_or_zero(available.get("score_completeness_ratio")),
                "first_available_date": _date_min_text(available.get("decision_date")),
                "last_available_date": _date_max_text(available.get("decision_date")),
                "first_missing_date": _date_min_text(missing.get("decision_date")),
                "last_missing_date": _date_max_text(missing.get("decision_date")),
                "coverage_status": status,
                "recommended_action": _recommended_action(status),
            }
        )
    return pd.DataFrame(records, columns=list(BASELINE_COVERAGE_BY_TICKER_COLUMNS))


def _coverage_by_date(joined: pd.DataFrame) -> pd.DataFrame:
    if joined.empty:
        return _empty_by_date()
    records: list[dict[str, Any]] = []
    for decision_date, group in joined.groupby("decision_date", sort=True):
        expected_rows = int(len(group))
        actual_rows = int(group["has_baseline_row"].sum())
        missing = group.loc[~group["has_baseline_row"]]
        coverage_ratio = actual_rows / expected_rows if expected_rows else 0.0
        records.append(
            {
                "decision_date": _date_text(decision_date),
                "expected_ticker_count": expected_rows,
                "actual_ticker_count": actual_rows,
                "missing_ticker_count": expected_rows - actual_rows,
                "coverage_ratio": round(float(coverage_ratio), 6),
                "score_completeness_avg": _mean_or_zero(
                    group.loc[group["has_baseline_row"]].get("score_completeness_ratio")
                ),
                "coverage_status": _coverage_status(coverage_ratio, actual_rows),
                "missing_tickers": ",".join(missing["ticker"].dropna().astype(str).tolist()),
            }
        )
    return pd.DataFrame(records, columns=list(BASELINE_COVERAGE_BY_DATE_COLUMNS))


def _summary_payload(
    *,
    status: str,
    start: date,
    end: date,
    baseline_score_path: Path,
    expected: pd.DataFrame,
    baseline: pd.DataFrame,
    joined: pd.DataFrame,
    limitations: list[str],
    artifacts: dict[str, Path],
) -> dict[str, Any]:
    expected_rows = int(len(expected))
    actual_rows = int(joined["has_baseline_row"].sum()) if "has_baseline_row" in joined else 0
    coverage_ratio = actual_rows / expected_rows if expected_rows else 0.0
    available = joined.loc[joined["has_baseline_row"]] if "has_baseline_row" in joined else joined
    return {
        "schema_version": "1.0",
        "report_type": SEC_PIT_BASELINE_COVERAGE_REPORT_TYPE,
        "task_id": SEC_PIT_BASELINE_COVERAGE_TASK_ID,
        "generated_at": _deterministic_generated_at(end),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "baseline_score_path": str(baseline_score_path),
        "expected_rows": expected_rows,
        "actual_rows": actual_rows,
        "coverage_ratio": round(float(coverage_ratio), 6),
        "missing_rows": expected_rows - actual_rows,
        "ticker_count": int(expected["ticker"].nunique()) if "ticker" in expected else 0,
        "date_count": (
            int(expected["decision_date"].nunique()) if "decision_date" in expected else 0
        ),
        "score_completeness_avg": _mean_or_zero(available.get("score_completeness_ratio")),
        "coverage_status": status,
        "recommended_action": _recommended_action(status),
        "baseline_rows": int(len(baseline)),
        "limitations": limitations,
        "output_artifacts": {key: str(value) for key, value in artifacts.items()},
    }


def _overall_status(joined: pd.DataFrame, baseline: pd.DataFrame) -> str:
    if baseline.empty:
        return "MISSING_BASELINE"
    if joined.empty:
        return "INSUFFICIENT_COVERAGE"
    expected_rows = int(len(joined))
    actual_rows = int(joined["has_baseline_row"].sum())
    coverage_ratio = actual_rows / expected_rows if expected_rows else 0.0
    return _coverage_status(coverage_ratio, actual_rows)


def _coverage_status(coverage_ratio: float, actual_rows: int) -> str:
    if actual_rows == 0:
        return "MISSING_BASELINE"
    if coverage_ratio >= COVERAGE_STATUS_OK_MIN:
        return "OK"
    if coverage_ratio >= COVERAGE_STATUS_LIMITED_MIN:
        return "LIMITED_COVERAGE"
    return "INSUFFICIENT_COVERAGE"


def _recommended_action(status: str) -> str:
    if status == "OK":
        return "Baseline coverage is sufficient for SEC PIT shadow monitoring."
    if status == "LIMITED_COVERAGE":
        return "Backfill missing baseline rows before treating shadow monitoring as conclusive."
    if status == "INSUFFICIENT_COVERAGE":
        return "Run historical baseline score backfill for the requested SEC PIT window."
    if status == "MISSING_BASELINE":
        return "Generate or point to a baseline score CSV before running shadow monitoring."
    return "Fix input artifact schema or parse errors, then rerun the coverage audit."


def _resolve_feature_panel_path(path: Path) -> Path:
    if path.exists():
        return path
    if DEFAULT_SEC_PIT_FEATURE_PANEL_PATH.exists():
        return DEFAULT_SEC_PIT_FEATURE_PANEL_PATH
    if LEGACY_SEC_PIT_FEATURE_PANEL_PATH.exists():
        return LEGACY_SEC_PIT_FEATURE_PANEL_PATH
    return path


def _gap_row(
    decision_date: str,
    ticker: str,
    gap_type: str,
    reason: str,
    recommended_fix: str,
) -> dict[str, str]:
    return {
        "decision_date": decision_date,
        "ticker": ticker,
        "gap_type": gap_type,
        "reason": reason,
        "recommended_fix": recommended_fix,
    }


def _empty_by_ticker() -> pd.DataFrame:
    return pd.DataFrame(columns=list(BASELINE_COVERAGE_BY_TICKER_COLUMNS))


def _empty_by_date() -> pd.DataFrame:
    return pd.DataFrame(columns=list(BASELINE_COVERAGE_BY_DATE_COLUMNS))


def _empty_gaps() -> pd.DataFrame:
    return pd.DataFrame(columns=list(BASELINE_GAP_COLUMNS))


def _optional_numeric_column(
    frame: pd.DataFrame,
    column: str,
    *,
    default: float | None = None,
) -> pd.Series:
    if column not in frame.columns:
        return pd.Series([default] * len(frame), index=frame.index, dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce")


def _first_existing_column(frame: pd.DataFrame, columns: tuple[str, ...]) -> str | None:
    for column in columns:
        if column in frame.columns:
            return column
    return None


def _mean_or_zero(values: object) -> float:
    if values is None:
        return 0.0
    series = pd.to_numeric(values, errors="coerce")
    if series.empty or series.dropna().empty:
        return 0.0
    return round(float(series.mean()), 6)


def _date_min_text(values: object) -> str:
    if values is None:
        return ""
    series = pd.Series(values).dropna()
    if series.empty:
        return ""
    return _date_text(series.min())


def _date_max_text(values: object) -> str:
    if values is None:
        return ""
    series = pd.Series(values).dropna()
    if series.empty:
        return ""
    return _date_text(series.max())


def _date_text(value: object) -> str:
    if isinstance(value, date):
        return value.isoformat()
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.date().isoformat()


def _float_or_none(value: object) -> float | None:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return None
    return float(parsed)


def _format_ratio(value: object) -> str:
    parsed = _float_or_none(value)
    if parsed is None:
        return ""
    return f"{parsed:.2%}"


def _deterministic_generated_at(end: date) -> str:
    return datetime(end.year, end.month, end.day, tzinfo=UTC).isoformat()


def _json_value(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_value(item) for item in value]
    return value
