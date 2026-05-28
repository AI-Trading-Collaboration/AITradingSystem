from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

SCHEMA_VERSION = 1
REPORT_TYPE = "market_panel"
PRODUCTION_EFFECT = "none"
MISSING_MARKET_PRICE_DATA = "MISSING_MARKET_PRICE_DATA"

PRICE_PROXIES: tuple[tuple[str, str], ...] = (
    ("SPY", "benchmark_proxy"),
    ("QQQ", "benchmark_proxy"),
    ("SMH", "ai_sector_proxy"),
    ("SOXX", "ai_sector_proxy"),
    ("^VIX", "risk_proxy"),
)
RATE_PROXIES: tuple[tuple[str, str], ...] = (("DGS10", "liquidity_proxy"),)
RETURN_WINDOWS: tuple[int, ...] = (1, 5, 20)


def default_market_panel_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"market_panel_{as_of.isoformat()}.json"


def default_market_panel_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"market_panel_{as_of.isoformat()}.md"


def build_market_panel_payload(
    *,
    as_of: date,
    prices_path: Path,
    rates_path: Path,
    data_quality_status: str = "NOT_RUN",
    data_quality_report_path: Path | None = None,
    read_cached_data: bool = True,
) -> dict[str, Any]:
    warnings: list[str] = []
    if read_cached_data:
        prices = _read_frame(prices_path, PRICE_PROXIES, "price", warnings)
        rates = _read_frame(rates_path, RATE_PROXIES, "rate", warnings)
    else:
        prices = None
        rates = None
        warnings.append("market_panel_cache_not_read:data_quality_failed")
    proxies: list[dict[str, Any]] = []
    for symbol, role in PRICE_PROXIES:
        proxies.append(
            _proxy_record(
                symbol=symbol,
                role=role,
                as_of=as_of,
                frame=prices,
                key_column="ticker",
                source_artifact=prices_path,
                change_mode="ratio",
            )
        )
    for symbol, role in RATE_PROXIES:
        proxies.append(
            _proxy_record(
                symbol=symbol,
                role=role,
                as_of=as_of,
                frame=rates,
                key_column="series",
                source_artifact=rates_path,
                change_mode="difference",
            )
        )
    available = [row for row in proxies if row["data_status"] in {"AVAILABLE", "PARTIAL_HISTORY"}]
    missing = [row for row in proxies if row["data_status"] == MISSING_MARKET_PRICE_DATA]
    missing_roles = _missing_roles(proxies)
    quality_warning = data_quality_status not in {"PASS", "NOT_RUN"}
    if not available or missing_roles:
        status = MISSING_MARKET_PRICE_DATA
    elif missing or warnings or quality_warning:
        status = "PASS_WITH_WARNINGS"
    else:
        status = "PASS"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "production_effect": PRODUCTION_EFFECT,
        "data_quality": {
            "status": data_quality_status,
            "report_path": (
                "" if data_quality_report_path is None else str(data_quality_report_path)
            ),
            "visible_in_output": True,
        },
        "source_artifacts": {
            "prices_daily": {
                "path": str(prices_path),
                "exists": prices_path.exists(),
                "role": "market_price_proxy_source",
            },
            "rates_daily": {
                "path": str(rates_path),
                "exists": rates_path.exists(),
                "role": "liquidity_rate_proxy_source",
            },
        },
        "summary": {
            "proxy_count": len(proxies),
            "available_proxy_count": len(available),
            "missing_proxy_count": len(missing),
            "missing_roles": missing_roles,
            "market_movement_sentence": _market_sentence(proxies),
        },
        "proxies": proxies,
        "warnings": warnings,
        "methodology": {
            "mode": "read_cached_market_and_rate_data_only",
            "price_return_formula": "current_adj_close / prior_observation_adj_close - 1",
            "rate_change_formula": "current_rate - prior_observation_rate",
            "trend_label_rule": "sign of 20-observation change; zero means FLAT_20D",
            "does_not_download_data": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def write_market_panel_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def write_market_panel_report(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_market_panel_markdown(payload), encoding="utf-8")
    return output_path


def render_market_panel_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    data_quality = _mapping(payload.get("data_quality"))
    lines = [
        f"# Market Panel {payload.get('as_of')}",
        "",
        f"- 状态：{_text(payload.get('status'), 'UNKNOWN')}",
        f"- 数据质量状态：{_text(data_quality.get('status'), 'UNKNOWN')}",
        f"- 数据质量报告：`{_text(data_quality.get('report_path'), 'MISSING')}`",
        f"- production_effect：`{_text(payload.get('production_effect'), PRODUCTION_EFFECT)}`",
        "",
        "本报告只读展示 benchmark、AI sector、risk 和 liquidity 代理的缓存市场表现；"
        "不下载数据、不重算 score、不生成交易指令。",
        "",
        "## 市场概览",
        "",
        f"- {_text(summary.get('market_movement_sentence'), 'MISSING')}",
        "",
        "## Proxy Panel",
        "",
        "| Role | Symbol | Last price | 1D | 5D | 20D | Trend | "
        "Risk interpretation | Data status | Source |",
        "|---|---|---:|---:|---:|---:|---|---|---|---|",
    ]
    for row in _records(payload.get("proxies")):
        lines.append(
            "| "
            f"{_text(row.get('role'))} | "
            f"`{_text(row.get('symbol'))}` | "
            f"{_format_number(row.get('last_price'))} | "
            f"{_format_change(row.get('return_1d'), row.get('change_mode'))} | "
            f"{_format_change(row.get('return_5d'), row.get('change_mode'))} | "
            f"{_format_change(row.get('return_20d'), row.get('change_mode'))} | "
            f"{_text(row.get('trend_label'), 'UNKNOWN')} | "
            f"{_escape_markdown_table(_text(row.get('risk_interpretation'), 'UNKNOWN'))} | "
            f"{_text(row.get('data_status'), 'UNKNOWN')} | "
            f"`{_text(row.get('source_artifact'))}` |"
        )
    if _text(payload.get("status")) == MISSING_MARKET_PRICE_DATA:
        lines.extend(
            [
                "",
                "## 降级说明",
                "",
                "- 缺少可用市场代理数据，报告不会补造 benchmark、AI sector、"
                "risk 或 liquidity 涨跌。",
            ]
        )
    lines.extend(
        [
            "",
            "## 审计边界",
            "",
            "- `return_1d` / `return_5d` / `return_20d` 对价格和 VIX 使用相邻观测收益率。",
            "- DGS10 等利率代理使用百分点变化，字段名保持为 `return_*` 以兼容 Reader Brief。",
            "- `trend_label` 只表示 20 个可用观测变化方向，不是交易信号。",
        ]
    )
    return "\n".join(lines) + "\n"


def _read_frame(
    path: Path,
    proxies: tuple[tuple[str, str], ...],
    kind: str,
    warnings: list[str],
) -> pd.DataFrame | None:
    if not path.exists():
        warnings.append(f"{kind}_source_missing:{path}")
        return None
    try:
        frame = pd.read_csv(path)
    except Exception as exc:
        warnings.append(f"{kind}_source_unreadable:{type(exc).__name__}:{path}")
        return None
    key_column = "ticker" if kind == "price" else "series"
    value_column = "adj_close" if kind == "price" else "value"
    required = {"date", key_column, value_column}
    missing_columns = sorted(required - set(frame.columns))
    if missing_columns:
        warnings.append(f"{kind}_source_missing_columns:{','.join(missing_columns)}")
        return None
    symbols = {symbol for symbol, _role in proxies}
    filtered = frame.loc[frame[key_column].astype(str).isin(symbols)].copy()
    filtered["_date"] = pd.to_datetime(filtered["date"], errors="coerce")
    filtered["_value"] = pd.to_numeric(filtered[value_column], errors="coerce")
    filtered = filtered.loc[filtered["_date"].notna() & filtered["_value"].notna()].copy()
    return filtered.sort_values([key_column, "_date"]).reset_index(drop=True)


def _proxy_record(
    *,
    symbol: str,
    role: str,
    as_of: date,
    frame: pd.DataFrame | None,
    key_column: str,
    source_artifact: Path,
    change_mode: str,
) -> dict[str, Any]:
    base = {
        "symbol": symbol,
        "role": role,
        "last_price": None,
        "return_1d": None,
        "return_5d": None,
        "return_20d": None,
        "trend_label": "MISSING",
        "risk_interpretation": "缺少市场代理数据，不能解释今日市场变化。",
        "data_status": MISSING_MARKET_PRICE_DATA,
        "source_artifact": str(source_artifact),
        "source_date": None,
        "change_mode": change_mode,
        "production_effect": PRODUCTION_EFFECT,
    }
    if frame is None or frame.empty:
        return base
    history = frame.loc[
        (frame[key_column].astype(str) == symbol) & (frame["_date"] <= pd.Timestamp(as_of))
    ].copy()
    if history.empty:
        return base
    history = history.sort_values("_date").reset_index(drop=True)
    current = float(history["_value"].iloc[-1])
    returns = {
        window: _window_change(history["_value"], window, change_mode=change_mode)
        for window in RETURN_WINDOWS
    }
    missing_returns = [window for window, value in returns.items() if value is None]
    record = {
        **base,
        "last_price": current,
        "return_1d": returns[1],
        "return_5d": returns[5],
        "return_20d": returns[20],
        "trend_label": _trend_label(returns[20]),
        "risk_interpretation": _risk_interpretation(
            symbol=symbol,
            role=role,
            change_1d=returns[1],
            change_mode=change_mode,
        ),
        "data_status": "PARTIAL_HISTORY" if missing_returns else "AVAILABLE",
        "source_date": pd.Timestamp(history["_date"].iloc[-1]).date().isoformat(),
        "missing_return_windows": missing_returns,
    }
    return record


def _window_change(values: pd.Series, window: int, *, change_mode: str) -> float | None:
    required = window + 1
    if len(values) < required:
        return None
    previous = float(values.iloc[-required])
    current = float(values.iloc[-1])
    if change_mode == "difference":
        return current - previous
    if previous == 0:
        return None
    return (current / previous) - 1.0


def _trend_label(change_20d: float | None) -> str:
    if change_20d is None:
        return "INSUFFICIENT_HISTORY"
    if change_20d > 0:
        return "UP_20D"
    if change_20d < 0:
        return "DOWN_20D"
    return "FLAT_20D"


def _risk_interpretation(
    *,
    symbol: str,
    role: str,
    change_1d: float | None,
    change_mode: str,
) -> str:
    if change_1d is None:
        return "历史不足，不能解释 1D 变化。"
    direction = "上行" if change_1d > 0 else "下行" if change_1d < 0 else "持平"
    if role == "risk_proxy":
        return f"{symbol} {direction}；VIX 上行通常表示风险压力上升，下行表示风险压力缓和。"
    if role == "liquidity_proxy":
        unit = "百分点" if change_mode == "difference" else "比例"
        return f"{symbol} {direction}；10Y yield 变化按{unit}披露，上行通常压制流动性。"
    if role == "ai_sector_proxy":
        return f"{symbol} {direction}；半导体/AI sector 代理反映 AI 交易主线强弱。"
    return f"{symbol} {direction}；benchmark proxy 反映大盘风险偏好。"


def _missing_roles(proxies: list[dict[str, Any]]) -> list[str]:
    roles = sorted({str(row["role"]) for row in proxies})
    missing = []
    for role in roles:
        role_rows = [row for row in proxies if row["role"] == role]
        if not any(row["data_status"] in {"AVAILABLE", "PARTIAL_HISTORY"} for row in role_rows):
            missing.append(role)
    return missing


def _market_sentence(proxies: list[dict[str, Any]]) -> str:
    available = [row for row in proxies if row["data_status"] in {"AVAILABLE", "PARTIAL_HISTORY"}]
    if not available:
        return "市场面板缺少可用价格/利率代理数据，不能描述今日市场涨跌。"
    preferred = {
        "benchmark_proxy": ("SPY", "QQQ"),
        "ai_sector_proxy": ("SMH", "SOXX"),
        "risk_proxy": ("^VIX",),
        "liquidity_proxy": ("DGS10",),
    }
    parts = []
    for role, symbols in preferred.items():
        row = next(
            (
                candidate
                for symbol in symbols
                for candidate in available
                if candidate["role"] == role and candidate["symbol"] == symbol
            ),
            None,
        )
        if row is None:
            continue
        parts.append(
            f"{row['symbol']} 1D={_format_change(row.get('return_1d'), row.get('change_mode'))}"
        )
    return "；".join(parts) + "。"


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _text(value: object, default: str = "") -> str:
    if value is None or value == "":
        return default
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _format_number(value: object) -> str:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return "UNKNOWN"
    return f"{number:.4f}"


def _format_change(value: object, change_mode: object) -> str:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return "UNKNOWN"
    if change_mode == "difference":
        return f"{number:+.4f}pp"
    return f"{number:+.2%}"


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
