from __future__ import annotations

import json
import os
import re
from datetime import UTC, datetime
from hashlib import sha256
from html.parser import HTMLParser
from math import sqrt
from pathlib import Path
from urllib.parse import unquote, urlparse
from urllib.request import Request, urlopen

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.models import (
    ETFConfigBundle,
    ETFP2Config,
    ETFP2SourceConfig,
    ETFQualityReport,
)

NO_LOOKAHEAD_TEST_ID = (
    "tests/test_etf_portfolio.py::"
    "test_no_lookahead_future_price_changes_do_not_change_signal_or_weights"
)
HOLDING_SYMBOL_ALIASES = (
    "holding_symbol",
    "ticker",
    "symbol",
    "holding_ticker",
    "identifier",
)
HOLDING_WEIGHT_ALIASES = (
    "holding_weight",
    "weight",
    "weight_pct",
    "weight_percent",
    "weight (%)",
    "% weight",
    "weighting",
)
NEWS_SYMBOL_ALIASES = ("symbol", "ticker", "subject", "asset")
NEWS_THEME_ALIASES = ("theme", "topic", "category", "event_theme")
NEWS_SUMMARY_ALIASES = ("summary", "headline", "title", "description")
NEWS_PUBLISHED_AT_ALIASES = ("published_at", "published", "published_time", "event_time")
NEWS_AVAILABLE_AT_ALIASES = (
    "available_at",
    "provider_available_at",
    "ingested_at",
    "downloaded_at",
)
NEWS_SENTIMENT_ALIASES = ("sentiment_score", "sentiment", "sentiment_value")
NEWS_RELEVANCE_ALIASES = ("relevance_score", "relevance", "confidence")
OPTIONS_SYMBOL_ALIASES = ("symbol", "ticker", "asset", "underlying")
OPTIONS_AS_OF_ALIASES = ("as_of", "date", "as_of_date", "observation_date", "trade_date")
OPTIONS_AVAILABLE_AT_ALIASES = (
    "available_at",
    "downloaded_at",
    "ingested_at",
    "provider_available_at",
    "timestamp",
)
OPTIONS_IV_RANK_ALIASES = ("iv_rank", "implied_vol_rank", "iv_percentile", "iv_rank_pct")
OPTIONS_SKEW_ZSCORE_ALIASES = (
    "skew_zscore",
    "skew_z",
    "skew",
    "put_call_skew_zscore",
    "skew_score",
)
OPTIONS_VXN_LEVEL_ALIASES = ("vxn_level", "vxn", "nasdaq_volatility_index")
OPTIONS_RISK_FLAG_ALIASES = ("risk_flag", "flag", "risk_state")
EDGAR_TEXT_DOCUMENT_COLUMNS = [
    "as_of",
    "symbol",
    "source_provider",
    "source_url",
    "filing_type",
    "filed_at",
    "available_at",
    "accession_number",
    "document_text_path",
    "document_text_checksum",
    "text_character_count",
    "text_excerpt",
    "fetch_status",
    "limitation",
    "checksum",
    "production_effect",
]
EDGAR_TEXT_EXCERPT_CHAR_LIMIT = 500


def build_source_contract_report(
    *,
    source_id: str,
    source: ETFP2SourceConfig,
    run_date,
    input_path: Path | None = None,
) -> pd.DataFrame:
    path = input_path or _resolve_repo_path(source.input_path)
    base = _source_base_row(source_id, source, run_date, path)
    frame = _read_optional_frame(path)
    if frame is None:
        return pd.DataFrame([{**base, "status": "MISSING_INPUT", "row_count": 0}])

    missing_columns = sorted(set(source.required_columns) - set(frame.columns))
    if missing_columns:
        return pd.DataFrame(
            [
                {
                    **base,
                    "status": "FAILED_SCHEMA",
                    "row_count": len(frame),
                    "missing_columns": ",".join(missing_columns),
                }
            ]
        )

    pit_issue_count = _pit_issue_count(frame, source.available_time_column, run_date)
    if pit_issue_count:
        status = "FAILED_PIT"
    elif frame.empty:
        status = "EMPTY"
    elif source.provider_status == "connected":
        status = "PASS"
    else:
        status = "PASS_WITH_LIMITATIONS"

    selected = _select_rows_as_of(frame, source.as_of_column, run_date)
    return pd.DataFrame(
        [
            {
                **base,
                "status": status,
                "row_count": len(frame),
                "rows_as_of": len(selected),
                "latest_as_of": _latest_as_of(frame, source.as_of_column),
                "pit_issue_count": pit_issue_count,
                "missing_columns": "",
                "numeric_summary": _numeric_summary(selected),
                "data_quality_status": _latest_text(selected, "data_quality_status"),
                "source_quality_status": _latest_text(
                    selected,
                    "vix_proxy_quality_status",
                ),
                "limitation": _latest_text(selected, "limitation"),
            }
        ]
    )


def import_p2_source(
    *,
    source_id: str,
    source: ETFP2SourceConfig,
    input_path: Path,
    output_path: Path | None = None,
    manifest_path: Path,
    provider: str,
    source_url: str,
    request_params: dict[str, object] | None = None,
    downloaded_at: datetime | None = None,
) -> pd.DataFrame:
    frame = _read_required_frame(input_path)
    missing_columns = sorted(set(source.required_columns) - set(frame.columns))
    downloaded = downloaded_at or datetime.now(UTC)
    output = output_path or _resolve_repo_path(source.input_path)
    if missing_columns:
        return pd.DataFrame(
            [
                {
                    "source_id": source_id,
                    "status": "FAILED_SCHEMA",
                    "input_path": str(input_path),
                    "output_path": str(output),
                    "missing_columns": ",".join(missing_columns),
                    "row_count": len(frame),
                    "provider": provider,
                    "source_url": source_url,
                    "downloaded_at": downloaded.isoformat(),
                    "checksum": "",
                    "manifest_path": str(manifest_path),
                    "production_effect": "none",
                }
            ]
        )

    normalized = frame[source.required_columns].copy()
    output.parent.mkdir(parents=True, exist_ok=True)
    normalized.to_csv(output, index=False)
    checksum = _file_sha256(output)
    _append_p2_manifest(
        manifest_path=manifest_path,
        row={
            "source_id": source_id,
            "provider": provider,
            "source_level": source.source_level,
            "source_url": source_url,
            "request_params": json.dumps(request_params or {}, ensure_ascii=False, sort_keys=True),
            "downloaded_at": downloaded.isoformat(),
            "row_count": len(normalized),
            "output_path": str(output),
            "checksum": checksum,
            "production_effect": "none",
        },
    )
    return pd.DataFrame(
        [
            {
                "source_id": source_id,
                "status": "IMPORTED",
                "input_path": str(input_path),
                "output_path": str(output),
                "missing_columns": "",
                "row_count": len(normalized),
                "provider": provider,
                "source_url": source_url,
                "downloaded_at": downloaded.isoformat(),
                "checksum": checksum,
                "manifest_path": str(manifest_path),
                "production_effect": "none",
            }
        ]
    )


def derive_edgar_text_events_from_timeline(
    *,
    source: ETFP2SourceConfig,
    timeline_path: Path,
    output_path: Path | None = None,
    manifest_path: Path,
    run_date,
    symbols: list[str] | None = None,
    downloaded_at: datetime | None = None,
) -> pd.DataFrame:
    timeline = _read_required_frame(timeline_path)
    required = {"ticker", "form", "filing_date"}
    available_candidates = [
        "available_time_utc",
        "acceptance_datetime_utc",
        "available_date",
        "available_for_signal_date",
    ]
    missing_columns = sorted(column for column in required if column not in timeline.columns)
    if not any(column in timeline.columns for column in available_candidates):
        missing_columns.append("available_time_utc|acceptance_datetime_utc|available_date")

    output = output_path or _resolve_repo_path(source.input_path)
    downloaded = downloaded_at or datetime.now(UTC)
    if missing_columns:
        return pd.DataFrame(
            [
                {
                    "source_id": "edgar_text",
                    "status": "FAILED_SCHEMA",
                    "timeline_path": str(timeline_path),
                    "output_path": str(output),
                    "missing_columns": ",".join(missing_columns),
                    "raw_row_count": len(timeline),
                    "row_count": 0,
                    "provider": "SEC EDGAR local filing timeline",
                    "source_url": str(timeline_path),
                    "downloaded_at": downloaded.isoformat(),
                    "checksum": "",
                    "manifest_path": str(manifest_path),
                    "production_effect": "none",
                }
            ]
        )

    frame = timeline.copy()
    if symbols:
        allowed = {symbol.upper() for symbol in symbols}
        frame = frame.loc[frame["ticker"].astype(str).str.upper().isin(allowed)].copy()

    frame["_as_of"] = _coalesced_datetime(
        frame,
        ["available_for_signal_date", "available_date", "filing_date"],
    )
    frame["_available_at"] = _coalesced_datetime(frame, available_candidates)
    frame["_filed_at"] = pd.to_datetime(frame["filing_date"], errors="coerce")
    frame = frame.loc[
        frame["_as_of"].notna()
        & frame["_available_at"].notna()
        & frame["_filed_at"].notna()
        & frame["ticker"].notna()
        & frame["form"].notna()
    ].copy()

    as_of_date = pd.Timestamp(run_date).date()
    eligible = frame.loc[
        (frame["_as_of"].dt.date <= as_of_date) & (frame["_available_at"].dt.date <= as_of_date)
    ].copy()

    rows = []
    for _, row in eligible.sort_values(["_as_of", "ticker", "form"]).iterrows():
        symbol = str(row["ticker"]).upper()
        filing_type = str(row["form"])
        filed_at = row["_filed_at"].date().isoformat()
        available_at = pd.Timestamp(row["_available_at"]).isoformat()
        as_of = row["_as_of"].date().isoformat()
        source_url = _first_present(
            row,
            ["source_url", "source_endpoint", "raw_payload_path"],
        )
        accession = _first_present(row, ["accession_number", "accession_number_compact"])
        raw_checksum = _first_present(row, ["raw_payload_sha256"])
        checksum = _row_checksum(
            [
                symbol,
                accession,
                filing_type,
                filed_at,
                available_at,
                source_url,
                raw_checksum,
            ]
        )
        rows.append(
            {
                "as_of": as_of,
                "symbol": symbol,
                "source_provider": "SEC EDGAR local filing timeline",
                "source_url": source_url,
                "filing_type": filing_type,
                "filed_at": filed_at,
                "available_at": available_at,
                "topic": f"sec_filing:{filing_type}",
                "sentiment_score": 0.0,
                "summary": (
                    "SEC filing metadata only; "
                    f"{symbol} {filing_type} filed_at={filed_at}, "
                    f"available_for_signal_date={as_of}."
                ),
                "checksum": checksum,
            }
        )

    canonical = pd.DataFrame(rows, columns=source.required_columns)
    output.parent.mkdir(parents=True, exist_ok=True)
    canonical.to_csv(output, index=False)
    checksum = _file_sha256(output)
    _append_p2_manifest(
        manifest_path=manifest_path,
        row={
            "source_id": "edgar_text",
            "provider": "SEC EDGAR local filing timeline",
            "source_level": source.source_level,
            "source_url": str(timeline_path),
            "request_params": json.dumps(
                {
                    "timeline_path": str(timeline_path),
                    "symbols": symbols or [],
                    "as_of": _date_text(run_date),
                    "upstream_checksum": _file_sha256(timeline_path),
                    "derivation": "filing_metadata_only_neutral_sentiment",
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            "downloaded_at": downloaded.isoformat(),
            "row_count": len(canonical),
            "output_path": str(output),
            "checksum": checksum,
            "production_effect": "none",
        },
    )
    status = "DERIVED" if not canonical.empty else "EMPTY"
    return pd.DataFrame(
        [
            {
                "source_id": "edgar_text",
                "status": status,
                "timeline_path": str(timeline_path),
                "output_path": str(output),
                "missing_columns": "",
                "raw_row_count": len(timeline),
                "eligible_row_count": len(eligible),
                "row_count": len(canonical),
                "latest_as_of": _latest_as_of(canonical, source.as_of_column),
                "provider": "SEC EDGAR local filing timeline",
                "source_url": str(timeline_path),
                "downloaded_at": downloaded.isoformat(),
                "checksum": checksum,
                "manifest_path": str(manifest_path),
                "production_effect": "none",
            }
        ]
    )


def fetch_edgar_text_documents_from_timeline(
    *,
    source: ETFP2SourceConfig,
    timeline_path: Path,
    document_dir: Path,
    output_path: Path,
    manifest_path: Path,
    run_date,
    symbols: list[str] | None = None,
    filing_types: list[str] | None = None,
    limit: int = 5,
    user_agent: str | None = None,
    timeout_seconds: float = 30.0,
    downloaded_at: datetime | None = None,
) -> pd.DataFrame:
    timeline = _read_required_frame(timeline_path)
    output = output_path
    downloaded = downloaded_at or datetime.now(UTC)
    required = {"ticker", "form", "filing_date", "source_url"}
    available_candidates = [
        "available_time_utc",
        "acceptance_datetime_utc",
        "available_date",
        "available_for_signal_date",
    ]
    missing_columns = sorted(column for column in required if column not in timeline.columns)
    if not any(column in timeline.columns for column in available_candidates):
        missing_columns.append("available_time_utc|acceptance_datetime_utc|available_date")
    if limit <= 0:
        return pd.DataFrame(
            [
                {
                    "source_id": "edgar_text_documents",
                    "status": "FAILED_LIMIT",
                    "timeline_path": str(timeline_path),
                    "output_path": str(output),
                    "document_dir": str(document_dir),
                    "missing_columns": "",
                    "raw_row_count": len(timeline),
                    "candidate_count": 0,
                    "fetched_document_count": 0,
                    "failed_document_count": 0,
                    "provider": "SEC EDGAR filing documents",
                    "source_url": str(timeline_path),
                    "downloaded_at": downloaded.isoformat(),
                    "checksum": "",
                    "manifest_path": str(manifest_path),
                    "limitation": "limit must be positive.",
                    "production_effect": "none",
                }
            ]
        )
    if missing_columns:
        return pd.DataFrame(
            [
                {
                    "source_id": "edgar_text_documents",
                    "status": "FAILED_SCHEMA",
                    "timeline_path": str(timeline_path),
                    "output_path": str(output),
                    "document_dir": str(document_dir),
                    "missing_columns": ",".join(missing_columns),
                    "raw_row_count": len(timeline),
                    "candidate_count": 0,
                    "fetched_document_count": 0,
                    "failed_document_count": 0,
                    "provider": "SEC EDGAR filing documents",
                    "source_url": str(timeline_path),
                    "downloaded_at": downloaded.isoformat(),
                    "checksum": "",
                    "manifest_path": str(manifest_path),
                    "limitation": "SEC filing timeline is missing required text fetch columns.",
                    "production_effect": "none",
                }
            ]
        )

    frame = timeline.copy()
    if symbols:
        allowed_symbols = {symbol.upper() for symbol in symbols}
        frame = frame.loc[frame["ticker"].astype(str).str.upper().isin(allowed_symbols)].copy()
    if filing_types:
        allowed_forms = {filing_type.upper() for filing_type in filing_types}
        frame = frame.loc[frame["form"].astype(str).str.upper().isin(allowed_forms)].copy()

    frame["_as_of"] = _coalesced_datetime(
        frame,
        ["available_for_signal_date", "available_date", "filing_date"],
    )
    frame["_available_at"] = _coalesced_datetime(frame, available_candidates)
    frame["_filed_at"] = pd.to_datetime(frame["filing_date"], errors="coerce")
    frame = frame.loc[
        frame["_as_of"].notna()
        & frame["_available_at"].notna()
        & frame["_filed_at"].notna()
        & frame["ticker"].notna()
        & frame["form"].notna()
        & frame["source_url"].notna()
    ].copy()
    as_of_date = pd.Timestamp(run_date).date()
    eligible = frame.loc[
        (frame["_as_of"].dt.date <= as_of_date) & (frame["_available_at"].dt.date <= as_of_date)
    ].copy()
    if "accession_number" in eligible.columns:
        eligible = eligible.drop_duplicates(["ticker", "form", "accession_number"])
    else:
        eligible = eligible.drop_duplicates(["ticker", "form", "source_url"])
    candidates = eligible.sort_values(
        ["_available_at", "ticker", "form"],
        ascending=[False, True, True],
    ).head(limit)

    resolved_user_agent = user_agent or os.environ.get("SEC_USER_AGENT", "")
    document_dir.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, object]] = []
    for _, row in candidates.iterrows():
        rows.append(
            _fetch_edgar_text_document_row(
                row=row,
                document_dir=document_dir,
                user_agent=resolved_user_agent,
                timeout_seconds=timeout_seconds,
            )
        )

    run_index = pd.DataFrame(rows, columns=EDGAR_TEXT_DOCUMENT_COLUMNS)
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        existing = _read_required_frame(output)
        for column in EDGAR_TEXT_DOCUMENT_COLUMNS:
            if column not in existing.columns:
                existing[column] = ""
        existing = existing[EDGAR_TEXT_DOCUMENT_COLUMNS].copy()
        run_keys = _edgar_text_index_keys(run_index)
        existing = existing.loc[~_edgar_text_index_keys(existing).isin(run_keys)].copy()
        index = pd.concat([existing, run_index], ignore_index=True)
    else:
        index = run_index
    if not index.empty:
        index = index.sort_values(
            ["as_of", "symbol", "filing_type", "accession_number", "source_url"],
        ).reset_index(drop=True)
    index.to_csv(output, index=False)
    output_checksum = _file_sha256(output)
    fetched_count = int((run_index.get("fetch_status", pd.Series(dtype=str)) == "FETCHED").sum())
    failed_count = len(run_index) - fetched_count
    if len(run_index) == 0:
        status = "EMPTY"
    elif fetched_count == len(run_index):
        status = "FETCHED"
    elif fetched_count:
        status = "FETCHED_WITH_ERRORS"
    else:
        status = "FAILED_FETCH"
    _append_p2_manifest(
        manifest_path=manifest_path,
        row={
            "source_id": "edgar_text_documents",
            "provider": "SEC EDGAR filing documents",
            "source_level": source.source_level,
            "source_url": str(timeline_path),
            "request_params": json.dumps(
                {
                    "timeline_path": str(timeline_path),
                    "document_dir": str(document_dir),
                    "symbols": symbols or [],
                    "filing_types": filing_types or [],
                    "as_of": _date_text(run_date),
                    "limit": limit,
                    "timeout_seconds": timeout_seconds,
                    "user_agent_provided": bool(resolved_user_agent),
                    "upstream_checksum": _file_sha256(timeline_path),
                    "derivation": "official_filing_text_cache_only_no_sentiment",
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            "downloaded_at": downloaded.isoformat(),
            "row_count": len(run_index),
            "output_path": str(output),
            "checksum": output_checksum,
            "production_effect": "none",
        },
    )
    return pd.DataFrame(
        [
            {
                "source_id": "edgar_text_documents",
                "status": status,
                "timeline_path": str(timeline_path),
                "output_path": str(output),
                "document_dir": str(document_dir),
                "missing_columns": "",
                "raw_row_count": len(timeline),
                "candidate_count": len(candidates),
                "fetched_document_count": fetched_count,
                "failed_document_count": failed_count,
                "output_row_count": len(index),
                "latest_as_of": _latest_as_of(run_index, "as_of"),
                "provider": "SEC EDGAR filing documents",
                "source_url": str(timeline_path),
                "downloaded_at": downloaded.isoformat(),
                "checksum": output_checksum,
                "manifest_path": str(manifest_path),
                "limitation": (
                    "Official filing text cache only; no financial statement "
                    "interpretation, sentiment inference, or investment conclusion."
                ),
                "production_effect": "none",
            }
        ]
    )


def build_edgar_text_topic_audit(
    *,
    document_index: pd.DataFrame,
    p2_config: ETFP2Config,
    run_date,
) -> pd.DataFrame:
    required = {
        "as_of",
        "symbol",
        "source_url",
        "filing_type",
        "available_at",
        "accession_number",
        "document_text_path",
        "fetch_status",
        "production_effect",
    }
    missing_columns = sorted(required - set(document_index.columns))
    base = {
        "date": _date_text(run_date),
        "module": "edgar_text_topic_audit",
        "policy_version": p2_config.policy_metadata.version,
        "candidate_only": p2_config.edgar_text_analysis.candidate_only,
        "auto_promotion": p2_config.edgar_text_analysis.auto_promotion,
        "production_effect": "none",
    }
    if missing_columns:
        return pd.DataFrame(
            [
                {
                    **base,
                    "analysis_status": "FAILED_SCHEMA",
                    "missing_columns": ",".join(missing_columns),
                    "as_of": "",
                    "symbol": "",
                    "filing_type": "",
                    "accession_number": "",
                    "source_url": "",
                    "document_text_path": "",
                    "topic": "",
                    "keyword_count": 0,
                    "matched_keywords": "",
                    "text_character_count": 0,
                    "limitation": "EDGAR text topic audit input is missing required columns.",
                }
            ]
        )

    frame = document_index.copy()
    frame["_as_of"] = pd.to_datetime(frame["as_of"], errors="coerce")
    selected = frame.loc[
        frame["_as_of"].notna()
        & (frame["_as_of"].dt.date <= pd.Timestamp(run_date).date())
        & (frame["fetch_status"].astype(str) == "FETCHED")
    ].copy()
    if selected.empty:
        return pd.DataFrame(
            [
                {
                    **base,
                    "analysis_status": "NO_FETCHED_DOCUMENTS",
                    "missing_columns": "",
                    "as_of": "",
                    "symbol": "",
                    "filing_type": "",
                    "accession_number": "",
                    "source_url": "",
                    "document_text_path": "",
                    "topic": "",
                    "keyword_count": 0,
                    "matched_keywords": "",
                    "text_character_count": 0,
                    "limitation": "No fetched EDGAR text documents are available as of run date.",
                }
            ]
        )

    rows: list[dict[str, object]] = []
    for _, row in selected.sort_values(["as_of", "symbol", "filing_type"]).iterrows():
        document_path = _resolve_repo_path(str(row["document_text_path"]))
        common = {
            **base,
            "missing_columns": "",
            "as_of": str(row["as_of"]),
            "symbol": str(row["symbol"]),
            "filing_type": str(row["filing_type"]),
            "accession_number": str(row["accession_number"]),
            "source_url": str(row["source_url"]),
            "document_text_path": str(document_path),
        }
        if not document_path.exists():
            rows.append(
                {
                    **common,
                    "analysis_status": "MISSING_TEXT_FILE",
                    "topic": "",
                    "keyword_count": 0,
                    "matched_keywords": "",
                    "text_character_count": 0,
                    "limitation": "Cached EDGAR text file is missing.",
                }
            )
            continue
        text = document_path.read_text(encoding="utf-8")
        text_count = len(text)
        if text_count < p2_config.edgar_text_analysis.min_text_characters:
            rows.append(
                {
                    **common,
                    "analysis_status": "LIMITED_SHORT_TEXT",
                    "topic": "",
                    "keyword_count": 0,
                    "matched_keywords": "",
                    "text_character_count": text_count,
                    "limitation": (
                        "Cached EDGAR text is shorter than configured "
                        "min_text_characters; no topic conclusion generated."
                    ),
                }
            )
            continue
        normalized = text.lower()
        for topic, keywords in p2_config.edgar_text_analysis.topic_keywords.items():
            matched = []
            count = 0
            for keyword in keywords:
                keyword_count = _keyword_occurrence_count(normalized, keyword)
                if keyword_count:
                    matched.append(str(keyword))
                    count += keyword_count
            rows.append(
                {
                    **common,
                    "analysis_status": "COUNTED",
                    "topic": topic,
                    "keyword_count": count,
                    "matched_keywords": ",".join(matched),
                    "text_character_count": text_count,
                    "limitation": (
                        "Keyword topic count only; no sentiment inference, "
                        "financial statement interpretation, or investment conclusion."
                    ),
                }
            )
    return pd.DataFrame(rows)


def derive_options_iv_skew_from_vix(
    *,
    source: ETFP2SourceConfig,
    p2_config: ETFP2Config,
    prices: pd.DataFrame,
    prices_path: Path,
    output_path: Path | None = None,
    manifest_path: Path,
    run_date,
    symbols: list[str],
    data_quality_status: str,
    downloaded_at: datetime | None = None,
) -> pd.DataFrame:
    output = output_path or _resolve_repo_path(source.input_path)
    downloaded = downloaded_at or datetime.now(UTC)
    frame = prices.copy()
    if "symbol" not in frame.columns and "ticker" in frame.columns:
        frame["symbol"] = frame["ticker"]
    if "adj_close" not in frame.columns and "close" in frame.columns:
        frame["adj_close"] = frame["close"]
    required_columns = {"date", "symbol", "adj_close"}
    missing_columns = sorted(required_columns - set(frame.columns))
    if missing_columns:
        return pd.DataFrame(
            [
                {
                    "source_id": "options_iv_skew",
                    "status": "FAILED_SCHEMA",
                    "prices_path": str(prices_path),
                    "output_path": str(output),
                    "missing_columns": ",".join(missing_columns),
                    "raw_row_count": len(prices),
                    "row_count": 0,
                    "provider": "Local market cache VIX proxy",
                    "source_url": str(prices_path),
                    "downloaded_at": downloaded.isoformat(),
                    "checksum": "",
                    "manifest_path": str(manifest_path),
                    "data_quality_status": data_quality_status,
                    "production_effect": "none",
                }
            ]
        )

    options = p2_config.options_risk
    frame["_symbol"] = frame["symbol"].astype(str)
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    source_rows = frame.loc[frame["_symbol"] == options.source_symbol].copy()
    duplicate_dates = source_rows.duplicated(["date"], keep=False)
    invalid_source = (
        source_rows["_date"].isna()
        | source_rows["_adj_close"].isna()
        | (source_rows["_adj_close"] <= 0)
        | duplicate_dates
    )
    if invalid_source.any():
        return pd.DataFrame(
            [
                {
                    "source_id": "options_iv_skew",
                    "status": "FAILED_VIX_SERIES_QUALITY",
                    "prices_path": str(prices_path),
                    "output_path": str(output),
                    "missing_columns": "",
                    "source_symbol": options.source_symbol,
                    "raw_row_count": len(prices),
                    "invalid_source_rows": int(invalid_source.sum()),
                    "duplicate_date_rows": int(duplicate_dates.sum()),
                    "row_count": 0,
                    "provider": "Local market cache VIX proxy",
                    "source_url": str(prices_path),
                    "downloaded_at": downloaded.isoformat(),
                    "checksum": "",
                    "manifest_path": str(manifest_path),
                    "data_quality_status": data_quality_status,
                    "vix_proxy_quality_status": "FAIL",
                    "production_effect": "none",
                }
            ]
        )

    vix = source_rows.copy()
    if vix.empty:
        return pd.DataFrame(
            [
                {
                    "source_id": "options_iv_skew",
                    "status": "MISSING_SOURCE_SYMBOL",
                    "prices_path": str(prices_path),
                    "output_path": str(output),
                    "missing_columns": "",
                    "source_symbol": options.source_symbol,
                    "raw_row_count": len(prices),
                    "row_count": 0,
                    "provider": "Local market cache VIX proxy",
                    "source_url": str(prices_path),
                    "downloaded_at": downloaded.isoformat(),
                    "checksum": "",
                    "manifest_path": str(manifest_path),
                    "data_quality_status": data_quality_status,
                    "production_effect": "none",
                }
            ]
        )

    as_of_date = pd.Timestamp(run_date).date()
    vix = vix.loc[vix["_date"].dt.date <= as_of_date].sort_values("_date").copy()
    vix["_iv_rank"] = (
        vix["_adj_close"]
        .rolling(options.lookback_days, min_periods=options.min_history_days)
        .apply(_rolling_last_percentile, raw=False)
    )
    vix = vix.loc[vix["_iv_rank"].notna()].copy()
    rows = []
    for _, row in vix.iterrows():
        as_of = row["_date"].date().isoformat()
        vix_level = float(row["_adj_close"])
        iv_rank = float(row["_iv_rank"])
        risk_flag = _vix_proxy_risk_flag(iv_rank, p2_config)
        for symbol in symbols:
            clean_symbol = str(symbol).upper()
            checksum = _row_checksum(
                [
                    as_of,
                    clean_symbol,
                    options.source_symbol,
                    vix_level,
                    iv_rank,
                    risk_flag,
                    data_quality_status,
                ]
            )
            rows.append(
                {
                    "as_of": as_of,
                    "symbol": clean_symbol,
                    "source_provider": "Local market cache VIX proxy",
                    "source_url": str(prices_path),
                    "available_at": as_of,
                    "iv_rank": iv_rank,
                    "skew_zscore": pd.NA,
                    "vxn_level": pd.NA,
                    "risk_flag": risk_flag,
                    "checksum": checksum,
                    "downloaded_at": downloaded.isoformat(),
                    "data_quality_status": data_quality_status,
                    "vix_proxy_quality_status": "PASS",
                    "limitation": "VIX proxy only; VXN and skew vendor fields are missing.",
                }
            )

    canonical = pd.DataFrame(
        rows,
        columns=[
            *source.required_columns,
            "source_url",
            "downloaded_at",
            "data_quality_status",
            "vix_proxy_quality_status",
            "limitation",
        ],
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    canonical.to_csv(output, index=False)
    checksum = _file_sha256(output)
    _append_p2_manifest(
        manifest_path=manifest_path,
        row={
            "source_id": "options_iv_skew",
            "provider": "Local market cache VIX proxy",
            "source_level": source.source_level,
            "source_url": str(prices_path),
            "request_params": json.dumps(
                {
                    "source_symbol": options.source_symbol,
                    "symbols": symbols,
                    "as_of": _date_text(run_date),
                    "lookback_days": options.lookback_days,
                    "min_history_days": options.min_history_days,
                    "elevated_iv_rank": options.elevated_iv_rank,
                    "stress_iv_rank": options.stress_iv_rank,
                    "upstream_checksum": _file_sha256(prices_path),
                    "derivation": "vix_proxy_missing_vxn_and_skew",
                    "data_quality_status": data_quality_status,
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            "downloaded_at": downloaded.isoformat(),
            "row_count": len(canonical),
            "output_path": str(output),
            "checksum": checksum,
            "production_effect": "none",
        },
    )
    status = "DERIVED" if not canonical.empty else "LIMITED_HISTORY"
    return pd.DataFrame(
        [
            {
                "source_id": "options_iv_skew",
                "status": status,
                "prices_path": str(prices_path),
                "output_path": str(output),
                "missing_columns": "",
                "source_symbol": options.source_symbol,
                "raw_row_count": len(prices),
                "eligible_vix_row_count": len(vix),
                "row_count": len(canonical),
                "latest_as_of": _latest_as_of(canonical, source.as_of_column),
                "provider": "Local market cache VIX proxy",
                "source_url": str(prices_path),
                "downloaded_at": downloaded.isoformat(),
                "checksum": checksum,
                "manifest_path": str(manifest_path),
                "data_quality_status": data_quality_status,
                "vix_proxy_quality_status": "PASS",
                "limitation": "VIX proxy only; VXN and skew vendor fields are missing.",
                "production_effect": "none",
            }
        ]
    )


def normalize_options_risk_source(
    *,
    source: ETFP2SourceConfig,
    p2_config: ETFP2Config,
    input_path: Path,
    provider: str,
    source_url: str,
    output_path: Path | None = None,
    manifest_path: Path,
    downloaded_at: datetime | None = None,
    symbol_column: str | None = None,
    as_of_column: str | None = None,
    available_at_column: str | None = None,
    iv_rank_column: str | None = None,
    skew_zscore_column: str | None = None,
    vxn_level_column: str | None = None,
    risk_flag_column: str | None = None,
) -> pd.DataFrame:
    raw = _read_required_frame(input_path)
    output = output_path or _resolve_repo_path(source.input_path)
    downloaded = downloaded_at or datetime.now(UTC)
    symbol_col = symbol_column or _find_column(raw, OPTIONS_SYMBOL_ALIASES)
    as_of_col = as_of_column or _find_column(raw, OPTIONS_AS_OF_ALIASES)
    available_col = available_at_column or _find_column(raw, OPTIONS_AVAILABLE_AT_ALIASES)
    iv_rank_col = iv_rank_column or _find_column(raw, OPTIONS_IV_RANK_ALIASES)
    skew_col = skew_zscore_column or _find_column(raw, OPTIONS_SKEW_ZSCORE_ALIASES)
    vxn_col = vxn_level_column or _find_column(raw, OPTIONS_VXN_LEVEL_ALIASES)
    risk_flag_col = risk_flag_column or _find_column(raw, OPTIONS_RISK_FLAG_ALIASES)
    missing_columns = [
        name
        for name, column in (
            ("symbol", symbol_col),
            ("as_of", as_of_col),
            ("available_at", available_col),
            ("iv_rank", iv_rank_col),
            ("skew_zscore", skew_col),
            ("vxn_level", vxn_col),
        )
        if column is None or column not in raw.columns
    ]
    if missing_columns:
        return _source_normalize_failure(
            source_id="options_iv_skew",
            status="FAILED_SCHEMA",
            input_path=input_path,
            output_path=output,
            missing_columns=missing_columns,
            raw_row_count=len(raw),
            provider=provider,
            source_url=source_url,
            downloaded_at=downloaded,
            manifest_path=manifest_path,
        )

    rows = []
    invalid_row_count = 0
    for _, row in raw.iterrows():
        symbol = str(row[symbol_col]).strip().upper()
        as_of_timestamp = _parse_timestamp(row[as_of_col])
        available_timestamp = _parse_timestamp(row[available_col])
        iv_rank = _parse_fraction(row[iv_rank_col])
        skew_zscore = _parse_float(row[skew_col])
        vxn_level = _parse_positive_float(row[vxn_col])
        if (
            not symbol
            or symbol.lower() == "nan"
            or pd.isna(as_of_timestamp)
            or pd.isna(available_timestamp)
            or iv_rank is None
            or skew_zscore is None
            or vxn_level is None
        ):
            invalid_row_count += 1
            continue
        as_of_text = as_of_timestamp.date().isoformat()
        row_source_url = _first_present(row, ["source_url", "url", "link"]) or source_url
        risk_flag = (
            str(row[risk_flag_col]).strip()
            if risk_flag_col and risk_flag_col in row.index and pd.notna(row[risk_flag_col])
            else ""
        )
        if not risk_flag:
            risk_flag = _vendor_options_risk_flag(iv_rank, p2_config)
        checksum = _row_checksum(
            [
                as_of_text,
                symbol,
                provider,
                row_source_url,
                available_timestamp.isoformat(),
                iv_rank,
                skew_zscore,
                vxn_level,
                risk_flag,
            ]
        )
        rows.append(
            {
                "as_of": as_of_text,
                "symbol": symbol,
                "source_provider": provider,
                "available_at": available_timestamp.isoformat(),
                "iv_rank": iv_rank,
                "skew_zscore": skew_zscore,
                "vxn_level": vxn_level,
                "risk_flag": risk_flag,
                "checksum": checksum,
                "source_url": row_source_url,
                "downloaded_at": downloaded.isoformat(),
                "limitation": (
                    "Vendor/manual options input; observe-only and not connected to "
                    "production signals."
                ),
            }
        )

    canonical_columns = [
        *source.required_columns,
        "source_url",
        "downloaded_at",
        "limitation",
    ]
    normalized = pd.DataFrame(rows, columns=canonical_columns)
    if normalized.empty:
        failure = _source_normalize_failure(
            source_id="options_iv_skew",
            status="FAILED_VALUES",
            input_path=input_path,
            output_path=output,
            missing_columns=[],
            raw_row_count=len(raw),
            provider=provider,
            source_url=source_url,
            downloaded_at=downloaded,
            manifest_path=manifest_path,
        )
        failure["invalid_row_count"] = invalid_row_count
        return failure

    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        existing = _read_required_frame(output)
        combined_columns = list(dict.fromkeys([*canonical_columns, *existing.columns]))
        existing = existing.reindex(columns=combined_columns)
        normalized = normalized.reindex(columns=combined_columns)
        normalized_keys = set(_options_source_keys(normalized))
        existing_keys = _options_source_keys(existing)
        existing = existing.loc[~existing_keys.isin(normalized_keys)].copy()
        combined = pd.concat([existing, normalized], ignore_index=True)
    else:
        combined = normalized
    combined = combined.sort_values(
        ["as_of", "symbol", "source_provider"],
        na_position="last",
    ).reset_index(drop=True)
    combined.to_csv(output, index=False)
    output_checksum = _file_sha256(output)
    _append_p2_manifest(
        manifest_path=manifest_path,
        row={
            "source_id": "options_iv_skew",
            "provider": provider,
            "source_level": source.source_level,
            "source_url": source_url,
            "request_params": json.dumps(
                {
                    "input_path": str(input_path),
                    "symbol_column": symbol_col,
                    "as_of_column": as_of_col,
                    "available_at_column": available_col,
                    "iv_rank_column": iv_rank_col,
                    "skew_zscore_column": skew_col,
                    "vxn_level_column": vxn_col,
                    "risk_flag_column": risk_flag_col or "derived_from_iv_rank_thresholds",
                    "elevated_iv_rank": p2_config.options_risk.elevated_iv_rank,
                    "stress_iv_rank": p2_config.options_risk.stress_iv_rank,
                    "raw_checksum": _file_sha256(input_path),
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            "downloaded_at": downloaded.isoformat(),
            "row_count": len(normalized),
            "output_path": str(output),
            "checksum": output_checksum,
            "production_effect": "none",
        },
    )
    return pd.DataFrame(
        [
            {
                "source_id": "options_iv_skew",
                "status": "NORMALIZED",
                "input_path": str(input_path),
                "output_path": str(output),
                "missing_columns": "",
                "raw_row_count": len(raw),
                "invalid_row_count": invalid_row_count,
                "row_count": len(normalized),
                "output_row_count": len(combined),
                "latest_as_of": _latest_as_of(normalized, source.as_of_column),
                "risk_flag_summary": json.dumps(
                    normalized["risk_flag"].value_counts().to_dict(),
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "provider": provider,
                "source_url": source_url,
                "downloaded_at": downloaded.isoformat(),
                "checksum": output_checksum,
                "manifest_path": str(manifest_path),
                "production_effect": "none",
            }
        ]
    )


def normalize_etf_holdings_source(
    *,
    source: ETFP2SourceConfig,
    input_path: Path,
    etf_symbol: str,
    provider: str,
    source_url: str,
    as_of,
    output_path: Path | None = None,
    manifest_path: Path,
    downloaded_at: datetime | None = None,
    holding_symbol_column: str | None = None,
    holding_weight_column: str | None = None,
) -> pd.DataFrame:
    raw = _read_required_frame(input_path)
    output = output_path or _resolve_repo_path(source.input_path)
    downloaded = downloaded_at or datetime.now(UTC)
    symbol_column = holding_symbol_column or _find_column(raw, HOLDING_SYMBOL_ALIASES)
    weight_column = holding_weight_column or _find_column(raw, HOLDING_WEIGHT_ALIASES)
    missing_columns = [
        name
        for name, column in (
            ("holding_symbol", symbol_column),
            ("holding_weight", weight_column),
        )
        if column is None or column not in raw.columns
    ]
    if missing_columns:
        return pd.DataFrame(
            [
                {
                    "source_id": "etf_holdings",
                    "status": "FAILED_SCHEMA",
                    "input_path": str(input_path),
                    "output_path": str(output),
                    "missing_columns": ",".join(missing_columns),
                    "raw_row_count": len(raw),
                    "row_count": 0,
                    "provider": provider,
                    "source_url": source_url,
                    "downloaded_at": downloaded.isoformat(),
                    "checksum": "",
                    "manifest_path": str(manifest_path),
                    "production_effect": "none",
                }
            ]
        )

    normalized_rows = []
    as_of_text = _date_text(as_of)
    clean_etf_symbol = etf_symbol.upper()
    for _, row in raw.iterrows():
        holding_symbol = str(row[symbol_column]).strip().upper()
        holding_weight = _parse_weight(row[weight_column])
        if not holding_symbol or holding_symbol.lower() == "nan" or holding_weight is None:
            continue
        checksum = _row_checksum(
            [
                as_of_text,
                clean_etf_symbol,
                holding_symbol,
                holding_weight,
                provider,
                source_url,
                downloaded.isoformat(),
            ]
        )
        normalized_rows.append(
            {
                "as_of": as_of_text,
                "etf_symbol": clean_etf_symbol,
                "holding_symbol": holding_symbol,
                "holding_weight": holding_weight,
                "source_provider": provider,
                "source_url": source_url,
                "downloaded_at": downloaded.isoformat(),
                "checksum": checksum,
            }
        )

    normalized = pd.DataFrame(normalized_rows, columns=source.required_columns)
    if normalized.empty:
        return pd.DataFrame(
            [
                {
                    "source_id": "etf_holdings",
                    "status": "FAILED_VALUES",
                    "input_path": str(input_path),
                    "output_path": str(output),
                    "missing_columns": "",
                    "raw_row_count": len(raw),
                    "row_count": 0,
                    "provider": provider,
                    "source_url": source_url,
                    "downloaded_at": downloaded.isoformat(),
                    "checksum": "",
                    "manifest_path": str(manifest_path),
                    "production_effect": "none",
                }
            ]
        )

    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        existing = _read_required_frame(output)
        existing = existing[source.required_columns].copy()
        existing = existing.loc[
            ~(
                (existing["as_of"].astype(str) == as_of_text)
                & (existing["etf_symbol"].astype(str).str.upper() == clean_etf_symbol)
            )
        ].copy()
        combined = pd.concat([existing, normalized], ignore_index=True)
    else:
        combined = normalized
    combined = combined.sort_values(
        ["as_of", "etf_symbol", "holding_weight", "holding_symbol"],
        ascending=[True, True, False, True],
    ).reset_index(drop=True)
    combined.to_csv(output, index=False)
    output_checksum = _file_sha256(output)
    _append_p2_manifest(
        manifest_path=manifest_path,
        row={
            "source_id": "etf_holdings",
            "provider": provider,
            "source_level": source.source_level,
            "source_url": source_url,
            "request_params": json.dumps(
                {
                    "input_path": str(input_path),
                    "as_of": as_of_text,
                    "etf_symbol": clean_etf_symbol,
                    "holding_symbol_column": symbol_column,
                    "holding_weight_column": weight_column,
                    "raw_checksum": _file_sha256(input_path),
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            "downloaded_at": downloaded.isoformat(),
            "row_count": len(normalized),
            "output_path": str(output),
            "checksum": output_checksum,
            "production_effect": "none",
        },
    )
    top = normalized.sort_values("holding_weight", ascending=False).iloc[0]
    return pd.DataFrame(
        [
            {
                "source_id": "etf_holdings",
                "status": "NORMALIZED",
                "input_path": str(input_path),
                "output_path": str(output),
                "missing_columns": "",
                "raw_row_count": len(raw),
                "row_count": len(normalized),
                "output_row_count": len(combined),
                "etf_symbol": clean_etf_symbol,
                "latest_as_of": as_of_text,
                "top_holding": str(top["holding_symbol"]),
                "top_holding_weight": float(top["holding_weight"]),
                "provider": provider,
                "source_url": source_url,
                "downloaded_at": downloaded.isoformat(),
                "checksum": output_checksum,
                "manifest_path": str(manifest_path),
                "production_effect": "none",
            }
        ]
    )


def normalize_news_theme_source(
    *,
    source: ETFP2SourceConfig,
    p2_config: ETFP2Config,
    input_path: Path,
    provider: str,
    source_url: str,
    output_path: Path | None = None,
    manifest_path: Path,
    downloaded_at: datetime | None = None,
    symbol_column: str | None = None,
    theme_column: str | None = None,
    summary_column: str | None = None,
    published_at_column: str | None = None,
    available_at_column: str | None = None,
    sentiment_column: str | None = None,
    relevance_column: str | None = None,
) -> pd.DataFrame:
    raw = _read_required_frame(input_path)
    output = output_path or _resolve_repo_path(source.input_path)
    downloaded = downloaded_at or datetime.now(UTC)
    symbol_col = symbol_column or _find_column(raw, NEWS_SYMBOL_ALIASES)
    theme_col = theme_column or _find_column(raw, NEWS_THEME_ALIASES)
    summary_col = summary_column or _find_column(raw, NEWS_SUMMARY_ALIASES)
    published_col = published_at_column or _find_column(raw, NEWS_PUBLISHED_AT_ALIASES)
    available_col = available_at_column or _find_column(raw, NEWS_AVAILABLE_AT_ALIASES)
    sentiment_col = sentiment_column or _find_column(raw, NEWS_SENTIMENT_ALIASES)
    relevance_col = relevance_column or _find_column(raw, NEWS_RELEVANCE_ALIASES)
    missing_columns = [
        name
        for name, column in (
            ("symbol", symbol_col),
            ("theme", theme_col),
            ("summary", summary_col),
            ("published_at", published_col),
        )
        if column is None or column not in raw.columns
    ]
    if p2_config.news_themes.require_explicit_sentiment and (
        sentiment_col is None or sentiment_col not in raw.columns
    ):
        missing_columns.append("sentiment_score")
    if missing_columns:
        return _source_normalize_failure(
            source_id="news_themes",
            status="FAILED_SCHEMA",
            input_path=input_path,
            output_path=output,
            missing_columns=missing_columns,
            raw_row_count=len(raw),
            provider=provider,
            source_url=source_url,
            downloaded_at=downloaded,
            manifest_path=manifest_path,
        )

    rows = []
    used_default_sentiment = False
    used_default_relevance = False
    for _, row in raw.iterrows():
        symbol = str(row[symbol_col]).strip().upper()
        theme = str(row[theme_col]).strip()
        summary = str(row[summary_col]).strip()
        published = _parse_timestamp(row[published_col])
        available = (
            _parse_timestamp(row[available_col]) if available_col else pd.Timestamp(downloaded)
        )
        if not symbol or symbol.lower() == "nan" or not theme or not summary:
            continue
        if pd.isna(published) or pd.isna(available):
            continue
        sentiment = (
            _parse_score(row[sentiment_col], lower=-1.0, upper=1.0) if sentiment_col else None
        )
        if sentiment is None:
            sentiment = p2_config.news_themes.neutral_sentiment_score
            used_default_sentiment = True
        relevance = (
            _parse_score(row[relevance_col], lower=0.0, upper=1.0) if relevance_col else None
        )
        if relevance is None:
            relevance = p2_config.news_themes.default_relevance_score
            used_default_relevance = True
        as_of_text = available.date().isoformat()
        source_url_value = _first_present(row, ["source_url", "url", "link"]) or source_url
        checksum = _row_checksum(
            [
                as_of_text,
                symbol,
                source_url_value,
                published.isoformat(),
                available.isoformat(),
                theme,
                sentiment,
                relevance,
                summary,
            ]
        )
        limitations = []
        if used_default_sentiment and sentiment_col is None:
            limitations.append("neutral_sentiment_default_used")
        if used_default_relevance and relevance_col is None:
            limitations.append("default_relevance_used")
        rows.append(
            {
                "as_of": as_of_text,
                "symbol": symbol,
                "source_provider": provider,
                "source_url": source_url_value,
                "published_at": published.isoformat(),
                "available_at": available.isoformat(),
                "theme": theme,
                "sentiment_score": sentiment,
                "relevance_score": relevance,
                "summary": summary,
                "checksum": checksum,
                "limitation": ";".join(limitations),
            }
        )

    normalized = pd.DataFrame(rows, columns=[*source.required_columns, "limitation"])
    if normalized.empty:
        return _source_normalize_failure(
            source_id="news_themes",
            status="FAILED_VALUES",
            input_path=input_path,
            output_path=output,
            missing_columns=[],
            raw_row_count=len(raw),
            provider=provider,
            source_url=source_url,
            downloaded_at=downloaded,
            manifest_path=manifest_path,
        )

    output.parent.mkdir(parents=True, exist_ok=True)
    normalized.to_csv(output, index=False)
    output_checksum = _file_sha256(output)
    _append_p2_manifest(
        manifest_path=manifest_path,
        row={
            "source_id": "news_themes",
            "provider": provider,
            "source_level": source.source_level,
            "source_url": source_url,
            "request_params": json.dumps(
                {
                    "input_path": str(input_path),
                    "symbol_column": symbol_col,
                    "theme_column": theme_col,
                    "summary_column": summary_col,
                    "published_at_column": published_col,
                    "available_at_column": available_col or "downloaded_at",
                    "sentiment_column": sentiment_col or "neutral_default",
                    "relevance_column": relevance_col or "default_relevance",
                    "raw_checksum": _file_sha256(input_path),
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            "downloaded_at": downloaded.isoformat(),
            "row_count": len(normalized),
            "output_path": str(output),
            "checksum": output_checksum,
            "production_effect": "none",
        },
    )
    return pd.DataFrame(
        [
            {
                "source_id": "news_themes",
                "status": "NORMALIZED",
                "input_path": str(input_path),
                "output_path": str(output),
                "missing_columns": "",
                "raw_row_count": len(raw),
                "row_count": len(normalized),
                "latest_as_of": _latest_as_of(normalized, source.as_of_column),
                "provider": provider,
                "source_url": source_url,
                "downloaded_at": downloaded.isoformat(),
                "used_default_sentiment": used_default_sentiment,
                "used_default_relevance": used_default_relevance,
                "checksum": output_checksum,
                "manifest_path": str(manifest_path),
                "production_effect": "none",
            }
        ]
    )


def build_news_theme_tracking_report(
    *,
    source: ETFP2SourceConfig,
    p2_config: ETFP2Config,
    run_date,
    input_path: Path | None = None,
) -> pd.DataFrame:
    contract = build_source_contract_report(
        source_id="news_themes",
        source=source,
        run_date=run_date,
        input_path=input_path,
    )
    if contract.iloc[0]["status"] not in {"PASS", "PASS_WITH_LIMITATIONS"}:
        return contract

    path = input_path or _resolve_repo_path(source.input_path)
    frame = _read_optional_frame(path)
    if frame is None:
        return contract
    parsed_as_of = pd.to_datetime(frame[source.as_of_column], errors="coerce")
    parsed_available = pd.to_datetime(frame[source.available_time_column], errors="coerce")
    run_day = pd.Timestamp(run_date).date()
    window_start = (
        pd.Timestamp(run_day) - pd.Timedelta(days=p2_config.news_themes.tracking_lookback_days - 1)
    ).date()
    selected = frame.loc[
        parsed_as_of.notna()
        & parsed_available.notna()
        & (parsed_as_of.dt.date <= run_day)
        & (parsed_available.dt.date <= run_day)
        & (parsed_available.dt.date >= window_start)
    ].copy()
    if selected.empty:
        row = contract.iloc[0].to_dict()
        row["status"] = "EMPTY_WINDOW"
        row["window_start"] = window_start.isoformat()
        row["window_end"] = run_day.isoformat()
        row["lookback_days"] = p2_config.news_themes.tracking_lookback_days
        return pd.DataFrame([row])

    selected["_sentiment"] = pd.to_numeric(selected["sentiment_score"], errors="coerce")
    selected["_relevance"] = pd.to_numeric(selected["relevance_score"], errors="coerce")
    selected["_available_at"] = parsed_available.loc[selected.index]
    selected["_published_at"] = pd.to_datetime(selected["published_at"], errors="coerce")
    rows = []
    for (symbol, theme), group in selected.groupby(["symbol", "theme"], dropna=False):
        valid_relevance = group["_relevance"].fillna(0.0).clip(lower=0.0)
        sentiment = group["_sentiment"]
        if float(valid_relevance.sum()) > 0:
            weighted_sentiment = float(
                (sentiment.fillna(0.0) * valid_relevance).sum() / valid_relevance.sum()
            )
        else:
            weighted_sentiment = float(sentiment.mean()) if sentiment.notna().any() else 0.0
        latest = group.sort_values(["_available_at", "_published_at"]).iloc[-1]
        source_limitations = sorted(
            {
                str(value)
                for value in group.get("limitation", pd.Series(dtype=object)).dropna()
                if str(value).strip()
            }
        )
        rows.append(
            {
                "date": _date_text(run_date),
                "module": "news_theme_tracking",
                "status": "TRACKED",
                "symbol": str(symbol),
                "theme": str(theme),
                "event_count": int(len(group)),
                "weighted_sentiment": weighted_sentiment,
                "avg_relevance": float(valid_relevance.mean()) if len(valid_relevance) else 0.0,
                "latest_published_at": _timestamp_text(latest.get("_published_at")),
                "latest_available_at": _timestamp_text(latest.get("_available_at")),
                "latest_summary": str(latest.get("summary", "")),
                "latest_source_url": str(latest.get("source_url", "")),
                "source_provider": str(latest.get("source_provider", "")),
                "source_limitation": ";".join(source_limitations),
                "window_start": window_start.isoformat(),
                "window_end": run_day.isoformat(),
                "lookback_days": p2_config.news_themes.tracking_lookback_days,
                "policy_version": p2_config.policy_metadata.version,
                "candidate_only": p2_config.news_themes.candidate_only,
                "auto_promotion": p2_config.news_themes.auto_promotion,
                "limitation": (
                    "Observe-only news theme tracking; no LLM inference, trading advice, "
                    "or production allocation impact."
                ),
                "production_effect": "none",
            }
        )
    report = pd.DataFrame(rows).sort_values(
        ["event_count", "avg_relevance", "symbol", "theme"],
        ascending=[False, False, True, True],
    )
    return report.head(p2_config.news_themes.max_report_rows).reset_index(drop=True)


def build_holdings_lookthrough_report(
    *,
    source: ETFP2SourceConfig,
    run_date,
    input_path: Path | None = None,
) -> pd.DataFrame:
    contract = build_source_contract_report(
        source_id="etf_holdings",
        source=source,
        run_date=run_date,
        input_path=input_path,
    )
    if contract.iloc[0]["status"] not in {"PASS", "PASS_WITH_LIMITATIONS"}:
        return contract

    path = input_path or _resolve_repo_path(source.input_path)
    frame = _read_optional_frame(path)
    if frame is None:
        return contract
    selected = _select_rows_as_of(frame, source.as_of_column, run_date)
    if selected.empty:
        row = contract.iloc[0].to_dict()
        row["status"] = "EMPTY_FOR_DATE"
        return pd.DataFrame([row])

    selected = selected.copy()
    selected["_weight"] = pd.to_numeric(selected["holding_weight"], errors="coerce")
    rows = []
    for etf_symbol, group in selected.groupby("etf_symbol", sort=True):
        valid = group.loc[group["_weight"].notna()].copy()
        if valid.empty:
            rows.append(
                {
                    "date": _date_text(run_date),
                    "module": "etf_holdings",
                    "status": "FAILED_SCHEMA",
                    "etf_symbol": etf_symbol,
                    "holding_count": 0,
                    "weight_sum": None,
                    "top_holding": "",
                    "top_holding_weight": None,
                    "production_effect": "none",
                }
            )
            continue
        top = valid.sort_values("_weight", ascending=False).iloc[0]
        rows.append(
            {
                "date": _date_text(run_date),
                "module": "etf_holdings",
                "status": (
                    "PASS_WITH_LIMITATIONS" if source.provider_status != "connected" else "PASS"
                ),
                "etf_symbol": etf_symbol,
                "holding_count": len(valid),
                "weight_sum": float(valid["_weight"].sum()),
                "top_holding": str(top["holding_symbol"]),
                "top_holding_weight": float(top["_weight"]),
                "source_provider": str(top["source_provider"]),
                "latest_as_of": _latest_as_of(valid, source.as_of_column),
                "production_effect": "none",
            }
        )
    return pd.DataFrame(rows)


def build_advanced_risk_report(
    *,
    allocation: pd.DataFrame,
    prices: pd.DataFrame,
    config: ETFConfigBundle,
    quality_report: ETFQualityReport,
    run_date,
) -> pd.DataFrame:
    if config.p2 is None:
        raise ValueError("ETF P2 config is required")
    p2 = config.p2.advanced_risk
    selected_allocation = _allocation_for_date(allocation, run_date)
    pivot = _price_pivot(prices)
    returns = pivot.pct_change().dropna().tail(p2.covariance_window)
    weights = _weight_vector(selected_allocation, returns.columns)
    reasons = []
    status = "PASS"
    if len(returns) < p2.covariance_window:
        status = "LIMITED_HISTORY"
        reasons.append("INSUFFICIENT_COVARIANCE_WINDOW")

    portfolio_vol = _portfolio_volatility(returns, weights)
    max_weight = max(weights.values()) if weights else 0.0
    avg_corr = _average_pairwise_correlation(returns)
    if max_weight > p2.concentration_warning_weight:
        status = "WARNING"
        reasons.append("CONCENTRATION_ABOVE_POLICY")
    if portfolio_vol is not None and portfolio_vol > p2.volatility_warning:
        status = "WARNING"
        reasons.append("VOLATILITY_ABOVE_POLICY")
    if avg_corr is not None and avg_corr > p2.correlation_warning:
        status = "WARNING"
        reasons.append("CORRELATION_ABOVE_POLICY")

    return pd.DataFrame(
        [
            {
                "date": _date_text(run_date),
                "module": "advanced_risk",
                "status": status,
                "portfolio_volatility": portfolio_vol,
                "max_target_weight": max_weight,
                "average_pairwise_correlation": avg_corr,
                "covariance_window": p2.covariance_window,
                "data_quality_status": quality_report.status,
                "reason_codes": json.dumps(reasons, ensure_ascii=False),
                "production_effect": "none",
            }
        ]
    )


def build_walk_forward_readiness_report(
    *,
    backtest_dir: Path,
    p2_config: ETFP2Config,
    run_date,
) -> pd.DataFrame:
    summaries = _backtest_summaries(backtest_dir)
    versions = {
        str(item.get("model_version", "")) for item in summaries if item.get("model_version")
    }
    status = "READY"
    reasons = []
    if len(summaries) < p2_config.walk_forward.min_completed_runs:
        status = "NOT_READY"
        reasons.append("INSUFFICIENT_BACKTEST_RUNS")
    if len(versions) < p2_config.walk_forward.min_distinct_model_versions:
        status = "NOT_READY"
        reasons.append("INSUFFICIENT_MODEL_VERSION_DIVERSITY")
    if not summaries:
        latest_run_id = ""
    else:
        latest_run_id = str(summaries[-1].get("run_id", ""))
    return pd.DataFrame(
        [
            {
                "date": _date_text(run_date),
                "module": "walk_forward",
                "status": status,
                "completed_run_count": len(summaries),
                "distinct_model_versions": len(versions),
                "latest_run_id": latest_run_id,
                "required_no_lookahead_evidence": NO_LOOKAHEAD_TEST_ID,
                "reason_codes": json.dumps(reasons, ensure_ascii=False),
                "production_effect": "none",
            }
        ]
    )


def build_ml_ranking_candidates(
    signals: pd.DataFrame,
    *,
    p2_config: ETFP2Config,
    run_date,
) -> pd.DataFrame:
    selected = _signals_for_date(signals, run_date)
    rows = []
    for _, row in selected.iterrows():
        score = (
            p2_config.ml_ranking.composite_weight * _float(row.get("composite_score"))
            + p2_config.ml_ranking.relative_strength_weight
            * _float(row.get("relative_strength_score"))
            + p2_config.ml_ranking.risk_weight * _float(row.get("risk_score"))
        )
        rows.append(
            {
                "date": _date_text(run_date),
                "module": "ml_ranking",
                "symbol": str(row["symbol"]),
                "candidate_score": score,
                "base_composite_score": _float(row.get("composite_score")),
                "relative_strength_score": _float(row.get("relative_strength_score")),
                "risk_score": _float(row.get("risk_score")),
                "model_stage": "candidate_only",
                "auto_promotion": p2_config.ml_ranking.auto_promotion,
                "production_effect": "none",
            }
        )
    frame = pd.DataFrame(rows).sort_values("candidate_score", ascending=False)
    frame["rank"] = range(1, len(frame) + 1)
    return frame.reset_index(drop=True)


def build_weight_optimizer_candidates(
    *,
    signals: pd.DataFrame,
    prices: pd.DataFrame,
    config: ETFConfigBundle,
    quality_report: ETFQualityReport,
    run_date,
) -> pd.DataFrame:
    if config.p2 is None:
        raise ValueError("ETF P2 config is required for weight optimizer")
    optimizer = config.p2.weight_optimizer
    selected = _signals_for_date(signals, run_date)
    tradeable_symbols = [
        symbol
        for symbol in config.assets.tradeable_symbols
        if symbol != "CASH" and symbol in set(selected["symbol"].astype(str))
    ]
    pivot = _price_pivot(prices)
    pivot = pivot.loc[pivot.index <= pd.Timestamp(run_date)].copy()
    returns = pivot[tradeable_symbols].pct_change().dropna(how="all").tail(optimizer.lookback_days)
    history_counts = returns.count()
    eligible_symbols = [
        symbol
        for symbol in tradeable_symbols
        if history_counts.get(symbol, 0) >= optimizer.min_history_days
    ]
    signal_rows = selected.set_index("symbol")
    if not eligible_symbols:
        return _weight_optimizer_cash_only_frame(
            config=config,
            quality_report=quality_report,
            run_date=run_date,
            status="LIMITED_HISTORY",
            limitation="No ETF symbol has enough price history for candidate optimization.",
        )

    components = {}
    annualized_returns = returns[eligible_symbols].mean() * 252
    annualized_volatility = returns[eligible_symbols].std(ddof=0) * sqrt(252)
    risk_adjusted = annualized_returns / annualized_volatility.replace(0, pd.NA)
    risk_adjusted_component = _min_max_component(risk_adjusted)
    inverse_volatility_component = _min_max_component(1 / annualized_volatility.replace(0, pd.NA))
    scores = {}
    for symbol in eligible_symbols:
        signal_component = _clamp01(_float(signal_rows.loc[symbol, "composite_score"]) / 100.0)
        risk_component = risk_adjusted_component.get(symbol, 0.0)
        inverse_vol_component = inverse_volatility_component.get(symbol, 0.0)
        candidate_score = (
            optimizer.signal_score_weight * signal_component
            + optimizer.risk_adjusted_return_weight * risk_component
            + optimizer.inverse_volatility_weight * inverse_vol_component
        )
        components[symbol] = {
            "signal_component": signal_component,
            "risk_adjusted_return_component": risk_component,
            "inverse_volatility_component": inverse_vol_component,
            "annualized_return": annualized_returns.get(symbol),
            "annualized_volatility": annualized_volatility.get(symbol),
            "candidate_score": candidate_score,
        }
        scores[symbol] = max(0.0, candidate_score)

    equity_budget = max(0.0, 1.0 - optimizer.min_cash_weight)
    caps = {
        symbol: min(optimizer.max_candidate_weight, config.assets.assets[symbol].max_weight)
        for symbol in eligible_symbols
    }
    candidate_weights = _candidate_weights_from_scores(scores, equity_budget, caps)
    cash_weight = max(0.0, 1.0 - sum(candidate_weights.values()))
    rows = []
    for symbol in tradeable_symbols:
        component = components.get(symbol, {})
        symbol_status = "CANDIDATE_ONLY" if symbol in eligible_symbols else "LIMITED_HISTORY"
        rows.append(
            {
                "date": _date_text(run_date),
                "module": "weight_optimizer",
                "status": symbol_status,
                "symbol": symbol,
                "candidate_weight": candidate_weights.get(symbol, 0.0),
                "candidate_score": component.get("candidate_score", 0.0),
                "signal_component": component.get("signal_component", 0.0),
                "risk_adjusted_return_component": component.get(
                    "risk_adjusted_return_component",
                    0.0,
                ),
                "inverse_volatility_component": component.get(
                    "inverse_volatility_component",
                    0.0,
                ),
                "annualized_return": component.get("annualized_return", pd.NA),
                "annualized_volatility": component.get("annualized_volatility", pd.NA),
                "history_days": int(history_counts.get(symbol, 0)),
                "lookback_days": optimizer.lookback_days,
                "min_history_days": optimizer.min_history_days,
                "max_candidate_weight": caps.get(symbol, 0.0),
                "data_quality_status": quality_report.status,
                "config_hash": config.config_hash,
                "model_stage": "candidate_only",
                "candidate_only": optimizer.candidate_only,
                "auto_promotion": optimizer.auto_promotion,
                "limitation": (
                    "Candidate-only heuristic optimizer; does not write target weights, "
                    "rebalance instructions, or trading actions."
                ),
                "production_effect": "none",
            }
        )
    rows.append(
        {
            "date": _date_text(run_date),
            "module": "weight_optimizer",
            "status": "CASH_ABSORBS_UNALLOCATED",
            "symbol": "CASH",
            "candidate_weight": cash_weight,
            "candidate_score": 0.0,
            "signal_component": 0.0,
            "risk_adjusted_return_component": 0.0,
            "inverse_volatility_component": 0.0,
            "annualized_return": pd.NA,
            "annualized_volatility": pd.NA,
            "history_days": 0,
            "lookback_days": optimizer.lookback_days,
            "min_history_days": optimizer.min_history_days,
            "max_candidate_weight": 1.0,
            "data_quality_status": quality_report.status,
            "config_hash": config.config_hash,
            "model_stage": "candidate_only",
            "candidate_only": optimizer.candidate_only,
            "auto_promotion": optimizer.auto_promotion,
            "limitation": (
                "Cash absorbs optimizer residual; candidate-only output is not a "
                "production allocation."
            ),
            "production_effect": "none",
        }
    )
    return pd.DataFrame(rows)


def build_ensemble_candidates(
    signals: pd.DataFrame,
    ml_candidates: pd.DataFrame,
    *,
    p2_config: ETFP2Config,
    run_date,
) -> pd.DataFrame:
    signal_rows = _signals_for_date(signals, run_date).set_index("symbol")
    ml_rows = ml_candidates.set_index("symbol") if not ml_candidates.empty else pd.DataFrame()
    rows = []
    for symbol, row in signal_rows.iterrows():
        ml_score = (
            _float(ml_rows.loc[symbol, "candidate_score"])
            if symbol in getattr(ml_rows, "index", [])
            else _float(row.get("composite_score"))
        )
        p0_score = _float(row.get("composite_score"))
        ensemble_score = (
            p2_config.ensemble.p0_signal_weight * p0_score
            + p2_config.ensemble.ml_candidate_weight * ml_score
        )
        rows.append(
            {
                "date": _date_text(run_date),
                "module": "ensemble",
                "symbol": str(symbol),
                "p0_signal_score": p0_score,
                "ml_candidate_score": ml_score,
                "ensemble_candidate_score": ensemble_score,
                "model_stage": "candidate_only",
                "auto_promotion": p2_config.ensemble.auto_promotion,
                "production_effect": "none",
            }
        )
    frame = pd.DataFrame(rows).sort_values("ensemble_candidate_score", ascending=False)
    frame["rank"] = range(1, len(frame) + 1)
    return frame.reset_index(drop=True)


def build_live_interface_preflight(
    *,
    p2_config: ETFP2Config,
    run_date,
) -> pd.DataFrame:
    live = p2_config.live_interface
    return pd.DataFrame(
        [
            {
                "date": _date_text(run_date),
                "module": "live_interface_preflight",
                "status": "BLOCKED_BY_POLICY",
                "enabled": live.enabled,
                "paper_only": live.paper_only,
                "read_only": live.read_only,
                "broker_routing_allowed": live.broker_routing_allowed,
                "multi_account_enabled": live.multi_account_enabled,
                "broker_order_route_called": False,
                "production_effect": "none",
            }
        ]
    )


def p2_metadata(config: ETFConfigBundle) -> dict[str, object]:
    version = config.p2.policy_metadata.version if config.p2 else "missing"
    return {
        "p2_policy_version": version,
        "auto_promotion": "false",
    }


def _resolve_repo_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def _read_optional_frame(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def _read_required_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"ETF P2 source input does not exist: {path}")
    frame = _read_optional_frame(path)
    if frame is None:
        raise FileNotFoundError(f"ETF P2 source input does not exist: {path}")
    return frame


def _append_p2_manifest(*, manifest_path: Path, row: dict[str, object]) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame([row])
    if manifest_path.exists():
        existing = pd.read_csv(manifest_path)
        frame = pd.concat([existing, frame], ignore_index=True)
    frame.to_csv(manifest_path, index=False)


def _edgar_text_index_keys(frame: pd.DataFrame) -> pd.Series:
    if frame.empty:
        return pd.Series(dtype=str)
    parts = [
        frame[column].astype(str) if column in frame.columns else pd.Series("", index=frame.index)
        for column in ("as_of", "symbol", "filing_type", "accession_number", "source_url")
    ]
    key = parts[0]
    for part in parts[1:]:
        key = key + "|" + part
    return key


def _fetch_edgar_text_document_row(
    *,
    row: pd.Series,
    document_dir: Path,
    user_agent: str,
    timeout_seconds: float,
) -> dict[str, object]:
    symbol = str(row["ticker"]).upper()
    filing_type = str(row["form"]).upper()
    source_url = str(row["source_url"]).strip()
    accession = _first_present(row, ["accession_number", "accession_number_compact"])
    filed_at = pd.Timestamp(row["_filed_at"]).date().isoformat()
    available_at = pd.Timestamp(row["_available_at"]).isoformat()
    as_of = pd.Timestamp(row["_as_of"]).date().isoformat()
    base = {
        "as_of": as_of,
        "symbol": symbol,
        "source_provider": "SEC EDGAR filing documents",
        "source_url": source_url,
        "filing_type": filing_type,
        "filed_at": filed_at,
        "available_at": available_at,
        "accession_number": accession,
        "document_text_path": "",
        "document_text_checksum": "",
        "text_character_count": 0,
        "text_excerpt": "",
        "fetch_status": "",
        "limitation": (
            "Official filing text cache only; no financial statement interpretation, "
            "sentiment inference, or investment conclusion."
        ),
        "checksum": "",
        "production_effect": "none",
    }
    try:
        payload = _read_document_bytes(
            source_url,
            user_agent=user_agent,
            timeout_seconds=timeout_seconds,
        )
        text = _document_bytes_to_visible_text(payload, source_url)
    except (OSError, RuntimeError, ValueError) as exc:
        reason = type(exc).__name__
        checksum = _row_checksum([as_of, symbol, filing_type, source_url, reason])
        return {
            **base,
            "fetch_status": "FAILED_FETCH",
            "limitation": f"{base['limitation']} fetch_error={reason}.",
            "checksum": checksum,
        }
    if not text:
        checksum = _row_checksum([as_of, symbol, filing_type, source_url, "EMPTY_TEXT"])
        return {
            **base,
            "fetch_status": "FAILED_EMPTY_TEXT",
            "limitation": f"{base['limitation']} extracted_text_empty.",
            "checksum": checksum,
        }

    document_name = "_".join(
        part
        for part in (
            _safe_slug(symbol),
            _safe_slug(filing_type),
            _safe_slug(accession or "no_accession"),
            _row_checksum([source_url])[:12],
        )
        if part
    )
    document_path = document_dir / f"{document_name}.txt"
    document_path.write_text(text, encoding="utf-8")
    text_checksum = _file_sha256(document_path)
    checksum = _row_checksum(
        [
            as_of,
            symbol,
            filing_type,
            filed_at,
            available_at,
            accession,
            source_url,
            text_checksum,
        ]
    )
    return {
        **base,
        "document_text_path": str(document_path),
        "document_text_checksum": text_checksum,
        "text_character_count": len(text),
        "text_excerpt": text[:EDGAR_TEXT_EXCERPT_CHAR_LIMIT],
        "fetch_status": "FETCHED",
        "checksum": checksum,
    }


def _read_document_bytes(source_url: str, *, user_agent: str, timeout_seconds: float) -> bytes:
    parsed = urlparse(source_url)
    if parsed.scheme in ("http", "https"):
        if not user_agent.strip():
            raise ValueError("SEC filing HTTP fetch requires SEC_USER_AGENT or --user-agent")
        request = Request(
            source_url,
            headers={
                "User-Agent": user_agent,
                "Accept-Encoding": "identity",
            },
        )
        with urlopen(request, timeout=timeout_seconds) as response:
            return response.read()
    if parsed.scheme == "file":
        path = Path(unquote(parsed.path))
        if os.name == "nt" and str(path).startswith("\\") and len(str(path)) > 3:
            path_text = str(path).lstrip("\\")
            if path_text[1:3] == ":\\":
                path = Path(path_text)
    else:
        path = Path(source_url)
    if not path.exists():
        raise FileNotFoundError(f"EDGAR text source does not exist: {source_url}")
    return path.read_bytes()


def _document_bytes_to_visible_text(payload: bytes, source_url: str) -> str:
    text = payload.decode("utf-8", errors="replace")
    lowered = source_url.lower()
    if lowered.endswith((".htm", ".html")) or _looks_like_html(text):
        text = _extract_html_text(text)
    return _normalize_whitespace(text)


def _looks_like_html(text: str) -> bool:
    sample = text[:1000].lower()
    return "<html" in sample or "<body" in sample or "</p>" in sample or "<div" in sample


def _extract_html_text(text: str) -> str:
    parser = _VisibleTextHTMLParser()
    parser.feed(text)
    parser.close()
    return parser.text()


class _VisibleTextHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._parts: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs) -> None:
        del attrs
        if tag.lower() in {"script", "style"}:
            self._skip_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"script", "style"} and self._skip_depth:
            self._skip_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._skip_depth:
            return
        stripped = data.strip()
        if stripped:
            self._parts.append(stripped)

    def text(self) -> str:
        return _normalize_whitespace(" ".join(self._parts))


def _normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _safe_slug(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return slug.strip("._-")


def _keyword_occurrence_count(normalized_text: str, keyword: str) -> int:
    normalized_keyword = _normalize_whitespace(str(keyword).lower())
    if not normalized_keyword:
        return 0
    return normalized_text.count(normalized_keyword)


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _row_checksum(values: list[object]) -> str:
    payload = "|".join("" if value is None else str(value) for value in values)
    return sha256(payload.encode("utf-8")).hexdigest()


def _coalesced_datetime(frame: pd.DataFrame, columns: list[str]) -> pd.Series:
    result = pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns, UTC]")
    for column in columns:
        if column not in frame.columns:
            continue
        parsed = pd.to_datetime(frame[column], errors="coerce", utc=True)
        result = result.fillna(parsed)
    return result


def _first_present(row: pd.Series, columns: list[str]) -> str:
    for column in columns:
        if column not in row.index:
            continue
        value = row[column]
        if pd.notna(value) and str(value).strip():
            return str(value)
    return ""


def _find_column(frame: pd.DataFrame, aliases: tuple[str, ...]) -> str | None:
    normalized = {_normalize_column_name(column): column for column in frame.columns}
    for alias in aliases:
        column = normalized.get(_normalize_column_name(alias))
        if column is not None:
            return column
    return None


def _normalize_column_name(value: object) -> str:
    return str(value).strip().lower().replace("_", "").replace("-", "").replace(" ", "")


def _parse_weight(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    is_percent = text.endswith("%")
    if is_percent:
        text = text[:-1].strip()
    try:
        parsed = float(text)
    except ValueError:
        return None
    if is_percent or parsed > 1:
        parsed = parsed / 100.0
    if parsed < 0 or parsed > 1:
        return None
    return parsed


def _parse_fraction(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    is_percent = text.endswith("%")
    if is_percent:
        text = text[:-1].strip()
    try:
        parsed = float(text)
    except ValueError:
        return None
    if is_percent or parsed > 1:
        parsed = parsed / 100.0
    if pd.isna(parsed) or parsed < 0 or parsed > 1:
        return None
    return parsed


def _parse_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        parsed = float(str(value).strip().replace(",", ""))
    except ValueError:
        return None
    if pd.isna(parsed):
        return None
    return parsed


def _parse_positive_float(value: object) -> float | None:
    parsed = _parse_float(value)
    if parsed is None or parsed <= 0:
        return None
    return parsed


def _parse_score(value: object, *, lower: float, upper: float) -> float | None:
    try:
        parsed = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed) or parsed < lower or parsed > upper:
        return None
    return parsed


def _parse_timestamp(value: object) -> pd.Timestamp:
    try:
        return pd.Timestamp(value)
    except (TypeError, ValueError):
        return pd.NaT


def _timestamp_text(value: object) -> str:
    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        return ""
    return timestamp.isoformat()


def _source_normalize_failure(
    *,
    source_id: str,
    status: str,
    input_path: Path,
    output_path: Path,
    missing_columns: list[str],
    raw_row_count: int,
    provider: str,
    source_url: str,
    downloaded_at: datetime,
    manifest_path: Path,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "source_id": source_id,
                "status": status,
                "input_path": str(input_path),
                "output_path": str(output_path),
                "missing_columns": ",".join(missing_columns),
                "raw_row_count": raw_row_count,
                "row_count": 0,
                "provider": provider,
                "source_url": source_url,
                "downloaded_at": downloaded_at.isoformat(),
                "checksum": "",
                "manifest_path": str(manifest_path),
                "production_effect": "none",
            }
        ]
    )


def _rolling_last_percentile(values: pd.Series) -> float:
    current = values.iloc[-1]
    if pd.isna(current):
        return float("nan")
    valid = pd.to_numeric(values, errors="coerce").dropna()
    if valid.empty:
        return float("nan")
    return float((valid <= current).sum() / len(valid))


def _vix_proxy_risk_flag(iv_rank: float, p2_config: ETFP2Config) -> str:
    options = p2_config.options_risk
    if iv_rank >= options.stress_iv_rank:
        return "VIX_PROXY_STRESS_MISSING_VXN_SKEW"
    if iv_rank >= options.elevated_iv_rank:
        return "VIX_PROXY_ELEVATED_MISSING_VXN_SKEW"
    return "VIX_PROXY_NORMAL_MISSING_VXN_SKEW"


def _vendor_options_risk_flag(iv_rank: float, p2_config: ETFP2Config) -> str:
    options = p2_config.options_risk
    if iv_rank >= options.stress_iv_rank:
        return "VENDOR_OPTIONS_STRESS"
    if iv_rank >= options.elevated_iv_rank:
        return "VENDOR_OPTIONS_ELEVATED"
    return "VENDOR_OPTIONS_NORMAL"


def _options_source_keys(frame: pd.DataFrame) -> pd.Series:
    return frame.apply(
        lambda row: (
            str(row.get("as_of", "")),
            str(row.get("symbol", "")).upper(),
            str(row.get("source_provider", "")),
        ),
        axis=1,
    )


def _source_base_row(
    source_id: str,
    source: ETFP2SourceConfig,
    run_date,
    path: Path,
) -> dict[str, object]:
    return {
        "date": _date_text(run_date),
        "module": source_id,
        "input_path": str(path),
        "provider_status": source.provider_status,
        "source_level": source.source_level,
        "required_columns": ",".join(source.required_columns),
        "as_of_column": source.as_of_column,
        "available_time_column": source.available_time_column,
        "missing_columns": "",
        "rows_as_of": 0,
        "latest_as_of": "",
        "pit_issue_count": 0,
        "numeric_summary": "",
        "data_quality_status": "",
        "source_quality_status": "",
        "limitation": "",
        "production_effect": "none",
    }


def _pit_issue_count(frame: pd.DataFrame, available_time_column: str, run_date) -> int:
    available = pd.to_datetime(frame[available_time_column], errors="coerce")
    parsed_date = pd.Timestamp(run_date).date()
    future_rows = available.notna() & (available.dt.date > parsed_date)
    return int(future_rows.sum())


def _select_rows_as_of(frame: pd.DataFrame, as_of_column: str, run_date) -> pd.DataFrame:
    parsed = pd.to_datetime(frame[as_of_column], errors="coerce")
    eligible = frame.loc[parsed.notna() & (parsed <= pd.Timestamp(run_date))].copy()
    if eligible.empty:
        return eligible
    latest = parsed.loc[eligible.index].max()
    return eligible.loc[parsed.loc[eligible.index] == latest].copy()


def _latest_as_of(frame: pd.DataFrame, as_of_column: str) -> str:
    parsed = pd.to_datetime(frame[as_of_column], errors="coerce").dropna()
    if parsed.empty:
        return ""
    return parsed.max().date().isoformat()


def _latest_text(frame: pd.DataFrame, column: str) -> str:
    if column not in frame.columns or frame.empty:
        return ""
    values = frame[column].dropna()
    if values.empty:
        return ""
    return str(values.iloc[-1])


def _numeric_summary(frame: pd.DataFrame) -> str:
    fields = [
        column
        for column in ("sentiment_score", "relevance_score", "iv_rank", "skew_zscore", "vxn_level")
        if column in frame.columns
    ]
    summary = {}
    for field in fields:
        values = pd.to_numeric(frame[field], errors="coerce").dropna()
        if not values.empty:
            summary[field] = round(float(values.mean()), 6)
    return json.dumps(summary, ensure_ascii=False, sort_keys=True)


def _allocation_for_date(allocation: pd.DataFrame, run_date) -> pd.DataFrame:
    frame = allocation.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    selected = frame.loc[frame["_date"] == pd.Timestamp(run_date)].copy()
    if selected.empty:
        raise ValueError(f"ETF allocation has no rows for {run_date}")
    return selected.drop(columns=["_date"])


def _price_pivot(prices: pd.DataFrame) -> pd.DataFrame:
    frame = prices.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    return frame.pivot(index="_date", columns="symbol", values="_adj_close").sort_index()


def _weight_vector(allocation: pd.DataFrame, symbols) -> dict[str, float]:
    weights = {
        str(row["symbol"]): float(row["target_weight"])
        for _, row in allocation.iterrows()
        if str(row["symbol"]) != "CASH"
    }
    return {symbol: weights.get(symbol, 0.0) for symbol in symbols}


def _portfolio_volatility(returns: pd.DataFrame, weights: dict[str, float]) -> float | None:
    if returns.empty or not weights:
        return None
    ordered = [symbol for symbol in returns.columns if symbol in weights]
    if not ordered:
        return None
    covariance = returns[ordered].cov() * 252
    vector = pd.Series({symbol: weights[symbol] for symbol in ordered})
    variance = float(vector.T @ covariance @ vector)
    if pd.isna(variance) or variance < 0:
        return None
    return sqrt(variance)


def _average_pairwise_correlation(returns: pd.DataFrame) -> float | None:
    if returns.shape[1] < 2:
        return None
    corr = returns.corr()
    values = []
    columns = list(corr.columns)
    for left_index, left in enumerate(columns):
        for right in columns[left_index + 1 :]:
            value = corr.loc[left, right]
            if pd.notna(value):
                values.append(float(value))
    if not values:
        return None
    return sum(values) / len(values)


def _backtest_summaries(backtest_dir: Path) -> list[dict[str, object]]:
    summaries = []
    if not backtest_dir.exists():
        return summaries
    for path in sorted(backtest_dir.glob("*/summary.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if "run_id" not in payload:
            payload["run_id"] = path.parent.name
        summaries.append(payload)
    return summaries


def _signals_for_date(signals: pd.DataFrame, run_date) -> pd.DataFrame:
    frame = signals.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    selected = frame.loc[frame["_date"] == pd.Timestamp(run_date)].copy()
    if selected.empty:
        raise ValueError(f"ETF signals have no rows for {run_date}")
    return selected.drop(columns=["_date"])


def _weight_optimizer_cash_only_frame(
    *,
    config: ETFConfigBundle,
    quality_report: ETFQualityReport,
    run_date,
    status: str,
    limitation: str,
) -> pd.DataFrame:
    if config.p2 is None:
        raise ValueError("ETF P2 config is required for weight optimizer")
    optimizer = config.p2.weight_optimizer
    return pd.DataFrame(
        [
            {
                "date": _date_text(run_date),
                "module": "weight_optimizer",
                "status": status,
                "symbol": "CASH",
                "candidate_weight": 1.0,
                "candidate_score": 0.0,
                "signal_component": 0.0,
                "risk_adjusted_return_component": 0.0,
                "inverse_volatility_component": 0.0,
                "annualized_return": pd.NA,
                "annualized_volatility": pd.NA,
                "history_days": 0,
                "lookback_days": optimizer.lookback_days,
                "min_history_days": optimizer.min_history_days,
                "max_candidate_weight": 1.0,
                "data_quality_status": quality_report.status,
                "config_hash": config.config_hash,
                "model_stage": "candidate_only",
                "candidate_only": optimizer.candidate_only,
                "auto_promotion": optimizer.auto_promotion,
                "limitation": limitation,
                "production_effect": "none",
            }
        ]
    )


def _min_max_component(values: pd.Series) -> dict[str, float]:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty:
        return {str(index): 0.0 for index in values.index}
    minimum = float(numeric.min())
    maximum = float(numeric.max())
    if abs(maximum - minimum) < 1e-12:
        return {str(index): 0.5 if pd.notna(value) else 0.0 for index, value in values.items()}
    return {
        str(index): (
            _clamp01((float(value) - minimum) / (maximum - minimum)) if pd.notna(value) else 0.0
        )
        for index, value in values.items()
    }


def _candidate_weights_from_scores(
    scores: dict[str, float],
    equity_budget: float,
    caps: dict[str, float],
) -> dict[str, float]:
    weights = {symbol: 0.0 for symbol in scores}
    remaining = {symbol for symbol, score in scores.items() if score > 0}
    remaining_budget = equity_budget
    while remaining and remaining_budget > 1e-12:
        total_score = sum(scores[symbol] for symbol in remaining)
        if total_score <= 0:
            break
        capped_symbols = set()
        allocations = {
            symbol: remaining_budget * scores[symbol] / total_score for symbol in remaining
        }
        for symbol, allocation in allocations.items():
            available_cap = max(0.0, caps.get(symbol, 0.0) - weights[symbol])
            if allocation >= available_cap:
                weights[symbol] += available_cap
                remaining_budget -= available_cap
                capped_symbols.add(symbol)
        if not capped_symbols:
            for symbol, allocation in allocations.items():
                weights[symbol] += allocation
            remaining_budget = 0.0
        remaining -= capped_symbols
    return weights


def _float(value: object) -> float:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return 0.0
    return float(parsed)


def _clamp01(value: float) -> float:
    if pd.isna(value):
        return 0.0
    return max(0.0, min(1.0, float(value)))


def _date_text(run_date) -> str:
    return pd.Timestamp(run_date).date().isoformat()
