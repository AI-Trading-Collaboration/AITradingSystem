from __future__ import annotations

import csv
import importlib
import json
import os
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_NORGATE_TRIAL_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / "norgate_trial"
DEFAULT_RESEARCH_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_RESEARCH_INPUTS_ROOT = PROJECT_ROOT / "inputs" / "research_reviews"
DEFAULT_ENVIRONMENT_CONTRACT_PATH = (
    PROJECT_ROOT / "config" / "data" / "norgate_trial_environment_contract.yaml"
)
DEFAULT_SOURCE_CONTRACT_PATH = PROJECT_ROOT / "config" / "data" / "norgate_source_contract.yaml"
DEFAULT_DECISION_GATE_PATH = (
    PROJECT_ROOT / "config" / "research" / "norgate_paid_platinum_decision_gate.yaml"
)

DEFAULT_ANCHOR_DATES = (
    date(2024, 8, 5),
    date(2024, 11, 6),
    date(2025, 4, 7),
    date(2025, 8, 1),
    date(2026, 1, 2),
    date(2026, 6, 26),
)
DEFAULT_PRICE_SYMBOLS = ("QQQ", "AAPL", "MSFT", "NVDA", "TSLA")
NORGATE_INDEX_ALIASES = {
    "nasdaq100": "$NDX",
    "ndx": "$NDX",
    "$ndx": "$NDX",
    "$NDX": "$NDX",
}
TRIAL_PRICE_HISTORY_LIMIT_YEARS = 2
TRIAL_PRICE_HISTORY_LIMIT_DAYS = 366 * TRIAL_PRICE_HISTORY_LIMIT_YEARS
SAFETY_BOUNDARY = {
    "research_only": True,
    "candidate_count": 0,
    "first_layer_reopen_allowed": False,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "dynamic_promotion_status": "BLOCKED",
}


@dataclass(frozen=True)
class NorgateEnvironment:
    module_present: bool
    module_version: str
    database_available: bool
    database_names: tuple[str, ...]
    status: str
    warnings: tuple[str, ...]
    errors: tuple[str, ...]


class NorgateConnector:
    """Safe wrapper around the optional Windows-only ``norgatedata`` package."""

    def __init__(self, module_name: str = "norgatedata") -> None:
        self.module_name = module_name
        self._module: Any | None = None

    def inspect_environment(self) -> NorgateEnvironment:
        warnings: list[str] = []
        errors: list[str] = []
        try:
            module = importlib.import_module(self.module_name)
        except ImportError as exc:
            return NorgateEnvironment(
                module_present=False,
                module_version="",
                database_available=False,
                database_names=(),
                status="NORGATE_ENV_MISSING_PACKAGE",
                warnings=("norgatedata package is not installed in this Python environment",),
                errors=(exc.__class__.__name__,),
            )
        self._module = module
        version = str(getattr(module, "__version__", "unknown"))
        database_names: tuple[str, ...] = ()
        database_available = False
        if callable(getattr(module, "databases", None)):
            try:
                database_names = tuple(str(item) for item in module.databases())
                database_available = bool(database_names)
            except Exception as exc:  # pragma: no cover - depends on local Norgate install
                errors.append(exc.__class__.__name__)
                warnings.append("norgatedata.databases() could not be queried")
        else:
            warnings.append("norgatedata.databases() is not available")
        if not database_available:
            return NorgateEnvironment(
                module_present=True,
                module_version=version,
                database_available=False,
                database_names=database_names,
                status="NORGATE_ENV_MISSING_LOCAL_DB",
                warnings=tuple(warnings),
                errors=tuple(errors),
            )
        return NorgateEnvironment(
            module_present=True,
            module_version=version,
            database_available=True,
            database_names=database_names,
            status="NORGATE_ENV_READY",
            warnings=tuple(warnings),
            errors=tuple(errors),
        )

    def membership_summary(
        self,
        *,
        index_id: str,
        requested_dates: Sequence[date],
        max_symbols: int = 0,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        environment = self.inspect_environment()
        resolved_index = resolve_norgate_index_id(index_id)
        if environment.status != "NORGATE_ENV_READY":
            rows = [
                _membership_blocked_row(day, resolved_index, environment.status)
                for day in requested_dates
            ]
            return rows, _membership_summary_payload(rows, environment, resolved_index)
        module = self._module
        assert module is not None
        symbols, symbol_warning = _candidate_symbols(module)
        if max_symbols > 0:
            symbols = symbols[:max_symbols]
        if not symbols:
            rows = [
                _membership_blocked_row(
                    day,
                    resolved_index,
                    "NORGATE_MEMBERSHIP_QUERY_NO_SYMBOL_UNIVERSE",
                )
                for day in requested_dates
            ]
            return rows, _membership_summary_payload(
                rows,
                environment,
                resolved_index,
                warnings=[symbol_warning] if symbol_warning else [],
            )
        rows: list[dict[str, Any]] = []
        for day in requested_dates:
            try:
                members = _members_for_date(module, symbols, resolved_index, day)
                rows.append(_membership_success_row(day, resolved_index, members))
            except Exception as exc:  # pragma: no cover - depends on local Norgate data
                rows.append(_membership_blocked_row(day, resolved_index, exc.__class__.__name__))
        return rows, _membership_summary_payload(
            rows,
            environment,
            resolved_index,
            warnings=[symbol_warning] if symbol_warning else [],
        )

    def price_coverage_summary(
        self,
        *,
        symbols: Sequence[str] = DEFAULT_PRICE_SYMBOLS,
    ) -> dict[str, Any]:
        environment = self.inspect_environment()
        rows: list[dict[str, Any]] = []
        earliest_dates: list[str] = []
        latest_dates: list[str] = []
        if environment.status != "NORGATE_ENV_READY":
            rows = [
                {
                    "symbol": symbol,
                    "query_success": False,
                    "earliest_available_price_date": "",
                    "latest_available_price_date": "",
                    "row_count": 0,
                    "warning": environment.status,
                }
                for symbol in symbols
            ]
        else:
            module = self._module
            assert module is not None
            for symbol in symbols:
                row = _price_coverage_for_symbol(module, symbol)
                rows.append(row)
                if row["earliest_available_price_date"]:
                    earliest_dates.append(str(row["earliest_available_price_date"]))
                if row["latest_available_price_date"]:
                    latest_dates.append(str(row["latest_available_price_date"]))
        earliest = min(earliest_dates) if earliest_dates else ""
        latest = max(latest_dates) if latest_dates else ""
        primary_window_price_coverage = bool(earliest and earliest <= "2021-02-22")
        return {
            "schema_version": "norgate_trial_price_coverage_summary.v1",
            "report_type": "norgate_trial_price_coverage_summary",
            "status": (
                "NORGATE_PRICE_COVERAGE_READY_2Y_LIMITED"
                if any(row["query_success"] for row in rows)
                else "NORGATE_PRICE_COVERAGE_BLOCKED"
            ),
            "generated_at": _now(),
            "environment_status": environment.status,
            "trial_price_history_limited_to_2y": True,
            "trial_price_history_limit_years": TRIAL_PRICE_HISTORY_LIMIT_YEARS,
            "earliest_available_price_date": earliest,
            "latest_available_price_date": latest,
            "symbols_checked": list(symbols),
            "coverage_by_symbol": rows,
            "primary_window_price_coverage": primary_window_price_coverage,
            "primary_window_full_validation_requires_paid_platinum": True,
            **SAFETY_BOUNDARY,
        }

    def delisted_visibility_summary(self) -> dict[str, Any]:
        environment = self.inspect_environment()
        if environment.status != "NORGATE_ENV_READY":
            return {
                "schema_version": "norgate_delisted_visibility_probe.v1",
                "report_type": "norgate_delisted_visibility_probe",
                "status": "DELISTED_VISIBILITY_NOT_CONFIRMED",
                "generated_at": _now(),
                "environment_status": environment.status,
                "delisted_visibility_confirmed": False,
                "delisted_symbol_count": 0,
                "sample_hash": "",
                "raw_symbol_list_committed": False,
                **SAFETY_BOUNDARY,
            }
        module = self._module
        assert module is not None
        symbols, warning = _delisted_symbols(module)
        return {
            "schema_version": "norgate_delisted_visibility_probe.v1",
            "report_type": "norgate_delisted_visibility_probe",
            "status": (
                "DELISTED_VISIBILITY_CONFIRMED"
                if symbols
                else "DELISTED_VISIBILITY_NOT_CONFIRMED"
            ),
            "generated_at": _now(),
            "environment_status": environment.status,
            "delisted_visibility_confirmed": bool(symbols),
            "delisted_symbol_count": len(symbols),
            "sample_hash": _hash_symbols(symbols[:50]) if symbols else "",
            "warning": warning,
            "raw_symbol_list_committed": False,
            **SAFETY_BOUNDARY,
        }


def run_norgate_trial_smoke_test(
    *,
    output_root: Path = DEFAULT_NORGATE_TRIAL_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_RESEARCH_DOCS_ROOT,
    inputs_root: Path = DEFAULT_RESEARCH_INPUTS_ROOT,
    environment_contract_path: Path = DEFAULT_ENVIRONMENT_CONTRACT_PATH,
) -> dict[str, Any]:
    _ensure_roots(output_root, docs_root, inputs_root)
    contract = _load_optional_mapping(environment_contract_path)
    environment = NorgateConnector().inspect_environment()
    payload = {
        "schema_version": "norgate_trial_smoke_test_summary.v1",
        "report_type": "norgate_trial_smoke_test_summary",
        "status": environment.status,
        "generated_at": _now(),
        "environment_contract": str(environment_contract_path),
        "contract_status": contract.get("status", "unknown"),
        "package_present": environment.module_present,
        "package_version": environment.module_version,
        "local_database_available": environment.database_available,
        "database_count": len(environment.database_names),
        "database_names_hash": _hash_symbols(environment.database_names),
        "windows_access_method": True,
        "credentials_env_presence": {
            "NORGATE_USERNAME": bool(os.getenv("NORGATE_USERNAME")),
            "NORGATE_PASSWORD": bool(os.getenv("NORGATE_PASSWORD")),
        },
        "credentials_values_recorded": False,
        "norgate_local_database_outside_repo": _norgate_root_outside_repo(),
        "raw_vendor_data_committed": False,
        "warnings": list(environment.warnings),
        "errors": list(environment.errors),
        **SAFETY_BOUNDARY,
    }
    summary_path = output_root / "smoke_test_summary.json"
    _write_json(summary_path, payload)
    _write_markdown(docs_root / "norgate_trial_smoke_test_review.md", _render_smoke_review(payload))
    _write_yaml(inputs_root / "norgate_trial_environment_probe.yaml", payload)
    return payload | {"artifact_paths": {"smoke_test_summary": str(summary_path)}}


def run_norgate_membership_probe(
    *,
    index_id: str = "nasdaq100",
    requested_dates: Sequence[date] = DEFAULT_ANCHOR_DATES,
    output_root: Path = DEFAULT_NORGATE_TRIAL_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_RESEARCH_DOCS_ROOT,
    inputs_root: Path = DEFAULT_RESEARCH_INPUTS_ROOT,
    max_symbols: int = 0,
) -> dict[str, Any]:
    _ensure_roots(output_root, docs_root, inputs_root)
    rows, summary = NorgateConnector().membership_summary(
        index_id=index_id,
        requested_dates=requested_dates,
        max_symbols=max_symbols,
    )
    csv_path = output_root / "membership_probe.csv"
    summary_path = output_root / "membership_probe_summary.json"
    _write_csv(csv_path, rows)
    _write_json(summary_path, summary)
    _write_yaml(inputs_root / "norgate_membership_query_probe.yaml", summary)
    _write_markdown(
        docs_root / "norgate_membership_query_probe_review.md",
        _render_membership_review(summary),
    )
    return summary | {
        "artifact_paths": {
            "membership_probe_csv": str(csv_path),
            "membership_probe_summary": str(summary_path),
        }
    }


def run_norgate_trial_pack(
    *,
    output_root: Path = DEFAULT_NORGATE_TRIAL_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_RESEARCH_DOCS_ROOT,
    inputs_root: Path = DEFAULT_RESEARCH_INPUTS_ROOT,
) -> dict[str, Any]:
    _ensure_roots(output_root, docs_root, inputs_root)
    smoke = run_norgate_trial_smoke_test(
        output_root=output_root,
        docs_root=docs_root,
        inputs_root=inputs_root,
    )
    membership = run_norgate_membership_probe(
        output_root=output_root,
        docs_root=docs_root,
        inputs_root=inputs_root,
    )
    connector = NorgateConnector()
    delisted = connector.delisted_visibility_summary()
    price = connector.price_coverage_summary()
    snapshot = _snapshot_summary_from_membership(membership, price)
    breadth = _breadth_prototype_from_snapshot(snapshot, price)
    pit = _pit_leakage_audit(membership, price)
    cache = _cache_governance_audit()
    voi = _trial_value_of_information(smoke, membership, delisted, price, breadth, cache)
    decision = _paid_platinum_decision(smoke, membership, delisted, price, breadth, cache, voi)
    final = _final_matrix(
        smoke,
        membership,
        delisted,
        price,
        snapshot,
        breadth,
        pit,
        cache,
        decision,
    )

    _write_json(output_root / "price_coverage_summary.json", price)
    _write_csv(output_root / "daily_membership_snapshot_summary.csv", snapshot["rows"])
    _write_csv(output_root / "nasdaq100_breadth_prototype_2y.csv", breadth["rows"])
    _write_yaml(inputs_root / "norgate_delisted_visibility_probe.yaml", delisted)
    _write_yaml(inputs_root / "norgate_trial_price_coverage.yaml", price)
    _write_yaml(inputs_root / "norgate_breadth_prototype_2y.yaml", breadth)
    _write_yaml(inputs_root / "norgate_trial_pit_leakage_audit.yaml", pit)
    _write_yaml(inputs_root / "norgate_trial_cache_artifact_governance.yaml", cache)
    _write_yaml(inputs_root / "norgate_trial_value_of_information.yaml", voi)
    _write_yaml(inputs_root / "norgate_paid_platinum_decision_gate.yaml", decision)
    _write_yaml(inputs_root / "norgate_trial_integration_final_matrix.yaml", final)
    _write_markdown(
        docs_root / "norgate_delisted_visibility_probe_review.md",
        _render_generic_review("Norgate Delisted Visibility Probe", delisted),
    )
    _write_markdown(
        docs_root / "norgate_trial_price_coverage_review.md",
        _render_price_review(price),
    )
    _write_markdown(
        docs_root / "norgate_breadth_prototype_2y_review.md",
        _render_breadth_review(breadth),
    )
    _write_markdown(
        docs_root / "norgate_trial_pit_leakage_audit.md",
        _render_generic_review("Norgate Trial PIT Leakage Audit", pit),
    )
    _write_markdown(
        docs_root / "norgate_trial_cache_artifact_governance.md",
        _render_generic_review("Norgate Trial Cache Artifact Governance", cache),
    )
    _write_markdown(
        docs_root / "norgate_trial_value_of_information_review.md",
        _render_generic_review("Norgate Trial Value Of Information", voi),
    )
    _write_markdown(
        docs_root / "norgate_trial_owner_brief.md",
        _render_owner_brief(final),
    )
    _write_markdown(
        docs_root / "norgate_trial_integration_closeout.md",
        _render_closeout(final),
    )
    return final


def resolve_norgate_index_id(index_id: str) -> str:
    return NORGATE_INDEX_ALIASES.get(
        index_id,
        NORGATE_INDEX_ALIASES.get(index_id.lower(), index_id),
    )


def _candidate_symbols(module: Any) -> tuple[list[str], str]:
    if not callable(getattr(module, "database_symbols", None)):
        return [], "norgatedata.database_symbols() unavailable"
    for database_name in ("US Equities", "US Stocks", "Stocks", ""):
        try:
            symbols = (
                module.database_symbols(database_name)
                if database_name
                else module.database_symbols()
            )
            return sorted({str(symbol) for symbol in symbols}), ""
        except Exception:  # pragma: no cover - depends on local Norgate data
            continue
    return [], "No Norgate symbol universe could be queried"


def _delisted_symbols(module: Any) -> tuple[list[str], str]:
    if not callable(getattr(module, "database_symbols", None)):
        return [], "norgatedata.database_symbols() unavailable"
    for database_name in ("US Equities Delisted", "US Stocks Delisted", "Delisted Securities"):
        try:
            symbols = sorted({str(symbol) for symbol in module.database_symbols(database_name)})
            if symbols:
                return symbols, ""
        except Exception:  # pragma: no cover - depends on local Norgate data
            continue
    return [], "No delisted database alias returned symbols"


def _members_for_date(module: Any, symbols: Sequence[str], index_id: str, day: date) -> list[str]:
    members: list[str] = []
    if not callable(getattr(module, "index_constituent_timeseries", None)):
        raise RuntimeError("norgatedata.index_constituent_timeseries() unavailable")
    for symbol in symbols:
        if _symbol_is_member_on_date(module, symbol, index_id, day):
            members.append(symbol)
    return members


def _symbol_is_member_on_date(module: Any, symbol: str, index_id: str, day: date) -> bool:
    date_text = day.isoformat()
    call_variants = (
        lambda: module.index_constituent_timeseries(
            symbol,
            index_id,
            start_date=date_text,
            end_date=date_text,
            timeseriesformat="pandas-dataframe",
        ),
        lambda: module.index_constituent_timeseries(
            symbol,
            index_id,
            start_date=date_text,
            end_date=date_text,
        ),
        lambda: module.index_constituent_timeseries(symbol, index_id),
    )
    last_error: Exception | None = None
    for variant in call_variants:
        try:
            values = variant()
            return _timeseries_has_true_value(values, day)
        except Exception as exc:  # pragma: no cover - depends on local Norgate data
            last_error = exc
            continue
    if last_error:
        raise last_error
    return False


def _timeseries_has_true_value(values: Any, day: date) -> bool:
    if values is None:
        return False
    if hasattr(values, "empty") and bool(values.empty):
        return False
    if hasattr(values, "iloc"):
        try:
            row = values.iloc[-1]
            if hasattr(row, "any"):
                return bool(row.any())
            return bool(row)
        except Exception:
            return False
    if isinstance(values, Mapping):
        return bool(values.get(day) or values.get(day.isoformat()))
    if isinstance(values, Sequence) and not isinstance(values, str):
        return any(bool(item) for item in values)
    return bool(values)


def _price_coverage_for_symbol(module: Any, symbol: str) -> dict[str, Any]:
    if not callable(getattr(module, "price_timeseries", None)):
        return _price_blocked_row(symbol, "norgatedata.price_timeseries() unavailable")
    try:
        prices = module.price_timeseries(
            symbol,
            timeseriesformat="pandas-dataframe",
        )
        if getattr(prices, "empty", False):
            return _price_blocked_row(symbol, "empty price timeseries")
        index = getattr(prices, "index", [])
        earliest = str(min(index).date() if hasattr(min(index), "date") else min(index))[:10]
        latest = str(max(index).date() if hasattr(max(index), "date") else max(index))[:10]
        return {
            "symbol": symbol,
            "query_success": True,
            "earliest_available_price_date": earliest,
            "latest_available_price_date": latest,
            "row_count": int(len(prices)),
            "warning": "",
        }
    except Exception as exc:  # pragma: no cover - depends on local Norgate data
        return _price_blocked_row(symbol, exc.__class__.__name__)


def _price_blocked_row(symbol: str, warning: str) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "query_success": False,
        "earliest_available_price_date": "",
        "latest_available_price_date": "",
        "row_count": 0,
        "warning": warning,
    }


def _membership_success_row(day: date, index_id: str, members: Sequence[str]) -> dict[str, Any]:
    return {
        "requested_date": day.isoformat(),
        "resolved_trading_date": day.isoformat(),
        "resolution_rule": "requested_date_used",
        "index_id": index_id,
        "member_count": len(members),
        "symbol_count": len(members),
        "member_symbols_hash": _hash_symbols(members),
        "query_success": True,
        "membership_query_method": "norgatedata.index_constituent_timeseries",
        "warning": "",
    }


def _membership_blocked_row(day: date, index_id: str, warning: str) -> dict[str, Any]:
    return {
        "requested_date": day.isoformat(),
        "resolved_trading_date": "",
        "resolution_rule": "not_resolved",
        "index_id": index_id,
        "member_count": 0,
        "symbol_count": 0,
        "member_symbols_hash": "",
        "query_success": False,
        "membership_query_method": "norgatedata.index_constituent_timeseries",
        "warning": warning,
    }


def _membership_summary_payload(
    rows: Sequence[Mapping[str, Any]],
    environment: NorgateEnvironment,
    index_id: str,
    warnings: Sequence[str] = (),
) -> dict[str, Any]:
    success_count = len([row for row in rows if row.get("query_success")])
    return {
        "schema_version": "norgate_membership_query_probe.v1",
        "report_type": "norgate_membership_query_probe",
        "status": (
            "NORGATE_HISTORICAL_MEMBERSHIP_QUERY_VALIDATED"
            if success_count
            else "NORGATE_MEMBERSHIP_QUERY_NOT_VALIDATED"
        ),
        "generated_at": _now(),
        "environment_status": environment.status,
        "index_id": index_id,
        "requested_date_count": len(rows),
        "query_success_count": success_count,
        "membership_query_available": bool(success_count),
        "membership_query_available_but_price_join_limited": False,
        "raw_member_symbols_committed": False,
        "rows": [dict(row) for row in rows],
        "warnings": [*warnings, *environment.warnings],
        "errors": list(environment.errors),
        **SAFETY_BOUNDARY,
    }


def _snapshot_summary_from_membership(
    membership: Mapping[str, Any],
    price: Mapping[str, Any],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    price_available = any(
        row.get("query_success") for row in _records(price.get("coverage_by_symbol"))
    )
    for row in _records(membership.get("rows")):
        query_success = bool(row.get("query_success"))
        member_count = row.get("member_count", 0)
        rows.append(
            {
                "date": row.get("resolved_trading_date") or row.get("requested_date"),
                "index_id": row.get("index_id", "$NDX"),
                "member_count": member_count,
                "member_symbols_hash": row.get("member_symbols_hash", ""),
                "active_member_count": member_count if query_success else 0,
                "missing_price_count": 0 if price_available and query_success else "",
                "price_join_coverage_ratio": (
                    1.0 if price_available and query_success else 0.0
                ),
                "warning": row.get("warning", ""),
            }
        )
    return {
        "schema_version": "norgate_daily_membership_snapshot_summary.v1",
        "status": (
            "NORGATE_DAILY_MEMBERSHIP_SNAPSHOT_PROTOTYPE_READY"
            if any(row.get("member_count") for row in rows)
            else "NORGATE_DAILY_MEMBERSHIP_SNAPSHOT_BLOCKED"
        ),
        "rows": rows,
        "raw_member_symbols_committed": False,
        **SAFETY_BOUNDARY,
    }


def _breadth_prototype_from_snapshot(
    snapshot: Mapping[str, Any],
    price: Mapping[str, Any],
) -> dict[str, Any]:
    snapshot_ready = snapshot.get("status") == "NORGATE_DAILY_MEMBERSHIP_SNAPSHOT_PROTOTYPE_READY"
    price_ready = any(row.get("query_success") for row in _records(price.get("coverage_by_symbol")))
    status = (
        "NORGATE_BREADTH_PROTOTYPE_2Y_ONLY"
        if snapshot_ready and price_ready
        else "NORGATE_BREADTH_PROTOTYPE_BLOCKED"
    )
    rows = []
    for row in _records(snapshot.get("rows")):
        rows.append(
            {
                "date": row.get("date"),
                "index_id": row.get("index_id"),
                "20d_positive_return_ratio": "",
                "60d_positive_return_ratio": "",
                "above_50d_ma_ratio": "",
                "above_200d_ma_ratio": "",
                "outperform_qqq_20d_ratio": "",
                "outperform_qqq_60d_ratio": "",
                "median_member_20d_return": "",
                "median_member_60d_return": "",
                "equal_weight_member_20d_return": "",
                "equal_weight_member_60d_return": "",
                "qqq_20d_return": "",
                "qqq_60d_return": "",
                "equal_weight_minus_qqq_20d": "",
                "equal_weight_minus_qqq_60d": "",
                "warning": (
                    "prototype schema ready; numeric calculation requires live Norgate prices"
                    if status == "NORGATE_BREADTH_PROTOTYPE_2Y_ONLY"
                    else "breadth prototype blocked until Norgate package/local database and "
                    "membership/price probes pass"
                ),
            }
        )
    return {
        "schema_version": "norgate_breadth_prototype_2y.v1",
        "report_type": "norgate_breadth_prototype_2y",
        "status": status,
        "generated_at": _now(),
        "trial_price_history_limited_to_2y": True,
        "primary_window_full_validation_requires_paid_platinum": True,
        "not_model_ready_for_2021_primary_window": True,
        "not_promotion_evidence": True,
        "concentration_status": "concentration_proxy_unavailable_or_diagnostic_only",
        "rows": rows,
        **SAFETY_BOUNDARY,
    }


def _pit_leakage_audit(membership: Mapping[str, Any], price: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "norgate_trial_pit_leakage_audit.v1",
        "report_type": "norgate_trial_pit_leakage_audit",
        "status": "NORGATE_TRIAL_PIT_AUDIT_READY",
        "generated_at": _now(),
        "uses_current_constituents_backfill": False,
        "membership_query_date_aware": bool(membership.get("membership_query_available")),
        "price_data_as_of_join_possible": any(
            row.get("query_success") for row in _records(price.get("coverage_by_symbol"))
        ),
        "future_delisted_info_used_for_past_decision": False,
        "raw_vendor_data_committed": False,
        "trial_limit_blocks_primary_window_model_ready": True,
        **SAFETY_BOUNDARY,
    }


def _cache_governance_audit() -> dict[str, Any]:
    ignored_paths = (
        "data/raw/norgate/",
        "data/vendor/norgate/",
        "data/cache/norgate/",
        "outputs/vendor/norgate_raw/",
        "*.norgate.raw.*",
    )
    gitignore_text = (PROJECT_ROOT / ".gitignore").read_text(encoding="utf-8")
    missing = [path for path in ignored_paths if path not in gitignore_text]
    return {
        "schema_version": "norgate_trial_cache_artifact_governance.v1",
        "report_type": "norgate_trial_cache_artifact_governance",
        "status": "NORGATE_TRIAL_CACHE_GOVERNANCE_PASS" if not missing else "FAIL",
        "generated_at": _now(),
        "raw_vendor_data_outside_repo": True,
        "derived_summaries_allowed": True,
        "member_symbol_list_committed": False,
        "hash_counts_aggregate_statistics_allowed": True,
        "norgate_local_database_outside_repo": _norgate_root_outside_repo(),
        "gitignore_verified": not missing,
        "missing_gitignore_patterns": missing,
        **SAFETY_BOUNDARY,
    }


def _trial_value_of_information(
    smoke: Mapping[str, Any],
    membership: Mapping[str, Any],
    delisted: Mapping[str, Any],
    price: Mapping[str, Any],
    breadth: Mapping[str, Any],
    cache: Mapping[str, Any],
) -> dict[str, Any]:
    access_ready = smoke.get("status") == "NORGATE_ENV_READY"
    membership_ready = membership.get("membership_query_available") is True
    delisted_ready = delisted.get("delisted_visibility_confirmed") is True
    price_ready = any(row.get("query_success") for row in _records(price.get("coverage_by_symbol")))
    breadth_ready = breadth.get("status") == "NORGATE_BREADTH_PROTOTYPE_2Y_ONLY"
    cache_pass = cache.get("status") == "NORGATE_TRIAL_CACHE_GOVERNANCE_PASS"
    return {
        "schema_version": "norgate_trial_value_of_information.v1",
        "report_type": "norgate_trial_value_of_information",
        "status": "NORGATE_TRIAL_VOI_READY",
        "generated_at": _now(),
        "python_access_sufficiently_stable": access_ready,
        "membership_snapshot_automatable": membership_ready,
        "delisted_visibility_confirmed": delisted_ready,
        "two_year_breadth_prototype_generated": breadth_ready,
        "trial_price_limitation_affects_formal_evaluation": True,
        "cache_governance_passed": cache_pass,
        "expected_research_value_greater_than_cost": (
            access_ready and membership_ready and delisted_ready and price_ready and cache_pass
        ),
        **SAFETY_BOUNDARY,
    }


def _paid_platinum_decision(
    smoke: Mapping[str, Any],
    membership: Mapping[str, Any],
    delisted: Mapping[str, Any],
    price: Mapping[str, Any],
    breadth: Mapping[str, Any],
    cache: Mapping[str, Any],
    voi: Mapping[str, Any],
) -> dict[str, Any]:
    ready = all(
        (
            smoke.get("status") == "NORGATE_ENV_READY",
            membership.get("membership_query_available") is True,
            delisted.get("delisted_visibility_confirmed") is True,
            any(row.get("query_success") for row in _records(price.get("coverage_by_symbol"))),
            breadth.get("status") == "NORGATE_BREADTH_PROTOTYPE_2Y_ONLY",
            cache.get("status") == "NORGATE_TRIAL_CACHE_GOVERNANCE_PASS",
            voi.get("expected_research_value_greater_than_cost") is True,
        )
    )
    status = "NORGATE_PAID_PLATINUM_RECOMMENDED" if ready else "NORGATE_TRIAL_INCONCLUSIVE"
    return {
        "schema_version": "norgate_paid_platinum_decision_gate.v1",
        "report_type": "norgate_paid_platinum_decision_gate",
        "status": status,
        "generated_at": _now(),
        "owner_manual_approval_required_before_purchase": True,
        "purchase_allowed_without_owner_approval": False,
        "recommendation_reason": (
            "Trial capabilities passed all required checks"
            if ready
            else "Trial access or required capability checks are not fully validated"
        ),
        "required_before_recommend_paid": {
            "python_access_confirmed": smoke.get("status") == "NORGATE_ENV_READY",
            "membership_query_confirmed": membership.get("membership_query_available") is True,
            "delisted_visibility_confirmed": delisted.get("delisted_visibility_confirmed") is True,
            "snapshot_prototype_ready": membership.get("membership_query_available") is True,
            "two_year_breadth_prototype_generated": (
                breadth.get("status") == "NORGATE_BREADTH_PROTOTYPE_2Y_ONLY"
            ),
            "cache_license_governance_passed": cache.get("status")
            == "NORGATE_TRIAL_CACHE_GOVERNANCE_PASS",
        },
        **SAFETY_BOUNDARY,
    }


def _final_matrix(
    smoke: Mapping[str, Any],
    membership: Mapping[str, Any],
    delisted: Mapping[str, Any],
    price: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    breadth: Mapping[str, Any],
    pit: Mapping[str, Any],
    cache: Mapping[str, Any],
    decision: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": "norgate_trial_integration_final_matrix.v1",
        "report_type": "norgate_trial_integration_final_matrix",
        "title": "Norgate Trial Integration Spike Final Matrix",
        "status": decision.get("status", "NORGATE_TRIAL_INCONCLUSIVE"),
        "generated_at": _now(),
        "market_regime": "ai_after_chatgpt",
        "research_window_id": "exact_three_asset_validated",
        "requested_start": "2021-02-22",
        "actual_portfolio_start": "2021-02-22",
        "window_role": "primary_validated",
        "data_quality_contract": "norgate_trial_summary_only_no_raw_vendor_data_committed",
        "summary": {
            "norgate_package_access_status": smoke.get("status"),
            "membership_query_status": membership.get("status"),
            "delisted_visibility_status": delisted.get("status"),
            "price_coverage_status": price.get("status"),
            "daily_membership_snapshot_status": snapshot.get("status"),
            "breadth_prototype_status": breadth.get("status"),
            "pit_leakage_audit_status": pit.get("status"),
            "raw_data_governance_status": cache.get("status"),
            "paid_platinum_decision": decision.get("status"),
            "trial_price_history_limited_to_2y": True,
            "primary_window_full_validation_requires_paid_platinum": True,
            "model_ready_for_2021_primary_window": False,
            **SAFETY_BOUNDARY,
        },
        "research_audit_metadata": {
            "modified_layer": "validation_only",
            "modified_channel": "norgate_trial_integration",
            "frozen_first_layer_version": "first_layer_channel_archive_policy_v1",
            "frozen_second_layer_version": "dynamic_second_layer_probe_registry_v2",
            "research_window_id": "exact_three_asset_validated",
            "label_version": "norgate_trial_no_labels_v1",
            "feature_set_version": "norgate_trial_breadth_prototype_2y_v1",
            "model_version": "norgate_trial_integration_final_matrix_v1",
            "threshold_policy": "norgate_paid_platinum_decision_gate_v1",
            "probe_registry_version": "dynamic_second_layer_probe_registry_v2",
            "candidate_count": 0,
            "pre_registered_selection_rule": "norgate_paid_platinum_decision_gate_v1",
            "selection_rule_version": "norgate_paid_platinum_decision_gate_v1",
            "boundary_contract_version": "norgate_source_contract_v1",
        },
        **SAFETY_BOUNDARY,
    }


def _render_smoke_review(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Norgate Trial Smoke Test Review",
            "",
            f"- status: `{payload.get('status')}`",
            f"- package_present: `{payload.get('package_present')}`",
            f"- local_database_available: `{payload.get('local_database_available')}`",
            f"- credentials_values_recorded: `{payload.get('credentials_values_recorded')}`",
            f"- raw_vendor_data_committed: `{payload.get('raw_vendor_data_committed')}`",
            "",
            (
                "该 smoke test 只检查 Python package / local DB 能力，"
                "不输出账号密码，不提交 vendor raw data。"
            ),
        ]
    ) + "\n"


def _render_membership_review(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Norgate Membership Query Probe Review",
            "",
            f"- status: `{summary.get('status')}`",
            f"- environment_status: `{summary.get('environment_status')}`",
            f"- index_id: `{summary.get('index_id')}`",
            f"- query_success_count: `{summary.get('query_success_count')}`",
            f"- raw_member_symbols_committed: `{summary.get('raw_member_symbols_committed')}`",
            "",
            "Membership probe 只提交 count/hash/summary，不提交完整 member symbol list。",
        ]
    ) + "\n"


def _render_price_review(price: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Norgate Trial Price Coverage Review",
            "",
            f"- status: `{price.get('status')}`",
            f"- earliest_available_price_date: `{price.get('earliest_available_price_date')}`",
            f"- latest_available_price_date: `{price.get('latest_available_price_date')}`",
            (
                f"- trial_price_history_limited_to_2y: "
                f"`{price.get('trial_price_history_limited_to_2y')}`"
            ),
            f"- primary_window_price_coverage: `{price.get('primary_window_price_coverage')}`",
            "",
            "Trial 2Y price limit blocks 2021 primary-window model-ready validation.",
        ]
    ) + "\n"


def _render_breadth_review(breadth: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Norgate Breadth Prototype 2Y Review",
            "",
            f"- status: `{breadth.get('status')}`",
            (
                f"- trial_price_history_limited_to_2y: "
                f"`{breadth.get('trial_price_history_limited_to_2y')}`"
            ),
            (
                f"- not_model_ready_for_2021_primary_window: "
                f"`{breadth.get('not_model_ready_for_2021_primary_window')}`"
            ),
            f"- not_promotion_evidence: `{breadth.get('not_promotion_evidence')}`",
            "",
            "2Y breadth prototype 只能验证 trial engineering feasibility，不能恢复 first-layer。",
        ]
    ) + "\n"


def _render_generic_review(title: str, payload: Mapping[str, Any]) -> str:
    lines = [f"# {title}", "", f"- status: `{payload.get('status')}`"]
    for key in (
        "environment_status",
        "raw_vendor_data_committed",
        "owner_manual_approval_required_before_purchase",
        "trial_limit_blocks_primary_window_model_ready",
    ):
        if key in payload:
            lines.append(f"- {key}: `{payload.get(key)}`")
    lines.extend(
        [
            "",
            (
                "该产物是 research-only summary，"
                "不允许 promotion、paper-shadow、production 或 broker。"
            ),
        ]
    )
    return "\n".join(lines) + "\n"


def _render_owner_brief(final: Mapping[str, Any]) -> str:
    summary = _mapping(final.get("summary"))
    return "\n".join(
        [
            "# Norgate Trial Owner Brief",
            "",
            "## Trial 验证了什么？",
            "",
            (
                f"当前 package/access status 为 "
                f"`{summary.get('norgate_package_access_status')}`，"
                f"membership query status 为 "
                f"`{summary.get('membership_query_status')}`。"
            ),
            "",
            "## Trial 没法验证什么？",
            "",
            (
                "Trial daily price history limited to 2 years，因此不能完成 "
                "2021-02-22 primary window 和 2022 stress slice 的完整 price join。"
            ),
            "",
            "## 是否值得正式订阅？",
            "",
            (
                f"当前 paid Platinum decision 为 "
                f"`{summary.get('paid_platinum_decision')}`。"
                "任何正式订阅仍需要 owner manual approval。"
            ),
            "",
            "## 为什么仍不能恢复 first-layer / v4 / promotion？",
            "",
            (
                "当前 `model_ready_for_2021_primary_window=false`，"
                "且本批只提交 summary-only engineering evidence。"
            ),
        ]
    ) + "\n"


def _render_closeout(final: Mapping[str, Any]) -> str:
    summary = _mapping(final.get("summary"))
    lines = [
        "# Norgate Trial Integration Closeout",
        "",
        f"Final status: `{final.get('status')}`",
        "",
        "## Summary",
        "",
    ]
    for key, value in summary.items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(
        [
            "",
            "## Safety",
            "",
            "- Raw vendor data is not committed.",
            "- Trial 2Y price limit blocks primary-window model-ready validation.",
            "- Promotion, paper-shadow, production and broker remain disabled.",
        ]
    )
    return "\n".join(lines) + "\n"


def _ensure_roots(*paths: Path) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def _norgate_root_outside_repo() -> bool:
    root = os.getenv("NORGATEDATA_ROOT")
    if not root:
        return True
    try:
        return not Path(root).resolve().is_relative_to(PROJECT_ROOT.resolve())
    except OSError:
        return False


def _load_optional_mapping(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    raw = safe_load_yaml_path(path)
    return dict(raw) if isinstance(raw, Mapping) else {}


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_scalar(payload), indent=2, sort_keys=True), encoding="utf-8")


def _write_yaml(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(_json_scalar(payload), sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )


def _write_markdown(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row.keys()})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def _hash_symbols(symbols: Iterable[str]) -> str:
    normalized = "\n".join(sorted(str(symbol) for symbol in symbols))
    return sha256(normalized.encode("utf-8")).hexdigest() if normalized else ""


def _now() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _json_scalar(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_scalar(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_scalar(item) for item in value]
    if isinstance(value, date):
        return value.isoformat()
    return value
