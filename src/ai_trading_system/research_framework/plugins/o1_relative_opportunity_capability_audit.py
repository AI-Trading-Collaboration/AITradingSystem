from __future__ import annotations

import hashlib
import math
import re
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from ai_trading_system.platform.artifacts import (
    canonical_json_bytes,
    sha256_path,
)
from ai_trading_system.research_framework.plugins.decision_target_capability_audit_model_ladder import (  # noqa: E501
    _build_feature_frame,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

SCHEMA_VERSION = "o1_relative_opportunity_synthetic_dataset.v1"
VALIDATION_SCHEMA_VERSION = "o1_relative_opportunity_synthetic_validation.v1"
READY_STATUS = "SYNTHETIC_BUILDER_VALIDATOR_PASS"
DATA_ROLE = "SYNTHETIC_FIXTURE_ONLY"

_AUDIT_POLICY_SCHEMA = "o1_relative_opportunity_capability_audit_policy.v1"
_HISTORICAL_MODEL_POLICY_SCHEMA = "decision_target_capability_audit_model_ladder_policy.v1"
_POLICY_ID = "TRADING_2464_O1_CAPABILITY_AUDIT_V1"
_OWNER_DECISION = (
    "owner_decision:TRADING-2464:2026-07-30:"
    "approve_o1_m1_ridge_cross_asset_state_single_family_v1"
)
_MODEL_ID = "M1_RIDGE_LINEAR"
_FEATURE_FAMILY = "CROSS_ASSET_STATE"
_PRICE_COLUMNS = ("date", "ticker", "adj_close")
_TICKERS = ("QQQ", "SGOV", "SPY")
_HEX_40 = re.compile(r"^[0-9a-f]{40}$")
_HEX_64 = re.compile(r"^[0-9a-f]{64}$")
_FLOAT_TOLERANCE = 1.0e-12


class O1SyntheticValidationError(ValueError):
    """Typed fail-closed error for synthetic-only builder inputs."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def build_synthetic_capability_dataset(
    *,
    audit_policy_path: Path,
    historical_model_policy_path: Path,
    price_panel: pd.DataFrame,
    event_ledger: Sequence[Mapping[str, Any]],
    source_commit_sha: str,
    data_role: str,
) -> dict[str, Any]:
    """Build a deterministic synthetic-only dataset without coverage or model execution."""

    audit_policy = _mapping(safe_load_yaml_path(audit_policy_path))
    historical_policy = _mapping(safe_load_yaml_path(historical_model_policy_path))
    _validate_policy_contract(
        audit_policy=audit_policy,
        historical_policy=historical_policy,
        historical_policy_sha256=sha256_path(historical_model_policy_path),
        source_commit_sha=source_commit_sha,
        data_role=data_role,
    )
    panel = _normalize_price_panel(price_panel)
    events = _normalize_event_ledger(event_ledger, audit_policy=audit_policy)
    feature_frame, feature_ids_by_family = _build_feature_frame(
        historical_policy,
        panel,
    )
    feature_ids = _selected_feature_ids(
        audit_policy=audit_policy,
        historical_policy=historical_policy,
        feature_ids_by_family=feature_ids_by_family,
    )
    rows = _build_dataset_rows(
        audit_policy=audit_policy,
        panel=panel,
        feature_frame=feature_frame,
        feature_ids=feature_ids,
    )
    if not rows:
        raise O1SyntheticValidationError("SYNTHETIC_DATASET_HAS_NO_ELIGIBLE_ROWS")
    folds = _build_fold_ledger(audit_policy=audit_policy, rows=rows)
    if not folds:
        raise O1SyntheticValidationError("SYNTHETIC_DATASET_HAS_NO_OUTER_FOLD")
    event_episodes = _build_event_episodes(
        audit_policy=audit_policy,
        events=events,
        common_sessions=_common_sessions(panel),
    )
    independent_frame = _independent_feature_frame(historical_policy, panel)
    reconstruction_errors = _row_reconstruction_errors(
        audit_policy=audit_policy,
        panel=panel,
        feature_frame=independent_frame,
        feature_ids=feature_ids,
        rows=rows,
    )
    if reconstruction_errors:
        raise O1SyntheticValidationError(reconstruction_errors[0])

    input_commitment = {
        "price_panel_sha256": _sha256_json(panel.to_dict(orient="records")),
        "event_ledger_sha256": _sha256_json(events),
        "price_row_count": len(panel),
        "common_session_count": len(_common_sessions(panel)),
        "event_row_count": len(events),
    }
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "validation_schema_version": VALIDATION_SCHEMA_VERSION,
        "status": READY_STATUS,
        "task_id": str(audit_policy["task_id"]),
        "data_role": DATA_ROLE,
        "authority": {
            "policy_id": str(audit_policy["policy_id"]),
            "owner_decision": str(audit_policy["owner_decision"]),
            "audit_policy_path": audit_policy_path.as_posix(),
            "audit_policy_sha256": sha256_path(audit_policy_path),
            "historical_model_policy_path": historical_model_policy_path.as_posix(),
            "historical_model_policy_sha256": sha256_path(
                historical_model_policy_path
            ),
            "source_commit_sha": source_commit_sha,
        },
        "input_commitment": input_commitment,
        "dataset_contract": {
            "target_id": "RELATIVE_OPPORTUNITY_SPREAD",
            "primary_horizon_common_sessions": _primary_horizon(audit_policy),
            "model_id": _MODEL_ID,
            "feature_family": _FEATURE_FAMILY,
            "feature_ids": feature_ids,
            "feature_count": len(feature_ids),
            "model_training_executed": False,
            "real_coverage_evaluated": False,
        },
        "requested_fixture_range": {
            "start": min(_common_sessions(panel)),
            "end": max(_common_sessions(panel)),
        },
        "evaluated_fixture_range": {
            "start": str(rows[0]["decision_date"]),
            "end": str(rows[-1]["decision_date"]),
        },
        "rows": rows,
        "fold_ledger": folds,
        "event_episodes": event_episodes,
        "dataset_commitment_sha256": _dataset_commitment(
            rows=rows,
            folds=folds,
            event_episodes=event_episodes,
        ),
        "safety": _safety_payload(),
    }
    return payload


def validate_synthetic_capability_dataset(
    payload: Mapping[str, Any],
    *,
    audit_policy_path: Path,
    historical_model_policy_path: Path,
    price_panel: pd.DataFrame,
    event_ledger: Sequence[Mapping[str, Any]],
    source_commit_sha: str,
    data_role: str,
) -> tuple[str, ...]:
    """Rebuild the payload and run a separate formula-level reconstruction."""

    try:
        expected = build_synthetic_capability_dataset(
            audit_policy_path=audit_policy_path,
            historical_model_policy_path=historical_model_policy_path,
            price_panel=price_panel,
            event_ledger=event_ledger,
            source_commit_sha=source_commit_sha,
            data_role=data_role,
        )
    except O1SyntheticValidationError as exc:
        return (f"SOURCE_REBUILD_FAILED:{exc.code}",)

    errors: list[str] = []
    comparisons = (
        ("schema_version", "SCHEMA_VERSION_MISMATCH"),
        ("validation_schema_version", "VALIDATION_SCHEMA_VERSION_MISMATCH"),
        ("status", "STATUS_MISMATCH"),
        ("data_role", "DATA_ROLE_MISMATCH"),
        ("authority", "AUTHORITY_MISMATCH"),
        ("input_commitment", "INPUT_COMMITMENT_MISMATCH"),
        ("dataset_contract", "DATASET_CONTRACT_MISMATCH"),
        ("requested_fixture_range", "REQUESTED_FIXTURE_RANGE_MISMATCH"),
        ("evaluated_fixture_range", "EVALUATED_FIXTURE_RANGE_MISMATCH"),
        ("rows", "DATASET_ROWS_MISMATCH"),
        ("fold_ledger", "FOLD_LEDGER_MISMATCH"),
        ("event_episodes", "EVENT_EPISODES_MISMATCH"),
        ("dataset_commitment_sha256", "DATASET_COMMITMENT_MISMATCH"),
        ("safety", "SAFETY_BOUNDARY_MISMATCH"),
    )
    for field, code in comparisons:
        if payload.get(field) != expected.get(field):
            errors.append(code)

    audit_policy = _mapping(safe_load_yaml_path(audit_policy_path))
    historical_policy = _mapping(safe_load_yaml_path(historical_model_policy_path))
    panel = _normalize_price_panel(price_panel)
    feature_frame = _independent_feature_frame(historical_policy, panel)
    feature_ids = _string_list(
        _mapping(expected.get("dataset_contract")).get("feature_ids")
    )
    rows = payload.get("rows")
    if isinstance(rows, Sequence) and not isinstance(rows, (str, bytes)):
        errors.extend(
            _row_reconstruction_errors(
                audit_policy=audit_policy,
                panel=panel,
                feature_frame=feature_frame,
                feature_ids=feature_ids,
                rows=[_mapping(row) for row in rows],
            )
        )
    else:
        errors.append("INDEPENDENT_ROW_STRUCTURE_INVALID")
    return tuple(dict.fromkeys(errors))


def _validate_policy_contract(
    *,
    audit_policy: Mapping[str, Any],
    historical_policy: Mapping[str, Any],
    historical_policy_sha256: str,
    source_commit_sha: str,
    data_role: str,
) -> None:
    if data_role != DATA_ROLE:
        raise O1SyntheticValidationError("REAL_DATA_ROLE_FORBIDDEN_IN_SYNTHETIC_STAGE")
    if not _HEX_40.fullmatch(source_commit_sha):
        raise O1SyntheticValidationError("SOURCE_COMMIT_SHA_INVALID")
    if audit_policy.get("schema_version") != _AUDIT_POLICY_SCHEMA:
        raise O1SyntheticValidationError("AUDIT_POLICY_SCHEMA_INVALID")
    if audit_policy.get("policy_id") != _POLICY_ID:
        raise O1SyntheticValidationError("AUDIT_POLICY_ID_INVALID")
    if audit_policy.get("owner_decision") != _OWNER_DECISION:
        raise O1SyntheticValidationError("OWNER_DECISION_INVALID")
    if historical_policy.get("schema_version") != _HISTORICAL_MODEL_POLICY_SCHEMA:
        raise O1SyntheticValidationError("HISTORICAL_MODEL_POLICY_SCHEMA_INVALID")
    authority = _mapping(audit_policy.get("authority"))
    historical_authority = _mapping(authority.get("historical_model_policy"))
    if historical_authority.get("sha256") != historical_policy_sha256:
        raise O1SyntheticValidationError("HISTORICAL_MODEL_POLICY_SHA256_MISMATCH")
    execution = _mapping(audit_policy.get("execution_binding"))
    if execution.get("real_coverage_read_allowed_now") is not False:
        raise O1SyntheticValidationError("REAL_COVERAGE_GATE_NOT_CLOSED")
    if execution.get("model_training_allowed_now") is not False:
        raise O1SyntheticValidationError("MODEL_TRAINING_GATE_NOT_CLOSED")
    synthetic_contract = _mapping(audit_policy.get("synthetic_validation_contract"))
    if synthetic_contract != {
        "schema_version": VALIDATION_SCHEMA_VERSION,
        "allowed_data_role": DATA_ROLE,
        "deterministic_double_build_required": True,
        "independent_formula_reconstruction_required": True,
        "negative_input_validation_required": True,
        "real_data_access_allowed": False,
        "coverage_classification_allowed": False,
        "model_training_allowed": False,
    }:
        raise O1SyntheticValidationError("SYNTHETIC_VALIDATION_CONTRACT_INVALID")
    target = _mapping(audit_policy.get("target_contract"))
    if (
        target.get("target_id") != "RELATIVE_OPPORTUNITY_SPREAD"
        or target.get("label")
        != "QQQ_FORWARD_TOTAL_RETURN - SGOV_FORWARD_TOTAL_RETURN"
        or target.get("primary_horizon_common_sessions") != 5
    ):
        raise O1SyntheticValidationError("TARGET_CONTRACT_INVALID")
    model_contract = _mapping(audit_policy.get("model_feature_contract"))
    if (
        model_contract.get("model_id") != _MODEL_ID
        or model_contract.get("family_prefix") != _FEATURE_FAMILY
        or model_contract.get("interaction_terms_allowed") is not False
    ):
        raise O1SyntheticValidationError("MODEL_FEATURE_CONTRACT_INVALID")
    model_rows = _mapping_rows(_mapping(historical_policy.get("model_policy")).get("models"))
    reviewed_model = next(
        (row for row in model_rows if row.get("model_id") == _MODEL_ID),
        None,
    )
    if reviewed_model is None:
        raise O1SyntheticValidationError("REVIEWED_MODEL_NOT_FOUND")
    if float(reviewed_model.get("ridge_penalty") or 0.0) != float(
        model_contract.get("ridge_penalty") or 0.0
    ):
        raise O1SyntheticValidationError("REVIEWED_MODEL_PENALTY_MISMATCH")


def _normalize_price_panel(panel: pd.DataFrame) -> pd.DataFrame:
    if tuple(str(column) for column in panel.columns) != _PRICE_COLUMNS:
        raise O1SyntheticValidationError("SYNTHETIC_PRICE_COLUMNS_INVALID")
    frame = panel.copy()
    if frame.empty:
        raise O1SyntheticValidationError("SYNTHETIC_PRICE_PANEL_EMPTY")
    dates = frame["date"].astype(str)
    parsed_dates = pd.to_datetime(dates, format="%Y-%m-%d", errors="coerce")
    if parsed_dates.isna().any() or not (
        parsed_dates.dt.strftime("%Y-%m-%d") == dates
    ).all():
        raise O1SyntheticValidationError("SYNTHETIC_PRICE_DATE_INVALID")
    frame["date"] = dates
    frame["ticker"] = frame["ticker"].astype(str)
    if set(frame["ticker"].unique()) != set(_TICKERS):
        raise O1SyntheticValidationError("SYNTHETIC_PRICE_TICKERS_INVALID")
    if frame.duplicated(["date", "ticker"]).any():
        raise O1SyntheticValidationError("SYNTHETIC_PRICE_DUPLICATE_KEY")
    canonical_order = frame.sort_values(["date", "ticker"], kind="stable").reset_index(
        drop=True
    )
    if not frame.reset_index(drop=True).equals(canonical_order):
        raise O1SyntheticValidationError("SYNTHETIC_PRICE_ROW_ORDER_INVALID")
    numeric = pd.to_numeric(frame["adj_close"], errors="coerce")
    if numeric.isna().any() or not np.isfinite(numeric.to_numpy(dtype=float)).all():
        raise O1SyntheticValidationError("SYNTHETIC_PRICE_NONFINITE")
    if not (numeric > 0.0).all():
        raise O1SyntheticValidationError("SYNTHETIC_PRICE_NONPOSITIVE")
    frame["adj_close"] = numeric.astype(float)
    counts = frame.groupby("date")["ticker"].nunique()
    if counts.empty or not (counts == len(_TICKERS)).all():
        raise O1SyntheticValidationError("SYNTHETIC_PRICE_COMMON_SESSION_INVALID")
    return frame


def _normalize_event_ledger(
    event_ledger: Sequence[Mapping[str, Any]],
    *,
    audit_policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    event_contract = _mapping(audit_policy.get("event_contract"))
    fields = _string_list(event_contract.get("exact_event_ledger_fields"))
    families = _string_list(event_contract.get("mandatory_event_families"))
    rows: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for raw_row in event_ledger:
        row = {str(key): value for key, value in raw_row.items()}
        if set(row) != set(fields):
            raise O1SyntheticValidationError("SYNTHETIC_EVENT_FIELDS_INVALID")
        event_id = str(row["event_id"])
        if not event_id or event_id in seen_ids:
            raise O1SyntheticValidationError("SYNTHETIC_EVENT_ID_INVALID")
        seen_ids.add(event_id)
        if str(row["event_family"]) not in families:
            raise O1SyntheticValidationError("SYNTHETIC_EVENT_FAMILY_INVALID")
        if not isinstance(row["request_parameters"], Mapping):
            raise O1SyntheticValidationError("SYNTHETIC_EVENT_REQUEST_PARAMETERS_INVALID")
        checksum = str(row["checksum"])
        if not _HEX_64.fullmatch(checksum):
            raise O1SyntheticValidationError("SYNTHETIC_EVENT_CHECKSUM_INVALID")
        timestamps = [
            _parse_aware_timestamp(str(row[field]))
            for field in (
                "event_timestamp",
                "source_published_time",
                "known_at",
                "available_at",
                "download_timestamp",
            )
        ]
        if not (
            timestamps[1] <= timestamps[2] <= timestamps[3] <= timestamps[4]
        ):
            raise O1SyntheticValidationError("SYNTHETIC_EVENT_VISIBILITY_ORDER_INVALID")
        for field in ("provider_name", "endpoint_or_file"):
            if not str(row[field]).strip():
                raise O1SyntheticValidationError("SYNTHETIC_EVENT_SOURCE_IDENTITY_INVALID")
        row["request_parameters"] = {
            str(key): value
            for key, value in sorted(
                _mapping(row["request_parameters"]).items(),
                key=lambda item: str(item[0]),
            )
        }
        rows.append(row)
    expected_order = sorted(
        rows,
        key=lambda row: (
            str(row["event_timestamp"]),
            str(row["event_family"]),
            str(row["event_id"]),
        ),
    )
    if rows != expected_order:
        raise O1SyntheticValidationError("SYNTHETIC_EVENT_ROW_ORDER_INVALID")
    if set(str(row["event_family"]) for row in rows) != set(families):
        raise O1SyntheticValidationError("SYNTHETIC_EVENT_FAMILY_SET_INCOMPLETE")
    return rows


def _selected_feature_ids(
    *,
    audit_policy: Mapping[str, Any],
    historical_policy: Mapping[str, Any],
    feature_ids_by_family: Mapping[str, Sequence[str]],
) -> list[str]:
    family_order = _string_list(
        _mapping(historical_policy.get("feature_policy")).get("family_order")
    )
    accumulated: list[str] = []
    for family in family_order:
        accumulated.extend(str(item) for item in feature_ids_by_family[family])
        if family == _FEATURE_FAMILY:
            break
    selected = _string_list(
        _mapping(audit_policy.get("model_feature_contract")).get("feature_ids")
    )
    if accumulated != selected or len(selected) != 28:
        raise O1SyntheticValidationError("REVIEWED_FEATURE_PREFIX_MISMATCH")
    return selected


def _build_dataset_rows(
    *,
    audit_policy: Mapping[str, Any],
    panel: pd.DataFrame,
    feature_frame: pd.DataFrame,
    feature_ids: Sequence[str],
) -> list[dict[str, Any]]:
    prices = _price_matrix(panel)
    sessions = [str(value) for value in prices.index]
    horizon = _primary_horizon(audit_policy)
    primary_start = str(_mapping(audit_policy.get("data_contract"))["primary_research_start"])
    rows: list[dict[str, Any]] = []
    for index, decision_date in enumerate(sessions):
        label_end_index = index + horizon
        if decision_date < primary_start or label_end_index >= len(sessions):
            continue
        values = feature_frame.loc[decision_date, list(feature_ids)].to_numpy(dtype=float)
        if not np.isfinite(values).all():
            continue
        label_end = sessions[label_end_index]
        target = (
            float(prices.loc[label_end, "QQQ"] / prices.loc[decision_date, "QQQ"] - 1.0)
            - float(prices.loc[label_end, "SGOV"] / prices.loc[decision_date, "SGOV"] - 1.0)
        )
        rows.append(
            {
                "row_id": f"O1-{decision_date}",
                "decision_date": decision_date,
                "label_interval_start": sessions[index + 1],
                "label_interval_end": label_end,
                "label_available_on_session": label_end,
                "target_value": target,
                "features": {
                    feature_id: float(value)
                    for feature_id, value in zip(feature_ids, values, strict=True)
                },
            }
        )
    return rows


def _build_fold_ledger(
    *,
    audit_policy: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    split = _mapping(audit_policy.get("split_contract"))
    initial = _positive_int(split.get("initial_train_raw_rows"))
    test_size = _positive_int(split.get("outer_test_raw_rows"))
    final_floor = _positive_int(split.get("final_partial_raw_row_floor"))
    embargo = _positive_int(split.get("embargo_common_sessions"))
    dates = [str(row["decision_date"]) for row in rows]
    folds: list[dict[str, Any]] = []
    test_start_index = initial
    fold_number = 1
    while test_start_index < len(rows):
        remaining = len(rows) - test_start_index
        if remaining < test_size and (
            split.get("include_final_partial_fold") is not True
            or remaining < final_floor
        ):
            break
        test_count = min(test_size, remaining)
        test_end_index = test_start_index + test_count - 1
        embargo_start_index = max(0, test_start_index - embargo)
        embargo_end_index = min(len(rows) - 1, test_end_index + embargo)
        train_cutoff_index = embargo_start_index - 1
        if train_cutoff_index < 0:
            break
        test_start = dates[test_start_index]
        test_end = dates[test_end_index]
        embargo_start = dates[embargo_start_index]
        embargo_end = dates[embargo_end_index]
        train_cutoff = dates[train_cutoff_index]
        train_dates: list[str] = []
        maturity_rejections = 0
        test_overlap_rejections = 0
        embargo_overlap_rejections = 0
        embargo_decision_rejections = 0
        for row in rows[:test_start_index]:
            decision_date = str(row["decision_date"])
            if decision_date >= embargo_start:
                embargo_decision_rejections += 1
                continue
            if str(row["label_available_on_session"]) > train_cutoff:
                maturity_rejections += 1
                continue
            label_start = str(row["label_interval_start"])
            label_end = str(row["label_interval_end"])
            if _intervals_overlap(label_start, label_end, test_start, test_end):
                test_overlap_rejections += 1
                continue
            if _intervals_overlap(label_start, label_end, embargo_start, embargo_end):
                embargo_overlap_rejections += 1
                continue
            train_dates.append(decision_date)
        test_dates = dates[test_start_index : test_end_index + 1]
        folds.append(
            {
                "fold_id": f"F{fold_number:02d}",
                "train_start": train_dates[0] if train_dates else None,
                "train_cutoff": train_cutoff,
                "test_start": test_start,
                "test_end": test_end,
                "embargo_start": embargo_start,
                "embargo_end": embargo_end,
                "candidate_train_row_count": test_start_index,
                "train_row_count": len(train_dates),
                "test_row_count": len(test_dates),
                "final_partial_fold": test_count < test_size,
                "maturity_rejection_count": maturity_rejections,
                "test_overlap_rejection_count": test_overlap_rejections,
                "embargo_overlap_rejection_count": embargo_overlap_rejections,
                "embargo_decision_rejection_count": embargo_decision_rejections,
                "train_decision_dates": train_dates,
                "test_decision_dates": test_dates,
                "train_membership_sha256": _sha256_json(train_dates),
                "test_membership_sha256": _sha256_json(test_dates),
            }
        )
        fold_number += 1
        test_start_index += test_count
    return folds


def _build_event_episodes(
    *,
    audit_policy: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
    common_sessions: Sequence[str],
) -> list[dict[str, Any]]:
    window = _mapping(audit_policy.get("event_contract")).get(
        "event_window_common_sessions"
    )
    if not isinstance(window, Sequence) or list(window) != [-1, 1]:
        raise O1SyntheticValidationError("EVENT_WINDOW_CONTRACT_INVALID")
    session_index = {value: index for index, value in enumerate(common_sessions)}
    episodes: list[dict[str, Any]] = []
    for event in events:
        event_date = _parse_aware_timestamp(str(event["event_timestamp"])).date().isoformat()
        if event_date not in session_index:
            raise O1SyntheticValidationError("SYNTHETIC_EVENT_ANCHOR_NOT_COMMON_SESSION")
        index = session_index[event_date]
        if index == 0 or index + 1 >= len(common_sessions):
            raise O1SyntheticValidationError("SYNTHETIC_EVENT_WINDOW_OUT_OF_RANGE")
        episodes.append(
            {
                "event_id": str(event["event_id"]),
                "event_family": str(event["event_family"]),
                "anchor_session": event_date,
                "window_start_session": common_sessions[index - 1],
                "window_end_session": common_sessions[index + 1],
            }
        )
    return episodes


def _independent_feature_frame(
    historical_policy: Mapping[str, Any],
    panel: pd.DataFrame,
) -> pd.DataFrame:
    prices = _price_matrix(panel)
    result = pd.DataFrame(index=prices.index)
    feature_policy = _mapping(historical_policy.get("feature_policy"))
    annualization = float(feature_policy.get("annualization_sessions") or 252)
    for definition in _mapping_rows(feature_policy.get("features")):
        feature_id = str(definition["feature_id"])
        kind = str(definition["kind"])
        if kind == "total_return":
            asset = str(definition["asset"])
            sessions = _positive_int(definition["sessions"])
            values = prices[asset].divide(prices[asset].shift(sessions)).subtract(1.0)
        elif kind == "relative_return":
            asset = str(definition["asset"])
            benchmark = str(definition["benchmark"])
            sessions = _positive_int(definition["sessions"])
            ratio = prices[asset].divide(prices[benchmark])
            values = ratio.divide(ratio.shift(sessions)).subtract(1.0)
        elif kind == "realized_volatility":
            asset = str(definition["asset"])
            sessions = _positive_int(definition["sessions"])
            returns = prices[asset].divide(prices[asset].shift(1)).subtract(1.0)
            values = returns.rolling(sessions, min_periods=sessions).std(
                ddof=0
            ) * math.sqrt(annualization)
        elif kind == "current_drawdown":
            asset = str(definition["asset"])
            sessions = _positive_int(definition["sessions"])
            rolling_peak = prices[asset].rolling(
                sessions,
                min_periods=sessions,
            ).max()
            values = prices[asset].divide(rolling_peak).subtract(1.0)
        elif kind == "difference":
            left = str(definition["left_feature"])
            right = str(definition["right_feature"])
            if left not in result or right not in result:
                raise O1SyntheticValidationError(
                    "INDEPENDENT_FEATURE_DEPENDENCY_INVALID"
                )
            values = result[left].subtract(result[right])
        else:
            raise O1SyntheticValidationError("INDEPENDENT_FEATURE_KIND_INVALID")
        result[feature_id] = values.astype(float)
    return result


def _row_reconstruction_errors(
    *,
    audit_policy: Mapping[str, Any],
    panel: pd.DataFrame,
    feature_frame: pd.DataFrame,
    feature_ids: Sequence[str],
    rows: Sequence[Mapping[str, Any]],
) -> list[str]:
    errors: list[str] = []
    prices = _price_matrix(panel)
    sessions = [str(value) for value in prices.index]
    session_index = {value: index for index, value in enumerate(sessions)}
    observed_dates = [str(row.get("decision_date")) for row in rows]
    if observed_dates != sorted(observed_dates) or len(observed_dates) != len(
        set(observed_dates)
    ):
        errors.append("INDEPENDENT_ROW_ORDER_INVALID")
    horizon = _primary_horizon(audit_policy)
    for row in rows:
        decision_date = str(row.get("decision_date"))
        if decision_date not in session_index:
            errors.append("INDEPENDENT_DECISION_SESSION_INVALID")
            continue
        index = session_index[decision_date]
        if index + horizon >= len(sessions):
            errors.append("INDEPENDENT_LABEL_MATURITY_INVALID")
            continue
        expected_end = sessions[index + horizon]
        if (
            row.get("label_interval_start") != sessions[index + 1]
            or row.get("label_interval_end") != expected_end
            or row.get("label_available_on_session") != expected_end
        ):
            errors.append("INDEPENDENT_LABEL_INTERVAL_MISMATCH")
        expected_target = (
            float(prices.loc[expected_end, "QQQ"] / prices.loc[decision_date, "QQQ"] - 1.0)
            - float(prices.loc[expected_end, "SGOV"] / prices.loc[decision_date, "SGOV"] - 1.0)
        )
        if not _float_equal(row.get("target_value"), expected_target):
            errors.append("INDEPENDENT_TARGET_RECONSTRUCTION_MISMATCH")
        observed_features = _mapping(row.get("features"))
        if list(observed_features) != list(feature_ids):
            errors.append("INDEPENDENT_FEATURE_ORDER_MISMATCH")
            continue
        for feature_id in feature_ids:
            expected_value = float(feature_frame.loc[decision_date, feature_id])
            if not _float_equal(observed_features.get(feature_id), expected_value):
                errors.append("INDEPENDENT_FEATURE_RECONSTRUCTION_MISMATCH")
                break
    return list(dict.fromkeys(errors))


def _dataset_commitment(
    *,
    rows: Sequence[Mapping[str, Any]],
    folds: Sequence[Mapping[str, Any]],
    event_episodes: Sequence[Mapping[str, Any]],
) -> str:
    return _sha256_json(
        {
            "rows": rows,
            "fold_ledger": folds,
            "event_episodes": event_episodes,
        }
    )


def _safety_payload() -> dict[str, Any]:
    return {
        "synthetic_fixture_only": True,
        "real_data_accessed": False,
        "real_coverage_read": False,
        "coverage_classification_emitted": False,
        "model_training_executed": False,
        "predictions_generated": False,
        "prospective_accessed": False,
        "candidate_family_created": False,
        "strategy_backtest_executed": False,
        "target_weights_generated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _price_matrix(panel: pd.DataFrame) -> pd.DataFrame:
    return panel.pivot(index="date", columns="ticker", values="adj_close").sort_index()


def _common_sessions(panel: pd.DataFrame) -> list[str]:
    return [str(value) for value in _price_matrix(panel).index]


def _primary_horizon(audit_policy: Mapping[str, Any]) -> int:
    return _positive_int(
        _mapping(audit_policy.get("target_contract")).get(
            "primary_horizon_common_sessions"
        )
    )


def _parse_aware_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise O1SyntheticValidationError("SYNTHETIC_EVENT_TIMESTAMP_INVALID") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise O1SyntheticValidationError("SYNTHETIC_EVENT_TIMESTAMP_NOT_AWARE")
    return parsed


def _intervals_overlap(
    left_start: str,
    left_end: str,
    right_start: str,
    right_end: str,
) -> bool:
    return left_start <= right_end and right_start <= left_end


def _float_equal(observed: Any, expected: float) -> bool:
    try:
        value = float(observed)
    except (TypeError, ValueError):
        return False
    return math.isfinite(value) and math.isclose(
        value,
        expected,
        rel_tol=_FLOAT_TOLERANCE,
        abs_tol=_FLOAT_TOLERANCE,
    )


def _positive_int(value: Any) -> int:
    if isinstance(value, bool):
        raise O1SyntheticValidationError("POSITIVE_INTEGER_CONTRACT_INVALID")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise O1SyntheticValidationError("POSITIVE_INTEGER_CONTRACT_INVALID") from exc
    if parsed <= 0:
        raise O1SyntheticValidationError("POSITIVE_INTEGER_CONTRACT_INVALID")
    return parsed


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _mapping(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    return {str(key): item for key, item in value.items()}


def _mapping_rows(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [_mapping(row) for row in value]


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [str(item) for item in value]
