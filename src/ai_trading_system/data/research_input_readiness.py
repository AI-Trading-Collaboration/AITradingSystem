"""Read-only inventory before research dispatch; never a data-consumption authority."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import stat
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.contracts.data_quality_capability import CapabilityFileBinding
from ai_trading_system.contracts.data_quality_execution import (
    DataQualityDateWindow,
    DataQualityExecutionReceipt,
)
from ai_trading_system.data.immutable_publish import (
    DataPublicationError,
    read_contained_artifact_bytes,
)
from ai_trading_system.data.quality_execution import (
    CanonicalDataQualityExecutionRequest,
    verify_data_quality_execution_receipt,
)
from ai_trading_system.trading_calendar import is_us_equity_trading_day
from ai_trading_system.us_equity_special_closure_policy import (
    CURRENT_US_EQUITY_SPECIAL_CLOSURE_POLICY_RELATIVE_PATH,
    default_us_equity_special_closure_policy,
)


class ConsumerInputDependencies(BaseModel):
    """Explicit diagnostic request, not a reviewed capability or an authorization."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    consumer_id: str = Field(min_length=1)
    consumer_version: str = Field(min_length=1)
    price_tickers: tuple[str, ...]
    price_fields: tuple[str, ...]
    rate_series: tuple[str, ...]
    rate_fields: tuple[str, ...]

    @model_validator(mode="after")
    def validate_dependencies(self) -> ConsumerInputDependencies:
        for field in ("price_tickers", "price_fields", "rate_series", "rate_fields"):
            values = getattr(self, field)
            if any(not value or value != value.strip() for value in values):
                raise ValueError(f"{field}: empty or unnormalized dependency")
            if len(values) != len(set(values)):
                raise ValueError(f"{field}: duplicate dependency")
        if not self.price_tickers or not {"date", "ticker"}.issubset(self.price_fields):
            raise ValueError("price dependencies require tickers and date/ticker fields")
        if self.rate_series and not {"date", "series"}.issubset(self.rate_fields):
            raise ValueError("rate dependencies require date/series fields")
        return self


class ResearchInputReadinessRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["research_input_readiness_request.v1"]
    execution_root: Path
    source_root: Path
    as_of: date
    requested_start: date
    requested_end: date
    execution_profile_id: Literal["manual.v1", "daily_default.v1"]
    expected_price_tickers: tuple[str, ...]
    expected_rate_series: tuple[str, ...]
    require_secondary_prices: bool = Field(strict=True)
    inputs: tuple[CapabilityFileBinding, ...]
    policy: CapabilityFileBinding
    receipt: CapabilityFileBinding | None
    consumer: ConsumerInputDependencies

    @field_validator("as_of", "requested_start", "requested_end", mode="before")
    @classmethod
    def validate_date(cls, value: Any) -> date:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, str):
            return date.fromisoformat(value)
        raise ValueError("dates must be explicit ISO dates")

    @model_validator(mode="after")
    def validate_request(self) -> ResearchInputReadinessRequest:
        roles = [binding.role for binding in self.inputs]
        if len(roles) != len(set(roles)):
            raise ValueError("duplicate input role")
        allowed = {"prices", "rates", "manifest", "secondary_prices", "backtest_manifest"}
        if not {"prices", "rates", "manifest"}.issubset(roles) or not set(roles) <= allowed:
            raise ValueError(
                "input roles must declare prices/rates/manifest and known optional roles"
            )
        if self.policy.role != "policy" or (self.receipt and self.receipt.role != "receipt"):
            raise ValueError("policy/receipt bindings must use their respective roles")
        if not set(self.consumer.price_tickers) <= set(self.expected_price_tickers):
            raise ValueError("consumer price dependency is outside canonical requested scope")
        if not set(self.consumer.rate_series) <= set(self.expected_rate_series):
            raise ValueError("consumer rate dependency is outside canonical requested scope")
        self.canonical_request()
        return self

    def canonical_request(self) -> CanonicalDataQualityExecutionRequest:
        paths = {item.role: Path(item.path) for item in self.inputs}
        return CanonicalDataQualityExecutionRequest(
            as_of=self.as_of,
            requested_window=DataQualityDateWindow(self.requested_start, self.requested_end),
            prices_path=paths["prices"],
            rates_path=paths["rates"],
            manifest_path=paths["manifest"],
            expected_price_tickers=self.expected_price_tickers,
            expected_rate_series=self.expected_rate_series,
            execution_profile_id=self.execution_profile_id,
            secondary_prices_path=paths.get("secondary_prices"),
            require_secondary_prices=self.require_secondary_prices,
            backtest_manifest_path=paths.get("backtest_manifest"),
            policy_path=Path(self.policy.path),
        )


class ReadinessInspectionError(ValueError):
    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}")


def inspect_research_input_readiness(request: ResearchInputReadinessRequest) -> dict[str, Any]:
    """Inspect only explicit inputs; no runner, DQ calculation, copy, or output write."""

    result: dict[str, Any] = {
        "schema_version": "research_input_readiness.v1",
        "status": "NOT_READY",
        "consumer": request.consumer.model_dump(mode="json"),
        "execution_root": str(request.execution_root),
        "source_root": str(request.source_root),
        "inspection_code_root": str(PROJECT_ROOT),
        "as_of": request.as_of.isoformat(),
        "execution_profile_id": request.execution_profile_id,
        "requested_window": {
            "start": request.requested_start.isoformat(),
            "end": request.requested_end.isoformat(),
        },
        "evaluated_window": None,
        "canonical_receipt_verification": "NOT_PERFORMED",
        "consumer_capability_verification": "NOT_PERFORMED",
        "input_inventory": [],
        "required_input_coverage": [],
        "blockers": [],
        "dispatch_allowed": False,
        "consumer_cutover_allowed": False,
        "dq_validation_executed": False,
        "production_effect": "none",
        "broker_action": "none",
        "limitations": [
            "仅表示本次显式输入的只读检查，不替代执行时 DQ/PIT、consumer scope 或运行授权。",
            "rate series 日期范围仅披露；质量结论来自既有 canonical receipt，不在诊断层放宽政策。",
        ],
    }
    try:
        # Pydantic model_copy(update=...) can bypass construction validation.
        # Revalidate the explicit request at the public API boundary as well.
        request = ResearchInputReadinessRequest.model_validate(request.model_dump())
        root = _root(request.source_root)
        execution_root = _root(request.execution_root)
        if root != execution_root:
            raise ReadinessInspectionError(
                "EXECUTION_SOURCE_ROOT_MISMATCH", "执行根与源根不同，不能跨根准入既有证据"
            )
        bindings = (*request.inputs, request.policy)
        contents: dict[str, bytes] = {}
        for binding in bindings:
            try:
                raw = _read_binding(root, binding)
                contents[binding.role] = raw
                result["input_inventory"].append(binding.model_dump(mode="json"))
            except (ReadinessInspectionError, DataPublicationError, OSError) as exc:
                _block(result, exc)
        _inspect_coverage(request, contents, root, result)
        if request.receipt is None:
            raise ReadinessInspectionError(
                "CANONICAL_RECEIPT_MISSING", "未提供既有 canonical receipt"
            )
        raw_receipt = _read_binding(root, request.receipt)
        receipt = DataQualityExecutionReceipt.from_json_bytes(raw_receipt)
        result["evaluated_window"] = receipt.evaluated_window.to_dict()
        result["canonical_report_status"] = receipt.report.status
        _match_receipt(request, receipt, root)
        verified = verify_data_quality_execution_receipt(
            Path(request.receipt.path),
            expected_as_of=request.as_of,
            expected_policy_path=Path(request.policy.path),
            expected_input_roles={item.role for item in request.inputs if item.role != "manifest"},
            project_root=root,
        )
        if verified.receipt != receipt:
            raise ReadinessInspectionError("RECEIPT_CHANGED", "核查期间 receipt 已变化")
        # Inventory is not an admission token: detect mutations while inspecting and
        # still require all authoritative execution-time gates after this function.
        for binding in (*bindings, request.receipt):
            _read_binding(root, binding)
        result["canonical_receipt_verification"] = "PASS"
        result["receipt_id"] = receipt.receipt_id
    except (ReadinessInspectionError, DataPublicationError, ValueError, OSError) as exc:
        _block(result, exc)
    if not result["blockers"]:
        result["status"] = "READY_FOR_REVIEW"
    return result


def _root(path: Path) -> Path:
    if not path.is_absolute():
        raise ReadinessInspectionError("ROOT_INVALID", "root 必须为显式绝对目录")
    for item in (path, *path.parents):
        metadata = item.lstat()
        if stat.S_ISLNK(metadata.st_mode) or getattr(metadata, "st_file_attributes", 0) & getattr(
            stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400
        ):
            raise ReadinessInspectionError("ROOT_REPARSE_FORBIDDEN", str(path))
    if not path.is_dir():
        raise ReadinessInspectionError("ROOT_INVALID", str(path))
    return path.resolve(strict=True)


def _relative(root: Path, value: str) -> str:
    path = Path(value)
    candidate = path if path.is_absolute() else root / path
    # Do not resolve links before the contained reader sees the original path.
    try:
        relative = candidate.relative_to(root)
    except ValueError as exc:
        raise ReadinessInspectionError("INPUT_OUTSIDE_EXECUTION_ROOT", value) from exc
    if ".." in relative.parts or not relative.parts:
        raise ReadinessInspectionError("INPUT_OUTSIDE_EXECUTION_ROOT", value)
    return relative.as_posix()


def _read_binding(root: Path, binding: CapabilityFileBinding) -> bytes:
    raw = read_contained_artifact_bytes(root=root, relative_path=_relative(root, binding.path))
    if len(raw) != binding.size_bytes or hashlib.sha256(raw).hexdigest() != binding.sha256:
        raise ReadinessInspectionError("INPUT_BINDING_MISMATCH", binding.role)
    if binding.role in {"prices", "rates", "manifest", "secondary_prices"}:
        rows = csv.DictReader(io.StringIO(raw.decode("utf-8-sig")))
        if sum(1 for _ in rows) != binding.row_count:
            raise ReadinessInspectionError("INPUT_ROW_COUNT_MISMATCH", binding.role)
    elif binding.row_count != 0:
        raise ReadinessInspectionError("INPUT_ROW_COUNT_MISMATCH", binding.role)
    return raw


def _match_receipt(
    request: ResearchInputReadinessRequest, receipt: DataQualityExecutionReceipt, root: Path
) -> None:
    if receipt.requested_window != request.canonical_request().requested_window:
        raise ReadinessInspectionError("REQUESTED_WINDOW_MISMATCH", "receipt 与请求窗口不同")
    invocation = {item.name: json.loads(item.value_json) for item in receipt.invocation}
    expected = {
        "execution_profile_id": request.execution_profile_id,
        "expected_price_tickers": list(request.expected_price_tickers),
        "expected_rate_series": list(request.expected_rate_series),
        "require_secondary_prices": request.require_secondary_prices,
        "manifest_path": _relative(
            root, next(x.path for x in request.inputs if x.role == "manifest")
        ),
    }
    if any(invocation.get(name) != value for name, value in expected.items()):
        raise ReadinessInspectionError(
            "RECEIPT_REQUEST_MISMATCH", "profile、scope 或 manifest 不匹配"
        )
    declared = {item.role: item for item in request.inputs}
    for actual in receipt.inputs:
        binding = declared.get(actual.role)
        if binding is None or (
            actual.path != _relative(root, binding.path)
            or actual.sha256 != binding.sha256
            or actual.size_bytes != binding.size_bytes
            or actual.row_count != binding.row_count
        ):
            raise ReadinessInspectionError("RECEIPT_INPUT_MISMATCH", actual.role)
        manifest = declared["manifest"]
        if actual.role != "backtest_manifest" and actual.manifest_sha256 != manifest.sha256:
            raise ReadinessInspectionError("RECEIPT_MANIFEST_MISMATCH", actual.role)
    # The read-only tool may run in another checkout; calendar/verifier semantics
    # must nevertheless equal the implementation bytes committed by the receipt.
    for source in receipt.validator.implementation_sources:
        raw = read_contained_artifact_bytes(root=PROJECT_ROOT, relative_path=source.path)
        if hashlib.sha256(raw).hexdigest() != source.sha256:
            raise ReadinessInspectionError("INSPECTION_CODE_IDENTITY_MISMATCH", source.path)


def _inspect_coverage(
    request: ResearchInputReadinessRequest,
    contents: dict[str, bytes],
    root: Path,
    result: dict[str, Any],
) -> None:
    calendar = default_us_equity_special_closure_policy()
    calendar_raw = read_contained_artifact_bytes(
        root=root,
        relative_path=CURRENT_US_EQUITY_SPECIAL_CLOSURE_POLICY_RELATIVE_PATH.as_posix(),
    )
    if hashlib.sha256(calendar_raw).hexdigest() != calendar.sha256:
        raise ReadinessInspectionError("CALENDAR_IDENTITY_MISMATCH", "源日历与核查日历不同")
    result["calendar"] = {"id": calendar.calendar_id, "policy_sha256": calendar.sha256}
    sessions: set[date] = set()
    current = request.requested_start
    while current <= request.requested_end:
        if is_us_equity_trading_day(current):
            sessions.add(current)
        current += timedelta(days=1)
    if not sessions:
        raise ReadinessInspectionError("REQUESTED_SESSION_SET_EMPTY", "请求窗口内没有 XNYS session")
    for role, key, instruments, fields in (
        ("prices", "ticker", request.consumer.price_tickers, request.consumer.price_fields),
        ("rates", "series", request.consumer.rate_series, request.consumer.rate_fields),
    ):
        if not instruments or role not in contents:
            continue
        reader = csv.DictReader(io.StringIO(contents[role].decode("utf-8-sig")))
        if not set(fields) <= set(reader.fieldnames or ()):
            raise ReadinessInspectionError("REQUIRED_FIELDS_MISSING", role)
        observed: dict[str, set[date]] = {instrument: set() for instrument in instruments}
        for row in reader:
            instrument = row.get(key)
            if instrument not in observed:
                continue
            row_date = date.fromisoformat(row["date"])
            if request.requested_start <= row_date <= request.requested_end:
                if any(not (row.get(field) or "").strip() for field in fields):
                    raise ReadinessInspectionError(
                        "REQUIRED_FIELD_VALUE_MISSING", f"{role}:{instrument}:{row_date}"
                    )
                if row_date in observed[instrument]:
                    raise ReadinessInspectionError(
                        "DUPLICATE_REQUIRED_INPUT_ROW", f"{role}:{instrument}"
                    )
                observed[instrument].add(row_date)
        for instrument, dates in observed.items():
            missing = sorted(sessions - dates) if role == "prices" else []
            result["required_input_coverage"].append(
                {
                    "role": role,
                    "instrument": instrument,
                    "start": min(dates).isoformat() if dates else None,
                    "end": max(dates).isoformat() if dates else None,
                    "observed_date_count": len(dates),
                    "missing_price_sessions": [value.isoformat() for value in missing],
                    "rate_quality_basis": "CANONICAL_RECEIPT_REQUIRED" if role == "rates" else None,
                }
            )
            if missing or not dates:
                _block(
                    result,
                    ReadinessInspectionError(
                        "REQUIRED_INPUT_COVERAGE_MISSING", f"{role}:{instrument}"
                    ),
                )


def _block(result: dict[str, Any], exc: Exception) -> None:
    item = {"code": str(getattr(exc, "code", "INPUT_INSPECTION_INVALID")), "detail": str(exc)}
    if item not in result["blockers"]:
        result["blockers"].append(item)


def load_research_input_readiness_request(content: bytes) -> ResearchInputReadinessRequest:
    """Strict JSON input; duplicate keys/non-finite constants never become a plan."""

    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    def reject(value: str) -> None:
        raise ValueError(f"non-finite JSON constant: {value}")

    payload = json.loads(content, object_pairs_hook=pairs, parse_constant=reject)
    return ResearchInputReadinessRequest.model_validate(payload)
