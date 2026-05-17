from __future__ import annotations

import asyncio
import re
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Self

import yaml
from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.trading_engine.portfolio.paper_portfolio import PaperPortfolio
from ai_trading_system.trading_engine.schemas.portfolio_state import PortfolioState

DEFAULT_IBKR_PAPER_READONLY_CONFIG_PATH = PROJECT_ROOT / "config" / "ibkr_paper_readonly.yaml"
READONLY_PRODUCTION_EFFECT = "none"
_ACCOUNT_PATTERN = re.compile(r"\b(?:DUP?|U)\d{3,}\b", re.IGNORECASE)
_SENSITIVE_KEY_FRAGMENTS = (
    "password",
    "token",
    "secret",
    "authorization",
    "credential",
    "api_key",
    "apikey",
    "session",
    "cookie",
)
_ACCOUNT_KEY_FRAGMENTS = ("account", "acct")
_CASH_SUMMARY_TAGS = {
    "availablefunds",
    "cashbalance",
    "settledcash",
    "totalcashbalance",
    "totalcashvalue",
    "netliquidation",
}


class IBKRReconciliationStatus(StrEnum):
    PASS = "PASS"
    LIMITED = "LIMITED"
    BLOCK = "BLOCK"


class IBKRPaperReadOnlyConfig(BaseModel):
    enabled: bool = False
    host: str = "127.0.0.1"
    port: int = Field(default=7497, ge=1, le=65535)
    client_id: int = Field(default=19009, ge=0)
    account_id: str = ""
    trading_mode: str = "paper"
    readonly: bool = True
    production_effect: str = READONLY_PRODUCTION_EFFECT

    @model_validator(mode="after")
    def normalize_config(self) -> Self:
        self.host = self.host.strip()
        self.account_id = self.account_id.strip()
        self.trading_mode = self.trading_mode.strip().lower()
        self.production_effect = self.production_effect.strip().lower()
        return self

    def assert_readonly_paper_settings(self) -> None:
        if self.trading_mode != "paper":
            raise RuntimeError("IBKR Paper read-only sync fail closed: trading_mode must be paper")
        if self.readonly is not True:
            raise RuntimeError("IBKR Paper read-only sync fail closed: readonly must be true")
        if self.production_effect != READONLY_PRODUCTION_EFFECT:
            raise RuntimeError(
                "IBKR Paper read-only sync fail closed: production_effect must be none"
            )

    def assert_connection_enabled(self) -> None:
        if not self.enabled:
            raise RuntimeError("IBKR Paper read-only sync is disabled by config")


@dataclass(frozen=True)
class AccountIdAssessment:
    status: IBKRReconciliationStatus
    masked_account_id: str
    reasons: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "masked_account_id": self.masked_account_id,
            "reasons": list(self.reasons),
        }


class IBKRReadOnlyReconciliationIssue(BaseModel):
    field: str = Field(min_length=1)
    severity: Literal["LIMITED", "BLOCK"]
    symbol: str | None = None
    expected: float | int | str | None = None
    actual: float | int | str | None = None
    message: str = Field(min_length=1)


class IBKRReadOnlyReconciliationResult(BaseModel):
    schema_version: str = "1.0"
    status: IBKRReconciliationStatus
    production_effect: Literal["none"] = READONLY_PRODUCTION_EFFECT
    source: str = "ibkr_paper_readonly"
    cash_summary_present: bool
    compared_positions: int
    unknown_positions: list[str] = Field(default_factory=list)
    issues: list[IBKRReadOnlyReconciliationIssue] = Field(default_factory=list)


class IBKRPaperReadOnlyReconciliation:
    def reconcile(
        self,
        *,
        account_summary: Any,
        positions: Any,
        local_portfolio: PaperPortfolio | PortfolioState | None = None,
    ) -> IBKRReadOnlyReconciliationResult:
        issues: list[IBKRReadOnlyReconciliationIssue] = []
        cash_summary_present = _has_cash_summary(account_summary)
        if not cash_summary_present:
            issues.append(
                IBKRReadOnlyReconciliationIssue(
                    field="cash_summary",
                    severity=IBKRReconciliationStatus.LIMITED.value,
                    message="IBKR account summary did not include a recognizable cash field",
                )
            )

        ibkr_positions = _extract_position_quantities(positions)
        local_positions = _extract_local_position_quantities(local_portfolio)
        unknown_positions: list[str] = []
        for symbol, ibkr_quantity in sorted(ibkr_positions.items()):
            local_quantity = local_positions.get(symbol)
            if local_portfolio is None or local_quantity is None:
                if abs(ibkr_quantity) > 1e-9:
                    unknown_positions.append(symbol)
                    issues.append(
                        IBKRReadOnlyReconciliationIssue(
                            field="unknown_position",
                            severity=IBKRReconciliationStatus.LIMITED.value,
                            symbol=symbol,
                            expected="local PaperPortfolio position",
                            actual=ibkr_quantity,
                            message="IBKR position is not represented in the local PaperPortfolio",
                        )
                    )
                continue
            if abs(local_quantity - ibkr_quantity) > 1e-9:
                issues.append(
                    IBKRReadOnlyReconciliationIssue(
                        field="position_quantity",
                        severity=IBKRReconciliationStatus.BLOCK.value,
                        symbol=symbol,
                        expected=local_quantity,
                        actual=ibkr_quantity,
                        message="IBKR and local PaperPortfolio quantities differ",
                    )
                )

        for symbol, local_quantity in sorted(local_positions.items()):
            if symbol in ibkr_positions or abs(local_quantity) <= 1e-9:
                continue
            issues.append(
                IBKRReadOnlyReconciliationIssue(
                    field="missing_ibkr_position",
                    severity=IBKRReconciliationStatus.BLOCK.value,
                    symbol=symbol,
                    expected=local_quantity,
                    actual=0,
                    message="Local PaperPortfolio has a position missing from IBKR positions",
                )
            )

        return IBKRReadOnlyReconciliationResult(
            status=_reconciliation_status(issues),
            cash_summary_present=cash_summary_present,
            compared_positions=len(ibkr_positions),
            unknown_positions=unknown_positions,
            issues=issues,
        )


class IBKRPaperReadOnlyAdapter:
    def __init__(
        self,
        *,
        config: IBKRPaperReadOnlyConfig,
        client: Any | None = None,
    ) -> None:
        self.config = config
        self._client = client
        self._connected = False

    def connect(self) -> dict[str, Any]:
        self.config.assert_readonly_paper_settings()
        self.config.assert_connection_enabled()
        assessment = assess_paper_account_id(self.config.account_id)
        if assessment.status != IBKRReconciliationStatus.PASS:
            raise RuntimeError(
                "IBKR Paper read-only sync fail closed: account_id must look like a paper account"
            )

        client = self._client or _create_ibkr_client()
        self._client = client
        try:
            client.connect(
                self.config.host,
                self.config.port,
                clientId=self.config.client_id,
                readonly=True,
                account=self.config.account_id,
            )
        except TypeError:
            client.connect(self.config.host, self.config.port, self.config.client_id)
        self._connected = _client_connected(client)
        return {
            "status": "CONNECTED" if self._connected else "UNKNOWN",
            "connected": self._connected,
            "host": self.config.host,
            "port": self.config.port,
            "client_id": self.config.client_id,
            "readonly": True,
            "production_effect": READONLY_PRODUCTION_EFFECT,
            "account_id_masked": mask_account_id(self.config.account_id),
        }

    def disconnect(self) -> None:
        if self._client is None:
            self._connected = False
            return
        disconnect = getattr(self._client, "disconnect", None)
        if callable(disconnect):
            disconnect()
        self._connected = False

    def get_account_summary(self) -> Any:
        raw = _call_client_account_method(
            self._require_client(),
            ("accountSummary", "reqAccountSummary"),
            self.config.account_id,
        )
        return sanitize_ibkr_payload(raw, account_id=self.config.account_id)

    def list_positions(self) -> list[Any]:
        raw = _call_first_available(self._require_client(), ("positions", "reqPositions"))
        return sanitize_ibkr_payload(raw, account_id=self.config.account_id)

    def list_open_orders(self) -> list[Any]:
        raw = _call_first_available(
            self._require_client(),
            ("openOrders", "reqOpenOrders", "reqAllOpenOrders"),
        )
        return sanitize_ibkr_payload(raw, account_id=self.config.account_id)

    def list_executions(self) -> list[Any]:
        raw = _call_first_available(
            self._require_client(),
            ("executions", "fills", "reqExecutions"),
        )
        return sanitize_ibkr_payload(raw, account_id=self.config.account_id)

    def get_contract_details(
        self,
        contract: Any | None = None,
        *,
        symbol: str | None = None,
    ) -> list[Any]:
        requested_contract = contract if contract is not None else _stock_contract(symbol or "NVDA")
        raw = _call_first_available(
            self._require_client(),
            ("reqContractDetails", "contractDetails", "getContractDetails"),
            requested_contract,
        )
        return sanitize_ibkr_payload(raw, account_id=self.config.account_id)

    def submit_order(self, *_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError(
            "IBKR Paper read-only adapter cannot submit_order; this integration is read-only "
            "and production_effect=none"
        )

    def _require_client(self) -> Any:
        if self._client is None:
            raise RuntimeError("IBKR Paper read-only adapter is not connected")
        return self._client


def load_ibkr_paper_readonly_config(
    path: Path | str = DEFAULT_IBKR_PAPER_READONLY_CONFIG_PATH,
) -> IBKRPaperReadOnlyConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as file:
        raw = yaml.safe_load(file) or {}
    return IBKRPaperReadOnlyConfig.model_validate(raw)


def assess_paper_account_id(account_id: str) -> AccountIdAssessment:
    normalized = account_id.strip().upper()
    masked = mask_account_id(account_id)
    if not normalized:
        return AccountIdAssessment(
            status=IBKRReconciliationStatus.BLOCK,
            masked_account_id=masked,
            reasons=("account_id_missing",),
        )
    if normalized.startswith(("DUP", "DU", "PAPER")):
        return AccountIdAssessment(
            status=IBKRReconciliationStatus.PASS,
            masked_account_id=masked,
            reasons=(),
        )
    return AccountIdAssessment(
        status=IBKRReconciliationStatus.BLOCK,
        masked_account_id=masked,
        reasons=("account_id_does_not_look_like_ibkr_paper_account",),
    )


def mask_account_id(account_id: str | None) -> str:
    value = "" if account_id is None else str(account_id).strip()
    if not value:
        return "missing"
    if len(value) <= 2:
        return value[0] + "***"
    if len(value) <= 6:
        return f"{value[:1]}***{value[-1:]}"
    prefix_length = 3 if value.upper().startswith("DUP") else 2
    return f"{value[:prefix_length]}***{value[-4:]}"


def sanitize_ibkr_payload(value: Any, *, account_id: str = "") -> Any:
    return _sanitize_value(_to_jsonable(value), account_id=account_id)


def _create_ibkr_client() -> Any:
    _ensure_asyncio_event_loop()
    try:
        from ib_insync import IB
    except (ImportError, ModuleNotFoundError) as exc:
        raise RuntimeError(
            "ib_insync is required for a real IBKR Paper read-only connection; "
            "tests should inject a mock client"
        ) from exc
    return IB()


def _ensure_asyncio_event_loop() -> None:
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())


def _client_connected(client: Any) -> bool:
    is_connected = getattr(client, "isConnected", None)
    if callable(is_connected):
        return bool(is_connected())
    return True


def _call_first_available(client: Any, method_names: Sequence[str], *args: Any) -> Any:
    for method_name in method_names:
        method = getattr(client, method_name, None)
        if callable(method):
            return method(*args)
    available = ", ".join(method_names)
    raise RuntimeError(f"IBKR client does not expose any expected read-only method: {available}")


def _call_client_account_method(
    client: Any,
    method_names: Sequence[str],
    account_id: str,
) -> Any:
    for method_name in method_names:
        method = getattr(client, method_name, None)
        if not callable(method):
            continue
        if account_id:
            try:
                return method(account=account_id)
            except TypeError:
                try:
                    return method(account_id)
                except TypeError:
                    pass
        return method()
    available = ", ".join(method_names)
    raise RuntimeError(f"IBKR client does not expose any expected account method: {available}")


def _stock_contract(symbol: str) -> Any:
    _ensure_asyncio_event_loop()
    try:
        from ib_insync import Stock
    except ImportError:
        return {"symbol": symbol.upper(), "secType": "STK", "exchange": "SMART", "currency": "USD"}
    return Stock(symbol.upper(), "SMART", "USD")


def _to_jsonable(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if is_dataclass(value) and not isinstance(value, type):
        return _to_jsonable(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _to_jsonable(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    try:
        raw_vars = vars(value)
    except TypeError:
        return str(value)
    if raw_vars:
        return {
            key: _to_jsonable(item) for key, item in raw_vars.items() if not key.startswith("_")
        }
    return str(value)


def _sanitize_value(value: Any, *, account_id: str) -> Any:
    if isinstance(value, Mapping):
        sanitized: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            normalized_key = key_text.lower().replace("-", "_")
            if any(fragment in normalized_key for fragment in _SENSITIVE_KEY_FRAGMENTS):
                sanitized[key_text] = "[REDACTED]"
            elif any(
                fragment in normalized_key for fragment in _ACCOUNT_KEY_FRAGMENTS
            ) and isinstance(item, str | int):
                sanitized[key_text] = mask_account_id(str(item))
            else:
                sanitized[key_text] = _sanitize_value(item, account_id=account_id)
        return sanitized
    if isinstance(value, list):
        return [_sanitize_value(item, account_id=account_id) for item in value]
    if isinstance(value, str):
        redacted = value
        if account_id:
            redacted = redacted.replace(account_id, mask_account_id(account_id))
        return _ACCOUNT_PATTERN.sub(lambda match: mask_account_id(match.group(0)), redacted)
    return value


def _has_cash_summary(value: Any) -> bool:
    jsonable = _to_jsonable(value)
    if isinstance(jsonable, Mapping):
        tag = str(jsonable.get("tag") or jsonable.get("Tag") or "").lower()
        if tag.replace("_", "") in _CASH_SUMMARY_TAGS:
            return True
        for key, item in jsonable.items():
            normalized_key = str(key).lower().replace("_", "")
            if normalized_key in _CASH_SUMMARY_TAGS and item not in ("", None):
                return True
            if _has_cash_summary(item):
                return True
        return False
    if isinstance(jsonable, list):
        if _looks_like_account_summary_row(jsonable):
            return True
        return any(_has_cash_summary(item) for item in jsonable)
    return False


def _looks_like_account_summary_row(row: list[Any]) -> bool:
    if len(row) < 3:
        return False
    tag = str(row[1]).lower().replace("_", "")
    return tag in _CASH_SUMMARY_TAGS and row[2] not in ("", None)


def _extract_local_position_quantities(
    local_portfolio: PaperPortfolio | PortfolioState | None,
) -> dict[str, float]:
    if local_portfolio is None:
        return {}
    state = (
        local_portfolio.snapshot()
        if isinstance(local_portfolio, PaperPortfolio)
        else local_portfolio
    )
    return {position.symbol.upper(): float(position.quantity) for position in state.positions}


def _extract_position_quantities(positions: Any) -> dict[str, float]:
    extracted: dict[str, float] = {}
    for record in _position_records(positions):
        symbol = _extract_position_symbol(record)
        quantity = _extract_position_quantity(record)
        if not symbol or quantity is None:
            continue
        extracted[symbol.upper()] = float(quantity)
    return extracted


def _position_records(positions: Any) -> list[Any]:
    if positions is None:
        return []
    if isinstance(positions, Sequence) and not isinstance(positions, str | bytes | bytearray):
        return list(positions)
    return [positions]


def _extract_position_symbol(record: Any) -> str | None:
    jsonable = _to_jsonable(record)
    if isinstance(jsonable, Mapping):
        direct_symbol = jsonable.get("symbol") or jsonable.get("localSymbol")
        if direct_symbol:
            return str(direct_symbol)
        contract = jsonable.get("contract")
        if isinstance(contract, Mapping):
            contract_symbol = contract.get("symbol") or contract.get("localSymbol")
            if contract_symbol:
                return str(contract_symbol)
    if isinstance(jsonable, list) and len(jsonable) >= 2:
        contract = jsonable[1]
        if isinstance(contract, Mapping):
            contract_symbol = contract.get("symbol") or contract.get("localSymbol")
            if contract_symbol:
                return str(contract_symbol)
    return None


def _extract_position_quantity(record: Any) -> float | None:
    jsonable = _to_jsonable(record)
    if isinstance(jsonable, Mapping):
        for key in ("quantity", "position", "pos"):
            raw_quantity = jsonable.get(key)
            if raw_quantity is None:
                continue
            try:
                return float(raw_quantity)
            except (TypeError, ValueError):
                return None
    if isinstance(jsonable, list) and len(jsonable) >= 3:
        try:
            return float(jsonable[2])
        except (TypeError, ValueError):
            return None
    return None


def _reconciliation_status(
    issues: Sequence[IBKRReadOnlyReconciliationIssue],
) -> IBKRReconciliationStatus:
    if any(issue.severity == IBKRReconciliationStatus.BLOCK.value for issue in issues):
        return IBKRReconciliationStatus.BLOCK
    if issues:
        return IBKRReconciliationStatus.LIMITED
    return IBKRReconciliationStatus.PASS
