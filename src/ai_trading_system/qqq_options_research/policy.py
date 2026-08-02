from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, ValidationInfo, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research.contracts import (
    QQQ_OPTIONS_CONTRACT_SHA256,
    QQQ_OPTIONS_SCHEMA_NAMES,
    ExportClassification,
    QQQOptionsContractError,
    QQQOptionsSafetyBoundary,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QQQ_OPTIONS_SHARED_CONTRACT_POLICY_PATH = Path(
    "config/research/qqq_options_shared_contract_v1.yaml"
)

_EXPECTED_ENUMS: dict[str, tuple[str, ...]] = {
    "capability_status": ("CONFIRMED", "CONTRADICTED", "UNKNOWN"),
    "dq_pit_status": ("FAIL", "NOT_EVALUATED", "PASS"),
    "export_classification": (
        "EXPORT_ALLOWED_DERIVED",
        "EXPORT_PROHIBITED",
        "QC_ONLY_NOT_EXPORTED",
        "UNKNOWN_REQUIRES_LICENSE_REVIEW",
    ),
    "option_right": ("CALL", "PUT"),
    "order_lifecycle": (
        "CANCELED",
        "CREATED",
        "FILLED",
        "PARTIALLY_FILLED",
        "REJECTED",
        "SUBMITTED",
        "UPDATED",
    ),
    "order_side": ("BUY_TO_OPEN", "SELL_TO_CLOSE"),
    "position_lifecycle": (
        "CLOSED",
        "EXIT_BLOCKED",
        "EXIT_PENDING",
        "FLAT",
        "INTENT_PENDING",
        "INVALID_RUN",
        "OPEN",
        "OPEN_PARTIAL",
        "SCOPE_VIOLATION",
    ),
    "reconciliation_status": (
        "EXPLAINED_DIFFERENCE",
        "FAIL",
        "INCOMPLETE",
        "PASS",
    ),
    "signal_direction": ("FLAT", "LONG_CALL", "LONG_PUT"),
}


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


def _required(value: str, field: str) -> str:
    if not value or value != value.strip():
        raise ValueError(f"{field} must be non-empty normalized text")
    return value


class SchemaRegistration(_StrictModel):
    schema_name: str
    schema_version: Literal["1.0.0"]
    model_name: str

    @field_validator("schema_name", "model_name")
    @classmethod
    def _validate_text(cls, value: str, info: ValidationInfo) -> str:
        return _required(value, str(info.field_name))


class EnumRegistration(_StrictModel):
    enum_id: str
    values: tuple[str, ...]

    @field_validator("enum_id")
    @classmethod
    def _validate_enum_id(cls, value: str) -> str:
        return _required(value, "enum_id")

    @field_validator("values")
    @classmethod
    def _validate_values(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or value != tuple(sorted(value)) or len(value) != len(set(value)):
            raise ValueError("enum values must be non-empty, sorted, and unique")
        return value


class QQQOptionsSharedContractPolicy(_StrictModel):
    schema_version: Literal["qqq_options_shared_contract_policy.v1"]
    policy_id: Literal["qqq_options_shared_contract_v1"]
    policy_version: Literal["1.0.0"]
    status: Literal["REVIEWED_ACTIVE_CONTRACT_FREEZE"]
    owner: str
    owner_decision: str
    rationale: str
    intended_effect: str
    validation_plan: str
    review_condition: str
    expiry_condition: str
    supported_schemas: tuple[SchemaRegistration, ...]
    enum_contracts: tuple[EnumRegistration, ...]
    account_currency: Literal["USD"]
    premium_unit: Literal["USD_PER_SHARE"]
    cash_unit: Literal["USD"]
    storage_timezone: Literal["UTC"]
    exchange_timezone: Literal["America/New_York"]
    checksum_algorithm: Literal["SHA-256"]
    canonical_json: Literal["UTF8_SORTED_KEYS_INDENT2_LF_NO_NAN"]
    contract_schema_sha256: str
    allowed_export_classifications: tuple[ExportClassification, ...]
    raw_field_classifications: tuple[Literal["EXPORT_PROHIBITED", "QC_ONLY_NOT_EXPORTED"], ...]
    investment_thresholds_frozen: Literal[False]
    threshold_owner_review_exit_condition: str
    license_unknown_state: Literal["UNKNOWN_REQUIRES_LICENSE_REVIEW"]
    safety: QQQOptionsSafetyBoundary

    @field_validator(
        "owner",
        "owner_decision",
        "rationale",
        "intended_effect",
        "validation_plan",
        "review_condition",
        "expiry_condition",
        "threshold_owner_review_exit_condition",
    )
    @classmethod
    def _validate_text(cls, value: str, info: ValidationInfo) -> str:
        return _required(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_contract_freeze(self) -> Self:
        if self.contract_schema_sha256 != QQQ_OPTIONS_CONTRACT_SHA256:
            raise ValueError("contract schema hash differs from the reviewed v1 freeze")
        schema_names = tuple(item.schema_name for item in self.supported_schemas)
        if schema_names != tuple(sorted(QQQ_OPTIONS_SCHEMA_NAMES)):
            raise ValueError("supported schemas must exactly match the shared record contract")
        if len(schema_names) != len(set(schema_names)):
            raise ValueError("supported schema names must be unique")
        enum_contracts = {item.enum_id: item.values for item in self.enum_contracts}
        if enum_contracts != _EXPECTED_ENUMS:
            raise ValueError("enum contracts differ from the reviewed v1 freeze")
        if self.allowed_export_classifications != tuple(
            sorted(
                (
                    "QC_ONLY_NOT_EXPORTED",
                    "EXPORT_ALLOWED_DERIVED",
                    "UNKNOWN_REQUIRES_LICENSE_REVIEW",
                    "EXPORT_PROHIBITED",
                )
            )
        ):
            raise ValueError("export classifications differ from the reviewed v1 freeze")
        if self.raw_field_classifications != (
            "EXPORT_PROHIBITED",
            "QC_ONLY_NOT_EXPORTED",
        ):
            raise ValueError("raw fields must remain prohibited or QuantConnect-only")
        return self


@dataclass(frozen=True)
class QQQOptionsSharedContractPolicyLoadResult:
    policy: QQQOptionsSharedContractPolicy
    policy_path: Path
    policy_sha256: str


def load_qqq_options_shared_contract_policy(
    path: Path = DEFAULT_QQQ_OPTIONS_SHARED_CONTRACT_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsSharedContractPolicyLoadResult:
    resolved = path if path.is_absolute() else project_root / path
    try:
        content = resolved.read_bytes()
        payload = safe_load_yaml_path(resolved)
        if not isinstance(payload, dict):
            raise TypeError("policy root must be a mapping")
        policy = QQQOptionsSharedContractPolicy.model_validate(payload, strict=False)
    except (OSError, TypeError, ValueError) as exc:
        raise QQQOptionsContractError(
            "QQQ_OPTIONS_SHARED_POLICY_INVALID", f"{resolved}: {exc}"
        ) from exc
    return QQQOptionsSharedContractPolicyLoadResult(
        policy=policy,
        policy_path=resolved,
        policy_sha256=hashlib.sha256(content).hexdigest(),
    )


__all__ = [
    "DEFAULT_QQQ_OPTIONS_SHARED_CONTRACT_POLICY_PATH",
    "EnumRegistration",
    "QQQOptionsSharedContractPolicy",
    "QQQOptionsSharedContractPolicyLoadResult",
    "SchemaRegistration",
    "load_qqq_options_shared_contract_policy",
]
