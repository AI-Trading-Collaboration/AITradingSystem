from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import PurePosixPath
from typing import ClassVar, NoReturn, Self, cast

from ai_trading_system.contracts.data_quality_execution import (
    DataQualityDateWindow,
    VerifiedDataQualityPreflight,
)

_VERIFIED_CONSUMER_AUTHORIZATION_SEAL = object()
_PRODUCTION_EFFECT_NONE = "none"


class DataQualityConsumerAuthorizationContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True)
class DataQualityConsumerAuthorizationAttestation:
    """Canonical, revocable authorization for exactly one DQ consumer."""

    schema_version: ClassVar[str] = "data_quality_consumer_authorization_attestation.v1"

    consumer_id: str
    consumer_version: str
    policy_id: str
    policy_version: str
    policy_path: str
    policy_sha256: str
    owner_decision_id: str
    authorized_at: datetime
    expires_at: datetime
    as_of: date
    requested_window: DataQualityDateWindow
    evaluated_window: DataQualityDateWindow
    receipt_id: str
    receipt_path: str
    receipt_sha256: str
    receipt_size_bytes: int
    receipt_status: str
    receipt_lineage_sha256: str
    input_roles: tuple[str, ...]
    publication_transaction_id: str
    publication_transaction_path: str
    publication_transaction_sha256: str
    publication_discovery_pointer_path: str
    publication_discovery_pointer_sha256: str
    publication_requested_start: date
    publication_requested_end: date
    publication_lineage_sha256: str
    consumer_dispatch_authorized: bool = True
    reversible_authorization: bool = True
    generic_consumer_cutover_allowed: bool = False
    automatic_non_daily_dispatch: bool = False
    production_effect: str = _PRODUCTION_EFFECT_NONE
    broker_action: str = _PRODUCTION_EFFECT_NONE

    def __post_init__(self) -> None:
        for field, value in (
            ("consumer_id", self.consumer_id),
            ("consumer_version", self.consumer_version),
            ("policy_id", self.policy_id),
            ("policy_version", self.policy_version),
            ("owner_decision_id", self.owner_decision_id),
            ("receipt_id", self.receipt_id),
            ("receipt_status", self.receipt_status),
            ("publication_transaction_id", self.publication_transaction_id),
        ):
            _text(value, field)
        for field, value in (
            ("policy_path", self.policy_path),
            ("receipt_path", self.receipt_path),
            ("publication_transaction_path", self.publication_transaction_path),
            ("publication_discovery_pointer_path", self.publication_discovery_pointer_path),
        ):
            _repo_path(value, field)
        for field, value in (
            ("policy_sha256", self.policy_sha256),
            ("receipt_sha256", self.receipt_sha256),
            ("receipt_lineage_sha256", self.receipt_lineage_sha256),
            ("publication_transaction_sha256", self.publication_transaction_sha256),
            (
                "publication_discovery_pointer_sha256",
                self.publication_discovery_pointer_sha256,
            ),
            ("publication_lineage_sha256", self.publication_lineage_sha256),
        ):
            _sha256(value, field)
        _aware_datetime(self.authorized_at, "authorized_at")
        _aware_datetime(self.expires_at, "expires_at")
        if self.expires_at <= self.authorized_at:
            _fail("DQ_CONSUMER_AUTHORIZATION_EXPIRED", "expires_at must follow authorized_at")
        _date(self.as_of, "as_of")
        _date(self.publication_requested_start, "publication_requested_start")
        _date(self.publication_requested_end, "publication_requested_end")
        if self.publication_requested_start > self.publication_requested_end:
            _fail("DQ_PUBLICATION_WINDOW_MISMATCH", "publication window is reversed")
        if self.as_of != self.requested_window.end:
            _fail("DQ_AS_OF_MISMATCH", "authorization as_of must equal requested window end")
        if not self.requested_window.contains(self.evaluated_window):
            _fail("DQ_WINDOW_MISMATCH", "evaluated window must be within requested window")
        if self.publication_requested_start > self.requested_window.start:
            _fail("DQ_PUBLICATION_WINDOW_MISMATCH", "publication starts after receipt request")
        if self.publication_requested_end != self.requested_window.end:
            _fail("DQ_PUBLICATION_WINDOW_MISMATCH", "publication end differs from receipt request")
        if self.receipt_status != "PASS":
            _fail("DQ_WARNING_NOT_ALLOWED", f"status={self.receipt_status}")
        if isinstance(self.receipt_size_bytes, bool) or self.receipt_size_bytes <= 0:
            _fail("DQ_CONSUMER_AUTHORIZATION_FIELDS_INVALID", "receipt size must be positive")
        roles = tuple(self.input_roles)
        if (
            not roles
            or roles != tuple(sorted(set(roles)))
            or any(not isinstance(item, str) or not item or item != item.strip() for item in roles)
        ):
            _fail("DQ_INPUT_SET_MISMATCH", "input_roles must be sorted, unique and non-empty")
        object.__setattr__(self, "input_roles", roles)
        if not self.consumer_dispatch_authorized or not self.reversible_authorization:
            _fail(
                "DQ_CONSUMER_NOT_AUTHORIZED",
                "scoped dispatch requires an explicit reversible authorization",
            )
        if self.generic_consumer_cutover_allowed or self.automatic_non_daily_dispatch:
            _fail(
                "DQ_CONSUMER_SCOPE_INVALID",
                "generic or non-daily dispatch is outside the scoped authorization",
            )
        if self.production_effect != _PRODUCTION_EFFECT_NONE:
            _fail("PRODUCTION_EFFECT_INVALID", self.production_effect)
        if self.broker_action != _PRODUCTION_EFFECT_NONE:
            _fail("BROKER_ACTION_INVALID", self.broker_action)

    @property
    def authorization_id(self) -> str:
        digest = hashlib.sha256(_canonical_compact(self._semantic_payload())).hexdigest()
        return f"dq_consumer_authorization_{digest}"

    @property
    def canonical_bytes(self) -> bytes:
        return (
            json.dumps(
                self.to_dict(),
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    def _semantic_payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "consumer_id": self.consumer_id,
            "consumer_version": self.consumer_version,
            "policy_id": self.policy_id,
            "policy_version": self.policy_version,
            "policy_path": self.policy_path,
            "policy_sha256": self.policy_sha256,
            "owner_decision_id": self.owner_decision_id,
            "authorized_at": self.authorized_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
            "as_of": self.as_of.isoformat(),
            "requested_window": self.requested_window.to_dict(),
            "evaluated_window": self.evaluated_window.to_dict(),
            "receipt_id": self.receipt_id,
            "receipt_path": self.receipt_path,
            "receipt_sha256": self.receipt_sha256,
            "receipt_size_bytes": self.receipt_size_bytes,
            "receipt_status": self.receipt_status,
            "receipt_lineage_sha256": self.receipt_lineage_sha256,
            "input_roles": list(self.input_roles),
            "publication_transaction_id": self.publication_transaction_id,
            "publication_transaction_path": self.publication_transaction_path,
            "publication_transaction_sha256": self.publication_transaction_sha256,
            "publication_discovery_pointer_path": self.publication_discovery_pointer_path,
            "publication_discovery_pointer_sha256": (self.publication_discovery_pointer_sha256),
            "publication_requested_start": self.publication_requested_start.isoformat(),
            "publication_requested_end": self.publication_requested_end.isoformat(),
            "publication_lineage_sha256": self.publication_lineage_sha256,
            "consumer_dispatch_authorized": self.consumer_dispatch_authorized,
            "reversible_authorization": self.reversible_authorization,
            "generic_consumer_cutover_allowed": self.generic_consumer_cutover_allowed,
            "automatic_non_daily_dispatch": self.automatic_non_daily_dispatch,
            "production_effect": self.production_effect,
            "broker_action": self.broker_action,
        }

    def to_dict(self) -> dict[str, object]:
        return {"authorization_id": self.authorization_id, **self._semantic_payload()}

    @classmethod
    def from_dict(cls, payload: object) -> Self:
        raw = _mapping(payload, "attestation")
        expected = {
            "authorization_id",
            "schema_version",
            "consumer_id",
            "consumer_version",
            "policy_id",
            "policy_version",
            "policy_path",
            "policy_sha256",
            "owner_decision_id",
            "authorized_at",
            "expires_at",
            "as_of",
            "requested_window",
            "evaluated_window",
            "receipt_id",
            "receipt_path",
            "receipt_sha256",
            "receipt_size_bytes",
            "receipt_status",
            "receipt_lineage_sha256",
            "input_roles",
            "publication_transaction_id",
            "publication_transaction_path",
            "publication_transaction_sha256",
            "publication_discovery_pointer_path",
            "publication_discovery_pointer_sha256",
            "publication_requested_start",
            "publication_requested_end",
            "publication_lineage_sha256",
            "consumer_dispatch_authorized",
            "reversible_authorization",
            "generic_consumer_cutover_allowed",
            "automatic_non_daily_dispatch",
            "production_effect",
            "broker_action",
        }
        _exact_fields(raw, expected)
        if raw["schema_version"] != cls.schema_version:
            _fail(
                "DQ_CONSUMER_AUTHORIZATION_SCHEMA_UNSUPPORTED",
                str(raw["schema_version"]),
            )
        attestation = cls(
            consumer_id=_text(raw["consumer_id"], "consumer_id"),
            consumer_version=_text(raw["consumer_version"], "consumer_version"),
            policy_id=_text(raw["policy_id"], "policy_id"),
            policy_version=_text(raw["policy_version"], "policy_version"),
            policy_path=_repo_path(raw["policy_path"], "policy_path"),
            policy_sha256=_sha256(raw["policy_sha256"], "policy_sha256"),
            owner_decision_id=_text(raw["owner_decision_id"], "owner_decision_id"),
            authorized_at=_datetime(raw["authorized_at"], "authorized_at"),
            expires_at=_datetime(raw["expires_at"], "expires_at"),
            as_of=_date_from(raw["as_of"], "as_of"),
            requested_window=_window(raw["requested_window"], "requested_window"),
            evaluated_window=_window(raw["evaluated_window"], "evaluated_window"),
            receipt_id=_text(raw["receipt_id"], "receipt_id"),
            receipt_path=_repo_path(raw["receipt_path"], "receipt_path"),
            receipt_sha256=_sha256(raw["receipt_sha256"], "receipt_sha256"),
            receipt_size_bytes=_integer(raw["receipt_size_bytes"], "receipt_size_bytes"),
            receipt_status=_text(raw["receipt_status"], "receipt_status"),
            receipt_lineage_sha256=_sha256(raw["receipt_lineage_sha256"], "receipt_lineage_sha256"),
            input_roles=_string_tuple(raw["input_roles"], "input_roles"),
            publication_transaction_id=_text(
                raw["publication_transaction_id"], "publication_transaction_id"
            ),
            publication_transaction_path=_repo_path(
                raw["publication_transaction_path"], "publication_transaction_path"
            ),
            publication_transaction_sha256=_sha256(
                raw["publication_transaction_sha256"],
                "publication_transaction_sha256",
            ),
            publication_discovery_pointer_path=_repo_path(
                raw["publication_discovery_pointer_path"],
                "publication_discovery_pointer_path",
            ),
            publication_discovery_pointer_sha256=_sha256(
                raw["publication_discovery_pointer_sha256"],
                "publication_discovery_pointer_sha256",
            ),
            publication_requested_start=_date_from(
                raw["publication_requested_start"], "publication_requested_start"
            ),
            publication_requested_end=_date_from(
                raw["publication_requested_end"], "publication_requested_end"
            ),
            publication_lineage_sha256=_sha256(
                raw["publication_lineage_sha256"], "publication_lineage_sha256"
            ),
            consumer_dispatch_authorized=_boolean(
                raw["consumer_dispatch_authorized"], "consumer_dispatch_authorized"
            ),
            reversible_authorization=_boolean(
                raw["reversible_authorization"], "reversible_authorization"
            ),
            generic_consumer_cutover_allowed=_boolean(
                raw["generic_consumer_cutover_allowed"],
                "generic_consumer_cutover_allowed",
            ),
            automatic_non_daily_dispatch=_boolean(
                raw["automatic_non_daily_dispatch"], "automatic_non_daily_dispatch"
            ),
            production_effect=_text(raw["production_effect"], "production_effect"),
            broker_action=_text(raw["broker_action"], "broker_action"),
        )
        supplied_id = _text(raw["authorization_id"], "authorization_id")
        if supplied_id != attestation.authorization_id:
            _fail(
                "DQ_CONSUMER_AUTHORIZATION_ID_MISMATCH",
                f"supplied={supplied_id} actual={attestation.authorization_id}",
            )
        return attestation

    @classmethod
    def from_json_bytes(cls, content: bytes) -> Self:
        try:
            decoded = content.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise DataQualityConsumerAuthorizationContractError(
                "DQ_CONSUMER_AUTHORIZATION_FIELDS_INVALID",
                "attestation is not UTF-8",
            ) from exc
        try:
            payload = json.loads(
                decoded,
                object_pairs_hook=_reject_duplicate_keys,
                parse_constant=_reject_nonfinite,
            )
        except (json.JSONDecodeError, DataQualityConsumerAuthorizationContractError) as exc:
            if isinstance(exc, DataQualityConsumerAuthorizationContractError):
                raise
            raise DataQualityConsumerAuthorizationContractError(
                "DQ_CONSUMER_AUTHORIZATION_FIELDS_INVALID",
                "attestation is not strict JSON",
            ) from exc
        attestation = cls.from_dict(payload)
        if content != attestation.canonical_bytes:
            _fail(
                "DQ_CONSUMER_AUTHORIZATION_FIELDS_INVALID",
                "attestation bytes are not canonical",
            )
        return attestation


@dataclass(frozen=True, init=False)
class VerifiedDataQualityConsumerAuthorization:
    """Verifier-only runtime capability consumed by the G4B dispatch adapter."""

    attestation: DataQualityConsumerAuthorizationAttestation
    preflight: VerifiedDataQualityPreflight
    verified_at: datetime

    def __init__(
        self,
        *,
        attestation: DataQualityConsumerAuthorizationAttestation,
        preflight: VerifiedDataQualityPreflight,
        verified_at: datetime,
        _verification_seal: object,
    ) -> None:
        if _verification_seal is not _VERIFIED_CONSUMER_AUTHORIZATION_SEAL:
            _fail(
                "DQ_CONSUMER_AUTHORIZATION_NOT_VERIFIED",
                "capability must be created by canonical verifier",
            )
        _aware_datetime(verified_at, "verified_at")
        if verified_at >= attestation.expires_at:
            _fail("DQ_CONSUMER_AUTHORIZATION_EXPIRED", attestation.authorization_id)
        if preflight.receipt_id != attestation.receipt_id:
            _fail("DQ_RECEIPT_ID_MISMATCH", preflight.receipt_id)
        object.__setattr__(self, "attestation", attestation)
        object.__setattr__(self, "preflight", preflight)
        object.__setattr__(self, "verified_at", verified_at)

    @property
    def consumer_id(self) -> str:
        return self.attestation.consumer_id

    @property
    def authorization_id(self) -> str:
        return self.attestation.authorization_id

    @property
    def as_of(self) -> date:
        return self.attestation.as_of


def _build_verified_data_quality_consumer_authorization(
    *,
    attestation: DataQualityConsumerAuthorizationAttestation,
    preflight: VerifiedDataQualityPreflight,
    verified_at: datetime,
) -> VerifiedDataQualityConsumerAuthorization:
    return VerifiedDataQualityConsumerAuthorization(
        attestation=attestation,
        preflight=preflight,
        verified_at=verified_at,
        _verification_seal=_VERIFIED_CONSUMER_AUTHORIZATION_SEAL,
    )


def canonical_sha256(payload: object) -> str:
    return hashlib.sha256(_canonical_compact(payload)).hexdigest()


def _canonical_compact(payload: object) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _window(value: object, field: str) -> DataQualityDateWindow:
    raw = _mapping(value, field)
    _exact_fields(raw, {"start", "end"})
    return DataQualityDateWindow(
        _date_from(raw["start"], f"{field}.start"),
        _date_from(raw["end"], f"{field}.end"),
    )


def _mapping(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        _fail("DQ_CONSUMER_AUTHORIZATION_FIELDS_INVALID", f"{field} must be a mapping")
    return cast(dict[str, object], value)


def _exact_fields(payload: dict[str, object], expected: set[str]) -> None:
    if set(payload) != expected:
        _fail(
            "DQ_CONSUMER_AUTHORIZATION_FIELDS_INVALID",
            f"fields expected={sorted(expected)} actual={sorted(payload)}",
        )


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        _fail("DQ_CONSUMER_AUTHORIZATION_FIELDS_INVALID", f"{field} must be non-empty text")
    return value


def _repo_path(value: object, field: str) -> str:
    text = _text(value, field)
    candidate = PurePosixPath(text)
    if (
        "\\" in text
        or candidate.is_absolute()
        or text != candidate.as_posix()
        or any(part in {"", ".", ".."} for part in candidate.parts)
    ):
        _fail("DQ_CONSUMER_AUTHORIZATION_FIELDS_INVALID", f"{field} is not repo-relative")
    return text


def _sha256(value: object, field: str) -> str:
    text = _text(value, field)
    if len(text) != 64 or any(item not in "0123456789abcdef" for item in text):
        _fail("DQ_CONSUMER_AUTHORIZATION_FIELDS_INVALID", f"{field} is not SHA-256")
    return text


def _integer(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        _fail("DQ_CONSUMER_AUTHORIZATION_FIELDS_INVALID", f"{field} must be integer")
    return value


def _boolean(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        _fail("DQ_CONSUMER_AUTHORIZATION_FIELDS_INVALID", f"{field} must be boolean")
    return value


def _date(value: object, field: str) -> date:
    if not isinstance(value, date) or isinstance(value, datetime):
        _fail("DQ_CONSUMER_AUTHORIZATION_FIELDS_INVALID", f"{field} must be date")
    return value


def _date_from(value: object, field: str) -> date:
    text = _text(value, field)
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise DataQualityConsumerAuthorizationContractError(
            "DQ_CONSUMER_AUTHORIZATION_FIELDS_INVALID",
            f"{field} must be ISO date",
        ) from exc
    if parsed.isoformat() != text:
        _fail("DQ_CONSUMER_AUTHORIZATION_FIELDS_INVALID", f"{field} is not canonical")
    return parsed


def _aware_datetime(value: object, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        _fail(
            "DQ_CONSUMER_AUTHORIZATION_FIELDS_INVALID",
            f"{field} must be timezone-aware datetime",
        )
    return value


def _datetime(value: object, field: str) -> datetime:
    text = _text(value, field)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise DataQualityConsumerAuthorizationContractError(
            "DQ_CONSUMER_AUTHORIZATION_FIELDS_INVALID",
            f"{field} must be ISO datetime",
        ) from exc
    _aware_datetime(parsed, field)
    if parsed.isoformat() != text:
        _fail("DQ_CONSUMER_AUTHORIZATION_FIELDS_INVALID", f"{field} is not canonical")
    return parsed


def _string_tuple(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        _fail("DQ_CONSUMER_AUTHORIZATION_FIELDS_INVALID", f"{field} must be list")
    return tuple(_text(item, f"{field}[]") for item in value)


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            _fail("DQ_CONSUMER_AUTHORIZATION_FIELDS_INVALID", f"duplicate key={key}")
        result[key] = value
    return result


def _reject_nonfinite(value: str) -> NoReturn:
    if value in {"NaN", "Infinity", "-Infinity"} or not math.isfinite(float(value)):
        _fail("DQ_CONSUMER_AUTHORIZATION_FIELDS_INVALID", f"non-finite value={value}")
    _fail("DQ_CONSUMER_AUTHORIZATION_FIELDS_INVALID", f"non-finite value={value}")


def _fail(code: str, message: str) -> NoReturn:
    raise DataQualityConsumerAuthorizationContractError(code, message)


__all__ = [
    "DataQualityConsumerAuthorizationAttestation",
    "DataQualityConsumerAuthorizationContractError",
    "VerifiedDataQualityConsumerAuthorization",
    "canonical_sha256",
]
