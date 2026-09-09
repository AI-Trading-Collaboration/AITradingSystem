"""Independent named-input DQ declarations and process-local verified bytes.

DTO parsing checks declarations, not files or provenance.  Only the named
verifier may mint a runtime seal after its zero-DQ verification.  Neither a PASS
receipt nor a seal authorizes a research, provider, or trading action.
"""

from __future__ import annotations

import hashlib
import json
import os
import types
from collections.abc import Mapping
from dataclasses import dataclass, fields
from datetime import date, datetime
from typing import ClassVar, Never, Self, SupportsIndex, cast, get_args, get_origin, get_type_hints

from ai_trading_system.contracts.data_quality import DataQualityEvidence
from ai_trading_system.contracts.data_quality_attribution import DataQualityCalendarBinding
from ai_trading_system.contracts.data_quality_execution import (
    DataQualityDateWindow,
    DataQualityInvocationParameter,
    DataQualityPolicyBinding,
    DataQualityReportBinding,
    DataQualityValidatorBinding,
    _data_quality_evidence_from_payload,
    _date_value,
    _datetime_value,
    _int_value,
    _repo_relative_path,
    _require_exact_keys,
    _sha256,
    _strict_json_loads,
    _text_value,
    canonical_json_value,
)
from ai_trading_system.contracts.named_execution_context import (
    NamedExecutionContext,
    NamedExecutionIdentity,
    _absolute_root,
    _git_id,
    require_named_execution_context,
)


class NamedDataQualityExecutionContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


def _invalid(message: str) -> Never:
    raise NamedDataQualityExecutionContractError("NAMED_DQ_FIELDS_INVALID", message)


def _mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        _invalid("object with string keys required")
    return cast(Mapping[str, object], value)


_REUSED_VALUES = (
    DataQualityDateWindow,
    DataQualityPolicyBinding,
    DataQualityValidatorBinding,
    DataQualityInvocationParameter,
    DataQualityReportBinding,
    NamedExecutionIdentity,
)


def _encode(value: object) -> object:
    if isinstance(
        value, (_NamedDTO, *_REUSED_VALUES, DataQualityEvidence, DataQualityCalendarBinding)
    ):
        return value.to_dict()
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, tuple):
        return [_encode(item) for item in value]
    if value is None or type(value) in (str, int, bool):
        return value
    _invalid("unsupported DTO value")
    raise AssertionError("unreachable")


def _decode(annotation: object, value: object) -> object:
    """Bounded decoder for this module's DTOs and the explicit pure values above."""
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin is types.UnionType:
        if value is None and type(None) in args:
            return None
        choices = tuple(item for item in args if item is not type(None))
        if len(choices) == 1:
            return _decode(choices[0], value)
        _invalid("unsupported union")
    if origin is tuple:
        if not isinstance(value, list):
            _invalid("array required")
        items = cast(list[object], value)
        if len(args) == 2 and args[1] is Ellipsis:
            return tuple(_decode(args[0], item) for item in items)
        if len(items) != len(args):
            _invalid("fixed tuple length mismatch")
        return tuple(_decode(kind, item) for kind, item in zip(args, items, strict=True))
    if annotation is datetime:
        return _datetime_value(value, "datetime")
    if annotation is date:
        return _date_value(value, "date")
    if annotation in (str, int, bool):
        if type(value) is not annotation:
            _invalid("scalar type mismatch")
        return value
    if isinstance(annotation, type) and issubclass(annotation, _NamedDTO):
        return annotation.from_dict(_mapping(value))
    for pure_type in _REUSED_VALUES:
        if annotation is pure_type:
            return pure_type.from_dict(_mapping(value))
    if annotation is DataQualityEvidence:
        return _data_quality_evidence_from_payload(_mapping(value))
    if annotation is DataQualityCalendarBinding:
        payload = _mapping(value)
        _require_exact_keys(
            payload, frozenset(DataQualityCalendarBinding.__dataclass_fields__), "calendar"
        )
        return DataQualityCalendarBinding(
            **{key: _text_value(item, key) for key, item in payload.items()}
        )
    _invalid("unsupported DTO annotation")
    raise AssertionError("unreachable")


@dataclass(frozen=True)
class _NamedDTO:
    schema_version: ClassVar[str | None] = None

    def to_dict(self) -> dict[str, object]:
        payload = {item.name: _encode(getattr(self, item.name)) for item in fields(self)}
        if self.schema_version is not None:
            payload = {"schema_version": self.schema_version, **payload}
        return payload

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> Self:
        names = frozenset(item.name for item in fields(cls))
        expected = names | ({"schema_version"} if cls.schema_version else set())
        _require_exact_keys(payload, frozenset(expected), cls.__name__)
        if cls.schema_version and payload["schema_version"] != cls.schema_version:
            _invalid("unsupported named schema")
        annotations = get_type_hints(cls)
        return cls(**{name: _decode(annotations[name], payload[name]) for name in names})

    def _check_types(self) -> None:
        # Round-trip each field without coercing constructor input; in particular
        # dates must already be dates and immutable collections must be tuples.
        annotations = get_type_hints(type(self))
        for item in fields(self):
            value = getattr(self, item.name)
            decoded = _decode(annotations[item.name], _encode(value))
            if type(value) is not type(decoded) or value != decoded:
                _invalid(f"incorrect typed field: {item.name}")


@dataclass(frozen=True)
class NamedDQRoots(_NamedDTO):
    source_root: str
    publication_root: str
    execution_root: str
    evidence_root: str

    def __post_init__(self) -> None:
        self._check_types()
        for item in fields(self):
            _absolute_root(getattr(self, item.name), item.name)


@dataclass(frozen=True)
class NamedArtifactBinding(_NamedDTO):
    root_role: str
    relative_path: str
    sha256: str
    size_bytes: int

    def __post_init__(self) -> None:
        self._check_types()
        if self.root_role not in {"SOURCE", "PUBLICATION", "EXECUTION", "EVIDENCE"}:
            _invalid("unknown root role")
        _repo_relative_path(self.relative_path, "artifact.relative_path")
        _sha256(self.sha256, "artifact.sha256")
        _int_value(self.size_bytes, "artifact.size_bytes")


# Fixed protocol/profile identities, not tunable investment thresholds. See
# TRADING-2564_S2c_Equal_Risk_Price_Consumer_Scope_V1.md §3. The manifest contains
# paths, not source hashes: pinning its reviewed bytes introduces no hash cycle.
EQUAL_RISK_PRICE_CONSUMER_ID = "simple_baseline_forward_aging_preview@1.0.0"
EQUAL_RISK_PRICE_SOURCE_MANIFEST_PATH = (
    "config/data_governance/named_equal_risk_price_consumer_sources_v1.json"
)
EQUAL_RISK_PRICE_SOURCE_MANIFEST_SHA256 = (
    "b3dd9c781237fc71234f793832ff5d426d0244db792b177111569459f460e822"
)
EQUAL_RISK_PRICE_REGISTRY_PATH = "config/research/simple_baseline_strategy_registry.yaml"
EQUAL_RISK_PRICE_TICKERS = ("QQQ", "SGOV", "TQQQ")
EQUAL_RISK_GUARD_RATE_SERIES = ("DGS2", "DGS10", "DTWEXBGS")
# Existing AGENTS.md primary-window policy; never inferred from retained runs.
EQUAL_RISK_PRIMARY_START = date(2021, 2, 22)

# S2c.2 adds a distinct consumer closure. Never add this pin to the S2c.1
# accessor: a captured-input request is not permission to import new consumers.
# See TRADING-2564_S2c2_Five_Candidate_Read_Only_Preview_V1.md §3.
FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_PATH = (
    "config/data_governance/named_simple_baseline_preview_sources_v1.json"
)
FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_SHA256 = (
    "8d338125acfbecb4ad6c863c9d21e87a9e2c541c9eb200cef94b4b98d2dda45e"
)

# S3b is a distinct execution/recording scope, never another accepted pin of
# the legacy 57/59 accessors or result DTO. The fixed manifest lists paths only.
# See TRADING-2564_S3b_Prospective_Capture_Execution_V1.md §3.
PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_PATH = (
    "config/data_governance/named_prospective_five_candidate_sources_v1.json"
)
PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_SHA256 = (
    "9a11ed94e1c318aee3c44a7d01ba0f31556bd4de355eef1f75893245fe8bc1ce"
)

# TRADING-2560 current-known Composer is a distinct, reviewed consumer. These
# protocol identities never widen any old price-only or recording-only accessor.
COMPOSER_PROSPECTIVE_SOURCE_MANIFEST_PATH = (
    "config/data_governance/named_composer_prospective_sources_v1.json"
)
COMPOSER_PROSPECTIVE_SOURCE_MANIFEST_SHA256 = (
    "2613012b0774aaf78b448ccf63ee469099dd796cea28de8060b0ad9cd3bc3d2a"
)
COMPOSER_INPUT_POLICY_PATH = "config/research/first_layer_composer_v2_known_snapshot_input_v1.yaml"
COMPOSER_INPUT_POLICY_SHA256 = "a882644608705b6b1fa2f9d907f780bf485762e3725a9c782f4a2859be7eeb09"
COMPOSER_SCOPE_ORDER = ("training", "exact_cash", "primary")
COMPOSER_SCOPE_SPECS = {
    "training": (date(2018, 1, 2), ("QQQ", "TQQQ", "SHY"), True),
    "exact_cash": (date(2020, 5, 28), ("SGOV",), False),
    "primary": (date(2021, 2, 22), ("QQQ", "SGOV", "TQQQ"), True),
}

_PROSPECTIVE_RECORDING_ROLES = frozenset(
    {
        "publication_pointer",
        "publication_transaction",
        "snapshot_manifest",
        "source_event",
        "commit_anchor",
        "source_manifest",
        "dq_report",
        "dq_receipt",
        "dq_successful_dispatch",
        "dq_parent",
        "dq_parent_request",
        "dq_child_stdout",
        "dq_child_stderr",
        "dq_pre_guard",
        "dq_post_guard",
    }
)


@dataclass(frozen=True)
class NamedEqualRiskPriceScope(_NamedDTO):
    """Fixed price-range request only; no strategy/PIT/readiness authorization."""

    schema_version: ClassVar[str] = "named_equal_risk_price_scope.v1"
    as_of: date
    requested_window: DataQualityDateWindow
    registry_binding: NamedArtifactBinding
    consumer_id: str = EQUAL_RISK_PRICE_CONSUMER_ID
    expected_price_tickers: tuple[str, ...] = EQUAL_RISK_PRICE_TICKERS
    guard_rate_series: tuple[str, ...] = EQUAL_RISK_GUARD_RATE_SERIES

    def __post_init__(self) -> None:
        self._check_types()
        if (
            self.consumer_id != EQUAL_RISK_PRICE_CONSUMER_ID
            or self.expected_price_tickers != EQUAL_RISK_PRICE_TICKERS
            or self.guard_rate_series != EQUAL_RISK_GUARD_RATE_SERIES
        ):
            _invalid("equal-risk price consumer and member sets are fixed")
        if (
            self.requested_window.start != EQUAL_RISK_PRIMARY_START
            or self.requested_window.end != self.as_of
        ):
            _invalid("equal-risk prices require the full primary window through the same as-of")
        if (
            self.registry_binding.root_role != "EXECUTION"
            or self.registry_binding.relative_path != EQUAL_RISK_PRICE_REGISTRY_PATH
            or self.registry_binding.size_bytes <= 0
        ):
            _invalid("equal-risk registry requires the fixed full EXECUTION binding")


def _unique(values: tuple[str, ...], label: str, *, nonempty: bool = True) -> None:
    if (nonempty and not values) or len(values) != len(set(values)):
        _invalid(f"{label}: empty/duplicate values")
    for value in values:
        _text_value(value, label)


@dataclass(frozen=True)
class NamedDQScope(_NamedDTO):
    as_of: date
    requested_window: DataQualityDateWindow
    expected_price_tickers: tuple[str, ...]
    expected_rate_series: tuple[str, ...]
    input_roles: tuple[str, ...]
    require_secondary_prices: bool

    def __post_init__(self) -> None:
        self._check_types()
        _unique(self.expected_price_tickers, "price tickers")
        _unique(self.expected_rate_series, "rate series")
        _unique(self.input_roles, "input roles")
        if not {"prices", "rates"}.issubset(self.input_roles) or not set(self.input_roles).issubset(
            {"prices", "rates", "secondary_prices"}
        ):
            _invalid("named v1 requires prices/rates and supports only optional secondary_prices")
        if self.require_secondary_prices and "secondary_prices" not in self.input_roles:
            _invalid("required secondary role missing")
        if self.requested_window.end > self.as_of:
            _invalid("requested window exceeds as-of")

    def covers(self, required: NamedDQScope) -> bool:
        return (
            self.as_of == required.as_of
            and self.requested_window.contains(required.requested_window)
            and set(required.expected_price_tickers).issubset(self.expected_price_tickers)
            and set(required.expected_rate_series).issubset(self.expected_rate_series)
            and set(required.input_roles).issubset(self.input_roles)
            and (not required.require_secondary_prices or self.require_secondary_prices)
        )


@dataclass(frozen=True)
class NamedComposerInputScope(_NamedDTO):
    """One of three fixed DQ roles; only a verified aggregate grants model input."""

    schema_version: ClassVar[str] = "named_composer_input_scope.v1"
    as_of: date
    segment: str
    input_policy_binding: NamedArtifactBinding

    def __post_init__(self) -> None:
        self._check_types()
        if self.segment not in COMPOSER_SCOPE_SPECS:
            _invalid("Composer requires an explicit fixed segment")
        if self.as_of < date(2025, 12, 3):
            _invalid("Composer current-session role must follow its historical cutoff")
        if (
            self.input_policy_binding.root_role != "EXECUTION"
            or self.input_policy_binding.relative_path != COMPOSER_INPUT_POLICY_PATH
            or self.input_policy_binding.sha256 != COMPOSER_INPUT_POLICY_SHA256
            or self.input_policy_binding.size_bytes <= 0
        ):
            _invalid("Composer requires the reviewed complete input policy binding")

    @property
    def dq_scope(self) -> NamedDQScope:
        return composer_dq_scope(as_of=self.as_of, segment=self.segment)


def composer_dq_scope(*, as_of: date, segment: str) -> NamedDQScope:
    """Pure fixed declaration, not a verified input or policy binding."""
    if type(as_of) is not date or segment not in COMPOSER_SCOPE_SPECS:
        _invalid("fixed Composer segment and exact date required")
    start, tickers, secondary = COMPOSER_SCOPE_SPECS[segment]
    return NamedDQScope(
        as_of=as_of,
        requested_window=DataQualityDateWindow(start, as_of),
        expected_price_tickers=tickers,
        expected_rate_series=EQUAL_RISK_GUARD_RATE_SERIES,
        input_roles=("prices", "rates", "secondary_prices") if secondary else ("prices", "rates"),
        require_secondary_prices=secondary,
    )


@dataclass(frozen=True)
class NamedSnapshotSelector(_NamedDTO):
    pointer_id: str
    pointer_sha256: str
    transaction_id: str
    transaction_sha256: str
    dataset_id: str = "download_composite"

    def __post_init__(self) -> None:
        self._check_types()
        for value in (self.pointer_id, self.transaction_id):
            _text_value(value, "selector id")
        for value in (self.pointer_sha256, self.transaction_sha256):
            _sha256(value, "selector sha256")
        if self.dataset_id != "download_composite":
            _invalid("unsupported named dataset")


@dataclass(frozen=True)
class NamedDQExecutionRequest(_NamedDTO):
    schema_version: ClassVar[str] = "named_data_quality_execution_request.v1"
    roots: NamedDQRoots
    selector: NamedSnapshotSelector
    scope: NamedDQScope
    source_output_relative_path: str
    policy_path: str
    execution_profile_id: str
    candidate_commit: str
    source_manifest_path: str
    source_manifest_sha256: str
    expected_evaluated_window: DataQualityDateWindow | None = None

    def __post_init__(self) -> None:
        self._check_types()
        for path in (self.source_output_relative_path, self.policy_path, self.source_manifest_path):
            _repo_relative_path(path, "request path")
        _git_id(self.candidate_commit, "request.candidate_commit")
        _sha256(self.source_manifest_sha256, "request.source_manifest_sha256")
        if self.execution_profile_id != "manual.v1":
            _invalid("named v1 supports only explicit manual.v1")
        if self.expected_evaluated_window is not None and not self.scope.requested_window.contains(
            self.expected_evaluated_window
        ):
            _invalid("expected evaluated window outside request")

    @property
    def request_id(self) -> str:
        return (
            "named_dq_request_"
            + hashlib.sha256(canonical_json_value(self.to_dict()).encode()).hexdigest()
        )

    @property
    def canonical_bytes(self) -> bytes:
        return (
            json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True, indent=2) + "\n"
        ).encode("utf-8")

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @classmethod
    def from_json_bytes(cls, content: bytes) -> NamedDQExecutionRequest:
        if type(content) is not bytes:
            _invalid("request requires immutable bytes")
        return cls.from_dict(_mapping(_strict_json_loads(content.decode("utf-8"))))


@dataclass(frozen=True)
class NamedPublicationBinding(_NamedDTO):
    dataset_id: str
    snapshot_id: str
    generation: int
    pointer_id: str
    pointer: NamedArtifactBinding
    transaction_id: str
    transaction: NamedArtifactBinding
    snapshot_manifest_id: str
    snapshot_manifest: NamedArtifactBinding
    source_event_id: str
    source_event: NamedArtifactBinding
    transaction_window: DataQualityDateWindow
    anchor_dataset_id: str
    anchor_pointer_id: str
    anchor_generation: int
    anchor_pointer: NamedArtifactBinding
    legacy_projection_status: str = "NOT_EVALUATED"

    def __post_init__(self) -> None:
        self._check_types()
        for value in (
            self.snapshot_id,
            self.pointer_id,
            self.transaction_id,
            self.snapshot_manifest_id,
            self.source_event_id,
        ):
            _text_value(value, "publication id")
        _text_value(self.anchor_pointer_id, "anchor_pointer_id")
        for artifact in (
            self.pointer,
            self.transaction,
            self.snapshot_manifest,
            self.source_event,
            self.anchor_pointer,
        ):
            if artifact.root_role != "PUBLICATION":
                _invalid("publication artifact must use publication root")
        _int_value(self.generation, "generation")
        _int_value(self.anchor_generation, "anchor_generation")
        if (
            self.dataset_id != "download_composite"
            or self.anchor_dataset_id != self.dataset_id
            or self.generation < 1
            or self.anchor_generation < self.generation
            or (
                self.anchor_generation == self.generation
                and (
                    self.anchor_pointer_id,
                    self.anchor_pointer.sha256,
                    self.anchor_pointer.size_bytes,
                )
                != (self.pointer_id, self.pointer.sha256, self.pointer.size_bytes)
            )
            or (
                self.anchor_generation > self.generation
                and (
                    self.anchor_pointer_id == self.pointer_id
                    or self.anchor_pointer.sha256 == self.pointer.sha256
                )
            )
            or self.legacy_projection_status != "NOT_EVALUATED"
        ):
            _invalid("publication/anchor identity conflict")


@dataclass(frozen=True)
class NamedManifestRowBinding(_NamedDTO):
    source_ordinal: int
    fields: tuple[tuple[str, str], ...]
    row_sha256: str
    record_ref: str

    def __post_init__(self) -> None:
        self._check_types()
        _int_value(self.source_ordinal, "manifest row ordinal")
        _unique(tuple(key for key, _ in self.fields), "manifest field names")
        _sha256(self.row_sha256, "manifest row sha256")
        actual = hashlib.sha256(canonical_json_value(dict(self.fields)).encode("utf-8")).hexdigest()
        if self.row_sha256 != actual or self.record_ref != "manifest_record_" + actual:
            _invalid("original raw manifest row hash/ref mismatch")


@dataclass(frozen=True)
class NamedManifestBinding(_NamedDTO):
    member: NamedArtifactBinding
    columns: tuple[str, ...]
    rows: tuple[NamedManifestRowBinding, ...]

    def __post_init__(self) -> None:
        self._check_types()
        _unique(self.columns, "manifest columns")
        if self.member.root_role != "PUBLICATION" or not self.rows:
            _invalid("nonempty immutable publication manifest required")
        if tuple(row.source_ordinal for row in self.rows) != tuple(range(len(self.rows))):
            _invalid("manifest ordinals must preserve complete original row order")
        if len({row.row_sha256 for row in self.rows}) != len(self.rows):
            _invalid("ambiguous duplicate manifest rows")
        if any(tuple(key for key, _ in row.fields) != self.columns for row in self.rows):
            _invalid("manifest complete column order mismatch")


_INPUT_SCHEMA_ROLES = {
    "prices": ("prices_daily.v1", "primary_market_prices"),
    "rates": ("rates_daily.v1", "primary_macro_rates"),
    "secondary_prices": ("prices_daily.v1", "secondary_market_prices"),
}


@dataclass(frozen=True)
class NamedDQInputBinding(_NamedDTO):
    role: str
    schema_id: str
    source_role: str
    member: NamedArtifactBinding
    row_count: int
    observed_min_date: date | None
    observed_max_date: date | None
    manifest_row_ordinal: int
    manifest_row_sha256: str
    original_output_path: str
    source_relative_path: str

    def __post_init__(self) -> None:
        self._check_types()
        if _INPUT_SCHEMA_ROLES.get(self.role) != (self.schema_id, self.source_role):
            _invalid("role/schema/source-role mismatch")
        if self.member.root_role != "PUBLICATION":
            _invalid("input member must use publication root")
        _int_value(self.row_count, "input.row_count")
        _int_value(self.manifest_row_ordinal, "input.manifest_row_ordinal")
        _sha256(self.manifest_row_sha256, "input.manifest_row_sha256")
        # Preserve the original raw string; normalization belongs to the explicit
        # source-root matcher, never to this stored manifest cell.
        if not self.original_output_path:
            _invalid("missing original output path")
        _repo_relative_path(self.source_relative_path, "input.source_relative_path")
        if (self.observed_min_date is None) != (self.observed_max_date is None):
            _invalid("incomplete observed dates")
        if (
            self.observed_min_date is not None
            and self.observed_max_date is not None
            and self.observed_min_date > self.observed_max_date
        ):
            _invalid("reversed observed dates")


@dataclass(frozen=True)
class NamedExecutionObservation(_NamedDTO):
    execution_pid: int
    source_lease_id: str
    import_completed_at: datetime
    pre_dq_checked_at: datetime
    terminal_checked_at: datetime

    def __post_init__(self) -> None:
        self._check_types()
        if _int_value(self.execution_pid, "execution_pid") == 0:
            _invalid("execution PID must be positive")
        _text_value(self.source_lease_id, "source_lease_id")
        if not self.import_completed_at <= self.pre_dq_checked_at <= self.terminal_checked_at:
            _invalid("execution observation chronology mismatch")


@dataclass(frozen=True)
class NamedDQExecutionReceipt(_NamedDTO):
    schema_version: ClassVar[str] = "named_data_quality_execution_receipt.v1"
    run_id: str
    contract_id: str
    request: NamedDQExecutionRequest
    started_at: datetime
    checked_at: datetime
    ended_at: datetime
    evaluated_window: DataQualityDateWindow
    publication: NamedPublicationBinding
    manifest: NamedManifestBinding
    inputs: tuple[NamedDQInputBinding, ...]
    policy: DataQualityPolicyBinding
    validator: DataQualityValidatorBinding
    execution: NamedExecutionIdentity
    execution_observation: NamedExecutionObservation
    execution_dependencies: tuple[NamedArtifactBinding, ...]
    calendar: DataQualityCalendarBinding
    calendar_policy: NamedArtifactBinding
    invocation: tuple[DataQualityInvocationParameter, ...]
    report: DataQualityReportBinding
    data_quality_evidence: DataQualityEvidence
    price_consistency_start_date: date
    rate_consistency_start_date: date
    coverage_semantics: str = "CANONICAL_DQ_RULES_ONLY"
    cutover_allowed: bool = False
    dispatch_allowed: bool = False
    production_effect: str = "none"
    broker_action: str = "none"

    def __post_init__(self) -> None:
        self._check_types()
        _text_value(self.run_id, "run_id")
        _text_value(self.contract_id, "contract_id")
        if not self.started_at <= self.checked_at <= self.ended_at:
            _invalid("receipt chronology mismatch")
        if self.checked_at.date() < self.request.scope.as_of:
            _invalid("DQ checked before as-of")
        if not self.request.scope.requested_window.contains(self.evaluated_window):
            _invalid("evaluated window outside requested window")
        if (
            self.request.expected_evaluated_window is not None
            and self.request.expected_evaluated_window != self.evaluated_window
        ):
            _invalid("expected evaluated window mismatch")
        selection = self.request.selector
        publication = self.publication
        if (
            publication.dataset_id != selection.dataset_id
            or publication.pointer_id != selection.pointer_id
            or publication.pointer.sha256 != selection.pointer_sha256
            or publication.transaction_id != selection.transaction_id
            or publication.transaction.sha256 != selection.transaction_sha256
            or not publication.transaction_window.contains(self.request.scope.requested_window)
        ):
            _invalid("exact selector/publication binding mismatch")
        if (
            self.execution.execution_root != self.request.roots.execution_root
            or self.execution.candidate_commit != self.request.candidate_commit
            or self.execution.source_manifest_path != self.request.source_manifest_path
            or self.execution.source_manifest_sha256 != self.request.source_manifest_sha256
            or self.policy.path != self.request.policy_path
        ):
            _invalid("request/executed code or policy identity mismatch")
        observations = self.execution_observation
        if (
            not observations.import_completed_at
            <= observations.pre_dq_checked_at
            <= self.started_at
            <= self.ended_at
            <= observations.terminal_checked_at
        ):
            _invalid("DQ is not enclosed by source checks")
        sources = {item.source_path: item.sha256 for item in self.execution.modules}
        if any(
            sources.get(item.path) != item.sha256 for item in self.validator.implementation_sources
        ):
            _invalid("validator source missing from actually compiled set")
        _unique(
            tuple(item.relative_path for item in self.execution_dependencies),
            "execution dependencies",
        )
        if any(item.root_role != "EXECUTION" for item in self.execution_dependencies):
            _invalid("runtime dependency must use execution root")
        deps = {(item.relative_path, item.sha256) for item in self.execution_dependencies}
        if (
            self.policy.path,
            self.policy.sha256,
        ) not in deps or self.calendar_policy not in self.execution_dependencies:
            _invalid("actual policy/calendar dependencies missing")
        if (
            self.calendar_policy.root_role != "EXECUTION"
            or self.calendar_policy.sha256 != self.calendar.special_closure_policy_sha256
        ):
            _invalid("calendar object/policy bytes mismatch")
        _unique(tuple(item.role for item in self.inputs), "input roles")
        _unique(tuple(item.member.relative_path for item in self.inputs), "input member paths")
        _unique(
            tuple(str(item.manifest_row_ordinal) for item in self.inputs),
            "matched manifest ordinals",
        )
        if set(item.role for item in self.inputs) != set(self.request.scope.input_roles):
            _invalid("receipt input role set differs from requested scope")
        for item in self.inputs:
            if item.manifest_row_ordinal >= len(self.manifest.rows):
                _invalid("missing original manifest row")
            row = self.manifest.rows[item.manifest_row_ordinal]
            if (
                row.row_sha256 != item.manifest_row_sha256
                or dict(row.fields).get("output_path") != item.original_output_path
            ):
                _invalid("input/original full manifest row mismatch")
            if not item.source_relative_path.startswith(
                self.request.source_output_relative_path + "/"
            ):
                _invalid("input outside declared source output directory")
        _unique(tuple(item.name for item in self.invocation), "invocation names")
        evidence = self.data_quality_evidence
        if (
            evidence.contract_id != self.contract_id
            or evidence.policy_id != self.policy.policy_id
            or evidence.policy_version != self.policy.policy_version
            or evidence.checked_at != self.checked_at
            or evidence.as_of != self.request.scope.as_of
            or evidence.status != self.report.status
            or evidence.passed != (self.report.status != "FAIL")
            or evidence.error_count != self.report.error_count
            or evidence.warning_count != self.report.warning_count
            or evidence.checked_input_count != len(self.inputs)
            or evidence.report_path != self.report.path
            or evidence.report_sha256 != self.report.sha256
            or evidence.blocking_issues != self.report.blocking_issue_codes
        ):
            _invalid("canonical evidence/report/input count conflict")
        if (
            self.coverage_semantics != "CANONICAL_DQ_RULES_ONLY"
            or self.cutover_allowed
            or self.dispatch_allowed
            or self.production_effect != "none"
            or self.broker_action != "none"
        ):
            _invalid("named DQ cannot authorize downstream actions")

    def _semantic_payload(self) -> dict[str, object]:
        return super().to_dict()

    @property
    def receipt_id(self) -> str:
        return (
            "named_dq_receipt_"
            + hashlib.sha256(
                canonical_json_value(self._semantic_payload()).encode("utf-8")
            ).hexdigest()
        )

    def to_dict(self) -> dict[str, object]:
        return {"receipt_id": self.receipt_id, **self._semantic_payload()}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> Self:
        expected = frozenset(item.name for item in fields(cls)) | {"schema_version", "receipt_id"}
        _require_exact_keys(payload, expected, "named receipt")
        result = super().from_dict(
            {key: value for key, value in payload.items() if key != "receipt_id"}
        )
        if payload["receipt_id"] != result.receipt_id:
            _invalid("named receipt content ID mismatch")
        return result

    @property
    def canonical_bytes(self) -> bytes:
        return (
            json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True, indent=2) + "\n"
        ).encode("utf-8")

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @classmethod
    def from_json_bytes(cls, content: bytes) -> NamedDQExecutionReceipt:
        if type(content) is not bytes:
            _invalid("receipt requires immutable bytes")
        receipt = cls.from_dict(_mapping(_strict_json_loads(content.decode("utf-8"))))
        if content != receipt.canonical_bytes:
            _invalid("named receipt bytes are not canonical")
        return receipt


@dataclass(frozen=True)
class NamedDQSuccessfulDispatchBinding(_NamedDTO):
    """Trusted coordinator's terminal-run association, never a signature/lease.

    The coordinator writes its parent receipt only after child exit 0 and the
    existing postguard PASS, then writes this association. The parent receipt
    must not contain this object's hash: the evidence graph is deliberately
    one-way. A verifier reads both artifacts using the execution root and their
    explicit hashes before this pure declaration can participate in sealing.
    """

    schema_version: ClassVar[str] = "named_data_quality_successful_dispatch.v1"
    receipt: NamedArtifactBinding
    receipt_id: str
    request_id: str
    request_sha256: str
    execution_identity_sha256: str
    candidate_commit: str
    execution_root: str
    execution_pid: int
    source_lease_id: str
    child_started_at: datetime
    child_terminal_checked_at: datetime
    parent_postchecked_at: datetime
    parent_receipt: NamedArtifactBinding
    child_exit_code: int = 0
    postguard_status: str = "PASS"
    proof_semantics: str = "TRUSTED_COORDINATOR_CORRELATION_ONLY"
    dispatch_allowed: bool = False
    production_effect: str = "none"
    broker_action: str = "none"

    def __post_init__(self) -> None:
        self._check_types()
        if self.receipt.root_role != "EVIDENCE" or self.parent_receipt.root_role != "EXECUTION":
            _invalid("successful dispatch requires EVIDENCE receipt and EXECUTION parent")
        for value in (self.receipt_id, self.request_id, self.source_lease_id):
            _text_value(value, "successful dispatch identity")
        _sha256(self.request_sha256, "successful dispatch request_sha256")
        _sha256(self.execution_identity_sha256, "successful dispatch execution_identity_sha256")
        _git_id(self.candidate_commit, "successful dispatch candidate_commit")
        _absolute_root(self.execution_root, "successful dispatch execution_root")
        if _int_value(self.execution_pid, "successful dispatch execution_pid") == 0:
            _invalid("successful dispatch requires positive original child PID")
        if (
            not self.child_started_at
            <= self.child_terminal_checked_at
            <= self.parent_postchecked_at
        ):
            _invalid("successful dispatch terminal chronology mismatch")
        if (
            self.child_exit_code != 0
            or self.postguard_status != "PASS"
            or self.proof_semantics != "TRUSTED_COORDINATOR_CORRELATION_ONLY"
            or self.dispatch_allowed
            or self.production_effect != "none"
            or self.broker_action != "none"
        ):
            _invalid("successful dispatch cannot represent failed exit/postguard or authority")

    def assert_matches_receipt(
        self, receipt: NamedDQExecutionReceipt, *, receipt_path: str
    ) -> None:
        """Check pure associations; no file, lease or coordinator authentication."""
        if not isinstance(receipt, NamedDQExecutionReceipt):
            _invalid("successful dispatch requires typed named receipt")
        _repo_relative_path(receipt_path, "successful dispatch receipt_path")
        observation = receipt.execution_observation
        if (
            self.receipt.relative_path != receipt_path
            or self.receipt.sha256 != receipt.canonical_sha256
            or self.receipt.size_bytes != len(receipt.canonical_bytes)
            or self.receipt_id != receipt.receipt_id
            or self.request_id != receipt.request.request_id
            or self.request_sha256 != receipt.request.canonical_sha256
            or self.execution_identity_sha256 != receipt.execution.stable_identity_sha256
            or self.candidate_commit != receipt.execution.candidate_commit
            or self.execution_root != receipt.execution.execution_root
            or self.execution_pid != observation.execution_pid
            or self.source_lease_id != observation.source_lease_id
        ):
            _invalid("successful dispatch does not match original receipt/request/execution")
        if not (
            self.child_started_at
            <= receipt.started_at
            <= receipt.ended_at
            <= observation.terminal_checked_at
            <= self.child_terminal_checked_at
            <= self.parent_postchecked_at
        ):
            _invalid("successful dispatch does not enclose original DQ terminal checks")

    def _semantic_payload(self) -> dict[str, object]:
        return super().to_dict()

    @property
    def dispatch_binding_id(self) -> str:
        material = canonical_json_value(self._semantic_payload()).encode("utf-8")
        return "named_dq_dispatch_" + hashlib.sha256(material).hexdigest()

    def to_dict(self) -> dict[str, object]:
        return {"dispatch_binding_id": self.dispatch_binding_id, **self._semantic_payload()}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> Self:
        expected = frozenset(item.name for item in fields(cls)) | {
            "schema_version",
            "dispatch_binding_id",
        }
        _require_exact_keys(payload, expected, "successful named dispatch")
        result = super().from_dict(
            {key: value for key, value in payload.items() if key != "dispatch_binding_id"}
        )
        if payload["dispatch_binding_id"] != result.dispatch_binding_id:
            _invalid("successful dispatch content ID mismatch")
        return result

    @property
    def canonical_bytes(self) -> bytes:
        return (
            json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True, indent=2) + "\n"
        ).encode("utf-8")

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @classmethod
    def from_json_bytes(cls, content: bytes) -> NamedDQSuccessfulDispatchBinding:
        if type(content) is not bytes:
            _invalid("successful dispatch requires immutable bytes")
        result = cls.from_dict(_mapping(_strict_json_loads(content.decode("utf-8"))))
        if content != result.canonical_bytes:
            _invalid("successful dispatch bytes are not canonical")
        return result


_VERIFIED_NAMED_SEAL = object()


@dataclass(frozen=True, init=False, eq=False)
class VerifiedNamedInputs:
    _receipt: NamedDQExecutionReceipt
    _successful_dispatch: NamedDQSuccessfulDispatchBinding
    _context: NamedExecutionContext
    _verifier_pid: int
    _captured: tuple[tuple[str, bytes], ...]
    _captured_dependencies: tuple[tuple[str, bytes], ...]
    _preview_sessions: tuple[date, ...]
    _preview_next_session: date | None
    _captured_recording_artifacts: tuple[tuple[str, NamedArtifactBinding, bytes], ...]

    def __init__(
        self,
        receipt: NamedDQExecutionReceipt,
        captured: tuple[tuple[str, bytes], ...],
        *,
        receipt_path: str,
        successful_dispatch: NamedDQSuccessfulDispatchBinding,
        _seal: object,
        captured_dependencies: tuple[tuple[str, bytes], ...] = (),
        preview_sessions: tuple[date, ...] = (),
        preview_next_session: date | None = None,
        captured_recording_artifacts: tuple[tuple[str, NamedArtifactBinding, bytes], ...] = (),
    ) -> None:
        if _seal is not _VERIFIED_NAMED_SEAL:
            _invalid("verified named inputs require verifier seal")
        if not isinstance(successful_dispatch, NamedDQSuccessfulDispatchBinding):
            _invalid("verified named inputs require successful dispatch binding")
        successful_dispatch.assert_matches_receipt(receipt, receipt_path=receipt_path)
        context = require_named_execution_context()
        if receipt.report.status != "PASS" or not receipt.data_quality_evidence.ready:
            _invalid("verified named inputs require strict PASS")
        if context.identity != receipt.execution:
            _invalid("current verifier code identity differs from original DQ identity")
        if type(captured) is not tuple or any(
            type(pair) is not tuple
            or len(pair) != 2
            or type(pair[0]) is not str
            or type(pair[1]) is not bytes
            for pair in captured
        ):
            _invalid("captured inputs must be immutable role/bytes tuples")
        if len(captured) != len({role for role, _ in captured}) or set(
            role for role, _ in captured
        ) != set(item.role for item in receipt.inputs):
            _invalid("captured input role mismatch")
        data = dict(captured)
        for item in receipt.inputs:
            content = data[item.role]
            if (
                len(content) != item.member.size_bytes
                or hashlib.sha256(content).hexdigest() != item.member.sha256
            ):
                _invalid("captured immutable member bytes mismatch")
        if type(captured_dependencies) is not tuple or any(
            type(pair) is not tuple
            or len(pair) != 2
            or type(pair[0]) is not str
            or type(pair[1]) is not bytes
            for pair in captured_dependencies
        ):
            _invalid("captured dependencies must be immutable path/bytes tuples")
        if captured_dependencies:
            dependencies = dict(captured_dependencies)
            if len(dependencies) != len(captured_dependencies) or set(dependencies) != {
                item.relative_path for item in receipt.execution_dependencies
            }:
                _invalid("captured execution dependency set mismatch")
            for dependency in receipt.execution_dependencies:
                content = dependencies[dependency.relative_path]
                if (
                    dependency.root_role != "EXECUTION"
                    or len(content) != dependency.size_bytes
                    or hashlib.sha256(content).hexdigest() != dependency.sha256
                ):
                    _invalid("captured execution dependency bytes mismatch")
        if type(preview_sessions) is not tuple or any(
            type(session) is not date for session in preview_sessions
        ):
            _invalid("preview calendar requires immutable date tuple")
        if preview_sessions or preview_next_session is not None:
            if (
                not captured_dependencies
                or (
                    receipt.execution.source_manifest_path,
                    receipt.execution.source_manifest_sha256,
                )
                not in {
                    (
                        FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_PATH,
                        FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_SHA256,
                    ),
                    (
                        PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_PATH,
                        PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_SHA256,
                    ),
                    (
                        COMPOSER_PROSPECTIVE_SOURCE_MANIFEST_PATH,
                        COMPOSER_PROSPECTIVE_SOURCE_MANIFEST_SHA256,
                    ),
                }
                or not preview_sessions
                or tuple(sorted(set(preview_sessions))) != preview_sessions
                or preview_sessions[0] < receipt.request.scope.requested_window.start
                or preview_sessions[-1] > receipt.request.scope.as_of
                or type(preview_next_session) is not date
                or preview_next_session <= receipt.request.scope.as_of
            ):
                _invalid("preview calendar witness does not bind the full verified request")
        prospective = (
            receipt.execution.source_manifest_path,
            receipt.execution.source_manifest_sha256,
        ) in {
            (
                PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_PATH,
                PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_SHA256,
            ),
            (
                COMPOSER_PROSPECTIVE_SOURCE_MANIFEST_PATH,
                COMPOSER_PROSPECTIVE_SOURCE_MANIFEST_SHA256,
            ),
        }
        if type(captured_recording_artifacts) is not tuple or any(
            type(row) is not tuple
            or len(row) != 3
            or type(row[0]) is not str
            or type(row[1]) is not NamedArtifactBinding
            or type(row[2]) is not bytes
            for row in captured_recording_artifacts
        ):
            _invalid("prospective recording closure requires immutable typed artifacts")
        if prospective:
            roles = [row[0] for row in captured_recording_artifacts]
            if len(set(roles)) != len(roles) or set(roles) != _PROSPECTIVE_RECORDING_ROLES:
                _invalid("prospective recording metadata closure must be complete")
            if not captured_dependencies or not preview_sessions:
                _invalid("prospective recording requires captured dependencies and calendar")
            for role, binding, content in captured_recording_artifacts:
                if (
                    len(content) != binding.size_bytes
                    or hashlib.sha256(content).hexdigest() != binding.sha256
                ):
                    _invalid("prospective recording artifact bytes mismatch: " + role)
        elif captured_recording_artifacts:
            _invalid("legacy profile cannot mint prospective recording metadata")
        object.__setattr__(self, "_receipt", receipt)
        object.__setattr__(self, "_successful_dispatch", successful_dispatch)
        object.__setattr__(self, "_context", context)
        object.__setattr__(self, "_verifier_pid", os.getpid())
        object.__setattr__(self, "_captured", tuple(sorted(captured)))
        object.__setattr__(self, "_captured_dependencies", tuple(sorted(captured_dependencies)))
        object.__setattr__(self, "_preview_sessions", preview_sessions)
        object.__setattr__(self, "_preview_next_session", preview_next_session)
        object.__setattr__(self, "_captured_recording_artifacts", captured_recording_artifacts)

    def _assert_current(self) -> None:
        if (
            self._verifier_pid != os.getpid()
            or require_named_execution_context() is not self._context
        ):
            _invalid("verified named inputs cannot cross PID/context")

    @property
    def receipt(self) -> NamedDQExecutionReceipt:
        self._assert_current()
        return self._receipt

    @property
    def successful_dispatch(self) -> NamedDQSuccessfulDispatchBinding:
        self._assert_current()
        return self._successful_dispatch

    @property
    def verifier_pid(self) -> int:
        self._assert_current()
        return self._verifier_pid

    @property
    def scope(self) -> NamedDQScope:
        self._assert_current()
        return self._receipt.request.scope

    def assert_scope_covered(self, required: NamedDQScope) -> None:
        self._assert_current()
        if (
            not isinstance(required, NamedDQScope)
            or not self._receipt.request.scope.covers(required)
            or not self._receipt.evaluated_window.contains(required.requested_window)
        ):
            _invalid("required consumption scope is not covered by canonical DQ")

    def bytes_for(self, role: str, *, required_scope: NamedDQScope) -> bytes:
        self.assert_scope_covered(required_scope)
        if role not in required_scope.input_roles:
            _invalid("requested role not declared in required scope")
        for bound_role, content in self._captured:
            if bound_role == role:
                return content
        _invalid("requested role is not verified")
        raise AssertionError("unreachable")

    def prices_for_equal_risk_preview(self, *, required_scope: NamedEqualRiskPriceScope) -> bytes:
        """Deliver captured prices under the separate reviewed price-range contract.

        The original strict canonical call checks every expected ticker/session
        over its full requested window. The legacy evaluated window is a common
        prices/rates intersection and remains unchanged. This method neither
        parses nor repairs prices, reruns DQ, verifies registry strategy semantics,
        nor claims historical availability or sufficient calculation lookback.
        """
        self._assert_current()
        if type(required_scope) is not NamedEqualRiskPriceScope:
            _invalid("equal-risk prices require the fixed typed consumer scope")
        execution = self._receipt.execution
        if (
            execution.source_manifest_path != EQUAL_RISK_PRICE_SOURCE_MANIFEST_PATH
            or execution.source_manifest_sha256 != EQUAL_RISK_PRICE_SOURCE_MANIFEST_SHA256
        ):
            _invalid("equal-risk prices require the reviewed exact source manifest")
        if required_scope.registry_binding not in self._receipt.execution_dependencies:
            _invalid("equal-risk registry differs from the captured execution dependency")
        required_dq = NamedDQScope(
            as_of=required_scope.as_of,
            requested_window=required_scope.requested_window,
            expected_price_tickers=required_scope.expected_price_tickers,
            expected_rate_series=required_scope.guard_rate_series,
            input_roles=("prices", "rates"),
            require_secondary_prices=False,
        )
        if not self._receipt.request.scope.covers(required_dq):
            _invalid("equal-risk price range is not covered by the original canonical request")
        for role, content in self._captured:
            if role == "prices":
                return content
        _invalid("equal-risk prices are not verified")
        raise AssertionError("unreachable")

    def inputs_for_five_candidate_preview(
        self, *, required_scope: NamedEqualRiskPriceScope
    ) -> tuple[bytes, bytes, tuple[date, ...], date]:
        """Fixed captured inputs and calendar witness; no loader or DQ call.

        The verifier minted the calendar tuple with the original bound XNYS
        functions before sealing. A caller cannot supply a calendar or registry
        mapping; clearing a global loader cache cannot introduce consumer I/O.
        """
        self._assert_current()
        if type(required_scope) is not NamedEqualRiskPriceScope:
            _invalid("five-candidate preview requires the fixed typed price scope")
        execution = self._receipt.execution
        if (
            execution.source_manifest_path != FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_PATH
            or execution.source_manifest_sha256 != FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_SHA256
        ):
            _invalid("five-candidate preview requires its reviewed exact source manifest")
        return self._captured_preview_inputs(required_scope=required_scope)

    def inputs_for_prospective_five_candidate_preview(
        self, *, required_scope: NamedEqualRiskPriceScope
    ) -> tuple[bytes, bytes, tuple[date, ...], date]:
        """S3b-only calculation access; the old 59-profile does not acquire it."""
        self._assert_prospective_profile()
        if type(required_scope) is not NamedEqualRiskPriceScope:
            _invalid("prospective preview requires the fixed typed price scope")
        return self._captured_preview_inputs(required_scope=required_scope)

    def _assert_prospective_profile(self) -> None:
        self._assert_current()
        if (
            self._receipt.execution.source_manifest_path,
            self._receipt.execution.source_manifest_sha256,
        ) != (
            PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_PATH,
            PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_SHA256,
        ):
            _invalid("prospective capture requires its reviewed exact source manifest")

    def _captured_preview_inputs(
        self, *, required_scope: NamedEqualRiskPriceScope
    ) -> tuple[bytes, bytes, tuple[date, ...], date]:
        # Called only after the public accessor checks its distinct exact pin.
        if required_scope.registry_binding not in self._receipt.execution_dependencies:
            _invalid("preview registry differs from the captured execution dependency")
        required_dq = NamedDQScope(
            as_of=required_scope.as_of,
            requested_window=required_scope.requested_window,
            expected_price_tickers=required_scope.expected_price_tickers,
            expected_rate_series=required_scope.guard_rate_series,
            input_roles=("prices", "rates"),
            require_secondary_prices=False,
        )
        if not self._receipt.request.scope.covers(required_dq):
            _invalid("preview price range is not covered by the original canonical request")
        dependencies = dict(self._captured_dependencies)
        registry = dependencies.get(EQUAL_RISK_PRICE_REGISTRY_PATH)
        sessions = tuple(
            session
            for session in self._preview_sessions
            if required_scope.requested_window.start <= session <= required_scope.as_of
        )
        if registry is None or not sessions or self._preview_next_session is None:
            _invalid("preview requires captured registry and verifier calendar witness")
        if sessions[-1] != required_scope.as_of:
            _invalid("preview as-of is not a canonical XNYS session")
        for role, content in self._captured:
            if role == "prices":
                return content, registry, sessions, self._preview_next_session
        _invalid("preview prices are not verified")

    def recording_closure_for_prospective(self) -> tuple[tuple[str, bytes], ...]:
        """Copy verified bytes for preservation, never grant feature/rates access.

        Includes every DQ input, even guard-only roles, and the metadata captured
        by the verifier before minting this seal. No source path is reopened.
        """
        self._assert_prospective_profile()
        return self._recording_closure(composer=False)

    def _assert_composer_profile(self) -> None:
        self._assert_current()
        if (
            self._receipt.execution.source_manifest_path,
            self._receipt.execution.source_manifest_sha256,
        ) != (
            COMPOSER_PROSPECTIVE_SOURCE_MANIFEST_PATH,
            COMPOSER_PROSPECTIVE_SOURCE_MANIFEST_SHA256,
        ):
            _invalid("Composer requires its separate reviewed exact source manifest")

    def inputs_for_composer_segment(
        self, *, required_scope: NamedComposerInputScope
    ) -> tuple[bytes, bytes, tuple[date, ...], date]:
        """Current-known prices/rates bytes for exactly one reviewed DQ segment.

        The full requested price coverage was checked by canonical DQ. Its
        common evaluated window is retained unchanged. The new input policy
        explicitly permits earlier rates under the original quality policy;
        this is neither a historical available-at proof nor aggregate readiness.
        The consumer must verify all three same-snapshot segments before fit.
        """
        self._assert_composer_profile()
        if type(required_scope) is not NamedComposerInputScope:
            _invalid("Composer requires its exact typed segment")
        if (
            self._receipt.request.scope != required_scope.dq_scope
            or required_scope.input_policy_binding not in self._receipt.execution_dependencies
            or self._receipt.publication.transaction_window.end != required_scope.as_of
        ):
            _invalid("Composer segment, policy or snapshot cutoff differs")
        if (
            not self._preview_sessions
            or self._preview_sessions[-1] != required_scope.as_of
            or self._preview_next_session is None
        ):
            _invalid("Composer requires the original verified XNYS calendar witness")
        inputs = dict(self._captured)
        return (
            inputs["prices"],
            inputs["rates"],
            self._preview_sessions,
            self._preview_next_session,
        )

    def recording_closure_for_composer(self) -> tuple[tuple[str, bytes], ...]:
        """Preserve this Composer segment; aggregate/input policy checks remain required."""
        self._assert_composer_profile()
        return self._recording_closure(composer=True)

    def _recording_closure(self, *, composer: bool) -> tuple[tuple[str, bytes], ...]:
        rows = [("input_" + role, content) for role, content in self._captured]
        bindings = {item.role: item.member for item in self._receipt.inputs}
        index = [
            {
                "role": "input_" + role,
                "binding": bindings[role].to_dict(),
                "semantic_role": (
                    "PRICE_FEATURE"
                    if role == "prices"
                    else (
                        "CURRENT_KNOWN_RATE_FEATURE"
                        if composer and role == "rates"
                        else "DQ_GUARD_ONLY"
                    )
                ),
            }
            for role, _ in self._captured
        ]
        for role, binding, content in self._captured_recording_artifacts:
            rows.append((role, content))
            index.append(
                {"role": role, "binding": binding.to_dict(), "semantic_role": "VERIFIED_PROVENANCE"}
            )
        dependencies = {item.relative_path: item for item in self._receipt.execution_dependencies}
        for ordinal, (path, content) in enumerate(self._captured_dependencies):
            role = f"dependency_{ordinal:03d}"
            rows.append((role, content))
            index.append(
                {
                    "role": role,
                    "binding": dependencies[path].to_dict(),
                    "semantic_role": "EXECUTION_DEPENDENCY",
                }
            )
        manifest = {
            "schema_version": (
                "composer_verified_input_closure.v1"
                if composer
                else "prospective_verified_input_closure.v1"
            ),
            "request_id": self._receipt.request.request_id,
            "receipt_id": self._receipt.receipt_id,
            "execution_identity_sha256": self._receipt.execution.stable_identity_sha256,
            "members": sorted(index, key=lambda item: str(item["role"])),
            "all_dq_input_roles": sorted(item.role for item in self._receipt.inputs),
            "recording_only": True,
            "rates_feature_access_granted": composer,
            "provider_available_at_status": "NOT_ESTABLISHED",
        }
        rows.append(("closure_manifest", canonical_json_value(manifest).encode("utf-8")))
        return tuple(sorted(rows))

    def __reduce_ex__(self, protocol: SupportsIndex) -> Never:
        raise TypeError("verified named inputs cannot be serialized")

    def __reduce__(self) -> Never:
        raise TypeError("verified named inputs cannot be serialized")


def _verified_named_inputs_from_receipt(
    receipt: NamedDQExecutionReceipt,
    *,
    receipt_path: str,
    successful_dispatch: NamedDQSuccessfulDispatchBinding,
    captured_inputs: tuple[tuple[str, bytes], ...],
    captured_dependencies: tuple[tuple[str, bytes], ...] = (),
    preview_sessions: tuple[date, ...] = (),
    preview_next_session: date | None = None,
    captured_recording_artifacts: tuple[tuple[str, NamedArtifactBinding, bytes], ...] = (),
) -> VerifiedNamedInputs:
    """Named verifier only, after full byte/provenance checks; not a verification API."""
    return VerifiedNamedInputs(
        receipt,
        captured_inputs,
        receipt_path=receipt_path,
        successful_dispatch=successful_dispatch,
        captured_dependencies=captured_dependencies,
        preview_sessions=preview_sessions,
        preview_next_session=preview_next_session,
        captured_recording_artifacts=captured_recording_artifacts,
        _seal=_VERIFIED_NAMED_SEAL,
    )
