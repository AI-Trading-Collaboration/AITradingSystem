from __future__ import annotations

import hashlib
import json
import re
import subprocess
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

import yaml

from ai_trading_system.platform.artifacts import write_bytes_atomic

HISTORICAL_ADAPTER_REVIEW_POLICY_SCHEMA_VERSION = "atlas_historical_adapter_review_policy.v1"
HISTORICAL_ADAPTER_REVIEW_PACK_SCHEMA_VERSION = "atlas_historical_adapter_review_pack.v1"
HISTORICAL_ADAPTER_REVIEW_VALIDATION_SCHEMA_VERSION = (
    "atlas_historical_adapter_review_validation.v1"
)
DEFAULT_POLICY_REPOSITORY_PATH = "config/atlas/historical_adapter_review_policy.yaml"
MANDATORY_EXCLUDED_REPOSITORY_PATH = "docs/research/growth_tilt_owner_diagnosis_pack.md"
EXPECTED_INVENTORY_CLASSIFICATION = "TRACKED_UNREGISTERED_REVIEW_REQUIRED"

READY_FOR_OWNER_ADAPTER_REVIEW = "READY_FOR_OWNER_ADAPTER_REVIEW"
NEEDS_SCHEMA_NORMALIZATION = "NEEDS_SCHEMA_NORMALIZATION"
NEEDS_SOURCE_REGISTRATION = "NEEDS_SOURCE_REGISTRATION"
REJECTED_FROM_FIRST_BATCH = "REJECTED_FROM_FIRST_BATCH"
DISPOSITION_CODES = (
    READY_FOR_OWNER_ADAPTER_REVIEW,
    NEEDS_SCHEMA_NORMALIZATION,
    NEEDS_SOURCE_REGISTRATION,
    REJECTED_FROM_FIRST_BATCH,
)

_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_BLOB_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_MARKDOWN_HEADING_PATTERN = re.compile(r"^#{1,6}\s+(.+?)\s*$")


class HistoricalAdapterReviewError(ValueError):
    pass


@dataclass(frozen=True)
class CandidateFamilyPolicy:
    candidate_family_id: str
    role_code: str
    json_path: str
    markdown_path: str


@dataclass(frozen=True)
class SlotRule:
    exact_fields: tuple[str, ...]
    suffixes: tuple[str, ...]


@dataclass(frozen=True)
class HistoricalAdapterReviewPolicy:
    policy_id: str
    inventory_path: str
    expected_inventory_id: str
    expected_inventory_sha256: str
    required_inventory_schema_version: str
    required_inventory_classification: str
    excluded_paths: tuple[str, ...]
    candidate_families: tuple[CandidateFamilyPolicy, ...]
    required_slots: tuple[str, ...]
    slot_rules: Mapping[str, SlotRule]
    identity_value_fields: tuple[str, ...]
    markdown_tokens: tuple[str, ...]
    max_pointer_records_per_candidate: int
    max_markdown_title_characters: int
    max_identity_token_characters: int

    @property
    def candidate_paths(self) -> tuple[str, ...]:
        return tuple(
            path
            for family in self.candidate_families
            for path in (family.json_path, family.markdown_path)
        )


@dataclass(frozen=True)
class HistoricalAdapterReviewPack:
    exact_commit: str
    policy_id: str
    policy_receipt: Mapping[str, object]
    inventory_receipt: Mapping[str, object]
    candidate_records: tuple[Mapping[str, object], ...]
    summary: Mapping[str, int]

    def _identity_payload(self) -> dict[str, object]:
        return {
            "schema_version": HISTORICAL_ADAPTER_REVIEW_PACK_SCHEMA_VERSION,
            "exact_commit": self.exact_commit,
            "policy_id": self.policy_id,
            "policy_receipt": dict(self.policy_receipt),
            "inventory_receipt": dict(self.inventory_receipt),
            "candidate_records": [dict(item) for item in self.candidate_records],
            "summary": dict(self.summary),
            "safety": _safety_payload(),
        }

    @property
    def review_pack_id(self) -> str:
        digest = hashlib.sha256(_canonical_json_bytes(self._identity_payload())).hexdigest()
        return f"atlas_historical_adapter_review_{digest[:20]}"

    def to_dict(self) -> dict[str, object]:
        return {"review_pack_id": self.review_pack_id, **self._identity_payload()}

    def canonical_json_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())


@dataclass(frozen=True)
class HistoricalAdapterReviewValidationResult:
    status: str
    review_pack_id: str
    exact_commit: str
    checks: tuple[str, ...]
    errors: tuple[str, ...]
    observed_summary: Mapping[str, int]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": HISTORICAL_ADAPTER_REVIEW_VALIDATION_SCHEMA_VERSION,
            "status": self.status,
            "review_pack_id": self.review_pack_id,
            "exact_commit": self.exact_commit,
            "checks": list(self.checks),
            "errors": list(self.errors),
            "observed_summary": dict(self.observed_summary),
            **_safety_payload(),
        }

    def canonical_json_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())


@dataclass(frozen=True)
class HistoricalAdapterReviewRenderedArtifact:
    path: str
    sha256: str
    size_bytes: int


def build_historical_adapter_review(
    *,
    repository_root: Path,
    exact_commit: str,
    policy_repository_path: str = DEFAULT_POLICY_REPOSITORY_PATH,
) -> HistoricalAdapterReviewPack:
    _require_exact_commit(exact_commit)
    root = repository_root.resolve()
    policy_path = _safe_repository_path(policy_repository_path)
    policy_bytes = _git_blob_bytes(root, exact_commit, policy_path)
    policy = _load_policy(_yaml_mapping(policy_bytes, "policy"))
    inventory_file = _resolve_repository_file(root, policy.inventory_path)
    inventory_bytes = inventory_file.read_bytes()
    inventory_payload = _json_mapping(inventory_bytes, "inventory")
    artifact_payloads: dict[str, bytes] = {}
    blob_sha1s: dict[str, str] = {}
    for candidate_path in policy.candidate_paths:
        artifact_payloads[candidate_path] = _git_blob_bytes(root, exact_commit, candidate_path)
        blob_sha1s[candidate_path] = _git_blob_sha1(root, exact_commit, candidate_path)
    return build_historical_adapter_review_from_payloads(
        exact_commit=exact_commit,
        policy=policy,
        policy_path=policy_path,
        policy_bytes=policy_bytes,
        inventory_payload=inventory_payload,
        inventory_bytes=inventory_bytes,
        artifact_payloads=artifact_payloads,
        blob_sha1s=blob_sha1s,
    )


def build_historical_adapter_review_from_payloads(
    *,
    exact_commit: str,
    policy: HistoricalAdapterReviewPolicy,
    policy_path: str,
    policy_bytes: bytes,
    inventory_payload: Mapping[str, object],
    inventory_bytes: bytes,
    artifact_payloads: Mapping[str, bytes],
    blob_sha1s: Mapping[str, str],
) -> HistoricalAdapterReviewPack:
    _require_exact_commit(exact_commit)
    _validate_inventory_binding(policy, inventory_payload, inventory_bytes)
    expected_paths = set(policy.candidate_paths)
    if set(artifact_payloads) != expected_paths or set(blob_sha1s) != expected_paths:
        raise HistoricalAdapterReviewError("CANDIDATE_ARTIFACT_ALLOWLIST_MISMATCH")
    inventory_paths = _inventory_classifications(inventory_payload)
    candidate_records = tuple(
        _build_candidate_record(
            family=family,
            policy=policy,
            inventory_paths=inventory_paths,
            artifact_payloads=artifact_payloads,
            blob_sha1s=blob_sha1s,
        )
        for family in policy.candidate_families
    )
    disposition_counts = Counter(str(item["disposition"]) for item in candidate_records)
    complete_slot_count = sum(
        all(bool(value) for value in _mapping(item["slot_coverage"], "slot_coverage").values())
        for item in candidate_records
    )
    summary = {
        "candidate_family_count": len(candidate_records),
        "candidate_artifact_count": len(expected_paths),
        "complete_required_slot_candidate_count": complete_slot_count,
        "schema_normalization_candidate_count": disposition_counts[NEEDS_SCHEMA_NORMALIZATION],
        "source_registration_candidate_count": disposition_counts[NEEDS_SOURCE_REGISTRATION],
        "owner_review_ready_candidate_count": sum(
            item["owner_review_readiness"] == READY_FOR_OWNER_ADAPTER_REVIEW
            for item in candidate_records
        ),
        "rejected_candidate_count": disposition_counts[REJECTED_FROM_FIRST_BATCH],
        "allowlist_outside_research_content_read_count": 0,
    }
    return HistoricalAdapterReviewPack(
        exact_commit=exact_commit,
        policy_id=policy.policy_id,
        policy_receipt=_artifact_receipt(policy_path, policy_bytes),
        inventory_receipt={
            **_artifact_receipt(policy.inventory_path, inventory_bytes),
            "inventory_id": policy.expected_inventory_id,
            "classification_required": policy.required_inventory_classification,
        },
        candidate_records=candidate_records,
        summary=summary,
    )


def validate_historical_adapter_review(
    review_pack: HistoricalAdapterReviewPack,
    *,
    repository_root: Path,
    policy_repository_path: str = DEFAULT_POLICY_REPOSITORY_PATH,
) -> HistoricalAdapterReviewValidationResult:
    checks = (
        "EXACT_COMMIT_BOUND",
        "POLICY_RECEIPT_BOUND",
        "INVENTORY_ID_AND_SHA_BOUND",
        "CANDIDATE_ALLOWLIST_EXACT",
        "CANDIDATE_BLOB_AND_CONTENT_RECEIPTS_BOUND",
        "JSON_PARSE_AND_POINTER_SHAPES_BOUND",
        "MARKDOWN_COMPANION_BOUND",
        "REQUIRED_SLOT_RULES_BOUND",
        "DISPOSITION_RULES_BOUND",
        "KNOWN_EXCLUSION_NOT_READ",
        "ALLOWLIST_OUTSIDE_CONTENT_READ_ZERO",
        "SOURCE_REGISTRATION_DISABLED",
        "ATLAS_RESULT_PROJECTION_DISABLED",
        "INVESTMENT_CONCLUSION_DISABLED",
        "PRODUCTION_EFFECT_NONE",
        "BROKER_ACTION_NONE",
        "CANONICAL_REBUILD_BYTE_IDENTICAL",
    )
    errors: list[str] = []
    try:
        rebuilt = build_historical_adapter_review(
            repository_root=repository_root,
            exact_commit=review_pack.exact_commit,
            policy_repository_path=policy_repository_path,
        )
        if rebuilt.canonical_json_bytes() != review_pack.canonical_json_bytes():
            errors.append("REVIEW_PACK_CANONICAL_REBUILD_MISMATCH")
    except (HistoricalAdapterReviewError, OSError, subprocess.SubprocessError) as exc:
        errors.append(f"REVIEW_PACK_REBUILD_FAILED:{type(exc).__name__}:{exc}")
    serialized = review_pack.to_dict()
    if MANDATORY_EXCLUDED_REPOSITORY_PATH in _all_string_values(serialized):
        errors.append("KNOWN_EXCLUSION_LEAKED_INTO_REVIEW_PACK")
    if serialized.get("safety") != _safety_payload():
        errors.append("REVIEW_PACK_SAFETY_BOUNDARY_MISMATCH")
    return HistoricalAdapterReviewValidationResult(
        status="PASS" if not errors else "FAIL",
        review_pack_id=review_pack.review_pack_id,
        exact_commit=review_pack.exact_commit,
        checks=checks,
        errors=tuple(errors),
        observed_summary=dict(review_pack.summary),
    )


def render_historical_adapter_review_markdown(
    review_pack: HistoricalAdapterReviewPack,
) -> str:
    rows: list[str] = []
    questions: list[str] = []
    for record in review_pack.candidate_records:
        coverage = _mapping(record["slot_coverage"], "slot_coverage")
        slots = " / ".join(f"{slot}={'✓' if value else '缺'}" for slot, value in coverage.items())
        rows.append(
            "|{role}|`{candidate}`|{slots}|`{disposition}`|`{json_path}`|".format(
                role=record["role_code"],
                candidate=record["candidate_family_id"],
                slots=slots,
                disposition=record["disposition"],
                json_path=_mapping(record["json_artifact"], "json_artifact")["path"],
            )
        )
        next_action_values = record["required_next_actions"]
        if not isinstance(next_action_values, tuple):
            raise HistoricalAdapterReviewError("REQUIRED_NEXT_ACTIONS_TUPLE_REQUIRED")
        next_actions = ", ".join(str(item) for item in next_action_values)
        questions.append(f"- `{record['candidate_family_id']}`：{next_actions or '无需动作'}")
    summary = review_pack.summary
    return "\n".join(
        [
            "# Atlas 首批历史 Adapter 审阅包 V1",
            "",
            f"- review_pack_id：`{review_pack.review_pack_id}`",
            f"- exact_commit：`{review_pack.exact_commit}`",
            "- 口径：只审阅 exact allowlist 的字段形态与来源收据，不输出研究字段值。",
            "- 本包不是策略结论、source registration 或页面接入授权。",
            "",
            "## 一眼看懂",
            "",
            f"- 候选 family：{summary['candidate_family_count']}",
            f"- 五类结构槽齐全：{summary['complete_required_slot_candidate_count']}",
            f"- 需要 schema normalization：{summary['schema_normalization_candidate_count']}",
            f"- 需要 source registration：{summary['source_registration_candidate_count']}",
            "",
            "## 研究主线候选",
            "",
            "|角色|candidate|结构槽|disposition|JSON authority candidate|",
            "|---|---|---|---|---|",
            *rows,
            "",
            "## Owner 后续审阅问题",
            "",
            *questions,
            "",
            "## 安全边界",
            "",
            "- `source_registration_performed=false`",
            "- `atlas_result_projection_performed=false`",
            "- `research_value_projection_performed=false`",
            "- `investment_conclusion_generated=false`",
            "- `production_effect=none`",
            "- `broker_action=none`",
            "",
        ]
    )


def write_historical_adapter_review_artifacts(
    review_pack: HistoricalAdapterReviewPack,
    output_directory: Path,
    *,
    repository_root: Path,
    policy_repository_path: str = DEFAULT_POLICY_REPOSITORY_PATH,
) -> tuple[HistoricalAdapterReviewRenderedArtifact, ...]:
    validation = validate_historical_adapter_review(
        review_pack,
        repository_root=repository_root,
        policy_repository_path=policy_repository_path,
    )
    if validation.status != "PASS":
        raise HistoricalAdapterReviewError(
            "ATLAS_HISTORICAL_ADAPTER_REVIEW_VALIDATION_FAILED:" + ",".join(validation.errors)
        )
    output_directory.mkdir(parents=True, exist_ok=True)
    payloads = {
        "review_pack.json": review_pack.canonical_json_bytes(),
        "review_pack.md": render_historical_adapter_review_markdown(review_pack).encode("utf-8"),
        "validation.json": validation.canonical_json_bytes(),
    }
    artifacts: list[HistoricalAdapterReviewRenderedArtifact] = []
    for name, payload in payloads.items():
        result = write_bytes_atomic(output_directory / name, payload)
        artifacts.append(
            HistoricalAdapterReviewRenderedArtifact(
                path=result.path.as_posix(),
                sha256=result.sha256,
                size_bytes=result.size_bytes,
            )
        )
    return tuple(artifacts)


def _build_candidate_record(
    *,
    family: CandidateFamilyPolicy,
    policy: HistoricalAdapterReviewPolicy,
    inventory_paths: Mapping[str, str],
    artifact_payloads: Mapping[str, bytes],
    blob_sha1s: Mapping[str, str],
) -> Mapping[str, object]:
    for path in (family.json_path, family.markdown_path):
        if inventory_paths.get(path) != policy.required_inventory_classification:
            raise HistoricalAdapterReviewError(
                f"CANDIDATE_INVENTORY_CLASSIFICATION_MISMATCH:{path}"
            )
        if path in policy.excluded_paths:
            raise HistoricalAdapterReviewError(f"CANDIDATE_PATH_EXCLUDED:{path}")
        if not _BLOB_PATTERN.fullmatch(blob_sha1s[path]):
            raise HistoricalAdapterReviewError(f"CANDIDATE_BLOB_SHA1_INVALID:{path}")
    json_bytes = artifact_payloads[family.json_path]
    markdown_bytes = artifact_payloads[family.markdown_path]
    json_payload = _json_mapping(json_bytes, family.json_path)
    markdown_text = _utf8_text(markdown_bytes, family.markdown_path)
    pointer_records = _json_shape_records(json_payload)
    if len(pointer_records) > policy.max_pointer_records_per_candidate:
        raise HistoricalAdapterReviewError(
            f"CANDIDATE_POINTER_RECORD_LIMIT_EXCEEDED:{family.candidate_family_id}"
        )
    slot_pointers = {
        slot: tuple(
            record["pointer"]
            for record in pointer_records
            if _field_matches_rule(str(record["field"]), policy.slot_rules[slot])
        )
        for slot in policy.required_slots
    }
    slot_coverage = {slot: bool(slot_pointers[slot]) for slot in policy.required_slots}
    missing_slots = [slot for slot, present in slot_coverage.items() if not present]
    title = _markdown_title(markdown_text, policy.max_markdown_title_characters)
    identity_tokens = _identity_tokens(
        json_payload,
        fields=policy.identity_value_fields,
        max_characters=policy.max_identity_token_characters,
    )
    markdown_identity_matches = tuple(
        token for token in identity_tokens if token.casefold() in markdown_text.casefold()
    )
    if missing_slots:
        disposition = NEEDS_SCHEMA_NORMALIZATION
        owner_review_readiness = "BLOCKED_PENDING_SCHEMA_NORMALIZATION"
        required_next_actions = tuple(
            [f"DEFINE_TYPED_SLOT:{slot}" for slot in missing_slots]
            + ["OWNER_REVIEW_AFTER_SCHEMA_NORMALIZATION"]
        )
    else:
        disposition = NEEDS_SOURCE_REGISTRATION
        owner_review_readiness = READY_FOR_OWNER_ADAPTER_REVIEW
        required_next_actions = (
            "OWNER_REVIEW_EXACT_ARTIFACT_SHA",
            "REVIEWED_SOURCE_REGISTRATION_REQUIRED",
            "TYPED_ADAPTER_TASK_REQUIRED",
        )
    return {
        "candidate_family_id": family.candidate_family_id,
        "role_code": family.role_code,
        "json_artifact": {
            **_artifact_receipt(family.json_path, json_bytes),
            "git_blob_sha1": blob_sha1s[family.json_path],
        },
        "markdown_artifact": {
            **_artifact_receipt(family.markdown_path, markdown_bytes),
            "git_blob_sha1": blob_sha1s[family.markdown_path],
            "title": title,
            "token_presence": {
                token: token.casefold() in markdown_text.casefold()
                for token in policy.markdown_tokens
            },
        },
        "inventory_classification": policy.required_inventory_classification,
        "top_level_kind": "object",
        "top_level_fields": tuple(sorted(json_payload)),
        "json_shape_records": pointer_records,
        "slot_pointers": slot_pointers,
        "slot_coverage": slot_coverage,
        "missing_required_slots": tuple(missing_slots),
        "json_identity_tokens": identity_tokens,
        "markdown_exact_identity_matches": markdown_identity_matches,
        "disposition": disposition,
        "owner_review_readiness": owner_review_readiness,
        "required_next_actions": required_next_actions,
        "research_field_values_serialized": False,
    }


def _load_policy(payload: Mapping[str, object]) -> HistoricalAdapterReviewPolicy:
    if payload.get("schema_version") != HISTORICAL_ADAPTER_REVIEW_POLICY_SCHEMA_VERSION:
        raise HistoricalAdapterReviewError("HISTORICAL_ADAPTER_REVIEW_POLICY_SCHEMA_MISMATCH")
    inventory = _mapping(payload.get("inventory"), "inventory")
    excluded_paths = tuple(
        _safe_repository_path(item)
        for item in _text_sequence(payload.get("excluded_paths"), "excluded_paths")
    )
    if MANDATORY_EXCLUDED_REPOSITORY_PATH not in excluded_paths:
        raise HistoricalAdapterReviewError("MANDATORY_KNOWN_EXCLUSION_MISSING")
    families = tuple(
        CandidateFamilyPolicy(
            candidate_family_id=_required_text(item, "candidate_family_id"),
            role_code=_required_text(item, "role_code"),
            json_path=_safe_repository_path(_required_text(item, "json_path")),
            markdown_path=_safe_repository_path(_required_text(item, "markdown_path")),
        )
        for item in _mapping_sequence(payload.get("candidate_families"), "candidate_families")
    )
    if not families or len({item.candidate_family_id for item in families}) != len(families):
        raise HistoricalAdapterReviewError("CANDIDATE_FAMILY_IDS_INVALID")
    candidate_paths = [
        path for family in families for path in (family.json_path, family.markdown_path)
    ]
    if len(candidate_paths) != len(set(candidate_paths)):
        raise HistoricalAdapterReviewError("CANDIDATE_PATHS_DUPLICATED")
    if any(path in excluded_paths for path in candidate_paths):
        raise HistoricalAdapterReviewError("KNOWN_EXCLUSION_IN_CANDIDATE_ALLOWLIST")
    required_slots = _text_sequence(payload.get("required_slots"), "required_slots")
    slot_rule_payload = _mapping(payload.get("slot_rules"), "slot_rules")
    if set(slot_rule_payload) != set(required_slots):
        raise HistoricalAdapterReviewError("SLOT_RULE_KEYS_MISMATCH")
    slot_rules = {
        slot: SlotRule(
            exact_fields=_text_sequence(
                _mapping(slot_rule_payload[slot], slot).get("exact_fields"),
                f"{slot}.exact_fields",
            ),
            suffixes=_text_sequence(
                _mapping(slot_rule_payload[slot], slot).get("suffixes"),
                f"{slot}.suffixes",
                allow_empty=True,
            ),
        )
        for slot in required_slots
    }
    disposition_codes = _text_sequence(payload.get("disposition_codes"), "disposition_codes")
    if disposition_codes != DISPOSITION_CODES:
        raise HistoricalAdapterReviewError("DISPOSITION_CODES_MISMATCH")
    safety = _mapping(payload.get("safety"), "safety")
    if dict(safety) != _safety_payload():
        raise HistoricalAdapterReviewError("HISTORICAL_ADAPTER_REVIEW_SAFETY_MISMATCH")
    required_classification = _required_text(inventory, "required_classification")
    if required_classification != EXPECTED_INVENTORY_CLASSIFICATION:
        raise HistoricalAdapterReviewError("INVENTORY_CLASSIFICATION_POLICY_MISMATCH")
    return HistoricalAdapterReviewPolicy(
        policy_id=_required_text(payload, "policy_id"),
        inventory_path=_safe_repository_path(_required_text(inventory, "path")),
        expected_inventory_id=_required_text(inventory, "expected_inventory_id"),
        expected_inventory_sha256=_required_sha256(inventory, "expected_sha256"),
        required_inventory_schema_version=_required_text(inventory, "required_schema_version"),
        required_inventory_classification=required_classification,
        excluded_paths=excluded_paths,
        candidate_families=families,
        required_slots=required_slots,
        slot_rules=slot_rules,
        identity_value_fields=_text_sequence(
            payload.get("identity_value_fields"), "identity_value_fields"
        ),
        markdown_tokens=_text_sequence(payload.get("markdown_tokens"), "markdown_tokens"),
        max_pointer_records_per_candidate=_positive_int(
            payload, "max_pointer_records_per_candidate"
        ),
        max_markdown_title_characters=_positive_int(payload, "max_markdown_title_characters"),
        max_identity_token_characters=_positive_int(payload, "max_identity_token_characters"),
    )


def _validate_inventory_binding(
    policy: HistoricalAdapterReviewPolicy,
    inventory_payload: Mapping[str, object],
    inventory_bytes: bytes,
) -> None:
    if hashlib.sha256(inventory_bytes).hexdigest() != policy.expected_inventory_sha256:
        raise HistoricalAdapterReviewError("HISTORICAL_COVERAGE_INVENTORY_SHA_MISMATCH")
    if inventory_payload.get("schema_version") != policy.required_inventory_schema_version:
        raise HistoricalAdapterReviewError("HISTORICAL_COVERAGE_INVENTORY_SCHEMA_MISMATCH")
    if inventory_payload.get("inventory_id") != policy.expected_inventory_id:
        raise HistoricalAdapterReviewError("HISTORICAL_COVERAGE_INVENTORY_ID_MISMATCH")


def _inventory_classifications(inventory_payload: Mapping[str, object]) -> Mapping[str, str]:
    records = _mapping_sequence(
        inventory_payload.get("tracked_path_records"), "tracked_path_records"
    )
    result: dict[str, str] = {}
    for record in records:
        path = _safe_repository_path(_required_text(record, "path"))
        if path in result:
            raise HistoricalAdapterReviewError(f"INVENTORY_TRACKED_PATH_DUPLICATE:{path}")
        result[path] = _required_text(record, "classification")
    return result


def _json_shape_records(payload: Mapping[str, object]) -> tuple[Mapping[str, object], ...]:
    records: list[Mapping[str, object]] = []

    def visit(value: object, pointer: str, field: str) -> None:
        if isinstance(value, Mapping):
            records.append(
                {"pointer": pointer or "/", "field": field, "kind": "object", "size": len(value)}
            )
            for key in sorted(value):
                if not isinstance(key, str):
                    raise HistoricalAdapterReviewError("JSON_OBJECT_KEY_NOT_STRING")
                visit(value[key], _join_pointer(pointer, key), key)
        elif isinstance(value, list):
            records.append(
                {"pointer": pointer, "field": field, "kind": "array", "size": len(value)}
            )
            for index, item in enumerate(value):
                visit(item, _join_pointer(pointer, str(index)), field)
        else:
            records.append({"pointer": pointer, "field": field, "kind": _json_scalar_kind(value)})

    visit(payload, "", "")
    return tuple(records)


def _field_matches_rule(field: str, rule: SlotRule) -> bool:
    return field in rule.exact_fields or any(field.endswith(suffix) for suffix in rule.suffixes)


def _identity_tokens(
    payload: Mapping[str, object],
    *,
    fields: Sequence[str],
    max_characters: int,
) -> tuple[str, ...]:
    tokens: set[str] = set()
    for field in fields:
        value = payload.get(field)
        if isinstance(value, str) and 0 < len(value) <= max_characters:
            tokens.add(value)
    return tuple(sorted(tokens))


def _markdown_title(text: str, max_characters: int) -> str:
    for line in text.splitlines():
        match = _MARKDOWN_HEADING_PATTERN.fullmatch(line.strip())
        if match:
            title = match.group(1).strip()
            if len(title) > max_characters:
                raise HistoricalAdapterReviewError("MARKDOWN_TITLE_TOO_LONG")
            return title
    raise HistoricalAdapterReviewError("MARKDOWN_TITLE_MISSING")


def _json_scalar_kind(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    raise HistoricalAdapterReviewError(f"JSON_VALUE_KIND_UNSUPPORTED:{type(value).__name__}")


def _join_pointer(pointer: str, token: str) -> str:
    escaped = token.replace("~", "~0").replace("/", "~1")
    return f"{pointer}/{escaped}" if pointer else f"/{escaped}"


def _artifact_receipt(path: str, payload: bytes) -> Mapping[str, object]:
    return {
        "path": path,
        "sha256": hashlib.sha256(payload).hexdigest(),
        "size_bytes": len(payload),
    }


def _git_blob_bytes(repository_root: Path, exact_commit: str, path: str) -> bytes:
    safe_path = _safe_repository_path(path)
    if safe_path == MANDATORY_EXCLUDED_REPOSITORY_PATH:
        raise HistoricalAdapterReviewError("KNOWN_EXCLUSION_READ_FORBIDDEN")
    result = subprocess.run(
        ["git", "show", f"{exact_commit}:{safe_path}"],
        cwd=repository_root,
        check=True,
        capture_output=True,
    )
    return result.stdout


def _git_blob_sha1(repository_root: Path, exact_commit: str, path: str) -> str:
    safe_path = _safe_repository_path(path)
    if safe_path == MANDATORY_EXCLUDED_REPOSITORY_PATH:
        raise HistoricalAdapterReviewError("KNOWN_EXCLUSION_HASH_FORBIDDEN")
    result = subprocess.run(
        ["git", "rev-parse", f"{exact_commit}:{safe_path}"],
        cwd=repository_root,
        check=True,
        capture_output=True,
        text=True,
    )
    value = result.stdout.strip()
    if not _BLOB_PATTERN.fullmatch(value):
        raise HistoricalAdapterReviewError(f"GIT_BLOB_SHA1_INVALID:{safe_path}")
    return value


def _resolve_repository_file(repository_root: Path, repository_path: str) -> Path:
    safe_path = _safe_repository_path(repository_path)
    candidate = (repository_root / Path(*PurePosixPath(safe_path).parts)).resolve()
    try:
        candidate.relative_to(repository_root)
    except ValueError as exc:
        raise HistoricalAdapterReviewError("REPOSITORY_PATH_ESCAPES_ROOT") from exc
    return candidate


def _safe_repository_path(value: str) -> str:
    normalized = value.replace("\\", "/").strip()
    pure = PurePosixPath(normalized)
    if (
        not normalized
        or pure.is_absolute()
        or ".." in pure.parts
        or normalized.startswith("//")
        or ":" in pure.parts[0]
    ):
        raise HistoricalAdapterReviewError(f"UNSAFE_REPOSITORY_PATH:{value}")
    return pure.as_posix()


def _yaml_mapping(payload: bytes, name: str) -> Mapping[str, object]:
    value = yaml.safe_load(_utf8_text(payload, name))
    return _mapping(value, name)


def _json_mapping(payload: bytes, name: str) -> Mapping[str, object]:
    try:
        value = json.loads(_utf8_text(payload, name))
    except json.JSONDecodeError as exc:
        raise HistoricalAdapterReviewError(f"JSON_PARSE_FAILED:{name}:{exc.msg}") from exc
    return _mapping(value, name)


def _utf8_text(payload: bytes, name: str) -> str:
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise HistoricalAdapterReviewError(f"UTF8_DECODE_FAILED:{name}") from exc


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise HistoricalAdapterReviewError(f"MAPPING_REQUIRED:{field}")
    if not all(isinstance(key, str) for key in value):
        raise HistoricalAdapterReviewError(f"MAPPING_KEYS_MUST_BE_STRINGS:{field}")
    return value


def _mapping_sequence(value: object, field: str) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, list):
        raise HistoricalAdapterReviewError(f"MAPPING_SEQUENCE_REQUIRED:{field}")
    return tuple(_mapping(item, f"{field}[{index}]") for index, item in enumerate(value))


def _text_sequence(
    value: object,
    field: str,
    *,
    allow_empty: bool = False,
) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item.strip() for item in value
    ):
        if allow_empty and value == []:
            return ()
        raise HistoricalAdapterReviewError(f"TEXT_SEQUENCE_REQUIRED:{field}")
    normalized = tuple(item.strip() for item in value)
    if len(normalized) != len(set(normalized)):
        raise HistoricalAdapterReviewError(f"TEXT_SEQUENCE_DUPLICATED:{field}")
    return normalized


def _required_text(payload: Mapping[str, object], field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        raise HistoricalAdapterReviewError(f"TEXT_REQUIRED:{field}")
    return value.strip()


def _required_sha256(payload: Mapping[str, object], field: str) -> str:
    value = _required_text(payload, field)
    if not re.fullmatch(r"[0-9a-f]{64}", value):
        raise HistoricalAdapterReviewError(f"SHA256_REQUIRED:{field}")
    return value


def _positive_int(payload: Mapping[str, object], field: str) -> int:
    value = payload.get(field)
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise HistoricalAdapterReviewError(f"POSITIVE_INT_REQUIRED:{field}")
    return value


def _require_exact_commit(value: str) -> None:
    if not _COMMIT_PATTERN.fullmatch(value):
        raise HistoricalAdapterReviewError("EXACT_COMMIT_REQUIRED")


def _all_string_values(value: object) -> set[str]:
    values: set[str] = set()
    if isinstance(value, Mapping):
        for key, item in value.items():
            values.add(str(key))
            values.update(_all_string_values(item))
    elif isinstance(value, (list, tuple)):
        for item in value:
            values.update(_all_string_values(item))
    elif isinstance(value, str):
        values.add(value)
    return values


def _safety_payload() -> dict[str, object]:
    return {
        "candidate_artifact_content_read_count": 12,
        "allowlist_outside_research_content_read_count": 0,
        "source_registration_performed": False,
        "atlas_result_projection_performed": False,
        "research_value_projection_performed": False,
        "investment_conclusion_generated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


__all__ = [
    "DEFAULT_POLICY_REPOSITORY_PATH",
    "DISPOSITION_CODES",
    "HISTORICAL_ADAPTER_REVIEW_PACK_SCHEMA_VERSION",
    "HISTORICAL_ADAPTER_REVIEW_POLICY_SCHEMA_VERSION",
    "HISTORICAL_ADAPTER_REVIEW_VALIDATION_SCHEMA_VERSION",
    "HistoricalAdapterReviewError",
    "HistoricalAdapterReviewPack",
    "HistoricalAdapterReviewPolicy",
    "HistoricalAdapterReviewRenderedArtifact",
    "HistoricalAdapterReviewValidationResult",
    "NEEDS_SCHEMA_NORMALIZATION",
    "NEEDS_SOURCE_REGISTRATION",
    "READY_FOR_OWNER_ADAPTER_REVIEW",
    "build_historical_adapter_review",
    "build_historical_adapter_review_from_payloads",
    "render_historical_adapter_review_markdown",
    "validate_historical_adapter_review",
    "write_historical_adapter_review_artifacts",
]
