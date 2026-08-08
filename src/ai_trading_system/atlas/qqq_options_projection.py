from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.contracts.strategy_research_qqq_options_projection import (
    QQQ_OPTIONS_PROJECTION_GROUP_IDS,
    QQQ_OPTIONS_PROJECTION_TASK_IDS,
    QQQOptionsProjectionCard,
    QQQOptionsProjectionGroup,
    StrategyResearchQQQOptionsProjectionBundle,
)

DEFAULT_QQQ_OPTIONS_PROJECTION_POLICY_PATH = "config/atlas/qqq_options_projection.yaml"
QQQ_OPTIONS_PROJECTION_POLICY_SCHEMA = "atlas_qqq_options_projection_policy.v1"
EXPECTED_OWNER_DECISION = (
    "owner_decision:TRADING-2501:2026-08-08:accept_read_only_owner_review_pack_recommendations_v1"
)
EXPECTED_SOURCE_SET_SHA256 = "29c97b0524c0ccf2ce1b215da9122bbfa875f45b08d682145a7409d6c1abd11f"
_EXPECTED_SAFETY: Mapping[str, object] = {
    "investment_conclusion_generated": False,
    "external_action": "none",
    "production_effect": "none",
    "broker_action": "none",
}


class QQQOptionsProjectionError(ValueError):
    pass


@dataclass(frozen=True)
class QQQOptionsProjectionPolicy:
    policy_id: str
    policy_version: str
    owner_decision: str
    primary_research_start: str
    aggregate_conclusion: str
    aggregate_explanation_zh: str
    source_set_sha256: str
    groups: tuple[QQQOptionsProjectionGroup, ...]
    cards: tuple[QQQOptionsProjectionCard, ...]
    safety: Mapping[str, object]
    policy_sha256: str


@dataclass(frozen=True)
class QQQOptionsProjectionValidation:
    schema_version: str
    status: str
    snapshot_id: str
    bundle_sha256: str
    policy_sha256: str
    source_set_sha256: str
    checks: tuple[str, ...]
    errors: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "snapshot_id": self.snapshot_id,
            "bundle_sha256": self.bundle_sha256,
            "policy_sha256": self.policy_sha256,
            "source_set_sha256": self.source_set_sha256,
            "checks": list(self.checks),
            "errors": list(self.errors),
            "investment_conclusion_generated": False,
            "external_action": "none",
            "production_effect": "none",
            "broker_action": "none",
        }


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise QQQOptionsProjectionError(f"QQQ_OPTIONS_PROJECTION_POLICY_MAPPING_REQUIRED:{field}")
    return value


def _exact_keys(payload: Mapping[str, Any], expected: set[str], field: str) -> None:
    actual = set(payload)
    if actual != expected:
        raise QQQOptionsProjectionError(
            f"QQQ_OPTIONS_PROJECTION_POLICY_KEYS_INVALID:{field}:"
            f"missing={sorted(expected - actual)}:extra={sorted(actual - expected)}"
        )


def _list_of_mappings(value: object, field: str) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, list):
        raise QQQOptionsProjectionError(f"QQQ_OPTIONS_PROJECTION_POLICY_LIST_REQUIRED:{field}")
    return tuple(_mapping(item, field) for item in value)


def _git_blob_identity(payload: bytes) -> str:
    header = f"blob {len(payload)}\0".encode("ascii")
    return hashlib.sha1(header + payload, usedforsecurity=False).hexdigest()


def _source_set_sha256(cards: tuple[QQQOptionsProjectionCard, ...]) -> str:
    manifest = "".join(
        f"{card.source.path}|{card.source.git_blob}|{card.source.byte_count}\n" for card in cards
    )
    return hashlib.sha256(manifest.encode("utf-8")).hexdigest()


def load_qqq_options_projection_policy(
    *,
    repository_root: Path,
    policy_path: Path | None = None,
) -> QQQOptionsProjectionPolicy:
    root = repository_root.resolve()
    resolved = (
        root / DEFAULT_QQQ_OPTIONS_PROJECTION_POLICY_PATH
        if policy_path is None
        else policy_path.resolve()
    )
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise QQQOptionsProjectionError("QQQ_OPTIONS_PROJECTION_POLICY_OUTSIDE_REPOSITORY") from exc
    raw = resolved.read_bytes()
    try:
        decoded = yaml.safe_load(raw.decode("utf-8"))
    except (UnicodeDecodeError, yaml.YAMLError) as exc:
        raise QQQOptionsProjectionError("QQQ_OPTIONS_PROJECTION_POLICY_INVALID") from exc
    payload = _mapping(decoded, "policy")
    expected_keys = {
        "schema_version",
        "policy_id",
        "policy_version",
        "owner_decision",
        "primary_research_start",
        "aggregate_conclusion",
        "aggregate_explanation_zh",
        "source_set_sha256",
        "groups",
        "cards",
        "safety",
    }
    _exact_keys(payload, expected_keys, "policy")
    if payload["schema_version"] != QQQ_OPTIONS_PROJECTION_POLICY_SCHEMA:
        raise QQQOptionsProjectionError("QQQ_OPTIONS_PROJECTION_POLICY_SCHEMA_INVALID")
    groups = tuple(
        QQQOptionsProjectionGroup.from_dict(item)
        for item in _list_of_mappings(payload["groups"], "groups")
    )
    cards = tuple(
        QQQOptionsProjectionCard.from_dict(item)
        for item in _list_of_mappings(payload["cards"], "cards")
    )
    if tuple(item.group_id for item in groups) != QQQ_OPTIONS_PROJECTION_GROUP_IDS:
        raise QQQOptionsProjectionError("QQQ_OPTIONS_PROJECTION_POLICY_GROUP_ORDER_INVALID")
    if tuple(item.task_id for item in cards) != QQQ_OPTIONS_PROJECTION_TASK_IDS:
        raise QQQOptionsProjectionError("QQQ_OPTIONS_PROJECTION_POLICY_CARD_ORDER_INVALID")
    if str(payload["owner_decision"]) != EXPECTED_OWNER_DECISION:
        raise QQQOptionsProjectionError("QQQ_OPTIONS_PROJECTION_OWNER_DECISION_INVALID")
    if str(payload["primary_research_start"]) != "2021-02-22":
        raise QQQOptionsProjectionError("QQQ_OPTIONS_PROJECTION_PRIMARY_START_INVALID")
    if str(payload["aggregate_conclusion"]) != "NO_GO_KEEP_BLOCKED":
        raise QQQOptionsProjectionError("QQQ_OPTIONS_PROJECTION_AGGREGATE_NO_GO_REQUIRED")
    if str(payload["source_set_sha256"]) != EXPECTED_SOURCE_SET_SHA256:
        raise QQQOptionsProjectionError("QQQ_OPTIONS_PROJECTION_SOURCE_SET_AUTHORITY_INVALID")
    if _source_set_sha256(cards) != EXPECTED_SOURCE_SET_SHA256:
        raise QQQOptionsProjectionError("QQQ_OPTIONS_PROJECTION_SOURCE_MANIFEST_INVALID")
    safety = _mapping(payload["safety"], "safety")
    if dict(safety) != dict(_EXPECTED_SAFETY):
        raise QQQOptionsProjectionError("QQQ_OPTIONS_PROJECTION_SAFETY_INVALID")
    return QQQOptionsProjectionPolicy(
        policy_id=str(payload["policy_id"]),
        policy_version=str(payload["policy_version"]),
        owner_decision=str(payload["owner_decision"]),
        primary_research_start=str(payload["primary_research_start"]),
        aggregate_conclusion=str(payload["aggregate_conclusion"]),
        aggregate_explanation_zh=str(payload["aggregate_explanation_zh"]),
        source_set_sha256=str(payload["source_set_sha256"]),
        groups=groups,
        cards=cards,
        safety=safety,
        policy_sha256=hashlib.sha256(raw).hexdigest(),
    )


def _validate_exact_sources(
    *, repository_root: Path, cards: tuple[QQQOptionsProjectionCard, ...]
) -> None:
    root = repository_root.resolve()
    for card in cards:
        source_path = (root / card.source.path).resolve()
        try:
            source_path.relative_to(root)
        except ValueError as exc:
            raise QQQOptionsProjectionError(
                f"QQQ_OPTIONS_PROJECTION_SOURCE_OUTSIDE_REPOSITORY:{card.task_id}"
            ) from exc
        if not source_path.is_file():
            raise QQQOptionsProjectionError(f"QQQ_OPTIONS_PROJECTION_SOURCE_MISSING:{card.task_id}")
        raw = source_path.read_bytes()
        if len(raw) != card.source.byte_count:
            raise QQQOptionsProjectionError(
                f"QQQ_OPTIONS_PROJECTION_SOURCE_BYTE_COUNT_DRIFT:{card.task_id}"
            )
        if _git_blob_identity(raw) != card.source.git_blob:
            raise QQQOptionsProjectionError(
                f"QQQ_OPTIONS_PROJECTION_SOURCE_BLOB_DRIFT:{card.task_id}"
            )


def build_qqq_options_projection(
    *,
    repository_root: Path,
    snapshot_id: str,
    policy: QQQOptionsProjectionPolicy | None = None,
) -> StrategyResearchQQQOptionsProjectionBundle:
    selected = policy or load_qqq_options_projection_policy(repository_root=repository_root)
    _validate_exact_sources(repository_root=repository_root, cards=selected.cards)
    bundle = StrategyResearchQQQOptionsProjectionBundle.seal(
        snapshot_id=snapshot_id,
        owner_decision=selected.owner_decision,
        primary_research_start=selected.primary_research_start,
        aggregate_conclusion=selected.aggregate_conclusion,
        aggregate_explanation_zh=selected.aggregate_explanation_zh,
        source_set_sha256=selected.source_set_sha256,
        groups=selected.groups,
        cards=selected.cards,
        policy_id=selected.policy_id,
        policy_version=selected.policy_version,
        policy_sha256=selected.policy_sha256,
        investment_conclusion_generated=False,
        external_action="none",
        production_effect="none",
        broker_action="none",
    )
    replay = StrategyResearchQQQOptionsProjectionBundle.from_json_bytes(bundle.canonical_bytes)
    if replay != bundle:
        raise QQQOptionsProjectionError("QQQ_OPTIONS_PROJECTION_CANONICAL_REPLAY_MISMATCH")
    return bundle


def validate_qqq_options_projection(
    *,
    repository_root: Path,
    bundle: StrategyResearchQQQOptionsProjectionBundle,
    policy: QQQOptionsProjectionPolicy | None = None,
) -> QQQOptionsProjectionValidation:
    selected = policy or load_qqq_options_projection_policy(repository_root=repository_root)
    expected = build_qqq_options_projection(
        repository_root=repository_root,
        snapshot_id=bundle.snapshot_id,
        policy=selected,
    )
    if bundle.canonical_bytes != expected.canonical_bytes:
        raise QQQOptionsProjectionError("QQQ_OPTIONS_PROJECTION_POLICY_DRIFT")
    return QQQOptionsProjectionValidation(
        schema_version="atlas_qqq_options_projection_validation.v1",
        status="PASS",
        snapshot_id=bundle.snapshot_id,
        bundle_sha256=bundle.content_sha256,
        policy_sha256=selected.policy_sha256,
        source_set_sha256=selected.source_set_sha256,
        checks=(
            "THIRTEEN_EXACT_SOURCES_MATCH",
            "OWNER_ACCEPTED_LAYERS_MATCH",
            "FOUR_READER_GROUPS_MATCH",
            "FIVE_STATUS_LAYERS_PRESENT",
            "TRADING_2489_SOURCE_STATUS_MISMATCH_DISCLOSED",
            "TRADING_2492_NO_GO_CAP_BEFORE_ORDER_FILL",
            "TRADING_2493_AGGREGATE_NO_GO_DOMINATES",
            "PRIMARY_RESEARCH_START_MATCH",
            "CANONICAL_REPLAY_MATCH",
            "NO_STRATEGY_PASS_OR_EXTERNAL_ACTION",
        ),
    )


__all__ = [
    "DEFAULT_QQQ_OPTIONS_PROJECTION_POLICY_PATH",
    "EXPECTED_OWNER_DECISION",
    "EXPECTED_SOURCE_SET_SHA256",
    "QQQOptionsProjectionError",
    "QQQOptionsProjectionPolicy",
    "QQQOptionsProjectionValidation",
    "build_qqq_options_projection",
    "load_qqq_options_projection_policy",
    "validate_qqq_options_projection",
]
