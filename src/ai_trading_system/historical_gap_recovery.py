from __future__ import annotations

import csv
import math
import re
import shutil
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from pathlib import Path, PurePosixPath

import yaml

from ai_trading_system.daily_input_capture import (
    validate_daily_input_capture_recovery_queue,
)
from ai_trading_system.platform.artifacts import (
    StrictJsonContractError,
    load_strict_json_text,
    sha256_bytes,
    write_bytes_atomic,
    write_json_atomic,
    write_markdown_atomic,
)

SCHEMA_VERSION = "historical_gap_recovery.v1"
VALIDATION_SCHEMA_VERSION = "historical_gap_recovery_validation.v1"
POLICY_SCHEMA_VERSION = "historical_gap_recovery_policy.v1"
STATUS = "HISTORICAL_GAP_RECOVERY_EVIDENCE"
TASK_ID = "OPS-079_HISTORICAL_DAILY_GAP_RECOVERY_EXECUTOR"
DEFAULT_POLICY_RELATIVE_PATH = Path("config/operations/historical_gap_recovery.yaml")
SCHEMA_RELATIVE_PATH = Path("docs/schema/historical_gap_recovery.v1.schema.json")
PAYLOAD_FILENAME = "historical_gap_recovery.json"
MARKDOWN_FILENAME = "historical_gap_recovery.md"
VALIDATION_FILENAME = "historical_gap_recovery_validation.json"
VALIDATION_MARKDOWN_FILENAME = "historical_gap_recovery_validation.md"
GUARD_BEFORE_FILENAME = "canonical_guard_before.json"
GUARD_AFTER_FILENAME = "canonical_guard_after.json"

_RECOVERY_ID_PATTERN = re.compile(r"^daily-input-recovery-[0-9a-f]{20}$")
_OWNER_DECISION_PATTERN = re.compile(r"^owner_decision:[A-Za-z0-9._:-]+$")
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_MARKET_INPUTS: tuple[tuple[str, Path], ...] = (
    ("prices_daily", Path("input/data/raw/prices_daily.csv")),
    ("prices_marketstack_daily", Path("input/data/raw/prices_marketstack_daily.csv")),
    ("rates_daily", Path("input/data/raw/rates_daily.csv")),
    ("download_manifest", Path("input/data/raw/download_manifest.csv")),
)
_CONSUMER_OUTPUT_FIELDS: tuple[str, ...] = (
    "canonical_capture_manifest",
    "canonical_source_state",
    "canonical_data_quality_receipt",
    "daily_score",
    "position",
    "decision_snapshot",
    "dashboard",
    "reader_brief",
    "weekly",
    "governance",
    "promotion",
    "backtest",
    "official_weights",
    "active_shadow_weights",
    "broker_order",
)
_EXCLUSIONS: tuple[str, ...] = (
    "canonical_daily_history_mutation",
    "historical_strict_pit_backfill",
    "consumer_cutover",
    "provider_or_openai_request",
    "score_position_or_decision_generation",
    "dashboard_reader_brief_or_latest_pointer",
    "weekly_governance_promotion_or_backtest",
    "official_or_active_shadow_weight_write",
    "broker_order_or_trading_action",
)
_TOP_LEVEL_KEYS = frozenset(
    {
        "schema_version",
        "status",
        "task_id",
        "owner_decision_id",
        "recovery_id",
        "session_date",
        "component_id",
        "action_status",
        "recovery_mode",
        "generated_at",
        "result_classification",
        "strict_pit_eligible",
        "consumer_cutover_allowed",
        "canonical_history_status",
        "queue_binding",
        "policy_binding",
        "source_snapshot",
        "evidence",
        "consumer_outputs",
        "source_artifacts",
        "canonical_guard_evidence",
        "exclusions",
        "safety",
    }
)


class HistoricalGapRecoveryError(ValueError):
    """Raised when historical gap evidence cannot be materialized safely."""


@dataclass(frozen=True)
class HistoricalGapRecoveryValidation:
    status: str
    checked_at: datetime
    recovery_id: str
    session_date: date
    component_id: str
    checks: tuple[str, ...]
    errors: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return self.status == "PASS"

    def to_payload(self) -> dict[str, object]:
        return {
            "schema_version": VALIDATION_SCHEMA_VERSION,
            "status": self.status,
            "checked_at": self.checked_at.isoformat(),
            "recovery_id": self.recovery_id,
            "session_date": self.session_date.isoformat(),
            "component_id": self.component_id,
            "checks": list(self.checks),
            "errors": list(self.errors),
            "production_effect": "none",
            "broker_action": False,
            "trading_action": False,
        }


@dataclass(frozen=True)
class HistoricalGapRecoveryBuild:
    bundle_path: Path
    payload_path: Path
    markdown_path: Path
    validation_path: Path
    validation: HistoricalGapRecoveryValidation


def default_historical_gap_recovery_output_root(project_root: Path) -> Path:
    return project_root / "outputs" / "replays" / "historical_gap_recovery"


def build_historical_gap_recovery(
    *,
    queue_path: Path,
    queue_validation_path: Path,
    recovery_id: str,
    owner_decision_id: str,
    project_root: Path,
    guard_paths: Sequence[Path],
    inventory_bundle: Path | None = None,
    sec_before_manifest: Path | None = None,
    sec_after_manifest: Path | None = None,
    policy_path: Path | None = None,
    output_root: Path | None = None,
    generated_at: datetime | None = None,
) -> HistoricalGapRecoveryBuild:
    """Build one immutable, isolated recovery-evidence bundle."""

    _validate_recovery_id(recovery_id)
    _validate_owner_decision_id(owner_decision_id)
    timestamp = generated_at or datetime.now(UTC)
    _require_aware_datetime(timestamp, "generated_at")
    root = project_root.resolve(strict=True)
    live_policy_path = _resolve_policy_path(policy_path, project_root=root)
    policy, policy_bytes = _load_policy(live_policy_path)
    queue, queue_bytes, queue_validation, queue_validation_bytes, item = _load_queue_contract(
        queue_path=queue_path,
        queue_validation_path=queue_validation_path,
        recovery_id=recovery_id,
        project_root=root,
        policy=policy,
    )
    session_date = _parse_date(item.get("session_date"), "queue item session_date")
    component_id = _required_text(item.get("component_id"), "component_id")
    contract = _allowed_contract(policy, component_id)
    _validate_queue_item(item, contract=contract)
    _validate_branch_arguments(
        component_id=component_id,
        inventory_bundle=inventory_bundle,
        sec_before_manifest=sec_before_manifest,
        sec_after_manifest=sec_after_manifest,
    )
    guards_before = _capture_guards(guard_paths, project_root=root)

    base_output = (
        output_root.resolve(strict=False)
        if output_root is not None
        else default_historical_gap_recovery_output_root(root)
    )
    _validate_output_root(base_output)
    final_root = base_output / session_date.isoformat() / recovery_id
    if final_root.exists():
        raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_OUTPUT_EXISTS:{final_root}")
    final_root.parent.mkdir(parents=True, exist_ok=True)
    final_root.mkdir()
    try:
        contract_root = final_root / "contract"
        source_root = final_root / "source"
        write_bytes_atomic(contract_root / live_policy_path.name, policy_bytes)
        schema_path = root / SCHEMA_RELATIVE_PATH
        schema_bytes = _read_regular_file(schema_path, "reviewed schema")
        write_bytes_atomic(contract_root / schema_path.name, schema_bytes)
        write_bytes_atomic(source_root / "recovery_queue.json", queue_bytes)
        write_bytes_atomic(
            source_root / "recovery_queue_validation.json",
            queue_validation_bytes,
        )
        write_json_atomic(final_root / GUARD_BEFORE_FILENAME, guards_before)

        if component_id == "market_macro":
            assert inventory_bundle is not None
            source_snapshot, evidence = _freeze_market_inventory(
                inventory_bundle=inventory_bundle,
                bundle_root=final_root,
                session_date=session_date,
                project_root=root,
                policy=policy,
            )
        else:
            assert sec_before_manifest is not None
            assert sec_after_manifest is not None
            source_snapshot, evidence = _freeze_sec_review(
                before_manifest=sec_before_manifest,
                after_manifest=sec_after_manifest,
                bundle_root=final_root,
                session_date=session_date,
                project_root=root,
                policy=policy,
            )

        guards_after = _capture_guards(guard_paths, project_root=root)
        if guards_before != guards_after:
            raise HistoricalGapRecoveryError("HISTORICAL_GAP_CANONICAL_GUARD_CHANGED")
        write_json_atomic(final_root / GUARD_AFTER_FILENAME, guards_after)
        source_artifacts = _source_artifact_records(final_root)
        result_classification = _required_text(
            contract.get("result_classification"),
            "result_classification",
        )
        payload: dict[str, object] = {
            "schema_version": SCHEMA_VERSION,
            "status": STATUS,
            "task_id": TASK_ID,
            "owner_decision_id": owner_decision_id,
            "recovery_id": recovery_id,
            "session_date": session_date.isoformat(),
            "component_id": component_id,
            "action_status": item["action_status"],
            "recovery_mode": item["recovery_mode"],
            "generated_at": timestamp.isoformat(),
            "result_classification": result_classification,
            "strict_pit_eligible": False,
            "consumer_cutover_allowed": False,
            "canonical_history_status": {
                "source_gap_status": item.get("source_gap_status"),
                "source_manifest_path": item.get("source_manifest_path"),
                "old_terminal_state_mutated": False,
                "canonical_capture_mutated": False,
                "unchanged": True,
            },
            "queue_binding": {
                "external_queue_path": _display_path(queue_path, project_root=root),
                "external_validation_path": _display_path(
                    queue_validation_path,
                    project_root=root,
                ),
                "queue_as_of": queue.get("as_of"),
                "queue_policy_version": queue.get("policy_version"),
                "queue_policy_sha256": queue.get("policy_sha256"),
                "item": dict(item),
                "queue_snapshot": _artifact_pointer(
                    source_root / "recovery_queue.json",
                    root=final_root,
                ),
                "validation_snapshot": _artifact_pointer(
                    source_root / "recovery_queue_validation.json",
                    root=final_root,
                ),
                "validation_status": queue_validation.get("status"),
            },
            "policy_binding": {
                "external_policy_path": _display_path(
                    live_policy_path,
                    project_root=root,
                ),
                "policy_id": policy.get("policy_id"),
                "policy_version": policy.get("version"),
                "policy_snapshot": _artifact_pointer(
                    contract_root / live_policy_path.name,
                    root=final_root,
                ),
                "schema_snapshot": _artifact_pointer(
                    contract_root / schema_path.name,
                    root=final_root,
                ),
            },
            "source_snapshot": source_snapshot,
            "evidence": evidence,
            "consumer_outputs": {key: None for key in _CONSUMER_OUTPUT_FIELDS},
            "source_artifacts": source_artifacts,
            "canonical_guard_evidence": {
                "unchanged": True,
                "before": guards_before,
                "after": guards_after,
                "before_snapshot": _artifact_pointer(
                    final_root / GUARD_BEFORE_FILENAME,
                    root=final_root,
                ),
                "after_snapshot": _artifact_pointer(
                    final_root / GUARD_AFTER_FILENAME,
                    root=final_root,
                ),
            },
            "exclusions": list(_EXCLUSIONS),
            "safety": _required_safety_payload(),
        }
        payload_path = final_root / PAYLOAD_FILENAME
        markdown_path = final_root / MARKDOWN_FILENAME
        write_json_atomic(payload_path, payload)
        write_markdown_atomic(markdown_path, render_historical_gap_recovery(payload))

        validation = validate_historical_gap_recovery(
            final_root,
            project_root=root,
            expected_queue_path=queue_path,
            expected_queue_validation_path=queue_validation_path,
            expected_recovery_id=recovery_id,
            expected_owner_decision_id=owner_decision_id,
            expected_guard_paths=guard_paths,
            expected_inventory_bundle=inventory_bundle,
            expected_sec_before_manifest=sec_before_manifest,
            expected_sec_after_manifest=sec_after_manifest,
            policy_path=live_policy_path,
        )
        if not validation.passed:
            raise HistoricalGapRecoveryError(
                "HISTORICAL_GAP_SELF_VALIDATION_FAILED:" + ";".join(validation.errors)
            )
        write_historical_gap_recovery_validation(validation, final_root)
    except Exception:
        if final_root.exists():
            shutil.rmtree(final_root)
        raise

    return HistoricalGapRecoveryBuild(
        bundle_path=final_root,
        payload_path=final_root / PAYLOAD_FILENAME,
        markdown_path=final_root / MARKDOWN_FILENAME,
        validation_path=final_root / VALIDATION_FILENAME,
        validation=validation,
    )


def validate_historical_gap_recovery(
    bundle_path: Path,
    *,
    project_root: Path,
    expected_queue_path: Path,
    expected_queue_validation_path: Path,
    expected_recovery_id: str,
    expected_owner_decision_id: str,
    expected_guard_paths: Sequence[Path],
    expected_inventory_bundle: Path | None = None,
    expected_sec_before_manifest: Path | None = None,
    expected_sec_after_manifest: Path | None = None,
    policy_path: Path | None = None,
) -> HistoricalGapRecoveryValidation:
    """Recompute one recovery bundle from frozen and explicitly bound live bytes."""

    checked_at = datetime.now(UTC)
    checks: list[str] = []
    errors: list[str] = []
    observed_date = date.min
    component_id = "UNKNOWN"
    try:
        _validate_recovery_id(expected_recovery_id)
        _validate_owner_decision_id(expected_owner_decision_id)
        root = _validate_directory(bundle_path, "bundle")
        project = project_root.resolve(strict=True)
        payload, _ = _load_strict_mapping(root / PAYLOAD_FILENAME, PAYLOAD_FILENAME)
        observed_date = _parse_date(payload.get("session_date"), "payload.session_date")
        component_id = _required_text(payload.get("component_id"), "payload.component_id")
        _validate_payload_constants(
            payload,
            expected_recovery_id=expected_recovery_id,
            expected_owner_decision_id=expected_owner_decision_id,
        )
        checks.append("payload_schema_identity_and_safety_constants")

        live_policy_path = _resolve_policy_path(policy_path, project_root=project)
        policy, policy_bytes = _load_policy(live_policy_path)
        policy_snapshot = root / "contract" / live_policy_path.name
        if _read_regular_file(policy_snapshot, "policy snapshot") != policy_bytes:
            raise HistoricalGapRecoveryError("HISTORICAL_GAP_POLICY_SNAPSHOT_DRIFT")
        schema_path = project / SCHEMA_RELATIVE_PATH
        schema_snapshot = root / "contract" / schema_path.name
        if _read_regular_file(schema_snapshot, "schema snapshot") != _read_regular_file(
            schema_path,
            "live schema",
        ):
            raise HistoricalGapRecoveryError("HISTORICAL_GAP_SCHEMA_SNAPSHOT_DRIFT")
        _validate_schema_snapshot(schema_snapshot)
        _validate_policy_binding(payload, root=root, policy=policy, policy_path=live_policy_path)
        checks.append("reviewed_policy_and_schema_binding")

        queue, _, queue_validation, _, item = _load_queue_contract(
            queue_path=expected_queue_path,
            queue_validation_path=expected_queue_validation_path,
            recovery_id=expected_recovery_id,
            project_root=project,
            policy=policy,
        )
        frozen_queue, frozen_queue_bytes = _load_strict_mapping(
            root / "source/recovery_queue.json",
            "frozen recovery queue",
        )
        frozen_validation, frozen_validation_bytes = _load_strict_mapping(
            root / "source/recovery_queue_validation.json",
            "frozen recovery queue validation",
        )
        if frozen_queue_bytes != _read_regular_file(expected_queue_path, "live queue"):
            raise HistoricalGapRecoveryError("HISTORICAL_GAP_QUEUE_SOURCE_DRIFT")
        if frozen_validation_bytes != _read_regular_file(
            expected_queue_validation_path,
            "live queue validation",
        ):
            raise HistoricalGapRecoveryError("HISTORICAL_GAP_QUEUE_VALIDATION_SOURCE_DRIFT")
        if frozen_queue != queue or frozen_validation != queue_validation:
            raise HistoricalGapRecoveryError("HISTORICAL_GAP_QUEUE_SNAPSHOT_DRIFT")
        _validate_queue_binding(
            payload,
            root=root,
            queue=queue,
            queue_validation=queue_validation,
            item=item,
            project_root=project,
            queue_path=expected_queue_path,
            queue_validation_path=expected_queue_validation_path,
        )
        contract = _allowed_contract(policy, component_id)
        _validate_queue_item(item, contract=contract)
        checks.append("content_derived_queue_and_recovery_item_binding")

        artifact_map = _validate_source_artifacts(payload, root=root)
        _validate_bundle_membership(root, artifact_map=artifact_map)
        checks.append("source_hash_size_and_member_allowlist")

        _validate_branch_arguments(
            component_id=component_id,
            inventory_bundle=expected_inventory_bundle,
            sec_before_manifest=expected_sec_before_manifest,
            sec_after_manifest=expected_sec_after_manifest,
        )
        if component_id == "market_macro":
            assert expected_inventory_bundle is not None
            expected_snapshot, expected_evidence = _derive_frozen_market_inventory(
                inventory_bundle=expected_inventory_bundle,
                bundle_root=root,
                session_date=observed_date,
                project_root=project,
                policy=policy,
                freeze=False,
            )
        else:
            assert expected_sec_before_manifest is not None
            assert expected_sec_after_manifest is not None
            expected_snapshot, expected_evidence = _derive_frozen_sec_review(
                before_manifest=expected_sec_before_manifest,
                after_manifest=expected_sec_after_manifest,
                bundle_root=root,
                session_date=observed_date,
                project_root=project,
                policy=policy,
                freeze=False,
            )
        if payload.get("source_snapshot") != expected_snapshot:
            raise HistoricalGapRecoveryError("HISTORICAL_GAP_SOURCE_SNAPSHOT_DRIFT")
        if payload.get("evidence") != expected_evidence:
            raise HistoricalGapRecoveryError("HISTORICAL_GAP_DERIVED_EVIDENCE_DRIFT")
        checks.append("branch_source_and_derived_evidence_recomputed")

        live_guards = _capture_guards(expected_guard_paths, project_root=project)
        guard_before = _load_strict_list(
            root / GUARD_BEFORE_FILENAME,
            GUARD_BEFORE_FILENAME,
        )
        guard_after = _load_strict_list(
            root / GUARD_AFTER_FILENAME,
            GUARD_AFTER_FILENAME,
        )
        guard_payload = _require_mapping(
            payload.get("canonical_guard_evidence"),
            "canonical_guard_evidence",
        )
        if guard_before != live_guards or guard_after != live_guards:
            raise HistoricalGapRecoveryError("HISTORICAL_GAP_CANONICAL_GUARD_DRIFT")
        if guard_payload.get("before") != live_guards or guard_payload.get("after") != live_guards:
            raise HistoricalGapRecoveryError("HISTORICAL_GAP_GUARD_PAYLOAD_DRIFT")
        if guard_payload.get("unchanged") is not True:
            raise HistoricalGapRecoveryError("HISTORICAL_GAP_GUARD_UNCHANGED_FALSE")
        _validate_pointer(
            guard_payload.get("before_snapshot"),
            path=root / GUARD_BEFORE_FILENAME,
            root=root,
        )
        _validate_pointer(
            guard_payload.get("after_snapshot"),
            path=root / GUARD_AFTER_FILENAME,
            root=root,
        )
        checks.append("canonical_guard_before_after_and_live_identity")

        expected_markdown = render_historical_gap_recovery(payload)
        actual_markdown = _read_regular_file(root / MARKDOWN_FILENAME, MARKDOWN_FILENAME)
        if actual_markdown != expected_markdown.encode("utf-8"):
            raise HistoricalGapRecoveryError("HISTORICAL_GAP_MARKDOWN_DRIFT")
        _validate_existing_validation_views(root)
        checks.append("markdown_and_validation_views")
    except Exception as exc:
        errors.append(str(exc))

    return HistoricalGapRecoveryValidation(
        status="PASS" if not errors else "FAIL",
        checked_at=checked_at,
        recovery_id=expected_recovery_id,
        session_date=observed_date,
        component_id=component_id,
        checks=tuple(checks),
        errors=tuple(errors),
    )


def write_historical_gap_recovery_validation(
    validation: HistoricalGapRecoveryValidation,
    bundle_path: Path,
) -> tuple[Path, Path]:
    json_path = bundle_path / VALIDATION_FILENAME
    markdown_path = bundle_path / VALIDATION_MARKDOWN_FILENAME
    payload = validation.to_payload()
    write_json_atomic(json_path, payload)
    write_markdown_atomic(markdown_path, render_historical_gap_validation(payload))
    return json_path, markdown_path


def render_historical_gap_recovery(payload: Mapping[str, object]) -> str:
    evidence = _require_mapping(payload.get("evidence"), "evidence")
    safety = _require_mapping(payload.get("safety"), "safety")
    lines = [
        "# Historical Gap Recovery Evidence",
        "",
        f"- Recovery ID：`{payload.get('recovery_id')}`",
        f"- Session：`{payload.get('session_date')}`",
        f"- Component：`{payload.get('component_id')}`",
        f"- Classification：`{payload.get('result_classification')}`",
        f"- Owner decision：`{payload.get('owner_decision_id')}`",
        "- Canonical history mutated：`false`",
        "- Strict PIT eligible：`false`",
        "- Consumer cutover allowed：`false`",
        "",
        "## Evidence",
        "",
    ]
    if payload.get("component_id") == "market_macro":
        lines.extend(
            [
                f"- Primary exact-date rows：`{evidence.get('primary_exact_date_rows')}`",
                f"- Secondary exact-date rows：`{evidence.get('secondary_exact_date_rows')}`",
                f"- Rates exact-date rows：`{evidence.get('rates_exact_date_rows')}`",
                "- Interpretation：isolated immutable market/macro fact evidence only.",
            ]
        )
    else:
        lines.extend(
            [
                f"- Compared tickers：`{evidence.get('ticker_count')}`",
                f"- Identical payload hashes：`{evidence.get('identical_payload_sha256_count')}`",
                f"- Contemporaneous evidence：`{evidence.get('contemporaneous_evidence_status')}`",
                "- Interpretation：post-cutoff equality is non-PIT review evidence only.",
            ]
        )
    lines.extend(
        [
            "",
            "## Safety",
            "",
            f"- Production effect：`{safety.get('production_effect')}`",
            "- Provider request performed：`"
            f"{str(safety.get('provider_request_performed')).lower()}`",
            f"- OpenAI request performed：`{str(safety.get('openai_request_performed')).lower()}`",
            f"- Broker action taken：`{str(safety.get('broker_action_taken')).lower()}`",
            f"- Trading action taken：`{str(safety.get('trading_action_taken')).lower()}`",
            "",
        ]
    )
    return "\n".join(lines)


def render_historical_gap_validation(payload: Mapping[str, object]) -> str:
    checks = _require_sequence(payload.get("checks"), "validation.checks")
    errors = _require_sequence(payload.get("errors"), "validation.errors")
    lines = [
        "# Historical Gap Recovery Validation",
        "",
        f"- Status：`{payload.get('status')}`",
        f"- Recovery ID：`{payload.get('recovery_id')}`",
        f"- Session：`{payload.get('session_date')}`",
        f"- Component：`{payload.get('component_id')}`",
        f"- Checks：`{len(checks)}`",
        f"- Errors：`{len(errors)}`",
        "- Production effect：`none`",
        "",
    ]
    for error in errors:
        lines.append(f"- Error：`{error}`")
    if errors:
        lines.append("")
    return "\n".join(lines)


def _freeze_market_inventory(**kwargs: object) -> tuple[dict[str, object], dict[str, object]]:
    return _derive_frozen_market_inventory(**kwargs, freeze=True)  # type: ignore[arg-type]


def _derive_frozen_market_inventory(
    *,
    inventory_bundle: Path,
    bundle_root: Path,
    session_date: date,
    project_root: Path,
    policy: Mapping[str, object],
    freeze: bool,
) -> tuple[dict[str, object], dict[str, object]]:
    inventory_root = _validate_directory(inventory_bundle, "inventory bundle")
    replay, replay_bytes = _load_strict_mapping(
        inventory_root / "replay_run.json",
        "inventory replay_run.json",
    )
    manifest, manifest_bytes = _load_strict_list_with_bytes(
        inventory_root / "input_freeze_manifest.json",
        "inventory input_freeze_manifest.json",
    )
    source_contract = _require_mapping(policy.get("source_contract"), "source_contract")
    for field, expected in (
        ("status", source_contract.get("market_inventory_status")),
        ("mode", source_contract.get("market_inventory_mode")),
        ("inventory_only", source_contract.get("market_inventory_only")),
        ("as_of", session_date.isoformat()),
    ):
        if replay.get(field) != expected:
            raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_MARKET_INVENTORY_FIELD:{field}")
    records = _records_by_artifact_id(manifest)
    replay_records = _records_by_artifact_id(
        _require_sequence(replay.get("input_records"), "replay.input_records")
    )
    if records != replay_records:
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_MARKET_INVENTORY_RECORD_DRIFT")
    market_root = bundle_root / "source/market"
    if freeze:
        write_bytes_atomic(market_root / "replay_run.json", replay_bytes)
        write_bytes_atomic(market_root / "input_freeze_manifest.json", manifest_bytes)
    else:
        if _read_regular_file(market_root / "replay_run.json", "frozen replay") != replay_bytes:
            raise HistoricalGapRecoveryError("HISTORICAL_GAP_MARKET_REPLAY_SOURCE_DRIFT")
        if (
            _read_regular_file(
                market_root / "input_freeze_manifest.json",
                "frozen manifest",
            )
            != manifest_bytes
        ):
            raise HistoricalGapRecoveryError("HISTORICAL_GAP_MARKET_MANIFEST_SOURCE_DRIFT")

    frozen_inputs: dict[str, Path] = {}
    for artifact_id, relative in _MARKET_INPUTS:
        record = records.get(artifact_id)
        if record is None or not str(record.get("status", "")).startswith("PASS"):
            raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_MARKET_INPUT_NOT_PASS:{artifact_id}")
        source_path = _contained_member(inventory_root, relative)
        source_bytes = _read_regular_file(source_path, artifact_id)
        _require_record_hash_size(source_bytes, record=record, label=artifact_id)
        frozen_path = market_root / relative
        if freeze:
            write_bytes_atomic(frozen_path, source_bytes)
        elif _read_regular_file(frozen_path, f"frozen {artifact_id}") != source_bytes:
            raise HistoricalGapRecoveryError(
                f"HISTORICAL_GAP_MARKET_INPUT_SOURCE_DRIFT:{artifact_id}"
            )
        frozen_inputs[artifact_id] = frozen_path

    primary = _price_facts(frozen_inputs["prices_daily"], session_date=session_date)
    secondary = _price_facts(
        frozen_inputs["prices_marketstack_daily"],
        session_date=session_date,
    )
    rates = _rate_facts(frozen_inputs["rates_daily"], session_date=session_date)
    source_snapshot: dict[str, object] = {
        "kind": "cache_only_market_macro_inventory",
        "external_inventory_path": _display_path(inventory_root, project_root=project_root),
        "run_id": replay.get("run_id"),
        "status": replay.get("status"),
        "mode": replay.get("mode"),
        "inventory_only": replay.get("inventory_only"),
        "visible_at": replay.get("visible_at"),
        "cutoff_policy": replay.get("cutoff_policy"),
        "replay_snapshot": _artifact_pointer(market_root / "replay_run.json", root=bundle_root),
        "manifest_snapshot": _artifact_pointer(
            market_root / "input_freeze_manifest.json",
            root=bundle_root,
        ),
        "input_snapshots": {
            artifact_id: _artifact_pointer(path, root=bundle_root)
            for artifact_id, path in sorted(frozen_inputs.items())
        },
    }
    evidence = {
        "classification": "IMMUTABLE_RAW_BACKFILL_EVIDENCE",
        "primary_exact_date_rows": len(primary),
        "secondary_exact_date_rows": len(secondary),
        "rates_exact_date_rows": len(rates),
        "primary_tickers": sorted(primary),
        "secondary_tickers": sorted(secondary),
        "rate_series": sorted(rates),
        "strict_pit_eligible": False,
        "consumer_cutover_allowed": False,
        "canonical_daily_evidence_status": "MISSING_OR_PARTIAL_UNCHANGED",
    }
    return source_snapshot, evidence


def _freeze_sec_review(**kwargs: object) -> tuple[dict[str, object], dict[str, object]]:
    return _derive_frozen_sec_review(**kwargs, freeze=True)  # type: ignore[arg-type]


def _derive_frozen_sec_review(
    *,
    before_manifest: Path,
    after_manifest: Path,
    bundle_root: Path,
    session_date: date,
    project_root: Path,
    policy: Mapping[str, object],
    freeze: bool,
) -> tuple[dict[str, object], dict[str, object]]:
    before, before_bytes = _load_strict_mapping(before_manifest, "SEC before manifest")
    after, after_bytes = _load_strict_mapping(after_manifest, "SEC after manifest")
    before_date = _parse_date(before.get("as_of"), "SEC before as_of")
    after_date = _parse_date(after.get("as_of"), "SEC after as_of")
    if not before_date < session_date < after_date:
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_SEC_DATE_BRACKET_INVALID")
    cutoff = datetime.combine(session_date, time.max, tzinfo=UTC)
    before_captured = _parse_datetime(before.get("captured_at"), "SEC before captured_at")
    after_captured = _parse_datetime(after.get("captured_at"), "SEC after captured_at")
    if before_captured > cutoff or after_captured <= cutoff:
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_SEC_CUTOFF_ORDER_INVALID")
    source_contract = _require_mapping(policy.get("source_contract"), "source_contract")
    if source_contract.get("sec_decision_cutoff_policy") != "end_of_session_date_utc":
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_SEC_CUTOFF_POLICY_INVALID")

    before_component = _sec_component(before)
    after_component = _sec_component(after)
    before_records = _sec_payload_records(before_component)
    after_records = _sec_payload_records(after_component)
    if set(before_records) != set(after_records) or not before_records:
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_SEC_TICKER_SET_DRIFT")
    sec_root = bundle_root / "source/sec"
    if freeze:
        write_bytes_atomic(sec_root / "before_capture_manifest.json", before_bytes)
        write_bytes_atomic(sec_root / "after_capture_manifest.json", after_bytes)
    else:
        if (
            _read_regular_file(
                sec_root / "before_capture_manifest.json",
                "frozen SEC before manifest",
            )
            != before_bytes
        ):
            raise HistoricalGapRecoveryError("HISTORICAL_GAP_SEC_BEFORE_MANIFEST_DRIFT")
        if (
            _read_regular_file(
                sec_root / "after_capture_manifest.json",
                "frozen SEC after manifest",
            )
            != after_bytes
        ):
            raise HistoricalGapRecoveryError("HISTORICAL_GAP_SEC_AFTER_MANIFEST_DRIFT")

    comparisons: list[dict[str, object]] = []
    for ticker in sorted(before_records):
        before_record = before_records[ticker]
        after_record = after_records[ticker]
        before_source = _project_member(project_root, before_record["path"])
        after_source = _project_member(project_root, after_record["path"])
        before_content = _read_regular_file(before_source, f"SEC before {ticker}")
        after_content = _read_regular_file(after_source, f"SEC after {ticker}")
        _require_record_hash_size(before_content, record=before_record, label=f"before:{ticker}")
        _require_record_hash_size(after_content, record=after_record, label=f"after:{ticker}")
        before_frozen = sec_root / "before" / before_source.name
        after_frozen = sec_root / "after" / after_source.name
        if freeze:
            write_bytes_atomic(before_frozen, before_content)
            write_bytes_atomic(after_frozen, after_content)
        else:
            if _read_regular_file(before_frozen, f"frozen before {ticker}") != before_content:
                raise HistoricalGapRecoveryError(
                    f"HISTORICAL_GAP_SEC_BEFORE_PAYLOAD_DRIFT:{ticker}"
                )
            if _read_regular_file(after_frozen, f"frozen after {ticker}") != after_content:
                raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_SEC_AFTER_PAYLOAD_DRIFT:{ticker}")
        comparisons.append(
            {
                "ticker": ticker,
                "before_sha256": sha256_bytes(before_content),
                "after_sha256": sha256_bytes(after_content),
                "identical": before_content == after_content,
                "before_snapshot": _artifact_pointer(before_frozen, root=bundle_root),
                "after_snapshot": _artifact_pointer(after_frozen, root=bundle_root),
            }
        )
    identical_count = sum(1 for row in comparisons if row["identical"] is True)
    source_snapshot: dict[str, object] = {
        "kind": "sec_companyfacts_before_after_non_pit_review",
        "external_before_manifest_path": _display_path(
            before_manifest,
            project_root=project_root,
        ),
        "external_after_manifest_path": _display_path(
            after_manifest,
            project_root=project_root,
        ),
        "before_as_of": before_date.isoformat(),
        "after_as_of": after_date.isoformat(),
        "before_captured_at": before_captured.isoformat(),
        "after_captured_at": after_captured.isoformat(),
        "decision_cutoff": cutoff.isoformat(),
        "before_manifest_snapshot": _artifact_pointer(
            sec_root / "before_capture_manifest.json",
            root=bundle_root,
        ),
        "after_manifest_snapshot": _artifact_pointer(
            sec_root / "after_capture_manifest.json",
            root=bundle_root,
        ),
        "payload_comparisons": comparisons,
    }
    evidence = {
        "classification": "MANUAL_NON_PIT_RAW_REVIEW",
        "ticker_count": len(comparisons),
        "identical_payload_sha256_count": identical_count,
        "changed_payload_sha256_count": len(comparisons) - identical_count,
        "contemporaneous_evidence_status": "MISSING",
        "strict_pit_eligible": False,
        "consumer_cutover_allowed": False,
        "interpretation": ("before_after_payload_comparison_only_after_capture_is_post_cutoff"),
    }
    return source_snapshot, evidence


def _load_queue_contract(
    *,
    queue_path: Path,
    queue_validation_path: Path,
    recovery_id: str,
    project_root: Path,
    policy: Mapping[str, object],
) -> tuple[dict[str, object], bytes, dict[str, object], bytes, Mapping[str, object]]:
    queue, queue_bytes = _load_strict_mapping(queue_path, "recovery queue")
    stored_validation, validation_bytes = _load_strict_mapping(
        queue_validation_path,
        "recovery queue validation",
    )
    queue_contract = _require_mapping(policy.get("queue_contract"), "queue_contract")
    if queue.get("schema_version") != queue_contract.get("schema_version"):
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_QUEUE_SCHEMA_INVALID")
    daily_policy_value = _required_text(queue.get("policy_path"), "queue.policy_path")
    daily_policy_path = _project_member(project_root, daily_policy_value)
    computed = validate_daily_input_capture_recovery_queue(
        queue_path,
        project_root=project_root,
        policy_path=daily_policy_path,
    )
    if computed != stored_validation or computed.get("status") != "PASS":
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_QUEUE_VALIDATION_NOT_PASS")
    for field in (
        "automatic_execution_allowed",
        "historical_strict_pit_backfill_allowed",
        "consumer_cutover_allowed",
        "old_terminal_state_mutation_allowed",
    ):
        if queue.get(field) is not False or queue_contract.get(field) is not False:
            raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_QUEUE_SAFETY:{field}")
    items = _require_sequence(queue.get("items"), "queue.items")
    selected = [
        _require_mapping(raw, "queue item")
        for raw in items
        if isinstance(raw, dict) and raw.get("recovery_id") == recovery_id
    ]
    if len(selected) != 1:
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_RECOVERY_ITEM_NOT_UNIQUE")
    return queue, queue_bytes, stored_validation, validation_bytes, selected[0]


def _load_policy(path: Path) -> tuple[dict[str, object], bytes]:
    content = _read_regular_file(path, "historical gap recovery policy")
    try:
        payload = yaml.safe_load(content.decode("utf-8"))
    except (UnicodeDecodeError, yaml.YAMLError) as exc:
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_POLICY_UNREADABLE") from exc
    if not isinstance(payload, dict):
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_POLICY_NOT_MAPPING")
    if payload.get("schema_version") != POLICY_SCHEMA_VERSION:
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_POLICY_SCHEMA_INVALID")
    if payload.get("status") != "OWNER_APPROVED_ENFORCED":
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_POLICY_NOT_APPROVED")
    safety = _require_mapping(payload.get("safety"), "policy.safety")
    for field, expected in _required_safety_payload().items():
        if safety.get(field) != expected:
            raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_POLICY_SAFETY:{field}")
    return payload, content


def _allowed_contract(policy: Mapping[str, object], component_id: str) -> Mapping[str, object]:
    contracts = _require_mapping(policy.get("allowed_contracts"), "allowed_contracts")
    contract = contracts.get(component_id)
    if not isinstance(contract, dict):
        raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_COMPONENT_NOT_ALLOWED:{component_id}")
    return contract


def _validate_queue_item(
    item: Mapping[str, object],
    *,
    contract: Mapping[str, object],
) -> None:
    for field in ("action_status", "recovery_mode"):
        if item.get(field) != contract.get(field):
            raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_QUEUE_ITEM_FIELD:{field}")
    expected_recovery_allowed = item.get("component_id") == "market_macro"
    if item.get("recovery_allowed") is not expected_recovery_allowed:
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_QUEUE_ITEM_RECOVERY_ALLOWED")
    for field, expected in (
        ("automatic_execution_allowed", False),
        ("strict_pit_eligible", False),
        ("consumer_cutover_allowed", False),
        ("production_effect", "none"),
        ("broker_action", False),
        ("trading_action", False),
    ):
        if item.get(field) != expected:
            raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_QUEUE_ITEM_SAFETY:{field}")


def _validate_branch_arguments(
    *,
    component_id: str,
    inventory_bundle: Path | None,
    sec_before_manifest: Path | None,
    sec_after_manifest: Path | None,
) -> None:
    if component_id == "market_macro":
        if (
            inventory_bundle is None
            or sec_before_manifest is not None
            or sec_after_manifest is not None
        ):
            raise HistoricalGapRecoveryError("HISTORICAL_GAP_MARKET_ARGUMENT_CONTRACT")
    elif component_id == "sec_companyfacts":
        if (
            inventory_bundle is not None
            or sec_before_manifest is None
            or sec_after_manifest is None
        ):
            raise HistoricalGapRecoveryError("HISTORICAL_GAP_SEC_ARGUMENT_CONTRACT")
    else:
        raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_COMPONENT_NOT_ALLOWED:{component_id}")


def _validate_payload_constants(
    payload: Mapping[str, object],
    *,
    expected_recovery_id: str,
    expected_owner_decision_id: str,
) -> None:
    if frozenset(payload) != _TOP_LEVEL_KEYS:
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_PAYLOAD_KEYS_INVALID")
    for field, expected in (
        ("schema_version", SCHEMA_VERSION),
        ("status", STATUS),
        ("task_id", TASK_ID),
        ("recovery_id", expected_recovery_id),
        ("owner_decision_id", expected_owner_decision_id),
        ("strict_pit_eligible", False),
        ("consumer_cutover_allowed", False),
    ):
        if payload.get(field) != expected:
            raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_PAYLOAD_CONSTANT:{field}")
    _parse_datetime(payload.get("generated_at"), "payload.generated_at")
    outputs = _require_mapping(payload.get("consumer_outputs"), "consumer_outputs")
    if set(outputs) != set(_CONSUMER_OUTPUT_FIELDS) or set(outputs.values()) != {None}:
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_CONSUMER_OUTPUTS_NOT_NULL")
    if payload.get("exclusions") != list(_EXCLUSIONS):
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_EXCLUSIONS_DRIFT")
    if payload.get("safety") != _required_safety_payload():
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_SAFETY_DRIFT")
    history = _require_mapping(payload.get("canonical_history_status"), "canonical_history_status")
    if (
        history.get("old_terminal_state_mutated") is not False
        or history.get("canonical_capture_mutated") is not False
        or history.get("unchanged") is not True
    ):
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_CANONICAL_HISTORY_PROMOTED")


def _validate_policy_binding(
    payload: Mapping[str, object],
    *,
    root: Path,
    policy: Mapping[str, object],
    policy_path: Path,
) -> None:
    binding = _require_mapping(payload.get("policy_binding"), "policy_binding")
    if binding.get("policy_id") != policy.get("policy_id"):
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_POLICY_ID_DRIFT")
    if binding.get("policy_version") != policy.get("version"):
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_POLICY_VERSION_DRIFT")
    _validate_pointer(
        binding.get("policy_snapshot"),
        path=root / "contract" / policy_path.name,
        root=root,
    )
    _validate_pointer(
        binding.get("schema_snapshot"),
        path=root / "contract" / SCHEMA_RELATIVE_PATH.name,
        root=root,
    )


def _validate_queue_binding(
    payload: Mapping[str, object],
    *,
    root: Path,
    queue: Mapping[str, object],
    queue_validation: Mapping[str, object],
    item: Mapping[str, object],
    project_root: Path,
    queue_path: Path,
    queue_validation_path: Path,
) -> None:
    binding = _require_mapping(payload.get("queue_binding"), "queue_binding")
    expected = {
        "external_queue_path": _display_path(queue_path, project_root=project_root),
        "external_validation_path": _display_path(
            queue_validation_path,
            project_root=project_root,
        ),
        "queue_as_of": queue.get("as_of"),
        "queue_policy_version": queue.get("policy_version"),
        "queue_policy_sha256": queue.get("policy_sha256"),
        "item": dict(item),
        "validation_status": queue_validation.get("status"),
    }
    for field, value in expected.items():
        if binding.get(field) != value:
            raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_QUEUE_BINDING:{field}")
    _validate_pointer(
        binding.get("queue_snapshot"),
        path=root / "source/recovery_queue.json",
        root=root,
    )
    _validate_pointer(
        binding.get("validation_snapshot"),
        path=root / "source/recovery_queue_validation.json",
        root=root,
    )


def _validate_source_artifacts(
    payload: Mapping[str, object],
    *,
    root: Path,
) -> dict[str, Path]:
    records = _require_sequence(payload.get("source_artifacts"), "source_artifacts")
    result: dict[str, Path] = {}
    for raw in records:
        record = _require_mapping(raw, "source artifact")
        path_value = _required_text(record.get("path"), "source artifact path")
        if path_value in result:
            raise HistoricalGapRecoveryError(
                f"HISTORICAL_GAP_SOURCE_ARTIFACT_DUPLICATE:{path_value}"
            )
        path = _bundle_member(root, path_value)
        content = _read_regular_file(path, path_value)
        _require_record_hash_size(content, record=record, label=path_value)
        result[path_value] = path
    expected_prefixes = ("contract/", "source/")
    if not result or any(not path.startswith(expected_prefixes) for path in result):
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_SOURCE_ARTIFACT_SCOPE")
    return result


def _validate_bundle_membership(root: Path, *, artifact_map: Mapping[str, Path]) -> None:
    allowed = set(artifact_map)
    allowed.update(
        {
            PAYLOAD_FILENAME,
            MARKDOWN_FILENAME,
            VALIDATION_FILENAME,
            VALIDATION_MARKDOWN_FILENAME,
            GUARD_BEFORE_FILENAME,
            GUARD_AFTER_FILENAME,
        }
    )
    observed: set[str] = set()
    for path in root.rglob("*"):
        if path.is_symlink():
            raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_BUNDLE_SYMLINK:{path}")
        if path.is_file():
            observed.add(path.relative_to(root).as_posix())
    unexpected = sorted(observed - allowed)
    missing = sorted(set(artifact_map) - observed)
    if unexpected or missing:
        raise HistoricalGapRecoveryError(
            "HISTORICAL_GAP_MEMBER_SET:" + ",".join((*unexpected, *missing))
        )


def _source_artifact_records(root: Path) -> list[dict[str, object]]:
    paths = sorted(
        path
        for prefix in (root / "contract", root / "source")
        for path in prefix.rglob("*")
        if path.is_file()
    )
    return [
        {
            "path": path.relative_to(root).as_posix(),
            "sha256": sha256_bytes(_read_regular_file(path, "source artifact")),
            "size_bytes": path.stat().st_size,
        }
        for path in paths
    ]


def _sec_component(manifest: Mapping[str, object]) -> Mapping[str, object]:
    components = _require_sequence(manifest.get("component_results"), "component_results")
    selected = [
        _require_mapping(raw, "component result")
        for raw in components
        if isinstance(raw, dict) and raw.get("component_id") == "sec_companyfacts"
    ]
    if len(selected) != 1:
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_SEC_COMPONENT_NOT_UNIQUE")
    component = selected[0]
    if (
        component.get("status") != "PASS"
        or component.get("recovery_mode") != "MANUAL_NON_PIT_RAW_REVIEW"
    ):
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_SEC_COMPONENT_NOT_PASS")
    return component


def _sec_payload_records(
    component: Mapping[str, object],
) -> dict[str, Mapping[str, object]]:
    records: dict[str, Mapping[str, object]] = {}
    for raw in _require_sequence(component.get("artifacts"), "SEC artifacts"):
        record = _require_mapping(raw, "SEC artifact")
        path_value = _required_text(record.get("path"), "SEC artifact path")
        name = PurePosixPath(path_value).name
        if not name.endswith("_companyfacts.json"):
            continue
        ticker = name.removesuffix("_companyfacts.json")
        if not ticker or ticker in records:
            raise HistoricalGapRecoveryError("HISTORICAL_GAP_SEC_TICKER_DUPLICATE")
        records[ticker] = record
    return records


def _price_facts(path: Path, *, session_date: date) -> dict[str, dict[str, str]]:
    rows = _csv_rows(path)
    selected: dict[str, dict[str, str]] = {}
    for row in rows:
        if row.get("date") != session_date.isoformat():
            continue
        ticker = str(row.get("ticker") or "").strip()
        if not ticker or ticker in selected:
            raise HistoricalGapRecoveryError("HISTORICAL_GAP_PRICE_TICKER_INVALID")
        close = _finite_number(row.get("close"), f"close:{ticker}")
        if close <= 0:
            raise HistoricalGapRecoveryError("HISTORICAL_GAP_PRICE_NON_POSITIVE")
        selected[ticker] = row
    if not selected:
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_PRICE_DATE_MISSING")
    return selected


def _rate_facts(path: Path, *, session_date: date) -> dict[str, dict[str, str]]:
    rows = _csv_rows(path)
    selected: dict[str, dict[str, str]] = {}
    for row in rows:
        if row.get("date") != session_date.isoformat():
            continue
        series = str(row.get("series") or "").strip()
        if not series or series in selected:
            raise HistoricalGapRecoveryError("HISTORICAL_GAP_RATE_SERIES_INVALID")
        _finite_number(row.get("value"), f"rate:{series}")
        selected[series] = row
    if not selected:
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_RATE_DATE_MISSING")
    return selected


def _csv_rows(path: Path) -> list[dict[str, str]]:
    content = _read_regular_file(path, "CSV input")
    try:
        text = content.decode("utf-8-sig")
        reader = csv.DictReader(text.splitlines())
        rows = [dict(row) for row in reader]
    except (UnicodeDecodeError, csv.Error) as exc:
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_CSV_INVALID") from exc
    if reader.fieldnames is None:
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_CSV_HEADER_MISSING")
    return rows


def _records_by_artifact_id(
    rows: Sequence[object],
) -> dict[str, Mapping[str, object]]:
    result: dict[str, Mapping[str, object]] = {}
    for raw in rows:
        record = _require_mapping(raw, "inventory record")
        artifact_id = _required_text(record.get("artifact_id"), "artifact_id")
        if artifact_id in result:
            raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_INVENTORY_DUPLICATE:{artifact_id}")
        result[artifact_id] = record
    return result


def _required_safety_payload() -> dict[str, object]:
    return {
        "strict_pit_eligible": False,
        "consumer_cutover_allowed": False,
        "canonical_daily_mutation_allowed": False,
        "production_effect": "none",
        "provider_request_performed": False,
        "openai_request_performed": False,
        "official_weight_write": False,
        "active_shadow_weight_write": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "trading_action_taken": False,
    }


def _capture_guards(paths: Sequence[Path], *, project_root: Path) -> list[dict[str, object]]:
    if not paths:
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_GUARD_REQUIRED")
    records: dict[str, dict[str, object]] = {}
    for path in paths:
        content = _read_regular_file(path, "canonical guard")
        display = _display_path(path, project_root=project_root)
        if display in records:
            raise HistoricalGapRecoveryError("HISTORICAL_GAP_GUARD_DUPLICATE")
        records[display] = {
            "path": display,
            "sha256": sha256_bytes(content),
            "size_bytes": len(content),
        }
    return [records[key] for key in sorted(records)]


def _artifact_pointer(path: Path, *, root: Path) -> dict[str, object]:
    content = _read_regular_file(path, "artifact pointer")
    return {
        "path": _member_path(path, root=root),
        "sha256": sha256_bytes(content),
        "size_bytes": len(content),
    }


def _validate_pointer(raw: object, *, path: Path, root: Path) -> None:
    if _require_mapping(raw, "artifact pointer") != _artifact_pointer(path, root=root):
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_ARTIFACT_POINTER_DRIFT")


def _require_record_hash_size(
    content: bytes,
    *,
    record: Mapping[str, object],
    label: str,
) -> None:
    sha = record.get("sha256")
    size = record.get("size_bytes")
    if size is None:
        size = len(content)
    if (
        not isinstance(sha, str)
        or _SHA256_PATTERN.fullmatch(sha) is None
        or sha256_bytes(content) != sha
        or not isinstance(size, int)
        or len(content) != size
    ):
        raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_HASH_SIZE_DRIFT:{label}")


def _validate_schema_snapshot(path: Path) -> None:
    payload, _ = _load_strict_mapping(path, "schema snapshot")
    if (
        payload.get("$schema") != "https://json-schema.org/draft/2020-12/schema"
        or payload.get("type") != "object"
        or payload.get("additionalProperties") is not False
    ):
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_SCHEMA_CONTRACT_INVALID")


def _validate_existing_validation_views(root: Path) -> None:
    json_path = root / VALIDATION_FILENAME
    markdown_path = root / VALIDATION_MARKDOWN_FILENAME
    if json_path.exists() != markdown_path.exists():
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_VALIDATION_VIEW_PARTIAL")
    if not json_path.exists():
        return
    payload, _ = _load_strict_mapping(json_path, VALIDATION_FILENAME)
    if payload.get("schema_version") != VALIDATION_SCHEMA_VERSION:
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_VALIDATION_SCHEMA_DRIFT")
    expected = render_historical_gap_validation(payload).encode("utf-8")
    if _read_regular_file(markdown_path, VALIDATION_MARKDOWN_FILENAME) != expected:
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_VALIDATION_MARKDOWN_DRIFT")


def _load_strict_mapping(path: Path, label: str) -> tuple[dict[str, object], bytes]:
    value, content = _load_strict_json(path, label)
    if not isinstance(value, dict):
        raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_JSON_NOT_MAPPING:{label}")
    return value, content


def _load_strict_list(path: Path, label: str) -> list[object]:
    value, _ = _load_strict_json(path, label)
    if not isinstance(value, list):
        raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_JSON_NOT_LIST:{label}")
    return value


def _load_strict_list_with_bytes(path: Path, label: str) -> tuple[list[object], bytes]:
    value, content = _load_strict_json(path, label)
    if not isinstance(value, list):
        raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_JSON_NOT_LIST:{label}")
    return value, content


def _load_strict_json(path: Path, label: str) -> tuple[object, bytes]:
    content = _read_regular_file(path, label)
    try:
        text = content.decode("utf-8")
        return load_strict_json_text(text, label=label), content
    except (UnicodeDecodeError, StrictJsonContractError) as exc:
        raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_JSON_INVALID:{label}") from exc


def _resolve_policy_path(path: Path | None, *, project_root: Path) -> Path:
    candidate = path if path is not None else project_root / DEFAULT_POLICY_RELATIVE_PATH
    if not candidate.is_absolute():
        candidate = project_root / candidate
    resolved = candidate.resolve(strict=True)
    if not resolved.is_relative_to(project_root):
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_POLICY_OUTSIDE_PROJECT")
    return resolved


def _validate_directory(path: Path, label: str) -> Path:
    if path.is_symlink():
        raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_DIRECTORY_SYMLINK:{label}")
    resolved = path.resolve(strict=True)
    if not resolved.is_dir():
        raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_DIRECTORY_INVALID:{label}")
    return resolved


def _validate_output_root(path: Path) -> None:
    if path.exists() and (not path.is_dir() or path.is_symlink()):
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_OUTPUT_ROOT_INVALID")
    path.mkdir(parents=True, exist_ok=True)


def _contained_member(root: Path, relative: Path) -> Path:
    candidate = (root / relative).resolve(strict=True)
    if not candidate.is_relative_to(root):
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_MEMBER_OUTSIDE_ROOT")
    return candidate


def _project_member(project_root: Path, value: object) -> Path:
    raw = _required_text(value, "project member path")
    pure = PurePosixPath(raw)
    if pure.is_absolute() or ".." in pure.parts or "\\" in raw:
        raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_PROJECT_PATH_INVALID:{raw}")
    candidate = (project_root / Path(*pure.parts)).resolve(strict=True)
    if not candidate.is_relative_to(project_root):
        raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_PROJECT_PATH_ESCAPE:{raw}")
    return candidate


def _bundle_member(root: Path, value: str) -> Path:
    pure = PurePosixPath(value)
    if pure.is_absolute() or ".." in pure.parts or "\\" in value:
        raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_BUNDLE_PATH_INVALID:{value}")
    candidate = (root / Path(*pure.parts)).resolve(strict=True)
    if not candidate.is_relative_to(root):
        raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_BUNDLE_PATH_ESCAPE:{value}")
    return candidate


def _member_path(path: Path, *, root: Path) -> str:
    resolved = path.resolve(strict=True)
    base = root.resolve(strict=True)
    if not resolved.is_relative_to(base):
        raise HistoricalGapRecoveryError("HISTORICAL_GAP_POINTER_OUTSIDE_ROOT")
    return resolved.relative_to(base).as_posix()


def _read_regular_file(path: Path, label: str) -> bytes:
    if path.is_symlink():
        raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_FILE_SYMLINK:{label}")
    resolved = path.resolve(strict=True)
    if not resolved.is_file():
        raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_FILE_INVALID:{label}")
    return resolved.read_bytes()


def _display_path(path: Path, *, project_root: Path) -> str:
    resolved = path.resolve(strict=False)
    if resolved.is_relative_to(project_root):
        return resolved.relative_to(project_root).as_posix()
    return str(resolved)


def _required_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_TEXT_REQUIRED:{label}")
    return value


def _require_mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_MAPPING_REQUIRED:{label}")
    return value


def _require_sequence(value: object, label: str) -> Sequence[object]:
    if not isinstance(value, list):
        raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_LIST_REQUIRED:{label}")
    return value


def _parse_date(value: object, label: str) -> date:
    raw = _required_text(value, label)
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as exc:
        raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_DATE_INVALID:{label}") from exc
    if parsed.isoformat() != raw:
        raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_DATE_NON_CANONICAL:{label}")
    return parsed


def _parse_datetime(value: object, label: str) -> datetime:
    raw = _required_text(value, label)
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_DATETIME_INVALID:{label}") from exc
    _require_aware_datetime(parsed, label)
    return parsed


def _require_aware_datetime(value: datetime, label: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_DATETIME_NAIVE:{label}")


def _finite_number(value: object, label: str) -> float:
    try:
        parsed = float(str(value))
    except (TypeError, ValueError) as exc:
        raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_NUMBER_INVALID:{label}") from exc
    if not math.isfinite(parsed):
        raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_NUMBER_NON_FINITE:{label}")
    return parsed


def _validate_recovery_id(value: str) -> None:
    if _RECOVERY_ID_PATTERN.fullmatch(value) is None:
        raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_RECOVERY_ID_INVALID:{value}")


def _validate_owner_decision_id(value: str) -> None:
    if _OWNER_DECISION_PATTERN.fullmatch(value) is None:
        raise HistoricalGapRecoveryError(f"HISTORICAL_GAP_OWNER_DECISION_ID_INVALID:{value}")
