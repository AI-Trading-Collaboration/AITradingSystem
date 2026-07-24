from __future__ import annotations

import re
import shutil
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from pathlib import Path, PurePosixPath

import pandas as pd

from ai_trading_system.config import (
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_universe,
)
from ai_trading_system.data.quality import (
    DataFileSummary,
    DataQualityReport,
    DownloadPublicationResolution,
    DownloadPublicationResolutionStatus,
    Severity,
    marketstack_reconciliation_path,
    render_data_quality_report,
    validate_data_cache,
    write_data_quality_report,
)
from ai_trading_system.platform.artifacts import (
    StrictJsonContractError,
    load_strict_json_text,
    sha256_bytes,
    write_bytes_atomic,
    write_json_atomic,
    write_markdown_atomic,
    write_text_atomic,
)
from ai_trading_system.trading_calendar import is_us_equity_trading_day

SCHEMA_VERSION = "limited_non_pit_reconstruction.v2"
VALIDATION_SCHEMA_VERSION = "limited_non_pit_reconstruction_validation.v1"
STATUS = "LIMITED_NON_PIT_RECONSTRUCTION"
TASK_ID = "OPS-068_2026_07_21_DAILY_GAP_RECOVERY"
PROJECT_DEFAULT_RESEARCH_START = date(2021, 2, 22)
SCHEMA_RELATIVE_PATH = Path("docs/schema/limited_non_pit_reconstruction.v2.schema.json")
PAYLOAD_FILENAME = "limited_non_pit_reconstruction.json"
MARKDOWN_FILENAME = "limited_non_pit_reconstruction.md"
VALIDATION_FILENAME = "limited_non_pit_reconstruction_validation.json"
VALIDATION_MARKDOWN_FILENAME = "limited_non_pit_reconstruction_validation.md"

_BUNDLE_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,159}$")
_OWNER_DECISION_PATTERN = re.compile(r"^owner_decision:[A-Za-z0-9._:-]+$")
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_INPUT_SPECS: tuple[tuple[str, str, Path], ...] = (
    ("primary_prices", "prices_daily", Path("input/data/raw/prices_daily.csv")),
    (
        "secondary_prices",
        "prices_marketstack_daily",
        Path("input/data/raw/prices_marketstack_daily.csv"),
    ),
    ("rates", "rates_daily", Path("input/data/raw/rates_daily.csv")),
    ("download_manifest", "download_manifest", Path("input/data/raw/download_manifest.csv")),
)
_STRICT_MISSING_ARTIFACT_IDS: tuple[str, ...] = (
    "fmp_forward_pit_normalized",
    "pit_validation_report",
    "fmp_forward_pit_fetch_report",
    "sec_fundamentals",
    "risk_event_openai_prereview_report",
)
_STRICT_NULL_FIELDS: tuple[str, ...] = (
    "fmp_forward_pit_normalized",
    "pit_validation_report",
    "fmp_forward_pit_fetch_report",
    "sec_fundamentals",
    "openai_prereview_report",
)
_CONCLUSION_NULL_FIELDS: tuple[str, ...] = (
    "daily_score",
    "position",
    "decision_snapshot",
    "dashboard",
    "reader_brief",
    "weekly",
    "governance",
    "promotion",
    "backtest",
    "weights",
    "production",
)
_EXCLUSIONS: tuple[str, ...] = (
    "canonical_daily_result",
    "daily_score_or_position",
    "decision_snapshot_or_dashboard",
    "reader_brief",
    "weekly_or_governance_evidence",
    "promotion_or_backtest_conclusion",
    "production_or_active_shadow_weights",
    "broker_order_or_trading_action",
)
_TOP_LEVEL_KEYS = frozenset(
    {
        "schema_version",
        "status",
        "task_id",
        "owner_decision_id",
        "as_of",
        "generated_at",
        "bundle_id",
        "scope",
        "canonical_daily_evidence_status",
        "reconstruction_conclusion_status",
        "source_inventory",
        "research_window",
        "data_quality",
        "input_summaries",
        "market_snapshot",
        "macro_snapshot",
        "strict_missing_inputs",
        "conclusion_outputs",
        "source_artifacts",
        "canonical_guard_evidence",
        "exclusions",
        "safety",
    }
)


class LimitedNonPitReconstructionError(ValueError):
    """Raised when isolated historical fact evidence cannot be proven safely."""


@dataclass(frozen=True)
class LimitedNonPitValidation:
    status: str
    checked_at: datetime
    bundle_id: str
    as_of: date
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
            "bundle_id": self.bundle_id,
            "as_of": self.as_of.isoformat(),
            "checks": list(self.checks),
            "errors": list(self.errors),
            "production_effect": "none",
        }


@dataclass(frozen=True)
class LimitedNonPitBuild:
    bundle_path: Path
    payload_path: Path
    markdown_path: Path
    validation_path: Path
    validation: LimitedNonPitValidation


def default_limited_non_pit_output_root(project_root: Path) -> Path:
    return project_root / "outputs" / "replays" / "limited_non_pit_reconstruction"


def build_limited_non_pit_reconstruction(
    *,
    inventory_bundle: Path,
    owner_decision_id: str,
    bundle_id: str,
    project_root: Path,
    output_root: Path | None = None,
    guard_paths: Sequence[Path],
    generated_at: datetime | None = None,
) -> LimitedNonPitBuild:
    """Build one isolated, non-canonical market/macro fact bundle."""

    _validate_owner_decision_id(owner_decision_id)
    _validate_bundle_id(bundle_id)
    timestamp = generated_at or datetime.now(UTC)
    _require_aware_datetime(timestamp, "generated_at")
    resolved_project_root = project_root.resolve(strict=True)
    inventory_root = _validate_input_directory(inventory_bundle, "inventory bundle")
    inventory_payload, inventory_bytes = _load_strict_mapping(
        inventory_root / "replay_run.json",
        "source replay_run.json",
    )
    manifest_payload, manifest_bytes = _load_strict_json(
        inventory_root / "input_freeze_manifest.json",
        "source input_freeze_manifest.json",
    )
    as_of, inventory_records = _validate_inventory(
        inventory_payload,
        manifest_payload,
        inventory_root=inventory_root,
    )
    guard_before = _capture_guard_paths(guard_paths, project_root=resolved_project_root)

    base_output = (
        output_root.resolve(strict=False)
        if output_root is not None
        else default_limited_non_pit_output_root(resolved_project_root)
    )
    _validate_output_root(base_output)
    final_root = base_output / as_of.isoformat() / bundle_id
    if final_root.exists():
        raise LimitedNonPitReconstructionError(
            f"LIMITED_NON_PIT_OUTPUT_EXISTS: {final_root}"
        )
    final_root.parent.mkdir(parents=True, exist_ok=True)
    staging_root = final_root
    if staging_root.exists():
        raise LimitedNonPitReconstructionError(
            f"LIMITED_NON_PIT_STAGING_EXISTS: {staging_root}"
        )
    staging_root.mkdir()
    try:
        schema_path = resolved_project_root / SCHEMA_RELATIVE_PATH
        schema_bytes = _read_regular_file(schema_path, "reviewed schema")
        write_bytes_atomic(staging_root / "contract" / schema_path.name, schema_bytes)
        write_bytes_atomic(staging_root / "source" / "replay_run.json", inventory_bytes)
        write_bytes_atomic(
            staging_root / "source" / "input_freeze_manifest.json",
            manifest_bytes,
        )

        for _, artifact_id, member_path in _INPUT_SPECS:
            source_path = _inventory_member_path(inventory_root, member_path)
            source_bytes = _read_regular_file(source_path, artifact_id)
            record = inventory_records[artifact_id]
            _require_hash_and_size(
                source_bytes,
                expected_sha256=record.get("sha256"),
                label=artifact_id,
            )
            write_bytes_atomic(staging_root / member_path, source_bytes)

        quality_report = _run_isolated_data_quality(
            staging_root,
            as_of=as_of,
        )
        if not quality_report.passed:
            error_codes = sorted(
                issue.code
                for issue in quality_report.issues
                if issue.severity == Severity.ERROR
            )
            raise LimitedNonPitReconstructionError(
                "LIMITED_NON_PIT_DATA_QUALITY_FAILED: " + ",".join(error_codes)
            )
        quality_report_path = _quality_report_path(staging_root, as_of)
        write_data_quality_report(quality_report, quality_report_path)
        reconciliation_path = marketstack_reconciliation_path(quality_report_path)
        write_text_atomic(
            reconciliation_path,
            _render_marketstack_reconciliation_csv(quality_report),
        )

        universe = load_universe()
        expected_tickers = configured_price_tickers(
            universe,
            include_full_ai_chain=False,
        )
        expected_series = configured_rate_series(universe)
        input_summaries = _input_summaries(quality_report, staging_root=staging_root)
        market_snapshot = _market_snapshot(
            staging_root / "input/data/raw/prices_daily.csv",
            as_of=as_of,
            expected_tickers=expected_tickers,
        )
        macro_snapshot = _macro_snapshot(
            staging_root / "input/data/raw/rates_daily.csv",
            as_of=as_of,
            expected_series=expected_series,
        )
        guard_after = _capture_guard_paths(guard_paths, project_root=resolved_project_root)
        if guard_before != guard_after:
            raise LimitedNonPitReconstructionError(
                "LIMITED_NON_PIT_CANONICAL_GUARD_CHANGED"
            )

        source_artifacts = _source_artifacts(staging_root)
        payload: dict[str, object] = {
            "schema_version": SCHEMA_VERSION,
            "status": STATUS,
            "task_id": TASK_ID,
            "owner_decision_id": owner_decision_id,
            "as_of": as_of.isoformat(),
            "generated_at": timestamp.isoformat(),
            "bundle_id": bundle_id,
            "scope": "checksum_backed_market_macro_facts_only",
            "canonical_daily_evidence_status": "MISSING",
            "reconstruction_conclusion_status": "INSUFFICIENT_DATA",
            "source_inventory": {
                "source_bundle_path": _display_path(
                    inventory_root,
                    project_root=resolved_project_root,
                ),
                "source_run_id": inventory_payload["run_id"],
                "source_status": "INCOMPLETE_REPLAY",
                "mode": "cache-only",
                "inventory_only": True,
                "visible_at": inventory_payload["visible_at"],
                "cutoff_policy": inventory_payload["cutoff_policy"],
                "replay_run_snapshot": _artifact_pointer(
                    staging_root / "source/replay_run.json",
                    root=staging_root,
                ),
                "input_freeze_manifest_snapshot": _artifact_pointer(
                    staging_root / "source/input_freeze_manifest.json",
                    root=staging_root,
                ),
            },
            "research_window": {
                "project_default_start": PROJECT_DEFAULT_RESEARCH_START.isoformat(),
                "selected_start": PROJECT_DEFAULT_RESEARCH_START.isoformat(),
                "selected_end": as_of.isoformat(),
                "canonical_requested_window": None,
                "evaluated_input_start": _minimum_input_date(input_summaries),
                "evaluated_input_end": _maximum_input_date(input_summaries),
                "interpretation": (
                    "isolated_fact_validation_only_no_investment_conclusion"
                ),
            },
            "data_quality": {
                "scope": "isolated_market_macro_fact_validation",
                "canonical_gate": False,
                "status": quality_report.status,
                "passed": True,
                "checked_at": quality_report.checked_at.isoformat(),
                "error_count": quality_report.error_count,
                "warning_count": quality_report.warning_count,
                "info_count": quality_report.info_count,
                "warning_codes": sorted(
                    {
                        issue.code
                        for issue in quality_report.issues
                        if issue.severity == Severity.WARNING
                    }
                ),
                "report": _artifact_pointer(quality_report_path, root=staging_root),
                "marketstack_reconciliation": _artifact_pointer(
                    reconciliation_path,
                    root=staging_root,
                ),
            },
            "input_summaries": input_summaries,
            "market_snapshot": market_snapshot,
            "macro_snapshot": macro_snapshot,
            "strict_missing_inputs": {field: None for field in _STRICT_NULL_FIELDS},
            "conclusion_outputs": {field: None for field in _CONCLUSION_NULL_FIELDS},
            "source_artifacts": source_artifacts,
            "canonical_guard_evidence": {
                "unchanged": True,
                "before": guard_before,
                "after": guard_after,
            },
            "exclusions": list(_EXCLUSIONS),
            "safety": _required_safety_payload(),
        }
        payload_path = staging_root / PAYLOAD_FILENAME
        markdown_path = staging_root / MARKDOWN_FILENAME
        write_json_atomic(payload_path, payload)
        write_markdown_atomic(markdown_path, render_limited_non_pit_markdown(payload))

        validation = validate_limited_non_pit_reconstruction(
            staging_root,
            project_root=resolved_project_root,
            expected_as_of=as_of,
            expected_owner_decision_id=owner_decision_id,
            expected_inventory_bundle=inventory_root,
        )
        if not validation.passed:
            raise LimitedNonPitReconstructionError(
                "LIMITED_NON_PIT_SELF_VALIDATION_FAILED: "
                + "; ".join(validation.errors)
            )
        write_limited_non_pit_validation(validation, staging_root)
    except Exception:
        if staging_root.exists():
            shutil.rmtree(staging_root)
        raise

    return LimitedNonPitBuild(
        bundle_path=final_root,
        payload_path=final_root / PAYLOAD_FILENAME,
        markdown_path=final_root / MARKDOWN_FILENAME,
        validation_path=final_root / VALIDATION_FILENAME,
        validation=validation,
    )


def validate_limited_non_pit_reconstruction(
    bundle_path: Path,
    *,
    project_root: Path,
    expected_as_of: date,
    expected_owner_decision_id: str,
    expected_inventory_bundle: Path,
) -> LimitedNonPitValidation:
    """Content-derived validator for a v2 limited non-PIT bundle."""

    checked_at = datetime.now(UTC)
    checks: list[str] = []
    errors: list[str] = []
    bundle_id = bundle_path.name
    observed_as_of = expected_as_of
    try:
        _validate_owner_decision_id(expected_owner_decision_id)
        root = _validate_input_directory(bundle_path, "limited non-PIT bundle")
        resolved_project_root = project_root.resolve(strict=True)
        payload, _ = _load_strict_mapping(
            root / PAYLOAD_FILENAME,
            PAYLOAD_FILENAME,
        )
        bundle_id = str(payload.get("bundle_id") or bundle_id)
        observed_as_of = _parse_date(payload.get("as_of"), "payload.as_of")
        _validate_payload_constants(
            payload,
            expected_as_of=expected_as_of,
            expected_owner_decision_id=expected_owner_decision_id,
        )
        checks.append("payload_schema_and_owner_contract")

        artifact_map = _validate_source_artifact_inventory(payload, root=root)
        if _read_regular_file(
            root / "contract" / SCHEMA_RELATIVE_PATH.name,
            "schema snapshot",
        ) != _read_regular_file(
            resolved_project_root / SCHEMA_RELATIVE_PATH,
            "reviewed project schema",
        ):
            raise LimitedNonPitReconstructionError(
                "LIMITED_NON_PIT_SCHEMA_SNAPSHOT_DRIFT"
            )
        checks.append("source_artifact_hashes_and_sizes")
        _validate_bundle_membership(root, artifact_map=artifact_map)
        checks.append("bundle_member_allowlist")

        source_inventory = _require_mapping(
            payload.get("source_inventory"),
            "source_inventory",
        )
        expected_inventory_root = _validate_input_directory(
            expected_inventory_bundle,
            "expected inventory bundle",
        )
        frozen_replay, frozen_replay_bytes = _load_strict_mapping(
            root / "source/replay_run.json",
            "frozen replay_run.json",
        )
        frozen_manifest, frozen_manifest_bytes = _load_strict_json(
            root / "source/input_freeze_manifest.json",
            "frozen input_freeze_manifest.json",
        )
        _, inventory_records = _validate_inventory(
            frozen_replay,
            frozen_manifest,
            inventory_root=root,
            frozen_member_root=root,
        )
        expected_source_inventory = {
            "source_bundle_path": _display_path(
                expected_inventory_root,
                project_root=resolved_project_root,
            ),
            "source_run_id": frozen_replay.get("run_id"),
            "source_status": frozen_replay.get("status"),
            "mode": frozen_replay.get("mode"),
            "inventory_only": frozen_replay.get("inventory_only"),
            "visible_at": frozen_replay.get("visible_at"),
            "cutoff_policy": frozen_replay.get("cutoff_policy"),
        }
        for field, expected_value in expected_source_inventory.items():
            if source_inventory.get(field) != expected_value:
                raise LimitedNonPitReconstructionError(
                    f"LIMITED_NON_PIT_SOURCE_INVENTORY_FIELD_DRIFT:{field}"
                )
        _validate_snapshot_pointer(
            source_inventory.get("replay_run_snapshot"),
            path=root / "source/replay_run.json",
            root=root,
        )
        _validate_snapshot_pointer(
            source_inventory.get("input_freeze_manifest_snapshot"),
            path=root / "source/input_freeze_manifest.json",
            root=root,
        )
        expected_replay_bytes = _read_regular_file(
            expected_inventory_root / "replay_run.json",
            "expected replay_run.json",
        )
        expected_manifest_bytes = _read_regular_file(
            expected_inventory_root / "input_freeze_manifest.json",
            "expected input_freeze_manifest.json",
        )
        if frozen_replay_bytes != expected_replay_bytes:
            raise LimitedNonPitReconstructionError(
                "LIMITED_NON_PIT_SOURCE_REPLAY_DRIFT"
            )
        if frozen_manifest_bytes != expected_manifest_bytes:
            raise LimitedNonPitReconstructionError(
                "LIMITED_NON_PIT_SOURCE_MANIFEST_DRIFT"
            )
        checks.append("source_inventory_snapshot_and_external_binding")

        for role, artifact_id, member_path in _INPUT_SPECS:
            member = root / member_path
            record = inventory_records[artifact_id]
            member_bytes = _read_regular_file(member, role)
            _require_hash_and_size(
                member_bytes,
                expected_sha256=record.get("sha256"),
                label=role,
            )
        checks.append("frozen_market_macro_input_binding")

        report = _run_isolated_data_quality(root, as_of=observed_as_of)
        _validate_data_quality_payload(payload, report=report, root=root)
        checks.append("content_derived_data_quality")
        _validate_input_summaries(payload, report=report, root=root)
        checks.append("content_derived_input_summaries")

        universe = load_universe()
        expected_tickers = configured_price_tickers(
            universe,
            include_full_ai_chain=False,
        )
        expected_series = configured_rate_series(universe)
        expected_market = _market_snapshot(
            root / "input/data/raw/prices_daily.csv",
            as_of=observed_as_of,
            expected_tickers=expected_tickers,
        )
        expected_macro = _macro_snapshot(
            root / "input/data/raw/rates_daily.csv",
            as_of=observed_as_of,
            expected_series=expected_series,
        )
        if payload.get("market_snapshot") != expected_market:
            raise LimitedNonPitReconstructionError(
                "LIMITED_NON_PIT_MARKET_SNAPSHOT_DRIFT"
            )
        if payload.get("macro_snapshot") != expected_macro:
            raise LimitedNonPitReconstructionError(
                "LIMITED_NON_PIT_MACRO_SNAPSHOT_DRIFT"
            )
        checks.append("content_derived_market_macro_facts")

        _validate_null_and_safety_contract(payload)
        checks.append("null_exclusion_and_safety_contract")
        _validate_guard_evidence(payload, project_root=resolved_project_root)
        checks.append("canonical_guard_before_after_identity")

        expected_markdown = render_limited_non_pit_markdown(payload)
        actual_markdown = _read_regular_file(
            root / MARKDOWN_FILENAME,
            MARKDOWN_FILENAME,
        ).decode("utf-8")
        if actual_markdown != expected_markdown:
            raise LimitedNonPitReconstructionError(
                "LIMITED_NON_PIT_MARKDOWN_CONTENT_DRIFT"
            )
        checks.append("content_derived_markdown")
    except (
        LimitedNonPitReconstructionError,
        OSError,
        UnicodeDecodeError,
        StrictJsonContractError,
        ValueError,
    ) as exc:
        errors.append(str(exc))

    return LimitedNonPitValidation(
        status="PASS" if not errors else "FAIL",
        checked_at=checked_at,
        bundle_id=bundle_id,
        as_of=observed_as_of,
        checks=tuple(checks),
        errors=tuple(errors),
    )


def write_limited_non_pit_validation(
    validation: LimitedNonPitValidation,
    bundle_path: Path,
) -> tuple[Path, Path]:
    json_path = bundle_path / VALIDATION_FILENAME
    markdown_path = bundle_path / VALIDATION_MARKDOWN_FILENAME
    write_json_atomic(json_path, validation.to_payload())
    write_markdown_atomic(markdown_path, render_limited_non_pit_validation(validation))
    return json_path, markdown_path


def render_limited_non_pit_markdown(payload: Mapping[str, object]) -> str:
    as_of = str(payload["as_of"])
    data_quality = _require_mapping(payload.get("data_quality"), "data_quality")
    source_inventory = _require_mapping(
        payload.get("source_inventory"),
        "source_inventory",
    )
    research_window = _require_mapping(payload.get("research_window"), "research_window")
    market_snapshot = _require_sequence(payload.get("market_snapshot"), "market_snapshot")
    macro_snapshot = _require_sequence(payload.get("macro_snapshot"), "macro_snapshot")
    source_artifacts = _require_sequence(
        payload.get("source_artifacts"),
        "source_artifacts",
    )
    guards = _require_mapping(
        payload.get("canonical_guard_evidence"),
        "canonical_guard_evidence",
    )
    lines = [
        f"# {as_of} 受限非 PIT 历史事实重建",
        "",
        "> 本报告不是 canonical daily、PIT 恢复、评分、仓位或投资建议。"
        "严格输入仍不完整，结论状态固定为 `INSUFFICIENT_DATA`。",
        "",
        f"- Schema：`{payload['schema_version']}`",
        f"- 状态：`{payload['status']}`",
        f"- Canonical daily evidence：`{payload['canonical_daily_evidence_status']}`",
        f"- Owner 决策：`{payload['owner_decision_id']}`",
        f"- Source inventory：`{source_inventory['source_bundle_path']}`",
        f"- Source run：`{source_inventory['source_run_id']}`",
        f"- Visibility cutoff：`{source_inventory['visible_at']}`",
        f"- DQ：`{data_quality['status']}`，errors={data_quality['error_count']}，"
        f"warnings={data_quality['warning_count']}，info={data_quality['info_count']}",
        "- DQ 边界：隔离 market/macro fact validation；不是 canonical requested-window "
        "receipt 或 daily gate。",
        f"- 研究窗口政策：`{research_window['selected_start']}` 至 "
        f"`{research_window['selected_end']}`；实际读取输入日期范围 "
        f"`{research_window['evaluated_input_start']}` 至 "
        f"`{research_window['evaluated_input_end']}`。",
        "- Production effect：`none`",
        "",
        "## Market facts",
        "",
        "|Ticker|Date|Close|Previous date|Change %|",
        "|---|---|---:|---|---:|",
    ]
    for raw in market_snapshot:
        row = _require_mapping(raw, "market_snapshot row")
        lines.append(
            f"|{row['ticker']}|{row['date']}|{row['close']}|"
            f"{row['previous_date']}|{row['change_pct']}|"
        )
    lines.extend(
        [
            "",
            "## Macro facts",
            "",
            "|Series|Date|Value|Previous date|Change|",
            "|---|---|---:|---|---:|",
        ]
    )
    for raw in macro_snapshot:
        row = _require_mapping(raw, "macro_snapshot row")
        lines.append(
            f"|{row['series']}|{row['date']}|{row['value']}|"
            f"{row['previous_date']}|{row['change']}|"
        )
    lines.extend(
        [
            "",
            "## Null contract",
            "",
            "- FMP forward PIT normalized/fetch/validation：`null`",
            "- SEC fundamentals：`null`",
            "- OpenAI prereview report：`null`",
            "- Daily score、position、Decision Snapshot、Dashboard、Reader Brief：`null`",
            "- Weekly、governance、promotion、backtest、weights、production：`null`",
            "",
            "## Source artifacts",
            "",
            "|Path|Size|SHA-256|",
            "|---|---:|---|",
        ]
    )
    for raw in source_artifacts:
        artifact = _require_mapping(raw, "source artifact")
        lines.append(
            f"|`{artifact['path']}`|{artifact['size_bytes']}|`{artifact['sha256']}`|"
        )
    before = _require_sequence(guards.get("before"), "canonical_guard_evidence.before")
    lines.extend(
        [
            "",
            "## Canonical guard",
            "",
            f"- Before/after unchanged：`{str(guards['unchanged']).lower()}`",
            f"- Guarded file count：{len(before)}",
            "",
            "## 禁止用途",
            "",
            "该 bundle 不得进入 canonical daily、Reader Brief、weekly、governance、"
            "promotion、backtest、weight、production 或 broker/trading 链路。",
            "",
        ]
    )
    return "\n".join(lines)


def render_limited_non_pit_validation(validation: LimitedNonPitValidation) -> str:
    lines = [
        "# Limited non-PIT reconstruction validation",
        "",
        f"- Status：`{validation.status}`",
        f"- Checked at：`{validation.checked_at.isoformat()}`",
        f"- Bundle id：`{validation.bundle_id}`",
        f"- As-of：`{validation.as_of.isoformat()}`",
        "- Production effect：`none`",
        "",
        "## Checks",
        "",
    ]
    lines.extend(f"- `{check}`" for check in validation.checks)
    lines.extend(["", "## Errors", ""])
    lines.extend(f"- {error}" for error in validation.errors)
    if not validation.errors:
        lines.append("- none")
    lines.append("")
    return "\n".join(lines)


def _validate_inventory(
    replay_payload: Mapping[str, object],
    manifest_payload: object,
    *,
    inventory_root: Path,
    frozen_member_root: Path | None = None,
) -> tuple[date, dict[str, Mapping[str, object]]]:
    if replay_payload.get("status") != "INCOMPLETE_REPLAY":
        raise LimitedNonPitReconstructionError(
            "LIMITED_NON_PIT_SOURCE_STATUS_NOT_INCOMPLETE"
        )
    if replay_payload.get("mode") != "cache-only":
        raise LimitedNonPitReconstructionError("LIMITED_NON_PIT_SOURCE_MODE_NOT_CACHE_ONLY")
    if replay_payload.get("inventory_only") is not True:
        raise LimitedNonPitReconstructionError(
            "LIMITED_NON_PIT_SOURCE_NOT_INVENTORY_ONLY"
        )
    if replay_payload.get("command_results") != []:
        raise LimitedNonPitReconstructionError(
            "LIMITED_NON_PIT_SOURCE_COMMAND_RESULTS_NOT_EMPTY"
        )
    as_of = _parse_date(replay_payload.get("as_of"), "replay_run.as_of")
    if not is_us_equity_trading_day(as_of):
        raise LimitedNonPitReconstructionError(
            "LIMITED_NON_PIT_AS_OF_NOT_US_EQUITY_TRADING_DAY"
        )
    records = _records_by_artifact_id(
        replay_payload.get("input_records"),
        "replay_run.input_records",
    )
    manifest_records = _records_by_artifact_id(
        manifest_payload,
        "input_freeze_manifest",
    )
    for artifact_id in _STRICT_MISSING_ARTIFACT_IDS:
        record = records.get(artifact_id)
        if record is None or record.get("status") != "MISSING":
            raise LimitedNonPitReconstructionError(
                f"LIMITED_NON_PIT_REQUIRED_MISSING_CONTRACT_DRIFT:{artifact_id}"
            )
    for _, artifact_id, member_path in _INPUT_SPECS:
        record = records.get(artifact_id)
        manifest_record = manifest_records.get(artifact_id)
        if record is None or manifest_record is None:
            raise LimitedNonPitReconstructionError(
                f"LIMITED_NON_PIT_INVENTORY_RECORD_MISSING:{artifact_id}"
            )
        for field in ("status", "included_count", "sha256"):
            if record.get(field) != manifest_record.get(field):
                raise LimitedNonPitReconstructionError(
                    f"LIMITED_NON_PIT_INVENTORY_MANIFEST_DRIFT:{artifact_id}:{field}"
                )
        if record.get("status") not in {"PASS", "PASS_WITH_EXCLUSIONS"}:
            raise LimitedNonPitReconstructionError(
                f"LIMITED_NON_PIT_INPUT_STATUS_INVALID:{artifact_id}"
            )
        expected_member = (
            (frozen_member_root or inventory_root) / member_path
            if frozen_member_root is not None
            else inventory_root / member_path
        )
        actual_member = (
            expected_member
            if frozen_member_root is not None
            else _inventory_member_path(inventory_root, member_path)
        )
        member_bytes = _read_regular_file(actual_member, artifact_id)
        _require_hash_and_size(
            member_bytes,
            expected_sha256=record.get("sha256"),
            label=artifact_id,
        )
        included_count = record.get("included_count")
        if not isinstance(included_count, int) or included_count < 0:
            raise LimitedNonPitReconstructionError(
                f"LIMITED_NON_PIT_INCLUDED_COUNT_INVALID:{artifact_id}"
            )
        observed_rows = _csv_data_row_count(member_bytes)
        if observed_rows != included_count:
            raise LimitedNonPitReconstructionError(
                f"LIMITED_NON_PIT_ROW_COUNT_DRIFT:{artifact_id}:"
                f"expected={included_count}:observed={observed_rows}"
            )
    return as_of, records


def _run_isolated_data_quality(root: Path, *, as_of: date) -> DataQualityReport:
    universe = load_universe()
    return validate_data_cache(
        prices_path=root / "input/data/raw/prices_daily.csv",
        rates_path=root / "input/data/raw/rates_daily.csv",
        expected_price_tickers=configured_price_tickers(
            universe,
            include_full_ai_chain=False,
        ),
        expected_rate_series=configured_rate_series(universe),
        quality_config=load_data_quality(),
        as_of=as_of,
        manifest_path=root / "input/data/raw/download_manifest.csv",
        secondary_prices_path=root / "input/data/raw/prices_marketstack_daily.csv",
        require_secondary_prices=True,
        download_publication_resolution=DownloadPublicationResolution(
            status=DownloadPublicationResolutionStatus.ABSENT
        ),
    )


def _quality_report_path(root: Path, as_of: date) -> Path:
    return root / "reports" / f"data_quality_market_macro_{as_of.isoformat()}.md"


def _validate_data_quality_payload(
    payload: Mapping[str, object],
    *,
    report: DataQualityReport,
    root: Path,
) -> None:
    quality = _require_mapping(payload.get("data_quality"), "data_quality")
    expected = {
        "scope": "isolated_market_macro_fact_validation",
        "canonical_gate": False,
        "status": report.status,
        "passed": report.passed,
        "error_count": report.error_count,
        "warning_count": report.warning_count,
        "info_count": report.info_count,
        "warning_codes": sorted(
            {
                issue.code
                for issue in report.issues
                if issue.severity == Severity.WARNING
            }
        ),
    }
    for field, expected_value in expected.items():
        if quality.get(field) != expected_value:
            raise LimitedNonPitReconstructionError(
                f"LIMITED_NON_PIT_DQ_FIELD_DRIFT:{field}"
            )
    if not report.passed:
        raise LimitedNonPitReconstructionError("LIMITED_NON_PIT_DQ_NOT_PASSING")
    checked_at = _parse_datetime(quality.get("checked_at"), "data_quality.checked_at")
    report_with_original_time = replace(report, checked_at=checked_at)
    report_path = _quality_report_path(root, report.as_of)
    actual_report = _read_regular_file(report_path, "data quality report").decode("utf-8")
    if actual_report != render_data_quality_report(report_with_original_time):
        raise LimitedNonPitReconstructionError(
            "LIMITED_NON_PIT_DQ_REPORT_CONTENT_DRIFT"
        )
    reconciliation_path = marketstack_reconciliation_path(report_path)
    actual_reconciliation = _read_regular_file(
        reconciliation_path,
        "marketstack reconciliation",
    ).decode("utf-8")
    if actual_reconciliation != _render_marketstack_reconciliation_csv(report):
        raise LimitedNonPitReconstructionError(
            "LIMITED_NON_PIT_DQ_RECONCILIATION_CONTENT_DRIFT"
        )
    _validate_snapshot_pointer(quality.get("report"), path=report_path, root=root)
    _validate_snapshot_pointer(
        quality.get("marketstack_reconciliation"),
        path=reconciliation_path,
        root=root,
    )


def _render_marketstack_reconciliation_csv(report: DataQualityReport) -> str:
    rows = [
        {
            "as_of": report.as_of.isoformat(),
            "date": record.date,
            "ticker": record.ticker,
            "severity": record.severity.value,
            "classification": record.classification,
            "rule_id": record.rule_id,
            "evidence": record.evidence,
            "primary_close": record.primary_close,
            "secondary_close": record.secondary_close,
            "close_diff_pct": record.close_diff_pct,
            "primary_adj_close": record.primary_adj_close,
            "secondary_adj_close": record.secondary_adj_close,
            "adj_close_diff_pct": record.adj_close_diff_pct,
        }
        for record in report.marketstack_reconciliation_records
    ]
    columns = [
        "as_of",
        "date",
        "ticker",
        "severity",
        "classification",
        "rule_id",
        "evidence",
        "primary_close",
        "secondary_close",
        "close_diff_pct",
        "primary_adj_close",
        "secondary_adj_close",
        "adj_close_diff_pct",
    ]
    return pd.DataFrame(rows, columns=columns).to_csv(index=False)


def _input_summaries(
    report: DataQualityReport,
    *,
    staging_root: Path,
) -> dict[str, object]:
    if report.secondary_price_summary is None or report.manifest_summary is None:
        raise LimitedNonPitReconstructionError(
            "LIMITED_NON_PIT_DQ_SUMMARY_MISSING"
        )
    summaries = {
        "primary_prices": report.price_summary,
        "secondary_prices": report.secondary_price_summary,
        "rates": report.rate_summary,
        "download_manifest": report.manifest_summary,
    }
    return {
        role: _data_file_summary(summary, root=staging_root)
        for role, summary in summaries.items()
    }


def _data_file_summary(summary: DataFileSummary, *, root: Path) -> dict[str, object]:
    if not summary.exists or summary.sha256 is None:
        raise LimitedNonPitReconstructionError(
            f"LIMITED_NON_PIT_DQ_INPUT_SUMMARY_INVALID:{summary.path}"
        )
    return {
        "path": _member_path(summary.path, root=root),
        "rows": summary.rows,
        "sha256": summary.sha256,
        "size_bytes": summary.path.stat().st_size,
        "min_date": None if summary.min_date is None else summary.min_date.isoformat(),
        "max_date": None if summary.max_date is None else summary.max_date.isoformat(),
    }


def _validate_input_summaries(
    payload: Mapping[str, object],
    *,
    report: DataQualityReport,
    root: Path,
) -> None:
    expected = _input_summaries(report, staging_root=root)
    if payload.get("input_summaries") != expected:
        raise LimitedNonPitReconstructionError(
            "LIMITED_NON_PIT_INPUT_SUMMARIES_DRIFT"
        )
    research_window = _require_mapping(payload.get("research_window"), "research_window")
    if research_window.get("evaluated_input_start") != _minimum_input_date(expected):
        raise LimitedNonPitReconstructionError(
            "LIMITED_NON_PIT_RESEARCH_WINDOW_EVALUATED_START_DRIFT"
        )
    if research_window.get("evaluated_input_end") != _maximum_input_date(expected):
        raise LimitedNonPitReconstructionError(
            "LIMITED_NON_PIT_RESEARCH_WINDOW_EVALUATED_END_DRIFT"
        )


def _market_snapshot(
    path: Path,
    *,
    as_of: date,
    expected_tickers: Sequence[str],
) -> list[dict[str, object]]:
    frame = pd.read_csv(path)
    frame["_date"] = pd.to_datetime(frame["date"], errors="raise").dt.date
    frame = frame.loc[frame["_date"] <= as_of].copy()
    rows: list[dict[str, object]] = []
    for ticker in expected_tickers:
        ticker_rows = frame.loc[frame["ticker"].astype(str) == ticker].sort_values("_date")
        current = ticker_rows.loc[ticker_rows["_date"] == as_of]
        if len(current) != 1:
            raise LimitedNonPitReconstructionError(
                f"LIMITED_NON_PIT_MARKET_AS_OF_ROW_INVALID:{ticker}:{len(current)}"
            )
        previous = ticker_rows.loc[ticker_rows["_date"] < as_of]
        if previous.empty:
            raise LimitedNonPitReconstructionError(
                f"LIMITED_NON_PIT_MARKET_PREVIOUS_ROW_MISSING:{ticker}"
            )
        current_row = current.iloc[0]
        previous_row = previous.iloc[-1]
        close = float(current_row["close"])
        previous_close = float(previous_row["close"])
        change_pct = ((close / previous_close) - 1.0) * 100.0
        rows.append(
            {
                "ticker": ticker,
                "date": as_of.isoformat(),
                "close": round(close, 8),
                "adj_close": round(float(current_row["adj_close"]), 8),
                "previous_date": previous_row["_date"].isoformat(),
                "previous_close": round(previous_close, 8),
                "change_pct": round(change_pct, 6),
                "source_role": "primary_prices",
            }
        )
    return rows


def _macro_snapshot(
    path: Path,
    *,
    as_of: date,
    expected_series: Sequence[str],
) -> list[dict[str, object]]:
    frame = pd.read_csv(path)
    frame["_date"] = pd.to_datetime(frame["date"], errors="raise").dt.date
    frame = frame.loc[frame["_date"] <= as_of].copy()
    rows: list[dict[str, object]] = []
    for series in expected_series:
        series_rows = frame.loc[frame["series"].astype(str) == series].sort_values("_date")
        if len(series_rows) < 2:
            raise LimitedNonPitReconstructionError(
                f"LIMITED_NON_PIT_MACRO_ROWS_MISSING:{series}"
            )
        current_row = series_rows.iloc[-1]
        previous_row = series_rows.iloc[-2]
        value = float(current_row["value"])
        previous_value = float(previous_row["value"])
        rows.append(
            {
                "series": series,
                "date": current_row["_date"].isoformat(),
                "value": round(value, 8),
                "previous_date": previous_row["_date"].isoformat(),
                "previous_value": round(previous_value, 8),
                "change": round(value - previous_value, 8),
                "source_role": "rates",
            }
        )
    return rows


def _source_artifacts(root: Path) -> list[dict[str, object]]:
    as_of = _parse_date(
        _load_strict_mapping(root / "source/replay_run.json", "source replay")[0].get(
            "as_of"
        ),
        "source replay as_of",
    )
    paths = [
        root / "contract" / SCHEMA_RELATIVE_PATH.name,
        root / "source/replay_run.json",
        root / "source/input_freeze_manifest.json",
        *(root / member_path for _, _, member_path in _INPUT_SPECS),
        _quality_report_path(root, as_of),
        marketstack_reconciliation_path(_quality_report_path(root, as_of)),
    ]
    return [_artifact_pointer(path, root=root) for path in paths]


def _validate_source_artifact_inventory(
    payload: Mapping[str, object],
    *,
    root: Path,
) -> dict[str, Mapping[str, object]]:
    raw_artifacts = _require_sequence(payload.get("source_artifacts"), "source_artifacts")
    artifact_map: dict[str, Mapping[str, object]] = {}
    for raw in raw_artifacts:
        artifact = _require_mapping(raw, "source artifact")
        path_value = artifact.get("path")
        if not isinstance(path_value, str):
            raise LimitedNonPitReconstructionError(
                "LIMITED_NON_PIT_ARTIFACT_PATH_INVALID"
            )
        if path_value in artifact_map:
            raise LimitedNonPitReconstructionError(
                f"LIMITED_NON_PIT_ARTIFACT_DUPLICATE:{path_value}"
            )
        path = _resolve_member(root, path_value)
        _validate_snapshot_pointer(artifact, path=path, root=root)
        artifact_map[path_value] = artifact
    expected_paths = {
        f"contract/{SCHEMA_RELATIVE_PATH.name}",
        "source/replay_run.json",
        "source/input_freeze_manifest.json",
        *(_member_path(root / member_path, root=root) for _, _, member_path in _INPUT_SPECS),
        _member_path(
            _quality_report_path(root, _parse_date(payload.get("as_of"), "payload.as_of")),
            root=root,
        ),
        _member_path(
            marketstack_reconciliation_path(
                _quality_report_path(
                    root,
                    _parse_date(payload.get("as_of"), "payload.as_of"),
                )
            ),
            root=root,
        ),
    }
    if set(artifact_map) != expected_paths:
        raise LimitedNonPitReconstructionError(
            "LIMITED_NON_PIT_ARTIFACT_SET_DRIFT"
        )
    schema_snapshot = root / "contract" / SCHEMA_RELATIVE_PATH.name
    if not schema_snapshot.is_file():
        raise LimitedNonPitReconstructionError("LIMITED_NON_PIT_SCHEMA_SNAPSHOT_MISSING")
    return artifact_map


def _validate_bundle_membership(
    root: Path,
    *,
    artifact_map: Mapping[str, Mapping[str, object]],
) -> None:
    allowed = set(artifact_map)
    allowed.update(
        {
            PAYLOAD_FILENAME,
            MARKDOWN_FILENAME,
            VALIDATION_FILENAME,
            VALIDATION_MARKDOWN_FILENAME,
        }
    )
    observed: set[str] = set()
    for path in root.rglob("*"):
        if path.is_symlink():
            raise LimitedNonPitReconstructionError(
                f"LIMITED_NON_PIT_BUNDLE_SYMLINK:{path}"
            )
        if path.is_file():
            observed.add(_member_path(path, root=root))
    unexpected = sorted(observed - allowed)
    missing = sorted(set(artifact_map) - observed)
    if unexpected or missing:
        raise LimitedNonPitReconstructionError(
            "LIMITED_NON_PIT_BUNDLE_MEMBER_DRIFT:"
            f"unexpected={','.join(unexpected)}:missing={','.join(missing)}"
        )


def _validate_payload_constants(
    payload: Mapping[str, object],
    *,
    expected_as_of: date,
    expected_owner_decision_id: str,
) -> None:
    if set(payload) != _TOP_LEVEL_KEYS:
        raise LimitedNonPitReconstructionError(
            "LIMITED_NON_PIT_TOP_LEVEL_KEYS_INVALID"
        )
    constants = {
        "schema_version": SCHEMA_VERSION,
        "status": STATUS,
        "task_id": TASK_ID,
        "owner_decision_id": expected_owner_decision_id,
        "as_of": expected_as_of.isoformat(),
        "scope": "checksum_backed_market_macro_facts_only",
        "canonical_daily_evidence_status": "MISSING",
        "reconstruction_conclusion_status": "INSUFFICIENT_DATA",
    }
    for field, expected in constants.items():
        if payload.get(field) != expected:
            raise LimitedNonPitReconstructionError(
                f"LIMITED_NON_PIT_PAYLOAD_CONSTANT_DRIFT:{field}"
            )
    _validate_bundle_id(str(payload.get("bundle_id") or ""))
    _parse_datetime(payload.get("generated_at"), "payload.generated_at")
    research_window = _require_mapping(payload.get("research_window"), "research_window")
    expected_window = {
        "project_default_start": PROJECT_DEFAULT_RESEARCH_START.isoformat(),
        "selected_start": PROJECT_DEFAULT_RESEARCH_START.isoformat(),
        "selected_end": expected_as_of.isoformat(),
        "canonical_requested_window": None,
        "interpretation": "isolated_fact_validation_only_no_investment_conclusion",
    }
    for field, expected in expected_window.items():
        if research_window.get(field) != expected:
            raise LimitedNonPitReconstructionError(
                f"LIMITED_NON_PIT_RESEARCH_WINDOW_DRIFT:{field}"
            )


def _validate_null_and_safety_contract(payload: Mapping[str, object]) -> None:
    strict_missing = _require_mapping(
        payload.get("strict_missing_inputs"),
        "strict_missing_inputs",
    )
    if set(strict_missing) != set(_STRICT_NULL_FIELDS) or any(
        strict_missing[field] is not None for field in _STRICT_NULL_FIELDS
    ):
        raise LimitedNonPitReconstructionError(
            "LIMITED_NON_PIT_STRICT_NULL_CONTRACT_DRIFT"
        )
    conclusions = _require_mapping(
        payload.get("conclusion_outputs"),
        "conclusion_outputs",
    )
    if set(conclusions) != set(_CONCLUSION_NULL_FIELDS) or any(
        conclusions[field] is not None for field in _CONCLUSION_NULL_FIELDS
    ):
        raise LimitedNonPitReconstructionError(
            "LIMITED_NON_PIT_CONCLUSION_NULL_CONTRACT_DRIFT"
        )
    exclusions = payload.get("exclusions")
    if exclusions != list(_EXCLUSIONS):
        raise LimitedNonPitReconstructionError(
            "LIMITED_NON_PIT_EXCLUSIONS_DRIFT"
        )
    if payload.get("safety") != _required_safety_payload():
        raise LimitedNonPitReconstructionError("LIMITED_NON_PIT_SAFETY_DRIFT")


def _required_safety_payload() -> dict[str, object]:
    return {
        "production_effect": "none",
        "provider_calls": False,
        "openai_calls": False,
        "canonical_cache_mutation": False,
        "canonical_state_mutation": False,
        "score_generated": False,
        "position_generated": False,
        "broker_action_allowed": False,
        "trading_action_taken": False,
    }


def _capture_guard_paths(
    guard_paths: Sequence[Path],
    *,
    project_root: Path,
) -> list[dict[str, object]]:
    if not guard_paths:
        raise LimitedNonPitReconstructionError(
            "LIMITED_NON_PIT_GUARD_PATHS_REQUIRED"
        )
    records: list[dict[str, object]] = []
    seen: set[Path] = set()
    for raw_path in guard_paths:
        path = raw_path if raw_path.is_absolute() else project_root / raw_path
        resolved = path.resolve(strict=True)
        if resolved in seen:
            raise LimitedNonPitReconstructionError(
                f"LIMITED_NON_PIT_GUARD_DUPLICATE:{resolved}"
            )
        seen.add(resolved)
        content = _read_regular_file(resolved, "canonical guard")
        records.append(
            {
                "path": _display_path(resolved, project_root=project_root),
                "sha256": sha256_bytes(content),
                "size_bytes": len(content),
            }
        )
    return sorted(records, key=lambda item: str(item["path"]))


def _validate_guard_evidence(
    payload: Mapping[str, object],
    *,
    project_root: Path,
) -> None:
    guard = _require_mapping(
        payload.get("canonical_guard_evidence"),
        "canonical_guard_evidence",
    )
    if guard.get("unchanged") is not True:
        raise LimitedNonPitReconstructionError(
            "LIMITED_NON_PIT_CANONICAL_GUARD_NOT_UNCHANGED"
        )
    before = _require_sequence(guard.get("before"), "canonical guard before")
    after = _require_sequence(guard.get("after"), "canonical guard after")
    if before != after or not before:
        raise LimitedNonPitReconstructionError(
            "LIMITED_NON_PIT_CANONICAL_GUARD_DRIFT"
        )
    for raw in before:
        record = _require_mapping(raw, "canonical guard record")
        path_value = record.get("path")
        if not isinstance(path_value, str) or not path_value:
            raise LimitedNonPitReconstructionError(
                "LIMITED_NON_PIT_CANONICAL_GUARD_PATH_INVALID"
            )
        sha = record.get("sha256")
        size = record.get("size_bytes")
        if not isinstance(sha, str) or _SHA256_PATTERN.fullmatch(sha) is None:
            raise LimitedNonPitReconstructionError(
                "LIMITED_NON_PIT_CANONICAL_GUARD_SHA_INVALID"
            )
        if not isinstance(size, int) or size < 0:
            raise LimitedNonPitReconstructionError(
                "LIMITED_NON_PIT_CANONICAL_GUARD_SIZE_INVALID"
            )
        candidate = Path(path_value)
        if not candidate.is_absolute():
            candidate = project_root / candidate
        candidate.resolve(strict=False)


def _artifact_pointer(path: Path, *, root: Path) -> dict[str, object]:
    content = _read_regular_file(path, "artifact pointer")
    return {
        "path": _member_path(path, root=root),
        "sha256": sha256_bytes(content),
        "size_bytes": len(content),
    }


def _validate_snapshot_pointer(
    raw_pointer: object,
    *,
    path: Path,
    root: Path,
) -> None:
    pointer = _require_mapping(raw_pointer, "artifact pointer")
    expected = _artifact_pointer(path, root=root)
    if pointer != expected:
        raise LimitedNonPitReconstructionError(
            f"LIMITED_NON_PIT_ARTIFACT_POINTER_DRIFT:{expected['path']}"
        )


def _minimum_input_date(input_summaries: Mapping[str, object]) -> str:
    values: list[str] = []
    for raw in input_summaries.values():
        summary = _require_mapping(raw, "input summary")
        value = summary.get("min_date")
        if isinstance(value, str):
            values.append(value)
    if not values:
        raise LimitedNonPitReconstructionError("LIMITED_NON_PIT_INPUT_MIN_DATE_MISSING")
    return min(values)


def _maximum_input_date(input_summaries: Mapping[str, object]) -> str:
    values: list[str] = []
    for raw in input_summaries.values():
        summary = _require_mapping(raw, "input summary")
        value = summary.get("max_date")
        if isinstance(value, str):
            values.append(value)
    if not values:
        raise LimitedNonPitReconstructionError("LIMITED_NON_PIT_INPUT_MAX_DATE_MISSING")
    return max(values)


def _records_by_artifact_id(
    value: object,
    label: str,
) -> dict[str, Mapping[str, object]]:
    records = _require_sequence(value, label)
    result: dict[str, Mapping[str, object]] = {}
    for raw in records:
        record = _require_mapping(raw, f"{label} record")
        artifact_id = record.get("artifact_id")
        if not isinstance(artifact_id, str) or not artifact_id:
            raise LimitedNonPitReconstructionError(
                f"LIMITED_NON_PIT_ARTIFACT_ID_INVALID:{label}"
            )
        if artifact_id in result:
            raise LimitedNonPitReconstructionError(
                f"LIMITED_NON_PIT_ARTIFACT_ID_DUPLICATE:{artifact_id}"
            )
        result[artifact_id] = record
    return result


def _load_strict_mapping(path: Path, label: str) -> tuple[dict[str, object], bytes]:
    payload, content = _load_strict_json(path, label)
    if not isinstance(payload, dict):
        raise LimitedNonPitReconstructionError(
            f"LIMITED_NON_PIT_JSON_ROOT_NOT_MAPPING:{label}"
        )
    return payload, content


def _load_strict_json(path: Path, label: str) -> tuple[object, bytes]:
    content = _read_regular_file(path, label)
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise LimitedNonPitReconstructionError(
            f"LIMITED_NON_PIT_JSON_NOT_UTF8:{label}"
        ) from exc
    try:
        return load_strict_json_text(text, label=label), content
    except StrictJsonContractError as exc:
        raise LimitedNonPitReconstructionError(
            f"LIMITED_NON_PIT_JSON_INVALID:{label}:{exc}"
        ) from exc


def _validate_input_directory(path: Path, label: str) -> Path:
    if path.is_symlink():
        raise LimitedNonPitReconstructionError(
            f"LIMITED_NON_PIT_DIRECTORY_SYMLINK:{label}:{path}"
        )
    resolved = path.resolve(strict=True)
    if not resolved.is_dir():
        raise LimitedNonPitReconstructionError(
            f"LIMITED_NON_PIT_DIRECTORY_INVALID:{label}:{path}"
        )
    return resolved


def _validate_output_root(path: Path) -> None:
    if path.exists() and (not path.is_dir() or path.is_symlink()):
        raise LimitedNonPitReconstructionError(
            f"LIMITED_NON_PIT_OUTPUT_ROOT_INVALID:{path}"
        )
    path.mkdir(parents=True, exist_ok=True)


def _inventory_member_path(inventory_root: Path, member_path: Path) -> Path:
    candidate = (inventory_root / member_path).resolve(strict=True)
    if not candidate.is_relative_to(inventory_root):
        raise LimitedNonPitReconstructionError(
            f"LIMITED_NON_PIT_INVENTORY_MEMBER_OUTSIDE_ROOT:{member_path}"
        )
    return candidate


def _resolve_member(root: Path, value: str) -> Path:
    pure = PurePosixPath(value)
    if pure.is_absolute() or ".." in pure.parts or "\\" in value:
        raise LimitedNonPitReconstructionError(
            f"LIMITED_NON_PIT_MEMBER_PATH_INVALID:{value}"
        )
    candidate = (root / Path(*pure.parts)).resolve(strict=True)
    if not candidate.is_relative_to(root):
        raise LimitedNonPitReconstructionError(
            f"LIMITED_NON_PIT_MEMBER_PATH_OUTSIDE_ROOT:{value}"
        )
    return candidate


def _member_path(path: Path, *, root: Path) -> str:
    resolved = path.resolve(strict=True)
    resolved_root = root.resolve(strict=True)
    if not resolved.is_relative_to(resolved_root):
        raise LimitedNonPitReconstructionError(
            f"LIMITED_NON_PIT_MEMBER_OUTSIDE_ROOT:{path}"
        )
    return resolved.relative_to(resolved_root).as_posix()


def _read_regular_file(path: Path, label: str) -> bytes:
    if path.is_symlink():
        raise LimitedNonPitReconstructionError(
            f"LIMITED_NON_PIT_FILE_SYMLINK:{label}:{path}"
        )
    resolved = path.resolve(strict=True)
    if not resolved.is_file():
        raise LimitedNonPitReconstructionError(
            f"LIMITED_NON_PIT_FILE_INVALID:{label}:{path}"
        )
    return resolved.read_bytes()


def _require_hash_and_size(
    content: bytes,
    *,
    expected_sha256: object,
    label: str,
) -> None:
    if (
        not isinstance(expected_sha256, str)
        or _SHA256_PATTERN.fullmatch(expected_sha256) is None
        or sha256_bytes(content) != expected_sha256
    ):
        raise LimitedNonPitReconstructionError(
            f"LIMITED_NON_PIT_SHA256_DRIFT:{label}"
        )


def _csv_data_row_count(content: bytes) -> int:
    text = content.decode("utf-8")
    lines = text.splitlines()
    return max(len(lines) - 1, 0)


def _display_path(path: Path, *, project_root: Path) -> str:
    resolved = path.resolve(strict=False)
    if resolved.is_relative_to(project_root):
        return resolved.relative_to(project_root).as_posix()
    return str(resolved)


def _require_mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise LimitedNonPitReconstructionError(
            f"LIMITED_NON_PIT_MAPPING_REQUIRED:{label}"
        )
    return value


def _require_sequence(value: object, label: str) -> Sequence[object]:
    if not isinstance(value, list):
        raise LimitedNonPitReconstructionError(
            f"LIMITED_NON_PIT_LIST_REQUIRED:{label}"
        )
    return value


def _validate_bundle_id(value: str) -> None:
    if _BUNDLE_ID_PATTERN.fullmatch(value) is None:
        raise LimitedNonPitReconstructionError(
            f"LIMITED_NON_PIT_BUNDLE_ID_INVALID:{value}"
        )


def _validate_owner_decision_id(value: str) -> None:
    if _OWNER_DECISION_PATTERN.fullmatch(value) is None:
        raise LimitedNonPitReconstructionError(
            f"LIMITED_NON_PIT_OWNER_DECISION_ID_INVALID:{value}"
        )


def _parse_date(value: object, label: str) -> date:
    if not isinstance(value, str):
        raise LimitedNonPitReconstructionError(
            f"LIMITED_NON_PIT_DATE_INVALID:{label}"
        )
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise LimitedNonPitReconstructionError(
            f"LIMITED_NON_PIT_DATE_INVALID:{label}:{value}"
        ) from exc
    if parsed.isoformat() != value:
        raise LimitedNonPitReconstructionError(
            f"LIMITED_NON_PIT_DATE_NON_CANONICAL:{label}:{value}"
        )
    return parsed


def _parse_datetime(value: object, label: str) -> datetime:
    if not isinstance(value, str):
        raise LimitedNonPitReconstructionError(
            f"LIMITED_NON_PIT_DATETIME_INVALID:{label}"
        )
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise LimitedNonPitReconstructionError(
            f"LIMITED_NON_PIT_DATETIME_INVALID:{label}:{value}"
        ) from exc
    _require_aware_datetime(parsed, label)
    return parsed


def _require_aware_datetime(value: datetime, label: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise LimitedNonPitReconstructionError(
            f"LIMITED_NON_PIT_DATETIME_NAIVE:{label}"
        )
