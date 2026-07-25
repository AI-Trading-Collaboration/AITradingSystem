from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data.download_publication import resolve_download_publication
from ai_trading_system.platform.artifacts import (
    canonical_json_bytes,
    load_strict_json_path,
    sha256_path,
    write_json_atomic,
    write_markdown_atomic,
)
from ai_trading_system.trading_calendar import is_us_equity_trading_day

DAILY_INPUT_CAPTURE_SCHEMA_VERSION = "daily_input_capture_manifest.v1"
DAILY_INPUT_GAP_LEDGER_SCHEMA_VERSION = "daily_input_capture_gap_ledger.v1"
DAILY_INPUT_RECOVERY_QUEUE_SCHEMA_VERSION = "daily_input_capture_recovery_queue.v1"
DEFAULT_DAILY_INPUT_CAPTURE_POLICY_PATH = (
    PROJECT_ROOT / "config" / "operations" / "daily_input_capture.yaml"
)
_REDACTED_ENV_FRAGMENTS = ("KEY", "TOKEN", "SECRET", "PASSWORD", "AUTHORIZATION")
_ERROR_SUMMARY_LIMIT = 600
_SUPPORTED_COMPONENT_IDS = (
    "market_macro",
    "fmp_forward_pit",
    "sec_companyfacts",
    "fmp_valuation",
    "official_policy_sources",
)
_SUPPORTED_BLOCKER_CODES = frozenset(
    {
        "CREDENTIAL_MISSING",
        "FILESYSTEM_INTEGRITY_FAILURE",
        "NONE",
        "PROVIDER_PERMISSION_DENIED",
        "PROVIDER_QUOTA_EXHAUSTED",
        "PROVIDER_SCHEMA_INVALID",
        "PROVIDER_UNAVAILABLE",
        "REQUEST_FAILED",
        "SOURCE_ATTEMPT_BUDGET_EXHAUSTED",
        "SOURCE_LEASE_CONFLICT",
        "SOURCE_STATE_INVALID",
    }
)
_SUPPORTED_RECOVERY_MODES = frozenset(
    {
        "HISTORICAL_RECAPTURE_FORBIDDEN",
        "IMMUTABLE_RAW_BACKFILL",
        "MANUAL_NON_PIT_RAW_REVIEW",
    }
)


@dataclass(frozen=True)
class CaptureComponentPolicy:
    source_revision: str
    supersedes_source_revisions: tuple[str, ...]
    max_attempts: int
    retry_delay_seconds: int
    retryable_blocker_codes: tuple[str, ...]
    recovery_mode: str


@dataclass(frozen=True)
class DailyInputCapturePolicy:
    policy_version: str
    owner: str
    status: str
    tracking_start: date
    calendar_authority: str
    decision_session_policy: str
    required_components: tuple[str, ...]
    raw_root: Path
    processed_root: Path
    external_root: Path
    report_root: Path
    source_control_root: Path
    blocker_taxonomy_version: str
    lease_ttl_seconds: int
    component_policies: Mapping[str, CaptureComponentPolicy]
    safety: Mapping[str, object]


@dataclass(frozen=True)
class DailyInputCapturePaths:
    as_of: date
    raw_root: Path
    processed_root: Path
    external_root: Path
    report_root: Path

    @property
    def manifest_json(self) -> Path:
        return self.report_root / f"daily_input_capture_manifest_{self.as_of.isoformat()}.json"

    @property
    def manifest_markdown(self) -> Path:
        return self.report_root / f"daily_input_capture_manifest_{self.as_of.isoformat()}.md"

    @property
    def validation_json(self) -> Path:
        return self.report_root / f"daily_input_capture_validation_{self.as_of.isoformat()}.json"

    @property
    def validation_markdown(self) -> Path:
        return self.report_root / f"daily_input_capture_validation_{self.as_of.isoformat()}.md"

    @property
    def gap_ledger_json(self) -> Path:
        return self.report_root.parent / "daily_input_capture_gap_ledger.json"

    @property
    def gap_ledger_markdown(self) -> Path:
        return self.report_root.parent / "daily_input_capture_gap_ledger.md"

    @property
    def recovery_queue_json(self) -> Path:
        return self.report_root.parent / "daily_input_capture_recovery_queue.json"

    @property
    def recovery_queue_markdown(self) -> Path:
        return self.report_root.parent / "daily_input_capture_recovery_queue.md"

    @property
    def recovery_queue_validation_json(self) -> Path:
        return self.report_root.parent / "daily_input_capture_recovery_queue_validation.json"

    @property
    def recovery_queue_validation_markdown(self) -> Path:
        return self.report_root.parent / "daily_input_capture_recovery_queue_validation.md"

    @property
    def pit_raw_dir(self) -> Path:
        return self.raw_root / "fmp_forward_pit"

    @property
    def market_macro_dir(self) -> Path:
        return self.raw_root / "market_macro"

    @property
    def market_prices_path(self) -> Path:
        return self.market_macro_dir / "prices_daily.csv"

    @property
    def market_secondary_prices_path(self) -> Path:
        return self.market_macro_dir / "prices_marketstack_daily.csv"

    @property
    def market_rates_path(self) -> Path:
        return self.market_macro_dir / "rates_daily.csv"

    @property
    def market_download_manifest_path(self) -> Path:
        return self.market_macro_dir / "download_manifest.csv"

    @property
    def pit_normalized_path(self) -> Path:
        return self.processed_root / f"fmp_forward_pit_{self.as_of.isoformat()}.csv"

    @property
    def pit_manifest_path(self) -> Path:
        return self.raw_root / "pit_snapshot_manifest.csv"

    @property
    def pit_fetch_report_path(self) -> Path:
        return self.report_root / f"fmp_forward_pit_fetch_{self.as_of.isoformat()}.md"

    @property
    def pit_validation_report_path(self) -> Path:
        return self.report_root / f"pit_snapshot_validation_{self.as_of.isoformat()}.md"

    @property
    def sec_companyfacts_dir(self) -> Path:
        return self.raw_root / "sec_companyfacts"

    @property
    def analyst_history_dir(self) -> Path:
        return self.raw_root / "fmp_analyst_estimates"

    @property
    def valuation_dir(self) -> Path:
        return self.external_root / "valuation_snapshots"

    @property
    def valuation_fetch_report_path(self) -> Path:
        return self.report_root / f"valuation_fmp_fetch_{self.as_of.isoformat()}.md"

    @property
    def valuation_validation_report_path(self) -> Path:
        return self.report_root / f"valuation_validation_{self.as_of.isoformat()}.md"

    @property
    def official_raw_dir(self) -> Path:
        return self.raw_root / "official_policy_sources"

    @property
    def official_processed_dir(self) -> Path:
        return self.processed_root / "official_policy"

    @property
    def official_candidates_path(self) -> Path:
        return (
            self.official_processed_dir
            / f"official_policy_source_candidates_{self.as_of.isoformat()}.csv"
        )

    @property
    def official_fetch_report_path(self) -> Path:
        return self.report_root / f"official_policy_source_fetch_{self.as_of.isoformat()}.md"

    @property
    def official_download_manifest_path(self) -> Path:
        return self.raw_root / "official_policy_download_manifest.csv"


@dataclass(frozen=True)
class CaptureComponent:
    component_id: str
    command: tuple[str, ...]
    expected_paths: tuple[Path, ...]
    required: bool
    source_revision: str = ""
    supersedes_source_revisions: tuple[str, ...] = ()
    max_attempts: int = 1
    retry_delay_seconds: int = 0
    retryable_blocker_codes: tuple[str, ...] = ()
    recovery_mode: str = "HISTORICAL_RECAPTURE_FORBIDDEN"
    snapshot_sources: tuple[Path, ...] = ()
    source_owned_paths: tuple[Path, ...] = ()


@dataclass(frozen=True)
class DailyInputCaptureResult:
    status: str
    validation_status: str
    manifest_path: Path
    validation_path: Path
    gap_ledger_path: Path
    recovery_queue_path: Path
    component_results: tuple[Mapping[str, object], ...]

    @property
    def passed(self) -> bool:
        return self.status == "CAPTURED"

    @property
    def closure_passed(self) -> bool:
        return self.validation_status == "PASS"


CaptureRunner = Callable[..., subprocess.CompletedProcess[str]]
CaptureSnapshotter = Callable[[CaptureComponent], None]
CaptureSleeper = Callable[[float], None]
CaptureClock = Callable[[], datetime]


def load_daily_input_capture_policy(
    path: Path = DEFAULT_DAILY_INPUT_CAPTURE_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> DailyInputCapturePolicy:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise ValueError("daily input capture policy must be a mapping")
    if raw.get("schema_version") != "daily_input_capture_policy.v2":
        raise ValueError("unsupported daily input capture policy schema_version")
    if raw.get("status") != "REVIEWED":
        raise ValueError("daily input capture policy must be REVIEWED")
    required_components = raw.get("required_components")
    paths = raw.get("paths")
    source_control = raw.get("source_control")
    safety = raw.get("safety")
    if not isinstance(required_components, list) or not all(
        isinstance(item, str) and item for item in required_components
    ):
        raise ValueError("required_components must be a non-empty string list")
    if len(set(required_components)) != len(required_components):
        raise ValueError("required_components must not contain duplicates")
    unknown_components = sorted(set(required_components) - set(_SUPPORTED_COMPONENT_IDS))
    if unknown_components:
        raise ValueError("unknown required capture components: " + ", ".join(unknown_components))
    if (
        not isinstance(paths, Mapping)
        or not isinstance(source_control, Mapping)
        or not isinstance(safety, Mapping)
    ):
        raise ValueError("daily input capture policy paths/source_control/safety must be mappings")
    _validate_safety(safety)

    component_policy_payload = source_control.get("component_policies")
    if not isinstance(component_policy_payload, Mapping):
        raise ValueError("source_control.component_policies must be a mapping")
    if set(component_policy_payload) != set(_SUPPORTED_COMPONENT_IDS):
        raise ValueError("source_control.component_policies must cover every supported component")
    component_policies: dict[str, CaptureComponentPolicy] = {}
    for component_id in _SUPPORTED_COMPONENT_IDS:
        component_raw = component_policy_payload.get(component_id)
        if not isinstance(component_raw, Mapping):
            raise ValueError(f"component policy must be a mapping: {component_id}")
        source_revision = _required_text(component_raw, "source_revision")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", source_revision):
            raise ValueError(f"invalid source_revision: {component_id}")
        supersedes_raw = component_raw.get("supersedes_source_revisions")
        if not isinstance(supersedes_raw, list) or not all(
            isinstance(item, str) and item for item in supersedes_raw
        ):
            raise ValueError(f"invalid supersedes_source_revisions: {component_id}")
        if (
            len(set(supersedes_raw)) != len(supersedes_raw)
            or source_revision in supersedes_raw
        ):
            raise ValueError(f"invalid source revision lineage: {component_id}")
        max_attempts = _positive_int(component_raw.get("max_attempts"), "max_attempts")
        retry_delay_seconds = _non_negative_int(
            component_raw.get("retry_delay_seconds"),
            "retry_delay_seconds",
        )
        retryable_raw = component_raw.get("retryable_blocker_codes")
        if not isinstance(retryable_raw, list) or not all(
            isinstance(item, str) and item in _SUPPORTED_BLOCKER_CODES for item in retryable_raw
        ):
            raise ValueError(f"invalid retryable_blocker_codes: {component_id}")
        if len(set(retryable_raw)) != len(retryable_raw):
            raise ValueError(f"duplicate retryable_blocker_codes: {component_id}")
        recovery_mode = _required_text(component_raw, "recovery_mode")
        if recovery_mode not in _SUPPORTED_RECOVERY_MODES:
            raise ValueError(f"unsupported recovery_mode: {component_id}:{recovery_mode}")
        component_policies[component_id] = CaptureComponentPolicy(
            source_revision=source_revision,
            supersedes_source_revisions=tuple(supersedes_raw),
            max_attempts=max_attempts,
            retry_delay_seconds=retry_delay_seconds,
            retryable_blocker_codes=tuple(retryable_raw),
            recovery_mode=recovery_mode,
        )
    lease_ttl_seconds = _positive_int(
        source_control.get("lease_ttl_seconds"),
        "lease_ttl_seconds",
    )

    def resolve_path(key: str) -> Path:
        value = paths.get(key)
        if not isinstance(value, str) or not value:
            raise ValueError(f"daily input capture policy path missing: {key}")
        candidate = Path(value)
        return candidate if candidate.is_absolute() else project_root / candidate

    tracking_value = raw.get("tracking_start")
    if isinstance(tracking_value, date):
        tracking_start = tracking_value
    elif isinstance(tracking_value, str) and tracking_value:
        tracking_start = date.fromisoformat(tracking_value)
    else:
        raise ValueError("daily input capture policy missing tracking_start")
    return DailyInputCapturePolicy(
        policy_version=_required_text(raw, "policy_version"),
        owner=_required_text(raw, "owner"),
        status=_required_text(raw, "status"),
        tracking_start=tracking_start,
        calendar_authority=_required_text(raw, "calendar_authority"),
        decision_session_policy=_required_text(raw, "decision_session_policy"),
        required_components=tuple(required_components),
        raw_root=resolve_path("raw_root"),
        processed_root=resolve_path("processed_root"),
        external_root=resolve_path("external_root"),
        report_root=resolve_path("report_root"),
        source_control_root=resolve_path("source_control_root"),
        blocker_taxonomy_version=_required_text(
            source_control,
            "blocker_taxonomy_version",
        ),
        lease_ttl_seconds=lease_ttl_seconds,
        component_policies=component_policies,
        safety=dict(safety),
    )


def daily_input_capture_paths(
    as_of: date,
    *,
    policy: DailyInputCapturePolicy,
) -> DailyInputCapturePaths:
    suffix = as_of.isoformat()
    return DailyInputCapturePaths(
        as_of=as_of,
        raw_root=policy.raw_root / suffix,
        processed_root=policy.processed_root / suffix,
        external_root=policy.external_root / suffix,
        report_root=policy.report_root / suffix,
    )


def build_daily_input_capture_components(
    *,
    as_of: date,
    paths: DailyInputCapturePaths,
    policy: DailyInputCapturePolicy,
    project_root: Path = PROJECT_ROOT,
    download_start: date = date(2018, 1, 1),
    full_universe: bool = False,
) -> tuple[CaptureComponent, ...]:
    as_of_text = as_of.isoformat()
    required = set(policy.required_components)
    market_raw_dir = project_root / "data" / "raw"
    market_command = [
        "aits",
        "download-data",
        "--start",
        download_start.isoformat(),
        "--end",
        as_of_text,
    ]
    if full_universe:
        market_command.append("--full-universe")
    component_policy = policy.component_policies
    components = (
        CaptureComponent(
            component_id="market_macro",
            command=tuple(market_command),
            expected_paths=(
                paths.market_prices_path,
                paths.market_secondary_prices_path,
                paths.market_rates_path,
                paths.market_download_manifest_path,
            ),
            required="market_macro" in required,
            source_revision=component_policy["market_macro"].source_revision,
            supersedes_source_revisions=component_policy[
                "market_macro"
            ].supersedes_source_revisions,
            max_attempts=component_policy["market_macro"].max_attempts,
            retry_delay_seconds=component_policy["market_macro"].retry_delay_seconds,
            retryable_blocker_codes=component_policy[
                "market_macro"
            ].retryable_blocker_codes,
            recovery_mode=component_policy["market_macro"].recovery_mode,
            snapshot_sources=(
                market_raw_dir / "prices_daily.csv",
                market_raw_dir / "prices_marketstack_daily.csv",
                market_raw_dir / "rates_daily.csv",
                market_raw_dir / "download_manifest.csv",
            ),
        ),
        CaptureComponent(
            component_id="fmp_forward_pit",
            command=(
                "aits",
                "pit-snapshots",
                "fetch-fmp-forward",
                "--as-of",
                as_of_text,
                "--raw-output-dir",
                str(paths.pit_raw_dir),
                "--normalized-output-path",
                str(paths.pit_normalized_path),
                "--manifest-path",
                str(paths.pit_manifest_path),
                "--pit-validation-report-path",
                str(paths.pit_validation_report_path),
                "--output-path",
                str(paths.pit_fetch_report_path),
            ),
            expected_paths=(
                paths.pit_raw_dir,
                paths.pit_normalized_path,
                paths.pit_manifest_path,
                paths.pit_fetch_report_path,
                paths.pit_validation_report_path,
            ),
            # The PIT manifest is a consumer-owned cross-kind aggregate. Older
            # daily plans rebuilt it in place after capture, so it cannot be a
            # source/session idempotency commitment. Raw payloads, normalized
            # bytes, and component reports remain exact reuse authority.
            source_owned_paths=(
                paths.pit_raw_dir,
                paths.pit_normalized_path,
                paths.pit_fetch_report_path,
                paths.pit_validation_report_path,
            ),
            required="fmp_forward_pit" in required,
            source_revision=component_policy["fmp_forward_pit"].source_revision,
            supersedes_source_revisions=component_policy[
                "fmp_forward_pit"
            ].supersedes_source_revisions,
            max_attempts=component_policy["fmp_forward_pit"].max_attempts,
            retry_delay_seconds=component_policy["fmp_forward_pit"].retry_delay_seconds,
            retryable_blocker_codes=component_policy[
                "fmp_forward_pit"
            ].retryable_blocker_codes,
            recovery_mode=component_policy["fmp_forward_pit"].recovery_mode,
        ),
        CaptureComponent(
            component_id="sec_companyfacts",
            command=(
                "aits",
                "fundamentals",
                "download-sec-companyfacts",
                "--output-dir",
                str(paths.sec_companyfacts_dir),
            ),
            expected_paths=(
                paths.sec_companyfacts_dir,
                paths.sec_companyfacts_dir / "sec_companyfacts_manifest.csv",
            ),
            required="sec_companyfacts" in required,
            source_revision=component_policy["sec_companyfacts"].source_revision,
            supersedes_source_revisions=component_policy[
                "sec_companyfacts"
            ].supersedes_source_revisions,
            max_attempts=component_policy["sec_companyfacts"].max_attempts,
            retry_delay_seconds=component_policy["sec_companyfacts"].retry_delay_seconds,
            retryable_blocker_codes=component_policy[
                "sec_companyfacts"
            ].retryable_blocker_codes,
            recovery_mode=component_policy["sec_companyfacts"].recovery_mode,
        ),
        CaptureComponent(
            component_id="fmp_valuation",
            command=(
                "aits",
                "valuation",
                "fetch-fmp",
                "--as-of",
                as_of_text,
                "--output-dir",
                str(paths.valuation_dir),
                "--analyst-history-dir",
                str(paths.analyst_history_dir),
                "--pit-normalized-path",
                str(paths.pit_normalized_path),
                "--output-path",
                str(paths.valuation_fetch_report_path),
                "--validation-report-path",
                str(paths.valuation_validation_report_path),
            ),
            expected_paths=(
                paths.valuation_dir,
                paths.analyst_history_dir,
                paths.valuation_fetch_report_path,
                paths.valuation_validation_report_path,
            ),
            required="fmp_valuation" in required,
            source_revision=component_policy["fmp_valuation"].source_revision,
            supersedes_source_revisions=component_policy[
                "fmp_valuation"
            ].supersedes_source_revisions,
            max_attempts=component_policy["fmp_valuation"].max_attempts,
            retry_delay_seconds=component_policy["fmp_valuation"].retry_delay_seconds,
            retryable_blocker_codes=component_policy[
                "fmp_valuation"
            ].retryable_blocker_codes,
            recovery_mode=component_policy["fmp_valuation"].recovery_mode,
        ),
        CaptureComponent(
            component_id="official_policy_sources",
            command=(
                "aits",
                "risk-events",
                "fetch-official-sources",
                "--as-of",
                as_of_text,
                "--raw-dir",
                str(paths.official_raw_dir),
                "--processed-dir",
                str(paths.official_processed_dir),
                "--download-manifest-path",
                str(paths.official_download_manifest_path),
                "--output-path",
                str(paths.official_fetch_report_path),
            ),
            expected_paths=(
                paths.official_raw_dir / as_of_text,
                paths.official_candidates_path,
                paths.official_download_manifest_path,
                paths.official_fetch_report_path,
            ),
            required="official_policy_sources" in required,
            source_revision=component_policy["official_policy_sources"].source_revision,
            supersedes_source_revisions=component_policy[
                "official_policy_sources"
            ].supersedes_source_revisions,
            max_attempts=component_policy["official_policy_sources"].max_attempts,
            retry_delay_seconds=component_policy[
                "official_policy_sources"
            ].retry_delay_seconds,
            retryable_blocker_codes=component_policy[
                "official_policy_sources"
            ].retryable_blocker_codes,
            recovery_mode=component_policy["official_policy_sources"].recovery_mode,
        ),
    )
    return components


def capture_daily_inputs(
    *,
    as_of: date,
    project_root: Path = PROJECT_ROOT,
    policy_path: Path = DEFAULT_DAILY_INPUT_CAPTURE_POLICY_PATH,
    download_start: date = date(2018, 1, 1),
    full_universe: bool = False,
    env: Mapping[str, str] | None = None,
    runner: CaptureRunner = subprocess.run,
    snapshotter: CaptureSnapshotter | None = None,
    sleeper: CaptureSleeper = time.sleep,
    clock: CaptureClock | None = None,
    generated_at: datetime | None = None,
) -> DailyInputCaptureResult:
    captured_at = generated_at or datetime.now(tz=UTC)
    if captured_at.tzinfo is None or captured_at.utcoffset() is None:
        raise ValueError("generated_at must be timezone-aware")
    if not is_us_equity_trading_day(as_of):
        raise ValueError(f"{as_of.isoformat()} is not an XNYS trading session")
    if download_start > as_of:
        raise ValueError("download_start must not be later than as_of")

    policy = load_daily_input_capture_policy(policy_path, project_root=project_root)
    if as_of < policy.tracking_start:
        raise ValueError(
            f"{as_of.isoformat()} precedes reviewed tracking_start "
            f"{policy.tracking_start.isoformat()}"
        )
    paths = daily_input_capture_paths(as_of, policy=policy)
    components = build_daily_input_capture_components(
        as_of=as_of,
        paths=paths,
        policy=policy,
        project_root=project_root,
        download_start=download_start,
        full_universe=full_universe,
    )
    checked_env = dict(os.environ if env is None else env)
    component_snapshotter = snapshotter or _snapshot_component_sources
    capture_clock = clock or _utc_now
    component_results: list[dict[str, object]] = []
    for component in components:
        component_results.append(
            _capture_component_with_source_control(
                component=component,
                as_of=as_of,
                policy=policy,
                policy_path=policy_path,
                project_root=project_root,
                env=checked_env,
                runner=runner,
                snapshotter=component_snapshotter,
                sleeper=sleeper,
                clock=capture_clock,
            )
        )

    required_passed = all(
        result["status"] == "PASS" for result in component_results if bool(result["required"])
    )
    status = "CAPTURED" if required_passed else "PARTIAL_CAPTURE"
    manifest = {
        "schema_version": DAILY_INPUT_CAPTURE_SCHEMA_VERSION,
        "policy_version": policy.policy_version,
        "policy_path": _relative_path(policy_path, project_root),
        "policy_sha256": sha256_path(policy_path),
        "as_of": as_of.isoformat(),
        "captured_at": captured_at.isoformat(),
        "calendar_authority": policy.calendar_authority,
        "decision_session_policy": policy.decision_session_policy,
        "blocker_taxonomy_version": policy.blocker_taxonomy_version,
        "status": status,
        "required_components": list(policy.required_components),
        "component_results": component_results,
        "data_quality_status": "NOT_EVALUATED",
        "pit_consumption_authorized": False,
        "score_allowed": False,
        "consumer_cutover_allowed": False,
        "production_effect": "none",
        "production_weight_write": False,
        "active_shadow_weight_write": False,
        "broker_action": False,
        "trading_action": False,
    }
    paths.report_root.mkdir(parents=True, exist_ok=True)
    write_json_atomic(paths.manifest_json, manifest)
    write_markdown_atomic(paths.manifest_markdown, _manifest_markdown(manifest))
    validation = validate_daily_input_capture_manifest(
        paths.manifest_json,
        project_root=project_root,
        policy_path=policy_path,
    )
    write_json_atomic(paths.validation_json, validation)
    write_markdown_atomic(paths.validation_markdown, _validation_markdown(validation))
    ledger = build_daily_input_capture_gap_ledger(
        as_of=as_of,
        project_root=project_root,
        policy_path=policy_path,
    )
    write_json_atomic(paths.gap_ledger_json, ledger)
    write_markdown_atomic(paths.gap_ledger_markdown, _gap_ledger_markdown(ledger))
    recovery_queue = build_daily_input_capture_recovery_queue(
        gap_ledger=ledger,
        project_root=project_root,
        policy_path=policy_path,
    )
    write_json_atomic(paths.recovery_queue_json, recovery_queue)
    write_markdown_atomic(
        paths.recovery_queue_markdown,
        _recovery_queue_markdown(recovery_queue),
    )
    recovery_validation = validate_daily_input_capture_recovery_queue(
        paths.recovery_queue_json,
        project_root=project_root,
        policy_path=policy_path,
    )
    write_json_atomic(paths.recovery_queue_validation_json, recovery_validation)
    write_markdown_atomic(
        paths.recovery_queue_validation_markdown,
        _recovery_queue_validation_markdown(recovery_validation),
    )
    return DailyInputCaptureResult(
        status=status if validation["status"] == "PASS" else "PARTIAL_CAPTURE",
        validation_status=str(validation["status"]),
        manifest_path=paths.manifest_json,
        validation_path=paths.validation_json,
        gap_ledger_path=paths.gap_ledger_json,
        recovery_queue_path=paths.recovery_queue_json,
        component_results=tuple(component_results),
    )


def _capture_component_with_source_control(
    *,
    component: CaptureComponent,
    as_of: date,
    policy: DailyInputCapturePolicy,
    policy_path: Path,
    project_root: Path,
    env: Mapping[str, str],
    runner: CaptureRunner,
    snapshotter: CaptureSnapshotter,
    sleeper: CaptureSleeper,
    clock: CaptureClock,
) -> dict[str, object]:
    source_root = policy.source_control_root / as_of.isoformat() / component.component_id
    state_path = source_root / "state.json"
    lock_path = source_root / "active.lock"
    idempotency_key = _source_idempotency_key(
        component=component,
        as_of=as_of,
    )
    policy_sha256 = sha256_path(policy_path)
    started_at = _aware_now(clock)
    state, state_issue, superseded_revision = _load_source_state(
        state_path,
        component=component,
        as_of=as_of,
        idempotency_key=idempotency_key,
    )
    superseded_state_path: Path | None = None
    if superseded_revision is not None:
        superseded_state_path = state_path
        revision_root = source_root / "revisions" / component.source_revision
        state_path = revision_root / "state.json"
        lock_path = revision_root / "active.lock"
        state, state_issue, nested_superseded_revision = _load_source_state(
            state_path,
            component=component,
            as_of=as_of,
            idempotency_key=idempotency_key,
        )
        if nested_superseded_revision is not None:
            state_issue = "revision-scoped source state cannot itself be superseded"
    if state_issue is not None:
        return _source_blocked_result(
            component=component,
            project_root=project_root,
            started_at=started_at,
            ended_at=_aware_now(clock),
            idempotency_key=idempotency_key,
            lease_status="NOT_ACQUIRED",
            blocker_code="SOURCE_STATE_INVALID",
            error_summary=state_issue,
            attempt_history=(),
        )
    prior_attempts = list(state.get("attempts", [])) if state else []
    if state and state.get("status") == "PASS":
        prior_result = state.get("component_result")
        reusable_result = (
            _reusable_pass_component_result(
                prior_result,
                component=component,
                project_root=project_root,
            )
            if isinstance(prior_result, Mapping)
            else None
        )
        if reusable_result is not None:
            return {
                **reusable_result,
                "source_lease_status": "REUSED_PASS",
                "idempotency_reused": True,
                "source_revision": component.source_revision,
                "source_state_path": _relative_path(state_path, project_root),
            }
        return _source_blocked_result(
            component=component,
            project_root=project_root,
            started_at=started_at,
            ended_at=_aware_now(clock),
            idempotency_key=idempotency_key,
            lease_status="NOT_ACQUIRED",
            blocker_code="SOURCE_STATE_INVALID",
            error_summary="terminal PASS source state artifact drift",
            attempt_history=tuple(prior_attempts),
        )
    if state and state.get("status") == "FAIL" and state.get("retry_allowed") is False:
        prior_result = state.get("component_result")
        if isinstance(prior_result, Mapping):
            return {
                **dict(prior_result),
                "source_lease_status": "REUSED_TERMINAL",
                "idempotency_reused": True,
                "source_revision": component.source_revision,
                "source_state_path": _relative_path(state_path, project_root),
            }
    if len(prior_attempts) >= component.max_attempts:
        return _source_blocked_result(
            component=component,
            project_root=project_root,
            started_at=started_at,
            ended_at=_aware_now(clock),
            idempotency_key=idempotency_key,
            lease_status="NOT_ACQUIRED",
            blocker_code="SOURCE_ATTEMPT_BUDGET_EXHAUSTED",
            error_summary="reviewed source/session attempt budget exhausted",
            attempt_history=tuple(prior_attempts),
        )

    lease, lease_status, lease_issue = _acquire_source_lease(
        lock_path=lock_path,
        source_root=source_root,
        component=component,
        as_of=as_of,
        idempotency_key=idempotency_key,
        ttl_seconds=policy.lease_ttl_seconds,
        clock=clock,
    )
    if lease is None:
        return _source_blocked_result(
            component=component,
            project_root=project_root,
            started_at=started_at,
            ended_at=_aware_now(clock),
            idempotency_key=idempotency_key,
            lease_status=lease_status,
            blocker_code="SOURCE_LEASE_CONFLICT",
            error_summary=lease_issue or "active source/session lease exists",
            attempt_history=tuple(prior_attempts),
        )

    result: dict[str, object] | None = None
    try:
        while len(prior_attempts) < component.max_attempts:
            attempt_started = _aware_now(clock)
            return_code = 1
            stdout_text = ""
            stderr_text = ""
            exception_summary: str | None = None
            try:
                completed = runner(
                    _execution_command(component.command),
                    cwd=project_root,
                    env=env,
                    text=True,
                    capture_output=True,
                    check=False,
                )
                return_code = int(completed.returncode)
                stdout_text = completed.stdout or ""
                stderr_text = completed.stderr or ""
            except Exception as exc:
                exception_summary = f"{type(exc).__name__}: {exc}"
            if return_code == 0 and component.snapshot_sources:
                try:
                    snapshotter(component)
                except Exception as exc:
                    return_code = 1
                    exception_summary = f"{type(exc).__name__}: {exc}"
            missing_expected = tuple(
                _relative_path(path, project_root)
                for path in component.expected_paths
                if not path.exists()
            )
            artifacts = _artifact_records(component.expected_paths, project_root)
            passed = return_code == 0 and not missing_expected
            error_summary = _sanitize_error_summary(
                exception_summary or stderr_text or (stdout_text if return_code else ""),
                env,
            )
            blocker_code = (
                "NONE"
                if passed
                else _classify_source_blocker(
                    error_summary=error_summary,
                    return_code=return_code,
                    missing_expected_paths=missing_expected,
                )
            )
            retry_allowed = (
                not passed
                and blocker_code in component.retryable_blocker_codes
                and len(prior_attempts) + 1 < component.max_attempts
            )
            retry_after_seconds = (
                component.retry_delay_seconds if retry_allowed else None
            )
            attempt_ended = _aware_now(clock)
            attempt = {
                "attempt_number": len(prior_attempts) + 1,
                "started_at": attempt_started.isoformat(),
                "ended_at": attempt_ended.isoformat(),
                "duration_seconds": round(
                    (attempt_ended - attempt_started).total_seconds(),
                    6,
                ),
                "return_code": return_code,
                "status": "PASS" if passed else "FAIL",
                "blocker_code": blocker_code,
                "retry_allowed": retry_allowed,
                "retry_after_seconds": retry_after_seconds,
                "stdout_line_count": len(stdout_text.splitlines()),
                "stderr_line_count": len(stderr_text.splitlines()),
                "error_summary": error_summary or None,
                "source_lease_id": lease["lease_id"],
            }
            prior_attempts.append(attempt)
            result = {
                "component_id": component.component_id,
                "required": component.required,
                "status": "PASS" if passed else "FAIL",
                "return_code": return_code,
                "attempt_count": len(prior_attempts),
                "max_attempts": component.max_attempts,
                "started_at": started_at.isoformat(),
                "ended_at": attempt_ended.isoformat(),
                "duration_seconds": round(
                    (attempt_ended - started_at).total_seconds(),
                    6,
                ),
                "command": list(component.command),
                "stdout_line_count": len(stdout_text.splitlines()),
                "stderr_line_count": len(stderr_text.splitlines()),
                "error_summary": error_summary or None,
                "missing_expected_paths": list(missing_expected),
                "artifacts": artifacts,
                "blocker_code": blocker_code,
                "retry_allowed": retry_allowed,
                "retry_after_seconds": retry_after_seconds,
                "attempt_history": list(prior_attempts),
                "source_idempotency_key": idempotency_key,
                "source_revision": component.source_revision,
                "source_state_path": _relative_path(state_path, project_root),
                "superseded_state_path": (
                    _relative_path(superseded_state_path, project_root)
                    if superseded_state_path is not None
                    else None
                ),
                "source_lease_id": lease["lease_id"],
                "source_lease_status": lease_status,
                "idempotency_reused": False,
                "recovery_mode": component.recovery_mode,
            }
            source_state = {
                "schema_version": "daily_input_capture_source_state.v1",
                "policy_version": policy.policy_version,
                "policy_sha256": policy_sha256,
                "source_revision": component.source_revision,
                "supersedes_source_revisions": list(
                    component.supersedes_source_revisions
                ),
                "superseded_state_path": (
                    _relative_path(superseded_state_path, project_root)
                    if superseded_state_path is not None
                    else None
                ),
                "as_of": as_of.isoformat(),
                "component_id": component.component_id,
                "source_idempotency_key": idempotency_key,
                "status": "PASS" if passed else ("IN_PROGRESS" if retry_allowed else "FAIL"),
                "retry_allowed": retry_allowed,
                "attempts": list(prior_attempts),
                "component_result": result,
                "updated_at": attempt_ended.isoformat(),
                "production_effect": "none",
            }
            write_json_atomic(state_path, source_state)
            if passed or not retry_allowed:
                break
            sleeper(float(component.retry_delay_seconds))
        if result is None:
            return _source_blocked_result(
                component=component,
                project_root=project_root,
                started_at=started_at,
                ended_at=_aware_now(clock),
                idempotency_key=idempotency_key,
                lease_status=lease_status,
                blocker_code="SOURCE_ATTEMPT_BUDGET_EXHAUSTED",
                error_summary="reviewed source/session attempt budget exhausted",
                attempt_history=tuple(prior_attempts),
            )
        return result
    except Exception as exc:
        ended_at = _aware_now(clock)
        return _source_blocked_result(
            component=component,
            project_root=project_root,
            started_at=started_at,
            ended_at=ended_at,
            idempotency_key=idempotency_key,
            lease_status=lease_status,
            blocker_code="FILESYSTEM_INTEGRITY_FAILURE",
            error_summary=_sanitize_error_summary(f"{type(exc).__name__}: {exc}", env),
            attempt_history=tuple(prior_attempts),
            lease_id=str(lease["lease_id"]),
        )
    finally:
        _release_source_lease(lock_path, expected_lease_id=str(lease["lease_id"]))


def _source_blocked_result(
    *,
    component: CaptureComponent,
    project_root: Path,
    started_at: datetime,
    ended_at: datetime,
    idempotency_key: str,
    lease_status: str,
    blocker_code: str,
    error_summary: str,
    attempt_history: Sequence[object],
    lease_id: str | None = None,
) -> dict[str, object]:
    return {
        "component_id": component.component_id,
        "required": component.required,
        "status": "FAIL",
        "return_code": 1,
        "attempt_count": len(attempt_history),
        "max_attempts": component.max_attempts,
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "duration_seconds": round((ended_at - started_at).total_seconds(), 6),
        "command": list(component.command),
        "stdout_line_count": 0,
        "stderr_line_count": 0,
        "error_summary": error_summary,
        "missing_expected_paths": [
            _relative_path(path, project_root)
            for path in component.expected_paths
            if not path.exists()
        ],
        "artifacts": _artifact_records(component.expected_paths, project_root),
        "blocker_code": blocker_code,
        "retry_allowed": False,
        "retry_after_seconds": None,
        "attempt_history": list(attempt_history),
        "source_idempotency_key": idempotency_key,
        "source_revision": component.source_revision,
        "source_lease_id": lease_id,
        "source_lease_status": lease_status,
        "idempotency_reused": lease_status.startswith("REUSED"),
        "recovery_mode": component.recovery_mode,
    }


def _load_source_state(
    path: Path,
    *,
    component: CaptureComponent,
    as_of: date,
    idempotency_key: str,
) -> tuple[Mapping[str, object] | None, str | None, str | None]:
    if not path.exists():
        return None, None, None
    try:
        payload = load_strict_json_path(path)
    except (OSError, ValueError) as exc:
        return None, f"source state unreadable: {exc}", None
    if not isinstance(payload, Mapping):
        return None, "source state must be an object", None
    expected = {
        "schema_version": "daily_input_capture_source_state.v1",
        "as_of": as_of.isoformat(),
        "component_id": component.component_id,
        "production_effect": "none",
    }
    mismatches = [key for key, value in expected.items() if payload.get(key) != value]
    if mismatches:
        return None, "source state identity mismatch: " + ", ".join(mismatches), None
    observed_revision = payload.get("source_revision") or payload.get("policy_version")
    if payload.get("source_idempotency_key") != idempotency_key:
        if observed_revision in component.supersedes_source_revisions:
            return None, None, str(observed_revision)
        return None, "source state identity mismatch: source_idempotency_key", None
    if observed_revision != component.source_revision:
        return None, "source state identity mismatch: source_revision", None
    attempts = payload.get("attempts")
    if not isinstance(attempts, list):
        return None, "source state attempts must be a list", None
    result = payload.get("component_result")
    if isinstance(result, Mapping):
        if (
            result.get("command") != list(component.command)
            or result.get("max_attempts") != component.max_attempts
            or result.get("recovery_mode") != component.recovery_mode
        ):
            return None, "source state component contract mismatch", None
    return payload, None, None


def _reusable_pass_component_result(
    result: Mapping[str, object],
    *,
    component: CaptureComponent,
    project_root: Path,
) -> dict[str, object] | None:
    artifacts = result.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        return None
    recorded_by_path: dict[str, Mapping[str, object]] = {}
    for artifact in artifacts:
        if not isinstance(artifact, Mapping):
            return None
        path_value = artifact.get("path")
        if (
            not isinstance(path_value, str)
            or not path_value
            or path_value in recorded_by_path
        ):
            return None
        recorded_by_path[path_value] = artifact

    authority_paths = component.source_owned_paths or component.expected_paths
    current_records = _artifact_records(authority_paths, project_root)
    current_by_path = {str(item["path"]): item for item in current_records}
    allowed_excluded_paths = {
        _relative_path(path, project_root)
        for path in component.expected_paths
        if path not in authority_paths and path.is_file()
    }
    recorded_paths = set(recorded_by_path)
    current_paths = set(current_by_path)
    excluded_paths = recorded_paths - current_paths
    if (
        current_paths - recorded_paths
        or excluded_paths - allowed_excluded_paths
        or any(
            recorded_by_path[path].get("size_bytes")
            != current_by_path[path]["size_bytes"]
            or recorded_by_path[path].get("sha256") != current_by_path[path]["sha256"]
            for path in current_paths
        )
    ):
        return None

    reusable = dict(result)
    reusable["artifacts"] = current_records
    if excluded_paths:
        reusable["artifact_reuse_scope"] = "SOURCE_OWNED_ONLY"
        reusable["excluded_non_authoritative_artifacts"] = sorted(excluded_paths)
    return reusable


def _source_idempotency_key(
    *,
    component: CaptureComponent,
    as_of: date,
) -> str:
    payload = {
        "as_of": as_of.isoformat(),
        "command": list(component.command),
        "component_id": component.component_id,
        "policy_version": component.source_revision,
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return f"daily-input-source-{digest[:24]}"


def _acquire_source_lease(
    *,
    lock_path: Path,
    source_root: Path,
    component: CaptureComponent,
    as_of: date,
    idempotency_key: str,
    ttl_seconds: int,
    clock: CaptureClock,
) -> tuple[dict[str, object] | None, str, str | None]:
    source_root.mkdir(parents=True, exist_ok=True)
    stale_reclaimed = False
    if lock_path.exists():
        existing, issue = _read_source_lease(lock_path)
        if issue is not None:
            return None, "BLOCKED_INVALID_LEASE", issue
        now = _aware_now(clock)
        try:
            expires_at = datetime.fromisoformat(str(existing["expires_at"]))
        except (KeyError, ValueError):
            return None, "BLOCKED_INVALID_LEASE", "source lease expiry invalid"
        if expires_at.tzinfo is None or expires_at.utcoffset() is None:
            return None, "BLOCKED_INVALID_LEASE", "source lease expiry must be timezone-aware"
        if expires_at > now:
            return None, "BLOCKED_ACTIVE_LEASE", f"active lease expires at {expires_at.isoformat()}"
        reclaimed, reclaim_issue = _reclaim_stale_source_lease(
            lock_path=lock_path,
            source_root=source_root,
            clock=clock,
        )
        if not reclaimed:
            return None, "BLOCKED_LEASE_RECLAIM", reclaim_issue
        stale_reclaimed = True
    acquired_at = _aware_now(clock)
    lease_id = "source-lease-" + hashlib.sha256(
        (
            f"{idempotency_key}:{acquired_at.isoformat()}:{os.getpid()}"
        ).encode()
    ).hexdigest()[:20]
    lease = {
        "schema_version": "daily_input_capture_source_lease.v1",
        "lease_id": lease_id,
        "source_idempotency_key": idempotency_key,
        "component_id": component.component_id,
        "as_of": as_of.isoformat(),
        "acquired_at": acquired_at.isoformat(),
        "expires_at": (acquired_at + timedelta(seconds=ttl_seconds)).isoformat(),
        "pid": os.getpid(),
        "production_effect": "none",
    }
    try:
        _write_json_exclusive(lock_path, lease)
    except FileExistsError:
        return None, "BLOCKED_ACTIVE_LEASE", "source lease acquired concurrently"
    return lease, ("STALE_RECLAIMED" if stale_reclaimed else "ACQUIRED"), None


def _read_source_lease(path: Path) -> tuple[Mapping[str, object], str | None]:
    try:
        payload = load_strict_json_path(path)
    except (OSError, ValueError) as exc:
        return {}, f"source lease unreadable: {exc}"
    if not isinstance(payload, Mapping):
        return {}, "source lease must be an object"
    if payload.get("schema_version") != "daily_input_capture_source_lease.v1":
        return {}, "source lease schema mismatch"
    return payload, None


def _reclaim_stale_source_lease(
    *,
    lock_path: Path,
    source_root: Path,
    clock: CaptureClock,
) -> tuple[bool, str | None]:
    arbiter_path = source_root / "reclaim.lock"
    arbiter = {
        "schema_version": "daily_input_capture_source_lease_reclaim.v1",
        "pid": os.getpid(),
        "acquired_at": _aware_now(clock).isoformat(),
        "production_effect": "none",
    }
    try:
        _write_json_exclusive(arbiter_path, arbiter)
    except FileExistsError:
        return False, "stale lease reclaim already active"
    try:
        existing, issue = _read_source_lease(lock_path)
        if issue is not None:
            return False, issue
        now = _aware_now(clock)
        expires_at = datetime.fromisoformat(str(existing["expires_at"]))
        if expires_at > now:
            return False, "lease was refreshed before stale reclaim"
        history_root = source_root / "lease_history"
        history_root.mkdir(parents=True, exist_ok=True)
        lease_id = str(existing.get("lease_id", "unknown"))
        archive_path = history_root / f"{lease_id}.expired.json"
        if archive_path.exists():
            return False, "stale lease archive already exists"
        os.replace(lock_path, archive_path)
        return True, None
    except (OSError, ValueError, KeyError) as exc:
        return False, f"stale lease reclaim failed: {exc}"
    finally:
        _release_source_lease(
            arbiter_path,
            expected_lease_id=None,
            expected_schema="daily_input_capture_source_lease_reclaim.v1",
        )


def _write_json_exclusive(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(path, flags, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(canonical_json_bytes(payload))
            stream.flush()
            os.fsync(stream.fileno())
    except Exception:
        try:
            os.close(descriptor)
        except OSError:
            pass
        raise


def _release_source_lease(
    path: Path,
    *,
    expected_lease_id: str | None,
    expected_schema: str = "daily_input_capture_source_lease.v1",
) -> None:
    if not path.exists():
        return
    try:
        payload = load_strict_json_path(path)
    except (OSError, ValueError):
        return
    if not isinstance(payload, Mapping) or payload.get("schema_version") != expected_schema:
        return
    if expected_lease_id is not None and payload.get("lease_id") != expected_lease_id:
        return
    path.unlink()


def _classify_source_blocker(
    *,
    error_summary: str,
    return_code: int,
    missing_expected_paths: Sequence[str],
) -> str:
    normalized = error_summary.casefold()
    if return_code == 0 and missing_expected_paths:
        return "FILESYSTEM_INTEGRITY_FAILURE"
    if any(
        token in normalized
        for token in ("api key", "credential", "missing key", "token required")
    ):
        return "CREDENTIAL_MISSING"
    if any(token in normalized for token in ("401", "403", "forbidden", "permission denied")):
        return "PROVIDER_PERMISSION_DENIED"
    if any(token in normalized for token in ("429", "quota", "rate limit")):
        return "PROVIDER_QUOTA_EXHAUSTED"
    if any(
        token in normalized
        for token in ("schema", "unexpected column", "invalid payload", "json decode")
    ):
        return "PROVIDER_SCHEMA_INVALID"
    if any(
        token in normalized
        for token in ("timeout", "timed out", "connection", "502", "503", "504", "unavailable")
    ):
        return "PROVIDER_UNAVAILABLE"
    if "no space" in normalized or "read-only file system" in normalized:
        return "FILESYSTEM_INTEGRITY_FAILURE"
    return "REQUEST_FAILED"


def _validate_attempt_history(
    attempts: Sequence[object],
    *,
    component_id: str,
    component_policy: CaptureComponentPolicy | None,
    issues: list[dict[str, str]],
) -> None:
    for index, attempt in enumerate(attempts, start=1):
        if not isinstance(attempt, Mapping):
            issues.append(
                {
                    "code": "COMPONENT_ATTEMPT_RECORD_INVALID",
                    "message": f"{component_id}:{index}",
                }
            )
            continue
        if attempt.get("attempt_number") != index:
            issues.append(
                {
                    "code": "COMPONENT_ATTEMPT_NUMBER_INVALID",
                    "message": f"{component_id}:{index}",
                }
            )
        blocker_code = attempt.get("blocker_code")
        status = attempt.get("status")
        if blocker_code not in _SUPPORTED_BLOCKER_CODES or status not in {"PASS", "FAIL"}:
            issues.append(
                {
                    "code": "COMPONENT_ATTEMPT_OUTCOME_INVALID",
                    "message": f"{component_id}:{index}",
                }
            )
        if not isinstance(attempt.get("source_lease_id"), str):
            issues.append(
                {
                    "code": "COMPONENT_ATTEMPT_LEASE_ID_INVALID",
                    "message": f"{component_id}:{index}",
                }
            )
        if status == "PASS" and blocker_code != "NONE":
            issues.append(
                {
                    "code": "COMPONENT_ATTEMPT_PASS_BLOCKER_MISMATCH",
                    "message": f"{component_id}:{index}",
                }
            )
        retry_allowed = attempt.get("retry_allowed")
        retry_after = attempt.get("retry_after_seconds")
        if not isinstance(retry_allowed, bool):
            issues.append(
                {
                    "code": "COMPONENT_ATTEMPT_RETRY_INVALID",
                    "message": f"{component_id}:{index}",
                }
            )
        elif retry_allowed:
            if (
                component_policy is None
                or blocker_code not in component_policy.retryable_blocker_codes
                or retry_after != component_policy.retry_delay_seconds
                or index >= component_policy.max_attempts
            ):
                issues.append(
                    {
                        "code": "COMPONENT_ATTEMPT_RETRY_POLICY_MISMATCH",
                        "message": f"{component_id}:{index}",
                    }
                )
        elif retry_after is not None:
            issues.append(
                {
                    "code": "COMPONENT_ATTEMPT_RETRY_AFTER_INVALID",
                    "message": f"{component_id}:{index}",
                }
            )


def _utc_now() -> datetime:
    return datetime.now(tz=UTC)


def _aware_now(clock: CaptureClock) -> datetime:
    value = clock()
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("capture clock must return a timezone-aware datetime")
    return value


def validate_daily_input_capture_manifest(
    manifest_path: Path,
    *,
    project_root: Path = PROJECT_ROOT,
    policy_path: Path = DEFAULT_DAILY_INPUT_CAPTURE_POLICY_PATH,
) -> dict[str, object]:
    issues: list[dict[str, str]] = []
    try:
        payload = load_strict_json_path(manifest_path)
    except (OSError, ValueError) as exc:
        return {
            "schema_version": "daily_input_capture_validation.v1",
            "manifest_path": _relative_path(manifest_path, project_root),
            "status": "FAIL",
            "issues": [{"code": "MANIFEST_UNREADABLE", "message": str(exc)}],
            "production_effect": "none",
        }
    if not isinstance(payload, Mapping):
        issues.append({"code": "MANIFEST_NOT_MAPPING", "message": "manifest must be an object"})
        payload = {}
    policy = load_daily_input_capture_policy(policy_path, project_root=project_root)
    if payload.get("schema_version") != DAILY_INPUT_CAPTURE_SCHEMA_VERSION:
        issues.append({"code": "SCHEMA_VERSION_MISMATCH", "message": "unsupported manifest schema"})
    if payload.get("policy_version") != policy.policy_version:
        issues.append({"code": "POLICY_VERSION_MISMATCH", "message": "policy version drift"})
    if payload.get("policy_sha256") != sha256_path(policy_path):
        issues.append({"code": "POLICY_CHECKSUM_MISMATCH", "message": "policy checksum drift"})
    if payload.get("blocker_taxonomy_version") != policy.blocker_taxonomy_version:
        issues.append(
            {
                "code": "BLOCKER_TAXONOMY_VERSION_MISMATCH",
                "message": "blocker taxonomy version drift",
            }
        )
    for field, expected in (
        ("production_effect", "none"),
        ("production_weight_write", False),
        ("active_shadow_weight_write", False),
        ("broker_action", False),
        ("trading_action", False),
        ("consumer_cutover_allowed", False),
        ("pit_consumption_authorized", False),
        ("score_allowed", False),
        ("data_quality_status", "NOT_EVALUATED"),
    ):
        if payload.get(field) != expected:
            issues.append(
                {"code": "SAFETY_BOUNDARY_MISMATCH", "message": f"{field} must be {expected!r}"}
            )
    component_results = payload.get("component_results")
    if not isinstance(component_results, list):
        issues.append(
            {"code": "COMPONENT_RESULTS_INVALID", "message": "component_results must be a list"}
        )
        component_results = []
    observed_ids: set[str] = set()
    required_failures = 0
    required_ids = set(policy.required_components)
    allowed_artifact_roots: tuple[Path, ...] = ()
    as_of_value = payload.get("as_of")
    try:
        manifest_date = date.fromisoformat(str(as_of_value))
    except ValueError:
        issues.append({"code": "AS_OF_INVALID", "message": "as_of must be YYYY-MM-DD"})
    else:
        if manifest_date < policy.tracking_start or not is_us_equity_trading_day(manifest_date):
            issues.append(
                {
                    "code": "AS_OF_NOT_TRACKED_XNYS_SESSION",
                    "message": str(as_of_value),
                }
            )
        scoped_paths = daily_input_capture_paths(manifest_date, policy=policy)
        allowed_artifact_roots = (
            scoped_paths.raw_root,
            scoped_paths.processed_root,
            scoped_paths.external_root,
            scoped_paths.report_root,
        )
    for component in component_results:
        if not isinstance(component, Mapping):
            issues.append(
                {"code": "COMPONENT_RESULT_INVALID", "message": "component result must be object"}
            )
            continue
        component_id = str(component.get("component_id", ""))
        if not component_id or component_id in observed_ids:
            issues.append(
                {"code": "COMPONENT_ID_INVALID", "message": "component ids must be unique"}
            )
        observed_ids.add(component_id)
        if component_id not in _SUPPORTED_COMPONENT_IDS:
            issues.append({"code": "COMPONENT_ID_UNKNOWN", "message": component_id or "<empty>"})
        expected_required = component_id in required_ids
        if component.get("required") is not expected_required:
            issues.append(
                {
                    "code": "COMPONENT_REQUIRED_MISMATCH",
                    "message": component_id,
                }
            )
        return_code = component.get("return_code")
        missing_expected = component.get("missing_expected_paths")
        component_status = component.get("status")
        if (
            not isinstance(return_code, int)
            or isinstance(return_code, bool)
            or not isinstance(missing_expected, list)
        ):
            issues.append(
                {
                    "code": "COMPONENT_EXECUTION_FIELDS_INVALID",
                    "message": component_id,
                }
            )
        else:
            expected_component_status = (
                "PASS" if return_code == 0 and not missing_expected else "FAIL"
            )
            if component_status != expected_component_status:
                issues.append(
                    {
                        "code": "COMPONENT_STATUS_MISMATCH",
                        "message": component_id,
                    }
                )
        component_policy = policy.component_policies.get(component_id)
        expected_max_attempts = (
            component_policy.max_attempts if component_policy is not None else None
        )
        attempt_count = component.get("attempt_count")
        max_attempts = component.get("max_attempts")
        blocker_code = component.get("blocker_code")
        if (
            expected_max_attempts is None
            or max_attempts != expected_max_attempts
            or not isinstance(attempt_count, int)
            or isinstance(attempt_count, bool)
            or attempt_count < 0
            or attempt_count > expected_max_attempts
        ):
            issues.append(
                {
                    "code": "COMPONENT_ATTEMPT_BUDGET_MISMATCH",
                    "message": component_id,
                }
            )
        if blocker_code not in _SUPPORTED_BLOCKER_CODES:
            issues.append(
                {
                    "code": "COMPONENT_BLOCKER_CODE_INVALID",
                    "message": component_id,
                }
            )
        elif component_status == "PASS" and blocker_code != "NONE":
            issues.append(
                {
                    "code": "COMPONENT_PASS_BLOCKER_MISMATCH",
                    "message": component_id,
                }
            )
        elif component_status == "FAIL" and blocker_code == "NONE":
            issues.append(
                {
                    "code": "COMPONENT_FAIL_BLOCKER_MISMATCH",
                    "message": component_id,
                }
            )
        attempt_history = component.get("attempt_history")
        if (
            not isinstance(attempt_history, list)
            or not isinstance(attempt_count, int)
            or len(attempt_history) != attempt_count
        ):
            issues.append(
                {
                    "code": "COMPONENT_ATTEMPT_HISTORY_INVALID",
                    "message": component_id,
                }
            )
        else:
            _validate_attempt_history(
                attempt_history,
                component_id=component_id,
                component_policy=component_policy,
                issues=issues,
            )
        if (
            attempt_count == 0
            and blocker_code
            not in {
                "SOURCE_ATTEMPT_BUDGET_EXHAUSTED",
                "SOURCE_LEASE_CONFLICT",
                "SOURCE_STATE_INVALID",
            }
        ):
            issues.append(
                {
                    "code": "ZERO_ATTEMPT_BLOCKER_INVALID",
                    "message": component_id,
                }
            )
        if component_policy is not None and component.get("recovery_mode") != (
            component_policy.recovery_mode
        ):
            issues.append(
                {
                    "code": "COMPONENT_RECOVERY_MODE_MISMATCH",
                    "message": component_id,
                }
            )
        if not isinstance(component.get("source_idempotency_key"), str):
            issues.append(
                {
                    "code": "SOURCE_IDEMPOTENCY_KEY_INVALID",
                    "message": component_id,
                }
            )
        if component.get("source_lease_status") not in {
            "ACQUIRED",
            "BLOCKED_ACTIVE_LEASE",
            "BLOCKED_INVALID_LEASE",
            "BLOCKED_LEASE_RECLAIM",
            "NOT_ACQUIRED",
            "REUSED_PASS",
            "REUSED_TERMINAL",
            "STALE_RECLAIMED",
        }:
            issues.append(
                {
                    "code": "SOURCE_LEASE_STATUS_INVALID",
                    "message": component_id,
                }
            )
        if expected_required and component_status != "PASS":
            required_failures += 1
        artifacts = component.get("artifacts")
        if not isinstance(artifacts, list):
            issues.append(
                {"code": "ARTIFACT_LIST_INVALID", "message": f"{component_id}: artifacts invalid"}
            )
            continue
        for artifact in artifacts:
            _validate_artifact_record(
                artifact,
                project_root=project_root,
                allowed_roots=allowed_artifact_roots,
                issues=issues,
            )
    missing_components = sorted(set(policy.required_components) - observed_ids)
    if missing_components:
        issues.append(
            {
                "code": "REQUIRED_COMPONENT_MISSING",
                "message": ", ".join(missing_components),
            }
        )
    expected_status = (
        "CAPTURED" if required_failures == 0 and not missing_components else "PARTIAL_CAPTURE"
    )
    if payload.get("status") != expected_status:
        issues.append(
            {
                "code": "CAPTURE_STATUS_MISMATCH",
                "message": f"expected {expected_status}",
            }
        )
    return {
        "schema_version": "daily_input_capture_validation.v1",
        "manifest_path": _relative_path(manifest_path, project_root),
        "as_of": payload.get("as_of"),
        "capture_status": payload.get("status"),
        "status": "PASS" if not issues else "FAIL",
        "issue_count": len(issues),
        "issues": issues,
        "production_effect": "none",
        "consumer_cutover_allowed": False,
        "broker_action": False,
        "trading_action": False,
    }


def build_daily_input_capture_gap_ledger(
    *,
    as_of: date,
    project_root: Path = PROJECT_ROOT,
    policy_path: Path = DEFAULT_DAILY_INPUT_CAPTURE_POLICY_PATH,
) -> dict[str, object]:
    policy = load_daily_input_capture_policy(policy_path, project_root=project_root)
    rows: list[dict[str, object]] = []
    cursor = policy.tracking_start
    while cursor <= as_of:
        if is_us_equity_trading_day(cursor):
            paths = daily_input_capture_paths(cursor, policy=policy)
            if not paths.manifest_json.exists():
                row_status = "MISSED"
                capture_status = None
                validation_status = "MISSING"
                manifest_path = None
            else:
                validation = validate_daily_input_capture_manifest(
                    paths.manifest_json,
                    project_root=project_root,
                    policy_path=policy_path,
                )
                try:
                    capture_payload = load_strict_json_path(paths.manifest_json)
                except ValueError:
                    capture_payload = {}
                capture_status = (
                    capture_payload.get("status") if isinstance(capture_payload, Mapping) else None
                )
                validation_status = validation["status"]
                row_status = (
                    "CAPTURED"
                    if capture_status == "CAPTURED" and validation_status == "PASS"
                    else "PARTIAL_CAPTURE"
                )
                manifest_path = _relative_path(paths.manifest_json, project_root)
            rows.append(
                {
                    "session_date": cursor.isoformat(),
                    "status": row_status,
                    "capture_status": capture_status,
                    "validation_status": validation_status,
                    "manifest_path": manifest_path,
                }
            )
        cursor += timedelta(days=1)
    counts = {
        status: sum(1 for row in rows if row["status"] == status)
        for status in ("CAPTURED", "PARTIAL_CAPTURE", "MISSED", "INSUFFICIENT_DATA")
    }
    return {
        "schema_version": DAILY_INPUT_GAP_LEDGER_SCHEMA_VERSION,
        "policy_version": policy.policy_version,
        "tracking_start": policy.tracking_start.isoformat(),
        "as_of": as_of.isoformat(),
        "calendar_authority": policy.calendar_authority,
        "decision_session_policy": policy.decision_session_policy,
        "status": (
            "PASS" if counts["PARTIAL_CAPTURE"] == 0 and counts["MISSED"] == 0 else "GAPS_PRESENT"
        ),
        "counts": counts,
        "sessions": rows,
        "production_effect": "none",
        "consumer_cutover_allowed": False,
        "broker_action": False,
        "trading_action": False,
    }


def build_daily_input_capture_recovery_queue(
    *,
    gap_ledger: Mapping[str, object],
    project_root: Path = PROJECT_ROOT,
    policy_path: Path = DEFAULT_DAILY_INPUT_CAPTURE_POLICY_PATH,
) -> dict[str, object]:
    policy = load_daily_input_capture_policy(policy_path, project_root=project_root)
    sessions = gap_ledger.get("sessions")
    if (
        gap_ledger.get("schema_version") != DAILY_INPUT_GAP_LEDGER_SCHEMA_VERSION
        or not isinstance(sessions, list)
    ):
        raise ValueError("validated daily input capture gap ledger required")
    queue_items: list[dict[str, object]] = []
    for row in sessions:
        if not isinstance(row, Mapping):
            raise ValueError("gap ledger session row must be an object")
        row_status = row.get("status")
        if row_status not in {"MISSED", "PARTIAL_CAPTURE"}:
            continue
        session_date = date.fromisoformat(str(row.get("session_date")))
        failed_by_component: dict[str, Mapping[str, object]] = {}
        manifest_path_value = row.get("manifest_path")
        if isinstance(manifest_path_value, str) and manifest_path_value:
            manifest_path = Path(manifest_path_value)
            if not manifest_path.is_absolute():
                manifest_path = project_root / manifest_path
            try:
                manifest = load_strict_json_path(manifest_path)
            except (OSError, ValueError):
                manifest = {}
            if isinstance(manifest, Mapping):
                component_results = manifest.get("component_results")
                if isinstance(component_results, list):
                    failed_by_component = {
                        str(item.get("component_id")): item
                        for item in component_results
                        if isinstance(item, Mapping) and item.get("status") != "PASS"
                    }
        missing_components = (
            policy.required_components
            if row_status == "MISSED"
            else tuple(
                component_id
                for component_id in policy.required_components
                if component_id in failed_by_component
            )
        )
        for component_id in missing_components:
            component_policy = policy.component_policies[component_id]
            failed_result = failed_by_component.get(component_id, {})
            recovery_mode = component_policy.recovery_mode
            recovery_allowed = recovery_mode == "IMMUTABLE_RAW_BACKFILL"
            action_status = {
                "IMMUTABLE_RAW_BACKFILL": "READY_FOR_MANUAL_RECOVERY",
                "MANUAL_NON_PIT_RAW_REVIEW": "OWNER_REVIEW_REQUIRED",
                "HISTORICAL_RECAPTURE_FORBIDDEN": "INSUFFICIENT_DATA",
            }[recovery_mode]
            recovery_body = {
                "session_date": session_date.isoformat(),
                "component_id": component_id,
                "recovery_mode": recovery_mode,
                "source_gap_status": row_status,
            }
            recovery_id = "daily-input-recovery-" + hashlib.sha256(
                json.dumps(
                    recovery_body,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()[:20]
            queue_items.append(
                {
                    "recovery_id": recovery_id,
                    **recovery_body,
                    "action_status": action_status,
                    "recovery_allowed": recovery_allowed,
                    "automatic_execution_allowed": False,
                    "strict_pit_eligible": False,
                    "consumer_cutover_allowed": False,
                    "source_manifest_path": manifest_path_value,
                    "source_blocker_code": failed_result.get("blocker_code"),
                    "source_attempt_count": failed_result.get("attempt_count", 0),
                    "source_idempotency_key": failed_result.get(
                        "source_idempotency_key"
                    ),
                    "production_effect": "none",
                    "broker_action": False,
                    "trading_action": False,
                }
            )
    queue_items.sort(key=lambda item: (str(item["session_date"]), str(item["component_id"])))
    counts = {
        status: sum(1 for item in queue_items if item["action_status"] == status)
        for status in (
            "READY_FOR_MANUAL_RECOVERY",
            "OWNER_REVIEW_REQUIRED",
            "INSUFFICIENT_DATA",
        )
    }
    return {
        "schema_version": DAILY_INPUT_RECOVERY_QUEUE_SCHEMA_VERSION,
        "policy_version": policy.policy_version,
        "policy_path": _relative_path(policy_path, project_root),
        "policy_sha256": sha256_path(policy_path),
        "as_of": gap_ledger.get("as_of"),
        "calendar_authority": policy.calendar_authority,
        "status": "PASS" if not queue_items else "GAPS_PRESENT",
        "item_count": len(queue_items),
        "counts": counts,
        "items": queue_items,
        "automatic_execution_allowed": False,
        "historical_strict_pit_backfill_allowed": False,
        "old_terminal_state_mutation_allowed": False,
        "consumer_cutover_allowed": False,
        "production_effect": "none",
        "broker_action": False,
        "trading_action": False,
    }


def validate_daily_input_capture_recovery_queue(
    queue_path: Path,
    *,
    project_root: Path = PROJECT_ROOT,
    policy_path: Path = DEFAULT_DAILY_INPUT_CAPTURE_POLICY_PATH,
) -> dict[str, object]:
    issues: list[dict[str, str]] = []
    try:
        payload = load_strict_json_path(queue_path)
    except (OSError, ValueError) as exc:
        return {
            "schema_version": "daily_input_capture_recovery_queue_validation.v1",
            "queue_path": _relative_path(queue_path, project_root),
            "status": "FAIL",
            "issues": [{"code": "QUEUE_UNREADABLE", "message": str(exc)}],
            "production_effect": "none",
        }
    if not isinstance(payload, Mapping):
        payload = {}
        issues.append({"code": "QUEUE_NOT_MAPPING", "message": "queue must be an object"})
    policy = load_daily_input_capture_policy(policy_path, project_root=project_root)
    if payload.get("schema_version") != DAILY_INPUT_RECOVERY_QUEUE_SCHEMA_VERSION:
        issues.append({"code": "QUEUE_SCHEMA_MISMATCH", "message": "unsupported queue schema"})
    if payload.get("policy_version") != policy.policy_version:
        issues.append({"code": "QUEUE_POLICY_VERSION_MISMATCH", "message": "policy drift"})
    if payload.get("policy_sha256") != sha256_path(policy_path):
        issues.append({"code": "QUEUE_POLICY_CHECKSUM_MISMATCH", "message": "policy drift"})
    for field, expected in (
        ("automatic_execution_allowed", False),
        ("historical_strict_pit_backfill_allowed", False),
        ("old_terminal_state_mutation_allowed", False),
        ("consumer_cutover_allowed", False),
        ("production_effect", "none"),
        ("broker_action", False),
        ("trading_action", False),
    ):
        if payload.get(field) != expected:
            issues.append(
                {
                    "code": "QUEUE_SAFETY_BOUNDARY_MISMATCH",
                    "message": f"{field} must be {expected!r}",
                }
            )
    items = payload.get("items")
    if not isinstance(items, list):
        items = []
        issues.append({"code": "QUEUE_ITEMS_INVALID", "message": "items must be a list"})
    observed_ids: set[str] = set()
    for item in items:
        if not isinstance(item, Mapping):
            issues.append({"code": "QUEUE_ITEM_INVALID", "message": "item must be an object"})
            continue
        recovery_id = str(item.get("recovery_id", ""))
        if not recovery_id or recovery_id in observed_ids:
            issues.append(
                {
                    "code": "QUEUE_RECOVERY_ID_INVALID",
                    "message": recovery_id or "<empty>",
                }
            )
        observed_ids.add(recovery_id)
        component_id = str(item.get("component_id", ""))
        component_policy = policy.component_policies.get(component_id)
        if component_policy is None:
            issues.append({"code": "QUEUE_COMPONENT_UNKNOWN", "message": component_id})
            continue
        if item.get("recovery_mode") != component_policy.recovery_mode:
            issues.append({"code": "QUEUE_RECOVERY_MODE_MISMATCH", "message": component_id})
        expected_allowed = component_policy.recovery_mode == "IMMUTABLE_RAW_BACKFILL"
        if item.get("recovery_allowed") is not expected_allowed:
            issues.append({"code": "QUEUE_RECOVERY_ALLOWED_MISMATCH", "message": component_id})
        if (
            item.get("automatic_execution_allowed") is not False
            or item.get("strict_pit_eligible") is not False
            or item.get("consumer_cutover_allowed") is not False
            or item.get("production_effect") != "none"
        ):
            issues.append({"code": "QUEUE_ITEM_SAFETY_MISMATCH", "message": component_id})
        try:
            session_date = date.fromisoformat(str(item.get("session_date")))
        except ValueError:
            issues.append({"code": "QUEUE_SESSION_DATE_INVALID", "message": component_id})
        else:
            if session_date < policy.tracking_start or not is_us_equity_trading_day(
                session_date
            ):
                issues.append({"code": "QUEUE_SESSION_NOT_TRACKED", "message": component_id})
    if payload.get("item_count") != len(items):
        issues.append({"code": "QUEUE_ITEM_COUNT_MISMATCH", "message": str(len(items))})
    expected_counts = {
        status: sum(
            1
            for item in items
            if isinstance(item, Mapping) and item.get("action_status") == status
        )
        for status in (
            "READY_FOR_MANUAL_RECOVERY",
            "OWNER_REVIEW_REQUIRED",
            "INSUFFICIENT_DATA",
        )
    }
    if payload.get("counts") != expected_counts:
        issues.append({"code": "QUEUE_COUNTS_MISMATCH", "message": "action counts drift"})
    return {
        "schema_version": "daily_input_capture_recovery_queue_validation.v1",
        "queue_path": _relative_path(queue_path, project_root),
        "as_of": payload.get("as_of"),
        "status": "PASS" if not issues else "FAIL",
        "issue_count": len(issues),
        "issues": issues,
        "automatic_execution_allowed": False,
        "historical_strict_pit_backfill_allowed": False,
        "consumer_cutover_allowed": False,
        "production_effect": "none",
        "broker_action": False,
        "trading_action": False,
    }


def _artifact_records(paths: Sequence[Path], project_root: Path) -> list[dict[str, object]]:
    files: dict[str, Path] = {}
    for path in paths:
        if path.is_file():
            files[_relative_path(path, project_root)] = path
        elif path.is_dir():
            for child in sorted(path.rglob("*")):
                if child.is_file():
                    files[_relative_path(child, project_root)] = child
    return [
        {
            "path": relative,
            "sha256": sha256_path(path),
            "size_bytes": path.stat().st_size,
        }
        for relative, path in sorted(files.items())
    ]


def _snapshot_component_sources(component: CaptureComponent) -> None:
    sources = component.snapshot_sources
    if component.component_id == "market_macro":
        publication = resolve_download_publication(
            output_dir=component.snapshot_sources[0].parent,
        )
        if publication.secondary_prices_path is None:
            raise OSError("market_macro: canonical secondary prices artifact missing")
        sources = (
            publication.prices_path,
            publication.secondary_prices_path,
            publication.rates_path,
            publication.manifest_path,
        )
    if len(sources) != len(component.expected_paths):
        raise OSError(f"{component.component_id}: snapshot source/destination count mismatch")
    for source, destination in zip(
        sources,
        component.expected_paths,
        strict=True,
    ):
        if not source.is_file():
            raise OSError(f"{component.component_id}: source artifact missing: {source}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)


def _validate_artifact_record(
    artifact: object,
    *,
    project_root: Path,
    allowed_roots: Sequence[Path],
    issues: list[dict[str, str]],
) -> None:
    if not isinstance(artifact, Mapping):
        issues.append({"code": "ARTIFACT_RECORD_INVALID", "message": "artifact must be object"})
        return
    relative_text = artifact.get("path")
    if not isinstance(relative_text, str) or not relative_text:
        issues.append({"code": "ARTIFACT_PATH_INVALID", "message": "artifact path missing"})
        return
    relative = Path(relative_text)
    if relative.is_absolute() or ".." in relative.parts:
        issues.append({"code": "ARTIFACT_PATH_ESCAPE", "message": relative_text})
        return
    path = project_root / relative
    resolved_path = path.resolve()
    if allowed_roots and not any(
        resolved_path.is_relative_to(root.resolve()) for root in allowed_roots
    ):
        issues.append({"code": "ARTIFACT_OUTSIDE_CAPTURE_SCOPE", "message": relative_text})
        return
    if not path.is_file():
        issues.append({"code": "ARTIFACT_MISSING", "message": relative_text})
        return
    if artifact.get("size_bytes") != path.stat().st_size:
        issues.append({"code": "ARTIFACT_SIZE_MISMATCH", "message": relative_text})
    if artifact.get("sha256") != sha256_path(path):
        issues.append({"code": "ARTIFACT_CHECKSUM_MISMATCH", "message": relative_text})


def _execution_command(command: Sequence[str]) -> tuple[str, ...]:
    if command and command[0] == "aits":
        return (sys.executable, "-m", "ai_trading_system.cli_direct", *command[1:])
    return tuple(command)


def _relative_path(path: Path, project_root: Path) -> str:
    try:
        return path.resolve().relative_to(project_root.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def _sanitize_error_summary(text: str, env: Mapping[str, str]) -> str:
    sanitized = text.replace("\r", " ").replace("\n", " ").strip()
    for name, value in env.items():
        if value and any(fragment in name.upper() for fragment in _REDACTED_ENV_FRAGMENTS):
            sanitized = sanitized.replace(value, "[REDACTED]")
    return sanitized[:_ERROR_SUMMARY_LIMIT]


def _required_text(mapping: Mapping[str, object], key: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"daily input capture policy missing {key}")
    return value


def _positive_int(value: object, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"daily input capture policy {field} must be a positive integer")
    return value


def _non_negative_int(value: object, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"daily input capture policy {field} must be a non-negative integer")
    return value


def _validate_safety(safety: Mapping[str, object]) -> None:
    expected = {
        "production_effect": "none",
        "production_weight_write": False,
        "active_shadow_weight_write": False,
        "broker_action": False,
        "trading_action": False,
    }
    for key, value in expected.items():
        if safety.get(key) != value:
            raise ValueError(f"daily input capture safety {key} must be {value!r}")


def _manifest_markdown(payload: Mapping[str, object]) -> str:
    lines = [
        f"# Daily Input Capture — {payload['as_of']}",
        "",
        f"- 状态：`{payload['status']}`",
        f"- policy：`{payload['policy_version']}`",
        f"- calendar authority：`{payload['calendar_authority']}`",
        f"- decision session：`{payload['decision_session_policy']}`",
        "- data quality：`NOT_EVALUATED`（capture 不替代严格 DQ）",
        "- production_effect：`none`",
        "",
        "| component | required | status | blocker | attempts | artifacts | missing |",
        "|---|---:|---|---|---:|---:|---:|",
    ]
    for item in payload["component_results"]:  # type: ignore[index]
        lines.append(
            "| {component_id} | {required} | {status} | {blocker_code} | "
            "{attempt_count}/{max_attempts} | {artifacts} | {missing} |".format(
                component_id=item["component_id"],
                required=str(item["required"]).lower(),
                status=item["status"],
                blocker_code=item["blocker_code"],
                attempt_count=item["attempt_count"],
                max_attempts=item["max_attempts"],
                artifacts=len(item["artifacts"]),
                missing=len(item["missing_expected_paths"]),
            )
        )
    lines.extend(
        [
            "",
            "即使状态为 `CAPTURED`，后续 `validate-data`、PIT、SEC、valuation、score 与",
            "Reader Brief gates 仍须独立通过；本产物不授权 weights、broker 或 trading action。",
            "",
        ]
    )
    return "\n".join(lines)


def _validation_markdown(payload: Mapping[str, object]) -> str:
    lines = [
        f"# Daily Input Capture Validation — {payload.get('as_of') or 'unknown'}",
        "",
        f"- 状态：`{payload['status']}`",
        f"- capture status：`{payload.get('capture_status')}`",
        f"- issue count：`{payload.get('issue_count', len(payload.get('issues', [])))}`",
        "- production_effect：`none`",
        "",
    ]
    for issue in payload.get("issues", []):
        lines.append(f"- `{issue['code']}`：{issue['message']}")
    if not payload.get("issues"):
        lines.append("- 未发现 manifest、checksum 或 safety boundary 问题。")
    lines.append("")
    return "\n".join(lines)


def _gap_ledger_markdown(payload: Mapping[str, object]) -> str:
    counts = payload["counts"]
    lines = [
        "# Daily Input Capture XNYS Session Gap Ledger",
        "",
        f"- 状态：`{payload['status']}`",
        f"- 跟踪窗口：`{payload['tracking_start']}` 至 `{payload['as_of']}`",
        f"- CAPTURED / PARTIAL / MISSED：`{counts['CAPTURED']}` / "
        f"`{counts['PARTIAL_CAPTURE']}` / `{counts['MISSED']}`",
        "- production_effect：`none`",
        "",
        "| session | status | capture | validation |",
        "|---|---|---|---|",
    ]
    for row in payload["sessions"]:
        lines.append(
            f"| {row['session_date']} | {row['status']} | "
            f"{row['capture_status'] or '-'} | {row['validation_status']} |"
        )
    lines.append("")
    return "\n".join(lines)


def _recovery_queue_markdown(payload: Mapping[str, object]) -> str:
    counts = payload["counts"]
    lines = [
        "# Daily Input Capture Recovery Queue",
        "",
        f"- 状态：`{payload['status']}`",
        f"- as_of：`{payload['as_of']}`",
        f"- item count：`{payload['item_count']}`",
        "- READY / OWNER_REVIEW / INSUFFICIENT_DATA："
        f"`{counts['READY_FOR_MANUAL_RECOVERY']}` / "
        f"`{counts['OWNER_REVIEW_REQUIRED']}` / `{counts['INSUFFICIENT_DATA']}`",
        "- automatic execution：`false`",
        "- historical strict PIT backfill：`false`",
        "- production_effect：`none`",
        "",
        "| session | component | action | recovery mode | blocker |",
        "|---|---|---|---|---|",
    ]
    for item in payload["items"]:
        lines.append(
            f"| {item['session_date']} | {item['component_id']} | "
            f"{item['action_status']} | {item['recovery_mode']} | "
            f"{item['source_blocker_code'] or '-'} |"
        )
    lines.extend(
        [
            "",
            "队列不自动请求 provider、不改写旧 manifest/run terminal，且任何恢复 bytes",
            "都不获得 strict PIT、DQ、score 或 consumer cutover 资格。",
            "",
        ]
    )
    return "\n".join(lines)


def _recovery_queue_validation_markdown(payload: Mapping[str, object]) -> str:
    lines = [
        "# Daily Input Capture Recovery Queue Validation",
        "",
        f"- 状态：`{payload['status']}`",
        f"- as_of：`{payload.get('as_of') or 'unknown'}`",
        f"- issue count：`{payload.get('issue_count', len(payload.get('issues', [])))}`",
        "- automatic execution：`false`",
        "- historical strict PIT backfill：`false`",
        "- production_effect：`none`",
        "",
    ]
    for issue in payload.get("issues", []):
        lines.append(f"- `{issue['code']}`：{issue['message']}")
    if not payload.get("issues"):
        lines.append("- queue lineage、recovery mode、counts 与 safety boundary 均通过。")
    lines.append("")
    return "\n".join(lines)
