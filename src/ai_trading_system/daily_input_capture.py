from __future__ import annotations

import os
import shutil
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data.download_publication import resolve_download_publication
from ai_trading_system.platform.artifacts import (
    load_strict_json_path,
    sha256_path,
    write_json_atomic,
    write_markdown_atomic,
)
from ai_trading_system.trading_calendar import is_us_equity_trading_day

DAILY_INPUT_CAPTURE_SCHEMA_VERSION = "daily_input_capture_manifest.v1"
DAILY_INPUT_GAP_LEDGER_SCHEMA_VERSION = "daily_input_capture_gap_ledger.v1"
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
_COMPONENT_MAX_ATTEMPTS = {
    "market_macro": 2,
    "fmp_forward_pit": 1,
    "sec_companyfacts": 1,
    "fmp_valuation": 1,
    "official_policy_sources": 1,
}


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
    max_attempts: int = 1
    snapshot_sources: tuple[Path, ...] = ()


@dataclass(frozen=True)
class DailyInputCaptureResult:
    status: str
    manifest_path: Path
    validation_path: Path
    gap_ledger_path: Path
    component_results: tuple[Mapping[str, object], ...]

    @property
    def passed(self) -> bool:
        return self.status == "CAPTURED"


CaptureRunner = Callable[..., subprocess.CompletedProcess[str]]
CaptureSnapshotter = Callable[[CaptureComponent], None]


def load_daily_input_capture_policy(
    path: Path = DEFAULT_DAILY_INPUT_CAPTURE_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> DailyInputCapturePolicy:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise ValueError("daily input capture policy must be a mapping")
    if raw.get("schema_version") != "daily_input_capture_policy.v1":
        raise ValueError("unsupported daily input capture policy schema_version")
    if raw.get("status") != "REVIEWED":
        raise ValueError("daily input capture policy must be REVIEWED")
    required_components = raw.get("required_components")
    paths = raw.get("paths")
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
    if not isinstance(paths, Mapping) or not isinstance(safety, Mapping):
        raise ValueError("daily input capture policy paths/safety must be mappings")
    _validate_safety(safety)

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
            max_attempts=2,
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
            required="fmp_forward_pit" in required,
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
    component_results: list[dict[str, object]] = []
    for component in components:
        started_at = datetime.now(tz=UTC)
        attempt_count = 0
        return_code = 1
        stdout_text = ""
        stderr_text = ""
        exception_summary: str | None = None
        for attempt in range(1, component.max_attempts + 1):
            attempt_count = attempt
            try:
                completed = runner(
                    _execution_command(component.command),
                    cwd=project_root,
                    env=checked_env,
                    text=True,
                    capture_output=True,
                    check=False,
                )
                return_code = int(completed.returncode)
                stdout_text = completed.stdout or ""
                stderr_text = completed.stderr or ""
                exception_summary = None
            except Exception as exc:
                return_code = 1
                stdout_text = ""
                stderr_text = ""
                exception_summary = f"{type(exc).__name__}: {exc}"
            if return_code == 0:
                break
        if return_code == 0 and component.snapshot_sources:
            try:
                component_snapshotter(component)
            except Exception as exc:
                return_code = 1
                exception_summary = f"{type(exc).__name__}: {exc}"
        ended_at = datetime.now(tz=UTC)
        missing_expected = tuple(
            _relative_path(path, project_root)
            for path in component.expected_paths
            if not path.exists()
        )
        artifact_records = _artifact_records(component.expected_paths, project_root)
        passed = return_code == 0 and not missing_expected
        error_summary = _sanitize_error_summary(
            exception_summary or stderr_text or (stdout_text if return_code else ""),
            checked_env,
        )
        component_results.append(
            {
                "component_id": component.component_id,
                "required": component.required,
                "status": "PASS" if passed else "FAIL",
                "return_code": return_code,
                "attempt_count": attempt_count,
                "max_attempts": component.max_attempts,
                "started_at": started_at.isoformat(),
                "ended_at": ended_at.isoformat(),
                "duration_seconds": round((ended_at - started_at).total_seconds(), 6),
                "command": list(component.command),
                "stdout_line_count": len(stdout_text.splitlines()),
                "stderr_line_count": len(stderr_text.splitlines()),
                "error_summary": error_summary or None,
                "missing_expected_paths": list(missing_expected),
                "artifacts": artifact_records,
            }
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
    return DailyInputCaptureResult(
        status=status if validation["status"] == "PASS" else "PARTIAL_CAPTURE",
        manifest_path=paths.manifest_json,
        validation_path=paths.validation_json,
        gap_ledger_path=paths.gap_ledger_json,
        component_results=tuple(component_results),
    )


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
        expected_max_attempts = _COMPONENT_MAX_ATTEMPTS.get(component_id)
        attempt_count = component.get("attempt_count")
        max_attempts = component.get("max_attempts")
        if (
            expected_max_attempts is None
            or max_attempts != expected_max_attempts
            or not isinstance(attempt_count, int)
            or isinstance(attempt_count, bool)
            or attempt_count < 1
            or attempt_count > expected_max_attempts
        ):
            issues.append(
                {
                    "code": "COMPONENT_ATTEMPT_BUDGET_MISMATCH",
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
        "| component | required | status | return code | artifacts | missing |",
        "|---|---:|---|---:|---:|---:|",
    ]
    for item in payload["component_results"]:  # type: ignore[index]
        lines.append(
            "| {component_id} | {required} | {status} | {return_code} | {artifacts} | "
            "{missing} |".format(
                component_id=item["component_id"],
                required=str(item["required"]).lower(),
                status=item["status"],
                return_code=item["return_code"],
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
