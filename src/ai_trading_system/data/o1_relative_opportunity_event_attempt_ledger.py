from __future__ import annotations

import hashlib
import html
import json
import platform
import re
import subprocess
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Final, NoReturn
from urllib.parse import urljoin, urlparse
from zoneinfo import ZoneInfo

import yaml

from ai_trading_system.data.o1_relative_opportunity_dq_candidate import (
    validate_o1_dq_gate,
)
from ai_trading_system.platform.artifacts import (
    canonical_json_bytes,
    sha256_path,
    write_bytes_atomic,
    write_json_atomic,
)

AUDIT_POLICY_SCHEMA_VERSION: Final = "o1_relative_opportunity_capability_audit_policy.v1"
AUDIT_POLICY_ID: Final = "TRADING_2464_O1_CAPABILITY_AUDIT_V1"
ATTEMPT_LEDGER_SCHEMA_VERSION: Final = "o1_relative_opportunity_attempt_ledger.v1"
EVENT_LEDGER_SCHEMA_VERSION: Final = "o1_relative_opportunity_event_ledger.v1"
SOURCE_MANIFEST_SCHEMA_VERSION: Final = "o1_relative_opportunity_event_source_manifest.v1"
FREEZE_GATE_SCHEMA_VERSION: Final = "o1_relative_opportunity_event_attempt_freeze_gate.v1"
TASK_ID: Final = "TRADING-2464"
DEFAULT_AUDIT_POLICY_PATH: Final = Path(
    "config/research/o1_relative_opportunity_capability_audit_v1.yaml"
)
REQUIRED_EVENT_FAMILIES: Final = ("FOMC", "CPI", "NFP")
ALLOWED_SOURCE_DOMAINS: Final = ("federalreserve.gov", "bls.gov")
PASS_STATUS: Final = "PASS_EVENT_AND_ATTEMPT_LEDGERS_FROZEN"
BLOCKED_STATUS: Final = "BLOCKED_PRIMARY_EVENT_SOURCE_ACQUISITION"
BLOCKED_CLASS: Final = "INSUFFICIENT_COVERAGE_OR_DQ"
_BLS_INDEX_URLS: Final = {
    "CPI": "https://www.bls.gov/bls/news-release/cpi.htm",
    "NFP": "https://www.bls.gov/bls/news-release/empsit.htm",
}
_FED_INDEX_TEMPLATE: Final = (
    "https://www.federalreserve.gov/newsevents/pressreleases/{year}-press-fomc.htm"
)
_BLS_RELEASE_PATTERN: Final = {
    "CPI": re.compile(r"/news\.release/archives/cpi_(\d{8})\.htm$", re.IGNORECASE),
    "NFP": re.compile(r"/news\.release/archives/empsit_(\d{8})\.htm$", re.IGNORECASE),
}
_FED_RELEASE_PATTERN: Final = re.compile(
    r"/newsevents/pressreleases/monetary(\d{8})a\.htm$",
    re.IGNORECASE,
)
_SHA_PATTERN: Final = re.compile(r"[0-9a-f]{64}")
_EASTERN: Final = ZoneInfo("America/New_York")
_BLS_TIMESTAMP_PATTERN: Final = re.compile(
    r"(?:embargoed|For release)\s+until\s+"
    # Employment Situation pages place the official USDL release identifier
    # between "until" and the embargo time; the identifier is bounded rather
    # than accepting arbitrary intervening text.
    r"(?:(?P<release_id>USDL[-\s]?\d{2,4}[-–]\d{3,5})\s+)?"
    r"(?P<hour>\d{1,2}):(?P<minute>\d{2})\s*"
    r"(?P<period>a\.?m\.?|p\.?m\.?)\s*"
    r"(?:\((?P<zone_parenthesized>ET|EDT|EST)\)|"
    r"(?P<zone_plain>ET|EDT|EST))\s*"
    r"(?:(?P<weekday>Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+)?"
    r"(?P<month>January|February|March|April|May|June|July|August|"
    r"September|October|November|December)\s+"
    r"(?P<day>\d{1,2}),\s+(?P<year>\d{4})",
    re.IGNORECASE,
)
_MONTH_NUMBERS: Final = {
    month.lower(): number
    for number, month in enumerate(
        (
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        ),
        start=1,
    )
}
_WEEKDAYS: Final = (
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
)
_REPLAY_SOURCE_MANIFEST_NAME: Final = "event_source_manifest_replay_v1.json"
_REPLAY_GATE_NAME: Final = "event_attempt_freeze_gate_replay_v1.json"
_REPLAY_MODE: Final = "CHECKSUM_VERIFIED_OFFLINE_REPLAY_NO_NETWORK"


class O1EventAttemptFreezeError(RuntimeError):
    def __init__(self, code: str, message: str, *, path: Path | None = None) -> None:
        self.code = code
        self.message = message
        self.path = path
        location = "" if path is None else f" [{path}]"
        super().__init__(f"{code}{location}: {message}")


@dataclass(frozen=True)
class SourceFetch:
    requested_url: str
    final_url: str
    downloaded_at: datetime
    status_code: int | None
    content_type: str | None
    body: bytes
    error: str | None = None


@dataclass(frozen=True)
class O1EventAttemptFreezeResult:
    status: str
    output_root: Path
    source_manifest_path: Path
    attempt_ledger_path: Path
    event_ledger_path: Path | None
    gate_path: Path
    gate: Mapping[str, object]


@dataclass(frozen=True)
class _SourceRecord:
    record: Mapping[str, object]
    body: bytes


class _AnchorParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._href: str | None = None
        self._text: list[str] = []
        self.links: list[tuple[str, str]] = []

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        if tag.lower() != "a":
            return
        self._href = next((value for name, value in attrs if name == "href"), None)
        self._text = []

    def handle_data(self, data: str) -> None:
        if self._href is not None:
            self._text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "a" and self._href is not None:
            self.links.append((self._href, " ".join(self._text).strip()))
            self._href = None
            self._text = []


def freeze_o1_event_and_attempt_ledgers(
    *,
    output_root: Path,
    project_root: Path,
    generated_at: datetime,
    audit_policy_path: Path = DEFAULT_AUDIT_POLICY_PATH,
    fetcher: Callable[[str], SourceFetch] | None = None,
    source_commit_sha: str | None = None,
    cli_argv: Sequence[str] = (),
) -> O1EventAttemptFreezeResult:
    """Freeze exact primary-source event lineage and one pre-result attempt family."""

    timestamp = _aware_utc(generated_at)
    root = project_root.resolve(strict=True)
    policy_path = _contained_file(root, audit_policy_path, "O1_EVENT_POLICY_MISSING")
    policy = _load_yaml_mapping(policy_path, "O1_EVENT_POLICY_INVALID")
    _validate_policy(policy)
    policy_sha256 = sha256_path(policy_path)
    software_identity = _software_identity(
        project_root=root,
        source_commit_sha=source_commit_sha,
        cli_argv=cli_argv,
    )
    dq_binding = _verify_dq_gate(policy=policy, project_root=root)
    resolved_output = _new_output_root(
        project_root=root,
        output_root=output_root,
        policy=policy,
    )

    resolved_output.mkdir(parents=False, exist_ok=False)
    raw_root = resolved_output / "raw_primary_sources"
    raw_root.mkdir()
    downloader = fetcher or _fetch_official_source
    request_records: list[Mapping[str, object]] = []
    source_records: list[_SourceRecord] = []
    blockers: list[Mapping[str, object]] = []
    discovered: dict[str, list[str]] = {family: [] for family in REQUIRED_EVENT_FAMILIES}

    index_requests = [
        ("CPI", "INDEX", _BLS_INDEX_URLS["CPI"]),
        ("NFP", "INDEX", _BLS_INDEX_URLS["NFP"]),
        *[
            ("FOMC", "INDEX", _FED_INDEX_TEMPLATE.format(year=year))
            for year in _research_years(policy)
        ],
    ]
    for family, source_role, url in index_requests:
        record = _capture_source(
            family=family,
            source_role=source_role,
            url=url,
            raw_root=raw_root,
            fetcher=downloader,
        )
        request_records.append(record.record)
        source_records.append(record)
        if record.record["status"] != "PASS":
            blockers.append(_source_blocker(record.record))
            continue
        try:
            release_urls = _discover_release_urls(
                family=family,
                index_url=url,
                body=record.body,
                policy=policy,
            )
        except O1EventAttemptFreezeError as exc:
            blockers.append(
                {
                    "code": exc.code,
                    "event_family": family,
                    "endpoint": url,
                    "message": exc.message,
                }
            )
            continue
        discovered[family].extend(release_urls)

    for family in REQUIRED_EVENT_FAMILIES:
        discovered[family] = sorted(set(discovered[family]))
        if not discovered[family]:
            blockers.append(
                {
                    "code": "O1_EVENT_RELEASE_DISCOVERY_EMPTY",
                    "event_family": family,
                    "message": "no official release URLs discovered in the research window",
                }
            )

    event_rows: list[Mapping[str, object]] = []
    if not blockers:
        for family in REQUIRED_EVENT_FAMILIES:
            for url in discovered[family]:
                record = _capture_source(
                    family=family,
                    source_role="RELEASE",
                    url=url,
                    raw_root=raw_root,
                    fetcher=downloader,
                )
                request_records.append(record.record)
                source_records.append(record)
                if record.record["status"] != "PASS":
                    blockers.append(_source_blocker(record.record))
                    continue
                try:
                    event_rows.append(
                        _event_row(
                            family=family,
                            source_record=record.record,
                            body=record.body,
                        )
                    )
                except O1EventAttemptFreezeError as exc:
                    blockers.append(
                        {
                            "code": exc.code,
                            "event_family": family,
                            "endpoint": url,
                            "message": exc.message,
                        }
                    )

    event_rows.sort(key=lambda row: (str(row["event_timestamp"]), str(row["event_family"])))
    _validate_event_rows(event_rows, blockers=blockers)
    status = BLOCKED_STATUS if blockers else PASS_STATUS
    attempt_ledger = _build_attempt_ledger(
        policy=policy,
        policy_path=policy_path,
        policy_sha256=policy_sha256,
        dq_binding=dq_binding,
        generated_at=timestamp,
        source_status=status,
        software_identity=software_identity,
    )
    attempt_path = resolved_output / "attempt_ledger.json"
    write_json_atomic(attempt_path, attempt_ledger)

    source_manifest = _build_source_manifest(
        policy=policy,
        generated_at=timestamp,
        status=status,
        requests=request_records,
        discovered=discovered,
        blockers=blockers,
        software_identity=software_identity,
    )
    source_manifest_path = resolved_output / "event_source_manifest.json"
    write_json_atomic(source_manifest_path, source_manifest)

    event_ledger: Mapping[str, object] | None = None
    event_ledger_path: Path | None = None
    if status == PASS_STATUS:
        event_ledger = _build_event_ledger(
            policy=policy,
            generated_at=timestamp,
            source_manifest=source_manifest,
            events=event_rows,
        )
        event_ledger_path = resolved_output / "event_ledger.json"
        write_json_atomic(event_ledger_path, event_ledger)

    gate = _build_gate(
        policy=policy,
        policy_path=policy_path,
        policy_sha256=policy_sha256,
        dq_binding=dq_binding,
        generated_at=timestamp,
        status=status,
        source_manifest_path=source_manifest_path,
        source_manifest=source_manifest,
        attempt_ledger_path=attempt_path,
        attempt_ledger=attempt_ledger,
        event_ledger_path=event_ledger_path,
        event_ledger=event_ledger,
        blockers=blockers,
        software_identity=software_identity,
    )
    validate_o1_event_attempt_freeze_gate(gate)
    gate_path = resolved_output / "event_attempt_freeze_gate.json"
    write_json_atomic(gate_path, gate)
    return O1EventAttemptFreezeResult(
        status=status,
        output_root=resolved_output,
        source_manifest_path=source_manifest_path,
        attempt_ledger_path=attempt_path,
        event_ledger_path=event_ledger_path,
        gate_path=gate_path,
        gate=gate,
    )


def replay_o1_event_and_attempt_ledgers_from_retained_sources(
    *,
    output_root: Path,
    project_root: Path,
    generated_at: datetime,
    initial_source_manifest_sha256: str,
    initial_attempt_ledger_sha256: str,
    initial_gate_sha256: str,
    audit_policy_path: Path = DEFAULT_AUDIT_POLICY_PATH,
    source_commit_sha: str | None = None,
    cli_argv: Sequence[str] = (),
) -> O1EventAttemptFreezeResult:
    """Reparse one immutable blocked acquisition without network access or overwrite."""

    timestamp = _aware_utc(generated_at)
    root = project_root.resolve(strict=True)
    policy_path = _contained_file(root, audit_policy_path, "O1_EVENT_POLICY_MISSING")
    policy = _load_yaml_mapping(policy_path, "O1_EVENT_POLICY_INVALID")
    _validate_policy(policy)
    policy_sha256 = sha256_path(policy_path)
    software_identity = _software_identity(
        project_root=root,
        source_commit_sha=source_commit_sha,
        cli_argv=cli_argv,
    )
    dq_binding = _verify_dq_gate(policy=policy, project_root=root)
    resolved_output = _existing_output_root(
        project_root=root,
        output_root=output_root,
        policy=policy,
    )

    initial_source_path = _contained_file(
        resolved_output,
        Path("event_source_manifest.json"),
        "O1_EVENT_REPLAY_INITIAL_SOURCE_MANIFEST_MISSING",
    )
    initial_attempt_path = _contained_file(
        resolved_output,
        Path("attempt_ledger.json"),
        "O1_EVENT_REPLAY_INITIAL_ATTEMPT_LEDGER_MISSING",
    )
    initial_gate_path = _contained_file(
        resolved_output,
        Path("event_attempt_freeze_gate.json"),
        "O1_EVENT_REPLAY_INITIAL_GATE_MISSING",
    )
    initial_source = _load_verified_json(
        initial_source_path,
        expected_sha256=initial_source_manifest_sha256,
        code="O1_EVENT_REPLAY_INITIAL_SOURCE_MANIFEST_TAMPERED",
    )
    initial_attempt = _load_verified_json(
        initial_attempt_path,
        expected_sha256=initial_attempt_ledger_sha256,
        code="O1_EVENT_REPLAY_INITIAL_ATTEMPT_LEDGER_TAMPERED",
    )
    initial_gate = _load_verified_json(
        initial_gate_path,
        expected_sha256=initial_gate_sha256,
        code="O1_EVENT_REPLAY_INITIAL_GATE_TAMPERED",
    )
    _validate_replay_authority(
        initial_source_path=initial_source_path,
        initial_source=initial_source,
        initial_attempt_path=initial_attempt_path,
        initial_attempt=initial_attempt,
        initial_gate=initial_gate,
        policy_sha256=policy_sha256,
        dq_binding=dq_binding,
    )

    replay_source_path = resolved_output / _REPLAY_SOURCE_MANIFEST_NAME
    event_ledger_path = resolved_output / "event_ledger.json"
    replay_gate_path = resolved_output / _REPLAY_GATE_NAME
    for candidate in (replay_source_path, event_ledger_path, replay_gate_path):
        if candidate.exists():
            _fail(
                "O1_EVENT_REPLAY_OUTPUT_EXISTS",
                "refusing to overwrite a replay artifact",
                path=candidate,
            )

    request_records, source_bodies, retained_artifact_paths = _verify_retained_sources(
        output_root=resolved_output,
        source_manifest=initial_source,
    )
    discovered = _rediscover_retained_release_urls(
        policy=policy,
        request_records=request_records,
        source_bodies=source_bodies,
    )
    event_rows = [
        _event_row(
            family=_text(record["event_family"], "event family"),
            source_record=record,
            body=source_bodies[index],
        )
        for index, record in enumerate(request_records)
        if record["source_role"] == "RELEASE"
    ]
    event_rows.sort(key=lambda row: (str(row["event_timestamp"]), str(row["event_family"])))
    _validate_event_rows(event_rows, blockers=[])

    replay_provenance = {
        "mode": _REPLAY_MODE,
        "network_accessed": False,
        "raw_source_refetched": False,
        "parser_correction": "BLS_USDL_IDENTIFIER_BETWEEN_EMBARGO_LABEL_AND_TIME_V1",
        "initial_failure_artifacts": {
            "event_source_manifest": _artifact_binding(
                initial_source_path,
                initial_source,
            ),
            "attempt_ledger": _artifact_binding(
                initial_attempt_path,
                initial_attempt,
            ),
            "event_attempt_freeze_gate": _artifact_binding(
                initial_gate_path,
                initial_gate,
            ),
        },
        "initial_blocker_codes": sorted(
            {
                _text(item["code"], "initial blocker code")
                for item in _sequence(initial_source["blockers"], "initial blockers")
                if isinstance(item, Mapping)
            }
        ),
        "retained_request_count": len(request_records),
        "retained_unique_artifact_count": len(retained_artifact_paths),
        "retained_unique_artifact_bytes": sum(
            path.stat().st_size for path in retained_artifact_paths
        ),
    }
    source_manifest = _build_source_manifest(
        policy=policy,
        generated_at=timestamp,
        status=PASS_STATUS,
        requests=request_records,
        discovered=discovered,
        blockers=[],
        software_identity=software_identity,
        replay_provenance=replay_provenance,
    )
    event_ledger = _build_event_ledger(
        policy=policy,
        generated_at=timestamp,
        source_manifest=source_manifest,
        events=event_rows,
    )

    write_json_atomic(replay_source_path, source_manifest)
    write_json_atomic(event_ledger_path, event_ledger)
    gate = _build_gate(
        policy=policy,
        policy_path=policy_path,
        policy_sha256=policy_sha256,
        dq_binding=dq_binding,
        generated_at=timestamp,
        status=PASS_STATUS,
        source_manifest_path=replay_source_path,
        source_manifest=source_manifest,
        attempt_ledger_path=initial_attempt_path,
        attempt_ledger=initial_attempt,
        event_ledger_path=event_ledger_path,
        event_ledger=event_ledger,
        blockers=[],
        software_identity=software_identity,
        replay_provenance=replay_provenance,
    )
    validate_o1_event_attempt_freeze_gate(gate)
    write_json_atomic(replay_gate_path, gate)
    return O1EventAttemptFreezeResult(
        status=PASS_STATUS,
        output_root=resolved_output,
        source_manifest_path=replay_source_path,
        attempt_ledger_path=initial_attempt_path,
        event_ledger_path=event_ledger_path,
        gate_path=replay_gate_path,
        gate=gate,
    )


def validate_o1_event_attempt_freeze_gate(gate: Mapping[str, object]) -> None:
    if gate.get("schema_version") != FREEZE_GATE_SCHEMA_VERSION:
        _fail("O1_EVENT_GATE_SCHEMA_INVALID", str(gate.get("schema_version")))
    status = gate.get("status")
    if status not in {PASS_STATUS, BLOCKED_STATUS}:
        _fail("O1_EVENT_GATE_STATUS_INVALID", str(status))
    gate_id = gate.get("gate_id")
    if not isinstance(gate_id, str):
        _fail("O1_EVENT_GATE_ID_INVALID", str(gate_id))
    body = {key: value for key, value in gate.items() if key != "gate_id"}
    if gate_id != f"o1_event_attempt_gate_{_digest(body)[:32]}":
        _fail("O1_EVENT_GATE_ID_MISMATCH", gate_id)
    authorization = _mapping(gate.get("next_authorization"), "next_authorization")
    coverage_allowed = authorization.get("coverage_only_gate_allowed")
    training_allowed = authorization.get("model_training_allowed")
    if (
        training_allowed is not False
        or authorization.get("canonical_run_allowed") is not False
        or authorization.get("production_allowed") is not False
    ):
        _fail(
            "O1_EVENT_GATE_SCOPE_INVALID",
            "model training, canonical run, and production must remain false",
        )
    artifacts = _mapping(gate.get("artifacts"), "artifacts")
    if status == PASS_STATUS:
        if coverage_allowed is not True or not isinstance(artifacts.get("event_ledger"), Mapping):
            _fail("O1_EVENT_GATE_SCOPE_INVALID", "PASS gate requires event ledger and coverage")
    elif (
        coverage_allowed is not False
        or artifacts.get("event_ledger") is not None
        or gate.get("mechanical_classification") != BLOCKED_CLASS
    ):
        _fail("O1_EVENT_GATE_SCOPE_INVALID", "blocked gate cannot authorize coverage")


def _fetch_official_source(url: str) -> SourceFetch:
    _require_official_url(url)
    downloaded_at = datetime.now(UTC)
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "AITradingSystem-TRADING-2464/1.0 "
                "(research audit; official-source acquisition)"
            ),
            "Accept": "text/html,application/xhtml+xml,application/pdf;q=0.9,*/*;q=0.1",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            final_url = response.geturl()
            _require_official_url(final_url)
            body = response.read()
            return SourceFetch(
                requested_url=url,
                final_url=final_url,
                downloaded_at=downloaded_at,
                status_code=int(response.status),
                content_type=response.headers.get_content_type(),
                body=body,
            )
    except urllib.error.HTTPError as exc:
        final_url = exc.geturl()
        _require_official_url(final_url)
        return SourceFetch(
            requested_url=url,
            final_url=final_url,
            downloaded_at=downloaded_at,
            status_code=int(exc.code),
            content_type=exc.headers.get_content_type() if exc.headers else None,
            body=exc.read(),
            error=f"HTTP_{exc.code}",
        )
    except urllib.error.URLError as exc:
        return SourceFetch(
            requested_url=url,
            final_url=url,
            downloaded_at=downloaded_at,
            status_code=None,
            content_type=None,
            body=b"",
            error=f"URL_ERROR:{type(exc.reason).__name__}",
        )


def _capture_source(
    *,
    family: str,
    source_role: str,
    url: str,
    raw_root: Path,
    fetcher: Callable[[str], SourceFetch],
) -> _SourceRecord:
    _require_official_url(url)
    fetched = fetcher(url)
    if fetched.requested_url != url:
        _fail(
            "O1_EVENT_FETCH_REQUEST_MISMATCH",
            f"expected={url} actual={fetched.requested_url}",
        )
    _require_official_url(fetched.final_url)
    downloaded_at = _aware_utc(fetched.downloaded_at)
    checksum = hashlib.sha256(fetched.body).hexdigest()
    artifact_path: str | None = None
    if fetched.body:
        suffix = _safe_suffix(fetched.final_url, fetched.content_type)
        family_dir = raw_root / family.lower()
        family_dir.mkdir(exist_ok=True)
        target = family_dir / f"{source_role.lower()}_{checksum[:16]}{suffix}"
        write_bytes_atomic(target, fetched.body)
        if sha256_path(target) != checksum:
            _fail("O1_EVENT_SOURCE_WRITE_TAMPER", url, path=target)
        artifact_path = target.relative_to(raw_root.parent).as_posix()
    status = (
        "PASS"
        if fetched.status_code == 200 and fetched.body and fetched.error is None
        else "BLOCKED"
    )
    record = {
        "event_family": family,
        "source_role": source_role,
        "provider_name": _provider_name(family),
        "provider_class": "primary_source",
        "endpoint": url,
        "final_url": fetched.final_url,
        "request_parameters": {},
        "download_timestamp": downloaded_at.isoformat(),
        "http_status": fetched.status_code,
        "content_type": fetched.content_type,
        "byte_size": len(fetched.body),
        "checksum": checksum,
        "artifact_path": artifact_path,
        "status": status,
        "error": fetched.error,
    }
    return _SourceRecord(record=record, body=fetched.body)


def _discover_release_urls(
    *,
    family: str,
    index_url: str,
    body: bytes,
    policy: Mapping[str, object],
) -> list[str]:
    text = _decode_html(body)
    parser = _AnchorParser()
    parser.feed(text)
    start, end = _research_window(policy)
    releases: list[str] = []
    pattern = _FED_RELEASE_PATTERN if family == "FOMC" else _BLS_RELEASE_PATTERN[family]
    for href, anchor_text in parser.links:
        absolute = urljoin(index_url, href)
        match = pattern.search(urlparse(absolute).path)
        if match is None:
            continue
        if family == "FOMC" and "FOMC statement" not in anchor_text:
            continue
        release_date = datetime.strptime(
            match.group(1),
            "%Y%m%d" if family == "FOMC" else "%m%d%Y",
        ).date()
        if start <= release_date <= end:
            _require_official_url(absolute)
            releases.append(absolute)
    return sorted(set(releases))


def _event_row(
    *,
    family: str,
    source_record: Mapping[str, object],
    body: bytes,
) -> Mapping[str, object]:
    url = _text(source_record.get("endpoint"), "source endpoint")
    timestamp = (
        _parse_fomc_release_timestamp(url, body)
        if family == "FOMC"
        else _parse_bls_release_timestamp(url, body)
    )
    iso_timestamp = timestamp.astimezone(UTC).isoformat()
    checksum = _sha(source_record.get("checksum"), "source checksum")
    return {
        "event_id": f"{family}_{iso_timestamp}",
        "event_family": family,
        "event_timestamp": iso_timestamp,
        "source_published_time": iso_timestamp,
        "known_at": iso_timestamp,
        "available_at": iso_timestamp,
        "provider_name": _provider_name(family),
        "endpoint_or_file": url,
        "request_parameters": {},
        "download_timestamp": source_record["download_timestamp"],
        "checksum": checksum,
        "source_artifact_path": source_record["artifact_path"],
        "known_at_semantics": "ACTUAL_RELEASE_OCCURRENCE_NOT_ADVANCE_SCHEDULE_KNOWLEDGE",
    }


def _parse_bls_release_timestamp(url: str, body: bytes) -> datetime:
    release_date_from_url = _date_from_release_url(url, _BLS_RELEASE_PATTERN)
    text = _visible_text(body)
    match = _BLS_TIMESTAMP_PATTERN.search(text)
    if match is None:
        _fail("O1_EVENT_BLS_TIMESTAMP_MISSING", url)
    release_date_from_body = date(
        int(match.group("year")),
        _MONTH_NUMBERS[match.group("month").lower()],
        int(match.group("day")),
    )
    if release_date_from_body != release_date_from_url:
        _fail(
            "O1_EVENT_BLS_RELEASE_DATE_MISMATCH",
            (
                f"url={release_date_from_url.isoformat()} "
                f"body={release_date_from_body.isoformat()} endpoint={url}"
            ),
        )
    weekday = match.group("weekday")
    if weekday is not None and weekday.lower() != _WEEKDAYS[release_date_from_body.weekday()]:
        _fail(
            "O1_EVENT_BLS_RELEASE_WEEKDAY_MISMATCH",
            f"date={release_date_from_body.isoformat()} weekday={weekday} endpoint={url}",
        )
    return _local_release_datetime(release_date_from_body, match)


def _parse_fomc_release_timestamp(url: str, body: bytes) -> datetime:
    match_date = _FED_RELEASE_PATTERN.search(urlparse(url).path)
    if match_date is None:
        _fail("O1_EVENT_FOMC_URL_INVALID", url)
    release_date = datetime.strptime(match_date.group(1), "%Y%m%d").date()
    text = _visible_text(body)
    match = re.search(
        r"For release at\s+(?P<hour>\d{1,2}):(?P<minute>\d{2})\s*"
        r"(?P<period>a\.?m\.?|p\.?m\.?)\s*(?:EDT|EST|ET)",
        text,
        re.IGNORECASE,
    )
    if match is None:
        _fail("O1_EVENT_FOMC_TIMESTAMP_MISSING", url)
    return _local_release_datetime(release_date, match)


def _local_release_datetime(release_date: date, match: re.Match[str]) -> datetime:
    hour = int(match.group("hour"))
    minute = int(match.group("minute"))
    period = match.group("period").lower().replace(".", "")
    if hour < 1 or hour > 12 or minute > 59:
        _fail("O1_EVENT_RELEASE_TIME_INVALID", match.group(0))
    if period == "pm" and hour != 12:
        hour += 12
    if period == "am" and hour == 12:
        hour = 0
    return datetime(
        release_date.year,
        release_date.month,
        release_date.day,
        hour,
        minute,
        tzinfo=_EASTERN,
    )


def _build_attempt_ledger(
    *,
    policy: Mapping[str, object],
    policy_path: Path,
    policy_sha256: str,
    dq_binding: Mapping[str, object],
    generated_at: datetime,
    source_status: str,
    software_identity: Mapping[str, object],
) -> Mapping[str, object]:
    attempt_contract = _mapping(policy["attempt_ledger_contract"], "attempt_ledger_contract")
    family_id = _text(attempt_contract["current_attempt_family_id"], "attempt family")
    body = {
        "schema_version": ATTEMPT_LEDGER_SCHEMA_VERSION,
        "task_id": TASK_ID,
        "status": "PRE_RESULT_FROZEN",
        "generated_at": generated_at.isoformat(),
        "append_only": True,
        "audit_policy": {
            "policy_id": AUDIT_POLICY_ID,
            "path": policy_path.as_posix(),
            "sha256_at_freeze": policy_sha256,
            "owner_decision": policy["owner_decision"],
        },
        "dq_gate": dict(dq_binding),
        "software_identity": dict(software_identity),
        "current_attempt": {
            "sequence": 1,
            "attempt_family_id": family_id,
            "model_id": _mapping(policy["model_feature_contract"], "model_feature_contract")[
                "model_id"
            ],
            "feature_family_prefix": _mapping(
                policy["model_feature_contract"], "model_feature_contract"
            )["family_prefix"],
            "source_status_at_freeze": source_status,
            "coverage_read": False,
            "result_read": False,
            "model_trained": False,
        },
        "historical_contamination": dict(
            _mapping(attempt_contract["historical_contamination"], "historical_contamination")
        ),
        "prohibited_same_attempt_additions": list(
            _sequence(
                attempt_contract["prohibited_same_attempt_additions"],
                "prohibited_same_attempt_additions",
            )
        ),
        "claim_boundary": _claim_boundary(),
    }
    return {"ledger_id": f"o1_attempt_ledger_{_digest(body)[:32]}", **body}


def _build_source_manifest(
    *,
    policy: Mapping[str, object],
    generated_at: datetime,
    status: str,
    requests: Sequence[Mapping[str, object]],
    discovered: Mapping[str, Sequence[str]],
    blockers: Sequence[Mapping[str, object]],
    software_identity: Mapping[str, object],
    replay_provenance: Mapping[str, object] | None = None,
) -> Mapping[str, object]:
    start, end = _research_window(policy)
    family_status = {
        family: {
            "index_request_count": sum(
                1
                for item in requests
                if item["event_family"] == family and item["source_role"] == "INDEX"
            ),
            "release_url_count": len(discovered[family]),
            "release_request_count": sum(
                1
                for item in requests
                if item["event_family"] == family and item["source_role"] == "RELEASE"
            ),
            "status": (
                "BLOCKED"
                if any(item.get("event_family") == family for item in blockers)
                else "PASS"
            ),
        }
        for family in REQUIRED_EVENT_FAMILIES
    }
    body = {
        "schema_version": SOURCE_MANIFEST_SCHEMA_VERSION,
        "task_id": TASK_ID,
        "status": status,
        "generated_at": generated_at.isoformat(),
        "research_window": {"start": start.isoformat(), "end": end.isoformat()},
        "allowed_source_domains": list(ALLOWED_SOURCE_DOMAINS),
        "software_identity": dict(software_identity),
        "requests": list(requests),
        "family_status": family_status,
        "blockers": list(blockers),
        "claim_boundary": _claim_boundary(),
    }
    if replay_provenance is not None:
        body["replay_provenance"] = dict(replay_provenance)
    return {"manifest_id": f"o1_event_source_{_digest(body)[:32]}", **body}


def _build_event_ledger(
    *,
    policy: Mapping[str, object],
    generated_at: datetime,
    source_manifest: Mapping[str, object],
    events: Sequence[Mapping[str, object]],
) -> Mapping[str, object]:
    required_fields = set(
        _sequence(
            _mapping(policy["event_contract"], "event_contract")["exact_event_ledger_fields"],
            "exact_event_ledger_fields",
        )
    )
    for row in events:
        if not required_fields.issubset(row):
            _fail(
                "O1_EVENT_LEDGER_FIELDS_MISSING",
                ",".join(sorted(required_fields - set(row))),
            )
    family_summary = {
        family: {
            "event_count": sum(1 for row in events if row["event_family"] == family),
            "first_event_timestamp": min(
                str(row["event_timestamp"])
                for row in events
                if row["event_family"] == family
            ),
            "last_event_timestamp": max(
                str(row["event_timestamp"])
                for row in events
                if row["event_family"] == family
            ),
        }
        for family in REQUIRED_EVENT_FAMILIES
    }
    body = {
        "schema_version": EVENT_LEDGER_SCHEMA_VERSION,
        "task_id": TASK_ID,
        "status": "PASS",
        "generated_at": generated_at.isoformat(),
        "source_manifest_id": source_manifest["manifest_id"],
        "event_window_common_sessions": list(
            _mapping(policy["event_contract"], "event_contract")[
                "event_window_common_sessions"
            ]
        ),
        "timestamp_semantics": (
            "actual official release occurrence; posthoc falsification strata only; "
            "never a model feature or claim of advance schedule knowledge"
        ),
        "family_summary": family_summary,
        "events": list(events),
        "claim_boundary": _claim_boundary(),
    }
    return {"ledger_id": f"o1_event_ledger_{_digest(body)[:32]}", **body}


def _build_gate(
    *,
    policy: Mapping[str, object],
    policy_path: Path,
    policy_sha256: str,
    dq_binding: Mapping[str, object],
    generated_at: datetime,
    status: str,
    source_manifest_path: Path,
    source_manifest: Mapping[str, object],
    attempt_ledger_path: Path,
    attempt_ledger: Mapping[str, object],
    event_ledger_path: Path | None,
    event_ledger: Mapping[str, object] | None,
    blockers: Sequence[Mapping[str, object]],
    software_identity: Mapping[str, object],
    replay_provenance: Mapping[str, object] | None = None,
) -> Mapping[str, object]:
    artifacts: dict[str, object] = {
        "event_source_manifest": _artifact_binding(source_manifest_path, source_manifest),
        "attempt_ledger": _artifact_binding(attempt_ledger_path, attempt_ledger),
        "event_ledger": (
            None
            if event_ledger_path is None or event_ledger is None
            else _artifact_binding(event_ledger_path, event_ledger)
        ),
    }
    body = {
        "schema_version": FREEZE_GATE_SCHEMA_VERSION,
        "task_id": TASK_ID,
        "status": status,
        "generated_at": generated_at.isoformat(),
        "audit_policy": {
            "policy_id": AUDIT_POLICY_ID,
            "path": policy_path.as_posix(),
            "sha256_at_execution": policy_sha256,
            "owner_decision": policy["owner_decision"],
        },
        "dq_gate": dict(dq_binding),
        "software_identity": dict(software_identity),
        "attempt_family_id": _mapping(
            policy["attempt_ledger_contract"], "attempt_ledger_contract"
        )["current_attempt_family_id"],
        "artifacts": artifacts,
        "blockers": list(blockers),
        "mechanical_classification": None if status == PASS_STATUS else BLOCKED_CLASS,
        "next_authorization": {
            "coverage_only_gate_allowed": status == PASS_STATUS,
            "model_training_allowed": False,
            "canonical_run_allowed": False,
            "production_allowed": False,
        },
        "claim_boundary": _claim_boundary(),
    }
    if replay_provenance is not None:
        body["replay_provenance"] = dict(replay_provenance)
    return {"gate_id": f"o1_event_attempt_gate_{_digest(body)[:32]}", **body}


def _artifact_binding(
    path: Path,
    payload: Mapping[str, object],
) -> Mapping[str, object]:
    identity = (
        payload.get("manifest_id")
        or payload.get("ledger_id")
        or payload.get("gate_id")
    )
    return {
        "id": identity,
        "path": path.as_posix(),
        "sha256": sha256_path(path),
        "byte_size": path.stat().st_size,
    }


def _verify_dq_gate(
    *,
    policy: Mapping[str, object],
    project_root: Path,
) -> Mapping[str, object]:
    evidence = _mapping(policy["isolated_dq_evidence"], "isolated_dq_evidence")
    if evidence.get("status") != "PASS":
        _fail("O1_EVENT_DQ_NOT_PASS", str(evidence.get("status")))
    gate_binding = _mapping(evidence["gate"], "isolated_dq_evidence.gate")
    gate_path = _contained_file(
        project_root,
        Path(_text(gate_binding["path"], "dq gate path")),
        "O1_EVENT_DQ_GATE_MISSING",
    )
    expected_sha = _sha(gate_binding["sha256"], "dq gate sha256")
    observed_sha = sha256_path(gate_path)
    if observed_sha != expected_sha:
        _fail(
            "O1_EVENT_DQ_GATE_TAMPERED",
            f"expected={expected_sha} actual={observed_sha}",
            path=gate_path,
        )
    gate = _load_json_mapping(gate_path, "O1_EVENT_DQ_GATE_INVALID")
    validate_o1_dq_gate(gate)
    if gate.get("gate_id") != gate_binding.get("gate_id"):
        _fail("O1_EVENT_DQ_GATE_ID_MISMATCH", str(gate.get("gate_id")))
    return {
        "gate_id": gate["gate_id"],
        "path": gate_path.as_posix(),
        "sha256": observed_sha,
        "fresh_receipt_id": _mapping(gate["fresh_data_quality"], "fresh_data_quality")[
            "receipt_id"
        ],
    }


def _validate_policy(policy: Mapping[str, object]) -> None:
    if policy.get("schema_version") != AUDIT_POLICY_SCHEMA_VERSION:
        _fail("O1_EVENT_POLICY_INVALID", "schema_version mismatch")
    if policy.get("policy_id") != AUDIT_POLICY_ID:
        _fail("O1_EVENT_POLICY_INVALID", "policy_id mismatch")
    if policy.get("owner_decision") != (
        "owner_decision:TRADING-2464:2026-07-30:"
        "approve_o1_m1_ridge_cross_asset_state_single_family_v1"
    ):
        _fail("O1_EVENT_OWNER_DECISION_MISSING", str(policy.get("owner_decision")))
    execution = _mapping(policy["execution_binding"], "execution_binding")
    if (
        execution.get("real_coverage_read_allowed_now") is not False
        or execution.get("model_training_allowed_now") is not False
        or execution.get("maximum_canonical_runs") != 1
    ):
        _fail("O1_EVENT_POLICY_SCOPE_INVALID", "pre-coverage scope is not frozen")
    event = _mapping(policy["event_contract"], "event_contract")
    if tuple(event.get("mandatory_event_families", ())) != REQUIRED_EVENT_FAMILIES:
        _fail("O1_EVENT_POLICY_FAMILIES_INVALID", str(event.get("mandatory_event_families")))
    if event.get("current_view_or_reconstructed_unknown_known_at_allowed") is not False:
        _fail("O1_EVENT_POLICY_SCOPE_INVALID", "current-view reconstruction is allowed")
    attempt = _mapping(policy["attempt_ledger_contract"], "attempt_ledger_contract")
    if (
        attempt.get("schema_version") != ATTEMPT_LEDGER_SCHEMA_VERSION
        or attempt.get("append_only") is not True
        or attempt.get("current_attempt_family_id")
        != "O1_M1_RIDGE_CROSS_ASSET_STATE_V1"
    ):
        _fail("O1_EVENT_ATTEMPT_POLICY_INVALID", str(attempt))


def _validate_event_rows(
    events: Sequence[Mapping[str, object]],
    *,
    blockers: list[Mapping[str, object]],
) -> None:
    if blockers:
        return
    ids = [str(row["event_id"]) for row in events]
    if len(ids) != len(set(ids)):
        _fail("O1_EVENT_DUPLICATE_ID", "event ids are not unique")
    for family in REQUIRED_EVENT_FAMILIES:
        if not any(row["event_family"] == family for row in events):
            _fail("O1_EVENT_FAMILY_EMPTY", family)


def _new_output_root(
    *,
    project_root: Path,
    output_root: Path,
    policy: Mapping[str, object],
) -> Path:
    evidence = _mapping(policy["isolated_dq_evidence"], "isolated_dq_evidence")
    allowed_parent = Path(_text(evidence["output_root"], "isolated DQ output root"))
    allowed_parent = allowed_parent.resolve(strict=True)
    resolved = output_root.resolve(strict=False)
    if resolved.parent != allowed_parent:
        _fail(
            "O1_EVENT_OUTPUT_ROOT_INVALID",
            f"expected parent={allowed_parent.as_posix()}",
            path=resolved,
        )
    if resolved.name != "o1_event_attempt_freeze_v1":
        _fail("O1_EVENT_OUTPUT_ROOT_INVALID", "unexpected workspace name", path=resolved)
    if resolved.exists():
        _fail("O1_EVENT_OUTPUT_ROOT_EXISTS", "refusing overwrite", path=resolved)
    if project_root not in resolved.parents:
        _fail("O1_EVENT_OUTPUT_ROOT_INVALID", "workspace escaped project root", path=resolved)
    return resolved


def _existing_output_root(
    *,
    project_root: Path,
    output_root: Path,
    policy: Mapping[str, object],
) -> Path:
    evidence = _mapping(policy["isolated_dq_evidence"], "isolated_dq_evidence")
    allowed_parent = Path(_text(evidence["output_root"], "isolated DQ output root"))
    allowed_parent = allowed_parent.resolve(strict=True)
    resolved = output_root.resolve(strict=True)
    if (
        resolved.parent != allowed_parent
        or resolved.name != "o1_event_attempt_freeze_v1"
        or project_root not in resolved.parents
        or not resolved.is_dir()
    ):
        _fail(
            "O1_EVENT_REPLAY_OUTPUT_ROOT_INVALID",
            f"expected retained root below {allowed_parent.as_posix()}",
            path=resolved,
        )
    return resolved


def _load_verified_json(
    path: Path,
    *,
    expected_sha256: str,
    code: str,
) -> Mapping[str, object]:
    expected = _sha(expected_sha256, f"{path.name} expected sha256")
    observed = sha256_path(path)
    if observed != expected:
        _fail(code, f"expected={expected} actual={observed}", path=path)
    return _load_json_mapping(path, code)


def _validate_replay_authority(
    *,
    initial_source_path: Path,
    initial_source: Mapping[str, object],
    initial_attempt_path: Path,
    initial_attempt: Mapping[str, object],
    initial_gate: Mapping[str, object],
    policy_sha256: str,
    dq_binding: Mapping[str, object],
) -> None:
    _validate_content_addressed_id(
        initial_source,
        id_field="manifest_id",
        prefix="o1_event_source_",
        code="O1_EVENT_REPLAY_INITIAL_SOURCE_MANIFEST_ID_MISMATCH",
    )
    _validate_content_addressed_id(
        initial_attempt,
        id_field="ledger_id",
        prefix="o1_attempt_ledger_",
        code="O1_EVENT_REPLAY_INITIAL_ATTEMPT_LEDGER_ID_MISMATCH",
    )
    validate_o1_event_attempt_freeze_gate(initial_gate)
    if (
        initial_source.get("schema_version") != SOURCE_MANIFEST_SCHEMA_VERSION
        or initial_source.get("status") != BLOCKED_STATUS
        or initial_gate.get("status") != BLOCKED_STATUS
    ):
        _fail(
            "O1_EVENT_REPLAY_INITIAL_STATUS_INVALID",
            "replay requires the immutable blocked source acquisition",
        )
    blockers = [
        _mapping(item, "initial blocker")
        for item in _sequence(initial_source.get("blockers"), "initial blockers")
    ]
    if not blockers or any(
        item.get("code") != "O1_EVENT_BLS_TIMESTAMP_MISSING"
        or item.get("event_family") != "NFP"
        for item in blockers
    ):
        _fail(
            "O1_EVENT_REPLAY_BLOCKER_SCOPE_INVALID",
            "only the reviewed NFP timestamp parser defect may be replayed",
        )
    if initial_gate.get("blockers") != initial_source.get("blockers"):
        _fail("O1_EVENT_REPLAY_BLOCKER_MISMATCH", "gate/source blockers differ")
    current_attempt = _mapping(initial_attempt.get("current_attempt"), "current_attempt")
    if (
        initial_attempt.get("schema_version") != ATTEMPT_LEDGER_SCHEMA_VERSION
        or initial_attempt.get("status") != "PRE_RESULT_FROZEN"
        or initial_attempt.get("append_only") is not True
        or current_attempt.get("attempt_family_id")
        != "O1_M1_RIDGE_CROSS_ASSET_STATE_V1"
        or current_attempt.get("source_status_at_freeze") != BLOCKED_STATUS
        or current_attempt.get("coverage_read") is not False
        or current_attempt.get("result_read") is not False
        or current_attempt.get("model_trained") is not False
    ):
        _fail(
            "O1_EVENT_REPLAY_ATTEMPT_SCOPE_INVALID",
            "initial attempt is not the untouched pre-result family",
        )
    if initial_gate.get("attempt_family_id") != current_attempt.get("attempt_family_id"):
        _fail("O1_EVENT_REPLAY_ATTEMPT_FAMILY_MISMATCH", "gate/attempt family differs")
    source_identity = initial_source.get("software_identity")
    if (
        source_identity != initial_attempt.get("software_identity")
        or source_identity != initial_gate.get("software_identity")
    ):
        _fail(
            "O1_EVENT_REPLAY_SOFTWARE_IDENTITY_MISMATCH",
            "initial artifacts do not bind one software identity",
        )
    audit_policy = _mapping(initial_gate.get("audit_policy"), "initial gate audit_policy")
    attempt_policy = _mapping(initial_attempt.get("audit_policy"), "initial attempt policy")
    if (
        audit_policy.get("sha256_at_execution") != policy_sha256
        or attempt_policy.get("sha256_at_freeze") != policy_sha256
        or audit_policy.get("owner_decision") != attempt_policy.get("owner_decision")
    ):
        _fail(
            "O1_EVENT_REPLAY_POLICY_IDENTITY_MISMATCH",
            "initial artifacts do not bind the active unchanged policy",
        )
    if initial_gate.get("dq_gate") != dq_binding or initial_attempt.get("dq_gate") != dq_binding:
        _fail("O1_EVENT_REPLAY_DQ_IDENTITY_MISMATCH", "initial DQ binding changed")
    artifacts = _mapping(initial_gate.get("artifacts"), "initial gate artifacts")
    _verify_artifact_binding(
        _mapping(artifacts.get("event_source_manifest"), "initial source binding"),
        expected_path=initial_source_path,
        payload=initial_source,
    )
    _verify_artifact_binding(
        _mapping(artifacts.get("attempt_ledger"), "initial attempt binding"),
        expected_path=initial_attempt_path,
        payload=initial_attempt,
    )
    if artifacts.get("event_ledger") is not None:
        _fail("O1_EVENT_REPLAY_INITIAL_SCOPE_INVALID", "blocked gate bound an event ledger")


def _validate_content_addressed_id(
    payload: Mapping[str, object],
    *,
    id_field: str,
    prefix: str,
    code: str,
) -> None:
    identity = payload.get(id_field)
    if not isinstance(identity, str):
        _fail(code, str(identity))
    body = {key: value for key, value in payload.items() if key != id_field}
    if identity != f"{prefix}{_digest(body)[:32]}":
        _fail(code, identity)


def _verify_artifact_binding(
    binding: Mapping[str, object],
    *,
    expected_path: Path,
    payload: Mapping[str, object],
) -> None:
    bound_path = Path(_text(binding.get("path"), "artifact binding path")).resolve(strict=True)
    if bound_path != expected_path:
        _fail(
            "O1_EVENT_REPLAY_ARTIFACT_PATH_MISMATCH",
            f"expected={expected_path.as_posix()} actual={bound_path.as_posix()}",
        )
    expected_identity = (
        payload.get("manifest_id")
        or payload.get("ledger_id")
        or payload.get("gate_id")
    )
    if (
        binding.get("id") != expected_identity
        or binding.get("sha256") != sha256_path(expected_path)
        or binding.get("byte_size") != expected_path.stat().st_size
    ):
        _fail(
            "O1_EVENT_REPLAY_ARTIFACT_BINDING_MISMATCH",
            expected_path.as_posix(),
        )


def _verify_retained_sources(
    *,
    output_root: Path,
    source_manifest: Mapping[str, object],
) -> tuple[
    list[Mapping[str, object]],
    dict[int, bytes],
    tuple[Path, ...],
]:
    raw_root = (output_root / "raw_primary_sources").resolve(strict=True)
    if not raw_root.is_dir():
        _fail("O1_EVENT_REPLAY_RAW_ROOT_INVALID", "not a directory", path=raw_root)
    requests = [
        _mapping(item, "retained source request")
        for item in _sequence(source_manifest.get("requests"), "retained source requests")
    ]
    if not requests:
        _fail("O1_EVENT_REPLAY_REQUESTS_EMPTY", "no retained source requests")
    seen_requests: set[tuple[str, str, str]] = set()
    referenced_paths: set[Path] = set()
    bodies: dict[int, bytes] = {}
    for index, record in enumerate(requests):
        family = _text(record.get("event_family"), "event family")
        source_role = _text(record.get("source_role"), "source role")
        endpoint = _text(record.get("endpoint"), "source endpoint")
        final_url = _text(record.get("final_url"), "source final_url")
        _require_official_url(endpoint)
        _require_official_url(final_url)
        request_key = (family, source_role, endpoint)
        if (
            family not in REQUIRED_EVENT_FAMILIES
            or source_role not in {"INDEX", "RELEASE"}
            or request_key in seen_requests
        ):
            _fail("O1_EVENT_REPLAY_REQUEST_SCOPE_INVALID", str(request_key))
        seen_requests.add(request_key)
        if (
            record.get("provider_name") != _provider_name(family)
            or record.get("provider_class") != "primary_source"
            or record.get("request_parameters") != {}
            or record.get("http_status") != 200
            or record.get("status") != "PASS"
            or record.get("error") is not None
        ):
            _fail("O1_EVENT_REPLAY_SOURCE_NOT_COMPLETE", endpoint)
        artifact_path = _contained_file(
            output_root,
            Path(_text(record.get("artifact_path"), "source artifact path")),
            "O1_EVENT_REPLAY_RAW_ARTIFACT_MISSING",
        )
        if raw_root not in artifact_path.parents:
            _fail(
                "O1_EVENT_REPLAY_RAW_ARTIFACT_ESCAPE",
                endpoint,
                path=artifact_path,
            )
        body = artifact_path.read_bytes()
        expected_sha = _sha(record.get("checksum"), "source checksum")
        if (
            hashlib.sha256(body).hexdigest() != expected_sha
            or len(body) != record.get("byte_size")
        ):
            _fail(
                "O1_EVENT_REPLAY_RAW_ARTIFACT_TAMPERED",
                endpoint,
                path=artifact_path,
            )
        referenced_paths.add(artifact_path)
        bodies[index] = body
    observed_paths = {
        path.resolve()
        for path in raw_root.rglob("*")
        if path.is_file()
    }
    if observed_paths != referenced_paths:
        _fail(
            "O1_EVENT_REPLAY_RAW_INVENTORY_MISMATCH",
            (
                f"referenced={len(referenced_paths)} "
                f"observed={len(observed_paths)}"
            ),
        )
    return requests, bodies, tuple(sorted(referenced_paths))


def _rediscover_retained_release_urls(
    *,
    policy: Mapping[str, object],
    request_records: Sequence[Mapping[str, object]],
    source_bodies: Mapping[int, bytes],
) -> Mapping[str, list[str]]:
    discovered: dict[str, list[str]] = {family: [] for family in REQUIRED_EVENT_FAMILIES}
    retained_releases: dict[str, list[str]] = {
        family: [] for family in REQUIRED_EVENT_FAMILIES
    }
    for index, record in enumerate(request_records):
        family = _text(record["event_family"], "event family")
        role = _text(record["source_role"], "source role")
        endpoint = _text(record["endpoint"], "source endpoint")
        if role == "INDEX":
            discovered[family].extend(
                _discover_release_urls(
                    family=family,
                    index_url=endpoint,
                    body=source_bodies[index],
                    policy=policy,
                )
            )
        else:
            retained_releases[family].append(endpoint)
    for family in REQUIRED_EVENT_FAMILIES:
        discovered[family] = sorted(set(discovered[family]))
        retained_releases[family] = sorted(set(retained_releases[family]))
        if discovered[family] != retained_releases[family]:
            _fail(
                "O1_EVENT_REPLAY_RELEASE_INVENTORY_MISMATCH",
                (
                    f"family={family} discovered={len(discovered[family])} "
                    f"retained={len(retained_releases[family])}"
                ),
            )
    return discovered


def _research_window(policy: Mapping[str, object]) -> tuple[date, date]:
    data_contract = _mapping(policy["data_contract"], "data_contract")
    return (
        date.fromisoformat(_text(data_contract["primary_research_start"], "research start")),
        date.fromisoformat(_text(data_contract["evaluated_end"], "evaluated end")),
    )


def _research_years(policy: Mapping[str, object]) -> range:
    start, end = _research_window(policy)
    return range(start.year, end.year + 1)


def _software_identity(
    *,
    project_root: Path,
    source_commit_sha: str | None,
    cli_argv: Sequence[str],
) -> Mapping[str, object]:
    commit = source_commit_sha or _git_head(project_root)
    if re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        _fail("O1_EVENT_SOURCE_COMMIT_INVALID", commit)
    dependency_manifest = _contained_file(
        project_root,
        Path("pyproject.toml"),
        "O1_EVENT_DEPENDENCY_MANIFEST_MISSING",
    )
    return {
        "python_version": platform.python_version(),
        "package_lock_path": dependency_manifest.relative_to(project_root).as_posix(),
        "package_lock_kind": "UNLOCKED_PROJECT_MANIFEST",
        "package_lock_sha256": sha256_path(dependency_manifest),
        "source_commit_sha": commit,
        "cli_argv": list(cli_argv),
    }


def _git_head(project_root: Path) -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=project_root,
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    if completed.returncode != 0:
        _fail("O1_EVENT_SOURCE_COMMIT_UNAVAILABLE", completed.stderr.strip())
    return completed.stdout.strip()


def _date_from_release_url(
    url: str,
    patterns: Mapping[str, re.Pattern[str]],
) -> date:
    path = urlparse(url).path
    for pattern in patterns.values():
        match = pattern.search(path)
        if match is not None:
            return datetime.strptime(match.group(1), "%m%d%Y").date()
    _fail("O1_EVENT_BLS_URL_INVALID", url)


def _source_blocker(record: Mapping[str, object]) -> Mapping[str, object]:
    return {
        "code": "O1_EVENT_OFFICIAL_SOURCE_UNAVAILABLE",
        "event_family": record["event_family"],
        "endpoint": record["endpoint"],
        "http_status": record["http_status"],
        "error": record["error"],
        "response_checksum": record["checksum"],
        "response_artifact_path": record["artifact_path"],
    }


def _provider_name(family: str) -> str:
    return (
        "Federal Reserve Board / FOMC"
        if family == "FOMC"
        else "U.S. Bureau of Labor Statistics"
    )


def _require_official_url(url: str) -> None:
    parsed = urlparse(url)
    hostname = (parsed.hostname or "").lower()
    if parsed.scheme != "https" or not any(
        hostname == domain or hostname.endswith(f".{domain}")
        for domain in ALLOWED_SOURCE_DOMAINS
    ):
        _fail("O1_EVENT_SOURCE_DOMAIN_FORBIDDEN", url)


def _safe_suffix(url: str, content_type: str | None) -> str:
    suffix = Path(urlparse(url).path).suffix.lower()
    if suffix in {".htm", ".html", ".pdf"}:
        return suffix
    if content_type == "application/pdf":
        return ".pdf"
    return ".html"


def _decode_html(body: bytes) -> str:
    return body.decode("utf-8", errors="replace")


def _visible_text(body: bytes) -> str:
    text = _decode_html(body)
    text = re.sub(r"(?is)<script.*?</script>|<style.*?</style>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", html.unescape(text)).strip()


def _claim_boundary() -> Mapping[str, object]:
    return {
        "coverage_audit_executed": False,
        "model_training_executed": False,
        "new_o1_result_read": False,
        "prospective_accessed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        _fail("O1_EVENT_TIMESTAMP_INVALID", "timestamp must be timezone-aware")
    return value.astimezone(UTC)


def _contained_file(root: Path, path: Path, code: str) -> Path:
    candidate = path if path.is_absolute() else root / path
    resolved = candidate.resolve(strict=True)
    if root != resolved and root not in resolved.parents:
        _fail("O1_EVENT_PATH_ESCAPE", str(path), path=resolved)
    if not resolved.is_file():
        _fail(code, "not a regular file", path=resolved)
    return resolved


def _load_yaml_mapping(path: Path, code: str) -> Mapping[str, object]:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        _fail(code, str(exc), path=path)
    return _mapping(payload, path.as_posix())


def _load_json_mapping(path: Path, code: str) -> Mapping[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        _fail(code, str(exc), path=path)
    return _mapping(payload, path.as_posix())


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        _fail("O1_EVENT_FIELDS_INVALID", f"{field} must be a mapping")
    return value


def _sequence(value: object, field: str) -> Sequence[object]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        _fail("O1_EVENT_FIELDS_INVALID", f"{field} must be a sequence")
    return value


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        _fail("O1_EVENT_FIELDS_INVALID", f"{field} must be non-empty text")
    return value


def _sha(value: object, field: str) -> str:
    text = _text(value, field)
    if _SHA_PATTERN.fullmatch(text) is None:
        _fail("O1_EVENT_FIELDS_INVALID", f"{field} must be lowercase SHA-256")
    return text


def _digest(payload: object) -> str:
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _fail(code: str, message: str, *, path: Path | None = None) -> NoReturn:
    raise O1EventAttemptFreezeError(code, message, path=path)
