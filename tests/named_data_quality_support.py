"""Synthetic named-DQ publication fixtures (tmp-path bytes, never the real market cache).

GOV-008 protocol v3: the actual-candidate parent that needed a publication transaction and an
S4D lease environment was removed with the R1 real-chain variants. What stays is the synthetic
publication builder used by DQ-semantics tests: market-like rows and their immutable publication
exist only under pytest's ``tmp_path``; nothing here grants consumer, research or trading scope.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from pathlib import Path

import pytest

from ai_trading_system.contracts.data_quality_execution import DataQualityDateWindow
from ai_trading_system.contracts.named_data_quality_execution import (
    EQUAL_RISK_GUARD_RATE_SERIES,
    EQUAL_RISK_PRICE_SOURCE_MANIFEST_PATH,
    EQUAL_RISK_PRICE_TICKERS,
    FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_PATH,
    PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_PATH,
    NamedDQExecutionRequest,
    NamedDQRoots,
    NamedDQScope,
    NamedSnapshotSelector,
)
from ai_trading_system.data.download_publication import (
    DownloadArtifactCandidate,
    DownloadSourceBinding,
    ValidatedDownloadPublication,
    publish_download_transaction,
)

ROOT = Path(__file__).resolve().parents[1]
SOURCE_MANIFEST_PATH = "config/data_governance/named_data_quality_execution_sources_v1.json"
BOOTSTRAP_PATH = "scripts/run_named_data_quality.py"
AS_OF = date(2026, 9, 3)
START = date(2026, 9, 2)
PRICE_BYTES = (
    b"date,ticker,open,high,low,close,adj_close,volume\n"
    b"2026-09-02,QQQ,100,102,99,100,100,1000\n"
    b"2026-09-03,QQQ,100,102,99,101,101,1100\n"
)
RATE_BYTES = b"date,series,value\n2026-09-02,DGS10,4.2\n2026-09-03,DGS10,4.2\n"


def _sha(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _json_bytes(payload: object) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
    ).encode("utf-8")


def _instant() -> str:
    return datetime.now(UTC).isoformat()


def _git(root: Path, *arguments: str) -> str:
    environment = {
        key: value for key, value in os.environ.items() if not key.upper().startswith("GIT_")
    }
    environment.update(
        GIT_CONFIG_NOSYSTEM="1",
        GIT_CONFIG_GLOBAL=os.devnull,
        GIT_CONFIG_SYSTEM=os.devnull,
        GIT_TERMINAL_PROMPT="0",
        GIT_NO_REPLACE_OBJECTS="1",
        GIT_NO_LAZY_FETCH="1",
        GIT_OPTIONAL_LOCKS="0",
    )
    result = subprocess.run(
        [
            "git",
            "--no-replace-objects",
            "--no-lazy-fetch",
            "-c",
            f"safe.directory={root.as_posix()}",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "protocol.allow=never",
            "-C",
            str(root),
            *arguments,
        ],
        env=environment,
        capture_output=True,
        check=False,
    )
    if result.returncode:
        pytest.fail("NAMED_PARENT_LOCAL_GIT_FAILED: " + " ".join(arguments))
    return result.stdout.decode("utf-8").strip()


def _write_new(path: Path, content: bytes) -> dict[str, object]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(content)
    return {"path": path.as_posix(), "sha256": _sha(content), "size_bytes": len(content)}


@dataclass(frozen=True)
class NamedExecutionFixture:
    request: NamedDQExecutionRequest
    source_root: Path
    publication_root: Path
    evidence_root: Path
    publication: ValidatedDownloadPublication
    input_bytes: dict[str, bytes]


def build_actual_candidate_fixture(
    tmp_path: Path,
    *,
    execution_root: Path = ROOT,
    prices_content: bytes | None = None,
    rates_content: bytes | None = None,
    requested_start: date = START,
    requested_end: date = AS_OF,
    as_of: date = AS_OF,
    expected_price_tickers: tuple[str, ...] = ("QQQ",),
    expected_rate_series: tuple[str, ...] = ("DGS10",),
    equal_risk_price_profile: bool = False,
    five_candidate_preview_profile: bool = False,
    prospective_capture_profile: bool = False,
    expected_evaluated_window: DataQualityDateWindow | None = None,
) -> NamedExecutionFixture:
    """Publish synthetic bytes, then relocate only that tmp-path publication.

    The publisher's finite source-kind enum has no TEST kind. LIVE_PROVIDER is
    therefore a *fixture schema value*, with an explicit SYNTHETIC_FIXTURE name
    and memory-only endpoint; it does not represent an actual provider request.
    Original manifest output paths are preserved when copying the synthetic
    publication. No source, policy, or real market cache is copied from ROOT.
    """
    if (
        type(equal_risk_price_profile) is not bool
        or type(five_candidate_preview_profile) is not bool
        or type(prospective_capture_profile) is not bool
        or sum(
            (equal_risk_price_profile, five_candidate_preview_profile, prospective_capture_profile)
        )
        > 1
    ):
        pytest.fail("NAMED_PARENT_PRICE_PROFILE_MUST_BE_BOOL")
    source_manifest_path = (
        PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_PATH
        if prospective_capture_profile
        else (
            FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_PATH
            if five_candidate_preview_profile
            else (
                EQUAL_RISK_PRICE_SOURCE_MANIFEST_PATH
                if equal_risk_price_profile
                else SOURCE_MANIFEST_PATH
            )
        )
    )
    execution_root = execution_root.resolve()
    if execution_root != ROOT:
        pytest.fail("NAMED_PARENT_ACTUAL_CHECKOUT_REQUIRED: no copied execution checkout")
    if Path(_git(execution_root, "rev-parse", "--show-toplevel")).resolve() != execution_root:
        pytest.fail("NAMED_PARENT_GIT_ROOT_MISMATCH")
    candidate = _git(execution_root, "rev-parse", "HEAD")
    source_root = tmp_path / "synthetic-source"
    source_output = source_root / "data/raw"
    publication_root = tmp_path / "synthetic-publication"
    evidence_root = tmp_path / "synthetic-evidence"
    evidence_root.mkdir(parents=True)
    inputs = {
        "prices": PRICE_BYTES if prices_content is None else prices_content,
        "rates": RATE_BYTES if rates_content is None else rates_content,
    }
    artifacts: list[DownloadArtifactCandidate] = []
    bindings: list[DownloadSourceBinding] = []
    for role, content in inputs.items():
        records = list(csv.DictReader(io.StringIO(content.decode("utf-8"))))
        dimension = "ticker" if role == "prices" else "series"
        event_id = f"synthetic-fixture:{role}"
        artifacts.append(
            DownloadArtifactCandidate(
                role=role,
                filename=f"{role}_daily.csv",
                content=content,
                row_count=len(records),
                source_event_ids=(event_id,),
            )
        )
        bindings.append(
            DownloadSourceBinding(
                source_event_id=event_id,
                artifact_role=role,
                source_kind="LIVE_PROVIDER",
                source_id=f"synthetic-fixture-{role}",
                provider="SYNTHETIC_FIXTURE",
                endpoint=f"memory:synthetic-fixture-{role}",
                request_parameters={"synthetic_only": True, "network_request_count": 0},
                winning_row_count=len(records),
                allocation_mode="REMAINDER",
                # Binding order is canonical (dimension, date); CSV bytes and
                # ordinals stay untouched. Do not deduplicate invalid fixtures.
                winning_row_keys=tuple(sorted((row[dimension], row["date"]) for row in records)),
            )
        )
    publication = publish_download_transaction(
        output_dir=source_output,
        requested_start=requested_start,
        requested_end=requested_end,
        published_at=datetime.combine(requested_end, time(21), tzinfo=UTC),
        artifacts=tuple(artifacts),
        source_bindings=tuple(bindings),
    )
    pointer = json.loads(publication.discovery_pointer_path.read_bytes())
    # Both ends are newly created fixture directories. This is not real cache
    # migration and does not write to, copy, or reinterpret the execution root.
    shutil.copytree(source_output, publication_root)
    manifest = (execution_root / source_manifest_path).read_bytes()
    window = DataQualityDateWindow(requested_start, requested_end)
    request = NamedDQExecutionRequest(
        roots=NamedDQRoots(
            source_root=source_root.as_posix(),
            publication_root=publication_root.as_posix(),
            execution_root=execution_root.as_posix(),
            evidence_root=evidence_root.as_posix(),
        ),
        selector=NamedSnapshotSelector(
            pointer_id=pointer["pointer_id"],
            pointer_sha256=publication.discovery_pointer_sha256,
            transaction_id=publication.transaction_id,
            transaction_sha256=publication.transaction_manifest_sha256,
        ),
        scope=NamedDQScope(
            as_of=as_of,
            requested_window=window,
            expected_price_tickers=expected_price_tickers,
            expected_rate_series=expected_rate_series,
            input_roles=("prices", "rates"),
            require_secondary_prices=False,
        ),
        source_output_relative_path="data/raw",
        policy_path="config/data_quality.yaml",
        execution_profile_id="manual.v1",
        candidate_commit=candidate,
        source_manifest_path=source_manifest_path,
        source_manifest_sha256=_sha(manifest),
        expected_evaluated_window=expected_evaluated_window or window,
    )
    return NamedExecutionFixture(
        request, source_root, publication_root, evidence_root, publication, inputs
    )


_PRICE_SCOPE_START = date(2021, 2, 22)
_PRICE_SCOPE_END = date(2021, 2, 24)
_PRICE_SCOPE_RATE_END = date(2021, 2, 23)


def equal_risk_scope_fixture(tmp_path: Path, *, case: str = "lag_one") -> NamedExecutionFixture:
    days = ["2021-02-22", "2021-02-23", "2021-02-24"]
    rows = [
        (day, ticker)
        for day in days
        for ticker in EQUAL_RISK_PRICE_TICKERS
        if not (
            (case == "sgov_missing_last" and (day, ticker) == (days[-1], "SGOV"))
            or (case == "sgov_internal_gap" and (day, ticker) == (days[1], "SGOV"))
            or (case == "tqqq_missing" and ticker == "TQQQ")
        )
    ]
    prices = (
        "date,ticker,open,high,low,close,adj_close,volume\n"
        + "".join(f"{day},{ticker},100,102,99,100,100,1000\n" for day, ticker in rows)
    ).encode()
    rate_days = days[:2]
    if case == "lag_two":
        rate_days = days[:1]
    elif case == "rates_future":
        rate_days = [*days[:2], "2021-02-25"]
    rate_rows = ["date,series,value\n"]
    stale_dates = {"2021-02-22": "2021-01-04", "2021-02-23": "2021-01-05"}
    for day in rate_days:
        for series in EQUAL_RISK_GUARD_RATE_SERIES:
            if case == "rates_missing" and series == "DGS2":
                continue
            observed = stale_dates[day] if case == "rates_stale" and series == "DGS2" else day
            value = "110" if series == "DTWEXBGS" else "1.5"
            rate_rows.append(f"{observed},{series},{value}\n")
    rates = "".join(rate_rows).encode()
    start = date(2021, 2, 23) if case == "request_tail_only" else _PRICE_SCOPE_START
    end = _PRICE_SCOPE_RATE_END if case == "request_through_t_minus_one" else _PRICE_SCOPE_END
    # A stale DGS2 is not hidden by other series reaching T-1. The existing
    # common observation is still T-1, but per-series canonical DQ must fail.
    evaluated_end = (
        _PRICE_SCOPE_START
        if case == "lag_two"
        else _PRICE_SCOPE_END
        if case == "rates_future"
        else _PRICE_SCOPE_RATE_END
    )
    fixture = build_actual_candidate_fixture(
        tmp_path,
        prices_content=prices,
        rates_content=rates,
        requested_start=start,
        requested_end=end,
        as_of=_PRICE_SCOPE_END,
        expected_price_tickers=(
            ("QQQ", "SGOV") if case == "request_two_tickers" else EQUAL_RISK_PRICE_TICKERS
        ),
        expected_rate_series=(
            ("DGS2", "DGS10") if case == "request_two_rates" else EQUAL_RISK_GUARD_RATE_SERIES
        ),
        equal_risk_price_profile=case != "old_manifest",
        expected_evaluated_window=DataQualityDateWindow(start, evaluated_end),
    )
    return fixture
