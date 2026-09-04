from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from ai_trading_system.contracts.data_quality_capability import CapabilityFileBinding
from ai_trading_system.data import quality, quality_capability, quality_execution
from ai_trading_system.data import research_input_readiness as readiness
from ai_trading_system.data.quality import DataFileSummary, DataQualityReport
from ai_trading_system.data.research_input_readiness import (
    ConsumerInputDependencies,
    ResearchInputReadinessRequest,
    inspect_research_input_readiness,
    load_research_input_readiness_request,
)

REPO = Path(__file__).resolve().parents[1]
START = date(2026, 7, 22)
END = date(2026, 7, 23)
CHECKED = datetime(2026, 7, 24, 9, tzinfo=UTC)


@dataclass(frozen=True)
class ReadinessFixture:
    root: Path
    request: ResearchInputReadinessRequest


def _binding(root: Path, path: str, role: str) -> CapabilityFileBinding:
    raw = (root / path).read_bytes()
    rows = (
        sum(1 for _ in csv.DictReader(io.StringIO(raw.decode("utf-8"))))
        if path.endswith(".csv")
        else 0
    )
    return CapabilityFileBinding(
        role=role,
        path=path,
        sha256=hashlib.sha256(raw).hexdigest(),
        size_bytes=len(raw),
        row_count=rows,
    )


def _forbidden(*args: Any, **kwargs: Any) -> Any:
    raise AssertionError("readiness must not dispatch DQ, capability calculation, or publication")


@pytest.fixture
def readiness_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> ReadinessFixture:
    """Build a synthetic retained receipt; the actual DQ evaluator never runs."""

    root = tmp_path / "project"
    (root / "data/raw").mkdir(parents=True)
    for relative in (
        "src/ai_trading_system/data/immutable_publish.py",
        "src/ai_trading_system/data/quality_execution.py",
        "src/ai_trading_system/data/quality.py",
        "src/ai_trading_system/trading_calendar.py",
        "src/ai_trading_system/us_equity_special_closure_policy.py",
        "config/data/us_equity_special_closure_registry.yaml",
        "config/data_quality.yaml",
    ):
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((REPO / relative).read_bytes())
    prices_path = "data/raw/prices_daily.csv"
    rates_path = "data/raw/rates_daily.csv"
    manifest_path = "data/raw/download_manifest.csv"
    (root / prices_path).write_text(
        "date,ticker,open,high,low,close,adj_close,volume,consumer_feature\n"
        "2026-07-22,QQQ,100,101,99,100,100,1000,1\n"
        "2026-07-22,SPY,100,101,99,100,100,1000,1\n"
        "2026-07-23,QQQ,100,101,99,100,100,1000,1\n"
        "2026-07-23,SPY,100,101,99,100,100,1000,1\n",
        encoding="utf-8",
    )
    (root / rates_path).write_text("date,series,value\n2026-07-22,DGS10,4.2\n", encoding="utf-8")
    with (root / manifest_path).open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "downloaded_at",
                "source_id",
                "provider",
                "endpoint",
                "request_parameters",
                "output_path",
                "row_count",
                "checksum_sha256",
            ]
        )
        for path, role in ((prices_path, "prices"), (rates_path, "rates")):
            binding = _binding(root, path, role)
            writer.writerow(
                [
                    CHECKED.isoformat(),
                    f"fixture_{role}",
                    "fixture",
                    "fixture",
                    "{}",
                    path,
                    binding.row_count,
                    binding.sha256,
                ]
            )
    request = ResearchInputReadinessRequest(
        schema_version="research_input_readiness_request.v1",
        execution_root=root,
        source_root=root,
        as_of=END,
        requested_start=START,
        requested_end=END,
        execution_profile_id="manual.v1",
        expected_price_tickers=("QQQ", "SPY"),
        expected_rate_series=("DGS10",),
        require_secondary_prices=False,
        inputs=tuple(
            _binding(root, path, role)
            for path, role in (
                (prices_path, "prices"),
                (rates_path, "rates"),
                (manifest_path, "manifest"),
            )
        ),
        policy=_binding(root, "config/data_quality.yaml", "policy"),
        receipt=None,
        consumer=ConsumerInputDependencies(
            consumer_id="synthetic_consumer",
            consumer_version="1.0.0",
            price_tickers=("QQQ",),
            price_fields=("date", "ticker", "adj_close"),
            rate_series=("DGS10",),
            rate_fields=("date", "series", "value"),
        ),
    )

    def fake_report(**kwargs: Any) -> DataQualityReport:
        return DataQualityReport(
            checked_at=CHECKED,
            as_of=END,
            price_summary=DataFileSummary(
                path=root / prices_path,
                exists=True,
                rows=4,
                sha256=request.inputs[0].sha256,
                min_date=START,
                max_date=END,
            ),
            rate_summary=DataFileSummary(
                path=root / rates_path,
                exists=True,
                rows=1,
                sha256=request.inputs[1].sha256,
                min_date=START,
                max_date=START,
            ),
            manifest_summary=DataFileSummary(
                path=root / manifest_path,
                exists=True,
                rows=2,
                sha256=request.inputs[2].sha256,
            ),
            expected_price_tickers=("QQQ", "SPY"),
            expected_rate_series=("DGS10",),
            price_consistency_start_date=date(2021, 2, 22),
            rate_consistency_start_date=date(2021, 2, 22),
        )

    clock = iter((CHECKED - timedelta(seconds=1), CHECKED + timedelta(seconds=1)))
    monkeypatch.setattr(
        quality_execution, "_utc_now", lambda: next(clock, CHECKED + timedelta(seconds=2))
    )
    monkeypatch.setattr(quality_execution, "validate_data_cache", fake_report)
    result = quality_execution.run_canonical_data_quality_execution(
        request.canonical_request(), project_root=root
    )
    request = request.model_copy(
        update={
            "receipt": _binding(root, result.receipt_path.relative_to(root).as_posix(), "receipt")
        }
    )
    for module, names in (
        (quality_execution, ("run_canonical_data_quality_execution", "validate_data_cache")),
        (quality, ("validate_data_cache",)),
        (
            quality_capability,
            ("build_consumer_data_capability", "verify_consumer_data_capability_receipt"),
        ),
    ):
        for name in names:
            monkeypatch.setattr(module, name, _forbidden)
    return ReadinessFixture(root, request)


def _codes(result: dict[str, Any]) -> set[str]:
    return {item["code"] for item in result["blockers"]}


def _rebind_inputs(fixture: ReadinessFixture) -> ResearchInputReadinessRequest:
    return fixture.request.model_copy(
        update={
            "inputs": tuple(
                _binding(fixture.root, item.path, item.role) for item in fixture.request.inputs
            )
        }
    )


def test_valid_receipt_allows_review_only_and_preserves_rates_lag(
    readiness_fixture: ReadinessFixture,
) -> None:
    before = {
        p.relative_to(readiness_fixture.root): p.read_bytes()
        for p in readiness_fixture.root.rglob("*")
        if p.is_file()
    }
    result = inspect_research_input_readiness(readiness_fixture.request)
    assert result["status"] == "READY_FOR_REVIEW", result
    assert result["canonical_receipt_verification"] == "PASS"
    assert result["evaluated_window"] == {"start": START.isoformat(), "end": START.isoformat()}
    assert result["requested_window"]["end"] == END.isoformat()
    assert not result["dispatch_allowed"] and not result["consumer_cutover_allowed"]
    assert not result["dq_validation_executed"]
    assert result["consumer_capability_verification"] == "NOT_PERFORMED"
    assert all("receipt" != key and "preflight" != key for key in result)
    after = {
        p.relative_to(readiness_fixture.root): p.read_bytes()
        for p in readiness_fixture.root.rglob("*")
        if p.is_file()
    }
    assert after == before


def test_missing_receipt_never_runs_dq(readiness_fixture: ReadinessFixture) -> None:
    result = inspect_research_input_readiness(
        readiness_fixture.request.model_copy(update={"receipt": None})
    )
    assert result["status"] == "NOT_READY"
    assert "CANONICAL_RECEIPT_MISSING" in _codes(result)


def test_root_mismatch_cannot_import_runtime_pass(
    readiness_fixture: ReadinessFixture, tmp_path: Path
) -> None:
    result = inspect_research_input_readiness(
        readiness_fixture.request.model_copy(update={"execution_root": tmp_path})
    )
    assert _codes(result) == {"EXECUTION_SOURCE_ROOT_MISMATCH"}
    assert result["canonical_receipt_verification"] == "NOT_PERFORMED"


@pytest.mark.parametrize("role", ["prices", "rates", "manifest", "policy", "receipt"])
def test_bound_byte_tamper_is_blocked(readiness_fixture: ReadinessFixture, role: str) -> None:
    bindings = (
        *readiness_fixture.request.inputs,
        readiness_fixture.request.policy,
        readiness_fixture.request.receipt,
    )
    binding = next(item for item in bindings if item and item.role == role)
    path = readiness_fixture.root / binding.path
    path.write_bytes(path.read_bytes() + b" ")
    result = inspect_research_input_readiness(readiness_fixture.request)
    assert result["status"] == "NOT_READY"
    assert "INPUT_BINDING_MISMATCH" in _codes(result)


@pytest.mark.parametrize(
    "change,code",
    [
        ({"execution_profile_id": "daily_default.v1"}, "RECEIPT_REQUEST_MISMATCH"),
        (
            {"requested_end": date(2026, 7, 24), "as_of": date(2026, 7, 24)},
            "REQUESTED_WINDOW_MISMATCH",
        ),
        ({"as_of": date(2026, 7, 24)}, "DQ_AS_OF_MISMATCH"),
        ({"expected_price_tickers": ("QQQ",)}, "RECEIPT_REQUEST_MISMATCH"),
    ],
)
def test_request_context_cannot_reuse_another_receipt(
    readiness_fixture: ReadinessFixture, change: dict[str, Any], code: str
) -> None:
    result = inspect_research_input_readiness(readiness_fixture.request.model_copy(update=change))
    assert result["status"] == "NOT_READY"
    assert code in _codes(result)


def test_required_ticker_missing_tail_is_not_hidden_by_other_ticker(
    readiness_fixture: ReadinessFixture,
) -> None:
    path = readiness_fixture.root / readiness_fixture.request.inputs[0].path
    path.write_text(
        path.read_text(encoding="utf-8").replace("2026-07-23,QQQ,100,101,99,100,100,1000,1\n", ""),
        encoding="utf-8",
    )
    result = inspect_research_input_readiness(_rebind_inputs(readiness_fixture))
    assert "REQUIRED_INPUT_COVERAGE_MISSING" in _codes(result)
    qqq = next(x for x in result["required_input_coverage"] if x["instrument"] == "QQQ")
    assert qqq["missing_price_sessions"] == ["2026-07-23"]


def test_required_extra_field_empty_does_not_count_as_coverage(
    readiness_fixture: ReadinessFixture,
) -> None:
    path = readiness_fixture.root / readiness_fixture.request.inputs[0].path
    path.write_text(
        path.read_text(encoding="utf-8").replace(
            "2026-07-23,QQQ,100,101,99,100,100,1000,1", "2026-07-23,QQQ,100,101,99,100,100,1000, "
        ),
        encoding="utf-8",
    )
    request = _rebind_inputs(readiness_fixture)
    request = request.model_copy(
        update={
            "consumer": request.consumer.model_copy(
                update={"price_fields": ("date", "ticker", "adj_close", "consumer_feature")}
            )
        }
    )
    result = inspect_research_input_readiness(request)
    assert "REQUIRED_FIELD_VALUE_MISSING" in _codes(result)


def test_wrong_input_path_is_not_matched_by_filename(readiness_fixture: ReadinessFixture) -> None:
    first, *others = readiness_fixture.request.inputs
    duplicate = readiness_fixture.root / "data/raw/other.csv"
    duplicate.write_bytes((readiness_fixture.root / first.path).read_bytes())
    request = readiness_fixture.request.model_copy(
        update={
            "inputs": (_binding(readiness_fixture.root, "data/raw/other.csv", "prices"), *others)
        }
    )
    result = inspect_research_input_readiness(request)
    assert "RECEIPT_INPUT_MISMATCH" in _codes(result)


def test_wrong_row_count_is_not_just_displayed(readiness_fixture: ReadinessFixture) -> None:
    first, *others = readiness_fixture.request.inputs
    request = readiness_fixture.request.model_copy(
        update={"inputs": (first.model_copy(update={"row_count": first.row_count + 1}), *others)}
    )
    assert "INPUT_ROW_COUNT_MISMATCH" in _codes(inspect_research_input_readiness(request))


def test_path_escape_is_blocked_before_read(readiness_fixture: ReadinessFixture) -> None:
    first, *others = readiness_fixture.request.inputs
    request = readiness_fixture.request.model_copy(
        update={"inputs": (first.model_copy(update={"path": "../outside.csv"}), *others)}
    )
    assert "INPUT_OUTSIDE_EXECUTION_ROOT" in _codes(inspect_research_input_readiness(request))


def test_symlink_input_is_blocked(readiness_fixture: ReadinessFixture) -> None:
    path = readiness_fixture.root / "data/raw/link.csv"
    try:
        path.symlink_to(readiness_fixture.root / readiness_fixture.request.inputs[0].path)
    except OSError as exc:
        pytest.skip(f"native symlink privilege unavailable: {exc}")
    first, *others = readiness_fixture.request.inputs
    request = readiness_fixture.request.model_copy(
        update={"inputs": (first.model_copy(update={"path": "data/raw/link.csv"}), *others)}
    )
    result = inspect_research_input_readiness(request)
    assert result["status"] == "NOT_READY"
    assert result["canonical_receipt_verification"] != "PASS"


def test_plan_rejects_duplicate_dependencies_and_unknown_fields(
    readiness_fixture: ReadinessFixture,
) -> None:
    payload = json.loads(readiness_fixture.request.model_dump_json())
    payload["consumer"]["price_tickers"] = ["QQQ", "QQQ"]
    with pytest.raises(ValueError, match="duplicate dependency"):
        load_research_input_readiness_request(json.dumps(payload).encode())
    payload["consumer"]["price_tickers"] = ["QQQ"]
    payload["execute"] = True
    with pytest.raises(ValueError, match="extra"):
        load_research_input_readiness_request(json.dumps(payload).encode())


@pytest.mark.parametrize("raw", [b'{"x": 1, "x": 2}', b'{"x": NaN}'])
def test_strict_plan_json(raw: bytes) -> None:
    with pytest.raises(ValueError):
        load_research_input_readiness_request(raw)


@pytest.mark.parametrize("foreign_pythonpath", [False, True])
def test_stdout_cli_is_nonmutating_on_missing_receipt(
    readiness_fixture: ReadinessFixture, tmp_path: Path, foreign_pythonpath: bool
) -> None:
    request_path = tmp_path / "request.json"
    request_path.write_text(
        readiness_fixture.request.model_copy(update={"receipt": None}).model_dump_json(),
        encoding="utf-8",
    )
    pythonpath = REPO / "src"
    if foreign_pythonpath:
        pythonpath = tmp_path / "foreign"
        package = pythonpath / "ai_trading_system"
        package.mkdir(parents=True)
        (package / "__init__.py").write_text(
            "raise AssertionError('wrong checkout imported')\n", encoding="utf-8"
        )
    environment = dict(os.environ, PYTHONPATH=str(pythonpath), PYTHONDONTWRITEBYTECODE="1")
    run = subprocess.run(
        [
            sys.executable,
            str(REPO / "scripts/research_input_readiness.py"),
            "--request",
            str(request_path),
        ],
        cwd=REPO,
        env=environment,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    assert run.returncode == 1, run.stderr
    result = json.loads(run.stdout)
    assert result["status"] == "NOT_READY"
    assert "CANONICAL_RECEIPT_MISSING" in _codes(result)
    assert not result["dispatch_allowed"]
    assert Path(result["inspection_code_root"]).resolve() == REPO.resolve()


@pytest.mark.parametrize("role", ["report", "source_policy", "source_validator"])
def test_canonical_verifier_still_rejects_bound_authority_drift(
    readiness_fixture: ReadinessFixture, role: str
) -> None:
    assert readiness_fixture.request.receipt is not None
    receipt = json.loads(
        (readiness_fixture.root / readiness_fixture.request.receipt.path).read_bytes()
    )
    paths = {
        "report": receipt["report"]["path"],
        "source_policy": readiness_fixture.request.policy.path,
        "source_validator": "src/ai_trading_system/data/quality.py",
    }
    target = readiness_fixture.root / paths[role]
    target.write_bytes(target.read_bytes() + b"\n")
    result = inspect_research_input_readiness(readiness_fixture.request)
    assert result["status"] == "NOT_READY"
    assert result["canonical_receipt_verification"] != "PASS"


def test_required_instrument_absent_and_missing_fields_are_explicit(
    readiness_fixture: ReadinessFixture,
) -> None:
    request = readiness_fixture.request.model_copy(
        update={
            "expected_price_tickers": ("QQQ", "SPY", "TQQQ"),
            "consumer": readiness_fixture.request.consumer.model_copy(
                update={"price_tickers": ("TQQQ",), "price_fields": ("date", "ticker", "adj_close")}
            ),
        }
    )
    result = inspect_research_input_readiness(request)
    assert "REQUIRED_INPUT_COVERAGE_MISSING" in _codes(result)
    request = readiness_fixture.request.model_copy(
        update={
            "consumer": readiness_fixture.request.consumer.model_copy(
                update={"price_fields": ("date", "ticker", "absent_feature")}
            )
        }
    )
    assert "REQUIRED_FIELDS_MISSING" in _codes(inspect_research_input_readiness(request))


def test_post_verification_drift_remains_not_ready(
    readiness_fixture: ReadinessFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = readiness.verify_data_quality_execution_receipt

    def verify_then_mutate(*args: Any, **kwargs: Any) -> Any:
        verified = original(*args, **kwargs)
        rates = readiness_fixture.root / readiness_fixture.request.inputs[1].path
        rates.write_bytes(rates.read_bytes() + b"\n")
        return verified

    monkeypatch.setattr(readiness, "verify_data_quality_execution_receipt", verify_then_mutate)
    result = inspect_research_input_readiness(readiness_fixture.request)
    assert "INPUT_BINDING_MISMATCH" in _codes(result)
    assert result["canonical_receipt_verification"] != "PASS"
    assert result["status"] == "NOT_READY"


def test_root_reparse_is_rejected_before_contained_reads(
    readiness_fixture: ReadinessFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = Path.lstat

    def root_metadata(path: Path) -> Any:
        metadata = original(path)
        if path == readiness_fixture.root:
            return SimpleNamespace(st_mode=metadata.st_mode, st_file_attributes=0x400)
        return metadata

    monkeypatch.setattr(Path, "lstat", root_metadata)
    monkeypatch.setattr(readiness, "read_contained_artifact_bytes", _forbidden)
    result = inspect_research_input_readiness(readiness_fixture.request)
    assert _codes(result) == {"ROOT_REPARSE_FORBIDDEN"}


def test_calendar_special_closure_is_not_an_artificial_price_gap(
    readiness_fixture: ReadinessFixture,
) -> None:
    prices = readiness_fixture.root / readiness_fixture.request.inputs[0].path
    prices.write_text(
        "date,ticker,open,high,low,close,adj_close,volume\n"
        "2025-01-08,QQQ,100,101,99,100,100,1000\n"
        "2025-01-10,QQQ,100,101,99,100,100,1000\n",
        encoding="utf-8",
    )
    request = _rebind_inputs(readiness_fixture).model_copy(
        update={
            "requested_start": date(2025, 1, 8),
            "requested_end": date(2025, 1, 10),
            "as_of": date(2025, 1, 10),
            "receipt": None,
            "consumer": readiness_fixture.request.consumer.model_copy(update={"rate_series": ()}),
        }
    )
    result = inspect_research_input_readiness(request)
    assert _codes(result) == {"CANONICAL_RECEIPT_MISSING"}
    assert result["required_input_coverage"][0]["missing_price_sessions"] == []


@pytest.mark.parametrize("field,value", [("as_of", 1), ("requested_end", True)])
def test_plan_dates_do_not_coerce_numbers(
    readiness_fixture: ReadinessFixture, field: str, value: object
) -> None:
    payload = json.loads(readiness_fixture.request.model_dump_json())
    payload[field] = value
    with pytest.raises(ValueError, match="explicit ISO dates"):
        load_research_input_readiness_request(json.dumps(payload).encode())
