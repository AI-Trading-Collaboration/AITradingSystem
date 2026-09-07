"""Actual committed candidate run/verify/preview, synthetic data and live fence only."""

from __future__ import annotations

import hashlib
import json
import math
from datetime import date, timedelta
from pathlib import Path

import pytest
from named_data_quality_support import (
    BOOTSTRAP_PATH,
    ROOT,
    build_actual_candidate_fixture,
    dispatch_actual_candidate_child,
)
from test_named_data_quality_candidate import (
    CURRENT_CALENDAR_PATH,
    EXPECTED_DEPENDENCIES,
    HISTORICAL_CALENDAR_PATH,
    _assert_parent,
    _fixture_tree,
    _pass_run,
    _read_receipt,
)

from ai_trading_system.contracts.data_quality_execution import DataQualityDateWindow
from ai_trading_system.contracts.named_data_quality_execution import (
    EQUAL_RISK_GUARD_RATE_SERIES,
    EQUAL_RISK_PRICE_REGISTRY_PATH,
    EQUAL_RISK_PRICE_TICKERS,
    FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_PATH,
    FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_SHA256,
)
from ai_trading_system.trading_calendar import is_us_equity_trading_day


def _fixture(tmp_path: Path, *, end: date = date(2022, 4, 18), case: str = "normal"):
    start = date(2021, 2, 22)
    days = []
    day = start
    while day <= end:
        if is_us_equity_trading_day(day):
            days.append(day)
        day += timedelta(days=1)
    prices = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    for index, observed in enumerate(days):
        for ticker in EQUAL_RISK_PRICE_TICKERS:
            amplitude = 0.0001 if ticker == "SGOV" else 0.005
            price = (
                100
                if case == "zero"
                else 100 * math.exp(0.0001 * index + amplitude * math.sin(index / 7))
            )
            prices.append(
                f"{observed},{ticker},{price},{price * 1.01},"
                f"{price * 0.99},{price},{price},1000\n"
            )
    rates = ["date,series,value\n"]
    for observed in days[:-1]:
        for series in EQUAL_RISK_GUARD_RATE_SERIES:
            value = (
                (111 if case == "rates_changed" else 110)
                if series == "DTWEXBGS"
                else (1.6 if case == "rates_changed" else 1.5)
            )
            rates.append(f"{observed},{series},{value}\n")
    return build_actual_candidate_fixture(
        tmp_path,
        prices_content="".join(prices).encode(),
        rates_content="".join(rates).encode(),
        requested_start=days[1] if case == "tail_scope" else start,
        requested_end=end,
        as_of=end,
        expected_price_tickers=(
            ("QQQ", "SGOV") if case == "two_assets" else EQUAL_RISK_PRICE_TICKERS
        ),
        expected_rate_series=EQUAL_RISK_GUARD_RATE_SERIES,
        equal_risk_price_profile=case == "old57",
        five_candidate_preview_profile=case not in {"old55", "old57"},
        expected_evaluated_window=DataQualityDateWindow(
            days[1] if case == "tail_scope" else start, days[-2]
        ),
    )


@pytest.mark.parametrize(
    "end,case",
    [
        (date(2022, 4, 1), "normal"),
        (date(2022, 4, 18), "normal"),
        (date(2022, 5, 2), "zero"),
        (date(2025, 1, 8), "normal"),
    ],
)
def test_actual_run_one_verify_zero_and_complete_read_only_preview(
    tmp_path: Path, end: date, case: str
) -> None:
    fixture = _fixture(tmp_path, end=end, case=case)
    run = _pass_run(fixture)
    receipt = _read_receipt(fixture, run)
    before = (_fixture_tree(fixture.publication_root), _fixture_tree(fixture.evidence_root))
    result = dispatch_actual_candidate_child(
        fixture, operation="verify", successful_run=run, five_candidate_preview_probe=True
    )
    assert result.returncode == 0, result.child_result
    _assert_parent(result, calls=0)
    assert all(result.child_result["probe_checks"].values())
    assert run.parent_receipt["child_pid"] != result.parent_receipt["child_pid"]
    assert result.child_result["original_dq_pid"] == receipt.execution_observation.execution_pid
    assert result.child_result["verifier_pid"] == result.parent_receipt["child_pid"]
    execution = receipt.execution
    assert execution.source_kind == "GIT_COMMIT_BYTES_COMPILED"
    assert execution.execution_root == ROOT.as_posix()
    assert execution.candidate_commit == fixture.request.candidate_commit
    assert result.child_result["execution_identity_sha256"] == execution.stable_identity_sha256
    manifest_bytes = (ROOT / FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_PATH).read_bytes()
    manifest = json.loads(manifest_bytes)
    assert execution.source_manifest_path == FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_PATH
    assert execution.source_manifest_sha256 == FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_SHA256
    assert execution.source_manifest_sha256 == hashlib.sha256(manifest_bytes).hexdigest()
    assert (
        execution.bootstrap_sha256
        == hashlib.sha256((ROOT / BOOTSTRAP_PATH).read_bytes()).hexdigest()
    )
    assert len(execution.modules) == len(manifest["modules"]) == 59
    expected_modules = {row["module_name"]: row for row in manifest["modules"]}
    assert {item.module_name for item in execution.modules} == set(expected_modules)
    for compiled in execution.modules:
        assert expected_modules[compiled.module_name] == {
            "module_name": compiled.module_name,
            "source_path": compiled.source_path,
            "is_package": compiled.is_package,
        }
        content = (ROOT / compiled.source_path).read_bytes()
        assert (compiled.sha256, compiled.size_bytes) == (
            hashlib.sha256(content).hexdigest(),
            len(content),
        )
        blob_algorithm = "sha1" if len(compiled.git_blob_id) == 40 else "sha256"
        blob = f"blob {len(content)}\0".encode("ascii") + content
        assert hashlib.new(blob_algorithm, blob).hexdigest() == compiled.git_blob_id
    assert sum(item.is_package for item in execution.modules) == 7
    dependencies = {item.relative_path: item for item in receipt.execution_dependencies}
    assert len(receipt.execution_dependencies) == 8
    assert (
        set(dependencies)
        == set(manifest["policy_dependencies"])
        == (EXPECTED_DEPENDENCIES | {EQUAL_RISK_PRICE_REGISTRY_PATH})
    )
    for path, binding in dependencies.items():
        content = (ROOT / path).read_bytes()
        assert binding.root_role == "EXECUTION"
        assert (binding.sha256, binding.size_bytes) == (
            hashlib.sha256(content).hexdigest(),
            len(content),
        )
    assert receipt.calendar.special_closure_policy_version == "1.1.0"
    assert receipt.calendar_policy == dependencies[CURRENT_CALENDAR_PATH]
    assert receipt.calendar.special_closure_policy_sha256 == receipt.calendar_policy.sha256
    assert dependencies[HISTORICAL_CALENDAR_PATH].sha256 == (
        "c0469a17a775df2dcde503c254c22db0cc7d8ad6e3a5884f2ed43c88e4dfbda4"
    )
    assert receipt.calendar_policy.sha256 != dependencies[HISTORICAL_CALENDAR_PATH].sha256
    preview = result.child_result["preview"]
    assert len(preview["candidates"]) == 5
    assert preview["data_quality_status"] == "PASS"
    assert preview["original_dq_evaluated_window"] == receipt.evaluated_window.to_dict()
    assert preview["consumed_price_window"]["end"] == end.isoformat()
    assert receipt.evaluated_window.end < end
    assert preview["registry_binding"] in [
        item.to_dict() for item in receipt.execution_dependencies
    ]
    assert preview["data_quality_receipt_id"] == receipt.receipt_id
    assert preview["read_only_plan"]["execution_allowed"] is False
    assert preview["returns_computed"] is preview["observation_created"] is False
    assert preview["pit_status"] == preview["oos_status"] == "NOT_ESTABLISHED"
    if end == date(2025, 1, 8):
        assert all(row["legacy_applied_session"] == "2025-01-10" for row in preview["candidates"])
    assert before == (_fixture_tree(fixture.publication_root), _fixture_tree(fixture.evidence_root))


def test_actual_guard_only_rates_do_not_change_targets(tmp_path: Path) -> None:
    previews = []
    for case in ("normal", "rates_changed"):
        fixture = _fixture(tmp_path / case, case=case)
        run = _pass_run(fixture)
        result = dispatch_actual_candidate_child(
            fixture, operation="verify", successful_run=run, five_candidate_preview_probe=True
        )
        assert result.returncode == 0, result.child_result
        _assert_parent(result, calls=0)
        previews.append(result.child_result["preview"])
    assert previews[0]["candidates"] == previews[1]["candidates"]
    assert previews[0]["data_quality_receipt_id"] != previews[1]["data_quality_receipt_id"]


@pytest.mark.parametrize("case", ["old55", "old57", "tail_scope", "two_assets", "short"])
def test_actual_preview_cannot_borrow_old_or_incomplete_authority(
    tmp_path: Path, case: str
) -> None:
    fixture = _fixture(
        tmp_path, case=case, end=date(2021, 2, 24) if case == "short" else date(2022, 4, 18)
    )
    run = _pass_run(fixture)
    result = dispatch_actual_candidate_child(
        fixture, operation="verify", successful_run=run, five_candidate_preview_probe=True
    )
    assert result.returncode == 2, result.child_result
    _assert_parent(result, calls=0)
    assert result.child_result["status"] == "BLOCKED"
    assert "preview" not in result.child_result
    expected_code, expected_detail = {
        "old55": ("NAMED_DQ_FIELDS_INVALID", "requires its reviewed exact source manifest"),
        "old57": ("NAMED_DQ_FIELDS_INVALID", "requires its reviewed exact source manifest"),
        "tail_scope": ("NAMED_DQ_FIELDS_INVALID", "not covered by the original canonical request"),
        "two_assets": ("NAMED_DQ_FIELDS_INVALID", "not covered by the original canonical request"),
        "short": ("NAMED_PREVIEW_LOOKBACK_INCOMPLETE", "available_prices=1, required_prices=62"),
    }[case]
    assert result.child_result["reason_code"] == expected_code
    assert expected_detail in result.child_result["detail"]
