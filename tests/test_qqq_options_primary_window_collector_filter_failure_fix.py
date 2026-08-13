from __future__ import annotations

import copy
import hashlib
import json
import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from pydantic import ValidationError

from ai_trading_system.qqq_options_research import (
    primary_window_collector_filter_failure_fix as fix_v1,
)

ROOT = Path(__file__).resolve().parents[1]
PACKAGE_NAMES = (
    "failure_receipt.json",
    "main.py",
    "owner_decision_request.md",
    "package_manifest.json",
)


def _raw_payloads(
    built: fix_v1.BuiltQCQQQOptionsCollectorFilterFailureFixPackage,
) -> dict[str, bytes]:
    return {
        "failure_receipt.json": built.failure_receipt.canonical_bytes,
        "main.py": built.corrected_project_code_bytes,
        "owner_decision_request.md": built.owner_decision_request_bytes,
        "package_manifest.json": built.manifest.canonical_bytes,
    }


@pytest.fixture
def sandbox_package(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[
    tuple[
        Path,
        Path,
        fix_v1.BuiltQCQQQOptionsCollectorFilterFailureFixPackage,
    ]
]:
    built = fix_v1.build_qc_qqq_options_collector_filter_failure_fix_package(
        project_root=ROOT
    )
    root = tmp_path / "repository"
    package_root = root / (
        fix_v1.DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_COLLECTOR_FILTER_FAILURE_FIX_PACKAGE_ROOT
    )
    package_root.mkdir(parents=True)
    for name, raw in _raw_payloads(built).items():
        (package_root / name).write_bytes(raw)

    def expected_builder(
        *, project_root: Path = ROOT
    ) -> fix_v1.BuiltQCQQQOptionsCollectorFilterFailureFixPackage:
        del project_root
        return built

    monkeypatch.setattr(
        fix_v1,
        "build_qc_qqq_options_collector_filter_failure_fix_package",
        expected_builder,
    )
    yield root, package_root, built


def test_policy_binds_frozen_2513_2517_and_failed_run_authority() -> None:
    loaded = fix_v1.load_qc_qqq_options_collector_filter_failure_fix_policy(
        project_root=ROOT
    )
    policy = loaded.policy

    assert loaded.policy_file_sha256 == (
        "28cc02d1ff88674c3efc98cf85f31e7a192d7e127421081cccde00daf054e955"
    )
    assert loaded.policy_canonical_sha256 == (
        "fa49cbb37ba6dfec0712136d3f72e64641f603e339d293f40db73957b1c85f9c"
    )
    assert policy.registration_base_repository_code_sha == (
        "2ba0e799bc5e9549eb0220f33a5c2d907de86596"
    )
    assert policy.historical_authority.project_code_lf_sha256 == (
        "d7f96fbb14e03a1f248b0a14b3ebdaa1bbeeada2d15f87fb3277b98b9c6641a6"
    )
    assert policy.failed_run.backtest_id == "9518360aeb329219cd83e78442a1d229"
    assert policy.failed_run.external_action_ledger_content_sha256 == (
        "e3979529e8bdca48e6b44a74376bbea635f02c218b3737afb265b33481e827f2"
    )
    assert policy.failed_run.run_attempt_consumption_content_sha256 == (
        "235bf53686052fabfd21089d6b0fb4dcafeb1b039375d142dfc9478ae595d498"
    )
    assert policy.failed_run.authorization_consumed is True
    assert policy.failed_run.evidence_collection_completed is False
    assert policy.failed_run.dq_pit_status == "NOT_EVALUATED"


def test_corrected_code_is_exact_one_line_failure_fix_without_thresholds() -> None:
    built = fix_v1.build_qc_qqq_options_collector_filter_failure_fix_package(
        project_root=ROOT
    )
    historical = (
        ROOT
        / "inputs/research/qqq_options/"
        "trading_2513_primary_window_derived_aggregate_run_proposal_v1/main.py"
    ).read_bytes()
    assert hashlib.sha256(historical).hexdigest() == (
        "d7f96fbb14e03a1f248b0a14b3ebdaa1bbeeada2d15f87fb3277b98b9c6641a6"
    )
    old = (
        "        option.set_filter("
        "lambda universe: universe.contracts(lambda symbols: symbols))\n"
    )
    new = (
        "        option.set_filter(\n"
        "            lambda universe: universe.contracts(\n"
        "                lambda contracts: [contract.symbol for contract in contracts]\n"
        "            )\n"
        "        )\n"
    )
    expected = historical.decode("utf-8").replace(old, new, 1).encode("utf-8")
    assert built.corrected_project_code_bytes == expected
    text = expected.decode("utf-8")
    assert old not in text
    assert text.count(new) == 1
    assert "[contract.symbol for contract in contracts]" in text
    for prohibited in (
        ".delta(",
        ".expiration(",
        ".include_weeklys(",
        ".iron_condor(",
        ".straddle(",
        "self.market_order(",
        "self.limit_order(",
        "self.object_store",
        "requests.",
        "urllib",
    ):
        assert prohibited not in text.lower()
    assert built.manifest.corrected_project_code_lf_sha256 == (
        "064a3bba10d1599a886eb52340ba843ff19ef9caf6a0da89ac5b5119c929d49d"
    )
    assert built.manifest.corrected_project_code_lf_byte_count == 26164


def test_explicit_symbol_list_selector_preserves_input_order() -> None:
    class Contract:
        def __init__(self, symbol: str) -> None:
            self.symbol = symbol

    contracts = [Contract("QQQ-A"), Contract("QQQ-B"), Contract("QQQ-C")]
    symbols = [contract.symbol for contract in contracts]
    assert symbols == ["QQQ-A", "QQQ-B", "QQQ-C"]
    assert len(symbols) == len(contracts)


def test_failure_receipt_is_canonical_consumed_failed_no_evidence_fact() -> None:
    built = fix_v1.build_qc_qqq_options_collector_filter_failure_fix_package(
        project_root=ROOT
    )
    receipt = built.failure_receipt

    assert receipt.lifecycle_status == "FAILED"
    assert receipt.scope_status == "FAIL"
    assert receipt.reason_code == "QC_RUNTIME_OPTION_FILTER_CASTING_ERROR"
    assert receipt.error_type == "InvalidCastException"
    assert "cannot be converted to IEnumerable<Symbol>" in receipt.error_message
    assert receipt.attempted_project_mutations == 1
    assert receipt.attempted_cloud_backtests == 1
    assert receipt.completed_results_downloads == 0
    assert receipt.orders == receipt.fills == 0
    assert receipt.authorization_consumed is True
    assert receipt.authorization_invalidated_for_further_runs is True
    assert receipt.evidence_collection_completed is False
    assert receipt.dq_pit_status == "NOT_EVALUATED"
    assert receipt.engine_status == "POLICY_BLOCKED_CASH_PRESERVATION"
    assert receipt.content_sha256 == (
        "eebb37bbbabe584bd38c013ef41c48fd1e8196bf7207d24c265301310c93fb07"
    )
    assert fix_v1.CollectorFilterFailedRunReceipt.from_json_bytes(
        receipt.canonical_bytes
    ) == receipt


def test_corrected_package_is_deterministic_golden_and_still_blocked() -> None:
    first = fix_v1.build_qc_qqq_options_collector_filter_failure_fix_package(
        project_root=ROOT
    )
    second = fix_v1.build_qc_qqq_options_collector_filter_failure_fix_package(
        project_root=ROOT
    )

    assert first == second
    assert first.manifest.content_sha256 == (
        "08c1b32901aa6dc67923c2432017438ac3bee7d90810d910ba2149dd2fd85931"
    )
    assert first.manifest.canonical_sha256 == (
        "a9d335f5c80a425301d25a39967b8d251d90960ef1301b55e55ac5b3380a21f7"
    )
    assert first.manifest.owner_reauthorization_status == (
        "OWNER_REAUTHORIZATION_NOT_PROVIDED"
    )
    assert first.manifest.decision == "OWNER_REAUTHORIZATION_REQUIRED"
    assert first.manifest.owner_policy_value_count == 0
    assert first.manifest.selection_authorized is False
    assert first.manifest.executable_policy_authorized is False
    assert first.manifest.engine_status == "POLICY_BLOCKED_CASH_PRESERVATION"
    assert first.manifest.maximum_orders == first.manifest.maximum_fills == 0
    assert first.manifest.external_action_performed_by_task is False


def test_owner_request_is_unsigned_v3_and_binds_failed_attempt() -> None:
    built = fix_v1.build_qc_qqq_options_collector_filter_failure_fix_package(
        project_root=ROOT
    )
    text = built.owner_decision_request_bytes.decode("utf-8")

    assert "owner_decision:TRADING-2518:<YYYY-MM-DD>" in text
    assert "collection_v3" in text
    assert "ordinary_pushed_main_sha:<ORDINARY_PUSHED_MAIN_SHA>" in text
    assert "9518360aeb329219cd83e78442a1d229" in text
    assert built.failure_receipt.content_sha256 in text
    assert built.manifest.corrected_project_code_lf_sha256 in text
    assert "maximum_cloud_backtests:1" in text
    assert "maximum_orders:0" in text
    assert "maximum_fills:0" in text
    assert "2516 v2 token 已在失败 run 中消耗" in text
    assert "POLICY_BLOCKED_CASH_PRESERVATION" in text
    assert "owner_decision:TRADING-2518:2026-" not in text
    assert "OWNER_SELECTED_EXPIRY_NOT_MORE_THAN_168_HOURS" in text


def test_repository_package_inventory_loader_and_artifact_hashes_are_exact() -> None:
    package_root = ROOT / (
        fix_v1.DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_COLLECTOR_FILTER_FAILURE_FIX_PACKAGE_ROOT
    )
    assert tuple(sorted(path.name for path in package_root.iterdir())) == PACKAGE_NAMES
    loaded = fix_v1.load_qc_qqq_options_collector_filter_failure_fix_package(
        project_root=ROOT
    )
    assert loaded.manifest.canonical_sha256 == (
        "a9d335f5c80a425301d25a39967b8d251d90960ef1301b55e55ac5b3380a21f7"
    )
    for artifact in loaded.manifest.artifacts:
        raw = (package_root / artifact.relative_path).read_bytes()
        assert hashlib.sha256(raw).hexdigest() == artifact.sha256
        assert len(raw) == artifact.byte_count


@pytest.mark.parametrize(
    "name",
    ("failure_receipt.json", "main.py", "owner_decision_request.md"),
)
def test_loader_rejects_each_tampered_artifact(
    sandbox_package: tuple[
        Path,
        Path,
        fix_v1.BuiltQCQQQOptionsCollectorFilterFailureFixPackage,
    ],
    name: str,
) -> None:
    root, package_root, _ = sandbox_package
    target = package_root / name
    target.write_bytes(target.read_bytes() + b"\n")

    with pytest.raises(
        fix_v1.QCQQQOptionsCollectorFilterFailureFixError,
        match="COLLECTOR_FILTER_FIX_PACKAGE_ADMISSION_FAILED",
    ):
        fix_v1.load_qc_qqq_options_collector_filter_failure_fix_package(
            project_root=root
        )


def test_loader_rejects_manifest_tamper_and_duplicate_json(
    sandbox_package: tuple[
        Path,
        Path,
        fix_v1.BuiltQCQQQOptionsCollectorFilterFailureFixPackage,
    ],
) -> None:
    root, package_root, built = sandbox_package
    manifest = package_root / "package_manifest.json"
    payload = json.loads(manifest.read_bytes())
    payload["content_sha256"] = "0" * 64
    manifest.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(
        fix_v1.QCQQQOptionsCollectorFilterFailureFixError,
        match="semantic content SHA-256 mismatch",
    ):
        fix_v1.load_qc_qqq_options_collector_filter_failure_fix_package(
            project_root=root
        )

    manifest.write_bytes(built.manifest.canonical_bytes)
    receipt = package_root / "failure_receipt.json"
    duplicate = receipt.read_bytes().replace(
        b'  "authorization_consumed": true,\n',
        b'  "authorization_consumed": true,\n  "authorization_consumed": true,\n',
        1,
    )
    receipt.write_bytes(duplicate)
    with pytest.raises(
        fix_v1.QCQQQOptionsCollectorFilterFailureFixError,
        match="duplicate JSON key",
    ):
        fix_v1.load_qc_qqq_options_collector_filter_failure_fix_package(
            project_root=root
        )


@pytest.mark.parametrize("mutation", ("extra", "missing"))
def test_loader_rejects_nonexact_inventory(
    sandbox_package: tuple[
        Path,
        Path,
        fix_v1.BuiltQCQQQOptionsCollectorFilterFailureFixPackage,
    ],
    mutation: str,
) -> None:
    root, package_root, _ = sandbox_package
    if mutation == "extra":
        (package_root / "extra.txt").write_text("unexpected\n", encoding="utf-8")
    else:
        (package_root / "main.py").unlink()

    with pytest.raises(
        fix_v1.QCQQQOptionsCollectorFilterFailureFixError,
        match="package file inventory is not exact",
    ):
        fix_v1.load_qc_qqq_options_collector_filter_failure_fix_package(
            project_root=root
        )


def test_loader_rejects_symlink_entry_when_supported(
    sandbox_package: tuple[
        Path,
        Path,
        fix_v1.BuiltQCQQQOptionsCollectorFilterFailureFixPackage,
    ],
) -> None:
    root, package_root, _ = sandbox_package
    target = package_root / "main.py"
    original = root / "main.original.py"
    target.rename(original)
    try:
        os.symlink(original, target)
    except OSError:
        pytest.skip("symlink creation is unavailable in this Windows environment")
    try:
        with pytest.raises(fix_v1.QCQQQOptionsCollectorFilterFailureFixError):
            fix_v1.load_qc_qqq_options_collector_filter_failure_fix_package(
                project_root=root
            )
    finally:
        if target.is_symlink():
            target.unlink()
        if original.exists():
            original.rename(target)


def test_policy_model_rejects_fake_authorized_or_relaxed_caps() -> None:
    loaded = fix_v1.load_qc_qqq_options_collector_filter_failure_fix_policy(
        project_root=ROOT
    )
    payload = loaded.policy.model_dump(mode="json")
    cases = []
    authorized = copy.deepcopy(payload)
    authorized["safety"]["owner_reauthorization_status"] = "AUTHORIZED"
    cases.append(authorized)
    order_cap = copy.deepcopy(payload)
    order_cap["maximum_orders"] = 1
    cases.append(order_cap)
    second_run = copy.deepcopy(payload)
    second_run["safety"]["second_cloud_backtest_allowed"] = True
    cases.append(second_run)
    wrong_start = copy.deepcopy(payload)
    wrong_start["requested_start"] = "2022-12-01"
    cases.append(wrong_start)

    for case in cases:
        with pytest.raises(ValidationError):
            fix_v1.QCQQQOptionsCollectorFilterFailureFixPolicy.model_validate(case)


def test_sealed_records_reject_noncanonical_or_forged_content_hash() -> None:
    built = fix_v1.build_qc_qqq_options_collector_filter_failure_fix_package(
        project_root=ROOT
    )
    receipt_payload = json.loads(built.failure_receipt.canonical_bytes)
    compact = json.dumps(receipt_payload, separators=(",", ":")).encode("utf-8")
    with pytest.raises(fix_v1.QCQQQOptionsCollectorFilterFailureFixError):
        fix_v1.CollectorFilterFailedRunReceipt.from_json_bytes(compact)

    receipt_payload["content_sha256"] = "f" * 64
    forged = (
        json.dumps(receipt_payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    ).encode("utf-8")
    with pytest.raises(
        fix_v1.QCQQQOptionsCollectorFilterFailureFixError,
        match="semantic content SHA-256 mismatch",
    ):
        fix_v1.CollectorFilterFailedRunReceipt.from_json_bytes(forged)
