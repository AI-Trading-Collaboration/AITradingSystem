from __future__ import annotations

import ast
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta, timezone

import pytest

from ai_trading_system.config import PROJECT_ROOT, DataQualityConfig, load_data_quality
from ai_trading_system.contracts import (
    DataQualityDateWindow,
    DataQualityEvidence,
    DataQualityExecutionContractError,
    DataQualityExecutionReceipt,
    DataQualityImplementationSourceBinding,
    DataQualityInputBinding,
    DataQualityInvocationParameter,
    DataQualityPolicyBinding,
    DataQualityReportBinding,
    DataQualityValidatorBinding,
    PolicyRole,
    VerifiedDataQualityPreflight,
)
from ai_trading_system.contracts.data_quality_execution import (
    _build_verified_data_quality_preflight,
    canonical_json_value,
)

AS_OF = date(2026, 7, 23)
STARTED_AT = datetime(2026, 7, 23, 9, 0, tzinfo=UTC)
CHECKED_AT = STARTED_AT + timedelta(seconds=1)
ENDED_AT = STARTED_AT + timedelta(seconds=2)
SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64
SHA_D = "d" * 64


def test_execution_receipt_round_trip_is_content_addressed_and_preflight_typed() -> None:
    receipt = _receipt()

    restored = DataQualityExecutionReceipt.from_json_bytes(receipt.canonical_bytes)
    preflight = _build_verified_data_quality_preflight(
        receipt=restored,
        receipt_path=_receipt_path(restored),
        receipt_sha256=restored.canonical_sha256,
        receipt_size_bytes=len(restored.canonical_bytes),
        verified_at=ENDED_AT + timedelta(seconds=1),
    )

    assert restored == receipt
    assert len(restored.receipt_id) == len("dq_execution_") + 64
    assert restored.canonical_bytes.endswith(b"\n")
    assert preflight.receipt_id == restored.receipt_id
    assert preflight.status == "PASS"
    assert preflight.as_of == AS_OF
    assert preflight.assert_strict_passed() is preflight
    assert [item.name for item in restored.invocation] == [
        "expected_price_tickers",
        "require_secondary_prices",
    ]
    assert [item.role for item in restored.inputs] == ["prices", "rates"]


def test_execution_receipt_rejects_schema_id_and_semantic_tamper() -> None:
    payload = _receipt().to_dict()
    payload["schema_version"] = "data_quality_execution_receipt.v999"
    with pytest.raises(DataQualityExecutionContractError, match="DQ_RECEIPT_SCHEMA_UNSUPPORTED"):
        DataQualityExecutionReceipt.from_dict(payload)

    payload = _receipt().to_dict()
    payload["receipt_id"] = "dq_execution_forged"
    with pytest.raises(DataQualityExecutionContractError, match="DQ_RECEIPT_ID_MISMATCH"):
        DataQualityExecutionReceipt.from_dict(payload)

    payload = deepcopy(_receipt().to_dict())
    report = payload["report"]
    assert isinstance(report, dict)
    report["sha256"] = SHA_D
    with pytest.raises(DataQualityExecutionContractError, match="DQ_REPORT_SHA_MISMATCH"):
        DataQualityExecutionReceipt.from_dict(payload)


def test_execution_receipt_rejects_unreviewed_policy_and_cutover_or_production_claims() -> None:
    with pytest.raises(DataQualityExecutionContractError, match="DQ_POLICY_NOT_REVIEWED"):
        DataQualityPolicyBinding(
            policy_id="DATA_QUALITY_CACHE_GATE",
            policy_version="data_quality_cache_gate.v1",
            status="DRAFT",
            owner="data_platform_owner",
            role=PolicyRole.DATA_QUALITY,
            path="config/data_quality.yaml",
            sha256=SHA_A,
        )

    with pytest.raises(DataQualityExecutionContractError, match="DQ_CONSUMER_NOT_AUTHORIZED"):
        replace(_receipt(), consumer_cutover_allowed=True)

    with pytest.raises(DataQualityExecutionContractError, match="PRODUCTION_EFFECT_INVALID"):
        replace(_receipt(), production_effect="enabled")


def test_report_status_and_warning_consumer_policy_fail_closed() -> None:
    with pytest.raises(DataQualityExecutionContractError, match="DQ_REPORT_STATUS_CONFLICT"):
        DataQualityReportBinding(
            path="outputs/reports/data_quality.md",
            sha256=SHA_C,
            size_bytes=123,
            status="PASS_WITH_WARNINGS",
            error_count=0,
            warning_count=0,
            info_count=0,
        )

    receipt = _receipt(status="PASS_WITH_WARNINGS", warning_count=1, issue_codes=("STALE",))
    preflight = _build_verified_data_quality_preflight(
        receipt=receipt,
        receipt_path=_receipt_path(receipt),
        receipt_sha256=receipt.canonical_sha256,
        receipt_size_bytes=len(receipt.canonical_bytes),
        verified_at=ENDED_AT + timedelta(seconds=1),
    )
    with pytest.raises(DataQualityExecutionContractError, match="DQ_WARNING_NOT_ALLOWED"):
        preflight.assert_strict_passed()

    failed = _receipt(status="FAIL", issue_codes=("DQ_INPUT_SHA_MISMATCH",))
    failed_preflight = _build_verified_data_quality_preflight(
        receipt=failed,
        receipt_path=_receipt_path(failed),
        receipt_sha256=failed.canonical_sha256,
        receipt_size_bytes=len(failed.canonical_bytes),
        verified_at=ENDED_AT + timedelta(seconds=1),
    )
    with pytest.raises(DataQualityExecutionContractError, match="DQ_EXECUTION_FAILED"):
        failed_preflight.assert_strict_passed()


def test_input_and_chronology_bindings_reject_incomplete_or_future_claims() -> None:
    with pytest.raises(DataQualityExecutionContractError, match="DQ_RECEIPT_FIELDS_INVALID"):
        DataQualityInputBinding(
            role="prices",
            path="data/raw/prices_daily.csv",
            exists=True,
            schema_id="prices_daily.v1",
            source_role="primary_market_prices",
            sha256=None,
            size_bytes=100,
            row_count=10,
        )

    receipt = _receipt()
    with pytest.raises(DataQualityExecutionContractError, match="DQ_EXECUTION_CHRONOLOGY_INVALID"):
        _build_verified_data_quality_preflight(
            receipt=receipt,
            receipt_path=_receipt_path(receipt),
            receipt_sha256=receipt.canonical_sha256,
            receipt_size_bytes=len(receipt.canonical_bytes),
            verified_at=STARTED_AT,
        )

    with pytest.raises(DataQualityExecutionContractError, match="DQ_RECEIPT_FIELDS_INVALID"):
        DataQualityDateWindow(
            datetime(2021, 2, 22, tzinfo=UTC),
            AS_OF,
        )

    non_utc = datetime(2026, 7, 23, 18, 0, tzinfo=timezone(timedelta(hours=9)))
    with pytest.raises(DataQualityExecutionContractError, match="DQ_EXECUTION_CHRONOLOGY_INVALID"):
        replace(_receipt(), started_at=non_utc)


def test_json_boundary_rejects_non_finite_duplicate_unknown_and_noncanonical_bytes() -> None:
    with pytest.raises(DataQualityExecutionContractError, match="DQ_RECEIPT_FIELDS_INVALID"):
        canonical_json_value(float("nan"))
    with pytest.raises(DataQualityExecutionContractError, match="DQ_RECEIPT_FIELDS_INVALID"):
        DataQualityInvocationParameter(name="bad", value_json="NaN")
    with pytest.raises(DataQualityExecutionContractError, match="non-string object key"):
        canonical_json_value({1: "silently-stringified-before-fix"})

    receipt = _receipt()
    duplicate = receipt.canonical_bytes.replace(
        b'{\n  "as_of":', b'{\n  "as_of": "2026-07-23",\n  "as_of":', 1
    )
    with pytest.raises(DataQualityExecutionContractError, match="duplicate JSON key"):
        DataQualityExecutionReceipt.from_json_bytes(duplicate)

    payload = receipt.to_dict()
    payload["unknown"] = True
    with pytest.raises(DataQualityExecutionContractError, match=r"unknown=\['unknown'\]"):
        DataQualityExecutionReceipt.from_dict(payload)

    with pytest.raises(DataQualityExecutionContractError, match="receipt bytes are not canonical"):
        DataQualityExecutionReceipt.from_json_bytes(receipt.canonical_bytes.rstrip())


def test_strict_text_paths_and_preflight_capability_cannot_be_forged() -> None:
    payload = deepcopy(_receipt().to_dict())
    policy = payload["policy"]
    assert isinstance(policy, dict)
    policy["owner"] = 7
    with pytest.raises(DataQualityExecutionContractError, match="DQ_RECEIPT_FIELDS_INVALID"):
        DataQualityExecutionReceipt.from_dict(payload)

    with pytest.raises(DataQualityExecutionContractError, match="repo-relative POSIX path"):
        DataQualityInputBinding(
            role="prices",
            path="..\\data\\prices.csv",
            exists=True,
            schema_id="prices_daily.v1",
            source_role="primary_market_prices",
            sha256=SHA_A,
            size_bytes=100,
            row_count=10,
        )

    receipt = _receipt()
    with pytest.raises(TypeError):
        VerifiedDataQualityPreflight(  # type: ignore[call-arg]
            receipt=receipt,
            receipt_path=_receipt_path(receipt),
            receipt_sha256=receipt.canonical_sha256,
            receipt_size_bytes=len(receipt.canonical_bytes),
            verified_at=ENDED_AT + timedelta(seconds=1),
        )

    with pytest.raises(DataQualityExecutionContractError, match="content-addressed"):
        _build_verified_data_quality_preflight(
            receipt=receipt,
            receipt_path="outputs/data_quality/executions/latest/receipt.json",
            receipt_sha256=receipt.canonical_sha256,
            receipt_size_bytes=len(receipt.canonical_bytes),
            verified_at=ENDED_AT + timedelta(seconds=1),
        )


def test_validator_entrypoint_is_bound_to_real_python_implementation_source() -> None:
    source = DataQualityImplementationSourceBinding(
        path="src/ai_trading_system/data/quality.py", sha256=SHA_A
    )
    with pytest.raises(DataQualityExecutionContractError, match="DQ_VALIDATOR_ENTRYPOINT_MISMATCH"):
        DataQualityValidatorBinding(
            validator_id="aits.validate-data",
            validator_version="quality.validate_data_cache.v1",
            entrypoint="not-an-entrypoint",
            implementation_sources=(source,),
        )
    with pytest.raises(
        DataQualityExecutionContractError, match="DQ_VALIDATOR_IMPLEMENTATION_MISSING"
    ):
        DataQualityValidatorBinding(
            validator_id="aits.validate-data",
            validator_version="quality.validate_data_cache.v1",
            entrypoint="ai_trading_system.data.quality_execution:run_canonical",
            implementation_sources=(source,),
        )
    with pytest.raises(
        DataQualityExecutionContractError, match="DQ_VALIDATOR_IMPLEMENTATION_MISSING"
    ):
        DataQualityImplementationSourceBinding(path="docs/readme.md", sha256=SHA_A)


def test_verifier_capability_factory_has_a_mechanical_import_whitelist() -> None:
    symbol = "_build_verified_data_quality_preflight"
    allowed = {
        "src/ai_trading_system/data/quality_execution.py",
        "tests/test_data_quality_execution_contract.py",
    }
    observed: set[str] = set()
    for root in (PROJECT_ROOT / "src", PROJECT_ROOT / "tests"):
        for path in root.rglob("*.py"):
            content = path.read_text(encoding="utf-8")
            if symbol not in content:
                continue
            tree = ast.parse(content, filename=str(path))
            imports_factory = any(
                (
                    isinstance(node, ast.ImportFrom)
                    and any(alias.name == symbol for alias in node.names)
                )
                or (isinstance(node, ast.Attribute) and node.attr == symbol)
                for node in ast.walk(tree)
            )
            if imports_factory:
                observed.add(path.relative_to(PROJECT_ROOT).as_posix())

    assert observed <= allowed
    assert "tests/test_data_quality_execution_contract.py" in observed

    seal_symbol = "_VERIFIED_PREFLIGHT_SEAL"
    seal_importers: set[str] = set()
    for root in (PROJECT_ROOT / "src", PROJECT_ROOT / "tests"):
        for path in root.rglob("*.py"):
            content = path.read_text(encoding="utf-8")
            if seal_symbol not in content:
                continue
            tree = ast.parse(content, filename=str(path))
            imports_seal = any(
                (
                    isinstance(node, ast.ImportFrom)
                    and any(alias.name == seal_symbol for alias in node.names)
                )
                or (isinstance(node, ast.Attribute) and node.attr == seal_symbol)
                for node in ast.walk(tree)
            )
            if imports_seal:
                seal_importers.add(path.relative_to(PROJECT_ROOT).as_posix())

    assert seal_importers == set()


def test_primary_data_quality_config_exposes_reviewed_governance_metadata() -> None:
    config = load_data_quality()

    assert config.governance is not None
    assert config.governance.policy_id == "DATA_QUALITY_CACHE_GATE"
    assert config.governance.policy_version == "data_quality_cache_gate.v1"
    assert config.governance.status == "REVIEWED"
    assert config.governance.role == "data_quality"

    payload = config.model_dump(mode="json")
    governance = payload["governance"]
    assert isinstance(governance, dict)
    governance["unknown_field"] = "forbidden"
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        DataQualityConfig.model_validate(payload)


def _receipt(
    *,
    status: str = "PASS",
    warning_count: int = 0,
    issue_codes: tuple[str, ...] = (),
) -> DataQualityExecutionReceipt:
    error_count = 1 if status == "FAIL" else 0
    passed = status != "FAIL"
    blocking_issue_codes = issue_codes if status == "FAIL" else ()
    policy = DataQualityPolicyBinding(
        policy_id="DATA_QUALITY_CACHE_GATE",
        policy_version="data_quality_cache_gate.v1",
        status="REVIEWED",
        owner="data_platform_owner",
        role=PolicyRole.DATA_QUALITY,
        path="config/data_quality.yaml",
        sha256=SHA_A,
    )
    report = DataQualityReportBinding(
        path="outputs/reports/data_quality.md",
        sha256=SHA_C,
        size_bytes=123,
        status=status,
        error_count=error_count,
        warning_count=warning_count,
        info_count=0,
        issue_codes=issue_codes,
        blocking_issue_codes=blocking_issue_codes,
    )
    evidence = DataQualityEvidence(
        contract_id="cached_market_macro_validation",
        policy_id=policy.policy_id,
        policy_version=policy.policy_version,
        status=status,
        passed=passed,
        checked_at=CHECKED_AT,
        as_of=AS_OF,
        report_path=report.path,
        report_sha256=report.sha256,
        error_count=error_count,
        warning_count=warning_count,
        checked_input_count=2,
        blocking_issues=blocking_issue_codes,
    )
    return DataQualityExecutionReceipt(
        run_id="dq-run-20260723T090000Z",
        contract_id=evidence.contract_id,
        started_at=STARTED_AT,
        ended_at=ENDED_AT,
        checked_at=CHECKED_AT,
        as_of=AS_OF,
        requested_window=DataQualityDateWindow(date(2021, 2, 22), AS_OF),
        evaluated_window=DataQualityDateWindow(date(2021, 2, 22), AS_OF),
        policy=policy,
        validator=DataQualityValidatorBinding(
            validator_id="aits.validate-data",
            validator_version="quality.validate_data_cache.v1",
            entrypoint="ai_trading_system.data.quality:validate_data_cache",
            implementation_sources=(
                DataQualityImplementationSourceBinding(
                    path="src/ai_trading_system/data/quality.py", sha256=SHA_B
                ),
                DataQualityImplementationSourceBinding(
                    path="src/ai_trading_system/data/quality_execution.py", sha256=SHA_C
                ),
            ),
        ),
        invocation=(
            DataQualityInvocationParameter.from_value("require_secondary_prices", True),
            DataQualityInvocationParameter.from_value(
                "expected_price_tickers", ["QQQ", "SGOV", "TQQQ"]
            ),
        ),
        inputs=(
            DataQualityInputBinding(
                role="rates",
                path="data/raw/rates_daily.csv",
                exists=True,
                schema_id="rates_daily.v1",
                source_role="primary_macro_rates",
                sha256=SHA_B,
                size_bytes=80,
                row_count=8,
                manifest_path="data/raw/download_manifest.csv",
                manifest_sha256=SHA_D,
                matched_source_ids=("fred",),
                matched_record_refs=("manifest_record_bbbbbbbbbbbbbbbb",),
            ),
            DataQualityInputBinding(
                role="prices",
                path="data/raw/prices_daily.csv",
                exists=True,
                schema_id="prices_daily.v1",
                source_role="primary_market_prices",
                sha256=SHA_A,
                size_bytes=100,
                row_count=10,
                manifest_path="data/raw/download_manifest.csv",
                manifest_sha256=SHA_D,
                matched_source_ids=("stooq",),
                matched_record_refs=("manifest_record_aaaaaaaaaaaaaaaa",),
            ),
        ),
        report=report,
        data_quality_evidence=evidence,
        dq_execution_provenance_verified=True,
        consumer_cutover_allowed=False,
        production_effect="none",
    )


def _receipt_path(receipt: DataQualityExecutionReceipt) -> str:
    return f"outputs/data_quality/executions/{receipt.receipt_id}/receipt.json"
