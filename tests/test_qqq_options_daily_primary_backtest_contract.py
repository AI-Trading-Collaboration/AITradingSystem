from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research.contracts import (
    QQQ_OPTIONS_CONTRACT_SHA256,
    DQCheckResult,
    DQReportRecord,
    QQQOptionsSafetyBoundary,
)
from ai_trading_system.qqq_options_research.daily_primary_backtest_contract import (
    DEFAULT_QQQ_OPTIONS_DAILY_PRIMARY_BACKTEST_POLICY_PATH,
    QQQOptionsDailyPrimaryBacktestContractError,
    QQQOptionsDailyPrimaryBacktestDescriptor,
    QQQOptionsDailyPrimaryBacktestPolicy,
    QQQOptionsDailyPrimaryBacktestRequest,
    ResearchWindowRole,
    build_qqq_options_daily_primary_backtest_descriptor,
    load_qqq_options_daily_primary_backtest_descriptor,
    load_qqq_options_daily_primary_backtest_policy,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

REPOSITORY_CODE_SHA = "2064a2e1855229f7260c725f8287174dc09b63f3"
SOURCE_ID = "qqq.options.daily.fixture"
SOURCE_CHECKSUM = "a" * 64
SESSIONS = (
    date(2021, 2, 22),
    date(2021, 2, 23),
    date(2021, 2, 24),
    date(2021, 2, 25),
    date(2021, 2, 26),
)
REQUIRED_CHECK_IDS = (
    "cache_identity",
    "chain_presence",
    "engine_identity",
    "evidence_identity",
    "exchange_calendar_identity",
    "fill_forward_ambiguity",
    "local_cache_dq_scope_separation",
    "open_interest_freshness",
    "order_fill_chronology",
    "prior_day_model_freshness",
    "provider_raw_checksum",
    "quote_freshness",
    "quote_integrity",
    "signal_selection_chronology",
    "symbol_mapping_identity",
)


def _safety() -> QQQOptionsSafetyBoundary:
    return QQQOptionsSafetyBoundary(
        research_only=True,
        promotion_allowed=False,
        paper_shadow_allowed=False,
        production_allowed=False,
        raw_options_data_export_allowed=False,
        strategy_execution_allowed=False,
        bounded_cloud_pilot_authorized=False,
        production_effect="none",
        broker_action="none",
    )


def _dq_report(
    *,
    dq_status: str = "PASS",
    pit_status: str = "PASS",
    scope: str = "qqq_options_event_dq_pit_identity",
    policy_sha256: str = ("1e0128c40d9b125f5ce7d2a264f8308eb6885af70ebbdfe2184fed9af85b2358"),
    repository_code_sha: str = REPOSITORY_CODE_SHA,
    requested_start: date = date(2021, 2, 22),
    requested_end: date = date(2021, 2, 26),
    generated_at_utc: datetime = datetime(2021, 2, 27, 1, tzinfo=UTC),
) -> DQReportRecord:
    checks = tuple(
        DQCheckResult(
            check_id=check_id,
            status="PASS" if dq_status == "PASS" else "FAIL",
            reason_code=None if dq_status == "PASS" else "TEST_SEMANTIC_FAILURE",
            observed_at_utc=generated_at_utc,
        )
        for check_id in REQUIRED_CHECK_IDS
    )
    return DQReportRecord.seal(
        schema_name="dq_report",
        schema_version="1.0.0",
        run_id="daily_contract_fixture",
        record_id="daily_contract_dq_report",
        created_at_utc=generated_at_utc,
        producer_version="trading2499.fixture.v1",
        repository_code_sha=repository_code_sha,
        policy_id="qqq_options_dq_pit_identity_v1",
        policy_version="1.0.0",
        policy_sha256=policy_sha256,
        contract_schema_sha256=QQQ_OPTIONS_CONTRACT_SHA256,
        source_ids=(SOURCE_ID,),
        source_checksums=(SOURCE_CHECKSUM,),
        requested_start=requested_start,
        requested_end=requested_end,
        evaluated_start=requested_start,
        evaluated_end=requested_end,
        storage_timezone="UTC",
        exchange_timezone="America/New_York",
        dq_status=dq_status,
        pit_status=pit_status,
        export_classification="EXPORT_ALLOWED_DERIVED",
        lineage_id="daily_contract_lineage",
        safety=_safety(),
        scope=scope,
        report_version="1.0.0",
        generated_at_utc=generated_at_utc,
        checks=checks,
    )


def _request(
    *,
    report: DQReportRecord | None = None,
    sessions: tuple[date, ...] = SESSIONS,
    role: ResearchWindowRole = "PRIMARY",
    reviewed_role_authority_id: str | None = None,
    dq_caveat: str | None = None,
    requested_start: date = date(2021, 2, 22),
    requested_end: date = date(2021, 2, 26),
    evaluated_start: date = date(2021, 2, 22),
    evaluated_end: date = date(2021, 2, 26),
    created_at_utc: datetime = datetime(2026, 8, 8, 6, tzinfo=UTC),
) -> QQQOptionsDailyPrimaryBacktestRequest:
    report = report or _dq_report(
        requested_start=requested_start,
        requested_end=requested_end,
    )
    return QQQOptionsDailyPrimaryBacktestRequest(
        run_id="trading2499_fixture",
        created_at_utc=created_at_utc,
        repository_code_sha=REPOSITORY_CODE_SHA,
        research_window_role=role,
        reviewed_role_authority_id=reviewed_role_authority_id,
        dq_caveat=dq_caveat,
        ticker="QQQ",
        underlying_resolution="DAILY",
        option_resolution="DAILY",
        signal_resolution="DAILY",
        normalization="RAW",
        exchange_calendar="XNYS",
        requested_start=requested_start,
        requested_end=requested_end,
        evaluated_start=evaluated_start,
        evaluated_end=evaluated_end,
        evaluated_sessions=sessions,
        source_id=SOURCE_ID,
        source_checksum=SOURCE_CHECKSUM,
        dq_report_bytes=report.canonical_bytes,
        dq_report_file_sha256=hashlib.sha256(report.canonical_bytes).hexdigest(),
    )


def _descriptor() -> QQQOptionsDailyPrimaryBacktestDescriptor:
    return build_qqq_options_daily_primary_backtest_descriptor(_request())


def test_tracked_policy_binds_reviewed_daily_only_authority() -> None:
    loaded = load_qqq_options_daily_primary_backtest_policy()
    policy = loaded.policy

    assert loaded.policy_path == (
        PROJECT_ROOT / DEFAULT_QQQ_OPTIONS_DAILY_PRIMARY_BACKTEST_POLICY_PATH
    )
    assert loaded.policy_sha256 == (
        "4a060600ef9d532e75449a09628a54b84c9b68eca41989e1e4ed18de54b3109a"
    )
    assert policy.daily_engineering_authorized is True
    assert policy.backtest_execution_authorized is False
    assert policy.primary_research_start == date(2021, 2, 22)
    assert policy.legacy_non_default_start == date(2022, 12, 1)
    assert policy.legacy_non_default_start_is_default is False
    assert policy.approved_non_primary_authorities == ()
    assert policy.criteria.mode == "UNRESOLVED"
    assert loaded.capability_review.review.successor_scope == "DAILY_ENGINEERING_ONLY"


def test_policy_rejects_hash_or_activation_tamper() -> None:
    payload = safe_load_yaml_path(
        PROJECT_ROOT / DEFAULT_QQQ_OPTIONS_DAILY_PRIMARY_BACKTEST_POLICY_PATH
    )
    assert isinstance(payload, dict)
    for field_name, replacement in (
        ("shared_policy_sha256", "f" * 64),
        ("backtest_execution_authorized", True),
        ("primary_research_start", "2022-12-01"),
        ("approved_non_primary_authorities", ["forged"]),
    ):
        tampered = dict(payload)
        tampered[field_name] = replacement
        with pytest.raises(ValidationError):
            QQQOptionsDailyPrimaryBacktestPolicy.model_validate(tampered, strict=False)


def test_primary_descriptor_is_canonical_and_replayable() -> None:
    descriptor = _descriptor()

    assert (
        QQQOptionsDailyPrimaryBacktestDescriptor.from_json_bytes(descriptor.canonical_bytes)
        == descriptor
    )
    assert descriptor.research_window_role == "PRIMARY"
    assert descriptor.requested_start == descriptor.evaluated_start == date(2021, 2, 22)
    assert descriptor.evaluated_sessions == SESSIONS
    assert descriptor.dq_admission.derivation == "CANONICAL_DQ_REPORT_FACTS"
    assert descriptor.dq_admission.passed_check_ids == REQUIRED_CHECK_IDS


def test_descriptor_loader_rejects_noncanonical_and_path_escape(tmp_path: Path) -> None:
    descriptor = _descriptor()
    report = _dq_report()
    canonical = tmp_path / "descriptor.json"
    canonical.write_bytes(descriptor.canonical_bytes)
    report_path = tmp_path / "dq_report.json"
    report_path.write_bytes(report.canonical_bytes)
    assert (
        load_qqq_options_daily_primary_backtest_descriptor(
            canonical, dq_report_path=report_path, project_root=tmp_path
        )
        == descriptor
    )

    noncanonical = tmp_path / "noncanonical.json"
    noncanonical.write_bytes(
        json.dumps(descriptor.model_dump(mode="json"), sort_keys=True).encode("utf-8")
    )
    with pytest.raises(QQQOptionsDailyPrimaryBacktestContractError, match="NOT_CANONICAL"):
        load_qqq_options_daily_primary_backtest_descriptor(
            noncanonical, dq_report_path=report_path, project_root=tmp_path
        )
    with pytest.raises(QQQOptionsDailyPrimaryBacktestContractError, match="escapes"):
        load_qqq_options_daily_primary_backtest_descriptor(
            PROJECT_ROOT / "pyproject.toml",
            dq_report_path=report_path,
            project_root=tmp_path,
        )


def test_descriptor_loader_rejects_symlink(tmp_path: Path) -> None:
    report_path = tmp_path / "dq_report.json"
    report_path.write_bytes(_dq_report().canonical_bytes)
    target = tmp_path / "target.json"
    target.write_bytes(_descriptor().canonical_bytes)
    link = tmp_path / "link.json"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("symlink creation is not available on this Windows host")
    with pytest.raises(QQQOptionsDailyPrimaryBacktestContractError, match="symlink"):
        load_qqq_options_daily_primary_backtest_descriptor(
            link, dq_report_path=report_path, project_root=tmp_path
        )


def test_descriptor_loader_replays_exact_dq_report_bytes(tmp_path: Path) -> None:
    descriptor_path = tmp_path / "descriptor.json"
    descriptor_path.write_bytes(_descriptor().canonical_bytes)
    wrong_report = _dq_report(scope="wrong_scope")
    report_path = tmp_path / "wrong_dq_report.json"
    report_path.write_bytes(wrong_report.canonical_bytes)

    with pytest.raises(QQQOptionsDailyPrimaryBacktestContractError, match="scope"):
        load_qqq_options_daily_primary_backtest_descriptor(
            descriptor_path, dq_report_path=report_path, project_root=tmp_path
        )


def test_session_input_order_does_not_change_descriptor_identity() -> None:
    forward = build_qqq_options_daily_primary_backtest_descriptor(_request())
    reversed_input = build_qqq_options_daily_primary_backtest_descriptor(
        _request(sessions=tuple(reversed(SESSIONS)))
    )
    assert reversed_input == forward
    assert reversed_input.canonical_sha256 == forward.canonical_sha256


@pytest.mark.parametrize(
    "sessions",
    [
        SESSIONS[:-1],
        SESSIONS + (date(2021, 2, 27),),
    ],
)
def test_incomplete_or_non_xnys_session_inventory_fails_closed(
    sessions: tuple[date, ...],
) -> None:
    with pytest.raises(QQQOptionsDailyPrimaryBacktestContractError, match="session inventory"):
        build_qqq_options_daily_primary_backtest_descriptor(_request(sessions=sessions))


def test_duplicate_sessions_are_rejected_before_admission() -> None:
    with pytest.raises(ValidationError, match="unique"):
        _request(sessions=SESSIONS + (SESSIONS[-1],))


def test_legacy_start_is_not_a_primary_default() -> None:
    with pytest.raises(QQQOptionsDailyPrimaryBacktestContractError, match="2021-02-22"):
        build_qqq_options_daily_primary_backtest_descriptor(
            _request(
                report=_dq_report(
                    requested_start=date(2022, 12, 1),
                    requested_end=date(2022, 12, 2),
                ),
                sessions=(date(2022, 12, 1), date(2022, 12, 2)),
                requested_start=date(2022, 12, 1),
                requested_end=date(2022, 12, 2),
                evaluated_start=date(2022, 12, 1),
                evaluated_end=date(2022, 12, 2),
            )
        )


@pytest.mark.parametrize("role", ["SENSITIVITY", "PROXY", "STRESS"])
def test_unreviewed_non_primary_role_fails_closed(role: ResearchWindowRole) -> None:
    with pytest.raises(QQQOptionsDailyPrimaryBacktestContractError, match="role authority"):
        build_qqq_options_daily_primary_backtest_descriptor(
            _request(
                role=role,
                reviewed_role_authority_id="unreviewed.authority",
                dq_caveat="test caveat",
            )
        )


def test_arbitrary_bytes_and_forged_pass_declaration_cannot_enter_request() -> None:
    payload = _request().model_dump(mode="python")
    payload["dq_report_bytes"] = b"{}"
    payload["dq_report_file_sha256"] = hashlib.sha256(b"{}").hexdigest()
    payload["caller_declared_dq_status"] = "PASS"
    with pytest.raises(ValidationError):
        QQQOptionsDailyPrimaryBacktestRequest.model_validate(payload)


@pytest.mark.parametrize(
    ("dq_status", "pit_status"),
    [("FAIL", "FAIL"), ("PASS", "NOT_EVALUATED")],
)
def test_semantic_dq_fail_or_unknown_cannot_admit(dq_status: str, pit_status: str) -> None:
    report = _dq_report(dq_status=dq_status, pit_status=pit_status)
    with pytest.raises(QQQOptionsDailyPrimaryBacktestContractError, match="must both derive"):
        build_qqq_options_daily_primary_backtest_descriptor(_request(report=report))


@pytest.mark.parametrize(
    ("report", "expected"),
    [
        (_dq_report(scope="wrong_scope"), "scope or version"),
        (_dq_report(policy_sha256="b" * 64), "policy or contract"),
        (_dq_report(repository_code_sha="f" * 40), "repository code SHA"),
        (
            _dq_report(
                requested_start=date(2021, 2, 23),
                requested_end=date(2021, 2, 26),
            ),
            "range mismatch",
        ),
    ],
)
def test_dq_scope_policy_code_or_range_mismatch_fails_closed(
    report: DQReportRecord, expected: str
) -> None:
    with pytest.raises(QQQOptionsDailyPrimaryBacktestContractError, match=expected):
        build_qqq_options_daily_primary_backtest_descriptor(_request(report=report))


def test_dq_as_of_after_descriptor_creation_fails_closed() -> None:
    report = _dq_report(generated_at_utc=datetime(2026, 8, 9, tzinfo=UTC))
    with pytest.raises(QQQOptionsDailyPrimaryBacktestContractError, match="as-of"):
        build_qqq_options_daily_primary_backtest_descriptor(
            _request(report=report, created_at_utc=datetime(2026, 8, 8, tzinfo=UTC))
        )


def test_dq_source_checksum_mismatch_fails_closed() -> None:
    payload = _request().model_dump(mode="python")
    payload["source_checksum"] = "c" * 64
    request = QQQOptionsDailyPrimaryBacktestRequest.model_validate(payload)
    with pytest.raises(QQQOptionsDailyPrimaryBacktestContractError, match="source identity"):
        build_qqq_options_daily_primary_backtest_descriptor(request)


def test_file_hash_mismatch_is_rejected_before_dq_semantics() -> None:
    payload = _request().model_dump(mode="python")
    payload["dq_report_file_sha256"] = "d" * 64
    with pytest.raises(ValidationError, match="file hash"):
        QQQOptionsDailyPrimaryBacktestRequest.model_validate(payload)


def test_policy_blocked_paths_preserve_cash_and_never_pretend_lifecycle_pass() -> None:
    descriptor = _descriptor()

    assert descriptor.input_admission_status == "PASS_DQ_CONTRACT_ONLY"
    assert descriptor.selection_status == "OWNER_REVIEW_REQUIRED"
    assert descriptor.execution_status == "OWNER_REVIEW_REQUIRED"
    assert descriptor.accounting_status == "OWNER_REVIEW_REQUIRED"
    assert descriptor.lifecycle_status == "OWNER_REVIEW_REQUIRED"
    assert descriptor.disposition == "POLICY_BLOCKED_CASH_PRESERVATION"
    assert descriptor.order_count == descriptor.fill_count == 0
    assert descriptor.fee_identity == "NOT_EVALUATED_POLICY_BLOCKED"
    assert descriptor.slippage_identity == "NOT_EVALUATED_POLICY_BLOCKED"
    assert descriptor.fill_identity == "NOT_EVALUATED_POLICY_BLOCKED"


def test_chronology_and_safety_forbid_daily_close_same_bar_and_external_effects() -> None:
    descriptor = _descriptor()

    assert descriptor.chronology.signal_to_selection == "PRIOR_COMPLETED_XNYS_SESSION"
    assert descriptor.chronology.option_model_inputs == ("PRIOR_COMPLETED_XNYS_SESSION_ONLY")
    assert descriptor.chronology.daily_close_fill_allowed is False
    assert descriptor.chronology.same_bar_fill_allowed is False
    assert descriptor.chronology.lookahead_allowed is False
    assert descriptor.safety.selection_allowed is False
    assert descriptor.safety.order_submit_allowed is False
    assert descriptor.safety.fill_allowed is False
    assert descriptor.safety.cloud_run_authorized is False
    assert descriptor.safety.raw_options_data_export_allowed is False
    assert descriptor.safety.investment_interpretation_allowed is False
    assert descriptor.safety.production_effect == descriptor.safety.broker_action == "none"


def test_descriptor_content_tamper_cannot_reseal_or_replay() -> None:
    payload = _descriptor().model_dump(mode="python")
    payload["selection_status"] = "PASS"
    with pytest.raises(ValidationError):
        QQQOptionsDailyPrimaryBacktestDescriptor.model_validate(payload)
    with pytest.raises(QQQOptionsDailyPrimaryBacktestContractError, match="CALLER_SUPPLIED"):
        QQQOptionsDailyPrimaryBacktestDescriptor.seal(**payload)


def test_valid_looking_authority_and_source_identity_tamper_cannot_reseal() -> None:
    descriptor = _descriptor()
    for field_name in ("shared_policy_sha256", "source_identity_sha256"):
        payload = {
            name: getattr(descriptor, name)
            for name in type(descriptor).model_fields
            if name != "content_sha256"
        }
        payload[field_name] = "e" * 64
        with pytest.raises(ValidationError, match="authority|source identity"):
            QQQOptionsDailyPrimaryBacktestDescriptor.seal(**payload)


def test_report_observation_order_cannot_claim_future_state() -> None:
    generated = datetime(2021, 2, 27, 1, tzinfo=UTC)
    report = _dq_report(generated_at_utc=generated)
    payload = report.model_dump(mode="python", exclude={"content_sha256"})
    first = dict(payload["checks"][0])
    first["observed_at_utc"] = generated + timedelta(seconds=1)
    payload["checks"] = (
        first,
        *payload["checks"][1:],
    )
    with pytest.raises(ValidationError, match="future state"):
        DQReportRecord.seal(**payload)
