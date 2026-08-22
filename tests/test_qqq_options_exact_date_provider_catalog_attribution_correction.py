from __future__ import annotations

import hashlib
import json
import sys
import types
from collections.abc import Iterator
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from ai_trading_system.qqq_options_research import (
    exact_date_provider_catalog_attribution_correction as proposal_v1,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_NAMES = (
    "main.py",
    "owner_decision_request.md",
    "package_manifest.json",
    "proposal.json",
    "run_scope.json",
)


class _FakeOptionUniverse:
    def __init__(
        self,
        availability_date: str,
        contract_count: int,
        *,
        source_date: str | None = None,
    ) -> None:
        self.end_time = datetime.fromisoformat(f"{availability_date}T16:00:00")
        self.time = (
            datetime.fromisoformat(f"{source_date}T16:00:00")
            if source_date is not None
            else self.end_time - timedelta(days=1)
        )
        self._contracts = tuple(object() for _ in range(contract_count))

    def __iter__(self) -> Iterator[object]:
        return iter(self._contracts)


class _HistoryAccessor:
    def __init__(
        self,
        records: tuple[_FakeOptionUniverse, ...] = (),
        error: Exception | None = None,
    ) -> None:
        self.records = records
        self.error = error
        self.calls: list[tuple[object, ...]] = []

    def __getitem__(self, item: object) -> _HistoryAccessor:
        del item
        return self

    def __call__(self, *args: object) -> tuple[_FakeOptionUniverse, ...]:
        self.calls.append(args)
        if self.error is not None:
            raise self.error
        return self.records


def _candidate_algorithm_class(
    built: proposal_v1.BuiltAttributionProposalPackage,
    monkeypatch: pytest.MonkeyPatch,
) -> type:
    algorithm_imports = types.ModuleType("AlgorithmImports")
    algorithm_imports.__all__ = ["QCAlgorithm", "Slice", "OptionUniverse"]
    algorithm_imports.QCAlgorithm = object
    algorithm_imports.Slice = object
    algorithm_imports.OptionUniverse = object
    monkeypatch.setitem(sys.modules, "AlgorithmImports", algorithm_imports)
    namespace: dict[str, object] = {}
    exec(compile(built.project_code_bytes, "main.py", "exec"), namespace)
    return namespace["QQQOptionsExactDateProviderCatalogAttributionCorrection"]


def _payloads(
    built: proposal_v1.BuiltAttributionProposalPackage,
) -> dict[str, bytes]:
    return {
        "main.py": built.project_code_bytes,
        "owner_decision_request.md": built.owner_decision_request_bytes,
        "package_manifest.json": built.manifest.canonical_bytes,
        "proposal.json": built.proposal.canonical_bytes,
        "run_scope.json": built.run_scope.canonical_bytes,
    }


@pytest.fixture
def sandbox_package(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[Path, proposal_v1.BuiltAttributionProposalPackage]]:
    built = proposal_v1.build_attribution_proposal_package(project_root=PROJECT_ROOT)
    package_root = tmp_path / "package"
    package_root.mkdir()
    for name, raw in _payloads(built).items():
        (package_root / name).write_bytes(raw)

    def expected_builder(
        *, project_root: Path = PROJECT_ROOT, policy_path: Path | None = None
    ) -> proposal_v1.BuiltAttributionProposalPackage:
        del project_root, policy_path
        return built

    monkeypatch.setattr(proposal_v1, "build_attribution_proposal_package", expected_builder)
    yield package_root, built


def test_policy_binds_released_predecessor_and_zero_external_counters() -> None:
    loaded = proposal_v1.load_attribution_proposal_policy(project_root=PROJECT_ROOT)
    policy = loaded.policy

    assert loaded.file_sha256 == "405e09dbdc58d7037e35de4d047bf4b80f9ced7030e69df96e56c727fb1af8c9"
    assert loaded.canonical_sha256 == (
        "5abae42535973e59f5064288e091e9c9ddcfdab416bb5eb7e9a40fc321c03229"
    )
    assert policy.registration_base_repository_code_sha == (
        "fb246ab362e6942e3f4948c1e1cd9247212f9897"
    )
    assert policy.source_backtest_id == "acf111f24d09a41870f9a23e93fcbe3b"
    assert policy.expected_session_count == 1202
    assert policy.expected_never_chain_session_count == 1
    assert policy.predecessor_package_manifest_content_sha256 == (
        "3978c94ad4a5fa00ef77ae9325bec727bc20df0bc722e123916f22e821b927c1"
    )
    assert policy.predecessor_project_code_sha256 == (
        "9307d438da6ba0b46f42c590db683d383d3b272e973bdede2819166ebbf18ebe"
    )
    assert policy.provider_query_timing == "ON_END_AFTER_UNIQUE_TARGET_FINALIZATION"
    assert policy.provider_query_interval == "TARGET_DATE_TO_NEXT_CALENDAR_DATE_END_EXCLUSIVE"
    assert policy.maximum_provider_query_attempts == 1
    assert policy.source_date_field == "OPTION_UNIVERSE_END_TIME_DATE"
    assert policy.exact_source_date_match_required is True
    assert policy.cross_date_fallback_allowed is False
    assert policy.execution_attribution_terminal_separation_required is True
    assert policy.external_action_authorized is False
    assert policy.maximum_orders == policy.maximum_fills == 0


def test_build_is_deterministic_and_golden() -> None:
    first = proposal_v1.build_attribution_proposal_package(project_root=PROJECT_ROOT)
    second = proposal_v1.build_attribution_proposal_package(project_root=PROJECT_ROOT)

    assert first == second
    assert first.run_scope.content_sha256 == (
        "dc83b410fcb844e6c05193f81b6c46e10359c9cb5af0a2eb83fcf6a26d9a2019"
    )
    assert first.run_scope.canonical_sha256 == (
        "132f1f8e82d73db8b77e8dd69daced2b12a39ad6cb9d45d365ef46fdbcc60f0a"
    )
    assert first.proposal.content_sha256 == (
        "7ecfda585fb1c84b4967193b624310292bd2efac55ce22e54fa19c79101f95a7"
    )
    assert first.proposal.canonical_sha256 == (
        "82a70010a7231ffeb15de833a2926c5032b39fba389af172341ba4d7a79609dd"
    )
    assert first.proposal.project_code_lf_byte_count == 26223
    assert first.proposal.project_code_lf_sha256 == (
        "86a3560f973c7720ac1362757d08e7263845bf3c9b0db51d0690740e54ee3fe4"
    )
    assert first.manifest.content_sha256 == (
        "d2cfac9c2b66a9e3e8203537cb2ed2a9bcec5ef6a7d17c9e8d40eee41c4c8737"
    )


def test_source_time_v2_preserves_v1_and_binds_executed_package() -> None:
    built = proposal_v1.build_attribution_proposal_package(
        project_root=PROJECT_ROOT,
        policy_path=proposal_v1.SOURCE_TIME_POLICY_PATH,
    )
    policy = built.policy.policy

    assert policy.schema_version.endswith(".v2")
    assert policy.policy_version == "2.0.0"
    assert policy.source_date_field == "OPTION_UNIVERSE_TIME_DATE"
    assert policy.target_project_id == 35444189
    assert policy.predecessor_package_manifest_content_sha256 == (
        "d2cfac9c2b66a9e3e8203537cb2ed2a9bcec5ef6a7d17c9e8d40eee41c4c8737"
    )
    assert policy.predecessor_project_code_sha256 == (
        "86a3560f973c7720ac1362757d08e7263845bf3c9b0db51d0690740e54ee3fe4"
    )
    assert built.run_scope.schema_version.endswith(".v2")
    assert built.proposal.schema_version.endswith(".v2")
    assert built.manifest.schema_version.endswith(".v2")
    text = built.project_code_bytes.decode("utf-8")
    assert "source_time = option_universe.time" in text
    assert "availability_date = option_universe.end_time.date()" in text
    assert "expected_availability_date = source_time.date() + timedelta(days=1)" in text
    assert "source_date = option_universe.end_time.date().isoformat()" not in text


@pytest.mark.parametrize(
    ("record", "expected_status", "expected_contract_count"),
    (
        (
            _FakeOptionUniverse(
                "2022-08-27", 314, source_date="2022-08-26"
            ),
            "EXACT_DATE_AVAILABLE",
            314,
        ),
        (
            _FakeOptionUniverse(
                "2022-08-26", 9, source_date="2022-08-25"
            ),
            "CROSS_DATE_FALLBACK",
            0,
        ),
        (
            _FakeOptionUniverse(
                "2022-08-28", 314, source_date="2022-08-26"
            ),
            "CROSS_DATE_FALLBACK",
            0,
        ),
    ),
)
def test_source_time_v2_uses_time_and_validates_next_day_availability(
    monkeypatch: pytest.MonkeyPatch,
    record: _FakeOptionUniverse,
    expected_status: str,
    expected_contract_count: int,
) -> None:
    built = proposal_v1.build_attribution_proposal_package(
        project_root=PROJECT_ROOT,
        policy_path=proposal_v1.SOURCE_TIME_POLICY_PATH,
    )
    algorithm_class = _candidate_algorithm_class(built, monkeypatch)
    algorithm = algorithm_class()
    history = _HistoryAccessor(records=(record,))
    algorithm.history = history
    algorithm._option = object()

    result = algorithm._probe_provider_catalog("2022-08-26")

    assert len(history.calls) == 1
    assert result["provider_probe_status"] == expected_status
    assert result["exact_date_contract_count"] == expected_contract_count
    assert result["cross_date_fallback_detected"] is (
        expected_status == "CROSS_DATE_FALLBACK"
    )


def test_scope_is_exact_primary_window_and_fail_closed() -> None:
    built = proposal_v1.build_attribution_proposal_package(project_root=PROJECT_ROOT)
    scope = built.run_scope

    assert scope.requested_start.isoformat() == "2021-02-22"
    assert scope.requested_end.isoformat() == "2025-12-02"
    assert len(scope.session_ids) == 1202
    assert scope.session_ids[0].isoformat() == "2021-02-22"
    assert scope.session_ids[-1].isoformat() == "2025-12-02"
    assert scope.expected_never_chain_session_count == 1
    assert scope.maximum_provider_query_attempts == 1
    assert scope.exact_source_date_match_required is True
    assert scope.cross_date_fallback_allowed is False
    assert scope.execution_attribution_terminal_separation_required is True
    assert scope.allowed_classifications == (
        "EXACT_DATE_CATALOG_AVAILABLE_SUBSCRIPTION_MISSING",
        "EXACT_DATE_CATALOG_EMPTY",
        "NO_EXACT_DATE_PROVIDER_EVIDENCE",
        "PROVIDER_PROBE_ERROR",
        "ATTRIBUTION_INDETERMINATE",
    )
    assert scope.raw_option_rows_allowed is False
    assert scope.contract_identifiers_allowed is False
    assert scope.individual_contract_fields_allowed is False
    assert scope.logs_as_data_allowed is False
    assert scope.object_store_allowed is False
    assert scope.current_project_mutations == scope.current_cloud_backtests == 0
    assert scope.maximum_orders == scope.maximum_fills == 0


def test_candidate_code_is_count_only_zero_order_and_compiles() -> None:
    built = proposal_v1.build_attribution_proposal_package(project_root=PROJECT_ROOT)
    text = built.project_code_bytes.decode("utf-8")

    compile(text, "main.py", "exec")
    assert "QQQOptionsExactDateProviderCatalogAttributionCorrection" in text
    assert "self.history[OptionUniverse](self._option, start_time, end_time)" in text
    assert "self.option_chain(" not in text
    assert '"TRADING2537_TARGET_SESSION_DATE"' in text
    assert '"TRADING2537_CROSS_DATE_FALLBACK_DETECTED"' in text
    assert "EXACT_DATE_CATALOG_AVAILABLE_SUBSCRIPTION_MISSING" in text
    assert "NO_EXACT_DATE_PROVIDER_EVIDENCE" in text
    assert "requested_range=" in text and "evaluated_range=" in text
    assert "contract_identifiers_exported=false" in text
    assert "individual_fields_exported=false" in text
    assert "orders=0|fills=0" in text
    for prohibited in (
        "self.market_order(",
        "self.limit_order(",
        "self.set_holdings(",
        "self.object_store",
        "self.debug(",
        "self.log(",
        "requests.",
        "urllib",
        "download(",
        ".bid_price",
        ".ask_price",
        ".greeks",
        ".implied_volatility",
        ".open_interest",
        ".volume",
        ".strike_price",
    ):
        assert prohibited not in text.lower()
    assert hashlib.sha256(built.project_code_bytes).hexdigest() == (
        built.proposal.project_code_lf_sha256
    )


@pytest.mark.parametrize(
    (
        "records",
        "error",
        "expected_status",
        "expected_attribution",
        "expected_terminal",
        "expected_contract_count",
    ),
    (
        (
            (_FakeOptionUniverse("2024-12-03", 3),),
            None,
            "EXACT_DATE_AVAILABLE",
            "EXACT_DATE_CATALOG_AVAILABLE_SUBSCRIPTION_MISSING",
            "RESOLVED",
            3,
        ),
        (
            (_FakeOptionUniverse("2024-12-03", 0),),
            None,
            "EXACT_DATE_EMPTY",
            "EXACT_DATE_CATALOG_EMPTY",
            "RESOLVED",
            0,
        ),
        (
            (),
            None,
            "NO_EXACT_DATE_RECORD",
            "NO_EXACT_DATE_PROVIDER_EVIDENCE",
            "INDETERMINATE",
            0,
        ),
        (
            (_FakeOptionUniverse("2024-12-02", 9),),
            None,
            "CROSS_DATE_FALLBACK",
            "NO_EXACT_DATE_PROVIDER_EVIDENCE",
            "INDETERMINATE",
            0,
        ),
        (
            (
                _FakeOptionUniverse("2024-12-02", 9),
                _FakeOptionUniverse("2024-12-03", 3),
            ),
            None,
            "CROSS_DATE_FALLBACK",
            "NO_EXACT_DATE_PROVIDER_EVIDENCE",
            "INDETERMINATE",
            3,
        ),
        (
            (),
            RuntimeError("sensitive provider detail"),
            "ERROR",
            "PROVIDER_PROBE_ERROR",
            "ERROR",
            0,
        ),
    ),
)
def test_candidate_exact_date_probe_fails_closed_on_cross_date_fallback(
    monkeypatch: pytest.MonkeyPatch,
    records: tuple[_FakeOptionUniverse, ...],
    error: Exception | None,
    expected_status: str,
    expected_attribution: str,
    expected_terminal: str,
    expected_contract_count: int,
) -> None:
    built = proposal_v1.build_attribution_proposal_package(project_root=PROJECT_ROOT)
    algorithm_class = _candidate_algorithm_class(built, monkeypatch)
    algorithm = algorithm_class()
    history = _HistoryAccessor(records=records, error=error)
    algorithm.history = history
    algorithm._option = object()

    result = algorithm._probe_provider_catalog("2024-12-03")

    assert len(history.calls) == 1
    assert result["provider_query_attempt_count"] == 1
    assert result["provider_probe_status"] == expected_status
    assert result["attribution"] == expected_attribution
    assert result["attribution_terminal"] == expected_terminal
    assert result["exact_date_contract_count"] == expected_contract_count
    if expected_status == "CROSS_DATE_FALLBACK":
        assert result["cross_date_fallback_detected"] is True
        assert result["non_target_record_count"] > 0


def test_candidate_on_data_never_queries_provider_history(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    built = proposal_v1.build_attribution_proposal_package(project_root=PROJECT_ROOT)
    algorithm_class = _candidate_algorithm_class(built, monkeypatch)
    algorithm = algorithm_class()
    history = _HistoryAccessor(error=AssertionError("on_data must not query history"))
    algorithm.history = history
    algorithm._equity = "QQQ"
    algorithm._option = "QQQ_OPTION"
    algorithm._states = {
        "2024-12-03": {
            "slice_events": 0,
            "equity_slice_present": False,
            "subscribed_chain_events": 0,
        }
    }
    data = SimpleNamespace(
        time=datetime(2024, 12, 3, 16),
        bars={"QQQ": object()},
        option_chains={},
    )

    algorithm.on_data(data)

    assert history.calls == []
    assert algorithm._states["2024-12-03"] == {
        "slice_events": 1,
        "equity_slice_present": True,
        "subscribed_chain_events": 0,
    }


def test_candidate_end_of_algorithm_queries_only_the_unique_target_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    built = proposal_v1.build_attribution_proposal_package(project_root=PROJECT_ROOT)
    algorithm_class = _candidate_algorithm_class(built, monkeypatch)
    algorithm = algorithm_class()
    expected_sessions = algorithm.on_end_of_algorithm.__globals__["EXPECTED_SESSIONS"]
    target_date = expected_sessions[len(expected_sessions) // 2]
    algorithm._states = {
        session: {
            "slice_events": 1,
            "equity_slice_present": True,
            "subscribed_chain_events": int(session != target_date),
        }
        for session in expected_sessions
    }
    history = _HistoryAccessor(records=(_FakeOptionUniverse(target_date, 4),))
    algorithm.history = history
    algorithm._option = object()
    algorithm._order_event_count = 0
    algorithm.portfolio = SimpleNamespace(invested=False)
    statistics: dict[str, str] = {}
    algorithm.set_runtime_statistic = statistics.__setitem__

    algorithm.on_end_of_algorithm()

    assert len(history.calls) == 1
    _, start_time, end_time = history.calls[0]
    assert start_time.date().isoformat() == target_date
    assert (end_time - start_time).days == 1
    assert statistics["TRADING2537_TARGET_SESSION_POSITION"] == "INTERIOR"
    assert statistics["TRADING2537_PROVIDER_QUERY_ATTEMPT_COUNT"] == "1"
    assert statistics["TRADING2537_ATTRIBUTION"] == (
        "EXACT_DATE_CATALOG_AVAILABLE_SUBSCRIPTION_MISSING"
    )
    assert statistics["TRADING2537_ATTRIBUTION_TERMINAL"] == "RESOLVED"
    assert statistics["TRADING2537_EXECUTION_TERMINAL"].startswith("status=COMPLETE|")


def test_candidate_incomplete_session_scope_does_not_query_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    built = proposal_v1.build_attribution_proposal_package(project_root=PROJECT_ROOT)
    algorithm_class = _candidate_algorithm_class(built, monkeypatch)
    algorithm = algorithm_class()
    expected_sessions = algorithm.on_end_of_algorithm.__globals__["EXPECTED_SESSIONS"]
    target_date = expected_sessions[0]
    algorithm._states = {
        session: {
            "slice_events": int(session != target_date),
            "equity_slice_present": session != target_date,
            "subscribed_chain_events": int(session != target_date),
        }
        for session in expected_sessions
    }
    history = _HistoryAccessor(error=AssertionError("invalid scope must not query history"))
    algorithm.history = history
    algorithm._option = object()
    algorithm._order_event_count = 0
    algorithm.portfolio = SimpleNamespace(invested=False)
    statistics: dict[str, str] = {}
    algorithm.set_runtime_statistic = statistics.__setitem__

    algorithm.on_end_of_algorithm()

    assert history.calls == []
    assert statistics["TRADING2537_PROVIDER_QUERY_ATTEMPT_COUNT"] == "0"
    assert statistics["TRADING2537_ATTRIBUTION"] == "ATTRIBUTION_INDETERMINATE"
    assert statistics["TRADING2537_EXECUTION_TERMINAL"].startswith("status=INVALID|")


def test_proposal_cannot_authorize_dq_pit_or_execution() -> None:
    proposal = proposal_v1.build_attribution_proposal_package(project_root=PROJECT_ROOT).proposal

    assert proposal.proposal_status == "OWNER_FINAL_TOKEN_REQUIRED"
    assert proposal.attribution_can_change_dq_pit_status is False
    assert proposal.selection_authorized is False
    assert proposal.engine_authorized is False
    assert proposal.external_action_performed is False
    assert proposal.maximum_orders == proposal.maximum_fills == 0


def test_owner_request_is_complete_but_unsigned() -> None:
    built = proposal_v1.build_attribution_proposal_package(project_root=PROJECT_ROOT)
    text = built.owner_decision_request_bytes.decode("utf-8")

    assert "owner_decision:TRADING-2537:<YYYY-MM-DD>" in text
    assert "ordinary_pushed_main_sha:<ORDINARY_PUSHED_MAIN_SHA>" in text
    assert "authorization_expires_at_utc:<OWNER_SELECTED_EXPIRY_NOT_MORE_THAN_168_HOURS>" in text
    assert "maximum_project_mutations:1" in text
    assert "maximum_cloud_backtests:1" in text
    assert "maximum_orders:0" in text
    assert "maximum_fills:0" in text
    assert "authorization_invalidates_on_first_run_attempt:true" in text
    assert "package_manifest_content_sha256:<FINAL_TRADING_2537" in text
    assert "predecessor_package_manifest_content_sha256:3978c94a" in text
    assert "owner_decision:TRADING-2537:2026-" not in text


def test_repository_package_inventory_and_replay_are_exact() -> None:
    package_root = proposal_v1.DEFAULT_PACKAGE_ROOT
    assert tuple(sorted(path.name for path in package_root.iterdir())) == PACKAGE_NAMES

    loaded = proposal_v1.load_attribution_proposal_package(project_root=PROJECT_ROOT)
    assert loaded.manifest.content_sha256 == (
        "d2cfac9c2b66a9e3e8203537cb2ed2a9bcec5ef6a7d17c9e8d40eee41c4c8737"
    )
    for artifact in loaded.manifest.artifacts:
        raw = (package_root / artifact.relative_path).read_bytes()
        assert len(raw) == artifact.byte_count
        assert hashlib.sha256(raw).hexdigest() == artifact.sha256


def test_source_time_v2_repository_package_replays_exactly() -> None:
    package_root = proposal_v1.SOURCE_TIME_PACKAGE_ROOT
    assert tuple(sorted(path.name for path in package_root.iterdir())) == PACKAGE_NAMES

    loaded = proposal_v1.load_attribution_proposal_package(
        project_root=PROJECT_ROOT,
        policy_path=proposal_v1.SOURCE_TIME_POLICY_PATH,
    )
    assert loaded.policy.policy.source_date_field == "OPTION_UNIVERSE_TIME_DATE"
    assert loaded.manifest.content_sha256 == (
        "03d0107a8de280781b3742e3deac653cdbb92730b65b6808c16d1aed8d611bd2"
    )
    assert loaded.proposal.project_code_lf_sha256 == (
        "06b26262823c8c56ebceb4c90356086e07b050f9192e087b5e35a3dc43c5eac2"
    )
    assert (
        "authorize_single_zero_order_option_universe_time_date_attribution_correction_v2"
        in loaded.owner_decision_request_bytes.decode("utf-8")
    )
    owner_request = loaded.owner_decision_request_bytes.decode("utf-8")
    assert "target_clone_project_id:35444189" in owner_request
    assert "original_project_mutations_allowed:0" in owner_request
    assert "expected_pre_mutation_lf_byte_count:26223" in owner_request
    assert "maximum_new_clones:0" in owner_request
    assert "maximum_saves:1" in owner_request
    assert "maximum_automatic_cloud_builds:1" in owner_request


@pytest.mark.parametrize(
    ("name", "expected_error"),
    (
        ("main.py", "ATTRIBUTION_PROPOSAL_PACKAGE_ARTIFACT_DRIFT"),
        (
            "owner_decision_request.md",
            "ATTRIBUTION_PROPOSAL_PACKAGE_ARTIFACT_DRIFT",
        ),
        ("proposal.json", "ATTRIBUTION_PROPOSAL_PACKAGE_RECORD_INVALID"),
        ("run_scope.json", "ATTRIBUTION_PROPOSAL_PACKAGE_RECORD_INVALID"),
    ),
)
def test_loader_rejects_each_tampered_artifact(
    sandbox_package: tuple[Path, proposal_v1.BuiltAttributionProposalPackage],
    name: str,
    expected_error: str,
) -> None:
    package_root, _ = sandbox_package
    target = package_root / name
    target.write_bytes(target.read_bytes() + b"\n")

    with pytest.raises(
        proposal_v1.AttributionProposalError,
        match=expected_error,
    ):
        proposal_v1.load_attribution_proposal_package(
            project_root=package_root.parent, package_root=package_root
        )


def test_loader_rejects_manifest_tamper(
    sandbox_package: tuple[Path, proposal_v1.BuiltAttributionProposalPackage],
) -> None:
    package_root, _ = sandbox_package
    path = package_root / "package_manifest.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["orders"] = 1
    path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(proposal_v1.AttributionProposalError):
        proposal_v1.load_attribution_proposal_package(
            project_root=package_root.parent, package_root=package_root
        )


def test_sealed_models_reject_extra_fields() -> None:
    built = proposal_v1.build_attribution_proposal_package(project_root=PROJECT_ROOT)
    payload = json.loads(built.proposal.canonical_bytes)
    payload["unexpected"] = True

    with pytest.raises(ValueError):
        proposal_v1.AttributionProposal.model_validate(payload)
