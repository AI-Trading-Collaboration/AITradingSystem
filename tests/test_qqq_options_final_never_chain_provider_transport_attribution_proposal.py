from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from pathlib import Path

import pytest

from ai_trading_system.qqq_options_research import (
    final_never_chain_provider_transport_attribution_proposal as proposal_v1,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_NAMES = (
    "main.py",
    "owner_decision_request.md",
    "package_manifest.json",
    "proposal.json",
    "run_scope.json",
)


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
        *, project_root: Path = PROJECT_ROOT
    ) -> proposal_v1.BuiltAttributionProposalPackage:
        del project_root
        return built

    monkeypatch.setattr(proposal_v1, "build_attribution_proposal_package", expected_builder)
    yield package_root, built


def test_policy_binds_released_predecessor_and_zero_external_counters() -> None:
    loaded = proposal_v1.load_attribution_proposal_policy(project_root=PROJECT_ROOT)
    policy = loaded.policy

    assert loaded.file_sha256 == (
        "3b101cdca7c85c01b9d4a5a5fe8a51b80ab0cc4d1e768bf8a9d8a31d830d01e1"
    )
    assert loaded.canonical_sha256 == (
        "1c075a5a7cc153e730d03a138f863f5ed3736b1424a1027c2f060eb59bb443bf"
    )
    assert policy.registration_base_repository_code_sha == (
        "c290f1244bb81df789d3b95d29d894b657943ca8"
    )
    assert policy.source_backtest_id == "acf111f24d09a41870f9a23e93fcbe3b"
    assert policy.expected_session_count == 1202
    assert policy.expected_never_chain_session_count == 1
    assert policy.external_action_authorized is False
    assert policy.maximum_orders == policy.maximum_fills == 0


def test_build_is_deterministic_and_golden() -> None:
    first = proposal_v1.build_attribution_proposal_package(project_root=PROJECT_ROOT)
    second = proposal_v1.build_attribution_proposal_package(project_root=PROJECT_ROOT)

    assert first == second
    assert first.run_scope.content_sha256 == (
        "98606ee39114622ba8e1d1f14fc06119f7829bac2c326feb98076b39324f4e8c"
    )
    assert first.run_scope.canonical_sha256 == (
        "ef67fefb3313a9881861150779ddcc1eca809f5b032c98e4eb0aff7e32469748"
    )
    assert first.proposal.content_sha256 == (
        "83f19609f617d8a2ec1ec68b935a7b54558f4e2ee6ff6884a430323d111612de"
    )
    assert first.proposal.canonical_sha256 == (
        "aff23a1fc9c49dfd3a8d14a6b8cf2940d9749eceb76d060a0e895a421e06fca3"
    )
    assert first.proposal.project_code_lf_byte_count == 22533
    assert first.proposal.project_code_lf_sha256 == (
        "9307d438da6ba0b46f42c590db683d383d3b272e973bdede2819166ebbf18ebe"
    )
    assert first.manifest.content_sha256 == (
        "3978c94ad4a5fa00ef77ae9325bec727bc20df0bc722e123916f22e821b927c1"
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
    assert scope.allowed_classifications == (
        "PROVIDER_CATALOG_EMPTY_FOR_TARGET_SESSION",
        "PROVIDER_CATALOG_AVAILABLE_BUT_SUBSCRIBED_SLICE_NEVER_DELIVERED",
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
    assert "QQQOptionsFinalNeverChainProviderTransportAttribution" in text
    assert "self.option_chain(self._option)" in text
    assert '"TRADING2535_TARGET_SESSION_DATE"' in text
    assert '"TRADING2535_PROVIDER_CONTRACT_COUNT"' in text
    assert "PROVIDER_CATALOG_AVAILABLE_BUT_SUBSCRIBED_SLICE_NEVER_DELIVERED" in text
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

    assert "owner_decision:TRADING-2535:<YYYY-MM-DD>" in text
    assert "ordinary_pushed_main_sha:<ORDINARY_PUSHED_MAIN_SHA>" in text
    assert "authorization_expires_at_utc:<OWNER_SELECTED_EXPIRY_NOT_MORE_THAN_168_HOURS>" in text
    assert "maximum_project_mutations:1" in text
    assert "maximum_cloud_backtests:1" in text
    assert "maximum_orders:0" in text
    assert "maximum_fills:0" in text
    assert "authorization_invalidates_on_first_run_attempt:true" in text
    assert "package_manifest_content_sha256:<FINAL_TRADING_2535" in text
    assert "owner_decision:TRADING-2535:2026-" not in text


def test_repository_package_inventory_and_replay_are_exact() -> None:
    package_root = proposal_v1.DEFAULT_PACKAGE_ROOT
    assert tuple(sorted(path.name for path in package_root.iterdir())) == PACKAGE_NAMES

    loaded = proposal_v1.load_attribution_proposal_package(project_root=PROJECT_ROOT)
    assert loaded.manifest.content_sha256 == (
        "3978c94ad4a5fa00ef77ae9325bec727bc20df0bc722e123916f22e821b927c1"
    )
    for artifact in loaded.manifest.artifacts:
        raw = (package_root / artifact.relative_path).read_bytes()
        assert len(raw) == artifact.byte_count
        assert hashlib.sha256(raw).hexdigest() == artifact.sha256


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
