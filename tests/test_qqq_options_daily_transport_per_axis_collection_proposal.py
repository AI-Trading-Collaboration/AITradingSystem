from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from pathlib import Path

import pytest

from ai_trading_system.qqq_options_research import (
    daily_transport_per_axis_collection_proposal as proposal_v1,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_NAMES = (
    "main.py",
    "owner_decision_request.md",
    "package_manifest.json",
    "proposal.json",
    "run_scope.json",
)


def _raw_payloads(
    built: proposal_v1.BuiltPerAxisCollectionProposalPackage,
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
) -> Iterator[
    tuple[Path, Path, proposal_v1.BuiltPerAxisCollectionProposalPackage]
]:
    built = proposal_v1.build_per_axis_collection_proposal_package(
        project_root=PROJECT_ROOT
    )
    root = tmp_path / "repository"
    package_root = root / (
        proposal_v1.DEFAULT_PACKAGE_ROOT.relative_to(PROJECT_ROOT)
    )
    package_root.mkdir(parents=True)
    for name, raw in _raw_payloads(built).items():
        (package_root / name).write_bytes(raw)

    def expected_builder(
        *, project_root: Path = PROJECT_ROOT
    ) -> proposal_v1.BuiltPerAxisCollectionProposalPackage:
        del project_root
        return built

    monkeypatch.setattr(
        proposal_v1, "build_per_axis_collection_proposal_package", expected_builder
    )
    yield root, package_root, built


def test_policy_loads_and_binds_exact_2528_unresolved_authority() -> None:
    loaded = proposal_v1.load_per_axis_collection_proposal_policy(
        project_root=PROJECT_ROOT
    )
    policy = loaded.policy

    assert loaded.file_sha256 == (
        "05f45abfc296cb9e622559fde0602f4274ac9a52a42cacb92b1d6cca86707cc9"
    )
    assert loaded.canonical_sha256 == (
        "417af9f94d81f83c44feb4dde3b663a7f67122abbda99ceb77bffd416c351f73"
    )
    assert policy.registration_base_repository_code_sha == (
        "4366092a2284557a659daa3bd497250ea0ce1052"
    )
    assert policy.source_diagnostic.diagnostic_content_sha256 == (
        "e8125e165f8acf6147f15fbd64701832ba6f602bbc98d69863d65ae942b8b7aa"
    )
    assert policy.source_diagnostic.diagnostic_canonical_sha256 == (
        "b2382b928a860685412add5ac091ac458d08ab9d246351a4a5a516d050eca9ac"
    )
    assert policy.source_diagnostic.root_cause_status == "UNRESOLVED"
    assert policy.source_diagnostic.reject_scope == "UNRESOLVED_COMBINATION"
    assert policy.safety.authorization_status == (
        "NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"
    )
    assert policy.safety.external_action_performed is False


def test_package_build_is_deterministic_and_golden() -> None:
    first = proposal_v1.build_per_axis_collection_proposal_package(
        project_root=PROJECT_ROOT
    )
    second = proposal_v1.build_per_axis_collection_proposal_package(
        project_root=PROJECT_ROOT
    )

    assert first == second
    assert first.manifest.canonical_sha256 == (
        "856d3fda6b5ffb6ac4d5bd56886d03c4a62868498d63fc9ae0a27a079b2f6d33"
    )
    assert first.run_scope.content_sha256 == (
        "6c10f143fa542505b4696f255303510015e6b2318f22d6ac83e1c0933a974c33"
    )
    assert first.proposal.content_sha256 == (
        "2c41024a72229245290599da58056d5b0fd31da9cce7a562e9b7fe9e411081c9"
    )
    assert first.proposal.project_code_lf_sha256 == (
        "adfc060fff3cfd840565fb000ac4a1759b6f54f847568dd46c5418912d0b1421"
    )
    assert first.proposal.project_code_lf_byte_count == 24420


def test_scope_is_exact_primary_window_per_axis_and_unexecuted() -> None:
    built = proposal_v1.build_per_axis_collection_proposal_package(
        project_root=PROJECT_ROOT
    )
    scope = built.run_scope
    proposal = built.proposal

    assert scope.requested_start.isoformat() == "2021-02-22"
    assert scope.requested_end.isoformat() == "2025-12-02"
    assert len(scope.session_ids) == 1202
    assert scope.session_ids[0].isoformat() == "2021-02-22"
    assert scope.session_ids[-1].isoformat() == "2025-12-02"
    assert scope.axes == tuple(proposal_v1.Axis)
    assert scope.statuses == tuple(proposal_v1.AxisStatus)
    assert len(proposal.axis_output_keys) == 32
    assert proposal.reason_code_contract == (
        "AXIS_PRESENT_VALID_VALUE_OBSERVED",
        "AXIS_MISSING_NO_VALUE_OBSERVED",
        "AXIS_INVALID_VALUE_OBSERVED_BUT_REJECTED",
        "AXIS_NOT_EVALUATED_CHAIN_ABSENT_OR_SESSION_NOT_OBSERVED",
    )
    assert scope.maximum_project_mutations == scope.maximum_cloud_backtests == 1
    assert scope.maximum_orders == scope.maximum_fills == 0
    assert scope.raw_option_rows_allowed is False
    assert scope.individual_contract_values_allowed is False
    assert scope.log_data_carrier_allowed is False
    assert scope.object_store_allowed is False
    assert scope.api_cli_http_allowed is False
    assert scope.external_action_performed is False
    assert proposal.owner_policy_value_count == 0
    assert proposal.executable_policy_authorized is False
    assert proposal.selection_authorized is False
    assert proposal.engine_status == "POLICY_BLOCKED_CASH_PRESERVATION"


def test_candidate_project_code_is_aggregate_only_zero_order_and_compiles() -> None:
    built = proposal_v1.build_per_axis_collection_proposal_package(
        project_root=PROJECT_ROOT
    )
    text = built.project_code_bytes.decode("utf-8")

    compile(text, "main.py", "exec")
    assert "QQQOptionsDailyTransportPerAxisAggregateCollector" in text
    assert "TRADING2529_OPTION_CHAIN_PRESENCE_PRESENT_SESSIONS" not in text
    assert '"TRADING2529_" + axis + "_" + status + "_SESSIONS"' in text
    assert "PER_AXIS_SESSION_COUNT_AGGREGATES_ONLY" not in text
    assert "raw_rows=false" in text
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
    ):
        assert prohibited not in text.lower()
    assert hashlib.sha256(built.project_code_bytes).hexdigest() == (
        built.proposal.project_code_lf_sha256
    )


def test_owner_request_is_complete_but_unsigned() -> None:
    built = proposal_v1.build_per_axis_collection_proposal_package(
        project_root=PROJECT_ROOT
    )
    text = built.owner_decision_request_bytes.decode("utf-8")

    assert "owner_decision:TRADING-2529:<YYYY-MM-DD>" in text
    assert "ordinary_pushed_main_sha:<ORDINARY_PUSHED_MAIN_SHA>" in text
    assert "authorization_expires_at_utc:<OWNER_SELECTED_EXPIRY_NOT_MORE_THAN_168_HOURS>" in text
    assert "maximum_project_mutations:1" in text
    assert "maximum_cloud_backtests:1" in text
    assert "maximum_orders:0" in text
    assert "maximum_fills:0" in text
    assert "authorization_invalidates_on_first_run_attempt:true" in text
    assert "当前 `external_action=none`" in text
    assert "OWNER_FINAL_TOKEN_REQUIRED" in text
    assert "owner_decision:TRADING-2529:2026-" not in text


def test_repository_package_inventory_and_loader_are_exact() -> None:
    package_root = proposal_v1.DEFAULT_PACKAGE_ROOT
    assert tuple(sorted(path.name for path in package_root.iterdir())) == PACKAGE_NAMES

    loaded = proposal_v1.load_per_axis_collection_proposal_package(
        project_root=PROJECT_ROOT
    )
    assert loaded.manifest.canonical_sha256 == (
        "856d3fda6b5ffb6ac4d5bd56886d03c4a62868498d63fc9ae0a27a079b2f6d33"
    )
    for artifact in loaded.manifest.artifacts:
        raw = (package_root / artifact.relative_path).read_bytes()
        assert hashlib.sha256(raw).hexdigest() == artifact.sha256
        assert len(raw) == artifact.byte_count


@pytest.mark.parametrize(
    "name", ("main.py", "owner_decision_request.md", "proposal.json", "run_scope.json")
)
def test_loader_rejects_each_tampered_artifact(
    sandbox_package: tuple[
        Path, Path, proposal_v1.BuiltPerAxisCollectionProposalPackage
    ],
    name: str,
) -> None:
    root, package_root, _ = sandbox_package
    target = package_root / name
    target.write_bytes(target.read_bytes() + b"\n")

    with pytest.raises(
        proposal_v1.PerAxisCollectionProposalError,
        match="PER_AXIS_PROPOSAL_PACKAGE_ADMISSION_FAILED",
    ):
        proposal_v1.load_per_axis_collection_proposal_package(
            package_root=package_root, project_root=root
        )


def test_loader_rejects_manifest_tamper(
    sandbox_package: tuple[
        Path, Path, proposal_v1.BuiltPerAxisCollectionProposalPackage
    ],
) -> None:
    root, package_root, _ = sandbox_package
    target = package_root / "package_manifest.json"
    payload = json.loads(target.read_text(encoding="utf-8"))
    payload["axis_count"] = 7
    target.write_text(
        json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )

    with pytest.raises(proposal_v1.PerAxisCollectionProposalError):
        proposal_v1.load_per_axis_collection_proposal_package(
            package_root=package_root, project_root=root
        )


@pytest.mark.parametrize("mutation", ("extra", "missing"))
def test_loader_rejects_nonexact_inventory(
    sandbox_package: tuple[
        Path, Path, proposal_v1.BuiltPerAxisCollectionProposalPackage
    ],
    mutation: str,
) -> None:
    root, package_root, _ = sandbox_package
    if mutation == "extra":
        (package_root / "extra.txt").write_text("unexpected\n", encoding="utf-8")
    else:
        (package_root / "main.py").unlink()

    with pytest.raises(
        proposal_v1.PerAxisCollectionProposalError,
        match="package file inventory is not exact",
    ):
        proposal_v1.load_per_axis_collection_proposal_package(
            package_root=package_root, project_root=root
        )


def test_loader_rejects_noncanonical_and_duplicate_json(
    sandbox_package: tuple[
        Path, Path, proposal_v1.BuiltPerAxisCollectionProposalPackage
    ],
) -> None:
    root, package_root, built = sandbox_package
    target = package_root / "proposal.json"
    payload = json.loads(target.read_text(encoding="utf-8"))
    target.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(proposal_v1.PerAxisCollectionProposalError):
        proposal_v1.load_per_axis_collection_proposal_package(
            package_root=package_root, project_root=root
        )

    target.write_bytes(
        built.proposal.canonical_bytes[:-2]
        + b',\n  "proposal_id": "TRADING_2529_DAILY_TRANSPORT_PER_AXIS_PROPOSAL_V1"\n}\n'
    )
    with pytest.raises(
        proposal_v1.PerAxisCollectionProposalError, match="duplicate JSON key"
    ):
        proposal_v1.load_per_axis_collection_proposal_package(
            package_root=package_root, project_root=root
        )


def test_policy_models_reject_axis_and_authorization_drift() -> None:
    loaded = proposal_v1.load_per_axis_collection_proposal_policy(
        project_root=PROJECT_ROOT
    )
    payload = loaded.policy.model_dump(mode="python")
    payload["axis_order"] = payload["axis_order"][:-1]
    with pytest.raises(ValueError, match="axis_order"):
        proposal_v1.PerAxisCollectionProposalPolicy.model_validate(payload)

    payload = loaded.policy.model_dump(mode="python")
    payload["safety"]["external_action_performed"] = True
    with pytest.raises(ValueError):
        proposal_v1.PerAxisCollectionProposalPolicy.model_validate(payload)


def test_sealed_records_reject_scope_and_action_drift() -> None:
    built = proposal_v1.build_per_axis_collection_proposal_package(
        project_root=PROJECT_ROOT
    )
    scope_payload = built.run_scope.model_dump(mode="json")
    scope_payload["maximum_orders"] = 1
    with pytest.raises(ValueError):
        proposal_v1.PerAxisCollectionRunScope.model_validate(scope_payload)

    proposal_payload = built.proposal.model_dump(mode="json")
    proposal_payload["raw_option_rows_allowed"] = True
    with pytest.raises(ValueError):
        proposal_v1.PerAxisCollectionProposal.model_validate(proposal_payload)
