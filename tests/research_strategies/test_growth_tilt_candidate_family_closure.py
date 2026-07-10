from __future__ import annotations

import copy
import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_candidate_family_closure as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_candidate_family_closure as closure,
)

REQUIREMENT_PATH = Path(
    "docs/requirements/"
    "TRADING-2438N_Growth_Tilt_Candidate_Family_Closure_And_"
    "Contract_First_Discovery_Pivot.md"
)


def test_family_closes_with_exact_terminal_status_and_route() -> None:
    payload = _build()

    assert payload["status"] == closure.READY_STATUS
    assert payload["closure_status"] == closure.CLOSURE_STATUS
    assert payload["closure_reason_codes"] == list(closure.CLOSURE_REASON_CODES)
    assert payload["candidate_dispositions"] == closure.EXPECTED_DISPOSITIONS
    assert payload["next_route"] == closure.NEXT_ROUTE
    assert payload["strict_validation_errors"] == []


def test_prerequisite_matrix_is_an_exact_two_pass_eight_blocked_copy() -> None:
    sources = _sources()
    payload = _build(sources)
    matrix = payload["replacement_a_prerequisite_matrix"]

    assert matrix == sources["m1e"]["prerequisite_matrix"]
    assert [row["prerequisite_id"] for row in matrix["rows"]] == list(
        closure.EXPECTED_PREREQUISITE_IDS
    )
    assert matrix["pass_count"] == 2
    assert matrix["blocked_count"] == 8
    assert matrix["blocker_codes"] == list(closure.EXPECTED_BLOCKER_CODES)


def test_zero_replay_is_not_misreported_as_candidate_failure() -> None:
    payload = _build()

    assert payload["pit_candidates_tested"] == 0
    assert payload["candidate_replay_fail_count"] == 0
    assert payload["candidate_replay_blocked_count"] == 0
    assert payload["null_metrics_interpreted_as_fail"] is False
    assert payload["runtime_metrics_materialized"] is False


def test_negative_result_ledger_preserves_four_hypotheses_and_lessons() -> None:
    ledger = _build()["negative_result_ledger"]

    assert ledger["record_count"] == 4
    assert [row["hypothesis_id"] for row in ledger["records"]] == list(
        closure.EXPECTED_DISPOSITIONS
    )
    assert ledger["required_research_design_lessons"] == list(
        closure.REQUIRED_LESSONS
    )
    assert ledger["configuration_order_is_performance_rank"] is False
    assert ledger["new_baseline_behavior_allowed"] is False


def test_every_ledger_record_has_reopen_and_prohibited_reuse_contracts() -> None:
    records = _build()["negative_result_ledger"]["records"]

    for record in records:
        assert record["non_executability_reason"]
        assert record["missing_baseline_capabilities"]
        assert record["research_design_lesson"]
        assert record["prohibited_future_reuse"]
        assert record["reopen_condition"]


def test_reopen_requires_independent_baseline_evidence_and_owner_review() -> None:
    policy = _build()["reopen_policy"]

    assert policy["allowed_new_evidence_types"] == list(
        closure.REOPEN_EVIDENCE_TYPES
    )
    assert policy["candidate_independent_change"] is True
    assert policy["candidate_work_may_motivate_baseline_change"] is False
    assert policy["owner_reapproval_required"] is True
    assert policy["screening_policy_refreeze_required"] is True
    assert policy["reopen_ready"] is False


def test_closure_disables_old_m2_route_without_behavior_changes() -> None:
    payload = _build()

    assert payload["family_route_enabled"] is False
    assert payload["closed_family_m2_route_disabled"] is True
    assert payload["approved_candidate_count"] == 0
    assert payload["m2_eligible_candidate_count"] == 0
    assert payload["runtime_code_invoked"] is False
    assert payload["replay_run"] is False
    assert payload["baseline_behavior_changed"] is False
    assert payload["candidate_behavior_implemented"] is False
    assert payload["portfolio_weight_mutated"] is False


def test_prerequisite_identity_drift_blocks_closure() -> None:
    sources = _sources()
    sources["m1e"]["prerequisite_matrix"]["rows"][0]["prerequisite_id"] = (
        "drifted_id"
    )

    payload = _build(sources)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert "m1e_prerequisite_identity_or_order_mismatch" in payload[
        "strict_validation_errors"
    ]


def test_adapter_readiness_drift_blocks_closure() -> None:
    sources = _sources()
    sources["adapters"]["adapter_contract_ready_count"] = 1

    payload = _build(sources)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert "adapter_zero_ready_four_blocked_invariant_failed" in payload[
        "strict_validation_errors"
    ]


def test_runner_writes_closure_ledger_and_markdown(tmp_path: Path) -> None:
    paths = _write_source_fixtures(tmp_path)

    payload = impl.run_growth_tilt_candidate_family_closure(
        m1e_path=paths["m1e"],
        adapters_path=paths["adapters"],
        owner_resolution_path=paths["owner_resolution"],
        candidate_set_path=paths["candidate_set"],
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
        as_of_date=date(2026, 7, 10),
        strict=True,
    )

    assert payload["status"] == closure.READY_STATUS
    assert (
        tmp_path / "outputs" / "growth_tilt_candidate_family_closure.json"
    ).exists()
    assert (
        tmp_path / "outputs" / "growth_tilt_candidate_negative_result_ledger.json"
    ).exists()
    assert (tmp_path / "docs" / "growth_tilt_candidate_family_closure.md").exists()


def test_runner_strict_mode_rejects_missing_m1e_source(tmp_path: Path) -> None:
    paths = _write_source_fixtures(tmp_path)

    with pytest.raises(ValueError, match="m1e missing"):
        impl.run_growth_tilt_candidate_family_closure(
            m1e_path=tmp_path / "missing.json",
            adapters_path=paths["adapters"],
            owner_resolution_path=paths["owner_resolution"],
            candidate_set_path=paths["candidate_set"],
            output_root=tmp_path / "outputs",
            docs_root=tmp_path / "docs",
            as_of_date=date(2026, 7, 10),
            strict=True,
        )


def test_cli_closes_family_without_replay(tmp_path: Path) -> None:
    paths = _write_source_fixtures(tmp_path)
    result = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-candidate-family-close",
            "--source-m1e",
            str(paths["m1e"]),
            "--source-adapters",
            str(paths["adapters"]),
            "--owner-resolution",
            str(paths["owner_resolution"]),
            "--candidate-set",
            str(paths["candidate_set"]),
            "--output-root",
            str(tmp_path / "outputs"),
            "--docs-root",
            str(tmp_path / "docs"),
            "--as-of",
            "2026-07-10",
            "--strict",
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert closure.READY_STATUS in result.output
    assert "pit_candidates_tested=0" in result.output
    assert "m2_eligible_candidate_count=0" in result.output
    assert "replay_run=false" in result.output


def test_registry_catalog_flow_and_requirement_are_aligned() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}

    assert entries[closure.REPORT_TYPE]["production_effect"] == "none"
    assert entries[closure.REPORT_TYPE]["broker_action"] == "none"
    assert all(
        item in Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
        for item in closure.REQUIRED_CATALOG_REFERENCES
    )
    assert all(
        item in Path("docs/system_flow.md").read_text(encoding="utf-8")
        for item in closure.REQUIRED_FLOW_REFERENCES
    )
    requirement = REQUIREMENT_PATH.read_text(encoding="utf-8")
    assert "candidate-independent baseline project" in requirement
    assert "zero placeholder candidates" in requirement


def _build(sources: dict[str, Any] | None = None) -> dict[str, Any]:
    return closure.build_growth_tilt_candidate_family_closure(
        sources or _sources(),
        report_registry={"reports": [{"report_id": closure.REPORT_TYPE}]},
        artifact_catalog_text="\n".join(closure.REQUIRED_CATALOG_REFERENCES),
        system_flow_text="\n".join(closure.REQUIRED_FLOW_REFERENCES),
        requirement_text=REQUIREMENT_PATH.read_text(encoding="utf-8"),
        as_of="2026-07-10",
    )


def _sources() -> dict[str, Any]:
    return {
        "m1e": _m1e_fixture(),
        "adapters": _adapters_fixture(),
        "owner_resolution": {"schema_version": closure.EXPECTED_OWNER_SCHEMA},
        "candidate_set": {"schema_version": closure.EXPECTED_CANDIDATE_SET_SCHEMA},
    }


def _m1e_fixture() -> dict[str, Any]:
    rows = [
        {
            "prerequisite_id": prerequisite_id,
            "status": "PASS" if index < 2 else "BLOCKED",
            "blocker_code": (
                None if index < 2 else closure.EXPECTED_BLOCKER_CODES[index - 2]
            ),
        }
        for index, prerequisite_id in enumerate(closure.EXPECTED_PREREQUISITE_IDS)
    ]
    return {
        "schema_version": closure.EXPECTED_M1E_SCHEMA,
        "status": closure.EXPECTED_M1E_STATUS,
        "disposition": "KEEP_REDEFINED_BLOCKED",
        "approved_candidate_count": 0,
        "m2_eligible_candidate_count": 0,
        "prerequisite_matrix": {
            "schema_version": "growth_tilt_replacement_candidate_prerequisite_matrix.v1",
            "replacement_candidate_id": "capped_recovery_permission_overlay",
            "rows": rows,
            "prerequisite_count": 10,
            "pass_count": 2,
            "blocked_count": 8,
            "all_prerequisites_ready": False,
            "blocker_codes": list(closure.EXPECTED_BLOCKER_CODES),
        },
    }


def _adapters_fixture() -> dict[str, Any]:
    return {
        "schema_version": closure.EXPECTED_ADAPTER_SCHEMA,
        "status": closure.EXPECTED_ADAPTER_STATUS,
        "adapter_contract_ready_count": 0,
        "adapter_contract_blocked_count": 4,
    }


def _write_source_fixtures(tmp_path: Path) -> dict[str, Path]:
    sources = _sources()
    paths: dict[str, Path] = {}
    for source_id, value in sources.items():
        path = tmp_path / f"{source_id}.json"
        path.write_text(json.dumps(copy.deepcopy(value)), encoding="utf-8")
        paths[source_id] = path
    return paths
