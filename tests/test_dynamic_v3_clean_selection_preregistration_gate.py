from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.contracts.research_context import (
    CoverageInterval,
    DataQualityContractRef,
    DateRange,
    EffectiveCoverage,
    MarketRegimeSpec,
    PolicyRef,
    ResearchWindowSpec,
    resolve_complete_research_context,
)
from ai_trading_system.contracts.research_lifecycle import ResearchPreregistration
from ai_trading_system.contracts.status import (
    EvidenceRole,
    PolicyRole,
    ResearchWindowRole,
)
from ai_trading_system.dynamic_v3_clean_selection_preregistration_gate import (
    MARKDOWN_FILENAME,
    build_dynamic_v3_clean_selection_preregistration_gate,
    validate_dynamic_v3_clean_selection_preregistration_gate,
    write_dynamic_v3_clean_selection_preregistration_gate,
)

AT = datetime(2024, 1, 2, 9, 0, tzinfo=UTC)


@dataclass(frozen=True)
class GateFixture:
    r2_manifest_path: Path
    preregistration_path: Path
    research_context_path: Path
    campaign_spec_path: Path
    r1_manifest_path: Path
    r1_report_path: Path
    candidate_universe_path: Path


def test_contract_only_clean_fixture_is_eligible_without_evaluator(tmp_path: Path) -> None:
    fixture = _clean_fixture(tmp_path)

    report = _build(fixture)

    assert report["eligibility_status"] == "ELIGIBLE_FOR_OWNER_AUTHORIZED_CLEAN_RUN"
    assert report["clean_run_unblocked"] is False
    assert report["unbiased_oos_claim_allowed"] is False
    assert report["candidate_expansion_allowed"] is False
    assert report["new_parameter_search_allowed"] is False
    assert report["evaluator_execution_allowed"] is False
    assert report["paper_shadow_change_allowed"] is False
    assert report["production_effect"] == "none"
    assert report["broker_action"] == "none"
    assert report["source_decision"]["source_top_n"] == 5


def test_legacy_source_top_n_is_blocked_as_contaminated(tmp_path: Path) -> None:
    fixture = _clean_fixture(tmp_path)
    manifest = _read_json(fixture.r1_manifest_path)
    manifest["top_n"] = 20
    _write_json(fixture.r1_manifest_path, manifest)
    _refresh_r2_commitment(fixture, "walk_forward_manifest", fixture.r1_manifest_path)
    source_report = _read_json(fixture.r1_report_path)
    source_report["source_candidate_selection_method"] = "full_period_source_leaderboard_top_n"
    source_report["source_selection_contamination"] = True
    _write_json(fixture.r1_report_path, source_report)
    _refresh_r2_commitment(fixture, "walk_forward_report", fixture.r1_report_path)

    report = _build(fixture)

    assert report["eligibility_status"] == "BLOCKED_CONTAMINATED_LEGACY_SOURCE"
    assert "source_origin_uncontaminated" in report["failed_checks"]


def test_result_visibility_and_freeze_chronology_fail_closed(tmp_path: Path) -> None:
    visibility = _clean_fixture(tmp_path / "visibility")
    preregistration = _read_json(visibility.preregistration_path)
    preregistration["result_visibility"] = "PARTIAL"
    preregistration.pop("preregistration_id")
    _write_json(visibility.preregistration_path, preregistration)

    visible_report = _build(visibility)

    assert visible_report["eligibility_status"] == "BLOCKED_RESULT_VISIBILITY"

    chronology = _clean_fixture(tmp_path / "chronology")
    preregistration = _read_json(chronology.preregistration_path)
    preregistration["frozen_at"] = "2025-04-01T00:00:00+00:00"
    preregistration.pop("preregistration_id")
    _write_json(chronology.preregistration_path, preregistration)
    new_preregistration = ResearchPreregistration.from_dict(preregistration)
    campaign = yaml.safe_load(chronology.campaign_spec_path.read_text(encoding="utf-8"))
    campaign["metadata"][
        "clean_selection_preregistration_id"
    ] = new_preregistration.preregistration_id
    chronology.campaign_spec_path.write_text(
        yaml.safe_dump(campaign, sort_keys=False), encoding="utf-8"
    )

    chronology_report = _build(chronology)

    assert chronology_report["eligibility_status"] == "BLOCKED_RESULT_VISIBILITY"
    assert "freeze_not_before_first_selected_result" in _details(
        chronology_report, "results_not_visible_at_freeze"
    )


def test_candidate_universe_checksum_drift_is_blocking_and_content_derived(
    tmp_path: Path,
) -> None:
    fixture = _clean_fixture(tmp_path)
    report = _build(fixture)
    output_root = tmp_path / "gate"
    write_dynamic_v3_clean_selection_preregistration_gate(report, output_root=output_root)
    fixture.candidate_universe_path.write_text(
        '{"candidate_ids":["candidate-a","tampered"]}\n', encoding="utf-8"
    )

    validation = validate_dynamic_v3_clean_selection_preregistration_gate(output_root=output_root)
    recomputed = _build(fixture)

    assert validation["status"] == "FAIL"
    assert recomputed["eligibility_status"] == "BLOCKED_SOURCE_DRIFT"


def test_train_or_test_overlap_with_locked_holdout_is_blocking(tmp_path: Path) -> None:
    fixture = _clean_fixture(tmp_path)
    report = _read_json(fixture.r1_report_path)
    report["locked_holdout"] = {"start": "2025-02-01", "end": "2025-12-31"}
    _write_json(fixture.r1_report_path, report)
    _refresh_r2_commitment(fixture, "walk_forward_report", fixture.r1_report_path)

    result = _build(fixture)

    assert result["eligibility_status"] == "BLOCKED_HOLDOUT_OVERLAP"
    assert result["window_holdout_analysis"]["overlap_count"] == 1


def test_source_top_n_tamper_after_write_invalidates_artifact(tmp_path: Path) -> None:
    fixture = _clean_fixture(tmp_path)
    report = _build(fixture)
    output_root = tmp_path / "gate"
    write_dynamic_v3_clean_selection_preregistration_gate(report, output_root=output_root)
    manifest = _read_json(fixture.r1_manifest_path)
    manifest["top_n"] = 6
    _write_json(fixture.r1_manifest_path, manifest)

    validation = validate_dynamic_v3_clean_selection_preregistration_gate(output_root=output_root)

    assert validation["status"] == "FAIL"
    assert validation["failed_check_count"] >= 1


def test_markdown_tamper_is_detected(tmp_path: Path) -> None:
    fixture = _clean_fixture(tmp_path)
    report = _build(fixture)
    output_root = tmp_path / "gate"
    write_dynamic_v3_clean_selection_preregistration_gate(report, output_root=output_root)
    (output_root / MARKDOWN_FILENAME).write_text("tampered\n", encoding="utf-8")

    validation = validate_dynamic_v3_clean_selection_preregistration_gate(output_root=output_root)

    assert validation["status"] == "FAIL"
    failed = {item["check_id"] for item in validation["checks"] if not item["passed"]}
    assert f"output_checksum:{MARKDOWN_FILENAME}" in failed
    assert "markdown_content_recomputed" in failed


def _build(fixture: GateFixture) -> dict[str, Any]:
    return build_dynamic_v3_clean_selection_preregistration_gate(
        r2_manifest_path=fixture.r2_manifest_path,
        preregistration_path=fixture.preregistration_path,
        research_context_path=fixture.research_context_path,
        campaign_spec_path=fixture.campaign_spec_path,
        generated_at=datetime(2026, 7, 20, 10, 0, tzinfo=UTC),
    )


def _clean_fixture(root: Path) -> GateFixture:
    root.mkdir(parents=True, exist_ok=True)
    source_root = root / "source"
    source_root.mkdir()
    candidate_universe_path = source_root / "candidate_universe.json"
    selection_rule_path = source_root / "selection_rule.yaml"
    candidate_universe_path.write_text('{"candidate_ids":["candidate-a"]}\n', encoding="utf-8")
    selection_rule_path.write_text(
        "schema_version: clean_selection_rule.v1\nrule: fixed_candidate_universe\n",
        encoding="utf-8",
    )
    policy_refs = _policy_refs(root)
    context = resolve_complete_research_context(
        regime=MarketRegimeSpec(
            regime_id="ai_after_chatgpt",
            anchor_date=date(2022, 11, 30),
            start_date=date(2022, 12, 1),
        ),
        window=ResearchWindowSpec(
            window_id="exact_three_asset_validated",
            start_date=date(2021, 2, 22),
            role=ResearchWindowRole.PRIMARY_VALIDATED,
            evidence_role=EvidenceRole.PRIMARY_DECISION_EVIDENCE,
        ),
        requested_range=DateRange(date(2021, 2, 22), date(2025, 2, 28)),
        actual_data_range=DateRange(date(2021, 2, 22), date(2025, 2, 28)),
        effective_feature_start=date(2021, 2, 22),
        effective_prediction_start=date(2024, 1, 2),
        actual_portfolio_start=date(2024, 1, 2),
        evaluation_range=DateRange(date(2024, 1, 2), date(2025, 2, 28)),
        as_of=date(2025, 2, 28),
        trading_calendar="XNYS",
        effective_coverage=EffectiveCoverage(
            (
                CoverageInterval(
                    source_id="fixture_prices",
                    date_range=DateRange(date(2021, 2, 22), date(2025, 2, 28)),
                ),
            )
        ),
        data_quality=DataQualityContractRef(
            contract_id="fixture_dq",
            status="PASS",
            passed=True,
            as_of=date(2025, 2, 28),
            policy_ref_id="fixture_data_quality",
        ),
        policy_refs=policy_refs,
    )
    context_path = root / "research_context.json"
    _write_json(context_path, context.to_dict())
    preregistration = ResearchPreregistration(
        hypothesis_id="dynamic-v3-clean-selection",
        hypothesis_statement="Frozen candidates are evaluated without result visibility.",
        owner="research_owner",
        baseline_id="b0-static",
        candidate_id="dynamic-v3-clean-universe-v1",
        research_context_id=context.context_id,
        selection_rule_id="clean-selection-rule-v1",
        selection_rule_sha256=_sha(selection_rule_path),
        metric_ids=("annualized_return", "max_drawdown"),
        policy_ref_ids=tuple(item.policy_id for item in policy_refs),
        validation_plan_ids=("trading-106-fold-local",),
        frozen_at=AT,
    )
    preregistration_path = root / "preregistration.json"
    _write_json(preregistration_path, preregistration.to_dict())
    campaign = _campaign(preregistration.preregistration_id, context.context_id)
    campaign_path = root / "campaign.yaml"
    campaign_path.write_text(yaml.safe_dump(campaign, sort_keys=False), encoding="utf-8")

    r1_report_path = root / "r1_walk_forward_report.json"
    r1_report = {
        "source_candidate_selection_method": "preregistered_candidate_universe",
        "source_selection_contamination": False,
        "windows": [
            {
                "window_index": 1,
                "effective_train_start": "2024-01-02",
                "effective_train_end": "2024-12-31",
                "effective_test_start": "2025-01-02",
                "effective_test_end": "2025-02-28",
            }
        ],
        "locked_holdout": {"start": "2026-01-01", "end": "2026-12-31"},
    }
    _write_json(r1_report_path, r1_report)
    r1_manifest_path = root / "r1_wf_manifest.json"
    r1_manifest = {
        "walk_forward_id": "contract-only-clean-fixture",
        "top_n": 5,
        "source_artifacts": {},
        "source_checksums": {},
        "clean_selection_contract": {
            "candidate_universe_id": "dynamic-v3-clean-universe-v1",
            "candidate_universe_path": str(candidate_universe_path),
            "candidate_universe_sha256": _sha(candidate_universe_path),
            "candidate_universe_origin": "preregistered_candidate_universe",
            "selection_rule_path": str(selection_rule_path),
            "selection_rule_sha256": _sha(selection_rule_path),
            "first_selected_result_at": "2025-03-01T00:00:00+00:00",
            "selected_after_result_visibility": False,
        },
    }
    _write_json(r1_manifest_path, r1_manifest)

    decision_path = root / "strategy_research_restart_r2_decision.json"
    _write_json(
        decision_path,
        {
            "decision_id": "r2-contract-only",
            "decision": "CONTINUE_EVIDENCE_CLOSURE",
            "status": "PASS",
            "candidate_expansion_allowed": False,
            "new_parameter_search_allowed": False,
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    r2_manifest_path = root / "strategy_research_restart_r2_manifest.json"
    _write_json(
        r2_manifest_path,
        {
            "decision_id": "r2-contract-only",
            "decision": "CONTINUE_EVIDENCE_CLOSURE",
            "walk_forward_id": "contract-only-clean-fixture",
            "input_commitments": {
                "walk_forward_manifest": _commitment(r1_manifest_path),
                "walk_forward_report": _commitment(r1_report_path),
            },
            "output_artifact_checksums": {decision_path.name: _sha(decision_path)},
        },
    )
    return GateFixture(
        r2_manifest_path=r2_manifest_path,
        preregistration_path=preregistration_path,
        research_context_path=context_path,
        campaign_spec_path=campaign_path,
        r1_manifest_path=r1_manifest_path,
        r1_report_path=r1_report_path,
        candidate_universe_path=candidate_universe_path,
    )


def _policy_refs(root: Path) -> tuple[PolicyRef, ...]:
    rows = []
    for policy_id, role in (
        ("fixture_market_regime", PolicyRole.MARKET_REGIME),
        ("fixture_research_window", PolicyRole.RESEARCH_WINDOW),
        ("fixture_data_quality", PolicyRole.DATA_QUALITY),
    ):
        path = root / f"{policy_id}.yaml"
        path.write_text(f"policy_id: {policy_id}\n", encoding="utf-8")
        rows.append(
            PolicyRef(
                policy_id=policy_id,
                role=role,
                version="v1",
                status="fixture",
                path=str(path),
                sha256=_sha(path),
            )
        )
    return tuple(rows)


def _campaign(preregistration_id: str, context_id: str) -> dict[str, Any]:
    return {
        "schema_version": "research_campaign.v1",
        "campaign_id": "dynamic-v3-clean-contract-only",
        "program_id": "dynamic-v3-clean-selection",
        "title": "Dynamic v3 clean selection contract-only fixture",
        "hypothesis": {
            "statement": "Frozen candidates retain evidence after fold-local evaluation.",
            "expected_gain": ["unbiased_oos_evidence"],
            "expected_failure_modes": ["negative_oos"],
        },
        "module_graph": {
            "baseline": "b0-static",
            "modules": [],
            "allowed_mechanisms": [],
            "forbidden_mechanisms": [],
            "allowed_interaction_order": 1,
        },
        "window_policy": {
            "development_catalog": "clean-development-v1",
            "diagnostic_catalog": "clean-test-v1",
            "holdout_catalog": "locked-holdout-v1",
            "holdout_access": "OWNER_AUTHORIZATION_REQUIRED",
        },
        "scorecard_policy": "dynamic-v3-clean-scorecard-v1",
        "owner_authorized_holdout": False,
        "safety": {
            "research_only": True,
            "manual_review_only": True,
            "official_target_weights": False,
            "paper_shadow_allowed": False,
            "broker_effect": "none",
            "order_effect": "none",
            "production_effect": "none",
        },
        "metadata": {
            "clean_selection_preregistration_id": preregistration_id,
            "research_context_id": context_id,
        },
    }


def _refresh_r2_commitment(fixture: GateFixture, name: str, path: Path) -> None:
    manifest = _read_json(fixture.r2_manifest_path)
    manifest["input_commitments"][name] = _commitment(path)
    _write_json(fixture.r2_manifest_path, manifest)


def _details(report: dict[str, Any], check_id: str) -> list[str]:
    return next(item["details"] for item in report["checks"] if item["check_id"] == check_id)


def _commitment(path: Path) -> dict[str, Any]:
    return {"path": str(path), "sha256": _sha(path), "size": path.stat().st_size}


def _sha(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
