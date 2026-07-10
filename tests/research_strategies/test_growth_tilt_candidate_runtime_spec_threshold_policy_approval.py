from __future__ import annotations

import copy
import json
from datetime import date
from pathlib import Path

import pytest
from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_candidate_runtime_spec_threshold_policy_approval as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_candidate_runtime_spec_threshold_policy_approval as approval,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

CANDIDATE_IDS = [
    "recovery_reentry_speedup_guard",
    "false_risk_off_confirmation_relaxation",
    "missed_upside_reentry_accelerator",
]
TEMPLATE_PATH = Path(
    "inputs/research_reviews/growth_tilt_candidate_runtime_spec_threshold_policy_review.yaml"
)


def test_default_owner_review_template_is_precisely_blocked() -> None:
    payload = _build(_template())

    assert payload["status"] == approval.BLOCKED_STATUS
    assert payload["candidate_ids"] == CANDIDATE_IDS
    assert payload["approved_candidate_count"] == 0
    assert payload["pending_candidate_count"] == 3
    assert payload["runtime_spec_ready_count"] == 0
    assert payload["metric_contract_ready_count"] == 0
    assert payload["threshold_policy_ready_count"] == 0
    assert payload["owner_input_gap_count"] == 27
    assert payload["recommended_next_research_task"] == approval.NEXT_ROUTE_BLOCKED
    assert payload["strict_validation_errors"] == []


def test_complete_owner_approval_is_ready_for_compute_plane_binding() -> None:
    payload = _build(_complete_review())

    assert payload["status"] == approval.READY_STATUS
    assert payload["approved_candidate_count"] == 3
    assert payload["pending_candidate_count"] == 0
    assert payload["runtime_spec_ready_count"] == 3
    assert payload["metric_contract_ready_count"] == 3
    assert payload["threshold_policy_ready_count"] == 3
    assert payload["owner_input_gap_count"] == 0
    assert payload["recommended_next_research_task"] == approval.NEXT_ROUTE_READY


def test_redefine_decision_routes_to_candidate_definition_design() -> None:
    review = _template()
    _record_nonapproval(review, CANDIDATE_IDS[0], "REDEFINE")
    payload = _build(review)

    assert payload["status"] == approval.REDEFINE_STATUS
    assert payload["redefine_candidate_count"] == 1
    assert payload["recommended_next_research_task"] == approval.NEXT_ROUTE_REDEFINE


def test_withdraw_decision_has_priority_over_redefine() -> None:
    review = _template()
    _record_nonapproval(review, CANDIDATE_IDS[0], "REDEFINE")
    _record_nonapproval(review, CANDIDATE_IDS[1], "WITHDRAW")
    payload = _build(review)

    assert payload["status"] == approval.WITHDRAW_STATUS
    assert payload["withdraw_candidate_count"] == 1
    assert payload["recommended_next_research_task"] == approval.NEXT_ROUTE_WITHDRAW


def test_invalid_owner_decision_fails_closed() -> None:
    review = _template()
    review["candidate_reviews"][0]["decision"] = "AUTO_APPROVE"
    payload = _build(review)

    assert payload["status"] == approval.BLOCKED_STATUS
    assert approval.DECISION_INVALID in payload["candidate_reviews"][0]["gap_codes"]


def test_approve_requires_owner_review_metadata() -> None:
    review = _complete_review()
    review["candidate_reviews"][0]["review_owner"] = None
    payload = _build(review)

    assert payload["status"] == approval.BLOCKED_STATUS
    assert approval.REVIEW_METADATA_INCOMPLETE in payload["candidate_reviews"][0][
        "gap_codes"
    ]


def test_approve_requires_non_empty_parameters() -> None:
    review = _complete_review()
    review["candidate_reviews"][0]["runtime_spec"]["parameters"] = {}
    payload = _build(review)

    assert payload["runtime_spec_ready_count"] == 2
    assert approval.RUNTIME_SPEC_INCOMPLETE in payload["candidate_reviews"][0][
        "gap_codes"
    ]


def test_approve_requires_all_six_metric_contracts() -> None:
    review = _complete_review()
    review["candidate_reviews"][0]["metric_specs"].pop()
    payload = _build(review)

    assert payload["metric_contract_ready_count"] == 2
    assert approval.METRIC_CONTRACT_INCOMPLETE in payload["candidate_reviews"][0][
        "gap_codes"
    ]


def test_metric_contract_requires_calculator_provenance() -> None:
    review = _complete_review()
    review["candidate_reviews"][0]["metric_specs"][0]["calculator_id"] = None
    payload = _build(review)
    row = _metric_row(payload, CANDIDATE_IDS[0], approval.REQUIRED_METRIC_IDS[0])

    assert row["ready"] is False
    assert "calculator_id" in row["missing_fields"]


def test_approve_requires_non_empty_threshold_set() -> None:
    review = _complete_review()
    review["candidate_reviews"][0]["threshold_specs"] = []
    payload = _build(review)

    assert payload["threshold_policy_ready_count"] == 2
    assert approval.THRESHOLD_POLICY_INCOMPLETE in payload["candidate_reviews"][0][
        "gap_codes"
    ]


@pytest.mark.parametrize("invalid", [None, float("nan"), float("inf"), float("-inf")])
def test_null_nan_and_infinite_threshold_values_fail_closed(
    invalid: float | None,
) -> None:
    review = _complete_review()
    review["candidate_reviews"][0]["threshold_specs"][0]["threshold_value"] = invalid
    payload = _build(review)
    row = _threshold_row(payload, CANDIDATE_IDS[0])

    assert row["ready"] is False
    assert approval.THRESHOLD_VALUE_INVALID in row["gap_codes"]
    assert payload["status"] == approval.BLOCKED_STATUS


def test_calculator_or_owner_emitted_zero_threshold_is_valid() -> None:
    payload = _build(_complete_review(threshold_value=0.0))

    assert _threshold_row(payload, CANDIDATE_IDS[0])["threshold_value"] == 0.0
    assert payload["status"] == approval.READY_STATUS


def test_negative_threshold_is_valid_when_owner_provided() -> None:
    payload = _build(_complete_review(threshold_value=-0.25, operator="LTE"))

    assert _threshold_row(payload, CANDIDATE_IDS[0])["ready"] is True
    assert payload["status"] == approval.READY_STATUS


def test_between_threshold_requires_two_finite_bounds() -> None:
    ready = _build(_complete_review(threshold_value=[-0.5, 0.5], operator="BETWEEN"))
    blocked = _complete_review(threshold_value=[-0.5], operator="BETWEEN")

    assert ready["status"] == approval.READY_STATUS
    assert _build(blocked)["status"] == approval.BLOCKED_STATUS


@pytest.mark.parametrize("operator", sorted(approval.SUPPORTED_OPERATORS))
def test_all_explicit_threshold_operators_are_supported(operator: str) -> None:
    value: float | list[float] = (
        [-1.0, 1.0] if operator in {"BETWEEN", "OUTSIDE"} else 0.0
    )
    payload = _build(_complete_review(threshold_value=value, operator=operator))

    assert payload["status"] == approval.READY_STATUS


def test_unknown_threshold_operator_is_a_strict_error() -> None:
    review = _complete_review(operator="APPROX")
    payload = _build(review)

    assert any(
        item.startswith("unsupported_threshold_operator")
        for item in payload["strict_validation_errors"]
    )
    assert payload["status"] == approval.BLOCKED_STATUS


def test_unknown_threshold_metric_is_a_strict_error() -> None:
    review = _complete_review()
    review["candidate_reviews"][0]["threshold_specs"][0]["metric_id"] = (
        "future_unknown_metric"
    )
    payload = _build(review)

    assert any(
        item.startswith("unknown_threshold_metric")
        for item in payload["strict_validation_errors"]
    )


def test_unknown_metric_contract_id_is_a_strict_error() -> None:
    review = _complete_review()
    review["candidate_reviews"][0]["metric_specs"][0]["metric_id"] = (
        "future_unknown_metric"
    )
    payload = _build(review)

    assert any(
        item.startswith("unknown_metric_id")
        for item in payload["strict_validation_errors"]
    )


def test_duplicate_candidate_id_is_a_strict_error() -> None:
    review = _complete_review()
    review["candidate_reviews"].append(copy.deepcopy(review["candidate_reviews"][0]))
    payload = _build(review)

    assert "duplicate_candidate_id" in payload["strict_validation_errors"]


def test_candidate_identity_and_order_drift_is_a_strict_error() -> None:
    review = _complete_review()
    review["candidate_ids"] = list(reversed(review["candidate_ids"]))
    review["candidate_reviews"] = list(reversed(review["candidate_reviews"]))
    payload = _build(review)

    assert "candidate_identity_or_order_drift" in payload["strict_validation_errors"]


def test_invalid_safety_boundary_is_a_strict_error() -> None:
    review = _complete_review()
    review["safety_boundary"]["paper_shadow_allowed"] = True
    payload = _build(review)

    assert "owner_review_safety_boundary_invalid" in payload[
        "strict_validation_errors"
    ]
    assert payload["status"] == approval.BLOCKED_STATUS


def test_source_schema_status_and_route_are_strictly_validated() -> None:
    source = _source()
    source["schema_version"] = "wrong.v1"
    source["status"] = "WRONG"
    source["next_route"] = "WRONG_ROUTE"
    payload = _build(_template(), source=source)

    assert {
        "source_2438m_schema_version_mismatch",
        "source_2438m_status_mismatch",
        "source_2438m_route_mismatch",
    }.issubset(payload["strict_validation_errors"])


@pytest.mark.parametrize(
    ("field", "value", "error_code"),
    [
        ("blocked_count", 2, "source_2438m_blocked_count_mismatch"),
        (
            "candidate_replay_outcome_rechecked",
            False,
            "source_2438m_outcome_not_rechecked",
        ),
    ],
)
def test_ready_requires_complete_source_recheck_lineage(
    field: str, value: object, error_code: str
) -> None:
    source = _source()
    source[field] = value
    payload = _build(_complete_review(), source=source)

    assert payload["status"] == approval.BLOCKED_STATUS
    assert payload["source_2438m_ready_for_owner_review"] is False
    assert error_code in payload["strict_validation_errors"]


def test_source_candidate_identity_is_pinned_even_when_review_matches() -> None:
    source = _source()
    source["top3_candidate_ids"] = list(reversed(CANDIDATE_IDS))
    review = _complete_review()
    review["candidate_ids"] = list(source["top3_candidate_ids"])
    review["candidate_reviews"] = list(reversed(review["candidate_reviews"]))
    payload = _build(review, source=source)

    assert payload["status"] == approval.BLOCKED_STATUS
    assert "source_authoritative_candidate_identity_or_order_mismatch" in payload[
        "strict_validation_errors"
    ]


def test_owner_review_top_level_lineage_is_strictly_validated() -> None:
    cases = (
        ("task_id", "WRONG", "owner_review_task_id_mismatch"),
        ("source_task_id", "WRONG", "owner_review_source_task_id_mismatch"),
        ("market_regime", "legacy", "owner_review_market_regime_mismatch"),
        ("as_of", "2026-07-07", "owner_review_as_of_mismatch"),
        ("status", None, "owner_review_status_missing"),
        (
            "owner_review_status",
            None,
            "owner_review_governance_status_missing",
        ),
    )
    for field, value, error_code in cases:
        review = _complete_review()
        review[field] = value
        payload = _build(review)

        assert payload["status"] == approval.BLOCKED_STATUS
        assert error_code in payload["strict_validation_errors"]


def test_threshold_policy_requires_approved_status_and_validation_evidence() -> None:
    review = _complete_review()
    threshold = review["candidate_reviews"][0]["threshold_specs"][0]
    threshold["policy_status"] = "DRAFT"
    threshold["validation_evidence"] = []
    payload = _build(review)
    row = _threshold_row(payload, CANDIDATE_IDS[0])

    assert row["ready"] is False
    assert approval.THRESHOLD_POLICY_INCOMPLETE in row["gap_codes"]


def test_source_artifacts_and_no_effect_boundary_are_preserved() -> None:
    artifacts = [{"path": "source.json", "sha256": "abc", "schema_version": "v1"}]
    payload = _build(_template(), source_artifacts=artifacts)

    assert payload["source_artifacts"] == artifacts
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_status"].startswith("NOT_APPLICABLE")
    assert payload["threshold_values_changed"] is False
    assert payload["candidate_parameters_changed"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"


def test_dynamic_runner_writes_blocked_owner_input_artifacts(tmp_path: Path) -> None:
    paths = _runner_sources(tmp_path, _template())
    output_root = tmp_path / "outputs"
    docs_root = tmp_path / "docs"

    payload = impl.run_growth_tilt_candidate_runtime_spec_threshold_policy_approval(
        source_2438m_path=paths["source"],
        owner_review_path=paths["owner_review"],
        requirement_doc_path=paths["requirement"],
        report_registry_path=paths["registry"],
        artifact_catalog_path=paths["catalog"],
        system_flow_path=paths["flow"],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
        strict=True,
    )

    assert payload["status"] == approval.BLOCKED_STATUS
    for filename in (
        "approval_readiness_result.json",
        "candidate_runtime_spec_review_matrix.json",
        "metric_contract_review_matrix.json",
        "threshold_policy_review_matrix.json",
        "owner_action_checklist.json",
        "no_effect_boundary.json",
    ):
        assert (output_root / filename).exists()
    assert (
        docs_root / "growth_tilt_candidate_runtime_spec_threshold_policy_approval.md"
    ).exists()


def test_dynamic_runner_strict_mode_rejects_missing_source(tmp_path: Path) -> None:
    paths = _runner_sources(tmp_path, _template())

    with pytest.raises(ValueError, match="source_2438m missing"):
        impl.run_growth_tilt_candidate_runtime_spec_threshold_policy_approval(
            source_2438m_path=tmp_path / "missing.json",
            owner_review_path=paths["owner_review"],
            requirement_doc_path=paths["requirement"],
            report_registry_path=paths["registry"],
            artifact_catalog_path=paths["catalog"],
            system_flow_path=paths["flow"],
            output_root=tmp_path / "missing_outputs",
            docs_root=tmp_path / "missing_docs",
            as_of_date=date(2026, 7, 8),
            strict=True,
        )


def test_cli_realistic_pending_owner_review_run(tmp_path: Path) -> None:
    paths = _runner_sources(tmp_path, _template())
    output_root = tmp_path / "cli_outputs"
    docs_root = tmp_path / "cli_docs"
    result = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-candidate-runtime-spec-threshold-policy-approval",
            "--source-2438m",
            str(paths["source"]),
            "--owner-review",
            str(paths["owner_review"]),
            "--requirement-doc",
            str(paths["requirement"]),
            "--report-registry",
            str(paths["registry"]),
            "--artifact-catalog",
            str(paths["catalog"]),
            "--system-flow",
            str(paths["flow"]),
            "--output-root",
            str(output_root),
            "--docs-root",
            str(docs_root),
            "--as-of",
            "2026-07-08",
            "--strict",
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert approval.BLOCKED_STATUS in result.output
    assert "pending_candidate_count=3" in result.output
    assert "threshold_values_changed=false" in result.output
    assert "paper_shadow_allowed=False" in result.output
    assert "broker_action=none" in result.output


def test_registry_catalog_system_flow_and_task_register_are_aligned() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[approval.REPORT_TYPE]

    assert entry["command"] == (
        "aits research strategies "
        "growth-tilt-candidate-runtime-spec-threshold-policy-approval"
    )
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("approval_readiness_result.json" in item for item in entry["artifact_globs"])
    assert all(
        item in Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
        for item in approval.REQUIRED_CATALOG_REFERENCES
    )
    assert all(
        item in Path("docs/system_flow.md").read_text(encoding="utf-8")
        for item in approval.REQUIRED_FLOW_REFERENCES
    )
    assert (
        "TRADING-2438M1_GROWTH_TILT_CANDIDATE_RUNTIME_SPEC_AND_THRESHOLD_POLICY_APPROVAL"
        in Path("docs/task_register.md").read_text(encoding="utf-8")
    )


def _build(
    owner_review: dict[str, object],
    *,
    source: dict[str, object] | None = None,
    source_artifacts: list[dict[str, str]] | None = None,
) -> dict[str, object]:
    return approval.build_growth_tilt_candidate_runtime_spec_threshold_policy_approval(
        source or _source(),
        owner_review,
        source_artifacts=source_artifacts or [],
        report_registry={"reports": [{"report_id": approval.REPORT_TYPE}]},
        artifact_catalog_text="\n".join(approval.REQUIRED_CATALOG_REFERENCES),
        system_flow_text="\n".join(approval.REQUIRED_FLOW_REFERENCES),
        requirement_text="TRADING-2438M1 APPROVE REDEFINE WITHDRAW",
        as_of="2026-07-08",
    )


def _source() -> dict[str, object]:
    return {
        "schema_version": approval.EXPECTED_SOURCE_SCHEMA,
        "status": approval.EXPECTED_SOURCE_STATUS,
        "next_route": approval.EXPECTED_SOURCE_ROUTE,
        "run_id": "growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution:2026-07-08",
        "as_of": "2026-07-08",
        "top3_candidate_ids": list(CANDIDATE_IDS),
        "candidate_replay_outcome_rechecked": True,
        "blocked_count": 3,
        "production_effect": "none",
        "broker_action": "none",
    }


def _template() -> dict[str, object]:
    return copy.deepcopy(safe_load_yaml_path(TEMPLATE_PATH))


def _complete_review(
    *,
    threshold_value: float | list[float] = 0.0,
    operator: str = "GTE",
) -> dict[str, object]:
    review = _template()
    review["status"] = "OWNER_INPUT_COMPLETE"
    review["owner_review_status"] = "APPROVED"
    for candidate in review["candidate_reviews"]:
        candidate_id = candidate["candidate_id"]
        candidate.update(
            {
                "decision": "APPROVE",
                "decision_rationale": "Owner approves this fixture contract for validation.",
                "review_owner": "project_owner",
                "reviewed_at": "2026-07-10T00:00:00Z",
                "review_condition": "review_after_pit_replay",
                "expiry_condition": "expire_on_policy_or_candidate_change",
                "next_route": approval.NEXT_ROUTE_READY,
            }
        )
        candidate["runtime_spec"].update(
            {
                "approved": True,
                "executor_id": "fixture_growth_tilt_executor",
                "executor_version": "v1",
                "input_contract_version": "v1",
                "source_policy_ref": "fixture_policy:v1",
                "parameters": {"fixture_parameter": 1},
            }
        )
        for metric in candidate["metric_specs"]:
            metric.update(
                {
                    "source_field": metric["metric_id"],
                    "unit": "decimal_delta",
                    "normalization_rule_id": "identity",
                    "calculator_id": "fixture_metric_calculator",
                    "calculator_version": "v1",
                }
            )
        candidate["threshold_specs"] = [
            {
                "threshold_id": f"threshold:{candidate_id}",
                "metric_id": "return_delta_vs_baseline",
                "operator": operator,
                "threshold_value": threshold_value,
                "policy_owner": "project_owner",
                "policy_version": "v1",
                "policy_status": "APPROVED",
                "rationale": "Fixture threshold for contract validation only.",
                "validation_evidence": ["fixture_validation_evidence"],
                "evaluator_id": "growth_tilt_runtime_threshold_evaluator",
                "evaluator_version": "v1",
                "review_condition": "review_after_pit_replay",
                "expiry_condition": "expire_on_policy_or_candidate_change",
            }
        ]
    return review


def _record_nonapproval(
    review: dict[str, object], candidate_id: str, decision: str
) -> None:
    candidate = next(
        item for item in review["candidate_reviews"] if item["candidate_id"] == candidate_id
    )
    candidate.update(
        {
            "decision": decision,
            "decision_rationale": f"Owner selected {decision}.",
            "review_owner": "project_owner",
            "reviewed_at": "2026-07-10T00:00:00Z",
            "next_route": "owner_selected_route",
        }
    )


def _metric_row(
    payload: dict[str, object], candidate_id: str, metric_id: str
) -> dict[str, object]:
    section = payload["metric_contract_review_matrix"]
    return next(
        item
        for item in section["rows"]
        if item["candidate_id"] == candidate_id and item["metric_id"] == metric_id
    )


def _threshold_row(
    payload: dict[str, object], candidate_id: str
) -> dict[str, object]:
    section = payload["threshold_policy_review_matrix"]
    return next(
        item for item in section["rows"] if item["candidate_id"] == candidate_id
    )


def _runner_sources(
    tmp_path: Path, owner_review: dict[str, object]
) -> dict[str, Path]:
    paths = {
        "source": tmp_path / "source.json",
        "owner_review": tmp_path / "owner_review.yaml",
        "requirement": tmp_path / "requirement.md",
        "registry": tmp_path / "registry.yaml",
        "catalog": tmp_path / "catalog.md",
        "flow": tmp_path / "flow.md",
    }
    paths["source"].write_text(json.dumps(_source()), encoding="utf-8")
    paths["owner_review"].write_text(json.dumps(owner_review), encoding="utf-8")
    paths["requirement"].write_text(
        "TRADING-2438M1 APPROVE REDEFINE WITHDRAW", encoding="utf-8"
    )
    paths["registry"].write_text(
        json.dumps({"reports": [{"report_id": approval.REPORT_TYPE}]}),
        encoding="utf-8",
    )
    paths["catalog"].write_text(
        "\n".join(approval.REQUIRED_CATALOG_REFERENCES), encoding="utf-8"
    )
    paths["flow"].write_text(
        "\n".join(approval.REQUIRED_FLOW_REFERENCES), encoding="utf-8"
    )
    return paths
