import hashlib
import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = ROOT / "config/governance/risk_tiered_external_action_evidence_admission_v1.yaml"
EVIDENCE_DIR = (
    ROOT
    / "inputs/research/qqq_options/trading_2537_existing_clone_exact_date_execution_v2"
)
V2_CANDIDATE = (
    ROOT
    / "inputs/research/qqq_options/"
    "trading_2537_exact_date_provider_catalog_attribution_correction_v2/main.py"
)


def _load_policy() -> dict[str, object]:
    return yaml.safe_load(POLICY_PATH.read_text(encoding="utf-8"))


def test_authorization_and_technical_validity_are_separate_axes() -> None:
    policy = _load_policy()
    principles = policy["principles"]
    admission = policy["admission"]

    assert principles["authorization_and_technical_validity_are_independent_axes"] is True
    assert principles["missing_preformatted_token_is_not_a_technical_rejection_reason"] is True
    assert admission["authorization_state_recorded_separately"] is True
    assert admission["technical_validation_state_recorded_separately"] is True
    assert admission["non_exact_preauthorization_alone_blocks_technical_admission"] is False


def test_risk_tiers_preserve_material_and_broker_authorization_boundaries() -> None:
    policy = _load_policy()
    tiers = policy["risk_tiers"]

    assert tiers["R1_BOUNDED_RESEARCH_SANDBOX"]["preformatted_exact_token_required"] is False
    assert tiers["R1_BOUNDED_RESEARCH_SANDBOX"]["standing_owner_scope_allowed"] is True
    assert (
        tiers["R2_MATERIAL_EXTERNAL_CHANGE"]["concise_explicit_owner_instruction_required"] is True
    )
    assert tiers["R3_PRODUCTION_OR_BROKER"]["separate_exact_scope_authorization_required"] is True


def test_trading_2537_current_standing_scope_is_zero_order_and_exact_candidate() -> None:
    policy = _load_policy()
    scope = policy["current_standing_scopes"]["TRADING_2537_SOURCE_TIME_V2_EXISTING_CLONE"]

    assert scope["authorization_state"] == "STANDING_OWNER_SCOPE"
    assert scope["scope_status"] == "CONSUMED_CLOSED"
    assert scope["technical_validation_state"] == "PASS"
    assert scope["target_clone_project_id"] == 35444189
    assert scope["original_project_id"] == 34808569
    assert scope["original_project_mutations_allowed"] == 0
    assert scope["maximum_new_clones"] == 0
    assert scope["project_code_lf_byte_count"] == 26587
    assert scope["project_code_lf_sha256"] == (
        "06b26262823c8c56ebceb4c90356086e07b050f9192e087b5e35a3dc43c5eac2"
    )
    assert scope["maximum_additional_project_mutations"] == 1
    assert scope["maximum_additional_saves"] == 1
    assert scope["maximum_additional_automatic_cloud_builds"] == 1
    assert scope["maximum_zero_order_cloud_backtests"] == 1
    assert scope["maximum_provider_queries"] == 1
    assert scope["maximum_orders"] == 0
    assert scope["maximum_fills"] == 0
    assert scope["automatic_retry_allowed"] is False
    assert scope["lean_engine_version"] == "2.5.0.0.18024"
    assert scope["build_id"] == "d432a0-8b195b"
    assert scope["backtest_id"] == "351d818182ef42b62f4d968016035854"
    assert scope["actual_counters"] == {
        "new_clones": 0,
        "original_project_mutations": 0,
        "additional_clone_project_mutations": 1,
        "additional_saves": 1,
        "additional_automatic_cloud_builds": 1,
        "zero_order_cloud_backtests": 1,
        "provider_queries": 1,
        "orders": 0,
        "fills": 0,
    }
    assert scope["broker_action"] == "none"
    assert scope["production_effect"] == "none"


def test_trading_2537_v2_execution_evidence_is_exact_and_self_verifying() -> None:
    admission = json.loads((EVIDENCE_DIR / "standing_scope_admission.json").read_text())
    terminal = json.loads((EVIDENCE_DIR / "export_safe_terminal_evidence.json").read_text())
    ledger = json.loads((EVIDENCE_DIR / "external_action_ledger.json").read_text())
    manifest = json.loads((EVIDENCE_DIR / "execution_evidence_manifest.json").read_text())

    candidate_lf = V2_CANDIDATE.read_bytes().replace(b"\r\n", b"\n")
    assert len(candidate_lf) == admission["candidate"]["lf_byte_count"] == 26587
    assert hashlib.sha256(candidate_lf).hexdigest() == admission["candidate"]["lf_sha256"]
    assert admission["manifest_replay_state"] == "PASS"
    assert admission["candidate_readback_state"] == "PASS"
    assert admission["scope_status"] == "CONSUMED_CLOSED"
    assert admission["automatic_retry_allowed"] is False

    assert terminal["technical_validation_state"] == "PASS"
    assert terminal["requested_range"] == terminal["evaluated_range"] == (
        "2021-02-22..2025-12-02"
    )
    assert terminal["expected_session_count"] == terminal["observed_session_count"] == 1202
    assert terminal["target_session_count"] == 1
    assert terminal["target_session_date"] == "2022-08-26"
    assert terminal["target_session_position"] == "INTERIOR"
    assert terminal["target_equity_slice_present"] is True
    assert terminal["target_subscribed_chain_event_count"] == 0
    assert terminal["provider_probe_status"] == "EXACT_DATE_AVAILABLE"
    assert terminal["provider_query_attempt_count"] == 1
    assert terminal["exact_date_record_count"] == 1
    assert terminal["exact_date_contract_count"] == 6496
    assert terminal["non_target_record_count"] == 0
    assert terminal["cross_date_fallback_detected"] is False
    assert terminal["attribution"] == "EXACT_DATE_CATALOG_AVAILABLE_SUBSCRIPTION_MISSING"
    assert terminal["attribution_terminal"] == "RESOLVED"
    assert terminal["execution_terminal"] == "COMPLETE"
    assert terminal["durable_subscription_repair_status"] == "NOT_IMPLEMENTED"
    assert terminal["data_quality_status"] == "FAIL"
    assert terminal["point_in_time_status"] == "NOT_EVALUATED"
    assert terminal["orders"] == terminal["fills"] == 0
    assert terminal["raw_option_rows_exported"] is False
    assert terminal["logs_used_as_data"] is False
    assert terminal["object_store_used"] is False

    assert [item["ordinal"] for item in ledger["actions"]] == list(range(1, 9))
    assert [item["action"] for item in ledger["actions"]] == [
        "manifest_replay",
        "clone_project_mutation",
        "clone_project_save",
        "automatic_cloud_build",
        "zero_order_cloud_backtest",
        "provider_query",
        "terminal_statistics_readback",
        "technical_evidence_admission",
    ]
    assert ledger["actual_counters"] == admission["declared_maxima"]
    assert ledger["lifetime_counters_after_v2"] == {
        "clones": 1,
        "project_mutations": 3,
        "saves": 3,
        "cloud_builds": 4,
        "cloud_backtests": 2,
        "provider_queries": 2,
        "orders": 0,
        "fills": 0,
    }
    assert ledger["retry_used"] is False
    assert ledger["original_project_mutated"] is False
    assert ledger["new_clone_created"] is False

    assert manifest["artifact_count"] == len(manifest["artifacts"]) == 3
    for name, expected_sha256 in manifest["artifacts"].items():
        assert hashlib.sha256((EVIDENCE_DIR / name).read_bytes()).hexdigest() == expected_sha256
    assert manifest["technical_validation_state"] == "PASS"
    assert manifest["exact_date_contract_count"] == 6496
    assert manifest["attribution_terminal"] == "RESOLVED"
    assert manifest["durable_subscription_repair_status"] == "NOT_IMPLEMENTED"
    assert manifest["successor_task_id"].startswith("TRADING-2541_")


def test_trading_2537_resolution_and_trading_2541_repair_boundary_are_disclosed() -> None:
    flow = (ROOT / "docs/system_flow.md").read_text(encoding="utf-8")
    atlas = (ROOT / "config/atlas/page_effectiveness.yaml").read_text(encoding="utf-8")
    requirement_2537 = (
        ROOT
        / "docs/requirements/"
        "TRADING-2537_QC_QQQ_Options_Exact_Date_Provider_Catalog_Attribution_Correction_V1.md"
    ).read_text(encoding="utf-8")
    requirement_2539 = (
        ROOT
        / "docs/requirements/"
        "TRADING-2539_QC_Cloud_File_API_Exact_Content_Mutation_And_Retry_Proposal_V1.md"
    ).read_text(encoding="utf-8")
    requirement_2541 = (
        ROOT
        / "docs/requirements/"
        "TRADING-2541_QC_QQQ_Options_Exact_Date_Subscription_Missing_Remediation_V1.md"
    ).read_text(encoding="utf-8")

    for content in (flow, atlas, requirement_2537, requirement_2539, requirement_2541):
        assert "2022-08-26" in content
        assert "6496" in content
    assert "## TRADING-2541 exact-date subscription missing remediation V1" in flow
    assert "EXACT_DATE_CATALOG_AVAILABLE_SUBSCRIPTION_MISSING" in flow
    assert "diagnosis" not in requirement_2541.lower() or "repair" in requirement_2541.lower()
    assert "数据修复尚未实现" in atlas
    assert "cross-date" in requirement_2541


def test_project_rules_and_flow_expose_successor_policy() -> None:
    agents = (ROOT / "AGENTS.md").read_text(encoding="utf-8")
    flow = (ROOT / "docs/system_flow.md").read_text(encoding="utf-8")
    requirement = (
        ROOT / "docs/requirements/DEVX-008_Risk_Tiered_External_Action_and_Evidence_Admission.md"
    ).read_text(encoding="utf-8")

    assert "## Risk-Tiered External Actions and Evidence Admission" in agents
    assert "R1_BOUNDED_RESEARCH_SANDBOX" in agents
    assert "authorization_state" in agents
    assert "technical_validation_state" in agents
    assert "## DEVX-008 风险分级外部动作与实证证据准入" in flow
    assert "STANDING_OWNER_SCOPE" in flow
    assert "动作授权与证据正确性分轴" in requirement
