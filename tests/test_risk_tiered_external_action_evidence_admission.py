from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = ROOT / "config/governance/risk_tiered_external_action_evidence_admission_v1.yaml"


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
    assert scope["broker_action"] == "none"
    assert scope["production_effect"] == "none"


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
