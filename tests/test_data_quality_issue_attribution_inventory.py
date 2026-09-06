from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest

from ai_trading_system.data.quality_issue_attribution_inventory import (
    DEFAULT_JSON_PATH,
    DEFAULT_MARKDOWN_PATH,
    DEFAULT_VALIDATION_PATH,
    AttributionInventoryError,
    build_attribution_readiness_inventory,
    load_and_validate_attribution_readiness_inventory,
    render_attribution_readiness_markdown,
    validate_attribution_readiness_inventory,
)
from ai_trading_system.platform.artifacts import load_strict_json_path

REPO_ROOT = Path(__file__).resolve().parents[1]

# Exact S2b emission identities, not issue-code patterns that grant isolation.
# Stable IDs bind path/function/emitter/code expression/occurrence, never line numbers.
NAMED_BINDING_SITES = {
    "dq_issue_site_209756a138c3d5004b20": (
        "STATIC_LITERAL",
        "download_manifest_named_binding_mismatch",
        "download_manifest_named_binding_mismatch",
    ),
    "dq_issue_site_3194f2d7bb6556c564c2": (
        "TEMPLATE_EXPRESSION",
        "f'{role}_named_download_publication_binding_mismatch'",
        None,
    ),
    "dq_issue_site_5b8c2dd5721991b230d2": (
        "DYNAMIC_EXPRESSION",
        "exc.code.removeprefix('DQ_').lower()",
        None,
    ),
}


def test_real_inventory_is_complete_fail_closed_and_owner_review_ready() -> None:
    inventory = build_attribution_readiness_inventory(repo_root=REPO_ROOT)

    assert inventory["status"] == "SOURCE_OWNER_REVIEW_REQUIRED"
    assert inventory["authority"] == {
        "phase": "DATA-GOV-002_PHASE_C",
        "inventory_is_authorization": False,
        "new_issue_migration_authorized": False,
        "message_or_sample_scope_inference_allowed": False,
        "unreviewed_default_scope_status": "GLOBAL_OR_UNKNOWN_SCOPE",
    }
    assert inventory["policy_authorized_issue_codes"] == ["prices_non_market_session_date"]
    assert inventory["summary"] == {
        "canonical_site_count": 72,
        "direct_constructor_site_count": 66,
        "factory_call_site_count": 6,
        "static_site_count": 57,
        "template_site_count": 12,
        "dynamic_site_count": 3,
        "unique_static_code_count": 54,
        "policy_authorized_code_count": 1,
        "policy_authorized_site_count": 1,
        "legacy_affected_instruments_site_count": 1,
        "owner_review_required_site_count": 71,
        "factory_implementation_constructor_count": 1,
        "noncanonical_constructor_site_count": 2,
    }
    site_ids = [site["site_id"] for site in inventory["sites"]]
    assert len(site_ids) == len(set(site_ids)) == 72
    authorized = [site for site in inventory["sites"] if site["existing_policy_authorized"]]
    assert len(authorized) == 1
    assert authorized[0]["static_code"] == "prices_non_market_session_date"
    assert authorized[0]["legacy_affected_instruments_present"] is True
    assert authorized[0]["phase_c_migration_eligible"] is False
    assert all(
        site["phase_c_migration_eligible"] is False
        and site["message_or_sample_scope_inference_allowed"] is False
        for site in inventory["sites"]
    )
    assert {site["source_path"] for site in inventory["noncanonical_constructor_sites"]} == {
        "src/ai_trading_system/scoring/baseline_score_backfill.py"
    }
    assert inventory["safety"]["data_quality_behavior_changed"] is False
    assert inventory["safety"]["new_issue_isolation_authorized"] is False
    assert inventory["safety"]["production_effect"] == "none"
    assert inventory["safety"]["broker_action"] == "none"


def test_named_binding_sites_have_exact_identity_without_scope_authority() -> None:
    inventory = build_attribution_readiness_inventory(repo_root=REPO_ROOT)
    sites = {
        site["site_id"]: site
        for site in inventory["sites"]
        if site["source_path"] == "src/ai_trading_system/data/quality.py"
        and site["enclosing_function"] == "_check_named_download_binding"
    }

    assert set(sites) == set(NAMED_BINDING_SITES)
    for site_id, expected in NAMED_BINDING_SITES.items():
        site = sites[site_id]
        assert (site["code_kind"], site["code_expression"], site["static_code"]) == expected
        assert site["emitter_kind"] == "DIRECT_CONSTRUCTOR"
        assert site["emitter"] == "DataQualityIssue"
        assert site["occurrence"] == 0
        assert site["scope_status"] == "GLOBAL_OR_UNKNOWN_SCOPE"
        assert site["owner_review_status"] == "OWNER_REVIEW_REQUIRED"
        assert site["phase_c_migration_eligible"] is False
        assert site["existing_policy_authorized"] is False
        assert site["legacy_affected_instruments_present"] is False
        assert site["message_or_sample_scope_inference_allowed"] is False
    assert inventory["policy_authorized_issue_codes"] == ["prices_non_market_session_date"]
    assert inventory["summary"]["policy_authorized_code_count"] == 1
    assert inventory["summary"]["policy_authorized_site_count"] == 1


@pytest.mark.parametrize("site_id", tuple(NAMED_BINDING_SITES))
@pytest.mark.parametrize("tamper", ["identity", "expression", "scope_promotion"])
def test_named_binding_site_tamper_cannot_create_attribution_authority(
    site_id: str,
    tamper: str,
) -> None:
    inventory = build_attribution_readiness_inventory(repo_root=REPO_ROOT)
    changed = deepcopy(inventory)
    site = next(item for item in changed["sites"] if item["site_id"] == site_id)
    if tamper == "identity":
        site["site_id"] = "dq_issue_site_unreviewed"
    elif tamper == "expression":
        site["code_expression"] = "rates_prices_named_binding_is_not_owner_review"
    else:
        assert tamper == "scope_promotion"
        site["scope_status"] = "EXISTING_POLICY_AUTHORIZED_INSTRUMENT_SCOPE"
        site["owner_review_status"] = "EXISTING_OWNER_REVIEWED_PILOT"
        site["phase_c_migration_eligible"] = True
        site["existing_policy_authorized"] = True

    validation = validate_attribution_readiness_inventory(changed, repo_root=REPO_ROOT)

    assert validation["status"] == "FAIL"
    assert "content_derived_rebuild_mismatch" in validation["errors"]
    if tamper == "scope_promotion":
        assert "phase_c_migration_authority_illegal" in validation["errors"]
    assert changed["policy_authorized_issue_codes"] == ["prices_non_market_session_date"]
    assert changed["authority"]["new_issue_migration_authorized"] is False


def test_tracked_inventory_markdown_and_validation_are_fresh() -> None:
    inventory_path = REPO_ROOT / DEFAULT_JSON_PATH
    markdown_path = REPO_ROOT / DEFAULT_MARKDOWN_PATH
    validation_path = REPO_ROOT / DEFAULT_VALIDATION_PATH

    inventory = load_strict_json_path(inventory_path)
    assert isinstance(inventory, dict)
    assert inventory == build_attribution_readiness_inventory(repo_root=REPO_ROOT)
    assert markdown_path.read_text(encoding="utf-8") == (
        render_attribution_readiness_markdown(inventory)
    )
    validation = load_and_validate_attribution_readiness_inventory(
        repo_root=REPO_ROOT,
        inventory_path=inventory_path,
    )
    assert validation["status"] == "PASS"
    assert load_strict_json_path(validation_path) == validation


@pytest.mark.parametrize(
    "tamper",
    [
        "site_removed",
        "scope_promoted",
        "source_hash",
        "safety",
    ],
)
def test_validator_rejects_inventory_tamper(tamper: str) -> None:
    inventory = build_attribution_readiness_inventory(repo_root=REPO_ROOT)
    changed = deepcopy(inventory)
    if tamper == "site_removed":
        changed["sites"] = changed["sites"][:-1]
    elif tamper == "scope_promoted":
        changed["sites"][0]["phase_c_migration_eligible"] = True
    elif tamper == "source_hash":
        changed["source_bindings"][0]["sha256"] = "0" * 64
    elif tamper == "safety":
        changed["safety"]["new_issue_isolation_authorized"] = True
    else:  # pragma: no cover
        raise AssertionError(tamper)

    result = validate_attribution_readiness_inventory(
        changed,
        repo_root=REPO_ROOT,
    )

    assert result["status"] == "FAIL"
    assert result["error_count"] >= 1
    assert "content_derived_rebuild_mismatch" in result["errors"]


def test_fixture_scanner_classifies_static_template_dynamic_and_factory(
    tmp_path: Path,
) -> None:
    _write_fixture_repository(tmp_path)

    inventory = build_attribution_readiness_inventory(repo_root=tmp_path)

    assert inventory["summary"] == {
        "canonical_site_count": 5,
        "direct_constructor_site_count": 3,
        "factory_call_site_count": 2,
        "static_site_count": 2,
        "template_site_count": 1,
        "dynamic_site_count": 2,
        "unique_static_code_count": 2,
        "policy_authorized_code_count": 1,
        "policy_authorized_site_count": 1,
        "legacy_affected_instruments_site_count": 1,
        "owner_review_required_site_count": 4,
        "factory_implementation_constructor_count": 1,
        "noncanonical_constructor_site_count": 1,
    }
    assert {site["code_kind"] for site in inventory["sites"]} == {
        "STATIC_LITERAL",
        "TEMPLATE_EXPRESSION",
        "DYNAMIC_EXPRESSION",
    }
    authorized = [site for site in inventory["sites"] if site["existing_policy_authorized"]]
    assert [site["static_code"] for site in authorized] == ["static_code"]


def test_conflicting_reviewed_policy_rules_fail_closed(tmp_path: Path) -> None:
    _write_fixture_repository(tmp_path)
    second_policy = tmp_path / "config/data_quality/regime_label_generator_capability_v1.yaml"
    second_policy.write_text(
        second_policy.read_text(encoding="utf-8").replace(
            "ALL_AFFECTED_INSTRUMENTS_OUTSIDE_REQUIRED_SCOPE",
            "DIFFERENT_UNREVIEWED_RULE",
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        AttributionInventoryError,
        match="inconsistent reviewed attribution rules",
    ):
        build_attribution_readiness_inventory(repo_root=tmp_path)


def test_markdown_is_projection_and_discloses_non_authority() -> None:
    inventory = build_attribution_readiness_inventory(repo_root=REPO_ROOT)

    markdown = render_attribution_readiness_markdown(inventory)

    assert inventory["inventory_id"] in markdown
    assert "本 inventory 不是新 issue 隔离授权" in markdown
    assert "GLOBAL_OR_UNKNOWN_SCOPE" in markdown
    assert "Source owner 必须逐 exact site/code 审查" in markdown
    assert "production_effect：`none`" in markdown
    assert "broker_action：`none`" in markdown


def _write_fixture_repository(root: Path) -> None:
    quality_path = root / "src/ai_trading_system/data/quality.py"
    execution_path = root / "src/ai_trading_system/data/quality_execution.py"
    noncanonical_path = root / "src/ai_trading_system/other.py"
    first_policy = root / "config/data_quality/decision_target_label_core_capability_v1.yaml"
    second_policy = root / "config/data_quality/regime_label_generator_capability_v1.yaml"
    for path in (
        quality_path,
        execution_path,
        noncanonical_path,
        first_policy,
        second_policy,
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
    quality_path.write_text(
        """
def emit(column, code, affected):
    DataQualityIssue(ERROR, "static_code", "message", affected_instruments=affected)
    DataQualityIssue(ERROR, f"prices_invalid_{column}", "message")
    DataQualityIssue(ERROR, code, "message")
""".lstrip(),
        encoding="utf-8",
    )
    execution_path.write_text(
        """
def emit(code):
    _provenance_issue("DQ_STATIC", "message")
    _provenance_issue(code, "message", role="prices")

def _provenance_issue(code, message, role=None):
    return DataQualityIssue(
        severity=ERROR,
        code=code,
        message=message,
        source="D0B canonical execution provenance",
        sample=role,
    )
""".lstrip(),
        encoding="utf-8",
    )
    noncanonical_path.write_text(
        'def emit():\n    DataQualityIssue(ERROR, "outside", "message")\n',
        encoding="utf-8",
    )
    policy_text = """
schema_version: data_quality_consumer_capability_policy.v1
policy_id: fixture
policy_version: 1.0.0
status: OWNER_APPROVED_FIXTURE
owner: fixture_owner
owner_decision_id: fixture_decision
allowed_global_error_codes:
  - static_code
global_error_attribution_rule: ALL_AFFECTED_INSTRUMENTS_OUTSIDE_REQUIRED_SCOPE
""".lstrip()
    first_policy.write_text(policy_text, encoding="utf-8")
    second_policy.write_text(
        policy_text.replace("policy_id: fixture", "policy_id: fixture_two"),
        encoding="utf-8",
    )
