from __future__ import annotations

import copy

import pytest
import yaml

from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_smoothed_promotion as promotion,
)


def _config() -> dict[str, object]:
    return yaml.safe_load(
        promotion.DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH.read_text(encoding="utf-8")
    )


def _policy() -> dict[str, object]:
    return promotion._promotion_policy(_config())


def test_promotion_policy_requires_candidate_gates_and_explicit_owner_decisions() -> None:
    policy = _policy()

    assert policy["candidate_required_for_owner_review"] is True
    assert policy["candidate_required_for_switch"] is True
    assert policy["eligible_readiness_decisions"] == ["PROMOTE_FOR_REVIEW"]
    assert "promote_to_primary_research_candidate" in policy["owner_decisions"]


def test_promotion_policy_rejects_disabled_candidate_gate() -> None:
    changed = copy.deepcopy(_config())
    changed["promotion_policy"]["candidate_required_for_switch"] = False

    with pytest.raises(
        promotion.DynamicV3SmoothedPromotionError,
        match="candidate requirements must be true",
    ):
        promotion._promotion_policy(changed)


def test_candidate_less_forward_binding_preserves_zero_targets() -> None:
    confirmation = {
        "confirmation_id": "confirmation-1",
        "smoothed_confirmation_targets": {
            "candidate_method": None,
            "targets": [],
        },
    }
    gate = {"gate_id": "gate-1", "gate_decision": {"candidate_method": None}}

    result = promotion._bound_targets("binding-1", confirmation, gate)

    assert result["candidate_method"] is None
    assert result["binding_status"] == "NOT_REGISTERED"
    assert result["targets"] == []
    assert promotion._forward_requirements(result)["requirements"] == []


def test_candidate_less_switch_plan_cannot_propose_fixed_method() -> None:
    gate = {
        "gate_decision": {
            "candidate_method": None,
            "gate_decision": "CONTINUE_OBSERVATION",
        }
    }
    binding = {
        "bound_confirmation_targets": {
            "candidate_method": None,
            "binding_status": "NOT_REGISTERED",
            "targets": [],
        }
    }

    result = promotion._switch_plan("switch-1", gate, binding, _policy())

    assert result["proposed_primary_research_candidate"] is None
    assert result["switch_decision"] == "NO_ELIGIBLE_CANDIDATE"
    assert result["actual_switch_executed"] is False
    assert promotion._switch_safety(result)["status"] == "PASS"


def test_candidate_less_owner_record_rejects_promotion() -> None:
    promotion_payload = {
        "promotion_evidence_summary": {"candidate_method": None},
        "promotion_blocking_issues": {"can_enter_owner_review": False},
    }
    gate = {
        "gate_decision": {
            "candidate_method": None,
            "gate_decision": "CONTINUE_OBSERVATION",
        }
    }
    switch = {
        "primary_switch_plan": {
            "candidate_method": None,
            "current_primary_research_candidate": "limited_adjustment",
            "proposed_primary_research_candidate": None,
            "switch_decision": "NO_ELIGIBLE_CANDIDATE",
        }
    }

    with pytest.raises(
        promotion.DynamicV3SmoothedPromotionError,
        match="cannot promote without an eligible candidate",
    ):
        promotion._owner_decision(
            "decision-1",
            promotion_payload,
            gate,
            switch,
            {
                "owner_decision": "promote_to_primary_research_candidate",
                "decision_reason": "invalid",
                "recorded_at": "2026-01-01T00:00:00+00:00",
            },
            _policy(),
        )


def test_validation_cache_is_content_addressed(tmp_path) -> None:
    artifact_id = "artifact-1"
    artifact = tmp_path / artifact_id
    artifact.mkdir()
    view = artifact / "manifest.json"
    view.write_text('{"status":"PASS"}', encoding="utf-8")
    calls = 0

    def validator(*, artifact_id: str, output_dir) -> dict[str, object]:
        nonlocal calls
        calls += 1
        return {"artifact_id": artifact_id, "output_dir": str(output_dir), "status": "PASS"}

    with promotion.smoothed_promotion_validation_session():
        first = promotion._cached_artifact_validation(
            validator=validator,
            validator_key="artifact_id",
            artifact_id=artifact_id,
            root=tmp_path,
        )
        second = promotion._cached_artifact_validation(
            validator=validator,
            validator_key="artifact_id",
            artifact_id=artifact_id,
            root=tmp_path,
        )
        view.write_text('{"status":"FAIL"}', encoding="utf-8")
        third = promotion._cached_artifact_validation(
            validator=validator,
            validator_key="artifact_id",
            artifact_id=artifact_id,
            root=tmp_path,
        )

    assert first == second == third
    assert calls == 2


def test_source_binding_is_bounded_but_live_validator_covers_input_snapshot(tmp_path) -> None:
    artifact_id = "artifact-1"
    artifact = tmp_path / artifact_id
    artifact.mkdir()
    snapshot = artifact / "example_input_snapshot.json"
    snapshot.write_text('{"source":"original"}\n', encoding="utf-8")
    (artifact / "example_manifest.json").write_text(
        '{"artifact_id":"artifact-1"}\n', encoding="utf-8"
    )
    (artifact / "business_view.json").write_text('{"decision":"CONTINUE"}\n', encoding="utf-8")

    def validator(*, artifact_id: str, output_dir) -> dict[str, object]:
        current = output_dir / artifact_id / "example_input_snapshot.json"
        return {
            "artifact_id": artifact_id,
            "status": "PASS" if "original" in current.read_text(encoding="utf-8") else "FAIL",
        }

    binding = promotion._source_binding(
        kind="example",
        artifact_id=artifact_id,
        root=tmp_path,
        validator=validator,
        validator_key="artifact_id",
        json_views=(
            "example_input_snapshot.json",
            "example_manifest.json",
            "business_view.json",
        ),
    )

    bundled_json = binding["bundle"]["json"]
    assert "example_input_snapshot.json" not in bundled_json
    assert set(bundled_json) == {"example_manifest.json", "business_view.json"}

    snapshot.write_text('{"source":"tampered"}\n', encoding="utf-8")
    errors = promotion._validate_binding(
        binding,
        kind="example",
        validator=validator,
        validator_key="artifact_id",
    )

    assert errors
    assert any("validation" in error for error in errors)
