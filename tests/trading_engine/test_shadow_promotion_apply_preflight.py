from __future__ import annotations

import builtins
import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_payload,
    build_daily_task_dashboard_report,
    render_daily_task_dashboard,
)
from ai_trading_system.trading_engine.shadow_promotion_apply_preflight import (
    build_shadow_promotion_apply_preflight_payload,
    write_shadow_promotion_apply_preflight_report,
)


def test_preflight_missing_proposal_is_insufficient_data(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 20)
    production_path = _write_production_profile(tmp_path)
    _write_current_shadow_weights(context["data_root"])
    _write_approval(
        context["data_root"],
        as_of,
        proposal_path=_proposal_path(context["data_root"], as_of),
        production_path=production_path,
        proposal_sha256="missing",
    )

    payload = build_shadow_promotion_apply_preflight_payload(
        as_of=as_of,
        data_root=context["data_root"],
        production_profile_path=production_path,
        generated_at=_fixed_generated_at(),
    )

    _assert_safe_preflight(payload)
    assert payload["preflight_decision"] == "INSUFFICIENT_DATA"
    assert payload["input_artifacts"]["promotion_proposal"]["status"] == "MISSING"


def test_preflight_missing_approval_is_insufficient_data(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 20)
    production_path = _write_production_profile(tmp_path)
    _write_current_shadow_weights(context["data_root"])
    _write_proposal(context["data_root"], as_of)

    payload = build_shadow_promotion_apply_preflight_payload(
        as_of=as_of,
        data_root=context["data_root"],
        production_profile_path=production_path,
        generated_at=_fixed_generated_at(),
    )

    _assert_safe_preflight(payload)
    assert payload["preflight_decision"] == "INSUFFICIENT_DATA"
    assert payload["input_artifacts"]["approval_artifact"]["status"] == "MISSING"


def test_preflight_rejects_unapproved_approval(tmp_path: Path) -> None:
    payload = _build_payload_with_mutations(tmp_path, approval_overrides={"approved": False})

    _assert_safe_preflight(payload)
    assert payload["preflight_decision"] == "APPROVAL_INVALID"
    assert "approved" in payload["approval_validation"]["blocking_reasons"]


def test_preflight_rejects_approval_hash_mismatch(tmp_path: Path) -> None:
    payload = _build_payload_with_mutations(
        tmp_path,
        approval_proposal_overrides={"proposal_sha256": "bad"},
    )

    _assert_safe_preflight(payload)
    assert payload["preflight_decision"] == "APPROVAL_INVALID"
    assert payload["approval_validation"]["proposal_hash_match"] is False


def test_preflight_rejects_missing_preflight_only_acknowledgement(tmp_path: Path) -> None:
    payload = _build_payload_with_mutations(
        tmp_path,
        safety_ack_overrides={"preflight_only": False},
    )

    _assert_safe_preflight(payload)
    assert payload["preflight_decision"] == "APPROVAL_INVALID"
    assert "preflight_only_acknowledged" in payload["approval_validation"]["blocking_reasons"]


def test_preflight_rejects_non_manual_review_proposal_decision(tmp_path: Path) -> None:
    payload = _build_payload_with_mutations(
        tmp_path,
        proposal_overrides={"proposal_decision": "CONTINUE_OBSERVATION"},
        approval_proposal_overrides={"proposal_decision": "CONTINUE_OBSERVATION"},
    )

    _assert_safe_preflight(payload)
    assert payload["preflight_decision"] == "PROPOSAL_INVALID"
    assert "proposal_decision" in payload["proposal_validation"]["blocking_reasons"]


def test_preflight_rejects_promotion_proposed_false(tmp_path: Path) -> None:
    payload = _build_payload_with_mutations(
        tmp_path,
        proposal_overrides={"promotion_proposed": False},
        approval_proposal_overrides={"promotion_proposed": False},
    )

    _assert_safe_preflight(payload)
    assert payload["preflight_decision"] == "PROPOSAL_INVALID"
    assert "promotion_proposed" in payload["proposal_validation"]["blocking_reasons"]


def test_preflight_rejects_promotion_executed_true(tmp_path: Path) -> None:
    payload = _build_payload_with_mutations(
        tmp_path,
        proposal_overrides={"promotion_executed": True},
    )

    _assert_safe_preflight(payload)
    assert payload["preflight_decision"] == "PROPOSAL_INVALID"
    assert "promotion_executed" in payload["proposal_validation"]["blocking_reasons"]


def test_preflight_rejects_non_none_production_effect(tmp_path: Path) -> None:
    payload = _build_payload_with_mutations(
        tmp_path,
        proposal_overrides={"production_effect": "writes_production"},
    )

    _assert_safe_preflight(payload)
    assert payload["preflight_decision"] == "PROPOSAL_INVALID"
    assert "production_effect" in payload["proposal_validation"]["blocking_reasons"]


def test_preflight_rejects_weight_key_mismatch(tmp_path: Path) -> None:
    payload = _build_payload_with_mutations(
        tmp_path,
        shadow_weights={"technical": 0.50, "fundamental": 0.50},
    )

    _assert_safe_preflight(payload)
    assert payload["preflight_decision"] == "WEIGHT_MISMATCH"
    assert payload["weight_validation"]["shadow_weight_keys_match"] is False


def test_preflight_rejects_proposed_weights_sum_not_one(tmp_path: Path) -> None:
    payload = _build_payload_with_mutations(
        tmp_path,
        proposed_weights={
            "technical": 0.30,
            "fundamental": 0.30,
            "macro": 0.20,
            "policy": 0.15,
            "sentiment": 0.15,
        },
        shadow_weights={
            "technical": 0.30,
            "fundamental": 0.30,
            "macro": 0.20,
            "policy": 0.15,
            "sentiment": 0.15,
        },
    )

    _assert_safe_preflight(payload)
    assert payload["preflight_decision"] == "WEIGHT_MISMATCH"
    assert payload["weight_validation"]["weights_sum_valid"] is False


def test_preflight_rejects_shadow_proposal_weight_mismatch(tmp_path: Path) -> None:
    payload = _build_payload_with_mutations(
        tmp_path,
        shadow_weights={
            "technical": 0.27,
            "fundamental": 0.23,
            "macro": 0.20,
            "policy": 0.15,
            "sentiment": 0.15,
        },
    )

    _assert_safe_preflight(payload)
    assert payload["preflight_decision"] == "WEIGHT_MISMATCH"
    assert payload["weight_validation"]["shadow_matches_proposal"] is False


def test_preflight_rejects_target_profile_path_mismatch(tmp_path: Path) -> None:
    payload = _build_payload_with_mutations(
        tmp_path,
        approval_target_overrides={
            "target_profile_path": str(tmp_path / "config" / "weights" / "other.json"),
        },
    )

    _assert_safe_preflight(payload)
    assert payload["preflight_decision"] == "TARGET_PROFILE_MISMATCH"
    assert "target_profile_path" in payload["target_profile_validation"]["blocking_reasons"]


def test_preflight_passes_and_writes_only_preflight_artifacts(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 20)
    production_path = _write_production_profile(tmp_path)
    production_before = production_path.read_text(encoding="utf-8")
    proposal_path = _write_proposal(context["data_root"], as_of)
    _write_approval(
        context["data_root"],
        as_of,
        proposal_path=proposal_path,
        production_path=production_path,
    )
    _write_current_shadow_weights(context["data_root"])

    payload = write_shadow_promotion_apply_preflight_report(
        as_of=as_of,
        data_root=context["data_root"],
        production_profile_path=production_path,
        generated_at=_fixed_generated_at(),
    )

    _assert_safe_preflight(payload)
    assert payload["preflight_decision"] == "PASS"
    assert production_path.read_text(encoding="utf-8") == production_before
    assert payload["diff_preview"]["changed_weight_keys"] == ["fundamental", "technical"]
    assert Path(payload["outputs"]["json"]).exists()
    assert Path(payload["outputs"]["markdown"]).exists()
    assert Path(payload["outputs"]["run_log_json"]).exists()
    assert Path(payload["outputs"]["run_log_markdown"]).exists()


def test_dashboard_reads_preflight_artifact_without_rerun(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 20)
    production_path = _write_production_profile(tmp_path)
    proposal_path = _write_proposal(context["data_root"], as_of)
    _write_approval(
        context["data_root"],
        as_of,
        proposal_path=proposal_path,
        production_path=production_path,
    )
    _write_current_shadow_weights(context["data_root"])
    preflight = write_shadow_promotion_apply_preflight_report(
        as_of=as_of,
        data_root=context["data_root"],
        production_profile_path=production_path,
        generated_at=_fixed_generated_at(),
    )
    _remove_preflight_inputs(context["data_root"])
    metadata_path = _write_dashboard_metadata(tmp_path, as_of)

    original_import = builtins.__import__

    def guarded_import(
        name: str,
        globals_: dict[str, object] | None = None,
        locals_: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        blocked_module_tokens = (
            "run_daily_shadow_weight_iteration",
            "run_daily_shadow_vs_production_comparison",
            "run_shadow_vs_production_multi_day_review",
            "run_shadow_promotion_proposal",
            "run_shadow_promotion_apply_preflight",
            "ai_trading_system.trading_engine.shadow_promotion_apply_preflight",
            "ai_trading_system.trading_engine.shadow_promotion_proposal",
            "ai_trading_system.trading_engine.brokers",
            "run_paper_trading_replay",
        )
        if any(token in name for token in blocked_module_tokens):
            raise AssertionError(f"dashboard must not import pipeline path: {name}")
        return original_import(name, globals_, locals_, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        run_report_path=None,
        reports_dir=context["reports_dir"],
    )
    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    summary = payload["shadow_promotion_apply_preflight"]
    assert summary["preflight_decision"] == "PASS"
    assert summary["production_effect"] == "none"
    assert summary["manual_review_only"] is True
    assert summary["promotion_executed"] is False
    assert summary["apply_executed"] is False
    assert summary["preflight_only"] is True
    assert summary["changed_weight_keys"] == ["fundamental", "technical"]
    assert summary["latest_preflight_markdown_path"] == preflight["outputs"]["markdown"]
    assert "Shadow Promotion Apply Preflight" in html
    assert "shadow_promotion_apply_preflight_2026-05-20.md" in html
    assert not _proposal_path(context["data_root"], as_of).exists()
    assert not _current_shadow_path(context["data_root"]).exists()


def _build_payload_with_mutations(
    tmp_path: Path,
    *,
    proposal_overrides: dict[str, Any] | None = None,
    approval_overrides: dict[str, Any] | None = None,
    approval_proposal_overrides: dict[str, Any] | None = None,
    approval_target_overrides: dict[str, Any] | None = None,
    safety_ack_overrides: dict[str, Any] | None = None,
    production_weights: dict[str, float] | None = None,
    shadow_weights: dict[str, float] | None = None,
    proposed_weights: dict[str, float] | None = None,
) -> dict[str, Any]:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 20)
    production_path = _write_production_profile(tmp_path, weights=production_weights)
    proposal_path = _write_proposal(
        context["data_root"],
        as_of,
        weights=proposed_weights,
        overrides=proposal_overrides,
    )
    _write_approval(
        context["data_root"],
        as_of,
        proposal_path=proposal_path,
        production_path=production_path,
        approval_overrides=approval_overrides,
        proposal_overrides=approval_proposal_overrides,
        target_overrides=approval_target_overrides,
        safety_ack_overrides=safety_ack_overrides,
    )
    _write_current_shadow_weights(context["data_root"], weights=shadow_weights)
    return build_shadow_promotion_apply_preflight_payload(
        as_of=as_of,
        data_root=context["data_root"],
        production_profile_path=production_path,
        generated_at=_fixed_generated_at(),
    )


def _write_context(tmp_path: Path) -> dict[str, Path]:
    data_root = tmp_path / "data"
    reports_dir = tmp_path / "outputs" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    return {"data_root": data_root, "reports_dir": reports_dir}


def _write_production_profile(
    tmp_path: Path,
    *,
    weights: dict[str, float] | None = None,
) -> Path:
    path = tmp_path / "config" / "weights" / "production_profile.json"
    _write_json(
        path,
        {
            "schema_version": "1.0",
            "profile_name": "production",
            "status": "production",
            "environment": "production",
            "base_weights": weights or _production_weights(),
        },
    )
    return path


def _write_current_shadow_weights(
    data_root: Path,
    *,
    weights: dict[str, float] | None = None,
) -> Path:
    path = _current_shadow_path(data_root)
    _write_json(
        path,
        {
            "schema_version": "1.0",
            "report_type": "current_shadow_weights",
            "mode": "shadow_only",
            "production_effect": "none",
            "manual_review_only": True,
            "last_updated_date": "2026-05-20",
            "weights": weights or _shadow_weights(),
            "audit": {"last_decision": "UPDATE"},
        },
    )
    return path


def _write_proposal(
    data_root: Path,
    as_of: date,
    *,
    weights: dict[str, float] | None = None,
    overrides: dict[str, Any] | None = None,
) -> Path:
    path = _proposal_path(data_root, as_of)
    payload = {
        "schema_version": "1.0",
        "report_type": "shadow_promotion_proposal",
        "task_id": "TRADING-018D",
        "date": as_of.isoformat(),
        "generated_at": _fixed_generated_at().isoformat(),
        "mode": "manual_promotion_proposal_only",
        "production_effect": "none",
        "manual_review_only": True,
        "promotion_proposed": True,
        "promotion_executed": False,
        "safe_for_production": False,
        "proposal_decision": "PROPOSE_FOR_MANUAL_REVIEW",
        "production_weights": _production_weights(),
        "shadow_weights": weights or _shadow_weights(),
        "proposed_production_weights": weights or _shadow_weights(),
        "outputs": {
            "json": str(path),
            "markdown": str(path.with_suffix(".md")),
        },
        "pipeline_contract": _safe_contract(),
    }
    payload.update(overrides or {})
    _write_json(path, payload)
    return path


def _write_approval(
    data_root: Path,
    as_of: date,
    *,
    proposal_path: Path,
    production_path: Path,
    proposal_sha256: str | None = None,
    approval_overrides: dict[str, Any] | None = None,
    proposal_overrides: dict[str, Any] | None = None,
    target_overrides: dict[str, Any] | None = None,
    safety_ack_overrides: dict[str, Any] | None = None,
) -> Path:
    path = data_root / "manual_approvals" / f"shadow_promotion_approval_{as_of.isoformat()}.json"
    proposal_section = {
        "proposal_date": as_of.isoformat(),
        "proposal_file": str(proposal_path),
        "proposal_sha256": proposal_sha256 or _sha256(proposal_path),
        "proposal_decision": "PROPOSE_FOR_MANUAL_REVIEW",
        "promotion_proposed": True,
    }
    proposal_section.update(proposal_overrides or {})
    target = {
        "target_profile_name": "production",
        "target_profile_path": str(production_path),
    }
    target.update(target_overrides or {})
    safety = {
        "preflight_only": True,
        "apply_not_authorized": True,
        "production_modification_not_authorized": True,
    }
    safety.update(safety_ack_overrides or {})
    payload = {
        "schema_version": "1.0",
        "approval_type": "shadow_promotion_apply_preflight",
        "approved": True,
        "approved_by": "manual_user",
        "approved_at": "2026-05-20T00:00:00Z",
        "proposal": proposal_section,
        "target": target,
        "approval_statement": (
            "I manually reviewed the shadow promotion proposal and approve running apply "
            "preflight only. This approval does not authorize production modification."
        ),
        "safety_acknowledgement": safety,
    }
    payload.update(approval_overrides or {})
    _write_json(path, payload)
    return path


def _write_dashboard_metadata(tmp_path: Path, as_of: date) -> Path:
    metadata_path = tmp_path / f"daily_ops_run_metadata_{as_of.isoformat()}.json"
    _write_json(
        metadata_path,
        {
            "run_id": f"daily_ops_run:{as_of.isoformat()}:test",
            "as_of": as_of.isoformat(),
            "generated_at": _fixed_generated_at().isoformat(),
            "project_root": str(tmp_path),
            "status": "PASS",
            "started_at": _fixed_generated_at().isoformat(),
            "finished_at": _fixed_generated_at().isoformat(),
            "visibility_cutoff": "2026-05-20T20:00:00Z",
            "input_visibility_status": "PASS",
            "git": {"commit": "test", "dirty": False},
            "commands": [],
            "step_results": [],
        },
    )
    return metadata_path


def _remove_preflight_inputs(data_root: Path) -> None:
    for path in (
        _proposal_path(data_root, date(2026, 5, 20)),
        data_root / "manual_approvals" / "shadow_promotion_approval_2026-05-20.json",
        _current_shadow_path(data_root),
    ):
        if path.exists():
            path.unlink()


def _proposal_path(data_root: Path, as_of: date) -> Path:
    return (
        data_root
        / "derived"
        / "weight_iterations"
        / "promotion"
        / "proposals"
        / f"shadow_promotion_proposal_{as_of.isoformat()}.json"
    )


def _current_shadow_path(data_root: Path) -> Path:
    return data_root / "derived" / "weight_iterations" / "shadow" / "current_shadow_weights.json"


def _production_weights() -> dict[str, float]:
    return {
        "technical": 0.25,
        "fundamental": 0.25,
        "macro": 0.20,
        "policy": 0.15,
        "sentiment": 0.15,
    }


def _shadow_weights() -> dict[str, float]:
    return {
        "technical": 0.26,
        "fundamental": 0.24,
        "macro": 0.20,
        "policy": 0.15,
        "sentiment": 0.15,
    }


def _safe_contract() -> dict[str, Any]:
    return {
        "runs_shadow_iteration_pipeline": False,
        "runs_comparison_pipeline": False,
        "runs_multi_day_review_pipeline": False,
        "runs_promotion_apply": False,
        "runs_scoring_pipeline": False,
        "runs_broker_runner": False,
        "runs_paper_runner": False,
        "runs_replay_runner": False,
        "writes_production_profile": False,
        "writes_production_weights": False,
        "writes_approved_profile": False,
        "promotes_shadow_to_production": False,
        "changes_daily_dashboard_main_conclusion": False,
        "triggers_trade": False,
        "production_effect": "none",
        "manual_review_only": True,
    }


def _assert_safe_preflight(payload: dict[str, Any]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["manual_review_only"] is True
    assert payload["promotion_executed"] is False
    assert payload["apply_executed"] is False
    assert payload["preflight_only"] is True
    assert payload["safe_for_production"] is False
    assert payload["pipeline_contract"]["runs_scoring_pipeline"] is False
    assert payload["pipeline_contract"]["runs_broker_runner"] is False
    assert payload["pipeline_contract"]["runs_replay_runner"] is False
    assert payload["pipeline_contract"]["writes_production_profile"] is False
    assert payload["pipeline_contract"]["writes_production_weights"] is False
    assert payload["pipeline_contract"]["promotes_shadow_to_production"] is False


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fixed_generated_at() -> datetime:
    return datetime(2026, 5, 20, 8, 0, tzinfo=UTC)
