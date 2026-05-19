from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_payload,
    build_daily_task_dashboard_report,
    render_daily_task_dashboard,
)
from ai_trading_system.trading_engine.reports.daily_shadow_weight_iteration import (
    build_daily_shadow_weight_iteration_payload,
    write_daily_shadow_weight_iteration_report,
)


def test_first_run_initializes_default_shadow_weights(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 18)

    payload = write_daily_shadow_weight_iteration_report(
        as_of=as_of,
        reports_dir=context["reports_dir"],
        data_root=context["data_root"],
        policy_path=context["policy"],
        production_profile_path=context["production_profile"],
        generated_at=_fixed_generated_at(),
    )

    current_path = (
        context["data_root"] / "derived/weight_iterations/shadow/current_shadow_weights.json"
    )
    current = json.loads(current_path.read_text(encoding="utf-8"))
    _assert_invariants(payload)
    _assert_invariants(current)
    assert payload["decision"] == "INSUFFICIENT_DATA"
    assert current["report_type"] == "current_shadow_weights"
    assert current["initialization_source"] == "production_profile_snapshot"
    assert abs(sum(current["weights"].values()) - 1.0) < 1e-9
    assert current["weights"]["technical"] == 0.25


def test_successful_update_writes_candidate_current_and_history(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 18)
    _write_ready_inputs(context["reports_dir"], as_of)

    payload = write_daily_shadow_weight_iteration_report(
        as_of=as_of,
        reports_dir=context["reports_dir"],
        data_root=context["data_root"],
        policy_path=context["policy"],
        production_profile_path=context["production_profile"],
        generated_at=_fixed_generated_at(),
    )

    outputs = payload["outputs"]
    current = json.loads(Path(outputs["current_shadow_weights"]).read_text(encoding="utf-8"))
    history = json.loads(Path(outputs["history_json"]).read_text(encoding="utf-8"))
    _assert_invariants(payload)
    _assert_invariants(current)
    assert payload["decision"] == "UPDATE"
    assert Path(outputs["candidate_json"]).exists()
    assert Path(outputs["candidate_markdown"]).exists()
    assert Path(outputs["history_json"]).exists()
    assert Path(outputs["history_markdown"]).exists()
    assert payload["run_log"]["current_state_updated"] is True
    assert current["weights"] == history["weights"]
    assert current["audit"]["update_count"] == 1
    assert current["audit"]["last_decision"] == "UPDATE"


def test_missing_artifact_is_insufficient_data_and_does_not_update_existing_current(
    tmp_path: Path,
) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 18)
    _write_current_state(context["data_root"], as_of)
    before = _read_current_text(context["data_root"])
    _write_ready_inputs(context["reports_dir"], as_of, include_trading_017=False)

    payload = write_daily_shadow_weight_iteration_report(
        as_of=as_of,
        reports_dir=context["reports_dir"],
        data_root=context["data_root"],
        policy_path=context["policy"],
        production_profile_path=context["production_profile"],
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["decision"] == "INSUFFICIENT_DATA"
    assert "TRADING-017" in payload["safety_checks"].get("missing_artifacts", []) or any(
        item["label"] == "TRADING-017" and item["status"] == "MISSING"
        for item in payload["input_artifacts"].values()
    )
    assert _read_current_text(context["data_root"]) == before
    assert not Path(payload["outputs"]["history_json"]).exists()


def test_safety_blocked_does_not_update_existing_current(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 18)
    _write_current_state(context["data_root"], as_of)
    before = _read_current_text(context["data_root"])
    _write_ready_inputs(context["reports_dir"], as_of, scheduler_status="BLOCK")

    payload = write_daily_shadow_weight_iteration_report(
        as_of=as_of,
        reports_dir=context["reports_dir"],
        data_root=context["data_root"],
        policy_path=context["policy"],
        production_profile_path=context["production_profile"],
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["decision"] == "SAFETY_BLOCKED"
    assert payload["safety_checks"]["status"] == "BLOCK"
    assert _read_current_text(context["data_root"]) == before


def test_scheduler_report_type_mismatch_is_insufficient_data(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 18)
    _write_current_state(context["data_root"], as_of)
    before = _read_current_text(context["data_root"])
    _write_ready_inputs(context["reports_dir"], as_of, scheduler_report_type="wrong_report_type")

    payload = write_daily_shadow_weight_iteration_report(
        as_of=as_of,
        reports_dir=context["reports_dir"],
        data_root=context["data_root"],
        policy_path=context["policy"],
        production_profile_path=context["production_profile"],
        generated_at=_fixed_generated_at(),
    )

    assert payload["decision"] == "INSUFFICIENT_DATA"
    assert payload["input_artifacts"]["trading_018a"]["expected_report_type"] == (
        "daily_weight_adjustment_scheduler_dry_run"
    )
    assert _read_current_text(context["data_root"]) == before


def test_low_confidence_is_no_update_and_does_not_update_existing_current(
    tmp_path: Path,
) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 18)
    _write_current_state(context["data_root"], as_of)
    before = _read_current_text(context["data_root"])
    _write_ready_inputs(context["reports_dir"], as_of, adjustment_confidence=0.2)

    payload = write_daily_shadow_weight_iteration_report(
        as_of=as_of,
        reports_dir=context["reports_dir"],
        data_root=context["data_root"],
        policy_path=context["policy"],
        production_profile_path=context["production_profile"],
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["decision"] == "NO_UPDATE"
    assert "below minimum" in payload["decision_reason"]
    assert _read_current_text(context["data_root"]) == before


def test_policy_confidence_defaults_drive_update_gate(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 18)
    _write_current_state(context["data_root"], as_of)
    _write_policy(
        context["policy"],
        minimum_adjustment_confidence=0.60,
        ready_for_manual_review_confidence=0.50,
        candidate_promising_confidence=0.40,
    )
    _write_ready_inputs(context["reports_dir"], as_of, adjustment_confidence=None)

    payload = write_daily_shadow_weight_iteration_report(
        as_of=as_of,
        reports_dir=context["reports_dir"],
        data_root=context["data_root"],
        policy_path=context["policy"],
        production_profile_path=context["production_profile"],
        generated_at=_fixed_generated_at(),
    )

    assert payload["decision"] == "NO_UPDATE"
    assert "0.5000" in payload["decision_reason"]


def test_missing_policy_blocks_shadow_update(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 18)
    _write_current_state(context["data_root"], as_of)
    _write_ready_inputs(context["reports_dir"], as_of)

    payload = write_daily_shadow_weight_iteration_report(
        as_of=as_of,
        reports_dir=context["reports_dir"],
        data_root=context["data_root"],
        policy_path=context["policy"].with_name("missing_policy.yaml"),
        production_profile_path=context["production_profile"],
        generated_at=_fixed_generated_at(),
    )

    assert payload["decision"] == "INSUFFICIENT_DATA"
    assert payload["policy"]["policy_file_exists"] is False


def test_delta_clamp_uses_abs_and_relative_limits(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 18)
    _write_current_state(context["data_root"], as_of)
    _write_ready_inputs(
        context["reports_dir"],
        as_of,
        proposed_delta={
            "technical": 0.50,
            "fundamental": -0.50,
            "macro": 0.0,
            "policy": 0.0,
            "sentiment": 0.0,
        },
    )

    payload = build_daily_shadow_weight_iteration_payload(
        as_of=as_of,
        reports_dir=context["reports_dir"],
        data_root=context["data_root"],
        policy_path=context["policy"],
        production_profile_path=context["production_profile"],
        current_state=json.loads(
            (
                context["data_root"]
                / "derived/weight_iterations/shadow/current_shadow_weights.json"
            ).read_text(encoding="utf-8")
        ),
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["decision"] == "UPDATE"
    previous = payload["previous_shadow_weights"]
    delta = payload["proposed_delta"]
    for key, value in delta.items():
        allowed = min(0.02, previous[key] * 0.05)
        assert abs(value) <= allowed + 1e-9
    assert {"technical", "fundamental"}.issubset(
        set(payload["constraints_applied"]["clamped_fields"])
    )


def test_update_normalizes_weights_to_one(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 18)
    _write_current_state(context["data_root"], as_of)
    _write_ready_inputs(
        context["reports_dir"],
        as_of,
        proposed_delta={
            "technical": 0.01,
            "fundamental": 0.0,
            "macro": 0.0,
            "policy": 0.0,
            "sentiment": 0.0,
        },
    )

    payload = write_daily_shadow_weight_iteration_report(
        as_of=as_of,
        reports_dir=context["reports_dir"],
        data_root=context["data_root"],
        policy_path=context["policy"],
        production_profile_path=context["production_profile"],
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["decision"] == "UPDATE"
    assert abs(sum(payload["new_shadow_weights"].values()) - 1.0) < 1e-9
    assert payload["constraints_applied"]["normalization_applied"] is True


def test_dashboard_reads_shadow_weight_iteration_without_rerun(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    as_of = date(2026, 5, 18)
    _write_ready_inputs(context["reports_dir"], as_of)
    write_daily_shadow_weight_iteration_report(
        as_of=as_of,
        reports_dir=context["reports_dir"],
        data_root=context["data_root"],
        policy_path=context["policy"],
        production_profile_path=context["production_profile"],
        generated_at=_fixed_generated_at(),
    )
    _remove_shadow_inputs(context["reports_dir"], as_of)
    metadata_path = _write_dashboard_metadata(tmp_path, as_of)

    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        run_report_path=None,
        reports_dir=context["reports_dir"],
    )
    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    summary = payload["shadow_weight_iteration"]
    assert summary["decision"] == "UPDATE"
    assert summary["production_effect"] == "none"
    assert summary["manual_review_only"] is True
    assert summary["current_weights"]["technical"] > 0.25
    assert "Shadow Weight Iteration" in html
    assert "shadow_weight_candidate_2026-05-18.md" in html


def _write_context(tmp_path: Path) -> dict[str, Path]:
    reports_dir = tmp_path / "outputs" / "reports"
    data_root = tmp_path / "data"
    config_dir = tmp_path / "config"
    weights_dir = config_dir / "weights"
    weights_dir.mkdir(parents=True, exist_ok=True)
    policy = config_dir / "daily_shadow_weight_iteration_policy.yaml"
    _write_policy(policy)
    production_profile = weights_dir / "weight_profile_current.yaml"
    production_profile.write_text(
        "\n".join(
            [
                'version: "test-production"',
                'status: "production"',
                "base_weights:",
                "  technical: 0.25",
                "  fundamental: 0.25",
                "  macro: 0.20",
                "  policy: 0.15",
                "  sentiment: 0.15",
                "",
            ]
        ),
        encoding="utf-8",
    )
    reports_dir.mkdir(parents=True, exist_ok=True)
    return {
        "reports_dir": reports_dir,
        "data_root": data_root,
        "policy": policy,
        "production_profile": production_profile,
    }


def _write_ready_inputs(
    reports_dir: Path,
    as_of: date,
    *,
    include_trading_017: bool = True,
    scheduler_status: str = "PASS",
    adjustment_confidence: float | None = 0.8,
    scheduler_report_type: str = "daily_weight_adjustment_scheduler_dry_run",
    proposed_delta: dict[str, float] | None = None,
) -> None:
    suffix = as_of.isoformat()
    candidate_id = f"weight_adjustment_candidate:{suffix}:shadow"
    delta = proposed_delta or {
        "technical": 0.04,
        "fundamental": -0.04,
        "macro": 0.0,
        "policy": 0.0,
        "sentiment": 0.0,
    }
    _write_json(
        reports_dir / f"weight_adjustment_candidates_{suffix}.json",
        {
            "schema_version": 1,
            "report_type": "weight_adjustment_candidates",
            "as_of": suffix,
            "mode": "observe_only",
            "production_effect": "none",
            "candidate_count": 1,
            "top_candidate_id": candidate_id,
            "outputs": {
                "json": str(reports_dir / f"weight_adjustment_candidates_{suffix}.json"),
                "markdown": str(reports_dir / f"weight_adjustment_candidates_{suffix}.md"),
            },
            "candidates": [
                {
                    "candidate_id": candidate_id,
                    "parameter_changes": [
                        {"parameter_id": f"base_weights.{key}", "delta": value}
                        for key, value in delta.items()
                    ],
                    "production_effect": "none",
                }
            ],
        },
    )
    _write_json(
        reports_dir / f"weight_candidate_evaluation_{suffix}.json",
        {
            "schema_version": 1,
            "report_type": "weight_candidate_evaluation",
            "as_of": suffix,
            "evaluation_status": "CANDIDATE_PROMISING_BUT_LIMITED",
            "evaluation_mode": "observe_only",
            "production_effect": "none",
        },
    )
    if include_trading_017:
        _write_json(
            reports_dir / f"weight_promotion_gate_{suffix}.json",
            {
                "schema_version": 1,
                "report_type": "weight_promotion_gate",
                "as_of": suffix,
                "promotion_gate_status": "READY_FOR_MANUAL_REVIEW",
                "gate_mode": "manual_review_only",
                "production_effect": "none",
                "summary": {
                    "promotion_gate_status": "READY_FOR_MANUAL_REVIEW",
                    "ready_for_manual_review_count": 1,
                    "main_blocked_by": "none",
                },
            },
        )
    _write_json(
        reports_dir / f"daily_weight_adjustment_summary_{suffix}.json",
        {
            "schema_version": 1,
            "report_type": "daily_weight_adjustment_summary",
            "as_of": suffix,
            "status": "OBSERVE_ONLY",
            "mode": "observe_only",
            "production_effect": "none",
            "manual_review_only": True,
            "promotion_gate_status": "READY_FOR_MANUAL_REVIEW",
            "ready_for_manual_review_count": 1,
            "top_candidate_id": candidate_id,
            "main_blocked_by": "none",
            "proposed_delta": delta,
        },
    )
    if adjustment_confidence is not None:
        summary_path = reports_dir / f"daily_weight_adjustment_summary_{suffix}.json"
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        summary["adjustment_confidence"] = adjustment_confidence
        _write_json(summary_path, summary)
    _write_json(
        reports_dir / f"daily_weight_adjustment_scheduler_dry_run_{suffix}.json",
        {
            "schema_version": 1,
            "report_type": scheduler_report_type,
            "as_of": suffix,
            "status": scheduler_status,
            "scheduler_dry_run_status": scheduler_status,
            "production_effect": "none",
            "manual_review_only": True,
            "safety_checks": {
                "status": scheduler_status,
                "scheduler_dry_run_status": scheduler_status,
                "blocking_reasons": [] if scheduler_status == "PASS" else ["test_block"],
            },
        },
    )


def _write_policy(
    path: Path,
    *,
    minimum_adjustment_confidence: float = 0.60,
    ready_for_manual_review_confidence: float = 0.80,
    candidate_promising_confidence: float = 0.70,
) -> None:
    path.write_text(
        "\n".join(
            [
                "policy_id: daily_shadow_weight_iteration_policy",
                "version: 1",
                "status: pilot_baseline",
                "owner: system",
                "production_effect: none",
                "thresholds:",
                "  max_abs_delta_per_day: 0.02",
                "  max_relative_delta_per_day: 0.05",
                f"  minimum_adjustment_confidence: {minimum_adjustment_confidence:.2f}",
                "  target_total_weight_sum: 1.0",
                "  total_weight_tolerance: 0.000001",
                "  min_weight: 0.0",
                "  max_weight: 1.0",
                "confidence_defaults:",
                f"  ready_for_manual_review: {ready_for_manual_review_confidence:.2f}",
                f"  candidate_promising_but_limited: {candidate_promising_confidence:.2f}",
                "conservative_default_weights:",
                "  technical: 0.25",
                "  fundamental: 0.25",
                "  macro: 0.20",
                "  policy: 0.15",
                "  sentiment: 0.15",
                "",
            ]
        ),
        encoding="utf-8",
    )


def _write_current_state(data_root: Path, as_of: date) -> None:
    path = data_root / "derived/weight_iterations/shadow/current_shadow_weights.json"
    _write_json(
        path,
        {
            "schema_version": "1.0",
            "report_type": "current_shadow_weights",
            "mode": "shadow_only",
            "production_effect": "none",
            "manual_review_only": True,
            "last_updated_date": as_of.isoformat(),
            "initialization_source": "test",
            "source": {"created_by": "test", "based_on_candidate": ""},
            "weights": {
                "technical": 0.25,
                "fundamental": 0.25,
                "macro": 0.20,
                "policy": 0.15,
                "sentiment": 0.15,
            },
            "constraints": {
                "max_abs_delta_per_day": 0.02,
                "max_relative_delta_per_day": 0.05,
                "normalization_required": True,
                "min_weight": 0.0,
                "max_weight": 1.0,
            },
            "audit": {"update_count": 0, "last_decision": "INITIALIZED"},
        },
    )


def _read_current_text(data_root: Path) -> str:
    path = data_root / "derived/weight_iterations/shadow/current_shadow_weights.json"
    return path.read_text(encoding="utf-8")


def _remove_shadow_inputs(reports_dir: Path, as_of: date) -> None:
    suffix = as_of.isoformat()
    for prefix in (
        "weight_adjustment_candidates",
        "weight_candidate_evaluation",
        "weight_promotion_gate",
        "daily_weight_adjustment_summary",
        "daily_weight_adjustment_scheduler_dry_run",
    ):
        path = reports_dir / f"{prefix}_{suffix}.json"
        if path.exists():
            path.unlink()


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
            "visibility_cutoff": "2026-05-18T20:00:00Z",
            "input_visibility_status": "PASS",
            "git": {"commit": "test", "dirty": False},
            "commands": [],
            "step_results": [],
        },
    )
    return metadata_path


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _assert_invariants(payload: dict[str, Any]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["manual_review_only"] is True


def _fixed_generated_at() -> datetime:
    return datetime(2026, 5, 18, 22, 0, tzinfo=UTC)
