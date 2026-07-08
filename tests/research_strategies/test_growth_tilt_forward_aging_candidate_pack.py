from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from ai_trading_system import dynamic_strategy_growth_tilt_forward_aging_candidate_pack as impl
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_forward_aging_candidate_pack as pack,
)


def test_forward_aging_candidate_pack_blocks_on_pit_replay_gate() -> None:
    payload = _build_payload(_source_documents())

    assert payload["status"] == pack.BLOCKED_PIT_REPLAY_STATUS
    assert payload["source_2438_ready"] is False
    assert payload["pit_replay_source_status"] == (
        "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP"
    )
    assert payload["data_quality_gate_executed"] is True
    assert payload["data_quality_gate_passed"] is True
    assert payload["data_quality_status"] == "PASS_WITH_WARNINGS"
    assert payload["forward_aging_candidate_pack_ready"] is False
    assert payload["candidate_tracking_artifact_ready"] is True
    assert payload["forward_observation_contract_ready"] is True
    assert payload["no_effect_boundary_ready"] is True
    assert payload["forward_aging_candidate_count"] == 0
    assert payload["forward_aging_candidate_count_if_unblocked"] == 0
    assert payload["observation_horizons"] == list(pack.OBSERVATION_HORIZONS)
    assert payload["valid_until_outcome_capture_ready"] is False
    assert payload["candidate_evidence_refresh_cadence"] == (
        "not_started_pit_replay_gate_blocked"
    )
    assert payload["forward_aging_observation_started"] is False
    assert payload["forward_aging_observation_written"] is False
    assert payload["candidate_tracking_started"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["recommended_next_research_task"] == pack.BLOCKED_ROUTE
    assert "source_2438_top3_pit_replay_ready" in payload["evidence_gap_ids"]
    assert "pit_replay_pass_candidate_available" in payload["evidence_gap_ids"]


def test_forward_aging_candidate_pack_ready_when_pit_replay_passes() -> None:
    sources = _source_documents()
    sources["source_2438_pit_replay"] = _source_2438_ready()
    sources["pit_replay_evidence"] = _pit_replay_evidence_ready()

    payload = _build_payload(sources)

    assert payload["status"] == pack.READY_STATUS
    assert payload["source_2438_ready"] is True
    assert payload["forward_aging_candidate_pack_ready"] is True
    assert payload["forward_aging_candidate_count"] == 1
    assert payload["valid_until_outcome_capture_ready"] is True
    assert payload["candidate_tracking_started"] is True
    assert payload["forward_aging_observation_started"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["recommended_next_research_task"] == pack.NEXT_ROUTE


def test_forward_aging_candidate_pack_blocks_on_data_quality_failure() -> None:
    sources = _source_documents()
    sources["data_quality_summary"] = {
        "data_quality_gate_executed": True,
        "data_quality_gate_passed": False,
        "data_quality_status": "FAIL",
        "data_quality_report_path": "outputs/reports/data_quality_fail.md",
    }

    payload = _build_payload(sources)

    assert payload["status"] == pack.BLOCKED_DATA_QUALITY_STATUS
    assert payload["data_quality_gate_passed"] is False
    assert "data_quality_gate_passed" in payload["evidence_gap_ids"]
    assert payload["forward_aging_candidate_count"] == 0
    assert payload["recommended_next_research_task"] == pack.BLOCKED_ROUTE


def test_forward_aging_candidate_pack_wrapper_writes_outputs(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "forward_aging_pack"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_forward_aging_candidate_pack(
        **_source_kwargs(paths),
        data_quality_summary_path=paths["data_quality_summary"],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == pack.BLOCKED_PIT_REPLAY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_gate_executed"] is True
    assert payload["forward_aging_candidate_count"] == 0
    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    for key in (
        "json_path",
        "forward_aging_candidate_pack_json",
        "candidate_tracking_artifact_json",
        "forward_observation_contract_json",
        "no_effect_boundary_json",
        "markdown_path",
        "forward_aging_candidate_pack_markdown",
        "candidate_tracking_artifact_markdown",
        "forward_observation_contract_markdown",
        "no_effect_boundary_markdown",
        "blocked_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_forward_aging_candidate_pack_cli_deterministic(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "forward_aging_pack_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-forward-aging-candidate-pack",
            *_source_args(paths),
            "--data-quality-summary",
            str(paths["data_quality_summary"]),
            "--as-of",
            "2026-07-08",
            "--output-root",
            str(output_root),
            "--docs-root",
            str(docs_root),
        ],
        env={"COLUMNS": "260"},
        terminal_width=260,
    )

    assert result.exit_code == 0, result.output
    assert pack.BLOCKED_PIT_REPLAY_STATUS in result.output
    assert "source_2438_ready=false" in result.output
    assert "data_quality_gate_executed=true" in result.output
    assert "data_quality_gate_passed=true" in result.output
    assert "data_quality_status=PASS_WITH_WARNINGS" in result.output
    assert "forward_aging_candidate_pack_ready=false" in result.output
    assert "candidate_tracking_artifact_ready=true" in result.output
    assert "forward_observation_contract_ready=true" in result.output
    assert "forward_aging_candidate_count=0" in result.output
    assert "valid_until_outcome_capture_ready=false" in result.output
    assert "candidate_evidence_refresh_cadence=not_started_pit_replay_gate_blocked" in (
        result.output
    )
    assert "forward_aging_observation_started=false" in result.output
    assert "forward_aging_observation_written=false" in result.output
    assert "paper_shadow_enabled=false" in result.output
    assert "production_enabled=false" in result.output
    assert "broker_enabled=false" in result.output
    assert f"next_route={pack.BLOCKED_ROUTE}" in result.output
    assert (output_root / "forward_aging_candidate_pack_result.json").exists()


def test_forward_aging_candidate_pack_missing_source_artifact_blocks(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    paths["source_2438"].unlink()

    payload = impl.run_growth_tilt_forward_aging_candidate_pack(
        **_source_kwargs(paths),
        data_quality_summary_path=paths["data_quality_summary"],
        output_root=tmp_path / "outputs" / "blocked",
        docs_root=tmp_path / "docs" / "research",
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == pack.BLOCKED_EVIDENCE_STATUS
    assert payload["source_validation_error_count"] > 0
    assert payload["source_2438_ready"] is False
    assert payload["forward_aging_candidate_pack_ready"] is False
    assert payload["candidate_tracking_artifact_ready"] is True
    assert payload["forward_observation_contract_ready"] is True
    assert payload["evidence_gap_ids"] == ["source_artifact_availability"]
    assert payload["paper_shadow_enabled"] is False
    assert payload["recommended_next_research_task"] == pack.BLOCKED_ROUTE
    assert Path(payload["artifact_paths"]["candidate_tracking_artifact_json"]).exists()


def test_forward_aging_candidate_pack_registry_catalog_system_flow() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[pack.REPORT_TYPE]

    assert entry["command"] == (
        "aits research strategies growth-tilt-forward-aging-candidate-pack"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "forward_aging_candidate_pack_result.json" in item
        for item in entry["artifact_globs"]
    )
    assert any("dynamic_strategy_2439_blocked_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert pack.REPORT_TYPE in catalog
    assert "growth-tilt-forward-aging-candidate-pack" in system_flow
    assert pack.BLOCKED_PIT_REPLAY_STATUS in system_flow
    assert pack.BLOCKED_ROUTE in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _build_payload(sources: dict[str, Any]) -> dict[str, Any]:
    return pack.build_growth_tilt_forward_aging_candidate_pack(
        sources["source_2438_pit_replay"],
        sources["pit_replay_evidence"],
        sources["pit_replay_blocker_summary"],
        sources["data_quality_summary"],
        report_registry=sources["report_registry"],
        artifact_catalog_text=sources["artifact_catalog_text"],
        system_flow_text=sources["system_flow_text"],
        research_doc_texts=sources["research_doc_texts"],
    )


def _source_documents() -> dict[str, Any]:
    return {
        "source_2438_pit_replay": _source_2438_blocked(),
        "pit_replay_evidence": _pit_replay_evidence_blocked(),
        "pit_replay_blocker_summary": _pit_replay_blocker_summary(),
        "data_quality_summary": _data_quality_summary(),
        "report_registry": {
            "reports": [{"report_id": report_id} for report_id in pack.REQUIRED_REPORT_IDS]
        },
        "artifact_catalog_text": "\n".join(pack.REQUIRED_CATALOG_REFERENCES),
        "system_flow_text": "\n".join(pack.REQUIRED_SYSTEM_FLOW_REFERENCES),
        "research_doc_texts": {
            "pit_replay": "PIT replay forward aging candidate pack",
            "evidence": "PIT replay evidence forward aging",
            "blocker": "PIT replay blocker forward aging",
        },
    }


def _source_2438_blocked() -> dict[str, Any]:
    return {
        "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP",
        "recommended_next_research_task": pack.BLOCKED_ROUTE,
        "pit_candidates_selected": 3,
        "pit_candidates_tested": 0,
        "pit_replay_pass_count": 0,
        "pit_replay_fail_count": 0,
        "pit_replay_blocked_count": 3,
        "promotion_review_candidate_count": 0,
        "pit_replay_executed": False,
        "data_quality_gate_executed": True,
        "data_quality_gate_passed": True,
        "data_quality_status": "PASS_WITH_WARNINGS",
    }


def _source_2438_ready() -> dict[str, Any]:
    return {
        "status": pack.EXPECTED_2438_STATUS,
        "recommended_next_research_task": pack.EXPECTED_2438_NEXT_ROUTE,
        "pit_candidates_selected": 3,
        "pit_candidates_tested": 3,
        "pit_replay_pass_count": 1,
        "pit_replay_fail_count": 2,
        "pit_replay_blocked_count": 0,
        "promotion_review_candidate_count": 1,
        "pit_replay_executed": True,
        "data_quality_gate_executed": True,
        "data_quality_gate_passed": True,
        "data_quality_status": "PASS_WITH_WARNINGS",
    }


def _pit_replay_evidence_blocked() -> dict[str, Any]:
    return {
        "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP",
        "pit_replay_evidence": {"rows": []},
    }


def _pit_replay_evidence_ready() -> dict[str, Any]:
    return {
        "status": pack.EXPECTED_2438_STATUS,
        "pit_replay_evidence": {
            "rows": [
                {
                    "candidate_id": "recovery_reentry_speedup_guard",
                    "pit_replay_passed": True,
                    "primary_value": "missed_upside_reduction",
                    "key_risk": "needs_forward_outcome_confirmation",
                },
                {
                    "candidate_id": "false_risk_off_confirmation_relaxation",
                    "pit_replay_passed": False,
                },
            ]
        },
    }


def _pit_replay_blocker_summary() -> dict[str, Any]:
    return {
        "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP",
        "pit_replay_blocker_summary": {
            "blocked": True,
            "next_route": pack.BLOCKED_ROUTE,
        },
    }


def _data_quality_summary() -> dict[str, Any]:
    return {
        "data_quality_gate_executed": True,
        "data_quality_gate_passed": True,
        "data_quality_status": "PASS_WITH_WARNINGS",
        "data_quality_report_path": "outputs/reports/data_quality_2026-07-08.md",
        "data_quality_as_of": "2026-07-08",
        "data_quality_error_count": 0,
        "data_quality_warning_count": 2,
        "data_quality_info_count": 12,
    }


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    sources = _source_documents()
    root = tmp_path / "sources"
    root.mkdir(parents=True)
    paths = {
        "source_2438": root / "top3_candidate_pit_replay_result.json",
        "pit_replay_evidence": root / "pit_replay_evidence.json",
        "pit_replay_blocker_summary": root / "pit_replay_blocker_summary.json",
        "pit_replay_doc": root / "growth_tilt_top3_candidate_pit_replay.md",
        "pit_replay_evidence_doc": root / "pit_replay_evidence.md",
        "pit_replay_blocker_doc": root / "pit_replay_blocker_summary.md",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
        "system_flow": root / "system_flow.md",
        "data_quality_summary": root / "data_quality_summary.json",
    }
    _write_json(paths["source_2438"], sources["source_2438_pit_replay"])
    _write_json(paths["pit_replay_evidence"], sources["pit_replay_evidence"])
    _write_json(
        paths["pit_replay_blocker_summary"],
        sources["pit_replay_blocker_summary"],
    )
    paths["pit_replay_doc"].write_text(
        "PIT replay forward aging candidate pack",
        encoding="utf-8",
    )
    paths["pit_replay_evidence_doc"].write_text(
        "PIT replay evidence forward aging",
        encoding="utf-8",
    )
    paths["pit_replay_blocker_doc"].write_text(
        "PIT replay blocker forward aging",
        encoding="utf-8",
    )
    paths["report_registry"].write_text(
        "reports:\n"
        + "\n".join(
            f"  - report_id: {report_id}" for report_id in pack.REQUIRED_REPORT_IDS
        )
        + "\n",
        encoding="utf-8",
    )
    paths["artifact_catalog"].write_text(
        sources["artifact_catalog_text"],
        encoding="utf-8",
    )
    paths["system_flow"].write_text(sources["system_flow_text"], encoding="utf-8")
    _write_json(paths["data_quality_summary"], sources["data_quality_summary"])
    return paths


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_2438_pit_replay_path": paths["source_2438"],
        "pit_replay_evidence_path": paths["pit_replay_evidence"],
        "pit_replay_blocker_summary_path": paths["pit_replay_blocker_summary"],
        "pit_replay_doc_path": paths["pit_replay_doc"],
        "pit_replay_evidence_doc_path": paths["pit_replay_evidence_doc"],
        "pit_replay_blocker_doc_path": paths["pit_replay_blocker_doc"],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
        "system_flow_path": paths["system_flow"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    return [
        "--source-2438-pit-replay",
        str(paths["source_2438"]),
        "--pit-replay-evidence",
        str(paths["pit_replay_evidence"]),
        "--pit-replay-blocker-summary",
        str(paths["pit_replay_blocker_summary"]),
        "--pit-replay-doc",
        str(paths["pit_replay_doc"]),
        "--pit-replay-evidence-doc",
        str(paths["pit_replay_evidence_doc"]),
        "--pit-replay-blocker-doc",
        str(paths["pit_replay_blocker_doc"]),
        "--report-registry",
        str(paths["report_registry"]),
        "--artifact-catalog",
        str(paths["artifact_catalog"]),
        "--system-flow",
        str(paths["system_flow"]),
    ]
