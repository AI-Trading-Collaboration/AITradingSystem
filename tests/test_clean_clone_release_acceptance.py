from __future__ import annotations

import importlib.util
import json
from datetime import date
from pathlib import Path
from types import ModuleType

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_sample_project_writes_minimal_auditable_inputs(tmp_path: Path) -> None:
    module = _load_module()
    sample_root = tmp_path / "sample_project"

    module._write_sample_project(sample_root, date(2026, 6, 19))

    registry = yaml.safe_load(
        (sample_root / "config" / "report_registry.yaml").read_text(encoding="utf-8")
    )
    waivers = yaml.safe_load(
        (sample_root / "config" / "report_index_visibility_waivers.yaml").read_text(
            encoding="utf-8"
        )
    )
    gate = json.loads(
        (
            sample_root
            / "outputs"
            / "reports"
            / "candidate_v2_research_gate_2026-06-19.json"
        ).read_text(encoding="utf-8")
    )

    report_ids = {item["report_id"] for item in registry["reports"]}
    assert {"engineering_surface_inventory", "candidate_v2_research_gate"} <= report_ids
    assert waivers["waivers"] == []
    assert gate["production_effect"] == "none"
    assert gate["reader_brief"]["safety_boundary"]
    assert "clean clone acceptance" in (
        sample_root / "docs" / "artifact_catalog.md"
    ).read_text(encoding="utf-8")


def test_markdown_renders_blocked_status_and_safety_boundary() -> None:
    module = _load_module()
    payload = {
        "as_of": "2026-06-19",
        "release_acceptance_status": module.BLOCKED_DIRTY_STATUS,
        "checkout_mode": "working_tree_snapshot",
        "production_effect": "none",
        "summary": {
            "step_count": 1,
            "failed_step_count": 0,
            "blocking_issue_count": 1,
            "clean_clone_verified": False,
        },
        "blocking_issues": [
            {
                "issue_id": "dirty_snapshot_is_not_release_clone",
                "message": "snapshot is not release evidence",
                "recommended_action": "commit_and_rerun_clean_clone",
            }
        ],
        "steps": [
            {
                "step_id": "sample_system_status",
                "status": "PASS",
                "exit_code": 0,
                "elapsed_seconds": 1.25,
            }
        ],
    }

    markdown = module._render_markdown(payload)

    assert module.BLOCKED_DIRTY_STATUS in markdown
    assert "dirty_snapshot_is_not_release_clone" in markdown
    assert "sample_system_status" in markdown
    assert "official target weights" in markdown
    assert "broker/order" in markdown


def _load_module() -> ModuleType:
    path = PROJECT_ROOT / "scripts" / "run_clean_clone_release_acceptance.py"
    spec = importlib.util.spec_from_file_location("run_clean_clone_release_acceptance", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
