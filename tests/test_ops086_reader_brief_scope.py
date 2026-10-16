"""OPS-086-09 (step 5b): the daily Reader Brief carries no research sections.

`aits reports reader-brief` takes `--scope all|daily` (default `all`, which is what it did before).
In the daily scope the 29 research-only sections are not built into the payload, are not rendered
into the HTML, and the Owner Daily Brief leaves out `research_review_due`. The daily plan, the
schedule and the daily-run finalization use the daily scope. Nothing else in the brief changes.
"""

from __future__ import annotations

import ast
import json
import re
from datetime import date
from pathlib import Path
from typing import Any

import pytest
import yaml
from test_reader_brief import _write_reader_brief_inputs
from typer.testing import CliRunner

from ai_trading_system import cli_direct
from ai_trading_system.cli import app
from ai_trading_system.cli_commands import ops as ops_cli
from ai_trading_system.ops_daily import build_daily_ops_plan
from ai_trading_system.platform.reporting.owner_daily import (
    build_owner_daily_brief_view_model,
    write_owner_daily_brief_sidecars,
)
from ai_trading_system.reports import reader_brief as reader_brief_module
from ai_trading_system.reports.reader_brief import (
    RESEARCH_ONLY_SECTIONS,
    build_reader_brief_payload,
    build_reader_brief_quality_payload,
    render_reader_brief_html,
    write_reader_brief_html,
    write_reader_brief_json,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.scheduled_tasks import load_scheduled_tasks_config

AS_OF = date(2026, 5, 4)
REPO_ROOT = Path(__file__).resolve().parents[1]

# The 29 sections the owner decided to drop from the daily brief (2026-10-16): payload key -> HTML
# title. Pinned here on purpose; a change to the constant changes this list in the same commit.
EXPECTED_SECTIONS = {
    "report_index_waiver_inventory": "Report Index Waiver Inventory",
    "reader_brief_consistency": "Reader Brief Consistency Pack",
    "production_boundary_static_scan": "Production Boundary Static Scan",
    "owner_review_template_v2": "Owner Review Template V2",
    "owner_decision_audit_log": "Owner Decision Audit Log",
    "research_monthly_review_pack": "Research Monthly Review Pack",
    "paper_shadow_promotion_board": "Paper Shadow Promotion Board",
    "candidate_rejection_postmortem": "Candidate Rejection Postmortem Template",
    "decision_snapshot_lifecycle_policy": "Decision Snapshot Lifecycle Policy",
    "extended_shadow_observation_clock": "Extended Shadow Observation Clock",
    "extended_shadow_protocol": "Extended Shadow Protocol",
    "research_roadmap_dashboard": "Research Roadmap Dashboard",
    "research_governance_end_to_end_pack": "Research Governance End-to-End Pack",
    "research_governance_recovery_pack": "Research Governance Recovery Pack",
    "decision_stage_governance_snapshot": "Decision-Stage Governance Snapshot",
    "return_to_research_governance_snapshot": "Return-To-Research Governance Snapshot",
    "next_research_cycle_snapshot": "Next Research Cycle Snapshot",
    "next_candidate_backfill": "Next Candidate Backfill",
    "next_candidate_stress_cost_benchmark": "Next Candidate Stress Cost Benchmark",
    "next_candidate_vs_returned_comparison": "Next Candidate Vs Returned Comparison",
    "next_candidate_signal_window_sensitivity": "Next Candidate Signal Window Sensitivity",
    "next_candidate_research_gate": "Next Candidate Research Gate",
    "next_candidate_owner_research_review_packet": "Next Candidate Owner Research Review Packet",
    "executable_binding_contract": "Executable Binding Contract",
    "executable_signal_binding": "Executable Signal Binding",
    "executable_research_weight_binding": "Executable Research Weight Binding",
    "executable_binding_safety_audit": "Executable Binding Safety Audit",
    "research_safety_boundary_audit": "Research Safety Boundary Audit",
    "task_register_consistency": "Task Register Consistency",
}
SCOPE_ONLY_KEYS = {"generated_at", "registry_scope", "omitted_sections"}


# --- fixtures ------------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def world(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    root = tmp_path_factory.mktemp("reader_brief_scope")
    inputs = _write_reader_brief_inputs(root)
    daily_ids = {
        entry["report_id"]
        for entry in load_report_registry(DEFAULT_REPORT_REGISTRY_PATH, scope="daily")["reports"]
    }
    mixed = json.loads(inputs["report_index"].read_text(encoding="utf-8"))
    kept = [item for item in mixed["reports"] if item.get("report_id") in daily_ids]
    # An index that was written for the daily scope already, with counts the file itself claims.
    daily_index = {
        **mixed,
        "registry": {"scope": "daily"},
        "reports": kept,
        "summary": {**mixed["summary"], "report_count": 777},
    }
    daily_index_path = root / "daily_scope" / "report_index_2026-05-04.json"
    daily_index_path.parent.mkdir()
    daily_index_path.write_text(json.dumps(daily_index), encoding="utf-8")
    return {
        "root": root,
        "inputs": inputs,
        "daily_ids": daily_ids,
        "mixed_index": inputs["report_index"],
        "daily_index": daily_index_path,
    }


def _build(world: dict[str, Any], *, index: Path | None = None, **extra: Any) -> dict[str, Any]:
    inputs = world["inputs"]
    return build_reader_brief_payload(
        as_of=AS_OF,
        reports_dir=world["root"],
        decision_snapshot_path=inputs["snapshot"],
        calculation_explainers_path=inputs["calculation_explainers"],
        daily_decision_summary_path=inputs["daily_decision_summary"],
        evidence_dashboard_json_path=inputs["evidence_dashboard"],
        daily_task_dashboard_json_path=inputs["daily_task_dashboard"],
        daily_report_path=inputs["daily_report"],
        trace_bundle_path=inputs["trace_bundle"],
        score_change_attribution_path=inputs["score_change_attribution"],
        market_panel_path=inputs["market_panel"],
        research_governance_summary_path=inputs["research_governance_summary"],
        report_index_path=index if index is not None else world["mixed_index"],
        documentation_contract_path=inputs["documentation_contract"],
        **extra,
    )


@pytest.fixture(scope="module")
def whole(world: dict[str, Any]) -> dict[str, Any]:
    return _build(world, index=world["daily_index"])


@pytest.fixture(scope="module")
def daily(world: dict[str, Any]) -> dict[str, Any]:
    return _build(world, index=world["daily_index"], registry_scope="daily")


def _titles(html: str) -> list[str]:
    return re.findall(r"<section><h2>(.*?)</h2>", html)


# --- the data ------------------------------------------------------------------------------------


def test_the_omitted_sections_are_the_29_the_owner_named() -> None:
    assert RESEARCH_ONLY_SECTIONS == EXPECTED_SECTIONS
    assert len(RESEARCH_ONLY_SECTIONS) == 29
    assert len(set(RESEARCH_ONLY_SECTIONS.values())) == 29


def test_every_omitted_section_reads_research_scope_reports_only() -> None:
    registry = yaml.safe_load(DEFAULT_REPORT_REGISTRY_PATH.read_text(encoding="utf-8"))["reports"]
    scope = {entry["report_id"]: entry["scope"] for entry in registry}
    source = Path(reader_brief_module.__file__).read_text(encoding="utf-8")
    functions = {
        node.name: node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef)
    }

    for key in RESEARCH_ONLY_SECTIONS:
        function = functions[f"_{key}_summary"]
        read = {
            argument.value
            for call in ast.walk(function)
            if isinstance(call, ast.Call)
            and getattr(call.func, "id", "") == "_report_index_artifact_path"
            for argument in call.args[1:]
            if isinstance(argument, ast.Constant) and isinstance(argument.value, str)
        }
        assert read, key
        # If one of these ever gets a daily producer, it has to leave the omitted list.
        assert {scope[report_id] for report_id in read} == {"research"}, (key, read)


# --- the payload ---------------------------------------------------------------------------------


def test_the_whole_scope_payload_is_what_it_was(world: dict[str, Any]) -> None:
    for payload in (_build(world), _build(world, registry_scope=None)):
        assert payload["registry_scope"] == "all"
        assert "omitted_sections" not in payload
        assert set(EXPECTED_SECTIONS) <= set(payload)
        assert payload["report_index_summary"]["report_count"] == 4
    # The mixed index is used as it is: its research records still show up in the navigation.
    ids = {item["artifact_id"] for item in _build(world)["report_navigation"]}
    assert "etf_portfolio_brief" in ids


def test_the_daily_payload_is_the_whole_payload_without_the_29_sections(
    whole: dict[str, Any], daily: dict[str, Any]
) -> None:
    assert daily["registry_scope"] == "daily"
    assert daily["omitted_sections"] == list(RESEARCH_ONLY_SECTIONS)
    assert set(daily) == (set(whole) - set(EXPECTED_SECTIONS)) | {"omitted_sections"}
    for key in set(whole) - set(EXPECTED_SECTIONS) - SCOPE_ONLY_KEYS:
        assert daily[key] == whole[key], key
    assert daily["status"] == whole["status"]


def test_daily_scope_trims_an_index_written_for_the_whole_registry(
    world: dict[str, Any],
) -> None:
    whole = _build(world)
    daily = _build(world, registry_scope="daily")

    assert whole["report_index_summary"]["report_count"] == 4
    summary = daily["report_index_summary"]
    # The fixture index lists daily_score (fresh) and data_quality (missing, required) as daily
    # entries; everything else in it is a research entry.
    assert (summary["report_count"], summary["missing_count"], summary["stale_count"]) == (2, 1, 0)
    assert summary["required_missing_count"] == 1
    assert [item["report_id"] for item in summary["problem_reports"]] == ["data_quality"]
    daily_navigation = {item["artifact_id"] for item in daily["report_navigation"]}
    whole_navigation = {item["artifact_id"] for item in whole["report_navigation"]}
    research_navigation = {name for name in whole_navigation if name.startswith("etf_")}
    assert research_navigation
    assert not daily_navigation & research_navigation
    assert {"daily_score", "report_index", "artifact_catalog"} <= daily_navigation
    calendar_ids = {
        report["report_id"]
        for group in daily["task_cadence_calendar"]["groups"]
        for report in group["reports"]
    }
    assert calendar_ids == {"daily_score", "data_quality"}


def test_daily_scope_recounts_the_four_counts_of_a_whole_registry_index(
    world: dict[str, Any],
) -> None:
    def record(report_id: str, status: str, **extra: Any) -> dict[str, Any]:
        return {
            "report_id": report_id,
            "title": report_id,
            "cadence": "daily",
            "owner": "system",
            "freshness_status": status,
            "artifact_date": "",
            "latest_artifact_path": "",
            "exists": False,
            "owner_action": "regenerate",
            "production_effect": "none",
            **extra,
        }

    records = [
        record("daily_score", "STALE"),
        record("data_quality", "MISSING", required_for_daily_reading=True),
        record("market_panel", "MISSING"),
        record("reader_brief", "FRESH"),
        record("etf_portfolio_brief", "MISSING", required_for_daily_reading=True),
        record("etf_forward_dashboard", "STALE"),
    ]
    assert {item["report_id"] for item in records[:4]} <= world["daily_ids"]
    assert not {item["report_id"] for item in records[4:]} & world["daily_ids"]
    path = world["root"] / "whole_registry" / "report_index_2026-05-04.json"
    path.parent.mkdir()
    # The counts in the file are the whole registry's; none of them is the daily answer.
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "report_index",
                "as_of": "2026-05-04",
                "status": "PASS_WITH_WARNINGS",
                "production_effect": "none",
                "registry": {"scope": "all"},
                "summary": {
                    "report_count": 6,
                    "missing_count": 3,
                    "stale_count": 2,
                    "required_missing_count": 2,
                },
                "reports": records,
            }
        ),
        encoding="utf-8",
    )

    whole = _build(world, index=path)["report_index_summary"]
    daily = _build(world, index=path, registry_scope="daily")["report_index_summary"]

    assert (whole["report_count"], whole["missing_count"], whole["stale_count"]) == (6, 3, 2)
    assert whole["required_missing_count"] == 2
    assert (daily["report_count"], daily["missing_count"], daily["stale_count"]) == (4, 2, 1)
    assert daily["required_missing_count"] == 1
    assert [item["report_id"] for item in daily["problem_reports"]] == [
        "daily_score",
        "data_quality",
        "market_panel",
    ]


def test_daily_scope_leaves_an_index_that_is_already_daily_alone(
    world: dict[str, Any], daily: dict[str, Any]
) -> None:
    # The file says report_count=777; a daily index is not recounted.
    assert daily["report_index_summary"]["report_count"] == 777
    assert daily["report_index_summary"]["missing_count"] == 1


def test_daily_scope_without_an_index_falls_back_to_the_daily_registry(
    world: dict[str, Any],
) -> None:
    missing = world["root"] / "no_such_index.json"
    for scope in (None, "daily"):
        calendar = _build(world, index=missing, registry_scope=scope)["task_cadence_calendar"]
        assert calendar["source"] == "registry_fallback"
        report_ids = {
            report["report_id"] for group in calendar["groups"] for report in group["reports"]
        }
        if scope == "daily":
            assert report_ids
            assert report_ids <= world["daily_ids"]
        else:
            assert report_ids - world["daily_ids"]


@pytest.mark.parametrize("scope", ["research", "weekly", ""])
def test_the_builder_supports_the_whole_and_the_daily_scope_only(
    world: dict[str, Any], scope: str
) -> None:
    with pytest.raises(ValueError, match="supports --scope all or daily"):
        _build(world, registry_scope=scope)


def test_the_validators_see_the_same_brief_in_both_scopes(
    world: dict[str, Any], whole: dict[str, Any], daily: dict[str, Any], tmp_path: Path
) -> None:
    results = {}
    for name, payload in (("whole", whole), ("daily", daily)):
        json_path = write_reader_brief_json(payload, tmp_path / name / "reader_brief.json")
        html_path = write_reader_brief_html(payload, tmp_path / name / "reader_brief.html")
        results[name] = build_reader_brief_quality_payload(
            reader_brief_payload=payload,
            reader_brief_json_path=json_path,
            reader_brief_html_path=html_path,
        )

    assert results["daily"]["status"] == results["whole"]["status"]
    assert results["daily"]["summary"] == results["whole"]["summary"]
    statuses = {
        name: [(check["check_id"], check["status"]) for check in result["checks"]]
        for name, result in results.items()
    }
    assert statuses["daily"] == statuses["whole"]
    assert results["daily"]["status"] != "FAILED"


# --- the HTML ------------------------------------------------------------------------------------


def test_the_whole_html_has_all_29_sections_and_the_daily_html_none_of_them(
    whole: dict[str, Any], daily: dict[str, Any]
) -> None:
    whole_titles = _titles(render_reader_brief_html(whole))
    daily_titles = _titles(render_reader_brief_html(daily))

    assert set(EXPECTED_SECTIONS.values()) <= set(whole_titles)
    assert not set(EXPECTED_SECTIONS.values()) & set(daily_titles)
    assert len(whole_titles) - len(daily_titles) == 29
    # Every other section is still there, in the same order.
    assert daily_titles == [t for t in whole_titles if t not in set(EXPECTED_SECTIONS.values())]
    assert {"Executive Summary", "Report Navigation", "Manual Review Queue"} <= set(daily_titles)


def test_a_payload_without_a_scope_field_renders_as_the_whole_brief(whole: dict[str, Any]) -> None:
    older = {key: value for key, value in whole.items() if key != "registry_scope"}

    assert render_reader_brief_html(older) == render_reader_brief_html(whole)


# --- the Owner Daily Brief -----------------------------------------------------------------------


def test_the_owner_brief_has_nine_sections_in_the_daily_scope(
    whole: dict[str, Any], daily: dict[str, Any]
) -> None:
    whole_view = build_owner_daily_brief_view_model(whole)
    daily_view = build_owner_daily_brief_view_model(daily)
    older = {key: value for key, value in whole.items() if key != "registry_scope"}

    whole_ids = [section.section_id for section in whole_view.sections]
    assert len(whole_ids) == 10
    assert "research_review_due" in whole_ids
    assert [s.section_id for s in daily_view.sections] == [
        name for name in whole_ids if name != "research_review_due"
    ]
    assert len(build_owner_daily_brief_view_model(older).sections) == 10
    kept = {s.section_id: s.to_dict() for s in daily_view.sections}
    for section in whole_view.sections:
        if section.section_id != "research_review_due":
            assert kept[section.section_id] == section.to_dict()


def test_the_owner_sidecars_follow_the_scope_of_the_payload(
    whole: dict[str, Any], daily: dict[str, Any], tmp_path: Path
) -> None:
    for name, payload, expected in (("daily", daily, 9), ("whole", whole, 10)):
        json_path, html_path = write_owner_daily_brief_sidecars(payload, output_dir=tmp_path / name)
        sections = json.loads(json_path.read_text(encoding="utf-8"))["sections"]
        assert len(sections) == expected
        assert html_path.read_text(encoding="utf-8").count('data-section-id="') == expected


# --- the commands --------------------------------------------------------------------------------


def _invoke_reader_brief(world: dict[str, Any], out_dir: Path, *extra: str):
    inputs = world["inputs"]
    out_dir.mkdir(parents=True, exist_ok=True)
    return CliRunner().invoke(
        app,
        [
            "reports",
            "reader-brief",
            "--as-of",
            "2026-05-04",
            "--reports-dir",
            str(out_dir),
            "--decision-snapshot-path",
            str(inputs["snapshot"]),
            "--calculation-explainers-path",
            str(inputs["calculation_explainers"]),
            "--daily-decision-summary-path",
            str(inputs["daily_decision_summary"]),
            "--evidence-dashboard-json-path",
            str(inputs["evidence_dashboard"]),
            "--daily-task-dashboard-json-path",
            str(inputs["daily_task_dashboard"]),
            "--daily-report-path",
            str(inputs["daily_report"]),
            "--trace-bundle-path",
            str(inputs["trace_bundle"]),
            "--score-change-attribution-path",
            str(inputs["score_change_attribution"]),
            "--market-panel-path",
            str(inputs["market_panel"]),
            "--research-governance-summary-path",
            str(inputs["research_governance_summary"]),
            "--report-index-path",
            str(inputs["report_index"]),
            "--documentation-contract-path",
            str(inputs["documentation_contract"]),
            *extra,
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )


def test_the_command_writes_the_scope_it_was_given(world: dict[str, Any], tmp_path: Path) -> None:
    for extra, scope, owner_sections in (
        (("--scope", "daily"), "daily", 9),
        ((), "all", 10),
        (("--scope", "all"), "all", 10),
    ):
        out_dir = tmp_path / f"{scope}_{len(extra)}"
        result = _invoke_reader_brief(world, out_dir, *extra)
        assert result.exit_code == 0, result.output
        written = json.loads((out_dir / "reader_brief_2026-05-04.json").read_text("utf-8"))
        assert written["registry_scope"] == scope
        assert (set(EXPECTED_SECTIONS) <= set(written)) == (scope == "all")
        owner = json.loads((out_dir / "owner_daily_brief_2026-05-04.json").read_text("utf-8"))
        assert len(owner["sections"]) == owner_sections
        html = (out_dir / "reader_brief_2026-05-04.html").read_text(encoding="utf-8")
        assert (set(EXPECTED_SECTIONS.values()) <= set(_titles(html))) == (scope == "all")
        assert (not set(EXPECTED_SECTIONS.values()) & set(_titles(html))) == (scope == "daily")


@pytest.mark.parametrize("scope", ["research", "weekly"])
def test_the_command_rejects_other_scopes(
    world: dict[str, Any], tmp_path: Path, scope: str
) -> None:
    result = _invoke_reader_brief(world, tmp_path / scope, "--scope", scope)

    assert result.exit_code == 2, result.output
    assert not (tmp_path / scope / "reader_brief_2026-05-04.json").exists()


def test_the_daily_dispatcher_passes_scope_to_reader_brief(monkeypatch) -> None:
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        cli_direct.reports_cli, "reader_brief_command", lambda **kwargs: calls.append(kwargs)
    )

    assert cli_direct.main(["reports", "reader-brief", "--latest", "--scope", "daily"]) == 0
    assert cli_direct.main(["reports", "reader-brief", "--latest"]) == 0
    assert cli_direct.main(["reports", "reader-brief", "--date", "2026-05-13"]) == 0

    assert calls == [
        {"as_of": None, "latest": True, "scope": "daily"},
        {"as_of": None, "latest": True},
        {"as_of": "2026-05-13", "latest": False},
    ]


def test_the_daily_run_finalization_builds_the_daily_brief(monkeypatch, tmp_path: Path) -> None:
    seen: dict[str, object] = {}

    class Stop(Exception):
        pass

    def spy(**kwargs: object) -> None:
        seen.update(kwargs)
        raise Stop

    monkeypatch.setattr(ops_cli, "build_reader_brief_payload", spy)

    with pytest.raises(Stop):
        ops_cli._refresh_reader_brief_from_daily_run_summary(
            as_of=AS_OF,
            reports_dir=tmp_path,
            trace_bundle_path=tmp_path / "trace.json",
            daily_decision_summary_path=tmp_path / "summary.json",
            daily_task_dashboard_json_path=tmp_path / "dashboard.json",
        )

    assert seen["registry_scope"] == "daily"


# --- the plan and the schedule -------------------------------------------------------------------


def test_the_daily_plan_and_schedule_run_the_reader_brief_in_the_daily_scope(
    tmp_path: Path,
) -> None:
    plan = build_daily_ops_plan(as_of=date(2026, 10, 14), project_root=tmp_path)
    steps = {step.step_id: step for step in plan.steps}
    tasks = load_scheduled_tasks_config().tasks_by_id()

    assert steps["reader_brief"].command == (
        "aits",
        "reports",
        "reader-brief",
        "--latest",
        "--scope",
        "daily",
    )
    assert tasks["daily_reader_brief"].command == "aits reports reader-brief --latest --scope daily"
    assert {"--scope", "daily"} <= set(tasks["daily_reader_brief"].command_contains)
    # The validator step is not scoped; it reads the brief that was written.
    assert "--scope" not in steps["validate_reader_brief"].command


def test_the_research_governance_summary_step_is_not_part_of_this_change() -> None:
    # Its removal is a separate step (5c): the brief still reads its output, and the schedule
    # still orders the brief after it.
    tasks = load_scheduled_tasks_config().tasks_by_id()

    assert "daily_research_governance_summary" in tasks
    assert "research_governance_summary" in tasks["daily_reader_brief"].dependencies
    registry = yaml.safe_load(DEFAULT_REPORT_REGISTRY_PATH.read_text(encoding="utf-8"))["reports"]
    entry = next(item for item in registry if item["report_id"] == "research_governance_summary")
    assert entry["scope"] == "daily"


# --- the records ---------------------------------------------------------------------------------


def test_the_requirement_and_flow_documents_record_the_scope() -> None:
    requirement = (REPO_ROOT / "docs/requirements/OPS-086_Daily_Release_Bundle.md").read_text(
        encoding="utf-8"
    )
    flow = (REPO_ROOT / "docs/system_flow.md").read_text(encoding="utf-8")
    catalog = (REPO_ROOT / "docs/artifact_catalog.md").read_text(encoding="utf-8")

    for text in (requirement, flow):
        assert "owner_decision:OPS-086:2026-10-16:reader_brief_daily_scope" in text
    assert "步骤 5b（OPS-086-09" in requirement
    assert "步骤 5c" in requirement
    assert "`aits reports reader-brief --latest --scope daily`" in flow
    assert "`registry_scope`" in flow
    assert "research_review_due" in flow
    assert "`--scope daily`" in catalog
    assert "OPS-086-09" in requirement
    card = yaml.safe_load(
        (REPO_ROOT / "tasks/OPS-086_DAILY_RELEASE_BUNDLE.yaml").read_text(encoding="utf-8")
    )
    assert "OPS-086-09" in json.dumps(card, ensure_ascii=False)
