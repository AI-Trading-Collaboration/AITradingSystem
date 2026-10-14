"""OPS-086-05: the report registry has a scope; the daily closeout handles the daily scope only.

`aits reports index`, `aits reports artifact-lineage` and `aits docs report-contract` take
`--scope all|daily|research` (default `all`, which is what they did before). The daily plan passes
`--scope daily`, so the new production state is not asked for the research entries it never has.
"""

from __future__ import annotations

import fnmatch
import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest
import yaml
from typer.testing import CliRunner

from ai_trading_system import cli_direct
from ai_trading_system.cli import app
from ai_trading_system.documentation_contract import build_documentation_contract_payload
from ai_trading_system.ops_daily import build_daily_ops_plan
from ai_trading_system.reports.artifact_lineage import (
    build_artifact_lineage_payload,
    validate_artifact_lineage_payload,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    build_report_index_payload,
    load_report_registry,
)
from ai_trading_system.scheduled_tasks import load_scheduled_tasks_config

AS_OF = date(2026, 10, 14)

# The daily scope, from the production evidence of 2026-10-14 (OPS-086-05 brief section 2):
# the entries whose artifacts the 27 daily steps (and the score-daily outputs) wrote on the last
# full run, plus the closeout reports themselves. Change this list in the same commit that adds a
# daily report or step.
DAILY_IDS = frozenset(
    {
        "artifact_lineage_graph",
        "artifact_lineage_validation",
        "backtest_input_diagnostics",
        "calculation_explainers",
        "daily_score",
        "data_quality",
        "documentation_contract",
        "evidence_dashboard",
        "forward_evidence_archive",
        "forward_evidence_daily_dry_run_archive",
        "market_data_freshness",
        "market_data_refresh",
        "market_panel",
        "portfolio_candidate_tracking",
        "reader_brief",
        "reader_brief_quality",
        "report_quality_gate",
        "research_governance_summary",
        "score_change_attribution",
        "sec_pit_shadow_monitor",
        "sec_pit_shadow_observe",
    }
)
SCOPED_STEPS = ("artifact_lineage", "report_index", "documentation_contract")


def _real_entries() -> list[dict[str, Any]]:
    raw = yaml.safe_load(DEFAULT_REPORT_REGISTRY_PATH.read_text(encoding="utf-8"))
    return raw["reports"]


def _entry(report_id: str, **extra: Any) -> dict[str, Any]:
    return {
        "report_id": report_id,
        "title": report_id,
        "group": "research",
        "cadence": "ad_hoc",
        "audience": "reviewer",
        "owner": "system",
        "command": "aits reports x",
        "artifact_globs": [f"outputs/reports/{report_id}_*.md"],
        "freshness_sla_days": 30,
        "freshness_rationale": "test",
        "owner_action": "review_if_missing_or_stale",
        **extra,
    }


def _registry_file(tmp_path: Path, entries: list[dict[str, Any]]) -> Path:
    path = tmp_path / "registry.yaml"
    path.write_text(
        yaml.safe_dump(
            {"schema_version": 1, "policy_version": "report_registry_v1", "reports": entries},
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    return path


# --- the data ------------------------------------------------------------------------------------


def test_every_registry_entry_has_a_scope_and_daily_is_the_evidence_list() -> None:
    entries = _real_entries()

    assert all(entry.get("scope") in {"daily", "research"} for entry in entries)
    assert {entry["report_id"] for entry in entries if entry["scope"] == "daily"} == DAILY_IDS
    assert len({entry["report_id"] for entry in entries}) == len(entries)


def test_required_daily_reading_entries_are_daily_and_the_dead_summary_is_not_required() -> None:
    required = {
        entry["report_id"] for entry in _real_entries() if entry.get("required_for_daily_reading")
    }

    # `reports daily-tasks` (daily_decision_summary's writer) is not a daily plan step.
    assert required == {"daily_score", "data_quality"}
    assert required <= DAILY_IDS


def test_every_entry_the_daily_plan_produces_is_in_the_daily_scope(tmp_path: Path) -> None:
    plan = build_daily_ops_plan(as_of=AS_OF, project_root=tmp_path)
    produced = set()
    for step in plan.steps:
        for path in step.produced_paths:
            relative = Path(path).relative_to(tmp_path).as_posix()
            produced.add(relative)
            if relative.startswith("outputs/"):
                produced.add(relative.removeprefix("outputs/"))
    matched = {
        entry["report_id"]
        for entry in _real_entries()
        if any(fnmatch.fnmatch(path, glob) for glob in entry["artifact_globs"] for path in produced)
    }

    assert len(matched) >= 15
    assert matched <= DAILY_IDS


# --- the loader ----------------------------------------------------------------------------------


def test_the_loader_returns_everything_by_default_and_one_scope_on_request() -> None:
    everything = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    daily = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH, scope="daily")
    research = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH, scope="research")

    all_ids = {entry["report_id"] for entry in everything["reports"]}
    assert len(all_ids) == len(_real_entries())
    assert {entry["report_id"] for entry in daily["reports"]} == DAILY_IDS
    assert {entry["report_id"] for entry in research["reports"]} == all_ids - DAILY_IDS
    for scoped in (daily, research):
        assert scoped["defaults"] == everything["defaults"]
        assert scoped["policy_version"] == everything["policy_version"]
        assert scoped["schema_version"] == everything["schema_version"]


def test_the_loader_rejects_a_bad_scope_value_or_a_required_research_entry(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="scope must be one of"):
        load_report_registry(DEFAULT_REPORT_REGISTRY_PATH, scope="weekly")
    bad_value = _registry_file(tmp_path, [_entry("a", scope="bogus")])
    with pytest.raises(ValueError, match="a scope must be one of"):
        load_report_registry(bad_value)
    required_research = _registry_file(
        tmp_path, [_entry("a", scope="research", required_for_daily_reading=True)]
    )
    with pytest.raises(ValueError, match="cannot be required_for_daily_reading"):
        load_report_registry(required_research)


def test_an_entry_without_scope_loads_but_belongs_to_no_scope(tmp_path: Path) -> None:
    path = _registry_file(
        tmp_path,
        [_entry("kept", scope="daily"), _entry("lab", scope="research"), _entry("bare")],
    )

    assert [e["report_id"] for e in load_report_registry(path)["reports"]] == [
        "kept",
        "lab",
        "bare",
    ]
    assert [e["report_id"] for e in load_report_registry(path, scope="daily")["reports"]] == [
        "kept"
    ]
    assert [e["report_id"] for e in load_report_registry(path, scope="research")["reports"]] == [
        "lab"
    ]
    only_bare = _registry_file(tmp_path, [_entry("bare")])
    with pytest.raises(ValueError, match="no entries in scope daily"):
        load_report_registry(only_bare, scope="daily")


# --- the three closeout builders -----------------------------------------------------------------


def test_report_index_lists_the_daily_scope_only_and_records_the_scope(tmp_path: Path) -> None:
    everything = build_report_index_payload(as_of=AS_OF, project_root=tmp_path)
    daily = build_report_index_payload(as_of=AS_OF, project_root=tmp_path, registry_scope="daily")

    assert everything["registry"]["scope"] == "all"
    assert daily["registry"]["scope"] == "daily"
    assert everything["summary"]["report_count"] == len(_real_entries())
    assert {item["report_id"] for item in daily["reports"]} == DAILY_IDS
    assert daily["summary"]["report_count"] == len(DAILY_IDS) == len(daily["reports"])
    # Nothing is produced in an empty state, so the daily-scope warnings are the daily entries'.
    assert daily["summary"]["missing_count"] <= len(DAILY_IDS)
    assert daily["summary"]["required_missing_count"] == 2
    for payload in (daily, everything):
        assert sorted(
            item["report_id"]
            for item in payload["reports"]
            if item["required_for_daily_reading"] and item["freshness_status"] == "MISSING"
        ) == ["daily_score", "data_quality"]
    assert len(daily["warnings"]) < len(everything["warnings"])


def test_lineage_in_the_daily_scope_blocks_exactly_where_the_full_registry_blocks(
    tmp_path: Path,
) -> None:
    blocking = {}
    for scope in (None, "daily"):
        lineage = build_artifact_lineage_payload(
            as_of=AS_OF, project_root=tmp_path, registry_scope=scope
        )
        assert lineage["input_artifacts"]["report_registry_scope"] == (scope or "all")
        report_nodes = {
            node["report_id"] for node in lineage["nodes"] if node.get("source") == "report_index"
        }
        daily_nodes = {"data_quality", "market_data_freshness", "market_data_refresh"}
        if scope == "daily":
            assert report_nodes == daily_nodes
        else:
            assert report_nodes > daily_nodes
        validation = validate_artifact_lineage_payload(lineage)
        blocking[scope] = {issue["issue_id"] for issue in validation["blocking_issues"]}

    # An empty state has no data artifacts and no PIT manifest; the research families only warn.
    assert blocking[None] == {"required_family_data_artifacts", "required_family_pit_manifest"}
    assert blocking["daily"] == blocking[None]


def test_documentation_contract_covers_the_scope_it_was_asked_for() -> None:
    everything = build_documentation_contract_payload(as_of=AS_OF)
    daily = build_documentation_contract_payload(as_of=AS_OF, registry_scope="daily")

    assert everything["inputs"]["registry_scope"] == "all"
    assert daily["inputs"]["registry_scope"] == "daily"
    assert everything["summary"]["report_count"] == len(_real_entries())
    assert daily["summary"]["report_count"] == len(DAILY_IDS)
    assert daily["status"] == "PASS"
    assert daily["summary"]["missing_catalog_count"] == 0


# --- the commands --------------------------------------------------------------------------------


def _invoke(*args: str):
    return CliRunner().invoke(app, list(args))


def test_the_commands_take_scope_and_write_it_into_their_reports(tmp_path: Path) -> None:
    index_json = tmp_path / "index.json"
    result = _invoke(
        "reports",
        "index",
        "--as-of",
        "2026-10-14",
        "--project-root",
        str(tmp_path),
        "--output-path",
        str(tmp_path / "index.html"),
        "--json-output-path",
        str(index_json),
        "--scope",
        "daily",
    )
    assert result.exit_code == 0, result.output
    written = json.loads(index_json.read_text(encoding="utf-8"))
    assert written["registry"]["scope"] == "daily"
    assert written["summary"]["report_count"] == len(DAILY_IDS)

    lineage_json = tmp_path / "lineage.json"
    result = _invoke(
        "reports",
        "artifact-lineage",
        "--as-of",
        "2026-10-14",
        "--project-root",
        str(tmp_path),
        "--json-output-path",
        str(lineage_json),
        "--markdown-output-path",
        str(tmp_path / "lineage.md"),
        "--scope",
        "daily",
    )
    assert result.exit_code == 0, result.output
    assert (
        json.loads(lineage_json.read_text(encoding="utf-8"))["input_artifacts"][
            "report_registry_scope"
        ]
        == "daily"
    )

    contract_json = tmp_path / "contract.json"
    result = _invoke(
        "docs",
        "report-contract",
        "--as-of",
        "2026-10-14",
        "--output-path",
        str(tmp_path / "contract.md"),
        "--json-output-path",
        str(contract_json),
        "--scope",
        "daily",
    )
    assert result.exit_code == 0, result.output
    written = json.loads(contract_json.read_text(encoding="utf-8"))
    assert written["inputs"]["registry_scope"] == "daily"
    assert written["summary"]["report_count"] == len(DAILY_IDS)


def test_the_commands_default_to_the_whole_registry_and_reject_a_bad_scope(
    tmp_path: Path,
) -> None:
    index_json = tmp_path / "index.json"
    base = [
        "reports",
        "index",
        "--as-of",
        "2026-10-14",
        "--project-root",
        str(tmp_path),
        "--output-path",
        str(tmp_path / "index.html"),
        "--json-output-path",
        str(index_json),
    ]
    assert _invoke(*base).exit_code == 0
    written = json.loads(index_json.read_text(encoding="utf-8"))
    assert written["registry"]["scope"] == "all"
    assert written["summary"]["report_count"] == len(_real_entries())

    for command in (
        ["reports", "index", "--project-root", str(tmp_path)],
        ["reports", "artifact-lineage", "--project-root", str(tmp_path)],
        ["docs", "report-contract"],
    ):
        result = _invoke(*command, "--as-of", "2026-10-14", "--scope", "weekly")
        assert result.exit_code == 2, result.output
        assert "--scope must be one of" in result.output


def test_the_daily_dispatcher_passes_scope_to_the_three_commands(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def recorder(name: str):
        def record(**kwargs: object) -> None:
            calls.append((name, kwargs))

        return record

    monkeypatch.setattr(cli_direct.reports_cli, "report_index_command", recorder("index"))
    monkeypatch.setattr(cli_direct.reports_cli, "artifact_lineage_command", recorder("lineage"))
    monkeypatch.setattr(cli_direct.docs_cli, "documentation_contract_command", recorder("docs"))

    assert cli_direct.main(["reports", "index", "--latest", "--scope", "daily"]) == 0
    assert cli_direct.main(["reports", "artifact-lineage", "--latest", "--scope", "daily"]) == 0
    assert cli_direct.main(["docs", "report-contract", "--latest", "--scope", "daily"]) == 0
    assert cli_direct.main(["reports", "index", "--latest"]) == 0
    assert cli_direct.main(["reports", "artifact-lineage", "--as-of", "2026-10-14"]) == 0
    assert cli_direct.main(["docs", "report-contract", "--latest"]) == 0

    assert calls == [
        ("index", {"as_of": None, "latest": True, "scope": "daily"}),
        ("lineage", {"as_of": None, "latest": True, "scope": "daily"}),
        ("docs", {"as_of": None, "latest": True, "scope": "daily"}),
        ("index", {"as_of": None, "latest": True}),
        ("lineage", {"as_of": "2026-10-14", "latest": False}),
        ("docs", {"as_of": None, "latest": True}),
    ]


# --- the daily plan and its schedule -------------------------------------------------------------


def test_the_daily_plan_scopes_exactly_the_three_closeout_registry_steps(tmp_path: Path) -> None:
    plan = build_daily_ops_plan(as_of=AS_OF, project_root=tmp_path)

    for step in plan.steps:
        has_scope = "--scope" in step.command
        assert has_scope == (step.step_id in SCOPED_STEPS), step.step_id
        if has_scope:
            assert step.command[-2:] == ("--scope", "daily"), step.step_id


def test_the_schedule_declares_the_scope_in_the_three_tasks() -> None:
    tasks = load_scheduled_tasks_config().tasks_by_id()

    for task_id in ("daily_artifact_lineage", "daily_report_index", "daily_documentation_contract"):
        assert task_id in tasks
        assert tasks[task_id].command.endswith("--scope daily"), task_id
        assert {"--scope", "daily"} <= set(tasks[task_id].command_contains), task_id
