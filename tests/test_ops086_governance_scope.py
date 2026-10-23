"""OPS-086-12 (step 5c): the daily run and the daily Reader Brief carry no research governance.

The daily plan no longer builds `research_governance_summary`; the daily scope of the Reader Brief
(`registry_scope == "daily"`) does not read one, builds nothing from it and renders nothing of it;
the daily task dashboard has no card for it. The `all` scope of the Reader Brief, the command
`aits reports research-governance-summary` and the weekly review that runs it stay as they were.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest
import yaml
from test_reader_brief import _write_reader_brief_inputs

from ai_trading_system.cli_commands import ops as ops_cli
from ai_trading_system.ops_daily import build_daily_ops_plan
from ai_trading_system.platform.reporting.owner_daily import (
    build_owner_daily_brief_view_model,
    write_owner_daily_brief_sidecars,
)
from ai_trading_system.reports.reader_brief import (
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

STEP_ID = "research_governance_summary"
# What the fixture's summary puts into the whole brief and the daily brief must not have.
SUMMARY_ONLY_ITEM_IDS = {"missing_weight_promotion_gate", "research_governance_status_review"}
SUMMARY_MARKERS = ("missing_weight_promotion_gate", "NOT_PROMOTABLE", "PASS_WITH_LIMITATIONS")
STATUS_PANEL_GONE = {
    "research_promotion_status",
    "raw_promotion_status",
    "research_promotion_explanation",
}
EXECUTIVE_SUMMARY_GONE = {"governance_status", "research_governance"}
HTML_GONE = (
    "Research Promotion Status",
    "研究晋升：",
    "研究治理：",
    "Backtest / Shadow / Governance",
    "governance_status",
    "研究/权重晋升链路",
)


# --- fixtures ------------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def world(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    root = tmp_path_factory.mktemp("reader_brief_governance")
    inputs = _write_reader_brief_inputs(root)
    daily_ids = {
        entry["report_id"]
        for entry in load_report_registry(DEFAULT_REPORT_REGISTRY_PATH, scope="daily")["reports"]
    }
    mixed = json.loads(inputs["report_index"].read_text(encoding="utf-8"))
    kept = [item for item in mixed["reports"] if item.get("report_id") in daily_ids]
    daily_index = {
        **mixed,
        "registry": {"scope": "daily"},
        "reports": kept,
        "summary": {**mixed["summary"], "report_count": 777},
    }
    # The same index with nothing required missing, so the status can show the summary's effect.
    clean = json.loads(json.dumps(daily_index))
    for record in clean["reports"]:
        if record["report_id"] == "data_quality":
            record.update(freshness_status="FRESH", exists=True)
    clean["summary"].update(missing_count=0, required_missing_count=0)
    paths = {}
    for name, index in (("daily_scope", daily_index), ("clean_scope", clean)):
        paths[name] = root / name / "report_index_2026-05-04.json"
        paths[name].parent.mkdir()
        paths[name].write_text(json.dumps(index), encoding="utf-8")
    return {"root": root, "inputs": inputs, **paths}


def _build(
    world: dict[str, Any],
    *,
    scope: str | None,
    index: str = "daily_scope",
    summary: str = "real",
) -> dict[str, Any]:
    inputs = world["inputs"]
    summary_path = {
        "real": inputs["research_governance_summary"],
        "missing": world["root"] / "no_such_summary.json",
        "none": None,
    }[summary]
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
        research_governance_summary_path=summary_path,
        report_index_path=world[index],
        documentation_contract_path=inputs["documentation_contract"],
        registry_scope=scope,
    )


@pytest.fixture(scope="module")
def whole(world: dict[str, Any]) -> dict[str, Any]:
    return _build(world, scope=None)


@pytest.fixture(scope="module")
def daily(world: dict[str, Any]) -> dict[str, Any]:
    return _build(world, scope="daily")


def _without_time(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if key != "generated_at"}


# --- the daily brief does not depend on the summary ----------------------------------------------


def test_the_daily_brief_is_the_same_whether_or_not_a_summary_exists(
    world: dict[str, Any], daily: dict[str, Any]
) -> None:
    for summary in ("missing", "none"):
        other = _build(world, scope="daily", summary=summary)
        assert _without_time(other) == _without_time(daily)
        other = {**other, "generated_at": daily["generated_at"]}
        assert render_reader_brief_html(other) == render_reader_brief_html(daily)
        assert build_owner_daily_brief_view_model(other).to_dict() == (
            build_owner_daily_brief_view_model(daily).to_dict()
        )


def test_nothing_from_the_summary_reaches_the_daily_payload_or_html(
    whole: dict[str, Any], daily: dict[str, Any]
) -> None:
    whole_text = json.dumps(whole, ensure_ascii=False)
    daily_text = json.dumps(daily, ensure_ascii=False)
    for marker in SUMMARY_MARKERS:
        assert marker in whole_text, marker
        assert marker not in daily_text, marker
    assert STEP_ID not in daily_text
    daily_html = render_reader_brief_html(daily)
    for marker in SUMMARY_MARKERS:
        assert marker not in daily_html, marker


def test_a_missing_summary_no_longer_limits_the_daily_brief(world: dict[str, Any]) -> None:
    # Before 5c the summary was an IMPORTANT input: without it the brief was LIMITED_READER_CONTEXT.
    whole_without = _build(world, scope=None, index="clean_scope", summary="missing")
    assert whole_without["status"] == "LIMITED_READER_CONTEXT"
    assert [
        item["artifact_id"] for item in whole_without["missing_limited_artifact_impact"]["items"]
    ] == [STEP_ID]

    for summary in ("real", "missing", "none"):
        payload = _build(world, scope="daily", index="clean_scope", summary=summary)
        impact = payload["missing_limited_artifact_impact"]
        assert payload["status"] == "OK"
        assert (impact["status"], impact["important_count"], impact["blocking_count"]) == (
            "OK",
            0,
            0,
        )
        assert impact["items"] == []
        assert not [warning for warning in payload.get("warnings", []) if STEP_ID in warning]


# --- the payload ---------------------------------------------------------------------------------


def test_the_daily_payload_has_no_governance_key_or_field(
    whole: dict[str, Any], daily: dict[str, Any]
) -> None:
    assert "backtest_shadow_governance" in whole
    assert "backtest_shadow_governance" not in daily

    assert STATUS_PANEL_GONE <= set(whole["status_panel"])
    assert {
        key: value for key, value in whole["status_panel"].items() if key not in STATUS_PANEL_GONE
    } == (daily["status_panel"])

    assert EXECUTIVE_SUMMARY_GONE <= set(whole["executive_summary"])
    assert daily["executive_summary"] == {
        **{
            key: value
            for key, value in whole["executive_summary"].items()
            if key not in EXECUTIVE_SUMMARY_GONE
        },
        "manual_review_count": len(daily["manual_review_queue"]["items"]),
    }

    assert STEP_ID in whole["source_inputs"]
    assert daily["source_inputs"] == {
        key: value for key, value in whole["source_inputs"].items() if key != STEP_ID
    }
    for key in ("appendix_links", "report_navigation"):
        assert STEP_ID in {item["artifact_id"] for item in whole[key]}
        assert [item["artifact_id"] for item in daily[key]] == [
            item["artifact_id"] for item in whole[key] if item["artifact_id"] != STEP_ID
        ]


def test_the_daily_impact_summary_has_two_chains(
    whole: dict[str, Any], daily: dict[str, Any]
) -> None:
    whole_chains = whole["missing_limited_artifact_impact"]["impact_summary"]
    daily_chains = daily["missing_limited_artifact_impact"]["impact_summary"]

    assert [chain["chain"] for chain in whole_chains] == [
        "今日评分链路",
        "阅读上下文",
        "研究/权重晋升链路",
    ]
    assert daily_chains == whole_chains[:2]
    for key in (
        "status",
        "blocking_count",
        "important_count",
        "optional_count",
        "info_count",
        "items",
    ):
        assert (
            daily["missing_limited_artifact_impact"][key]
            == whole["missing_limited_artifact_impact"][key]
        )


def _review_summary(payload: dict[str, Any]) -> str:
    queue = payload["manual_review_queue"]
    impact = payload["missing_limited_artifact_impact"]
    chains = {chain["chain"]: chain["status"] for chain in impact["impact_summary"]}
    critical = sum(group["count"] for group in queue["groups"] if group["severity"] == "critical")
    return (
        f"当前有 {len(queue['items'])} 个复核项，其中 critical={critical}；"
        f"缺失/受限 artifact 中 blocking={impact['blocking_count']}, "
        f"important={impact['important_count']}。"
        f"今日评分链路={chains['今日评分链路']}；阅读上下文={chains['阅读上下文']}"
    )


def test_the_daily_narrative_has_no_governance_sentence(
    whole: dict[str, Any], daily: dict[str, Any]
) -> None:
    assert "research_governance_summary" in whole["narrative_executive_summary"]
    narrative = daily["narrative_executive_summary"]

    assert "research_governance_summary" not in narrative
    assert narrative["manual_review_summary"] == _review_summary(daily) + "。"
    assert whole["narrative_executive_summary"]["manual_review_summary"].startswith(
        _review_summary(whole) + "；研究/权重晋升链路="
    )
    others = {
        key: value
        for key, value in whole["narrative_executive_summary"].items()
        if key not in {"research_governance_summary", "manual_review_summary"}
    }
    assert {
        key: value for key, value in narrative.items() if key != "manual_review_summary"
    } == others


def test_the_summary_adds_nothing_to_the_daily_review_queue(
    whole: dict[str, Any], daily: dict[str, Any]
) -> None:
    whole_items = whole["manual_review_queue"]["items"]
    daily_items = daily["manual_review_queue"]["items"]

    assert SUMMARY_ONLY_ITEM_IDS <= {item["action_id"] for item in whole_items}
    assert daily_items == [
        item for item in whole_items if item["action_id"] not in SUMMARY_ONLY_ITEM_IDS
    ]
    assert not {"research_governance", "weight_iteration"} & {
        item["category"] for item in daily_items
    }
    queue = daily["manual_review_queue"]
    assert sum(group["count"] for group in queue["groups"]) == len(daily_items)
    assert sum(group["count"] for group in queue["impact_groups"]) == len(daily_items)
    assert {item["action_id"] for item in queue["top_items"]} <= {
        item["action_id"] for item in daily_items
    }


def test_the_daily_checklist_has_no_promotion_line(
    whole: dict[str, Any], daily: dict[str, Any]
) -> None:
    whole_list = whole["action_checklist"]
    daily_list = daily["action_checklist"]

    assert [item for item in whole_list if item["impact_type"] == "research_promotion"]
    assert not [item for item in daily_list if item["impact_type"] == "research_promotion"]
    assert len(daily_list) == len(whole_list) - 1
    assert [item["priority"] for item in daily_list] == list(range(1, len(daily_list) + 1))
    assert daily_list[:2] == whole_list[:2]

    def body(item: dict[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in item.items() if key != "priority"}

    assert body(daily_list[-1]) == body(whole_list[-1])


def test_the_whole_scope_still_has_all_of_it(world: dict[str, Any], whole: dict[str, Any]) -> None:
    assert whole["registry_scope"] == "all"
    assert whole["backtest_shadow_governance"]["source"] == STEP_ID
    assert whole["status_panel"]["research_promotion_status"] == "BLOCKED_BY_MISSING_ARTIFACTS"
    html = render_reader_brief_html(whole)
    for marker in HTML_GONE:
        assert marker in html, marker
    # A payload without a scope field is the whole brief too.
    older = {key: value for key, value in whole.items() if key != "registry_scope"}
    assert render_reader_brief_html(older) == html


# --- the HTML ------------------------------------------------------------------------------------


def test_the_daily_html_shows_no_governance(daily: dict[str, Any]) -> None:
    html = render_reader_brief_html(daily)

    for marker in HTML_GONE:
        assert marker not in html, marker
    assert "Reader Brief Build Status" in html
    assert "Decision Usability" in html
    assert html.count('<article class="status-panel-card">') == 2
    assert "只读阅读入口；不是交易指令" in html
    assert "production_effect=none" in html


# --- the validators ------------------------------------------------------------------------------


def test_the_quality_check_reads_the_two_status_fields_of_the_daily_brief(
    whole: dict[str, Any], daily: dict[str, Any], tmp_path: Path
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
    checks = {
        name: {check["check_id"]: check for check in result["checks"]}
        for name, result in results.items()
    }

    assert results["daily"]["status"] == results["whole"]["status"] != "FAILED"
    assert {k: c["status"] for k, c in checks["daily"].items()} == {
        k: c["status"] for k, c in checks["whole"].items()
    }
    assert (
        checks["daily"]["status_panel"]["message"] == "首屏拆分 Build / Decision Usability 状态。"
    )
    assert checks["whole"]["status_panel"]["message"] == (
        "首屏拆分 Build / Decision Usability / Research Promotion 状态。"
    )
    # The check still fails a daily brief that has lost one of the two fields.
    broken = {**daily, "status_panel": {**daily["status_panel"], "decision_usability": ""}}
    result = build_reader_brief_quality_payload(reader_brief_payload=broken)
    status = next(check for check in result["checks"] if check["check_id"] == "status_panel")
    assert status["status"] != checks["daily"]["status_panel"]["status"]


# --- the Owner Daily Brief -----------------------------------------------------------------------


def test_the_owner_brief_portfolio_section_has_two_source_keys_in_the_daily_scope(
    world: dict[str, Any], whole: dict[str, Any], daily: dict[str, Any], tmp_path: Path
) -> None:
    whole_view = build_owner_daily_brief_view_model(whole)
    daily_view = build_owner_daily_brief_view_model(daily)
    whole_section = next(s for s in whole_view.sections if s.section_id == "portfolio_and_shadow")
    daily_section = next(s for s in daily_view.sections if s.section_id == "portfolio_and_shadow")

    assert whole_section.source_keys == (
        "backtest_shadow_governance",
        "portfolio_control_research",
        "portfolio_control_forward_aging",
    )
    assert daily_section.source_keys == (
        "portfolio_control_research",
        "portfolio_control_forward_aging",
    )
    assert [key for key, _ in daily_section.facts] == list(daily_section.source_keys)
    assert not any("backtest_shadow_governance" in caveat for caveat in daily_section.caveats)
    assert [s.section_id for s in daily_view.sections] == [
        s.section_id for s in whole_view.sections if s.section_id != "research_review_due"
    ]

    json_path, html_path = write_owner_daily_brief_sidecars(daily, output_dir=tmp_path)
    assert "backtest_shadow_governance" not in json_path.read_text(encoding="utf-8")
    assert "backtest_shadow_governance" not in html_path.read_text(encoding="utf-8")


def test_the_owner_brief_is_not_limited_by_the_missing_governance_key(
    world: dict[str, Any],
) -> None:
    payload = _build(world, scope="daily", index="clean_scope", summary="missing")
    section = next(
        s
        for s in build_owner_daily_brief_view_model(payload).sections
        if s.section_id == "portfolio_and_shadow"
    )

    assert ("backtest_shadow_governance", "MISSING") not in section.facts


# --- the daily run -------------------------------------------------------------------------------


def test_the_daily_plan_has_no_governance_step(tmp_path: Path) -> None:
    plan = build_daily_ops_plan(as_of=date(2026, 10, 14), project_root=tmp_path)
    step_ids = [step.step_id for step in plan.steps]

    assert len(step_ids) == 26
    assert STEP_ID not in step_ids
    assert step_ids[step_ids.index("documentation_contract") + 1] == "reader_brief"
    assert not [step for step in plan.steps if "research-governance-summary" in step.command]
    assert not [
        path
        for step in plan.steps
        for path in step.produced_paths
        if "research_governance_summary" in str(path)
    ]


def test_the_schedule_has_no_daily_governance_task_and_keeps_the_weekly_review() -> None:
    tasks = load_scheduled_tasks_config().tasks_by_id()

    assert "daily_research_governance_summary" not in tasks
    assert not [
        task_id
        for task_id, task in tasks.items()
        if getattr(task, "daily_plan_step_id", "") == STEP_ID
    ]
    assert STEP_ID not in tasks["daily_reader_brief"].dependencies
    # The brief still runs after the contract step that used to be between them.
    assert "documentation_contract" in tasks["daily_reader_brief"].dependencies
    weekly = tasks["weekly_research_governance_summary_review"]
    assert weekly.command == "aits reports research-governance-summary --latest"


def test_the_runtime_control_policy_does_not_list_the_step() -> None:
    policy = yaml.safe_load(
        (REPO_ROOT / "config/operations/runtime_control.yaml").read_text(encoding="utf-8")
    )

    assert STEP_ID not in policy["terminal_recovery_allowed_from_step_ids"]
    assert STEP_ID not in policy["source_retry_from_step_ids"]
    assert "reader_brief" in policy["terminal_recovery_allowed_from_step_ids"]


def test_the_finalization_builds_the_brief_without_a_summary_path(
    monkeypatch, tmp_path: Path
) -> None:
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
    assert "research_governance_summary_path" not in seen


# --- the registry --------------------------------------------------------------------------------


def test_the_summary_is_a_research_report_and_not_a_dashboard_card() -> None:
    registry = yaml.safe_load(DEFAULT_REPORT_REGISTRY_PATH.read_text(encoding="utf-8"))["reports"]
    entry = next(item for item in registry if item["report_id"] == STEP_ID)
    daily_ids = {
        item["report_id"]
        for item in load_report_registry(DEFAULT_REPORT_REGISTRY_PATH, scope="daily")["reports"]
    }

    assert entry["scope"] == "research"
    assert entry["include_in_daily_task_dashboard"] is False
    assert entry["command"] == "aits reports research-governance-summary --date YYYY-MM-DD"
    assert STEP_ID not in daily_ids
    assert len(daily_ids) == 20


# --- the records ---------------------------------------------------------------------------------


def test_the_requirement_and_flow_documents_record_step_5c() -> None:
    requirement = (REPO_ROOT / "docs/requirements/OPS-086_Daily_Release_Bundle.md").read_text(
        encoding="utf-8"
    )
    flow = (REPO_ROOT / "docs/system_flow.md").read_text(encoding="utf-8")
    runbook = (REPO_ROOT / "docs/operations/operations_runbook.md").read_text(encoding="utf-8")
    tag = "owner_decision:OPS-086:2026-10-23:reader_brief_no_research_governance"

    for text in (requirement, flow):
        assert tag in text
    assert "步骤 5c（OPS-086-12" in requirement
    assert "步骤 5c（等 Baker 答复" not in requirement
    assert "步骤 5c 另定" not in flow
    assert "`research_governance_summary` 一步" in flow
    assert "`docs report-contract` → `reports reader-brief`" in runbook
    assert "research-governance-summary" not in runbook
    card = yaml.safe_load(
        (REPO_ROOT / "tasks/OPS-086_DAILY_RELEASE_BUNDLE.yaml").read_text(encoding="utf-8")
    )
    assert "OPS-086-12" in json.dumps(card, ensure_ascii=False)
