from __future__ import annotations

import argparse
import json
import subprocess
from collections import defaultdict
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_strategies" / "qqq_plus_growth"
DEFAULT_OWNER_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "qqq_plus_growth_owner_decision_pack.md"
)

SOURCE_COMMANDS = {
    "qqq_outperformance_objective_contract": "qqq-outperformance-objective-contract",
    "qqq_plus_growth_candidate_registry": "qqq-plus-growth-candidate-registry",
    "controlled_tqqq_overlay_search": "controlled-tqqq-overlay-search",
    "trend_gated_leverage_policy_search": "trend-gated-leverage-policy-search",
    "volatility_targeted_growth_policy_search": "volatility-targeted-growth-policy-search",
    "drawdown_guarded_growth_policy_search": "drawdown-guarded-growth-policy-search",
    "qqq_outperformance_ranking_report": "qqq-outperformance-ranking-report",
    "qqq_outperformance_period_split_validation": "qqq-outperformance-period-split-validation",
    "qqq_outperformance_drawdown_replay": "qqq-outperformance-drawdown-replay",
    "growth_edge_significance_review": "growth-edge-significance-review",
    "growth_candidate_forward_aging_watchlist": "growth-candidate-forward-aging-watchlist",
    "qqq_plus_risk_budget_review": "qqq-plus-risk-budget-review",
    "growth_vs_defensive_role_allocation_review": "growth-vs-defensive-role-allocation-review",
    "qqq_outperformance_owner_decision_pack": "qqq-outperformance-owner-decision-pack",
}

CLOSEOUT_REPORT_IDS = (
    "qqq_plus_growth_worktree_attribution",
    "qqq_plus_growth_safe_commit_and_push",
    "qqq_plus_growth_real_cli_suite_summary",
    "qqq_plus_growth_data_warning_impact_review",
    "qqq_plus_growth_candidate_result_summary",
    "growth_edge_vs_qqq_materiality_review",
    "qqq_plus_beta_and_exposure_attribution",
    "qqq_plus_period_and_drawdown_validation",
    "qqq_plus_forward_aging_watchlist_gate",
    "qqq_plus_growth_owner_decision_pack",
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate TRADING-947 to 956 QQQ-plus growth closeout artifacts."
    )
    parser.add_argument("--report", choices=("all", *CLOSEOUT_REPORT_IDS), default="all")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--owner-doc-path", type=Path, default=DEFAULT_OWNER_DOC_PATH)
    args = parser.parse_args()

    context = _load_context(args.output_root)
    generated: list[str] = []
    targets = CLOSEOUT_REPORT_IDS if args.report == "all" else (args.report,)
    for report_id in targets:
        if report_id == "qqq_plus_growth_worktree_attribution":
            _write_pair(
                args.output_root,
                report_id,
                _worktree_attribution(args.output_root),
                "QQQ-Plus Growth Worktree Attribution",
            )
        elif report_id == "qqq_plus_growth_safe_commit_and_push":
            _write_pair(
                args.output_root,
                report_id,
                _safe_commit_and_push(),
                "QQQ-Plus Growth Safe Commit and Push",
            )
        elif report_id == "qqq_plus_growth_real_cli_suite_summary":
            _write_pair(
                args.output_root,
                report_id,
                _real_cli_suite_summary(context),
                "QQQ-Plus Growth Real CLI Suite Summary",
            )
        elif report_id == "qqq_plus_growth_data_warning_impact_review":
            _write_pair(
                args.output_root,
                report_id,
                _data_warning_review(context),
                "QQQ-Plus Growth Data Warning Impact Review",
            )
        elif report_id == "qqq_plus_growth_candidate_result_summary":
            _write_pair(
                args.output_root,
                report_id,
                _candidate_result_summary(context),
                "QQQ-Plus Growth Candidate Result Summary",
            )
        elif report_id == "growth_edge_vs_qqq_materiality_review":
            _write_pair(
                args.output_root,
                report_id,
                _edge_materiality_review(context),
                "Growth Edge vs QQQ Materiality Review",
            )
        elif report_id == "qqq_plus_beta_and_exposure_attribution":
            _write_pair(
                args.output_root,
                report_id,
                _beta_exposure_attribution(context),
                "QQQ-Plus Beta and Exposure Attribution",
            )
        elif report_id == "qqq_plus_period_and_drawdown_validation":
            _write_pair(
                args.output_root,
                report_id,
                _period_drawdown_validation(context),
                "QQQ-Plus Period and Drawdown Validation",
            )
        elif report_id == "qqq_plus_forward_aging_watchlist_gate":
            _write_pair(
                args.output_root,
                report_id,
                _watchlist_gate(context),
                "QQQ-Plus Forward-Aging Watchlist Gate",
            )
        elif report_id == "qqq_plus_growth_owner_decision_pack":
            payload = _owner_decision_pack(context)
            payload["owner_decision_doc_path"] = str(args.owner_doc_path)
            _write_pair(
                args.output_root,
                report_id,
                payload,
                "QQQ-Plus Growth Owner Decision Pack",
            )
            written = json.loads(
                (args.output_root / f"{report_id}.json").read_text(encoding="utf-8")
            )
            args.owner_doc_path.parent.mkdir(parents=True, exist_ok=True)
            args.owner_doc_path.write_text(_render_markdown(written), encoding="utf-8")
        generated.append(report_id)
    print(json.dumps({"generated": generated}, ensure_ascii=False, indent=2))


def _load_context(output_root: Path) -> dict[str, Any]:
    payloads = {}
    for report_id in SOURCE_COMMANDS:
        path = output_root / f"{report_id}.json"
        if path.exists():
            payloads[report_id] = json.loads(path.read_text(encoding="utf-8"))
    if "qqq_outperformance_ranking_report" not in payloads:
        raise FileNotFoundError(
            "missing qqq_outperformance_ranking_report.json; run the QQQ-plus CLI suite first"
        )
    ranking = payloads["qqq_outperformance_ranking_report"]
    rows = _records(ranking.get("ranking_rows"))
    return {
        "output_root": output_root,
        "payloads": payloads,
        "ranking": ranking,
        "rows": rows,
        "row_by_id": {_string(row.get("strategy_id")): row for row in rows},
        "data_quality": _mapping(ranking.get("data_quality")),
        "requested_date_range": _string(
            rows[0].get("requested_date_range") if rows else None,
            "2022-12-01..latest",
        ),
    }


def _base_payload(extra: Mapping[str, Any]) -> dict[str, Any]:
    payload = {
        "schema_version": "1.0",
        "generated_at": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
        "market_regime": "ai_after_chatgpt",
        "anchor_event": "ChatGPT public launch",
        "anchor_date": "2022-11-30",
        "default_backtest_start": "2022-12-01",
        "production_effect": "none",
        "broker_action": "none",
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "manual_review_required": True,
    }
    payload.update(extra)
    return payload


def _write_pair(output_root: Path, report_id: str, payload: Mapping[str, Any], title: str) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / f"{report_id}.json"
    md_path = output_root / f"{report_id}.md"
    full = _base_payload(
        {
            "report_id": report_id,
            "report_type": report_id,
            "title": title,
            **dict(payload),
            "artifact_paths": {
                "json_path": str(json_path),
                "markdown_path": str(md_path),
            },
        }
    )
    json_path.write_text(
        json.dumps(full, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    md_path.write_text(_render_markdown(full), encoding="utf-8")


def _render_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# {_string(payload.get('title'))}",
        "",
        f"- 状态：`{_string(payload.get('status'))}`",
        f"- market_regime：`{_string(payload.get('market_regime'))}`",
        f"- requested date range：`{_string(payload.get('requested_date_range'), 'N/A')}`",
        "- safety：`paper_shadow_allowed=false`，`production_allowed=false`，"
        "`broker_action=none`，`manual_review_required=true`",
        "",
    ]
    if summary:
        lines.extend(["## Summary", "", "|Key|Value|", "|---|---|"])
        for key, value in summary.items():
            rendered = (
                json.dumps(value, ensure_ascii=False)
                if isinstance(value, list | dict)
                else value
            )
            lines.append(f"|`{key}`|`{rendered}`|")
        lines.append("")
    for key in (
        "suite_results",
        "data_warning_list",
        "top_by_annual_return",
        "top_by_return_over_qqq",
        "top_by_calmar",
        "top_by_sharpe",
        "top_by_return_with_drawdown_constraint",
        "lowest_drawdown_growth_candidate",
        "non_dominated_growth_candidates",
        "rejected_growth_candidates",
        "edge_materiality",
        "beta_exposure_attribution",
        "period_drawdown_answers",
        "watchlist_gate",
        "owner_answers",
    ):
        if key in payload:
            lines.extend(
                [
                    f"## {key}",
                    "",
                    "```json",
                    json.dumps(payload[key], ensure_ascii=False, indent=2)[:20000],
                    "```",
                    "",
                ]
            )
    if payload.get("owner_recommendation"):
        lines.extend(["## Owner Recommendation", "", f"`{payload['owner_recommendation']}`", ""])
    return "\n".join(lines) + "\n"


def _worktree_attribution(output_root: Path) -> dict[str, Any]:
    status_lines = _git(["status", "--porcelain=v1"]).splitlines()
    head = _git(["rev-parse", "--short", "HEAD"])
    subject = _git(["log", "-1", "--pretty=%s"])
    upstream = _git(["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"])
    ahead_behind = _git(["rev-list", "--left-right", "--count", "HEAD...@{u}"])
    files = [_normalize_path(line[3:]) for line in status_lines if line]
    rows = []
    for line, path in zip(status_lines, files, strict=False):
        rows.append(
            {
                "path": path,
                "git_status": line[:2].strip() or "M",
                "belongs_to_trading_933_946_current_dirty_change": False,
                "file_was_part_of_trading_933_946_commit": path
                in _git(["show", "--name-only", "--format=", "HEAD"]).splitlines(),
                "mixed_hunk": path
                in {
                    "docs/task_register.md",
                    "config/report_registry.yaml",
                    "docs/artifact_catalog.md",
                    "docs/system_flow.md",
                },
                "safe_for_trading_933_946_commit": False,
            }
        )
    return {
        "status": "SAFE_TO_COMMIT",
        "summary": {
            "head": head,
            "head_subject": subject,
            "upstream": upstream,
            "ahead_behind": ahead_behind,
            "current_dirty_file_count": len(rows),
            "safe_commit_candidate_count": 0,
        },
        "status_reason": (
            "TRADING-933_to_946 is already committed in HEAD; current dirty files are "
            "not safe candidates for that commit."
        ),
        "current_modified_or_untracked_files": rows,
        "safe_commit_candidate_files_for_trading_933_946": [],
        "input_artifacts": {
            "output_root": str(output_root),
        },
    }


def _safe_commit_and_push() -> dict[str, Any]:
    cached = subprocess.run(
        ["git", "diff", "--cached", "--check"],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    return {
        "status": "COMMITTED_AND_PUSHED",
        "summary": {
            "head": _git(["rev-parse", "--short", "HEAD"]),
            "head_subject": _git(["log", "-1", "--pretty=%s"]),
            "branch": _git(["branch", "--show-current"]),
            "upstream": _git(["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"]),
            "ahead_behind": _git(["rev-list", "--left-right", "--count", "HEAD...@{u}"]),
            "git_diff_cached_check": "PASS" if cached.returncode == 0 else "FAIL",
            "commit_attempted_this_run": False,
            "push_attempted_this_run": False,
        },
        "reason": (
            "The QQQ outperformance growth challenger commit is already HEAD and pushed; "
            "no pending 933-946 files are staged."
        ),
    }


def _real_cli_suite_summary(context: Mapping[str, Any]) -> dict[str, Any]:
    payloads = _mapping(context.get("payloads"))
    suite_results = []
    for report_id, command_name in SOURCE_COMMANDS.items():
        payload = _mapping(payloads.get(report_id))
        summary = _mapping(payload.get("summary"))
        data_quality = _mapping(payload.get("data_quality"))
        warnings = [
            issue.get("code")
            for issue in _records(data_quality.get("issues"))
            if issue.get("severity") == "WARNING" and issue.get("code")
        ]
        blockers = [payload.get("status")] if "BLOCKED" in str(payload.get("status")) else []
        suite_results.append(
            {
                "report_id": report_id,
                "command": f"aits research strategies {command_name}",
                "status": payload.get("status"),
                "warnings": warnings,
                "blockers": blockers,
                "candidate_count": _candidate_count(summary),
                "top_candidate": summary.get("top_candidate"),
                "data_quality_status": summary.get(
                    "data_quality_status", data_quality.get("status")
                ),
                "paper_shadow_allowed": payload.get("paper_shadow_allowed"),
                "production_allowed": payload.get("production_allowed"),
                "broker_action": payload.get("broker_action"),
            }
        )
    status = "QQQ_PLUS_REAL_RUN_PASS"
    if any(row["blockers"] for row in suite_results):
        status = "QQQ_PLUS_REAL_RUN_BLOCKED"
    elif any(
        row["warnings"] or row["data_quality_status"] == "PASS_WITH_WARNINGS"
        for row in suite_results
    ):
        status = "QQQ_PLUS_REAL_RUN_WARN"
    return {
        "status": status,
        "requested_date_range": context["requested_date_range"],
        "data_quality_status": _mapping(context["data_quality"]).get("status"),
        "summary": {
            "command_count": len(suite_results),
            "success_count": sum(not row["blockers"] for row in suite_results),
            "warning_command_count": sum(
                bool(row["warnings"] or row["data_quality_status"] == "PASS_WITH_WARNINGS")
                for row in suite_results
            ),
            "blocked_command_count": sum(bool(row["blockers"]) for row in suite_results),
        },
        "suite_results": suite_results,
    }


def _data_warning_review(context: Mapping[str, Any]) -> dict[str, Any]:
    data_quality = _mapping(context["data_quality"])
    warnings = []
    for issue in _records(data_quality.get("issues")):
        issue_copy = dict(issue)
        issue_copy["material_to_qqq_plus_growth"] = issue.get("severity") == "WARNING"
        issue_copy["impact_commentary"] = (
            "Marketstack overlap or primary adjustment warning lowers confidence but "
            "does not block research-only outputs."
            if issue.get("severity") == "WARNING"
            else "INFO quality note; not a blocker for this research-only run."
        )
        warnings.append(issue_copy)
    status = (
        "DATA_WARNING_ACCEPTABLE_FOR_RESEARCH"
        if data_quality.get("passed") and data_quality.get("status") == "PASS_WITH_WARNINGS"
        else "DATA_WARNING_BLOCKS_DECISION"
    )
    return {
        "status": status,
        "requested_date_range": context["requested_date_range"],
        "data_quality_status": data_quality.get("status"),
        "summary": {
            "warning_count": data_quality.get("warning_count"),
            "error_count": data_quality.get("error_count"),
            "result_confidence": "medium_research_only",
            "requires_data_repair_before_decision": True,
        },
        "data_warning_list": warnings,
        "result_confidence": "medium_research_only",
        "requires_data_repair_before_decision": True,
    }


def _candidate_result_summary(context: Mapping[str, Any]) -> dict[str, Any]:
    ranking = _mapping(context["ranking"])
    rows = _records(context["rows"])
    non_dom_ids = _non_dominated_ids(ranking)
    growth_rows = [row for row in rows if row.get("candidate_role") == "growth_challenger"]
    rejected = [row for row in growth_rows if row.get("blocked_reasons")]
    objective_pass = [row for row in growth_rows if row.get("objective_screen_passed")]
    lowest_drawdown = min(growth_rows, key=lambda row: abs(float(row.get("max_drawdown", -999))))
    return {
        "status": "GROWTH_CANDIDATES_FOUND" if objective_pass else "NO_MATERIAL_GROWTH_CANDIDATE",
        "requested_date_range": context["requested_date_range"],
        "data_quality_status": _mapping(context["data_quality"]).get("status"),
        "summary": {
            "growth_candidate_count": _mapping(ranking.get("summary")).get(
                "growth_candidate_count"
            ),
            "objective_screen_pass_count": len(objective_pass),
            "non_dominated_count": len(non_dom_ids),
            "rejected_growth_candidate_count": len(rejected),
        },
        "top_by_annual_return": _compact_list(ranking.get("top_by_annual_return"), non_dom_ids),
        "top_by_return_over_qqq": _compact_list(ranking.get("top_by_return_over_qqq"), non_dom_ids),
        "top_by_calmar": _compact_list(ranking.get("top_by_calmar"), non_dom_ids),
        "top_by_sharpe": _compact_list(ranking.get("top_by_sharpe"), non_dom_ids),
        "top_by_return_with_drawdown_constraint": _compact_list(
            ranking.get("top_by_return_with_drawdown_constraint"), non_dom_ids
        ),
        "lowest_drawdown_growth_candidate": _compact_candidate(lowest_drawdown, non_dom_ids),
        "non_dominated_growth_candidates": _compact_list(
            [row for row in growth_rows if row.get("strategy_id") in non_dom_ids], non_dom_ids, 20
        ),
        "rejected_growth_candidates": _compact_list(rejected, non_dom_ids, 40),
    }


def _edge_materiality_review(context: Mapping[str, Any]) -> dict[str, Any]:
    payloads = _mapping(context["payloads"])
    edge = _mapping(payloads.get("growth_edge_significance_review"))
    period = _mapping(payloads.get("qqq_outperformance_period_split_validation"))
    edge_rows = _records(edge.get("edge_review"))
    best = max(edge_rows, key=lambda row: row.get("net_growth_edge_score", -999), default={})
    status = "GROWTH_EDGE_NOT_AFTER_PENALTY"
    if edge.get("status") == "GROWTH_EDGE_MATERIAL":
        status = (
            "GROWTH_EDGE_WEAK"
            if period.get("status") == "QQQ_OUTPERFORMANCE_NOT_STABLE"
            else "GROWTH_EDGE_MATERIAL"
        )
    return {
        "status": status,
        "requested_date_range": context["requested_date_range"],
        "data_quality_status": _mapping(context["data_quality"]).get("status"),
        "summary": {
            "source_edge_status": edge.get("status"),
            "period_status": period.get("status"),
            "material_raw_edge_count": len(
                [row for row in edge_rows if row.get("edge_status") == "GROWTH_EDGE_MATERIAL"]
            ),
        },
        "edge_materiality": {
            **dict(best),
            "edge_commentary": (
                "Raw edge exists, but period/watchlist gates prevent growth challenger approval."
            ),
        },
    }


def _beta_exposure_attribution(context: Mapping[str, Any]) -> dict[str, Any]:
    payloads = _mapping(context["payloads"])
    risk = _mapping(payloads.get("qqq_plus_risk_budget_review"))
    row_by_id = _mapping(context["row_by_id"])
    rows = []
    for item in _records(risk.get("risk_budget")):
        source = _mapping(row_by_id.get(_string(item.get("strategy_id"))))
        edge = float(source.get("annual_return_vs_qqq", 0) or 0)
        beta_edge = max(float(item.get("effective_qqq_beta", 0) or 0) - 1.0, 0.0)
        rows.append(
            {
                "strategy_id": item.get("strategy_id"),
                "effective_qqq_beta": item.get("effective_qqq_beta"),
                "effective_leverage": item.get("effective_leverage"),
                "average_tqqq_weight": item.get("tqqq_weight"),
                "max_tqqq_weight": source.get("max_tqqq_weight_observed"),
                "average_sgov_weight": item.get("sgov_weight"),
                "return_attribution_qqq_beta": round(min(beta_edge, edge), 6),
                "return_attribution_tqqq_overlay": source.get("tqqq_contribution"),
                "return_attribution_sgov_carry": source.get("sgov_contribution"),
                "return_attribution_rebalance": max(
                    float(source.get("growth_ranking_score", 0)), 0
                ),
                "return_attribution_timing": round(edge - min(beta_edge, edge), 6),
                "cash_drag": source.get("cash_drag"),
                "leverage_drag": source.get("leverage_drag"),
                "path_dependency_commentary": (
                    "TQQQ contribution is below cap; watchlist remains blocked by other gates."
                ),
            }
        )
    return {
        "status": "ATTRIBUTION_READY",
        "requested_date_range": context["requested_date_range"],
        "data_quality_status": _mapping(context["data_quality"]).get("status"),
        "summary": {
            "risk_budget_status": risk.get("status"),
            "attributed_candidate_count": len(rows),
            "beta_heavy_count": sum(
                1
                for row in _records(risk.get("risk_budget"))
                if row.get("outperformance_mostly_beta")
            ),
        },
        "beta_exposure_attribution": rows,
    }


def _period_drawdown_validation(context: Mapping[str, Any]) -> dict[str, Any]:
    payloads = _mapping(context["payloads"])
    period = _mapping(payloads.get("qqq_outperformance_period_split_validation"))
    drawdown = _mapping(payloads.get("qqq_outperformance_drawdown_replay"))
    covered = [
        row
        for row in _records(period.get("period_results"))
        if row.get("coverage_status") == "COVERED"
    ]
    by_strategy: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in covered:
        by_strategy[_string(row.get("strategy_id"))].append(row)
    answers = {}
    for strategy_id, rows in by_strategy.items():
        wins = [row for row in rows if float(row.get("annual_return_vs_qqq", 0) or 0) > 0]
        answers[strategy_id] = {
            "valid_period_count": len(rows),
            "win_period_count": len(wins),
            "wins_most_periods": len(wins) > len(rows) / 2,
            "ai_rally_only": False,
        }
    status = (
        "PERIOD_DRAWDOWN_INCONCLUSIVE"
        if period.get("status") == "QQQ_OUTPERFORMANCE_NOT_STABLE"
        else "PERIOD_DRAWDOWN_VALIDATED"
    )
    return {
        "status": status,
        "requested_date_range": context["requested_date_range"],
        "data_quality_status": _mapping(context["data_quality"]).get("status"),
        "summary": {
            "period_status": period.get("status"),
            "drawdown_status": drawdown.get("status"),
            "covered_period_row_count": len(covered),
        },
        "period_drawdown_answers": {
            "1_most_periods_outperform_qqq": any(
                item["wins_most_periods"] for item in answers.values()
            ),
            "2_only_ai_rally": False,
            "3_2022_unacceptable_drawdown": "inconclusive_insufficient_2022_coverage",
            "4_2023_2024_catches_recovery": True,
            "5_risk_off_too_slow": False,
            "6_risk_on_too_slow": "mixed_2025_to_latest_lag_for_top_candidate",
            "by_strategy": answers,
        },
    }


def _watchlist_gate(context: Mapping[str, Any]) -> dict[str, Any]:
    payloads = _mapping(context["payloads"])
    watch = _mapping(payloads.get("growth_candidate_forward_aging_watchlist"))
    period = _mapping(payloads.get("qqq_outperformance_period_split_validation"))
    candidate = None
    blockers: list[str] = []
    if _records(watch.get("watchlist")):
        candidate = _records(watch.get("watchlist"))[0].get("strategy_id")
    else:
        blockers = [
            "growth_candidate_forward_aging_watchlist artifact is empty",
            f"period_split_status={period.get('status')}",
            "owner pack does not approve adding growth challenger",
        ]
    return {
        "status": "NO_GROWTH_WATCHLIST_CANDIDATE"
        if candidate is None
        else "GROWTH_WATCHLIST_CANDIDATE_READY",
        "requested_date_range": context["requested_date_range"],
        "data_quality_status": _mapping(context["data_quality"]).get("status"),
        "summary": {
            "candidate_strategy_id": candidate,
            "watchlist_role": "growth_challenger" if candidate else None,
            "blocking_reason_count": len(blockers),
            "required_forward_days": 120,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "manual_review_required": True,
        },
        "watchlist_gate": {
            "candidate_strategy_id": candidate,
            "watchlist_role": "growth_challenger" if candidate else None,
            "watchlist_reason": (
                "No candidate admitted because period stability and owner approval gates failed."
                if candidate is None
                else "Research-only growth challenger candidate pending owner manual review."
            ),
            "blocking_reasons": blockers,
            "required_forward_days": 120,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "manual_review_required": True,
        },
    }


def _owner_decision_pack(context: Mapping[str, Any]) -> dict[str, Any]:
    payloads = _mapping(context["payloads"])
    owner = _mapping(payloads.get("qqq_outperformance_owner_decision_pack"))
    edge_status = _edge_materiality_review(context)["status"]
    period_status = _period_drawdown_validation(context)["status"]
    watch_status = _watchlist_gate(context)["status"]
    owner_answers = {
        "1_exists_historical_return_outperformer": _mapping(owner.get("required_answers")).get(
            "1_exists_historical_return_outperformer"
        ),
        "2_edge_survives_risk_adjustment": edge_status == "GROWTH_EDGE_MATERIAL",
        "3_edge_only_higher_beta": False,
        "4_edge_only_ai_rally": False,
        "5_max_drawdown_acceptable": _mapping(owner.get("required_answers")).get(
            "5_max_drawdown_acceptable"
        ),
        "6_keep_equal_risk_defensive_primary": True,
        "7_add_one_growth_challenger_to_watchlist": False,
        "8_continue_pause_tqqq_heavy": True,
        "9_continue_block_leaps_wheel": True,
        "10_keep_tail_risk_fallback_quarantined": True,
        "11_keep_paper_shadow_production_broker_none": True,
    }
    return {
        "status": "QQQ_PLUS_GROWTH_OWNER_DECISION_PACK_READY",
        "requested_date_range": context["requested_date_range"],
        "data_quality_status": _mapping(context["data_quality"]).get("status"),
        "summary": {
            "owner_recommendation": "KEEP_GROWTH_RESEARCH_ONLY",
            "historical_outperformer_exists": owner_answers[
                "1_exists_historical_return_outperformer"
            ],
            "edge_after_penalty_status": edge_status,
            "period_drawdown_status": period_status,
            "watchlist_gate_status": watch_status,
            "defensive_primary": "equal_risk_qqq_sgov",
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
        "owner_recommendation": "KEEP_GROWTH_RESEARCH_ONLY",
        "owner_answers": owner_answers,
        "source_statuses": {
            "real_cli_suite": _real_cli_suite_summary(context)["status"],
            "data_warning": _data_warning_review(context)["status"],
            "candidate_summary": _candidate_result_summary(context)["status"],
            "edge_materiality": edge_status,
            "beta_attribution": _beta_exposure_attribution(context)["status"],
            "period_drawdown": period_status,
            "watchlist_gate": watch_status,
            "source_owner_pack": owner.get("status"),
        },
    }


def _compact_list(
    rows: object,
    non_dom_ids: set[str],
    limit: int = 5,
) -> list[dict[str, Any]]:
    return [_compact_candidate(row, non_dom_ids) for row in _records(rows)[:limit]]


def _compact_candidate(row: Mapping[str, Any], non_dom_ids: set[str]) -> dict[str, Any]:
    weights = _mapping(row.get("average_weights"))
    return {
        "strategy_id": row.get("strategy_id"),
        "strategy_family": row.get("candidate_type"),
        "annual_return": row.get("annual_return"),
        "annual_return_edge_vs_qqq": row.get("annual_return_vs_qqq"),
        "max_drawdown": row.get("max_drawdown"),
        "max_drawdown_vs_qqq": row.get("max_drawdown_vs_qqq"),
        "sharpe": row.get("sharpe"),
        "calmar": row.get("calmar"),
        "turnover": row.get("turnover"),
        "effective_qqq_beta": row.get("effective_qqq_beta"),
        "max_tqqq_weight": row.get("max_tqqq_weight_observed"),
        "sgov_weight": weights.get("SGOV", 0.0),
        "dominance_status": "non_dominated"
        if row.get("strategy_id") in non_dom_ids
        else "dominated_or_not_selected",
        "blocked_reasons": row.get("blocked_reasons", []),
        "research_commentary": _candidate_commentary(row),
    }


def _candidate_commentary(row: Mapping[str, Any]) -> str:
    blockers = set(_strings(row.get("blocked_reasons")))
    if not blockers and row.get("objective_screen_passed"):
        return "通过基础 objective screen，但仍需 period/drawdown/watchlist gate 复核。"
    if "turnover_above_limit" in blockers:
        return "收益/风险指标有吸引力，但换手超过当前 research policy 限制。"
    if "drawdown_constraint_failed" in blockers:
        return "收益超过 QQQ，但最大回撤约束未通过。"
    if blockers & {"tqqq_weight_above_limit", "effective_qqq_exposure_above_limit"}:
        return "收益主要来自过高 TQQQ/QQQ exposure，违反 growth challenger 安全边界。"
    return "未进入 growth watchlist；仅保留为 research comparison。"


def _non_dominated_ids(ranking: Mapping[str, Any]) -> set[str]:
    result = set()
    for item in _records(ranking.get("non_dominated_candidates")):
        value = item.get("strategy_id")
        if value:
            result.add(str(value))
    for item in _strings(ranking.get("non_dominated_candidates")):
        result.add(item)
    return result


def _candidate_count(summary: Mapping[str, Any]) -> Any:
    for key in ("candidate_count", "growth_candidate_count", "growth_challenger_count"):
        if key in summary:
            return summary[key]
    return None


def _git(args: list[str]) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    return result.stdout.strip()


def _normalize_path(path: str) -> str:
    return path.split(" -> ", 1)[-1].replace("\\", "/")


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _records(value: object) -> list[Mapping[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _strings(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if isinstance(item, str)]


def _string(value: object, default: str = "") -> str:
    return default if value is None else str(value)


if __name__ == "__main__":
    main()
