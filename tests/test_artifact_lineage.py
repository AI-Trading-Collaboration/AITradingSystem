from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.artifact_lineage import (
    build_artifact_lineage_payload,
    render_artifact_lineage_markdown,
    validate_artifact_lineage_payload,
    write_artifact_lineage_json,
    write_artifact_lineage_markdown,
    write_artifact_lineage_validation_json,
    write_artifact_lineage_validation_markdown,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

RUN_DATE = date(2026, 5, 4)

COMPLETE_REQUIRED_REPORT_IDS = (
    "cache_catalog",
    "data_refresh_audit",
    "pit_source_manifest",
    "etf_dynamic_v3_signal_input_completeness",
    "etf_dynamic_v3_paper_shadow_daily",
    "etf_dynamic_v3_paper_shadow_drift_monitor",
    "etf_dynamic_v3_paper_shadow_weekly_review",
    "etf_dynamic_v3_evidence_staleness_monitor",
    "etf_dynamic_v3_shadow_continuation_readiness",
    "etf_dynamic_v3_owner_review",
)


def test_artifact_lineage_passes_complete_required_families(tmp_path: Path) -> None:
    _write_data_cache(tmp_path)
    payload = build_artifact_lineage_payload(
        as_of=RUN_DATE,
        project_root=tmp_path,
        report_index_payload=_report_index(tmp_path),
        report_index_path=tmp_path / "report_index.json",
    )
    validation = validate_artifact_lineage_payload(payload)
    markdown = render_artifact_lineage_markdown(payload)

    assert payload["lineage_status"] == "PASS"
    assert validation["validation_status"] == "PASS"
    assert payload["summary"]["available_required_family_count"] == 11
    assert payload["summary"]["passing_required_edge_count"] == 11
    assert all(edge["status"] == "PASS" for edge in payload["edges"])
    assert "Artifact Lineage Graph" in markdown


def test_artifact_lineage_blocks_missing_owner_review_family(tmp_path: Path) -> None:
    _write_data_cache(tmp_path)
    report_ids = [
        report_id
        for report_id in COMPLETE_REQUIRED_REPORT_IDS
        if report_id != "etf_dynamic_v3_owner_review"
    ]

    payload = build_artifact_lineage_payload(
        as_of=RUN_DATE,
        project_root=tmp_path,
        report_index_payload=_report_index(tmp_path, report_ids=report_ids),
    )

    issue_ids = {issue["issue_id"] for issue in payload["blocking_issues"]}
    edge_status = {
        edge["edge_id"]: edge["status"]
        for edge in payload["edges"]
        if edge["edge_id"] == "readiness_reports__to__owner_reviews"
    }
    assert payload["lineage_status"] == "FAIL"
    assert "required_family_owner_reviews" in issue_ids
    assert edge_status == {"readiness_reports__to__owner_reviews": "MISSING_NODE"}


def test_artifact_lineage_blocks_unsafe_node_production_effect(tmp_path: Path) -> None:
    _write_data_cache(tmp_path)
    payload = build_artifact_lineage_payload(
        as_of=RUN_DATE,
        project_root=tmp_path,
        report_index_payload=_report_index(
            tmp_path,
            production_effect_overrides={"cache_catalog": "mutates"},
        ),
    )

    assert payload["lineage_status"] == "FAIL"
    assert any(
        issue["issue_id"].startswith("safe_production_effect_cache_catalog")
        for issue in payload["blocking_issues"]
    )


def test_artifact_lineage_cli_writes_report_and_validation(tmp_path: Path) -> None:
    _write_data_cache(tmp_path)
    index_path = tmp_path / "report_index_2026-05-04.json"
    lineage_json = tmp_path / "artifact_lineage_graph_2026-05-04.json"
    lineage_md = tmp_path / "artifact_lineage_graph_2026-05-04.md"
    validation_json = tmp_path / "artifact_lineage_validation_2026-05-04.json"
    validation_md = tmp_path / "artifact_lineage_validation_2026-05-04.md"
    index_path.write_text(
        json.dumps(_report_index(tmp_path), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "reports",
            "artifact-lineage",
            "--date",
            RUN_DATE.isoformat(),
            "--project-root",
            str(tmp_path),
            "--report-index-path",
            str(index_path),
            "--json-output-path",
            str(lineage_json),
            "--markdown-output-path",
            str(lineage_md),
        ],
    )
    assert result.exit_code == 0, result.output

    validation_result = runner.invoke(
        app,
        [
            "reports",
            "validate-artifact-lineage",
            "--date",
            RUN_DATE.isoformat(),
            "--artifact-lineage-json-path",
            str(lineage_json),
            "--json-output-path",
            str(validation_json),
            "--markdown-output-path",
            str(validation_md),
        ],
    )

    assert validation_result.exit_code == 0, validation_result.output
    payload = json.loads(lineage_json.read_text(encoding="utf-8"))
    validation_payload = json.loads(validation_json.read_text(encoding="utf-8"))
    assert payload["report_type"] == "artifact_lineage_graph"
    assert payload["lineage_status"] == "PASS"
    assert validation_payload["report_type"] == "artifact_lineage_validation"
    assert validation_payload["validation_status"] == "PASS"
    assert validation_payload["input_artifacts"]["artifact_lineage_graph"] == str(lineage_json)
    assert lineage_md.exists()
    assert validation_md.exists()


def test_artifact_lineage_registry_and_reader_brief_summary(tmp_path: Path) -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    report_ids = {item["report_id"] for item in registry["reports"]}
    assert "artifact_lineage_graph" in report_ids
    assert "artifact_lineage_validation" in report_ids

    lineage_path = tmp_path / "artifact_lineage_graph_2026-05-04.json"
    _write_json(
        lineage_path,
        {
            "report_type": "artifact_lineage_graph",
            "lineage_status": "PASS_WITH_WARNINGS",
            "status": "PASS_WITH_WARNINGS",
            "production_effect": "none",
            "next_action": "review_stale_lineage_artifacts",
            "summary": {
                "node_count": 14,
                "available_node_count": 13,
                "required_family_count": 11,
                "available_required_family_count": 11,
                "required_edge_count": 11,
                "passing_required_edge_count": 11,
                "blocking_issue_count": 0,
                "warning_issue_count": 1,
            },
        },
    )
    summary = reader_brief._artifact_lineage_graph_summary(
        {
            "reports": [
                {
                    "report_id": "artifact_lineage_graph",
                    "latest_artifact_path": str(lineage_path),
                    "exists": True,
                }
            ]
        }
    )

    assert summary["availability"] == "AVAILABLE"
    assert summary["lineage_status"] == "PASS_WITH_WARNINGS"
    assert summary["available_required_family_count"] == 11
    assert summary["detail_report"] == str(lineage_path)


def test_artifact_lineage_write_helpers_create_markdown_and_json(tmp_path: Path) -> None:
    _write_data_cache(tmp_path)
    payload = build_artifact_lineage_payload(
        as_of=RUN_DATE,
        project_root=tmp_path,
        report_index_payload=_report_index(tmp_path),
    )
    validation = validate_artifact_lineage_payload(payload)

    assert write_artifact_lineage_json(payload, tmp_path / "lineage.json").exists()
    assert write_artifact_lineage_markdown(payload, tmp_path / "lineage.md").exists()
    assert write_artifact_lineage_validation_json(
        validation,
        tmp_path / "validation.json",
    ).exists()
    assert write_artifact_lineage_validation_markdown(
        validation,
        tmp_path / "validation.md",
    ).exists()


def _report_index(
    tmp_path: Path,
    *,
    report_ids: list[str] | tuple[str, ...] = COMPLETE_REQUIRED_REPORT_IDS,
    production_effect_overrides: dict[str, str] | None = None,
) -> dict[str, object]:
    production_effect_overrides = production_effect_overrides or {}
    return {
        "status": "PASS",
        "reports": [
            _report_index_record(
                tmp_path,
                report_id,
                production_effect=production_effect_overrides.get(report_id, "none"),
            )
            for report_id in report_ids
        ],
    }


def _report_index_record(
    tmp_path: Path,
    report_id: str,
    *,
    production_effect: str = "none",
) -> dict[str, object]:
    path = tmp_path / "reports" / f"{report_id}.json"
    _write_json(
        path,
        {
            "report_type": report_id,
            "purpose": f"Fixture report for {report_id}.",
            "input_artifacts": {"fixture": "input.json"},
            "output_decision": "PASS",
            "safety_boundary": {"production_effect": production_effect},
            "limitations": ["fixture only"],
            "next_action": "continue_fixture_validation",
            "status": "PASS",
            "production_effect": production_effect,
        },
    )
    return {
        "report_id": report_id,
        "title": report_id,
        "latest_artifact_path": str(path),
        "exists": True,
        "freshness_status": "FRESH",
        "artifact_status": "PASS",
        "artifact_production_effect": production_effect,
    }


def _write_data_cache(tmp_path: Path) -> None:
    price_path = tmp_path / "data" / "raw" / "prices_daily.csv"
    price_path.parent.mkdir(parents=True, exist_ok=True)
    price_path.write_text(
        "date,ticker,open,high,low,close,adj_close,volume\n"
        "2026-05-04,NVDA,1,1,1,1,1,100\n",
        encoding="utf-8",
    )


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
