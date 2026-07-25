from __future__ import annotations

import json
import subprocess
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import ai_trading_system.daily_input_capture as capture_module
from ai_trading_system.daily_input_capture import (
    build_daily_input_capture_components,
    capture_daily_inputs,
    daily_input_capture_paths,
    load_daily_input_capture_policy,
    validate_daily_input_capture_manifest,
)
from ai_trading_system.ops_daily import build_daily_ops_plan


def _write_policy(path: Path, project_root: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            (
                "schema_version: daily_input_capture_policy.v1",
                "policy_version: daily_input_capture_test_v1",
                "owner: test_owner",
                "status: REVIEWED",
                "tracking_start: 2026-07-27",
                "calendar_authority: XNYS",
                "decision_session_policy: XNYS_DECISION_SESSION_ALIGNED",
                "required_components:",
                "  - market_macro",
                "  - fmp_forward_pit",
                "  - sec_companyfacts",
                "  - fmp_valuation",
                "  - official_policy_sources",
                "paths:",
                f"  raw_root: {project_root.as_posix()}/data/raw/daily_input_capture",
                f"  processed_root: {project_root.as_posix()}/data/processed/daily_input_capture",
                f"  external_root: {project_root.as_posix()}/data/external/daily_input_capture",
                f"  report_root: {project_root.as_posix()}/outputs/daily_input_capture",
                "safety:",
                "  production_effect: none",
                "  production_weight_write: false",
                "  active_shadow_weight_write: false",
                "  broker_action: false",
                "  trading_action: false",
                "",
            )
        ),
        encoding="utf-8",
    )


def _runner_for_components(
    components,
    *,
    failing_component: str | None = None,
    calls: list[str],
):
    component_by_token = {
        "download-data": components[0],
        "pit-snapshots": components[1],
        "download-sec-companyfacts": components[2],
        "valuation": components[3],
        "risk-events": components[4],
    }

    def runner(command: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
        component = next(item for token, item in component_by_token.items() if token in command)
        calls.append(component.component_id)
        artifact_paths = component.expected_paths
        for expected in artifact_paths:
            if component.component_id == failing_component and expected != artifact_paths[0]:
                continue
            if expected.suffix:
                expected.parent.mkdir(parents=True, exist_ok=True)
                expected.write_text(
                    f"{component.component_id}:{expected.name}\n",
                    encoding="utf-8",
                )
            else:
                expected.mkdir(parents=True, exist_ok=True)
        return_code = 1 if component.component_id == failing_component else 0
        return subprocess.CompletedProcess(
            command,
            return_code,
            stdout=f"{component.component_id} stdout\n",
            stderr="provider failure\n" if return_code else "",
        )

    return runner


def test_market_macro_snapshot_uses_validated_immutable_publication(
    monkeypatch,
    tmp_path: Path,
) -> None:
    policy_path = tmp_path / "capture_policy.yaml"
    _write_policy(policy_path, tmp_path)
    policy = load_daily_input_capture_policy(policy_path, project_root=tmp_path)
    paths = daily_input_capture_paths(date(2026, 7, 27), policy=policy)
    component = build_daily_input_capture_components(
        as_of=date(2026, 7, 27),
        paths=paths,
        policy=policy,
        project_root=tmp_path,
    )[0]
    immutable_root = tmp_path / "immutable"
    immutable_sources = []
    for filename in (
        "prices_daily.csv",
        "prices_marketstack_daily.csv",
        "rates_daily.csv",
        "download_manifest.csv",
    ):
        source = immutable_root / filename
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text(f"immutable:{filename}\n", encoding="utf-8")
        immutable_sources.append(source)
    monkeypatch.setattr(
        capture_module,
        "resolve_download_publication",
        lambda **_: SimpleNamespace(
            prices_path=immutable_sources[0],
            secondary_prices_path=immutable_sources[1],
            rates_path=immutable_sources[2],
            manifest_path=immutable_sources[3],
        ),
    )

    capture_module._snapshot_component_sources(component)

    assert [
        destination.read_text(encoding="utf-8") for destination in component.expected_paths
    ] == [source.read_text(encoding="utf-8") for source in immutable_sources]


def test_capture_attempts_all_components_and_retains_partial_inputs(tmp_path: Path) -> None:
    policy_path = tmp_path / "capture_policy.yaml"
    _write_policy(policy_path, tmp_path)
    policy = load_daily_input_capture_policy(policy_path, project_root=tmp_path)
    paths = daily_input_capture_paths(date(2026, 7, 27), policy=policy)
    components = build_daily_input_capture_components(
        as_of=date(2026, 7, 27),
        paths=paths,
        policy=policy,
        project_root=tmp_path,
    )
    calls: list[str] = []

    result = capture_daily_inputs(
        as_of=date(2026, 7, 27),
        project_root=tmp_path,
        policy_path=policy_path,
        runner=_runner_for_components(
            components,
            failing_component="sec_companyfacts",
            calls=calls,
        ),
        snapshotter=lambda _component: None,
        generated_at=datetime(2026, 7, 28, 0, 30, tzinfo=UTC),
    )

    assert result.status == "PARTIAL_CAPTURE"
    assert calls == [
        "market_macro",
        "fmp_forward_pit",
        "sec_companyfacts",
        "fmp_valuation",
        "official_policy_sources",
    ]
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["status"] == "PARTIAL_CAPTURE"
    assert manifest["data_quality_status"] == "NOT_EVALUATED"
    assert manifest["consumer_cutover_allowed"] is False
    assert manifest["production_effect"] == "none"
    assert paths.valuation_validation_report_path.exists()
    ledger = json.loads(result.gap_ledger_path.read_text(encoding="utf-8"))
    assert ledger["sessions"][-1]["status"] == "PARTIAL_CAPTURE"
    market_result = next(
        item for item in manifest["component_results"] if item["component_id"] == "market_macro"
    )
    assert market_result["attempt_count"] == 1
    assert market_result["max_attempts"] == 2

    sec_result = next(
        item for item in manifest["component_results"] if item["component_id"] == "sec_companyfacts"
    )
    sec_result["required"] = False
    result.manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    required_tamper_validation = validate_daily_input_capture_manifest(
        result.manifest_path,
        project_root=tmp_path,
        policy_path=policy_path,
    )
    assert required_tamper_validation["status"] == "FAIL"
    assert "COMPONENT_REQUIRED_MISMATCH" in {
        issue["code"] for issue in required_tamper_validation["issues"]
    }


def test_market_macro_exhausts_two_attempts_before_other_sources_continue(
    tmp_path: Path,
) -> None:
    policy_path = tmp_path / "capture_policy.yaml"
    _write_policy(policy_path, tmp_path)
    policy = load_daily_input_capture_policy(policy_path, project_root=tmp_path)
    paths = daily_input_capture_paths(date(2026, 7, 27), policy=policy)
    components = build_daily_input_capture_components(
        as_of=date(2026, 7, 27),
        paths=paths,
        policy=policy,
        project_root=tmp_path,
    )
    calls: list[str] = []

    result = capture_daily_inputs(
        as_of=date(2026, 7, 27),
        project_root=tmp_path,
        policy_path=policy_path,
        runner=_runner_for_components(
            components,
            failing_component="market_macro",
            calls=calls,
        ),
        snapshotter=lambda _component: None,
        generated_at=datetime(2026, 7, 28, 0, 30, tzinfo=UTC),
    )

    assert result.status == "PARTIAL_CAPTURE"
    assert calls == [
        "market_macro",
        "market_macro",
        "fmp_forward_pit",
        "sec_companyfacts",
        "fmp_valuation",
        "official_policy_sources",
    ]
    market_result = result.component_results[0]
    assert market_result["attempt_count"] == 2
    assert market_result["max_attempts"] == 2
    assert market_result["status"] == "FAIL"


def test_capture_checksum_tamper_fails_closed_and_gap_ledger_keeps_missed_sessions(
    tmp_path: Path,
) -> None:
    policy_path = tmp_path / "capture_policy.yaml"
    _write_policy(policy_path, tmp_path)
    policy = load_daily_input_capture_policy(policy_path, project_root=tmp_path)
    paths = daily_input_capture_paths(date(2026, 7, 29), policy=policy)
    components = build_daily_input_capture_components(
        as_of=date(2026, 7, 29),
        paths=paths,
        policy=policy,
        project_root=tmp_path,
    )
    calls: list[str] = []

    result = capture_daily_inputs(
        as_of=date(2026, 7, 29),
        project_root=tmp_path,
        policy_path=policy_path,
        runner=_runner_for_components(components, calls=calls),
        snapshotter=lambda _component: None,
        generated_at=datetime(2026, 7, 30, 0, 30, tzinfo=UTC),
    )

    assert result.status == "CAPTURED"
    ledger = json.loads(result.gap_ledger_path.read_text(encoding="utf-8"))
    assert [row["status"] for row in ledger["sessions"]] == [
        "MISSED",
        "MISSED",
        "CAPTURED",
    ]
    paths.valuation_validation_report_path.write_text("tampered\n", encoding="utf-8")
    validation = validate_daily_input_capture_manifest(
        result.manifest_path,
        project_root=tmp_path,
        policy_path=policy_path,
    )
    assert validation["status"] == "FAIL"
    assert "ARTIFACT_CHECKSUM_MISMATCH" in {issue["code"] for issue in validation["issues"]}


def test_daily_plan_binds_capture_before_strict_consumers() -> None:
    plan = build_daily_ops_plan(
        as_of=date(2026, 7, 27),
        skip_risk_event_openai_precheck=True,
    )
    step_ids = [step.step_id for step in plan.steps]

    assert step_ids[:3] == [
        "capture_daily_inputs",
        "validate_data",
        "pit_snapshots_build_manifest",
    ]
    assert "download_data" not in step_ids
    assert "pit_snapshots_fetch_fmp_forward" not in step_ids
    assert "sec_companyfacts" not in step_ids
    assert "valuation_snapshots" not in step_ids
    capture_step = plan.steps[0]
    assert capture_step.blocks_downstream is True
    assert capture_step.required_env_vars == ()
    sec_step = next(step for step in plan.steps if step.step_id == "sec_metrics")
    score_step = next(step for step in plan.steps if step.step_id == "score_daily")
    assert "daily_input_capture" in " ".join(sec_step.command)
    assert "--valuation-path" in score_step.command
    assert "daily_input_capture" in " ".join(score_step.command)
