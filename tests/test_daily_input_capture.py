from __future__ import annotations

import json
import subprocess
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import ai_trading_system.daily_input_capture as capture_module
from ai_trading_system.daily_input_capture import (
    build_daily_input_capture_components,
    build_daily_input_capture_recovery_queue,
    capture_daily_inputs,
    daily_input_capture_paths,
    load_daily_input_capture_policy,
    validate_daily_input_capture_manifest,
    validate_daily_input_capture_recovery_queue,
)
from ai_trading_system.ops_daily import build_daily_ops_plan


def _write_policy(path: Path, project_root: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            (
                "schema_version: daily_input_capture_policy.v2",
                "policy_version: daily_input_capture_test_v2",
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
                f"  source_control_root: {project_root.as_posix()}/outputs/source_control",
                "source_control:",
                "  blocker_taxonomy_version: daily_input_capture_blockers_v1",
                "  lease_ttl_seconds: 1800",
                "  component_policies:",
                "    market_macro:",
                "      source_revision: daily_input_capture_test_v2",
                "      supersedes_source_revisions: []",
                "      max_attempts: 2",
                "      retry_delay_seconds: 0",
                "      retryable_blocker_codes:",
                "        - PROVIDER_UNAVAILABLE",
                "        - REQUEST_FAILED",
                "      recovery_mode: IMMUTABLE_RAW_BACKFILL",
                "    fmp_forward_pit:",
                "      source_revision: daily_input_capture_test_v2",
                "      supersedes_source_revisions: []",
                "      max_attempts: 1",
                "      retry_delay_seconds: 0",
                "      retryable_blocker_codes: []",
                "      recovery_mode: HISTORICAL_RECAPTURE_FORBIDDEN",
                "    sec_companyfacts:",
                "      source_revision: daily_input_capture_test_v2",
                "      supersedes_source_revisions: []",
                "      max_attempts: 1",
                "      retry_delay_seconds: 0",
                "      retryable_blocker_codes: []",
                "      recovery_mode: MANUAL_NON_PIT_RAW_REVIEW",
                "    fmp_valuation:",
                "      source_revision: daily_input_capture_test_v2",
                "      supersedes_source_revisions: []",
                "      max_attempts: 1",
                "      retry_delay_seconds: 0",
                "      retryable_blocker_codes: []",
                "      recovery_mode: HISTORICAL_RECAPTURE_FORBIDDEN",
                "    official_policy_sources:",
                "      source_revision: daily_input_capture_test_v2",
                "      supersedes_source_revisions: []",
                "      max_attempts: 1",
                "      retry_delay_seconds: 0",
                "      retryable_blocker_codes: []",
                "      recovery_mode: HISTORICAL_RECAPTURE_FORBIDDEN",
                "  rationale: test policy",
                "  validation_evidence: tests",
                "  review_condition: test change",
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


def test_reviewed_policy_governs_source_control_and_recovery_modes() -> None:
    policy = load_daily_input_capture_policy()

    assert policy.policy_version == "daily_input_capture_v5"
    assert policy.tracking_start == date(2026, 7, 24)
    assert policy.blocker_taxonomy_version == "daily_input_capture_blockers_v1"
    assert policy.lease_ttl_seconds == 1800
    assert policy.component_policies["market_macro"].max_attempts == 2
    assert policy.component_policies["market_macro"].source_revision == (
        "market_macro_v4_relocation_recovery"
    )
    assert policy.component_policies["market_macro"].supersedes_source_revisions == (
        "daily_input_capture_v3",
    )
    assert policy.component_policies["market_macro"].retry_delay_seconds == 5
    assert policy.component_policies["market_macro"].retryable_blocker_codes == (
        "PROVIDER_UNAVAILABLE",
        "REQUEST_FAILED",
    )
    assert policy.component_policies["market_macro"].recovery_mode == (
        "IMMUTABLE_RAW_BACKFILL"
    )
    assert policy.component_policies["sec_companyfacts"].recovery_mode == (
        "MANUAL_NON_PIT_RAW_REVIEW"
    )
    assert {
        policy.component_policies[component_id].recovery_mode
        for component_id in (
            "fmp_forward_pit",
            "fmp_valuation",
            "official_policy_sources",
        )
    } == {"HISTORICAL_RECAPTURE_FORBIDDEN"}


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
    assert result.closure_passed is True
    assert result.passed is False
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
    queue = json.loads(paths.recovery_queue_json.read_text(encoding="utf-8"))
    assert queue["status"] == "GAPS_PRESENT"
    assert queue["item_count"] == 1
    assert queue["items"][0]["component_id"] == "sec_companyfacts"
    assert queue["items"][0]["action_status"] == "OWNER_REVIEW_REQUIRED"
    assert queue["items"][0]["strict_pit_eligible"] is False
    queue_validation = validate_daily_input_capture_recovery_queue(
        paths.recovery_queue_json,
        project_root=tmp_path,
        policy_path=policy_path,
    )
    assert queue_validation["status"] == "PASS"
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
    assert market_result["blocker_code"] == "REQUEST_FAILED"
    assert market_result["attempt_history"][0]["retry_allowed"] is True
    assert market_result["attempt_history"][0]["retry_after_seconds"] == 0
    assert market_result["attempt_history"][1]["retry_allowed"] is False


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

    assert step_ids[:4] == [
        "capture_daily_inputs",
        "validate_data",
        "pit_snapshots_project_fmp_forward",
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


def test_source_scoped_state_reuses_pass_without_repeating_provider_requests(
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
    first = capture_daily_inputs(
        as_of=date(2026, 7, 27),
        project_root=tmp_path,
        policy_path=policy_path,
        runner=_runner_for_components(components, calls=calls),
        snapshotter=lambda _component: None,
        generated_at=datetime(2026, 7, 28, 0, 30, tzinfo=UTC),
    )
    assert first.status == "CAPTURED"
    assert len(calls) == 5

    def unexpected_runner(
        command: tuple[str, ...],
        **_: object,
    ) -> subprocess.CompletedProcess[str]:
        raise AssertionError(f"provider request repeated: {command}")

    second = capture_daily_inputs(
        as_of=date(2026, 7, 27),
        project_root=tmp_path,
        policy_path=policy_path,
        runner=unexpected_runner,
        snapshotter=lambda _component: None,
        generated_at=datetime(2026, 7, 28, 0, 35, tzinfo=UTC),
    )

    assert second.status == "CAPTURED"
    assert all(item["source_lease_status"] == "REUSED_PASS" for item in second.component_results)
    assert all(item["idempotency_reused"] is True for item in second.component_results)
    assert all(
        str(item["source_idempotency_key"]).startswith("daily-input-source-")
        for item in second.component_results
    )


def test_fmp_pass_reuse_excludes_legacy_consumer_mutated_aggregate_manifest(
    tmp_path: Path,
) -> None:
    policy_path = tmp_path / "capture_policy.yaml"
    _write_policy(policy_path, tmp_path)
    policy = load_daily_input_capture_policy(policy_path, project_root=tmp_path)
    as_of = date(2026, 7, 27)
    paths = daily_input_capture_paths(as_of, policy=policy)
    components = build_daily_input_capture_components(
        as_of=as_of,
        paths=paths,
        policy=policy,
        project_root=tmp_path,
    )
    first = capture_daily_inputs(
        as_of=as_of,
        project_root=tmp_path,
        policy_path=policy_path,
        runner=_runner_for_components(components, calls=[]),
        snapshotter=lambda _component: None,
        generated_at=datetime(2026, 7, 28, 0, 30, tzinfo=UTC),
    )
    assert first.status == "CAPTURED"
    source_state_path = (
        policy.source_control_root / as_of.isoformat() / "fmp_forward_pit" / "state.json"
    )
    source_state_bytes = source_state_path.read_bytes()
    paths.pit_manifest_path.write_text("consumer-expanded-aggregate\n", encoding="utf-8")

    second = capture_daily_inputs(
        as_of=as_of,
        project_root=tmp_path,
        policy_path=policy_path,
        runner=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("provider request repeated")
        ),
        snapshotter=lambda _component: None,
        generated_at=datetime(2026, 7, 28, 0, 35, tzinfo=UTC),
    )

    fmp = next(
        item for item in second.component_results if item["component_id"] == "fmp_forward_pit"
    )
    assert second.status == "CAPTURED"
    assert fmp["idempotency_reused"] is True
    assert fmp["artifact_reuse_scope"] == "SOURCE_OWNED_ONLY"
    assert fmp["excluded_non_authoritative_artifacts"] == [
        "data/raw/daily_input_capture/2026-07-27/pit_snapshot_manifest.csv"
    ]
    assert all(
        item["path"]
        != "data/raw/daily_input_capture/2026-07-27/pit_snapshot_manifest.csv"
        for item in fmp["artifacts"]
    )
    assert source_state_path.read_bytes() == source_state_bytes


def test_fmp_pass_reuse_still_rejects_source_owned_artifact_drift(
    tmp_path: Path,
) -> None:
    policy_path = tmp_path / "capture_policy.yaml"
    _write_policy(policy_path, tmp_path)
    policy = load_daily_input_capture_policy(policy_path, project_root=tmp_path)
    as_of = date(2026, 7, 27)
    paths = daily_input_capture_paths(as_of, policy=policy)
    components = build_daily_input_capture_components(
        as_of=as_of,
        paths=paths,
        policy=policy,
        project_root=tmp_path,
    )
    capture_daily_inputs(
        as_of=as_of,
        project_root=tmp_path,
        policy_path=policy_path,
        runner=_runner_for_components(components, calls=[]),
        snapshotter=lambda _component: None,
        generated_at=datetime(2026, 7, 28, 0, 30, tzinfo=UTC),
    )
    paths.pit_normalized_path.write_text("tampered\n", encoding="utf-8")

    second = capture_daily_inputs(
        as_of=as_of,
        project_root=tmp_path,
        policy_path=policy_path,
        runner=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("provider request repeated")
        ),
        snapshotter=lambda _component: None,
        generated_at=datetime(2026, 7, 28, 0, 35, tzinfo=UTC),
    )

    fmp = next(
        item for item in second.component_results if item["component_id"] == "fmp_forward_pit"
    )
    assert second.status == "PARTIAL_CAPTURE"
    assert fmp["blocker_code"] == "SOURCE_STATE_INVALID"
    assert fmp["error_summary"] == "terminal PASS source state artifact drift"


def test_component_revision_reopens_only_superseded_failed_source(
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
    first_calls: list[str] = []
    first = capture_daily_inputs(
        as_of=date(2026, 7, 27),
        project_root=tmp_path,
        policy_path=policy_path,
        runner=_runner_for_components(
            components,
            failing_component="market_macro",
            calls=first_calls,
        ),
        snapshotter=lambda _component: None,
        generated_at=datetime(2026, 7, 28, 0, 30, tzinfo=UTC),
    )
    assert first.status == "PARTIAL_CAPTURE"
    legacy_market_state = (
        policy.source_control_root / "2026-07-27" / "market_macro" / "state.json"
    )
    legacy_market_state_raw = legacy_market_state.read_bytes()

    revised = policy_path.read_text(encoding="utf-8").replace(
        "policy_version: daily_input_capture_test_v2",
        "policy_version: daily_input_capture_test_v3",
    )
    revised = revised.replace(
        (
            "    market_macro:\n"
            "      source_revision: daily_input_capture_test_v2\n"
            "      supersedes_source_revisions: []"
        ),
        (
            "    market_macro:\n"
            "      source_revision: market_macro_test_v3\n"
            "      supersedes_source_revisions:\n"
            "        - daily_input_capture_test_v2"
        ),
        1,
    )
    policy_path.write_text(revised, encoding="utf-8")
    revised_policy = load_daily_input_capture_policy(policy_path, project_root=tmp_path)
    revised_paths = daily_input_capture_paths(date(2026, 7, 27), policy=revised_policy)
    revised_components = build_daily_input_capture_components(
        as_of=date(2026, 7, 27),
        paths=revised_paths,
        policy=revised_policy,
        project_root=tmp_path,
    )
    second_calls: list[str] = []
    second = capture_daily_inputs(
        as_of=date(2026, 7, 27),
        project_root=tmp_path,
        policy_path=policy_path,
        runner=_runner_for_components(revised_components, calls=second_calls),
        snapshotter=lambda _component: None,
        generated_at=datetime(2026, 7, 28, 0, 35, tzinfo=UTC),
    )

    assert second.status == "CAPTURED"
    assert second_calls == ["market_macro"]
    market = second.component_results[0]
    assert market["source_revision"] == "market_macro_test_v3"
    assert market["idempotency_reused"] is False
    assert market["superseded_state_path"] == (
        "outputs/source_control/2026-07-27/market_macro/state.json"
    )
    assert all(
        item["idempotency_reused"] is True for item in second.component_results[1:]
    )
    assert legacy_market_state.read_bytes() == legacy_market_state_raw
    assert (
        policy.source_control_root
        / "2026-07-27"
        / "market_macro"
        / "revisions"
        / "market_macro_test_v3"
        / "state.json"
    ).is_file()


def test_non_retryable_quota_blocker_does_not_consume_other_source_budgets(
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
    normal_runner = _runner_for_components(components, calls=calls)

    def quota_runner(
        command: tuple[str, ...],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        if "download-data" in command:
            calls.append("market_macro")
            return subprocess.CompletedProcess(
                command,
                1,
                stdout="",
                stderr="HTTP 429 provider rate limit quota exhausted",
            )
        return normal_runner(command, **kwargs)

    result = capture_daily_inputs(
        as_of=date(2026, 7, 27),
        project_root=tmp_path,
        policy_path=policy_path,
        runner=quota_runner,
        snapshotter=lambda _component: None,
        generated_at=datetime(2026, 7, 28, 0, 30, tzinfo=UTC),
    )

    market = result.component_results[0]
    assert result.status == "PARTIAL_CAPTURE"
    assert market["blocker_code"] == "PROVIDER_QUOTA_EXHAUSTED"
    assert market["attempt_count"] == 1
    assert market["max_attempts"] == 2
    assert market["retry_allowed"] is False
    assert calls == [
        "market_macro",
        "fmp_forward_pit",
        "sec_companyfacts",
        "fmp_valuation",
        "official_policy_sources",
    ]
    assert all(item["attempt_count"] == 1 for item in result.component_results[1:])


def test_active_source_lease_blocks_only_its_component(tmp_path: Path) -> None:
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
    lock_path = (
        policy.source_control_root
        / "2026-07-27"
        / "market_macro"
        / "active.lock"
    )
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text(
        json.dumps(
            {
                "schema_version": "daily_input_capture_source_lease.v1",
                "lease_id": "source-lease-existing",
                "source_idempotency_key": "existing",
                "component_id": "market_macro",
                "as_of": "2026-07-27",
                "acquired_at": "2026-07-28T00:00:00+00:00",
                "expires_at": "2026-07-28T01:00:00+00:00",
                "pid": 1,
                "production_effect": "none",
            }
        ),
        encoding="utf-8",
    )
    calls: list[str] = []
    fixed_now = datetime(2026, 7, 28, 0, 30, tzinfo=UTC)
    result = capture_daily_inputs(
        as_of=date(2026, 7, 27),
        project_root=tmp_path,
        policy_path=policy_path,
        runner=_runner_for_components(components, calls=calls),
        snapshotter=lambda _component: None,
        clock=lambda: fixed_now,
        generated_at=fixed_now,
    )

    market = result.component_results[0]
    assert market["blocker_code"] == "SOURCE_LEASE_CONFLICT"
    assert market["source_lease_status"] == "BLOCKED_ACTIVE_LEASE"
    assert market["attempt_count"] == 0
    assert calls == [
        "fmp_forward_pit",
        "sec_companyfacts",
        "fmp_valuation",
        "official_policy_sources",
    ]
    assert result.closure_passed is True


def test_expired_source_lease_is_audited_before_recovery(tmp_path: Path) -> None:
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
    source_root = policy.source_control_root / "2026-07-27" / "market_macro"
    lock_path = source_root / "active.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text(
        json.dumps(
            {
                "schema_version": "daily_input_capture_source_lease.v1",
                "lease_id": "source-lease-stale",
                "source_idempotency_key": "stale",
                "component_id": "market_macro",
                "as_of": "2026-07-27",
                "acquired_at": "2026-07-27T23:00:00+00:00",
                "expires_at": "2026-07-27T23:30:00+00:00",
                "pid": 1,
                "production_effect": "none",
            }
        ),
        encoding="utf-8",
    )
    fixed_now = datetime(2026, 7, 28, 0, 30, tzinfo=UTC)
    calls: list[str] = []
    result = capture_daily_inputs(
        as_of=date(2026, 7, 27),
        project_root=tmp_path,
        policy_path=policy_path,
        runner=_runner_for_components(components, calls=calls),
        snapshotter=lambda _component: None,
        clock=lambda: fixed_now,
        generated_at=fixed_now,
    )

    assert result.status == "CAPTURED"
    assert result.component_results[0]["source_lease_status"] == "STALE_RECLAIMED"
    assert (
        source_root / "lease_history" / "source-lease-stale.expired.json"
    ).is_file()
    assert not lock_path.exists()


def test_recovery_queue_for_missed_sessions_never_authorizes_historical_pit(
    tmp_path: Path,
) -> None:
    policy_path = tmp_path / "capture_policy.yaml"
    _write_policy(policy_path, tmp_path)
    ledger = {
        "schema_version": "daily_input_capture_gap_ledger.v1",
        "as_of": "2026-07-27",
        "sessions": [
            {
                "session_date": "2026-07-27",
                "status": "MISSED",
                "manifest_path": None,
            }
        ],
    }
    queue = build_daily_input_capture_recovery_queue(
        gap_ledger=ledger,
        project_root=tmp_path,
        policy_path=policy_path,
    )
    queue_path = tmp_path / "recovery_queue.json"
    queue_path.write_text(json.dumps(queue), encoding="utf-8")

    assert queue["item_count"] == 5
    assert queue["counts"] == {
        "READY_FOR_MANUAL_RECOVERY": 1,
        "OWNER_REVIEW_REQUIRED": 1,
        "INSUFFICIENT_DATA": 3,
    }
    assert all(item["automatic_execution_allowed"] is False for item in queue["items"])
    assert all(item["strict_pit_eligible"] is False for item in queue["items"])
    assert validate_daily_input_capture_recovery_queue(
        queue_path,
        project_root=tmp_path,
        policy_path=policy_path,
    )["status"] == "PASS"

    queue["historical_strict_pit_backfill_allowed"] = True
    queue_path.write_text(json.dumps(queue), encoding="utf-8")
    tampered = validate_daily_input_capture_recovery_queue(
        queue_path,
        project_root=tmp_path,
        policy_path=policy_path,
    )
    assert tampered["status"] == "FAIL"
    assert "QUEUE_SAFETY_BOUNDARY_MISMATCH" in {issue["code"] for issue in tampered["issues"]}
