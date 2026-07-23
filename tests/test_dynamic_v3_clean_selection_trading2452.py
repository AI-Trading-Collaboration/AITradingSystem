from __future__ import annotations

import json
import shutil
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from ai_trading_system import dynamic_v3_trading2452_historical_evaluator as evaluator_module
from ai_trading_system.contracts.research_context import ResearchEvaluationContext
from ai_trading_system.data.quality import (
    DataFileSummary,
    DataQualityIssue,
    DataQualityReport,
    Severity,
)
from ai_trading_system.dynamic_v3_clean_selection_trading2452 import (
    DEFAULT_PACKAGE_ROOT,
    SAFETY,
    build_trading2452_package,
    validate_trading2452_package,
)
from ai_trading_system.dynamic_v3_trading2452_historical_evaluator import (
    DEFAULT_WORKERS,
    DynamicV3Trading2452EvaluatorError,
    _assert_execution_boundary,
    _effective_windows,
    select_train_only_top_n,
)
from ai_trading_system.trading_calendar import is_us_equity_trading_day


def test_trading2452_closed_package_stays_frozen_and_live_policy_drift_fails_closed() -> None:
    manifest_path = DEFAULT_PACKAGE_ROOT / "package_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    validation = validate_trading2452_package()

    assert manifest["package_id"] == "dynamic-v3-clean-trading2452_11991ac7965cfcd7aa18"
    assert sha256(manifest_path.read_bytes()).hexdigest() == (
        "8319cd55d727701a2ae57c556ac8bca2bbae06b2e6bc61d589b290520ff6c47f"
    )
    failed_checks = {
        str(check["check_id"]) for check in validation["checks"] if not check["passed"]
    }
    assert validation["status"] == "FAIL"
    assert validation["failed_check_count"] == 7
    assert validation["eligibility_status"] == "BLOCKED_INVALID_TRADING2452_PACKAGE"
    assert failed_checks == {
        "selection_input_commitments_fresh",
        "content_recomputed:research_context.json",
        "content_recomputed:preregistration.json",
        "content_recomputed:campaign.json",
        "content_recomputed:source_contract.json",
        "content_recomputed:eligibility.json",
        "content_recomputed:package_manifest.json",
    }
    assert validation["prospective_holdout_access_allowed"] is False
    assert validation["unbiased_oos_claim_allowed"] is False
    assert validation["production_effect"] == "none"
    assert validation["broker_action"] == "none"


def test_historical_evaluator_uses_reviewed_parallel_worker_default() -> None:
    assert DEFAULT_WORKERS == 24


def test_historical_evaluator_help_exits_before_research_execution(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def _unexpected_execution() -> dict[str, Any]:
        raise AssertionError("--help must exit before research execution")

    monkeypatch.setattr(
        evaluator_module,
        "run_trading2452_historical_seen_evaluator",
        _unexpected_execution,
    )

    with pytest.raises(SystemExit) as raised:
        evaluator_module.main(["--help"])

    assert raised.value.code == 0
    assert "owner-authorized TRADING-2452" in capsys.readouterr().out


def test_active_package_uses_2021_primary_window_six_folds_and_recent_diagnostic() -> None:
    payloads = build_trading2452_package()
    context = ResearchEvaluationContext.from_dict(payloads["research_context.json"])
    eligibility = payloads["eligibility.json"]
    manifest = payloads["package_manifest.json"]
    active_text = json.dumps(payloads, ensure_ascii=False, sort_keys=True)

    assert context.research_window_start.isoformat() == "2021-02-22"
    assert context.regime_start.isoformat() == "2021-02-22"
    assert context.market_regime_id == "unified_primary_2021"
    assert context.research_window_id == "exact_three_asset_validated"
    assert eligibility["historical_fold_count"] == 6
    assert eligibility["main_historical_end"] == "2025-12-31"
    assert eligibility["recent_known_diagnostic"] == {
        "catalog_id": "recent_known_pre_freeze_2026_v1",
        "start": "2026-01-02",
        "end": "2026-07-20",
        "included_in_main_fold_ranking": False,
    }
    assert eligibility["prospective_holdout"]["start"] == "2026-07-22"
    assert eligibility["prospective_holdout"]["accessed"] is False
    assert "2022-12-01" not in active_text
    assert manifest["result_artifacts_consumed"] == []


def test_candidate_universe_is_exact_300_and_excludes_result_or_legacy_inputs() -> None:
    payloads = build_trading2452_package()
    universe = payloads["candidate_universe.json"]
    manifest = payloads["package_manifest.json"]
    candidates = universe["candidates"]
    selection_paths = [
        item["path"]
        for key, item in manifest["selection_input_commitments"].items()
        if not key.startswith("policy:")
    ]

    assert universe["candidate_count"] == 300
    assert len(candidates) == 300
    assert len({item["candidate_id"] for item in candidates}) == 300
    assert universe["candidate_universe_origin"] == "preregistered_candidate_universe"
    assert universe["result_artifacts_consumed"] == []
    assert not any(
        token in path.lower()
        for path in selection_paths
        for token in (
            "leaderboard",
            "candidate_results",
            "real_evaluation",
            "trading2451_dynamic_v3_clean_selection",
        )
    )


def test_legacy_trading2451_package_is_preserved_only_as_immutable_evidence() -> None:
    manifest = build_trading2452_package()["package_manifest.json"]
    legacy = manifest["historical_immutable_evidence"]

    assert legacy["package_role"] == "HISTORICAL_IMMUTABLE_PREREGISTRATION_EVIDENCE_ONLY"
    assert legacy["selection_input"] is False
    assert legacy["active_policy_source"] is False
    assert legacy["artifact_count"] == 9
    assert manifest["legacy_immutable_evidence_selection_allowed"] is False
    assert all(
        item["path"].startswith("inputs/research/trading2451_dynamic_v3_clean_selection/")
        for item in legacy["artifacts"].values()
    )


def test_candidate_content_or_order_tamper_fails_closed(tmp_path: Path) -> None:
    package = _copy_package(tmp_path)
    path = package / "candidate_universe.json"
    payload = _read_json(path)
    payload["candidates"][0], payload["candidates"][1] = (
        payload["candidates"][1],
        payload["candidates"][0],
    )
    _write_json(path, payload)

    validation = validate_trading2452_package(package_root=package)

    assert validation["status"] == "FAIL"
    assert validation["eligibility_status"] == "BLOCKED_INVALID_TRADING2452_PACKAGE"


def test_result_or_legacy_source_injection_fails_closed(tmp_path: Path) -> None:
    for name, replacement in (
        (
            "result",
            "reports/etf_portfolio/dynamic_v3_rescue/sweeps/legacy/leaderboard.json",
        ),
        (
            "legacy_package",
            "inputs/research/trading2451_dynamic_v3_clean_selection/candidate_universe.json",
        ),
    ):
        package = _copy_package(tmp_path / name)
        path = package / "selection_rule.yaml"
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        payload["candidate_universe"]["source_config"] = replacement
        path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

        validation = validate_trading2452_package(package_root=package)

        assert validation["status"] == "FAIL"


def test_window_boundary_or_test_selection_tamper_fails_closed(tmp_path: Path) -> None:
    primary = _copy_package(tmp_path / "primary")
    windows_path = primary / "window_catalog.yaml"
    windows = yaml.safe_load(windows_path.read_text(encoding="utf-8"))
    windows["primary_research_window"]["start_date"] = "2022-12-01"
    windows_path.write_text(yaml.safe_dump(windows, sort_keys=False), encoding="utf-8")
    assert validate_trading2452_package(package_root=primary)["status"] == "FAIL"

    overlap = _copy_package(tmp_path / "overlap")
    windows_path = overlap / "window_catalog.yaml"
    windows = yaml.safe_load(windows_path.read_text(encoding="utf-8"))
    windows["prospective_holdout"]["start"] = "2025-01-02"
    windows_path.write_text(yaml.safe_dump(windows, sort_keys=False), encoding="utf-8")
    assert validate_trading2452_package(package_root=overlap)["status"] == "FAIL"

    leakage = _copy_package(tmp_path / "leakage")
    selection_path = leakage / "selection_rule.yaml"
    selection = yaml.safe_load(selection_path.read_text(encoding="utf-8"))
    selection["selection"]["test_metric_selection_allowed"] = True
    selection_path.write_text(yaml.safe_dump(selection, sort_keys=False), encoding="utf-8")
    assert validate_trading2452_package(package_root=leakage)["status"] == "FAIL"


def test_authorization_or_safety_tamper_fails_closed(tmp_path: Path) -> None:
    package = _copy_package(tmp_path)
    path = package / "eligibility.json"
    payload = _read_json(path)
    payload["prospective_holdout_access_allowed"] = True
    payload["safety"]["prospective_holdout_access_allowed"] = True
    _write_json(path, payload)

    validation = validate_trading2452_package(package_root=package)

    assert validation["status"] == "FAIL"
    assert validation["prospective_holdout_access_allowed"] is False
    assert validation["safety"] == SAFETY


def test_evidence_row_floor_policy_tamper_fails_closed(tmp_path: Path) -> None:
    package = _copy_package(tmp_path)
    path = package / "selection_rule.yaml"
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    payload["execution"]["evidence_row_floors"]["minimum_train_rows"] = 125
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    validation = validate_trading2452_package(package_root=package)

    assert validation["status"] == "FAIL"
    assert validation["eligibility_status"] == "BLOCKED_INVALID_TRADING2452_PACKAGE"


def test_train_only_top20_ranking_is_deterministic_and_ignores_test_fields() -> None:
    rows = [
        {
            "candidate_id": f"candidate_{index:03d}",
            "evidence_status": "COMPLETE",
            "gate": "review_required",
            "selection_score": round(index / 1000, 6),
            "parameters": {"index": index},
            "test_metric_that_must_not_be_used": 1000 - index,
        }
        for index in range(300)
    ]
    rows[299]["selection_score"] = rows[298]["selection_score"]

    selected = select_train_only_top_n(rows, top_n=20)

    assert len(selected) == 20
    assert selected[0]["candidate_id"] == "candidate_298"
    assert selected[1]["candidate_id"] == "candidate_299"
    assert {row["candidate_id"] for row in selected} == {
        f"candidate_{index:03d}" for index in range(280, 300)
    }

    changed_test_only = [dict(row) for row in rows]
    for row in changed_test_only:
        row["test_metric_that_must_not_be_used"] = -999999
    assert [
        row["candidate_id"] for row in select_train_only_top_n(changed_test_only, top_n=20)
    ] == [row["candidate_id"] for row in selected]


def test_effective_six_fold_windows_apply_xnys_purge_and_embargo() -> None:
    windows = yaml.safe_load(
        (DEFAULT_PACKAGE_ROOT / "window_catalog.yaml").read_text(encoding="utf-8")
    )
    trading_dates = []
    cursor = date(2021, 2, 22)
    while cursor <= date(2026, 7, 20):
        if is_us_equity_trading_day(cursor):
            trading_dates.append(cursor)
        cursor += timedelta(days=1)

    effective = _effective_windows(
        replay=windows["historical_protocol_replay"],
        trading_dates=trading_dates,
    )

    assert len(effective) == 6
    assert effective[0]["effective_train_start"] == "2021-02-22"
    assert effective[0]["effective_train_end"] == "2022-12-29"
    assert effective[0]["effective_test_start"] == "2023-01-04"
    assert effective[0]["effective_test_end"] == "2023-06-30"
    assert effective[-1]["effective_test_end"] == "2025-12-31"
    assert all(row["purge_trading_days"] == 1 for row in effective)
    assert all(row["embargo_trading_days"] == 1 for row in effective)


def test_evaluator_execution_boundary_rejects_legacy_or_prospective_access() -> None:
    selection = yaml.safe_load(
        (DEFAULT_PACKAGE_ROOT / "selection_rule.yaml").read_text(encoding="utf-8")
    )
    windows = yaml.safe_load(
        (DEFAULT_PACKAGE_ROOT / "window_catalog.yaml").read_text(encoding="utf-8")
    )

    _assert_execution_boundary(selection=selection, windows=windows)

    legacy = json.loads(json.dumps(windows))
    legacy["primary_research_window"]["start_date"] = "2022-12-01"
    with pytest.raises(DynamicV3Trading2452EvaluatorError):
        _assert_execution_boundary(selection=selection, windows=legacy)

    prospective = json.loads(json.dumps(selection))
    prospective["safety"]["prospective_holdout_access_allowed"] = True
    with pytest.raises(DynamicV3Trading2452EvaluatorError):
        _assert_execution_boundary(selection=prospective, windows=windows)


@pytest.mark.parametrize(
    ("consistency_start", "issues"),
    [
        (
            date(2021, 2, 22),
            (
                DataQualityIssue(
                    severity=Severity.ERROR,
                    code="test_data_quality_blocker",
                    message="test-only blocker",
                ),
            ),
        ),
        (date(2022, 12, 1), ()),
    ],
    ids=("same_source_validation_failure", "primary_window_policy_mismatch"),
)
def test_runtime_data_quality_failure_stops_before_evaluator(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    consistency_start: date,
    issues: tuple[DataQualityIssue, ...],
) -> None:
    selection = yaml.safe_load(
        (DEFAULT_PACKAGE_ROOT / "selection_rule.yaml").read_text(encoding="utf-8")
    )
    windows = yaml.safe_load(
        (DEFAULT_PACKAGE_ROOT / "window_catalog.yaml").read_text(encoding="utf-8")
    )
    package = {
        "candidate_universe.json": {
            "candidates": [
                {"candidate_id": f"candidate_{index:03d}", "parameters": {"index": index}}
                for index in range(300)
            ]
        },
        "package_manifest.json": {"package_id": "package_test"},
    }
    prices = tmp_path / "prices.csv"
    rates = tmp_path / "rates.csv"
    prices.write_text("date,ticker,close\n", encoding="utf-8")
    rates.write_text("date,series,value\n", encoding="utf-8")
    summary = DataFileSummary(path=prices, exists=True, rows=0)
    failed_report = DataQualityReport(
        checked_at=datetime(2026, 7, 21, tzinfo=UTC),
        as_of=date(2026, 7, 20),
        price_summary=summary,
        rate_summary=DataFileSummary(path=rates, exists=True, rows=0),
        expected_price_tickers=("QQQ",),
        expected_rate_series=("DGS10",),
        price_consistency_start_date=consistency_start,
        rate_consistency_start_date=consistency_start,
        issues=issues,
    )

    monkeypatch.setattr(
        evaluator_module,
        "validate_trading2452_package",
        lambda **_kwargs: {"status": "PASS", "package_id": "package_test"},
    )
    monkeypatch.setattr(evaluator_module, "_load_package", lambda _root: package)
    monkeypatch.setattr(
        evaluator_module,
        "_load_yaml",
        lambda path: selection if path.name == "selection_rule.yaml" else windows,
    )

    def fake_dq(**kwargs: Any) -> DataQualityReport:
        kwargs["output_path"].write_text("# blocked\n", encoding="utf-8")
        return failed_report

    monkeypatch.setattr(evaluator_module, "_run_data_quality_gate", fake_dq)
    monkeypatch.setattr(
        evaluator_module.r1,
        "_load_runtime_context",
        lambda **_kwargs: pytest.fail("evaluator must not start after DQ failure"),
    )

    result = evaluator_module.run_trading2452_historical_seen_evaluator(
        package_root=tmp_path / "package",
        prices_path=prices,
        rates_path=rates,
        output_root=tmp_path / "outputs",
        workers=1,
        generated_at=datetime(2026, 7, 21, tzinfo=UTC),
    )

    assert result["status"] == "BLOCKED_DATA_QUALITY"
    assert result["manifest"]["evaluator_executed"] is False
    assert result["manifest"]["blocked_reason"] == "RUNTIME_DATA_QUALITY_GATE_FAILS"
    assert result["manifest"]["prospective_holdout_accessed"] is False
    assert result["manifest"]["production_effect"] == "none"
    dq_payload = _read_json(result["run_dir"] / "data_quality_gate.json")
    assert dq_payload["passed"] is False
    assert dq_payload["required_consistency_start_date"] == "2021-02-22"
    if not issues:
        assert dq_payload["same_source_validation_passed"] is True
        assert dq_payload["primary_window_alignment_passed"] is False


def test_affinity_batches_are_deterministic_balanced_and_restore_worker_order() -> None:
    plans = [
        (
            index,
            {"candidate_id": f"candidate_{index:03d}"},
            {
                "v0.3a": f"a{index % 2}",
                "v0.3b": "b0",
                "v0.3c": f"c{index % 8}",
                "v0.3d": f"d{index % 3}",
            },
        )
        for index in range(300)
    ]

    one_batch, one_metadata = evaluator_module._balanced_affinity_batches(plans=plans, workers=1)
    eight_batches, eight_metadata = evaluator_module._balanced_affinity_batches(
        plans=plans, workers=8
    )
    repeated_batches, repeated_metadata = evaluator_module._balanced_affinity_batches(
        plans=plans, workers=8
    )
    sixteen_batches, sixteen_metadata = evaluator_module._balanced_affinity_batches(
        plans=plans, workers=16
    )
    twenty_four_batches, twenty_four_metadata = evaluator_module._balanced_affinity_batches(
        plans=plans, workers=24
    )

    assert one_metadata == {"anchor_label": "v0.3c", "batch_sizes": [300]}
    assert eight_metadata == repeated_metadata
    assert eight_batches == repeated_batches
    assert max(eight_metadata["batch_sizes"]) - min(eight_metadata["batch_sizes"]) <= 1
    canonical_one = sorted(item[0] for batch in one_batch for item in batch)
    canonical_eight = sorted(item[0] for batch in eight_batches for item in batch)
    canonical_sixteen = sorted(item[0] for batch in sixteen_batches for item in batch)
    canonical_twenty_four = sorted(item[0] for batch in twenty_four_batches for item in batch)
    assert sixteen_metadata["anchor_label"] == twenty_four_metadata["anchor_label"] == "v0.3c"
    assert (
        canonical_one
        == canonical_eight
        == canonical_sixteen
        == canonical_twenty_four
        == list(range(300))
    )


def test_phase_global_candidate_precompute_plan_has_exact_unique_cardinality() -> None:
    plans = [
        (
            index,
            {"candidate_id": f"candidate_{index:03d}"},
            {
                "v0.3a": f"a{index % 72}",
                "v0.3b": f"b{index % 6}",
                "v0.3c": f"c{index % 144}",
                "v0.3d": f"d{index % 6}",
            },
        )
        for index in range(300)
    ]

    precompute_plan = evaluator_module._global_candidate_precompute_plan(plans)

    assert len(precompute_plan) == 228
    assert {
        label: sum(item[0] == label for item in precompute_plan)
        for label in ("v0.3a", "v0.3b", "v0.3c", "v0.3d")
    } == {"v0.3a": 72, "v0.3b": 6, "v0.3c": 144, "v0.3d": 6}
    assert len({(label, policy_hash) for label, policy_hash, _ in precompute_plan}) == 228


def test_phase_runner_precomputes_each_candidate_policy_hash_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidates = [
        {
            "candidate_id": f"candidate_{index}",
            "hashes": {label: f"{label}-shared" for label in ("a", "b", "c", "d")},
        }
        for index in range(3)
    ]
    candidate_jobs: list[tuple[Any, ...]] = []
    monkeypatch.setattr(
        evaluator_module,
        "_fixed_policy_hashes",
        lambda _runtime: {"base": "base-hash", "v02": "v02-hash", "v04": "v04-hash"},
    )
    monkeypatch.setattr(
        evaluator_module,
        "_precompute_fixed_policy_process_job",
        lambda job: {
            "candidate_id": job[5],
            "dynamic_allocation_policy_hash": job[6],
        },
    )
    monkeypatch.setattr(
        evaluator_module,
        "_candidate_policy_hashes",
        lambda *, runtime, candidate: dict(candidate["hashes"]),
    )

    def fake_candidate_precompute(job: tuple[Any, ...]) -> dict[str, Any]:
        candidate_jobs.append(job)
        return {
            "candidate_id": job[1],
            "dynamic_allocation_policy_hash": job[2],
        }

    monkeypatch.setattr(
        evaluator_module,
        "_precompute_candidate_policy_process_job",
        fake_candidate_precompute,
    )

    def fake_batch(job: tuple[Any, ...]) -> dict[str, Any]:
        batch = job[0]
        records = job[7]
        return {
            "rows": [
                {"input_index": input_index, "row": {"candidate_id": candidate["candidate_id"]}}
                for input_index, candidate, _ in batch
            ],
            "telemetry": {
                "candidate_report_artifact_loads": len(records),
                "candidate_report_memory_hits": 4 * len(batch) - len(records),
            },
        }

    monkeypatch.setattr(evaluator_module, "_evaluate_affinity_batch", fake_batch)
    monkeypatch.setattr(
        evaluator_module,
        "_cleanup_candidate_cache_artifacts",
        lambda *, records, **_kwargs: (
            [dict(record) for record in records],
            {
                "status": "PASS",
                "validated_artifact_count": len(records),
                "deleted_artifact_count": len(records),
                "released_bytes": 0,
                "directory_empty": True,
            },
        ),
    )

    rows, telemetry, fixed_records, candidate_records = evaluator_module._run_phase_jobs(
        candidates=candidates,
        window_index=1,
        phase="train",
        start=date(2021, 2, 22),
        end=date(2022, 12, 29),
        package_id="package",
        workers=16,
        executor=None,
        runtime=object(),
        runtime_binding={"binding_hash": "binding"},
        candidate_cache_root=Path("unused"),
    )

    assert [row["candidate_id"] for row in rows] == [
        "candidate_0",
        "candidate_1",
        "candidate_2",
    ]
    assert len(fixed_records) == 3
    assert len(candidate_jobs) == len(candidate_records) == 4
    assert telemetry["expected_global_unique_candidate_reports"] == 4
    assert telemetry["candidate_report_computations"] == 4
    assert telemetry["candidate_report_cache_hits"] == 8


def test_candidate_report_cache_never_caches_final_candidate_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakePolicy:
        def __init__(self, policy_hash: str) -> None:
            self.policy_hash = policy_hash

    runtime = SimpleNamespace(
        prices=object(),
        etf_config=object(),
        dynamic_robustness_policy=object(),
        data_quality_status="PASS",
        data_quality_report_path="dq.md",
        prices_path=Path("prices.csv"),
    )
    evaluator_module._WORKER_CONTEXT = {
        "runtime": runtime,
        "runtime_binding": {"binding_hash": "binding"},
        "generated": datetime(2026, 7, 21, tzinfo=UTC),
        "config": object(),
        "walk_policy": {},
        "trading_dates": [],
    }
    candidates = [
        {
            "candidate_id": f"candidate_{index}",
            "hashes": {label: f"{label}-shared" for label in ("a", "b", "c", "d")},
        }
        for index in range(3)
    ]
    plans = [
        (index, candidate, dict(candidate["hashes"])) for index, candidate in enumerate(candidates)
    ]
    payload_calls: list[str] = []
    monkeypatch.setattr(evaluator_module, "_load_fixed_reports", lambda **_kwargs: {})
    monkeypatch.setattr(
        evaluator_module,
        "_candidate_materialized_policies",
        lambda *, runtime, candidate: {
            label: FakePolicy(policy_hash) for label, policy_hash in candidate["hashes"].items()
        },
    )
    monkeypatch.setattr(evaluator_module, "_policy_hash", lambda policy: policy.policy_hash)

    monkeypatch.setattr(
        evaluator_module.dynamic_robustness,
        "build_dynamic_robustness_report",
        lambda **_kwargs: pytest.fail("candidate batch must not recompute reports"),
    )
    monkeypatch.setattr(
        evaluator_module,
        "_load_candidate_report",
        lambda **kwargs: {"candidate_id": kwargs["label"]},
    )

    def fake_payload(**kwargs: Any) -> dict[str, Any]:
        candidate_id = str(kwargs["result"]["candidate_id"])
        payload_calls.append(candidate_id)
        return {"candidate_id": candidate_id}

    monkeypatch.setattr(evaluator_module.r1, "_evaluate_candidate_payload", fake_payload)
    monkeypatch.setattr(
        evaluator_module.r1,
        "_summarize_fold_payload",
        lambda **kwargs: {"candidate_id": kwargs["payload"]["candidate_id"]},
    )
    result = evaluator_module._evaluate_affinity_batch(
        (
            plans,
            1,
            "train",
            date(2021, 2, 22),
            date(2022, 12, 29),
            "package",
            [],
            [
                {
                    "candidate_id": label,
                    "dynamic_allocation_policy_hash": policy_hash,
                }
                for label, policy_hash in plans[0][2].items()
            ],
            "binding",
        )
    )

    assert payload_calls == ["candidate_0", "candidate_1", "candidate_2"]
    assert result["telemetry"]["candidate_report_artifact_loads"] == 4
    assert result["telemetry"]["candidate_report_memory_hits"] == 8
    assert [item["row"]["candidate_id"] for item in result["rows"]] == payload_calls


def test_workers_one_eight_and_sixteen_preserve_canonical_rows_and_top20(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakePolicy:
        def __init__(self, policy_hash: str) -> None:
            self.policy_hash = policy_hash

    runtime = SimpleNamespace(
        prices=object(),
        etf_config=object(),
        dynamic_robustness_policy=object(),
        data_quality_status="PASS",
        data_quality_report_path="dq.md",
        prices_path=Path("prices.csv"),
    )
    evaluator_module._WORKER_CONTEXT = {
        "runtime": runtime,
        "runtime_binding": {"binding_hash": "binding"},
        "generated": datetime(2026, 7, 21, tzinfo=UTC),
        "config": object(),
        "walk_policy": {},
        "trading_dates": [],
    }
    candidates = [
        {
            "candidate_id": f"candidate_{index:03d}",
            "parameters": {"index": index},
            "hashes": {
                "a": f"a{index % 2}",
                "b": "b0",
                "c": f"c{index % 8}",
                "d": f"d{index % 3}",
            },
        }
        for index in range(40)
    ]
    plans = [
        (index, candidate, dict(candidate["hashes"])) for index, candidate in enumerate(candidates)
    ]
    monkeypatch.setattr(evaluator_module, "_load_fixed_reports", lambda **_kwargs: {})
    monkeypatch.setattr(
        evaluator_module,
        "_candidate_materialized_policies",
        lambda *, runtime, candidate: {
            label: FakePolicy(policy_hash) for label, policy_hash in candidate["hashes"].items()
        },
    )
    monkeypatch.setattr(evaluator_module, "_policy_hash", lambda policy: policy.policy_hash)
    monkeypatch.setattr(
        evaluator_module.dynamic_robustness,
        "build_dynamic_robustness_report",
        lambda **_kwargs: pytest.fail("candidate batch must not recompute reports"),
    )
    monkeypatch.setattr(
        evaluator_module,
        "_load_candidate_report",
        lambda **kwargs: {"candidate_id": kwargs["label"]},
    )
    monkeypatch.setattr(
        evaluator_module.r1,
        "_evaluate_candidate_payload",
        lambda **kwargs: {"candidate_id": kwargs["result"]["candidate_id"]},
    )
    monkeypatch.setattr(
        evaluator_module.r1,
        "_summarize_fold_payload",
        lambda **kwargs: {
            "candidate_id": kwargs["payload"]["candidate_id"],
            "evidence_status": "COMPLETE",
            "gate": "review_required",
            "selection_score": int(kwargs["payload"]["candidate_id"].split("_")[-1]),
        },
    )

    def evaluate(worker_count: int) -> list[dict[str, Any]]:
        batches, _ = evaluator_module._balanced_affinity_batches(plans=plans, workers=worker_count)
        record_index = {
            (label, policy_hash): {
                "candidate_id": label,
                "dynamic_allocation_policy_hash": policy_hash,
            }
            for _, _, hashes in plans
            for label, policy_hash in hashes.items()
        }
        indexed = []
        for batch in batches:
            result = evaluator_module._evaluate_affinity_batch(
                (
                    batch,
                    1,
                    "train",
                    date(2021, 2, 22),
                    date(2022, 12, 29),
                    "package",
                    [],
                    evaluator_module._candidate_records_for_batch(batch, record_index),
                    "binding",
                )
            )
            indexed.extend(result["rows"])
        indexed.sort(key=lambda item: item["input_index"])
        return [item["row"] for item in indexed]

    rows_one = evaluate(1)
    rows_eight = evaluate(8)
    rows_sixteen = evaluate(16)
    rows_twenty_four = evaluate(24)

    assert rows_one == rows_eight == rows_sixteen == rows_twenty_four
    assert (
        [row["candidate_id"] for row in select_train_only_top_n(rows_one, top_n=20)]
        == [row["candidate_id"] for row in select_train_only_top_n(rows_eight, top_n=20)]
        == [row["candidate_id"] for row in select_train_only_top_n(rows_sixteen, top_n=20)]
        == [row["candidate_id"] for row in select_train_only_top_n(rows_twenty_four, top_n=20)]
    )


def test_fixed_report_cache_tamper_or_binding_mismatch_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FakePolicy:
        pass

    runtime = SimpleNamespace(
        dynamic_robustness_policy=object(),
        data_quality_status="PASS",
    )
    policy = FakePolicy()
    cache_root = tmp_path / "fixed_report_cache"
    cache_root.mkdir()
    evaluator_module._WORKER_CONTEXT = {"fixed_cache_root": cache_root}
    monkeypatch.setattr(evaluator_module, "_fixed_policies", lambda _runtime: {"base": policy})
    monkeypatch.setattr(evaluator_module, "_policy_hash", lambda _policy: "policy-hash")
    monkeypatch.setattr(
        evaluator_module.real_evaluation,
        "_validated_precomputed_robustness_report",
        lambda **kwargs: dict(kwargs["report"]),
    )
    report = {"status": "PASS"}
    report_sha = evaluator_module.sha256(evaluator_module.canonical_json_bytes(report)).hexdigest()
    envelope = {
        "phase": "train",
        "window_index": 1,
        "requested_range": {"start": "2021-02-22", "end": "2022-12-29"},
        "runtime_binding_hash": "binding",
        "candidate_id": "base",
        "dynamic_allocation_policy_hash": "policy-hash",
        "report_sha256": report_sha,
        "report": report,
        "prospective_holdout_accessed": False,
    }
    path = cache_root / "base.json"
    evaluator_module.write_json_atomic(path, envelope)
    record = {
        "relative_path": path.name,
        "file_sha256": evaluator_module._file_sha256(path),
        "report_sha256": report_sha,
        "phase": "train",
        "window_index": 1,
        "requested_range": envelope["requested_range"],
        "runtime_binding_hash": "binding",
        "candidate_id": "base",
        "dynamic_allocation_policy_hash": "policy-hash",
    }

    assert evaluator_module._load_fixed_reports(
        records=[record],
        runtime=runtime,
        phase="train",
        window_index=1,
        start=date(2021, 2, 22),
        end=date(2022, 12, 29),
        binding_hash="binding",
    ) == {"base": report}

    bad_binding = dict(record, runtime_binding_hash="other")
    with pytest.raises(DynamicV3Trading2452EvaluatorError, match="binding mismatch"):
        evaluator_module._load_fixed_reports(
            records=[bad_binding],
            runtime=runtime,
            phase="train",
            window_index=1,
            start=date(2021, 2, 22),
            end=date(2022, 12, 29),
            binding_hash="binding",
        )

    path.write_text("{}\n", encoding="utf-8")
    with pytest.raises(DynamicV3Trading2452EvaluatorError, match="checksum mismatch"):
        evaluator_module._load_fixed_reports(
            records=[record],
            runtime=runtime,
            phase="train",
            window_index=1,
            start=date(2021, 2, 22),
            end=date(2022, 12, 29),
            binding_hash="binding",
        )


def test_candidate_report_artifact_tamper_or_binding_mismatch_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FakePolicy:
        policy_hash = "policy-hash"

    runtime = SimpleNamespace(
        dynamic_robustness_policy=object(),
        data_quality_status="PASS",
    )
    cache_root = tmp_path / "candidate_report_cache"
    cache_root.mkdir()
    evaluator_module._WORKER_CONTEXT = {"candidate_cache_root": cache_root}
    monkeypatch.setattr(evaluator_module, "_policy_hash", lambda policy: policy.policy_hash)
    monkeypatch.setattr(
        evaluator_module.real_evaluation,
        "_validated_precomputed_robustness_report",
        lambda **kwargs: dict(kwargs["report"]),
    )
    report = {"status": "PASS"}
    report_sha = evaluator_module.sha256(evaluator_module.canonical_json_bytes(report)).hexdigest()
    envelope = {
        "cache_kind": "candidate_robustness_report",
        "phase": "train",
        "window_index": 1,
        "requested_range": {"start": "2021-02-22", "end": "2022-12-29"},
        "runtime_binding_hash": "binding",
        "candidate_id": "v0.3a",
        "dynamic_allocation_policy_hash": "policy-hash",
        "report_sha256": report_sha,
        "report": report,
        "prospective_holdout_accessed": False,
    }
    path = cache_root / evaluator_module._candidate_cache_filename(
        label="v0.3a",
        policy_hash="policy-hash",
        phase="train",
        window_index=1,
        start="2021-02-22",
        end="2022-12-29",
        binding_hash="binding",
    )
    evaluator_module.write_json_atomic(path, envelope)
    record = {
        "relative_path": path.name,
        "file_sha256": evaluator_module._file_sha256(path),
        "report_sha256": report_sha,
        "phase": "train",
        "window_index": 1,
        "requested_range": envelope["requested_range"],
        "runtime_binding_hash": "binding",
        "candidate_id": "v0.3a",
        "dynamic_allocation_policy_hash": "policy-hash",
    }

    assert (
        evaluator_module._load_candidate_report(
            record=record,
            runtime=runtime,
            policy=FakePolicy(),
            label="v0.3a",
            phase="train",
            window_index=1,
            start=date(2021, 2, 22),
            end=date(2022, 12, 29),
            binding_hash="binding",
        )
        == report
    )
    assert (
        evaluator_module._candidate_cache_files_valid(
            cache_root=cache_root,
            records=[record],
            runtime_binding_hash="binding",
        )
        is True
    )

    extra_path = cache_root / "untracked.json"
    extra_path.write_text("{}\n", encoding="utf-8")
    assert (
        evaluator_module._candidate_cache_files_valid(
            cache_root=cache_root,
            records=[record],
            runtime_binding_hash="binding",
        )
        is False
    )
    extra_path.unlink()

    with pytest.raises(DynamicV3Trading2452EvaluatorError, match="binding mismatch"):
        evaluator_module._load_candidate_report(
            record=dict(record, runtime_binding_hash="other"),
            runtime=runtime,
            policy=FakePolicy(),
            label="v0.3a",
            phase="train",
            window_index=1,
            start=date(2021, 2, 22),
            end=date(2022, 12, 29),
            binding_hash="binding",
        )

    path.write_text("{}\n", encoding="utf-8")
    with pytest.raises(DynamicV3Trading2452EvaluatorError, match="checksum mismatch"):
        evaluator_module._load_candidate_report(
            record=record,
            runtime=runtime,
            policy=FakePolicy(),
            label="v0.3a",
            phase="train",
            window_index=1,
            start=date(2021, 2, 22),
            end=date(2022, 12, 29),
            binding_hash="binding",
        )
    with pytest.raises(DynamicV3Trading2452EvaluatorError, match="cleanup blocked"):
        evaluator_module._cleanup_candidate_cache_artifacts(
            cache_root=cache_root,
            records=[record],
            runtime_binding_hash="binding",
        )
    assert path.exists()


def test_candidate_report_global_precompute_writes_one_bound_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FakePolicy:
        policy_hash = "d" * 64

    runtime = SimpleNamespace(
        prices=object(),
        etf_config=object(),
        dynamic_robustness_policy=object(),
        data_quality_status="PASS",
        data_quality_report_path="dq.md",
        prices_path=Path("prices.csv"),
    )
    cache_root = tmp_path / "candidate_report_cache"
    cache_root.mkdir()
    evaluator_module._WORKER_CONTEXT = {
        "runtime": runtime,
        "runtime_binding": {"binding_hash": "binding"},
        "candidate_cache_root": cache_root,
    }
    monkeypatch.setattr(
        evaluator_module,
        "_candidate_materialized_policies",
        lambda **_kwargs: {"v0.3a": FakePolicy()},
    )
    monkeypatch.setattr(evaluator_module, "_policy_hash", lambda policy: policy.policy_hash)
    build_calls: list[str] = []

    def fake_build(**kwargs: Any) -> dict[str, Any]:
        build_calls.append(str(kwargs["candidate_id"]))
        return {"status": "PASS", "candidate_id": kwargs["candidate_id"]}

    monkeypatch.setattr(
        evaluator_module.dynamic_robustness,
        "build_dynamic_robustness_report",
        fake_build,
    )
    monkeypatch.setattr(
        evaluator_module.real_evaluation,
        "_validated_precomputed_robustness_report",
        lambda **kwargs: dict(kwargs["report"]),
    )
    job = (
        {"candidate_id": "candidate_000"},
        "v0.3a",
        "d" * 64,
        "train",
        1,
        date(2021, 2, 22),
        date(2022, 12, 29),
        "binding",
    )

    record = evaluator_module._precompute_candidate_policy_process_job(job)

    assert build_calls == ["v0.3a"]
    assert record["dynamic_allocation_policy_hash"] == "d" * 64
    assert (cache_root / record["relative_path"]).is_file()
    with pytest.raises(DynamicV3Trading2452EvaluatorError, match="already exists"):
        evaluator_module._precompute_candidate_policy_process_job(job)

    commitments, cleanup = evaluator_module._cleanup_candidate_cache_artifacts(
        cache_root=cache_root,
        records=[record],
        runtime_binding_hash="binding",
    )

    assert cleanup["status"] == "PASS"
    assert cleanup["deleted_artifact_count"] == 1
    assert cleanup["directory_empty"] is True
    assert not any(cache_root.iterdir())
    assert commitments[0]["cleanup_status"] == "DELETED_AFTER_PHASE_CONSUMPTION"
    assert (
        evaluator_module._candidate_cache_commitments_valid(
            run_dir=tmp_path,
            records=commitments,
            runtime_binding_hash="binding",
        )
        is True
    )
    unexpected = cache_root / "unexpected.json"
    unexpected.write_text("{}\n", encoding="utf-8")
    assert (
        evaluator_module._candidate_cache_commitments_valid(
            run_dir=tmp_path,
            records=commitments,
            runtime_binding_hash="binding",
        )
        is False
    )
    unexpected.unlink()


def test_phase_runner_rejects_prospective_before_any_precompute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        evaluator_module,
        "_fixed_policy_hashes",
        lambda _runtime: pytest.fail("prospective phase must stop before fixed precompute"),
    )
    with pytest.raises(DynamicV3Trading2452EvaluatorError, match="prospective"):
        evaluator_module._run_phase_jobs(
            candidates=[],
            window_index=0,
            phase="recent_known_diagnostic",
            start=date(2026, 7, 21),
            end=date(2026, 7, 22),
            package_id="package",
            workers=1,
            executor=None,
            runtime=object(),
            runtime_binding={"binding_hash": "binding"},
            candidate_cache_root=Path("unused"),
        )


def test_empty_phase_skips_all_report_precompute(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_root = tmp_path / "candidate_cache"
    cache_root.mkdir()
    monkeypatch.setattr(
        evaluator_module,
        "_fixed_policy_hashes",
        lambda _runtime: pytest.fail("empty phase must not build fixed reports"),
    )

    rows, telemetry, fixed_records, candidate_commitments = evaluator_module._run_phase_jobs(
        candidates=[],
        window_index=1,
        phase="test",
        start=date(2023, 1, 4),
        end=date(2023, 6, 30),
        package_id="package",
        workers=24,
        executor=None,
        runtime=object(),
        runtime_binding={"binding_hash": "binding"},
        candidate_cache_root=cache_root,
    )

    assert rows == fixed_records == candidate_commitments == []
    assert telemetry["candidate_count"] == 0
    assert telemetry["fixed_report_computations"] == 0
    assert telemetry["candidate_cache_cleanup"]["status"] == "PASS"
    assert telemetry["candidate_cache_cleanup"]["directory_empty"] is True


def test_jsonl_writer_emits_exactly_one_valid_object_per_line(tmp_path: Path) -> None:
    path = tmp_path / "rows.jsonl"
    rows = [
        {"candidate_id": "candidate_001", "nested": {"value": 1}},
        {"candidate_id": "候选_002", "values": [1, 2, 3]},
    ]

    evaluator_module._write_jsonl(path, rows)

    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == len(rows)
    assert [json.loads(line) for line in lines] == rows
    assert evaluator_module._read_jsonl(path) == rows


def test_runtime_binding_source_tamper_fails_closed() -> None:
    payload = {
        "prices": {"path": "prices.csv", "sha256": "a" * 64},
        "source_hashes": {"evaluator": "b" * 64},
    }
    binding = {**payload, "binding_hash": evaluator_module._stable_hash(payload)}

    assert evaluator_module._runtime_binding_payload_valid(binding) is True

    tampered = json.loads(json.dumps(binding))
    tampered["source_hashes"]["evaluator"] = "c" * 64
    assert evaluator_module._runtime_binding_payload_valid(tampered) is False


@pytest.mark.parametrize(
    ("status", "validation_status", "expected_exit_code"),
    [
        ("REVIEW_REQUIRED_HISTORICAL_SEEN_ONLY", "PASS", 0),
        ("INCOMPLETE_EVIDENCE", "PASS", 0),
        ("REVIEW_REQUIRED_HISTORICAL_SEEN_ONLY", "FAIL", 1),
        ("BLOCKED_DATA_QUALITY", None, 1),
    ],
)
def test_main_exit_code_uses_artifact_validation_not_investment_status(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    status: str,
    validation_status: str | None,
    expected_exit_code: int,
) -> None:
    result: dict[str, Any] = {
        "status": status,
        "run_id": "run",
        "run_dir": Path("run"),
        "production_effect": "none",
        "broker_action": "none",
    }
    if validation_status is not None:
        result["validation"] = {"status": validation_status}
    monkeypatch.setattr(
        evaluator_module,
        "run_trading2452_historical_seen_evaluator",
        lambda: result,
    )

    exit_code = evaluator_module.main()
    summary = json.loads(capsys.readouterr().out)

    assert exit_code == expected_exit_code
    assert summary["status"] == status
    assert summary["artifact_validation_status"] == validation_status


def _copy_package(root: Path) -> Path:
    package = root / "package"
    shutil.copytree(DEFAULT_PACKAGE_ROOT, package)
    return package


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
