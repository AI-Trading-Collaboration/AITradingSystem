from __future__ import annotations

import copy
import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
import yaml

from ai_trading_system.research_framework.plugins import (
    o1_relative_opportunity_coverage as coverage_module,
)
from ai_trading_system.research_framework.plugins.o1_relative_opportunity_coverage import (
    BLOCKED_CLASS,
    BLOCKED_STATUS,
    O1CoverageError,
    _BoundInputs,
    _effective_sample,
    _evaluate_coverage,
    _event_coverage,
    run_o1_coverage_only,
    validate_o1_coverage_gate,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
POLICY_PATH = (
    PROJECT_ROOT / "config/research/o1_relative_opportunity_capability_audit_v1.yaml"
)
HISTORICAL_POLICY_PATH = (
    PROJECT_ROOT / "config/research/decision_target_capability_audit_model_ladder_v1.yaml"
)


def test_coverage_only_runner_writes_mechanical_gate_without_model_outputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root, policy_path, bound = _runner_fixture(tmp_path)
    monkeypatch.setattr(
        coverage_module,
        "_software_identity",
        lambda **kwargs: {
            "python_version": "3.test",
            "package_lock_path": "pyproject.toml",
            "package_lock_kind": "UNLOCKED_PROJECT_MANIFEST",
            "package_lock_sha256": "1" * 64,
            "source_commit_sha": "a" * 40,
            "contract_freeze_source_base_sha": "b" * 40,
            "cli_argv": ["coverage-test"],
        },
    )
    monkeypatch.setattr(coverage_module, "_load_bound_inputs", lambda **kwargs: bound)
    output_root = tmp_path / "retained" / "o1_coverage_only_v1"

    result = run_o1_coverage_only(
        output_root=output_root,
        project_root=project_root,
        generated_at=datetime(2026, 7, 30, 1, 2, tzinfo=UTC),
        audit_policy_path=policy_path.relative_to(project_root),
        source_commit_sha="a" * 40,
        cli_argv=("coverage-test",),
    )

    assert result.report["status"] == BLOCKED_STATUS
    assert result.report["mechanical_classification"] == BLOCKED_CLASS
    assert result.report["coverage"]["completed_outer_fold_count"] >= 5
    assert result.report["coverage"]["all_mandatory_coverage_checks_passed"] is False
    assert any(
        row["check_id"] == "F01_TRAIN_EFFECTIVE_SAMPLE" and not row["passed"]
        for row in result.report["coverage"]["checks"]
    )
    assert result.report["attempt_execution"] == {
        "attempt_family_id": "O1_M1_RIDGE_CROSS_ASSET_STATE_V1",
        "coverage_read": True,
        "model_trained": False,
        "prediction_generated": False,
        "metric_generated": False,
    }
    assert "predictions" not in result.report
    assert "metrics" not in result.report
    assert result.gate["next_authorization"]["model_training_allowed_now"] is False
    assert result.gate["next_authorization"]["canonical_run_allowed_now"] is False
    validate_o1_coverage_gate(result.gate)

    with pytest.raises(O1CoverageError, match="O1_COVERAGE_OUTPUT_ALREADY_EXISTS"):
        run_o1_coverage_only(
            output_root=output_root,
            project_root=project_root,
            generated_at=datetime(2026, 7, 30, 1, 3, tzinfo=UTC),
            audit_policy_path=policy_path.relative_to(project_root),
        )


def test_gate_digest_and_scope_tamper_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root, policy_path, bound = _runner_fixture(tmp_path)
    monkeypatch.setattr(
        coverage_module,
        "_software_identity",
        lambda **kwargs: {
            "python_version": "3.test",
            "package_lock_path": "pyproject.toml",
            "package_lock_kind": "UNLOCKED_PROJECT_MANIFEST",
            "package_lock_sha256": "1" * 64,
            "source_commit_sha": "a" * 40,
            "contract_freeze_source_base_sha": "b" * 40,
            "cli_argv": [],
        },
    )
    monkeypatch.setattr(coverage_module, "_load_bound_inputs", lambda **kwargs: bound)
    result = run_o1_coverage_only(
        output_root=tmp_path / "retained/o1_coverage_only_v1",
        project_root=project_root,
        generated_at=datetime(2026, 7, 30, 1, 2, tzinfo=UTC),
        audit_policy_path=policy_path.relative_to(project_root),
    )
    tampered = copy.deepcopy(result.gate)
    tampered["next_authorization"]["model_training_allowed_now"] = True

    with pytest.raises(O1CoverageError, match="O1_COVERAGE_GATE_ID_MISMATCH"):
        validate_o1_coverage_gate(tampered)


def test_effective_sample_uses_frozen_non_overlap_and_positive_autocorrelation() -> None:
    constant = _effective_sample([1.0] * 126, horizon=5)
    assert constant == {
        "raw_sample": 126,
        "non_overlap_equivalent": 25,
        "autocorrelation_ess": 126.0,
        "effective_sample": 25.0,
        "positive_autocorrelation_sum_lag_1_to_5": 0.0,
    }
    alternating = _effective_sample([float(index % 2) for index in range(126)], horizon=5)
    assert alternating["non_overlap_equivalent"] == 25
    assert alternating["effective_sample"] == 25.0


def test_non_session_event_is_missing_and_never_shifted_to_a_market_date() -> None:
    policy = yaml.safe_load(POLICY_PATH.read_text(encoding="utf-8"))
    sessions = ["2026-04-02", "2026-04-06", "2026-04-07"]
    rows = [
        {"decision_date": value}
        for value in sessions
    ]
    folds = [
        {
            "fold_id": "F01",
            "test_decision_dates": sessions,
        }
    ]
    event_rows = [
        {
            "event_id": f"{family}-holiday",
            "event_family": family,
            "event_timestamp": "2026-04-03T12:30:00+00:00",
        }
        for family in ("FOMC", "CPI", "NFP")
    ]

    result = _event_coverage(
        policy=policy,
        folds=folds,
        rows=rows,
        event_rows=event_rows,
        common_sessions=sessions,
    )

    assert "not shifted" in result["anchor_rule"]
    assert all(
        row["missing_common_session_episode_count"] == 1
        and row["eligible_oof_episode_count"] == 0
        for row in result["families"]
    )


def test_bound_file_hash_tamper_is_rejected(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    artifact = project_root / "artifact.json"
    artifact.write_text("{}\n", encoding="utf-8")
    binding = {
        "path": "artifact.json",
        "sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
        "byte_size": artifact.stat().st_size,
    }
    artifact.write_text('{"tampered": true}\n', encoding="utf-8")

    with pytest.raises(O1CoverageError, match="O1_COVERAGE_BOUND_FILE_TAMPERED"):
        coverage_module._verify_bound_file(project_root, binding)


def test_synthetic_coverage_evaluation_is_deterministic() -> None:
    policy = yaml.safe_load(POLICY_PATH.read_text(encoding="utf-8"))
    historical = safe_load_yaml_path(HISTORICAL_POLICY_PATH)
    panel = _synthetic_panel()
    events = _synthetic_events(panel)

    first = _evaluate_coverage(
        policy=policy,
        historical_policy=historical,
        panel=panel,
        event_rows=events,
    )
    second = _evaluate_coverage(
        policy=copy.deepcopy(policy),
        historical_policy=copy.deepcopy(historical),
        panel=panel.copy(),
        event_rows=copy.deepcopy(events),
    )

    assert first == second
    assert first["status"] == BLOCKED_STATUS
    assert first["mechanical_classification"] == BLOCKED_CLASS


def _runner_fixture(
    tmp_path: Path,
) -> tuple[Path, Path, _BoundInputs]:
    project_root = tmp_path / "project"
    project_root.mkdir()
    (project_root / ".git").mkdir()
    (project_root / "pyproject.toml").write_text(
        '[project]\nname = "coverage-fixture"\nversion = "0.0.0"\n',
        encoding="utf-8",
    )
    retained_root = tmp_path / "retained"
    retained_root.mkdir()
    policy = yaml.safe_load(POLICY_PATH.read_text(encoding="utf-8"))
    policy["status"] = "OWNER_APPROVED_EVENT_LEDGER_FROZEN_COVERAGE_ONLY_READY"
    policy["execution_binding"]["real_coverage_read_allowed_now"] = True
    policy["execution_binding"].pop("coverage_attempt_consumed", None)
    policy["execution_binding"].pop("canonical_run_allowed_now", None)
    policy["safety"]["new_o1_result_read"] = False
    policy["safety"]["coverage_audit_executed"] = False
    policy.pop("coverage_evidence", None)
    policy["isolated_dq_evidence"]["output_root"] = retained_root.as_posix()
    policy_path = project_root / "config/research/o1_policy.yaml"
    policy_path.parent.mkdir(parents=True)
    policy_path.write_text(
        yaml.safe_dump(policy, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    panel = _synthetic_panel()
    events = tuple(_synthetic_events(panel))
    historical = safe_load_yaml_path(HISTORICAL_POLICY_PATH)
    return (
        project_root,
        policy_path,
        _BoundInputs(
            policy=policy,
            policy_path=policy_path,
            policy_sha256=hashlib.sha256(policy_path.read_bytes()).hexdigest(),
            historical_policy=historical,
            historical_policy_path=HISTORICAL_POLICY_PATH,
            panel=panel,
            event_rows=events,
            evidence={
                "dq_gate": {
                    "gate_id": "dq",
                    "path": "dq.json",
                    "sha256": "2" * 64,
                },
                "event_source_manifest": {
                    "manifest_id": "source",
                    "path": "source.json",
                    "sha256": "3" * 64,
                },
                "event_ledger": {
                    "ledger_id": "events",
                    "path": "events.json",
                    "sha256": "4" * 64,
                },
                "attempt_ledger": {
                    "ledger_id": "attempt",
                    "path": "attempt.json",
                    "sha256": "5" * 64,
                },
                "event_gate": {
                    "gate_id": "event-gate",
                    "path": "event-gate.json",
                    "sha256": "6" * 64,
                },
            },
            input_commitment={
                role: {
                    "path": f"{role}.csv",
                    "sha256": str(index) * 64,
                    "size_bytes": index,
                    "verified": True,
                }
                for index, role in enumerate(
                    ("prices", "rates", "secondary_prices"),
                    start=7,
                )
            },
            dq_summary={
                "status": "PASS",
                "error_count": 0,
                "warning_count": 0,
                "requested_start": "2021-02-22",
                "requested_end": "2026-07-27",
                "evaluated_start": "2021-02-22",
                "evaluated_end": "2026-07-24",
                "receipt_id": "dq_execution_fixture",
                "receipt_sha256": "a" * 64,
            },
        ),
    )


def _synthetic_panel() -> pd.DataFrame:
    sessions = pd.bdate_range("2021-02-22", "2026-07-24")
    index = np.arange(len(sessions), dtype=float)
    prices = {
        "QQQ": 100.0 * np.exp(0.0004 * index + 0.02 * np.sin(index / 17.0)),
        "SGOV": 100.0 * np.exp(0.0001 * index + 0.0005 * np.sin(index / 31.0)),
        "SPY": 100.0 * np.exp(0.0003 * index + 0.015 * np.sin(index / 23.0)),
    }
    rows: list[dict[str, Any]] = []
    for position, session in enumerate(sessions):
        for ticker in ("QQQ", "SGOV", "SPY"):
            rows.append(
                {
                    "date": session.date().isoformat(),
                    "ticker": ticker,
                    "adj_close": float(prices[ticker][position]),
                }
            )
    return pd.DataFrame(rows, columns=["date", "ticker", "adj_close"])


def _synthetic_events(panel: pd.DataFrame) -> list[dict[str, object]]:
    sessions = sorted(str(value) for value in panel["date"].unique())
    rows: list[dict[str, object]] = []
    for sequence, position in enumerate((700, 830, 960, 1090, 1220), start=1):
        for family in ("FOMC", "CPI", "NFP"):
            rows.append(
                {
                    "event_id": f"{family}-{sequence:02d}",
                    "event_family": family,
                    "event_timestamp": f"{sessions[position]}T13:30:00+00:00",
                }
            )
    return rows
