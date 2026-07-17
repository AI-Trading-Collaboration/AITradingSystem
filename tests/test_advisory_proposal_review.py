from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_backtest_sim_helpers import run_advisory_proposal_review_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_backtest_simulation as sim
from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    PROPOSAL_REVIEW_DECISIONS,
    DynamicV3BacktestSimulationError,
    validate_advisory_proposal_review_artifact,
)
from ai_trading_system.platform.artifacts.validation_session import (
    artifact_validation_session,
)


@pytest.fixture(scope="module")
def immutable_advisory_proposal_review(
    tmp_path_factory: pytest.TempPathFactory,
) -> Any:
    """Build the direct Confirmation upstream once per worker/module."""
    root = tmp_path_factory.mktemp("advisory-proposal-review-upstream")
    monkeypatch = pytest.MonkeyPatch()
    try:
        with artifact_validation_session():
            fixture = run_advisory_proposal_review_fixture(root, monkeypatch)
            yield fixture
    finally:
        monkeypatch.undo()


@pytest.fixture
def advisory_proposal_review_fixture(
    immutable_advisory_proposal_review: dict[str, Any],
) -> dict[str, Any]:
    return immutable_advisory_proposal_review


@pytest.fixture
def mutable_advisory_review_fixture(
    immutable_advisory_proposal_review: dict[str, Any], artifact_name: str
) -> Any:
    path = (
        immutable_advisory_proposal_review["proposal_review"]["proposal_review_dir"]
        / artifact_name
    )
    original = path.read_bytes()
    try:
        yield immutable_advisory_proposal_review
    finally:
        path.write_bytes(original)


@pytest.fixture
def mutable_risk_return_fixture(
    immutable_advisory_proposal_review: dict[str, Any],
) -> Any:
    path = (
        immutable_advisory_proposal_review["risk_return"]["risk_return_dir"]
        / "risk_return_report.md"
    )
    original = path.read_bytes()
    try:
        yield immutable_advisory_proposal_review
    finally:
        path.write_bytes(original)


@pytest.fixture
def mutable_calibration_fixture(
    immutable_advisory_proposal_review: dict[str, Any],
) -> Any:
    path = (
        immutable_advisory_proposal_review["calibration"]["calibration_pack_dir"]
        / "reader_brief_section.md"
    )
    original = path.read_bytes()
    try:
        yield immutable_advisory_proposal_review
    finally:
        path.write_bytes(original)


def test_advisory_proposal_review_requires_owner_and_no_auto_apply(
    advisory_proposal_review_fixture: dict[str, Any],
) -> None:
    fixture = advisory_proposal_review_fixture
    review = fixture["proposal_review"]
    manifest = review["manifest"]
    matrix = review["proposal_decision_matrix"]

    assert matrix["auto_apply"] is False
    assert matrix["owner_approval_required"] is True
    assert matrix["position_advisory_config_mutated"] is False
    assert all(row["decision"] in PROPOSAL_REVIEW_DECISIONS for row in matrix["proposals"])
    assert all(row["auto_apply"] is False for row in matrix["proposals"])
    assert all("0.55" not in " ".join(row["conditions"]) for row in matrix["proposals"])
    assert manifest["owner_approval_required"] is True
    assert manifest["broker_action_allowed"] is False
    assert (review["proposal_review_dir"] / "owner_approval_checklist.md").exists()
    assert (
        review["proposal_review_dir"] / "advisory_proposal_review_input_snapshot.json"
    ).exists()
    assert "no production" in review["reader_brief_section"].lower()

    validation = validate_advisory_proposal_review_artifact(
        proposal_review_id=review["proposal_review_id"],
        output_dir=fixture["proposal_review_dir"],
    )
    assert validation["status"] == "PASS"


def test_advisory_proposal_review_rejects_naive_cutoff_before_output(
    tmp_path: Path, advisory_proposal_review_fixture: dict[str, Any]
) -> None:
    fixture = advisory_proposal_review_fixture
    with pytest.raises(DynamicV3BacktestSimulationError, match="timezone-aware"):
        _run_review(fixture, tmp_path / "new", datetime(2026, 7, 31, 13))
    assert not (tmp_path / "new").exists()


def test_advisory_proposal_review_rejects_invalid_source_before_output(
    tmp_path: Path, mutable_risk_return_fixture: dict[str, Any]
) -> None:
    fixture = mutable_risk_return_fixture
    source = fixture["risk_return"]["risk_return_dir"] / "risk_return_report.md"
    source.write_text(source.read_text(encoding="utf-8") + "tamper\n", encoding="utf-8")
    with pytest.raises(DynamicV3BacktestSimulationError, match="source validation"):
        _run_review(
            fixture, tmp_path / "new", datetime(2026, 7, 31, 13, tzinfo=UTC)
        )
    assert not (tmp_path / "new").exists()


def test_advisory_proposal_review_empty_source_proposals_are_not_fabricated() -> None:
    policy = sim._load_advisory_proposal_review_policy(
        sim.DEFAULT_ADVISORY_PROPOSAL_REVIEW_POLICY_PATH
    )
    matrix = sim._proposal_decision_matrix(
        proposals={"proposals": []},
        risk_summary={},
        defensive_summary={},
        key_findings={},
        policy=policy,
    )
    assert matrix["proposals"] == []
    assert matrix["evidence_status"] == "INSUFFICIENT_DATA"


def test_advisory_proposal_review_rejects_invalid_policy_before_output(
    tmp_path: Path, advisory_proposal_review_fixture: dict[str, Any]
) -> None:
    fixture = advisory_proposal_review_fixture
    policy_path = tmp_path / "invalid.yaml"
    policy_path.write_text("schema_version: invalid\n", encoding="utf-8")
    with pytest.raises(DynamicV3BacktestSimulationError, match="policy schema"):
        _run_review(
            fixture,
            tmp_path / "new",
            datetime(2026, 7, 31, 13, tzinfo=UTC),
            policy_path=policy_path,
        )
    assert not (tmp_path / "new").exists()


@pytest.mark.parametrize(
    "artifact_name",
    [
        "proposal_review_manifest.json",
        "proposal_decision_matrix.json",
        "advisory_proposal_review_input_snapshot.json",
        "owner_approval_checklist.md",
        "advisory_proposal_review_report.md",
        "reader_brief_section.md",
    ],
)
def test_advisory_proposal_review_validator_rejects_output_tamper(
    mutable_advisory_review_fixture: dict[str, Any], artifact_name: str
) -> None:
    fixture = mutable_advisory_review_fixture
    review = fixture["proposal_review"]
    path = review["proposal_review_dir"] / artifact_name
    if path.suffix == ".md":
        path.write_text(path.read_text(encoding="utf-8") + "tamper\n", encoding="utf-8")
    else:
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["tampered"] = True
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    validation = validate_advisory_proposal_review_artifact(
        proposal_review_id=review["proposal_review_id"],
        output_dir=fixture["proposal_review_dir"],
    )
    assert validation["status"] == "FAIL"


def test_advisory_proposal_review_validator_rejects_live_source_drift(
    mutable_calibration_fixture: dict[str, Any],
) -> None:
    fixture = mutable_calibration_fixture
    review = fixture["proposal_review"]
    source = fixture["calibration"]["calibration_pack_dir"] / "reader_brief_section.md"
    source.write_text(source.read_text(encoding="utf-8") + "tamper\n", encoding="utf-8")
    validation = validate_advisory_proposal_review_artifact(
        proposal_review_id=review["proposal_review_id"],
        output_dir=fixture["proposal_review_dir"],
    )
    assert validation["status"] == "FAIL"


def test_advisory_proposal_review_validator_rejects_live_policy_drift(
    tmp_path: Path, advisory_proposal_review_fixture: dict[str, Any]
) -> None:
    fixture = advisory_proposal_review_fixture
    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        sim.DEFAULT_ADVISORY_PROPOSAL_REVIEW_POLICY_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    review = _run_review(
        fixture,
        tmp_path / "review",
        datetime(2026, 7, 31, 13, tzinfo=UTC),
        policy_path=policy_path,
    )
    policy_path.write_text(
        policy_path.read_text(encoding="utf-8") + "# drift\n", encoding="utf-8"
    )
    validation = validate_advisory_proposal_review_artifact(
        proposal_review_id=review["proposal_review_id"],
        output_dir=tmp_path / "review",
    )
    assert validation["status"] == "FAIL"


def _run_review(
    fixture: dict[str, Any],
    output_dir: Path,
    generated_at: datetime,
    *,
    policy_path: Path = sim.DEFAULT_ADVISORY_PROPOSAL_REVIEW_POLICY_PATH,
) -> dict[str, Any]:
    return sim.run_advisory_proposal_review(
        interpretation_id=fixture["interpretation"]["interpretation_id"],
        risk_return_id=fixture["risk_return"]["risk_return_id"],
        defensive_validation_id=fixture["defensive_validation"]["defensive_validation_id"],
        calibration_id=fixture["calibration"]["calibration_pack_id"],
        interpretation_dir=fixture["interpretation_dir"],
        risk_return_dir=fixture["risk_return_dir"],
        defensive_validation_dir=fixture["defensive_validation_dir"],
        calibration_dir=fixture["calibration_dir"],
        output_dir=output_dir,
        policy_path=policy_path,
        generated_at=generated_at,
    )
