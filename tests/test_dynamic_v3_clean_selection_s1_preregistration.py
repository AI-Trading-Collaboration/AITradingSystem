from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import pytest
import yaml

from ai_trading_system.contracts.research_context import ResearchEvaluationContext
from ai_trading_system.contracts.research_lifecycle import ResearchPreregistration
from ai_trading_system.dynamic_v3_clean_selection_s1_preregistration import (
    DEFAULT_PACKAGE_ROOT,
    SAFETY,
    build_dynamic_v3_clean_selection_s1_package,
    validate_dynamic_v3_clean_selection_s1_package,
)
from ai_trading_system.research_campaign import CampaignSpec


def test_frozen_s1_package_is_historical_and_fails_closed_after_window_migration() -> None:
    validation = validate_dynamic_v3_clean_selection_s1_package()

    assert validation["status"] == "FAIL"
    assert validation["failed_check_count"] >= 1
    assert validation["eligibility_status"] == "BLOCKED_INVALID_PREREGISTRATION_PACKAGE"
    assert validation["evaluator_execution_allowed"] is False
    assert validation["clean_run_authorized"] is False
    assert validation["locked_holdout_access_allowed"] is False
    assert validation["unbiased_oos_claim_allowed"] is False
    assert validation["production_effect"] == "none"
    assert validation["broker_action"] == "none"


def test_candidate_universe_is_exact_deterministic_300_without_result_inputs() -> None:
    payloads = build_dynamic_v3_clean_selection_s1_package()
    universe = payloads["candidate_universe.json"]
    manifest = payloads["package_manifest.json"]
    candidates = universe["candidates"]

    assert universe["candidate_count"] == 300
    assert len(candidates) == 300
    assert len({item["candidate_id"] for item in candidates}) == 300
    assert universe["candidate_universe_origin"] == "preregistered_candidate_universe"
    assert universe["result_artifacts_consumed"] == []
    assert manifest["result_artifacts_consumed"] == []
    assert manifest["result_artifact_count"] == 0
    selection_paths = [
        item["path"]
        for key, item in manifest["selection_input_commitments"].items()
        if not key.startswith("policy:")
    ]
    forbidden = (
        "leaderboard",
        "candidate_results",
        "candidate_report",
        "real_evaluation",
        "fold_evaluations",
    )
    assert not any(token in path.lower() for path in selection_paths for token in forbidden)


def test_canonical_contracts_cross_link_and_keep_runtime_dq_pending() -> None:
    payloads = build_dynamic_v3_clean_selection_s1_package()
    context = ResearchEvaluationContext.from_dict(payloads["research_context.json"])
    preregistration = ResearchPreregistration.from_dict(payloads["preregistration.json"])
    campaign = CampaignSpec.model_validate(payloads["campaign.json"])

    assert context.status.value == "BLOCKED"
    assert context.blocking_issues == ("RUNTIME_DATA_QUALITY_GATE_REQUIRED_BEFORE_EVALUATOR",)
    assert preregistration.result_visibility.value == "NONE"
    assert preregistration.research_context_id == context.context_id
    assert campaign.metadata["clean_selection_preregistration_id"] == (
        preregistration.preregistration_id
    )
    assert campaign.owner_authorized_holdout is False
    assert campaign.metadata["clean_run_authorized"] is False
    assert campaign.metadata["evaluator_executed"] is False
    assert campaign.safety.production_effect == "none"


def test_candidate_content_or_order_tamper_fails_closed(tmp_path: Path) -> None:
    package = _copy_package(tmp_path)
    path = package / "candidate_universe.json"
    payload = _read_json(path)
    payload["candidates"][0], payload["candidates"][1] = (
        payload["candidates"][1],
        payload["candidates"][0],
    )
    _write_json(path, payload)

    validation = validate_dynamic_v3_clean_selection_s1_package(package_root=package)

    assert validation["status"] == "FAIL"
    assert validation["eligibility_status"] == "BLOCKED_INVALID_PREREGISTRATION_PACKAGE"


def test_result_source_injection_fails_closed(tmp_path: Path) -> None:
    package = _copy_package(tmp_path)
    path = package / "selection_rule.yaml"
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    payload["candidate_universe"][
        "source_config"
    ] = "reports/etf_portfolio/dynamic_v3_rescue/sweeps/legacy/leaderboard.json"
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    validation = validate_dynamic_v3_clean_selection_s1_package(package_root=package)

    assert validation["status"] == "FAIL"
    assert validation["failed_check_count"] >= 1


def test_window_overlap_or_test_selection_tamper_fails_closed(tmp_path: Path) -> None:
    overlap = _copy_package(tmp_path / "overlap")
    window_path = overlap / "window_catalog.yaml"
    windows = yaml.safe_load(window_path.read_text(encoding="utf-8"))
    windows["prospective_holdout"]["start"] = "2024-01-02"
    window_path.write_text(yaml.safe_dump(windows, sort_keys=False), encoding="utf-8")

    overlap_validation = validate_dynamic_v3_clean_selection_s1_package(package_root=overlap)

    assert overlap_validation["status"] == "FAIL"

    leakage = _copy_package(tmp_path / "leakage")
    selection_path = leakage / "selection_rule.yaml"
    selection = yaml.safe_load(selection_path.read_text(encoding="utf-8"))
    selection["selection"]["test_metric_selection_allowed"] = True
    selection_path.write_text(yaml.safe_dump(selection, sort_keys=False), encoding="utf-8")

    leakage_validation = validate_dynamic_v3_clean_selection_s1_package(package_root=leakage)

    assert leakage_validation["status"] == "FAIL"


@pytest.mark.parametrize(
    ("filename", "field_path", "replacement"),
    [
        ("selection_rule.yaml", ("selection", "top_n"), 21),
        (
            "selection_rule.yaml",
            ("selection", "score", "weights", "drawdown_preservation"),
            0.21,
        ),
        ("selection_rule.yaml", ("execution", "slippage_bps"), 3.0),
        (
            "window_catalog.yaml",
            ("historical_protocol_replay", "folds", 0, "test_end"),
            "2023-06-29",
        ),
    ],
)
def test_frozen_investment_policy_tamper_fails_closed(
    tmp_path: Path,
    filename: str,
    field_path: tuple[str | int, ...],
    replacement: object,
) -> None:
    package = _copy_package(tmp_path)
    path = package / filename
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    _set_nested(payload, field_path, replacement)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    validation = validate_dynamic_v3_clean_selection_s1_package(package_root=package)

    assert validation["status"] == "FAIL"
    assert validation["eligibility_status"] == "BLOCKED_INVALID_PREREGISTRATION_PACKAGE"
    assert any(
        "frozen policy fingerprint mismatch" in detail
        for check in validation["checks"]
        for detail in check["details"]
    )


def test_authorization_or_safety_tamper_fails_closed(tmp_path: Path) -> None:
    package = _copy_package(tmp_path)
    path = package / "eligibility.json"
    payload = _read_json(path)
    payload["clean_run_authorized"] = True
    payload["safety"]["clean_run_authorized"] = True
    _write_json(path, payload)

    validation = validate_dynamic_v3_clean_selection_s1_package(package_root=package)

    assert validation["status"] == "FAIL"
    assert validation["clean_run_authorized"] is False
    assert validation["safety"] == SAFETY


def _copy_package(root: Path) -> Path:
    package = root / "package"
    shutil.copytree(DEFAULT_PACKAGE_ROOT, package)
    return package


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _set_nested(
    payload: dict[str, Any], field_path: tuple[str | int, ...], replacement: object
) -> None:
    cursor: Any = payload
    for field in field_path[:-1]:
        cursor = cursor[field]
    cursor[field_path[-1]] = replacement
