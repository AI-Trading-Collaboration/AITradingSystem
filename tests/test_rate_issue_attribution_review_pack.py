from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest

from ai_trading_system.data.quality_issue_attribution_inventory import (
    build_attribution_readiness_inventory,
)
from ai_trading_system.data.rate_issue_attribution_review_pack import (
    DEFAULT_JSON_PATH,
    DEFAULT_MARKDOWN_PATH,
    EXPECTED_SITE_BY_CODE,
    RateIssueAttributionReviewError,
    ReviewPackPaths,
    build_rate_issue_attribution_review_pack,
    load_and_validate_rate_issue_attribution_review_pack,
    render_rate_issue_attribution_review_markdown,
    validate_rate_issue_attribution_review_pack,
)
from ai_trading_system.platform.artifacts import load_strict_json_path, write_json_atomic

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_real_review_pack_is_fresh_and_keeps_all_decisions_pending() -> None:
    built = build_rate_issue_attribution_review_pack(repo_root=PROJECT_ROOT)
    tracked = load_strict_json_path(PROJECT_ROOT / DEFAULT_JSON_PATH)

    assert tracked == built
    assert built["status"] == "SOURCE_OWNER_DECISION_PENDING"
    assert built["summary"] == {
        "candidate_site_count": 6,
        "single_source_row_site_count": 4,
        "current_and_previous_observation_site_count": 2,
        "error_site_count": 5,
        "warning_site_count": 1,
        "pending_source_owner_decision_count": 6,
        "contract_wave_candidate_count": 6,
        "runtime_attribution_implemented_site_count": 0,
        "new_issue_isolation_authorized_site_count": 0,
    }
    assert {
        candidate["issue_code"]: candidate["site_id"] for candidate in built["candidates"]
    } == EXPECTED_SITE_BY_CODE
    assert all(
        candidate["source_owner_decision"] == "PENDING_SOURCE_OWNER_DECISION"
        and candidate["runtime_attribution_implemented"] is False
        and candidate["new_issue_isolation_authorized"] is False
        for candidate in built["candidates"]
    )


def test_review_pack_distinguishes_single_row_and_row_pair_dependencies() -> None:
    built = build_rate_issue_attribution_review_pack(repo_root=PROJECT_ROOT)
    by_code = {candidate["issue_code"]: candidate for candidate in built["candidates"]}

    for code in (
        "rates_invalid_date",
        "rates_invalid_value",
        "rates_non_finite_value",
        "rates_out_of_range",
    ):
        assert by_code[code]["scope_taxonomy"] == "SINGLE_SOURCE_ROW"
        assert by_code[code]["row_dependencies"] == ["TRIGGER_ROW"]

    for code in (
        "rates_extreme_daily_change",
        "rates_suspicious_daily_change",
    ):
        assert by_code[code]["scope_taxonomy"] == "CURRENT_AND_PREVIOUS_VALID_OBSERVATION"
        assert by_code[code]["row_dependencies"] == [
            "PREVIOUS_VALID_SAME_SERIES_ROW",
            "TRIGGER_ROW",
        ]
        assert by_code[code]["affected_window_rule"] == "PREVIOUS_TO_TRIGGER_DATE_INCLUSIVE"


def test_review_pack_recommends_only_fail_closed_series_level_rule() -> None:
    built = build_rate_issue_attribution_review_pack(repo_root=PROJECT_ROOT)
    recommendation = built["contract_wave_recommendation"]

    assert (
        recommendation["recommended_initial_isolation_rule"]
        == "ALL_AFFECTED_RATE_SERIES_OUTSIDE_REQUIRED_SCOPE"
    )
    assert recommendation["series_attribution_must_be_complete_and_non_empty"] is True
    assert recommendation["unknown_or_incomplete_attribution_remains_global"] is True
    assert recommendation["window_or_row_level_isolation_authorized"] is False
    assert recommendation["runtime_schema_change_required"] is True
    assert recommendation["contract_wave_started"] is False
    assert built["authority"] == {
        "review_pack_is_authorization": False,
        "source_owner_decision_recorded": False,
        "new_issue_isolation_authorized": False,
        "runtime_contract_change_authorized": False,
        "capability_policy_change_authorized": False,
        "consumer_migration_authorized": False,
        "message_or_sample_scope_inference_allowed": False,
    }


def test_review_pack_exposes_every_missing_dimension_fail_closed_condition() -> None:
    built = build_rate_issue_attribution_review_pack(repo_root=PROJECT_ROOT)
    by_code = {candidate["issue_code"]: candidate for candidate in built["candidates"]}

    for candidate in built["candidates"]:
        assert candidate["affected_price_tickers"] == []
        assert candidate["affected_source_roles"] == ["primary_macro_rates"]
        assert (
            "EXACT_CANONICAL_SOURCE_ROLE"
            in candidate["attribution_completeness_requirements"]
        )
        assert (
            "NON_EMPTY_NORMALIZED_RATE_SERIES"
            in candidate["attribution_completeness_requirements"]
        )
        assert "RATE_SERIES_MISSING_OR_BLANK" in candidate["incomplete_when"]

    assert (
        by_code["rates_invalid_date"]["affected_window_rule"]
        == "UNAVAILABLE_FOR_INVALID_DATE"
    )
    for code in (
        "rates_invalid_value",
        "rates_non_finite_value",
        "rates_out_of_range",
    ):
        assert (
            by_code[code]["affected_window_rule"]
            == "EXACT_TRIGGER_DATE_WHEN_PARSEABLE"
        )

    for code in (
        "rates_extreme_daily_change",
        "rates_suspicious_daily_change",
    ):
        candidate = by_code[code]
        assert (
            "PARSEABLE_TRIGGER_AND_PREVIOUS_DATES"
            in candidate["attribution_completeness_requirements"]
        )
        assert "PREVIOUS_VALID_OBSERVATION_UNAVAILABLE" in candidate["incomplete_when"]

    recommendation = built["contract_wave_recommendation"]
    assert recommendation["unknown_or_incomplete_attribution_remains_global"] is True
    assert recommendation["window_or_row_level_isolation_authorized"] is False


def test_validator_rejects_output_tamper() -> None:
    built = build_rate_issue_attribution_review_pack(repo_root=PROJECT_ROOT)
    tampered = deepcopy(built)
    tampered["candidates"][0]["source_owner_decision"] = "APPROVE_FOR_CONTRACT_WAVE"

    validation = validate_rate_issue_attribution_review_pack(
        tampered,
        repo_root=PROJECT_ROOT,
    )

    assert validation["status"] == "FAIL"
    assert "review_pack_content_mismatch" in validation["errors"]


def test_markdown_exposes_pending_and_no_authorization_boundaries() -> None:
    built = build_rate_issue_attribution_review_pack(repo_root=PROJECT_ROOT)
    markdown = render_rate_issue_attribution_review_markdown(built)

    assert markdown == (PROJECT_ROOT / DEFAULT_MARKDOWN_PATH).read_text(encoding="utf-8")
    assert "PENDING_SOURCE_OWNER_DECISION" in markdown
    assert "当前新增隔离授权：`0`" in markdown
    assert "window/row-level isolation 仍未授权" in markdown
    assert "不自动启动 C3" in markdown


def test_validator_rejects_canonical_source_drift(tmp_path: Path) -> None:
    root = _minimal_repo(tmp_path)
    built = build_rate_issue_attribution_review_pack(repo_root=root)
    source_path = root / "src/ai_trading_system/data/quality.py"
    source_path.write_text(
        source_path.read_text(encoding="utf-8") + "\n# semantic source drift\n",
        encoding="utf-8",
    )

    validation = validate_rate_issue_attribution_review_pack(
        built,
        repo_root=root,
    )

    assert validation["status"] == "FAIL"
    assert validation["errors"][0].startswith("rebuild_failed:RateIssueAttributionReviewError:")
    assert "C1 inventory is not current" in validation["errors"][0]


def test_validator_rejects_dq_policy_drift(tmp_path: Path) -> None:
    root = _minimal_repo(tmp_path)
    built = build_rate_issue_attribution_review_pack(repo_root=root)
    policy_path = root / "config/data_quality.yaml"
    policy_path.write_text(
        policy_path.read_text(encoding="utf-8").replace(
            "extreme_daily_change_abs: 2.0",
            "extreme_daily_change_abs: 2.1",
        ),
        encoding="utf-8",
    )

    validation = validate_rate_issue_attribution_review_pack(
        built,
        repo_root=root,
    )

    assert validation["status"] == "FAIL"
    assert "review_pack_content_mismatch" in validation["errors"]
    assert "review_pack_id_mismatch" in validation["errors"]


def test_proposal_cannot_silently_widen_candidate_scope(tmp_path: Path) -> None:
    root = _minimal_repo(tmp_path)
    proposal_path = root / "config/data_quality/rate_row_issue_attribution_review_v1.yaml"
    proposal_path.write_text(
        proposal_path.read_text(encoding="utf-8").replace(
            "site_id: dq_issue_site_0e7f3d74bfa489801c83",
            "site_id: dq_issue_site_not_reviewed",
            1,
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        RateIssueAttributionReviewError,
        match="candidate site/code mismatch",
    ):
        build_rate_issue_attribution_review_pack(repo_root=root)


def test_proposal_cannot_silently_change_canonical_source_role(tmp_path: Path) -> None:
    root = _minimal_repo(tmp_path)
    proposal_path = root / "config/data_quality/rate_row_issue_attribution_review_v1.yaml"
    proposal_path.write_text(
        proposal_path.read_text(encoding="utf-8").replace(
            "affected_source_roles: [primary_macro_rates]",
            "affected_source_roles: [unreviewed_rate_source]",
            1,
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        RateIssueAttributionReviewError,
        match="candidate source role mismatch",
    ):
        build_rate_issue_attribution_review_pack(repo_root=root)


def test_loader_and_path_containment_fail_closed(tmp_path: Path) -> None:
    validation = load_and_validate_rate_issue_attribution_review_pack(
        repo_root=PROJECT_ROOT,
        pack_path=PROJECT_ROOT / DEFAULT_JSON_PATH,
    )
    assert validation["status"] == "PASS"

    paths = ReviewPackPaths(repo_root=tmp_path)
    with pytest.raises(RateIssueAttributionReviewError, match="escapes repository"):
        paths.resolve("../outside.json")


def _minimal_repo(tmp_path: Path) -> Path:
    quality_path = tmp_path / "src/ai_trading_system/data/quality.py"
    execution_path = tmp_path / "src/ai_trading_system/data/quality_execution.py"
    policy_dir = tmp_path / "config/data_quality"
    inventory_path = (
        tmp_path / "inputs/data_quality/dq_issue_attribution_readiness_inventory_v1.json"
    )
    quality_path.parent.mkdir(parents=True)
    policy_dir.mkdir(parents=True)
    inventory_path.parent.mkdir(parents=True)

    quality_path.write_text(
        """
def _validate_rates():
    DataQualityIssue(Severity.ERROR, "rates_invalid_date", "x", rows=1, sample="x", source="x")
    DataQualityIssue(Severity.ERROR, "rates_invalid_value", "x", rows=1, sample="x", source="x")
    DataQualityIssue(Severity.ERROR, "rates_non_finite_value", "x", rows=1, sample="x", source="x")

def _check_rate_ranges():
    DataQualityIssue(Severity.ERROR, "rates_out_of_range", "x", rows=1, sample="x", source="x")

def _check_rate_moves():
    DataQualityIssue(
        Severity.ERROR,
        "rates_extreme_daily_change",
        "x",
        rows=1,
        sample="x",
        source="x",
    )
    DataQualityIssue(
        Severity.WARNING,
        "rates_suspicious_daily_change",
        "x",
        rows=1,
        sample="x",
        source="x",
    )
""".lstrip(),
        encoding="utf-8",
    )
    execution_path.write_text(
        """
def _provenance_issue(code, message):
    return DataQualityIssue(Severity.ERROR, code, message)
""".lstrip(),
        encoding="utf-8",
    )
    capability_policy = """
schema_version: data_quality_consumer_capability_policy.v1
policy_id: fixture
policy_version: 1.0.0
status: OWNER_APPROVED_FIXTURE
owner: fixture_owner
owner_decision_id: fixture_decision
allowed_global_error_codes:
  - prices_non_market_session_date
global_error_attribution_rule: ALL_AFFECTED_INSTRUMENTS_OUTSIDE_REQUIRED_SCOPE
""".lstrip()
    for filename in (
        "decision_target_label_core_capability_v1.yaml",
        "regime_label_generator_capability_v1.yaml",
    ):
        (policy_dir / filename).write_text(capability_policy, encoding="utf-8")
    (policy_dir / "rate_row_issue_attribution_review_v1.yaml").write_text(
        (PROJECT_ROOT / "config/data_quality/rate_row_issue_attribution_review_v1.yaml").read_text(
            encoding="utf-8"
        ),
        encoding="utf-8",
    )
    (tmp_path / "config/data_quality.yaml").write_text(
        """
governance:
  policy_id: DATA_QUALITY_CACHE_GATE
  policy_version: data_quality_cache_gate.fixture
  status: REVIEWED
  owner: data_platform_owner
  role: data_quality
  reviewed_at: 2026-07-26
  review_condition: fixture drift requires review
rates:
  max_stale_calendar_days: 7
  min_plausible_value: -1.0
  max_plausible_value: 25.0
  suspicious_daily_change_abs: 0.75
  extreme_daily_change_abs: 2.0
  consistency_start_date: 2021-02-22
  series_overrides: {}
""".lstrip(),
        encoding="utf-8",
    )
    inventory = build_attribution_readiness_inventory(repo_root=tmp_path)
    write_json_atomic(inventory_path, inventory)
    return tmp_path
