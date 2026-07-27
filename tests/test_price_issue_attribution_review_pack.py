from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
from pathlib import Path

import pytest

from ai_trading_system.contracts.data_quality_attribution import (
    load_price_non_market_session_attribution_decision,
)
from ai_trading_system.data.price_issue_attribution_review_pack import (
    AUTHORITY_FALSE_FIELDS,
    DEFAULT_JSON_PATH,
    DEFAULT_MARKDOWN_PATH,
    EXPECTED_ISSUE_CODE,
    EXPECTED_SITE_ID,
    PriceIssueAttributionReviewError,
    ReviewPackPaths,
    build_price_issue_attribution_review_pack,
    render_price_issue_attribution_review_markdown,
    validate_price_issue_attribution_review_pack,
)
from ai_trading_system.data.quality_issue_attribution_inventory import (
    build_attribution_readiness_inventory,
)
from ai_trading_system.platform.artifacts import (
    load_strict_json_path,
    write_json_atomic,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_real_review_pack_is_frozen_by_exact_owner_decision_binding() -> None:
    tracked = _tracked_review_pack()
    decision = load_price_non_market_session_attribution_decision()

    assert decision.review_pack_id == tracked["review_pack_id"]
    assert decision.review_pack_sha256 == sha256(
        (PROJECT_ROOT / DEFAULT_JSON_PATH).read_bytes()
    ).hexdigest()
    assert tracked["status"] == "SOURCE_OWNER_DECISION_PENDING"
    assert tracked["summary"] == {
        "candidate_site_count": 1,
        "distinct_non_session_date_row_set_site_count": 1,
        "caller_supplied_severity_site_count": 1,
        "pending_source_owner_decision_count": 1,
        "current_rows_value_is_distinct_date_count": True,
        "current_rows_value_is_source_row_count": False,
        "complete_trigger_row_identity_required": True,
        "runtime_attribution_implemented_site_count": 0,
        "new_issue_isolation_authorized_site_count": 0,
    }
    candidate = tracked["candidate"]
    assert candidate["site_id"] == EXPECTED_SITE_ID
    assert candidate["issue_code"] == EXPECTED_ISSUE_CODE
    assert candidate["source_owner_decision"] == "PENDING_SOURCE_OWNER_DECISION"
    assert candidate["runtime_attribution_implemented"] is False


def test_review_pack_separates_distinct_dates_from_trigger_rows() -> None:
    candidate = _tracked_review_pack()["candidate"]

    assert candidate["current_rows_semantics"] == "DISTINCT_NON_SESSION_DATE_COUNT"
    assert candidate["current_sample_semantics"] == ("FIRST_TEN_DISTINCT_NON_SESSION_DATES")
    assert candidate["affected_rows_rule"] == (
        "ALL_TRIGGER_ROWS_WITH_SOURCE_ORDINAL_AND_CANONICAL_ROW_DIGEST"
    )
    assert candidate["row_identity_fields"] == [
        "source_ordinal",
        "canonical_row_digest",
    ]
    assert (
        "DISTINCT_DATE_COUNT_MISTAKEN_FOR_TRIGGER_ROW_COUNT" in candidate["false_isolation_risks"]
    )
    assert (
        "FIRST_TEN_SAMPLE_DATES_MISTAKEN_FOR_COMPLETE_DATE_SCOPE"
        in candidate["false_isolation_risks"]
    )


def test_review_pack_exposes_all_six_dimensions_and_fail_closed_conditions() -> None:
    built = _tracked_review_pack()
    candidate = built["candidate"]

    assert candidate["affected_price_tickers_rule"] == (
        "DISTINCT_NORMALIZED_NON_EMPTY_TICKERS_FROM_ALL_TRIGGER_ROWS"
    )
    assert candidate["affected_rate_series"] == []
    assert candidate["affected_source_roles"] == ["primary_market_prices"]
    assert candidate["affected_date_rule"] == ("DISTINCT_NON_SESSION_DATES_WITHIN_REQUESTED_WINDOW")
    assert candidate["affected_fields"] == ["date"]
    assert candidate["affected_rows_rule"] == (
        "ALL_TRIGGER_ROWS_WITH_SOURCE_ORDINAL_AND_CANONICAL_ROW_DIGEST"
    )
    assert "ANY_TRIGGER_ROW_TICKER_IS_MISSING_OR_BLANK" in candidate["incomplete_when"]
    assert (
        "CALENDAR_AUTHORITY_OR_SPECIAL_CLOSURE_BINDING_IS_MISSING_OR_STALE"
        in candidate["incomplete_when"]
    )
    assert (
        built["contract_wave_recommendation"]["unknown_or_incomplete_attribution_remains_global"]
        is True
    )


def test_review_pack_binds_calendar_authority_and_keeps_all_authority_false() -> None:
    built = _tracked_review_pack()
    binding_roles = {item["role"] for item in built["input_bindings"]}

    assert {
        "c1_readiness_inventory",
        "source_owner_review_proposal",
        "canonical_dq_source",
        "reviewed_dq_policy",
        "calendar_runtime_source",
        "special_closure_policy_loader",
        "reviewed_special_closure_policy",
    } == binding_roles
    assert built["calendar_authority"]["special_closure_policy_bound"] is True
    assert built["authority"] == {field: False for field in AUTHORITY_FALSE_FIELDS}
    assert built["safety"]["data_quality_issue_schema_changed"] is False
    assert built["safety"]["capability_policy_or_classifier_changed"] is False
    assert built["safety"]["consumer_migration_executed"] is False


def test_validator_rejects_output_tamper(tmp_path: Path) -> None:
    root = _minimal_repo(tmp_path)
    built = build_price_issue_attribution_review_pack(repo_root=root)
    tampered = deepcopy(built)
    tampered["candidate"]["source_owner_decision"] = "APPROVE_FOR_CONTRACT_WAVE"

    validation = validate_price_issue_attribution_review_pack(
        tampered,
        repo_root=root,
    )

    assert validation["status"] == "FAIL"
    assert "review_pack_content_mismatch" in validation["errors"]


def test_markdown_exposes_pending_and_no_authorization_boundaries() -> None:
    built = _tracked_review_pack()
    markdown = render_price_issue_attribution_review_markdown(built)

    assert markdown == (PROJECT_ROOT / DEFAULT_MARKDOWN_PATH).read_text(encoding="utf-8")
    assert "PENDING_SOURCE_OWNER_DECISION" in markdown
    assert "当前新增 runtime/schema/isolation/consumer 授权：`0`" in markdown
    assert "distinct non-session date 数" in markdown
    assert "window/row-level isolation 仍未授权" in markdown
    assert "不自动启动 C3" in markdown


def test_validator_rejects_canonical_dq_source_drift(tmp_path: Path) -> None:
    root = _minimal_repo(tmp_path)
    built = build_price_issue_attribution_review_pack(repo_root=root)
    source_path = root / "src/ai_trading_system/data/quality.py"
    source_path.write_text(
        source_path.read_text(encoding="utf-8") + "\n# source drift\n",
        encoding="utf-8",
    )

    validation = validate_price_issue_attribution_review_pack(
        built,
        repo_root=root,
    )

    assert validation["status"] == "FAIL"
    assert validation["errors"][0].startswith("rebuild_failed:PriceIssueAttributionReviewError:")
    assert "C1 inventory is not current" in validation["errors"][0]


@pytest.mark.parametrize(
    ("relative_path", "needle", "replacement"),
    [
        (
            "src/ai_trading_system/trading_calendar.py",
            "value.weekday() < 5",
            "value.weekday() <= 5",
        ),
        (
            "config/data/us_equity_special_closure_registry.yaml",
            "status: REVIEWED",
            "status: REVISED",
        ),
        (
            "config/data_quality.yaml",
            "policy_version: data_quality_cache_gate.fixture",
            "policy_version: data_quality_cache_gate.fixture-drift",
        ),
    ],
)
def test_validator_rejects_bound_calendar_or_policy_drift(
    tmp_path: Path,
    relative_path: str,
    needle: str,
    replacement: str,
) -> None:
    root = _minimal_repo(tmp_path)
    built = build_price_issue_attribution_review_pack(repo_root=root)
    path = root / relative_path
    path.write_text(
        path.read_text(encoding="utf-8").replace(needle, replacement),
        encoding="utf-8",
    )

    validation = validate_price_issue_attribution_review_pack(
        built,
        repo_root=root,
    )

    assert validation["status"] == "FAIL"
    assert "review_pack_content_mismatch" in validation["errors"]
    assert "review_pack_id_mismatch" in validation["errors"]


@pytest.mark.parametrize(
    ("needle", "replacement", "error_match"),
    [
        (
            "scope_taxonomy: DISTINCT_NON_SESSION_DATE_ROW_SET",
            "scope_taxonomy: CONTIGUOUS_WINDOW",
            "candidate scope_taxonomy must equal",
        ),
        (
            "    - primary_market_prices",
            "    - unreviewed_price_source",
            "candidate affected_source_roles must equal",
        ),
        (
            "source_owner_decision: PENDING_SOURCE_OWNER_DECISION",
            "source_owner_decision: APPROVE_FOR_CONTRACT_WAVE",
            "candidate source_owner_decision must equal",
        ),
        (
            "  affected_fields:\n    - date",
            "  affected_fields: []",
            "candidate affected_fields must equal",
        ),
    ],
)
def test_proposal_tamper_fails_closed(
    tmp_path: Path,
    needle: str,
    replacement: str,
    error_match: str,
) -> None:
    root = _minimal_repo(tmp_path)
    proposal_path = root / "config/data_quality/price_non_market_session_attribution_review_v1.yaml"
    proposal_path.write_text(
        proposal_path.read_text(encoding="utf-8").replace(
            needle,
            replacement,
            1,
        ),
        encoding="utf-8",
    )

    with pytest.raises(PriceIssueAttributionReviewError, match=error_match):
        build_price_issue_attribution_review_pack(repo_root=root)


def test_loader_and_path_containment_fail_closed(tmp_path: Path) -> None:
    decision = load_price_non_market_session_attribution_decision()
    assert decision.review_pack_id == _tracked_review_pack()["review_pack_id"]

    paths = ReviewPackPaths(repo_root=tmp_path)
    with pytest.raises(
        PriceIssueAttributionReviewError,
        match="escapes repository",
    ):
        paths.resolve("../outside.json")


def _tracked_review_pack() -> dict[str, object]:
    payload = load_strict_json_path(PROJECT_ROOT / DEFAULT_JSON_PATH)
    assert isinstance(payload, dict)
    return payload


def _minimal_repo(tmp_path: Path) -> Path:
    quality_path = tmp_path / "src/ai_trading_system/data/quality.py"
    execution_path = tmp_path / "src/ai_trading_system/data/quality_execution.py"
    calendar_path = tmp_path / "src/ai_trading_system/trading_calendar.py"
    closure_loader_path = tmp_path / "src/ai_trading_system/us_equity_special_closure_policy.py"
    policy_dir = tmp_path / "config/data_quality"
    closure_policy_path = tmp_path / "config/data/us_equity_special_closure_registry.yaml"
    inventory_path = (
        tmp_path / "inputs/data_quality/dq_issue_attribution_readiness_inventory_v1.json"
    )
    quality_path.parent.mkdir(parents=True)
    policy_dir.mkdir(parents=True)
    closure_policy_path.parent.mkdir(parents=True)
    inventory_path.parent.mkdir(parents=True)

    quality_path.write_text(
        """
def _check_price_market_calendar_dates(
    frame,
    requested_window,
    issues,
    *,
    source,
    severity,
):
    issues.append(
        DataQualityIssue(
            severity,
            "prices_non_market_session_date",
            "x",
            rows=1,
            sample="x",
            source=source,
            affected_instruments=("QQQ",),
        )
    )
""".lstrip(),
        encoding="utf-8",
    )
    execution_path.write_text("", encoding="utf-8")
    calendar_path.write_text(
        """
def is_us_equity_trading_day(value):
    return value.weekday() < 5 and value not in us_equity_full_day_holidays(value.year)
""".lstrip(),
        encoding="utf-8",
    )
    closure_loader_path.write_text(
        """
def default_us_equity_special_closure_policy():
    return load_policy("config/data/us_equity_special_closure_registry.yaml")
""".lstrip(),
        encoding="utf-8",
    )
    closure_policy_path.write_text(
        """
schema_version: us_equity_special_closure_registry.v1
policy_id: us_equity_special_closure_registry
status: REVIEWED
closures: []
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
        (policy_dir / filename).write_text(
            capability_policy,
            encoding="utf-8",
        )
    (policy_dir / "price_non_market_session_attribution_review_v1.yaml").write_text(
        (
            PROJECT_ROOT / "config/data_quality/price_non_market_session_attribution_review_v1.yaml"
        ).read_text(encoding="utf-8"),
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
  reviewed_at: 2026-07-27
  review_condition: fixture drift requires review
prices:
  consistency_start_date: 2021-02-22
""".lstrip(),
        encoding="utf-8",
    )
    inventory = build_attribution_readiness_inventory(repo_root=tmp_path)
    write_json_atomic(inventory_path, inventory)
    return tmp_path
