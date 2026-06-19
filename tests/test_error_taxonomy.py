from pathlib import Path

import pytest
import yaml

from ai_trading_system.error_taxonomy import (
    ERROR_CATEGORY_CODES,
    REQUIRED_LOG_FIELDS,
    ErrorCategory,
    build_error_record,
    error_category_definition,
    normalize_error_category,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_error_taxonomy_matches_engineering_closeout_policy() -> None:
    policy = yaml.safe_load(
        (PROJECT_ROOT / "config" / "engineering_closeout_policy.yaml").read_text(
            encoding="utf-8"
        )
    )
    stage_b = policy["stage_b"]

    assert list(ERROR_CATEGORY_CODES) == stage_b["required_error_categories"]
    assert list(REQUIRED_LOG_FIELDS) == stage_b["required_log_fields"]


def test_error_taxonomy_definitions_have_next_actions() -> None:
    for code in ERROR_CATEGORY_CODES:
        definition = error_category_definition(code)

        assert definition.code.value == code
        assert definition.summary
        assert definition.default_next_action


def test_build_error_record_normalizes_category() -> None:
    record = build_error_record(
        category=ErrorCategory.INPUT_MISSING,
        message="source artifact is missing",
        stage="stage_b_readiness",
        status="FAIL",
        artifact_id="report_index",
    )

    assert record["error_category"] == "INPUT_MISSING"
    assert record["stage"] == "stage_b_readiness"
    assert record["artifact_id"] == "report_index"
    assert record["next_action"] == "locate_or_generate_required_input_without_fabrication"


def test_unknown_error_category_fails_closed() -> None:
    with pytest.raises(ValueError, match="Unknown error category"):
        normalize_error_category("MISSING_BUT_NOT_GOVERNED")
