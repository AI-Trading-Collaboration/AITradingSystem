from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path

import pytest

from ai_trading_system import (
    cache_catalog,
    data_refresh_audit,
    data_source_fallback_policy,
    dynamic_strategy_growth_tilt_pit_replay_engine_blocker_closure,
    dynamic_strategy_growth_tilt_remaining_candidate_pit_replay_blocker_closure,
    dynamic_strategy_growth_tilt_top3_candidate_level_pit_replay_blocker_closure,
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay_engine_remediation,
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck,
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck_after_candidate_blocker_closure,
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck_after_output_closure,
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck_after_remaining_blocker_closure,
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck_after_runtime_remediation,
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure,
)
from ai_trading_system.platform.artifacts import (
    sha256_path,
    write_json_atomic,
    write_json_atomic_without_trailing_newline,
    write_text_atomic,
)
from ai_trading_system.research_framework.runtime_metadata import (
    PIT_REPLAY_OBSERVE_ONLY_SAFETY_FALSE_FIELDS,
    with_pit_replay_observe_only_runtime_metadata,
)
from ai_trading_system.trading_engine import (
    data_freshness_summary,
    notification_delivery_audit_summary,
    notification_delivery_failure_classification,
    operator_brief_notification_approval_gate,
    operator_brief_notification_delivery_preflight,
    operator_brief_notification_dispatch_preview,
    operator_brief_notification_draft,
    operator_brief_notification_draft_dispatch,
    parameter_governance_daily_digest,
    parameter_governance_summary,
    pipeline_health_summary,
    retry_candidate_queue,
    retry_execution_dry_run,
)


def test_g1_2_canonical_json_writer_preserves_exact_legacy_bytes(tmp_path: Path) -> None:
    payload = {"z": "中文", "a": {"value": 1}, "items": [3, 2, 1]}
    expected = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True).encode(
        "utf-8"
    )
    path = tmp_path / "writer" / "artifact.json"
    result = write_json_atomic_without_trailing_newline(path, payload)

    assert path.read_bytes() == expected
    assert not path.read_bytes().endswith(b"\n")
    assert result.path == path
    assert result.atomic is True
    assert not tuple(path.parent.glob(f".{path.name}.*.tmp"))


def test_g1_2_canonical_text_writer_preserves_exact_legacy_bytes(tmp_path: Path) -> None:
    content = "第一行\nsecond line\n"
    path = tmp_path / "writer" / "artifact.md"
    result = write_text_atomic(path, content)

    assert path.read_bytes() == content.encode("utf-8")
    assert result.path == path
    assert result.atomic is True
    assert not tuple(path.parent.glob(f".{path.name}.*.tmp"))


def test_g1_2_private_writer_wrappers_are_removed() -> None:
    modules = (cache_catalog, data_refresh_audit, data_source_fallback_policy)

    assert all(not hasattr(module, "_write_json") for module in modules)
    assert all(not hasattr(module, "_write_text") for module in modules)


def test_g1_3a_summary_json_preserves_insertion_order_newline_and_oserror(
    tmp_path: Path,
) -> None:
    payload = {"z": "中文", "a": {"value": 1}}
    expected = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    path = tmp_path / "summary" / "artifact.json"

    result = write_json_atomic(path, payload, sort_keys=False)

    assert path.read_bytes() == expected
    assert path.read_bytes().endswith(b"\n")
    assert result.path == path
    assert result.atomic is True
    blocking_parent = tmp_path / "not-a-directory"
    blocking_parent.write_text("block", encoding="utf-8")
    with pytest.raises(OSError):
        write_json_atomic(blocking_parent / "artifact.json", payload, sort_keys=False)


def test_g1_3a_summary_private_writer_helpers_are_removed() -> None:
    modules = (
        data_freshness_summary,
        pipeline_health_summary,
        parameter_governance_summary,
        parameter_governance_daily_digest,
        notification_delivery_audit_summary,
    )

    assert all(not hasattr(module, "_write_json") for module in modules)
    assert all(not hasattr(module, "_write_text") for module in modules)


def test_g1_3b_notification_retry_private_writer_helpers_are_removed() -> None:
    modules = (
        operator_brief_notification_approval_gate,
        notification_delivery_failure_classification,
        operator_brief_notification_delivery_preflight,
        operator_brief_notification_dispatch_preview,
        operator_brief_notification_draft,
        operator_brief_notification_draft_dispatch,
        retry_execution_dry_run,
        retry_candidate_queue,
    )

    assert all(not hasattr(module, "_write_json") for module in modules)
    assert all(not hasattr(module, "_write_text") for module in modules)


def test_g1_3c_sha256_path_preserves_streaming_digest_and_oserror(tmp_path: Path) -> None:
    content = (b"checksum-boundary\x00" * 65537) + b"tail"
    path = tmp_path / "checksum.bin"
    path.write_bytes(content)

    assert sha256_path(path) == hashlib.sha256(content).hexdigest()
    assert sha256_path(path, chunk_size=17) == hashlib.sha256(content).hexdigest()
    with pytest.raises(OSError):
        sha256_path(tmp_path / "missing.bin")
    with pytest.raises(ValueError, match="chunk_size must be positive"):
        sha256_path(path, chunk_size=0)


def test_g1_3c_private_checksum_helpers_are_removed() -> None:
    modules = (
        data_freshness_summary,
        pipeline_health_summary,
        notification_delivery_audit_summary,
        operator_brief_notification_approval_gate,
        operator_brief_notification_delivery_preflight,
        operator_brief_notification_dispatch_preview,
        operator_brief_notification_draft,
        operator_brief_notification_draft_dispatch,
    )

    assert all(not hasattr(module, "_sha256") for module in modules)
    assert all(not hasattr(module, "_sha256_path") for module in modules)


def test_g1_3d_pit_replay_runtime_metadata_preserves_ordered_contract() -> None:
    errors = ["source_a missing", "source_b invalid"]
    result = with_pit_replay_observe_only_runtime_metadata(
        {"existing": "value", "as_of": "legacy", "production_effect": "unsafe"},
        source_validation_errors=errors,
        as_of_date=date(2026, 7, 11),
        task_register_id="TRADING-TEST",
        report_type="pit_replay_test",
        generated_at="2026-07-11T00:00:00Z",
    )

    assert list(result)[:14] == [
        "existing",
        "as_of",
        "production_effect",
        "generated_at",
        "market_regime",
        "market_regime_summary",
        "source_validation_errors",
        "source_validation_error_count",
        "manual_review_required",
        "manual_review_only",
        "observe_only",
        "task_register_id",
        "report_type",
        "broker_action",
    ]
    assert result["as_of"] == "2026-07-11"
    assert result["generated_at"] == "2026-07-11T00:00:00Z"
    assert result["source_validation_errors"] is errors
    assert result["source_validation_error_count"] == 2
    assert result["manual_review_required"] is True
    assert result["manual_review_only"] is True
    assert result["observe_only"] is True
    assert result["production_effect"] == "none"
    assert result["broker_action"] == "none"
    assert all(result[field] is False for field in PIT_REPLAY_OBSERVE_ONLY_SAFETY_FALSE_FIELDS)


def test_g1_3d_private_metadata_helpers_are_removed_and_safety_alias_is_shared() -> None:
    modules = (
        dynamic_strategy_growth_tilt_pit_replay_engine_blocker_closure,
        dynamic_strategy_growth_tilt_remaining_candidate_pit_replay_blocker_closure,
        dynamic_strategy_growth_tilt_top3_candidate_level_pit_replay_blocker_closure,
        dynamic_strategy_growth_tilt_top3_candidate_pit_replay_engine_remediation,
        dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck,
        dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck_after_candidate_blocker_closure,
        dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck_after_output_closure,
        dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck_after_remaining_blocker_closure,
        dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck_after_runtime_remediation,
        dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure,
    )

    assert len(PIT_REPLAY_OBSERVE_ONLY_SAFETY_FALSE_FIELDS) == 39
    assert all(not hasattr(module, "_with_runtime_metadata") for module in modules)
    assert all(
        module.SAFETY_FALSE_FIELDS is PIT_REPLAY_OBSERVE_ONLY_SAFETY_FALSE_FIELDS
        for module in modules
    )
