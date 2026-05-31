from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from ai_trading_system.trading_engine.parameters import shadow_backtest
from ai_trading_system.trading_engine.parameters.promotion_rules import PromotionDecision
from ai_trading_system.trading_engine.parameters.weight_stability import (
    _stability_summary_from_tuning,
    load_weight_stability_config,
)
from ai_trading_system.trading_engine.parameters.weight_stability_readiness import (
    build_weight_stability_readiness_payload,
    validate_weight_stability_readiness_payload,
    write_weight_stability_readiness_summary,
)
from trading_engine.weight_stability_readiness_helpers import (
    sample_weight_stability_readiness_payload,
)


def test_readiness_blocks_on_freshness_manifest_and_price_coverage(tmp_path: Path) -> None:
    as_of = date(2026, 5, 29)
    _write_input_artifacts(tmp_path, as_of, freshness_status="MISSING", diagnostics_ok=False)

    payload = build_weight_stability_readiness_payload(
        as_of=as_of,
        project_root=tmp_path,
        output_root=tmp_path / "artifacts" / "weight_stability_readiness",
    )

    assert validate_weight_stability_readiness_payload(payload) == []
    assert payload["metadata"]["status"] == "RECOVERY_FAILED"
    eligibility = payload["stable_tuning_eligibility"]
    assert eligibility["can_run"] is False
    assert "freshness" in eligibility["blocking_checks"]
    assert "backtest_manifest" in eligibility["blocking_checks"]
    assert "price_coverage" in eligibility["blocking_checks"]
    price = payload["readiness_checks"]["price_coverage"]
    assert price["high_missing_ratio_symbols"] == ["GOOGL", "BRK.B", "SGOV"]
    assert "SINGLE_DAY_PRICE_CACHE" in price["special_findings"]


def test_limited_signal_snapshot_is_usable_when_dates_align(tmp_path: Path) -> None:
    as_of = date(2026, 5, 29)
    _write_input_artifacts(tmp_path, as_of, freshness_status="OK", diagnostics_ok=True)

    payload = build_weight_stability_readiness_payload(
        as_of=as_of,
        project_root=tmp_path,
        output_root=tmp_path / "artifacts" / "weight_stability_readiness",
    )

    assert payload["metadata"]["status"] == "LIMITED_READY"
    assert payload["stable_tuning_eligibility"]["can_run"] is True
    signal = payload["readiness_checks"]["signal_snapshot"]
    assert signal["status"] == "LIMITED"
    assert signal["can_continue"] is True
    assert signal["real_signals"] == 2
    assert signal["missing_signals"] == 0


def test_missing_backtest_manifest_is_specific_blocker(tmp_path: Path) -> None:
    as_of = date(2026, 5, 29)
    _write_json(
        _artifact_path(
            tmp_path,
            "data_freshness",
            as_of,
            "market_data_freshness_summary.json",
        ),
        _freshness_payload(as_of, "OK"),
    )
    _write_json(
        tmp_path / "artifacts" / "signal_snapshots" / as_of.isoformat() / "signal_snapshot.json",
        _signal_snapshot_payload(as_of),
    )

    payload = build_weight_stability_readiness_payload(
        as_of=as_of,
        project_root=tmp_path,
        output_root=tmp_path / "artifacts" / "weight_stability_readiness",
    )

    assert payload["readiness_checks"]["backtest_manifest"]["status"] == "MISSING"
    assert payload["readiness_checks"]["backtest_manifest"]["reason"] == (
        "backtest_input_diagnostics_missing"
    )
    assert "backtest_manifest" in payload["stable_tuning_eligibility"]["blocking_checks"]


def test_stable_tuning_summary_references_blocked_readiness(tmp_path: Path) -> None:
    as_of = date(2026, 5, 29)
    readiness_path = (
        tmp_path
        / "artifacts"
        / "weight_stability_readiness"
        / as_of.isoformat()
        / "weight_stability_readiness_summary.json"
    )
    readiness_payload = sample_weight_stability_readiness_payload(as_of=as_of)
    write_weight_stability_readiness_summary(
        readiness_payload,
        readiness_path,
        readiness_path.with_suffix(".md"),
    )

    summary = _stability_summary_from_tuning(
        _weight_tuning_insufficient_payload(as_of),
        {"candidates": []},
        config=load_weight_stability_config(),
        config_path=Path("config/parameters/weight_tuning_v0_2_stability.yaml"),
        output_root=tmp_path / "artifacts" / "weight_stability",
        dry_run=False,
        input_readiness_payload=readiness_payload,
        input_readiness_path=readiness_path,
    )

    assert summary["metadata"]["status"] == "INSUFFICIENT_DATA"
    assert summary["metadata"]["reason"] == "input_readiness_blocked"
    assert summary["search_summary"]["candidates_backtested"] == 0
    assert summary["input_readiness"]["report"] == str(readiness_path)
    assert summary["recommended_candidate"]["reason"] == "input_readiness_blocked"


def test_shadow_backtest_supporting_artifact_references_readiness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    as_of = date(2026, 5, 29)
    monkeypatch.setattr(shadow_backtest, "PROJECT_ROOT", tmp_path)
    artifact_dir = tmp_path / "artifacts" / "weight_stability_readiness" / as_of.isoformat()
    write_weight_stability_readiness_summary(
        sample_weight_stability_readiness_payload(as_of=as_of),
        artifact_dir / "weight_stability_readiness_summary.json",
        artifact_dir / "weight_stability_readiness_summary.md",
    )
    decision = PromotionDecision(
        status="rejected",
        reason="signal quality limited",
        hard_rejections=("signal_quality_limited",),
        manual_review_items=(),
        criteria_results={},
    )

    payload = shadow_backtest._promotion_decision_payload(
        decision,
        as_of=as_of,
        backtest_mode="full_signal_backtest_limited",
    )

    assert "weight_stability_readiness" in payload["supporting_artifacts"]
    assert payload["weight_stability_readiness_status"] == "RECOVERY_FAILED"
    assert payload["weight_stability_readiness_can_run"] is False


def _write_input_artifacts(
    tmp_path: Path,
    as_of: date,
    *,
    freshness_status: str,
    diagnostics_ok: bool,
) -> None:
    _write_json(
        _artifact_path(
            tmp_path,
            "data_freshness",
            as_of,
            "market_data_freshness_summary.json",
        ),
        _freshness_payload(as_of, freshness_status),
    )
    _write_json(
        _artifact_path(
            tmp_path,
            "data_refresh",
            as_of,
            "market_data_refresh_summary.json",
        ),
        _refresh_payload(as_of, after_status=freshness_status),
    )
    _write_json(
        tmp_path / "artifacts" / "signal_snapshots" / as_of.isoformat() / "signal_snapshot.json",
        _signal_snapshot_payload(as_of),
    )
    _write_json(
        _artifact_path(
            tmp_path,
            "data_quality",
            as_of,
            "backtest_input_diagnostics.json",
        ),
        _diagnostics_payload(as_of, ok=diagnostics_ok),
    )
    manifest_path = (
        tmp_path
        / "artifacts"
        / "backtest_snapshots"
        / as_of.isoformat()
        / "backtest_input_manifest.json"
    )
    _write_json(manifest_path, {"status": "OK"})


def _freshness_payload(as_of: date, status: str) -> dict[str, object]:
    return {
        "schema_version": 1,
        "report_type": "market_data_freshness",
        "metadata": {
            "run_id": f"market-data-freshness-{as_of.isoformat()}",
            "status": status,
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
        },
        "data_dates": {
            "tracking_date": as_of.isoformat(),
            "expected_data_date": as_of.isoformat(),
            "effective_data_date": as_of.isoformat(),
            "latest_manifest_date": as_of.isoformat(),
        },
        "freshness": {"status": status},
        "tracking_readiness": {
            "readiness": "active_tracking" if status == "OK" else "cannot_track"
        },
    }


def _refresh_payload(as_of: date, *, after_status: str) -> dict[str, object]:
    return {
        "schema_version": 1,
        "report_type": "market_data_refresh",
        "metadata": {
            "run_id": f"market-data-refresh-{as_of.isoformat()}",
            "status": "OK",
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
        },
        "after": {"freshness_status": after_status},
        "actions": {"refreshed_backtest_manifest": True},
        "remaining_limitations": ["market data freshness remains MISSING"]
        if after_status != "OK"
        else [],
    }


def _signal_snapshot_payload(as_of: date) -> dict[str, object]:
    return {
        "schema_version": 1,
        "report_type": "signal_snapshot",
        "metadata": {
            "snapshot_id": f"signal-snapshot-{as_of.isoformat()}",
            "as_of": as_of.isoformat(),
            "status": "LIMITED",
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
        },
        "signals": {
            "trend_momentum": {"status": "OK", "quality": "price_derived", "values": []},
            "sector_strength": {"status": "OK", "quality": "price_derived", "values": []},
            "macro_liquidity": {"status": "LIMITED", "quality": "proxy_or_neutral", "values": []},
            "earnings_quality": _fallback_signal(),
            "valuation_risk": _fallback_signal(),
            "event_risk": _fallback_signal(),
        },
    }


def _diagnostics_payload(as_of: date, *, ok: bool) -> dict[str, object]:
    price_assets = [
        {"symbol": "QQQ", "status": "OK", "missing_ratio": 0.0},
        {"symbol": "SMH", "status": "OK", "missing_ratio": 0.0},
        {"symbol": "NVDA", "status": "OK", "missing_ratio": 0.0},
        {"symbol": "TSM", "status": "OK", "missing_ratio": 0.0},
        {"symbol": "MSFT", "status": "OK", "missing_ratio": 0.0},
    ]
    if not ok:
        price_assets.extend(
            [
                {"symbol": "GOOGL", "status": "FAILED", "missing_ratio": 0.999},
                {"symbol": "BRK.B", "status": "FAILED", "missing_ratio": 0.999},
                {"symbol": "SGOV", "status": "FAILED", "missing_ratio": 0.999},
            ]
        )
    return {
        "schema_version": 1,
        "report_type": "backtest_input_diagnostics",
        "metadata": {
            "run_id": f"backtest-input-diagnostics-{as_of.isoformat()}",
            "status": "OK" if ok else "FAILED",
            "production_effect": "none",
            "manual_review_required": not ok,
        },
        "summary": {
            "overall_status": "LIMITED" if ok else "FAILED",
            "backtest_mode": "full_signal_backtest_limited" if ok else "blocked",
            "can_run_shadow_backtest": ok,
            "blocking_reasons": []
            if ok
            else [
                "Insufficient date coverage: required 2022-05-23 to 2026-05-29, "
                "available 2026-05-29 to 2026-05-29.",
                "Price missing ratio is too high for GOOGL, BRK.B, SGOV.",
            ],
        },
        "checks": {
            "asset_coverage": {"status": "OK", "missing_assets": []},
            "date_coverage": {
                "status": "OK" if ok else "INSUFFICIENT_DATA",
                "required_start_date": "2022-05-23",
                "required_end_date": as_of.isoformat(),
                "available_start_date": "2022-05-23" if ok else as_of.isoformat(),
                "available_end_date": as_of.isoformat(),
            },
            "price_data": {
                "status": "OK" if ok else "LIMITED",
                "max_allowed_missing_ratio": 0.02,
                "assets": price_assets,
            },
        },
    }


def _weight_tuning_insufficient_payload(as_of: date) -> dict[str, object]:
    return {
        "schema_version": 1,
        "report_type": "weight_tuning",
        "metadata": {
            "run_id": f"weight-tuning-{as_of.isoformat()}",
            "generated_at": datetime(2026, 5, 31, tzinfo=UTC).isoformat(),
            "status": "INSUFFICIENT_DATA",
            "reason": "validate-data gate failed before weight tuning.",
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "market_regime": "ai_after_chatgpt",
            "market_regime_anchor": "2022-11-30",
            "requested_date_range": {"start": "2022-12-01", "end": as_of.isoformat()},
        },
        "search": {"candidates_generated": 0, "candidates_evaluated": 0},
        "signal_quality": {"status": "LIMITED"},
        "recommended_candidate": {"status": "no_candidate", "reason": "blocked"},
        "safety": {
            "fallback_signals_free_tuned": False,
            "production_config_modified": False,
            "production_write_allowed": False,
        },
    }


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _artifact_path(tmp_path: Path, section: str, as_of: date, filename: str) -> Path:
    return tmp_path / "artifacts" / section / as_of.isoformat() / filename


def _fallback_signal() -> dict[str, object]:
    return {
        "status": "NEUTRAL_FALLBACK",
        "quality": "neutral_fallback",
        "values": [],
    }
