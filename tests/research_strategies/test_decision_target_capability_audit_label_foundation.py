from __future__ import annotations

import copy
import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

from ai_trading_system.contracts import CanonicalStatus
from ai_trading_system.research_framework import (
    ExperimentRunRequest,
    resolve_experiment_spec,
    run_experiment,
)
from ai_trading_system.research_framework.plugins.decision_target_capability_audit_label_foundation import (  # noqa: E501
    BLOCKED_STATUS,
    READY_STATUS,
    build_decision_target_source_package,
    build_label_payload,
    decision_target_label_foundation_registry,
    render_label_markdown,
    validate_label_payload,
)
from ai_trading_system.trading_calendar import is_us_equity_trading_day
from ai_trading_system.yaml_loader import safe_load_yaml_path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SPEC_PATH = (
    PROJECT_ROOT
    / "config/research/experiments/decision_target_capability_audit_label_foundation.yaml"
)
POLICY_PATH = (
    PROJECT_ROOT / "config/research/decision_target_capability_audit_label_foundation_v2.yaml"
)
DATA_QUALITY_POLICY_PATH = PROJECT_ROOT / "config/data_quality.yaml"
CAPABILITY_POLICY_PATH = (
    PROJECT_ROOT / "config/data_quality/decision_target_label_core_capability_v1.yaml"
)
AS_OF = date(2021, 5, 28)
GENERATED_AT = datetime(2026, 7, 25, 12, 0, tzinfo=UTC)


def test_label_foundation_is_deterministic_temporally_explicit_and_tamper_evident(
    tmp_path: Path,
) -> None:
    sources, _ = _sources(tmp_path)

    first = build_label_payload(sources, as_of=AS_OF)
    second = build_label_payload(copy.deepcopy(sources), as_of=AS_OF)

    assert first == second
    assert first["status"] == READY_STATUS
    assert first["model_training_executed"] is False
    assert first["candidate_search_executed"] is False
    assert first["target_weights_changed"] is False
    rows = first["evaluation"]["label_rows"]
    first_row = rows[0]
    assert first_row["decision_date"] == "2021-02-22"
    assert first_row["horizon_id"] == "1d"
    assert first_row["label_start_date"] == "2021-02-23"
    assert first_row["label_end_date"] == "2021-02-23"
    assert first_row["label_available_on_session"] == first_row["label_end_date"]
    targets = first_row["excess_return_targets"]
    assert (
        abs(targets["QQQ_MINUS_SGOV"] - targets["SPY_MINUS_SGOV"] - targets["QQQ_MINUS_SPY"])
        < 1.0e-12
    )
    assert set(first_row["future_path_risk"]) == {"QQQ", "SPY", "SGOV"}
    assert first["label_foundation_summary"]["embargo_numeric_value_defined"] is False
    assert validate_label_payload(first, sources, as_of=AS_OF) == ()

    tampered = copy.deepcopy(first)
    tampered["evaluation"]["label_rows"][0]["excess_return_targets"]["QQQ_MINUS_SGOV"] = 99.0
    assert validate_label_payload(tampered, sources, as_of=AS_OF) == ("LABEL_CONTENT_MISMATCH",)

    tampered_summary = copy.deepcopy(first)
    tampered_summary["label_foundation_summary"]["label_row_count"] = 0
    assert validate_label_payload(tampered_summary, sources, as_of=AS_OF) == (
        "LABEL_SUMMARY_MISMATCH",
        "LABEL_MARKDOWN_MISMATCH",
    )

    tampered_safety = copy.deepcopy(first)
    tampered_safety["target_weights_changed"] = True
    assert validate_label_payload(tampered_safety, sources, as_of=AS_OF) == (
        "LABEL_SAFETY_BOUNDARY_MISMATCH",
    )


def test_canonical_data_quality_failure_blocks_before_panel_materialization(
    tmp_path: Path,
) -> None:
    policy = safe_load_yaml_path(POLICY_PATH)
    prices = _prices()
    bad = prices.iloc[0].copy()
    prices = pd.concat([prices, bad.to_frame().T], ignore_index=True)
    prices_path, rates_path = _write_market_sources(tmp_path, prices=prices)

    package = build_decision_target_source_package(
        policy=policy,
        prices_path=prices_path,
        rates_path=rates_path,
        output_root=tmp_path / "source",
        as_of=AS_OF,
        expected_price_tickers=["QQQ", "SPY", "SGOV"],
        expected_rate_series=["DGS3MO"],
        captured_at=GENERATED_AT,
        capability_policy_path=CAPABILITY_POLICY_PATH,
        data_quality_policy_path=DATA_QUALITY_POLICY_PATH,
    )
    sources = {
        "label_policy": policy,
        "market_panel_package": package,
        "data_quality_policy": safe_load_yaml_path(DATA_QUALITY_POLICY_PATH),
        "requirement_text": "research-only",
    }
    payload = build_label_payload(sources, as_of=AS_OF)

    assert package["data_quality_evidence"]["passed"] is False
    assert package["panel_materialized"] is False
    assert package["panel"] is None
    assert payload["status"] == BLOCKED_STATUS
    assert "prices_duplicate_keys" in payload["strict_validation_errors"]
    assert payload["evaluation"] is None


def test_prospective_source_value_fails_closed_even_with_updated_file_commitment(
    tmp_path: Path,
) -> None:
    sources, _ = _sources(tmp_path)
    package = copy.deepcopy(sources["market_panel_package"])
    panel_path = Path(package["panel"]["path"])
    panel = pd.read_csv(panel_path)
    future = panel.iloc[-1].copy()
    future["date"] = "2021-06-01"
    panel = pd.concat([panel, future.to_frame().T], ignore_index=True)
    panel.to_csv(panel_path, index=False, lineterminator="\n")
    package["panel"] = {
        "path": str(panel_path),
        "sha256": hashlib.sha256(panel_path.read_bytes()).hexdigest(),
        "size_bytes": panel_path.stat().st_size,
        "row_count": len(panel),
    }
    sources["market_panel_package"] = package

    payload = build_label_payload(sources, as_of=AS_OF)

    assert payload["status"] == BLOCKED_STATUS
    assert payload["strict_validation_errors"] == ["CAPABILITY_RECEIPT_VERIFICATION_FAILED"]


def test_scoped_dq_exception_and_numeric_embargo_fail_closed(tmp_path: Path) -> None:
    sources, _ = _sources(tmp_path)
    package = copy.deepcopy(sources["market_panel_package"])
    package["scoped_data_quality_exception_used"] = True
    sources["market_panel_package"] = package

    scoped = build_label_payload(sources, as_of=AS_OF)

    assert scoped["status"] == BLOCKED_STATUS
    assert "SCOPED_DATA_QUALITY_EXCEPTION_FORBIDDEN" in scoped["strict_validation_errors"]

    sources, _ = _sources(tmp_path / "embargo")
    policy = copy.deepcopy(sources["label_policy"])
    policy["split_readiness"]["embargo_sessions"] = 5
    sources["label_policy"] = policy
    embargo = build_label_payload(sources, as_of=AS_OF)

    assert embargo["status"] == BLOCKED_STATUS
    assert embargo["strict_validation_errors"] == ["UNGOVERNED_EMBARGO_POLICY_INVALID"]


def test_generic_runner_writes_pass_envelope_ledger_and_chinese_report(
    tmp_path: Path,
) -> None:
    _, paths = _sources(tmp_path)
    result = run_experiment(
        resolved_spec=resolve_experiment_spec(SPEC_PATH),
        plugins=decision_target_label_foundation_registry(),
        request=ExperimentRunRequest(
            project_root=PROJECT_ROOT,
            output_root=tmp_path / "outputs",
            docs_root=tmp_path / "docs",
            as_of=AS_OF,
            input_overrides=paths,
            strict=True,
            generated_at=GENERATED_AT,
        ),
    )

    assert result.payload["status"] == READY_STATUS
    assert result.envelope.status is CanonicalStatus.PASS
    assert result.envelope.data_quality_required is True
    assert result.envelope.data_quality is not None
    assert result.envelope.data_quality.ready is True
    assert result.ledger.entry("evaluate_and_render").status is CanonicalStatus.PASS
    assert result.output_paths["reader_markdown"].read_text(encoding="utf-8") == (
        render_label_markdown(result.payload)
    )


def test_experiment_contract_maps_ready_and_blocked_without_action_effect() -> None:
    spec = resolve_experiment_spec(SPEC_PATH).value

    assert spec.data_quality_required is True
    assert spec.investment_facing_envelope is False
    assert spec.production_effect.value == "none"
    assert spec.broker_action == "none"
    assert spec.canonical_status(READY_STATUS) is CanonicalStatus.PASS
    assert spec.canonical_status(BLOCKED_STATUS) is CanonicalStatus.BLOCKED


def _sources(tmp_path: Path) -> tuple[dict[str, object], dict[str, Path]]:
    policy = safe_load_yaml_path(POLICY_PATH)
    prices_path, rates_path = _write_market_sources(tmp_path, prices=_prices())
    source_root = tmp_path / "source"
    build_decision_target_source_package(
        policy=policy,
        prices_path=prices_path,
        rates_path=rates_path,
        output_root=source_root,
        as_of=AS_OF,
        expected_price_tickers=["QQQ", "SPY", "SGOV"],
        expected_rate_series=["DGS3MO"],
        captured_at=GENERATED_AT,
        capability_policy_path=CAPABILITY_POLICY_PATH,
        data_quality_policy_path=DATA_QUALITY_POLICY_PATH,
    )
    package_path = source_root / "market_panel_package.json"
    stored_package = json.loads(package_path.read_text(encoding="utf-8"))
    requirement_path = tmp_path / "requirement.md"
    requirement_path.parent.mkdir(parents=True, exist_ok=True)
    requirement_path.write_text("research-only; no model or weights\n", encoding="utf-8")
    paths = {
        "label_policy": POLICY_PATH,
        "market_panel_package": package_path,
        "data_quality_policy": DATA_QUALITY_POLICY_PATH,
        "requirement_text": requirement_path,
    }
    return {
        "label_policy": policy,
        "market_panel_package": stored_package,
        "data_quality_policy": safe_load_yaml_path(DATA_QUALITY_POLICY_PATH),
        "requirement_text": requirement_path.read_text(encoding="utf-8"),
    }, paths


def _write_market_sources(
    root: Path,
    *,
    prices: pd.DataFrame,
) -> tuple[Path, Path]:
    root.mkdir(parents=True, exist_ok=True)
    prices_path = root / "prices_daily.csv"
    prices.to_csv(prices_path, index=False, lineterminator="\n")
    rates_path = root / "rates_daily.csv"
    dates = sorted(prices["date"].unique())
    pd.DataFrame(
        {
            "date": dates,
            "series": "DGS3MO",
            "value": 5.0,
            "source": "test_fixture",
        }
    ).to_csv(rates_path, index=False, lineterminator="\n")
    return prices_path, rates_path


def _prices() -> pd.DataFrame:
    dates = pd.DatetimeIndex(
        [
            value
            for value in pd.date_range("2021-02-22", AS_OF.isoformat(), freq="D")
            if is_us_equity_trading_day(value.date())
        ]
    )
    qqq_returns = [
        0.0,
        *([0.01, -0.007, 0.006, -0.003, 0.008] * len(dates))[: len(dates) - 1],
    ]
    spy_returns = [value * 0.7 for value in qqq_returns]
    sgov_returns = [0.0, *([0.00005] * (len(dates) - 1))]
    rows: list[dict[str, object]] = []
    for ticker, returns in (
        ("QQQ", qqq_returns),
        ("SPY", spy_returns),
        ("SGOV", sgov_returns),
    ):
        price = 100.0
        for session, asset_return in zip(dates, returns, strict=True):
            price *= 1.0 + asset_return
            rows.append(
                {
                    "date": session.date().isoformat(),
                    "ticker": ticker,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "adj_close": price,
                    "volume": 1_000_000,
                    "source": "test_fixture",
                }
            )
    return pd.DataFrame(rows)
