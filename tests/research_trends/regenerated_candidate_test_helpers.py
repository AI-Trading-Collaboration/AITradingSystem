from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

from ai_trading_system.first_layer_candidate_generator_runtime import (
    validate_candidate_generation_bundle,
)
from ai_trading_system.first_layer_candidate_generators_regenerate import (
    run_first_layer_candidate_generators_regenerate,
)
from ai_trading_system.first_layer_candidate_signal_generator import (
    CandidateGenerationBundle,
    CandidateGeneratorContext,
)
from ai_trading_system.regenerated_candidate_actual_path_validation import (
    run_regenerated_candidate_actual_path_validation,
)
from ai_trading_system.regenerated_candidate_generator_common import (
    REGENERATED_CANDIDATE_FAMILY,
)


def write_price_fixture(tmp_path: Path, *, include_vix: bool = True) -> Path:
    path = tmp_path / "prices_daily.csv"
    dates = pd.bdate_range("2022-12-01", "2023-02-15")
    rows: list[dict[str, object]] = []
    tickers = {
        "QQQ": 100.0,
        "SPY": 90.0,
        "SMH": 80.0,
        "TLT": 110.0,
    }
    if include_vix:
        tickers["^VIX"] = 20.0
    for index, current in enumerate(dates):
        for ticker, base in tickers.items():
            if ticker == "^VIX":
                close = base + (index % 6)
                adj_close = close
            elif ticker == "TLT":
                adj_close = base * (1.0 - (index * 0.0004))
                close = adj_close
            else:
                adj_close = base * (1.0 + (index * 0.002) + ((index % 5) * 0.0005))
                close = adj_close
            rows.append(
                {
                    "date": current.date().isoformat(),
                    "ticker": ticker,
                    "symbol": ticker,
                    "open": close,
                    "high": close,
                    "low": close,
                    "close": close,
                    "adj_close": adj_close,
                    "volume": 1000,
                    "source": "pytest_fixture",
                    "updated_at": "2026-06-29T00:00:00Z",
                    "source_symbol": ticker,
                    "canonical_symbol": ticker,
                }
            )
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def write_rates_fixture(tmp_path: Path) -> Path:
    path = tmp_path / "rates_daily.csv"
    rows: list[dict[str, object]] = []
    for index, current in enumerate(pd.bdate_range("2022-12-01", "2023-02-15")):
        rows.append(
            {
                "date": current.date().isoformat(),
                "series": "DGS10",
                "value": 3.5 + index * 0.001,
            }
        )
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def build_regenerated_artifact_fixture(tmp_path: Path) -> dict[str, Path]:
    price_path = write_price_fixture(tmp_path)
    rates_path = write_rates_fixture(tmp_path)
    output_dir = tmp_path / "regenerated"
    run_first_layer_candidate_generators_regenerate(
        candidates="baseline_plus_trend_structure,risk_appetite,volatility_regime",
        target_assets="QQQ,SPY,SMH",
        start_date=date(2023, 1, 3),
        end_date=date(2023, 1, 10),
        horizons="5d,10d,20d",
        output_dir=output_dir,
        mode="regenerated_candidate_artifacts",
        prices_path=price_path,
        rates_path=rates_path,
        marketstack_prices_path=None,
    )
    return {
        "input_dir": output_dir,
        "prices_path": price_path,
        "rates_path": rates_path,
    }


def build_regenerated_actual_path_validation_fixture(tmp_path: Path) -> dict[str, Path]:
    fixture = build_regenerated_artifact_fixture(tmp_path)
    output_dir = tmp_path / "actual_path_validation"
    run_regenerated_candidate_actual_path_validation(
        input_dir=fixture["input_dir"],
        candidates="baseline_plus_trend_structure,risk_appetite,volatility_regime",
        target_assets="QQQ,SPY,SMH",
        horizons="5d,10d,20d",
        output_dir=output_dir,
        mode="actual_path_validation",
        prices_path=fixture["prices_path"],
        rates_path=fixture["rates_path"],
        marketstack_prices_path=None,
        docs_root=tmp_path / "actual_path_docs",
    )
    return {
        **fixture,
        "validation_dir": output_dir,
        "generator_dir": fixture["input_dir"],
    }


def regenerated_context(
    tmp_path: Path,
    *,
    candidate_id: str,
    price_path: Path,
) -> CandidateGeneratorContext:
    return CandidateGeneratorContext(
        candidate_id=candidate_id,
        candidate_family=REGENERATED_CANDIDATE_FAMILY,
        target_asset="QQQ,SPY,SMH",
        start_date=date(2023, 1, 16),
        end_date=date(2023, 1, 20),
        horizon="5d,10d,20d",
        output_dir=tmp_path / candidate_id,
        mode="regenerated_candidate_artifacts",
        generated_at=datetime(2026, 6, 29, tzinfo=UTC),
        signal_spec_version="first_layer_candidate_signal_spec.v1",
        prediction_schema_version="candidate_bound_prediction_artifact.v1",
        input_snapshot_hash=f"{candidate_id}_input_hash",
        feature_snapshot_hash=f"{candidate_id}_feature_hash",
        source_paths=(price_path, Path(__file__)),
        source_hashes=(f"{candidate_id}_price_hash", f"{candidate_id}_source_hash"),
    )


def build_and_validate_bundle(generator: object, context: CandidateGeneratorContext):
    spec = generator.build_signal_spec(context)
    records = generator.generate_signal_series(context, spec)
    artifact = generator.generate_prediction_artifact(context, spec, records)
    bundle = CandidateGenerationBundle(
        context=context,
        signal_spec=spec,
        signal_records=records,
        prediction_artifact=artifact,
    )
    validation = validate_candidate_generation_bundle(
        bundle,
        task_id="TRADING-2284_TREND_RISK_VOLATILITY_EXECUTABLE_CANDIDATE_GENERATORS",
    )
    return spec, records, artifact, validation


def assert_common_candidate_contract(
    *,
    candidate_id: str,
    records: list[object],
    artifact: dict[str, object],
    validation: dict[str, object],
) -> None:
    assert validation["status"] == "PASS", validation["errors"]
    assert records
    payload = records[0].to_dict()
    assert payload["candidate_id"] == candidate_id
    assert payload["target_asset"] in {"QQQ", "SPY", "SMH"}
    assert payload["horizon"] in {"5d", "10d", "20d"}
    assert payload["source_artifact_hash"]
    assert payload["provenance"]["regeneration_mode"] == "deterministic_regeneration"
    assert -1.0 <= payload["signal_value"] <= 1.0
    assert 0.0 <= payload["signal_confidence"] <= 1.0
    assert payload["promotion_eligible"] is False
    assert payload["promotion_allowed"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["broker_action"] == "none"
    assert artifact["candidate_id"] == candidate_id
    assert artifact["artifact_role"] == "regenerated_executable_candidate_artifact"
    assert artifact["historical_executable_artifact"] is True
    assert artifact["actual_path_validation_ready"] is False
    assert artifact["promotion_eligible"] is False
    assert artifact["promotion_allowed"] is False
    assert artifact["paper_shadow_allowed"] is False
    assert artifact["production_allowed"] is False
    assert artifact["broker_action"] == "none"
