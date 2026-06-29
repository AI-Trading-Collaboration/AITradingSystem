from __future__ import annotations

from pathlib import Path

from regenerated_candidate_test_helpers import (
    assert_common_candidate_contract,
    build_and_validate_bundle,
    regenerated_context,
    write_price_fixture,
)

from ai_trading_system.volatility_regime_candidate_generator import (
    VOLATILITY_REGIME_CANDIDATE_ID,
    VolatilityRegimeCandidateGenerator,
)


def test_volatility_regime_generator_contract_with_realized_vol_proxy(
    tmp_path: Path,
) -> None:
    price_path = write_price_fixture(tmp_path, include_vix=False)
    generator = VolatilityRegimeCandidateGenerator()
    context = regenerated_context(
        tmp_path,
        candidate_id=VOLATILITY_REGIME_CANDIDATE_ID,
        price_path=price_path,
    )

    spec, records, artifact, validation = build_and_validate_bundle(generator, context)

    assert spec.candidate_id == "volatility_regime"
    assert spec.pit_policy == "pit_approximation"
    assert set(spec.output_signal_names) == {
        "volatility_regime_score",
        "volatility_expansion_score",
        "stress_regime_score",
        "volatility_compression_score",
    }
    first = records[0].to_dict()
    assert first["provenance"]["vix_available"] is False
    assert first["provenance"]["volatility_proxy_mode"] == "realized_volatility_only"
    assert first["provenance"]["proxy_input_used"] is True
    assert_common_candidate_contract(
        candidate_id=VOLATILITY_REGIME_CANDIDATE_ID,
        records=records,
        artifact=artifact,
        validation=validation,
    )
