from __future__ import annotations

from pathlib import Path

from regenerated_candidate_test_helpers import (
    assert_common_candidate_contract,
    build_and_validate_bundle,
    regenerated_context,
    write_price_fixture,
)

from ai_trading_system.risk_appetite_candidate_generator import (
    RISK_APPETITE_CANDIDATE_ID,
    RiskAppetiteCandidateGenerator,
)


def test_risk_appetite_generator_contract_and_missing_optional_inputs(
    tmp_path: Path,
) -> None:
    price_path = write_price_fixture(tmp_path)
    generator = RiskAppetiteCandidateGenerator()
    context = regenerated_context(
        tmp_path,
        candidate_id=RISK_APPETITE_CANDIDATE_ID,
        price_path=price_path,
    )

    spec, records, artifact, validation = build_and_validate_bundle(generator, context)

    assert spec.candidate_id == "risk_appetite"
    assert set(spec.output_signal_names) == {
        "risk_appetite_score",
        "risk_on_confirmation_score",
        "risk_off_pressure_score",
        "semiconductor_risk_appetite_score",
    }
    first = records[0].to_dict()
    assert set(first["provenance"]["missing_inputs"]) == {"GLD", "UUP"}
    assert first["provenance"]["proxy_input_used"] is True
    assert "standalone rebalance trigger" in first["provenance"]["proxy_limitations"][0]
    assert_common_candidate_contract(
        candidate_id=RISK_APPETITE_CANDIDATE_ID,
        records=records,
        artifact=artifact,
        validation=validation,
    )
