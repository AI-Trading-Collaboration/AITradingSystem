from __future__ import annotations

from pathlib import Path

from regenerated_candidate_test_helpers import (
    assert_common_candidate_contract,
    build_and_validate_bundle,
    regenerated_context,
    write_price_fixture,
)

from ai_trading_system.baseline_plus_trend_structure_generator import (
    BASELINE_PLUS_TREND_STRUCTURE_CANDIDATE_ID,
    BaselinePlusTrendStructureGenerator,
)


def test_baseline_plus_trend_structure_generator_contract(tmp_path: Path) -> None:
    price_path = write_price_fixture(tmp_path)
    generator = BaselinePlusTrendStructureGenerator()
    context = regenerated_context(
        tmp_path,
        candidate_id=BASELINE_PLUS_TREND_STRUCTURE_CANDIDATE_ID,
        price_path=price_path,
    )

    spec, records, artifact, validation = build_and_validate_bundle(generator, context)

    assert spec.candidate_id == "baseline_plus_trend_structure"
    assert set(spec.supported_horizons) == {"5d", "10d", "20d"}
    assert set(spec.output_signal_names) == {
        "trend_structure_score",
        "trend_confirmation_score",
        "trend_weakening_score",
        "relative_strength_score",
    }
    assert {record.target_asset for record in records} == {"QQQ", "SPY", "SMH"}
    assert {record.horizon for record in records} == {"5d", "10d", "20d"}
    assert {record.signal_name for record in records} == set(spec.output_signal_names)
    assert_common_candidate_contract(
        candidate_id=BASELINE_PLUS_TREND_STRUCTURE_CANDIDATE_ID,
        records=records,
        artifact=artifact,
        validation=validation,
    )
