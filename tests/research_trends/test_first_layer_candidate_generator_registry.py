from __future__ import annotations

import pytest

from ai_trading_system.first_layer_candidate_generator_registry import (
    CandidateGeneratorRegistry,
    default_candidate_generator_registry,
)
from ai_trading_system.first_layer_candidate_signal_generator import CandidateGeneratorError
from ai_trading_system.framework_smoke_candidate_generator import FrameworkSmokeCandidateGenerator


def test_default_registry_lists_and_gets_framework_smoke_generator() -> None:
    registry = default_candidate_generator_registry()

    listed = registry.list_generators()
    generator = registry.get_generator("framework_smoke_candidate")

    assert generator.generator_id == "framework_smoke_candidate"
    assert listed == [
        {
            "generator_id": "framework_smoke_candidate",
            "generator_version": "framework_smoke_candidate_generator.v1",
            "candidate_family": "first_layer_executable_candidate",
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        }
    ]


def test_unknown_generator_fails_closed() -> None:
    registry = default_candidate_generator_registry()

    with pytest.raises(CandidateGeneratorError, match="unknown first-layer candidate generator"):
        registry.get_generator("risk_appetite")


def test_duplicate_generator_registration_fails_closed() -> None:
    registry = CandidateGeneratorRegistry()
    registry.register_generator(FrameworkSmokeCandidateGenerator())

    with pytest.raises(CandidateGeneratorError, match="duplicate generator_id"):
        registry.register_generator(FrameworkSmokeCandidateGenerator())
