from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ai_trading_system.first_layer_candidate_signal_generator import (
    CandidateGeneratorError,
    FirstLayerCandidateSignalGenerator,
    generator_operation_safety_fields,
)


@dataclass
class CandidateGeneratorRegistry:
    _generators: dict[str, FirstLayerCandidateSignalGenerator] = field(default_factory=dict)

    def register_generator(self, generator: FirstLayerCandidateSignalGenerator) -> None:
        generator_id = str(generator.generator_id).strip()
        if not generator_id:
            raise CandidateGeneratorError("generator_id must be non-empty")
        if generator_id in self._generators:
            raise CandidateGeneratorError(f"duplicate generator_id: {generator_id}")
        self._generators[generator_id] = generator

    def get_generator(self, generator_id: str) -> FirstLayerCandidateSignalGenerator:
        key = str(generator_id).strip()
        if key not in self._generators:
            available = ", ".join(sorted(self._generators)) or "<none>"
            raise CandidateGeneratorError(
                f"unknown first-layer candidate generator '{key}', available: {available}"
            )
        return self._generators[key]

    def list_generators(self) -> list[dict[str, Any]]:
        return [
            {
                "generator_id": generator.generator_id,
                "generator_version": generator.generator_version,
                "candidate_family": generator.candidate_family,
                **generator_operation_safety_fields(),
            }
            for generator in sorted(
                self._generators.values(),
                key=lambda item: item.generator_id,
            )
        ]


def default_candidate_generator_registry() -> CandidateGeneratorRegistry:
    from ai_trading_system.baseline_plus_trend_structure_generator import (
        BaselinePlusTrendStructureGenerator,
    )
    from ai_trading_system.framework_smoke_candidate_generator import (
        FrameworkSmokeCandidateGenerator,
    )
    from ai_trading_system.risk_appetite_candidate_generator import (
        RiskAppetiteCandidateGenerator,
    )
    from ai_trading_system.volatility_regime_candidate_generator import (
        VolatilityRegimeCandidateGenerator,
    )

    registry = CandidateGeneratorRegistry()
    registry.register_generator(FrameworkSmokeCandidateGenerator())
    registry.register_generator(BaselinePlusTrendStructureGenerator())
    registry.register_generator(RiskAppetiteCandidateGenerator())
    registry.register_generator(VolatilityRegimeCandidateGenerator())
    return registry


_DEFAULT_REGISTRY = default_candidate_generator_registry()


def register_generator(generator: FirstLayerCandidateSignalGenerator) -> None:
    _DEFAULT_REGISTRY.register_generator(generator)


def get_generator(generator_id: str) -> FirstLayerCandidateSignalGenerator:
    return _DEFAULT_REGISTRY.get_generator(generator_id)


def list_generators() -> list[dict[str, Any]]:
    return _DEFAULT_REGISTRY.list_generators()
