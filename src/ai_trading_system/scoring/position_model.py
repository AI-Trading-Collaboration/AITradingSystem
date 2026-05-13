from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass


@dataclass(frozen=True)
class ModuleScore:
    name: str
    score: float
    weight: float
    reason: str


@dataclass(frozen=True)
class PositionBand:
    min_position: float
    max_position: float
    label: str


@dataclass(frozen=True)
class PositionBandRule:
    min_score: float
    min_position: float
    max_position: float
    label: str


@dataclass(frozen=True)
class PositionGate:
    gate_id: str
    label: str
    source: str
    max_position: float
    triggered: bool
    reason: str
    gate_class: str = "hard_cap"
    target_effect: str = "max_position_cap"
    execution_effect: str = "final_position_limit"


@dataclass(frozen=True)
class PositionRecommendation:
    total_score: float
    model_risk_asset_ai_band: PositionBand
    risk_asset_ai_band: PositionBand
    total_asset_ai_band: PositionBand
    total_risk_asset_band: PositionBand
    label: str
    components: tuple[ModuleScore, ...]
    position_gates: tuple[PositionGate, ...]

    @property
    def min_position(self) -> float:
        return self.risk_asset_ai_band.min_position

    @property
    def max_position(self) -> float:
        return self.risk_asset_ai_band.max_position

    @property
    def triggered_position_gates(self) -> tuple[PositionGate, ...]:
        return tuple(gate for gate in self.position_gates if gate.triggered)


class WeightedScoreModel:
    def __init__(
        self,
        position_bands: Iterable[PositionBandRule],
    ) -> None:
        self.position_bands = tuple(position_bands)
        self._validate_position_bands(self.position_bands)

    def recommend(
        self,
        components: list[ModuleScore],
        total_risk_asset_min: float = 1.0,
        total_risk_asset_max: float = 1.0,
        position_gates: tuple[PositionGate, ...] = (),
    ) -> PositionRecommendation:
        if not components:
            raise ValueError("components must not be empty")
        self._validate_position_range(total_risk_asset_min, total_risk_asset_max)

        total_weight = sum(component.weight for component in components)
        if total_weight <= 0:
            raise ValueError("total weight must be positive")

        for component in components:
            self._validate_component(component)

        total_score = sum(
            component.score * component.weight for component in components
        ) / total_weight
        model_risk_asset_ai_band = self._position_band(total_score)
        all_position_gates = (
            PositionGate(
                gate_id="score_model",
                label="评分模型仓位",
                source="weighted_score_model",
                max_position=model_risk_asset_ai_band.max_position,
                triggered=True,
                reason=(
                    "综合评分映射出的 AI 仓位区间上限："
                    f"{model_risk_asset_ai_band.max_position:.0%}。"
                ),
                gate_class="score_mapping",
                target_effect="raw_position_mapping",
                execution_effect="base_signal_to_raw_position",
            ),
            *position_gates,
        )
        for gate in all_position_gates:
            self._validate_position_range(0.0, gate.max_position)

        final_max_position = min(gate.max_position for gate in all_position_gates)
        risk_asset_ai_band = PositionBand(
            min(model_risk_asset_ai_band.min_position, final_max_position),
            final_max_position,
            self._final_label(model_risk_asset_ai_band, final_max_position),
        )
        total_risk_asset_band = PositionBand(
            total_risk_asset_min,
            total_risk_asset_max,
            "总风险资产预算",
        )
        total_asset_ai_band = PositionBand(
            risk_asset_ai_band.min_position * total_risk_asset_min,
            risk_asset_ai_band.max_position * total_risk_asset_max,
            risk_asset_ai_band.label,
        )
        return PositionRecommendation(
            total_score=total_score,
            model_risk_asset_ai_band=model_risk_asset_ai_band,
            risk_asset_ai_band=risk_asset_ai_band,
            total_asset_ai_band=total_asset_ai_band,
            total_risk_asset_band=total_risk_asset_band,
            label=risk_asset_ai_band.label,
            components=tuple(components),
            position_gates=all_position_gates,
        )

    @staticmethod
    def _validate_component(component: ModuleScore) -> None:
        if not 0 <= component.score <= 100:
            raise ValueError(f"{component.name} score must be between 0 and 100")
        if component.weight <= 0:
            raise ValueError(f"{component.name} weight must be positive")

    @staticmethod
    def _validate_position_range(min_position: float, max_position: float) -> None:
        if not 0 <= min_position <= max_position <= 1:
            raise ValueError("position range must satisfy 0 <= min <= max <= 1")

    @classmethod
    def _validate_position_bands(cls, position_bands: tuple[PositionBandRule, ...]) -> None:
        if not position_bands:
            raise ValueError("position_bands must not be empty")
        previous_min_score = 101.0
        for band in position_bands:
            if not 0 <= band.min_score <= 100:
                raise ValueError("position band score floor must be between 0 and 100")
            if band.min_score >= previous_min_score:
                raise ValueError(
                    "position bands must be sorted by descending min_score"
                )
            cls._validate_position_range(band.min_position, band.max_position)
            previous_min_score = band.min_score
        if abs(position_bands[-1].min_score) > 1e-9:
            raise ValueError("position bands must include a final floor at 0")

    def _position_band(self, score: float) -> PositionBand:
        for band in self.position_bands:
            if score >= band.min_score:
                return PositionBand(
                    band.min_position,
                    band.max_position,
                    band.label,
                )
        raise ValueError("position bands must include a floor for all scores")

    @staticmethod
    def _final_label(model_band: PositionBand, final_max_position: float) -> str:
        if final_max_position >= model_band.max_position:
            return model_band.label
        return f"{model_band.label}/仓位受限"
