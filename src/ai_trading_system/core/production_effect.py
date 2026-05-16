from __future__ import annotations

from enum import StrEnum


class ProductionEffect(StrEnum):
    PRODUCTION = "production"
    ADVISORY = "advisory"
    NONE = "none"
    VALIDATION_ONLY = "validation-only"
    BLOCKED = "blocked"

    @classmethod
    def parse(
        cls,
        value: str | ProductionEffect | None,
        *,
        default: ProductionEffect | None = None,
    ) -> ProductionEffect:
        if isinstance(value, cls):
            return value
        normalized = "" if value is None else value.strip().lower()
        if not normalized:
            if default is not None:
                return default
            raise ValueError("production_effect is required")
        for effect in cls:
            if normalized == effect.value:
                return effect
        raise ValueError(f"unknown production_effect: {value!r}")

    @property
    def affects_production(self) -> bool:
        return self in {ProductionEffect.PRODUCTION, ProductionEffect.ADVISORY}
