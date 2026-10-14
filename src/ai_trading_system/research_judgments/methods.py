"""Reviewed registry of research method labels, independent of judgment content."""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_METHODS_PATH = PROJECT_ROOT / "config" / "research" / "research_judgment_methods.yaml"


class MethodSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    label: str = Field(min_length=1)
    mechanizable: Literal["full", "partial", "none"]
    screen_template: Literal["event_study", "leading_indicator", "cross_section", "none"]
    question: str = Field(min_length=1)

    @field_validator("label", "question")
    @classmethod
    def nonempty_text(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("method text must not be empty")
        return value


class MethodRegistry(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1]
    methods: tuple[MethodSpec, ...] = Field(min_length=1)

    @field_validator("schema_version", mode="before")
    @classmethod
    def exact_version(cls, value: object) -> object:
        if type(value) is not int or value != 1:
            raise ValueError("unsupported registry schema version")
        return value

    @model_validator(mode="after")
    def validate_ids(self) -> Self:
        ids = self.ids()
        if len(ids) != len(self.methods):
            raise ValueError("method ids must be unique")
        if "other" not in ids:
            raise ValueError("method registry must include other")
        return self

    def ids(self) -> set[str]:
        return {method.id for method in self.methods}


def load_methods(path: Path = DEFAULT_METHODS_PATH) -> MethodRegistry:
    return MethodRegistry.model_validate(safe_load_yaml_path(path))
