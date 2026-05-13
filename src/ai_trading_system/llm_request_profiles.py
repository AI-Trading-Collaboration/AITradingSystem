from __future__ import annotations

from pathlib import Path
from typing import Literal, Self

import yaml
from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT

DEFAULT_LLM_REQUEST_PROFILES_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "llm_request_profiles.yaml"
)

OpenAIReasoningEffortConfig = Literal["none", "minimal", "low", "medium", "high", "xhigh"]
OpenAIHttpClientConfig = Literal["requests", "urllib"]


class LlmRequestProfilePolicyMetadata(BaseModel):
    version: str = Field(min_length=1)
    status: str = Field(min_length=1)
    owner: str = Field(min_length=1)
    rationale: str = Field(min_length=1)
    validation: str = Field(min_length=1)
    review_after_reports: int = Field(gt=0)


class LlmFormalAssessmentProfile(BaseModel):
    enabled: bool = False
    min_confidence: float = Field(default=0.0, ge=0, le=1)
    next_review_days: int = Field(default=1, ge=0)
    overwrite: bool = False


class LlmRequestProfile(BaseModel):
    profile_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.-]+$")
    description: str = Field(min_length=1)
    provider: Literal["openai"] = "openai"
    api_family: Literal["responses"] = "responses"
    endpoint: str = Field(min_length=1)
    model: str = Field(min_length=1)
    reasoning_effort: OpenAIReasoningEffortConfig
    timeout_seconds: float = Field(gt=0)
    http_client: OpenAIHttpClientConfig
    cache_ttl_hours: float = Field(gt=0)
    max_retries: int = Field(ge=0)
    max_candidates: int | None = Field(default=None, ge=0)
    official_policy_limit: int | None = Field(default=None, ge=0)
    formal_assessment: LlmFormalAssessmentProfile = Field(
        default_factory=LlmFormalAssessmentProfile
    )


class LlmRequestProfilesConfig(BaseModel):
    policy_metadata: LlmRequestProfilePolicyMetadata
    default_profile_id: str = Field(min_length=1)
    profiles: list[LlmRequestProfile] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_profiles(self) -> Self:
        profile_ids = [profile.profile_id for profile in self.profiles]
        duplicate_ids = sorted(
            {profile_id for profile_id in profile_ids if profile_ids.count(profile_id) > 1}
        )
        if duplicate_ids:
            raise ValueError(
                f"LLM request profile ids must be unique: {', '.join(duplicate_ids)}"
            )
        if self.default_profile_id not in set(profile_ids):
            raise ValueError("default_profile_id must match a configured profile_id")
        return self

    def get_profile(self, profile_id: str | None = None) -> LlmRequestProfile:
        requested = profile_id or self.default_profile_id
        for profile in self.profiles:
            if profile.profile_id == requested:
                return profile
        configured = ", ".join(profile.profile_id for profile in self.profiles)
        raise ValueError(
            f"unknown LLM request profile '{requested}', available: {configured}"
        )


def load_llm_request_profiles(
    path: Path | str = DEFAULT_LLM_REQUEST_PROFILES_CONFIG_PATH,
) -> LlmRequestProfilesConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as file:
        raw = yaml.safe_load(file)
    return LlmRequestProfilesConfig.model_validate(raw)
