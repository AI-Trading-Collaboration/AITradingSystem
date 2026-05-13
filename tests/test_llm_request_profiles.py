from __future__ import annotations

import pytest

from ai_trading_system.llm_request_profiles import (
    DEFAULT_LLM_REQUEST_PROFILES_CONFIG_PATH,
    LlmRequestProfilesConfig,
    load_llm_request_profiles,
)


def test_default_llm_request_profiles_load() -> None:
    config = load_llm_request_profiles(DEFAULT_LLM_REQUEST_PROFILES_CONFIG_PATH)

    assert config.policy_metadata.version == "llm_request_profiles_v1_cost_pilot_2026-05-13"
    assert config.default_profile_id == "llm_claim_prereview"
    assert {
        profile.profile_id for profile in config.profiles
    } >= {
        "llm_claim_prereview",
        "risk_event_single_prereview",
        "risk_event_triaged_official_candidates",
        "risk_event_daily_official_precheck",
    }


def test_daily_llm_request_profile_carries_candidate_and_formal_defaults() -> None:
    profile = load_llm_request_profiles().get_profile("risk_event_daily_official_precheck")

    assert profile.model == "gpt-5.5"
    assert profile.reasoning_effort == "medium"
    assert profile.timeout_seconds == 120
    assert profile.http_client == "requests"
    assert profile.cache_ttl_hours == 24
    assert profile.max_retries == 2
    assert profile.max_candidates == 10
    assert profile.official_policy_limit == 30
    assert profile.formal_assessment.enabled is True
    assert profile.formal_assessment.next_review_days == 1


def test_llm_request_profiles_reject_duplicate_ids() -> None:
    raw = {
        "policy_metadata": {
            "version": "test",
            "status": "pilot",
            "owner": "system",
            "rationale": "test",
            "validation": "test",
            "review_after_reports": 1,
        },
        "default_profile_id": "duplicate",
        "profiles": [
            {
                "profile_id": "duplicate",
                "description": "one",
                "endpoint": "https://api.openai.com/v1/responses",
                "model": "gpt-5.5",
                "reasoning_effort": "high",
                "timeout_seconds": 120,
                "http_client": "requests",
                "cache_ttl_hours": 24,
                "max_retries": 2,
            },
            {
                "profile_id": "duplicate",
                "description": "two",
                "endpoint": "https://api.openai.com/v1/responses",
                "model": "gpt-5.5",
                "reasoning_effort": "high",
                "timeout_seconds": 120,
                "http_client": "requests",
                "cache_ttl_hours": 24,
                "max_retries": 2,
            },
        ],
    }

    with pytest.raises(ValueError, match="must be unique"):
        LlmRequestProfilesConfig.model_validate(raw)
