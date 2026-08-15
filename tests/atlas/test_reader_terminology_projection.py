from __future__ import annotations

from pathlib import Path

import pytest

from ai_trading_system.atlas.reader_terminology_projection import (
    ReaderTerminologyPolicy,
    ReaderTerminologyProjectionError,
    load_reader_terminology_policy,
    project_reader_text,
)
from ai_trading_system.contracts.strategy_research_reader_terminology import (
    ReaderTermClassification,
    ReaderTermDefinition,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_reader_terminology_policy_loads_exact_reader_boundary() -> None:
    policy = load_reader_terminology_policy(repository_root=PROJECT_ROOT)
    assert policy.reader_profile.profile_id == "atlas_strategy_research_general_reader"
    assert policy.policy_id == "atlas_reader_facing_terminology_first_use_v1"
    assert policy.unknown_identifier_patterns == (
        "TASK_ID",
        "UPPER_SNAKE_IDENTIFIER",
        "GIT_OR_CONTENT_HASH",
        "REPOSITORY_PATH",
        "RUNTIME_COMPOSED_IDENTIFIER",
    )
    assert len(policy.terms) >= 40
    assert {"dq_pit", "keep_closed", "authorization_consumed_invalid"} <= {
        item.term_id for item in policy.terms
    }
    assert policy.safety["primary_research_start"] == "2021-02-22"
    assert policy.safety["investment_conclusion_generated"] is False
    assert project_reader_text(
        text="KEEP_CLOSED 与 strategy PASS",
        policy=policy,
    ) == "保持研究关闭 与 策略结论通过"


def test_reader_terminology_policy_rejects_profile_binding_drift(tmp_path: Path) -> None:
    profile_path = tmp_path / "profile.yaml"
    profile_path.write_bytes((PROJECT_ROOT / "config/atlas/reader_profile.yaml").read_bytes())
    policy_path = tmp_path / "terminology.yaml"
    policy_path.write_text(
        (PROJECT_ROOT / "config/atlas/reader_terminology.yaml")
        .read_text(encoding="utf-8")
        .replace(
            "reader_profile_id: atlas_strategy_research_general_reader",
            "reader_profile_id: wrong_profile",
        ),
        encoding="utf-8",
    )
    with pytest.raises(
        ReaderTerminologyProjectionError,
        match="READER_TERMINOLOGY_PROFILE_BINDING_INVALID",
    ):
        load_reader_terminology_policy(
            repository_root=tmp_path,
            reader_profile_path="profile.yaml",
            terminology_path="terminology.yaml",
        )


def test_reader_terminology_policy_rejects_case_insensitive_alias_ambiguity() -> None:
    base = load_reader_terminology_policy(repository_root=PROJECT_ROOT)
    conflicting = ReaderTermDefinition(
        term_id="conflicting_atlas",
        display_name_zh="冲突 Atlas 别名",
        aliases=("atlas",),
        classification=ReaderTermClassification.GLOSSARY,
        plain_definition_zh="故意构造的冲突术语。",
        why_needed_zh="证明大小写变体不能被两个术语同时认领。",
    )
    with pytest.raises(
        ReaderTerminologyProjectionError,
        match="READER_TERMINOLOGY_ALIAS_AMBIGUOUS:atlas:atlas:conflicting_atlas",
    ):
        ReaderTerminologyPolicy(
            policy_id=base.policy_id,
            policy_version=base.policy_version,
            status=base.status,
            owner=base.owner,
            reader_profile=base.reader_profile,
            reader_profile_sha256=base.reader_profile_sha256,
            terminology_policy_sha256=base.terminology_policy_sha256,
            unknown_identifier_patterns=base.unknown_identifier_patterns,
            excluded_non_reader_regions=base.excluded_non_reader_regions,
            raw_reader_replacements=base.raw_reader_replacements,
            terms=(*base.terms, conflicting),
            safety=base.safety,
        )
