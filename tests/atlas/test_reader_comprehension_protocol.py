from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from ai_trading_system.atlas.reader_comprehension_protocol import (
    PENDING_OWNER_POLICY,
    ReaderComprehensionProtocolError,
    load_reader_comprehension_protocol,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
POLICY_PATH = PROJECT_ROOT / "config/atlas/reader_comprehension_protocol.yaml"


def test_reader_comprehension_protocol_keeps_owner_policy_pending() -> None:
    protocol = load_reader_comprehension_protocol(repository_root=PROJECT_ROOT)

    assert protocol.status == PENDING_OWNER_POLICY
    assert set(protocol.owner_policy_slots.values()) == {PENDING_OWNER_POLICY}
    assert protocol.recording["two_independent_reviewers_required"] is True
    assert protocol.recording["identity_change_requires_new_round"] is True
    assert protocol.safety["participant_recruitment_authorized"] is False
    assert protocol.safety["pilot_execution_authorized"] is False
    assert protocol.safety["automated_pass_allowed"] is False


def test_reader_comprehension_protocol_covers_reader_questions_dates_and_change() -> None:
    protocol = load_reader_comprehension_protocol(repository_root=PROJECT_ROOT)

    assert protocol.scenarios == (
        "CURRENT_RESEARCH_MAINLINE",
        "LARGEST_CURRENT_BLOCKER",
        "ENGINEERING_VS_RESEARCH_EVIDENCE",
        "PROHIBITED_INFERENCES",
        "NEXT_OWNER_AND_ACTION",
        "INVESTMENT_ORDER_ENGINE_AUTHORITY",
        "DATE_AND_FRESHNESS",
        "SNAPSHOT_CHANGE",
    )


def test_reader_comprehension_protocol_rejects_unreviewed_threshold(tmp_path: Path) -> None:
    payload = yaml.safe_load(POLICY_PATH.read_text(encoding="utf-8"))
    payload["owner_policy_slots"]["pass_threshold"] = "80_percent"
    candidate = tmp_path / "protocol.yaml"
    candidate.write_text(yaml.safe_dump(payload, allow_unicode=True), encoding="utf-8")

    with pytest.raises(ReaderComprehensionProtocolError, match="OWNER_POLICY_PREEMPTED"):
        load_reader_comprehension_protocol(
            repository_root=tmp_path,
            protocol_path="protocol.yaml",
        )


def test_reader_comprehension_protocol_rejects_automated_pass(tmp_path: Path) -> None:
    payload = yaml.safe_load(POLICY_PATH.read_text(encoding="utf-8"))
    payload["safety"]["automated_pass_allowed"] = True
    candidate = tmp_path / "protocol.yaml"
    candidate.write_text(yaml.safe_dump(payload, allow_unicode=True), encoding="utf-8")

    with pytest.raises(ReaderComprehensionProtocolError, match="SAFETY_INVALID"):
        load_reader_comprehension_protocol(
            repository_root=tmp_path,
            protocol_path="protocol.yaml",
        )
