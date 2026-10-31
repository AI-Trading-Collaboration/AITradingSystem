"""RISK-017: the precheck request names the registered risk events and only allows their ids.

The 2026-10-31 12:00 JST daily (as_of 2026-10-30) stopped in score_daily because the model
answered a congressional bill with two risk ids it had made up; the request never told it which
ids config/risk_events.yaml registers, and the schema allowed any string. With a risk config the
request now lists the registered events, the schema limits risk_id_candidate to their ids, and
an answer that fits none of them is an empty list, which the formal assessment skips with a
warning. Without a risk config the request is the old one. The formal assessment's
unknown-id stop (RISK-012) is unchanged.
"""

from __future__ import annotations

import copy
import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import (
    DataSourceConfig,
    DataSourceLlmPermissionConfig,
    DataSourcesConfig,
    RiskEventsConfig,
    load_risk_events,
)
from ai_trading_system.llm_precheck import (
    LLM_CLAIM_PREREVIEW_PROMPT_VERSION,
    LLM_CLAIM_PREREVIEW_REGISTERED_RISK_PROMPT_VERSION,
    OPENAI_LLM_CLAIM_RESPONSE_FORMAT,
    REGISTERED_RISK_ID_RULE,
    LlmClaimPrecheckInput,
    OpenAIJsonResponse,
    openai_llm_claim_response_format,
    run_openai_claim_precheck,
)
from ai_trading_system.official_policy_sources import OfficialPolicyCandidate
from ai_trading_system.risk_event_llm_formal import build_llm_formal_assessment_report
from ai_trading_system.risk_event_prereview import (
    run_openai_risk_event_prereview_for_official_candidates,
    write_risk_event_prereview_queue,
)

OLD_SYSTEM_TEXT = (
    "你是投资决策支持系统的证据分类器。只能抽取和分类事实断言，"
    "不得给出看多、看空、买入、卖出、加仓、减仓或仓位建议。"
    "所有输出必须保持 pending_review，不能写成正式评分或仓位闸门输入。"
)
AS_OF = date(2026, 5, 4)
GENERATED_AT = datetime(2026, 5, 4, tzinfo=UTC)
OFFICIAL_SOURCE_ID = "official_congress_bills"


class _Recorder:
    def __init__(self, *responses: OpenAIJsonResponse) -> None:
        self.payloads: list[dict[str, Any]] = []
        self._responses = list(responses)

    def __call__(
        self,
        _url: str,
        headers: Mapping[str, str],
        payload: Mapping[str, Any],
        _timeout_seconds: float,
    ) -> OpenAIJsonResponse:
        self.payloads.append(copy.deepcopy(dict(payload)))
        if len(self._responses) > 1:
            return self._responses.pop(0)
        return self._responses[0]


def _risk_id_items(response_format: Mapping[str, Any]) -> Any:
    claim = response_format["schema"]["properties"]["claims"]["items"]
    candidate = claim["properties"]["risk_event_candidate"]
    return candidate["properties"]["risk_id_candidate"]["items"]


def _registered_ids(risk_events: RiskEventsConfig) -> list[str]:
    return sorted(rule.event_id for rule in risk_events.event_rules)


def test_registered_rule_text_names_the_list_and_the_empty_answer() -> None:
    assert LLM_CLAIM_PREREVIEW_PROMPT_VERSION == "llm_claim_precheck_v1"
    assert LLM_CLAIM_PREREVIEW_REGISTERED_RISK_PROMPT_VERSION == "llm_claim_precheck_v2"
    assert REGISTERED_RISK_ID_RULE == (
        "risk_event_candidate.risk_id_candidate 只能填写 registered_risk_events 里的 event_id；"
        "没有符合的就输出空数组，不要自造 ID。"
    )


def test_response_format_without_ids_is_the_old_constant() -> None:
    assert openai_llm_claim_response_format() is OPENAI_LLM_CLAIM_RESPONSE_FORMAT
    assert openai_llm_claim_response_format(None) is OPENAI_LLM_CLAIM_RESPONSE_FORMAT
    assert _risk_id_items(OPENAI_LLM_CLAIM_RESPONSE_FORMAT) == {"type": "string"}


def test_response_format_with_ids_limits_only_risk_id_candidate() -> None:
    before = copy.deepcopy(OPENAI_LLM_CLAIM_RESPONSE_FORMAT)
    limited = openai_llm_claim_response_format(["taiwan_x", "cloud_x", "taiwan_x"])

    assert limited is not OPENAI_LLM_CLAIM_RESPONSE_FORMAT
    assert limited["name"] == LLM_CLAIM_PREREVIEW_REGISTERED_RISK_PROMPT_VERSION
    assert _risk_id_items(limited) == {"type": "string", "enum": ["cloud_x", "taiwan_x"]}
    rest = copy.deepcopy(limited)
    rest["name"] = before["name"]
    claim = rest["schema"]["properties"]["claims"]["items"]
    candidate = claim["properties"]["risk_event_candidate"]
    candidate["properties"]["risk_id_candidate"]["items"] = {"type": "string"}
    assert rest == before
    assert OPENAI_LLM_CLAIM_RESPONSE_FORMAT == before


def test_response_format_refuses_an_empty_id_list() -> None:
    try:
        openai_llm_claim_response_format([])
    except ValueError as exc:
        assert "registered_risk_ids" in str(exc)
    else:
        raise AssertionError("an empty id list must raise ValueError")


def test_request_lists_registered_events_sorted_and_limits_the_schema(tmp_path: Path) -> None:
    risk_events = load_risk_events()
    shuffled = risk_events.model_copy(
        update={"event_rules": list(reversed(risk_events.event_rules))}
    )
    recorder = _Recorder(_openai_response(["ai_chip_export_control_upgrade"]))

    report = run_openai_claim_precheck(
        _packet(),
        api_key="TEST_ONLY_NON_PROVIDER_SECRET",
        data_sources=_data_sources(),
        risk_events=shuffled,
        input_path=tmp_path / "input.yaml",
        http_post_json=recorder,
        generated_at=GENERATED_AT,
    )

    assert report.error_count == 0
    assert len(recorder.payloads) == 1
    payload = recorder.payloads[0]
    system_text = payload["input"][0]["content"]
    user = json.loads(payload["input"][1]["content"])
    assert system_text == OLD_SYSTEM_TEXT + REGISTERED_RISK_ID_RULE
    expected_events = [
        {
            "event_id": rule.event_id,
            "name": rule.name,
            "level": rule.level,
            "description": rule.description,
        }
        for rule in sorted(risk_events.event_rules, key=lambda item: item.event_id)
    ]
    assert user["registered_risk_events"] == expected_events
    assert payload["text"]["format"] == openai_llm_claim_response_format(
        _registered_ids(risk_events)
    )
    assert _risk_id_items(payload["text"]["format"]) == {
        "type": "string",
        "enum": _registered_ids(risk_events),
    }
    assert report.records[0].prompt_version == LLM_CLAIM_PREREVIEW_REGISTERED_RISK_PROMPT_VERSION


def test_request_without_risk_config_is_the_old_request(tmp_path: Path) -> None:
    recorder = _Recorder(_openai_response(["ai_chip_export_control_upgrade"]))

    report = run_openai_claim_precheck(
        _packet(),
        api_key="TEST_ONLY_NON_PROVIDER_SECRET",
        data_sources=_data_sources(),
        input_path=tmp_path / "input.yaml",
        http_post_json=recorder,
        generated_at=GENERATED_AT,
    )

    assert report.error_count == 0
    payload = recorder.payloads[0]
    assert payload["input"][0]["content"] == OLD_SYSTEM_TEXT
    assert "registered_risk_events" not in json.loads(payload["input"][1]["content"])
    assert payload["text"]["format"] is not None
    assert payload["text"]["format"] == OPENAI_LLM_CLAIM_RESPONSE_FORMAT
    assert report.records[0].prompt_version == LLM_CLAIM_PREREVIEW_PROMPT_VERSION


def test_limited_and_old_requests_have_different_checksums_and_cache_keys(
    tmp_path: Path,
) -> None:
    cache_dir = tmp_path / "cache"
    recorder = _Recorder(_openai_response(["ai_chip_export_control_upgrade"]))
    kwargs: dict[str, Any] = {
        "api_key": "TEST_ONLY_NON_PROVIDER_SECRET",
        "data_sources": _data_sources(cache_allowed=True),
        "input_path": tmp_path / "input.yaml",
        "http_post_json": recorder,
        "generated_at": GENERATED_AT,
        "openai_cache_dir": cache_dir,
    }

    old = run_openai_claim_precheck(_packet(), **kwargs)
    limited = run_openai_claim_precheck(_packet(), risk_events=load_risk_events(), **kwargs)

    assert old.error_count == 0
    assert limited.error_count == 0
    assert len(recorder.payloads) == 2
    assert limited.records[0].cache_status != "HIT"
    assert old.records[0].cache_key != limited.records[0].cache_key
    assert old.records[0].input_checksum_sha256 != limited.records[0].input_checksum_sha256


def test_risk_config_without_rules_stops_before_any_request(tmp_path: Path) -> None:
    recorder = _Recorder(_openai_response([]))

    report = run_openai_claim_precheck(
        _packet(),
        api_key="TEST_ONLY_NON_PROVIDER_SECRET",
        data_sources=_data_sources(),
        risk_events=load_risk_events().model_copy(update={"event_rules": []}),
        input_path=tmp_path / "input.yaml",
        http_post_json=recorder,
        generated_at=GENERATED_AT,
    )

    assert recorder.payloads == []
    assert report.status == "FAIL"
    assert report.records == ()
    assert [issue.code for issue in report.issues] == ["llm_precheck_registered_risk_ids_empty"]


def test_daily_official_prereview_sends_the_registered_ids(tmp_path: Path) -> None:
    risk_events = load_risk_events()
    recorder = _Recorder(_openai_response(["ai_chip_export_control_upgrade"]))

    report = run_openai_risk_event_prereview_for_official_candidates(
        (_official_candidate(tmp_path, "a"),),
        api_key="TEST_ONLY_NON_PROVIDER_SECRET",
        data_sources=_data_sources(),
        risk_events=risk_events,
        input_path=tmp_path / "official_policy_source_candidates_2026-05-04.csv",
        as_of=AS_OF,
        http_post_json=recorder,
        generated_at=GENERATED_AT,
    )

    assert report.error_count == 0
    payload = recorder.payloads[0]
    user = json.loads(payload["input"][1]["content"])
    assert [item["event_id"] for item in user["registered_risk_events"]] == _registered_ids(
        risk_events
    )
    assert _risk_id_items(payload["text"]["format"])["enum"] == _registered_ids(risk_events)
    assert report.records[0].prompt_version == LLM_CLAIM_PREREVIEW_REGISTERED_RISK_PROMPT_VERSION


def test_an_answer_that_fits_no_registered_event_no_longer_stops_the_batch(
    tmp_path: Path,
) -> None:
    # The 10-30 shape: one candidate fits no registered event, another fits one.
    risk_events = load_risk_events()
    recorder = _Recorder(
        _openai_response([], claim_id="claim_001"),
        _openai_response(["ai_chip_export_control_upgrade"], claim_id="claim_001"),
    )

    prereview = run_openai_risk_event_prereview_for_official_candidates(
        (_official_candidate(tmp_path, "a"), _official_candidate(tmp_path, "b")),
        api_key="TEST_ONLY_NON_PROVIDER_SECRET",
        data_sources=_data_sources(),
        risk_events=risk_events,
        input_path=tmp_path / "official_policy_source_candidates_2026-05-04.csv",
        as_of=AS_OF,
        http_post_json=recorder,
        generated_at=GENERATED_AT,
    )
    assert prereview.error_count == 0
    assert sorted(len(record.matched_risk_ids) for record in prereview.records) == [0, 1]
    queue_path = write_risk_event_prereview_queue(prereview, tmp_path / "queue.json")

    formal = build_llm_formal_assessment_report(
        queue_path,
        as_of=AS_OF,
        risk_events=risk_events,
        generated_at=GENERATED_AT,
    )

    assert formal.error_count == 0
    assert formal.status == "PASS_WITH_WARNINGS"
    assert [occurrence.event_id for occurrence in formal.occurrences] == [
        "ai_chip_export_control_upgrade"
    ]
    assert formal.attestation is not None
    assert "llm_formal_assessment_record_without_risk_id" in {issue.code for issue in formal.issues}
    assert "llm_formal_assessment_unknown_risk_id" not in {issue.code for issue in formal.issues}


def test_an_unknown_only_id_in_the_queue_still_stops_the_batch(tmp_path: Path) -> None:
    # RISK-012 stays as the backstop, for example for a queue made before this change.
    risk_events = load_risk_events()
    recorder = _Recorder(
        _openai_response(["ai_chip_export_control_upgrade"], claim_id="claim_001"),
    )
    prereview = run_openai_risk_event_prereview_for_official_candidates(
        (_official_candidate(tmp_path, "a"),),
        api_key="TEST_ONLY_NON_PROVIDER_SECRET",
        data_sources=_data_sources(),
        risk_events=risk_events,
        input_path=tmp_path / "official_policy_source_candidates_2026-05-04.csv",
        as_of=AS_OF,
        http_post_json=recorder,
        generated_at=GENERATED_AT,
    )
    queue_path = write_risk_event_prereview_queue(prereview, tmp_path / "queue.json")
    queue = json.loads(queue_path.read_text(encoding="utf-8"))
    queue["records"][0]["matched_risk_ids"] = [
        "ai_market_stress_bailout_restriction_policy",
        "ai_financial_stability_policy_signal",
    ]
    queue_path.write_text(json.dumps(queue, ensure_ascii=False), encoding="utf-8")

    formal = build_llm_formal_assessment_report(
        queue_path,
        as_of=AS_OF,
        risk_events=risk_events,
        generated_at=GENERATED_AT,
    )

    assert formal.status == "FAIL"
    assert formal.occurrences == ()
    assert formal.attestation is None
    assert "llm_formal_assessment_unknown_risk_id" in {issue.code for issue in formal.issues}


def _packet() -> LlmClaimPrecheckInput:
    return LlmClaimPrecheckInput.model_validate(
        {
            "precheck_id": "precheck:claim:2026-05-04",
            "source_id": OFFICIAL_SOURCE_ID,
            "source_url": "https://www.congress.gov/bill/example",
            "source_name": "Congress.gov",
            "source_title": "Example resolution",
            "published_at": AS_OF,
            "captured_at": AS_OF,
            "content_text": "官方政策/地缘候选元数据。",
            "content_sent_level": "metadata_only",
        }
    )


def _data_sources(*, cache_allowed: bool = False) -> DataSourcesConfig:
    return DataSourcesConfig(
        sources=[
            DataSourceConfig(
                source_id=OFFICIAL_SOURCE_ID,
                provider="Congress.gov",
                source_type="primary_source",
                status="active",
                domains=["news_events"],
                endpoint="https://example.test",
                adapter="test",
                cadence="event_driven",
                audit_fields=[
                    "provider",
                    "endpoint",
                    "request_parameters",
                    "downloaded_at",
                    "row_count",
                    "checksum",
                ],
                validation_checks=["schema"],
                limitations=["test source"],
                llm_permission=DataSourceLlmPermissionConfig(
                    license_scope="test_scope",
                    personal_use_only=True,
                    external_llm_allowed=True,
                    cache_allowed=cache_allowed,
                    redistribution_allowed=False,
                    max_content_sent_level="metadata_only",
                    approval_ref="owner_test_approval",
                    reviewed_at=AS_OF,
                ),
            )
        ]
    )


def _official_candidate(tmp_path: Path, key: str) -> OfficialPolicyCandidate:
    return OfficialPolicyCandidate(
        candidate_id=f"official:{OFFICIAL_SOURCE_ID}:{key}",
        as_of=AS_OF,
        source_id=OFFICIAL_SOURCE_ID,
        provider="Congress.gov",
        source_type="primary_source",
        source_name="Congress.gov",
        source_url=f"https://www.congress.gov/bill/{key}",
        source_title=f"Example resolution {key}",
        published_at=AS_OF,
        captured_at=AS_OF,
        matched_topics=("ai_policy",),
        matched_risk_ids=("ai_chip_export_control_upgrade",),
        affected_tickers=("NVDA",),
        affected_nodes=("export_controls",),
        evidence_grade_floor="A",
        review_status="pending_review",
        review_questions=("是否构成需要人工复核的政策风险？",),
        raw_payload_path=tmp_path / "raw.json",
        raw_payload_sha256="c" * 64,
        row_count=1,
    )


def _openai_response(
    risk_ids: list[str],
    *,
    claim_id: str = "claim:policy:2026-05-04",
) -> OpenAIJsonResponse:
    output = {
        "overall_summary_zh": "该来源包含一条待复核政策线索。",
        "prohibited_actions_ack": True,
        "claims": [
            {
                "claim_id": claim_id,
                "claim_text_zh": "决议表达了对 AI 市场压力的关切。",
                "source_span_ref": "metadata:title",
                "affected_tickers": ["NVDA"],
                "affected_nodes": ["export_controls"],
                "claim_type": "risk_event",
                "novelty": "new",
                "impact_horizon": "medium_term",
                "evidence_grade_suggestion": "B",
                "confidence": 0.6,
                "conflicts_or_uncertainties": [],
                "required_review_questions": ["是否为正式立法？"],
                "risk_event_candidate": {
                    "risk_id_candidate": risk_ids,
                    "status_candidate": "watch",
                    "level_candidate": "L1",
                    "severity_candidate": "low",
                    "probability_candidate": "low",
                    "scope_candidate": "market",
                    "time_sensitivity_candidate": "medium_term",
                    "action_class_candidate": "manual_review",
                    "missing_confirmations": [],
                    "review_questions": [],
                },
                "thesis_signal_match": [],
                "manual_review_status": "pending_review",
                "prohibited_actions_ack": True,
            }
        ],
    }
    return OpenAIJsonResponse(
        status_code=200,
        headers={"x-request-id": "req_risk017"},
        body={
            "id": "resp_risk017",
            "output": [
                {
                    "type": "message",
                    "content": [
                        {"type": "output_text", "text": json.dumps(output, ensure_ascii=False)}
                    ],
                }
            ],
        },
    )
