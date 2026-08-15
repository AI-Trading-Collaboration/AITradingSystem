from __future__ import annotations

import copy
import json
import subprocess
from hashlib import sha1, sha256
from pathlib import Path

import pytest
import yaml

from ai_trading_system.atlas.historical_source_adapters import (
    BaselinePayload,
    BranchDecisionPayload,
    ComponentAttributionPayload,
    HistoricalSourceAdapterError,
    HistoricalSourceRole,
    MonthlyReviewPayload,
    ProgramSnapshotPayload,
    build_historical_source_adapter_bundle,
    build_historical_source_adapter_bundle_from_payloads,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
APPROVED_COMMIT = "b385f4140b54d936c57f889a55c6f5ba99d074f9"
ADAPTER_REGISTRY_PATH = PROJECT_ROOT / "config" / "atlas" / "historical_source_adapters.yaml"
SOURCE_REGISTRY_PATH = PROJECT_ROOT / "config" / "atlas" / "source_registry.yaml"
EXPECTED_SOURCE_PATHS = {
    "docs/research/b0_static_strategic_baseline_result.json",
    "docs/research/b1_b4_component_result_attribution.json",
    "docs/research/final_branch_decision_snapshot.json",
    "docs/research/monthly_research_program_review.json",
    "docs/research/weight_research_program_v1_snapshot.json",
}


def _bundle():
    return build_historical_source_adapter_bundle(
        repository_root=PROJECT_ROOT,
        exact_commit=APPROVED_COMMIT,
    )


def _git_bytes(path: str) -> bytes:
    result = subprocess.run(
        ["git", "show", f"{APPROVED_COMMIT}:{path}"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
    )
    return result.stdout


def _git_blob(payload: bytes) -> str:
    header = f"blob {len(payload)}\0".encode()
    return sha1(header + payload).hexdigest()


def _pure_inputs():
    adapter_registry = yaml.safe_load(ADAPTER_REGISTRY_PATH.read_text(encoding="utf-8"))
    source_registry = yaml.safe_load(SOURCE_REGISTRY_PATH.read_text(encoding="utf-8"))
    assert isinstance(adapter_registry, dict)
    assert isinstance(source_registry, dict)
    paths = [item["source_path"] for item in adapter_registry["adapters"]]
    source_payloads = {path: _git_bytes(path) for path in paths}
    return (
        adapter_registry,
        source_registry,
        source_payloads,
        {path: _git_blob(payload) for path, payload in source_payloads.items()},
    )


def _replace_approved_payload(
    *,
    adapter_registry: dict,
    source_payloads: dict[str, bytes],
    source_blob_sha1s: dict[str, str],
    path: str,
    payload: dict,
) -> None:
    payload_bytes = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    replacement_blob = "a" * 40
    entry = next(item for item in adapter_registry["adapters"] if item["source_path"] == path)
    entry["approved_git_blob_sha1"] = replacement_blob
    entry["approved_sha256"] = sha256(payload_bytes).hexdigest()
    source_payloads[path] = payload_bytes
    source_blob_sha1s[path] = replacement_blob


def test_builds_five_deterministic_git_canonical_typed_records() -> None:
    first = _bundle()
    second = _bundle()
    assert first.to_dict() == second.to_dict()
    assert first.bundle_id == second.bundle_id
    assert first.primary_research_start == "2021-02-22"
    assert first.evidence_exact_commit == APPROVED_COMMIT
    assert len(first.records) == 5
    assert {record.source_path for record in first.records} == EXPECTED_SOURCE_PATHS
    assert {record.role for record in first.records} == set(HistoricalSourceRole)
    assert [type(record.role_payload) for record in first.records] == [
        BaselinePayload,
        ComponentAttributionPayload,
        BranchDecisionPayload,
        MonthlyReviewPayload,
        ProgramSnapshotPayload,
    ]


def test_records_preserve_reviewed_historical_meaning_without_promotion() -> None:
    bundle = _bundle()
    records = {record.role: record for record in bundle.records}

    baseline = records[HistoricalSourceRole.BASELINE]
    assert baseline.windows[0].requested_start == "2023-01-03"
    assert baseline.windows[0].evaluated_end == "2023-07-27"
    assert isinstance(baseline.role_payload, BaselinePayload)
    assert baseline.role_payload.benchmark_id == "B000"
    assert not baseline.role_payload.holdout_accessed

    attribution = records[HistoricalSourceRole.COMPONENT_ATTRIBUTION]
    assert isinstance(attribution.role_payload, ComponentAttributionPayload)
    assert dict(attribution.role_payload.module_statuses)["B4"] == (
        "INCONCLUSIVE_NOT_INDEPENDENTLY_USEFUL"
    )
    assert not attribution.role_payload.holdout_accessed

    decision = records[HistoricalSourceRole.BRANCH_DECISION]
    assert isinstance(decision.role_payload, BranchDecisionPayload)
    assert decision.role_payload.selected_branch == "CONTINUE_B2_ONLY_PATH"
    assert decision.windows[0].requested_start == "2022-12-01"
    assert not decision.role_payload.b5_allowed
    assert not decision.role_payload.b6_allowed
    assert not decision.role_payload.v3_allowed
    assert not decision.role_payload.paper_shadow_allowed

    monthly = records[HistoricalSourceRole.MONTHLY_REVIEW]
    assert isinstance(monthly.role_payload, MonthlyReviewPayload)
    assert "B5" in monthly.role_payload.active_blockers
    assert monthly.windows[0].requested_start == "2022-12-01"

    program = records[HistoricalSourceRole.PROGRAM_SNAPSHOT]
    assert isinstance(program.role_payload, ProgramSnapshotPayload)
    assert program.data_quality is None
    assert program.role_payload.v3_mini_gate_status == "V3_BLOCKED"
    assert not program.role_payload.selected_modules

    assert bundle.production_effect == "none"
    assert not bundle.investment_conclusion_allowed
    assert not bundle.backtest_execution_allowed
    assert not bundle.model_execution_allowed
    assert not bundle.page_projection_allowed
    for record in bundle.records:
        assert not record.research_context_complete
        assert not record.data_quality_ready
        assert record.legacy_history_partial
        assert record.research_only
        assert record.manual_review_only
        assert record.historical_record
        assert not record.current_primary_default
        assert not record.result_projection_allowed
        assert not record.page_projection_allowed
        assert not record.investment_conclusion_generated
        assert record.production_effect == "none"
        assert record.broker_action == "none"


def test_roadmap_is_excluded_without_becoming_a_registered_source() -> None:
    adapter_registry, source_registry, _, _ = _pure_inputs()
    excluded = adapter_registry["excluded_candidates"]
    assert excluded == [
        {
            "candidate_family_id": "atlas_historical_candidate_next_roadmap_v1",
            "source_path": "docs/research/next_research_program_roadmap.json",
            "reason": "缺少满足机械校验要求的 lineage slot；本任务不得读取、注册或适配该工件。",
        }
    ]
    registered_paths = {item["source_path"] for item in source_registry["sources"]}
    assert "docs/research/next_research_program_roadmap.json" not in registered_paths
    assert all(
        item["source_path"] != "docs/research/next_research_program_roadmap.json"
        for item in adapter_registry["adapters"]
    )


def test_rejects_content_tamper_before_json_adaptation() -> None:
    adapter_registry, source_registry, source_payloads, source_blob_sha1s = _pure_inputs()
    path = "docs/research/b0_static_strategic_baseline_result.json"
    source_payloads[path] += b"\n"
    with pytest.raises(HistoricalSourceAdapterError, match="SHA-256 mismatch"):
        build_historical_source_adapter_bundle_from_payloads(
            exact_commit=APPROVED_COMMIT,
            adapter_registry=adapter_registry,
            source_registry=source_registry,
            source_payloads=source_payloads,
            source_blob_sha1s=source_blob_sha1s,
        )


def test_rejects_registration_that_relaxes_historical_safety_boundary() -> None:
    adapter_registry, source_registry, source_payloads, source_blob_sha1s = _pure_inputs()
    tampered_registry = copy.deepcopy(source_registry)
    source = next(
        item
        for item in tampered_registry["sources"]
        if item["source_ref_id"] == "historical-b0-baseline"
    )
    source["data_quality_ready"] = True
    with pytest.raises(
        HistoricalSourceAdapterError,
        match="data_quality_ready must remain False",
    ):
        build_historical_source_adapter_bundle_from_payloads(
            exact_commit=APPROVED_COMMIT,
            adapter_registry=adapter_registry,
            source_registry=tampered_registry,
            source_payloads=source_payloads,
            source_blob_sha1s=source_blob_sha1s,
        )


def test_rejects_non_governed_primary_research_start() -> None:
    adapter_registry, source_registry, source_payloads, source_blob_sha1s = _pure_inputs()
    tampered_adapter_registry = copy.deepcopy(adapter_registry)
    tampered_adapter_registry["primary_research_start"] = "2022-12-01"
    with pytest.raises(HistoricalSourceAdapterError, match="2021-02-22"):
        build_historical_source_adapter_bundle_from_payloads(
            exact_commit=APPROVED_COMMIT,
            adapter_registry=tampered_adapter_registry,
            source_registry=source_registry,
            source_payloads=source_payloads,
            source_blob_sha1s=source_blob_sha1s,
        )


def test_rejects_missing_exact_git_payload() -> None:
    adapter_registry, source_registry, source_payloads, source_blob_sha1s = _pure_inputs()
    source_payloads.pop("docs/research/b0_static_strategic_baseline_result.json")
    with pytest.raises(HistoricalSourceAdapterError, match="missing exact Git payload"):
        build_historical_source_adapter_bundle_from_payloads(
            exact_commit=APPROVED_COMMIT,
            adapter_registry=adapter_registry,
            source_registry=source_registry,
            source_payloads=source_payloads,
            source_blob_sha1s=source_blob_sha1s,
        )


def test_rejects_git_blob_identity_drift() -> None:
    adapter_registry, source_registry, source_payloads, source_blob_sha1s = _pure_inputs()
    path = "docs/research/b0_static_strategic_baseline_result.json"
    source_blob_sha1s[path] = "a" * 40
    with pytest.raises(HistoricalSourceAdapterError, match="Git blob mismatch"):
        build_historical_source_adapter_bundle_from_payloads(
            exact_commit=APPROVED_COMMIT,
            adapter_registry=adapter_registry,
            source_registry=source_registry,
            source_payloads=source_payloads,
            source_blob_sha1s=source_blob_sha1s,
        )


def test_rejects_source_path_traversal_before_payload_read() -> None:
    adapter_registry, source_registry, source_payloads, source_blob_sha1s = _pure_inputs()
    adapter_registry["adapters"][0]["source_path"] = "../outside.json"
    with pytest.raises(HistoricalSourceAdapterError, match="unsafe repository path"):
        build_historical_source_adapter_bundle_from_payloads(
            exact_commit=APPROVED_COMMIT,
            adapter_registry=adapter_registry,
            source_registry=source_registry,
            source_payloads=source_payloads,
            source_blob_sha1s=source_blob_sha1s,
        )


@pytest.mark.parametrize(
    ("field", "replacement", "message"),
    [
        ("schema_version", 2, "schema version mismatch"),
        ("task_id", "TRADING-OTHER", "task id mismatch"),
        ("report_type", "different_report", "report type mismatch"),
    ],
)
def test_rejects_role_contract_identity_drift(
    field: str, replacement: object, message: str
) -> None:
    adapter_registry, source_registry, source_payloads, source_blob_sha1s = _pure_inputs()
    path = "docs/research/b0_static_strategic_baseline_result.json"
    payload = json.loads(source_payloads[path])
    payload[field] = replacement
    _replace_approved_payload(
        adapter_registry=adapter_registry,
        source_payloads=source_payloads,
        source_blob_sha1s=source_blob_sha1s,
        path=path,
        payload=payload,
    )
    with pytest.raises(HistoricalSourceAdapterError, match=message):
        build_historical_source_adapter_bundle_from_payloads(
            exact_commit=APPROVED_COMMIT,
            adapter_registry=adapter_registry,
            source_registry=source_registry,
            source_payloads=source_payloads,
            source_blob_sha1s=source_blob_sha1s,
        )


def test_rejects_missing_lineage_field_after_identity_validation() -> None:
    adapter_registry, source_registry, source_payloads, source_blob_sha1s = _pure_inputs()
    path = "docs/research/weight_research_program_v1_snapshot.json"
    payload = json.loads(source_payloads[path])
    del payload["included_artifacts"]
    _replace_approved_payload(
        adapter_registry=adapter_registry,
        source_payloads=source_payloads,
        source_blob_sha1s=source_blob_sha1s,
        path=path,
        payload=payload,
    )
    with pytest.raises(HistoricalSourceAdapterError, match="included_artifacts"):
        build_historical_source_adapter_bundle_from_payloads(
            exact_commit=APPROVED_COMMIT,
            adapter_registry=adapter_registry,
            source_registry=source_registry,
            source_payloads=source_payloads,
            source_blob_sha1s=source_blob_sha1s,
        )
