from __future__ import annotations

import copy
import json
import subprocess
from hashlib import sha256
from pathlib import Path

import pytest
import yaml

from ai_trading_system.atlas import historical_projection_review as projection
from ai_trading_system.atlas.historical_projection_review import (
    HistoricalProjectionReviewError,
    build_historical_projection_review,
    validate_historical_projection_review,
    write_historical_projection_review_artifacts,
)
from ai_trading_system.atlas.historical_projection_review_renderer import (
    render_historical_projection_review_html,
)
from ai_trading_system.atlas.page_effectiveness import (
    validate_page_effectiveness_manifest,
)
from ai_trading_system.contracts.strategy_research_page_effectiveness import (
    PageFreshnessStatus,
    StrategyResearchPageEffectivenessManifest,
)

ROOT = Path(__file__).resolve().parents[2]
POLICY_PATH = ROOT / "config" / "atlas" / "historical_projection_review.yaml"


def _exact_commit() -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _policy() -> dict[str, object]:
    loaded = yaml.safe_load(POLICY_PATH.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def _build(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    policy_mutator: object | None = None,
    page_bytes: bytes = b"<html><body>canonical fixture</body></html>",
):
    policy = copy.deepcopy(_policy())
    canonical = policy["canonical_page"]
    assert isinstance(canonical, dict)
    canonical["expected_size_bytes"] = len(page_bytes)
    canonical["expected_sha256"] = sha256(page_bytes).hexdigest()
    if policy_mutator is not None:
        assert callable(policy_mutator)
        policy_mutator(policy)
    monkeypatch.setattr(projection, "_yaml_mapping", lambda _payload, _name: policy)
    page = tmp_path / "canonical.html"
    page.write_bytes(page_bytes)
    pack = build_historical_projection_review(
        repository_root=ROOT,
        evidence_exact_commit=_exact_commit(),
        canonical_page_file=page,
    )
    return pack, page


def test_pack_uses_exact_five_records_and_expected_counts(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    pack, _page = _build(monkeypatch, tmp_path)
    assert pack.current_counts == {
        "sources": 13,
        "nodes": 21,
        "edges": 22,
        "results": 8,
        "attributions": 12,
    }
    assert pack.candidate_counts == {
        "sources": 13,
        "nodes": 27,
        "edges": 28,
        "results": 13,
        "attributions": 17,
    }
    assert len(pack.records) == 5
    assert {item["source_ref_id"] for item in pack.records} == {
        "historical-b0-baseline",
        "historical-b1-b4-attribution",
        "historical-final-branch-decision",
        "historical-monthly-program-review",
        "historical-weight-program-snapshot",
    }


def test_status_mapping_is_explicit_and_all_display_limited(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    pack, _page = _build(monkeypatch, tmp_path)
    by_source = {str(item["source_ref_id"]): item for item in pack.records}
    assert by_source["historical-weight-program-snapshot"]["proposed_raw_status"] == "LIMITED"
    assert sum(item["proposed_raw_status"] == "PASS" for item in pack.records) == 4
    assert {item["proposed_display_status"] for item in pack.records} == {"LIMITED"}
    assert all(item["mapping_rationale"] for item in pack.records)
    assert all(item["investment_facing"] is False for item in pack.records)


def test_dq_and_window_distinctions_are_preserved(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    pack, _page = _build(monkeypatch, tmp_path)
    by_source = {str(item["source_ref_id"]): item for item in pack.records}
    baseline = by_source["historical-b0-baseline"]
    assert baseline["data_quality_label"] == "PASS_WITH_WARNINGS"
    assert baseline["windows"] == [
        {
            "window_id": "normal_market_regime",
            "source_field": "window",
            "requested_start": "2023-01-03",
            "requested_end": "2023-07-31",
            "evaluated_start": "2023-01-03",
            "evaluated_end": "2023-07-27",
        }
    ]
    program = by_source["historical-weight-program-snapshot"]
    assert program["data_quality"] is None
    assert program["data_quality_label"] == "未提供（null）"
    assert program["windows"] == []
    decision = by_source["historical-final-branch-decision"]
    assert decision["windows"][0]["requested_start"] == "2022-12-01"
    assert pack.primary_research_start == "2021-02-22"


def test_candidate_graph_is_contains_only_and_provenance_is_neutral(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    pack, _page = _build(monkeypatch, tmp_path)
    assert len(pack.candidate_nodes) == 6
    assert len(pack.candidate_edges) == 6
    assert len(pack.candidate_results) == 5
    assert len(pack.candidate_attributions) == 5
    assert {item["edge_kind"] for item in pack.candidate_edges} == {"CONTAINS"}
    assert {item["direction"] for item in pack.candidate_attributions} == {"NEUTRAL"}
    assert all(item["investment_facing"] is False for item in pack.candidate_results)
    assert len({item["node_id"] for item in pack.candidate_nodes}) == 6


def test_renderer_has_flow_cards_and_no_active_surface(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    pack, _page = _build(monkeypatch, tmp_path)
    html = render_historical_projection_review_html(pack)
    assert "整个策略系统中的位置" in html
    assert "当前关注" in html
    assert "未投影 · review only" in html
    assert html.count('data-source-ref="historical-') == 5
    assert html.count('data-display-status="LIMITED"') == 5
    lowered = html.lower()
    assert "<script" not in lowered
    assert "<form" not in lowered
    assert " src=" not in lowered
    assert " href=" not in lowered


def test_validation_rebuild_is_byte_identical(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    pack, page = _build(monkeypatch, tmp_path)
    validation = validate_historical_projection_review(
        pack,
        repository_root=ROOT,
        canonical_page_file=page,
    )
    assert validation.status == "PASS"
    assert len(validation.checks) == 16
    assert validation.errors == ()


def test_writer_is_byte_identical_across_directories(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    pack, page = _build(monkeypatch, tmp_path)
    first = tmp_path / "first"
    second = tmp_path / "second"
    write_historical_projection_review_artifacts(
        pack, first, repository_root=ROOT, canonical_page_file=page
    )
    write_historical_projection_review_artifacts(
        pack, second, repository_root=ROOT, canonical_page_file=page
    )
    expected = {"index.html", "review_pack.json", "review_pack.md", "validation.json"}
    assert {item.name for item in first.iterdir()} == expected
    assert {item.name for item in second.iterdir()} == expected
    for name in expected:
        assert (first / name).read_bytes() == (second / name).read_bytes()


def test_canonical_page_tamper_fails_closed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    pack, page = _build(monkeypatch, tmp_path)
    page.write_bytes(b"tampered")
    with pytest.raises(HistoricalProjectionReviewError, match="CANONICAL_PAGE_IDENTITY_MISMATCH"):
        build_historical_projection_review(
            repository_root=ROOT,
            evidence_exact_commit=pack.evidence_exact_commit,
            canonical_page_file=page,
        )


def test_candidate_id_collision_fails_closed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def collide(policy: dict[str, object]) -> None:
        group = policy["group_node"]
        root_edge = policy["root_edge"]
        assert isinstance(group, dict) and isinstance(root_edge, dict)
        group["node_id"] = "program-strategy-research"
        root_edge["to_node_id"] = "program-strategy-research"

    with pytest.raises(HistoricalProjectionReviewError, match="CANDIDATE_ID_COLLISION"):
        _build(monkeypatch, tmp_path, policy_mutator=collide)


def test_original_status_policy_drift_fails_closed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def drift(policy: dict[str, object]) -> None:
        records = policy["records"]
        assert isinstance(records, list) and isinstance(records[0], dict)
        records[0]["expected_original_status"] = "PASS"

    with pytest.raises(HistoricalProjectionReviewError, match="ORIGINAL_STATUS_MISMATCH"):
        _build(monkeypatch, tmp_path, policy_mutator=drift)


def test_safety_policy_tamper_fails_closed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def tamper(policy: dict[str, object]) -> None:
        safety = policy["safety"]
        assert isinstance(safety, dict)
        safety["page_projection_performed"] = True

    with pytest.raises(HistoricalProjectionReviewError, match="POLICY_SAFETY_BOUNDARY_MISMATCH"):
        _build(monkeypatch, tmp_path, policy_mutator=tamper)


def test_current_count_drift_fails_closed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def drift(policy: dict[str, object]) -> None:
        counts = policy["expected_current_counts"]
        assert isinstance(counts, dict)
        counts["nodes"] = 22

    with pytest.raises(HistoricalProjectionReviewError, match="CURRENT_COUNT_MISMATCH"):
        _build(monkeypatch, tmp_path, policy_mutator=drift)


def test_forbidden_roadmap_policy_cannot_be_removed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def remove(policy: dict[str, object]) -> None:
        policy["forbidden_candidate_family_ids"] = []

    with pytest.raises(HistoricalProjectionReviewError, match="FORBIDDEN_CANDIDATE_SET_MISMATCH"):
        _build(monkeypatch, tmp_path, policy_mutator=remove)


def test_local_canonical_page_uses_current_successor_identity_when_available() -> None:
    canonical_policy = _policy()["canonical_page"]
    assert isinstance(canonical_policy, dict)
    repository_path = canonical_policy["repository_path"]
    assert isinstance(repository_path, str)
    relative_path = Path(repository_path)
    assert not relative_path.is_absolute()
    assert ".." not in relative_path.parts
    canonical = ROOT / relative_path
    if not canonical.is_file():
        pytest.skip("local canonical ignored artifact not hydrated")
    payload = canonical.read_bytes()
    effectiveness_sidecar = canonical.parent / "page_effectiveness.json"
    if effectiveness_sidecar.is_file():
        manifest = StrategyResearchPageEffectivenessManifest.from_json_bytes(
            effectiveness_sidecar.read_bytes()
        )
        rendered_payloads: dict[str, bytes] = {}
        for identity in manifest.rendered_artifacts:
            artifact = ROOT / identity.locator
            assert artifact.is_file()
            rendered_payloads[Path(identity.locator).name] = artifact.read_bytes()
        validation = validate_page_effectiveness_manifest(
            repository_root=ROOT,
            manifest=manifest,
            rendered_payloads=rendered_payloads,
        )
        page_identity = next(
            identity
            for identity in manifest.rendered_artifacts
            if identity.locator == repository_path
        )
        validation_sidecar = json.loads(
            (canonical.parent / "page_effectiveness_validation.json").read_text(encoding="utf-8")
        )

        assert validation.status == "PASS"
        assert manifest.freshness_status is PageFreshnessStatus.CURRENT
        assert manifest.repository_commit == _exact_commit()
        assert len(payload) == page_identity.byte_count
        assert sha256(payload).hexdigest() == page_identity.sha256
        assert [item.task_id.split("_", 1)[0] for item in manifest.task_coverage] == [
            *[f"TRADING-{task_id}" for task_id in range(2481, 2505)],
            "TRADING-2506",
            "TRADING-2507",
            "TRADING-2508",
            "TRADING-2509",
            "TRADING-2510",
            "TRADING-2511",
            "TRADING-2512",
            "TRADING-2513",
        ]
        assert validation_sidecar["status"] == "PASS"
        assert validation_sidecar["manifest_sha256"] == manifest.content_sha256
        assert validation_sidecar["page_sha256"] == page_identity.sha256
    else:
        assert len(payload) == 182100
        assert sha256(payload).hexdigest() == (
            "1bbfb90fb5b2eb7dd1cbea29e07ca60338e29ba995f5f263162736519d7ea337"
        )
    assert payload.count(b'data-historical-record="true"') == 5
    assert payload.count(b'data-aggregate-conclusion="NO_GO_KEEP_BLOCKED"') == 1
    assert all(
        payload.count(f'data-qqq-task="TRADING-{task_id}"'.encode()) == 1
        for task_id in range(2481, 2494)
    )
    if not effectiveness_sidecar.is_file():
        expected_sidecars = {
            "status_explanations.json": (
                27641,
                "a4e832fdd043a81948b293becbe0a785d84fb5d48d0423fec90ec93984bd6d16",
            ),
            "status_explanation_validation.json": (
                684,
                "0465631c0862adca5acf4b41672294b1482e7a40aa68106c35489fbf8c7b8377",
            ),
            "qqq_options_projection.json": (
                17551,
                "cf22c77583ce3976d24e74df67077a13a46cd22c10f37a5b697b1e7fa2aa26df",
            ),
            "qqq_options_projection_validation.json": (
                895,
                "f05a577c00669cb2855b83c17a82ec1a8f1b212b630d3a94f836eb885f8348e2",
            ),
        }
        for name, (expected_size, expected_sha256) in expected_sidecars.items():
            sidecar = canonical.parent / name
            assert sidecar.is_file()
            sidecar_payload = sidecar.read_bytes()
            assert len(sidecar_payload) == expected_size
            assert sha256(sidecar_payload).hexdigest() == expected_sha256
