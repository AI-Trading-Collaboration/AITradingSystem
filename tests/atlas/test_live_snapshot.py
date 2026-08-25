from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

from ai_trading_system.atlas.live_snapshot import (
    build_live_snapshot_bundle,
    build_reader_decision_projection,
    load_live_snapshot_policy,
)
from ai_trading_system.atlas.page_effectiveness import (
    build_page_effectiveness_manifest,
    load_page_effectiveness_policy,
    repository_head,
    unclassified_page_successors,
)
from ai_trading_system.atlas.snapshot_builder import build_atlas_bundle
from ai_trading_system.contracts.strategy_research_cited_query import CitedQueryQuestionId
from ai_trading_system.contracts.strategy_research_explorer import (
    StrategyResearchExplorerSnapshot,
)
from ai_trading_system.platform.architecture.task_registry_canonical import (
    validate_canonical_registry,
)

ROOT = Path(__file__).resolve().parents[2]


def test_live_snapshot_binds_all_page_tasks_events_requirements_and_commit() -> None:
    head = repository_head(ROOT)
    bundle = build_live_snapshot_bundle(repository_root=ROOT, exact_commit=head)
    manifest = build_page_effectiveness_manifest(
        repository_root=ROOT,
        source_snapshot_commit=head,
    )
    current_sources = {item.source_ref_id: item for item in bundle.current_snapshot.sources}

    assert bundle.comparison_snapshot.generated_at.isoformat() == "2026-08-02T00:00:00+09:00"
    assert bundle.current_snapshot.generated_at > bundle.comparison_snapshot.generated_at
    assert bundle.research_state_as_of.startswith("2026-08-25T")
    assert bundle.evidence_evaluated_at is None
    assert {item.exact_commit for item in bundle.current_snapshot.sources} == {head}
    assert bundle.current_diff.before_snapshot_id == bundle.comparison_snapshot.snapshot_id
    assert bundle.current_diff.after_snapshot_id == bundle.current_snapshot.snapshot_id
    assert set(bundle.target_ids) == set(CitedQueryQuestionId)

    for task in manifest.task_coverage:
        slug = task.task_id.lower().replace("_", "-")
        requirement = current_sources[f"live-requirement-{slug}"]
        event = current_sources[f"live-task-event-{slug}"]
        assert requirement.content_sha256 == task.requirement_sha256
        assert requirement.source_path == task.requirement_path
        assert event.content_sha256 == task.task_fragment_sha256
        assert event.source_path == task.task_fragment_path
        assert event.artifact_identity == f"task-event:{task.task_event_id}"


def test_live_snapshot_is_not_the_old_fixture_only_title_and_summary_mutation() -> None:
    head = repository_head(ROOT)
    historical = build_atlas_bundle(repository_root=ROOT, exact_commit=head).snapshot
    synthetic = StrategyResearchExplorerSnapshot.build(
        title=historical.title + " next",
        generated_at=historical.generated_at + timedelta(days=1),
        sources=historical.sources,
        nodes=tuple(
            replace(item, summary=item.summary + " 已增加引用式问答入口。")
            if item.node_id == "program-strategy-research"
            else item
            for item in historical.nodes
        ),
        edges=historical.edges,
        results=historical.results,
        attributions=historical.attributions,
    )
    live = build_live_snapshot_bundle(repository_root=ROOT, exact_commit=head)

    assert live.current_snapshot.snapshot_id != synthetic.snapshot_id
    assert len(live.current_snapshot.nodes) > len(synthetic.nodes)
    assert any(
        item.source_ref_id.startswith("live-task-event-")
        for item in live.current_snapshot.sources
    )


def test_unclassified_successor_is_detected_before_live_projection() -> None:
    policy = load_page_effectiveness_policy(repository_root=ROOT)
    registry = validate_canonical_registry(project_root=ROOT)
    without_latest = replace(policy, task_sources=policy.task_sources[:-1])

    assert unclassified_page_successors(registry, without_latest) == (
        "TRADING-2545_ATLAS_CURRENT_STATE_DOMINANCE_AND_READER_CARD_REPAIR_V1",
    )


def test_live_policy_separates_research_evidence_and_page_dates() -> None:
    policy = load_live_snapshot_policy(repository_root=ROOT)
    assert policy.task_status_mapping["PROPOSED"] == "NOT_DUE"
    bundle = build_live_snapshot_bundle(
        repository_root=ROOT,
        exact_commit=repository_head(ROOT),
    )

    assert policy.current_mainline_task_id.startswith("TRADING-2542F_")
    assert bundle.research_state_as_of != bundle.page_source_commit_at
    assert bundle.evidence_evaluated_at is None
    assert bundle.status_object_zh == (
        "veto / option-signal result-blind architecture 草案已验证；DQ、action guard、四项独立 "
        "market-state veto、option alpha 与 optional option-risk diagnostic 已分层，当前等待 "
        "Owner exact freeze，series、manifest、真实 DQ 与 backtest 继续关闭"
    )


def test_reader_decision_projection_separates_transport_from_dq_pit_promotion() -> None:
    manifest = build_page_effectiveness_manifest(repository_root=ROOT)
    projection = build_reader_decision_projection(
        repository_root=ROOT,
        coverage=manifest.task_coverage,
        policy=load_live_snapshot_policy(repository_root=ROOT),
    )

    assert projection.normal_session_count == 1201
    assert projection.recovered_session_count == 1
    assert projection.unresolved_session_count == 0
    assert projection.observed_session_count == projection.expected_session_count == 1202
    assert projection.dq_pit_promoted is False
    assert all(
        "TRADING-2545_ATLAS_CURRENT_STATE_DOMINANCE_AND_READER_CARD_REPAIR_V1"
        in item.source_task_ids
        for item in projection.reader_cards
    )
    visible = " ".join(item.text_zh for item in projection.reader_cards)
    assert "合计 1202/1202" in visible
    assert "仍有 1 天全日未出现期权链" not in visible
    assert "先解释唯一缺链交易日" not in visible


def test_canonical_writer_has_no_test_fixture_dependency(tmp_path: Path) -> None:
    source = (ROOT / "scripts" / "render_atlas_strategy_research_page.py").read_text(
        encoding="utf-8"
    )

    assert "tests." not in source
    assert "_payloads" not in source
    assert "build_live_snapshot_bundle" in source
    assert "sys.path.insert(0, str(SOURCE_ROOT))" in source

    poisoned = tmp_path / "poisoned-import"
    package = poisoned / "ai_trading_system"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text(
        'raise RuntimeError("EXTERNAL_EDITABLE_INSTALL_USED")\n',
        encoding="utf-8",
    )
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(poisoned)
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "render_atlas_strategy_research_page.py"),
            "--help",
        ],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert "EXTERNAL_EDITABLE_INSTALL_USED" not in completed.stderr
