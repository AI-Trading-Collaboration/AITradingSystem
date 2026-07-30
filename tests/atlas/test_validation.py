from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from ai_trading_system.atlas.snapshot_builder import build_atlas_bundle
from ai_trading_system.atlas.validation import (
    validate_atlas_bundle,
    validation_json_bytes,
)
from ai_trading_system.contracts import (
    ExplorerSourceKind,
    StrategyResearchExplorerSnapshot,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
EXACT_COMMIT = "adfd3d5817a9797c35f97d01b92ced2e01663373"


def _bundle():
    return build_atlas_bundle(
        repository_root=PROJECT_ROOT,
        exact_commit=EXACT_COMMIT,
    )


def test_canonical_bundle_passes_independent_validation() -> None:
    result = validate_atlas_bundle(_bundle())
    assert result.status == "PASS"
    assert result.error_count == 0
    assert result.errors == ()
    assert validation_json_bytes(result).endswith(b"\n")


def test_mvp_validator_rejects_unverified_context_source() -> None:
    bundle = _bundle()
    changed_sources = (
        replace(
            bundle.snapshot.sources[0],
            source_kind=ExplorerSourceKind.UNVERIFIED_CONTEXT,
        ),
        *bundle.snapshot.sources[1:],
    )
    changed_snapshot = StrategyResearchExplorerSnapshot.build(
        title=bundle.snapshot.title,
        generated_at=bundle.snapshot.generated_at,
        sources=changed_sources,
        nodes=bundle.snapshot.nodes,
        edges=bundle.snapshot.edges,
        results=bundle.snapshot.results,
        attributions=bundle.snapshot.attributions,
    )
    result = validate_atlas_bundle(replace(bundle, snapshot=changed_snapshot))
    assert result.status == "FAIL"
    assert "ATLAS_MVP_UNVERIFIED_CONTEXT_SOURCE_FORBIDDEN" in result.errors


def test_mvp_validator_rejects_investment_facing_result() -> None:
    bundle = _bundle()
    changed_results = (
        replace(bundle.snapshot.results[0], investment_facing=True),
        *bundle.snapshot.results[1:],
    )
    changed_snapshot = StrategyResearchExplorerSnapshot.build(
        title=bundle.snapshot.title,
        generated_at=bundle.snapshot.generated_at,
        sources=bundle.snapshot.sources,
        nodes=bundle.snapshot.nodes,
        edges=bundle.snapshot.edges,
        results=changed_results,
        attributions=bundle.snapshot.attributions,
    )
    result = validate_atlas_bundle(replace(bundle, snapshot=changed_snapshot))
    assert result.status == "FAIL"
    assert "ATLAS_MVP_INVESTMENT_FACING_RESULT_FORBIDDEN" in result.errors
