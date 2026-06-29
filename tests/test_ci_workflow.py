from __future__ import annotations

from pathlib import Path

WORKFLOW_PATH = Path(".github/workflows/ci.yml")


def test_ci_push_uses_fast_validation_tier_by_default() -> None:
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    assert "VALIDATION_TIER: fast-unit" in workflow
    assert 'run: python scripts/run_validation_tier.py "$VALIDATION_TIER"' in workflow
    assert "run_validation_tier.py full --pytest" not in workflow


def test_ci_keeps_full_validation_for_scheduled_and_manual_runs() -> None:
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    assert "workflow_dispatch:" in workflow
    assert "validation_tier:" in workflow
    assert "- full" in workflow
    assert "schedule:" in workflow
    assert 'run: echo "VALIDATION_TIER=full" >> "$GITHUB_ENV"' in workflow
    assert 'run: echo "VALIDATION_TIER=${{ inputs.validation_tier }}" >> "$GITHUB_ENV"' in workflow


def test_ci_cancels_superseded_branch_runs() -> None:
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    assert "concurrency:" in workflow
    assert "group: ${{ github.workflow }}-${{ github.ref }}" in workflow
    assert (
        "cancel-in-progress: ${{ github.event_name == 'push' "
        "|| github.event_name == 'pull_request' }}"
    ) in workflow
