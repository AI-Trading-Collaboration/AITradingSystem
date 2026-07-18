from __future__ import annotations

from pathlib import Path

WORKFLOW_PATH = Path(".github/workflows/ci.yml")


def test_ci_push_uses_fast_validation_tier_by_default() -> None:
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    assert "VALIDATION_TIER: fast-unit" in workflow
    assert 'python scripts/run_validation_tier.py "$VALIDATION_TIER"' in workflow
    assert "default: fast-unit" in workflow
    assert "run_validation_tier.py full --pytest" not in workflow


def test_ci_keeps_full_validation_for_scheduled_and_manual_runs() -> None:
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    assert "workflow_dispatch:" in workflow
    assert "validation_tier:" in workflow
    assert "- full" in workflow
    assert "schedule:" in workflow
    assert 'echo "VALIDATION_TIER=full" >> "$GITHUB_ENV"' in workflow
    assert 'echo "VALIDATION_TRIGGER_REASON=scheduled_ci" >> "$GITHUB_ENV"' in workflow
    assert 'echo "VALIDATION_TASK_ID=CI_DAILY_FULL" >> "$GITHUB_ENV"' in workflow
    assert 'echo "VALIDATION_TIER=${{ inputs.validation_tier }}" >> "$GITHUB_ENV"' in workflow


def test_ci_persists_canonical_validation_trigger_provenance() -> None:
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    assert "validation_trigger_reason:" in workflow
    assert "task_id:" in workflow
    assert "VALIDATION_BOUNDARY_ID:" in workflow
    assert "--write-runtime-artifact" in workflow
    assert '--trigger-reason="$VALIDATION_TRIGGER_REASON"' in workflow
    assert '--task-id="$VALIDATION_TASK_ID"' in workflow
    assert '--boundary-id="$VALIDATION_BOUNDARY_ID"' in workflow


def test_ci_does_not_offer_unverifiable_manual_failure_rerun() -> None:
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    assert "failure_fix_rerun" not in workflow
    assert "parent_run:" not in workflow
    assert "VALIDATION_PARENT_RUN" not in workflow
    assert "--parent-run=" not in workflow


def test_ci_always_uploads_validation_evidence() -> None:
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    assert "- name: Upload validation evidence" in workflow
    assert "if: ${{ always() }}" in workflow
    assert "uses: actions/upload-artifact@v4" in workflow
    assert "outputs/validation_runtime/" in workflow
    assert "pytest-results.xml" in workflow


def test_ci_cancels_superseded_branch_runs() -> None:
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    assert "concurrency:" in workflow
    assert "group: ${{ github.workflow }}-${{ github.ref }}" in workflow
    assert (
        "cancel-in-progress: ${{ github.event_name == 'push' "
        "|| github.event_name == 'pull_request' }}"
    ) in workflow
