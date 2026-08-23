from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SKILL_ROOT = REPO_ROOT / "tools" / "codex_skills" / "run-web-pro-git-review"


def _read(relative_path: str) -> str:
    return (SKILL_ROOT / relative_path).read_text(encoding="utf-8")


def test_skill_bundle_has_required_files_and_no_placeholders() -> None:
    required_files = (
        "SKILL.md",
        "agents/openai.yaml",
        "references/prompt-template.md",
    )

    for relative_path in required_files:
        path = SKILL_ROOT / relative_path
        assert path.is_file(), f"missing skill file: {relative_path}"
        assert "TODO" not in path.read_text(encoding="utf-8")


def test_skill_frontmatter_and_trigger_cover_exact_git_web_pro_review() -> None:
    content = _read("SKILL.md")

    assert content.startswith("---\n")
    assert "name: run-web-pro-git-review" in content
    assert "logged-in ChatGPT Web Pro" in content
    assert "Git exact commit" in content
    assert "strategy-research" in content


def test_skill_preserves_authority_routing_and_recovery_boundaries() -> None:
    content = _read("SKILL.md")

    required_terms = (
        "repository is public or the user explicitly authorizes",
        "Exclude secrets",
        "keeping Git, local policy, and executable",
        "CANNOT_VERIFY_EXACT_BACKEND_ROUTE",
        "UI_PRO_AND_SELF_REPORT_PRO_ROUTE_UNVERIFIED",
        "ROUTING_MISMATCH_SIGNAL",
        "ROUTING_ATTESTATION_UNAVAILABLE",
        "BACKEND_ROUTE_VERIFIED",
        "Never resubmit",
        "Never treat the webpage answer as implementation authorization",
    )

    for term in required_terms:
        assert term in content


def test_explicit_non_sensitive_review_request_does_not_require_repeat_confirmation() -> None:
    content = _read("SKILL.md")

    assert "do not ask for a second \"send now\" confirmation" in content
    assert "non-sensitive public Git or" in content
    assert "personal or sensitive data" in content
    assert "private or unscoped content" in content
    assert "a second submission after an" in content


def test_prompt_template_requires_identity_exact_urls_and_planning_sections() -> None:
    content = _read("references/prompt-template.md")

    required_terms = (
        "<REPOSITORY_URL>",
        "<EXACT_COMMIT_SHA>",
        "<EXACT_TREE_URL>",
        "<NUMBERED_EXACT_BLOB_URLS>",
        "<PLANNING_QUESTION>",
        "MODEL_IDENTITY_AND_ROUTING_RISK",
        "CANNOT_VERIFY_EXACT_BACKEND_ROUTE",
        "A. REPOSITORY_RETRIEVAL",
        "B. CURRENT_STATE",
        "C. RECOMMENDED_SEQUENCE",
        "D. FIRST_TASK_SPEC",
        "E. EXECUTION_TOPOLOGY",
        "F. FALSIFICATION_AND_STOP_MATRIX",
        "G. DOWNSTREAM_GATES",
        "H. 给出未来 <TIME_HORIZON>",
    )

    for term in required_terms:
        assert term in content


def test_openai_metadata_exposes_explicit_skill_invocation() -> None:
    content = _read("agents/openai.yaml")

    assert "display_name:" in content
    assert "short_description:" in content
    assert "$run-web-pro-git-review" in content
