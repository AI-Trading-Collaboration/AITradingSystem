from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from ai_trading_system.heuristic_governance import (
    build_heuristic_governance_payload,
    render_heuristic_governance_markdown,
    write_heuristic_governance_json,
    write_heuristic_governance_report,
)


def test_heuristic_governance_flags_unregistered_numeric_literal(tmp_path: Path) -> None:
    _write_source(tmp_path, "def label(score):\n    return score >= 42\n")
    config_path = _write_governance_config(tmp_path)

    payload = build_heuristic_governance_payload(
        as_of=date(2026, 6, 7),
        config_path=config_path,
        project_root=tmp_path,
    )

    assert payload["status"] == "FAIL"
    assert payload["summary"]["unregistered_numeric_literal_count"] == 1
    finding = payload["unregistered_numeric_literal_findings"][0]
    assert finding["path"] == "src/sample.py"
    assert finding["expression"] == "score >= 42"
    assert finding["numeric_literals"] == ["42"]


def test_heuristic_governance_flags_unregistered_numeric_default(tmp_path: Path) -> None:
    _write_source(tmp_path, "def label(score=42):\n    return score\n")
    config_path = _write_governance_config(tmp_path)

    payload = build_heuristic_governance_payload(
        as_of=date(2026, 6, 7),
        config_path=config_path,
        project_root=tmp_path,
    )

    assert payload["status"] == "FAIL"
    assert payload["summary"]["unregistered_numeric_literal_count"] == 1
    finding = payload["unregistered_numeric_literal_findings"][0]
    assert finding["path"] == "src/sample.py"
    assert finding["expression"] == "label default score=42"
    assert finding["numeric_literals"] == ["42"]


def test_heuristic_governance_registered_baseline_passes(tmp_path: Path) -> None:
    _write_source(
        tmp_path,
        "def label(score):\n"
        "    if score >= 42:\n"
        "        return 'high'\n"
        "    return 'low'\n",
    )
    config_path = _write_governance_config(
        tmp_path,
        baseline=(
            "  - path: src/sample.py\n"
            "    line_hint: 2\n"
            '    expression: "score >= 42"\n'
            "    category: test_policy_threshold\n"
            '    rationale: "测试用 baseline。"\n'
            '    validation: "tests/test_heuristic_governance.py"\n'
        ),
    )

    payload = build_heuristic_governance_payload(
        as_of=date(2026, 6, 7),
        config_path=config_path,
        project_root=tmp_path,
    )
    markdown = render_heuristic_governance_markdown(payload)
    report_path = write_heuristic_governance_report(payload, tmp_path / "audit.md")
    json_path = write_heuristic_governance_json(payload, tmp_path / "audit.json")

    assert payload["status"] == "PASS"
    assert payload["summary"]["registered_numeric_literal_count"] == 1
    assert payload["summary"]["unregistered_numeric_literal_count"] == 0
    assert "production_effect=none" in markdown
    assert "test_policy_threshold" in report_path.read_text(encoding="utf-8")
    assert json.loads(json_path.read_text(encoding="utf-8"))["status"] == "PASS"


def test_heuristic_governance_fails_missing_policy_metadata(tmp_path: Path) -> None:
    _write_source(tmp_path, "def identity(value):\n    return value\n")
    policy_path = tmp_path / "config" / "sample_policy.yaml"
    policy_path.parent.mkdir(parents=True, exist_ok=True)
    policy_path.write_text(
        "\n".join(
            [
                "policy_metadata:",
                "  version: sample_policy_v1",
                "  status: pilot",
                "  owner: system",
                "  rationale: ''",
                "  review_after_reports: 8",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    config_path = _write_governance_config(
        tmp_path,
        required_policy_metadata=(
            "  - path: config/sample_policy.yaml\n"
            "    section: policy_metadata\n"
            "    required_fields:\n"
            "      - version\n"
            "      - status\n"
            "      - owner\n"
            "      - rationale\n"
            "      - validation\n"
            "      - review_after_reports\n"
        ),
    )

    payload = build_heuristic_governance_payload(
        as_of=date(2026, 6, 7),
        config_path=config_path,
        project_root=tmp_path,
    )

    assert payload["status"] == "FAIL"
    assert payload["summary"]["failed_policy_metadata_check_count"] == 1
    check = payload["policy_metadata_checks"][0]
    assert check["missing_fields"] == ["rationale", "validation"]


def test_default_heuristic_governance_audit_passes() -> None:
    payload = build_heuristic_governance_payload(as_of=date(2026, 6, 7))

    assert payload["status"] == "PASS"
    assert payload["production_effect"] == "none"
    assert payload["policy_version"] == "heuristic_governance_v1"
    assert payload["summary"]["unregistered_numeric_literal_count"] == 0
    assert payload["summary"]["failed_policy_metadata_check_count"] == 0


def _write_source(tmp_path: Path, text: str) -> Path:
    source_path = tmp_path / "src" / "sample.py"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text(text, encoding="utf-8")
    return source_path


def _write_governance_config(
    tmp_path: Path,
    *,
    baseline: str = "",
    required_policy_metadata: str = "",
) -> Path:
    config_path = tmp_path / "config" / "heuristic_governance.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        "\n".join(
            [
                "policy_metadata:",
                "  version: test_heuristic_governance_v1",
                "  status: pilot",
                "  owner: system",
                '  rationale: "测试用 heuristic governance policy。"',
                '  validation: "tests/test_heuristic_governance.py"',
                "  review_after_reports: 1",
                "audit_scope:",
                "  source_paths:",
                "    - src/sample.py",
                "allowed_numeric_literals:",
                "  scale_bounds: [0, 1, 100]",
                "numeric_literal_baseline:",
                baseline.rstrip() if baseline else "  []",
                "required_policy_metadata:",
                required_policy_metadata.rstrip() if required_policy_metadata else "  []",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return config_path
