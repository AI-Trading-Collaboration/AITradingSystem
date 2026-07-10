from __future__ import annotations

from pathlib import Path

from ai_trading_system.platform.architecture import (
    capture_direct_writer_baseline,
    validate_architecture_dependencies,
)
from ai_trading_system.platform.artifacts import write_text_atomic, write_yaml_atomic

PROJECT_ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = PROJECT_ROOT / "config/architecture/arch_004c_dependency_policy.yaml"
BASELINE_PATH = PROJECT_ROOT / "inputs/architecture/arch_004c_direct_writer_baseline.yaml"
SOURCE_ROOT = PROJECT_ROOT / "src/ai_trading_system"


def test_repository_architecture_dependency_and_direct_writer_ratchets_pass() -> None:
    report = validate_architecture_dependencies(
        policy_path=POLICY_PATH,
        baseline_path=BASELINE_PATH,
        source_root=SOURCE_ROOT,
    )

    assert report.status == "PASS"
    assert report.violations == ()
    assert report.scanned_python_files >= 769
    assert report.current_direct_writer_calls <= report.baseline_direct_writer_calls


def test_gate_reports_layer_and_new_direct_writer_with_owner_and_remediation(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "src/ai_trading_system"
    bad_path = source_root / "contracts/bad_contract.py"
    write_text_atomic(
        bad_path,
        "from ai_trading_system.config import PROJECT_ROOT\n"
        "def write(path):\n"
        "    path.write_text('bad', encoding='utf-8')\n",
    )
    policy_path, baseline_path = _write_fixture_policy(tmp_path, entries=[])

    report = validate_architecture_dependencies(
        policy_path=policy_path,
        baseline_path=baseline_path,
        source_root=source_root,
    )

    assert report.status == "FAIL"
    assert {item.rule_id for item in report.violations} == {
        "CONTRACTS_REVERSE_DEPENDENCY_FORBIDDEN",
        "NEW_DIRECT_ARTIFACT_WRITER_FORBIDDEN",
    }
    assert all(item.owner for item in report.violations)
    assert all(item.remediation for item in report.violations)


def test_direct_writer_baseline_is_a_count_ratchet_not_a_path_waiver(tmp_path: Path) -> None:
    source_root = tmp_path / "src/ai_trading_system"
    source_path = source_root / "legacy_writer.py"
    write_text_atomic(
        source_path,
        "def write(path):\n" "    path.write_text('old', encoding='utf-8')\n",
    )
    baseline_path = tmp_path / "baseline.yaml"
    capture_direct_writer_baseline(
        source_root=source_root,
        output_path=baseline_path,
        canonical_writer_path="src/ai_trading_system/platform/artifacts/writer.py",
        source_commit="test",
    )
    policy_path, _ = _write_fixture_policy(tmp_path, baseline_path=baseline_path)

    first = validate_architecture_dependencies(
        policy_path=policy_path,
        baseline_path=baseline_path,
        source_root=source_root,
    )
    assert first.status == "PASS"

    write_text_atomic(
        source_path,
        "def write(path):\n"
        "    path.write_text('old', encoding='utf-8')\n"
        "    path.write_text('new', encoding='utf-8')\n",
    )
    second = validate_architecture_dependencies(
        policy_path=policy_path,
        baseline_path=baseline_path,
        source_root=source_root,
    )
    assert second.status == "FAIL"
    assert second.violations[0].rule_id == "NEW_DIRECT_ARTIFACT_WRITER_FORBIDDEN"
    assert "count=2 exceeds frozen baseline=1" in second.violations[0].message


def _write_fixture_policy(
    tmp_path: Path,
    *,
    entries: list[dict[str, object]] | None = None,
    baseline_path: Path | None = None,
) -> tuple[Path, Path]:
    policy_path = tmp_path / "policy.yaml"
    actual_baseline = baseline_path or tmp_path / "baseline.yaml"
    write_yaml_atomic(
        policy_path,
        {
            "schema_version": "test.v1",
            "policy_id": "test_policy",
            "canonical_writer_path": "src/ai_trading_system/platform/artifacts/writer.py",
            "layer_rules": [
                {
                    "rule_id": "CONTRACTS_REVERSE_DEPENDENCY_FORBIDDEN",
                    "path_prefix": "src/ai_trading_system/contracts/",
                    "owner": "architecture",
                    "forbidden_import_prefixes": ["ai_trading_system.config"],
                    "remediation": "move dependency",
                }
            ],
            "direct_writer_ratchet": {
                "owner": "platform_io",
                "remediation": "use canonical writer",
            },
        },
        sort_keys=False,
    )
    if baseline_path is None:
        write_yaml_atomic(
            actual_baseline,
            {
                "schema_version": "test.v1",
                "entries": entries or [],
            },
            sort_keys=False,
        )
    return policy_path, actual_baseline
