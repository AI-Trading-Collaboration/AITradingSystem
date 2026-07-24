from __future__ import annotations

import subprocess
from pathlib import Path

from ai_trading_system.platform.architecture.wave_readiness import (
    load_strict_json_text,
    validate_committed_wave_readiness_carrier,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = PROJECT_ROOT / "config/architecture/arch_004_wave15_d0b3_g4b_g3_close_readiness.yaml"
EVIDENCE_PATH = (
    PROJECT_ROOT / "inputs/architecture/arch_004_wave15_d0b3_g4b_g3_close_parallel_readiness.json"
)
REQUIREMENT_PATH = (
    PROJECT_ROOT / "docs/requirements/ARCH-004_Wave15_D0B3_G4B_G3_Close_Parallel_Readiness.md"
)


def test_wave15_exact_policy_and_evidence_are_atomic_across_c_and_d() -> None:
    output_paths = (POLICY_PATH, EVIDENCE_PATH)
    present = tuple(path.exists() or path.is_symlink() for path in output_paths)
    assert present[0] is present[1], "Wave15 carrier outputs must appear as one atomic pair"
    if not all(present):
        requirement = REQUIREMENT_PATH.read_text(encoding="utf-8")
        assert "current stage=`S0_AUTHORIZED_AWAITING_EXACT_CARRIER`" in requirement
        assert "- `D`：承载 reviewed Wave15 policy 与 exact readiness evidence" in requirement
        assert POLICY_PATH.relative_to(PROJECT_ROOT).as_posix() in requirement
        assert EVIDENCE_PATH.relative_to(PROJECT_ROOT).as_posix() in requirement
        history = subprocess.run(
            [
                "git",
                "log",
                "--format=%H",
                "--",
                *(path.relative_to(PROJECT_ROOT).as_posix() for path in output_paths),
            ],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        assert history == "", "C state is invalid after either carrier output enters Git history"
        return

    for path in output_paths:
        assert path.is_file(), f"{path} must be a regular file"
        assert not path.is_symlink(), f"{path} must not be a symlink"
    payload = load_strict_json_text(
        EVIDENCE_PATH.read_text(encoding="utf-8"),
        label=EVIDENCE_PATH.relative_to(PROJECT_ROOT).as_posix(),
    )
    assert isinstance(payload, dict)
    validate_committed_wave_readiness_carrier(
        payload,
        project_root=PROJECT_ROOT,
        policy_path=POLICY_PATH,
        evidence_path=EVIDENCE_PATH,
    )
