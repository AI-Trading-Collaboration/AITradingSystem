from __future__ import annotations

import argparse
import json
from pathlib import Path

from ai_trading_system.platform.architecture.wave_readiness import (
    WaveReadinessError,
    build_wave_readiness_evidence,
    canonical_evidence_bytes,
    load_wave_readiness_evidence,
    validate_wave_readiness_evidence,
)
from ai_trading_system.platform.artifacts import write_bytes_atomic


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build or validate a generic architecture wave readiness artifact."
    )
    parser.add_argument("action", choices=("build", "validate"))
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    root = args.project_root.resolve()
    policy_path = _resolve(root, args.policy)
    evidence_path = _resolve(root, args.evidence)
    try:
        if args.action == "build":
            first = build_wave_readiness_evidence(
                project_root=root,
                policy_path=policy_path,
                output_path=evidence_path,
            )
            second = build_wave_readiness_evidence(
                project_root=root,
                policy_path=policy_path,
                output_path=evidence_path,
            )
            first_bytes = canonical_evidence_bytes(first)
            second_bytes = canonical_evidence_bytes(second)
            if first_bytes != second_bytes:
                raise WaveReadinessError(
                    "BUILD_NON_DETERMINISTIC",
                    "two consecutive in-memory builds differed",
                )
            write_bytes_atomic(evidence_path, first_bytes)
            result = {
                "status": "PASS",
                "action": "build",
                "path": evidence_path.relative_to(root).as_posix(),
                "evidence_checksum": first["evidence_checksum"],
                "byte_identical_double_build": True,
                "production_effect": "none",
            }
        else:
            evidence = load_wave_readiness_evidence(evidence_path)
            validate_wave_readiness_evidence(
                evidence,
                project_root=root,
                policy_path=policy_path,
                evidence_path=evidence_path,
            )
            result = {
                "status": "PASS",
                "action": "validate",
                "path": evidence_path.relative_to(root).as_posix(),
                "evidence_checksum": evidence["evidence_checksum"],
                "production_effect": "none",
            }
    except (OSError, ValueError) as exc:
        code = exc.code if isinstance(exc, WaveReadinessError) else type(exc).__name__
        print(
            json.dumps(
                {
                    "status": "FAIL",
                    "code": code,
                    "message": str(exc),
                    "production_effect": "none",
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


def _resolve(root: Path, path: Path) -> Path:
    candidate = path if path.is_absolute() else root / path
    resolved = candidate.resolve()
    if not resolved.is_relative_to(root):
        raise ValueError(f"path outside project root: {path}")
    return resolved


if __name__ == "__main__":
    raise SystemExit(main())
