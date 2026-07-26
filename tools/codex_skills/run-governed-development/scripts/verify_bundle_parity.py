#!/usr/bin/env python3
"""Verify canonical and installed skill bundles are byte-identical."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

SCHEMA_VERSION = "codex_skill_bundle_parity.v1"


def bundle_hashes(root: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if not path.is_file() or "__pycache__" in path.parts or path.suffix == ".pyc":
            continue
        relative = path.relative_to(root).as_posix()
        result[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--canonical", required=True)
    parser.add_argument("--installed", required=True)
    args = parser.parse_args()

    canonical = Path(args.canonical).resolve()
    installed = Path(args.installed).resolve()
    canonical_hashes = bundle_hashes(canonical)
    installed_hashes = bundle_hashes(installed)
    all_paths = sorted(set(canonical_hashes) | set(installed_hashes))
    mismatches = [
        {
            "path": path,
            "canonical_sha256": canonical_hashes.get(path),
            "installed_sha256": installed_hashes.get(path),
        }
        for path in all_paths
        if canonical_hashes.get(path) != installed_hashes.get(path)
    ]
    payload = {
        "schema_version": SCHEMA_VERSION,
        "status": "PASS" if not mismatches else "FAIL",
        "canonical": canonical.as_posix(),
        "installed": installed.as_posix(),
        "file_count": len(all_paths),
        "mismatches": mismatches,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if not mismatches else 2


if __name__ == "__main__":
    raise SystemExit(main())
