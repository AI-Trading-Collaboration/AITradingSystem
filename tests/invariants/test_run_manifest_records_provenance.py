"""Invariant G2: a run manifest says which code and which exact configuration produced the result.

Manifests used to record configuration *paths* only, so a result could not be tied to the content
that produced it. The manifest must carry the commit, whether result-affecting code was modified,
and a content hash for every configuration file.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from ai_trading_system import report_traceability


def _manifest(config_paths: dict[str, Path]) -> dict:
    return report_traceability._run_manifest(
        run_id="run-1",
        command="aits test",
        date_window={"start": "2021-02-22", "end": "2026-04-30"},
        market_regime=None,
        config_paths=config_paths,
        output_artifacts=[],
    )


def test_manifest_binds_the_result_to_config_content(tmp_path: Path) -> None:
    config = tmp_path / "rules.yaml"
    config.write_bytes(b"threshold: 0.5\n")
    provenance = _manifest({"rules": config})["provenance"]
    assert provenance["config_sha256"]["rules"] == hashlib.sha256(b"threshold: 0.5\n").hexdigest()


def test_changing_a_config_changes_the_recorded_hash(tmp_path: Path) -> None:
    config = tmp_path / "rules.yaml"
    config.write_bytes(b"threshold: 0.5\n")
    before = _manifest({"rules": config})["provenance"]["config_sha256"]["rules"]
    config.write_bytes(b"threshold: 0.6\n")
    after = _manifest({"rules": config})["provenance"]["config_sha256"]["rules"]
    assert before != after


def test_manifest_names_the_commit_and_whether_code_was_modified() -> None:
    config = Path(__file__).resolve().parents[2] / "config" / "scoring_rules.yaml"
    git = _manifest({"scoring_rules": config})["provenance"]["git"]
    assert set(git) == {"commit", "branch", "code_modified"}
    assert len(git["commit"]) == 40
    assert isinstance(git["code_modified"], bool)
