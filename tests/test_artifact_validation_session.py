from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from ai_trading_system.platform.artifacts.validation_session import (
    artifact_validation_session,
    cached_artifact_validation,
)


def test_validation_session_reuses_only_unchanged_pass_artifact(tmp_path: Path) -> None:
    artifact_id = "artifact-1"
    artifact_root = tmp_path / artifact_id
    artifact_root.mkdir()
    payload_path = artifact_root / "payload.txt"
    payload_path.write_text("first", encoding="utf-8")
    calls: list[str] = []

    def validator(*, item_id: str, output_dir: Path) -> dict[str, Any]:
        content = (output_dir / item_id / "payload.txt").read_text(encoding="utf-8")
        calls.append(content)
        return {"status": "PASS", "details": {"content": content}}

    with artifact_validation_session():
        first = cached_artifact_validation(
            validator=validator,
            validator_key="item_id",
            artifact_id=artifact_id,
            root=tmp_path,
        )
        first["details"]["content"] = "caller-mutation"
        cached = cached_artifact_validation(
            validator=validator,
            validator_key="item_id",
            artifact_id=artifact_id,
            root=tmp_path,
        )
        payload_path.write_text("second", encoding="utf-8")
        changed = cached_artifact_validation(
            validator=validator,
            validator_key="item_id",
            artifact_id=artifact_id,
            root=tmp_path,
        )

    assert calls == ["first", "second"]
    assert cached["details"]["content"] == "first"
    assert changed["details"]["content"] == "second"


def test_validation_session_never_reuses_failure(tmp_path: Path) -> None:
    artifact_id = "artifact-1"
    artifact_root = tmp_path / artifact_id
    artifact_root.mkdir()
    (artifact_root / "payload.txt").write_text("invalid", encoding="utf-8")
    calls = 0

    def validator(*, item_id: str, output_dir: Path) -> dict[str, Any]:
        nonlocal calls
        assert item_id == artifact_id
        assert output_dir == tmp_path
        calls += 1
        return {"status": "FAIL"}

    with artifact_validation_session():
        for _ in range(2):
            cached_artifact_validation(
                validator=validator,
                validator_key="item_id",
                artifact_id=artifact_id,
                root=tmp_path,
            )

    assert calls == 2


def test_validation_session_invalidates_when_transitive_bound_source_changes(
    tmp_path: Path,
) -> None:
    artifact_id = "artifact-1"
    artifact_root = tmp_path / artifact_id
    artifact_root.mkdir()
    source_path = tmp_path / "live-source.csv"
    source_path.write_text("first", encoding="utf-8")
    source_bytes = source_path.read_bytes()
    snapshot = {
        "schema_version": "test.snapshot.v1",
        "source": {
            "path": str(source_path.resolve()),
            "sha256": hashlib.sha256(source_bytes).hexdigest(),
            "size_bytes": len(source_bytes),
        },
    }
    (artifact_root / "input_snapshot.json").write_text(
        json.dumps(snapshot, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    calls: list[str] = []

    def validator(*, item_id: str, output_dir: Path) -> dict[str, Any]:
        content = source_path.read_text(encoding="utf-8")
        calls.append(content)
        return {"status": "PASS", "item_id": item_id, "source": content}

    with artifact_validation_session():
        first = cached_artifact_validation(
            validator=validator,
            validator_key="item_id",
            artifact_id=artifact_id,
            root=tmp_path,
        )
        source_path.write_text("second", encoding="utf-8")
        changed = cached_artifact_validation(
            validator=validator,
            validator_key="item_id",
            artifact_id=artifact_id,
            root=tmp_path,
        )
        source_path.write_bytes(source_bytes)
        restored = cached_artifact_validation(
            validator=validator,
            validator_key="item_id",
            artifact_id=artifact_id,
            root=tmp_path,
        )

    assert calls == ["first", "second"]
    assert first["source"] == "first"
    assert changed["source"] == "second"
    assert restored["source"] == "first"
