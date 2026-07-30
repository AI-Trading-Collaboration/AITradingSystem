from __future__ import annotations

import copy
import hashlib
from pathlib import Path

import pytest
import yaml

from ai_trading_system.atlas.source_projection import (
    AtlasSourceProjectionError,
    load_source_registry,
    project_source_refs,
)
from ai_trading_system.contracts import ExplorerSourceKind

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REGISTRY_PATH = PROJECT_ROOT / "config" / "atlas" / "source_registry.yaml"
EXACT_COMMIT = "adfd3d5817a9797c35f97d01b92ced2e01663373"


def _registry_payload() -> dict[str, object]:
    payload = yaml.safe_load(REGISTRY_PATH.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _write_registry(tmp_path: Path, payload: dict[str, object]) -> Path:
    path = tmp_path / "registry.yaml"
    path.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    return path


def test_projected_sources_bind_exact_commit_and_content_sha() -> None:
    registry = load_source_registry(REGISTRY_PATH)
    sources = project_source_refs(
        repository_root=PROJECT_ROOT,
        registry=registry,
        exact_commit=EXACT_COMMIT,
    )
    assert len(sources) == 6
    assert len({item.source_ref_id for item in sources}) == len(sources)
    assert all(item.exact_commit == EXACT_COMMIT for item in sources)
    assert all(item.source_kind is ExplorerSourceKind.GIT_AUTHORITY for item in sources)
    for item in sources:
        assert (
            item.content_sha256
            == hashlib.sha256((PROJECT_ROOT / item.source_path).read_bytes()).hexdigest()
        )


def test_registry_rejects_source_path_traversal(tmp_path: Path) -> None:
    payload = copy.deepcopy(_registry_payload())
    payload["sources"][0]["source_path"] = "../secrets.txt"
    registry = load_source_registry(_write_registry(tmp_path, payload))
    with pytest.raises(
        AtlasSourceProjectionError,
        match="ATLAS_SOURCE_PATH_INVALID",
    ):
        project_source_refs(
            repository_root=PROJECT_ROOT,
            registry=registry,
            exact_commit=EXACT_COMMIT,
        )


def test_registry_rejects_duplicate_source_identity(tmp_path: Path) -> None:
    payload = copy.deepcopy(_registry_payload())
    payload["sources"][1]["source_ref_id"] = payload["sources"][0]["source_ref_id"]
    registry = load_source_registry(_write_registry(tmp_path, payload))
    with pytest.raises(
        AtlasSourceProjectionError,
        match="ATLAS_DUPLICATE_SOURCE_REF_ID",
    ):
        project_source_refs(
            repository_root=PROJECT_ROOT,
            registry=registry,
            exact_commit=EXACT_COMMIT,
        )


def test_registry_rejects_invalid_commit_identity() -> None:
    registry = load_source_registry(REGISTRY_PATH)
    with pytest.raises(
        AtlasSourceProjectionError,
        match="ATLAS_EXACT_COMMIT_INVALID",
    ):
        project_source_refs(
            repository_root=PROJECT_ROOT,
            registry=registry,
            exact_commit="main",
        )
