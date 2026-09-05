from __future__ import annotations

import json
import os
import subprocess
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from test_data_download_publication import PUBLISHED_AT, _publish
from test_data_quality_execution import AS_OF as DQ_AS_OF
from test_data_quality_execution import ExecutionFixture, _install_report_spy
from test_data_quality_execution import execution_fixture as execution_fixture
from test_immutable_data_publish import DATASET_ID, GENERATED_AT, _case

from ai_trading_system.data import download_publication as download
from ai_trading_system.data import immutable_publish as immutable
from ai_trading_system.data.download_publication import (
    DownloadPublicationIntegrityError,
    ValidatedDownloadPublication,
    resolve_named_download_publication,
)
from ai_trading_system.data.immutable_publish import (
    DataPublicationIntegrityError,
    ValidatedCurrentSnapshot,
    ValidatedNamedSnapshot,
    ValidatedSnapshot,
    publish_immutable_snapshot,
    validate_current_snapshot,
    validate_named_snapshot,
)
from ai_trading_system.data.quality_execution import (
    DataQualityExecutionError,
    run_canonical_data_quality_execution,
    verify_data_quality_execution_receipt,
)
from ai_trading_system.platform.artifacts import canonical_json_bytes, sha256_bytes


def _publish_snapshot(root: Path, generation: int = 1) -> ValidatedCurrentSnapshot:
    payload = f"synthetic snapshot generation {generation}\n".encode()
    request, _ = _case(
        root,
        payload,
        run_id=f"named-test-{generation}",
        generated_at=GENERATED_AT + timedelta(minutes=generation),
    )
    return publish_immutable_snapshot(
        store_root=root / "store",
        evidence_root=root,
        request=request,
        payload=payload,
    ).snapshot


def _named(root: Path, selected: ValidatedCurrentSnapshot) -> ValidatedNamedSnapshot:
    return validate_named_snapshot(
        store_root=root / "store",
        dataset_id=selected.dataset_id,
        pointer_id=selected.pointer_id,
        expected_pointer_sha256=selected.pointer_sha256,
    )


def _download_arguments(root: Path, publication: ValidatedDownloadPublication) -> dict[str, Any]:
    pointer = json.loads(publication.discovery_pointer_path.read_bytes())
    return {
        "output_dir": root,
        "pointer_id": pointer["pointer_id"],
        "expected_pointer_sha256": publication.discovery_pointer_sha256,
        "expected_transaction_id": publication.transaction_id,
        "expected_transaction_sha256": publication.transaction_manifest_sha256,
    }


def _tree_inventory(root: Path) -> dict[str, tuple[int, int, str]]:
    return {
        path.relative_to(root).as_posix(): (
            path.stat().st_size,
            path.stat().st_mtime_ns,
            sha256_bytes(path.read_bytes()),
        )
        for path in root.rglob("*")
        if path.is_file()
    }


def test_named_current_has_separate_type_and_exact_commit_anchor(tmp_path: Path) -> None:
    current = _publish_snapshot(tmp_path)

    named = _named(tmp_path, current)

    assert isinstance(named, ValidatedSnapshot)
    assert isinstance(current, ValidatedSnapshot)
    assert isinstance(named, ValidatedNamedSnapshot)
    assert not isinstance(named, ValidatedCurrentSnapshot)
    assert not issubclass(ValidatedNamedSnapshot, ValidatedCurrentSnapshot)
    assert named.pointer_id == current.pointer_id
    assert named.pointer_sha256 == current.pointer_sha256
    assert named.payload_path == current.payload_path
    assert named.envelope == current.envelope
    assert named.pointer_path == (
        tmp_path / "store/pointer_history" / DATASET_ID / f"{current.pointer_id}.json"
    )
    anchor = named.commit_anchor
    assert anchor.dataset_id == current.dataset_id
    assert anchor.pointer_id == current.pointer_id
    assert anchor.pointer_sha256 == current.pointer_sha256
    assert anchor.pointer_path == current.pointer_path
    assert anchor.generation == current.generation


def test_named_ancestor_remains_fixed_when_current_advances(tmp_path: Path) -> None:
    first = _publish_snapshot(tmp_path)
    first_bytes = first.payload_path.read_bytes()
    second = _publish_snapshot(tmp_path, 2)
    third = _publish_snapshot(tmp_path, 3)

    named = _named(tmp_path, first)

    assert named.generation == 1
    assert named.pointer_id == first.pointer_id
    assert named.pointer_sha256 == first.pointer_sha256
    assert named.payload_path.read_bytes() == first_bytes
    assert named.payload_path not in {second.payload_path, third.payload_path}
    assert named.commit_anchor.pointer_id == third.pointer_id
    assert named.commit_anchor.generation == 3


@pytest.mark.parametrize("existing_anchor", [False, True])
def test_complete_precommit_rejected_orphan_is_not_a_committed_snapshot(
    tmp_path: Path, existing_anchor: bool
) -> None:
    current = _publish_snapshot(tmp_path) if existing_anchor else None
    store = tmp_path / "store"
    history_root = store / "pointer_history" / DATASET_ID
    before = set(history_root.iterdir()) if history_root.exists() else set()
    payload = b"complete synthetic orphan after reference validation\n"
    request, _ = _case(
        tmp_path,
        payload,
        run_id="named-orphan-rejected-before-commit",
        generated_at=GENERATED_AT + timedelta(minutes=2),
    )
    validated_orphans: list[Path] = []

    def reject_after_validation() -> None:
        candidates = set(history_root.iterdir()) - before
        assert len(candidates) == 1
        history = candidates.pop()
        pointer = json.loads(history.read_bytes())
        # The real publisher has already validated this entire installed candidate.
        for role in ("snapshot", "manifest", "source_event"):
            artifact = store / pointer[role]["path"]
            assert sha256_bytes(artifact.read_bytes()) == pointer[role]["sha256"]
        validated_orphans.append(history)
        raise RuntimeError("synthetic precommit owner precondition rejected")

    with pytest.raises(RuntimeError, match="synthetic precommit owner precondition rejected"):
        publish_immutable_snapshot(
            store_root=store,
            evidence_root=tmp_path,
            request=request,
            payload=payload,
            pre_commit_validator=reject_after_validation,
        )
    history = validated_orphans[0]
    raw = history.read_bytes()
    with pytest.raises(DataPublicationIntegrityError):
        validate_named_snapshot(
            store_root=store,
            dataset_id=DATASET_ID,
            pointer_id=history.stem,
            expected_pointer_sha256=sha256_bytes(raw),
        )
    if current is not None:
        assert (
            validate_current_snapshot(
                store_root=store, evidence_root=tmp_path, dataset_id=DATASET_ID
            )
            == current
        )
    else:
        assert not (store / "current" / f"{DATASET_ID}.json").exists()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("dataset_id", "other_dataset"),
        ("dataset_id", "../outside"),
        ("pointer_id", "data_pointer_" + "0" * 32),
        ("pointer_id", "../outside"),
        ("pointer_id", "data_pointer_" + "A" * 32),
        ("expected_pointer_sha256", "0" * 64),
        ("expected_pointer_sha256", "invalid-sha"),
    ],
)
def test_named_rejects_missing_misbound_or_unsafe_explicit_identity_without_fallback(
    tmp_path: Path, field: str, value: str
) -> None:
    current = _publish_snapshot(tmp_path)
    arguments: dict[str, Any] = {
        "store_root": tmp_path / "store",
        "dataset_id": current.dataset_id,
        "pointer_id": current.pointer_id,
        "expected_pointer_sha256": current.pointer_sha256,
    }
    arguments[field] = value

    with pytest.raises((ValueError, DataPublicationIntegrityError)):
        validate_named_snapshot(**arguments)

    assert (
        current.pointer_path.read_bytes()
        == (
            tmp_path / "store/pointer_history" / DATASET_ID / f"{current.pointer_id}.json"
        ).read_bytes()
    )


@pytest.mark.parametrize("role", ["payload_path", "source_event_path", "manifest_path"])
def test_named_rejects_selected_ancestor_artifact_tamper(tmp_path: Path, role: str) -> None:
    first = _publish_snapshot(tmp_path)
    _publish_snapshot(tmp_path, 2)
    path: Path = getattr(first, role)
    path.write_bytes(path.read_bytes() + b"tampered")

    with pytest.raises(DataPublicationIntegrityError):
        _named(tmp_path, first)


@pytest.mark.parametrize("mutation", ["missing", "generation_gap", "predecessor_sha", "cycle"])
def test_named_rejects_broken_history_chain(tmp_path: Path, mutation: str) -> None:
    first = _publish_snapshot(tmp_path)
    second = _publish_snapshot(tmp_path, 2)
    _publish_snapshot(tmp_path, 3)
    path = tmp_path / "store/pointer_history" / DATASET_ID / f"{second.pointer_id}.json"
    if mutation == "missing":
        path.unlink()
    else:
        pointer = json.loads(path.read_bytes())
        if mutation == "generation_gap":
            pointer["generation"] = 7
        elif mutation == "predecessor_sha":
            pointer["previous_pointer_sha256"] = "0" * 64
        else:
            pointer["previous_pointer_id"] = pointer["pointer_id"]
            pointer["previous_pointer_sha256"] = second.pointer_sha256
        path.write_bytes(canonical_json_bytes(pointer))

    with pytest.raises(DataPublicationIntegrityError):
        _named(tmp_path, first)


@pytest.mark.parametrize("changed_role", ["selected", "anchor"])
def test_named_rechecks_selected_and_commit_anchor_after_reference_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, changed_role: str
) -> None:
    first = _publish_snapshot(tmp_path)
    second = _publish_snapshot(tmp_path, 2)
    selected_path = tmp_path / "store/pointer_history" / DATASET_ID / f"{first.pointer_id}.json"
    original_validate = immutable._validate_references
    changes: list[Path] = []

    def replace_after_validation(root: Path, pointer: Any):
        result = original_validate(root, pointer)
        if pointer["pointer_id"] == first.pointer_id:
            target = selected_path if changed_role == "selected" else second.pointer_path
            target.write_bytes(target.read_bytes() + b"\n")
            changes.append(target)
        return result

    monkeypatch.setattr(immutable, "_validate_references", replace_after_validation)
    with pytest.raises(DataPublicationIntegrityError):
        _named(tmp_path, first)
    assert len(changes) == 1


@pytest.mark.parametrize("target_kind", ["selected", "anchor", "payload", "quality"])
def test_named_inherits_multiple_link_rejection(tmp_path: Path, target_kind: str) -> None:
    first = _publish_snapshot(tmp_path)
    second = _publish_snapshot(tmp_path, 2)
    quality = first.envelope.data_quality
    assert quality is not None and quality.report_path is not None
    targets = {
        "selected": tmp_path / "store/pointer_history" / DATASET_ID / f"{first.pointer_id}.json",
        "anchor": second.pointer_path,
        "payload": first.payload_path,
        "quality": tmp_path / "store" / quality.report_path,
    }
    external = tmp_path / "extra-hardlink.bin"
    os.link(targets[target_kind], external)

    with pytest.raises(DataPublicationIntegrityError, match="ARTIFACT_MULTIPLE_LINKS"):
        _named(tmp_path, first)
    assert external.stat().st_nlink == 2


def test_named_rejects_reparse_history_directory_without_reading_target(tmp_path: Path) -> None:
    first = _publish_snapshot(tmp_path)
    history_root = tmp_path / "store/pointer_history" / DATASET_ID
    displaced = tmp_path / "displaced-synthetic-history"
    history_root.rename(displaced)
    if os.name == "nt":
        created = subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(history_root), str(displaced)],
            capture_output=True,
            text=True,
            check=False,
        )
        if created.returncode:
            pytest.skip(f"junction creation unavailable: {created.stderr}")
    else:
        history_root.symlink_to(displaced, target_is_directory=True)

    with pytest.raises(DataPublicationIntegrityError, match="ARTIFACT_PATH_REPARSE_POINT"):
        _named(tmp_path, first)
    assert sha256_bytes((displaced / f"{first.pointer_id}.json").read_bytes()) == (
        first.pointer_sha256
    )


def test_named_download_returns_structural_wrapper_without_current_type(tmp_path: Path) -> None:
    publication = _publish(tmp_path, include_secondary=True)
    result = resolve_named_download_publication(**_download_arguments(tmp_path, publication))

    assert not isinstance(result, ValidatedDownloadPublication)
    assert isinstance(result.snapshot, ValidatedNamedSnapshot)
    assert result.publication.transaction_id == publication.transaction_id
    assert result.publication.transaction_manifest_sha256 == publication.transaction_manifest_sha256
    assert result.publication.prices_path == publication.prices_path
    assert result.publication.rates_path == publication.rates_path
    assert result.publication.secondary_prices_path == publication.secondary_prices_path
    assert result.publication.legacy_projection_verified is False
    assert result.validation_scope == "STRUCTURAL_PUBLICATION_ONLY"
    assert result.legacy_projection_status == "NOT_EVALUATED"
    assert result.dispatch_allowed is False
    assert result.consumer_cutover_allowed is False
    assert result.production_effect == "none"


@pytest.mark.parametrize("legacy_state", ["changed", "missing"])
def test_named_download_ancestor_ignores_legacy_and_current_advancement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, legacy_state: str
) -> None:
    first = _publish(tmp_path, include_secondary=True)
    arguments = _download_arguments(tmp_path, first)
    second = _publish(tmp_path, published_at=PUBLISHED_AT + timedelta(minutes=1))
    for path in (
        first.legacy_prices_path,
        first.legacy_rates_path,
        first.legacy_manifest_path,
        first.legacy_secondary_prices_path,
    ):
        assert path is not None
        if legacy_state == "changed":
            path.write_bytes(b"mutable legacy bytes are not named input")
        else:
            path.unlink(missing_ok=True)
    original_read = download._read_required

    def forbid_legacy_read(root: Path, relative_path: str, code: str) -> bytes:
        assert relative_path.startswith(".download_publications/")
        return original_read(root, relative_path, code)

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("named lookup invoked legacy/current resolver")

    monkeypatch.setattr(download, "_read_required", forbid_legacy_read)
    monkeypatch.setattr(download, "_legacy_projection_matches", forbidden)
    monkeypatch.setattr(download, "resolve_download_publication", forbidden)
    result = resolve_named_download_publication(**arguments)

    assert result.publication.transaction_id == first.transaction_id != second.transaction_id
    assert result.publication.transaction_manifest_path == first.transaction_manifest_path
    assert result.snapshot.commit_anchor.pointer_sha256 == second.discovery_pointer_sha256
    assert result.publication.legacy_projection_verified is False
    assert result.legacy_projection_status == "NOT_EVALUATED"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("pointer_id", "data_pointer_" + "0" * 32),
        ("pointer_id", "../outside"),
        ("expected_pointer_sha256", "0" * 64),
        ("expected_transaction_id", "download_txn_" + "0" * 32),
        ("expected_transaction_id", "../outside"),
        ("expected_transaction_sha256", "0" * 64),
        ("expected_transaction_sha256", "not-a-sha"),
    ],
)
def test_named_download_rejects_explicit_identity_mismatch(
    tmp_path: Path, field: str, value: str
) -> None:
    publication = _publish(tmp_path)
    arguments = _download_arguments(tmp_path, publication)
    arguments[field] = value

    with pytest.raises((ValueError, DownloadPublicationIntegrityError)):
        resolve_named_download_publication(**arguments)


@pytest.mark.parametrize(
    "role", ["prices_path", "rates_path", "manifest_path", "secondary_prices_path"]
)
def test_named_download_revalidates_every_immutable_member(tmp_path: Path, role: str) -> None:
    publication = _publish(tmp_path, include_secondary=True)
    arguments = _download_arguments(tmp_path, publication)
    member: Path = getattr(publication, role)
    member.write_bytes(member.read_bytes() + b"tampered")

    with pytest.raises(DownloadPublicationIntegrityError):
        resolve_named_download_publication(**arguments)


@pytest.mark.parametrize("role", ["pointer_path", "manifest_path", "payload_path"])
def test_named_download_rejects_bytes_changed_on_second_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, role: str
) -> None:
    publication = _publish(tmp_path)
    arguments = _download_arguments(tmp_path, publication)
    selected = immutable.validate_named_snapshot(
        store_root=tmp_path / ".download_publications",
        dataset_id="download_composite",
        pointer_id=arguments["pointer_id"],
        expected_pointer_sha256=arguments["expected_pointer_sha256"],
    )
    target = getattr(selected, role).relative_to(tmp_path).as_posix()
    original_read = download._read_required
    changes: list[str] = []

    def changed_second_read(root: Path, relative_path: str, code: str) -> bytes:
        raw = original_read(root, relative_path, code)
        if relative_path == target:
            changes.append(relative_path)
            return raw + b"\n"
        return raw

    monkeypatch.setattr(download, "_read_required", changed_second_read)
    with pytest.raises(DownloadPublicationIntegrityError):
        resolve_named_download_publication(**arguments)
    assert changes


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("validation_scope", "CANONICAL_DQ_PASS"),
        ("legacy_projection_status", "PASS"),
        ("dispatch_allowed", True),
        ("consumer_cutover_allowed", True),
        ("production_effect", "production"),
    ],
)
def test_named_download_wrapper_rejects_permission_upgrade(
    tmp_path: Path, field: str, value: Any
) -> None:
    publication = _publish(tmp_path)
    result = resolve_named_download_publication(**_download_arguments(tmp_path, publication))

    with pytest.raises((ValueError, DownloadPublicationIntegrityError)):
        replace(result, **{field: value})


@pytest.mark.parametrize(
    "mismatch", ["snapshot", "transaction_path", "pointer_sha", "legacy_verified"]
)
def test_named_download_wrapper_rejects_identity_or_inner_authority_mismatch(
    tmp_path: Path, mismatch: str
) -> None:
    first = _publish(tmp_path)
    first_arguments = _download_arguments(tmp_path, first)
    second = _publish(tmp_path, published_at=PUBLISHED_AT + timedelta(minutes=1))
    result = resolve_named_download_publication(**first_arguments)
    if mismatch == "snapshot":
        other = resolve_named_download_publication(**_download_arguments(tmp_path, second))
        replacement = {"snapshot": other.snapshot}
    elif mismatch == "transaction_path":
        replacement = {
            "publication": replace(
                result.publication, transaction_manifest_path=second.transaction_manifest_path
            )
        }
    elif mismatch == "pointer_sha":
        replacement = {
            "publication": replace(result.publication, discovery_pointer_sha256="0" * 64)
        }
    else:
        replacement = {"publication": replace(result.publication, legacy_projection_verified=True)}

    with pytest.raises((ValueError, DownloadPublicationIntegrityError)):
        replace(result, **replacement)


@pytest.mark.parametrize("missing_target", [False, True])
def test_named_download_success_and_failure_have_no_filesystem_side_effects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, missing_target: bool
) -> None:
    publication = _publish(tmp_path)
    arguments = _download_arguments(tmp_path, publication)
    if missing_target:
        arguments["pointer_id"] = "data_pointer_" + "0" * 32
    before = _tree_inventory(tmp_path)

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("read-only resolver attempted write, lock, discovery or legacy access")

    with monkeypatch.context() as guarded:
        guarded.setattr(immutable, "publish_immutable_snapshot", forbidden)
        guarded.setattr(immutable, "write_contained_artifact_bytes", forbidden)
        guarded.setattr(immutable, "_file_lock", forbidden)
        guarded.setattr(download, "publish_immutable_snapshot", forbidden)
        guarded.setattr(download, "write_contained_artifact_bytes", forbidden)
        guarded.setattr(download, "_legacy_projection_matches", forbidden)
        guarded.setattr(Path, "glob", forbidden)
        guarded.setattr(Path, "rglob", forbidden)
        if missing_target:
            with pytest.raises(DownloadPublicationIntegrityError):
                resolve_named_download_publication(**arguments)
        else:
            assert resolve_named_download_publication(**arguments).dispatch_allowed is False

    assert _tree_inventory(tmp_path) == before


def test_named_missing_store_does_not_create_any_directory(tmp_path: Path) -> None:
    root = tmp_path / "missing-publication"
    with pytest.raises((ValueError, DownloadPublicationIntegrityError)):
        resolve_named_download_publication(
            output_dir=root,
            pointer_id="data_pointer_" + "0" * 32,
            expected_pointer_sha256="0" * 64,
            expected_transaction_id="download_txn_" + "0" * 32,
            expected_transaction_sha256="0" * 64,
        )
    assert not root.exists()


@pytest.mark.parametrize(
    "mismatch",
    [
        "anchor_type",
        "same_generation_pointer_id",
        "same_generation_pointer_sha",
        "different_store",
        "selected_history_path",
        "bool_generation",
        "invalid_pointer_id",
        "relative_anchor_path",
    ],
)
def test_named_snapshot_and_wrapper_reject_incoherent_commit_anchor(
    tmp_path: Path, mismatch: str
) -> None:
    publication = _publish(tmp_path)
    result = resolve_named_download_publication(**_download_arguments(tmp_path, publication))
    named = result.snapshot
    anchor = named.commit_anchor

    with pytest.raises(
        (ValueError, DataPublicationIntegrityError, DownloadPublicationIntegrityError)
    ):
        if mismatch == "anchor_type":
            invalid_named = replace(named, commit_anchor=object())
        elif mismatch == "same_generation_pointer_id":
            invalid_named = replace(
                named, commit_anchor=replace(anchor, pointer_id="data_pointer_" + "0" * 32)
            )
        elif mismatch == "same_generation_pointer_sha":
            invalid_named = replace(named, commit_anchor=replace(anchor, pointer_sha256="0" * 64))
        elif mismatch == "different_store":
            invalid_named = replace(
                named,
                commit_anchor=replace(
                    anchor, pointer_path=tmp_path / "other-store/current/download_composite.json"
                ),
            )
        elif mismatch == "selected_history_path":
            invalid_named = replace(
                named, pointer_path=named.pointer_path.with_name("unbound.json")
            )
        elif mismatch == "bool_generation":
            invalid_named = replace(named, commit_anchor=replace(anchor, generation=True))
        elif mismatch == "invalid_pointer_id":
            invalid_named = replace(named, commit_anchor=replace(anchor, pointer_id="../outside"))
        else:
            invalid_named = replace(
                named,
                commit_anchor=replace(anchor, pointer_path=Path("current/download_composite.json")),
            )
        replace(result, snapshot=invalid_named)


@pytest.mark.parametrize(
    ("changed_role", "expected_code"),
    [
        ("anchor_advance", "DOWNLOAD_NAMED_COMMIT_ANCHOR_CHANGED"),
        ("anchor_replace", "DOWNLOAD_NAMED_COMMIT_ANCHOR_CHANGED"),
        ("selected", "DOWNLOAD_OUTER_POINTER_CHANGED"),
        ("outer_manifest", "DOWNLOAD_OUTER_PUBLICATION_BINDING_MISMATCH"),
    ],
)
def test_named_download_rebinds_anchor_and_selected_outer_after_transaction_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    changed_role: str,
    expected_code: str,
) -> None:
    first = _publish(tmp_path)
    arguments = _download_arguments(tmp_path, first)
    _publish(tmp_path, published_at=PUBLISHED_AT + timedelta(minutes=1))
    selected = validate_named_snapshot(
        store_root=tmp_path / ".download_publications",
        dataset_id="download_composite",
        pointer_id=arguments["pointer_id"],
        expected_pointer_sha256=arguments["expected_pointer_sha256"],
    )
    original_validate = download._validate_transaction
    changes: list[str] = []

    def change_after_transaction(*args: Any, **kwargs: Any):
        result = original_validate(*args, **kwargs)
        if not changes:
            # Set the marker before publishing: the real publisher validates its
            # own synthetic generation through this same shared helper.
            changes.append(changed_role)
            if changed_role == "anchor_advance":
                _publish(tmp_path, published_at=PUBLISHED_AT + timedelta(minutes=2))
            else:
                target = {
                    "anchor_replace": selected.commit_anchor.pointer_path,
                    "selected": selected.pointer_path,
                    "outer_manifest": selected.manifest_path,
                }[changed_role]
                target.write_bytes(target.read_bytes() + b"\n")
        return result

    monkeypatch.setattr(download, "_validate_transaction", change_after_transaction)
    with pytest.raises(DownloadPublicationIntegrityError, match=expected_code):
        resolve_named_download_publication(**arguments)
    assert changes == [changed_role]


def test_immutable_validator_source_change_invalidates_existing_synthetic_dq_receipt(
    execution_fixture: ExecutionFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = _install_report_spy(monkeypatch, status="PASS")
    result = run_canonical_data_quality_execution(
        execution_fixture.request, project_root=execution_fixture.root
    )
    source = execution_fixture.root / "src/ai_trading_system/data/immutable_publish.py"
    assert source.is_relative_to(execution_fixture.root)
    assert any(
        binding.path == "src/ai_trading_system/data/immutable_publish.py"
        for binding in result.receipt.validator.implementation_sources
    )
    receipt_bytes = result.receipt_path.read_bytes()
    source.write_bytes(source.read_bytes() + b"\n# synthetic future publication implementation\n")

    with pytest.raises(DataQualityExecutionError, match="DQ_VALIDATOR_SHA_MISMATCH"):
        verify_data_quality_execution_receipt(
            result.receipt_path,
            expected_as_of=DQ_AS_OF,
            expected_policy_path=execution_fixture.policy_path,
            expected_input_roles=("prices", "rates"),
            project_root=execution_fixture.root,
        )
    assert len(calls) == 1  # The synthetic spy ran once; verification never reruns DQ.
    assert result.receipt_path.read_bytes() == receipt_bytes
