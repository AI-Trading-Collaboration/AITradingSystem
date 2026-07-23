from __future__ import annotations

import csv
import io
import json
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from ai_trading_system.data import download_publication as publication_module
from ai_trading_system.data.download_publication import (
    DownloadArtifactCandidate,
    DownloadLegacyProjectionError,
    DownloadPublicationIntegrityError,
    DownloadSourceBinding,
    publish_download_transaction,
    resolve_download_publication,
    resolve_download_publication_if_present,
)
from ai_trading_system.data.immutable_publish import validate_current_snapshot

PUBLISHED_AT = datetime(2026, 7, 23, 2, 0, tzinfo=UTC)
START = date(2026, 4, 29)
END = date(2026, 4, 30)


def test_publish_uses_public_immutable_current_for_canonical_composite(
    tmp_path: Path,
) -> None:
    result = _publish(tmp_path)
    resolved = resolve_download_publication(output_dir=tmp_path)
    current = validate_current_snapshot(
        store_root=tmp_path / ".download_publications",
        evidence_root=tmp_path,
        dataset_id="download_composite",
    )

    assert resolved == result
    assert result.transaction_manifest_path == current.payload_path
    assert result.discovery_pointer_path == current.pointer_path
    assert result.prices_path.parent.parent.name == "members"
    assert result.rates_path.parent.parent.name == "members"
    assert result.manifest_path.parent.parent.name == "members"
    assert result.legacy_projection_verified is True
    assert result.atomicity_scope == "IMMUTABLE_GENERATION_DISCOVERY_POINTER_ONLY"
    assert result.legacy_projection_role == "COMPATIBILITY_ONLY"
    assert result.legacy_projection_atomicity == "NOT_GUARANTEED"
    assert result.consumer_cutover_allowed is False
    assert result.production_effect == "none"

    transaction = json.loads(result.transaction_manifest_path.read_text(encoding="utf-8"))
    assert transaction["schema_version"] == "download_publication_transaction.v1"
    assert transaction["discovery_schema_version"] == "data_current_pointer.v1"
    assert transaction["validation_scope"] == "STRUCTURAL_PUBLICATION_ONLY"
    assert transaction["dq_execution_provenance_verified"] is False
    assert transaction["consumer_cutover_allowed"] is False
    assert transaction["production_effect"] == "none"
    assert transaction["legacy_projection_atomicity"] == "NOT_GUARANTEED"

    outer_manifest = json.loads(current.manifest_path.read_text(encoding="utf-8"))
    assert outer_manifest["dq_execution_provenance_verified"] is False
    assert outer_manifest["consumer_cutover_allowed"] is False


def test_composite_artifact_has_one_row_and_separate_source_events(tmp_path: Path) -> None:
    prices = _prices_bytes(("QQQ", "VIX"))
    result = publish_download_transaction(
        output_dir=tmp_path,
        requested_start=START,
        requested_end=END,
        published_at=PUBLISHED_AT,
        artifacts=(
            DownloadArtifactCandidate(
                role="prices",
                filename="prices_daily.csv",
                content=prices,
                row_count=2,
                source_event_ids=("prices:cboe", "prices:primary"),
            ),
            _rates_artifact(),
        ),
        source_bindings=(
            DownloadSourceBinding(
                source_event_id="prices:primary",
                artifact_role="prices",
                source_kind="LIVE_PROVIDER",
                source_id="primary",
                provider="Primary",
                endpoint="primary",
                request_parameters={"tickers": ["QQQ"]},
                winning_row_count=1,
                allocation_mode="EXPLICIT_KEYS",
                winning_row_keys=(("QQQ", "2026-04-30"),),
            ),
            DownloadSourceBinding(
                source_event_id="prices:cboe",
                artifact_role="prices",
                source_kind="LIVE_PROVIDER",
                source_id="cboe",
                provider="CBOE",
                endpoint="cboe",
                request_parameters={"tickers": ["VIX"]},
                winning_row_count=1,
                allocation_mode="REMAINDER",
                winning_row_keys=(("VIX", "2026-04-30"),),
            ),
            _rates_source(),
        ),
    )

    transaction = json.loads(result.transaction_manifest_path.read_text(encoding="utf-8"))
    price_artifacts = [item for item in transaction["artifacts"] if item["role"] == "prices"]
    price_events = [
        item for item in transaction["source_event_records"] if item["artifact_role"] == "prices"
    ]
    legacy = pd.read_csv(result.legacy_manifest_path)
    legacy_prices = legacy.loc[legacy["output_path"].astype(str) == str(result.legacy_prices_path)]

    assert len(price_artifacts) == 1
    assert price_artifacts[0]["row_count"] == 2
    assert price_artifacts[0]["source_event_ids"] == [
        "prices:cboe",
        "prices:primary",
    ]
    assert {item["source_id"] for item in price_events} == {"cboe", "primary"}
    assert {item["winning_row_count"] for item in price_events} == {1}
    assert len(legacy_prices) == 1
    assert legacy_prices.iloc[0]["row_count"] == 2
    assert legacy_prices.iloc[0]["source_id"] == "composite_prices_publication"


def test_row_count_mismatch_fails_before_canonical_or_legacy_output(tmp_path: Path) -> None:
    bad = DownloadArtifactCandidate(
        role="prices",
        filename="prices_daily.csv",
        content=_prices_bytes(("QQQ",)),
        row_count=2,
        source_event_ids=("prices:primary",),
    )

    with pytest.raises(
        DownloadPublicationIntegrityError,
        match="DOWNLOAD_ARTIFACT_ROW_COUNT_MISMATCH",
    ):
        publish_download_transaction(
            output_dir=tmp_path,
            requested_start=START,
            requested_end=END,
            published_at=PUBLISHED_AT,
            artifacts=(bad, _rates_artifact()),
            source_bindings=(_prices_source(), _rates_source()),
        )

    assert not (tmp_path / ".download_publications/current/download_composite.json").exists()
    assert not (tmp_path / "prices_daily.csv").exists()
    assert not (tmp_path / "download_manifest.csv").exists()


def test_malformed_existing_manifest_fails_without_overwriting_legacy(
    tmp_path: Path,
) -> None:
    old_prices = b"legacy-prices"
    (tmp_path / "prices_daily.csv").write_bytes(old_prices)
    (tmp_path / "download_manifest.csv").write_text("wrong,columns\n1,2\n", encoding="utf-8")

    with pytest.raises(
        DownloadPublicationIntegrityError,
        match="DOWNLOAD_MANIFEST_INVALID",
    ):
        _publish(tmp_path)

    assert (tmp_path / "prices_daily.csv").read_bytes() == old_prices
    assert not (tmp_path / ".download_publications/current/download_composite.json").exists()


def test_current_manifest_semantic_mismatch_fails_before_any_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_append = publication_module._append_manifest

    def tampered_append(previous, records):
        raw = original_append(previous, records)
        return _rewrite_manifest_row(raw, row_index=-1, field="row_count", value="99")

    monkeypatch.setattr(publication_module, "_append_manifest", tampered_append)

    with pytest.raises(
        DownloadPublicationIntegrityError,
        match="DOWNLOAD_MANIFEST_CURRENT_GENERATION_MISMATCH",
    ):
        _publish(tmp_path)

    assert not (tmp_path / ".download_publications/current/download_composite.json").exists()
    assert not (tmp_path / "prices_daily.csv").exists()
    assert not (tmp_path / "rates_daily.csv").exists()
    assert not (tmp_path / "download_manifest.csv").exists()


def test_current_manifest_transaction_must_be_unique_before_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_append = publication_module._append_manifest

    def duplicated_append(previous, records):
        raw = original_append(previous, records)
        return _duplicate_manifest_suffix(raw, suffix_size=len(records))

    monkeypatch.setattr(publication_module, "_append_manifest", duplicated_append)

    with pytest.raises(
        DownloadPublicationIntegrityError,
        match="DOWNLOAD_MANIFEST_CURRENT_TRANSACTION_DUPLICATE",
    ):
        _publish(tmp_path)

    assert not (tmp_path / ".download_publications/current/download_composite.json").exists()
    assert not (tmp_path / "download_manifest.csv").exists()


def test_member_tamper_invalidates_canonical_resolver(tmp_path: Path) -> None:
    result = _publish(tmp_path)
    result.prices_path.write_bytes(b"tampered")

    with pytest.raises(
        DownloadPublicationIntegrityError,
        match="DOWNLOAD_ARTIFACT_BINDING_MISMATCH",
    ):
        resolve_download_publication(output_dir=tmp_path)


def test_resolver_rejects_manifest_semantics_even_with_matching_ref(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _publish(tmp_path)
    tampered_raw = _rewrite_manifest_row(
        result.manifest_path.read_bytes(),
        row_index=-1,
        field="row_count",
        value="99",
    )
    tampered_digest = publication_module._sha256(tampered_raw)
    tampered_relative = f".download_publications/members/{tampered_digest}/download_manifest.csv"
    original_parse = publication_module._strict_canonical_json
    original_read = publication_module._read_required

    def transaction_with_matching_manifest_ref(
        raw: bytes,
        *,
        schema: str,
        code: str,
    ) -> dict[str, object]:
        payload = original_parse(raw, schema=schema, code=code)
        if schema == publication_module.DOWNLOAD_PUBLICATION_SCHEMA_VERSION:
            manifest = payload["download_manifest"]
            assert isinstance(manifest, dict)
            manifest["path"] = tampered_relative
            manifest["sha256"] = tampered_digest
            manifest["size_bytes"] = len(tampered_raw)
        return payload

    def read_tampered_manifest(root: Path, relative_path: str, code: str) -> bytes:
        if relative_path == tampered_relative:
            return tampered_raw
        return original_read(root, relative_path, code)

    monkeypatch.setattr(
        publication_module,
        "_strict_canonical_json",
        transaction_with_matching_manifest_ref,
    )
    monkeypatch.setattr(publication_module, "_read_required", read_tampered_manifest)

    with pytest.raises(
        DownloadPublicationIntegrityError,
        match="DOWNLOAD_MANIFEST_CURRENT_GENERATION_MISMATCH",
    ):
        resolve_download_publication(output_dir=tmp_path)


def test_projection_failure_is_typed_after_pointer_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = publication_module._write_compatibility_artifact

    def fail_rates(root: Path, relative_path: str, content: bytes) -> None:
        if relative_path == "rates_daily.csv":
            raise OSError("injected legacy projection failure")
        original(root, relative_path, content)

    monkeypatch.setattr(publication_module, "_write_compatibility_artifact", fail_rates)

    with pytest.raises(DownloadLegacyProjectionError) as exc_info:
        _publish(tmp_path)

    assert exc_info.value.code == "DOWNLOAD_LEGACY_PROJECTION_FAILED"
    assert exc_info.value.commit_state == "POINTER_COMMITTED_PROJECTION_FAILED"
    assert (tmp_path / ".download_publications/current/download_composite.json").is_file()
    canonical = resolve_download_publication(output_dir=tmp_path)
    assert canonical.transaction_id.startswith("download_txn_")
    assert canonical.legacy_projection_verified is False
    assert canonical.prices_path.is_file()
    assert not canonical.legacy_rates_path.exists()


def test_source_contribution_underbind_fails_closed(tmp_path: Path) -> None:
    with pytest.raises(
        DownloadPublicationIntegrityError,
        match="DOWNLOAD_SOURCE_BINDING_MISMATCH",
    ):
        publish_download_transaction(
            output_dir=tmp_path,
            requested_start=START,
            requested_end=END,
            published_at=PUBLISHED_AT,
            artifacts=(
                DownloadArtifactCandidate(
                    role="prices",
                    filename="prices_daily.csv",
                    content=_prices_bytes(("QQQ", "TQQQ")),
                    row_count=2,
                    source_event_ids=("prices:primary",),
                ),
                _rates_artifact(),
            ),
            source_bindings=(
                DownloadSourceBinding(
                    source_event_id="prices:primary",
                    artifact_role="prices",
                    source_kind="LIVE_PROVIDER",
                    source_id="primary",
                    provider="Primary",
                    endpoint="primary",
                    request_parameters={"tickers": ["QQQ", "TQQQ"]},
                    winning_row_count=1,
                    allocation_mode="REMAINDER",
                    winning_row_keys=(("QQQ", "2026-04-30"),),
                ),
                _rates_source(),
            ),
        )

    assert not (tmp_path / ".download_publications/current/download_composite.json").exists()


@pytest.mark.parametrize("bad_row_count", [True, 1.0])
def test_artifact_row_count_requires_exact_int(
    tmp_path: Path,
    bad_row_count: object,
) -> None:
    class IntSubclass(int):
        pass

    values = (bad_row_count, IntSubclass(1)) if bad_row_count is True else (bad_row_count,)
    for index, value in enumerate(values):
        output = tmp_path / str(index)
        with pytest.raises(
            DownloadPublicationIntegrityError,
            match="DOWNLOAD_ARTIFACT_ROW_COUNT_MISMATCH",
        ):
            publish_download_transaction(
                output_dir=output,
                requested_start=START,
                requested_end=END,
                published_at=PUBLISHED_AT,
                artifacts=(
                    DownloadArtifactCandidate(
                        role="prices",
                        filename="prices_daily.csv",
                        content=_prices_bytes(("QQQ",)),
                        row_count=value,  # type: ignore[arg-type]
                        source_event_ids=("prices:primary",),
                    ),
                    _rates_artifact(),
                ),
                source_bindings=(_prices_source(), _rates_source()),
            )
        assert not (output / ".download_publications/current/download_composite.json").exists()


@pytest.mark.parametrize("bad_contribution", [True, 1.0])
def test_source_contribution_requires_exact_int(
    tmp_path: Path,
    bad_contribution: object,
) -> None:
    class IntSubclass(int):
        pass

    values = (bad_contribution, IntSubclass(1)) if bad_contribution is True else (bad_contribution,)
    for index, value in enumerate(values):
        output = tmp_path / str(index)
        bad_source = DownloadSourceBinding(
            source_event_id="prices:primary",
            artifact_role="prices",
            source_kind="LIVE_PROVIDER",
            source_id="primary",
            provider="Primary",
            endpoint="primary",
            request_parameters={},
            winning_row_count=value,  # type: ignore[arg-type]
            allocation_mode="REMAINDER",
            winning_row_keys=(("QQQ", "2026-04-30"),),
        )
        with pytest.raises(
            DownloadPublicationIntegrityError,
            match="DOWNLOAD_SOURCE_ROW_COUNT_MISMATCH",
        ):
            publish_download_transaction(
                output_dir=output,
                requested_start=START,
                requested_end=END,
                published_at=PUBLISHED_AT,
                artifacts=(_prices_artifact(), _rates_artifact()),
                source_bindings=(bad_source, _rates_source()),
            )
        assert not (output / ".download_publications/current/download_composite.json").exists()


def test_publication_base_reuses_one_validated_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _publish(tmp_path)
    current = validate_current_snapshot(
        store_root=tmp_path / ".download_publications",
        evidence_root=tmp_path,
        dataset_id="download_composite",
    )
    old_manifest_relative = first.manifest_path.relative_to(tmp_path).as_posix()
    original_read = publication_module._read_required
    old_manifest_reads = 0
    pointer_reads = 0

    def guarded_read(root: Path, relative_path: str, code: str) -> bytes:
        nonlocal old_manifest_reads, pointer_reads
        if relative_path == publication_module._CURRENT_POINTER:
            pointer_reads += 1
        if relative_path == old_manifest_relative:
            old_manifest_reads += 1
        return original_read(root, relative_path, code)

    monkeypatch.setattr(publication_module, "_read_required", guarded_read)
    second = _publish(tmp_path, published_at=PUBLISHED_AT + timedelta(minutes=1))
    transaction = json.loads(second.transaction_manifest_path.read_text(encoding="utf-8"))

    assert transaction["base_pointer_sha256"] == current.pointer_sha256
    assert old_manifest_reads == 1
    assert pointer_reads == 3


def test_resolver_if_present_does_not_create_missing_root(tmp_path: Path) -> None:
    missing = tmp_path / "not-created"

    assert resolve_download_publication_if_present(output_dir=missing) is None
    assert not missing.exists()


def test_resolver_rejects_pointer_change_after_public_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _publish(tmp_path)
    original_read = publication_module._read_required

    def changed_pointer(root: Path, relative_path: str, code: str) -> bytes:
        if relative_path == publication_module._CURRENT_POINTER:
            return b"{}\n"
        return original_read(root, relative_path, code)

    monkeypatch.setattr(publication_module, "_read_required", changed_pointer)

    with pytest.raises(
        DownloadPublicationIntegrityError,
        match="DOWNLOAD_OUTER_POINTER_CHANGED",
    ):
        resolve_download_publication(output_dir=tmp_path)


def test_resolver_rejects_outer_manifest_second_read_tamper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _publish(tmp_path)
    current = validate_current_snapshot(
        store_root=tmp_path / ".download_publications",
        evidence_root=tmp_path,
        dataset_id="download_composite",
    )
    manifest_relative = current.manifest_path.relative_to(tmp_path).as_posix()
    original_read = publication_module._read_required

    def tampered_manifest(root: Path, relative_path: str, code: str) -> bytes:
        raw = original_read(root, relative_path, code)
        return raw + b"\n" if relative_path == manifest_relative else raw

    monkeypatch.setattr(publication_module, "_read_required", tampered_manifest)

    with pytest.raises(
        DownloadPublicationIntegrityError,
        match="DOWNLOAD_OUTER_PUBLICATION_BINDING_MISMATCH",
    ):
        resolve_download_publication(output_dir=tmp_path)


def test_resolver_rejects_transaction_base_pointer_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _publish(tmp_path)
    _publish(tmp_path, published_at=PUBLISHED_AT + timedelta(minutes=1))
    original_parse = publication_module._strict_canonical_json

    def mismatched_transaction(
        raw: bytes,
        *,
        schema: str,
        code: str,
    ) -> dict[str, object]:
        payload = original_parse(raw, schema=schema, code=code)
        if schema == publication_module.DOWNLOAD_PUBLICATION_SCHEMA_VERSION:
            payload["base_pointer_sha256"] = None
        return payload

    monkeypatch.setattr(
        publication_module,
        "_strict_canonical_json",
        mismatched_transaction,
    )

    with pytest.raises(
        DownloadPublicationIntegrityError,
        match="DOWNLOAD_BASE_POINTER_BINDING_MISMATCH",
    ):
        resolve_download_publication(output_dir=tmp_path)


def test_post_commit_attestation_failure_is_typed_and_skips_projection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_resolve = publication_module._resolve_validated_generation
    projection_calls = 0

    def fail_attestation(root: Path):
        raise DownloadPublicationIntegrityError("INJECTED_ATTESTATION_FAILURE", str(root))

    def count_projection(root: Path, relative_path: str, content: bytes) -> None:
        nonlocal projection_calls
        del root, relative_path, content
        projection_calls += 1

    monkeypatch.setattr(publication_module, "_resolve_validated_generation", fail_attestation)
    monkeypatch.setattr(publication_module, "_write_compatibility_artifact", count_projection)

    with pytest.raises(DownloadPublicationIntegrityError) as exc_info:
        _publish(tmp_path)

    assert exc_info.value.code == "DOWNLOAD_POST_COMMIT_ATTESTATION_FAILED"
    assert exc_info.value.commit_state == "POINTER_COMMITTED"
    assert projection_calls == 0
    monkeypatch.setattr(publication_module, "_resolve_validated_generation", original_resolve)
    canonical = resolve_download_publication(output_dir=tmp_path)
    assert canonical.transaction_id.startswith("download_txn_")


def test_secondary_present_to_absent_retires_stale_fixed_projection(tmp_path: Path) -> None:
    first = _publish(tmp_path, include_secondary=True)
    assert first.legacy_secondary_prices_path is not None
    assert first.legacy_secondary_prices_path.is_file()

    second = _publish(tmp_path, published_at=PUBLISHED_AT + timedelta(minutes=1))

    assert second.secondary_prices_path is None
    assert second.legacy_secondary_prices_path is None
    assert second.legacy_projection_verified is True
    assert not (tmp_path / "prices_marketstack_daily.csv").exists()


def test_secondary_retirement_directory_failure_is_typed_after_commit(tmp_path: Path) -> None:
    first = _publish(tmp_path, include_secondary=True)
    assert first.legacy_secondary_prices_path is not None
    first.legacy_secondary_prices_path.unlink()
    first.legacy_secondary_prices_path.mkdir()

    with pytest.raises(DownloadLegacyProjectionError) as exc_info:
        _publish(tmp_path, published_at=PUBLISHED_AT + timedelta(minutes=1))

    assert exc_info.value.commit_state == "POINTER_COMMITTED_PROJECTION_FAILED"
    canonical = resolve_download_publication(output_dir=tmp_path)
    assert canonical.secondary_prices_path is None
    assert canonical.legacy_projection_verified is False
    assert (tmp_path / "prices_marketstack_daily.csv").is_dir()


def test_secondary_retirement_unlinks_symlink_without_touching_target(tmp_path: Path) -> None:
    first = _publish(tmp_path, include_secondary=True)
    assert first.legacy_secondary_prices_path is not None
    external = tmp_path.parent / f"{tmp_path.name}_external_secondary.csv"
    external.write_bytes(b"external")
    first.legacy_secondary_prices_path.unlink()
    try:
        first.legacy_secondary_prices_path.symlink_to(external)
    except OSError as exc:
        pytest.skip(f"symlink unavailable: {exc}")

    second = _publish(tmp_path, published_at=PUBLISHED_AT + timedelta(minutes=1))

    assert second.legacy_projection_verified is True
    assert not first.legacy_secondary_prices_path.exists()
    assert external.read_bytes() == b"external"


def test_secondary_retirement_detects_immediate_recreation_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _publish(tmp_path, include_secondary=True)
    assert first.legacy_secondary_prices_path is not None
    target = first.legacy_secondary_prices_path
    original_unlink = Path.unlink

    def unlink_then_recreate(path: Path, *args: object, **kwargs: object) -> None:
        original_unlink(path, *args, **kwargs)
        if path == target:
            path.write_bytes(b"raced")

    monkeypatch.setattr(Path, "unlink", unlink_then_recreate)

    with pytest.raises(DownloadLegacyProjectionError) as exc_info:
        _publish(tmp_path, published_at=PUBLISHED_AT + timedelta(minutes=1))

    assert exc_info.value.commit_state == "POINTER_COMMITTED_PROJECTION_FAILED"
    assert target.read_bytes() == b"raced"


def test_duplicate_artifact_row_key_fails_before_commit(tmp_path: Path) -> None:
    duplicated = DownloadArtifactCandidate(
        role="prices",
        filename="prices_daily.csv",
        content=_prices_bytes(("QQQ", "QQQ")),
        row_count=2,
        source_event_ids=("prices:primary",),
    )

    with pytest.raises(
        DownloadPublicationIntegrityError,
        match="DOWNLOAD_ARTIFACT_KEY_DUPLICATE",
    ):
        publish_download_transaction(
            output_dir=tmp_path,
            requested_start=START,
            requested_end=END,
            published_at=PUBLISHED_AT,
            artifacts=(duplicated, _rates_artifact()),
            source_bindings=(_prices_source(), _rates_source()),
        )

    assert not (tmp_path / ".download_publications/current/download_composite.json").exists()


def test_duplicate_source_event_id_across_artifacts_fails_closed(tmp_path: Path) -> None:
    shared_event_id = "source:shared"
    prices = replace(_prices_artifact(), source_event_ids=(shared_event_id,))
    rates = replace(_rates_artifact(), source_event_ids=(shared_event_id,))
    price_source = replace(_prices_source(), source_event_id=shared_event_id)
    rate_source = replace(_rates_source(), source_event_id=shared_event_id)

    with pytest.raises(
        DownloadPublicationIntegrityError,
        match="DOWNLOAD_SOURCE_BINDING_MISMATCH",
    ):
        publish_download_transaction(
            output_dir=tmp_path,
            requested_start=START,
            requested_end=END,
            published_at=PUBLISHED_AT,
            artifacts=(prices, rates),
            source_bindings=(price_source, rate_source),
        )


@pytest.mark.parametrize(
    "case",
    [
        "unknown_key",
        "overlap",
        "gap",
        "no_remainder",
        "multiple_remainders",
        "wrong_largest_remainder",
        "wrong_tie_remainder",
    ],
)
def test_exact_source_partition_rejects_invalid_allocation(
    tmp_path: Path,
    case: str,
) -> None:
    if case == "unknown_key":
        tickers = ("QQQ", "TQQQ")
        sources = (
            _partition_source("prices:a", "EXPLICIT_KEYS", (("SPY", "2026-04-30"),)),
            _partition_source(
                "prices:b",
                "REMAINDER",
                (("QQQ", "2026-04-30"), ("TQQQ", "2026-04-30")),
            ),
        )
    elif case == "overlap":
        tickers = ("QQQ", "TQQQ")
        sources = (
            _partition_source(
                "prices:a",
                "EXPLICIT_KEYS",
                (("QQQ", "2026-04-30"),),
            ),
            _partition_source(
                "prices:b",
                "REMAINDER",
                (("QQQ", "2026-04-30"), ("TQQQ", "2026-04-30")),
            ),
        )
    elif case == "gap":
        tickers = ("QQQ", "TQQQ")
        sources = (
            _partition_source(
                "prices:a",
                "EXPLICIT_KEYS",
                (("QQQ", "2026-04-30"),),
            ),
            _partition_source("prices:b", "REMAINDER", ()),
        )
    elif case == "no_remainder":
        tickers = ("QQQ", "TQQQ")
        sources = (
            _partition_source(
                "prices:a",
                "EXPLICIT_KEYS",
                (("QQQ", "2026-04-30"),),
            ),
            _partition_source(
                "prices:b",
                "EXPLICIT_KEYS",
                (("TQQQ", "2026-04-30"),),
            ),
        )
    elif case == "multiple_remainders":
        tickers = ("QQQ", "TQQQ")
        sources = (
            _partition_source(
                "prices:a",
                "REMAINDER",
                (("QQQ", "2026-04-30"),),
            ),
            _partition_source(
                "prices:b",
                "REMAINDER",
                (("TQQQ", "2026-04-30"),),
            ),
        )
    elif case == "wrong_largest_remainder":
        tickers = ("QQQ", "TQQQ", "VIX")
        sources = (
            _partition_source(
                "prices:a",
                "EXPLICIT_KEYS",
                (("QQQ", "2026-04-30"), ("TQQQ", "2026-04-30")),
            ),
            _partition_source(
                "prices:b",
                "REMAINDER",
                (("VIX", "2026-04-30"),),
            ),
        )
    else:
        tickers = ("QQQ", "TQQQ")
        sources = (
            _partition_source(
                "prices:a",
                "EXPLICIT_KEYS",
                (("QQQ", "2026-04-30"),),
            ),
            _partition_source(
                "prices:z",
                "REMAINDER",
                (("TQQQ", "2026-04-30"),),
            ),
        )

    with pytest.raises(
        DownloadPublicationIntegrityError,
        match="DOWNLOAD_SOURCE_BINDING_MISMATCH|DOWNLOAD_SOURCE_ROW_COUNT_MISMATCH",
    ):
        _publish_partitioned_prices(
            tmp_path,
            tickers=tickers,
            sources=sources,
        )


def test_zero_winner_explicit_source_is_preserved(tmp_path: Path) -> None:
    result = _publish_partitioned_prices(
        tmp_path,
        tickers=("QQQ",),
        sources=(
            _partition_source("prices:attempted_empty", "EXPLICIT_KEYS", ()),
            _partition_source(
                "prices:primary",
                "REMAINDER",
                (("QQQ", "2026-04-30"),),
            ),
        ),
    )

    resolved = resolve_download_publication(output_dir=tmp_path)
    transaction = json.loads(result.transaction_manifest_path.read_text(encoding="utf-8"))
    empty = _transaction_source(transaction, "prices:attempted_empty")

    assert resolved.transaction_id == result.transaction_id
    assert empty["winning_row_count"] == 0
    assert empty["explicit_row_keys"] == []
    assert empty["allocation_mode"] == "EXPLICIT_KEYS"


def test_empty_artifact_allows_one_zero_row_remainder(tmp_path: Path) -> None:
    empty_prices = DownloadArtifactCandidate(
        role="prices",
        filename="prices_daily.csv",
        content=_prices_bytes(()),
        row_count=0,
        source_event_ids=("prices:empty",),
    )
    empty_source = _partition_source("prices:empty", "REMAINDER", ())

    result = publish_download_transaction(
        output_dir=tmp_path,
        requested_start=START,
        requested_end=END,
        published_at=PUBLISHED_AT,
        artifacts=(empty_prices, _rates_artifact()),
        source_bindings=(empty_source, _rates_source()),
    )
    resolved = resolve_download_publication(output_dir=tmp_path)
    transaction = json.loads(result.transaction_manifest_path.read_text(encoding="utf-8"))
    persisted = _transaction_source(transaction, "prices:empty")

    assert resolved.artifact_row_count["prices"] == 0
    assert persisted["allocation_mode"] == "REMAINDER"
    assert persisted["winning_row_count"] == 0
    assert persisted["explicit_row_keys"] == []


def test_transaction_and_legacy_manifest_store_compact_source_partition(
    tmp_path: Path,
) -> None:
    result = _publish_partitioned_prices(
        tmp_path,
        tickers=("QQQ", "TQQQ", "VIX"),
        sources=(
            _partition_source(
                "prices:a_remainder",
                "REMAINDER",
                (("TQQQ", "2026-04-30"), ("VIX", "2026-04-30")),
            ),
            _partition_source(
                "prices:z_explicit",
                "EXPLICIT_KEYS",
                (("QQQ", "2026-04-30"),),
            ),
        ),
    )
    transaction = json.loads(result.transaction_manifest_path.read_text(encoding="utf-8"))
    remainder = _transaction_source(transaction, "prices:a_remainder")
    explicit = _transaction_source(transaction, "prices:z_explicit")
    legacy = pd.read_csv(result.legacy_manifest_path)
    price_row = legacy.loc[
        legacy["output_path"].astype(str) == str(result.legacy_prices_path)
    ].iloc[-1]
    legacy_parameters = json.loads(str(price_row["request_parameters"]))

    assert remainder["explicit_row_keys"] == []
    assert remainder["winning_row_count"] == 2
    assert explicit["explicit_row_keys"] == [["QQQ", "2026-04-30"]]
    assert explicit["winning_row_count"] == 1
    assert all(
        "explicit_row_keys" not in event and "winning_row_keys" not in event
        for event in legacy_parameters["source_events"]
    )


def test_request_parameters_are_deep_snapshotted_before_publication_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parameters: dict[str, object] = {
        "start": START.isoformat(),
        "end": END.isoformat(),
        "nested": {"tickers": ["QQQ"]},
    }
    source = replace(_prices_source(), request_parameters=parameters)
    original_validate = publication_module._validate_current_predecessor_bindings

    def mutate_external_mapping_after_validation(
        *,
        root: Path,
        source_bindings: tuple[DownloadSourceBinding, ...],
    ) -> str | None:
        nested = parameters["nested"]
        assert isinstance(nested, dict)
        tickers = nested["tickers"]
        assert isinstance(tickers, list)
        tickers.append("TQQQ")
        return original_validate(root=root, source_bindings=source_bindings)

    monkeypatch.setattr(
        publication_module,
        "_validate_current_predecessor_bindings",
        mutate_external_mapping_after_validation,
    )
    result = publish_download_transaction(
        output_dir=tmp_path,
        requested_start=START,
        requested_end=END,
        published_at=PUBLISHED_AT,
        artifacts=(_prices_artifact(), _rates_artifact()),
        source_bindings=(source, _rates_source()),
    )
    transaction = json.loads(result.transaction_manifest_path.read_text(encoding="utf-8"))
    persisted = _transaction_source(transaction, source.source_event_id)
    legacy = pd.read_csv(result.manifest_path)
    legacy_prices = legacy.loc[
        legacy["output_path"].astype(str) == str(result.legacy_prices_path)
    ].iloc[-1]
    legacy_parameters = json.loads(str(legacy_prices["request_parameters"]))
    legacy_event = next(
        event
        for event in legacy_parameters["source_events"]
        if event["source_event_id"] == source.source_event_id
    )

    assert parameters["nested"] == {"tickers": ["QQQ", "TQQQ"]}
    assert persisted["request_parameters"]["nested"] == {"tickers": ["QQQ"]}
    assert legacy_event["request_parameters_sha256"] == publication_module._canonical_sha256(
        persisted["request_parameters"]
    )


@pytest.mark.parametrize(
    ("case", "error_code"),
    [
        ("count", "DOWNLOAD_SOURCE_ROW_COUNT_MISMATCH"),
        ("digest", "DOWNLOAD_SOURCE_ROW_COUNT_MISMATCH"),
        ("unknown_key", "DOWNLOAD_SOURCE_BINDING_MISMATCH"),
        ("key_order", "DOWNLOAD_SOURCE_BINDING_MISMATCH"),
        ("source_set", "DOWNLOAD_SOURCE_BINDING_MISMATCH"),
        ("scope", "DOWNLOAD_SOURCE_BINDING_MISMATCH"),
    ],
)
def test_resolver_rejects_exact_source_binding_transaction_tamper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
    error_code: str,
) -> None:
    _publish_partitioned_prices(
        tmp_path,
        tickers=("IWM", "QQQ", "SPY", "TQQQ", "VIX"),
        sources=(
            _partition_source(
                "prices:a_remainder",
                "REMAINDER",
                (
                    ("QQQ", "2026-04-30"),
                    ("TQQQ", "2026-04-30"),
                    ("VIX", "2026-04-30"),
                ),
            ),
            _partition_source(
                "prices:z_explicit",
                "EXPLICIT_KEYS",
                (("IWM", "2026-04-30"), ("SPY", "2026-04-30")),
            ),
        ),
    )
    original_parse = publication_module._strict_canonical_json

    def tampered_transaction(
        raw: bytes,
        *,
        schema: str,
        code: str,
    ) -> dict[str, object]:
        payload = original_parse(raw, schema=schema, code=code)
        if schema != publication_module.DOWNLOAD_PUBLICATION_SCHEMA_VERSION:
            return payload
        explicit = _transaction_source(payload, "prices:z_explicit")
        if case == "count":
            explicit["winning_row_count"] = 3
        elif case == "digest":
            explicit["winning_row_keys_sha256"] = "0" * 64
        elif case == "unknown_key":
            explicit["explicit_row_keys"] = [
                ["AAA", "2026-04-30"],
                ["SPY", "2026-04-30"],
            ]
        elif case == "key_order":
            keys = explicit["explicit_row_keys"]
            assert isinstance(keys, list)
            explicit["explicit_row_keys"] = list(reversed(keys))
        elif case == "source_set":
            prices = _transaction_artifact(payload, "prices")
            prices["source_event_ids"] = ["prices:a_remainder"]
        else:
            payload["source_binding_scope"] = "DECLARED_SOURCE_ONLY"
        return payload

    monkeypatch.setattr(
        publication_module,
        "_strict_canonical_json",
        tampered_transaction,
    )

    with pytest.raises(DownloadPublicationIntegrityError, match=error_code):
        resolve_download_publication(output_dir=tmp_path)


def test_canonical_predecessor_binding_roundtrips_exact_metadata(
    tmp_path: Path,
) -> None:
    first = _publish(tmp_path)
    parameters = _canonical_predecessor_parameters(first)
    predecessor_source = _canonical_predecessor_source(parameters)

    second = publish_download_transaction(
        output_dir=tmp_path,
        requested_start=START,
        requested_end=END,
        published_at=PUBLISHED_AT + timedelta(minutes=1),
        artifacts=(
            replace(
                _prices_artifact(),
                source_event_ids=(predecessor_source.source_event_id,),
            ),
            _rates_artifact(),
        ),
        source_bindings=(predecessor_source, _rates_source()),
    )
    resolved = resolve_download_publication(output_dir=tmp_path)
    transaction = json.loads(second.transaction_manifest_path.read_text(encoding="utf-8"))
    persisted = _transaction_source(transaction, predecessor_source.source_event_id)

    assert resolved.transaction_id == second.transaction_id
    assert persisted["request_parameters"] == parameters
    assert parameters["predecessor_transaction_id"] == first.transaction_id
    assert parameters["predecessor_discovery_pointer_sha256"] == first.discovery_pointer_sha256
    assert parameters["predecessor_artifact_sha256"] == first.artifact_sha256["prices"]
    assert parameters["predecessor_artifact_row_count"] == first.artifact_row_count["prices"]


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("predecessor_discovery_pointer_sha256", None),
        ("predecessor_transaction_id", "download_txn_00000000000000000000000000000000"),
        ("predecessor_discovery_pointer_sha256", "0" * 64),
        ("predecessor_artifact_sha256", "1" * 64),
        ("predecessor_artifact_row_count", 99),
    ],
)
def test_canonical_predecessor_missing_or_tampered_metadata_fails_closed(
    tmp_path: Path,
    field: str,
    replacement: object,
) -> None:
    first = _publish(tmp_path)
    parameters = _canonical_predecessor_parameters(first)
    if replacement is None:
        del parameters[field]
    else:
        parameters[field] = replacement
    predecessor_source = _canonical_predecessor_source(parameters)

    with pytest.raises(
        DownloadPublicationIntegrityError,
        match="DOWNLOAD_PREDECESSOR_BINDING_MISMATCH",
    ):
        publish_download_transaction(
            output_dir=tmp_path,
            requested_start=START,
            requested_end=END,
            published_at=PUBLISHED_AT + timedelta(minutes=1),
            artifacts=(
                replace(
                    _prices_artifact(),
                    source_event_ids=(predecessor_source.source_event_id,),
                ),
                _rates_artifact(),
            ),
            source_bindings=(predecessor_source, _rates_source()),
        )

    assert resolve_download_publication(output_dir=tmp_path).transaction_id == first.transaction_id


def test_resolver_rejects_tampered_canonical_predecessor_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _publish(tmp_path)
    predecessor_source = _canonical_predecessor_source(_canonical_predecessor_parameters(first))
    publish_download_transaction(
        output_dir=tmp_path,
        requested_start=START,
        requested_end=END,
        published_at=PUBLISHED_AT + timedelta(minutes=1),
        artifacts=(
            replace(
                _prices_artifact(),
                source_event_ids=(predecessor_source.source_event_id,),
            ),
            _rates_artifact(),
        ),
        source_bindings=(predecessor_source, _rates_source()),
    )
    original_parse = publication_module._strict_canonical_json

    def tampered_transaction(
        raw: bytes,
        *,
        schema: str,
        code: str,
    ) -> dict[str, object]:
        payload = original_parse(raw, schema=schema, code=code)
        if schema == publication_module.DOWNLOAD_PUBLICATION_SCHEMA_VERSION:
            source = _transaction_source(payload, predecessor_source.source_event_id)
            parameters = source["request_parameters"]
            assert isinstance(parameters, dict)
            parameters["predecessor_artifact_sha256"] = "0" * 64
            source["request_parameters_sha256"] = publication_module._canonical_sha256(parameters)
        return payload

    monkeypatch.setattr(
        publication_module,
        "_strict_canonical_json",
        tampered_transaction,
    )

    with pytest.raises(
        DownloadPublicationIntegrityError,
        match="DOWNLOAD_PREDECESSOR_BINDING_MISMATCH",
    ):
        resolve_download_publication(output_dir=tmp_path)


@pytest.mark.parametrize(
    "case",
    ["artifact_event_id", "binding_event_id", "binding_source_id"],
)
def test_unhashable_source_identifier_boundary_is_typed(
    tmp_path: Path,
    case: str,
) -> None:
    prices = _prices_artifact()
    source = _prices_source()
    if case == "artifact_event_id":
        prices = replace(
            prices,
            source_event_ids=(["unhashable"],),  # type: ignore[arg-type]
        )
    elif case == "binding_event_id":
        source = replace(
            source,
            source_event_id=["unhashable"],  # type: ignore[arg-type]
        )
    else:
        source = replace(
            source,
            source_id=["unhashable"],  # type: ignore[arg-type]
        )

    with pytest.raises(DownloadPublicationIntegrityError) as exc_info:
        publish_download_transaction(
            output_dir=tmp_path,
            requested_start=START,
            requested_end=END,
            published_at=PUBLISHED_AT,
            artifacts=(prices, _rates_artifact()),
            source_bindings=(source, _rates_source()),
        )

    assert exc_info.value.code == "DOWNLOAD_SOURCE_BINDING_MISMATCH"


def test_list_of_pairs_request_parameters_boundary_is_typed(tmp_path: Path) -> None:
    source = replace(
        _prices_source(),
        request_parameters=[("start", START.isoformat())],  # type: ignore[arg-type]
    )

    with pytest.raises(DownloadPublicationIntegrityError) as exc_info:
        publish_download_transaction(
            output_dir=tmp_path,
            requested_start=START,
            requested_end=END,
            published_at=PUBLISHED_AT,
            artifacts=(_prices_artifact(), _rates_artifact()),
            source_bindings=(source, _rates_source()),
        )

    assert exc_info.value.code == "DOWNLOAD_JSON_FIELD_INVALID"


def _publish(
    root: Path,
    *,
    published_at: datetime = PUBLISHED_AT,
    include_secondary: bool = False,
):
    artifacts = [_prices_artifact(), _rates_artifact()]
    sources = [_prices_source(), _rates_source()]
    if include_secondary:
        artifacts.append(_secondary_artifact())
        sources.append(_secondary_source())
    return publish_download_transaction(
        output_dir=root,
        requested_start=START,
        requested_end=END,
        published_at=published_at,
        artifacts=tuple(artifacts),
        source_bindings=tuple(sources),
    )


def _prices_artifact() -> DownloadArtifactCandidate:
    return DownloadArtifactCandidate(
        role="prices",
        filename="prices_daily.csv",
        content=_prices_bytes(("QQQ",)),
        row_count=1,
        source_event_ids=("prices:primary",),
    )


def _rates_artifact() -> DownloadArtifactCandidate:
    return DownloadArtifactCandidate(
        role="rates",
        filename="rates_daily.csv",
        content=b"date,series,value\n2026-04-30,DGS10,4.0\n",
        row_count=1,
        source_event_ids=("rates:fred",),
    )


def _secondary_artifact() -> DownloadArtifactCandidate:
    return DownloadArtifactCandidate(
        role="secondary_prices",
        filename="prices_marketstack_daily.csv",
        content=_prices_bytes(("QQQ",)),
        row_count=1,
        source_event_ids=("secondary:provider",),
    )


def _prices_source() -> DownloadSourceBinding:
    return DownloadSourceBinding(
        source_event_id="prices:primary",
        artifact_role="prices",
        source_kind="LIVE_PROVIDER",
        source_id="primary",
        provider="Primary",
        endpoint="primary",
        request_parameters={"start": START.isoformat(), "end": END.isoformat()},
        winning_row_count=1,
        allocation_mode="REMAINDER",
        winning_row_keys=(("QQQ", "2026-04-30"),),
    )


def _rates_source() -> DownloadSourceBinding:
    return DownloadSourceBinding(
        source_event_id="rates:fred",
        artifact_role="rates",
        source_kind="LIVE_PROVIDER",
        source_id="fred",
        provider="FRED",
        endpoint="fred",
        request_parameters={"start": START.isoformat(), "end": END.isoformat()},
        winning_row_count=1,
        allocation_mode="REMAINDER",
        winning_row_keys=(("DGS10", "2026-04-30"),),
    )


def _secondary_source() -> DownloadSourceBinding:
    return DownloadSourceBinding(
        source_event_id="secondary:provider",
        artifact_role="secondary_prices",
        source_kind="LIVE_PROVIDER",
        source_id="secondary",
        provider="Secondary",
        endpoint="secondary",
        request_parameters={"start": START.isoformat(), "end": END.isoformat()},
        winning_row_count=1,
        allocation_mode="REMAINDER",
        winning_row_keys=(("QQQ", "2026-04-30"),),
    )


def _partition_source(
    source_event_id: str,
    allocation_mode: str,
    winning_row_keys: tuple[tuple[str, str], ...],
) -> DownloadSourceBinding:
    return DownloadSourceBinding(
        source_event_id=source_event_id,
        artifact_role="prices",
        source_kind="LIVE_PROVIDER",
        source_id=source_event_id,
        provider="Test provider",
        endpoint="test",
        request_parameters={"event": source_event_id},
        winning_row_count=len(winning_row_keys),
        allocation_mode=allocation_mode,
        winning_row_keys=winning_row_keys,
    )


def _publish_partitioned_prices(
    root: Path,
    *,
    tickers: tuple[str, ...],
    sources: tuple[DownloadSourceBinding, ...],
):
    prices = DownloadArtifactCandidate(
        role="prices",
        filename="prices_daily.csv",
        content=_prices_bytes(tickers),
        row_count=len(tickers),
        source_event_ids=tuple(sorted(source.source_event_id for source in sources)),
    )
    return publish_download_transaction(
        output_dir=root,
        requested_start=START,
        requested_end=END,
        published_at=PUBLISHED_AT,
        artifacts=(prices, _rates_artifact()),
        source_bindings=(*sources, _rates_source()),
    )


def _transaction_source(
    transaction: dict[str, object],
    source_event_id: str,
) -> dict[str, object]:
    records = transaction["source_event_records"]
    assert isinstance(records, list)
    for record in records:
        assert isinstance(record, dict)
        if record.get("source_event_id") == source_event_id:
            return record
    raise AssertionError(f"missing source event {source_event_id}")


def _transaction_artifact(
    transaction: dict[str, object],
    role: str,
) -> dict[str, object]:
    artifacts = transaction["artifacts"]
    assert isinstance(artifacts, list)
    for artifact in artifacts:
        assert isinstance(artifact, dict)
        if artifact.get("role") == role:
            return artifact
    raise AssertionError(f"missing artifact role {role}")


def _canonical_predecessor_parameters(
    predecessor,
) -> dict[str, object]:
    publication_root = next(
        parent
        for parent in predecessor.transaction_manifest_path.parents
        if parent.name == ".download_publications"
    )
    root = publication_root.parent
    return {
        "predecessor_transaction_id": predecessor.transaction_id,
        "predecessor_transaction_path": predecessor.transaction_manifest_path.relative_to(
            root
        ).as_posix(),
        "predecessor_transaction_sha256": predecessor.transaction_manifest_sha256,
        "predecessor_discovery_pointer_sha256": predecessor.discovery_pointer_sha256,
        "predecessor_artifact_role": "prices",
        "predecessor_artifact_path": predecessor.prices_path.relative_to(root).as_posix(),
        "predecessor_artifact_sha256": predecessor.artifact_sha256["prices"],
        "predecessor_artifact_row_count": predecessor.artifact_row_count["prices"],
        "lineage_scope": "IMMEDIATE_PREDECESSOR_ONLY",
        "raw_provider_provenance": False,
        "origin_lineage_complete": False,
        "origin_status": "CANONICAL_IMMEDIATE_PREDECESSOR",
        "data_quality_provenance": False,
    }


def _canonical_predecessor_source(
    parameters: dict[str, object],
) -> DownloadSourceBinding:
    return DownloadSourceBinding(
        source_event_id="prices:canonical_predecessor",
        artifact_role="prices",
        source_kind="CANONICAL_PREDECESSOR_REUSE",
        source_id="canonical_predecessor",
        provider="Canonical predecessor",
        endpoint="immutable_publication",
        request_parameters=parameters,
        winning_row_count=1,
        allocation_mode="REMAINDER",
        winning_row_keys=(("QQQ", "2026-04-30"),),
    )


def _prices_bytes(tickers: tuple[str, ...]) -> bytes:
    lines = ["date,ticker,open,high,low,close,adj_close,volume"]
    lines.extend(f"2026-04-30,{ticker},1,1,1,1,1,1" for ticker in tickers)
    return ("\n".join(lines) + "\n").encode("utf-8")


def _rewrite_manifest_row(
    raw: bytes,
    *,
    row_index: int,
    field: str,
    value: str,
) -> bytes:
    reader = csv.DictReader(io.StringIO(raw.decode("utf-8-sig"), newline=""))
    fieldnames = list(reader.fieldnames or ())
    rows = list(reader)
    rows[row_index][field] = value
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue().encode("utf-8")


def _duplicate_manifest_suffix(raw: bytes, *, suffix_size: int) -> bytes:
    reader = csv.DictReader(io.StringIO(raw.decode("utf-8-sig"), newline=""))
    fieldnames = list(reader.fieldnames or ())
    rows = list(reader)
    rows.extend(dict(row) for row in rows[-suffix_size:])
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue().encode("utf-8")
