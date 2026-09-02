from __future__ import annotations

import json
import os
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from ai_trading_system.atlas.controlled_https_preview import (
    APPROVED_LOCAL_BUNDLE,
    TASK_ID,
    ControlledHttpsPreviewError,
    ControlledHttpsPreviewPolicy,
    ExternalPreviewAuthorization,
    bind_external_preview_authorization,
    build_controlled_preview_manifest,
    load_controlled_https_preview_policy,
    load_controlled_preview_manifest,
    replay_controlled_preview_bundle,
    validate_https_endpoint_bytes,
    write_controlled_preview_bundle,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
POLICY_PATH = PROJECT_ROOT / "config/atlas/controlled_https_preview_policy.yaml"
SOURCE_COMMIT = "1" * 40
GENERATED_AT = datetime(2026, 9, 2, 1, 0, tzinfo=UTC)
ORIGIN = "https://preview.example.test"


def _pending_policy() -> ControlledHttpsPreviewPolicy:
    return load_controlled_https_preview_policy(repository_root=PROJECT_ROOT)


def _approved_policy() -> ControlledHttpsPreviewPolicy:
    pending = _pending_policy()
    return replace(
        pending,
        status=APPROVED_LOCAL_BUNDLE,
        owner_decision_slots={
            "hosting_provider": "SYNTHETIC_PRIVATE_PREVIEW",
            "https_origin": ORIGIN,
            "private_access_mode": "PRIVATE_AUTHENTICATED",
            "ttl_hours": 24,
            "cost_ceiling_usd": "5.00",
            "retention_days": 30,
            "browser_matrix": ["Chromium synthetic"],
            "viewport_matrix": ["1440x900", "390x844"],
            "assistive_technology_matrix": ["Keyboard synthetic"],
            "cleanup_authority": "Synthetic test owner",
        },
        safety={
            **pending.safety,
            "local_bundle_generation_authorized": True,
        },
    )


def _source_bundle(root: Path) -> tuple[str, ...]:
    (root / "assets").mkdir(parents=True)
    (root / "index.html").write_text(
        "<!doctype html><html><head>"
        '<link rel="stylesheet" href="assets/site.css">'
        "</head><body><main><h1>Preview</h1>"
        '<a href="audit.json">Audit</a>'
        '<script src="assets/site.js"></script>'
        "</main></body></html>",
        encoding="utf-8",
    )
    (root / "assets/site.css").write_text("main { max-width: 70rem; }", encoding="utf-8")
    (root / "assets/site.js").write_text(
        "document.documentElement.dataset.preview = 'static';", encoding="utf-8"
    )
    (root / "audit.json").write_text(
        json.dumps({"status": "PASS"}, sort_keys=True), encoding="utf-8"
    )
    return ("index.html", "assets/site.css", "assets/site.js", "audit.json")


def _build_manifest(source: Path):
    allowlist = _source_bundle(source)
    manifest = build_controlled_preview_manifest(
        source_directory=source,
        allowlisted_paths=allowlist,
        policy=_approved_policy(),
        source_commit=SOURCE_COMMIT,
        source_generator="synthetic_atlas_writer",
        source_generator_version="1.0.0-test",
        generated_at_utc=GENERATED_AT,
    )
    return allowlist, manifest


def test_repository_policy_is_pending_and_cannot_build(tmp_path: Path) -> None:
    policy = _pending_policy()
    assert policy.status == "PENDING_OWNER_DECISION"
    assert policy.ready_for_local_bundle is False
    assert policy.safety["external_deployment_authorized"] is False
    assert policy.safety["production_effect"] == "none"
    assert policy.safety["broker_action"] == "none"
    assert policy.policy_sha256 == __import__("hashlib").sha256(
        POLICY_PATH.read_bytes()
    ).hexdigest()

    source = tmp_path / "source"
    allowlist = _source_bundle(source)
    with pytest.raises(
        ControlledHttpsPreviewError,
        match="PREVIEW_POLICY_OWNER_DECISION_PENDING",
    ):
        build_controlled_preview_manifest(
            source_directory=source,
            allowlisted_paths=allowlist,
            policy=policy,
            source_commit=SOURCE_COMMIT,
            source_generator="synthetic_atlas_writer",
            source_generator_version="1.0.0-test",
            generated_at_utc=GENERATED_AT,
        )


def test_manifest_and_materialized_bundle_are_deterministic(tmp_path: Path) -> None:
    source = tmp_path / "source"
    allowlist = _source_bundle(source)
    policy = _approved_policy()
    first = build_controlled_preview_manifest(
        source_directory=source,
        allowlisted_paths=allowlist,
        policy=policy,
        source_commit=SOURCE_COMMIT,
        source_generator="synthetic_atlas_writer",
        source_generator_version="1.0.0-test",
        generated_at_utc=GENERATED_AT,
    )
    second = build_controlled_preview_manifest(
        source_directory=source,
        allowlisted_paths=tuple(reversed(allowlist)),
        policy=policy,
        source_commit=SOURCE_COMMIT,
        source_generator="synthetic_atlas_writer",
        source_generator_version="1.0.0-test",
        generated_at_utc=GENERATED_AT,
    )
    assert first.canonical_json_bytes() == second.canonical_json_bytes()
    assert first.authorization_state == "NOT_AUTHORIZED"
    assert first.authorization_ref is None
    assert first.expires_at_utc == GENERATED_AT + timedelta(hours=24)

    output = tmp_path / "bundle"
    written = write_controlled_preview_bundle(
        source_directory=source,
        output_directory=output,
        allowlisted_paths=allowlist,
        policy=policy,
        source_commit=SOURCE_COMMIT,
        source_generator="synthetic_atlas_writer",
        source_generator_version="1.0.0-test",
        generated_at_utc=GENERATED_AT,
    )
    manifest_path = output / written.manifest_name
    loaded = load_controlled_preview_manifest(manifest_path)
    assert loaded == written
    replay = replay_controlled_preview_bundle(
        bundle_directory=output,
        checked_at_utc=GENERATED_AT + timedelta(hours=1),
    )
    assert replay.status == "PASS"
    assert replay.manifest_sha256 == written.content_sha256
    assert replay.checked_file_count == len(allowlist) + 1


@pytest.mark.parametrize(
    "html,reason",
    [
        (
            '<html><script src="https://cdn.example.test/app.js"></script></html>',
            "PREVIEW_EXTERNAL_REFERENCE_FORBIDDEN",
        ),
        (
            '<html><img src="data:image/png;base64,AAAA"></html>',
            "PREVIEW_EXTERNAL_REFERENCE_FORBIDDEN",
        ),
        (
            "<html><script>fetch('/runtime')</script></html>",
            "PREVIEW_DYNAMIC_NETWORK_API_FORBIDDEN",
        ),
        (
            '<html><a href="http://127.0.0.1:8000/private">local</a></html>',
            "PREVIEW_EXTERNAL_REFERENCE_FORBIDDEN",
        ),
        (
            '<html><link rel="stylesheet" href="missing.css"></html>',
            "PREVIEW_REFERENCE_NOT_ALLOWLISTED",
        ),
    ],
)
def test_network_runtime_and_unlisted_references_fail_closed(
    tmp_path: Path, html: str, reason: str
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "index.html").write_text(html, encoding="utf-8")
    with pytest.raises(ControlledHttpsPreviewError, match=reason):
        build_controlled_preview_manifest(
            source_directory=source,
            allowlisted_paths=("index.html",),
            policy=_approved_policy(),
            source_commit=SOURCE_COMMIT,
            source_generator="synthetic_atlas_writer",
            source_generator_version="1.0.0-test",
            generated_at_utc=GENERATED_AT,
        )


def test_traversal_duplicate_and_manifest_self_reference_fail_closed(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    _source_bundle(source)
    common = {
        "source_directory": source,
        "policy": _approved_policy(),
        "source_commit": SOURCE_COMMIT,
        "source_generator": "synthetic_atlas_writer",
        "source_generator_version": "1.0.0-test",
        "generated_at_utc": GENERATED_AT,
    }
    with pytest.raises(ControlledHttpsPreviewError, match="PREVIEW_RELATIVE_PATH_INVALID"):
        build_controlled_preview_manifest(
            allowlisted_paths=("../index.html",), **common
        )
    with pytest.raises(ControlledHttpsPreviewError, match="PREVIEW_ALLOWLIST_INVALID"):
        build_controlled_preview_manifest(
            allowlisted_paths=("index.html", "index.html"), **common
        )
    with pytest.raises(
        ControlledHttpsPreviewError,
        match="PREVIEW_MANIFEST_MUST_NOT_BE_ALLOWLISTED",
    ):
        build_controlled_preview_manifest(
            allowlisted_paths=("index.html", "controlled_https_preview_manifest.json"),
            **common,
        )


def test_symlink_asset_fails_closed_when_supported(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    target = source / "target.html"
    target.write_text("<html></html>", encoding="utf-8")
    link = source / "index.html"
    try:
        os.symlink(target, link)
    except OSError:
        pytest.skip("symlink creation is unavailable on this host")
    with pytest.raises(ControlledHttpsPreviewError, match="PREVIEW_ASSET_SYMLINK_FORBIDDEN"):
        build_controlled_preview_manifest(
            source_directory=source,
            allowlisted_paths=("index.html",),
            policy=_approved_policy(),
            source_commit=SOURCE_COMMIT,
            source_generator="synthetic_atlas_writer",
            source_generator_version="1.0.0-test",
            generated_at_utc=GENERATED_AT,
        )


def test_replay_rejects_missing_extra_drift_and_expired_bundle(tmp_path: Path) -> None:
    source = tmp_path / "source"
    allowlist = _source_bundle(source)
    output = tmp_path / "bundle"
    manifest = write_controlled_preview_bundle(
        source_directory=source,
        output_directory=output,
        allowlisted_paths=allowlist,
        policy=_approved_policy(),
        source_commit=SOURCE_COMMIT,
        source_generator="synthetic_atlas_writer",
        source_generator_version="1.0.0-test",
        generated_at_utc=GENERATED_AT,
    )
    (output / "unexpected.json").write_text("{}", encoding="utf-8")
    with pytest.raises(ControlledHttpsPreviewError, match="FILE_SET_MISMATCH"):
        replay_controlled_preview_bundle(
            bundle_directory=output, checked_at_utc=GENERATED_AT
        )
    (output / "unexpected.json").unlink()
    (output / "audit.json").write_text('{"status":"DRIFT"}', encoding="utf-8")
    with pytest.raises(ControlledHttpsPreviewError, match="ASSET_DRIFT:audit.json"):
        replay_controlled_preview_bundle(
            bundle_directory=output, checked_at_utc=GENERATED_AT
        )
    (output / "audit.json").write_bytes(source.joinpath("audit.json").read_bytes())
    with pytest.raises(ControlledHttpsPreviewError, match="PREVIEW_BUNDLE_EXPIRED"):
        replay_controlled_preview_bundle(
            bundle_directory=output,
            checked_at_utc=manifest.expires_at_utc + timedelta(seconds=1),
        )


def test_endpoint_requires_exact_r2_binding_origin_window_and_bytes(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    allowlist, manifest = _build_manifest(source)
    authorization = ExternalPreviewAuthorization(
        authorization_ref="owner_decision:TRADING-2526:synthetic-r2",
        authorization_state="EXACT_PREAUTHORIZED",
        task_id=TASK_ID,
        source_commit=manifest.source_commit,
        asset_set_sha256=manifest.asset_set_sha256,
        https_origin=ORIGIN,
        authorized_at_utc=GENERATED_AT,
        expires_at_utc=GENERATED_AT + timedelta(hours=12),
    )
    with pytest.raises(ControlledHttpsPreviewError, match="NOT_AUTHORIZED"):
        validate_https_endpoint_bytes(
            manifest=manifest,
            authorization=authorization,
            endpoint_origin=ORIGIN,
            response_bytes_by_path={},
            checked_at_utc=GENERATED_AT,
        )
    bound = bind_external_preview_authorization(
        manifest=manifest, authorization=authorization
    )
    responses = {path: (source / path).read_bytes() for path in allowlist}
    responses[bound.manifest_name] = bound.canonical_json_bytes()
    receipt = validate_https_endpoint_bytes(
        manifest=bound,
        authorization=authorization,
        endpoint_origin=ORIGIN,
        response_bytes_by_path=responses,
        checked_at_utc=GENERATED_AT + timedelta(hours=1),
    )
    assert receipt.endpoint_origin == ORIGIN
    assert receipt.status == "PASS"

    with pytest.raises(ControlledHttpsPreviewError, match="ORIGIN_MISMATCH"):
        validate_https_endpoint_bytes(
            manifest=bound,
            authorization=authorization,
            endpoint_origin="https://other.example.test",
            response_bytes_by_path=responses,
            checked_at_utc=GENERATED_AT + timedelta(hours=1),
        )
    drifted = dict(responses)
    drifted["index.html"] += b"drift"
    with pytest.raises(ControlledHttpsPreviewError, match="BYTE_DRIFT:index.html"):
        validate_https_endpoint_bytes(
            manifest=bound,
            authorization=authorization,
            endpoint_origin=ORIGIN,
            response_bytes_by_path=drifted,
            checked_at_utc=GENERATED_AT + timedelta(hours=1),
        )
    with pytest.raises(ControlledHttpsPreviewError, match="PREVIEW_ENDPOINT_EXPIRED"):
        validate_https_endpoint_bytes(
            manifest=bound,
            authorization=authorization,
            endpoint_origin=ORIGIN,
            response_bytes_by_path=responses,
            checked_at_utc=authorization.expires_at_utc + timedelta(seconds=1),
        )
