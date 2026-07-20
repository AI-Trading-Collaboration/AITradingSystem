from __future__ import annotations

from datetime import UTC, datetime

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.external_request_cache import (
    invalidate_external_request_cache,
    lookup_external_request_cache,
    write_external_request_cache_response,
)

runner = CliRunner()


def test_external_request_cache_invalidation_cli_is_audited_and_offline(tmp_path) -> None:
    url = "https://example.test/data"
    response = write_external_request_cache_response(
        provider="Example Provider",
        api_family="prices",
        method="GET",
        url=url,
        status_code=200,
        response_headers={"content-type": "application/json"},
        content=b'{"ok":true}',
        cache_dir=tmp_path,
        requested_at=datetime(2026, 7, 20, 1, 0, tzinfo=UTC),
    )
    lookup = lookup_external_request_cache(
        provider="Example Provider",
        api_family="prices",
        method="GET",
        url=url,
        cache_dir=tmp_path,
    )
    assert lookup.generation_id is not None
    assert lookup.body_sha256 is not None

    result = runner.invoke(
        app,
        [
            "invalidate-external-request-cache",
            "--metadata-path",
            str(lookup.metadata_path),
            "--expected-generation-id",
            lookup.generation_id,
            "--expected-body-sha256",
            lookup.body_sha256,
            "--actor",
            "test-operator",
            "--reason",
            "contract test",
            "--reference",
            "OPS-064",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "production_effect=none" in result.output
    assert "next_business_request_may_contact_provider=true" in result.output
    invalidated = lookup_external_request_cache(
        provider="Example Provider",
        api_family="prices",
        method="GET",
        url=url,
        cache_dir=tmp_path,
    )
    assert invalidated.status == "INVALIDATED_REVALIDATE"
    assert invalidated.response is None
    assert response.cache_key == invalidated.cache_key


def test_external_request_cache_invalidation_cli_rejects_stale_cas(tmp_path) -> None:
    url = "https://example.test/data"
    write_external_request_cache_response(
        provider="Example Provider",
        api_family="prices",
        method="GET",
        url=url,
        status_code=200,
        response_headers={},
        content=b"positive",
        cache_dir=tmp_path,
        requested_at=datetime(2026, 7, 20, 1, 0, tzinfo=UTC),
    )
    lookup = lookup_external_request_cache(
        provider="Example Provider",
        api_family="prices",
        method="GET",
        url=url,
        cache_dir=tmp_path,
    )
    assert lookup.body_sha256 is not None

    result = runner.invoke(
        app,
        [
            "invalidate-external-request-cache",
            "--metadata-path",
            str(lookup.metadata_path),
            "--expected-generation-id",
            "stale-generation",
            "--expected-body-sha256",
            lookup.body_sha256,
            "--actor",
            "test-operator",
            "--reason",
            "stale contract test",
            "--reference",
            "OPS-064",
        ],
    )

    assert result.exit_code != 0
    assert "stale cache invalidation target" in result.output


def test_clean_selection_preregistration_gate_commands_are_registered() -> None:
    build_help = runner.invoke(
        app,
        ["research", "ops", "clean-selection-preregistration-gate", "--help"],
    )
    validate_help = runner.invoke(
        app,
        [
            "research",
            "ops",
            "validate-clean-selection-preregistration-gate",
            "--help",
        ],
    )

    assert build_help.exit_code == 0, build_help.output
    assert "--r2-manifest" in build_help.output
    assert validate_help.exit_code == 0, validate_help.output
    assert "--output-root" in validate_help.output


def test_invalidation_function_requires_a_current_v2_generation(tmp_path) -> None:
    try:
        invalidate_external_request_cache(
            provider="Missing",
            api_family="missing",
            method="GET",
            url="https://example.test/missing",
            expected_generation_id="missing-generation",
            expected_body_sha256="0" * 64,
            actor="test-operator",
            reason="missing contract test",
            reference="OPS-064",
            cache_dir=tmp_path,
        )
    except ValueError as exc:
        assert "valid v2 current generation" in str(exc)
    else:  # pragma: no cover - contract guard
        raise AssertionError("missing generation must fail closed")
