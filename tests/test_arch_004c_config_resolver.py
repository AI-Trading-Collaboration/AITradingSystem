from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import BaseModel, Field

from ai_trading_system import config as legacy_config
from ai_trading_system.platform.config import (
    ConfigResolutionError,
    MarketRegimeConfig,
    MarketRegimesConfig,
    load_market_regimes,
    market_regime_by_id,
    resolve_market_regimes,
    resolve_yaml_config,
)


class SampleConfig(BaseModel):
    schema_version: str
    value: int = Field(gt=0)


def test_typed_config_resolver_records_path_hash_version_status_and_loaded_at(
    tmp_path: Path,
) -> None:
    path = tmp_path / "sample.yaml"
    path.write_text(
        "schema_version: sample.v1\npolicy_metadata:\n  status: pilot\nvalue: 3\n",
        encoding="utf-8",
    )
    loaded_at = datetime(2026, 7, 10, 22, 0, tzinfo=UTC)

    resolved = resolve_yaml_config(
        path,
        SampleConfig,
        policy_id="sample_policy",
        loaded_at=loaded_at,
    )

    assert resolved.value.value == 3
    assert resolved.reference.to_dict() == {
        "policy_id": "sample_policy",
        "version": "sample.v1",
        "status": "pilot",
        "path": str(path),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "loaded_at": loaded_at.isoformat(),
    }


@pytest.mark.parametrize(
    ("content", "code"),
    [
        ("- not-a-mapping\n", "CONFIG_ROOT_NOT_MAPPING"),
        ("schema_version: sample.v1\nvalue: 0\n", "CONFIG_SCHEMA_INVALID"),
    ],
)
def test_typed_config_resolver_fails_closed_on_root_and_schema(
    tmp_path: Path,
    content: str,
    code: str,
) -> None:
    path = tmp_path / "invalid.yaml"
    path.write_text(content, encoding="utf-8")

    with pytest.raises(ConfigResolutionError, match=code):
        resolve_yaml_config(path, SampleConfig, policy_id="sample")


def test_typed_config_resolver_fails_closed_on_missing_file(tmp_path: Path) -> None:
    with pytest.raises(ConfigResolutionError, match="CONFIG_FILE_MISSING"):
        resolve_yaml_config(tmp_path / "missing.yaml", SampleConfig, policy_id="sample")


def test_market_regime_loader_moved_to_platform_with_legacy_import_parity() -> None:
    resolved = resolve_market_regimes()
    platform_config = load_market_regimes()
    legacy_loaded = legacy_config.load_market_regimes()

    assert legacy_config.MarketRegimeConfig is MarketRegimeConfig
    assert legacy_config.MarketRegimesConfig is MarketRegimesConfig
    assert legacy_loaded == platform_config == resolved.value
    assert legacy_config.DEFAULT_MARKET_REGIMES_CONFIG_PATH == Path(resolved.reference.path)
    default_regime = market_regime_by_id(
        resolved.value,
        resolved.value.default_backtest_regime,
    )
    assert default_regime.regime_id == "ai_after_chatgpt"
    assert default_regime.anchor_date.isoformat() == "2022-11-30"
    assert default_regime.start_date.isoformat() == "2022-12-01"
    assert (
        resolved.reference.sha256
        == hashlib.sha256(legacy_config.DEFAULT_MARKET_REGIMES_CONFIG_PATH.read_bytes()).hexdigest()
    )
