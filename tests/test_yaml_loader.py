from __future__ import annotations

import math

import pytest

from ai_trading_system.yaml_loader import (
    StrictYamlError,
    StrictYamlOptions,
    load_strict_yaml_text,
    safe_load_yaml_text,
)

HASHABLE_MERGE_OPTIONS = StrictYamlOptions(
    key_policy="HASHABLE",
    flatten_mapping=True,
    reject_non_finite=False,
)


def test_safe_loader_behavior_remains_duplicate_last_value_wins() -> None:
    assert safe_load_yaml_text("value: first\nvalue: second\n") == {
        "value": "second"
    }


def test_strict_loader_rejects_duplicate_with_key_and_line() -> None:
    with pytest.raises(StrictYamlError) as caught:
        load_strict_yaml_text("root:\n  value: first\n  value: second\n")

    assert caught.value.code == "DUPLICATE_KEY"
    assert caught.value.detail == "key='value' line=3"


def test_strict_loader_rejects_non_string_and_unhashable_keys() -> None:
    with pytest.raises(StrictYamlError) as non_string:
        load_strict_yaml_text("1: value\n")
    assert non_string.value.code == "NON_STRING_KEY"
    assert non_string.value.detail == "line=1"

    with pytest.raises(StrictYamlError) as unhashable:
        load_strict_yaml_text(
            "? [left, right]\n: value\n",
            options=HASHABLE_MERGE_OPTIONS,
        )
    assert unhashable.value.code == "UNHASHABLE_KEY"
    assert unhashable.value.detail == "line=1"


def test_hashable_merge_mode_preserves_merge_semantics_and_numeric_keys() -> None:
    payload = load_strict_yaml_text(
        "base: &base\n"
        "  enabled: true\n"
        "child:\n"
        "  <<: *base\n"
        "  value: 2\n"
        "1: numeric-key\n",
        options=HASHABLE_MERGE_OPTIONS,
    )

    assert payload == {
        "base": {"enabled": True},
        "child": {"enabled": True, "value": 2},
        1: "numeric-key",
    }


def test_hashable_merge_mode_rejects_duplicate_created_by_explicit_override() -> None:
    with pytest.raises(StrictYamlError) as caught:
        load_strict_yaml_text(
            "base: &base\n"
            "  enabled: true\n"
            "child:\n"
            "  <<: *base\n"
            "  enabled: false\n",
            options=HASHABLE_MERGE_OPTIONS,
        )

    assert caught.value.code == "DUPLICATE_KEY"
    assert caught.value.detail == "key='enabled' line=5"


@pytest.mark.parametrize(
    "text",
    [
        "value: .nan\n",
        "value: 1e999\n",
    ],
)
def test_default_strict_loader_rejects_non_finite_values(text: str) -> None:
    with pytest.raises(StrictYamlError) as caught:
        load_strict_yaml_text(text)
    assert caught.value.code == "NON_FINITE_NUMBER"


def test_configured_loader_preserves_prior_non_finite_behavior() -> None:
    payload = load_strict_yaml_text(
        "value: .nan\n",
        options=HASHABLE_MERGE_OPTIONS,
    )
    assert math.isnan(payload["value"])


def test_default_strict_loader_rejects_cycle_malformed_and_unsafe_tag() -> None:
    with pytest.raises(StrictYamlError) as cycle:
        load_strict_yaml_text("root: &root\n  self: *root\n")
    assert cycle.value.code == "CYCLIC_ALIAS"

    with pytest.raises(StrictYamlError) as malformed:
        load_strict_yaml_text("root: [\n")
    assert malformed.value.code == "INVALID"

    with pytest.raises(StrictYamlError) as unsafe:
        load_strict_yaml_text("value: !!python/object:builtins.object {}\n")
    assert unsafe.value.code == "INVALID"
