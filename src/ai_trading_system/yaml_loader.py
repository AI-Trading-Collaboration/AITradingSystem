from __future__ import annotations

import math
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Any, Literal

import yaml

_SAFE_LOADER: type[yaml.SafeLoader] = getattr(yaml, "CSafeLoader", yaml.SafeLoader)
_EXPONENT_NUMBER_RE = re.compile(
    r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)[eE][+-]?\d+$"
)
_NON_FINITE_TEXT = frozenset(
    {
        "nan",
        "+nan",
        "-nan",
        "inf",
        "+inf",
        "-inf",
        "infinity",
        "+infinity",
        "-infinity",
    }
)

StrictYamlKeyPolicy = Literal["STRING", "HASHABLE"]


class StrictYamlError(ValueError):
    """Typed syntax/safety failure from the shared strict YAML primitive."""

    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}")


@dataclass(frozen=True)
class StrictYamlOptions:
    """Parsing controls needed to preserve each reviewed caller contract."""

    key_policy: StrictYamlKeyPolicy = "STRING"
    flatten_mapping: bool = True
    reject_non_finite: bool = True

    def __post_init__(self) -> None:
        if self.key_policy not in {"STRING", "HASHABLE"}:
            raise ValueError(f"unsupported strict YAML key policy: {self.key_policy}")


_DEFAULT_STRICT_YAML_OPTIONS = StrictYamlOptions()


def safe_load_yaml_text(text: str) -> Any:
    return yaml.load(text, Loader=_SAFE_LOADER)


def safe_load_yaml_path(path: Path) -> Any:
    return safe_load_yaml_text(path.read_text(encoding="utf-8"))


def load_strict_yaml_text(
    text: str,
    *,
    options: StrictYamlOptions = _DEFAULT_STRICT_YAML_OPTIONS,
    label: str = "yaml",
) -> Any:
    """Load safe YAML with explicit duplicate-key and value-safety semantics."""

    loader_class = _strict_loader_class(options)
    try:
        value = yaml.load(text, Loader=loader_class)
    except StrictYamlError:
        raise
    except RecursionError as exc:
        raise StrictYamlError("CYCLIC_ALIAS", label) from exc
    except yaml.YAMLError as exc:
        if "recursive" in str(exc).lower():
            raise StrictYamlError("CYCLIC_ALIAS", label) from exc
        raise StrictYamlError("INVALID", label) from exc
    if options.reject_non_finite:
        _reject_non_finite(value, label)
    return value


@cache
def _strict_loader_class(options: StrictYamlOptions) -> type[yaml.SafeLoader]:
    class ConfiguredStrictYamlLoader(yaml.SafeLoader):
        pass

    def construct_unique_mapping(
        loader: yaml.SafeLoader,
        node: yaml.nodes.MappingNode,
        deep: bool = False,
    ) -> dict[object, object]:
        if options.flatten_mapping:
            loader.flatten_mapping(node)
        result: dict[object, object] = {}
        for key_node, value_node in node.value:
            key = loader.construct_object(  # type: ignore[no-untyped-call]
                key_node,
                deep=deep,
            )
            line = key_node.start_mark.line + 1
            if options.key_policy == "STRING" and not isinstance(key, str):
                raise StrictYamlError("NON_STRING_KEY", f"line={line}")
            try:
                duplicate = key in result
            except TypeError as exc:
                raise StrictYamlError("UNHASHABLE_KEY", f"line={line}") from exc
            if duplicate:
                raise StrictYamlError("DUPLICATE_KEY", f"key={key!r} line={line}")
            result[key] = loader.construct_object(  # type: ignore[no-untyped-call]
                value_node,
                deep=deep,
            )
        return result

    ConfiguredStrictYamlLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
        construct_unique_mapping,
    )
    return ConfiguredStrictYamlLoader


def _reject_non_finite(
    value: object,
    field: str,
    *,
    visiting: set[int] | None = None,
    visited: set[int] | None = None,
) -> None:
    active = visiting if visiting is not None else set()
    complete = visited if visited is not None else set()
    if isinstance(value, float) and not math.isfinite(value):
        raise StrictYamlError("NON_FINITE_NUMBER", field)
    if isinstance(value, str):
        lowered = value.lower()
        if lowered in _NON_FINITE_TEXT:
            raise StrictYamlError("NON_FINITE_NUMBER", field)
        if _EXPONENT_NUMBER_RE.fullmatch(value):
            try:
                parsed = float(value)
            except ValueError:
                parsed = 0.0
            if not math.isfinite(parsed):
                raise StrictYamlError("NON_FINITE_NUMBER", field)
    children: Iterable[object]
    if isinstance(value, Mapping):
        children = value.values()
    elif isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    ):
        children = value
    else:
        return
    identity = id(value)
    if identity in active:
        raise StrictYamlError("CYCLIC_ALIAS", field)
    if identity in complete:
        return
    active.add(identity)
    try:
        for index, child in enumerate(children):
            _reject_non_finite(
                child,
                f"{field}[{index}]",
                visiting=active,
                visited=complete,
            )
    finally:
        active.remove(identity)
    complete.add(identity)


__all__ = [
    "StrictYamlError",
    "StrictYamlOptions",
    "load_strict_yaml_text",
    "safe_load_yaml_path",
    "safe_load_yaml_text",
]
