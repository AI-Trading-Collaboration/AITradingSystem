from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

DEFAULT_PRICE_SYMBOL_ALIASES: dict[str, str] = {"BRK.B": "BRK-B"}


@dataclass(frozen=True)
class SymbolResolution:
    canonical_symbol: str
    source_symbol: str
    mapping_status: str
    used_by: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "canonical_symbol": self.canonical_symbol,
            "source_symbol": self.source_symbol,
            "mapping_status": self.mapping_status,
            "used_by": list(self.used_by),
        }


def source_symbol_for(
    canonical_symbol: str,
    *,
    aliases: dict[str, str] | None = None,
) -> str:
    normalized = canonical_symbol.strip()
    mapping = aliases or DEFAULT_PRICE_SYMBOL_ALIASES
    return mapping.get(normalized, normalized)


def canonical_symbol_for(
    source_symbol: str,
    *,
    aliases: dict[str, str] | None = None,
) -> str:
    normalized = source_symbol.strip()
    mapping = aliases or DEFAULT_PRICE_SYMBOL_ALIASES
    inverse = {source: canonical for canonical, source in mapping.items()}
    return inverse.get(normalized, normalized)


def resolve_symbol(
    canonical_symbol: str,
    *,
    manifest_mapping: dict[str, object] | None = None,
    used_by: Iterable[str] = (),
) -> SymbolResolution:
    normalized = canonical_symbol.strip()
    source = _manifest_source_symbol(normalized, manifest_mapping) or source_symbol_for(normalized)
    status = "OK"
    if (
        normalized in DEFAULT_PRICE_SYMBOL_ALIASES
        and source != DEFAULT_PRICE_SYMBOL_ALIASES[normalized]
    ):
        status = "SYMBOL_MAPPING_MISSING"
    return SymbolResolution(
        canonical_symbol=normalized,
        source_symbol=source,
        mapping_status=status,
        used_by=tuple(dict.fromkeys(str(item) for item in used_by if str(item))),
    )


def resolve_symbols(
    canonical_symbols: Iterable[str],
    *,
    manifest_mapping: dict[str, object] | None = None,
    used_by: Iterable[str] = (),
) -> dict[str, SymbolResolution]:
    return {
        symbol: resolve_symbol(symbol, manifest_mapping=manifest_mapping, used_by=used_by)
        for symbol in dict.fromkeys(str(item).strip() for item in canonical_symbols if str(item))
    }


def symbol_mapping_payload(
    canonical_symbols: Iterable[str],
    *,
    manifest_mapping: dict[str, object] | None = None,
    used_by: Iterable[str] = (),
) -> dict[str, dict[str, object]]:
    return {
        symbol: resolution.to_dict()
        for symbol, resolution in resolve_symbols(
            canonical_symbols,
            manifest_mapping=manifest_mapping,
            used_by=used_by,
        ).items()
    }


def _manifest_source_symbol(
    canonical_symbol: str,
    manifest_mapping: dict[str, object] | None,
) -> str:
    if not manifest_mapping:
        return ""
    raw = manifest_mapping.get(canonical_symbol)
    if isinstance(raw, dict):
        value = raw.get("source_symbol") or raw.get("source")
        return str(value).strip() if value else ""
    if isinstance(raw, str):
        return raw.strip()
    return ""
