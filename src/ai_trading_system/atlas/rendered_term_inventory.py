from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from html.parser import HTMLParser

from ai_trading_system.atlas.reader_terminology_projection import ReaderTerminologyPolicy
from ai_trading_system.contracts.strategy_research_reader_terminology import (
    ReaderInteractionState,
    ReaderTermClassification,
    ReaderTermDefinition,
    ReaderTerminologyContractError,
    ReaderTextLayer,
    ReaderTextSurface,
    RenderedAuditIdentifier,
    RenderedReaderSurface,
    RenderedTermInventory,
    RenderedTermOccurrence,
    RenderedTermResolution,
)

_WHITESPACE = re.compile(r"\s+")
_UNKNOWN_PATTERNS: dict[str, re.Pattern[str]] = {
    "TASK_ID": re.compile(r"(?<![A-Za-z0-9_])TRADING-\d+(?:_[A-Z0-9]+)*(?![A-Za-z0-9_])"),
    "UPPER_SNAKE_IDENTIFIER": re.compile(
        r"(?<![A-Za-z0-9_])[A-Z][A-Z0-9]+(?:_[A-Z0-9]+)+(?![A-Za-z0-9_])"
    ),
    "GIT_OR_CONTENT_HASH": re.compile(r"(?<![0-9a-f])[0-9a-f]{40}(?:[0-9a-f]{24})?(?![0-9a-f])"),
    "REPOSITORY_PATH": re.compile(
        r"(?<![A-Za-z0-9_])(?:config|docs|inputs|outputs|registry|src|tests)/"
        r"[A-Za-z0-9_.\-/]+"
    ),
    "RUNTIME_COMPOSED_IDENTIFIER": re.compile(
        r"(?<![A-Za-z0-9_])[a-z][a-z0-9]+(?:-[a-z0-9]+){2,}(?![A-Za-z0-9_])"
    ),
}
_VOID_TAGS = {
    "area",
    "base",
    "br",
    "col",
    "embed",
    "hr",
    "img",
    "input",
    "link",
    "meta",
    "param",
    "source",
    "track",
    "wbr",
}
_SKIPPED_TAGS = {"style", "script", "template"}
_RAW_STATUS_TOKEN = re.compile(r"(?<![A-Za-z0-9_])(?:PASS|LIMITED|CURRENT)(?![A-Za-z0-9_])")


class RenderedTermInventoryError(ValueError):
    pass


@dataclass
class _Frame:
    tag: str
    locator: str
    attrs: dict[str, str]
    audit: bool
    closed_details: bool
    definition_context: bool
    child_tag_counts: dict[str, int] = field(default_factory=dict)
    text_parts: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class _Surface:
    ordinal: int
    text: str
    locator: str
    surface: ReaderTextSurface
    interaction_state: ReaderInteractionState
    layer: ReaderTextLayer
    in_definition: bool


def _normalized_text(value: str) -> str:
    return _WHITESPACE.sub(" ", value).strip()


def _alias_pattern(alias: str) -> re.Pattern[str]:
    escaped = re.escape(alias)
    if re.search(r"[A-Za-z0-9]", alias):
        return re.compile(rf"(?<![A-Za-z0-9_]){escaped}(?![A-Za-z0-9_])")
    return re.compile(escaped)


class _ReaderSurfaceParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.stack: list[_Frame] = []
        self.surfaces: list[_Surface] = []
        self.definition_locations: dict[str, tuple[int, str]] = {}
        self.described_by: list[
            tuple[str, tuple[str, ...], ReaderTextLayer, bool]
        ] = []
        self.text_by_id: dict[str, list[str]] = {}
        self.excluded_regions: set[str] = set()

    def _next_locator(self, tag: str, attrs: dict[str, str]) -> str:
        if self.stack:
            parent = self.stack[-1]
            index = parent.child_tag_counts.get(tag, 0) + 1
            parent.child_tag_counts[tag] = index
            prefix = parent.locator + "/"
        else:
            index = 1
            prefix = ""
        element_id = attrs.get("id")
        return prefix + (f"{tag}#{element_id}" if element_id else f"{tag}[{index}]")

    def _layer(self, attrs: dict[str, str]) -> ReaderTextLayer:
        inherited = any(frame.audit for frame in self.stack)
        return (
            ReaderTextLayer.AUDIT
            if inherited or attrs.get("data-reader-layer") == "audit"
            else ReaderTextLayer.READER
        )

    def _interaction_state(self, layer: ReaderTextLayer) -> ReaderInteractionState:
        if layer is ReaderTextLayer.AUDIT:
            return ReaderInteractionState.AUDIT_DISCLOSURE
        last_closed_details = next(
            (
                index
                for index in range(len(self.stack) - 1, -1, -1)
                if self.stack[index].closed_details
            ),
            None,
        )
        if last_closed_details is None:
            return ReaderInteractionState.DEFAULT
        if any(frame.tag == "summary" for frame in self.stack[last_closed_details + 1 :]):
            return ReaderInteractionState.DEFAULT
        return ReaderInteractionState.EXPANDED_DISCLOSURE

    def _add_surface(
        self,
        *,
        text: str,
        locator: str,
        surface: ReaderTextSurface,
        layer: ReaderTextLayer,
        in_definition: bool,
    ) -> None:
        normalized = _normalized_text(text)
        if not normalized:
            return
        state = (
            ReaderInteractionState.ATTRIBUTE
            if surface is not ReaderTextSurface.VISIBLE_TEXT
            and layer is ReaderTextLayer.READER
            else self._interaction_state(layer)
        )
        self.surfaces.append(
            _Surface(
                ordinal=len(self.surfaces) + 1,
                text=normalized,
                locator=locator,
                surface=surface,
                interaction_state=state,
                layer=layer,
                in_definition=in_definition,
            )
        )

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        attr_map = {key: "" if value is None else value for key, value in attrs}
        locator = self._next_locator(tag, attr_map)
        layer = self._layer(attr_map)
        closed_details = tag == "details" and "open" not in attr_map
        frame = _Frame(
            tag=tag,
            locator=locator,
            attrs=attr_map,
            audit=layer is ReaderTextLayer.AUDIT,
            closed_details=closed_details,
            definition_context=(
                bool(attr_map.get("data-term-definition"))
                or any(item.definition_context for item in self.stack)
            ),
        )
        self.stack.append(frame)
        if tag in _SKIPPED_TAGS:
            parent_tag = self.stack[-2].tag if len(self.stack) > 1 else "document"
            self.excluded_regions.add(f"{parent_tag}/{tag}")
        definition_id = attr_map.get("data-term-definition")
        if definition_id:
            if definition_id in self.definition_locations:
                raise RenderedTermInventoryError(
                    f"RENDERED_TERM_DEFINITION_DUPLICATE:{definition_id}"
                )
            self.definition_locations[definition_id] = (len(self.surfaces) + 1, locator)
        for attribute, surface_kind in (
            ("aria-label", ReaderTextSurface.ARIA_LABEL),
            ("title", ReaderTextSurface.TITLE),
        ):
            value = attr_map.get(attribute)
            if value:
                self._add_surface(
                    text=value,
                    locator=f"{locator}@{attribute}",
                    surface=surface_kind,
                    layer=layer,
                    in_definition=frame.definition_context,
                )
        described_by = tuple(attr_map.get("aria-describedby", "").split())
        if described_by:
            self.described_by.append(
                (locator, described_by, layer, frame.definition_context)
            )
        if tag in _VOID_TAGS:
            self._close_top(tag)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)
        if self.stack and self.stack[-1].tag == tag.lower():
            self._close_top(tag.lower())

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        for index in range(len(self.stack) - 1, -1, -1):
            if self.stack[index].tag == tag:
                while len(self.stack) > index:
                    self._close_top(self.stack[-1].tag)
                return

    def _close_top(self, tag: str) -> None:
        if not self.stack:
            return
        frame = self.stack.pop()
        if frame.tag != tag:
            raise RenderedTermInventoryError(
                f"RENDERED_TERM_HTML_STACK_INVALID:{tag}:{frame.tag}"
            )
        element_id = frame.attrs.get("id")
        if element_id and frame.text_parts:
            self.text_by_id[element_id] = list(frame.text_parts)
        if self.stack and frame.text_parts:
            self.stack[-1].text_parts.extend(frame.text_parts)

    def handle_data(self, data: str) -> None:
        if not self.stack or any(frame.tag in _SKIPPED_TAGS for frame in self.stack):
            return
        normalized = _normalized_text(data)
        if not normalized:
            return
        for frame in self.stack:
            frame.text_parts.append(normalized)
        frame = self.stack[-1]
        layer = (
            ReaderTextLayer.AUDIT
            if any(item.audit for item in self.stack)
            else ReaderTextLayer.READER
        )
        self._add_surface(
            text=normalized,
            locator=frame.locator,
            surface=ReaderTextSurface.VISIBLE_TEXT,
            layer=layer,
            in_definition=any(item.definition_context for item in self.stack),
        )

    def finish(self) -> None:
        while self.stack:
            self._close_top(self.stack[-1].tag)
        for locator, ref_ids, layer, in_definition in self.described_by:
            missing = tuple(ref_id for ref_id in ref_ids if ref_id not in self.text_by_id)
            if missing:
                raise RenderedTermInventoryError(
                    "RENDERED_TERM_ARIA_DESCRIPTION_TARGET_MISSING:" + ",".join(missing)
                )
            text = " ".join(
                part for ref_id in ref_ids for part in self.text_by_id[ref_id]
            )
            self._add_surface(
                text=text,
                locator=f"{locator}@aria-describedby",
                surface=ReaderTextSurface.ARIA_DESCRIPTION,
                layer=layer,
                in_definition=in_definition,
            )


def _matched_terms(
    text: str,
    terms: tuple[ReaderTermDefinition, ...],
) -> tuple[tuple[int, int, ReaderTermDefinition, str], ...]:
    candidates: list[tuple[int, int, ReaderTermDefinition, str]] = []
    for term in terms:
        for alias in sorted(term.aliases, key=len, reverse=True):
            for match in _alias_pattern(alias).finditer(text):
                candidates.append((match.start(), match.end(), term, match.group(0)))
    selected: list[tuple[int, int, ReaderTermDefinition, str]] = []
    occupied: set[int] = set()
    for candidate in sorted(
        candidates,
        key=lambda item: (item[0], -(item[1] - item[0]), item[2].term_id),
    ):
        start, end, _term, _matched = candidate
        if any(index in occupied for index in range(start, end)):
            continue
        selected.append(candidate)
        occupied.update(range(start, end))
    return tuple(sorted(selected, key=lambda item: (item[0], item[1], item[2].term_id)))


def _unknown_identifiers(
    surface: _Surface,
    *,
    matched_ranges: tuple[tuple[int, int], ...],
    enabled_patterns: tuple[str, ...],
) -> tuple[str, ...]:
    unknown: list[str] = []
    for pattern_id in enabled_patterns:
        pattern = _UNKNOWN_PATTERNS[pattern_id]
        for match in pattern.finditer(surface.text):
            if any(
                match.start() >= start and match.end() <= end
                for start, end in matched_ranges
            ):
                continue
            unknown.append(f"{pattern_id}:{match.group(0)}@{surface.locator}")
    return tuple(unknown)


def _alias_case_drifts(
    surface: _Surface,
    *,
    terms: tuple[ReaderTermDefinition, ...],
    matched_ranges: tuple[tuple[int, int], ...],
) -> tuple[str, ...]:
    drifts: list[str] = []
    for term in terms:
        for alias in term.aliases:
            if not any(character.isalpha() for character in alias):
                continue
            escaped = re.escape(alias)
            pattern = (
                re.compile(
                    rf"(?<![A-Za-z0-9_]){escaped}(?![A-Za-z0-9_])",
                    re.IGNORECASE,
                )
                if re.search(r"[A-Za-z0-9]", alias)
                else re.compile(escaped, re.IGNORECASE)
            )
            for match in pattern.finditer(surface.text):
                if any(
                    match.start() >= start and match.end() <= end
                    for start, end in matched_ranges
                ):
                    continue
                if match.group(0) != alias:
                    drifts.append(
                        f"{term.term_id}:{match.group(0)}@{surface.locator}"
                    )
    return tuple(drifts)


def _is_raw_default_identifier(value: str) -> bool:
    return (
        value == "capability GO"
        or _RAW_STATUS_TOKEN.search(value) is not None
        or any(pattern.search(value) is not None for pattern in _UNKNOWN_PATTERNS.values())
    )


def build_rendered_term_inventory(
    *, html_bytes: bytes, policy: ReaderTerminologyPolicy
) -> RenderedTermInventory:
    try:
        html_text = html_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RenderedTermInventoryError("RENDERED_TERM_HTML_UTF8_REQUIRED") from exc
    parser = _ReaderSurfaceParser()
    parser.feed(html_text)
    parser.close()
    parser.finish()
    unexpected_exclusions = parser.excluded_regions - set(
        policy.excluded_non_reader_regions
    )
    if unexpected_exclusions:
        raise RenderedTermInventoryError(
            "RENDERED_TERM_EXCLUSION_UNDECLARED:"
            + ",".join(sorted(unexpected_exclusions))
        )
    terms_by_id = {item.term_id: item for item in policy.terms}
    if set(parser.definition_locations) != set(terms_by_id):
        missing = sorted(set(terms_by_id) - set(parser.definition_locations))
        extra = sorted(set(parser.definition_locations) - set(terms_by_id))
        raise RenderedTermInventoryError(
            f"RENDERED_TERM_DEFINITION_SET_INVALID:missing={missing}:extra={extra}"
        )

    occurrences: list[RenderedTermOccurrence] = []
    rendered_surfaces: list[RenderedReaderSurface] = []
    audit_identifiers: list[RenderedAuditIdentifier] = []
    unknowns: list[str] = []
    case_drifts: list[str] = []
    raw_reader_exposures: list[str] = []
    for surface in parser.surfaces:
        matches = _matched_terms(surface.text, policy.terms)
        matched_ranges = tuple((start, end) for start, end, _term, _text in matches)
        if surface.layer is ReaderTextLayer.AUDIT:
            for pattern_id in policy.unknown_identifier_patterns:
                for match in _UNKNOWN_PATTERNS[pattern_id].finditer(surface.text):
                    audit_identifiers.append(
                        RenderedAuditIdentifier(
                            surface_ordinal=surface.ordinal,
                            pattern_id=pattern_id,
                            identifier=match.group(0),
                            dom_locator=surface.locator,
                        )
                    )
        else:
            unknowns.extend(
                _unknown_identifiers(
                    surface,
                    matched_ranges=matched_ranges,
                    enabled_patterns=policy.unknown_identifier_patterns,
                )
            )
            case_drifts.extend(
                _alias_case_drifts(
                    surface,
                    terms=policy.terms,
                    matched_ranges=matched_ranges,
                )
            )
        rendered_surfaces.append(
            RenderedReaderSurface(
                ordinal=surface.ordinal,
                normalized_text=surface.text,
                dom_locator=surface.locator,
                surface=surface.surface,
                interaction_state=surface.interaction_state,
                reader_layer=surface.layer,
                term_ids=tuple(dict.fromkeys(term.term_id for _, _, term, _ in matches)),
            )
        )
        for _start, _end, term, matched_text in matches:
            if (
                surface.layer is ReaderTextLayer.READER
                and not surface.in_definition
                and surface.interaction_state
                in {ReaderInteractionState.DEFAULT, ReaderInteractionState.ATTRIBUTE}
                and _is_raw_default_identifier(matched_text)
            ):
                raw_reader_exposures.append(
                    f"{term.term_id}:{matched_text}@{surface.locator}"
                )
            if (
                term.classification is ReaderTermClassification.AUDIT_ONLY
                and surface.layer is not ReaderTextLayer.AUDIT
            ):
                raise RenderedTermInventoryError(
                    f"RENDERED_TERM_AUDIT_ONLY_EXPOSED:{term.term_id}:{surface.locator}"
                )
            occurrences.append(
                RenderedTermOccurrence(
                    term_id=term.term_id,
                    matched_text=matched_text,
                    ordinal=surface.ordinal,
                    dom_locator=surface.locator,
                    surface=surface.surface,
                    interaction_state=surface.interaction_state,
                    reader_layer=surface.layer,
                )
            )
    if case_drifts:
        raise RenderedTermInventoryError(
            "RENDERED_TERM_ALIAS_CASE_DRIFT:"
            + "|".join(sorted(set(case_drifts)))
        )
    if unknowns:
        raise RenderedTermInventoryError(
            "RENDERED_TERM_UNKNOWN_IDENTIFIER:" + "|".join(sorted(set(unknowns)))
        )
    if raw_reader_exposures:
        raise RenderedTermInventoryError(
            "RENDERED_TERM_RAW_IDENTIFIER_EXPOSED:"
            + "|".join(sorted(set(raw_reader_exposures)))
        )

    resolutions: list[RenderedTermResolution] = []
    for term in sorted(policy.terms, key=lambda item: item.term_id):
        term_occurrences = [item for item in occurrences if item.term_id == term.term_id]
        if not term_occurrences:
            raise RenderedTermInventoryError(
                f"RENDERED_TERM_NOT_RENDERED:{term.term_id}"
            )
        first = min(term_occurrences, key=lambda item: item.ordinal)
        explanation = (
            None
            if term.classification
            in {
                ReaderTermClassification.COMMON_LANGUAGE,
                ReaderTermClassification.AUDIT_ONLY,
            }
            else parser.definition_locations[term.term_id]
        )
        try:
            resolutions.append(
                RenderedTermResolution(
                    term_id=term.term_id,
                    classification=term.classification,
                    first_occurrence_ordinal=first.ordinal,
                    first_occurrence_locator=first.dom_locator,
                    explanation_ordinal=None if explanation is None else explanation[0],
                    explanation_locator=None if explanation is None else explanation[1],
                )
            )
        except ReaderTerminologyContractError as exc:
            raise RenderedTermInventoryError(str(exc)) from exc

    inventory = RenderedTermInventory.seal(
        html_sha256=hashlib.sha256(html_bytes).hexdigest(),
        reader_profile_id=policy.reader_profile.profile_id,
        reader_profile_sha256=policy.reader_profile_sha256,
        terminology_policy_id=policy.policy_id,
        terminology_policy_sha256=policy.terminology_policy_sha256,
        scanned_surface_count=len(parser.surfaces),
        surfaces=tuple(rendered_surfaces),
        occurrences=tuple(sorted(occurrences, key=lambda item: item.ordinal)),
        resolutions=tuple(resolutions),
        audit_identifiers=tuple(audit_identifiers),
        excluded_non_reader_regions=policy.excluded_non_reader_regions,
    )
    replay = RenderedTermInventory.from_json_bytes(inventory.canonical_bytes)
    if replay != inventory:
        raise RenderedTermInventoryError("RENDERED_TERM_CANONICAL_REPLAY_MISMATCH")
    return inventory


__all__ = [
    "RenderedTermInventoryError",
    "build_rendered_term_inventory",
]
