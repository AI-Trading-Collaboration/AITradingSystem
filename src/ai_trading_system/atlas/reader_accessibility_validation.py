from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from html.parser import HTMLParser

from ai_trading_system.contracts.strategy_research_reader_projection import (
    ReaderSectionId,
)


class ReaderAccessibilityValidationError(ValueError):
    pass


@dataclass(frozen=True)
class ReaderAccessibilityViolation:
    code: str
    locator: str
    detail: str

    def to_dict(self) -> dict[str, str]:
        return {"code": self.code, "locator": self.locator, "detail": self.detail}


@dataclass(frozen=True)
class ReaderAccessibilityValidationResult:
    schema_version = "atlas_reader_accessibility_validation.v1"

    html_sha256: str
    status: str
    violations: tuple[ReaderAccessibilityViolation, ...]
    section_order: tuple[str, ...]
    automated_engineering_status: str
    owner_visual_status: str = "PENDING_REVIEW"
    reader_comprehension_status: str = "PENDING_REVIEW"
    production_effect: str = "none"
    broker_action: str = "none"

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[0-9a-f]{64}", self.html_sha256):
            raise ReaderAccessibilityValidationError("READER_A11Y_HTML_SHA_INVALID")
        expected_status = "PASS" if not self.violations else "FAIL"
        if self.status != expected_status or self.automated_engineering_status != expected_status:
            raise ReaderAccessibilityValidationError("READER_A11Y_STATUS_INVALID")
        if (
            self.owner_visual_status != "PENDING_REVIEW"
            or self.reader_comprehension_status != "PENDING_REVIEW"
            or self.production_effect != "none"
            or self.broker_action != "none"
        ):
            raise ReaderAccessibilityValidationError("READER_A11Y_SAFETY_INVALID")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "html_sha256": self.html_sha256,
            "status": self.status,
            "violations": [item.to_dict() for item in self.violations],
            "section_order": list(self.section_order),
            "automated_engineering_status": self.automated_engineering_status,
            "owner_visual_status": self.owner_visual_status,
            "reader_comprehension_status": self.reader_comprehension_status,
            "production_effect": self.production_effect,
            "broker_action": self.broker_action,
        }


@dataclass
class _Frame:
    tag: str
    locator: str
    attrs: dict[str, str]
    child_counts: dict[str, int] = field(default_factory=dict)
    text_parts: list[str] = field(default_factory=list)


class _AccessibilityParser(HTMLParser):
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

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.stack: list[_Frame] = []
        self.violations: list[ReaderAccessibilityViolation] = []
        self.h1_count = 0
        self.heading_levels: list[tuple[int, str]] = []
        self.landmarks: set[str] = set()
        self.skip_link_found = False
        self.section_order: list[str] = []
        self.card_disclosure_counts: dict[str, int] = {}
        self.table_captions: dict[str, int] = {}
        self.element_ids: set[str] = set()
        self.term_occurrences: dict[
            tuple[str, str], list[tuple[str, bool, str]]
        ] = {}

    def _next_locator(self, tag: str, attrs: dict[str, str]) -> str:
        if self.stack:
            parent = self.stack[-1]
            index = parent.child_counts.get(tag, 0) + 1
            parent.child_counts[tag] = index
            prefix = parent.locator + "/"
        else:
            index = 1
            prefix = ""
        return prefix + (f"{tag}#{attrs['id']}" if attrs.get("id") else f"{tag}[{index}]")

    def _violate(self, code: str, locator: str, detail: str) -> None:
        self.violations.append(ReaderAccessibilityViolation(code, locator, detail))

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        attr_map = {key: "" if value is None else value for key, value in attrs}
        locator = self._next_locator(tag, attr_map)
        if attr_map.get("id"):
            self.element_ids.add(attr_map["id"])
        if tag in {"main", "nav", "footer"}:
            self.landmarks.add(tag)
        if tag == "a" and attr_map.get("href") == "#main-content":
            self.skip_link_found = True
        if tag == "h1":
            self.h1_count += 1
        if re.fullmatch(r"h[1-6]", tag):
            self.heading_levels.append((int(tag[1]), locator))
        if tag == "details":
            if any(frame.tag == "details" for frame in self.stack):
                self._violate("NESTED_DISCLOSURE", locator, "details 不得嵌套 details")
            card = next(
                (frame for frame in reversed(self.stack) if "data-reader-card" in frame.attrs),
                None,
            )
            if card is not None:
                self.card_disclosure_counts[card.locator] = (
                    self.card_disclosure_counts.get(card.locator, 0) + 1
                )
        if "data-reader-section" in attr_map:
            self.section_order.append(attr_map["data-reader-section"])
        if "data-always-visible" in attr_map and any(
            frame.tag == "details" for frame in self.stack
        ):
            self._violate(
                "ALWAYS_VISIBLE_INSIDE_DISCLOSURE",
                locator,
                attr_map["data-always-visible"],
            )
        if "data-term-trigger" in attr_map:
            term_id = attr_map["data-term-trigger"]
            reader_section = next(
                (
                    frame.attrs["data-reader-section"]
                    for frame in reversed(self.stack)
                    if "data-reader-section" in frame.attrs
                ),
                "document",
            )
            first = attr_map.get("data-term-first") == "true"
            description_id = attr_map.get("aria-describedby", "")
            self.term_occurrences.setdefault((reader_section, term_id), []).append(
                (locator, first, description_id)
            )
            if not description_id:
                self._violate("TERM_DESCRIPTION_MISSING", locator, "aria-describedby required")
            if first and attr_map.get("tabindex") != "0":
                self._violate("TERM_FIRST_USE_NOT_FOCUSABLE", locator, "tabindex=0 required")
            if not first and attr_map.get("tabindex") == "0":
                self._violate("TERM_REPEAT_ADDS_TAB_STOP", locator, "repeat must not add tab stop")
            if attr_map.get("title") and not attr_map.get("aria-describedby"):
                self._violate("TERM_TITLE_ONLY", locator, "title cannot be the only definition")
        if tag in {"button", "a", "input", "select", "textarea"} and any(
            frame.tag in {"button", "a"} for frame in self.stack
        ):
            self._violate("NESTED_INTERACTIVE_CONTROL", locator, tag)
        if tag == "table":
            self.table_captions[locator] = 0
        if tag == "caption":
            table = next((frame for frame in reversed(self.stack) if frame.tag == "table"), None)
            if table is not None:
                self.table_captions[table.locator] = self.table_captions.get(table.locator, 0) + 1
        if tag == "th" and attr_map.get("scope") not in {"row", "col", "rowgroup", "colgroup"}:
            self._violate("TABLE_HEADER_SCOPE_MISSING", locator, "th scope required")
        if tag not in self._VOID_TAGS:
            self.stack.append(_Frame(tag=tag, locator=locator, attrs=attr_map))

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)

    def handle_data(self, data: str) -> None:
        for frame in self.stack:
            frame.text_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if not self.stack:
            return
        index = next(
            (
                position
                for position in range(len(self.stack) - 1, -1, -1)
                if self.stack[position].tag == tag
            ),
            None,
        )
        if index is None:
            return
        frame = self.stack[index]
        text = " ".join(frame.text_parts).strip()
        if "data-always-visible" in frame.attrs and not text:
            self._violate(
                "ALWAYS_VISIBLE_TEXT_EMPTY",
                frame.locator,
                frame.attrs["data-always-visible"],
            )
        del self.stack[index:]

    def finish(self) -> None:
        if self.h1_count != 1:
            self._violate("H1_COUNT_INVALID", "document", str(self.h1_count))
        for previous, current in zip(self.heading_levels, self.heading_levels[1:], strict=False):
            if current[0] > previous[0] + 1:
                self._violate("HEADING_LEVEL_SKIPPED", current[1], f"h{previous[0]}->h{current[0]}")
        for landmark in ("main", "nav", "footer"):
            if landmark not in self.landmarks:
                self._violate("LANDMARK_MISSING", "document", landmark)
        if not self.skip_link_found:
            self._violate("SKIP_LINK_MISSING", "document", "href=#main-content")
        for locator, count in self.card_disclosure_counts.items():
            if count > 1:
                self._violate("CARD_DISCLOSURE_BUDGET_EXCEEDED", locator, str(count))
        for locator, count in self.table_captions.items():
            if count != 1:
                self._violate("TABLE_CAPTION_INVALID", locator, str(count))
        for (reader_section, term_id), occurrences in self.term_occurrences.items():
            first_positions = [
                position
                for position, (_, first, _) in enumerate(occurrences)
                if first
            ]
            if len(first_positions) != 1:
                self._violate(
                    "TERM_FIRST_USE_COUNT_INVALID",
                    occurrences[0][0],
                    f"{reader_section}:{term_id}:{len(first_positions)}",
                )
            elif first_positions[0] != 0:
                self._violate(
                    "TERM_FIRST_USE_ORDER_INVALID",
                    occurrences[first_positions[0]][0],
                    f"{reader_section}:{term_id}",
                )
            for locator, _, description_id in occurrences:
                if description_id and description_id not in self.element_ids:
                    self._violate(
                        "TERM_DESCRIPTION_TARGET_MISSING",
                        locator,
                        description_id,
                    )


def validate_reader_accessibility(html_bytes: bytes) -> ReaderAccessibilityValidationResult:
    try:
        html = html_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ReaderAccessibilityValidationError("READER_A11Y_HTML_UTF8_INVALID") from exc
    parser = _AccessibilityParser()
    parser.feed(html)
    parser.close()
    parser.finish()
    expected_order = tuple(item.value for item in ReaderSectionId)
    observed_order = tuple(parser.section_order)
    if observed_order != expected_order:
        parser.violations.append(
            ReaderAccessibilityViolation(
                "READER_SECTION_ORDER_INVALID",
                "document",
                f"expected={expected_order}:actual={observed_order}",
            )
        )
    violations = tuple(
        sorted(
            parser.violations,
            key=lambda item: (item.code, item.locator, item.detail),
        )
    )
    status = "PASS" if not violations else "FAIL"
    return ReaderAccessibilityValidationResult(
        html_sha256=hashlib.sha256(html_bytes).hexdigest(),
        status=status,
        violations=violations,
        section_order=observed_order,
        automated_engineering_status=status,
    )


__all__ = [
    "ReaderAccessibilityValidationError",
    "ReaderAccessibilityValidationResult",
    "ReaderAccessibilityViolation",
    "validate_reader_accessibility",
]
