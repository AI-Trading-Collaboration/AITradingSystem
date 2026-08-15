from __future__ import annotations

from pathlib import Path

import pytest

from ai_trading_system.atlas.reader_terminology_projection import (
    ReaderTerminologyPolicy,
    load_reader_terminology_policy,
)
from ai_trading_system.atlas.rendered_term_inventory import (
    RenderedTermInventoryError,
    build_rendered_term_inventory,
)
from ai_trading_system.contracts.strategy_research_reader_terminology import (
    ReaderTermClassification,
    ReaderTermDefinition,
    ReaderTerminologyContractError,
    ReaderTextSurface,
    RenderedTermInventory,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _glossary(policy: ReaderTerminologyPolicy) -> str:
    return "".join(
        (
            f'<article id="term-{term.term_id}" data-term-definition="{term.term_id}">'
            f"<h2>{term.display_name_zh}</h2>"
            f"<span>{term.aliases[0]}</span>"
            f"<p>{term.plain_definition_zh}</p>"
            f"<p>{term.why_needed_zh}</p>"
            "</article>"
        )
        for term in policy.terms
    )


def _html(policy: ReaderTerminologyPolicy, body: str) -> bytes:
    return (
        "<!doctype html><html><head><title>策略研究页面</title>"
        "<style>.x{color:red}</style></head><body>"
        f'<details><summary>词语说明</summary>{_glossary(policy)}</details>'
        f"{body}</body></html>"
    ).encode()


def test_rendered_term_inventory_is_exact_and_replayable() -> None:
    policy = load_reader_terminology_policy(repository_root=PROJECT_ROOT)
    html = _html(
        policy,
        '<main aria-label="策略研究解释"><p>DQ/PIT 用于核对 primary-window。</p>'
        '<details data-reader-layer="audit"><summary>审计详情</summary>'
        '<code>TRADING-2523_SAMPLE_ID</code></details></main>',
    )
    inventory = build_rendered_term_inventory(html_bytes=html, policy=policy)
    assert inventory.scanned_surface_count == len(inventory.surfaces)
    assert inventory.scanned_surface_count > len(policy.terms)
    assert inventory.audit_identifier_count >= 1
    assert inventory.audit_identifier_count == len(inventory.audit_identifiers)
    assert any(
        item.identifier == "TRADING-2523_SAMPLE_ID"
        for item in inventory.audit_identifiers
    )
    assert any(
        item.normalized_text == "DQ/PIT 用于核对 primary-window。"
        for item in inventory.surfaces
    )
    assert {"keep_closed", "dq_pit", "strategy_pass"} <= {
        item.term_id for item in inventory.resolutions
    }
    replayed = RenderedTermInventory.from_json_bytes(inventory.canonical_bytes)
    assert replayed == inventory
    assert replayed.html_sha256 == inventory.html_sha256


def test_rendered_term_inventory_rejects_unknown_reader_identifier() -> None:
    policy = load_reader_terminology_policy(repository_root=PROJECT_ROOT)
    html = _html(
        policy,
        "<main><p>UNREGISTERED_RUNTIME_STATE</p></main>",
    )
    with pytest.raises(
        RenderedTermInventoryError,
        match="RENDERED_TERM_UNKNOWN_IDENTIFIER",
    ):
        build_rendered_term_inventory(html_bytes=html, policy=policy)


@pytest.mark.parametrize(
    "body",
    (
        "<details><summary>展开说明</summary><p>UNREGISTERED_CLOSED_STATE</p></details>",
        "<main><p>unregistered-runtime-identifier</p></main>",
    ),
)
def test_rendered_term_inventory_checks_all_reader_disclosures_and_runtime_ids(
    body: str,
) -> None:
    policy = load_reader_terminology_policy(repository_root=PROJECT_ROOT)
    with pytest.raises(
        RenderedTermInventoryError,
        match="RENDERED_TERM_UNKNOWN_IDENTIFIER",
    ):
        build_rendered_term_inventory(
            html_bytes=_html(policy, body),
            policy=policy,
        )


def test_rendered_term_inventory_allows_raw_identifiers_only_in_audit_layer() -> None:
    policy = load_reader_terminology_policy(repository_root=PROJECT_ROOT)
    inventory = build_rendered_term_inventory(
        html_bytes=_html(
            policy,
            '<details data-reader-layer="audit"><summary>审计详情</summary>'
            "<p>TRADING-9999 UNREGISTERED_RUNTIME_STATE raw-runtime-identifier</p>"
            "</details>",
        ),
        policy=policy,
    )
    assert inventory.audit_identifier_count == 3
    assert {item.identifier for item in inventory.audit_identifiers} == {
        "TRADING-9999",
        "UNREGISTERED_RUNTIME_STATE",
        "raw-runtime-identifier",
    }


def test_rendered_term_inventory_rejects_alias_case_drift() -> None:
    policy = load_reader_terminology_policy(repository_root=PROJECT_ROOT)
    with pytest.raises(
        RenderedTermInventoryError,
        match="RENDERED_TERM_ALIAS_CASE_DRIFT:keep_closed:Keep_Closed",
    ):
        build_rendered_term_inventory(
            html_bytes=_html(policy, "<main><p>Keep_Closed</p></main>"),
            policy=policy,
        )


def test_rendered_term_inventory_rejects_registered_raw_id_on_default_layer() -> None:
    policy = load_reader_terminology_policy(repository_root=PROJECT_ROOT)
    with pytest.raises(
        RenderedTermInventoryError,
        match="RENDERED_TERM_RAW_IDENTIFIER_EXPOSED:keep_closed:KEEP_CLOSED",
    ):
        build_rendered_term_inventory(
            html_bytes=_html(policy, "<main><p>KEEP_CLOSED</p></main>"),
            policy=policy,
        )


def test_rendered_term_inventory_covers_accessible_attributes() -> None:
    policy = load_reader_terminology_policy(repository_root=PROJECT_ROOT)
    inventory = build_rendered_term_inventory(
        html_bytes=_html(
            policy,
            '<p id="term-description">source lineage 说明</p>'
            '<button aria-label="DQ/PIT" title="primary-window" '
            'aria-describedby="term-description">打开说明</button>',
        ),
        policy=policy,
    )
    surfaces = {
        item.surface
        for item in inventory.occurrences
        if item.term_id in {"source_lineage", "dq_pit", "primary_window"}
    }
    assert surfaces >= {
        # These attributes are reader-facing even when they are not visible on screen.
        ReaderTextSurface.ARIA_LABEL,
        ReaderTextSurface.ARIA_DESCRIPTION,
        ReaderTextSurface.TITLE,
    }


def test_rendered_term_inventory_rejects_missing_aria_description_target() -> None:
    policy = load_reader_terminology_policy(repository_root=PROJECT_ROOT)
    with pytest.raises(
        RenderedTermInventoryError,
        match="RENDERED_TERM_ARIA_DESCRIPTION_TARGET_MISSING:missing-description",
    ):
        build_rendered_term_inventory(
            html_bytes=_html(
                policy,
                '<button aria-describedby="missing-description">打开说明</button>',
            ),
            policy=policy,
        )


def test_rendered_term_inventory_rejects_duplicate_definition_target() -> None:
    policy = load_reader_terminology_policy(repository_root=PROJECT_ROOT)
    with pytest.raises(
        RenderedTermInventoryError,
        match="RENDERED_TERM_DEFINITION_DUPLICATE:keep_closed",
    ):
        build_rendered_term_inventory(
            html_bytes=_html(
                policy,
                '<section data-term-definition="keep_closed">KEEP_CLOSED</section>',
            ),
            policy=policy,
        )


def test_rendered_term_inventory_rejects_audit_only_term_on_reader_layer() -> None:
    base = load_reader_terminology_policy(repository_root=PROJECT_ROOT)
    audit_term = ReaderTermDefinition(
        term_id="audit_secret",
        display_name_zh="仅审计术语",
        aliases=("AUDIT_SECRET",),
        classification=ReaderTermClassification.AUDIT_ONLY,
        plain_definition_zh="只允许出现在审计层的测试标识。",
        why_needed_zh="证明审计层不能成为读者层的旁路。",
    )
    policy = ReaderTerminologyPolicy(
        policy_id=base.policy_id,
        policy_version=base.policy_version,
        status=base.status,
        owner=base.owner,
        reader_profile=base.reader_profile,
        reader_profile_sha256=base.reader_profile_sha256,
        terminology_policy_sha256=base.terminology_policy_sha256,
        unknown_identifier_patterns=base.unknown_identifier_patterns,
        excluded_non_reader_regions=base.excluded_non_reader_regions,
        raw_reader_replacements=base.raw_reader_replacements,
        terms=(*base.terms, audit_term),
        safety=base.safety,
    )
    html = _html(
        base,
        '<section data-reader-layer="audit" data-term-definition="audit_secret">'
        "<p>AUDIT_SECRET</p></section><main><p>AUDIT_SECRET</p></main>",
    )
    with pytest.raises(
        RenderedTermInventoryError,
        match="RENDERED_TERM_AUDIT_ONLY_EXPOSED:audit_secret",
    ):
        build_rendered_term_inventory(html_bytes=html, policy=policy)


def test_rendered_term_inventory_rejects_noncanonical_replay_bytes() -> None:
    policy = load_reader_terminology_policy(repository_root=PROJECT_ROOT)
    inventory = build_rendered_term_inventory(
        html_bytes=_html(policy, "<main><p>普通读者文字</p></main>"),
        policy=policy,
    )
    with pytest.raises(
        ReaderTerminologyContractError,
        match="RENDERED_TERM_NON_CANONICAL_BYTES",
    ):
        RenderedTermInventory.from_json_bytes(inventory.canonical_bytes + b" ")


def test_rendered_term_inventory_rejects_late_first_use() -> None:
    base = load_reader_terminology_policy(repository_root=PROJECT_ROOT)
    term = ReaderTermDefinition(
        term_id="late_term",
        display_name_zh="稍后解释",
        aliases=("late term",),
        classification=ReaderTermClassification.GLOSSARY,
        plain_definition_zh="这是一个故意放迟的测试术语。",
        why_needed_zh="用于证明首现顺序会 fail closed。",
    )
    policy = ReaderTerminologyPolicy(
        policy_id=base.policy_id,
        policy_version=base.policy_version,
        status=base.status,
        owner=base.owner,
        reader_profile=base.reader_profile,
        reader_profile_sha256=base.reader_profile_sha256,
        terminology_policy_sha256=base.terminology_policy_sha256,
        unknown_identifier_patterns=base.unknown_identifier_patterns,
        excluded_non_reader_regions=base.excluded_non_reader_regions,
        raw_reader_replacements=base.raw_reader_replacements,
        terms=(*base.terms, term),
        safety=base.safety,
    )
    html = (
        "<!doctype html><html><head><title>策略研究页面</title></head><body>"
        "<p>late term</p>"
        f'<section data-term-definition="late_term"><h2>{term.display_name_zh}</h2>'
        f"<p>{term.plain_definition_zh}</p><p>{term.why_needed_zh}</p></section>"
        f"{_glossary(base)}</body></html>"
    ).encode()
    with pytest.raises(
        RenderedTermInventoryError,
        match="RENDERED_TERM_EXPLANATION_AFTER_FIRST_USE",
    ):
        build_rendered_term_inventory(html_bytes=html, policy=policy)


def test_rendered_term_inventory_requires_all_definition_targets() -> None:
    policy = load_reader_terminology_policy(repository_root=PROJECT_ROOT)
    html = b"<!doctype html><html><body><p>plain text</p></body></html>"
    with pytest.raises(
        RenderedTermInventoryError,
        match="RENDERED_TERM_DEFINITION_SET_INVALID",
    ):
        build_rendered_term_inventory(html_bytes=html, policy=policy)
