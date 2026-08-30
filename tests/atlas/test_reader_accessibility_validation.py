from __future__ import annotations

from ai_trading_system.atlas.reader_accessibility_validation import (
    validate_reader_accessibility,
)
from ai_trading_system.contracts.strategy_research_reader_projection import ReaderSectionId


def _sections() -> str:
    return "".join(
        f'<section data-reader-section="{item.value}"><h2>{item.value}</h2></section>'
        for item in ReaderSectionId
    )


def _valid_html() -> bytes:
    return f"""
    <!doctype html><html lang="zh-CN"><body>
    <a href="#main-content">跳到主要内容</a>
    <nav aria-label="页面导航"><a href="#main-content">阅读研究</a></nav>
    <main id="main-content"><h1>策略研究</h1>
    {_sections()}
    <article data-reader-card="problem"><h2>为什么研究</h2>
      <span data-term-trigger="evidence" data-term-first="true" tabindex="0"
        aria-describedby="term-evidence">证据</span>
      <span id="term-evidence">支持有限陈述的记录。</span>
      <details><summary>查看数据范围与限制</summary><p>范围说明</p></details>
    </article>
    <p data-always-visible="critical-risk">当前证据不能支持策略有效。</p>
    <table><caption>日期范围</caption><tr><th scope="col">字段</th></tr></table>
    </main><footer>审计入口</footer></body></html>
    """.encode()


def test_accessibility_validator_accepts_reader_contract_fixture() -> None:
    result = validate_reader_accessibility(_valid_html())

    assert result.status == "PASS"
    assert result.violations == ()
    assert result.section_order == tuple(item.value for item in ReaderSectionId)
    assert result.owner_visual_status == "PENDING_REVIEW"
    assert result.reader_comprehension_status == "PENDING_REVIEW"


def test_accessibility_validator_rejects_nested_disclosure_and_hidden_risk() -> None:
    invalid = _valid_html().replace(
        b"<p>\xe8\x8c\x83\xe5\x9b\xb4\xe8\xaf\xb4\xe6\x98\x8e</p></details>",
        (
            b'<p data-always-visible="critical-risk">risk</p>'
            b"<details><summary>nested</summary></details></details>"
        ),
    )
    result = validate_reader_accessibility(invalid)
    codes = {item.code for item in result.violations}

    assert result.status == "FAIL"
    assert "NESTED_DISCLOSURE" in codes
    assert "ALWAYS_VISIBLE_INSIDE_DISCLOSURE" in codes


def test_accessibility_validator_rejects_term_hover_only_and_extra_tab_stop() -> None:
    invalid = _valid_html().replace(
        b'data-term-first="true" tabindex="0"\n        aria-describedby="term-evidence"',
        b'data-term-first="false" tabindex="0" title="definition"',
    )
    result = validate_reader_accessibility(invalid)
    codes = {item.code for item in result.violations}

    assert "TERM_DESCRIPTION_MISSING" in codes
    assert "TERM_REPEAT_ADDS_TAB_STOP" in codes
    assert "TERM_TITLE_ONLY" in codes


def test_accessibility_validator_requires_one_source_ordered_first_use() -> None:
    invalid = _valid_html().replace(
        b'data-term-first="true"',
        b'data-term-first="false"',
        1,
    )
    result = validate_reader_accessibility(invalid)

    assert any(item.code == "TERM_FIRST_USE_COUNT_INVALID" for item in result.violations)


def test_accessibility_validator_requires_real_description_target() -> None:
    invalid = _valid_html().replace(b'id="term-evidence"', b'id="other-term"', 1)
    result = validate_reader_accessibility(invalid)

    assert any(item.code == "TERM_DESCRIPTION_TARGET_MISSING" for item in result.violations)


def test_accessibility_validator_rejects_reader_section_reordering() -> None:
    invalid = _valid_html().replace(
        b'data-reader-section="TRUST_STRIP"',
        b'data-reader-section="AUDIT_DESTINATIONS"',
        1,
    )
    result = validate_reader_accessibility(invalid)

    assert any(item.code == "READER_SECTION_ORDER_INVALID" for item in result.violations)
