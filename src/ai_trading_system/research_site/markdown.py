"""Small, escaped Markdown renderer for repository research documents."""

from __future__ import annotations

import html
import re


def cells(line: str) -> list[str]:
    """The repository's pipe-table cell convention."""
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def _inline(text: str) -> str:
    # Tokenize before formatting so bold markers inside code stay literal.
    parts = re.split(r"(`[^`]+`)", text)
    return "".join(
        "<code>" + html.escape(part[1:-1], quote=False) + "</code>"
        if part.startswith("`") and part.endswith("`")
        else re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", html.escape(part, quote=False))
        for part in parts
    )


def markdown_to_html(text: str) -> str:
    """Render only headings, lists, pipe tables, paragraphs, code and bold."""
    lines = text.splitlines()
    blocks: list[str] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        if not line.strip():
            index += 1
            continue
        heading = re.match(r"^(#{1,4}) (.*)$", line)
        if heading:
            level = len(heading[1])
            blocks.append(f"<h{level}>{_inline(heading[2])}</h{level}>")
            index += 1
        elif line.startswith("|"):
            headers = cells(line)
            table = ['<div class="wrap"><table class="stack"><thead><tr>']
            table.extend(f"<th>{_inline(cell)}</th>" for cell in headers)
            table.append("</tr></thead><tbody>")
            index += 1
            while index < len(lines) and lines[index].startswith("|"):
                row = cells(lines[index])
                if not all(re.fullmatch(r":?-{3,}:?", cell) for cell in row):
                    table.append("<tr>")
                    for column, cell in enumerate(row):
                        label = headers[column] if column < len(headers) else ""
                        table.append(
                            f'<td data-label="{html.escape(label, quote=True)}">'
                            f"{_inline(cell)}</td>"
                        )
                    table.append("</tr>")
                index += 1
            table.append("</tbody></table></div>")
            blocks.append("".join(table))
        elif line.startswith("- "):
            items = ["<ul>"]
            while index < len(lines) and lines[index].startswith("- "):
                items.append(f"<li>{_inline(lines[index][2:])}</li>")
                index += 1
            items.append("</ul>")
            blocks.append("".join(items))
        else:
            paragraph = [line]
            index += 1
            while (
                index < len(lines)
                and lines[index].strip()
                and not lines[index].startswith(("|", "- "))
                and not re.match(r"^#{1,4} ", lines[index])
            ):
                paragraph.append(lines[index])
                index += 1
            blocks.append(f"<p>{_inline(chr(10).join(paragraph))}</p>")
    return "\n".join(blocks)
