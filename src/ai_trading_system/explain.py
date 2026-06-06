from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Literal

import yaml

ExplainKind = Literal["auto", "field", "artifact", "gate"]

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_FIELDS_PATH = PROJECT_ROOT / "docs" / "schema" / "fields.yaml"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"
DEFAULT_CALCULATION_LOGIC_PATH = PROJECT_ROOT / "docs" / "calculation_logic.md"

_GATE_EXPLAINERS: dict[str, dict[str, object]] = {
    "data gate": {
        "title": "Data Gate / 数据质量门禁",
        "meaning": "在特征、评分、回测或报告使用 cached market/macro data 前执行质量校验。",
        "produced_by": "aits validate-data 或调用同一 validation code path 的运行命令",
        "upstream_fields": [
            "data/raw/prices_daily.csv",
            "data/raw/rates_daily.csv",
            "download manifest / reconciliation evidence",
        ],
        "downstream_usage": [
            "score-daily",
            "backtest",
            "daily report",
            "Reader Brief",
        ],
        "production_effect": "fail_closed_quality_gate",
        "common_misunderstanding": "Data Gate PASS 只说明缓存可用，不等于投资结论可靠。",
    },
    "binding gate": {
        "title": "Binding Gate / 最严格仓位闸门",
        "meaning": "在多个 position gate 中实际压低 final position 的最严格限制。",
        "produced_by": "score-daily position gate evaluation / decision snapshot",
        "upstream_fields": [
            "model risk asset AI position",
            "confidence-adjusted position",
            "macro risk budget",
            "valuation/thesis/risk/portfolio caps",
        ],
        "downstream_usage": [
            "daily report Binding Gate Ladder",
            "decision snapshot",
            "Reader Brief score-to-position funnel",
        ],
        "production_effect": "advisory_position_cap",
        "common_misunderstanding": (
            "最高分模块不是最终仓位原因；binding gate 才说明哪个限制真正约束了仓位。"
        ),
    },
    "position gate": {
        "title": "Position Gate / 仓位闸门",
        "meaning": "把模型仓位按置信度、宏观预算、估值、thesis、风险事件和组合约束逐层限制。",
        "produced_by": "score-daily decision builder",
        "upstream_fields": [
            "overall score",
            "confidence",
            "macro risk asset budget",
            "risk events",
            "valuation snapshots",
            "trade theses",
        ],
        "downstream_usage": [
            "final_risk_asset_ai_position",
            "daily report",
            "decision snapshot",
            "prediction ledger",
        ],
        "production_effect": "advisory_position_cap",
        "common_misunderstanding": "Position gate 是解释和限制层，不是 broker 下单指令。",
    },
}


def explain_query(
    query: str,
    *,
    kind: ExplainKind = "auto",
    fields_path: Path = DEFAULT_FIELDS_PATH,
    artifact_catalog_path: Path = DEFAULT_ARTIFACT_CATALOG_PATH,
    calculation_logic_path: Path = DEFAULT_CALCULATION_LOGIC_PATH,
) -> dict[str, Any]:
    term = query.strip()
    if not term:
        raise ValueError("query must not be empty")
    if kind not in {"auto", "field", "artifact", "gate"}:
        raise ValueError(f"unsupported explain kind: {kind}")

    if kind in {"auto", "field"}:
        field_result = _explain_field(term, fields_path)
        if field_result["found"] or kind == "field":
            return field_result
    if kind in {"auto", "gate"}:
        gate_result = _explain_gate(term, calculation_logic_path)
        if gate_result["found"] or kind == "gate":
            return gate_result
    if kind in {"auto", "artifact"}:
        artifact_result = _explain_artifact(term, artifact_catalog_path)
        if artifact_result["found"] or kind == "artifact":
            return artifact_result
    return _missing_result(term, kind)


def render_explain_result(result: dict[str, Any]) -> str:
    query = str(result.get("query", ""))
    if not result.get("found"):
        return "\n".join(
            [
                f"# 解释查询：{query}",
                "",
                "- 状态：MISSING",
                f"- 类型：{result.get('kind', 'auto')}",
                f"- 说明：{result.get('message', '未找到解释。')}",
                "",
                "建议：先检查 `docs/schema/fields.yaml`、`docs/artifact_catalog.md` "
                "和 `docs/calculation_logic.md`，或改用更精确的字段 / artifact 名称。",
            ]
        )

    kind = str(result.get("kind", ""))
    if kind == "artifact":
        return _render_artifact_result(result)
    return _render_single_result(result)


def _explain_field(query: str, fields_path: Path) -> dict[str, Any]:
    fields = _load_fields(fields_path)
    normalized_query = _normalize(query)
    key = query if query in fields else _find_field_key(normalized_query, fields)
    if key is None:
        return _missing_result(query, "field")
    payload = fields[key]
    if not isinstance(payload, dict):
        return _missing_result(query, "field")
    return {
        "found": True,
        "kind": "field",
        "query": query,
        "id": key,
        "source_path": str(fields_path),
        "title": key,
        "meaning": payload.get("meaning", ""),
        "produced_by": payload.get("produced_by", ""),
        "upstream_fields": _list(payload.get("upstream_fields")),
        "downstream_usage": _list(payload.get("downstream_usage")),
        "production_effect": payload.get("production_effect", ""),
        "common_misunderstanding": payload.get("common_misunderstanding", ""),
    }


def _explain_gate(query: str, calculation_logic_path: Path) -> dict[str, Any]:
    normalized_query = _normalize(query)
    for key, payload in _GATE_EXPLAINERS.items():
        if key in normalized_query or normalized_query in _normalize(str(payload["title"])):
            return {
                "found": True,
                "kind": "gate",
                "query": query,
                "id": key,
                "source_path": str(calculation_logic_path),
                **payload,
            }
    return _missing_result(query, "gate")


def _explain_artifact(query: str, artifact_catalog_path: Path) -> dict[str, Any]:
    normalized_query = _normalize(query)
    matches: list[dict[str, str]] = []
    for line in artifact_catalog_path.read_text(encoding="utf-8").splitlines():
        if not line.startswith("|") or normalized_query not in _normalize(line):
            continue
        cells = _split_markdown_table_row(line)
        if len(cells) < 7 or set(cells[0]) <= {"-", ":"} or cells[0] == "Artifact":
            continue
        matches.append(
            {
                "artifact": cells[0],
                "producer": cells[1],
                "inputs": cells[2],
                "key_fields": cells[3],
                "downstream_usage": cells[4],
                "production_effect": cells[5],
                "common_misunderstanding": cells[6],
            }
        )
    if not matches:
        return _missing_result(query, "artifact")
    return {
        "found": True,
        "kind": "artifact",
        "query": query,
        "id": query,
        "source_path": str(artifact_catalog_path),
        "matches": matches[:5],
    }


def _render_single_result(result: dict[str, Any]) -> str:
    lines = [
        f"# 解释查询：{result['query']}",
        "",
        "- 状态：FOUND",
        f"- 类型：{result['kind']}",
        f"- ID：`{result['id']}`",
        f"- 来源：`{result['source_path']}`",
        f"- 含义：{result.get('meaning', '')}",
        f"- 生成者：{result.get('produced_by', '')}",
        f"- production_effect：{result.get('production_effect', '')}",
        f"- 常见误解：{result.get('common_misunderstanding', '')}",
        "",
        "## 上游输入",
    ]
    lines.extend(f"- {item}" for item in _list(result.get("upstream_fields")))
    lines.extend(["", "## 下游用途"])
    lines.extend(f"- {item}" for item in _list(result.get("downstream_usage")))
    return "\n".join(lines)


def _render_artifact_result(result: dict[str, Any]) -> str:
    lines = [
        f"# 解释查询：{result['query']}",
        "",
        "- 状态：FOUND",
        "- 类型：artifact",
        f"- 来源：`{result['source_path']}`",
        "",
    ]
    for index, match in enumerate(result["matches"], start=1):
        lines.extend(
            [
                f"## 匹配 {index}",
                f"- Artifact：{match['artifact']}",
                f"- 生成者：{match['producer']}",
                f"- 上游输入：{match['inputs']}",
                f"- 关键字段：{match['key_fields']}",
                f"- 下游用途：{match['downstream_usage']}",
                f"- production_effect：{match['production_effect']}",
                f"- 常见误解：{match['common_misunderstanding']}",
                "",
            ]
        )
    return "\n".join(lines).rstrip()


def _load_fields(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    fields = payload.get("fields", {}) if isinstance(payload, dict) else {}
    if not isinstance(fields, dict):
        return {}
    return fields


def _find_field_key(normalized_query: str, fields: dict[str, Any]) -> str | None:
    for key in fields:
        normalized_key = _normalize(key)
        if normalized_query == normalized_key or normalized_key.endswith(f".{normalized_query}"):
            return key
    for key in fields:
        if normalized_query in _normalize(key):
            return key
    return None


def _split_markdown_table_row(line: str) -> list[str]:
    stripped = line.strip().strip("|")
    return [re.sub(r"<br\s*/?>", " / ", cell.strip()) for cell in stripped.split("|")]


def _missing_result(query: str, kind: str) -> dict[str, Any]:
    return {
        "found": False,
        "kind": kind,
        "query": query,
        "message": "没有在当前字段字典、gate explainer 或 artifact catalog 中找到匹配项。",
    }


def _normalize(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower())


def _list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, tuple):
        return [str(item) for item in value]
    if value is None or value == "":
        return []
    return [str(value)]
