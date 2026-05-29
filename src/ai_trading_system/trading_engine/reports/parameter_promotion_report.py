from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

PARAMETER_PROMOTION_REPORT_TYPE = "parameter_promotion_decision"
PARAMETER_PROMOTION_SCHEMA_VERSION = 1


def default_parameter_promotion_output_dir(output_root: Path, as_of: date) -> Path:
    return output_root / as_of.isoformat()


def default_parameter_promotion_json_path(output_root: Path, as_of: date) -> Path:
    return (
        default_parameter_promotion_output_dir(output_root, as_of)
        / "parameter_promotion_decision.json"
    )


def default_parameter_promotion_markdown_path(output_root: Path, as_of: date) -> Path:
    return (
        default_parameter_promotion_output_dir(output_root, as_of)
        / "parameter_promotion_decision.md"
    )


def write_parameter_promotion_decision(
    payload: dict[str, Any],
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(render_parameter_promotion_markdown(payload), encoding="utf-8")
    return json_path, markdown_path


def render_parameter_promotion_markdown(payload: dict[str, Any]) -> str:
    decision = _mapping(payload.get("promotion_decision"))
    metadata = _mapping(payload.get("metadata"))
    lines = [
        "# Parameter Promotion Decision",
        "",
        f"- status：`{decision.get('status', 'UNKNOWN')}`",
        f"- reason：{decision.get('reason', '')}",
        f"- production_effect：`{metadata.get('production_effect', 'none')}`",
        f"- manual_review_required：`{metadata.get('manual_review_required', True)}`",
        f"- auto_promotion：`{metadata.get('auto_promotion', False)}`",
        f"- baseline_parameter_version：`{metadata.get('baseline_parameter_version', 'UNKNOWN')}`",
        "- candidate_parameter_version："
        f"`{metadata.get('candidate_parameter_version', 'UNKNOWN')}`",
        "",
        "## Hard Rejections",
        "",
    ]
    hard_rejections = [str(item) for item in decision.get("hard_rejections", []) if str(item)]
    lines.extend([f"- `{item}`" for item in hard_rejections] or ["- none"])
    lines.extend(["", "## Manual Review Items", ""])
    review_items = [str(item) for item in decision.get("manual_review_items", []) if str(item)]
    lines.extend([f"- `{item}`" for item in review_items] or ["- none"])
    lines.extend(
        [
            "",
            "## Safety Boundary",
            "",
            "- 本报告只提供人工复核建议，不修改 production 参数、权重、"
            "hard gates、broker 或交易动作。",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def reports_alias_paths(reports_dir: Path, as_of: date) -> tuple[Path, Path]:
    return (
        reports_dir / f"parameter_promotion_decision_{as_of.isoformat()}.json",
        reports_dir / f"parameter_promotion_decision_{as_of.isoformat()}.md",
    )


def write_parameter_promotion_report_alias(
    payload: dict[str, Any],
    reports_dir: Path,
    as_of: date,
) -> tuple[Path, Path]:
    json_path, markdown_path = reports_alias_paths(reports_dir, as_of)
    return write_parameter_promotion_decision(payload, json_path, markdown_path)


def load_parameter_promotion_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _mapping(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}
