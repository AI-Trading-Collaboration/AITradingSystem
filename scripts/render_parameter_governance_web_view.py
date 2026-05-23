from __future__ import annotations

import argparse
import sys
import webbrowser
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.parameter_governance_web_view import (  # noqa: E402
    DEFAULT_DATA_ROOT,
    write_parameter_governance_web_view,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Render a read-only static parameter governance web view."
    )
    parser.add_argument(
        "--date",
        help=(
            "Web view date in YYYY-MM-DD format. If omitted, the latest governance "
            "summary artifact is used."
        ),
    )
    parser.add_argument(
        "--data-root",
        default=str(DEFAULT_DATA_ROOT),
        help="Data root containing existing governance summary artifacts.",
    )
    parser.add_argument(
        "--governance-summary-file",
        help="Explicit parameter_governance_summary_YYYY-MM-DD.json path to read only.",
    )
    parser.add_argument("--output-file", help="Output HTML file path.")
    parser.add_argument("--metadata-file", help="Output render metadata JSON path.")
    parser.add_argument(
        "--open-browser",
        default="false",
        choices=("true", "false"),
        help="Open the generated local HTML file in the default browser.",
    )
    args = parser.parse_args()

    metadata = write_parameter_governance_web_view(
        as_of=date.fromisoformat(args.date) if args.date else None,
        data_root=Path(args.data_root),
        governance_summary_file=(
            Path(args.governance_summary_file) if args.governance_summary_file else None
        ),
        output_file=Path(args.output_file) if args.output_file else None,
        metadata_file=Path(args.metadata_file) if args.metadata_file else None,
    )
    output_artifacts = metadata["output_artifacts"]
    render_summary = metadata["render_summary"]
    print(f"Parameter Governance Web View：{metadata['render_decision']}")
    print(f"governance_state：{render_summary['governance_state']}")
    print(f"action_required：{render_summary['action_required']}")
    print(f"action_level：{render_summary['action_level']}")
    print(f"safety_boundary_status：{render_summary['safety_boundary_status']}")
    print(f"production_effect：{metadata['production_effect']}")
    print(f"manual_review_only：{metadata['manual_review_only']}")
    print(f"governance_only：{metadata['governance_only']}")
    print(f"web_view_only：{metadata['web_view_only']}")
    print(f"apply_executed_by_web_view：{metadata['apply_executed_by_web_view']}")
    print(f"rollback_executed_by_web_view：{metadata['rollback_executed_by_web_view']}")
    print(f"broker_execution：{metadata['broker_execution']}")
    print(f"replay_execution：{metadata['replay_execution']}")
    print(f"trading_execution：{metadata['trading_execution']}")
    print(f"HTML：{output_artifacts['html']['path']}")
    print(f"Metadata：{output_artifacts['metadata']['path']}")

    if args.open_browser == "true":
        webbrowser.open(Path(output_artifacts["html"]["path"]).resolve().as_uri())


if __name__ == "__main__":
    main()
