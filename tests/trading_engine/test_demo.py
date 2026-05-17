from __future__ import annotations

import importlib.util
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DEMO_PATH = REPO_ROOT / "scripts" / "run_paper_trading_demo.py"
_DEMO_SPEC = importlib.util.spec_from_file_location("run_paper_trading_demo", DEMO_PATH)
assert _DEMO_SPEC is not None
_DEMO_MODULE = importlib.util.module_from_spec(_DEMO_SPEC)
assert _DEMO_SPEC.loader is not None
_DEMO_SPEC.loader.exec_module(_DEMO_MODULE)
run_demo = _DEMO_MODULE.run_demo


def test_paper_trading_demo_generates_report_and_audit_logs(tmp_path: Path) -> None:
    summary = run_demo(
        as_of=date(2026, 5, 17),
        audit_root=tmp_path / "audit",
        report_dir=tmp_path / "reports",
    )

    assert summary["generated_intents"] == 3
    assert summary["approved"] == 2
    assert summary["rejected"] == 1
    assert summary["submitted"] == 2
    assert summary["filled"] == 1
    assert summary["open"] == 1
    report_path = summary["report_path"]
    assert isinstance(report_path, Path)
    assert report_path.exists()
    text = report_path.read_text(encoding="utf-8")
    assert "Paper Trading Daily Report" in text
    assert "风控通过 / 拒绝：2 / 1" in text
    assert (tmp_path / "audit" / "risk_check_log" / "2026-05-17.jsonl").exists()
