from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.brokers.ibkr_readonly import (  # noqa: E402
    DEFAULT_IBKR_PAPER_READONLY_CONFIG_PATH,
    IBKRPaperReadOnlyAdapter,
    IBKRPaperReadOnlyReconciliation,
    IBKRReadOnlyReconciliationIssue,
    IBKRReadOnlyReconciliationResult,
    IBKRReconciliationStatus,
    assess_paper_account_id,
    load_ibkr_paper_readonly_config,
    sanitize_ibkr_payload,
)
from ai_trading_system.trading_engine.portfolio.paper_portfolio import PaperPortfolio  # noqa: E402
from ai_trading_system.trading_engine.schemas.portfolio_state import PortfolioState  # noqa: E402


def run_snapshot(
    *,
    as_of: date,
    config_path: Path | str = DEFAULT_IBKR_PAPER_READONLY_CONFIG_PATH,
    output_dir: Path | str = REPO_ROOT / "outputs" / "reports",
    client: Any | None = None,
    sample_symbol: str = "NVDA",
    local_portfolio: PaperPortfolio | PortfolioState | None = None,
) -> dict[str, Any]:
    config = load_ibkr_paper_readonly_config(config_path)
    config.assert_readonly_paper_settings()
    account_assessment = assess_paper_account_id(config.account_id)
    payload = _base_payload(
        as_of=as_of,
        account_assessment=account_assessment.as_dict(),
        account_id_masked=account_assessment.masked_account_id,
        readonly=config.readonly,
        production_effect=config.production_effect,
    )

    if not config.enabled:
        payload["connection_status"] = {
            "status": "DISABLED",
            "connected": False,
            "reason": "config enabled=false",
            "production_effect": "none",
            "readonly": True,
        }
        payload["reconciliation"] = _blocked_reconciliation(
            "IBKR Paper read-only sync is disabled by config"
        ).model_dump(mode="json")
        payload["reconciliation_status"] = IBKRReconciliationStatus.BLOCK.value
        return _write_outputs(payload=payload, as_of=as_of, output_dir=Path(output_dir))

    if account_assessment.status != IBKRReconciliationStatus.PASS:
        payload["connection_status"] = {
            "status": "BLOCKED_ACCOUNT_ID",
            "connected": False,
            "reason": "account_id does not pass paper account check",
            "production_effect": "none",
            "readonly": True,
        }
        payload["reconciliation"] = _blocked_reconciliation(
            "account_id does not look like an IBKR Paper / DUP account"
        ).model_dump(mode="json")
        payload["reconciliation_status"] = IBKRReconciliationStatus.BLOCK.value
        return _write_outputs(payload=payload, as_of=as_of, output_dir=Path(output_dir))

    adapter = IBKRPaperReadOnlyAdapter(config=config, client=client)
    try:
        payload["connection_status"] = adapter.connect()
        payload["account_summary"] = adapter.get_account_summary()
        payload["positions"] = adapter.list_positions()
        payload["open_orders"] = adapter.list_open_orders()
        payload["recent_executions"] = _recent_records(adapter.list_executions())
        payload["contract_details_sample"] = {
            "symbol": sample_symbol.upper(),
            "details": adapter.get_contract_details(symbol=sample_symbol),
        }
        reconciliation = IBKRPaperReadOnlyReconciliation().reconcile(
            account_summary=payload["account_summary"],
            positions=payload["positions"],
            local_portfolio=local_portfolio,
        )
        payload["reconciliation"] = reconciliation.model_dump(mode="json")
        payload["reconciliation_status"] = reconciliation.status.value
    except Exception as exc:
        payload["connection_status"] = {
            "status": "ERROR",
            "connected": False,
            "error_type": type(exc).__name__,
            "message": sanitize_ibkr_payload(str(exc), account_id=config.account_id),
            "production_effect": "none",
            "readonly": True,
        }
        payload["reconciliation"] = _blocked_reconciliation(
            "IBKR read-only snapshot failed before account data could be reconciled"
        ).model_dump(mode="json")
        payload["reconciliation_status"] = IBKRReconciliationStatus.BLOCK.value
    finally:
        adapter.disconnect()

    return _write_outputs(payload=payload, as_of=as_of, output_dir=Path(output_dir))


def render_markdown_snapshot(payload: dict[str, Any]) -> str:
    lines = [
        "# IBKR Paper Account Read-only Snapshot",
        "",
        f"- 日期：{payload['as_of']}",
        f"- Snapshot status：{payload['snapshot_status']}",
        f"- Connection status：{payload['connection_status'].get('status', 'UNKNOWN')}",
        f"- Account id：`{payload['account_id_masked']}`",
        f"- Reconciliation status：{payload['reconciliation_status']}",
        f"- production_effect={payload['production_effect']}",
        f"- readonly={str(payload['readonly']).lower()}",
        "- 安全边界：本报告只读读取 IBKR Paper 账户状态，不提交、不取消、不修改订单。",
        "",
        "## Account Summary",
        "",
        _json_block(payload.get("account_summary", [])),
        "",
        "## Positions",
        "",
        _json_block(payload.get("positions", [])),
        "",
        "## Open Orders",
        "",
        _json_block(payload.get("open_orders", [])),
        "",
        "## Recent Executions",
        "",
        _json_block(payload.get("recent_executions", [])),
        "",
        "## Contract Details Sample",
        "",
        _json_block(payload.get("contract_details_sample", {})),
        "",
        "## PaperPortfolio Reconciliation",
        "",
        _json_block(payload.get("reconciliation", {})),
    ]
    return "\n".join(lines).rstrip() + "\n"


def _base_payload(
    *,
    as_of: date,
    account_assessment: dict[str, Any],
    account_id_masked: str,
    readonly: bool,
    production_effect: str,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "report_type": "ibkr_paper_account_snapshot",
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "production_effect": production_effect,
        "readonly": readonly,
        "snapshot_status": "BLOCK",
        "connection_status": {"status": "NOT_RUN", "connected": False},
        "account_id_masked": account_id_masked,
        "account_id_assessment": account_assessment,
        "account_summary": [],
        "positions": [],
        "open_orders": [],
        "recent_executions": [],
        "contract_details_sample": {},
        "reconciliation_status": "BLOCK",
        "reconciliation": {},
    }


def _write_outputs(
    *,
    payload: dict[str, Any],
    as_of: date,
    output_dir: Path,
) -> dict[str, Any]:
    payload["snapshot_status"] = _snapshot_status(payload)
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"ibkr_paper_account_snapshot_{as_of.isoformat()}.json"
    markdown_path = output_dir / f"ibkr_paper_account_snapshot_{as_of.isoformat()}.md"
    payload["output_paths"] = {
        "json": str(json_path),
        "markdown": str(markdown_path),
    }
    sanitized_payload = sanitize_ibkr_payload(payload, account_id=_unmaskable_account_hint(payload))
    json_text = json.dumps(sanitized_payload, ensure_ascii=False, indent=2)
    markdown_text = render_markdown_snapshot(sanitized_payload)
    _assert_no_obvious_unmasked_account_id(json_text)
    _assert_no_obvious_unmasked_account_id(markdown_text)
    json_path.write_text(json_text + "\n", encoding="utf-8")
    markdown_path.write_text(markdown_text, encoding="utf-8")
    return sanitized_payload


def _snapshot_status(payload: dict[str, Any]) -> str:
    connection_status = payload.get("connection_status", {})
    if isinstance(connection_status, dict) and connection_status.get("status") in {
        "DISABLED",
        "BLOCKED_ACCOUNT_ID",
        "ERROR",
    }:
        return IBKRReconciliationStatus.BLOCK.value
    account_status = payload.get("account_id_assessment", {}).get("status")
    reconciliation_status = payload.get("reconciliation_status")
    statuses = {account_status, reconciliation_status}
    if IBKRReconciliationStatus.BLOCK.value in statuses:
        return IBKRReconciliationStatus.BLOCK.value
    if IBKRReconciliationStatus.LIMITED.value in statuses:
        return IBKRReconciliationStatus.LIMITED.value
    return IBKRReconciliationStatus.PASS.value


def _blocked_reconciliation(reason: str) -> IBKRReadOnlyReconciliationResult:
    return IBKRReadOnlyReconciliationResult(
        status=IBKRReconciliationStatus.BLOCK,
        cash_summary_present=False,
        compared_positions=0,
        issues=[
            IBKRReadOnlyReconciliationIssue(
                field="snapshot",
                severity=IBKRReconciliationStatus.BLOCK.value,
                message=reason,
            )
        ],
    )


def _recent_records(records: Any, *, limit: int = 10) -> list[Any]:
    if not isinstance(records, list):
        return [records] if records else []
    return records[-limit:]


def _json_block(value: Any) -> str:
    return "```json\n" + json.dumps(value, ensure_ascii=False, indent=2) + "\n```"


def _unmaskable_account_hint(payload: dict[str, Any]) -> str:
    masked = str(payload.get("account_id_masked") or "")
    if "***" in masked or masked == "missing":
        return ""
    return masked


def _assert_no_obvious_unmasked_account_id(text: str) -> None:
    # Guard against accidentally writing full IBKR account identifiers into artifacts.
    if any(fragment in text.upper() for fragment in ("DUP000000", "DU000000")):
        return
    import re

    if re.search(r"\b(?:DUP?|U)\d{5,}\b", text, flags=re.IGNORECASE):
        raise RuntimeError("IBKR snapshot output contains an unmasked account id")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run an IBKR Paper read-only account snapshot.")
    parser.add_argument(
        "--date",
        default=date.today().isoformat(),
        help="Snapshot date in YYYY-MM-DD format. Defaults to today.",
    )
    parser.add_argument(
        "--config-path",
        "--config",
        dest="config_path",
        default=str(DEFAULT_IBKR_PAPER_READONLY_CONFIG_PATH),
        help="Path to IBKR Paper read-only YAML config.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "outputs" / "reports"),
        help="Directory for snapshot JSON and Markdown outputs.",
    )
    parser.add_argument(
        "--sample-symbol",
        default="NVDA",
        help="Stock symbol used for the read-only contract details sample.",
    )
    args = parser.parse_args()
    payload = run_snapshot(
        as_of=date.fromisoformat(args.date),
        config_path=Path(args.config_path),
        output_dir=Path(args.output_dir),
        sample_symbol=args.sample_symbol,
    )
    print(f"Snapshot status：{payload['snapshot_status']}")
    print(f"Connection status：{payload['connection_status'].get('status', 'UNKNOWN')}")
    print(f"Reconciliation：{payload['reconciliation_status']}")
    print(f"JSON：{payload['output_paths']['json']}")
    print(f"Markdown：{payload['output_paths']['markdown']}")


if __name__ == "__main__":
    main()
