"""Chinese status summary for one scheduled daily run (OPS-082).

``scripts/ops/daily_scheduler_run.ps1`` calls this once, after its single ``aits ops daily-run``
call. The summary only reads files: the captured daily-run output, the run-control state and
ledger, the run metadata, the daily input capture manifest, and the data quality discovery pointer
and receipt. The blocker classification is plain code; no LLM takes part
(``owner_decision:OPS-082:2026-10-11:deterministic_scheduler_v1`` (c)). The notification is the
two files it writes: ``<stem>.summary.md`` (Chinese) and ``<stem>.summary.json``.

The classification is a first triage for the operator, not a verdict: every reason names the
evidence it came from so it can be checked. production_effect=none; broker_action=none.

Standard library only, so a summary is still written when the product package cannot be imported.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "ops_daily_scheduler_summary.v1"

NONE = "NONE"
DATA_SOURCE_UNAVAILABLE = "DATA_SOURCE_UNAVAILABLE"
DATA_QUALITY_FAILED = "DATA_QUALITY_FAILED"
CODE_DEFECT = "CODE_DEFECT"
CONTROL_PLANE = "CONTROL_PLANE"
WAITING_FOR_NEW_AS_OF = "WAITING_FOR_NEW_AS_OF"

CATEGORY_LABELS = {
    NONE: "无阻断",
    DATA_SOURCE_UNAVAILABLE: "数据源不可用",
    DATA_QUALITY_FAILED: "数据质量未通过",
    CODE_DEFECT: "代码缺陷",
    CONTROL_PLANE: "控制面问题",
    WAITING_FOR_NEW_AS_OF: "等待新 as_of",
}

WINDOW_LABELS = {
    "PRIMARY": "09:30 主窗口（Asia/Tokyo）",
    "RESCUE": "17:30 补救窗口（Asia/Tokyo，同一计划任务的第二个触发时间）",
}

# Capture blocker codes from ``daily_input_capture._classify_source_blocker``. Provider-side codes,
# including authentication and quota errors after a paid subscription ends, mean the source is
# unavailable; the remaining codes are local source-control or filesystem problems.
DATA_SOURCE_BLOCKERS = frozenset(
    {
        "CREDENTIAL_MISSING",
        "PROVIDER_PERMISSION_DENIED",
        "PROVIDER_QUOTA_EXHAUSTED",
        "PROVIDER_SCHEMA_INVALID",
        "PROVIDER_UNAVAILABLE",
        "REQUEST_FAILED",
    }
)
CONTROL_PLANE_CAPTURE_BLOCKERS = frozenset(
    {
        "FILESYSTEM_INTEGRITY_FAILURE",
        "SOURCE_ATTEMPT_BUDGET_EXHAUSTED",
        "SOURCE_LEASE_CONFLICT",
        "SOURCE_STATE_INVALID",
    }
)
PASSING_RUN_STATUSES = frozenset({"PASS", "PASS_WITH_SKIPS", "PASS_WITH_LIMITATIONS"})
# Steps that always run at the end; their failure is usually a consequence of an upstream blocker.
ALWAYS_RUN_STEPS = frozenset({"pipeline_health", "secret_hygiene"})
# Formatting bound for quoted error text in the summary; not a policy threshold.
_EXCERPT_CHARS = 240
_SECRET_NAME_FRAGMENTS = ("KEY", "TOKEN", "SECRET", "PASSWORD", "AUTH", "CREDENTIAL")
_SECRET_QUERY = re.compile(
    r"(?i)\b(apikey|api_key|access_key|token|key|password|secret)=([^&\s\"']+)"
)
_TRACEBACK = "Traceback (most recent call last)"


@dataclass
class Finding:
    category: str
    reason: str

    def to_dict(self) -> dict[str, str]:
        return {
            "category": self.category,
            "category_label": CATEGORY_LABELS[self.category],
            "reason": self.reason,
        }


@dataclass
class Evidence:
    runtime_root: Path
    window: str
    exit_code: int | None
    precheck_blocker: str | None
    log_text: str | None
    run_id: str | None = None
    as_of: str | None = None
    cli_status: str | None = None
    state: Mapping[str, Any] | None = None
    state_path: Path | None = None
    state_is_current_run: bool = False
    ledger: Mapping[str, Any] | None = None
    ledger_path: Path | None = None
    metadata: Mapping[str, Any] | None = None
    metadata_path: Path | None = None
    capture: Mapping[str, Any] | None = None
    capture_path: Path | None = None
    dq_pointer: Mapping[str, Any] | None = None
    dq_pointer_path: Path | None = None
    dq_receipt: Mapping[str, Any] | None = None
    read_errors: list[str] = field(default_factory=list)


def parse_cli_output(text: str) -> dict[str, Any]:
    """Pull the run id and the outcome lines that ``aits ops daily-run`` prints."""

    def first(pattern: str) -> str | None:
        match = re.search(pattern, text)
        return match.group(1) if match else None

    run_control = first(r"Canonical run control：(\S+)")
    executed = first(r"每日运行执行：(\S+)")
    finalization_failed = first(r"Canonical finalization：FAILED \((\S+)\)")
    if finalization_failed is None and "Canonical finalization：FAIL_FINALIZATION" in text:
        finalization_failed = "FAIL_FINALIZATION"
    return {
        "run_id": first(r"Run ID：(\S+)"),
        "run_control_status": run_control,
        "executed_status": executed,
        "finalization_failure": finalization_failed,
        "traceback": _TRACEBACK in text,
    }


def as_of_from_run_id(run_id: str | None) -> str | None:
    if not run_id:
        return None
    match = re.fullmatch(r"daily_ops_run:(\d{4}-\d{2}-\d{2}):\S+", run_id)
    return match.group(1) if match else None


def safe_run_id(run_id: str) -> str:
    """Same rule as ``ai_trading_system.run_artifacts.safe_run_id`` (run bundle directory)."""

    cleaned = re.sub(r"[^A-Za-z0-9._=-]+", "_", run_id.strip()).strip("._-")
    return cleaned or "run"


def redact(text: str, env: Mapping[str, str]) -> str:
    redacted = _SECRET_QUERY.sub(lambda match: f"{match.group(1)}=<redacted>", text)
    for name, value in env.items():
        if len(value) >= 6 and any(fragment in name.upper() for fragment in _SECRET_NAME_FRAGMENTS):
            redacted = redacted.replace(value, f"<redacted:{name}>")
    return redacted


def excerpt(text: object, env: Mapping[str, str]) -> str:
    flat = " ".join(str(text or "").split())
    flat = redact(flat, env)
    return flat if len(flat) <= _EXCERPT_CHARS else flat[: _EXCERPT_CHARS - 1] + "…"


def collect_evidence(
    *,
    runtime_root: Path,
    window: str,
    exit_code: int | None,
    precheck_blocker: str | None,
    log_path: Path | None,
) -> Evidence:
    log_text: str | None = None
    read_errors: list[str] = []
    if log_path is not None and log_path.is_file():
        log_text = log_path.read_text(encoding="utf-8", errors="replace")
    elif precheck_blocker is None:
        read_errors.append(f"daily-run 输出日志不存在：{log_path}")
    evidence = Evidence(
        runtime_root=runtime_root,
        window=window,
        exit_code=exit_code,
        precheck_blocker=precheck_blocker,
        log_text=log_text,
        read_errors=read_errors,
    )
    if log_text is None:
        return evidence
    parsed = parse_cli_output(log_text)
    evidence.run_id = parsed["run_id"]
    evidence.as_of = as_of_from_run_id(evidence.run_id)
    evidence.cli_status = parsed["run_control_status"] or parsed["executed_status"]
    if evidence.as_of is None:
        return evidence

    states_dir = runtime_root / "outputs" / "run_control" / "daily" / "states"
    candidates: list[tuple[Path, Mapping[str, Any]]] = []
    for path in sorted(states_dir.glob("*.json")):
        if path.name.endswith(".run_ledger.json"):
            continue
        payload = _load_json(path, evidence)
        if isinstance(payload, Mapping) and payload.get("as_of") == evidence.as_of:
            candidates.append((path, payload))
    current = [item for item in candidates if item[1].get("run_id") == evidence.run_id]
    chosen = current or sorted(candidates, key=lambda item: str(item[1].get("updated_at", "")))
    if chosen:
        evidence.state_path, evidence.state = chosen[-1]
        evidence.state_is_current_run = bool(current)
        key = str(evidence.state.get("idempotency_key", ""))
        ledger_path = states_dir / f"{key}.run_ledger.json"
        if key and ledger_path.is_file():
            evidence.ledger_path = ledger_path
            ledger = _load_json(ledger_path, evidence)
            evidence.ledger = ledger if isinstance(ledger, Mapping) else None

    metadata_run_id = (
        str(evidence.state.get("run_id"))
        if evidence.state is not None and not evidence.state_is_current_run
        else evidence.run_id
    )
    if metadata_run_id:
        pattern = (
            f"*/as_of_{evidence.as_of}__{safe_run_id(metadata_run_id)}/metadata/"
            f"daily_ops_run_metadata_{evidence.as_of}.json"
        )
        matches = sorted((runtime_root / "outputs" / "runs" / "daily").glob(pattern))
        if matches:
            evidence.metadata_path = matches[-1]
            metadata = _load_json(matches[-1], evidence)
            evidence.metadata = metadata if isinstance(metadata, Mapping) else None

    capture_path = (
        runtime_root
        / "outputs"
        / "daily_input_capture"
        / evidence.as_of
        / f"daily_input_capture_manifest_{evidence.as_of}.json"
    )
    if capture_path.is_file():
        evidence.capture_path = capture_path
        capture = _load_json(capture_path, evidence)
        evidence.capture = capture if isinstance(capture, Mapping) else None

    pointer_path = (
        runtime_root
        / "outputs"
        / "data_quality"
        / "executions"
        / "discovery"
        / "daily_default"
        / evidence.as_of
        / "current.json"
    )
    if pointer_path.is_file():
        evidence.dq_pointer_path = pointer_path
        pointer = _load_json(pointer_path, evidence)
        if isinstance(pointer, Mapping):
            evidence.dq_pointer = pointer
            receipt_path = runtime_root / str(pointer.get("receipt_path", ""))
            if pointer.get("receipt_path") and receipt_path.is_file():
                receipt = _load_json(receipt_path, evidence)
                evidence.dq_receipt = receipt if isinstance(receipt, Mapping) else None
    return evidence


def step_results(evidence: Evidence) -> list[dict[str, str]]:
    """Per-step status: run metadata when available, otherwise the run-control ledger."""

    if evidence.metadata is not None:
        return [
            {
                "step_id": str(item.get("step_id", "")),
                "status": str(item.get("status", "")),
                "detail": str(item.get("error") or item.get("skip_reason") or ""),
                "diagnostic_path": str(item.get("diagnostic_path") or ""),
            }
            for item in evidence.metadata.get("step_results", [])
            if isinstance(item, Mapping)
        ]
    if evidence.ledger is not None:
        return [
            {
                "step_id": str(item.get("step_id", "")),
                "status": str(item.get("status", "")),
                "detail": ",".join(str(code) for code in item.get("blocker_codes", [])),
                "diagnostic_path": "",
            }
            for item in evidence.ledger.get("entries", [])
            if isinstance(item, Mapping)
        ]
    return []


def capture_components(evidence: Evidence) -> list[Mapping[str, Any]]:
    if evidence.capture is None:
        return []
    return [
        item for item in evidence.capture.get("component_results", []) if isinstance(item, Mapping)
    ]


def classify_capture_component(component: Mapping[str, Any]) -> str | None:
    if component.get("status") == "PASS":
        return None
    blocker = str(component.get("blocker_code", ""))
    if blocker in CONTROL_PLANE_CAPTURE_BLOCKERS:
        return CONTROL_PLANE
    if _TRACEBACK in str(component.get("error_summary") or ""):
        return CODE_DEFECT
    if blocker in DATA_SOURCE_BLOCKERS:
        return DATA_SOURCE_UNAVAILABLE
    return CODE_DEFECT


def classify_executed(evidence: Evidence) -> list[Finding]:
    """Findings for a run whose steps actually executed (the current run or the terminal parent)."""

    findings: list[Finding] = []
    metadata_status = (
        str(evidence.metadata.get("status")) if evidence.metadata is not None else None
    )
    for issue in (evidence.metadata or {}).get("input_visibility_issues", []) or []:
        code = str(issue.get("code", "")) if isinstance(issue, Mapping) else str(issue)
        if code == "daily_run_as_of_in_future":
            findings.append(
                Finding(WAITING_FOR_NEW_AS_OF, f"输入可见性：{code}（as_of 尚未 provider-ready）")
            )
        else:
            findings.append(Finding(CONTROL_PLANE, f"输入可见性：{code}"))
    for component in capture_components(evidence):
        category = classify_capture_component(component)
        if category is not None:
            findings.append(
                Finding(
                    category,
                    f"capture 组件 {component.get('component_id')} {component.get('status')}，"
                    f"blocker_code={component.get('blocker_code')}",
                )
            )
    steps = step_results(evidence)
    upstream_blocked = bool(findings) or any(step["status"] == "BLOCKED" for step in steps)
    for step in steps:
        detail = step["detail"]
        if "MISSING_ENV:" in detail:
            findings.append(Finding(CONTROL_PLANE, f"步骤 {step['step_id']} 缺少环境变量"))
            continue
        if step["status"] != "FAIL":
            continue
        if step["step_id"] == "validate_data":
            findings.append(
                Finding(DATA_QUALITY_FAILED, "validate_data FAIL（数据质量门禁未通过）")
            )
        elif step["step_id"] in ALWAYS_RUN_STEPS and upstream_blocked:
            findings.append(
                Finding(
                    CODE_DEFECT if not findings else findings[0].category,
                    f"{step['step_id']} FAIL（always-run 收尾检查，上游已阻断，通常是其结果）",
                )
            )
        elif _TRACEBACK in detail:
            findings.append(Finding(CODE_DEFECT, f"步骤 {step['step_id']} 抛出未处理异常"))
        else:
            findings.append(
                Finding(CODE_DEFECT, f"步骤 {step['step_id']} FAIL（需按诊断文件复核）")
            )
    if metadata_status == "FAIL_FINALIZATION":
        findings.append(Finding(CODE_DEFECT, "canonical finalization 失败（FAIL_FINALIZATION）"))
    return findings


def classify(evidence: Evidence) -> tuple[str, list[Finding]]:
    """Return the primary category and every finding, most important first."""

    if evidence.precheck_blocker:
        return CONTROL_PLANE, [
            Finding(
                CONTROL_PLANE,
                f"包装脚本前置检查失败，未调用 daily-run：{evidence.precheck_blocker}",
            )
        ]
    if evidence.log_text is None or evidence.exit_code is None:
        return CONTROL_PLANE, [Finding(CONTROL_PLANE, "缺少 daily-run 输出或退出码")]
    parsed = parse_cli_output(evidence.log_text)
    run_control = parsed["run_control_status"]
    if run_control == "RUN_CONTROL_ALREADY_COMPLETE":
        return NONE, [Finding(NONE, "同一 workflow/规格/as_of 已 PASS，本次未重复执行")]
    if run_control == "RUN_CONTROL_BLOCKED_TERMINAL_RECOVERY_REQUIRED":
        prior = evidence.state or {}
        findings = [
            Finding(
                WAITING_FOR_NEW_AS_OF,
                f"同一 as_of 已有终态 {prior.get('status', 'UNKNOWN')}"
                f"（run_id={prior.get('run_id', 'UNKNOWN')}），本次未执行任何步骤；"
                "没有合法恢复边界时等待下一个 provider-ready 交易日的普通运行",
            )
        ]
        findings.extend(
            Finding(item.category, f"原终态：{item.reason}") for item in classify_executed(evidence)
        )
        return WAITING_FOR_NEW_AS_OF, findings
    if run_control is not None:
        return CONTROL_PLANE, [Finding(CONTROL_PLANE, f"运行控制未放行：{run_control}")]
    if evidence.run_id is None:
        if parsed["traceback"]:
            return CODE_DEFECT, [Finding(CODE_DEFECT, "daily-run 在输出 Run ID 前抛出未处理异常")]
        return CONTROL_PLANE, [Finding(CONTROL_PLANE, "无法从 daily-run 输出解析 Run ID")]

    findings = classify_executed(evidence)
    if parsed["finalization_failure"] and not any(
        "finalization" in item.reason for item in findings
    ):
        findings.append(
            Finding(CODE_DEFECT, f"canonical finalization 失败：{parsed['finalization_failure']}")
        )
    if parsed["traceback"] and not findings:
        findings.append(Finding(CODE_DEFECT, "daily-run 输出含未处理异常"))
    status = parsed["executed_status"] or (evidence.metadata or {}).get("status")
    if not findings:
        if status in PASSING_RUN_STATUSES and evidence.exit_code == 0:
            return NONE, [Finding(NONE, f"daily-run {status}")]
        return CONTROL_PLANE, [
            Finding(
                CONTROL_PLANE,
                f"无法从证据确定阻断原因（status={status}，exit_code={evidence.exit_code}）",
            )
        ]
    order = (
        CONTROL_PLANE,
        DATA_SOURCE_UNAVAILABLE,
        WAITING_FOR_NEW_AS_OF,
        CODE_DEFECT,
        DATA_QUALITY_FAILED,
    )
    primary = min((item.category for item in findings), key=order.index)
    return primary, findings


def data_quality_summary(evidence: Evidence) -> dict[str, Any]:
    step = next(
        (item for item in step_results(evidence) if item["step_id"] == "validate_data"), None
    )
    summary: dict[str, Any] = {
        "validate_data_status": step["status"] if step else "NOT_RECORDED",
        "validate_data_detail": step["detail"] if step else "",
        "discovery_pointer": str(evidence.dq_pointer_path) if evidence.dq_pointer_path else None,
        "receipt_id": None,
        "receipt_status": None,
        "receipt_passed": None,
        "requested_window": None,
        "evaluated_window": None,
    }
    if evidence.dq_pointer is not None:
        summary["receipt_id"] = evidence.dq_pointer.get("receipt_id")
        summary["published_at"] = evidence.dq_pointer.get("published_at")
    if evidence.dq_receipt is not None:
        quality = evidence.dq_receipt.get("data_quality_evidence") or {}
        summary["receipt_status"] = quality.get("status")
        summary["receipt_passed"] = quality.get("passed")
        summary["requested_window"] = evidence.dq_receipt.get("requested_window")
        summary["evaluated_window"] = evidence.dq_receipt.get("evaluated_window")
    return summary


def build_summary(
    evidence: Evidence,
    *,
    started_at: str | None,
    finished_at: str | None,
    runtime_head: str | None,
    log_path: Path | None,
    env: Mapping[str, str],
) -> dict[str, Any]:
    primary, findings = classify(evidence)
    state = evidence.state or {}
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_by": "scripts/ops/daily_scheduler_summary.py",
        "llm_used": False,
        "window": evidence.window,
        "started_at": started_at,
        "finished_at": finished_at,
        "runtime_root": str(evidence.runtime_root),
        "runtime_head": runtime_head,
        "daily_run_exit_code": evidence.exit_code,
        "precheck_blocker": evidence.precheck_blocker,
        "run_id": evidence.run_id,
        "as_of": evidence.as_of,
        "cli_status": evidence.cli_status,
        "run_state": {
            "path": str(evidence.state_path) if evidence.state_path else None,
            "belongs_to_this_invocation": evidence.state_is_current_run,
            "run_id": state.get("run_id"),
            "status": state.get("status"),
            "attempt": state.get("attempt"),
            "blocker_codes": list(state.get("blocker_codes", [])),
        },
        "run_ledger_path": str(evidence.ledger_path) if evidence.ledger_path else None,
        "run_metadata_path": str(evidence.metadata_path) if evidence.metadata_path else None,
        "blocker_category": primary,
        "blocker_category_label": CATEGORY_LABELS[primary],
        "findings": [item.to_dict() for item in findings],
        "capture_manifest_path": str(evidence.capture_path) if evidence.capture_path else None,
        "capture_status": (evidence.capture or {}).get("status"),
        "capture_components": [
            {
                "component_id": component.get("component_id"),
                "status": component.get("status"),
                "blocker_code": component.get("blocker_code"),
                "attempt_count": component.get("attempt_count"),
                "category": classify_capture_component(component),
                "error_excerpt": excerpt(component.get("error_summary"), env),
            }
            for component in capture_components(evidence)
        ],
        "data_quality": data_quality_summary(evidence),
        "steps": [
            {**step, "detail": excerpt(step["detail"], env)} for step in step_results(evidence)
        ],
        "log_path": str(log_path) if log_path else None,
        "read_errors": evidence.read_errors,
        "production_effect": "none",
        "broker_action": "none",
    }


def render_markdown(summary: Mapping[str, Any]) -> str:
    state = summary["run_state"]
    dq = summary["data_quality"]
    lines = [
        f"# 每日调度运行摘要：{summary.get('as_of') or 'as_of 未知'}（{summary['window']}）",
        "",
        "由 `scripts/ops/daily_scheduler_summary.py` 按文件证据用代码生成，没有 LLM 参与。",
        "分类是给运维的初步判断，每条依据都写明出处，需要时按证据路径复核。",
        "",
        f"- 窗口：{WINDOW_LABELS.get(summary['window'], summary['window'])}",
        f"- 开始 / 结束：{summary.get('started_at')} / {summary.get('finished_at')}",
        f"- 运行副本：`{summary['runtime_root']}`；HEAD `{summary.get('runtime_head')}`",
        f"- daily-run 退出码：{summary.get('daily_run_exit_code')}",
        f"- Run ID：`{summary.get('run_id')}`；as_of：{summary.get('as_of')}",
        f"- CLI 结果：`{summary.get('cli_status')}`",
        (
            f"- 运行状态（run state）：`{state.get('status')}`，attempt {state.get('attempt')}，"
            f"run_id `{state.get('run_id')}`"
            + (
                ""
                if state.get("belongs_to_this_invocation")
                else "（同一 as_of 的已有终态，不是本次执行）"
            )
        ),
        f"- **阻断分类：{summary['blocker_category_label']}**（`{summary['blocker_category']}`）",
        (
            f"- 数据质量：validate_data `{dq['validate_data_status']}`"
            + (f"；{dq['validate_data_detail']}" if dq.get("validate_data_detail") else "")
        ),
    ]
    if dq.get("receipt_id"):
        lines.append(
            f"- DQ 回执：`{dq['receipt_id']}`，status `{dq.get('receipt_status')}`，"
            f"passed={dq.get('receipt_passed')}（发现指针只用于发现，不是质量证明）"
        )
    lines += [
        "- production_effect=none；broker_action=none",
        "",
        "## 分类依据",
        "",
    ]
    lines += [f"- {item['category_label']}：{item['reason']}" for item in summary["findings"]]
    if summary["capture_components"]:
        lines += [
            "",
            f"## 输入采集组件（capture 状态 `{summary.get('capture_status')}`）",
            "",
            "| 组件 | 状态 | blocker_code | 尝试次数 | 分类 | 错误摘要（脱敏、截断） |",
            "|---|---|---|---|---|---|",
        ]
        for component in summary["capture_components"]:
            category = component.get("category")
            lines.append(
                f"| {component.get('component_id')} | {component.get('status')} | "
                f"{component.get('blocker_code')} | {component.get('attempt_count')} | "
                f"{CATEGORY_LABELS.get(category, '-') if category else '-'} | "
                f"{_cell(component.get('error_excerpt'))} |"
            )
    if summary["steps"]:
        lines += ["", "## 每步状态", "", "| 步骤 | 状态 | 说明（截断） |", "|---|---|---|"]
        for step in summary["steps"]:
            lines.append(f"| {step['step_id']} | {step['status']} | {_cell(step['detail'])} |")
    lines += ["", "## 证据路径", ""]
    for label, key in (
        ("daily-run 输出日志", "log_path"),
        ("run state", None),
        ("run ledger", "run_ledger_path"),
        ("run metadata", "run_metadata_path"),
        ("capture manifest", "capture_manifest_path"),
    ):
        value = state.get("path") if key is None else summary.get(key)
        lines.append(f"- {label}：`{value}`" if value else f"- {label}：无")
    if dq.get("discovery_pointer"):
        lines.append(f"- DQ 发现指针：`{dq['discovery_pointer']}`")
    if summary["read_errors"]:
        lines += ["", "## 证据读取问题", ""]
        lines += [f"- {item}" for item in summary["read_errors"]]
    return "\n".join(lines) + "\n"


def write_summary(summary: Mapping[str, Any], output_stem: Path) -> tuple[Path, Path]:
    output_stem.parent.mkdir(parents=True, exist_ok=True)
    json_path = output_stem.with_name(output_stem.name + ".summary.json")
    md_path = output_stem.with_name(output_stem.name + ".summary.md")
    json_path.write_bytes(
        (json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")
    )
    md_path.write_bytes(render_markdown(summary).encode("utf-8"))
    return md_path, json_path


def _load_json(path: Path, evidence: Evidence) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        evidence.read_errors.append(f"{path}: {type(exc).__name__}: {exc}")
        return None


def _cell(value: object) -> str:
    return str(value or "").replace("|", "\\|").replace("\n", " ")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--runtime-root", required=True)
    parser.add_argument("--window", required=True, choices=sorted(WINDOW_LABELS))
    parser.add_argument("--output-stem", required=True)
    parser.add_argument("--log")
    parser.add_argument("--exit-code", type=int)
    parser.add_argument("--precheck-blocker")
    parser.add_argument("--started-at")
    parser.add_argument("--finished-at")
    parser.add_argument("--runtime-head")
    args = parser.parse_args(argv)
    log_path = Path(args.log) if args.log else None
    evidence = collect_evidence(
        runtime_root=Path(args.runtime_root),
        window=args.window,
        exit_code=args.exit_code,
        precheck_blocker=args.precheck_blocker,
        log_path=log_path,
    )
    summary = build_summary(
        evidence,
        started_at=args.started_at,
        finished_at=args.finished_at,
        runtime_head=args.runtime_head,
        log_path=log_path,
        env=os.environ,
    )
    md_path, json_path = write_summary(summary, Path(args.output_stem))
    print(f"summary_md={md_path}")
    print(f"summary_json={json_path}")
    print(f"blocker_category={summary['blocker_category']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
