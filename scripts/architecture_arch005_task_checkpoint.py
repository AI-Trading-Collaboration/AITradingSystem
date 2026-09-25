"""Explicit task-source checkpoint entry point; no automatic environment repair."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = str(PROJECT_ROOT / "src")
if SOURCE_ROOT in sys.path:
    sys.path.remove(SOURCE_ROOT)
sys.path.insert(0, SOURCE_ROOT)

from ai_trading_system.platform.architecture import source_preservation as safe  # noqa: E402
from ai_trading_system.platform.architecture.task_checkpoint import (  # noqa: E402
    SAFETY,
    TaskCheckpoint,
    TaskCheckpointError,
    _json,
)


def _input(path: Path) -> Path:
    absolute = path.absolute()
    relative = absolute.relative_to(PROJECT_ROOT)
    if relative.parts[:2] != ("outputs", "validation_runtime") or absolute.suffix != ".json":
        raise ValueError("请求/计划必须位于本可信工作区 outputs/validation_runtime 下")
    safe._configuration_path(absolute)
    absolute.resolve(strict=False).relative_to(PROJECT_ROOT / "outputs/validation_runtime")
    return absolute


def main() -> int:
    parser = argparse.ArgumentParser(description="DEVX-015：保存任务源码，不授予集成/验证/发布资格")
    commands = parser.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("plan", help="只读核对显式scope，并写入不可覆盖的精确请求")
    plan.add_argument("--scope", required=True, type=Path)
    plan.add_argument("--output", required=True, type=Path)
    capture = commands.add_parser("capture", help="保存精确请求中的普通源码增改删")
    capture.add_argument("--request", required=True, type=Path)
    worker = commands.add_parser("capture-worker", help="仅供同租约受控Job内的固定worker入口")
    worker.add_argument("--execution-request", required=True, type=Path)
    recover = commands.add_parser(
        "recover-terminal", help="仅从完整终态证据重建独立回执，不重采或重派发"
    )
    recover.add_argument("--request", required=True, type=Path)
    recover.add_argument("--actor", required=True)
    interrupted = commands.add_parser(
        "recover-interrupted", help="确认原执行终态后保全失败证据并释放原租约，不重派发"
    )
    interrupted.add_argument("--request", required=True, type=Path)
    interrupted.add_argument("--actor", required=True)
    interrupted.add_argument(
        "--action", choices=("observe", "terminate_frozen_job"), default="observe"
    )
    validate = commands.add_parser("validate", help="独立验证既有source-only快照")
    validate.add_argument("--receipt", required=True, type=Path)
    args = parser.parse_args()
    try:
        engine = TaskCheckpoint(PROJECT_ROOT)
        if args.command == "plan":
            # Check the destination before scope execution; never replace a request.
            output = _input(args.output)
            if output.exists():
                raise ValueError("计划输出已存在，不覆盖或自动重试")
            request = engine.plan(_json(_input(args.scope)))
            safe._write_once(output, safe._json_bytes(request))
            result = {
                "schema_version": "task_checkpoint_plan_result.v1",
                "status": "PASS",
                "request_path": output.as_posix(),
                "request_sha256": safe._sha(safe._json_bytes(request)),
                "file_count": len(request["files"]),
                "safety": dict(SAFETY),
                "production_effect": "none",
                "broker_action": "none",
            }
        elif args.command == "capture":
            result = engine.capture(_json(_input(args.request)))
        elif args.command == "capture-worker":
            result = engine.capture_worker(args.execution_request)
        elif args.command == "recover-terminal":
            result = engine.recover_terminal(_json(_input(args.request)), actor=args.actor)
        elif args.command == "recover-interrupted":
            result = engine.recover_interrupted(
                _json(_input(args.request)), actor=args.actor, action=args.action
            )
        else:
            result = engine.validate(args.receipt)
    except (TaskCheckpointError, safe.SourcePreservationError, OSError, ValueError) as exc:
        result = {
            "schema_version": "task_checkpoint_command_result.v1",
            "status": "BLOCKED",
            "reason_code": getattr(exc, "code", "TASK_CHECKPOINT_COMMAND_INVALID"),
            "detail": getattr(exc, "message", "请求、路径或运行环境不满足固定合同"),
            "safety": dict(SAFETY),
            "production_effect": "none",
            "broker_action": "none",
        }
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
