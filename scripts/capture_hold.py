"""Operator entry for capture holds (GOV-008 protocol v3): acquire, release, status.

A prospective capture child (``scripts/run_named_data_quality.py --source-hold-id ...``) only
restores and rechecks an existing hold; this script is how an operator or agent creates and ends
one. Output is one JSON object per call so the hold id can be passed on verbatim.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from ai_trading_system.data.capture_hold import (
    HOLD_ROOT_RELATIVE,
    CaptureHoldError,
    acquire_capture_hold,
    parse_hold_record,
    release_capture_hold_by_id,
)


def _root(value: str | None) -> Path:
    if value:
        return Path(value).resolve(strict=True)
    top = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True, check=True
    ).stdout.strip()
    return Path(top).resolve(strict=True)


def _status(root: Path) -> dict[str, object]:
    store = root / HOLD_ROOT_RELATIVE
    rows = []
    for path in sorted((store / "records").glob("hold-*.json")):
        record = parse_hold_record(path.read_bytes())
        released = (store / "released" / path.name).is_file()
        rows.append({**record, "released": released})
    return {
        "schema_version": "capture_hold_status.v1",
        "execution_root": root.as_posix(),
        "holds": rows,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="采集 hold：获取、释放、查看（GOV-008 协议 v3）")
    parser.add_argument("--root", help="执行根目录；默认当前 git 仓库根")
    commands = parser.add_subparsers(dest="command", required=True)
    acquire = commands.add_parser(
        "acquire", help="获取 hold（要求 HEAD 等于候选 commit 且代码未修改）"
    )
    acquire.add_argument("--candidate-commit", required=True)
    acquire.add_argument("--path", action="append", required=True, dest="paths")
    acquire.add_argument("--actor", required=True)
    acquire.add_argument("--ttl-minutes", type=int, required=True)
    release = commands.add_parser("release", help="释放 hold（含崩溃进程遗留的 hold）")
    release.add_argument("--hold-id", required=True)
    commands.add_parser("status", help="列出 hold 记录与释放状态")
    args = parser.parse_args(argv)
    try:
        root = _root(args.root)
        if args.command == "acquire":
            hold = acquire_capture_hold(
                execution_root=root,
                candidate_commit=args.candidate_commit,
                required_paths=tuple(args.paths),
                actor=args.actor,
                ttl_seconds=args.ttl_minutes * 60,
            )
            payload: dict[str, object] = {"status": "ACQUIRED", **hold.record}
        elif args.command == "release":
            release_capture_hold_by_id(execution_root=root, hold_id=args.hold_id)
            payload = {"status": "RELEASED", "hold_id": args.hold_id}
        else:
            payload = _status(root)
    except (CaptureHoldError, OSError, subprocess.CalledProcessError) as exc:
        print(json.dumps({"status": "BLOCKED", "reason": str(exc)}, ensure_ascii=False))
        return 2
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
