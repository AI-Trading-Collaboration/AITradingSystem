from __future__ import annotations

import argparse
import json
import stat
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = str(PROJECT_ROOT / "src")
if SOURCE_ROOT in sys.path:
    sys.path.remove(SOURCE_ROOT)
sys.path.insert(0, SOURCE_ROOT)

from ai_trading_system.platform.architecture.source_preservation import (  # noqa: E402
    DEFAULT_POLICY_PATH,
    SourcePreservation,
    SourcePreservationError,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="DEVX-014 仅源码保全；不授予验证、发布或研究执行权限",
    )
    parser.add_argument("--repository", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY_PATH)
    commands = parser.add_subparsers(dest="command", required=True)
    preserve = commands.add_parser("preserve", help="严格校验显式请求并保全 raw source identity")
    preserve.add_argument("--request", required=True, type=Path)
    validate = commands.add_parser("validate", help="独立重验既有 source-only receipt")
    validate.add_argument("--receipt", required=True, type=Path)
    return parser.parse_args()


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"JSON 重复字段：{key}")
        result[key] = value
    return result


def _read_request(path: Path) -> object:
    # Requests are JSON under the trusted checkout's explicit engineering runtime.
    # Resolve/containment and reparse checks precede reading any input bytes.
    absolute = path.absolute()
    relative = absolute.relative_to(PROJECT_ROOT)
    if relative.parts[:2] != ("outputs", "validation_runtime") or path.suffix != ".json":
        raise ValueError("request 必须是 trusted checkout outputs/validation_runtime 下的 JSON")
    for item in (absolute, *absolute.parents):
        info = item.lstat()
        if stat.S_ISLNK(info.st_mode) or (
            getattr(info, "st_file_attributes", 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT
        ):
            raise ValueError("request 路径不允许 symlink/reparse")
    absolute.resolve(strict=True).relative_to(PROJECT_ROOT / "outputs/validation_runtime")
    if not absolute.is_file():
        raise ValueError("request 必须是普通文件")
    return json.loads(absolute.read_text(encoding="utf-8"), object_pairs_hook=_unique_object)


def main() -> int:
    args = parse_args()
    try:
        preservation = SourcePreservation(
            project_root=args.repository.resolve(), policy_path=args.policy.resolve()
        )
        if args.command == "preserve":
            request = _read_request(args.request)
            if not isinstance(request, dict):
                raise ValueError("request 必须是 JSON object")
            result = preservation.preserve(request)
        else:
            result = preservation.validate(args.receipt)
    except (SourcePreservationError, OSError, ValueError) as exc:
        result = {
            "schema_version": "source_preservation_command_result.v1",
            "status": "BLOCKED",
            "reason_code": (
                exc.code if isinstance(exc, SourcePreservationError) else "SOURCE_COMMAND_FAILED"
            ),
            "detail": str(exc),
            "production_effect": "none",
            "broker_action": "none",
        }
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
