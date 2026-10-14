"""校验、安装并激活日报发布包；不删除已有 release（OPS-086）。"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import stat
import subprocess
import sys
import tempfile
import zipfile
from datetime import UTC, datetime
from pathlib import Path, PureWindowsPath


class DeployRefused(Exception):
    def __init__(self, code: str, detail: str):
        super().__init__(detail)
        self.code = code
        self.detail = detail


def _run(argv: list[str], *, env=None, cwd=None):
    return subprocess.run(argv, env=env, cwd=cwd, capture_output=True, text=True, check=False)


def _now():
    return datetime.now(UTC)


def _stamp(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def venv_python(release_dir: Path) -> Path:
    return Path(release_dir) / ".venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def venv_aits(release_dir: Path) -> Path:
    return Path(release_dir) / ".venv" / ("Scripts/aits.exe" if os.name == "nt" else "bin/aits")


def _release_id(value) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9]{8}-[0-9a-f]{9}", value) is not None


def _manifest(payload):
    """标准库独立校验器：部署前不能导入尚未安装/尚未核对的包代码。"""
    if not isinstance(payload, dict) or payload.get("schema_version") != "aits_release.v1":
        raise ValueError("RELEASE.json 版本或对象无效")
    for key in ("source_commit", "source_tree", "origin_main_commit"):
        if not isinstance(payload.get(key), str) or not re.fullmatch(r"[0-9a-f]{40}", payload[key]):
            raise ValueError(f"{key} 无效")
    if (
        not _release_id(payload.get("release_id"))
        or payload["release_id"][9:] != payload["source_commit"][:9]
    ):
        raise ValueError("release_id 无效")
    if not isinstance(payload.get("built_at"), str):
        raise ValueError("built_at 无效")
    files = payload.get("files")
    if not isinstance(files, list) or not files:
        raise ValueError("files 为空或无效")
    names = []
    digest = hashlib.sha256()
    for item in files:
        if not isinstance(item, dict):
            raise ValueError("files 项不是对象")
        path = item.get("path")
        if (
            not isinstance(path, str)
            or not path
            or any(c in path for c in ("\\", ":", "\0"))
            or any(part in ("", ".", "..") for part in path.split("/"))
            or path == "RELEASE.json"
            or path.split("/")[0] in ("data", "outputs", "artifacts", ".git", ".venv")
        ):
            raise ValueError(f"不安全的清单路径：{path!r}")
        if not isinstance(item.get("sha256"), str) or not re.fullmatch(
            r"[0-9a-f]{64}", item["sha256"]
        ):
            raise ValueError("文件 sha256 无效")
        if type(item.get("size")) is not int or item["size"] < 0:
            raise ValueError("文件 size 无效")
        names.append(path)
        digest.update(f"{item['sha256']}  {path}\n".encode())
    if names != sorted(set(names)) or payload.get("files_sha256") != digest.hexdigest():
        raise ValueError("清单顺序、重复项或 files_sha256 无效")
    return payload


def _bundle_contents(bundle: Path):
    try:
        raw = bundle.read_bytes()
        expected = f"{hashlib.sha256(raw).hexdigest()}  {bundle.name}"
        if bundle.with_name(bundle.name + ".sha256").read_text("utf-8").strip() != expected:
            raise ValueError("zip sha256 不一致")
    except (OSError, ValueError) as exc:
        raise DeployRefused("BUNDLE_SHA256_MISMATCH", str(exc)) from exc
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            names = archive.namelist()
            manifest = _manifest(json.loads(archive.read("RELEASE.json")))
            if len(names) != len(set(names)) or set(names) != {
                "RELEASE.json",
                *(item["path"] for item in manifest["files"]),
            }:
                raise ValueError("zip 文件集合与清单不一致")
            contents = {}
            for info in archive.infolist():
                mode = info.external_attr >> 16
                if info.is_dir() or stat.S_ISLNK(mode):
                    raise ValueError("zip 只允许普通文件")
                contents[info.filename] = archive.read(info)
            for item in manifest["files"]:
                data = contents[item["path"]]
                if len(data) != item["size"] or hashlib.sha256(data).hexdigest() != item["sha256"]:
                    raise ValueError(f"文件校验失败：{item['path']}")
            for required in (
                "pyproject.toml",
                "scripts/ops/run_daily.ps1",
                "scripts/ops/daily_scheduler_run.ps1",
            ):
                if required not in contents:
                    raise ValueError(f"发布包缺少 {required}")
            return manifest, contents
    except (OSError, ValueError, KeyError, TypeError, RuntimeError, zipfile.BadZipFile) as exc:
        raise DeployRefused("BUNDLE_INVALID", str(exc)) from exc


def _read_current(daily: Path):
    path = daily / "current.json"
    if not path.exists():
        return None
    try:
        current = json.loads(path.read_text("utf-8"))
        if (
            not isinstance(current, dict)
            or current.get("schema_version") != "aits_daily_current.v1"
            or not _release_id(current.get("release_id"))
            or not isinstance(current.get("state_root"), str)
            or not _absolute(current["state_root"])
        ):
            raise ValueError("current.json 字段无效")
        return current
    except (OSError, ValueError) as exc:
        raise DeployRefused("CURRENT_INVALID", str(exc)) from exc


def _absolute(value: str) -> bool:
    return Path(value).is_absolute() or PureWindowsPath(value).is_absolute()


def _idle(state: str, task: str, run):
    result = run(["schtasks", "/query", "/tn", task, "/fo", "LIST"])
    if result.returncode == 0 and re.search(
        r"^\s*Status:\s*Running\s*$", result.stdout, re.MULTILINE | re.IGNORECASE
    ):
        raise DeployRefused("TASK_RUNNING", "计划任务正在运行")
    locks = Path(state) / "outputs/run_control/daily/locks"
    if locks.exists() and any(path.is_file() for path in locks.rglob("*")):
        raise DeployRefused("RUN_LOCK_PRESENT", "存在日报运行锁文件")


def _checked(run, argv, code, **kwargs):
    try:
        result = run([str(arg) for arg in argv], **kwargs)
    except OSError as exc:
        raise DeployRefused(code, str(exc)) from exc
    if result.returncode != 0:
        raise DeployRefused(code, f"命令失败（exit={result.returncode}）：{result.stderr}")
    return result


def _verify(release: Path, run):
    _checked(
        run,
        [
            venv_python(release),
            "-I",
            "-m",
            "ai_trading_system.release_identity",
            "verify",
            "--root",
            release,
        ],
        "VERIFY_FAILED",
    )


def _constraints(text: str) -> str:
    lines = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith(("#", "-e")):
            continue
        name = re.split(r"[\s=<>!~\[;@]", line, maxsplit=1)[0].lower().replace("_", "-")
        if name != "ai-trading-system":
            lines.append(line)
    return "".join(line + "\n" for line in lines)


def _activate(daily: Path, release: Path, manifest, current, state: str, now):
    payload = {
        "schema_version": "aits_daily_current.v1",
        "release_id": manifest["release_id"],
        "previous_release_id": current["release_id"] if current else None,
        "state_root": state,
        "source_commit": manifest["source_commit"],
        "activated_at": _stamp(now()),
    }
    current_path, wrapper = daily / "current.json", daily / "run_daily.ps1"
    originals = {
        path: path.read_bytes() if path.exists() else None for path in (current_path, wrapper)
    }
    replacements = {
        current_path: (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode(),
        wrapper: (release / "scripts/ops/run_daily.ps1").read_bytes(),
    }
    replaced = []
    try:
        for path, data in replacements.items():
            path.with_name(path.name + ".tmp").write_bytes(data)
        for path in replacements:
            os.replace(path.with_name(path.name + ".tmp"), path)
            replaced.append(path)
    except OSError:
        # A failed second replace must not leave the pointer changed after a refusal.
        for path in reversed(replaced):
            data = originals[path]
            if data is None:
                path.unlink()
            else:
                temporary = path.with_name(path.name + ".tmp")
                temporary.write_bytes(data)
                os.replace(temporary, path)
        raise
    finally:
        for path in replacements:
            path.with_name(path.name + ".tmp").unlink(missing_ok=True)


def _cleanup_candidates(daily: Path, current):
    releases = daily / "releases"
    ids = sorted(
        path.name for path in releases.iterdir() if path.is_dir() and _release_id(path.name)
    )
    # Three retained versions is the OPS-086 section 5.3 operational retention rule,
    # not an investment/promotion threshold; this only suggests, never deletes.
    keep = {*ids[-3:], current["release_id"], current["previous_release_id"]}
    for release_id in ids:
        if release_id not in keep:
            print(f"cleanup_candidate={release_id}")


def main(argv=None, *, run=_run, now=_now) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--bundle", type=Path)
    action.add_argument("--activate")
    parser.add_argument("--daily-root", type=Path, required=True)
    parser.add_argument("--state-root")
    parser.add_argument("--constraints-from")
    parser.add_argument("--unpinned", action="store_true")
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--task-name", default="AITradingSystem Daily Run")
    args = parser.parse_args(argv)
    daily = args.daily_root.resolve()
    release_id = args.activate
    outcome, code, detail = "REFUSED", None, ""
    try:
        current = _read_current(daily)
        if current is None and (args.state_root is None or args.activate):
            raise DeployRefused(
                "STATE_ROOT_REQUIRED", "首次部署必须指定 --state-root；切换须有 current.json"
            )
        state = args.state_root if args.state_root is not None else current["state_root"]
        if not _absolute(state):
            raise DeployRefused("STATE_ROOT_INVALID", "状态根必须是绝对路径")
        if current and args.state_root is not None and state != current["state_root"]:
            raise DeployRefused("STATE_ROOT_CHANGED", "状态根不能随 release 改变")
        _idle(state, args.task_name, run)
        if args.activate:
            if not _release_id(release_id):
                raise DeployRefused("RELEASE_MISSING", "release_id 格式无效")
            release = daily / "releases" / release_id
            if not (release / "RELEASE.json").is_file():
                raise DeployRefused("RELEASE_MISSING", "待激活 release 不存在")
            try:
                manifest = _manifest(json.loads((release / "RELEASE.json").read_text("utf-8")))
                if manifest["release_id"] != release_id:
                    raise ValueError("目录编号与清单不一致")
            except (OSError, ValueError) as exc:
                raise DeployRefused("VERIFY_FAILED", str(exc)) from exc
            _verify(release, run)
        else:
            previous_freeze = (
                daily / "releases" / current["release_id"] / ".venv_freeze.txt" if current else None
            )
            if (
                not args.constraints_from
                and not args.unpinned
                and not (previous_freeze and previous_freeze.is_file())
            ):
                raise DeployRefused("CONSTRAINTS_REQUIRED", "请指定约束来源或显式 --unpinned")
            manifest, contents = _bundle_contents(args.bundle)
            release_id = manifest["release_id"]
            release = daily / "releases" / release_id
            if release.exists():
                raise DeployRefused("RELEASE_EXISTS", "release 目录已存在")
            release.mkdir(parents=True)
            try:
                for path, data in contents.items():
                    target = release / path
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(data)
                constraints = release / ".venv_constraints.txt"
                if not args.unpinned:
                    text = (
                        _checked(
                            run, [args.constraints_from, "-m", "pip", "freeze"], "PIP_FREEZE_FAILED"
                        ).stdout
                        if args.constraints_from
                        else previous_freeze.read_text("utf-8")
                    )
                    constraints.write_text(_constraints(text), encoding="utf-8", newline="\n")
                _checked(run, [args.python, "-m", "venv", release / ".venv"], "VENV_FAILED")
                install = [venv_python(release), "-m", "pip", "install", "-e", release]
                if not args.unpinned:
                    install += ["-c", constraints]
                _checked(run, install, "PIP_INSTALL_FAILED")
                freeze = _checked(
                    run, [venv_python(release), "-m", "pip", "freeze"], "PIP_FREEZE_FAILED"
                )
                (release / ".venv_freeze.txt").write_text(
                    freeze.stdout, encoding="utf-8", newline="\n"
                )
                _verify(release, run)
                for path in contents:
                    target = release / path
                    target.chmod(
                        target.stat().st_mode & ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)
                    )
                with tempfile.TemporaryDirectory(prefix="OPS-086-daily-plan-") as temporary:
                    _checked(
                        run,
                        [
                            venv_aits(release),
                            "ops",
                            "daily-plan",
                            "--fail-on-missing-env",
                            "--output-path",
                            Path(temporary) / "daily_ops_plan.md",
                        ],
                        "DAILY_PLAN_FAILED",
                        env={
                            **os.environ,
                            "AITS_STATE_ROOT": state,
                            "PYTHONDONTWRITEBYTECODE": "1",
                        },
                        cwd=release,
                    )
            except (DeployRefused, OSError):
                failed = release.with_name(
                    release_id + ".failed-" + now().astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
                )
                release.rename(failed)
                raise
        _activate(daily, release, manifest, current, state, now)
        outcome, detail = "ACTIVATED", "发布包已校验并激活"
    except DeployRefused as exc:
        code, detail = exc.code, exc.detail
    except OSError as exc:
        code, detail = "DEPLOY_IO_FAILED", str(exc)
    daily.mkdir(parents=True, exist_ok=True)
    record = {
        "at": _stamp(now()),
        "action": "activate" if args.activate else "deploy",
        "outcome": outcome,
        "code": code,
        "release_id": release_id,
        "detail": detail,
    }
    with (daily / "deploy_log.jsonl").open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    print(f"outcome={outcome}")
    if outcome == "ACTIVATED":
        print(f"release_id={release_id}")
        _cleanup_candidates(daily, _read_current(daily))
        return 0
    print(f"code={code}\n拒绝原因={detail}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
