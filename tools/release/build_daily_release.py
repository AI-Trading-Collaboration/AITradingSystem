"""从 origin/main 可达提交构建无 git 的日报发布包（OPS-086）。"""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import io
import json
import re
import subprocess
import sys
import tarfile
import zipfile
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

import yaml

from ai_trading_system.release_identity import (
    RELEASE_FILE,
    SCHEMA_VERSION,
    ReleaseFile,
    files_digest,
)


def _git(repo: Path, *args: str) -> bytes:
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, check=True).stdout


def _glob_regex(pattern: str) -> re.Pattern[str]:
    parts = []
    index = 0
    while index < len(pattern):
        char = pattern[index]
        if pattern.startswith("**", index):
            index += 2
            if pattern.startswith("/", index):
                index += 1
                parts.append("(?:.*/)?")
            else:
                parts.append(".*")
        elif char == "*":
            parts.append("[^/]*")
            index += 1
        elif char == "?":
            parts.append("[^/]")
            index += 1
        else:
            parts.append(re.escape(char))
            index += 1
    return re.compile("^" + "".join(parts) + "$")


def _registry_match(path: str, pattern: str) -> bool:
    names, patterns = path.split("/"), pattern.split("/")
    return len(names) == len(patterns) and all(
        fnmatch.fnmatchcase(name, part) and (not name.startswith(".") or part.startswith("."))
        for name, part in zip(names, patterns, strict=True)
    )


def release_file_list(repo: Path, commit: str) -> list[str]:
    """只读提交里的规则、登记表与跟踪清单，不要求 origin/main。"""
    rules = yaml.safe_load(_git(repo, "show", f"{commit}:config/release/daily_release.yaml"))
    if rules["schema_version"] != "daily_release_manifest.v1":
        raise ValueError("清单规则版本不支持")
    registry = yaml.safe_load(_git(repo, "show", f"{commit}:{rules['include_registry_globs']}"))
    includes = [_glob_regex(pattern) for pattern in rules["include"]]
    excludes = [_glob_regex(pattern) for pattern in rules["exclude"]]
    registry_globs = [
        pattern
        for entry in registry["reports"]
        if entry.get("scope") == "daily"
        for pattern in entry.get("artifact_globs", [])
    ]
    tracked = _git(repo, "ls-tree", "-r", "-z", "--name-only", commit).decode("utf-8").split("\0")
    return sorted(
        path
        for path in tracked
        if path
        and (
            any(pattern.fullmatch(path) for pattern in includes)
            or any(_registry_match(path, pattern) for pattern in registry_globs)
        )
        and not any(pattern.fullmatch(path) for pattern in excludes)
    )


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[2])
    args = parser.parse_args(argv)
    partial = None
    owns_partial = False
    owns_sidecar = False
    complete = False
    try:
        commit = (
            _git(args.repo, "rev-parse", "--verify", f"{args.commit}^{{commit}}").decode().strip()
        )
        _git(args.repo, "merge-base", "--is-ancestor", commit, "refs/remotes/origin/main")
        timestamp = int(_git(args.repo, "show", "-s", "--format=%ct", commit))
        release_id = datetime.fromtimestamp(timestamp, UTC).strftime("%Y%m%d") + "-" + commit[:9]
        bundle = args.out_dir / f"{release_id}.zip"
        sidecar = bundle.with_name(bundle.name + ".sha256")
        if bundle.exists() or sidecar.exists():
            raise ValueError(f"发布包已存在：{bundle}")
        selected = release_file_list(args.repo, commit)
        wanted = set(selected)
        contents = {}
        archive = _git(args.repo, "archive", "--format=tar", commit)
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as tar:
            for member in tar:
                if member.name in wanted:
                    if not member.isfile():
                        raise ValueError(f"清单项不是普通文件：{member.name}")
                    handle = tar.extractfile(member)
                    if handle is None:
                        raise ValueError(f"无法读取：{member.name}")
                    contents[member.name] = handle.read()
        if sorted(contents) != selected:
            raise ValueError("git archive 缺少清单文件")
        files = [
            ReleaseFile(path, hashlib.sha256(contents[path]).hexdigest(), len(contents[path]))
            for path in selected
        ]
        rules = _git(args.repo, "show", f"{commit}:config/release/daily_release.yaml")
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "release_id": release_id,
            "source_commit": commit,
            "source_tree": _git(args.repo, "rev-parse", f"{commit}^{{tree}}").decode().strip(),
            "origin_main_commit": _git(args.repo, "rev-parse", "refs/remotes/origin/main")
            .decode()
            .strip(),
            "built_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "manifest_rules_sha256": hashlib.sha256(rules).hexdigest(),
            "dependencies_sha256": hashlib.sha256(contents["pyproject.toml"]).hexdigest(),
            "files": [asdict(item) for item in files],
            "files_sha256": files_digest(files),
        }
        args.out_dir.mkdir(parents=True, exist_ok=True)
        partial = bundle.with_name(bundle.name + ".partial")
        with partial.open("xb") as output:
            owns_partial = True
            with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as zip_out:
                for path in selected:
                    zip_out.writestr(zipfile.ZipInfo(path, (1980, 1, 1, 0, 0, 0)), contents[path])
                zip_out.writestr(
                    zipfile.ZipInfo(RELEASE_FILE, (1980, 1, 1, 0, 0, 0)),
                    json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                )
        digest = hashlib.sha256(partial.read_bytes()).hexdigest()
        # Exclusive creation preserves an existing sidecar, just as for the zip.
        with sidecar.open("x", encoding="utf-8", newline="\n") as handle:
            owns_sidecar = True
            handle.write(f"{digest}  {bundle.name}\n")
        partial.rename(bundle)
        complete = True
        print(f"发布包={bundle}\nrelease_id={release_id}")
        return 0
    except (
        OSError,
        ValueError,
        KeyError,
        TypeError,
        yaml.YAMLError,
        subprocess.CalledProcessError,
        tarfile.TarError,
        zipfile.BadZipFile,
    ) as exc:
        print(f"error={exc}", file=sys.stderr)
        return 2
    finally:
        if owns_partial and partial.exists():
            partial.unlink()
        if owns_sidecar and not complete:
            sidecar.unlink(missing_ok=True)


if __name__ == "__main__":
    sys.exit(main())
