"""DEVX-033: queue order, temporary-directory cleanup and timing summary of tests/conftest.py."""

from __future__ import annotations

import importlib.util
import json
import os
import stat
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

CONFTEST = Path(__file__).with_name("conftest.py")
SEED = Path(__file__).with_name("file_durations.json")


def load_conftest():
    spec = importlib.util.spec_from_file_location("gate_timing_conftest", CONFTEST)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def items(*nodeids: str) -> list[SimpleNamespace]:
    return [SimpleNamespace(nodeid=n) for n in nodeids]


def test_files_are_ordered_longest_first_and_keep_their_test_order() -> None:
    gate = load_conftest()
    collected = items(
        "tests/a.py::t1",
        "tests/a.py::t2",
        "tests/b.py::t1",
        "tests/c.py::t1",
        "tests/c.py::t2",
        "tests/c.py::t3",
        "tests/d.py::t1",
    )
    ordered = gate.order_by_duration(collected, {"tests/b.py": 9.0, "tests/a.py": 0.5})
    # b recorded 9 s; c unknown, 3 tests x 0.3 s; a recorded 0.5 s; d unknown, 0.3 s.
    assert [i.nodeid for i in ordered] == [
        "tests/b.py::t1",
        "tests/c.py::t1",
        "tests/c.py::t2",
        "tests/c.py::t3",
        "tests/a.py::t1",
        "tests/a.py::t2",
        "tests/d.py::t1",
    ]


def test_equal_durations_keep_collection_order_and_files_together() -> None:
    gate = load_conftest()
    collected = items("tests/x.py::a", "tests/y.py::a", "tests/x.py::b", "tests/z.py::a")
    ordered = gate.order_by_duration(collected, {"tests/x.py": 1.0, "tests/y.py": 1.0})
    assert [i.nodeid for i in ordered] == [
        "tests/x.py::a",
        "tests/x.py::b",
        "tests/y.py::a",
        "tests/z.py::a",
    ]


def test_recorded_durations_override_the_seed(tmp_path: Path) -> None:
    gate = load_conftest()
    assert gate.merge_durations({"a": 1.0, "b": 2.0}, {"b": 5, "c": 0.5, "bad": "x"}) == {
        "a": 1.0,
        "b": 5.0,
        "c": 0.5,
    }
    assert gate.merge_durations({"a": 1.0}, None) == {"a": 1.0}
    assert gate.load_seed(tmp_path / "missing.json") == {}
    (tmp_path / "broken.json").write_text("{", encoding="utf-8")
    assert gate.load_seed(tmp_path / "broken.json") == {}


def test_seed_file_names_existing_test_files() -> None:
    gate = load_conftest()
    seed = gate.load_seed(SEED)
    root = Path(__file__).resolve().parents[1]
    assert len(seed) > 300
    assert all(v >= 0 for v in seed.values())
    missing = [f for f in seed if not (root / f).is_file()]
    assert len(missing) <= len(seed) // 10


def test_remove_tree_deletes_read_only_files(tmp_path: Path) -> None:
    gate = load_conftest()
    target = tmp_path / "popen-gw0" / "repo" / ".git" / "objects" / "ab"
    target.mkdir(parents=True)
    obj = target / "cdef"
    obj.write_text("x", encoding="utf-8")
    os.chmod(obj, stat.S_IREAD)
    gate.remove_tree(tmp_path / "popen-gw0")
    assert not (tmp_path / "popen-gw0").exists()
    gate.remove_tree(tmp_path / "never-existed")


TEST_FILE = """import os
from pathlib import Path


def log(name):
    with open(os.environ["ORDER_LOG"], "a", encoding="utf-8") as f:
        f.write(name + "\\n")


def test_one(tmp_path):
    log("{name}")
    (tmp_path / "out.txt").write_text("x", encoding="utf-8")


def test_two():
    log("{name}")
    assert os.environ.get("FAIL_IN") != "{name}"
"""

MANY_FILE = """import pytest


@pytest.mark.parametrize("i", range(1000))
def test_many(i):
    assert i >= 0
"""


def run_pytest(project: Path, temproot: Path, *args: str, fail_in: str = "") -> str:
    env = {k: v for k, v in os.environ.items() if not k.startswith(("PYTEST_", "AITS_PI_"))}
    env["PYTEST_DEBUG_TEMPROOT"] = str(temproot)
    env["ORDER_LOG"] = str(project / "order.log")
    env["FAIL_IN"] = fail_in
    done = subprocess.run(
        [sys.executable, "-m", "pytest", "-n", "1", "--dist", "loadfile", "-q", *args],
        cwd=project,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=300,
    )
    return f"exit={done.returncode}\n{done.stdout}{done.stderr}"


def test_xdist_run_orders_files_cleans_up_and_records_timing(tmp_path: Path) -> None:
    project = tmp_path / "project"
    temproot = tmp_path / "temproot"
    project.mkdir()
    temproot.mkdir()
    (project / "pytest.ini").write_text("[pytest]\n", encoding="utf-8")
    (project / "conftest.py").write_bytes(CONFTEST.read_bytes())
    seed = {"files": {"test_b.py": 9.0, "test_c.py": 5.0, "test_many.py": 20.0}}
    (project / "file_durations.json").write_text(json.dumps(seed), encoding="utf-8")
    for name in ("test_a", "test_b", "test_c"):
        (project / f"{name}.py").write_text(TEST_FILE.replace("{name}", name), encoding="utf-8")
    # A third test in a: xdist's own order (by test count) would run a before b and c.
    with (project / "test_a.py").open("a", encoding="utf-8") as f:
        f.write('\n\ndef test_three():\n    log("test_a")\n')
    (project / "test_many.py").write_text(MANY_FILE, encoding="utf-8")

    out = run_pytest(project, temproot)
    assert out.startswith("exit=0"), out
    order = (project / "order.log").read_text(encoding="utf-8").split()
    # Collected a, b, c, many; queued many (20 s), b (9 s), c (5 s), then a (unknown, 3 x 0.3 s).
    assert order == ["test_b", "test_b", "test_c", "test_c", "test_a", "test_a", "test_a"]
    lines = [line for line in out.splitlines() if line.strip()]
    assert any(line.startswith("gate timing: 1 workers, 1007 tests;") for line in lines), out
    assert "passed" in lines[-1] and "gate timing" not in lines[-1]
    cache = project / ".pytest_cache" / "v" / "aits"
    recorded = json.loads((cache / "file_durations").read_text(encoding="utf-8"))
    assert set(recorded) == {"test_a.py", "test_b.py", "test_c.py", "test_many.py"}
    timing = json.loads((cache / "last_gate_timing").read_text(encoding="utf-8"))
    assert timing["tests"] == 1007 and timing["workers"] == 1
    assert list(temproot.glob("pytest-of-*/pytest-[0-9]*/popen-gw0")) == []

    # An earlier recording of a file this run does not touch is kept.
    (cache / "file_durations").write_text(
        json.dumps({**recorded, "test_gone.py": 7.5}), encoding="utf-8"
    )
    recorded = json.loads((cache / "file_durations").read_text(encoding="utf-8"))
    out = run_pytest(project, temproot, "test_many.py")
    assert out.startswith("exit=0"), out
    rerun = json.loads((cache / "file_durations").read_text(encoding="utf-8"))
    assert set(rerun) == set(recorded) and rerun["test_gone.py"] == 7.5

    recorded = rerun
    (project / "order.log").unlink()
    out = run_pytest(project, temproot, "test_a.py", fail_in="test_a")
    assert out.startswith("exit=1"), out
    kept = list(temproot.glob("pytest-of-*/pytest-[0-9]*/popen-gw0"))
    assert len(kept) == 1 and list(kept[0].glob("*/out.txt")), out
    assert "gate timing" not in out
    # A run below 1000 tests leaves the recorded durations alone.
    assert json.loads((cache / "file_durations").read_text(encoding="utf-8")) == recorded
