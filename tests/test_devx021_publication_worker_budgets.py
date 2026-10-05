"""DEVX-021 section 8: the real local publication's hang bounds are named, ordered and fit the lease TTL.

The worker wall and the git-child wait bound a hung publication worker or git process. They are not
performance expectations: the real publication replays a 689 MB lease store in every git hook, so the
former 3600 s / 1800 s literals (values for a small idle repository) cut v26 off after the
fast-forward merge. These tests keep the values named, ordered and coherent with the checkout lease.
"""

from __future__ import annotations

import ast
from pathlib import Path

import yaml

from ai_trading_system.platform.architecture import workflow_coordination as coordination

ROOT = Path(__file__).resolve().parents[1]
CHECKOUT_POLICY = ROOT / "config/architecture/arch_005_s4d_checkout_guard.yaml"
COORDINATION_SOURCE = ROOT / "src/ai_trading_system/platform/architecture/workflow_coordination.py"
# Literal values before DEVX-021 section 8.
FORMER_WORKER_WALL_SECONDS = 3600
FORMER_GIT_CHILD_WAIT_SECONDS = 1800
# Worker start-up (about 2,100 s in v26) plus closing margin (about 1,500 s) around the
# git-child wait.
WORKER_PHASES_AROUND_GIT_CHILD_SECONDS = 3600


def _timeout_arguments(source: str, function: str) -> dict[str, list[str]]:
    """``timeout=`` arguments of ``.wait``/``.wait_exit`` calls inside the named function."""
    found: dict[str, list[str]] = {}
    for node in ast.walk(ast.parse(source)):
        if not (isinstance(node, ast.FunctionDef) and node.name == function):
            continue
        for call in ast.walk(node):
            if not (
                isinstance(call, ast.Call)
                and isinstance(call.func, ast.Attribute)
                and call.func.attr in {"wait", "wait_exit"}
            ):
                continue
            for keyword in call.keywords:
                if keyword.arg == "timeout":
                    found.setdefault(call.func.attr, []).append(ast.unparse(keyword.value))
    return found


def test_budgets_exceed_the_former_values_and_leave_room_around_the_git_child() -> None:
    assert coordination.LOCAL_PUBLICATION_GIT_CHILD_WAIT_SECONDS > FORMER_GIT_CHILD_WAIT_SECONDS
    assert coordination.LOCAL_PUBLICATION_WORKER_WALL_SECONDS > FORMER_WORKER_WALL_SECONDS
    assert (
        coordination.LOCAL_PUBLICATION_WORKER_WALL_SECONDS
        - coordination.LOCAL_PUBLICATION_GIT_CHILD_WAIT_SECONDS
        >= WORKER_PHASES_AROUND_GIT_CHILD_SECONDS
    )


def test_worker_wall_fits_within_half_of_the_checkout_lease_ttl() -> None:
    lease = yaml.safe_load(CHECKOUT_POLICY.read_text(encoding="utf-8"))["lease"]
    # The Full driver heartbeats the lease while it runs; the publication starts afterwards, so the
    # worker needs the wall plus the gap between the Full's end and the publication trigger.
    assert coordination.LOCAL_PUBLICATION_WORKER_WALL_SECONDS <= lease["ttl_seconds"] / 2
    assert lease["heartbeat_interval_seconds"] < lease["ttl_seconds"]


def test_publication_call_sites_use_the_named_budgets() -> None:
    source = COORDINATION_SOURCE.read_text(encoding="utf-8")
    assert _timeout_arguments(source, "publish_local") == {
        "wait": ["LOCAL_PUBLICATION_WORKER_WALL_SECONDS"]
    }
    assert _timeout_arguments(source, "run_publication_worker") == {
        "wait_exit": ["LOCAL_PUBLICATION_GIT_CHILD_WAIT_SECONDS"]
    }


def test_call_site_detector_sees_a_literal_timeout() -> None:
    # Self-check: the detector reports what the former code looked like, so a regression to a
    # literal cannot pass the call-site test above unnoticed.
    former = (
        "def publish_local(self):\n"
        "    code = handle.wait(timeout=3600)\n"
        "def run_publication_worker(self):\n"
        "    child.wait_exit(timeout=1800)\n"
    )
    assert _timeout_arguments(former, "publish_local") == {"wait": ["3600"]}
    assert _timeout_arguments(former, "run_publication_worker") == {"wait_exit": ["1800"]}
    assert _timeout_arguments(former, "another_function") == {}
