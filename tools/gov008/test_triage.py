"""GOV-008 P1: triage every test file against the owner's "delete by default, keep with proof" rule.

Inputs: git-tracked tests plus a Full ``test_runtime_profile.json`` (per-node durations).
Output: ``test_triage.csv`` (one row per test file) and ``test_triage_summary.json``.

Verdicts:
  DELETE_WITH_MECHANISM        tests of the publication/workflow machinery that is being replaced.
  DELETE_WITH_ETF_RETIREMENT   tests of the retired ETF dynamic v3 family (owner, 2026-10-10).
  REVIEW_DELETE_LEASE_VARIANTS research-capture tests that depend on the lease/publication
                               environment: delete those variants, keep behavior contracts.
  KEEP_CONTRACT                research-capture tests with no machinery dependency.
  REVIEW_DECOUPLE              product tests that bind to the machinery: decouple, then keep.
  KEEP_PR                      product tests (fast nodes run in the PR suite, slow nodes nightly).
  KEEP_DEPENDENCY_DIRECTION_REVIEW  dependency-direction checks worth keeping in a lighter form.

usage: python tools/gov008/test_triage.py --repo . --profile <test_runtime_profile.json> --out <dir>
"""

from __future__ import annotations

import argparse
import collections
import csv
import hashlib
import json
import re
import subprocess
from pathlib import Path

from modgraph import MACHINERY_PREFIX, PKG, git_tracked, is_machinery_script, raw_imports

# Boundary between the PR suite and the nightly suite. TEMPORARY PILOT BASELINE (GOV-008 section 4):
# derived from the 2026-10-10 Full profile (110 product nodes >= 30 s hold 2.2 of 4.2 node-hours);
# exit condition: calibrated against the measured PR-suite wall time in P2 spike S1.
SLOW_NODE_SECONDS = 30.0

# File-name groups, identical to the P1 mapping document (M1-M7 mechanisms, R1 research capture).
GROUP_PATTERNS = [
    ("M2", r"test_devx015"),
    (
        "M1",
        r"test_arch_005_(integration_publication|publication|lease|s4d|checkout|parallel|s2_kernel|"
        r"s3_scheduler|s4_dispatch|integration_revalidation|supervised|s4a|compat_ledger)|"
        r"test_devx0(16|21|23)|test_devx_016|test_arch_005_prebootstrap",
    ),
    ("M3", r"test_arch_005_(task_checkpoint|source_preservation|bootstrap|handoff|portable)"),
    (
        "M4",
        r"test_arch_005_(task_registry|task_source|s5|task_portfolio)|test_gov_?006|task_portfolio",
    ),
    (
        "M5",
        r"test_arch_004|test_devx_006|test_arch_005_m|test_devex|dependency|callback|cli_contract|wave|"
        r"test_trading24\d\d_architecture_contract",
    ),
    (
        "M6",
        r"test_validation_(tier|runtime|readiness)|test_devx018|test_devx022|scheduling|pytest_runtime",
    ),
    ("M7", r"test_governed_development_skill|test_devx_007|web_pro"),
    (
        "R1",
        r"test_(named_|composer_prospective|prospective_|research_outcome_access|refined_method|"
        r"qqq_options_daily_transport|growth_action_value_mandatory)",
    ),
]
MACHINERY_TOKENS = (
    "architecture_arch005_",
    "run_validation_tier",
    "integration_publication_fence",
    "AITS_NAMED_DQ_PUBLICATION_TRANSACTION",
    "checkout_guard",
    "FileExecutionLeaseStore",
    "source_lease",
)
ETF_PREFIXES = (f"{PKG}.etf_portfolio", f"{PKG}.interfaces.cli.etf_portfolio")
# Owner decision 2026-10-10 (GOV-008 P1 review): Atlas is retired.
ATLAS_PREFIX = f"{PKG}.atlas"


def load_frozen_modules(lists_dir: Path) -> set[str]:
    """Dormant research and frozen CLI/framework modules from the code_usage classification.

    Machinery, ETF and Atlas have their own test verdicts, so they are not repeated here.
    """
    path = lists_dir / "code_usage.csv"
    if not path.exists():
        return set()
    with path.open(encoding="utf-8", newline="") as handle:
        return {
            row["module"]
            for row in csv.DictReader(handle)
            if row["class"] in ("FREEZE_DORMANT", "FREEZE_CLI_FRAMEWORK")
        }


def imports_frozen(imports: set[str], frozen: set[str]) -> bool:
    """True when any import names a frozen module (or an attribute of one)."""
    for dotted in imports:
        parts = dotted.split(".")
        if any(".".join(parts[:k]) in frozen for k in range(2, len(parts) + 1)):
            return True
    return False


def group_of(path: str) -> str:
    for group, pattern in GROUP_PATTERNS:
        if re.search(pattern, path):
            return group
    return "P"


def support_closure(
    start: set[str], support: dict[str, Path], seen: set[str] | None = None
) -> set[str]:
    """Imports of a test plus those of the flat ``tests/*_support``-style helper modules it uses."""
    seen = seen or set()
    result = set(start)
    for name in start:
        head = name.split(".")[0]
        if head in support and head not in seen:
            seen.add(head)
            result |= support_closure(raw_imports(support[head]), support, seen)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    repo = args.repo.resolve()
    out = args.out if args.out.is_absolute() else repo / args.out
    out.mkdir(parents=True, exist_ok=True)

    profile_bytes = args.profile.read_bytes()
    profile = json.loads(profile_bytes)
    per_file: dict[str, dict[str, float]] = collections.defaultdict(
        lambda: {"nodes": 0, "seconds": 0.0, "slow_nodes": 0, "slow_seconds": 0.0}
    )
    for node in profile["nodes"]:
        row = per_file[node["file"]]
        seconds = float(node["duration_seconds"])
        row["nodes"] += 1
        row["seconds"] += seconds
        if seconds >= SLOW_NODE_SECONDS:
            row["slow_nodes"] += 1
            row["slow_seconds"] += seconds

    frozen_modules = load_frozen_modules(out)
    tracked = [p for p in git_tracked(repo, "tests") if p.endswith(".py")]
    support = {
        Path(p).stem: repo / p
        for p in tracked
        if not Path(p).name.startswith("test_") and p.count("/") == 1
    }
    rows = []
    for rel in tracked:
        if not Path(rel).name.startswith("test_"):
            continue
        imports = support_closure(raw_imports(repo / rel), support)
        text = (repo / rel).read_text(encoding="utf-8", errors="ignore")
        for helper in {n.split(".")[0] for n in imports} & set(support):
            text += (support[helper]).read_text(encoding="utf-8", errors="ignore")
        machinery_import = any(
            n == MACHINERY_PREFIX
            or n.startswith(MACHINERY_PREFIX + ".")
            or is_machinery_script(n.split(".")[-1])
            for n in imports
        )
        token_hit = any(token in text for token in MACHINERY_TOKENS)
        etf = any(n == p or n.startswith(p + ".") for n in imports for p in ETF_PREFIXES) or bool(
            re.search(r"dynamic_v3|test_etf_", rel)
        )
        atlas = rel.startswith("tests/atlas/") or any(
            n == ATLAS_PREFIX or n.startswith(ATLAS_PREFIX + ".") for n in imports
        )
        group = group_of(rel)
        if group.startswith("M"):
            cls, verdict = group, "DELETE_WITH_MECHANISM"
            if group == "M5" and "dependency" in rel:
                verdict = "KEEP_DEPENDENCY_DIRECTION_REVIEW"
        elif etf:
            cls, verdict = "ETF", "DELETE_WITH_ETF_RETIREMENT"
        elif atlas:
            cls, verdict = "ATLAS", "DELETE_WITH_ATLAS_RETIREMENT"
        elif imports_frozen(imports, frozen_modules):
            cls, verdict = "FROZEN_CODE", "DELETE_WITH_FROZEN_CODE"
        elif group == "R1":
            if machinery_import or token_hit:
                cls, verdict = "R1_LEASE_DEPENDENT", "REVIEW_DELETE_LEASE_VARIANTS"
            else:
                cls, verdict = "R1_CONTRACT", "KEEP_CONTRACT"
        elif machinery_import or token_hit:
            cls, verdict = "P_COUPLED", "REVIEW_DECOUPLE"
        else:
            cls, verdict = "P", "KEEP_PR"
        stats = per_file.get(
            rel, {"nodes": 0, "seconds": 0.0, "slow_nodes": 0, "slow_seconds": 0.0}
        )
        rows.append(
            {
                "test_file": rel,
                "class": cls,
                "verdict": verdict,
                "nodes": int(stats["nodes"]),
                "node_seconds": round(stats["seconds"], 1),
                "slow_nodes": int(stats["slow_nodes"]),
                "slow_node_seconds": round(stats["slow_seconds"], 1),
                "machinery_import": int(machinery_import),
                "machinery_token": int(token_hit),
                "etf": int(etf),
            }
        )
    with (out / "test_triage.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)

    # Exclude list consumed by tools/gov008/pr_filter.py (the PR-suite transition plugin). It is an
    # exclude list on purpose: a test file added after this triage is collected by default.
    exclude_verdicts = (
        "DELETE_WITH_MECHANISM",
        "DELETE_WITH_ETF_RETIREMENT",
        "DELETE_WITH_ATLAS_RETIREMENT",
        "DELETE_WITH_FROZEN_CODE",
        "REVIEW_DELETE_LEASE_VARIANTS",
    )
    exclude_files = sorted(r["test_file"] for r in rows if r["verdict"] in exclude_verdicts)
    (out / "pr_exclude.txt").write_text(
        "\n".join(exclude_files) + "\n", encoding="utf-8", newline="\n"
    )

    by_verdict: dict[str, dict[str, float]] = collections.defaultdict(
        lambda: {"files": 0, "nodes": 0, "node_hours": 0.0}
    )
    for row in rows:
        entry = by_verdict[row["verdict"]]
        entry["files"] += 1
        entry["nodes"] += row["nodes"]
        entry["node_hours"] += row["node_seconds"] / 3600
    keep_pr_seconds = sum(
        r["node_seconds"] - r["slow_node_seconds"]
        for r in rows
        if r["verdict"] in ("KEEP_PR", "KEEP_CONTRACT")
    )
    nightly_seconds = sum(
        r["slow_node_seconds"] for r in rows if r["verdict"] in ("KEEP_PR", "KEEP_CONTRACT")
    )
    review_seconds = sum(r["node_seconds"] for r in rows if r["verdict"].startswith("REVIEW"))
    head = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
    summary = {
        "source_commit": head,
        "profile": str(args.profile),
        "profile_sha256": hashlib.sha256(profile_bytes).hexdigest(),
        "slow_node_seconds_boundary": SLOW_NODE_SECONDS,
        "by_verdict": {
            k: {kk: round(vv, 2) for kk, vv in v.items()} for k, v in sorted(by_verdict.items())
        },
        "pr_suite_node_hours_keep_only": round(keep_pr_seconds / 3600, 2),
        "nightly_node_hours_keep_only": round(nightly_seconds / 3600, 2),
        "review_pool_node_hours": round(review_seconds / 3600, 2),
        "caveats": [
            "file-name groups and direct-import detection are approximations",
            "REVIEW_* verdicts need the owner's exception decisions before deletion",
            "node durations come from a saturated 16-worker Full: 1.1-1.4x an idle host",
        ],
    }
    (out / "test_triage_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=1) + "\n", encoding="utf-8", newline="\n"
    )
    print(json.dumps(summary["by_verdict"], ensure_ascii=False, indent=1))
    print(
        {
            k: summary[k]
            for k in (
                "pr_suite_node_hours_keep_only",
                "nightly_node_hours_keep_only",
                "review_pool_node_hours",
            )
        }
    )


if __name__ == "__main__":
    main()
