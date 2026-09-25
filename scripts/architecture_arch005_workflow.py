"""Public DEVX-015 engineering workflow commands; no implicit Full or publication."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from ai_trading_system.platform.architecture.workflow_contract import bounded_regular_bytes
from ai_trading_system.platform.architecture.workflow_integration import (
    build_controlled_merge_plan,
    finish_source_installation,
    freeze_controlled_merge_review,
    inspect_architecture_merge_outputs,
    inspect_canonical_merge_outputs,
    inspect_compatibility_merge_outputs,
    inspect_controlled_candidate_delta,
    inspect_report_merge_outputs,
    recover_source_candidate,
    source_candidate_worker,
    source_installation_worker,
    start_source_candidate,
    start_source_installation,
    validate_controlled_merge_plan,
)
from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(description="DEVX-015 当前权威下的受控工程工作流")
    commands = parser.add_subparsers(dest="command", required=True)
    source = commands.add_parser("source-candidate", help="实际Job生成并构造私有S；不安装或发布。")
    source.add_argument("--task-id", required=True)
    source.add_argument("--publication-transaction", type=Path, required=True)
    source.add_argument("--actor", required=True)
    source.add_argument("--request-id", required=True)
    worker = commands.add_parser("source-worker", help="仅同一已绑定Job内的source worker。")
    worker.add_argument("--execution-request", type=Path, required=True)
    install_worker = commands.add_parser(
        "source-install-worker", help="仅同一受托管Job内安装或恢复。"
    )
    install_worker.add_argument("--execution-request", type=Path, required=True)
    for name in ("source-install", "source-install-recover", "source-final-handoff"):
        install = commands.add_parser(
            name, help="按原source计划安装或恢复；不发布、不重派发旧请求。"
        )
        install.add_argument("--task-id", required=True)
        install.add_argument("--publication-transaction", type=Path, required=True)
        install.add_argument("--actor", required=True)
        install.add_argument("--source-request-id", required=True)
        install.add_argument("--request-id", required=True)
    recovery = commands.add_parser(
        "source-recover", help="恢复原source请求并安全失败释放；不重派发。"
    )
    recovery.add_argument("--task-id", required=True)
    recovery.add_argument("--publication-transaction", type=Path, required=True)
    recovery.add_argument("--actor", required=True)
    recovery.add_argument("--request-id", required=True)
    commands.add_parser("control-inspect", help="只读核验当前主机注册；不创建或启用协调根。")
    canonical = commands.add_parser(
        "canonical-input-inspect", help="只读核验canonical输出全集及main历史；不生成或构造候选。"
    )
    canonical.add_argument("--task-id", required=True)
    architecture = commands.add_parser(
        "architecture-input-inspect", help="只读重算五项architecture输出；不生成或构造候选。"
    )
    architecture.add_argument("--task-id", required=True)
    report = commands.add_parser(
        "report-input-inspect", help="只读核验报告流官方输出与main已知历史文件；不生成或构造候选。"
    )
    report.add_argument("--task-id", required=True)
    compatibility = commands.add_parser(
        "compatibility-input-inspect",
        help="只读核验兼容性全部官方fragments与历史顺序；不生成或删除。",
    )
    compatibility.add_argument("--task-id", required=True)
    candidate = commands.add_parser(
        "candidate-input-inspect", help="核验完整候选delta的精确源码/官方输出归属；不生成或提交。"
    )
    candidate.add_argument("--task-id", required=True)
    candidate.add_argument("--publication-transaction", type=Path, required=True)
    candidate.add_argument("--actor", required=True)
    drain = commands.add_parser(
        "control-drain-inspect", help="只读观察已登记旧租约，不以TTL抢占或启用迁移。"
    )
    drain.add_argument("--lease-policy", type=Path, required=True)
    enrollment = commands.add_parser(
        "control-enrollment-plan", help="只读盘点首次主机迁移；计划不授权安装或激活。"
    )
    enrollment.add_argument("--lease-policy", type=Path, required=True)
    enrollment.add_argument("--control-root", type=Path, required=True)
    enrollment.add_argument("--legacy-root", type=Path, action="append", required=True)
    enrollment.add_argument("--additional-repository", type=Path, action="append", default=[])
    for name in ("control-enroll", "control-enroll-recover"):
        command = commands.add_parser(name, help="管理员登记/恢复 DRAINING；不启用 ACTIVE。")
        command.add_argument("--lease-policy", type=Path, required=True)
        command.add_argument("--actor", required=True)
        command.add_argument("--plan" if name == "control-enroll" else "--control-root",
                             type=Path, required=True)
    for name in ("merge-plan", "merge-validate", "merge-review"):
        command = commands.add_parser(name)
        command.add_argument("--task-id", required=True)
        if name == "merge-plan":
            command.add_argument(
                "--record", action="store_true", help="保存不可变计划，输出短引用；不修改Git refs。"
            )
        if name == "merge-validate":
            command.add_argument("--plan", type=Path, required=True)
            command.add_argument("--planning-only", action="store_true")
        if name == "merge-review":
            command.add_argument("--proposal", type=Path, required=True)
            command.add_argument("--publication-transaction", type=Path, required=True)
            command.add_argument("--actor", required=True)
            command.add_argument("--change-id", required=True)
    args = parser.parse_args()
    if args.command == "source-candidate":
        result = start_source_candidate(
            PROJECT_ROOT,
            args.task_id,
            transaction_path=args.publication_transaction,
            actor=args.actor,
            request_id=args.request_id,
        )
    elif args.command == "source-worker":
        result = source_candidate_worker(PROJECT_ROOT, args.execution_request)
    elif args.command == "source-install-worker":
        result = source_installation_worker(PROJECT_ROOT, args.execution_request)
    elif args.command == "source-final-handoff":
        result = finish_source_installation(
            PROJECT_ROOT,
            args.task_id,
            transaction_path=args.publication_transaction,
            actor=args.actor,
            source_request_id=args.source_request_id,
            request_id=args.request_id,
        )
    elif args.command in {"source-install", "source-install-recover"}:
        result = start_source_installation(
            PROJECT_ROOT,
            args.task_id,
            transaction_path=args.publication_transaction,
            actor=args.actor,
            source_request_id=args.source_request_id,
            request_id=args.request_id,
            action="RECOVER" if args.command == "source-install-recover" else "INSTALL",
        )
    elif args.command == "source-recover":
        result = recover_source_candidate(
            PROJECT_ROOT,
            args.task_id,
            transaction_path=args.publication_transaction,
            actor=args.actor,
            request_id=args.request_id,
        )
    elif args.command == "canonical-input-inspect":
        result = inspect_canonical_merge_outputs(PROJECT_ROOT, args.task_id)
    elif args.command == "architecture-input-inspect":
        result = inspect_architecture_merge_outputs(PROJECT_ROOT, args.task_id)
    elif args.command == "report-input-inspect":
        result = inspect_report_merge_outputs(PROJECT_ROOT, args.task_id)
    elif args.command == "compatibility-input-inspect":
        result = inspect_compatibility_merge_outputs(PROJECT_ROOT, args.task_id)
    elif args.command == "candidate-input-inspect":
        result = inspect_controlled_candidate_delta(
            PROJECT_ROOT,
            args.task_id,
            transaction_path=args.publication_transaction,
            actor=args.actor,
        )
    elif args.command in {"control-enroll", "control-enroll-recover"}:
        from ai_trading_system.platform.architecture.parallel_control_kernel import (
            load_parallel_control_policy,
        )
        from ai_trading_system.platform.architecture.workflow_coordination import (
            enroll_host_draining,
        )

        result = enroll_host_draining(
            PROJECT_ROOT, policy=load_parallel_control_policy(args.lease_policy), actor=args.actor,
            plan_path=args.plan if args.command == "control-enroll" else None,
            recover_root=args.control_root if args.command == "control-enroll-recover" else None,
        )
    elif args.command == "control-enrollment-plan":
        from ai_trading_system.platform.architecture.parallel_control_kernel import (
            load_parallel_control_policy,
        )
        from ai_trading_system.platform.architecture.workflow_coordination import (
            plan_host_enrollment,
        )

        result = plan_host_enrollment(
            PROJECT_ROOT, policy=load_parallel_control_policy(args.lease_policy),
            control_root=args.control_root, legacy_roots=args.legacy_root,
            additional_repositories=args.additional_repository,
        )
    elif args.command == "control-drain-inspect":
        from ai_trading_system.platform.architecture.parallel_control_kernel import (
            load_parallel_control_policy,
        )
        from ai_trading_system.platform.architecture.workflow_coordination import (
            inspect_control_drain,
        )

        result = inspect_control_drain(
            PROJECT_ROOT, policy=load_parallel_control_policy(args.lease_policy)
        )
    elif args.command == "control-inspect":
        from ai_trading_system.platform.architecture.workflow_coordination import (
            inspect_host_control,
        )

        result = inspect_host_control(PROJECT_ROOT)
    elif args.command == "merge-plan":
        result = build_controlled_merge_plan(PROJECT_ROOT, args.task_id)
        if args.record:
            from ai_trading_system.platform.architecture.integration_publication_fence import (
                _write_json_exclusive,
            )

            destination = (
                PROJECT_ROOT
                / "outputs/architecture/workflow_integration/plans"
                / (result["plan_sha256"] + ".json")
            )
            if not destination.exists():
                _write_json_exclusive(destination, result)
            content = bounded_regular_bytes(destination)
            if load_strict_json_text(content.decode("utf-8")) != result:
                raise SystemExit("existing immutable plan differs")
            result = {
                "status": result["status"],
                "plan_sha256": result["plan_sha256"],
                "plan_ref": {
                    "path": destination.relative_to(PROJECT_ROOT).as_posix(),
                    "sha256": hashlib.sha256(content).hexdigest(),
                },
            }
    elif args.command == "merge-validate":
        plan = load_strict_json_text(bounded_regular_bytes(args.plan).decode("utf-8"))
        result = validate_controlled_merge_plan(
            PROJECT_ROOT, args.task_id, plan, require_review=not args.planning_only
        )
    else:
        proposal = load_strict_json_text(bounded_regular_bytes(args.proposal).decode("utf-8"))
        result = freeze_controlled_merge_review(
            PROJECT_ROOT,
            args.task_id,
            proposal=proposal,
            transaction_path=args.publication_transaction,
            actor=args.actor,
            change_id=args.change_id,
        )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
