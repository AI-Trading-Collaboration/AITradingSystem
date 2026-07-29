from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.data.access_control import (  # noqa: E402
    ACL_ATTESTATION_SCHEMA_VERSION,
    ACL_CLEANUP_RECEIPT_SCHEMA_VERSION,
    ACL_REHEARSAL_BUNDLE_SCHEMA_VERSION,
    apply_isolated_store_acl,
    build_acl_attestation,
    load_acl_policy,
    validate_acl_attestation,
    validate_acl_rehearsal_bundle,
)
from ai_trading_system.data.immutable_publish import (  # noqa: E402
    write_contained_artifact_bytes,
)
from ai_trading_system.platform.artifacts import canonical_json_bytes  # noqa: E402

DEFAULT_POLICY_PATH = PROJECT_ROOT / "config" / "data" / "data_foundation_acl.yaml"
DEFAULT_OUTPUT_PARENT = PROJECT_ROOT / "outputs" / "validation_runtime"


def run_acl_rehearsal(
    *,
    policy_path: Path,
    output_dir: Path,
    allowed_output_parent: Path,
    generated_at: datetime,
) -> dict[str, object]:
    if generated_at.tzinfo is None or generated_at.utcoffset() is None:
        raise ValueError("generated_at must be timezone-aware")
    output_root = _prepare_output_root(output_dir, allowed_output_parent)
    policy = load_acl_policy(policy_path)
    store_root = output_root / "live_rehearsal_store"
    store_root.mkdir()
    cleanup_required = True
    try:
        apply_isolated_store_acl(
            store_root,
            allowed_parent=output_root,
            policy=policy,
        )
        attestation = build_acl_attestation(
            store_root,
            policy=policy,
            generated_at=generated_at,
        )
        validate_acl_attestation(
            attestation,
            store_root=store_root,
            policy=policy,
        )
        attestation_raw = canonical_json_bytes(attestation)
        attestation_write = write_contained_artifact_bytes(
            root=output_root,
            relative_path="acl_attestation.json",
            content=attestation_raw,
            immutable=True,
        )
        attestation_pointer = _pointer(
            path="acl_attestation.json",
            artifact_id=_text(attestation.get("attestation_id"), "attestation_id"),
            schema_version=ACL_ATTESTATION_SCHEMA_VERSION,
            raw=attestation_raw,
        )
        if (
            attestation_write.sha256 != attestation_pointer["sha256"]
            or attestation_write.size_bytes != attestation_pointer["size_bytes"]
        ):
            raise RuntimeError("attestation durable-write result mismatch")
        shutil.rmtree(store_root)
        cleanup_required = False
        if store_root.exists():
            raise RuntimeError("rehearsal store cleanup failed")
        cleanup_body: dict[str, object] = {
            "schema_version": ACL_CLEANUP_RECEIPT_SCHEMA_VERSION,
            "store_identity": _text(attestation.get("store_identity"), "store_identity"),
            "resolved_store_root": _text(
                attestation.get("resolved_store_root"),
                "resolved_store_root",
            ),
            "root_existed_before": True,
            "root_exists_after": False,
            "cleanup_method": "trusted_writer_shutil_rmtree",
            "attestation_id": _text(attestation.get("attestation_id"), "attestation_id"),
            "attestation_sha256": hashlib.sha256(attestation_raw).hexdigest(),
            "production_effect": "none",
            "broker_action": "none",
        }
        cleanup = {
            "cleanup_receipt_id": f"acl_cleanup_{_digest(cleanup_body)[:32]}",
            **cleanup_body,
        }
        cleanup_raw = canonical_json_bytes(cleanup)
        cleanup_write = write_contained_artifact_bytes(
            root=output_root,
            relative_path="cleanup_receipt.json",
            content=cleanup_raw,
            immutable=True,
        )
        cleanup_pointer = _pointer(
            path="cleanup_receipt.json",
            artifact_id=_text(cleanup.get("cleanup_receipt_id"), "cleanup_receipt_id"),
            schema_version=ACL_CLEANUP_RECEIPT_SCHEMA_VERSION,
            raw=cleanup_raw,
        )
        if (
            cleanup_write.sha256 != cleanup_pointer["sha256"]
            or cleanup_write.size_bytes != cleanup_pointer["size_bytes"]
        ):
            raise RuntimeError("cleanup durable-write result mismatch")
        bundle_body: dict[str, object] = {
            "schema_version": ACL_REHEARSAL_BUNDLE_SCHEMA_VERSION,
            "status": "PASS",
            "generated_at": generated_at.astimezone(UTC).isoformat(),
            "policy_id": policy.policy_id,
            "policy_version": policy.policy_version,
            "policy_sha256": policy.policy_sha256,
            "attestation": attestation_pointer,
            "cleanup_receipt": cleanup_pointer,
            "claim_boundary": {
                "historical_manifest_store_acl_verified": False,
                "generic_consumer_cutover_allowed": False,
            },
            "production_effect": "none",
            "broker_action": "none",
        }
        bundle = {
            "bundle_id": f"data_foundation_acl_bundle_{_digest(bundle_body)[:32]}",
            **bundle_body,
        }
        bundle_raw = canonical_json_bytes(bundle)
        write_contained_artifact_bytes(
            root=output_root,
            relative_path="rehearsal_bundle.json",
            content=bundle_raw,
            immutable=True,
        )
        validated = validate_acl_rehearsal_bundle(
            output_root / "rehearsal_bundle.json",
            policy_path=policy_path,
        )
        if validated != bundle:
            raise RuntimeError("bundle validation normalized content")
        return bundle
    finally:
        if cleanup_required and store_root.exists():
            shutil.rmtree(store_root)


def _prepare_output_root(output_dir: Path, allowed_output_parent: Path) -> Path:
    parent = allowed_output_parent.resolve(strict=True)
    candidate = output_dir.resolve(strict=False)
    if candidate == parent or not candidate.is_relative_to(parent):
        raise ValueError("output_dir must be a child of allowed_output_parent")
    if candidate.exists():
        if not candidate.is_dir() or any(candidate.iterdir()):
            raise ValueError("output_dir must be missing or an empty directory")
    else:
        candidate.mkdir(parents=False)
    return candidate.resolve(strict=True)


def _pointer(
    *,
    path: str,
    artifact_id: str,
    schema_version: str,
    raw: bytes,
) -> dict[str, object]:
    return {
        "path": path,
        "artifact_id": artifact_id,
        "schema_version": schema_version,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "size_bytes": len(raw),
    }


def _digest(payload: Mapping[str, object]) -> str:
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be non-empty text")
    return value


def _parse_generated_at(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("generated-at must include a UTC offset")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="运行隔离的 Data Foundation store ACL 原生验收",
    )
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY_PATH)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--generated-at", type=_parse_generated_at, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    bundle = run_acl_rehearsal(
        policy_path=args.policy,
        output_dir=args.output_dir,
        allowed_output_parent=DEFAULT_OUTPUT_PARENT,
        generated_at=args.generated_at,
    )
    print(
        json.dumps(
            {
                "状态": bundle["status"],
                "bundle_id": bundle["bundle_id"],
                "ACL范围": "exact isolated store only",
                "历史store_acl_verified": False,
                "generic consumer cutover": False,
                "production_effect": "none",
                "broker_action": "none",
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
