from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, date, datetime
from pathlib import Path

from ai_trading_system.run_artifacts import (
    build_run_artifact_paths,
    default_daily_run_id,
    mirror_canonical_daily_ops_outputs_to_legacy,
    mirror_legacy_reports_to_run,
    prepare_run_directories,
    safe_run_id,
    write_run_manifest,
)


def test_run_artifact_paths_are_execution_time_scoped(tmp_path: Path) -> None:
    run_id = "daily_ops_run:2026-05-06:test/id"
    generated_at = datetime(2026, 5, 6, 15, 30, tzinfo=UTC)
    paths = prepare_run_directories(
        build_run_artifact_paths(
            as_of=date(2026, 5, 6),
            run_id=run_id,
            output_root=tmp_path / "runs",
            generated_at=generated_at,
        )
    )

    assert safe_run_id(run_id) == "daily_ops_run_2026-05-06_test_id"
    assert paths.execution_timestamp_utc == "20260506T153000Z"
    assert paths.run_root == (
        tmp_path
        / "runs"
        / "daily"
        / "20260506T153000Z"
        / "as_of_2026-05-06__daily_ops_run_2026-05-06_test_id"
    )
    assert paths.reports_dir.is_dir()
    assert paths.traces_dir.is_dir()
    assert paths.metadata_dir.is_dir()


def test_default_daily_run_id_uses_as_of_and_utc_timestamp() -> None:
    run_id = default_daily_run_id(
        date(2026, 5, 6),
        generated_at=datetime(2026, 5, 6, 15, 30, tzinfo=UTC),
    )

    assert run_id == "daily_ops_run:2026-05-06:20260506T153000Z"


def test_run_manifest_checksums_and_mirrors_without_payload_text(
    tmp_path: Path,
) -> None:
    paths = prepare_run_directories(
        build_run_artifact_paths(
            as_of=date(2026, 5, 6),
            run_id="daily_ops_run:2026-05-06:test",
            output_root=tmp_path / "runs",
            generated_at=datetime(2026, 5, 6, 21, 5, tzinfo=UTC),
        )
    )
    legacy_reports_dir = tmp_path / "outputs" / "reports"
    legacy_score = legacy_reports_dir / "daily_score_2026-05-06.md"
    legacy_trace = legacy_reports_dir / "evidence" / "daily_score_2026-05-06_trace.json"
    legacy_daily_ops = legacy_reports_dir / "daily_ops_run_2026-05-06.md"
    for path, text in (
        (legacy_score, "# score\n"),
        (legacy_trace, '{"trace": true}\n'),
        (legacy_daily_ops, "# legacy daily ops should not overwrite canonical\n"),
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    canonical_plan = paths.reports_dir / "daily_ops_plan_2026-05-06.md"
    canonical_run = paths.reports_dir / "daily_ops_run_2026-05-06.md"
    canonical_metadata = paths.metadata_dir / "daily_ops_run_metadata_2026-05-06.json"
    input_path = tmp_path / "data" / "raw" / "prices_daily.csv"
    for path, text in (
        (canonical_plan, "# canonical plan\n"),
        (canonical_run, "# canonical run\n"),
        (
            canonical_metadata,
            '{"stdout": "SECRET_SHOULD_NOT_APPEAR", "stderr": "RAW_STDERR"}\n',
        ),
        (input_path, "date,ticker,close\n2026-05-06,NVDA,1\n"),
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    copied_to_run = mirror_legacy_reports_to_run(
        as_of=date(2026, 5, 6),
        legacy_reports_dir=legacy_reports_dir,
        paths=paths,
    )
    copied_to_legacy = mirror_canonical_daily_ops_outputs_to_legacy(
        paths=paths,
        legacy_reports_dir=legacy_reports_dir,
    )
    manifest_path = write_run_manifest(
        paths=paths,
        project_root=tmp_path,
        status="PASS",
        visibility_cutoff=datetime(2026, 5, 6, 21, 0, tzinfo=UTC),
        visibility_cutoff_source="test",
        legacy_output_mode="mirror",
        input_artifacts=(input_path,),
        canonical_output_artifacts=(
            canonical_plan,
            canonical_run,
            canonical_metadata,
            *copied_to_run,
        ),
        legacy_output_artifacts=(*copied_to_legacy,),
        generated_at=datetime(2026, 5, 6, 21, 5, tzinfo=UTC),
    )

    assert (paths.reports_dir / "daily_score_2026-05-06.md").exists()
    assert (paths.traces_dir / "daily_score_2026-05-06_trace.json").exists()
    assert legacy_daily_ops.read_text(encoding="utf-8") == "# canonical run\n"
    manifest_text = manifest_path.read_text(encoding="utf-8")
    manifest = json.loads(manifest_text)
    expected_sha = hashlib.sha256(input_path.read_bytes()).hexdigest()
    input_records = {
        record["path"]: record for record in manifest["input_artifacts"]
    }
    assert input_records[str(input_path)]["sha256"] == expected_sha
    assert manifest["run_id"] == "daily_ops_run:2026-05-06:test"
    assert manifest["execution_timestamp_utc"] == "20260506T210500Z"
    assert manifest["legacy_output_mode"] == "mirror"
    assert "SECRET_SHOULD_NOT_APPEAR" not in manifest_text
    assert "RAW_STDERR" not in manifest_text


def test_mirror_legacy_reports_to_run_ignores_stale_same_date_outputs(
    tmp_path: Path,
) -> None:
    paths = prepare_run_directories(
        build_run_artifact_paths(
            as_of=date(2026, 5, 10),
            run_id="daily_ops_run:2026-05-10:test",
            output_root=tmp_path / "runs",
            generated_at=datetime(2026, 5, 10, 16, 0, tzinfo=UTC),
        )
    )
    legacy_reports_dir = tmp_path / "outputs" / "reports"
    stale_dashboard = legacy_reports_dir / "evidence_dashboard_2026-05-10.html"
    fresh_health = legacy_reports_dir / "pipeline_health_2026-05-10.md"
    for path, text in (
        (stale_dashboard, "<html>old dashboard</html>\n"),
        (fresh_health, "# current health\n"),
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    run_started_at = datetime(2026, 5, 10, 16, 0, tzinfo=UTC)
    os.utime(stale_dashboard, (run_started_at.timestamp() - 10, run_started_at.timestamp() - 10))
    os.utime(fresh_health, (run_started_at.timestamp() + 10, run_started_at.timestamp() + 10))

    copied = mirror_legacy_reports_to_run(
        as_of=date(2026, 5, 10),
        legacy_reports_dir=legacy_reports_dir,
        paths=paths,
        min_modified_at=run_started_at,
    )

    assert paths.reports_dir / "pipeline_health_2026-05-10.md" in copied
    assert (paths.reports_dir / "pipeline_health_2026-05-10.md").exists()
    assert not (paths.reports_dir / "evidence_dashboard_2026-05-10.html").exists()
