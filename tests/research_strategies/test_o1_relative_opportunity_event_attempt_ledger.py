from __future__ import annotations

import copy
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml

import ai_trading_system.data.o1_relative_opportunity_event_attempt_ledger as ledger_module
from ai_trading_system.data.o1_relative_opportunity_event_attempt_ledger import (
    BLOCKED_STATUS,
    PASS_STATUS,
    O1EventAttemptFreezeError,
    SourceFetch,
    freeze_o1_event_and_attempt_ledgers,
    replay_o1_event_and_attempt_ledgers_from_retained_sources,
    validate_o1_event_attempt_freeze_gate,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
POLICY_PATH = PROJECT_ROOT / "config/research/o1_relative_opportunity_capability_audit_v1.yaml"
GENERATED_AT = datetime(2026, 7, 30, 0, 0, tzinfo=UTC)


def test_success_freezes_deterministic_event_and_attempt_ledgers(tmp_path: Path) -> None:
    project_root, policy_path, output_root = _project_fixture(tmp_path / "a")
    fetcher = _fixture_fetcher()
    first = freeze_o1_event_and_attempt_ledgers(
        output_root=output_root,
        project_root=project_root,
        generated_at=GENERATED_AT,
        audit_policy_path=policy_path.relative_to(project_root),
        fetcher=fetcher,
        source_commit_sha="a" * 40,
    )
    project_root_2, policy_path_2, output_root_2 = _project_fixture(tmp_path / "b")
    second = freeze_o1_event_and_attempt_ledgers(
        output_root=output_root_2,
        project_root=project_root_2,
        generated_at=GENERATED_AT,
        audit_policy_path=policy_path_2.relative_to(project_root_2),
        fetcher=_fixture_fetcher(),
        source_commit_sha="a" * 40,
    )

    assert first.status == PASS_STATUS
    assert first.event_ledger_path is not None
    assert second.event_ledger_path is not None
    first_events = json.loads(first.event_ledger_path.read_text(encoding="utf-8"))
    second_events = json.loads(second.event_ledger_path.read_text(encoding="utf-8"))
    assert first_events == second_events
    assert [row["event_family"] for row in first_events["events"]] == [
        "NFP",
        "CPI",
        "FOMC",
    ]
    assert first_events["events"][0]["event_timestamp"] == "2021-03-05T13:30:00+00:00"
    assert first_events["events"][1]["event_timestamp"] == "2021-03-10T13:30:00+00:00"
    assert first_events["events"][2]["event_timestamp"] == "2021-03-17T18:00:00+00:00"
    assert first.gate["next_authorization"] == {
        "coverage_only_gate_allowed": True,
        "model_training_allowed": False,
        "canonical_run_allowed": False,
        "production_allowed": False,
    }
    assert first.gate["claim_boundary"]["new_o1_result_read"] is False


def test_official_source_failure_writes_blocker_and_never_event_ledger(
    tmp_path: Path,
) -> None:
    project_root, policy_path, output_root = _project_fixture(tmp_path)

    def blocked_fetcher(url: str) -> SourceFetch:
        return SourceFetch(
            requested_url=url,
            final_url=url,
            downloaded_at=GENERATED_AT,
            status_code=403 if "bls.gov" in url else 200,
            content_type="text/html",
            body=(
                b"<html>Access Denied</html>"
                if "bls.gov" in url
                else _fed_index_html()
            ),
            error="HTTP_403" if "bls.gov" in url else None,
        )

    result = freeze_o1_event_and_attempt_ledgers(
        output_root=output_root,
        project_root=project_root,
        generated_at=GENERATED_AT,
        audit_policy_path=policy_path.relative_to(project_root),
        fetcher=blocked_fetcher,
        source_commit_sha="a" * 40,
    )

    assert result.status == BLOCKED_STATUS
    assert result.event_ledger_path is None
    assert not (output_root / "event_ledger.json").exists()
    assert result.gate["mechanical_classification"] == "INSUFFICIENT_COVERAGE_OR_DQ"
    assert result.gate["next_authorization"]["coverage_only_gate_allowed"] is False
    manifest = json.loads(result.source_manifest_path.read_text(encoding="utf-8"))
    assert {item["http_status"] for item in manifest["requests"]} == {200, 403}
    assert any(item["response_artifact_path"] for item in manifest["blockers"])
    assert json.loads(result.attempt_ledger_path.read_text(encoding="utf-8"))[
        "current_attempt"
    ]["result_read"] is False


def test_forbidden_source_domain_is_rejected_before_fetch(tmp_path: Path) -> None:
    project_root, policy_path, output_root = _project_fixture(tmp_path)

    with pytest.raises(O1EventAttemptFreezeError) as exc:
        ledger_module._capture_source(
            family="CPI",
            source_role="INDEX",
            url="https://example.com/cpi",
            raw_root=output_root.parent / "raw",
            fetcher=_fixture_fetcher(),
        )

    assert exc.value.code == "O1_EVENT_SOURCE_DOMAIN_FORBIDDEN"


def test_missing_release_timestamp_fails_closed(tmp_path: Path) -> None:
    project_root, policy_path, output_root = _project_fixture(tmp_path)
    responses = _fixture_responses()
    responses["https://www.bls.gov/news.release/archives/cpi_03102021.htm"] = (
        b"<html><body>Consumer Price Index</body></html>"
    )

    result = freeze_o1_event_and_attempt_ledgers(
        output_root=output_root,
        project_root=project_root,
        generated_at=GENERATED_AT,
        audit_policy_path=policy_path.relative_to(project_root),
        fetcher=_fixture_fetcher(responses),
        source_commit_sha="a" * 40,
    )

    assert result.status == BLOCKED_STATUS
    assert result.event_ledger_path is None
    assert any(
        item["code"] == "O1_EVENT_BLS_TIMESTAMP_MISSING"
        for item in result.gate["blockers"]
    )


def test_dq_gate_tamper_is_rejected_before_output_creation(tmp_path: Path) -> None:
    project_root, policy_path, output_root = _project_fixture(tmp_path)
    policy = yaml.safe_load(policy_path.read_text(encoding="utf-8"))
    dq_path = project_root / policy["isolated_dq_evidence"]["gate"]["path"]
    dq_path.write_bytes(dq_path.read_bytes() + b" ")

    with pytest.raises(O1EventAttemptFreezeError) as exc:
        freeze_o1_event_and_attempt_ledgers(
            output_root=output_root,
            project_root=project_root,
            generated_at=GENERATED_AT,
            audit_policy_path=policy_path.relative_to(project_root),
            fetcher=_fixture_fetcher(),
            source_commit_sha="a" * 40,
        )

    assert exc.value.code == "O1_EVENT_DQ_GATE_TAMPERED"
    assert not output_root.exists()


def test_attempt_family_policy_mismatch_is_rejected_before_output(tmp_path: Path) -> None:
    project_root, policy_path, output_root = _project_fixture(tmp_path)
    policy = yaml.safe_load(policy_path.read_text(encoding="utf-8"))
    policy["attempt_ledger_contract"]["current_attempt_family_id"] = "POST_RESULT_FAMILY"
    policy_path.write_text(yaml.safe_dump(policy, sort_keys=False), encoding="utf-8")

    with pytest.raises(O1EventAttemptFreezeError) as exc:
        freeze_o1_event_and_attempt_ledgers(
            output_root=output_root,
            project_root=project_root,
            generated_at=GENERATED_AT,
            audit_policy_path=policy_path.relative_to(project_root),
            fetcher=_fixture_fetcher(),
            source_commit_sha="a" * 40,
        )

    assert exc.value.code == "O1_EVENT_ATTEMPT_POLICY_INVALID"
    assert not output_root.exists()


def test_gate_id_detects_authorization_tamper(tmp_path: Path) -> None:
    project_root, policy_path, output_root = _project_fixture(tmp_path)
    result = freeze_o1_event_and_attempt_ledgers(
        output_root=output_root,
        project_root=project_root,
        generated_at=GENERATED_AT,
        audit_policy_path=policy_path.relative_to(project_root),
        fetcher=_fixture_fetcher(),
        source_commit_sha="a" * 40,
    )
    tampered = copy.deepcopy(result.gate)
    tampered["next_authorization"]["model_training_allowed"] = True

    with pytest.raises(O1EventAttemptFreezeError) as exc:
        validate_o1_event_attempt_freeze_gate(tampered)

    assert exc.value.code == "O1_EVENT_GATE_ID_MISMATCH"


def test_duplicate_event_ids_fail_closed() -> None:
    row = {"event_id": "CPI_same", "event_family": "CPI"}
    with pytest.raises(O1EventAttemptFreezeError) as exc:
        ledger_module._validate_event_rows(
            [
                row,
                row,
                {"event_id": "NFP_one", "event_family": "NFP"},
                {"event_id": "FOMC_one", "event_family": "FOMC"},
            ],
            blockers=[],
        )
    assert exc.value.code == "O1_EVENT_DUPLICATE_ID"


def test_bls_parser_accepts_bounded_usdl_identifier_and_checks_body_date() -> None:
    timestamp = ledger_module._parse_bls_release_timestamp(
        "https://www.bls.gov/news.release/archives/empsit_01052024.htm",
        (
            b"Transmission of material in this news release is embargoed until "
            b"USDL-24-0006 8:30 a.m. (ET) Friday, January 5, 2024"
        ),
    )
    assert timestamp.astimezone(UTC).isoformat() == "2024-01-05T13:30:00+00:00"

    with pytest.raises(O1EventAttemptFreezeError) as exc:
        ledger_module._parse_bls_release_timestamp(
            "https://www.bls.gov/news.release/archives/empsit_01052024.htm",
            (
                b"Transmission of material in this news release is embargoed until "
                b"USDL-24-0006 8:30 a.m. (ET) Monday, January 8, 2024"
            ),
        )
    assert exc.value.code == "O1_EVENT_BLS_RELEASE_DATE_MISMATCH"


def test_blocked_parser_acquisition_replays_without_refetch_or_overwrite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root, policy_path, output_root = _project_fixture(tmp_path)
    original_parser = ledger_module._parse_bls_release_timestamp

    def reject_nfp_for_initial_freeze(url: str, body: bytes) -> datetime:
        if "/empsit_" in url:
            raise O1EventAttemptFreezeError("O1_EVENT_BLS_TIMESTAMP_MISSING", url)
        return original_parser(url, body)

    monkeypatch.setattr(
        ledger_module,
        "_parse_bls_release_timestamp",
        reject_nfp_for_initial_freeze,
    )
    initial = freeze_o1_event_and_attempt_ledgers(
        output_root=output_root,
        project_root=project_root,
        generated_at=GENERATED_AT,
        audit_policy_path=policy_path.relative_to(project_root),
        fetcher=_fixture_fetcher(),
        source_commit_sha="a" * 40,
    )
    monkeypatch.setattr(
        ledger_module,
        "_parse_bls_release_timestamp",
        original_parser,
    )
    assert initial.status == BLOCKED_STATUS
    immutable_before = {
        path.name: path.read_bytes()
        for path in (
            initial.source_manifest_path,
            initial.attempt_ledger_path,
            initial.gate_path,
        )
    }

    replay = replay_o1_event_and_attempt_ledgers_from_retained_sources(
        output_root=output_root,
        project_root=project_root,
        generated_at=GENERATED_AT + timedelta(minutes=1),
        audit_policy_path=policy_path.relative_to(project_root),
        initial_source_manifest_sha256=ledger_module.sha256_path(
            initial.source_manifest_path
        ),
        initial_attempt_ledger_sha256=ledger_module.sha256_path(
            initial.attempt_ledger_path
        ),
        initial_gate_sha256=ledger_module.sha256_path(initial.gate_path),
        source_commit_sha="b" * 40,
    )

    assert replay.status == PASS_STATUS
    assert replay.source_manifest_path.name == "event_source_manifest_replay_v1.json"
    assert replay.gate_path.name == "event_attempt_freeze_gate_replay_v1.json"
    assert replay.attempt_ledger_path == initial.attempt_ledger_path
    assert replay.event_ledger_path is not None
    replay_events = json.loads(replay.event_ledger_path.read_text(encoding="utf-8"))
    assert len(replay_events["events"]) == 3
    assert replay.gate["replay_provenance"]["network_accessed"] is False
    assert replay.gate["next_authorization"]["coverage_only_gate_allowed"] is True
    for path in (
        initial.source_manifest_path,
        initial.attempt_ledger_path,
        initial.gate_path,
    ):
        assert path.read_bytes() == immutable_before[path.name]


def test_retained_replay_rejects_raw_byte_tamper_before_writing_outputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root, policy_path, output_root = _project_fixture(tmp_path)
    original_parser = ledger_module._parse_bls_release_timestamp

    def reject_nfp_for_initial_freeze(url: str, body: bytes) -> datetime:
        if "/empsit_" in url:
            raise O1EventAttemptFreezeError("O1_EVENT_BLS_TIMESTAMP_MISSING", url)
        return original_parser(url, body)

    monkeypatch.setattr(
        ledger_module,
        "_parse_bls_release_timestamp",
        reject_nfp_for_initial_freeze,
    )
    initial = freeze_o1_event_and_attempt_ledgers(
        output_root=output_root,
        project_root=project_root,
        generated_at=GENERATED_AT,
        audit_policy_path=policy_path.relative_to(project_root),
        fetcher=_fixture_fetcher(),
        source_commit_sha="a" * 40,
    )
    monkeypatch.setattr(
        ledger_module,
        "_parse_bls_release_timestamp",
        original_parser,
    )
    manifest = json.loads(initial.source_manifest_path.read_text(encoding="utf-8"))
    nfp_release = next(
        item
        for item in manifest["requests"]
        if item["event_family"] == "NFP" and item["source_role"] == "RELEASE"
    )
    raw_path = output_root / nfp_release["artifact_path"]
    raw_path.write_bytes(raw_path.read_bytes() + b"TAMPER")

    with pytest.raises(O1EventAttemptFreezeError) as exc:
        replay_o1_event_and_attempt_ledgers_from_retained_sources(
            output_root=output_root,
            project_root=project_root,
            generated_at=GENERATED_AT + timedelta(minutes=1),
            audit_policy_path=policy_path.relative_to(project_root),
            initial_source_manifest_sha256=ledger_module.sha256_path(
                initial.source_manifest_path
            ),
            initial_attempt_ledger_sha256=ledger_module.sha256_path(
                initial.attempt_ledger_path
            ),
            initial_gate_sha256=ledger_module.sha256_path(initial.gate_path),
            source_commit_sha="b" * 40,
        )

    assert exc.value.code == "O1_EVENT_REPLAY_RAW_ARTIFACT_TAMPERED"
    assert not (output_root / "event_source_manifest_replay_v1.json").exists()
    assert not (output_root / "event_attempt_freeze_gate_replay_v1.json").exists()
    assert not (output_root / "event_ledger.json").exists()


def _project_fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    project_root = tmp_path / "project"
    project_root.mkdir(parents=True)
    policy = yaml.safe_load(POLICY_PATH.read_text(encoding="utf-8"))
    policy["execution_binding"]["real_coverage_read_allowed_now"] = False
    policy_path = project_root / "config/research/o1_policy.yaml"
    policy_path.parent.mkdir(parents=True)
    (project_root / "pyproject.toml").write_text(
        "[project]\nname = \"o1-event-fixture\"\nversion = \"0.0.0\"\n",
        encoding="utf-8",
    )
    dq_relative = Path("outputs/validation_runtime/candidate/o1_dq_gate.json")
    dq_path = project_root / dq_relative
    dq_path.parent.mkdir(parents=True)
    source_dq_path = PROJECT_ROOT / policy["isolated_dq_evidence"]["gate"]["path"]
    dq_path.write_bytes(source_dq_path.read_bytes())
    dq_gate = json.loads(dq_path.read_text(encoding="utf-8"))
    policy["isolated_dq_evidence"]["output_root"] = dq_path.parent.as_posix()
    policy["isolated_dq_evidence"]["gate"] = {
        "gate_id": dq_gate["gate_id"],
        "path": dq_relative.as_posix(),
        "sha256": ledger_module.sha256_path(dq_path),
    }
    policy_path.write_text(yaml.safe_dump(policy, sort_keys=False), encoding="utf-8")
    return project_root, policy_path, dq_path.parent / "o1_event_attempt_freeze_v1"


def _fixture_fetcher(
    responses: dict[str, bytes] | None = None,
):
    payloads = _fixture_responses() if responses is None else responses

    def fetch(url: str) -> SourceFetch:
        body = payloads[url]
        return SourceFetch(
            requested_url=url,
            final_url=url,
            downloaded_at=GENERATED_AT,
            status_code=200,
            content_type="text/html",
            body=body,
        )

    return fetch


def _fixture_responses() -> dict[str, bytes]:
    responses = {
        "https://www.bls.gov/bls/news-release/cpi.htm": (
            b'<a href="/news.release/archives/cpi_03102021.htm">February 2021 CPI</a>'
        ),
        "https://www.bls.gov/bls/news-release/empsit.htm": (
            b'<a href="/news.release/archives/empsit_03052021.htm">'
            b"February 2021 Employment Situation</a>"
        ),
        "https://www.bls.gov/news.release/archives/cpi_03102021.htm": (
            b"<p>Transmission of material in this release is embargoed until "
            b"8:30 a.m. (ET) Wednesday, March 10, 2021.</p>"
        ),
        "https://www.bls.gov/news.release/archives/empsit_03052021.htm": (
            b"<p>Transmission of material in this release is embargoed until "
            b"USDL-21-0306 8:30 a.m. (ET) Friday, March 5, 2021.</p>"
        ),
        "https://www.federalreserve.gov/newsevents/pressreleases/monetary20210317a.htm": (
            b"<p>For release at 2:00 p.m. EDT</p>"
        ),
    }
    for year in range(2021, 2027):
        responses[
            f"https://www.federalreserve.gov/newsevents/pressreleases/{year}-press-fomc.htm"
        ] = _fed_index_html() if year == 2021 else b"<html><body>No in-window fixture</body></html>"
    return responses


def _fed_index_html() -> bytes:
    return (
        b'<a href="/newsevents/pressreleases/monetary20210317a.htm">'
        b"Federal Reserve issues FOMC statement</a>"
    )
