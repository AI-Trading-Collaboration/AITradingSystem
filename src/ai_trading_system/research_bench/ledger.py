"""Trial ledger and window reads (TRADING-2565 S3b): every research run is recorded, failed ones
too.

One question lives in ``research/<question_id>/``:

- ``prereg.yaml`` (zone C) and the strategy module it names;
- ``runs/0001.json``, ``0002.json``, ...: one record per run. Each carries the sha256 of the record
  before it, so a changed or removed record breaks the chain;
- ``window_reads.jsonl``: one line per registered read of the validation or consistency window,
  chained the same way.

Records hold summaries only, never daily series: the long-history data stays on the machine.

Rules:

- Development is open. Every distinct configuration is a trial: strategy, the content of its
  module, parameters, rebalance, delay, cost and QQQ series. A repeat is recorded but is not a new
  trial, and a new trial beyond the preregistered cap is refused before it runs.
- Validation is read once per question, and only when ``prereg.yaml`` and the strategy module on
  disk are the versions on main and main's ``docs/requirements`` carry the prereg's owner decision.
  The read is registered before anything runs; it permits one run of each validated strategy, with
  the preregistered parameters.
- The consistency window opens only after the validation read, under the same approval.
- Validation and consistency runs always face the standard falsification (S3c), and their rules
  are judged on the main result and the falsification together. A read is refused while a rule
  names a metric no run has. Development runs are falsified on request; that is not a new trial.

Trial counts and the deflated Sharpe ratio use the development trials of every question.

A record cannot stop someone from deleting a file before it is committed. The chain makes a change
visible once records are committed, and the invariant suite fails if git history shows a record or
read that was ever deleted or changed.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.core.provenance import git_output, git_provenance
from ai_trading_system.paths import join_root
from ai_trading_system.research_bench.checks import Strategy
from ai_trading_system.research_bench.data import BenchData
from ai_trading_system.research_bench.deflated import (
    ReturnMoments,
    deflated_sharpe,
    sample_variance,
)
from ai_trading_system.research_bench.prereg import (
    PREREG_FILE,
    Prereg,
    evaluate_rules,
    load_prereg,
    rule_problems,
)
from ai_trading_system.research_bench.run import (
    BenchCheckError,
    RunResult,
    RunSpec,
    WindowPermit,
    run_strategy,
    spec_record,
)
from ai_trading_system.research_bench.settings import (
    LoadedSettings,
    WindowRole,
    load_settings,
    resolve_window,
)

RUNS_DIR = "runs"
READS_FILE = "window_reads.jsonl"
RUN_SCHEMA = "research_run.v1"
READ_SCHEMA = "research_window_read.v1"
RECORD_NAME = re.compile(r"^([0-9]{4,})\.json$")
ERROR_TEXT_LIMIT = 300


class LedgerError(RuntimeError):
    """The ledger refuses: a broken chain, a window that may not be read, or a run it cannot
    record."""


class TrialCapError(LedgerError):
    """A new development trial would exceed the preregistered cap."""


@dataclass(frozen=True)
class Question:
    directory: Path
    prereg: Prereg
    prereg_path: Path
    prereg_sha256: str
    strategy_path: Path
    strategy_sha256: str

    @property
    def question_id(self) -> str:
        return self.prereg.question_id


@dataclass(frozen=True)
class RecordedRun:
    record: dict[str, Any]
    result: RunResult | None


def research_root(loaded: LoadedSettings) -> Path:
    return join_root(loaded.root, loaded.settings.research_root)


def load_question(directory: Path) -> Question:
    prereg_path = directory.joinpath(PREREG_FILE)
    prereg = load_prereg(prereg_path)
    if prereg.question_id != directory.name:
        raise ValueError(f"{prereg_path} names question {prereg.question_id}, not {directory.name}")
    strategy_path = directory.joinpath(prereg.strategy_module)
    if not strategy_path.is_file():
        raise FileNotFoundError(f"strategy module {strategy_path} does not exist")
    return Question(
        directory=directory,
        prereg=prereg,
        prereg_path=prereg_path,
        prereg_sha256=_file_sha256(prereg_path),
        strategy_path=strategy_path,
        strategy_sha256=_file_sha256(strategy_path),
    )


def load_strategy(question: Question, strategy_id: str) -> Strategy:
    entry = question.prereg.strategy(strategy_id)
    name = "aits_research_" + re.sub(r"[^0-9a-z]+", "_", question.question_id.lower())
    spec = importlib.util.spec_from_file_location(
        f"{name}_{question.strategy_sha256[:12]}", question.strategy_path
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {question.strategy_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    function = getattr(module, entry.function, None)
    if not callable(function):
        raise AttributeError(f"{question.strategy_path.name} has no function {entry.function}")
    return function  # type: ignore[no-any-return]


# Chains ---------------------------------------------------------------------------------------


def chain_digest(data: bytes) -> str:
    """sha256 of a record's bytes, CRLF read as LF so a checkout's line endings do not matter."""

    return hashlib.sha256(data.replace(b"\r\n", b"\n")).hexdigest()


def run_records(directory: Path) -> list[dict[str, Any]]:
    """The question's run records in order; raises if the chain is broken."""

    problems = verify_runs(directory)
    if problems:
        raise LedgerError(f"run records of {directory.name} are broken: {'; '.join(problems)}")
    return [json.loads(path.read_bytes()) for path in _record_paths(directory)]


def verify_runs(directory: Path) -> list[str]:
    runs = directory.joinpath(RUNS_DIR)
    if not runs.exists():
        return []
    problems = [
        f"{RUNS_DIR}/{path.name} is not a run record"
        for path in sorted(runs.iterdir())
        if not (path.is_file() and RECORD_NAME.match(path.name))
    ]
    previous: str | None = None
    for position, path in enumerate(_record_paths(directory), start=1):
        name = f"{RUNS_DIR}/{path.name}"
        data = path.read_bytes()
        try:
            record = json.loads(data)
        except ValueError:
            problems.append(f"{name} is not JSON")
            previous = chain_digest(data)
            continue
        if _record_number(path) != position:
            problems.append(f"{name}: expected record {position:04d}; a record is missing")
        if not isinstance(record, dict) or record.get("schema_version") != RUN_SCHEMA:
            problems.append(f"{name}: schema_version is not {RUN_SCHEMA}")
        elif record.get("seq") != _record_number(path):
            problems.append(f"{name}: seq {record.get('seq')} does not match the file name")
        elif record.get("question_id") != directory.name:
            problems.append(f"{name}: question_id is not {directory.name}")
        elif record.get("prev_sha256") != previous:
            problems.append(f"{name}: prev_sha256 does not match the record before it")
        previous = chain_digest(data)
    return problems


def window_reads(directory: Path) -> list[dict[str, Any]]:
    problems = verify_reads(directory)
    if problems:
        raise LedgerError(f"window reads of {directory.name} are broken: {'; '.join(problems)}")
    path = directory.joinpath(READS_FILE)
    if not path.exists():
        return []
    return [json.loads(line) for line in _read_lines(path)]


def verify_reads(directory: Path) -> list[str]:
    path = directory.joinpath(READS_FILE)
    if not path.exists():
        return []
    problems: list[str] = []
    previous: str | None = None
    for position, line in enumerate(_read_lines(path), start=1):
        try:
            entry = json.loads(line)
        except ValueError:
            problems.append(f"{READS_FILE} line {position} is not JSON")
            previous = chain_digest(line)
            continue
        if not isinstance(entry, dict) or entry.get("schema_version") != READ_SCHEMA:
            problems.append(f"{READS_FILE} line {position}: schema_version is not {READ_SCHEMA}")
        elif entry.get("seq") != position:
            problems.append(f"{READS_FILE} line {position}: seq is {entry.get('seq')}")
        elif entry.get("question_id") != directory.name:
            problems.append(f"{READS_FILE} line {position}: question_id is not {directory.name}")
        elif entry.get("prev_sha256") != previous:
            problems.append(f"{READS_FILE} line {position}: prev_sha256 does not match")
        previous = chain_digest(line)
    return problems


# Trials ---------------------------------------------------------------------------------------


def trial_key(
    spec: Mapping[str, Any], cost_bps: float, strategy_sha256: str, qqq_series: str
) -> str:
    """One development trial: everything that changes what is tested, but not the window."""

    payload = {
        "spec": {key: value for key, value in spec.items() if key != "window_role"},
        "cost_bps": float(cost_bps),
        "strategy_sha256": strategy_sha256,
        "qqq_series": qqq_series,
    }
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def development_trials(records: list[dict[str, Any]]) -> dict[str, int]:
    """Trial key to trial number, in the order the trials were first run."""

    trials: dict[str, int] = {}
    for record in records:
        trial = record.get("trial")
        if isinstance(trial, dict) and trial.get("key") not in trials:
            trials[str(trial["key"])] = len(trials) + 1
    return trials


# Approval and window reads --------------------------------------------------------------------


def approval_problems(question: Question, ref: str) -> list[str]:
    """Why the validation window may not be read yet; empty when the prereg is approved."""

    problems: list[str] = []
    decision = question.prereg.owner_decision
    if not decision:
        problems.append("prereg.yaml names no owner_decision")
    toplevel = git_output(question.directory, "rev-parse", "--show-toplevel")
    if toplevel is None:
        return [*problems, f"{question.directory} is not inside a git repository"]
    top = Path(toplevel).resolve()
    for path in (question.prereg_path, question.strategy_path):
        rel = path.resolve().relative_to(top).as_posix()
        on_ref = git_output(top, "rev-parse", "--verify", "--quiet", f"{ref}:{rel}")
        here = git_output(top, "hash-object", "--", rel)
        if on_ref is None:
            problems.append(f"{rel} is not on {ref}")
        elif on_ref != here:
            problems.append(f"{rel} differs from the version on {ref}")
    if decision and not git_output(
        top, "grep", "-F", "-l", "-e", decision, ref, "--", "docs/requirements"
    ):
        problems.append(f"{decision} is not in docs/requirements on {ref}")
    return problems


def register_read(
    question: Question, role: WindowRole, loaded: LoadedSettings | None = None
) -> WindowPermit:
    """Record a read of the validation or consistency window before it happens, and permit it."""

    loaded = loaded or load_settings()
    if role == "development":
        raise ValueError("the development window needs no registered read")
    ref = loaded.settings.approval_ref
    problems = rule_problems(question.prereg, loaded.settings)
    if problems:
        raise LedgerError(
            f"{question.question_id} has rules no run can meet: {'; '.join(problems)}"
        )
    problems = approval_problems(question, ref)
    if problems:
        raise LedgerError(f"{question.question_id} is not approved: {'; '.join(problems)}")
    entries = window_reads(question.directory)
    validation = [entry for entry in entries if entry.get("role") == "validation"]
    if role == "validation" and validation:
        raise LedgerError(
            f"the validation window of {question.question_id} was read in "
            f"{validation[0]['read_id']}; it is read once per question"
        )
    if role == "consistency_check" and not validation:
        raise LedgerError("the consistency window opens only after the validation read")
    window = resolve_window(loaded, question.prereg.question_type, role)
    seq = len(entries) + 1
    read_id = f"{question.question_id}-w{seq:04d}"
    strategies = tuple(question.prereg.validation.strategies)
    path = question.directory.joinpath(READS_FILE)
    lines = _read_lines(path) if path.exists() else []
    entry = {
        "schema_version": READ_SCHEMA,
        "question_id": question.question_id,
        "seq": seq,
        "read_id": read_id,
        "role": role,
        "window_id": window.window_id,
        "strategies": list(strategies),
        "prereg_sha256": question.prereg_sha256,
        "strategy_module_sha256": question.strategy_sha256,
        "owner_decision": question.prereg.owner_decision,
        "approval_ref": ref,
        "approval_commit": git_output(question.directory, "rev-parse", ref),
        "recorded_at": _now(),
        "prev_sha256": chain_digest(lines[-1]) if lines else None,
    }
    text = json.dumps(entry, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    with path.open("ab") as handle:
        handle.write(text.encode("utf-8") + b"\n")
    return WindowPermit(question.question_id, role, read_id, strategies)


# Runs -----------------------------------------------------------------------------------------


def run_and_record(
    question: Question,
    data: BenchData,
    strategy_id: str,
    *,
    role: WindowRole = "development",
    params: Mapping[str, Any] | None = None,
    permit: WindowPermit | None = None,
    loaded: LoadedSettings | None = None,
    falsify: bool = False,
) -> RecordedRun:
    """Run one preregistered strategy and record the run, whatever its outcome.

    Development runs may override parameters (each distinct configuration is a trial) and are
    falsified when ``falsify`` is set. Validation and consistency runs use the preregistered
    parameters under a registered read and are always falsified. A run that fails its checks or
    raises is recorded, then the error is raised again.
    """

    loaded = loaded or load_settings()
    prereg = question.prereg
    entry = prereg.strategy(strategy_id)
    _check_data(question, data)
    falsify = falsify or role != "development"
    confidence = prereg.validation.confidence or loaded.settings.falsification.confidence
    if role == "development":
        merged = {**entry.params, **dict(params or {})}
        read_id = None
    else:
        if params:
            raise LedgerError(f"{role} runs use the preregistered parameters only")
        read_id = _check_permit(question, strategy_id, role, permit)
        merged = dict(entry.params)
    spec = RunSpec(
        question_type=prereg.question_type,
        strategy_id=strategy_id,
        params=merged,
        window_role=role,
        rebalance=entry.rebalance.to_rebalance(),
        delay_days=entry.delay_days,
        leveraged_benchmarks=entry.leveraged_benchmarks,
    )
    specified = spec_record(spec)
    records = run_records(question.directory)
    trial = None
    if role == "development":
        key = trial_key(
            specified, loaded.cost_bps, question.strategy_sha256, prereg.data.qqq_series
        )
        trials = development_trials(records)
        if key not in trials and len(trials) >= prereg.trial_cap:
            raise TrialCapError(
                f"{question.question_id} has used its {prereg.trial_cap} trials; a new "
                "configuration needs a new preregistration"
            )
        trial = {"key": key, "number": trials.get(key, len(trials) + 1), "new": key not in trials}

    window = resolve_window(loaded, prereg.question_type, role)
    body: dict[str, Any] = {
        "strategy": {
            "id": strategy_id,
            "function": entry.function,
            "module": question.strategy_path.name,
            "module_sha256": question.strategy_sha256,
        },
        "spec": specified,
        "cost_bps": loaded.cost_bps,
        "falsification_confidence": confidence if falsify else None,
        "trial": trial,
        "window_read_id": read_id,
        "window": {
            "window_id": window.window_id,
            "role": role,
            "start": window.start.isoformat(),
            "end": None if window.end is None else window.end.isoformat(),
        },
        "source": dict(data.source),
        "prereg_sha256": question.prereg_sha256,
        "settings_sha256": loaded.sha256,
        "provenance": git_provenance(question.directory),
    }
    try:
        strategy = load_strategy(question, strategy_id)
        result = run_strategy(
            data,
            strategy,
            spec,
            loaded,
            permit=permit,
            falsification_confidence=confidence if falsify else None,
        )
    except BenchCheckError as exc:
        _append_run(question, {**body, "status": "check_failed", "problems": exc.problems})
        raise
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}".splitlines()[0][:ERROR_TEXT_LIMIT]
        _append_run(question, {**body, "status": "error", "error": error})
        raise
    outcome = result.to_dict()
    falsification = outcome.get("falsification") or {}
    rules = None
    if role != "development" and strategy_id in prereg.validation.rules:
        rules = evaluate_rules(prereg.validation.rules[strategy_id], {**falsification, **outcome})
    record = _append_run(
        question,
        {
            **body,
            "status": "ok",
            "result": {
                key: outcome[key]
                for key in (
                    "strategy",
                    "benchmarks",
                    "comparison",
                    "exposure_gap",
                    "checks",
                    "fingerprint",
                    "falsification",
                    "events",
                )
            },
            "rules": rules,
            "sample_floor_met": int(result.strategy["return_days"])
            >= prereg.sample_floor.min_return_days,
        },
    )
    return RecordedRun(record=record, result=result)


# Summary --------------------------------------------------------------------------------------


def question_directories(root_dir: Path) -> list[Path]:
    """Directories with a prereg.yaml; a name starting with ``_`` (the template) is no question."""

    if not root_dir.is_dir():
        return []
    found = [path.parent for path in root_dir.glob(f"*/{PREREG_FILE}")]
    return sorted(path for path in found if not path.name.startswith("_"))


def ledger_summary(root_dir: Path) -> dict[str, Any]:
    """Trial counts per question and overall, chain status, and the deflated Sharpe ratio of every
    successful run against all development trials so far."""

    questions: dict[str, dict[str, Any]] = {}
    trial_sharpes: list[float] = []
    for directory in question_directories(root_dir):
        problems = verify_runs(directory) + verify_reads(directory)
        records = (
            [json.loads(path.read_bytes()) for path in _record_paths(directory)]
            if not problems
            else []
        )
        reads = window_reads(directory) if not problems else []
        trials = development_trials(records)
        latest: dict[str, float] = {}
        for record in records:
            trial = record.get("trial")
            sharpe = _strategy_field(record, "sharpe_daily")
            if isinstance(trial, dict) and sharpe is not None:
                latest[str(trial["key"])] = sharpe
        trial_sharpes.extend(latest.values())
        try:
            cap: int | None = load_prereg(directory.joinpath(PREREG_FILE)).trial_cap
        except (ValueError, OSError):
            cap = None
        questions[directory.name] = {
            "problems": problems,
            "records": records,
            "trials": len(trials),
            "trial_cap": cap,
            "reads": [(read["read_id"], read["role"]) for read in reads],
        }
    total = sum(item["trials"] for item in questions.values())
    variance = sample_variance(trial_sharpes)
    for item in questions.values():
        item["runs"] = [_run_line(record, total, variance) for record in item.pop("records")]
    return {
        "questions": questions,
        "global_trials": total,
        "trial_sharpe_variance": variance,
    }


def _run_line(record: dict[str, Any], trials: int, variance: float | None) -> dict[str, Any]:
    line: dict[str, Any] = {
        "run_id": record.get("run_id"),
        "strategy": record.get("strategy", {}).get("id"),
        "role": record.get("window", {}).get("role"),
        "status": record.get("status"),
        "trial": (record.get("trial") or {}).get("number"),
        "sharpe_annual": _strategy_field(record, "sharpe_vs_cash"),
        "deflated_sharpe": None,
    }
    sharpe = _strategy_field(record, "sharpe_daily")
    skew = _strategy_field(record, "excess_skewness")
    kurt = _strategy_field(record, "excess_kurtosis")
    days = _strategy_field(record, "return_days")
    if None in (sharpe, skew, kurt, days) or (trials > 1 and variance is None):
        return line
    moments = ReturnMoments(float(sharpe), float(skew), float(kurt), int(days))  # type: ignore[arg-type]
    try:
        line["deflated_sharpe"] = deflated_sharpe(moments, max(trials, 1), variance or 0.0)
    except ValueError:
        pass
    return line


def _strategy_field(record: dict[str, Any], name: str) -> float | None:
    result = record.get("result")
    if not isinstance(result, dict):
        return None
    value = result.get("strategy", {}).get(name)
    if isinstance(value, int | float) and math.isfinite(value):
        return float(value)
    return None


# Helpers --------------------------------------------------------------------------------------


def _check_data(question: Question, data: BenchData) -> None:
    wanted = question.prereg.data
    source = data.source
    if source.get("snapshot_id") != wanted.snapshot_id:
        raise LedgerError(
            f"{question.question_id} is preregistered on snapshot {wanted.snapshot_id}, "
            f"not {source.get('snapshot_id')}"
        )
    if source.get("qqq_series") != wanted.qqq_series:
        raise LedgerError(
            f"{question.question_id} is preregistered on the {wanted.qqq_series} QQQ series"
        )


def _check_permit(
    question: Question, strategy_id: str, role: WindowRole, permit: WindowPermit | None
) -> str:
    if permit is None or permit.question_id != question.question_id or permit.role != role:
        raise LedgerError(f"a {role} run needs the permit of a read registered for this question")
    if strategy_id not in permit.strategies:
        raise LedgerError(f"read {permit.read_id} does not cover strategy {strategy_id}")
    registered = {
        entry["read_id"]: entry
        for entry in window_reads(question.directory)
        if entry.get("role") == role
    }
    entry = registered.get(permit.read_id)
    if entry is None:
        raise LedgerError(f"read {permit.read_id} is not registered")
    if (
        entry.get("prereg_sha256") != question.prereg_sha256
        or entry.get("strategy_module_sha256") != question.strategy_sha256
    ):
        raise LedgerError(f"prereg or strategy module changed after read {permit.read_id}")
    for record in run_records(question.directory):
        if (
            record.get("window_read_id") == permit.read_id
            and record.get("strategy", {}).get("id") == strategy_id
        ):
            raise LedgerError(f"read {permit.read_id} already ran strategy {strategy_id}")
    return permit.read_id


def _append_run(question: Question, body: dict[str, Any]) -> dict[str, Any]:
    paths = _record_paths(question.directory)
    problems = verify_runs(question.directory)
    if problems:
        raise LedgerError(f"run records are broken: {'; '.join(problems)}")
    seq = len(paths) + 1
    record = {
        "schema_version": RUN_SCHEMA,
        "question_id": question.question_id,
        "seq": seq,
        "run_id": f"{question.question_id}-r{seq:04d}",
        "recorded_at": _now(),
        "prev_sha256": chain_digest(paths[-1].read_bytes()) if paths else None,
        **body,
    }
    runs = question.directory.joinpath(RUNS_DIR)
    runs.mkdir(parents=True, exist_ok=True)
    text = json.dumps(_plain(record), ensure_ascii=False, indent=1, sort_keys=True, allow_nan=False)
    with runs.joinpath(f"{seq:04d}.json").open("xb") as handle:
        handle.write(text.encode("utf-8") + b"\n")
    return record


def _record_paths(directory: Path) -> list[Path]:
    runs = directory.joinpath(RUNS_DIR)
    if not runs.exists():
        return []
    found = [path for path in runs.iterdir() if path.is_file() and RECORD_NAME.match(path.name)]
    return sorted(found, key=_record_number)


def _record_number(path: Path) -> int:
    match = RECORD_NAME.match(path.name)
    return int(match.group(1)) if match else -1


def _read_lines(path: Path) -> list[bytes]:
    return [line for line in path.read_bytes().replace(b"\r\n", b"\n").split(b"\n") if line]


def _plain(value: Any) -> Any:
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_plain(item) for item in value]
    return value


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


# Admission and history ------------------------------------------------------------------------

DECISION_FILE = "decision.md"


def admission_problems(directory: Path) -> list[str]:
    """A question's decision may cite only recorded runs and registered reads."""

    path = directory.joinpath(DECISION_FILE)
    if not path.exists():
        return []
    text = path.read_text(encoding="utf-8")
    question_id = re.escape(directory.name)
    cited_runs = set(re.findall(rf"\b{question_id}-r[0-9]{{4,}}\b", text))
    cited_reads = set(re.findall(rf"\b{question_id}-w[0-9]{{4,}}\b", text))
    problems = verify_runs(directory) + verify_reads(directory)
    if problems:
        return problems
    runs = {record.get("run_id") for record in run_records(directory)}
    reads = {entry.get("read_id") for entry in window_reads(directory)}
    problems.extend(
        f"{DECISION_FILE} cites {run_id}, which is not a recorded run"
        for run_id in sorted(cited_runs - runs)
    )
    problems.extend(
        f"{DECISION_FILE} cites {read_id}, which is not a registered read"
        for read_id in sorted(cited_reads - reads)
    )
    if not cited_runs:
        problems.append(f"{DECISION_FILE} cites no recorded run")
    return problems


def history_problems(toplevel: Path, research_dir: str = "research") -> list[str]:
    """Run records are only ever added and window reads only ever appended, in git history."""

    changed = git_output(
        toplevel,
        "log",
        "--diff-filter=DMR",
        "--name-only",
        "--format=",
        "--",
        f"{research_dir}/*/{RUNS_DIR}/*.json",
    )
    problems = [
        f"git history changes or removes the run record {name}"
        for name in sorted(set((changed or "").split()))
    ]
    deleted = git_output(
        toplevel,
        "log",
        "--diff-filter=DR",
        "--name-only",
        "--format=",
        "--",
        f"{research_dir}/*/{READS_FILE}",
    )
    problems.extend(
        f"git history removes the window reads {name}"
        for name in sorted(set((deleted or "").split()))
    )
    touched = git_output(
        toplevel, "log", "--name-only", "--format=", "--", f"{research_dir}/*/{READS_FILE}"
    )
    for name in sorted(set((touched or "").split())):
        commits = git_output(toplevel, "log", "--format=%H", "--", name) or ""
        for commit in commits.split():
            before = git_output(toplevel, "show", f"{commit}^:{name}")
            after = git_output(toplevel, "show", f"{commit}:{name}")
            if before is not None and (after is None or not after.startswith(before)):
                problems.append(f"commit {commit[:12]} rewrites earlier lines of {name}")
    return problems
