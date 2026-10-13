"""Research bench (TRADING-2565, TRADING-2567 S3): one simulator, one cost, fixed benchmarks.

A strategy is a function ``(inputs, params) -> pd.Series`` that gives, for every trading date, the
target exposure to the Nasdaq-100 (0 to 3) it would hold given what was known at that date's close.
Everything that past research lines re-implemented, and often got wrong, lives here instead:
execution timing, holdings that drift between trades, cost, the benchmarks and the metrics. Every
run first checks that no target uses later data and that every declared parameter is read.

Data comes only from checked long-history snapshots (``research_data.qqq_long_history``). Research
runs go through ``ledger.run_and_record``, which records every run (failed ones too) under
``research/<question>/``, counts trials against the preregistered cap, and opens the validation
window once per question, only for an approved preregistration (S3b). Validation and consistency
runs always face the standard falsification (``falsify``): cost and delay reruns, leave-one-out,
same-exposure random timing and a block bootstrap; ``report`` shows the first screen (S3c).
"""

from ai_trading_system.research_bench.checks import Strategy
from ai_trading_system.research_bench.data import BenchData, from_daily_frame, from_snapshot
from ai_trading_system.research_bench.ledger import (
    LedgerError,
    Question,
    TrialCapError,
    load_question,
    register_read,
    run_and_record,
)
from ai_trading_system.research_bench.run import (
    BenchCheckError,
    RunResult,
    RunSpec,
    WindowNotOpenError,
    WindowPermit,
    benchmark_summaries,
    run_strategy,
)
from ai_trading_system.research_bench.settings import load_settings, resolve_window
from ai_trading_system.research_bench.simulate import Rebalance, simulate

__all__ = [
    "BenchCheckError",
    "BenchData",
    "LedgerError",
    "Question",
    "Rebalance",
    "RunResult",
    "RunSpec",
    "Strategy",
    "TrialCapError",
    "WindowNotOpenError",
    "WindowPermit",
    "benchmark_summaries",
    "from_daily_frame",
    "from_snapshot",
    "load_question",
    "load_settings",
    "register_read",
    "resolve_window",
    "run_and_record",
    "run_strategy",
    "simulate",
]
