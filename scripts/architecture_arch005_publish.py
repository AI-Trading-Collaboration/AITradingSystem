#!/usr/bin/env python3
"""One-command ordinary publication run (DEVX-016 S3): plan | run | resume | status.

Orchestrates the existing reviewed fence, generator, validation and publication commands in the
manual flow's order, keeps a hash-chained journal under outputs/architecture/publication_runs/, and
stops at the owner authorization gate. See docs/operations/operations_runbook.md and
docs/requirements/DEVX-016_Single_Agent_Governance_Simplification.md section 10.6.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from ai_trading_system.platform.architecture.parallel_replay_scope import (
    ROLE_S3_COMMAND,
    apply_switch,
)
from ai_trading_system.platform.architecture.publication_cli import default_environment, main

ROOT = Path(__file__).resolve().parents[1]

if __name__ == "__main__":
    wired = default_environment(ROOT)
    # DEVX-023 P6: this process's own lease replays follow the reviewed scope as well (the
    # children already got it through the environment default_environment built for them).
    apply_switch(wired.parallel_replay, ROLE_S3_COMMAND, os.environ)
    sys.exit(main(environment=wired))
