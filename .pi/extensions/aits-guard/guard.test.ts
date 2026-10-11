// Run: node --test .pi/extensions/aits-guard/guard.test.ts  (Node >= 23.6 strips the types)
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { test } from "node:test";

import guard, {
  buildReport,
  checkCommand,
  checkWrite,
  commandVerdict,
  detectUnconfirmedCChanges,
  findRepoRoot,
  initializeCZoneBaseline,
  makeState,
  truncateToolResult,
  writeReport,
  writeVerdict,
} from "./index.ts";

function fakeRepo(initGit = false): string {
  const root = mkdtempSync(join(tmpdir(), "aits-guard-"));
  mkdirSync(join(root, "config"));
  mkdirSync(join(root, "src", "pkg"), { recursive: true });
  mkdirSync(join(root, "docs", "requirements"), { recursive: true });
  mkdirSync(join(root, "docs", "research"), { recursive: true });
  writeFileSync(
    join(root, "config", "gov008_ship.yaml"),
    [
      "schema_version: 1",
      "tree_clean_exclusions:",
      "  - docs/research/private_pack.md",
      "",
      "zones:",
      "  # Zone C: semantic-critical.",
      "  C:",
      "    - config/data_quality.yaml",
      "    - src/ai_trading_system/scoring/**",
      "    - AGENTS.md",
      "  # Zone A: records.",
      "  A:",
      "    - docs/**",
      '    - "*.md"',
      "",
      "gates:",
      "  A: []",
    ].join("\n"),
  );
  if (initGit) {
    execFileSync("git", ["init", "-b", "main"], { cwd: root });
    execFileSync("git", ["config", "user.email", "pi@example.invalid"], { cwd: root });
    execFileSync("git", ["config", "user.name", "Pi Test"], { cwd: root });
    writeFileSync(join(root, "docs", "research", "private_pack.md"), "do not read");
    execFileSync("git", ["add", "config/gov008_ship.yaml", "docs/research/private_pack.md"], { cwd: root });
    execFileSync("git", ["commit", "-m", "init"], { cwd: root });
  }
  return root;
}

function commitAll(root: string, message: string): void {
  execFileSync("git", ["add", "config/gov008_ship.yaml", "docs/research/private_pack.md", "docs/requirements"], {
    cwd: root,
  });
  execFileSync("git", ["commit", "-m", message], { cwd: root });
}

function withEnv<T>(vars: Record<string, string | undefined>, fn: () => T): T {
  const old = new Map(Object.keys(vars).map((key) => [key, process.env[key]]));
  for (const [key, value] of Object.entries(vars)) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
  try {
    return fn();
  } finally {
    for (const [key, value] of old) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
}

async function withEnvAsync<T>(vars: Record<string, string | undefined>, fn: () => Promise<T>): Promise<T> {
  const old = new Map(Object.keys(vars).map((key) => [key, process.env[key]]));
  for (const [key, value] of Object.entries(vars)) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
  try {
    return await fn();
  } finally {
    for (const [key, value] of old) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
}

async function makeHandlers(root: string): Promise<Record<string, (event: unknown, ctx?: unknown) => Promise<unknown>>> {
  const handlers: Record<string, (event: unknown, ctx?: unknown) => Promise<unknown>> = {};
  guard({ on: (name: string, h: (event: unknown, ctx: unknown) => Promise<unknown>) => ((handlers[name] = h), () => undefined) } as never);
  if (handlers.session_start) await handlers.session_start({ type: "session_start" }, { cwd: root });
  return handlers;
}

function ctx(root: string, hasUI = true, answer = true): unknown {
  return { cwd: root, hasUI, ui: { confirm: async () => answer, notify: () => undefined } };
}

test("forbidden git commands are blocked with specific rule names", () => {
  const root = fakeRepo();
  for (const [command, rule] of [
    ["git push --force origin claude/x", "force-push"],
    ["git push -f", "force-push"],
    ["git push origin +claude/x", "force-push"],
    ["git push --force-with-lease", "force-push"],
    ["git push origin main", "pushing main directly (use tools/gov008/ship.py)"],
    ["git update-ref refs/heads/main abc123", "moving main outside ship"],
    ["git rebase main", "rebase"],
    ["git reset --hard HEAD~1", "git reset --hard"],
    ["git filter-branch --tree-filter x", "history rewrite"],
    ["git clean -fdx", "git clean of untracked files"],
    ["git checkout -- .", "discarding all working-tree changes"],
    ["git restore .", "discarding all working-tree changes"],
    ["git commit --no-verify -m x", "skipping git hooks"],
  ]) {
    const verdict = checkCommand(root, command);
    assert.ok(verdict && "block" in verdict, `expected block: ${command}`);
    assert.equal(verdict.rule, rule);
  }
});

test("ordinary non-wide commands pass", () => {
  const root = fakeRepo();
  for (const command of [
    "git push origin claude/gov008-p6-pi",
    "git checkout -b claude/topic main",
    "git clean -n",
    ".venv/Scripts/python.exe tools/gov008/ship.py --repo . --execute --push",
    ".venv/Scripts/python.exe -m pytest tests -n 16 --dist loadfile -q",
    "git diff -- tools/tasks.py",
  ]) {
    assert.equal(checkCommand(root, command), undefined, command);
  }
});

test("commands touching an excluded path are blocked", () => {
  const root = fakeRepo();
  for (const command of [
    "cat docs/research/private_pack.md",
    "git add docs/research/private_pack.md",
    "git diff -- docs/research/private_pack.md",
    "git status -- . ':(exclude,literal)docs/research/private_pack.md' docs/research/private_pack.md",
  ]) {
    const verdict = checkCommand(root, command);
    assert.ok(verdict && "block" in verdict, `expected block: ${command}`);
    assert.equal(verdict.rule, "excluded-file");
  }
});

test("exclude pathspecs for an excluded path pass (AGENTS.md requires them)", () => {
  const root = fakeRepo();
  for (const command of [
    "git status --short -- . ':(exclude,literal)docs/research/private_pack.md'",
    'git diff --stat -- . ":(exclude,literal)docs/research/private_pack.md"',
    "git diff -- . ':(literal,exclude)docs/research/private_pack.md'",
    "git status -- . ':(exclude)docs/research/private_pack.md'",
    "git status -- . ':!docs/research/private_pack.md'",
    "git add -A -- . ':^docs/research/private_pack.md'",
  ]) {
    assert.equal(checkCommand(root, command), undefined, command);
  }
});

test("writes: excluded, rendered views and pinned evidence are blocked with rule names", () => {
  const root = fakeRepo();
  const excluded = checkWrite(root, root, "docs/research/private_pack.md");
  assert.ok(excluded && "block" in excluded);
  assert.equal(excluded.rule, "excluded-file");
  for (const path of [
    "docs/task_register.md",
    "docs/task_register_completed.md",
    "registry/development_tasks/2f/abc.yaml",
    "config/etf_portfolio/assets.yaml",
    "src/ai_trading_system/etf_portfolio/regime.py",
  ]) {
    const verdict = checkWrite(root, root, path);
    assert.ok(verdict && "block" in verdict, `expected block: ${path}`);
    assert.equal(verdict.rule, "never-write");
  }
});

test("writes: zone C asks, other paths and paths outside the repo pass", () => {
  const root = fakeRepo();
  const zoneC = checkWrite(root, join(root, "src", "pkg"), "../../config/data_quality.yaml");
  assert.ok(zoneC && "confirm" in zoneC);
  assert.ok(checkWrite(root, root, "src/ai_trading_system/scoring/rules.py"));
  assert.equal(checkWrite(root, root, "src/ai_trading_system/reports/x.py"), undefined);
  assert.equal(checkWrite(root, root, "docs/requirements/X.md"), undefined);
  assert.equal(checkWrite(root, root, join(tmpdir(), "elsewhere.txt")), undefined);
});

test("C zone lock allows read grep find ls and terminates mutating tools", async () => {
  const root = fakeRepo();
  const handlers = await makeHandlers(root);
  const denied = await handlers.tool_call(
    { type: "tool_call", toolCallId: "deny", toolName: "edit", input: { path: "AGENTS.md" } },
    ctx(root, true, false),
  );
  assert.equal((denied as { terminate: boolean }).terminate, true);
  for (const toolName of ["read", "grep", "find", "ls"]) {
    assert.equal(await handlers.tool_call({ type: "tool_call", toolCallId: toolName, toolName, input: {} }, ctx(root)), undefined);
  }
  for (const toolName of ["bash", "powershell", "edit", "write"]) {
    const input = toolName === "bash" || toolName === "powershell" ? { command: "echo ok" } : { path: "notes.md" };
    const result = await handlers.tool_call({ type: "tool_call", toolCallId: toolName, toolName, input }, ctx(root));
    assert.equal((result as { block: boolean }).block, true);
    assert.equal((result as { terminate: boolean }).terminate, true);
  }
});

test("owner decision only passes for valid ids committed on main", () => {
  const valid = "owner_decision:DEVX-025:2026-10-11:ship_gate_publish_split";
  const root = fakeRepo(true);
  writeFileSync(join(root, "docs", "requirements", "X.md"), `${valid}\n`);
  withEnv({ AITS_PI_OWNER_DECISION: valid }, () => {
    const state = makeState();
    const uncommitted = writeVerdict(root, root, "AGENTS.md", state);
    assert.ok(uncommitted && "block" in uncommitted);
    assert.equal(uncommitted.rule, "owner-decision-missing");
    assert.equal(uncommitted.terminate, true);
    assert.equal(state.locked, true);
  });
  execFileSync("git", ["add", "docs/requirements/X.md"], { cwd: root });
  execFileSync("git", ["commit", "-m", "decision"], { cwd: root });
  withEnv({ AITS_PI_OWNER_DECISION: valid }, () => {
    const state = makeState();
    assert.equal(writeVerdict(root, root, "AGENTS.md", state), undefined);
    assert.deepEqual([...state.confirmedCPaths], ["AGENTS.md"]);
  });
  withEnv({ AITS_PI_OWNER_DECISION: "owner_decision:DEVX-025:missing" }, () => {
    const state = makeState();
    const malformed = writeVerdict(root, root, "AGENTS.md", state);
    assert.ok(malformed && "block" in malformed);
    assert.equal(malformed.rule, "owner-decision-missing");
  });
});

test("command checks also cover powershell-style tool use through the extension handler", async () => {
  const root = fakeRepo();
  const handlers = await makeHandlers(root);
  const result = await handlers.tool_call(
    { type: "tool_call", toolCallId: "1", toolName: "powershell", input: { command: "git push -f" } },
    ctx(root),
  );
  assert.equal((result as { block: boolean }).block, true);
  assert.equal(await handlers.tool_call(
    { type: "tool_call", toolCallId: "2", toolName: "powershell", input: { command: "Write-Output ok" } },
    ctx(root),
  ), undefined);
});

test("system python blocks without terminate; venv python passes", async () => {
  const root = fakeRepo();
  for (const command of ["python tools/tasks.py check", "py -m pytest tests", "pytest tests"]) {
    const verdict = checkCommand(root, command);
    assert.ok(verdict && "block" in verdict, command);
    assert.equal(verdict.rule, "system-python");
    assert.equal(verdict.terminate, undefined);
    assert.match(verdict.reason, /\.venv\/Scripts\/python\.exe/);
  }
  assert.equal(checkCommand(root, ".venv/Scripts/python.exe tools/tasks.py check"), undefined);
  const handlers = await makeHandlers(root);
  const blocked = await handlers.tool_call(
    { type: "tool_call", toolCallId: "py", toolName: "bash", input: { command: "python tools/tasks.py check" } },
    ctx(root),
  );
  assert.equal((blocked as { block: boolean }).block, true);
  assert.equal((blocked as { terminate?: boolean }).terminate, undefined);
});

test("repo-wide git commands require tree_clean_exclusions pathspecs", () => {
  const root = fakeRepo();
  for (const command of ["git status", "git status --short", "git diff", "git add -A", "git add ."]) {
    const verdict = checkCommand(root, command);
    assert.ok(verdict && "block" in verdict, command);
    assert.equal(verdict.rule, "git-whole-repo");
  }
  assert.equal(checkCommand(root, "git status --short -- . ':(exclude,literal)docs/research/private_pack.md'"), undefined);
  assert.equal(checkCommand(root, "git diff -- tools/tasks.py"), undefined);
});

test("aits ops variants require reading the operations runbook first", () => {
  const root = fakeRepo();
  const state = makeState();
  for (const command of ["aits ops daily-run", "aits.exe ops daily-run", ".venv/Scripts/aits ops daily-run", ".venv/Scripts/aits.exe ops daily-run"]) {
    const blocked = commandVerdict(root, command, state);
    assert.ok(blocked && "block" in blocked, command);
    assert.equal(blocked.rule, "runbook-before-ops");
  }
  state.runbookRead = true;
  assert.equal(commandVerdict(root, "aits.exe ops daily-run", state), undefined);
});

test("tasks.py set notes is blocked and DONE needs explicit allow flag", () => {
  const root = fakeRepo();
  const notes = checkCommand(root, ".venv/Scripts/python.exe tools/tasks.py set DEVX-1 notes=x");
  assert.ok(notes && "block" in notes);
  assert.equal(notes.rule, "tasks-set-notes");
  withEnv({ AITS_PI_ALLOW_DONE: undefined }, () => {
    const done = checkCommand(root, ".venv/Scripts/python.exe tools/tasks.py set DEVX-1 status=DONE");
    assert.ok(done && "block" in done);
    assert.equal(done.rule, "tasks-done-flag");
  });
  withEnv({ AITS_PI_ALLOW_DONE: "1" }, () => {
    assert.equal(checkCommand(root, ".venv/Scripts/python.exe tools/tasks.py set DEVX-1 status=DONE"), undefined);
  });
  assert.equal(checkCommand(root, ".venv/Scripts/python.exe tools/tasks.py append-note DEVX-1 x"), undefined);
});

test("large bash outputs are written to a file and short/read outputs are unchanged", async () => {
  const root = fakeRepo(true);
  const runDir = mkdtempSync(join(tmpdir(), "aits-run-"));
  withEnv({ AITS_PI_RUN_DIR: runDir }, () => {
    assert.equal(truncateToolResult(root, "small", "short"), undefined);
    const large = "x".repeat(100_000);
    const summary = truncateToolResult(root, "large/call", large);
    assert.ok(summary);
    assert.match(summary.text, /full output written to/);
    assert.ok(summary.text.length < 32_000);
    assert.equal(readFileSync(join(runDir, "tool-large_call.txt"), "utf8"), large);
  });

  const handlers = await makeHandlers(root);
  const largeRead = "y".repeat(100_000);
  assert.equal(
    await handlers.tool_result(
      { type: "tool_result", toolCallId: "read1", toolName: "read", input: {}, content: [{ type: "text", text: largeRead }], isError: false },
      ctx(root),
    ),
    undefined,
  );
  const bashResult = withEnv({ AITS_PI_RUN_DIR: runDir }, () =>
    handlers.tool_result(
      {
        type: "tool_result",
        toolCallId: "bash1",
        toolName: "bash",
        input: { command: "echo long" },
        content: [{ type: "text", text: "z".repeat(100_000) }],
        structuredContent: { raw: true },
        isError: false,
      },
      ctx(root),
    ),
  );
  const resolved = await bashResult;
  assert.ok(resolved);
  assert.ok(((resolved as { content: { text: string }[] }).content[0].text.length) < 32_000);
  assert.deepEqual((resolved as { structuredContent: unknown }).structuredContent, {
    truncated: true,
    full_output_path: join(runDir, "tool-bash1.txt"),
  });
});

test("truncation without run dir writes under git common dir", () => {
  const root = fakeRepo(true);
  withEnv({ AITS_PI_RUN_DIR: undefined }, () => {
    const summary = truncateToolResult(root, "default-dir", "q".repeat(100_000));
    assert.ok(summary);
    assert.equal(dirname(summary.path), join(root, ".git", "pi-tool-output"));
    assert.equal(readFileSync(summary.path, "utf8"), "q".repeat(100_000));
  });
});

test("report includes committed and working-tree files and omits report when run dir is unset", () => {
  const root = fakeRepo(true);
  execFileSync("git", ["checkout", "-b", "pi/report"], { cwd: root });
  writeFileSync(join(root, "notes.md"), "allowed");
  writeFileSync(join(root, "src", "pkg", "committed.py"), "out of scope");
  execFileSync("git", ["add", "notes.md", "src/pkg/committed.py"], { cwd: root });
  execFileSync("git", ["commit", "-m", "report changes"], { cwd: root });
  writeFileSync(join(root, "src", "pkg", "working.py"), "also out of scope");
  const runDir = mkdtempSync(join(tmpdir(), "aits-report-"));
  withEnv(
    {
      AITS_PI_RUN_DIR: runDir,
      AITS_PI_RUN: "DEVX-025-01",
      AITS_PI_TASK: "DEVX-025",
      AITS_PI_ZONE: "B",
      AITS_PI_ALLOWED_PATHS: "docs/**;*.md",
    },
    () => {
      const state = makeState();
      state.blocked.push({ tool: "bash", rule: "x", reason: "y" });
      state.tests.push({ command: "node --test guard.test.ts", result: "passed", summary: "ok" });
      const report = buildReport(root, state) as { schema: string; changed_files: Record<string, string[]>; out_of_scope_files: string[] };
      assert.equal(report.schema, "aits.pi.run-report/0");
      assert.ok(report.changed_files.A.includes("notes.md"));
      assert.ok(report.changed_files.B.includes("src/pkg/committed.py"));
      assert.ok(report.changed_files.B.includes("src/pkg/working.py"));
      assert.ok(!report.out_of_scope_files.includes("notes.md"));
      assert.ok(report.out_of_scope_files.includes("src/pkg/committed.py"));
      assert.ok(report.out_of_scope_files.includes("src/pkg/working.py"));
      writeReport(root, state);
      assert.equal(JSON.parse(readFileSync(join(runDir, "report.json"), "utf8")).run, "DEVX-025-01");
    },
  );
  const noReportDir = mkdtempSync(join(tmpdir(), "aits-no-report-"));
  withEnv({ AITS_PI_RUN_DIR: undefined }, () => {
    writeReport(noReportDir, makeState());
    assert.equal(existsSync(join(noReportDir, "report.json")), false);
  });
});

test("C zone detection ignores baseline changes and locks only new session changes", () => {
  const root = fakeRepo(true);
  writeFileSync(join(root, "config", "data_quality.yaml"), "baseline");
  const state = makeState();
  initializeCZoneBaseline(root, state);
  assert.deepEqual(detectUnconfirmedCChanges(root, state), []);
  mkdirSync(join(root, "src", "ai_trading_system", "scoring"), { recursive: true });
  writeFileSync(join(root, "src", "ai_trading_system", "scoring", "rules.py"), "new");
  assert.deepEqual(detectUnconfirmedCChanges(root, state), ["src/ai_trading_system/scoring/rules.py"]);
  state.confirmedCPaths.add("src/ai_trading_system/scoring/rules.py");
  assert.deepEqual(detectUnconfirmedCChanges(root, state), []);
});

test("root is found from a subdirectory", () => {
  const root = fakeRepo();
  assert.equal(findRepoRoot(join(root, "src", "pkg")), root);
});

test("extension handler blocks, asks, handles no UI denial, and records rules", async () => {
  const root = fakeRepo();
  let handlers = await makeHandlers(root);
  const push = { type: "tool_call", toolCallId: "1", toolName: "bash", input: { command: "git push -f" } };
  const blocked = await handlers.tool_call(push, ctx(root));
  assert.equal((blocked as { block: boolean }).block, true);
  assert.equal((blocked as { terminate?: boolean }).terminate, undefined);

  const editAllowed = { type: "tool_call", toolCallId: "2", toolName: "edit", input: { path: "AGENTS.md" } };
  assert.equal(await handlers.tool_call(editAllowed, ctx(root, true, true)), undefined);

  handlers = await makeHandlers(root);
  const noUiDenied = { type: "tool_call", toolCallId: "3", toolName: "edit", input: { path: "config/data_quality.yaml" } };
  const noUi = await handlers.tool_call(noUiDenied, ctx(root, false, true));
  assert.equal((noUi as { block: boolean }).block, true);
  assert.equal((noUi as { terminate: boolean }).terminate, true);
  const read = { type: "tool_call", toolCallId: "4", toolName: "read", input: { path: "AGENTS.md" } };
  assert.equal(await handlers.tool_call(read, ctx(root, false, true)), undefined);
  const writeAfterLock = { type: "tool_call", toolCallId: "5", toolName: "write", input: { path: "notes.md" } };
  assert.equal(((await handlers.tool_call(writeAfterLock, ctx(root))) as { block: boolean }).block, true);
});

test("run directory artifacts cannot be written by tools or commands", () => {
  const root = fakeRepo();
  const runDir = mkdtempSync(join(tmpdir(), "aits-run-artifacts-"));
  withEnv({ AITS_PI_RUN_DIR: runDir }, () => {
    for (const name of ["report.json", "checks.json", "acceptance.json", "publish.json"]) {
      const verdict = checkWrite(root, root, join(runDir, name));
      assert.ok(verdict && "block" in verdict, name);
      assert.equal(verdict.rule, "run-artifact");
    }
    assert.equal(checkWrite(root, root, join(runDir, "equivalence.json")), undefined);
    assert.equal(checkWrite(root, root, join(runDir, "tmp.py")), undefined);
    for (const command of [
      `echo x > ${join(runDir, "report.json")}`,
      `echo x >> ${join(runDir, "checks.json")}`,
      `echo x | tee ${join(runDir, "acceptance.json")}`,
      `Set-Content ${join(runDir, "publish.json")} x`,
      `Add-Content ${join(runDir, "report.json")} x`,
      `Out-File ${join(runDir, "checks.json")}`,
      `New-Item ${join(runDir, "acceptance.json")}`,
      `Copy-Item x ${join(runDir, "publish.json")}`,
      `Move-Item x ${join(runDir, "report.json")}`,
      `Remove-Item ${join(runDir, "checks.json")}`,
      `cp x ${join(runDir, "acceptance.json")}`,
      `mv x ${join(runDir, "publish.json")}`,
      `rm ${join(runDir, "report.json")}`,
      `del ${join(runDir, "checks.json")}`,
      `copy x ${join(runDir, "acceptance.json")}`,
      `move x ${join(runDir, "publish.json")}`,
    ]) {
      const verdict = checkCommand(root, command);
      assert.ok(verdict && "block" in verdict, command);
      assert.equal(verdict.rule, "run-artifact");
    }
    for (const command of [
      `cat ${join(runDir, "report.json")}`,
      `Get-Content ${join(runDir, "checks.json")}`,
      "grep -n publish.json scripts/agent/publish.ps1",
      `.venv/Scripts/python.exe scripts/agent/accept_checks.py --run-dir ${runDir}`,
    ]) assert.equal(checkCommand(root, command), undefined, command);
  });
  withEnv({ AITS_PI_RUN_DIR: undefined }, () => {
    assert.equal(checkWrite(root, root, join(runDir, "report.json")), undefined);
  });
});

test("heredoc bodies are ignored except when executed by a shell", () => {
  const root = fakeRepo();
  assert.equal(checkCommand(root, "cat > x.py <<'PY'\nprint(1)\nPY"), undefined);
  assert.equal(checkCommand(root, "cd x && cat > a.py <<'PY'\nprint(1)\nPY"), undefined);
  assert.equal(checkCommand(root, "cat > a.sh <<'SH'\npython -V\nSH"), undefined);
  assert.equal(checkCommand(root, ".venv/Scripts/python.exe - <<'PY'\nimport sys\nPY"), undefined);
  assert.equal(checkCommand(root, "cat > x.py <<-EOF\nprint(1)\n\tEOF"), undefined);
  for (const command of [
    "python - <<'PY'\nPY",
    "cat <<'EOF'\nx\nEOF\npython -V",
    "bash <<'EOF'\npython -V\nEOF",
    "cd x && bash <<'EOF'\npython -V\nEOF",
    "cat <<'EOF' | bash\npython -V\nEOF",
    "env bash <<'EOF'\npython -V\nEOF",
    "cat <<'EOF' | sed s/a/b/ | bash\npython -V\nEOF",
    "cat <<'EOF' > a.sh; bash a.sh\npython -V\nEOF",
    "cat <<<'x'\npython -V",
  ]) {
    const verdict = checkCommand(root, command);
    assert.ok(verdict && "block" in verdict, command);
    assert.equal(verdict.rule, "system-python");
  }
});

test("split command segments are checked and recorded", async () => {
  const root = fakeRepo(true);
  for (const [command, rule] of [
    ["cd x; python -V", "system-python"],
    ["true || python -V", "system-python"],
    ["cd x; git status", "git-whole-repo"],
  ]) {
    const verdict = checkCommand(root, command);
    assert.ok(verdict && "block" in verdict, command);
    assert.equal(verdict.rule, rule);
  }
  const handlers = await makeHandlers(root);
  const command = "cd repo; .venv/Scripts/python.exe -m pytest tests/x.py -q";
  await handlers.tool_result({ type: "tool_result", toolCallId: "seg", toolName: "bash", input: { command }, content: [{ type: "text", text: "ok" }], isError: false }, ctx(root));
  const runDir = mkdtempSync(join(tmpdir(), "aits-segment-report-"));
  await withEnvAsync({ AITS_PI_RUN_DIR: runDir }, async () => {
    await handlers.agent_settled({ type: "agent_settled" }, ctx(root));
  });
  const report = JSON.parse(readFileSync(join(runDir, "report.json"), "utf8")) as { tests: { command: string }[] };
  assert.deepEqual(report.tests.map((item) => item.command), [command]);
});

function assertBlocked(root: string, rule: string, commands: readonly string[]): void {
  for (const command of commands) {
    const verdict = checkCommand(root, command);
    assert.ok(verdict && "block" in verdict, `expected block: ${command}`);
    assert.equal(verdict.rule, rule, command);
  }
}

function assertPasses(root: string, commands: readonly string[]): void {
  for (const command of commands) assert.equal(checkCommand(root, command), undefined, command);
}

test("segments split at pipes and background & but not inside quotes or comments", () => {
  const root = fakeRepo();
  assertBlocked(root, "system-python", [
    "cat x | python -",
    "cat x |python -",
    "git log -1 | python -c 'print(1)'",
    "sleep 1 & python -V",
    "make 2>&1 |& python -",
    'echo "a; b" ; python -V',
  ]);
  assertBlocked(root, "git-whole-repo", ['echo "x" && git status', "git status --short | head -5"]);
  assertPasses(root, [
    'git commit -m "fix; git status"',
    'git commit -m "note: python -V && pytest"',
    "echo 'a | python -V'",
    'git commit -m "release notes\n\npython 3.11 is required"',
    "git status --short -- . ':(exclude,literal)docs/research/private_pack.md' # it's clean; python -V",
    '.venv/Scripts/python.exe -m pytest -k "a or b" -q 2>&1 | tail -5',
    ".venv/Scripts/python.exe -m pytest tests/x.py -q &> pytest.log",
  ]);
});

test("system python is found behind wrappers, assignments and paths; venv python passes", () => {
  const root = fakeRepo();
  assertBlocked(root, "system-python", [
    "env -i python -V",
    "env -u HOME python -V",
    "env -- python -V",
    "PYTHONPATH=src python -m x",
    "command python -V",
    "nohup python x.py",
    "time -p python x.py",
    "exec -a name python x.py",
    "python3 -V",
    "python3.11 -V",
    "Python -V",
    "/usr/bin/python3 -V",
    "C:/Users/x/AppData/Local/Programs/Python/Python311/python.exe -V",
    '"C:\\Program Files\\Python311\\python.exe" -V',
  ]);
  assertPasses(root, [
    ".venv/Scripts/python.exe -V",
    "./.venv/Scripts/python.exe -V",
    ".venv\\Scripts\\python.exe -V",
    "D:/Work/AITradingSystem_agent/.venv/Scripts/python.exe -V",
    "env AITS_X=1 .venv/Scripts/python.exe -V",
    ".venv/Scripts/pytest.exe -q",
    "command -v python",
    "which python",
    "grep -n python README.md",
  ]);
});

test("heredoc bodies stay checked when a shell runs them right after | or || or behind env options", () => {
  const root = fakeRepo();
  assertBlocked(root, "system-python", [
    "cat x|bash <<'EOF'\npython -V\nEOF",
    "false ||bash <<'EOF'\npython -V\nEOF",
    "cat <<'EOF' |bash\npython -V\nEOF",
    "env -i bash <<'EOF'\npython -V\nEOF",
    "env -u X bash <<'EOF'\npython -V\nEOF",
  ]);
  assertPasses(root, ["cat > a.md <<'EOF'\nit's python -V; fine\nEOF", "cd x|| cat > a.py <<'PY'\nimport os\nPY"]);
});

test("whole-repo git rule covers global options, add -u, commit -a and stash", () => {
  const root = fakeRepo();
  assertBlocked(root, "git-whole-repo", [
    "git -C . status",
    "git --no-pager diff",
    "git -c core.quotepath=off status --short",
    "git add -u",
    "git add --all",
    "git add -Av",
    'git commit -am "x"',
    "git commit -a -m x",
    "git commit --all -m x",
    "git stash",
    "git stash push -m wip",
    "git stash -u",
    "git status -- . src",
    "git diff -- ':!docs/other.md'",
    "git status -- . ':(exclude)docs/other.md'",
  ]);
  assertPasses(root, [
    'git commit -m "add -a flag"',
    'git commit -m "-a"',
    "git commit -F msg.txt",
    "git add src/a.py tests/test_a.py",
    "git stash list",
    "git stash pop",
    "git stash show -p",
    "git -C . status --short -- . ':(exclude,literal)docs/research/private_pack.md'",
    "git stash push -- . ':(exclude,literal)docs/research/private_pack.md'",
    "git diff --name-only main...HEAD -- . ':(exclude,literal)docs/research/private_pack.md'",
    "git diff -- src/a.py",
    "git log --stat",
  ]);
  const verdict = checkCommand(root, "git commit -am x");
  assert.ok(verdict && "block" in verdict);
  assert.match(verdict.reason, /git add the changed files by path/);
});

test("forbidden git commands are blocked behind git global options", () => {
  const root = fakeRepo();
  for (const [command, rule] of [
    ["git -C D:/x push --force origin pi/x", "force-push"],
    ["git --no-pager rebase main", "rebase"],
    ["git -c user.name=x reset --hard HEAD", "git reset --hard"],
    ["git -C . push origin main", "pushing main directly (use tools/gov008/ship.py)"],
  ]) {
    assertBlocked(root, rule, [command]);
  }
});

test("test command recording ignores mentions and records real test invocations", async () => {
  const root = fakeRepo(true);
  const runToolResult = async (handlers: Record<string, (event: unknown, ctx?: unknown) => Promise<unknown>>, command: string) => {
    await handlers.tool_result({ type: "tool_result", toolCallId: command, toolName: "bash", input: { command }, content: [{ type: "text", text: "ok" }], isError: false }, ctx(root));
  };

  const handlers = await makeHandlers(root);
  for (const command of [
    "cat >> tests/test_x.py <<'PY'\nimport pytest\nPY",
    "grep pytest tests/x.py",
    "echo pytest",
    "Get-Content -LiteralPath 'C:/Users/x/AppData/Local/Temp/pytest-of-JACK/pytest-1/run/stderr.log' -Raw",
    ".venv/Scripts/python.exe -c \"from pathlib import Path; print(Path(r'C:/Temp/pytest-of-JACK/x.log').exists())\"",
  ]) {
    await runToolResult(handlers, command);
  }
  const ignoredRunDir = mkdtempSync(join(tmpdir(), "aits-report-ignored-"));
  await withEnvAsync({ AITS_PI_RUN_DIR: ignoredRunDir }, async () => {
    await handlers.agent_settled({ type: "agent_settled" }, ctx(root));
  });
  let report = JSON.parse(readFileSync(join(ignoredRunDir, "report.json"), "utf8")) as { tests: { command: string }[] };
  assert.deepEqual(report.tests, []);

  const handlers2 = await makeHandlers(root);
  const commands = [
    ".venv/Scripts/python.exe -m pytest tests/x.py -q",
    "cd repo && .venv/Scripts/python.exe -m ruff check x",
    "node --test .pi/extensions/aits-guard/guard.test.ts",
  ];
  for (const command of commands) await runToolResult(handlers2, command);
  const runDir = mkdtempSync(join(tmpdir(), "aits-report-tests-"));
  await withEnvAsync({ AITS_PI_RUN_DIR: runDir }, async () => {
    await handlers2.agent_settled({ type: "agent_settled" }, ctx(root));
  });
  report = JSON.parse(readFileSync(join(runDir, "report.json"), "utf8")) as { tests: { command: string }[] };
  assert.deepEqual(report.tests.map((item) => item.command), commands);
});
