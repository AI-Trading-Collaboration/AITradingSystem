// Run: node --test .pi/extensions/aits-guard/guard.test.ts  (Node >= 23.6 strips the types)
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import guard, {
  buildReport,
  checkCommand,
  checkWrite,
  commandVerdict,
  detectUnconfirmedCChanges,
  findRepoRoot,
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
    execFileSync("git", ["add", "config/gov008_ship.yaml"], { cwd: root });
    execFileSync("git", ["commit", "-m", "init"], { cwd: root });
  }
  return root;
}

function withEnv(vars: Record<string, string | undefined>, fn: () => void): void {
  const old = new Map(Object.keys(vars).map((key) => [key, process.env[key]]));
  for (const [key, value] of Object.entries(vars)) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
  try {
    fn();
  } finally {
    for (const [key, value] of old) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
}

test("forbidden git commands are blocked", () => {
  const root = fakeRepo();
  for (const command of [
    "git push --force origin claude/x",
    "git push -f",
    "git push origin +claude/x",
    "git push --force-with-lease",
    "git push origin main",
    "git update-ref refs/heads/main abc123",
    "git rebase main",
    "git reset --hard HEAD~1",
    "git filter-branch --tree-filter x",
    "git clean -fdx",
    "git checkout -- .",
    "git restore .",
    "git commit --no-verify -m x",
  ]) {
    const verdict = checkCommand(root, command);
    assert.ok(verdict && "block" in verdict, `expected block: ${command}`);
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

test("writes: excluded, rendered views and pinned evidence are blocked", () => {
  const root = fakeRepo();
  for (const path of [
    "docs/research/private_pack.md",
    "docs/task_register.md",
    "docs/task_register_completed.md",
    "registry/development_tasks/2f/abc.yaml",
    "config/etf_portfolio/assets.yaml",
    "src/ai_trading_system/etf_portfolio/regime.py",
  ]) {
    const verdict = checkWrite(root, root, path);
    assert.ok(verdict && "block" in verdict, `expected block: ${path}`);
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

test("C zone lock blocks non-read tools but allows read grep find ls", () => {
  const root = fakeRepo();
  const state = makeState();
  state.locked = true;
  assert.ok(commandVerdict(root, "git status", state));
  for (const tool of ["read", "grep", "find", "ls"]) assert.ok(new Set(["read", "grep", "find", "ls"]).has(tool));
});

test("owner decision environment allows C zone only when original text exists", () => {
  const root = fakeRepo(true);
  writeFileSync(join(root, "docs", "requirements", "X.md"), "owner_decision:DEVX-025:2026-10-11:ship_gate_publish_split\n");
  execFileSync("git", ["add", "docs/requirements/X.md"], { cwd: root });
  execFileSync("git", ["commit", "-m", "decision"], { cwd: root });
  withEnv({ AITS_PI_OWNER_DECISION: "owner_decision:DEVX-025:2026-10-11:ship_gate_publish_split" }, () => {
    const state = makeState();
    assert.equal(writeVerdict(root, root, "AGENTS.md", state), undefined);
    assert.deepEqual([...state.confirmedCPaths], ["AGENTS.md"]);
  });
  withEnv({ AITS_PI_OWNER_DECISION: "owner_decision:DEVX-025:missing" }, () => {
    const state = makeState();
    const verdict = writeVerdict(root, root, "AGENTS.md", state);
    assert.ok(verdict && "block" in verdict);
    assert.equal(state.locked, true);
  });
});

test("command checks also cover powershell-style tool use through the extension handler", async () => {
  const root = fakeRepo();
  const handlers: Record<string, (event: unknown, ctx: unknown) => Promise<unknown>> = {};
  guard({ on: (name: string, h: (event: unknown, ctx: unknown) => Promise<unknown>) => ((handlers[name] = h), () => undefined) } as never);
  const result = await handlers.tool_call(
    { type: "tool_call", toolCallId: "1", toolName: "powershell", input: { command: "git push -f" } },
    { cwd: root, hasUI: true, ui: { confirm: async () => true } },
  );
  assert.equal((result as { block: boolean }).block, true);
  assert.equal(await handlers.tool_call(
    { type: "tool_call", toolCallId: "2", toolName: "powershell", input: { command: "Write-Output ok" } },
    { cwd: root, hasUI: true, ui: { confirm: async () => true } },
  ), undefined);
});

test("system python, py, and pytest are blocked; venv python passes", () => {
  const root = fakeRepo();
  for (const command of ["python tools/tasks.py check", "py -m pytest tests", "pytest tests"]) {
    const verdict = checkCommand(root, command);
    assert.ok(verdict && "block" in verdict, command);
    assert.match(verdict.reason, /\.venv\/Scripts\/python\.exe/);
  }
  assert.equal(checkCommand(root, ".venv/Scripts/python.exe tools/tasks.py check"), undefined);
});

test("repo-wide git commands require tree_clean_exclusions pathspecs", () => {
  const root = fakeRepo();
  for (const command of ["git status", "git status --short", "git diff", "git add -A", "git add ."]) {
    const verdict = checkCommand(root, command);
    assert.ok(verdict && "block" in verdict, command);
  }
  assert.equal(checkCommand(root, "git status --short -- . ':(exclude,literal)docs/research/private_pack.md'"), undefined);
  assert.equal(checkCommand(root, "git diff -- tools/tasks.py"), undefined);
});

test("aits ops requires reading the operations runbook first", () => {
  const root = fakeRepo();
  const state = makeState();
  const blocked = commandVerdict(root, "aits ops daily-run", state);
  assert.ok(blocked && "block" in blocked);
  state.runbookRead = true;
  assert.equal(commandVerdict(root, "aits ops daily-run", state), undefined);
});

test("tasks.py set notes is blocked and DONE needs explicit allow flag", () => {
  const root = fakeRepo();
  assert.ok(checkCommand(root, ".venv/Scripts/python.exe tools/tasks.py set DEVX-1 notes=x"));
  withEnv({ AITS_PI_ALLOW_DONE: undefined }, () => {
    assert.ok(checkCommand(root, ".venv/Scripts/python.exe tools/tasks.py set DEVX-1 status=DONE"));
  });
  withEnv({ AITS_PI_ALLOW_DONE: "1" }, () => {
    assert.equal(checkCommand(root, ".venv/Scripts/python.exe tools/tasks.py set DEVX-1 status=DONE"), undefined);
  });
  assert.equal(checkCommand(root, ".venv/Scripts/python.exe tools/tasks.py append-note DEVX-1 x"), undefined);
});

test("large tool results are written to a file and small results are unchanged", () => {
  const root = fakeRepo(true);
  const runDir = mkdtempSync(join(tmpdir(), "aits-run-"));
  withEnv({ AITS_PI_RUN_DIR: runDir }, () => {
    assert.equal(truncateToolResult(root, "small", "short"), undefined);
    const large = Array.from({ length: 6000 }, (_, i) => `line ${i}`).join("\n");
    const summary = truncateToolResult(root, "large/call", large);
    assert.ok(summary);
    assert.match(summary, /full output written to/);
    assert.match(summary, /line 0/);
    assert.match(summary, /line 5999/);
    assert.equal(readFileSync(join(runDir, "tool-large_call.txt"), "utf8"), large);
  });
});

test("agent_settled report has the required schema and is written when run dir is set", () => {
  const root = fakeRepo(true);
  writeFileSync(join(root, "notes.md"), "changed");
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
      state.tests.push({ command: "node --test guard.test.ts", result: "PASS", summary: "ok" });
      const report = buildReport(root, state) as { schema: string; changed_files: Record<string, string[]>; out_of_scope_files: string[] };
      assert.equal(report.schema, "aits.pi.run-report/0");
      assert.deepEqual(report.changed_files.A, ["notes.md"]);
      assert.deepEqual(report.out_of_scope_files, []);
      writeReport(root, state);
      assert.equal(JSON.parse(readFileSync(join(runDir, "report.json"), "utf8")).run, "DEVX-025-01");
    },
  );
});

test("bash result detects unconfirmed C zone changes and clean B changes pass", () => {
  const root = fakeRepo(true);
  const state = makeState();
  writeFileSync(join(root, "config", "data_quality.yaml"), "changed");
  assert.deepEqual(detectUnconfirmedCChanges(root, state), ["config/data_quality.yaml"]);
  state.confirmedCPaths.add("config/data_quality.yaml");
  assert.deepEqual(detectUnconfirmedCChanges(root, state), []);
  writeFileSync(join(root, "src", "pkg", "x.py"), "changed");
  assert.deepEqual(detectUnconfirmedCChanges(root, state), []);
});

test("root is found from a subdirectory", () => {
  const root = fakeRepo();
  assert.equal(findRepoRoot(join(root, "src", "pkg")), root);
});

test("extension handler blocks, asks, locks, and passes", async () => {
  const root = fakeRepo();
  const handlers: Record<string, (event: unknown, ctx: unknown) => Promise<unknown>> = {};
  guard({ on: (name: string, h: (event: unknown, ctx: unknown) => Promise<unknown>) => ((handlers[name] = h), () => undefined) } as never);
  const ctx = (hasUI: boolean, answer: boolean) => ({
    cwd: root,
    hasUI,
    ui: { confirm: async () => answer, notify: () => undefined },
  });
  const push = { type: "tool_call", toolCallId: "1", toolName: "bash", input: { command: "git push -f" } };
  assert.equal(((await handlers.tool_call(push, ctx(true, true))) as { block: boolean }).block, true);

  const editAllowed = { type: "tool_call", toolCallId: "2", toolName: "edit", input: { path: "AGENTS.md" } };
  assert.equal(await handlers.tool_call(editAllowed, ctx(true, true)), undefined);

  const editDenied = { type: "tool_call", toolCallId: "3", toolName: "edit", input: { path: "config/data_quality.yaml" } };
  assert.equal(((await handlers.tool_call(editDenied, ctx(true, false))) as { block: boolean }).block, true);
  const read = { type: "tool_call", toolCallId: "4", toolName: "read", input: { path: "AGENTS.md" } };
  assert.equal(await handlers.tool_call(read, ctx(false, true)), undefined);
  const writeAfterLock = { type: "tool_call", toolCallId: "5", toolName: "write", input: { path: "notes.md" } };
  assert.equal(((await handlers.tool_call(writeAfterLock, ctx(false, true))) as { block: boolean }).block, true);
});
