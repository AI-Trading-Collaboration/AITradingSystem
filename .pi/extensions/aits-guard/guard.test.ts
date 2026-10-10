// Run: node --test .pi/extensions/aits-guard/guard.test.ts  (Node >= 23.6 strips the types)
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import guard, { checkCommand, checkWrite, findRepoRoot } from "./index.ts";

function fakeRepo(): string {
  const root = mkdtempSync(join(tmpdir(), "aits-guard-"));
  mkdirSync(join(root, "config"));
  mkdirSync(join(root, "src", "pkg"), { recursive: true });
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
  return root;
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

test("ordinary commands pass", () => {
  const root = fakeRepo();
  for (const command of [
    "git status -- . ':(exclude,literal)docs/x.md'",
    "git push origin claude/gov008-p6-pi",
    "git checkout -b claude/topic main",
    "git clean -n",
    "python tools/gov008/ship.py --repo . --execute --push",
    "python -m pytest tests -n 16 --dist loadfile -q",
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

test("root is found from a subdirectory", () => {
  const root = fakeRepo();
  assert.equal(findRepoRoot(join(root, "src", "pkg")), root);
});

test("extension handler blocks, asks, and passes", async () => {
  const root = fakeRepo();
  let handler: ((event: unknown, ctx: unknown) => Promise<unknown>) | undefined;
  guard({ on: (_name: string, h: typeof handler) => (handler = h) } as never);
  assert.ok(handler);
  const ctx = (hasUI: boolean, answer: boolean) => ({
    cwd: root,
    hasUI,
    ui: { confirm: async () => answer, notify: () => undefined },
  });
  const push = { type: "tool_call", toolCallId: "1", toolName: "bash", input: { command: "git push -f" } };
  assert.equal(((await handler(push, ctx(true, true))) as { block: boolean }).block, true);
  const edit = { type: "tool_call", toolCallId: "2", toolName: "edit", input: { path: "AGENTS.md" } };
  assert.equal(await handler(edit, ctx(true, true)), undefined);
  assert.equal(((await handler(edit, ctx(true, false))) as { block: boolean }).block, true);
  assert.equal(((await handler(edit, ctx(false, true))) as { block: boolean }).block, true);
  const read = { type: "tool_call", toolCallId: "3", toolName: "read", input: { path: "AGENTS.md" } };
  assert.equal(await handler(read, ctx(true, true)), undefined);
});
