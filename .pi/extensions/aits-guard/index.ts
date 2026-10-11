// AITradingSystem guard rails for the pi coding agent (GOV-008 P6).
//
// pi has no permission system of its own: tools run with the user's OS permissions. This extension
// blocks the repository actions AGENTS.md forbids for an agent and asks before edits to zone C paths.
// It is a guard rail against mistakes, not a security boundary: a determined command can get around
// pattern checks. The real controls stay the ship gate, the zone C owner decision, and git history.
import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, isAbsolute, join, relative, sep } from "node:path";

import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

type Verdict = { block: true; reason: string } | { confirm: string; path?: string } | undefined;
type BlockRecord = { tool?: string; command?: string; path?: string; rule: string; reason: string };
type TestRecord = { command: string; result: string; summary: string };
type ShipRecord = { mode: string; ok: boolean; sha: string; summary: string };
type RunState = {
  locked: boolean;
  blocked: BlockRecord[];
  cZoneConfirmations: string[];
  confirmedCPaths: Set<string>;
  runbookRead: boolean;
  tests: TestRecord[];
  ship: ShipRecord;
};

// Paths that must never be written by the agent, whatever the ship policy says.
const NEVER_WRITE: readonly string[] = [
  "docs/task_register.md", // rendered by tools/tasks.py
  "docs/task_register_completed.md", // rendered by tools/tasks.py
  "registry/development_tasks/**", // bytes pinned by the TRADING-2548 contract
  "config/etf_portfolio/**", // research evidence (GOV-008 P4 block 2)
  "src/ai_trading_system/etf_portfolio/regime.py", // bytes pinned by the QQQ options contract
];

const READ_ONLY_WHEN_LOCKED = new Set(["read", "grep", "find", "ls"]);
const LOCK_REASON = "C 区确认被拒，停止并汇报";
const MAX_TOOL_RESULT_CHARS = 32_000; // about 8,000 tokens at 4 chars/token.

// Shell commands AGENTS.md forbids (history rewrite, force push, moving main outside ship, hooks off).
const FORBIDDEN_COMMANDS: readonly [RegExp, string][] = [
  [/\bgit\s+push\b[^\n;&|]*(\s--force\b|\s--force-with-lease\b|\s-f\b|\s\+\S)/, "force-push"],
  [/\bgit\s+push\b[^\n;&|]*\bmain\b/, "pushing main directly (use tools/gov008/ship.py)"],
  [/\bgit\s+update-ref\b[^\n;&|]*refs\/heads\/main\b/, "moving main outside ship"],
  [/\bgit\s+rebase\b/, "rebase"],
  [/\bgit\s+reset\s+[^\n;&|]*--hard\b/, "git reset --hard"],
  [/\bgit\s+filter-(branch|repo)\b/, "history rewrite"],
  [/\bgit\s+clean\b[^\n;&|]*\s-[a-zA-Z]*[fdx]/, "git clean of untracked files"],
  [/\bgit\s+(checkout|restore)\s+(--\s+)?\.(\s|$)/, "discarding all working-tree changes"],
  [/--no-verify\b/, "skipping git hooks"],
];

export function makeState(): RunState {
  return {
    locked: false,
    blocked: [],
    cZoneConfirmations: [],
    confirmedCPaths: new Set(),
    runbookRead: false,
    tests: [],
    ship: { mode: "none", ok: false, sha: "", summary: "not run" },
  };
}

export function findRepoRoot(start: string): string | undefined {
  let dir = start;
  for (;;) {
    if (existsSync(join(dir, "config", "gov008_ship.yaml"))) return dir;
    const parent = dirname(dir);
    if (parent === dir) return undefined;
    dir = parent;
  }
}

function readShipPolicy(root: string): { exclusions: string[]; zoneA: string[]; zoneC: string[] } {
  const path = join(root, "config", "gov008_ship.yaml");
  if (!existsSync(path)) return { exclusions: [], zoneA: [], zoneC: [] };
  const exclusions: string[] = [];
  const zoneA: string[] = [];
  const zoneC: string[] = [];
  let section: "exclusions" | "zoneA" | "zoneC" | undefined;
  for (const raw of readFileSync(path, "utf8").split(/\r?\n/)) {
    if (/^tree_clean_exclusions:\s*$/.test(raw)) section = "exclusions";
    else if (/^\s{2}C:\s*$/.test(raw)) section = "zoneC";
    else if (/^\s{2}A:\s*$/.test(raw)) section = "zoneA";
    else if (/^\S/.test(raw) || /^\s{2}[A-Za-z]+:\s*$/.test(raw)) section = undefined;
    const item = /^\s*-\s+"?([^"#]+?)"?\s*(#.*)?$/.exec(raw);
    if (item && section === "exclusions") exclusions.push(item[1]);
    if (item && section === "zoneA") zoneA.push(item[1]);
    if (item && section === "zoneC") zoneC.push(item[1]);
  }
  return { exclusions, zoneA, zoneC };
}

function globToRegExp(glob: string): RegExp {
  let out = "";
  for (let i = 0; i < glob.length; i++) {
    const c = glob[i];
    if (c === "*" && glob[i + 1] === "*") {
      out += ".*";
      i++;
    } else if (c === "*") out += "[^/]*";
    else if (c === "?") out += "[^/]";
    else out += c.replace(/[.+^${}()|[\]\\]/g, "\\$&");
  }
  return new RegExp(`^${out}$`);
}

function matchesAny(rel: string, globs: readonly string[]): boolean {
  return globs.some((glob) => globToRegExp(glob).test(rel));
}

function repoRelative(root: string, cwd: string, target: string): string | undefined {
  const absolute = isAbsolute(target) ? target : join(cwd, target);
  const rel = relative(root, absolute);
  if (rel === "" || rel.startsWith("..") || isAbsolute(rel)) return undefined;
  return rel.split(sep).join("/");
}

function requirementFiles(dir: string): string[] {
  const found: string[] = [];
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) found.push(...requirementFiles(path));
    else if (entry.isFile() && entry.name.endsWith(".md")) found.push(path);
  }
  return found;
}

function ownerDecisionVerified(root: string, decision = process.env.AITS_PI_OWNER_DECISION ?? ""): boolean {
  if (decision.trim() === "") return false;
  const dir = join(root, "docs", "requirements");
  if (!existsSync(dir)) return false;
  return requirementFiles(dir).some((path) => readFileSync(path, "utf8").includes(decision));
}

function escapeRegExp(text: string): string {
  return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function hasExcludePathspec(command: string, excluded: string): boolean {
  const excludeForm = new RegExp(
    String.raw`:(?:\((?=[^)]*\bexclude\b)[a-z,]+\)|!|\^)` + escapeRegExp(excluded),
  );
  return excludeForm.test(command);
}

function hasAllExclusions(command: string, exclusions: readonly string[]): boolean {
  return exclusions.every((excluded) => hasExcludePathspec(command, excluded));
}

function splitShellCommands(command: string): string[] {
  return command.split(/\s*(?:&&|\|\||;|\r?\n)\s*/).filter(Boolean);
}

function isWholeRepoGitCommand(part: string): boolean {
  const trimmed = part.trim();
  if (/\bgit\s+add\s+(?:[^;&|]*\s)?(?:-A\b|\.(?:\s|$))/.test(trimmed)) return true;
  const statusOrDiff = /\bgit\s+(status|diff)\b/.exec(trimmed);
  if (!statusOrDiff) return false;
  const after = trimmed.slice((statusOrDiff.index ?? 0) + statusOrDiff[0].length).trim();
  const marker = /(?:^|\s)--\s+(.+)$/.exec(after);
  if (!marker) return true;
  const pathspecs = marker[1].trim();
  return pathspecs === "" || pathspecs === "." || /^\.\s+['"]?:\((?:literal,)?exclude\b/.test(pathspecs);
}

export function commandVerdict(root: string, command: string, state = makeState()): Verdict {
  if (state.locked) return { block: true, reason: LOCK_REASON };
  for (const [pattern, label] of FORBIDDEN_COMMANDS) {
    if (pattern.test(command)) return { block: true, reason: `blocked by AGENTS.md: ${label}` };
  }
  const policy = readShipPolicy(root);
  for (const excluded of policy.exclusions) {
    // AGENTS.md requires excluding these paths from repository-wide git commands with an exclude
    // pathspec (":(exclude,literal)<path>", ":(exclude)<path>", ":!<path>", ":^<path>"); that form never
    // reads the file. Any other mention of the path is blocked.
    const excludeForm = new RegExp(
      String.raw`:(?:\((?=[^)]*\bexclude\b)[a-z,]+\)|!|\^)` + escapeRegExp(excluded),
      "g",
    );
    if (command.replace(excludeForm, "").includes(excluded)) {
      return { block: true, reason: `${excluded} is an unrelated dirty file: never open or modify it` };
    }
  }
  for (const part of splitShellCommands(command)) {
    if (/^\s*(python|py|pytest)(\s|$)/i.test(part)) {
      return {
        block: true,
        reason: "use the project venv interpreter: .venv/Scripts/python.exe (for pytest: .venv/Scripts/python.exe -m pytest ...)",
      };
    }
    if (isWholeRepoGitCommand(part) && !hasAllExclusions(part, policy.exclusions)) {
      return {
        block: true,
        reason: "repository-wide git status/diff/add must include tree_clean_exclusions as exact exclude pathspecs",
      };
    }
    if (/\baits\s+ops\b/.test(part) && !state.runbookRead) {
      return { block: true, reason: "read docs/operations/operations_runbook.md before running aits ops commands" };
    }
    if (/\btools\/tasks\.py\s+set\b/.test(part) || /\btools\\tasks\.py\s+set\b/.test(part)) {
      if (/\bnotes=/.test(part)) return { block: true, reason: "use tools/tasks.py append-note instead of set notes=" };
      if (/\bstatus=DONE\b/.test(part) && process.env.AITS_PI_ALLOW_DONE !== "1") {
        return { block: true, reason: "status=DONE requires AITS_PI_ALLOW_DONE=1" };
      }
    }
  }
  return undefined;
}

export function checkCommand(root: string, command: string): Verdict {
  return commandVerdict(root, command);
}

export function writeVerdict(root: string, cwd: string, target: string, state = makeState()): Verdict {
  if (state.locked) return { block: true, reason: LOCK_REASON };
  const rel = repoRelative(root, cwd, target);
  if (rel === undefined) return undefined;
  const policy = readShipPolicy(root);
  if (policy.exclusions.includes(rel)) {
    return { block: true, reason: `${rel} is an unrelated dirty file (tree_clean_exclusions): never modify it` };
  }
  if (matchesAny(rel, NEVER_WRITE)) {
    return { block: true, reason: `${rel} is protected (rendered view or pinned research evidence)` };
  }
  if (matchesAny(rel, policy.zoneC)) {
    if (ownerDecisionVerified(root)) {
      state.confirmedCPaths.add(rel);
      state.cZoneConfirmations.push(`${rel}: AITS_PI_OWNER_DECISION`);
      return undefined;
    }
    if ((process.env.AITS_PI_OWNER_DECISION ?? "") !== "") {
      state.locked = true;
      return { block: true, reason: LOCK_REASON };
    }
    return { confirm: `${rel} is a zone C path: the change needs an owner decision. Allow this edit?`, path: rel };
  }
  return undefined;
}

export function checkWrite(root: string, cwd: string, target: string): Verdict {
  return writeVerdict(root, cwd, target);
}

function changedFiles(root: string): string[] {
  try {
    return execFileSync("git", ["status", "--porcelain", "--untracked-files=all"], { cwd: root, encoding: "utf8" })
      .split(/\r?\n/)
      .filter(Boolean)
      .map((line) => line.slice(3).replace(/"/g, ""))
      .map((path) => path.split(" -> ").pop() ?? path)
      .map((path) => path.split(sep).join("/"));
  } catch {
    return [];
  }
}

function zoneFor(root: string, rel: string): "A" | "B" | "C" {
  const policy = readShipPolicy(root);
  if (matchesAny(rel, policy.zoneC)) return "C";
  if (matchesAny(rel, policy.zoneA)) return "A";
  return "B";
}

export function detectUnconfirmedCChanges(root: string, state: RunState): string[] {
  return changedFiles(root).filter((rel) => zoneFor(root, rel) === "C" && !state.confirmedCPaths.has(rel));
}

function readToolPath(root: string, cwd: string, input: Record<string, unknown>): string | undefined {
  const raw = typeof input.path === "string" ? input.path : undefined;
  return raw ? repoRelative(root, cwd, raw) : undefined;
}

function textContent(event: { content?: unknown }): string | undefined {
  if (!Array.isArray(event.content)) return undefined;
  const text = event.content
    .map((part) => {
      if (typeof part === "string") return part;
      if (part && typeof part === "object" && "text" in part) return String((part as { text: unknown }).text ?? "");
      return "";
    })
    .join("");
  return text;
}

function runOutputDir(root: string): string {
  const configured = process.env.AITS_PI_RUN_DIR;
  if (configured && configured.trim() !== "") return configured;
  try {
    const common = execFileSync("git", ["rev-parse", "--git-common-dir"], { cwd: root, encoding: "utf8" }).trim();
    return isAbsolute(common) ? join(common, "pi-tool-output") : join(root, common, "pi-tool-output");
  } catch {
    return join(root, ".git", "pi-tool-output");
  }
}

export function truncateToolResult(root: string, toolCallId: string, content: string): string | undefined {
  if (content.length <= MAX_TOOL_RESULT_CHARS) return undefined;
  const dir = runOutputDir(root);
  mkdirSync(dir, { recursive: true });
  const path = join(dir, `tool-${toolCallId.replace(/[^A-Za-z0-9_.-]/g, "_")}.txt`);
  writeFileSync(path, content, "utf8");
  const lines = content.split(/\r?\n/);
  const head = lines.slice(0, 40).join("\n");
  const tail = lines.slice(Math.max(40, lines.length - 80)).join("\n");
  return `${head}\n\n[... full output written to ${path}; showing first 40 and last 80 lines ...]\n\n${tail}`;
}

function recordCommandResult(state: RunState, command: string, output: string | undefined, isError: boolean): void {
  const result = isError ? "FAIL" : "PASS";
  const summary = (output ?? "").split(/\r?\n/).slice(-8).join("\n").trim();
  if (/\b(pytest|ruff|node\s+--test|tools[\\/]tasks\.py\s+(check|render))\b/.test(command)) {
    state.tests.push({ command, result, summary });
  }
  if (/tools[\\/]gov008[\\/]ship\.py\b/.test(command)) {
    const mode = /--dry-run\b/.test(command) ? "dry-run" : /--execute\b/.test(command) ? "execute" : "unknown";
    state.ship = { mode, ok: !isError, sha: currentSha(), summary };
  }
}

function currentSha(): string {
  try {
    return execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim();
  } catch {
    return "";
  }
}

function groupChangedFiles(root: string): Record<"A" | "B" | "C", string[]> {
  const groups: Record<"A" | "B" | "C", string[]> = { A: [], B: [], C: [] };
  for (const rel of changedFiles(root)) groups[zoneFor(root, rel)].push(rel);
  return groups;
}

function allowedPathGlobs(): string[] {
  return (process.env.AITS_PI_ALLOWED_PATHS ?? "")
    .split(/[;\n]/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function outOfScopeFiles(root: string): string[] {
  const allowed = allowedPathGlobs();
  if (allowed.length === 0) return [];
  return changedFiles(root).filter((rel) => !matchesAny(rel, allowed));
}

function commitRange(root: string): string[] {
  try {
    return execFileSync("git", ["rev-list", "--oneline", "main..HEAD"], { cwd: root, encoding: "utf8" })
      .split(/\r?\n/)
      .filter(Boolean);
  } catch {
    return [];
  }
}

function branchName(root: string): string {
  try {
    return execFileSync("git", ["branch", "--show-current"], { cwd: root, encoding: "utf8" }).trim();
  } catch {
    return "";
  }
}

export function buildReport(root: string, state: RunState): unknown {
  return {
    schema: "aits.pi.run-report/0",
    run: process.env.AITS_PI_RUN ?? "",
    task: process.env.AITS_PI_TASK ?? "",
    zone: process.env.AITS_PI_ZONE ?? "",
    branch: branchName(root),
    commits: commitRange(root),
    changed_files: groupChangedFiles(root),
    out_of_scope_files: outOfScopeFiles(root),
    blocked: state.blocked,
    c_zone_confirmations: state.cZoneConfirmations,
    tests: state.tests,
    ship: state.ship,
    subagents: [],
  };
}

export function writeReport(root: string, state: RunState): void {
  const dir = process.env.AITS_PI_RUN_DIR;
  if (!dir) return;
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, "report.json"), JSON.stringify(buildReport(root, state), null, 2), "utf8");
}

function block(state: RunState, record: BlockRecord): { block: true; reason: string; terminate: true } {
  state.blocked.push(record);
  return { block: true, reason: record.reason, terminate: true };
}

export default function (pi: ExtensionAPI) {
  const state = makeState();

  pi.on("tool_call", async (event, ctx) => {
    const root = findRepoRoot(ctx.cwd);
    if (root === undefined) return undefined; // not inside this repository
    const input = (event.input ?? {}) as Record<string, unknown>;
    if (state.locked && !READ_ONLY_WHEN_LOCKED.has(event.toolName)) {
      return block(state, { tool: event.toolName, rule: "c-zone-lock", reason: LOCK_REASON });
    }
    if (event.toolName === "read" && readToolPath(root, ctx.cwd, input) === "docs/operations/operations_runbook.md") {
      state.runbookRead = true;
    }
    let verdict: Verdict;
    if ((event.toolName === "bash" || event.toolName === "powershell") && typeof input.command === "string") {
      verdict = commandVerdict(root, input.command, state);
    } else if ((event.toolName === "write" || event.toolName === "edit") && typeof input.path === "string") {
      verdict = writeVerdict(root, ctx.cwd, input.path, state);
    }
    if (verdict === undefined) return undefined;
    if ("block" in verdict) {
      return block(state, {
        tool: event.toolName,
        command: typeof input.command === "string" ? input.command : undefined,
        path: typeof input.path === "string" ? input.path : undefined,
        rule: "policy",
        reason: verdict.reason,
      });
    }
    if (ctx.hasUI && (await ctx.ui.confirm("AITradingSystem zone C", verdict.confirm))) {
      if (verdict.path) {
        state.confirmedCPaths.add(verdict.path);
        state.cZoneConfirmations.push(`${verdict.path}: ui-confirmed`);
      }
      return undefined;
    }
    state.locked = true;
    return block(state, { tool: event.toolName, path: verdict.path, rule: "c-zone-denied", reason: LOCK_REASON });
  });

  pi.on("tool_result", async (event) => {
    const root = findRepoRoot(process.cwd());
    if (root === undefined) return undefined;
    const input = (event.input ?? {}) as Record<string, unknown>;
    const command = typeof input.command === "string" ? input.command : undefined;
    const output = textContent(event as { content?: unknown });
    if ((event.toolName === "bash" || event.toolName === "powershell") && command) {
      recordCommandResult(state, command, output, event.isError);
      const unconfirmed = detectUnconfirmedCChanges(root, state);
      if (unconfirmed.length > 0) {
        state.locked = true;
        state.blocked.push({ tool: event.toolName, command, path: unconfirmed.join(", "), rule: "c-zone-status", reason: LOCK_REASON });
      }
    }
    if (output) {
      const truncated = truncateToolResult(root, event.toolCallId, output);
      if (truncated) return { content: [{ type: "text", text: truncated }] };
    }
    return undefined;
  });

  pi.on("agent_settled", async () => {
    const root = findRepoRoot(process.cwd());
    if (root !== undefined) writeReport(root, state);
  });
}
