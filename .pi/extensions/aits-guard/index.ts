// AITradingSystem guard rails for the pi coding agent (GOV-008 P6).
//
// pi has no permission system of its own: tools run with the user's OS permissions. This extension
// blocks the repository actions AGENTS.md forbids for an agent and asks before edits to zone C paths.
// It is a guard rail against mistakes, not a security boundary: a determined command can get around
// pattern checks. The real controls stay the ship gate, the zone C owner decision, and git history.
import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from "node:path";

import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

type BlockVerdict = { block: true; reason: string; rule: string; terminate?: boolean };
type ConfirmVerdict = { confirm: string; path?: string };
type Verdict = BlockVerdict | ConfirmVerdict | undefined;
type BlockRecord = { tool?: string; command?: string; path?: string; rule: string; reason: string };
type TestRecord = { command: string; result: string; summary: string };
type ShipRecord = { mode: string; ok: boolean; sha: string; summary: string };
type RunState = {
  locked: boolean;
  blocked: BlockRecord[];
  cZoneConfirmations: string[];
  confirmedCPaths: Set<string>;
  cZoneBaseline?: Set<string>;
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
const HEAD_CHARS = 4_000;
const TAIL_CHARS = 8_000;
const OWNER_DECISION_ID = /^owner_decision:[A-Za-z0-9_.-]+:\d{4}-\d{2}-\d{2}:[A-Za-z0-9_.-]+$/;
const RUN_ARTIFACT_FILES = new Set(["report.json", "checks.json", "acceptance.json", "publish.json"]);

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

function normalizedAbsolute(path: string): string {
  return resolve(path).split(sep).join("/").toLowerCase();
}

function isRunArtifactPath(target: string, cwd = process.cwd()): boolean {
  const runDir = process.env.AITS_PI_RUN_DIR;
  if (!runDir) return false;
  const absolute = isAbsolute(target) ? target : join(cwd, target);
  const artifact = basename(absolute).toLowerCase();
  if (!RUN_ARTIFACT_FILES.has(artifact)) return false;
  const rel = relative(runDir, absolute);
  return rel !== "" && !rel.startsWith("..") && !isAbsolute(rel);
}

function hasRunArtifactWrite(command: string, cwd = process.cwd()): boolean {
  const runDir = process.env.AITS_PI_RUN_DIR;
  if (!runDir) return false;
  const runPrefix = normalizedAbsolute(runDir).replace(/\/+$/, "");
  const filePattern = String.raw`(?:report|checks|acceptance|publish)\.json`;
  const absPattern = escapeRegExp(runPrefix).replace(/\//g, String.raw`[\\/]`) + String.raw`[\\/][^\s'"|;&<>]*` + filePattern;
  const quotedAbs = String.raw`["']?` + absPattern + String.raw`["']?`;
  const relFile = String.raw`["']?(?:\.\.?[\\/])?(?:[^\s'"|;&<>]+[\\/])*` + filePattern + String.raw`["']?`;
  const normalized = command.replace(/\\/g, "/").toLowerCase();
  const target = String.raw`(?:` + quotedAbs + String.raw`|` + relFile + String.raw`)`;
  const writePatterns = [
    new RegExp(String.raw`(?:^|\s)(?:>|>>|tee\b)[^\n;&|]*` + target, "i"),
    new RegExp(String.raw`\b(?:Set-Content|Add-Content|Out-File|New-Item|Copy-Item|Move-Item|Remove-Item|cp|mv|rm|del|copy|move)\b[^\n;&|]*` + target, "i"),
  ];
  return writePatterns.some((pattern) => pattern.test(normalized));
}

function ownerDecisionVerified(root: string, decision = process.env.AITS_PI_OWNER_DECISION ?? ""): boolean {
  if (!OWNER_DECISION_ID.test(decision)) return false;
  try {
    execFileSync("git", ["grep", "-F", "-e", decision, "main", "--", "docs/requirements"], {
      cwd: root,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    });
    return true;
  } catch {
    return false;
  }
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

function firstShellWord(command: string): string {
  const match = /^\s*(?:env\s+)?((?:["'][^"']+["']|\S)+)/.exec(command);
  return (match?.[1] ?? "").replace(/^['"]|['"]$/g, "");
}

function isShellInterpreter(word: string): boolean {
  const name = basename(word).toLowerCase().replace(/\.exe$/, "");
  return ["bash", "sh", "zsh", "pwsh", "powershell"].includes(name);
}

function stripHeredocBodies(command: string): string {
  const receiver = firstShellWord(command);
  if (isShellInterpreter(receiver)) return command;
  const lines = command.split(/\r?\n/);
  const out: string[] = [];
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    out.push(line);
    const matches = [...line.matchAll(/<<(-)?\s*(?!<)(?:'([^']+)'|"([^"]+)"|([A-Za-z_][A-Za-z0-9_]*))/g)];
    if (matches.length === 0) continue;
    for (const match of matches) {
      const allowTabs = match[1] === "-";
      const marker = match[2] ?? match[3] ?? match[4] ?? "";
      i += 1;
      while (i < lines.length) {
        const candidate = allowTabs ? lines[i].replace(/^\t+/, "") : lines[i];
        if (candidate === marker) break;
        i += 1;
      }
    }
  }
  return out.join("\n");
}

function splitShellCommands(command: string): string[] {
  return stripHeredocBodies(command).split(/\s*(?:&&|\|\|;|\r?\n)\s*/).filter(Boolean);
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

function isAitsOpsCommand(part: string): boolean {
  return /(?:^|\s)(?:\.venv[\\/]Scripts[\\/]aits(?:\.exe)?|aits(?:\.exe)?)\s+ops\b/.test(part);
}

export function commandVerdict(root: string, command: string, state = makeState()): Verdict {
  if (state.locked) return { block: true, reason: LOCK_REASON, rule: "c-zone-lock", terminate: true };
  for (const [pattern, label] of FORBIDDEN_COMMANDS) {
    if (pattern.test(command)) return { block: true, rule: label, reason: `blocked by AGENTS.md: ${label}` };
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
      return {
        block: true,
        rule: "excluded-file",
        reason: `${excluded} is an unrelated dirty file: never open or modify it`,
      };
    }
  }
  if (hasRunArtifactWrite(command, root)) {
    return { block: true, rule: "run-artifact", reason: "run artifacts are written by guard, accept_checks, acceptance, or publish scripts" };
  }
  for (const part of splitShellCommands(command)) {
    if (/^\s*(python|py|pytest)(\s|$)/i.test(part)) {
      return {
        block: true,
        rule: "system-python",
        reason: "use the project venv interpreter: .venv/Scripts/python.exe (for pytest: .venv/Scripts/python.exe -m pytest ...)",
      };
    }
    if (isWholeRepoGitCommand(part) && !hasAllExclusions(part, policy.exclusions)) {
      return {
        block: true,
        rule: "git-whole-repo",
        reason: "repository-wide git status/diff/add must include tree_clean_exclusions as exact exclude pathspecs",
      };
    }
    if (isAitsOpsCommand(part) && !state.runbookRead) {
      return {
        block: true,
        rule: "runbook-before-ops",
        reason: "read docs/operations/operations_runbook.md before running aits ops commands",
      };
    }
    if (/\btools\/tasks\.py\s+set\b/.test(part) || /\btools\\tasks\.py\s+set\b/.test(part)) {
      if (/\bnotes=/.test(part)) {
        return { block: true, rule: "tasks-set-notes", reason: "use tools/tasks.py append-note instead of set notes=" };
      }
      if (/\bstatus=DONE\b/.test(part) && process.env.AITS_PI_ALLOW_DONE !== "1") {
        return { block: true, rule: "tasks-done-flag", reason: "status=DONE requires AITS_PI_ALLOW_DONE=1" };
      }
    }
  }
  return undefined;
}

export function checkCommand(root: string, command: string): Verdict {
  return commandVerdict(root, command);
}

export function writeVerdict(root: string, cwd: string, target: string, state = makeState()): Verdict {
  if (state.locked) return { block: true, reason: LOCK_REASON, rule: "c-zone-lock", terminate: true };
  if (isRunArtifactPath(target, cwd)) {
    return { block: true, rule: "run-artifact", reason: "run artifacts are written by guard, accept_checks, acceptance, or publish scripts" };
  }
  const rel = repoRelative(root, cwd, target);
  if (rel === undefined) return undefined;
  const policy = readShipPolicy(root);
  if (policy.exclusions.includes(rel)) {
    return {
      block: true,
      rule: "excluded-file",
      reason: `${rel} is an unrelated dirty file (tree_clean_exclusions): never modify it`,
    };
  }
  if (matchesAny(rel, NEVER_WRITE)) {
    return { block: true, rule: "never-write", reason: `${rel} is protected (rendered view or pinned research evidence)` };
  }
  if (matchesAny(rel, policy.zoneC)) {
    if (ownerDecisionVerified(root)) {
      state.confirmedCPaths.add(rel);
      state.cZoneConfirmations.push(`${rel}: AITS_PI_OWNER_DECISION`);
      return undefined;
    }
    if ((process.env.AITS_PI_OWNER_DECISION ?? "") !== "") {
      state.locked = true;
      return { block: true, rule: "owner-decision-missing", reason: LOCK_REASON, terminate: true };
    }
    return { confirm: `${rel} is a zone C path: the change needs an owner decision. Allow this edit?`, path: rel };
  }
  return undefined;
}

export function checkWrite(root: string, cwd: string, target: string): Verdict {
  return writeVerdict(root, cwd, target);
}

function gitPathspecArgs(root: string): string[] {
  return ["--", ".", ...readShipPolicy(root).exclusions.map((excluded) => `:(exclude,literal)${excluded}`)];
}

function normalizeRel(path: string): string {
  return path.split(sep).join("/");
}

function statusChangedFiles(root: string): string[] {
  const out = execFileSync("git", ["status", "--porcelain=v1", "-z", "--untracked-files=all", ...gitPathspecArgs(root)], {
    cwd: root,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "ignore"],
  });
  const items = out.split("\0").filter(Boolean);
  const paths: string[] = [];
  for (let i = 0; i < items.length; i++) {
    const item = items[i];
    const code = item.slice(0, 2);
    const path = item.slice(3);
    if (path) paths.push(normalizeRel(path));
    if (code.includes("R") || code.includes("C")) i += 1;
  }
  return paths;
}

function committedChangedFiles(root: string): string[] {
  const out = execFileSync("git", ["diff", "--name-only", "-z", "main...HEAD", ...gitPathspecArgs(root)], {
    cwd: root,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "ignore"],
  });
  return out.split("\0").filter(Boolean).map(normalizeRel);
}

function changedFiles(root: string): string[] {
  const paths = new Set<string>();
  try {
    for (const path of committedChangedFiles(root)) paths.add(path);
  } catch {
    // main may not exist in non-git test fixtures.
  }
  try {
    for (const path of statusChangedFiles(root)) paths.add(path);
  } catch {
    // not a git repository or status failed.
  }
  return [...paths].sort();
}

function zoneFor(root: string, rel: string): "A" | "B" | "C" {
  const policy = readShipPolicy(root);
  if (matchesAny(rel, policy.zoneC)) return "C";
  if (matchesAny(rel, policy.zoneA)) return "A";
  return "B";
}

export function initializeCZoneBaseline(root: string, state: RunState): void {
  if (state.cZoneBaseline !== undefined) return;
  state.cZoneBaseline = new Set(changedFiles(root).filter((rel) => zoneFor(root, rel) === "C"));
}

export function detectUnconfirmedCChanges(root: string, state: RunState): string[] {
  const baseline = state.cZoneBaseline ?? new Set<string>();
  return changedFiles(root).filter(
    (rel) => zoneFor(root, rel) === "C" && !state.confirmedCPaths.has(rel) && !baseline.has(rel),
  );
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

function limitedHeadAndTail(content: string): { head: string; tail: string } {
  const lines = content.split(/\r?\n/);
  const head = lines.slice(0, 40).join("\n").slice(0, HEAD_CHARS);
  const tailLines = lines.slice(Math.max(40, lines.length - 80)).join("\n");
  const tail = tailLines.slice(Math.max(0, tailLines.length - TAIL_CHARS));
  return { head, tail };
}

export function truncateToolResult(root: string, toolCallId: string, content: string): { text: string; path: string } | undefined {
  if (content.length <= MAX_TOOL_RESULT_CHARS) return undefined;
  const dir = runOutputDir(root);
  mkdirSync(dir, { recursive: true });
  const path = join(dir, `tool-${toolCallId.replace(/[^A-Za-z0-9_.-]/g, "_")}.txt`);
  writeFileSync(path, content, "utf8");
  const { head, tail } = limitedHeadAndTail(content);
  return {
    path,
    text: `${head}\n\n[... full output written to ${path}; showing first 40 and last 80 lines ...]\n\n${tail}`,
  };
}

function stripEnvAssignments(part: string): string {
  return part.trim().replace(/^(?:[A-Za-z_][A-Za-z0-9_]*=[^\s]+\s+)*/, "").trim();
}

function commandWords(part: string): string[] {
  return stripEnvAssignments(part).match(/"[^"]*"|'[^']*'|\S+/g)?.map((word) => word.replace(/^['"]|['"]$/g, "")) ?? [];
}

function isPythonWord(word: string): boolean {
  const normalized = word.replace(/\\/g, "/").toLowerCase();
  const name = basename(normalized);
  return word === "python" || word === "python3" || word === "py" || name === "python.exe";
}

function isTestCommandPart(part: string): boolean {
  const words = commandWords(part);
  if (words.length === 0) return false;
  const first = basename(words[0].replace(/\\/g, "/")).toLowerCase();
  if (first === "pytest" || first === "ruff") return true;
  if (first === "node") return words.includes("--test");
  if (!isPythonWord(words[0])) return false;
  if (words[1] === "-m" && ["pytest", "ruff"].includes(words[2] ?? "")) return true;
  const script = (words[1] ?? "").replace(/\\/g, "/");
  return script === "tools/tasks.py" && ["check", "render"].includes(words[2] ?? "");
}

function recordCommandResult(state: RunState, command: string, output: string | undefined, isError: boolean, root: string): void {
  const result = isError ? "failed" : "passed";
  const summary = (output ?? "").split(/\r?\n/).slice(-8).join("\n").trim();
  if (splitShellCommands(command).some(isTestCommandPart)) {
    state.tests.push({ command, result, summary });
  }
  if (/tools[\\/]gov008[\\/]ship\.py\b/.test(command)) {
    const mode = /--dry-run\b/.test(command) ? "dry-run" : /--execute\b/.test(command) ? "execute" : /--gate\b/.test(command) ? "gate" : /--publish\b/.test(command) ? "publish" : "unknown";
    state.ship = { mode, ok: !isError, sha: currentSha(root), summary };
  }
}

function currentSha(root: string): string {
  try {
    return execFileSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).trim();
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

function block(state: RunState, record: BlockRecord, terminate = false): { block: true; reason: string; terminate?: true } {
  state.blocked.push(record);
  return terminate ? { block: true, reason: record.reason, terminate: true } : { block: true, reason: record.reason };
}

function isBlockVerdict(verdict: Verdict): verdict is BlockVerdict {
  return verdict !== undefined && "block" in verdict;
}

export default function (pi: ExtensionAPI) {
  const state = makeState();

  pi.on("session_start", async (_event, ctx) => {
    const root = findRepoRoot(ctx.cwd);
    if (root !== undefined) initializeCZoneBaseline(root, state);
  });

  pi.on("tool_call", async (event, ctx) => {
    const root = findRepoRoot(ctx.cwd);
    if (root === undefined) return undefined; // not inside this repository
    initializeCZoneBaseline(root, state);
    const input = (event.input ?? {}) as Record<string, unknown>;
    if (state.locked && !READ_ONLY_WHEN_LOCKED.has(event.toolName)) {
      return block(state, { tool: event.toolName, rule: "c-zone-lock", reason: LOCK_REASON }, true);
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
    if (isBlockVerdict(verdict)) {
      return block(
        state,
        {
          tool: event.toolName,
          command: typeof input.command === "string" ? input.command : undefined,
          path: typeof input.path === "string" ? input.path : undefined,
          rule: verdict.rule,
          reason: verdict.reason,
        },
        verdict.terminate === true,
      );
    }
    if (ctx.hasUI && (await ctx.ui.confirm("AITradingSystem zone C", verdict.confirm))) {
      if (verdict.path) {
        state.confirmedCPaths.add(verdict.path);
        state.cZoneConfirmations.push(`${verdict.path}: ui-confirmed`);
      }
      return undefined;
    }
    state.locked = true;
    return block(state, { tool: event.toolName, path: verdict.path, rule: "c-zone-denied", reason: LOCK_REASON }, true);
  });

  pi.on("tool_result", async (event, ctx) => {
    const root = findRepoRoot(ctx.cwd);
    if (root === undefined) return undefined;
    const input = (event.input ?? {}) as Record<string, unknown>;
    const command = typeof input.command === "string" ? input.command : undefined;
    const output = textContent(event as { content?: unknown });
    if ((event.toolName === "bash" || event.toolName === "powershell") && command) {
      recordCommandResult(state, command, output, event.isError, root);
      const unconfirmed = detectUnconfirmedCChanges(root, state);
      if (unconfirmed.length > 0) {
        state.locked = true;
        state.blocked.push({
          tool: event.toolName,
          command,
          path: unconfirmed.join(", "),
          rule: "c-zone-status",
          reason: LOCK_REASON,
        });
      }
      if (output) {
        const truncated = truncateToolResult(root, event.toolCallId, output);
        if (truncated) {
          const result: Record<string, unknown> = { content: [{ type: "text", text: truncated.text }] };
          if (event.structuredContent !== undefined) {
            result.structuredContent = { truncated: true, full_output_path: truncated.path };
          }
          return result;
        }
      }
    }
    return undefined;
  });

  pi.on("agent_settled", async (_event, ctx) => {
    const root = findRepoRoot(ctx.cwd);
    if (root !== undefined) writeReport(root, state);
  });
}
