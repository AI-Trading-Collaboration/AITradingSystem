// AITradingSystem guard rails for the pi coding agent (GOV-008 P6).
//
// pi has no permission system of its own: tools run with the user's OS permissions. This extension
// blocks the repository actions AGENTS.md forbids for an agent and asks before edits to zone C paths.
// It is a guard rail against mistakes, not a security boundary: a determined command can get around
// pattern checks. The real controls stay the ship gate, the zone C owner decision, and git history.
import { existsSync, readFileSync } from "node:fs";
import { dirname, isAbsolute, join, relative, sep } from "node:path";

import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

type Verdict = { block: true; reason: string } | { confirm: string } | undefined;

// Paths that must never be written by the agent, whatever the ship policy says.
const NEVER_WRITE: readonly string[] = [
  "docs/task_register.md", // rendered by tools/tasks.py
  "docs/task_register_completed.md", // rendered by tools/tasks.py
  "registry/development_tasks/**", // bytes pinned by the TRADING-2548 contract
  "config/etf_portfolio/**", // research evidence (GOV-008 P4 block 2)
  "src/ai_trading_system/etf_portfolio/regime.py", // bytes pinned by the QQQ options contract
];

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

export function findRepoRoot(start: string): string | undefined {
  let dir = start;
  for (;;) {
    if (existsSync(join(dir, "config", "gov008_ship.yaml"))) return dir;
    const parent = dirname(dir);
    if (parent === dir) return undefined;
    dir = parent;
  }
}

function readShipPolicy(root: string): { exclusions: string[]; zoneC: string[] } {
  const path = join(root, "config", "gov008_ship.yaml");
  if (!existsSync(path)) return { exclusions: [], zoneC: [] };
  const exclusions: string[] = [];
  const zoneC: string[] = [];
  let section: "exclusions" | "zoneC" | undefined;
  for (const raw of readFileSync(path, "utf8").split(/\r?\n/)) {
    if (/^tree_clean_exclusions:\s*$/.test(raw)) section = "exclusions";
    else if (/^\s{2}C:\s*$/.test(raw)) section = "zoneC";
    else if (/^\S/.test(raw) || /^\s{2}[A-Za-z]+:\s*$/.test(raw)) section = undefined;
    const item = /^\s*-\s+"?([^"#]+?)"?\s*(#.*)?$/.exec(raw);
    if (item && section === "exclusions") exclusions.push(item[1]);
    if (item && section === "zoneC") zoneC.push(item[1]);
  }
  return { exclusions, zoneC };
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

export function checkWrite(root: string, cwd: string, target: string): Verdict {
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
    return { confirm: `${rel} is a zone C path: the change needs an owner decision. Allow this edit?` };
  }
  return undefined;
}

export function checkCommand(root: string, command: string): Verdict {
  for (const [pattern, label] of FORBIDDEN_COMMANDS) {
    if (pattern.test(command)) return { block: true, reason: `blocked by AGENTS.md: ${label}` };
  }
  for (const excluded of readShipPolicy(root).exclusions) {
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
  return undefined;
}

function escapeRegExp(text: string): string {
  return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

export default function (pi: ExtensionAPI) {
  pi.on("tool_call", async (event, ctx) => {
    const root = findRepoRoot(ctx.cwd);
    if (root === undefined) return undefined; // not inside this repository
    const input = (event.input ?? {}) as Record<string, unknown>;
    let verdict: Verdict;
    if (event.toolName === "bash" && typeof input.command === "string") {
      verdict = checkCommand(root, input.command);
    } else if ((event.toolName === "write" || event.toolName === "edit") && typeof input.path === "string") {
      verdict = checkWrite(root, ctx.cwd, input.path);
    }
    if (verdict === undefined) return undefined;
    if ("block" in verdict) return verdict;
    if (ctx.hasUI && (await ctx.ui.confirm("AITradingSystem zone C", verdict.confirm))) return undefined;
    return { block: true, reason: `${verdict.confirm} (not approved)` };
  });
}
