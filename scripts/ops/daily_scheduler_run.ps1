<#
.SYNOPSIS
OPS-082 scheduled daily run: one call of the runtime checkout's `aits ops daily-run`.

.DESCRIPTION
The Windows Task Scheduler task registered by register_daily_scheduler_task.ps1 runs this script
from the runtime checkout (owner_decision:OPS-082:2026-10-11:deterministic_scheduler_v1). The
runtime checkout is the repository two levels above this file.

Order, each step fail-closed:
  1. Precheck: the runtime working tree is clean (git status --porcelain), HEAD is reachable from
     the local origin/main ref, and .venv\Scripts\aits.exe exists. On failure daily-run is NOT
     called; the summary records a control-plane blocker.
  2. Environment: only the variables daily-run reads (see $DailyRunEnvironment). A missing process
     value is taken from the user environment scope. Values are never printed or logged; the log
     records PRESENT or MISSING per name.
  3. Exactly one call of `<runtime>\.venv\Scripts\aits.exe ops daily-run`. A duplicate trigger for
     an as_of that already has a terminal state is refused by the daily-run run control itself.
  4. Exit code and output go to <runtime>\outputs\run_control\scheduler\<date>_<window>.log.
  5. scripts\ops\daily_scheduler_summary.py writes the Chinese summary next to the log, by code.
     Notification is these files only.

This file is ASCII on purpose: Windows PowerShell 5.1 reads a BOM-less script as the ANSI code
page. production_effect=none; broker_action=none.
#>
[CmdletBinding()]
param(
    [ValidateSet('AUTO', 'PRIMARY', 'RESCUE')]
    [string]$Window = 'AUTO'
)

Set-StrictMode -Version 3
$ErrorActionPreference = 'Stop'

# Variables `aits ops daily-run` and the commands it starts actually read (grep of src,
# OPS-082 2026-10-11): FMP, Marketstack, SEC user agent, OpenAI (score-daily risk-event
# precheck), Congress.gov and GovInfo (official policy sources). Nothing reads the retired
# Codex automation contract (AITS_EXTERNAL_SCHEDULER, AITS_OPS_*), so it is not set.
$DailyRunEnvironment = @(
    'FMP_API_KEY',
    'MARKETSTACK_API_KEY',
    'SEC_USER_AGENT',
    'OPENAI_API_KEY',
    'CONGRESS_API_KEY',
    'GOVINFO_API_KEY'
)
$OriginMainRef = 'refs/remotes/origin/main'
# The 17:30 trigger is the same task's second start time; anything before 13:00 Tokyo is the
# 09:30 primary window. Display label only; run control deduplicates by as_of.
$RescueWindowStartHour = 13

$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)
$RuntimeRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$Aits = Join-Path $RuntimeRoot '.venv\Scripts\aits.exe'
$Python = Join-Path $RuntimeRoot '.venv\Scripts\python.exe'
$SummaryScript = Join-Path $RuntimeRoot 'scripts\ops\daily_scheduler_summary.py'
$OutputDir = Join-Path $RuntimeRoot 'outputs\run_control\scheduler'

function Get-TokyoNow {
    [TimeZoneInfo]::ConvertTimeBySystemTimeZoneId([DateTime]::UtcNow, 'Tokyo Standard Time')
}

function Get-IsoNow {
    [DateTimeOffset]::Now.ToString('yyyy-MM-ddTHH:mm:ss.fffzzz')
}

function Add-LogText([string]$Path, [string]$Text) {
    [System.IO.File]::AppendAllText($Path, $Text, $Utf8NoBom)
}

function Add-LogFile([string]$Path, [string]$Source, [string]$Label) {
    Add-LogText $Path ("----- {0} -----`n" -f $Label)
    if (Test-Path -LiteralPath $Source) {
        $bytes = [System.IO.File]::ReadAllBytes($Source)
        $stream = [System.IO.File]::Open($Path, [System.IO.FileMode]::Append)
        try { $stream.Write($bytes, 0, $bytes.Length) } finally { $stream.Dispose() }
        Remove-Item -LiteralPath $Source
    }
    Add-LogText $Path "`n"
}

function Invoke-Git([string[]]$Arguments) {
    # Windows PowerShell 5.1 turns native stderr into terminating errors under 'Stop'.
    $ErrorActionPreference = 'Continue'
    $output = & git -C $RuntimeRoot @Arguments 2>&1
    return [pscustomobject]@{ ExitCode = $LASTEXITCODE; Output = @($output | ForEach-Object { "$_" }) }
}

$tokyoNow = Get-TokyoNow
if ($Window -eq 'AUTO') {
    if ($tokyoNow.Hour -lt $RescueWindowStartHour) { $Window = 'PRIMARY' } else { $Window = 'RESCUE' }
}
New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null
$stemName = '{0}_{1}' -f $tokyoNow.ToString('yyyy-MM-dd'), $Window
$suffix = 1
$stem = Join-Path $OutputDir $stemName
while (Test-Path -LiteralPath ($stem + '.log')) {
    $suffix += 1
    $stem = Join-Path $OutputDir ('{0}_{1}' -f $stemName, $suffix)
}
$logPath = $stem + '.log'
$startedAt = Get-IsoNow
Add-LogText $logPath ("ops_daily_scheduler_run log (OPS-082)`nwindow={0}`nstarted_at={1}`nruntime_root={2}`n" -f $Window, $startedAt, $RuntimeRoot)

$precheckBlocker = $null
$runtimeHead = $null
$status = Invoke-Git @('status', '--porcelain=v1', '--untracked-files=normal')
if ($status.ExitCode -ne 0) {
    $precheckBlocker = 'RUNTIME_CHECKOUT_GIT_FAILED'
} elseif (@($status.Output | Where-Object { $_.Trim() }).Count -gt 0) {
    $precheckBlocker = 'RUNTIME_CHECKOUT_DIRTY'
    Add-LogText $logPath ("dirty_entries={0}`n" -f @($status.Output | Where-Object { $_.Trim() }).Count)
}
if (-not $precheckBlocker) {
    $head = Invoke-Git @('rev-parse', '--verify', 'HEAD^{commit}')
    $origin = Invoke-Git @('rev-parse', '--verify', ($OriginMainRef + '^{commit}'))
    if ($head.ExitCode -ne 0 -or $origin.ExitCode -ne 0) {
        $precheckBlocker = 'RUNTIME_CHECKOUT_GIT_FAILED'
    } else {
        $runtimeHead = $head.Output[0].Trim()
        Add-LogText $logPath ("runtime_head={0}`norigin_main={1}`n" -f $runtimeHead, $origin.Output[0].Trim())
        $ancestor = Invoke-Git @('merge-base', '--is-ancestor', $runtimeHead, $OriginMainRef)
        if ($ancestor.ExitCode -eq 1) {
            $precheckBlocker = 'RUNTIME_CHECKOUT_HEAD_NOT_ON_ORIGIN_MAIN'
        } elseif ($ancestor.ExitCode -ne 0) {
            $precheckBlocker = 'RUNTIME_CHECKOUT_GIT_FAILED'
        }
    }
}
if (-not $precheckBlocker -and -not (Test-Path -LiteralPath $Aits)) {
    $precheckBlocker = 'AITS_EXECUTABLE_MISSING'
}

$exitCode = $null
if ($precheckBlocker) {
    Add-LogText $logPath ("precheck_blocker={0}`ndaily_run_invoked=false`n" -f $precheckBlocker)
} else {
    foreach ($name in $DailyRunEnvironment) {
        $value = [Environment]::GetEnvironmentVariable($name, 'Process')
        if ([string]::IsNullOrWhiteSpace($value)) {
            $value = [Environment]::GetEnvironmentVariable($name, 'User')
            if (-not [string]::IsNullOrWhiteSpace($value)) {
                [Environment]::SetEnvironmentVariable($name, $value, 'Process')
            }
        }
        if ([string]::IsNullOrWhiteSpace($value)) { $presence = 'MISSING' } else { $presence = 'PRESENT' }
        Add-LogText $logPath ("env {0}={1}`n" -f $name, $presence)
        $value = $null
    }
    $stdoutPath = $stem + '.stdout.tmp'
    $stderrPath = $stem + '.stderr.tmp'
    Add-LogText $logPath ("command={0} ops daily-run`ndaily_run_invoked=true`n" -f $Aits)
    try {
        $process = Start-Process -FilePath $Aits -ArgumentList @('ops', 'daily-run') `
            -WorkingDirectory $RuntimeRoot -NoNewWindow -PassThru `
            -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath
        $null = $process.Handle
        $process.WaitForExit()
        $exitCode = $process.ExitCode
    } catch {
        Add-LogText $logPath ("daily_run_start_error={0}`n" -f $_.Exception.Message)
        $precheckBlocker = 'DAILY_RUN_START_FAILED'
    }
    Add-LogFile $logPath $stdoutPath 'daily-run stdout'
    Add-LogFile $logPath $stderrPath 'daily-run stderr'
    Add-LogText $logPath ("daily_run_exit_code={0}`n" -f $exitCode)
}
$finishedAt = Get-IsoNow
Add-LogText $logPath ("finished_at={0}`n" -f $finishedAt)

$summaryArgs = @('-I', $SummaryScript, '--runtime-root', $RuntimeRoot, '--window', $Window,
    '--output-stem', $stem, '--started-at', $startedAt, '--finished-at', $finishedAt)
if ($runtimeHead) { $summaryArgs += @('--runtime-head', $runtimeHead) }
if ($precheckBlocker) {
    $summaryArgs += @('--precheck-blocker', $precheckBlocker)
} else {
    $summaryArgs += @('--log', $logPath)
    if ($null -ne $exitCode) { $summaryArgs += @('--exit-code', "$exitCode") }
}
try {
    $ErrorActionPreference = 'Continue'
    $summaryOutput = & $Python @summaryArgs 2>&1 | ForEach-Object { "$_" }
    Add-LogText $logPath ("----- summary -----`n{0}`nsummary_exit_code={1}`n" -f ($summaryOutput -join "`n"), $LASTEXITCODE)
} catch {
    Add-LogText $logPath ("summary_error={0}`n" -f $_.Exception.Message)
}

# Task Scheduler records this as the last run result: daily-run's own code, 2 when the precheck
# stopped the run before daily-run, 3 when daily-run gave no exit code.
if ($precheckBlocker) { exit 2 }
if ($null -eq $exitCode) { exit 3 }
exit $exitCode
