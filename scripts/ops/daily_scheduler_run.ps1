<#
.SYNOPSIS
OPS-082 scheduled daily run: one call of the runtime checkout's `aits ops daily-run`.

.DESCRIPTION
The Windows Task Scheduler task registered by register_daily_scheduler_task.ps1 runs this script
from the runtime checkout (owner_decision:OPS-082:2026-10-11:deterministic_scheduler_v1). The
runtime checkout is the repository two levels above this file.

Order, each step fail-closed:
  1. Precheck: a checkout is clean with HEAD reachable from local origin/main; a git-free
     release passes RELEASE.json verification and uses a separate absolute AITS_STATE_ROOT.
     .venv\Scripts\aits.exe must exist. Failure skips daily-run with a control-plane blocker.
  2. Environment: only the variables daily-run reads (see $DailyRunEnvironment). A missing process
     value is taken from the user environment scope. Values are never printed or logged; the log
     records PRESENT or MISSING per name.
  3. Exactly one call of `<runtime>\.venv\Scripts\aits.exe ops daily-run`. A duplicate trigger for
     an as_of that already has a terminal state is refused by the daily-run run control itself. The
     rescue window adds --retry-after-source-failure (OPS-085): daily-run then retries the as_of
     once, from the capture step, when its first run failed only on a temporary data-source
     failure; any other terminal state is refused as before.
  4. Exit code and output go to <state-root>\outputs\run_control\scheduler\<date>_<window>.log
     (state-root is AITS_STATE_ROOT when set, otherwise the runtime directory).
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
# OPS-082 2026-10-11): FMP, SEC user agent, OpenAI (score-daily risk-event precheck),
# Congress.gov and GovInfo (official policy sources). Marketstack is no longer a daily source
# (DATA-022, 2026-10-12). Nothing reads the retired Codex automation contract
# (AITS_EXTERNAL_SCHEDULER, AITS_OPS_*), so it is not set.
$DailyRunEnvironment = @(
    'FMP_API_KEY',
    'SEC_USER_AGENT',
    'OPENAI_API_KEY',
    'CONGRESS_API_KEY',
    'GOVINFO_API_KEY'
)
$OriginMainRef = 'refs/remotes/origin/main'
# The 17:30 trigger is the same task's second start time; anything before 15:00 Tokyo is the
# 12:00 primary window (OPS-084), including a late start after the PC wakes. Run control
# deduplicates by as_of; the window only sets the summary label and whether daily-run may retry
# a temporary data-source failure (OPS-085).
$RescueWindowStartHour = 15

$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)
$RuntimeRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$Aits = Join-Path $RuntimeRoot '.venv\Scripts\aits.exe'
$Python = Join-Path $RuntimeRoot '.venv\Scripts\python.exe'
$SummaryScript = Join-Path $RuntimeRoot 'scripts\ops\daily_scheduler_summary.py'
$ReleaseManifest = Join-Path $RuntimeRoot 'RELEASE.json'
$precheckBlocker = $null
$StateRoot = $RuntimeRoot
$stateRootValue = [Environment]::GetEnvironmentVariable('AITS_STATE_ROOT', 'Process')
if (-not [string]::IsNullOrWhiteSpace($stateRootValue)) {
    if ($stateRootValue -match '^([A-Za-z]:\\|\\\\)') {
        $StateRoot = [System.IO.Path]::GetFullPath($stateRootValue).TrimEnd('\')
    } else {
        $precheckBlocker = 'STATE_ROOT_INVALID'
    }
}
$OutputDir = Join-Path $StateRoot 'outputs\run_control\scheduler'

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

function Invoke-ReleaseVerify {
    # Match Invoke-Git's native stderr handling on Windows PowerShell 5.1.
    $ErrorActionPreference = 'Continue'
    $output = & $Python -I -m ai_trading_system.release_identity verify --root $RuntimeRoot 2>&1
    return [pscustomobject]@{ ExitCode = $LASTEXITCODE; Output = @($output | ForEach-Object { "$_" }) }
}

function Get-VerifyValue([string[]]$Lines, [string]$Name) {
    $prefix = $Name + '='
    $line = @($Lines | Where-Object { $_.StartsWith($prefix) } | Select-Object -First 1)
    if ($line.Count -gt 0) { return $line[0].Substring($prefix.Length).Trim() }
    return $null
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

if ($StateRoot -ne $RuntimeRoot) {
    Add-LogText $logPath ("state_root={0}`n" -f $StateRoot)
}
$runtimeHead = $null
$releaseId = $null
if ($precheckBlocker -eq 'STATE_ROOT_INVALID') {
    # Keep the invalid-state-root blocker and write the log under the runtime directory.
} elseif (Test-Path -LiteralPath (Join-Path $RuntimeRoot '.git')) {
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
} elseif (Test-Path -LiteralPath $ReleaseManifest) {
    if ($StateRoot -eq $RuntimeRoot) {
        $precheckBlocker = 'STATE_ROOT_NOT_SET'
    } elseif (-not (Test-Path -LiteralPath $Python)) {
        $precheckBlocker = 'AITS_EXECUTABLE_MISSING'
    } else {
        $verify = Invoke-ReleaseVerify
        Add-LogText $logPath ("release_verify_exit_code={0}`n" -f $verify.ExitCode)
        if ($verify.ExitCode -eq 0) {
            $runtimeHead = Get-VerifyValue $verify.Output 'source_commit'
            $releaseId = Get-VerifyValue $verify.Output 'release_id'
            if (-not $runtimeHead -or -not $releaseId) {
                $precheckBlocker = 'RUNTIME_RELEASE_INVALID'
            } else {
                Add-LogText $logPath ("release_id={0}`nruntime_head={1}`n" -f $releaseId, $runtimeHead)
            }
        } elseif ($verify.ExitCode -eq 1) {
            $precheckBlocker = 'RUNTIME_RELEASE_MODIFIED'
            $mismatchCount = Get-VerifyValue $verify.Output 'mismatches'
            Add-LogText $logPath ("release_mismatches={0}`n" -f $mismatchCount)
        } else {
            $precheckBlocker = 'RUNTIME_RELEASE_INVALID'
        }
    }
} else {
    $precheckBlocker = 'RUNTIME_IDENTITY_MISSING'
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
    $dailyRunArguments = @('ops', 'daily-run')
    # OPS-085 (owner_decision:OPS-085:2026-10-13:rescue_retry_after_source_failure): only the rescue
    # window asks for the one automatic retry; daily-run checks the evidence and decides.
    if ($Window -eq 'RESCUE') { $dailyRunArguments += '--retry-after-source-failure' }
    Add-LogText $logPath ("command={0} {1}`ndaily_run_invoked=true`n" -f $Aits, ($dailyRunArguments -join ' '))
    try {
        $process = Start-Process -FilePath $Aits -ArgumentList $dailyRunArguments `
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
    '--state-root', $StateRoot,
    '--output-stem', $stem, '--started-at', $startedAt, '--finished-at', $finishedAt)
if ($releaseId) { $summaryArgs += @('--release-id', $releaseId) }
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
