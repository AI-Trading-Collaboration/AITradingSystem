<#
.SYNOPSIS
OPS-082: register the single Windows scheduled task that starts the daily run.

.DESCRIPTION
owner_decision:OPS-082:2026-10-11:deterministic_scheduler_v1: Windows Task Scheduler is the only
external scheduling entry. One task, two daily triggers (09:30 and 17:30 Asia/Tokyo; the second is
the same-day rescue time, and daily-run's run control deduplicates by as_of). The task runs as the
current user, only while that user is logged on, with no stored password and without elevation;
StartWhenAvailable; no parallel instance.

Idempotent: Register-ScheduledTask -Force replaces a task with the same path and name, so running
this script again never creates a second task. It refuses when another scheduled task already
starts the daily run, so there is only ever one scheduling source.

The action starts Windows PowerShell through `conhost.exe --headless`, so no console window opens.
conhost does not forward the child's exit code: the task's last run result only says the wrapper
started. The run outcome is in <runtime>\outputs\run_control\scheduler\<date>_<window>.log and the
summary files next to it.

This file is ASCII on purpose (Windows PowerShell 5.1 reads BOM-less scripts as ANSI).
#>
[CmdletBinding()]
param(
    [string]$RuntimeRoot = 'D:\Work\AITradingSystem_ops_runtime',
    [string]$TaskName = 'AITradingSystem Daily Run',
    [string]$TaskPath = '\'
)

Set-StrictMode -Version 3
$ErrorActionPreference = 'Stop'

$PrimaryTime = '09:30'
$RescueTime = '17:30'
$TriggerTimeZoneId = 'Tokyo Standard Time'
# Upper bound for one wrapper run; a normal daily run takes well under an hour. Operational
# timeout, not a policy threshold. An interrupted run is resumed by daily-run's run control.
$ExecutionTimeLimit = New-TimeSpan -Hours 6

if ((Get-TimeZone).Id -ne $TriggerTimeZoneId) {
    throw ("Trigger times are Asia/Tokyo but the host time zone is {0}; refusing to register." -f (Get-TimeZone).Id)
}
$root = (Resolve-Path -LiteralPath $RuntimeRoot).Path
$wrapper = Join-Path $root 'scripts\ops\daily_scheduler_run.ps1'
$aits = Join-Path $root '.venv\Scripts\aits.exe'
foreach ($required in @($wrapper, $aits)) {
    if (-not (Test-Path -LiteralPath $required)) { throw "Runtime checkout is missing $required" }
}

$others = @(Get-ScheduledTask | Where-Object {
    -not ($_.TaskName -eq $TaskName -and $_.TaskPath -eq $TaskPath)
} | Where-Object {
    $text = (@($_.Actions) | ForEach-Object {
        $execute = $_.PSObject.Properties['Execute']
        $arguments = $_.PSObject.Properties['Arguments']
        '{0} {1}' -f $(if ($execute) { $execute.Value }), $(if ($arguments) { $arguments.Value })
    }) -join ' '
    $text -match 'daily_scheduler_run\.ps1|ops daily-run'
})
if ($others.Count -gt 0) {
    $names = ($others | ForEach-Object { $_.TaskPath + $_.TaskName }) -join ', '
    throw "Another scheduled task already starts the daily run: $names. Only one scheduling source is allowed."
}

$conhost = Join-Path $env:SystemRoot 'System32\conhost.exe'
$powershell = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
$arguments = '--headless "{0}" -NoProfile -NonInteractive -File "{1}"' -f $powershell, $wrapper
$action = New-ScheduledTaskAction -Execute $conhost -Argument $arguments -WorkingDirectory $root
$triggers = @(
    (New-ScheduledTaskTrigger -Daily -At $PrimaryTime),
    (New-ScheduledTaskTrigger -Daily -At $RescueTime)
)
$user = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit $ExecutionTimeLimit -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
$description = ('OPS-082 deterministic daily scheduler: one call of aits ops daily-run in {0} ' +
    'via scripts\ops\daily_scheduler_run.ps1. Outcome: outputs\run_control\scheduler\. ' +
    'production_effect=none; broker_action=none.') -f $root

Register-ScheduledTask -TaskName $TaskName -TaskPath $TaskPath -Action $action -Trigger $triggers `
    -Principal $principal -Settings $settings -Description $description -Force | Out-Null

$task = Get-ScheduledTask -TaskName $TaskName -TaskPath $TaskPath
[pscustomobject]@{
    task = $task.TaskPath + $task.TaskName
    state = [string]$task.State
    user = $task.Principal.UserId
    logon_type = [string]$task.Principal.LogonType
    run_level = [string]$task.Principal.RunLevel
    triggers = (@($task.Triggers) | ForEach-Object { $_.StartBoundary }) -join ', '
    start_when_available = $task.Settings.StartWhenAvailable
    multiple_instances = [string]$task.Settings.MultipleInstances
    execute = (@($task.Actions) | ForEach-Object { $_.Execute + ' ' + $_.Arguments }) -join ' | '
} | Format-List
