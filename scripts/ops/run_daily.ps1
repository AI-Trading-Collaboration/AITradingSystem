<#
.SYNOPSIS
OPS-086: start the daily run from the currently activated release bundle.

.DESCRIPTION
Read current.json beside this entry point, validate its pointer and state root, then invoke the
release wrapper with the same window. Invalid pointers are logged without starting a daily run.
This file is ASCII for Windows PowerShell 5.1; error logs use UTF-8 without BOM.
#>
[CmdletBinding()]
param(
    [ValidateSet('AUTO', 'PRIMARY', 'RESCUE')]
    [string]$Window = 'AUTO'
)

Set-StrictMode -Version 3
$ErrorActionPreference = 'Stop'
$DailyRoot = $PSScriptRoot

try {
    $currentPath = Join-Path $DailyRoot 'current.json'
    $current = [System.IO.File]::ReadAllText($currentPath) | ConvertFrom-Json
    if ($null -eq $current) { throw 'current.json is empty.' }
    $schema = $current.PSObject.Properties['schema_version']
    $release = $current.PSObject.Properties['release_id']
    $state = $current.PSObject.Properties['state_root']
    if ($null -eq $schema -or $schema.Value -cne 'aits_daily_current.v1') {
        throw 'current.json schema_version is invalid.'
    }
    if ($null -eq $release -or $release.Value -isnot [string] -or
        $release.Value -cnotmatch '^\d{8}-[0-9a-f]{9}$') {
        throw 'current.json release_id is invalid.'
    }
    if ($null -eq $state -or $state.Value -isnot [string] -or
        $state.Value -notmatch '^([A-Za-z]:\\|\\\\)') {
        throw 'current.json state_root is not an absolute Windows path.'
    }
    $releaseId = $release.Value
    $stateRoot = $state.Value
    $releaseRoot = Join-Path (Join-Path $DailyRoot 'releases') $releaseId
    $wrapper = Join-Path $releaseRoot 'scripts\ops\daily_scheduler_run.ps1'
    if (-not (Test-Path -LiteralPath (Join-Path $releaseRoot 'RELEASE.json') -PathType Leaf)) {
        throw 'Release manifest is missing.'
    }
    if (-not (Test-Path -LiteralPath $wrapper -PathType Leaf)) {
        throw 'Release wrapper is missing.'
    }
} catch {
    $reason = $_.Exception.Message -replace '[\r\n]+', ' '
    $line = '{0} window={1} reason={2}' -f [DateTime]::UtcNow.ToString('o'), $Window, $reason
    $errorLog = Join-Path $DailyRoot 'run_daily_error.log'
    [System.IO.File]::AppendAllText($errorLog, $line + [Environment]::NewLine,
        (New-Object System.Text.UTF8Encoding($false)))
    exit 2
}

$env:AITS_STATE_ROOT = $stateRoot
$env:PYTHONDONTWRITEBYTECODE = '1'
& $wrapper -Window $Window
exit $LASTEXITCODE
