[CmdletBinding()]
param(
    [Parameter(Mandatory=$true, Position=0)][string]$BriefPath,
    [string]$Fix,
    [switch]$Direct,
    [switch]$DryRun,
    [string]$Repo = (Resolve-Path ".").Path,
    [string]$Now,
    [int]$TimeoutSeconds = 7200
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ConfigPath = Join-Path $ScriptDir "agent_config.json"
$Python = Join-Path $Repo ".venv/Scripts/python.exe"
if (-not (Test-Path -LiteralPath $Python)) { $Python = "python" }

function Read-JsonFile([string]$Path) {
    return Get-Content -LiteralPath $Path -Raw -Encoding UTF8 | ConvertFrom-Json
}

function ConvertTo-Hashtable($Object) {
    $h = @{}
    $Object.PSObject.Properties | ForEach-Object { $h[$_.Name] = $_.Value }
    return $h
}

function Test-InDailyWindow($Config, [string]$NowValue) {
    if ($NowValue) { $nowDt = [DateTimeOffset]::Parse($NowValue) } else { $nowDt = [DateTimeOffset]::Now }
    $tz = [TimeZoneInfo]::FindSystemTimeZoneById("Tokyo Standard Time")
    $jst = [TimeZoneInfo]::ConvertTime($nowDt, $tz)
    foreach ($w in $Config.daily_windows_jst) {
        $startParts = $w.start.Split(":")
        $endParts = $w.end.Split(":")
        $start = [DateTimeOffset]::new($jst.Year, $jst.Month, $jst.Day, [int]$startParts[0], [int]$startParts[1], 0, $jst.Offset)
        $end = [DateTimeOffset]::new($jst.Year, $jst.Month, $jst.Day, [int]$endParts[0], [int]$endParts[1], 0, $jst.Offset)
        if ($jst -ge $start -and $jst -le $end) { return @{ in_window=$true; window=("{0}-{1} JST" -f $w.start,$w.end); now=$jst.ToString("yyyy-MM-dd HH:mm zzz") } }
    }
    return @{ in_window=$false; now=$jst.ToString("yyyy-MM-dd HH:mm zzz") }
}

$config = Read-JsonFile $ConfigPath
$briefFull = (Resolve-Path -LiteralPath $BriefPath).Path
$briefJsonText = & $Python (Join-Path $ScriptDir "brief.py") show $briefFull --repo $Repo 2>&1
if ($LASTEXITCODE -ne 0) {
    $briefJsonText | ForEach-Object { Write-Error $_ }
    exit 2
}
$brief = $briefJsonText | ConvertFrom-Json
$window = Test-InDailyWindow $config $Now
if ($window.in_window) {
    Write-Host ("拒绝启动：当前 {0} 落在日报窗口 {1}" -f $window.now, $window.window)
    exit 2
}

$runId = if ($brief.run) { [string]$brief.run } else { [IO.Path]::GetFileNameWithoutExtension($briefFull) }
$runDir = Join-Path $config.runs_root $runId
$attempt = "01"
$purpose = "run"
$promptFile = $briefFull
$sessionFile = $null
if ($Fix) {
    $purpose = "fix"
    $promptFile = (Resolve-Path -LiteralPath $Fix).Path
    if (Test-Path -LiteralPath (Join-Path $runDir "attempts")) {
        $n = (Get-ChildItem -LiteralPath (Join-Path $runDir "attempts") -Directory -ErrorAction SilentlyContinue | Measure-Object).Count + 1
        $attempt = "{0:D2}" -f $n
    } else { $attempt = "02" }
    $latestSession = Get-ChildItem -LiteralPath (Join-Path $runDir "session") -Filter "*.jsonl" -File -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if ($latestSession) { $sessionFile = $latestSession.FullName }
}

$modelKey = [string]$brief.model
$modelId = [string]$config.models.$modelKey
$acceptancePath = Join-Path $runDir "acceptance.json"
$rejectedCount = 0
if (Test-Path -LiteralPath $acceptancePath) {
    $acc = Read-JsonFile $acceptancePath
    $rejectedCount = @($acc.reviews | Where-Object { $_.verdict -eq "rejected" }).Count
}
if ($Fix -and $rejectedCount -ge 2) { $modelId = [string]$config.models.cloud }
$lastExit = $null
$attemptRoot = Join-Path $runDir "attempts"
if (Test-Path -LiteralPath $attemptRoot) {
    $last = Get-ChildItem -LiteralPath $attemptRoot -Directory | Sort-Object Name -Descending | Select-Object -First 1
    if ($last -and (Test-Path -LiteralPath (Join-Path $last.FullName "exit.json"))) { $lastExit = Read-JsonFile (Join-Path $last.FullName "exit.json") }
}
if ($Fix -and $modelKey -eq "local" -and $lastExit -and $lastExit.stopReason -eq "error") { $modelId = [string]$config.models.cloud }
if ($brief.thinking -and $brief.thinking -ne "default") { $modelArg = $modelId + ":" + [string]$brief.thinking } else { $modelArg = $modelId }

$envMap = [ordered]@{
    AITS_PI_RUN_DIR = $runDir
    AITS_PI_RUN = [string]$brief.run
    AITS_PI_TASK = [string]$brief.task
    AITS_PI_ZONE = [string]$brief.zone
    AITS_PI_ALLOWED_PATHS = ([string[]]$brief.allowed_paths -join ";")
}
if ($brief.zone -eq "C") { $envMap.AITS_PI_OWNER_DECISION = [string]$brief.owner_decision }
if ($brief.allow_done -eq $true) { $envMap.AITS_PI_ALLOW_DONE = "1" }

$toolList = if ($brief.tools -eq "readonly") { "read,grep,find,ls" } else { "read,bash,edit,write" }
$piPreview = "pi -p --mode json --model $modelArg --tools $toolList --session-dir $runDir\session"
if ($sessionFile) { $piPreview = "pi -p --mode json --model $modelArg --tools $toolList --session $sessionFile" }

if ($DryRun) {
    Write-Host "DryRun: would run"
    Write-Host $piPreview
    Write-Host "RunDir=$runDir"
    Write-Host "Attempt=$attempt Purpose=$purpose Direct=$Direct"
    foreach ($k in $envMap.Keys) { Write-Host ("{0}={1}" -f $k, $envMap[$k]) }
    exit 0
}

New-Item -ItemType Directory -Force -Path $runDir | Out-Null
Copy-Item -LiteralPath $briefFull -Destination (Join-Path $runDir "brief.md") -Force
[ordered]@{ queued_at = [DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"); brief = $briefFull } | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $runDir "queue.json") -Encoding UTF8

$request = [ordered]@{
    repo = $Repo
    run_dir = $runDir
    attempt = $attempt
    purpose = $purpose
    prompt_file = $promptFile
    model_id = $modelArg
    tools = [string]$brief.tools
    session_file = $sessionFile
    aits_env = $envMap
}

if ($Direct) {
    & (Join-Path $ScriptDir "agent-run.ps1") -Repo $Repo -RunDir $runDir -Attempt $attempt -Purpose $purpose -PromptFile $promptFile -ModelId $modelArg -Tools ([string]$brief.tools) -AitsEnv (ConvertTo-Hashtable ([pscustomobject]$envMap)) -SessionFile $sessionFile
} else {
    $queueFile = [string]$config.queue_file
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $queueFile) | Out-Null
    $request | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $queueFile -Encoding UTF8
    schtasks /Run /TN ([string]$config.scheduled_task_name) | Out-Null
    $exitPath = Join-Path (Join-Path (Join-Path $runDir "attempts") $attempt) "exit.json"
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline -and -not (Test-Path -LiteralPath $exitPath)) { Start-Sleep -Seconds 5 }
    if (-not (Test-Path -LiteralPath $exitPath)) { throw "Timed out waiting for $exitPath" }
}

& $Python (Join-Path $ScriptDir "runs_board.py") ([string]$config.runs_root) | Out-Host
$exitInfo = Read-JsonFile (Join-Path (Join-Path (Join-Path $runDir "attempts") $attempt) "exit.json")
$reportPath = Join-Path $runDir "report.json"
$report = if (Test-Path -LiteralPath $reportPath) { Read-JsonFile $reportPath } else { $null }
Write-Host ("stopReason={0}" -f $exitInfo.stopReason)
if ($report) {
    Write-Host ("blocked={0}" -f @($report.blocked).Count)
    Write-Host ("branch={0} commits={1}" -f $report.branch, (@($report.commits) -join ","))
    Write-Host ("out_of_scope_files={0}" -f (@($report.out_of_scope_files) -join ","))
}
