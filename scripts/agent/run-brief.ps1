[CmdletBinding()]
param(
    [Parameter(Mandatory=$true, Position=0)][string]$BriefPath,
    [string]$Fix,
    [switch]$Direct,
    [switch]$DryRun,
    [string]$Repo = (Resolve-Path ".").Path,
    [string]$Now,
    [int]$TimeoutSeconds = 7200,
    [string]$ConfigPath
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
if (-not $ConfigPath) { $ConfigPath = Join-Path $ScriptDir "agent_config.json" }
$Python = Join-Path $Repo ".venv/Scripts/python.exe"
if (-not (Test-Path -LiteralPath $Python)) { $Python = "python" }
$Utf8NoBom = New-Object Text.UTF8Encoding $false

function Read-JsonFile([string]$Path) { return Get-Content -LiteralPath $Path -Raw -Encoding UTF8 | ConvertFrom-Json }
function Write-JsonNoBom([string]$Path, $Object) { [IO.File]::WriteAllText($Path, ($Object | ConvertTo-Json -Depth 10), $Utf8NoBom) }
function ConvertTo-Hashtable($Object) { $h = @{}; $Object.PSObject.Properties | ForEach-Object { $h[$_.Name] = $_.Value }; return $h }

function Test-InDailyWindow($Config, [string]$NowValue) {
    if ($NowValue) { $nowDt = [DateTimeOffset]::Parse($NowValue) } else { $nowDt = [DateTimeOffset]::Now }
    $tz = [TimeZoneInfo]::FindSystemTimeZoneById("Tokyo Standard Time")
    $jst = [TimeZoneInfo]::ConvertTime($nowDt, $tz)
    foreach ($w in $Config.daily_windows_jst) {
        $s = $w.start.Split(":"); $e = $w.end.Split(":")
        $start = [DateTimeOffset]::new($jst.Year, $jst.Month, $jst.Day, [int]$s[0], [int]$s[1], 0, $jst.Offset)
        $end = [DateTimeOffset]::new($jst.Year, $jst.Month, $jst.Day, [int]$e[0], [int]$e[1], 0, $jst.Offset)
        if ($jst -ge $start -and $jst -le $end) { return @{ in_window=$true; window=("{0}-{1} JST" -f $w.start,$w.end); now=$jst.ToString("yyyy-MM-dd HH:mm zzz") } }
    }
    return @{ in_window=$false; now=$jst.ToString("yyyy-MM-dd HH:mm zzz") }
}

$config = Read-JsonFile $ConfigPath
$briefFull = (Resolve-Path -LiteralPath $BriefPath).Path
$errFile = [IO.Path]::GetTempFileName()
$oldEap = $ErrorActionPreference
$ErrorActionPreference = "Continue"
try {
    $briefJsonText = & $Python (Join-Path $ScriptDir "brief.py") show $briefFull --repo $Repo 2>$errFile
    $briefCode = $LASTEXITCODE
} finally {
    $ErrorActionPreference = $oldEap
}
$briefErr = Get-Content -LiteralPath $errFile -Encoding UTF8 -ErrorAction SilentlyContinue
Remove-Item -LiteralPath $errFile -Force -ErrorAction SilentlyContinue
if ($briefCode -ne 0) {
    $briefErr | ForEach-Object { Write-Host $_ }
    exit 2
}
$brief = ($briefJsonText -join "`n") | ConvertFrom-Json
$window = Test-InDailyWindow $config $Now
if ($window.in_window) { Write-Host ("拒绝启动：当前 {0} 落在日报窗口 {1}" -f $window.now, $window.window); exit 2 }

$runId = if ($brief.run) { [string]$brief.run } else { [IO.Path]::GetFileNameWithoutExtension($briefFull) }
$runDir = Join-Path $config.runs_root $runId
$attemptRoot = Join-Path $runDir "attempts"
$attempt = "01"; $purpose = "run"; $sessionFile = $null
if (-not $Fix -and (Test-Path -LiteralPath $attemptRoot) -and (Get-ChildItem -LiteralPath $attemptRoot -Directory -ErrorAction SilentlyContinue)) {
    Write-Host "拒绝启动：运行目录已有 attempts；如需续跑请使用 -Fix。"
    exit 2
}
if ($Fix) {
    $purpose = "fix"
    if (-not (Test-Path -LiteralPath $attemptRoot)) { Write-Host "拒绝修正：找不到已有 attempts。"; exit 2 }
    $n = (Get-ChildItem -LiteralPath $attemptRoot -Directory -ErrorAction SilentlyContinue | Measure-Object).Count + 1
    $attempt = "{0:D2}" -f $n
    $latestSession = Get-ChildItem -LiteralPath (Join-Path $runDir "session") -Filter "*.jsonl" -File -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if (-not $latestSession) { Write-Host "拒绝修正：找不到可续跑的会话文件。"; exit 2 }
    $sessionFile = $latestSession.FullName
}

$modelKey = [string]$brief.model
$modelId = [string]$config.models.$modelKey
$acceptancePath = Join-Path $runDir "acceptance.json"
$rejectedCount = 0
if (Test-Path -LiteralPath $acceptancePath) { $acc = Read-JsonFile $acceptancePath; $rejectedCount = @($acc.reviews | Where-Object { $_.verdict -eq "rejected" }).Count }
if ($Fix -and $rejectedCount -ge 2) { $modelId = [string]$config.models.cloud }
$lastExit = $null
if (Test-Path -LiteralPath $attemptRoot) {
    $last = Get-ChildItem -LiteralPath $attemptRoot -Directory | Sort-Object Name -Descending | Select-Object -First 1
    if ($last -and (Test-Path -LiteralPath (Join-Path $last.FullName "exit.json"))) { $lastExit = Read-JsonFile (Join-Path $last.FullName "exit.json") }
}
if ($Fix -and $modelKey -eq "local" -and $lastExit -and $lastExit.stopReason -eq "error") { $modelId = [string]$config.models.cloud }
$modelArg = if ($brief.thinking -and $brief.thinking -ne "default") { $modelId + ":" + [string]$brief.thinking } else { $modelId }

$envMap = [ordered]@{ AITS_PI_RUN_DIR=$runDir; AITS_PI_RUN=[string]$brief.run; AITS_PI_TASK=[string]$brief.task; AITS_PI_ZONE=[string]$brief.zone; AITS_PI_ALLOWED_PATHS=([string[]]$brief.allowed_paths -join ";") }
if ($brief.zone -eq "C") { $envMap.AITS_PI_OWNER_DECISION = [string]$brief.owner_decision }
if ($brief.allow_done -eq $true) { $envMap.AITS_PI_ALLOW_DONE = "1" }

$promptTarget = if ($Fix) { Join-Path (Join-Path $attemptRoot $attempt) "prompt.md" } else { Join-Path $runDir "brief.md" }
$toolList = if ($brief.tools -eq "readonly") { "read,grep,find,ls" } else { "read,bash,edit,write" }
$piPreview = "pi -p @$promptTarget --mode json --model $modelArg --tools $toolList --session-dir $runDir\session"
if ($sessionFile) { $piPreview = "pi -p @$promptTarget --mode json --model $modelArg --tools $toolList --session $sessionFile" }

if ($DryRun) {
    Write-Host "DryRun: would run"
    Write-Host $piPreview
    Write-Host "RunDir=$runDir"
    Write-Host "Attempt=$attempt Purpose=$purpose Direct=$Direct"
    foreach ($k in $envMap.Keys) { Write-Host ("{0}={1}" -f $k, $envMap[$k]) }
    exit 0
}

New-Item -ItemType Directory -Force -Path $runDir | Out-Null
if ($Fix) {
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $promptTarget) | Out-Null
    Copy-Item -LiteralPath (Resolve-Path -LiteralPath $Fix).Path -Destination $promptTarget -Force
} else {
    Copy-Item -LiteralPath $briefFull -Destination $promptTarget -Force
    Write-JsonNoBom (Join-Path $runDir "queue.json") ([ordered]@{ queued_at=[DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"); brief=$briefFull })
}

$requestRepo = if ($Direct) { $Repo } else { [string]$config.agent_clone }
$request = [ordered]@{ repo=$requestRepo; run_dir=$runDir; attempt=$attempt; purpose=$purpose; prompt_file=$promptTarget; model_id=$modelArg; tools=[string]$brief.tools; session_file=$sessionFile; aits_env=$envMap }

if ($Direct) {
    & (Join-Path $ScriptDir "agent-run.ps1") -Repo $Repo -RunDir $runDir -Attempt $attempt -Purpose $purpose -PromptFile $promptTarget -ModelId $modelArg -Tools ([string]$brief.tools) -AitsEnv (ConvertTo-Hashtable ([pscustomobject]$envMap)) -SessionFile $sessionFile
    $runExit = $LASTEXITCODE
} else {
    $queueFile = [string]$config.queue_file
    if (Test-Path -LiteralPath $queueFile) { Write-Host "拒绝入队：队列文件已存在，上一请求可能尚未被取走：$queueFile"; exit 2 }
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $queueFile) | Out-Null
    Write-JsonNoBom $queueFile $request
    schtasks /Run /TN ([string]$config.scheduled_task_name) | Out-Null
    $exitPath = Join-Path (Join-Path (Join-Path $runDir "attempts") $attempt) "exit.json"
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline -and -not (Test-Path -LiteralPath $exitPath)) { Start-Sleep -Seconds 5 }
    if (-not (Test-Path -LiteralPath $exitPath)) { throw "Timed out waiting for $exitPath" }
    $exitInfo = Read-JsonFile $exitPath
    $runExit = if ($exitInfo.exit_code -eq 0 -and $exitInfo.stopReason -and $exitInfo.stopReason -ne "error" -and $exitInfo.stopReason -ne "aborted") { 0 } else { 1 }
}

& $Python (Join-Path $ScriptDir "runs_board.py") ([string]$config.runs_root) | Out-Host
$exitInfo = Read-JsonFile (Join-Path (Join-Path (Join-Path $runDir "attempts") $attempt) "exit.json")
$reportPath = Join-Path $runDir "report.json"
$report = if (Test-Path -LiteralPath $reportPath) { Read-JsonFile $reportPath } else { $null }
Write-Host ("stopReason={0}" -f $exitInfo.stopReason)
if ($report) { Write-Host ("blocked={0}" -f @($report.blocked).Count); Write-Host ("branch={0} commits={1}" -f $report.branch, (@($report.commits) -join ",")); Write-Host ("out_of_scope_files={0}" -f (@($report.out_of_scope_files) -join ",")) }
exit $runExit
