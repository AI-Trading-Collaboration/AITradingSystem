[CmdletBinding()]
param(
    [Parameter(Mandatory=$true, Position=0)][string]$BriefPath,
    [string]$Fix,
    [switch]$Direct,
    [switch]$DryRun,
    [string]$Repo = (Resolve-Path ".").Path,
    [string]$Now,
    [int]$TimeoutSeconds = 7200,
    [int]$PickupSeconds = 120,
    [string]$ConfigPath,
    [string]$StateRoot
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

function Test-InDailyWindow([string]$ConfigFile, [string]$NowValue) {
    # daily_window.py (DEVX-031): a window with a label opens early once that day's scheduled run
    # wrote its summary; exit 3 means a closed window, any other non-zero exit counts as closed too.
    # A closed window only prints a note: pi may start inside it, the gate and publish wait (3.7).
    $windowArgs = @("-I", (Join-Path $ScriptDir "daily_window.py"), "--config", $ConfigFile)
    if ($NowValue) { $windowArgs += @("--now", $NowValue) }
    $oldEap = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $windowText = (& $Python @windowArgs 2>&1 | ForEach-Object { "$_" }) -join "`n"
        $windowCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $oldEap
    }
    if ($windowCode -eq 0) { return @{ in_window=$false } }
    if ($windowCode -eq 3) {
        $state = $windowText | ConvertFrom-Json
        return @{ in_window=$true; window=[string]$state.window; now=[string]$state.now; reason=[string]$state.reason }
    }
    return @{ in_window=$true; window="(unknown)"; now="(unknown)"; reason=$windowText }
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
$window = Test-InDailyWindow $ConfigPath $Now
if ($window.in_window) { Write-Host ("注意：当前 {0} 在日报窗口 {1} 内（{2}）；pi 照常启动，门禁和发布等窗口打开。" -f $window.now, $window.window, $window.reason) }

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
$effectiveKey = $modelKey
# Acceptance records live under review_root, which the agent account cannot write (DEVX-025 run 10).
if (-not $config.review_root) { Write-Host "拒绝启动：配置里没有 review_root（验收记录根目录）。"; exit 2 }
$acceptancePath = Join-Path (Join-Path ([string]$config.review_root) $runId) "acceptance.json"
$rejectedCount = 0
if (Test-Path -LiteralPath $acceptancePath) { $acc = Read-JsonFile $acceptancePath; $rejectedCount = @($acc.reviews | Where-Object { $_.verdict -eq "rejected" }).Count }
$lastExit = $null
if (Test-Path -LiteralPath $attemptRoot) {
    $last = Get-ChildItem -LiteralPath $attemptRoot -Directory | Sort-Object Name -Descending | Select-Object -First 1
    if ($last -and (Test-Path -LiteralPath (Join-Path $last.FullName "exit.json"))) { $lastExit = Read-JsonFile (Join-Path $last.FullName "exit.json") }
}
if ($Fix) {
    # Model routing (DEVX-025 section 7): after one local error or one rejection, the fix runs on the cloud model.
    $lastFailed = (-not $lastExit) -or (-not $lastExit.stopReason) -or ($lastExit.stopReason -eq "error") -or ($lastExit.stopReason -eq "aborted")
    if ($rejectedCount -ge 2) { $effectiveKey = "cloud" }
    if ($modelKey -eq "local" -and ($rejectedCount -ge 1 -or $lastFailed)) { $effectiveKey = "cloud" }
}
$modelId = [string]$config.models.$effectiveKey
$thinking = [string]$brief.thinking
if ((-not $thinking) -or $thinking -eq "default") {
    $thinking = ""
    if ($config.default_thinking -and $config.default_thinking.$effectiveKey) { $thinking = [string]$config.default_thinking.$effectiveKey }
}
$modelArg = if ($thinking) { $modelId + ":" + $thinking } else { $modelId }

$envMap = [ordered]@{ AITS_PI_RUN_DIR=$runDir; AITS_PI_RUN=[string]$brief.run; AITS_PI_TASK=[string]$brief.task; AITS_PI_ZONE=[string]$brief.zone; AITS_PI_ALLOWED_PATHS=([string[]]$brief.allowed_paths -join ";") }
if ($brief.zone -eq "C") { $envMap.AITS_PI_OWNER_DECISION = [string]$brief.owner_decision }
if ($brief.allow_done -eq $true) { $envMap.AITS_PI_ALLOW_DONE = "1" }
# Research lane (owner_decision:TRADING-2565:2026-10-13:research_throughput): a run in the research worktree reads
# research data and state from the main checkout through AITS_STATE_ROOT (paths.state_dir).
if ($StateRoot) {
    if (-not ($StateRoot -match '^([A-Za-z]:[\\/]|\\\\|/)') -or -not (Test-Path -LiteralPath $StateRoot -PathType Container)) {
        Write-Host "拒绝启动：-StateRoot 必须是已存在目录的绝对路径：$StateRoot"
        exit 2
    }
    $envMap.AITS_STATE_ROOT = $StateRoot
}

$promptTarget = if ($Fix) { Join-Path (Join-Path $attemptRoot $attempt) "prompt.md" } else { Join-Path $runDir "brief.md" }
$toolList = if ($brief.tools -eq "readonly") { "read,grep,find,ls" } else { "read,bash,edit,write" }
$piPreview = "pi -p @$promptTarget --mode json --model $modelArg --tools $toolList --approve --session-dir $runDir\session"
if ($sessionFile) { $piPreview = "pi -p @$promptTarget --mode json --model $modelArg --tools $toolList --approve --session $sessionFile" }

if ($DryRun) {
    Write-Host "DryRun: would run"
    Write-Host $piPreview
    Write-Host "RunDir=$runDir"
    Write-Host "Attempt=$attempt Purpose=$purpose Direct=$Direct"
    Write-Host "ModelKey=$modelKey EffectiveModel=$effectiveKey Rejected=$rejectedCount"
    foreach ($k in $envMap.Keys) { Write-Host ("{0}={1}" -f $k, $envMap[$k]) }
    exit 0
}

New-Item -ItemType Directory -Force -Path $runDir | Out-Null
if ($Fix) {
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $promptTarget) | Out-Null
    Copy-Item -LiteralPath (Resolve-Path -LiteralPath $Fix).Path -Destination $promptTarget -Force
} else {
    # A brief that already is <run_dir>/brief.md (lane.py start, DEVX-031) is its own prompt: Copy-Item refuses a self-copy.
    $sameFile = Test-Path -LiteralPath $promptTarget -PathType Leaf
    if ($sameFile) { $sameFile = [string]::Equals((Resolve-Path -LiteralPath $promptTarget).Path, $briefFull, [StringComparison]::OrdinalIgnoreCase) }
    if (-not $sameFile) { Copy-Item -LiteralPath $briefFull -Destination $promptTarget -Force }
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
    $taskName = [string]$config.scheduled_task_name
    $oldEap = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $runOut = (& schtasks.exe /Run /TN $taskName 2>&1 | Out-String).Trim()
        $runCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $oldEap
    }
    Write-Host ("schtasks /Run exit {0}: {1}" -f $runCode, $runOut)
    if ($runCode -ne 0) {
        Remove-Item -LiteralPath $queueFile -Force -ErrorAction SilentlyContinue
        Write-Host "拒绝继续：计划任务启动失败，已删除本次写入的队列文件。"
        exit 2
    }
    $pickupDeadline = (Get-Date).AddSeconds($PickupSeconds)
    while ((Get-Date) -lt $pickupDeadline -and (Test-Path -LiteralPath $queueFile)) { Start-Sleep -Seconds 2 }
    if (Test-Path -LiteralPath $queueFile) {
        $ErrorActionPreference = "Continue"
        try {
            $queryOut = (& schtasks.exe /Query /TN $taskName /V /FO LIST 2>&1 | Out-String)
        } finally {
            $ErrorActionPreference = $oldEap
        }
        $queryOut -split "`r?`n" | Where-Object { $_ -match "^(Status|Last Run Time|Last Result):" } | ForEach-Object { Write-Host $_.Trim() }
        Remove-Item -LiteralPath $queueFile -Force -ErrorAction SilentlyContinue
        Write-Host ("拒绝继续：计划任务在 {0} 秒内没有取走请求，已删除队列文件。" -f $PickupSeconds)
        exit 2
    }
    $exitPath = Join-Path (Join-Path (Join-Path $runDir "attempts") $attempt) "exit.json"
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline -and -not (Test-Path -LiteralPath $exitPath)) { Start-Sleep -Seconds 5 }
    if (-not (Test-Path -LiteralPath $exitPath)) { throw "Timed out waiting for $exitPath" }
    $exitInfo = Read-JsonFile $exitPath
    $runExit = if ($exitInfo.exit_code -eq 0 -and $exitInfo.stopReason -and $exitInfo.stopReason -ne "error" -and $exitInfo.stopReason -ne "aborted") { 0 } else { 1 }
}

& $Python (Join-Path $ScriptDir "runs_board.py") ([string]$config.runs_root) --review-root ([string]$config.review_root) | Out-Host
$exitInfo = Read-JsonFile (Join-Path (Join-Path (Join-Path $runDir "attempts") $attempt) "exit.json")
$reportPath = Join-Path $runDir "report.json"
$report = if (Test-Path -LiteralPath $reportPath) { Read-JsonFile $reportPath } else { $null }
Write-Host ("stopReason={0}" -f $exitInfo.stopReason)
if ($report) { Write-Host ("blocked={0}" -f @($report.blocked).Count); Write-Host ("branch={0} commits={1}" -f $report.branch, (@($report.commits) -join ",")); Write-Host ("out_of_scope_files={0}" -f (@($report.out_of_scope_files) -join ",")) }
exit $runExit
