[CmdletBinding()]
param(
    [string]$Repo,
    [string]$RunDir,
    [string]$Attempt = "01",
    [ValidateSet("run", "fix", "ship")][string]$Purpose = "run",
    [string]$PromptFile,
    [string]$ModelId,
    [ValidateSet("default", "readonly")][string]$Tools = "default",
    [hashtable]$AitsEnv = @{},
    [string]$SessionFile,
    [string]$RequestFile
)

function Fail-Param([string]$Message) {
    Write-Error $Message
    exit 2
}

function Get-IsoUtcNow {
    return [DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ", [Globalization.CultureInfo]::InvariantCulture)
}

function Get-LastAssistantStopReason([string]$EventsPath) {
    if (-not (Test-Path -LiteralPath $EventsPath)) { return $null }
    $reason = $null
    $errorMessage = $null
    Get-Content -LiteralPath $EventsPath -Encoding UTF8 | ForEach-Object {
        if (-not $_) { return }
        try { $obj = $_ | ConvertFrom-Json -ErrorAction Stop } catch { return }
        if ($obj.type -eq "message_end" -and $obj.message.role -eq "assistant") {
            $reason = $obj.message.stopReason
            $errorMessage = $obj.message.errorMessage
        }
    }
    return @{ stopReason = $reason; errorMessage = $errorMessage }
}

if ($RequestFile) {
    if (-not (Test-Path -LiteralPath $RequestFile)) { Fail-Param "RequestFile not found: $RequestFile" }
    $request = Get-Content -LiteralPath $RequestFile -Raw -Encoding UTF8 | ConvertFrom-Json
    $Repo = $request.repo
    $RunDir = $request.run_dir
    $Attempt = $request.attempt
    $Purpose = $request.purpose
    $PromptFile = $request.prompt_file
    $ModelId = $request.model_id
    $Tools = $request.tools
    $SessionFile = $request.session_file
    $AitsEnv = @{}
    if ($request.aits_env) {
        $request.aits_env.PSObject.Properties | ForEach-Object { $AitsEnv[$_.Name] = [string]$_.Value }
    }
}

if (-not $Repo) { Fail-Param "Repo is required" }
if (-not $RunDir) { Fail-Param "RunDir is required" }
if (-not $Attempt) { Fail-Param "Attempt is required" }
if (-not $PromptFile) { Fail-Param "PromptFile is required" }
if (-not $ModelId) { Fail-Param "ModelId is required" }
if (-not (Test-Path -LiteralPath $Repo -PathType Container)) { Fail-Param "Repo not found: $Repo" }
if (-not (Test-Path -LiteralPath $PromptFile -PathType Leaf)) { Fail-Param "PromptFile not found: $PromptFile" }

$attemptDir = Join-Path (Join-Path $RunDir "attempts") $Attempt
New-Item -ItemType Directory -Force -Path $attemptDir | Out-Null
$sessionDir = Join-Path $RunDir "session"
New-Item -ItemType Directory -Force -Path $sessionDir | Out-Null
$emptyInput = Join-Path $attemptDir "stdin.empty"
Set-Content -LiteralPath $emptyInput -Value "" -NoNewline -Encoding ASCII
$eventsPath = Join-Path $attemptDir "events.jsonl"
$stderrPath = Join-Path $attemptDir "stderr.log"
$exitPath = Join-Path $attemptDir "exit.json"

$toolList = if ($Tools -eq "readonly") { "read,grep,find,ls" } else { "read,bash,edit,write" }
$modelArg = $ModelId
$args = @("-p", (Get-Content -LiteralPath $PromptFile -Raw -Encoding UTF8), "--mode", "json", "--model", $modelArg, "--tools", $toolList)
if ($SessionFile) {
    $args += @("--session", $SessionFile)
} else {
    $args += @("--session-dir", $sessionDir)
}

$started = Get-IsoUtcNow
$psi = New-Object System.Diagnostics.ProcessStartInfo
$psi.FileName = "pi"
foreach ($arg in $args) { [void]$psi.ArgumentList.Add($arg) }
$psi.WorkingDirectory = (Resolve-Path -LiteralPath $Repo).Path
$psi.UseShellExecute = $false
$psi.RedirectStandardInput = $true
$psi.RedirectStandardOutput = $true
$psi.RedirectStandardError = $true
foreach ($key in $AitsEnv.Keys) { $psi.Environment[$key] = [string]$AitsEnv[$key] }

try {
    $proc = [System.Diagnostics.Process]::Start($psi)
    $proc.StandardInput.Close()
    $outTask = $proc.StandardOutput.ReadToEndAsync()
    $errTask = $proc.StandardError.ReadToEndAsync()
    $proc.WaitForExit()
    [IO.File]::WriteAllText($eventsPath, $outTask.Result, [Text.UTF8Encoding]::new($false))
    [IO.File]::WriteAllText($stderrPath, $errTask.Result, [Text.UTF8Encoding]::new($false))
    $code = $proc.ExitCode
} catch {
    [IO.File]::WriteAllText($stderrPath, $_.Exception.ToString(), [Text.UTF8Encoding]::new($false))
    $code = 1
}
$ended = Get-IsoUtcNow

$stop = Get-LastAssistantStopReason $eventsPath
$ok = ($code -eq 0 -and $stop -and $stop.stopReason -and $stop.stopReason -ne "error" -and $stop.stopReason -ne "aborted")
$exitObject = [ordered]@{
    attempt = $Attempt
    purpose = $Purpose
    started_at = $started
    ended_at = $ended
    exit_code = $code
    stopReason = if ($stop) { $stop.stopReason } else { $null }
    errorMessage = if ($stop) { $stop.errorMessage } else { $null }
}
$exitObject | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $exitPath -Encoding UTF8

try {
    $sessionToExport = $SessionFile
    if (-not $sessionToExport -and (Test-Path -LiteralPath $sessionDir)) {
        $latest = Get-ChildItem -LiteralPath $sessionDir -Filter "*.jsonl" -File -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 1
        if ($latest) { $sessionToExport = $latest.FullName }
    }
    if ($sessionToExport) { & pi --export $sessionToExport (Join-Path $RunDir "session.html") | Out-Null }
} catch {
    Add-Content -LiteralPath $stderrPath -Value ("`npi --export failed: " + $_.Exception.Message) -Encoding UTF8
}

if ($ok) { exit 0 }
exit 1
