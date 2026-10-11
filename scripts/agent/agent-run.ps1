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
    [string]$RequestFile,
    [string]$PiCommand
)

$Utf8NoBom = New-Object Text.UTF8Encoding $false
$LoadedRequestFile = $null

function Fail-Param([string]$Message) { Write-Error $Message; exit 2 }
function Get-IsoUtcNow { return [DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ", [Globalization.CultureInfo]::InvariantCulture) }
function Write-JsonNoBom([string]$Path, $Object) { [IO.File]::WriteAllText($Path, ($Object | ConvertTo-Json -Depth 10), $Utf8NoBom) }

function Quote-Arg([string]$Value) {
    if ($null -eq $Value) { return '""' }
    if ($Value -notmatch '[\s"]' -and $Value -ne '') { return $Value }
    $sb = New-Object Text.StringBuilder
    [void]$sb.Append('"')
    $slashes = 0
    foreach ($ch in $Value.ToCharArray()) {
        if ($ch -eq '\') { $slashes++; continue }
        if ($ch -eq '"') { [void]$sb.Append('\' * ($slashes * 2 + 1)); [void]$sb.Append('"'); $slashes = 0; continue }
        if ($slashes) { [void]$sb.Append('\' * $slashes); $slashes = 0 }
        [void]$sb.Append($ch)
    }
    if ($slashes) { [void]$sb.Append('\' * ($slashes * 2)) }
    [void]$sb.Append('"')
    return $sb.ToString()
}

function Join-Args([string[]]$ArgList) { return (($ArgList | ForEach-Object { Quote-Arg $_ }) -join ' ') }

function Resolve-Pi([string]$Override) {
    $cmdPath = $Override
    if (-not $cmdPath) {
        $cmd = Get-Command pi -ErrorAction SilentlyContinue | Select-Object -First 1
        if (-not $cmd) { throw "pi command not found" }
        $cmdPath = $cmd.Source
    }
    $resolved = Resolve-Path -LiteralPath $cmdPath -ErrorAction SilentlyContinue
    if ($resolved) { $cmdPath = $resolved.Path }
    $ext = [IO.Path]::GetExtension($cmdPath).ToLowerInvariant()
    if ($ext -eq ".exe") { return @{ FileName = $cmdPath; Prefix = @() } }
    if ($ext -eq ".py") {
        $py = if ($env:PYTHON) { $env:PYTHON } else { "python" }
        return @{ FileName = $py; Prefix = @($cmdPath) }
    }
    if ($ext -eq ".cmd" -or $ext -eq ".ps1") {
        $text = Get-Content -LiteralPath $cmdPath -Raw -Encoding UTF8
        $js = $null
        $matches = [regex]::Matches($text, '"([^"]+\.js)"|([^\s"]+\.js)')
        foreach ($m in $matches) {
            $candidate = if ($m.Groups[1].Value) { $m.Groups[1].Value } else { $m.Groups[2].Value }
            $candidate = $candidate -replace '%~dp0', (Split-Path -Parent $cmdPath) + '\'
            $candidate = $candidate -replace '\$PSScriptRoot', (Split-Path -Parent $cmdPath)
            if (-not [IO.Path]::IsPathRooted($candidate)) { $candidate = Join-Path (Split-Path -Parent $cmdPath) $candidate }
            if (Test-Path -LiteralPath $candidate) { $js = (Resolve-Path -LiteralPath $candidate).Path; break }
        }
        if (-not $js) { throw "could not find pi JavaScript entry in $cmdPath" }
        $node = (Get-Command node.exe -ErrorAction SilentlyContinue | Select-Object -First 1).Source
        if (-not $node) { $node = (Get-Command node -ErrorAction SilentlyContinue | Select-Object -First 1).Source }
        if (-not $node) { throw "node.exe not found for $cmdPath" }
        return @{ FileName = $node; Prefix = @($js) }
    }
    return @{ FileName = $cmdPath; Prefix = @() }
}

function Get-LastAssistantStopReason([string]$EventsPath) {
    if (-not (Test-Path -LiteralPath $EventsPath)) { return $null }
    $reason = $null; $errorMessage = $null
    Get-Content -LiteralPath $EventsPath -Encoding UTF8 | ForEach-Object {
        if (-not $_) { return }
        try { $obj = $_ | ConvertFrom-Json -ErrorAction Stop } catch { return }
        if ($obj.type -eq "message_end" -and $obj.message.role -eq "assistant") {
            $reason = $obj.message.stopReason; $errorMessage = $obj.message.errorMessage
        }
    }
    return @{ stopReason = $reason; errorMessage = $errorMessage }
}

if ($RequestFile) {
    if (-not (Test-Path -LiteralPath $RequestFile)) { Fail-Param "RequestFile not found: $RequestFile" }
    $LoadedRequestFile = (Resolve-Path -LiteralPath $RequestFile).Path
    $request = Get-Content -LiteralPath $LoadedRequestFile -Raw -Encoding UTF8 | ConvertFrom-Json
    $Repo = $request.repo; $RunDir = $request.run_dir; $Attempt = $request.attempt; $Purpose = $request.purpose
    $PromptFile = $request.prompt_file; $ModelId = $request.model_id; $Tools = $request.tools; $SessionFile = $request.session_file
    $AitsEnv = @{}
    if ($request.aits_env) { $request.aits_env.PSObject.Properties | ForEach-Object { $AitsEnv[$_.Name] = [string]$_.Value } }
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
if ($LoadedRequestFile) { Move-Item -LiteralPath $LoadedRequestFile -Destination (Join-Path $attemptDir "request.json") -Force }
$sessionDir = Join-Path $RunDir "session"
New-Item -ItemType Directory -Force -Path $sessionDir | Out-Null
$eventsPath = Join-Path $attemptDir "events.jsonl"
$stderrPath = Join-Path $attemptDir "stderr.log"
$exitPath = Join-Path $attemptDir "exit.json"

$toolList = if ($Tools -eq "readonly") { "read,grep,find,ls" } else { "read,bash,edit,write" }
$piArgs = @("-p", "@$PromptFile", "--mode", "json", "--model", $ModelId, "--tools", $toolList)
if ($SessionFile) { $piArgs += @("--session", $SessionFile) } else { $piArgs += @("--session-dir", $sessionDir) }

$started = Get-IsoUtcNow
try {
    $pi = Resolve-Pi $PiCommand
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $pi.FileName
    $psi.Arguments = Join-Args ([string[]]($pi.Prefix + $piArgs))
    $psi.WorkingDirectory = (Resolve-Path -LiteralPath $Repo).Path
    $psi.UseShellExecute = $false
    $psi.RedirectStandardInput = $true; $psi.RedirectStandardOutput = $true; $psi.RedirectStandardError = $true
    try { $psi.StandardInputEncoding = $Utf8NoBom } catch { }
    @($psi.Environment.Keys) | Where-Object { $_ -like "AITS_PI_*" } | ForEach-Object { $psi.Environment.Remove($_) }
    foreach ($key in $AitsEnv.Keys) { $psi.Environment[$key] = [string]$AitsEnv[$key] }
    $proc = [System.Diagnostics.Process]::Start($psi)
    $proc.StandardInput.Close()
    $proc.StandardInput.Dispose()
    $outTask = $proc.StandardOutput.ReadToEndAsync(); $errTask = $proc.StandardError.ReadToEndAsync()
    $proc.WaitForExit()
    [IO.File]::WriteAllText($eventsPath, $outTask.Result, $Utf8NoBom)
    [IO.File]::WriteAllText($stderrPath, $errTask.Result, $Utf8NoBom)
    $code = $proc.ExitCode
} catch {
    [IO.File]::WriteAllText($stderrPath, $_.Exception.ToString(), $Utf8NoBom)
    $code = 1
}
$ended = Get-IsoUtcNow
$stop = Get-LastAssistantStopReason $eventsPath
$ok = ($code -eq 0 -and $stop -and $stop.stopReason -and $stop.stopReason -ne "error" -and $stop.stopReason -ne "aborted")
Write-JsonNoBom $exitPath ([ordered]@{ attempt=$Attempt; purpose=$Purpose; started_at=$started; ended_at=$ended; exit_code=$code; stopReason=if($stop){$stop.stopReason}else{$null}; errorMessage=if($stop){$stop.errorMessage}else{$null} })

try {
    $sessionToExport = $SessionFile
    if (-not $sessionToExport -and (Test-Path -LiteralPath $sessionDir)) {
        $latest = Get-ChildItem -LiteralPath $sessionDir -Filter "*.jsonl" -File -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 1
        if ($latest) { $sessionToExport = $latest.FullName }
    }
    if ($sessionToExport) {
        $pi = Resolve-Pi $PiCommand
        $exportPsi = New-Object System.Diagnostics.ProcessStartInfo
        $exportPsi.FileName = $pi.FileName
        $exportPsi.Arguments = Join-Args ([string[]]($pi.Prefix + @("--export", $sessionToExport, (Join-Path $RunDir "session.html"))))
        $exportPsi.WorkingDirectory = (Resolve-Path -LiteralPath $Repo).Path
        $exportPsi.UseShellExecute = $false
        $exportProc = [System.Diagnostics.Process]::Start($exportPsi)
        $exportProc.WaitForExit()
    }
} catch { Add-Content -LiteralPath $stderrPath -Value ("`npi --export failed: " + $_.Exception.Message) -Encoding UTF8 }

if ($ok) { exit 0 }
exit 1
