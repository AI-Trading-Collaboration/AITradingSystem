param(
  [Parameter(Mandatory=$true)][string]$Draft,
  [string]$ExpectSha256,
  [switch]$Yes,
  [string]$Repo,
  [string]$Base = "main",
  [string]$Python
)

# Writes one owner decision record into <review_root>\decisions\ (owner_decision:DEVX-029:2026-10-13:decision_scoped_signoff).
# The draft comes from decision_drafts_root in the Base branch's agent_config.json, never from the agent's run or
# clone directories. main's ship.py --check-decision-record validates it. The owner runs this script and types Y,
# or the machine session runs it with -Yes and the draft's SHA-256 right after the owner's own message naming this
# decision. It never overwrites a record: revoking a decision is deleting its record file.

$ErrorActionPreference = "Stop"

function Quote-Arg([string]$Value) {
  if ($Value -eq "") { return '""' }
  if ($Value -notmatch '[\s"]') { return $Value }
  return '"' + ($Value -replace '([\\]*)"', '$1$1\"' -replace '([\\]+)$', '$1$1') + '"'
}

function Invoke-Native([string]$FileName, [string[]]$ArgList, [string]$WorkingDirectory) {
  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = $FileName
  $quoted = @()
  foreach ($item in $ArgList) { $quoted += (Quote-Arg $item) }
  $psi.Arguments = [string]::Join(" ", $quoted)
  $psi.WorkingDirectory = $WorkingDirectory
  $psi.UseShellExecute = $false
  $psi.RedirectStandardOutput = $true
  $psi.RedirectStandardError = $true
  $psi.StandardOutputEncoding = [System.Text.Encoding]::UTF8
  $psi.StandardErrorEncoding = [System.Text.Encoding]::UTF8
  $proc = New-Object System.Diagnostics.Process
  $proc.StartInfo = $psi
  [void]$proc.Start()
  $errTask = $proc.StandardError.ReadToEndAsync()
  $stdout = $proc.StandardOutput.ReadToEnd()
  $proc.WaitForExit()
  $stderr = $errTask.Result
  return @{ code = $proc.ExitCode; stdout = $stdout; stderr = $stderr }
}

function Copy-GitBlob([string]$Spec, [string]$OutFile) {
  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = "git"
  $psi.Arguments = "-C " + (Quote-Arg $Repo) + " show " + (Quote-Arg $Spec)
  $psi.UseShellExecute = $false
  $psi.RedirectStandardOutput = $true
  $psi.RedirectStandardError = $true
  $proc = New-Object System.Diagnostics.Process
  $proc.StartInfo = $psi
  [void]$proc.Start()
  $errTask = $proc.StandardError.ReadToEndAsync()
  $fs = [System.IO.File]::Open($OutFile, [System.IO.FileMode]::Create, [System.IO.FileAccess]::Write)
  try { $proc.StandardOutput.BaseStream.CopyTo($fs) } finally { $fs.Close() }
  $proc.WaitForExit()
  $stderr = $errTask.Result
  if ($proc.ExitCode -ne 0) { throw "git show $Spec failed: $stderr" }
}

function Get-Sha256([string]$Path) {
  return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Test-Inside([string]$Path, [string]$Root) {
  if ([string]::IsNullOrWhiteSpace($Root)) { return $false }
  $full = [System.IO.Path]::GetFullPath($Root).TrimEnd('\', '/')
  return ($Path.StartsWith($full + [System.IO.Path]::DirectorySeparatorChar, [System.StringComparison]::OrdinalIgnoreCase) -or $Path.Equals($full, [System.StringComparison]::OrdinalIgnoreCase))
}

function Stop-Approve([string]$Reason) {
  Write-Host $Reason
  exit 2
}

if ([string]::IsNullOrWhiteSpace($Repo)) {
  $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
  $Repo = Split-Path -Parent (Split-Path -Parent $scriptDir)
}
$Repo = [System.IO.Path]::GetFullPath($Repo)
if ([string]::IsNullOrWhiteSpace($Python)) { $Python = Join-Path $Repo ".venv\Scripts\python.exe" }

foreach ($entry in [System.Environment]::GetEnvironmentVariables().GetEnumerator()) {
  if ([string]$entry.Key -like "AITS_PI_*") { Stop-Approve "AITS_PI environment variable is set: $($entry.Key)" }
}
if ($Yes -and ($ExpectSha256 -cnotmatch "^[0-9a-f]{64}$")) { Stop-Approve "-Yes needs -ExpectSha256 with the draft's 64-character lowercase hex SHA-256" }
if ((-not [string]::IsNullOrEmpty($ExpectSha256)) -and ($ExpectSha256 -cnotmatch "^[0-9a-f]{64}$")) { Stop-Approve "ExpectSha256 is not a 64-character lowercase hex SHA-256" }

$configResult = Invoke-Native "git" @("-C", $Repo, "show", "$Base`:scripts/agent/agent_config.json") $Repo
if ($configResult.code -ne 0) { Stop-Approve "cannot read agent_config.json on ${Base}: $($configResult.stderr.Trim())" }
$config = $configResult.stdout | ConvertFrom-Json
$reviewRoot = [string]$config.review_root
$draftsRoot = [string]$config.decision_drafts_root
if ([string]::IsNullOrWhiteSpace($reviewRoot)) { Stop-Approve "agent_config.json on $Base has no review_root" }
if ([string]::IsNullOrWhiteSpace($draftsRoot)) { Stop-Approve "agent_config.json on $Base has no decision_drafts_root" }

$Draft = [System.IO.Path]::GetFullPath($Draft)
if (-not (Test-Path -LiteralPath $Draft -PathType Leaf)) { Stop-Approve "draft not found: $Draft" }
if (-not (Test-Inside $Draft $draftsRoot)) { Stop-Approve "draft must be inside decision_drafts_root ${draftsRoot}: $Draft" }
foreach ($root in @([string]$config.runs_root, [string]$config.agent_clone, $reviewRoot)) {
  if (Test-Inside $Draft $root) { Stop-Approve "draft must not be inside ${root}: $Draft" }
}
$draftHash = Get-Sha256 $Draft
if ((-not [string]::IsNullOrEmpty($ExpectSha256)) -and ($draftHash -cne $ExpectSha256)) {
  Stop-Approve "draft SHA-256 $draftHash does not match ExpectSha256 $ExpectSha256"
}

$tempDir = Join-Path ([System.IO.Path]::GetTempPath()) ("aits-approve-" + [System.Guid]::NewGuid().ToString("N"))
[System.IO.Directory]::CreateDirectory($tempDir) | Out-Null
try {
  $tempShip = Join-Path $tempDir "ship.py"
  Copy-GitBlob "$Base`:tools/gov008/ship.py" $tempShip
  $tempHash = (Invoke-Native "git" @("-C", $Repo, "hash-object", $tempShip) $Repo).stdout.Trim()
  $baseHash = (Invoke-Native "git" @("-C", $Repo, "rev-parse", "$Base`:tools/gov008/ship.py") $Repo).stdout.Trim()
  if ($tempHash -cne $baseHash) { Stop-Approve "temporary ship.py hash does not match $Base" }
  $check = Invoke-Native $Python @("-I", $tempShip, "--check-decision-record", $Draft) $Repo
} finally {
  Remove-Item -LiteralPath $tempDir -Recurse -Force -ErrorAction SilentlyContinue
}
if ($check.code -ne 0) { Stop-Approve ("draft rejected: " + ($check.stdout + $check.stderr).Trim()) }
$summary = $null
foreach ($line in ($check.stdout -split "`r?`n")) {
  if ($line.Trim().StartsWith("{")) { $summary = $line.Trim() | ConvertFrom-Json }
}
if ($summary -eq $null) { Stop-Approve "ship.py --check-decision-record did not print result JSON" }
if ($summary.sha256 -cne $draftHash) { Stop-Approve "draft changed while it was checked" }

Write-Host "decision: $($summary.decision)"
Write-Host "zone C paths it approves:"
foreach ($glob in @($summary.c_paths)) { Write-Host "  $glob" }
Write-Host "expected result: $($summary.expected_result)"
Write-Host "valid until: $($summary.expires)"
Write-Host "draft SHA-256: $draftHash"

if (-not $Yes) {
  $answer = Read-Host "Write this record into the review root? Type Y to confirm"
  if ($answer -cne "Y") {
    Write-Host "not written"
    exit 1
  }
}

$decisionDir = Join-Path $reviewRoot "decisions"
$target = Join-Path $decisionDir ([string]$summary.record_name)
if (Test-Path -LiteralPath $target -PathType Leaf) {
  if ((Get-Sha256 $target) -ceq $draftHash) {
    Write-Host "an identical record already exists: $target"
    exit 0
  }
  Stop-Approve "a different record with this name exists; not overwritten: $target"
}
[System.IO.Directory]::CreateDirectory($decisionDir) | Out-Null
Copy-Item -LiteralPath $Draft -Destination $target
$written = Get-Sha256 $target
if ($written -cne $draftHash) { Stop-Approve "written record SHA-256 $written differs from the draft $draftHash" }
Write-Host "written: $target"
Write-Host "SHA-256: $written"
exit 0
