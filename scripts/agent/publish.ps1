param(
  [Parameter(Mandatory=$true)][string]$Branch,
  [Parameter(Mandatory=$true)][string]$RunDir,
  [string]$Repo,
  [string]$Base = "main",
  [string]$Python,
  [switch]$NoPush
)

$ErrorActionPreference = "Stop"

function Get-UtcIso { (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ", [System.Globalization.CultureInfo]::InvariantCulture) }

function Write-Utf8NoBom([string]$Path, [string]$Text) {
  $encoding = New-Object System.Text.UTF8Encoding($false)
  [System.IO.File]::WriteAllText($Path, $Text, $encoding)
}

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

function Git([string[]]$ArgList, [switch]$AllowFailure) {
  $result = Invoke-Native "git" $ArgList $Repo
  if (($result.code -ne 0) -and (-not $AllowFailure)) {
    throw "git $([string]::Join(' ', $ArgList)) failed: $($result.stderr.Trim())"
  }
  return $result
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

if ([string]::IsNullOrWhiteSpace($Repo)) {
  $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
  $Repo = Split-Path -Parent (Split-Path -Parent $scriptDir)
}
$Repo = [System.IO.Path]::GetFullPath($Repo)
if ([string]::IsNullOrWhiteSpace($Python)) { $Python = Join-Path $Repo ".venv\Scripts\python.exe" }
$RunDir = [System.IO.Path]::GetFullPath($RunDir)
$startedAt = Get-UtcIso
$startBranch = $null
$tempDir = $null

function Ref-Short([string]$Ref) {
  $r = Git @("rev-parse", $Ref) -AllowFailure
  if ($r.code -eq 0) { return $r.stdout.Trim() }
  return $null
}

function Write-PublishJson([bool]$Ok, [string]$Summary, [object]$Result, [bool]$BranchDeleted) {
  $mainSha = Ref-Short $Base
  $gateTrailer = $null
  $pushed = $false
  if ($Result -ne $null) {
    if ($Result.PSObject.Properties.Name -contains "shipped_sha") { $mainSha = [string]$Result.shipped_sha }
    if ($Result.PSObject.Properties.Name -contains "gate_trailer") { $gateTrailer = [string]$Result.gate_trailer }
    if ($Result.PSObject.Properties.Name -contains "pushed") { $pushed = [bool]$Result.pushed }
  }
  $obj = [ordered]@{
    run = (Split-Path -Leaf $RunDir)
    mode = "publish"
    ok = $Ok
    started_at = $startedAt
    ended_at = (Get-UtcIso)
    main_sha = $mainSha
    gate_trailer = $gateTrailer
    pushed = $pushed
    branch_deleted = $BranchDeleted
    summary = $Summary
  }
  Write-Utf8NoBom (Join-Path $RunDir "publish.json") ($obj | ConvertTo-Json -Depth 8 -Compress)
}

$validationErrors = @()
$acceptedTree = $null
foreach ($entry in [System.Environment]::GetEnvironmentVariables().GetEnumerator()) {
  if ([string]$entry.Key -like "AITS_PI_*") { $validationErrors += "AITS_PI environment variable is set: $($entry.Key)" }
}
if (-not (Test-Path -LiteralPath $RunDir -PathType Container)) {
  $validationErrors += "RunDir does not exist: $RunDir"
} else {
  $acceptPath = Join-Path $RunDir "acceptance.json"
  if (-not (Test-Path -LiteralPath $acceptPath -PathType Leaf)) {
    $validationErrors += "acceptance.json is missing in RunDir"
  } else {
    try {
      $accept = Get-Content -LiteralPath $acceptPath -Raw -Encoding UTF8 | ConvertFrom-Json
      $reviews = @($accept.reviews)
      if (($reviews.Count -eq 0) -or ($reviews[$reviews.Count - 1].verdict -ne "accepted")) {
        $validationErrors += "last acceptance review verdict is not accepted"
      } else {
        $acceptedTree = [string]$reviews[$reviews.Count - 1].tree
        if ($acceptedTree -notmatch "^[0-9a-f]{40}$") {
          $validationErrors += "last accepted review tree is missing or is not a 40-character lowercase hex tree"
        }
      }
    } catch {
      $validationErrors += "acceptance.json cannot be read: $($_.Exception.Message)"
    }
  }
}
try {
  $gitDir = (Git @("rev-parse", "--git-dir")).stdout.Trim()
  $commonDir = (Git @("rev-parse", "--git-common-dir")).stdout.Trim()
  if ($gitDir -ne $commonDir) { $validationErrors += "Repo is not the main checkout (git-dir differs from git-common-dir)" }
  $startBranch = (Git @("branch", "--show-current")).stdout.Trim()
  if (($startBranch -ne $Base) -and ($startBranch -ne $Branch)) {
    $validationErrors += "current branch must be Base '$Base' or Branch '$Branch', got '$startBranch'"
  }
  if ($acceptedTree -match "^[0-9a-f]{40}$") {
    $branchTree = (Git @("rev-parse", "$Branch`^{tree}")).stdout.Trim()
    if ($branchTree -ne $acceptedTree) {
      $validationErrors += "Branch tree $branchTree does not match accepted review tree $acceptedTree"
    }
  }
} catch {
  $validationErrors += $_.Exception.Message
}
if ($validationErrors.Count -gt 0) {
  foreach ($reason in $validationErrors) { Write-Host $reason }
  exit 2
}

$resultObj = $null
$branchDeleted = $false
try {
  if ($startBranch -eq $Base) { [void](Git @("switch", $Branch)) }
  $tempDir = Join-Path ([System.IO.Path]::GetTempPath()) ("aits-publish-" + [System.Guid]::NewGuid().ToString("N"))
  [System.IO.Directory]::CreateDirectory($tempDir) | Out-Null
  $tempShip = Join-Path $tempDir "ship.py"
  Copy-GitBlob "$Base`:tools/gov008/ship.py" $tempShip
  $tempHash = (Git @("hash-object", $tempShip)).stdout.Trim()
  $baseHash = (Git @("rev-parse", "$Base`:tools/gov008/ship.py")).stdout.Trim()
  if ($tempHash -ne $baseHash) { throw "temporary ship.py hash does not match $Base" }

  $publishArgs = @("-I", $tempShip, "--repo", $Repo, "--base", $Base, "--publish")
  if (-not $NoPush) { $publishArgs += "--push" }
  $publishResult = Invoke-Native $Python $publishArgs $Repo
  Write-Utf8NoBom (Join-Path $RunDir "publish.log") ($publishResult.stdout + $publishResult.stderr)
  if ($publishResult.code -ne 0) { throw "ship.py --publish failed with exit $($publishResult.code): $($publishResult.stderr.Trim())" }
  $jsonLine = $null
  foreach ($line in ($publishResult.stdout -split "`r?`n")) {
    if ($line.Trim().StartsWith("{")) { $jsonLine = $line.Trim() }
  }
  if ($jsonLine -eq $null) { throw "ship.py --publish did not print result JSON" }
  $resultObj = $jsonLine | ConvertFrom-Json

  [void](Git @("switch", $Base))
  $localBase = (Git @("rev-parse", $Base)).stdout.Trim()
  if ($localBase -ne $resultObj.shipped_sha) { throw "$Base is $localBase, expected $($resultObj.shipped_sha)" }
  if (-not $NoPush) {
    $originBase = (Git @("rev-parse", "origin/$Base")).stdout.Trim()
    if ($originBase -ne $resultObj.shipped_sha) { throw "origin/$Base is $originBase, expected $($resultObj.shipped_sha)" }
  }
  $delete = Git @("branch", "-d", $Branch) -AllowFailure
  $branchDeleted = ($delete.code -eq 0)
  $summary = "published $Branch to $Base"
  if (-not $branchDeleted) { $summary += "; branch deletion with -d failed" }
  Write-PublishJson $true $summary $resultObj $branchDeleted
  if ($tempDir -ne $null) { Remove-Item -LiteralPath $tempDir -Recurse -Force -ErrorAction SilentlyContinue }
  exit 0
} catch {
  $reason = $_.Exception.Message
  try {
    $nowBranch = (Git @("branch", "--show-current") -AllowFailure).stdout.Trim()
    if (($startBranch -ne $null) -and ($nowBranch -ne $startBranch)) { [void](Git @("switch", $startBranch) -AllowFailure) }
  } catch { }
  if ($tempDir -ne $null) { Remove-Item -LiteralPath $tempDir -Recurse -Force -ErrorAction SilentlyContinue }
  Write-Host $reason
  Write-PublishJson $false $reason $resultObj $branchDeleted
  exit 1
}
