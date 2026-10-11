#Requires -PSEdition Desktop
[CmdletBinding(SupportsShouldProcess=$true)]
param(
    [switch]$CheckOnly,
    [string]$WorkRoot = "D:\Work",
    [string]$Repo = (Resolve-Path ".").Path,
    [string]$UserName = "aits-agent",
    [string]$AgentClone = "D:\Work\AITradingSystem_agent",
    [string]$AgentWorktree = "D:\Work\AITradingSystem_agent_wt",
    [string]$RunsRoot = "D:\Work\AITradingSystem_agent_runs",
    [string]$QueueFile = "D:\Work\AITradingSystem_agent_runs\queue\request.json",
    [string]$TaskName = "AITradingSystem pi agent",
    [int]$ExecutionLimitHours = 2,
    [string]$PiCommand = "C:\Program Files\aits-pi\pi.cmd"
)

$ErrorActionPreference = "Stop"
$preview = [bool]$WhatIfPreference
$ProblemList = New-Object System.Collections.ArrayList
$UsersSid = "S-1-5-32-545"
$AuthenticatedUsersSid = "S-1-5-11"

function Add-Problem([string]$Message) {
    if ($ProblemList -contains $Message) { return }
    [void]$ProblemList.Add($Message)
    [Console]::Error.WriteLine("WARNING: " + $Message)
}
function Check-Native([string]$Name) { if ($LASTEXITCODE -ne 0) { throw "$Name failed with exit code $LASTEXITCODE" } }

function Test-Right($Rights, [string]$Kind) {
    $mask = ([int64]$Rights) -band 4294967295
    $writeMask = [int64]1343029590
    $readMask = [int64]2415919105
    if ($Kind -eq "write") { return (($mask -band $writeMask) -ne 0) }
    return (($mask -band $readMask) -ne 0)
}

function Test-PathAcl([string]$Path, [switch]$AlsoRead) {
    $acl = (New-Object System.IO.DirectoryInfo($Path)).GetAccessControl()
    foreach ($ace in $acl.GetAccessRules($true, $true, [System.Security.Principal.SecurityIdentifier])) {
        if ($ace.AccessControlType -ne "Allow") { continue }
        $sid = $ace.IdentityReference.Value
        if ($sid -ne $UsersSid -and $sid -ne $AuthenticatedUsersSid) { continue }
        $name = $sid
        try { $name = $ace.IdentityReference.Translate([System.Security.Principal.NTAccount]).Value } catch { }
        if (Test-Right $ace.FileSystemRights "write") { Add-Problem "$Path is writable by $name ($sid)" }
        if ($AlsoRead -and (Test-Right $ace.FileSystemRights "read")) { Add-Problem "$Path is readable by $name ($sid)" }
    }
}

function Show-MachineSecretNames {
    $names = [Environment]::GetEnvironmentVariables("Machine").Keys | Where-Object { $_ -match "KEY|TOKEN|SECRET|PASSWORD" } | Sort-Object
    if ($names) {
        Add-Problem ("Machine environment variables have secret-like names: " + ($names -join ", "))
    } else { Write-Host "No machine environment variables with secret-like names." }
}

function Test-TwsPorts {
    $ports = @(7496, 7497, 4001, 4002)
    $listeners = Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue | Where-Object { $ports -contains $_.LocalPort }
    if ($listeners) { $listeners | ForEach-Object { Add-Problem ("Port {0} is listening on {1}" -f $_.LocalPort,$_.LocalAddress) } } else { Write-Host "No TWS/Gateway ports are listening." }
}

function Invoke-CheckOnly {
    $ProblemList.Clear() | Out-Null
    Test-PathAcl $WorkRoot
    Test-PathAcl $Repo -AlsoRead
    Show-MachineSecretNames
    Test-TwsPorts
    if ($ProblemList.Count -eq 0) { Write-Host "CheckOnly passed." } else { Write-Warning ("CheckOnly found {0} problem(s)." -f $ProblemList.Count) }
    return $ProblemList.Count
}

$problemCount = Invoke-CheckOnly
if ($CheckOnly) {
    if ($problemCount -gt 0) { exit 1 }
    exit 0
}
if ($problemCount -gt 0) { throw "Full setup stopped because CheckOnly found problems." }

$piProblemStart = $ProblemList.Count
if (-not (Test-Path -LiteralPath $PiCommand -PathType Leaf)) {
    Add-Problem "pi command not found: $PiCommand"
} else {
    Test-PathAcl (Split-Path -Parent $PiCommand)
}
if ($ProblemList.Count -gt $piProblemStart) {
    if ($preview) {
        Write-Host "Full setup would stop here: pi command is missing or writable by standard users."
    } else {
        throw "Full setup stopped: pi command is missing or writable by standard users. Install pi to $PiCommand as Administrator first."
    }
} else {
    Write-Host "pi command: $PiCommand"
}

if (-not $preview) {
    $principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
    if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) { throw "Full setup must run as Administrator." }
}

$password = $null
if (-not $preview) { $password = Read-Host -AsSecureString "Password for local standard user $UserName" }
$user = Get-LocalUser -Name $UserName -ErrorAction SilentlyContinue
if (-not $user) {
    if ($PSCmdlet.ShouldProcess($UserName, "create local standard user")) { New-LocalUser -Name $UserName -Password $password -PasswordNeverExpires | Out-Null }
} else { Write-Host "User $UserName already exists; skipping creation." }
if ($user) { $agentSid = $user.Sid.Value } elseif ($preview) { $agentSid = "<SID of $UserName>" } else { $agentSid = (Get-LocalUser -Name $UserName).Sid.Value }

foreach ($dir in @($AgentClone, $AgentWorktree, $RunsRoot, (Split-Path -Parent $QueueFile))) {
    if ($PSCmdlet.ShouldProcess($dir, "create directory")) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }
}
$ownerIdentity = [Security.Principal.WindowsIdentity]::GetCurrent()
$ownerSid = $ownerIdentity.User.Value
Write-Host "Owner account: $($ownerIdentity.Name) ($ownerSid)"
foreach ($dir in @($AgentClone, $AgentWorktree, $RunsRoot)) {
    if ($PSCmdlet.ShouldProcess($dir, "disable inheritance; grant Administrators, SYSTEM, and current account full control; grant $UserName modify")) {
        icacls $dir /inheritance:r /grant:r "*S-1-5-32-544:(OI)(CI)F" "*S-1-5-18:(OI)(CI)F" "*${ownerSid}:(OI)(CI)F" "*${agentSid}:(OI)(CI)M" | Out-Null; Check-Native "icacls restrict inheritance"
        icacls $dir
    }
}

$gitDir = Join-Path $Repo ".git"
if ($PSCmdlet.ShouldProcess($gitDir, "grant $UserName read")) { icacls $gitDir /grant "${UserName}:(OI)(CI)R" | Out-Null; Check-Native "icacls .git read" }

if (-not (Test-Path -LiteralPath (Join-Path $AgentClone ".git"))) {
    if ($PSCmdlet.ShouldProcess($AgentClone, "clone main from source .git")) { git clone --branch main $gitDir $AgentClone | Out-Null; Check-Native "git clone" }
} else { Write-Host "Agent clone already exists; skipping clone." }

foreach ($safeRaw in @($Repo, $gitDir, $AgentClone, $AgentWorktree)) {
    $safe = $safeRaw.Replace('\', '/')
    $existing = git config --system --get-all safe.directory 2>$null
    if ($existing -notcontains $safe) {
        if ($PSCmdlet.ShouldProcess($safe, "add system git safe.directory")) { git config --system --add safe.directory $safe; Check-Native "git safe.directory" }
    }
}

$sourceCfg = Join-Path $Repo ".venv\pyvenv.cfg"
$venvHome = $null
if (Test-Path -LiteralPath $sourceCfg) { foreach ($line in Get-Content -LiteralPath $sourceCfg) { if ($line -match '^home\s*=\s*(.+)$') { $venvHome = $Matches[1].Trim() } } }
$basePython = if ($venvHome) { Join-Path $venvHome "python.exe" } else { "python" }
$agentPython = Join-Path $AgentClone ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $agentPython)) {
    if ($PSCmdlet.ShouldProcess($AgentClone, "create venv")) { & $basePython -m venv (Join-Path $AgentClone ".venv"); Check-Native "python -m venv" }
}
if ($PSCmdlet.ShouldProcess($AgentClone, "install project and dev dependencies")) {
    & $agentPython -m pip install -e $AgentClone; Check-Native "pip install -e"
    & $agentPython -m pip install pytest pytest-xdist ruff pyyaml; Check-Native "pip install dev tools"
}

$blockedPython = $basePython
if (-not (Get-NetFirewallRule -DisplayName "AITradingSystem agent base python no outbound" -ErrorAction SilentlyContinue)) {
    if ($PSCmdlet.ShouldProcess($blockedPython, "block outbound firewall rule for $UserName")) {
        $agentSddl = "D:(A;;CC;;;$agentSid)"
        New-NetFirewallRule -DisplayName "AITradingSystem agent base python no outbound" -Direction Outbound -Program $blockedPython -LocalUser $agentSddl -Action Block | Out-Null
    }
}
Write-Host "1b manual validation required: HTTPS as $UserName should fail; HTTPS as your account should succeed."

$taskArgument = "-NoProfile -ExecutionPolicy Bypass -File `"{0}`" -RequestFile `"{1}`" -PiCommand `"{2}`"" -f (Join-Path $AgentClone "scripts\agent\agent-run.ps1"), $QueueFile, $PiCommand
$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument $taskArgument
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -ExecutionTimeLimit (New-TimeSpan -Hours $ExecutionLimitHours) -MultipleInstances IgnoreNew
if ($PSCmdlet.ShouldProcess($TaskName, "register manual scheduled task: powershell.exe $taskArgument")) {
    $plainPassword = [Runtime.InteropServices.Marshal]::PtrToStringBSTR([Runtime.InteropServices.Marshal]::SecureStringToBSTR($password))
    try {
        Register-ScheduledTask -TaskName $TaskName -Action $action -Settings $settings -User $UserName -Password $plainPassword -RunLevel Limited -Force | Out-Null
    } finally { $plainPassword = $null }
    $service = New-Object -ComObject Schedule.Service
    $service.Connect()
    $registeredTask = $service.GetFolder("\").GetTask($TaskName)
    $taskSddl = $registeredTask.GetSecurityDescriptor(4)
    $ownerAce = "(A;;GRGX;;;$ownerSid)"
    if (-not $taskSddl.Contains($ownerAce)) {
        $aceStart = $taskSddl.IndexOf("(")
        $newSddl = if ($aceStart -ge 0) { $taskSddl.Insert($aceStart, $ownerAce) } else { $taskSddl + $ownerAce }
        $registeredTask.SetSecurityDescriptor($newSddl, 0)
    }
    Write-Host ("Task DACL: " + $registeredTask.GetSecurityDescriptor(4))
}
