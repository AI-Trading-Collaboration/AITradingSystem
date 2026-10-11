[CmdletBinding(SupportsShouldProcess=$true)]
param(
    [switch]$CheckOnly,
    [string]$Repo = (Resolve-Path ".").Path,
    [string]$UserName = "aits-agent",
    [string]$AgentClone = "D:\Work\AITradingSystem_agent",
    [string]$AgentWorktree = "D:\Work\AITradingSystem_agent\_agent_wt",
    [string]$RunsRoot = "D:\Work\AITradingSystem_agent_runs",
    [string]$QueueFile = "D:\Work\AITradingSystem_agent_runs\queue\request.json",
    [string]$TaskName = "AITradingSystem pi agent"
)

$ErrorActionPreference = "Stop"

function Test-WorkAcl {
    $acl = Get-Acl -LiteralPath "D:\Work"
    $hits = @()
    foreach ($ace in $acl.Access) {
        $id = $ace.IdentityReference.Value
        $rights = $ace.FileSystemRights.ToString()
        if (($id -match "Authenticated Users|Users") -and ($rights -match "Modify|Write|FullControl")) { $hits += ("$id $rights") }
    }
    if ($hits.Count -eq 0) { Write-Warning "D:\Work is not writable by Authenticated Users or Users" } else { Write-Host ("D:\Work writable ACL: " + ($hits -join "; ")) }
}

function Show-MachineSecretNames {
    $names = [Environment]::GetEnvironmentVariables("Machine").Keys | Where-Object { $_ -match "KEY|TOKEN|SECRET|PASSWORD" } | Sort-Object
    if ($names) { Write-Host "Machine environment variables with secret-like names:"; $names | ForEach-Object { Write-Host "  $_" } } else { Write-Host "No machine environment variables with secret-like names." }
}

function Test-TwsPorts {
    $ports = @(7496, 7497, 4001, 4002)
    $listeners = Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue | Where-Object { $ports -contains $_.LocalPort }
    if ($listeners) { $listeners | ForEach-Object { Write-Warning ("Port {0} is listening on {1}" -f $_.LocalPort,$_.LocalAddress) } } else { Write-Host "No TWS/Gateway ports are listening." }
}

function Invoke-CheckOnly {
    Test-WorkAcl
    Show-MachineSecretNames
    Test-TwsPorts
}

Invoke-CheckOnly
if ($CheckOnly) { return }

$principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) { throw "Full setup must run as Administrator." }

$user = Get-LocalUser -Name $UserName -ErrorAction SilentlyContinue
if (-not $user) {
    $password = Read-Host -AsSecureString "Password for local standard user $UserName"
    if ($PSCmdlet.ShouldProcess($UserName, "create local standard user")) { New-LocalUser -Name $UserName -Password $password -PasswordNeverExpires:$false | Out-Null }
} else { Write-Host "User $UserName already exists; skipping." }

foreach ($dir in @($AgentClone, $AgentWorktree, $RunsRoot, (Split-Path -Parent $QueueFile))) {
    if ($PSCmdlet.ShouldProcess($dir, "create directory")) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }
}

foreach ($dir in @($AgentClone, $AgentWorktree, $RunsRoot)) {
    if ($PSCmdlet.ShouldProcess($dir, "grant $UserName modify and current user read")) {
        icacls $dir /grant "${UserName}:(OI)(CI)M" | Out-Null
        icacls $dir /grant "${env:USERNAME}:(OI)(CI)R" | Out-Null
    }
}

$gitDir = Join-Path $Repo ".git"
if ($PSCmdlet.ShouldProcess($gitDir, "grant $UserName read")) { icacls $gitDir /grant "${UserName}:(OI)(CI)R" | Out-Null }

if (-not (Test-Path -LiteralPath (Join-Path $AgentClone ".git"))) {
    if ($PSCmdlet.ShouldProcess($AgentClone, "clone main from source .git")) { git clone --branch main $gitDir $AgentClone | Out-Null }
} else { Write-Host "Agent clone already exists; skipping clone." }

$sourceCfg = Join-Path $Repo ".venv\pyvenv.cfg"
$home = $null
if (Test-Path -LiteralPath $sourceCfg) {
    foreach ($line in Get-Content -LiteralPath $sourceCfg) { if ($line -match '^home\s*=\s*(.+)$') { $home = $Matches[1].Trim() } }
}
$basePython = if ($home) { Join-Path $home "python.exe" } else { "python" }
$agentPython = Join-Path $AgentClone ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $agentPython)) {
    if ($PSCmdlet.ShouldProcess($AgentClone, "create venv")) { & $basePython -m venv (Join-Path $AgentClone ".venv") }
}
if ($PSCmdlet.ShouldProcess($AgentClone, "editable install")) { & $agentPython -m pip install -e $AgentClone }

$agentPythonExe = Join-Path $AgentClone ".venv\Scripts\python.exe"
if (-not (Get-NetFirewallRule -DisplayName "AITradingSystem agent python no outbound" -ErrorAction SilentlyContinue)) {
    if ($PSCmdlet.ShouldProcess($agentPythonExe, "block outbound firewall rule")) {
        New-NetFirewallRule -DisplayName "AITradingSystem agent python no outbound" -Direction Outbound -Program $agentPythonExe -Action Block | Out-Null
    }
}

$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument ("-NoProfile -ExecutionPolicy Bypass -File `"{0}`" -RequestFile `"{1}`"" -f (Join-Path $AgentClone "scripts\agent\agent-run.ps1"), $QueueFile)
$principalTask = New-ScheduledTaskPrincipal -UserId $UserName -LogonType Password -RunLevel Limited
if ($PSCmdlet.ShouldProcess($TaskName, "register manual scheduled task")) {
    Register-ScheduledTask -TaskName $TaskName -Action $action -Principal $principalTask -Force | Out-Null
}
