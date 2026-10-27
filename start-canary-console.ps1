<#
.SYNOPSIS
    Windows One-Click Canary Launcher for AI Shuttle Control Center.
.DESCRIPTION
    Safely starts or activates the local AI Shuttle Control Center in Canary mode.
#>
[CmdletBinding()]
param(
    [string]$ProjectRoot = "D:\project\ai-shuttle-console",
    [string]$ConfigPath = "config.canary.local.20261026-07.json",
    [int]$Port = 8765,
    [int]$TimeoutSeconds = 30,
    [switch]$NoBrowser,
    [switch]$Foreground
)

$ErrorActionPreference = "Stop"

# 1. Resolve Project Root
if (-not $ProjectRoot -or -not (Test-Path -LiteralPath $ProjectRoot)) {
    if ($PSScriptRoot -and (Test-Path -LiteralPath $PSScriptRoot)) {
        $ProjectRoot = $PSScriptRoot
    } else {
        $ProjectRoot = "D:\project\ai-shuttle-console"
    }
}
$ProjectRoot = (Resolve-Path -LiteralPath $ProjectRoot).Path

# Resolve Config Path
if ([System.IO.Path]::IsPathRooted($ConfigPath)) {
    $ResolvedConfigPath = $ConfigPath
} else {
    $ResolvedConfigPath = Join-Path $ProjectRoot $ConfigPath
}
$ConfigFileName = Split-Path -Leaf $ResolvedConfigPath
$PythonExe = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$RunPy = Join-Path $ProjectRoot "run.py"
$TargetUrl = "http://127.0.0.1:$Port"

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host " AI Shuttle Control Center — Windows Canary Launcher" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "Execution Mode : CANARY" -ForegroundColor Green
Write-Host "Config File    : $ResolvedConfigPath"
Write-Host "Project Root   : $ProjectRoot"
Write-Host "Target URL     : $TargetUrl"
Write-Host "============================================================"

# Preflight Checks
if (-not (Test-Path -LiteralPath $ProjectRoot -PathType Container)) {
    Write-Error "Preflight failed: Project root '$ProjectRoot' does not exist."
    exit 1
}

if (-not (Test-Path -LiteralPath $PythonExe -PathType Leaf)) {
    Write-Error "Preflight failed: Python virtual environment executable not found at '$PythonExe'."
    exit 1
}

if (-not (Test-Path -LiteralPath $RunPy -PathType Leaf)) {
    Write-Error "Preflight failed: Startup script 'run.py' not found at '$RunPy'."
    exit 1
}

if (-not (Test-Path -LiteralPath $ResolvedConfigPath -PathType Leaf)) {
    Write-Error "Preflight failed: Configuration file '$ResolvedConfigPath' not found."
    exit 1
}

try {
    $rawConfig = Get-Content -LiteralPath $ResolvedConfigPath -Raw -Encoding UTF8
    $parsedConfig = $rawConfig | ConvertFrom-Json
    if (-not $parsedConfig.canary_mode) {
        Write-Error "Preflight failed: Configuration '$ConfigFileName' does not have 'canary_mode: true'."
        exit 1
    }
} catch {
    Write-Error "Preflight failed: Configuration file '$ResolvedConfigPath' is not valid JSON. Error: $_"
    exit 1
}

Write-Host "Preflight validation passed." -ForegroundColor Green

# Port Safety Check
function Get-PortListeners([int]$checkPort) {
    $pids = @()
    try {
        $conns = Get-NetTCPConnection -LocalPort $checkPort -State Listen -ErrorAction SilentlyContinue
        foreach ($c in $conns) {
            if ($c.OwningProcess -and ($pids -notcontains $c.OwningProcess)) {
                $pids += $c.OwningProcess
            }
        }
    } catch {}
    if ($pids.Count -eq 0) {
        try {
            $lines = netstat -ano -p tcp | Select-String ":$checkPort\s+.*LISTENING\s+(\d+)"
            foreach ($m in $lines) {
                if ($m.Matches.Groups.Count -gt 1) {
                    $pidVal = [int]$m.Matches.Groups[1].Value
                    if ($pidVal -and ($pids -notcontains $pidVal)) {
                        $pids += $pidVal
                    }
                }
            }
        } catch {}
    }
    return $pids
}

function Get-ProcessCommandLine([int]$pidVal) {
    try {
        $proc = Get-CimInstance Win32_Process -Filter "ProcessId = $pidVal" -ErrorAction SilentlyContinue
        if ($proc -and $proc.CommandLine) { return $proc.CommandLine }
    } catch {}
    try {
        $proc = Get-WmiObject Win32_Process -Filter "ProcessId = $pidVal" -ErrorAction SilentlyContinue
        if ($proc -and $proc.CommandLine) { return $proc.CommandLine }
    } catch {}
    return $null
}

function Test-IsCanaryServerProcess([string]$cmdLine, [string]$expectedCfg) {
    if (-not $cmdLine) { return $false }
    $hasRunPy = ($cmdLine -like "*run.py*") -or ($cmdLine -like "*ai-shuttle-console*")
    $hasCanary = $cmdLine -like "*--canary*"
    $hasExpectedConfig = $cmdLine -like "*$expectedCfg*"
    $hasProductionConfig = ($cmdLine -like "*config.example.json*")
    return ($hasRunPy -and $hasCanary -and $hasExpectedConfig -and (-not $hasProductionConfig))
}

$listeners = Get-PortListeners -checkPort $Port

if ($listeners.Count -gt 0) {
    Write-Host "Port $Port is currently in use. Inspecting active listener(s)..."
    $allMatchCanary = $true
    $conflictingProcesses = @()

    foreach ($pidVal in $listeners) {
        $procName = "Unknown"
        try {
            $pObj = Get-Process -Id $pidVal -ErrorAction SilentlyContinue
            if ($pObj) { $procName = $pObj.ProcessName }
        } catch {}

        $cmdLine = Get-ProcessCommandLine -pidVal $pidVal
        Write-Host "  Found listener PID: $pidVal ($procName)"
        Write-Host "  Command Line: $cmdLine"

        if (Test-IsCanaryServerProcess -cmdLine $cmdLine -expectedCfg $ConfigFileName) {
            # Matches canary
        } else {
            $allMatchCanary = $false
            $conflictingProcesses += [PSCustomObject]@{
                PID = $pidVal
                ProcessName = $procName
                CommandLine = $cmdLine
            }
        }
    }

    if ($allMatchCanary) {
        Write-Host ""
        Write-Host "AI Shuttle Canary is already running (PID: $($listeners -join ', '))." -ForegroundColor Green
        if (-not $NoBrowser) {
            Write-Host "Opening $TargetUrl in default browser..."
            Start-Process $TargetUrl
        }
        exit 0
    } else {
        $details = ""
        foreach ($cp in $conflictingProcesses) {
            $details += "`n  PID         : $($cp.PID)`n  ProcessName : $($cp.ProcessName)`n  CommandLine : $($cp.CommandLine)"
        }
        $conflictMsg = @"
PORT CONFLICT: Port $Port belongs to another or unexpected process.$details

Refusing to start. DO NOT kill or restart unexpected processes automatically.
DO NOT silently use another port.
Operator intervention is required to resolve this port conflict.
"@
        Write-Error $conflictMsg
        exit 1
    }
}

# Launch Canary Runtime
Write-Host "Port $Port is free. Launching AI Shuttle Canary runtime..." -ForegroundColor Cyan

if ($Foreground) {
    Write-Host "Running in foreground console..."
    Set-Location -LiteralPath $ProjectRoot
    & $PythonExe run.py --canary --config $ConfigFileName
    exit $LASTEXITCODE
}

# Launch in dedicated visible console window
$windowTitle = "AI Shuttle Canary Console"
$serverCommand = "& { `$Host.UI.RawUI.WindowTitle = '$windowTitle'; Set-Location -LiteralPath '$ProjectRoot'; & '$PythonExe' run.py --canary --config '$ConfigFileName' }"

$startArgs = @(
    "-NoExit",
    "-ExecutionPolicy", "Bypass",
    "-Command", $serverCommand
)

$serverProcess = Start-Process -FilePath "powershell.exe" `
    -ArgumentList $startArgs `
    -WorkingDirectory $ProjectRoot `
    -PassThru

Write-Host "Dedicated console launched (Launcher PID: $($serverProcess.Id))."
Write-Host "Waiting for AI Shuttle Canary to become ready on $TargetUrl..."

# Startup Readiness Check
$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
$isReady = $false
$timeoutMs = $TimeoutSeconds * 1000

while ($stopwatch.ElapsedMilliseconds -lt $timeoutMs) {
    if ($serverProcess.HasExited) {
        Write-Error "Server process exited prematurely with code $($serverProcess.ExitCode). Please inspect the dedicated console window."
        exit 1
    }

    try {
        $stateResponse = Invoke-WebRequest -Uri "$TargetUrl/api/state" -UseBasicParsing -TimeoutSec 2 -ErrorAction Stop
        if ($stateResponse.StatusCode -eq 200) {
            $isReady = $true
            break
        }
    } catch {
        Start-Sleep -Milliseconds 500
    }
}

if ($isReady) {
    Write-Host ""
    Write-Host "AI Shuttle Canary ready" -ForegroundColor Green
    if (-not $NoBrowser) {
        Write-Host "Opening $TargetUrl in default browser..."
        Start-Process $TargetUrl
    }
    exit 0
} else {
    Write-Error "AI Shuttle Canary failed to become ready on $TargetUrl within $TimeoutSeconds seconds. The dedicated console window remains open for diagnostics."
    exit 1
}
