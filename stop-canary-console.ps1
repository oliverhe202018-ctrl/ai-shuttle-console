<#
.SYNOPSIS
    Safe Stopper for AI Shuttle Control Center (Canary Mode).
.DESCRIPTION
    Stops the AI Shuttle Canary server only after strictly verifying that:
    1. The process owns port 8765;
    2. Its command line belongs to this project;
    3. It includes '--canary';
    4. It uses the expected Canary configuration.
    Never kills arbitrary python processes or uses broad taskkill.
#>
[CmdletBinding()]
param(
    [string]$ProjectRoot = "D:\project\ai-shuttle-console",
    [string]$ExpectedConfig = "config.canary.local.20261026-07.json",
    [int]$Port = 8765
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

# Helper: Find listeners on port
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

# Helper: Get process command line
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

if ($listeners.Count -eq 0) {
    Write-Host "No process is listening on port $Port. Nothing to stop." -ForegroundColor Yellow
    exit 0
}

$stoppedCount = 0

foreach ($pidVal in $listeners) {
    $procName = "Unknown"
    try {
        $procObj = Get-Process -Id $pidVal -ErrorAction SilentlyContinue
        if ($procObj) { $procName = $procObj.ProcessName }
    } catch {}

    $cmdLine = Get-ProcessCommandLine -pidVal $pidVal

    Write-Host "Inspecting process on port $Port (PID: $pidVal, Name: $procName)..."
    Write-Host "  CommandLine: $cmdLine"

    # Strictly verify all 4 criteria
    if (-not (Test-IsCanaryServerProcess -cmdLine $cmdLine -expectedCfg $ExpectedConfig)) {
        $refuseMsg = @"
REFUSING TO STOP process PID $pidVal ($procName):
Process command line does not match verified Canary server criteria.
Expected: run.py with --canary and $ExpectedConfig
Observed: $cmdLine
Operator intervention required. Never kill arbitrary or production processes.
"@
        Write-Error $refuseMsg
        exit 1
    }

    Write-Host "Process PID $pidVal verified as AI Shuttle Canary server. Stopping safely..." -ForegroundColor Yellow
    
    # Check parent process (if it was launched in dedicated powershell console)
    $parentPid = $null
    try {
        $cimProc = Get-CimInstance Win32_Process -Filter "ProcessId = $pidVal" -ErrorAction SilentlyContinue
        if ($cimProc) { $parentPid = $cimProc.ParentProcessId }
    } catch {}

    # Stop the specific verified server process
    Stop-Process -Id $pidVal -Force
    Start-Sleep -Milliseconds 500

    # If parent was the dedicated powershell launcher window, clean it up as well
    if ($parentPid) {
        try {
            $parentCmd = Get-ProcessCommandLine -pidVal $parentPid
            if ($parentCmd -like "*AI Shuttle Canary Console*") {
                Stop-Process -Id $parentPid -Force -ErrorAction SilentlyContinue
            }
        } catch {}
    }

    $stoppedCount++
}

# Verify port is released
Start-Sleep -Milliseconds 500
$remaining = Get-PortListeners -checkPort $Port
if ($remaining.Count -gt 0) {
    Write-Warning "Port $Port still has listening process(es): $($remaining -join ', ')"
} else {
    Write-Host "AI Shuttle Canary stopped successfully. Port $Port is now free." -ForegroundColor Green
}
