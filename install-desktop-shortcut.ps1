<#
.SYNOPSIS
    Desktop Shortcut Installer for AI Shuttle Control Center (Canary Mode).
.DESCRIPTION
    Creates or idempotently updates a Windows desktop shortcut pointing to
    "Start AI Shuttle Canary.cmd" using WScript.Shell.
#>
[CmdletBinding()]
param(
    [string]$ProjectRoot = "D:\project\ai-shuttle-console",
    [string]$DesktopPath = ""
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

$TargetCmd = Join-Path $ProjectRoot "Start AI Shuttle Canary.cmd"
if (-not (Test-Path -LiteralPath $TargetCmd)) {
    Write-Error "Target command file '$TargetCmd' does not exist."
    exit 1
}

# 2. Resolve Desktop Path
$WshShell = New-Object -ComObject WScript.Shell

if (-not $DesktopPath) {
    try {
        $DesktopPath = $WshShell.SpecialFolders.Item("Desktop")
    } catch {}
    if (-not $DesktopPath -or -not (Test-Path -LiteralPath $DesktopPath)) {
        $DesktopPath = [System.Environment]::GetFolderPath([System.Environment+SpecialFolder]::Desktop)
    }
}

if (-not $DesktopPath -or -not (Test-Path -LiteralPath $DesktopPath)) {
    Write-Error "Unable to resolve current user's Desktop folder."
    exit 1
}

$ShortcutPath = Join-Path $DesktopPath "AI Shuttle Canary.lnk"
$isUpdate = Test-Path -LiteralPath $ShortcutPath
$actionText = if ($isUpdate) { "updated" } else { "created" }

Write-Host "Configuring Desktop shortcut: $ShortcutPath"
$Shortcut = $WshShell.CreateShortcut($ShortcutPath)
$Shortcut.TargetPath = $TargetCmd
$Shortcut.WorkingDirectory = $ProjectRoot
$Shortcut.Description = "Start Local AI Shuttle Control Center in isolated Canary mode"

# Check for existing project icon if any
$IconCandidates = @(
    (Join-Path $ProjectRoot "app\static\favicon.ico"),
    (Join-Path $ProjectRoot "app\static\icon.ico"),
    (Join-Path $ProjectRoot "favicon.ico")
)
foreach ($icon in $IconCandidates) {
    if (Test-Path -LiteralPath $icon) {
        $Shortcut.IconLocation = "$icon,0"
        break
    }
}

$Shortcut.Save()
[System.Runtime.InteropServices.Marshal]::ReleaseComObject($WshShell) | Out-Null

# 3. Verify Creation and Inspect Properties
if (-not (Test-Path -LiteralPath $ShortcutPath)) {
    Write-Error "Failed to create desktop shortcut at '$ShortcutPath'."
    exit 1
}

$VerifyShell = New-Object -ComObject WScript.Shell
$VerifyShortcut = $VerifyShell.CreateShortcut($ShortcutPath)
$resolvedTarget = $VerifyShortcut.TargetPath
$resolvedWorkDir = $VerifyShortcut.WorkingDirectory
$resolvedDesc = $VerifyShortcut.Description
$resolvedArgs = $VerifyShortcut.Arguments
[System.Runtime.InteropServices.Marshal]::ReleaseComObject($VerifyShell) | Out-Null

Write-Host "Shortcut $actionText successfully." -ForegroundColor Green
Write-Host "  Shortcut Path    : $ShortcutPath"
Write-Host "  Target Path      : $resolvedTarget"
Write-Host "  Working Directory: $resolvedWorkDir"
Write-Host "  Description      : $resolvedDesc"
Write-Host "  Arguments        : $resolvedArgs"
