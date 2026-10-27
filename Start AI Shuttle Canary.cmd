@echo off
setlocal
set "SCRIPT_DIR=%~dp0"
if "%SCRIPT_DIR:~-1%"=="\" set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"

cd /d "%SCRIPT_DIR%"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%\start-canary-console.ps1" %*
set "EXIT_CODE=%ERRORLEVEL%"

if %EXIT_CODE% neq 0 (
    echo.
    echo ========================================================
    echo  AI Shuttle Canary launcher failed with exit code %EXIT_CODE%.
    echo ========================================================
    pause
)

exit /b %EXIT_CODE%
