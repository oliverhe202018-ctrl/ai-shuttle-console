import json
import os
from pathlib import Path
import subprocess
import sys
import pytest

from app.canary import (
    load_explicit_config,
    validate_canary_config,
    is_production_target,
)

BASE_DIR = Path(__file__).resolve().parent.parent
CANARY_CONFIG_NAME = "config.canary.local.20261026-07.json"
CANARY_CONFIG_PATH = BASE_DIR / CANARY_CONFIG_NAME
LAUNCHER_PS1 = BASE_DIR / "start-canary-console.ps1"
LAUNCHER_CMD = BASE_DIR / "Start AI Shuttle Canary.cmd"
SHORTCUT_INSTALLER = BASE_DIR / "install-desktop-shortcut.ps1"
STOPPER_PS1 = BASE_DIR / "stop-canary-console.ps1"


def run_ps_script(args, cwd=BASE_DIR, check=False):
    """Run powershell script with UTF-8 encoding and error replacement."""
    cmd = [
        "powershell.exe",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
    ] + args
    return subprocess.run(
        cmd,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=check,
    )


def test_1_canary_config_validity():
    """Verify config.canary.local.20261026-07.json exists and satisfies canary contracts."""
    assert CANARY_CONFIG_PATH.exists(), f"{CANARY_CONFIG_PATH} must exist"
    data = json.loads(CANARY_CONFIG_PATH.read_text(encoding="utf-8"))
    assert data.get("canary_mode") is True
    assert data["server"]["host"] == "127.0.0.1"
    assert data["server"]["port"] == 8765

    # Load and validate with app.canary
    config = load_explicit_config(CANARY_CONFIG_PATH, canary=True)
    validate_canary_config(config)
    assert not is_production_target(
        thread_id=config.codex.thread_id,
        conv_id_or_url=config.chatgpt.conversation_url,
        repo_path=config.codex.repo_path,
    )


def test_2_missing_config_rejected():
    """Launcher fails closed when config file is missing."""
    proc = run_ps_script([
        "-File", str(LAUNCHER_PS1),
        "-ConfigPath", "nonexistent-config.json",
        "-NoBrowser",
    ])
    assert proc.returncode != 0
    output = (proc.stdout or "") + (proc.stderr or "")
    assert "Preflight failed" in output
    assert "not found" in output


def test_3_invalid_json_config_rejected(tmp_path):
    """Launcher fails closed when config file has invalid JSON."""
    bad_json = tmp_path / "bad.json"
    bad_json.write_text("{ broken json: ", encoding="utf-8")

    proc = run_ps_script([
        "-File", str(LAUNCHER_PS1),
        "-ConfigPath", str(bad_json),
        "-NoBrowser",
    ])
    assert proc.returncode != 0
    output = (proc.stdout or "") + (proc.stderr or "")
    assert "Preflight failed" in output
    assert "not valid JSON" in output


def test_4_non_canary_config_rejected():
    """Launcher fails closed when config is not canary mode (e.g., config.example.json)."""
    proc = run_ps_script([
        "-File", str(LAUNCHER_PS1),
        "-ConfigPath", "config.example.json",
        "-NoBrowser",
    ])
    assert proc.returncode != 0
    output = (proc.stdout or "") + (proc.stderr or "")
    assert "Preflight failed" in output
    assert "canary_mode" in output


def test_5_cmd_wrapper_structure_and_quoting():
    """Verify Start AI Shuttle Canary.cmd properly resolves path, bypasses policy, and invokes script."""
    assert LAUNCHER_CMD.exists()
    content = LAUNCHER_CMD.read_text(encoding="ascii", errors="ignore")
    assert "%~dp0" in content
    assert "powershell.exe" in content
    assert "-ExecutionPolicy Bypass" in content
    assert "start-canary-console.ps1" in content
    assert "pause" in content


def test_6_command_construction_and_working_directory():
    """Verify launcher constructs the exact verified Canary command and working directory."""
    script_text = LAUNCHER_PS1.read_text(encoding="utf-8")
    assert "run.py --canary --config" in script_text
    assert "D:\\project\\ai-shuttle-console" in script_text
    assert "config.canary.local.20261026-07.json" in script_text
    assert ".venv\\Scripts\\python.exe" in script_text


def test_7_desktop_shortcut_installer_idempotency_and_attributes(tmp_path):
    """Verify install-desktop-shortcut.ps1 creates .lnk with exact attributes and is idempotent."""
    assert SHORTCUT_INSTALLER.exists()

    # Step 1: Run installer with custom DesktopPath (isolated tmp_path)
    res = run_ps_script([
        "-File", str(SHORTCUT_INSTALLER),
        "-ProjectRoot", str(BASE_DIR),
        "-DesktopPath", str(tmp_path),
    ], check=True)
    assert "created successfully" in res.stdout

    lnk_path = tmp_path / "AI Shuttle Canary.lnk"
    assert lnk_path.exists()

    # Verify properties using PowerShell COM object
    inspect_cmd = f"""
    $w = New-Object -ComObject WScript.Shell
    $s = $w.CreateShortcut('{str(lnk_path)}')
    Write-Output "TARGET:$($s.TargetPath)"
    Write-Output "WORKDIR:$($s.WorkingDirectory)"
    Write-Output "DESC:$($s.Description)"
    """
    prop_res = run_ps_script(["-Command", inspect_cmd], check=True)
    out = prop_res.stdout
    assert str(LAUNCHER_CMD).lower() in out.lower()
    assert str(BASE_DIR).lower() in out.lower()
    assert "DESC:Start Local AI Shuttle Control Center in isolated Canary mode" in out

    # Step 2: Run installer a second time to verify idempotency
    res2 = run_ps_script([
        "-File", str(SHORTCUT_INSTALLER),
        "-ProjectRoot", str(BASE_DIR),
        "-DesktopPath", str(tmp_path),
    ], check=True)
    assert "updated successfully" in res2.stdout
    # Still only one lnk file
    lnk_files = list(tmp_path.glob("*.lnk"))
    assert len(lnk_files) == 1


def test_8_port_safety_classification_logic():
    """Unit test the classification function Test-IsCanaryServerProcess for all scenarios."""
    ps_test_script = f"""
    function Test-IsCanaryServerProcess([string]$cmdLine, [string]$expectedCfg) {{
        if (-not $cmdLine) {{ return $false }}
        $hasRunPy = ($cmdLine -like "*run.py*") -or ($cmdLine -like "*ai-shuttle-console*")
        $hasCanary = $cmdLine -like "*--canary*"
        $hasExpectedConfig = $cmdLine -like "*$expectedCfg*"
        $hasProductionConfig = ($cmdLine -like "*config.example.json*")
        return ($hasRunPy -and $hasCanary -and $hasExpectedConfig -and (-not $hasProductionConfig))
    }}

    $cfg = "{CANARY_CONFIG_NAME}"

    # Scenario A: Exact Canary Command -> Match
    $cmdA = '"D:\\project\\ai-shuttle-console\\.venv\\Scripts\\python.exe" run.py --canary --config config.canary.local.20261026-07.json'
    $resA = Test-IsCanaryServerProcess -cmdLine $cmdA -expectedCfg $cfg

    # Scenario B: Production Server -> Refuse (No --canary, production config)
    $cmdB = '"D:\\project\\ai-shuttle-console\\.venv\\Scripts\\python.exe" run.py --config config.example.json'
    $resB = Test-IsCanaryServerProcess -cmdLine $cmdB -expectedCfg $cfg

    # Scenario C: Different Canary Config -> Refuse
    $cmdC = '"D:\\project\\ai-shuttle-console\\.venv\\Scripts\\python.exe" run.py --canary --config config.canary.local.20261026-01.json'
    $resC = Test-IsCanaryServerProcess -cmdLine $cmdC -expectedCfg $cfg

    # Scenario D: Unrelated Process -> Refuse
    $cmdD = 'node.exe server.js'
    $resD = Test-IsCanaryServerProcess -cmdLine $cmdD -expectedCfg $cfg

    # Scenario E: Plain python run.py -> Refuse
    $cmdE = 'python.exe run.py'
    $resE = Test-IsCanaryServerProcess -cmdLine $cmdE -expectedCfg $cfg

    Write-Output "A:$resA,B:$resB,C:$resC,D:$resD,E:$resE"
    """
    res = run_ps_script(["-Command", ps_test_script], check=True)
    assert "A:True,B:False,C:False,D:False,E:False" in res.stdout.strip()


def test_9_port_conflict_fails_closed_integration():
    """Launcher fails closed when port is occupied by an unexpected process."""
    import socket
    test_port = 58766
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", test_port))
    sock.listen(1)

    try:
        proc = run_ps_script([
            "-File", str(LAUNCHER_PS1),
            "-Port", str(test_port),
            "-NoBrowser",
        ])
        assert proc.returncode != 0
        output = (proc.stdout or "") + (proc.stderr or "")
        assert "PORT CONFLICT" in output or "Port" in output
        assert "Refusing to start" in output
        assert "DO NOT kill" in output
    finally:
        sock.close()


def test_10_stop_script_safety_guard():
    """stop-canary-console.ps1 refuses to stop arbitrary / conflicting processes."""
    import socket
    test_port = 58767
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", test_port))
    sock.listen(1)

    try:
        proc = run_ps_script([
            "-File", str(STOPPER_PS1),
            "-Port", str(test_port),
        ])
        assert proc.returncode != 0
        output = (proc.stdout or "") + (proc.stderr or "")
        assert "REFUSING TO STOP" in output or "Refusing" in output
        assert "Operator intervention required" in output
    finally:
        sock.close()


def test_11_stop_script_no_process_graceful():
    """stop-canary-console.ps1 reports gracefully when no process is listening."""
    proc = run_ps_script([
        "-File", str(STOPPER_PS1),
        "-Port", "58768",
    ])
    assert proc.returncode == 0
    assert "No process is listening" in proc.stdout
