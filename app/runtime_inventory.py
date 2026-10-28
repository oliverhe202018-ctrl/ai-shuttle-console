"""Read-only inventory of live Codex app-server roots and their production sessions."""
import ctypes
import os
from pathlib import Path
import re
import sqlite3

from app.canary import same_path
from app.session_liveness import inspect_rollout, normalized_path


def discover_runtime_roots():
    """Inspect x64 Windows process parameters without attaching or executing code.

    Only home-root variables are retained. Missing access/platform evidence is
    an error, never an empty authoritative inventory.
    """
    if os.name != "nt" or ctypes.sizeof(ctypes.c_void_p) != 8:
        raise ValueError("PRODUCTION_RUNTIME_ROOT_DISCOVERY_UNAVAILABLE")
    from ctypes import wintypes as w
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    native = ctypes.WinDLL("ntdll")
    class Entry(ctypes.Structure):
        _fields_ = [("size", w.DWORD), ("usage", w.DWORD), ("pid", w.DWORD),
                    ("heap", ctypes.c_size_t), ("module", w.DWORD), ("threads", w.DWORD),
                    ("parent", w.DWORD), ("priority", w.LONG), ("flags", w.DWORD), ("name", w.WCHAR * 260)]
    kernel.CreateToolhelp32Snapshot.argtypes = [w.DWORD, w.DWORD]
    kernel.CreateToolhelp32Snapshot.restype = w.HANDLE
    kernel.Process32FirstW.argtypes = kernel.Process32NextW.argtypes = [w.HANDLE, ctypes.POINTER(Entry)]
    kernel.OpenProcess.argtypes = [w.DWORD, w.BOOL, w.DWORD]
    kernel.OpenProcess.restype = w.HANDLE
    kernel.CloseHandle.argtypes = [w.HANDLE]
    kernel.ReadProcessMemory.argtypes = [w.HANDLE, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t, ctypes.POINTER(ctypes.c_size_t)]
    native.NtQueryInformationProcess.argtypes = [w.HANDLE, w.ULONG, ctypes.c_void_p, w.ULONG, ctypes.POINTER(w.ULONG)]
    snapshot = kernel.CreateToolhelp32Snapshot(2, 0)
    if snapshot == ctypes.c_void_p(-1).value:
        raise ValueError("PRODUCTION_RUNTIME_PROCESS_INVENTORY_UNAVAILABLE")
    result = []
    try:
        entry = Entry()
        entry.size = ctypes.sizeof(entry)
        ok = kernel.Process32FirstW(snapshot, ctypes.byref(entry))
        while ok:
            if entry.name.lower() == "codex.exe":
                handle = kernel.OpenProcess(0x410, False, entry.pid)
                if not handle:
                    raise ValueError("PRODUCTION_RUNTIME_PROCESS_ACCESS_UNKNOWN")
                try:
                    def read(address, length):
                        buf = ctypes.create_string_buffer(length)
                        count = ctypes.c_size_t()
                        if not kernel.ReadProcessMemory(handle, address, buf, length, ctypes.byref(count)) or count.value != length:
                            raise ValueError("PRODUCTION_RUNTIME_PROCESS_READ_UNKNOWN")
                        return buf.raw
                    basic = ctypes.create_string_buffer(48)
                    returned = w.ULONG()
                    if native.NtQueryInformationProcess(handle, 0, basic, 48, ctypes.byref(returned)):
                        raise ValueError("PRODUCTION_RUNTIME_PROCESS_PARAMETERS_UNKNOWN")
                    peb = int.from_bytes(basic.raw[8:16], "little")
                    params = int.from_bytes(read(peb + 0x20, 8), "little")
                    def unicode_string(offset):
                        length = int.from_bytes(read(params + offset, 2), "little")
                        address = int.from_bytes(read(params + offset + 8, 8), "little")
                        if length > 65534:
                            raise ValueError("PRODUCTION_RUNTIME_PROCESS_PARAMETERS_UNKNOWN")
                        return read(address, length).decode("utf-16-le")
                    command = unicode_string(0x70)
                    if re.search(r"(?:^|\s)app-server(?:\s|$)", command):
                        address = int.from_bytes(read(params + 0x80, 8), "little")
                        env_bytes = bytearray()
                        for offset in range(0, 1024 * 1024, 2):
                            env_bytes.extend(read(address + offset, 2))
                            if env_bytes[-4:] == b"\0\0\0\0":
                                break
                        else:
                            raise ValueError("PRODUCTION_RUNTIME_ENVIRONMENT_UNKNOWN")
                        allowed = {"CODEX_HOME", "CODEX_SQLITE_HOME", "USERPROFILE", "HOME"}
                        env = dict(item.split("=", 1) for item in env_bytes.decode("utf-16-le").split("\0")
                                   if "=" in item and item.split("=", 1)[0] in allowed)
                        profile = env.get("USERPROFILE") or env.get("HOME")
                        home = env.get("CODEX_HOME") or (str(Path(profile) / ".codex") if profile else None)
                        if not home:
                            raise ValueError("PRODUCTION_RUNTIME_HOME_UNKNOWN")
                        result.append({"process_id": entry.pid, "home": home,
                                       "sqlite_home": env.get("CODEX_SQLITE_HOME") or home,
                                       "home_source": "process_environment" if env.get("CODEX_HOME") else "process_profile_default"})
                finally:
                    kernel.CloseHandle(handle)
            ok = kernel.Process32NextW(snapshot, ctypes.byref(entry))
    finally:
        kernel.CloseHandle(snapshot)
    return result


def inspect_runtime_inventory(workspace, primary_sessions_dir, exclude_thread=None, processes=None):
    """Every live runtime root is included; unreadable or missing evidence blocks."""
    sessions, extras, roots = [], [], {}
    def unknown(reason, **evidence):
        extras.append({"thread_id": None, "classification": "UNKNOWN_LIVENESS", "blocking": True,
                       "reason": reason, **evidence})
    try:
        processes = discover_runtime_roots() if processes is None else processes
    except (OSError, ValueError) as exc:
        unknown(str(exc))
        processes = []
    roots[str(normalized_path(primary_sessions_dir).resolve())] = None
    for process in processes:
        roots[str((normalized_path(process["home"]) / "sessions").resolve())] = process["sqlite_home"]
    for root, sql_home in roots.items():
        parsed_ids = set()
        if not Path(root).is_dir():
            unknown("PRODUCTION_SESSION_INVENTORY_UNAVAILABLE", session_root=root)
            continue
        for path in Path(root).rglob("*.jsonl"):
            try:
                report = inspect_rollout(path, workspace, exclude_thread)
                if report is not None:
                    sessions.append(report)
                    parsed_ids.add(report["thread_id"])
            except (OSError, ValueError) as exc:
                unknown(str(exc), rollout_path=str(path), session_root=root)
        if sql_home is None:
            unknown("PRODUCTION_RUNTIME_ROOT_NOT_CORRELATED", session_root=root)
            continue
        try:
            state = normalized_path(sql_home) / "state_5.sqlite"
            with sqlite3.connect(state.resolve().as_uri() + "?mode=ro", uri=True) as db:
                for tid, cwd, path in db.execute("SELECT id,cwd,rollout_path FROM threads"):
                    if tid != exclude_thread and cwd and same_path(normalized_path(cwd), normalized_path(workspace)) and tid not in parsed_ids:
                        unknown("REGISTERED_PRODUCTION_ROLLOUT_UNRESOLVED", thread_id=tid,
                                registered_rollout_path=path, session_root=root)
        except (OSError, sqlite3.Error) as exc:
            unknown("PRODUCTION_RUNTIME_REGISTRY_UNAVAILABLE", session_root=root, detail=str(exc))
    return {"sessions": sessions, "blockers": [r for r in sessions if r["blocking"]] + extras,
            "scope": {"processes": processes, "session_roots": list(roots), "complete": not extras}}
