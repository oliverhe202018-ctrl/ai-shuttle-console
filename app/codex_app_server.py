import asyncio
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger("shuttle.codex_app_server")


class CodexAppServerError(Exception):
    """Base exception for Codex app-server lifecycle and RPC operations."""
    pass


class CodexRpcError(CodexAppServerError):
    """JSON-RPC error response from Codex app-server."""
    def __init__(self, code: int, message: str, data: Any = None):
        super().__init__(f"Codex RPC error {code}: {message}")
        self.code = code
        self.message = message
        self.data = data


class CodexRpcTimeoutError(CodexAppServerError):
    """Timeout waiting for Codex app-server response."""
    pass


class CodexProcessTerminatedError(CodexAppServerError):
    """Codex app-server child process terminated unexpectedly."""
    pass


class CodexResumeIdentityMismatchError(CodexAppServerError):
    """Resumed thread identity does not match expected identity."""
    pass


class CodexLifecycleCapabilityUnavailableError(CodexAppServerError):
    """A required lifecycle capability is unavailable in installed Codex."""
    pass


class CodexResumedQueueNotDispatchedError(CodexAppServerError):
    """Queued submission failed to dispatch after thread resume."""
    pass


class AppServerTransport:
    """Manages the official `codex app-server --stdio` subprocess."""

    def __init__(self, cli_path: str):
        self.cli_path = cli_path
        self.process: Optional[asyncio.subprocess.Process] = None
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        """Start the app-server child process over stdio."""
        async with self._lock:
            if self.process and self.process.returncode is None:
                return

            cli = Path(self.cli_path)
            if not cli.is_file():
                raise CodexAppServerError(f"Codex CLI executable not found: {self.cli_path}")

            cmd = [str(cli), "app-server", "--stdio"]
            logger.info("Launching Codex app-server stdio process: %s", cmd)

            self.process = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

    def is_alive(self) -> bool:
        return self.process is not None and self.process.returncode is None

    @property
    def pid(self) -> Optional[int]:
        return self.process.pid if self.process else None

    async def send_line(self, line: str) -> None:
        if not self.is_alive() or not self.process or not self.process.stdin:
            raise CodexProcessTerminatedError("App-server process is not running")
        data = (line.rstrip("\r\n") + "\n").encode("utf-8")
        self.process.stdin.write(data)
        await self.process.stdin.drain()

    async def read_line(self) -> str:
        if not self.is_alive() or not self.process or not self.process.stdout:
            raise CodexProcessTerminatedError("App-server process is not running")
        line = await self.process.stdout.readline()
        if not line:
            return ""
        return line.decode("utf-8")

    async def stop(self) -> None:
        """Gracefully terminate app-server child process."""
        async with self._lock:
            if not self.process:
                return
            proc = self.process
            self.process = None
            if proc.returncode is None:
                try:
                    proc.terminate()
                    await asyncio.wait_for(proc.wait(), timeout=3.0)
                except (asyncio.TimeoutError, ProcessLookupError):
                    try:
                        proc.kill()
                        await proc.wait()
                    except Exception:
                        pass
                logger.info("Codex app-server process stopped (PID: %s)", proc.pid)


class AppServerRpcClient:
    """JSON-RPC 2.0 client communicating over AppServerTransport."""

    def __init__(self, transport: AppServerTransport):
        self.transport = transport
        self._next_id = 1
        self._pending_requests: Dict[int, asyncio.Future] = {}
        self._notification_handlers: Dict[str, List[Callable[[dict], Any]]] = {}
        self._request_handlers: Dict[str, Callable[[dict], Any]] = {}
        self._read_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self.last_error: Optional[str] = None

    async def start(self) -> None:
        await self.transport.start()
        if self._read_task is None or self._read_task.done():
            self._read_task = asyncio.create_task(self._read_loop())

    async def _read_loop(self) -> None:
        try:
            while self.transport.is_alive():
                line = await self.transport.read_line()
                if not line:
                    break
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    msg = json.loads(stripped)
                except Exception as exc:
                    logger.warning("Unparseable JSON from app-server: %s", exc)
                    continue

                req_id = msg.get("id")
                method = msg.get("method")

                # 1. Response to our client request
                if req_id is not None and req_id in self._pending_requests:
                    fut = self._pending_requests.pop(req_id)
                    if not fut.done():
                        if "error" in msg:
                            err = msg["error"]
                            fut.set_exception(
                                CodexRpcError(
                                    code=err.get("code", -32000),
                                    message=err.get("message", "Unknown error"),
                                    data=err.get("data"),
                                )
                            )
                        else:
                            fut.set_result(msg.get("result"))

                # 2. Server request to client (e.g. approval, user input)
                elif req_id is not None and method:
                    handler = self._request_handlers.get(method) or self._request_handlers.get("*")
                    if handler:
                        try:
                            res = handler(msg)
                            if asyncio.iscoroutine(res):
                                asyncio.create_task(res)
                        except Exception as h_err:
                            logger.error("Error in server request handler for %s: %s", method, h_err)

                # 3. Server notification (no req_id)
                elif method:
                    handlers = self._notification_handlers.get(method, []) + self._notification_handlers.get("*", [])
                    for h in handlers:
                        try:
                            res = h(msg)
                            if asyncio.iscoroutine(res):
                                asyncio.create_task(res)
                        except Exception as n_err:
                            logger.error("Error in notification handler for %s: %s", method, n_err)

        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.error("Error in app-server read loop: %s", exc)
            self.last_error = str(exc)
        finally:
            # Cancel all pending requests
            for req_id, fut in list(self._pending_requests.items()):
                if not fut.done():
                    fut.set_exception(CodexProcessTerminatedError("App-server read loop ended"))
            self._pending_requests.clear()

    async def call(self, method: str, params: Optional[dict] = None, timeout: float = 30.0) -> Any:
        if not self.transport.is_alive():
            await self.start()

        async with self._lock:
            call_id = self._next_id
            self._next_id += 1

        fut = asyncio.get_running_loop().create_future()
        self._pending_requests[call_id] = fut

        payload = {
            "id": call_id,
            "method": method,
            "params": params if params is not None else {}
        }
        await self.transport.send_line(json.dumps(payload))

        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            self._pending_requests.pop(call_id, None)
            raise CodexRpcTimeoutError(f"Request {method} (id={call_id}) timed out after {timeout}s")

    def register_notification_handler(self, method: str, handler: Callable[[dict], Any]) -> None:
        self._notification_handlers.setdefault(method, []).append(handler)

    def register_server_request_handler(self, method: str, handler: Callable[[dict], Any]) -> None:
        self._request_handlers[method] = handler

    async def stop(self) -> None:
        if self._read_task and not self._read_task.done():
            self._read_task.cancel()
        await self.transport.stop()


class CodexThreadLifecycleAdapter:
    """Official Codex lifecycle adapter using app-server protocol."""

    def __init__(self, rpc_client: AppServerRpcClient):
        self.rpc_client = rpc_client
        self.server_info: Dict[str, Any] = {}
        self.pending_human_actions: List[Dict[str, Any]] = []
        self._setup_handlers()

    def _setup_handlers(self) -> None:
        # Register interactive server requests that require human intervention
        interactive_methods = [
            "applyPatchApproval",
            "execCommandApproval",
            "item/commandExecution/requestApproval",
            "item/fileChange/requestApproval",
            "item/permissions/requestApproval",
            "item/tool/requestUserInput",
            "mcpServer/elicitation/request",
        ]
        for m in interactive_methods:
            self.rpc_client.register_server_request_handler(m, self._handle_interactive_request)

    def _handle_interactive_request(self, msg: dict) -> None:
        method = msg.get("method")
        params = msg.get("params", {})
        action_record = {
            "request_id": msg.get("id"),
            "method": method,
            "params": params,
            "timestamp": asyncio.get_event_loop().time(),
        }
        logger.warning("Codex app-server requested human action/approval: %s (id=%s)", method, msg.get("id"))
        self.pending_human_actions.append(action_record)

    def pop_human_actions(self) -> List[Dict[str, Any]]:
        actions = list(self.pending_human_actions)
        self.pending_human_actions.clear()
        return actions

    async def initialize(self, client_name: str = "ai-shuttle-console", client_version: str = "0.1.0") -> dict:
        """Perform official initialization handshake with app-server."""
        res = await self.rpc_client.call("initialize", {
            "clientInfo": {
                "name": client_name,
                "version": client_version,
            },
            "capabilities": {
                "experimentalApi": False,
            }
        })
        self.server_info = res or {}
        return self.server_info

    async def list_loaded_threads(self) -> List[str]:
        """List thread IDs currently loaded in memory via `thread/loaded/list`."""
        res = await self.rpc_client.call("thread/loaded/list", {})
        if isinstance(res, dict):
            return res.get("data", []) or []
        return []

    async def read_thread(self, thread_id: str, include_turns: bool = False) -> dict:
        """Inspect thread metadata and status via `thread/read`."""
        if not thread_id:
            raise ValueError("Thread ID is required")
        res = await self.rpc_client.call("thread/read", {
            "threadId": thread_id,
            "includeTurns": include_turns
        })
        if isinstance(res, dict):
            return res.get("thread", {}) or {}
        return {}

    async def check_thread_state(self, thread_id: str) -> dict:
        """Determine whether thread is loaded/active or cold/notLoaded."""
        loaded_list = await self.list_loaded_threads()
        is_in_loaded_list = thread_id in loaded_list
        thread_info = await self.read_thread(thread_id, include_turns=False)
        status = thread_info.get("status", {})
        status_type = status.get("type", "unknown") if isinstance(status, dict) else str(status)

        is_cold = (not is_in_loaded_list) or (status_type == "notLoaded")

        return {
            "thread_id": thread_id,
            "is_loaded": is_in_loaded_list,
            "status_type": status_type,
            "is_cold": is_cold,
            "cwd": thread_info.get("cwd"),
            "thread_info": thread_info
        }

    async def resume_thread(self, thread_id: str, expected_workspace: Optional[str] = None) -> dict:
        """Activate a cold thread via official `thread/resume` API.

        Validates thread identity and workspace against expected targets.
        """
        if not thread_id:
            raise ValueError("Thread ID cannot be empty")

        try:
            res = await self.rpc_client.call("thread/resume", {
                "threadId": thread_id
            })
        except CodexRpcError as exc:
            if "already has an active writer" in str(exc).lower():
                logger.info(
                    "thread/resume: Thread %s already has an active writer; treating as active/loaded: %s",
                    thread_id,
                    exc,
                )
                return {
                    "thread": {"id": thread_id, "cwd": expected_workspace},
                    "active_writer": True,
                    "status": "already_active",
                }
            raise
        if not isinstance(res, dict):
            raise CodexAppServerError(f"Invalid thread/resume response format: {res}")

        thread_obj = res.get("thread", {})
        res_thread_id = thread_obj.get("id") or res.get("id")
        if not res_thread_id or res_thread_id.strip().lower() != thread_id.strip().lower():
            raise CodexResumeIdentityMismatchError(
                f"Resumed thread ID mismatch: expected {thread_id}, got {res_thread_id}"
            )

        res_cwd = thread_obj.get("cwd") or res.get("cwd")
        if expected_workspace and res_cwd:
            from app.canary import same_path
            if not same_path(res_cwd, expected_workspace):
                raise CodexResumeIdentityMismatchError(
                    f"Resumed thread workspace mismatch: expected {expected_workspace}, got {res_cwd}"
                )

        return res

    async def start_thread(self, cwd: str, title: Optional[str] = None, experimental_api: bool = False) -> dict:
        """Start a new persistent thread via official `thread/start` API."""
        params: Dict[str, Any] = {"cwd": cwd, "experimentalApi": experimental_api}
        if title:
            params["title"] = title
        res = await self.rpc_client.call("thread/start", params)
        if not isinstance(res, dict):
            raise CodexAppServerError(f"Invalid thread/start response format: {res}")
        return res

    async def start_turn(self, thread_id: str, prompt: str) -> dict:
        """Execute exactly one turn via official `turn/start` API."""
        if not thread_id:
            raise ValueError("Thread ID cannot be empty")
        res = await self.rpc_client.call("turn/start", {
            "threadId": thread_id,
            "input": [{"type": "text", "text": prompt}],
        })
        if not isinstance(res, dict):
            raise CodexAppServerError(f"Invalid turn/start response format: {res}")
        return res


class CodexAppServerManager:
    """High-level singleton manager for the Codex app-server lifecycle."""

    def __init__(self, cli_path: str):
        self.cli_path = cli_path
        self.production_shadow_mode = False
        self.transport = AppServerTransport(cli_path)
        self.rpc_client = AppServerRpcClient(self.transport)
        self.adapter = CodexThreadLifecycleAdapter(self.rpc_client)
        self.initialized = False
        self.capabilities: Dict[str, Any] = {}

    async def ensure_started(self) -> None:
        """Start and initialize app-server if not already running."""
        if not self.transport.is_alive() or not self.initialized:
            await self.rpc_client.start()
            server_info = await self.adapter.initialize()
            self.initialized = True
            self.capabilities = {
                "cli_path": self.cli_path,
                "server_info": server_info,
                "thread_resume_supported": True,
                "thread_read_supported": True,
                "thread_loaded_list_supported": True,
                "thread_queue_supported": False,  # experimental, disabled by default
                "experimental_queue_start_enabled": False,
            }

    async def get_capabilities(self) -> Dict[str, Any]:
        await self.ensure_started()
        return dict(self.capabilities)

    async def is_thread_cold(self, thread_id: str) -> Tuple[bool, str]:
        await self.ensure_started()
        state = await self.adapter.check_thread_state(thread_id)
        return state["is_cold"], state["status_type"]

    async def resume_thread_safely(self, thread_id: str, expected_workspace: Optional[str] = None) -> dict:
        if self.production_shadow_mode:
            raise ValueError("PRODUCTION_AUTO_MODE_NOT_AUTHORIZED")
        await self.ensure_started()
        return await self.adapter.resume_thread(thread_id, expected_workspace=expected_workspace)

    async def check_human_actions(self) -> List[Dict[str, Any]]:
        return self.adapter.pop_human_actions()


    async def start_thread(self, cwd: str, title: Optional[str] = None, experimental_api: bool = False) -> dict:
        if self.production_shadow_mode:
            raise ValueError("PRODUCTION_DEVELOPMENT_EXECUTION_FORBIDDEN")
        await self.ensure_started()
        return await self.adapter.start_thread(cwd=cwd, title=title, experimental_api=experimental_api)

    async def start_turn(self, thread_id: str, prompt: str) -> dict:
        if self.production_shadow_mode:
            raise ValueError("PRODUCTION_DEVELOPMENT_EXECUTION_FORBIDDEN")
        await self.ensure_started()
        return await self.adapter.start_turn(thread_id=thread_id, prompt=prompt)

    async def start_readonly_handoff(self, workspace: str) -> dict:
        """Narrow capability: new thread, fixed protocol, OS sandbox read-only.

        No caller-supplied prompt, development thread, resume, or queue endpoint.
        The transactional coordinator consumes its parent capture grant first.
        """
        from app.production_shadow import build_handoff_instruction_prompt
        if not self.production_shadow_mode:
            raise ValueError("PRODUCTION_SHADOW_CONFIG_REQUIRED")
        await self.ensure_started()
        result = await self.rpc_client.call("thread/start", {
            "cwd": workspace, "sandbox": "read-only", "approvalPolicy": "never",
            "experimentalApi": False,
        })
        thread = result.get("thread", {})
        tid = thread.get("id")
        from app.canary import same_path, real_uuid
        real_uuid(tid)
        if not thread.get("cwd") or not same_path(thread["cwd"], workspace):
            raise ValueError("ATOMIC_HANDOFF_WORKSPACE_MISMATCH")
        result = await self.rpc_client.call("turn/start", {
            "threadId": tid, "input": [{"type": "text", "text": build_handoff_instruction_prompt()}],
            "cwd": workspace, "approvalPolicy": "never", "sandboxPolicy": {"type": "readOnly"},
        })
        turn = result.get("turn", {}).get("id")
        if not turn:
            raise ValueError("ATOMIC_HANDOFF_TURN_ID_MISSING")
        return {"thread_id": tid, "turn_id": turn}

    async def shutdown(self) -> None:
        self.initialized = False
        await self.rpc_client.stop()
