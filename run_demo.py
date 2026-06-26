"""
Aether-Core :: full-stack demo launcher
=======================================

Spins up every layer plus a tiny stdlib HTTP server so a browser can
load ``web/index.html`` and exercise the system end-to-end:

    python run_demo.py

Then open http://localhost:8080/ in two browser tabs and add devices,
drag them around, wire them together -- changes propagate live to every
tab. State is persisted to ``./ledger_demo.jsonl`` in the project root
and survives a restart.

Press Ctrl+C to shut down cleanly.
"""
from __future__ import annotations

import asyncio
import http.server
import logging
import signal
import socketserver
import threading
from pathlib import Path
from typing import Any, Optional

from aether_core.crdt import Operation
from aether_core.gateway import ClientGateway, compose_hooks
from aether_core.mesh import MeshNode
from aether_core.storage import ChronoLedger

PROJECT_ROOT = Path(__file__).resolve().parent
LEDGER_PATH  = PROJECT_ROOT / "ledger_demo.jsonl"
WEB_DIR      = PROJECT_ROOT / "web"

MESH_PORT     = 8201
GATEWAY_PORT  = 8211
HTTP_PORT     = 8080


class _SilentHTTPHandler(http.server.SimpleHTTPRequestHandler):
    """SimpleHTTPRequestHandler that doesn't spam the console."""
    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
        return


def _start_http_server() -> tuple[socketserver.ThreadingTCPServer, threading.Thread]:
    """Start a stdlib static-file server in a background thread."""
    class _Handler(_SilentHTTPHandler):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, directory=str(WEB_DIR), **kwargs)

    socketserver.ThreadingTCPServer.allow_reuse_address = True
    httpd = socketserver.ThreadingTCPServer(("127.0.0.1", HTTP_PORT), _Handler)
    thread = threading.Thread(target=httpd.serve_forever, name="http-static",
                              daemon=True)
    thread.start()
    return httpd, thread


async def _run() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    # ----- assemble the stack ---------------------------------------------
    # ChronoLedger and ClientGateway both subscribe to the mesh's on_op
    # stream. compose_hooks fans the single mesh callback out to both.
    ledger = ChronoLedger(LEDGER_PATH)
    placeholder: dict[str, ClientGateway] = {}

    async def composed(op: Operation[Any, Any], src: Optional[str]) -> None:
        await ledger.on_op(op, src)
        gw = placeholder.get("g")
        if gw is not None:
            await gw.on_op(op, src)

    mesh = MeshNode("alpha", port=MESH_PORT, on_op=composed)
    gateway = ClientGateway(mesh, host="127.0.0.1", port=GATEWAY_PORT)
    placeholder["g"] = gateway

    await ledger.boot(mesh)
    await mesh.start()
    await gateway.start()

    httpd, _ = _start_http_server()

    print()
    print("┌──────────────────────────────────────────────────────────────┐")
    print("│  Aether-Core V3 demo is live.                                │")
    print("│                                                              │")
    print(f"│    Topology   : http://localhost:{HTTP_PORT}/                       │")
    print(f"│    Kanban     : http://localhost:{HTTP_PORT}/demos/kanban.html      │")
    print(f"│    Markdown   : http://localhost:{HTTP_PORT}/demos/markdown.html    │")
    print(f"│    Gateway    : ws://localhost:{GATEWAY_PORT}                          │")
    print(f"│    Mesh peer  : ws://localhost:{MESH_PORT}                          │")
    print(f"│    Ledger     : {str(LEDGER_PATH.name):<44} │")
    print("│                                                              │")
    print("│  Open any URL in two tabs and watch them sync in real time.  │")
    print("│  Stop the server, refresh — pages render from localStorage.  │")
    print("│  Press Ctrl+C to shut down.                                  │")
    print("└──────────────────────────────────────────────────────────────┘")
    print()

    # ----- run until interrupted -----------------------------------------
    stop = asyncio.Event()
    loop = asyncio.get_event_loop()

    def _request_stop(*_args: Any) -> None:
        """Ask the main coroutine to wind down. Safe from any context."""
        loop.call_soon_threadsafe(stop.set)

    # Try the asyncio-native way first (Unix). On Windows that raises
    # NotImplementedError, so fall back to a plain signal.signal handler.
    # The plain handler is fine here because asyncio.run() executes the
    # loop on the main thread, which is where signal.signal must be called.
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            try:
                signal.signal(sig, _request_stop)
            except (ValueError, OSError):
                pass

    # On Windows, Python's interpreter only checks for pending signals at
    # bytecode boundaries; long-running asyncio waits in C can starve the
    # handler. Polling every 500ms guarantees we yield back to the
    # interpreter often enough that Ctrl+C is processed promptly.
    try:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass

    print("\nshutting down…")
    httpd.shutdown()
    await gateway.stop()
    await mesh.stop()
    await ledger.close()
    print("clean exit.")


if __name__ == "__main__":
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        # Defensive: a Ctrl+C arriving before our handler installs (or
        # between asyncio.run returning and process exit) should not
        # print an ugly traceback.
        print("\ninterrupted.")
