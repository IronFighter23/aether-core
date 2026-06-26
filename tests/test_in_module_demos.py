"""
Wrap the in-module self-tests as pytest cases so ``uv run pytest`` runs
the full suite (protocol + crdt + mesh + storage + gateway + compact).

Some of the in-module demos are sync (crdt, compact) and some are async
(mesh, storage, gateway). The sync ones additionally call
``asyncio.run`` internally, which conflicts with pytest-asyncio's loop.
To keep the test surface uniform we shell out to ``python -m`` for the
sync demos and ``await`` directly for the async ones.
"""
from __future__ import annotations

import importlib
import inspect
import subprocess
import sys
from typing import Callable

import pytest

ASYNC_DEMOS: list[tuple[str, str]] = [
    ("mesh",    "aether_core.mesh"),
    ("storage", "aether_core.storage"),
    ("gateway", "aether_core.gateway"),
]

SYNC_DEMOS: list[tuple[str, str]] = [
    ("crdt",    "aether_core.crdt"),
    ("compact", "aether_core.compact"),
]


@pytest.mark.parametrize(("name", "module"), ASYNC_DEMOS,
                         ids=[m[0] for m in ASYNC_DEMOS])
async def test_async_in_module_demo(name: str, module: str) -> None:
    mod = importlib.import_module(module)
    demo: Callable = mod._demo  # type: ignore[attr-defined]
    assert inspect.iscoroutinefunction(demo), f"{module}._demo is not async"
    await demo()


@pytest.mark.parametrize(("name", "module"), SYNC_DEMOS,
                         ids=[m[0] for m in SYNC_DEMOS])
def test_sync_in_module_demo(name: str, module: str) -> None:
    """
    Run sync demos in a subprocess. They internally call asyncio.run(),
    which cannot be nested under pytest-asyncio's running loop, so we
    invoke them with a fresh interpreter and assert exit code 0.
    """
    result = subprocess.run(
        [sys.executable, "-m", module],
        capture_output=True, text=True, timeout=120,
    )
    assert result.returncode == 0, (
        f"{module} demo exited {result.returncode}\n"
        f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
    )
    assert "PROVEN" in result.stdout, (
        f"{module} demo did not print PROVEN. stdout:\n{result.stdout}"
    )
