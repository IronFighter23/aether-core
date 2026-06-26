"""Console-script entry points exposed via ``pyproject.toml``."""
from __future__ import annotations

import sys
from pathlib import Path


def demo() -> None:
    """Launch the full demo (gateway + mesh + ledger + HTTP server)."""
    repo_root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(repo_root))
    import run_demo  # noqa: F401  -- runs on import (asyncio.run at bottom)


def benchmark() -> None:
    """Run the public benchmark suite and print the results."""
    repo_root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(repo_root))
    from benchmarks.run_benchmarks import main as _main
    _main()
