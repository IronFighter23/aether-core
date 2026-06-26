# Documentation

Welcome to the Aether-Core docs. The repository's top-level
[README](../README.md) tells you *what* Aether-Core is; this folder
tells you *how* to use it.

## Where to start

**Have never used Aether-Core before** → start with
[**Getting Started**](getting-started.md). A 10-minute tutorial that
walks you from `git clone` to a working two-tab collaborative app.

**Want to understand the mental model** → read
[**Concepts**](concepts.md). What CRDTs are, why we use HLC stamps,
what "zero-transit" means in concrete terms, and how the snapshot vs
ledger boundary works.

**Looking up a specific method or option** → reach for the API
references:
- [**JavaScript API**](api-javascript.md) — the browser-side
  `Aether` class. Every method, every callback, every option.
- [**Python API**](api-python.md) — the relay. `MeshNode`,
  `ClientGateway`, `ChronoLedger`, `SecurityLimits`, plus how to
  write a custom `MeshPubSub` driver.

**Building a specific feature** → check the
[**Recipes**](recipes.md). Counters, lists, presence cursors, undo,
schema versioning, optimistic UI patterns.

**Putting this in production** → read
[**Deployment**](deployment.md). Reverse proxy + TLS, Docker, multi-
node federation, scaling guidance, observability.

**Something isn't working** → see
[**Troubleshooting**](troubleshooting.md). Common errors and their fixes.

## Adjacent reading

These live at the repo root, not under `docs/`, because they're
referenced from package metadata and GitHub conventions:

- [**SECURITY.md**](../SECURITY.md) — threat model, every applied
  mitigation, disclosure policy.
- [**BENCHMARKS.md**](../BENCHMARKS.md) — real performance numbers
  with methodology.
- [**README.md**](../README.md) — the project overview.

## Documentation conventions

Throughout these docs:

- **Bash commands** assume you're inside the repository root with
  `uv sync` already run. If you're on raw Python, substitute
  `python -m` for `uv run`.
- **Code examples** are runnable. If you copy-paste an example and
  it fails, that's a bug in the docs; please open an issue.
- **Wire-protocol examples** match the canonical block in
  [`README.md`](../README.md#wire-protocol), which is enforced by
  [`tests/test_protocol_conformance.py`](../tests/test_protocol_conformance.py).
  The docs cannot silently drift from the running code.
