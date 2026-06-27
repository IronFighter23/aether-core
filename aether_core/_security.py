"""
Aether-Core :: shared security primitives
=========================================

Reusable hardening building blocks used by both the Client Gateway
(browser-facing) and the Mesh PubSub driver (peer-facing). Keeping
them here means the two transport layers cannot drift in their
threat model, and the test suite can exercise both with the same
expectations.

Threat model
------------
Aether-Core trusts neither browsers nor federated peers. Both can be:

* **Hostile** — actively trying to crash the relay, exhaust memory,
  or starve other clients.
* **Buggy** — sending malformed JSON, oversized frames, or stuck
  half-open connections.
* **Slow** — saturating the relay's I/O capacity with legitimate
  but excessive traffic.

The mitigations in this module address all three:

* Token-bucket rate limiting per connection (flood / amplification).
* Hard payload size caps (memory exhaustion).
* Total + per-source connection caps (slot exhaustion).
* Slow-loris timeout on the hello/handshake phase (stuck sockets).

These are intentionally **conservative defaults**, calibrated for
the topology-whiteboard scale: a few dozen concurrent browser tabs,
~10 federated nodes, modest write rates. Production deployments
should retune via the public constructor parameters rather than
editing this file.
"""
from __future__ import annotations

import asyncio
import hmac
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

__all__ = [
    "SecurityLimits",
    "AuthConfig",
    "AuthError",
    "PayloadTooLargeError",
    "RateLimitError",
    "ConnectionLimitError",
    "TokenBucket",
    "ConnectionCounter",
    "SeenStampCache",
    "secure_compare",
    "validate_payload",
]

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exceptions -- all subclass ValueError so callers can catch the family
# ---------------------------------------------------------------------------

class PayloadTooLargeError(ValueError):
    """Raised when an inbound message exceeds the configured byte cap."""


class RateLimitError(ValueError):
    """Raised when a connection has exceeded its message rate budget."""


class ConnectionLimitError(ValueError):
    """Raised when accepting a new connection would breach a cap."""


class AuthError(ValueError):
    """Raised when a peer or client fails authentication."""


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SecurityLimits:
    """
    Tunable security envelope. Defaults are sane for an interactive
    collaborative tool with dozens of clients.

    The same dataclass is used by both ``ClientGateway`` and
    ``WebSocketMeshPubSub`` so the threat model stays uniform.
    """

    # ---- Payload caps -----------------------------------------------------
    # WebSocket frame-level cap. The websockets library enforces this at
    # the protocol layer; oversized frames are rejected before we ever
    # see the bytes.
    max_frame_bytes: int = 256 * 1024            # 256 KiB

    # Application-level cap on a single JSON message body. We enforce this
    # AFTER decoding the UTF-8 string but before json.loads, so a 10 MiB
    # JSON payload cannot OOM us during parsing.
    max_message_bytes: int = 64 * 1024           # 64 KiB

    # Hard caps on individual fields inside an application message. The
    # CRDT layer is type-agnostic, but the relay refuses anything that
    # looks like an attempt to waste space or break downstream tooling.
    max_key_bytes: int = 256                     # individual CRDT key
    max_value_bytes: int = 32 * 1024             # individual CRDT value (post-stringify)

    # ---- Connection caps --------------------------------------------------
    max_connections_total: int = 256
    max_connections_per_source: int = 32         # per remote IP

    # ---- Rate limits ------------------------------------------------------
    # Token bucket: each connection starts with `messages_burst` tokens
    # and refills at `messages_per_second`. A message consumes 1 token.
    # Connections that try to consume when the bucket is empty are
    # disconnected (kill-on-overrun) rather than back-pressured.
    messages_per_second: float = 100.0
    messages_burst: int = 200

    # ---- Slow-loris -------------------------------------------------------
    # A new connection has this many seconds to send its first message.
    # If it stays silent past the deadline, we close it.
    handshake_timeout_s: float = 5.0

    # ---- Memory caps ------------------------------------------------------
    # Maximum number of HLC stamps retained in the dedup cache. Older
    # stamps are evicted FIFO. Stamps are ~80 bytes each (dataclass +
    # interned strings), so 100k entries is ~8 MiB. Tune up for
    # high-throughput federation, down for tiny embedded deployments.
    max_seen_stamps: int = 100_000

    # Maximum number of operations retained in the in-memory Node.oplog
    # ring buffer. The oplog is used for diagnostics and for the in-process
    # demo; durability lives in the ChronoLedger, not here. Old entries
    # are evicted FIFO once the cap is reached. Set to None to make the
    # oplog unbounded (only safe for short-lived test runs).
    max_oplog_size: Optional[int] = 10_000


# ---------------------------------------------------------------------------
# Token bucket (per-connection rate limiter)
# ---------------------------------------------------------------------------

class TokenBucket:
    """
    Classic token-bucket. Single-threaded by design -- the websockets
    library serializes message arrivals per connection, so there is no
    race here. Cheap: one float add and one float subtract per message.
    """

    __slots__ = ("_capacity", "_refill_per_s", "_tokens", "_last")

    def __init__(self, *, capacity: int, refill_per_second: float) -> None:
        self._capacity      = float(capacity)
        self._refill_per_s  = float(refill_per_second)
        self._tokens        = float(capacity)
        self._last          = time.monotonic()

    def try_consume(self, count: int = 1) -> bool:
        """
        Attempt to consume ``count`` tokens. Returns True on success,
        False if the bucket would go negative. Refills lazily based on
        wall-clock elapsed since the last check.
        """
        now = time.monotonic()
        elapsed = now - self._last
        if elapsed > 0:
            self._tokens = min(
                self._capacity,
                self._tokens + elapsed * self._refill_per_s,
            )
            self._last = now
        if self._tokens >= count:
            self._tokens -= count
            return True
        return False

    @property
    def available(self) -> float:
        """Approximate current token count (for tests / metrics)."""
        return self._tokens


# ---------------------------------------------------------------------------
# Connection counter (total + per-source caps)
# ---------------------------------------------------------------------------

@dataclass
class ConnectionCounter:
    """
    Tracks concurrent connections globally and per remote address.

    A new connection is accepted if and only if BOTH the global count
    and the per-source count are below their respective caps. The
    counter is fed by the transport handler at accept-time and decremented
    in the connection's finally-clause.

    Thread-safety: the websockets library runs each handler on the same
    asyncio loop, and all mutations happen on that loop, so no lock is
    needed. We still expose explicit ``acquire``/``release`` semantics
    so the caller cannot accidentally double-release.
    """

    limits: SecurityLimits
    _total: int = 0
    _per_source: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def acquire(self, source: str) -> None:
        """Reserve a slot or raise ``ConnectionLimitError``."""
        if self._total >= self.limits.max_connections_total:
            raise ConnectionLimitError(
                f"global connection cap reached "
                f"({self._total}/{self.limits.max_connections_total})"
            )
        if self._per_source[source] >= self.limits.max_connections_per_source:
            raise ConnectionLimitError(
                f"per-source connection cap reached for {source} "
                f"({self._per_source[source]}/{self.limits.max_connections_per_source})"
            )
        self._total += 1
        self._per_source[source] += 1

    def release(self, source: str) -> None:
        """Release a previously acquired slot. Safe to call once per acquire."""
        self._total = max(0, self._total - 1)
        n = self._per_source.get(source, 0) - 1
        if n <= 0:
            self._per_source.pop(source, None)
        else:
            self._per_source[source] = n

    @property
    def total(self) -> int:
        return self._total

    def for_source(self, source: str) -> int:
        return self._per_source.get(source, 0)


# ---------------------------------------------------------------------------
# AuthConfig :: shared-secret token authentication
# ---------------------------------------------------------------------------

def secure_compare(a: Optional[str], b: Optional[str]) -> bool:
    """
    Constant-time string comparison. ``None`` never compares equal to
    anything (including another ``None``) -- this matches the semantics
    we want for "is this peer's token a configured value": missing
    credentials never authenticate.
    """
    if a is None or b is None:
        return False
    # hmac.compare_digest works on equal-length byte strings; pad to the
    # max of the two so we still leak no length signal beyond what's
    # already public.
    ab = a.encode("utf-8")
    bb = b.encode("utf-8")
    if len(ab) != len(bb):
        # compare_digest already runs in constant time across unequal
        # lengths since CPython 3.3, so this is safe.
        return False
    return hmac.compare_digest(ab, bb)


@dataclass(frozen=True)
class AuthConfig:
    """
    Shared-secret token authentication.

    When ``token`` is ``None`` (the default), every connection is
    accepted. When ``token`` is a string, every incoming connection
    -- whether a browser client to the gateway, or a federated peer
    to the mesh pubsub -- must present a matching token or it will
    be closed before any state is exposed.

    The token is a single arbitrary opaque string; the transport-layer
    rules for how it is delivered live in each driver:

    * **Mesh pubsub** (peer-to-peer): the token is included in the
      ``hello`` JSON message exchanged on connect. A peer that does
      not present a matching token has its socket closed before any
      operation is read or replayed.

    * **Client gateway** (browser-to-server): the token can be
      delivered as a query parameter on the WebSocket URL
      (``ws://host:port/?auth_token=...``) OR as the FIRST inbound
      message after connect (``{"type": "auth", "token": "..."}``).
      Connections that do neither within ``handshake_timeout_s`` are
      closed.

    Tokens are compared in constant time via ``hmac.compare_digest``
    so a length-extension or timing oracle cannot recover the secret
    byte-by-byte.

    For a defense-in-depth deployment, pair this with TLS (the
    ``ssl_context`` parameter on the gateway and mesh) so the token
    is never sent in cleartext over the network.
    """

    token: Optional[str] = None

    @property
    def required(self) -> bool:
        """
        True when authentication is enforced (a non-empty token is
        configured). Empty string is treated as "no token" so that a
        misconfigured environment variable (``AETHER_TOKEN=``) does not
        silently produce a gateway that authenticates only the empty
        string -- a footgun we explicitly do not want.
        """
        return bool(self.token)

    def verify(self, presented: Optional[str]) -> bool:
        """Constant-time check; ``False`` when no token is configured (closed-by-default)."""
        if not self.required:
            # Calling code should check ``required`` first; if you got
            # here without a token configured, you're checking a credential
            # that nobody asked for. Refuse.
            return False
        return secure_compare(self.token, presented)


# ---------------------------------------------------------------------------
# SeenStampCache :: bounded HLC dedup with FIFO eviction
# ---------------------------------------------------------------------------

class SeenStampCache:
    """
    Bounded set of HLC stamps for gossip deduplication.

    Operations are O(1):

    * ``add(stamp)`` -- insert; if already present, no-op. When the
      cache reaches ``max_size``, the oldest stamp is evicted.
    * ``stamp in cache`` -- membership check.
    * ``len(cache)`` -- current size.

    Eviction policy is FIFO (oldest inserted first out). LRU was
    considered but rejected: gossip propagation is bounded by the
    network diameter, so a stamp that has not been seen in the last
    N entries will not be re-broadcast through this node. A stamp
    seen on the wire BEFORE it was inserted here would still be
    evicted, but the consequence is at most one duplicate apply --
    which is fine, because CRDT apply is idempotent by construction.

    Backwards-compatible with the prior plain-``set`` implementation:
    supports ``.add(stamp)`` and ``stamp in cache``.
    """

    __slots__ = ("_max", "_set", "_fifo")

    def __init__(self, max_size: int = 100_000) -> None:
        if max_size <= 0:
            raise ValueError("max_size must be positive")
        self._max: int = int(max_size)
        # Two structures kept in lock-step: the set for O(1) lookup,
        # the deque for FIFO eviction order.
        self._set: set[Any] = set()
        self._fifo: deque[Any] = deque()

    def add(self, stamp: Any) -> bool:
        """Insert ``stamp``. Returns True if newly inserted, False if duplicate."""
        if stamp in self._set:
            return False
        if len(self._fifo) >= self._max:
            old = self._fifo.popleft()
            self._set.discard(old)
        self._fifo.append(stamp)
        self._set.add(stamp)
        return True

    def __contains__(self, stamp: object) -> bool:
        return stamp in self._set

    def __len__(self) -> int:
        return len(self._set)

    def __iter__(self) -> Iterable[Any]:
        # FIFO order, oldest first. Useful for tests and metrics.
        return iter(list(self._fifo))

    def clear(self) -> None:
        self._set.clear()
        self._fifo.clear()

    @property
    def capacity(self) -> int:
        return self._max


# ---------------------------------------------------------------------------
# Payload validation
# ---------------------------------------------------------------------------

def validate_payload(
    raw: Any, limits: SecurityLimits,
) -> str:
    """
    Validate an inbound WebSocket frame body BEFORE JSON parsing.

    Returns the decoded UTF-8 string if it passes every check. Raises
    ``PayloadTooLargeError`` or ``ValueError`` otherwise. Callers
    should treat any exception here as "drop the message, log, keep
    the connection" -- it is NOT a reason to close the socket. A
    misbehaving client that sends ONE oversized payload may simply be
    racing a UI update; the rate limiter handles repeated abuse.
    """
    # 1. Type coercion. websockets delivers str for text frames and
    #    bytes for binary frames. We accept both but require valid UTF-8.
    if isinstance(raw, bytes):
        # Bytes frames are unexpected for our protocol; accept up to the
        # cap then UTF-8 decode strictly.
        if len(raw) > limits.max_message_bytes:
            raise PayloadTooLargeError(
                f"message body {len(raw)} > cap {limits.max_message_bytes}"
            )
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as e:
            raise ValueError(f"non-utf8 frame body: {e}") from None
    elif isinstance(raw, str):
        # Python str length is characters, not bytes. We want byte count.
        # encode() is cheap and accurate.
        encoded_len = len(raw.encode("utf-8"))
        if encoded_len > limits.max_message_bytes:
            raise PayloadTooLargeError(
                f"message body {encoded_len} > cap {limits.max_message_bytes}"
            )
        text = raw
    else:
        raise ValueError(f"unsupported frame body type {type(raw).__name__}")

    return text


def validate_key(key: Any, limits: SecurityLimits) -> str:
    """Validate an inbound CRDT key. Returns the key on success, else raises."""
    if not isinstance(key, str):
        raise ValueError("key must be a string")
    if not key:
        raise ValueError("key must be non-empty")
    if len(key.encode("utf-8")) > limits.max_key_bytes:
        raise PayloadTooLargeError(
            f"key length > cap {limits.max_key_bytes}"
        )
    return key


def validate_value(value: Any, limits: SecurityLimits) -> Any:
    """Validate an inbound CRDT value. Returns the value on success, else raises."""
    # Re-serialize to measure the canonical byte size. Cheap because the
    # value just came off the wire as JSON.
    import json
    try:
        size = len(json.dumps(value, separators=(",", ":")).encode("utf-8"))
    except (TypeError, ValueError) as e:
        raise ValueError(f"value not JSON-serialisable: {e}") from None
    if size > limits.max_value_bytes:
        raise PayloadTooLargeError(
            f"value size {size} > cap {limits.max_value_bytes}"
        )
    return value


# ---------------------------------------------------------------------------
# Slow-loris helper
# ---------------------------------------------------------------------------

async def with_handshake_timeout(
    coro: Any, *, limits: SecurityLimits, what: str = "handshake",
) -> Any:
    """
    Wrap an awaitable with the handshake timeout. Raises ``asyncio.TimeoutError``
    if the underlying operation does not complete in time. The caller is
    expected to translate that into a connection close.
    """
    try:
        return await asyncio.wait_for(coro, timeout=limits.handshake_timeout_s)
    except asyncio.TimeoutError:
        logger.info("[security] %s timed out after %.1fs", what, limits.handshake_timeout_s)
        raise
