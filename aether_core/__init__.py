"""Aether-Core: Zero-Transit Architecture primitives."""

__author__  = "Nishant Bhatte"
__version__ = "0.4.0"
__license__ = "MIT"

from aether_core._security import (
    AuthConfig,
    AuthError,
    SecurityLimits,
    SeenStampCache,
    secure_compare,
)
from aether_core.crdt import (
    HybridLogicalClock,
    HLCGenerator,
    LWWRegister,
    LWWMap,
    Operation,
    OpKind,
    Node,
)
from aether_core.mesh import (
    MeshNode,
    MeshPubSub,
    WebSocketMeshPubSub,
    serialize_hlc,
    deserialize_hlc,
    serialize_operation,
    deserialize_operation,
)
from aether_core.storage import ChronoLedger
from aether_core.gateway import ClientGateway, compose_hooks
from aether_core.compact import compact, load_snapshot, snapshot_path_for

__all__ = [
    "AuthConfig",
    "AuthError",
    "SecurityLimits",
    "SeenStampCache",
    "secure_compare",
    "HybridLogicalClock",
    "HLCGenerator",
    "LWWRegister",
    "LWWMap",
    "Operation",
    "OpKind",
    "Node",
    "MeshNode",
    "MeshPubSub",
    "WebSocketMeshPubSub",
    "serialize_hlc",
    "deserialize_hlc",
    "serialize_operation",
    "deserialize_operation",
    "ChronoLedger",
    "ClientGateway",
    "compose_hooks",
    "compact",
    "load_snapshot",
    "snapshot_path_for",
]
