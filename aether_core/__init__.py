"""Aether-Core: Zero-Transit Architecture primitives."""

__author__  = "Nishant Bhatte"
__version__ = "0.1.0"
__license__ = "MIT"

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
    serialize_hlc,
    deserialize_hlc,
    serialize_operation,
    deserialize_operation,
)
from aether_core.storage import ChronoLedger
from aether_core.gateway import ClientGateway, compose_hooks

__all__ = [
    "HybridLogicalClock",
    "HLCGenerator",
    "LWWRegister",
    "LWWMap",
    "Operation",
    "OpKind",
    "Node",
    "MeshNode",
    "serialize_hlc",
    "deserialize_hlc",
    "serialize_operation",
    "deserialize_operation",
    "ChronoLedger",
    "ClientGateway",
    "compose_hooks",
]
