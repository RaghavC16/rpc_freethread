"""Public API for the No-GIL RPC runtime package."""

from nogil_rpc._version import __version__
from nogil_rpc.errors import (
    ActorNotFoundError,
    ActorOwnershipError,
    ConnectionClosedError,
    DuplicateFunctionError,
    FunctionNotFoundError,
    FunctionNotRemoteError,
    ProtocolError,
    RegistryError,
    RemoteClassNotFoundError,
    RemoteError,
    RpcError,
    SerializationError,
)
from nogil_rpc.object_ref import ObjectRef
from nogil_rpc.rpc_client import ActorHandle, RemoteProcess, connect
from nogil_rpc.runtime import RpcRuntime, remote
from nogil_rpc.serializer import PickleSerializer, Serializer

__all__ = [
    "ActorHandle",
    "ActorNotFoundError",
    "ActorOwnershipError",
    "ConnectionClosedError",
    "DuplicateFunctionError",
    "FunctionNotFoundError",
    "FunctionNotRemoteError",
    "ObjectRef",
    "PickleSerializer",
    "ProtocolError",
    "RegistryError",
    "RemoteClassNotFoundError",
    "RemoteError",
    "RemoteProcess",
    "RpcError",
    "RpcRuntime",
    "SerializationError",
    "Serializer",
    "__version__",
    "connect",
    "remote",
]
