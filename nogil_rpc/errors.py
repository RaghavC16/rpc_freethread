"""Shared exception types for nogil_rpc."""


class RpcError(Exception):
    """Base class for RPC runtime errors."""


class SerializationError(RpcError):
    """Raised when a value cannot be serialized or deserialized."""


class ProtocolError(RpcError):
    """Raised when a peer sends invalid protocol data."""


class ConnectionClosedError(RpcError):
    """Raised when a connection closes before an operation completes."""


class RegistryError(RpcError):
    """Base class for function registry errors."""


class FunctionNotRemoteError(RegistryError):
    """Raised when registering a function that is not marked remote."""


class DuplicateFunctionError(RegistryError):
    """Raised when registering a duplicate function name."""


class FunctionNotFoundError(RegistryError):
    """Raised when a requested function is not registered."""


class RemoteClassNotFoundError(RegistryError):
    """Raised when a requested remote class is not registered."""


class ActorNotFoundError(RpcError):
    """Raised when a requested actor instance does not exist."""


class RemoteError(RpcError):
    """Raised by clients when a remote function fails."""

    def __init__(self, error_type: str, message: str) -> None:
        self.error_type = error_type
        super().__init__(f"{error_type}: {message}")
