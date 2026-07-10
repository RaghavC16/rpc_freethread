"""Serialization interface and pickle-backed implementation."""

from __future__ import annotations

import pickle
from typing import Any, Protocol, runtime_checkable

from nogil_rpc.errors import SerializationError


@runtime_checkable
class Serializer(Protocol):
    """Converts Python values to bytes and back."""

    def dumps(self, value: Any) -> bytes:
        """Serialize a Python value to bytes."""
        ...

    def loads(self, payload: bytes) -> Any:
        """Deserialize bytes into a Python value."""
        ...


class PickleSerializer:
    """Serializer for trusted Python-to-Python RPC calls."""

    def __init__(self, protocol: int = pickle.HIGHEST_PROTOCOL) -> None:
        self._protocol = protocol

    def dumps(self, value: Any) -> bytes:
        try:
            return pickle.dumps(value, protocol=self._protocol)
        except Exception as exc:
            raise SerializationError(f"failed to serialize value: {exc}") from exc

    def loads(self, payload: bytes) -> Any:
        try:
            return pickle.loads(payload)
        except Exception as exc:
            raise SerializationError(f"failed to deserialize payload: {exc}") from exc
