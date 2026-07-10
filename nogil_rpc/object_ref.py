"""Future-like reference for a remote call result."""

from __future__ import annotations

from enum import Enum
from threading import Condition
from typing import Any

from nogil_rpc.errors import RemoteError


class _ObjectRefState(Enum):
    PENDING = "pending"
    FINISHED = "finished"
    FAILED = "failed"


class ObjectRef:
    """Represents a pending or completed remote result."""

    def __init__(self, call_id: str) -> None:
        self._call_id = call_id
        self._condition = Condition()
        self._state = _ObjectRefState.PENDING
        self._result: Any = None
        self._error: RemoteError | None = None

    @property
    def call_id(self) -> str:
        return self._call_id

    def get(self, timeout: float | None = None) -> Any:
        """Block until the remote call finishes, then return or raise."""
        with self._condition:
            if self._state is _ObjectRefState.PENDING:
                self._condition.wait_for(
                    lambda: self._state is not _ObjectRefState.PENDING,
                    timeout=timeout,
                )

            if self._state is _ObjectRefState.PENDING:
                raise TimeoutError(f"remote call {self._call_id!r} timed out")

            if self._state is _ObjectRefState.FAILED:
                if self._error is None:
                    raise RemoteError("RemoteError", "remote call failed")
                raise self._error

            return self._result

    def ready(self) -> bool:
        """Return whether this reference has completed."""
        with self._condition:
            return self._state is not _ObjectRefState.PENDING

    def set_result(self, result: Any) -> None:
        """Mark this reference as successfully completed."""
        with self._condition:
            self._ensure_pending()
            self._result = result
            self._state = _ObjectRefState.FINISHED
            self._condition.notify_all()

    def set_error(self, error_type: str, message: str) -> None:
        """Mark this reference as failed with a remote error."""
        with self._condition:
            self._ensure_pending()
            self._error = RemoteError(error_type, message)
            self._state = _ObjectRefState.FAILED
            self._condition.notify_all()

    def _ensure_pending(self) -> None:
        if self._state is not _ObjectRefState.PENDING:
            raise RuntimeError(f"remote call {self._call_id!r} is already complete")
