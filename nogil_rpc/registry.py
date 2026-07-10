"""Thread-safe registry for remotely callable functions."""

from __future__ import annotations

from collections.abc import Callable
from threading import Lock
from typing import TypeVar

from nogil_rpc.errors import (
    DuplicateFunctionError,
    FunctionNotFoundError,
    FunctionNotRemoteError,
)

F = TypeVar("F", bound=Callable[..., object])


class FunctionRegistry:
    """Maps exposed function names to Python callables."""

    def __init__(self) -> None:
        self._functions: dict[str, Callable[..., object]] = {}
        self._lock = Lock()

    def register(self, fn: F, name: str | None = None) -> F:
        """Register a function under its own name or an explicit name."""
        function_name = name if name is not None else fn.__name__

        if getattr(fn, "__remote__", False) is not True:
            raise FunctionNotRemoteError(
                f"function {fn.__name__!r} is not marked with @remote"
            )

        with self._lock:
            if function_name in self._functions:
                raise DuplicateFunctionError(
                    f"function name {function_name!r} is already registered"
                )
            self._functions[function_name] = fn

        return fn

    def get(self, name: str) -> Callable[..., object]:
        """Return a registered function by name."""
        with self._lock:
            try:
                return self._functions[name]
            except KeyError as exc:
                raise FunctionNotFoundError(
                    f"function name {name!r} is not registered"
                ) from exc

    def list_functions(self) -> tuple[str, ...]:
        """Return registered function names in deterministic order."""
        with self._lock:
            return tuple(sorted(self._functions))
