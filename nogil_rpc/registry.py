"""Thread-safe registry for remotely callable functions."""

from __future__ import annotations

from collections.abc import Callable
from threading import Lock
from typing import TypeVar

from nogil_rpc.errors import (
    DuplicateFunctionError,
    FunctionNotFoundError,
    FunctionNotRemoteError,
    RemoteClassNotFoundError,
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


class RemoteRegistry:
    """Process-wide registry of remote functions and actor classes."""

    def __init__(self) -> None:
        self._functions: dict[str, Callable[..., object]] = {}
        self._classes: dict[str, type[object]] = {}
        self._lock = Lock()

    def register(self, target: F) -> F:
        """Register a decorated function or actor class by name."""
        if getattr(target, "__remote__", False) is not True:
            raise FunctionNotRemoteError(
                f"target {target.__name__!r} is not marked with @remote"
            )

        name = target.__name__
        with self._lock:
            if name in self._functions or name in self._classes:
                raise DuplicateFunctionError(
                    f"remote name {name!r} is already registered"
                )
            if isinstance(target, type):
                self._classes[name] = target
            else:
                self._functions[name] = target
        return target

    def get_function(self, name: str) -> Callable[..., object]:
        with self._lock:
            try:
                return self._functions[name]
            except KeyError as exc:
                raise FunctionNotFoundError(
                    f"function name {name!r} is not registered"
                ) from exc

    def get_class(self, name: str) -> type[object]:
        with self._lock:
            try:
                return self._classes[name]
            except KeyError as exc:
                raise RemoteClassNotFoundError(
                    f"remote class name {name!r} is not registered"
                ) from exc

    def catalog(self) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Return stable function and actor-class name snapshots."""
        with self._lock:
            return tuple(sorted(self._functions)), tuple(sorted(self._classes))


REMOTE_REGISTRY = RemoteRegistry()
