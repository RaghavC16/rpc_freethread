"""Server-side runtime entry points."""

from __future__ import annotations

import socket
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from threading import Event, Lock, Thread
from typing import Any
from typing import TypeVar, cast

from nogil_rpc.protocol import read_frame, write_frame
from nogil_rpc.registry import FunctionRegistry
from nogil_rpc.serializer import PickleSerializer, Serializer

F = TypeVar("F", bound=Callable[..., object])


def remote(fn: F) -> F:
    """Mark a function as remotely callable."""
    setattr(fn, "__remote__", True)
    return cast(F, fn)


@dataclass(eq=False)
class _RuntimeConnection:
    sock: socket.socket
    write_lock: Lock = field(default_factory=Lock)


class RpcRuntime:
    """Server runtime for registered remote functions."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 50051,
        *,
        max_workers: int = 32,
        serializer: Serializer | None = None,
        backlog: int = 128,
    ) -> None:
        self.host = host
        self.port = port
        self._max_workers = max_workers
        self._serializer = serializer if serializer is not None else PickleSerializer()
        self._backlog = backlog
        self._registry = FunctionRegistry()
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._server_sock: socket.socket | None = None
        self._accept_thread: Thread | None = None
        self._stop_event = Event()
        self._lifecycle_lock = Lock()
        self._connections: set[_RuntimeConnection] = set()
        self._connections_lock = Lock()

    @property
    def address(self) -> tuple[str, int]:
        """Return the bound host and port."""
        return self.host, self.port

    def register(self, fn: F, name: str | None = None) -> F:
        """Expose a marked remote function through this runtime."""
        return self._registry.register(fn, name=name)

    def start(self) -> None:
        """Bind, listen, and start accepting client connections."""
        with self._lifecycle_lock:
            if self._server_sock is not None:
                raise RuntimeError("runtime is already started")

            server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                server_sock.bind((self.host, self.port))
                server_sock.listen(self._backlog)
                self.host, self.port = server_sock.getsockname()
                self._server_sock = server_sock
                self._stop_event.clear()
                self._accept_thread = Thread(
                    target=self._accept_loop,
                    args=(server_sock,),
                    name="nogil-rpc-runtime-accept",
                    daemon=True,
                )
                self._accept_thread.start()
            except Exception:
                server_sock.close()
                self._server_sock = None
                raise

    def wait(self, timeout: float | None = None) -> None:
        """Block until the runtime is stopped or timeout expires."""
        self._stop_event.wait(timeout=timeout)

    def stop(self) -> None:
        """Stop accepting clients, close connections, and stop workers."""
        with self._lifecycle_lock:
            self._stop_event.set()
            server_sock = self._server_sock
            self._server_sock = None
            if server_sock is not None:
                try:
                    server_sock.close()
                except OSError:
                    pass

        with self._connections_lock:
            connections = tuple(self._connections)
            self._connections.clear()

        for connection in connections:
            try:
                connection.sock.close()
            except OSError:
                pass

        self._executor.shutdown(wait=True, cancel_futures=True)

    def _accept_loop(self, server_sock: socket.socket) -> None:
        while not self._stop_event.is_set():
            try:
                client_sock, _ = server_sock.accept()
            except OSError:
                if self._stop_event.is_set():
                    return
                continue

            connection = _RuntimeConnection(client_sock)
            with self._connections_lock:
                self._connections.add(connection)

            Thread(
                target=self._handle_connection,
                args=(connection,),
                name="nogil-rpc-runtime-client",
                daemon=True,
            ).start()

    def _handle_connection(self, connection: _RuntimeConnection) -> None:
        try:
            while not self._stop_event.is_set():
                payload = read_frame(connection.sock)
                message = self._serializer.loads(payload)
                self._handle_call_message(connection, message)
        except Exception:
            pass
        finally:
            with self._connections_lock:
                self._connections.discard(connection)
            try:
                connection.sock.close()
            except OSError:
                pass

    def _handle_call_message(
        self,
        connection: _RuntimeConnection,
        message: Any,
    ) -> None:
        if not isinstance(message, dict):
            raise ValueError("call message must be a dictionary")
        if message.get("type") != "call":
            raise ValueError(f"unexpected message type {message.get('type')!r}")

        call_id = message.get("call_id")
        function_name = message.get("function")
        if not isinstance(call_id, str):
            raise ValueError("call_id must be a string")
        if not isinstance(function_name, str):
            raise ValueError("function must be a string")

        args = message.get("args", [])
        kwargs = message.get("kwargs", {})
        if not isinstance(args, list):
            raise ValueError("args must be a list")
        if not isinstance(kwargs, dict):
            raise ValueError("kwargs must be a dictionary")

        self._executor.submit(
            self._execute_call,
            connection,
            call_id,
            function_name,
            tuple(args),
            kwargs,
        )

    def _execute_call(
        self,
        connection: _RuntimeConnection,
        call_id: str,
        function_name: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        try:
            fn = self._registry.get(function_name)
            result = fn(*args, **kwargs)
        except Exception as exc:
            response = {
                "type": "result",
                "call_id": call_id,
                "ok": False,
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
        else:
            response = {
                "type": "result",
                "call_id": call_id,
                "ok": True,
                "result": result,
            }

        try:
            payload = self._serializer.dumps(response)
        except Exception as exc:
            fallback = {
                "type": "result",
                "call_id": call_id,
                "ok": False,
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
            payload = self._serializer.dumps(fallback)

        write_frame(connection.sock, payload, write_lock=connection.write_lock)
