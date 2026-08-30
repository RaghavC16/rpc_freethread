"""Server-side runtime entry points."""

from __future__ import annotations

import inspect
import socket
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from threading import Event, Lock, Thread
from typing import Any, TypeVar

from nogil_rpc.errors import ActorNotFoundError, ConnectionClosedError, ProtocolError
from nogil_rpc.protocol import read_frame, write_frame
from nogil_rpc.registry import REMOTE_REGISTRY
from nogil_rpc.serializer import PickleSerializer, Serializer

F = TypeVar("F", bound=Callable[..., object])


def remote(fn: F) -> F:
    """Expose a function or actor class to runtimes in this process."""
    setattr(fn, "__remote__", True)
    return REMOTE_REGISTRY.register(fn)


@dataclass(eq=False)
class _RuntimeConnection:
    sock: socket.socket
    write_lock: Lock = field(default_factory=Lock)
    closed: Event = field(default_factory=Event)


@dataclass(eq=False)
class _ActorEntry:
    instance: object
    owner: _RuntimeConnection
    executor: ThreadPoolExecutor = field(
        default_factory=lambda: ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="nogil-rpc-actor",
        )
    )


class RpcRuntime:
    """Server runtime for functions exposed with @remote."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 50051,
        *,
        max_workers: int = 32,
        serializer: Serializer | None = None,
        backlog: int = 128,
        max_frame_size: int = 64 * 1024 * 1024,
    ) -> None:
        if type(max_frame_size) is not int or max_frame_size <= 0:
            raise ValueError("max_frame_size must be a positive integer")
        self.host = host
        self.port = port
        self._max_workers = max_workers
        self._serializer = serializer if serializer is not None else PickleSerializer()
        self._backlog = backlog
        self._max_frame_size = max_frame_size
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._server_sock: socket.socket | None = None
        self._accept_thread: Thread | None = None
        self._stop_event = Event()
        self._lifecycle_lock = Lock()
        self._connections: set[_RuntimeConnection] = set()
        self._connections_lock = Lock()
        self._actors: dict[str, _ActorEntry] = {}
        self._actors_lock = Lock()
        self._closed = False

    @property
    def address(self) -> tuple[str, int]:
        """Return the bound host and port."""
        return self.host, self.port

    def start(self) -> None:
        """Bind, listen, and start accepting client connections."""
        with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError("runtime cannot be restarted after stop")
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
            if self._closed:
                return
            self._closed = True
            self._stop_event.set()
            server_sock = self._server_sock
            self._server_sock = None
            if server_sock is not None:
                try:
                    server_sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    server_sock.close()
                except OSError:
                    pass

        with self._connections_lock:
            connections = tuple(self._connections)
            self._connections.clear()

        for connection in connections:
            connection.closed.set()
            try:
                connection.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                connection.sock.close()
            except OSError:
                pass

        self._executor.shutdown(wait=True, cancel_futures=True)

        with self._actors_lock:
            actors = tuple(self._actors.values())
            self._actors.clear()
        for actor in actors:
            actor.executor.shutdown(wait=True, cancel_futures=True)

    def __enter__(self) -> RpcRuntime:
        self.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.stop()

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
            self._send_catalog(connection)
            while not self._stop_event.is_set():
                payload = read_frame(
                    connection.sock, max_frame_size=self._max_frame_size
                )
                message = self._serializer.loads(payload)
                self._handle_message(connection, message)
        except Exception:
            pass
        finally:
            connection.closed.set()
            with self._connections_lock:
                self._connections.discard(connection)
            try:
                connection.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                connection.sock.close()
            except OSError:
                pass
            self._remove_connection_actors(connection)

    def _send_catalog(self, connection: _RuntimeConnection) -> None:
        functions, actors = REMOTE_REGISTRY.catalog()
        payload = self._serializer.dumps(
            {"type": "catalog", "functions": functions, "actors": actors}
        )
        write_frame(
            connection.sock,
            payload,
            write_lock=connection.write_lock,
            max_frame_size=self._max_frame_size,
        )

    def _handle_message(
        self,
        connection: _RuntimeConnection,
        message: Any,
    ) -> None:
        if not isinstance(message, dict):
            raise ProtocolError("request message must be a dictionary")

        message_type = message.get("type")
        if message_type == "call":
            self._handle_function_call(connection, message)
            return
        if message_type == "create_actor":
            self._handle_actor_create(connection, message)
            return
        if message_type == "call_actor":
            self._handle_actor_call(connection, message)
            return
        if message_type == "destroy_actor":
            self._handle_actor_destroy(connection, message)
            return
        raise ProtocolError(f"unexpected message type {message_type!r}")

    def _handle_function_call(
        self,
        connection: _RuntimeConnection,
        message: dict[str, Any],
    ) -> None:
        call_id = self._require_string(message, "call_id")
        function_name = self._require_string(message, "function")
        args, kwargs = self._require_arguments(message)

        self._executor.submit(
            self._execute_call,
            connection,
            call_id,
            function_name,
            args,
            kwargs,
        )

    def _handle_actor_create(
        self,
        connection: _RuntimeConnection,
        message: dict[str, Any],
    ) -> None:
        call_id = self._require_string(message, "call_id")
        actor_id = self._require_string(message, "actor_id")
        class_name = self._require_string(message, "class")
        args, kwargs = self._require_arguments(message)

        self._executor.submit(
            self._create_actor,
            connection,
            call_id,
            actor_id,
            class_name,
            args,
            kwargs,
        )

    def _handle_actor_call(
        self,
        connection: _RuntimeConnection,
        message: dict[str, Any],
    ) -> None:
        call_id = self._require_string(message, "call_id")
        actor_id = self._require_string(message, "actor_id")
        method_name = self._require_string(message, "method")
        args, kwargs = self._require_arguments(message)

        with self._actors_lock:
            actor = self._actors.get(actor_id)
            if actor is not None:
                actor.executor.submit(
                    self._execute_actor_method,
                    connection,
                    call_id,
                    actor.instance,
                    method_name,
                    args,
                    kwargs,
                )
                return

        self._executor.submit(
            self._send_error,
            connection,
            call_id,
            ActorNotFoundError(f"actor {actor_id!r} does not exist"),
        )

    def _handle_actor_destroy(
        self,
        connection: _RuntimeConnection,
        message: dict[str, Any],
    ) -> None:
        call_id = self._require_string(message, "call_id")
        actor_id = self._require_string(message, "actor_id")
        self._executor.submit(
            self._destroy_actor,
            connection,
            call_id,
            actor_id,
        )

    @staticmethod
    def _require_string(message: dict[str, Any], field_name: str) -> str:
        value = message.get(field_name)
        if not isinstance(value, str):
            raise ProtocolError(f"{field_name} must be a string")
        return value

    @staticmethod
    def _require_arguments(
        message: dict[str, Any],
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        args = message.get("args", [])
        kwargs = message.get("kwargs", {})
        if not isinstance(args, list):
            raise ProtocolError("args must be a list")
        if not isinstance(kwargs, dict):
            raise ProtocolError("kwargs must be a dictionary")
        return tuple(args), kwargs

    def _execute_call(
        self,
        connection: _RuntimeConnection,
        call_id: str,
        function_name: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        try:
            fn = REMOTE_REGISTRY.get_function(function_name)
            result = self._invoke_sync(fn, args, kwargs)
        except Exception as exc:
            self._send_error(connection, call_id, exc)
        else:
            self._send_result(connection, call_id, result)

    def _create_actor(
        self,
        connection: _RuntimeConnection,
        call_id: str,
        actor_id: str,
        class_name: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        actor: _ActorEntry | None = None
        inserted = False
        try:
            actor_class = REMOTE_REGISTRY.get_class(class_name)
            instance = actor_class(*args, **kwargs)
            actor = _ActorEntry(instance, connection)
            with self._actors_lock:
                if connection.closed.is_set():
                    raise ConnectionClosedError("client disconnected during actor creation")
                if actor_id in self._actors:
                    raise ValueError(f"actor {actor_id!r} already exists")
                self._actors[actor_id] = actor
                inserted = True
        except Exception as exc:
            if actor is not None and not inserted:
                actor.executor.shutdown(wait=True, cancel_futures=True)
            if not connection.closed.is_set():
                self._send_error(connection, call_id, exc)
        else:
            self._send_result(connection, call_id, None)

    def _execute_actor_method(
        self,
        connection: _RuntimeConnection,
        call_id: str,
        instance: object,
        method_name: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        try:
            if method_name.startswith("_"):
                raise AttributeError("private actor methods are not remotely callable")
            method = getattr(instance, method_name)
            if not callable(method):
                raise TypeError(f"actor attribute {method_name!r} is not callable")
            result = self._invoke_sync(method, args, kwargs)
        except Exception as exc:
            self._send_error(connection, call_id, exc)
        else:
            self._send_result(connection, call_id, result)

    def _destroy_actor(
        self,
        connection: _RuntimeConnection,
        call_id: str,
        actor_id: str,
    ) -> None:
        with self._actors_lock:
            actor = self._actors.pop(actor_id, None)
        if actor is None:
            self._send_error(
                connection,
                call_id,
                ActorNotFoundError(f"actor {actor_id!r} does not exist"),
            )
            return
        actor.executor.shutdown(wait=True, cancel_futures=False)
        self._send_result(connection, call_id, None)

    def _remove_connection_actors(self, connection: _RuntimeConnection) -> None:
        with self._actors_lock:
            actor_ids = [
                actor_id
                for actor_id, actor in self._actors.items()
                if actor.owner is connection
            ]
            actors = [self._actors.pop(actor_id) for actor_id in actor_ids]
        for actor in actors:
            actor.executor.shutdown(wait=True, cancel_futures=True)

    @staticmethod
    def _invoke_sync(
        target: Callable[..., object],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        if inspect.iscoroutinefunction(target) or inspect.isasyncgenfunction(target):
            raise TypeError("async remote callables are not supported")

        result = target(*args, **kwargs)
        if inspect.isawaitable(result):
            close = getattr(result, "close", None)
            if close is not None:
                close()
            raise TypeError("remote callable returned an awaitable")
        if inspect.isgenerator(result) or inspect.isasyncgen(result):
            close = getattr(result, "close", None)
            if close is not None:
                close()
            raise TypeError("remote callable returned a generator")
        return result

    def _send_result(
        self,
        connection: _RuntimeConnection,
        call_id: str,
        result: Any,
    ) -> None:
        self._send_response(
            connection,
            {"type": "result", "call_id": call_id, "ok": True, "result": result},
        )

    def _send_error(
        self,
        connection: _RuntimeConnection,
        call_id: str,
        exc: Exception,
    ) -> None:
        self._send_response(
            connection,
            {
                "type": "result",
                "call_id": call_id,
                "ok": False,
                "error_type": type(exc).__name__,
                "error": str(exc),
            },
        )

    def _send_response(
        self,
        connection: _RuntimeConnection,
        response: dict[str, Any],
    ) -> None:
        try:
            payload = self._serializer.dumps(response)
        except Exception as exc:
            fallback = {
                "type": "result",
                "call_id": response["call_id"],
                "ok": False,
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
            try:
                payload = self._serializer.dumps(fallback)
            except Exception:
                self._abort_connection(connection)
                raise

        try:
            write_frame(
                connection.sock,
                payload,
                write_lock=connection.write_lock,
                max_frame_size=self._max_frame_size,
            )
        except Exception:
            self._abort_connection(connection)
            raise

    @staticmethod
    def _abort_connection(connection: _RuntimeConnection) -> None:
        connection.closed.set()
        try:
            connection.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            connection.sock.close()
        except OSError:
            pass
