"""Client-side connection and dynamic remote function proxies."""

from __future__ import annotations

import socket
from collections.abc import Mapping
from threading import Lock, Thread
from typing import Any
from uuid import uuid4

from nogil_rpc.errors import ConnectionClosedError, ProtocolError, RemoteError
from nogil_rpc.object_ref import ObjectRef
from nogil_rpc.protocol import read_frame, write_frame
from nogil_rpc.serializer import PickleSerializer, Serializer


def connect(
    address: str,
    *,
    serializer: Serializer | None = None,
    timeout: float | None = None,
) -> RemoteProcess:
    """Connect to an RPC runtime and return a dynamic process proxy."""
    host, port = _parse_address(address)
    sock = socket.create_connection((host, port), timeout=timeout)
    serializer_impl = serializer if serializer is not None else PickleSerializer()
    try:
        catalog = serializer_impl.loads(read_frame(sock))
        actor_names = _parse_catalog(catalog)
        sock.settimeout(None)
        connection = RpcClientConnection(
            sock,
            serializer=serializer_impl,
            default_timeout=timeout,
        )
    except Exception:
        sock.close()
        raise
    return RemoteProcess(connection, actor_names=actor_names)


class RpcClientConnection:
    """Owns one client socket and the pending call map."""

    def __init__(
        self,
        sock: socket.socket,
        *,
        serializer: Serializer | None = None,
        start_response_reader: bool = True,
        default_timeout: float | None = None,
    ) -> None:
        self._sock = sock
        self._serializer = (
            serializer if serializer is not None else PickleSerializer()
        )
        self._pending: dict[str, ObjectRef] = {}
        self._pending_lock = Lock()
        self._write_lock = Lock()
        self._closed = False
        self._lifecycle_lock = Lock()
        self._default_timeout = default_timeout

        self._reader_thread: Thread | None = None
        if start_response_reader:
            self._reader_thread = Thread(
                target=self._read_responses,
                name="nogil-rpc-client-reader",
                daemon=True,
            )
            self._reader_thread.start()

    def call(
        self,
        function_name: str,
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
    ) -> ObjectRef:
        """Send a remote function call and return its result reference."""
        return self._send_request(
            {
                "type": "call",
                "function": function_name,
                "args": list(args),
                "kwargs": dict(kwargs),
            }
        )

    def create_actor(
        self,
        class_name: str,
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
    ) -> ActorHandle:
        """Construct a persistent remote actor and return its handle."""
        actor_id = str(uuid4())
        ref = self._send_request(
            {
                "type": "create_actor",
                "actor_id": actor_id,
                "class": class_name,
                "args": list(args),
                "kwargs": dict(kwargs),
            }
        )
        ref.get(timeout=self._default_timeout)
        return ActorHandle(self, actor_id)

    def call_actor(
        self,
        actor_id: str,
        method_name: str,
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
    ) -> ObjectRef:
        """Invoke one method on a persistent remote actor."""
        return self._send_request(
            {
                "type": "call_actor",
                "actor_id": actor_id,
                "method": method_name,
                "args": list(args),
                "kwargs": dict(kwargs),
            }
        )

    def destroy_actor(self, actor_id: str) -> None:
        """Destroy a persistent remote actor after queued methods finish."""
        ref = self._send_request({"type": "destroy_actor", "actor_id": actor_id})
        ref.get(timeout=self._default_timeout)

    def _send_request(self, request: dict[str, Any]) -> ObjectRef:
        call_id = str(uuid4())
        ref = ObjectRef(call_id)
        message = {**request, "call_id": call_id}
        payload = self._serializer.dumps(message)

        try:
            with self._lifecycle_lock:
                if self._closed:
                    raise ConnectionClosedError("client connection is closed")
                with self._pending_lock:
                    self._pending[call_id] = ref
                write_frame(self._sock, payload, write_lock=self._write_lock)
        except Exception:
            with self._pending_lock:
                self._pending.pop(call_id, None)
            if not ref.ready():
                ref.set_error("ConnectionError", "failed to send remote call")
            raise

        return ref

    def close(self) -> None:
        """Close the client connection and fail pending calls."""
        self._close_with_error("ConnectionClosedError", "client connection closed")

    def _read_responses(self) -> None:
        try:
            while True:
                payload = read_frame(self._sock)
                message = self._serializer.loads(payload)
                self._handle_response(message)
        except Exception as exc:
            self._close_with_error(type(exc).__name__, str(exc))

    def _handle_response(self, message: Any) -> None:
        if not isinstance(message, dict):
            raise ProtocolError("response message must be a dictionary")
        if message.get("type") != "result":
            raise ProtocolError(f"unexpected response type {message.get('type')!r}")

        call_id = message.get("call_id")
        if not isinstance(call_id, str):
            raise ProtocolError("response call_id must be a string")

        with self._pending_lock:
            ref = self._pending.pop(call_id, None)
        if ref is None:
            raise ProtocolError(f"received response for unknown call_id {call_id!r}")

        if message.get("ok") is True:
            ref.set_result(message.get("result"))
            return

        if message.get("ok") is False:
            error_type = message.get("error_type", "RemoteError")
            error = message.get("error", "remote call failed")
            ref.set_error(str(error_type), str(error))
            return

        raise ProtocolError("response ok field must be true or false")

    def _close_with_error(self, error_type: str, message: str) -> None:
        with self._lifecycle_lock:
            if self._closed:
                return
            self._closed = True
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except (AttributeError, OSError):
                pass
            try:
                self._sock.close()
            except OSError:
                pass

        with self._pending_lock:
            pending = tuple(self._pending.values())
            self._pending.clear()

        for ref in pending:
            ref.set_error(error_type, message)


class RemoteProcess:
    """Dynamic proxy for functions exposed by one RPC runtime."""

    def __init__(
        self,
        connection: RpcClientConnection,
        *,
        actor_names: frozenset[str] = frozenset(),
    ) -> None:
        self._connection = connection
        self._actor_names = actor_names

    def __getattr__(self, remote_name: str) -> Any:
        if remote_name.startswith("_"):
            raise AttributeError(remote_name)
        if remote_name in self._actor_names:
            return RemoteActorClassProxy(self._connection, remote_name)
        return RemoteFunctionProxy(self._connection, remote_name)

    def close(self) -> None:
        """Close the underlying connection and fail any pending calls."""
        self._connection.close()

    def __enter__(self) -> RemoteProcess:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


class RemoteFunctionProxy:
    """Proxy for one remote function name."""

    def __init__(self, connection: RpcClientConnection, function_name: str) -> None:
        self._connection = connection
        self._function_name = function_name

    def remote(self, *args: Any, **kwargs: Any) -> ObjectRef:
        return self._connection.call(self._function_name, args, kwargs)


class RemoteActorClassProxy:
    """Client proxy used to construct one kind of remote actor."""

    def __init__(self, connection: RpcClientConnection, class_name: str) -> None:
        self._connection = connection
        self._class_name = class_name

    def remote(self, *args: Any, **kwargs: Any) -> ActorHandle:
        return self._connection.create_actor(self._class_name, args, kwargs)


class ActorHandle:
    """Reference to one persistent object owned by an RPC runtime."""

    def __init__(self, connection: RpcClientConnection, actor_id: str) -> None:
        self._connection = connection
        self._actor_id = actor_id
        self._closed = False
        self._lifecycle_lock = Lock()

    @property
    def actor_id(self) -> str:
        return self._actor_id

    def __getattr__(self, method_name: str) -> ActorMethodProxy:
        if method_name.startswith("_"):
            raise AttributeError(method_name)
        return ActorMethodProxy(self, method_name)

    def close(self) -> None:
        with self._lifecycle_lock:
            if self._closed:
                return
            self._closed = True
        try:
            self._connection.destroy_actor(self._actor_id)
        except ConnectionClosedError:
            pass
        except RemoteError as exc:
            if exc.error_type != "ActorNotFoundError":
                raise

    def __enter__(self) -> ActorHandle:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def _call(
        self,
        method_name: str,
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
    ) -> ObjectRef:
        with self._lifecycle_lock:
            if self._closed:
                raise ConnectionClosedError("actor handle is closed")
            return self._connection.call_actor(
                self._actor_id,
                method_name,
                args,
                kwargs,
            )


class ActorMethodProxy:
    """Dynamic proxy for one method on a remote actor."""

    def __init__(self, actor: ActorHandle, method_name: str) -> None:
        self._actor = actor
        self._method_name = method_name

    def remote(self, *args: Any, **kwargs: Any) -> ObjectRef:
        return self._actor._call(self._method_name, args, kwargs)


def _parse_catalog(message: Any) -> frozenset[str]:
    if not isinstance(message, dict) or message.get("type") != "catalog":
        raise ProtocolError("server did not send a valid remote catalog")
    function_names = message.get("functions")
    actor_names = message.get("actors")
    if not isinstance(function_names, (list, tuple)) or not all(
        isinstance(name, str) for name in function_names
    ):
        raise ProtocolError("catalog functions must be a sequence of strings")
    if not isinstance(actor_names, (list, tuple)) or not all(
        isinstance(name, str) for name in actor_names
    ):
        raise ProtocolError("catalog actors must be a sequence of strings")
    if set(function_names).intersection(actor_names):
        raise ProtocolError("catalog names cannot be both functions and actors")
    return frozenset(actor_names)


def _parse_address(address: str) -> tuple[str, int]:
    host, separator, port_text = address.rpartition(":")
    if not separator or not host or not port_text:
        raise ValueError("address must be in 'host:port' form")
    try:
        port = int(port_text)
    except ValueError as exc:
        raise ValueError(f"invalid port {port_text!r}") from exc
    return host, port
