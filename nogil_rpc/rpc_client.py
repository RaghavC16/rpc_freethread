"""Client-side connection and dynamic remote function proxies."""

from __future__ import annotations

import socket
from collections.abc import Mapping
from threading import Lock, Thread
from typing import Any
from uuid import uuid4

from nogil_rpc.errors import ConnectionClosedError, ProtocolError
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
    connection = RpcClientConnection(sock, serializer=serializer)
    return RemoteProcess(connection)


class RpcClientConnection:
    """Owns one client socket and the pending call map."""

    def __init__(
        self,
        sock: socket.socket,
        *,
        serializer: Serializer | None = None,
        start_response_reader: bool = True,
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
        call_id = str(uuid4())
        ref = ObjectRef(call_id)
        request = {
            "type": "call",
            "call_id": call_id,
            "function": function_name,
            "args": list(args),
            "kwargs": dict(kwargs),
        }
        payload = self._serializer.dumps(request)

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

    def __init__(self, connection: RpcClientConnection) -> None:
        self._connection = connection

    def __getattr__(self, function_name: str) -> RemoteFunctionProxy:
        if function_name.startswith("_"):
            raise AttributeError(function_name)
        return RemoteFunctionProxy(self._connection, function_name)

    def close(self) -> None:
        self._connection.close()


class RemoteFunctionProxy:
    """Proxy for one remote function name."""

    def __init__(self, connection: RpcClientConnection, function_name: str) -> None:
        self._connection = connection
        self._function_name = function_name

    def remote(self, *args: Any, **kwargs: Any) -> ObjectRef:
        return self._connection.call(self._function_name, args, kwargs)


def _parse_address(address: str) -> tuple[str, int]:
    host, separator, port_text = address.rpartition(":")
    if not separator or not host or not port_text:
        raise ValueError("address must be in 'host:port' form")
    try:
        port = int(port_text)
    except ValueError as exc:
        raise ValueError(f"invalid port {port_text!r}") from exc
    return host, port
