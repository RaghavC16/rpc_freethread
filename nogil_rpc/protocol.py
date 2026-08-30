"""Length-prefixed TCP framing helpers."""

from __future__ import annotations

import struct
from contextlib import nullcontext
from threading import Lock
from typing import BinaryIO

from nogil_rpc.errors import ConnectionClosedError, ProtocolError

HEADER_SIZE = 4
DEFAULT_MAX_FRAME_SIZE = 64 * 1024 * 1024


def write_frame(
    sock: BinaryIO,
    payload: bytes,
    *,
    write_lock: Lock | None = None,
    max_frame_size: int = DEFAULT_MAX_FRAME_SIZE,
) -> None:
    """Write one length-prefixed payload to a socket-like object."""
    if len(payload) > max_frame_size:
        raise ProtocolError(
            f"frame length {len(payload)} exceeds maximum {max_frame_size}"
        )

    header = struct.pack(">I", len(payload))
    context = write_lock if write_lock is not None else nullcontext()
    with context:
        sock.sendall(header)
        sock.sendall(payload)


def read_frame(
    sock: BinaryIO,
    *,
    max_frame_size: int = DEFAULT_MAX_FRAME_SIZE,
) -> bytes:
    """Read one complete length-prefixed payload from a socket-like object."""
    header = _read_exact(sock, HEADER_SIZE)
    frame_length = struct.unpack(">I", header)[0]

    if frame_length > max_frame_size:
        raise ProtocolError(
            f"frame length {frame_length} exceeds maximum {max_frame_size}"
        )

    return _read_exact(sock, frame_length)


def _read_exact(sock: BinaryIO, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size

    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionClosedError(
                f"connection closed while reading {size} bytes"
            )
        chunks.append(chunk)
        remaining -= len(chunk)

    return b"".join(chunks)
