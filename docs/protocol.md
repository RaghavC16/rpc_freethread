# Wire protocol

`nogil_rpc` uses a trusted Python-to-Python protocol over TCP. Each message is
serialized and placed in a frame consisting of a four-byte, big-endian unsigned
payload length followed by that many payload bytes. The default maximum frame
size is 64 MiB.

## Connection handshake

Immediately after accepting a connection, the server sends a `catalog` message:

```python
{
    "type": "catalog",
    "functions": ("function_name", ...),
    "actors": ("ActorClass", ...),
}
```

## Requests and responses

Function calls use a `call` request. Actor lifecycle and method calls use
`create_actor`, `attach_actor`, `call_actor`, and `destroy_actor`. Every request
carries a client-generated `call_id`; result messages echo that identifier and
contain either `{"ok": True, "result": ...}` or error metadata with
`"ok": False`.

The client may have multiple requests in flight on one connection. Server
functions execute in a shared thread pool. Each actor has a single-thread
executor, preserving submission order for that actor while allowing separate
actors to run concurrently.

## Delivery and failure semantics

There are no automatic retries, deduplication, heartbeats, or reconnection. If
a connection is lost after a request is sent, the caller cannot tell whether
the operation executed. Applications must make retried operations idempotent
or supply their own request identifiers when they need stronger semantics.
Actors belong to the connection that created them and are removed when that
connection closes. Other connections may use `attach_actor` with an opaque
actor ID to obtain a non-owning handle to the same actor. An attached handle
may invoke methods but may not destroy the actor.

## Serialization and security

The default `PickleSerializer` supports ordinary pickle-compatible Python
values. Client and server must use compatible serializers and Python object
definitions. Because pickle is unsafe for untrusted input, the protocol is
suitable only between mutually trusted peers on a protected network.
