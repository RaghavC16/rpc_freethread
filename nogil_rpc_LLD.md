# `nogil_rpc` Low-Level Design

## 1. Purpose and Scope

`nogil_rpc` is a small Python-to-Python remote procedure call runtime built for
experiments with free-threaded CPython. It gives user code a Ray-like API without
depending on Ray, gRPC, protobuf, or project-specific application logic:

```python
@remote
def add(a, b):
    return a + b

runtime = RpcRuntime("127.0.0.1", 50051)
runtime.start()

worker = connect("127.0.0.1:50051")
result = worker.add.remote(2, 3).get()
```

The package supports two execution models:

1. **Remote functions** are independent tasks executed concurrently by a
   runtime-wide thread pool.
2. **Remote actors** are persistent class instances stored in the server.
   Calls to one instance execute serially, while different actor instances may
   execute concurrently.

The implementation is intentionally compact. It is a prototype for studying
threaded RPC dispatch under a disabled GIL, not a production distributed system.
In particular, the current implementation does not authenticate peers, encrypt
traffic, reconnect, retry, persist state, stream results, or recover calls after
a server restart.

This document describes the implementation currently in the repository and
separately describes planned at-most-once retry work. Planned behavior should
not be mistaken for a guarantee of the current code.

## 2. Design Goals

The implementation makes the following deliberate choices:

- Present a small public API: `@remote`, `RpcRuntime`, `connect`, `ObjectRef`,
  and `ActorHandle`.
- Keep application functions and classes ordinary Python objects.
- Use dynamic proxies to preserve the familiar `.remote(...).get()` shape.
- Use only the Python standard library.
- Make shared-state synchronization explicit instead of relying on the GIL.
- Permit concurrent execution of CPU-bound Python when running on
  free-threaded CPython.
- Preserve mutable state and method ordering within an actor.
- Separate serialization from framing so either layer can evolve.
- Convert remote failures into a consistent client-side `RemoteError`.
- Keep the wire protocol simple enough to inspect and test directly.

Non-goals include language interoperability, untrusted-network security,
durability, cluster scheduling, object storage, resource placement, and the full
Ray API.

## 3. Repository Layout

```text
rpc_freethread/
├── nogil_rpc/
│   ├── __init__.py       Public exports
│   ├── errors.py         Package exception hierarchy
│   ├── serializer.py     Serializer protocol and pickle implementation
│   ├── protocol.py       Length-prefixed TCP framing
│   ├── registry.py       Function/class registration
│   ├── object_ref.py     Future-like result object
│   ├── rpc_client.py     Connection, response reader, and client proxies
│   └── runtime.py        Decorator, server, dispatch, and actors
├── tests/
│   ├── test_nogil_rpc.py Unit and integration tests
│   └── test_control_plane_benchmark.py
│                          Backend-neutral benchmark correctness
├── examples/
│   ├── server.py         Minimal function server
│   └── client.py         Minimal concurrent client
├── benchmarks/
│   ├── rpc_thread_eval.py
│   │                      GIL-on/GIL-off function and actor benchmark
│   ├── control_plane_compare.py
│   │                      Ray versus nogil_rpc shared-actor benchmark
│   ├── plot_control_plane_results.py
│   │                      JSON-to-SVG/PNG benchmark renderer
│   └── results/           Checked-in 10-run comparison data and plots
├── README.md             User-facing setup and usage
├── ray_control_plane_analysis.md
│                          alpha-beta-CROWN Ray orchestration investigation
├── alpha_beta_crown_integration.md
│                          Generic API gaps and verifier integration plan
├── nogil_rpc_implementation_plan.md
│                          Original plan and retry roadmap
├── pyproject.toml        Package metadata
└── requirements.txt     Editable install; no runtime dependencies
```

The package targets Python 3.11 or newer. `pyproject.toml` declares no external
runtime dependencies.

## 4. System Architecture

### 4.1 Component view

```text
Server process                                      Client process

@remote function/class
        │
        ▼
process-wide REMOTE_REGISTRY
        │
        ▼
RpcRuntime ── TCP listener ◄══════════════════════ connect()
   │                                               │
   ├── accept thread                               ├── RemoteProcess
   ├── one reader thread per connection            │    ├── function proxy
   ├── shared function/constructor pool            │    └── actor-class proxy
   ├── actor table                                 ├── RpcClientConnection
   └── one single-thread pool per actor             │    ├── pending call map
                                                    │    └── response-reader thread
                                                    └── ObjectRef / ActorHandle
```

Both sides serialize Python dictionaries, frame them with a four-byte length
prefix, and exchange them over a persistent TCP connection. A UUID `call_id`
correlates each result with the `ObjectRef` created for its request.

### 4.2 Server threading model

The server has:

- One daemon accept thread per `RpcRuntime`.
- One daemon connection-reader thread per connected client.
- One runtime-wide `ThreadPoolExecutor` with `max_workers` threads.
- One `ThreadPoolExecutor(max_workers=1)` for every live actor.

Connection-reader threads deserialize and validate messages but do not run
application functions. They submit work to an executor and immediately return
to reading the socket.

### 4.3 Client threading model

Each client connection has:

- Calling application threads, which may submit concurrently.
- One daemon response-reader thread.
- One pending-call dictionary shared by submitters and the response reader.
- One socket-write lock so frames from concurrent callers cannot interleave.

`ObjectRef.get()` blocks the calling thread on a condition variable; it does not
consume the socket itself.

## 5. Public API

`nogil_rpc/__init__.py` exports exactly:

```python
ActorHandle
ObjectRef
RpcRuntime
connect
remote
```

### 5.1 `@remote`

`runtime.remote(target)`:

1. Sets `target.__remote__ = True`.
2. Inserts the target into the process-wide `REMOTE_REGISTRY`.
3. Returns the original function or class unchanged.

The target remains directly callable in its local process. Registration occurs
at decoration/import time rather than when a runtime starts. Consequently:

- Every `RpcRuntime` in that process sees the same decorated targets.
- A newly connected client receives the registry snapshot that exists at
  connection time.
- Duplicate names are rejected process-wide.
- Import order determines which registrations have occurred before a client
  receives its catalog.

The decorator supports functions and classes. Decorating an individual instance
method is not an actor registration mechanism: actors require decorating the
class.

### 5.2 `RpcRuntime`

Important constructor parameters:

- `host`, `port`: bind address; `port=0` requests an ephemeral port.
- `max_workers`: size of the shared function/constructor executor.
- `serializer`: pluggable serializer, defaulting to `PickleSerializer`.
- `backlog`: TCP listen backlog.

Important methods:

- `start()`: bind, listen, and start the accept thread.
- `wait(timeout=None)`: wait on the runtime stop event.
- `stop()`: idempotently close network resources and executors.
- `address`: return the actual bound `(host, port)`.

A stopped runtime cannot be restarted because its executor has been shut down
and `_closed` is permanent.

### 5.3 `connect`

`connect("host:port", serializer=None, timeout=None)`:

1. Parses the address.
2. Opens a TCP connection using the requested timeout.
3. Reads and validates the server's initial catalog.
4. Restores the socket to blocking mode.
5. Starts the background response reader.
6. Returns a `RemoteProcess`.

The timeout is a connection-handshake timeout and is also the default wait used
for synchronous actor creation/destruction. Ordinary function and method
`ObjectRef`s use the timeout explicitly supplied to `get()`.

### 5.4 Dynamic remote proxies

`RemoteProcess.__getattr__` implements remote name access:

- A name listed in the catalog's actor set produces
  `RemoteActorClassProxy`.
- Every other public name produces `RemoteFunctionProxy`.
- Names beginning with `_` are rejected locally.

Function names are not restricted to the catalog on the client. An unknown
function can therefore be submitted and will fail remotely with
`FunctionNotFoundError`. Actor names need the catalog because the client must
distinguish construction from a function call.

`RemoteFunctionProxy.remote(*args, **kwargs)` returns an `ObjectRef`
asynchronously.

`RemoteActorClassProxy.remote(*args, **kwargs)` waits synchronously for
construction and then returns an `ActorHandle`.

`ActorHandle.__getattr__` creates an `ActorMethodProxy`; its `.remote()` call
returns an `ObjectRef`. `ActorHandle.close()` synchronously destroys the actor
and is idempotent from that handle's perspective.

## 6. Module-Level Design

### 6.1 `errors.py`

The exception hierarchy is:

```text
Exception
└── RpcError
    ├── SerializationError
    ├── ProtocolError
    ├── ConnectionClosedError
    ├── RegistryError
    │   ├── FunctionNotRemoteError
    │   ├── DuplicateFunctionError
    │   ├── FunctionNotFoundError
    │   └── RemoteClassNotFoundError
    ├── ActorNotFoundError
    └── RemoteError
```

Server exceptions are not reconstructed as their original Python types on the
client. The server sends their type name and string message. The client creates:

```python
RemoteError(error_type="ValueError", message="bad value")
```

`RemoteError.error_type` preserves the remote class name for programmatic
inspection, while its displayed text is `"ValueError: bad value"`. Tracebacks
and arbitrary exception fields are not transmitted.

### 6.2 `serializer.py`

`Serializer` is a runtime-checkable structural protocol:

```python
class Serializer(Protocol):
    def dumps(self, value: Any) -> bytes: ...
    def loads(self, payload: bytes) -> Any: ...
```

`PickleSerializer` uses the highest available pickle protocol. It wraps any
serialization or deserialization exception in `SerializationError`, providing a
stable error type to the rest of the package.

Design rationale:

- Pickle naturally supports many Python argument and return types.
- The interface permits later replacement without coupling framing or dispatch
  to pickle.
- Pickle is unsafe for untrusted input. Both peers must be trusted because
  deserialization can execute code.
- Callable definitions are not normally sent; calls send registered names.

Client and server must use compatible serializers. There is no serializer
negotiation in the handshake.

### 6.3 `protocol.py`

Each message uses this binary frame:

```text
0                   4                                4 + N
+-------------------+-----------------------------------+
| N, unsigned >I    | N serialized payload bytes        |
+-------------------+-----------------------------------+
```

- The header is a four-byte unsigned big-endian integer.
- The default maximum payload is 64 MiB.
- `write_frame()` checks the limit, creates `header + payload`, and calls
  `sendall()`.
- `read_frame()` reads exactly four bytes, validates the size, then reads the
  complete payload.
- `_read_exact()` loops because one `recv()` is not guaranteed to satisfy the
  requested byte count.
- EOF before a complete frame raises `ConnectionClosedError`.

TCP is a byte stream and does not preserve application message boundaries.
Length-prefixing supplies those boundaries. A per-connection write lock is
essential because multiple worker threads can finish simultaneously; without
it, bytes from two frames could interleave.

There is no protocol magic number, version, compression flag, checksum, or
chunking. Oversized results fail rather than stream.

### 6.4 `registry.py`

There are two registry classes:

- `FunctionRegistry` is the original function-only abstraction retained and
  unit-tested. It supports explicit names but is not used by `RpcRuntime`.
- `RemoteRegistry` is the active process-wide function-and-class registry.

`RemoteRegistry` owns:

```python
_functions: dict[str, Callable]
_classes: dict[str, type]
_lock: Lock
```

`register()` requires the `__remote__` marker, uses `target.__name__`, and
rejects collisions across both namespaces. `get_function()` and `get_class()`
raise distinct lookup errors. `catalog()` returns deterministic sorted tuple
snapshots while holding the registry lock.

`REMOTE_REGISTRY` is a module singleton. Runtime instances do not have private
function registries.

The registry lock matters in free-threaded Python: decoration, catalog
generation, and executor lookups may otherwise access mutable dictionaries
concurrently.

### 6.5 `object_ref.py`

`ObjectRef` is a small future with this state machine:

```text
                  set_result
PENDING ─────────────────────────► FINISHED
   │
   └─────────────────────────────► FAILED
                  set_error
```

It stores:

- Immutable `call_id`.
- A `Condition`, which includes the synchronization lock.
- State enum: `PENDING`, `FINISHED`, or `FAILED`.
- Result value or `RemoteError`.

Behavior:

- `ready()` is a synchronized non-blocking state check.
- `get(timeout)` waits until the state changes.
- A timeout raises built-in `TimeoutError` but leaves the ref pending; a later
  `get()` may still succeed.
- A failed ref raises its stored `RemoteError` on every `get()`.
- Double completion raises `RuntimeError`.

The client response-reader thread is the normal completer. Connection shutdown
also completes all pending refs as failures.

### 6.6 `rpc_client.py`

#### `RpcClientConnection`

This class owns the actual socket and correlation state:

```python
_pending: dict[call_id, ObjectRef]
_pending_lock: Lock
_write_lock: Lock
_closed: bool
_lifecycle_lock: Lock
_reader_thread: Thread | None
```

`call()`, `create_actor()`, `call_actor()`, and `destroy_actor()` construct
operation-specific dictionaries and delegate to `_send_request()`.

`_send_request()`:

1. Generates a UUID4 `call_id`.
2. Creates the corresponding `ObjectRef`.
3. Copies the request and adds `call_id`.
4. Serializes before mutating pending state.
5. Under the lifecycle lock, checks the connection is open.
6. Adds the ref to `_pending`.
7. Writes one complete frame under `_write_lock`.
8. On send failure, removes and fails the ref, then re-raises.

The lifecycle lock spans pending insertion and frame transmission. This avoids a
close/send race in which the ref could be completed twice or left untracked.

`_read_responses()` continuously reads, deserializes, and passes messages to
`_handle_response()`. A valid result atomically removes the matching ref from
the pending map and completes it. Unknown IDs and malformed responses are
protocol errors.

Any reader exception invokes `_close_with_error()`, which:

- Marks the connection closed once.
- Shuts down and closes the socket.
- Moves all pending refs out of the protected dictionary.
- Fails each ref outside the lock.

The response-reader thread is daemonized and is not explicitly joined by
`close()`.

#### Proxy objects

The proxy classes intentionally contain little logic:

```text
RemoteProcess
  ├── RemoteFunctionProxy ──► RpcClientConnection.call
  └── RemoteActorClassProxy ─► RpcClientConnection.create_actor

ActorHandle
  └── ActorMethodProxy ──────► RpcClientConnection.call_actor
```

This keeps dynamic attribute syntax separate from transport and correlation.

`ActorHandle` protects `_closed` with its own lifecycle lock. It marks itself
closed before sending destruction, so concurrent new method calls are rejected.
It suppresses a closed connection and an `ActorNotFoundError` during cleanup,
making repeated or post-disconnect cleanup practical.

#### Catalog and address validation

`_parse_catalog()` requires:

- A dictionary with `"type": "catalog"`.
- Function and actor fields that are list/tuple sequences of strings.
- Disjoint function and actor names.

It returns only the actor-name set, because function access can use the generic
function proxy.

`_parse_address()` accepts the last colon as the separator and converts the port
to an integer. This supports simple `host:port` strings but is not a complete
bracketed-IPv6 address parser and does not validate the numeric port range
itself.

### 6.7 `runtime.py`

#### Private records

`_RuntimeConnection` contains:

- The accepted socket.
- Its socket-write lock.
- A `closed` event visible to actor creation and shutdown.

It uses `@dataclass(eq=False)`, retaining identity hashing so it can be stored in
the runtime's connection set.

`_ActorEntry` contains:

- The persistent Python instance.
- The creating `_RuntimeConnection`.
- Its dedicated single-thread executor.

#### Runtime state

`RpcRuntime` owns:

```python
_executor             shared function/constructor ThreadPoolExecutor
_server_sock          listening socket or None
_accept_thread        accept-loop thread
_stop_event           wait/stop coordination
_lifecycle_lock       start/stop state
_connections          active connection records
_connections_lock     connection-set synchronization
_actors               actor_id -> _ActorEntry
_actors_lock          actor-table synchronization
_closed               permanent terminal lifecycle flag
```

#### Start and accept

`start()` holds the lifecycle lock while it validates state, creates the socket,
sets `SO_REUSEADDR`, binds, listens, records an ephemeral address if applicable,
and starts the daemon accept thread.

The accept loop creates a `_RuntimeConnection`, adds it to the protected active
set, and starts a daemon client-reader thread. The client-reader first sends the
catalog, then repeatedly reads, deserializes, validates, and dispatches requests.

The connection handler catches all errors and closes the connection. This keeps
one malformed or disconnected client from terminating the accept loop, but the
server currently does not log the swallowed exception.

#### Dispatch

`_handle_message()` accepts exactly four request types:

- `call`
- `create_actor`
- `call_actor`
- `destroy_actor`

`_require_string()` validates names and IDs. `_require_arguments()` requires
wire-format `args` to be a list and `kwargs` to be a dictionary, then converts
args to a tuple for invocation. Invalid top-level or field types close the
connection rather than generating an operation result.

#### Invocation rules

`_invoke_sync()` is shared by remote functions and actor methods. It rejects:

- Coroutine functions.
- Async-generator functions.
- Returned awaitables.
- Returned generators and async generators.

If an unsupported object has a `close()` method, the runtime invokes it before
raising `TypeError` to avoid leaking coroutine/generator resources.

Constructors are invoked directly in `_create_actor()` and are expected to be
synchronous as ordinary Python construction.

#### Result transmission

`_send_result()` and `_send_error()` both create a standard result dictionary.
`_send_response()` first tries to serialize that dictionary. If result
serialization fails, it creates a small failure response describing the
serialization exception. If even the fallback cannot be serialized, or if the
frame cannot be written, it aborts the connection.

Aborting the connection causes the client's response reader to fail all of its
pending refs, preventing indefinite waits in the normal connection-failure
path.

## 7. Wire Protocol

All logical messages are Python dictionaries serialized by the configured
serializer.

### 7.1 Initial catalog

The server sends this before accepting requests:

```python
{
    "type": "catalog",
    "functions": ("add", "work"),
    "actors": ("Counter",),
}
```

The catalog is a connection-time snapshot. There is no update message when
registrations change later.

### 7.2 Function request

```python
{
    "type": "call",
    "call_id": "<uuid4>",
    "function": "add",
    "args": [2, 3],
    "kwargs": {},
}
```

### 7.3 Actor construction

```python
{
    "type": "create_actor",
    "call_id": "<uuid4>",
    "actor_id": "<uuid4>",
    "class": "Counter",
    "args": [10],
    "kwargs": {},
}
```

`call_id` identifies the construction operation; `actor_id` identifies the
persistent object created by it.

### 7.4 Actor method call

```python
{
    "type": "call_actor",
    "call_id": "<uuid4>",
    "actor_id": "<uuid4>",
    "method": "increment",
    "args": [4],
    "kwargs": {},
}
```

### 7.5 Actor destruction

```python
{
    "type": "destroy_actor",
    "call_id": "<uuid4>",
    "actor_id": "<uuid4>",
}
```

### 7.6 Successful result

```python
{
    "type": "result",
    "call_id": "<uuid4>",
    "ok": True,
    "result": 5,
}
```

Construction and destruction return `result=None`.

### 7.7 Failed result

```python
{
    "type": "result",
    "call_id": "<uuid4>",
    "ok": False,
    "error_type": "ValueError",
    "error": "invalid value",
}
```

There is no explicit acknowledgement, cancellation, progress, deadline, or
result-lookup message in the current protocol.

## 8. End-to-End Function Call Lifecycle

For:

```python
ref = worker.add.remote(2, 3)
value = ref.get(timeout=2)
```

the complete path is:

1. `RemoteProcess.__getattr__("add")` creates a `RemoteFunctionProxy`.
2. Its `remote()` calls `RpcClientConnection.call()`.
3. `_send_request()` creates a UUID and `ObjectRef`.
4. It serializes the `call` dictionary, records the pending ref, and writes the
   framed payload.
5. The server connection thread reads and deserializes the request.
6. `_handle_message()` selects `_handle_function_call()`.
7. Fields are validated and the work is submitted to the shared executor.
8. `_execute_call()` resolves `add` from `REMOTE_REGISTRY`.
9. `_invoke_sync()` executes `add(2, 3)` in a worker thread.
10. `_send_result()` serializes and writes the response under the connection's
    write lock.
11. The client response-reader thread reads and validates the response.
12. It removes the UUID from `_pending` and calls `ref.set_result(5)`.
13. The condition variable wakes `ref.get()`, which returns `5`.

Submission and completion order are independent for remote functions. Two calls
sent in order may complete in the opposite order; correlation is by `call_id`,
not response position.

## 9. End-to-End Actor Lifecycle

### 9.1 Construction

For:

```python
counter = worker.Counter.remote(10)
```

1. The initial catalog tells `RemoteProcess` that `Counter` is a class.
2. `RemoteActorClassProxy.remote()` asks the connection to create an actor.
3. The client generates a UUID `actor_id` and a separate operation `call_id`.
4. It sends `create_actor` and synchronously waits on the construction ref.
5. The server submits `_create_actor()` to its shared executor.
6. The class is resolved from `REMOTE_REGISTRY` and instantiated.
7. The server creates `_ActorEntry(instance, owner_connection, executor)`.
8. Under `_actors_lock`, it verifies the connection is still open, rejects an
   ID collision, and inserts the entry.
9. A successful result wakes the constructor caller.
10. Only then does the client expose `ActorHandle(actor_id)`.

If construction or insertion fails, no handle is returned. An executor created
for an uninserted entry is shut down.

### 9.2 Method call

For:

```python
ref = counter.increment.remote(4)
```

1. `ActorHandle.__getattr__("increment")` creates an `ActorMethodProxy`.
2. The handle verifies it has not been closed.
3. The client sends `call_actor`.
4. Under `_actors_lock`, the runtime finds the entry and submits the call to
   that entry's executor.
5. The single worker looks up the member on the retained instance.
6. Private names are rejected; non-callable attributes are rejected.
7. `_invoke_sync()` executes the bound method.
8. The result follows the ordinary response/`ObjectRef` path.

Every call to one actor goes through the same single-thread executor. This
provides mutual exclusion for actor methods and preserves the order in which
requests are submitted to that executor. Calls to different actors use
different executors and may overlap.

The guarantee does not make arbitrary state outside the actor safe. If multiple
actors or remote functions share a global mutable object, the application must
provide its own synchronization.

### 9.3 Destruction

`ActorHandle.close()` marks the local handle closed and sends `destroy_actor`.
The server removes the entry from the actor table, waits for already queued
actor methods by shutting down its executor with `cancel_futures=False`, and
then reports success.

Removing the table entry before waiting prevents new calls from being queued
during destruction. Calls already accepted by the actor executor finish.

### 9.4 Disconnect cleanup

Actors are associated with the connection that created them. When a connection
handler exits, `_remove_connection_actors()` removes every actor whose
`owner is connection` and shuts its executor down with cancellation enabled for
work that has not started.

Actor state is memory-only and is lost on explicit close, owner disconnect, or
runtime stop.

## 10. Concurrency and Free-Threading Design

The code assumes the GIL provides no mutual exclusion.

| Shared state/resource | Synchronization | Reason |
|---|---|---|
| Process-wide remote registry | `RemoteRegistry._lock` | Concurrent decoration, lookup, catalog |
| Runtime lifecycle | `_lifecycle_lock` | Atomic start/stop and terminal state |
| Runtime connection set | `_connections_lock` | Accept, disconnect, and stop race |
| Runtime actor table | `_actors_lock` | Create, lookup, destroy, cleanup |
| Per-connection server socket writes | `_RuntimeConnection.write_lock` | Prevent response-frame interleaving |
| Client lifecycle | `RpcClientConnection._lifecycle_lock` | Serialize close with submission |
| Client pending map | `_pending_lock` | Submitters versus response reader/close |
| Client socket writes | `_write_lock` | Prevent request-frame interleaving |
| Each `ObjectRef` | `Condition` | State visibility and waiter wakeup |
| Each actor handle lifecycle | `ActorHandle._lifecycle_lock` | Close versus method submission |
| Each actor's instance state | Single-thread executor | Serialize its methods |

Remote functions have no automatic application-state locking. `max_workers`
only bounds tasks using the shared executor. Every actor introduces its own
thread-pool object and potentially a worker thread; the implementation does not
globally cap the number of actor executor threads.

The actor executor is a behavioral choice, not just a lock:

```text
one actor:
call A ─► call B ─► call C

separate actors:
actor 1: call A ─► call B
actor 2: call X ─► call Y
```

With a GIL-enabled interpreter, CPU-bound executor threads largely serialize at
the interpreter level. With a free-threaded build and the GIL disabled, separate
function workers and separate actors can run Python bytecode in parallel.

## 11. Error Handling by Layer

### 11.1 Local submission errors

Address parsing, connection establishment, request serialization, a closed
client, or socket send failure can raise directly from `.remote()`. If a ref was
inserted before a send failure, the client removes and fails it.

### 11.2 Remote application and lookup errors

Function lookup failures, method lookup failures, application exceptions, and
unsupported return shapes are caught by the server and sent as failed result
messages. `ObjectRef.get()` raises `RemoteError`.

### 11.3 Actor construction errors

Construction is synchronous, so `RemoteActorClassProxy.remote()` itself raises
`RemoteError`; no `ActorHandle` is returned.

### 11.4 Protocol and connection errors

Malformed frames/messages generally terminate that connection. The client
reader then fails all refs still in its pending map. Server connection-handler
exceptions are intentionally isolated but currently swallowed without logging.

### 11.5 Result serialization errors

An unpickleable result is replaced by a failure response, normally with
`error_type == "SerializationError"`. If the serializer cannot encode even that
fallback, the connection is aborted.

### 11.6 Timeouts

An `ObjectRef.get(timeout)` timeout is local waiting behavior only. It does not:

- Cancel server execution.
- Remove the ref from the client pending map.
- Send a deadline to the server.
- Guarantee the call did not finish.

The result can still arrive and complete the ref later.

## 12. Startup and Shutdown Semantics

### 12.1 Client close

`RemoteProcess.close()` delegates to its connection. Close is idempotent:

- The socket is shut down and closed once.
- All pending refs are failed.
- Future submissions raise `ConnectionClosedError`.
- Server-side connection cleanup eventually destroys owned actors.

### 12.2 Runtime stop

`RpcRuntime.stop()`:

1. Atomically marks the runtime closed and signals its stop event.
2. Shuts down and closes the listener.
3. Removes and closes all active client sockets.
4. Shuts down the shared executor, waiting for running tasks and cancelling
   queued tasks.
5. Removes all remaining actors.
6. Shuts down every actor executor, waiting for running work and cancelling
   queued work.

Repeated calls are harmless. `wait()` returns after the stop event is set,
which occurs near the beginning of stop; it is a lifecycle notification, not a
separate guarantee that all executor cleanup has completed.

The daemon accept and connection threads are not explicitly joined. Closing
their sockets causes their loops to exit.

## 13. Tests

`tests/test_nogil_rpc.py` contains 33 tests split between core unit-style tests
and loopback integration tests. `tests/test_control_plane_benchmark.py` adds
three backend-neutral correctness tests for deterministic branching, frontier
mutation, counter reconciliation, and invalid-snapshot detection.

### 13.1 Test support objects

- `FakeSocket` provides deterministic in-memory frame reads/writes.
- `BlockingSendSocket` creates a controlled send/close race.
- `FailResultSerializer` forces total result serialization failure.

### 13.2 Core coverage

The core tests verify:

- `@remote` marking and direct local callability.
- Required remote markers, duplicate detection, and missing lookups.
- Function/class namespace separation and deterministic catalogs.
- Catalog validation.
- Runtime stop idempotence and no restart.
- Pickle round trips and wrapped serialization failures.
- Frame boundaries, size rejection, and EOF handling.
- `ObjectRef` success, timeout, failure, and double-completion protection.
- Rejection of invalid/unknown client responses.
- Address parsing.
- Safe behavior when close races a blocked request send.

### 13.3 Integration coverage

The integration tests start a runtime on loopback with an ephemeral port and
verify:

- Basic calls, args/kwargs, and remote exceptions.
- Unknown function propagation.
- Concurrent calls and concurrent client threads sharing one connection.
- Process-wide registrations shared by multiple runtimes.
- Actor state persistence, submission order, and instance isolation.
- Concurrent execution of separate actors.
- Method/constructor error propagation and idempotent destruction.
- Rejection of missing, private, and non-callable actor members.
- Explicit failure of async and generator callables.
- Unpickleable result fallback and total serialization failure.
- Preservation of all updates under 100 concurrent actor submissions.
- Cleanup of connection-owned actors.

Run under either environment:

```bash
.venv/bin/python -m unittest discover -s tests
.venv-ft/bin/python -m unittest discover -s tests
```

These tests validate important concurrency paths but are not exhaustive
model-checking, load, soak, fault-injection, or security tests.

## 14. Benchmark Design

`benchmarks/rpc_thread_eval.py` measures pure-Python CPU-bound work through the
RPC path. It supports:

- `--target function`: shared runtime executor.
- `--target actor`: actor member method.
- `run`: one interpreter configuration.
- `compare`: the same free-threaded interpreter with `-X gil=1` and
  `-X gil=0`.

The function and actor method call the same `_cpu_bound_work()`, which:

1. Creates a deterministic `random.Random(seed)` per invocation.
2. Creates a list of one million integers.
3. Performs `iterations` random choices and appends them to an answer list.
4. Returns `len(ans)`, keeping the RPC result small.

Returning the whole answer list would shift the benchmark toward memory use,
pickle serialization, socket transfer, and the 64 MiB frame limit. The scalar
return keeps attention on execution throughput.

### 14.1 Function mode

Tasks are sent to `worker.cpu_bound_task.remote(...)`; `max_workers` controls
available execution concurrency.

### 14.2 Actor mode

The benchmark creates one actor per worker by default or uses `--actors N`.
Tasks are assigned round-robin:

```python
actor_handles[task_id % actor_count].cpu_bound_task.remote(...)
```

One actor would serialize every task. Multiple actors are required to measure
parallel actor-method execution. Actor construction and destruction occur
outside the timed section.

### 14.3 Measurements

The report includes:

- Wall-clock time.
- Process CPU time.
- CPU parallelism ratio (`process CPU / wall time`).
- Tasks and iterations per second.
- A scalar checksum.
- Build-time free-threading and runtime GIL status.

`compare` runs subprocesses for each worker/GIL combination, repeats them, and
reports medians. It computes free-threaded throughput speedup as:

```text
GIL-disabled iterations/sec / GIL-enabled iterations/sec
```

The benchmark starts server and client objects in the same Python process and
uses loopback TCP. Process CPU time therefore includes client, server, worker,
serialization, and networking work. It is an end-to-end local RPC benchmark,
not a cross-host latency benchmark.

### 14.4 Ray control-plane comparison

`benchmarks/control_plane_compare.py` models the shared domain-list actor used by
the alpha-beta-CROWN neural network verifier without importing its Torch, CUDA,
or solver stack.
The backend-neutral `FrontierCore` owns a heap of compact
`(priority, depth, token)` records and exposes four actor operations:

- `claim_batch()` atomically removes control records;
- `submit_children()` publishes deterministic branched records;
- `size()` provides a progress query;
- `snapshot()` returns reconciliation counters.

Thin `RpcFrontierClient` and `RayFrontierClient` adapters invoke the same state
machine through `ObjectRef.get()` and `ray.get()` respectively. Multiple local
coordinator threads issue synchronous claim/publish/query cycles concurrently,
matching the central serialized actor boundary while keeping data-plane work out
of the measurement.

The `compare` command launches clean subprocesses in three modes: Ray and
`nogil_rpc` under the exact same regular CPython 3.14.6 interpreter, followed by
`nogil_rpc` under CPython 3.14.6t with `-X gil=0`. The regular pair isolates
framework overhead, the two `nogil_rpc` modes isolate free-threading, and the
Ray/GIL versus RPC/no-GIL pair shows their combined effect. Its default matrix
uses 1, 2, 4, and 6 coordinators. It reports framework/Python identity, GIL
state, startup time, actor creation time, steady-state control calls per second,
amortized time per call, raw runs, medians, and a reconciled final snapshot.

This is intentionally a first-phase control-plane comparison. It does not model
Ray placement groups, transferable actor handles, nested worker-to-pool actor
calls, GPU process isolation, tensor object-store transport, `ray.wait`,
forceful cancellation, or failure recovery. The production Ray trace, proposed
full worker-slice benchmark, and interpretation boundaries are documented in
`ray_control_plane_analysis.md`.

## 15. Important Current Limitations

### 15.1 Delivery semantics

The current system has no retry or deduplication table. Receipt of every `call`
message schedules execution, even if a repeated message has the same `call_id`.
If the connection fails after execution but before result delivery, the client
cannot know whether the side effect occurred.

The current effective guarantee is therefore neither at-most-once nor
at-least-once across communication failure. Under a healthy connection, one
submitted request is executed once; after an ambiguous failure, the caller must
decide how to recover.

### 15.2 Actors

- All public callable members of a `@remote` class are exposed.
- There is no per-method `@remote` allowlist.
- Private names are blocked, but that is an API convention rather than a
  security boundary for an untrusted peer.
- Remote field get/set is not supported.
- Actors are connection-owned and memory-only.
- An actor has one executor, so a long method blocks later methods.
- Actor thread count is not globally bounded.
- The server finds actors by UUID; dispatch does not separately verify that the
  requesting connection is the recorded owner.

### 15.3 Protocol and security

- Pickle requires trusted peers.
- No authentication, authorization, TLS, integrity protection, or quotas.
- No protocol version negotiation.
- No serializer negotiation.
- No compression, streaming, or frames over 64 MiB.
- No backpressure beyond TCP and executor queues.

### 15.4 Execution model

- No coroutine, async generator, or generator support.
- No cancellation, deadline propagation, progress, or priorities.
- No process isolation; application crashes/native faults can terminate the
  runtime.
- No worker recovery or task rescheduling.
- Remote functions that mutate shared state must synchronize it themselves.

### 15.5 Observability

- No structured logging, metrics endpoint, tracing, request history, or health
  protocol.
- Server connection exceptions are swallowed.
- Remote tracebacks are not returned.
- The benchmark subprocess helper uses captured output and `check=True`; a child
  failure surfaces as `CalledProcessError` unless its captured stderr is
  inspected.

### 15.6 Registration and discovery

- Registration is process-global and name-based.
- Names cannot be explicitly overridden in `RemoteRegistry`.
- Duplicate names cannot be replaced/unregistered.
- Catalogs are snapshots and are not refreshed.
- Client function proxy creation is permissive, so spelling errors fail only
  after a network round trip.

## 16. Planned At-Most-Once Communication Retries

This section describes the agreed roadmap, not current functionality.

### 16.1 Required server call table

Add a runtime-owned, locked table keyed by the stable `call_id`:

```text
UNKNOWN ─► ACCEPTED ─► RUNNING ─► COMPLETED
```

Each record must retain enough immutable request identity to detect conflicting
reuse of an ID and, once complete, the full success or error response.

The critical transition is `UNKNOWN -> ACCEPTED`. It must occur atomically
under the call-table lock before executor submission. Otherwise concurrent
duplicates could both schedule application work.

### 16.2 Duplicate behavior

- `UNKNOWN`: insert `ACCEPTED`, acknowledge, and schedule exactly once.
- `ACCEPTED`/`RUNNING`: do not schedule; report that the original is pending.
- `COMPLETED`: resend the cached success or exception response.
- Same ID with different request content: reject as a protocol/idempotency
  violation.

Successful results and exceptions must be cached symmetrically. Re-executing a
failed function would still violate at-most-once semantics.

### 16.3 Client behavior

The client needs to distinguish:

1. Request definitely not accepted.
2. Acceptance unknown because request or acknowledgement was lost.
3. Request accepted and still running.
4. Request completed but result was lost.

It may resend the original request with the same ID while acceptance is unknown.
After acceptance, it must perform status/result lookup rather than creating a
new execution. Reconnect logic must preserve pending call IDs and serialized
requests.

### 16.4 Bounded retention

The in-memory table needs:

- A documented completed-result retention duration.
- A maximum entry count.
- A cleanup policy that never evicts `ACCEPTED` or `RUNNING` work merely to
  satisfy the completed cache bound.
- Explicit behavior when a client looks up an expired result.

Retention creates a limit on how late a client may safely recover a result.

### 16.5 Restart boundary

An in-memory table only guarantees at-most-once while that worker process and
its call table remain alive. After restart, `UNKNOWN` cannot distinguish:

- A genuinely new call.
- A call completed by the previous process whose response was lost.

Claiming restart-safe at-most-once behavior requires durable acceptance state
and durable cached responses, committed before execution/acknowledgement as
appropriate.

### 16.6 Required fault tests

The roadmap calls for deterministic tests of:

- Lost initial request.
- Lost acceptance acknowledgement.
- Duplicate while accepted or running.
- Concurrent duplicate requests.
- Lost successful result and subsequent lookup.
- Lost exception result and subsequent lookup.
- Cache expiration and capacity.
- Conflicting reuse of a call ID.
- Worker restart ambiguity.

Actor creation, state-mutating actor calls, and destruction require especially
careful decisions because duplicate execution changes persistent state. The
protocol should explicitly define whether the first retry milestone covers all
operation types or begins with remote functions only.

## 17. Design Trade-offs and Rationale

### Threads instead of processes

Threads minimize dispatch overhead and directly exercise free-threaded CPython.
They also share registered callables and actor objects naturally. The trade-off
is weak fault isolation and little CPU scaling on a conventional GIL build.

### One executor per actor

This gives simple state safety and ordering without forcing every actor author
to lock each method. It also permits separate actors to run concurrently. The
cost is one executor per instance, unbounded potential thread growth, and no
intra-actor parallel methods.

### Dynamic proxies

`worker.name.remote()` and `actor.method.remote()` require no generated stubs.
The trade-off is that typos and signature errors are discovered at runtime, and
static typing/IDE completion is limited.

### Process-wide registry

Decoration becomes enough to expose code, and multiple runtimes can share the
same application definitions. The trade-off is global name coupling, import-time
side effects, and reduced test/runtime isolation.

### Pickle over framed TCP

This is small, flexible, and Python-native, making it appropriate for a trusted
prototype. It sacrifices language interoperability, safety on untrusted
networks, schema evolution, and large-object streaming.

### `ObjectRef` plus one response reader

One reader prevents multiple application threads from racing to consume a
single ordered socket. UUID correlation allows out-of-order completion. The
trade-off is that one malformed response or connection failure affects every
pending call on that connection.

## 18. Suggested Reading Order

For an end-to-end code walkthrough:

1. `nogil_rpc/__init__.py` for the supported API.
2. `examples/server.py` and `examples/client.py` for its shape.
3. `runtime.remote()` and `registry.RemoteRegistry` for exposure/discovery.
4. `rpc_client.py` proxy classes for call construction.
5. `object_ref.py` for client-visible completion.
6. `serializer.py` and `protocol.py` for transport representation.
7. `RpcClientConnection` for send, correlation, and close behavior.
8. `RpcRuntime` start/accept/dispatch paths.
9. Runtime actor creation, method execution, and cleanup paths.
10. `tests/test_nogil_rpc.py` to see each runtime guarantee exercised.
11. `benchmarks/rpc_thread_eval.py` for the free-threading experiment.
12. `ray_control_plane_analysis.md` for alpha-beta-CROWN's Ray architecture.
13. `benchmarks/control_plane_compare.py` and its correctness tests for the
    cross-framework control-plane experiment.
14. `alpha_beta_crown_integration.md` for the generic API and adapter boundary.
15. `nogil_rpc_implementation_plan.md` for the retry roadmap.

## 19. Summary

`nogil_rpc` is a layered RPC prototype:

```text
application callable
    ↕ registry / dynamic proxy
request dictionary + UUID
    ↕ serializer
payload bytes
    ↕ length-prefixed TCP
server dispatch
    ↕ shared executor or actor executor
result dictionary
    ↕ client response reader
ObjectRef
```

Its central free-threading design is explicit ownership and locking of every
shared runtime structure, combined with a shared function pool and per-actor
serialized execution. That design provides useful local concurrency and
persistent remote objects while keeping the code small enough to study.

Its present reliability boundary is a healthy, live connection to a live server
process. UUID correlation exists, but server-side call history does not. The
next major design step is a locked acceptance/completion table plus reconnect,
acknowledgement, and result-lookup protocol support to provide bounded
at-most-once communication retries while the server process remains alive.
