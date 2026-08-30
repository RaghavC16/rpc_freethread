# nogil_rpc

`nogil_rpc` is a small, project-agnostic Python-to-Python RPC runtime for
free-threaded Python experiments. It lets a server expose functions and
stateful actors with `@remote` and another process call them through a Ray-like
`.remote(...).get()` API.

The package owns only RPC coordination. Scheduling policies, domain payloads,
GPU execution, retries, and alpha-beta-CROWN integration remain application
responsibilities.

## Installation

Install the supporting package directly into the alpha-beta-CROWN environment:

```bash
python -m pip install -e /path/to/rpc_freethread
```

`nogil-rpc` supports Python 3.11 and newer and has no runtime dependencies.
It includes PEP 561 typing metadata. The installed package version is available
as `nogil_rpc.__version__`.

## Quick Start

Decorate functions and start a runtime:

```python
from nogil_rpc import RpcRuntime, remote


@remote
def add(a, b):
    return a + b


with RpcRuntime(host="127.0.0.1", port=50051) as runtime:
    runtime.wait()
```

Call the runtime from another process:

```python
from nogil_rpc import connect


with connect("127.0.0.1:50051") as worker:
    ref = worker.add.remote(2, 3)
    print(ref.get())
```

`RpcRuntime(..., max_frame_size=...)` and `connect(..., max_frame_size=...)`
may use a matching larger positive limit for trusted, one-time bootstrap
objects. The default remains 64 MiB; iterative tensor payloads should use a
dedicated data plane rather than increasing this limit indiscriminately.

Expected output:

```text
5
```

## Run The Example

Activate the project environment and install the local package:

```bash
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Start the example server in one terminal:

```bash
.venv/bin/python examples/server.py
```

It should print:

```text
listening on 127.0.0.1:50051
```

Then run the example client in a second terminal:

```bash
.venv/bin/python examples/client.py
```

Expected client output:

```text
5
```

Stop the server with `Ctrl-C` when finished.

## ObjectRef

`.remote(...)` returns an `ObjectRef` immediately. Use `get(timeout=None)` to
wait for the result, or `ready()` to check completion without blocking.

Remote exceptions are raised locally as `RemoteError`.

The supported public imports are `remote`, `connect`, `RpcRuntime`,
`RemoteProcess`, `ObjectRef`, `ActorHandle`, `Serializer`, `PickleSerializer`,
the exception types exported by `nogil_rpc`, and `__version__`. Other modules
and underscored names are implementation details.

## Remote Actors

Decorating a class exposes it as a persistent remote actor:

```python
from nogil_rpc import RpcRuntime, remote


@remote
class Counter:
    def __init__(self, value=0):
        self.value = value

    def increment(self, amount=1):
        self.value += amount
        return self.value


runtime = RpcRuntime(host="127.0.0.1", port=50051)
runtime.start()
runtime.wait()
```

Construct the actor and call its member functions from the client:

```python
from nogil_rpc import connect


worker = connect("127.0.0.1:50051")
counter = worker.Counter.remote(10)
try:
    print(counter.increment.remote().get())
    print(counter.increment.remote(amount=4).get())
finally:
    counter.close()
    worker.close()
```

Each actor keeps its instance state on the server. Calls to one actor execute in
submission order, while separate actors can execute concurrently. Actor
construction and `close()` are synchronous; actor methods return `ObjectRef`
instances. Remote actors currently support synchronous methods whose arguments
and results can be serialized by the configured serializer.

Constructor, function, and method exceptions are returned to the client as
`RemoteError`, with the original exception class name available through
`error_type`. Missing methods, non-callable attributes, unsupported async or
generator callables, and serialization failures follow the same error path.
Private actor attributes are not exposed. Closing a client connection releases
the actors it created, and both actor and runtime shutdown are idempotent.

## Free-Threading Safety

The runtime is written as if the GIL does not protect shared state:

- the function registry is locked
- the actor instance registry is locked
- client pending-call maps are locked
- runtime connection sets are locked
- socket writes are serialized per connection
- remote functions may run concurrently in the worker pool
- methods on one actor are serialized through its own executor

`@remote` adds functions to a process-wide registry shared by runtimes in that
process. Application state mutated by remote functions should use its own locks.

## Development

Activate the project environment and install the package in editable mode:

```bash
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Run tests:

```bash
.venv/bin/python -m unittest discover -s tests
```

The wire format and compatibility expectations are documented in
`docs/protocol.md`.

## Security and reliability boundaries

The default serializer uses `pickle`, so this runtime is only for trusted
alpha-beta-CROWN machines on a protected network. It currently provides no
authentication, encryption, retries, deduplication, heartbeats, reconnection,
or durable actor state. A caller that loses its connection may not know whether
an in-flight operation executed.

## Free-Threaded Python

This repo can also be tested with the project-local free-threaded Python build:

```bash
.venv-ft/bin/python -c "import sysconfig; print(sysconfig.get_config_var('Py_GIL_DISABLED'))"
.venv-ft/bin/python -m unittest discover -s tests
```

The expected `Py_GIL_DISABLED` output is:

```text
1
```

This local `.venv-ft` was created from a source-built `python3.14t` using native
`venv --without-pip`. The build environment did not provide zlib/OpenSSL
headers, so pip is not available in `.venv-ft`; the repo is made importable in
that venv through a local `.pth` file instead. The RPC package itself uses only
standard-library modules available in that build.

## Free-Threading Benchmark

The benchmark in `benchmarks/rpc_thread_eval.py` runs the same pure-Python
CPU-bound workload as either a remote function or a remote actor member
function. This is the right kind of workload for checking free-threading:
I/O-heavy code can overlap even with the GIL, but CPU-bound Python bytecode
should only scale across threads when the GIL is disabled.

Run the benchmark once with the normal venv:

```bash
.venv/bin/python benchmarks/rpc_thread_eval.py run --workers 8 --tasks 32 --iterations 1000000
```

Run the same benchmark with the free-threaded venv:

```bash
.venv-ft/bin/python benchmarks/rpc_thread_eval.py run --workers 8 --tasks 32 --iterations 1000000
```

To benchmark the same workload through a remote actor member function, select
the actor target. The benchmark creates one actor per worker by default and
distributes calls across them; a single actor intentionally executes its calls
serially.

```bash
.venv-ft/bin/python benchmarks/rpc_thread_eval.py run --target actor --workers 8 --tasks 32 --iterations 1000000
```

Use `--actors N` to choose a different actor count. Actor construction and
destruction are excluded from the timed section so the result measures member
function call throughput.

For the cleanest comparison, use the same free-threaded interpreter with its
GIL enabled and disabled. The command runs each configuration several times,
reports the median, and sweeps across 1, 2, 4, and 6 runtime workers:

```bash
.venv-ft/bin/python benchmarks/rpc_thread_eval.py compare --workers 1 2 4 6 --tasks 24 --iterations 1000000 --repetitions 3
```

Add `--target actor` to run the same GIL-enabled versus GIL-disabled comparison
through actor member functions.

The key metric is `CPU parallelism ratio`, calculated as process CPU time divided
by wall time. A CPU-bound GIL build should usually be near `1.0x`, while a
free-threaded build can rise above `1.0x` when multiple Python threads are truly
running at the same time. Throughput is reported as tasks/sec and iterations/sec.
Exact numbers depend on CPU count, system load, and workload size, so compare
the two modes on the same machine with the same arguments. The report separately
shows whether the interpreter supports free-threading and whether the GIL was
enabled for that particular run.

## Ray Control-Plane Comparison

`benchmarks/control_plane_compare.py` compares Ray and `nogil_rpc` on a miniature
version of the shared-domain control plane in the alpha-beta-CROWN neural
network verifier. One persistent actor owns a priority frontier of compact
domain records. Concurrent coordinators stand in for solver workers: they claim
a batch, create deterministic child records locally, publish the children, and
periodically query progress. The runtime itself remains independent of
alpha-beta-CROWN and contains no verifier-specific code.

The benchmark deliberately excludes Torch, CUDA, NumPy arrays, and solver work.
It measures small control messages and actor coordination rather than Ray's
shared-memory tensor data path. Runtime startup and actor creation are reported
separately from steady-state control-call throughput.

Run the default concurrency/throughput comparison at 1, 2, 4, and 6 concurrent
coordinators. `--rounds` is per coordinator, so total control-plane work grows
with the coordinator count:

```bash
.venv/bin/python benchmarks/control_plane_compare.py compare \
  --coordinators 1 2 4 6 \
  --rounds 200 \
  --batch-size 8 \
  --repetitions 10 \
  --json > benchmarks/results/control_plane_3way.json
```

By default, the launcher runs three modes:

```text
Ray/GIL:          .python-3.14/bin/python3.14
nogil_rpc/GIL:    .python-3.14/bin/python3.14
nogil_rpc/no-GIL: .venv-ft/bin/python -X gil=0
```

Ray and the GIL-enabled `nogil_rpc` baseline deliberately share the exact same
regular interpreter. This isolates framework overhead. Comparing the two
`nogil_rpc` modes then isolates the effect of free-threading, while comparing
`nogil_rpc/no-GIL` with `Ray/GIL` shows the combined effect. Override the paths
with `--regular-python` and `--free-threaded-python`. Use `--json` for raw
per-run and median results.

Plot a JSON result as a dependency-free SVG, or install Matplotlib and use a
`.png` output name for a raster copy:

```bash
.venv/bin/python benchmarks/plot_control_plane_results.py \
  benchmarks/results/control_plane_3way.json \
  benchmarks/results/control_plane_3way.svg
```

The checked-in 10-run result reached 8,142 control calls/second with six
coordinators under free-threaded CPython, 4.33x the Ray/GIL median for this
compact control-plane workload. This is not an end-to-end alpha-beta-CROWN
speedup claim; the benchmark excludes solver computation and tensor transport.

The comparison models the serialized shared domain-list actor and its
claim/publish/query traffic. Its coordinators are local client threads, not
remote solver ranks. It does not reproduce the intended cross-machine scalable
branch-and-bound topology, multiple load-balanced domain-list services, the
free-threaded preprocess/solve/postprocess pipeline inside each rank, Parallel
CROWN tensor parallelism, bulky domain transport, process failure isolation, or
forceful actor termination. The detailed production-code investigation and
fairness boundaries are in
`ray_control_plane_analysis.md`.

The intended alpha-beta-CROWN integration uses `nogil_rpc` only for the control
request. A remote `request_a_domain` call returns a compact descriptor or token;
a separate data-plane implementation consumes that descriptor and transfers the
domain payload. Tensor transport and Parallel CROWN collectives are outside the
runtime's scope.

The proposed packaging boundary and integration milestones for using the
generic runtime from alpha-beta-CROWN are documented in
`alpha_beta_crown_integration.md`.
