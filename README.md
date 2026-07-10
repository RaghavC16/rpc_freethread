# nogil_rpc

`nogil_rpc` is a small Python-to-Python RPC runtime for free-threaded Python
experiments. It lets one process register marked functions and another process
call them with a Ray-like `.remote(...).get()` API.

## Quick Start

Start a runtime and register functions explicitly:

```python
from nogil_rpc import RpcRuntime, remote


@remote
def add(a, b):
    return a + b


runtime = RpcRuntime(host="127.0.0.1", port=50051)
runtime.register(add)
runtime.start()
runtime.wait()
```

Call the runtime from another process:

```python
from nogil_rpc import connect


worker = connect("127.0.0.1:50051")
try:
    ref = worker.add.remote(2, 3)
    print(ref.get())
finally:
    worker.close()
```

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

## Free-Threading Safety

The runtime is written as if the GIL does not protect shared state:

- the function registry is locked
- client pending-call maps are locked
- runtime connection sets are locked
- socket writes are serialized per connection
- remote functions may run concurrently in the worker pool

Application state mutated by remote functions should use its own locks.

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
