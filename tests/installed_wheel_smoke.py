"""Exercise an installed wheel across independent server/client processes."""

from __future__ import annotations

import argparse
from importlib.metadata import version
import selectors
import signal
import subprocess
import sys
from pathlib import Path


def run_server() -> None:
    from nogil_rpc import RpcRuntime, remote

    @remote
    def wheel_add(left: int, right: int) -> int:
        return left + right

    @remote
    def wheel_fail() -> None:
        raise ValueError("expected wheel error")

    @remote
    class WheelCounter:
        def __init__(self, value: int = 0) -> None:
            self.value = value

        def increment(self, amount: int = 1) -> int:
            self.value += amount
            return self.value

    runtime = RpcRuntime(host="127.0.0.1", port=0, max_workers=4)

    def stop_runtime(signum: int, frame: object) -> None:
        del signum, frame
        runtime.stop()

    signal.signal(signal.SIGTERM, stop_runtime)
    runtime.start()
    host, port = runtime.address
    print(f"{host}:{port}", flush=True)
    runtime.wait()


def read_address(process: subprocess.Popen[str], timeout: float = 10.0) -> str:
    assert process.stdout is not None
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    if not selector.select(timeout):
        raise TimeoutError("installed-wheel server did not become ready")
    address = process.stdout.readline().strip()
    if not address:
        stderr = process.stderr.read() if process.stderr is not None else ""
        raise RuntimeError(f"installed-wheel server exited before ready: {stderr}")
    return address


def run_client_server_smoke() -> None:
    import nogil_rpc
    from nogil_rpc import RemoteError, connect

    if version("nogil-rpc") != nogil_rpc.__version__:
        raise AssertionError("distribution and import versions do not match")

    script = Path(__file__).resolve()
    process = subprocess.Popen(
        [sys.executable, str(script), "--server"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        address = read_address(process)
        with connect(address, timeout=5) as worker:
            assert worker.wheel_add.remote(20, 22).get(timeout=5) == 42

            with worker.WheelCounter.remote(10) as counter:
                assert counter.increment.remote(5).get(timeout=5) == 15

            try:
                worker.wheel_fail.remote().get(timeout=5)
            except RemoteError as exc:
                assert exc.error_type == "ValueError"
            else:
                raise AssertionError("remote exception did not propagate")
    finally:
        process.terminate()
        try:
            _, stderr = process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            _, stderr = process.communicate(timeout=5)
        if process.returncode not in (0, -signal.SIGTERM):
            raise RuntimeError(
                f"installed-wheel server exited with {process.returncode}: {stderr}"
            )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", action="store_true")
    args = parser.parse_args()
    if args.server:
        run_server()
    else:
        run_client_server_smoke()
        print("installed wheel smoke test passed")


if __name__ == "__main__":
    main()
