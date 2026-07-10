from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Event, Thread
import unittest

from nogil_rpc import ObjectRef, RpcRuntime, connect, remote
from nogil_rpc.errors import (
    DuplicateFunctionError,
    FunctionNotFoundError,
    FunctionNotRemoteError,
    RemoteError,
)
from nogil_rpc.protocol import read_frame, write_frame
from nogil_rpc.registry import FunctionRegistry
from nogil_rpc.rpc_client import RpcClientConnection
from nogil_rpc.serializer import PickleSerializer


class FakeSocket:
    def __init__(self) -> None:
        self.buffer = bytearray()

    def sendall(self, payload: bytes) -> None:
        self.buffer.extend(payload)

    def recv(self, size: int) -> bytes:
        chunk = bytes(self.buffer[:size])
        del self.buffer[:size]
        return chunk


class BlockingSendSocket(FakeSocket):
    def __init__(self) -> None:
        super().__init__()
        self.entered_send = Event()
        self.release_send = Event()
        self.closed = False

    def sendall(self, payload: bytes) -> None:
        self.entered_send.set()
        self.release_send.wait(timeout=2)
        super().sendall(payload)

    def close(self) -> None:
        self.closed = True


class CoreTests(unittest.TestCase):
    def test_remote_marks_function(self) -> None:
        @remote
        def add(a, b):
            return a + b

        self.assertIs(add.__remote__, True)
        self.assertEqual(add(2, 3), 5)

    def test_registry_requires_remote_functions(self) -> None:
        def plain():
            return None

        with self.assertRaises(FunctionNotRemoteError):
            FunctionRegistry().register(plain)

    def test_registry_rejects_duplicates(self) -> None:
        @remote
        def add(a, b):
            return a + b

        registry = FunctionRegistry()
        registry.register(add)

        with self.assertRaises(DuplicateFunctionError):
            registry.register(add)

    def test_registry_missing_function(self) -> None:
        with self.assertRaises(FunctionNotFoundError):
            FunctionRegistry().get("missing")

    def test_serializer_round_trips_python_objects(self) -> None:
        serializer = PickleSerializer()
        value = {"numbers": [1, 2, 3], "nested": {"ok": True}}

        self.assertEqual(serializer.loads(serializer.dumps(value)), value)

    def test_frame_helpers_preserve_boundaries(self) -> None:
        sock = FakeSocket()

        write_frame(sock, b"hello")
        write_frame(sock, b"world")

        self.assertEqual(read_frame(sock), b"hello")
        self.assertEqual(read_frame(sock), b"world")

    def test_object_ref_returns_times_out_and_raises(self) -> None:
        ref = ObjectRef("ok")
        ref.set_result(42)
        self.assertTrue(ref.ready())
        self.assertEqual(ref.get(), 42)

        timeout_ref = ObjectRef("timeout")
        with self.assertRaises(TimeoutError):
            timeout_ref.get(timeout=0)

        failed_ref = ObjectRef("failed")
        failed_ref.set_error("ValueError", "bad value")
        with self.assertRaises(RemoteError) as raised:
            failed_ref.get()
        self.assertEqual(raised.exception.error_type, "ValueError")

    def test_client_close_during_send_does_not_double_complete_ref(self) -> None:
        sock = BlockingSendSocket()
        connection = RpcClientConnection(sock, start_response_reader=False)
        refs = []
        errors = []

        def call_remote() -> None:
            try:
                refs.append(connection.call("add", (2, 3), {}))
            except Exception as exc:
                errors.append(exc)

        call_thread = Thread(target=call_remote)
        call_thread.start()
        self.assertTrue(sock.entered_send.wait(timeout=2))

        close_thread = Thread(target=connection.close)
        close_thread.start()
        sock.release_send.set()

        call_thread.join(timeout=2)
        close_thread.join(timeout=2)

        self.assertFalse(call_thread.is_alive())
        self.assertFalse(close_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(refs), 1)
        with self.assertRaises(RemoteError):
            refs[0].get(timeout=0)


class IntegrationTests(unittest.TestCase):
    def start_runtime(self) -> RpcRuntime:
        runtime = RpcRuntime(host="127.0.0.1", port=0, max_workers=8)
        runtime.start()
        self.addCleanup(runtime.stop)
        return runtime

    def connect_worker(self, runtime: RpcRuntime):
        host, port = runtime.address
        worker = connect(f"{host}:{port}", timeout=2)
        self.addCleanup(worker.close)
        return worker

    def test_remote_add(self) -> None:
        @remote
        def add(a, b):
            return a + b

        runtime = self.start_runtime()
        runtime.register(add)
        worker = self.connect_worker(runtime)

        self.assertEqual(worker.add.remote(2, 3).get(timeout=2), 5)

    def test_args_and_kwargs(self) -> None:
        @remote
        def scale_add(a, b, *, scale=1):
            return (a + b) * scale

        runtime = self.start_runtime()
        runtime.register(scale_add)
        worker = self.connect_worker(runtime)

        self.assertEqual(worker.scale_add.remote(2, 3, scale=4).get(timeout=2), 20)

    def test_remote_exception_propagates(self) -> None:
        @remote
        def fail():
            raise ValueError("bad value")

        runtime = self.start_runtime()
        runtime.register(fail)
        worker = self.connect_worker(runtime)

        with self.assertRaises(RemoteError) as raised:
            worker.fail.remote().get(timeout=2)
        self.assertEqual(raised.exception.error_type, "ValueError")

    def test_concurrent_remote_calls(self) -> None:
        @remote
        def double(value):
            return value * 2

        runtime = self.start_runtime()
        runtime.register(double)
        worker = self.connect_worker(runtime)

        refs = [worker.double.remote(i) for i in range(32)]
        self.assertEqual([ref.get(timeout=2) for ref in refs], [i * 2 for i in range(32)])

    def test_concurrent_client_threads_share_one_connection(self) -> None:
        @remote
        def triple(value):
            return value * 3

        runtime = self.start_runtime()
        runtime.register(triple)
        worker = self.connect_worker(runtime)

        with ThreadPoolExecutor(max_workers=16) as executor:
            refs = list(executor.map(lambda i: worker.triple.remote(i), range(64)))

        self.assertEqual([ref.get(timeout=2) for ref in refs], [i * 3 for i in range(64)])

    def test_runtimes_do_not_share_registry_state(self) -> None:
        @remote
        def only_here():
            return "registered"

        first = self.start_runtime()
        second = self.start_runtime()
        first.register(only_here)

        first_worker = self.connect_worker(first)
        second_worker = self.connect_worker(second)

        self.assertEqual(first_worker.only_here.remote().get(timeout=2), "registered")
        with self.assertRaises(RemoteError) as raised:
            second_worker.only_here.remote().get(timeout=2)
        self.assertEqual(raised.exception.error_type, "FunctionNotFoundError")


if __name__ == "__main__":
    unittest.main()
