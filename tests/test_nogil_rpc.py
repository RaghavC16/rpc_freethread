from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import struct
from threading import Barrier, Event, Thread
from time import monotonic, sleep
import unittest

from nogil_rpc import (
    ActorHandle,
    ObjectRef,
    RpcRuntime,
    __version__,
    connect,
    remote,
)
from nogil_rpc.errors import (
    ConnectionClosedError,
    DuplicateFunctionError,
    FunctionNotFoundError,
    FunctionNotRemoteError,
    ProtocolError,
    RemoteClassNotFoundError,
    RemoteError,
    SerializationError,
)
from nogil_rpc.protocol import read_frame, write_frame
from nogil_rpc.registry import FunctionRegistry, RemoteRegistry
from nogil_rpc.rpc_client import RpcClientConnection, _parse_address, _parse_catalog
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


class FailResultSerializer(PickleSerializer):
    def dumps(self, value):
        if isinstance(value, dict) and value.get("type") == "result":
            raise SerializationError("result serialization intentionally failed")
        return super().dumps(value)


class CoreTests(unittest.TestCase):
    def test_remote_marks_function(self) -> None:
        @remote
        def marked_add(a, b):
            return a + b

        self.assertIs(marked_add.__remote__, True)
        self.assertEqual(marked_add(2, 3), 5)

    def test_registry_requires_remote_functions(self) -> None:
        def plain():
            return None

        with self.assertRaises(FunctionNotRemoteError):
            FunctionRegistry().register(plain)

    def test_registry_rejects_duplicates(self) -> None:
        @remote
        def duplicate_add(a, b):
            return a + b

        registry = FunctionRegistry()
        registry.register(duplicate_add)

        with self.assertRaises(DuplicateFunctionError):
            registry.register(duplicate_add)

    def test_registry_missing_function(self) -> None:
        with self.assertRaises(FunctionNotFoundError):
            FunctionRegistry().get("missing")

        with self.assertRaises(RemoteClassNotFoundError):
            RemoteRegistry().get_class("MissingActor")

    def test_remote_registry_separates_functions_and_classes(self) -> None:
        registry = RemoteRegistry()

        def registry_function():
            return None

        class RegistryActor:
            pass

        registry_function.__remote__ = True
        RegistryActor.__remote__ = True
        registry.register(registry_function)
        registry.register(RegistryActor)

        self.assertIs(registry.get_function("registry_function"), registry_function)
        self.assertIs(registry.get_class("RegistryActor"), RegistryActor)
        self.assertEqual(
            registry.catalog(),
            (("registry_function",), ("RegistryActor",)),
        )

    def test_remote_registry_rejects_names_shared_by_function_and_class(self) -> None:
        registry = RemoteRegistry()

        def SharedRemoteName():
            return None

        duplicate_class = type("SharedRemoteName", (), {})
        SharedRemoteName.__remote__ = True
        duplicate_class.__remote__ = True
        registry.register(SharedRemoteName)

        with self.assertRaises(DuplicateFunctionError):
            registry.register(duplicate_class)

    def test_catalog_validation(self) -> None:
        self.assertEqual(
            _parse_catalog(
                {
                    "type": "catalog",
                    "functions": ("add",),
                    "actors": ("Counter",),
                }
            ),
            frozenset({"Counter"}),
        )

        invalid_catalogs = (
            None,
            {"type": "result", "functions": (), "actors": ()},
            {
                "type": "catalog",
                "functions": "add",
                "actors": (),
            },
            {
                "type": "catalog",
                "functions": (),
                "actors": (1,),
            },
            {
                "type": "catalog",
                "functions": ("Same",),
                "actors": ("Same",),
            },
        )
        for catalog in invalid_catalogs:
            with self.subTest(catalog=catalog):
                with self.assertRaises(ProtocolError):
                    _parse_catalog(catalog)

    def test_public_version_metadata(self) -> None:
        self.assertEqual(__version__, "0.2.0")

    def test_runtime_stop_is_idempotent_and_prevents_restart(self) -> None:
        runtime = RpcRuntime()
        runtime.stop()
        runtime.stop()

        with self.assertRaises(RuntimeError):
            runtime.start()

    def test_frame_limit_is_configurable_and_validated(self) -> None:
        runtime = RpcRuntime(max_frame_size=1024)
        self.assertEqual(runtime._max_frame_size, 1024)
        runtime.stop()

        for invalid in (0, -1, 1.5, True):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    RpcRuntime(max_frame_size=invalid)
                with self.assertRaises(ValueError):
                    connect("127.0.0.1:1", max_frame_size=invalid)

    def test_serializer_round_trips_python_objects(self) -> None:
        serializer = PickleSerializer()
        value = {"numbers": [1, 2, 3], "nested": {"ok": True}}

        self.assertEqual(serializer.loads(serializer.dumps(value)), value)

        with self.assertRaises(SerializationError):
            serializer.dumps(lambda: None)
        with self.assertRaises(SerializationError):
            serializer.loads(b"not a pickle payload")

    def test_frame_helpers_preserve_boundaries(self) -> None:
        sock = FakeSocket()

        write_frame(sock, b"hello")
        write_frame(sock, b"world")

        self.assertEqual(read_frame(sock), b"hello")
        self.assertEqual(read_frame(sock), b"world")

    def test_frame_helpers_reject_invalid_or_closed_frames(self) -> None:
        with self.assertRaises(ProtocolError):
            write_frame(FakeSocket(), b"too large", max_frame_size=4)

        oversized = FakeSocket()
        oversized.buffer.extend(struct.pack(">I", 10))
        with self.assertRaises(ProtocolError):
            read_frame(oversized, max_frame_size=4)

        with self.assertRaises(ConnectionClosedError):
            read_frame(FakeSocket())

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

        with self.assertRaises(RuntimeError):
            ref.set_result(43)
        with self.assertRaises(RuntimeError):
            failed_ref.set_error("ValueError", "again")

    def test_client_rejects_invalid_or_unknown_responses(self) -> None:
        connection = RpcClientConnection(FakeSocket(), start_response_reader=False)

        invalid_responses = (
            None,
            {"type": "call"},
            {"type": "result", "call_id": 1, "ok": True},
            {"type": "result", "call_id": "unknown", "ok": True},
        )
        for response in invalid_responses:
            with self.subTest(response=response):
                with self.assertRaises(ProtocolError):
                    connection._handle_response(response)

    def test_address_validation(self) -> None:
        self.assertEqual(_parse_address("localhost:50051"), ("localhost", 50051))
        for address in ("localhost", ":50051", "localhost:", "localhost:not-a-port"):
            with self.subTest(address=address):
                with self.assertRaises(ValueError):
                    _parse_address(address)

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
        def integration_add(a, b):
            return a + b

        runtime = self.start_runtime()
        worker = self.connect_worker(runtime)

        self.assertEqual(worker.integration_add.remote(2, 3).get(timeout=2), 5)

    def test_runtime_and_connection_context_managers(self) -> None:
        @remote
        def context_add(a, b):
            return a + b

        with RpcRuntime(host="127.0.0.1", port=0) as runtime:
            host, port = runtime.address
            with connect(f"{host}:{port}", timeout=2) as worker:
                self.assertEqual(worker.context_add.remote(4, 5).get(timeout=2), 9)

            with self.assertRaises(ConnectionClosedError):
                worker.context_add.remote(1, 2)

    def test_actor_context_manager_closes_actor(self) -> None:
        @remote
        class ContextActor:
            def value(self):
                return 7

        runtime = self.start_runtime()
        worker = self.connect_worker(runtime)
        with worker.ContextActor.remote() as actor:
            self.assertEqual(actor.value.remote().get(timeout=2), 7)

        with self.assertRaises(ConnectionClosedError):
            actor.value.remote()

    def test_args_and_kwargs(self) -> None:
        @remote
        def scale_add(a, b, *, scale=1):
            return (a + b) * scale

        runtime = self.start_runtime()
        worker = self.connect_worker(runtime)

        self.assertEqual(worker.scale_add.remote(2, 3, scale=4).get(timeout=2), 20)

    def test_remote_exception_propagates(self) -> None:
        @remote
        def fail():
            raise ValueError("bad value")

        runtime = self.start_runtime()
        worker = self.connect_worker(runtime)

        with self.assertRaises(RemoteError) as raised:
            worker.fail.remote().get(timeout=2)
        self.assertEqual(raised.exception.error_type, "ValueError")

    def test_unknown_remote_function_propagates_registry_error(self) -> None:
        runtime = self.start_runtime()
        worker = self.connect_worker(runtime)

        with self.assertRaises(RemoteError) as raised:
            worker.function_that_does_not_exist.remote().get(timeout=2)
        self.assertEqual(raised.exception.error_type, "FunctionNotFoundError")

    def test_concurrent_remote_calls(self) -> None:
        @remote
        def double(value):
            return value * 2

        runtime = self.start_runtime()
        worker = self.connect_worker(runtime)

        refs = [worker.double.remote(i) for i in range(32)]
        self.assertEqual([ref.get(timeout=2) for ref in refs], [i * 2 for i in range(32)])

    def test_concurrent_client_threads_share_one_connection(self) -> None:
        @remote
        def triple(value):
            return value * 3

        runtime = self.start_runtime()
        worker = self.connect_worker(runtime)

        with ThreadPoolExecutor(max_workers=16) as executor:
            refs = list(executor.map(lambda i: worker.triple.remote(i), range(64)))

        self.assertEqual([ref.get(timeout=2) for ref in refs], [i * 3 for i in range(64)])

    def test_runtimes_share_decorated_functions(self) -> None:
        @remote
        def only_here():
            return "registered"

        first = self.start_runtime()
        second = self.start_runtime()

        first_worker = self.connect_worker(first)
        second_worker = self.connect_worker(second)

        self.assertEqual(first_worker.only_here.remote().get(timeout=2), "registered")
        self.assertEqual(second_worker.only_here.remote().get(timeout=2), "registered")

    def test_remote_actor_preserves_member_state_and_call_order(self) -> None:
        @remote
        class StatefulCounter:
            def __init__(self, value=0):
                self.value = value

            def increment(self, amount=1):
                self.value += amount
                return self.value

            def get(self):
                return self.value

        runtime = self.start_runtime()
        worker = self.connect_worker(runtime)
        counter = worker.StatefulCounter.remote(10)
        self.addCleanup(counter.close)

        self.assertIsInstance(counter, ActorHandle)
        refs = [counter.increment.remote() for _ in range(3)]
        self.assertEqual([ref.get(timeout=2) for ref in refs], [11, 12, 13])
        self.assertEqual(counter.increment.remote(amount=7).get(timeout=2), 20)
        self.assertEqual(counter.get.remote().get(timeout=2), 20)

    def test_remote_actor_instances_have_isolated_state(self) -> None:
        @remote
        class IsolatedCounter:
            def __init__(self, value):
                self.value = value

            def increment(self):
                self.value += 1
                return self.value

        runtime = self.start_runtime()
        worker = self.connect_worker(runtime)
        first = worker.IsolatedCounter.remote(0)
        second = worker.IsolatedCounter.remote(100)
        self.addCleanup(first.close)
        self.addCleanup(second.close)

        self.assertEqual(first.increment.remote().get(timeout=2), 1)
        self.assertEqual(second.increment.remote().get(timeout=2), 101)

    def test_actor_can_be_shared_through_non_owning_attached_handle(self) -> None:
        @remote
        class SharedCounter:
            def __init__(self):
                self.value = 0

            def increment(self):
                self.value += 1
                return self.value

        runtime = self.start_runtime()
        owner_process = self.connect_worker(runtime)
        attached_process = self.connect_worker(runtime)
        owner = owner_process.SharedCounter.remote()
        attached = attached_process.attach_actor(owner.actor_id)
        self.addCleanup(owner.close)

        self.assertTrue(owner.owns_actor)
        self.assertFalse(attached.owns_actor)
        self.assertEqual(owner.increment.remote().get(timeout=2), 1)
        self.assertEqual(attached.increment.remote().get(timeout=2), 2)

        attached.close()
        self.assertEqual(owner.increment.remote().get(timeout=2), 3)

    def test_attaching_missing_actor_fails(self) -> None:
        runtime = self.start_runtime()
        worker = self.connect_worker(runtime)

        with self.assertRaises(RemoteError) as raised:
            worker.attach_actor("missing-actor")
        self.assertEqual(raised.exception.error_type, "ActorNotFoundError")

    def test_non_owner_connection_cannot_destroy_shared_actor(self) -> None:
        @remote
        class ProtectedActor:
            def ping(self):
                return "pong"

        runtime = self.start_runtime()
        owner_process = self.connect_worker(runtime)
        attached_process = self.connect_worker(runtime)
        owner = owner_process.ProtectedActor.remote()
        attached = attached_process.attach_actor(owner.actor_id)
        self.addCleanup(owner.close)

        with self.assertRaises(RemoteError) as raised:
            attached_process._connection.destroy_actor(owner.actor_id)
        self.assertEqual(raised.exception.error_type, "ActorOwnershipError")
        self.assertEqual(attached.ping.remote().get(timeout=2), "pong")

    def test_owner_disconnect_releases_actor_for_attached_handles(self) -> None:
        @remote
        class OwnerScopedActor:
            def ping(self):
                return "pong"

        runtime = self.start_runtime()
        owner_process = self.connect_worker(runtime)
        attached_process = self.connect_worker(runtime)
        owner = owner_process.OwnerScopedActor.remote()
        attached = attached_process.attach_actor(owner.actor_id)

        owner_process.close()
        deadline = monotonic() + 2
        while monotonic() < deadline:
            with runtime._actors_lock:
                if owner.actor_id not in runtime._actors:
                    break
            sleep(0.01)

        with self.assertRaises(RemoteError) as raised:
            attached.ping.remote().get(timeout=2)
        self.assertEqual(raised.exception.error_type, "ActorNotFoundError")

    def test_separate_remote_actors_execute_concurrently(self) -> None:
        rendezvous = Barrier(2)

        @remote
        class ConcurrentActor:
            def meet(self):
                rendezvous.wait(timeout=2)
                return "met"

        runtime = self.start_runtime()
        worker = self.connect_worker(runtime)
        first = worker.ConcurrentActor.remote()
        second = worker.ConcurrentActor.remote()
        self.addCleanup(first.close)
        self.addCleanup(second.close)

        first_ref = first.meet.remote()
        second_ref = second.meet.remote()
        self.assertEqual(first_ref.get(timeout=2), "met")
        self.assertEqual(second_ref.get(timeout=2), "met")

    def test_remote_actor_propagates_errors_and_can_be_destroyed(self) -> None:
        @remote
        class FallibleActor:
            def fail(self):
                raise ValueError("actor failed")

        runtime = self.start_runtime()
        worker = self.connect_worker(runtime)
        actor = worker.FallibleActor.remote()

        with self.assertRaises(RemoteError) as raised:
            actor.fail.remote().get(timeout=2)
        self.assertEqual(raised.exception.error_type, "ValueError")

        actor.close()
        actor.close()
        with self.assertRaises(ConnectionClosedError):
            actor.fail.remote()

    def test_remote_actor_constructor_error_is_raised(self) -> None:
        @remote
        class BrokenActor:
            def __init__(self):
                raise ValueError("constructor failed")

        runtime = self.start_runtime()
        worker = self.connect_worker(runtime)

        with self.assertRaises(RemoteError) as raised:
            worker.BrokenActor.remote()
        self.assertEqual(raised.exception.error_type, "ValueError")

    def test_remote_actor_rejects_missing_private_and_noncallable_members(self) -> None:
        @remote
        class StrictActor:
            def __init__(self):
                self.value = 42

            def _private(self):
                return "private"

        runtime = self.start_runtime()
        worker = self.connect_worker(runtime)
        actor = worker.StrictActor.remote()
        self.addCleanup(actor.close)

        with self.assertRaises(RemoteError) as missing:
            actor.missing.remote().get(timeout=2)
        self.assertEqual(missing.exception.error_type, "AttributeError")

        with self.assertRaises(RemoteError) as noncallable:
            actor.value.remote().get(timeout=2)
        self.assertEqual(noncallable.exception.error_type, "TypeError")

        with self.assertRaises(AttributeError):
            actor._private.remote()

    def test_async_and_generator_callables_fail_explicitly(self) -> None:
        @remote
        async def unsupported_async_function():
            return 1

        @remote
        def unsupported_generator_function():
            yield 1

        @remote
        class UnsupportedCallableActor:
            async def async_method(self):
                return 1

            def generator_method(self):
                yield 1

        runtime = self.start_runtime()
        worker = self.connect_worker(runtime)
        actor = worker.UnsupportedCallableActor.remote()
        self.addCleanup(actor.close)

        calls = (
            worker.unsupported_async_function.remote(),
            worker.unsupported_generator_function.remote(),
            actor.async_method.remote(),
            actor.generator_method.remote(),
        )
        for ref in calls:
            with self.subTest(call_id=ref.call_id):
                with self.assertRaises(RemoteError) as raised:
                    ref.get(timeout=2)
                self.assertEqual(raised.exception.error_type, "TypeError")

    def test_unpickleable_results_become_remote_serialization_errors(self) -> None:
        @remote
        def return_unpickleable_result():
            return lambda: None

        runtime = self.start_runtime()
        worker = self.connect_worker(runtime)

        with self.assertRaises(RemoteError) as raised:
            worker.return_unpickleable_result.remote().get(timeout=2)
        self.assertEqual(raised.exception.error_type, "SerializationError")

    def test_total_response_serialization_failure_closes_connection(self) -> None:
        @remote
        def response_that_cannot_be_serialized():
            return 1

        runtime = RpcRuntime(
            host="127.0.0.1",
            port=0,
            serializer=FailResultSerializer(),
        )
        runtime.start()
        self.addCleanup(runtime.stop)
        host, port = runtime.address
        worker = connect(
            f"{host}:{port}",
            serializer=FailResultSerializer(),
            timeout=2,
        )
        self.addCleanup(worker.close)

        ref = worker.response_that_cannot_be_serialized.remote()
        with self.assertRaises(RemoteError):
            ref.get(timeout=2)
        self.assertTrue(ref.ready())

    def test_concurrent_actor_calls_preserve_every_state_update(self) -> None:
        @remote
        class ContendedCounter:
            def __init__(self):
                self.value = 0

            def increment(self):
                self.value += 1
                return self.value

            def get(self):
                return self.value

        runtime = self.start_runtime()
        worker = self.connect_worker(runtime)
        actor = worker.ContendedCounter.remote()
        self.addCleanup(actor.close)

        with ThreadPoolExecutor(max_workers=16) as executor:
            refs = list(executor.map(lambda _: actor.increment.remote(), range(100)))

        results = [ref.get(timeout=2) for ref in refs]
        self.assertEqual(sorted(results), list(range(1, 101)))
        self.assertEqual(actor.get.remote().get(timeout=2), 100)

    def test_client_disconnect_releases_owned_actors(self) -> None:
        @remote
        class DisconnectActor:
            def ping(self):
                return "pong"

        runtime = self.start_runtime()
        worker = self.connect_worker(runtime)
        actor = worker.DisconnectActor.remote()
        with runtime._actors_lock:
            self.assertEqual(len(runtime._actors), 1)

        worker.close()
        deadline = monotonic() + 2
        while monotonic() < deadline:
            with runtime._actors_lock:
                if not runtime._actors:
                    break
            sleep(0.01)
        with runtime._actors_lock:
            self.assertEqual(runtime._actors, {})
        actor.close()


if __name__ == "__main__":
    unittest.main()
