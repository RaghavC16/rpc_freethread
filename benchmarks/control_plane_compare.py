"""Compare Ray and nogil_rpc on a branch-and-bound control-plane workload.

The workload intentionally transports only small Python metadata records. It
models the shared domain-list actor in the alpha-beta-CROWN neural network
verifier:

* concurrent coordinators claim a batch of domains from one stateful actor;
* coordinators perform a tiny deterministic "branch" step locally;
* generated child domains are submitted to the shared actor;
* periodic size queries report progress.

This is a control-plane benchmark, not a tensor/data-plane benchmark. The
comparison runs Ray and nogil_rpc under the same regular CPython interpreter,
then runs nogil_rpc under the matching free-threaded CPython build. Framework
startup and actor creation are reported separately from steady-state RPC time.
"""

from __future__ import annotations

import argparse
import heapq
import json
import os
import platform
import subprocess
import sys
import sysconfig
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from statistics import median
from threading import Barrier
from typing import Any, Protocol

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from nogil_rpc import RpcRuntime, connect, remote

MASK_63 = (1 << 63) - 1
Domain = tuple[int, int, int]


class FrontierCore:
    """Single-owner priority frontier used by both RPC backends."""

    def __init__(self, initial_domains: int) -> None:
        if initial_domains < 1:
            raise ValueError("initial_domains must be at least 1")

        self._heap: list[tuple[int, int, int, int]] = []
        self._next_sequence = 0
        self._claimed = 0
        self._added = 0
        self._claim_calls = 0
        self._submit_calls = 0
        self._size_calls = 0

        initial = [
            ((token * 104_729) & MASK_63, 0, token)
            for token in range(initial_domains)
        ]
        self._push_domains(initial)

    def claim_batch(self, batch_size: int) -> list[Domain]:
        """Remove and return the highest-priority metadata batch."""
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")

        self._claim_calls += 1
        count = min(batch_size, len(self._heap))
        claimed = []
        for _ in range(count):
            priority, _, depth, token = heapq.heappop(self._heap)
            claimed.append((priority, depth, token))
        self._claimed += len(claimed)
        return claimed

    def submit_children(self, domains: list[Domain]) -> int:
        """Add branched metadata records and return the new frontier size."""
        self._submit_calls += 1
        self._added += len(domains)
        self._push_domains(domains)
        return len(self._heap)

    def size(self) -> int:
        self._size_calls += 1
        return len(self._heap)

    def snapshot(self) -> dict[str, int]:
        """Return compact state used to validate equivalent execution."""
        return {
            "frontier_size": len(self._heap),
            "claimed": self._claimed,
            "added": self._added,
            "claim_calls": self._claim_calls,
            "submit_calls": self._submit_calls,
            "size_calls": self._size_calls,
        }

    def _push_domains(self, domains: list[Domain]) -> None:
        for priority, depth, token in domains:
            heapq.heappush(
                self._heap,
                (priority, self._next_sequence, depth, token),
            )
            self._next_sequence += 1


@remote
class RpcFrontierActor(FrontierCore):
    """nogil_rpc actor exposing the shared control-plane frontier."""


class RayFrontierActor(FrontierCore):
    """Class wrapped with ray.remote only when the Ray backend is selected."""


class FrontierClient(Protocol):
    def claim_batch(self, batch_size: int) -> list[Domain]: ...

    def submit_children(self, domains: list[Domain]) -> int: ...

    def size(self) -> int: ...

    def snapshot(self) -> dict[str, int]: ...


class RpcFrontierClient:
    def __init__(self, actor: Any, timeout: float) -> None:
        self._actor = actor
        self._timeout = timeout

    def claim_batch(self, batch_size: int) -> list[Domain]:
        return self._actor.claim_batch.remote(batch_size).get(timeout=self._timeout)

    def submit_children(self, domains: list[Domain]) -> int:
        return self._actor.submit_children.remote(domains).get(timeout=self._timeout)

    def size(self) -> int:
        return self._actor.size.remote().get(timeout=self._timeout)

    def snapshot(self) -> dict[str, int]:
        return self._actor.snapshot.remote().get(timeout=self._timeout)


class RayFrontierClient:
    def __init__(self, actor: Any, ray_module: Any) -> None:
        self._actor = actor
        self._ray = ray_module

    def claim_batch(self, batch_size: int) -> list[Domain]:
        return self._ray.get(self._actor.claim_batch.remote(batch_size))

    def submit_children(self, domains: list[Domain]) -> int:
        return self._ray.get(self._actor.submit_children.remote(domains))

    def size(self) -> int:
        return self._ray.get(self._actor.size.remote())

    def snapshot(self) -> dict[str, int]:
        return self._ray.get(self._actor.snapshot.remote())


def branch_domains(
    domains: list[Domain],
    *,
    coordinator_id: int,
    round_index: int,
    branch_factor: int,
) -> tuple[list[Domain], int]:
    """Create small deterministic child records outside the frontier actor."""
    children = []
    checksum = 0
    for priority, depth, token in domains:
        for branch_index in range(branch_factor):
            child_token = (
                token * 6_364_136_223_846_793_005
                + coordinator_id * 1_442_695_040_888_963_407
                + round_index * 104_729
                + branch_index
                + 1
            ) & MASK_63
            child_priority = (
                priority
                + (child_token & 0xFFFF)
                + (depth + 1) * 257
                + branch_index
            ) & MASK_63
            children.append((child_priority, depth + 1, child_token))
            checksum ^= child_token
    return children, checksum


def exercise_control_plane(
    frontier: FrontierClient,
    *,
    coordinators: int,
    rounds: int,
    batch_size: int,
    branch_factor: int,
    query_every: int,
) -> tuple[float, int]:
    """Drive concurrent synchronous control-plane calls to one actor."""
    barrier = Barrier(coordinators + 1)

    def coordinate(coordinator_id: int) -> int:
        checksum = 0
        barrier.wait()
        for round_index in range(rounds):
            domains = frontier.claim_batch(batch_size)
            if len(domains) != batch_size:
                raise RuntimeError(
                    f"frontier underflow: requested {batch_size}, got {len(domains)}"
                )
            children, round_checksum = branch_domains(
                domains,
                coordinator_id=coordinator_id,
                round_index=round_index,
                branch_factor=branch_factor,
            )
            frontier.submit_children(children)
            checksum ^= round_checksum
            if query_every > 0 and (round_index + 1) % query_every == 0:
                checksum ^= frontier.size()
        return checksum

    with ThreadPoolExecutor(
        max_workers=coordinators,
        thread_name_prefix="control-plane-coordinator",
    ) as executor:
        futures = [
            executor.submit(coordinate, coordinator_id)
            for coordinator_id in range(coordinators)
        ]
        started = time.perf_counter()
        barrier.wait()
        checksums = [future.result() for future in futures]
        elapsed = time.perf_counter() - started

    checksum = 0
    for value in checksums:
        checksum ^= value
    return elapsed, checksum


def validate_snapshot(
    snapshot: dict[str, int],
    *,
    initial_domains: int,
    coordinators: int,
    rounds: int,
    batch_size: int,
    branch_factor: int,
    query_every: int,
) -> None:
    claim_calls = coordinators * rounds
    claimed = claim_calls * batch_size
    added = claimed * branch_factor
    size_calls = (
        coordinators * (rounds // query_every)
        if query_every > 0
        else 0
    )
    expected = {
        "frontier_size": initial_domains - claimed + added,
        "claimed": claimed,
        "added": added,
        "claim_calls": claim_calls,
        "submit_calls": claim_calls,
        "size_calls": size_calls,
    }
    if snapshot != expected:
        raise RuntimeError(
            f"backend produced an invalid frontier snapshot: "
            f"expected={expected}, actual={snapshot}"
        )


def run_backend(
    *,
    backend: str,
    coordinators: int,
    rounds: int,
    batch_size: int,
    branch_factor: int,
    query_every: int,
    initial_domains: int | None,
    timeout: float,
) -> dict[str, Any]:
    if coordinators < 1:
        raise ValueError("coordinators must be at least 1")
    if rounds < 1:
        raise ValueError("rounds must be at least 1")
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")
    if branch_factor < 1:
        raise ValueError("branch_factor must be at least 1")
    if query_every < 0:
        raise ValueError("query_every cannot be negative")

    initial_count = (
        initial_domains
        if initial_domains is not None
        else max(coordinators * batch_size * 2, batch_size)
    )

    total_started = time.perf_counter()
    if backend == "nogil_rpc":
        result = _run_nogil_rpc(
            coordinators=coordinators,
            rounds=rounds,
            batch_size=batch_size,
            branch_factor=branch_factor,
            query_every=query_every,
            initial_domains=initial_count,
            timeout=timeout,
        )
    elif backend == "ray":
        result = _run_ray(
            coordinators=coordinators,
            rounds=rounds,
            batch_size=batch_size,
            branch_factor=branch_factor,
            query_every=query_every,
            initial_domains=initial_count,
        )
    else:
        raise ValueError("backend must be 'nogil_rpc' or 'ray'")
    result["total_seconds"] = time.perf_counter() - total_started
    return result


def _run_nogil_rpc(
    *,
    coordinators: int,
    rounds: int,
    batch_size: int,
    branch_factor: int,
    query_every: int,
    initial_domains: int,
    timeout: float,
) -> dict[str, Any]:
    startup_started = time.perf_counter()
    runtime = RpcRuntime(
        host="127.0.0.1",
        port=0,
        max_workers=max(2, coordinators),
    )
    runtime.start()
    host, port = runtime.address
    worker = connect(f"{host}:{port}", timeout=timeout)
    startup_seconds = time.perf_counter() - startup_started

    actor = None
    try:
        actor_started = time.perf_counter()
        actor = worker.RpcFrontierActor.remote(initial_domains)
        frontier = RpcFrontierClient(actor, timeout)
        actor_create_seconds = time.perf_counter() - actor_started

        workload_seconds, checksum = exercise_control_plane(
            frontier,
            coordinators=coordinators,
            rounds=rounds,
            batch_size=batch_size,
            branch_factor=branch_factor,
            query_every=query_every,
        )
        snapshot = frontier.snapshot()
    finally:
        if actor is not None:
            actor.close()
        worker.close()
        runtime.stop()

    return _build_result(
        backend="nogil_rpc",
        framework_version="local-0.1.0",
        startup_seconds=startup_seconds,
        actor_create_seconds=actor_create_seconds,
        workload_seconds=workload_seconds,
        checksum=checksum,
        snapshot=snapshot,
        coordinators=coordinators,
        rounds=rounds,
        batch_size=batch_size,
        branch_factor=branch_factor,
        query_every=query_every,
        initial_domains=initial_domains,
    )


def _run_ray(
    *,
    coordinators: int,
    rounds: int,
    batch_size: int,
    branch_factor: int,
    query_every: int,
    initial_domains: int,
) -> dict[str, Any]:
    try:
        import ray
    except ImportError as exc:
        raise RuntimeError(
            "Ray is not installed in this interpreter; install it in the "
            "regular interpreter or pass --regular-python to compare"
        ) from exc

    startup_started = time.perf_counter()
    ray.init(
        num_cpus=max(2, coordinators + 1),
        include_dashboard=False,
        ignore_reinit_error=True,
        logging_level="ERROR",
    )
    startup_seconds = time.perf_counter() - startup_started

    actor = None
    try:
        actor_started = time.perf_counter()
        actor_class = ray.remote(num_cpus=1)(RayFrontierActor)
        actor = actor_class.remote(initial_domains)
        ray.get(actor.snapshot.remote())
        frontier = RayFrontierClient(actor, ray)
        actor_create_seconds = time.perf_counter() - actor_started

        workload_seconds, checksum = exercise_control_plane(
            frontier,
            coordinators=coordinators,
            rounds=rounds,
            batch_size=batch_size,
            branch_factor=branch_factor,
            query_every=query_every,
        )
        snapshot = frontier.snapshot()
    finally:
        if actor is not None:
            ray.kill(actor)
        ray.shutdown()

    return _build_result(
        backend="ray",
        framework_version=ray.__version__,
        startup_seconds=startup_seconds,
        actor_create_seconds=actor_create_seconds,
        workload_seconds=workload_seconds,
        checksum=checksum,
        snapshot=snapshot,
        coordinators=coordinators,
        rounds=rounds,
        batch_size=batch_size,
        branch_factor=branch_factor,
        query_every=query_every,
        initial_domains=initial_domains,
    )


def _build_result(
    *,
    backend: str,
    framework_version: str,
    startup_seconds: float,
    actor_create_seconds: float,
    workload_seconds: float,
    checksum: int,
    snapshot: dict[str, int],
    coordinators: int,
    rounds: int,
    batch_size: int,
    branch_factor: int,
    query_every: int,
    initial_domains: int,
) -> dict[str, Any]:
    validate_snapshot(
        snapshot,
        initial_domains=initial_domains,
        coordinators=coordinators,
        rounds=rounds,
        batch_size=batch_size,
        branch_factor=branch_factor,
        query_every=query_every,
    )
    mutation_calls = coordinators * rounds * 2
    query_calls = (
        coordinators * (rounds // query_every)
        if query_every > 0
        else 0
    )
    control_calls = mutation_calls + query_calls

    return {
        "backend": backend,
        "framework_version": framework_version,
        "executable": sys.executable,
        "python_version": sys.version.split()[0],
        "platform": platform.platform(),
        "logical_cpu_count": os.cpu_count(),
        "free_threaded_build": bool(sysconfig.get_config_var("Py_GIL_DISABLED")),
        "gil_enabled": _is_gil_enabled(),
        "coordinators": coordinators,
        "rounds_per_coordinator": rounds,
        "batch_size": batch_size,
        "branch_factor": branch_factor,
        "query_every": query_every,
        "initial_domains": initial_domains,
        "control_calls": control_calls,
        "startup_seconds": startup_seconds,
        "actor_create_seconds": actor_create_seconds,
        "workload_seconds": workload_seconds,
        "control_calls_per_second": (
            control_calls / workload_seconds if workload_seconds else 0.0
        ),
        "amortized_call_time_ms": (
            workload_seconds * 1000.0 / control_calls if control_calls else 0.0
        ),
        "checksum": checksum,
        "snapshot": snapshot,
    }


def compare_backends(
    *,
    regular_python: Path,
    free_threaded_python: Path,
    repetitions: int,
    coordinator_counts: list[int],
    workload_args: list[str],
) -> dict[str, Any]:
    if repetitions < 1:
        raise ValueError("repetitions must be at least 1")
    if not coordinator_counts or any(count < 1 for count in coordinator_counts):
        raise ValueError("coordinator counts must all be at least 1")

    script = Path(__file__).resolve()
    comparisons = []
    for coordinator_count in coordinator_counts:
        backend_args = [
            "--coordinators",
            str(coordinator_count),
            *workload_args,
        ]
        ray_regular_runs = [
            _run_child(
                [str(regular_python), str(script), "run"],
                ["--backend", "ray", *backend_args],
            )
            for _ in range(repetitions)
        ]
        rpc_regular_runs = [
            _run_child(
                [str(regular_python), str(script), "run"],
                ["--backend", "nogil_rpc", *backend_args],
            )
            for _ in range(repetitions)
        ]
        rpc_free_threaded_runs = [
            _run_child(
                [
                    str(free_threaded_python),
                    "-X",
                    "gil=0",
                    str(script),
                    "run",
                ],
                ["--backend", "nogil_rpc", *backend_args],
            )
            for _ in range(repetitions)
        ]

        ray_regular = _median_result(ray_regular_runs)
        rpc_regular = _median_result(rpc_regular_runs)
        rpc_free_threaded = _median_result(rpc_free_threaded_runs)
        ray_rate = ray_regular["control_calls_per_second"]
        rpc_regular_rate = rpc_regular["control_calls_per_second"]
        comparisons.append(
            {
                "coordinators": coordinator_count,
                "ray_regular": ray_regular,
                "nogil_rpc_regular": rpc_regular,
                "nogil_rpc_free_threaded": rpc_free_threaded,
                "nogil_rpc_regular_throughput_vs_ray_regular": (
                    rpc_regular_rate / ray_rate
                    if ray_rate
                    else 0.0
                ),
                "nogil_rpc_free_threaded_throughput_vs_regular": (
                    rpc_free_threaded["control_calls_per_second"]
                    / rpc_regular_rate
                    if rpc_regular_rate
                    else 0.0
                ),
                "nogil_rpc_free_threaded_throughput_vs_ray_regular": (
                    rpc_free_threaded["control_calls_per_second"] / ray_rate
                    if ray_rate
                    else 0.0
                ),
                "raw_runs": {
                    "ray_regular": ray_regular_runs,
                    "nogil_rpc_regular": rpc_regular_runs,
                    "nogil_rpc_free_threaded": rpc_free_threaded_runs,
                },
            }
        )

    return {
        "repetitions": repetitions,
        "coordinator_counts": coordinator_counts,
        "comparisons": comparisons,
    }


def _run_child(command_prefix: list[str], args: list[str]) -> dict[str, Any]:
    completed = subprocess.run(
        [*command_prefix, *args, "--json"],
        text=True,
        capture_output=True,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(
            f"benchmark child failed with exit code {completed.returncode}: "
            f"{' '.join(command_prefix)}\n{detail}"
        )
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "benchmark child did not emit valid JSON:\n"
            f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        ) from exc


def _median_result(runs: list[dict[str, Any]]) -> dict[str, Any]:
    result = runs[0].copy()
    for key in (
        "startup_seconds",
        "actor_create_seconds",
        "workload_seconds",
        "control_calls_per_second",
        "amortized_call_time_ms",
        "total_seconds",
    ):
        result[key] = median(run[key] for run in runs)
    result["repetitions"] = len(runs)
    return result


def _is_gil_enabled() -> bool:
    checker = getattr(sys, "_is_gil_enabled", None)
    if checker is not None:
        return bool(checker())
    return not bool(sysconfig.get_config_var("Py_GIL_DISABLED"))


def _add_workload_args(
    parser: argparse.ArgumentParser,
    *,
    coordinator_sweep: bool,
) -> None:
    if coordinator_sweep:
        parser.add_argument(
            "--coordinators",
            type=int,
            nargs="+",
            default=[1, 2, 4, 6],
            metavar="N",
        )
    else:
        parser.add_argument("--coordinators", type=int, default=6)
    parser.add_argument("--rounds", type=int, default=200)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--branch-factor", type=int, default=2)
    parser.add_argument(
        "--query-every",
        type=int,
        default=10,
        help="issue one size query every N rounds per coordinator; 0 disables",
    )
    parser.add_argument(
        "--initial-domains",
        type=int,
        default=None,
        help="initial frontier size (default: 2 * coordinators * batch size)",
    )
    parser.add_argument("--timeout", type=float, default=120.0)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare Ray and nogil_rpc on shared-actor control-plane RPCs."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="run one backend")
    run_parser.add_argument(
        "--backend",
        choices=("nogil_rpc", "ray"),
        required=True,
    )
    _add_workload_args(run_parser, coordinator_sweep=False)
    run_parser.add_argument("--json", action="store_true")

    compare_parser = subparsers.add_parser(
        "compare",
        help="compare both frameworks with the GIL, then nogil_rpc without it",
    )
    compare_parser.add_argument(
        "--regular-python",
        type=Path,
        default=Path(".python-3.14/bin/python3.14"),
        help="regular CPython used for both Ray and nogil_rpc",
    )
    compare_parser.add_argument(
        "--free-threaded-python",
        type=Path,
        default=Path(".venv-ft/bin/python"),
        help="free-threaded CPython used for nogil_rpc with -X gil=0",
    )
    compare_parser.add_argument("--repetitions", type=int, default=3)
    _add_workload_args(compare_parser, coordinator_sweep=True)
    compare_parser.add_argument("--json", action="store_true")
    return parser


def _workload_cli_args(args: argparse.Namespace) -> list[str]:
    values = [
        "--rounds",
        str(args.rounds),
        "--batch-size",
        str(args.batch_size),
        "--branch-factor",
        str(args.branch_factor),
        "--query-every",
        str(args.query_every),
        "--timeout",
        str(args.timeout),
    ]
    if args.initial_domains is not None:
        values.extend(("--initial-domains", str(args.initial_domains)))
    return values


def print_run_result(result: dict[str, Any]) -> None:
    print(
        f"{result['backend']}: {result['control_calls_per_second']:.1f} "
        f"control calls/s, {result['amortized_call_time_ms']:.3f} "
        "amortized ms/call"
    )
    print(
        f"  workload={result['workload_seconds']:.3f}s, "
        f"startup={result['startup_seconds']:.3f}s, "
        f"actor_create={result['actor_create_seconds']:.3f}s"
    )
    print(
        f"  Python {result['python_version']}, "
        f"free_threaded={result['free_threaded_build']}, "
        f"gil_enabled={result['gil_enabled']}"
    )
    print(f"  snapshot={result['snapshot']}")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "run":
        result = run_backend(
            backend=args.backend,
            coordinators=args.coordinators,
            rounds=args.rounds,
            batch_size=args.batch_size,
            branch_factor=args.branch_factor,
            query_every=args.query_every,
            initial_domains=args.initial_domains,
            timeout=args.timeout,
        )
        if args.json:
            print(json.dumps(result, sort_keys=True))
        else:
            print_run_result(result)
        return

    if args.command == "compare":
        result = compare_backends(
            regular_python=args.regular_python,
            free_threaded_python=args.free_threaded_python,
            repetitions=args.repetitions,
            coordinator_counts=args.coordinators,
            workload_args=_workload_cli_args(args),
        )
        if args.json:
            print(json.dumps(result, sort_keys=True))
        else:
            print("Steady-state control-plane comparison (median)")
            for comparison in result["comparisons"]:
                print()
                print(f"Coordinators: {comparison['coordinators']}")
                print("Ray on regular CPython:")
                print_run_result(comparison["ray_regular"])
                print("nogil_rpc on regular CPython:")
                print_run_result(comparison["nogil_rpc_regular"])
                print("nogil_rpc on free-threaded CPython:")
                print_run_result(comparison["nogil_rpc_free_threaded"])
                print(
                    "nogil_rpc/GIL throughput / Ray/GIL throughput: "
                    f"{comparison['nogil_rpc_regular_throughput_vs_ray_regular']:.2f}x"
                )
                print(
                    "nogil_rpc/no-GIL throughput / nogil_rpc/GIL throughput: "
                    f"{comparison['nogil_rpc_free_threaded_throughput_vs_regular']:.2f}x"
                )
                print(
                    "nogil_rpc/no-GIL throughput / Ray/GIL throughput: "
                    f"{comparison['nogil_rpc_free_threaded_throughput_vs_ray_regular']:.2f}x"
                )
        return

    parser.error(f"unknown command {args.command!r}")


if __name__ == "__main__":
    main()
