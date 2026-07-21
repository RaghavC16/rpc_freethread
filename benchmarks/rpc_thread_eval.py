"""Benchmark RPC worker throughput with the GIL enabled and disabled.

This benchmark intentionally uses pure-Python CPU-bound work exposed as either a
remote function or a remote actor method. That is the useful test for
free-threading: on a GIL build, CPU-bound Python threads should mostly serialize;
on a free-threaded build, worker threads can execute Python bytecode in parallel.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import subprocess
import sys
import sysconfig
import time
from pathlib import Path
from statistics import median
from typing import Any

from nogil_rpc import RpcRuntime, connect, remote


@remote
def cpu_bound_task(iterations: int, seed: int) -> int:
    """Run deterministic CPU-bound Python bytecode."""

    return _cpu_bound_work(iterations, seed)


@remote
class CpuBoundActor:
    """Expose the benchmark workload through a remote member function."""

    def cpu_bound_task(self, iterations: int, seed: int) -> int:
        return _cpu_bound_work(iterations, seed)


def _cpu_bound_work(iterations: int, seed: int) -> int:
    """Run the same workload for function and actor benchmark modes."""

    # value = seed | 1
    # for index in range(iterations):
    #     value = ((value * 1_103_515_245) + 12_345 + index) & 0x7FFFFFFF
    # return value

    rng = random.Random(seed)
    large_list = list(range(1_000_000))
    ans = []
    for _ in range(iterations):
        ans.append(rng.choice(large_list))

    return len(ans)


def evaluate(
    *,
    workers: int,
    tasks: int,
    iterations: int,
    timeout: float,
    target: str = "function",
    actors: int | None = None,
) -> dict[str, Any]:
    if target not in {"function", "actor"}:
        raise ValueError("target must be 'function' or 'actor'")
    actor_count = workers if actors is None else actors
    if target == "actor" and actor_count < 1:
        raise ValueError("actors must be at least 1 for the actor target")

    runtime = RpcRuntime(host="127.0.0.1", port=0, max_workers=workers)
    runtime.start()

    host, port = runtime.address
    worker = connect(f"{host}:{port}", timeout=timeout)
    actor_handles = []

    try:
        if target == "actor":
            actor_handles = [worker.CpuBoundActor.remote() for _ in range(actor_count)]

        started_wall = time.perf_counter()
        started_cpu = time.process_time()

        if target == "actor":
            refs = [
                actor_handles[task_id % actor_count].cpu_bound_task.remote(
                    iterations, task_id
                )
                for task_id in range(tasks)
            ]
        else:
            refs = [
                worker.cpu_bound_task.remote(iterations, task_id)
                for task_id in range(tasks)
            ]

        results = [ref.get(timeout=timeout) for ref in refs]

        elapsed_wall = time.perf_counter() - started_wall
        elapsed_cpu = time.process_time() - started_cpu
    finally:
        for actor_handle in actor_handles:
            actor_handle.close()
        worker.close()
        runtime.stop()

    checksum = 0
    for result in results:
        checksum ^= result

    return {
        "executable": sys.executable,
        "version": sys.version.split()[0],
        "free_threaded_build": bool(sysconfig.get_config_var("Py_GIL_DISABLED")),
        "gil_enabled": _is_gil_enabled(),
        "target": target,
        "actors": actor_count if target == "actor" else 0,
        "workers": workers,
        "tasks": tasks,
        "iterations_per_task": iterations,
        "total_iterations": tasks * iterations,
        "wall_seconds": elapsed_wall,
        "process_cpu_seconds": elapsed_cpu,
        "cpu_parallelism_ratio": elapsed_cpu / elapsed_wall if elapsed_wall else 0.0,
        "tasks_per_second": tasks / elapsed_wall if elapsed_wall else 0.0,
        "iterations_per_second": (tasks * iterations) / elapsed_wall
        if elapsed_wall
        else 0.0,
        "checksum": checksum,
    }


def compare(
    *,
    python: Path,
    workers: list[int],
    tasks: int,
    iterations: int,
    timeout: float,
    repetitions: int,
    target: str = "function",
    actors: int | None = None,
) -> dict[str, Any]:
    script = Path(__file__).resolve()
    comparisons = []
    for worker_count in workers:
        common_args = [
            str(script),
            "run",
            "--workers",
            str(worker_count),
            "--tasks",
            str(tasks),
            "--iterations",
            str(iterations),
            "--timeout",
            str(timeout),
            "--target",
            target,
            "--json",
        ]
        if actors is not None:
            common_args.extend(("--actors", str(actors)))

        gil = _run_repeated(
            python,
            common_args,
            gil_enabled=True,
            repetitions=repetitions,
        )
        free_threaded = _run_repeated(
            python,
            common_args,
            gil_enabled=False,
            repetitions=repetitions,
        )
        speedup = (
            free_threaded["iterations_per_second"]
            / gil["iterations_per_second"]
            if gil["iterations_per_second"]
            else 0.0
        )
        comparisons.append(
            {
                "workers": worker_count,
                "gil": gil,
                "free_threaded": free_threaded,
                "free_threaded_speedup": speedup,
            }
        )

    return {
        "python": str(python),
        "repetitions": repetitions,
        "target": target,
        "comparisons": comparisons,
    }


def _run_repeated(
    python: Path,
    args: list[str],
    *,
    gil_enabled: bool,
    repetitions: int,
) -> dict[str, Any]:
    runs = [
        _run_child(python, args, gil_enabled=gil_enabled)
        for _ in range(repetitions)
    ]
    result = runs[0].copy()
    for key in (
        "wall_seconds",
        "process_cpu_seconds",
        "cpu_parallelism_ratio",
        "tasks_per_second",
        "iterations_per_second",
    ):
        result[key] = median(run[key] for run in runs)
    result["repetitions"] = repetitions
    return result


def _run_child(
    python: Path,
    args: list[str],
    *,
    gil_enabled: bool,
) -> dict[str, Any]:
    completed = subprocess.run(
        [str(python), "-X", f"gil={int(gil_enabled)}", *args],
        check=True,
        text=True,
        capture_output=True,
    )
    result = json.loads(completed.stdout)
    if result["gil_enabled"] is not gil_enabled:
        mode = "enabled" if gil_enabled else "disabled"
        raise RuntimeError(f"interpreter did not run with the GIL {mode}")
    return result


def _is_gil_enabled() -> bool:
    checker = getattr(sys, "_is_gil_enabled", None)
    if checker is not None:
        return bool(checker())
    return not bool(sysconfig.get_config_var("Py_GIL_DISABLED"))


def print_report(result: dict[str, Any]) -> None:
    print(f"Python: {result['version']} ({result['executable']})")
    print(f"Free-threaded build: {result['free_threaded_build']}")
    print(f"GIL enabled at runtime: {result['gil_enabled']}")
    target = result["target"]
    if target == "actor":
        print(f"RPC target: actor method ({result['actors']} actors)")
    else:
        print("RPC target: function")
    print(
        "Workload: "
        f"{result['tasks']} tasks x {result['iterations_per_task']} iterations, "
        f"{result['workers']} workers"
    )
    print(f"Wall time: {result['wall_seconds']:.3f}s")
    print(f"Process CPU time: {result['process_cpu_seconds']:.3f}s")
    print(f"CPU parallelism ratio: {result['cpu_parallelism_ratio']:.2f}x")
    print(f"Tasks/sec: {result['tasks_per_second']:.2f}")
    print(f"Iterations/sec: {result['iterations_per_second']:.0f}")
    print(f"Checksum: {result['checksum']}")


def print_compare_report(result: dict[str, Any]) -> None:
    print(
        f"Same-interpreter comparison: {result['python']} "
        f"({result['repetitions']} repetitions; median reported)"
    )
    for comparison in result["comparisons"]:
        print()
        print(f"Workers: {comparison['workers']}")
        print("GIL enabled")
        print_report(comparison["gil"])
        print()
        print("GIL disabled")
        print_report(comparison["free_threaded"])
        print(
            "Free-threaded throughput speedup: "
            f"{comparison['free_threaded_speedup']:.2f}x"
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Measure nogil_rpc CPU-bound throughput with multiple threads."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="run once with this interpreter")
    add_workload_args(run_parser)
    run_parser.add_argument("--json", action="store_true", help="emit JSON only")

    compare_parser = subparsers.add_parser(
        "compare",
        help="run one free-threaded interpreter with its GIL enabled and disabled",
    )
    compare_parser.add_argument(
        "--python",
        type=Path,
        default=Path(".venv-ft/bin/python"),
        help="path to a free-threaded Python executable",
    )
    compare_parser.add_argument(
        "--workers",
        type=int,
        nargs="+",
        default=[1, 2, 4, 6],
        metavar="N",
        help="worker counts to benchmark",
    )
    compare_parser.add_argument("--tasks", type=int, default=24)
    compare_parser.add_argument("--iterations", type=int, default=1_000_000)
    compare_parser.add_argument("--timeout", type=float, default=120.0)
    compare_parser.add_argument("--repetitions", type=int, default=3)
    add_target_args(compare_parser)
    compare_parser.add_argument("--json", action="store_true", help="emit JSON only")

    return parser


def add_workload_args(parser: argparse.ArgumentParser) -> None:
    default_workers = min(8, os.cpu_count() or 2)
    parser.add_argument("--workers", type=int, default=default_workers)
    parser.add_argument("--tasks", type=int, default=default_workers * 4)
    parser.add_argument("--iterations", type=int, default=1_000_000)
    parser.add_argument("--timeout", type=float, default=120.0)
    add_target_args(parser)


def add_target_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--target",
        choices=("function", "actor"),
        default="function",
        help="benchmark a remote function or a remote actor member function",
    )
    parser.add_argument(
        "--actors",
        type=int,
        default=None,
        help="actor instances to use in actor mode (default: worker count)",
    )


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "run":
        result = evaluate(
            workers=args.workers,
            tasks=args.tasks,
            iterations=args.iterations,
            timeout=args.timeout,
            target=args.target,
            actors=args.actors,
        )
        if args.json:
            print(json.dumps(result, sort_keys=True))
        else:
            print_report(result)
        return

    if args.command == "compare":
        result = compare(
            python=args.python,
            workers=args.workers,
            tasks=args.tasks,
            iterations=args.iterations,
            timeout=args.timeout,
            repetitions=args.repetitions,
            target=args.target,
            actors=args.actors,
        )
        if args.json:
            print(json.dumps(result, sort_keys=True))
        else:
            print_compare_report(result)
        return

    parser.error(f"unknown command {args.command!r}")


if __name__ == "__main__":
    main()
