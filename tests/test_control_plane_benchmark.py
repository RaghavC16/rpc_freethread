"""Correctness tests for the backend-neutral control-plane workload."""

from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from benchmarks.control_plane_compare import (
    FrontierCore,
    branch_domains,
    compare_backends,
    validate_snapshot,
)


class ControlPlaneBenchmarkTests(unittest.TestCase):
    def test_frontier_claim_submit_and_snapshot(self) -> None:
        frontier = FrontierCore(initial_domains=8)

        claimed = frontier.claim_batch(2)
        children, checksum = branch_domains(
            claimed,
            coordinator_id=0,
            round_index=0,
            branch_factor=2,
        )
        size = frontier.submit_children(children)

        self.assertEqual(len(claimed), 2)
        self.assertEqual(len(children), 4)
        self.assertNotEqual(checksum, 0)
        self.assertEqual(size, 10)
        self.assertEqual(
            frontier.snapshot(),
            {
                "frontier_size": 10,
                "claimed": 2,
                "added": 4,
                "claim_calls": 1,
                "submit_calls": 1,
                "size_calls": 0,
            },
        )

    def test_branching_is_deterministic(self) -> None:
        domains = [(10, 2, 99), (20, 3, 101)]
        first = branch_domains(
            domains,
            coordinator_id=4,
            round_index=7,
            branch_factor=2,
        )
        second = branch_domains(
            domains,
            coordinator_id=4,
            round_index=7,
            branch_factor=2,
        )

        self.assertEqual(first, second)

    def test_snapshot_validation_reconciles_counts(self) -> None:
        snapshot = {
            "frontier_size": 28,
            "claimed": 20,
            "added": 40,
            "claim_calls": 10,
            "submit_calls": 10,
            "size_calls": 4,
        }
        validate_snapshot(
            snapshot,
            initial_domains=8,
            coordinators=2,
            rounds=5,
            batch_size=2,
            branch_factor=2,
            query_every=2,
        )

        invalid = dict(snapshot, claimed=19)
        with self.assertRaises(RuntimeError):
            validate_snapshot(
                invalid,
                initial_domains=8,
                coordinators=2,
                rounds=5,
                batch_size=2,
                branch_factor=2,
                query_every=2,
            )

    def test_compare_runs_three_interpreter_framework_modes(self) -> None:
        def fake_child(command: list[str], args: list[str]) -> dict[str, object]:
            backend = args[args.index("--backend") + 1]
            free_threaded = "-X" in command
            rate = 10.0 if backend == "ray" else (30.0 if free_threaded else 20.0)
            return {
                "backend": backend,
                "startup_seconds": 1.0,
                "actor_create_seconds": 1.0,
                "workload_seconds": 1.0,
                "control_calls_per_second": rate,
                "amortized_call_time_ms": 1.0,
                "total_seconds": 1.0,
            }

        with patch(
            "benchmarks.control_plane_compare._run_child",
            side_effect=fake_child,
        ) as run_child:
            result = compare_backends(
                regular_python=Path("python-regular"),
                free_threaded_python=Path("python-free-threaded"),
                repetitions=1,
                coordinator_counts=[2],
                workload_args=["--rounds", "5"],
            )

        comparison = result["comparisons"][0]
        self.assertEqual(run_child.call_count, 3)
        self.assertEqual(
            comparison["nogil_rpc_regular_throughput_vs_ray_regular"],
            2.0,
        )
        self.assertEqual(
            comparison["nogil_rpc_free_threaded_throughput_vs_regular"],
            1.5,
        )
        self.assertEqual(
            comparison["nogil_rpc_free_threaded_throughput_vs_ray_regular"],
            3.0,
        )


if __name__ == "__main__":
    unittest.main()
