"""
run_experiments.py
==================
Main experiment orchestration module for the latency-aware inference system.

This module:
1. Defines all experimental configurations
2. Executes load tests with specific parameters
3. Records results to CSV
4. Generates plots
5. Provides command-line interface

Architecture:
- Each experiment is a list of configurations
- Each configuration specifies: request_rate, batch_size, scheduler, failure_rate, etc.
- Results are appended to CSV files for easy analysis
- Plots are generated from CSV data

Supported Experiments:
- load: Request rate vs P99 latency
- batch: Batch size trade-off analysis
- scheduler: Scheduler strategy comparison
- failure: Failure injection analysis
- all: Run all experiments
"""

import argparse
import csv
import logging
import os
import sys
import time
from datetime import datetime
from typing import List, Dict, Any
import json
import requests

# Paths are always relative to this file, regardless of working directory.
_HERE = os.path.dirname(os.path.abspath(__file__))

# Ensure experiments/ is on the path so local imports work from any cwd
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from load_test import LoadTester
from metrics import ExperimentMetrics, RequestLog
from plot_results import (
    plot_load_vs_latency,
    plot_batch_size_vs_throughput,
    plot_batch_size_vs_p99,
    plot_scheduler_comparison,
    plot_failure_rate_vs_p99,
    plot_failure_rate_vs_throughput,
    plot_latency_sweep,
    plot_bursty_comparison,
    plot_fault_tolerance,
    plot_dynamic_scaling,
    ensure_dir_exists,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
)
logger = logging.getLogger(__name__)


class ExperimentConfig:
    """
    Represents a single experiment configuration.

    Attributes:
        name: Experiment name
        request_rate: Requests per second
        batch_size: Model batch size
        scheduler: Scheduler strategy name
        failure_rate: Injected failure rate (0.0 - 1.0)
        duration: Test duration in seconds
        num_threads: Number of concurrent threads
    """

    def __init__(
        self,
        name: str,
        request_rate: float,
        batch_size: int = 1,
        scheduler: str = "fifo",
        failure_rate: float = 0.0,
        duration: int = 10,
        num_threads: int = 16,
    ):
        self.name = name
        self.request_rate = request_rate
        self.batch_size = batch_size
        self.scheduler = scheduler
        self.failure_rate = failure_rate
        self.duration = duration
        self.num_threads = num_threads

    def __repr__(self):
        return (
            f"ExperimentConfig("
            f"rate={self.request_rate}, "
            f"batch={self.batch_size}, "
            f"sched={self.scheduler}, "
            f"failure={self.failure_rate})"
        )


class ExperimentRunner:
    """
    Orchestrates experiment execution, result collection, and analysis.

    This class manages:
    1. Configuration of multiple experiment scenarios
    2. Execution of load tests
    3. Collection of metrics
    4. CSV result writing
    5. Plot generation

    Engineering Notes:
    - Separates configuration from execution
    - Results are persisted immediately to CSV (no in-memory buffering)
    - Tolerates individual run failures without aborting
    - Generates plots from finalized CSV data
    """

    def __init__(
        self,
        api_url: str = "http://localhost:8000/predict",
        results_dir: str = None,
        plots_dir: str = None,
    ):
        """
        Initialize experiment runner.

        Args:
            api_url: Target API server URL
            results_dir: Directory for CSV outputs
            plots_dir: Directory for PNG plots
        """
        self.api_url = api_url
        self.results_dir = results_dir or os.path.join(_HERE, "results")
        self.plots_dir = plots_dir or os.path.join(_HERE, "plots")

        # Create directories if missing
        ensure_dir_exists(self.results_dir)
        ensure_dir_exists(self.plots_dir)

        logger.info(f"Experiment runner initialized")
        logger.info(f"API URL: {self.api_url}")
        logger.info(f"Results dir: {self.results_dir}")
        logger.info(f"Plots dir: {self.plots_dir}")

    def _apply_configuration_to_server(self, config: ExperimentConfig):
        """
        Apply experiment configuration to API server.

        Calls API server configuration endpoints:
        - POST /config/batch_size?size=8
        - POST /config/scheduler?strategy=latency_aware
        - POST /config/failure_rate?rate=0.1

        Args:
            config: Experiment configuration to apply
        """
        base_url = self.api_url.rsplit("/predict", 1)[0]
        endpoints = (
            ("batch_size", {"batch_size": config.batch_size}),
            ("scheduler", {"scheduler": config.scheduler}),
            ("failure_rate", {"failure_rate": config.failure_rate}),
        )

        try:
            for endpoint, payload in endpoints:
                response = requests.post(
                    f"{base_url}/config/{endpoint}",
                    json=payload,
                    timeout=5,
                )
                response.raise_for_status()
            time.sleep(0.2)
            logger.debug(f"Applied configuration to server: {config}")
        except requests.RequestException as e:
            logger.warning(f"Could not apply config to server: {e}")

    def run_single_experiment(
        self,
        config: ExperimentConfig,
        verbose: bool = False,
    ) -> Dict[str, Any]:
        """
        Run a single experiment with given configuration.

        Args:
            config: Experiment configuration
            verbose: If True, show progress during test

        Returns:
            Dictionary with metrics results
        """
        logger.info(f"Running: {config}")

        # Apply configuration to server
        self._apply_configuration_to_server(config)

        # Run load test
        tester = LoadTester(
            url=self.api_url,
            request_rate=config.request_rate,
            duration=config.duration,
            num_threads=config.num_threads,
        )

        try:
            if verbose:
                logs = tester.run_with_progress()
            else:
                logs = tester.run()
        except Exception as e:
            logger.error(f"Load test failed: {e}")
            raise

        # Calculate metrics
        metrics_calc = ExperimentMetrics(logs, config.name)
        metrics = metrics_calc.get_metrics()

        # Add configuration info to results
        result = {
            "experiment_name": config.name,
            "setting_name": f"rate_{config.request_rate}_batch_{config.batch_size}_sched_{config.scheduler}",
            "request_rate": config.request_rate,
            "batch_size": config.batch_size,
            "scheduler": config.scheduler,
            "failure_rate": config.failure_rate,
            "config_failure_rate": config.failure_rate,  # For plotting
            "total_requests": metrics["total_requests"],
            "completed_requests": metrics["completed_requests"],
            "failed_requests": metrics["failed_requests"],
            "avg_latency": metrics["avg_latency"],
            "p50_latency": metrics["p50_latency"],
            "p95_latency": metrics["p95_latency"],
            "p99_latency": metrics["p99_latency"],
            "throughput": metrics["throughput"],
            "failure_rate_actual": metrics["failure_rate"],
            "timestamp": metrics["timestamp"],
        }

        logger.info(
            f"Results: "
            f"total={result['total_requests']} "
            f"completed={result['completed_requests']} "
            f"failed={result['failed_requests']} "
            f"throughput={result['throughput']:.2f} req/s "
            f"P99={result['p99_latency']:.2f}ms"
        )

        return result

    def run_experiments_batch(
        self,
        configs: List[ExperimentConfig],
        csv_file: str,
        verbose: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Run multiple experiments and save results to CSV.

        Args:
            configs: List of experiment configurations
            csv_file: Path to CSV file for results
            verbose: Show progress during tests

        Returns:
            List of result dictionaries
        """
        csv_path = os.path.join(self.results_dir, csv_file)
        results = []

        # Always start fresh — stale rows corrupt plots
        if os.path.exists(csv_path):
            os.remove(csv_path)

        file_exists = False

        # Prepare CSV field names (from first result or predefined)
        fieldnames = [
            "experiment_name",
            "setting_name",
            "request_rate",
            "batch_size",
            "scheduler",
            "failure_rate",
            "config_failure_rate",
            "total_requests",
            "completed_requests",
            "failed_requests",
            "avg_latency",
            "p50_latency",
            "p95_latency",
            "p99_latency",
            "throughput",
            "failure_rate_actual",
            "timestamp",
        ]

        # Run experiments
        for i, config in enumerate(configs):
            try:
                result = self.run_single_experiment(config, verbose)
                results.append(result)

                # Write to CSV immediately (append mode)
                with open(csv_path, "a", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    if not file_exists or i == 0:
                        writer.writeheader()
                        file_exists = True
                    writer.writerow(result)

                logger.info(f"Result saved to {csv_path}")

            except Exception as e:
                logger.error(f"Experiment failed: {config} - {str(e)}")
                continue

        return results

    # =========================================================================
    # Experiment Definitions
    # =========================================================================

    def experiment_load_vs_latency(
        self,
        request_rates: List[int] = None,
        batch_size: int = 4,
        scheduler: str = "fifo",
        duration: int = 20,
        num_threads: int = 64,
    ):
        """
        Experiment 1: Load vs Latency

        Purpose: Understand how request rate affects tail latency (P99).

        Configuration:
        - Vary request rates: [10, 50, 100, 200] requests/sec (default)
        - Fix scheduler, batch size, worker count
        - Run each for 10 seconds
        - Record P99 latency, throughput, failure rate

        Research Question:
        - At what request rate does performance degrade?
        - Is degradation linear or super-linear?

        Args:
            request_rates: List of rates to test
            batch_size: Fixed batch size
            scheduler: Fixed scheduler strategy
            duration: Test duration per rate
            num_threads: Number of worker threads
        """
        if request_rates is None:
            request_rates = [50, 100, 200, 400, 800]

        logger.info("=" * 70)
        logger.info("EXPERIMENT 1: Load vs Latency")
        logger.info("=" * 70)

        configs = [
            ExperimentConfig(
                name="load_vs_latency",
                request_rate=rate,
                batch_size=batch_size,
                scheduler=scheduler,
                failure_rate=0.0,
                duration=duration,
                num_threads=num_threads,
            )
            for rate in request_rates
        ]

        results = self.run_experiments_batch(
            configs,
            "load_vs_latency.csv",
            verbose=False,
        )

        # Generate plot
        if results:
            plot_load_vs_latency(results, os.path.join(self.plots_dir, "load_vs_latency.png"))

        return results

    def experiment_batch_size_tradeoff(
        self,
        request_rate: int = 200,
        batch_sizes: List[int] = None,
        scheduler: str = "batching",
        duration: int = 20,
        num_threads: int = 16,
    ):
        """
        Experiment 2: Batch Size Trade-off

        Purpose: Understand trade-off between batching and latency/throughput.

        Configuration:
        - Fix request rate: 100 req/s (default)
        - Vary batch sizes: [1, 4, 8, 16]
        - Record throughput and P99 latency

        Research Question:
        - Does larger batching improve throughput?
        - At what point does batching hurt tail latency?

        Args:
            request_rate: Fixed request rate
            batch_sizes: List of batch sizes to test
            scheduler: Scheduler strategy
            duration: Test duration per batch size
            num_threads: Number of worker threads
        """
        if batch_sizes is None:
            batch_sizes = [1, 4, 8, 16]

        logger.info("=" * 70)
        logger.info("EXPERIMENT 2: Batch Size Trade-off")
        logger.info("=" * 70)

        configs = [
            ExperimentConfig(
                name="batch_size_tradeoff",
                request_rate=request_rate,
                batch_size=batch,
                scheduler=scheduler,
                failure_rate=0.0,
                duration=duration,
                num_threads=num_threads,
            )
            for batch in batch_sizes
        ]

        results = self.run_experiments_batch(
            configs,
            "batch_size_tradeoff.csv",
            verbose=False,
        )

        # Generate plots
        if results:
            plot_batch_size_vs_throughput(
                results, os.path.join(self.plots_dir, "batch_size_vs_throughput.png")
            )
            plot_batch_size_vs_p99(results, os.path.join(self.plots_dir, "batch_size_vs_p99.png"))

        return results

    def experiment_scheduler_comparison(
        self,
        request_rate: int = 150,
        batch_size: int = 8,
        schedulers: List[str] = None,
        duration: int = 20,
        num_threads: int = 16,
    ):
        """
        Experiment 3: Scheduler Comparison

        Purpose: Compare different scheduler strategies under same load.

        Configuration:
        - Test schedulers: ["fifo", "batching", "latency_aware"]
        - Fix request rate, batch size, worker count
        - Run each for 10 seconds
        - Record P99 latency, throughput

        Research Question:
        - Which scheduler achieves lowest P99 latency?
        - What is the throughput trade-off?

        Args:
            request_rate: Fixed request rate
            batch_size: Fixed batch size
            schedulers: List of scheduler strategies
            duration: Test duration per scheduler
            num_threads: Number of worker threads
        """
        if schedulers is None:
            schedulers = ["fifo", "batching", "latency_aware"]

        logger.info("=" * 70)
        logger.info("EXPERIMENT 3: Scheduler Comparison")
        logger.info("=" * 70)

        configs = [
            ExperimentConfig(
                name="scheduler_comparison",
                request_rate=request_rate,
                batch_size=batch_size,
                scheduler=sched,
                failure_rate=0.0,
                duration=duration,
                num_threads=num_threads,
            )
            for sched in schedulers
        ]

        results = self.run_experiments_batch(
            configs,
            "scheduler_comparison.csv",
            verbose=False,
        )

        # Generate plot
        if results:
            plot_scheduler_comparison(
                results, os.path.join(self.plots_dir, "scheduler_comparison_p99.png")
            )

        return results

    def experiment_failure_injection(
        self,
        request_rate: int = 100,
        batch_size: int = 4,
        scheduler: str = "latency_aware",
        failure_rates: List[float] = None,
        duration: int = 10,
        num_threads: int = 4,
    ):
        """
        Experiment 4: Failure Injection

        Purpose: Understand system resilience under component failures.

        Configuration:
        - Inject failures: [0.0, 0.1, 0.2] failure rate (default)
        - Fix request rate, batch size, scheduler
        - Run each for 10 seconds
        - Record P99 latency, throughput, actual failure rate

        Research Question:
        - How does failure rate impact P99 latency?
        - Can retries mask failures?
        - What is the effective throughput degradation?

        TODO: This experiment requires API server support for:
        - POST /config/failure_rate?rate=0.1
        - Workers must inject random failures at specified rate

        Args:
            request_rate: Fixed request rate
            batch_size: Fixed batch size
            scheduler: Fixed scheduler strategy
            failure_rates: List of failure rates to test
            duration: Test duration per failure rate
            num_threads: Number of worker threads
        """
        if failure_rates is None:
            failure_rates = [0.0, 0.1, 0.2]

        logger.info("=" * 70)
        logger.info("EXPERIMENT 4: Failure Injection")
        logger.info("=" * 70)
        logger.info("WARNING: This experiment requires API server to support failure injection.")
        logger.info("TODO: Ensure your workers implement random failure injection.")
        logger.info("")

        configs = [
            ExperimentConfig(
                name="failure_injection",
                request_rate=request_rate,
                batch_size=batch_size,
                scheduler=scheduler,
                failure_rate=failure,
                duration=duration,
                num_threads=num_threads,
            )
            for failure in failure_rates
        ]

        results = self.run_experiments_batch(
            configs,
            "failure_injection.csv",
            verbose=False,
        )

        # Generate plots
        if results:
            plot_failure_rate_vs_p99(
                results, os.path.join(self.plots_dir, "failure_rate_vs_p99.png")
            )
            plot_failure_rate_vs_throughput(
                results, os.path.join(self.plots_dir, "failure_rate_vs_throughput.png")
            )

        return results

    def experiment_fault_tolerance(
        self,
        request_rate: int = 50,
        batch_size: int = 4,
        phase_duration: int = 15,
        num_threads: int = 4,
    ):
        """
        Experiment: Worker Fault Tolerance.

        Phase 1: Run with all workers healthy.
        Action:  Kill worker-2 (port 50052) to simulate failure.
        Phase 2: Run with remaining workers — ZooKeeper auto-detects the loss.
        Demonstrates self-healing via ZooKeeper ephemeral node removal.
        """
        logger.info("=" * 70)
        logger.info("EXPERIMENT: Fault Tolerance (worker failure & recovery)")
        logger.info("=" * 70)

        cfg = ExperimentConfig(
            name="fault_tolerance",
            request_rate=request_rate,
            batch_size=batch_size,
            scheduler="fifo",
            failure_rate=0.0,
            duration=phase_duration,
            num_threads=num_threads,
        )
        self._apply_configuration_to_server(cfg)

        # Phase 1: all workers healthy
        logger.info("Phase 1: all workers running")
        logs1 = LoadTester(
            url=self.api_url,
            request_rate=request_rate,
            duration=phase_duration,
            num_threads=num_threads,
        ).run()
        m1 = ExperimentMetrics(logs1, "fault_tolerance").get_metrics()
        logger.info(
            "Phase 1: P99=%.1fms throughput=%.1f err_rate=%.2f",
            m1["p99_latency"], m1["throughput"], m1["failure_rate"],
        )

        # Kill worker-1 — use -sTCP:LISTEN so we only kill the server process,
        # not the API server's client connection to that port.
        logger.info("Killing worker-1 (port 50052)...")
        import subprocess
        result = subprocess.run(
            ["lsof", "-ti", "TCP:50052", "-sTCP:LISTEN"], capture_output=True, text=True
        )
        pids = result.stdout.strip().split()
        for pid in pids:
            subprocess.run(["kill", "-9", pid])
        logger.info("Worker-1 killed — waiting 4s for ZooKeeper to detect...")
        time.sleep(4)

        # Phase 2: degraded (3 workers)
        logger.info("Phase 2: running with remaining workers")
        logs2 = LoadTester(
            url=self.api_url,
            request_rate=request_rate,
            duration=phase_duration,
            num_threads=num_threads,
        ).run()
        m2 = ExperimentMetrics(logs2, "fault_tolerance").get_metrics()
        logger.info(
            "Phase 2: P99=%.1fms throughput=%.1f err_rate=%.2f",
            m2["p99_latency"], m2["throughput"], m2["failure_rate"],
        )

        results = [
            {
                "phase": "Before Failure",
                "num_workers": 4,
                "p99_latency": m1["p99_latency"],
                "throughput": m1["throughput"],
                "failure_rate_actual": m1["failure_rate"],
            },
            {
                "phase": "After Failure",
                "num_workers": 3,
                "p99_latency": m2["p99_latency"],
                "throughput": m2["throughput"],
                "failure_rate_actual": m2["failure_rate"],
            },
        ]

        csv_path = os.path.join(self.results_dir, "fault_tolerance.csv")
        if os.path.exists(csv_path):
            os.remove(csv_path)
        fieldnames = ["phase", "num_workers", "p99_latency", "throughput", "failure_rate_actual"]
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)

        plot_fault_tolerance(results, os.path.join(self.plots_dir, "fault_tolerance.png"))
        return results

    def experiment_dynamic_scaling(
        self,
        request_rate: int = 100,
        batch_size: int = 4,
        phase_duration: int = 15,
        num_threads: int = 8,
    ):
        """
        Experiment: Dynamic Horizontal Scaling.

        Starts with 2 workers (system near/over capacity), then launches
        2 more workers mid-experiment. ZooKeeper notifies the pool and
        throughput increases without restarting the API server.
        """
        logger.info("=" * 70)
        logger.info("EXPERIMENT: Dynamic Scaling (2 → 4 workers)")
        logger.info("=" * 70)

        import subprocess

        cfg = ExperimentConfig(
            name="dynamic_scaling",
            request_rate=request_rate,
            batch_size=batch_size,
            scheduler="fifo",
            failure_rate=0.0,
            duration=phase_duration,
            num_threads=num_threads,
        )
        self._apply_configuration_to_server(cfg)

        time.sleep(1)  # ensure clean state

        # Phase 1: 2 workers
        logger.info("Phase 1: running with 2 workers at %d req/s", request_rate)
        logs1 = LoadTester(
            url=self.api_url,
            request_rate=request_rate,
            duration=phase_duration,
            num_threads=num_threads,
        ).run()
        m1 = ExperimentMetrics(logs1, "dynamic_scaling").get_metrics()
        logger.info("Phase 1: P99=%.1fms throughput=%.1f", m1["p99_latency"], m1["throughput"])

        # Start 1 more worker (2 → 3)
        logger.info("Starting worker-2 (port 50053)...")
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        subprocess.Popen(
            ["python", "-m", "system.grpc_worker_server", "--worker-id", "2", "--port", "50053"],
            cwd=root,
        )
        logger.info("Waiting 4s for worker to register with ZooKeeper...")
        time.sleep(4)

        # Phase 2: 4 workers
        logger.info("Phase 2: running with 4 workers at %d req/s", request_rate)
        logs2 = LoadTester(
            url=self.api_url,
            request_rate=request_rate,
            duration=phase_duration,
            num_threads=num_threads,
        ).run()
        m2 = ExperimentMetrics(logs2, "dynamic_scaling").get_metrics()
        logger.info("Phase 2: P99=%.1fms throughput=%.1f", m2["p99_latency"], m2["throughput"])

        results = [
            {"num_workers": 2, "p99_latency": m1["p99_latency"], "throughput": m1["throughput"]},
            {"num_workers": 3, "p99_latency": m2["p99_latency"], "throughput": m2["throughput"]},
        ]

        csv_path = os.path.join(self.results_dir, "dynamic_scaling.csv")
        if os.path.exists(csv_path):
            os.remove(csv_path)
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["num_workers", "p99_latency", "throughput"])
            writer.writeheader()
            writer.writerows(results)

        plot_dynamic_scaling(results, os.path.join(self.plots_dir, "dynamic_scaling.png"))
        return results

    def experiment_bursty(
        self,
        normal_rate: int = 50,
        burst_rate: int = 300,
        normal_duration: int = 5,
        burst_duration: int = 2,
        cycles: int = 3,
        batch_size: int = 4,
        num_threads: int = 16,
    ):
        """
        Experiment: Bursty traffic comparison across schedulers.

        Alternates between normal load and burst load to simulate real-world
        traffic spikes. Compares P99 latency for fifo, batching, latency_aware.

        Pattern per cycle: normal_duration seconds at normal_rate,
                           then burst_duration seconds at burst_rate.
        """
        schedulers = ["fifo", "batching", "latency_aware"]
        csv_path = os.path.join(self.results_dir, "bursty.csv")
        if os.path.exists(csv_path):
            os.remove(csv_path)

        fieldnames = [
            "experiment_name", "scheduler", "normal_rate", "burst_rate",
            "total_requests", "completed_requests", "failed_requests",
            "avg_latency", "p50_latency", "p95_latency", "p99_latency",
            "throughput", "timestamp",
        ]

        logger.info("=" * 70)
        logger.info("EXPERIMENT: Bursty Traffic (all schedulers)")
        logger.info(f"Pattern: {normal_duration}s @ {normal_rate} req/s → {burst_duration}s @ {burst_rate} req/s × {cycles} cycles")
        logger.info("=" * 70)

        results = []
        for scheduler in schedulers:
            logger.info(f"--- Scheduler: {scheduler} ---")

            # Apply config
            cfg = ExperimentConfig(
                name="bursty",
                request_rate=normal_rate,
                batch_size=batch_size,
                scheduler=scheduler,
                failure_rate=0.0,
                duration=normal_duration,
                num_threads=num_threads,
            )
            self._apply_configuration_to_server(cfg)

            all_logs = []
            for cycle in range(cycles):
                logger.info(f"  Cycle {cycle + 1}/{cycles}: normal phase")
                normal_tester = LoadTester(
                    url=self.api_url,
                    request_rate=normal_rate,
                    duration=normal_duration,
                    num_threads=num_threads,
                )
                all_logs.extend(normal_tester.run())

                logger.info(f"  Cycle {cycle + 1}/{cycles}: burst phase")
                burst_tester = LoadTester(
                    url=self.api_url,
                    request_rate=burst_rate,
                    duration=burst_duration,
                    num_threads=num_threads,
                )
                all_logs.extend(burst_tester.run())

            metrics = ExperimentMetrics(all_logs, "bursty").get_metrics()
            result = {
                "experiment_name": "bursty",
                "scheduler": scheduler,
                "normal_rate": normal_rate,
                "burst_rate": burst_rate,
                "total_requests": metrics["total_requests"],
                "completed_requests": metrics["completed_requests"],
                "failed_requests": metrics["failed_requests"],
                "avg_latency": metrics["avg_latency"],
                "p50_latency": metrics["p50_latency"],
                "p95_latency": metrics["p95_latency"],
                "p99_latency": metrics["p99_latency"],
                "throughput": metrics["throughput"],
                "timestamp": metrics["timestamp"],
            }
            results.append(result)

            logger.info(
                f"  {scheduler}: total={result['total_requests']} "
                f"completed={result['completed_requests']} "
                f"failed={result['failed_requests']} "
                f"throughput={result['throughput']:.2f} req/s "
                f"P99={result['p99_latency']:.2f}ms"
            )

            with open(csv_path, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if scheduler == schedulers[0]:
                    writer.writeheader()
                writer.writerow(result)

        if results:
            plot_bursty_comparison(
                results,
                os.path.join(self.plots_dir, "bursty_comparison.png"),
            )

        return results

    def experiment_latency_sweep(
        self,
        request_rates: List[int] = None,
        batch_size: int = 4,
        duration: int = 20,
        num_threads: int = 16,
    ):
        """
        Experiment: Request Rate Sweep across all schedulers.

        Sweeps request_rate over a range and compares P99 latency for
        fifo, batching, and latency_aware side by side.
        """
        if request_rates is None:
            request_rates = [100, 150, 200, 250, 300]

        schedulers = ["fifo", "batching", "latency_aware"]

        logger.info("=" * 70)
        logger.info("EXPERIMENT: Latency Sweep (all schedulers)")
        logger.info("=" * 70)

        configs = [
            ExperimentConfig(
                name="latency_sweep",
                request_rate=rate,
                batch_size=batch_size,
                scheduler=sched,
                failure_rate=0.0,
                duration=duration,
                num_threads=num_threads,
            )
            for sched in schedulers
            for rate in request_rates
        ]

        results = self.run_experiments_batch(configs, "latency_sweep.csv")

        if results:
            plot_latency_sweep(
                results,
                os.path.join(self.plots_dir, "latency_sweep.png"),
            )

        return results

    def run_all_experiments(self):
        """Run all experiments in sequence."""
        logger.info("=" * 70)
        logger.info("RUNNING ALL EXPERIMENTS")
        logger.info("=" * 70)

        self.experiment_load_vs_latency()
        self.experiment_batch_size_tradeoff()
        self.experiment_scheduler_comparison()
        self.experiment_failure_injection()
        self.experiment_latency_sweep()
        self.experiment_bursty()

        self._print_summary()

    def _print_summary(self):
        """Print summary of all results."""
        logger.info("")
        logger.info("=" * 70)
        logger.info("EXPERIMENT SUMMARY")
        logger.info("=" * 70)

        # List all results files
        logger.info("Results files:")
        for filename in sorted(os.listdir(self.results_dir)):
            if filename.endswith(".csv"):
                filepath = os.path.join(self.results_dir, filename)
                size = os.path.getsize(filepath)
                logger.info(f"  - {filename} ({size} bytes)")

        logger.info("")
        logger.info("Plot files:")
        for filename in sorted(os.listdir(self.plots_dir)):
            if filename.endswith(".png"):
                filepath = os.path.join(self.plots_dir, filename)
                size = os.path.getsize(filepath)
                logger.info(f"  - {filename} ({size} bytes)")

        logger.info("")
        logger.info(f"Results directory: {os.path.abspath(self.results_dir)}")
        logger.info(f"Plots directory: {os.path.abspath(self.plots_dir)}")


def main():
    """Command-line interface for running experiments."""
    parser = argparse.ArgumentParser(
        description="Run latency-aware inference system experiments",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python experiments/run_experiments.py --experiment load
  python experiments/run_experiments.py --experiment batch
  python experiments/run_experiments.py --experiment scheduler
  python experiments/run_experiments.py --experiment failure
  python experiments/run_experiments.py --experiment all

  # Custom parameters
  python experiments/run_experiments.py --experiment load \\
    --url http://localhost:9000/predict \\
    --duration 20 \\
    --threads 8

  # Batch size experiment with custom values
  python experiments/run_experiments.py --experiment batch \\
    --request-rates 50 100 150 \\
    --batch-sizes 1 2 4 8 16
        """,
    )

    parser.add_argument(
        "--experiment",
        choices=["load", "batch", "scheduler", "failure", "sweep", "bursty",
                 "fault", "scaling", "all"],
        default="load",
        help="Experiment to run (default: load)",
    )

    parser.add_argument(
        "--url",
        default="http://localhost:8000/predict",
        help="API server URL (default: http://localhost:8000/predict)",
    )

    parser.add_argument(
        "--duration",
        type=int,
        default=10,
        help="Test duration per configuration in seconds (default: 10)",
    )

    parser.add_argument(
        "--threads",
        type=int,
        default=4,
        help="Number of concurrent client threads (default: 4)",
    )

    parser.add_argument(
        "--request-rates",
        type=int,
        nargs="+",
        help="Request rates for load experiment (default: 10 50 100 200)",
    )

    parser.add_argument(
        "--batch-sizes",
        type=int,
        nargs="+",
        help="Batch sizes for batch experiment (default: 1 4 8 16)",
    )

    parser.add_argument(
        "--schedulers",
        nargs="+",
        help="Schedulers for comparison (default: fifo batching latency_aware)",
    )

    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory for CSV files (default: experiments/results/)",
    )

    parser.add_argument(
        "--plot-dir",
        default=None,
        help="Output directory for plots (default: experiments/plots/)",
    )

    args = parser.parse_args()

    # Create runner
    runner = ExperimentRunner(
        api_url=args.url,
        results_dir=args.output_dir,
        plots_dir=args.plot_dir,
    )

    # Run requested experiment
    try:
        if args.experiment == "load":
            runner.experiment_load_vs_latency(
                request_rates=args.request_rates,
                duration=args.duration,
                num_threads=args.threads,
            )

        elif args.experiment == "batch":
            runner.experiment_batch_size_tradeoff(
                batch_sizes=args.batch_sizes,
                duration=args.duration,
                num_threads=args.threads,
            )

        elif args.experiment == "scheduler":
            runner.experiment_scheduler_comparison(
                schedulers=args.schedulers,
                duration=args.duration,
                num_threads=args.threads,
            )

        elif args.experiment == "failure":
            runner.experiment_failure_injection(
                duration=args.duration,
                num_threads=args.threads,
            )

        elif args.experiment == "bursty":
            runner.experiment_bursty(
                num_threads=args.threads,
            )

        elif args.experiment == "fault":
            runner.experiment_fault_tolerance(
                num_threads=args.threads,
            )

        elif args.experiment == "scaling":
            runner.experiment_dynamic_scaling(
                num_threads=args.threads,
            )

        elif args.experiment == "sweep":
            runner.experiment_latency_sweep(
                request_rates=args.request_rates,
                duration=args.duration,
                num_threads=args.threads,
            )

        elif args.experiment == "all":
            runner.run_all_experiments()

        runner._print_summary()

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Experiment failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
