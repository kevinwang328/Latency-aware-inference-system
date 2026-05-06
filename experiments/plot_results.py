"""
plot_results.py
===============
Plotting utilities for experiment result visualization.

This module provides functions to generate publication-quality plots
from experiment results using matplotlib.

Design:
- One function per plot type
- PNG output only (no GUI display)
- Clear labels and formatting
- Consistent style across plots

Features:
- Load vs Latency
- Batch Size Trade-off (throughput & P99)
- Scheduler Comparison
- Failure Injection Analysis
"""

import matplotlib.pyplot as plt
import os
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)


def setup_plot_style():
    """Configure matplotlib for consistent plotting style."""
    plt.style.use("seaborn-v0_8-darkgrid" if "seaborn-v0_8-darkgrid" in plt.style.available else "default")
    plt.rcParams.update({
        "figure.figsize": (10, 6),
        "font.size": 12,
        "axes.labelsize": 12,
        "axes.titlesize": 14,
        "xtick.labelsize": 10,
        "ytick.labelsize": 10,
        "legend.fontsize": 10,
        "lines.linewidth": 2,
        "lines.markersize": 8,
    })


def ensure_dir_exists(directory: str):
    """Create directory if it doesn't exist."""
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        logger.info(f"Created directory: {directory}")


def plot_load_vs_latency(
    results: List[Dict[str, Any]],
    output_file: str,
):
    """
    Plot request rate vs P99 latency.

    This plot shows how tail latency degrades as request rate increases,
    which is fundamental to understanding system performance.

    Args:
        results: List of experiment results sorted by request_rate
        output_file: Path to save PNG file

    Expected result fields:
        - request_rate: Requests per second
        - p99_latency: P99 latency in milliseconds
    """
    setup_plot_style()
    ensure_dir_exists(os.path.dirname(output_file))

    request_rates = [r["request_rate"] for r in results]
    p99_latencies = [r["p99_latency"] for r in results]

    fig, ax = plt.subplots(figsize=(10, 6))

    ax.plot(
        request_rates,
        p99_latencies,
        marker="o",
        linestyle="-",
        linewidth=2,
        markersize=8,
        color="#1f77b4",
    )

    ax.set_xlabel("Request Rate (requests/sec)", fontsize=12, fontweight="bold")
    ax.set_ylabel("P99 Latency (ms)", fontsize=12, fontweight="bold")
    ax.set_title("Load vs P99 Latency", fontsize=14, fontweight="bold")
    ax.grid(True, alpha=0.3)

    # Add value labels on points
    for x, y in zip(request_rates, p99_latencies):
        ax.annotate(f"{y:.1f}ms", (x, y), textcoords="offset points", xytext=(0, 10), ha="center")

    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()
    logger.info(f"Saved plot: {output_file}")


def plot_batch_size_vs_throughput(
    results: List[Dict[str, Any]],
    output_file: str,
):
    """
    Plot batch size vs throughput.

    This plot shows how batching affects system throughput (requests/sec).

    Args:
        results: List of experiment results sorted by batch_size
        output_file: Path to save PNG file

    Expected result fields:
        - batch_size: Batch size
        - throughput: Requests per second
    """
    setup_plot_style()
    ensure_dir_exists(os.path.dirname(output_file))

    batch_sizes = [str(r["batch_size"]) for r in results]
    throughputs = [r["throughput"] for r in results]

    fig, ax = plt.subplots(figsize=(10, 6))

    ax.bar(batch_sizes, throughputs, color="#2ca02c", alpha=0.8, edgecolor="black", linewidth=1.5)

    ax.set_xlabel("Batch Size", fontsize=12, fontweight="bold")
    ax.set_ylabel("Throughput (requests/sec)", fontsize=12, fontweight="bold")
    ax.set_title("Batch Size vs Throughput", fontsize=14, fontweight="bold")
    ax.grid(True, alpha=0.3, axis="y")

    # Add value labels on bars
    for i, (batch, throughput) in enumerate(zip(batch_sizes, throughputs)):
        ax.text(i, throughput, f"{throughput:.1f}", ha="center", va="bottom", fontweight="bold")

    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()
    logger.info(f"Saved plot: {output_file}")


def plot_batch_size_vs_p99(
    results: List[Dict[str, Any]],
    output_file: str,
):
    """
    Plot batch size vs P99 latency.

    This plot shows the latency trade-off of batching:
    larger batches may increase latency but improve throughput.

    Args:
        results: List of experiment results sorted by batch_size
        output_file: Path to save PNG file

    Expected result fields:
        - batch_size: Batch size
        - p99_latency: P99 latency in milliseconds
    """
    setup_plot_style()
    ensure_dir_exists(os.path.dirname(output_file))

    batch_sizes = [str(r["batch_size"]) for r in results]
    p99_latencies = [r["p99_latency"] for r in results]

    fig, ax = plt.subplots(figsize=(10, 6))

    ax.bar(batch_sizes, p99_latencies, color="#ff7f0e", alpha=0.8, edgecolor="black", linewidth=1.5)

    ax.set_xlabel("Batch Size", fontsize=12, fontweight="bold")
    ax.set_ylabel("P99 Latency (ms)", fontsize=12, fontweight="bold")
    ax.set_title("Batch Size vs P99 Latency", fontsize=14, fontweight="bold")
    ax.grid(True, alpha=0.3, axis="y")

    # Add value labels on bars
    for i, (batch, p99) in enumerate(zip(batch_sizes, p99_latencies)):
        ax.text(i, p99, f"{p99:.1f}ms", ha="center", va="bottom", fontweight="bold")

    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()
    logger.info(f"Saved plot: {output_file}")


def plot_scheduler_comparison(
    results: List[Dict[str, Any]],
    output_file: str,
):
    """
    Plot scheduler comparison: P99 latency across schedulers.

    This plot demonstrates how different scheduling strategies affect
    tail latency under the same load conditions.

    Args:
        results: List of experiment results, one per scheduler
        output_file: Path to save PNG file

    Expected result fields:
        - scheduler: Scheduler name (string)
        - p99_latency: P99 latency in milliseconds
    """
    setup_plot_style()
    ensure_dir_exists(os.path.dirname(output_file))

    schedulers = [r["scheduler"] for r in results]
    p99_latencies = [r["p99_latency"] for r in results]

    fig, ax = plt.subplots(figsize=(10, 6))

    colors = ["#1f77b4", "#ff7f0e", "#2ca02c"]
    ax.bar(schedulers, p99_latencies, color=colors[:len(schedulers)], alpha=0.8, edgecolor="black", linewidth=1.5)

    ax.set_xlabel("Scheduler Strategy", fontsize=12, fontweight="bold")
    ax.set_ylabel("P99 Latency (ms)", fontsize=12, fontweight="bold")
    ax.set_title("Scheduler Comparison - P99 Latency", fontsize=14, fontweight="bold")
    ax.grid(True, alpha=0.3, axis="y")

    # Add value labels on bars
    for i, (scheduler, p99) in enumerate(zip(schedulers, p99_latencies)):
        ax.text(i, p99, f"{p99:.1f}ms", ha="center", va="bottom", fontweight="bold")

    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()
    logger.info(f"Saved plot: {output_file}")


def plot_failure_rate_vs_p99(
    results: List[Dict[str, Any]],
    output_file: str,
):
    """
    Plot failure injection: failure rate vs P99 latency.

    This plot shows the impact of worker/system failures on tail latency,
    useful for understanding failure handling and retry impacts.

    Args:
        results: List of experiment results sorted by failure_rate
        output_file: Path to save PNG file

    Expected result fields:
        - failure_rate (config): Injected failure rate (0.0 - 1.0)
        - p99_latency: P99 latency in milliseconds
    """
    setup_plot_style()
    ensure_dir_exists(os.path.dirname(output_file))

    failure_rates = [r.get("config_failure_rate", r.get("failure_rate", 0)) for r in results]
    p99_latencies = [r["p99_latency"] for r in results]

    fig, ax = plt.subplots(figsize=(10, 6))

    ax.plot(
        failure_rates,
        p99_latencies,
        marker="s",
        linestyle="-",
        linewidth=2,
        markersize=8,
        color="#d62728",
    )

    ax.set_xlabel("Injected Failure Rate", fontsize=12, fontweight="bold")
    ax.set_ylabel("P99 Latency (ms)", fontsize=12, fontweight="bold")
    ax.set_title("Failure Injection - P99 Latency Impact", fontsize=14, fontweight="bold")
    ax.grid(True, alpha=0.3)

    # Format x-axis as percentages
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: "{:.0%}".format(y)))

    # Add value labels on points
    for x, y in zip(failure_rates, p99_latencies):
        ax.annotate(f"{y:.1f}ms", (x, y), textcoords="offset points", xytext=(0, 10), ha="center")

    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()
    logger.info(f"Saved plot: {output_file}")


def plot_failure_rate_vs_throughput(
    results: List[Dict[str, Any]],
    output_file: str,
):
    """
    Plot failure injection: failure rate vs throughput.

    This plot shows how system failures reduce effective throughput.

    Args:
        results: List of experiment results sorted by failure_rate
        output_file: Path to save PNG file

    Expected result fields:
        - failure_rate (config): Injected failure rate
        - throughput: Requests per second
    """
    setup_plot_style()
    ensure_dir_exists(os.path.dirname(output_file))

    failure_rates = [r.get("config_failure_rate", r.get("failure_rate", 0)) for r in results]
    throughputs = [r["throughput"] for r in results]

    fig, ax = plt.subplots(figsize=(10, 6))

    ax.plot(
        failure_rates,
        throughputs,
        marker="o",
        linestyle="-",
        linewidth=2,
        markersize=8,
        color="#9467bd",
    )

    ax.set_xlabel("Injected Failure Rate", fontsize=12, fontweight="bold")
    ax.set_ylabel("Throughput (requests/sec)", fontsize=12, fontweight="bold")
    ax.set_title("Failure Injection - Throughput Impact", fontsize=14, fontweight="bold")
    ax.grid(True, alpha=0.3)

    # Format x-axis as percentages
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: "{:.0%}".format(y)))

    # Add value labels on points
    for x, y in zip(failure_rates, throughputs):
        ax.annotate(f"{y:.1f}", (x, y), textcoords="offset points", xytext=(0, 10), ha="center")

    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()
    logger.info(f"Saved plot: {output_file}")


def plot_latency_sweep(
    results: List[Dict[str, Any]],
    output_file: str,
):
    """
    Plot request rate vs P99 latency with one line per scheduler.

    Args:
        results: All latency_sweep results (mixed schedulers)
        output_file: Path to save PNG file
    """
    setup_plot_style()
    ensure_dir_exists(os.path.dirname(output_file))

    schedulers = ["fifo", "batching", "latency_aware"]
    colors = {"fifo": "#1f77b4", "batching": "#ff7f0e", "latency_aware": "#2ca02c"}
    markers = {"fifo": "o", "batching": "s", "latency_aware": "^"}

    fig, ax = plt.subplots(figsize=(10, 6))

    for scheduler in schedulers:
        rows = sorted(
            [r for r in results if r["scheduler"] == scheduler],
            key=lambda r: r["request_rate"],
        )
        if not rows:
            continue
        rates = [r["request_rate"] for r in rows]
        p99s = [r["p99_latency"] for r in rows]
        ax.plot(
            rates, p99s,
            marker=markers[scheduler],
            label=scheduler,
            color=colors[scheduler],
            linewidth=2,
            markersize=8,
        )

    ax.set_xlabel("Request Rate (req/s)", fontsize=12, fontweight="bold")
    ax.set_ylabel("P99 Latency (ms)", fontsize=12, fontweight="bold")
    ax.set_title("Request Rate Sweep — P99 Latency by Scheduler", fontsize=14, fontweight="bold")
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()
    logger.info(f"Saved plot: {output_file}")


def plot_bursty_comparison(
    results: List[Dict[str, Any]],
    output_file: str,
):
    """
    Bar chart comparing P99 latency across schedulers under bursty traffic.

    Args:
        results: One result dict per scheduler
        output_file: Path to save PNG file
    """
    setup_plot_style()
    ensure_dir_exists(os.path.dirname(output_file))

    schedulers = [r["scheduler"] for r in results]
    p99s = [r["p99_latency"] for r in results]
    colors = {"fifo": "#1f77b4", "batching": "#ff7f0e", "latency_aware": "#2ca02c"}

    fig, ax = plt.subplots(figsize=(10, 6))
    bar_colors = [colors.get(s, "#999999") for s in schedulers]
    ax.bar(schedulers, p99s, color=bar_colors, alpha=0.8, edgecolor="black", linewidth=1.5)

    for i, (s, p99) in enumerate(zip(schedulers, p99s)):
        ax.text(i, p99, f"{p99:.1f}ms", ha="center", va="bottom", fontweight="bold")

    ax.set_xlabel("Scheduler Strategy", fontsize=12, fontweight="bold")
    ax.set_ylabel("P99 Latency (ms)", fontsize=12, fontweight="bold")
    ax.set_title("Bursty Traffic — P99 Latency by Scheduler", fontsize=14, fontweight="bold")
    ax.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()
    logger.info(f"Saved plot: {output_file}")


def plot_fault_tolerance(
    results: list,
    output_file: str,
):
    """Before/after comparison when a worker is killed mid-experiment."""
    setup_plot_style()
    ensure_dir_exists(os.path.dirname(output_file))

    phases = [r["phase"] for r in results]
    p99s = [r["p99_latency"] for r in results]
    throughputs = [r["throughput"] for r in results]
    error_rates = [r["failure_rate_actual"] * 100 for r in results]

    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    colors = ["#2ca02c", "#d62728"]

    for ax, values, title, ylabel in zip(
        axes,
        [p99s, throughputs, error_rates],
        ["P99 Latency", "Throughput", "Error Rate"],
        ["ms", "req/s", "%"],
    ):
        bars = ax.bar(phases, values, color=colors, alpha=0.8, edgecolor="black")
        for bar, val in zip(bars, values):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height(),
                f"{val:.1f}{ylabel}",
                ha="center", va="bottom", fontweight="bold",
            )
        ax.set_title(title, fontsize=13, fontweight="bold")
        ax.set_ylabel(ylabel, fontsize=11)
        ax.grid(True, alpha=0.3, axis="y")

    fig.suptitle("Fault Tolerance: Worker Failure & Recovery", fontsize=14, fontweight="bold")
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()
    logger.info(f"Saved plot: {output_file}")


def plot_dynamic_scaling(
    results: list,
    output_file: str,
):
    """Throughput and P99 as worker count increases."""
    setup_plot_style()
    ensure_dir_exists(os.path.dirname(output_file))

    worker_counts = [str(r["num_workers"]) for r in results]
    throughputs = [r["throughput"] for r in results]
    p99s = [r["p99_latency"] for r in results]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

    ax1.bar(worker_counts, throughputs, color="#1f77b4", alpha=0.8, edgecolor="black")
    for i, v in enumerate(throughputs):
        ax1.text(i, v, f"{v:.1f}", ha="center", va="bottom", fontweight="bold")
    ax1.set_xlabel("Number of Workers", fontsize=12, fontweight="bold")
    ax1.set_ylabel("Throughput (req/s)", fontsize=12, fontweight="bold")
    ax1.set_title("Throughput vs Worker Count", fontsize=13, fontweight="bold")
    ax1.grid(True, alpha=0.3, axis="y")

    ax2.bar(worker_counts, p99s, color="#ff7f0e", alpha=0.8, edgecolor="black")
    for i, v in enumerate(p99s):
        ax2.text(i, v, f"{v:.1f}ms", ha="center", va="bottom", fontweight="bold")
    ax2.set_xlabel("Number of Workers", fontsize=12, fontweight="bold")
    ax2.set_ylabel("P99 Latency (ms)", fontsize=12, fontweight="bold")
    ax2.set_title("P99 Latency vs Worker Count", fontsize=13, fontweight="bold")
    ax2.grid(True, alpha=0.3, axis="y")

    fig.suptitle("Horizontal Scaling: Throughput & Latency", fontsize=14, fontweight="bold")
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()
    logger.info(f"Saved plot: {output_file}")


def plot_latency_percentiles(
    results: List[Dict[str, Any]],
    output_file: str,
    x_field: str = "request_rate",
    x_label: str = "Request Rate (requests/sec)",
    title_prefix: str = "Request Rate",
):
    """
    Plot all latency percentiles (P50, P95, P99) on same graph.

    Args:
        results: List of experiment results
        output_file: Path to save PNG file
        x_field: Field to use for x-axis
        x_label: Label for x-axis
        title_prefix: Prefix for plot title
    """
    setup_plot_style()
    ensure_dir_exists(os.path.dirname(output_file))

    x_values = [r[x_field] for r in results]
    p50_values = [r["p50_latency"] for r in results]
    p95_values = [r["p95_latency"] for r in results]
    p99_values = [r["p99_latency"] for r in results]

    fig, ax = plt.subplots(figsize=(10, 6))

    ax.plot(x_values, p50_values, marker="o", label="P50", linewidth=2, markersize=8)
    ax.plot(x_values, p95_values, marker="s", label="P95", linewidth=2, markersize=8)
    ax.plot(x_values, p99_values, marker="^", label="P99", linewidth=2, markersize=8)

    ax.set_xlabel(x_label, fontsize=12, fontweight="bold")
    ax.set_ylabel("Latency (ms)", fontsize=12, fontweight="bold")
    ax.set_title(f"{title_prefix} - Latency Percentiles", fontsize=14, fontweight="bold")
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()
    logger.info(f"Saved plot: {output_file}")
