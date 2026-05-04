# Latency-Aware Inference System

A research platform for studying scheduling strategies and their effect on tail latency (P99) in ML inference services.

## Project Structure

```
project_root/
├── system/                  ← Core inference system
│   ├── api_server.py        API server (FastAPI) with /predict & /config/* endpoints
│   ├── scheduler.py         FIFO, Batching, and Latency-Aware scheduler strategies
│   ├── worker.py            Worker pool — simulates model inference, supports failure injection
│   ├── task.py              Task dataclass (pending → processing → done/failed)
│   ├── queue.py             Thread-safe bounded task queue
│   └── config.py            Runtime-mutable system configuration (singleton)
│
├── experiments/             ← Benchmarking suite
│   ├── load_test.py         Concurrent HTTP load generator
│   ├── metrics.py           Latency percentile & throughput calculations
│   ├── plot_results.py      Matplotlib visualisation utilities
│   ├── run_experiments.py   Experiment orchestrator & CLI
│   ├── results/             CSV output (auto-created)
│   └── plots/               PNG plots (auto-created)
│
├── requirements.txt
└── README.md
```

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Start the API server (from project root)
python -m system.api_server
# Server runs at http://localhost:8000

# 3. In a second terminal, run experiments
python experiments/run_experiments.py --experiment load
python experiments/run_experiments.py --experiment batch
python experiments/run_experiments.py --experiment scheduler
python experiments/run_experiments.py --experiment failure
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/predict` | Submit inference request `{"x": value}` |
| POST | `/config/batch_size` | Set batch size `{"batch_size": 8}` |
| POST | `/config/scheduler` | Set strategy `{"scheduler": "latency_aware"}` |
| POST | `/config/failure_rate` | Inject failures `{"failure_rate": 0.1}` |
| GET | `/status` | System health and queue stats |

Scheduler strategies: `fifo`, `batching`, `latency_aware`

## Experiments

| Experiment | What varies | Key metric |
|-----------|-------------|------------|
| `load` | Request rate (10–200 req/s) | P99 latency vs load |
| `batch` | Batch size (1–16) | Throughput & P99 trade-off |
| `scheduler` | Strategy | P99 latency per strategy |
| `failure` | Failure rate (0–20%) | P99 & throughput degradation |
