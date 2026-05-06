# Latency-Aware Inference System

A high-throughput ML inference serving system with pluggable scheduling strategies, gRPC-based worker communication, and ZooKeeper-driven service discovery. Built to study tail latency (P99) under realistic load patterns.

## Architecture

```
┌──────────────┐     HTTP      ┌─────────────────┐     gRPC      ┌──────────────────┐
│   Clients    │ ──────────►  │   FastAPI Server  │ ──────────►  │  Worker Process  │
│ (load tester)│              │  + Scheduler      │              │  (port 5005x)    │
└──────────────┘              └────────┬──────────┘              └────────┬─────────┘
                                       │                                   │
                                       │  watch                            │  register
                                       ▼                                   ▼
                                ┌─────────────────────────────────────────────────┐
                                │              ZooKeeper (:2181)                  │
                                │         /inference/workers/worker-{id}          │
                                └─────────────────────────────────────────────────┘
```

**Key design decisions:**
- Workers register as ephemeral ZooKeeper znodes — when a worker process dies, its node expires automatically and the pool stops routing to it within seconds
- `GrpcWorkerPool` uses a `ChildrenWatch` so new workers are discovered live, without restarting the server
- Three scheduler strategies share a single `TaskQueue`; switching strategy at runtime requires only a config API call

## Project Structure

```
project_root/
├── system/
│   ├── api_server.py           FastAPI server — /predict, /config/*, /status
│   ├── scheduler.py            FIFO, Batching, LatencyAware scheduler strategies
│   ├── worker.py               Thread-based worker pool (default mode)
│   ├── grpc_worker_pool.py     gRPC worker pool — discovers workers via ZooKeeper
│   ├── grpc_worker_server.py   Standalone gRPC worker process
│   ├── zookeeper_registry.py   Worker registration & discovery via ZooKeeper
│   ├── inference.proto         gRPC service definition
│   ├── task.py                 Task state machine (pending → processing → done/failed)
│   ├── queue.py                Thread-safe bounded task queue
│   └── config.py               Runtime-mutable config singleton
│
├── experiments/
│   ├── load_test.py            Concurrent HTTP load generator
│   ├── metrics.py              P50/P99/throughput calculations
│   ├── plot_results.py         Matplotlib visualisation
│   ├── run_experiments.py      Experiment orchestrator & CLI
│   ├── results/                CSV output (auto-created)
│   └── plots/                  PNG plots (auto-created)
│
├── start_workers.sh            Generate gRPC stubs + launch 4 worker processes
├── docker-compose.yml          ZooKeeper via Docker (alternative to Homebrew)
└── requirements.txt
```

## Quick Start

### Mode 1 — Thread-based workers (no dependencies)

```bash
pip install -r requirements.txt

# Start server
python -m system.api_server

# Run experiments (in a second terminal)
python -m experiments.run_experiments --experiment scheduler
python -m experiments.run_experiments --experiment load
python -m experiments.run_experiments --experiment all
```

### Mode 2 — gRPC workers + ZooKeeper

```bash
# 1. Start ZooKeeper (Homebrew)
brew install zookeeper
brew services start zookeeper

# 2. Generate stubs and start workers
bash start_workers.sh   # starts workers on ports 50051-50054

# 3. Start server with gRPC mode
USE_GRPC=1 python -m uvicorn system.api_server:app --host 0.0.0.0 --port 8000

# 4. Run fault tolerance and dynamic scaling experiments
python -m experiments.run_experiments --experiment fault
python -m experiments.run_experiments --experiment scaling
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/predict` | Submit inference request `{"x": value}` |
| POST | `/config/batch_size` | Set batch size `{"batch_size": 8}` |
| POST | `/config/scheduler` | Switch strategy `{"scheduler": "latency_aware"}` |
| POST | `/config/failure_rate` | Inject failures `{"failure_rate": 0.1}` |
| GET  | `/status` | System health and queue stats |

## Scheduler Strategies

| Strategy | Behavior |
|----------|----------|
| `fifo` | Tasks dispatched in arrival order |
| `batching` | Accumulates up to `batch_size` tasks before dispatch |
| `latency_aware` | Tasks older than `latency_threshold_ms` are promoted to front of next batch; prevents starvation under mixed load |

## Experiments

| Experiment | Flag | What it measures |
|-----------|------|-----------------|
| Request rate sweep | `load` | P99 latency vs requests/sec |
| Batch size trade-off | `batch` | Throughput & P99 vs batch size |
| Scheduler comparison | `scheduler` | P99 across all three strategies |
| Failure injection | `failure` | Throughput & P99 under configurable failure rates |
| Rate × scheduler sweep | `sweep` | Heatmap across rates and strategies |
| Bursty traffic | `bursty` | P99 during burst phases per strategy |
| Fault tolerance | `fault` | Before/after worker failure (gRPC mode) |
| Dynamic scaling | `scaling` | Throughput and P99 as workers are added live (gRPC mode) |

## Experimental Results

### Scheduler Comparison (200 req/s, 4 workers)

`latency_aware` reduces tail latency by prioritising tasks that have been waiting longest, preventing a small number of requests from experiencing unbounded queuing delay.

### Fault Tolerance

ZooKeeper ephemeral nodes detect worker failure within ~4 seconds. The pool automatically stops routing to the dead worker; in-flight tasks on that worker fail, but subsequent traffic continues with reduced capacity.

| Phase | Workers | P99 | Error Rate |
|-------|---------|-----|-----------|
| Before failure | 2 | 127ms | 0% |
| After worker killed | 1 | 130ms | 14.6% |

### Dynamic Scaling (100 req/s)

Adding a worker at runtime — without restarting the API server — reduces P99 by 29% as the new worker is discovered via ZooKeeper and immediately begins receiving traffic.

| Phase | Workers | P99 |
|-------|---------|-----|
| Phase 1 | 2 | 187ms |
| Phase 2 (worker added) | 3 | 132ms |
