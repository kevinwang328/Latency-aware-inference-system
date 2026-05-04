# Implementation Guide: Experiments Module Integration

## 📋 Overview

This guide explains how the experiments module integrates with your inference system and what additional components you need to implement for full functionality.

## 🏗️ System Architecture

```
┌─────────────┐
│   Client    │
│  (Load Gen) │
└──────┬──────┘
       │ HTTP POST /predict
       │
┌──────▼──────────────────┐
│   API Server            │
│  - Task creation        │
│  - Queue management     │
│  - Configuration (TODO) │
└──────┬──────────────────┘
       │ RPC
       │
┌──────▼──────────────────┐
│   Scheduler             │
│ - FIFO                  │
│ - Batching              │
│ - Latency-Aware (TODO)  │
└──────┬──────────────────┘
       │
┌──────▼──────────────────┐
│   Workers (N instances)│
│ - Process batches      │
│ - Run model predictions│
│ - Report results       │
└───────────────────────┘
```

## ✅ What's Already Implemented

### Load Generator (`experiments/load_test.py`)
- ✓ Concurrent HTTP requests using thread pool
- ✓ Precise latency measurement (before send → after receive)
- ✓ Failure tolerance and logging
- ✓ Configurable: rate, duration, threads, request body
- ✓ Progress reporting

### Metrics (`experiments/metrics.py`)
- ✓ Latency percentile calculations (P50, P95, P99)
- ✓ Throughput computation
- ✓ Failure rate analysis
- ✓ Handles edge cases (empty logs, all failures)
- ✓ Both numpy-optimized and pure Python implementations

### Plotting (`experiments/plot_results.py`)
- ✓ All required plots (load, batch, scheduler, failure)
- ✓ PNG output (300 DPI)
- ✓ Clear labels and formatting
- ✓ No GUI display required

### Orchestration (`experiments/run_experiments.py`)
- ✓ Experiment configuration and execution
- ✓ CSV result collection
- ✓ Command-line interface with argparse
- ✓ Progress tracking and logging

## 🔴 TODO Items (Integration Points)

### 1. API Server Configuration Endpoints

The experiments module expects optional configuration endpoints. These allow changing system parameters without restarting:

**Endpoint 1: Set Batch Size**
```
Method: POST
Path: /config/batch_size
Request Body: {"batch_size": 8}
Response: {"status": "ok", "batch_size": 8}

TODO: Implement in your API server
Location: run_experiments.py line ~550 (_apply_configuration_to_server method)
```

**Endpoint 2: Set Scheduler Strategy**
```
Method: POST
Path: /config/scheduler
Request Body: {"scheduler": "latency_aware"}
Response: {"status": "ok", "scheduler": "latency_aware"}

Supported schedulers: "fifo", "batching", "latency_aware"

TODO: Implement in your API server
```

**Endpoint 3: Inject Failure Rate**
```
Method: POST
Path: /config/failure_rate
Request Body: {"failure_rate": 0.1}
Response: {"status": "ok", "failure_rate": 0.1}

Note: Workers should randomly fail at this rate

TODO: Implement in your API server + workers
```

**Example Implementation (FastAPI):**
```python
# In your main API server
from fastapi import FastAPI

app = FastAPI()

# Global configuration
config = {
    "batch_size": 4,
    "scheduler": "fifo",
    "failure_rate": 0.0,
}

@app.post("/config/batch_size")
async def set_batch_size(params: dict):
    config["batch_size"] = params["batch_size"]
    # Notify scheduler of new batch size
    scheduler.set_batch_size(params["batch_size"])
    return {"status": "ok", "batch_size": config["batch_size"]}

@app.post("/config/scheduler")
async def set_scheduler(params: dict):
    config["scheduler"] = params["scheduler"]
    # Reconfigure scheduler
    scheduler.set_strategy(params["scheduler"])
    return {"status": "ok", "scheduler": config["scheduler"]}

@app.post("/config/failure_rate")
async def set_failure_rate(params: dict):
    config["failure_rate"] = params["failure_rate"]
    # Propagate to workers
    for worker in workers:
        worker.set_failure_rate(params["failure_rate"])
    return {"status": "ok", "failure_rate": config["failure_rate"]}
```

### 2. Worker Failure Injection

For the failure injection experiment, workers need to:

```python
# In your worker implementation
import random

class Worker:
    def __init__(self):
        self.failure_rate = 0.0

    def set_failure_rate(self, rate: float):
        """Enable failure injection."""
        self.failure_rate = rate

    def process_batch(self, batch):
        # Randomly fail at configured rate
        if random.random() < self.failure_rate:
            raise Exception("Injected failure")

        # Process normally
        return self.model.predict(batch)
```

### 3. Scheduler Implementation

If not already implemented, add these scheduler strategies:

**FIFO Scheduler:**
```python
class FIFOScheduler:
    """Simple first-in-first-out scheduling."""

    def schedule(self, tasks: List[Task]) -> List[Batch]:
        """Create batches in order received."""
        batches = []
        for i in range(0, len(tasks), self.batch_size):
            batch = tasks[i:i+self.batch_size]
            batches.append(Batch(batch))
        return batches
```

**Latency-Aware Scheduler:**
```python
class LatencyAwareScheduler:
    """Schedule to optimize tail latency (P99)."""

    def schedule(self, tasks: List[Task]) -> List[Batch]:
        """
        Create smaller batches for older tasks (lower latency).
        Batch older tasks with fewer newer tasks to maintain P99.
        """
        # Prioritize based on task age
        sorted_tasks = sorted(tasks, key=lambda t: t.creation_time)
        batches = []

        for i, task in enumerate(sorted_tasks):
            age = time.time() - task.creation_time

            # Smaller batch size for old tasks
            if age > THRESHOLD:
                batch_size = 1  # Process immediately
            else:
                batch_size = self.batch_size

            # Create batch when full or task is old enough
            if i % batch_size == 0:
                batch = sorted_tasks[i:i+batch_size]
                batches.append(Batch(batch))

        return batches
```

## 🔧 Integration Checklist

### Phase 1: Basic Integration (Can run experiments now)
- [ ] API server has `/predict` endpoint
- [ ] `/predict` accepts JSON body: `{"x": value}`
- [ ] Returns HTTP 200 on success
- [ ] Load generator can connect and send requests

### Phase 2: Enhanced Experiments
- [ ] Implement `/config/batch_size` endpoint
- [ ] Implement `/config/scheduler` endpoint
- [ ] Implement batch size and scheduler logic
- [ ] Test all 3 scheduler strategies work

### Phase 3: Failure Handling
- [ ] Implement `/config/failure_rate` endpoint
- [ ] Workers support failure rate configuration
- [ ] Workers respect retry logic
- [ ] Run failure injection experiment

## 🚀 Testing the Integration

### Step 1: Verify API Server

```bash
# Test simple request
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"x": 1}'

# Expected response
{"result": 1}  # Or your model output
```

### Step 2: Run Simple Load Test

```bash
cd Latency-aware-inference-system

# Install dependencies
pip install -r experiments/requirements.txt

# Run load test (10 requests/sec, 5 seconds)
python experiments/run_experiments.py --experiment load \
  --request-rates 10 \
  --duration 5 \
  --threads 1

# Check CSV output
cat experiments/results/load_vs_latency.csv

# Check plot
open experiments/plots/load_vs_latency.png
```

### Step 3: Gradually Add Features

```bash
# After implementing batch size config
python experiments/run_experiments.py --experiment batch

# After implementing scheduler config
python experiments/run_experiments.py --experiment scheduler

# After implementing failure injection
python experiments/run_experiments.py --experiment failure
```

## 📊 Expected Results Format

CSV files should accumulate results like this:

```csv
experiment_name,setting_name,request_rate,batch_size,scheduler,failure_rate,total_requests,completed_requests,failed_requests,avg_latency,p50_latency,p95_latency,p99_latency,throughput,failure_rate_actual,timestamp
load_vs_latency,rate_10_batch_4_sched_latency_aware,10,4,latency_aware,0.0,100,98,2,5.23,4.51,12.34,25.67,9.80,0.0200,2024-05-03T10:30:45.123456
load_vs_latency,rate_50_batch_4_sched_latency_aware,50,4,latency_aware,0.0,500,490,10,8.45,7.12,18.90,42.33,49.00,0.0200,2024-05-03T10:30:52.654321
```

## 🔍 Debugging Tips

### If experiments fail to run:

1. **Check server is running:**
   ```bash
   curl http://localhost:8000/predict
   # Should not timeout
   ```

2. **Check Python environment:**
   ```bash
   python -c "import requests; import matplotlib; print('Dependencies OK')"
   ```

3. **Enable verbose logging:**
   ```bash
   python experiments/run_experiments.py --experiment load --duration 3
   # Look for error messages in output
   ```

4. **Check file permissions:**
   ```bash
   ls -l experiments/results/
   # Should be writable
   ```

## 📈 Performance Baselines

Here are typical performance characteristics for reference:

```
Tiny Model (inference <1ms):
- 100 req/s: P99 ~15ms
- 200 req/s: P99 ~50ms

Small Model (inference ~5ms):
- 100 req/s: P99 ~50ms
- 200 req/s: P99 ~150ms+

Large Model (inference ~50ms):
- 100 req/s: P99 ~500ms
- 200 req/s: Queue only, requests wait
```

## 🎯 Next Steps

1. **Review requirements.txt** - Install all dependencies
2. **Review load_test.py** - Understand request generation
3. **Review metrics.py** - Understand metric calculations
4. **Implement configuration endpoints** - Allow experiments to change settings
5. **Run experiments/run_experiments.py --experiment load** - Start simple
6. **Progressively enable more experiments** - As features are implemented
7. **Analyze results** - Study CSV output and plots

## 📝 Key Files Reference

| File | Purpose | Integration Points |
|------|---------|-------------------|
| `metrics.py` | Calculate latency percentiles, throughput | No changes needed |
| `load_test.py` | Generate concurrent HTTP load | No changes needed |
| `plot_results.py` | Create visualization plots | No changes needed |
| `run_experiments.py` | Orchestrate experiments, call API | **Line 550**: Add config endpoints |
| Your API server | Accept `/predict` requests | Add `/config/*` endpoints |
| Your scheduler | Route requests to workers | Support 3 strategies |
| Your workers | Process batches | Support failure injection |

## ⚙️ Configuration Summary

Configuration is passed through HTTP POST endpoints:

```
1. Batch Size
   - Range: 1-128 (adjust based on memory)
   - Default: 4
   - Impact: Larger → higher throughput, higher latency

2. Scheduler Strategy
   - FIFO: Process in order
   - Batching: Group by batch size
   - Latency-Aware: Minimize P99
   - Default: latency_aware

3. Failure Rate
   - Range: 0.0 - 1.0 (0% to 100%)
   - Default: 0.0 (no failures)
   - Impact: Tests retry logic and error handling
```

## 🤔 Frequently Asked Questions

**Q: Can I run experiments without implementing all config endpoints?**
A: Yes! The experiments will work, but you'll get the same results for each configuration. The plotting will still show trends. Implement config endpoints when ready.

**Q: How long should each experiment run?**
A: Default 10 seconds per configuration. For quick testing, reduce to 3-5 seconds. For publication, use 30+ seconds to reduce noise.

**Q: What if my API server is on a different machine?**
A: Use `--url` flag: `python run_experiments.py --experiment load --url http://192.168.1.100:8000/predict`

**Q: Can I add custom experiments?**
A: Yes! Extend `ExperimentRunner` with new methods following the same pattern. Add plotting functions to `plot_results.py`.

**Q: How do I interpret the latency percentiles?**
A: P50 = median latency, P95 = 95% of requests are faster, P99 = tail latency (most critical for user experience)

---

**Last Updated:** 2024-05-03
**Status:** Complete - Ready for Integration
**Next Phase:** Implement configuration endpoints in API server
