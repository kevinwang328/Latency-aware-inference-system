# Quick Start Guide

## ⚡ 60-Second Setup

### 1. Install Dependencies
```bash
cd Latency-aware-inference-system
pip install -r experiments/requirements.txt
```

### 2. Ensure API Server is Running
```bash
# In another terminal
python your_api_server.py
# Should be listening on http://localhost:8000/predict
```

### 3. Run Your First Experiment
```bash
python experiments/run_experiments.py --experiment load
```

### 4. Check Results
```bash
# View raw data
cat experiments/results/load_vs_latency.csv

# View plot
open experiments/plots/load_vs_latency.png  # macOS
xdg-open experiments/plots/load_vs_latency.png  # Linux
```

---

## 📊 All Available Experiments

```bash
# Test 1: How does load affect latency?
python experiments/run_experiments.py --experiment load

# Test 2: Batch size trade-offs
python experiments/run_experiments.py --experiment batch

# Test 3: Compare scheduler strategies
python experiments/run_experiments.py --experiment scheduler

# Test 4: Failure handling
python experiments/run_experiments.py --experiment failure

# Run all experiments
python experiments/run_experiments.py --experiment all
```

---

## 🔧 Common Options

```bash
# Longer test: 30 seconds per configuration
python experiments/run_experiments.py --experiment load --duration 30

# More threads: 16 concurrent clients
python experiments/run_experiments.py --experiment batch --threads 16

# Custom server URL
python experiments/run_experiments.py --experiment load \
  --url http://192.168.1.100:8000/predict

# Custom request rates
python experiments/run_experiments.py --experiment load \
  --request-rates 5 10 20 50 100 200

# Custom batch sizes
python experiments/run_experiments.py --experiment batch \
  --batch-sizes 1 2 4 8 16 32
```

---

## 📁 What Gets Generated

```
experiments/
├── results/
│   ├── load_vs_latency.csv              ← Raw data
│   ├── batch_size_tradeoff.csv
│   ├── scheduler_comparison.csv
│   └── failure_injection.csv
└── plots/
    ├── load_vs_latency.png              ← Visualization
    ├── batch_size_vs_throughput.png
    ├── batch_size_vs_p99.png
    ├── scheduler_comparison_p99.png
    ├── failure_rate_vs_p99.png
    └── failure_rate_vs_throughput.png
```

---

## 💾 CSV Format

Each CSV has:
- `request_rate`: Requests per second
- `p99_latency`: P99 tail latency (ms)
- `throughput`: Actual throughput (req/s)
- `avg_latency`, `p50_latency`, `p95_latency`: Other percentiles
- `failed_requests`: Count of failed requests

---

## ❌ Troubleshooting

### "Cannot reach API server"
Check server is running:
```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"x": 1}'
```

### No results in CSV
- Check server is responding normally
- Check tests ran: look for "Load test completed" message
- Increase duration: `--duration 20`

### Very high latency values
- Normal if load is too high (>300 req/s)
- Check if server is overwhelming
- Try lower request rates first

---

## 📖 More Info

- See `experiments/README.md` for detailed documentation
- See `IMPLEMENTATION_GUIDE.md` for integration details
- See `experiments/example_usage.py` for programmatic usage

---

**Ready?** Run your first experiment:
```bash
python experiments/run_experiments.py --experiment load
```
