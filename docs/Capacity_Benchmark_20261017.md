# Capacity experiments, 2026-10-17

Monitoring observes a running service over time. Load testing deliberately creates traffic to measure capacity. This tool performs load experiments; existing Prometheus metrics remain the monitoring source.

## Reproduce isolated simulation

Install the platform dependencies, then run from the repository root:

```sh
python experiments/benchmark_capacity.py --output results/simulation.jsonl \
  --workers 1 2 4 --strategies fifo batching latency_aware \
  --batch-sizes 1 4 --rates 40 --duration 3
```

Each combination starts a fresh loopback-only CPU simulation and terminates it afterward. It does not use the live gateway, database, GPU, or environment credentials. The workload computes x squared with 60 ms simulated batch overhead. This is a short functional pilot, not a production latency guarantee.

| Workers | Strategy | Batch | Completed/s including drain | P95 total seconds | Client slots skipped |
|---:|---|---:|---:|---:|---:|
| 1 | fifo | 1 | 14.63 | 4.339 | 13 |
| 1 | fifo | 4 | 39.16 | 0.174 | 0 |
| 1 | batching | 1 | 14.54 | 4.385 | 13 |
| 1 | batching | 4 | 39.03 | 0.181 | 0 |
| 1 | latency_aware | 1 | 14.55 | 4.398 | 13 |
| 1 | latency_aware | 4 | 14.65 | 4.368 | 13 |
| 2 | fifo | 1 | 29.24 | 0.847 | 7 |
| 2 | fifo | 4 | 38.46 | 0.174 | 0 |
| 2 | batching | 1 | 29.19 | 1.092 | 0 |
| 2 | batching | 4 | 38.15 | 0.172 | 1 |
| 2 | latency_aware | 1 | 28.52 | 0.977 | 4 |
| 2 | latency_aware | 4 | 29.44 | 1.057 | 0 |
| 4 | fifo | 1 | 38.76 | 0.106 | 1 |
| 4 | fifo | 4 | 38.44 | 0.175 | 0 |
| 4 | batching | 1 | 37.84 | 0.110 | 4 |
| 4 | batching | 4 | 38.40 | 0.170 | 0 |
| 4 | latency_aware | 1 | 39.13 | 0.124 | 0 |
| 4 | latency_aware | 4 | 38.81 | 0.103 | 1 |

Target arrival rate was 40/s for three seconds. Skipped client slots are reported separately from submitted requests and server errors. Throughput includes draining all submitted work. Simulation batching amortizes artificial overhead; real V7.3 executes individual jobs and must not inherit this gain by assumption.

## Bounded real generation

Use an existing backend upload path. The following command can submit at most 14 model requests and will incur real GPU/LLM usage:

```sh
python experiments/benchmark_capacity.py --mode real --allow-real \
  --url http://127.0.0.1:18000 --audio-path /app/backend/storage/uploads/EXISTING.wav \
  --concurrency 1 2 4 --kinds audio text --max-jobs 14 --timeout 600 \
  --output results/real-new.jsonl
```

For an authenticated backend, provide its already issued token as BENCHMARK_BACKEND_TOKEN in the process environment, not a command-line argument. The script never prints the token. It does not modify production settings. Each request gets a separate test project under one generated capacity-* owner. Keep recorded IDs for investigation; there are no automatic resubmissions or cleanup deletions. A failed stage stops subsequent stages, after its already submitted peers finish. HTTP 202 alone is not success: audio must reach succeeded and both outputs must download; text must return a successful snapshot and downloadable outputs. Current validation checks Vital settings JSON and nonempty, nontruncated PCM WAV data, and records sizes and SHA-256. Download and validation time is included in total latency. The original pilot used WAV header validation; stronger validation was added afterward and covered by tests.

### Observed production baseline

| Kind | Client concurrency | Completed/submitted | Individual total seconds | Throughput/s |
|---|---:|---:|---|---:|
| audio | 1 | 1/1 | 27.87 | 0.0359 |
| audio | 2 | 2/2 | 26.61, 53.47 | 0.0374 |
| audio | 4 | 4/4 | 25.64, 50.53, 74.05, 97.90 | 0.0409 |
| text | 1 | 1/1 | 30.72 | 0.0326 |
| text | 2 | 1/2 | 2.82 (no preset), 20.88 | 0.0479 |

Seven audio calls succeeded with one execution attempt each. Of three text calls, two generated verified artifacts and one returned state=chat: “I couldn't complete the request plan. Please try again; nothing was changed.” Its durable record shows remote_submission_possible=false and no snapshot. Text concurrency 4 was therefore not run. This identifies a planning-stage failure, not evidence of a Vast worker capacity failure. No small-sample P95 is reported.

The audio concurrency-4 jobs began executing near 1.2, 25.5, 50.3 and 73.7 seconds and completed near 25.3, 50.2, 73.8 and 97.7 seconds. Throughput remained approximately one result every 24–28 seconds. Current audio admission is 1 and its Vast adapter execution lock is serial. More client concurrency increased queue delay, not independent GPU execution.

## Settings and recommendation

- Keep production FIFO, batch_size=1 and SYNTHX_JOBS_MAX_INFLIGHT=1 for this single serial audio executor pending a separate replica-capacity experiment. No live settings were changed by this work.
- /status adds actual registered/ready routing adapters and ready_by_kind, plus durable audio active/max_inflight. config.num_workers belongs to local CPU simulation. An adapter count is not proof of independent model replicas or physical GPUs.
- POST /config/jobs accepts {"max_inflight": 1..32} using the existing INFERENCE_SERVICE_TOKEN bearer authentication. It changes future durable audio claims only; reducing below current active work retains all leases and lets active work drain. It does not regulate text tasks. Values are process-local; set SYNTHX_JOBS_MAX_INFLIGHT for restart persistence.
- More real audio capacity requires independently runnable model replicas with distinct execution capacity and correct capability registration. Duplicating adapter entries pointing at the same locked model does not provide this.
- Before another real text load run, investigate planning failures. Do not describe a state=chat response as a successful generated preset.
