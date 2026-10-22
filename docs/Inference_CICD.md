# Inference platform CI/CD

The pipeline owns **only the EC2 gateway and EC2 text-worker adapter**. Synth_X's
pipeline owns the business backend and database migrations. Vast model services,
training, SSH tunnel and GPU assets stay outside this deployment.

## Triggers and checks

- PR: isolated PostgreSQL 17 regression tests, linux/amd64 image build, image smoke.
- Main push: repeat the checks and export that tested image as an Actions artifact.
  Never rebuild on EC2.
- CD is disabled unless `INFERENCE_DEPLOY_ENABLED=true`. Initially keep
  `INFERENCE_AUTO_DEPLOY` unset: deployment then requires manual workflow dispatch
  on main. Automatic main releases require a later explicit opt-in.
- Dedicated secret: `INFERENCE_EC2_DEPLOY_KEY`, installed only after host setup
  approval. Do not reuse the backend deployment key.

Keep both deploy variables unset until the first-rollout review. A green PR means
CI passed, **not** that production was deployed.

## Normal release transaction

1. Transfer the image and SHA-256 through SSH with the pinned EC2 host key.
   Forced receiver accepts only `deploy-platform <40-hex SHA> <64-hex hash>`.
2. Acquire Synth_X backend's existing host deployment lock. Verify archive hash,
   expected tag, latest origin/main SHA, image revision and amd64 architecture.
   Export sources with `git archive`, preserving the host checkout.
3. Check running image pins and unchanged production env file. Save a recovery
   journal before maintenance. Busy jobs cause exit 75 without stopping services;
   Actions retries at most three times, 30 seconds apart.
4. Fence backend mutations with its existing protocol, then wait for its admitted
   requests, operations and SQL jobs. The business backend stays running.
5. Fence gateway mutations with `gateway-drain`. Keep worker admission open while
   admitted gateway handlers, API queue and worker RPC queues finish.
6. Fence the adapter with a separate `worker-drain`. Wait for actual server
   handlers, including those whose caller already disconnected. Both processes
   must acknowledge the owning maintenance token.
7. Recreate only gateway and text-worker with `--no-deps --no-build`, retaining
   `deploy_default`, gateway binding `127.0.0.1:8100`, worker 73/audio and 122/text,
   and env-configured job concurrency (default one).
8. Check image IDs, gateway readiness, maintenance acknowledgments, Vast audio
   gRPC, adapter gRPC and Vast text health. No model/LLM generation is submitted.
9. Atomically update shared backend dependency pins and platform state under the
   same lock. Open worker, gateway, then backend admission; remove the journal.

After a failure that stopped services, restore both previous platform images and
shared dependency pins before reopening. If rollback fails, retain maintenance
and the journal for operator recovery. Drain timeout leaves old services running.
Complete tokens are published atomically and exclusively; foreign markers are
never removed. The backend receiver is imported only when its SHA-256 matches the
reviewed config. A receiver change requires a compatibility review.

No platform step runs migrations, prints expanded Compose/env config, changes
backend image/release state, restarts the tunnel, or touches Vast processes/data.
This is a brief maintenance deployment, not zero-downtime blue/green deployment.

## First rollout: owner review and approved bootstrap

Existing EC2 platform images lack this maintenance protocol. `prepare_host.py`
marks adoption as `bootstrap_required=true`, and normal CD refuses it. **Do not
clear that flag to bypass the bootstrap.** Review with the owner before live work:

- Exact main SHA, successful CI/image checks, dedicated forced SSH identity.
- Recorded gateway/text image pair, backend image/pins, private bindings/network,
  and installed backend receiver hash; retain the legacy rollback configuration.
- Preserve `.env.production` and secrets. Verify worker addresses, capabilities
  and concurrency without exposing credentials.
- Agree a quiet maintenance window. Close backend admission and finish existing
  work. Ensure no direct gateway/adapter client can submit during the legacy
  transition. Old images cannot acknowledge platform fences; if activity cannot
  be ruled out, **defer** rather than assume the system is idle.
- Under the shared host lock, install the reviewed root-owned receiver/private
  state, create its control directory and use the reviewed two-service Compose
  layout with its read-only maintenance mount and separate marker paths.
- Start only the two platform services using the CI-tested image. Verify protocol,
  image IDs and transports, update shared dependency pins, and regenerate adoption
  state from the protocol-capable running pair. If validation fails, restore the
  recorded legacy pair and pins during the same maintenance window.
- Enable manual CD only after bootstrap/adoption succeeds. Automatic main deploy
  stays disabled until a later decision.

Offline host proposal command, after copying reviewed files:

```sh
python3 deploy/ci/prepare_host.py --output /tmp/inference-cicd-proposal-UNIQUE
```

This inspects Docker and writes a **new proposal directory only**. It does not
install keys, change services or Actions variables. It verifies private layout,
backend dependency pins and, for ready adoption, the read-only maintenance mount.
The proposal's Compose/images files become the rollback source only after the
operator reviews and installs the proposal at the paths recorded in its state.

## Interrupted deployment recovery

SIGTERM/SIGHUP attempts cleanup/rollback. SIGKILL, power loss or a second failure
can leave `platform-cicd/transaction.json` and maintenance markers. The next
release refuses to proceed. An operator must:

1. Acquire the shared backend `deploy.lock`; keep new traffic fenced.
2. Read the journal's previous Compose path/image pair and original backend config.
   Compare actual container IDs/images, current state and shared dependency pins.
3. Restore the recorded pair using its immutable image overrides and
   `--no-deps --no-build` on gateway/text-worker only; restore matching shared pins.
   Do not restart backend/tunnel/Vast or run migrations.
4. Verify readiness, both platform protocols and Vast transports, plus unchanged
   backend/tunnel images and env file. Keep maintenance closed if any check fails.
5. Inspect tokens in `control/worker-drain`, `control/gateway-drain` and backend
   `/var/lib/synthx/.deploy-drain`. Remove only this transaction's matching tokens
   in worker→gateway→backend order. Finish/adopt the consistent state and archive
   the resolved journal before another release.

Never blindly delete a marker or journal to retry. The pre-maintenance journal
retains original config and release paths; marker contents identify the owning
maintenance window. The first live release must be supervised.

## Local verification

```sh
python -m pip install -r requirements-ci.txt
SYNTHX_TEST_DATABASE_URL=postgresql://synthx_test:ci-only@127.0.0.1:5432/synthx_platform_ci \
  python deploy/ci/run_tests.py
python -m pip freeze > .ci-constraints.txt
docker build --platform linux/amd64 --build-arg REVISION="$(git rev-parse HEAD)" \
  -t "synthx-platform:$(git rev-parse HEAD)" -f deploy/ci/Dockerfile.platform .
python deploy/ci/smoke_image.py "synthx-platform:$(git rev-parse HEAD)"
```

Only use a disposable loopback database named `synthx_platform_ci*`. The runner
rejects remote/general database URLs and fails on skipped tests. `schema.sql` is
a test fixture, never a production migration. Production image dependencies are
constrained to the versions tested in CI. Image smoke uses CPU simulation,
API/metrics, maintenance rejection, and text adapter imports without contacting
Vast. GPU performance and preset quality need separate real-model testing.


## Text v1.2.3 only: ordinary retrieval and independent MFCC

Ordinary requests must pin `model=text_v1.2.3`,
`pipeline_version=text_v1.2.3`, capability `synthx_text_v123`, and a target engine.
`TEXT_SERVICE_URL=http://vast-tunnel:18083` is the sole ordinary retrieval origin.
Ordinary `text_v1.2.2`, `text_v1.2` and `text_v1` requests are rejected before queueing.
No request falls back to an older retrieval model.

MFCC anchor retrieval is a different operation, not compatibility:
`model=pipeline_version=text_v1.2_refinement`, capability
`synthx_text_refinement`, and `TEXT_REFINEMENT_SERVICE_URL=http://vast-tunnel:18080`.
It requires `retrieve_similar`, a bank candidate identity and verified native/audio
hashes. Its shared CLAP scorer may report `text_v1.2`; the scheduled task envelope
remains `text_v1.2_refinement`. Never relabel a scorer response as ordinary123.
Both capabilities share one adapter slot. Worker ID122 is an opaque existing
identity used by metrics and maintenance, not a supported model version.

### Coordinated first cutover is required

The existing product backend still sends ordinary122. Do not install this strict
platform alone and reopen that backend: text generation would stop working.
The product and platform receivers require reviewed prerequisites in opposite
orders for this first incompatible transition. A coordinated maintenance plan
must hold admission closed across both image changes, validate both chains,
and restore both releases together on failure. This PR does not implement or
execute that first joint rollout.

As an additional fail-closed gate, GitHub deployment requires
`INFERENCE_TEXT_V123_CUTOVER_READY=true`; leave it unset until the joint cutover
has been completed and verified. The updated receiver also checks that the
running product pins123 before loading/stopping anything. Install reviewed
receiver changes before re-enabling normal CI/CD. The existing installed receiver
is not automatically replaced by a repository push.

Keep both private SSH forwards18083 and18080, audio50061, the current host key,
keys, Docker network and loopback bindings. Platform CI/CD never recreates the
tunnel or changes Vast training. Product details and remaining SQL/auth/conversion
release blockers are in Synth_X `docs/Text_V123_Deployment.md`.

A rollback may restore a whole earlier release during maintenance. Its health
probe is operational recovery; it does not enable automatic ordinary122 requests
in the strict release. Do not set an ordinary122 pipeline variable as a fallback.
