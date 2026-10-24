# Inference platform CI/CD

The pipeline owns **only the EC2 gateway and EC2 text-worker adapter**. Synth_X's
pipeline owns the business backend and database migrations. Vast model services,
training, SSH tunnel and GPU assets stay outside this deployment.

## Triggers and checks

- PR: isolated PostgreSQL 17 regression tests, linux/amd64 image build, image smoke.
- Main push: repeat the checks and export the tested image as an Actions artifact,
  then automatically deploy **staging** when `INFERENCE_STAGING_DEPLOY_ENABLED=true`.
  Never rebuild on EC2.
- Manual `workflow_dispatch` selects `target=staging|dev`, defaulting to staging.
  Dev requires `INFERENCE_DEPLOY_ENABLED=true` and
  `INFERENCE_TEXT_V123_CUTOVER_READY=true`. No main push deploys dev;
  `INFERENCE_AUTO_DEPLOY` is no longer consumed.
- Reuse the dedicated platform secret `INFERENCE_EC2_DEPLOY_KEY` for the restricted
  two-command dispatcher. The backend deployment key remains separate.
- Keep the staging deploy variable disabled until the isolated stack has been
  bootstrapped, adopted and tested. The variable does not perform first setup.
- The always-running main release-status job fails if tests, image, or deployment
  did not succeed, including a disabled or skipped deployment. A green test job
  alone is not a confirmed release.

## Fixed environment identities

| Boundary | Staging | Dev (legacy default) |
| --- | --- | --- |
| SSH command | `deploy-platform-staging SHA SHA256` | `deploy-platform SHA SHA256` |
| Receiver config | `/home/ubuntu/services/.synthx/staging-platform-cicd/config.json` | `/home/ubuntu/services/.synthx/platform-cicd/config.json` |
| Backend config / shared lock directory | `/home/ubuntu/services/.synthx/staging-cicd` | `/home/ubuntu/services/.synthx/cicd` |
| Compose project | `synthx-staging-platform` | `deploy` |
| External network | `synthx-staging_default` | `deploy_default` |
| Gateway loopback port | `18100` | `8100` |
| Model DNS | `model-tunnel` | `vast-tunnel` |
| Protected tunnel container | `synthx-staging-models-model-tunnel-1` | `deploy-vast-tunnel-1` |

The receiver accepts only `environment=staging|dev`; a missing environment retains
legacy dev behavior. Command/config mismatches, mixed backend/platform environments,
and cross-environment state/config paths fail before acquiring a deployment lock.
The platform gateway and adapter are `synthx-staging-platform-gateway-1` and
`synthx-staging-platform-text-worker-1`; backend is `synthx-staging-backend-1`.

Staging gateway consumes root-owned mode-600
`/opt/synthx-staging/.env.staging.platform`, containing **only** `DATABASE_URL` and
`INFERENCE_SERVICE_TOKEN`. Use the restricted staging database principal and the
same strong staging token as backend `/opt/synthx-staging/.env.staging`; never copy
the dev token, authentication, LLM, storage, or admin credentials into this file.
The receiver uses `sudo -n sha256sum` to verify the root-owned platform environment
and backend environment without reading their plaintext. Its narrowly scoped sudo
policy must permit that exact command for those two fixed paths as well as the existing approved Docker operations.

Docker Compose 2.30+ is required for the staging raw environment file. The
`!override` directive replaces the base env-file parser, and CLI interpolation
uses `/dev/null`. Compose loads `compose.platform.staging.yml` with its gateway-only read-only CA
mount from `/opt/synthx-staging/deploy/certs/supabase-prod-ca-2021.crt` to
`/etc/synthx-staging/supabase-prod-ca-2021.crt`. The platform DSN must exactly match the validated backend DSN. Compose injects
`PGSSLROOTCERT` with that container CA path and `PGSSLMODE=verify-full`; neither
TLS setting belongs in the two-key platform file. Before preparing or starting a release, the receiver invokes the backend staging
validator with `--platform-env` under the fixed root-capable staging Python
runtime. The validator accepts exactly the two gateway keys, requires both to
match the validated backend environment, and emits only a sanitized attestation.
The validator runs from the recorded backend release source at
`staging-cicd/releases/<sha>/source`, after checking its exact Compose path,
revision marker, and running backend image. Bootstrap must establish this record
and include a validator that supports `--platform-env` before enabling CI.

Both services mount the selected platform state `control` directory
read-only. Staging validation rejects other mounts, capability additions, host
namespaces and container-name overrides. Gateway is limited to one CPU/1 GiB;
adapter to 0.5 CPU/512 MiB. Both drop capabilities, disallow privilege escalation,
and the adapter publishes no host port.

Only text scheduling and model transport connectivity are in this rollout scope.
The shared audio worker has its existing storage configuration; a healthy private
audio gRPC connection does not demonstrate end-to-end staging audio isolation.
Keep backend remote audio inference disabled until that separate boundary is ready.

## Normal release transaction

1. Transfer the image and SHA-256 through SSH with the pinned EC2 host key.
   Forced receiver accepts only the two fixed commands above with 40/64 lowercase hex arguments.
2. Acquire Synth_X backend's existing host deployment lock. Verify archive hash,
   expected tag, latest origin/main SHA, image revision and amd64 architecture.
   Export sources with `git archive`, preserving the host checkout.
3. Check running image pins and unchanged selected gateway env file. Save a recovery
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
   the selected external network and loopback gateway binding, worker 73/audio and 122/text,
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
- After staging bootstrap/adoption succeeds, enable `INFERENCE_STAGING_DEPLOY_ENABLED`
  for automatic main releases. Dev remains manual behind its existing cutover gates.

Offline host proposal command, after copying reviewed files:

```sh
python3 deploy/ci/prepare_host.py --environment staging --output /tmp/inference-cicd-proposal-UNIQUE
```

`--environment dev` selects the legacy dev stack. The default is staging.
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

For manual dev deployment, GitHub requires `INFERENCE_TEXT_V123_CUTOVER_READY=true`;
leave it unset until the dev joint cutover has been completed and verified.
Staging uses its independent `INFERENCE_STAGING_DEPLOY_ENABLED` readiness gate. The updated receiver also checks that the
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
