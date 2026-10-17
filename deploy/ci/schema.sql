-- Disposable CI fixture, never a production migration. The business backend owns schema migrations.
CREATE SCHEMA synthx;
CREATE TABLE synthx.schema_migrations(version INTEGER PRIMARY KEY);
CREATE TABLE synthx.projects(id TEXT PRIMARY KEY,user_id TEXT NOT NULL,title TEXT NOT NULL,created_at TIMESTAMPTZ NOT NULL);
CREATE TABLE synthx.snapshots(project_id TEXT REFERENCES synthx.projects(id),id TEXT,PRIMARY KEY(project_id,id));
CREATE TABLE synthx.inference_jobs (
    job_id UUID PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES synthx.projects(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL,
    model TEXT NOT NULL CHECK (model = 'v7_3_full'),
    spec JSONB NOT NULL CHECK (jsonb_typeof(spec) = 'object'),
    spec_sha256 TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued','running','succeeded','failed')),
    attempt_id UUID,
    attempts INTEGER NOT NULL DEFAULT 0,
    lease_until TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deadline_at TIMESTAMPTZ NOT NULL,
    result JSONB,
    error JSONB,
    snapshot_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (project_id, snapshot_id) REFERENCES synthx.snapshots(project_id, id)
        DEFERRABLE INITIALLY DEFERRED
);
CREATE INDEX inference_jobs_due ON synthx.inference_jobs(status, next_attempt_at, lease_until);
CREATE INDEX inference_jobs_project ON synthx.inference_jobs(project_id, created_at DESC);
REVOKE ALL ON synthx.inference_jobs FROM PUBLIC;
ALTER TABLE synthx.inference_jobs ENABLE ROW LEVEL SECURITY;
INSERT INTO synthx.schema_migrations(version) VALUES (2);
