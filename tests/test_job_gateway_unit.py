"""Verify scheduling behavior with a database boundary double and real queue/tasks."""
import asyncio
import hashlib
import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import httpx

from system.queue import TaskQueue


class MemoryStore:
    """Only the SQL boundary is replaced; task and queue state remain real."""
    def __init__(self):
        spec = {"job_id": "job", "model": "v7_3_full"}
        self.row = {"job_id": "job", "spec": spec,
                    "spec_sha256": hashlib.sha256(b'{"job_id":"job","model":"v7_3_full"}').hexdigest(),
                    "attempt_id": "attempt", "status": "queued", "attempts": 0,
                    "deadline_at": datetime.now(timezone.utc) + timedelta(seconds=3600)}

    def get(self, job_id):
        return dict(self.row) if job_id == "job" else None

    def claim(self, job_id=None, lease_seconds=120):
        if self.row["status"] != "queued":
            return None
        self.row.update(status="running", attempts=1)
        return dict(self.row)

    def expire_deadlines(self):
        pass

    def renew(self, job_id, attempt_id, lease_seconds):
        return True

    def complete(self, job_id, attempt_id, result=None, error=None):
        self.row.update(status="succeeded" if error is None else "failed", result=result, error=error)
        return True

    def defer(self, job_id, attempt_id, error, retry_seconds):
        self.row.update(status="queued", error=error)
        return True


class JobGatewayUnitTests(unittest.TestCase):
    def test_completion_flows_from_queue_task_back_to_durable_state(self):
        from system.job_gateway import JobGateway
        store, queue = MemoryStore(), TaskQueue(maxsize=1)
        gateway = JobGateway(store, queue)
        gateway.submit("job")
        task = queue.get(timeout=0)
        self.assertEqual(task.input_data, {"kind": "synthx_v73", "spec": {"job_id": "job", "model": "v7_3_full"}, "attempt_id": "attempt"})
        task.mark_done({"answer": 42})
        gateway.poll()
        self.assertEqual(store.row["status"], "succeeded")
        self.assertEqual(store.row["result"], {"answer": 42})

    def test_disabled_jobs_are_unavailable_without_database_or_token(self):
        from system import api_server
        async def request():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=api_server.app), base_url="http://test") as client:
                response = await client.post("/jobs", json={"job_id": "a502c1b4-5067-40ae-a029-cea8e5d4337e"})
                self.assertEqual(response.status_code, 503)
        with patch.object(api_server, "_job_gateway", None), patch.object(api_server, "_jobs_token", None):
            asyncio.run(request())

    def test_invalid_admission_configuration_does_not_start_workers(self):
        from system import api_server
        from system.config import config
        original_batch = config.batch_size
        self.addCleanup(lambda: config.update(batch_size=original_batch))
        class Pool:
            def start(self):
                raise AssertionError("workers started before validation")
        async def startup():
            async with api_server.lifespan(api_server.app):
                self.fail("invalid configuration started")
        with patch.object(api_server, "JOBS_ENABLED", True), patch.object(api_server, "USE_GRPC", False), \
             patch.object(api_server, "WorkerPool", Pool), \
             patch("system.job_store.JobStore", lambda _: MemoryStore()), \
             patch.dict(os.environ, {"DATABASE_URL": "test-only", "INFERENCE_SERVICE_TOKEN": "test-only", "SYNTHX_JOBS_MAX_INFLIGHT": "0"}):
            with self.assertRaises(ValueError):
                asyncio.run(startup())


if __name__ == "__main__":
    unittest.main()
