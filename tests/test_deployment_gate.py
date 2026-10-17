import asyncio
import json
from pathlib import Path
import tempfile
import threading
import unittest
import urllib.request
from fastapi import FastAPI
import httpx
from system.deployment import DeploymentGate, DeploymentMiddleware, start_worker_health
from system.grpc_worker_server import WorkerServicer
from system import inference_pb2 as pb


class GateTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.marker=Path(self.tmp.name)/'drain';self.gate=DeploymentGate(self.marker)
    def test_gateway_draining_rejects_new_work_but_keeps_health_reads(self):
        app=FastAPI();app.add_middleware(DeploymentMiddleware,gate=self.gate)
        @app.post('/text/tasks')
        async def work():return {'ran':True}
        @app.get('/health/deployment')
        async def health():return self.gate.snapshot()
        self.marker.write_text('mine')
        async def check():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
                reply=await client.post('/text/tasks',json={})
                self.assertEqual(reply.status_code,503)
                state=(await client.get('/health/deployment')).json()
                self.assertEqual(state['drain_id'],'mine');self.assertEqual(state['active'],0)
        asyncio.run(check())
    def test_worker_counts_processing_after_caller_cancels_until_handler_really_returns(self):
        started=threading.Event();finish=threading.Event()
        class Backend:
            def process(self,data,context,deadline=None):
                started.set();finish.wait(2);return {'result':'done'}
        class Context:
            def is_active(self):return False
        worker=WorkerServicer(122,Backend(),deployment_gate=self.gate)
        request=pb.BatchRequest(tasks=[pb.TaskRequest(task_id='task',input_json='{}')])
        thread=threading.Thread(target=lambda:worker.ProcessBatch(request,Context()));thread.start()
        try:
            self.assertTrue(started.wait(1));self.marker.write_text('maintenance')
            self.assertEqual(self.gate.snapshot()['active'],1)
            rejected=worker.ProcessBatch(request,Context())
            self.assertFalse(rejected.results[0].success);self.assertEqual(rejected.results[0].error_code,'worker_busy')
        finally:finish.set();thread.join()
        self.assertEqual(self.gate.snapshot()['active'],0)
    def test_worker_health_reports_same_process_gate_over_loopback(self):
        health=start_worker_health(self.gate,0)
        try:
            self.gate.enter();self.marker.write_text('maintenance')
            with urllib.request.urlopen('http://127.0.0.1:'+str(health.server_port)+'/health/deployment',timeout=2) as response:
                state=json.load(response)
            self.assertEqual(state,{'protocol':1,'draining':True,'drain_id':'maintenance','active':1})
        finally:self.gate.leave();health.shutdown();health.server_close()

if __name__=='__main__':unittest.main()
