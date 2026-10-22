"""Exercise authentication and dispatch through the actual ASGI endpoint."""
import asyncio
import json
import unittest
from concurrent import futures
from unittest.mock import patch
import grpc
import httpx
from system import api_server, inference_pb2, inference_pb2_grpc
from system.queue import TaskQueue
from system.scheduler import Scheduler
from system.grpc_worker_pool import GrpcWorkerPool

class TextGatewayTests(unittest.IsolatedAsyncioTestCase):
    async def test_authenticated_text_stage_is_scheduled_and_unknown_versions_rejected(self):
        class Echo(inference_pb2_grpc.WorkerServiceServicer):
            def ProcessBatch(self,request,context):
                return inference_pb2.BatchResponse(results=[inference_pb2.TaskResult(task_id=t.task_id,success=True,result_json=json.dumps({'model':'text_v1.2.3','operation':'retrieve','result':{'model':'text_v1.2.3','candidates':[{'candidate_id':'vital:one'}]}})) for t in request.tasks])
        server=grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        inference_pb2_grpc.add_WorkerServiceServicer_to_server(Echo(),server)
        port=server.add_insecure_port('127.0.0.1:0'); server.start()
        self.addCleanup(lambda:server.stop(0).wait())
        with patch.dict('os.environ',{'WORKER_CAPABILITIES':'{"text":["synthx_text_v123"]}'}):
            pool=GrpcWorkerPool(static_workers={'text':'127.0.0.1:%d'%port}); pool.start(); self.addCleanup(pool.stop)
            queue=TaskQueue(maxsize=4); scheduler=Scheduler(queue,pool.submit); scheduler.start(); self.addCleanup(scheduler.stop)
            with patch.multiple(api_server,_task_queue=queue,_job_gateway=object(),_jobs_token='private-test',create=True):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=api_server.app),base_url='http://test') as client:
                    body={'model':'text_v1.2.3','operation':'retrieve','payload':{'description':'warm pad','pipeline_version':'text_v1.2.3'}}
                    unauth=await client.post('/text/tasks',json=body)
                    self.assertEqual(unauth.status_code,401)
                    result=await client.post('/text/tasks',json=body,headers={'Authorization':'Bearer private-test'})
                    self.assertEqual(result.status_code,200,result.text)
                    self.assertEqual(result.json()['result']['candidates'],[{'candidate_id':'vital:one'}])
                    body['model']='text_v1.2'
                    wrong=await client.post('/text/tasks',json=body,headers={'Authorization':'Bearer private-test'})
                    self.assertEqual(wrong.status_code,422)
                    body['model']='text_v1.2.3'; body['operation']='refresh'
                    wrong=await client.post('/text/tasks',json=body,headers={'Authorization':'Bearer private-test'})
                    self.assertEqual(wrong.status_code,422)

                    body['operation']='retrieve'
                    for invalid in [[],{}]:
                        body['payload']['pipeline_version']=invalid
                        wrong=await client.post('/text/tasks',json=body,headers={'Authorization':'Bearer private-test'})
                        self.assertEqual(wrong.status_code,422)
