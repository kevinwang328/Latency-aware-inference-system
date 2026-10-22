"""Real HTTP → gRPC version routing, rejecting every retired text version."""
import json
import threading
import unittest
from concurrent import futures
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

import grpc
import httpx
from system import api_server, inference_pb2_grpc
from system.grpc_worker_server import WorkerServicer
from system.grpc_worker_pool import GrpcWorkerPool
from system.queue import TaskQueue
from system.scheduler import Scheduler
from system.worker_text import TextWorker


class TextV123GatewayTests(unittest.IsolatedAsyncioTestCase):
    async def test_v123_routes_to_its_origin_and_rejects_mismatched_versions(self):
        calls = []
        answer_model = ['text_v1.2.3']
        class HTTP(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                calls.append((self.path, body))
                self.send_response(200); self.end_headers()
                result = {'model':answer_model[0]}
                if self.path.endswith('retrieve'):
                    result.update(primary_candidate_id='vital:a',candidates=[{'candidate_id':'vital:a'}])
                else:
                    result.update(use_clap=True,ranked_candidates=[{'candidate_id':'vital:a','clap_similarity':.4}])
                self.wfile.write(json.dumps(result).encode())
        http = ThreadingHTTPServer(('127.0.0.1',0), HTTP)
        threading.Thread(target=http.serve_forever,daemon=True).start()
        self.addCleanup(http.server_close); self.addCleanup(http.shutdown)
        url = 'http://127.0.0.1:%d' % http.server_port
        refinement_calls=[]
        class RefinementHTTP(HTTP):
            def do_POST(self):
                body=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                refinement_calls.append((self.path,body))
                self.send_response(200);self.end_headers()
                self.wfile.write(json.dumps({'model':'text_v1.2_refinement' if self.path.endswith('retrieve') else 'text_v1.2','use_clap':True,'ranked_candidates':[]}).encode())
        refinement_http=ThreadingHTTPServer(('127.0.0.1',0),RefinementHTTP)
        threading.Thread(target=refinement_http.serve_forever,daemon=True).start()
        self.addCleanup(refinement_http.server_close);self.addCleanup(refinement_http.shutdown)
        worker = TextWorker(url,refinement_url='http://127.0.0.1:%d'%refinement_http.server_port)
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        inference_pb2_grpc.add_WorkerServiceServicer_to_server(WorkerServicer(122,worker),server)
        port=server.add_insecure_port('127.0.0.1:0'); server.start()
        self.addCleanup(lambda:server.stop(0).wait())
        with patch.dict('os.environ',{'WORKER_CAPABILITIES':'{"text":["synthx_text_v123","synthx_text_refinement"]}'}):
            pool=GrpcWorkerPool(static_workers={'text':'127.0.0.1:%d'%port});pool.start();self.addCleanup(pool.stop)
            queue=TaskQueue(maxsize=4); scheduler=Scheduler(queue,pool.submit);scheduler.start();self.addCleanup(scheduler.stop)
            with patch.multiple(api_server,_task_queue=queue,_job_gateway=object(),_jobs_token='test-only',create=True):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=api_server.app),base_url='http://test') as client:
                    headers={'Authorization':'Bearer test-only'}
                    body={'model':'text_v1.2.3','operation':'retrieve','payload':{'pipeline_version':'text_v1.2.3','description':'warm pad','target_engine':'vital'}}
                    self.assertEqual((await client.post('/text/tasks',json=body)).status_code,401)
                    result=await client.post('/text/tasks',json=body,headers=headers)
                    self.assertEqual(result.status_code,200,result.text)
                    self.assertEqual(result.json()['model'],'text_v1.2.3')
                    self.assertEqual(calls,[('/text-v1/retrieve',body['payload'])])
                    body['payload']['pipeline_version']='text_v1.2_refinement'
                    result=await client.post('/text/tasks',json=body,headers=headers)
                    self.assertEqual(result.status_code,422)
                    self.assertEqual(len(calls),1)
                    body['payload']['pipeline_version']='text_v1.2.3'
                    answer_model[0]='text_v1.2.2'
                    result=await client.post('/text/tasks',json=body,headers=headers)
                    self.assertEqual(result.status_code,502)
                    body.update(operation='rerank',payload={'candidate_ids':['vital:a'],'caption':'pad'})
                    result=await client.post('/text/tasks',json=body,headers=headers)
                    self.assertEqual(result.status_code,502)
                    answer_model[0]='text_v1.2.3'
                    result=await client.post('/text/tasks',json=body,headers=headers)
                    self.assertEqual(result.status_code,200,result.text)
                    # Independent MFCC retrieval must use its own identity and origin.
                    prior_calls=len(calls)
                    body={'model':'text_v1.2_refinement','operation':'retrieve','payload':{
                        'pipeline_version':'text_v1.2_refinement','operation':'retrieve_similar',
                        'target_engine':'vital','anchor_candidate_id':'vital:a',
                        'anchor_native_sha256':'a'*64,'anchor_audio_sha256':'b'*64}}
                    result=await client.post('/text/tasks',json=body,headers=headers)
                    self.assertEqual(result.status_code,200,result.text)
                    self.assertEqual(result.json()['model'],'text_v1.2_refinement')
                    self.assertEqual(refinement_calls,[('/text-v1/retrieve',body['payload'])])
                    self.assertEqual(len(calls),prior_calls)
                    body['model']='text_v1.2.2'
                    self.assertEqual((await client.post('/text/tasks',json=body,headers=headers)).status_code,422)
                    self.assertEqual(len(refinement_calls),1)
                    body={'model':'text_v1.2_refinement','operation':'rerank','payload':{'candidate_ids':['vital:a'],'caption':'pad'}}
                    result=await client.post('/text/tasks',json=body,headers=headers)
                    self.assertEqual(result.status_code,200,result.text)
                    self.assertEqual(result.json()['result']['model'],'text_v1.2')

    async def test_invalid_origin_fails_at_startup(self):
        with self.assertRaises(ValueError):
            TextWorker('')
