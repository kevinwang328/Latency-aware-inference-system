"""Version pinning and RPC/HTTP boundaries for the text adapter."""
from concurrent import futures
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import time
import unittest
from unittest.mock import patch
import grpc
from system import inference_pb2, inference_pb2_grpc
from system.grpc_worker_server import WorkerServicer
from system.grpc_worker_pool import GrpcWorkerPool
from system.task import Task, TaskStatus

class TextWorkerTests(unittest.TestCase):
    def setUp(self):
        self.calls = []
        self.http_status = 200
        self.delay = 0
        self.raw = None
        self.entered = threading.Event()
        self.release = None
        self.answer = {"model":"text_v1.2.3", "primary_candidate_id":"vital:a", "candidates":[{"candidate_id":"vital:a"}]}
        outer = self
        class HTTP(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                outer.calls.append((self.path, body))
                outer.entered.set()
                if outer.release is not None: outer.release.wait(2)
                time.sleep(outer.delay)
                self.send_response(outer.http_status); self.end_headers()
                try: self.wfile.write(outer.raw if outer.raw is not None else json.dumps(outer.answer).encode())
                except BrokenPipeError: pass
        self.http = ThreadingHTTPServer(('127.0.0.1',0), HTTP)
        self.thread = threading.Thread(target=self.http.serve_forever, daemon=True); self.thread.start()
        self.addCleanup(self.http.server_close); self.addCleanup(self.http.shutdown)
        self.url = 'http://127.0.0.1:%d' % self.http.server_port

    def worker(self):
        from system.worker_text import TextWorker
        return TextWorker(self.url)

    def payload(self):
        return {"kind":"synthx_text_v123", "model":"text_v1.2.3", "operation":"retrieve", "payload":{"pipeline_version":"text_v1.2.3", "description":"warm pad", "target_engine":"vital"}}

    def test_real_grpc_adapter_calls_versioned_http_and_preserves_result(self):
        server=grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        inference_pb2_grpc.add_WorkerServiceServicer_to_server(WorkerServicer(122,self.worker()),server)
        port=server.add_insecure_port('127.0.0.1:0'); server.start()
        self.addCleanup(lambda: server.stop(0).wait())
        with grpc.insecure_channel('127.0.0.1:%d'%port) as channel:
            result=inference_pb2_grpc.WorkerServiceStub(channel).ProcessBatch(inference_pb2.BatchRequest(tasks=[inference_pb2.TaskRequest(task_id='one',input_json=json.dumps(self.payload()),remaining_seconds=10)]),timeout=10).results[0]
        self.assertTrue(result.success,result.error)
        self.assertEqual(json.loads(result.result_json)['result'],self.answer)
        self.assertEqual(self.calls,[('/text-v1/retrieve',self.payload()['payload'])])

    def test_wrong_version_and_arbitrary_operation_never_reach_http(self):
        from system.worker_v73 import WorkerError
        worker=self.worker()
        for field,value in [('model','text_v1.2'),('operation','refresh'),('kind','synthx_v73')]:
            data=self.payload(); data[field]=value
            with self.subTest(field=field),self.assertRaises(WorkerError): worker.process(data,None,deadline=time.monotonic()+5)
        for version in ['text_v1', [], {}]:
            data=self.payload(); data['payload']['pipeline_version']=version
            with self.subTest(version=version),self.assertRaises(WorkerError): worker.process(data,None,deadline=time.monotonic()+5)
        self.assertEqual(self.calls,[])

    def test_expired_or_cancelled_task_never_runs(self):
        from system.worker_v73 import WorkerError
        worker=self.worker()
        with self.assertRaises(WorkerError) as error: worker.process(self.payload(),None,deadline=time.monotonic()-1)
        self.assertEqual(error.exception.code,'deadline_exceeded')
        class Context:
            def is_active(self): return False
        with self.assertRaises(WorkerError): worker.process(self.payload(),Context(),deadline=time.monotonic()+5)
        self.assertEqual(self.calls,[])

    def test_service_downgrade_is_rejected(self):
        from system.worker_v73 import WorkerError
        self.answer['model']='text_v1.2'
        with self.assertRaises(WorkerError) as error: self.worker().process(self.payload(),None,deadline=time.monotonic()+5)
        self.assertEqual(error.exception.code,'invalid_result')

    def test_rerank_preserves_clap_order_and_current_version(self):
        data=self.payload(); data['operation']='rerank'; data['payload']={'candidate_ids':['vital:a'],'caption':'pad'}
        self.answer={'model':'text_v1.2.3','use_clap':True,'ranked_candidates':[{'candidate_id':'vital:a','clap_similarity':.42}]}
        result=self.worker().process(data,None,deadline=time.monotonic()+5)
        self.assertEqual(result['model'],'text_v1.2.3')
        self.assertEqual(result['result']['ranked_candidates'],[{'candidate_id':'vital:a','clap_similarity':.42}])
        self.assertEqual(self.calls[0][0],'/text-v1/rerank')

    def test_upstream_errors_and_malformed_results_are_explicit(self):
        from system.worker_v73 import WorkerError
        worker=self.worker()
        for status,raw,want in [(422,b'bad','invalid_spec'),(503,b'busy','overloaded'),(200,b'not-json','invalid_result'),(200,b'[]','invalid_result'),(200,b'{"score":NaN}','invalid_result')]:
            self.http_status=status; self.raw=raw
            with self.subTest(status=status,raw=raw), self.assertRaises(WorkerError) as error: worker.process(self.payload(),None,deadline=time.monotonic()+3)
            self.assertEqual(error.exception.code,want)

    def test_busy_worker_rejects_second_request_without_parallel_gpu_calls(self):
        from system.worker_v73 import WorkerError
        worker=self.worker(); self.release=threading.Event()
        with futures.ThreadPoolExecutor(max_workers=1) as executor:
            pending=executor.submit(worker.process,self.payload(),None,deadline=time.monotonic()+5)
            try:
                self.assertTrue(self.entered.wait(1))
                with self.assertRaises(WorkerError) as error: worker.process(self.payload(),None,deadline=time.monotonic()+5)
                self.assertEqual(error.exception.code,'busy')
            finally: self.release.set()
            self.assertEqual(pending.result()['model'],'text_v1.2.3')
        self.assertEqual(len(self.calls),1)

    def test_deadline_bounds_slow_http_without_returning_late_success(self):
        from system.worker_v73 import WorkerError
        self.delay=.2
        with self.assertRaises(WorkerError) as error: self.worker().process(self.payload(),None,deadline=time.monotonic()+.05)
        self.assertEqual(error.exception.code,'deadline_exceeded')

class CapabilityRoutingTests(unittest.TestCase):
    def test_mixed_batch_routes_each_task_only_to_compatible_worker(self):
        class Echo(inference_pb2_grpc.WorkerServiceServicer):
            def __init__(self,name): self.name=name
            def ProcessBatch(self,request,context):
                return inference_pb2.BatchResponse(results=[inference_pb2.TaskResult(task_id=t.task_id,success=True,result_json=json.dumps({'worker':self.name})) for t in request.tasks])
        addresses={}
        for name in ['audio','text']:
            server=grpc.server(futures.ThreadPoolExecutor(max_workers=1))
            inference_pb2_grpc.add_WorkerServiceServicer_to_server(Echo(name),server)
            port=server.add_insecure_port('127.0.0.1:0'); server.start()
            self.addCleanup(lambda s=server:s.stop(0).wait())
            addresses[name]='127.0.0.1:%d'%port
        with patch.dict('os.environ',{'WORKER_CAPABILITIES':json.dumps({'audio':['synthx_v73'],'text':['synthx_text_v123']})}):
            pool=GrpcWorkerPool(static_workers=addresses); pool.start(); self.addCleanup(pool.stop)
            tasks=[Task(input_data={'kind':'synthx_v73'},deadline=time.monotonic()+3),Task(input_data={'kind':'synthx_text_v123'},deadline=time.monotonic()+3)]
            pool.submit(tasks)
            until=time.monotonic()+3
            while any(t.status not in (TaskStatus.DONE,TaskStatus.FAILED) for t in tasks) and time.monotonic()<until: time.sleep(.01)
            self.assertEqual([t.result for t in tasks],[{'worker':'audio'},{'worker':'text'}])
            pool._workers['text'].set_routing_state('DRAINING')
            unavailable=Task(input_data={'kind':'synthx_text_v123'})
            pool.submit([unavailable])
            self.assertEqual(unavailable.status,TaskStatus.FAILED)
            self.assertEqual(unavailable.error_code,'overloaded')
            self.assertFalse(pool.try_resubmit(unavailable,'text'))
