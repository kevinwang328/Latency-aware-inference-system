import importlib.util,io,json,tempfile,threading,unittest,wave
from unittest.mock import patch
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
SPEC=importlib.util.spec_from_file_location('benchmark_capacity',ROOT/'experiments/benchmark_capacity.py')

class CapacityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(cls.module)

    def test_real_budget_is_checked_before_any_request(self):
        with self.assertRaises(ValueError):self.module.validate_real_plan([1,2,4],['audio','text'],13,False)
        with self.assertRaises(ValueError):self.module.validate_real_plan([1,2,4],['audio','text'],13,True)
        self.assertEqual(self.module.validate_real_plan([1,2,4],['audio','text'],14,True),14)

    def test_small_samples_do_not_claim_a_reliable_tail_percentile(self):
        summary=self.module.summarize([{'ok':True,'seconds':1},{'ok':True,'seconds':3},{'ok':False,'seconds':2}],4)
        self.assertEqual(summary['completed'],2);self.assertEqual(summary['failed'],1)
        self.assertEqual(summary['completion_rps'],.5);self.assertIsNone(summary['p95_seconds'])
        self.assertEqual(summary['max_seconds'],3)

    def test_accepted_audio_must_reach_terminal_success_and_valid_artifacts(self):
        polls=[]
        wav=io.BytesIO()
        with wave.open(wav,'wb') as output:
            output.setnchannels(1);output.setsampwidth(2);output.setframerate(44100);output.writeframes(b'\x00\x00'*100)

        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def do_POST(self):
                body=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                if self.path=='/projects':value={'id':'project-1'}
                else:value={'job_id':body['job_id'],'status':'queued'}
                self.send_response(202);self.end_headers();self.wfile.write(json.dumps(value).encode())
            def do_GET(self):
                if '/artifacts/' in self.path:
                    data=b'{"settings":{}}' if 'final.vital' in self.path else wav.getvalue()
                else:
                    polls.append(self.path);data=json.dumps({'status':'running' if len(polls)==1 else 'succeeded','attempts':1}).encode()
                self.send_response(200);self.end_headers();self.wfile.write(data)
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            result=self.module.real_request('http://127.0.0.1:'+str(server.server_port),'audio','owner','/uploaded.wav','pad',20,poll_interval=.01)
        finally:server.shutdown();server.server_close();thread.join()
        self.assertTrue(result['ok']);self.assertGreaterEqual(len(polls),2)
        self.assertEqual(result['terminal_status'],'succeeded');self.assertEqual(result['artifacts']['preset']['bytes'],15)

    def test_nonobject_json_is_a_reportable_validation_failure(self):
        with self.assertRaises(ValueError):self.module.verify_artifact(b'[]',True)
        with patch.object(self.module,'request',return_value=b'[]'):
            with self.assertRaises(ValueError):self.module.js('http://localhost','/chat')

    def test_header_only_or_truncated_preview_is_not_success(self):
        with self.assertRaises(ValueError):self.module.verify_artifact(b'RIFF'+b'\x00'*4+b'WAVE',False)

    def test_requested_response_timeout_is_not_silently_capped_to_sixty_seconds(self):
        # HTTP is the boundary; no 60-second sleep is required to verify its contract.
        class Response:
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def read1(self,size):return b''
        class Opener:
            def open(inner,req,timeout):
                self.assertEqual(timeout,120);return Response()
        with patch.object(self.module.urllib.request,'build_opener',return_value=Opener()):
            self.module.request('http://localhost','/chat',{'message':'pad'},timeout=120)

if __name__=='__main__':unittest.main()
