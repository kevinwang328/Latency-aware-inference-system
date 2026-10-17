#!/usr/bin/env python3
"""Smoke the built image in CPU simulation; no model, LLM, or cloud credentials."""
import json
import subprocess
import sys
import time
import urllib.request
import uuid


def docker(*args):
    return subprocess.check_output(['docker',*args],text=True,timeout=120).strip()


def main(image):
    docker('run','--rm','--network','none','-e','INFERENCE_BACKEND=text_v1.2.2',
        '-e','TEXT_SERVICE_URL=http://unreachable.invalid',image,'python','-c',
        'from system.grpc_worker_server import WorkerServicer; assert WorkerServicer(122).backend is not None')
    name = 'platform-ci-smoke-' + uuid.uuid4().hex[:12]
    try:
        docker('run','-d','--rm','--name',name,'-e','USE_GRPC=0','-e','SYNTHX_JOBS_ENABLED=0','-e','PLATFORM_DRAIN_FILE=/tmp/platform-smoke-drain','-p','127.0.0.1::8000',image)
        ports=json.loads(docker('inspect','--format','{{json .NetworkSettings.Ports}}',name))
        base='http://127.0.0.1:'+ports['8000/tcp'][0]['HostPort']
        deadline=time.monotonic()+60
        while True:
            try:
                with urllib.request.urlopen(base+'/status',timeout=3) as r:
                    assert json.load(r)['status']=='running'
                break
            except (OSError,ValueError):
                if time.monotonic()>=deadline:raise RuntimeError('Image API did not start')
                time.sleep(.5)
        request=urllib.request.Request(base+'/predict',data=b'{"x":3}',headers={'Content-Type':'application/json'})
        with urllib.request.urlopen(request,timeout=10) as r: assert json.load(r)['result']['prediction']==9
        with urllib.request.urlopen(base+'/metrics',timeout=3) as r: assert b'inference_api_queue_depth' in r.read()
        docker('exec',name,'python','-c',"from pathlib import Path; Path('/tmp/platform-smoke-drain').write_text('smoke')")
        with urllib.request.urlopen(base+'/health/deployment',timeout=3) as response:
            state=json.load(response);assert state['draining'] and state['active']==0
        try:
            urllib.request.urlopen(request,timeout=3)
            raise AssertionError('Drain admitted new inference')
        except __import__('urllib.error',fromlist=['HTTPError']).HTTPError as error:
            assert error.code==503
        print('Built image: adapter imports, CPU API, metrics and maintenance admission passed')
    finally:
        subprocess.run(['docker','rm','-f',name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=30)

if __name__ == '__main__':main(sys.argv[1])
