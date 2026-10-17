#!/usr/bin/env python3
"""Bounded capacity experiments. Simulation is the default; real jobs need --allow-real.

Each real stage submits exactly its concurrency count, without automatic retries.
The HTTP 202 acknowledgement is never counted as successful model generation.
"""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor,as_completed
import hashlib,io,json,math,os,pathlib,socket,subprocess,sys,tempfile,time,urllib.error,urllib.parse,urllib.request,uuid,wave

ROOT=pathlib.Path(__file__).resolve().parents[1]

def summarize(rows,seconds):
    values=sorted(r['seconds'] for r in rows if r.get('ok'))
    def percentile(q):return values[math.ceil(len(values)*q)-1] if values else None
    return {'submitted':len(rows),'completed':len(values),'failed':len(rows)-len(values),
            'completion_rps':round(len(values)/seconds,4) if seconds else 0,
            'p50_seconds':percentile(.5),'p95_seconds':percentile(.95) if len(values)>=20 else None,
            'max_seconds':max(values) if values else None,'wall_seconds':round(seconds,3),
            'errors':dict(Counter(r.get('error','unknown') for r in rows if not r.get('ok')))}

def validate_real_plan(concurrency,kinds,max_jobs,allow_real):
    total=sum(concurrency)*len(kinds)
    if not allow_real:raise ValueError('Real model calls require --allow-real')
    if total>max_jobs:raise ValueError(f'Plan would submit {total} jobs, above --max-jobs={max_jobs}')
    return total

def request(base,route,data=None,timeout=600):
    headers={'Content-Type':'application/json'}
    token=os.environ.get('BENCHMARK_BACKEND_TOKEN','')
    if token:headers['Authorization']='Bearer '+token
    req=urllib.request.Request(base.rstrip('/')+route,data=None if data is None else json.dumps(data).encode(),headers=headers)
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
    deadline=time.monotonic()+timeout
    with opener.open(req,timeout=timeout) as response:
        chunks=[];size=0
        while True:
            if time.monotonic()>deadline:raise TimeoutError('Overall request deadline exceeded')
            chunk=response.read1(64*1024)
            if not chunk:break
            size+=len(chunk)
            if size>32*1024**2:raise ValueError('Response exceeds benchmark limit')
            chunks.append(chunk)
        return b''.join(chunks)

def js(base,route,data=None,timeout=600):
    value=json.loads(request(base,route,data,timeout))
    if not isinstance(value,dict):raise ValueError('Expected a JSON object response')
    return value

def verify_artifact(data,native):
    if native:
        document=json.loads(data)
        if not isinstance(document,dict) or not isinstance(document.get('settings'),dict):raise ValueError('Invalid native preset')
    else:
        try:
            with wave.open(io.BytesIO(data)) as preview:
                if preview.getnframes() < 1 or len(preview.readframes(preview.getnframes())) < preview.getnframes()*preview.getnchannels()*preview.getsampwidth():
                    raise ValueError('Empty or truncated WAV preview')
        except (wave.Error,EOFError) as error:raise ValueError('Invalid WAV preview') from error
    return {'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()}

def real_request(base,kind,owner,audio_path,prompt,timeout,poll_interval=.5):
    row={'id':str(uuid.uuid4()),'kind':kind,'ok':False};start=time.perf_counter()
    try:
        project=js(base,'/projects',{'user_id':owner,'title':'Capacity probe '+row['id'][:8],'synth_engine':'vital'})
        row['project_id']=project['id']
        if kind=='audio':
            row['job_id']=str(uuid.uuid4())
            job=js(base,'/inference/jobs',{'project_id':project['id'],'user_id':owner,'audio_path':audio_path,'mode':'poly','job_id':row['job_id']})
            row['submission_seconds']=round(time.perf_counter()-start,3);row['transitions']=[]
            last=None
            while time.perf_counter()-start<timeout:
                job=js(base,'/inference/jobs/'+row['job_id']+'?user_id='+owner,timeout=30)
                if job['status']!=last:
                    last=job['status'];row['transitions'].append({'status':last,'seconds':round(time.perf_counter()-start,3)})
                if last in {'succeeded','failed'}:break
                time.sleep(poll_interval)
            row['terminal_status']=job['status'];row['attempts']=job.get('attempts')
            if job['status']!='succeeded':raise ValueError('audio_'+job['status'])
            prefix='/inference/jobs/'+row['job_id']+'/artifacts/'
            routes=[prefix+'final.vital?user_id='+owner,prefix+'final.wav?user_id='+owner]
        else:
            session=js(base,'/sessions',{'user_id':owner,'project_id':project['id']})
            canonical=js(base,'/projects/'+project['id']+'/sync?user_id='+owner)
            row['operation_id']=str(uuid.uuid4())
            result=js(base,'/chat',{'session_id':session['id'],'message':prompt,'operation_id':row['operation_id'],
                'project_id':project['id'],'user_id':owner,'expected_active_snapshot_id':canonical['project'].get('active_snapshot_id'),
                'expected_mutation_revision':canonical['mutation_revision']},timeout=timeout)
            row['terminal_status']=result.get('state')
            if result.get('state')!='success':raise ValueError('text_'+str(result.get('state')))
            row['snapshot_id']=result['snapshot_id'];routes=['/snapshots/'+result['snapshot_id']+'/native','/snapshots/'+result['snapshot_id']+'/preview']
        row['generation_seconds']=round(time.perf_counter()-start,3)
        row['artifacts']={name:verify_artifact(request(base,route,timeout=60),native) for name,route,native in [('preset',routes[0],True),('preview',routes[1],False)]}
        row['ok']=True
    except urllib.error.HTTPError as error:row['error']='http_'+str(error.code)
    except (OSError,ValueError,KeyError,TypeError) as error:row['error']=type(error).__name__+': '+str(error)[:160]
    row['seconds']=round(time.perf_counter()-start,3)
    return row

def free_port():
    with socket.socket() as sock:sock.bind(('127.0.0.1',0));return sock.getsockname()[1]

def simulate(workers,strategy,batch_size,rate,duration,concurrency):
    port=free_port();base='http://127.0.0.1:'+str(port)
    launcher=f"from system.config import config; config.update(num_workers={workers},batch_size={batch_size},scheduler_strategy={strategy!r},failure_rate=0,inference_latency_ms=60,max_queue_size=128); import uvicorn; uvicorn.run('system.api_server:app',host='127.0.0.1',port={port},access_log=False,log_level='warning')"
    env={'PATH':os.environ.get('PATH',''),'PYTHONDONTWRITEBYTECODE':'1','USE_GRPC':'0','SYNTHX_JOBS_ENABLED':'0'}
    with tempfile.TemporaryFile() as log:
        child=subprocess.Popen([sys.executable,'-c',launcher],cwd=ROOT,env=env,stdout=log,stderr=subprocess.STDOUT)
        try:
            deadline=time.monotonic()+15
            while True:
                if child.poll() is not None:raise RuntimeError('Simulation server exited')
                try:
                    state=js(base,'/status',timeout=1)
                    if state['config']['num_workers']!=workers:raise RuntimeError('Wrong simulation server')
                    break
                except OSError:
                    if time.monotonic()>deadline:raise TimeoutError('Simulation startup timed out')
                    time.sleep(.1)
            slots=__import__('threading').BoundedSemaphore(concurrency);rows=[];futures=[];skipped=0;started=time.perf_counter()
            def send():
                tick=time.perf_counter()
                try:
                    result=js(base,'/predict',{'x':3},timeout=35)
                    return {'ok':result.get('result',{}).get('prediction')==9,'seconds':time.perf_counter()-tick,'worker_id':result.get('result',{}).get('worker_id')}
                except (OSError,ValueError) as error:return {'ok':False,'seconds':time.perf_counter()-tick,'error':type(error).__name__}
                finally:slots.release()
            with ThreadPoolExecutor(max_workers=concurrency) as pool:
                for index in range(math.ceil(rate*duration)):
                    due=started+index/rate;time.sleep(max(0,due-time.perf_counter()))
                    if time.perf_counter()>=started+duration or time.perf_counter()-due>=1/rate or not slots.acquire(False):skipped+=1;continue
                    futures.append(pool.submit(send))
                time.sleep(max(0,started+duration-time.perf_counter()))
                rows=[future.result() for future in futures]
            elapsed=time.perf_counter()-started
            return {'mode':'simulation','workers':workers,'strategy':strategy,'batch_size':batch_size,'target_rps':rate,
                    'duration':duration,'generator_skipped':skipped,'observed_worker_ids':sorted({r['worker_id'] for r in rows if r.get('worker_id') is not None}),**summarize(rows,elapsed)}
        finally:
            child.terminate()
            try:child.wait(timeout=10)
            except subprocess.TimeoutExpired:child.kill();child.wait()

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode',choices=['simulation','real'],default='simulation')
    parser.add_argument('--output',required=True,help='New JSONL output file (must not exist)')
    parser.add_argument('--workers',type=int,nargs='+',default=[1,2,4]);parser.add_argument('--strategies',nargs='+',choices=['fifo','batching','latency_aware'],default=['fifo'])
    parser.add_argument('--batch-sizes',type=int,nargs='+',default=[1]);parser.add_argument('--rates',type=float,nargs='+',default=[20]);parser.add_argument('--duration',type=float,default=10)
    parser.add_argument('--client-concurrency',type=int,default=64)
    parser.add_argument('--url',default='http://127.0.0.1:8000');parser.add_argument('--allow-real',action='store_true')
    parser.add_argument('--concurrency',type=int,nargs='+',default=[1,2,4]);parser.add_argument('--kinds',choices=['audio','text'],nargs='+',default=['audio','text'])
    parser.add_argument('--max-jobs',type=int,default=14);parser.add_argument('--timeout',type=float,default=600)
    parser.add_argument('--audio-path');parser.add_argument('--prompt',default='Create a warm ambient pad preset for Vital, with an audio preview.')
    args=parser.parse_args()
    target=urllib.parse.urlsplit(args.url)
    if target.scheme not in {'http','https'} or not target.hostname or target.username or target.password or target.query or target.fragment:parser.error('--url must be an HTTP(S) URL without credentials, query or fragment')
    numbers=[*args.workers,*args.batch_sizes,*args.rates,*args.concurrency,args.duration,args.client_concurrency,args.timeout,args.max_jobs]
    if any(not math.isfinite(n) or n<=0 for n in numbers) or args.client_concurrency>512 or max(args.workers)>32 or max(args.concurrency)>16:parser.error('Values must be positive, finite and within bounded concurrency limits')
    if args.mode=='real':
        try:validate_real_plan(args.concurrency,args.kinds,args.max_jobs,args.allow_real)
        except ValueError as error:parser.error(str(error))
        if 'audio' in args.kinds and not args.audio_path:parser.error('--audio-path must name an existing backend upload')
    output=pathlib.Path(args.output);output.parent.mkdir(parents=True,exist_ok=True)
    with output.open('x') as stream:
        def emit(value):stream.write(json.dumps(value)+'\n');stream.flush();print(json.dumps(value),flush=True)
        if args.mode=='simulation':
            for workers in args.workers:
                for strategy in args.strategies:
                    for batch in args.batch_sizes:
                        for rate in args.rates:emit(simulate(workers,strategy,batch,rate,args.duration,args.client_concurrency))
        else:
            owner='capacity-'+uuid.uuid4().hex
            emit({'mode':'real','owner':owner,'planned_jobs':sum(args.concurrency)*len(args.kinds),'changes_production_config':False,'url':args.url,'prompt':args.prompt,'audio_path':args.audio_path,'timeout_seconds':args.timeout})
            for kind in args.kinds:
                for concurrent in args.concurrency:
                    started=time.perf_counter();rows=[]
                    with ThreadPoolExecutor(max_workers=concurrent) as pool:
                        futures=[pool.submit(real_request,args.url,kind,owner,args.audio_path,args.prompt,args.timeout) for _ in range(concurrent)]
                        for future in as_completed(futures):rows.append(future.result());emit({'event':'request',**rows[-1]})
                    emit({'event':'stage','kind':kind,'client_concurrency':concurrent,**summarize(rows,time.perf_counter()-started)})
                    if any(not row['ok'] for row in rows):raise SystemExit('Stage failed; remaining load stopped. Inspect recorded IDs without resubmitting.')

if __name__=='__main__':main()
