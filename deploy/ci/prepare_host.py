#!/usr/bin/env python3
"""Read-only Docker inspection plus an offline proposal; never installs or deploys."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import urllib.request

if __package__:
    from .deploy_platform import Environment, validate_environment
else:
    from deploy_platform import Environment, validate_environment
BACKEND_RECEIVER = Path('/usr/local/lib/synthx-ci/deploy_backend.py')


def adoption_plan(containers, protocol_ready, control_dir=None, environment='dev'):
    target = Environment(environment)
    control_dir = control_dir or target.state_dir/'control'
    gateway, worker = target.containers['gateway'], target.containers['text-worker']
    for name in (gateway, worker, target.backend_container, target.tunnel_container):
        item=containers[name]
        if not item['State']['Running'] or set(item['NetworkSettings']['Networks'])!={target.network}:
            raise ValueError('Existing required services must be running on the selected private network')
    ports=containers[gateway]['HostConfig'].get('PortBindings') or {}
    if ports!={'8000/tcp':[{'HostIp':'127.0.0.1','HostPort':str(target.gateway_port)}]} or containers[worker]['HostConfig'].get('PortBindings'):
        raise ValueError('Gateway/text adapter bindings differ from approved private layout')
    if protocol_ready:
        for name in (gateway, worker):
            mounts=containers[name].get('Mounts',[])
            expected={(str(control_dir),'/run/synthx-deploy')}
            if target.name=='staging' and name==gateway:
                expected.add(('/opt/synthx-staging/deploy/certs/supabase-prod-ca-2021.crt',
                              '/etc/synthx-staging/supabase-prod-ca-2021.crt'))
            if len(mounts)!=len(expected) or {(m.get('Source'),m.get('Destination')) for m in mounts}!=expected or any(m.get('Type')!='bind' or m.get('RW') is not False for m in mounts):
                raise ValueError('Ready adoption requires approved read-only maintenance and CA mounts')
    return {'sha':'adopted','compose':str(target.state_dir/'bootstrap/compose.platform.yml'),
        'images':{'gateway':containers[gateway]['Image'],'text-worker':containers[worker]['Image']},
        'bootstrap_required':not protocol_ready}


def docker(*args):
    return subprocess.check_output(['sudo','-n','docker',*args],text=True,timeout=30).strip()


def protocol_ready(target):
    try:
        with urllib.request.urlopen(f'http://127.0.0.1:{target.gateway_port}/health/deployment',timeout=5) as response:
            gateway=json.load(response)
        program="import json,urllib.request;print(json.dumps(json.load(urllib.request.urlopen('http://127.0.0.1:16062/health/deployment',timeout=5))))"
        worker=json.loads(docker('exec',target.containers['text-worker'],'python','-c',program))
        return all(s.get('protocol')==1 and s.get('active')==0 and s.get('draining') is False for s in (gateway,worker))
    except (OSError,ValueError,subprocess.SubprocessError):return False


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True,help='New private proposal directory')
    parser.add_argument('--environment', choices=('staging', 'dev'), default='staging')
    args=parser.parse_args()
    target=Environment(args.environment)
    shared=json.loads(target.backend_config.read_text())
    config={'environment':target.name,'repo':'/home/ubuntu/services/Latency-aware-inference-system',
        'state_dir':str(target.state_dir),'backend_config':str(target.backend_config),
        'backend_receiver':str(BACKEND_RECEIVER),
        'backend_receiver_sha256':hashlib.sha256(BACKEND_RECEIVER.read_bytes()).hexdigest(),
        'drain_timeout_seconds':300}
    validate_environment(config, shared, target.name)
    containers={name:json.loads(docker('inspect',name))[0] for name in
        (*target.containers.values(),target.backend_container,target.tunnel_container)}
    for name,image in shared['dependency_images'].items():
        if name not in containers or containers[name]['Image']!=image:
            raise ValueError('Platform/tunnel differs from the backend release pins')
    backend=json.loads((Path(shared['state_dir'])/'current.json').read_text())
    if containers[target.backend_container]['Image']!=backend['image']:
        raise ValueError('Backend is not its recorded CI/CD image')
    current=adoption_plan(containers,protocol_ready(target),environment=target.name)
    args.output.mkdir(mode=0o700,parents=True,exist_ok=False)
    for name,value in (('config.json',config),('current.json',current),
        ('images.json',{'services':{s:{'image':image} for s,image in current['images'].items()}})):
        path=args.output/name;path.write_text(json.dumps(value,indent=2)+'\n');os.chmod(path,0o600)
    shutil.copyfile(Path(__file__).with_name('compose.platform.yml'),args.output/'compose.platform.yml')
    if target.name=='staging':
        shutil.copyfile(Path(__file__).with_name('compose.platform.staging.yml'),args.output/'compose.platform.staging.yml')
    print(json.dumps({'proposal':str(args.output),'bootstrap_required':current['bootstrap_required'],
        'environment':target.name,'services_changed':False,'note':'Review before installing state, SSH access or enabling deployment.'}))

if __name__=='__main__':main()
