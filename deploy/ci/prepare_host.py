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

STATE = Path('/home/ubuntu/services/.synthx/platform-cicd')
BACKEND_CONFIG = Path('/home/ubuntu/services/.synthx/cicd/config.json')
BACKEND_RECEIVER = Path('/usr/local/lib/synthx-ci/deploy_backend.py')
NAMES = ('deploy-gateway-1','deploy-text-worker-1','deploy-backend-1','deploy-vast-tunnel-1')


def adoption_plan(containers, protocol_ready, control_dir=STATE/'control'):
    for name in NAMES:
        item=containers[name]
        if not item['State']['Running'] or set(item['NetworkSettings']['Networks'])!={'deploy_default'}:
            raise ValueError('Existing required services must be running on deploy_default')
    ports=containers['deploy-gateway-1']['HostConfig'].get('PortBindings') or {}
    if ports!={'8000/tcp':[{'HostIp':'127.0.0.1','HostPort':'8100'}]} or containers['deploy-text-worker-1']['HostConfig'].get('PortBindings'):
        raise ValueError('Gateway/text adapter bindings differ from approved private layout')
    if protocol_ready:
        for name in ('deploy-gateway-1','deploy-text-worker-1'):
            mounts=containers[name].get('Mounts',[])
            if len(mounts)!=1 or mounts[0].get('Type')!='bind' or mounts[0].get('RW') is not False or mounts[0].get('Source')!=str(control_dir) or mounts[0].get('Destination')!='/run/synthx-deploy':
                raise ValueError('Ready adoption requires the approved read-only maintenance mount')
    return {'sha':'adopted','compose':str(STATE/'bootstrap/compose.platform.yml'),
        'images':{'gateway':containers['deploy-gateway-1']['Image'],'text-worker':containers['deploy-text-worker-1']['Image']},
        'bootstrap_required':not protocol_ready}


def docker(*args):
    return subprocess.check_output(['sudo','-n','docker',*args],text=True,timeout=30).strip()


def protocol_ready():
    try:
        with urllib.request.urlopen('http://127.0.0.1:8100/health/deployment',timeout=5) as response:
            gateway=json.load(response)
        program="import json,urllib.request;print(json.dumps(json.load(urllib.request.urlopen('http://127.0.0.1:16062/health/deployment',timeout=5))))"
        worker=json.loads(docker('exec','deploy-text-worker-1','python','-c',program))
        return all(s.get('protocol')==1 and s.get('active')==0 and s.get('draining') is False for s in (gateway,worker))
    except (OSError,ValueError,subprocess.SubprocessError):return False


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True,help='New private proposal directory')
    args=parser.parse_args()
    containers={name:json.loads(docker('inspect',name))[0] for name in NAMES}
    shared=json.loads(BACKEND_CONFIG.read_text())
    for name,image in shared['dependency_images'].items():
        if name not in containers or containers[name]['Image']!=image:
            raise ValueError('Platform/tunnel differs from the backend release pins')
    backend=json.loads((Path(shared['state_dir'])/'current.json').read_text())
    if containers['deploy-backend-1']['Image']!=backend['image']:
        raise ValueError('Backend is not its recorded CI/CD image')
    current=adoption_plan(containers,protocol_ready())
    config={'repo':'/home/ubuntu/services/Latency-aware-inference-system','state_dir':str(STATE),
        'backend_config':str(BACKEND_CONFIG),'backend_receiver':str(BACKEND_RECEIVER),
        'backend_receiver_sha256':hashlib.sha256(BACKEND_RECEIVER.read_bytes()).hexdigest(),
        'drain_timeout_seconds':300}
    args.output.mkdir(mode=0o700,parents=True,exist_ok=False)
    for name,value in (('config.json',config),('current.json',current),
        ('images.json',{'services':{s:{'image':image} for s,image in current['images'].items()}})):
        path=args.output/name;path.write_text(json.dumps(value,indent=2)+'\n');os.chmod(path,0o600)
    shutil.copyfile(Path(__file__).with_name('compose.platform.yml'),args.output/'compose.platform.yml')
    print(json.dumps({'proposal':str(args.output),'bootstrap_required':current['bootstrap_required'],
        'production_changed':False,'note':'Review before installing state, SSH access or enabling deployment.'}))

if __name__=='__main__':main()
