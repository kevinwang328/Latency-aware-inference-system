import asyncio,json,unittest
from unittest.mock import patch
import httpx
from system.job_gateway import JobGateway
from system.queue import TaskQueue
from tests.test_job_gateway_unit import MemoryStore

class CapacityControlsTests(unittest.TestCase):
    def test_lowering_admission_keeps_existing_work_and_blocks_new_claims(self):
        store=MemoryStore();gateway=JobGateway(store,TaskQueue(maxsize=4),max_inflight=2)
        gateway.submit('job');gateway.set_max_inflight(1)
        self.assertEqual(gateway.snapshot(),{'max_inflight':1,'active':1})
        self.assertEqual(store.get('job')['status'],'running')
        with self.assertRaises(ValueError):gateway.set_max_inflight(0)

    def test_mutating_admission_requires_service_authentication(self):
        from system import api_server
        gateway=JobGateway(MemoryStore(),TaskQueue(maxsize=4))
        async def check():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=api_server.app),base_url='http://test') as c:
                self.assertEqual((await c.post('/config/jobs',json={'max_inflight':2})).status_code,401)
                self.assertEqual(gateway.max_inflight,1)
                reply=await c.post('/config/jobs',json={'max_inflight':2},headers={'Authorization':'Bearer test-token'})
                self.assertEqual(reply.status_code,200);self.assertEqual(reply.json()['max_inflight'],2)
                self.assertEqual((await c.post('/config/jobs',json={'max_inflight':33},headers={'Authorization':'Bearer test-token'})).status_code,422)
        with patch.object(api_server,'_job_gateway',gateway),patch.object(api_server,'_jobs_token','test-token'):asyncio.run(check())

    def test_status_counts_registered_capable_workers_instead_of_config_default(self):
        from system.grpc_worker_pool import GrpcWorkerPool
        class Worker:
            def __init__(self,name,ready):self.worker_id=name;self.ready=ready
            def is_ready(self):return self.ready
            def busy(self):return False
        pool=GrpcWorkerPool(static_workers={})
        pool._capabilities={'73':{'synthx_v73'},'122':{'synthx_text_v122'},'old':{'synthx_v73'}}
        pool._workers={w.worker_id:w for w in [Worker('73',True),Worker('122',True),Worker('old',False)]}
        status=pool.snapshot()
        self.assertEqual(status['registered'],3);self.assertEqual(status['ready'],2)
        self.assertEqual(status['ready_by_kind'],{'synthx_v73':1,'synthx_text_v122':1})
