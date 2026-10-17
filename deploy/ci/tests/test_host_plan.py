import copy
import unittest
from deploy.ci.prepare_host import adoption_plan
from deploy.ci.run_tests import validate_test_database


class HostPlanTests(unittest.TestCase):
    def containers(self):
        return {name:{'Image':'sha256:'+letter*64,'State':{'Running':True},
            'NetworkSettings':{'Networks':{'deploy_default':{}}},
            'HostConfig':{'PortBindings':{'8000/tcp':[{'HostIp':'127.0.0.1','HostPort':'8100'}]} if name=='deploy-gateway-1' else {}},
            'Config':{'Env':[]},'Mounts':[]} for name,letter in (
                ('deploy-gateway-1','a'),('deploy-text-worker-1','b'),('deploy-backend-1','c'),('deploy-vast-tunnel-1','d'))}
    def test_legacy_adoption_cannot_be_mistaken_for_deploy_ready(self):
        plan=adoption_plan(self.containers(),False)
        self.assertTrue(plan['bootstrap_required'])
        self.assertEqual(plan['images'],{'gateway':'sha256:'+'a'*64,'text-worker':'sha256:'+'b'*64})
    def test_missing_running_service_or_public_binding_blocks_adoption(self):
        for mutate in (lambda c:c['deploy-text-worker-1']['State'].update(Running=False),
            lambda c:c['deploy-gateway-1']['HostConfig']['PortBindings']['8000/tcp'][0].update(HostIp='0.0.0.0')):
            value=self.containers();mutate(value)
            with self.assertRaises(ValueError):adoption_plan(value,True)
    def test_ci_database_guard_rejects_cloud_and_general_local_database(self):
        self.assertTrue(validate_test_database('postgresql://u:p@127.0.0.1:5432/synthx_platform_ci'))
        for url in ('postgresql://u:p@db.example.com/synthx_platform_ci','postgresql://u:p@127.0.0.1/production',''):
            with self.assertRaises(ValueError):validate_test_database(url)

if __name__=='__main__':unittest.main()
