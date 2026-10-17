import unittest
from deploy.ci.deploy_platform import Runtime


class ComposeEnvironmentTests(unittest.TestCase):
    def test_sudo_preserves_only_explicit_nonsecret_compose_paths(self):
        runtime=object.__new__(Runtime)
        calls=[]
        runtime.command=lambda args,**kwargs:calls.append((args,kwargs)) or ''
        runtime.docker('compose','config',env={'SYNTHX_ENV_FILE':'/private/env','PLATFORM_CONTROL_DIR':'/private/control'})
        args,kwargs=calls[0]
        self.assertEqual(args[:4],['sudo','-n','--preserve-env=SYNTHX_ENV_FILE,PLATFORM_CONTROL_DIR','docker'])
        self.assertEqual(kwargs['env']['SYNTHX_ENV_FILE'],'/private/env')
        calls.clear();runtime.docker('inspect','deploy-gateway-1')
        self.assertEqual(calls[0][0][:3],['sudo','-n','docker'])
