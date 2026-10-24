import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from deploy.ci import deploy_platform as platform

SHA = 'a' * 40
HASH = 'b' * 64


class EnvironmentTests(unittest.TestCase):
    def test_dispatch_only_selects_fixed_environment_commands(self):
        self.assertEqual(platform.parse_command(f'deploy-platform {SHA} {HASH}'), ('dev', SHA, HASH))
        self.assertEqual(platform.parse_command(f'deploy-platform-staging {SHA} {HASH}'), ('staging', SHA, HASH))
        for command in (f'deploy-platform-prod {SHA} {HASH}', f'deploy-platform-staging {SHA} {HASH} dev',
                        f'deploy-platform-staging {SHA} {HASH};id', f'deploy-platform-staging main {HASH}'):
            with self.assertRaises(platform.DeployError): platform.parse_command(command)

    def config(self):
        return {'environment': 'staging', 'state_dir': '/home/ubuntu/services/.synthx/staging-platform-cicd',
                'backend_config': '/home/ubuntu/services/.synthx/staging-cicd/config.json'}

    def shared(self):
        return {'environment': 'staging', 'state_dir': '/home/ubuntu/services/.synthx/staging-cicd',
                'env_file': '/opt/synthx-staging/.env.staging', 'repo': '/srv/backend'}

    def test_cross_environment_config_is_rejected_before_import_or_docker(self):
        platform.validate_environment(self.config(), self.shared(), 'staging')
        for side, key, value in [('platform', 'environment', 'production'), ('platform', 'environment', 'dev'),
            ('platform', 'state_dir', '/home/ubuntu/services/.synthx/platform-cicd'),
            ('platform', 'backend_config', '/home/ubuntu/services/.synthx/cicd/config.json'),
            ('backend', 'environment', 'dev'), ('backend', 'state_dir', '/home/ubuntu/services/.synthx/cicd'),
            ('backend', 'env_file', '/srv/backend/.env.production')]:
            config, shared = self.config(), self.shared()
            (config if side == 'platform' else shared)[key] = value
            with self.subTest(side=side, key=key), self.assertRaises(platform.DeployError):
                platform.validate_environment(config, shared, 'staging')

    def test_missing_environment_preserves_legacy_dev_and_rejects_unknown(self):
        config = {'state_dir': '/home/ubuntu/services/.synthx/platform-cicd', 'backend_config': '/home/ubuntu/services/.synthx/cicd/config.json'}
        shared = {'state_dir': '/home/ubuntu/services/.synthx/cicd', 'repo': '/srv/backend'}
        self.assertEqual(platform.validate_environment(config, shared).name, 'dev')
        for value in ('prod', '', None, 'STAGING'):
            config['environment'] = value
            with self.assertRaises(platform.DeployError): platform.validate_environment(config, shared)

    def layout(self):
        services = {name: {'networks': {'default': None}, 'security_opt': ['no-new-privileges:true'],
                          'cap_drop': ['ALL'], 'cpus': cpu, 'mem_limit': memory}
                    for name, cpu, memory in [('gateway', 1, 1073741824), ('text-worker', .5, 536870912)]}
        services['gateway']['environment'] = {'PGSSLMODE': 'verify-full', 'PGSSLROOTCERT': '/etc/synthx-staging/supabase-prod-ca-2021.crt'}
        services['gateway']['ports'] = [{'host_ip': '127.0.0.1', 'published': '18100', 'target': 8000}]
        return {'name': 'synthx-staging-platform', 'services': services,
                'networks': {'default': {'external': True, 'name': 'synthx-staging_default'}}}

    def test_staging_cannot_attach_dev_network_ports_or_unbounded_resources(self):
        platform.validate_layout(self.layout(), environment='staging')
        for mutate in (lambda c: c['networks']['default'].update(name='deploy_default'),
                       lambda c: c['services']['gateway']['ports'][0].update(published='8100'),
                       lambda c: c['services']['gateway'].update(cpus=0),
                       lambda c: c['services']['text-worker'].update(mem_limit=0),
                       lambda c: c['services']['gateway'].update(cap_drop=[]),
                       lambda c: c['services']['gateway'].update(security_opt=[]),
                       lambda c: c.update(name='deploy'),
                       lambda c: c['services']['gateway'].update(container_name='deploy-gateway-1'),
                       lambda c: c['services']['gateway'].update(cap_add=['SYS_ADMIN']),
                       lambda c: c['services']['gateway'].update(pid='host'),
                       lambda c: c['services']['gateway']['environment'].update(PGSSLMODE='require'),
                       lambda c: c['services']['gateway']['environment'].update(PGSSLROOTCERT='/tmp/other.crt')):
            layout = self.layout(); mutate(layout)
            with self.assertRaises(platform.DeployError): platform.validate_layout(layout, environment='staging')

    def test_staging_compose_uses_selected_paths_and_project(self):
        runtime = object.__new__(platform.Runtime)
        runtime.target = platform.Environment('staging')
        runtime.root = Path('/home/ubuntu/services/.synthx/staging-platform-cicd')
        runtime.env_file = Path('/opt/synthx-staging/.env.staging.platform')
        calls = []
        runtime.command = lambda args, **kwargs: calls.append((args, kwargs)) or ''
        runtime.compose({'compose': '/tmp/release/compose.platform.yml'}, 'config')
        args, kwargs = calls[0]
        self.assertIn('synthx-staging-platform', args)
        self.assertIn('/dev/null', args)
        self.assertEqual(kwargs['env']['SYNTHX_ENV_FILE'], '/opt/synthx-staging/.env.staging.platform')
        self.assertIn('/tmp/release/compose.platform.staging.yml', args)
        self.assertEqual(kwargs['env']['PLATFORM_MODEL_HOST'], 'model-tunnel')
        self.assertEqual(kwargs['env']['PLATFORM_GATEWAY_PORT'], '18100')
        self.assertEqual(kwargs['env']['PLATFORM_NETWORK'], 'synthx-staging_default')
        self.assertNotIn('deploy', args)


    def test_staging_accepts_only_control_and_gateway_ca_mounts(self):
        layout = self.layout()
        control = '/home/ubuntu/services/.synthx/staging-platform-cicd/control'
        marker = {'type': 'bind', 'source': control, 'target': '/run/synthx-deploy', 'read_only': True}
        ca = {'type': 'bind', 'source': '/opt/synthx-staging/deploy/certs/supabase-prod-ca-2021.crt',
              'target': '/etc/synthx-staging/supabase-prod-ca-2021.crt', 'read_only': True}
        layout['services']['gateway']['volumes'] = [marker, ca]
        layout['services']['text-worker']['volumes'] = [marker]
        platform.validate_layout(layout, Path(control), 'staging')
        for value in (False, None):
            ca['read_only'] = value
            with self.assertRaises(platform.DeployError): platform.validate_layout(layout, Path(control), 'staging')
        ca['read_only'] = True
        ca['source'] = '/opt/synthx-dev/root.crt'
        with self.assertRaises(platform.DeployError): platform.validate_layout(layout, Path(control), 'staging')

    def test_environment_hash_reads_root_file_via_digest_only(self):
        runtime = object.__new__(platform.Runtime)
        runtime.target = platform.Environment('staging')
        runtime.env_file = Path('/opt/synthx-staging/.env.staging.platform')
        calls = []
        runtime.command = lambda args, **kwargs: calls.append(args) or ('e' * 64 + '  ' + str(runtime.env_file))
        self.assertEqual(runtime.environment_digest(), 'e' * 64)
        self.assertEqual(calls, [['sudo', '-n', 'sha256sum', '/opt/synthx-staging/.env.staging.platform']])


    def test_backend_environment_change_blocks_platform_release(self):
        runtime = object.__new__(platform.Runtime)
        runtime.target = platform.Environment('staging')
        runtime.env_file = Path('/opt/synthx-staging/.env.staging.platform')
        runtime.backend_env_file = Path('/opt/synthx-staging/.env.staging')
        runtime.environment_hash = 'a' * 64
        runtime.backend_environment_hash = 'b' * 64
        runtime.protected = {}
        hashes = {str(runtime.env_file): 'a' * 64, str(runtime.backend_env_file): 'b' * 64}
        runtime.command = lambda args, **kwargs: hashes[args[-1]] + '  ' + args[-1]
        runtime.assert_protected()
        hashes[str(runtime.backend_env_file)] = 'c' * 64
        with self.assertRaises(platform.DeployError): runtime.assert_protected()

    def test_record_updates_only_staging_dependency_pins(self):
        with tempfile.TemporaryDirectory() as directory:
            runtime = object.__new__(platform.Runtime)
            runtime.target = platform.Environment('staging')
            runtime.root = Path(directory)
            runtime.shared_path = runtime.root / 'backend.json'
            runtime.shared = {'dependency_images': {'synthx-staging-models-model-tunnel-1': 'tunnel'}}
            runtime.record({'sha': SHA}, {'gateway': 'new-gateway', 'text-worker': 'new-worker'}, {'sha': 'old'})
            pins = json.loads(runtime.shared_path.read_text())['dependency_images']
            self.assertEqual(pins, {'synthx-staging-models-model-tunnel-1': 'tunnel',
                                  'synthx-staging-platform-gateway-1': 'new-gateway',
                                  'synthx-staging-platform-text-worker-1': 'new-worker'})


class StagingAdoptionTests(unittest.TestCase):
    def containers(self):
        names = ('synthx-staging-platform-gateway-1', 'synthx-staging-platform-text-worker-1',
                 'synthx-staging-backend-1', 'synthx-staging-models-model-tunnel-1')
        containers = {name: {'Image': 'image-' + name, 'State': {'Running': True},
                'NetworkSettings': {'Networks': {'synthx-staging_default': {}}},
                'HostConfig': {'PortBindings': {'8000/tcp': [{'HostIp': '127.0.0.1', 'HostPort': '18100'}]} if name == names[0] else {}},
                'Mounts': [{'Type': 'bind', 'RW': False, 'Source': '/home/ubuntu/services/.synthx/staging-platform-cicd/control',
                            'Destination': '/run/synthx-deploy'}]} for name in names}
        containers[names[0]]['Mounts'].append({'Type': 'bind', 'RW': False, 'Source': '/opt/synthx-staging/deploy/certs/supabase-prod-ca-2021.crt', 'Destination': '/etc/synthx-staging/supabase-prod-ca-2021.crt'})
        return containers

    def test_adoption_records_staging_rollback_source_and_rejects_dev_attachment(self):
        from deploy.ci.prepare_host import adoption_plan
        current = adoption_plan(self.containers(), True, environment='staging')
        self.assertFalse(current['bootstrap_required'])
        self.assertEqual(current['compose'], '/home/ubuntu/services/.synthx/staging-platform-cicd/bootstrap/compose.platform.yml')
        self.assertEqual(current['images']['gateway'], 'image-synthx-staging-platform-gateway-1')
        containers = self.containers()
        containers['synthx-staging-platform-gateway-1']['NetworkSettings']['Networks']['deploy_default'] = {}
        with self.assertRaises(ValueError): adoption_plan(containers, True, environment='staging')


class PlatformCredentialPreflightTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.runtime = object.__new__(platform.Runtime)
        r = self.runtime
        r.target = platform.Environment('staging')
        r.target.backend_state = Path(self.tmp.name).resolve()
        r.backend_env_file = Path('/opt/synthx-staging/.env.staging')
        r.env_file = Path('/opt/synthx-staging/.env.staging.platform')
        self.source = r.target.backend_state / 'releases' / SHA / 'source'
        self.source.mkdir(parents=True)
        (self.source / '.synthx-revision').write_text(SHA)
        self.state = {'sha': SHA, 'compose': str(self.source / 'deploy/ci/compose.backend-staging.yml'), 'image': 'sha256:' + HASH}
        self.save()
        self.calls = []
        r.inspect = lambda name, expression: 'sha256:' + HASH
        r.command = lambda args, **kwargs: self.calls.append((args, kwargs)) or json.dumps({'status': 'passed', 'platform_checked': True})

    def save(self):
        (self.runtime.target.backend_state / 'current.json').write_text(json.dumps(self.state))

    def test_validates_protected_subset_from_recorded_backend_source_without_plaintext_output(self):
        self.runtime.assert_platform_environment()
        args, kwargs = self.calls[0]
        self.assertEqual(args, ['sudo', '-n', '/opt/synthx-staging/.venv/bin/python', '-m',
            'backend.scripts.staging_config', '--manifest', '/opt/synthx-staging/.staging-secrets/target.json',
            '--runtime-env', '/opt/synthx-staging/.env.staging', '--platform-env', '/opt/synthx-staging/.env.staging.platform'])
        self.assertEqual(kwargs['cwd'], self.source)

    def test_foreign_source_marker_image_or_nonpassing_report_never_passes(self):
        original = dict(self.state)
        for key, value in [('sha', 'main'), ('compose', '/home/ubuntu/services/Synth_X/deploy/compose.aws.yml')]:
            self.state = {**original, key: value}; self.save()
            with self.assertRaises(platform.DeployError): self.runtime.assert_platform_environment()
        self.assertEqual(self.calls, [])
        self.state = original; self.save()
        (self.source / '.synthx-revision').write_text('b' * 40)
        with self.assertRaises(platform.DeployError): self.runtime.assert_platform_environment()
        (self.source / '.synthx-revision').write_text(SHA)
        self.runtime.inspect = lambda *args: 'sha256:' + 'c' * 64
        with self.assertRaises(platform.DeployError): self.runtime.assert_platform_environment()
        self.runtime.inspect = lambda *args: 'sha256:' + HASH
        for report in ({'status': 'passed'}, {'status': 'blocked', 'platform_checked': True}):
            self.runtime.command = lambda *args, **kwargs: json.dumps(report)
            with self.assertRaises(platform.DeployError): self.runtime.assert_platform_environment()

    def test_credential_rejection_prevents_docker_fetch_load_and_start(self):
        from unittest.mock import Mock
        r = self.runtime
        r.assert_platform_environment = Mock(side_effect=platform.DeployError('credentials rejected'))
        r.compose = Mock()
        for action in (lambda: r.prepare(SHA), lambda: r.start({'compose': '/tmp/unused.yml'}, {})):
            with self.assertRaisesRegex(platform.DeployError, 'credentials rejected'): action()
        self.assertEqual(self.calls, [])
        r.compose.assert_not_called()

    def test_environment_mutation_after_validation_prevents_start(self):
        from unittest.mock import Mock
        r = self.runtime
        r.assert_platform_environment = Mock()
        r.assert_protected = Mock(side_effect=platform.DeployError('environment changed'))
        r.compose = Mock()
        with self.assertRaisesRegex(platform.DeployError, 'environment changed'):
            r.start({'compose': '/tmp/unused.yml'}, {})
        r.compose.assert_not_called()
