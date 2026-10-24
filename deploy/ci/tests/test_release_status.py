"""Main releases must not report green when deployment did not happen."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'release_status.py'


class ReleaseStatusTests(unittest.TestCase):
    def run_status(self, **changes):
        with tempfile.TemporaryDirectory() as directory:
            summary = Path(directory) / 'summary'
            env = {**os.environ, 'GITHUB_STEP_SUMMARY': str(summary), 'REVISION': 'a' * 40,
                   'DEPLOY_TARGET': 'staging', 'DEPLOY_RESULT': 'success',
                   'IMAGE_RESULT': 'success', 'TEST_RESULT': 'success',
                   'STAGING_DEPLOY_ENABLED': 'true', 'DEV_DEPLOY_ENABLED': 'true',
                   'DEV_CUTOVER_READY': 'true', **changes}
            result = subprocess.run([sys.executable, str(SCRIPT)], env=env, text=True, capture_output=True)
            return result, summary.read_text() if summary.exists() else ''

    def test_success_requires_verified_deployment_and_upstream_success(self):
        for target in ('staging', 'dev'):
            result, summary = self.run_status(DEPLOY_TARGET=target)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('verified', summary)

    def test_skipped_disabled_failed_or_cancelled_release_cannot_pass(self):
        for changes in ({'DEPLOY_RESULT': 'skipped'}, {'DEPLOY_RESULT': 'failure'},
                        {'DEPLOY_RESULT': 'cancelled'}, {'IMAGE_RESULT': 'failure'},
                        {'TEST_RESULT': 'failure'}, {'STAGING_DEPLOY_ENABLED': ''},
                        {'DEPLOY_TARGET': 'dev', 'DEV_DEPLOY_ENABLED': ''},
                        {'DEPLOY_TARGET': 'dev', 'DEV_CUTOVER_READY': ''},
                        {'DEPLOY_TARGET': 'production'}):
            with self.subTest(changes=changes):
                result, summary = self.run_status(**changes)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('::error::', result.stdout)
                self.assertIn('NOT been confirmed', summary)
