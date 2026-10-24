#!/usr/bin/env python3
"""Fail main's release result unless its selected deployment was verified."""
import os
from pathlib import Path


def main():
    target = os.environ.get('DEPLOY_TARGET', '')
    deployment = os.environ.get('DEPLOY_RESULT', '')
    image = os.environ.get('IMAGE_RESULT', '')
    tests = os.environ.get('TEST_RESULT', '')
    reason = ''
    if target not in ('staging', 'dev'):
        reason = 'Unsupported deployment target.'
    elif target == 'staging' and os.environ.get('STAGING_DEPLOY_ENABLED') != 'true':
        reason = 'Main was not deployed: INFERENCE_STAGING_DEPLOY_ENABLED is not true.'
    elif target == 'dev' and (os.environ.get('DEV_DEPLOY_ENABLED') != 'true' or os.environ.get('DEV_CUTOVER_READY') != 'true'):
        reason = 'Manual dev deployment requires INFERENCE_DEPLOY_ENABLED and INFERENCE_TEXT_V123_CUTOVER_READY.'
    elif any(result != 'success' for result in (tests, image, deployment)):
        reason = 'Main was not deployed successfully. Inspect test, image and deployment jobs.'
    summary = f"### {target} release\nRevision: {os.environ.get('REVISION', '')}\nTests: {tests}\nImage build: {image}\nDeployment: {deployment}\n"
    if reason:
        print('::error::' + reason)
        summary += f'This revision has NOT been confirmed running in {target}.\n'
    else:
        summary += 'The deployment receiver verified the running image and readiness.\n'
    with Path(os.environ['GITHUB_STEP_SUMMARY']).open('a') as stream:
        stream.write(summary)
    return 1 if reason else 0


if __name__ == '__main__':
    raise SystemExit(main())
