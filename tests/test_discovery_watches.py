"""Check complete discovery snapshots and admission during draining."""

import json
import threading
import time
import unittest
from unittest.mock import patch

from kazoo.exceptions import ConnectionLoss
from system.zookeeper_registry import WorkerDiscovery, WorkerInfo
from system.task import TaskStatus
import test_worker_handoff as fixtures
from test_zookeeper_registry import FakeClient


class WatchClient(FakeClient):
    def __init__(self):
        super().__init__()
        self.children_callback = None
        self.data_callbacks = {}
        self.watch_count = {}
        self.fail_reads = False

    def get_children(self, path):
        if self.fail_reads:
            raise ConnectionLoss()
        return [node.rsplit('/', 1)[1] for node in list(self.nodes)]

    def ChildrenWatch(self, path):
        def install(callback):
            self.children_callback = callback
            callback(self.get_children(path))
            return callback
        return install

    def DataWatch(self, path):
        def install(callback):
            self.watch_count[path] = self.watch_count.get(path, 0) + 1
            self.data_callbacks[path] = callback
            node = self.nodes.get(path)
            callback(node[0] if node else None, None, None)
            return callback
        return install

    def put(self, wid, state='READY'):
        path = f'/inference/workers/{wid}'
        self.nodes[path] = (json.dumps({'address': f'{wid}:50051', 'state': state}).encode(), 99)
        if path in self.data_callbacks:
            self.data_callbacks[path](self.nodes[path][0], None, None)
        if self.children_callback:
            self.children_callback(self.get_children('/inference/workers'))

    def remove(self, wid):
        path = f'/inference/workers/{wid}'
        del self.nodes[path]
        if path in self.data_callbacks:
            self.data_callbacks[path](None, None, None)
        self.children_callback(self.get_children('/inference/workers'))


class DiscoveryWatchTests(unittest.TestCase):
    def setUp(self):
        self.client = WatchClient()
        self.client.put('a')
        self.client.put('b')
        self.snapshots = []
        self.lock = threading.Lock()
        def receive(workers):
            with self.lock:
                self.snapshots.append(dict(workers))
        with patch('system.zookeeper_registry.KazooClient', return_value=self.client):
            self.discovery = WorkerDiscovery(on_change=receive)
        self.addCleanup(self.discovery.stop)

    def wait_snapshot(self, expected):
        end = time.monotonic() + 3
        while time.monotonic() < end:
            with self.lock:
                if self.snapshots and self.snapshots[-1] == expected:
                    return
            time.sleep(.01)
        self.fail(f'Expected {expected}, got {self.snapshots}')

    def test_initial_callbacks_never_publish_a_partial_membership(self):
        expected = {'a': WorkerInfo('a:50051', 'READY'), 'b': WorkerInfo('b:50051', 'READY')}
        self.wait_snapshot(expected)
        self.assertTrue(all(set(s) == {'a', 'b'} for s in self.snapshots))

    def test_draining_retains_other_workers_and_installs_one_watch(self):
        path = '/inference/workers/a'
        data = b'{"address":"a:50051","state":"DRAINING"}'
        self.client.nodes[path] = (data, 99)
        self.client.data_callbacks[path](data, None, None)
        self.wait_snapshot({'a': WorkerInfo('a:50051', 'DRAINING'), 'b': WorkerInfo('b:50051', 'READY')})
        self.assertTrue(all(count == 1 for count in self.client.watch_count.values()))

    def test_same_id_can_rejoin_without_duplicate_watch(self):
        self.client.remove('a')
        self.wait_snapshot({'b': WorkerInfo('b:50051', 'READY')})
        self.client.put('a')
        self.wait_snapshot({'a': WorkerInfo('a:50051', 'READY'), 'b': WorkerInfo('b:50051', 'READY')})
        self.assertEqual(self.client.watch_count['/inference/workers/a'], 1)

    def test_read_failure_does_not_publish_empty_membership(self):
        expected = {'a': WorkerInfo('a:50051', 'READY'), 'b': WorkerInfo('b:50051', 'READY')}
        self.wait_snapshot(expected)
        self.client.fail_reads = True
        try:
            self.client.data_callbacks['/inference/workers/a'](None, None, None)
            time.sleep(.1)
            self.assertEqual(self.discovery.get_workers(), expected)
            self.assertTrue(all(set(s) == {'a', 'b'} for s in self.snapshots))
        finally:
            self.client.fail_reads = False


class DrainingAdmissionTests(unittest.TestCase):
    def test_rejects_new_work_but_completes_accepted_batches(self):
        fixture = fixtures.HandoffTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        worker = fixture.pool._workers['dead']
        active, queued, new = fixture.task(), fixture.task(), fixture.task()
        self.assertTrue(worker.try_submit([active]))
        self.assertTrue(fixture.dead.started.wait(2))
        self.assertTrue(worker.try_submit([queued]))
        worker.set_routing_state('DRAINING')
        self.assertFalse(worker.try_submit([new]))
        fixture.dead.blocked = False
        fixture.wait_done([active, queued])
        self.assertEqual([active.status, queued.status], [TaskStatus.DONE, TaskStatus.DONE])
        self.assertEqual([active.retry_count, queued.retry_count], [0, 0])
        self.assertEqual(new.status, TaskStatus.PENDING)
        self.assertFalse(worker.try_submit([new]), "An empty draining proxy must reject new work")
