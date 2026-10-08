"""Regression tests for registration recovery, without a ZooKeeper server."""

import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from kazoo.client import KazooState
from kazoo.exceptions import ConnectionLoss, NodeExistsError, NoNodeError
from system.zookeeper_registry import WorkerRegistry


class FakeClient:
    def __init__(self):
        self.connected = False
        self.client_id = (1, b"password")
        self.nodes = {}
        self.listeners = []
        self.fail_creates = 0
        self.create_threads = []

    def add_listener(self, listener):
        self.listeners.append(listener)

    def remove_listener(self, listener):
        self.listeners.remove(listener)

    def emit(self, state):
        self.connected = state == KazooState.CONNECTED
        for listener in list(self.listeners):
            listener(state)

    def start(self):
        self.emit(KazooState.CONNECTED)

    def ensure_path(self, path):
        pass

    def create(self, path, data, ephemeral=False):
        self.create_threads.append(threading.get_ident())
        if self.fail_creates:
            self.fail_creates -= 1
            raise ConnectionLoss()
        if path in self.nodes:
            raise NodeExistsError()
        self.nodes[path] = (data, self.client_id[0] if ephemeral else 0)

    def get(self, path):
        if path not in self.nodes:
            raise NoNodeError()
        data, owner = self.nodes[path]
        return data, SimpleNamespace(ephemeralOwner=owner)

    def set(self, path, data):
        self.nodes[path] = (data, self.nodes[path][1])

    def exists(self, path):
        if path in self.nodes:
            return SimpleNamespace(ephemeralOwner=self.nodes[path][1])

    def delete(self, path):
        del self.nodes[path]

    def stop(self):
        self.nodes = {p: v for p, v in self.nodes.items() if v[1] != self.client_id[0]}
        self.emit(KazooState.LOST)

    def close(self):
        pass

    def expire(self):
        self.nodes.clear()
        self.emit(KazooState.LOST)
        self.client_id = (self.client_id[0] + 1, b"new password")


class RegistrationTests(unittest.TestCase):
    def setUp(self):
        self.client = FakeClient()
        self.client_patch = patch("system.zookeeper_registry.KazooClient", return_value=self.client)
        self.client_patch.start()
        self.addCleanup(self.client_patch.stop)
        interval = patch.object(WorkerRegistry, "_RETRY_INTERVAL", .02, create=True)
        interval.start()
        self.addCleanup(interval.stop)
        self.registry = WorkerRegistry()
        self.addCleanup(self.registry.deregister)
        self.path = "/inference/workers/worker-7"

    def wait_registered(self):
        end = time.monotonic() + 1
        while time.monotonic() < end:
            if self.client.nodes.get(self.path) == (b"worker-7:50051", self.client.client_id[0]):
                return
            time.sleep(.01)
        self.fail("worker was not re-registered in the new session")

    def test_expired_session_recreates_registration(self):
        self.registry.register(7, "worker-7:50051")
        self.client.expire()
        self.client.emit(KazooState.CONNECTED)
        self.wait_registered()

    def test_transient_registration_failure_is_retried(self):
        self.registry.register(7, "worker-7:50051")
        self.client.expire()
        self.client.fail_creates = 1
        self.client.emit(KazooState.CONNECTED)
        self.wait_registered()
        self.assertEqual(self.client.fail_creates, 0)

    def test_callback_does_not_do_network_work(self):
        self.registry.register(7, "worker-7:50051")
        self.client.expire()
        self.client.create_threads.clear()
        callback_thread = threading.get_ident()
        self.client.emit(KazooState.CONNECTED)
        self.wait_registered()
        self.assertNotIn(callback_thread, self.client.create_threads)

    def test_short_disconnect_preserves_session_owner(self):
        self.registry.register(7, "worker-7:50051")
        self.client.emit(KazooState.SUSPENDED)
        self.client.emit(KazooState.CONNECTED)
        self.wait_registered()

    def test_foreign_session_node_is_not_overwritten_or_deleted(self):
        self.client.nodes[self.path] = (b"other-worker:50051", 99)
        self.registry.register(7, "worker-7:50051")
        self.registry.deregister()
        self.assertEqual(self.client.nodes[self.path], (b"other-worker:50051", 99))

    def test_startup_waits_for_old_session_then_registers(self):
        self.client.nodes[self.path] = (b"old-worker:50051", 99)
        self.registry.register(7, "worker-7:50051")
        self.assertEqual(self.client.nodes[self.path], (b"old-worker:50051", 99))
        del self.client.nodes[self.path]
        self.wait_registered()

    def test_initial_connection_loss_is_retried(self):
        self.client.fail_creates = 1
        self.registry.register(7, "worker-7:50051")
        self.wait_registered()

    def test_deregister_prevents_recreation(self):
        self.registry.register(7, "worker-7:50051")
        self.registry.deregister()
        self.client.emit(KazooState.CONNECTED)
        time.sleep(.06)
        self.assertNotIn(self.path, self.client.nodes)


if __name__ == "__main__":
    unittest.main()
