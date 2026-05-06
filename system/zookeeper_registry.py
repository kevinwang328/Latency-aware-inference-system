"""
zookeeper_registry.py
=====================
Worker registration (worker side) and discovery (scheduler side) via ZooKeeper.

Workers create ephemeral znodes so they are automatically removed when the
process dies, giving the scheduler instant failure detection.
"""

import logging
from typing import Callable, Dict, Optional

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError

logger = logging.getLogger(__name__)

_WORKERS_PATH = "/inference/workers"


class WorkerRegistry:
    """Used by each worker process to register / deregister itself."""

    def __init__(self, zk_hosts: str = "localhost:2181"):
        self._zk = KazooClient(hosts=zk_hosts)
        self._zk.start()
        self._zk.ensure_path(_WORKERS_PATH)
        self._worker_id: Optional[int] = None

    def register(self, worker_id: int, address: str) -> None:
        self._worker_id = worker_id
        path = f"{_WORKERS_PATH}/worker-{worker_id}"
        try:
            self._zk.create(path, address.encode(), ephemeral=True)
        except NodeExistsError:
            self._zk.set(path, address.encode())
        logger.info("Registered worker-%d at %s", worker_id, address)

    def deregister(self) -> None:
        if self._worker_id is not None:
            path = f"{_WORKERS_PATH}/worker-{self._worker_id}"
            if self._zk.exists(path):
                self._zk.delete(path)
        self._zk.stop()


class WorkerDiscovery:
    """
    Used by the WorkerPool (scheduler side) to discover available workers.
    Calls on_change(workers: Dict[str, str]) whenever the worker set changes,
    where the dict maps worker-id → "host:port".
    """

    def __init__(
        self,
        zk_hosts: str = "localhost:2181",
        on_change: Optional[Callable[[Dict[str, str]], None]] = None,
    ):
        self._zk = KazooClient(hosts=zk_hosts)
        self._on_change = on_change
        self._zk.start()
        self._zk.ensure_path(_WORKERS_PATH)

        @self._zk.ChildrenWatch(_WORKERS_PATH)
        def _watch(children):
            workers = self._read_workers(children)
            logger.info("Worker set changed: %s", list(workers.keys()))
            if self._on_change:
                self._on_change(workers)

    def get_workers(self) -> Dict[str, str]:
        children = self._zk.get_children(_WORKERS_PATH)
        return self._read_workers(children)

    def _read_workers(self, children) -> Dict[str, str]:
        workers = {}
        for child in children:
            data, _ = self._zk.get(f"{_WORKERS_PATH}/{child}")
            workers[child] = data.decode()
        return workers

    def stop(self) -> None:
        self._zk.stop()
