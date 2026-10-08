"""
zookeeper_registry.py
=====================
Worker registration (worker side) and discovery (scheduler side) via ZooKeeper.

Workers create ephemeral znodes, which disappear when their ZooKeeper session
expires. Registration is restored after reconnection with a new session.
"""

import logging
import threading
from typing import Callable, Dict, Optional

from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import KazooException, NodeExistsError

logger = logging.getLogger(__name__)

_WORKERS_PATH = "/inference/workers"


class WorkerRegistry:
    """Used by each worker process to register / deregister itself."""

    _RETRY_INTERVAL = 1.0

    def __init__(self, zk_hosts: str = "localhost:2181"):
        """Open a ZooKeeper session and start a separate registration recovery thread."""
        self._worker_id: Optional[int] = None
        self._address: Optional[str] = None
        self._registered_session: Optional[int] = None
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._wakeup = threading.Event()
        self._zk = KazooClient(hosts=zk_hosts)
        self._zk.add_listener(self._on_state_change)
        try:
            self._zk.start()
            self._zk.ensure_path(_WORKERS_PATH)
        except Exception:
            self._zk.stop()
            self._zk.close()
            raise
        self._thread = threading.Thread(
            target=self._registration_loop, daemon=True, name="WorkerRegistration"
        )
        self._thread.start()

    def _on_state_change(self, state) -> None:
        # Kazoo listeners must return promptly: no network calls or locks here.
        """Wake recovery without blocking the Kazoo event callback with network work."""
        if state == KazooState.LOST:
            logger.warning("ZooKeeper session lost; registration will be restored")
        if not self._stop.is_set():
            self._wakeup.set()

    def register(self, worker_id: int, address: str) -> None:
        """Set the worker identity and retry transient or ownership conflicts in the background."""
        with self._lock:
            if self._stop.is_set():
                raise RuntimeError("WorkerRegistry is closed")
            self._worker_id = worker_id
            self._address = address
            try:
                self._ensure_registered()
            except KazooException as exc:
                # A previous session may still own this identity at startup.
                # Keep the worker alive; the registration loop retries without
                # overwriting or deleting another session's node.
                self._registered_session = None
                logger.warning("Initial worker registration pending; will retry: %s", exc)
        self._wakeup.set()

    def _ensure_registered(self) -> None:
        """Create this session's ephemeral node without taking another session's identity."""
        path = f"{_WORKERS_PATH}/worker-{self._worker_id}"
        session = self._zk.client_id[0]
        data = self._address.encode()
        self._zk.ensure_path(_WORKERS_PATH)
        try:
            self._zk.create(path, data, ephemeral=True)
        except NodeExistsError:
            current_data, stat = self._zk.get(path)
            if stat.ephemeralOwner != session:
                # Updating a different session's node does not transfer ownership.
                # Wait for that node to disappear rather than steal its identity.
                raise NodeExistsError(f"Worker identity already owned: {path}")
            if current_data != data:
                self._zk.set(path, data)
        if self._registered_session != session:
            logger.info("Registered worker-%d at %s", self._worker_id, self._address)
        self._registered_session = session

    def _registration_loop(self) -> None:
        """Restore registration after session loss or a pending startup registration."""
        while not self._stop.is_set():
            changed = self._wakeup.wait(self._RETRY_INTERVAL)
            self._wakeup.clear()
            if self._stop.is_set():
                break
            if not self._zk.connected:
                continue
            with self._lock:
                if self._stop.is_set() or self._worker_id is None:
                    continue
                session = self._zk.client_id[0]
                if not changed and self._registered_session == session:
                    continue
                try:
                    self._ensure_registered()
                except KazooException as exc:
                    self._registered_session = None
                    logger.warning("Worker registration failed; will retry: %s", exc)

    def deregister(self) -> None:
        """Stop recovery and close the session, removing only its own ephemeral nodes."""
        if self._stop.is_set():
            return
        self._stop.set()
        self._wakeup.set()
        self._zk.remove_listener(self._on_state_change)
        # Closing the session removes only nodes owned by this session, and
        # stopping Kazoo releases any registration call blocked by disconnects.
        self._zk.stop()
        self._thread.join(timeout=3)
        self._zk.close()


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
        """Watch registration children and notify the pool when membership changes."""
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
        """Read the currently registered worker addresses."""
        children = self._zk.get_children(_WORKERS_PATH)
        return self._read_workers(children)

    def _read_workers(self, children) -> Dict[str, str]:
        """Resolve registration node names to their advertised gRPC addresses."""
        workers = {}
        for child in children:
            data, _ = self._zk.get(f"{_WORKERS_PATH}/{child}")
            workers[child] = data.decode()
        return workers

    def stop(self) -> None:
        """Stop the discovery client and its background connection."""
        self._zk.stop()
