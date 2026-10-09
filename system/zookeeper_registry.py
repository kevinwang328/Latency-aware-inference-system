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
import json
from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import KazooException, NodeExistsError, NoNodeError
from dataclasses import dataclass
logger = logging.getLogger(__name__)

_WORKERS_PATH = "/inference/workers"


@dataclass(frozen=True)
class WorkerInfo:
    """Immutable metadata for a registered worker."""

    address: str
    state: str


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
        self._state: str = "READY"
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
        data = json.dumps({
            "address": self._address,
            "state": self._state,
        }).encode("utf-8")
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

    def set_state(self, state: str) -> None:
        """Updating the worker's state"""
        with self._lock:
            self._state = state
            self._ensure_registered()


class WorkerDiscovery:
    """Publish complete address/state snapshots through a single refresh thread."""

    _RETRY_INTERVAL = 1.0

    def __init__(
        self,
        zk_hosts: str = "localhost:2181",
        on_change: Optional[Callable[[Dict[str, WorkerInfo]], None]] = None,
    ):
        """Install watches before reading the initial snapshot to avoid missed changes."""
        self._on_change = on_change
        self._lock = threading.Lock()
        self._workers: Dict[str, WorkerInfo] = {}
        self._published_workers: Optional[Dict[str, WorkerInfo]] = None
        self._watched_workers: set[str] = set()
        self._stop = threading.Event()
        self._wakeup = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._zk = KazooClient(hosts=zk_hosts)
        self._zk.add_listener(self._on_state_change)
        try:
            self._zk.start()
            self._zk.ensure_path(_WORKERS_PATH)

            @self._zk.ChildrenWatch(_WORKERS_PATH)
            def _watch(children):
                if self._stop.is_set():
                    return False
                # Kazoo callbacks only schedule work; they never call the pool.
                self._wakeup.set()

            self._refresh_workers()
            self._thread = threading.Thread(
                target=self._refresh_loop, daemon=True, name="WorkerDiscovery"
            )
            self._thread.start()
        except Exception:
            self.stop()
            raise

    def _on_state_change(self, state) -> None:
        """Refresh after reconnection, including unchanged child membership."""
        if state == KazooState.CONNECTED and not self._stop.is_set():
            self._wakeup.set()

    def get_workers(self) -> Dict[str, WorkerInfo]:
        """Return a copy of the last successfully read complete snapshot."""
        with self._lock:
            return dict(self._workers)

    def _refresh_loop(self) -> None:
        """Serialize reads and pool notifications; retain membership on read failures."""
        while not self._stop.is_set():
            self._wakeup.wait()
            self._wakeup.clear()
            if self._stop.is_set():
                break
            try:
                self._refresh_workers()
            except Exception:
                logger.exception("Worker discovery refresh failed; retaining last snapshot")
                if not self._stop.wait(self._RETRY_INTERVAL):
                    self._wakeup.set()

    def _refresh_workers(self) -> None:
        """Subscribe to new paths and then reconcile membership from ZooKeeper."""
        children = self._zk.get_children(_WORKERS_PATH)
        for wid in children:
            if self._stop.is_set():
                return
            if wid not in self._watched_workers:
                self._watch_worker(wid)
                self._watched_workers.add(wid)

        workers = self._read_workers(children)
        if self._stop.is_set():
            return
        with self._lock:
            self._workers = dict(workers)
        if workers != self._published_workers:
            logger.info("Worker set/state changed: %s", workers)
            if self._on_change:
                self._on_change(dict(workers))
            self._published_workers = dict(workers)

    def _read_workers(self, children) -> Dict[str, WorkerInfo]:
        """Read current metadata; a missing node is different from a failed read."""
        workers = {}
        for child in children:
            try:
                data, _ = self._zk.get(f"{_WORKERS_PATH}/{child}")
            except NoNodeError:
                continue
            workers[child] = self._parse_worker(data)
        return workers

    @staticmethod
    def _parse_worker(data: bytes) -> WorkerInfo:
        """Accept JSON registration metadata and legacy address-only nodes."""
        text = data.decode("utf-8").strip()
        if text.startswith("{"):
            metadata = json.loads(text)
            address, state = metadata["address"], metadata["state"]
        else:
            address, state = text, "READY"
        if not isinstance(address, str) or not address:
            raise ValueError("Worker address must be a nonempty string")
        if state not in ("READY", "DRAINING"):
            raise ValueError(f"Unsupported worker state: {state!r}")
        return WorkerInfo(address=address, state=state)

    def _watch_worker(self, worker_id: str) -> None:
        """Keep one watch per path, including deletion and same-ID recreation."""
        path = f"{_WORKERS_PATH}/{worker_id}"

        @self._zk.DataWatch(path)
        def on_data_change(data, stat, event):
            if self._stop.is_set():
                return False
            # Include deletion notifications; the refresh reconciles the full list.
            self._wakeup.set()

    def stop(self) -> None:
        """Wake the refresh thread and release the discovery session and watches."""
        if self._stop.is_set():
            return
        self._stop.set()
        self._wakeup.set()
        self._zk.remove_listener(self._on_state_change)
        self._zk.stop()
        if self._thread and threading.current_thread() is not self._thread:
            self._thread.join(timeout=3)
        self._zk.close()
