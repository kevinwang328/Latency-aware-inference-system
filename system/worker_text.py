"""Version-pinned adapter for the existing private Vast text service.

Only retrieval and CLAP reranking are scheduled here. LLM orchestration and
artifact verification/commit remain in the product backend. HTTP timeouts do
not promise to cancel computation already accepted by the Vast service.
"""
import http.client
import json
import logging
import math
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from .worker_v73 import WorkerError

logger = logging.getLogger(__name__)
MODEL = "text_v1.2.2"
KIND = "synthx_text_v122"
ROUTES = {"retrieve": "/text-v1/retrieve", "rerank": "/text-v1/rerank"}
MAX_RESULT = 8 * 1024 * 1024

class TextWorker:
    def __init__(self, base_url=None):
        self.base_url = (base_url or os.environ.get("TEXT_SERVICE_URL", "")).rstrip("/")
        parsed = urllib.parse.urlsplit(self.base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path:
            raise ValueError("TEXT_SERVICE_URL must be a service origin without credentials or path")
        self._slot = threading.Lock()

    @staticmethod
    def _check(context, deadline):
        if deadline is not None and (not math.isfinite(deadline) or time.monotonic() >= deadline):
            raise WorkerError("deadline_exceeded", "text task deadline exceeded")
        if context is not None and not context.is_active():
            raise WorkerError("cancelled", "text task caller disconnected")

    def process(self, data, context, *, deadline=None):
        self._check(context, deadline)
        if not isinstance(data, dict) or set(data) != {"kind", "model", "operation", "payload"} or data.get("kind") != KIND or data.get("model") != MODEL:
            raise WorkerError("invalid_spec", "text task must pin text_v1.2.2")
        operation, payload = data["operation"], data["payload"]
        if not isinstance(operation, str) or operation not in ROUTES or not isinstance(payload, dict):
            raise WorkerError("invalid_spec", "only text retrieve/rerank operations are allowed")
        if operation == "retrieve" and payload.get("pipeline_version") not in (MODEL, "text_v1.2_refinement"):
            raise WorkerError("invalid_spec", "retrieval pipeline does not match text worker")
        try:
            body = json.dumps(payload, allow_nan=False).encode()
        except (ValueError, TypeError) as exc:
            raise WorkerError("invalid_spec", "text payload must be finite JSON") from exc
        if len(body) > 256 * 1024:
            raise WorkerError("invalid_spec", "text payload exceeds 256 KiB")
        if not self._slot.acquire(blocking=False):
            raise WorkerError("busy", "text worker is busy")
        started = time.monotonic()
        try:
            self._check(context, deadline)
            timeout = 600 if operation == "retrieve" else 180
            if deadline is not None:
                timeout = min(timeout, max(.001, deadline - time.monotonic()))
            request = urllib.request.Request(self.base_url + ROUTES[operation], data=body,
                headers={"Content-Type":"application/json"}, method="POST")
            try:
                with urllib.request.urlopen(request, timeout=timeout) as response:
                    content = response.read(MAX_RESULT + 1)
            except urllib.error.HTTPError as exc:
                code = "invalid_spec" if exc.code in (400, 404, 422) else "overloaded" if exc.code in (429, 503) else "worker_error"
                raise WorkerError(code, "Vast text service returned HTTP %s" % exc.code) from exc
            except (TimeoutError, urllib.error.URLError, OSError, http.client.HTTPException) as exc:
                code = "deadline_exceeded" if isinstance(exc, TimeoutError) or isinstance(getattr(exc, "reason", None), TimeoutError) else "transport_error"
                raise WorkerError(code, "Vast text service request failed") from exc
            self._check(context, deadline)
            if len(content) > MAX_RESULT:
                raise WorkerError("invalid_result", "text result exceeds 8 MiB")
            try:
                result = json.loads(content)
                json.dumps(result, allow_nan=False)
            except (ValueError, UnicodeDecodeError, TypeError) as exc:
                raise WorkerError("invalid_result", "text result must be finite JSON") from exc
            if not isinstance(result, dict):
                raise WorkerError("invalid_result", "text result must be an object")
            if operation == "retrieve" and result.get("model") != payload["pipeline_version"]:
                raise WorkerError("invalid_result", "Vast text service returned a different model version")
            return {"model":MODEL, "operation":operation, "result":result}
        finally:
            logger.info("Text worker model=%s operation=%s elapsed_seconds=%.3f", MODEL, operation, time.monotonic()-started)
            self._slot.release()
