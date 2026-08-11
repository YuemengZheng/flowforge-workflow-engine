"""Run artifacts in object storage, over the S3 API.

Why object storage at all: a checkpoint is small, keyed and hot, which is what
Redis and SQL are for. A run's *output* is neither — a report, a diff, a batch of
per-item results — and putting a megabyte of it in a cache row is how caches turn
into databases by accident. So artifacts go to S3, and what the store keeps is a
key.

MinIO is the deployment target and is what the tests run against, but nothing
here is MinIO-specific: it is the S3 REST API with SigV4, so the same client
speaks to AWS by changing the endpoint. Signing is the same
:func:`~flowforge.providers.sigv4_headers` the Bedrock adapter uses — S3 was the
second caller, which is why that function is not buried in the LLM code.

Path-style addressing (``endpoint/bucket/key``) by default, because
``bucket.endpoint`` requires DNS a local MinIO does not have.
"""

from __future__ import annotations

import json
import os
import xml.etree.ElementTree as ElementTree
from dataclasses import dataclass
from typing import Any, Mapping
from urllib.parse import quote

from .engine import RunResult
from .errors import FlowForgeError
from .pool import HTTPConnectionPool
from .providers import sigv4_headers


class ArtifactError(FlowForgeError):
    """Object storage refused, or could not be reached."""


@dataclass
class StoredArtifact:
    """Where something was put, and what went in."""

    key: str
    size: int
    content_type: str

    @property
    def uri(self) -> str:
        return f"s3://{self.key}"


class S3Client:
    """The five S3 calls this project needs, signed with SigV4.

    Deliberately not a general S3 client: no multipart, no versioning, no ACLs.
    A run artifact is one PUT and one GET, and the honest way to keep a
    hand-rolled client correct is to keep it small.
    """

    def __init__(
        self,
        endpoint: str = "",
        access_key: str = "",
        secret_key: str = "",
        bucket: str = "flowforge",
        region: str = "us-east-1",
        pool: HTTPConnectionPool | None = None,
        path_style: bool = True,
    ) -> None:
        self.endpoint = (
            endpoint or os.environ.get("FLOWFORGE_S3_ENDPOINT", "http://127.0.0.1:9000")
        ).rstrip("/")
        self.access_key = access_key or os.environ.get("AWS_ACCESS_KEY_ID", "")
        self.secret_key = secret_key or os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        self.bucket = bucket
        self.region = region
        self.path_style = path_style
        self._pool = pool or HTTPConnectionPool()
        self._owns_pool = pool is None

    def url_for(self, key: str = "") -> str:
        encoded = quote(key, safe="/")
        if self.path_style:
            return f"{self.endpoint}/{self.bucket}" + (f"/{encoded}" if key else "")
        scheme, _, host = self.endpoint.partition("://")
        return f"{scheme}://{self.bucket}.{host}" + (f"/{encoded}" if key else "/")

    async def _send(
        self,
        method: str,
        url: str,
        body: bytes = b"",
        content_type: str | None = None,
        ok: tuple[int, ...] = (200, 204),
    ) -> tuple[int, dict[str, str], bytes]:
        if not self.access_key or not self.secret_key:
            raise ArtifactError(
                "no S3 credentials: pass access_key/secret_key or set "
                "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
            )
        headers = sigv4_headers(
            method,
            url,
            body,
            region=self.region,
            service="s3",
            access_key=self.access_key,
            secret_key=self.secret_key,
        )
        if content_type:
            headers["Content-Type"] = content_type
        status, response_headers, raw = await self._pool.request(
            method, url, body, headers
        )
        if status not in ok:
            raise ArtifactError(
                f"S3 {method} {url} -> {status}: "
                f"{raw.decode('utf-8', errors='replace')[:400]}"
            )
        return status, response_headers, raw

    async def ensure_bucket(self) -> None:
        """Create the bucket unless it is already there.

        ``409 BucketAlreadyOwnedByYou`` is success by another name — two workers
        starting at once must not make one of them fail.
        """
        url = self.url_for()
        try:
            await self._send("PUT", url, ok=(200, 204))
        except ArtifactError as exc:
            if "409" not in str(exc) and "BucketAlready" not in str(exc):
                raise

    async def put(
        self, key: str, data: bytes, content_type: str = "application/octet-stream"
    ) -> StoredArtifact:
        await self._send("PUT", self.url_for(key), data, content_type, ok=(200,))
        return StoredArtifact(f"{self.bucket}/{key}", len(data), content_type)

    async def get(self, key: str) -> bytes:
        _, _, raw = await self._send("GET", self.url_for(key), ok=(200,))
        return raw

    async def delete(self, key: str) -> bool:
        status, _, _ = await self._send(
            "DELETE", self.url_for(key), ok=(200, 204, 404)
        )
        return status in (200, 204)

    async def list_keys(self, prefix: str = "") -> list[str]:
        """``ListObjectsV2``. XML in, sorted keys out."""
        query = "list-type=2" + (f"&prefix={quote(prefix, safe='')}" if prefix else "")
        _, _, raw = await self._send("GET", f"{self.url_for()}?{query}", ok=(200,))
        try:
            root = ElementTree.fromstring(raw.decode("utf-8"))
        except ElementTree.ParseError as exc:
            raise ArtifactError(f"unparseable ListObjectsV2 response: {exc}") from exc
        # Namespace-agnostic on purpose: AWS and MinIO disagree about whether the
        # default namespace is present, and matching on the local name is stable.
        return sorted(
            element.text
            for element in root.iter()
            if element.tag.rsplit("}", 1)[-1] == "Key" and element.text
        )

    async def close(self) -> None:
        if self._owns_pool:
            await self._pool.close()


class ArtifactStore:
    """Run outputs as JSON objects under ``runs/<run_id>/``."""

    def __init__(self, client: S3Client, prefix: str = "runs") -> None:
        self.client = client
        self.prefix = prefix.strip("/")

    def key_for(self, run_id: str, name: str = "result.json") -> str:
        return f"{self.prefix}/{run_id}/{name}"

    async def save_result(self, result: RunResult) -> StoredArtifact:
        """Store what a run produced, and what it cost, as one object."""
        document = {
            "run": result.run_id,
            "status": result.status.value,
            "stats": result.stats.as_dict(),
            "outputs": result.outputs,
            "failures": result.failures,
            "nodes": {
                node_id: {
                    "status": record.status.value,
                    "ms": round(record.duration_ms, 3),
                    "attempts": record.attempts,
                    "wave": record.wave,
                }
                for node_id, record in result.nodes.items()
            },
        }
        raw = json.dumps(document, ensure_ascii=False, default=str).encode("utf-8")
        return await self.client.put(
            self.key_for(result.run_id), raw, "application/json"
        )

    async def load_result(self, run_id: str) -> dict[str, Any]:
        raw = await self.client.get(self.key_for(run_id))
        return json.loads(raw.decode("utf-8"))

    async def save_blob(
        self, run_id: str, name: str, data: bytes, content_type: str = "text/plain"
    ) -> StoredArtifact:
        return await self.client.put(self.key_for(run_id, name), data, content_type)

    async def list_runs(self) -> list[str]:
        keys = await self.client.list_keys(f"{self.prefix}/")
        return sorted({key.split("/")[1] for key in keys if "/" in key})

    async def close(self) -> None:
        await self.client.close()


def store_from_env(**overrides: Any) -> ArtifactStore:
    """Build a store from ``FLOWFORGE_S3_*`` / ``AWS_*`` environment variables."""
    settings: dict[str, Any] = {
        "endpoint": os.environ.get("FLOWFORGE_S3_ENDPOINT", ""),
        "bucket": os.environ.get("FLOWFORGE_S3_BUCKET", "flowforge"),
        "region": os.environ.get("FLOWFORGE_S3_REGION", "us-east-1"),
    }
    settings.update({k: v for k, v in overrides.items() if v is not None})
    return ArtifactStore(S3Client(**settings))
