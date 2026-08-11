"""The S3 artifact client, against a fake bucket and optionally a real MinIO.

``FakeS3`` is a real socket speaking real HTTP: it stores objects in a dict,
answers ``ListObjectsV2`` with the XML shape MinIO does, and records the headers
it was sent so the signing can be asserted. ``RealMinIOTests`` runs the same
operations against an actual server when ``FLOWFORGE_TEST_S3=host:port`` is set:

    docker run -d --rm --name ff-minio -p 9000:9000 \
      -e MINIO_ROOT_USER=flowforge -e MINIO_ROOT_PASSWORD=flowforge123 \
      minio/minio server /data
    FLOWFORGE_TEST_S3=127.0.0.1:9000 python3 -m unittest discover -s tests -t .
"""

import asyncio
import json
import os
import unittest

from flowforge import Graph, WorkflowEngine
from flowforge.artifacts import ArtifactError, ArtifactStore, S3Client

REAL_S3 = os.environ.get("FLOWFORGE_TEST_S3")


class FakeS3:
    """Enough of the S3 REST API to drive S3Client."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.buckets: set[str] = set()
        self.requests: list[dict] = []
        self.server: asyncio.AbstractServer | None = None

    async def start(self) -> int:
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        return self.server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()

    @property
    def last(self) -> dict:
        return self.requests[-1]

    async def _handle(self, reader, writer):
        while True:
            request_line = await reader.readline()
            if not request_line.strip():
                break
            method, target, _ = request_line.decode().split()
            headers = {}
            while True:
                line = await reader.readline()
                if line in (b"\r\n", b"\n", b""):
                    break
                name, _, value = line.decode().partition(":")
                headers[name.strip().lower()] = value.strip()
            length = int(headers.get("content-length", 0) or 0)
            body = await reader.readexactly(length) if length else b""
            self.requests.append(
                {"method": method, "target": target, "headers": headers, "body": body}
            )

            status, payload, content_type = self._dispatch(method, target, body)
            writer.write(
                f"HTTP/1.1 {status} {'OK' if status < 300 else 'Error'}\r\n"
                f"Content-Type: {content_type}\r\n"
                f"Content-Length: {len(payload)}\r\n"
                f"Connection: keep-alive\r\n\r\n".encode()
                + payload
            )
            await writer.drain()
        writer.close()

    def _dispatch(self, method, target, body):
        path, _, query = target.partition("?")
        segments = [s for s in path.split("/") if s]
        bucket = segments[0] if segments else ""
        key = "/".join(segments[1:])

        if method == "PUT" and not key:
            if bucket in self.buckets:
                return 409, b"<Error><Code>BucketAlreadyOwnedByYou</Code></Error>", "application/xml"
            self.buckets.add(bucket)
            return 200, b"", "application/xml"
        if method == "PUT":
            self.objects[key] = body
            return 200, b"", "application/xml"
        if method == "GET" and "list-type=2" in query:
            prefix = ""
            for part in query.split("&"):
                if part.startswith("prefix="):
                    from urllib.parse import unquote

                    prefix = unquote(part[len("prefix=") :])
            listed = "".join(
                f"<Contents><Key>{k}</Key><Size>{len(v)}</Size></Contents>"
                for k, v in sorted(self.objects.items())
                if k.startswith(prefix)
            )
            xml = (
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                f"<Name>{bucket}</Name>{listed}</ListBucketResult>"
            )
            return 200, xml.encode(), "application/xml"
        if method == "GET":
            if key not in self.objects:
                return 404, b"<Error><Code>NoSuchKey</Code></Error>", "application/xml"
            return 200, self.objects[key], "application/octet-stream"
        if method == "DELETE":
            existed = self.objects.pop(key, None) is not None
            return (204 if existed else 404), b"", "application/xml"
        return 405, b"<Error><Code>MethodNotAllowed</Code></Error>", "application/xml"


class S3ClientTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fake = FakeS3()
        port = await self.fake.start()
        self.client = S3Client(
            endpoint=f"http://127.0.0.1:{port}",
            access_key="key",
            secret_key="secret",
            bucket="flowforge",
        )

    async def asyncTearDown(self):
        await self.client.close()
        await self.fake.stop()

    async def test_put_and_get_round_trip(self):
        stored = await self.client.put("a/b.txt", b"hello", "text/plain")
        self.assertEqual(stored.key, "flowforge/a/b.txt")
        self.assertEqual(stored.size, 5)
        self.assertEqual(await self.client.get("a/b.txt"), b"hello")

    async def test_requests_are_signed(self):
        await self.client.put("signed.txt", b"x")
        sent = self.fake.last

        self.assertTrue(sent["headers"]["authorization"].startswith("AWS4-HMAC-SHA256 "))
        self.assertIn("/s3/aws4_request", sent["headers"]["authorization"])
        self.assertIn("x-amz-content-sha256", sent["headers"])

    async def test_path_style_addressing_puts_the_bucket_in_the_path(self):
        await self.client.put("k", b"v")
        self.assertEqual(self.fake.last["target"], "/flowforge/k")

    async def test_virtual_host_addressing_puts_it_in_the_host(self):
        client = S3Client(
            endpoint="https://s3.example.com",
            access_key="k",
            secret_key="s",
            bucket="bkt",
            path_style=False,
        )
        self.assertEqual(client.url_for("obj"), "https://bkt.s3.example.com/obj")

    async def test_missing_object_is_an_artifact_error(self):
        with self.assertRaises(ArtifactError) as raised:
            await self.client.get("nope")
        self.assertIn("404", str(raised.exception))

    async def test_delete_reports_whether_it_removed_anything(self):
        await self.client.put("gone", b"x")
        self.assertTrue(await self.client.delete("gone"))
        self.assertFalse(await self.client.delete("gone"))

    async def test_list_keys_parses_the_xml_and_honours_the_prefix(self):
        for key in ("runs/1/result.json", "runs/2/result.json", "other/x"):
            await self.client.put(key, b"{}")

        self.assertEqual(
            await self.client.list_keys("runs/"),
            ["runs/1/result.json", "runs/2/result.json"],
        )
        self.assertEqual(len(await self.client.list_keys()), 3)

    async def test_ensure_bucket_tolerates_an_existing_bucket(self):
        await self.client.ensure_bucket()
        await self.client.ensure_bucket()  # 409 is success by another name
        self.assertIn("flowforge", self.fake.buckets)

    async def test_missing_credentials_are_reported(self):
        client = S3Client(endpoint="http://127.0.0.1:1", access_key="", secret_key="")
        with self.assertRaises(ArtifactError) as raised:
            await client.put("k", b"v")
        self.assertIn("AWS_ACCESS_KEY_ID", str(raised.exception))


class ArtifactStoreTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fake = FakeS3()
        port = await self.fake.start()
        self.store = ArtifactStore(
            S3Client(
                endpoint=f"http://127.0.0.1:{port}",
                access_key="key",
                secret_key="secret",
            )
        )

    async def asyncTearDown(self):
        await self.store.close()
        await self.fake.stop()

    async def result(self):
        graph = Graph.from_file("examples/diamond.json")
        return await WorkflowEngine(graph).run({"q": "hi"})

    async def test_a_run_result_round_trips_as_json(self):
        result = await self.result()
        stored = await self.store.save_result(result)

        self.assertEqual(stored.content_type, "application/json")
        loaded = await self.store.load_result(result.run_id)
        self.assertEqual(loaded["run"], result.run_id)
        self.assertEqual(loaded["status"], "completed")
        self.assertEqual(loaded["outputs"], result.outputs)

    async def test_the_stored_document_records_per_node_timings(self):
        result = await self.result()
        await self.store.save_result(result)
        loaded = await self.store.load_result(result.run_id)

        self.assertEqual(set(loaded["nodes"]), set(result.nodes))
        for record in loaded["nodes"].values():
            self.assertIn("status", record)
            self.assertIn("ms", record)

    async def test_blobs_live_under_the_run(self):
        stored = await self.store.save_blob("run1", "report.md", b"# hi", "text/markdown")
        self.assertEqual(stored.key, "flowforge/runs/run1/report.md")

    async def test_list_runs_deduplicates_by_run_id(self):
        await self.store.save_blob("r1", "a.txt", b"a")
        await self.store.save_blob("r1", "b.txt", b"b")
        await self.store.save_blob("r2", "a.txt", b"a")

        self.assertEqual(await self.store.list_runs(), ["r1", "r2"])


@unittest.skipUnless(REAL_S3, "set FLOWFORGE_TEST_S3=host:port to run these")
class RealMinIOTests(unittest.IsolatedAsyncioTestCase):
    """The same operations against an actual MinIO."""

    async def asyncSetUp(self):
        host, _, port = REAL_S3.partition(":")
        self.client = S3Client(
            endpoint=f"http://{host}:{int(port or 9000)}",
            access_key=os.environ.get("FLOWFORGE_TEST_S3_KEY", "flowforge"),
            secret_key=os.environ.get("FLOWFORGE_TEST_S3_SECRET", "flowforge123"),
            bucket=os.environ.get("FLOWFORGE_TEST_S3_BUCKET", "flowforge-test"),
        )
        await self.client.ensure_bucket()
        self.store = ArtifactStore(self.client)

    async def asyncTearDown(self):
        for key in await self.client.list_keys():
            await self.client.delete(key)
        await self.client.close()

    async def test_put_get_delete_against_a_real_server(self):
        await self.client.put("hello.txt", b"real bytes", "text/plain")
        self.assertEqual(await self.client.get("hello.txt"), b"real bytes")
        self.assertIn("hello.txt", await self.client.list_keys())
        self.assertTrue(await self.client.delete("hello.txt"))

    async def test_sigv4_is_accepted_by_minio(self):
        """The signature is the thing a fake cannot check: MinIO verifies it."""
        await self.client.put("signed/ok.json", b'{"a":1}', "application/json")
        self.assertEqual(json.loads(await self.client.get("signed/ok.json")), {"a": 1})

    async def test_a_run_result_round_trips_through_real_object_storage(self):
        result = await WorkflowEngine(Graph.from_file("examples/diamond.json")).run(
            {"q": "hi"}
        )
        await self.store.save_result(result)
        loaded = await self.store.load_result(result.run_id)

        self.assertEqual(loaded["outputs"], result.outputs)
        self.assertIn(result.run_id, await self.store.list_runs())


if __name__ == "__main__":
    unittest.main()
