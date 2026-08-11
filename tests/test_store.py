"""RESP codec and RedisRunStore.

Two layers of coverage. The fake below is a minimal in-process Redis speaking
real RESP over a real socket — always runs, no daemon needed, and it can force
awkward cases (two-key SCAN pages) that a real server would not produce for a
handful of keys. On top of that, ``RealRedisTests`` runs the same store against
an actual ``redis-server`` when ``FLOWFORGE_TEST_REDIS=host:port`` is set, and
skips otherwise:

    docker run -d --rm --name ff-redis -p 6399:6379 redis:7-alpine
    FLOWFORGE_TEST_REDIS=127.0.0.1:6399 python3 -m unittest discover -s tests -t .
"""

import asyncio
import os
import unittest

from flowforge import (
    Checkpoint,
    Graph,
    MemoryRunStore,
    RedisClient,
    RedisError,
    RedisRunStore,
    RunStatus,
    WorkflowEngine,
)
from flowforge.store import encode_command, read_reply

REAL_REDIS = os.environ.get("FLOWFORGE_TEST_REDIS")


class FakeRedis:
    """SET/GET/DEL/SCAN/PING over RESP. Enough to drive RedisRunStore."""

    def __init__(self) -> None:
        self.data: dict[str, bytes] = {}
        self.expiries: dict[str, int] = {}
        self.commands: list[list[str]] = []
        self.server: asyncio.AbstractServer | None = None

    async def start(self) -> int:
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        return self.server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()

    async def _handle(self, reader, writer):
        try:
            while True:
                header = await reader.readline()
                if not header:
                    return
                argc = int(header[1:-2])
                args = []
                for _ in range(argc):
                    length = int((await reader.readline())[1:-2])
                    args.append((await reader.readexactly(length + 2))[:-2])
                writer.write(self._dispatch([a.decode() for a in args]))
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionResetError):
            return
        finally:
            writer.close()

    def _dispatch(self, args: list[str]) -> bytes:
        self.commands.append(args)
        name = args[0].upper()
        if name == "PING":
            return b"+PONG\r\n"
        if name == "SET":
            key, value = args[1], args[2]
            self.data[key] = value.encode()
            if "EX" in (a.upper() for a in args[3:]):
                index = [a.upper() for a in args].index("EX")
                self.expiries[key] = int(args[index + 1])
            return b"+OK\r\n"
        if name == "GET":
            value = self.data.get(args[1])
            if value is None:
                return b"$-1\r\n"
            return b"$%d\r\n%s\r\n" % (len(value), value)
        if name == "DEL":
            return b":%d\r\n" % sum(self.data.pop(k, None) is not None for k in args[1:])
        if name == "SCAN":
            prefix = args[3].rstrip("*")
            keys = [k for k in sorted(self.data) if k.startswith(prefix)]
            # Deliberately paginate two at a time to exercise the cursor loop.
            cursor = int(args[1])
            page, nxt = keys[cursor : cursor + 2], cursor + 2
            if nxt >= len(keys):
                nxt = 0
            body = b"".join(b"$%d\r\n%s\r\n" % (len(k), k.encode()) for k in page)
            return b"*2\r\n$%d\r\n%d\r\n*%d\r\n%s" % (
                len(str(nxt)), nxt, len(page), body,
            )
        return b"-ERR unknown command '%s'\r\n" % name.encode()


def sample_checkpoint(run_id: str = "r1") -> Checkpoint:
    return Checkpoint(
        run_id=run_id,
        workflow_id="wf",
        fingerprint="fp",
        awaiting={"ask": {"prompt": "ok?"}},
        waves=2,
    )


class CodecTests(unittest.TestCase):
    def test_command_encoding(self):
        self.assertEqual(
            encode_command("SET", "k", 12),
            b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\n12\r\n",
        )

    def test_utf8_arguments_are_length_prefixed_in_bytes(self):
        encoded = encode_command("SET", "键")
        self.assertIn(b"$3\r\n", encoded)  # 3 bytes, not 1 character


class ReplyTests(unittest.IsolatedAsyncioTestCase):
    async def parse(self, raw: bytes):
        reader = asyncio.StreamReader()
        reader.feed_data(raw)
        reader.feed_eof()
        return await read_reply(reader)

    async def test_simple_string(self):
        self.assertEqual(await self.parse(b"+PONG\r\n"), "PONG")

    async def test_integer(self):
        self.assertEqual(await self.parse(b":42\r\n"), 42)

    async def test_bulk_string_and_nil(self):
        self.assertEqual(await self.parse(b"$5\r\nhello\r\n"), b"hello")
        self.assertIsNone(await self.parse(b"$-1\r\n"))

    async def test_array(self):
        self.assertEqual(await self.parse(b"*2\r\n:1\r\n$2\r\nab\r\n"), [1, b"ab"])

    async def test_error_reply_raises(self):
        with self.assertRaises(RedisError) as ctx:
            await self.parse(b"-WRONGTYPE nope\r\n")
        self.assertIn("WRONGTYPE", str(ctx.exception))

    async def test_closed_connection_raises(self):
        with self.assertRaises(RedisError):
            await self.parse(b"")


class RedisRunStoreTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fake = FakeRedis()
        port = await self.fake.start()
        self.store = RedisRunStore(client=RedisClient("127.0.0.1", port), ttl_s=60)

    async def asyncTearDown(self):
        await self.store.close()
        await self.fake.stop()

    async def test_ping(self):
        self.assertTrue(await self.store.ping())

    async def test_save_load_round_trip(self):
        original = sample_checkpoint()
        await self.store.save(original)
        loaded = await self.store.load("r1")

        self.assertEqual(loaded.as_dict(), original.as_dict())
        self.assertEqual(self.fake.expiries["flowforge:run:r1"], 60)

    async def test_missing_key_is_none(self):
        self.assertIsNone(await self.store.load("ghost"))

    async def test_delete_reports_whether_it_removed_anything(self):
        await self.store.save(sample_checkpoint())
        self.assertTrue(await self.store.delete("r1"))
        self.assertFalse(await self.store.delete("r1"))

    async def test_scan_walks_every_page_and_strips_the_prefix(self):
        for i in range(5):
            await self.store.save(sample_checkpoint(f"run{i}"))

        self.assertEqual(
            await self.store.list_ids(), ["run0", "run1", "run2", "run3", "run4"]
        )
        self.assertTrue(any(c[0] == "SCAN" for c in self.fake.commands))
        self.assertFalse(any(c[0] == "KEYS" for c in self.fake.commands))

    async def test_ttl_can_be_disabled(self):
        store = RedisRunStore(client=self.store.client, ttl_s=None)
        await store.save(sample_checkpoint("no_ttl"))
        self.assertNotIn("flowforge:run:no_ttl", self.fake.expiries)

    async def test_server_error_surfaces_as_redis_error(self):
        with self.assertRaises(RedisError):
            await self.store.client.execute("BOGUS", "x")

    async def test_both_stores_satisfy_the_same_interface(self):
        for store in (MemoryRunStore(), self.store):
            with self.subTest(store=type(store).__name__):
                await store.save(sample_checkpoint("shared"))
                loaded = await store.load("shared")
                self.assertEqual(loaded.workflow_id, "wf")
                self.assertIn("shared", await store.list_ids())
                self.assertTrue(await store.delete("shared"))


@unittest.skipUnless(REAL_REDIS, "set FLOWFORGE_TEST_REDIS=host:port to run these")
class RealRedisTests(unittest.IsolatedAsyncioTestCase):
    """The same store, against an actual redis-server.

    Uses its own key prefix and cleans up after itself, so it is safe to point
    at a shared instance.
    """

    async def asyncSetUp(self):
        host, _, port = REAL_REDIS.partition(":")
        self.store = RedisRunStore(
            client=RedisClient(host, int(port or 6379)),
            prefix=f"flowforge:test:{os.getpid()}:",
            ttl_s=60,
        )

    async def asyncTearDown(self):
        for run_id in await self.store.list_ids():
            await self.store.delete(run_id)
        await self.store.close()

    async def test_server_answers_ping(self):
        self.assertTrue(await self.store.ping())

    async def test_checkpoint_survives_a_real_round_trip(self):
        original = sample_checkpoint("real1")
        await self.store.save(original)
        loaded = await self.store.load("real1")

        self.assertEqual(loaded.as_dict(), original.as_dict())

    async def test_ttl_is_applied_by_the_server(self):
        await self.store.save(sample_checkpoint("ttl"))
        ttl = await self.store.client.execute("TTL", f"{self.store.prefix}ttl")
        self.assertGreater(ttl, 0)
        self.assertLessEqual(ttl, 60)

    async def test_scan_finds_every_key_on_a_real_server(self):
        for i in range(25):
            await self.store.save(sample_checkpoint(f"scan{i:02d}"))

        found = await self.store.list_ids()
        self.assertEqual(len(found), 25)
        self.assertEqual(found[0], "scan00")

    async def test_delete_and_miss(self):
        await self.store.save(sample_checkpoint("gone"))
        self.assertTrue(await self.store.delete("gone"))
        self.assertFalse(await self.store.delete("gone"))
        self.assertIsNone(await self.store.load("gone"))

    async def test_real_error_reply_is_raised(self):
        with self.assertRaises(RedisError):
            await self.store.client.execute("BOGUSCOMMAND", "x")

    async def test_pause_and_resume_through_a_real_server(self):
        """The whole point of the store: a run paused here, answered there."""
        from pathlib import Path

        graph = Graph.from_file(
            Path(__file__).resolve().parent.parent / "examples" / "approval.json"
        )
        paused = await WorkflowEngine(graph).run()
        self.assertIs(paused.status, RunStatus.PAUSED)
        await self.store.save(paused.checkpoint)

        # A second process would have nothing but the run id and Redis.
        revived = await self.store.load(paused.run_id)
        final = await WorkflowEngine(graph).resume(revived, {"ask": {"approved": True}})

        self.assertIs(final.status, RunStatus.COMPLETED)
        self.assertEqual(final.branch_of("gate"), "go")
        self.assertEqual(
            final.outputs_of("report")["message"],
            "sha a1b2c3d -> go (deployed: a1b2c3d)",
        )


if __name__ == "__main__":
    unittest.main()
