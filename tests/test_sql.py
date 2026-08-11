"""The SQL run store, against sqlite and against a real MySQL.

``StoreContract`` holds every assertion once; the two subclasses only differ in
which store they build. sqlite runs always (standard library, no daemon). MySQL
runs when ``FLOWFORGE_TEST_MYSQL=host:port`` is set and skips otherwise, so the
same contract is what gets verified against the real server rather than a
paraphrase of it:

    docker run -d --rm --name ff-mysql -p 3399:3306 \
      -e MYSQL_ROOT_PASSWORD=flowforge -e MYSQL_DATABASE=flowforge mysql:8.4
    FLOWFORGE_TEST_MYSQL=127.0.0.1:3399 python3 -m unittest discover -s tests -t .
"""

import asyncio
import os
import unittest
from pathlib import Path

from flowforge import (
    Checkpoint,
    Graph,
    MemoryRunStore,
    RunStatus,
    WorkflowEngine,
)
from flowforge.sql import MYSQL, SQLITE, SQLRunStore, SQLStoreError

REAL_MYSQL = os.environ.get("FLOWFORGE_TEST_MYSQL")
EXAMPLES = Path(__file__).resolve().parent.parent / "examples"


def sample_checkpoint(run_id: str = "r1") -> Checkpoint:
    return Checkpoint(
        run_id=run_id,
        workflow_id="wf",
        fingerprint="fp",
        awaiting={"ask": {"prompt": "ok?"}},
        waves=2,
    )


class StoreContract:
    """Assertions every SQLRunStore backend must satisfy identically."""

    # sqlite's in-memory database is per-connection, so it can only ever run
    # one; MySQL exercises the pool properly.
    concurrent_connections = 1

    async def make_store(self, **kwargs) -> SQLRunStore:  # pragma: no cover
        raise NotImplementedError

    async def asyncSetUp(self):
        self.store = await self.make_store(ttl_s=60)

    async def asyncTearDown(self):
        for run_id in await self.store.list_ids():
            await self.store.delete(run_id)
        await self.store.purge_expired()
        await self.store.close()

    async def test_ping(self):
        self.assertTrue(await self.store.ping())

    async def test_save_load_round_trip(self):
        original = sample_checkpoint()
        await self.store.save(original)
        loaded = await self.store.load("r1")

        self.assertEqual(loaded.as_dict(), original.as_dict())

    async def test_missing_row_is_none(self):
        self.assertIsNone(await self.store.load("ghost"))

    async def test_delete_reports_whether_it_removed_anything(self):
        await self.store.save(sample_checkpoint())
        self.assertTrue(await self.store.delete("r1"))
        self.assertFalse(await self.store.delete("r1"))

    async def test_list_ids_is_sorted(self):
        for i in (3, 1, 4, 0, 2):
            await self.store.save(sample_checkpoint(f"run{i}"))

        self.assertEqual(
            await self.store.list_ids(), ["run0", "run1", "run2", "run3", "run4"]
        )

    async def test_saving_twice_updates_the_row_instead_of_failing(self):
        """Resuming rewrites a run: the upsert is the whole reason for Dialect."""
        await self.store.save(sample_checkpoint("same"))
        second = Checkpoint(
            run_id="same",
            workflow_id="wf2",
            fingerprint="fp2",
            awaiting={},
            waves=9,
        )
        await self.store.save(second)

        loaded = await self.store.load("same")
        self.assertEqual(loaded.workflow_id, "wf2")
        self.assertEqual(loaded.waves, 9)
        self.assertEqual(await self.store.list_ids(), ["same"])

    async def test_an_expired_row_is_absent_and_swept(self):
        store = await self.make_store(ttl_s=-1)  # already past its expiry
        try:
            await store.save(sample_checkpoint("stale"))
            # Physically present, logically gone.
            self.assertIsNone(await store.load("stale"))
            self.assertEqual(await store.list_ids(), [])
            # load() swept it on the way out, so there is nothing left to purge.
            self.assertEqual(await store.purge_expired(), 0)
        finally:
            await store.close()

    async def test_purge_expired_removes_rows_nobody_looked_at(self):
        store = await self.make_store(ttl_s=-1)
        try:
            for i in range(3):
                await store.save(sample_checkpoint(f"old{i}"))
            self.assertEqual(await store.purge_expired(), 3)
            self.assertEqual(await store.purge_expired(), 0)
        finally:
            await store.close()

    async def test_ttl_none_keeps_the_row_forever(self):
        store = await self.make_store(ttl_s=None)
        try:
            await store.save(sample_checkpoint("kept"))
            self.assertIsNotNone(await store.load("kept"))
            self.assertEqual(await store.purge_expired(), 0)
            await store.delete("kept")
        finally:
            await store.close()

    async def test_concurrent_saves_all_land(self):
        """A wide wave writes its frontier all at once; the pool must cope."""
        store = await self.make_store(
            ttl_s=60, max_connections=self.concurrent_connections
        )
        try:
            await asyncio.gather(
                *(store.save(sample_checkpoint(f"wave{i:02d}")) for i in range(20))
            )
            self.assertEqual(len(await store.list_ids()), 20)
            for i in range(20):
                await store.delete(f"wave{i:02d}")
        finally:
            await store.close()

    async def test_interface_matches_the_in_memory_store(self):
        for store in (MemoryRunStore(), self.store):
            with self.subTest(store=type(store).__name__):
                await store.save(sample_checkpoint("shared"))
                loaded = await store.load("shared")
                self.assertEqual(loaded.workflow_id, "wf")
                self.assertIn("shared", await store.list_ids())
                self.assertTrue(await store.delete("shared"))

    async def test_pause_and_resume_through_the_database(self):
        """The point of the store: a run paused here, answered somewhere else."""
        graph = Graph.from_file(EXAMPLES / "approval.json")
        paused = await WorkflowEngine(graph).run()
        self.assertIs(paused.status, RunStatus.PAUSED)
        await self.store.save(paused.checkpoint)

        # A second worker has nothing but the run id and the database.
        revived = await self.store.load(paused.run_id)
        final = await WorkflowEngine(graph).resume(revived, {"ask": {"approved": True}})

        self.assertIs(final.status, RunStatus.COMPLETED)
        self.assertEqual(final.branch_of("gate"), "go")
        self.assertEqual(
            final.outputs_of("report")["message"],
            "sha a1b2c3d -> go (deployed: a1b2c3d)",
        )
        await self.store.delete(paused.run_id)

    async def test_crash_recovery_frontier_survives_the_database(self):
        """Per-wave checkpointing works against SQL, not only in memory."""
        graph = Graph.from_file(EXAMPLES / "diamond.json")
        engine = WorkflowEngine(graph, store=self.store, checkpoint_every_wave=True)
        result = await engine.run(run_id="crashy")

        self.assertIs(result.status, RunStatus.COMPLETED)
        # A finished run leaves no frontier behind.
        self.assertIsNone(await self.store.load("crashy"))


class SQLiteStoreTests(StoreContract, unittest.IsolatedAsyncioTestCase):
    async def make_store(self, **kwargs) -> SQLRunStore:
        return SQLRunStore.sqlite(**kwargs)

    async def test_dialect_is_sqlite(self):
        self.assertIs(self.store.dialect, SQLITE)

    async def test_a_bad_table_name_is_refused(self):
        # Interpolated into DDL, so it cannot be parameterised — reject instead.
        with self.assertRaises(ValueError):
            SQLRunStore.sqlite(table="runs; DROP TABLE users")

    async def test_max_connections_must_be_positive(self):
        with self.assertRaises(ValueError):
            SQLRunStore.sqlite(max_connections=0)

    async def test_in_memory_refuses_a_pool_it_cannot_honour(self):
        # Each :memory: connection is a separate database, so silently
        # accepting this would scatter writes across several of them.
        with self.assertRaises(ValueError):
            SQLRunStore.sqlite(max_connections=4)

    async def test_a_closed_store_refuses_work(self):
        store = SQLRunStore.sqlite()
        await store.save(sample_checkpoint("x"))
        await store.close()
        with self.assertRaises(SQLStoreError):
            await store.load("x")

    async def test_a_driver_error_surfaces_as_sql_store_error(self):
        store = SQLRunStore.sqlite(create_table=False)
        try:
            with self.assertRaises(SQLStoreError):
                await store.load("anything")  # no table was ever created
        finally:
            await store.close()

    async def test_a_file_backed_store_outlives_its_connections(self):
        import tempfile

        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "runs.db")
            first = SQLRunStore.sqlite(path, ttl_s=None)
            await first.save(sample_checkpoint("durable"))
            await first.close()

            second = SQLRunStore.sqlite(path, ttl_s=None)
            try:
                loaded = await second.load("durable")
                self.assertEqual(loaded.workflow_id, "wf")
            finally:
                await second.close()


@unittest.skipUnless(REAL_MYSQL, "set FLOWFORGE_TEST_MYSQL=host:port to run these")
class MySQLStoreTests(StoreContract, unittest.IsolatedAsyncioTestCase):
    concurrent_connections = 4

    async def make_store(self, **kwargs) -> SQLRunStore:
        host, _, port = REAL_MYSQL.partition(":")
        return SQLRunStore.mysql(
            host=host,
            port=int(port or 3306),
            user=os.environ.get("FLOWFORGE_TEST_MYSQL_USER", "root"),
            password=os.environ.get("FLOWFORGE_TEST_MYSQL_PASSWORD", "flowforge"),
            database=os.environ.get("FLOWFORGE_TEST_MYSQL_DB", "flowforge"),
            **kwargs,
        )

    async def test_dialect_is_mysql(self):
        self.assertIs(self.store.dialect, MYSQL)

    async def test_payload_column_holds_a_large_checkpoint(self):
        """LONGTEXT rather than TEXT: TEXT would truncate a wide run at 64 KiB."""
        big = Checkpoint(
            run_id="big",
            workflow_id="wf",
            fingerprint="fp",
            awaiting={},
            waves=1,
            nodes={
                f"node{i}": {"status": "completed", "outputs": {"blob": "x" * 200}}
                for i in range(500)
            },
        )
        self.assertGreater(len(big.to_json()), 64 * 1024)

        await self.store.save(big)
        loaded = await self.store.load("big")

        self.assertEqual(len(loaded.nodes), 500)
        self.assertEqual(loaded.as_dict(), big.as_dict())
        await self.store.delete("big")


if __name__ == "__main__":
    unittest.main()
