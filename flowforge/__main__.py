"""CLI: ``python -m flowforge run examples/diamond.json``."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from typing import Any

from .engine import WorkflowEngine
from .errors import FlowForgeError
from .graph import Graph


def _cmd_run(args: argparse.Namespace) -> int:
    graph = Graph.from_file(args.workflow)
    inputs = json.loads(args.inputs) if args.inputs else {}
    engine = WorkflowEngine(graph, max_concurrency=args.max_concurrency)

    if args.stream:
        return asyncio.run(_stream(engine, inputs, sse=args.sse))

    result = engine.run_sync(inputs)
    print(f"run {result.run_id}  {result.status.value}")
    for node_id, record in result.nodes.items():
        branch = f"  -> {record.branch}" if record.branch else ""
        attempts = f"  x{record.attempts}" if record.attempts > 1 else ""
        print(
            f"  wave {record.wave or '-':>2}  {record.status.value:<9} "
            f"{record.duration_ms:7.1f}ms  {node_id}{branch}{attempts}"
            + (f"  <- {record.error}" if record.error else "")
        )
    print("stats " + json.dumps(result.stats.as_dict()))
    if result.paused:
        for node_id, question in result.awaiting.items():
            print(f"awaiting {node_id}: {question.get('prompt', '')}")
        print(
            "resume with: POST /runs/<run_id>/resume "
            '{"answers": {"<node>": {...}}}'
        )
    else:
        print("outputs " + json.dumps(result.outputs, default=str))
    return 0 if result.ok else 1


async def _stream(engine: WorkflowEngine, inputs: dict, sse: bool) -> int:
    """Consume the event stream, as an SSE endpoint or a client would."""
    failed = False
    async for event in engine.stream(inputs):
        if sse:
            print(event.to_sse(), end="", flush=True)
        else:
            print(event.to_json(), flush=True)
        if event.type == "run.failed":
            failed = True
    return 1 if failed else 0


def _cmd_inspect(args: argparse.Namespace) -> int:
    graph = Graph.from_file(args.workflow)
    print(f"{graph.id}: {len(graph)} nodes, {len(graph.edges)} edges")
    print(f"roots: {', '.join(graph.roots)}")
    print(f"leaves: {', '.join(graph.leaves)}")
    print(f"max parallel width: {graph.max_width}")
    for i, level in enumerate(graph.waves(), 1):
        print(f"  level {i}: {', '.join(level)}")
    return 0


def _mysql_store(dsn: str) -> Any:
    """``[user[:password]@]host[:port][/database]`` -> a SQLRunStore."""
    from .sql import SQLRunStore

    credentials, _, location = dsn.rpartition("@")
    user, _, password = credentials.partition(":")
    endpoint, _, database = location.partition("/")
    host, _, port = endpoint.partition(":")
    return SQLRunStore.mysql(
        host=host or "127.0.0.1",
        port=int(port or 3306),
        user=user or "root",
        password=password,
        database=database or "flowforge",
    )


def _store_from_args(args: argparse.Namespace) -> Any:
    """The one run store the arguments ask for, or ``None`` for in-process.

    An explicit flag beats the environment rather than conflicting with it. That
    matters in a container: ``FLOWFORGE_REDIS`` may be set because the *queue*
    lives there while the checkpoint store is deliberately MySQL, and treating
    the two as a contradiction would make that combination unexpressible.
    """
    from .store import RedisRunStore

    flags = [
        (flag, value)
        for flag, value in (
            ("--redis", args.redis),
            ("--mysql", args.mysql),
            ("--sqlite", args.sqlite),
        )
        if value
    ]
    if len(flags) > 1:
        raise FlowForgeError(
            f"choose one store, not {' and '.join(flag for flag, _ in flags)}"
        )

    if flags:
        chosen, target = flags[0]
    else:
        for variable, flag in (
            ("FLOWFORGE_MYSQL", "--mysql"),
            ("FLOWFORGE_SQLITE", "--sqlite"),
            ("FLOWFORGE_REDIS_STORE", "--redis"),
            ("FLOWFORGE_REDIS", "--redis"),
        ):
            target = os.environ.get(variable, "")
            if target:
                chosen = flag
                break
        else:
            return None

    if chosen == "--redis":
        host, _, port = target.partition(":")
        print(f"paused runs -> redis {host}:{port or 6379}")
        return RedisRunStore(host=host, port=int(port or 6379))
    if chosen == "--mysql":
        print(f"paused runs -> mysql {target.rpartition('@')[2]}")
        return _mysql_store(target)

    from .sql import SQLRunStore

    print(f"paused runs -> sqlite {target}")
    return SQLRunStore.sqlite(target)


def _queue_endpoint(args: argparse.Namespace) -> str:
    """Where the job queue lives — its own setting, not the store's."""
    return (
        args.queue_redis
        or os.environ.get("FLOWFORGE_QUEUE_REDIS")
        or os.environ.get("FLOWFORGE_REDIS")
        or "127.0.0.1"
    )


def _event_sink(args: argparse.Namespace) -> Any:
    """A Kafka sink if one was asked for, else nothing."""
    endpoint = getattr(args, "kafka", None) or os.environ.get("FLOWFORGE_KAFKA")
    if not endpoint:
        return None
    from .kafka import KafkaClient, KafkaEventSink

    topic = getattr(args, "kafka_topic", None) or os.environ.get(
        "FLOWFORGE_KAFKA_TOPIC", "flowforge.events"
    )
    types = os.environ.get("FLOWFORGE_KAFKA_EVENTS", "")
    print(f"run events -> kafka {endpoint} topic {topic}")
    return KafkaEventSink(
        KafkaClient.from_endpoint(endpoint),
        topic=topic,
        types=[t.strip() for t in types.split(",") if t.strip()] or None,
    )


def _cmd_serve(args: argparse.Namespace) -> int:
    from .service import WorkflowService

    store = _store_from_args(args)
    service = WorkflowService.from_directory(
        args.directory,
        max_concurrency=args.max_concurrency,
        store=store,
        event_sink=_event_sink(args),
    )

    from .api import FASTAPI_AVAILABLE

    if args.builtin or not FASTAPI_AVAILABLE:
        # The fallback: same service, hand-rolled HTTP. Chosen explicitly, or
        # because the extra is not installed.
        from .server import WorkflowServer

        why = "--builtin" if args.builtin else "fastapi not installed"
        print(f"serving with the built-in asyncio server ({why})")
        fallback = WorkflowServer(
            service.workflows,
            max_concurrency=args.max_concurrency,
            store=service.store,
            event_sink=service.event_sink,
        )
        try:
            asyncio.run(fallback.serve(args.host, args.port))
        except KeyboardInterrupt:
            print("\nstopped")
        return 0

    from .api import serve as serve_fastapi

    print(f"serving with fastapi/uvicorn on {args.host}:{args.port}")
    try:
        asyncio.run(serve_fastapi(service, args.host, args.port))
    except KeyboardInterrupt:
        print("\nstopped")
    return 0


def _cmd_worker(args: argparse.Namespace) -> int:
    """Consume queued jobs instead of serving HTTP."""
    from .service import WorkflowService
    from .store import RedisClient
    from .worker import Worker, install_signal_handlers

    store = _store_from_args(args)
    service = WorkflowService.from_directory(
        args.directory,
        max_concurrency=args.max_concurrency,
        store=store,
        event_sink=_event_sink(args),
    )
    host, _, port = _queue_endpoint(args).partition(":")
    client = RedisClient(host=host, port=int(port or 6379))

    artifacts = None
    if args.artifacts or os.environ.get("FLOWFORGE_S3_ENDPOINT"):
        from .artifacts import store_from_env

        artifacts = store_from_env(endpoint=args.artifacts or None)
        print(f"artifacts -> {artifacts.client.endpoint}/{artifacts.client.bucket}")

    async def main() -> None:
        if artifacts is not None:
            await artifacts.client.ensure_bucket()
        worker = Worker(service, client, queue=args.queue, artifacts=artifacts)
        stop = asyncio.Event()
        install_signal_handlers(stop)
        try:
            await worker.run_forever(stop)
        finally:
            print(
                f"worker: {worker.completed} completed, {worker.failed} failed",
                flush=True,
            )
            await worker.close()

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nstopped")
    return 0


def _cmd_submit(args: argparse.Namespace) -> int:
    """Push a job onto the worker's queue."""
    from .store import RedisClient
    from .worker import Job, enqueue

    host, _, port = _queue_endpoint(args).partition(":")
    client = RedisClient(host=host, port=int(port or 6379))
    job = Job(args.workflow, json.loads(args.inputs) if args.inputs else {})

    async def main() -> int:
        try:
            depth = await enqueue(client, job, args.queue)
        finally:
            await client.close()
        return depth

    depth = asyncio.run(main())
    print(f"queued {job.workflow} on {args.queue} (depth {depth})")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="flowforge")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="execute a workflow JSON file")
    run.add_argument("workflow")
    run.add_argument("--inputs", help="JSON object passed to the start node")
    run.add_argument("--max-concurrency", type=int, default=None)
    run.add_argument(
        "--stream", action="store_true", help="print events as they happen"
    )
    run.add_argument(
        "--sse", action="store_true", help="with --stream, emit SSE frames"
    )
    run.set_defaults(func=_cmd_run)

    inspect = sub.add_parser("inspect", help="show graph shape without running it")
    inspect.add_argument("workflow")
    inspect.set_defaults(func=_cmd_inspect)

    serve = sub.add_parser("serve", help="serve a workflow directory over HTTP/SSE")
    serve.add_argument("directory", nargs="?", default="examples")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    serve.add_argument("--max-concurrency", type=int, default=None)
    serve.add_argument(
        "--redis",
        metavar="HOST[:PORT]",
        help="store paused runs in Redis instead of in process "
        "(or set FLOWFORGE_REDIS)",
    )
    serve.add_argument(
        "--mysql",
        metavar="[USER[:PASSWORD]@]HOST[:PORT][/DB]",
        help="store paused runs in MySQL, for checkpoints that must outlive a "
        "cache (or set FLOWFORGE_MYSQL). Needs flowforge[mysql]",
    )
    serve.add_argument(
        "--sqlite",
        metavar="PATH",
        help="store paused runs in a sqlite file — the same SQL store with no "
        "server to run (or set FLOWFORGE_SQLITE)",
    )
    serve.add_argument(
        "--builtin",
        action="store_true",
        help="use the dependency-free asyncio server instead of FastAPI",
    )
    serve.add_argument(
        "--kafka",
        metavar="HOST[:PORT]",
        help="also publish run events to a Kafka topic (or set FLOWFORGE_KAFKA)",
    )
    serve.add_argument("--kafka-topic", default=None)
    serve.set_defaults(func=_cmd_serve)

    worker = sub.add_parser("worker", help="run queued workflows off a Redis list")
    worker.add_argument("directory", nargs="?", default="examples")
    worker.add_argument("--queue", default="flowforge:jobs")
    worker.add_argument(
        "--queue-redis", metavar="HOST[:PORT]", help="defaults to FLOWFORGE_REDIS"
    )
    worker.add_argument(
        "--artifacts",
        metavar="URL",
        help="S3/MinIO endpoint for run results (or set FLOWFORGE_S3_ENDPOINT)",
    )
    worker.add_argument("--max-concurrency", type=int, default=None)
    worker.add_argument("--redis", metavar="HOST[:PORT]")
    worker.add_argument("--mysql", metavar="[USER[:PASSWORD]@]HOST[:PORT][/DB]")
    worker.add_argument("--sqlite", metavar="PATH")
    worker.add_argument(
        "--kafka",
        metavar="HOST[:PORT]",
        help="also publish run events to a Kafka topic (or set FLOWFORGE_KAFKA)",
    )
    worker.add_argument("--kafka-topic", default=None)
    worker.set_defaults(func=_cmd_worker)

    submit = sub.add_parser("submit", help="queue a workflow for a worker to run")
    submit.add_argument("workflow")
    submit.add_argument("--inputs", help="JSON object passed to the start node")
    submit.add_argument("--queue", default="flowforge:jobs")
    submit.add_argument(
        "--queue-redis", metavar="HOST[:PORT]", help="defaults to FLOWFORGE_REDIS"
    )
    submit.set_defaults(func=_cmd_submit)

    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except FlowForgeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
