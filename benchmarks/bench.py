"""Benchmarks for the scheduler.

Seven experiments, each repeated and reported as median plus p95 so the numbers
have a stated methodology rather than a single lucky run:

  1. concurrent vs serial  — the same graph with the ready set dispatched as a
     batch, versus one node at a time. The honest ceiling on the speedup is the
     graph's own width, which is why the width is reported alongside.
  2. wide fan-out          — N independent nodes in one wave, N = 100/300/500,
     measuring the peak concurrency actually observed.
  3. scheduler overhead    — no-op nodes, so wall time minus time inside
     ``gather`` is pure coordination cost.
  4. iteration             — a batch of sub-workflow runs dispatched at once
     versus one at a time. Both sides are this engine; ``max_concurrency=1``
     reproduces the sequential loop shape, it does not time another project.
  5. connection pooling    — a burst of Redis commands through the pool versus
     through one connection, which is the design the pool replaced. Needs a
     real server: the thing being timed is a round trip, so an in-process fake
     would measure the wrong quantity. Skipped, not faked, when none is up.
  6. variable resolution   — a compiled config plan rendered per attempt, versus
     parsing the config on every resolve.
  7. event publishing      — what turning on the Kafka sink costs a run. Needs a
     broker; measures the batched sink against no sink at all, and against the
     synchronous mode, which is what the batching replaced.

    python3 benchmarks/bench.py            # full run, writes results.json
    python3 benchmarks/bench.py --repeats 5
    FLOWFORGE_BENCH_REDIS=127.0.0.1:6399 \
      FLOWFORGE_BENCH_KAFKA=127.0.0.1:9492 python3 benchmarks/bench.py
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import statistics
import sys
from pathlib import Path
from time import perf_counter
from typing import Any, Callable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from flowforge import (  # noqa: E402
    Edge,
    Graph,
    NodeSpec,
    RedisClient,
    RunStatus,
    WorkflowEngine,
)

RESULTS = Path(__file__).resolve().parent / "results.json"


# ----------------------------------------------------------------- fixtures


def parallel_branches(branches: int, depth: int, delay: float) -> Graph:
    """`branches` independent chains of `depth` nodes each, joined at the end."""
    nodes = [NodeSpec("start", "start"), NodeSpec("join", "stub")]
    edges = []
    for b in range(branches):
        previous = "start"
        for d in range(depth):
            node_id = f"b{b}_{d}"
            nodes.append(NodeSpec(node_id, "stub", {"delay": delay}))
            edges.append(Edge(previous, node_id))
            previous = node_id
        edges.append(Edge(previous, "join"))
    return Graph(nodes, edges, graph_id=f"chains_{branches}x{depth}")


def wide_fanout(width: int, delay: float) -> Graph:
    """One root, `width` independent workers, one join — a single wide wave."""
    nodes = [NodeSpec("root", "stub"), NodeSpec("join", "stub")]
    edges = []
    for i in range(width):
        nodes.append(NodeSpec(f"w{i}", "stub", {"delay": delay}))
        edges += [Edge("root", f"w{i}"), Edge(f"w{i}", "join")]
    return Graph(nodes, edges, graph_id=f"wide_{width}")


def noop_chain_and_fan(total: int) -> Graph:
    """`total` zero-work nodes, half sequential and half parallel."""
    half = total // 2
    nodes = [NodeSpec("root", "stub")]
    edges = []
    previous = "root"
    for i in range(half):
        nodes.append(NodeSpec(f"c{i}", "stub"))
        edges.append(Edge(previous, f"c{i}"))
        previous = f"c{i}"
    for i in range(total - half):
        nodes.append(NodeSpec(f"p{i}", "stub"))
        edges.append(Edge(previous, f"p{i}"))
    return Graph(nodes, edges, graph_id=f"noop_{total}")


# ------------------------------------------------------------------ harness


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(round(fraction * (len(ordered) - 1))))
    return ordered[index]


def summarize(values: list[float]) -> dict[str, float]:
    return {
        "median": round(statistics.median(values), 3),
        "p95": round(percentile(values, 0.95), 3),
        "min": round(min(values), 3),
        "max": round(max(values), 3),
    }


async def measure(
    engine_factory: Callable[[], WorkflowEngine], repeats: int, warmup: int = 2
) -> dict[str, Any]:
    for _ in range(warmup):
        await engine_factory().run()

    wall: list[float] = []
    scheduler: list[float] = []
    peak = 0
    nodes = 0
    for _ in range(repeats):
        result = await engine_factory().run()
        if result.status is not RunStatus.COMPLETED:
            raise RuntimeError(f"benchmark run failed: {result.failures}")
        wall.append(result.stats.total_ms)
        scheduler.append(result.stats.scheduler_ms)
        peak = max(peak, result.stats.peak_concurrency)
        nodes = result.stats.nodes_executed
    return {
        "repeats": repeats,
        "nodes": nodes,
        "peak_concurrency": peak,
        "wall_ms": summarize(wall),
        "scheduler_ms": summarize(scheduler),
    }


# -------------------------------------------------------------- experiments


async def experiment_concurrency(repeats: int) -> dict[str, Any]:
    branches, depth, delay = 5, 10, 0.005
    graph = parallel_branches(branches, depth, delay)

    batched = await measure(lambda: WorkflowEngine(graph), repeats)
    serial = await measure(lambda: WorkflowEngine(graph, max_concurrency=1), repeats)

    serial_ms = serial["wall_ms"]["median"]
    batched_ms = batched["wall_ms"]["median"]
    return {
        "graph": {
            "nodes": len(graph),
            "width": graph.max_width,
            "depth": depth,
            "node_delay_ms": delay * 1000,
        },
        "serial": serial,
        "batched": batched,
        "latency_reduction_pct": round(100 * (serial_ms - batched_ms) / serial_ms, 1),
        "speedup": round(serial_ms / batched_ms, 2),
    }


async def experiment_width(repeats: int) -> dict[str, Any]:
    delay = 0.01
    results = {}
    for width in (100, 300, 500):
        graph = wide_fanout(width, delay)
        results[str(width)] = await measure(lambda g=graph: WorkflowEngine(g), repeats)
    return {"node_delay_ms": delay * 1000, "by_width": results}


async def experiment_overhead(repeats: int) -> dict[str, Any]:
    results = {}
    for total in (50, 500):
        graph = noop_chain_and_fan(total)
        measured = await measure(lambda g=graph: WorkflowEngine(g), repeats)
        per_node = measured["scheduler_ms"]["median"] / measured["nodes"]
        measured["scheduler_us_per_node"] = round(per_node * 1000, 2)
        results[str(total)] = measured
    return {"by_size": results}


def iteration_graph(items: int, delay: float, max_concurrency: int) -> Graph:
    """One iterate node fanning a sub-workflow across ``items``."""
    return Graph.from_dict(
        {
            "id": f"iterate_{items}_{max_concurrency}",
            "nodes": [
                {
                    "id": "loop",
                    "type": "iterate",
                    "config": {
                        "items": list(range(items)),
                        "max_concurrency": max_concurrency,
                        "workflow": {
                            "nodes": [
                                {
                                    "id": "work",
                                    "type": "stub",
                                    "config": {
                                        "delay": delay,
                                        "output": {"v": "{{inputs.item}}"},
                                    },
                                }
                            ]
                        },
                    },
                }
            ],
        }
    )


async def experiment_iteration(repeats: int) -> dict[str, Any]:
    """Concurrent iteration vs the sequential loop shape.

    ``max_concurrency=1`` reproduces a sequential ``for item in items: await``,
    which is how the reference implementation walks a batch; the unbounded run
    dispatches the whole batch at once.
    """
    items, delay = 20, 0.01
    concurrent = await measure(
        lambda: WorkflowEngine(iteration_graph(items, delay, 0)), repeats
    )
    sequential = await measure(
        lambda: WorkflowEngine(iteration_graph(items, delay, 1)), repeats
    )
    saved = 100 * (1 - concurrent["wall_ms"]["median"] / sequential["wall_ms"]["median"])
    return {
        "items": items,
        "node_delay_ms": delay * 1000,
        "sequential": sequential,
        "concurrent": concurrent,
        "latency_reduction_pct": round(saved, 1),
        "speedup": round(
            sequential["wall_ms"]["median"] / concurrent["wall_ms"]["median"], 2
        ),
    }


def prompt_config() -> dict[str, Any]:
    """A node config shaped like a real LLM call, not a microbenchmark toy.

    Mixed on purpose: static keys that dominate a real config, one whole-value
    reference, one long interpolated prompt, a nested dict and a list.
    """
    return {
        "provider": "anthropic",
        "model": "claude-opus-5",
        "max_tokens": 1024,
        "temperature": 0.2,
        "stream": True,
        "options": {"top_p": 0.95, "stop": ["\n\n", "END"], "retry_hint": "none"},
        "system": "You are a careful reviewer. Be concise.",
        "history": "{{plan.messages}}",
        "prompt": (
            "Question: {{inputs.question}}\n"
            "Context from {{fetch.source}} (fetched {{fetch.at}}):\n"
            "{{fetch.text}}\n"
            "Earlier answer, if any: {{previous.answer ?? \"none\"}}\n"
            "Reviewer notes: {{review.notes ?? \"none yet\"}}\n"
            "Answer in {{inputs.language}} for {{inputs.audience}}."
        ),
        "metadata": {
            "run_label": "{{inputs.label}}",
            "attempt_of": "{{inputs.question}}",
            "tags": ["review", "{{inputs.language}}", "batch"],
        },
    }


def _strip_templates(value: Any) -> Any:
    """The same structure with every reference replaced by a plain string."""
    if isinstance(value, str):
        return "plain text" if "{{" in value else value
    if isinstance(value, dict):
        return {key: _strip_templates(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_strip_templates(item) for item in value]
    return value


def prompt_pool() -> Any:
    from flowforge import VariablePool

    pool = VariablePool(
        {
            "question": "Does the scheduler handle diamonds?",
            "language": "English",
            "audience": "a hiring manager",
            "label": "bench-1",
        }
    )
    pool.set_outputs("plan", {"messages": [{"role": "user", "content": "hi"}]})
    pool.set_outputs(
        "fetch",
        {
            "source": "README.md",
            "at": "2026-08-10T12:00:00Z",
            "text": "Kahn's algorithm keeps a counter per node. " * 8,
        },
    )
    pool.set_outputs("previous", {})
    pool.set_outputs("review", {})
    return pool


def experiment_resolution(repeats: int) -> dict[str, Any]:
    """Compiled template rendering versus parsing on every resolve.

    Both arms produce the same value from the same config and pool — the
    baseline is ``resolve_uncached``, which is the implementation this replaced,
    kept in the module for exactly this comparison. What the compiler removes is
    per-resolve work that never depended on the pool: the regex scan, the
    ``a.b.c`` splitting, the ``??`` literal parse, and the rebuilding of subtrees
    that contain no references at all.

    Resolves per iteration is deliberately large: one node attempt is a single
    resolve, so timing one would measure the clock. This measures throughput.
    """
    from flowforge.variables import clear_compile_cache, compile_template

    config = prompt_config()
    pool = prompt_pool()
    per_iteration = 2_000

    # Correctness gate: a speedup on a different answer is not a speedup.
    plan = compile_template(config)
    if plan.render(pool) != pool.resolve_uncached(config):
        raise RuntimeError("compiled and uncached resolution disagree")

    def timed(work: Callable[[], Any]) -> list[float]:
        for _ in range(2):
            for _ in range(per_iteration):
                work()
        samples = []
        for _ in range(repeats):
            started = perf_counter()
            for _ in range(per_iteration):
                work()
            samples.append((perf_counter() - started) * 1000)
        return samples

    # The compiled arm pays compilation once, as the engine does at construction.
    compiled = timed(lambda: plan.render(pool))
    # The baseline re-parses every time, so its cache must not help it.
    clear_compile_cache()
    baseline = timed(lambda: pool.resolve_uncached(config))

    # Second arm, reported separately rather than folded into the headline: the
    # same config with its templates removed. Many real nodes look like this
    # (start, end, a fixed tool call), and it is where the static short-circuit
    # does the most — it renders as the original object instead of a rebuilt
    # copy. Quoting this as "the" resolution speedup would be picking the
    # flattering case, which is why it has its own row.
    static_config = _strip_templates(config)
    static_plan = compile_template(static_config)
    static_compiled = timed(lambda: static_plan.render(pool))
    clear_compile_cache()
    static_baseline = timed(lambda: pool.resolve_uncached(static_config))

    baseline_ms = statistics.median(baseline)
    compiled_ms = statistics.median(compiled)
    static_baseline_ms = statistics.median(static_baseline)
    static_compiled_ms = statistics.median(static_compiled)
    return {
        "template_free_config": {
            "uncached": {
                "wall_ms": summarize(static_baseline),
                "us_per_resolve": round(
                    1000 * static_baseline_ms / per_iteration, 3
                ),
            },
            "compiled": {
                "wall_ms": summarize(static_compiled),
                "us_per_resolve": round(
                    1000 * static_compiled_ms / per_iteration, 3
                ),
            },
            "speedup": round(static_baseline_ms / static_compiled_ms, 2),
        },
        "resolves_per_iteration": per_iteration,
        "repeats": repeats,
        "config_keys": len(config),
        "references": len(list(pool.references(config))),
        "uncached": {
            "wall_ms": summarize(baseline),
            "us_per_resolve": round(1000 * baseline_ms / per_iteration, 3),
        },
        "compiled": {
            "wall_ms": summarize(compiled),
            "us_per_resolve": round(1000 * compiled_ms / per_iteration, 3),
        },
        "speedup": round(baseline_ms / compiled_ms, 2),
        "latency_reduction_pct": round(
            100 * (baseline_ms - compiled_ms) / baseline_ms, 1
        ),
    }


async def experiment_publishing(repeats: int, endpoint: str | None) -> dict[str, Any]:
    """What publishing every run event costs the run itself.

    Telemetry belongs off the critical path, and this is the number that says so:
    ``required=True`` publishes synchronously — one produce, one round trip, per
    event — which is what the default batched sink replaced.
    """
    if endpoint is None:
        return {"skipped": "no reachable kafka — set FLOWFORGE_BENCH_KAFKA=host:port"}
    from flowforge.kafka import KafkaClient, KafkaEventSink

    graph = wide_fanout(100, 0.0)  # no simulated I/O: engine + publish cost only
    topic = f"flowforge.bench.{int(perf_counter() * 1000) % 100000}"

    async def timed(sink: Any) -> tuple[list[float], int]:
        engine = WorkflowEngine(graph, event_sink=sink)
        for _ in range(2):
            await engine.run()
        if sink is not None:
            await sink.flush()
        samples = []
        for _ in range(repeats):
            started = perf_counter()
            await engine.run()
            samples.append((perf_counter() - started) * 1000)
        events = 0
        if sink is not None:
            await sink.flush()
            events = sink.stats()["published"]
        return samples, events

    baseline, _ = await timed(None)

    batched = KafkaEventSink(KafkaClient.from_endpoint(endpoint), topic=topic)
    try:
        batched_samples, batched_events = await timed(batched)
        batched_stats = batched.stats()
    finally:
        await batched.close()

    synchronous = KafkaEventSink(
        KafkaClient.from_endpoint(endpoint), topic=topic, required=True
    )
    try:
        sync_samples, _ = await timed(synchronous)
    finally:
        await synchronous.close()

    base_ms = statistics.median(baseline)
    batched_ms = statistics.median(batched_samples)
    sync_ms = statistics.median(sync_samples)
    return {
        "endpoint": endpoint,
        "nodes": len(graph),
        "events_per_run": batched_events // max(1, repeats + 2),
        "no_sink": {"wall_ms": summarize(baseline)},
        "batched": {
            "wall_ms": summarize(batched_samples),
            "overhead_x": round(batched_ms / base_ms, 2),
            "batches": batched_stats["batches"],
        },
        "synchronous": {
            "wall_ms": summarize(sync_samples),
            "overhead_x": round(sync_ms / base_ms, 2),
        },
        "batching_speedup": round(sync_ms / batched_ms, 2),
    }


async def find_kafka() -> str | None:
    from flowforge.kafka import KafkaClient

    configured = os.environ.get("FLOWFORGE_BENCH_KAFKA")
    for endpoint in ([configured] if configured else []) + ["127.0.0.1:9492"]:
        client = KafkaClient.from_endpoint(endpoint, timeout_s=3)
        try:
            await client.connect()
            return endpoint
        except Exception:
            continue
        finally:
            await client.close()
    return None


async def find_redis() -> str | None:
    """First reachable endpoint, or ``None`` so the experiment can skip itself."""
    configured = os.environ.get("FLOWFORGE_BENCH_REDIS")
    candidates = ([configured] if configured else []) + [
        "127.0.0.1:6399",
        "127.0.0.1:6379",
    ]
    for endpoint in candidates:
        host, _, port = endpoint.partition(":")
        client = RedisClient(host=host, port=int(port or 6379), max_connections=1)
        try:
            if await client.execute("PING"):
                return endpoint
        except Exception:
            continue
        finally:
            await client.close()
    return None


async def experiment_pool(repeats: int, endpoint: str | None) -> dict[str, Any]:
    """Pooled versus one shared connection, on a burst of concurrent commands.

    The single-connection arm is not a straw man — it is the design this pool
    replaced. Redis serves one command at a time per connection, so a shared
    connection behind a lock serialises the whole burst no matter how much
    concurrency the scheduler arranged above it. The pooled arm uses the
    shipped default (``max_connections=10``), not one connection per command,
    because that is what a caller actually gets.
    """
    if endpoint is None:
        return {
            "skipped": "no reachable redis — set FLOWFORGE_BENCH_REDIS=host:port",
        }
    host, _, port = endpoint.partition(":")
    commands = 200
    keys = [f"flowforge:bench:{i}" for i in range(commands)]

    async def server_version() -> str:
        client = RedisClient(host=host, port=int(port or 6379), max_connections=1)
        try:
            info = await client.execute("INFO", "server")
            text = info.decode() if isinstance(info, bytes) else str(info)
            for line in text.splitlines():
                if line.startswith("redis_version:"):
                    return line.split(":", 1)[1].strip()
            return "unknown"
        finally:
            await client.close()

    async def timed(max_connections: int) -> dict[str, Any]:
        client = RedisClient(
            host=host, port=int(port or 6379), max_connections=max_connections
        )

        async def burst() -> None:
            await asyncio.gather(
                *(client.execute("SET", key, "1") for key in keys)
            )

        try:
            for _ in range(2):
                await burst()
            samples = []
            for _ in range(repeats):
                started = perf_counter()
                await burst()
                samples.append((perf_counter() - started) * 1000)
            return {
                "max_connections": max_connections,
                "repeats": repeats,
                "wall_ms": summarize(samples),
                "pool": client.stats,
            }
        finally:
            # Only the keys this wrote — the endpoint may not be a scratch server.
            await client.execute("DEL", *keys)
            await client.close()

    pooled = await timed(10)
    single = await timed(1)
    single_ms = single["wall_ms"]["median"]
    pooled_ms = pooled["wall_ms"]["median"]
    return {
        "endpoint": endpoint,
        "redis_version": await server_version(),
        "commands": commands,
        "single": single,
        "pooled": pooled,
        "latency_reduction_pct": round(100 * (single_ms - pooled_ms) / single_ms, 1),
        "speedup": round(single_ms / pooled_ms, 2),
    }


async def main(repeats: int) -> int:
    environment = {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cpu_count": os.cpu_count(),
    }
    report = {
        "environment": environment,
        "methodology": (
            "each configuration runs 2 warmup iterations then `repeats` measured "
            "iterations; wall time and scheduler overhead are taken from "
            "RunResult.stats and reported as median and p95. Node work is "
            "simulated I/O (asyncio.sleep), not CPU."
        ),
        "concurrency": await experiment_concurrency(repeats),
        "width": await experiment_width(repeats),
        "overhead": await experiment_overhead(repeats),
        "iteration": await experiment_iteration(repeats),
        "pool": await experiment_pool(repeats, await find_redis()),
        "resolution": experiment_resolution(repeats),
        "publishing": await experiment_publishing(repeats, await find_kafka()),
    }
    RESULTS.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    print(f"{environment['platform']}  python {environment['python']}  "
          f"{environment['cpu_count']} cpus  repeats={repeats}\n")

    c = report["concurrency"]
    print("1. concurrent vs serial")
    print(f"   graph: {c['graph']['nodes']} nodes, width {c['graph']['width']}, "
          f"depth {c['graph']['depth']}, {c['graph']['node_delay_ms']}ms per node")
    print(f"   serial  (max_concurrency=1): {c['serial']['wall_ms']['median']:8.1f} ms "
          f"(p95 {c['serial']['wall_ms']['p95']:.1f})")
    print(f"   batched (ready set gathered): {c['batched']['wall_ms']['median']:8.1f} ms "
          f"(p95 {c['batched']['wall_ms']['p95']:.1f})")
    print(f"   -> {c['latency_reduction_pct']}% lower latency ({c['speedup']}x), "
          f"peak concurrency {c['batched']['peak_concurrency']}\n")

    print("2. wide fan-out")
    for width, data in report["width"]["by_width"].items():
        print(f"   {width:>4} independent nodes: {data['wall_ms']['median']:7.1f} ms "
              f"(p95 {data['wall_ms']['p95']:.1f}), peak concurrency "
              f"{data['peak_concurrency']}")
    print()

    print("3. scheduler overhead (no-op nodes)")
    for total, data in report["overhead"]["by_size"].items():
        print(f"   {total:>4} nodes: {data['scheduler_ms']['median']:6.3f} ms total "
              f"-> {data['scheduler_us_per_node']} us per node")
    it = report["iteration"]
    print("4. iteration: concurrent vs sequential")
    print(f"   {it['items']} items x {it['node_delay_ms']}ms sub-workflow")
    print(f"   sequential (max_concurrency=1): {it['sequential']['wall_ms']['median']:8.1f} ms "
          f"(p95 {it['sequential']['wall_ms']['p95']:.1f})")
    print(f"   concurrent (whole batch):       {it['concurrent']['wall_ms']['median']:8.1f} ms "
          f"(p95 {it['concurrent']['wall_ms']['p95']:.1f})")
    print(f"   -> {it['latency_reduction_pct']}% lower latency ({it['speedup']}x)\n")

    p = report["pool"]
    print("5. connection pooling (real redis)")
    if "skipped" in p:
        print(f"   skipped: {p['skipped']}\n")
    else:
        print(f"   {p['commands']} concurrent commands -> {p['endpoint']} "
              f"(redis {p['redis_version']})")
        print(f"   one connection:  {p['single']['wall_ms']['median']:8.1f} ms "
              f"(p95 {p['single']['wall_ms']['p95']:.1f})")
        print(f"   pooled (max {p['pooled']['max_connections']}): "
              f"{p['pooled']['wall_ms']['median']:8.1f} ms "
              f"(p95 {p['pooled']['wall_ms']['p95']:.1f})")
        print(f"   -> {p['latency_reduction_pct']}% lower latency ({p['speedup']}x), "
              f"connections created {p['pooled']['pool']['created']}, "
              f"reused {p['pooled']['pool']['reused']}\n")

    r = report["resolution"]
    print("6. variable resolution (compiled vs parse-every-time)")
    print(f"   config: {r['config_keys']} keys, {r['references']} references, "
          f"{r['resolves_per_iteration']} resolves per iteration")
    print(f"   uncached: {r['uncached']['wall_ms']['median']:8.2f} ms "
          f"({r['uncached']['us_per_resolve']} us per resolve)")
    print(f"   compiled: {r['compiled']['wall_ms']['median']:8.2f} ms "
          f"({r['compiled']['us_per_resolve']} us per resolve)")
    print(f"   -> {r['latency_reduction_pct']}% less time ({r['speedup']}x)")
    tf = r["template_free_config"]
    print(f"   same config with no templates in it: "
          f"{tf['uncached']['us_per_resolve']} -> "
          f"{tf['compiled']['us_per_resolve']} us per resolve "
          f"({tf['speedup']}x, static short-circuit)\n")

    pub = report["publishing"]
    print("7. event publishing (real kafka)")
    if "skipped" in pub:
        print(f"   skipped: {pub['skipped']}\n")
    else:
        print(f"   {pub['nodes']}-node run, ~{pub['events_per_run']} events per run")
        print(f"   no sink              : {pub['no_sink']['wall_ms']['median']:8.2f} ms")
        print(f"   batched sink         : {pub['batched']['wall_ms']['median']:8.2f} ms "
              f"({pub['batched']['overhead_x']}x, {pub['batched']['batches']} batches)")
        print(f"   synchronous (required): {pub['synchronous']['wall_ms']['median']:7.2f} ms "
              f"({pub['synchronous']['overhead_x']}x)")
        print(f"   -> batching is {pub['batching_speedup']}x faster than one produce "
              f"per event\n")

    print(f"wrote {RESULTS}")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=15)
    args = parser.parse_args()
    raise SystemExit(asyncio.run(main(args.repeats)))
