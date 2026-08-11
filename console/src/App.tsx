import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Dag } from "./Dag";
import { EventStream } from "./EventStream";
import {
  fetchShape,
  listWorkflows,
  pausedRuns,
  streamRun,
  type NodeStatus,
  type RunEvent,
  type WorkflowShape,
} from "./api";
import { applyEvent, tally } from "./statuses";

type Phase = "idle" | "running" | "paused" | "completed" | "failed";

/**
 * `?run=<workflow>` runs it on load, and `&then=resume` answers the pause it hits.
 *
 * Not a test hook: it is how the screenshots in the README are captured (headless
 * Chrome cannot press buttons), and it makes a link that demonstrates itself.
 */
function demoParams() {
  const params = new URLSearchParams(window.location.search);
  return { run: params.get("run") ?? "", then: params.get("then") ?? "" };
}

export default function App() {
  const [workflows, setWorkflows] = useState<string[]>([]);
  const [selected, setSelected] = useState<string>("");
  const [shape, setShape] = useState<WorkflowShape | null>(null);
  const [statuses, setStatuses] = useState<Record<string, NodeStatus>>({});
  const [events, setEvents] = useState<RunEvent[]>([]);
  const [phase, setPhase] = useState<Phase>("idle");
  const [runId, setRunId] = useState<string>("");
  const [stats, setStats] = useState<Record<string, number> | null>(null);
  const [awaiting, setAwaiting] = useState<string>("");
  const [error, setError] = useState<string>("");

  const demo = useMemo(demoParams, []);

  useEffect(() => {
    listWorkflows()
      .then((ids) => {
        setWorkflows(ids);
        setSelected((current) => current || demo.run || ids[0] || "");
      })
      .catch((exc) => setError(String(exc)));
  }, [demo.run]);

  useEffect(() => {
    if (!selected) return;
    fetchShape(selected)
      .then((next) => {
        setShape(next);
        setStatuses({});
        setEvents([]);
        setPhase("idle");
        setRunId("");
        setStats(null);
        setAwaiting("");
      })
      .catch((exc) => setError(String(exc)));
  }, [selected]);

  const consume = useCallback((event: RunEvent) => {
    setEvents((current) => [...current, event]);
    setStatuses((current) => applyEvent(current, event));
    if (event.run) setRunId(event.run);
    if (event.stats) setStats(event.stats);
    if (event.type === "run.paused") {
      setPhase("paused");
      setAwaiting(Object.keys(event.awaiting ?? {}).join(", "));
    }
    if (event.type === "run.completed") setPhase("completed");
    if (event.type === "run.failed") setPhase("failed");
  }, []);

  const run = useCallback(async () => {
    if (!selected) return;
    setStatuses({});
    setEvents([]);
    setStats(null);
    setAwaiting("");
    setError("");
    setPhase("running");
    try {
      await streamRun(`/runs/${encodeURIComponent(selected)}/stream`, {}, consume);
    } catch (exc) {
      setError(String(exc));
      setPhase("failed");
    }
  }, [selected, consume]);

  /**
   * Resume the paused run. The point of the demo: the nodes that already
   * completed do not appear again as `node.started`, because the engine restored
   * them from the checkpoint instead of re-executing them.
   */
  const resume = useCallback(async () => {
    const target = runId || (await pausedRuns())[0];
    if (!target) {
      setError("no paused run to resume");
      return;
    }
    const before = new Set(
      Object.entries(statuses)
        .filter(([, status]) => status === "completed")
        .map(([id]) => id),
    );
    setPhase("running");
    setError("");
    const rerun: string[] = [];
    try {
      await streamRun(
        `/runs/${encodeURIComponent(target)}/resume-stream`,
        { answers: Object.fromEntries((awaiting || "ask").split(", ").map((n) => [n, { approved: true }])) },
        (event) => {
          if (event.type === "node.started" && event.node && before.has(event.node)) {
            rerun.push(event.node);
          }
          consume(event);
        },
      );
      if (rerun.length) setError(`re-executed completed nodes: ${rerun.join(", ")}`);
    } catch (exc) {
      setError(String(exc));
      setPhase("failed");
    }
  }, [runId, statuses, awaiting, consume]);

  const started = useRef(false);
  useEffect(() => {
    if (!demo.run || started.current || !shape || shape.id !== demo.run) return;
    started.current = true;
    void run();
  }, [demo.run, shape, run]);

  // A second phase for `&then=resume`, so a capture can reach the state *after*
  // a resume without anyone clicking.
  const resumed = useRef(false);
  useEffect(() => {
    if (demo.then !== "resume" || phase !== "paused" || resumed.current) return;
    resumed.current = true;
    void resume();
  }, [demo.then, phase, resume]);

  const counts = useMemo(() => tally(statuses), [statuses]);
  const total = shape?.nodes.length ?? 0;

  return (
    <div className="app">
      <header>
        <div>
          <h1>FlowForge</h1>
          <p className="muted">Distributed workflow orchestration engine</p>
        </div>
        <div className="controls">
          <select value={selected} onChange={(e) => setSelected(e.target.value)}>
            {workflows.map((id) => (
              <option key={id} value={id}>
                {id}
              </option>
            ))}
          </select>
          <button onClick={run} disabled={phase === "running"}>
            Run
          </button>
          <button onClick={resume} disabled={phase !== "paused"}>
            Resume
          </button>
        </div>
      </header>

      {error && <div className="error">{error}</div>}

      <main>
        <section className="graph">
          {shape ? <Dag shape={shape} statuses={statuses} /> : <p className="muted">Loading…</p>}
        </section>
        <section className="events">
          <h2>Event stream <span className="muted">SSE, in emit order</span></h2>
          <EventStream events={events} />
        </section>
      </main>

      <footer>
        <span className={`pill ${phase}`}>{phase}</span>
        {runId && <span>run <code>{runId}</code></span>}
        <span>
          {counts.completed ?? 0}/{total} completed
        </span>
        {(counts.skipped ?? 0) > 0 && <span>{counts.skipped} skipped</span>}
        {awaiting && phase === "paused" && <span>awaiting <code>{awaiting}</code></span>}
        {stats && (
          <>
            <span>{stats.total_ms?.toFixed(1)}ms wall</span>
            <span>peak concurrency {stats.peak_concurrency}</span>
            <span>{stats.waves} waves</span>
          </>
        )}
      </footer>
    </div>
  );
}
