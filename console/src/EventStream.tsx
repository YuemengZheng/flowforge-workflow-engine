import { useEffect, useRef } from "react";
import type { RunEvent } from "./api";

const TONE: Record<string, string> = {
  "run.started": "#5b9dd9",
  "run.completed": "#4caf7d",
  "run.failed": "#e0574f",
  "run.paused": "#e2b340",
  "run.resumed": "#5b9dd9",
  "node.started": "#9aa3b2",
  "node.completed": "#4caf7d",
  "node.failed": "#e0574f",
  "node.skipped": "#5a6070",
  "node.paused": "#e2b340",
  "node.retry": "#e2b340",
  "node.delta": "#6b7280",
};

/** The raw event stream, exactly as it arrives — seq included, so order is visible. */
export function EventStream({ events }: { events: RunEvent[] }) {
  const tail = useRef<HTMLDivElement>(null);
  useEffect(() => {
    tail.current?.scrollIntoView({ block: "end" });
  }, [events.length]);

  return (
    <div className="stream">
      {events.length === 0 && <p className="muted">No events yet — press Run.</p>}
      {events.map((event) => (
        <div key={`${event.run}-${event.seq}`} className="frame">
          <span className="seq">{String(event.seq).padStart(3, "0")}</span>
          <span className="etype" style={{ color: TONE[event.type] ?? "#e6e8ee" }}>
            {event.type}
          </span>
          {event.node && <span className="enode">{event.node}</span>}
          {typeof event.ms === "number" && <span className="ems">{event.ms.toFixed(1)}ms</span>}
          {event.type === "node.delta" && event.text && (
            <span className="etext">{String(event.text).slice(0, 40)}</span>
          )}
        </div>
      ))}
      <div ref={tail} />
    </div>
  );
}
