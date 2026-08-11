import type { NodeStatus, RunEvent } from "./api";

/** Which node status an event implies. Unlisted event types change nothing. */
const BY_EVENT: Record<string, NodeStatus> = {
  "node.started": "running",
  "node.completed": "completed",
  "node.failed": "failed",
  "node.skipped": "skipped",
  "node.paused": "paused",
};

export const COLOURS: Record<NodeStatus, { border: string; fill: string }> = {
  pending: { border: "#3f4451", fill: "#1b1e24" },
  running: { border: "#e2b340", fill: "#3a2f14" },
  completed: { border: "#4caf7d", fill: "#16301f" },
  failed: { border: "#e0574f", fill: "#341917" },
  skipped: { border: "#5a6070", fill: "#20242c" },
  paused: { border: "#5b9dd9", fill: "#152534" },
};

export function applyEvent(
  statuses: Record<string, NodeStatus>,
  event: RunEvent,
): Record<string, NodeStatus> {
  const next = BY_EVENT[event.type];
  if (!next || !event.node) return statuses;
  return { ...statuses, [event.node]: next };
}

export function tally(statuses: Record<string, NodeStatus>) {
  const counts: Record<string, number> = {};
  for (const status of Object.values(statuses)) {
    counts[status] = (counts[status] ?? 0) + 1;
  }
  return counts;
}
