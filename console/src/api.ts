/**
 * The backend, as the console uses it.
 *
 * SSE is read with `fetch` and a stream reader rather than `EventSource`,
 * because the run endpoints are POSTs — `EventSource` can only issue GETs, and
 * turning a run into a GET would make it a cacheable, retryable side effect.
 */

export type NodeStatus =
  | "pending"
  | "running"
  | "completed"
  | "failed"
  | "skipped"
  | "paused";

export interface WorkflowShape {
  id: string;
  nodes: { id: string; type: string }[];
  edges: { source: string; target: string; branch: string | null }[];
  waves: string[][];
  max_width: number;
}

export interface RunEvent {
  type: string;
  seq: number;
  run: string;
  at: number;
  node?: string;
  status?: string;
  ms?: number;
  text?: string;
  stats?: Record<string, number>;
  awaiting?: Record<string, { prompt?: string }>;
  [key: string]: unknown;
}

export async function listWorkflows(): Promise<string[]> {
  const response = await fetch("/workflows");
  if (!response.ok) throw new Error(`GET /workflows -> ${response.status}`);
  const body = await response.json();
  return body.workflows.map((w: { id: string }) => w.id);
}

export async function fetchShape(id: string): Promise<WorkflowShape> {
  const response = await fetch(`/workflows/${encodeURIComponent(id)}`);
  if (!response.ok) throw new Error(`GET /workflows/${id} -> ${response.status}`);
  return response.json();
}

/**
 * Stream a run, calling `onEvent` as each frame arrives.
 *
 * Frames are buffered across chunk boundaries: TCP splits wherever it likes, and
 * a reader that assumes one chunk is one frame drops events under load. This is
 * the same care the server takes on the way out.
 */
export async function streamRun(
  path: string,
  body: unknown,
  onEvent: (event: RunEvent) => void,
  signal?: AbortSignal,
): Promise<void> {
  const response = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body ?? {}),
    signal,
  });
  if (!response.ok || !response.body) {
    const detail = await response.text().catch(() => "");
    throw new Error(`POST ${path} -> ${response.status} ${detail.slice(0, 200)}`);
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    let cut = buffer.indexOf("\n\n");
    while (cut !== -1) {
      const frame = buffer.slice(0, cut);
      buffer = buffer.slice(cut + 2);
      const line = frame.split("\n").find((l) => l.startsWith("data:"));
      if (line) {
        try {
          onEvent(JSON.parse(line.slice(5).trim()));
        } catch {
          // A frame we cannot parse is one lost event, not a dead stream.
        }
      }
      cut = buffer.indexOf("\n\n");
    }
  }
}

export async function pausedRuns(): Promise<string[]> {
  const response = await fetch("/runs");
  if (!response.ok) return [];
  return (await response.json()).paused ?? [];
}
