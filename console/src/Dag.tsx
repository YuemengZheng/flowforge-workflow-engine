import { useMemo } from "react";
import ReactFlow, { Background, Controls, MarkerType, Position } from "reactflow";
import type { Edge, Node } from "reactflow";
import "reactflow/dist/style.css";
import type { NodeStatus, WorkflowShape } from "./api";
import { COLOURS } from "./statuses";

const COLUMN = 190;
const ROW = 84;

/**
 * The DAG, laid out from the server's wave list.
 *
 * The backend already computes the wave layout — the same one the scheduler
 * dispatches by — so the console does not reimplement a topological sort. Wave
 * index is the x axis, which means the picture reads left to right in the order
 * things actually run.
 */
export function Dag({
  shape,
  statuses,
}: {
  shape: WorkflowShape;
  statuses: Record<string, NodeStatus>;
}) {
  const nodes: Node[] = useMemo(() => {
    const types = new Map(shape.nodes.map((n) => [n.id, n.type]));
    return shape.waves.flatMap((wave, column) =>
      wave.map((id, row) => {
        const status = statuses[id] ?? "pending";
        const colour = COLOURS[status];
        return {
          id,
          position: {
            x: column * COLUMN,
            y: (row - (wave.length - 1) / 2) * ROW,
          },
          data: { label: `${id}\n${types.get(id) ?? ""}` },
          style: {
            background: colour.fill,
            border: `2px solid ${colour.border}`,
            borderRadius: 8,
            color: "#e6e8ee",
            fontSize: 12,
            width: 150,
            padding: 8,
            whiteSpace: "pre-line" as const,
            textAlign: "center" as const,
          },
          sourcePosition: Position.Right,
          targetPosition: Position.Left,
        };
      }),
    );
  }, [shape, statuses]);

  const edges: Edge[] = useMemo(
    () =>
      shape.edges.map((edge, index) => {
        // A branch edge is only "taken" if its target actually ran; that is what
        // makes skip propagation visible rather than something you take on faith.
        const targetStatus = statuses[edge.target] ?? "pending";
        const dead = targetStatus === "skipped";
        return {
          id: `e${index}`,
          source: edge.source,
          target: edge.target,
          label: edge.branch ?? undefined,
          animated: targetStatus === "running",
          style: {
            stroke: dead ? "#3a3f4b" : "#6b7280",
            strokeWidth: 1.5,
            strokeDasharray: dead ? "4 4" : undefined,
          },
          labelStyle: { fill: "#9aa3b2", fontSize: 10 },
          labelBgStyle: { fill: "#12141a" },
          markerEnd: { type: MarkerType.ArrowClosed, color: dead ? "#3a3f4b" : "#6b7280" },
        };
      }),
    [shape, statuses],
  );

  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      fitView
      proOptions={{ hideAttribution: true }}
      nodesDraggable={false}
      nodesConnectable={false}
      elementsSelectable={false}
    >
      <Background color="#242833" gap={18} />
      <Controls showInteractive={false} />
    </ReactFlow>
  );
}
