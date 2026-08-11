"""FlowForge — a small DAG workflow engine with a Kahn-driven scheduler."""

from .artifacts import ArtifactError, ArtifactStore, S3Client, StoredArtifact
from .checkpoint import Checkpoint, CheckpointError, graph_fingerprint
from .engine import NodeRun, RunResult, RunStats, RunStatus, WorkflowEngine
from .errors import (
    CycleError,
    FlowForgeError,
    GraphError,
    NodeExecutionError,
    NodePaused,
    UnknownNodeTypeError,
)
from .events import Event, EventStream, sse_body
from .graph import Edge, Graph, NodeSpec
from .iteration import IterateNode, IterationError
from .kafka import (
    KafkaClient,
    KafkaError,
    KafkaEventSink,
    KafkaProtocolError,
    Record,
    crc32c,
)
from .llm import (
    AnthropicProvider,
    EchoProvider,
    LLMError,
    LLMNode,
    build_provider,
    known_providers,
    register_provider,
)
from .mcp import (
    MCPClient,
    MCPError,
    MCPGateway,
    MCPToolError,
    ToolSpec,
    get_gateway,
    register_gateway,
)
from .nodes import (
    ConditionError,
    Node,
    NodeContext,
    NodeRegistry,
    NodeStatus,
    evaluate_condition,
    registry,
)
from .pool import ConnectionPool, HTTPConnectionPool, PoolError, PooledConnection
from .providers import (
    OPENAI_PROFILES,
    BedrockProvider,
    OpenAICompatibleProvider,
    Profile,
    VertexProvider,
    close_shared_pool,
    sigv4_headers,
)
from .retry import ErrorStrategy, RetryPolicy
from .service import ServiceError, WorkflowService
from .sql import MYSQL, SQLITE, Dialect, SQLRunStore, SQLStoreError
from .store import (
    MemoryRunStore,
    RedisClient,
    RedisError,
    RedisRunStore,
    RunStore,
)
from .variables import VariableError, VariablePool
from .worker import Job, Worker, enqueue

__version__ = "0.5.0"

__all__ = [
    "AnthropicProvider",
    "ArtifactError",
    "ArtifactStore",
    "BedrockProvider",
    "Checkpoint",
    "CheckpointError",
    "ConditionError",
    "ConnectionPool",
    "CycleError",
    "Dialect",
    "EchoProvider",
    "Edge",
    "ErrorStrategy",
    "Event",
    "EventStream",
    "FlowForgeError",
    "Graph",
    "GraphError",
    "HTTPConnectionPool",
    "IterateNode",
    "IterationError",
    "Job",
    "KafkaClient",
    "KafkaError",
    "KafkaEventSink",
    "KafkaProtocolError",
    "LLMError",
    "LLMNode",
    "MCPClient",
    "MCPError",
    "MCPGateway",
    "MCPToolError",
    "MYSQL",
    "MemoryRunStore",
    "Node",
    "NodeContext",
    "NodeExecutionError",
    "NodePaused",
    "NodeRegistry",
    "NodeRun",
    "NodeSpec",
    "NodeStatus",
    "OPENAI_PROFILES",
    "OpenAICompatibleProvider",
    "PoolError",
    "PooledConnection",
    "Profile",
    "Record",
    "RedisClient",
    "RedisError",
    "RedisRunStore",
    "RetryPolicy",
    "RunResult",
    "RunStats",
    "RunStatus",
    "RunStore",
    "S3Client",
    "SQLITE",
    "SQLRunStore",
    "SQLStoreError",
    "ServiceError",
    "StoredArtifact",
    "ToolSpec",
    "UnknownNodeTypeError",
    "VariableError",
    "VariablePool",
    "VertexProvider",
    "Worker",
    "WorkflowEngine",
    "WorkflowService",
    "build_provider",
    "close_shared_pool",
    "crc32c",
    "enqueue",
    "evaluate_condition",
    "get_gateway",
    "graph_fingerprint",
    "known_providers",
    "register_gateway",
    "register_provider",
    "registry",
    "sigv4_headers",
    "sse_body",
]
