"""The FastAPI service layer.

This is what the containers run. It is a thin shell: every decision about what a
run means lives in :class:`~flowforge.service.WorkflowService`, shared with the
dependency-free server in ``server.py``, so the two surfaces answer identically
and neither can quietly drift.

What FastAPI is actually here for — the things the hand-rolled server cannot give
for free:

* **A typed request contract** and a generated OpenAPI schema at ``/openapi.json``,
  so a caller can see the shape of a run request without reading the source.
* **Lifespan management**, which is what finally closes the Redis and SQL
  connection pools on shutdown. The fallback server never did.
* **Streaming with back-pressure preserved.** ``StreamingResponse`` over an async
  generator keeps the property that matters: a slow client parks the response,
  the bounded queue fills, and the producing node suspends rather than buffering
  the whole run.

Error envelopes are deliberately identical to the fallback's — ``{"error": ...}``
with the same status codes, including a 400 rather than FastAPI's default 422 for
a malformed body, because two surfaces of one service disagreeing about what a
bad request looks like is a bug for the client either way.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Mapping

from .service import ServiceError, WorkflowService

try:  # pragma: no cover - exercised by whether the extra is installed
    from fastapi import FastAPI, Request
    from fastapi.exceptions import RequestValidationError
    from fastapi.responses import JSONResponse, StreamingResponse
    from pydantic import BaseModel, Field

    FASTAPI_AVAILABLE = True
except ImportError:  # pragma: no cover
    FASTAPI_AVAILABLE = False


if FASTAPI_AVAILABLE:

    class RunRequest(BaseModel):
        """Body of a run request. Absent entirely is also valid."""

        inputs: dict[str, Any] = Field(
            default_factory=dict, description="Passed to the start node"
        )

    class ResumeRequest(BaseModel):
        """Body of a resume request: the answers the run is waiting on."""

        answers: dict[str, Any] = Field(
            default_factory=dict,
            description="Node id -> the answer it is waiting on",
        )

    # These must live at module scope, not inside create_app. This module uses
    # `from __future__ import annotations`, so handler annotations are strings
    # that FastAPI resolves against module globals — a model defined in a closure
    # is invisible there, and the body silently never binds.


SSE_HEADERS = {
    "Cache-Control": "no-cache",
    "Connection": "close",
    # Nginx and friends will otherwise sit on the stream until it ends, which
    # defeats the entire point of streaming.
    "X-Accel-Buffering": "no",
}


def require_fastapi() -> None:
    if not FASTAPI_AVAILABLE:
        raise ImportError(
            "the FastAPI service layer needs its extra: "
            "pip install 'flowforge[api]'. The dependency-free fallback is "
            "flowforge.server.WorkflowServer"
        )


def create_app(service: WorkflowService, title: str = "FlowForge") -> Any:
    """Build the ASGI app around an already-configured service."""
    require_fastapi()

    @asynccontextmanager
    async def lifespan(app: Any) -> AsyncIterator[None]:
        yield
        # Closes the Redis or SQL pool behind the run store, and any pooled
        # sockets the model adapters left open.
        await service.close()
        from .providers import close_shared_pool

        await close_shared_pool()

    app = FastAPI(
        title=title,
        version="0.5.0",
        summary="A DAG workflow engine with a Kahn-driven concurrent scheduler",
        lifespan=lifespan,
    )
    app.state.service = service

    @app.exception_handler(ServiceError)
    async def _service_error(request: Request, exc: ServiceError) -> JSONResponse:
        return JSONResponse({"error": str(exc)}, status_code=exc.status)

    @app.exception_handler(RequestValidationError)
    async def _bad_body(request: Request, exc: RequestValidationError) -> JSONResponse:
        # 400, not FastAPI's 422, to match the fallback server byte for byte.
        return JSONResponse({"error": f"invalid request body: {exc.errors()}"}, 400)

    @app.get("/health", tags=["ops"])
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/workflows", tags=["workflows"])
    async def workflows() -> dict[str, list[dict[str, Any]]]:
        return {"workflows": service.catalogue()}

    @app.get("/workflows/{workflow_id}", tags=["workflows"])
    async def workflow_shape(workflow_id: str) -> dict[str, Any]:
        """Nodes, edges and wave layout — enough to draw the DAG."""
        return service.shape_of(workflow_id)

    @app.get("/runs", tags=["runs"])
    async def paused_runs() -> dict[str, list[str]]:
        """Runs waiting for an answer. Survives a restart when the store does."""
        return {"paused": await service.paused_ids()}

    @app.post("/runs/{workflow_id}", tags=["runs"])
    async def start_run(
        workflow_id: str, body: RunRequest | None = None
    ) -> dict[str, Any]:
        result = await service.start(workflow_id, (body or RunRequest()).inputs)
        return service.payload_for(result)

    @app.post("/runs/{workflow_id}/stream", tags=["runs"])
    async def stream_run(workflow_id: str, body: RunRequest | None = None) -> Any:
        # engine_for raises 404 here, before the response starts — once the
        # stream is open the status is already sent and cannot be taken back.
        events = service.stream(workflow_id, (body or RunRequest()).inputs)
        return StreamingResponse(
            service.sse_frames(events),
            media_type="text/event-stream",
            headers=SSE_HEADERS,
        )

    @app.post("/runs/{run_id}/resume", tags=["runs"])
    async def resume_run(
        run_id: str, body: ResumeRequest | None = None
    ) -> dict[str, Any]:
        result = await service.resume(run_id, (body or ResumeRequest()).answers)
        return service.payload_for(result)

    @app.post("/runs/{run_id}/resume-stream", tags=["runs"])
    async def resume_run_streaming(
        run_id: str, body: ResumeRequest | None = None
    ) -> Any:
        events = await service.resume_stream(run_id, (body or ResumeRequest()).answers)
        return StreamingResponse(
            service.sse_frames(events),
            media_type="text/event-stream",
            headers=SSE_HEADERS,
        )

    return app


def app_from_directory(
    directory: str = "examples",
    max_concurrency: int | None = None,
    store: Any = None,
) -> Any:
    """Convenience for ``uvicorn``: build a service from a workflow directory."""
    service = WorkflowService.from_directory(
        directory, max_concurrency=max_concurrency, store=store
    )
    return create_app(service)


async def serve(
    service: WorkflowService,
    host: str = "127.0.0.1",
    port: int = 8000,
    log_level: str = "info",
) -> None:
    """Run the app under uvicorn, in this event loop."""
    require_fastapi()
    import uvicorn

    config = uvicorn.Config(
        create_app(service), host=host, port=port, log_level=log_level
    )
    await uvicorn.Server(config).serve()


#: FastAPI's own documentation endpoints. Not part of this service's contract,
#: so they are excluded when pinning the surface.
DOC_ROUTES = frozenset({"/docs", "/docs/oauth2-redirect", "/redoc", "/openapi.json"})


def routes_for(
    service: WorkflowService, include_docs: bool = False
) -> list[tuple[str, str]]:
    """(method, path) pairs. Used by the tests to pin the surface."""
    app = create_app(service)
    found = []
    for route in app.routes:
        path = getattr(route, "path", "")
        if not include_docs and path in DOC_ROUTES:
            continue
        methods: Mapping[str, Any] | set = getattr(route, "methods", set()) or set()
        for method in sorted(methods):
            if method not in ("HEAD", "OPTIONS"):
                found.append((method, path))
    return sorted(found)
