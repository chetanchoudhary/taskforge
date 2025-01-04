from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from prometheus_fastapi_instrumentator import Instrumentator

from taskforge.api.docs import custom_openapi, setup_api_docs
from taskforge.api.routes import api_router
from taskforge.core.config import settings
from taskforge.di import Container
from taskforge.exceptions import TaskForgeError
from taskforge.integrations.events import event_bus
from taskforge.integrations.webhooks import webhook_manager

logger = structlog.get_logger()


class TaskForgeAPI:
    """Main FastAPI application class"""

    def __init__(self):
        self.app = FastAPI(
            title="TaskForge API",
            description="Distributed Job Processing API",
            version="1.0.0",
            docs_url="/api/docs",
            redoc_url="/api/redoc",
            openapi_url="/api/openapi.json",
            lifespan=self.lifespan,
        )

        self.setup_middleware()
        self.setup_exception_handlers()
        self.setup_routes()
        self.setup_monitoring()

        # Set custom OpenAPI schema
        self.app.openapi = lambda: custom_openapi(self.app)
        setup_api_docs(self.app)

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        """Manage API server lifecycle"""
        # Startup
        logger.info("Starting TaskForge API")

        # Initialize container
        app.container = await Container.init()

        # Initialize components
        app.state.orchestrator = app.container.orchestrator()
        app.state.workflow_engine = app.container.workflow_engine()
        app.state.scheduler = app.container.scheduler()

        # Start event bus and webhook manager
        await event_bus.start()
        await webhook_manager.start()

        yield

        # Shutdown
        logger.info("Shutting down TaskForge API")
        await event_bus.stop()
        await webhook_manager.stop()
        await app.container.cleanup()

    def setup_middleware(self):
        """Set up API middleware"""
        # CORS
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.security.allowed_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # Gzip compression
        self.app.add_middleware(GZipMiddleware, minimum_size=1000)

        # Request logging
        @self.app.middleware("http")
        async def log_requests(request: Request, call_next):
            logger.info(
                "Incoming request",
                method=request.method,
                url=str(request.url),
                client=request.client.host,
            )
            response = await call_next(request)
            return response

    def setup_exception_handlers(self):
        """Set up global exception handlers"""

        @self.app.exception_handler(TaskForgeError)
        async def taskforge_exception_handler(request: Request, exc: TaskForgeError):
            return JSONResponse(
                status_code=400,
                content={
                    "detail": str(exc),
                    "code": exc.__class__.__name__,
                    "params": exc.details,
                },
            )

        @self.app.exception_handler(HTTPException)
        async def http_exception_handler(request: Request, exc: HTTPException):
            return JSONResponse(
                status_code=exc.status_code,
                content={
                    "detail": exc.detail,
                    "code": "http_error",
                },
            )

    def setup_routes(self):
        """Set up API routes"""
        self.app.include_router(api_router)

    def setup_monitoring(self):
        """Set up Prometheus monitoring"""
        Instrumentator().instrument(self.app).expose(self.app)

    def start(self):
        """Start the API server"""
        import uvicorn

        uvicorn.run(
            self.app,
            host=settings.api_host,
            port=settings.api_port,
            log_level=settings.log_level.lower(),
        )


# Create app instance
app = TaskForgeAPI().app
