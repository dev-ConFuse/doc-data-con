"""
Data Connector Service - Main Application
Port: 8081
Role: Universal source integration and intelligent routing
"""

import sys
import asyncio
import structlog
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

# Load environment variables from .map.env (non-sensitive), .secret.env (sensitive), and .env.local
load_dotenv(".map.env")
load_dotenv(".secret.env", override=True)
load_dotenv(".env.local", override=True)

from app.config import get_settings
from app.infra.db.postgres import (
    init_postgresql,
    close_postgresql,
    health_check as postgres_health_check,
)
from app.routing.core_routes import router as api_router
from app.routing.documents_routes import router as documents_router

# from app.routing.databases_routes import router as databases_router
from app.connectors.notion_client import notion_router, notion_callback_router
from app.utils.error_handling import ErrorHandlingMiddleware, setup_error_handlers

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    settings = get_settings()
    logger.info(
        "Starting data-connector service (fail-fast mode)",
        port=settings.port,
    )

    # Initialize PostgreSQL (REQUIRED - will crash if unavailable)
    await init_postgresql()

    # --- Credential Storage Initialization ---
    try:
        from app.security.credentials import init_credential_storage
        from app.infra.db.postgres import get_session

        credential_storage = init_credential_storage(
            encryption_key=settings.encryption_key, db_session_factory=get_session
        )
        app.state.credential_storage = credential_storage
        logger.info("Credential storage initialized successfully")
    except Exception as e:
        logger.error("Failed to initialize credential storage", error=str(e))
        raise  # Fail fast if credential storage cannot be initialized

        # Removed dead TokenRefreshService code

    logger.info("Application startup complete")
    yield







    # Close database connections
    await close_postgresql()

    logger.info("Data-connector service stopped")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    settings = get_settings()

    app = FastAPI(
        title="Data Connector Service",
        description="Universal source integration and intelligent routing for ConFuse",
        version="2.0.0",
        lifespan=lifespan,
    )

    # Add error handling middleware
    app.add_middleware(ErrorHandlingMiddleware)

    # Correlation IDs + per-request access logs (outcome classification)
    from app.utils.logger import (
        AccessLogMiddleware,
        CorrelationIdMiddleware,
        configure_logging,
    )
    configure_logging(level=settings.log_level, log_format=settings.log_format)
    app.add_middleware(AccessLogMiddleware)
    app.add_middleware(CorrelationIdMiddleware)

    # Setup error handlers
    setup_error_handlers(app)

    # Add security headers middleware
    try:
        from app.security.security_headers import SecurityHeadersMiddleware

        app.add_middleware(SecurityHeadersMiddleware)
    except ImportError:
        logger.warning("confuse_common not installed, skipping SecurityHeadersMiddleware")

    # Add rate limiting middleware
    try:
        from app.security.middleware_rate_limit import RateLimitMiddleware

        app.add_middleware(RateLimitMiddleware)
    except ImportError:
        logger.warning("confuse_common not installed, skipping RateLimitMiddleware")

    # Configure CORS (MUST be last middleware to be outermost - FastAPI executes in reverse order)
    origins = [origin.strip() for origin in settings.cors_origins.split(",")]

    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(documents_router)

    app.include_router(notion_router)
    app.include_router(notion_callback_router)

    # Include API routes
    app.include_router(api_router)

    # Health check endpoint
    @app.api_route("/", methods=["GET", "HEAD"])
    async def root_check():
        return {"status": "ok"}

    @app.get("/health")
    async def health_check():
        """Comprehensive health check endpoint."""
        health_status = {"status": "healthy", "version": "2.0.0", "components": {}}

        overall_healthy = True

        # Check PostgreSQL
        postgres_health = await postgres_health_check()
        health_status["components"]["postgresql"] = postgres_health
        if postgres_health and postgres_health.get("status") != "healthy":
            overall_healthy = False

        # Set overall status
        health_status["status"] = "healthy" if overall_healthy else "unhealthy"

        return health_status

    return app


app = create_app()


# trigger reload
if __name__ == "__main__":
    import uvicorn
    import asyncio

    settings = get_settings()

    # Run only the HTTP server; gRPC and Kafka removed as data-connector communicates
    # with unified-processor via HTTP only.
    config = uvicorn.Config(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
    )
    server = uvicorn.Server(config)
    asyncio.run(server.serve())
