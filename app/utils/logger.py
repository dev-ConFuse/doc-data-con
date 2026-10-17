"""Structured logging + per-request observability for the data connectors.

Uniform ConFuse logging contract (mirrors the Rust/TS services):
  * correlation IDs (`x-correlation-id`) — accepted or generated per request,
    bound to structlog contextvars and echoed on the response;
  * one access record per request with outcome classification — `success`
    (2xx), `redirect` (3xx + Location), `client_error` (4xx), `server_error`
    (5xx) — so failures and redirects are as visible as successes;
  * secret-looking query values are redacted before logging.
"""

import logging
import structlog
import sys
import time
from uuid import uuid4
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi import Request

SENSITIVE_QUERY_KEYS = ("token", "key", "secret", "password", "auth")


def sanitize_query(query: str | None) -> str | None:
    """Redact secret-looking query values so access logs never leak credentials."""
    if not query:
        return query
    parts = []
    for pair in query.split("&"):
        key = pair.split("=", 1)[0]
        if any(needle in key.lower() for needle in SENSITIVE_QUERY_KEYS):
            parts.append(f"{key}=[redacted]")
        else:
            parts.append(pair)
    return "&".join(parts)


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """Middleware to extract or generate a correlation ID and bind it to structlog."""
    async def dispatch(self, request: Request, call_next):
        correlation_id = (
            request.headers.get("x-correlation-id")
            or request.headers.get("x-request-id")
            or str(uuid4())
        )

        # Bind it to structlog's thread-local/async context
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(
            correlation_id=correlation_id,
            method=request.method,
            path=request.url.path,
        )

        response = await call_next(request)
        response.headers["X-Correlation-ID"] = correlation_id
        return response


class AccessLogMiddleware(BaseHTTPMiddleware):
    """One access record per request: timing + outcome classification.

    Logs request start at debug and completion at info/warn/error so
    successes, redirects, client errors and server errors are all visible
    in the container logs (`podman logs <service>`).
    """

    async def dispatch(self, request: Request, call_next):
        log = structlog.get_logger("access")
        start = time.perf_counter()
        query = sanitize_query(request.url.query)
        client = request.client.host if request.client else None
        user_agent = request.headers.get("user-agent")
        log.debug(
            "request started",
            query=query,
            client=client,
            user_agent=user_agent,
        )
        try:
            response = await call_next(request)
        except Exception:
            duration_ms = round((time.perf_counter() - start) * 1000, 2)
            log.error(
                "request failed",
                duration_ms=duration_ms,
                outcome="server_error",
                exc_info=True,
            )
            raise
        duration_ms = round((time.perf_counter() - start) * 1000, 2)
        status = response.status_code
        base = {
            "status": status,
            "duration_ms": duration_ms,
            "query": query,
            "client": client,
            "user_agent": user_agent,
        }
        if status < 300:
            log.info("request completed", outcome="success", **base)
        elif status < 400:
            log.warning(
                "request redirected",
                outcome="redirect",
                location=response.headers.get("location"),
                **base,
            )
        elif status < 500:
            log.warning("request failed", outcome="client_error", **base)
        else:
            log.error("request failed", outcome="server_error", **base)
        return response


def configure_logging(level: str, log_format: str) -> None:
    """Configure structlog to format logs and inject context variables.

    `log_format` is "json" (machine-parseable, deployed default) or "console"
    (human-readable). Values come from LOG_LEVEL / LOG_FORMAT (env-policy);
    an unknown level fails fast rather than silently defaulting.
    """
    level_value = logging._nameToLevel.get(level.upper())
    if level_value is None:
        raise ValueError(f"Invalid LOG_LEVEL: {level!r} (expected debug/info/warning/error/critical)")
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.CallsiteParameterAdder(
            {
                structlog.processors.CallsiteParameter.FILENAME,
                structlog.processors.CallsiteParameter.FUNC_NAME,
                structlog.processors.CallsiteParameter.LINENO,
            }
        ),
    ]

    if log_format == "json":
        processors = shared_processors + [
            structlog.processors.dict_tracebacks,
            structlog.processors.JSONRenderer(),
        ]
    else:
        processors = shared_processors + [
            structlog.processors.format_exc_info,
            structlog.dev.ConsoleRenderer(colors=True),
        ]

    structlog.configure(
        processors=processors,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=level_value,
    )
    # uvicorn's access log is a second, unredacted access record (it logs the raw
    # query string) — AccessLogMiddleware emits the contract record instead.
    # A drop filter (not `disabled`): uvicorn's dictConfig runs after app import
    # and resets logger flags, but it never removes filters.
    class _DropAccessLogFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            return False

    logging.getLogger("uvicorn.access").addFilter(_DropAccessLogFilter())

def get_logger(name=None):
    return structlog.get_logger(name)
