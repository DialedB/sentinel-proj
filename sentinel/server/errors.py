"""Uniform HTTP errors without echoing secrets, nonfinite values or internals."""

import logging
import sqlite3

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException

from sentinel.common.errors import DomainError, storage_unavailable

logger = logging.getLogger(__name__)
_ACTIONS = {
    400: ("bad_request", "Correct the request syntax and retry."),
    404: ("not_found", "Check the resource ID or register the service first."),
    405: ("method_not_allowed", "Use the documented HTTP method."),
    409: ("conflict", "Refresh task state; use the current lease or a new event ID."),
    422: ("validation_error", "Correct the indicated fields and retry."),
    503: ("service_unavailable", "Check service health and retry with backoff."),
}


def error_response(
    status: int, code: str, message: str, next_action: str, **extra: object
) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={
            "detail": message,
            "error": {"code": code, "message": message, "next_action": next_action},
            **extra,
        },
    )


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(DomainError)
    async def domain_error(request: Request, error: DomainError) -> JSONResponse:
        if error.status >= 500:
            logger.error("%s path=%s: %s", error.code, request.url.path, error)
        return error_response(error.status, error.code, str(error), error.next_action)

    @app.exception_handler(HTTPException)
    async def http_error(request: Request, error: HTTPException) -> JSONResponse:
        code, action = _ACTIONS.get(
            error.status_code, ("http_error", "Check the API documentation.")
        )
        response = error_response(error.status_code, code, str(error.detail), action)
        if error.headers:
            response.headers.update(error.headers)
        return response

    @app.exception_handler(RequestValidationError)
    async def validation_error(
        request: Request, error: RequestValidationError
    ) -> JSONResponse:
        # Do not serialize raw input/ctx: either can contain NaN, secrets, or exceptions.
        fields = [
            {"loc": item["loc"], "type": item["type"], "message": item["msg"]}
            for item in error.errors()
        ]
        return error_response(
            422,
            "validation_error",
            "Request validation failed",
            _ACTIONS[422][1],
            fields=fields,
        )

    @app.exception_handler(sqlite3.Error)
    async def storage_error(request: Request, error: sqlite3.Error) -> JSONResponse:
        logger.exception("Storage failure path=%s", request.url.path)
        failure = storage_unavailable()
        return error_response(
            failure.status, failure.code, str(failure), failure.next_action
        )

    @app.exception_handler(Exception)
    async def unexpected_error(request: Request, error: Exception) -> JSONResponse:
        logger.exception("Unexpected failure path=%s", request.url.path)
        return error_response(
            500,
            "internal_error",
            "Unexpected service failure",
            "Inspect central logs before retrying; retain the event ID for safe resubmission.",
        )
