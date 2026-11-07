"""Event submission and task processing loops."""

import asyncio
import logging

import httpx
from pydantic import ValidationError

from sentinel.common.config import ServiceConfig
from sentinel.common.http_client import ServiceHTTPClient
from sentinel.common.models import ServiceRole
from sentinel.common.tasks import Failure, SecurityEvent, Success, Task, TaskResult
from sentinel.producer.generator import SecurityEventGenerator
from sentinel.worker.executor import WorkerExecutor

logger = logging.getLogger(__name__)


async def produce(config: ServiceConfig, client: ServiceHTTPClient) -> None:
    generator = SecurityEventGenerator(config.service_id)
    pending: SecurityEvent | None = None
    while True:
        if pending is None:
            pending = generator.generate()
        try:
            await client.submit_event(pending)
            logger.info(
                "Submitted event=%s producer=%s", pending.event_id, config.service_id
            )
            pending = None
        except (httpx.HTTPError, ValidationError) as error:
            logger.warning(
                "Event submission failed producer=%s: %s", config.service_id, error
            )
        await asyncio.sleep(config.event_generation_interval)


async def work(config: ServiceConfig, client: ServiceHTTPClient) -> None:
    executor = WorkerExecutor()
    pending: TaskResult | None = None
    task: Task | None = None
    while True:
        try:
            if pending is None:
                task = await client.claim_task(config.service_id)
                if task is not None:
                    outcome: Success | Failure
                    try:
                        if config.processing_delay:
                            await asyncio.sleep(config.processing_delay)
                        result = await asyncio.to_thread(
                            executor.execute,
                            task.command,
                            task.event.model_dump(mode="json"),
                        )
                        outcome = Success(status="success", result=result)
                    except Exception as error:
                        outcome = Failure(
                            status="failure", error=str(error) or type(error).__name__
                        )
                    pending = TaskResult(worker_id=config.service_id, outcome=outcome)
            if pending is not None:
                assert task is not None
                await client.submit_result(task.task_id, pending)
                logger.info(
                    "Acknowledged task=%s worker=%s", task.task_id, config.service_id
                )
                pending = None
        except httpx.HTTPStatusError as error:
            # A lost acknowledgement yields 409; central restart can yield 404.
            # Neither is a reason to execute again or to block this worker forever.
            if pending is not None and error.response.status_code in (404, 409):
                logger.warning(
                    "Discarding unacknowledged terminal result task=%s: %s",
                    task.task_id,
                    error,
                )
                pending = None
            else:
                logger.warning(
                    "Worker HTTP failure worker=%s: %s", config.service_id, error
                )
        except (httpx.HTTPError, ValidationError) as error:
            logger.warning(
                "Worker transport failure worker=%s: %s", config.service_id, error
            )
        await asyncio.sleep(config.worker_poll_interval)


async def run_pipeline(
    config: ServiceConfig,
    client: ServiceHTTPClient,
    registered: asyncio.Event,
) -> None:
    await registered.wait()
    interval = (
        config.event_generation_interval
        if config.role == ServiceRole.PRODUCER
        else config.worker_poll_interval
    )
    await asyncio.sleep(interval)
    if config.role == ServiceRole.PRODUCER:
        await produce(config, client)
    else:
        await work(config, client)
