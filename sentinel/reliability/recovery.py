"""Periodic lease recovery; cancellation waits for the current short transaction."""

import asyncio
import logging
import sqlite3
from collections.abc import Callable
from typing import TypeVar

from sentinel.storage.task_repository import TaskRepository

logger = logging.getLogger(__name__)
T = TypeVar("T")


async def storage_call(call: Callable[[], T]) -> T:
    operation = asyncio.create_task(asyncio.to_thread(call))
    try:
        return await asyncio.shield(operation)
    except asyncio.CancelledError:
        # Threads cannot be cancelled. Join before lifespan teardown completes.
        await operation
        raise


async def recover_once(repository: TaskRepository) -> int:
    count = await storage_call(repository.recover_expired)
    if count:
        logger.info("Recovered %s expired task leases", count)
    return count


async def run_recovery(repository: TaskRepository, interval: float) -> None:
    while True:
        await asyncio.sleep(interval)
        try:
            await recover_once(repository)
        except sqlite3.Error:
            logger.exception("Task lease recovery failed; retrying next interval")
