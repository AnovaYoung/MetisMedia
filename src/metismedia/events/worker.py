"""Redis Streams consumer worker with retry and DLQ support."""

import asyncio
import inspect
import json
import logging
import random
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from metismedia.contracts.enums import NodeName
from metismedia.core.budget import Budget, BudgetExceeded, BudgetState
from metismedia.core.ledger import CostLedger
from metismedia.db.repos import RunRepo
from metismedia.db.session import db_session
from metismedia.events.bus import EventBus
from metismedia.events.constants import GROUP_NAME, MAX_RETRIES, STREAM_MAIN
from metismedia.events.envelope import EventEnvelope
from metismedia.events.idempotency import already_processed, mark_processed

logger = logging.getLogger(__name__)

# Handler type: async function taking EventEnvelope, returning None
# Extended handlers may also accept budget and ledger kwargs
Handler = Callable[..., Awaitable[None]]

# Backoff configuration
BACKOFF_BASE_SECONDS = 0.5
BACKOFF_JITTER_MAX = 0.2


def _handler_accepts_kwarg(handler: Handler, kwarg: str) -> bool:
    """Check if handler accepts a given keyword argument.

    Args:
        handler: Handler function or callable
        kwarg: Keyword argument name to check

    Returns:
        True if handler accepts the kwarg (explicit param or **kwargs)
    """
    try:
        sig = inspect.signature(handler)
        params = sig.parameters

        if kwarg in params:
            return True

        for param in params.values():
            if param.kind == inspect.Parameter.VAR_KEYWORD:
                return True

        return False
    except (ValueError, TypeError):
        return False


async def _invoke_handler(
    handler: Handler,
    envelope: EventEnvelope,
    ledger: CostLedger | None = None,
    budget_state: BudgetState | None = None,
) -> None:
    """Invoke handler with envelope, passing optional kwargs if supported.

    Backward compatible: if handler only accepts envelope, call with just envelope.
    If handler accepts ledger or budget_state kwarg, pass them.

    Args:
        handler: Handler function or callable
        envelope: Event envelope to pass
        ledger: Optional cost ledger
        budget_state: Optional per-run budget state for enforcement
    """
    kwargs: dict[str, Any] = {}

    if ledger is not None and _handler_accepts_kwarg(handler, "ledger"):
        kwargs["ledger"] = ledger
    if budget_state is not None and _handler_accepts_kwarg(handler, "budget_state"):
        kwargs["budget_state"] = budget_state

    if kwargs:
        await handler(envelope, **kwargs)
    else:
        await handler(envelope)


def calculate_backoff(attempt: int) -> float:
    """Calculate backoff time with exponential factor and jitter.

    Args:
        attempt: Current attempt number (1-based)

    Returns:
        Backoff time in seconds
    """
    exponential = BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
    jitter = random.uniform(0, BACKOFF_JITTER_MAX)
    return exponential + jitter


def decode_envelope(message_data: dict[bytes, bytes]) -> EventEnvelope:
    """Decode Redis stream message to EventEnvelope.

    Args:
        message_data: Raw Redis message data (bytes keys/values)

    Returns:
        Decoded EventEnvelope

    Raises:
        ValueError: If required fields (tenant_id, node) are missing or invalid
    """
    data = {k.decode(): v.decode() for k, v in message_data.items()}

    if not data.get("node"):
        raise ValueError("Missing required field: node")
    try:
        node = NodeName(data["node"])
    except ValueError as e:
        raise ValueError(f"Invalid node value: {data['node']}") from e

    if not data.get("tenant_id"):
        raise ValueError("Missing required field: tenant_id")
    try:
        tenant_id = UUID(data["tenant_id"])
    except ValueError as e:
        raise ValueError(f"Invalid tenant_id value: {data['tenant_id']}") from e

    return EventEnvelope(
        event_id=UUID(data["event_id"]),
        occurred_at=datetime.fromisoformat(data["occurred_at"]),
        tenant_id=tenant_id,
        node=node,
        event_name=data["event_name"],
        payload=json.loads(data["payload"]) if data.get("payload") else {},
        trace_id=data["trace_id"],
        run_id=data["run_id"],
        idempotency_key=data["idempotency_key"],
        attempt=int(data.get("attempt", 0)),
    )


class Worker:
    """Redis Streams consumer worker."""

    def __init__(
        self,
        redis: Redis,
        bus: EventBus,
        group_name: str = GROUP_NAME,
        consumer_name: str = "worker-1",
    ) -> None:
        """Initialize worker.

        Args:
            redis: Redis async client
            bus: EventBus for publishing retries/DLQ
            group_name: Consumer group name
            consumer_name: Unique consumer name within group
        """
        self.redis = redis
        self.bus = bus
        self.group_name = group_name
        self.consumer_name = consumer_name
        self._stop_requested = False
        self._budget_states: dict[str, BudgetState] = {}

    async def ensure_group(self, stream: str = STREAM_MAIN) -> None:
        """Ensure consumer group exists, creating if necessary.

        Args:
            stream: Stream name to create group for
        """
        try:
            await self.redis.xgroup_create(
                stream, self.group_name, id="0", mkstream=True
            )
            logger.info(f"Created consumer group '{self.group_name}' on stream '{stream}'")
        except ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.debug(f"Consumer group '{self.group_name}' already exists")
            else:
                raise

    def stop(self) -> None:
        """Request worker to stop after current iteration."""
        self._stop_requested = True

    async def run(
        self,
        handler_registry: dict[str, Handler],
        stop_after: int | None = None,
        stream: str = STREAM_MAIN,
        block_ms: int = 1000,
        count: int = 10,
        budget: Budget | None = None,
        ledger: CostLedger | None = None,
    ) -> int:
        """Run worker loop, processing messages from stream.

        Args:
            handler_registry: Dict mapping event_name to handler function
            stop_after: Stop after processing N messages (for testing)
            stream: Stream to consume from
            block_ms: XREADGROUP block timeout in ms
            count: Max messages per read
            budget: Optional budget limits. Worker passes budget/ledger to handlers;
                budget enforcement occurs at the node/runtime layer (Module 6).
            ledger: Optional cost ledger; passed to handlers for recording costs.

        Returns:
            Number of messages processed
        """
        await self.ensure_group(stream)

        processed_count = 0
        self._stop_requested = False

        while not self._stop_requested:
            if stop_after is not None and processed_count >= stop_after:
                break

            messages = await self.redis.xreadgroup(
                groupname=self.group_name,
                consumername=self.consumer_name,
                streams={stream: ">"},
                count=count,
                block=block_ms,
            )

            if not messages:
                continue

            for stream_name, stream_messages in messages:
                for message_id, message_data in stream_messages:
                    try:
                        envelope = decode_envelope(message_data)
                        await self._process_message(
                            message_id, envelope, handler_registry, stream,
                            budget=budget, ledger=ledger,
                        )
                        processed_count += 1
                    except Exception as e:
                        logger.exception(f"Fatal error processing message {message_id}: {e}")
                        await self.redis.xack(stream, self.group_name, message_id)

        return processed_count

    async def _process_message(
        self,
        message_id: str | bytes,
        envelope: EventEnvelope,
        handler_registry: dict[str, Handler],
        stream: str,
        budget: Budget | None = None,
        ledger: CostLedger | None = None,
    ) -> None:
        """Process a single message.

        Args:
            message_id: Redis message ID
            envelope: Decoded event envelope
            handler_registry: Handler functions by event name
            stream: Stream name for acking
            budget: Optional budget limits (enforcement at node/runtime layer, Module 6).
            ledger: Optional cost ledger; passed through to handler invocation.
        """
        if await already_processed(self.redis, envelope):
            logger.debug(f"Skipping already processed event: {envelope.idempotency_key}")
            await self.redis.xack(stream, self.group_name, message_id)
            return

        handler = handler_registry.get(envelope.event_name)
        if handler is None:
            logger.warning(f"No handler for event: {envelope.event_name}")
            await self.redis.xack(stream, self.group_name, message_id)
            return

        budget_state: BudgetState | None = None
        if budget is not None:
            key = f"{envelope.tenant_id}:{envelope.run_id}"
            if key not in self._budget_states:
                self._budget_states[key] = BudgetState()
            budget_state = self._budget_states[key]

        try:
            await _invoke_handler(
                handler, envelope, ledger=ledger, budget_state=budget_state
            )
            await mark_processed(self.redis, envelope)
            await self.redis.xack(stream, self.group_name, message_id)
            logger.debug(f"Successfully processed event: {envelope.event_id}")

        except BudgetExceeded as e:
            logger.warning(f"Budget exceeded for run {envelope.run_id}: {e}")
            async with db_session() as session:
                run_repo = RunRepo(session)
                await run_repo.update_status(
                    tenant_id=envelope.tenant_id,
                    run_id=UUID(envelope.run_id),
                    status="failed",
                    error_message=f"Budget exceeded: {e}",
                )
                await session.commit()
            await self.redis.xack(stream, self.group_name, message_id)

        except Exception as e:
            error_msg = str(e)
            current_attempt = envelope.attempt + 1

            if current_attempt < MAX_RETRIES:
                backoff = calculate_backoff(current_attempt)
                logger.warning(
                    f"Handler failed (attempt {current_attempt}/{MAX_RETRIES}), "
                    f"retrying in {backoff:.2f}s: {error_msg}"
                )
                await asyncio.sleep(backoff)

                retry_envelope = EventEnvelope(
                    event_id=envelope.event_id,
                    occurred_at=envelope.occurred_at,
                    tenant_id=envelope.tenant_id,
                    node=envelope.node,
                    event_name=envelope.event_name,
                    payload=envelope.payload,
                    trace_id=envelope.trace_id,
                    run_id=envelope.run_id,
                    idempotency_key=envelope.idempotency_key,
                    attempt=current_attempt,
                )
                await self.bus.publish(retry_envelope)
                await self.redis.xack(stream, self.group_name, message_id)
                logger.debug(f"Requeued event with attempt={current_attempt}")

            else:
                logger.error(
                    f"Max retries ({MAX_RETRIES}) exceeded for event {envelope.event_id}, "
                    f"moving to DLQ: {error_msg}"
                )
                dlq_envelope = EventEnvelope(
                    event_id=envelope.event_id,
                    occurred_at=envelope.occurred_at,
                    tenant_id=envelope.tenant_id,
                    node=envelope.node,
                    event_name=envelope.event_name,
                    payload=envelope.payload,
                    trace_id=envelope.trace_id,
                    run_id=envelope.run_id,
                    idempotency_key=envelope.idempotency_key,
                    attempt=current_attempt,
                )
                await self.bus.publish_dlq(dlq_envelope, error_msg)
                await self.redis.xack(stream, self.group_name, message_id)
