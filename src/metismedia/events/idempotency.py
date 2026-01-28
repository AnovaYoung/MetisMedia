"""Idempotency helpers for event processing."""

from redis.asyncio import Redis

from metismedia.events.constants import IDEM_TTL_SECONDS
from metismedia.events.envelope import EventEnvelope


def build_idem_key(envelope: EventEnvelope) -> str:
    """Build idempotency key from event envelope.

    Args:
        envelope: Event envelope

    Returns:
        Idempotency key string: "idem:{node}:{idempotency_key}"
    """
    return f"idem:{envelope.node.value}:{envelope.idempotency_key}"


async def already_processed(redis: Redis, envelope: EventEnvelope) -> bool:
    """Check if event has already been processed.

    Args:
        redis: Redis async client
        envelope: Event envelope

    Returns:
        True if already processed, False otherwise
    """
    key = build_idem_key(envelope)
    result = await redis.get(key)
    return result is not None


async def mark_processed(
    redis: Redis,
    envelope: EventEnvelope,
    ttl_seconds: int = IDEM_TTL_SECONDS,
) -> None:
    """Mark event as processed with TTL.

    Args:
        redis: Redis async client
        envelope: Event envelope
        ttl_seconds: TTL in seconds (defaults to IDEM_TTL_SECONDS)
    """
    key = build_idem_key(envelope)
    await redis.setex(key, ttl_seconds, "1")
