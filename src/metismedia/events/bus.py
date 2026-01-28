"""Redis Streams event bus publisher."""

from redis.asyncio import Redis

from metismedia.events.constants import STREAM_DLQ, STREAM_MAIN
from metismedia.events.envelope import EventEnvelope


class EventBus:
    """Event bus for publishing events to Redis Streams."""

    def __init__(self, redis: Redis) -> None:
        """Initialize event bus.

        Args:
            redis: Redis async client
        """
        self.redis = redis

    async def publish(self, envelope: EventEnvelope) -> str:
        """Publish event to main stream.

        Args:
            envelope: Event envelope to publish

        Returns:
            Redis message ID (e.g., "1234567890123-0")
        """
        fields = envelope.as_redis_fields()
        message_id = await self.redis.xadd(STREAM_MAIN, fields)
        if isinstance(message_id, bytes):
            return message_id.decode()
        return message_id

    async def publish_dlq(self, envelope: EventEnvelope, error: str) -> str:
        """Publish event to dead letter queue with error information.

        Args:
            envelope: Event envelope that failed processing
            error: Error message/description

        Returns:
            Redis message ID
        """
        fields = envelope.as_redis_fields()
        fields["error"] = error
        fields["dlq_reason"] = "max_retries_exceeded"
        message_id = await self.redis.xadd(STREAM_DLQ, fields)
        if isinstance(message_id, bytes):
            return message_id.decode()
        return message_id
