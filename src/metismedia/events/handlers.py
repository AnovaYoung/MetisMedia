"""Test handlers for event bus testing."""

from metismedia.events.envelope import EventEnvelope


class HandlerError(Exception):
    """Error raised by test handlers."""

    pass


async def handler_ok(envelope: EventEnvelope) -> None:
    """Handler that always succeeds.

    Args:
        envelope: Event envelope (unused, always succeeds)
    """
    pass


def make_handler_flaky(fail_until_attempt: int = 3):
    """Create a flaky handler that fails until a certain attempt.

    Args:
        fail_until_attempt: Fail until this attempt number (exclusive)

    Returns:
        Async handler function
    """

    async def handler_flaky(envelope: EventEnvelope) -> None:
        """Handler that fails first N times based on attempt field."""
        if envelope.attempt < fail_until_attempt:
            raise HandlerError(
                f"Flaky failure at attempt {envelope.attempt} "
                f"(will succeed at attempt {fail_until_attempt})"
            )

    return handler_flaky


async def handler_always_fail(envelope: EventEnvelope) -> None:
    """Handler that always throws an error.

    Args:
        envelope: Event envelope

    Raises:
        HandlerError: Always raised
    """
    raise HandlerError(f"Always fails: event_id={envelope.event_id}")


class SpyHandler:
    """Handler that tracks invocation count for testing."""

    def __init__(self) -> None:
        self.call_count = 0
        self.envelopes: list[EventEnvelope] = []

    async def __call__(self, envelope: EventEnvelope) -> None:
        """Handle event and track invocation."""
        self.call_count += 1
        self.envelopes.append(envelope)


def get_test_handler_registry():
    """Get a registry of test handlers.

    Returns:
        Dict mapping event names to handler functions
    """
    return {
        "test.ok": handler_ok,
        "test.flaky": make_handler_flaky(3),
        "test.always_fail": handler_always_fail,
    }
