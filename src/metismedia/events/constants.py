"""Constants for Redis event bus."""

# Stream names
STREAM_MAIN = "metismedia:events"
STREAM_DLQ = "metismedia:events:dlq"

# Consumer group name
GROUP_NAME = "metismedia-workers"

# Retry configuration
MAX_RETRIES = 5

# Idempotency TTL (1 day in seconds)
IDEM_TTL_SECONDS = 86400

# Event name constants (mirroring contracts/events.py)
EVENT_CAMPAIGN_CREATED = "campaign.created"
EVENT_CAMPAIGN_COMPLETED = "campaign.completed"
EVENT_NODE_STARTED = "node.started"
EVENT_NODE_COMPLETED = "node.completed"
EVENT_NODE_FAILED = "node.failed"
