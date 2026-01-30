"""Enum definitions matching the Master Contract."""

from enum import Enum


class PolarityTarget(str, Enum):
    """Polarity target classification."""

    POSITIVE = "positive"
    NEGATIVE = "negative"
    NEUTRAL = "neutral"


class CommercialMode(str, Enum):
    """Commercial interaction mode."""

    PAID = "paid"
    GIFTED = "gifted"
    COLLABORATION = "collaboration"
    PERMISSIONED_ONLY = "permissioned_only"


class Platform(str, Enum):
    """Social media platform identifiers."""

    INSTAGRAM = "instagram"
    TIKTOK = "tiktok"
    TWITTER = "twitter"
    YOUTUBE = "youtube"
    LINKEDIN = "linkedin"
    REDDIT = "reddit"
    DISCORD = "discord"
    TELEGRAM = "telegram"
    CREATOR_WEB = "creator_web"
    OTHER = "other"


class ReceiptType(str, Enum):
    """Type of receipt/evidence."""

    POST = "post"
    COMMENT = "comment"
    ARTICLE = "article"
    VIDEO = "video"
    PODCAST = "podcast"
    NEWSLETTER = "newsletter"
    RSS_FEED = "rss_feed"
    PROFILE = "profile"
    OTHER = "other"


class NodeStatus(str, Enum):
    """Status of a node execution."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    PARTIAL = "partial"


class ReasonCode(str, Enum):
    """Reason codes for decisions and outcomes."""

    # Node B reason codes
    SAFETY_BLOCK = "safety_block"
    MMS_THRESHOLD_NOT_MET = "mms_threshold_not_met"
    PULSE_CHECK_FAILED = "pulse_check_failed"
    BUDGET_EXCEEDED = "budget_exceeded"
    RESERVATION_CONFLICT = "reservation_conflict"

    # Node C reason codes
    DEDUPE_MATCH = "dedupe_match"
    DIVERSITY_FILTER = "diversity_filter"
    QUALITY_THRESHOLD = "quality_threshold"
    DISCOVERY_BUDGET_EXCEEDED = "discovery_budget_exceeded"

    # Node D reason codes
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
    CONTRADICTORY_EVIDENCE = "contradictory_evidence"
    UNKNOWN_FIELD = "unknown_field"
    RECEIPT_MISSING = "receipt_missing"

    # Node E reason codes
    CONTACT_NOT_FOUND = "contact_not_found"
    CONTACT_VERIFICATION_FAILED = "contact_verification_failed"
    PERMISSIONED_ONLY = "permissioned_only"

    # Node F reason codes
    MISSING_CONTACT = "missing_contact"
    TEMPLATE_ERROR = "template_error"
    RECEIPT_INCLUSION_FAILED = "receipt_inclusion_failed"

    # General reason codes
    SUCCESS = "success"
    PARTIAL_SUCCESS = "partial_success"
    TIMEOUT = "timeout"
    PROVIDER_ERROR = "provider_error"
    VALIDATION_ERROR = "validation_error"
    UNKNOWN_ERROR = "unknown_error"
