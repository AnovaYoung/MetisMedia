"""Canonical enum definitions aligned to MetisMedia v2.1 Master Contract."""

from enum import Enum


class PolarityIntent(str, Enum):
    """Polarity intent classification."""

    ALLIES = "allies"
    CRITICS = "critics"
    WATCHLIST = "watchlist"


class CommercialMode(str, Enum):
    """Commercial interaction mode."""

    EARNED = "earned"
    PAID = "paid"
    HYBRID = "hybrid"
    UNKNOWN = "unknown"


class CacheStatus(str, Enum):
    """Cache lookup status."""

    CACHE_HIT = "cache_hit"
    PARTIAL_HIT = "partial_hit"
    CACHE_MISS = "cache_miss"


class PulseStatus(str, Enum):
    """Pulse check status."""

    PASS = "pass"
    FAIL = "fail"
    INCONCLUSIVE = "inconclusive"


class ReceiptType(str, Enum):
    """Type of receipt/evidence."""

    SOCIAL = "social"
    CREATOR = "creator"
    THREAD = "thread"
    AUDIO = "audio"


class NodeName(str, Enum):
    """Node identifiers in the orchestration graph."""

    A = "A"
    B = "B"
    C = "C"
    D = "D"
    E = "E"
    F = "F"
    G = "G"


class Platform(str, Enum):
    """Social media platform identifiers (MVP, policy-safe)."""

    X = "x"
    BLUESKY = "bluesky"
    SUBSTACK = "substack"
    BLOG = "blog"
    NEWSLETTER = "newsletter"
    PODCAST = "podcast"
    YOUTUBE = "youtube"
    REDDIT = "reddit"
    OTHER = "other"
