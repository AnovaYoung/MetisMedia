"""Stable reason codes for decisions and outcomes."""

from enum import Enum


class ReasonCode(str, Enum):
    """Stable reason codes aligned to MetisMedia v2.1 Master Contract."""

    # Safety reason codes
    SAFETY_BURNOUT = "safety_burnout"
    SAFETY_COOLDOWN = "safety_cooldown"
    SAFETY_OPT_OUT = "safety_opt_out"

    # Filter reason codes
    THIRD_RAIL_MATCH = "third_rail_match"
    PLATFORM_MISMATCH = "platform_mismatch"
    GEO_MISMATCH = "geo_mismatch"
    TIER_MISMATCH = "tier_mismatch"
    COMMERCIAL_MISMATCH = "commercial_mismatch"

    # Staleness reason codes
    STALE_OVER_14D = "stale_over_14d"

    # MMS reason codes
    MMS_BELOW_PRECHECK = "mms_below_precheck"
    MMS_BELOW_CACHE = "mms_below_cache"

    # Pulse reason codes
    PULSE_FAIL_DRIFT = "pulse_fail_drift"
    PULSE_INCONCLUSIVE_SCRAPE = "pulse_inconclusive_scrape"

    # Connector reason codes
    CONNECTOR_RATE_LIMIT = "connector_rate_limit"
    CONNECTOR_BLOCKED = "connector_blocked"

    # Discovery reason codes
    NO_RECEIPTS = "no_receipts"
    DUPLICATE_ENTITY = "duplicate_entity"
    LOW_AUTHENTICITY = "low_authenticity"

    # Budget reason codes
    BUDGET_EXHAUSTED = "budget_exhausted"
    TIME_BUDGET_EXHAUSTED = "time_budget_exhausted"
