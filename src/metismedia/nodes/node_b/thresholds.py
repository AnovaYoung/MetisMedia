"""Node B thresholds: precheck, cache-eligible, pulse. Conservative defaults."""

# Precheck: MMS must be >= τ_pre to pass initial gate
TAU_PRE: float = 0.85

# Cache-eligible: after pulse, MMS >= τ_cache to use cache path
TAU_CACHE: float = 0.90

# Pulse: minimum similarity for pulse to be considered conclusive
PULSE_SIMILARITY_MIN: float = 0.85
