"""Node B MMS scoring: recency, polarity alignment, product-of-experts. No DB or provider calls."""

import math

_EPS = 1e-10
_RECENCY_HALFLIFE_DAYS = 7.0
_RECENCY_HARD_CUTOFF_DAYS = 14.0
_POLARITY_SCALE = 10  # desired/influencer in [-10, +10]


def compute_recency_score(age_days: float) -> float:
    """Recency score with hard cutoff and exponential decay.

    - If age_days > 14 => 0.0
    - Else exponential decay with half-life 7 days: score = 0.5^(age_days/7)
    """
    if age_days > _RECENCY_HARD_CUTOFF_DAYS:
        return 0.0
    return math.exp(-math.log(2) * age_days / _RECENCY_HALFLIFE_DAYS)


def compute_polarity_alignment(desired: int, influencer: int) -> float:
    """Polarity alignment in [0, 1]. desired and influencer in [-10, +10].

    - Allies case: if desired > 0 and influencer < 0 => 0
    - Otherwise continuous alignment: (1 + (desired * influencer) / 100) / 2 clipped to [0, 1]
    """
    if desired > 0 and influencer < 0:
        return 0.0
    # Continuous: same sign => high; opposite => low. (1 + desired*influencer/100)/2 in [0,1]
    raw = (1.0 + (desired * influencer) / (_POLARITY_SCALE * _POLARITY_SCALE)) / 2.0
    return max(0.0, min(1.0, raw))


def product_of_experts(
    factors: dict[str, float],
    weights: dict[str, float],
    eps: float = _EPS,
) -> float:
    """Product-of-experts: exp(sum(w * ln(max(eps, x))) / sum(w))."""
    if not factors or not weights:
        return 0.0
    total_weight = 0.0
    weighted_log_sum = 0.0
    for name, w in weights.items():
        if w <= 0:
            continue
        x = factors.get(name, 0.0)
        total_weight += w
        weighted_log_sum += w * math.log(max(eps, x))
    if total_weight <= 0:
        return 0.0
    return math.exp(weighted_log_sum / total_weight)


def compute_mms(
    similarity: float,
    recency_score: float,
    polarity_alignment: float,
    weights: dict[str, float] | None = None,
) -> float:
    """MMS in [0, 1] as product-of-experts of similarity, recency, polarity."""
    if weights is None:
        weights = {"similarity": 1.0, "recency": 1.0, "polarity": 1.0}
    factors = {
        "similarity": max(0.0, min(1.0, similarity)),
        "recency": max(0.0, min(1.0, recency_score)),
        "polarity": max(0.0, min(1.0, polarity_alignment)),
    }
    return max(0.0, min(1.0, product_of_experts(factors, weights)))
