"""Tests for Node B scoring: recency, polarity, product-of-experts, MMS. No DB or provider calls."""

import pytest

from metismedia.nodes.node_b.scoring import (
    compute_mms,
    compute_polarity_alignment,
    compute_recency_score,
    product_of_experts,
)


class TestComputeRecencyScore:
    """Test recency score: hard cutoff at 14 days, exponential decay."""

    def test_zero_at_cutoff(self) -> None:
        assert compute_recency_score(14.0) == pytest.approx(0.5 ** (14 / 7), rel=1e-9)

    def test_zero_above_cutoff(self) -> None:
        assert compute_recency_score(14.01) == 0.0
        assert compute_recency_score(15) == 0.0
        assert compute_recency_score(100) == 0.0

    def test_one_at_zero_days(self) -> None:
        assert compute_recency_score(0.0) == 1.0

    def test_half_life_at_seven_days(self) -> None:
        assert compute_recency_score(7.0) == pytest.approx(0.5, rel=1e-9)

    def test_monotonic_decay(self) -> None:
        r0 = compute_recency_score(0)
        r3 = compute_recency_score(3)
        r7 = compute_recency_score(7)
        r14 = compute_recency_score(14)
        assert r0 > r3 > r7 > r14 > 0


class TestComputePolarityAlignment:
    """Test polarity alignment: allies hard-zero, continuous otherwise."""

    def test_allies_hard_zero(self) -> None:
        """desired > 0 and influencer < 0 => 0."""
        assert compute_polarity_alignment(1, -1) == 0.0
        assert compute_polarity_alignment(10, -10) == 0.0
        assert compute_polarity_alignment(5, -3) == 0.0

    def test_continuous_aligned_positive(self) -> None:
        """Both positive => high alignment."""
        assert compute_polarity_alignment(10, 10) == 1.0
        assert compute_polarity_alignment(5, 5) > 0.5

    def test_continuous_opposite_negative(self) -> None:
        """Both negative (critics) aligned; opposite signs (already covered) zero for allies."""
        assert compute_polarity_alignment(-10, -10) == 1.0
        assert compute_polarity_alignment(-5, -5) > 0.5

    def test_in_range(self) -> None:
        for d in [-10, 0, 10]:
            for i in [-10, 0, 10]:
                v = compute_polarity_alignment(d, i)
                assert 0 <= v <= 1, f"desired={d} influencer={i} => {v}"


class TestProductOfExperts:
    """Test product-of-experts: collapses when one factor is low."""

    def test_collapses_if_one_factor_low(self) -> None:
        """One low factor pulls the product down (PoE is geometric mean in equal-weight case)."""
        factors = {"a": 1.0, "b": 1.0, "c": 0.01}
        weights = {"a": 1.0, "b": 1.0, "c": 1.0}
        out = product_of_experts(factors, weights)
        assert out < 0.5
        assert out > 0

    def test_high_when_all_high(self) -> None:
        factors = {"a": 1.0, "b": 1.0, "c": 1.0}
        weights = {"a": 1.0, "b": 1.0, "c": 1.0}
        assert product_of_experts(factors, weights) == pytest.approx(1.0, rel=1e-9)

    def test_zero_factor_dominated_by_eps(self) -> None:
        factors = {"x": 0.0}
        weights = {"x": 1.0}
        out = product_of_experts(factors, weights)
        assert out > 0
        assert out < 0.01

    def test_single_factor(self) -> None:
        assert product_of_experts({"s": 0.5}, {"s": 1.0}) == pytest.approx(0.5, rel=1e-9)

    def test_empty_returns_zero(self) -> None:
        assert product_of_experts({}, {"a": 1.0}) == 0.0
        assert product_of_experts({"a": 1.0}, {}) == 0.0


class TestComputeMms:
    """MMS monotonic sanity checks."""

    def test_mms_in_zero_one(self) -> None:
        for s in [0.0, 0.5, 1.0]:
            for r in [0.0, 0.5, 1.0]:
                for p in [0.0, 0.5, 1.0]:
                    m = compute_mms(s, r, p)
                    assert 0 <= m <= 1, f"s={s} r={r} p={p} => mms={m}"

    def test_mms_monotonic_in_similarity(self) -> None:
        r, p = 0.8, 0.9
        m0 = compute_mms(0.5, r, p)
        m1 = compute_mms(0.7, r, p)
        m2 = compute_mms(1.0, r, p)
        assert m0 < m1 < m2

    def test_mms_monotonic_in_recency(self) -> None:
        s, p = 0.9, 0.9
        m0 = compute_mms(s, 0.3, p)
        m1 = compute_mms(s, 0.7, p)
        m2 = compute_mms(s, 1.0, p)
        assert m0 < m1 < m2

    def test_mms_monotonic_in_polarity(self) -> None:
        s, r = 0.9, 0.9
        m0 = compute_mms(s, r, 0.2)
        m1 = compute_mms(s, r, 0.6)
        m2 = compute_mms(s, r, 1.0)
        assert m0 < m1 < m2

    def test_mms_zero_if_any_zero(self) -> None:
        assert compute_mms(0.0, 1.0, 1.0) < 0.01
        assert compute_mms(1.0, 0.0, 1.0) < 0.01
        assert compute_mms(1.0, 1.0, 0.0) < 0.01
