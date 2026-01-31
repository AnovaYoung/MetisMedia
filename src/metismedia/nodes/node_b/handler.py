"""Node B handler: Genesis Guard - safety + MMS + pulse check."""

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from metismedia.contracts.enums import CacheStatus, NodeName, PolarityIntent, PulseStatus
from metismedia.contracts.models import CostEstimate, DirectiveObject
from metismedia.contracts.reasons import ReasonCode
from metismedia.core.budget import Budget, BudgetExceeded, BudgetState, budget_guard
from metismedia.core.ledger import CostEntry, CostLedger, compute_cost
from metismedia.db.repos import CampaignRepo, RunRepo
from metismedia.events.bus import EventBus
from metismedia.events.envelope import EventEnvelope
from metismedia.events.idemkeys import make_idempotency_key
from metismedia.nodes.node_b.scoring import (
    compute_mms,
    compute_polarity_alignment,
    compute_recency_score,
)
from metismedia.nodes.node_b.thresholds import PULSE_SIMILARITY_MIN, TAU_CACHE, TAU_PRE
from metismedia.providers.embedding_provider import EmbeddingProvider, MockEmbeddingProvider, cosine_similarity
from metismedia.providers.pulse_provider import MockPulseProvider, PulseProvider

logger = logging.getLogger(__name__)

DEFAULT_PRESELECT_K = 200
DEFAULT_DESIRED_COUNT = 10
PULSE_CACHE_TTL_HOURS = 24


@dataclass
class ScoredCandidate:
    """Candidate with computed scores."""

    influencer_id: UUID
    similarity: float
    recency_score: float
    polarity_alignment: float
    mms: float
    last_scraped_at: datetime | None
    polarity_score: int | None
    primary_url: str | None
    bio_text: str | None
    last_pulse_checked_at: datetime | None
    recent_embedding_id: UUID | None


@dataclass
class PulseResult:
    """Result of pulse check for a candidate."""

    influencer_id: UUID
    status: PulseStatus
    reason_codes: list[ReasonCode]
    recent_similarity: float | None = None
    updated_recent_embedding_id: UUID | None = None


def _record_cost(
    envelope: EventEnvelope,
    ledger: CostLedger | None,
    budget: Budget | None,
    budget_state: BudgetState | None,
    provider: str,
    operation: str,
    unit_cost: float,
    quantity: float,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Record cost and enforce budget."""
    dollars = compute_cost(unit_cost, quantity)
    entry = CostEntry(
        tenant_id=envelope.tenant_id,
        trace_id=envelope.trace_id,
        run_id=envelope.run_id,
        node=NodeName.B,
        provider=provider,
        operation=operation,
        unit_cost=unit_cost,
        quantity=quantity,
        dollars=dollars,
        metadata=metadata or {},
    )
    if ledger is not None:
        ledger.record(entry)
    if budget is not None and budget_state is not None:
        budget_guard(
            budget, budget_state,
            cost_delta=dollars,
            provider=provider,
            calls_delta=1,
            node=NodeName.B.value,
        )
        budget_state.dollars_spent += dollars
        budget_state.provider_calls[provider] = (
            budget_state.provider_calls.get(provider, 0) + 1
        )


async def _get_campaign_context(
    session: AsyncSession,
    tenant_id: UUID,
    campaign_id: UUID,
) -> dict[str, Any] | None:
    """Fetch campaign and extract brief context."""
    campaign_repo = CampaignRepo(session)
    campaign = await campaign_repo.get_campaign(tenant_id, campaign_id)
    if not campaign:
        return None

    brief = campaign.get("brief") or {}
    if isinstance(brief, str):
        brief = json.loads(brief) if brief.strip() else {}
    return {
        "campaign_id": campaign_id,
        "polarity_intent": brief.get("polarity_intent", "allies"),
        "commercial_mode": brief.get("commercial_mode", "earned"),
        "slot_values": brief.get("slot_values", {}),
        "target_psychographics": brief.get("target_psychographics", {}),
    }


def _polarity_intent_to_desired(polarity_intent: str) -> int:
    """Convert polarity intent string to desired polarity score."""
    if polarity_intent == "allies":
        return 10
    elif polarity_intent == "critics":
        return -10
    elif polarity_intent == "watchlist":
        return 0
    return 10


async def _safety_prefilter_candidates(
    session: AsyncSession,
    tenant_id: UUID,
    query_embedding_id: UUID,
    campaign_context: dict[str, Any],
    limit: int = DEFAULT_PRESELECT_K,
) -> list[ScoredCandidate]:
    """Safety Shield: SQL prefilter + vector similarity selection.

    Excludes:
    - do_not_contact = true
    - cooling_off_until > now
    - active reservations
    - third rail matches (if terms provided)

    Returns top K by vector similarity.
    """
    now = datetime.now(timezone.utc)
    slot_values = campaign_context.get("slot_values", {})

    third_rail_terms = slot_values.get("third_rail_terms") or []
    if isinstance(third_rail_terms, str):
        third_rail_terms = [t.strip() for t in third_rail_terms.split(",") if t.strip()]

    third_rail_clause = ""
    params: dict[str, Any] = {
        "tenant_id": tenant_id,
        "query_embedding_id": query_embedding_id,
        "now": now,
        "limit": limit,
    }

    if third_rail_terms:
        third_rail_pattern = "|".join(third_rail_terms)
        third_rail_clause = "AND (i.bio_text IS NULL OR i.bio_text !~* :third_rail_pattern)"
        params["third_rail_pattern"] = third_rail_pattern

    platform_clause = ""
    platforms = slot_values.get("platform_vector")
    if platforms:
        if isinstance(platforms, str):
            platforms = [platforms]
        platform_clause = "AND (i.platform IS NULL OR i.platform = ANY(:platforms))"
        params["platforms"] = platforms

    geography_clause = ""
    geography = slot_values.get("geography")
    if geography:
        geography_clause = "AND (i.geography IS NULL OR i.geography ILIKE :geography)"
        params["geography"] = f"%{geography}%"

    query = f"""
        WITH query_vec AS (
            SELECT vector FROM embeddings
            WHERE id = :query_embedding_id AND tenant_id = :tenant_id
        ),
        eligible AS (
            SELECT
                i.id as influencer_id,
                1 - (e.vector <=> (SELECT vector FROM query_vec)) as similarity,
                i.last_scraped_at,
                i.polarity_score,
                i.primary_url,
                i.bio_text,
                i.last_pulse_checked_at,
                i.recent_embedding_id
            FROM influencers i
            JOIN embeddings e ON i.bio_embedding_id = e.id
            WHERE i.tenant_id = :tenant_id
              AND e.tenant_id = :tenant_id
              AND i.do_not_contact = false
              AND (i.cooling_off_until IS NULL OR i.cooling_off_until <= :now)
              AND (SELECT vector FROM query_vec) IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM reservations r
                  WHERE r.tenant_id = :tenant_id
                    AND r.influencer_id = i.id
                    AND r.reserved_until > :now
              )
              {third_rail_clause}
              {platform_clause}
              {geography_clause}
            ORDER BY e.vector <=> (SELECT vector FROM query_vec)
            LIMIT :limit
        )
        SELECT * FROM eligible
    """

    result = await session.execute(text(query), params)
    rows = result.fetchall()

    desired_polarity = _polarity_intent_to_desired(campaign_context.get("polarity_intent", "allies"))

    candidates = []
    for row in rows:
        last_scraped = row.last_scraped_at
        age_days = 999.0
        if last_scraped:
            age_days = (now - last_scraped).total_seconds() / 86400.0

        recency_score = compute_recency_score(age_days)
        influencer_polarity = row.polarity_score if row.polarity_score is not None else 0
        polarity_alignment = compute_polarity_alignment(desired_polarity, influencer_polarity)
        mms = compute_mms(row.similarity, recency_score, polarity_alignment)

        candidates.append(
            ScoredCandidate(
                influencer_id=row.influencer_id,
                similarity=row.similarity,
                recency_score=recency_score,
                polarity_alignment=polarity_alignment,
                mms=mms,
                last_scraped_at=row.last_scraped_at,
                polarity_score=row.polarity_score,
                primary_url=row.primary_url,
                bio_text=row.bio_text,
                last_pulse_checked_at=row.last_pulse_checked_at,
                recent_embedding_id=row.recent_embedding_id,
            )
        )

    return candidates


def _filter_by_mms_threshold(
    candidates: list[ScoredCandidate],
    threshold: float = TAU_PRE,
) -> tuple[list[ScoredCandidate], list[tuple[ScoredCandidate, ReasonCode]]]:
    """Filter candidates by MMS threshold."""
    passing = []
    rejected = []

    for c in candidates:
        if c.mms >= threshold:
            passing.append(c)
        else:
            rejected.append((c, ReasonCode.MMS_BELOW_PRECHECK))

    return passing, rejected


async def _reserve_candidates(
    session: AsyncSession,
    tenant_id: UUID,
    candidates: list[ScoredCandidate],
    reason: str,
    reservation_duration_minutes: int = 30,
) -> dict[UUID, UUID]:
    """Atomically reserve candidates, return mapping of influencer_id -> reservation_id."""
    now = datetime.now(timezone.utc)
    reserved_until = now + timedelta(minutes=reservation_duration_minutes)

    reservations = {}
    for c in candidates:
        exists = await session.execute(
            text("""
                SELECT 1 FROM reservations
                WHERE tenant_id = :tenant_id
                  AND influencer_id = :influencer_id
                  AND reserved_until > :now
            """),
            {"tenant_id": tenant_id, "influencer_id": c.influencer_id, "now": now},
        )
        if exists.fetchone():
            continue

        reservation_id = uuid4()
        await session.execute(
            text("""
                INSERT INTO reservations (id, tenant_id, influencer_id, reserved_until, reason, created_at, updated_at)
                VALUES (:id, :tenant_id, :influencer_id, :reserved_until, :reason, :now, :now)
            """),
            {
                "id": reservation_id,
                "tenant_id": tenant_id,
                "influencer_id": c.influencer_id,
                "reserved_until": reserved_until,
                "reason": reason,
                "now": now,
            },
        )
        reservations[c.influencer_id] = reservation_id

    return reservations


async def _pulse_check_candidate(
    session: AsyncSession,
    tenant_id: UUID,
    candidate: ScoredCandidate,
    campaign_embedding: list[float],
    pulse_provider: PulseProvider,
    embedding_provider: EmbeddingProvider,
    envelope: EventEnvelope,
    ledger: CostLedger | None,
    budget: Budget | None,
    budget_state: BudgetState | None,
) -> PulseResult:
    """Perform pulse check for a candidate.

    1. Check if recent pulse is cached (within TTL)
    2. If not, fetch recent summaries and embed them
    3. Compare with campaign embedding
    4. Update influencer's last_pulse_checked_at and recent_embedding_id
    """
    now = datetime.now(timezone.utc)

    if candidate.last_pulse_checked_at:
        cache_age = now - candidate.last_pulse_checked_at
        if cache_age < timedelta(hours=PULSE_CACHE_TTL_HOURS) and candidate.recent_embedding_id:
            result = await session.execute(
                text("""
                    SELECT vector::text FROM embeddings
                    WHERE tenant_id = :tenant_id AND id = :embedding_id
                """),
                {"tenant_id": tenant_id, "embedding_id": candidate.recent_embedding_id},
            )
            row = result.fetchone()
            if row and row[0]:
                vec_str = row[0].strip("[]")
                recent_vec = [float(x) for x in vec_str.split(",")] if vec_str else None
                if recent_vec:
                    similarity = cosine_similarity(campaign_embedding, recent_vec)
                    status = PulseStatus.PASS if similarity >= PULSE_SIMILARITY_MIN else PulseStatus.FAIL
                    reason_codes = [] if status == PulseStatus.PASS else [ReasonCode.PULSE_FAIL_DRIFT]
                    return PulseResult(
                        influencer_id=candidate.influencer_id,
                        status=status,
                        reason_codes=reason_codes,
                        recent_similarity=similarity,
                    )

    if not candidate.primary_url:
        return PulseResult(
            influencer_id=candidate.influencer_id,
            status=PulseStatus.INCONCLUSIVE,
            reason_codes=[ReasonCode.PULSE_INCONCLUSIVE_SCRAPE],
        )

    try:
        summaries = await pulse_provider.fetch_recent_summaries(candidate.primary_url, limit=3)
        _record_cost(
            envelope, ledger, budget, budget_state,
            "pulse_provider", "fetch_summaries", 0.01, 1.0,
            metadata={"influencer_id": str(candidate.influencer_id)},
        )
    except BudgetExceeded:
        raise  # Re-raise budget exceptions
    except Exception as e:
        logger.warning(f"Pulse fetch failed for {candidate.influencer_id}: {e}")
        return PulseResult(
            influencer_id=candidate.influencer_id,
            status=PulseStatus.INCONCLUSIVE,
            reason_codes=[ReasonCode.PULSE_INCONCLUSIVE_SCRAPE],
        )

    if not summaries:
        return PulseResult(
            influencer_id=candidate.influencer_id,
            status=PulseStatus.INCONCLUSIVE,
            reason_codes=[ReasonCode.PULSE_INCONCLUSIVE_SCRAPE],
        )

    combined_text = " ".join(s.summary for s in summaries)

    try:
        embeddings = await embedding_provider.embed([combined_text])
        _record_cost(
            envelope, ledger, budget, budget_state,
            "embedding_provider", "embed", 0.0001, 1.0,
            metadata={"influencer_id": str(candidate.influencer_id)},
        )
        recent_vec = embeddings[0]
    except BudgetExceeded:
        raise  # Re-raise budget exceptions
    except Exception as e:
        logger.warning(f"Embedding failed for {candidate.influencer_id}: {e}")
        return PulseResult(
            influencer_id=candidate.influencer_id,
            status=PulseStatus.INCONCLUSIVE,
            reason_codes=[ReasonCode.PULSE_INCONCLUSIVE_SCRAPE],
        )

    new_embedding_id = uuid4()
    await session.execute(
        text("""
            INSERT INTO embeddings (id, tenant_id, kind, embedding_model, embedding_dims, embedding_norm, vector, created_at, updated_at)
            VALUES (:id, :tenant_id, 'recent', 'pulse', :dims, 'l2', :vector, :now, :now)
        """),
        {
            "id": new_embedding_id,
            "tenant_id": tenant_id,
            "kind": "recent",
            "dims": len(recent_vec),
            "vector": str(recent_vec),
            "now": now,
        },
    )

    await session.execute(
        text("""
            UPDATE influencers
            SET last_pulse_checked_at = :now, recent_embedding_id = :embedding_id, updated_at = :now
            WHERE tenant_id = :tenant_id AND id = :influencer_id
        """),
        {
            "tenant_id": tenant_id,
            "influencer_id": candidate.influencer_id,
            "embedding_id": new_embedding_id,
            "now": now,
        },
    )

    similarity = cosine_similarity(campaign_embedding, recent_vec)
    status = PulseStatus.PASS if similarity >= PULSE_SIMILARITY_MIN else PulseStatus.FAIL
    reason_codes = [] if status == PulseStatus.PASS else [ReasonCode.PULSE_FAIL_DRIFT]

    return PulseResult(
        influencer_id=candidate.influencer_id,
        status=status,
        reason_codes=reason_codes,
        recent_similarity=similarity,
        updated_recent_embedding_id=new_embedding_id,
    )


async def _get_campaign_embedding(
    session: AsyncSession,
    tenant_id: UUID,
    query_embedding_id: UUID,
) -> list[float] | None:
    """Fetch campaign embedding vector."""
    result = await session.execute(
        text("""
            SELECT vector::text FROM embeddings
            WHERE tenant_id = :tenant_id AND id = :embedding_id
        """),
        {"tenant_id": tenant_id, "embedding_id": query_embedding_id},
    )
    row = result.fetchone()
    if not row or not row[0]:
        return None

    vec_str = row[0].strip("[]")
    return [float(x) for x in vec_str.split(",")] if vec_str else None


async def handle_node_b_input(
    envelope: EventEnvelope,
    session: AsyncSession,
    budget: Budget,
    ledger: CostLedger | None,
    bus: EventBus,
    budget_state: BudgetState | None = None,
    pulse_provider: PulseProvider | None = None,
    embedding_provider: EmbeddingProvider | None = None,
) -> None:
    """Node B handler: Genesis Guard.

    Steps:
    A) Get campaign context + query_embedding_id
    B) Safety Shield SQL prefilter
    C) Vector similarity top K selection
    D) Compute MMS per candidate
    E) Filter by τ_pre threshold
    F) Reserve passing candidates atomically
    G) Pulse check top N until desired_count pass
    H) Produce DirectiveObjects + emit next events
    """
    tenant_id = envelope.tenant_id
    campaign_id_str = envelope.payload.get("campaign_id")
    query_embedding_id_str = envelope.payload.get("query_embedding_id")
    desired_count = envelope.payload.get("limit", DEFAULT_DESIRED_COUNT)

    if pulse_provider is None:
        pulse_provider = MockPulseProvider()
    if embedding_provider is None:
        embedding_provider = MockEmbeddingProvider()

    if not campaign_id_str:
        logger.error("Node B: No campaign_id in payload")
        await _mark_run_failed(session, tenant_id, envelope.run_id, "No campaign_id")
        return

    campaign_id = UUID(campaign_id_str)

    campaign_context = await _get_campaign_context(session, tenant_id, campaign_id)
    if not campaign_context:
        logger.error(f"Node B: Campaign {campaign_id} not found")
        await _mark_run_failed(session, tenant_id, envelope.run_id, f"Campaign {campaign_id} not found")
        return

    if not query_embedding_id_str:
        query_embedding_id_str = campaign_context.get("slot_values", {}).get("query_embedding_id")

    if not query_embedding_id_str:
        logger.warning("Node B: No query_embedding_id, completing with 0 targets")
        await _mark_run_completed_no_targets(session, tenant_id, envelope.run_id, campaign_id_str)
        return

    query_embedding_id = UUID(query_embedding_id_str)

    campaign_embedding = await _get_campaign_embedding(session, tenant_id, query_embedding_id)
    if not campaign_embedding:
        logger.error(f"Node B: Campaign embedding {query_embedding_id} not found")
        await _mark_run_failed(session, tenant_id, envelope.run_id, "Campaign embedding not found")
        return

    _record_cost(
        envelope, ledger, budget, budget_state,
        "postgres", "safety_prefilter", 0.001, 1.0,
    )

    candidates = await _safety_prefilter_candidates(
        session, tenant_id, query_embedding_id, campaign_context, limit=DEFAULT_PRESELECT_K
    )

    if not candidates:
        logger.warning("Node B: No candidates after safety prefilter")
        await _mark_run_completed_no_targets(session, tenant_id, envelope.run_id, campaign_id_str)
        return

    _record_cost(
        envelope, ledger, budget, budget_state,
        "internal", "mms_compute", 0.0, float(len(candidates)),
    )

    passing, rejected = _filter_by_mms_threshold(candidates, TAU_PRE)

    if not passing:
        logger.warning("Node B: No candidates passed MMS threshold")
        await _mark_run_completed_no_targets(session, tenant_id, envelope.run_id, campaign_id_str)
        return

    passing.sort(key=lambda c: c.mms, reverse=True)

    reservations = await _reserve_candidates(
        session, tenant_id, passing[:desired_count * 2],
        reason=f"campaign:{campaign_id}",
    )

    if not reservations:
        logger.warning("Node B: No candidates could be reserved")
        await _mark_run_completed_no_targets(session, tenant_id, envelope.run_id, campaign_id_str)
        return

    reserved_candidates = [c for c in passing if c.influencer_id in reservations]

    pulse_passing: list[tuple[ScoredCandidate, PulseResult]] = []
    pulse_failing: list[tuple[ScoredCandidate, PulseResult]] = []

    for candidate in reserved_candidates[:desired_count * 2]:
        if len(pulse_passing) >= desired_count:
            break

        try:
            pulse_result = await _pulse_check_candidate(
                session, tenant_id, candidate, campaign_embedding,
                pulse_provider, embedding_provider,
                envelope, ledger, budget, budget_state,
            )

            if pulse_result.status == PulseStatus.PASS:
                pulse_passing.append((candidate, pulse_result))
            else:
                pulse_failing.append((candidate, pulse_result))
        except BudgetExceeded as e:
            logger.warning(f"Node B: Budget exceeded during pulse check: {e}")
            raise
        except Exception as e:
            logger.warning(f"Node B: Pulse check failed for {candidate.influencer_id}: {e}")
            pulse_failing.append((
                candidate,
                PulseResult(
                    influencer_id=candidate.influencer_id,
                    status=PulseStatus.INCONCLUSIVE,
                    reason_codes=[ReasonCode.PULSE_INCONCLUSIVE_SCRAPE],
                ),
            ))

    if len(pulse_passing) >= desired_count:
        cache_status = CacheStatus.CACHE_HIT
    elif len(pulse_passing) > 0:
        cache_status = CacheStatus.PARTIAL_HIT
    else:
        cache_status = CacheStatus.CACHE_MISS

    cost_estimate = CostEstimate(
        operation="node_b_genesis_guard",
        provider="internal",
        estimated_cost=0.05 * len(pulse_passing),
    )

    for candidate, pulse_result in pulse_passing:
        reservation_id = reservations.get(candidate.influencer_id)

        directive = DirectiveObject(
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            influencer_id=candidate.influencer_id,
            action="proceed",
            reason_codes=pulse_result.reason_codes,
            cost_estimate=cost_estimate,
            reservation_id=reservation_id,
            cache_status=cache_status,
            pulse_status=pulse_result.status,
            metadata={
                "mms": candidate.mms,
                "similarity": candidate.similarity,
                "recency_score": candidate.recency_score,
                "polarity_alignment": candidate.polarity_alignment,
                "recent_similarity": pulse_result.recent_similarity,
            },
        )

        next_envelope = EventEnvelope(
            tenant_id=tenant_id,
            node=NodeName.B,
            event_name="node_b.directive_emitted",
            trace_id=envelope.trace_id,
            run_id=envelope.run_id,
            idempotency_key=make_idempotency_key(
                tenant_id=tenant_id,
                run_id=envelope.run_id,
                node=NodeName.B,
                event_name="node_b.directive_emitted",
                step=f"proceed:{candidate.influencer_id}",
            ),
            payload={
                "campaign_id": str(campaign_id),
                "influencer_id": str(candidate.influencer_id),
                "reservation_id": str(reservation_id) if reservation_id else None,
                "action": "proceed",
                "mms": candidate.mms,
                "similarity": candidate.similarity,
                "cache_status": cache_status.value,
                "pulse_status": pulse_result.status.value,
            },
        )
        await bus.publish(next_envelope)

    logger.info(
        f"Node B: {len(pulse_passing)} passed pulse check, "
        f"{len(pulse_failing)} failed, cache_status={cache_status.value}"
    )

    if len(pulse_passing) == 0:
        await _mark_run_completed_no_targets(session, tenant_id, envelope.run_id, str(campaign_id))
        return

    if cache_status == CacheStatus.CACHE_MISS:
        needed_count = desired_count - len(pulse_passing)
        node_c_envelope = EventEnvelope(
            tenant_id=tenant_id,
            node=NodeName.C,
            event_name="node_c.discovery_needed",
            trace_id=envelope.trace_id,
            run_id=envelope.run_id,
            idempotency_key=make_idempotency_key(
                tenant_id=tenant_id,
                run_id=envelope.run_id,
                node=NodeName.C,
                event_name="node_c.discovery_needed",
                step="bulk",
            ),
            payload={
                "campaign_id": str(campaign_id),
                "needed_count": needed_count,
                "constraints": campaign_context.get("slot_values", {}),
            },
        )
        await bus.publish(node_c_envelope)


async def _mark_run_failed(
    session: AsyncSession,
    tenant_id: UUID,
    run_id: str,
    error_message: str,
) -> None:
    """Mark run as failed."""
    run_repo = RunRepo(session)
    await run_repo.update_status(
        tenant_id=tenant_id,
        run_id=UUID(run_id),
        status="failed",
        error_message=error_message,
    )


async def _mark_run_completed_no_targets(
    session: AsyncSession,
    tenant_id: UUID,
    run_id: str,
    campaign_id: str,
) -> None:
    """Mark run as completed with 0 targets."""
    run_repo = RunRepo(session)
    await run_repo.update_status(
        tenant_id=tenant_id,
        run_id=UUID(run_id),
        status="completed",
        result_json={
            "target_cards_count": 0,
            "targets_count": 0,
            "drafts_count": 0,
            "total_cost_dollars": 0.0,
            "cost_summary": {},
            "notes": ["No candidates passed Node B filters"],
        },
    )
