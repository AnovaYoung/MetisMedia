"""Initial schema with extensions and MVP tables.

Revision ID: 001_initial_schema
Revises:
Create Date: 2025-01-28

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from pgvector.sqlalchemy import Vector

revision: str = "001_initial_schema"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

EMBEDDING_DIMS = 1536


def upgrade() -> None:
    # Create extensions
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    op.execute("CREATE EXTENSION IF NOT EXISTS citext")

    # A) campaigns
    op.create_table(
        "campaigns",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("tenant_id", sa.UUID(), nullable=False),
        sa.Column("trace_id", sa.Text(), nullable=True),
        sa.Column("run_id", sa.Text(), nullable=True),
        sa.Column("brief", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_campaigns_tenant_id", "campaigns", ["tenant_id"])

    # B) embeddings
    op.create_table(
        "embeddings",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("tenant_id", sa.UUID(), nullable=False),
        sa.Column("embedding_model", sa.Text(), nullable=True),
        sa.Column("embedding_dims", sa.Integer(), nullable=True),
        sa.Column("embedding_norm", sa.Text(), nullable=True),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("vector", Vector(EMBEDDING_DIMS), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("kind IN ('campaign', 'bio', 'recent')", name="ck_embeddings_kind"),
    )
    op.create_index("ix_embeddings_tenant_kind", "embeddings", ["tenant_id", "kind"])
    op.execute(
        "CREATE INDEX ix_embeddings_vector ON embeddings USING ivfflat (vector vector_cosine_ops) WITH (lists = 100)"
    )

    # C) influencers
    op.create_table(
        "influencers",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("tenant_id", sa.UUID(), nullable=False),
        sa.Column("canonical_name", sa.Text(), nullable=False),
        sa.Column("primary_url", sa.Text(), nullable=True),
        sa.Column("platform", sa.Text(), nullable=True),
        sa.Column("geography", sa.Text(), nullable=True),
        sa.Column("follower_count", sa.Integer(), nullable=True),
        sa.Column("commercial_mode", sa.Text(), nullable=True),
        sa.Column("polarity_score", sa.Integer(), nullable=True),
        sa.Column("bio_embedding_id", sa.UUID(), sa.ForeignKey("embeddings.id"), nullable=True),
        sa.Column("recent_embedding_id", sa.UUID(), sa.ForeignKey("embeddings.id"), nullable=True),
        sa.Column("bio_text", sa.Text(), nullable=True),
        sa.Column("last_scraped_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("last_pulse_checked_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("do_not_contact", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("cooling_off_until", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_influencers_tenant_id", "influencers", ["tenant_id"])
    op.execute(
        "CREATE UNIQUE INDEX ix_influencers_tenant_url ON influencers (tenant_id, primary_url) WHERE primary_url IS NOT NULL"
    )
    op.execute("CREATE INDEX ix_influencers_bio_text_trgm ON influencers USING gin (bio_text gin_trgm_ops)")

    # D) influencer_platforms
    op.create_table(
        "influencer_platforms",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("tenant_id", sa.UUID(), nullable=False),
        sa.Column("influencer_id", sa.UUID(), sa.ForeignKey("influencers.id"), nullable=False),
        sa.Column("platform", sa.Text(), nullable=False),
        sa.Column("handle", sa.Text(), nullable=False),
        sa.Column("profile_url", sa.Text(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_unique_constraint(
        "uq_influencer_platforms_tenant_platform_handle",
        "influencer_platforms",
        ["tenant_id", "platform", "handle"],
    )

    # E) receipts
    op.create_table(
        "receipts",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("tenant_id", sa.UUID(), nullable=False),
        sa.Column("influencer_id", sa.UUID(), sa.ForeignKey("influencers.id"), nullable=True),
        sa.Column("type", sa.Text(), nullable=True),
        sa.Column("url", sa.Text(), nullable=True),
        sa.Column("excerpt", sa.Text(), nullable=True),
        sa.Column("occurred_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("source_platform", sa.Text(), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("provenance", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_receipts_tenant_id", "receipts", ["tenant_id"])

    # F) target_cards
    op.create_table(
        "target_cards",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("tenant_id", sa.UUID(), nullable=False),
        sa.Column("campaign_id", sa.UUID(), sa.ForeignKey("campaigns.id"), nullable=False),
        sa.Column("influencer_id", sa.UUID(), sa.ForeignKey("influencers.id"), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_unique_constraint(
        "uq_target_cards_tenant_campaign_influencer",
        "target_cards",
        ["tenant_id", "campaign_id", "influencer_id"],
    )

    # G) contact_methods
    op.create_table(
        "contact_methods",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("tenant_id", sa.UUID(), nullable=False),
        sa.Column("influencer_id", sa.UUID(), sa.ForeignKey("influencers.id"), nullable=False),
        sa.Column("method", sa.Text(), nullable=False),
        sa.Column("value", sa.Text(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("verified", sa.Boolean(), nullable=True),
        sa.Column("provenance", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_unique_constraint(
        "uq_contact_methods_tenant_influencer_method_value",
        "contact_methods",
        ["tenant_id", "influencer_id", "method", "value"],
    )

    # H) drafts
    op.create_table(
        "drafts",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("tenant_id", sa.UUID(), nullable=False),
        sa.Column("campaign_id", sa.UUID(), sa.ForeignKey("campaigns.id"), nullable=False),
        sa.Column("influencer_id", sa.UUID(), sa.ForeignKey("influencers.id"), nullable=False),
        sa.Column("channel", sa.Text(), nullable=True),
        sa.Column("subject", sa.Text(), nullable=True),
        sa.Column("body", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_drafts_tenant_id", "drafts", ["tenant_id"])

    # I) pitch_events
    op.create_table(
        "pitch_events",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("tenant_id", sa.UUID(), nullable=False),
        sa.Column("influencer_id", sa.UUID(), sa.ForeignKey("influencers.id"), nullable=False),
        sa.Column("campaign_id", sa.UUID(), sa.ForeignKey("campaigns.id"), nullable=False),
        sa.Column("channel", sa.Text(), nullable=True),
        sa.Column("event_type", sa.Text(), nullable=True),
        sa.Column("occurred_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_pitch_events_tenant_influencer_occurred", "pitch_events", ["tenant_id", "influencer_id", "occurred_at"])

    # J) reservations
    op.create_table(
        "reservations",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("tenant_id", sa.UUID(), nullable=False),
        sa.Column("influencer_id", sa.UUID(), sa.ForeignKey("influencers.id"), nullable=False),
        sa.Column("reserved_until", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_reservations_tenant_influencer", "reservations", ["tenant_id", "influencer_id"])
    op.create_index("ix_reservations_reserved_until", "reservations", ["reserved_until"])


def downgrade() -> None:
    op.drop_table("reservations")
    op.drop_table("pitch_events")
    op.drop_table("drafts")
    op.drop_table("contact_methods")
    op.drop_table("target_cards")
    op.drop_table("receipts")
    op.drop_table("influencer_platforms")
    op.drop_table("influencers")
    op.drop_table("embeddings")
    op.drop_table("campaigns")

    op.execute("DROP EXTENSION IF EXISTS citext")
    op.execute("DROP EXTENSION IF EXISTS pg_trgm")
    op.execute("DROP EXTENSION IF EXISTS vector")
