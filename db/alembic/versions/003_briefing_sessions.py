"""Add briefing_sessions table for Node A.

Revision ID: 003_briefing_sessions
Revises: 002_runs_table
Create Date: 2025-01-29

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "003_briefing_sessions"
down_revision: Union[str, None] = "002_runs_table"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "briefing_sessions",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("tenant_id", sa.UUID(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default="active"),
        sa.Column("slots_json", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("confidences_json", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("messages_json", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("missing_slots", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("run_id", sa.UUID(), sa.ForeignKey("runs.id"), nullable=True),
        sa.Column("campaign_id", sa.UUID(), sa.ForeignKey("campaigns.id"), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            "status IN ('active', 'finalized', 'abandoned')",
            name="ck_briefing_sessions_status",
        ),
    )
    op.create_index("ix_briefing_sessions_tenant_id", "briefing_sessions", ["tenant_id"])
    op.create_index("ix_briefing_sessions_status", "briefing_sessions", ["status"])


def downgrade() -> None:
    op.drop_table("briefing_sessions")
