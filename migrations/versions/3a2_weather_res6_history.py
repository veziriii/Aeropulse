"""create weather_res6_history

Revision ID: 3a2_weather_res6_history
Revises: 3a1_open_sky_states
Create Date: 2025-10-03 00:00:01
"""

from alembic import op
import sqlalchemy as sa

revision = "3a2_weather_res6_history"
down_revision = "3a1_open_sky_states"
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        """
    CREATE TABLE IF NOT EXISTS public.weather_res6_history (
        id BIGSERIAL PRIMARY KEY,
        h3_res6 TEXT NOT NULL,
        fetched_at TIMESTAMPTZ NOT NULL,
        weather JSONB NOT NULL
    );
    CREATE INDEX IF NOT EXISTS ix_weather_hist_cell_time ON public.weather_res6_history(h3_res6, fetched_at DESC);
    """
    )


def downgrade():
    op.execute("DROP TABLE IF EXISTS public.weather_res6_history;")
