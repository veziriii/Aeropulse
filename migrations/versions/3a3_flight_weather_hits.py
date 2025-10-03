"""create flight_weather_hits

Revision ID: 3a3_flight_weather_hits
Revises: 3a2_weather_res6_history
Create Date: 2025-10-03 00:00:02
"""

from alembic import op
import sqlalchemy as sa

revision = "3a3_flight_weather_hits"
down_revision = "3a2_weather_res6_history"
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        """
    CREATE TABLE IF NOT EXISTS public.flight_weather_hits (
        id BIGSERIAL PRIMARY KEY,
        ts_state TIMESTAMPTZ NOT NULL,
        icao24 TEXT NOT NULL,
        callsign TEXT,
        h3_res6 TEXT NOT NULL,
        weather_at TIMESTAMPTZ NOT NULL,
        weather JSONB NOT NULL,
        weather_summary TEXT
    );
    CREATE INDEX IF NOT EXISTS ix_fwh_icao24_ts ON public.flight_weather_hits(icao24, ts_state DESC);
    CREATE INDEX IF NOT EXISTS ix_fwh_h3_ts ON public.flight_weather_hits(h3_res6, ts_state DESC);
    """
    )


def downgrade():
    op.execute("DROP TABLE IF EXISTS public.flight_weather_hits;")
