"""create opensky_states

Revision ID: 3a1_open_sky_states
Revises: 2cdce3d061e8
Create Date: 2025-10-03 00:00:00
"""

from alembic import op
import sqlalchemy as sa

revision = "3a1_open_sky_states"
down_revision = "2cdce3d061e8"
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        """
    CREATE TABLE IF NOT EXISTS public.opensky_states (
        id BIGSERIAL PRIMARY KEY,
        ts TIMESTAMPTZ NOT NULL,
        icao24 TEXT NOT NULL,
        callsign TEXT,
        lat DOUBLE PRECISION,
        lon DOUBLE PRECISION,
        h3_res6 TEXT,
        on_ground BOOLEAN,
        velocity DOUBLE PRECISION,
        heading DOUBLE PRECISION,
        vert_rate DOUBLE PRECISION,
        geo_altitude DOUBLE PRECISION,
        baro_altitude DOUBLE PRECISION
    );
    CREATE INDEX IF NOT EXISTS ix_opensky_states_ts ON public.opensky_states(ts DESC);
    CREATE INDEX IF NOT EXISTS ix_opensky_states_icao24_ts ON public.opensky_states(icao24, ts DESC);
    CREATE INDEX IF NOT EXISTS ix_opensky_states_h3_ts ON public.opensky_states(h3_res6, ts DESC);
    """
    )


def downgrade():
    op.execute("DROP TABLE IF EXISTS public.opensky_states;")
