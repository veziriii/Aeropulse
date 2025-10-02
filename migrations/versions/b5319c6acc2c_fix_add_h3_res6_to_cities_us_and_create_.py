"""fix: add h3_res6 to cities_us and create weather_res6 (idempotent)"""

from alembic import op
import sqlalchemy as sa

# === REQUIRED Alembic identifiers ===
revision = "b5319c6acc2c"  # must match a unique id; using the filename prefix is fine
down_revision = (
    "2cdce3d061e8"  # <- your current head before this fix (you mentioned this earlier)
)
branch_labels = None
depends_on = None

SCHEMA = "public"


def upgrade():
    op.execute(
        f"ALTER TABLE {SCHEMA}.cities_us ADD COLUMN IF NOT EXISTS h3_res6 VARCHAR(16);"
    )
    op.execute(
        f"CREATE INDEX IF NOT EXISTS ix_cities_us_h3_res6 ON {SCHEMA}.cities_us (h3_res6);"
    )
    op.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.weather_res6 (
            h3_res6 VARCHAR(16) PRIMARY KEY,
            last_updated TIMESTAMP NULL,
            weather JSONB NULL
        );
    """
    )
    op.execute(
        f"CREATE UNIQUE INDEX IF NOT EXISTS ix_weather_res6_h3 ON {SCHEMA}.weather_res6 (h3_res6);"
    )


def downgrade():
    # Optional rollback
    op.execute(f"DROP INDEX IF EXISTS {SCHEMA}.ix_weather_res6_h3;")
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.weather_res6;")
    op.execute(f"DROP INDEX IF EXISTS {SCHEMA}.ix_cities_us_h3_res6;")
    op.execute(f"ALTER TABLE {SCHEMA}.cities_us DROP COLUMN IF EXISTS h3_res6;")
