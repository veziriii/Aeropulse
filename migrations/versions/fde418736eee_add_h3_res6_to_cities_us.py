"""restore missing stub: add h3_res6 to cities_us (no-op placeholder)"""

from alembic import op
import sqlalchemy as sa

# REQUIRED Alembic identifiers
revision = "fde418736eee"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # NOTE: This is a placeholder to restore the revision chain.
    # If you really want this migration to do the original work,
    # you could put:
    # op.add_column("cities_us", sa.Column("h3_res6", sa.String(length=16), nullable=True))
    # op.create_index("ix_cities_us_h3_res6", "cities_us", ["h3_res6"], unique=False)
    # But since your later fix migration uses IF NOT EXISTS, we keep this no-op.
    pass


def downgrade():
    pass
