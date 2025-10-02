"""create weather_res6 table

Revision ID: 2cdce3d061e8
Revises: fde418736eee
Create Date: 2025-10-02 00:45:28.358624

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "2cdce3d061e8"
down_revision: Union[str, Sequence[str], None] = "fde418736eee"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
