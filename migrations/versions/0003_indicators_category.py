from __future__ import annotations
from alembic import op
import sqlalchemy as sa

revision = '0003_indicators_category'
down_revision = '0002_datasets'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('indicators', sa.Column('category', sa.String(120), nullable=True))
    # optional index for category
    try:
        op.create_index('ix_indicators_category', 'indicators', ['category'], unique=False)
    except Exception:
        pass


def downgrade() -> None:
    try:
        op.drop_index('ix_indicators_category', table_name='indicators')
    except Exception:
        pass
    op.drop_column('indicators', 'category')

