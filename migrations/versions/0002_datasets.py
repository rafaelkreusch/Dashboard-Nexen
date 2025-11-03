from __future__ import annotations
from alembic import op
import sqlalchemy as sa

revision = '0002_datasets'
down_revision = '0001_init'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'datasets',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('organization_id', sa.Integer, nullable=False, index=True),
        sa.Column('name', sa.String(200), nullable=False),
        sa.Column('description', sa.Text()),
        sa.Column('query_sql', sa.Text(), nullable=False),
        sa.Column('options_json', sa.JSON()),
        sa.Column('created_at', sa.DateTime, server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    )
    op.create_index('ix_datasets_org', 'datasets', ['organization_id'], unique=False)


def downgrade() -> None:
    op.drop_index('ix_datasets_org', table_name='datasets')
    op.drop_table('datasets')

