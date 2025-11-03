from __future__ import annotations
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = '0004_membership_permissions'
down_revision = '0003_indicators_category'
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    def ensure_column(table: str, column: sa.Column):
        if column.name not in [col['name'] for col in inspector.get_columns(table)]:
            op.add_column(table, column)

    ensure_column('memberships', sa.Column('can_manage_datasources', sa.Boolean(), server_default=sa.false(), nullable=False))
    ensure_column('memberships', sa.Column('can_manage_datasets', sa.Boolean(), server_default=sa.false(), nullable=False))
    ensure_column('memberships', sa.Column('can_manage_indicators', sa.Boolean(), server_default=sa.false(), nullable=False))
    ensure_column('memberships', sa.Column('can_manage_members', sa.Boolean(), server_default=sa.false(), nullable=False))

    try:
        op.alter_column('memberships', 'can_manage_datasources', server_default=None)
        op.alter_column('memberships', 'can_manage_datasets', server_default=None)
        op.alter_column('memberships', 'can_manage_indicators', server_default=None)
        op.alter_column('memberships', 'can_manage_members', server_default=None)
    except Exception:
        pass

    if 'indicator_folder_permissions' not in inspector.get_table_names():
        op.create_table(
            'indicator_folder_permissions',
            sa.Column('id', sa.Integer(), primary_key=True),
            sa.Column('organization_id', sa.Integer(), nullable=False),
            sa.Column('user_id', sa.Integer(), nullable=False),
            sa.Column('folder', sa.String(length=120), nullable=False),
            sa.Column('can_edit', sa.Boolean(), nullable=False, server_default=sa.false()),
            sa.Column('created_at', sa.DateTime(), server_default=sa.func.now()),
        )
        try:
            op.create_index('ix_indicator_folder_perm_org_user', 'indicator_folder_permissions', ['organization_id', 'user_id'], unique=False)
        except Exception:
            pass


def downgrade() -> None:
    try:
        op.drop_index('ix_indicator_folder_perm_org_user', table_name='indicator_folder_permissions')
    except Exception:
        pass
    op.drop_table('indicator_folder_permissions')
    op.drop_column('memberships', 'can_manage_members')
    op.drop_column('memberships', 'can_manage_indicators')
    op.drop_column('memberships', 'can_manage_datasets')
    op.drop_column('memberships', 'can_manage_datasources')
