from __future__ import annotations
from alembic import op
import sqlalchemy as sa
from datetime import datetime

# revision identifiers, used by Alembic.
revision = '0001_init'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'organizations',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('name', sa.String(200), nullable=False),
        sa.Column('slug', sa.String(100), nullable=False, unique=True),
        sa.Column('plan', sa.String(50), nullable=False, server_default='free'),
        sa.Column('created_at', sa.DateTime, nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
    )
    op.create_index('ix_organizations_slug', 'organizations', ['slug'], unique=True)

    op.create_table(
        'users',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('email', sa.String(200), nullable=False, unique=True),
        sa.Column('name', sa.String(200), nullable=False),
        sa.Column('password_hash', sa.String(200), nullable=False),
        sa.Column('created_at', sa.DateTime, nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
    )
    op.create_index('ix_users_email', 'users', ['email'], unique=True)

    op.create_table(
        'memberships',
        sa.Column('user_id', sa.Integer, sa.ForeignKey('users.id'), primary_key=True),
        sa.Column('organization_id', sa.Integer, sa.ForeignKey('organizations.id'), primary_key=True),
        sa.Column('role', sa.String(20), nullable=False, server_default='Viewer'),
    )

    op.create_table(
        'data_sources',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('organization_id', sa.Integer, nullable=False, index=True),
        sa.Column('type', sa.String(30), nullable=False),
        sa.Column('sqlalchemy_url', sa.String(500)),
        sa.Column('config_json', sa.JSON()),
        sa.Column('is_recurring', sa.Boolean, nullable=False, server_default=sa.text('0')),
        sa.Column('interval_minutes', sa.Integer),
        sa.Column('created_at', sa.DateTime, nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
    )
    op.create_index('ix_data_sources_org', 'data_sources', ['organization_id'], unique=False)

    op.create_table(
        'staging_records',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('organization_id', sa.Integer, nullable=False, index=True),
        sa.Column('raw_json', sa.JSON(), nullable=False),
        sa.Column('created_at', sa.DateTime, nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
    )
    op.create_index('ix_staging_records_org', 'staging_records', ['organization_id'], unique=False)

    op.create_table(
        'curated_records',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('organization_id', sa.Integer, nullable=False, index=True),
        sa.Column('dt_cadastro', sa.DateTime),
        sa.Column('uf', sa.String(4)),
        sa.Column('faixa_vencimento', sa.String(100)),
        sa.Column('dt_vencimento', sa.DateTime),
        sa.Column('vl_titulo', sa.Float),
        sa.Column('situacao_processo', sa.String(100)),
        sa.Column('vl_total_repasse', sa.Float),
        sa.Column('created_at', sa.DateTime, nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
    )
    op.create_index('ix_curated_records_org', 'curated_records', ['organization_id'], unique=False)

    op.create_table(
        'indicators',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('organization_id', sa.Integer, nullable=False, index=True),
        sa.Column('key', sa.String(100), index=True),
        sa.Column('name', sa.String(200)),
        sa.Column('dataset', sa.String(100)),
        sa.Column('formula_sql', sa.Text()),
        sa.Column('default_filters_json', sa.JSON()),
        sa.Column('fmt', sa.String(50)),
        sa.Column('created_at', sa.DateTime, nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
    )
    op.create_index('ix_indicators_org', 'indicators', ['organization_id'], unique=False)

    op.create_table(
        'dashboards',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('organization_id', sa.Integer, nullable=False, index=True),
        sa.Column('name', sa.String(200)),
        sa.Column('description', sa.Text()),
        sa.Column('layout_json', sa.JSON()),
        sa.Column('is_public', sa.Boolean, nullable=False, server_default=sa.text('0')),
        sa.Column('public_token', sa.String(200)),
        sa.Column('theme_json', sa.JSON()),
        sa.Column('created_at', sa.DateTime, nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
    )
    op.create_index('ix_dashboards_org', 'dashboards', ['organization_id'], unique=False)

    op.create_table(
        'charts',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('organization_id', sa.Integer, nullable=False, index=True),
        sa.Column('dashboard_id', sa.Integer, sa.ForeignKey('dashboards.id')),
        sa.Column('type', sa.String(30)),
        sa.Column('query_sql', sa.Text()),
        sa.Column('options_json', sa.JSON()),
        sa.Column('position', sa.Integer),
        sa.Column('created_at', sa.DateTime, nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
    )
    op.create_index('ix_charts_org', 'charts', ['organization_id'], unique=False)

    op.create_table(
        'job_runs',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('organization_id', sa.Integer, nullable=False, index=True),
        sa.Column('target_type', sa.String(50)),
        sa.Column('target_id', sa.Integer),
        sa.Column('status', sa.String(20)),
        sa.Column('started_at', sa.DateTime, nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('finished_at', sa.DateTime),
        sa.Column('logs', sa.Text()),
    )
    op.create_index('ix_job_runs_org', 'job_runs', ['organization_id'], unique=False)

    # Seed demo dashboard and some curated data
    conn = op.get_bind()
    # Create demo org if not exists
    res = conn.execute(sa.text("SELECT id FROM organizations WHERE slug = :slug"), {"slug": "demo"}).fetchone()
    if res is None:
        conn.execute(sa.text("INSERT INTO organizations (name, slug, plan) VALUES (:n, :s, :p)"), {"n": "Demo Org", "s": "demo", "p": "dev"})
        org_id = conn.execute(sa.text("SELECT id FROM organizations WHERE slug='demo'"))
        org_id = org_id.scalar_one()
    else:
        org_id = res[0]

    now = datetime.utcnow()
    # Insert sample curated rows
    samples = [
        {"organization_id": org_id, "uf": "SP", "faixa_vencimento": "0-30", "dt_vencimento": now, "vl_titulo": 1200.5, "situacao_processo": "aberto", "vl_total_repasse": 200.0, "dt_cadastro": now},
        {"organization_id": org_id, "uf": "RJ", "faixa_vencimento": "31-60", "dt_vencimento": now, "vl_titulo": 800.0, "situacao_processo": "pago", "vl_total_repasse": 800.0, "dt_cadastro": now},
        {"organization_id": org_id, "uf": "MG", "faixa_vencimento": "61-90", "dt_vencimento": now, "vl_titulo": 500.0, "situacao_processo": "aberto", "vl_total_repasse": 0.0, "dt_cadastro": now},
        {"organization_id": org_id, "uf": "SP", "faixa_vencimento": ">90", "dt_vencimento": now, "vl_titulo": 300.0, "situacao_processo": "aberto", "vl_total_repasse": 100.0, "dt_cadastro": now},
    ]
    for s in samples:
        conn.execute(sa.text(
            """
            INSERT INTO curated_records (organization_id, dt_cadastro, uf, faixa_vencimento, dt_vencimento, vl_titulo, situacao_processo, vl_total_repasse, created_at)
            VALUES (:organization_id, :dt_cadastro, :uf, :faixa_vencimento, :dt_vencimento, :vl_titulo, :situacao_processo, :vl_total_repasse, :created_at)
            """
        ), {**s, "created_at": now})

    # Insert demo dashboard
    conn.execute(sa.text(
        """
        INSERT INTO dashboards (organization_id, name, description, layout_json, is_public, theme_json)
        VALUES (:org, :name, :desc, '{}', :pub, '{}')
        """
    ), {
        "org": org_id,
        "name": "Cobrança B2B – Exemplo",
        "desc": "Dashboard de exemplo com KPIs",
        "pub": False,
    })


def downgrade() -> None:
    op.drop_index('ix_job_runs_org', table_name='job_runs')
    op.drop_table('job_runs')
    op.drop_index('ix_charts_org', table_name='charts')
    op.drop_table('charts')
    op.drop_index('ix_dashboards_org', table_name='dashboards')
    op.drop_table('dashboards')
    op.drop_index('ix_indicators_org', table_name='indicators')
    op.drop_table('indicators')
    op.drop_index('ix_curated_records_org', table_name='curated_records')
    op.drop_table('curated_records')
    op.drop_index('ix_staging_records_org', table_name='staging_records')
    op.drop_table('staging_records')
    op.drop_index('ix_data_sources_org', table_name='data_sources')
    op.drop_table('data_sources')
    op.drop_table('memberships')
    op.drop_index('ix_users_email', table_name='users')
    op.drop_table('users')
    op.drop_index('ix_organizations_slug', table_name='organizations')
    op.drop_table('organizations')
