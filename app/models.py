from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean, Text, JSON, Float
from sqlalchemy.orm import relationship, Mapped, mapped_column
from app.database import Base


class Organization(Base):
    __tablename__ = 'organizations'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    slug: Mapped[str] = mapped_column(String(100), unique=True, index=True, nullable=False)
    plan: Mapped[str] = mapped_column(String(50), default='free')
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class User(Base):
    __tablename__ = 'users'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    email: Mapped[str] = mapped_column(String(200), unique=True, index=True, nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    password_hash: Mapped[str] = mapped_column(String(200), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Membership(Base):
    __tablename__ = 'memberships'

    user_id: Mapped[int] = mapped_column(Integer, ForeignKey('users.id'), primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, ForeignKey('organizations.id'), primary_key=True)
    role: Mapped[str] = mapped_column(String(20), default='Viewer')


class DataSource(Base):
    __tablename__ = 'data_sources'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    type: Mapped[str] = mapped_column(String(30), nullable=False)  # sql, csv_upload, google_sheets
    sqlalchemy_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    config_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    is_recurring: Mapped[bool] = mapped_column(Boolean, default=False)
    interval_minutes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class StagingRecord(Base):
    __tablename__ = 'staging_records'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    raw_json: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class CuratedRecord(Base):
    __tablename__ = 'curated_records'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    dt_cadastro: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    uf: Mapped[str | None] = mapped_column(String(4), nullable=True, index=True)
    faixa_vencimento: Mapped[str | None] = mapped_column(String(100), nullable=True, index=True)
    dt_vencimento: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    vl_titulo: Mapped[float | None] = mapped_column(Float, nullable=True)
    situacao_processo: Mapped[str | None] = mapped_column(String(100), nullable=True)
    vl_total_repasse: Mapped[float | None] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Indicator(Base):
    __tablename__ = 'indicators'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    key: Mapped[str] = mapped_column(String(100), index=True)
    name: Mapped[str] = mapped_column(String(200))
    dataset: Mapped[str | None] = mapped_column(String(100))
    formula_sql: Mapped[str | None] = mapped_column(Text)
    default_filters_json: Mapped[dict | None] = mapped_column(JSON)
    fmt: Mapped[str | None] = mapped_column(String(50))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Dashboard(Base):
    __tablename__ = 'dashboards'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    name: Mapped[str] = mapped_column(String(200))
    description: Mapped[str | None] = mapped_column(Text)
    layout_json: Mapped[dict | None] = mapped_column(JSON)
    is_public: Mapped[bool] = mapped_column(Boolean, default=False)
    public_token: Mapped[str | None] = mapped_column(String(200))
    theme_json: Mapped[dict | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Chart(Base):
    __tablename__ = 'charts'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    dashboard_id: Mapped[int] = mapped_column(Integer, ForeignKey('dashboards.id'))
    type: Mapped[str] = mapped_column(String(30))  # bar, line, map, table, kpi
    query_sql: Mapped[str] = mapped_column(Text)
    options_json: Mapped[dict | None] = mapped_column(JSON)
    position: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class JobRun(Base):
    __tablename__ = 'job_runs'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    target_type: Mapped[str] = mapped_column(String(50))  # datasource|indicator|etc
    target_id: Mapped[int | None] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(20))  # success|error|running
    started_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    logs: Mapped[str | None] = mapped_column(Text)
