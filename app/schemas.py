from datetime import datetime, date
from typing import Optional, Any
from pydantic import BaseModel, Field


class TokenOut(BaseModel):
    access_token: str
    token_type: str = "bearer"


class CurrentUserOut(BaseModel):
    id: int
    email: str
    name: Optional[str] = None
    role: Optional[str] = None
    organization_id: int
    organization_slug: str


class IndicatorFolderPermissionOut(BaseModel):
    folder: str
    can_edit: bool = False


class MemberOut(BaseModel):
    id: int
    email: str
    name: Optional[str] = None
    role: str
    can_manage_datasources: bool = False
    can_manage_datasets: bool = False
    can_manage_indicators: bool = False
    can_manage_members: bool = False
    indicator_folders: list[IndicatorFolderPermissionOut] = Field(default_factory=list)


class MemberInviteIn(BaseModel):
    email: str
    name: Optional[str] = None
    password: Optional[str] = None
    role: str = "Viewer"
    can_manage_datasources: bool = False
    can_manage_datasets: bool = False
    can_manage_indicators: bool = False
    can_manage_members: bool = False
    indicator_folders: list[IndicatorFolderPermissionOut] = Field(default_factory=list)


class MemberUpdateIn(BaseModel):
    name: Optional[str] = None
    password: Optional[str] = None
    role: Optional[str] = None
    can_manage_datasources: Optional[bool] = None
    can_manage_datasets: Optional[bool] = None
    can_manage_indicators: Optional[bool] = None
    can_manage_members: Optional[bool] = None
    indicator_folders: Optional[list[IndicatorFolderPermissionOut]] = None


class DevLoginIn(BaseModel):
    email: str
    name: str = "Dev User"
    org_slug: str = "demo"
    org_name: str = "Demo Org"


class RegisterIn(BaseModel):
    email: str
    name: str
    password: str
    org_slug: str
    org_name: str


class LoginIn(BaseModel):
    email: str
    password: str
    org_slug: str | None = None


class DataSourceTestIn(BaseModel):
    sqlalchemy_url: str


class DataSourceCreateIn(BaseModel):
    type: str
    sqlalchemy_url: Optional[str] = None
    config_json: Optional[dict] = None
    is_recurring: Optional[bool] = False
    interval_minutes: Optional[int] = None


class DataSourceOut(BaseModel):
    id: int
    type: str
    sqlalchemy_url: Optional[str]
    config_json: Optional[dict]
    is_recurring: bool
    interval_minutes: Optional[int]

    class Config:
        from_attributes = True


class IngestSQLIn(BaseModel):
    data_source_id: int
    query: str


class SheetsIn(BaseModel):
    spreadsheet_id: str
    range: str


class IndicatorQuery(BaseModel):
    from_: Optional[date] = Field(default=None, alias="from")
    to: Optional[date] = None
    uf: Optional[str] = None
    situacao_processo: Optional[str] = None


class DashboardCreateIn(BaseModel):
    name: str
    description: Optional[str] = None
    layout_json: Optional[dict] = None
    is_public: bool = False
    theme_json: Optional[dict] = None


class DashboardOut(BaseModel):
    id: int
    name: str
    description: Optional[str]
    layout_json: Optional[dict]
    is_public: bool
    public_token: Optional[str]
    theme_json: Optional[dict]

    class Config:
        from_attributes = True
