from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select, delete, insert
from sqlalchemy.orm import Session

from app.deps import get_current_ctx, DbSession
from app.models import Membership, User, Organization, IndicatorFolderPermission, Indicator
from app.schemas import MemberOut, MemberInviteIn, MemberUpdateIn, IndicatorFolderPermissionOut
from app.security import hash_password


router = APIRouter(prefix="/org", tags=["organization"])


def get_membership_record(db: Session, org_id: int, user_id: int) -> Membership | None:
    return db.scalar(
        select(Membership).where(
            Membership.organization_id == org_id,
            Membership.user_id == user_id,
        )
    )


def ensure_can_manage_members(db: Session, org_id: int, user_id: int) -> Membership:
    m = get_membership_record(db, org_id, user_id)
    if not m:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Permissão negada")
    role = (m.role or '').lower()
    if role in ('owner', 'admin') or m.can_manage_members:
        return m
    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Permissão negada")


@router.get('/members', response_model=list[MemberOut])
def list_members(db: DbSession, ctx=Depends(get_current_ctx)):
    org = db.scalar(select(Organization).where(Organization.id == ctx.organization_id))
    if not org:
        raise HTTPException(status_code=404, detail="Organização não encontrada")
    rows = db.execute(
        select(
            User.id,
            User.email,
            User.name,
            Membership.role,
            Membership.can_manage_datasources,
            Membership.can_manage_datasets,
            Membership.can_manage_indicators,
            Membership.can_manage_members,
        )
        .join(Membership, Membership.user_id == User.id)
        .where(Membership.organization_id == ctx.organization_id)
        .order_by(User.name)
    ).all()

    folder_rows = db.execute(
        select(
            IndicatorFolderPermission.user_id,
            IndicatorFolderPermission.folder,
            IndicatorFolderPermission.can_edit,
        ).where(
            IndicatorFolderPermission.organization_id == ctx.organization_id
        )
    ).all()

    folders_by_user: dict[int, list[IndicatorFolderPermissionOut]] = {}
    for fr in folder_rows:
        folders_by_user.setdefault(fr.user_id, []).append(
            IndicatorFolderPermissionOut(folder=fr.folder, can_edit=bool(fr.can_edit))
        )

    result: list[MemberOut] = []
    for row in rows:
        result.append(
            MemberOut(
                id=row.id,
                email=row.email,
                name=row.name,
                role=row.role,
                can_manage_datasources=bool(row.can_manage_datasources),
                can_manage_datasets=bool(row.can_manage_datasets),
                can_manage_indicators=bool(row.can_manage_indicators),
                can_manage_members=bool(row.can_manage_members),
                indicator_folders=folders_by_user.get(row.id, []),
            )
        )
    return result


def _normalize_folder_name(folder: str | None) -> str:
    if folder is None:
        return ""
    return folder.strip()


def _apply_indicator_folders(db: Session, org_id: int, user_id: int, folders: list[IndicatorFolderPermissionOut]):
    db.execute(
        delete(IndicatorFolderPermission).where(
            IndicatorFolderPermission.organization_id == org_id,
            IndicatorFolderPermission.user_id == user_id,
        )
    )
    to_insert = []
    seen: set[tuple[str, bool]] = set()
    for f in folders:
        name = _normalize_folder_name(f.folder)
        key = (name, bool(f.can_edit))
        if not name and name != "":
            continue
        if key in seen:
            continue
        seen.add(key)
        to_insert.append(
            dict(
                organization_id=org_id,
                user_id=user_id,
                folder=name,
                can_edit=bool(f.can_edit),
            )
        )
    if to_insert:
        db.execute(insert(IndicatorFolderPermission), to_insert)


@router.post('/members', response_model=MemberOut)
def add_member(payload: MemberInviteIn, db: DbSession, ctx=Depends(get_current_ctx)):
    ensure_can_manage_members(db, ctx.organization_id, ctx.user_id)
    user = db.scalar(select(User).where(User.email == payload.email))
    if not user:
        if not payload.password or not payload.name:
            raise HTTPException(status_code=400, detail="Nome e senha são obrigatórios para novo usuário")
        user = User(
            email=payload.email,
            name=payload.name,
            password_hash=hash_password(payload.password),
        )
        db.add(user)
        db.flush()
    else:
        # update name if provided
        if payload.name and payload.name.strip() and payload.name != user.name:
            user.name = payload.name

    membership = db.scalar(
        select(Membership).where(
            Membership.user_id == user.id,
            Membership.organization_id == ctx.organization_id,
        )
    )

    fields = dict(
        role=payload.role,
        can_manage_datasources=payload.can_manage_datasources,
        can_manage_datasets=payload.can_manage_datasets,
        can_manage_indicators=payload.can_manage_indicators,
        can_manage_members=payload.can_manage_members,
    )

    if membership:
        for key, value in fields.items():
            setattr(membership, key, value)
    else:
        membership = Membership(
            user_id=user.id,
            organization_id=ctx.organization_id,
            **fields,
        )
        db.add(membership)

    _apply_indicator_folders(db, ctx.organization_id, user.id, payload.indicator_folders)
    db.commit()
    folders = [
        IndicatorFolderPermissionOut(folder=perm.folder, can_edit=perm.can_edit)
        for perm in db.execute(
            select(IndicatorFolderPermission).where(
                IndicatorFolderPermission.organization_id == ctx.organization_id,
                IndicatorFolderPermission.user_id == user.id,
            )
        ).scalars()
    ]
    return MemberOut(
        id=user.id,
        email=user.email,
        name=user.name,
        role=membership.role,
        can_manage_datasources=membership.can_manage_datasources,
        can_manage_datasets=membership.can_manage_datasets,
        can_manage_indicators=membership.can_manage_indicators,
        can_manage_members=membership.can_manage_members,
        indicator_folders=folders,
    )


@router.put('/members/{user_id}', response_model=MemberOut)
def update_member(user_id: int, payload: MemberUpdateIn, db: DbSession, ctx=Depends(get_current_ctx)):
    ensure_can_manage_members(db, ctx.organization_id, ctx.user_id)
    membership = db.scalar(
        select(Membership).where(
            Membership.user_id == user_id,
            Membership.organization_id == ctx.organization_id,
        )
    )
    if not membership:
        raise HTTPException(status_code=404, detail="Membro não encontrado")

    if payload.role is not None:
        membership.role = payload.role
    if payload.can_manage_datasources is not None:
        membership.can_manage_datasources = payload.can_manage_datasources
    if payload.can_manage_datasets is not None:
        membership.can_manage_datasets = payload.can_manage_datasets
    if payload.can_manage_indicators is not None:
        membership.can_manage_indicators = payload.can_manage_indicators
    if payload.can_manage_members is not None:
        membership.can_manage_members = payload.can_manage_members

    if payload.indicator_folders is not None:
        _apply_indicator_folders(db, ctx.organization_id, user_id, payload.indicator_folders)

    user = db.get(User, user_id)
    if payload.name and user:
        user.name = payload.name
    if payload.password:
        if not user:
            raise HTTPException(status_code=404, detail="Usuário não encontrado")
        user.password_hash = hash_password(payload.password)

    db.commit()
    if user:
        db.refresh(user)
    folders = [
        IndicatorFolderPermissionOut(folder=perm.folder, can_edit=perm.can_edit)
        for perm in db.execute(
            select(IndicatorFolderPermission).where(
                IndicatorFolderPermission.organization_id == ctx.organization_id,
                IndicatorFolderPermission.user_id == user_id,
            )
        ).scalars()
    ]
    return MemberOut(
        id=user.id if user else user_id,
        email=user.email if user else "",
        name=user.name if user else "",
        role=membership.role,
        can_manage_datasources=membership.can_manage_datasources,
        can_manage_datasets=membership.can_manage_datasets,
        can_manage_indicators=membership.can_manage_indicators,
        can_manage_members=membership.can_manage_members,
        indicator_folders=folders,
    )


@router.delete('/members/{user_id}')
def remove_member(user_id: int, db: DbSession, ctx=Depends(get_current_ctx)):
    ensure_can_manage_members(db, ctx.organization_id, ctx.user_id)
    db.execute(delete(Membership).where(Membership.user_id == user_id, Membership.organization_id == ctx.organization_id))
    db.execute(
        delete(IndicatorFolderPermission).where(
            IndicatorFolderPermission.user_id == user_id,
            IndicatorFolderPermission.organization_id == ctx.organization_id,
        )
    )
    db.commit()
    return {"ok": True}


@router.get('/info')
def org_info(db: DbSession, ctx=Depends(get_current_ctx)):
    org = db.scalar(select(Organization).where(Organization.id == ctx.organization_id))
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")
    m = db.scalar(select(Membership).where(Membership.organization_id == ctx.organization_id, Membership.user_id == ctx.user_id))
    role = (m.role if m else None) or 'Viewer'
    can_manage_members = bool(m.can_manage_members) if m else False
    if m and (m.role or '').lower() in ('owner', 'admin'):
        can_manage_members = True
    return {
        "id": org.id,
        "name": org.name,
        "slug": org.slug,
        "role": role,
        "can_manage_members": can_manage_members,
    }


@router.get('/indicator-folders')
def list_indicator_folders(db: DbSession, ctx=Depends(get_current_ctx)):
    rows = db.execute(
        select(Indicator.category)
        .where(Indicator.organization_id == ctx.organization_id)
        .distinct()
        .order_by(Indicator.category)
    ).all()
    folders = [row.category for row in rows if row.category]
    return {"folders": folders}
