from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import select

from app.database import get_db
from app.deps import get_current_ctx, DbSession
from app.models import Organization, User, Membership
from app.schemas import DevLoginIn, TokenOut, RegisterIn, LoginIn, CurrentUserOut
from app.security import create_access_token, hash_password, verify_password


router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/dev-login", response_model=TokenOut)
def dev_login(payload: DevLoginIn, db: Session = Depends(get_db)):
    # Create organization if not exists
    org = db.scalar(select(Organization).where(Organization.slug == payload.org_slug))
    if not org:
        org = Organization(name=payload.org_name, slug=payload.org_slug, plan='dev')
        db.add(org)
        db.flush()

    # Create user if not exists
    user = db.scalar(select(User).where(User.email == payload.email))
    if not user:
        user = User(email=payload.email, name=payload.name, password_hash=hash_password("dev"))
        db.add(user)
        db.flush()

    # Ensure membership
    member = db.scalar(select(Membership).where(Membership.user_id == user.id, Membership.organization_id == org.id))
    if not member:
        db.add(Membership(user_id=user.id, organization_id=org.id, role='Owner'))

    db.commit()

    token = create_access_token(user_id=user.id, organization_id=org.id)
    return TokenOut(access_token=token)


@router.post("/register", response_model=TokenOut)
def register(payload: RegisterIn, db: Session = Depends(get_db)):
    # Check existing user/org
    user = db.scalar(select(User).where(User.email == payload.email))
    if user:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email já cadastrado")

    org = db.scalar(select(Organization).where(Organization.slug == payload.org_slug))
    if org:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Slug de organização já em uso")

    org = Organization(name=payload.org_name, slug=payload.org_slug, plan='free')
    db.add(org)
    db.flush()

    user = User(email=payload.email, name=payload.name, password_hash=hash_password(payload.password))
    db.add(user)
    db.flush()

    db.add(Membership(user_id=user.id, organization_id=org.id, role='Owner'))
    db.commit()

    token = create_access_token(user_id=user.id, organization_id=org.id)
    return TokenOut(access_token=token)


@router.post("/login", response_model=TokenOut)
def login(payload: LoginIn, db: Session = Depends(get_db)):
    user = db.scalar(select(User).where(User.email == payload.email))
    if not user or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Credenciais inválidas")

    # Resolve org: by slug if provided else first membership
    org = None
    if payload.org_slug:
        org = db.scalar(select(Organization).where(Organization.slug == payload.org_slug))
    else:
        m = db.scalar(select(Membership).where(Membership.user_id == user.id))
        if m:
            org = db.scalar(select(Organization).where(Organization.id == m.organization_id))
    if not org:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Organização não encontrada para este usuário")

    # Ensure membership exists
    member = db.scalar(select(Membership).where(Membership.user_id == user.id, Membership.organization_id == org.id))
    if not member:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Sem acesso à organização")

    token = create_access_token(user_id=user.id, organization_id=org.id)
    return TokenOut(access_token=token)


@router.get("/me", response_model=CurrentUserOut)
def current_user(db: DbSession, ctx=Depends(get_current_ctx)):
    user = db.get(User, ctx.user_id)
    org = db.get(Organization, ctx.organization_id)
    if not user or not org:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Usuário ou organização não encontrados")
    membership = db.scalar(
        select(Membership.role).where(
            Membership.user_id == user.id,
            Membership.organization_id == org.id,
        )
    )
    role = membership or None
    return CurrentUserOut(
        id=user.id,
        email=user.email,
        name=user.name,
        role=role,
        organization_id=org.id,
        organization_slug=org.slug,
    )
