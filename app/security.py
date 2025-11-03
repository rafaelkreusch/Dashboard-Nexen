from datetime import datetime, timedelta, timezone
from typing import Optional
import jwt
from fastapi import HTTPException, status
from passlib.context import CryptContext
from app.settings import settings


# Use pbkdf2_sha256 as default to avoid bcrypt 72-byte limit issues on some platforms.
pwd_context = CryptContext(schemes=["pbkdf2_sha256", "bcrypt"], deprecated="auto")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def hash_password(password: str) -> str:
    try:
        return pwd_context.hash(password)
    except Exception:
        # Fallback ultra-safe: shorten if a weird backend raises on long inputs
        return pwd_context.hash(password[:72])


def create_access_token(*, user_id: int, organization_id: int, expires_minutes: int = 60 * 24) -> str:
    now = datetime.now(tz=timezone.utc)
    payload = {
        "sub": str(user_id),
        "org": organization_id,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=expires_minutes)).timestamp()),
    }
    token = jwt.encode(payload, settings.JWT_SECRET, algorithm=settings.JWT_ALG)
    return token


def decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, settings.JWT_SECRET, algorithms=[settings.JWT_ALG])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expirado")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token inválido")
