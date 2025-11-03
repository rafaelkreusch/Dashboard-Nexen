from typing import Annotated
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session

from app.database import get_db
from app.security import decode_token


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/dev-login")


class RequestContext:
    def __init__(self, user_id: int, organization_id: int):
        self.user_id = user_id
        self.organization_id = organization_id


def get_current_ctx(token: Annotated[str, Depends(oauth2_scheme)]) -> RequestContext:
    payload = decode_token(token)
    user_id = int(payload.get("sub"))
    org_id = int(payload.get("org"))
    if not user_id or not org_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token inválido")
    return RequestContext(user_id=user_id, organization_id=org_id)


DbSession = Annotated[Session, Depends(get_db)]

