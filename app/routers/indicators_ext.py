from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from pydantic import BaseModel, field_validator

from app.deps import get_current_ctx, DbSession


router = APIRouter(prefix="/indicators", tags=["indicators"])  # endpoints extras para categorias/pastas


class CategoryCreate(BaseModel):
    name: str
    color: str | None = None

    @field_validator('name')
    @classmethod
    def strip_and_validate(cls, value: str) -> str:
        value = (value or '').strip()
        if not value:
            raise ValueError('Nome da pasta e obrigatorio')
        return value

    @field_validator('color')
    @classmethod
    def validate_color(cls, value: str | None) -> str | None:
        if not value:
            return None
        value = value.strip()
        if not value.startswith('#') or len(value) != 7:
            raise ValueError('Cor deve estar no formato #RRGGBB')
        hex_digits = set('0123456789abcdefABCDEF')
        for ch in value[1:]:
            if ch not in hex_digits:
                raise ValueError('Cor deve estar no formato #RRGGBB')
        return value


class CategoryUpdate(BaseModel):
    name: str | None = None
    color: str | None = None

    @field_validator('name')
    @classmethod
    def validate_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        trimmed = value.strip()
        if not trimmed:
            raise ValueError('Nome nao pode ser vazio')
        return trimmed

    @field_validator('color')
    @classmethod
    def validate_color(cls, value: str | None) -> str | None:
        if not value:
            return None
        value = value.strip()
        if not value.startswith('#') or len(value) != 7:
            raise ValueError('Cor deve estar no formato #RRGGBB')
        hex_digits = set('0123456789abcdefABCDEF')
        for ch in value[1:]:
            if ch not in hex_digits:
                raise ValueError('Cor deve estar no formato #RRGGBB')
        return value


@router.post('/{indicator_id}/move')
def move_indicator(indicator_id: int, payload: dict, db: DbSession, ctx=Depends(get_current_ctx)):
    category = payload.get('category')
    row = db.execute(text("SELECT id FROM indicators WHERE id=:i AND organization_id=:o"), {"i": indicator_id, "o": ctx.organization_id}).first()
    if not row:
        raise HTTPException(status_code=404, detail='Indicador nao encontrado')
    db.execute(text("UPDATE indicators SET category=:c WHERE id=:i"), {"c": category, "i": indicator_id})
    db.commit()
    out = db.execute(text("SELECT id, key, name, dataset, fmt, category FROM indicators WHERE id=:i"), {"i": indicator_id}).mappings().first()
    return out


@router.get('/categories')
def list_categories(db: DbSession, ctx=Depends(get_current_ctx)):
    existing = db.execute(
        text("SELECT name, color FROM indicator_categories WHERE organization_id=:o ORDER BY name"),
        {"o": ctx.organization_id}
    ).mappings().all()
    known = {row['name'] for row in existing}
    extras = sorted(db.execute(
        text(
            """
            SELECT DISTINCT category AS name
            FROM indicators
            WHERE organization_id=:o AND category IS NOT NULL AND category <> ''
            """
        ),
        {"o": ctx.organization_id}
    ).scalars().all())
    for name in extras:
        if name not in known:
            existing.append({"name": name, "color": None})
    return existing


@router.post('/categories')
def create_category(payload: CategoryCreate, db: DbSession, ctx=Depends(get_current_ctx)):
    name = payload.name
    color = payload.color
    try:
        result = db.execute(
            text("UPDATE indicator_categories SET color=:c WHERE organization_id=:o AND name=:n"),
            {"o": ctx.organization_id, "n": name, "c": color}
        )
        if result.rowcount == 0:
            db.execute(
                text("INSERT INTO indicator_categories (organization_id, name, color) VALUES (:o, :n, :c)"),
                {"o": ctx.organization_id, "n": name, "c": color}
            )
        db.commit()
        return {"ok": True, "name": name, "color": color}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.patch('/categories/{current_name}')
def update_category(current_name: str, payload: CategoryUpdate, db: DbSession, ctx=Depends(get_current_ctx)):
    original = (current_name or '').strip()
    if not original:
        raise HTTPException(status_code=400, detail='Nome invalido')
    new_name = payload.name.strip() if payload.name else original
    color = payload.color
    try:
        row = db.execute(
            text("SELECT id FROM indicator_categories WHERE organization_id=:o AND name=:n"),
            {"o": ctx.organization_id, "n": original}
        ).first()
        if not row:
            db.execute(
                text("INSERT OR IGNORE INTO indicator_categories (organization_id, name, color) VALUES (:o, :n, :c)"),
                {"o": ctx.organization_id, "n": original, "c": color}
            )
        if new_name != original:
            db.execute(
                text("UPDATE indicators SET category=:new WHERE organization_id=:o AND category=:old"),
                {"new": new_name, "o": ctx.organization_id, "old": original}
            )
            db.execute(
                text("UPDATE indicator_categories SET name=:new WHERE organization_id=:o AND name=:old"),
                {"new": new_name, "o": ctx.organization_id, "old": original}
            )
        if color is not None:
            db.execute(
                text("UPDATE indicator_categories SET color=:c WHERE organization_id=:o AND name=:n"),
                {"c": color, "o": ctx.organization_id, "n": new_name}
            )
        db.commit()
        result = db.execute(
            text("SELECT name, color FROM indicator_categories WHERE organization_id=:o AND name=:n"),
            {"o": ctx.organization_id, "n": new_name}
        ).mappings().first()
        return result or {"name": new_name, "color": color}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.delete('/categories/{name}')
def delete_category(name: str, db: DbSession, ctx=Depends(get_current_ctx)):
    trimmed = (name or '').strip()
    if not trimmed:
        raise HTTPException(status_code=400, detail='Nome invalido')
    try:
        db.execute(
            text("UPDATE indicators SET category=NULL WHERE organization_id=:o AND category=:n"),
            {"o": ctx.organization_id, "n": trimmed}
        )
        db.execute(
            text("DELETE FROM indicator_categories WHERE organization_id=:o AND name=:n"),
            {"o": ctx.organization_id, "n": trimmed}
        )
        db.commit()
        return {"ok": True, "name": trimmed}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
