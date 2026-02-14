# app/routers/legal_persons.py
from __future__ import annotations

from typing import Optional, Dict, Any, List
from datetime import date

from fastapi import APIRouter, HTTPException, Query, Body
from pydantic import BaseModel
from sqlalchemy import text

from app.db import engine

router = APIRouter(prefix="/legal-persons", tags=["legal_persons"])


# -------------------------
# Pydantic Models
# -------------------------
class LegalPersonIn(BaseModel):
    full_name: str
    last_name: Optional[str] = None
    middle_name: Optional[str] = None
    first_name: Optional[str] = None
    birthday: Optional[date] = None
    nationality: Optional[str] = None
    id_number: Optional[str] = None
    id_expiry_date: Optional[date] = None
    passport_number: Optional[str] = None
    passport_expiry_date: Optional[date] = None
    legal_address: Optional[str] = None
    postal_code: Optional[str] = None


# -------------------------
# Helpers
# -------------------------
def _status_where(status: str) -> str:
    s = (status or "active").lower().strip()
    if s == "deleted":
        return "deleted_at IS NOT NULL"
    if s == "all":
        return "1=1"
    return "deleted_at IS NULL"


# -------------------------
# List (search + status)
# GET /legal-persons?q=...&status=active|deleted|all&limit=200
# -------------------------
@router.get("")
def list_legal_persons(
    q: Optional[str] = Query(default=None),
    status: str = Query(default="active"),
    limit: int = Query(default=200, ge=1, le=500),
):
    q_s = (q or "").strip()
    where = [_status_where(status)]
    params: Dict[str, Any] = {"limit": limit}

    if q_s:
        where.append(
            "(full_name LIKE :q OR id_number LIKE :q OR passport_number LIKE :q)"
        )
        params["q"] = f"%{q_s}%"

    sql = text(f"""
        SELECT
            id,
            full_name,
            last_name,
            middle_name,
            first_name,
            birthday,
            nationality,
            id_number,
            id_expiry_date,
            passport_number,
            passport_expiry_date,
            legal_address,
            postal_code,
            created_at,
            updated_at,
            deleted_at
        FROM legal_persons
        WHERE {" AND ".join(where)}
        ORDER BY id DESC
        LIMIT :limit
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql, params).mappings().all()

    return {"items": [dict(r) for r in rows]}


# -------------------------
# Get detail
# -------------------------
@router.get("/{person_id}")
def get_legal_person(person_id: int):
    with engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT
                    id,
                    full_name,
                    last_name,
                    middle_name,
                    first_name,
                    birthday,
                    nationality,
                    id_number,
                    id_expiry_date,
                    passport_number,
                    passport_expiry_date,
                    legal_address,
                    postal_code,
                    created_at,
                    updated_at,
                    deleted_at
                FROM legal_persons
                WHERE id=:id
                LIMIT 1
            """),
            {"id": person_id},
        ).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="Legal person not found")
    return dict(row)


# -------------------------
# Create
# -------------------------
@router.post("")
def create_legal_person(payload: LegalPersonIn):
    data = payload.model_dump()

    if not (data.get("full_name") or "").strip():
        raise HTTPException(status_code=400, detail="full_name is required")

    sql = text("""
        INSERT INTO legal_persons
            (full_name, last_name, middle_name, first_name, birthday,
             nationality, id_number, id_expiry_date,
             passport_number, passport_expiry_date,
             legal_address, postal_code,
             created_at, updated_at, deleted_at)
        VALUES
            (:full_name, :last_name, :middle_name, :first_name, :birthday,
             :nationality, :id_number, :id_expiry_date,
             :passport_number, :passport_expiry_date,
             :legal_address, :postal_code,
             NOW(), NOW(), NULL)
    """)

    with engine.begin() as conn:
        conn.execute(sql, data)
        new_id = conn.execute(text("SELECT LAST_INSERT_ID()")).scalar()

    return {"id": int(new_id)}


# -------------------------
# Update
# -------------------------
@router.put("/{person_id}")
def update_legal_person(person_id: int, payload: LegalPersonIn):
    data = payload.model_dump()
    if not (data.get("full_name") or "").strip():
        raise HTTPException(status_code=400, detail="full_name is required")

    sql = text("""
        UPDATE legal_persons
        SET
            full_name=:full_name,
            last_name=:last_name,
            middle_name=:middle_name,
            first_name=:first_name,
            birthday=:birthday,
            nationality=:nationality,
            id_number=:id_number,
            id_expiry_date=:id_expiry_date,
            passport_number=:passport_number,
            passport_expiry_date=:passport_expiry_date,
            legal_address=:legal_address,
            postal_code=:postal_code,
            updated_at=NOW()
        WHERE id=:id
        LIMIT 1
    """)

    with engine.begin() as conn:
        r = conn.execute(sql, {**data, "id": person_id})
        if r.rowcount == 0:
            raise HTTPException(status_code=404, detail="Legal person not found")

    return {"ok": True}


# -------------------------
# Soft delete / restore
# -------------------------
@router.post("/{person_id}/delete")
def delete_legal_person(person_id: int):
    with engine.begin() as conn:
        r = conn.execute(
            text("""
                UPDATE legal_persons
                SET deleted_at=NOW(), updated_at=NOW()
                WHERE id=:id
                LIMIT 1
            """),
            {"id": person_id},
        )
        if r.rowcount == 0:
            raise HTTPException(status_code=404, detail="Legal person not found")
    return {"ok": True}


@router.post("/{person_id}/restore")
def restore_legal_person(person_id: int):
    with engine.begin() as conn:
        r = conn.execute(
            text("""
                UPDATE legal_persons
                SET deleted_at=NULL, updated_at=NOW()
                WHERE id=:id
                LIMIT 1
            """),
            {"id": person_id},
        )
        if r.rowcount == 0:
            raise HTTPException(status_code=404, detail="Legal person not found")
    return {"ok": True}
