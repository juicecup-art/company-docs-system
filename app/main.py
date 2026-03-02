# -*- coding: utf-8 -*-
"""
Company Document Management System (FastAPI + SQLAlchemy Core)

本文件包含：
1) Companies CRUD（软删除 + restore）
2) Legal Persons CRUD（软删除）
3) Company <-> Legal Person 绑定/解绑（company_legal_persons）
4) Company Platforms CRUD（company_platforms）

⚠️ 你原始代码存在的问题（已在此整理版中修复）：
- 同一路由重复声明多次：例如 @app.post("/legal-persons") 在原文件里出现了多次，
  FastAPI 会发生“后者覆盖前者”或启动时报错，导致行为不稳定。
- Pydantic Model 多次重复定义（LegalPersonCreate/Update/Patch 等），维护困难。
- import 在文件中到处散落且重复（from pydantic import BaseModel 重复多次）。
- 平台接口路径不一致：有的写 /company-platforms，有的写 /companies/{id}/platforms，有的写 /platforms/{id}
  这里我统一成一套清晰的 REST 风格（见下文）。

实现风格说明：
- 使用 SQLAlchemy Core（text SQL）+ engine.begin() 做事务。
- 查询结果使用 .mappings().first() 方便转 dict。
- MySQL 风格：使用 NOW() 和 LAST_INSERT_ID()（如果你换成 PostgreSQL，需要改这两处）。
"""
from dotenv import load_dotenv
load_dotenv()
from typing import Optional, Any, Dict
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from fastapi import HTTPException
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
from fastapi import HTTPException, Query
from sqlalchemy import select, insert, update, and_, desc, func
from datetime import datetime
from app.routers.document_upload import router as document_upload_router
from app.routers.auth import router as auth_router
from app.routers.ui import router as ui_router
from app.routers.legal_persons import router as legal_persons_router
from app.models.document import documents
from app.db import engine, init_db
from app.routers import documents_admin
import asyncio
from fastapi.templating import Jinja2Templates
from app.services.purge_service import purge_loop
from app.routers.admin import router as admin_router
from app.routers.tickets import router as tickets_router
from app.routers.ui_tickets import router as ui_tickets_router



app = FastAPI(title="Company Document Management System", version="0.1.0")
app.state.templates = Jinja2Templates(directory="app/templates")

@app.on_event("startup")
async def _startup_purge_task():
    asyncio.create_task(purge_loop())

# # ✅ 静态资源（可选）
# app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.include_router(tickets_router)
app.include_router(ui_tickets_router)
# =========================================================
app.include_router(admin_router)
app.include_router(ui_router)
app.include_router(auth_router)
app.include_router(document_upload_router)
app.include_router(legal_persons_router)
app.include_router(documents_admin.router)
# =========================================================
# Startup / Basic Health
# =========================================================

# print("=== UI bulk routes ===")
# for r in app.routes:
#     p = getattr(r, "path", "")
#     m = getattr(r, "methods", set())
#     if p.startswith("/ui/documents/bulk"):
#         print(p, m)
# print("=======================")

print("=== ROUTES START ===")
for r in app.routes:
    p = getattr(r, "path", "")
    if p.startswith("/ui/companies/") and "/platforms/" in p:
        print(p, getattr(r, "methods", None))
print("=== ROUTES END ===")
@app.on_event("startup")
def on_startup() -> None:
    """应用启动时初始化数据库（例如建表/迁移等由 init_db 内部处理）"""
    init_db()


@app.get("/health")
def health() -> Dict[str, str]:
    """健康检查：用于容器/负载均衡探活"""
    return {"status": "ok"}


@app.get("/db-test")
def db_test() -> Dict[str, Any]:
    """数据库连通性测试（SELECT 1）"""
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1")).scalar()
    return {"db": result}


# =========================================================
# Pydantic Schemas
# =========================================================

# ---------- Companies ----------

class CompanyCreate(BaseModel):
    company_name: str
    country: Optional[str] = None
    registration_number: Optional[str] = None
    vat_number: Optional[str] = None
    cui: Optional[str] = None
    address: Optional[str] = None
    postal_code: Optional[str] = None
    register_time: Optional[str] = None  # "YYYY-MM-DD"


class CompanyUpdate(CompanyCreate):
    """PUT：全量更新（通常 company_name 必填，其余可空）"""
    pass


class CompanyPatch(BaseModel):
    """PATCH：部分更新（只更新传入字段）"""
    company_name: Optional[str] = None
    country: Optional[str] = None
    registration_number: Optional[str] = None
    vat_number: Optional[str] = None
    cui: Optional[str] = None
    address: Optional[str] = None
    postal_code: Optional[str] = None
    register_time: Optional[str] = None  # "YYYY-MM-DD"


# ---------- Legal Persons ----------

class LegalPersonCreate(BaseModel):
    """
    法人创建：
    - full_name 必填，其它可选
    - 日期字段用字符串（建议你未来改成 date 类型并加校验）
    """
    full_name: str
    last_name: Optional[str] = None
    middle_name: Optional[str] = None
    first_name: Optional[str] = None
    birthday: Optional[str] = None            # "YYYY-MM-DD"
    nationality: Optional[str] = None
    id_number: Optional[str] = None
    id_expiry_date: Optional[str] = None      # "YYYY-MM-DD"
    passport_number: Optional[str] = None
    passport_expiry_date: Optional[str] = None  # "YYYY-MM-DD"
    legal_address: Optional[str] = None
    postal_code: Optional[str] = None


class LegalPersonUpdate(LegalPersonCreate):
    """PUT：全量更新（一般 full_name 必填，其它字段可为空）"""
    pass


class LegalPersonPatch(BaseModel):
    """PATCH：部分更新（只更新传入字段）"""
    full_name: Optional[str] = None
    last_name: Optional[str] = None
    middle_name: Optional[str] = None
    first_name: Optional[str] = None
    birthday: Optional[str] = None
    nationality: Optional[str] = None
    id_number: Optional[str] = None
    id_expiry_date: Optional[str] = None
    passport_number: Optional[str] = None
    passport_expiry_date: Optional[str] = None
    legal_address: Optional[str] = None
    postal_code: Optional[str] = None


# ---------- Company Platforms ----------

class PlatformCreate(BaseModel):
    """
    平台创建：
    - company_id 通常从 path 里拿，这里也保留字段用于一致性校验
    """
    company_id: int
    platform_name: str = Field(..., max_length=100)
    store_url: Optional[str] = Field(None, max_length=500)
    domain: Optional[str] = Field(None, max_length=255)


class PlatformPatch(BaseModel):
    """平台 PATCH 更新"""
    platform_name: Optional[str] = Field(None, max_length=100)
    store_url: Optional[str] = Field(None, max_length=500)
    domain: Optional[str] = Field(None, max_length=255)


# ---------- Company <-> Legal Person Binding ----------

class CompanyLegalPersonBindCreate(BaseModel):
    """绑定法人到公司（用于 /companies/{company_id}/legal-persons）"""
    legal_person_id: int
    role: Optional[str] = "director"


class CompanyLegalPersonLinkPatch(BaseModel):
    """更新绑定关系（目前只允许更新 role）"""
    role: Optional[str] = None

# ---------- Pydantic documents Models（Create/Update/Patch/Out） ----------
class DocumentCreate(BaseModel):
    company_id: int
    uploaded_by: int
    category: str = Field(..., max_length=50)
    title: str = Field(..., max_length=255)
    original_filename: str = Field(..., max_length=255)
    storage_path: str = Field(..., max_length=800)
    mime_type: Optional[str] = Field(None, max_length=100)
    file_size: Optional[int] = None
    file_sha256: Optional[str] = Field(None, min_length=64, max_length=64)
    file_type: Optional[str] = Field(None, max_length=50)


class DocumentUpdate(BaseModel):
    # PUT: 允许你要求全量也行；这里做成可选，实际按你习惯
    company_id: Optional[int] = None
    uploaded_by: Optional[int] = None
    category: Optional[str] = Field(None, max_length=50)
    title: Optional[str] = Field(None, max_length=255)
    original_filename: Optional[str] = Field(None, max_length=255)
    storage_path: Optional[str] = Field(None, max_length=800)
    mime_type: Optional[str] = Field(None, max_length=100)
    file_size: Optional[int] = None
    file_sha256: Optional[str] = Field(None, min_length=64, max_length=64)
    file_type: Optional[str] = Field(None, max_length=50)


class DocumentOut(BaseModel):
    id: int
    company_id: int
    uploaded_by: int
    category: str
    title: str
    original_filename: str
    storage_path: str
    mime_type: Optional[str] = None
    file_size: Optional[int] = None
    file_sha256: Optional[str] = None
    is_deleted: int
    deleted_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    file_type: Optional[str] = Field(None, max_length=50)


# =========================================================
# Companies CRUD (soft delete)
# =========================================================

@app.post("/companies")
def create_company(payload: CompanyCreate) -> Dict[str, Any]:
    """创建公司（默认未删除）"""
    sql = text("""
        INSERT INTO companies
        (company_name, country, registration_number, vat_number, cui, address, postal_code, register_time, created_at, updated_at)
        VALUES
        (:company_name, :country, :registration_number, :vat_number, :cui, :address, :postal_code, :register_time, NOW(), NOW())
    """)
    with engine.begin() as conn:
        conn.execute(sql, payload.model_dump())
        new_id = conn.execute(text("SELECT LAST_INSERT_ID()")).scalar()
        row = conn.execute(
            text("SELECT * FROM companies WHERE id=:id"),
            {"id": new_id},
        ).mappings().first()

    return {"item": dict(row)}


@app.get("/companies")
def list_companies(
    q: Optional[str] = Query(None, description="模糊搜索 company_name / registration_number / vat_number / cui"),
    country: Optional[str] = Query(None, description="国家筛选，如 RO/HU/BG"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> Dict[str, Any]:
    """
    公司列表：
    - 默认只返回未软删（deleted_at IS NULL）
    - 支持 q 模糊搜索 + country 筛选
    - 支持 limit/offset 分页
    """
    where = ["deleted_at IS NULL"]
    params: Dict[str, Any] = {"limit": limit, "offset": offset}

    if country:
        where.append("country = :country")
        params["country"] = country

    if q:
        where.append("""
            (
              company_name LIKE :q OR
              registration_number LIKE :q OR
              vat_number LIKE :q OR
              cui LIKE :q
            )
        """)
        params["q"] = f"%{q}%"

    where_sql = " AND ".join(where)

    with engine.connect() as conn:
        total = conn.execute(
            text(f"SELECT COUNT(*) FROM companies WHERE {where_sql}"),
            params,
        ).scalar()

        rows = conn.execute(
            text(f"""
                SELECT *
                FROM companies
                WHERE {where_sql}
                ORDER BY id DESC
                LIMIT :limit OFFSET :offset
            """),
            params,
        ).mappings().all()

    return {"total": int(total or 0), "items": [dict(r) for r in rows]}


@app.get("/companies/{company_id}")
def get_company(company_id: int) -> Dict[str, Any]:
    """查询单个公司（仅未软删）"""
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM companies WHERE id=:id AND deleted_at IS NULL"),
            {"id": company_id},
        ).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="Company not found")
    return {"item": dict(row)}


@app.put("/companies/{company_id}")
def update_company(company_id: int, payload: CompanyUpdate) -> Dict[str, Any]:
    """PUT 全量更新公司（仅未软删可更新）"""
    with engine.begin() as conn:
        exists = conn.execute(
            text("SELECT id FROM companies WHERE id=:id AND deleted_at IS NULL"),
            {"id": company_id},
        ).scalar()
        if not exists:
            raise HTTPException(status_code=404, detail="Company not found")

        conn.execute(text("""
            UPDATE companies SET
              company_name=:company_name,
              country=:country,
              registration_number=:registration_number,
              vat_number=:vat_number,
              cui=:cui,
              address=:address,
              postal_code=:postal_code,
              register_time=:register_time,
              updated_at=NOW()
            WHERE id=:id
        """), {**payload.model_dump(), "id": company_id})

        row = conn.execute(
            text("SELECT * FROM companies WHERE id=:id"),
            {"id": company_id},
        ).mappings().first()

    return {"item": dict(row)}


@app.patch("/companies/{company_id}")
def patch_company(company_id: int, payload: CompanyPatch) -> Dict[str, Any]:
    """PATCH 部分更新公司（仅更新传入字段）"""
    data = payload.model_dump(exclude_none=True)
    if not data:
        return {"updated": False, "reason": "no fields to update"}

    set_sql = ", ".join([f"{k} = :{k}" for k in data.keys()]) + ", updated_at=NOW()"
    params: Dict[str, Any] = {**data, "id": company_id}

    with engine.begin() as conn:
        exists = conn.execute(
            text("SELECT id FROM companies WHERE id=:id AND deleted_at IS NULL"),
            {"id": company_id},
        ).scalar()
        if not exists:
            raise HTTPException(status_code=404, detail="Company not found")

        conn.execute(text(f"UPDATE companies SET {set_sql} WHERE id=:id"), params)

        row = conn.execute(
            text("SELECT * FROM companies WHERE id=:id"),
            {"id": company_id},
        ).mappings().first()

    return {"item": dict(row)}


@app.delete("/companies/{company_id}")
def delete_company(company_id: int) -> Dict[str, Any]:
    """软删除公司（设置 deleted_at）"""
    with engine.begin() as conn:
        exists = conn.execute(
            text("SELECT id FROM companies WHERE id=:id AND deleted_at IS NULL"),
            {"id": company_id},
        ).scalar()
        if not exists:
            raise HTTPException(status_code=404, detail="Company not found")

        conn.execute(
            text("UPDATE companies SET deleted_at=NOW(), updated_at=NOW() WHERE id=:id"),
            {"id": company_id},
        )

    return {"deleted": True, "id": company_id}


@app.post("/companies/{company_id}/restore")
def restore_company(company_id: int) -> Dict[str, Any]:
    """恢复已软删的公司（deleted_at -> NULL）"""
    with engine.begin() as conn:
        exists = conn.execute(
            text("SELECT id FROM companies WHERE id=:id AND deleted_at IS NOT NULL"),
            {"id": company_id},
        ).scalar()
        if not exists:
            raise HTTPException(status_code=404, detail="Company not found or not deleted")

        conn.execute(
            text("UPDATE companies SET deleted_at=NULL, updated_at=NOW() WHERE id=:id"),
            {"id": company_id},
        )

    return {"restored": True, "id": company_id}


# =========================================================
# Legal Persons CRUD (soft delete)
# =========================================================

@app.post("/legal-persons")
def create_legal_person(payload: LegalPersonCreate) -> Dict[str, Any]:
    """创建法人"""
    sql = text("""
        INSERT INTO legal_persons
        (full_name, last_name, middle_name, first_name, birthday, nationality,
         id_number, id_expiry_date, passport_number, passport_expiry_date,
         legal_address, postal_code, created_at, updated_at)
        VALUES
        (:full_name, :last_name, :middle_name, :first_name, :birthday, :nationality,
         :id_number, :id_expiry_date, :passport_number, :passport_expiry_date,
         :legal_address, :postal_code, NOW(), NOW())
    """)
    with engine.begin() as conn:
        conn.execute(sql, payload.model_dump())
        new_id = conn.execute(text("SELECT LAST_INSERT_ID()")).scalar()
        row = conn.execute(
            text("SELECT * FROM legal_persons WHERE id=:id"),
            {"id": new_id},
        ).mappings().first()

    return {"item": dict(row)}


@app.get("/legal-persons")
def list_legal_persons(
    include_deleted: bool = Query(False, description="是否包含已软删的法人"),
    limit: int = Query(200, ge=1, le=500),
) -> Dict[str, Any]:
    """法人列表：默认不包含 deleted_at 非空记录"""
    where = "" if include_deleted else "WHERE deleted_at IS NULL"
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT id, full_name, first_name, last_name, nationality, created_at, updated_at, deleted_at
            FROM legal_persons
            {where}
            ORDER BY id DESC
            LIMIT :limit
        """), {"limit": limit}).mappings().all()

    return {"items": [dict(r) for r in rows]}


@app.get("/legal-persons/{legal_person_id}")
def get_legal_person(legal_person_id: int) -> Dict[str, Any]:
    """查询单个法人（仅未软删）"""
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT *
            FROM legal_persons
            WHERE id=:id AND deleted_at IS NULL
        """), {"id": legal_person_id}).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="Legal person not found")
    return {"item": dict(row)}


@app.put("/legal-persons/{legal_person_id}")
def update_legal_person(legal_person_id: int, payload: LegalPersonUpdate) -> Dict[str, Any]:
    """PUT 全量更新法人（仅未软删可更新）"""
    with engine.begin() as conn:
        exists = conn.execute(text("""
            SELECT id FROM legal_persons WHERE id=:id AND deleted_at IS NULL
        """), {"id": legal_person_id}).scalar()
        if not exists:
            raise HTTPException(status_code=404, detail="Legal person not found")

        conn.execute(text("""
            UPDATE legal_persons SET
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
        """), {**payload.model_dump(), "id": legal_person_id})

        row = conn.execute(
            text("SELECT * FROM legal_persons WHERE id=:id"),
            {"id": legal_person_id},
        ).mappings().first()

    return {"item": dict(row)}


@app.patch("/legal-persons/{legal_person_id}")
def patch_legal_person(legal_person_id: int, payload: LegalPersonPatch) -> Dict[str, Any]:
    """PATCH 部分更新法人（仅更新传入字段）"""
    data = payload.model_dump(exclude_none=True)
    if not data:
        return {"updated": False, "reason": "no fields provided"}

    set_clause = ", ".join([f"{k}=:{k}" for k in data.keys()])
    sql = text(f"""
        UPDATE legal_persons SET
            {set_clause},
            updated_at=NOW()
        WHERE id=:id AND deleted_at IS NULL
    """)

    with engine.begin() as conn:
        updated = conn.execute(sql, {**data, "id": legal_person_id}).rowcount
        if updated == 0:
            raise HTTPException(status_code=404, detail="Legal person not found")

        row = conn.execute(
            text("SELECT * FROM legal_persons WHERE id=:id"),
            {"id": legal_person_id},
        ).mappings().first()

    return {"item": dict(row)}


@app.delete("/legal-persons/{legal_person_id}")
def delete_legal_person(legal_person_id: int) -> Dict[str, Any]:
    """软删除法人（设置 deleted_at）"""
    with engine.begin() as conn:
        updated = conn.execute(text("""
            UPDATE legal_persons
            SET deleted_at=NOW(), updated_at=NOW()
            WHERE id=:id AND deleted_at IS NULL
        """), {"id": legal_person_id}).rowcount

    if updated == 0:
        raise HTTPException(status_code=404, detail="Legal person not found")

    return {"deleted": True, "id": legal_person_id}


# =========================================================
# Company <-> Legal Person Bindings (company_legal_persons)
# =========================================================

@app.post("/companies/{company_id}/legal-persons")
def bind_legal_person(company_id: int, payload: CompanyLegalPersonBindCreate) -> Dict[str, Any]:
    """
    绑定法人到公司：
    - company 必须存在且未软删
    - legal_person 必须存在（这里允许已软删与否你可自行决定；目前按存在即可）
    - 避免重复绑定（同公司同法人只允许一条）
    """
    with engine.begin() as conn:
        company = conn.execute(
            text("SELECT id FROM companies WHERE id=:id AND deleted_at IS NULL"),
            {"id": company_id},
        ).scalar()
        if not company:
            raise HTTPException(status_code=404, detail="Company not found")

        person = conn.execute(
            text("SELECT id FROM legal_persons WHERE id=:id AND deleted_at IS NULL"),
            {"id": payload.legal_person_id},
        ).scalar()
        if not person:
            raise HTTPException(status_code=404, detail="Legal person not found")

        exists = conn.execute(text("""
            SELECT id FROM company_legal_persons
            WHERE company_id=:company_id AND legal_person_id=:legal_person_id
        """), {"company_id": company_id, "legal_person_id": payload.legal_person_id}).scalar()

        if exists:
            return {"bound": True, "id": exists, "note": "already bound"}

        conn.execute(text("""
            INSERT INTO company_legal_persons (company_id, legal_person_id, role, created_at)
            VALUES (:company_id, :legal_person_id, :role, NOW())
        """), {
            "company_id": company_id,
            "legal_person_id": payload.legal_person_id,
            "role": payload.role or "director",
        })

        new_id = conn.execute(text("SELECT LAST_INSERT_ID()")).scalar()
        row = conn.execute(
            text("SELECT * FROM company_legal_persons WHERE id=:id"),
            {"id": new_id},
        ).mappings().first()

    return {"bound": True, "item": dict(row) if row else {"id": new_id}}


@app.get("/companies/{company_id}/legal-persons")
def list_company_legal_persons(company_id: int) -> Dict[str, Any]:
    """查询某公司绑定的法人列表（JOIN legal_persons 返回法人信息）"""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT
              clp.id AS link_id,
              clp.company_id,
              clp.legal_person_id,
              clp.role,
              clp.created_at,
              lp.full_name,
              lp.first_name,
              lp.middle_name,
              lp.last_name,
              lp.nationality,
              lp.id_number,
              lp.passport_number
            FROM company_legal_persons clp
            JOIN legal_persons lp ON lp.id = clp.legal_person_id
            WHERE clp.company_id = :company_id
            ORDER BY clp.id DESC
        """), {"company_id": company_id}).mappings().all()

    return {"items": [dict(r) for r in rows]}


@app.patch("/company-legal-persons/{link_id}")
def patch_company_legal_person_link(link_id: int, payload: CompanyLegalPersonLinkPatch) -> Dict[str, Any]:
    """更新绑定关系（目前只支持 role）"""
    data = payload.model_dump(exclude_none=True)
    if not data:
        return {"updated": False, "reason": "no fields"}

    sets = ", ".join([f"{k}=:{k}" for k in data.keys()])
    sql = text(f"UPDATE company_legal_persons SET {sets} WHERE id=:id")

    with engine.begin() as conn:
        rc = conn.execute(sql, {**data, "id": link_id}).rowcount
        if rc == 0:
            raise HTTPException(status_code=404, detail="Link not found")

        row = conn.execute(
            text("SELECT * FROM company_legal_persons WHERE id=:id"),
            {"id": link_id},
        ).mappings().first()

    return {"item": dict(row)}


@app.delete("/companies/{company_id}/legal-persons/{legal_person_id}")
def unbind_company_legal_person(company_id: int, legal_person_id: int) -> Dict[str, Any]:
    """
    解绑（按 company_id + legal_person_id 删除绑定行）
    - 如果你希望“解绑也走软删”，可以把 DELETE 改成 UPDATE deleted_at。
    """
    with engine.begin() as conn:
        deleted = conn.execute(text("""
            DELETE FROM company_legal_persons
            WHERE company_id=:company_id AND legal_person_id=:legal_person_id
        """), {"company_id": company_id, "legal_person_id": legal_person_id}).rowcount

    if deleted == 0:
        raise HTTPException(status_code=404, detail="Binding not found")

    return {"deleted": True, "company_id": company_id, "legal_person_id": legal_person_id}


@app.delete("/company-legal-persons/{link_id}")
def delete_company_legal_person_link(link_id: int) -> Dict[str, Any]:
    """按 link_id 删除绑定关系（提供给后台管理更方便）"""
    with engine.begin() as conn:
        rc = conn.execute(text("DELETE FROM company_legal_persons WHERE id=:id"), {"id": link_id}).rowcount

    if rc == 0:
        raise HTTPException(status_code=404, detail="Link not found")

    return {"deleted": True, "id": link_id}


# =========================================================
# Company Platforms (company_platforms)
# =========================================================

@app.post("/companies/{company_id}/platforms")
def create_platform(company_id: int, payload: PlatformCreate) -> Dict[str, Any]:
    """
    给公司新增平台：
    - path company_id 必须与 body.company_id 一致（防止误绑）
    - company 必须存在且未软删
    """
    if payload.company_id != company_id:
        raise HTTPException(status_code=400, detail="company_id mismatch")

    with engine.begin() as conn:
        c = conn.execute(
            text("SELECT id FROM companies WHERE id=:id AND deleted_at IS NULL"),
            {"id": company_id},
        ).scalar()
        if not c:
            raise HTTPException(status_code=404, detail="Company not found")

        conn.execute(text("""
            INSERT INTO company_platforms
            (company_id, platform_name, store_url, domain, created_at)
            VALUES
            (:company_id, :platform_name, :store_url, :domain, NOW())
        """), payload.model_dump())

        new_id = conn.execute(text("SELECT LAST_INSERT_ID()")).scalar()
        row = conn.execute(
            text("SELECT * FROM company_platforms WHERE id=:id"),
            {"id": new_id},
        ).mappings().first()

    return {"item": dict(row)}


@app.get("/companies/{company_id}/platforms")
def list_platforms(company_id: int) -> Dict[str, Any]:
    """列出公司平台（公司必须存在且未软删）"""
    with engine.connect() as conn:
        c = conn.execute(
            text("SELECT id FROM companies WHERE id=:id AND deleted_at IS NULL"),
            {"id": company_id},
        ).scalar()
        if not c:
            raise HTTPException(status_code=404, detail="Company not found")

        rows = conn.execute(text("""
            SELECT id, company_id, platform_name, store_url, domain, created_at
            FROM company_platforms
            WHERE company_id=:company_id
            ORDER BY id DESC
        """), {"company_id": company_id}).mappings().all()

    return {"items": [dict(r) for r in rows]}


@app.patch("/platforms/{platform_id}")
def patch_platform(platform_id: int, payload: PlatformPatch) -> Dict[str, Any]:
    """PATCH 更新平台（按 platform_id）"""
    data = payload.model_dump(exclude_none=True)
    if not data:
        return {"updated": False, "reason": "no fields"}

    set_sql = ", ".join([f"{k}=:{k}" for k in data.keys()])
    sql = text(f"UPDATE company_platforms SET {set_sql} WHERE id=:id")

    with engine.begin() as conn:
        exists = conn.execute(
            text("SELECT id FROM company_platforms WHERE id=:id"),
            {"id": platform_id},
        ).scalar()
        if not exists:
            raise HTTPException(status_code=404, detail="Platform not found")

        conn.execute(sql, {**data, "id": platform_id})
        row = conn.execute(
            text("SELECT * FROM company_platforms WHERE id=:id"),
            {"id": platform_id},
        ).mappings().first()

    return {"item": dict(row)}


@app.delete("/platforms/{platform_id}")
def delete_platform(platform_id: int) -> Dict[str, Any]:
    """删除平台（物理删除）"""
    with engine.begin() as conn:
        rc = conn.execute(
            text("DELETE FROM company_platforms WHERE id=:id"),
            {"id": platform_id},
        ).rowcount

    if rc == 0:
        raise HTTPException(status_code=404, detail="Platform not found")

    return {"deleted": True, "id": platform_id}

# =========================================================
# 公司聚合详情接口
# =========================================================

@app.get("/companies/{company_id}/full")
def get_company_full(company_id: int):
    # 1) company
    with engine.connect() as conn:
        company = conn.execute(text("""
            SELECT *
            FROM companies
            WHERE id=:id AND deleted_at IS NULL
        """), {"id": company_id}).mappings().first()

        if not company:
            raise HTTPException(status_code=404, detail="Company not found")

        # 2) platforms
        platforms = conn.execute(text("""
            SELECT id, company_id, platform_name, store_url, domain, created_at
            FROM company_platforms
            WHERE company_id=:company_id
            ORDER BY id DESC
        """), {"company_id": company_id}).mappings().all()

        # 3) legal persons (bindings + person info)
        legal_persons = conn.execute(text("""
            SELECT
              clp.id AS link_id,
              clp.company_id,
              clp.legal_person_id,
              clp.role,
              clp.created_at AS linked_at,

              lp.full_name,
              lp.last_name,
              lp.middle_name,
              lp.first_name,
              lp.birthday,
              lp.nationality,
              lp.id_number,
              lp.id_expiry_date,
              lp.passport_number,
              lp.passport_expiry_date,
              lp.legal_address,
              lp.postal_code
            FROM company_legal_persons clp
            JOIN legal_persons lp ON lp.id = clp.legal_person_id
            WHERE clp.company_id = :company_id
              AND (lp.deleted_at IS NULL OR lp.deleted_at = '')
            ORDER BY clp.id DESC
        """), {"company_id": company_id}).mappings().all()

    return {
        "company": dict(company),
        "platforms": [dict(x) for x in platforms],
        "legal_persons": [dict(x) for x in legal_persons],
    }

# FastAPI 路由：Documents CRUD + 查询 + 软删除/恢复

# ---------- helper ----------
def _row_to_dict(row):
    return dict(row._mapping)

# 1) Create
@app.post("/documents", response_model=DocumentOut)
def create_document(payload: DocumentCreate):
    with engine.begin() as conn:
        stmt = (
            insert(documents)
            .values(**payload.model_dump(), is_deleted=0, deleted_at=None)
        )
        res = conn.execute(stmt)
        doc_id = res.inserted_primary_key[0]

        row = conn.execute(
            select(documents).where(documents.c.id == doc_id)
        ).fetchone()

        return _row_to_dict(row)

# 2) Get by id（默认不返回已删除）
@app.get("/documents/{doc_id}", response_model=DocumentOut)
def get_document(doc_id: int, include_deleted: bool = False):
    with engine.begin() as conn:
        cond = [documents.c.id == doc_id]
        if not include_deleted:
            cond.append(documents.c.is_deleted == 0)

        row = conn.execute(select(documents).where(and_(*cond))).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Document not found")
        return _row_to_dict(row)

# 3) List + Query（company_id / category / keyword / uploader / 时间范围 / 分页）
@app.get("/documents", response_model=list[DocumentOut])
def list_documents(
    company_id: int | None = Query(None),
    category: str | None = Query(None, max_length=50),
    uploaded_by: int | None = Query(None),
    keyword: str | None = Query(None, description="search in title/original_filename"),
    include_deleted: bool = Query(False),

    created_from: datetime | None = Query(None),
    created_to: datetime | None = Query(None),

    limit: int = Query(20, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    with engine.begin() as conn:
        cond = []
        if not include_deleted:
            cond.append(documents.c.is_deleted == 0)
        if company_id is not None:
            cond.append(documents.c.company_id == company_id)
        if category is not None:
            cond.append(documents.c.category == category)
        if uploaded_by is not None:
            cond.append(documents.c.uploaded_by == uploaded_by)
        if keyword:
            like = f"%{keyword}%"
            cond.append(
                (documents.c.title.like(like)) | (documents.c.original_filename.like(like))
            )
        if created_from:
            cond.append(documents.c.created_at >= created_from)
        if created_to:
            cond.append(documents.c.created_at <= created_to)

        stmt = select(documents)
        if cond:
            stmt = stmt.where(and_(*cond))

        stmt = stmt.order_by(desc(documents.c.created_at)).limit(limit).offset(offset)
        rows = conn.execute(stmt).fetchall()
        return [_row_to_dict(r) for r in rows]

# 4) Update（PATCH风格：只更新传入字段）
@app.patch("/documents/{doc_id}", response_model=DocumentOut)
def patch_document(doc_id: int, payload: DocumentUpdate):
    data = payload.model_dump(exclude_unset=True)
    if not data:
        raise HTTPException(status_code=400, detail="No fields to update")

    # 防止把已删除的文档误更新（你也可以允许 include_deleted 更新）
    with engine.begin() as conn:
        exists = conn.execute(
            select(documents.c.id).where(and_(documents.c.id == doc_id, documents.c.is_deleted == 0))
        ).fetchone()
        if not exists:
            raise HTTPException(status_code=404, detail="Document not found")

        conn.execute(
            update(documents)
            .where(documents.c.id == doc_id)
            .values(**data, updated_at=func.now())
        )

        row = conn.execute(select(documents).where(documents.c.id == doc_id)).fetchone()
        return _row_to_dict(row)

# 5) Soft delete
@app.delete("/documents/{doc_id}")
def delete_document(doc_id: int):
    with engine.begin() as conn:
        res = conn.execute(
            update(documents)
            .where(and_(documents.c.id == doc_id, documents.c.is_deleted == 0))
            .values(is_deleted=1, deleted_at=func.now(), updated_at=func.now())
        )
        if res.rowcount == 0:
            raise HTTPException(status_code=404, detail="Document not found or already deleted")
        return {"ok": True, "id": doc_id}

# 6) Restore
@app.post("/documents/{doc_id}/restore")
def restore_document(doc_id: int):
    with engine.begin() as conn:
        res = conn.execute(
            update(documents)
            .where(and_(documents.c.id == doc_id, documents.c.is_deleted == 1))
            .values(is_deleted=0, deleted_at=None, updated_at=func.now())
        )
        if res.rowcount == 0:
            raise HTTPException(status_code=404, detail="Document not found or not deleted")
        return {"ok": True, "id": doc_id}
import os
@app.get("/test-feishu")
async def test_feishu():
    import os
    from app.services.feishu_notify import send_feishu_card

    wh = (os.getenv("FEISHU_WEBHOOK") or "").strip()
    sec = (os.getenv("FEISHU_SECRET") or "").strip()

    card = {
        "schema": "2.0",
        "header": {"title": {"tag": "plain_text", "content": "测试通知"}, "template": "blue"},
        "body": {"elements": [{"tag": "markdown", "content": "飞书通知测试 ✅"}]},
    }

    resp = await send_feishu_card(card)

    return {
        "wh_tail": wh[-12:] if wh else "",
        "sec_tail": sec[-8:] if sec else "",
        "feishu_resp": resp,
    }