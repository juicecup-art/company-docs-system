# app/routers/platforms.py
import os
import uuid
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Request, UploadFile, File, Form
from fastapi.responses import RedirectResponse, FileResponse, HTMLResponse
from sqlalchemy import select, insert, update, and_, text
from fastapi.responses import FileResponse, RedirectResponse, HTMLResponse
from starlette.responses import Response  # 可选


from app.db import db
from app.db_tables import (
    documents,
    platform_documents,
    platform_text_fields,
)

router = APIRouter(prefix="/ui")

UPLOAD_ROOT = "uploads"
ALLOWED_EXT = {".doc", ".docx", ".pdf", ".png", ".jpg", ".jpeg", ".webp"}

# ✅ 你的虚拟公司 __PLATFORM_GROUP__ 的真实 id
PLATFORM_GROUP_COMPANY_ID = 85


def safe_filename(name: str) -> str:
    return (name or "file").replace("/", "_").replace("\\", "_")


def ext_of(filename: str) -> str:
    return os.path.splitext((filename or "").lower())[1]


def platform_key_of(name: str) -> str:
    return (name or "").strip().lower()


def get_or_create_anchor_cp(conn, platform_name: str) -> int:
    """
    保证平台聚合页一定有一个“锚点 company_platforms”记录：
    company_id = PLATFORM_GROUP_COMPANY_ID(85)
    platform_name = platform_name
    返回 anchor_platform_id = cp.id
    """
    pkey = platform_key_of(platform_name)

    row = conn.execute(
        text("""
            SELECT id
            FROM company_platforms
            WHERE company_id = :cid
              AND LOWER(TRIM(platform_name)) = :pkey
            ORDER BY id DESC
            LIMIT 1
        """),
        {"cid": PLATFORM_GROUP_COMPANY_ID, "pkey": pkey},
    ).mappings().first()

    if row:
        return int(row["id"])

    res = conn.execute(
        text("""
            INSERT INTO company_platforms (company_id, platform_name, created_at, updated_at)
            VALUES (:cid, :pname, NOW(), NOW())
        """),
        {"cid": PLATFORM_GROUP_COMPANY_ID, "pname": platform_name},
    )
    conn.commit()
    return int(res.lastrowid)


# =========================
# 平台聚合页（公司列表 + 顶部文本/上传）
# GET /ui/platforms/{platform_name}
# =========================
@router.get("/platforms/{platform_name}", response_class=HTMLResponse)
def platform_detail_agg(request: Request, platform_name: str):
    pkey = platform_key_of(platform_name)

    q_rows = text("""
        SELECT
            cp.id AS cp_id,
            cp.company_id,
            c.company_name,
            c.country,
            cp.store_url,
            cp.domain,
            cp.status,
            cp.progress,
            cp.updated_at
        FROM company_platforms cp
        JOIN companies c ON c.id = cp.company_id
        WHERE LOWER(TRIM(cp.platform_name)) = :pkey
          AND cp.company_id <> :fake_cid
        ORDER BY cp.updated_at DESC, cp.id DESC
    """)

    with db.connect() as conn:
        anchor_platform_id = get_or_create_anchor_cp(conn, platform_name)

        rows = conn.execute(
            q_rows, {"pkey": pkey, "fake_cid": PLATFORM_GROUP_COMPANY_ID}
        ).mappings().all()

        # ✅ 平台聚合文本：复用 platform_text_fields
        q_fields = (
            select(platform_text_fields)
            .where(and_(
                platform_text_fields.c.company_id == PLATFORM_GROUP_COMPANY_ID,
                platform_text_fields.c.platform_id == anchor_platform_id,
                platform_text_fields.c.is_deleted == 0,
            ))
            .order_by(
                platform_text_fields.c.sort_no.asc(),
                platform_text_fields.c.id.asc(),
            )
        )
        fields = conn.execute(q_fields).mappings().all()

        # ✅ 平台聚合文件：复用 platform_documents + documents
        q_files = (
            select(
                platform_documents.c.id.label("pd_id"),
                documents.c.id.label("doc_id"),
                documents.c.original_filename,
                documents.c.mime_type,
                documents.c.file_size,
                documents.c.created_at,
            )
            .select_from(
                platform_documents.join(documents, platform_documents.c.document_id == documents.c.id)
            )
            .where(and_(
                platform_documents.c.company_id == PLATFORM_GROUP_COMPANY_ID,
                platform_documents.c.platform_id == anchor_platform_id,
                platform_documents.c.doc_role == "file",          # ✅ 用库里最常见的值
                platform_documents.c.is_deleted == 0,
                documents.c.is_deleted == 0,
                documents.c.group_key.like("platform_group:%"),   # ✅ 用 group_key 区分聚合文件
            ))
            .order_by(platform_documents.c.id.desc())
        )
        files = conn.execute(q_files).mappings().all()

    return request.app.state.templates.TemplateResponse(
        "platform_detail.html",
        {
            "request": request,
            "platform_name": platform_name,
            "platform_key": pkey,
            "rows": rows,
            "fields": fields,
            "files": files,
            "anchor_platform_id": anchor_platform_id,  # 可选：调试用
        },
    )


# =========================
# 平台聚合页：文本框 add/update/delete（复用 platform_text_fields）
# =========================
@router.post("/platforms/{platform_name}/text/add", name="platform_group_text_add")
async def platform_group_text_add(
    request: Request,
    platform_name: str,
    label: str = Form("文本框"),
    content: str = Form(""),
    sort_no: int = Form(0),
):
    label_s = (label or "文本框").strip()
    content_s = (content or "").strip()
    if not content_s:
        return RedirectResponse(f"/ui/platforms/{platform_name}?msg=内容不能为空", status_code=303)

    with db.connect() as conn:
        anchor_platform_id = get_or_create_anchor_cp(conn, platform_name)
        conn.execute(
            insert(platform_text_fields).values(
                company_id=PLATFORM_GROUP_COMPANY_ID,
                platform_id=anchor_platform_id,
                label=label_s[:80],
                content=content_s,
                sort_no=int(sort_no or 0),
                is_deleted=0,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
        )
        conn.commit()

    return RedirectResponse(f"/ui/platforms/{platform_name}", status_code=303)


@router.post("/platforms/{platform_name}/text/{field_id}/update", name="platform_group_text_update")
async def platform_group_text_update(
    request: Request,
    platform_name: str,
    field_id: int,
    label: str = Form("文本框"),
    content: str = Form(""),
    sort_no: int = Form(0),
):
    label_s = (label or "文本框").strip()
    content_s = (content or "").strip()

    with db.connect() as conn:
        anchor_platform_id = get_or_create_anchor_cp(conn, platform_name)
        conn.execute(
            update(platform_text_fields)
            .where(and_(
                platform_text_fields.c.id == field_id,
                platform_text_fields.c.company_id == PLATFORM_GROUP_COMPANY_ID,
                platform_text_fields.c.platform_id == anchor_platform_id,
                platform_text_fields.c.is_deleted == 0,
            ))
            .values(
                label=label_s[:80],
                content=content_s,
                sort_no=int(sort_no or 0),
                updated_at=datetime.utcnow(),
            )
        )
        conn.commit()

    return RedirectResponse(f"/ui/platforms/{platform_name}", status_code=303)


@router.post("/platforms/{platform_name}/text/{field_id}/delete", name="platform_group_text_delete")
def platform_group_text_delete(request: Request, platform_name: str, field_id: int):
    with db.connect() as conn:
        anchor_platform_id = get_or_create_anchor_cp(conn, platform_name)
        conn.execute(
            update(platform_text_fields)
            .where(and_(
                platform_text_fields.c.id == field_id,
                platform_text_fields.c.company_id == PLATFORM_GROUP_COMPANY_ID,
                platform_text_fields.c.platform_id == anchor_platform_id,
                platform_text_fields.c.is_deleted == 0,
            ))
            .values(is_deleted=1, deleted_at=datetime.utcnow(), updated_at=datetime.utcnow())
        )
        conn.commit()
    return RedirectResponse(f"/ui/platforms/{platform_name}", status_code=303)


# =========================
# 平台聚合页：文件上传/删除（复用 documents + platform_documents）
# =========================
@router.post("/platforms/{platform_name}/files/upload", name="platform_group_file_upload")
async def platform_group_file_upload(
    request: Request,
    platform_name: str,
    file: UploadFile = File(...),
):
    filename = safe_filename(file.filename or "file")
    ext = ext_of(filename)
    if ext not in ALLOWED_EXT:
        return RedirectResponse(f"/ui/platforms/{platform_name}?msg=不支持的文件类型", status_code=303)

    pkey = platform_key_of(platform_name)

    rel_dir = os.path.join("platform_groups", pkey)
    abs_dir = os.path.join(UPLOAD_ROOT, rel_dir)
    os.makedirs(abs_dir, exist_ok=True)

    new_name = f"{uuid.uuid4().hex}{ext}"
    abs_path = os.path.join(abs_dir, new_name)
    rel_path = os.path.join(rel_dir, new_name).replace("\\", "/")

    data = await file.read()
    with open(abs_path, "wb") as f:
        f.write(data)

    mime = file.content_type or "application/octet-stream"
    size = len(data)
    user_id = getattr(request.state, "user_id", None) or 1

    with db.connect() as conn:
        # 你现在已经有虚拟公司 __PLATFORM_GROUP__ = id 85
        anchor_platform_id = get_or_create_anchor_cp(conn, platform_name)

        # 1) 写 documents（documents 表有 updated_at，所以这里保留）
        res = conn.execute(
            insert(documents).values(
                company_id=PLATFORM_GROUP_COMPANY_ID,      # 85
                uploaded_by=user_id,
                group_key=f"platform_group:{pkey}",
                file_type="platform_group_file",
                category="platform",
                title=f"Platform({platform_name}) File",
                original_filename=filename,
                storage_path=rel_path,
                mime_type=mime,
                file_size=size,
                file_sha256=None,
                is_deleted=0,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
        )
        doc_id = res.lastrowid

        # 2) 绑到 platform_documents（⚠️关键：不要写 updated_at）
        conn.execute(
            insert(platform_documents).values(
                company_id=PLATFORM_GROUP_COMPANY_ID,  # 85
                platform_id=anchor_platform_id,
                document_id=doc_id,
                doc_role="file",          # 用你库里肯定允许的值
                is_deleted=0,
                created_at=datetime.utcnow(),
                # ✅ 不要写 updated_at（你表里没有）
            )
        )

        conn.commit()

    return RedirectResponse(f"/ui/platforms/{platform_name}", status_code=303)

@router.post("/platforms/{platform_name}/files/{pd_id}/delete", name="platform_group_file_delete")
def platform_group_file_delete(request: Request, platform_name: str, pd_id: int):
    with db.connect() as conn:
        anchor_platform_id = get_or_create_anchor_cp(conn, platform_name)

        conn.execute(
            update(platform_documents)
            .where(and_(
                platform_documents.c.id == pd_id,
                platform_documents.c.company_id == PLATFORM_GROUP_COMPANY_ID,
                platform_documents.c.platform_id == anchor_platform_id,
                platform_documents.c.doc_role == "file",
                platform_documents.c.is_deleted == 0,
            ))
            .values(
                is_deleted=1,
                deleted_at=datetime.utcnow(),
                # ✅ 不要 updated_at
            )
        )
        conn.commit()

    return RedirectResponse(f"/ui/platforms/{platform_name}", status_code=303)
@router.get("/docs/{doc_id}/download", name="ui_doc_download")
def ui_doc_download(doc_id: int):
    with db.connect() as conn:
        d = conn.execute(
            select(documents).where(and_(documents.c.id == doc_id, documents.c.is_deleted == 0))
        ).mappings().first()

    if not d:
        return HTMLResponse("Not Found", status_code=404)

    abs_path = os.path.join(UPLOAD_ROOT, d["storage_path"])
    if not os.path.exists(abs_path):
        return HTMLResponse("File Missing", status_code=404)

    return FileResponse(
        abs_path,
        filename=d["original_filename"],
        media_type=d["mime_type"] or "application/octet-stream",
    )


@router.get("/docs/{doc_id}/preview", name="ui_doc_preview")
def ui_doc_preview(request: Request, doc_id: int):
    with db.connect() as conn:
        d = conn.execute(
            select(documents).where(and_(documents.c.id == doc_id, documents.c.is_deleted == 0))
        ).mappings().first()

    if not d:
        return HTMLResponse("Not Found", status_code=404)

    abs_path = os.path.join(UPLOAD_ROOT, d["storage_path"])
    if not os.path.exists(abs_path):
        return HTMLResponse("File Missing", status_code=404)

    mime = (d["mime_type"] or "application/octet-stream").lower()
    filename = d["original_filename"] or "file"
    ext = ext_of(filename)

    # ✅ 只有这些类型适合 inline 预览
    can_inline = mime.startswith("image/") or mime == "application/pdf" or ext == ".pdf"

    if not can_inline:
        # 其它类型（doc/docx等）浏览器没法直接预览：跳下载
        return RedirectResponse(request.url_for("ui_doc_download", doc_id=doc_id), status_code=302)

    # ✅ inline 预览：关键是 headers 里 Content-Disposition: inline
    return FileResponse(
        abs_path,
        media_type=mime,
        filename=filename,
        headers={"Content-Disposition": f'inline; filename="{filename}"'},
    )