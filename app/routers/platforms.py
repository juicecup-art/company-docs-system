# app/ui/routes/platforms.py
import os, uuid
from datetime import datetime
from fastapi import APIRouter, Request, UploadFile, File
from fastapi.responses import RedirectResponse, FileResponse, HTMLResponse
from sqlalchemy import select, insert, update, and_
from app.db import db  # 你的 engine/conn 封装
from app.db_tables import documents, platform_documents, platform_text_fields

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse, HTMLResponse
from sqlalchemy import text

router = APIRouter(prefix="/ui")

UPLOAD_ROOT = "uploads"  # 你本地路径：/uploads/{company_id}/...

def guess_group_key():
    return "platform"

def safe_filename(name: str) -> str:
    return name.replace("/", "_").replace("\\", "_")

@router.get("/platforms/{platform_name}", response_class=HTMLResponse)
def platform_group_redirect(platform_name: str):
    # 找到这个 platform_name 的任意一条 company_platforms 记录
    with db.connect() as conn:
        row = conn.execute(
            text("""
                SELECT id AS platform_id, company_id
                FROM company_platforms
                WHERE platform_name = :name
                ORDER BY id DESC
                LIMIT 1
            """),
            {"name": platform_name},
        ).mappings().first()

    if not row:
        return RedirectResponse("/ui/platforms", status_code=303)

    return RedirectResponse(
        f"/ui/companies/{row['company_id']}/platforms/{row['platform_id']}",
        status_code=303
    )

@router.get("/companies/{company_id}/platforms/{platform_id}", response_class=HTMLResponse)
def platform_detail(request: Request, company_id: int, platform_id: int):

    print("HIT platform_detail", company_id, platform_id)

    # 1️⃣ 平台基本信息
    q_platform = select(
        text("platform_name")
    ).select_from(
        text("company_platforms")
    ).where(
        text("id = :pid AND company_id = :cid")
    )

    # 2️⃣ 文本框
    q_fields = select(platform_text_fields).where(
        and_(
            platform_text_fields.c.company_id == company_id,
            platform_text_fields.c.platform_id == platform_id,
            platform_text_fields.c.is_deleted == 0,
        )
    ).order_by(
        platform_text_fields.c.sort_no.asc(),
        platform_text_fields.c.id.asc()
    )

    # 3️⃣ Word 文件
    q_docs = (
        select(
            platform_documents.c.id.label("pd_id"),
            documents.c.id.label("doc_id"),
            documents.c.original_filename,
            documents.c.mime_type,
            documents.c.file_size,
            documents.c.created_at,
        )
        .select_from(
            platform_documents.join(
                documents,
                platform_documents.c.document_id == documents.c.id
            )
        )
        .where(
            and_(
                platform_documents.c.company_id == company_id,
                platform_documents.c.platform_id == platform_id,
                platform_documents.c.doc_role == "word",
                platform_documents.c.is_deleted == 0,
                documents.c.is_deleted == 0,
            )
        )
        .order_by(platform_documents.c.id.desc())
    )

    # 4️⃣ 下方公司列表（按 platform_name 聚合，而不是 cp.id=:pid）
    q_rows = text("""
        SELECT
            cp.id AS cp_id,
            cp.company_id,
            c.company_name,
            c.country,
            cp.store_url,
            cp.domain,
            cp.created_at
        FROM company_platforms cp
        JOIN companies c ON c.id = cp.company_id
        WHERE LOWER(TRIM(cp.platform_name)) = :pkey
        ORDER BY cp.id DESC
    """)
    with db.connect() as conn:

        # 平台名称
        platform_row = conn.execute(
            text("""
                SELECT platform_name
                FROM company_platforms
                WHERE id=:pid AND company_id=:cid
            """),
            {"pid": platform_id, "cid": company_id},
        ).mappings().first()

        if not platform_row:
            return RedirectResponse("/ui", status_code=303)

        platform_name = platform_row["platform_name"]
        pkey = (platform_name or "").strip().lower()
        rows = conn.execute(q_rows, {"pkey": pkey}).mappings().all()
        fields = conn.execute(q_fields).mappings().all()
        word_docs = conn.execute(q_docs).mappings().all()
        rows = conn.execute(q_rows, {"pid": platform_id}).mappings().all()

    return request.app.state.templates.TemplateResponse(
        "platform_detail.html",
        {
            "request": request,
            "company_id": company_id,
            "platform_id": platform_id,
            "platform_name": platform_name,
            "fields": fields,
            "word_docs": word_docs,
            "rows": rows,
        },
    )

@router.post("/companies/{company_id}/platforms/{platform_id}/text/add", name="platform_text_add")
async def platform_text_add(request: Request, company_id: int, platform_id: int):
    form = await request.form()
    label = (form.get("label") or "文本框").strip()
    content = (form.get("content") or "").strip()
    sort_no = int(form.get("sort_no") or 0)

    if not content:
        return RedirectResponse(f"/ui/companies/{company_id}/platforms/{platform_id}", status_code=303)

    with db.connect() as conn:
        conn.execute(
            insert(platform_text_fields).values(
          company_id=company_id,
          platform_id=platform_id,
          label=label[:80],
          content=content,
          sort_no=sort_no,
          is_deleted=0,                 # ✅补
          created_at=datetime.utcnow(),
          updated_at=datetime.utcnow(), # ✅如果表要求 NOT NULL
      )
        )
        conn.commit()

    return RedirectResponse(f"/ui/companies/{company_id}/platforms/{platform_id}", status_code=303)


@router.post("/companies/{company_id}/platforms/{platform_id}/text/{field_id}/update", name="platform_text_update")
async def platform_text_update(request: Request, company_id: int, platform_id: int, field_id: int):
    form = await request.form()
    label = (form.get("label") or "文本框").strip()
    content = (form.get("content") or "").strip()
    sort_no = int(form.get("sort_no") or 0)

    with db.connect() as conn:
        conn.execute(
            update(platform_text_fields)
            .where(and_(
                platform_text_fields.c.id == field_id,
                platform_text_fields.c.company_id == company_id,
                platform_text_fields.c.platform_id == platform_id,
                platform_text_fields.c.is_deleted == 0,
            ))
            .values(label=label[:80], content=content, sort_no=sort_no, updated_at=datetime.utcnow())
        )
        conn.commit()

    return RedirectResponse(f"/ui/companies/{company_id}/platforms/{platform_id}", status_code=303)


@router.post(
    "/companies/{company_id}/platforms/{platform_id}/word/{pd_id}/delete",
    name="platform_word_delete",
)
def platform_word_delete(company_id: int, platform_id: int, pd_id: int):
    with db.connect() as conn:
        conn.execute(
            update(platform_documents)
            .where(and_(
                platform_documents.c.id == pd_id,
                platform_documents.c.company_id == company_id,
                platform_documents.c.platform_id == platform_id,
                platform_documents.c.doc_role == "word",
                platform_documents.c.is_deleted == 0,
            ))
            .values(is_deleted=1, deleted_at=datetime.utcnow())
        )
        conn.commit()
    return RedirectResponse(f"/ui/companies/{company_id}/platforms/{platform_id}", status_code=303)

ALLOWED_WORD_EXT = {".doc", ".docx", ".pdf"}  # 你要只允许 word 就删掉 .pdf

# 允许上传：word/pdf + 图片
ALLOWED_WORD_EXT = {".doc", ".docx", ".pdf", ".png", ".jpg", ".jpeg", ".webp"}

def ext_of(filename: str) -> str:
    return os.path.splitext((filename or "").lower())[1]

@router.post(
    "/companies/{company_id}/platforms/{platform_id}/word/upload",
    name="platform_word_upload",
)
async def platform_word_upload(
    request: Request,
    company_id: int,
    platform_id: int,
    file: UploadFile = File(...),
):
    filename = safe_filename(file.filename or "file")
    ext = ext_of(filename)

    if ext not in ALLOWED_WORD_EXT:
        return RedirectResponse(
            f"/ui/companies/{company_id}/platforms/{platform_id}",
            status_code=303,
        )

    rel_dir = os.path.join(str(company_id), "platforms", str(platform_id), "word")
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
        res = conn.execute(
            insert(documents).values(
                company_id=company_id,
                uploaded_by=user_id,
                group_key=guess_group_key(),
                file_type="platform_file",      # 你也可以继续用 platform_word
                category="platform",
                title="Platform File",
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

        conn.execute(
            insert(platform_documents).values(
                company_id=company_id,
                platform_id=platform_id,
                document_id=doc_id,
                doc_role="word",
                is_deleted=0,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
        )
        conn.commit()

    return RedirectResponse(
        f"/ui/companies/{company_id}/platforms/{platform_id}",
        status_code=303,
    )@router.post("/companies/{company_id}/platforms/{platform_id}/word/{pd_id}/delete")

def platform_word_delete(company_id: int, platform_id: int, pd_id: int):
    with db.connect() as conn:
        conn.execute(
            update(platform_documents)
            .where(and_(
                platform_documents.c.id == pd_id,
                platform_documents.c.company_id == company_id,
                platform_documents.c.platform_id == platform_id,
                platform_documents.c.doc_role == "word",
                platform_documents.c.is_deleted == 0,
            ))
            .values(is_deleted=1, deleted_at=datetime.utcnow())
        )
        conn.commit()

    return RedirectResponse(f"/ui/companies/{company_id}/platforms/{platform_id}", status_code=303)
@router.get("/docs/{doc_id}/download")
def download_doc(doc_id: int):
    with db.connect() as conn:
        d = conn.execute(
            select(documents).where(and_(documents.c.id == doc_id, documents.c.is_deleted == 0))
        ).mappings().first()

    if not d:
        return RedirectResponse("/ui", status_code=303)

    abs_path = os.path.join(UPLOAD_ROOT, d["storage_path"])
    return FileResponse(
        abs_path,
        filename=d["original_filename"],
        media_type=d["mime_type"] or "application/octet-stream",
    )
@router.get("/docs/{doc_id}/preview", response_class=HTMLResponse)
def preview_doc(request: Request, doc_id: int):
    with db.connect() as conn:
        d = conn.execute(
            select(documents).where(and_(documents.c.id == doc_id, documents.c.is_deleted == 0))
        ).mappings().first()

    if not d:
        return HTMLResponse("Not Found", status_code=404)

    mime = (d["mime_type"] or "").lower()
    ext = ext_of(d["original_filename"] or "")

    can_inline = mime.startswith("image/") or mime == "application/pdf" or ext == ".pdf"

    return request.app.state.templates.TemplateResponse(
        "doc_preview.html",
        {"request": request, "doc": d, "can_inline": can_inline},
    )