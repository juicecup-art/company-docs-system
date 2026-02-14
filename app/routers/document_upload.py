# app/routers/document_upload.py

from __future__ import annotations

import os
import uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Depends
from fastapi.responses import FileResponse
from sqlalchemy import select, insert, update, and_, func, desc, text
from sqlalchemy.exc import IntegrityError

from app.db import engine
from app.models.document import documents
from app.services.file_storage import save_upload_file

from fastapi import Depends
from app.auth.deps import get_current_user
from app.schemas.document import DocumentOutWithDedup

from fastapi import Depends
from app.auth.deps import get_current_user

# ✅ 如果你要“接口强制登录”，就打开下面两行并在每个接口加 Depends(get_current_user)
# from app.auth.deps import get_current_user


router = APIRouter(prefix="/documents", tags=["Documents"])


# ---------------------------
# Helpers
# ---------------------------

def row_to_dict(row) -> Dict[str, Any]:
    return dict(row._mapping)


def ensure_company_exists(conn, company_id: int):
    row = conn.execute(
        text("SELECT id FROM companies WHERE id=:id LIMIT 1"),
        {"id": company_id},
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"company_id={company_id} not found")


def ensure_user_exists(conn, user_id: int):
    row = conn.execute(
        text("SELECT id FROM users WHERE id=:id LIMIT 1"),
        {"id": user_id},
    ).fetchone()
    if not row:
        raise HTTPException(status_code=400, detail=f"uploaded_by={user_id} not found")


def safe_delete_files_after_commit(paths: List[str]):
    """事务结束后删除磁盘文件；paths 支持重复，函数内会去重。"""
    seen = set()
    for p in paths:
        if not p or p in seen:
            continue
        seen.add(p)

        fp = p.replace("/", os.sep)
        try:
            if os.path.exists(fp):
                os.remove(fp)
        except Exception:
            # 不要因为删除失败影响主流程
            pass


def get_group_key_from_old(old: Dict[str, Any]) -> str:
    # MySQL 返回 dict 里理论上会有 group_key；如果没有，报 500 提醒你表/模型/查询不一致
    gk = old.get("group_key")
    if not gk:
        raise HTTPException(
            status_code=500,
            detail="old document missing group_key; check DB column and documents table mapping",
        )
    return str(gk)


# ---------------------------
# Upload
# ---------------------------

@router.post("/upload", response_model=DocumentOutWithDedup)
def upload_document(
    file: UploadFile = File(...),
    company_id: int = Form(...),
    category: str = Form(...),
    title: str = Form(...),
    file_type: str = Form(""),
    group_key: str = Form(""),
    dedup: bool = Form(True),
    current_user: dict = Depends(get_current_user),
):
    uploaded_by = int(current_user["id"])
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing filename")

    storage_path, file_size, sha256_hex = save_upload_file(
        file,
        company_id=company_id,
        category=category,
    )

    post_commit_delete_paths: List[str] = []

    with engine.begin() as conn:
        ensure_company_exists(conn, company_id)
        ensure_user_exists(conn, uploaded_by)

        if dedup:
            existed = conn.execute(
                select(documents).where(
                    and_(
                        documents.c.company_id == company_id,
                        documents.c.file_sha256 == sha256_hex,
                        documents.c.is_deleted == 0,
                    )
                )
            ).fetchone()
            if existed:
                post_commit_delete_paths.append(storage_path)
                data = row_to_dict(existed)
                data["deduped"] = True
                return data

        # ✅ 如果 UI 传了 group_key 就用它，否则生成新的
        group_key_s = (group_key or "").strip() or uuid.uuid4().hex

        payload = {
            "company_id": company_id,
            "group_key": group_key_s,
            "uploaded_by": uploaded_by,
            "category": category,
            "title": title,
            "file_type": (file_type or "").strip(),   # ✅ 修复点：写入 DB
            "original_filename": file.filename,
            "storage_path": storage_path,
            "mime_type": file.content_type,
            "file_size": file_size,
            "file_sha256": sha256_hex,
            "is_deleted": 0,
            "deleted_at": None,
        }

        res = conn.execute(insert(documents).values(**payload))
        doc_id = res.inserted_primary_key[0]
        row = conn.execute(select(documents).where(documents.c.id == doc_id)).fetchone()

        data = row_to_dict(row)
        data["deduped"] = False

    safe_delete_files_after_commit(post_commit_delete_paths)
    return data


# ---------------------------
# Download / Path
# ---------------------------

@router.get("/{doc_id}/download")
def download_document(doc_id: int, include_deleted: bool = False):
    with engine.begin() as conn:
        cond = [documents.c.id == doc_id]
        if not include_deleted:
            cond.append(documents.c.is_deleted == 0)

        row = conn.execute(select(documents).where(and_(*cond))).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Document not found")

        d = dict(row._mapping)
        storage_path = d["storage_path"]
        file_path = storage_path.replace("/", os.sep)

        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="File not found on disk")

        return FileResponse(
            path=file_path,
            media_type=d.get("mime_type") or "application/octet-stream",
            filename=d.get("original_filename") or os.path.basename(file_path),
        )


@router.get("/{doc_id}/path")
def get_document_path(doc_id: int):
    with engine.begin() as conn:
        row = conn.execute(
            select(documents.c.id, documents.c.storage_path, documents.c.original_filename)
            .where(documents.c.id == doc_id)
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Document not found")
        return dict(row._mapping)


# ---------------------------
# Replace (versioning)
# ---------------------------

@router.post("/{doc_id}/replace")
def replace_document(
    doc_id: int,
    file: UploadFile = File(...),
    uploaded_by: int = Form(...),
    dedup: bool = Form(True),
    delete_old_file: bool = Form(True),
):
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing filename")

    post_commit_delete_paths: List[str] = []
    deleted_old_file = False

    with engine.begin() as conn:
        # 1) 找旧记录：必须存在且未删除
        old_row = conn.execute(
            select(documents).where(
                and_(
                    documents.c.id == doc_id,
                    documents.c.is_deleted == 0,
                )
            )
        ).fetchone()
        if not old_row:
            raise HTTPException(status_code=404, detail="old document not found (or already deleted)")

        old = dict(old_row._mapping)

        # 2) 校验 uploaded_by
        ensure_user_exists(conn, uploaded_by)

        # 3) 保存新文件（拿 sha256）
        storage_path, file_size, sha256_hex = save_upload_file(
            file,
            company_id=int(old["company_id"]),
            category=str(old["category"]),
        )

        # 4) dedup：同公司 + sha256 + 未删除 => 命中则只软删旧记录，不插新记录
        existed = None
        if dedup:
            existed = conn.execute(
                select(documents).where(
                    and_(
                        documents.c.company_id == old["company_id"],
                        documents.c.file_sha256 == sha256_hex,
                        documents.c.is_deleted == 0,
                    )
                )
            ).fetchone()

        # 5) 软删旧记录
        conn.execute(
            update(documents)
            .where(documents.c.id == doc_id)
            .values(is_deleted=1, deleted_at=func.now())
        )

        # 6) 旧文件磁盘删除：只有当该 storage_path 仅被这一条记录引用才删
        if delete_old_file:
            old_path = old.get("storage_path")
            if old_path:
                ref_cnt = conn.execute(
                    select(func.count())
                    .select_from(documents)
                    .where(documents.c.storage_path == old_path)
                ).scalar_one()
                if ref_cnt == 1:
                    post_commit_delete_paths.append(old_path)

        # 7) dedup 命中：不插新记录，新上传的文件也没用 -> 事务后删掉
        if existed:
            existed_id = dict(existed._mapping)["id"]
            post_commit_delete_paths.append(storage_path)

            result = {
                "ok": True,
                "old_id": doc_id,
                "new_id": existed_id,
                "deduped": True,
                "old_deleted": True,
                "deleted_old_file": False,
            }
        else:
            # 8) dedup 未命中：插入新版本，group_key 必须保持旧的一致
            gk = get_group_key_from_old(old)

            payload = {
                "company_id": old["company_id"],
                "group_key": gk,
                "uploaded_by": uploaded_by,
                "category": old["category"],
                "title": old["title"],
                "original_filename": file.filename,
                "storage_path": storage_path,
                "mime_type": file.content_type,
                "file_size": file_size,
                "file_sha256": sha256_hex,
                "is_deleted": 0,
                "deleted_at": None,
            }

            try:
                res = conn.execute(insert(documents).values(**payload))
            except IntegrityError as e:
                msg = str(getattr(e, "orig", e))
                if "FOREIGN KEY" in msg and "uploaded_by" in msg:
                    raise HTTPException(status_code=400, detail=f"uploaded_by={uploaded_by} not found")
                if "FOREIGN KEY" in msg and "company_id" in msg:
                    raise HTTPException(status_code=404, detail=f"company_id={old['company_id']} not found")
                raise

            new_id = res.inserted_primary_key[0]
            result = {
                "ok": True,
                "old_id": doc_id,
                "new_id": new_id,
                "deduped": False,
                "old_deleted": True,
                "deleted_old_file": False,
            }

    # 事务后删磁盘文件
    # 这里只在返回里标记“旧文件是否删掉”，新文件（dedup命中时）删不删不重要
    old_storage_path = old.get("storage_path") if "old" in locals() else None
    for p in list(dict.fromkeys(post_commit_delete_paths)):
        fp = p.replace("/", os.sep)
        try:
            if os.path.exists(fp):
                os.remove(fp)
                if old_storage_path and p == old_storage_path:
                    deleted_old_file = True
        except Exception:
            pass

    result["deleted_old_file"] = deleted_old_file
    return result


# ---------------------------
# History (by group_key)
# ---------------------------

@router.get("/{doc_id}/history")
def document_history(doc_id: int):
    with engine.begin() as conn:
        cur = conn.execute(select(documents).where(documents.c.id == doc_id)).fetchone()
        if not cur:
            raise HTTPException(status_code=404, detail="Document not found")

        cur_d = dict(cur._mapping)
        gk = get_group_key_from_old(cur_d)

        rows = conn.execute(
            select(documents)
            .where(documents.c.group_key == gk)
            .order_by(desc(documents.c.created_at), desc(documents.c.id))
        ).fetchall()

        items = [dict(r._mapping) for r in rows]

        # current：最新且未删除的那条（按 created_at desc）
        current_id = None
        for it in items:
            if it.get("is_deleted") == 0:
                current_id = it["id"]
                break

        return {
            "group_key": gk,
            "current_id": current_id,
            "items": items,
        }


# ---------------------------
# Make Current (by group_key)
# ---------------------------

@router.post("/{doc_id}/make-current")
def make_document_current(
    doc_id: int,
    delete_replaced_files: bool = Form(True),
):
    to_delete_paths: List[str] = []

    with engine.begin() as conn:
        target = conn.execute(select(documents).where(documents.c.id == doc_id)).fetchone()
        if not target:
            raise HTTPException(status_code=404, detail="Document not found")

        t = dict(target._mapping)
        gk = get_group_key_from_old(t)

        # 找目前 current
        current = conn.execute(
            select(documents)
            .where(and_(documents.c.group_key == gk, documents.c.is_deleted == 0))
            .order_by(desc(documents.c.created_at), desc(documents.c.id))
        ).fetchone()
        current_id = dict(current._mapping)["id"] if current else None

        # 把同组所有“未删除”版本软删（除了目标 doc_id）
        alive_rows = conn.execute(
            select(documents.c.id, documents.c.storage_path)
            .where(and_(documents.c.group_key == gk, documents.c.is_deleted == 0, documents.c.id != doc_id))
        ).fetchall()

        if alive_rows:
            conn.execute(
                update(documents)
                .where(and_(documents.c.group_key == gk, documents.c.is_deleted == 0, documents.c.id != doc_id))
                .values(is_deleted=1, deleted_at=func.now())
            )
            if delete_replaced_files:
                for r in alive_rows:
                    to_delete_paths.append(dict(r._mapping)["storage_path"])

        # 目标设为 current
        conn.execute(
            update(documents)
            .where(documents.c.id == doc_id)
            .values(is_deleted=0, deleted_at=None)
        )

        # 计算哪些路径安全删除（没有其他记录引用）
        safe_delete_files: List[str] = []
        if delete_replaced_files and to_delete_paths:
            for p in set(to_delete_paths):
                ref = conn.execute(
                    select(func.count())
                    .select_from(documents)
                    .where(and_(documents.c.storage_path == p, documents.c.id != doc_id))
                ).scalar_one()
                if ref == 0:
                    safe_delete_files.append(p)

    safe_delete_files_after_commit(safe_delete_files)

    return {
        "ok": True,
        "made_current_id": doc_id,
        "previous_current_id": current_id,
        "deleted_files": safe_delete_files,
    }


# ---------------------------
# Group delete / restore (by group_key)
# ---------------------------

@router.post("/{doc_id}/delete-group")
def delete_document_group(doc_id: int, delete_files: bool = Form(False)):
    """
    软删除整个版本组（group_key）。
    delete_files=True：仅当某 storage_path 不再被任何记录引用时才删除磁盘文件（安全删）
    """
    to_delete_paths: List[str] = []

    with engine.begin() as conn:
        row = conn.execute(select(documents).where(documents.c.id == doc_id)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Document not found")

        d = dict(row._mapping)
        gk = get_group_key_from_old(d)

        if delete_files:
            rows = conn.execute(
                select(documents.c.storage_path)
                .where(documents.c.group_key == gk)
            ).fetchall()
            to_delete_paths = [dict(r._mapping)["storage_path"] for r in rows if dict(r._mapping).get("storage_path")]

        conn.execute(
            update(documents)
            .where(documents.c.group_key == gk)
            .values(is_deleted=1, deleted_at=func.now())
        )

        safe_delete_files: List[str] = []
        if delete_files and to_delete_paths:
            for p in set(to_delete_paths):
                ref = conn.execute(
                    select(func.count())
                    .select_from(documents)
                    .where(documents.c.storage_path == p)
                ).scalar_one()
                # 组内软删后，仍然可能有别的组引用同一个路径（极少，但我们安全处理）
                # ref>0 表示还有引用，不能删；ref==0 才能删
                if ref == 0:
                    safe_delete_files.append(p)

    safe_delete_files_after_commit(safe_delete_files)

    return {"ok": True, "group_deleted": True, "deleted_files": safe_delete_files}


@router.post("/{doc_id}/restore-group")
def restore_document_group(doc_id: int):
    """恢复整个版本组（group_key），并把最新版本设为 current（is_deleted=0），其余保持 deleted=1。"""
    with engine.begin() as conn:
        row = conn.execute(select(documents).where(documents.c.id == doc_id)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Document not found")

        d = dict(row._mapping)
        gk = get_group_key_from_old(d)

        # 先全部恢复（都设为未删除）
        conn.execute(
            update(documents)
            .where(documents.c.group_key == gk)
            .values(is_deleted=0, deleted_at=None)
        )

        # 再把“除最新那条外”的其他全部软删，确保只有一个 current
        latest = conn.execute(
            select(documents.c.id)
            .where(documents.c.group_key == gk)
            .order_by(desc(documents.c.created_at), desc(documents.c.id))
        ).fetchone()
        latest_id = dict(latest._mapping)["id"] if latest else None

        if latest_id:
            conn.execute(
                update(documents)
                .where(and_(documents.c.group_key == gk, documents.c.id != latest_id))
                .values(is_deleted=1, deleted_at=func.now())
            )

        return {"ok": True, "group_restored": True, "current_id": latest_id}
