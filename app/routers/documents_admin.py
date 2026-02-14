# app/routers/documents_admin.py
from __future__ import annotations

import os
from typing import Any, Dict, Optional
from typing import List
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from pydantic import BaseModel
from app.db import engine
from app.auth.deps import require_admin

router = APIRouter(prefix="/documents", tags=["documents-admin"])

# 本地上传目录根（按你实际项目调整）
UPLOADS_ROOT = os.path.abspath(os.path.join(os.getcwd(), "uploads"))


def _is_safe_path(path: str) -> bool:
    """只允许删除 uploads 根目录下的文件，防止误删系统文件。"""
    try:
        ap = os.path.abspath(path)
        return ap.startswith(UPLOADS_ROOT + os.sep) or ap == UPLOADS_ROOT
    except Exception:
        return False
    
class BulkHardDeleteIn(BaseModel):
    ids: List[int]

def _remove_file_and_empty_dirs(file_path: str) -> None:
    """删除文件，并尽量清理空目录（最多向上清理到 uploads 根目录）。"""
    if not file_path:
        return
    if not _is_safe_path(file_path):
        raise RuntimeError(f"unsafe path: {file_path}")
    if not os.path.exists(file_path):
        return

    os.remove(file_path)

    # 清理空目录：一路向上，直到 uploads_root
    cur_dir = os.path.dirname(os.path.abspath(file_path))
    while cur_dir and _is_safe_path(cur_dir) and cur_dir != UPLOADS_ROOT:
        try:
            if os.path.isdir(cur_dir) and not os.listdir(cur_dir):
                os.rmdir(cur_dir)
                cur_dir = os.path.dirname(cur_dir)
            else:
                break
        except Exception:
            break


@router.delete("/{doc_id}/hard-delete")
def hard_delete_document(
    doc_id: int,
    admin=Depends(require_admin),
) -> Dict[str, Any]:
    """
    管理员硬删除：
    - 仅允许删除回收站(is_deleted=1)的记录（避免误删正常文件）
    - 删除磁盘文件
    - 删除 DB 行
    """
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT id, is_deleted, storage_path
                FROM documents
                WHERE id=:id
                LIMIT 1
                """
            ),
            {"id": doc_id},
        ).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="Document not found")

    if int(row.get("is_deleted") or 0) != 1:
        raise HTTPException(status_code=400, detail="Document is not in recycle bin")

    storage_path = (row.get("storage_path") or "").strip()

    # 1) 先删文件（文件删不掉就不删 DB，防止丢索引）
    try:
        if storage_path:
            _remove_file_and_empty_dirs(storage_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to remove file: {e}")

    # 2) 再删 DB 行
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM documents WHERE id=:id AND is_deleted=1 LIMIT 1"),
            {"id": doc_id},
        )

    return {"ok": True, "doc_id": doc_id}

@router.post("/bulk-hard-delete")
def bulk_hard_delete(payload: BulkHardDeleteIn, admin=Depends(require_admin)):
    ids = [int(x) for x in payload.ids if int(x) > 0]
    ids = sorted(set(ids))
    if not ids:
        return {"deleted": [], "skipped": [], "errors": []}

    deleted, skipped, errors = [], [], []

    with engine.begin() as conn:
        # 1) 先查出所有符合条件（已软删）的记录
        rows = conn.execute(
            text("""
                SELECT id, storage_path, is_deleted
                FROM documents
                WHERE id IN :ids
            """),
            {"ids": tuple(ids)},
        ).mappings().all()

        row_map = {r["id"]: r for r in rows}

        for doc_id in ids:
            r = row_map.get(doc_id)
            if not r:
                skipped.append({"id": doc_id, "reason": "not_found"})
                continue
            if int(r["is_deleted"] or 0) != 1:
                skipped.append({"id": doc_id, "reason": "not_in_recycle"})
                continue

            storage_path = r.get("storage_path") or ""
            if storage_path and not _is_safe_path(storage_path):
                errors.append({"id": doc_id, "reason": "unsafe_path", "path": storage_path})
                continue

            # 2) 删除磁盘文件（存在则删，不存在也继续）
            try:
                if storage_path and os.path.exists(storage_path):
                    os.remove(storage_path)
            except Exception as e:
                errors.append({"id": doc_id, "reason": "file_delete_failed", "detail": str(e)})
                continue

            # 3) 删除 DB 行
            conn.execute(text("DELETE FROM documents WHERE id=:id"), {"id": doc_id})
            deleted.append(doc_id)

    return {"deleted": deleted, "skipped": skipped, "errors": errors}