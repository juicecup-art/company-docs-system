# app/services/purge_service.py
from __future__ import annotations

import os
import asyncio
from datetime import datetime
from typing import Dict, Any

from sqlalchemy import text

from app.db import engine

DOC_RETENTION_DAYS = int(os.getenv("DOC_RETENTION_DAYS", "7"))
PURGE_INTERVAL_SECONDS = int(os.getenv("PURGE_INTERVAL_SECONDS", "3600"))  # 默认每小时跑一次

# 只允许删 uploads 根目录下的文件（按你项目实际路径）
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
UPLOADS_ROOT = os.path.abspath(os.path.join(PROJECT_ROOT, "uploads"))


def _is_safe_path(p: str) -> bool:
    try:
        ap = os.path.abspath(p)
        return ap.startswith(UPLOADS_ROOT + os.sep)
    except Exception:
        return False


def purge_once(retention_days: int = DOC_RETENTION_DAYS, batch_size: int = 200) -> Dict[str, Any]:
    stats = {
        "retention_days": retention_days,
        "picked": 0,
        "deleted": 0,
        "missing_file": 0,
        "skipped_unsafe_path": 0,
        "errors": 0,
    }

    pick_sql = text(
        """
        SELECT id, storage_path
        FROM documents
        WHERE is_deleted = 1
          AND deleted_at IS NOT NULL
          AND deleted_at <= (NOW() - INTERVAL :days DAY)
        ORDER BY deleted_at ASC
        LIMIT :limit
        """
    )
    delete_sql = text("DELETE FROM documents WHERE id=:id LIMIT 1")

    with engine.begin() as conn:
        rows = conn.execute(pick_sql, {"days": retention_days, "limit": batch_size}).mappings().all()
        stats["picked"] = len(rows)

        for r in rows:
            doc_id = int(r["id"])
            path = (r.get("storage_path") or "").strip()

            try:
                if path:
                    if not _is_safe_path(path):
                        stats["skipped_unsafe_path"] += 1
                    else:
                        try:
                            os.remove(path)
                        except FileNotFoundError:
                            stats["missing_file"] += 1
                        except Exception:
                            stats["errors"] += 1
                else:
                    stats["missing_file"] += 1

                conn.execute(delete_sql, {"id": doc_id})
                stats["deleted"] += 1
            except Exception:
                stats["errors"] += 1

    return stats


async def purge_loop():
    # 启动时先跑一次（可选）
    try:
        st = purge_once()
        print(f"[purge] first run: {st}")
    except Exception as e:
        print(f"[purge] first run error: {e}")

    while True:
        await asyncio.sleep(PURGE_INTERVAL_SECONDS)
        try:
            st = purge_once()
            print(f"[purge] {datetime.now().isoformat(timespec='seconds')} {st}")
        except Exception as e:
            print(f"[purge] error: {e}")
