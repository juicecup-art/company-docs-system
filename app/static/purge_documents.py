# app/static/purge_documents.py
from __future__ import annotations

import os
import sys
from datetime import datetime
from typing import Dict, Any, List

from sqlalchemy import text

# 让脚本可独立运行：python app/static/purge_documents.py
# 确保能 import app.db
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db import engine  # noqa: E402


DOC_RETENTION_DAYS = int(os.getenv("DOC_RETENTION_DAYS", "7"))
BATCH_SIZE = int(os.getenv("PURGE_BATCH_SIZE", "200"))

# （可选）额外保护：只允许删 uploads 目录下的文件
UPLOADS_ROOT = os.path.abspath(os.path.join(ROOT, "uploads"))


def _is_safe_path(p: str) -> bool:
    try:
        ap = os.path.abspath(p)
        return ap.startswith(UPLOADS_ROOT + os.sep)
    except Exception:
        return False


def purge_once(retention_days: int = DOC_RETENTION_DAYS, batch_size: int = BATCH_SIZE) -> Dict[str, Any]:
    stats = {
        "retention_days": retention_days,
        "batch_size": batch_size,
        "picked": 0,
        "deleted": 0,
        "missing_file": 0,
        "skipped_unsafe_path": 0,
        "errors": 0,
    }

    # 先取一批待清理的记录（避免一次性扫太多）
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

    # 真正删 DB 的语句
    delete_sql = text("DELETE FROM documents WHERE id = :id LIMIT 1")

    with engine.begin() as conn:
        rows = conn.execute(pick_sql, {"days": retention_days, "limit": batch_size}).mappings().all()

        stats["picked"] = len(rows)

        for r in rows:
            doc_id = int(r["id"])
            path = (r.get("storage_path") or "").strip()

            try:
                # 1) 删文件（安全路径保护）
                if path:
                    if not _is_safe_path(path):
                        stats["skipped_unsafe_path"] += 1
                    else:
                        try:
                            os.remove(path)
                        except FileNotFoundError:
                            stats["missing_file"] += 1
                        except Exception:
                            # 文件删失败也继续尝试删 DB？这里选择：算 error，但仍继续删 DB
                            stats["errors"] += 1
                else:
                    # 没路径也算 missing_file
                    stats["missing_file"] += 1

                # 2) 删 DB 行
                conn.execute(delete_sql, {"id": doc_id})
                stats["deleted"] += 1

            except Exception:
                stats["errors"] += 1

    return stats


def main():
    start = datetime.now()
    stats = purge_once()
    cost = (datetime.now() - start).total_seconds()

    print(
        f"[purge_documents] retention_days={stats['retention_days']} "
        f"picked={stats['picked']} deleted={stats['deleted']} "
        f"missing_file={stats['missing_file']} skipped_unsafe_path={stats['skipped_unsafe_path']} "
        f"errors={stats['errors']} cost={cost:.2f}s"
    )


if __name__ == "__main__":
    main()
