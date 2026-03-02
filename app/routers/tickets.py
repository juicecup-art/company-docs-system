# app/routers/tickets.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Generator, Optional, Dict, Any, List
from datetime import datetime
import json

from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.engine import Connection

from app.db import engine
from app.auth.deps import get_current_user

router = APIRouter(prefix="/tickets", tags=["tickets"])

# -------------------------
# DB Conn (事务)
# -------------------------
def get_conn() -> Generator[Connection, None, None]:
    with engine.begin() as conn:
        yield conn

# -------------------------
# 权限 helpers（按你现有风格）
# -------------------------
def _is_admin(user: dict) -> bool:
    return bool(user) and user.get("role") == "admin"

def _has_company_view(user: dict, company_id: int) -> bool:
    if _is_admin(user):
        return True
    with engine.connect() as conn:
        row = conn.execute(text("""
          SELECT 1 FROM user_company_permissions
          WHERE user_id=:uid AND company_id=:cid AND (can_view=1 OR can_edit=1 OR can_docs=1)
          LIMIT 1
        """), {"uid": int(user["id"]), "cid": int(company_id)}).first()
    return bool(row)

def _has_company_edit(user: dict, company_id: int) -> bool:
    if _is_admin(user):
        return True
    with engine.connect() as conn:
        row = conn.execute(text("""
          SELECT 1 FROM user_company_permissions
          WHERE user_id=:uid AND company_id=:cid AND can_edit=1
          LIMIT 1
        """), {"uid": int(user["id"]), "cid": int(company_id)}).first()
    return bool(row)

def _can_read_ticket(user: dict, t: dict) -> bool:
    if _is_admin(user):
        return True
    uid = int(user["id"])
    if int(t.get("requester_user_id") or 0) == uid:
        return True
    if t.get("assignee_user_id") and int(t["assignee_user_id"]) == uid:
        return True
    return False

def _can_write_ticket(user: dict, t: dict) -> bool:
    """仅 admin / 创建人 可写（编辑/删除/改状态等写操作）"""
    if _is_admin(user):
        return True
    uid = int(user["id"])
    return int(t.get("requester_user_id") or 0) == uid
# -------------------------
# 状态机（你截图那 5 个按钮就是这里决定能不能点）
# -------------------------
STATUS_NEXT: Dict[str, List[str]] = {
    "NEW": ["TRIAGE", "IN_PROGRESS", "REJECTED"],
    "TRIAGE": ["IN_PROGRESS", "PENDING_APPROVAL", "REJECTED"],
    "IN_PROGRESS": ["PENDING_APPROVAL", "RESOLVED", "REJECTED"],
    "PENDING_APPROVAL": ["IN_PROGRESS", "RESOLVED", "REJECTED"],
    "RESOLVED": ["CLOSED", "IN_PROGRESS"],
    "CLOSED": [],
    "REJECTED": [],
}

class StatusIn(BaseModel):
    to_status: str

# =========================================================
# 状态流转：POST /tickets/{ticket_id}/status
# =========================================================
@router.post("/{ticket_id}/status")
def change_status(
    ticket_id: int,
    payload: StatusIn,
    current_user: dict = Depends(get_current_user),
    conn: Connection = Depends(get_conn),
):
    # 1) 取工单
    t = conn.execute(text("""
        SELECT id, company_id, requester_user_id, assignee_user_id, status, is_deleted,
               resolved_at, closed_at
        FROM tickets
        WHERE id=:id
        LIMIT 1
    """), {"id": ticket_id}).mappings().first()

    if not t or int(t["is_deleted"] or 0) == 1:
        raise HTTPException(404, "Ticket not found")

    t = dict(t)

    # 2) 可读/可写校验（避免越权改状态）
    if not _can_read_ticket(current_user, t):
        raise HTTPException(403, "no permission")
    if not _can_write_ticket(current_user, t):
        raise HTTPException(403, "no permission")

    from_status = (t.get("status") or "").strip()
    to_status = (payload.to_status or "").strip().upper()

    if not to_status:
        raise HTTPException(400, "to_status required")

    # 3) 状态机校验
    allowed = STATUS_NEXT.get(from_status, [])
    if to_status not in allowed:
        raise HTTPException(
            400,
            f"invalid transition: {from_status} -> {to_status} (allowed: {allowed})",
        )

    # 4) 自动时间戳逻辑（你表里有 resolved_at / closed_at）
    resolved_at = t.get("resolved_at")
    closed_at = t.get("closed_at")

    # 进入 RESOLVED：填 resolved_at
    set_resolved_at = None
    if to_status == "RESOLVED" and not resolved_at:
        set_resolved_at = "NOW()"

    # 进入 CLOSED：填 closed_at（并确保 resolved_at 也有）
    set_closed_at = None
    set_resolved_at_if_missing = None
    if to_status == "CLOSED":
        if not resolved_at:
            set_resolved_at_if_missing = "NOW()"
        if not closed_at:
            set_closed_at = "NOW()"

    # 从 RESOLVED/CLOSED 回退到 IN_PROGRESS：不清时间（审计保留）
    # 如果你想回退就清掉时间戳，也可以做：resolved_at=NULL/closed_at=NULL

    # 5) 更新 tickets
    sql_set = ["status=:st", "updated_at=NOW()"]
    params = {"st": to_status, "id": ticket_id}

    if set_resolved_at:
        sql_set.append("resolved_at=NOW()")
    if set_resolved_at_if_missing:
        sql_set.append("resolved_at=IFNULL(resolved_at, NOW())")
    if set_closed_at:
        sql_set.append("closed_at=NOW()")

    conn.execute(
        text(f"UPDATE tickets SET {', '.join(sql_set)} WHERE id=:id"),
        params,
    )

    # 6) 写事件审计
    conn.execute(
        text("""
          INSERT INTO ticket_events
            (ticket_id, actor_user_id, event_type, from_status, to_status, payload_json, created_at)
          VALUES
            (:tid, :uid, 'status_changed', :from_s, :to_s, :payload, NOW())
        """),
        {
            "tid": ticket_id,
            "uid": int(current_user["id"]),
            "from_s": from_status,
            "to_s": to_status,
            "payload": json.dumps(
                {"allowed": allowed}, ensure_ascii=False
            ),
        },
    )

    return {"ok": True, "from": from_status, "to": to_status}
