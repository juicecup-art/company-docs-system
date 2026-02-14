# app/routers/ui_tickets.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import json
import os
import uuid
from datetime import datetime
from typing import Any, Dict, Optional, List, Tuple
from urllib.parse import quote

from fastapi import APIRouter, Request, Query, Form, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import text

from app.db import engine
from app.auth.jwt import decode_access_token

"""
✅ 工单 UI 路由（单文件最终版 - 去重整理）
- prefix="/ui/tickets"
- 工单列表 / 新建 / 详情 / 评论 / 指派 / 状态流转 / 批量
- 附件：上传 / 下载(受控) / 替换 / 删除（硬删）
- 进度：新增 / 删除（硬删）+ 可带附件引用
- 权限：
  - 可见：admin / 创建人 / 任意负责人(主负责人 assignee_user_id 或 ticket_assignees)
  - 可操作（指派/改状态/评论/批量/进度）：admin / 创建人 / 任意负责人
  - 可管理附件 & 删除工单：仅 admin / 主负责人(assignee_user_id)
⚠️ ticket_attachments 表没有 is_deleted/updated_at/deleted_at 字段，所以这里不引用它们。
"""

# =========================================================
# ✅ Router
# =========================================================
router = APIRouter(prefix="/ui/tickets", tags=["ui-tickets"])
templates = Jinja2Templates(directory="app/templates")
COOKIE_NAME = "access_token"

# =========================================================
# 工单状态/流转（给 UI 模板用）
# =========================================================
ALL_STATUSES = ["UNRECEIVED", "WAITING", "PROCESSING", "SOLVED", "CLOSED"]

STATUS_LABELS_ZH = {
    "UNRECEIVED": "未接收",
    "WAITING": "等待中",
    "PROCESSING": "处理中",
    "SOLVED": "已解决",
    "CLOSED": "已关闭",
}

STATUS_NEXT = {
    "UNRECEIVED": ["WAITING", "PROCESSING"],
    "WAITING": ["PROCESSING", "SOLVED"],
    "PROCESSING": ["WAITING", "SOLVED"],
    "SOLVED": ["CLOSED", "PROCESSING"],
    "CLOSED": [],
}

# =========================================================
# Auth / ctx helpers
# =========================================================
def _is_admin(user: Dict[str, Any] | None) -> bool:
    return bool(user) and user.get("role") == "admin"

def get_secret_key() -> str:
    key = os.environ.get("JWT_SECRET")
    if not key:
        raise RuntimeError("Missing JWT_SECRET in environment")
    return key

def _redirect(url: str) -> RedirectResponse:
    return RedirectResponse(url=url, status_code=302)

def _redirect_back(request: Request, fallback: str = "/ui/tickets") -> RedirectResponse:
    referer = request.headers.get("referer")
    return RedirectResponse(url=referer or fallback, status_code=302)

def _get_token_from_cookie(request: Request) -> Optional[str]:
    return request.cookies.get(COOKIE_NAME)

def _decode_user_id_from_token(token: str) -> Optional[int]:
    payload = decode_access_token(token, get_secret_key())
    if not payload:
        return None
    sub = payload.get("sub")
    if not sub:
        return None
    try:
        return int(sub)
    except Exception:
        return None

def _get_user_by_id(user_id: int) -> Optional[Dict[str, Any]]:
    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT id, username, display_name, email, role, status
                FROM users
                WHERE id=:id
                LIMIT 1
            """),
            {"id": user_id},
        ).mappings().first()
    return dict(row) if row else None

def _get_current_user_for_ui(request: Request) -> Optional[Dict[str, Any]]:
    token = _get_token_from_cookie(request)
    if not token:
        return None
    uid = _decode_user_id_from_token(token)
    if not uid:
        return None
    u = _get_user_by_id(uid)
    if not u:
        return None
    if int(u.get("status") or 0) != 1:
        return None
    return u

def _base_ctx(request: Request, current_user: dict | None, active: str = "") -> Dict[str, Any]:
    return {"request": request, "current_user": current_user, "active": active}

def _render_no_permission(
    request: Request,
    current_user: Dict[str, Any] | None,
    active: str,
    message: str,
    back_url: str = "/ui/tickets",
):
    return templates.TemplateResponse(
        "no_permission.html",
        {**_base_ctx(request, current_user, active), "message": message, "back_url": back_url},
        status_code=200,
    )

# =========================================================
# Common utils
# =========================================================
def _int_or_none(v: str) -> Optional[int]:
    v = (v or "").strip()
    if not v:
        return None
    try:
        return int(v)
    except Exception:
        return None

def _dt_or_none(v: str) -> Optional[str]:
    v = (v or "").strip()
    if not v:
        return None
    if len(v) == 10 and v[4] == "-" and v[7] == "-":
        return v + " 00:00:00"
    return v

def _make_ticket_no_by_id(ticket_id: int) -> str:
    return f"T{datetime.now().strftime('%Y%m%d')}-{ticket_id}"

def _list_active_users_for_assign() -> List[Dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT id, username, display_name, role
                FROM users
                WHERE status=1
                ORDER BY
                  CASE WHEN role='admin' THEN 0 ELSE 1 END,
                  id ASC
                LIMIT 500
            """)
        ).mappings().all()
    return [dict(r) for r in rows]

def _list_ticket_categories_for_filter(limit: int = 200) -> List[str]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT DISTINCT category
                FROM tickets
                WHERE is_deleted=0 AND category IS NOT NULL AND category<>''
                ORDER BY category ASC
                LIMIT :lim
            """),
            {"lim": int(limit)},
        ).fetchall()
    return [r[0] for r in rows if r and r[0]]

# =========================================================
# Permission helpers (minimal + correct with multi-assignees)
# =========================================================
def _is_related_user(current_user: Dict[str, Any], ticket_id: int, ticket_row: Dict[str, Any]) -> bool:
    """创建人 / 主负责人 / ticket_assignees 任意负责人"""
    if _is_admin(current_user):
        return True
    uid = int(current_user["id"])
    if int(ticket_row.get("requester_user_id") or 0) == uid:
        return True
    if int(ticket_row.get("assignee_user_id") or 0) == uid:
        return True
    # multi assignee
    with engine.connect() as conn:
        r = conn.execute(
            text("SELECT 1 FROM ticket_assignees WHERE ticket_id=:tid AND user_id=:uid LIMIT 1"),
            {"tid": int(ticket_id), "uid": uid},
        ).first()
    return bool(r)

def _can_view_ticket(current_user: Dict[str, Any], ticket_id: int, ticket_row: Dict[str, Any]) -> bool:
    return _is_related_user(current_user, ticket_id, ticket_row)

def _can_operate_ticket(current_user: Dict[str, Any], ticket_id: int, ticket_row: Dict[str, Any]) -> bool:
    return _is_related_user(current_user, ticket_id, ticket_row)

def _can_manage_ticket(current_user: Dict[str, Any], ticket_row: Dict[str, Any]) -> bool:
    """仅 admin / 主负责人(assignee_user_id)"""
    if _is_admin(current_user):
        return True
    uid = int(current_user["id"])
    return int(ticket_row.get("assignee_user_id") or 0) == uid

# =========================================================
# WHERE builder (list + count reuse)
# =========================================================
def _build_ticket_where(
    current_user: Dict[str, Any],
    status: str,
    q: str,
    scope: str,
    company_id: Optional[int],
    exclude_solved_default: bool = True,
    category: str = "",
    priority: str = "",
) -> Tuple[List[str], Dict[str, Any]]:
    where = ["t.is_deleted=0"]
    params: Dict[str, Any] = {}

    uid = int(current_user["id"])
    scope_s = (scope or "").strip() or "all"

    assignee_expr = """
    (
      t.assignee_user_id = :uid
      OR EXISTS (
        SELECT 1 FROM ticket_assignees ta
        WHERE ta.ticket_id = t.id AND ta.user_id = :uid
      )
    )
    """

    if scope_s == "mine":
        where.append("t.requester_user_id=:uid")
        params["uid"] = uid
    elif scope_s == "assigned":
        where.append(assignee_expr)
        params["uid"] = uid
    else:
        if not _is_admin(current_user):
            where.append(f"(t.requester_user_id=:uid OR {assignee_expr})")
            params["uid"] = uid

    status_s = (status or "").strip().upper()
    if not status_s and exclude_solved_default:
        where.append("t.status <> 'SOLVED'")
    elif status_s:
        where.append("t.status=:st")
        params["st"] = status_s

    q_s = (q or "").strip()
    if q_s:
        where.append("""
        (
            t.ticket_no LIKE CONCAT('%',:q,'%')
            OR t.title LIKE CONCAT('%',:q,'%')
            OR t.description LIKE CONCAT('%',:q,'%')
            OR t.company_name LIKE CONCAT('%',:q,'%')
            OR t.platform_name LIKE CONCAT('%',:q,'%')
        )
        """)
        params["q"] = q_s

    if company_id is not None:
        where.append("t.company_id=:cid")
        params["cid"] = int(company_id)

    cat_s = (category or "").strip()
    if cat_s:
        where.append("t.category=:cat")
        params["cat"] = cat_s

    pr_s = (priority or "").strip().upper()
    if pr_s:
        where.append("UPPER(t.priority)=:pr")
        params["pr"] = pr_s

    return where, params

def _sql_last_status_mark_by_assignees(alias_ticket: str = "t") -> str:
    """
    ✅ 负责人最新一次【状态流转标记】时间：
    - 仅统计 ticket_progress.new_status IS NOT NULL 的记录（代表状态流转/标记）
    - 且操作人必须是负责人：主负责人 或 ticket_assignees 中的任一负责人
    """
    return f"""
    (
      SELECT MAX(tp.created_at)
      FROM ticket_progress tp
      WHERE tp.ticket_id = {alias_ticket}.id
        AND tp.new_status IS NOT NULL
        AND (
          tp.user_id = {alias_ticket}.assignee_user_id
          OR EXISTS (
              SELECT 1 FROM ticket_assignees ta2
              WHERE ta2.ticket_id = {alias_ticket}.id
                AND ta2.user_id = tp.user_id
          )
        )
    )
    """

# ✅ 统计每个状态数量（受 scope/q/category/priority 影响，但不受 status 影响）
def _count_by_status(conn, base_where_sql: str, params: dict) -> dict:
    rows = conn.execute(
        text(f"""
            SELECT UPPER(COALESCE(status,'WAITING')) AS st, COUNT(*) AS cnt
            FROM tickets t
            WHERE t.is_deleted=0
              {base_where_sql}
            GROUP BY UPPER(COALESCE(status,'WAITING'))
        """),
        params,
    ).mappings().all()

    counts = { (r["st"] or "").upper(): int(r["cnt"] or 0) for r in rows }
    # 确保所有状态都存在键
    for s in ["UNRECEIVED","WAITING","PROCESSING","SOLVED","CLOSED"]:
        counts.setdefault(s, 0)
    counts["ALL"] = sum(counts[s] for s in ["UNRECEIVED","WAITING","PROCESSING","SOLVED","CLOSED"])
    return counts

# =========================================================
# 1) List
# =========================================================
@router.get("", response_class=HTMLResponse)
def ui_tickets(
    request: Request,
    status: str | None = Query(default=""),
    q: str | None = Query(default=""),
    scope: str | None = Query(default="all"),
    company_id: str | None = Query(default=""),
    category: str | None = Query(default=""),
    priority: str | None = Query(default=""),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    include_solved: str | None = Query(default=""),
):
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    qv_q = (q or "").strip()
    qv_scope = (scope or "all").strip() or "all"
    qv_status = (status or "").strip().upper()
    qv_company_id = (company_id or "").strip()
    qv_category = (category or "").strip()
    qv_priority = (priority or "").strip().upper()
    cid = _int_or_none(qv_company_id)

    include_solved_flag = (include_solved or "").strip() == "1"

        # ✅ 默认就查询（不再要求必须带筛选参数）
    categories = _list_ticket_categories_for_filter()

    where, params = _build_ticket_where(
        current_user=current_user,
        status=qv_status,
        q=qv_q,
        scope=qv_scope,
        company_id=cid,
        exclude_solved_default=(not include_solved_flag),
        category=qv_category,
        priority=qv_priority,
    )
    params.update({"limit": limit, "offset": offset})

    # ✅ 顶部状态数量（同一筛选条件下按 status 分组统计）
    where_for_counts, params_for_counts = _build_ticket_where(
        current_user=current_user,
        status="",  # 不限制单一状态，统计全部
        q=qv_q,
        scope=qv_scope,
        company_id=cid,
        exclude_solved_default=False,  # ✅ 统计永远包含 SOLVED
        category=qv_category,
        priority=qv_priority,
    )
    sql_counts = text(f"""
        SELECT UPPER(t.status) AS st, COUNT(1) AS cnt
        FROM tickets t
        WHERE {" AND ".join(where_for_counts)}
        GROUP BY UPPER(t.status)
    """)

    with engine.connect() as conn:
        cnt_rows = conn.execute(sql_counts, params_for_counts).mappings().all()

    status_counts = { (r["st"] or "").upper(): int(r["cnt"] or 0) for r in cnt_rows }
    for s in ["UNRECEIVED","WAITING","PROCESSING","SOLVED","CLOSED"]:
        status_counts.setdefault(s, 0)
    status_counts["ALL"] = sum(status_counts[s] for s in ["UNRECEIVED","WAITING","PROCESSING","SOLVED","CLOSED"])
    total_counts_all = status_counts["ALL"]

    last_status_sql = _sql_last_status_mark_by_assignees("t")

    sql_list = text(f"""
        SELECT
          t.*,
          COALESCE(u1.display_name, u1.username) AS requester_name,
          COALESCE(u2.display_name, u2.username) AS primary_assignee_name,
          (
              SELECT GROUP_CONCAT(COALESCE(u_sub.display_name, u_sub.username) SEPARATOR ', ')
              FROM ticket_assignees ta
              JOIN users u_sub ON u_sub.id = ta.user_id
              WHERE ta.ticket_id = t.id
          ) AS all_assignee_names,

          /* ✅ 更新时间：负责人最近状态流转标记时间（fallback updated_at） */
          COALESCE(
              {last_status_sql},
              t.updated_at
          ) AS last_mark_at

        FROM tickets t
        LEFT JOIN users u1 ON u1.id = t.requester_user_id
        LEFT JOIN users u2 ON u2.id = t.assignee_user_id
        WHERE {" AND ".join(where)}
        ORDER BY
          CASE UPPER(t.priority)
              WHEN 'URGENT' THEN 0
              WHEN 'NORMAL' THEN 1
              WHEN 'LOW' THEN 2
              ELSE 9
          END,
          last_mark_at DESC,
          t.id DESC
        LIMIT :limit OFFSET :offset
    """)
    sql_total = text(f"SELECT COUNT(1) FROM tickets t WHERE {' AND '.join(where)}")

    with engine.connect() as conn:
        rows = conn.execute(sql_list, params).mappings().all()
        total = int(conn.execute(sql_total, params).scalar() or 0)

    return templates.TemplateResponse(
        "tickets_index.html",
        {
            **_base_ctx(request, current_user, "tickets"),
            "rows": rows,
            "total": total,
            "has_filter": True,
            "qv": {
                "q": qv_q,
                "scope": qv_scope,
                "status": qv_status,
                "company_id": qv_company_id,
                "include_solved": "1" if include_solved_flag else "",
                "category": qv_category,
                "priority": qv_priority,
            },
            "status_counts": status_counts,
            "status_counts_all": total_counts_all,
            "categories": categories,
            "ALL_STATUSES": ALL_STATUSES,
            "STATUS_LABELS_ZH": STATUS_LABELS_ZH,
        },
    )

def _redirect_back_to_list_keep_query(request: Request) -> RedirectResponse:
    qs = str(request.url.query)
    if qs:
        return RedirectResponse(url=f"/ui/tickets?{qs}", status_code=302)
    return _redirect_back(request)

# =========================================================
# 2) New
# =========================================================
@router.get("/new", response_class=HTMLResponse)
def ui_ticket_new(request: Request):
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    assignees = _list_active_users_for_assign()

    companies: List[Dict[str, Any]] = []
    try:
        with engine.connect() as conn:
            cols = conn.execute(text("SHOW COLUMNS FROM companies")).mappings().all()
            colnames = [str(c["Field"]) for c in cols]
            candidates = [
                "company_name", "name", "title", "display_name", "full_name",
                "legal_name", "short_name", "company", "name_cn", "name_en",
            ]
            name_col = next((c for c in candidates if c in colnames), None)

            if not name_col:
                rows = conn.execute(text("SELECT id FROM companies ORDER BY id DESC LIMIT 1000")).mappings().all()
                companies = [{"id": int(r["id"]), "name": f"Company #{int(r['id'])}"} for r in rows]
            else:
                rows = conn.execute(text(f"SELECT id, `{name_col}` AS name FROM companies ORDER BY id DESC LIMIT 1000")).mappings().all()
                companies = []
                for r in rows:
                    cid = int(r["id"])
                    nm = (r.get("name") or "").strip()
                    companies.append({"id": cid, "name": nm or f"Company #{cid}"})
    except Exception:
        companies = []

    return templates.TemplateResponse(
        "ticket_form.html",
        {
            **_base_ctx(request, current_user, "tickets"),
            "mode": "new",
            "ticket": {},
            "assignees": assignees,
            "companies": companies,
        },
    )

# =========================================================
# 3) Create (BUGFIX: category check)
# =========================================================
@router.post("/create")
def ui_ticket_create(
    request: Request,
    company_id: str = Form(""),
    company_name: str = Form(""),
    platform_name: str = Form(""),
    group_name: str = Form(""),
    task_type: str = Form(""),
    category: str = Form(""),
    priority: str = Form("NORMAL"),
    due_at: str = Form(""),
    assignee_user_id: str = Form(""),
    title: str = Form(""),
    description: str = Form(""),
    remark: str = Form(""),
    attachments: List[UploadFile] = File(default=[]),
    images: List[UploadFile] = File(default=[]),
):
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    cid = _int_or_none(company_id)
    cname = (company_name or "").strip() or None
    desc = (description or "").strip() or None
    due = _dt_or_none(due_at)
    remark_s = (remark or "").strip() or None
    assignee_id = _int_or_none(assignee_user_id)

    title_s = (title or "").strip()
    group_val = (group_name or "").strip()
    if not group_val:
        return templates.TemplateResponse(
            "ticket_form.html",
            {
                **_base_ctx(request, current_user, "tickets"),
                "mode": "new",
                "ticket": {
                    "company_name": cname,
                    "platform_name": (platform_name or "").strip() or None,
                    "group_name": "",
                    "title": title_s,
                    "description": desc,
                    "remark": remark_s,
                    "priority": (priority or "NORMAL").strip().upper(),
                    "category": (task_type or category or "").strip(),
                    "assignee_user_id": _int_or_none(assignee_user_id),
                    "due_at": due_at,
                },
                "assignees": _list_active_users_for_assign(),
                "companies": [],
                "err": "任务组别不能为空，请选择。",
            },
            status_code=400,
        )

    if assignee_id is None:
        return templates.TemplateResponse(
            "ticket_form.html",
            {
                **_base_ctx(request, current_user, "tickets"),
                "mode": "new",
                "ticket": {
                    "company_name": cname,
                    "platform_name": (platform_name or "").strip() or None,
                    "group_name": group_val,
                    "title": title_s,
                    "description": desc,
                    "remark": remark_s,
                    "priority": (priority or "NORMAL").strip().upper(),
                    "category": (task_type or category or "").strip(),
                    "assignee_user_id": "",
                    "due_at": due_at,
                },
                "assignees": _list_active_users_for_assign(),
                "companies": [],
                "err": "负责人不能为空，请选择负责人。",
            },
            status_code=400,
        )
    if not title_s:
        return RedirectResponse("/ui/tickets/new", status_code=302)
    

    # ✅ FIX：只判断 category_val，不再用 if not category 这种错误判断
    category_val = (task_type or "").strip() or (category or "").strip()
    if not category_val:
        return templates.TemplateResponse(
            "ticket_form.html",
            {
                **_base_ctx(request, current_user, "tickets"),
                "mode": "new",
                "ticket": {
                    "company_name": cname,
                    "platform_name": (platform_name or "").strip() or None,
                    "group_name": (group_name or "").strip() or None,
                    "title": title_s,
                    "description": desc,
                    "remark": remark_s,
                    "priority": (priority or "NORMAL").strip().upper(),
                    "category": "",
                    "assignee_user_id": assignee_id,
                    "due_at": due_at,
                },
                "assignees": _list_active_users_for_assign(),
                "companies": [],
                "err": "任务类型（category）不能为空，请选择。",
            },
            status_code=400,
        )

    pr = (priority or "NORMAL").strip().upper()
    if pr not in ("URGENT", "NORMAL", "LOW"):
        pr = "NORMAL"

    st0 = "WAITING"

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO tickets
                (ticket_no, company_id, company_name, platform_name, group_name,
                 title, description, remark,
                 category, priority, status,
                 requester_user_id, assignee_user_id,
                 due_at, resolved_at, closed_at,
                 is_deleted, deleted_at, created_at, updated_at)
                VALUES
                ('TEMP', :company_id, :company_name, :platform_name, :group_name,
                 :title, :description, :remark,
                 :category, :priority, :status,
                 :requester_user_id, :assignee_user_id,
                 :due_at, NULL, NULL,
                 0, NULL, NOW(), NOW())
            """),
            {
                "company_id": cid,
                "company_name": cname,
                "platform_name": (platform_name or "").strip() or None,
                "group_name": (group_name or "").strip() or None,
                "title": title_s,
                "description": desc,
                "remark": remark_s,
                "category": category_val,
                "priority": pr,
                "status": st0,
                "group_name": group_val or None,
                "requester_user_id": int(current_user["id"]),
                "assignee_user_id": assignee_id,
                "due_at": due,
            },
        )

        new_id = int(conn.execute(text("SELECT LAST_INSERT_ID()")).scalar() or 0)
        if new_id <= 0:
            return RedirectResponse("/ui/tickets/new", status_code=302)

        ticket_no = _make_ticket_no_by_id(new_id)
        conn.execute(text("UPDATE tickets SET ticket_no=:no WHERE id=:id LIMIT 1"), {"no": ticket_no, "id": new_id})

        conn.execute(
            text("""
                INSERT INTO ticket_events
                (ticket_id, actor_user_id, event_type, from_status, to_status, payload_json, created_at)
                VALUES
                (:tid, :uid, 'created', NULL, 'NEW',
                 JSON_OBJECT('company_id', :cid, 'company_name', :cname, 'category', :cat),
                 NOW())
            """),
            {"tid": new_id, "uid": int(current_user["id"]), "cid": cid, "cname": cname, "cat": category_val},
        )

        base_dir = os.path.join("uploads", "tickets", str(new_id))
        files_dir = os.path.join(base_dir, "files")
        imgs_dir = os.path.join(base_dir, "images")
        os.makedirs(files_dir, exist_ok=True)
        os.makedirs(imgs_dir, exist_ok=True)

        def _save_one(f: UploadFile, kind: str) -> None:
            if not f or not f.filename:
                return
            ext = os.path.splitext(f.filename)[1]
            safe_name = uuid.uuid4().hex + ext
            target_dir = imgs_dir if kind == "image" else files_dir
            rel_path = os.path.join(target_dir, safe_name).replace("\\", "/")

            content = f.file.read()
            with open(rel_path, "wb") as out:
                out.write(content)

            conn.execute(
                text("""
                    INSERT INTO ticket_attachments
                      (ticket_id, kind, original_name, stored_path, mime_type, size_bytes, uploaded_by, created_at)
                    VALUES
                      (:tid, :kind, :oname, :spath, :mime, :sz, :uid, NOW())
                """),
                {
                    "tid": new_id,
                    "kind": kind,
                    "oname": f.filename,
                    "spath": rel_path,
                    "mime": getattr(f, "content_type", None),
                    "sz": len(content),
                    "uid": int(current_user["id"]),
                },
            )

        for f in attachments or []:
            _save_one(f, "file")
        for f in images or []:
            _save_one(f, "image")

    return RedirectResponse(url=f"/ui/tickets/{new_id}", status_code=302)

# =========================================================
# 4) Detail
# =========================================================
@router.get("/{ticket_id}", response_class=HTMLResponse)
def ui_ticket_detail(request: Request, ticket_id: int):
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return RedirectResponse("/ui/login", status_code=302)

    last_status_sql = _sql_last_status_mark_by_assignees("t")

    with engine.connect() as conn:
        ticket = conn.execute(
            text("""
                SELECT t.*,
                  COALESCE(u1.display_name, u1.username) AS requester_name,
                  COALESCE(u2.display_name, u2.username) AS primary_assignee_name,
                COALESCE(
                (SELECT MAX(tp.created_at) FROM ticket_progress tp WHERE tp.ticket_id=t.id),
                t.updated_at
                ) AS last_mark_at
                FROM tickets t
                LEFT JOIN users u1 ON u1.id = t.requester_user_id
                LEFT JOIN users u2 ON u2.id = t.assignee_user_id
                WHERE t.id=:id AND t.is_deleted=0
                LIMIT 1
            """),
            {"id": ticket_id},
        ).mappings().first()

        if not ticket:
            return templates.TemplateResponse(
                "not_found.html",
                {"request": request, "current_user": current_user},
                status_code=404,
            )

        ticket_dict = dict(ticket)

        if not _can_view_ticket(current_user, ticket_id, ticket_dict):
            return templates.TemplateResponse(
                "no_permission.html",
                {"request": request, "current_user": current_user, "message": "无权查看"},
                status_code=403,
            )

        progress_rows = conn.execute(
            text("""
                SELECT p.*,
                       COALESCE(u.display_name, u.username) AS user_name
                FROM ticket_progress p
                LEFT JOIN users u ON u.id=p.user_id
                WHERE p.ticket_id=:tid
                ORDER BY p.id DESC
                LIMIT 500
            """),
            {"tid": ticket_id},
        ).mappings().all()

        def _parse_ids(s):
            try:
                arr = json.loads(s or "[]")
                return [int(x) for x in arr if str(x).isdigit()]
            except Exception:
                return []

        all_p_aids: List[int] = []
        for p in progress_rows:
            all_p_aids.extend(_parse_ids(p.get("attachments")))

        progress_attachments_map: Dict[int, Dict[str, Any]] = {}
        if all_p_aids:
            uniq = sorted(set(all_p_aids))
            in_sql = ",".join(str(x) for x in uniq)
            arows = conn.execute(
                text(f"""
                    SELECT id, ticket_id, kind, original_name, stored_path, mime_type, size_bytes, created_at
                    FROM ticket_attachments
                    WHERE ticket_id=:tid AND id IN ({in_sql})
                """),
                {"tid": ticket_id},
            ).mappings().all()
            progress_attachments_map = {int(a["id"]): dict(a) for a in arows}

        assignees_rows = conn.execute(
            text("""
                SELECT u.id, u.username, u.display_name, u.role
                FROM ticket_assignees ta
                JOIN users u ON u.id = ta.user_id
                WHERE ta.ticket_id = :tid
            """),
            {"tid": ticket_id},
        ).mappings().all()
        current_assignees = [dict(r) for r in assignees_rows]

        if not current_assignees and ticket_dict.get("assignee_user_id"):
            u_old = conn.execute(
                text("SELECT id, username, display_name FROM users WHERE id=:uid"),
                {"uid": ticket_dict["assignee_user_id"]},
            ).mappings().first()
            if u_old:
                current_assignees.append(dict(u_old))

        comments = conn.execute(
            text("""
                SELECT c.*, COALESCE(u.display_name, u.username) as user_name
                FROM ticket_comments c
                LEFT JOIN users u ON u.id=c.user_id
                WHERE c.ticket_id=:tid
                ORDER BY c.id ASC
            """),
            {"tid": ticket_id},
        ).mappings().all()

        events = conn.execute(
            text("""
                SELECT e.*, COALESCE(u.display_name, u.username) as actor_name
                FROM ticket_events e
                LEFT JOIN users u ON u.id=e.actor_user_id
                WHERE e.ticket_id=:tid
                ORDER BY e.id DESC
                LIMIT 50
            """),
            {"tid": ticket_id},
        ).mappings().all()

        attachments = conn.execute(
            text("SELECT * FROM ticket_attachments WHERE ticket_id=:tid ORDER BY id DESC"),
            {"tid": ticket_id},
        ).mappings().all()

        all_users = _list_active_users_for_assign()

    return templates.TemplateResponse(
        "ticket_detail.html",
        {
            "request": request,
            "current_user": current_user,
            "ticket": ticket_dict,
            "current_assignees": current_assignees,
            "comments": comments,
            "events": events,
            "assignees": all_users,
            "attachments": attachments,
            "STATUSES": ALL_STATUSES,
            "STATUS_NEXT": STATUS_NEXT,
            "active": "tickets",
            "progress_rows": progress_rows,
            "progress_attachments_map": progress_attachments_map,
            "STATUS_LABELS_ZH": STATUS_LABELS_ZH,
        },
    )

# =========================================================
# 5) Comment
# =========================================================
@router.post("/{ticket_id}/comment")
def ui_ticket_add_comment(request: Request, ticket_id: int, body: str = Form(...)):
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    body_s = (body or "").strip()
    if not body_s:
        return RedirectResponse(url=f"/ui/tickets/{ticket_id}", status_code=302)

    with engine.begin() as conn:
        t = conn.execute(
            text("SELECT * FROM tickets WHERE id=:id AND is_deleted=0 LIMIT 1"),
            {"id": ticket_id},
        ).mappings().first()
        if not t:
            return RedirectResponse("/ui/tickets", status_code=302)

        t = dict(t)
        if not _can_view_ticket(current_user, ticket_id, t):
            return _render_no_permission(request, current_user, "tickets", "你没有权限在该工单下评论。", f"/ui/tickets/{ticket_id}")

        conn.execute(
            text("INSERT INTO ticket_comments (ticket_id, user_id, body, created_at) VALUES (:tid, :uid, :body, NOW())"),
            {"tid": ticket_id, "uid": int(current_user["id"]), "body": body_s},
        )
        conn.execute(
            text("""
                INSERT INTO ticket_events
                (ticket_id, actor_user_id, event_type, from_status, to_status, payload_json, created_at)
                VALUES
                (:tid, :uid, 'comment_added', NULL, NULL, JSON_OBJECT('len', :ln), NOW())
            """),
            {"tid": ticket_id, "uid": int(current_user["id"]), "ln": len(body_s)},
        )

    return RedirectResponse(url=f"/ui/tickets/{ticket_id}", status_code=302)

# =========================================================
# 6) Assign (multi)
# =========================================================
@router.post("/{ticket_id}/assign")
def ui_ticket_assign(
    request: Request,
    ticket_id: int,
    assignee_user_ids: List[int] = Form(default=[]),
):
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return RedirectResponse("/ui/login", status_code=302)

    actor_id = int(current_user["id"])

    with engine.begin() as conn:
        t = conn.execute(
            text("SELECT * FROM tickets WHERE id=:id AND is_deleted=0 LIMIT 1"),
            {"id": ticket_id},
        ).mappings().first()
        if not t:
            return RedirectResponse("/ui/tickets", status_code=302)
        t = dict(t)

        if not _can_operate_ticket(current_user, ticket_id, t):
            return _render_no_permission(request, current_user, "tickets", "你没有权限修改该工单的指派（仅管理员/创建人/被指派人可操作）。", f"/ui/tickets/{ticket_id}")

        new_ids: List[int] = []
        seen = set()
        for uid in assignee_user_ids or []:
            try:
                uid = int(uid)
            except Exception:
                continue
            if uid <= 0 or uid in seen:
                continue
            seen.add(uid)
            new_ids.append(uid)

        conn.execute(text("DELETE FROM ticket_assignees WHERE ticket_id=:tid"), {"tid": ticket_id})

        if new_ids:
            vals = [{"tid": ticket_id, "uid": uid} for uid in new_ids]
            conn.execute(text("INSERT INTO ticket_assignees (ticket_id, user_id) VALUES (:tid, :uid)"), vals)

        primary_assignee = new_ids[0] if new_ids else None
        conn.execute(
            text("UPDATE tickets SET assignee_user_id=:uid, updated_at=NOW() WHERE id=:tid"),
            {"uid": primary_assignee, "tid": ticket_id},
        )

        conn.execute(
            text("""
                INSERT INTO ticket_events (ticket_id, actor_user_id, event_type, payload_json, created_at)
                VALUES (:tid, :uid, 'assigned', :payload, NOW())
            """),
            {"tid": ticket_id, "uid": actor_id, "payload": json.dumps({"assigned_ids": new_ids}, ensure_ascii=False)},
        )

    return RedirectResponse(f"/ui/tickets/{ticket_id}", status_code=302)

# =========================================================
# 7) Status change
# =========================================================
@router.post("/{ticket_id}/status")
def ui_ticket_change_status(request: Request, ticket_id: int, to_status: str = Form("")):
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    to_s = (to_status or "").strip().upper()
    if not to_s:
        return RedirectResponse(url=f"/ui/tickets/{ticket_id}", status_code=302)

    with engine.begin() as conn:
        t = conn.execute(
            text("SELECT id, status, requester_user_id, assignee_user_id, is_deleted FROM tickets WHERE id=:id LIMIT 1"),
            {"id": ticket_id},
        ).mappings().first()

        if not t or int(t.get("is_deleted") or 0) == 1:
            return _redirect_back(request)

        t = dict(t)
        from_s = (t.get("status") or "").upper()

        if not _can_operate_ticket(current_user, ticket_id, t):
            return _render_no_permission(request, current_user, "tickets", "你没有权限变更该工单状态（仅管理员/创建人/被指派人可操作）。", f"/ui/tickets/{ticket_id}")

        allowed = STATUS_NEXT.get(from_s, [])
        if to_s not in allowed:
            return RedirectResponse(url=f"/ui/tickets/{ticket_id}", status_code=302)

        conn.execute(
            text("""
                UPDATE tickets
                SET status=:to_status,
                    updated_at=NOW(),
                    resolved_at = CASE WHEN :to_status='SOLVED' THEN NOW() ELSE resolved_at END,
                    closed_at   = CASE WHEN :to_status='CLOSED' THEN NOW() ELSE closed_at   END
                WHERE id=:id
                LIMIT 1
            """),
            {"to_status": to_s, "id": ticket_id},
        )

        conn.execute(
            text("""
                INSERT INTO ticket_events
                (ticket_id, actor_user_id, event_type, from_status, to_status, payload_json, created_at)
                VALUES
                (:tid, :uid, 'status_changed', :from_s, :to_s,
                 JSON_OBJECT('from', :from_s, 'to', :to_s),
                 NOW())
            """),
            {"tid": ticket_id, "uid": int(current_user["id"]), "from_s": from_s, "to_s": to_s},
        )

    return RedirectResponse(url=f"/ui/tickets/{ticket_id}", status_code=302)

# =========================================================
# 8) Batch
# =========================================================
@router.post("/batch")
def ui_ticket_batch(
    request: Request,
    ticket_ids: List[int] = Form(default=[]),
    to_status: str = Form(default=""),
    assignee_user_id: str = Form(default=""),
):
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not ticket_ids:
        return _redirect_back(request)

    to_s = (to_status or "").strip().upper()
    if to_s and to_s not in ALL_STATUSES:
        return _redirect_back(request)

    assign_raw = (assignee_user_id or "").strip()
    assign_mode = "noop"  # noop / set / clear
    new_assignee: Optional[int] = None

    if assign_raw == "":
        assign_mode = "noop"
    elif assign_raw == "0":
        assign_mode = "clear"
        new_assignee = None
    else:
        uid_val = _int_or_none(assign_raw)
        if uid_val is None:
            return _redirect_back(request)
        assign_mode = "set"
        new_assignee = uid_val

    if (not to_s) and (assign_mode == "noop"):
        return _redirect_back(request)

    actor_id = int(current_user["id"])

    with engine.begin() as conn:
        if assign_mode == "set":
            u = conn.execute(
                text("SELECT id FROM users WHERE id=:id AND status=1 LIMIT 1"),
                {"id": int(new_assignee)},
            ).first()
            if not u:
                return _redirect_back(request)

        for tid in ticket_ids:
            t = conn.execute(
                text("SELECT id, status, requester_user_id, assignee_user_id, is_deleted FROM tickets WHERE id=:id LIMIT 1"),
                {"id": int(tid)},
            ).mappings().first()

            if not t or int(t.get("is_deleted") or 0) == 1:
                continue

            t = dict(t)
            if not _can_operate_ticket(current_user, int(tid), t):
                continue

            if to_s:
                from_s = (t.get("status") or "").upper()
                allowed = STATUS_NEXT.get(from_s, [])
                if to_s in allowed:
                    conn.execute(
                        text("""
                            UPDATE tickets
                            SET status=:to_status,
                                updated_at=NOW(),
                                resolved_at = CASE WHEN :to_status='SOLVED' THEN NOW() ELSE resolved_at END,
                                closed_at   = CASE WHEN :to_status='CLOSED' THEN NOW() ELSE closed_at   END
                            WHERE id=:id
                            LIMIT 1
                        """),
                        {"to_status": to_s, "id": int(tid)},
                    )
                    conn.execute(
                        text("""
                            INSERT INTO ticket_events
                            (ticket_id, actor_user_id, event_type, from_status, to_status, payload_json, created_at)
                            VALUES
                            (:tid, :uid, 'batch_status_changed', :from_s, :to_s,
                             JSON_OBJECT('batch', 1, 'from', :from_s, 'to', :to_s),
                             NOW())
                        """),
                        {"tid": int(tid), "uid": actor_id, "from_s": from_s, "to_s": to_s},
                    )

            if assign_mode != "noop":
                old_aid = t.get("assignee_user_id")
                old_aid = int(old_aid) if old_aid is not None else None
                target_aid = int(new_assignee) if assign_mode == "set" else None
                if old_aid == target_aid:
                    continue

                conn.execute(
                    text("UPDATE tickets SET assignee_user_id=:aid, updated_at=NOW() WHERE id=:id LIMIT 1"),
                    {"aid": target_aid, "id": int(tid)},
                )
                conn.execute(
                    text("""
                        INSERT INTO ticket_events
                        (ticket_id, actor_user_id, event_type, from_status, to_status, payload_json, created_at)
                        VALUES
                        (:tid, :uid, 'batch_assigned', NULL, NULL,
                         JSON_OBJECT('batch', 1, 'from_assignee_user_id', :from_aid, 'to_assignee_user_id', :to_aid),
                         NOW())
                    """),
                    {"tid": int(tid), "uid": actor_id, "from_aid": old_aid, "to_aid": target_aid},
                )

    return _redirect_back_to_list_keep_query(request)

# =========================================================
# 9) Attachment get
# =========================================================
@router.get("/{ticket_id}/attachment/{attachment_id}")
def ui_ticket_attachment_get(
    request: Request,
    ticket_id: int,
    attachment_id: int,
    inline: str | None = Query(default=""),
):
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    with engine.connect() as conn:
        t = conn.execute(
            text("""
                SELECT t.*
                FROM tickets t
                WHERE t.id=:id AND t.is_deleted=0
                LIMIT 1
            """),
            {"id": ticket_id},
        ).mappings().first()
        if not t:
            return _redirect("/ui/tickets")
        t = dict(t)

        if not _can_view_ticket(current_user, ticket_id, t):
            return _render_no_permission(request, current_user, "tickets", "你没有权限查看该工单附件。", f"/ui/tickets/{ticket_id}")

        a = conn.execute(
            text("""
                SELECT id, ticket_id, kind, original_name, stored_path, mime_type, size_bytes, created_at
                FROM ticket_attachments
                WHERE id=:aid AND ticket_id=:tid
                LIMIT 1
            """),
            {"aid": attachment_id, "tid": ticket_id},
        ).mappings().first()
        if not a:
            return _redirect(f"/ui/tickets/{ticket_id}")
        a = dict(a)

    path = a.get("stored_path") or ""
    if not path or (not os.path.exists(path)):
        return _redirect(f"/ui/tickets/{ticket_id}")

    filename = a.get("original_name") or os.path.basename(path)
    media_type = a.get("mime_type") or "application/octet-stream"
    is_inline = (inline or "").strip() == "1"
    disp = "inline" if is_inline else "attachment"
    safe_filename = quote(filename)

    return FileResponse(
        path=path,
        media_type=media_type,
        filename=filename,
        headers={"Content-Disposition": f"{disp}; filename*=UTF-8''{safe_filename}"},
    )

# =========================================================
# 10) Upload attachment (manage only)
# =========================================================
@router.post("/{ticket_id}/upload")
async def ui_ticket_upload(
    request: Request,
    ticket_id: int,
    files: List[UploadFile] = File(default=[]),
):
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not files:
        return RedirectResponse(url=f"/ui/tickets/{ticket_id}", status_code=302)

    with engine.begin() as conn:
        t = conn.execute(
            text("SELECT * FROM tickets WHERE id=:id AND is_deleted=0 LIMIT 1"),
            {"id": ticket_id},
        ).mappings().first()
        if not t:
            return _redirect("/ui/tickets")
        t = dict(t)

        if not _can_manage_ticket(current_user, t):
            return _render_no_permission(request, current_user, "tickets", "你没有权限上传附件（仅主负责人/管理员可操作）。", f"/ui/tickets/{ticket_id}")

        base_dir = os.path.join("uploads", "tickets", str(ticket_id))
        files_dir = os.path.join(base_dir, "files")
        imgs_dir = os.path.join(base_dir, "images")
        os.makedirs(files_dir, exist_ok=True)
        os.makedirs(imgs_dir, exist_ok=True)

        def _guess_kind(upload: UploadFile) -> str:
            ct = (upload.content_type or "").lower()
            return "image" if ct.startswith("image/") else "file"

        saved = 0
        for f in files:
            if not f or not f.filename:
                continue

            kind = _guess_kind(f)
            ext = os.path.splitext(f.filename)[1]
            safe_name = uuid.uuid4().hex + ext
            target_dir = imgs_dir if kind == "image" else files_dir
            rel_path = os.path.join(target_dir, safe_name).replace("\\", "/")

            content = await f.read()
            with open(rel_path, "wb") as out:
                out.write(content)

            conn.execute(
                text("""
                    INSERT INTO ticket_attachments
                      (ticket_id, kind, original_name, stored_path, mime_type, size_bytes, uploaded_by, created_at)
                    VALUES
                      (:tid, :kind, :oname, :spath, :mime, :sz, :uid, NOW())
                """),
                {
                    "tid": ticket_id,
                    "kind": kind,
                    "oname": f.filename,
                    "spath": rel_path,
                    "mime": f.content_type,
                    "sz": len(content),
                    "uid": int(current_user["id"]),
                },
            )
            saved += 1

        conn.execute(
            text("""
                INSERT INTO ticket_events
                (ticket_id, actor_user_id, event_type, from_status, to_status, payload_json, created_at)
                VALUES
                (:tid, :uid, 'attachment_uploaded', NULL, NULL,
                 JSON_OBJECT('count', :cnt),
                 NOW())
            """),
            {"tid": ticket_id, "uid": int(current_user["id"]), "cnt": saved},
        )

    return RedirectResponse(url=f"/ui/tickets/{ticket_id}", status_code=302)

# =========================================================
# 11) Attachment delete (hard)
# =========================================================
@router.post("/{ticket_id}/attachment/{attachment_id}/delete")
def ui_ticket_attachment_delete(request: Request, ticket_id: int, attachment_id: int):
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    with engine.begin() as conn:
        t = conn.execute(
            text("SELECT * FROM tickets WHERE id=:id AND is_deleted=0 LIMIT 1"),
            {"id": ticket_id},
        ).mappings().first()
        if not t:
            return _redirect("/ui/tickets")
        t = dict(t)

        if not _can_manage_ticket(current_user, t):
            return _render_no_permission(request, current_user, "tickets", "你没有权限删除附件（仅主负责人/管理员可操作）。", f"/ui/tickets/{ticket_id}")

        a = conn.execute(
            text("SELECT id, stored_path, original_name FROM ticket_attachments WHERE id=:aid AND ticket_id=:tid LIMIT 1"),
            {"aid": attachment_id, "tid": ticket_id},
        ).mappings().first()
        if not a:
            return RedirectResponse(url=f"/ui/tickets/{ticket_id}", status_code=302)
        a = dict(a)
        old_path = (a.get("stored_path") or "").replace("\\", "/")

        conn.execute(
            text("DELETE FROM ticket_attachments WHERE id=:aid AND ticket_id=:tid LIMIT 1"),
            {"aid": attachment_id, "tid": ticket_id},
        )

        conn.execute(
            text("""
                INSERT INTO ticket_events
                (ticket_id, actor_user_id, event_type, payload_json, created_at)
                VALUES
                (:tid, :uid, 'attachment_deleted',
                 JSON_OBJECT('attachment_id', :aid, 'name', :name),
                 NOW())
            """),
            {"tid": ticket_id, "uid": int(current_user["id"]), "aid": attachment_id, "name": a.get("original_name")},
        )

    try:
        must_prefix = f"uploads/tickets/{ticket_id}/"
        if old_path.startswith(must_prefix) and os.path.exists(old_path):
            os.remove(old_path)
    except Exception:
        pass

    return RedirectResponse(url=f"/ui/tickets/{ticket_id}", status_code=302)

# =========================================================
# 12) Attachment replace
# =========================================================
@router.post("/{ticket_id}/attachment/{attachment_id}/replace")
async def ui_ticket_attachment_replace(
    request: Request,
    ticket_id: int,
    attachment_id: int,
    file: UploadFile = File(...),
):
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not file or not file.filename:
        return RedirectResponse(url=f"/ui/tickets/{ticket_id}", status_code=302)

    content = await file.read()
    if not content:
        return RedirectResponse(url=f"/ui/tickets/{ticket_id}", status_code=302)

    new_mime = (file.content_type or "").lower()
    new_kind = "image" if new_mime.startswith("image/") else "file"

    with engine.begin() as conn:
        t = conn.execute(
            text("SELECT * FROM tickets WHERE id=:id AND is_deleted=0 LIMIT 1"),
            {"id": ticket_id},
        ).mappings().first()
        if not t:
            return _redirect("/ui/tickets")
        t = dict(t)

        if not _can_manage_ticket(current_user, t):
            return _render_no_permission(request, current_user, "tickets", "你没有权限替换附件（仅主负责人/管理员可操作）。", f"/ui/tickets/{ticket_id}")

        a = conn.execute(
            text("SELECT id, kind, original_name, stored_path, mime_type FROM ticket_attachments WHERE id=:aid AND ticket_id=:tid LIMIT 1"),
            {"aid": attachment_id, "tid": ticket_id},
        ).mappings().first()
        if not a:
            return RedirectResponse(url=f"/ui/tickets/{ticket_id}", status_code=302)
        a = dict(a)
        old_path = (a.get("stored_path") or "").replace("\\", "/")

        base_dir = os.path.join("uploads", "tickets", str(ticket_id))
        files_dir = os.path.join(base_dir, "files")
        imgs_dir = os.path.join(base_dir, "images")
        os.makedirs(files_dir, exist_ok=True)
        os.makedirs(imgs_dir, exist_ok=True)

        ext = os.path.splitext(file.filename)[1]
        safe_name = uuid.uuid4().hex + ext
        target_dir = imgs_dir if new_kind == "image" else files_dir
        new_rel_path = os.path.join(target_dir, safe_name).replace("\\", "/")

        with open(new_rel_path, "wb") as out:
            out.write(content)

        conn.execute(
            text("""
                UPDATE ticket_attachments
                SET kind=:kind,
                    original_name=:oname,
                    stored_path=:spath,
                    mime_type=:mime,
                    size_bytes=:sz
                WHERE id=:aid AND ticket_id=:tid
                LIMIT 1
            """),
            {
                "kind": new_kind,
                "oname": file.filename,
                "spath": new_rel_path,
                "mime": new_mime or None,
                "sz": len(content),
                "aid": attachment_id,
                "tid": ticket_id,
            },
        )

        conn.execute(
            text("""
                INSERT INTO ticket_events
                (ticket_id, actor_user_id, event_type, payload_json, created_at)
                VALUES
                (:tid, :uid, 'attachment_replaced',
                 JSON_OBJECT('attachment_id', :aid, 'from', :from_name, 'to', :to_name),
                 NOW())
            """),
            {
                "tid": ticket_id,
                "uid": int(current_user["id"]),
                "aid": attachment_id,
                "from_name": a.get("original_name"),
                "to_name": file.filename,
            },
        )

    try:
        must_prefix = f"uploads/tickets/{ticket_id}/"
        if old_path.startswith(must_prefix) and os.path.exists(old_path):
            os.remove(old_path)
    except Exception:
        pass

    return RedirectResponse(url=f"/ui/tickets/{ticket_id}", status_code=302)

# =========================================================
# 13) Delete ticket (soft)
# =========================================================
@router.post("/{ticket_id}/delete")
def ui_ticket_delete(request: Request, ticket_id: int):
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    with engine.begin() as conn:
        t = conn.execute(
            text("SELECT * FROM tickets WHERE id=:id AND is_deleted=0 LIMIT 1"),
            {"id": ticket_id},
        ).mappings().first()
        if not t:
            return _redirect("/ui/tickets")
        t = dict(t)

        if not _can_manage_ticket(current_user, t):
            return _render_no_permission(request, current_user, "tickets", "你没有权限删除工单（仅主负责人/管理员可操作）。", f"/ui/tickets/{ticket_id}")

        conn.execute(
            text("UPDATE tickets SET is_deleted=1, deleted_at=NOW(), updated_at=NOW() WHERE id=:id LIMIT 1"),
            {"id": ticket_id},
        )

        conn.execute(
            text("""
                INSERT INTO ticket_events
                (ticket_id, actor_user_id, event_type, payload_json, created_at)
                VALUES
                (:tid, :uid, 'ticket_deleted',
                 JSON_OBJECT('ticket_id', :tid),
                 NOW())
            """),
            {"tid": ticket_id, "uid": int(current_user["id"])},
        )

    return _redirect("/ui/tickets")

# =========================================================
# 14/15) Edit ticket + save assignees
# =========================================================
def _save_assignees(conn, ticket_id: int, assignee_ids: list[int]):
    conn.execute(text("DELETE FROM ticket_assignees WHERE ticket_id=:tid"), {"tid": ticket_id})
    if assignee_ids:
        vals = [{"tid": ticket_id, "uid": int(uid)} for uid in assignee_ids]
        conn.execute(text("INSERT INTO ticket_assignees (ticket_id, user_id) VALUES (:tid, :uid)"), vals)
    primary_uid = assignee_ids[0] if assignee_ids else None
    conn.execute(text("UPDATE tickets SET assignee_user_id=:uid WHERE id=:tid"), {"uid": primary_uid, "tid": ticket_id})

@router.get("/{ticket_id}/edit", response_class=HTMLResponse)
def ui_ticket_edit(request: Request, ticket_id: int):
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    with engine.connect() as conn:
        ticket = conn.execute(text("SELECT * FROM tickets WHERE id=:id AND is_deleted=0 LIMIT 1"), {"id": ticket_id}).mappings().first()
        if not ticket:
            return templates.TemplateResponse("not_found.html", {**_base_ctx(request, current_user, "tickets")}, status_code=200)

        ticket_dict = dict(ticket)
        if not _can_operate_ticket(current_user, ticket_id, ticket_dict):
            return _render_no_permission(request, current_user, "tickets", "你没有权限修改该工单。", f"/ui/tickets/{ticket_id}")

    assignees = _list_active_users_for_assign()
    companies: List[Dict[str, Any]] = []
    try:
        with engine.connect() as conn2:
            cols = conn2.execute(text("SHOW COLUMNS FROM companies")).mappings().all()
            colnames = [str(c["Field"]) for c in cols]
            candidates = [
                "company_name", "name", "title", "display_name", "full_name",
                "legal_name", "short_name", "company", "name_cn", "name_en",
            ]
            name_col = next((c for c in candidates if c in colnames), None)
            if not name_col:
                rows = conn2.execute(text("SELECT id FROM companies ORDER BY id DESC LIMIT 1000")).mappings().all()
                companies = [{"id": int(r["id"]), "name": f"Company #{int(r['id'])}"} for r in rows]
            else:
                rows = conn2.execute(text(f"SELECT id, `{name_col}` AS name FROM companies ORDER BY id DESC LIMIT 1000")).mappings().all()
                companies = []
                for r in rows:
                    cid = int(r["id"])
                    nm = (r.get("name") or "").strip()
                    companies.append({"id": cid, "name": nm or f"Company #{cid}"})
    except Exception:
        companies = []

    return templates.TemplateResponse(
        "ticket_form.html",
        {**_base_ctx(request, current_user, "tickets"), "mode": "edit", "ticket": ticket_dict, "assignees": assignees, "companies": companies},
    )

@router.post("/{ticket_id}/edit")
def ui_ticket_edit_post(
    request: Request,
    ticket_id: int,
    company_id: str = Form(""),
    company_name: str = Form(""),
    group_name: str = Form(""),
    task_type: str = Form(""),
    priority: str = Form("NORMAL"),   # ✅ 新增
    description: str = Form(""),
    due_at: str = Form(""),
    assignee_user_ids: List[int] = Form([]),
    remark: str = Form(""),
    title: str = Form(""),
):
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    pr = (priority or "NORMAL").strip().upper()
    if pr not in ("URGENT", "NORMAL", "LOW"):
        pr = "NORMAL"

    with engine.begin() as conn:
        t = conn.execute(
            text("SELECT * FROM tickets WHERE id=:id AND is_deleted=0 LIMIT 1"),
            {"id": ticket_id}
        ).mappings().first()

        if not t:
            return _redirect("/ui/tickets")

        t = dict(t)

        if not _can_operate_ticket(current_user, ticket_id, t):
            return _render_no_permission(
                request,
                current_user,
                "tickets",
                "你没有权限修改该工单。",
                f"/ui/tickets/{ticket_id}"
            )

        conn.execute(
            text("""
                UPDATE tickets SET
                  company_id=:cid,
                  company_name=:cname,
                  group_name=:gname,
                  title=:title,
                  description=:desc,
                  remark=:remark,
                  category=:cat,
                  priority=:priority,   -- ✅ 关键更新
                  due_at=:due,
                  updated_at=NOW()
                WHERE id=:id AND is_deleted=0
            """),
            {
                "cid": _int_or_none(company_id),
                "cname": (company_name or "").strip() or None,
                "gname": (group_name or "").strip() or None,
                "title": (title or "").strip(),
                "desc": (description or "").strip() or None,
                "remark": (remark or "").strip() or None,
                "cat": (task_type or "").strip() or None,
                "priority": pr,  # ✅ 写入数据库
                "due": _dt_or_none(due_at),
                "id": ticket_id,
            },
        )

        # 多负责人
        _save_assignees(
            conn,
            ticket_id,
            [int(x) for x in (assignee_user_ids or []) if int(x) > 0]
        )

        conn.execute(
            text("""
                INSERT INTO ticket_events
                  (ticket_id, actor_user_id, event_type, created_at)
                VALUES
                  (:tid, :uid, 'edited', NOW())
            """),
            {"tid": ticket_id, "uid": int(current_user["id"])}
        )

    return RedirectResponse(url=f"/ui/tickets/{ticket_id}", status_code=302)

# =========================================================
@router.post("/{ticket_id}/progress")
async def ui_ticket_add_progress(
    request: Request,
    ticket_id: int,
    content: str = Form(""),
    new_status: str = Form(""),
    files: List[UploadFile] = File(default=[]),
):
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    content_s = (content or "").strip()
    if not content_s:
        return RedirectResponse(url=f"/ui/tickets/{ticket_id}", status_code=302)

    to_s = (new_status or "").strip().upper()
    if to_s and to_s not in ALL_STATUSES:
        to_s = ""

    with engine.begin() as conn:
        t = conn.execute(text("SELECT * FROM tickets WHERE id=:id AND is_deleted=0 LIMIT 1"), {"id": ticket_id}).mappings().first()
        if not t:
            return _redirect("/ui/tickets")
        t = dict(t)

        if not _can_operate_ticket(current_user, ticket_id, t):
            return _render_no_permission(request, current_user, "tickets", "你没有权限提交该工单进度（仅管理员/创建人/被指派人可操作）。", f"/ui/tickets/{ticket_id}")

        old_s = (t.get("status") or "").upper()

        base_dir = os.path.join("uploads", "tickets", str(ticket_id))
        files_dir = os.path.join(base_dir, "files")
        imgs_dir = os.path.join(base_dir, "images")
        os.makedirs(files_dir, exist_ok=True)
        os.makedirs(imgs_dir, exist_ok=True)

        def _guess_kind(upload: UploadFile) -> str:
            ct = (upload.content_type or "").lower()
            return "image" if ct.startswith("image/") else "file"

        attachment_ids: list[int] = []
        for f in files or []:
            if not f or not f.filename:
                continue
            kind = _guess_kind(f)
            ext = os.path.splitext(f.filename)[1]
            safe_name = uuid.uuid4().hex + ext
            target_dir = imgs_dir if kind == "image" else files_dir
            rel_path = os.path.join(target_dir, safe_name).replace("\\", "/")

            data = await f.read()
            with open(rel_path, "wb") as out:
                out.write(data)

            conn.execute(
                text("""
                    INSERT INTO ticket_attachments
                      (ticket_id, kind, original_name, stored_path, mime_type, size_bytes, uploaded_by, created_at)
                    VALUES
                      (:tid, :kind, :oname, :spath, :mime, :sz, :uid, NOW())
                """),
                {"tid": ticket_id, "kind": kind, "oname": f.filename, "spath": rel_path, "mime": f.content_type, "sz": len(data), "uid": int(current_user["id"])},
            )
            aid = int(conn.execute(text("SELECT LAST_INSERT_ID()")).scalar() or 0)
            if aid:
                attachment_ids.append(aid)

        conn.execute(
            text("""
                INSERT INTO ticket_progress
                  (ticket_id, user_id, content, old_status, new_status, attachments, created_at)
                VALUES
                  (:tid, :uid, :content, :old_s, :new_s, :atts, NOW())
            """),
            {"tid": ticket_id, "uid": int(current_user["id"]), "content": content_s, "old_s": old_s or None, "new_s": to_s or None, "atts": json.dumps(attachment_ids, ensure_ascii=False)},
        )

        if to_s and to_s != old_s:
            conn.execute(
                text("""
                    UPDATE tickets
                    SET status=:st,
                        updated_at=NOW(),
                        resolved_at = CASE WHEN :st='SOLVED' THEN NOW() ELSE resolved_at END,
                        closed_at   = CASE WHEN :st='CLOSED' THEN NOW() ELSE closed_at   END
                    WHERE id=:id
                """),
                {"st": to_s, "id": ticket_id},
            )

        conn.execute(
            text("""
                INSERT INTO ticket_events
                (ticket_id, actor_user_id, event_type, from_status, to_status, payload_json, created_at)
                VALUES
                (:tid, :uid, 'progress_added', :from_s, :to_s,
                 JSON_OBJECT('attachments', :cnt),
                 NOW())
            """),
            {"tid": ticket_id, "uid": int(current_user["id"]), "from_s": old_s or None, "to_s": to_s or None, "cnt": len(attachment_ids)},
        )

    return RedirectResponse(url=f"/ui/tickets/{ticket_id}", status_code=302)

@router.post("/{ticket_id}/progress/{progress_id}/delete")
def ui_ticket_delete_progress(request: Request, ticket_id: int, progress_id: int):
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    with engine.begin() as conn:
        t = conn.execute(text("SELECT * FROM tickets WHERE id=:id AND is_deleted=0 LIMIT 1"), {"id": ticket_id}).mappings().first()
        if not t:
            return _redirect("/ui/tickets")
        t = dict(t)

        if not _can_operate_ticket(current_user, ticket_id, t):
            return _render_no_permission(request, current_user, "tickets", "你没有权限删除处理进度（仅管理员/创建人/被指派人可操作）。", f"/ui/tickets/{ticket_id}")

        p = conn.execute(text("SELECT id, ticket_id FROM ticket_progress WHERE id=:pid LIMIT 1"), {"pid": progress_id}).mappings().first()
        if not p or int(p["ticket_id"]) != int(ticket_id):
            return RedirectResponse(url=f"/ui/tickets/{ticket_id}", status_code=302)

        conn.execute(text("DELETE FROM ticket_progress WHERE id=:pid LIMIT 1"), {"pid": progress_id})

        conn.execute(
            text("""
                INSERT INTO ticket_events
                (ticket_id, actor_user_id, event_type, payload_json, created_at)
                VALUES
                (:tid, :uid, 'progress_deleted', JSON_OBJECT('progress_id', :pid), NOW())
            """),
            {"tid": ticket_id, "uid": int(current_user["id"]), "pid": progress_id},
        )

    return RedirectResponse(url=f"/ui/tickets/{ticket_id}", status_code=302)
