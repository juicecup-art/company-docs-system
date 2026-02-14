# app/routers/admin.py
from __future__ import annotations

from fastapi import APIRouter, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from starlette.status import HTTP_303_SEE_OTHER

from sqlalchemy import text, bindparam

from app.db import engine
from app.routers.ui import _get_current_user_for_ui, templates
from app.auth.password import get_password_hash

router = APIRouter(prefix="/ui/admin", tags=["ui-admin"])

# =========================
# Helpers
# =========================
def _require_admin(user: dict | None):
    if not user or user.get("role") != "admin":
        # UI 场景你也可以改成 render no_permission.html
        raise HTTPException(status_code=403, detail="Admin only")


def _redir(url: str):
    return RedirectResponse(url, status_code=HTTP_303_SEE_OTHER)


def _to_int(v, default=0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _get_user_row(user_id: int):
    with engine.connect() as conn:
        return conn.execute(
            text(
                """
                SELECT id, username, display_name, email, phone, department, role, status,
                       last_login_at, created_at, updated_at
                FROM users
                WHERE id=:id
                LIMIT 1
                """
            ),
            {"id": user_id},
        ).mappings().first()
    
def _normalize_country(c: str | None) -> str:
    c = (c or "").strip().upper()
    return c or "OTHER"



# =========================
# Users list
# GET /ui/admin/users
# =========================
@router.get("/users")
def ui_admin_users(request: Request):
    current_user = _get_current_user_for_ui(request)
    _require_admin(current_user)

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id, username, display_name, email, phone, department, role, status, created_at, updated_at
                FROM users
                ORDER BY id DESC
                LIMIT 500
                """
            )
        ).mappings().all()

    return templates.TemplateResponse(
        "admin_users.html",
        {
            "request": request,
            "current_user": current_user,
            "users": rows,
        },
    )


# =========================
# New user (GET/POST)
# =========================
@router.get("/users/new")
def ui_admin_user_new(request: Request):
    current_user = _get_current_user_for_ui(request)
    _require_admin(current_user)

    return templates.TemplateResponse(
        "admin_user_form.html",
        {
            "request": request,
            "current_user": current_user,
            "mode": "new",
            "row": {},
            "error": None,
        },
    )


@router.post("/users/new")
def ui_admin_user_create(
    request: Request,
    username: str = Form(...),
    display_name: str = Form(""),
    email: str = Form(""),
    phone: str = Form(""),
    department: str = Form(""),
    role: str = Form("user"),
    status: int = Form(1),
    password: str = Form(...),  # ✅ 明文密码
):
    current_user = _get_current_user_for_ui(request)
    _require_admin(current_user)

    username = (username or "").strip()
    password = (password or "").strip()

    row_back = {
        "username": username,
        "display_name": display_name,
        "email": email,
        "phone": phone,
        "department": department,
        "role": role,
        "status": status,
    }

    if not username:
        return templates.TemplateResponse(
            "admin_user_form.html",
            {
                "request": request,
                "current_user": current_user,
                "mode": "new",
                "row": row_back,
                "error": "用户名不能为空",
            },
        )

    if not password or len(password) < 6:
        return templates.TemplateResponse(
            "admin_user_form.html",
            {
                "request": request,
                "current_user": current_user,
                "mode": "new",
                "row": row_back,
                "error": "密码不能为空，且建议至少 6 位",
            },
        )

    pw_hash = get_password_hash(password)  # ✅ 生成 bcrypt hash

    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO users
                      (username, display_name, email, phone, department, role, status, password_hash, created_at, updated_at)
                    VALUES
                      (:username, :display_name, :email, :phone, :department, :role, :status, :password_hash, NOW(), NOW())
                    """
                ),
                {
                    "username": username,
                    "display_name": (display_name or "").strip() or username, 
                    "email": (email or "").strip() or None,
                    "phone": (phone or "").strip() or None,
                    "department": (department or "").strip(),
                    "role": (role or "user").strip(),
                    "status": _to_int(status, 1),
                    "password_hash": pw_hash,
                },
            )
    except Exception as e:
        return templates.TemplateResponse(
            "admin_user_form.html",
            {
                "request": request,
                "current_user": current_user,
                "mode": "new",
                "row": row_back,
                "error": f"创建失败（可能用户名重复）：{str(e)}",
            },
        )

    return _redir("/ui/admin/users")


# =========================
# Edit user (GET/POST)
# =========================
@router.get("/users/{user_id}/edit")
def ui_admin_user_edit(request: Request, user_id: int):
    current_user = _get_current_user_for_ui(request)
    _require_admin(current_user)

    row = _get_user_row(user_id)
    if not row:
        return _redir("/ui/admin/users")

    return templates.TemplateResponse(
        "admin_user_form.html",
        {
            "request": request,
            "current_user": current_user,
            "mode": "edit",
            "row": dict(row),
            "error": None,
        },
    )


@router.post("/users/{user_id}/edit")
def ui_admin_user_update(
    request: Request,
    user_id: int,
    display_name: str = Form(""),
    email: str = Form(""),
    phone: str = Form(""),
    department: str = Form(""),
    role: str = Form("user"),
    status: int = Form(1),
):
    current_user = _get_current_user_for_ui(request)
    _require_admin(current_user)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE users SET
                  display_name=:display_name,
                  email=:email,
                  phone=:phone,
                  department=:department,
                  role=:role,
                  status=:status,
                  updated_at=NOW()
                WHERE id=:id
                LIMIT 1
                """
            ),
            {
                "id": user_id,
                "display_name": (display_name or "").strip() or "Unnamed",
                "email": (email or "").strip() or None,
                "phone": (phone or "").strip() or None,
                "department": (department or "").strip(),
                "role": (role or "user").strip(),
                "status": _to_int(status, 1),
            },
        )

    return _redir("/ui/admin/users")


# =========================
# Admin change password (GET/POST)
# 仅管理员可改任何人的密码
# =========================
@router.get("/users/{user_id}/password")
def ui_admin_user_password_page(request: Request, user_id: int):
    current_user = _get_current_user_for_ui(request)
    _require_admin(current_user)

    row = _get_user_row(user_id)
    if not row:
        return _redir("/ui/admin/users")

    return templates.TemplateResponse(
        "admin_user_password.html",
        {
            "request": request,
            "current_user": current_user,
            "row": dict(row),
            "error": None,
            "ok": None,
        },
    )


@router.post("/users/{user_id}/password")
def ui_admin_user_password_save(
    request: Request,
    user_id: int,
    new_password: str = Form(...),
):
    current_user = _get_current_user_for_ui(request)
    _require_admin(current_user)

    row = _get_user_row(user_id)
    if not row:
        return _redir("/ui/admin/users")

    new_password = (new_password or "").strip()
    if not new_password or len(new_password) < 6:
        return templates.TemplateResponse(
            "admin_user_password.html",
            {
                "request": request,
                "current_user": current_user,
                "row": dict(row),
                "error": "新密码不能为空，且建议至少 6 位",
                "ok": None,
            },
        )

    pw_hash = get_password_hash(new_password)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE users
                SET password_hash=:ph, updated_at=NOW()
                WHERE id=:id
                LIMIT 1
                """
            ),
            {"ph": pw_hash, "id": user_id},
        )

    # 回到同页面给提示
    row2 = _get_user_row(user_id)
    return templates.TemplateResponse(
        "admin_user_password.html",
        {
            "request": request,
            "current_user": current_user,
            "row": dict(row2) if row2 else dict(row),
            "error": None,
            "ok": "密码已更新",
        },
    )


# =========================
# Delete user (POST)
# POST /ui/admin/users/{user_id}/delete
# =========================
@router.post("/users/{user_id}/delete")
def ui_admin_user_delete(request: Request, user_id: int):
    current_user = _get_current_user_for_ui(request)
    _require_admin(current_user)

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM user_company_permissions WHERE user_id=:uid"), {"uid": user_id})
        conn.execute(text("DELETE FROM users WHERE id=:id LIMIT 1"), {"id": user_id})

    return _redir("/ui/admin/users")


# =========================
# Company permissions (GET/POST)
# =========================
@router.get("/users/{user_id}/companies")
def ui_admin_user_companies(request: Request, user_id: int):
    current_user = _get_current_user_for_ui(request)
    _require_admin(current_user)

    with engine.connect() as conn:
        user_row = conn.execute(
            text(
                """
                SELECT id, username, display_name, email, role, status
                FROM users WHERE id=:id
                LIMIT 1
                """
            ),
            {"id": user_id},
        ).mappings().first()

        if not user_row:
            return _redir("/ui/admin/users")

        companies = conn.execute(
            text(
                """
                SELECT id, company_name
                FROM companies
                WHERE deleted_at IS NULL
                ORDER BY company_name
                """
            )
        ).mappings().all()

    perms = conn.execute(
        text(
            """
            SELECT company_id, can_view, can_edit, can_docs
            FROM user_company_permissions
            WHERE user_id=:uid
            """
        ),
        {"uid": user_id},
    ).mappings().all()

    perm_map = {p["company_id"]: p for p in perms}

    return templates.TemplateResponse(
        "admin_user_companies.html",
        {
            "request": request,
            "current_user": current_user,
            "user_row": dict(user_row),
            "companies": companies,
            "perm_map": perm_map,
            "error": None,
        },
    )


@router.post("/users/{user_id}/companies")
async def ui_admin_user_companies_save(request: Request, user_id: int):
    current_user = _get_current_user_for_ui(request)
    _require_admin(current_user)

    form = await request.form()
    company_ids = form.getlist("company_ids")  # 页面里每行一个 hidden company_ids

    with engine.begin() as conn:
        # 先全删再重建：简单、确定、不留脏数据
        conn.execute(text("DELETE FROM user_company_permissions WHERE user_id=:uid"), {"uid": user_id})

        for cid_str in company_ids:
            cid = _to_int(cid_str, 0)
            if cid <= 0:
                continue

            v = 1 if form.get(f"can_view_{cid}") == "on" else 0
            e = 1 if form.get(f"can_edit_{cid}") == "on" else 0
            d = 1 if form.get(f"can_docs_{cid}") == "on" else 0

            # 规则：edit => view；docs => view
            if e == 1:
                v = 1
            if d == 1:
                v = 1

            # 三个都没勾就不写
            if v == 0 and e == 0 and d == 0:
                continue

            conn.execute(
                text(
                    """
                    INSERT INTO user_company_permissions
                        (user_id, company_id, can_view, can_edit, can_docs, created_at)
                    VALUES
                        (:uid, :cid, :v, :e, :d, NOW())
                    """
                ),
                {"uid": user_id, "cid": cid, "v": v, "e": e, "d": d}
            )

    return _redir(f"/ui/admin/users/{user_id}/companies")

@router.get("/users/{user_id}/company-permissions", response_class=HTMLResponse)
def admin_user_company_permissions(
    request: Request,
    user_id: int,
    country: str | None = None,
    q: str | None = None,
):
    current_user = _get_current_user_for_ui(request)
    _require_admin(current_user)

    # ✅ country 允许为空：空=全部
    country_in = (country or "").strip()
    country_norm = _normalize_country(country_in) if country_in else ""  # "" 表示全部
    q = (q or "").strip()

    with engine.begin() as conn:
        user_row = conn.execute(
            text("SELECT id, username, display_name, role, status FROM users WHERE id=:uid"),
            {"uid": user_id},
        ).mappings().first()

        if not user_row:
            return templates.TemplateResponse(
                "admin/not_found.html",
                {"request": request, "current_user": current_user},
            )

        countries = conn.execute(
            text("""
                SELECT DISTINCT COALESCE(NULLIF(TRIM(UPPER(country)), ''), 'OTHER') AS country
                FROM companies
                WHERE deleted_at IS NULL
                ORDER BY 1
            """)
        ).scalars().all()

        # ✅ 不要再自动强制 countries[0]！
        # 如果用户传了一个国家但不在列表里，就回到“全部”
        if country_norm and (country_norm not in countries):
            country_norm = ""

        rows = conn.execute(
            text("""
                SELECT
                    c.id,
                    c.company_name,
                    COALESCE(NULLIF(TRIM(UPPER(c.country)), ''), 'OTHER') AS country,
                    COALESCE(p.can_view, 0) AS can_view,
                    COALESCE(p.can_edit, 0) AS can_edit,
                    COALESCE(p.can_docs, 0) AS can_docs

                FROM companies c
                LEFT JOIN user_company_permissions p
                    ON p.company_id = c.id AND p.user_id = :uid
                WHERE c.deleted_at IS NULL
                  AND (:country = '' OR COALESCE(NULLIF(TRIM(UPPER(c.country)), ''), 'OTHER') = :country)
                  AND (:q = '' OR c.company_name LIKE CONCAT('%', :q, '%')
                       OR c.registration_number LIKE CONCAT('%', :q, '%')
                       OR c.vat_number LIKE CONCAT('%', :q, '%')
                       OR c.cui LIKE CONCAT('%', :q, '%'))
                ORDER BY c.company_name
            """),
            {"uid": user_id, "country": country_norm, "q": q},
        ).mappings().all()

        total = len(rows)
        view_cnt = sum(1 for r in rows if int(r["can_view"]) == 1)
        edit_cnt = sum(1 for r in rows if int(r["can_edit"]) == 1)
        docs_cnt = sum(1 for r in rows if int(r["can_docs"]) == 1)

    return templates.TemplateResponse(
        "user_company_permissions_v2.html",
        {
            "request": request,
            "current_user": current_user,
            "user": user_row,
            "countries": countries,
            "country": country_norm,  # ✅ 这里现在可能是 ""（全部）
            "q": q,
            "rows": rows,
            "stats": {"total": total, "view_cnt": view_cnt, "edit_cnt": edit_cnt, "docs_cnt": docs_cnt},
        },
    )


@router.post("/users/{user_id}/company-permissions/bulk", response_class=JSONResponse)
def admin_user_company_permissions_bulk(
    request: Request,
    user_id: int,
    country: str = Form(""),  # ✅ 不要必填，否则很容易 422
    q: str = Form(""),
    mode: str = Form(...),
):
    current_user = _get_current_user_for_ui(request)
    _require_admin(current_user)

    country_in = (country or "").strip()
    country_norm = _normalize_country(country_in) if country_in else ""  # "" 表示全部
    q = (q or "").strip()
    mode = (mode or "").strip()

    allowed_modes = {
        "set_view",
        "set_edit",
        "set_docs",
        "clear",
        "clear_edit_only",
        "clear_docs_only",
    }
    if mode not in allowed_modes:
        return JSONResponse({"ok": False, "error": "Invalid mode"}, status_code=400)

    with engine.begin() as conn:
        company_ids = conn.execute(
            text(
                """
                SELECT c.id
                FROM companies c
                WHERE c.deleted_at IS NULL
                  AND (:country = '' OR COALESCE(NULLIF(TRIM(UPPER(c.country)), ''), 'OTHER') = :country)
                  AND (
                        :q = ''
                        OR c.company_name LIKE CONCAT('%', :q, '%')
                        OR c.registration_number LIKE CONCAT('%', :q, '%')
                        OR c.vat_number LIKE CONCAT('%', :q, '%')
                        OR c.cui LIKE CONCAT('%', :q, '%')
                  )
                """
            ),
            {"country": country_norm, "q": q},
        ).scalars().all()

        if not company_ids:
            return {"ok": True, "affected": 0}

        ids_stmt = bindparam("ids", expanding=True)

        # ---------- clear / partial clear ----------
        if mode == "clear":
            conn.execute(
                text(
                    "DELETE FROM user_company_permissions "
                    "WHERE user_id=:uid AND company_id IN :ids"
                ).bindparams(ids_stmt),
                {"uid": user_id, "ids": company_ids},
            )
            return {"ok": True, "affected": len(company_ids)}

        if mode == "clear_edit_only":
            conn.execute(
                text(
                    "UPDATE user_company_permissions "
                    "SET can_edit=0 "
                    "WHERE user_id=:uid AND company_id IN :ids"
                ).bindparams(ids_stmt),
                {"uid": user_id, "ids": company_ids},
            )
            return {"ok": True, "affected": len(company_ids)}

        if mode == "clear_docs_only":
            conn.execute(
                text(
                    "UPDATE user_company_permissions "
                    "SET can_docs=0 "
                    "WHERE user_id=:uid AND company_id IN :ids"
                ).bindparams(ids_stmt),
                {"uid": user_id, "ids": company_ids},
            )
            return {"ok": True, "affected": len(company_ids)}

        # ---------- set_view / set_edit / set_docs ----------
        if mode in ("set_view", "set_edit", "set_docs"):
            if mode == "set_view":
                conn.execute(
                    text("""
                        INSERT INTO user_company_permissions (user_id, company_id, can_view, can_edit, can_docs, created_at)
                        VALUES (:uid, :cid, 1, 0, 0, NOW())
                        ON DUPLICATE KEY UPDATE
                        can_view=1
                    """),
                    [{"uid": user_id, "cid": cid} for cid in company_ids],
                )
                return {"ok": True, "affected": len(company_ids)}

            if mode == "set_edit":
                conn.execute(
                    text("""
                        INSERT INTO user_company_permissions (user_id, company_id, can_view, can_edit, can_docs, created_at)
                        VALUES (:uid, :cid, 1, 1, 0, NOW())
                        ON DUPLICATE KEY UPDATE
                        can_view=1,
                        can_edit=1
                    """),
                    [{"uid": user_id, "cid": cid} for cid in company_ids],
                )
                return {"ok": True, "affected": len(company_ids)}

            if mode == "set_docs":
                conn.execute(
                    text("""
                        INSERT INTO user_company_permissions (user_id, company_id, can_view, can_edit, can_docs, created_at)
                        VALUES (:uid, :cid, 1, 0, 1, NOW())
                        ON DUPLICATE KEY UPDATE
                        can_view=1,
                        can_docs=1
                    """),
                    [{"uid": user_id, "cid": cid} for cid in company_ids],
                )
                return {"ok": True, "affected": len(company_ids)}

@router.post(
    "/users/{user_id}/company-permissions/company/{company_id}",
    response_class=JSONResponse,
)
async def admin_user_company_permissions_update_one(
    request: Request,
    user_id: int,
    company_id: int,
):
    current_user = _get_current_user_for_ui(request)
    _require_admin(current_user)

    data = await request.json()

    can_view = 1 if str(data.get("can_view", 0)) in {"1", "true", "True"} else 0
    can_edit = 1 if str(data.get("can_edit", 0)) in {"1", "true", "True"} else 0
    can_docs = 1 if str(data.get("can_docs", 0)) in {"1", "true", "True"} else 0

    # 规则：edit => view；docs => view
    if can_edit == 1:
        can_view = 1
    if can_docs == 1:
        can_view = 1

    with engine.begin() as conn:
        # 全 0：删除该行
        if can_view == 0 and can_edit == 0 and can_docs == 0:
            conn.execute(
                text("DELETE FROM user_company_permissions WHERE user_id=:uid AND company_id=:cid"),
                {"uid": user_id, "cid": company_id},
            )
            return {"ok": True, "can_view": 0, "can_edit": 0, "can_docs": 0}

        conn.execute(
            text(
                """
                INSERT INTO user_company_permissions
                    (user_id, company_id, can_view, can_edit, can_docs, created_at)
                VALUES
                    (:uid, :cid, :v, :e, :d, NOW())
                ON DUPLICATE KEY UPDATE
                  can_view=:v,
                  can_edit=:e,
                  can_docs=:d
                """
            ),
            {"uid": user_id, "cid": company_id, "v": can_view, "e": can_edit, "d": can_docs},
        )

    return {"ok": True, "can_view": can_view, "can_edit": can_edit, "can_docs": can_docs}
