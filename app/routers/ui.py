from __future__ import annotations
from fastapi.responses import JSONResponse
from sqlalchemy import text
import re
from datetime import datetime
from typing import Any, Dict, List, Optional
from fastapi import Request, HTTPException
import os, uuid
from typing import List
from fastapi import UploadFile, File, HTTPException
from fastapi.responses import FileResponse, RedirectResponse
from starlette.status import HTTP_303_SEE_OTHER
import os
import shutil
from fastapi.responses import JSONResponse
# ---------- FastAPI 相关 ----------
from fastapi import UploadFile, File, Form, Request  # ⚠️重复：下面又 import 了一次 File/Form/Request/UploadFile
import httpx
from fastapi.responses import RedirectResponse  # ⚠️重复：下面又 import
from fastapi.responses import RedirectResponse, HTMLResponse  # ⚠️重复：重复导入
from fastapi import (  # ⚠️重复：File/Form/Request/UploadFile 已在上面出现
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    Query,
    Request,
    UploadFile,
)
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse  # ⚠️重复：重复导入
from fastapi.templating import Jinja2Templates

# ---------- SQLAlchemy / DB ----------
from sqlalchemy import bindparam, text
from sqlalchemy.exc import IntegrityError
from urllib.parse import urlencode
import shutil
from app.db import engine
from app.auth.jwt import decode_access_token
from app.auth.deps import get_current_user

from typing import Dict, Any
from sqlalchemy import text, bindparam

# =========================================================
# Router / Templates / Constants
# =========================================================
router = APIRouter(prefix="/ui", tags=["ui"])
templates = Jinja2Templates(directory="app/templates")

COOKIE_NAME = "access_token"

# 回收站保留天数（环境变量控制，默认 7 天）
DOC_RETENTION_DAYS = int(os.getenv("DOC_RETENTION_DAYS", "7"))

# =========================================================
# Cookie / API base helpers
# =========================================================

def _api_base(request: Request) -> str:
    """
    API base 拼接辅助（当前返回空字符串，表示同域相对路径）。
    你项目里如果已有统一配置就用你的；这里保守用相对。
    UI 同域代理通常直接拼 /documents...
    """
    return ""


# =========================================================
# Auth helpers
# =========================================================
def get_secret_key() -> str:
    """
    获取 JWT_SECRET（用于 decode_access_token）
    """
    import os

    key = os.environ.get("JWT_SECRET")
    if not key:
        raise RuntimeError("Missing JWT_SECRET in environment")
    return key


def _redirect(url: str) -> RedirectResponse:
    """
    统一 302 跳转
    """
    return RedirectResponse(url=url, status_code=302)


def _get_token_from_cookie(request: Request) -> Optional[str]:
    """
    从 Cookie 里取 token（⚠️重复定义：会覆盖上面那个 _get_token_from_cookie）
    这里使用 COOKIE_NAME 常量，行为与上面“access_token”硬编码一致。
    """
    return request.cookies.get(COOKIE_NAME)


def _decode_user_id_from_token(token: str) -> Optional[int]:
    """
    从 JWT token 中解析用户 ID（sub 字段）
    """
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
    """
    DB 查询用户基本信息（用于 UI 会话识别）
    """
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT id, username, display_name, email, phone, department, role, status
                FROM users
                WHERE id=:id
                LIMIT 1
                """
            ),
            {"id": user_id},
        ).mappings().first()
    return dict(row) if row else None


def _get_current_user_for_ui(request: Request) -> Optional[Dict[str, Any]]:
    """
    UI 侧“从 cookie token 解析 -> 取用户 -> 校验 status”
    - status=1 表示启用
    """
    token = _get_token_from_cookie(request)
    if not token:
        return None

    user_id = _decode_user_id_from_token(token)
    if not user_id:
        return None

    user = _get_user_by_id(user_id)
    if not user:
        return None

    if int(user.get("status") or 0) != 1:
        return None

    return user


def _q_int(v: str | None) -> int | None:
    """
    字符串转 int：
    - None / "" => None
    - 非数字 => None
    """
    if v is None:
        return None
    v = v.strip()
    if v == "":
        return None
    try:
        return int(v)
    except ValueError:
        return None


def _clamp_int(v, default: int, min_v: int, max_v: int) -> int:
    """
    将输入转换为 int 并限制在 [min_v, max_v]，异常则返回 default
    """
    try:
        x = int(v)
    except Exception:
        return default
    return max(min_v, min(max_v, x))


def _base_ctx(request: Request, current_user: dict | None, active: str = "") -> Dict[str, Any]:
    """
    给模板渲染的基础上下文（统一 request/current_user/active）
    """
    return {
        "request": request,
        "current_user": current_user,
        "active": active,
    }


# =========================================================
# Upload helpers
# =========================================================
def _make_group_key(company_id: int) -> str:
    """
    生成 group_key（用于“同批次上传”的归组）
    格式：COMP{company_id}-{YYYYMMDD-HHMMSS}
    """
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    return f"COMP{company_id}-{ts}"


def _title_from_filename(name: str) -> str:
    """
    ✅ title 非必填：如果用户不填，则从文件名生成（去扩展名）
    """
    name = (name or "").strip()
    if not name:
        return "原件"

    # 兼容 Windows / Linux 路径
    base = name.split("/")[-1].split("\\")[-1]

    # 去掉末尾扩展名（最多 8 位）
    base = re.sub(r"\.[A-Za-z0-9]{1,8}$", "", base).strip()
    return base or name


def _get_company_brief(company_id: int) -> dict | None:
    """
    取公司简要信息（upload 页展示）
    """
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT id, company_name, country, registration_number, vat_number, company_status
                FROM companies
                WHERE id=:id
                LIMIT 1
                """
            ),
            {"id": int(company_id)},
        ).mappings().first()
    return dict(row) if row else None


# =========================================================
# Permission helpers
# =========================================================
def _render_no_permission(
    request: Request,
    current_user: Dict[str, Any] | None,
    active: str,
    message: str,
    back_url: str = "/ui/companies",
):
    """
    统一“无权限”页面渲染（模板：no_permission.html）
    """
    return templates.TemplateResponse(
        "no_permission.html",
        {
            **_base_ctx(request, current_user, active),
            "message": message,
            "back_url": back_url,
        },
        status_code=200,
    )


def _is_admin(user: Dict[str, Any] | None) -> bool:
    """
    是否 admin（role == 'admin'）
    """
    return bool(user) and (user.get("role") == "admin")


def _get_permitted_company_ids(user: Dict[str, Any] | None, need: str = "view") -> List[int]:
    """
    获取用户有权限的 company_id 列表（用于过滤）
    - need="view": can_view=1 OR can_edit=1 OR can_docs=1
    - need="edit": can_edit=1
    - need="docs": can_docs=1
    admin：返回 []（你后续用 None 表示不限）
    """
    if not user:
        return []
    if _is_admin(user):
        return []

    uid = int(user["id"])

    if need == "edit":
        sql = text("""
            SELECT company_id
            FROM user_company_permissions
            WHERE user_id=:uid AND can_edit=1
        """)
        with engine.connect() as conn:
            rows = conn.execute(sql, {"uid": uid}).fetchall()
        return [int(r[0]) for r in rows]

    if need == "docs":
        sql = text("""
            SELECT company_id
            FROM user_company_permissions
            WHERE user_id=:uid AND can_docs=1
        """)
        with engine.connect() as conn:
            rows = conn.execute(sql, {"uid": uid}).fetchall()
        return [int(r[0]) for r in rows]

    # view
    sql = text("""
        SELECT company_id
        FROM user_company_permissions
        WHERE user_id=:uid AND (can_view=1 OR can_edit=1 OR can_docs=1)
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"uid": uid}).fetchall()
    return [int(r[0]) for r in rows]


def _has_company_perm(user: Dict[str, Any] | None, company_id: int, need: str = "view") -> bool:
    """
    是否具备公司权限
    - admin：True
    - 非 admin：查 user_company_permissions
      规则：
        can_edit => view
        can_docs => view
    """
    if not user:
        return False
    if _is_admin(user):
        return True

    uid = int(user["id"])
    with engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT can_view, can_edit, can_docs
                FROM user_company_permissions
                WHERE user_id=:uid AND company_id=:cid
                LIMIT 1
            """),
            {"uid": uid, "cid": int(company_id)},
        ).mappings().first()

    if not row:
        return False

    can_view = int(row.get("can_view") or 0)
    can_edit = int(row.get("can_edit") or 0)
    can_docs = int(row.get("can_docs") or 0)

    if can_edit == 1:
        can_view = 1
    if can_docs == 1:
        can_view = 1

    if need == "view":
        return can_view == 1
    if need == "edit":
        return can_edit == 1
    if need == "docs":
        return can_docs == 1
    return False

def _require_company_docs_perm_or_403(
    request: Request,
    current_user: Dict[str, Any] | None,
    company_id: int,
    active: str = "documents",
    back_url: str = "/ui/companies",
):
    """
    文档权限：必须 company.view + can_docs=1（admin 永远放行）
    """
    if not _has_company_perm(current_user, company_id, need="view"):
        return _render_no_permission(
            request,
            current_user,
            active=active,
            message="你没有权限访问该公司的信息（需要“查看”权限）。",
            back_url=back_url,
        )

    if not _has_company_docs_perm(current_user, company_id):
        return _render_no_permission(
            request,
            current_user,
            active=active,
            message="你没有文档权限（Docs）。请联系管理员开通。",
            back_url=back_url,
        )

    return None  # ✅ 表示通过


def _can_view_legal_person(user: Dict[str, Any] | None, person_id: int) -> bool:
    """
    法人“查看权限”：
    - admin：True
    - 否则：法人绑定到某公司 + 用户对该公司具备 view/edit
    """
    if not user:
        return False
    if _is_admin(user):
        return True

    uid = int(user["id"])
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT 1
                FROM company_legal_persons clp
                JOIN user_company_permissions ucp
                  ON ucp.company_id = clp.company_id
                WHERE clp.legal_person_id=:pid
                  AND ucp.user_id=:uid
                  AND (ucp.can_view=1 OR ucp.can_edit=1)
                LIMIT 1
                """
            ),
            {"pid": int(person_id), "uid": uid},
        ).first()
    return bool(row)


def _can_edit_legal_person(user: Dict[str, Any] | None, person_id: int) -> bool:
    """
    法人“编辑权限”：
    - admin：True
    - 否则：法人绑定到某公司 + 用户对该公司具备 can_edit=1
    """
    if not user:
        return False
    if _is_admin(user):
        return True

    uid = int(user["id"])
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT 1
                FROM company_legal_persons clp
                JOIN user_company_permissions ucp
                  ON ucp.company_id = clp.company_id
                WHERE clp.legal_person_id=:pid
                  AND ucp.user_id=:uid
                  AND ucp.can_edit=1
                LIMIT 1
                """
            ),
            {"pid": int(person_id), "uid": uid},
        ).first()
    return bool(row)


def _list_companies_for_dropdown(user: Dict[str, Any] | None, need: str = "view") -> List[Dict[str, Any]]:
    """
    给 Upload / 下拉选择公司用：返回 id, company_name（按权限过滤）
    - admin：返回全部未删除公司（limit 1000）
    - 非 admin：仅返回 permitted ids
    """
    if not user:
        return []
    if _is_admin(user):
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT id, company_name
                    FROM companies
                    WHERE deleted_at IS NULL
                    ORDER BY company_name
                    LIMIT 1000
                    """
                )
            ).mappings().all()
        return [dict(r) for r in rows]

    ids = _get_permitted_company_ids(user, need=need)
    if not ids:
        return []

    stmt = text(
        """
        SELECT id, company_name
        FROM companies
        WHERE deleted_at IS NULL
          AND id IN :ids
        ORDER BY company_name
        LIMIT 1000
        """
    ).bindparams(bindparam("ids", expanding=True))

    with engine.connect() as conn:
        rows = conn.execute(stmt, {"ids": ids}).mappings().all()
    return [dict(r) for r in rows]


def _sync_company_vats(conn, company_id: int, vats: list[str]) -> None:
    """
    同步 company_vat_numbers：
    - 删除旧的 -> 插入新的
    - vats 已经在外部去空 / 去重
    """
    conn.execute(
        text("DELETE FROM company_vat_numbers WHERE company_id=:cid"),
        {"cid": company_id},
    )

    for v in vats:
        conn.execute(
            text(
                """
                INSERT INTO company_vat_numbers (company_id, vat_number, created_at)
                VALUES (:cid, :vat, NOW())
                """
            ),
            {"cid": company_id, "vat": v},
        )

def _get_company_perm_for_ui(current_user: dict | None, company_id: int) -> dict:
    """
    返回 dict: {"can_view":0/1,"can_edit":0/1,"can_docs":0/1}
    规则：
    - admin 默认全权限
    - 普通用户从 user_company_permissions 表取
    - edit=>view, docs=>view
    """
    if not current_user:
        return {"can_view": 0, "can_edit": 0, "can_docs": 0}

    if current_user.get("role") == "admin":
        return {"can_view": 1, "can_edit": 1, "can_docs": 1}

    uid = current_user.get("id")
    if not uid:
        return {"can_view": 0, "can_edit": 0, "can_docs": 0}

    with engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT can_view, can_edit, can_docs
                FROM user_company_permissions
                WHERE user_id=:uid AND company_id=:cid
                LIMIT 1
            """),
            {"uid": uid, "cid": company_id},
        ).mappings().first()

    if not row:
        return {"can_view": 0, "can_edit": 0, "can_docs": 0}

    v = int(row.get("can_view") or 0)
    e = int(row.get("can_edit") or 0)
    d = int(row.get("can_docs") or 0)

    if e == 1:
        v = 1
    if d == 1:
        v = 1

    return {"can_view": v, "can_edit": e, "can_docs": d}


def normalize_mysql_date(value: str | None) -> str | None:
    """
    Accepts:
      - YYYY-MM-DD
      - DD-MM-YYYY
      - DD.MM.YYYY
      - DD/MM/YYYY
      - YYYY/MM/DD
      - with time: 'YYYY-MM-DD HH:MM:SS' or 'DD-MM-YYYY HH:MM:SS'
    Returns:
      - 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS' (MySQL safe)
      - None if blank
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None

    # Try formats with time first
    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
        "%d.%m.%Y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
    ]
    for f in fmts:
        try:
            dt = datetime.strptime(s, f)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass

    # Date only
    fmts2 = [
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%d.%m.%Y",
        "%d/%m/%Y",
        "%Y/%m/%d",
    ]
    for f in fmts2:
        try:
            d = datetime.strptime(s, f).date()
            return d.strftime("%Y-%m-%d")
        except ValueError:
            pass

    # If already looks like ISO date prefix, keep first 10
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]

    # Unknown format -> raise to catch earlier (or return None if you prefer)
    raise ValueError(f"Unsupported date format: {s}")

# =========================================================
# UI: Login / Logout
# =========================================================
@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    """
    登录页
    """
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@router.post("/login", response_class=HTMLResponse)
async def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
):
    """
    登录提交：
    - 调用 API: POST /auth/login
    - 成功后把 access_token 写入 cookie
    """
    api_url = str(request.base_url).rstrip("/") + "/auth/login"

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(api_url, json={"username": username, "password": password})

    if resp.status_code != 200:
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "用户名或密码错误"},
            status_code=200,
        )

    data = resp.json()
    token = data.get("access_token")
    if not token:
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "登录失败：无 token"},
            status_code=200,
        )

    r = _redirect("/ui/upload")
    r.set_cookie(COOKIE_NAME, token, httponly=True, samesite="lax")
    return r


@router.get("/logout")
async def logout(request: Request):
    """
    退出：删除 cookie 并回登录页
    """
    r = _redirect("/ui/login")
    r.delete_cookie(COOKIE_NAME)
    return r


@router.post("/companies/{company_id}/delete")
def ui_company_delete_hard(request: Request, company_id: int):
    current_user = _get_current_user_for_ui(request)

    perms = _get_company_perm_for_ui(current_user, company_id)
    if not perms.get("can_edit"):
        return templates.TemplateResponse(
            "no_permission.html",
            {"request": request, "current_user": current_user, "no_permission": True},
            status_code=403,
        )

    # 1️⃣ 先删数据库（事务）
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM company_legal_persons WHERE company_id=:id"), {"id": company_id})
        conn.execute(text("DELETE FROM company_platforms WHERE company_id=:id"), {"id": company_id})
        conn.execute(text("DELETE FROM documents WHERE company_id=:id"), {"id": company_id})
        conn.execute(text("DELETE FROM companies WHERE id=:id LIMIT 1"), {"id": company_id})

    # 2️⃣ 再删磁盘目录（放在事务外）
    uploads_dir = f"./uploads/{company_id}"

    if os.path.isdir(uploads_dir):
        shutil.rmtree(uploads_dir)

    return RedirectResponse("/ui/companies", status_code=HTTP_303_SEE_OTHER)
# =========================================================
# UI: Upload
# =========================================================
@router.get("/upload", response_class=HTMLResponse)
async def ui_upload_get(
    request: Request,
    company_id: int | None = Query(default=None),
):
    """
    上传页面：
    - 必须登录
    - 下拉公司列表按 edit 权限过滤
    - 默认选第一个公司
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    companies = _list_companies_for_dropdown(current_user, need="edit")

    selected_company_id = company_id or (int(companies[0]["id"]) if companies else None)
    company_brief = _get_company_brief(int(selected_company_id)) if selected_company_id else None

    return templates.TemplateResponse(
        "upload.html",
        {
            **_base_ctx(request, current_user, "upload"),
            "ok": None,
            "error": None,
            "result_json": None,
            "companies": companies,
            "selected_company_id": selected_company_id,
            "company_brief": company_brief,
            "default_category": "原件",
            "default_title": "",          # ✅ 默认留空
            "default_dedup": "true",
            "default_group_key": "",
        },
    )


@router.post("/upload", response_class=HTMLResponse)
async def ui_upload_post(
    request: Request,
    file: UploadFile = File(...),
    company_id: int = Form(...),
    category: str = Form(""),
    title: str = Form(""),       # ✅ 非必填
    dedup: str = Form("true"),
    file_type: str = Form(""),   # ✅ 文件类型
    group_key: str = Form(""),
):
    """
    上传提交：
    - 权限：必须对 company 具备 edit 权限
    - title 为空时：从 filename 自动生成
    - group_key 为空时：自动生成
    - 调 API: POST /documents/upload（带 Bearer token）
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _has_company_perm(current_user, company_id, need="edit"):
        return _render_no_permission(
            request,
            current_user,
            active="upload",
            message="你没有权限上传到该公司（需要“编辑”权限）。请联系管理员开通。",
            back_url=f"/ui/upload?company_id={company_id}",
        )

    companies = _list_companies_for_dropdown(current_user, need="edit")
    company_brief = _get_company_brief(company_id)

    # 分类：空 -> 原件
    category_s = (category or "").strip() or "原件"

    # title：空 -> 取文件名
    title_s = (title or "").strip()
    if not title_s:
        title_s = _title_from_filename(file.filename)

    # 去重参数：限定 true/false
    dedup_s = (dedup or "true").strip().lower()
    if dedup_s not in ("true", "false"):
        dedup_s = "true"

    # 文件类型：允许为空
    file_type_s = (file_type or "").strip()

    # group_key：空 -> 自动生成
    group_key_s = (group_key or "").strip()
    if not group_key_s:
        group_key_s = _make_group_key(company_id)

    token = _get_token_from_cookie(request)
    api_url = str(request.base_url).rstrip("/") + "/documents/upload"

    # 保险：确保 stream 从头开始
    try:
        file.file.seek(0)
    except Exception:
        pass

    # -------- 调用 API 上传 --------
    try:
        async with httpx.AsyncClient(timeout=300.0) as client:
            files = {"file": (file.filename, file.file, file.content_type)}
            data = {
                "company_id": str(company_id),
                "category": category_s,
                "title": title_s,
                "file_type": file_type_s,     # ✅ 关键：把 file_type 传给 API
                "dedup": dedup_s,
                "group_key": group_key_s,
            }
            resp = await client.post(
                api_url,
                headers={"Authorization": f"Bearer {token}"},
                data=data,
                files=files,
            )
    except Exception as e:
        # 网络/超时等异常：回显错误
        return templates.TemplateResponse(
            "upload.html",
            {
                **_base_ctx(request, current_user, "upload"),
                "ok": False,
                "error": f"上传失败：{e}",
                "result_json": None,
                "companies": companies,
                "selected_company_id": company_id,
                "company_brief": company_brief,
                "default_category": category_s,
                "default_title": title_s,
                "default_file_type": file_type_s,
                "default_dedup": dedup_s,
                "default_group_key": group_key_s,
            },
            status_code=200,
        )

    # API 返回 4xx/5xx：回显错误
    if resp.status_code >= 400:
        err_text = resp.text
        if len(err_text) > 300:
            err_text = err_text[:300] + "..."
        return templates.TemplateResponse(
            "upload.html",
            {
                **_base_ctx(request, current_user, "upload"),
                "ok": False,
                "error": f"上传失败：{resp.status_code} - {err_text}",
                "result_json": None,
                "companies": companies,
                "selected_company_id": company_id,
                "company_brief": company_brief,
                "default_category": category_s,
                "default_title": title_s,
                "default_file_type": file_type_s,
                "default_dedup": dedup_s,
                "default_group_key": group_key_s,
            },
            status_code=200,
        )

    # 成功：回显 result_json，并清空 title/file_type/group_key（方便下一次上传）
    return templates.TemplateResponse(
        "upload.html",
        {
            **_base_ctx(request, current_user, "upload"),
            "ok": True,
            "error": None,
            "result_json": resp.json(),
            "companies": companies,
            "selected_company_id": company_id,
            "company_brief": company_brief,
            "default_category": category_s,
            "default_title": "",             # ✅ 成功后清空
            "default_file_type": "",         # ✅ 成功后清空
            "default_dedup": dedup_s,
            "default_group_key": "",         # ✅ 成功后清空
        },
        status_code=200,
    )


# =========================================================
# UI: Documents（列表）
# ✅只保留一个；带分页；带权限过滤；
# ✅下拉 options；支持 file_type / q（任意字段搜索）
# =========================================================
@router.get("/documents", response_class=HTMLResponse)
async def ui_documents(
    request: Request,
    company_id: str | None = Query(default=None),
    company: str | None = Query(default=None),
    category: str | None = Query(default=None),
    file_type: str | None = Query(default=None),   # ✅ 文件类别筛选
    title: str | None = Query(default=None),
    q: str | None = Query(default=None),           # ✅ 任意字段搜索
    is_deleted: str | None = Query(default=None),  # "", "0", "1"
    group_key: str | None = Query(default=None),
    page: str | None = Query(default="1"),
    limit: str | None = Query(default="20"),
):
    """
    文档列表页：
    - 登录校验
    - 非 admin：按 user_company_permissions 过滤
    - 支持多条件筛选 + 分页
    - 下拉 options：按权限范围取 distinct file_type/category
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    # 分页参数
    limit_i = _clamp_int(limit, 20, 1, 200)
    page_i = _clamp_int(page, 1, 1, 10_000)
    offset = (page_i - 1) * limit_i

    # 解析 company_id：既支持纯数字 id，也支持字符串 company_code
    q_company_raw = (company_id or "").strip()
    q_company_id = _q_int(q_company_raw) or 0
    q_company_code = q_company_raw if (q_company_raw and q_company_id == 0) else ""

    # 其他筛选项
    q_company = (company or "").strip()
    q_category = (category or "").strip()
    q_file_type = (file_type or "").strip()
    q_title = (title or "").strip()
    q_any = (q or "").strip()
    q_group_key = (group_key or "").strip()
    q_is_deleted = (is_deleted or "").strip()

    # 动态 where
    where = ["1=1"]
    params: Dict[str, Any] = {"limit": limit_i, "offset": offset}

    # ✅ 非 admin：按权限过滤公司
    permitted_ids = None
    if not _is_admin(current_user):
        permitted_ids = None
        if not _is_admin(current_user):
            # ✅ 文档列表：用 docs 权限过滤公司
            permitted_ids = _get_permitted_company_ids(current_user, need="view")
            # 上面这一行你原来就有，但它不看 can_docs —— 要换成 docs 版本：
            permitted_ids = []
            with engine.connect() as conn:
                permitted_ids = conn.execute(
                    text("""
                        SELECT company_id
                        FROM user_company_permissions
                        WHERE user_id=:uid AND can_docs=1
                    """),
                    {"uid": int(current_user["id"])},
                ).scalars().all()
            permitted_ids = [int(x) for x in permitted_ids]

            if not permitted_ids:
                # ✅ 没有任何 docs 权限：直接无权限页（而不是显示空表）
                return _render_no_permission(
                    request,
                    current_user,
                    active="documents",
                    message="你没有任何公司的文档权限（Docs）。请联系管理员开通。",
                    back_url="/ui/companies",
                )

            where.append("d.company_id IN :cids")
            params["cids"] = permitted_ids

    # company_id（数字）
    if q_company_id > 0:
        # 额外校验：访问该公司权限（避免用户直接拼 url）
        if not _has_company_perm(current_user, q_company_id, need="view"):
            return _render_no_permission(
                request,
                current_user,
                active="documents",
                message="你没有权限查看该公司的文档（需要“查看”权限）。",
                back_url="/ui/documents",
            )
        where.append("d.company_id = :company_id")
        params["company_id"] = q_company_id

    # company_code（字符串）
    elif q_company_code:
        where.append("LOWER(COALESCE(TRIM(c.company_code),'')) = :company_code")
        params["company_code"] = q_company_code.lower()

    # 公司名
    if q_company:
        where.append("c.company_name LIKE CONCAT('%', :company, '%')")
        params["company"] = q_company

    # 分类
    if q_category:
        where.append("d.category = :category")
        params["category"] = q_category

    # 文件类型
    if q_file_type:
        where.append("d.file_type = :file_type")
        params["file_type"] = q_file_type

    # 标题
    if q_title:
        where.append("d.title LIKE CONCAT('%', :title, '%')")
        params["title"] = q_title

    # group_key
    if q_group_key:
        where.append("d.group_key = :group_key")
        params["group_key"] = q_group_key

    # 删除状态
    if q_is_deleted in ("0", "1"):
        where.append("d.is_deleted = :is_deleted")
        params["is_deleted"] = int(q_is_deleted)

    # 任意字段搜索（“全部”）
    if q_any:
        where.append(
            """
            (
              COALESCE(d.title,'') LIKE CONCAT('%', :q_any, '%')
              OR COALESCE(d.original_filename,'') LIKE CONCAT('%', :q_any, '%')
              OR COALESCE(d.category,'') LIKE CONCAT('%', :q_any, '%')
              OR COALESCE(d.file_type,'') LIKE CONCAT('%', :q_any, '%')
              OR COALESCE(d.group_key,'') LIKE CONCAT('%', :q_any, '%')
              OR COALESCE(c.company_name,'') LIKE CONCAT('%', :q_any, '%')
              OR COALESCE(c.company_code,'') LIKE CONCAT('%', :q_any, '%')
            )
            """
        )
        params["q_any"] = q_any

    where_sql = " AND ".join(where)

    # 统计总数
    count_stmt = text(
        f"""
        SELECT COUNT(*)
        FROM documents d
        LEFT JOIN companies c ON c.id = d.company_id
        WHERE {where_sql}
        """
    )

    # 列表数据
    list_stmt = text(
        f"""
        SELECT
            d.id,
            d.company_id,
            COALESCE(c.company_name, '-') AS company_name,
            d.title,
            d.category,
            d.original_filename,
            d.file_type,
            d.created_at,
            d.group_key,
            d.is_deleted
        FROM documents d
        LEFT JOIN companies c ON c.id = d.company_id
        WHERE {where_sql}
        ORDER BY d.id DESC
        LIMIT :limit OFFSET :offset
        """
    )

    # 下拉 options：按权限范围取 distinct（并可跟随 is_deleted 筛选）
    opt_where = ["1=1"]
    opt_params: Dict[str, Any] = {}

    if permitted_ids is not None:
        opt_where.append("d.company_id IN :cids")
        opt_params["cids"] = permitted_ids

    if q_is_deleted in ("0", "1"):
        opt_where.append("d.is_deleted = :opt_is_deleted")
        opt_params["opt_is_deleted"] = int(q_is_deleted)

    opt_where_sql = " AND ".join(opt_where)

    opt_file_type_stmt = text(
        f"""
        SELECT DISTINCT d.file_type
        FROM documents d
        WHERE {opt_where_sql}
          AND d.file_type IS NOT NULL AND d.file_type <> ''
        ORDER BY d.file_type
        """
    )
    opt_category_stmt = text(
        f"""
        SELECT DISTINCT d.category
        FROM documents d
        WHERE {opt_where_sql}
          AND d.category IS NOT NULL AND d.category <> ''
        ORDER BY d.category
        """
    )

    # IN (:cids) expanding
    if permitted_ids is not None:
        count_stmt = count_stmt.bindparams(bindparam("cids", expanding=True))
        list_stmt = list_stmt.bindparams(bindparam("cids", expanding=True))
        opt_file_type_stmt = opt_file_type_stmt.bindparams(bindparam("cids", expanding=True))
        opt_category_stmt = opt_category_stmt.bindparams(bindparam("cids", expanding=True))

    with engine.connect() as conn:
        total = conn.execute(count_stmt, params).scalar() or 0
        rows = conn.execute(list_stmt, params).mappings().all()
        file_type_options = conn.execute(opt_file_type_stmt, opt_params).scalars().all()
        category_options = conn.execute(opt_category_stmt, opt_params).scalars().all()

    total = int(total)
    total_pages = max(1, (total + limit_i - 1) // limit_i)

    q_obj = {
        "company_id": q_company_raw,
        "company": q_company,
        "category": q_category,
        "file_type": q_file_type,
        "title": q_title,
        "q": q_any,
        "is_deleted": q_is_deleted,
        "group_key": q_group_key,
        "limit": str(limit_i),
        "page": str(page_i),
    }

    return templates.TemplateResponse(
        "documents.html",
        {
            **_base_ctx(request, current_user, "documents"),
            "rows": rows,
            "q": q_obj,
            "total": total,
            "page": page_i,
            "limit": limit_i,
            "total_pages": total_pages,
            "file_type_options": list(file_type_options or []),
            "category_options": list(category_options or []),
        },
    )
@router.post("/companies/bulk-delete")
async def ui_companies_bulk_delete(request: Request):
    current_user = _get_current_user_for_ui(request)
    if not current_user or current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    payload = await request.json()
    company_ids = payload.get("company_ids") or []
    company_ids = [int(x) for x in company_ids if str(x).isdigit()]

    if not company_ids:
        return JSONResponse({"ok": True, "deleted": 0})

    # ✅ 这里复用你单删的逻辑：先删数据库，再删 uploads 目录
    deleted = 0
    errors = []

    with engine.begin() as conn:
        for cid in company_ids:
            try:
                # 1) 先删关联数据（按你项目实际表补齐）
                conn.execute(text("DELETE FROM documents WHERE company_id=:cid"), {"cid": cid})
                conn.execute(text("DELETE FROM company_platforms WHERE company_id=:cid"), {"cid": cid})
                conn.execute(text("DELETE FROM company_legal_persons WHERE company_id=:cid"), {"cid": cid})
                conn.execute(text("DELETE FROM user_company_permissions WHERE company_id=:cid"), {"cid": cid})

                # 2) 再删公司
                rc = conn.execute(text("DELETE FROM companies WHERE id=:cid LIMIT 1"), {"cid": cid}).rowcount
                if rc:
                    deleted += 1
            except Exception as e:
                errors.append({"company_id": cid, "error": str(e)})

    # 3) 删除磁盘目录（放在事务外也行）
    for cid in company_ids:
        try:
            uploads_dir = os.path.join("uploads", str(cid))
            if os.path.isdir(uploads_dir):
                shutil.rmtree(uploads_dir)
        except Exception as e:
            errors.append({"company_id": cid, "error": f"remove uploads failed: {e}"})

    return JSONResponse({"ok": True, "deleted": deleted, "errors": errors})

# =========================================================
# UI: Document detail / download / group history / delete / restore
# =========================================================
@router.get("/documents/{doc_id:int}", response_class=HTMLResponse)
async def ui_document_detail(request: Request, doc_id: int):
    """
    文档详情页：
    - 查 documents + companies
    - 校验 company view 权限
    - 返回 can_edit（用于模板显示编辑/删除按钮）
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT
                    d.id,
                    d.company_id,
                    COALESCE(c.company_code, '-') AS company_code,
                    COALESCE(c.company_name, '-') AS company_name,
                    d.group_key,
                    d.uploaded_by,
                    d.category,
                    d.file_type,
                    d.title,
                    d.original_filename,
                    d.storage_path,
                    d.mime_type,
                    d.file_size,
                    d.file_sha256,
                    d.is_deleted,
                    d.deleted_at,
                    d.created_at,
                    d.updated_at
                FROM documents d
                LEFT JOIN companies c ON c.id = d.company_id
                WHERE d.id=:id
                LIMIT 1
                """
            ),
            {"id": doc_id},
        ).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="Document not found")

    company_id = int(row["company_id"])
    if not _has_company_perm(current_user, company_id, need="view"):
        return _render_no_permission(
            request,
            current_user,
            active="documents",
            message="你没有权限查看该文档所属公司（需要“查看”权限）。",
            back_url="/ui/documents",
        )

    can_edit = _has_company_perm(current_user, company_id, need="edit")

    deny = _require_company_docs_perm_or_403(
        request, current_user, company_id,
        active="documents",
        back_url="/ui/documents",
    )
    if deny:
        return deny


    return templates.TemplateResponse(
        "document_detail.html",
        {**_base_ctx(request, current_user, "documents"), "doc": dict(row), "can_edit": can_edit},
    )


@router.get("/documents/{doc_id}/download")
async def ui_documents_download(request: Request, doc_id: int):
    """
    文档下载：
    - 先查 doc.company_id 做权限校验
    - 再代理请求 API /documents/{id}/download
    - 把 Content-Disposition 透传给浏览器
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id, company_id FROM documents WHERE id=:id LIMIT 1"),
            {"id": doc_id},
        ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Document not found")

    if not _has_company_perm(current_user, int(row["company_id"]), need="view"):
        return _redirect("/ui/documents")

    token = _get_token_from_cookie(request)
    api_url = str(request.base_url).rstrip("/") + f"/documents/{doc_id}/download"

    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.get(api_url, headers={"Authorization": f"Bearer {token}"})

    deny = _require_company_docs_perm_or_403(
        request, current_user, int(row["company_id"]),
        active="documents",
        back_url="/ui/documents",
    )
    if deny:
        return deny


    if resp.status_code in (401, 403):
        return _redirect("/ui/login")
    if resp.status_code >= 400:
        return HTMLResponse(content=resp.text, status_code=resp.status_code)

    content_type = resp.headers.get("content-type", "application/octet-stream")
    content_disp = resp.headers.get("content-disposition")

    headers = {}
    if content_disp:
        headers["Content-Disposition"] = content_disp

    return StreamingResponse(iter([resp.content]), media_type=content_type, headers=headers)


@router.get("/groups/{group_key}", response_class=HTMLResponse)
async def ui_group_history(request: Request, group_key: str):
    """
    按 group_key 查看同批次上传历史
    - 非 admin：按可见公司过滤
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    gk = (group_key or "").strip()
    if not gk:
        raise HTTPException(status_code=400, detail="group_key is required")

    where = ["d.group_key = :gk"]
    params: Dict[str, Any] = {"gk": gk}

    permitted = None
    if not _is_admin(current_user):
        permitted = None
        if not _is_admin(current_user):
            with engine.connect() as conn:
                permitted = conn.execute(
                    text("""
                        SELECT company_id
                        FROM user_company_permissions
                        WHERE user_id=:uid AND can_docs=1
                    """),
                    {"uid": int(current_user["id"])},
                ).scalars().all()
            permitted = [int(x) for x in permitted]

            if not permitted:
                return _render_no_permission(
                    request,
                    current_user,
                    active="documents",
                    message="你没有文档权限（Docs）。",
                    back_url="/ui/companies",
                )

            where.append("d.company_id IN :cids")
            params["cids"] = permitted

    stmt = text(
        f"""
        SELECT
            d.id,
            d.company_id,
            c.company_name,
            d.title,
            d.category,
            d.original_filename,
            d.created_at,
            d.updated_at,
            d.group_key,
            d.is_deleted,
            d.deleted_at
        FROM documents d
        LEFT JOIN companies c ON c.id = d.company_id
        WHERE {" AND ".join(where)}
        ORDER BY d.created_at DESC, d.id DESC
        """
    )
    if permitted is not None:
        stmt = stmt.bindparams(bindparam("cids", expanding=True))

    with engine.connect() as conn:
        rows = conn.execute(stmt, params).mappings().all()

    return templates.TemplateResponse(
        "group_history.html",
        {**_base_ctx(request, current_user, "documents"), "rows": rows, "group_key": gk},
    )


@router.post("/documents/{doc_id}/delete")
async def ui_document_delete(request: Request, doc_id: int):
    """
    软删除文档（is_deleted=1, deleted_at=NOW）
    - 权限：company edit
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id, company_id FROM documents WHERE id=:id LIMIT 1"),
            {"id": doc_id},
        ).mappings().first()
    if not row:
        return _redirect("/ui/documents")

    if not _has_company_perm(current_user, int(row["company_id"]), need="edit"):
        return _render_no_permission(
            request,
            current_user,
            active="documents",
            message="你没有权限删除该文档（需要“编辑”权限）。",
            back_url="/ui/documents",
        )
    
    if not _has_company_docs_perm(current_user, int(row["company_id"])):
        return _render_no_permission(
            request,
            current_user,
            active="documents",
            message="你没有文档权限（Docs），无法进行该文档操作。",
            back_url="/ui/documents",
        )

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE documents
                SET is_deleted = 1,
                    deleted_at = NOW(),
                    updated_at = NOW()
                WHERE id = :id
                LIMIT 1
                """
            ),
            {"id": doc_id},
        )

    referer = request.headers.get("referer") or "/ui/documents"
    return RedirectResponse(url=referer, status_code=302)


@router.post("/documents/{doc_id}/restore")
async def ui_document_restore(request: Request, doc_id: int):
    """
    恢复软删除文档（is_deleted=0, deleted_at=NULL）
    - 权限：company edit
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id, company_id FROM documents WHERE id=:id LIMIT 1"),
            {"id": doc_id},
        ).mappings().first()
    if not row:
        return _redirect("/ui/documents")

    if not _has_company_perm(current_user, int(row["company_id"]), need="edit"):
        return _render_no_permission(
            request,
            current_user,
            active="documents",
            message="你没有权限恢复该文档（需要“编辑”权限）。",
            back_url="/ui/documents",
        )

    if not _has_company_docs_perm(current_user, int(row["company_id"])):
        return _render_no_permission(
            request,
            current_user,
            active="documents",
            message="你没有文档权限（Docs），无法进行该文档操作。",
            back_url="/ui/documents",
        )


    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE documents
                SET is_deleted = 0,
                    deleted_at = NULL,
                    updated_at = NOW()
                WHERE id = :id
                LIMIT 1
                """
            ),
            {"id": doc_id},
        )

    referer = request.headers.get("referer") or "/ui/documents"
    return RedirectResponse(url=referer, status_code=302)


# =========================================================
# UI: Companies（列表/详情/编辑/保存/删除/恢复）
# =========================================================

def _select_companies_sql_for_user(
    user: Dict[str, Any],
    limit_i: int,
    q_s: str,
    company_status_s: str,
):
    """
    根据用户权限拼 companies 列表 SQL：
    - company_status: '', '已注销','税号失效','税号未生效','变更法人','法人证件过期','未设置'
    - q: 多字段模糊 + company_code 精确/模糊
    - 非 admin：按 permitted company_ids 过滤
    """
    where = ["1=1"]
    params: Dict[str, Any] = {"limit": limit_i}

    # ✅ 公司状态筛选（替换掉 deleted_at active/deleted/all）
    cs = (company_status_s or "").strip()
    if cs:
        if cs == "未设置":
            where.append("(company_status IS NULL OR TRIM(company_status) = '')")
        else:
            where.append("company_status = :company_status")
            params["company_status"] = cs

    # ✅ 关键词搜索（保留你原逻辑）
    if q_s:
        where.append(
            """
            (
              COALESCE(company_name,'') LIKE :q OR
              COALESCE(registration_number,'') LIKE :q OR
              COALESCE(country,'') LIKE :q OR
              COALESCE(vat_number,'') LIKE :q OR
              COALESCE(cui,'') LIKE :q OR
              COALESCE(address,'') LIKE :q OR
              COALESCE(postal_code,'') LIKE :q OR
              COALESCE(registration_authority,'') LIKE :q OR
              COALESCE(company_domain,'') LIKE :q OR
              COALESCE(company_status,'') LIKE :q OR
              LOWER(COALESCE(TRIM(company_code),'')) = :q_code_exact OR
              LOWER(COALESCE(TRIM(company_code),'')) LIKE :q_code_like OR
              EXISTS (
                SELECT 1
                FROM company_legal_persons clp
                JOIN legal_persons lp ON lp.id = clp.legal_person_id
                WHERE clp.company_id = companies.id
                    AND lp.deleted_at IS NULL
                    AND COALESCE(lp.full_name,'') LIKE :q
                LIMIT 1
          )
            )
            """
        )
        params["q"] = f"%{q_s}%"
        params["q_code_exact"] = q_s.strip().lower()
        params["q_code_like"] = f"%{q_s.strip().lower()}%"

    permitted = None
    if not _is_admin(user):
        permitted = _get_permitted_company_ids(user, need="view")
        if not permitted:
            where.append("1=0")
        else:
            where.append("id IN :cids")
            params["cids"] = permitted

# 找到 _select_companies_sql_for_user 函数，替换 stmt 定义部分：

    stmt = text(
        f"""
        SELECT
            id,
            company_name,
            company_code,
            country,
            registration_number,
            vat_number,
            cui,
            address,
            postal_code,
            register_time,
            registration_authority,
            company_domain,
            company_status,
            created_at,
            updated_at,
            deleted_at,
            -- ✅ 新增：关联查询法人名字 (取最新绑定的一个)
            (
                SELECT lp.full_name 
                FROM company_legal_persons clp
                JOIN legal_persons lp ON lp.id = clp.legal_person_id
                WHERE clp.company_id = companies.id 
                ORDER BY clp.id DESC 
                LIMIT 1
            ) AS legal_person_name
        FROM companies
        WHERE {" AND ".join(where)}
        ORDER BY id DESC
        LIMIT :limit
        """
    ) 

    if permitted is not None and permitted:
        stmt = stmt.bindparams(bindparam("cids", expanding=True))

    return stmt, params


@router.get("/companies", response_class=HTMLResponse)
async def ui_companies(
    request: Request,
    q: str | None = Query(default=None),
    company_status: str | None = Query(default=""),
    limit: str | None = Query(default="200"),
):
    """
    公司列表页：
    - 按权限过滤
    - 支持 q、status、limit
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    q_s = (q or "").strip()
    company_status_s = (company_status or "").strip()

    # ✅ 白名单（避免乱传值）
    allowed = {"", "已注销", "税号失效", "税号未生效", "变更法人", "法人证件过期"}
    if company_status_s not in allowed:
        company_status_s = ""

    limit_i = _q_int(limit) or 200
    limit_i = max(1, min(limit_i, 500))

    # ✅ 这里同步改 helper：不再传 status_s，而是传 company_status_s
    stmt, params = _select_companies_sql_for_user(
        current_user, limit_i, q_s, company_status_s
    )

    with engine.connect() as conn:
        rows = conn.execute(stmt, params).mappings().all()

    return templates.TemplateResponse(
        "companies.html",
        {
            **_base_ctx(request, current_user, "companies"),
            "rows": rows,
            "q": {"q": q_s, "limit": str(limit_i), "company_status": company_status_s},  # ✅
        },
    )

@router.get("/companies/new", response_class=HTMLResponse)
async def ui_company_new(request: Request):
    """
    新建公司页（仅 admin）
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _is_admin(current_user):
        return _render_no_permission(
            request,
            current_user,
            active="companies",
            message="你没有权限新建公司（仅管理员可操作）。",
            back_url="/ui/companies",
        )

    return templates.TemplateResponse(
        "company_form.html",
        {**_base_ctx(request, current_user, "companies"), "mode": "new", "vat_numbers": [], "company": {}},
    )

# --------------------------------------------------------------------
# UI: Companies（详情）
# --------------------------------------------------------------------
@router.get("/companies/{company_id}", response_class=HTMLResponse)
async def ui_company_detail(request: Request, company_id: int):
    """
    公司详情页：
    - 权限：需要 company.view
    - 页面数据：
      - company 基础信息（companies 表）
      - vat_numbers（company_vat_numbers 表，多 VAT）
      - docs / addresses / platforms / legal_persons（公司关联数据，展示用）
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    # ✅ 需要“查看”权限
    if not _has_company_perm(current_user, company_id, need="view"):
        return _render_no_permission(
            request,
            current_user,
            active="companies",
            message="你没有权限查看该公司（需要“查看”权限）。请联系管理员开通。",
            back_url="/ui/companies",
        )
    
    can_edit = _has_company_perm(current_user, company_id, need="edit")
    can_docs = _has_company_docs_perm(current_user, company_id)  # ✅ 先算好

    # ✅ 一个连接里把公司相关信息查出来（按你当前实现保留）
    with engine.connect() as conn:
        # 1) 公司基础信息
        company = conn.execute(
            text(
                """
                SELECT
                    id,
                    company_name,
                    company_code,
                    country,
                    registration_number,
                    vat_number,
                    cui,
                    address,
                    postal_code,
                    register_time,
                    registration_authority,
                    company_domain,
                    company_status,
                    created_at,
                    updated_at,
                    deleted_at
                FROM companies
                WHERE id=:id
                LIMIT 1
                """
            ),
            {"id": company_id},
        ).mappings().first()

        if not company:
            raise HTTPException(status_code=404, detail="Company not found")

        # 2) VAT 列表（多条）
        vat_numbers = conn.execute(
            text(
                """
                SELECT vat_number
                FROM company_vat_numbers
                WHERE company_id=:cid
                ORDER BY id ASC
                """
            ),
            {"cid": company_id},
        ).scalars().all()

    # ✅ 最近文档：只有 can_docs 才查询
        docs = []
        if can_docs:
            docs = conn.execute(
                text(
                    """
                    SELECT
                        id, company_id, title, category, original_filename,
                        created_at, group_key, is_deleted
                    FROM documents
                    WHERE company_id=:cid
                    ORDER BY created_at DESC, id DESC
                    LIMIT 20
                    """
                ),
                {"cid": company_id},
            ).mappings().all()

        # 4) 公司地址（展示）
        addresses = conn.execute(
            text(
                """
                SELECT
                    id, company_id, address, postal_code, address_type,
                    is_current, note, created_at
                FROM company_addresses
                WHERE company_id=:cid
                ORDER BY is_current DESC, id DESC
                LIMIT 50
                """
            ),
            {"cid": company_id},
        ).mappings().all()

        # 5) 平台列表（展示）
        platforms = conn.execute(
            text(
                """
                SELECT
                    id AS platform_id,
                    company_id,
                    platform_name,
                    platform_email,
                    progress,
                    created_at
                FROM company_platforms
                WHERE company_id=:cid
                ORDER BY created_at DESC, platform_id DESC
                LIMIT 50
                """
            ),
            {"cid": company_id},
        ).mappings().all()
        
        # 6) 法人绑定（展示）
        legal_persons = conn.execute(
            text(
                """
                SELECT
                    clp.id AS link_id,
                    clp.company_id,
                    clp.legal_person_id,
                    clp.role,
                    clp.created_at AS linked_at,
                    lp.full_name,
                    lp.nationality,
                    lp.birthday,
                    lp.id_number,
                    lp.passport_number
                FROM company_legal_persons clp
                JOIN legal_persons lp ON lp.id = clp.legal_person_id
                WHERE clp.company_id=:cid
                ORDER BY clp.id DESC
                LIMIT 100
                """
            ),
            {"cid": company_id},
        ).mappings().all()

    # ✅ 页面上是否显示“编辑按钮”等
    can_edit = _has_company_perm(current_user, company_id, need="edit")

    return templates.TemplateResponse(
        "company_detail.html",
        {
            **_base_ctx(request, current_user, "companies"),
            "company": dict(company),
            "vat_numbers": list(vat_numbers),  # ✅ 传给模板：多 VAT
            "docs": docs,
            "addresses": addresses,
            "platforms": platforms,
            "legal_persons": legal_persons,
            "can_edit": can_edit,
            "can_docs": can_docs,
        },
    )


# --------------------------------------------------------------------
# UI: Companies（编辑页）
# --------------------------------------------------------------------
@router.get("/companies/{company_id}/edit", response_class=HTMLResponse)
async def ui_company_edit(request: Request, company_id: int):
    """
    公司编辑页：
    - 权限：需要 company.edit
    - 返回 company_form.html（mode=edit），并带回 vat_numbers 列表
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _has_company_perm(current_user, company_id, need="edit"):
        return _render_no_permission(
            request,
            current_user,
            active="companies",
            message="你没有权限编辑该公司（需要“编辑”权限）。",
            back_url=f"/ui/companies/{company_id}",
        )

    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT
                    id,
                    company_name,
                    company_code,
                    country,
                    registration_number,
                    vat_number,               -- ✅ 本地税号 local_tax_no（你表字段名仍为 vat_number）
                    cui,
                    address,
                    postal_code,
                    register_time,
                    registration_authority,
                    company_domain,
                    company_status,
                    deleted_at
                FROM companies
                WHERE id=:id
                LIMIT 1
                """
            ),
            {"id": company_id},
        ).mappings().first()

        if not row:
            raise HTTPException(status_code=404, detail="Company not found")

        vats = conn.execute(
            text(
                """
                SELECT vat_number
                FROM company_vat_numbers
                WHERE company_id=:cid
                ORDER BY id ASC
                """
            ),
            {"cid": company_id},
        ).scalars().all()

    return templates.TemplateResponse(
        "company_form.html",
        {
            **_base_ctx(request, current_user, "companies"),
            "mode": "edit",
            "company": dict(row),
            "vat_numbers": list(vats),
            "can_edit": True,
        },
    )


# --------------------------------------------------------------------
# UI: Companies（保存：新建/编辑）
# --------------------------------------------------------------------
@router.post("/companies/save")
async def ui_company_save(
    request: Request,
    company_id: str = Form(""),
    company_name: str = Form(...),
    company_code: str = Form(""),
    country: str = Form(""),
    registration_number: str = Form(""),
    # 旧字段：兼容单条 VAT（历史表单字段）
    vat_number: str = Form(""),
    # 本地税号（存 companies.vat_number）
    local_tax_no: str = Form(""),
    # VAT 列表（存 company_vat_numbers）
    vat_numbers: List[str] = Form(default=[]),
    cui: str = Form(""),
    address: str = Form(""),
    postal_code: str = Form(""),
    register_time: str = Form(""),
    registration_authority: str = Form(""),
    company_domain: str = Form(""),
    company_status: str = Form(""),
    # 法人信息
    lp_full_name: str = Form(""),
    lp_last_name: str = Form(""),
    lp_middle_name: str = Form(""),
    lp_first_name: str = Form(""),
    lp_birthday: str = Form(""),  # date
    lp_nationality: str = Form(""),
    lp_id_number: str = Form(""),
    lp_id_expiry_date: str = Form(""),  # date
    lp_passport_number: str = Form(""),
    lp_passport_expiry_date: str = Form(""),  # date
    lp_legal_address: str = Form(""),
    lp_postal_code: str = Form(""),
    lp_role: str = Form(""),

):
    """
    公司保存：
    - company_id 为空 => 新建（仅 admin）
    - company_id 有值 => 编辑（需要 company.edit）
    - VAT 处理：
      - companies.vat_number: 当作 local_tax_no（本地税号）
      - company_vat_numbers: 多 VAT 列表
      - 若 vat_numbers 为空但 legacy vat_number 有值，则用 legacy 值补一条
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    cid = _q_int(company_id)

    # ✅ 新建：仅 admin
    if cid is None:
        if not _is_admin(current_user):
            return _render_no_permission(
                request,
                current_user,
                "companies",
                "你没有权限新建公司（仅管理员可操作）。",
                "/ui/companies",
            )
    # ✅ 编辑：需要 edit 权限
    else:
        if not _has_company_perm(current_user, cid, need="edit"):
            return _render_no_permission(
                request,
                current_user,
                "companies",
                "你没有权限编辑该公司（需要“编辑”权限）。",
                f"/ui/companies/{cid}",
            )

    # register_time：空字符串 -> None
    rt = (register_time or "").strip()
    rt_val = normalize_mysql_date(rt)  # ✅ 这里做规范化

    # VAT 列表清洗：去空、去重、保序（按输入顺序）
    clean_vats: List[str] = []
    seen = set()
    for x in vat_numbers:
        x = (x or "").strip()
        if not x:
            continue
        if x in seen:
            continue
        seen.add(x)
        clean_vats.append(x)

    # 兼容旧 vat_number：如果 vat_numbers 没填，但 legacy 填了，就补上
    legacy_vat = (vat_number or "").strip()
    if not clean_vats and legacy_vat:
        clean_vats = [legacy_vat]

    # companies 表字段 payload（不变）
    payload = {
        "company_name": (company_name or "").strip(),
        "company_code": (company_code or "").strip() or None,
        "country": (country or "").strip() or None,
        "registration_number": (registration_number or "").strip() or None,
        "vat_number": (local_tax_no or "").strip() or None,
        "cui": (cui or "").strip() or None,
        "address": (address or "").strip() or None,
        "postal_code": (postal_code or "").strip() or None,
        "register_time": rt_val,  # ✅ 已规范化
        "registration_authority": (registration_authority or "").strip() or None,
        "company_domain": (company_domain or "").strip() or None,
        "company_status": (company_status or "").strip() or None,
    }
    # company_name 必填（为空就回列表）
    if payload["company_name"] == "":
        return RedirectResponse(url="/ui/companies", status_code=302)

    with engine.begin() as conn:
        # -------------------------
        # 编辑
        # -------------------------
        if cid:
            conn.execute(
                text(
                    """
                    UPDATE companies
                    SET
                        company_name=:company_name,
                        company_code=:company_code,
                        country=:country,
                        registration_number=:registration_number,
                        vat_number=:vat_number,
                        cui=:cui,
                        address=:address,
                        postal_code=:postal_code,
                        register_time=:register_time,
                        registration_authority=:registration_authority,
                        company_domain=:company_domain,
                        company_status=:company_status,
                        updated_at=NOW()
                    WHERE id=:id
                    LIMIT 1
                    """
                ),
                {**payload, "id": cid},
            )

            # ✅ 同步 company_vat_numbers（按你已有 helper：先删后插）
            _sync_company_vats(conn, cid, clean_vats)

            return RedirectResponse(url=f"/ui/companies/{cid}", status_code=302)

        # -------------------------
        # 新建
        # -------------------------
        try:
            conn.execute(
                text(
                    """
                    INSERT INTO companies
                        (company_name, company_code, country, registration_number, vat_number, cui,
                         address, postal_code, register_time, registration_authority, company_domain,
                         company_status, created_at, updated_at, deleted_at)
                    VALUES
                        (:company_name, :company_code, :country, :registration_number, :vat_number, :cui,
                         :address, :postal_code, :register_time, :registration_authority, :company_domain,
                         :company_status, NOW(), NOW(), NULL)
                    """
                ),
                payload,
            )
        except IntegrityError as e:
            # ✅ 唯一键冲突：uq_country_reg（country + registration_number）
            msg = str(e.orig) if getattr(e, "orig", None) else str(e)
            if "uq_country_reg" in msg or "Duplicate entry" in msg:
                return templates.TemplateResponse(
                    "company_form.html",
                    {
                        **_base_ctx(request, current_user, "companies"),
                        "mode": "new",
                        "company": payload,
                        "vat_numbers": clean_vats,
                        "error": "国家 + 注册号 已存在（country + registration_number 必须唯一）。请更换注册号或修改国家。",
                    },
                    status_code=200,
                )
            raise

        # ✅ 拿到新 ID 后，再写 VAT 列表
        new_id = int(conn.execute(text("SELECT LAST_INSERT_ID()")).scalar() or 0)
        if new_id > 0:
            _sync_company_vats(conn, new_id, clean_vats)

        # ✅ 可选：新建法人并绑定（默认忽略；full_name 不填就跳过）
        lp_name = (lp_full_name or "").strip()
        if lp_name:
            def _to_date(s: str):
                s = (s or "").strip()
                return s if s else None  # 你现在直接传给 MySQL date，空就 None

            lp_payload = {
                "full_name": lp_name,
                "last_name": (lp_last_name or "").strip() or None,
                "middle_name": (lp_middle_name or "").strip() or None,
                "first_name": (lp_first_name or "").strip() or None,
                "birthday": _to_date(lp_birthday),
                "nationality": (lp_nationality or "").strip() or None,
                "id_number": (lp_id_number or "").strip() or None,
                "id_expiry_date": _to_date(lp_id_expiry_date),
                "passport_number": (lp_passport_number or "").strip() or None,
                "passport_expiry_date": _to_date(lp_passport_expiry_date),
                "legal_address": (lp_legal_address or "").strip() or None,
                "postal_code": (lp_postal_code or "").strip() or None,
            }
            role_s = (lp_role or "").strip()

            # 1) 去重：优先用 id_number / passport_number 找现有法人复用
            existing_lp = None
            if lp_payload["id_number"]:
                existing_lp = conn.execute(
                    text("""
                        SELECT id
                        FROM legal_persons
                        WHERE deleted_at IS NULL AND id_number=:idno
                        LIMIT 1
                    """),
                    {"idno": lp_payload["id_number"]},
                ).mappings().first()

            if (not existing_lp) and lp_payload["passport_number"]:
                existing_lp = conn.execute(
                    text("""
                        SELECT id
                        FROM legal_persons
                        WHERE deleted_at IS NULL AND passport_number=:pp
                        LIMIT 1
                    """),
                    {"pp": lp_payload["passport_number"]},
                ).mappings().first()

            if existing_lp:
                lp_id = int(existing_lp["id"])
            else:
                conn.execute(
                    text("""
                        INSERT INTO legal_persons
                            (full_name, last_name, middle_name, first_name,
                             birthday, nationality,
                             id_number, id_expiry_date,
                             passport_number, passport_expiry_date,
                             legal_address, postal_code,
                             created_at, updated_at, deleted_at)
                        VALUES
                            (:full_name, :last_name, :middle_name, :first_name,
                             :birthday, :nationality,
                             :id_number, :id_expiry_date,
                             :passport_number, :passport_expiry_date,
                             :legal_address, :postal_code,
                             NOW(), NOW(), NULL)
                    """),
                    lp_payload,
                )
                lp_id = int(conn.execute(text("SELECT LAST_INSERT_ID()")).scalar() or 0)

            # 2) 绑定 company_legal_persons（去重）
            if lp_id > 0:
                exists_link = conn.execute(
                    text("""
                        SELECT id FROM company_legal_persons
                        WHERE company_id=:cid AND legal_person_id=:lpid
                        LIMIT 1
                    """),
                    {"cid": new_id, "lpid": lp_id},
                ).first()

                if not exists_link:
                    conn.execute(
                        text("""
                            INSERT INTO company_legal_persons
                                (company_id, legal_person_id, role, created_at)
                            VALUES
                                (:cid, :lpid, :role, NOW())
                        """),
                        {"cid": new_id, "lpid": lp_id, "role": role_s},
                    )


    return RedirectResponse(url="/ui/companies", status_code=302)


# --------------------------------------------------------------------
# UI: Companies（软删 / 恢复）——仅 admin
# --------------------------------------------------------------------
@router.post("/companies/{company_id}/delete")
async def ui_company_delete(request: Request, company_id: int):
    """软删公司：设置 deleted_at（仅管理员）"""
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _is_admin(current_user):
        return _render_no_permission(
            request,
            current_user,
            active="companies",
            message="你没有权限删除公司（仅管理员可操作）。",
            back_url=f"/ui/companies/{company_id}",
        )

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE companies
                SET deleted_at = NOW(), updated_at = NOW()
                WHERE id=:id
                LIMIT 1
                """
            ),
            {"id": company_id},
        )

    referer = request.headers.get("referer") or "/ui/companies"
    return RedirectResponse(url=referer, status_code=302)


@router.post("/companies/{company_id}/restore")
async def ui_company_restore(request: Request, company_id: int):
    """恢复公司：清空 deleted_at（仅管理员）"""
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _is_admin(current_user):
        return _render_no_permission(
            request,
            current_user,
            active="companies",
            message="你没有权限恢复公司（仅管理员可操作）。",
            back_url="/ui/companies",
        )

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE companies
                SET deleted_at = NULL, updated_at = NOW()
                WHERE id=:id
                LIMIT 1
                """
            ),
            {"id": company_id},
        )

    return RedirectResponse(url="/ui/companies?status=deleted", status_code=302)

def _has_company_docs_perm(user: dict, company_id: int) -> bool:
    if not user:
        return False
    if _is_admin(user):
        return True
    with engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT 1
                FROM user_company_permissions
                WHERE user_id=:uid AND company_id=:cid AND can_docs=1
                LIMIT 1
            """),
            {"uid": int(user["id"]), "cid": int(company_id)},
        ).first()
    return bool(row)


# =========================================================
# UI: Company -> Legal Persons (list / bind / unbind)
# =========================================================
@router.get("/companies/{company_id}/legal-persons", response_class=HTMLResponse)
async def ui_company_legal_persons(request: Request, company_id: int):
    """
    公司-法人绑定管理页：
    - view：可看绑定关系与候选列表
    - edit：可执行 bind / unbind
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _has_company_perm(current_user, company_id, need="view"):
        return _render_no_permission(
            request,
            current_user,
            active="companies",
            message="你没有权限查看该公司的法人绑定（需要“查看”权限）。",
            back_url=f"/ui/companies/{company_id}",
        )

    with engine.connect() as conn:
        company = conn.execute(
            text(
                "SELECT id, company_name, company_code FROM companies WHERE id=:id LIMIT 1"
            ),
            {"id": company_id},
        ).mappings().first()
        if not company:
            raise HTTPException(status_code=404, detail="Company not found")

        # 已绑定列表（含法人信息）
        bound = conn.execute(
            text(
                """
                SELECT
                    clp.id AS link_id,
                    clp.role,
                    clp.created_at AS linked_at,
                    lp.id AS legal_person_id,
                    lp.full_name,
                    lp.nationality,
                    lp.birthday,
                    lp.id_number,
                    lp.passport_number,
                    lp.deleted_at
                FROM company_legal_persons clp
                JOIN legal_persons lp ON lp.id = clp.legal_person_id
                WHERE clp.company_id = :cid
                ORDER BY clp.id DESC
                """
            ),
            {"cid": company_id},
        ).mappings().all()

        # 候选法人（简单拉取最近 300 个，按你现有逻辑）
        candidates = conn.execute(
            text(
                """
                SELECT id, full_name, nationality, birthday, id_number, passport_number
                FROM legal_persons
                WHERE deleted_at IS NULL
                ORDER BY id DESC
                LIMIT 300
                """
            )
        ).mappings().all()

    can_edit = _has_company_perm(current_user, company_id, need="edit")

    return templates.TemplateResponse(
        "company_legal_persons.html",
        {
            **_base_ctx(request, current_user, "companies"),
            "company": dict(company),
            "bound": bound,
            "candidates": candidates,
            "can_edit": can_edit,
        },
    )


@router.post("/companies/{company_id}/legal-persons/bind")
async def ui_company_legal_person_bind(
    request: Request,
    company_id: int,
    legal_person_id: int = Form(...),
    role: str = Form(""),
):
    """绑定法人到公司：去重插入（需要 company.edit）"""
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _has_company_perm(current_user, company_id, need="edit"):
        return _render_no_permission(
            request,
            current_user,
            active="companies",
            message="你没有权限绑定法人（需要“编辑”权限）。",
            back_url=f"/ui/companies/{company_id}/legal-persons",
        )

    role_s = (role or "").strip()

    with engine.begin() as conn:
        exists = conn.execute(
            text(
                """
                SELECT id FROM company_legal_persons
                WHERE company_id=:cid AND legal_person_id=:lpid
                LIMIT 1
                """
            ),
            {"cid": company_id, "lpid": legal_person_id},
        ).mappings().first()

        if not exists:
            conn.execute(
                text(
                    """
                    INSERT INTO company_legal_persons
                        (company_id, legal_person_id, role, created_at)
                    VALUES
                        (:cid, :lpid, :role, NOW())
                    """
                ),
                {"cid": company_id, "lpid": legal_person_id, "role": role_s},
            )

    return RedirectResponse(url=f"/ui/companies/{company_id}/legal-persons", status_code=302)


@router.post("/companies/{company_id}/legal-persons/{link_id}/unbind")
async def ui_company_legal_person_unbind(request: Request, company_id: int, link_id: int):
    """解绑法人（按 link_id 删除）（需要 company.edit）"""
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _has_company_perm(current_user, company_id, need="edit"):
        return _render_no_permission(
            request,
            current_user,
            active="companies",
            message="你没有权限解绑法人（需要“编辑”权限）。",
            back_url=f"/ui/companies/{company_id}/legal-persons",
        )

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM company_legal_persons WHERE id=:id AND company_id=:cid LIMIT 1"),
            {"id": link_id, "cid": company_id},
        )

    return RedirectResponse(url=f"/ui/companies/{company_id}/legal-persons", status_code=302)


# =========================================================
# UI: Company Addresses
# =========================================================
@router.get("/companies/{company_id}/addresses", response_class=HTMLResponse)
async def ui_company_addresses(request: Request, company_id: int):
    """
    公司地址管理页：
    - view：可看地址列表
    - edit：可新增/编辑/删除/设为当前地址
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _has_company_perm(current_user, company_id, need="view"):
        return _render_no_permission(
            request,
            current_user,
            active="companies",
            message="你没有权限查看公司地址（需要“查看”权限）。",
            back_url=f"/ui/companies/{company_id}",
        )

    with engine.connect() as conn:
        company = conn.execute(
            text(
                """
                SELECT id, company_name, country, registration_number
                FROM companies
                WHERE id=:id
                LIMIT 1
                """
            ),
            {"id": company_id},
        ).mappings().first()

        if not company:
            raise HTTPException(status_code=404, detail="Company not found")

        rows = conn.execute(
            text(
                """
                SELECT
                    id, company_id, address, postal_code,
                    address_type, is_current, note, created_at
                FROM company_addresses
                WHERE company_id = :cid
                ORDER BY is_current DESC, id DESC
                """
            ),
            {"cid": company_id},
        ).mappings().all()

    can_edit = _has_company_perm(current_user, company_id, need="edit")

    return templates.TemplateResponse(
        "company_addresses.html",
        {
            **_base_ctx(request, current_user, "companies"),
            "company": dict(company),
            "rows": rows,
            "can_edit": can_edit,
        },
    )


@router.post("/companies/{company_id}/addresses/save")
async def ui_company_address_save(
    request: Request,
    company_id: int,
    address_id: str = Form(""),
    address: str = Form(...),
    postal_code: str = Form(""),
    address_type: str = Form(""),
    is_current: str = Form("0"),
    note: str = Form(""),
):
    """
    地址保存（新增/编辑）：
    - is_current=1 时，先把该公司其它地址置为非当前，再写入该条为 current
    - 权限：company.edit
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _has_company_perm(current_user, company_id, need="edit"):
        return _render_no_permission(
            request,
            current_user,
            active="companies",
            message="你没有权限编辑公司地址（需要“编辑”权限）。",
            back_url=f"/ui/companies/{company_id}/addresses",
        )

    aid = _q_int(address_id)
    is_current_i = 1 if (is_current or "").strip() in ("1", "true", "True", "on") else 0

    payload = {
        "company_id": company_id,
        "address": (address or "").strip(),
        "postal_code": (postal_code or "").strip(),
        "address_type": (address_type or "").strip(),
        "is_current": is_current_i,
        "note": (note or "").strip(),
    }

    if payload["address"] == "":
        return RedirectResponse(url=f"/ui/companies/{company_id}/addresses", status_code=302)

    with engine.begin() as conn:
        # 若设为当前地址：先清空其它 current
        if is_current_i == 1:
            conn.execute(
                text("UPDATE company_addresses SET is_current = 0 WHERE company_id = :cid"),
                {"cid": company_id},
            )

        # 编辑
        if aid:
            conn.execute(
                text(
                    """
                    UPDATE company_addresses
                    SET
                        address = :address,
                        postal_code = :postal_code,
                        address_type = :address_type,
                        is_current = :is_current,
                        note = :note
                    WHERE id = :id AND company_id = :company_id
                    LIMIT 1
                    """
                ),
                {**payload, "id": aid},
            )
        # 新建
        else:
            conn.execute(
                text(
                    """
                    INSERT INTO company_addresses
                        (company_id, address, postal_code, address_type, is_current, note, created_at)
                    VALUES
                        (:company_id, :address, :postal_code, :address_type, :is_current, :note, NOW())
                    """
                ),
                payload,
            )

    return RedirectResponse(url=f"/ui/companies/{company_id}/addresses", status_code=302)


@router.post("/companies/{company_id}/addresses/{address_id}/delete")
async def ui_company_address_delete(request: Request, company_id: int, address_id: int):
    """删除地址（硬删 company_addresses 记录）（需要 company.edit）"""
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _has_company_perm(current_user, company_id, need="edit"):
        return _render_no_permission(
            request,
            current_user,
            active="companies",
            message="你没有权限删除公司地址（需要“编辑”权限）。",
            back_url=f"/ui/companies/{company_id}/addresses",
        )

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                DELETE FROM company_addresses
                WHERE id=:id AND company_id=:cid
                LIMIT 1
                """
            ),
            {"id": address_id, "cid": company_id},
        )

    return RedirectResponse(url=f"/ui/companies/{company_id}/addresses", status_code=302)


@router.post("/companies/{company_id}/addresses/{address_id}/set-current")
async def ui_company_address_set_current(request: Request, company_id: int, address_id: int):
    """设置当前地址：先清空该公司所有 current，再把指定地址置 current（需要 company.edit）"""
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _has_company_perm(current_user, company_id, need="edit"):
        return _render_no_permission(
            request,
            current_user,
            active="companies",
            message="你没有权限设置当前地址（需要“编辑”权限）。",
            back_url=f"/ui/companies/{company_id}/addresses",
        )

    with engine.begin() as conn:
        conn.execute(
            text("UPDATE company_addresses SET is_current = 0 WHERE company_id = :cid"),
            {"cid": company_id},
        )
        conn.execute(
            text(
                """
                UPDATE company_addresses
                SET is_current = 1
                WHERE id = :id AND company_id = :cid
                LIMIT 1
                """
            ),
            {"id": address_id, "cid": company_id},
        )

    return RedirectResponse(url=f"/ui/companies/{company_id}/addresses", status_code=302)


# =========================================================
# UI: Company Platforms（列表 / 新增 / 删除 / make-primary）
# =========================================================
@router.get("/companies/{company_id}/platforms", response_class=HTMLResponse)
async def ui_company_platforms(request: Request, company_id: int):
    """
    公司平台列表页：
    - view：可查看平台列表
    - edit：可新增/删除/设为主平台（你的“主平台”逻辑是更新 created_at）
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _has_company_perm(current_user, company_id, need="view"):
        return _render_no_permission(
            request,
            current_user,
            active="companies",
            message="你没有权限查看公司平台（需要“查看”权限）。",
            back_url=f"/ui/companies/{company_id}",
        )

    with engine.connect() as conn:
        company = conn.execute(
            text("SELECT id, company_name FROM companies WHERE id=:id LIMIT 1"),
            {"id": company_id},
        ).mappings().first()
        if not company:
            raise HTTPException(status_code=404, detail="Company not found")

        rows = conn.execute(
            text(
                """
                SELECT id, company_id, platform_name, store_url, domain, created_at
                FROM company_platforms
                WHERE company_id=:cid
                ORDER BY created_at DESC, id DESC
                """
            ),
            {"cid": company_id},
        ).mappings().all()

        # 你当前逻辑：created_at 最新的作为 primary
        primary_id = rows[0]["id"] if rows else None

    can_edit = _has_company_perm(current_user, company_id, need="edit")

    return templates.TemplateResponse(
        "company_platforms.html",
        {
            **_base_ctx(request, current_user, "companies"),
            "company": dict(company),
            "rows": rows,
            "primary_id": primary_id,
            "can_edit": can_edit,
        },
    )


@router.post("/companies/{company_id}/platforms/add")
async def ui_company_platform_add(request: Request, company_id: int,
                                  platform_name: str = Form(...),
                                  store_url: str = Form(""),
                                  domain: str = Form("")):
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _has_company_perm(current_user, company_id, need="edit"):
        return _render_no_permission(
            request, current_user, active="companies",
            message="你没有权限新增平台（需要“编辑”权限）。",
            back_url=f"/ui/companies/{company_id}/platforms",
        )

    platform_name = (platform_name or "").strip()
    store_url = (store_url or "").strip() or None
    domain = (domain or "").strip() or None

    if not platform_name:
        return RedirectResponse(url=f"/ui/companies/{company_id}/platforms", status_code=302)

    # 规范化（可选，但强烈建议：避免 OBI / obi / OBI 这种混乱）
    platform_key = platform_name.strip()

    with engine.begin() as conn:
        # 先查是否已存在
        exists = conn.execute(
            text("""
                SELECT id
                FROM company_platforms
                WHERE company_id=:cid AND platform_name=:pname
                LIMIT 1
            """),
            {"cid": company_id, "pname": platform_key},
        ).first()

        if exists:
            # 你现在平台列表页是 GET /companies/{id}/platforms
            # 最简单：用 querystring 回显提示（模板里你可以显示 msg）
            return RedirectResponse(
                url=f"/ui/companies/{company_id}/platforms?msg=平台已存在",
                status_code=302,
            )

        try:
            conn.execute(
                text("""
                    INSERT INTO company_platforms
                        (company_id, platform_name, store_url, domain, created_at)
                    VALUES
                        (:cid, :pname, :store_url, :domain, NOW())
                """),
                {"cid": company_id, "pname": platform_key, "store_url": store_url, "domain": domain},
            )
        except IntegrityError:
            # 并发情况下两个人同时点，仍可能撞唯一键，这里兜底
            return RedirectResponse(
                url=f"/ui/companies/{company_id}/platforms?msg=平台已存在",
                status_code=302,
            )

    return RedirectResponse(url=f"/ui/companies/{company_id}/platforms", status_code=302)

@router.post("/companies/{company_id}/platforms/{platform_id}/delete")
async def ui_company_platform_delete(request: Request, company_id: int, platform_id: int):
    """删除平台（硬删 company_platforms 记录）（需要 company.edit）"""
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _has_company_perm(current_user, company_id, need="edit"):
        return _render_no_permission(
            request,
            current_user,
            active="companies",
            message="你没有权限删除平台（需要“编辑”权限）。",
            back_url=f"/ui/companies/{company_id}/platforms",
        )

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                DELETE FROM company_platforms
                WHERE id=:pid AND company_id=:cid
                LIMIT 1
                """
            ),
            {"pid": platform_id, "cid": company_id},
        )

    return RedirectResponse(url=f"/ui/companies/{company_id}/platforms", status_code=302)

@router.post("/companies/{company_id}/platforms/{platform_id}/make-primary")
async def ui_company_platform_make_primary(request: Request, company_id: int, platform_id: int):
    """
    设为主平台：
    - 你当前逻辑：把该平台 created_at 更新为 NOW()，使其排在列表最前
    - 权限：company.edit
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _has_company_perm(current_user, company_id, need="edit"):
        return _render_no_permission(
            request,
            current_user,
            active="companies",
            message="你没有权限设置主平台（需要“编辑”权限）。",
            back_url=f"/ui/companies/{company_id}/platforms",
        )

    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT id
                FROM company_platforms
                WHERE id=:pid AND company_id=:cid
                LIMIT 1
                """
            ),
            {"pid": platform_id, "cid": company_id},
        ).first()

        if not row:
            raise HTTPException(status_code=404, detail="Platform not found")

        conn.execute(
            text(
                """
                UPDATE company_platforms
                SET created_at = NOW()
                WHERE id=:pid AND company_id=:cid
                LIMIT 1
                """
            ),
            {"pid": platform_id, "cid": company_id},
        )

    return RedirectResponse(url=f"/ui/companies/{company_id}/platforms", status_code=302)

# =========================
# 平台目录
# GET /ui/platforms
# =========================
@router.get("/platforms", response_class=HTMLResponse)
def ui_platforms(request: Request):
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    uid = int(current_user["id"])

    if _is_admin(current_user):
        sql = text("""
            SELECT
                LOWER(TRIM(cp.platform_name)) AS platform_key,
                MIN(cp.platform_name) AS platform_name,
                COUNT(DISTINCT cp.company_id) AS company_cnt,
                COUNT(*) AS row_cnt,
                MAX(COALESCE(cp.updated_at, cp.created_at)) AS last_updated
            FROM company_platforms cp
            JOIN companies c
              ON c.id = cp.company_id
             AND c.deleted_at IS NULL
            GROUP BY LOWER(TRIM(cp.platform_name))
            ORDER BY last_updated DESC
        """)
        params = {}
    else:
        sql = text("""
            SELECT
                LOWER(TRIM(cp.platform_name)) AS platform_key,
                MIN(cp.platform_name) AS platform_name,
                COUNT(DISTINCT cp.company_id) AS company_cnt,
                COUNT(*) AS row_cnt,
                MAX(COALESCE(cp.updated_at, cp.created_at)) AS last_updated
            FROM company_platforms cp
            JOIN companies c
              ON c.id = cp.company_id
             AND c.deleted_at IS NULL
            JOIN user_company_permissions ucp
              ON ucp.company_id = c.id
             AND ucp.user_id = :uid
             AND (ucp.can_view=1 OR ucp.can_edit=1 OR ucp.can_docs=1)
            GROUP BY LOWER(TRIM(cp.platform_name))
            ORDER BY last_updated DESC
        """)
        params = {"uid": uid}

    with engine.connect() as conn:
        rows = conn.execute(sql, params).mappings().all()

    db_stats = {r["platform_key"]: dict(r) for r in rows}


    return templates.TemplateResponse(
        "platforms_index.html",
        {
            **_base_ctx(request, current_user, "platforms"),
            "db_stats": db_stats, # 传字典给前端
        },
    )

#数据库
@router.get("/database", response_class=HTMLResponse)
async def ui_database(request: Request, q: str | None = Query(default=None)):
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    q_s = (q or "").strip()

    return templates.TemplateResponse(
        "database_index.html",
        {
            **_base_ctx(request, current_user, "database"),
            "q": {"q": q_s},
        },
    )


@router.get("/database/bank", response_class=HTMLResponse)
async def ui_database_bank(
    request: Request,
    q: str | None = Query(default=None),
    limit: str | None = Query(default="200"),
):
    """
    银行卡汇总右侧公司列表：
    - 复用 companies 列表的搜索逻辑（_select_companies_sql_for_user）
    - 这样 /ui/companies 和 /ui/database 用同一套 q 行为
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    q_s = (q or "").strip()
    limit_i = _q_int(limit) or 200
    limit_i = max(1, min(limit_i, 500))

    # 直接复用公司列表的 SQL 生成逻辑，company_status 置空即可
    stmt, params = _select_companies_sql_for_user(
        current_user, limit_i, q_s, ""
    )

    with engine.connect() as conn:
        rows = conn.execute(stmt, params).mappings().all()

    return templates.TemplateResponse(
        "database_bank_detail.html",
        {
            **_base_ctx(request, current_user, "database"),
            "rows": [dict(r) for r in rows],
        },
    )

# =========================
# 平台详情（某个平台下有哪些公司）
# GET /ui/platforms/{platform_name}
# =========================
@router.get("/platforms/{platform_name}", response_class=HTMLResponse)
def ui_platform_detail(request: Request, platform_name: str):
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    uid = int(current_user["id"])
    p_key = (platform_name or "").strip().lower()

    if _is_admin(current_user):
        sql = text("""
            SELECT
                cp.id as cp_id,
                cp.company_id,
                c.company_name,
                COALESCE(NULLIF(TRIM(UPPER(c.country)), ''), 'OTHER') AS country,
                cp.store_url,
                cp.domain,
                cp.status,
                cp.progress,
                cp.updated_at
            FROM company_platforms cp
            JOIN companies c ON c.id = cp.company_id AND c.deleted_at IS NULL
            WHERE LOWER(TRIM(cp.platform_name)) = :pkey
            ORDER BY cp.updated_at DESC
        """)
        params = {"pkey": p_key}
    else:
        sql = text("""
            SELECT
                cp.id as cp_id,
                cp.company_id,
                c.company_name,
                COALESCE(NULLIF(TRIM(UPPER(c.country)), ''), 'OTHER') AS country,
                cp.store_url,
                cp.domain,
                cp.status,
                cp.progress,
                cp.updated_at
            FROM company_platforms cp
            JOIN companies c ON c.id = cp.company_id AND c.deleted_at IS NULL
            JOIN user_company_permissions ucp ON ucp.company_id = c.id
             AND ucp.user_id = :uid
             AND (ucp.can_view=1 OR ucp.can_edit=1 OR ucp.can_docs=1)
            WHERE LOWER(TRIM(cp.platform_name)) = :pkey
            ORDER BY cp.updated_at DESC
        """)
        params = {"uid": uid, "pkey": p_key}

    with engine.connect() as conn:
        rows = conn.execute(sql, params).mappings().all()

    return templates.TemplateResponse(
        "platform_detail.html",
        {**_base_ctx(request, current_user, "platforms"), "platform_name": platform_name, "rows": rows},
    )

# =========================
# 平台-公司详情
# GET /ui/company-platforms/{cp_id}
# =========================
@router.get("/company-platforms/{cp_id}", response_class=HTMLResponse)
def ui_platform_company_detail(request: Request, cp_id: int):
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    with engine.connect() as conn:
        if _is_admin(current_user):
            row = conn.execute(
                text("""
                    SELECT
                      cp.id AS platform_id,
                      cp.company_id,
                      cp.platform_name
                    FROM company_platforms cp
                    JOIN companies c
                      ON c.id = cp.company_id
                     AND c.deleted_at IS NULL
                    WHERE cp.id = :cpid
                    LIMIT 1
                """),
                {"cpid": cp_id},
            ).mappings().first()
        else:
            uid = int(current_user["id"])
            row = conn.execute(
                text("""
                    SELECT
                      cp.id AS platform_id,
                      cp.company_id,
                      cp.platform_name
                    FROM company_platforms cp
                    JOIN companies c
                      ON c.id = cp.company_id
                     AND c.deleted_at IS NULL
                    JOIN user_company_permissions ucp
                      ON ucp.company_id = c.id
                     AND ucp.user_id = :uid
                     AND (ucp.can_view = 1 OR ucp.can_edit = 1 OR ucp.can_docs = 1)
                    WHERE cp.id = :cpid
                    LIMIT 1
                """),
                {"uid": uid, "cpid": cp_id},
            ).mappings().first()

    if not row:
        return templates.TemplateResponse(
            "not_found.html",
            {"request": request, "current_user": current_user, "active": "platforms"},
        )

    company_id = int(row["company_id"])
    platform_id = int(row["platform_id"])
    return RedirectResponse(
        url=f"/ui/companies/{company_id}/platforms/{platform_id}?from=platforms",
        status_code=302,
    )

# =========================
# 兼容旧路径（你浏览器以前点的那个）
# GET /ui/platforms/{platform_name}/companies/{cp_id}
# =========================
@router.get("/platforms/{platform_name}/companies/{cp_id}")
def ui_platform_company_redirect(request: Request, platform_name: str, cp_id: int):
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    return RedirectResponse(url=f"/ui/company-platforms/{cp_id}", status_code=302)



# =========================================================
# UI: Legal Persons（列表 / 新建 / 编辑 / 保存 / 删除 / 恢复）
# =========================================================
@router.get("/legal-persons", response_class=HTMLResponse)
async def ui_legal_persons(
    request: Request,
    q: str | None = Query(default=None),
    status: str | None = Query(default="active"),
    limit: str | None = Query(default="200"),
):
    """
    法人列表页：
    - admin：看全部
    - 非 admin：只能看到“绑定到自己有权限公司”的法人（通过 JOIN company_legal_persons + ucp）
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    q_s = (q or "").strip()
    status_s = (status or "active").strip().lower()
    if status_s not in ("active", "deleted", "all"):
        status_s = "active"

    limit_i = _q_int(limit) or 200
    limit_i = max(1, min(limit_i, 500))

    where = ["1=1"]
    params: Dict[str, Any] = {"limit": limit_i}

    # 删除状态筛选
    if status_s == "active":
        where.append("lp.deleted_at IS NULL")
    elif status_s == "deleted":
        where.append("lp.deleted_at IS NOT NULL")

    # 关键词搜索
    if q_s:
        where.append("(lp.full_name LIKE :q OR lp.id_number LIKE :q OR lp.passport_number LIKE :q)")
        params["q"] = f"%{q_s}%"

    # 非 admin：按“绑定公司权限”过滤
    join_sql = ""
    if not _is_admin(current_user):
        join_sql = """
            JOIN company_legal_persons clp ON clp.legal_person_id = lp.id
            JOIN user_company_permissions ucp ON ucp.company_id = clp.company_id
        """
        where.append("ucp.user_id = :uid AND (ucp.can_view=1 OR ucp.can_edit=1)")
        params["uid"] = int(current_user["id"])

    sql = text(
        f"""
        SELECT DISTINCT
            lp.id,
            lp.full_name,
            lp.nationality,
            lp.birthday,
            lp.id_number,
            lp.id_expiry_date,
            lp.passport_number,
            lp.passport_expiry_date,
            lp.postal_code,
            lp.deleted_at,
            lp.updated_at
        FROM legal_persons lp
        {join_sql}
        WHERE {" AND ".join(where)}
        ORDER BY lp.id DESC
        LIMIT :limit
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(sql, params).mappings().all()

    return templates.TemplateResponse(
        "legal_persons.html",
        {
            **_base_ctx(request, current_user, "legal_persons"),
            "rows": rows,
            "q": {"q": q_s, "status": status_s, "limit": str(limit_i)},
        },
    )


@router.get("/legal-persons/new", response_class=HTMLResponse)
async def ui_legal_person_new(
    request: Request,
    company_id: int | None = Query(default=None),
):
    """
    法人新建页：
    - 可选 company_id：用于从公司详情入口跳转来“在该公司下新增”
    - 若带 company_id：需要 company.edit 权限
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    company = None
    if company_id:
        if not _has_company_perm(current_user, company_id, need="edit"):
            return _render_no_permission(
                request,
                current_user,
                active="legal_persons",
                message="你没有权限在该公司下新增法人（需要“编辑”权限）。",
                back_url=f"/ui/companies/{company_id}",
            )
        with engine.connect() as conn:
            company = conn.execute(
                text("SELECT id, company_name FROM companies WHERE id=:id LIMIT 1"),
                {"id": company_id},
            ).mappings().first()

    return templates.TemplateResponse(
        "legal_person_form.html",
        {
            **_base_ctx(request, current_user, "legal_persons"),
            "mode": "new",
            "person": {},
            "company": dict(company) if company else None,
            "can_edit": True,  # 新建页默认可提交（真正权限由入口 company_id 校验）
        },
    )


@router.get("/legal-persons/{person_id}/edit", response_class=HTMLResponse)
async def ui_legal_person_edit(
    request: Request,
    person_id: int,
    company_id: int | None = Query(default=None),
):
    """
    法人编辑页：
    - view 权限：_can_view_legal_person（基于绑定公司权限）
    - edit 权限：_can_edit_legal_person（基于绑定公司 can_edit）
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _can_view_legal_person(current_user, person_id):
        return _render_no_permission(
            request,
            current_user,
            active="legal_persons",
            message="你没有权限查看该法人信息（需要其绑定公司对你开放“查看”权限）。",
            back_url="/ui/legal-persons",
        )

    with engine.connect() as conn:
        person = conn.execute(
            text(
                """
                SELECT id, full_name, last_name, middle_name, first_name, birthday,
                       nationality, id_number, id_expiry_date,
                       passport_number, passport_expiry_date,
                       legal_address, postal_code, deleted_at
                FROM legal_persons
                WHERE id=:id
                LIMIT 1
                """
            ),
            {"id": person_id},
        ).mappings().first()

        if not person:
            raise HTTPException(status_code=404, detail="Legal person not found")

        company = None
        if company_id:
            company = conn.execute(
                text("SELECT id, company_name FROM companies WHERE id=:id LIMIT 1"),
                {"id": company_id},
            ).mappings().first()

    can_edit = _can_edit_legal_person(current_user, person_id)

    return templates.TemplateResponse(
        "legal_person_form.html",
        {
            **_base_ctx(request, current_user, "legal_persons"),
            "mode": "edit",
            "person": dict(person),
            "company": dict(company) if company else None,
            "can_edit": can_edit,
        },
    )


@router.post("/legal-persons/save")
async def ui_legal_person_save(
    request: Request,
    person_id: str = Form(""),
    company_id: str = Form(""),
    full_name: str = Form(...),
    last_name: str = Form(""),
    middle_name: str = Form(""),
    first_name: str = Form(""),
    birthday: str = Form(""),
    nationality: str = Form(""),
    id_number: str = Form(""),
    id_expiry_date: str = Form(""),
    passport_number: str = Form(""),
    passport_expiry_date: str = Form(""),
    legal_address: str = Form(""),
    postal_code: str = Form(""),
):
    """
    法人保存（新增/编辑）：
    - 编辑：需要 _can_edit_legal_person
    - 新增：直接 insert
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    pid = _q_int(person_id)
    cid = _q_int(company_id)
    # 编辑权限校验
    if pid:
        if not _can_edit_legal_person(current_user, pid):
            return _render_no_permission(
                request,
                current_user,
                active="legal_persons",
                message="你没有权限编辑该法人（需要其绑定公司对你开放“编辑”权限）。",
                back_url=f"/ui/legal-persons/{pid}/edit",
            )

    fn = (full_name or "").strip()
    if fn == "":
        if cid:
            return RedirectResponse(url=f"/ui/companies/{cid}", status_code=302)
        return RedirectResponse(url="/ui/legal-persons", status_code=302)

    def _date_or_none(s: str) -> str | None:
        s = (s or "").strip()
        return s if s else None

    payload = {
        "full_name": fn,
        "last_name": (last_name or "").strip() or None,
        "middle_name": (middle_name or "").strip() or None,
        "first_name": (first_name or "").strip() or None,
        "birthday": _date_or_none(birthday),
        "nationality": (nationality or "").strip() or None,
        "id_number": (id_number or "").strip() or None,
        "id_expiry_date": _date_or_none(id_expiry_date),
        "passport_number": (passport_number or "").strip() or None,
        "passport_expiry_date": _date_or_none(passport_expiry_date),
        "legal_address": (legal_address or "").strip() or None,
        "postal_code": (postal_code or "").strip() or None,
    }

    with engine.begin() as conn:
        if pid:
            conn.execute(
                text(
                    """
                    UPDATE legal_persons
                    SET
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
                    LIMIT 1
                    """
                ),
                {**payload, "id": pid},
            )
        else:
            conn.execute(
                text(
                    """
                    INSERT INTO legal_persons
                        (full_name, last_name, middle_name, first_name, birthday,
                         nationality, id_number, id_expiry_date,
                         passport_number, passport_expiry_date,
                         legal_address, postal_code,
                         created_at, updated_at, deleted_at)
                    VALUES
                        (:full_name, :last_name, :middle_name, :first_name, :birthday,
                         :nationality, :id_number, :id_expiry_date,
                         :passport_number, :passport_expiry_date,
                         :legal_address, :postal_code,
                         NOW(), NOW(), NULL)
                    """
                ),
                payload,
            )

    if cid:
        return RedirectResponse(url=f"/ui/companies/{cid}", status_code=302)

    return RedirectResponse(url="/ui/legal-persons", status_code=302)


@router.post("/legal-persons/{person_id}/delete")
async def ui_legal_person_delete(request: Request, person_id: int):
    """法人软删（deleted_at=NOW）（需要 _can_edit_legal_person）"""
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _can_edit_legal_person(current_user, person_id):
        return _render_no_permission(
            request,
            current_user,
            active="legal_persons",
            message="你没有权限删除该法人（需要其绑定公司对你开放“编辑”权限）。",
            back_url=f"/ui/legal-persons/{person_id}/edit",
        )

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE legal_persons
                SET deleted_at=NOW(), updated_at=NOW()
                WHERE id=:id
                LIMIT 1
                """
            ),
            {"id": person_id},
        )

    referer = request.headers.get("referer") or "/ui/legal-persons"
    return RedirectResponse(url=referer, status_code=302)


@router.post("/legal-persons/{person_id}/restore")
async def ui_legal_person_restore(request: Request, person_id: int):
    """法人恢复（deleted_at=NULL）（需要 _can_edit_legal_person）"""
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _can_edit_legal_person(current_user, person_id):
        return _render_no_permission(
            request,
            current_user,
            active="legal_persons",
            message="你没有权限恢复该法人（需要其绑定公司对你开放“编辑”权限）。",
            back_url="/ui/legal-persons",
        )

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE legal_persons
                SET deleted_at=NULL, updated_at=NOW()
                WHERE id=:id
                LIMIT 1
                """
            ),
            {"id": person_id},
        )

    referer = request.headers.get("referer") or "/ui/legal-persons?status=deleted"
    return RedirectResponse(url=referer, status_code=302)


# =========================================================
# UI: Documents Recycle（回收站：展示 + 硬删 + 批量恢复/硬删）
# =========================================================

# ⚠️ 注意：你这里又 import 了一次 os（重复 import 不影响运行，按“只整理不改逻辑”保留）

@router.get("/documents/recycle", response_class=HTMLResponse)
def documents_recycle(request: Request):
    """
    回收站列表页：
    - 展示 is_deleted=1 的文档
    - 计算 purge_at（预计清理时间）与 remaining_days（剩余天数）
    - msg：通过 querystring 回显批量操作结果
    """

    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")
    if not _is_admin(current_user):
        return _render_no_permission(
            request, current_user, active="documents",
            message="你没有权限查看回收站（仅管理员）。",
            back_url="/ui/documents",
        )


    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                    d.id,
                    d.company_id,
                    d.original_filename,
                    d.storage_path,
                    d.deleted_at,
                    -- 预计清理时间
                    DATE_ADD(d.deleted_at, INTERVAL :retention DAY) AS purge_at,
                    -- 剩余天数（按天）
                    GREATEST(0, :retention - TIMESTAMPDIFF(DAY, d.deleted_at, NOW())) AS remaining_days
                FROM documents d
                WHERE d.is_deleted = 1
                ORDER BY d.deleted_at DESC
                LIMIT 500
                """
            ),
            {"retention": DOC_RETENTION_DAYS},
        ).mappings().all()

    msg = request.query_params.get("msg")

    return templates.TemplateResponse(
        "documents_recycle.html",
        {
            "request": request,
            "rows": rows,
            "current_user": current_user,
            "retention_days": DOC_RETENTION_DAYS,
            "msg": msg,
            "now_iso": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        },
    )


@router.post("/documents/{doc_id}/hard-delete")
async def ui_document_hard_delete(request: Request, doc_id: int):
    """
    文档硬删除（转发给 API DELETE /documents/{doc_id}/hard-delete）：
    - 权限：仅 admin
    - 成功后回 /ui/documents
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _is_admin(current_user):
        return _render_no_permission(
            request,
            current_user,
            active="documents",
            message="你没有权限进行硬删除（仅管理员）。",
            back_url="/ui/documents",
        )

    token = _get_token_from_cookie(request)
    api_url = str(request.base_url).rstrip("/") + f"/documents/{doc_id}/hard-delete"

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.request(
                "DELETE",
                api_url,
                headers={"Authorization": f"Bearer {token}"},
            )
    except Exception as e:
        return HTMLResponse(f"硬删除失败：{e}", status_code=500)

    if resp.status_code in (401, 403):
        return _redirect("/ui/login")

    if resp.status_code >= 400:
        return HTMLResponse(
            f"硬删除失败：{resp.status_code} - {resp.text}",
            status_code=resp.status_code,
        )

    return RedirectResponse("/ui/documents", status_code=302)


@router.post("/documents/bulk-restore")
def ui_bulk_restore(
    request: Request,
    ids: List[int] = Form(default=[]),
):
    """
    批量恢复：
    - 先查出 (doc_id, company_id) 逐个校验 company.edit
    - 再批量 update documents
    - 最后回回收站页并带 msg
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    ids = [int(x) for x in ids if str(x).isdigit() and int(x) > 0]
    ids = sorted(set(ids))
    if not ids:
        qs = urlencode({"msg": "⚠️ 未选择任何文档"})
        return RedirectResponse(f"/ui/documents/recycle?{qs}", status_code=302)

    stmt_sel = text("SELECT id, company_id FROM documents WHERE id IN :ids").bindparams(
        bindparam("ids", expanding=True)
    )

    with engine.connect() as conn:
        rows = conn.execute(stmt_sel, {"ids": ids}).fetchall()

    # ✅ 逐个权限校验（需要 edit）
    for doc_id, company_id in rows:
        if not _has_company_perm(current_user, int(company_id), need="edit"):
            return _render_no_permission(
                request,
                current_user,
                active="documents",
                message=f"你没有权限恢复文档 ID={doc_id}（需要编辑权限）。",
                back_url="/ui/documents/recycle",
            )

    stmt_upd = text(
        """
        UPDATE documents
        SET is_deleted=0, deleted_at=NULL, updated_at=NOW()
        WHERE id IN :ids
        """
    ).bindparams(bindparam("ids", expanding=True))

    with engine.begin() as conn:
        conn.execute(stmt_upd, {"ids": ids})

    qs = urlencode({"msg": f"✅ 批量恢复完成：{len(ids)} 个"})
    return RedirectResponse(f"/ui/documents/recycle?{qs}", status_code=302)


@router.post("/documents/bulk-hard-delete")
def ui_bulk_hard_delete(
    request: Request,
    ids: List[int] = Form(default=[]),
):
    """
    批量硬删除：
    - 权限：admin only
    - 调 API：POST /documents/bulk-hard-delete，body={"ids":[...]}
    - 回回收站页并带 msg
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _is_admin(current_user):
        qs = urlencode({"msg": "❌ No permission (admin only)"})
        return RedirectResponse(f"/ui/documents/recycle?{qs}", status_code=302)

    token = _get_token_from_cookie(request)
    if not token:
        return _redirect("/ui/login")

    ids = [int(x) for x in ids if str(x).isdigit() and int(x) > 0]
    ids = sorted(set(ids))
    if not ids:
        qs = urlencode({"msg": "⚠️ 未选择任何文档"})
        return RedirectResponse(f"/ui/documents/recycle?{qs}", status_code=302)

    api_url = str(request.base_url).rstrip("/") + "/documents/bulk-hard-delete"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        with httpx.Client(timeout=60.0) as client:
            resp = client.post(api_url, json={"ids": ids}, headers=headers)
    except Exception:
        qs = urlencode({"msg": "❌ API 调用失败"})
        return RedirectResponse(f"/ui/documents/recycle?{qs}", status_code=302)

    if resp.status_code >= 400:
        qs = urlencode({"msg": f"❌ 硬删除失败 ({resp.status_code})"})
        return RedirectResponse(f"/ui/documents/recycle?{qs}", status_code=302)

    data = resp.json() if resp.content else {}
    msg = (
        f"✅ 批量硬删除完成："
        f"deleted={len(data.get('deleted', []))}, "
        f"skipped={len(data.get('skipped', []))}, "
        f"errors={len(data.get('errors', []))}"
    )
    qs = urlencode({"msg": msg})
    return RedirectResponse(f"/ui/documents/recycle?{qs}", status_code=302)


# =========================================================
# UI: Platform Detail / Save / Attach-Doc / Detach-Doc / Toggle-Doc / Upload-and-link
# =========================================================

@router.get("/companies/{company_id}/platforms/{platform_id}", response_class=HTMLResponse)
async def ui_company_platform_detail(request: Request, company_id: int, platform_id: int):
    """
    平台详情页：
    - 权限：company.view
    - 数据：
      - owner_options：用户下拉（用于 owner_user_id）
      - platform_images：平台图片列表
      - company / platform / docs（docs 带 is_linked 标记）
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _has_company_perm(current_user, company_id, need="view"):
        return _render_no_permission(
            request,
            current_user,
            "companies",
            "你没有权限查看该公司平台（需要“查看”权限）。",
            back_url=f"/ui/companies/{company_id}",
        )

    can_edit = _has_company_perm(current_user, company_id, need="edit")
    can_docs = _has_company_docs_perm(current_user, company_id)  # ✅ 新增

    # ✅ owner 下拉 + platform_images（你要求同一个 conn 作用域里，保留）
    with engine.connect() as conn:
        owner_options = conn.execute(
            text(
                """
                SELECT id, COALESCE(display_name, username) AS name, email
                FROM users
                WHERE status = 1
                ORDER BY id DESC
                LIMIT 300
                """
            )
        ).mappings().all()

        platform_images = conn.execute(
            text(
                """
                SELECT id, image_path, created_at
                FROM platform_images
                WHERE company_id=:cid AND platform_id=:pid
                ORDER BY id DESC
                LIMIT 50
                """
            ),
            {"cid": company_id, "pid": platform_id},
        ).mappings().all()

    # ✅ company / platform / docs（按你原逻辑再次开 conn）
    with engine.connect() as conn:
        company = conn.execute(
            text(
                "SELECT id, company_name, company_code FROM companies WHERE id=:id LIMIT 1"
            ),
            {"id": company_id},
        ).mappings().first()
        if not company:
            raise HTTPException(status_code=404, detail="Company not found")
        
        platform = conn.execute(
            text(
                """
                SELECT
                    id,
                    company_id,
                    platform_name,
                    store_url,
                    domain,
                    bank_card_no,
                    bank_card_owner,
                    bank_card_image,
                    created_at,
                    zini_ip,
                    platform_email,
                    progress,
                    status,
                    owner_user_id,
                    notes,
                    updated_at,
                    image_path
                FROM company_platforms
                WHERE id=:pid AND company_id=:cid
                LIMIT 1
                """
            ),
            {"pid": platform_id, "cid": company_id},
        ).mappings().first()

        if not platform:
            raise HTTPException(status_code=404, detail="Platform not found")

        docs = conn.execute(
            text(
                """
                SELECT
                    d.id,
                    d.title,
                    d.category,
                    d.file_type,
                    d.created_at,
                    d.is_deleted,
                    CASE WHEN pd.id IS NULL THEN 0 ELSE 1 END AS is_linked
                FROM documents d
                LEFT JOIN platform_documents pd
                  ON pd.document_id = d.id
                 AND pd.platform_id = :pid
                 AND pd.company_id = :cid
                WHERE d.company_id = :cid
                  AND d.is_deleted = 0
                ORDER BY d.created_at DESC, d.id DESC
                LIMIT 200
                """
            ),
            {"cid": company_id, "pid": platform_id},
        ).mappings().all()

    can_edit = _has_company_perm(current_user, company_id, need="edit")

    return templates.TemplateResponse(
        "company_platform_detail.html",
        {
            **_base_ctx(request, current_user, "companies"),
            "owner_options": owner_options,
            "company": dict(company),
            "platform_images": platform_images,
            "platform": dict(platform),
            "docs": docs,
            "can_edit": can_edit,
            "can_docs": can_docs,
        },
    )


@router.post("/companies/{company_id}/platforms/{platform_id}/save")
async def ui_company_platform_save(
    request: Request,
    company_id: int,
    platform_id: int,
    platform_name: str = Form(""),
    store_url: str = Form(""),
    domain: str = Form(""),
    zini_ip: str = Form(""),
    bank_card_no: str = Form(""),
    bank_card_owner: str = Form(""),
    platform_email: str = Form(""),
    progress: str = Form(""),
    status: str = Form(""),
    owner_user_id: str = Form(""),
    notes: str = Form(""),
):
    """
    平台详情保存：
    - 权限：company.edit
    - 逻辑：payload 构建后，动态拼 SET（仅跳过 platform_name=None 的情况）
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _has_company_perm(current_user, company_id, need="edit"):
        return _render_no_permission(
            request,
            current_user,
            "companies",
            "你没有权限编辑平台信息（需要“编辑”权限）。",
            back_url=f"/ui/companies/{company_id}/platforms/{platform_id}",
        )

    def _int_or_none(v: str):
        v = (v or "").strip()
        if not v:
            return None
        try:
            return int(v)
        except Exception:
            return None

    payload = {
        "platform_name": (platform_name or "").strip() or None,
        "store_url": (store_url or "").strip() or None,
        "domain": (domain or "").strip() or None,
        "bank_card_no": (bank_card_no or "").strip() or None,
        "bank_card_owner": (bank_card_owner or "").strip() or None,
        "zini_ip": (zini_ip or "").strip() or None,
        "platform_email": (platform_email or "").strip() or None,
        "progress": _int_or_none(progress),
        "status": (status or "").strip() or None,
        "owner_user_id": _int_or_none(owner_user_id),
        "notes": (notes or "").strip() or None,
    }

    # platform_name 你可以强制必填；这里按你原平台逻辑：为空就不改
    sets = []
    params = {"cid": company_id, "pid": platform_id}
    for k, v in payload.items():
        if v is None and k in ("platform_name",):  # platform_name 不允许写 NULL 时就跳过
            continue
        sets.append(f"{k} = :{k}")
        params[k] = v

    if not sets:
        return RedirectResponse(url=f"/ui/companies/{company_id}/platforms/{platform_id}", status_code=302)

    with engine.begin() as conn:
        rc = conn.execute(
            text(
                f"""
                UPDATE company_platforms
                SET {", ".join(sets)}, updated_at=NOW()
                WHERE id=:pid AND company_id=:cid
                LIMIT 1
                """
            ),
            params,
        ).rowcount

        if rc == 0:
            raise HTTPException(status_code=404, detail="Platform not found")

    return RedirectResponse(url=f"/ui/companies/{company_id}/platforms/{platform_id}", status_code=302)


@router.post("/companies/{company_id}/platforms/{platform_id}/bank-card-image")
async def ui_company_platform_bank_image_upload(
    request: Request,
    company_id: int,
    platform_id: int,
    file: UploadFile = File(...),
):
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _has_company_perm(current_user, company_id, need="edit"):
        return _render_no_permission(
            request,
            current_user,
            "companies",
            "你没有权限编辑平台信息（需要“编辑”权限）。",
            back_url=f"/ui/companies/{company_id}/platforms/{platform_id}",
        )

    if not file or not file.filename:
        return RedirectResponse(
            url=f"/ui/companies/{company_id}/platforms/{platform_id}",
            status_code=HTTP_303_SEE_OTHER,
        )

    ext = os.path.splitext(file.filename or "")[1].lower() or ".jpg"
    filename = f"bankcard_{company_id}_{platform_id}_{uuid.uuid4().hex}{ext}"
    upload_dir = os.path.abspath(os.path.join(os.getcwd(), "uploads", "bankcards"))
    os.makedirs(upload_dir, exist_ok=True)
    dest_path = os.path.join(upload_dir, filename)

    with open(dest_path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    rel_path = os.path.relpath(dest_path, os.getcwd())

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE company_platforms
                SET bank_card_image=:p, updated_at=NOW()
                WHERE id=:pid AND company_id=:cid
                LIMIT 1
                """
            ),
            {"p": rel_path, "pid": platform_id, "cid": company_id},
        )

    return RedirectResponse(
        url=f"/ui/companies/{company_id}/platforms/{platform_id}",
        status_code=HTTP_303_SEE_OTHER,
    )


@router.post("/companies/{company_id}/platforms/{platform_id}/bank-card-image/delete")
async def ui_company_platform_bank_image_delete(
    request: Request,
    company_id: int,
    platform_id: int,
):
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _has_company_perm(current_user, company_id, need="edit"):
        return _render_no_permission(
            request,
            current_user,
            "companies",
            "你没有权限编辑平台信息（需要“编辑”权限）。",
            back_url=f"/ui/companies/{company_id}/platforms/{platform_id}",
        )

    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT bank_card_image
                FROM company_platforms
                WHERE id=:pid AND company_id=:cid
                LIMIT 1
                """
            ),
            {"pid": platform_id, "cid": company_id},
        ).mappings().first()

        if row and row["bank_card_image"]:
            path = os.path.abspath(os.path.join(os.getcwd(), row["bank_card_image"]))
            try:
                if os.path.exists(path):
                    os.remove(path)
            except Exception:
                pass

        conn.execute(
            text(
                """
                UPDATE company_platforms
                SET bank_card_image=NULL, updated_at=NOW()
                WHERE id=:pid AND company_id=:cid
                LIMIT 1
                """
            ),
            {"pid": platform_id, "cid": company_id},
        )

    return RedirectResponse(
        url=f"/ui/companies/{company_id}/platforms/{platform_id}",
        status_code=HTTP_303_SEE_OTHER,
    )


@router.get("/companies/{company_id}/platforms/{platform_id}/bank-card-image")
async def ui_company_platform_bank_image_file(
    request: Request,
    company_id: int,
    platform_id: int,
):
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _has_company_perm(current_user, company_id, need="view"):
        return _render_no_permission(
            request,
            current_user,
            "companies",
            "你没有权限查看平台信息（需要“查看”权限）。",
            back_url=f"/ui/companies/{company_id}/platforms/{platform_id}",
        )

    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT bank_card_image
                FROM company_platforms
                WHERE id=:pid AND company_id=:cid
                LIMIT 1
                """
            ),
            {"pid": platform_id, "cid": company_id},
        ).mappings().first()

    if not row or not row["bank_card_image"]:
        raise HTTPException(status_code=404, detail="No bank card image")

    path = os.path.abspath(os.path.join(os.getcwd(), row["bank_card_image"]))
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File missing")

    return FileResponse(path)

@router.post("/companies/{company_id}/platforms/{platform_id}/attach-doc")
async def ui_platform_attach_doc(
    request: Request,
    company_id: int,
    platform_id: int,
    document_id: str = Form(...),
):
    """
    绑定文档（平台-文档关联）：
    - 权限：company.edit
    - 防跨公司：确认 documents.company_id == company_id
    - 去重：INSERT IGNORE
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _has_company_perm(current_user, company_id, need="edit"):
        return _render_no_permission(
            request,
            current_user,
            "companies",
            "你没有权限绑定文件（需要“编辑”权限）。",
            back_url=f"/ui/companies/{company_id}/platforms/{platform_id}",
        )

    try:
        doc_id = int(document_id)
    except Exception:
        return RedirectResponse(url=f"/ui/companies/{company_id}/platforms/{platform_id}", status_code=302)

    with engine.begin() as conn:
        ok = conn.execute(
            text("SELECT id FROM documents WHERE id=:id AND company_id=:cid LIMIT 1"),
            {"id": doc_id, "cid": company_id},
        ).scalar()
        if not ok:
            return RedirectResponse(url=f"/ui/companies/{company_id}/platforms/{platform_id}", status_code=302)

        conn.execute(
            text(
                """
                INSERT IGNORE INTO platform_documents (company_id, platform_id, document_id, created_at)
                VALUES (:cid, :pid, :did, NOW())
                """
            ),
            {"cid": company_id, "pid": platform_id, "did": doc_id},
        )

    return RedirectResponse(url=f"/ui/companies/{company_id}/platforms/{platform_id}", status_code=302)


@router.post("/companies/{company_id}/platforms/{platform_id}/detach-doc")
async def ui_platform_detach_doc(
    request: Request,
    company_id: int,
    platform_id: int,
    document_id: str = Form(...),
):
    """解绑文档（需要 company.edit）"""
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _has_company_perm(current_user, company_id, need="edit"):
        return _render_no_permission(
            request,
            current_user,
            "companies",
            "你没有权限解绑文件（需要“编辑”权限）。",
            back_url=f"/ui/companies/{company_id}/platforms/{platform_id}",
        )

    try:
        doc_id = int(document_id)
    except Exception:
        return RedirectResponse(url=f"/ui/companies/{company_id}/platforms/{platform_id}", status_code=302)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                DELETE FROM platform_documents
                WHERE company_id=:cid AND platform_id=:pid AND document_id=:did
                LIMIT 1
                """
            ),
            {"cid": company_id, "pid": platform_id, "did": doc_id},
        )

    return RedirectResponse(url=f"/ui/companies/{company_id}/platforms/{platform_id}", status_code=302)


@router.post("/companies/{company_id}/platforms/{platform_id}/toggle-doc")
async def ui_platform_toggle_doc(
    request: Request,
    company_id: int,
    platform_id: int,
    document_id: str = Form(...),
):
    """
    toggle 绑定/解绑（一个按钮）：
    - 校验平台属于公司
    - 校验文档属于公司
    - 存在 link 则删，不存在则插
    - 权限：company.edit
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _has_company_perm(current_user, company_id, need="edit"):
        return _render_no_permission(
            request,
            current_user,
            active="companies",
            message="你没有权限绑定/解绑文件（需要“编辑”权限）。",
            back_url=f"/ui/companies/{company_id}/platforms/{platform_id}",
        )

    try:
        doc_id = int(document_id)
    except Exception:
        return RedirectResponse(url=f"/ui/companies/{company_id}/platforms/{platform_id}", status_code=302)

    with engine.begin() as conn:
        p = conn.execute(
            text("SELECT id FROM company_platforms WHERE id=:pid AND company_id=:cid LIMIT 1"),
            {"pid": platform_id, "cid": company_id},
        ).scalar()
        if not p:
            raise HTTPException(status_code=404, detail="Platform not found")

        d = conn.execute(
            text("SELECT id FROM documents WHERE id=:did AND company_id=:cid LIMIT 1"),
            {"did": doc_id, "cid": company_id},
        ).scalar()
        if not d:
            return RedirectResponse(url=f"/ui/companies/{company_id}/platforms/{platform_id}", status_code=302)

        link_id = conn.execute(
            text(
                """
                SELECT id
                FROM platform_documents
                WHERE company_id=:cid AND platform_id=:pid AND document_id=:did
                LIMIT 1
                """
            ),
            {"cid": company_id, "pid": platform_id, "did": doc_id},
        ).scalar()

        if link_id:
            conn.execute(
                text("DELETE FROM platform_documents WHERE id=:id LIMIT 1"),
                {"id": int(link_id)},
            )
        else:
            conn.execute(
                text(
                    """
                    INSERT INTO platform_documents (company_id, platform_id, document_id, created_at)
                    VALUES (:cid, :pid, :did, NOW())
                    """
                ),
                {"cid": company_id, "pid": platform_id, "did": doc_id},
            )

    return RedirectResponse(url=f"/ui/companies/{company_id}/platforms/{platform_id}", status_code=302)


def _extract_doc_id(payload: Any) -> Optional[int]:
    """
    从 upload API 响应里提取 doc_id（兼容多种结构）：
    - {"id": 123}
    - {"doc_id": 123}
    - {"item": {"id": 123}}
    - {"document": {"id": 123}}
    """
    if not payload:
        return None
    if isinstance(payload, dict):
        for k in ("id", "doc_id", "document_id"):
            v = payload.get(k)
            if isinstance(v, int):
                return v
            if isinstance(v, str) and v.isdigit():
                return int(v)
        for k in ("item", "document"):
            v = payload.get(k)
            if isinstance(v, dict):
                vv = v.get("id")
                if isinstance(vv, int):
                    return vv
                if isinstance(vv, str) and vv.isdigit():
                    return int(vv)
    return None


@router.post("/companies/{company_id}/platforms/{platform_id}/upload-and-link")
async def ui_platform_upload_and_link(
    request: Request,
    company_id: int,
    platform_id: int,
    file: UploadFile = File(...),
    category: str = Form("原件"),
    title: str = Form(""),
    file_type: str = Form(""),
    dedup: str = Form("true"),
):
    """
    平台页“上传并绑定”：
    1) 校验 company.edit
    2) 校验平台属于公司
    3) 复用 /documents/upload 上传文件
    4) 若能从返回中提取 doc_id，则写 platform_documents 关联
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _has_company_perm(current_user, company_id, need="edit"):
        return _render_no_permission(
            request,
            current_user,
            active="companies",
            message="你没有权限在该平台上传并绑定文件（需要“编辑”权限）。",
            back_url=f"/ui/companies/{company_id}/platforms/{platform_id}",
        )

    with engine.connect() as conn:
        p = conn.execute(
            text("SELECT id FROM company_platforms WHERE id=:pid AND company_id=:cid LIMIT 1"),
            {"pid": platform_id, "cid": company_id},
        ).scalar()
    if not p:
        return RedirectResponse(url=f"/ui/companies/{company_id}/platforms/{platform_id}", status_code=302)

    category_s = (category or "").strip() or "原件"
    title_s = (title or "").strip()
    if not title_s:
        title_s = _title_from_filename(file.filename)
    file_type_s = (file_type or "").strip()
    dedup_s = (dedup or "true").strip().lower()
    if dedup_s not in ("true", "false"):
        dedup_s = "true"

    group_key_s = _make_group_key(company_id)

    token = _get_token_from_cookie(request)
    api_url = str(request.base_url).rstrip("/") + "/documents/upload"

    try:
        try:
            file.file.seek(0)
        except Exception:
            pass

        async with httpx.AsyncClient(timeout=300.0) as client:
            files = {"file": (file.filename, file.file, file.content_type)}
            data = {
                "company_id": str(company_id),
                "category": category_s,
                "title": title_s,
                "file_type": file_type_s,
                "dedup": dedup_s,
                "group_key": group_key_s,
            }
            resp = await client.post(
                api_url,
                headers={"Authorization": f"Bearer {token}"},
                data=data,
                files=files,
            )
    except Exception as e:
        return RedirectResponse(url=f"/ui/companies/{company_id}/platforms/{platform_id}", status_code=302)

    if resp.status_code >= 400:
        return RedirectResponse(url=f"/ui/companies/{company_id}/platforms/{platform_id}", status_code=302)

    payload = resp.json() if resp.content else {}
    doc_id = _extract_doc_id(payload)

    if doc_id:
        with engine.begin() as conn:
            exists = conn.execute(
                text(
                    """
                    SELECT id
                    FROM platform_documents
                    WHERE company_id=:cid AND platform_id=:pid AND document_id=:did
                    LIMIT 1
                    """
                ),
                {"cid": company_id, "pid": platform_id, "did": doc_id},
            ).scalar()

            if not exists:
                conn.execute(
                    text(
                        """
                        INSERT INTO platform_documents (company_id, platform_id, document_id, created_at)
                        VALUES (:cid, :pid, :did, NOW())
                        """
                    ),
                    {"cid": company_id, "pid": platform_id, "did": doc_id},
                )

    return RedirectResponse(url=f"/ui/companies/{company_id}/platforms/{platform_id}", status_code=302)


# =========================================================
# UI: Platform Images（上传 / 访问 / 删除 / 替换）
# =========================================================

PLATFORM_IMG_BASE = os.getenv("PLATFORM_IMG_BASE", "uploads/platforms")


@router.post("/companies/{company_id}/platforms/{platform_id}/images/upload")
async def ui_platform_images_upload(
    request: Request,
    company_id: int,
    platform_id: int,
    images: List[UploadFile] = File(...),  # 多图
):
    """
    平台多图上传：
    - 权限：company.edit
    - 保存路径：{PLATFORM_IMG_BASE}/{company_id}/{platform_id}/{uuid}.{ext}
    - DB：platform_images(company_id, platform_id, image_path)
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _has_company_perm(current_user, company_id, need="edit"):
        return _render_no_permission(
            request,
            current_user,
            "companies",
            "你没有权限上传平台图片（需要编辑权限）。",
            f"/ui/companies/{company_id}/platforms/{platform_id}",
        )

    with engine.connect() as conn:
        ok = conn.execute(
            text("SELECT 1 FROM company_platforms WHERE id=:pid AND company_id=:cid LIMIT 1"),
            {"pid": platform_id, "cid": company_id},
        ).scalar()
    if not ok:
        return RedirectResponse(f"/ui/companies/{company_id}/platforms/{platform_id}", status_code=302)

    folder = os.path.join(PLATFORM_IMG_BASE, str(company_id), str(platform_id))
    os.makedirs(folder, exist_ok=True)

    rows = []
    for img in images:
        ct = (img.content_type or "").lower()
        if not ct.startswith("image/"):
            continue

        ext = ".png"
        if "jpeg" in ct or "jpg" in ct:
            ext = ".jpg"
        elif "webp" in ct:
            ext = ".webp"

        filename = f"{uuid.uuid4().hex}{ext}"
        abs_path = os.path.join(folder, filename)
        rel_path = abs_path.replace("\\", "/")

        content = await img.read()
        with open(abs_path, "wb") as f:
            f.write(content)

        rows.append({"cid": company_id, "pid": platform_id, "p": rel_path})

    if rows:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO platform_images(company_id, platform_id, image_path)
                    VALUES (:cid, :pid, :p)
                    """
                ),
                rows,
            )

    return RedirectResponse(f"/ui/companies/{company_id}/platforms/{platform_id}", status_code=302)


@router.get("/platform-images/{image_id}")
async def ui_platform_image_file(request: Request, image_id: int):
    """
    访问平台图片文件：
    - 根据 image_id 找到 image_path
    - 权限：对 image 所属 company 需要 company.view
    - 返回 FileResponse
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT company_id, image_path FROM platform_images WHERE id=:id LIMIT 1"),
            {"id": image_id},
        ).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="No image")

    company_id = int(row["company_id"])
    if not _has_company_perm(current_user, company_id, need="view"):
        return _render_no_permission(
            request,
            current_user,
            "companies",
            "你没有权限查看该平台图片（需要查看权限）。",
            "/ui/companies",
        )

    path = row["image_path"]
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File missing")

    return FileResponse(path)


@router.post("/companies/{company_id}/platforms/{platform_id}/images/{image_id}/delete")
async def ui_platform_image_delete_one(request: Request, company_id: int, platform_id: int, image_id: int):
    """
    删除单张平台图片：
    - 权限：company.edit
    - 删除 DB 记录 + 尝试删除本地文件
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _has_company_perm(current_user, company_id, need="edit"):
        return _render_no_permission(
            request,
            current_user,
            "companies",
            "你没有权限删除平台图片（需要编辑权限）。",
            f"/ui/companies/{company_id}/platforms/{platform_id}",
        )

    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT image_path FROM platform_images
                WHERE id=:iid AND company_id=:cid AND platform_id=:pid
                LIMIT 1
                """
            ),
            {"iid": image_id, "cid": company_id, "pid": platform_id},
        ).mappings().first()

        if row:
            p = row["image_path"]
            conn.execute(text("DELETE FROM platform_images WHERE id=:iid LIMIT 1"), {"iid": image_id})
            try:
                if p and os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass

    return RedirectResponse(f"/ui/companies/{company_id}/platforms/{platform_id}", status_code=302)


@router.post("/companies/{company_id}/platforms/{platform_id}/images/{image_id}/replace")
async def ui_platform_image_replace_one(
    request: Request,
    company_id: int,
    platform_id: int,
    image_id: int,
    image: UploadFile = File(...),
):
    """
    替换单张平台图片：
    - 权限：company.edit
    - 保存新文件 -> 更新/插入 DB -> 尝试删除旧文件
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    if not _has_company_perm(current_user, company_id, need="edit"):
        return _render_no_permission(
            request,
            current_user,
            "companies",
            "你没有权限替换平台图片（需要编辑权限）。",
            f"/ui/companies/{company_id}/platforms/{platform_id}",
        )

    ct = (image.content_type or "").lower()
    if not ct.startswith("image/"):
        return RedirectResponse(f"/ui/companies/{company_id}/platforms/{platform_id}", status_code=302)

    ext = ".png"
    if "jpeg" in ct or "jpg" in ct:
        ext = ".jpg"
    elif "webp" in ct:
        ext = ".webp"

    folder = os.path.join(PLATFORM_IMG_BASE, str(company_id), str(platform_id))
    os.makedirs(folder, exist_ok=True)

    new_filename = f"{uuid.uuid4().hex}{ext}"
    abs_path = os.path.join(folder, new_filename)
    rel_path = abs_path.replace("\\", "/")

    content = await image.read()
    with open(abs_path, "wb") as f:
        f.write(content)

    with engine.begin() as conn:
        old = conn.execute(
            text(
                """
                SELECT image_path FROM platform_images
                WHERE id=:iid AND company_id=:cid AND platform_id=:pid
                LIMIT 1
                """
            ),
            {"iid": image_id, "cid": company_id, "pid": platform_id},
        ).mappings().first()

        if not old:
            conn.execute(
                text("INSERT INTO platform_images(company_id, platform_id, image_path) VALUES (:cid, :pid, :p)"),
                {"cid": company_id, "pid": platform_id, "p": rel_path},
            )
        else:
            conn.execute(
                text("UPDATE platform_images SET image_path=:p WHERE id=:iid LIMIT 1"),
                {"p": rel_path, "iid": image_id},
            )
            try:
                if old["image_path"] and os.path.exists(old["image_path"]):
                    os.remove(old["image_path"])
            except Exception:
                pass

    return RedirectResponse(f"/ui/companies/{company_id}/platforms/{platform_id}", status_code=302)


# =========================================================
# UI: Documents（修改分类）
# =========================================================
@router.post("/documents/{document_id}/set-category")
async def ui_document_set_category(
    request: Request,
    document_id: int,
    category: str = Form(""),
):
    """
    修改文档分类：
    - 先查 documents.company_id 做权限校验（company.edit）
    - allowed：{"原件","翻译"}
    - 成功后回 referer
    """
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return _redirect("/ui/login")

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id, company_id FROM documents WHERE id=:id LIMIT 1"),
            {"id": document_id},
        ).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="Document not found")

    company_id = int(row["company_id"])

    if not _has_company_perm(current_user, company_id, need="edit"):
        return _render_no_permission(
            request,
            current_user,
            "companies",
            "你没有权限修改文档分类（需要编辑权限）。",
            back_url=f"/ui/documents/{document_id}",
        )

    category = (category or "").strip()
    allowed = {"原件", "翻译"}
    if category not in allowed:
        return RedirectResponse(url=f"/ui/companies/{company_id}/platforms", status_code=302)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE documents
                SET category=:c, updated_at=NOW()
                WHERE id=:id
                LIMIT 1
                """
            ),
            {"c": category, "id": document_id},
        )

    referer = request.headers.get("referer") or f"/ui/documents/{document_id}"
    return RedirectResponse(url=referer, status_code=302)


