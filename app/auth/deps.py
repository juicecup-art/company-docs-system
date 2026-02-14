# app/auth/deps.py
from __future__ import annotations

import os
from typing import Optional, Dict, Any

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import text

from app.db import engine
from app.auth.jwt import decode_access_token

security = HTTPBearer(auto_error=False)


def get_secret_key() -> str:
    key = os.environ.get("JWT_SECRET")
    if not key:
        raise RuntimeError("Missing JWT_SECRET in environment")
    return key


def get_current_user(
    request: Request,
    cred: HTTPAuthorizationCredentials = Depends(security),
) -> Dict[str, Any]:
    # 1) Header: Authorization: Bearer xxx
    token = None
    if cred and cred.scheme and cred.scheme.lower() == "bearer":
        token = cred.credentials

    # 2) Cookie: access_token（Jinja2 UI 用）
    if not token:
        token = request.cookies.get("access_token")

    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    payload = decode_access_token(token, get_secret_key())
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid token")

    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token payload")

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
            {"id": int(user_id)},
        ).mappings().first()

    if not row:
        raise HTTPException(status_code=401, detail="User not found")

    user = dict(row)
    if int(user.get("status") or 0) != 1:
        raise HTTPException(status_code=403, detail="User disabled")

    return user


def require_admin(user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    """
    给 API 管理员接口用：必须是 admin
    """
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    return user
