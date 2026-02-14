# app/routers/auth.py
from fastapi import APIRouter, HTTPException
from sqlalchemy import text

from app.db import engine
from app.schemas.auth import LoginIn, TokenOut, UserMeOut
from app.auth.password import verify_password
from app.auth.jwt import create_access_token
from app.auth.deps import get_current_user, get_secret_key
from fastapi import Depends

router = APIRouter(prefix="/auth", tags=["Auth"])


@router.post("/login", response_model=TokenOut)
def login(data: LoginIn):
    # 1) 查用户
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT id, username, password_hash, status
                FROM users
                WHERE username=:u
                LIMIT 1
                """
            ),
            {"u": data.username},
        ).fetchone()

    if not row:
        raise HTTPException(status_code=400, detail="Invalid username or password")

    user = dict(row._mapping)

    if user.get("status") != 1:
        raise HTTPException(status_code=403, detail="User disabled")

    # 2) 校验密码
    if not verify_password(data.password, user["password_hash"]):
        raise HTTPException(status_code=400, detail="Invalid username or password")

    # 3) 发 JWT（sub = user id）
    token = create_access_token(
        data={"sub": str(user["id"])},
        secret_key=get_secret_key(),
        expires_minutes=60 * 8,
    )
    return {"access_token": token, "token_type": "bearer"}


@router.get("/me", response_model=UserMeOut)
def me(current_user=Depends(get_current_user)):
    return current_user
