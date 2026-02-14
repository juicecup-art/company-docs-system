# app/auth/jwt.py
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Dict

from jose import jwt, JWTError

ALGORITHM = "HS256"


def create_access_token(
    data: Dict[str, Any],
    secret_key: str,
    expires_minutes: int = 60 * 8,  # 默认 8 小时
) -> str:
    to_encode = dict(data)
    expire = datetime.now(timezone.utc) + timedelta(minutes=expires_minutes)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, secret_key, algorithm=ALGORITHM)


def decode_access_token(token: str, secret_key: str) -> Optional[Dict[str, Any]]:
    try:
        payload = jwt.decode(token, secret_key, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        return None
