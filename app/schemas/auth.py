# app/schemas/auth.py
from pydantic import BaseModel


class LoginIn(BaseModel):
    username: str
    password: str


class TokenOut(BaseModel):
    access_token: str
    token_type: str = "bearer"


class UserMeOut(BaseModel):
    id: int
    username: str
    display_name: str | None = None
    email: str | None = None
    phone: str | None = None
    department: str | None = None
    role: str | None = None
    status: int
