import os
import httpx
from typing import Dict, Any

FEISHU_WEBHOOK = (os.getenv("FEISHU_WEBHOOK") or "").strip()
APP_PUBLIC_BASE_URL = (os.getenv("APP_PUBLIC_BASE_URL") or "").strip().rstrip("/")

def send_feishu_text_sync(text: str) -> Dict[str, Any]:
    if not FEISHU_WEBHOOK:
        return {"skipped": True, "reason": "FEISHU_WEBHOOK empty"}

    payload = {"msg_type": "text", "content": {"text": text}}

    with httpx.Client(timeout=10) as client:
        r = client.post(FEISHU_WEBHOOK, json=payload)
        # 飞书经常 http 200 + code!=0，所以不要 raise_for_status 直接看 body
        try:
            data = r.json()
        except Exception:
            data = {"raw": r.text}
        return {"http_status": r.status_code, "data": data}

def make_ticket_url(ticket_id: int) -> str:
    if not APP_PUBLIC_BASE_URL:
        # 没公网也行：先给本地链接，至少开发环境点得开
        return f"http://127.0.0.1:8000/ui/tickets/{ticket_id}"
    return f"{APP_PUBLIC_BASE_URL}/ui/tickets/{ticket_id}"

# app/services/feishu_notify.py
from sqlalchemy import text
from app.db import engine

def get_ticket_notify_context(ticket_id: int) -> dict:
    with engine.connect() as conn:
        t = conn.execute(text("""
            SELECT
              t.id,
              t.ticket_no,
              t.title,
              t.status,
              t.priority,
              t.company_name,
              t.assignee_user_id,

              COALESCE(u_req.display_name, u_req.username)  AS requester_name,
              COALESCE(u_act.display_name, u_act.username)  AS actor_name,
              COALESCE(u_asg.display_name, u_asg.username)  AS assignee_name

            FROM tickets t
            LEFT JOIN users u_req ON u_req.id = t.requester_user_id
            LEFT JOIN users u_asg ON u_asg.id = t.assignee_user_id
            LEFT JOIN users u_act ON u_act.id = t.updated_by   -- 如果你有 updated_by；没有就由调用方传 actor_id 再查
            WHERE t.id=:id
            LIMIT 1
        """), {"id": ticket_id}).mappings().first()

    return dict(t) if t else {}