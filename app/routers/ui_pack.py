from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Request, Form
from fastapi.responses import RedirectResponse
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from app.db import engine
from app.routers.ui import _get_current_user_for_ui, _has_company_perm


router = APIRouter(prefix="/ui", tags=["ui-pack"])


@router.post("/companies/{company_id}/platforms/add-pack")
async def ui_company_platform_add_pack(
    request: Request,
    company_id: int,
    platform_name: str = Form(...),
    store_url: str = Form(""),
    domain: str = Form(""),
):
    current_user = _get_current_user_for_ui(request)
    if not current_user:
        return RedirectResponse(url="/ui/login", status_code=302)

    if not _has_company_perm(current_user, company_id, need="edit"):
        return RedirectResponse(
            url=f"/ui/companies/{company_id}/platforms",
            status_code=302,
        )

    platform_name = (platform_name or "").strip()
    store_url = (store_url or "").strip() or None
    domain = (domain or "").strip() or None

    if not platform_name:
        return RedirectResponse(
            url=f"/ui/companies/{company_id}/platforms",
            status_code=302,
        )

    platform_key = platform_name.strip()
    packaging_name = platform_key if "包装法" in platform_name else None

    with engine.begin() as conn:
        exists = conn.execute(
            text(
                """
                SELECT id
                FROM company_platforms
                WHERE company_id=:cid AND platform_name=:pname
                LIMIT 1
                """
            ),
            {"cid": company_id, "pname": platform_key},
        ).first()

        if exists:
            return RedirectResponse(
                url=f"/ui/companies/{company_id}/platforms?msg=平台已存在",
                status_code=302,
            )

        try:
            conn.execute(
                text(
                    """
                    INSERT INTO company_platforms
                        (company_id, platform_name, store_url, domain, packaging_name, created_at)
                    VALUES
                        (:cid, :pname, :store_url, :domain, :packaging_name, NOW())
                    """
                ),
                {
                    "cid": company_id,
                    "pname": platform_key,
                    "store_url": store_url,
                    "domain": domain,
                    "packaging_name": packaging_name,
                },
            )
        except IntegrityError:
            return RedirectResponse(
                url=f"/ui/companies/{company_id}/platforms?msg=平台已存在",
                status_code=302,
            )

    return RedirectResponse(
        url=f"/ui/companies/{company_id}/platforms",
        status_code=302,
    )
