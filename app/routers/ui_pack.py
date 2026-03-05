from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Request, Form, Response
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
    step = "start"
    try:
        step = "auth_user"
        current_user = _get_current_user_for_ui(request)
        if not current_user:
            return Response(content="DEBUG_FAIL:auth_no_user", status_code=401)

        step = "check_perm"
        if not _has_company_perm(current_user, company_id, need="edit"):
            return Response(content="DEBUG_FAIL:no_permission", status_code=403)

        step = "sanitize_input"
        platform_name = (platform_name or "").strip()
        store_url = (store_url or "").strip() or None
        domain = (domain or "").strip() or None

        if not platform_name:
            return Response(content="DEBUG_FAIL:empty_platform_name", status_code=400)

        step = "compute_packing_name"
        platform_key = platform_name.strip()
        packing_name = platform_key if "包装法" in platform_name else None

        # 如果是“包装法”记录，则 platform_name 留空，只写 packing_name
        platform_value = None if packing_name else platform_key

        step = "db_exists_check"
        with engine.begin() as conn:
            # 对“包装法”记录，用 packing_name 做查重；
            # 其他情况用 platform_name 做查重。
            if packing_name:
                exists = conn.execute(
                    text(
                        """
                        SELECT id
                        FROM company_platforms
                        WHERE company_id=:cid AND packing_name=:pname
                        LIMIT 1
                        """
                    ),
                    {"cid": company_id, "pname": packing_name},
                ).first()
            else:
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
                return Response(content="DEBUG_FAIL:platform_exists", status_code=400)

            step = "db_insert"
            try:
                conn.execute(
                    text(
                        """
                        INSERT INTO company_platforms
                            (company_id, platform_name, store_url, domain, packing_name, created_at)
                        VALUES
                            (:cid, :platform_name, :store_url, :domain, :packing_name, NOW())
                        """
                    ),
                    {
                        "cid": company_id,
                        "platform_name": platform_value,
                        "store_url": store_url,
                        "domain": domain,
                        "packing_name": packing_name,
                    },
                )
            except IntegrityError as e:
                return Response(
                    content=f"DEBUG_FAIL:integrity_error:{e}",
                    status_code=400,
                )

        step = "success"
        return RedirectResponse(
            url=f"/ui/companies/{company_id}/platforms",
            status_code=302,
        )
    except Exception as e:
        return Response(
            content=f"DEBUG_FAIL:{step}:{type(e).__name__}:{e}",
            status_code=500,
        )
