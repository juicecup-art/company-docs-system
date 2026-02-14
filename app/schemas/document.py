# app/schemas/document.py

from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
from typing import Optional
from datetime import datetime
from pydantic import BaseModel


class ReplaceResult(BaseModel):
    ok: bool
    old_id: int
    new_id: int
    deduped: bool
    old_deleted: bool
    deleted_old_file: bool = False


class DocumentOut(BaseModel):
    id: int
    company_id: int
    uploaded_by: int

    category: str
    title: str
    file_type: str | None = None   # ✅ 加这里
    original_filename: str
    storage_path: str

    mime_type: Optional[str] = None
    file_size: Optional[int] = None
    file_sha256: Optional[str] = None

    is_deleted: int
    deleted_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

class DocumentOutWithDedup(DocumentOut):
    deduped: bool