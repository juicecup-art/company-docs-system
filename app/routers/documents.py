from pydantic import BaseModel

class DocumentOut(BaseModel):
    id: int
    company_id: int
    group_key: str
    uploaded_by: int
    category: str
    title: str
    original_filename: str
    storage_path: str
    mime_type: str | None = None
    file_size: int
    file_sha256: str
    is_deleted: int
    deleted_at: str | None = None
    created_at: str | None = None
    updated_at: str | None = None

class DocumentOutWithDedup(DocumentOut):
    deduped: bool
