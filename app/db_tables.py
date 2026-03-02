# app/db_tables.py
from sqlalchemy import (
    Table, Column, BigInteger, Integer, String, Text, DateTime, MetaData
)
from sqlalchemy.sql import func

metadata = MetaData()

documents = Table(
    "documents", metadata,
    Column("id", BigInteger, primary_key=True),
    Column("company_id", BigInteger, nullable=False),
    Column("group_key", String(64), nullable=False),
    Column("file_type", String(50)),
    Column("uploaded_by", BigInteger),
    Column("category", String(50)),
    Column("title", String(255)),
    Column("original_filename", String(255), nullable=False),
    Column("storage_path", String(800), nullable=False),
    Column("mime_type", String(100)),
    Column("file_size", BigInteger),
    Column("file_sha256", String(64)),
    Column("is_deleted", Integer, nullable=False, server_default="0"),
    Column("deleted_at", DateTime),
    Column("created_at", DateTime, server_default=func.now()),
    Column("updated_at", DateTime),
)

platform_documents = Table(
    "platform_documents", metadata,
    Column("id", BigInteger, primary_key=True),
    Column("company_id", BigInteger, nullable=False),
    Column("platform_id", BigInteger, nullable=False),
    Column("document_id", BigInteger, nullable=False),
    Column("doc_role", String(30), nullable=False, server_default="word"),
    Column("is_deleted", Integer, nullable=False, server_default="0"),
    Column("deleted_at", DateTime),
    Column("created_at", DateTime, server_default=func.now()),
)

platform_text_fields = Table(
    "platform_text_fields", metadata,
    Column("id", BigInteger, primary_key=True),
    Column("company_id", BigInteger, nullable=False),
    Column("platform_id", BigInteger, nullable=False),
    Column("label", String(80), nullable=False, server_default="文本框"),
    Column("content", Text, nullable=False),
    Column("sort_no", Integer, nullable=False, server_default="0"),
    Column("created_at", DateTime, server_default=func.now()),
    Column("updated_at", DateTime),
    Column("is_deleted", Integer, nullable=False, server_default="0"),
    Column("deleted_at", DateTime),
)