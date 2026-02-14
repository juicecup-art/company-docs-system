from sqlalchemy import (
    Table, Column, BigInteger, String, DateTime, Text, CHAR, SmallInteger,
    ForeignKey, MetaData, func
)

metadata = MetaData()

documents = Table(
    "documents",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),

    Column("company_id", BigInteger, ForeignKey("companies.id"), nullable=False),
    Column("uploaded_by", BigInteger, nullable=False),  # 先不强制FK，避免你用户表未就绪

    Column("category", String(50), nullable=False),
    Column("title", String(255), nullable=False),

    Column("original_filename", String(255), nullable=False),
    Column("storage_path", String(800), nullable=False),
    Column("file_type", String(50), nullable=True),

    Column("mime_type", String(100), nullable=True),
    Column("file_size", BigInteger, nullable=True),
    Column("file_sha256", CHAR(64), nullable=True),

    Column("is_deleted", SmallInteger, nullable=False, server_default="0"),
    Column("deleted_at", DateTime, nullable=True),

    Column("created_at", DateTime, nullable=True, server_default=func.now()),
    Column("updated_at", DateTime, nullable=True, server_default=func.now(), onupdate=func.now()),
    Column("group_key", String(64), nullable=False),

)
