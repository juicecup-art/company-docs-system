from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, DateTime, Text, ForeignKey, UniqueConstraint
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Company(Base):
    __tablename__ = "companies"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(32), unique=True, nullable=False)          # 例如 RO-22
    country = Column(String(8), nullable=False)                      # RO / HU / DE...
    name = Column(String(255), nullable=False)

    address = Column(Text, nullable=True)
    address_new = Column(Text, nullable=True)
    postal_code = Column(String(32), nullable=True)

    established_date = Column(String(32), nullable=True)             # 先用字符串，后面再改 DATE
    register_authority = Column(Text, nullable=True)
    trade_register_no = Column(String(64), nullable=True)
    cui = Column(String(64), nullable=True)
    vat = Column(String(64), nullable=True)
    tax_no_de = Column(String(64), nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    legal_person_links = relationship("CompanyLegalPerson", back_populates="company", cascade="all, delete-orphan")
    platforms = relationship("CompanyPlatform", back_populates="company", cascade="all, delete-orphan")


class LegalPerson(Base):
    __tablename__ = "legal_persons"

    id = Column(Integer, primary_key=True, autoincrement=True)
    full_name = Column(String(255), nullable=False)
    first_name = Column(String(128), nullable=True)
    last_name = Column(String(128), nullable=True)

    nationality = Column(String(64), nullable=True)
    gender = Column(String(16), nullable=True)

    address = Column(Text, nullable=True)
    postal_code = Column(String(32), nullable=True)

    id_no = Column(String(64), nullable=True)
    id_expiry_range = Column(String(64), nullable=True)              # 06.02.25-03.08.2031

    passport_no = Column(String(64), nullable=True)
    passport_expiry_range = Column(String(64), nullable=True)        # 2025.02.10-2035.02.10

    birth_date = Column(String(32), nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    company_links = relationship("CompanyLegalPerson", back_populates="legal_person", cascade="all, delete-orphan")


class CompanyLegalPerson(Base):
    __tablename__ = "company_legal_persons"
    __table_args__ = (
        UniqueConstraint("company_id", "legal_person_id", name="uq_company_legal_person"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    legal_person_id = Column(Integer, ForeignKey("legal_persons.id"), nullable=False)

    role = Column(String(64), nullable=True)  # director / manager / owner...
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    company = relationship("Company", back_populates="legal_person_links")
    legal_person = relationship("LegalPerson", back_populates="company_links")


class CompanyPlatform(Base):
    __tablename__ = "company_platforms"
    __table_args__ = (
        UniqueConstraint("company_id", "platform", "account_name", name="uq_company_platform_account"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)

    platform = Column(String(64), nullable=False)     # amazon / ebay / shopify...
    account_name = Column(String(255), nullable=True)
    store_url = Column(Text, nullable=True)
    note = Column(Text, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    company = relationship("Company", back_populates="platforms")
