import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

from app.db_models import Base

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)

def init_db():
    Base.metadata.create_all(bind=engine)

