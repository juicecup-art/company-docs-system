# tools/gen_hash.py
from app.auth.password import get_password_hash

print(get_password_hash("123456"))
