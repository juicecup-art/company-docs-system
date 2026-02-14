# app/services/file_storage.py
import hashlib
import os
import re
from pathlib import Path
from datetime import datetime
from typing import Tuple

UPLOAD_ROOT = Path("uploads")  # 你项目根目录已有 uploads/

_SAFE_CHARS = re.compile(r"[^A-Za-z0-9._-]+")


def sanitize_filename(name: str) -> str:
    """
    将文件名变成安全的（避免奇怪字符 / 路径穿越）
    """
    name = name.strip().replace("\\", "/").split("/")[-1]  # 防止 ../ 和 windows 路径
    name = _SAFE_CHARS.sub("_", name)
    if not name:
        name = "file"
    return name[:180]  # 避免过长


def save_upload_file(
    file_obj,
    *,
    company_id: int,
    category: str,
) -> Tuple[str, int, str]:
    """
    保存 UploadFile.file 到 uploads/ 下，返回:
    (storage_path, file_size, sha256_hex)
    """
    # 目录结构：uploads/<company_id>/<category>/<YYYYMM>/
    yyyymm = datetime.utcnow().strftime("%Y%m")
    safe_category = _SAFE_CHARS.sub("_", category.strip())[:50] or "uncategorized"
    target_dir = UPLOAD_ROOT / str(company_id) / safe_category / yyyymm
    target_dir.mkdir(parents=True, exist_ok=True)

    # 文件名：<timestamp>_<sanitized_original>
    # （如果你想更强唯一性，可再加随机串/uuid）
    original_name = getattr(file_obj, "filename", None) or "file"
    safe_name = sanitize_filename(original_name)
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
    final_name = f"{ts}_{safe_name}"
    target_path = target_dir / final_name

    # 流式写入 + 同时计算 sha256 / size
    h = hashlib.sha256()
    size = 0

    with open(target_path, "wb") as out:
        while True:
            chunk = file_obj.file.read(1024 * 1024)  # 1MB
            if not chunk:
                break
            out.write(chunk)
            h.update(chunk)
            size += len(chunk)

    sha256_hex = h.hexdigest()
    storage_path = str(target_path).replace(os.sep, "/")  # 统一成 /

    return storage_path, size, sha256_hex
