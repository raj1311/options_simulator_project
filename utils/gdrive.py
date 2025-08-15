
import re, os, gdown
from typing import Optional

def _extract_id(link_or_id: str) -> Optional[str]:
    s = link_or_id.strip()
    # If it's already an ID (no slashes, short-ish), accept
    if "/" not in s and len(s) >= 10:
        return s
    # Common Drive URL patterns
    m = re.search(r"/d/([a-zA-Z0-9_-]{20,})", s)
    if m: return m.group(1)
    m = re.search(r"id=([a-zA-Z0-9_-]{20,})", s)
    if m: return m.group(1)
    return None

def download_file(link_or_id: str, out_path: str) -> str:
    """
    Download a large file from Google Drive to `out_path` using gdown.
    Accepts share links or file IDs.
    """
    file_id = _extract_id(link_or_id)
    if not file_id:
        raise ValueError("Could not parse Google Drive link or ID")
    url = f"https://drive.google.com/uc?id={file_id}"
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    gdown.download(url, out_path, quiet=False, fuzzy=True)
    return out_path
