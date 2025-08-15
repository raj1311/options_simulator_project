
from __future__ import annotations
import os, io, mimetypes
from typing import Optional
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

def _gauth_from_service_account_json(json_str: str) -> GoogleAuth:
    """Build GoogleAuth from a service account JSON string (not path)."""
    import json as _json, tempfile
    data = _json.loads(json_str)
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
    tmp.write(_json.dumps(data).encode("utf-8"))
    tmp.flush(); tmp.close()

    gauth = GoogleAuth(settings={
        "client_config_backend": "service",
        "service_config": {
            "client_json_file_path": tmp.name,
            "scope": ["https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"],
        },
    })
    gauth.ServiceAuth()
    return gauth

def get_drive(service_account_json: str) -> GoogleDrive:
    gauth = _gauth_from_service_account_json(service_account_json)
    return GoogleDrive(gauth)

def ensure_folder(drive: GoogleDrive, name: str, parent_id: Optional[str]) -> str:
    """Create (or find) a folder by name under parent_id; return its file ID."""
    q = f"mimeType='application/vnd.google-apps.folder' and name='{name.replace(\"'\",\"\\'\")}' and trashed=false"
    if parent_id:
        q += f" and '{parent_id}' in parents"
    items = drive.ListFile({'q': q}).GetList()
    if items:
        return items[0]['id']
    # create
    meta = {'title': name, 'mimeType': 'application/vnd.google-apps.folder'}
    if parent_id:
        meta['parents'] = [{'id': parent_id}]
    f = drive.CreateFile(meta)
    f.Upload()
    return f['id']

def upload_file(drive: GoogleDrive, local_path: str, parent_id: str) -> str:
    """Upload a single file, updating if a file with same name exists; return file id."""
    name = os.path.basename(local_path)
    q = f"title='{name.replace(\"'\",\"\\'\")}' and '{parent_id}' in parents and trashed=false"
    items = drive.ListFile({'q': q}).GetList()
    if items:
        f = items[0]
    else:
        f = drive.CreateFile({'title': name, 'parents': [{'id': parent_id}]})
    f.SetContentFile(local_path)
    f.Upload()
    return f['id']

def upload_folder_recursive(drive: GoogleDrive, local_dir: str, parent_id: str) -> str:
    """Upload an entire folder tree to Google Drive under parent_id. Returns the top folder id."""
    top_name = os.path.basename(os.path.normpath(local_dir))
    top_id = ensure_folder(drive, top_name, parent_id)

    for root, dirs, files in os.walk(local_dir):
        # map current local folder to Drive folder
        rel = os.path.relpath(root, local_dir)
        cur_parent_id = top_id if rel == "." else top_id
        # build folder chain based on rel:
        if rel != ".":
            # ensure nested folders
            parts = [p for p in rel.split(os.sep) if p and p != "."]
            parent = top_id
            for p in parts:
                parent = ensure_folder(drive, p, parent)
            cur_parent_id = parent

        for fn in files:
            upload_file(drive, os.path.join(root, fn), cur_parent_id)
    return top_id
