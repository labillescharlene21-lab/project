import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


def make_batch_id(source_code: str, params: dict) -> str:
    normalized = json.dumps(params, sort_keys=True, separators=(",", ":"), default=str)
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:12]
    return f"{source_code}_{digest}"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def count_csv_rows(path: Path) -> int:
    """Count data rows (excludes header). Handles UTF-8 BOM and quoted fields containing newlines."""
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        try:
            next(reader)  # header
        except StopIteration:
            return 0
        return sum(1 for _ in reader)


def file_entry(path: Path, fmt: str, row_count: int | None) -> dict:
    return {
        "name": path.name,
        "format": fmt,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
        "row_count": row_count,
    }


def write_manifest(
    batch_dir: Path,
    *,
    source_code: str,
    batch_id: str,
    params: dict,
    requests_log: list[dict],
    files: list[dict],
    status: str,
    warnings: list[str] | None = None,
    notes: str = "",
) -> Path:
    total_rows = sum(f["row_count"] for f in files if f.get("row_count") is not None)
    manifest = {
        "manifest_version": 1,
        "source_code": source_code,
        "batch_id": batch_id,
        "params": params,
        "retrieved_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "requests_log": requests_log,
        "files": files,
        "total_rows": total_rows,
        "status": status,
        "warnings": warnings or [],
        "notes": notes,
    }

    batch_dir.mkdir(parents=True, exist_ok=True)
    final_path = batch_dir / "manifest.json"
    tmp_path = batch_dir / "manifest.json.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    tmp_path.replace(final_path)  # atomic on POSIX and Windows
    return final_path


def read_manifest(batch_dir: Path) -> dict | None:
    path = Path(batch_dir) / "manifest.json"
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def file_is_complete(batch_dir: Path, name: str) -> bool:
    manifest = read_manifest(batch_dir)
    if manifest is None:
        return False
    entry = next((f for f in manifest["files"] if f["name"] == name), None)
    if entry is None:
        return False
    file_path = Path(batch_dir) / name
    if not file_path.exists():
        return False
    return sha256_file(file_path) == entry["sha256"]