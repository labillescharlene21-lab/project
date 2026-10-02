"""ING-5: Natural Earth 10m admin-1 states/provinces polygons.

Flow
1. Compute batch_id from the pinned download_url only; if the batch is already complete
   (manifest status success and every file intact) return without any network call.
2. Download the zip exactly as received to data/raw/natural_earth/batch_id={id}/.
3. Verify zip integrity (ZipFile.testzip() must return None), extract into extracted/.
4. Read the .shp with GeoPandas; record feature count (row_count on the .shp entry),
   CRS and column names (manifest notes).
5. Resume: a zip that is complete per manifest is not downloaded again.

Run:
    python -m src.extract.boundaries
"""
from __future__ import annotations

import io
import zipfile
from pathlib import Path, PurePosixPath

from src.utils import config as cfg
from src.utils import http, log, manifest, paths
from src.utils.exceptions import ConfigError, EmptyResponseError, SourceRequestError

STAGE = "extract"
SOURCE_CODE = "natural_earth"
ZIP_NAME = "ne_10m_admin_1_states_provinces.zip"
SHP_NAME = "ne_10m_admin_1_states_provinces.shp"
EXTRACT_DIR = "extracted"


def _download_url() -> str:
    url = cfg.load_yaml("sources").get("natural_earth", {}).get("download_url")
    if not url or not str(url).startswith("http"):
        raise ConfigError("sources.yaml natural_earth.download_url is not pinned to a real URL")
    return str(url)


def verify_zip(content: bytes, url: str) -> None:
    """Raise SourceRequestError unless the bytes are a valid zip with no corrupt member."""
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            bad = zf.testzip()
    except zipfile.BadZipFile as exc:
        raise SourceRequestError(f"GET {url}: response is not a valid zip file: {exc}") from exc
    if bad is not None:
        raise SourceRequestError(f"GET {url}: zip integrity check failed on member {bad!r}")


def safe_extract(zip_path: Path, dest: Path) -> list[Path]:
    """Extract all members into dest; refuse paths that would escape dest. Returns file paths."""
    dest.mkdir(parents=True, exist_ok=True)
    root = dest.resolve()
    out: list[Path] = []
    with zipfile.ZipFile(zip_path) as zf:
        for info in zf.infolist():
            target = (dest / info.filename).resolve()
            if root != target and root not in target.parents:
                raise SourceRequestError(f"{zip_path.name}: unsafe member path {info.filename!r}")
        zf.extractall(dest)
        for info in zf.infolist():
            if not info.is_dir():
                out.append(dest / info.filename)
    return out


def inspect_shapefile(shp_path: Path) -> tuple[int, str, list[str]]:
    """Return (feature_count, crs, column_names) read with GeoPandas."""
    import geopandas as gpd  # heavy import; kept lazy so other extractors do not need it

    gdf = gpd.read_file(shp_path)
    if len(gdf) == 0:
        raise EmptyResponseError(f"{shp_path.name} contains no features")
    crs = gdf.crs.to_string() if gdf.crs is not None else "unknown"
    return len(gdf), crs, [c for c in gdf.columns if c != gdf.geometry.name] + [gdf.geometry.name]


def _entry(batch_dir: Path, path: Path, row_count: int | None = None) -> dict:
    e = manifest.file_entry(path, path.suffix.lstrip(".").lower() or "file", row_count)
    e["name"] = PurePosixPath(path.relative_to(batch_dir)).as_posix()
    return e


def _all_complete(batch_dir: Path, existing: dict | None) -> bool:
    if not existing or existing.get("status") != "success":
        return False
    names = [f["name"] for f in existing["files"]]
    return ZIP_NAME in names and any(n.endswith(".shp") for n in names) and all(
        manifest.file_is_complete(batch_dir, n) for n in names
    )


def run(**overrides) -> Path:
    """Run the extraction. Returns the path to the batch manifest.json."""
    cfg.load_env()
    url = _download_url()
    params = {"download_url": url}
    batch_id = manifest.make_batch_id(SOURCE_CODE, params)
    logger = log.get_logger(STAGE, SOURCE_CODE, batch_id)
    batch_dir = paths.raw_batch_dir(SOURCE_CODE, batch_id, create=True)

    existing = manifest.read_manifest(batch_dir)
    if _all_complete(batch_dir, existing):
        logger.info("Batch already complete, nothing to do")
        return batch_dir / "manifest.json"

    requests_log: list[dict] = list(existing.get("requests_log", [])) if existing else []
    zip_path = batch_dir / ZIP_NAME

    def _write(files: list[dict], status: str, notes: str = "") -> Path:
        return manifest.write_manifest(
            batch_dir, source_code=SOURCE_CODE, batch_id=batch_id, params=params,
            requests_log=requests_log, files=files, status=status, notes=notes,
        )

    if existing and manifest.file_is_complete(batch_dir, ZIP_NAME):
        logger.info("Zip already complete, skipping download")
        zip_entry = next(f for f in existing["files"] if f["name"] == ZIP_NAME)
    else:
        logger.info("Downloading %s", url)
        resp = http.request(http.build_session(), "GET", url, logger=logger)
        verify_zip(resp.content, url)
        zip_path.write_bytes(resp.content)  # as received
        requests_log.append({"method": "GET", "url": url, "http_status": resp.status_code})
        zip_entry = _entry(batch_dir, zip_path)
        _write([zip_entry], "partial", "zip downloaded; not yet extracted")
        logger.info("Saved %s (%d bytes)", ZIP_NAME, zip_entry["size_bytes"])

    extracted = safe_extract(zip_path, batch_dir / EXTRACT_DIR)
    shps = [p for p in extracted if p.name == SHP_NAME]
    if len(shps) != 1:
        raise SourceRequestError(f"{ZIP_NAME}: expected exactly one {SHP_NAME}, found {[p.name for p in extracted]}")
    shp = shps[0]

    n_features, crs, columns = inspect_shapefile(shp)
    logger.info("Features: %d, CRS: %s", n_features, crs)
    logger.info("Columns: %s", columns)

    files = [zip_entry] + [
        _entry(batch_dir, p, n_features if p == shp else None) for p in sorted(extracted)
    ]
    path = _write(files, "success", f"features={n_features}; crs={crs}; columns={columns}")
    logger.info("Finished: %d features, %d files", n_features, len(files))
    return path


def _cli():
    import argparse

    argparse.ArgumentParser(description="Download Natural Earth 10m admin-1 states/provinces").parse_args()
    print(f"Manifest written to: {run()}")


if __name__ == "__main__":
    _cli()
