"""Unit tests for src/extract/boundaries.py (no real network; a tiny fake shapefile zip)."""
import io
import json
import shutil
import zipfile

import pytest
import responses
import yaml

from src.extract import boundaries as b
from src.utils import manifest
from src.utils.exceptions import ConfigError, SourceRequestError

pytest.importorskip("geopandas")
import geopandas as gpd  # noqa: E402
from shapely.geometry import box  # noqa: E402

URL = "https://example.test/ne_10m_admin_1_states_provinces.zip"


@pytest.fixture
def pinned(isolated_dirs):
    """Point natural_earth.download_url at a fake URL in the temp config copy."""
    _, cfg_dir = isolated_dirs
    path = cfg_dir / "sources.yaml"
    data = yaml.safe_load(path.read_text())
    data["natural_earth"]["download_url"] = URL
    path.write_text(yaml.safe_dump(data))
    return path


def make_zip(tmp_path, compression=zipfile.ZIP_STORED) -> bytes:
    gdf = gpd.GeoDataFrame(
        {"iso_a2": ["US", "CA"], "adm1_code": ["USA-1", "CAN-1"], "name": ["A", "B"]},
        geometry=[box(0, 0, 1, 1), box(1, 1, 2, 2)], crs="EPSG:4326",
    )
    src = tmp_path / "src"
    src.mkdir()
    gdf.to_file(src / "ne_10m_admin_1_states_provinces.shp")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression) as zf:
        for p in sorted(src.iterdir()):
            zf.write(p, p.name)
    return buf.getvalue()


def serve(content: bytes, status: int = 200):
    responses.add(responses.GET, URL, body=content, status=status)


@responses.activate
def test_happy_path(tmp_path, pinned):
    serve(make_zip(tmp_path))
    mpath = b.run()
    m = json.loads(mpath.read_text())
    assert m["status"] == "success"
    assert m["params"] == {"download_url": URL}
    by_name = {f["name"]: f for f in m["files"]}
    assert b.ZIP_NAME in by_name
    shp = by_name["extracted/" + b.SHP_NAME]
    assert shp["row_count"] == 2
    assert "EPSG:4326" in m["notes"] and "iso_a2" in m["notes"]
    assert gpd.read_file(mpath.parent / "extracted" / b.SHP_NAME).shape[0] == 2


@responses.activate
def test_rerun_is_noop_without_network(tmp_path, pinned):
    serve(make_zip(tmp_path))
    mpath = b.run()
    before = mpath.read_bytes()
    responses.reset()  # any further request would raise ConnectionError
    assert b.run() == mpath
    assert mpath.read_bytes() == before
    assert len(responses.calls) == 0


@responses.activate
def test_resume_reuses_complete_zip(tmp_path, pinned):
    serve(make_zip(tmp_path))
    mpath = b.run()
    shutil.rmtree(mpath.parent / "extracted")
    m = json.loads(mpath.read_text())
    m["status"] = "partial"
    mpath.write_text(json.dumps(m))
    responses.reset()
    b.run()
    assert (mpath.parent / "extracted" / b.SHP_NAME).exists()
    assert json.loads(mpath.read_text())["status"] == "success"
    assert len(responses.calls) == 0


@responses.activate
def test_not_a_zip_is_rejected(pinned):
    serve(b"<html>error</html>")
    with pytest.raises(SourceRequestError, match="not a valid zip"):
        b.run()


@responses.activate
def test_corrupt_member_fails_testzip(tmp_path, pinned):
    data = bytearray(make_zip(tmp_path))
    with zipfile.ZipFile(io.BytesIO(bytes(data))) as zf:
        info = zf.getinfo(b.SHP_NAME)
    data[info.header_offset + 30 + len(info.filename) + 10] ^= 0xFF  # flip a byte of stored data
    serve(bytes(data))
    with pytest.raises(SourceRequestError, match="integrity"):
        b.run()


def test_unpinned_url_raises_before_network(isolated_dirs):
    _, cfg_dir = isolated_dirs
    path = cfg_dir / "sources.yaml"
    data = yaml.safe_load(path.read_text())
    data["natural_earth"]["download_url"] = "TODO"
    path.write_text(yaml.safe_dump(data))
    with pytest.raises(ConfigError):
        b.run()


def test_safe_extract_rejects_path_traversal(tmp_path):
    zp = tmp_path / "evil.zip"
    with zipfile.ZipFile(zp, "w") as zf:
        zf.writestr("../evil.txt", "x")
    with pytest.raises(SourceRequestError, match="unsafe"):
        b.safe_extract(zp, tmp_path / "out")


@responses.activate
def test_zip_without_expected_shp_is_rejected(pinned):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("other.txt", "x")
    serve(buf.getvalue())
    with pytest.raises(SourceRequestError, match="expected exactly one"):
        b.run()
