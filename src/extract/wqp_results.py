"""ING-3: Water Quality Portal results for the sites sampled in ING-2.

Flow
1. Find the sampled site list: the newest raw batch whose manifest has status "success" and
   lists sampled_sites.csv (written by ING-2), or --sites-file during development.
2. Sort and de-duplicate the WQP site IDs, then split them into fixed-size chunks
   (deterministic chunk numbering: same sites -> same chunks).
3. For each chunk, POST to the Result service with a JSON body (site IDs + the four indicators)
   and the date range in the query string. Save the response exactly as received
   (chunk_0001.zip) and extract its CSV (chunk_0001.csv).
4. Record every file in manifest.json after each chunk, so an interrupted run resumes.
5. Empty chunks are allowed (sites can have no results in range) but logged; all chunks
   empty -> EmptyResponseError.

Retries: DE1's session (urllib3 Retry) does not retry POST requests, so each chunk is retried
here: up to http.max_retries extra attempts with exponential backoff.

Run:
    python -m src.extract.wqp_results --sites-file tests/fixtures/wqp_sites_dev.csv --max-chunks 1
    python -m src.extract.wqp_results                    # uses ING-2's latest successful batch
    python -m src.extract.wqp_results --show-sample      # print columns + 3 rows of the latest batch
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import inspect
import io
import json
import time
import zipfile
from pathlib import Path

import pandas as pd

from src.utils import http
from src.utils.config import load_yaml, validate_period
from src.utils.exceptions import ConfigError, EmptyResponseError, SourceRequestError
from src.utils.log import get_logger
from src.utils.manifest import (
    count_csv_rows,
    file_entry,
    file_is_complete,
    make_batch_id,
    read_manifest,
    write_manifest,
)
from src.utils.paths import data_dir, raw_batch_dir

SOURCE_CODE = "wqp_results"
STAGE = "extract"
SITES_FILE_NAME = "sampled_sites.csv"
SECONDS_BETWEEN_REQUESTS = 1.0


# ---------------------------------------------------------------- inputs
def find_latest_sites_file() -> tuple[Path, str]:
    """Newest successful raw batch that lists sampled_sites.csv. Returns (path, batch_id)."""
    candidates = []
    raw = data_dir() / "raw"
    for manifest_path in raw.rglob("manifest.json") if raw.exists() else []:
        m = json.loads(manifest_path.read_text(encoding="utf-8"))
        names = {f.get("name") for f in m.get("files", [])}
        if m.get("status") == "success" and SITES_FILE_NAME in names:
            candidates.append((m.get("retrieved_at_utc", ""), manifest_path.parent / SITES_FILE_NAME, m["batch_id"]))
    if not candidates:
        raise ConfigError(f"No successful ING-2 batch with {SITES_FILE_NAME} under {raw}. "
                          f"Run ING-2 first or pass --sites-file.")
    _, path, batch_id = sorted(candidates)[-1]
    return path, batch_id


def load_site_ids(path: Path) -> list[str]:
    df = pd.read_csv(path, dtype=str)
    if "source_site_id" not in df.columns:
        raise ConfigError(f"{path} has no 'source_site_id' column (columns: {list(df.columns)})")
    ids = sorted({s.strip() for s in df["source_site_id"].dropna() if s.strip()})
    if not ids:
        raise ConfigError(f"{path} contains no site IDs")
    return ids


def make_chunks(site_ids: list[str], size: int) -> list[list[str]]:
    """Deterministic: sorted, de-duplicated, fixed size."""
    ids = sorted(set(site_ids))
    return [ids[i:i + size] for i in range(0, len(ids), size)]


# ---------------------------------------------------------------- one request
def _post(session, url: str, query: dict, body: dict, log):
    """Call ING-0's http.request with a JSON body, whatever the keyword is named there."""
    params = inspect.signature(http.request).parameters
    kw = "json_body" if "json_body" in params else "json"
    return http.request(session, "POST", url, params=query, logger=log, **{kw: body})


def post_with_retries(session, url, query, body, log, max_retries: int, backoff: float):
    for attempt in range(max_retries + 1):
        try:
            return _post(session, url, query, body, log)
        except SourceRequestError as exc:
            if attempt == max_retries:
                raise
            wait = backoff * (2 ** attempt)
            log.warning(f"chunk request failed ({exc}); retry {attempt + 1}/{max_retries} in {wait:.0f}s")
            time.sleep(wait)


def save_response(content: bytes, batch_dir: Path, n: int) -> tuple[Path | None, Path]:
    """Save the body as received. Zip -> keep zip + extract its CSV. Plain CSV -> save as CSV."""
    csv_path = batch_dir / f"chunk_{n:04d}.csv"
    if zipfile.is_zipfile(io.BytesIO(content)):
        zip_path = batch_dir / f"chunk_{n:04d}.zip"
        zip_path.write_bytes(content)
        with zipfile.ZipFile(zip_path) as zf:
            members = [m for m in zf.namelist() if m.lower().endswith(".csv")]
            if len(members) != 1:
                raise SourceRequestError(f"{zip_path.name}: expected 1 CSV inside, found {members}")
            csv_path.write_bytes(zf.read(members[0]))
        return zip_path, csv_path
    csv_path.write_bytes(content)
    return None, csv_path


def dates_outside_period(csv_path: Path, start_year: int, end_year: int) -> dict | None:
    """Check ActivityStartDate against the period. Returns a summary if any row falls outside."""
    try:
        dates = pd.read_csv(csv_path, usecols=["ActivityStartDate"], dtype=str)["ActivityStartDate"]
    except ValueError:                      # column missing (e.g. empty chunk with a different header)
        return None
    years = pd.to_numeric(dates.str[:4], errors="coerce").dropna()
    bad = years[(years < start_year) | (years > end_year)]
    if bad.empty:
        return None
    valid = dates[dates.str[:4].isin(bad.astype(int).astype(str))]
    return {"n": int(len(bad)), "min": valid.min(), "max": valid.max()}


# ---------------------------------------------------------------- main entry
def run(**overrides) -> Path:
    """Download results for every chunk. Returns the manifest path."""
    sources, sampling = load_yaml("sources"), load_yaml("sampling")
    wqp, http_cfg = sources["wqp"], sources.get("http", {})
    start_year = int(overrides.get("start_year") or sampling["period"]["start_year"])
    end_year = int(overrides.get("end_year") or sampling["period"]["end_year"])
    validate_period(start_year, end_year)

    if overrides.get("sites_file"):
        sites_path = Path(overrides["sites_file"])
        site_ids = load_site_ids(sites_path)
        sites_ref = "file-" + hashlib.sha256("\n".join(site_ids).encode()).hexdigest()[:12]
    else:
        sites_path, sites_ref = find_latest_sites_file()
        site_ids = load_site_ids(sites_path)

    names = [wqp["characteristic_map"][k] for k in sorted(wqp["characteristic_map"])]
    chunk_size = int(overrides.get("chunk_size") or wqp["results_sites_per_request"])
    chunks = make_chunks(site_ids, chunk_size)

    params = {
        "sites_ref": sites_ref,
        "characteristic_names": names,
        "start_date": f"01-01-{start_year}",
        "end_date": f"12-31-{end_year}",
        "chunk_size": chunk_size,
        "result_url": wqp["result_url"],
        # v2: dates moved into the JSON body (WQP ignored them in the query string on POST).
        # Changing this value gives a new batch_id, so old downloads are never reused.
        "request_format": "v2-dates-in-body",
    }
    batch_id = make_batch_id(SOURCE_CODE, params)
    log = get_logger(STAGE, SOURCE_CODE, batch_id)
    log.info(f"sites: {len(site_ids)} from {sites_path}; chunks: {len(chunks)} of up to {chunk_size}")

    batch_dir = raw_batch_dir(SOURCE_CODE, batch_id)
    previous = read_manifest(batch_dir) or {}
    files = {f["name"]: f for f in previous.get("files", [])}
    requests_log = list(previous.get("requests") or previous.get("requests_log") or [])
    warnings = list(previous.get("warnings", []))
    empty = set(previous.get("params", {}).get("_empty_chunks", []))

    def save(status: str) -> Path:
        p = dict(params, _empty_chunks=sorted(empty))
        return write_manifest(batch_dir, source_code=SOURCE_CODE, batch_id=batch_id, params=p,
                              requests_log=requests_log, files=[files[k] for k in sorted(files)],
                              status=status, warnings=warnings,
                              notes=f"sites file: {sites_path.name}; {len(site_ids)} sites")

    # Output options go in the query string; every filter goes in the JSON body.
    query = {"mimeType": "csv", "zip": wqp.get("zip", "yes"), "sorted": wqp.get("sorted", "no")}
    session = http.build_session()
    max_retries = int(http_cfg.get("max_retries", 3))
    backoff = float(http_cfg.get("backoff_factor", 2))
    max_chunks = overrides.get("max_chunks")
    done = skipped = 0

    for n, chunk in enumerate(chunks, start=1):
        csv_name = f"chunk_{n:04d}.csv"
        if csv_name in files and file_is_complete(batch_dir, csv_name):
            skipped += 1
            continue
        if max_chunks is not None and done >= int(max_chunks):
            log.info(f"stopping after {done} new chunks (--max-chunks); rerun to continue")
            return save("partial")
        if done:
            time.sleep(SECONDS_BETWEEN_REQUESTS)

        body = {"siteid": chunk, "characteristicName": names,
                "startDateLo": params["start_date"], "startDateHi": params["end_date"]}
        response = post_with_retries(session, wqp["result_url"], query, body, log, max_retries, backoff)
        zip_path, csv_path = save_response(response.content, batch_dir, n)
        rows = count_csv_rows(csv_path)

        wqp_counts = {k: v for k, v in response.headers.items() if k.lower().startswith("total-")}
        for k, v in response.headers.items():
            if k.lower() == "warning":
                warnings.append(f"{csv_name}: WQP warning: {v}")
                log.warning(f"{csv_name}: WQP warning header: {v}")
        expected = wqp_counts.get("Total-Result-Count")
        if expected is not None and str(expected).isdigit() and int(expected) != rows:
            warnings.append(f"{csv_name}: CSV has {rows} rows but Total-Result-Count header says {expected}")
            log.warning(warnings[-1])
        outside = dates_outside_period(csv_path, start_year, end_year)
        if outside:
            warnings.append(f"{csv_name}: {outside['n']} rows dated outside {start_year}-{end_year} "
                            f"(earliest {outside['min']}, latest {outside['max']}); WQP date filter not applied")
            log.warning(warnings[-1])
        if rows == 0:
            empty.add(csv_name)
            log.warning(f"{csv_name}: 0 rows for {len(chunk)} sites (allowed; sites may have no results in range)")
        else:
            empty.discard(csv_name)

        if zip_path is not None:
            files[zip_path.name] = file_entry(zip_path, "zip", None)
        files[csv_name] = file_entry(csv_path, "csv", rows)
        requests_log.append({"method": "POST", "url": wqp["result_url"], "query": query,
                             "body_sites": len(chunk), "first_site": chunk[0], "last_site": chunk[-1],
                             "http_status": response.status_code, "wqp_count_headers": wqp_counts,
                             "file": csv_name})
        done += 1
        save("partial")
        log.info(f"{csv_name}: {rows} rows ({n}/{len(chunks)})")

    total = sum(f["row_count"] or 0 for name, f in files.items() if name.endswith(".csv"))
    if total == 0:
        save("failed")
        raise EmptyResponseError(f"All {len(chunks)} WQP chunks returned 0 rows; check site IDs, "
                                 f"characteristic names and dates in the manifest")
    path = save("success")
    log.info(f"done: chunks {len(chunks)}, downloaded {done}, skipped {skipped}, empty {len(empty)}, "
             f"total rows {total}, status success")
    return path


def show_sample(batch_dir: Path | None = None) -> None:
    """Print the real column names and 3 rows (for the PR evidence)."""
    if batch_dir is None:
        manifests = sorted((data_dir() / "raw" / SOURCE_CODE).rglob("manifest.json"))
        if not manifests:
            raise ConfigError("No wqp_results batch found; run the extractor first")
        batch_dir = max(manifests, key=lambda p: p.stat().st_mtime).parent
    for csv_path in sorted(batch_dir.glob("chunk_*.csv")):
        with csv_path.open(encoding="utf-8-sig", newline="") as f:
            rows = list(csv.reader(f))
        if len(rows) > 1:
            print(f"{csv_path.name}: {len(rows[0])} columns")
            print(", ".join(rows[0]))
            for r in rows[1:4]:
                print(dict(zip(rows[0], r)))
            return
    print("All chunks are empty")


def main() -> None:
    parser = argparse.ArgumentParser(description="ING-3: WQP results for sampled sites")
    parser.add_argument("--sites-file", help="CSV with a source_site_id column (dev); default: latest ING-2 batch")
    parser.add_argument("--start-year", type=int)
    parser.add_argument("--end-year", type=int)
    parser.add_argument("--max-chunks", type=int, help="download at most N new chunks, then stop (status partial)")
    parser.add_argument("--show-sample", action="store_true", help="print columns + 3 rows of the latest batch")
    args = parser.parse_args()
    if args.show_sample:
        show_sample()
        return
    print(run(sites_file=args.sites_file, start_year=args.start_year, end_year=args.end_year,
              max_chunks=args.max_chunks))


if __name__ == "__main__":
    main()
