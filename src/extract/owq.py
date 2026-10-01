"""Extractor for Open Water Quality (GEMStat, Eionet) via the scripted export endpoint.

Export mode: scripted (confirmed via live-request spike, see ING-2 PR).
Real endpoint (not on openwaterquality.org itself):
  https://lume-inventory-api.evan-thomas-3d8.workers.dev/api/wq/export

Responses are capped at 100,000 rows (X-Row-Count / X-Row-Capped headers).
Chunking strategy: split by year first; if a single year+indicator combo is
still capped, split further by loc_type (binary split, since loc_type is a
fixed finite list and this always converges).

Some source/indicator/year combinations legitimately return zero data rows
(e.g. Eionet reports no fecal/total coliform at all, since it's a bathing-
water dataset scoped to E. coli and enterococci). These are treated as
expected-empty: logged as a warning and recorded in the manifest, not raised.
"""

import argparse
from pathlib import Path

from src.utils import config as cfg
from src.utils import http, log, manifest, paths
from src.utils.exceptions import EmptyResponseError

STAGE = "extract"


def _load_owq_config() -> dict:
    return cfg.load_yaml("sources")["owq"]


def _fetch_chunk(session, logger, endpoint, source_label, var_code, year, loc_types, row_cap):
    """Fetch one year x indicator x loc_type-subset chunk. Splits loc_types on cap."""
    params = {
        "year_from": year,
        "year_to": year,
        "var": var_code,
        "source": source_label,
        "loc_type": ",".join(loc_types),
        "format": "csv",
    }
    resp = http.request(session, "GET", endpoint, params=params, logger=logger)

    capped = resp.headers.get("X-Row-Capped") == "1"

    if not capped:
        yield params, resp
        return

    if len(loc_types) == 1:
        logger.warning(
            "Row cap hit with a single loc_type (%s); accepting truncated data for year=%s var=%s",
            loc_types[0], year, var_code,
        )
        yield params, resp
        return

    mid = len(loc_types) // 2
    logger.info(
        "Row cap hit (year=%s var=%s, %d loc_types); splitting into two halves",
        year, var_code, len(loc_types),
    )
    yield from _fetch_chunk(session, logger, endpoint, source_label, var_code, year, loc_types[:mid], row_cap)
    yield from _fetch_chunk(session, logger, endpoint, source_label, var_code, year, loc_types[mid:], row_cap)


def _extract_one_source(session, logger, owq_cfg, source_code, source_label, start_year, end_year, batch_dir):
    endpoint = owq_cfg["export_endpoint"]
    indicators = owq_cfg["indicators"]
    loc_types = owq_cfg["loc_types"]

    files = []
    requests_log = []
    warnings = []
    chunk_index = 0

    for indicator_name, var_code in indicators.items():
        for year in range(start_year, end_year + 1):
            for params, resp in _fetch_chunk(
                session, logger, endpoint, source_label, var_code, year, loc_types, owq_cfg["row_cap"]
            ):
                chunk_index += 1
                file_name = f"{source_code}_{indicator_name}_{year}_chunk{chunk_index:04d}.csv"
                file_path = batch_dir / file_name

                if not resp.content:
                    raise EmptyResponseError(
                        f"{source_code}/{indicator_name}/{year}: empty response body from {endpoint}"
                    )

                file_path.write_bytes(resp.content)
                row_count = manifest.count_csv_rows(file_path)

                if row_count == 0:
                    msg = f"{source_code}/{indicator_name}/{year}: zero data rows (expected-empty)"
                    logger.warning(msg)
                    warnings.append(msg)

                files.append(manifest.file_entry(file_path, "csv", row_count))
                requests_log.append({
                    "method": "GET", "url": endpoint, "query": params,
                    "http_status": resp.status_code,
                })
                logger.info(
                    "Saved %s (%d rows, capped=%s)",
                    file_name, row_count, resp.headers.get("X-Row-Capped") == "1",
                )

    first_var = next(iter(indicators.values()))
    geo_params = {
        "year_from": start_year, "year_to": end_year,
        "var": first_var, "source": source_label,
        "loc_type": ",".join(loc_types), "format": "geojson",
    }
    geo_resp = http.request(session, "GET", endpoint, params=geo_params, logger=logger)
    geo_name = f"{source_code}_sites.geojson"
    geo_path = batch_dir / geo_name
    geo_path.write_bytes(geo_resp.content)
    files.append(manifest.file_entry(geo_path, "geojson", None))
    requests_log.append({
        "method": "GET", "url": endpoint, "query": geo_params,
        "http_status": geo_resp.status_code,
    })

    return files, requests_log, warnings


def run(**overrides) -> Path:
    cfg.load_env()
    owq_cfg = _load_owq_config()
    sampling_cfg = cfg.load_yaml("sampling")

    start_year = overrides.get("start_year", sampling_cfg["period"]["start_year"])
    end_year = overrides.get("end_year", sampling_cfg["period"]["end_year"])
    cfg.validate_period(start_year, end_year)

    session = http.build_session()
    last_manifest_path = None

    for source_code, source_label in owq_cfg["sources"].items():
        logger = log.get_logger(STAGE, source_code)
        params = {
            "source_label": source_label,
            "start_year": start_year,
            "end_year": end_year,
            "indicators": sorted(owq_cfg["indicators"].values()),
            "format": "csv+geojson",
            "export_endpoint": owq_cfg["export_endpoint"],
        }
        batch_id = manifest.make_batch_id(source_code, params)
        batch_dir = paths.raw_batch_dir(source_code, batch_id, create=True)

        existing = manifest.read_manifest(batch_dir)
        if existing is not None and all(
            manifest.file_is_complete(batch_dir, f["name"]) for f in existing["files"]
        ):
            logger.info("Batch %s already complete, skipping", batch_id)
            last_manifest_path = batch_dir / "manifest.json"
            continue

        logger.info("Starting extraction for %s (%s)", source_code, source_label)
        files, requests_log, warnings = _extract_one_source(
            session, logger, owq_cfg, source_code, source_label, start_year, end_year, batch_dir
        )

        last_manifest_path = manifest.write_manifest(
            batch_dir,
            source_code=source_code,
            batch_id=batch_id,
            params=params,
            requests_log=requests_log,
            files=files,
            status="success",
            warnings=warnings,
        )
        logger.info("Finished %s: %d files written (%d warnings)", source_code, len(files), len(warnings))

    return last_manifest_path


def _cli():
    parser = argparse.ArgumentParser(description="Extract GEMStat/Eionet data via Open Water Quality export API")
    parser.add_argument("--start-year", type=int, default=None)
    parser.add_argument("--end-year", type=int, default=None)
    args = parser.parse_args()

    overrides = {}
    if args.start_year is not None:
        overrides["start_year"] = args.start_year
    if args.end_year is not None:
        overrides["end_year"] = args.end_year

    manifest_path = run(**overrides)
    print(f"Manifest written to: {manifest_path}")


if __name__ == "__main__":
    _cli()