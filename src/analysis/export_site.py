"""Export the data behind the GitHub Pages site (docs/index.html).

Reads the curated layer (data/curated/*.parquet, written by CUR-1 and MART-1) and writes ONE file,
docs/data/site_data.js, that the page loads with a plain <script> tag (so it also works when the
page is opened straight from disk). Read-only on data/; rerun after every pipeline run:

    python -m src.analysis.export_site

Contents of window.SITE_DATA
    meta        generated_at, curated batch id, period, weather coverage, counts
    strata      per stratum x realm: sites, hotspots, share, median ratio to threshold
    top_regions the 10 best-ranked regions (MART-1 priority ranking)
    top_sites   the 10 worst persistent-hotspot sites (highest exceedance rate, then ratio to threshold)
    legend      the severity scale (Low / Moderate / High / Critical / not enough data) with its definitions
    sites       every sampled site: position, stratum, realm, status, severity, key indicators
Site status: "hotspot" (persistent), "monitored" (scored, not a hotspot), "insufficient" (no scored years).
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from src.utils.config import PROJECT_ROOT, load_yaml
from src.utils.exceptions import ConfigError
from src.utils.log import get_logger
from src.utils.paths import curated_dir

log = get_logger("analysis", "site", None)
REQUIRED = ["dim_site", "mart_site_hotspot", "mart_region_priority"]

# Severity scale of the brand book (Low / Moderate / High / Critical) mapped onto MART-1's results.
# The Moderate boundary is the poor-year rule from config/business_rules.yaml (more than 10% of days above the limit).
CRITICAL_MIN_RATE = 0.50
SEVERITY_LEGEND = [
    {"key": "low", "label": "Low", "text": "Not a persistent hotspot; {low} of sampling days or fewer above the limit"},
    {"key": "moderate", "label": "Moderate", "text": "Not a persistent hotspot, but more than {low} of days above the limit"},
    {"key": "high", "label": "High", "text": "Persistent hotspot: poor in at least 3 of its last 5 years"},
    {"key": "critical", "label": "Critical", "text": "Persistent hotspot with half or more of sampling days above the limit"},
    {"key": "none", "label": "Not enough data", "text": "Too few sampling days to score"},
]


def severity_class(status: str, exceedance_rate, poor_year_rate: float) -> str:
    """Brand-book severity level for one site."""
    if status == "insufficient" or exceedance_rate is None or pd.isna(exceedance_rate):
        return "none"
    if status == "hotspot":
        return "critical" if exceedance_rate >= CRITICAL_MIN_RATE else "high"
    return "moderate" if exceedance_rate > poor_year_rate else "low"


def _clean(v):
    """JSON-safe scalar: NaN/NA -> None, numpy types -> Python."""
    if v is None or (not isinstance(v, str) and pd.isna(v)):
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating, float)):
        return round(float(v), 4)
    if isinstance(v, (np.bool_,)):
        return bool(v)
    return v


def build_payload(t: dict[str, pd.DataFrame], period: dict, batch_id: str, generated_at: str,
                  poor_year_rate: float = 0.10) -> dict:
    sites_dim = t["dim_site"]
    hot = t["mart_site_hotspot"].drop_duplicates("site_key")
    regions = t["mart_region_priority"]

    keep = ["site_key", "source_code", "stratum", "realm", "latitude", "longitude", "region_code",
            "water_body_type"]
    s = sites_dim[[c for c in keep if c in sites_dim.columns]].merge(
        hot[["site_key", "is_persistent_hotspot", "exceedance_rate", "median_ratio_to_threshold", "poor_years",
             "years_monitored", "trend_slope", "n_samples"]], on="site_key", how="left")
    s = s.merge(regions[["region_code", "admin1_name", "country_name"]], on="region_code", how="left")
    s["status"] = np.where(s["years_monitored"].isna() | (s["years_monitored"] == 0), "insufficient",
                           np.where(s["is_persistent_hotspot"].fillna(False).astype(bool), "hotspot", "monitored"))
    s = s.dropna(subset=["latitude", "longitude"]).sort_values("site_key").reset_index(drop=True)
    s["severity"] = [severity_class(st, r, poor_year_rate) for st, r in zip(s["status"], s["exceedance_rate"])]

    scored = s[s["status"] != "insufficient"]
    by_stratum = (scored.groupby(["stratum", "realm"]).agg(
        sites=("site_key", "count"), hotspots=("status", lambda x: int((x == "hotspot").sum())),
        median_ratio=("median_ratio_to_threshold", "median"), median_exceedance=("exceedance_rate", "median"))
        .reset_index())
    by_stratum["share"] = by_stratum["hotspots"] / by_stratum["sites"]

    ranked = regions[regions["status"] == "ranked"].sort_values("priority_rank")
    top_regions = [{
        "rank": int(r.priority_rank), "region": r.admin1_name, "country": r.country_name,
        "country_iso": r.country_iso, "sites": int(r.n_sites), "hotspots": int(r.n_hotspots),
        "score": round(float(r.priority_score), 3), "rank_in_country": _clean(r.rank_in_country),
    } for r in ranked.head(10).itertuples()]

    worst = s[s["status"] == "hotspot"].sort_values(
        ["exceedance_rate", "median_ratio_to_threshold", "site_key"], ascending=[False, False, True]).head(10)
    top_sites = [{
        "site": r.site_key, "stratum": r.stratum, "realm": r.realm, "region": _clean(r.admin1_name),
        "country": _clean(r.country_name), "exceedance_rate": _clean(r.exceedance_rate),
        "ratio": _clean(r.median_ratio_to_threshold), "poor_years": _clean(r.poor_years),
    } for r in worst.itertuples()]

    fo = t.get("fact_observation")
    weather = round(float(fo["rain_48h_mm"].notna().mean()), 4) if fo is not None and len(fo) else None
    meta = {
        "generated_at": generated_at, "curated_batch": batch_id,
        "period": f"{period['start_year']}–{period['end_year']}",
        "observations": int(len(fo)) if fo is not None else None,
        "sites": int(len(s)), "sites_scored": int(len(scored)), "hotspots": int((s["status"] == "hotspot").sum()),
        "regions_ranked": int(len(ranked)), "regions_unranked": int(len(regions) - len(ranked)),
        "weather_coverage": weather,
    }
    meta["hotspot_share"] = round(meta["hotspots"] / meta["sites_scored"], 4) if meta["sites_scored"] else None

    cols = ["site_key", "latitude", "longitude", "stratum", "realm", "source_code", "status", "severity", "exceedance_rate",
            "median_ratio_to_threshold", "poor_years", "years_monitored", "admin1_name", "country_name",
            "water_body_type"]
    sites_out = [{k: _clean(v) for k, v in zip(cols, row)} for row in s[cols].itertuples(index=False)]
    legend = [dict(item, text=item["text"].format(low=f"{poor_year_rate:.0%}")) for item in SEVERITY_LEGEND]
    return {"meta": meta, "legend": legend, "strata": [{k: _clean(v) for k, v in r.items()} for r in by_stratum.to_dict("records")],
            "top_regions": top_regions, "top_sites": top_sites, "sites": sites_out}


def run(docs_dir: Path | None = None) -> Path:
    cur = curated_dir()
    missing = [n for n in REQUIRED if not (cur / f"{n}.parquet").exists()]
    if missing:
        raise ConfigError(f"Missing curated tables {missing} in {cur}; run CUR-1 and MART-1 first")
    tables = {n: pd.read_parquet(cur / f"{n}.parquet") for n in REQUIRED}
    fo = cur / "fact_observation.parquet"
    if fo.exists():
        tables["fact_observation"] = pd.read_parquet(fo, columns=["obs_key", "rain_48h_mm"])
    manifest = cur / "_manifest.json"
    batch = json.loads(manifest.read_text(encoding="utf-8")).get("batch_id", "unknown") if manifest.exists() else "unknown"
    poor_rate = float(load_yaml("business_rules")["exceedance"]["poor_year_exceedance_rate"])
    payload = build_payload(tables, load_yaml("sampling")["period"], batch,
                            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"), poor_rate)
    out_dir = (Path(docs_dir) if docs_dir else PROJECT_ROOT / "docs") / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "site_data.js"
    tmp = path.with_suffix(".js.tmp")
    tmp.write_text("window.SITE_DATA = " + json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + ";\n",
                   encoding="utf-8")
    os.replace(tmp, path)
    m = payload["meta"]
    log.info(f"{m['sites']} sites ({m['hotspots']} hotspots), {len(payload['top_regions'])} top regions -> {path}")
    return path


if __name__ == "__main__":
    print(run())
