"""EDA / descriptive statistics and source profiling report (DOC-2).

Reads what the pipeline produced and writes:
    docs/eda_report.md        the profiling report (tables + figures), regenerated on every run
    docs/figures/eda_*.png          figures for the report and slides
    outputs/eda/*.csv               every table as CSV

Inputs (whatever exists is used; missing layers are reported, not fatal):
    data/raw/**/manifest.json                          raw volumes per source
    data/staging/observations (hive partitions)        harmonized observations       (required)
    data/staging/sites/sites.parquet                   sites, eligibility, sampling   (required)
    data/staging/_drop_log.parquet, observations/_manifest.json   drops, unmapped units
    data/curated/*.parquet                             weather coverage, hotspots     (optional)

Read-only: never writes to data/. Run:  python -m src.analysis.eda
"""
from __future__ import annotations

import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")                     # no display needed (servers, Docker, CI)
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import pyarrow.dataset as ds  # noqa: E402

from src.utils.config import PROJECT_ROOT, load_yaml  # noqa: E402
from src.utils.log import get_logger  # noqa: E402
from src.utils.paths import curated_dir, data_dir, staging_dir  # noqa: E402
from src.validation.core import outputs_dir  # noqa: E402

log = get_logger("analysis", "eda", None)
INDICATOR_ORDER = ["e_coli", "enterococci", "fecal_coliform", "total_coliform"]
# Common laboratory upper reporting limits (MPN tube tables and IDEXX Quanti-Tray, with dilutions).
# A result exactly at one of these usually means "at least this much": a right-censored value.
UPPER_LIMITS = [1600, 2400, 2419.6, 16000, 24000, 24196, 160000, 240000, 241960, 2419600]
PLAIN_INT_COLUMNS = {"year"}


# ---------------------------------------------------------------- helpers
def md_table(df: pd.DataFrame, floatfmt: str = "{:,.2f}") -> str:
    """Markdown table without extra dependencies."""
    def fmt(v, col=None):
        if isinstance(v, (bool, np.bool_)):
            return "yes" if v else "no"
        if col in PLAIN_INT_COLUMNS and isinstance(v, (int, np.integer)):
            return str(v)
        if isinstance(v, (float, np.floating)):
            return "" if pd.isna(v) else floatfmt.format(v)
        if isinstance(v, (int, np.integer)):
            return f"{v:,}"
        return "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)
    cols = list(df.columns)
    lines = ["| " + " | ".join(map(str, cols)) + " |", "|" + "---|" * len(cols)]
    lines += ["| " + " | ".join(fmt(v, c) for v, c in zip(row, cols)) + " |" for row in df.itertuples(index=False)]
    return "\n".join(lines)


def thresholds(sampling: dict) -> dict[str, float]:
    return {spec["primary_indicator"]: float(spec["threshold_cfu_100ml"]) for spec in sampling["realms"].values()}


# ---------------------------------------------------------------- loading
def load_inputs() -> dict:
    stg = staging_dir()
    obs = ds.dataset(stg / "observations", format="parquet", partitioning="hive").to_table().to_pandas()
    obs["sample_date"] = pd.to_datetime(obs["sample_date"])
    sites = pd.read_parquet(stg / "sites" / "sites.parquet")
    drops_path = stg / "_drop_log.parquet"
    drops = pd.read_parquet(drops_path) if drops_path.exists() else pd.DataFrame(
        columns=["stage", "source_code", "reason", "row_count", "batch_id"])
    mpath = stg / "observations" / "_manifest.json"
    stg_manifest = json.loads(mpath.read_text(encoding="utf-8")) if mpath.exists() else {}
    cur = curated_dir()
    curated = {n: pd.read_parquet(cur / f"{n}.parquet")
               for n in ("fact_observation", "dim_site", "mart_site_hotspot", "mart_region_priority")
               if (cur / f"{n}.parquet").exists()}
    raw = []
    for m in sorted((data_dir() / "raw").rglob("manifest.json")):
        j = json.loads(m.read_text(encoding="utf-8"))
        raw.append({"source": j.get("source_code"), "batch_id": j.get("batch_id"), "status": j.get("status"),
                    "files": len(j.get("files", [])), "rows": j.get("total_rows") or 0,
                    "size_mb": sum(f.get("size_bytes") or 0 for f in j.get("files", [])) / 1e6})
    return {"obs": obs, "sites": sites, "drops": drops, "stg_manifest": stg_manifest,
            "curated": curated, "raw": pd.DataFrame(raw)}


# ---------------------------------------------------------------- tables
def table_raw(raw: pd.DataFrame, stg_manifest: dict) -> pd.DataFrame:
    """Raw batches actually used by staging (STG-2 inputs), plus every other batch present."""
    used = set(stg_manifest.get("params", {}).get("inputs", {}).values())
    if raw.empty:
        return raw
    t = raw.copy()
    t["used_by_staging"] = t["batch_id"].isin(used)
    return t.sort_values(["source", "batch_id"]).reset_index(drop=True)


def table_volume(obs, sites, curated, stg_manifest) -> pd.DataFrame:
    rows_in = {s: c.get("rows_in") for s, c in stg_manifest.get("row_counts", {}).items()}
    out = []
    for src, g in obs.groupby("source_code"):
        fo = curated.get("fact_observation")
        cur_n = int(fo["obs_key"].str.startswith(f"{src}:").sum()) if fo is not None else None
        out.append({"source": src, "raw_rows_read": rows_in.get(src), "staging_rows": len(g),
                    "curated_rows": cur_n, "sampled_sites": int(sites[sites["is_sampled"] & (sites["source_code"] == src)].shape[0]),
                    "sites_with_data": g["site_key"].nunique(),
                    "first_date": g["sample_date"].min().date(), "last_date": g["sample_date"].max().date()})
    return pd.DataFrame(out)


def table_by_indicator(obs) -> pd.DataFrame:
    t = obs.pivot_table(index="indicator_code", columns="source_code", values="obs_key", aggfunc="count", fill_value=0)
    t["total"] = t.sum(axis=1)
    return t.reindex([i for i in INDICATOR_ORDER if i in t.index]).reset_index()


def table_by_year(obs) -> pd.DataFrame:
    t = obs.assign(year=obs["sample_date"].dt.year).pivot_table(
        index="year", columns="source_code", values="obs_key", aggfunc="count", fill_value=0)
    t["total"] = t.sum(axis=1)
    return t.reset_index()


def table_value_stats(obs, thr: dict) -> pd.DataFrame:
    rows = []
    for (src, ind), g in obs.groupby(["source_code", "indicator_code"]):
        v = g["value_cfu_100ml"].dropna()
        limit = thr.get(ind)
        rows.append({"source": src, "indicator": ind, "n": len(v),
                     "min": v.min(), "p25": v.quantile(.25), "median": v.median(), "p75": v.quantile(.75),
                     "p95": v.quantile(.95), "max": v.max(),
                     "geo_mean": float(np.expm1(np.log1p(v).mean())) if len(v) else np.nan,
                     "pct_above_threshold": (v > limit).mean() * 100 if limit else np.nan,
                     "pct_at_lab_upper_limit": v.round(1).isin(UPPER_LIMITS).mean() * 100,
                     "pct_censored": g["is_censored"].mean() * 100})
    return pd.DataFrame(rows)


def table_sites(sites) -> pd.DataFrame:
    s = sites[sites["stratum"].notna()]
    t = s.groupby(["stratum", "realm"]).agg(candidates=("site_key", "count"), eligible=("is_eligible", "sum"),
                                            sampled=("is_sampled", "sum")).reset_index()
    return t.astype({"eligible": int, "sampled": int})


def table_samples_per_site(obs) -> pd.DataFrame:
    days = obs.groupby(["source_code", "site_key"]).agg(sample_days=("sample_date", "nunique"),
                                                        years=("sample_date", lambda d: d.dt.year.nunique()))
    return days.groupby("source_code").agg(sites=("sample_days", "count"), min_days=("sample_days", "min"),
                                           median_days=("sample_days", "median"), max_days=("sample_days", "max"),
                                           median_years=("years", "median")).reset_index()


def table_nulls(obs) -> pd.DataFrame:
    pct = obs.isna().mean().mul(100).round(2)
    return pct[pct > 0].sort_values(ascending=False).rename("pct_null").rename_axis("column").reset_index()


def table_drops(drops) -> pd.DataFrame:
    if drops.empty:
        return drops
    return drops.groupby(["stage", "source_code", "reason"], as_index=False)["row_count"].sum() \
        .sort_values(["stage", "source_code", "row_count"], ascending=[True, True, False])


def table_unmapped_units(stg_manifest) -> pd.DataFrame:
    rows = [{"source": s, "unit": u, "rows": n}
            for s, units in stg_manifest.get("unmapped_units", {}).items() for u, n in units.items()]
    return pd.DataFrame(rows, columns=["source", "unit", "rows"]).sort_values("rows", ascending=False)


def table_hotspots(curated) -> pd.DataFrame | None:
    if "mart_site_hotspot" not in curated or "dim_site" not in curated:
        return None
    h = curated["mart_site_hotspot"].merge(curated["dim_site"][["site_key", "stratum"]], on="site_key")
    return h.groupby(["stratum", "realm"]).agg(
        sites=("site_key", "count"), hotspots=("is_persistent_hotspot", "sum"),
        hotspot_share=("is_persistent_hotspot", "mean"), median_ratio=("median_ratio_to_threshold", "median"),
        median_exceedance_rate=("exceedance_rate", "median")).reset_index()


def table_weather(curated) -> pd.DataFrame | None:
    fo = curated.get("fact_observation")
    if fo is None:
        return None
    fo = fo.assign(source=fo["obs_key"].str.split(":").str[0])
    return fo.groupby("source").agg(observations=("obs_key", "count"),
                                    pct_with_rain=("rain_48h_mm", lambda s: s.notna().mean() * 100),
                                    pct_wet=("is_wet", lambda s: (s == True).mean() * 100)).reset_index()  # noqa: E712


# ---------------------------------------------------------------- figures
def save(fig, path: Path) -> str:
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)
    return path.name


def figures(obs, thr, curated, fig_dir: Path) -> dict[str, str]:
    fig_dir.mkdir(parents=True, exist_ok=True)
    out = {}

    t = obs.assign(year=obs["sample_date"].dt.year).groupby(["year", "source_code"]).size().unstack(fill_value=0)
    fig, ax = plt.subplots(figsize=(8, 4))
    t.plot(kind="bar", stacked=True, ax=ax)
    ax.set(title="Observations per year by source", xlabel="Year", ylabel="Observations")
    out["per_year"] = save(fig, fig_dir / "eda_observations_per_year.png")

    m = obs.assign(month=obs["sample_date"].dt.month).groupby(["month", "source_code"]).size().unstack(fill_value=0)
    fig, ax = plt.subplots(figsize=(8, 4))
    m.plot(ax=ax, marker="o")
    ax.set(title="Seasonality: observations per calendar month", xlabel="Month", ylabel="Observations", xticks=range(1, 13))
    out["seasonality"] = save(fig, fig_dir / "eda_seasonality.png")

    scored = [i for i in INDICATOR_ORDER if i in thr and i in set(obs["indicator_code"])]
    fig, axes = plt.subplots(1, max(len(scored), 1), figsize=(6 * max(len(scored), 1), 4), squeeze=False)
    for ax, ind in zip(axes[0], scored):
        v = obs.loc[obs["indicator_code"] == ind, "value_cfu_100ml"].dropna()
        ax.hist(np.log10(v + 1), bins=40)
        ax.axvline(np.log10(thr[ind] + 1), color="red", linestyle="--", label=f"threshold {thr[ind]:.0f}")
        ax.set(title=f"{ind}: distribution (log10 of value + 1)", xlabel="log10(CFU/100 mL + 1)", ylabel="Observations")
        ax.legend()
    out["values"] = save(fig, fig_dir / "eda_value_distribution.png")

    days = obs.groupby(["source_code", "site_key"])["sample_date"].nunique().reset_index(name="days")
    fig, ax = plt.subplots(figsize=(8, 4))
    for src, g in days.groupby("source_code"):
        ax.hist(g["days"], bins=30, alpha=.5, label=src)
    ax.set(title="Sample-days per site", xlabel="Sample-days (2015–2025)", ylabel="Sites")
    ax.legend()
    out["per_site"] = save(fig, fig_dir / "eda_samples_per_site.png")

    hs = table_hotspots(curated)
    if hs is not None:
        fig, ax = plt.subplots(figsize=(8, 4))
        labels = hs["stratum"] + " / " + hs["realm"]
        ax.barh(labels, hs["hotspot_share"] * 100)
        ax.set(title="Persistent hotspot share by stratum", xlabel="% of scored sites", xlim=(0, 100))
        out["hotspots"] = save(fig, fig_dir / "eda_hotspot_share.png")
    return out


# ---------------------------------------------------------------- report
def build_report(tables: dict, figs: dict, inputs: dict, period: dict) -> str:
    obs, sites = inputs["obs"], inputs["sites"]
    stg_id = inputs["stg_manifest"].get("batch_id", "unknown")
    lines = [
        "# Source Profiling and Descriptive Statistics",
        "",
        "*Generated by `python -m src.analysis.eda` from the pipeline's current output; do not edit by hand. "
        f"Staging batch `{stg_id}`, period {period['start_year']}–{period['end_year']}.*",
        "",
        "## 1. Summary",
        "",
        f"- **{len(obs):,} harmonized observations** from **{obs['site_key'].nunique()} sites** "
        f"({int(sites['is_sampled'].sum())} sampled) across **{obs['source_code'].nunique()} sources**.",
        f"- Dates {obs['sample_date'].min().date()} to {obs['sample_date'].max().date()}; "
        f"{obs['is_censored'].mean():.1%} of values are censored (below/above a detection limit).",
        "",
        "## 2. Raw batches",
        "",
        md_table(tables["raw"]) if not tables["raw"].empty else "_No raw manifests found._",
        "",
        "## 3. Volume by layer",
        "",
        md_table(tables["volume"]),
        "",
        "## 4. Observations by indicator and source",
        "",
        md_table(tables["by_indicator"]),
        "",
        "## 5. Observations by year",
        "",
        md_table(tables["by_year"]),
        "",
        f"![Observations per year]({'figures/' + figs['per_year']})",
        "",
        f"![Seasonality]({'figures/' + figs['seasonality']})",
        "",
        "## 6. Values (CFU/100 mL)",
        "",
        md_table(tables["values"]),
        "",
        "Geometric mean uses the same shift as MART-1 (exp(mean(log(v + 1))) − 1). "
        "`pct_above_threshold` uses the EPA single-sample values (scored indicators only). "
        "`pct_at_lab_upper_limit`: values exactly at a common laboratory upper reporting limit "
        "(e.g. 2,419.6 or 24,196 MPN/100 mL); these usually mean \"at least this much\", so true levels "
        "may be higher and severity is understated.",
        "",
        f"![Value distribution]({'figures/' + figs['values']})",
        "",
        "## 7. Sites, eligibility and sampling",
        "",
        md_table(tables["sites"]),
        "",
        md_table(tables["per_site"]),
        "",
        f"![Samples per site]({'figures/' + figs['per_site']})",
        "",
        "## 8. Data quality: missing values, drops, unmapped units",
        "",
        "**Columns with missing values (staging observations):**",
        "",
        md_table(tables["nulls"]) if not tables["nulls"].empty else "_No missing values._",
        "",
        "**Rows dropped after raw, by reason:**",
        "",
        md_table(tables["drops"]) if not tables["drops"].empty else "_No drop log found._",
        "",
        "**Unmapped units (dropped):**",
        "",
        md_table(tables["units"]) if not tables["units"].empty else "_None._",
        "",
    ]
    if tables.get("weather") is not None:
        lines += ["## 9. Weather coverage (curated)", "", md_table(tables["weather"]), "",
                  "Rain-based results are only meaningful once coverage is complete (full ING-4 pull).", ""]
    if tables.get("hotspots") is not None:
        lines += ["## 10. Persistent hotspots by stratum (MART-1)", "", md_table(tables["hotspots"]), "",
                  f"![Hotspot share]({'figures/' + figs['hotspots']})", "",
                  "Strata measure different kinds of water (GEMStat river stations, Eionet bathing sites, "
                  "WQP mixed networks), so cross-stratum differences reflect monitoring design as well as pollution.", ""]
    return "\n".join(lines)


def run(docs_dir: Path | None = None) -> Path:
    """Build tables, figures and the report. docs_dir defaults to <repo>/docs (tests pass a temp folder)."""
    sampling = load_yaml("sampling")
    thr = thresholds(sampling)
    inputs = load_inputs()
    obs, sites, curated = inputs["obs"], inputs["sites"], inputs["curated"]
    tables = {
        "raw": table_raw(inputs["raw"], inputs["stg_manifest"]),
        "volume": table_volume(obs, sites, curated, inputs["stg_manifest"]),
        "by_indicator": table_by_indicator(obs),
        "by_year": table_by_year(obs),
        "values": table_value_stats(obs, thr),
        "sites": table_sites(sites),
        "per_site": table_samples_per_site(obs),
        "nulls": table_nulls(obs),
        "drops": table_drops(inputs["drops"]),
        "units": table_unmapped_units(inputs["stg_manifest"]),
        "weather": table_weather(curated),
        "hotspots": table_hotspots(curated),
    }
    csv_dir = outputs_dir() / "eda"
    csv_dir.mkdir(parents=True, exist_ok=True)
    for name, t in tables.items():
        if t is not None:
            t.to_csv(csv_dir / f"{name}.csv", index=False, encoding="utf-8-sig")
    docs = Path(docs_dir) if docs_dir else PROJECT_ROOT / "docs"
    figs = figures(obs, thr, curated, docs / "figures")
    report = docs / "eda_report.md"
    report.write_text(build_report(tables, figs, inputs, sampling["period"]), encoding="utf-8")
    log.info(f"{len(obs):,} observations profiled; report {report}; {len(figs)} figures; tables in {csv_dir}")
    return report


if __name__ == "__main__":
    print(run())
