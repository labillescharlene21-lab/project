# Source Inventory
 
**Owner:**  · **Started:** 
 
"Accessed" = date the source page/documentation was last checked. Update it to the date of the actual data pull on Day 2, and record the batch_id.
Items marked **VERIFY** must be confirmed on the provider's site before submission.
 
---
 
## 1. GEMStat (via Open Water Quality export)
 
| Field | Value |
|---|---|
| Original provider | UNEP GEMS/Water (GEMStat) |
| Harmonized by | Open Water Quality project: UNU-INWEH, Colorado State University, University of Colorado Boulder |
| URL | https://openwaterquality.org/ (Data tab) · original: https://gemstat.org/ |
| Format | CSV (observations), GeoJSON (sites) |
| Retrieval method | Scripted export request (confirmed ING-2 spike). Endpoint: `https://lume-inventory-api.evan-thomas-3d8.workers.dev/api/wq/export` — separate Cloudflare Worker domain, not `openwaterquality.org` itself. `GET`, no auth, CORS open. Params: `year_from`, `year_to`, `var` (indicator code), `source=GEMStat`, `loc_type`, `format`. Responses capped at 100,000 rows (`X-Row-Count`/`X-Row-Capped` headers); extractor chunks by year with a loc_type-split fallback on cap. |
| Coverage | Global; used for Africa, Asia, and Latin America strata |
| Size (OWQ, all years) | ~400K observations |
| Update frequency | Unknown. OWQ appears to be a research compilation (manuscript in preparation); treat as a periodic snapshot. VERIFY |
| License / terms | Observations remain the property of the contributing programmes and must be cited on reuse; each export includes an attribution file. GEMStat's own data policy: VERIFY |
| Accessed | 2026-09-27 (ING-2 spike + real extraction run; batch_id owq_gemstat_<id>) |
| Known limitations | Already harmonized by OWQ (MPN treated as CFU; censored values filled at half the detection limit; cross-source dedupe at ~1 km). Sparse, irregular sampling in many countries. Original GEMStat access is manual. `region` field can include non-target strata (e.g. observed value "Canada"); staging sampling must filter on `region`, not just `source`, to correctly select the intended Africa/Asia/Latin America strata. |
 
## 2. Eionet bathing water (via Open Water Quality export)
 
| Field | Value |
|---|---|
| Original provider | European Environment Agency (EEA) / Eionet |
| Harmonized by | Open Water Quality project (as above) |
| URL | https://openwaterquality.org/ (Data tab) |
| Format | CSV, GeoJSON |
| Retrieval method | Same scripted endpoint as GEMStat, `source=Eionet`. Confirmed via ING-2 spike; same cap/chunking behavior applies. |
| Coverage | Europe; main source of marine / enterococci observations |
| Size (OWQ, all years) | ~2.4M observations |
| Update frequency | Unknown for OWQ snapshot. Bathing water monitoring itself is seasonal (bathing season). VERIFY |
| License / terms | As above. EEA data terms: VERIFY |
| Accessed | 2026-09-27 (ING-2 spike + real extraction run; batch_id owq_eionet_e86ef04d7716) |
| Known limitations | Bathing sites only, sampled mainly during the bathing season, so winter conditions are under-represented. Pre-harmonized by OWQ. Originally obtained by OWQ via web scraping. Zero fecal/total coliform data across the full 2015–2025 pull (dataset is scoped to E. coli/enterococci only — expected, not a bug). Zero E. coli/enterococci data from 2022 onward — cause unconfirmed (possible reporting lag vs. dataset cutoff); flagged as VERIFY. |

### Confirmed `var` codes (ING-2 spike)

| Indicator | `var` code |
|---|---|
| E. coli | `ecoli` |
| Enterococci | `entero` |
| Fecal coliform | `fc` |
| Total coliform | `tc` |
 
## 3. Water Quality Portal (WQP)
 
| Field | Value |
|---|---|
| Provider | USGS, US EPA, National Water Quality Monitoring Council |
| URL | https://www.waterqualitydata.us/ · web services: https://www.waterqualitydata.us/webservices_documentation/ |
| Format | CSV (also available: TSV, Excel, KML) |
| Retrieval method | REST API (site catalog, then results for sampled sites) |
| Coverage | United States; ~6.9M fecal indicator observations |
| Update frequency | Continuous, as contributing agencies submit data |
| License / terms | US federal public data. Usage/citation terms: VERIFY |
| Accessed | 2026-09-26 |
| Known limitations | Data submitted by 400+ organizations with inconsistent units, characteristic names, and detection-limit reporting. WQX 3.0 profiles are in beta; we use the current (non-beta) services. Raw data needs full harmonization in staging. |
 
## 4. Open-Meteo Historical Weather API
 
| Field | Value |
|---|---|
| Provider | Open-Meteo (based on ERA5 / ERA5-Land reanalysis) |
| URL | https://open-meteo.com/en/docs/historical-weather-api · endpoint: https://archive-api.open-meteo.com/v1/archive |
| Format | JSON |
| Retrieval method | REST API, one request per 0.25° grid cell containing sampled sites |
| Variables used | `precipitation_sum`, `temperature_2m_mean` (daily) |
| Coverage | Global, 1940 onward |
| Update frequency | Daily (most recent days lag behind real time). VERIFY |
| License / terms | No API key required for non-commercial use. Data license and attribution: VERIFY on the Open-Meteo site |
| Accessed | 2026-09-26 |
| Known limitations | Gridded model reanalysis, not station measurements; local convective rainfall may be under- or over-estimated. Rate limits apply to the free tier. |
 
## 5. Natural Earth Admin-1 States/Provinces (10m)
 
| Field | Value |
|---|---|
| Provider | Natural Earth |
| URL | https://www.naturalearthdata.com/?p=480 |
| Format | Shapefile (zip) |
| Retrieval method | Automated file download (URL pinned in `config/sources.yaml`) |
| Coverage | Global first-order administrative divisions |
| Update frequency | Irregular versioned releases |
| License / terms | Public domain |
| Accessed | 2026-09-26 |
| Known limitations | Theme marked beta by Natural Earth. Some countries (e.g. France) are represented at top-level regions only. Administrative, not watershed, boundaries. |
 
---
 
## Excluded sources
 
| Source | Reason |
|---|---|
| WPdx | ~1,300 observations globally; too few repeat samples per site to assess persistence |
| UK Open WIMS, DataStream, Hub'Eau, NMMP, LAWA | Out of scope for the 7-day build; listed as future improvements (native API ingestion) |