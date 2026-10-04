# Business Rules: Hotspots, Priority and Scope

**Status:** Approved (RULES-1) · **Config:** `config/business_rules.yaml`, `config/sampling.yaml`, `config/mappings.yaml`

Every value that decides which sites count as persistent hotspots, and how regions are ranked, is listed here with what it means and why we chose it. Each justification is one of three kinds:

- **Standard:** taken from a published criterion (cited)
- **Data:** chosen from what the data showed
- **Assumption:** a judgement call, stated openly so it can be challenged

All values live in config files, so changing one changes the results without code changes.

---

## 1. Safety thresholds

| Rule | Value | Meaning | Justification |
|---|---|---|---|
| Freshwater threshold | **410 CFU/100 mL** *E. coli* | A freshwater sample-day "exceeds" above this value | **Standard:** EPA 2012 Recreational Water Quality Criteria, statistical threshold value (STV) for *E. coli* ([EPA](https://www.epa.gov/wqc/recreational-water-quality-criteria-and-methods)) |
| Marine threshold | **130 CFU/100 mL** enterococci | A marine sample-day "exceeds" above this value | **Standard:** same EPA 2012 criteria, STV for enterococci |
| Scored indicators | *E. coli* (freshwater), enterococci (marine) | Only these decide hotspots | **Standard:** the indicators EPA recommends for recreational water. Fecal and total coliform are ingested and reported but **not scored**: neither has a current comparable recreational criterion, and total coliform isn't specific to fecal pollution |
| MPN = CFU | factor 1.0 | Results reported as MPN/100 mL are treated as CFU/100 mL | **Assumption** (same as Open Water Quality): both estimate viable bacteria per 100 mL; they're not identical methods |

## 2. From samples to sample-days

| Rule | Value | Meaning | Justification |
|---|---|---|---|
| Same-day replicates | **shifted geometric mean**, exp(mean(log(v + 1))) − 1 | Several samples at one site on one day count as one sample-day | **Standard:** EPA's criteria summarize bacteria counts with geometric means, because the counts are log-normally distributed. The +1 shift (so zero values are allowed) is our addition; at the values that matter (hundreds of CFU) its effect is negligible. A single sample keeps its exact value |
| Censored values | "<x" → x/2, ">x" → x | Values below a detection limit count as half the limit; above-limit values count as the limit | **Assumption:** the same rule Open Water Quality applied, so WQP and OWQ values are treated identically. Right-censored values are underestimates, which makes exceedance counts conservative |
| QC samples | blanks dropped, field replicates kept | Blanks aren't environmental samples; replicates are real samples, combined per day | **Data:** WQP includes "Quality Control Sample" activity types (ING-3) |

## 3. Persistent hotspots

| Rule | Value | Meaning | Justification |
|---|---|---|---|
| Poor year | **more than 10%** of sample-days exceed | A site-year is "poor" when exceedances are more frequent than the criterion allows | **Standard:** EPA's STV is set so it should not be exceeded in more than 10% of samples. We apply it per calendar year instead of EPA's 30-day window, because most sites are sampled too rarely for 30-day windows (**adaptation**, stated in limitations) |
| Minimum data per year | **5 sample-days** | Years with fewer are "insufficient" and ignored, neither poor nor good | **Assumption:** with fewer than 5 days, a single exceedance (≥ 20%) would make the year poor. To check after the first full run: share of site-years excluded (`docs/analysis_findings.md`) |
| Persistence window | most recent **5** classified years | Only recent years count | **Assumption:** the project targets *current* problems for rehabilitation; old problems may already be fixed |
| Persistent hotspot | poor in **at least 3** of those years | A recurring problem, not a one-off spike | **Assumption:** a majority of recent years. One or two poor years can come from a single storm season; three suggests a structural source |
| Trend | Theil-Sen slope of annual exceedance rate, needs **≥ 3** years | Positive = worsening | **Standard:** Theil-Sen is a widely used robust trend estimator for environmental time series (insensitive to outliers). Fewer than 3 years gives no meaningful slope |

## 4. Rainfall

| Rule | Value | Meaning | Justification |
|---|---|---|---|
| Antecedent rain | sample day + previous day (48 h); + one more day (72 h) | Rain shortly before sampling | **Assumption:** runoff and sewer overflows typically raise bacteria levels within 1–3 days of rain, so 48 h and 72 h windows capture that effect |
| Wet sample-day | **≥ 10 mm** in 48 h | Above this, a sample-day is "wet" | **Assumption:** no universal standard exists; beach-advisory rules vary by agency. 10 mm is a moderate rain that typically produces runoff. Tested in the sensitivity check (section 7) |
| Missing weather | excluded from **both** wet and dry rates | Unknown is not counted as dry | **Assumption:** treating unknown weather as dry would understate wet effects, so unknown days are left out |
| Date alignment | sample date (local) vs weather day (UTC) | Daily totals may include rain after sampling | **Assumption:** the one-day offset is small relative to 48 h windows; stated as a limitation in README §15 |

## 5. Regional priority ranking

| Rule | Value | Meaning | Justification |
|---|---|---|---|
| Minimum sites per region | **3** | Regions with fewer sampled sites are listed but not ranked | **Assumption:** the persistence component is a share of sites. With 1 site it can only be 0% or 100%, and with 2 sites only 0/50/100%, so one site's status decides the region's score. From 3 sites the share has at least four levels and no single site determines it. **To review after the first full run** (reported in `docs/analysis_findings.md`): how many regions reach 3 sites. If too few, options are lowering to 2 (stated as a weaker ranking) or ranking at country level for sparse strata |
| Component: persistence | share of the region's sites that are hotspots | | **Assumption:** directly measures the project's problem, recurring pollution |
| Component: severity | median of sites' median value ÷ threshold | How far above safe levels | **Assumption:** dividing by each realm's EPA threshold makes fresh and marine sites comparable |
| Component: trend | median site trend slope | Getting worse or better | **Assumption:** worsening regions need action sooner |
| Component: rain sensitivity | median of wet ÷ dry exceedance rate | Pollution driven by runoff | **Assumption:** points to runoff and sewer sources, which rehabilitation can target |
| Weights | **0.40 / 0.30 / 0.15 / 0.15** (sum **1.00**) | Persistence counts most | **Assumption:** persistence is the project's definition of a hotspot, so it weighs most; severity next; trend and rain sensitivity refine the order. Tested in the sensitivity check |
| Normalization | min-max across ranked regions | Each component scaled 0–1 before weighting | **Assumption:** puts components with different units on the same scale before weighting |
| Missing component | scored 0, recorded in `notes` | | **Assumption:** keeps the region in the ranking without inventing a value; the gap is visible in `notes` |
| Ties | more hotspots first, then region code | | **Assumption:** any fixed rule works; this one makes ranks identical on every rerun |

## 6. Scope and sampling decisions

| Rule | Value | Justification |
|---|---|---|
| Period | 2015–2025 | **Assumption:** recent enough to be relevant, long enough for 5-year persistence |
| Sites per stratum and realm (K) | **60**, fixed seed 42 | **Data:** US eligible pools are 9,052 freshwater / 1,484 marine; 60 keeps strata balanced, the weather pull feasible (~156 cells/day free tier), and the pipeline rerunnable. Volume still far exceeds 10,000 records |
| Site eligibility | ≥ 24 samples of the primary indicator across ≥ 3 years | **Assumption:** enough history to assess persistence |
| Estuaries | marine (enterococci) | **Standard:** EPA's criteria use enterococci for marine and estuarine (brackish) waters |
| WQP site types | only rivers/streams, lakes, reservoirs, estuaries, ocean; BEACH Program, canals, Great Lakes and stream subtypes excluded (~10,455 sites) | **Data (ING-2):** eligible pools already far exceed K; changing the mapping later would force redoing the weather pull. Stated in limitations |
| Enterococci spelling | "Enterococcus" and "Enterococci" both counted | **Data:** same indicator; "Enterococci" returned 0 summary rows but is kept defensively (ING-2) |
| Eionet water body "other" | excluded (65% of Eionet rows) | **Data (STG-1):** Europe marine already has 224 eligible for 60 slots, and "other" can't be reliably classified as fresh or marine; misclassifying would score sites against the wrong threshold |
| Asia and Africa strata | empty | **Data (STG-1):** Africa has no GEMStat sites; Asia's data is coliform-only. Final sample: US 120, Europe 120, Latin America 60 = **300 sites**. Reported as a data-availability finding |
| French overseas regions | excluded from the Europe stratum | **Assumption:** tropical territories, with different rainfall, would distort a Europe stratum |
| Engineered water bodies | canals, ditches, stormwater excluded | **Assumption:** the ranking targets natural water bodies for rehabilitation; EPA recreational criteria aren't designed for conveyance infrastructure |

## 7. Sensitivity check

**Decision: yes**, as part of ANA-2. After the first full run, recompute the regional ranking with:
1. equal weights (0.25 each), and
2. persistence only (1.0 / 0 / 0 / 0),
3. wet threshold 5 mm and 20 mm,

and report how many of the top-10 regions change. If the top regions stay mostly the same, the ranking doesn't depend on our assumptions; if they change a lot, we say so.

## 8. Change log

| Date | Change | By |
|---|---|---|
| 2026-10-03 | Initial approved values | PM |
| 2026-10-04 | Labels made consistent (Standard / Data / Assumption only); minimum-sites justification strengthened (review by @antoniopaulocuyo) | PM |
