## Key findings 
- Every row is accounted for. 1.36 million raw OWQ rows and 13,240 WQP rows became 25,108 staging rows = 25,108 curated rows. Nearly all OWQ rows were dropped only because their sites weren't sampled, by design.
- Latin America (GEMStat, mostly Mexico) is heavily polluted. E. coli median 750 CFU/100 mL, nearly double the 410 limit, and 56% of samples exceed it. 88% of sites are persistent hotspots.
- Europe's bathing sites are clean. Only about 3% of samples exceed the limits, and medians are 10–15.
- The US sits in between. About 14–15% of samples exceed, and roughly a quarter to a third of sites are persistent hotspots.
- The time coverage is very uneven. WQP covers 2015–2025, Eionet ends in 2021, and GEMStat effectively ends in 2018 (1,062 rows in 2018, then 225, 118, 5). Latin America's "recent years" are really 2015–2018, which is an important caveat for comparing strata.

## Current Limitations
- Lab upper limits. GEMStat values cluster at 24,196 and 2,419,600. These are laboratory reporting maximums (IDEXX Quanti-Tray with dilutions), meaning "at least this much." The true levels are likely higher, so the severity scores understate Latin America's pollution. The exceedance and hotspot results aren't affected, since those values are far above 410 anyway. The fix adds a pct_at_lab_upper_limit column so this shows up in the report.
- Censoring is only visible for WQP (5–13%). OWQ had already filled censored values before export, so the 0% for OWQ means "unknown," not "none."
- About 55% missing in activity_id, activity_type, and result_status are simply the OWQ rows, since OWQ doesn't have those fields. That's expected.
- WQP unit drops: 1,449 rows (~11% of WQP) dropped as unmapped units, mostly "MPN" and "CFU" with no volume. That's the STG-2 follow-up, which would recover about 1,271 rows.
- The weather section isn't meaningful yet (0–15% coverage from the short test pull).