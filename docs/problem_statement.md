# Problem Statement and Objectives
 
**Owner:** Labilles **Status:** WIP
 
## Title
 
Identifying Persistent Fecal-Indicator Pollution Hotspots for Water Rehabilitation Planning: A Global Multi-Source Water Quality Data Pipeline
 
## Background
 
Whether a river, lake, or beach is considered safe is usually decided by counting fecal indicator bacteria: *Escherichia coli* in fresh water and *intestinal enterococci* in marine water. These measurements are collected by hundreds of agencies across many countries, and each publishes them with its own schema, units, indicator names, and sampling schedule.
 
The Open Water Quality project (UNU-INWEH) recently compiled about 11.1 million of these observations from nine public sources and found two structural problems: openly shared records are heavily concentrated in the United States, Europe, and Canada, and most sites are sampled too infrequently to capture short, rainfall-driven contamination events.
 
## Problem
 
Because the data is fragmented, planners cannot easily answer a basic question: **which places have pollution problems that keep coming back, year after year?** A single high reading may be a one-off event. A site that exceeds safety thresholds in most years is a structural problem that needs rehabilitation. Telling the two apart requires consolidating many years of data from many sources, harmonizing it, and linking it to weather conditions such as rainfall.
 
## Why this needs a data pipeline
 
A one-time analysis would be outdated as soon as new monitoring data is published. The same steps (ingestion from multiple sources, harmonization of units and indicators, validation, weather integration, and scoring) must run every time the data is refreshed. An automated, validated, reproducible pipeline makes the hotspot rankings repeatable, traceable back to source records, and easy to update.
 
## Stakeholders
 
| Stakeholder | How they use the data product |
|---|---|
| **Water rehabilitation planners / river basin organizations** (primary) | Use the regional priority ranking to decide where to invest in rehabilitation first |
| **National and regional environmental regulators** | Identify sites with persistent threshold exceedances for compliance follow-up |
| **Public health agencies** | Identify recreational waters with recurring risk, and whether risk rises after rainfall |
| **Local government units** | See which hotspots fall within their administrative region |
| **Researchers and NGOs** | Reuse the harmonized, documented curated dataset for further analysis |
 
## Objectives
 
1. Automatically ingest fecal indicator observations, weather, and boundary data from 5 independent sources in multiple formats.
2. Harmonize observations into one schema, one indicator vocabulary, and one unit (CFU/100 mL).
3. Apply automated data-quality checks at the raw, staging, and curated layers.
4. Store the curated data model in PostgreSQL with keys and relationships.
5. Orchestrate the full flow with Apache Airflow in a Dockerized environment.
6. Identify persistent hotspot sites and rank regions for rehabilitation priority.
7. Measure whether rainfall is associated with higher exceedance rates at hotspot sites.
## Key questions
 
1. Which monitoring sites exceed the single-sample safety threshold repeatedly across years?
2. Which administrative regions contain the most persistent hotspots?
3. Are exceedance rates higher after rainfall (wet days) than in dry conditions?
4. Which regions should be prioritized for rehabilitation, and why?
## Expected data product
 
- **Site hotspot table** (`mart_site_hotspot`): per-site persistence, severity, trend, and wet vs. dry exceedance rates.
- **Regional priority ranking** (`mart_region_priority`, also exported as CSV): regions ranked by a documented, weighted priority score.
- **Hotspot map** and supporting analytics (bonus).
## Scope
 
| In scope | Out of scope |
|---|---|
| Surface water: rivers, streams, lakes, reservoirs, estuaries, coastal/marine | Groundwater and drinking-water supply compliance |
| *E. coli* (fresh) and enterococci (marine) as scored indicators | Chemical pollutants and nutrients |
| Fecal and total coliform as supplementary, unscored indicators | Real-time alerting or forecasting of individual events |
| 2015–2025 | Data before 2015 |
| Stratified global sample: US, Europe, Africa, Asia, Latin America | Exhaustive coverage of every monitored site |
 
## Known limitations
 
- The analysis covers a stratified sample of sites, not every monitored water body.
- Sampling frequency and laboratory methods differ between countries, so comparisons use the ratio to threshold rather than raw concentrations.
- Weather data is gridded reanalysis, not a measurement at the sampling site.
- Regions with little openly shared data (much of the Global South) are represented by fewer sites. This reflects gaps in data sharing, not necessarily lower pollution.
## Success criteria
 
- One Airflow run takes all sources from raw to marts without manual steps.
- Rerunning with the same parameters produces identical row counts.
- Every curated record can be traced back to its source file and batch.
- The priority ranking can be reproduced from the repository on another machine.