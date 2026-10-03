-- Representative queries (MART-1). Run in psql or any SQL client against the warehouse.
-- Table and column names follow the ERD (natural keys). Each query states the question it answers.

-- Q1. Which 10 regions should be prioritized for rehabilitation first?
SELECT r.priority_rank,
       r.admin1_name,
       r.country_name,
       r.n_sites,
       r.n_hotspots,
       ROUND(r.priority_score::numeric, 3) AS priority_score
FROM mart_region_priority r
WHERE r.status = 'ranked'
ORDER BY r.priority_rank
LIMIT 10;

-- Q2. How many persistent hotspots are there in each stratum and realm?
SELECT s.stratum,
       h.realm,
       COUNT(*)                                            AS sites_scored,
       SUM(CASE WHEN h.is_persistent_hotspot THEN 1 ELSE 0 END) AS persistent_hotspots,
       ROUND(AVG(CASE WHEN h.is_persistent_hotspot THEN 1.0 ELSE 0 END), 3) AS hotspot_share
FROM mart_site_hotspot h
JOIN dim_site s ON s.site_key = h.site_key
GROUP BY s.stratum, h.realm
ORDER BY s.stratum, h.realm;

-- Q3. Do exceedance rates rise after rainfall, and is the effect stronger at hotspots?
SELECT h.is_persistent_hotspot,
       COUNT(*) AS sites,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY h.wet_exceedance_rate)::numeric, 3) AS median_wet_rate,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY h.dry_exceedance_rate)::numeric, 3) AS median_dry_rate
FROM mart_site_hotspot h
WHERE h.wet_exceedance_rate IS NOT NULL
  AND h.dry_exceedance_rate IS NOT NULL
GROUP BY h.is_persistent_hotspot
ORDER BY h.is_persistent_hotspot DESC;

-- Q4. Which countries have the most persistent hotspots?
SELECT g.country_name,
       COUNT(*)                                            AS sites_scored,
       SUM(CASE WHEN h.is_persistent_hotspot THEN 1 ELSE 0 END) AS persistent_hotspots
FROM mart_site_hotspot h
JOIN dim_site s   ON s.site_key = h.site_key
JOIN dim_region g ON g.region_code = s.region_code
GROUP BY g.country_name
ORDER BY persistent_hotspots DESC, g.country_name;

-- Q5. LINEAGE TRACE (live demo): where did one hotspot observation come from?
-- Follows one record from the top-ranked region back to its raw file and batch.
-- Deterministic: always picks the same record (lowest obs_key at the lowest site_key).
WITH top_region AS (
    SELECT region_code FROM mart_region_priority WHERE priority_rank = 1
),
hot_site AS (
    SELECT h.site_key
    FROM mart_site_hotspot h
    JOIN dim_site s ON s.site_key = h.site_key
    WHERE h.is_persistent_hotspot
      AND s.region_code = (SELECT region_code FROM top_region)
    ORDER BY h.site_key
    LIMIT 1
)
SELECT o.obs_key,
       o.site_key,
       s.source_code,
       s.source_site_id,
       o.source_record_id,
       o.indicator_code,
       o.sample_date,
       o.original_value,
       o.original_unit,
       o.value_cfu_100ml,
       o.raw_batch_id,
       o.raw_file,
       s.region_code
FROM fact_observation o
JOIN dim_site s ON s.site_key = o.site_key
WHERE o.site_key = (SELECT site_key FROM hot_site)
ORDER BY o.obs_key
LIMIT 1;

-- Q6. SPOT CHECK (MART-1 real-data verification): recount one site's years by hand.
-- Replace :site_key with a site from Q5. Compare n_days / exceed_days with mart_site_hotspot.
-- (Replicates on the same day are collapsed in the mart, so compare distinct days.)
SELECT EXTRACT(YEAR FROM o.sample_date)::int AS year,
       COUNT(DISTINCT o.sample_date)         AS n_days,
       COUNT(DISTINCT o.sample_date) FILTER (WHERE o.exceeds_threshold) AS days_with_any_exceeding_replicate
FROM fact_observation o
JOIN dim_site s      ON s.site_key = o.site_key
JOIN dim_indicator i ON i.indicator_code = o.indicator_code AND i.realm = s.realm AND i.is_scored
WHERE o.site_key = :'site_key'
GROUP BY 1
ORDER BY 1;
