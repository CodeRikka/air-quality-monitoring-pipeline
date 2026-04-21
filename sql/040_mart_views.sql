CREATE OR REPLACE VIEW mart.v_latest_station_observation AS
WITH ranked AS (
    SELECT
        c.*,
        ROW_NUMBER() OVER (
            PARTITION BY c.location_id, c.pollutant_code
            ORDER BY c.timestamp_start_utc DESC, c.updated_at DESC
        ) AS rn
    FROM core.fact_observation_current c
)
SELECT
    r.natural_key,
    r.location_id,
    l.location_type,
    l.country_code,
    l.state_code,
    l.county_code,
    l.site_number,
    l.aqsid,
    l.site_name,
    l.city_name,
    l.latitude,
    l.longitude,
    r.pollutant_code,
    p.pollutant_name,
    r.metric_type,
    r.value_numeric,
    r.unit,
    r.data_granularity,
    r.timestamp_start_utc,
    r.timestamp_local,
    r.source_system,
    r.source_dataset,
    r.data_status,
    t.threshold_type,
    t.threshold_value,
    (r.value_numeric > t.threshold_value) AS is_exceedance
FROM ranked r
JOIN core.dim_location l ON r.location_id = l.location_id
JOIN core.dim_pollutant p ON r.pollutant_code = p.pollutant_code
LEFT JOIN core.dim_exceedance_threshold t
    ON r.pollutant_code = t.pollutant_code
   AND r.unit = t.unit
   AND t.is_active = TRUE
WHERE r.rn = 1;

CREATE OR REPLACE VIEW mart.v_daily_trend AS
SELECT
    l.state_code,
    l.county_code,
    c.location_id,
    c.pollutant_code,
    p.pollutant_name,
    DATE_TRUNC('day', c.timestamp_start_utc) AS day_utc,
    AVG(c.value_numeric) AS avg_value,
    MIN(c.value_numeric) AS min_value,
    MAX(c.value_numeric) AS max_value,
    COUNT(*) AS observation_count,
    COUNT(*) FILTER (WHERE c.data_status = 'final') AS final_count,
    COUNT(*) FILTER (WHERE c.data_status = 'provisional') AS provisional_count
FROM core.fact_observation_current c
JOIN core.dim_location l ON c.location_id = l.location_id
JOIN core.dim_pollutant p ON c.pollutant_code = p.pollutant_code
GROUP BY 1, 2, 3, 4, 5, 6;

CREATE OR REPLACE VIEW mart.v_monthly_trend AS
SELECT
    l.state_code,
    l.county_code,
    c.location_id,
    c.pollutant_code,
    p.pollutant_name,
    DATE_TRUNC('month', c.timestamp_start_utc) AS month_utc,
    AVG(c.value_numeric) AS avg_value,
    MIN(c.value_numeric) AS min_value,
    MAX(c.value_numeric) AS max_value,
    COUNT(*) AS observation_count
FROM core.fact_observation_current c
JOIN core.dim_location l ON c.location_id = l.location_id
JOIN core.dim_pollutant p ON c.pollutant_code = p.pollutant_code
GROUP BY 1, 2, 3, 4, 5, 6;

CREATE OR REPLACE VIEW mart.v_seasonal_trend AS
SELECT
    l.state_code,
    l.county_code,
    c.location_id,
    c.pollutant_code,
    p.pollutant_name,
    EXTRACT(YEAR FROM c.timestamp_start_utc) AS year_utc,
    EXTRACT(QUARTER FROM c.timestamp_start_utc) AS quarter_utc,
    AVG(c.value_numeric) AS avg_value,
    MIN(c.value_numeric) AS min_value,
    MAX(c.value_numeric) AS max_value,
    COUNT(*) AS observation_count
FROM core.fact_observation_current c
JOIN core.dim_location l ON c.location_id = l.location_id
JOIN core.dim_pollutant p ON c.pollutant_code = p.pollutant_code
GROUP BY 1, 2, 3, 4, 5, 6, 7;

CREATE OR REPLACE VIEW mart.v_region_coverage AS
WITH daily_observed AS (
    SELECT
        l.state_code,
        l.county_code,
        c.pollutant_code,
        DATE_TRUNC('day', c.timestamp_start_utc) AS day_utc,
        c.location_id,
        c.natural_key
    FROM core.fact_observation_current c
    JOIN core.dim_location l ON c.location_id = l.location_id
),
region_station_totals AS (
    SELECT
        l.state_code,
        l.county_code,
        c.pollutant_code,
        COUNT(DISTINCT c.location_id) AS total_station_count
    FROM core.fact_observation_current c
    JOIN core.dim_location l ON c.location_id = l.location_id
    GROUP BY 1, 2, 3
)
SELECT
    o.state_code,
    o.county_code,
    o.pollutant_code,
    p.pollutant_name,
    o.day_utc,
    COUNT(o.natural_key) AS observation_count,
    COUNT(DISTINCT o.location_id) AS reporting_station_count,
    t.total_station_count,
    CASE
        WHEN t.total_station_count > 0 THEN
            COUNT(DISTINCT o.location_id)::DOUBLE PRECISION / t.total_station_count::DOUBLE PRECISION
        ELSE NULL
    END AS station_coverage_ratio
FROM daily_observed o
JOIN region_station_totals t
  ON o.state_code = t.state_code
 AND o.county_code = t.county_code
 AND o.pollutant_code = t.pollutant_code
JOIN core.dim_pollutant p ON o.pollutant_code = p.pollutant_code
GROUP BY 1, 2, 3, 4, 5, 8;

CREATE OR REPLACE VIEW mart.v_exceedance_events AS
SELECT
    c.natural_key,
    c.location_id,
    l.state_code,
    l.county_code,
    c.pollutant_code,
    p.pollutant_name,
    c.value_numeric,
    c.unit,
    c.timestamp_start_utc,
    c.timestamp_local,
    c.source_system,
    c.data_status,
    t.threshold_type,
    t.threshold_value,
    (c.value_numeric - t.threshold_value) AS exceedance_margin,
    CASE
        WHEN t.threshold_value > 0 THEN c.value_numeric / t.threshold_value
        ELSE NULL
    END AS exceedance_ratio
FROM core.fact_observation_current c
JOIN core.dim_location l ON c.location_id = l.location_id
JOIN core.dim_pollutant p ON c.pollutant_code = p.pollutant_code
JOIN core.dim_exceedance_threshold t
  ON c.pollutant_code = t.pollutant_code
 AND c.unit = t.unit
 AND t.is_active = TRUE
WHERE c.value_numeric > t.threshold_value;

CREATE OR REPLACE VIEW mart.v_anomaly_spikes AS
WITH daily_region AS (
    SELECT
        l.state_code,
        l.county_code,
        c.pollutant_code,
        DATE_TRUNC('day', c.timestamp_start_utc) AS day_utc,
        AVG(c.value_numeric) AS day_avg_value,
        COUNT(*) AS observation_count
    FROM core.fact_observation_current c
    JOIN core.dim_location l ON c.location_id = l.location_id
    GROUP BY 1, 2, 3, 4
),
scored AS (
    SELECT
        d.*,
        AVG(d.day_avg_value) OVER (
            PARTITION BY d.state_code, d.county_code, d.pollutant_code
            ORDER BY d.day_utc
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS baseline_avg_value,
        STDDEV_SAMP(d.day_avg_value) OVER (
            PARTITION BY d.state_code, d.county_code, d.pollutant_code
            ORDER BY d.day_utc
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS baseline_stddev
    FROM daily_region d
)
SELECT
    s.state_code,
    s.county_code,
    s.pollutant_code,
    p.pollutant_name,
    s.day_utc,
    s.day_avg_value,
    s.observation_count,
    s.baseline_avg_value,
    s.baseline_stddev,
    (s.day_avg_value - s.baseline_avg_value) AS anomaly_delta,
    CASE
        WHEN s.baseline_stddev > 0 THEN
            (s.day_avg_value - s.baseline_avg_value) / s.baseline_stddev
        ELSE NULL
    END AS anomaly_zscore
FROM scored s
JOIN core.dim_pollutant p ON s.pollutant_code = p.pollutant_code
WHERE s.baseline_stddev IS NOT NULL
  AND s.baseline_stddev > 0
  AND ABS((s.day_avg_value - s.baseline_avg_value) / s.baseline_stddev) >= 2.5;
