-- YTD RETENTION Heatmap (only cohort_year = 2021), dynamic cutoff by max(session_start)
WITH
cutoff AS (
  SELECT
    MAX(session_start)::date                    AS max_sess_date,
    EXTRACT(MONTH FROM MAX(session_start))::int AS m,
    EXTRACT(DAY   FROM MAX(session_start))::int AS d
  FROM sessions
),

/* ---------- ACTIVE COHORT (first active year) ---------- */
active_years AS (  -- years a user is active (≥1 session) under YTD cutoff
  SELECT DISTINCT
    EXTRACT(YEAR FROM s.session_start)::int AS ytd_year,
    s.user_id
  FROM sessions s
  CROSS JOIN cutoff c
  WHERE s.session_start::date <= MAKE_DATE(EXTRACT(YEAR FROM s.session_start)::int, c.m, c.d)
),
first_active AS (  -- cohort = first active year
  SELECT user_id, MIN(ytd_year) AS cohort_year_ytd
  FROM active_years
  GROUP BY user_id
),
pairs_active AS (  -- cohort year paired with all later active years
  SELECT
    fa.cohort_year_ytd,
    ay.ytd_year AS compare_year_ytd,
    fa.user_id
  FROM first_active fa
  JOIN active_years ay
    ON ay.user_id = fa.user_id
   AND ay.ytd_year >= fa.cohort_year_ytd
),
cohort_size_active AS (
  SELECT cohort_year_ytd, COUNT(DISTINCT user_id) AS cohort_size
  FROM first_active
  GROUP BY cohort_year_ytd
),

/* ---------- BOOKER COHORT (first booking year) ---------- */
booker_years AS (  -- years a user is a booker (≥1 trip) under YTD cutoff
  SELECT DISTINCT
    EXTRACT(YEAR FROM s.session_start)::int AS ytd_year,
    s.user_id
  FROM sessions s
  CROSS JOIN cutoff c
  WHERE s.trip_id IS NOT NULL
    AND s.session_start::date <= MAKE_DATE(EXTRACT(YEAR FROM s.session_start)::int, c.m, c.d)
),
first_booker AS (  -- cohort = first booking year
  SELECT user_id, MIN(ytd_year) AS cohort_year_ytd
  FROM booker_years
  GROUP BY user_id
),
pairs_booker AS (  -- cohort year paired with all later booker years
  SELECT
    fb.cohort_year_ytd,
    by.ytd_year AS compare_year_ytd,
    fb.user_id
  FROM first_booker fb
  JOIN booker_years by
    ON by.user_id = fb.user_id
   AND by.ytd_year >= fb.cohort_year_ytd
),
cohort_size_booker AS (
  SELECT cohort_year_ytd, COUNT(DISTINCT user_id) AS cohort_size
  FROM first_booker
  GROUP BY cohort_year_ytd
)

/* ---------- FINAL (filtered to cohort_year = 2021) ---------- */
SELECT
  'active' AS cohort_type,
  p.cohort_year_ytd,
  p.compare_year_ytd,
  cs.cohort_size,
  COUNT(DISTINCT p.user_id) AS retained_users,
  ROUND(COUNT(DISTINCT p.user_id)::numeric / NULLIF(cs.cohort_size,0) * 100, 2) AS retention_rate_pct
FROM pairs_active p
JOIN cohort_size_active cs USING (cohort_year_ytd)
WHERE p.cohort_year_ytd = 2021
GROUP BY 1,2,3, cs.cohort_size

UNION ALL

SELECT
  'booker' AS cohort_type,
  p.cohort_year_ytd,
  p.compare_year_ytd,
  cs.cohort_size,
  COUNT(DISTINCT p.user_id) AS retained_users,
  ROUND(COUNT(DISTINCT p.user_id)::numeric / NULLIF(cs.cohort_size,0) * 100, 2) AS retention_rate_pct
FROM pairs_booker p
JOIN cohort_size_booker cs USING (cohort_year_ytd)
WHERE p.cohort_year_ytd = 2021
GROUP BY 1,2,3, cs.cohort_size

ORDER BY cohort_type, compare_year_ytd;