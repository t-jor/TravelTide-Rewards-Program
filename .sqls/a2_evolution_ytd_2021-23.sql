-- Evolution of TravelTide over fiscal years 2021-2023
-- YTD METRICS — aligns all years to the same month/day as the latest session (28.07.2023)
WITH
cutoff AS (
  SELECT
    MAX(session_start)::date                    AS max_sess_date,
    EXTRACT(MONTH FROM MAX(session_start))::int AS m,
    EXTRACT(DAY   FROM MAX(session_start))::int AS d
  FROM sessions
),

-- Sessions YTD (aggregated once)
sess_agg_ytd AS (
  SELECT
    EXTRACT(YEAR FROM s.session_start)::int AS the_year,
    COUNT(*)                                 AS sessions,
    COUNT(DISTINCT s.user_id)                AS active_users,
    COUNT(DISTINCT s.trip_id)                AS trips_booked
  FROM sessions s
  CROSS JOIN cutoff c
  WHERE s.session_start::date <= MAKE_DATE(EXTRACT(YEAR FROM s.session_start)::int, c.m, c.d)
  GROUP BY 1
),

-- Trips per user YTD to derive repeat/booker metrics
per_user_trips_ytd AS (
  SELECT
    EXTRACT(YEAR FROM s.session_start)::int AS the_year,
    s.user_id,
    COUNT(DISTINCT s.trip_id)               AS trips_user_year
  FROM sessions s
  CROSS JOIN cutoff c
  WHERE s.trip_id IS NOT NULL
    AND s.session_start::date <= MAKE_DATE(EXTRACT(YEAR FROM s.session_start)::int, c.m, c.d)
  GROUP BY 1,2
),
repeat_and_bookers_ytd AS (
  SELECT
    the_year,
    COUNT(*) FILTER (WHERE trips_user_year > 1) AS repeat_bookers,
    COUNT(*)                                    AS bookers_total
  FROM per_user_trips_ytd
  GROUP BY 1
),

-- New users by signup year (YTD-aligned)
new_users_ytd AS (
  SELECT
    EXTRACT(YEAR FROM u.sign_up_date)::int AS the_year,
    COUNT(DISTINCT u.user_id)              AS new_users
  FROM users u
  CROSS JOIN cutoff c
  WHERE u.sign_up_date::date <= MAKE_DATE(EXTRACT(YEAR FROM u.sign_up_date)::int, c.m, c.d)
  GROUP BY 1
),

-- Distinct report years present in sessions or signups (YTD)
years_ytd AS (
  SELECT the_year FROM sess_agg_ytd
  UNION
  SELECT the_year FROM new_users_ytd
),

-- Total registered users cumulative up to the YTD cutoff (same month/day per year)
total_users_ytd AS (
  SELECT
    y.the_year,
    COUNT(*) AS total_users
  FROM years_ytd y
  CROSS JOIN cutoff c
  JOIN users u
    ON u.sign_up_date::date <= MAKE_DATE(y.the_year, c.m, c.d)
  GROUP BY y.the_year
)

SELECT
  y.the_year,
  COALESCE(sa.sessions,0)       AS sessions,
  COALESCE(sa.trips_booked,0)   AS trips_booked,
  COALESCE(sa.active_users,0)   AS active_users,
  COALESCE(nu.new_users,0)      AS new_users,
  COALESCE(tu.total_users,0)    AS total_users,
  (COALESCE(tu.total_users,0) - COALESCE(sa.active_users,0)) AS dormant_users,

  -- YoY growth within YTD (percent)
  ROUND( (COALESCE(sa.sessions,0)     - LAG(COALESCE(sa.sessions,0))     OVER(ORDER BY y.the_year))
         / NULLIF(LAG(COALESCE(sa.sessions,0))     OVER(ORDER BY y.the_year), 0)::numeric * 100, 2) AS growth_sessions_pct,
  ROUND( (COALESCE(sa.trips_booked,0) - LAG(COALESCE(sa.trips_booked,0)) OVER(ORDER BY y.the_year))
         / NULLIF(LAG(COALESCE(sa.trips_booked,0)) OVER(ORDER BY y.the_year), 0)::numeric * 100, 2) AS growth_trips_pct,
  ROUND( (COALESCE(sa.active_users,0) - LAG(COALESCE(sa.active_users,0)) OVER(ORDER BY y.the_year))
         / NULLIF(LAG(COALESCE(sa.active_users,0)) OVER(ORDER BY y.the_year), 0)::numeric * 100, 2) AS growth_active_users_pct,
  ROUND( (COALESCE(nu.new_users,0)    - LAG(COALESCE(nu.new_users,0))    OVER(ORDER BY y.the_year))
         / NULLIF(LAG(COALESCE(nu.new_users,0))    OVER(ORDER BY y.the_year), 0)::numeric * 100, 2) AS growth_new_users_pct,
  ROUND( (COALESCE(tu.total_users,0)  - LAG(COALESCE(tu.total_users,0))  OVER(ORDER BY y.the_year))
         / NULLIF(LAG(COALESCE(tu.total_users,0))  OVER(ORDER BY y.the_year), 0)::numeric * 100, 2) AS growth_total_users_pct,

  -- Efficiency & loyalty proxies (YTD-aligned)
  ROUND( (COALESCE(sa.trips_booked,0)::numeric / NULLIF(COALESCE(sa.sessions,0),0)) * 100, 2) AS session_to_booking_conv_pct,
  ROUND( (COALESCE(sa.trips_booked,0)::numeric / NULLIF(COALESCE(sa.active_users,0),0)) , 2)  AS trips_per_active_user,
  ROUND( (COALESCE(sa.sessions,0)::numeric     / NULLIF(COALESCE(sa.active_users,0),0)) , 2)  AS sessions_per_active_user,
  ROUND( (COALESCE(rb.repeat_bookers,0)::numeric / NULLIF(COALESCE(rb.bookers_total,0),0)) * 100, 2) AS repeat_booker_share_pct,

  -- Base coverage (registered vs active/booker)
  ROUND( (COALESCE(sa.active_users,0)::numeric / NULLIF(COALESCE(tu.total_users,0),0)) * 100, 2) AS active_user_rate_pct,
  ROUND( (COALESCE(rb.bookers_total,0)::numeric / NULLIF(COALESCE(tu.total_users,0),0)) * 100, 2) AS booker_rate_pct,
  ROUND( ((COALESCE(tu.total_users,0) - COALESCE(sa.active_users,0))::numeric / NULLIF(COALESCE(tu.total_users,0),0)) * 100, 2) AS dormant_rate_pct
FROM years_ytd y
LEFT JOIN sess_agg_ytd           sa ON sa.the_year = y.the_year
LEFT JOIN new_users_ytd          nu ON nu.the_year = y.the_year
LEFT JOIN total_users_ytd        tu ON tu.the_year = y.the_year
LEFT JOIN repeat_and_bookers_ytd rb ON rb.the_year = y.the_year
ORDER BY y.the_year;









