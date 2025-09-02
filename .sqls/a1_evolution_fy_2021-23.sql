-- Evolution of TravelTide over fiscal years 2021-2023
-- FULL-YEAR METRICS
WITH
-- Sessions aggregated once
sess_agg AS (
  SELECT
    EXTRACT(YEAR FROM s.session_start)::int AS the_year,
    COUNT(*)                                 AS sessions,
    COUNT(DISTINCT s.user_id)                AS active_users,
    COUNT(DISTINCT s.trip_id)                AS trips_booked
  FROM sessions s
  GROUP BY 1
),
-- Trips per user to derive repeat/booker metrics
per_user_trips AS (
  SELECT
    EXTRACT(YEAR FROM s.session_start)::int AS the_year,
    s.user_id,
    COUNT(DISTINCT s.trip_id)               AS trips_user_year
  FROM sessions s
  WHERE s.trip_id IS NOT NULL
  GROUP BY 1,2
),
repeat_and_bookers AS (
  SELECT
    the_year,
    COUNT(*) FILTER (WHERE trips_user_year > 1) AS repeat_bookers,
    COUNT(*)                                    AS bookers_total
  FROM per_user_trips
  GROUP BY 1
),
-- New users by signup year
new_users AS (
  SELECT
    EXTRACT(YEAR FROM u.sign_up_date)::int AS the_year,
    COUNT(DISTINCT u.user_id)              AS new_users
  FROM users u
  GROUP BY 1
),
-- Total registered users cumulative by year end (Dec 31)
years AS (
  SELECT the_year FROM sess_agg
  UNION
  SELECT the_year FROM new_users
),
total_users AS (
  SELECT
    y.the_year,
    COUNT(*) AS total_users
  FROM years y
  JOIN users u
    ON u.sign_up_date::date < MAKE_DATE(y.the_year + 1, 1, 1)
  GROUP BY y.the_year
)

SELECT
  y.the_year,
  COALESCE(sa.sessions,0)             AS sessions,
  COALESCE(sa.trips_booked,0)         AS trips_booked,
  COALESCE(sa.active_users,0)         AS active_users,
  COALESCE(nu.new_users,0)            AS new_users,
  COALESCE(tu.total_users,0)          AS total_users,
  (COALESCE(tu.total_users,0) - COALESCE(sa.active_users,0)) AS dormant_users,

  -- YoY growth (percent)
  ROUND( (COALESCE(sa.sessions,0) - LAG(COALESCE(sa.sessions,0)) OVER(ORDER BY y.the_year))
         / NULLIF(LAG(COALESCE(sa.sessions,0)) OVER(ORDER BY y.the_year),0)::numeric * 100, 2) AS growth_sessions_pct,
  ROUND( (COALESCE(sa.trips_booked,0) - LAG(COALESCE(sa.trips_booked,0)) OVER(ORDER BY y.the_year))
         / NULLIF(LAG(COALESCE(sa.trips_booked,0)) OVER(ORDER BY y.the_year),0)::numeric * 100, 2) AS growth_trips_pct,
  ROUND( (COALESCE(sa.active_users,0) - LAG(COALESCE(sa.active_users,0)) OVER(ORDER BY y.the_year))
         / NULLIF(LAG(COALESCE(sa.active_users,0)) OVER(ORDER BY y.the_year),0)::numeric * 100, 2) AS growth_active_users_pct,
  ROUND( (COALESCE(nu.new_users,0) - LAG(COALESCE(nu.new_users,0)) OVER(ORDER BY y.the_year))
         / NULLIF(LAG(COALESCE(nu.new_users,0)) OVER(ORDER BY y.the_year),0)::numeric * 100, 2) AS growth_new_users_pct,
  ROUND( (COALESCE(tu.total_users,0) - LAG(COALESCE(tu.total_users,0)) OVER(ORDER BY y.the_year))
         / NULLIF(LAG(COALESCE(tu.total_users,0)) OVER(ORDER BY y.the_year),0)::numeric * 100, 2) AS growth_total_users_pct,

  -- Efficiency & loyalty proxies
  ROUND( (COALESCE(sa.trips_booked,0)::numeric / NULLIF(COALESCE(sa.sessions,0),0)) * 100, 2) AS session_to_booking_conv_pct,
  ROUND( (COALESCE(sa.trips_booked,0)::numeric / NULLIF(COALESCE(sa.active_users,0),0)) , 2)  AS trips_per_active_user,
  ROUND( (COALESCE(sa.sessions,0)::numeric     / NULLIF(COALESCE(sa.active_users,0),0)) , 2)  AS sessions_per_active_user,
  ROUND( (COALESCE(rb.repeat_bookers,0)::numeric / NULLIF(COALESCE(rb.bookers_total,0),0)) * 100, 2) AS repeat_booker_share_pct,

  -- Base coverage
  ROUND( (COALESCE(sa.active_users,0)::numeric / NULLIF(COALESCE(tu.total_users,0),0)) * 100, 2) AS active_user_rate_pct,
  ROUND( (COALESCE(rb.bookers_total,0)::numeric / NULLIF(COALESCE(tu.total_users,0),0)) * 100, 2) AS booker_rate_pct,
  ROUND( ((COALESCE(tu.total_users,0) - COALESCE(sa.active_users,0))::numeric / NULLIF(COALESCE(tu.total_users,0),0)) * 100, 2) AS dormant_rate_pct
FROM years y
LEFT JOIN sess_agg          sa ON sa.the_year = y.the_year
LEFT JOIN new_users         nu ON nu.the_year = y.the_year
LEFT JOIN total_users       tu ON tu.the_year = y.the_year
LEFT JOIN repeat_and_bookers rb ON rb.the_year = y.the_year
ORDER BY y.the_year;

