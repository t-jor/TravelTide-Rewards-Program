-- YTD KPIs + YoY growth (with optional sessions), dynamic cutoff
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
    EXTRACT(YEAR FROM s.session_start)::int AS ytd_year,
    COUNT(*)                                 AS sessions,
    COUNT(DISTINCT s.user_id)                AS active_users,
    COUNT(DISTINCT s.trip_id)                AS trips_booked
  FROM sessions s
  CROSS JOIN cutoff c
  WHERE s.session_start::date <= MAKE_DATE(EXTRACT(YEAR FROM s.session_start)::int, c.m, c.d)
  GROUP BY 1
),

-- New users YTD
new_users_ytd AS (
  SELECT
    EXTRACT(YEAR FROM u.sign_up_date)::int AS ytd_year,
    COUNT(DISTINCT u.user_id)              AS new_users
  FROM users u
  CROSS JOIN cutoff c
  WHERE u.sign_up_date::date <= MAKE_DATE(EXTRACT(YEAR FROM u.sign_up_date)::int, c.m, c.d)
  GROUP BY 1
),

-- Total registered users cumulative up to the YTD cutoff
years_ytd AS (
  SELECT ytd_year FROM sess_agg_ytd
  UNION
  SELECT ytd_year FROM new_users_ytd
),
total_users_ytd AS (
  SELECT
    y.ytd_year,
    COUNT(*) AS total_users
  FROM years_ytd y
  CROSS JOIN cutoff c
  JOIN users u
    ON u.sign_up_date::date <= MAKE_DATE(y.ytd_year, c.m, c.d)
  GROUP BY y.ytd_year
)

SELECT
  y.ytd_year,
  -- Base counts
  COALESCE(tu.total_users,0)  AS total_users,
  COALESCE(nu.new_users,0)    AS new_users,
  COALESCE(sa.active_users,0) AS active_users,
  COALESCE(sa.trips_booked,0) AS trips_booked,
  COALESCE(sa.sessions,0)     AS sessions,        -- optional

  -- Efficiency
  ROUND((COALESCE(sa.trips_booked,0)::numeric
        / NULLIF(COALESCE(sa.active_users,0),0)), 2) AS trips_per_active_user,

  -- YoY growth (% within YTD)
  ROUND((COALESCE(tu.total_users,0) - LAG(COALESCE(tu.total_users,0)) OVER(ORDER BY y.ytd_year))
        / NULLIF(LAG(COALESCE(tu.total_users,0)) OVER(ORDER BY y.ytd_year),0)::numeric * 100, 2) AS growth_total_users_pct,
  ROUND((COALESCE(nu.new_users,0) - LAG(COALESCE(nu.new_users,0)) OVER(ORDER BY y.ytd_year))
        / NULLIF(LAG(COALESCE(nu.new_users,0)) OVER(ORDER BY y.ytd_year),0)::numeric * 100, 2)    AS growth_new_users_pct,
  ROUND((COALESCE(sa.active_users,0) - LAG(COALESCE(sa.active_users,0)) OVER(ORDER BY y.ytd_year))
        / NULLIF(LAG(COALESCE(sa.active_users,0)) OVER(ORDER BY y.ytd_year),0)::numeric * 100, 2) AS growth_active_users_pct,
  ROUND((COALESCE(sa.trips_booked,0) - LAG(COALESCE(sa.trips_booked,0)) OVER(ORDER BY y.ytd_year))
        / NULLIF(LAG(COALESCE(sa.trips_booked,0)) OVER(ORDER BY y.ytd_year),0)::numeric * 100, 2) AS growth_trips_pct,
  ROUND((COALESCE(sa.sessions,0) - LAG(COALESCE(sa.sessions,0)) OVER(ORDER BY y.ytd_year))
        / NULLIF(LAG(COALESCE(sa.sessions,0)) OVER(ORDER BY y.ytd_year),0)::numeric * 100, 2)     AS growth_sessions_pct -- optional

FROM years_ytd y
LEFT JOIN sess_agg_ytd    sa ON sa.ytd_year = y.ytd_year
LEFT JOIN new_users_ytd   nu ON nu.ytd_year = y.ytd_year
LEFT JOIN total_users_ytd tu ON tu.ytd_year = y.ytd_year
ORDER BY y.ytd_year;
