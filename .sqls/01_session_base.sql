/*
01_session_base.sql
Session-based foundation:
- Cohort (starting 2023-01-05; active users with >7 sessions)
- Join sessions + flights + hotels + users
- Cleaning: nights/discounts/booked
- Trip status ('booked'/'cancelled'/'none')
- Derived session features (clicks_per_min, age_group, distance, categories)
*/


-- STEP 1: SESSION-LEVEL
-- 1) Cohort filter
WITH 
filtered_sessions AS (
  SELECT *
  FROM sessions
  WHERE session_start >= '2023-01-04'
),
active_users AS (
  SELECT user_id
  FROM filtered_sessions
  GROUP BY user_id
  HAVING COUNT(*) > 7
),

  
-- 2) Destination-country mapping (verified via airport coordinates)
mapping(destination, destination_country) AS (
  VALUES
      ('buenos aires', 'argentina'),
      ('agra', 'india'),
      ('auckland', 'new zealand'),
      ('amsterdam', 'netherlands'),
      ('atlanta', 'usa'),
      ('abu dhabi', 'united arab emirates'),
      ('austin', 'usa'),
      ('antalya', 'turkey'),
      ('barcelona', 'spain'),
      ('seattle', 'usa'),
      ('el paso', 'usa'),
      ('bangkok', 'thailand'),
      ('bangalore', 'india'),
      ('nashville', 'usa'),
      ('boston', 'usa'),
      ('brussels', 'belgium'),
      ('budapest', 'hungary'),
      ('baltimore', 'usa'),
      ('sydney', 'canada'),
      ('cairo', 'egypt'),
      ('guangzhou', 'china'),
      ('paris', 'france'),
      ('charlotte', 'usa'),
      ('colombo', 'sri lanka'),
      ('columbus', 'usa'),
      ('casablanca', 'morocco'),
      ('cape town', 'south africa'),
      ('chengdu', 'china'),
      ('dallas', 'usa'),
      ('washington', 'usa'),
      ('delhi', 'india'),
      ('denver', 'usa'),
      ('detroit', 'usa'),
      ('dalian', 'china'),
      ('tucson', 'usa'),
      ('denpasar', 'indonesia'),
      ('dublin', 'ireland'),
      ('durban', 'south africa'),
      ('dubai', 'united arab emirates'),
      ('edinburgh', 'united kingdom'),
      ('houston', 'usa'),
      ('fresno', 'usa'),
      ('rome', 'usa'),
      ('florence', 'usa'),
      ('fort worth', 'usa'),
      ('fukuoka', 'japan'),
      ('rio de janeiro', 'brazil'),
      ('seoul', 'south korea'),
      ('geneva', 'switzerland'),
      ('hamburg', 'germany'),
      ('hanoi', 'vietnam'),
      ('heraklion', 'greece'),
      ('hong kong', 'china'),
      ('phuket', 'thailand'),
      ('johannesburg', 'south africa'),
      ('jakarta', 'indonesia'),
      ('hamilton', 'canada'),
      ('tokyo', 'japan'),
      ('honolulu', 'usa'),
      ('hurghada', 'egypt'),
      ('indianapolis', 'usa'),
      ('istanbul', 'turkey'),
      ('osaka', 'japan'),
      ('jaipur', 'india'),
      ('jacksonville', 'usa'),
      ('new york', 'usa'),
      ('johor bahru', 'malaysia'),
      ('jerusalem', 'israel'),
      ('kuala lumpur', 'malaysia'),
      ('guilin', 'china'),
      ('las vegas', 'usa'),
      ('los angeles', 'usa'),
      ('london', 'canada'),
      ('milan', 'italy'),
      ('lisbon', 'portugal'),
      ('lagos', 'nigeria'),
      ('louisville', 'usa'),
      ('phoenix', 'usa'),
      ('madrid', 'spain'),
      ('orlando', 'usa'),
      ('chicago', 'usa'),
      ('melbourne', 'usa'),
      ('memphis', 'usa'),
      ('mexico city', 'mexico'),
      ('macau', 'china'),
      ('miami', 'usa'),
      ('milwaukee', 'usa'),
      ('manila', 'philippines'),
      ('munich', 'germany'),
      ('naples', 'italy'),
      ('nice', 'france'),
      ('san diego', 'usa'),
      ('oklahoma city', 'usa'),
      ('portland', 'usa'),
      ('beijing', 'china'),
      ('philadelphia', 'usa'),
      ('pune', 'india'),
      ('prague', 'czechia'),
      ('punta cana', 'dominican republic'),
      ('copenhagen', 'denmark'),
      ('san antonio', 'usa'),
      ('riyadh', 'saudi arabia'),
      ('san francisco', 'usa'),
      ('ho chi minh city', 'vietnam'),
      ('shanghai', 'china'),
      ('singapore', 'singapore'),
      ('san jose', 'usa'),
      ('moscow', 'russia'),
      ('berlin', 'germany'),
      ('shenzhen', 'china'),
      ('qingdao', 'china'),
      ('taipei', 'taiwan'),
      ('venice', 'italy'),
      ('vienna', 'austria'),
      ('warsaw', 'poland'),
      ('xi''an', 'china'),
      ('winnipeg', 'canada'),
      ('edmonton', 'canada'),
      ('montreal', 'canada'),
      ('toronto', 'canada'),
      ('ottawa', 'canada'),
      ('quebec', 'canada'),
      ('vancouver', 'canada'),
      ('calgary', 'canada')
),

  
-- 3) Session-level enrichment & cleaning
session_based_prep AS (
  SELECT
  *,

  -- nights cleaning
  CASE 
    WHEN nights < 0 AND return_time IS NULL THEN nights * -1
    WHEN nights < 0 AND return_time IS NOT NULL THEN date(return_time) - date(check_in_time)
    WHEN nights = 0 AND return_time IS NULL THEN nights + 1 --Annahme: es gibt keine "Day use" Zimmer, sondern immer mindestens eine Nacht
    WHEN nights = 0 AND return_time IS NOT NULL THEN date(return_time) - date(check_in_time)
    ELSE date(check_out_time) - date(check_in_time) 
  END AS nights_cleaned,

  -- discounts cleaning
  CASE WHEN flight_discount IS TRUE AND flight_discount_amount IS NULL THEN FALSE
      ELSE COALESCE(flight_discount, FALSE) END AS flight_discount_cleaned,
  CASE WHEN hotel_discount IS TRUE AND hotel_discount_amount IS NULL THEN false
      ELSE COALESCE(hotel_discount, FALSE) END AS hotel_discount_cleaned,  

  -- booked flags cleaning
  CASE WHEN flight_booked IS TRUE AND trip_airline IS NULL THEN false
      ELSE COALESCE(flight_booked, FALSE) END AS flight_booked_cleaned,
  CASE WHEN hotel_booked IS TRUE AND hotel_name IS NULL THEN false
      ELSE COALESCE(hotel_booked, FALSE) END AS hotel_booked_cleaned,

  -- hotel city (suffix after '-')
  CASE WHEN hotel_name ~ '-' THEN TRIM( REGEXP_REPLACE(hotel_name, '^.*-\s*', '') ) ELSE NULL END AS hotel_city,

  -- session duration (min)
  ROUND(EXTRACT(EPOCH FROM (session_end - session_start)) / 60.0, 2) AS session_duration_min,

  -- DOWs + flight trip length
  EXTRACT(ISODOW FROM departure_time) AS departure_dow,
  EXTRACT(ISODOW FROM return_time) AS return_dow,
  GREATEST(date(return_time) - date(departure_time), 1) AS flight_trip_length,
  EXTRACT(ISODOW FROM check_in_time) AS check_in_dow,

  -- age as of 2023-12-31 (dataset year)
  DATE_PART('year', AGE(DATE '2023-12-31', birthdate)) AS age_years,

  -- flight distance km (one-way ; origin=home in dataset)
  CASE
    WHEN origin_airport = home_airport
        AND home_airport_lat IS NOT NULL AND home_airport_lon IS NOT NULL
        AND destination_airport_lat IS NOT NULL AND destination_airport_lon IS NOT NULL
    THEN haversine_distance(
          home_airport_lat, home_airport_lon,
          destination_airport_lat, destination_airport_lon
             )
    ELSE NULL
  END AS flight_distance_km

  FROM filtered_sessions s
  JOIN active_users au     USING (user_id)
  JOIN users u             USING (user_id)
  LEFT JOIN flights f      USING (trip_id) -- USING keeps a single trip_id
  LEFT JOIN hotels h       USING (trip_id)
  LEFT JOIN mapping m      USING (destination)
),

  
-- 4) Trip status per trip_id (important as: one session_id for booking [cancellation = 'false'] and another for cancelling [cancellation = 'true'])
trip_status AS (
  SELECT
  trip_id,
  CASE
    WHEN BOOL_OR(cancellation) THEN 'cancelled'
    WHEN BOOL_OR(flight_booked_cleaned OR hotel_booked_cleaned) THEN 'booked'
    ELSE 'none'
  END AS trip_status
  
  FROM session_based_prep
  WHERE trip_id IS NOT NULL
  GROUP BY trip_id
),

  
-- 5) Final session-based table with derived metrics
session_based_final AS (

SELECT
  sb.*,
  -- ensure browsing-only sessions get trip_status = 'none'
  CASE WHEN trip_id IS NULL THEN 'none' ELSE trip_status END AS trip_status,

  -- DOW check-out
  EXTRACT(ISODOW FROM (date(check_in_time) + nights_cleaned)) AS dow_check_out_cleaned,

  -- age groups
  CASE
    WHEN birthdate IS NULL OR sb.age_years IS NULL THEN 'unknown'
    WHEN age_years < 26 THEN '18-25'
    WHEN age_years < 36 THEN '26-35'
    WHEN age_years < 46 THEN '36-45'
    WHEN age_years < 56 THEN '46-55'
    ELSE '55+'
  END AS age_group,

  -- booking categories
  CASE
    WHEN hotel_booked_cleaned AND NOT flight_booked_cleaned THEN 'hotel only'
    WHEN flight_booked_cleaned AND NOT hotel_booked_cleaned THEN 'flight only'
    WHEN hotel_booked_cleaned AND flight_booked_cleaned THEN 'flight+hotel'
    ELSE 'browsing only'
  END AS booking_category,

  -- browsing metrics
  ROUND(page_clicks / NULLIF(sb.session_duration_min, 0), 2) AS clicks_per_min,
  
  -- metrics by booking category
  CASE WHEN hotel_booked_cleaned AND NOT flight_booked_cleaned THEN page_clicks END AS clicks_hotel_only,
  CASE WHEN flight_booked_cleaned AND NOT hotel_booked_cleaned THEN page_clicks END AS clicks_flight_only,
  CASE WHEN hotel_booked_cleaned AND flight_booked_cleaned THEN page_clicks END AS clicks_flight_hotel,
  CASE WHEN NOT hotel_booked_cleaned AND NOT flight_booked_cleaned THEN page_clicks END AS clicks_browsing_only,

  -- metrics by trip_status
  CASE WHEN trip_status ='booked' THEN page_clicks END AS clicks_booking,
  CASE WHEN trip_status = 'cancelled' THEN page_clicks END AS clicks_cancellation,
  CASE WHEN NOT hotel_booked_cleaned AND NOT flight_booked_cleaned THEN page_clicks END AS clicks_browsing,

  CASE WHEN trip_status ='booked' THEN session_duration_min END AS session_duration_booking,
  CASE WHEN trip_status = 'cancelled' THEN session_duration_min END AS session_duration_cancellation,
  CASE WHEN NOT hotel_booked_cleaned AND NOT flight_booked_cleaned THEN session_duration_min END AS session_duration_browsing,
  
  -- lead times
  CASE WHEN trip_status = 'booked' AND hotel_booked_cleaned AND flight_booked_cleaned THEN date(departure_time) - date(session_start) END AS leadtime_flight_hotel_combined,
  CASE WHEN trip_status = 'booked' AND hotel_booked_cleaned AND NOT flight_booked_cleaned THEN date(check_in_time) - date(session_start) END AS leadtime_hotel_only,
  CASE WHEN trip_status = 'booked' AND NOT hotel_booked_cleaned AND flight_booked_cleaned THEN date(departure_time) - date(session_start) END AS leadtime_flight_only,
  
  -- stay length category (hotel nights)
  CASE
    WHEN hotel_booked_cleaned IS FALSE THEN NULL
    WHEN nights_cleaned <= 3 THEN 'short (1-3)'
    WHEN nights_cleaned <= 7 THEN 'mid (4-7)'
    ELSE 'long (8+)'
  END AS stay_length,

  -- flight trip nights/days (if return booked)
  CASE WHEN NOT flight_booked_cleaned OR NOT return_flight_booked THEN NULL
      ELSE date(return_time) - date(departure_time) END AS flight_trip_nights,
  CASE WHEN NOT flight_booked_cleaned OR NOT return_flight_booked THEN NULL
      ELSE (date(return_time) - date(departure_time) + 1) END AS flight_trip_days,

  -- weekender / short weekday flags (flight & hotel)
  ( flight_booked_cleaned AND return_flight_booked
    AND departure_dow IN (5,6)
    AND date(return_time) - date(departure_time) BETWEEN 1 AND 2
  ) AS is_weekender_flight,
  
  ( hotel_booked_cleaned 
    AND check_in_dow IN (5,6)
    AND nights_cleaned BETWEEN 1 AND 2
  ) AS is_weekender_hotel,

  ( flight_booked_cleaned AND return_flight_booked
    AND date(return_time) - date(departure_time) <= 4  
    AND departure_dow NOT IN (6,7)
    AND return_dow NOT IN (6,7)
    AND return_dow > departure_dow
  ) AS is_short_weekday_flight,
  
  ( hotel_booked_cleaned 
    AND nights_cleaned BETWEEN 1 AND 4  
    AND check_in_dow NOT IN (6,7)
    AND check_in_dow + nights_cleaned NOT IN (6,7)
  ) AS is_short_weekday_hotel,
  
  -- seasons
  CASE WHEN hotel_booked_cleaned IS TRUE THEN
      CASE EXTRACT(MONTH FROM check_in_time)
        WHEN 12 THEN 'winter' WHEN 1 THEN 'winter' WHEN 2 THEN 'winter'
        WHEN 3  THEN 'spring' WHEN 4 THEN 'spring' WHEN 5 THEN 'spring'
        WHEN 6  THEN 'summer' WHEN 7 THEN 'summer' WHEN 8 THEN 'summer'
        WHEN 9  THEN 'fall'   WHEN 10 THEN 'fall'  WHEN 11 THEN 'fall'
      END
  ELSE NULL END AS season_hotel,

  CASE WHEN flight_booked_cleaned IS TRUE THEN
      CASE EXTRACT(MONTH FROM departure_time)
        WHEN 12 THEN 'winter' WHEN 1 THEN 'winter' WHEN 2 THEN 'winter'
        WHEN 3  THEN 'spring' WHEN 4 THEN 'spring' WHEN 5 THEN 'spring'
        WHEN 6  THEN 'summer' WHEN 7 THEN 'summer' WHEN 8 THEN 'summer'
        WHEN 9  THEN 'fall'   WHEN 10 THEN 'fall'  WHEN 11 THEN 'fall'
      END
  ELSE NULL END AS season_flight,

  -- flight distance category
  CASE
    WHEN flight_distance_km IS NULL THEN NULL
    WHEN flight_distance_km <= 1500 THEN 'short-haul'
    WHEN flight_distance_km <= 3500 THEN 'medium-haul'
    ELSE 'long-haul'
  END AS flight_distance_category,

  -- destination category (origin=home IN('usa', 'canada') in dataset)
  CASE
    WHEN flight_distance_km IS NULL THEN NULL
    WHEN home_country = destination_country THEN 'domestic'
    WHEN home_country != destination_country AND destination_country IN('usa', 'canada', 'mexico') THEN 'international'
    ELSE 'intercontinental'
  END AS flight_destination_category,

  -- pricing
  CASE
    WHEN flight_booked_cleaned IS TRUE
        AND base_fare_usd IS NOT NULL
        AND seats IS NOT NULL AND seats > 0
    THEN ROUND(base_fare_usd / seats::numeric, 2) ELSE NULL
  END AS flight_price_per_seat,

  CASE
    WHEN flight_booked_cleaned IS TRUE
        AND base_fare_usd IS NOT NULL AND seats IS NOT NULL AND flight_distance_km IS NOT NULL
        AND flight_distance_km > 0 AND seats > 0
        AND return_flight_booked IS FALSE
    THEN ROUND((base_fare_usd / seats::numeric) / flight_distance_km::numeric, 4)
    WHEN flight_booked_cleaned IS TRUE
        AND base_fare_usd IS NOT NULL AND seats IS NOT NULL AND flight_distance_km IS NOT NULL
        AND flight_distance_km > 0 AND seats > 0
        AND return_flight_booked
    THEN ROUND((base_fare_usd / seats::numeric) / (flight_distance_km::numeric *2), 4) 
    ELSE NULL
  END AS flight_price_per_km
  
  FROM session_based_prep sb
  LEFT JOIN trip_status ts USING(trip_id)
)

SELECT * FROM session_based_final;