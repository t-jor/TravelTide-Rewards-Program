/*
04_master_version_short.sql
End-to-end pipeline (reduced to relevant features for choosen segmentation, final SELECT at bottom):
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
  
-- 2) Session-level enrichment & cleaning
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
),

  
-- 3) Trip status per trip_id (important as: one session_id for booking [cancellation = 'false'] and another for cancelling [cancellation = 'true'])
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


-- 4) Final session-based table with derived metrics
session_based_final AS (

SELECT
  sb.*,
  -- ensure browsing-only sessions get trip_status = 'none'
  CASE WHEN trip_id IS NULL THEN 'none' ELSE trip_status END AS trip_status,

  -- DOW check-out
  EXTRACT(ISODOW FROM (date(check_in_time) + nights_cleaned)) AS dow_check_out_cleaned,

  -- booking categories
  CASE
    WHEN hotel_booked_cleaned AND NOT flight_booked_cleaned THEN 'hotel only'
    WHEN flight_booked_cleaned AND NOT hotel_booked_cleaned THEN 'flight only'
    WHEN hotel_booked_cleaned AND flight_booked_cleaned THEN 'flight+hotel'
    ELSE 'browsing only'
  END AS booking_category,

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
),

  
-- STEP 2: USER-LEVEL
-- User-level totals and general features
user_based_prep AS (
  SELECT
  user_id,

  -- browsing
  SUM(session_duration_min) AS total_min_sessions,
  SUM(session_duration_booking) AS total_min_booking,
  SUM(session_duration_cancellation) AS total_min_cancellation,
  SUM(session_duration_browsing) AS total_min_browsing,
  SUM(page_clicks) AS total_page_clicks,
  SUM(clicks_booking) AS total_clicks_booking,
  SUM(clicks_cancellation) AS total_clicks_cancellation,
  SUM(clicks_browsing) AS total_clicks_browsing,
  COUNT(session_id) AS total_sessions,
  SUM(CASE WHEN trip_id IS NULL THEN 1 ELSE 0 END) AS total_browsing_only,
  
  -- trips & bookings
  COUNT(DISTINCT trip_id) AS total_bookings, -- cancelled bookings are counted once (cf. 2 sessions: 1 booking, 1 cancellation)
  SUM(CASE WHEN cancellation THEN 1 ELSE 0 END) AS total_cancellations,
  SUM(CASE WHEN flight_booked_cleaned AND hotel_booked_cleaned AND trip_status = 'booked' THEN 1 ELSE 0 END) AS valid_combined_bookings,

  -- flights  
  SUM(CASE WHEN flight_booked_cleaned AND trip_status = 'booked' THEN 1 ELSE 0 END) AS valid_flight_bookings,
  SUM(CASE WHEN flight_booked_cleaned AND trip_status = 'booked' AND booking_category = 'flight only' THEN 1 ELSE 0 END) AS flight_bookings_only, 

  -- hotels
  SUM(CASE WHEN hotel_booked_cleaned AND trip_status = 'booked' THEN 1 ELSE 0 END) AS valid_hotel_bookings,
  SUM(CASE WHEN hotel_booked_cleaned AND trip_status = 'booked' AND booking_category = 'hotel only' THEN 1 ELSE 0 END) AS hotel_bookings_only,  

  -- pricing & discount
  SUM(CASE WHEN flight_discount_cleaned AND flight_booked_cleaned AND trip_status = 'booked' THEN 1 ELSE 0 END) AS discount_flight_bookings,
  ROUND(AVG(CASE WHEN flight_booked_cleaned AND trip_status = 'booked' THEN flight_discount_amount END), 4) AS avg_flight_discount,
  SUM(CASE WHEN flight_booked_cleaned AND trip_status = 'booked' THEN flight_price_per_km - (flight_price_per_km * COALESCE(flight_discount_amount, 0)) END) AS sum_flight_price_per_km,

  SUM(CASE WHEN hotel_discount_cleaned AND hotel_booked_cleaned AND trip_status = 'booked' THEN 1 ELSE 0 END) AS discount_hotel_bookings,
  ROUND(AVG(CASE WHEN hotel_booked_cleaned AND trip_status = 'booked' THEN hotel_discount_amount END), 4) AS avg_hotel_discount, 
  SUM(CASE WHEN hotel_booked_cleaned AND trip_status = 'booked' THEN hotel_per_room_usd - (hotel_per_room_usd * COALESCE(hotel_discount_amount, 0)) END) AS sum_hotel_price_per_night,

  SUM(CASE WHEN (flight_discount_cleaned AND flight_booked_cleaned OR hotel_discount_cleaned AND hotel_booked_cleaned) AND trip_status = 'booked' THEN 1 ELSE 0 END) AS sum_discount_bookings,

  -- seats/bags/rooms
  SUM(CASE WHEN trip_status = 'booked' THEN seats ELSE 0 END) AS valid_seats, -- count only for non-cancelled bookings (for trip_status != 'booked', the value is 0, not NULL)
  SUM(CASE WHEN trip_status = 'booked' THEN checked_bags ELSE 0 END) AS valid_bags,
  SUM(CASE WHEN trip_status = 'booked' THEN rooms ELSE 0 END) AS valid_rooms, 

  -- nights split
  SUM(CASE WHEN trip_status = 'booked' AND booking_category = 'flight+hotel' THEN flight_trip_nights ELSE 0 END) AS nights_combined_flight_hotel, 
  SUM(CASE WHEN trip_status = 'booked' AND flight_booked_cleaned THEN flight_trip_nights ELSE 0 END) AS nights_flight_total,
  SUM(CASE WHEN trip_status = 'booked' AND booking_category = 'hotel only' THEN nights_cleaned ELSE 0 END) AS nights_hotel_only,  
  SUM(CASE WHEN trip_status = 'booked' AND booking_category = 'flight only' THEN flight_trip_nights ELSE 0 END) AS nights_flight_only,
  
  -- trip types
  COUNT(CASE WHEN trip_status = 'booked' AND (is_short_weekday_flight OR is_short_weekday_hotel) THEN 1 END) AS short_weekday_trips,
  COUNT(CASE WHEN trip_status = 'booked' AND (is_weekender_flight OR is_weekender_hotel) THEN 1 END) AS weekend_trips,
  
  -- user info
  MAX(age_years) AS age,
  MAX(CASE WHEN has_children THEN 1 ELSE 0 END) AS has_children,
  MAX(date(session_start)) FILTER (WHERE trip_id IS NOT NULL AND trip_status = 'booked') - MAX(sign_up_date) AS last_booking_since_signup

  FROM session_based_final
  GROUP BY user_id
),

-- User-level averages
user_based_avg AS (

  SELECT
  user_id,

  -- working age
 (MAX(age) BETWEEN 20 AND 67)::int AS is_in_working_age,

  -- browsing metrics
  ROUND(max((total_min_sessions::numeric) / NULLIF(total_sessions, 0)), 4) AS avg_min_per_session,
  ROUND(max((total_min_booking::numeric) / NULLIF(total_bookings - total_cancellations, 0)), 4) AS avg_min_booking,
  ROUND(max((total_min_cancellation::numeric) / NULLIF(total_cancellations, 0)), 4) AS avg_min_cancellation,
  ROUND(max((total_min_browsing::numeric) / NULLIF(total_browsing_only, 0)), 4) AS avg_min_browsing,
  
  ROUND(max((total_page_clicks::numeric) / NULLIF(total_sessions, 0)), 4) AS avg_page_clicks,
  ROUND(max((total_clicks_booking::numeric) / NULLIF(total_bookings - total_cancellations, 0)), 4) AS avg_clicks_booking,
  ROUND(max((total_clicks_cancellation::numeric) / NULLIF(total_cancellations, 0)), 4) AS avg_clicks_cancellation,
  ROUND(max((total_clicks_browsing::numeric) / NULLIF(total_browsing_only, 0)), 4) AS avg_clicks_browsing,

  -- trips & bookings
  MAX(total_bookings - total_cancellations) AS valid_bookings,

  -- pricing & discount
  ROUND(MAX(sum_flight_price_per_km / valid_flight_bookings), 4) AS avg_flight_price_per_km,
  ROUND(MAX(sum_hotel_price_per_night / valid_hotel_bookings), 4) AS avg_hotel_price_per_night,
  
  -- seats/bags/rooms
  ROUND(max((valid_seats::numeric) / NULLIF(valid_flight_bookings, 0)), 4) AS avg_seats,
  ROUND(max((valid_bags::numeric) / NULLIF(valid_flight_bookings, 0)), 4) AS avg_bags,
  ROUND(max((valid_rooms::numeric) / NULLIF(valid_hotel_bookings, 0)), 4) AS avg_rooms,

  -- nights split
  ROUND(max((nights_combined_flight_hotel::numeric) / NULLIF(valid_combined_bookings, 0)), 4) AS avg_nights_combined,
  ROUND(max((nights_flight_total::numeric) / NULLIF(valid_flight_bookings, 0)), 4) AS avg_nights_flight_total,
  ROUND(max((nights_hotel_only::numeric) / NULLIF(hotel_bookings_only, 0)), 4) AS avg_nights_hotel_only,
  ROUND(max((nights_flight_only::numeric) / NULLIF(flight_bookings_only, 0)), 4) AS avg_nights_flight_only,
  ROUND(max(((nights_combined_flight_hotel + nights_flight_only + nights_hotel_only)::numeric) / NULLIF((total_bookings - total_cancellations), 0)), 4) AS avg_nights_total,

  -- trip types
  COALESCE(ROUND(max(short_weekday_trips::numeric / NULLIF((total_bookings - total_cancellations), 0)), 4), 0) weekdays_travel_quote,
  COALESCE(ROUND(max(weekend_trips::numeric / NULLIF((total_bookings - total_cancellations), 0)), 4), 0) weekend_travel_quote

  FROM user_based_prep 
  GROUP BY user_id
),

  
-- STEP 3: FEATURES + SCORING
-- Normalize and define final features
features_norm AS (
  
  SELECT
  user_id,

  -- passthroughs
  has_children,
  is_in_working_age,
  weekdays_travel_quote,
  weekend_travel_quote,  
  
  -- min-max (invert where "lower is better")
  COALESCE(1-ROUND((avg_bags - MIN(avg_bags) OVER()) / (MAX(avg_bags) OVER() - MIN(avg_bags) OVER()), 4), 0) AS avg_bags_norm_invert, 
  COALESCE(1-ROUND((avg_seats - MIN(avg_seats) OVER()) / (MAX(avg_seats) OVER() - MIN(avg_seats) OVER()), 4), 0) AS avg_seats_norm_invert, 
  COALESCE(1-ROUND((avg_nights_total - MIN(avg_nights_total) OVER()) / (MAX(avg_nights_total) OVER() - MIN(avg_nights_total) OVER()), 4), 0) AS avg_nights_total_norm_invert,
  
  -- percentile flags
  CASE WHEN avg_min_booking < (SELECT percentile_cont(0.2) WITHIN GROUP (ORDER BY avg_min_booking)::numeric FROM user_based_avg) THEN 1 ELSE 0 END AS is_quick_booker_p20,
  CASE WHEN avg_min_booking > (SELECT percentile_cont(0.8) WITHIN GROUP (ORDER BY avg_min_booking)::numeric FROM user_based_avg) THEN 1 ELSE 0 END AS is_slow_booker_p80,
  CASE WHEN avg_clicks_booking < (SELECT percentile_cont(0.2) WITHIN GROUP (ORDER BY avg_clicks_booking)::numeric FROM user_based_avg) THEN 1 ELSE 0 END AS is_low_clicks_booker_p20,
  CASE WHEN avg_clicks_booking > (SELECT percentile_cont(0.8) WITHIN GROUP (ORDER BY avg_clicks_booking)::numeric FROM user_based_avg) THEN 1 ELSE 0 END AS is_high_clicks_booker_p80,
  CASE WHEN valid_bookings > (SELECT percentile_disc(0.8) WITHIN GROUP (ORDER BY valid_bookings)::numeric FROM user_based_avg) THEN 1 ELSE 0 END AS is_frequent_traveler_p80,
  CASE WHEN avg_flight_price_per_km > (SELECT percentile_cont(0.8) WITHIN GROUP (ORDER BY avg_flight_price_per_km)::numeric FROM user_based_avg) THEN 1 ELSE 0 END AS is_high_price_flight_p80,
  CASE WHEN avg_flight_price_per_km < (SELECT percentile_cont(0.2) WITHIN GROUP (ORDER BY avg_flight_price_per_km)::numeric FROM user_based_avg) THEN 1 ELSE 0 END AS is_budget_price_flight_p20,
  CASE WHEN avg_hotel_price_per_night > (SELECT percentile_cont(0.8) WITHIN GROUP (ORDER BY avg_hotel_price_per_night)::numeric FROM user_based_avg) THEN 1 ELSE 0 END AS is_high_price_hotel_p80,
  CASE WHEN avg_hotel_price_per_night < (SELECT percentile_cont(0.2) WITHIN GROUP (ORDER BY avg_hotel_price_per_night)::numeric FROM user_based_avg) THEN 1 ELSE 0 END AS is_budget_price_hotel_p20,  

  -- conditions
  CASE WHEN total_sessions = total_browsing_only OR (total_bookings >=1 AND valid_bookings = 0) THEN 1 ELSE 0 END AS is_dreamer_no_bookings, -- no bookings or only cancelled bookings
  CASE WHEN last_booking_since_signup <= 28 THEN 1 ELSE 0 END AS is_new_customer_booking,
  CASE WHEN avg_seats BETWEEN 3 AND 6 THEN 1 ELSE 0 END AS has_middle_num_seats,
  CASE WHEN avg_bags BETWEEN 1 AND 4 THEN 1 ELSE 0 END AS has_middle_num_bags,
  CASE WHEN avg_rooms BETWEEN 1 AND 4 THEN 1 ELSE 0 END AS has_middle_num_rooms,
  CASE WHEN age >= 60 AND valid_bookings >= 1 THEN 1 ELSE 0 END AS is_senior_traveler,
  CASE WHEN age <= 25 AND valid_bookings >= 1 THEN 1 ELSE 0 END AS is_young_traveler,

  -- quotes
  COALESCE(ROUND(sum_discount_bookings::numeric / NULLIF(valid_bookings, 0), 4), 0) AS discount_booking_quote, 
  COALESCE(ROUND(discount_flight_bookings::numeric / NULLIF(valid_flight_bookings, 0), 4), 0) AS discount_flight_quote,
  COALESCE(ROUND(discount_hotel_bookings::numeric / NULLIF(valid_hotel_bookings, 0), 4), 0) AS discount_hotel_quote  
  
  FROM user_based_avg uavg
  JOIN user_based_prep prep
  USING(user_id)
),

  
-- Weight features and calculate scores
user_scores AS (
  SELECT *,

  CASE WHEN is_in_working_age = 0 OR weekdays_travel_quote < 0.5 THEN 0 ELSE avg_bags_norm_invert * 0.2 + avg_seats_norm_invert * 0.2 + weekdays_travel_quote * 0.6 END AS score_business,
  CASE WHEN has_children = 0 THEN 0 ELSE has_middle_num_seats * 0.2 + has_middle_num_rooms * 0.1 + has_middle_num_bags * 0.1 + has_children * 0.6 END AS score_family,
  is_frequent_traveler_p80 AS score_frequent_traveler,
  is_new_customer_booking AS score_new_booking_customer,
  is_dreamer_no_bookings AS score_dreamer,
  is_young_traveler AS score_young_traveler,
  CASE WHEN is_high_price_hotel_p80 > 0.5 OR is_high_price_flight_p80 > 0.5 THEN is_high_price_hotel_p80 * 0.5 + is_high_price_flight_p80 * 0.5 ELSE 0 END AS score_premium_traveler,
  CASE WHEN is_budget_price_hotel_p20 > 0.5 OR is_budget_price_flight_p20 > 0.5 THEN is_budget_price_hotel_p20 * 0.5 + is_budget_price_flight_p20 * 0.5 ELSE 0 END AS score_budget_traveler,
  CASE WHEN discount_booking_quote < 0.5 THEN 0 ELSE is_high_clicks_booker_p80 * 0.2 + is_slow_booker_p80 * 0.2 + discount_booking_quote * 0.6 END AS score_deal_hunter

  FROM features_norm
),


-- Assign the segment with MAX-score per user (step 1) 
user_scores_tall AS (

  SELECT
  user_id,
  segment,
  score

  FROM user_scores u
  CROSS JOIN LATERAL (
    VALUES
      ('Business', score_business),
      ('Family', score_family),
      ('Frequent Traveler', score_frequent_traveler),
      ('New Customer', score_new_booking_customer),
      ('Dreamer', score_dreamer),
      ('Young Traveler', score_young_traveler), 
      ('Premium Traveler', score_premium_traveler),
      ('Budget Traveler', score_budget_traveler),
      ('Deal Hunter', score_deal_hunter)
    ) AS temp_table(segment, score)
),

  
-- Assign the segment with MAX-score per user (step 2) 
assigned AS (
  SELECT DISTINCT ON (user_id)
  user_id,
  segment AS assigned_segment, 
  score

  FROM user_scores_tall
  ORDER BY user_id, score DESC, segment ASC
),

  
-- Assign 'Others' segment to users with MAX-score < 0.3
assigned_cleaned AS (
  SELECT
  user_id,
  CASE WHEN score < 0.3 THEN 'Others' ELSE assigned_segment END AS final_segment,
  score
  FROM assigned
)

--/*
  
-- FINAL OUTPUT
-- A) Segment summary + perk mapping
SELECT
  final_segment,
  COUNT(*) AS users,
  ROUND(COUNT(*)::numeric / SUM(COUNT(*)) OVER (), 2) AS share_perc,
  ROUND(((COUNT(*) FILTER (WHERE score >= 0.5))::numeric / COUNT(*)), 2) AS good_match_perc,

  CASE 
      WHEN final_segment = 'Business' THEN 'Free Cancellation'
      WHEN final_segment = 'Family' THEN 'Free Hotel Meal'
      WHEN final_segment = 'Frequent Traveler' THEN 'Free Cancellation & Lounge'
      WHEN final_segment = 'New Customer' THEN 'Free Hotel Night With Flight'
      WHEN final_segment = 'Dreamer' THEN 'Welcome Discount & 48h Free Cancellation'
      WHEN final_segment = 'Young Traveler' THEN 'Welcome Drink & Free Late Check-Out'
      WHEN final_segment = 'Premium Traveler' THEN 'Priority Check-In & Lounge'
      WHEN final_segment = 'Budget Traveler' THEN 'Free Checked-Bag'
      WHEN final_segment = 'Deal Hunter' THEN 'Special Deal - Discount Offers'
      WHEN final_segment = 'Others' THEN 'Baseline Discount - TBD'
      ELSE 'NULL'
      END AS perk
  
  FROM assigned_cleaned
  GROUP BY final_segment
  ORDER BY users DESC, final_segment
  ;
  