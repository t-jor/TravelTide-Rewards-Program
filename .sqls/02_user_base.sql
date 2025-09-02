/*
02_user_base.sql
Aggregate session-level → user-level:
- Browsing metrics
- Bookings/cancellations & categories
- Seats/bags/rooms/nights
- Prices & discounts
- Lead times
- Per-user averages/quotes
*/

-- STEP 2: USER-LEVEL
-- User-level totals and general features
WITH
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
  COUNT(*) FILTER (WHERE trip_status = 'booked' AND flight_booked_cleaned AND flight_destination_category = 'domestic') AS domestic_flight_bookings,
  COUNT(*) FILTER (WHERE trip_status = 'booked' AND flight_booked_cleaned AND flight_destination_category = 'international') AS international_flight_bookings,
  COUNT(*) FILTER (WHERE trip_status = 'booked' AND flight_booked_cleaned AND flight_destination_category = 'intercontinental') AS intercontinental_flight_bookings,

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
  COUNT(CASE WHEN seats = 1 AND trip_status = 'booked' THEN 1 END) AS single_seat_bookings,
  SUM(CASE WHEN trip_status = 'booked' THEN checked_bags ELSE 0 END) AS valid_bags,
  COUNT(CASE WHEN checked_bags = 0 AND trip_status = 'booked' THEN 1 END) AS no_bag_bookings,
  SUM(CASE WHEN trip_status = 'booked' THEN rooms ELSE 0 END) AS valid_rooms, 
  COUNT(CASE WHEN trip_status = 'booked' AND rooms = 1 THEN 1 END) AS one_room_bookings, 
  COUNT(CASE WHEN trip_status = 'booked' AND flight_booked_cleaned AND hotel_booked_cleaned AND rooms = 1 AND  seats = 1 THEN 1 END) AS single_room_bookings,
  COUNT(CASE WHEN trip_status = 'booked' AND flight_booked_cleaned AND hotel_booked_cleaned AND rooms = 1 AND seats = 2 THEN 1 END) AS double_room_bookings,

  -- nights split
  SUM(CASE WHEN trip_status = 'booked' AND booking_category = 'flight+hotel' THEN flight_trip_nights ELSE 0 END) AS nights_combined_flight_hotel, 
  SUM(CASE WHEN trip_status = 'booked' AND flight_booked_cleaned THEN flight_trip_nights ELSE 0 END) AS nights_flight_total,
  SUM(CASE WHEN trip_status = 'booked' AND booking_category = 'hotel only' THEN nights_cleaned ELSE 0 END) AS nights_hotel_only,  
  SUM(CASE WHEN trip_status = 'booked' AND booking_category = 'flight only' THEN flight_trip_nights ELSE 0 END) AS nights_flight_only,
  
  -- trip types
  COUNT(CASE WHEN trip_status = 'booked' AND (is_short_weekday_flight OR is_short_weekday_hotel) THEN 1 END) AS short_weekday_trips,
  COUNT(CASE WHEN trip_status = 'booked' AND (is_weekender_flight OR is_weekender_hotel) THEN 1 END) AS weekend_trips,

  -- lead times
  SUM(leadtime_flight_hotel_combined) AS sum_leadtime_flight_hotel,
  SUM(leadtime_hotel_only) AS sum_leadtime_hotel_only, 
  SUM(leadtime_flight_only) AS sum_leadtime_flight_only,
  
  -- user info
  MAX(age_years) AS age,
  MAX(CASE WHEN married THEN 1 ELSE 0 END) AS is_married,
  MAX(CASE WHEN has_children THEN 1 ELSE 0 END) AS has_children,
  MAX(date(session_start)) FILTER (WHERE trip_id IS NOT NULL AND trip_status = 'booked') - MAX(sign_up_date) AS last_booking_since_signup,
  MAX(date(session_start)) - MAX(sign_up_date) AS last_session_since_signup


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
  COALESCE(ROUND(max(weekend_trips::numeric / NULLIF((total_bookings - total_cancellations), 0)), 4), 0) weekend_travel_quote,
 
  -- lead times
  ROUND(max((sum_leadtime_flight_hotel::numeric) / NULLIF(valid_combined_bookings, 0)), 4) AS avg_leadtime_combined,
  ROUND(max((sum_leadtime_hotel_only::numeric) / NULLIF(hotel_bookings_only, 0)), 4) AS avg_leadtime_hotel_only,
  ROUND(max((sum_leadtime_flight_only::numeric) / NULLIF(flight_bookings_only, 0)), 4) AS avg_leadtime_flight_only

  FROM user_based_prep 
  GROUP BY user_id
)

SELECT * FROM user_based_avg;

-- needs to be connected to session_based_final (01_session_base.sql) 