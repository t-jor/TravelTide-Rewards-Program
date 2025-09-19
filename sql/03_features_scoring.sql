/*
03_features_scoring.sql
- Normalize features (min-max; invert where "lower is better")
- Percentile-based flags (p20/p80)
- Quotes for discount/destination
- Weighted segment scores
- Assign best-fit segment per user
- Perk mapping summary
*/


-- STEP 3: FEATURES + SCORING
-- Normalize and define final features
WITH
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
  CASE WHEN avg_leadtime_combined < (SELECT percentile_cont(0.2) WITHIN GROUP (ORDER BY avg_leadtime_combined)::numeric FROM user_based_avg) THEN 1 ELSE 0 END AS is_short_leadtime_p20,
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
  COALESCE(ROUND(discount_hotel_bookings::numeric / NULLIF(valid_hotel_bookings, 0), 4), 0) AS discount_hotel_quote,  
  COALESCE(ROUND(domestic_flight_bookings::numeric / NULLIF(valid_flight_bookings, 0), 4), 0) AS domestic_flight_quote,
  COALESCE(ROUND(international_flight_bookings::numeric / NULLIF(valid_flight_bookings, 0), 4), 0) AS international_flight_quote,
  COALESCE(ROUND(intercontinental_flight_bookings::numeric / NULLIF(valid_flight_bookings, 0), 4), 0) AS intercontinental_flight_quote
  
  FROM user_based_avg uavg
  JOIN user_based_prep prep
  USING(user_id)
),

  
-- Weight features and calculate scores
user_scores AS (
  SELECT *,

  CASE WHEN is_in_working_age = 0 OR weekdays_travel_quote < 0.5 THEN 0 ELSE avg_bags_norm_invert * 0.2 + avg_seats_norm_invert * 0.2 + weekdays_travel_quote * 0.6 END AS score_business,
  CASE WHEN has_children = 0 THEN 0 ELSE has_middle_num_seats * 0.2 + has_middle_num_rooms * 0.1 + has_middle_num_bags * 0.1 + has_children * 0.6 END AS score_family,
  --CASE WHEN weekend_travel_quote < 0.5 THEN 0 ELSE weekend_travel_quote END AS score_weekender,
  is_frequent_traveler_p80 AS score_frequent_traveler,
  is_new_customer_booking AS score_new_booking_customer,
  is_dreamer_no_bookings AS score_dreamer,
  --is_senior_traveler AS score_senior_traveler,
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
      --('Weekender', score_weekender),
      ('Frequent Traveler', score_frequent_traveler),
      ('New Customer', score_new_booking_customer),
      ('Dreamer', score_dreamer),
      --('Senior Traveler', score_senior_traveler),
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
      --WHEN final_segment = 'Senior Traveler' THEN 'Free Hotel Meal'
      WHEN final_segment = 'Young Traveler' THEN 'Welcome Drink & Free Late Check-Out'
      WHEN final_segment = 'Premium Traveler' THEN 'Priority Check-In & Lounge'
      WHEN final_segment = 'Budget Traveler' THEN 'Free Checked-Bag'
      WHEN final_segment = 'Deal Hunter' THEN 'Special Deal - Discount Offers'
      --WHEN final_segment = 'Weekender' THEN 'Welcome Drink & Free Late Check-Out'
      WHEN final_segment = 'Others' THEN 'Baseline Discount - TBD'
      ELSE 'NULL'
      END AS perk
  
  FROM assigned_cleaned
  GROUP BY final_segment
  ORDER BY users DESC, final_segment
  ;

  -- needs to be connected to user_based_avg (02_user_base.sql) 
  