# TravelTide — Technical README (Segmentation Pipeline)

This document explains how the SQL-based pipeline segments TravelTide users into meaningful customer groups.  
It complements the `00_master_version_complete.sql` file, which contains the full end-to-end query.

---

## 1. Data Preparation

We join session, user, flight, and hotel data into a **session-based table**.  
Filters applied:  

- Only sessions from `2023-01-04` onwards  
- Users with at least 7 sessions  

```sql
-- Cohort filter
WITH filtered_sessions AS (
  SELECT *
  FROM sessions
  WHERE session_start >= '2023-01-04'
),
active_users AS (
  SELECT user_id
  FROM filtered_sessions
  GROUP BY user_id
  HAVING COUNT(*) > 7
)
```

---

## 2. Feature Engineering

From raw attributes, we derive clean and normalized metrics.  
Key transformations include:  

- Cleaning of negative/zero nights  
- Fixing discount and booking flags  
- Deriving trip status (`booked`, `cancelled`, `none`)  
- Calculating session duration, lead times, flight distances  
- Categorizing age, stay length, destination type  

```sql
-- Example: Nights cleaning
CASE 
  WHEN nights < 0 AND return_time IS NULL THEN nights * -1
  WHEN nights = 0 AND return_time IS NULL THEN nights + 1
  ELSE date(check_out_time) - date(check_in_time)
END AS nights_cleaned
```

These enrichments build the `session_based_final` CTE, which is the cleaned session-level dataset.

---

## 3. User-Level Aggregations

We aggregate session features into **user-level metrics**:  

- Total bookings, cancellations, discounts  
- Average minutes/clicks per session  
- Pricing metrics: flight cost per km, hotel cost per night  
- Seats, bags, rooms per booking  
- Night counts and travel frequency (weekend vs. weekday)  

```sql
-- Example: average minutes per session
ROUND(SUM(session_duration_min)::numeric / NULLIF(COUNT(session_id), 0), 4) AS avg_min_per_session
```

The result is stored in `user_based_avg`, which provides normalized per-user features.

---

## 4. Features & Normalization

We create binary flags and normalized values for segmentation:  

- **Percentile flags**: quick/slow bookers, low/high clickers, short lead times  
- **Price sensitivity**: p20/p80 of flight/hotel prices  
- **Condition flags**: dreamers (only cancelled/no bookings), new customers, young/senior travelers  

```sql
-- Example: quick booker flag
CASE 
  WHEN avg_min_booking < (
    SELECT percentile_cont(0.2) WITHIN GROUP (ORDER BY avg_min_booking)
    FROM user_based_avg
  ) THEN 1 ELSE 0 
END AS is_quick_booker_p20
```

---

## 5. Scoring & Segmentation

We compute weighted scores for each segment.  
Each user receives a score per segment, then is assigned to the one with the highest score.

```sql
-- Business score
CASE 
  WHEN is_in_working_age = 0 OR weekdays_travel_quote < 0.5 THEN 0
  ELSE avg_bags_norm_invert * 0.2 
     + avg_seats_norm_invert * 0.2 
     + weekdays_travel_quote * 0.6 
END AS score_business
```

If all scores are below `0.3`, the user is assigned to **Others**.

Segments include:  
Business, Family, Frequent Traveler, Premium, Budget, Deal Hunter, Dreamer, New, Young, Others.

---

## 6. Final Output

The final query summarizes segment sizes, shares, and perk mapping.  

```sql
SELECT
  final_segment,
  COUNT(*) AS users,
  ROUND(COUNT(*)::numeric / SUM(COUNT(*)) OVER (), 2) AS share_perc,
  CASE 
    WHEN final_segment = 'Business' THEN 'Free Cancellation'
    WHEN final_segment = 'Family' THEN 'Free Hotel Meal'
    WHEN final_segment = 'Frequent Traveler' THEN 'Free Cancellation & Lounge'
    WHEN final_segment = 'Premium Traveler' THEN 'Priority Check-In & Lounge'
    -- etc. for other segments
    ELSE 'Baseline Discount - TBD'
  END AS perk
FROM assigned_cleaned
GROUP BY final_segment
ORDER BY users DESC;
```

This output is the **segment-perk summary** used in the final report and executive summary.

---

## 7. Validation & Debugging

Optional queries in the SQL file allow inspection of intermediate results:  

- Per-user assignment (`assigned`)  
- Feature distributions (`features_norm`)  
- User-level averages (`user_based_avg`)  
- Session-level cleaning (`session_based_final`)  

```sql
-- Inspect per-user assignment
SELECT * 
FROM assigned
ORDER BY score DESC;
```

---

## 8. Reproducibility Notes

- Pipeline is **idempotent**: reruns always produce the same results.  
- No temp tables, no writes → safe for read-only DBs.  
- All `JOIN`s on `trip_id` use `USING(trip_id)` to avoid duplicates.  
- Percentiles are cohort-based → relative to current dataset.  
- Tie-breaks in equal scores are resolved alphabetically.  

---

## 9. Tuning Guide

Parameters that can be adjusted:  

- **Cohort date** → change in `filtered_sessions`  
- **Active user threshold** → default = 7 sessions  
- **Percentile cut-offs** → adjust in `features_norm`  
- **Segment weights** → formulas in `user_scores`  
- **Others threshold** → default = score < 0.3  

---

## 👤 Author

**Thomas Jortzig**  
TravelTide Mastery Project | 05.09.2025
