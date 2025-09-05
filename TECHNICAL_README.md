# TravelTide — Technical README (Segmentation Pipeline)

This document explains how the segmentation dataset is built end-to-end:
from raw session data to per-user segment assignment and perk mapping.

- Single entry point SQL: **`00_master_version_complete.sql`**
- Design: only CTEs + final `SELECT` (read-only friendly)
- Output: segment summary + perk mapping, plus optional debug views

---

## 1) Data Inputs & Assumptions

### Required tables

- `sessions` – one row per session (includes booking flags, clicks, timestamps)
- `users` – one row per user (signup date, demographics incl. `birthdate`, `has_children`, `married`, `home_airport`, `home_country`)
- `flights` – flight booking attributes (base fare, airports, times, discounts)
- `hotels` – hotel booking attributes (rooms, nights, per-night price, discounts)

### Cohort & filters

- Sessions on/after **`2023-01-04`**  
  (`WHERE session_start >= '2023-01-04'`)
- **Active users** only: **`COUNT(session_id) > 7`** sessions in the filtered window

### Geo helper

- CTE `mapping(destination, destination_country)` used to derive destination country
- UDF required: **`haversine_distance(lat1, lon1, lat2, lon2)`**  
  (used to compute `flight_distance_km`)

---

## 2) Session-Level Prep (CTEs)

### 2.1 Cleaning highlights (CTE: `session_based_prep`)

- **Nights**  
  - negative nights: fix by sign/using return/check-in dates  
  - zero nights: treat as **min. 1 night** (no day-use)
- **Discount flags**  
  - set discount to `FALSE` if amount is `NULL`
- **Booked flags**  
  - set booked to `FALSE` if core details missing (e.g., flight booked but airline null)
- **Hotel city**  
  - extract suffix after `"-"` from `hotel_name`
- **Session duration (min)**  
  - `ROUND(EXTRACT(EPOCH FROM (session_end - session_start))/60, 2)`
- **Age** (as of `2023-12-31`)
- **Flight distance (km)**  
  - computed via `haversine_distance` using home and destination coordinates

### 2.2 Trip status (CTE: `trip_status`)

- At `trip_id` level:
  - `cancelled` if **any** session has `cancellation = TRUE`
  - `booked` if any flight or hotel booked (and not cancelled)
  - `none` otherwise

### 2.3 Derived metrics (CTE: `session_based_final`)

Adds:

- `trip_status` per session (fallback `none` if `trip_id IS NULL`)
- `age_group`, `booking_category` (hotel only / flight only / flight+hotel / browsing)
- Browsing metrics (`clicks_per_min`, per-category clicks & durations)
- **Lead times** for each booking category
- **Stay length** buckets and flight-trip days/nights
- Weekender / short weekday flags (flight & hotel)
- **Seasons** (hotel check-in month / flight depart month)
- **Flight distance** category and **destination** category
- **Pricing**:
  - `flight_price_per_seat = base_fare_usd / seats`
  - `flight_price_per_km` (handles one-way vs. return)
  - `hotel_per_room_usd` already present in source (used later at user-level)

---

## 3) User-Level Aggregations

### 3.1 Totals (CTE: `user_based_prep`)

- Sums of time/clicks split by intent (booking/cancellation/browsing)
- **Bookings**: `total_bookings` counts each trip once (even if later cancelled)
- **Valid bookings**: bookings with `trip_status = 'booked'`
- Flight/hotel splits, **domestic/international/intercontinental** flight counts
- Discounts: counts and average discount amounts (flight/hotel)
- Seats, bags, rooms (valid-only), single/double-room patterns
- Nights: combined vs. hotel-only vs. flight-only; total nights
- Trip archetypes: `short_weekday_trips`, `weekend_trips`
- **Leadtime** sums (for later averages)
- **User info**: `age`, `is_married`, `has_children`
- Recency vs. signup: `last_booking_since_signup`, `last_session_since_signup`

### 3.2 Averages & rates (CTE: `user_based_avg`)

- **Working age flag**: `20–67` → `is_in_working_age`
- Average minutes/clicks per context
- **`valid_bookings`** (booked − cancelled)
- Average prices: `avg_flight_price_per_km`, `avg_hotel_price_per_night`
- **Seats/Bags/Rooms** averages
- **Nights** averages (combined/flight/hotel/total)
- **Travel quotes**: `weekdays_travel_quote`, `weekend_travel_quote`
- **Lead time** averages per category

---

## 4) Features & Normalization (CTE: `features_norm`)

### 4.1 Pass-throughs

- `has_children`, `is_in_working_age`, `weekdays_travel_quote`, `weekend_travel_quote`

### 4.2 Min-max normalization (invert where “lower is better”)

- `avg_bags_norm_invert`, `avg_seats_norm_invert`, `avg_nights_total_norm_invert`

### 4.3 Percentile flags (per user distribution)

- Quick/slow booker: `avg_min_booking` **p20/p80**  
- Low/high clicks per booking: **p20/p80**  
- Short lead time: **p20**  
- **Frequent traveler**: `valid_bookings` **p80 (disc)**  
- **Price level** flags for flight/hotel: **p20/p80**

### 4.4 Condition flags

- **Dreamer**: `total_sessions = total_browsing_only` **OR** (`total_bookings >=1` AND `valid_bookings = 0`)
- **New customer**: `last_booking_since_signup <= 28` days
- Family heuristics: `has_middle_num_seats` (3–6), `has_middle_num_rooms` (1–4), `has_middle_num_bags` (1–4)
- Age-based: `is_senior_traveler` (≥60 & ≥1 booking), `is_young_traveler` (≤25 & ≥1 booking)

### 4.5 Quotes (per user)

- `discount_booking_quote`, `discount_flight_quote`, `discount_hotel_quote`
- Destination mix quotes (domestic/international/intercontinental)

---

## 5) Scoring (CTE: `user_scores`)

Weighted rules per segment (weights mirror your feature tables):

- **Business**
  - Guard: `is_in_working_age = 1` **and** `weekdays_travel_quote ≥ 0.5`
  - Score = `0.6*weekdays_travel_quote + 0.2*avg_bags_norm_invert + 0.2*avg_seats_norm_invert`
- **Family**
  - Guard: `has_children = 1`
  - Score = `0.6*has_children + 0.2*has_middle_num_seats + 0.1*has_middle_num_rooms + 0.1*has_middle_num_bags`
- **Frequent Traveler**
  - Score = `is_frequent_traveler_p80` (binary)
- **New Customer**
  - Score = `is_new_customer_booking` (binary)
- **Dreamer**
  - Score = `is_dreamer_no_bookings` (binary)
- **Young Traveler**
  - Score = `is_young_traveler` (binary)
- **Premium Traveler**
  - Score = `0.5*is_high_price_hotel_p80 + 0.5*is_high_price_flight_p80` (if any high-price flag, else 0)
- **Budget Traveler**
  - Score = `0.5*is_budget_price_hotel_p20 + 0.5*is_budget_price_flight_p20` (if any low-price flag, else 0)
- **Deal Hunter**
  - Guard: `discount_booking_quote ≥ 0.5`
  - Score = `0.6*discount_booking_quote + 0.2*is_high_clicks_booker_p80 + 0.2*is_slow_booker_p80`

> Notes  
>
> - Unused prototypes (e.g., Senior/Weekender) are commented out in SQL and can be re-enabled.  
> - All scores are **0–1**. Guards zero-out the score if prerequisites fail.

---

## 6) Segment Assignment

### 6.1 Tall view (CTE: `user_scores_tall`)

- Unpivot per user into `(user_id, segment, score)` via `CROSS JOIN LATERAL VALUES (…)`.

### 6.2 Pick best segment (CTE: `assigned`)

- `DISTINCT ON (user_id)` ordered by `score DESC, segment ASC`  
  → tie-break is alphabetical and deterministic.

### 6.3 Fallback to “Others” (CTE: `assigned_cleaned`)

- If `MAX(score) < 0.3` → assign **`Others`**

---

## 7) Final Outputs

### 7.1 Segment summary + perk mapping (final SELECT)

Columns:

- `final_segment`, `users`, `share_perc`, `good_match_perc` (share with score ≥ 0.5),
- `perk` mapping:

  - Business → **Free Cancellation**
  - Family → **Free Hotel Meal**
  - Frequent Traveler → **Free Cancellation & Lounge**
  - New Customer → **Free Hotel Night With Flight**
  - Dreamer → **Welcome Discount & 48h Free Cancellation**
  - Young Traveler → **Welcome Drink & Free Late Check-Out**
  - Premium Traveler → **Priority Check-In & Lounge**
  - Budget Traveler → **Free Checked-Bag**
  - Deal Hunter → **Special Deal – Discount Offers**
  - Others → **Baseline Discount – TBD**

### 7.2 Optional debug queries (in file, commented)

- B) Inspect per-user assignment  
- C) Inspect `features_norm`  
- D) Inspect `user_based_avg`  
- E) Inspect `session_based_final`

Uncomment the block you need and run.

---

## 8) How to Run

1. Ensure DB user can execute the `haversine_distance` UDF (or replace with equivalent).
2. Open `00_master_version_complete.sql` in your SQL client (psql, DBeaver, DataGrip).
3. Run the whole script.  
   - The final `SELECT` returns the **segment summary table**.  
   - Uncomment sections **B–E** for deep-dives.
4. Export results to Tableau/Sheets for visuals as needed.

---

## 9) Quality Checks & Edge Cases

- **Cancellation logic:** A cancelled trip has a separate session; `trip_status` handles this correctly.  
- **Zero/negative nights:** coerced to **≥ 1** or recomputed from dates.  
- **Discount flags:** `TRUE` with `NULL` amount → forced to `FALSE`.  
- **Division safety:** all rate/avg formulas use `NULLIF(…, 0)` guards.  
- **Percentiles:** computed against the **current cohort** (`user_based_avg`) to stay seasonally relevant.  
- **Tie-breaks:** alphabetic on `segment` ensures deterministic assignment.  
- **Others threshold:** `0.3` can be tuned if you want fewer/more “Others”.

---

## 10) Tuning Guide

- **Change cohort** → edit date in CTE `filtered_sessions`.  
- **Active user threshold** → change `COUNT(*) > 7` in `active_users`.  
- **Feature thresholds** → adjust percentile cut-offs in `features_norm`.  
- **Weights** → edit formulas in `user_scores`.  
- **Others threshold** → adjust `score < 0.3` in `assigned_cleaned`.  
- **Add a segment** → add score formula in `user_scores`, extend `VALUES (…)` in `user_scores_tall`, and update perk mapping in final `CASE`.

---

## 11) Reproducibility Notes

- The pipeline is **idempotent**: running the script re-computes all CTEs from inputs.  
- No temp tables / writes → safe for read-only environments.  
- All joins on `trip_id` use `USING(trip_id)` to avoid duplicate columns.

---

## 12) Attribution

- SQL author: **Thomas Jortzig**  
- Docs & visuals: Final Report + Executive Summary (2025)
