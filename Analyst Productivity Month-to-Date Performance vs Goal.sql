Analyst Productivity: Month-to-Date Performance vs Goal
Built a Databricks SQL dashboard that tracks each analyst’s month-to-date throughput against a monthly target. 
The query calculates business days in the current month, multiplies by a daily goal to derive a monthly target per analyst, and counts unique items reviewed per person from a nested event structure. 
The dashboard surfaces MTD totals, monthly goal, and percent-to-goal for each team member, enabling managers and executives to monitor progress in real time.

# Techniques:

Parameter CTE for date boundaries & daily goal
Business-day calendar generation using sequence + dayofweek
Mapping human-readable names to system user IDs via a dimension table
Flattening nested arrays with LATERAL VIEW EXPLODE
Using ROW_NUMBER() to get the latest review per item per reviewer
Aggregation and KPI calculations (percent to goal)

## SQL

-- Team Performance: Month-to-Date Output vs Monthly Goal
-- ------------------------------------------------------
-- For a set of analysts, calculate:
--   • Month-to-date completed items
--   • Monthly goal (daily_goal × business days in month)
--   • Percent of goal achieved

WITH params AS (
  SELECT
    -- You can hard-code a specific month if needed:
    -- DATE '2025-10-01' AS month_start,
    -- DATE '2025-10-31' AS month_end_full,
    date_trunc('month', current_date()) AS month_start,
    last_day(current_date())            AS month_end_full,
    current_date()                      AS today,
    772                                 AS daily_goal           -- items per day per analyst
),

-- Explicit list of team members to include
TeamMembers AS (
  SELECT * FROM VALUES
    ('Candice','Hensen'),
    ('Deion','Passi'),
    ('Elizabeth','Levine'),
    ('Jolene','Febres'),
    ('Karen','Warnes'),
    ('Laura','Ronzzo'),
    ('Nick','Era'),
    ('Phillip','Astorino'),
    ('Robin','Dungan'),
    ('Suzanne','Ullman'),
    ('Leann','Mandl'),
    ('Thomas','Sullivan')
  AS t(first_name, last_name)
),

-- Map team members to their system user IDs
AllowedReviewers AS (
  SELECT
    CAST(u.user_id AS STRING)                    AS reviewer_id,
    CONCAT(u.first_name, ' ', u.last_name)       AS reviewer_name
  FROM dim.user_accounts u
  JOIN TeamMembers tm
    ON LOWER(u.first_name) = LOWER(tm.first_name)
   AND LOWER(u.last_name)  = LOWER(tm.last_name)
),

-- Count all business days (Mon–Fri) in the month
BusinessDaysInMonth AS (
  SELECT COUNT(1) AS business_days_in_month
  FROM (
    SELECT explode(sequence(
      (SELECT month_start    FROM params),
      (SELECT month_end_full FROM params),
      INTERVAL 1 DAY
    )) AS d
  )
  WHERE dayofweek(d) BETWEEN 2 AND 6   -- 1 = Sun, 7 = Sat
),

-- Latest review per item per reviewer
FlattenedReviews AS (
  SELECT
    e.item_id,
    DATE(a.reviewed_at)                 AS review_day,
    CAST(a.reviewer_id AS STRING)       AS reviewer_id,
    ROW_NUMBER() OVER (
      PARTITION BY e.item_id, CAST(a.reviewer_id AS STRING)
      ORDER BY CAST(a.reviewed_at AS TIMESTAMP) DESC
    ) AS rn
  FROM fact.review_events e
  LATERAL VIEW EXPLODE(e.review_attempts) AS a
  WHERE a.reviewer_id IS NOT NULL
    AND CAST(a.reviewer_id AS STRING) IN (
      SELECT reviewer_id FROM AllowedReviewers
    )
),

-- Month-to-date item counts (from month_start through today)
MTDCounts AS (
  SELECT
    fr.reviewer_id,
    COUNT(*) AS mtd_total
  FROM FlattenedReviews fr
  JOIN params p
    ON fr.rn = 1
   AND fr.review_day BETWEEN p.month_start AND p.today
  GROUP BY fr.reviewer_id
)

SELECT
  ar.reviewer_name                                      AS analyst,
  COALESCE(m.mtd_total, 0)                              AS mtd_total,
  (p.daily_goal * b.business_days_in_month)             AS monthly_goal,
  ROUND(
    100.0 * COALESCE(m.mtd_total, 0)
    / NULLIF(p.daily_goal * b.business_days_in_month, 0),
    2
  )                                                     AS percent_to_goal
FROM AllowedReviewers ar
CROSS JOIN params p
CROSS JOIN BusinessDaysInMonth b
LEFT JOIN MTDCounts m
  ON m.reviewer_id = ar.reviewer_id
ORDER BY ar.reviewer_name;
