Day-of-Week Seasonal Forecast for Incoming Workload (Databricks SQL)
Built a forecasting query that uses the last 21 days of history to calculate average volume per weekday (Mon–Sun), then projects the next 30 days of incoming items. 
The logic generates a zero-filled historical calendar, computes daily counts, aggregates by ISO weekday, and joins those averages to a 30-day horizon. 
This supports workload planning, staffing decisions, and backlog forecasting.

# SQL

-- Day-of-Week Seasonal Average Forecast
-- • Training window: last 21 days (excluding today)
-- • Forecast horizon: next 30 days (including today)

WITH base_events AS (
  -- Source events representing incoming items
  SELECT
    DATE_TRUNC('DAY', created_at) AS d
  FROM fact.review_events
  WHERE created_at < CURRENT_DATE()
    AND (bucket_placement_method = 'Manual'
         OR bucket_name = 'Review')    -- proxy for "needs human review"
),

hist_calendar AS (
  -- Zero-filled 21-day history window
  SELECT seq_dt AS d
  FROM (
    SELECT EXPLODE(SEQUENCE(
      DATE_SUB(CURRENT_DATE(), 21),   -- 21 days ago
      DATE_SUB(CURRENT_DATE(), 1),    -- yesterday
      INTERVAL 1 DAY
    )) AS seq_dt
  )
),

hist_counts AS (
  -- Daily counts + ISO-style DOW (1=Mon..7=Sun)
  SELECT
    c.d,
    COALESCE(COUNT(b.d), 0) AS y,
    CASE WHEN dayofweek(c.d) = 1 THEN 7 ELSE dayofweek(c.d) - 1 END AS iso_dow
  FROM hist_calendar c
  LEFT JOIN base_events b ON b.d = c.d
  GROUP BY c.d
),

dow_avg AS (
  -- Average per weekday over the last 21 days
  SELECT
    CASE WHEN dayofweek(d) = 1 THEN 7 ELSE dayofweek(d) - 1 END AS iso_dow,
    AVG(y) AS avg_y
  FROM hist_counts
  GROUP BY CASE WHEN dayofweek(d) = 1 THEN 7 ELSE dayofweek(d) - 1 END
),

horizon AS (
  -- Forecast horizon: next 30 days (today .. +29)
  SELECT
    seq_dt AS d,
    CASE WHEN dayofweek(seq_dt) = 1 THEN 7 ELSE dayofweek(seq_dt) - 1 END AS iso_dow
  FROM (
    SELECT EXPLODE(SEQUENCE(
      CURRENT_DATE(),
      DATE_ADD(CURRENT_DATE(), 29),
      INTERVAL 1 DAY
    )) AS seq_dt
  )
),

weekday_labels AS (
  -- Weekday labels for charting
  SELECT 1 AS iso_dow, 'Mon' AS weekday UNION ALL
  SELECT 2, 'Tue' UNION ALL
  SELECT 3, 'Wed' UNION ALL
  SELECT 4, 'Thu' UNION ALL
  SELECT 5, 'Fri' UNION ALL
  SELECT 6, 'Sat' UNION ALL
  SELECT 7, 'Sun'
),

forecast AS (
  SELECT
    h.d,
    CAST(ROUND(COALESCE(da.avg_y, 0), 0) AS INT) AS forecast_value,
    h.iso_dow
  FROM horizon h
  LEFT JOIN dow_avg da USING (iso_dow)
)

SELECT
  f.d         AS date,
  f.forecast_value,
  wl.weekday,  -- useful for charts
  f.iso_dow    -- useful for grouping
FROM forecast f
LEFT JOIN weekday_labels wl USING (iso_dow)
ORDER BY date;
