Data Labeling Accuracy Audit
Built a query to identify cases where a line item was manually reviewed by one analyst without selecting a product match, then later corrected by a different analyst who did select a valid match. 
The logic flattens nested match-attempt arrays, identifies the earliest manual review per line item, and compares it to subsequent manual reviews to detect “missed matches” attributable to specific reviewers. 
This powers an accuracy KPI used in performance coaching and quality audits.

# SQL 

-- Accuracy Audit:
-- Find items where a given reviewer did a manual review but left the product
-- unmatched, and a different reviewer later corrected it by selecting a product.

WITH Flattened AS (
  SELECT
    e.line_item_id,
    e.event_id,
    a.matched_product_id,
    a.matched_at,
    a.reviewer_id,
    a.match_type,
    ROW_NUMBER() OVER (
      PARTITION BY e.line_item_id
      ORDER BY a.matched_at ASC
    ) AS rn
  FROM analytics.review_events e
  LATERAL VIEW OUTER EXPLODE(e.match_attempts) AS a
  WHERE a.match_type <> 'Automatic'
),

-- First (earliest) manual review per line_item_id
FirstMatch AS (
  SELECT *
  FROM Flattened
  WHERE rn = 1
),

-- Later manual review attempts with a successful product match
LaterMatches AS (
  SELECT *
  FROM Flattened
  WHERE rn > 1
    AND matched_product_id IS NOT NULL
),

-- Line items where:
-- 1) The first reviewer did NOT select a product (matched_product_id IS NULL)
-- 2) A later *different* reviewer did select a product
Corrections AS (
  SELECT
    f.line_item_id,
    f.reviewer_id        AS original_reviewer_id,
    l.reviewer_id        AS correcting_reviewer_id,
    f.matched_at         AS original_reviewed_at,
    l.matched_at         AS corrected_at,
    l.matched_product_id AS corrected_product_id
  FROM FirstMatch f
  JOIN LaterMatches l
    ON f.line_item_id = l.line_item_id
   AND f.reviewer_id <> l.reviewer_id
  WHERE f.matched_product_id IS NULL
    AND f.reviewer_id = {{target_reviewer_id}}   -- parameter for the reviewer being audited
)

SELECT *
FROM Corrections
ORDER BY corrected_at DESC;
