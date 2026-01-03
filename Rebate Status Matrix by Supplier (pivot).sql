Rebate Status Matrix by Supplier (pivot)
Implemented a pivoted view that shows counts of rebates by status (Draft, Pending, Approved, Paid, etc.) for each supplier, plus row totals. 
This was requested by leadership to provide a single view of rebate pipeline health across suppliers and is now a core visualization on the rebates dashboard.

# SQL

-- Rebates by Status and Supplier for a given month

WITH base AS (
  SELECT
    CASE r.status_enum
      WHEN 1  THEN 'Draft'
      WHEN 2  THEN 'Pending'
      WHEN 3  THEN 'Supplier Disputed'
      WHEN 5  THEN 'Platform Declined'
      WHEN 6  THEN 'Supplier Approved'
      WHEN 7  THEN 'Platform Approved'
      WHEN 8  THEN 'Auto Approved'
      WHEN 9  THEN 'Payment Pending'
      WHEN 10 THEN 'Paid'
      ELSE COALESCE(CAST(r.status_enum AS STRING), 'Unknown')
    END AS rebate_status,
    CASE r.supplier_id
      WHEN 1  THEN 'Supplier_A'
      WHEN 2  THEN 'Supplier_B'
      WHEN 3  THEN 'Supplier_C'
      WHEN 4  THEN 'Supplier_D'
      WHEN 5  THEN 'Supplier_E'
      WHEN 6  THEN 'Supplier_F'
      WHEN 7  THEN 'Supplier_G'
      WHEN 8  THEN 'Supplier_H'
      WHEN 9  THEN 'Supplier_I'
      WHEN 10 THEN 'Supplier_J'
      ELSE COALESCE(CAST(r.supplier_id AS STRING), 'Other')
    END AS supplier_name,
    r.submission_date
  FROM fact.rebates r
  WHERE submission_date >= DATE '2025-09-01'
    AND submission_date <  DATE '2025-10-01'
)

SELECT
  rebate_status,
  `Supplier_A`,
  `Supplier_B`,
  `Supplier_C`,
  `Supplier_D`,
  `Supplier_E`,
  `Supplier_F`,
  `Supplier_G`,
  `Supplier_H`,
  `Supplier_I`,
  `Supplier_J`,
  COALESCE(`Supplier_A`,0) + COALESCE(`Supplier_B`,0) + COALESCE(`Supplier_C`,0) +
  COALESCE(`Supplier_D`,0) + COALESCE(`Supplier_E`,0) + COALESCE(`Supplier_F`,0) +
  COALESCE(`Supplier_G`,0) + COALESCE(`Supplier_H`,0) + COALESCE(`Supplier_I`,0) +
  COALESCE(`Supplier_J`,0) AS total
FROM (
  SELECT rebate_status, supplier_name
  FROM base
) src
PIVOT (
  COUNT(*) FOR supplier_name IN (
    'Supplier_A',
    'Supplier_B',
    'Supplier_C',
    'Supplier_D',
    'Supplier_E',
    'Supplier_F',
    'Supplier_G',
    'Supplier_H',
    'Supplier_I',
    'Supplier_J'
  )
)
ORDER BY rebate_status;
