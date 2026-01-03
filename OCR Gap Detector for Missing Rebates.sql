OCR Gap Detector for Missing Rebates
Created a query to surface cases where rebate-eligible line items were automatically matched to products with 100% confidence, but OCR failed to capture quantity or cost (0 values).
The result set links directly to the offending line item in the internal review tool and includes product metadata, enabling operations to correct issues that would otherwise block rebate generation.

# SQL

-- Identify automatically matched, rebate-eligible line items
-- where OCR missed quantity or cost (0 values)

WITH exploded AS (
  SELECT
    e.document_id,
    e.parsed_document,
    a AS match_attempt
  FROM raw.product_match_events e
  LATERAL VIEW OUTER EXPLODE(e.match_attempts) AS a
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY document_id
      ORDER BY match_attempt.match_date DESC
    ) AS rn
  FROM exploded
),
latest_automatic_matches AS (
  SELECT *
  FROM ranked
  WHERE rn = 1
    AND match_attempt.match_type = 'Automatic'
    AND match_attempt.top_confidence_score = 100
)

SELECT
  li.line_item_guid,
  CONCAT('https://internal-tools/line-item/', li.line_item_guid) AS line_item_url,
  li.line_item_title,
  li.uploaded_at,
  li.purchased_at,
  li.customer_id,
  pc.manufacturer_part_number,
  pc.manufacturer_product_name,
  pc.upc
FROM latest_automatic_matches lam
LEFT JOIN dim.proof_of_purchase_line_items li
  ON lam.parsed_document.origin_entity_id = li.line_item_guid
LEFT JOIN dim.product_catalog pc
  ON pc.product_uuid = lam.parsed_document.global_product_catalog_id
LEFT JOIN raw.rebate_products rp
  ON rp.global_product_catalog_id = lam.parsed_document.global_product_catalog_id
LEFT JOIN raw.rebate_product_terms rpt
  ON rpt.product_id = rp.product_id
LEFT JOIN raw.rebate_terms t
  ON t.term_id = rpt.term_id
WHERE t.term_status = 'Active'
  AND (CAST(li.quantity AS DOUBLE) = 0 OR CAST(li.unit_cost AS DOUBLE) = 0);
