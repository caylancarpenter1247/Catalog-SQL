Rebate Operations View with Deep Links to Line Items
Built an enriched rebate query that joins rebates to their underlying line items and product matches, adds human-readable status and supplier labels, and generates deep links into both the rebate portal and the line-item review tool. 
This resolved a major usability gap for operations staff, who previously had to manually cross-reference IDs across systems to investigate issues or correct rebate information.

# SQL

-- Enrich rebate records with line-item context and deep links
WITH line_items_dedup AS (
  SELECT
    supplier_line_item_id,
    external_line_item_guid,
    external_line_item_id,
    tenant_id,
    synced_at,
    ROW_NUMBER() OVER (
      PARTITION BY supplier_line_item_id
      ORDER BY synced_at DESC
    ) AS rn
  FROM raw.rebate_supplier_line_items
)

SELECT
    r.*,
    li.external_line_item_guid,
    li.external_line_item_id,
    CONCAT('https://internal-tools/line-item/', li.external_line_item_guid) AS line_item_url,
    CONCAT('https://internal-portal/rebates/', r.rebate_id)                 AS rebate_url,
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
        ELSE CAST(r.status_enum AS STRING)
    END AS rebate_status_label,
    s.supplier_name,
    r.tenant_id            AS rebate_tenant_id,
    r.synced_at            AS rebate_synced_at,
    li.synced_at           AS line_item_synced_at,
    pm.global_product_catalog_id,
    pm.source_system       AS receipt_source,
    pm.customer_id,
    pm.line_item_title,
    pm.product_match_confidence_score,
    pm.sku,
    pm.ocr_vendor_name,
    DATEDIFF(r.submission_date, r.created_on) AS days_between_created_and_submitted
FROM raw.rebates AS r
LEFT JOIN raw.rebate_line_item_links AS ril
  ON r.rebate_id = ril.rebate_id
LEFT JOIN line_items_dedup AS li
  ON ril.supplier_line_item_id = li.supplier_line_item_id
 AND li.rn = 1
LEFT JOIN dim.product_match_line_items AS pm
  ON LOWER(TRIM(pm.line_item_guid)) = LOWER(TRIM(li.external_line_item_guid))
LEFT JOIN dim.suppliers s
  ON r.supplier_id = s.supplier_id;
