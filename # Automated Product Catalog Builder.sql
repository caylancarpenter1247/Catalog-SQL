# Automated Product Catalog Builder (Entity Resolution Across Scrape, Vendor, and PIM)

This project demonstrates a SQL-based entity-resolution pipeline that builds a 
deduped, enriched product catalog from three sources:

- Retailer transaction / vendor feed data
- Web-scraped product pages
- An existing PIM catalog

The goal is to produce a **ready-to-upload catalog file per brand** that powers
reliable product matching and reduces manual catalog curation.

## Problem

We had large volumes of retailer purchase data and scraped product pages, but no
clean, unified catalog suitable for product matching or analytics. The existing
PIM contained partial data with inconsistent identifiers, and onboarding new
brands required manual spreadsheet work.

## Approach

1. **Normalize identifiers** across sources (UPC, model, SKU, GTIN).
2. **Join scrape data to vendor feed** using external product IDs.
3. **Score record quality** based on presence of description, images, URLs, and titles.
4. **Self-deduplicate** within each brand across UPC, model, SKU, and GTIN using
   window functions and the quality score.
5. **Cross-deduplicate vs PIM**:
   - Drop rows whose UPC already exists in the PIM.
   - Flag rows whose model collides with any PIM identifier (MPN, SKU, model).
6. **Emit a PIM-ready file** with:
   - Canonical identifiers
   - Descriptions and URLs
   - QA flags for collisions
   - Lineage back to scrape and vendor records

## Technologies & Techniques

- Databricks SQL / Spark SQL
- CTE pipelines
- Identifier normalization and regex cleaning
- Entity resolution across multiple systems
- Window functions for ranking and deduplication
- Cross-system reconciliation against a PIM snapshot

## Sample SQL:

WITH scrape_ok AS ( ... ),
     vendor_ok AS ( ... ),
     joined_raw AS ( ... ),
     joined_scored AS ( ... ),
     joined_ranked AS ( ... ),
     brand_dedup AS ( ... ),
     catalog AS ( ... ),
     screened AS ( ... ),
     ready_to_upload AS ( ... )
SELECT ...
FROM ready_to_upload;

### Sanitized Full SQL Query: 

-- Param: {{brand_name}}  -- e.g. '3M' (case-insensitive)

WITH
/* 0) WEB SCRAPE: only successful rows with a model; normalize keys */
scrape_ok AS (
  SELECT
    CAST(s.brand        AS STRING) AS brand,
    CAST(s.model        AS STRING) AS model,
    CAST(s.product_name AS STRING) AS product_name,
    CAST(s.product_url  AS STRING) AS product_url,
    CAST(s.description  AS STRING) AS scrape_description,
    CAST(s.store_sku    AS STRING) AS scrape_sku,
    CAST(s.product_id   AS STRING) AS scrape_product_id_str,
    CAST(s.gtin         AS STRING) AS scrape_gtin,  -- used for self-dedupe

    -- normalized keys
    regexp_replace(lower(trim(CAST(s.model AS STRING))), '[^a-z0-9]', '') AS model_norm,
    regexp_replace(lower(trim(CAST(s.store_sku AS STRING))), '[^a-z0-9]', '') AS sku_norm,
    regexp_replace(coalesce(trim(CAST(s.gtin AS STRING)), ''), '[^0-9]', '') AS gtin_norm
  FROM raw.website_products_scrape s
  JOIN raw.website_products_scrape_status st
    ON s.product_url = st.product_url
  WHERE st.status = 'Success'
    AND s.model IS NOT NULL AND trim(CAST(s.model AS STRING)) <> ''
    AND lower(CAST(s.brand AS STRING)) = lower({{brand_name}})
),

/* 1) VENDOR FEED: require UPC + non-null Store SKU; normalize keys */
vendor_ok AS (
  SELECT
    v.vendor_product_id,
    CAST(v.brand_name         AS STRING) AS brand_name,
    CAST(v.title              AS STRING) AS title,
    CAST(v.store_sku          AS STRING) AS vendor_sku,
    CAST(v.upc                AS STRING) AS vendor_upc,
    CAST(v.external_product_id AS STRING) AS vendor_external_product_id_str,
    CAST(v.description        AS STRING) AS vendor_description,
    CAST(v.product_url        AS STRING) AS vendor_product_url,
    CAST(v.image_urls         AS STRING) AS image_urls,

    -- normalized
    regexp_replace(lower(trim(CAST(v.store_sku AS STRING))), '[^a-z0-9]', '') AS vendor_sku_norm,
    regexp_replace(coalesce(trim(CAST(v.upc AS STRING)), ''), '[^0-9]', '') AS upc_norm
  FROM raw.vendor_products v
  WHERE v.upc IS NOT NULL  AND trim(CAST(v.upc AS STRING)) <> ''
    AND v.store_sku IS NOT NULL AND trim(CAST(v.store_sku AS STRING)) <> ''
    AND lower(CAST(v.brand_name AS STRING)) = lower({{brand_name}})
),

/* 2) JOIN: external product id ↔ scraped product id
      attach UPC & vendor fields to scrape rows */
joined_raw AS (
  SELECT
    s.brand,
    v.brand_name,
    s.model,
    s.product_name,
    s.product_url,
    s.scrape_description,
    s.scrape_sku,
    s.scrape_product_id_str,
    s.scrape_gtin,

    v.title,
    v.vendor_product_url,
    v.image_urls,
    v.vendor_product_id,
    v.vendor_sku,
    v.vendor_upc,
    v.vendor_external_product_id_str,

    -- normalized identifiers
    s.model_norm,
    s.sku_norm,
    s.gtin_norm,
    v.upc_norm
  FROM scrape_ok s
  JOIN vendor_ok v
    ON v.vendor_external_product_id_str = s.scrape_product_id_str
),

/* 3a) SCORE rows to pick the “best” record per key */
joined_scored AS (
  SELECT
    j.*,
    (
      (CASE WHEN j.scrape_description IS NOT NULL AND j.scrape_description <> '' THEN 1 ELSE 0 END) +
      (CASE WHEN j.image_urls        IS NOT NULL AND j.image_urls        <> '' THEN 1 ELSE 0 END) +
      (CASE WHEN j.vendor_product_url IS NOT NULL AND j.vendor_product_url <> '' THEN 1 ELSE 0 END) +
      (CASE WHEN j.title             IS NOT NULL AND j.title             <> '' THEN 1 ELSE 0 END)
    ) AS quality_score,
    length(coalesce(j.scrape_description, '')) AS desc_len
  FROM joined_raw j
),

/* 3b) SELF-DEDUPE within this brand:
       ensure a single record per UPC, Model, SKU, GTIN */
joined_ranked AS (
  SELECT
    js.*,

    CASE WHEN js.upc_norm   IS NULL OR js.upc_norm   = '' THEN 1
         ELSE row_number() OVER (
                PARTITION BY js.upc_norm
                ORDER BY js.quality_score DESC, js.desc_len DESC, js.vendor_product_id ASC
              )
    END AS rn_upc,

    CASE WHEN js.model_norm IS NULL OR js.model_norm = '' THEN 1
         ELSE row_number() OVER (
                PARTITION BY js.model_norm
                ORDER BY js.quality_score DESC, js.desc_len DESC, js.vendor_product_id ASC
              )
    END AS rn_model,

    CASE WHEN js.sku_norm   IS NULL OR js.sku_norm   = '' THEN 1
         ELSE row_number() OVER (
                PARTITION BY js.sku_norm
                ORDER BY js.quality_score DESC, js.desc_len DESC, js.vendor_product_id ASC
              )
    END AS rn_sku,

    CASE WHEN js.gtin_norm  IS NULL OR js.gtin_norm  = '' THEN 1
         ELSE row_number() OVER (
                PARTITION BY js.gtin_norm
                ORDER BY js.quality_score DESC, js.desc_len DESC, js.vendor_product_id ASC
              )
    END AS rn_gtin
  FROM joined_scored js
),

brand_dedup AS (
  SELECT *
  FROM joined_ranked
  WHERE rn_upc = 1 AND rn_model = 1 AND rn_sku = 1 AND rn_gtin = 1
),

/* 4) EXISTING PIM / PRODUCT CATALOG snapshot (for cross-system dedupe) */
catalog AS (
  SELECT
    CAST(c.manufacturer_part_number  AS STRING) AS manufacturer_part_number,
    CAST(c.manufacturer_sku          AS STRING) AS manufacturer_sku,
    CAST(c.manufacturer_model_number AS STRING) AS manufacturer_model_number,
    CAST(c.upc                       AS STRING) AS catalog_upc,

    regexp_replace(lower(trim(CAST(c.manufacturer_part_number  AS STRING))), '[^a-z0-9]', '') AS norm_mpn,
    regexp_replace(lower(trim(CAST(c.manufacturer_sku          AS STRING))), '[^a-z0-9]', '') AS norm_sku,
    regexp_replace(lower(trim(CAST(c.manufacturer_model_number AS STRING))), '[^a-z0-9]', '') AS norm_model,
    regexp_replace(coalesce(trim(CAST(c.upc AS STRING)), ''), '[^0-9]', '')                    AS norm_upc
  FROM dim.product_catalog c
),

/* 5) CROSS-DEDUPE vs existing catalog:
      - drop UPC duplicates
      - only flag model collisions */
screened AS (
  SELECT
    d.*,

    CASE WHEN d.upc_norm IS NOT NULL AND d.upc_norm <> '' AND EXISTS (
      SELECT 1 FROM catalog c WHERE c.norm_upc = d.upc_norm
    ) THEN TRUE ELSE FALSE END AS duplicate_upc_in_catalog,

    CASE WHEN d.model_norm IS NOT NULL AND d.model_norm <> '' AND EXISTS (
      SELECT 1 FROM catalog c WHERE d.model_norm IN (c.norm_mpn, c.norm_sku, c.norm_model)
    ) THEN TRUE ELSE FALSE END AS model_matches_identifier_in_catalog
  FROM brand_dedup d
),

/* 6) FINAL set to upload: exclude rows whose UPC already exists in the PIM */
ready_to_upload AS (
  SELECT *
  FROM screened
  WHERE duplicate_upc_in_catalog = FALSE OR duplicate_upc_in_catalog IS NULL
)

-- ==== OUTPUT: PIM upload format + lineage / flags ====
SELECT
  uuid()                          AS uuid,
  'GenericProduct'                AS family,

  -- use model as canonical manufacturer identifiers
  model                           AS manufacturer_part_number,
  model                           AS manufacturer_sku,
  model                           AS manufacturer_model_number,

  vendor_upc                      AS upc,
  CAST(NULL AS STRING)            AS ean,  -- left blank by design

  product_name                    AS manufacturer_product_name,
  product_url                     AS product_url,
  scrape_description              AS manufacturer_description,
  brand                           AS brand,
  brand                           AS brand_name,
  'disabled'                      AS publish_status,

  -- flags for QA / notification
  model_matches_identifier_in_catalog,
  duplicate_upc_in_catalog,

  -- lineage
  scrape_product_id_str           AS scraped_product_id,
  vendor_external_product_id_str  AS vendor_external_id,
  vendor_sku                      AS vendor_store_sku,
  image_urls
FROM ready_to_upload
ORDER BY brand, model;
