# Automated Product Catalog Builder

*Entity Resolution Across Retailer Data, Web Scraping, and Product Information Management*

## Overview

I built a SQL workflow in Databricks to automate product catalog creation using retailer purchase data, scraped product listings, and existing records in our product information management (PIM) system.

The catalog supported an automated product-matching system that identified products from retailer receipts using product identifiers and descriptive metadata.

The goal was to automate the manual work involved in expanding the catalog while improving the quality and coverage of product data available for matching.

## The Problem

We had access to a significant amount of product information through retailer purchase data, including product URLs and identifiers. A separate web-scraping process developed by our Data Science team allowed us to collect additional information directly from retailer product listings.

Initially, my team started manually reviewing those URLs, identifying products, collecting missing attributes, and preparing records for import into the PIM.

The process worked, but it required significant manual research and spreadsheet work. With the volume of product data available, it wasn't realistic to continue expanding the catalog at that pace.

There was also a larger opportunity beyond simply adding products.

Our automated matching system used catalog identifiers and descriptive metadata to recognize products from incoming receipts. Improving catalog coverage meant more opportunities to identify products automatically, including purchases from retailers outside the original data source.

## Solution Architecture

The workflow combined three sources:

- **Retailer/vendor data:** Purchase-related metadata, UPCs, SKUs, external product IDs, and available product attributes.
- **Web-scraped product listings:** Product models, names, descriptions, URLs, and other attributes collected from retailer websites.
- **Existing PIM records:** Catalog identifiers used to check for existing products and potential conflicts.

The SQL pipeline followed six stages:

1. **Validate and normalize:** Filter source records based on minimum identifier requirements and standardize UPC, model, SKU, and GTIN values.
2. **Join the data:** Use retailer-specific external product IDs to connect scraped listings with vendor product records.
3. **Score metadata quality:** Evaluate candidates based on the availability of descriptions, images, URLs, and titles.
4. **Resolve duplicates:** Rank records across normalized identifiers, prioritizing metadata completeness and description length.
5. **Reconcile with the PIM:** Exclude existing UPCs and flag potential model identifier conflicts.
6. **Generate the import file:** Map the final output to Akeneo's CSV import structure, allowing files to be exported directly from Databricks and prepared for import without manual column formatting.

## Design Decisions

### Minimum Identifier Requirements

Retailer product listings weren't always complete or consistent. Some records contained useful information but lacked the identifiers needed to reliably connect them to purchase data or existing catalog records.

I intentionally required specific identifiers before allowing records into the automated workflow.

That meant excluding some potentially usable products, but it helped avoid introducing records that couldn't be reliably matched downstream.

### Connecting Retailer and Scraped Data

The retailer's external product IDs provided a common reference between vendor product records and scraped listings.

Using these IDs allowed me to combine purchase-related metadata with descriptions, images, and other product attributes collected from the retailer's website.

This was important because much of the information needed to build out the catalog was already available. We just needed a repeatable way to connect and use it.

### Metadata Quality and Description Length

Not every product record contained the same amount of useful information.

I created a quality score based on the presence of four attributes: description, images, product URL, and title.

When candidates had the same score, I prioritized longer descriptions.

That decision was based on how the downstream product-matching algorithm worked. It used descriptive keywords to help recognize products, so additional product details could provide useful information for matching.

Description length wasn't a guarantee of better data, but it was a practical way to prioritize potentially richer records without reviewing each candidate manually.

### Deduplication Across Multiple Identifiers

A product could appear in multiple records, sometimes with different UPC, model, SKU, or GTIN values.

I used SQL window functions to rank records independently across each normalized identifier, using metadata quality and description length to prioritize candidates.

A record needed to rank first across all applicable identifiers before making it into the automated output.

This was intentional. I wanted to avoid adding questionable records just to maximize the number of products in the catalog.

Records that didn't pass those checks could be reviewed separately rather than automatically included.

### Existing Catalog Reconciliation

After selecting the strongest candidates, I compared them against existing PIM records.

Products with UPCs already in the catalog were excluded from the new-product output. I also checked model values against existing manufacturer part numbers, SKUs, and model numbers to flag potential conflicts.

This helped distinguish new catalog additions from records that might require additional review or enrichment.

### Akeneo-Ready Import Files

One of the requirements I built into the workflow was that the final output needed to match the CSV import structure used by Akeneo.

The final SQL output included the required column names, product family, manufacturer identifiers, descriptions, URLs, and publishing status.

This allowed me to run the query for a selected brand, export the results directly from Databricks, and use the file for import into Akeneo without manually restructuring the data in a spreadsheet.

I also retained source identifiers and QA flags to support record validation and investigation when needed.

Eliminating the manual file-preparation step was an important part of making the workflow useful beyond the query itself.

## Downstream Product Matching and Human Review

The catalog supported more than product onboarding. It also provided reference data for the automated product-matching system used to identify products from incoming retailer receipts.

When UPCs or manufacturer part numbers were parsed from receipts, the system could use those identifiers to recognize products already in the catalog, including purchases from retailers outside the original source.

Additional descriptions and product attributes also provided more information for the AI matching algorithm's confidence scoring.

I managed the team responsible for reviewing product matches through an internal application developed by the engineering team I worked alongside.

The human-in-the-loop process allowed analysts to investigate uncertain matches, validate product identification, and support continued improvements to the matching system.

Expanding the catalog helped extend product recognition beyond the existing rebate-qualified catalogs and gave both the algorithm and review team more complete product information to work with.

## Business Impact

Before this workflow, my team was manually reviewing product URLs from purchase metadata to identify products and prepare catalog records.

Automating that process reduced brand-specific catalog preparation from hours of research and spreadsheet work to minutes.

Based on the manual process already underway, I estimated that the broader automation effort avoided approximately **4,000 hours of manual product research and catalog preparation**.

The workflow also made it possible to expand catalog coverage faster, support automatic product identification across additional retailers, and improve the information available to the downstream matching algorithm and human review process.

## Technologies and Techniques

- Databricks SQL / Spark SQL
- Common Table Expressions (CTEs)
- Multi-source joins and data enrichment
- Identifier normalization using regular expressions
- Window functions and record ranking
- Metadata completeness scoring
- Multi-identifier deduplication
- Existing catalog reconciliation
- Akeneo CSV import mapping
- QA flags and source record traceability

## About the SQL

The query below is a sanitized version of the workflow. Proprietary identifiers, table names, and other implementation details have been generalized.

Some source validation rules and surrounding operational review steps are not fully represented in this example.

---

## Sanitized SQL Query

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

#### Solution Architecture

       Retailer Scraped Data              Vendor Feed Data
   (Model, Name, Description, etc)   +   (UPC, SKU, Ext ID, Images)
                        \              /
                         \            /
                       Entity Join on External Product ID
                                 |
                                 v
                          Enriched Records
                                 |
                +----------------+----------------+
                | normalization & key scoring     |
                v                                 |
        self-dedupe (UPC / Model / SKU / GTIN)    |
                |                                 |
                +---------------+-----------------+
                                |
                        Cross-dedupe vs PIM
                                |
                                v
                  Ready-to-upload Brand Catalog
               (PIM-formatted, deduped, enriched)

#### Impact
   
1. Reduced catalog-building time from hours/days → minutes
   - Previously required manual spreadsheet cleaning
   - Catalog upload per brand became a single filtered query
   - Enabled a scalable ingestion process
2. Increased product-matching accuracy
   - Clean catalog identifiers
   - Eliminated duplicate products
   - Rapid increase in accurate training data 
3. Improved supplier reporting
   - Reliable product and purchase data from receipts
   - Cleaner data for downstream attribution
4. Boosted data quality and integrity across systems
   - PIM stayed clean and duplication free
   - Scrapped/Vendor mismatches were resolved systematically
   - QA flags prevented silent data corruption 
