# SQL Portfolio: Operational Analytics & Data Products

A collection of sanitized SQL case studies showing how I use data to solve operational problems, improve decision-making, and build more reliable workflows.

These examples were adapted from real business use cases across product catalog management, data-labeling operations, forecasting, quality assurance, and rebate operations. Proprietary names, identifiers, URLs, and schema details have been generalized or replaced.

## What This Portfolio Demonstrates

This repository focuses on more than query syntax. Each case study begins with a business problem and uses SQL to create an actionable output for operators, managers, or executives.

Across the projects, the work demonstrates:

- Translating ambiguous business problems into measurable logic
- Building multi-step transformations with CTEs
- Working with nested and semi-structured data in Databricks SQL
- Using window functions for ranking, deduplication, and event sequencing
- Normalizing identifiers across disconnected systems
- Creating operational KPIs, audits, forecasts, and exception queues
- Designing outputs for dashboards, investigation workflows, and downstream systems
- Balancing automation with QA controls and human review

## Case Studies

### 1. [Automated Product Catalog Builder](Case%20Study%3A%20Automated%20Product%20Catalog%20Builder.sql)

Builds a deduplicated, enriched, PIM-ready product catalog by combining retailer data, web-scraped product records, vendor feeds, and an existing product catalog.

**Business problem:** Product onboarding required extensive spreadsheet work, identifiers were inconsistent across systems, and duplicate records reduced downstream matching accuracy.

**Solution highlights:**

- Normalizes UPC, SKU, GTIN, model, and manufacturer identifiers
- Joins web-scraped records to vendor-product data
- Scores record completeness to retain the strongest candidate
- Deduplicates within each brand using multiple identifiers
- Reconciles new records against the existing product catalog
- Produces a structured upload file with lineage and QA flags

**Techniques:** Entity resolution, regex normalization, window functions, quality scoring, cross-system reconciliation, parameterized SQL, CTE pipelines

---

### 2. [Analyst Productivity: Month-to-Date Performance vs. Goal](Analyst%20Productivity%20Month-to-Date%20Performance%20vs%20Goal.sql)

Calculates each analyst's month-to-date output, monthly target, and percent-to-goal for use in a management dashboard.

**Business problem:** Leaders needed a reliable, current view of team performance that accounted for the number of business days in each month.

**Solution highlights:**

- Generates the current month's business-day calendar
- Maps employee names to system user IDs
- Flattens nested review-attempt arrays
- Retains the latest review per item and reviewer
- Calculates monthly goals from a configurable daily target
- Preserves analysts with zero completed items in the output

**Techniques:** Calendar generation, `LATERAL VIEW EXPLODE`, `ROW_NUMBER`, parameter CTEs, KPI calculations, left joins

---

### 3. [Data Labeling Accuracy Audit](Data%20Labeling%20Accuracy%20Audit.sql)

Identifies items initially left unmatched by one reviewer and later corrected by another reviewer.

**Business problem:** Throughput alone did not reveal whether analysts were overlooking valid product matches. Managers needed a defensible way to identify missed-match patterns for coaching and quality assurance.

**Solution highlights:**

- Flattens nested review histories
- Sequences manual review attempts chronologically
- Identifies the first reviewer decision
- Detects later successful corrections by a different reviewer
- Produces reviewer-level exception records for audit and coaching

**Techniques:** Event sequencing, nested data, window functions, exception detection, parameterized filtering

---

### 4. [Day-of-Week Seasonal Forecast for Incoming Work](Day-of-Week%20Seasonal%20Forecast%20for%20Incoming%20Line%20Items.sql)

Forecasts the next 30 days of incoming workload using recent weekday-level seasonality.

**Business problem:** Daily incoming volume varied substantially by weekday, making simple averages unreliable for staffing and backlog planning.

**Solution highlights:**

- Creates a zero-filled 21-day historical calendar
- Calculates daily incoming volume
- Derives average volume by ISO weekday
- Generates a 30-day forecast horizon
- Maps weekday averages to future dates for dashboarding

**Techniques:** Time-series preparation, zero-filled calendars, date sequences, seasonal averages, forecasting logic

---

### 5. [OCR Gap Detector for Missing Rebates](OCR%20Gap%20Detector%20for%20Missing%20Rebates.sql)

Surfaces rebate-eligible line items that were confidently matched to a product but could not generate a rebate because OCR returned a quantity or cost of zero.

**Business problem:** High-confidence product matches could still fail downstream when key receipt fields were missing, creating silent revenue leakage.

**Solution highlights:**

- Extracts and ranks nested match attempts
- Retains the latest automatic match
- Filters to 100% confidence matches
- Confirms active rebate eligibility
- Detects missing quantity or unit cost
- Generates direct investigation links for operations

**Techniques:** Exception monitoring, nested records, ranking, multi-table joins, operational deep links, revenue protection

---

### 6. [Rebate Operations Records Enrichment](Rebate%20Operations%20Records%20Enrichment.sql)

Creates an operations-friendly rebate view with underlying line-item context, product-match details, readable status labels, and direct links to investigation tools.

**Business problem:** Operations staff had to manually cross-reference IDs across systems to investigate rebate issues.

**Solution highlights:**

- Deduplicates synchronized line-item records
- Connects rebates to source line items and product matches
- Converts status enums into readable labels
- Adds supplier and receipt-source context
- Calculates processing-cycle timing
- Generates deep links into operational systems

**Techniques:** Record enrichment, deduplication, enum mapping, lineage, operational usability, cycle-time analysis

---

### 7. [Rebate Status Matrix by Supplier](Rebate%20Status%20Matrix%20by%20Supplier%20%28pivot%29.sql)

Creates a pivoted matrix of rebate counts by status and supplier for an executive dashboard.

**Business problem:** Leadership needed one view of rebate pipeline health across suppliers rather than separate reports or manually assembled summaries.

**Solution highlights:**

- Translates system enums into business-readable statuses
- Groups activity by supplier and reporting period
- Pivots supplier values into dashboard-ready columns
- Calculates total volume for each status

**Techniques:** SQL pivoting, status mapping, reporting-period filters, executive dashboard preparation

## Technical Environment

The examples primarily use **Databricks SQL / Spark SQL** and demonstrate patterns including:

```text
CTEs                         Window functions
LATERAL VIEW EXPLODE        Date and sequence generation
Conditional aggregation     PIVOT
Regex-based normalization   Parameterized queries
Entity resolution           Data-quality exception logic
```

The schemas and tables shown are representative and are not included in this repository. These queries are intended as portfolio case studies rather than directly executable examples against a public dataset.

## Business Themes

| Theme | Examples |
|---|---|
| Operational performance | Analyst productivity dashboard |
| Data quality and governance | Accuracy audit, OCR gap detection |
| Forecasting and planning | Day-of-week workload forecast |
| Entity resolution | Automated product catalog builder |
| Revenue protection | OCR gap detector, rebate enrichment |
| Executive reporting | Supplier rebate-status matrix |
| Workflow enablement | Deep links and investigation-ready outputs |

## About Me

I am an analytics and operations leader who builds better ways of working at the intersection of data, systems, and business process.

My experience includes SQL analytics, product and catalog data, operational forecasting, KPI design, data-quality controls, workflow automation, stakeholder alignment, and translating business needs into scalable technical solutions.

This repository is designed to show both the technical implementation and the reasoning behind the work: the problem being solved, the decisions encoded in the query, and the operational value of the output.

## Notes

- All examples have been sanitized.
- Company-specific names, URLs, IDs, and schema details have been replaced or generalized.
- The underlying data is proprietary and is not included.
- Queries may require adaptation to run outside their original Databricks environment.
