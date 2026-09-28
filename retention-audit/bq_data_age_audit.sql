-- ============================================================================
-- BigQuery data-age audit (7-year retention)
-- ----------------------------------------------------------------------------
-- Finds, across EVERY table in a project/region, how much data is older than
-- 7 years — based on the date VALUES stored in the rows, not on table
-- creation time or ingestion-time partitions.
--
-- Read-only:
--   * No tables are created (not even temp tables). Results are collected in
--     script variables and returned as normal query output.
--   * Needs: BigQuery Data Viewer on the prod project + BigQuery Job User on
--     the project the query runs in.
--
-- How to run (BigQuery Console):
--   1. Find/replace  PROD_PROJECT  with the prod project id.
--   2. Find/replace  region-us     with the datasets' region
--      (region-eu, region-us-central1, ...). Run once per region.
--      If running from another project, set More > Query settings > Location
--      to the same region.
--   3. Optionally adjust max_scan_gb below (per-table safety cap).
--   4. Run. The Console shows 3 results (one per final SELECT);
--      use "Download results" to save each one.
--
-- What is checked:
--   * DATE / DATETIME / TIMESTAMP columns.
--   * STRING columns whose name looks like a date (date|time|_at|_dt|_ts),
--     parsed as ISO 'YYYY-MM-DD...'. Non-ISO values are counted in
--     unparsed_strings.
--   * Top-level columns only (dates nested in STRUCT/ARRAY or stored as epoch
--     INT64 are not covered — those tables show up in Output 3).
--
-- Outputs:
--   1. Data age + storage per table/column, largest estimated old data first.
--   2. Tables skipped (estimated scan > max_scan_gb) or that errored.
--   3. Tables with no date-like column (cannot be aged by content).
--
-- If the script fails with a variable-size error (very many tables), add
--   AND c.table_schema = 'some_dataset'
-- to the WHERE clause below and run one dataset at a time.
-- ============================================================================

DECLARE cutoff DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 7 YEAR);
DECLARE max_scan_gb FLOAT64 DEFAULT 100;   -- per-table safety cap (estimated)

DECLARE acc ARRAY<STRUCT<dataset STRING, table_name STRING, column_name STRING, data_type STRING,
                         oldest DATE, newest DATE, rows_older_than_7y INT64,
                         rows_with_value INT64, unparsed_strings INT64>> DEFAULT [];
DECLARE one ARRAY<STRUCT<dataset STRING, table_name STRING, column_name STRING, data_type STRING,
                         oldest DATE, newest DATE, rows_older_than_7y INT64,
                         rows_with_value INT64, unparsed_strings INT64>>;
DECLARE notes ARRAY<STRING> DEFAULT [];

FOR t IN (
  WITH cols AS (
    SELECT c.table_catalog, c.table_schema, c.table_name, c.column_name, c.data_type,
      CASE c.data_type
        WHEN 'DATE'      THEN FORMAT('`%s`', c.column_name)
        WHEN 'DATETIME'  THEN FORMAT('DATE(`%s`)', c.column_name)
        WHEN 'TIMESTAMP' THEN FORMAT('DATE(`%s`)', c.column_name)
        ELSE FORMAT('SAFE_CAST(SUBSTR(`%s`, 1, 10) AS DATE)', c.column_name)  -- ISO strings
      END AS date_expr
    FROM `PROD_PROJECT.region-us.INFORMATION_SCHEMA.COLUMNS` c
    JOIN `PROD_PROJECT.region-us.INFORMATION_SCHEMA.TABLES` tb
      USING (table_catalog, table_schema, table_name)
    WHERE tb.table_type IN ('BASE TABLE', 'SNAPSHOT')
      AND NOT STARTS_WITH(c.table_schema, '_')
      AND (c.data_type IN ('DATE', 'DATETIME', 'TIMESTAMP')
           OR (c.data_type = 'STRING'
               AND REGEXP_CONTAINS(LOWER(c.column_name), r'date|time|_at$|_dt$|_ts$')))
  )
  SELECT cols.table_schema AS ds, cols.table_name AS tbl,
    -- rough scan estimate: rows x date columns x 16 bytes
    ROUND(COALESCE(ANY_VALUE(s.total_rows), 0) * COUNT(*) * 16 / POW(1024, 3), 2) AS est_gb,
    FORMAT("""
      SELECT ARRAY_AGG(STRUCT('%s' AS dataset, '%s' AS table_name, name AS column_name,
                              type AS data_type, oldest, newest, rows_older_than_7y,
                              rows_with_value, unparsed_strings))
      FROM (
        SELECT c.name, c.type, MIN(c.d) AS oldest, MAX(c.d) AS newest,
               COUNTIF(c.d < @cutoff) AS rows_older_than_7y, COUNT(c.d) AS rows_with_value,
               COUNTIF(c.raw IS NOT NULL AND c.d IS NULL) AS unparsed_strings
        FROM `%s.%s.%s`, UNNEST([%s]) AS c
        GROUP BY c.name, c.type)""",
      cols.table_schema, cols.table_name,
      cols.table_catalog, cols.table_schema, cols.table_name,
      STRING_AGG(FORMAT(
        "STRUCT('%s' AS name, '%s' AS type, %s AS d, CAST(`%s` AS STRING) AS raw)",
        cols.column_name, cols.data_type, cols.date_expr, cols.column_name), ', ')) AS stmt
  FROM cols
  LEFT JOIN `PROD_PROJECT.region-us.INFORMATION_SCHEMA.TABLE_STORAGE` s
    ON s.project_id = cols.table_catalog
   AND s.table_schema = cols.table_schema
   AND s.table_name = cols.table_name
   AND NOT s.deleted
  GROUP BY cols.table_catalog, cols.table_schema, cols.table_name
)
DO
  IF t.est_gb > max_scan_gb THEN
    SET notes = ARRAY_CONCAT(notes, [FORMAT('%s.%s  SKIPPED (~%.1f GB estimated scan)', t.ds, t.tbl, t.est_gb)]);
  ELSE
    BEGIN
      EXECUTE IMMEDIATE t.stmt INTO one USING cutoff AS cutoff;
      SET acc = ARRAY_CONCAT(acc, IFNULL(one, []));
    EXCEPTION WHEN ERROR THEN
      SET notes = ARRAY_CONCAT(notes, [FORMAT('%s.%s  ERROR: %s', t.ds, t.tbl, @@error.message)]);
    END;
  END IF;
END FOR;


-- ----------------------------------------------------------------------------
-- Output 1: data age + storage per table/column
--   est_old_logical_gb = table logical size x share of rows older than 7 years
--   (assumes old and new rows are similar in size; good enough for ranking).
--   Pick ONE business-date column per table — birth/expiry dates are not the
--   record's age.
-- ----------------------------------------------------------------------------
SELECT
  r.dataset,
  r.table_name,
  r.column_name,
  r.data_type,
  r.oldest,
  r.newest,
  r.rows_older_than_7y,
  r.rows_with_value,
  r.unparsed_strings,
  s.total_rows,
  ROUND(SAFE_DIVIDE(r.rows_older_than_7y, s.total_rows) * 100, 1)           AS pct_rows_older_than_7y,
  ROUND(s.total_logical_bytes  / POW(1024, 3), 2)                            AS table_logical_gb,
  ROUND(s.total_physical_bytes / POW(1024, 3), 2)                            AS table_physical_gb,
  ROUND(SAFE_DIVIDE(r.rows_older_than_7y, s.total_rows)
        * s.total_logical_bytes / POW(1024, 3), 2)                           AS est_old_logical_gb,
  ROUND(SAFE_DIVIDE(r.rows_older_than_7y, s.total_rows)
        * s.total_physical_bytes / POW(1024, 3), 2)                          AS est_old_physical_gb
FROM UNNEST(acc) r
LEFT JOIN `PROD_PROJECT.region-us.INFORMATION_SCHEMA.TABLE_STORAGE` s
  ON s.table_schema = r.dataset
 AND s.table_name = r.table_name
 AND NOT s.deleted
ORDER BY est_old_logical_gb DESC NULLS LAST, table_logical_gb DESC;


-- ----------------------------------------------------------------------------
-- Output 2: tables skipped (too large for the cap) or that errored.
--   Check skipped tables individually on their business-date column only.
-- ----------------------------------------------------------------------------
SELECT note
FROM UNNEST(notes) AS note
ORDER BY note;


-- ----------------------------------------------------------------------------
-- Output 3: tables with no date-like column (cannot be aged by content)
-- ----------------------------------------------------------------------------
SELECT
  tb.table_schema AS dataset,
  tb.table_name,
  ROUND(s.total_logical_bytes / POW(1024, 3), 2) AS table_logical_gb
FROM `PROD_PROJECT.region-us.INFORMATION_SCHEMA.TABLES` tb
LEFT JOIN `PROD_PROJECT.region-us.INFORMATION_SCHEMA.TABLE_STORAGE` s
  ON s.table_schema = tb.table_schema
 AND s.table_name = tb.table_name
 AND NOT s.deleted
WHERE tb.table_type IN ('BASE TABLE', 'SNAPSHOT')
  AND NOT STARTS_WITH(tb.table_schema, '_')
  AND CONCAT(tb.table_schema, '.', tb.table_name) NOT IN
      (SELECT CONCAT(dataset, '.', table_name) FROM UNNEST(acc))
ORDER BY table_logical_gb DESC NULLS LAST;


-- ============================================================================
-- Reference queries (run separately, free — metadata only)
-- ============================================================================

-- Which billing model each dataset uses (no row = LOGICAL, the default).
-- LOGICAL  -> use est_old_logical_gb
-- PHYSICAL -> use est_old_physical_gb (includes time travel + fail-safe)
--
-- SELECT schema_name, option_value AS storage_billing_model
-- FROM `PROD_PROJECT.region-us.INFORMATION_SCHEMA.SCHEMATA_OPTIONS`
-- WHERE option_name = 'storage_billing_model';

-- Exact old-data size for a table partitioned on its business-date column
-- (PARTITIONS is dataset-scoped; replace dataset_name).
--
-- SELECT table_name,
--        ROUND(SUM(IF(partition_id < FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 7 YEAR)),
--                     total_logical_bytes, 0)) / POW(1024, 3), 2) AS old_logical_gb
-- FROM `PROD_PROJECT.dataset_name.INFORMATION_SCHEMA.PARTITIONS`
-- WHERE partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')
-- GROUP BY table_name
-- ORDER BY old_logical_gb DESC;
