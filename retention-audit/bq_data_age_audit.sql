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
--   3. Adjust the settings below if needed.
--   4. Run the WHOLE script in one go (the outputs read variables filled by
--      the loop). The Console shows one child job per statement; open
--      "View results" on the last three and "Download results" for each.
--
-- Speed:
--   * Tables smaller than min_table_gb are not scanned (listed in Output 2).
--   * Tables are checked batch_size at a time in one query, so per-query
--     startup overhead is paid once per batch, not once per table.
--   * To parallelise, open several Console tabs and give each a different
--     dataset filter (see "Optional dataset filter" below).
--   * Cancelling mid-run returns nothing — results live in script variables.
--
-- What is checked:
--   * DATE / DATETIME / TIMESTAMP columns.
--   * STRING columns whose name looks like a date (date|time|_at|_dt|_ts),
--     parsed as ISO 'YYYY-MM-DD...'. Non-ISO values are counted in
--     unparsed_strings.
--   * Top-level columns only (dates nested in STRUCT/ARRAY or stored as epoch
--     INT64 are not covered — those tables show up in Output 2 as
--     "no date-like column").
--
-- Outputs:
--   1. Data age + storage per table/column, largest estimated old data first.
--   2. Every table NOT checked, with the reason and its size.
--   3. Batches that errored (the error message names the failing table;
--      re-run that dataset with the offending table excluded).
-- ============================================================================

DECLARE cutoff       DATE    DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 7 YEAR);
DECLARE min_table_gb FLOAT64 DEFAULT 1;     -- skip tables smaller than this (negligible cost)
DECLARE max_scan_gb  FLOAT64 DEFAULT 100;   -- skip tables whose estimated scan is larger
DECLARE batch_size   INT64   DEFAULT 50;    -- tables checked per query

DECLARE acc ARRAY<STRUCT<dataset STRING, table_name STRING, column_name STRING, data_type STRING,
                         oldest DATE, newest DATE, rows_older_than_7y INT64,
                         rows_with_value INT64, unparsed_strings INT64>> DEFAULT [];
DECLARE one ARRAY<STRUCT<dataset STRING, table_name STRING, column_name STRING, data_type STRING,
                         oldest DATE, newest DATE, rows_older_than_7y INT64,
                         rows_with_value INT64, unparsed_strings INT64>>;
DECLARE errors ARRAY<STRING> DEFAULT [];

FOR b IN (
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
      -- Optional dataset filter (use to run datasets in parallel tabs):
      -- AND c.table_schema IN ('dataset_a', 'dataset_b')
      -- Exclude the Firestore scan's audit dataset (its rows carry old
      -- create_time values and would show up as "old data"):
      -- AND c.table_schema != 'audit'
      AND (c.data_type IN ('DATE', 'DATETIME', 'TIMESTAMP')
           OR (c.data_type = 'STRING'
               AND REGEXP_CONTAINS(LOWER(c.column_name), r'date|time|_at$|_dt$|_ts$')))
  ),
  per_table AS (
    SELECT
      cols.table_schema AS ds,
      cols.table_name   AS tbl,
      COALESCE(ANY_VALUE(s.total_logical_bytes), 0) / POW(1024, 3) AS table_gb,
      -- rough scan estimate: rows x date columns x 16 bytes
      COALESCE(ANY_VALUE(s.total_rows), 0) * COUNT(*) * 16 / POW(1024, 3) AS est_gb,
      FORMAT("""
        (SELECT '%s' AS dataset, '%s' AS table_name, c.name AS column_name, c.type AS data_type,
                MIN(c.d) AS oldest, MAX(c.d) AS newest,
                COUNTIF(c.d < @cutoff) AS rows_older_than_7y, COUNT(c.d) AS rows_with_value,
                COUNTIF(c.raw IS NOT NULL AND c.d IS NULL) AS unparsed_strings
         FROM `%s.%s.%s`, UNNEST([%s]) AS c
         GROUP BY c.name, c.type)""",
        cols.table_schema, cols.table_name,
        cols.table_catalog, cols.table_schema, cols.table_name,
        STRING_AGG(FORMAT(
          "STRUCT('%s' AS name, '%s' AS type, %s AS d, CAST(`%s` AS STRING) AS raw)",
          cols.column_name, cols.data_type, cols.date_expr, cols.column_name), ', ')) AS q
    FROM cols
    LEFT JOIN `PROD_PROJECT.region-us.INFORMATION_SCHEMA.TABLE_STORAGE` s
      ON s.project_id = cols.table_catalog
     AND s.table_schema = cols.table_schema
     AND s.table_name = cols.table_name
     AND NOT s.deleted
    GROUP BY cols.table_catalog, cols.table_schema, cols.table_name
  ),
  eligible AS (
    SELECT *, DIV(ROW_NUMBER() OVER (ORDER BY ds, tbl) - 1, batch_size) AS batch_no
    FROM per_table
    WHERE table_gb >= min_table_gb
      AND est_gb   <= max_scan_gb
  )
  SELECT
    batch_no,
    STRING_AGG(CONCAT(ds, '.', tbl), ', ' ORDER BY ds, tbl) AS tables_in_batch,
    FORMAT("""
      SELECT ARRAY_AGG(STRUCT(dataset, table_name, column_name, data_type, oldest, newest,
                              rows_older_than_7y, rows_with_value, unparsed_strings))
      FROM (%s)""",
      STRING_AGG(q, '\nUNION ALL\n')) AS stmt
  FROM eligible
  GROUP BY batch_no
  ORDER BY batch_no
)
DO
  BEGIN
    EXECUTE IMMEDIATE b.stmt INTO one USING cutoff AS cutoff;
    SET acc = ARRAY_CONCAT(acc, IFNULL(one, []));
  EXCEPTION WHEN ERROR THEN
    SET errors = ARRAY_CONCAT(errors,
      [FORMAT('batch %d [%s]  ERROR: %s', b.batch_no, b.tables_in_batch, @@error.message)]);
  END;
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
-- Output 2: every table NOT checked, with the reason.
--   * too large       -> check individually on its business-date column only
--   * below min size  -> negligible cost; lower min_table_gb to include
--   * no date column  -> dates may be epoch INT64 / nested; review manually
--   * (anything else) -> its batch errored, see Output 3
-- ----------------------------------------------------------------------------
SELECT
  tb.table_schema AS dataset,
  tb.table_name,
  ROUND(s.total_logical_bytes / POW(1024, 3), 2) AS table_logical_gb,
  dc.date_columns,
  CASE
    WHEN dc.date_columns IS NULL THEN 'no date-like column'
    WHEN COALESCE(s.total_logical_bytes, 0) / POW(1024, 3) < min_table_gb THEN 'below min_table_gb'
    WHEN COALESCE(s.total_rows, 0) * dc.date_columns * 16 / POW(1024, 3) > max_scan_gb
      THEN FORMAT('too large (~%.1f GB estimated scan)', s.total_rows * dc.date_columns * 16 / POW(1024, 3))
    ELSE 'batch errored - see Output 3'
  END AS reason
FROM `PROD_PROJECT.region-us.INFORMATION_SCHEMA.TABLES` tb
LEFT JOIN `PROD_PROJECT.region-us.INFORMATION_SCHEMA.TABLE_STORAGE` s
  ON s.table_schema = tb.table_schema
 AND s.table_name = tb.table_name
 AND NOT s.deleted
LEFT JOIN (
  SELECT table_schema, table_name, COUNT(*) AS date_columns
  FROM `PROD_PROJECT.region-us.INFORMATION_SCHEMA.COLUMNS`
  WHERE data_type IN ('DATE', 'DATETIME', 'TIMESTAMP')
     OR (data_type = 'STRING'
         AND REGEXP_CONTAINS(LOWER(column_name), r'date|time|_at$|_dt$|_ts$'))
  GROUP BY table_schema, table_name
) dc
  ON dc.table_schema = tb.table_schema
 AND dc.table_name = tb.table_name
WHERE tb.table_type IN ('BASE TABLE', 'SNAPSHOT')
  AND NOT STARTS_WITH(tb.table_schema, '_')
  -- AND tb.table_schema != 'audit'   -- keep in sync with the exclusion above
  AND CONCAT(tb.table_schema, '.', tb.table_name) NOT IN
      (SELECT CONCAT(dataset, '.', table_name) FROM UNNEST(acc))
ORDER BY table_logical_gb DESC NULLS LAST;


-- ----------------------------------------------------------------------------
-- Output 3: batches that errored (message names the failing table)
-- ----------------------------------------------------------------------------
SELECT e AS error
FROM UNNEST(errors) AS e;


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
